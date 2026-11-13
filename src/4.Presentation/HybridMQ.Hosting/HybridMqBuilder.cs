namespace HybridMQ.Hosting;

using System.Net;
using System.Text.Json;

using HybridMQ.Client;
using HybridMQ.Client.Abstractions;
using HybridMQ.Cluster;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Protocol;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using HybridMQ.Storage.File;
using HybridMQ.Storage.MongoDb;
using HybridMQ.Storage.PostgreSql;
using HybridMQ.Transport.Local;
using HybridMQ.Transport.Tcp;

public enum StorageBackendType
{
    File,
    PostgreSql,
    MongoDb
}

public enum TransportBackendType
{
    EmbeddedDirect,
    LocalInMemory,
    LocalNamedPipe,
    Tcp
}

public sealed class HybridMqOptions
{
    public StorageBackendType StorageType { get; set; } = StorageBackendType.File;
    public string DataDirectory { get; set; } = "./data/hybridmq";
    public string? ConnectionString { get; set; }
    public string DatabaseOrSchemaName { get; set; } = "hybridmq";

    public TransportBackendType TransportType { get; set; } = TransportBackendType.EmbeddedDirect;
    public string LocalPipePrefix { get; set; } = "hybridmq_pipe";
    public IPEndPoint? TcpListenEndPoint { get; set; }
    public IPEndPoint? RemoteBrokerEndPoint { get; set; }
    public NodeId TargetBrokerNodeId { get; set; } = new("broker-primary");

    public ResourceProfile Profile { get; set; } = ResourceProfile.Standard;
    public int? OverrideMaxMemoryMb { get; set; }
    public NodeId NodeId { get; set; } = new("broker-primary");

    // --- 클러스터링 옵션 ---
    public string? AdvertisedEndpoint { get; set; }
    public IReadOnlyList<string> SeedPeers { get; set; } = [];
    public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan PartitionLeaseTtl { get; set; } = TimeSpan.FromSeconds(10);
}

public sealed class HybridMqRuntimeHost : IAsyncDisposable
{
    private readonly CancellationTokenSource? _clusterCts;
    private readonly Task? _clusterGossipTask;
    private readonly TcpTransportChannel? _peerTransport;

    public BrokerEngine Engine { get; }
    public IHybridMqInstance Client { get; }
    public ClusterMembershipManager Membership { get; }
    public PartitionLeaseCoordinator LeaseCoordinator { get; }
    public TcpBrokerServer? TcpServer { get; }
    public LocalNamedPipeServer? PipeServer { get; }

    public HybridMqRuntimeHost(
        BrokerEngine engine,
        IHybridMqInstance client,
        ClusterMembershipManager membership,
        PartitionLeaseCoordinator leaseCoordinator,
        TcpBrokerServer? tcpServer = null,
        LocalNamedPipeServer? pipeServer = null,
        TcpTransportChannel? peerTransport = null,
        CancellationTokenSource? clusterCts = null,
        Task? clusterGossipTask = null)
    {
        Engine = engine;
        Client = client;
        Membership = membership;
        LeaseCoordinator = leaseCoordinator;
        TcpServer = tcpServer;
        PipeServer = pipeServer;
        _peerTransport = peerTransport;
        _clusterCts = clusterCts;
        _clusterGossipTask = clusterGossipTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_clusterCts is not null)
        {
            await _clusterCts.CancelAsync().ConfigureAwait(false);
            if (_clusterGossipTask is not null)
            {
                try { await _clusterGossipTask.ConfigureAwait(false); } catch (OperationCanceledException) { }
            }
            _clusterCts.Dispose();
        }

        await Client.DisposeAsync().ConfigureAwait(false);
        if (TcpServer is not null) await TcpServer.DisposeAsync().ConfigureAwait(false);
        if (PipeServer is not null) await PipeServer.DisposeAsync().ConfigureAwait(false);
        if (_peerTransport is not null) await _peerTransport.DisposeAsync().ConfigureAwait(false);

        // 리스 반납 -> 엔진 종료 -> 멤버십 종료 순서로 안전하게 정리
        await LeaseCoordinator.DisposeAsync().ConfigureAwait(false);
        await Engine.DisposeAsync().ConfigureAwait(false);
        await Membership.DisposeAsync().ConfigureAwait(false);
    }
}

public static class HybridMqFactory
{
    public static IStorageProvider CreateStorageProvider(HybridMqOptions opts)
    {
        return opts.StorageType switch
        {
            StorageBackendType.File => new FileStorageProvider(opts.DataDirectory),
            StorageBackendType.PostgreSql => new PostgreSqlStorageProvider(
                opts.ConnectionString ?? throw new ArgumentException("ConnectionString is required for PostgreSQL storage."),
                opts.DatabaseOrSchemaName),
            StorageBackendType.MongoDb => new MongoDbStorageProvider(
                opts.ConnectionString ?? throw new ArgumentException("ConnectionString is required for MongoDB storage."),
                opts.DatabaseOrSchemaName),
            _ => throw new NotSupportedException($"Unsupported storage: {opts.StorageType}")
        };
    }

    public static async ValueTask<IHybridMqInstance> CreateEmbeddedAsync(Action<HybridMqOptions>? configure = null, CancellationToken ct = default)
    {
        var host = await StartFullRuntimeAsync(configure, ct).ConfigureAwait(false);
        return new HostBoundClientWrapper(host);
    }

    public static async ValueTask<HybridMqRuntimeHost> StartFullRuntimeAsync(Action<HybridMqOptions>? configure = null, CancellationToken ct = default)
    {
        var opts = new HybridMqOptions();
        configure?.Invoke(opts);

        var storage = CreateStorageProvider(opts);
        var budget = new MemoryBudgetManager(opts.Profile, opts.OverrideMaxMemoryMb);

        string initialEndpoint = opts.AdvertisedEndpoint
            ?? (opts.TcpListenEndPoint is not null ? $"127.0.0.1:{opts.TcpListenEndPoint.Port}" : "127.0.0.1:5679");

        // 1. 멤버십 매니저 생성 및 자체 등록 (MonitorPeersAsync 백그라운드 감시 루프 시작)
        var membership = new ClusterMembershipManager(opts.NodeId, initialEndpoint, opts.HeartbeatTimeout);
        await membership.RegisterSelfAsync(ct).ConfigureAwait(false);

        // 2. 브로커 엔진 및 파티션 리스 코디네이터 초기화
        var engine = new BrokerEngine(opts.NodeId, storage, budget, membership);
        await engine.StartAsync(ct).ConfigureAwait(false);

        var leaseCoordinator = new PartitionLeaseCoordinator(opts.NodeId, storage, engine, opts.PartitionLeaseTtl);
        leaseCoordinator.Start();

        // 피어 장애 감지 시 죽은 노드가 쥐고 있던 파티션 리스 재획득(Rebalance) 시도
        //membership.OnPeerDied += _ => _ =  leaseCoordinator.RebalancePartitionsAsync();
        membership.OnPeerDied += deadPeer =>
        {
            _ = Task.Run(() => leaseCoordinator.RebalancePartitionsAsync().AsTask());
        };
        // 3. 피어 간 RPC 포워딩용 채널 및 ClusterRpcRouter 구성
        var peerTransport = new TcpTransportChannel();
        var localDispatcher = new BrokerRpcDispatcher(engine);
        var clusterRouter = new ClusterRpcRouter(opts.NodeId, localDispatcher, leaseCoordinator, peerTransport);

        async ValueTask<ReadOnlyMemory<byte>> HandleRpcBytes(ReadOnlyMemory<byte> reqBytes, CancellationToken token)
        {
            var req = JsonSerializer.Deserialize(reqBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcRequest)!;

            // 피어 노드 간 Ping(Heartbeat) 수신 시 멤버십 갱신 + peerTransport 라우팅 테이블 등록
            if (req.Command == BrokerCommandType.Ping && !string.IsNullOrEmpty(req.Group))
            {
                var senderId = new NodeId(req.Topic ?? "unknown-node");
                membership.RecordHeartbeat(senderId, req.Group);

                if (IPEndPoint.TryParse(req.Group, out var senderEp))
                    peerTransport.RegisterNodeEndpoint(senderId, senderEp);

                var respPayload = new BrokerRpcResponse(req.CorrelationId, true, Error: opts.NodeId.Value);
                return JsonSerializer.SerializeToUtf8Bytes(respPayload, BrokerProtocolJsonContext.Default.BrokerRpcResponse);
            }

            // 일반 요청은 ClusterRpcRouter를 통해 파티션 리더 노드로 자동 라우팅(또는 로컬 처리)
            var resp = await clusterRouter.RouteAndDispatchAsync(req, token).ConfigureAwait(false);
            return JsonSerializer.SerializeToUtf8Bytes(resp, BrokerProtocolJsonContext.Default.BrokerRpcResponse);
        }

        switch (opts.TransportType)
        {
            case TransportBackendType.EmbeddedDirect:
                return new HybridMqRuntimeHost(engine, new EmbeddedHybridMqInstance(engine), membership, leaseCoordinator, peerTransport: peerTransport);

            case TransportBackendType.LocalInMemory:
                {
                    var bus = new LocalInMemoryTransportBus();
                    bus.RegisterEndpoint(opts.NodeId, (_, frame, token) => HandleRpcBytes(frame, token));
                    var channel = new LocalTransportChannel(NodeId.NewId(), bus);
                    return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), membership, leaseCoordinator, peerTransport: peerTransport);
                }

            case TransportBackendType.LocalNamedPipe:
                {
                    var pipeServer = new LocalNamedPipeServer(opts.LocalPipePrefix, opts.NodeId, HandleRpcBytes);
                    pipeServer.Start();
                    var channel = new LocalTransportChannel(NodeId.NewId(), opts.LocalPipePrefix);
                    return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), membership, leaseCoordinator, pipeServer: pipeServer, peerTransport: peerTransport);
                }

            case TransportBackendType.Tcp:
                {
                    var listenEp = opts.TcpListenEndPoint ?? new IPEndPoint(IPAddress.Loopback, 0);
                    var tcpServer = new TcpBrokerServer(listenEp, HandleRpcBytes);
                    tcpServer.Start();

                    string actualAdvertised = opts.AdvertisedEndpoint ?? $"127.0.0.1:{tcpServer.BoundPort}";
                    membership.RecordHeartbeat(opts.NodeId, actualAdvertised);

                    var channel = new TcpTransportChannel();
                    var connectEp = opts.RemoteBrokerEndPoint ?? new IPEndPoint(IPAddress.Loopback, tcpServer.BoundPort);
                    channel.RegisterNodeEndpoint(opts.NodeId, connectEp);

                    CancellationTokenSource? clusterCts = null;
                    Task? clusterGossipTask = null;

                    if (opts.SeedPeers.Count > 0)
                    {
                        clusterCts = new CancellationTokenSource();
                        clusterGossipTask = RunClusterHeartbeatLoopAsync(
                            membership,
                            peerTransport,
                            opts.NodeId,
                            actualAdvertised,
                            opts.SeedPeers,
                            clusterCts.Token);
                    }

                    return new HybridMqRuntimeHost(
                        engine,
                        new RemoteHybridMqInstance(channel, opts.NodeId),
                        membership,
                        leaseCoordinator,
                        tcpServer: tcpServer,
                        peerTransport: peerTransport,
                        clusterCts: clusterCts,
                        clusterGossipTask: clusterGossipTask);
                }

            default:
                throw new NotSupportedException($"Unsupported transport: {opts.TransportType}");
        }
    }

    private static async Task RunClusterHeartbeatLoopAsync(
        ClusterMembershipManager membership,
        TcpTransportChannel peerTransport,
        NodeId localNodeId,
        string localAdvertisedEndpoint,
        IReadOnlyList<string> seedPeers,
        CancellationToken ct)
    {
        var probeChannel = new TcpTransportChannel();
        var knownEndpoints = new HashSet<string>(seedPeers);

        while (!ct.IsCancellationRequested)
        {
            foreach (var peer in membership.GetClusterSnapshot())
            {
                if (peer.NodeId != localNodeId && !string.IsNullOrEmpty(peer.Endpoint))
                    knownEndpoints.Add(peer.Endpoint);
            }

            foreach (var epStr in knownEndpoints.ToList())
            {
                if (epStr == localAdvertisedEndpoint) continue;

                try
                {
                    if (!IPEndPoint.TryParse(epStr, out var remoteEp)) continue;

                    var tempPeerKey = new NodeId($"probe-{epStr}");
                    probeChannel.RegisterNodeEndpoint(tempPeerKey, remoteEp);

                    var pingReq = new BrokerRpcRequest(
                        CorrelationId: Guid.NewGuid().ToString("N"),
                        Command: BrokerCommandType.Ping,
                        Topic: localNodeId.Value,
                        Group: localAdvertisedEndpoint);

                    byte[] reqBytes = JsonSerializer.SerializeToUtf8Bytes(pingReq, BrokerProtocolJsonContext.Default.BrokerRpcRequest);
                    var respBytes = await probeChannel.RequestAsync(tempPeerKey, reqBytes, ct).ConfigureAwait(false);
                    var resp = JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse);

                    if (resp is { Success: true } && !string.IsNullOrEmpty(resp.Error))
                    {
                        var remoteNodeId = new NodeId(resp.Error);
                        membership.RecordHeartbeat(remoteNodeId, epStr);

                        // ClusterRpcRouter가 lease.Owner(remoteNodeId)로 즉시 포워딩할 수 있도록 엔드포인트 매핑 등록
                        peerTransport.RegisterNodeEndpoint(remoteNodeId, remoteEp);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                    // 상대 노드 다운 또는 아직 기동 전 -> 무시 (MonitorPeersAsync가 타임아웃 시 Dead 처리)
                }
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        await probeChannel.DisposeAsync().ConfigureAwait(false);
    }

    public static IHybridMqInstance ConnectRemoteTcp(IPEndPoint remoteEndPoint, NodeId? targetNodeId = null)
    {
        var target = targetNodeId ?? new NodeId("broker-primary");
        var channel = new TcpTransportChannel();
        channel.RegisterNodeEndpoint(target, remoteEndPoint);
        return new RemoteHybridMqInstance(channel, target);
    }

    private sealed class HostBoundClientWrapper(HybridMqRuntimeHost host) : IHybridMqInstance
    {
        public IProducer Producer => host.Client.Producer;
        public IConsumer Consumer => host.Client.Consumer;
        public IAdminClient Admin => host.Client.Admin;
        public ValueTask DisposeAsync() => host.DisposeAsync();
    }
}