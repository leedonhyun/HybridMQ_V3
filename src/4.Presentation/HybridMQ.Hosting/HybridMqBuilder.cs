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
}

public sealed class HybridMqRuntimeHost : IAsyncDisposable
{
    private readonly CancellationTokenSource? _clusterCts;
    private readonly Task? _clusterGossipTask;

    public BrokerEngine Engine { get; }
    public IHybridMqInstance Client { get; }
    public ClusterMembershipManager? Membership { get; }
    public TcpBrokerServer? TcpServer { get; }
    public LocalNamedPipeServer? PipeServer { get; }

    public HybridMqRuntimeHost(
        BrokerEngine engine,
        IHybridMqInstance client,
        ClusterMembershipManager? membership = null,
        TcpBrokerServer? tcpServer = null,
        LocalNamedPipeServer? pipeServer = null,
        CancellationTokenSource? clusterCts = null,
        Task? clusterGossipTask = null)
    {
        Engine = engine;
        Client = client;
        Membership = membership;
        TcpServer = tcpServer;
        PipeServer = pipeServer;
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
        await Engine.DisposeAsync().ConfigureAwait(false);
        if (Membership is not null) await Membership.DisposeAsync().ConfigureAwait(false);
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

        var membership = new ClusterMembershipManager(opts.NodeId, initialEndpoint, opts.HeartbeatTimeout);
        var engine = new BrokerEngine(opts.NodeId, storage, budget, membership);
        await engine.StartAsync(ct).ConfigureAwait(false);

        var dispatcher = new BrokerRpcDispatcher(engine);
        async ValueTask<ReadOnlyMemory<byte>> HandleRpcBytes(ReadOnlyMemory<byte> reqBytes, CancellationToken token)
        {
            var req = JsonSerializer.Deserialize(reqBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcRequest)!;

            // 클러스터 노드 간 Ping(Heartbeat) 처리:
            // req.Topic = 발신자 NodeId, req.Group = 발신자 Endpoint
            if (req.Command == BrokerCommandType.Ping && !string.IsNullOrEmpty(req.Group))
            {
                var senderId = new NodeId(req.Topic ?? "unknown-node");
                membership.RecordHeartbeat(senderId, req.Group);

                // Success=true 응답의 Error 필드에 자신의 NodeId를 실어 회신
                var respPayload = new BrokerRpcResponse(req.CorrelationId, true, Error: opts.NodeId.Value);
                return JsonSerializer.SerializeToUtf8Bytes(respPayload, BrokerProtocolJsonContext.Default.BrokerRpcResponse);
            }

            var resp = await dispatcher.DispatchAsync(req, token).ConfigureAwait(false);
            return JsonSerializer.SerializeToUtf8Bytes(resp, BrokerProtocolJsonContext.Default.BrokerRpcResponse);
        }

        switch (opts.TransportType)
        {
            case TransportBackendType.EmbeddedDirect:
                return new HybridMqRuntimeHost(engine, new EmbeddedHybridMqInstance(engine), membership);

            case TransportBackendType.LocalInMemory:
                {
                    var bus = new LocalInMemoryTransportBus();
                    bus.RegisterEndpoint(opts.NodeId, (_, frame, token) => HandleRpcBytes(frame, token));
                    var channel = new LocalTransportChannel(NodeId.NewId(), bus);
                    return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), membership);
                }

            case TransportBackendType.LocalNamedPipe:
                {
                    var pipeServer = new LocalNamedPipeServer(opts.LocalPipePrefix, opts.NodeId, HandleRpcBytes);
                    pipeServer.Start();
                    var channel = new LocalTransportChannel(NodeId.NewId(), opts.LocalPipePrefix);
                    return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), membership, pipeServer: pipeServer);
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
                            opts.NodeId,
                            actualAdvertised,
                            opts.SeedPeers,
                            clusterCts.Token);
                    }

                    return new HybridMqRuntimeHost(
                        engine,
                        new RemoteHybridMqInstance(channel, opts.NodeId),
                        membership,
                        tcpServer: tcpServer,
                        clusterCts: clusterCts,
                        clusterGossipTask: clusterGossipTask);
                }

            default:
                throw new NotSupportedException($"Unsupported transport: {opts.TransportType}");
        }
    }

    private static async Task RunClusterHeartbeatLoopAsync(
        ClusterMembershipManager membership,
        NodeId localNodeId,
        string localAdvertisedEndpoint,
        IReadOnlyList<string> seedPeers,
        CancellationToken ct)
    {
        var peerChannel = new TcpTransportChannel();
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

                    var tempPeerKey = new NodeId($"peer-{epStr}");
                    peerChannel.RegisterNodeEndpoint(tempPeerKey, remoteEp);

                    var pingReq = new BrokerRpcRequest(
                        CorrelationId: Guid.NewGuid().ToString("N"),
                        Command: BrokerCommandType.Ping,
                        Topic: localNodeId.Value,
                        Group: localAdvertisedEndpoint);

                    byte[] reqBytes = JsonSerializer.SerializeToUtf8Bytes(pingReq, BrokerProtocolJsonContext.Default.BrokerRpcRequest);
                    var respBytes = await peerChannel.RequestAsync(tempPeerKey, reqBytes, ct).ConfigureAwait(false);
                    var resp = JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse);

                    if (resp is { Success: true } && !string.IsNullOrEmpty(resp.Error))
                    {
                        membership.RecordHeartbeat(new NodeId(resp.Error), epStr);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                    // 상대 노드 다운 또는 아직 기동 전 -> 무시 (타임아웃 시 EvaluatePeerHealth가 Dead 처리)
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

        await peerChannel.DisposeAsync().ConfigureAwait(false);
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