namespace HybridMQ.Hosting;

using System.Net;
using System.Text.Json;
using HybridMQ.Client;
using HybridMQ.Client.Abstractions;
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
}

public sealed class HybridMqRuntimeHost : IAsyncDisposable
{
    public BrokerEngine Engine { get; }
    public IHybridMqInstance Client { get; }
    public TcpBrokerServer? TcpServer { get; }
    public LocalNamedPipeServer? PipeServer { get; }

    public HybridMqRuntimeHost(
        BrokerEngine engine,
        IHybridMqInstance client,
        TcpBrokerServer? tcpServer = null,
        LocalNamedPipeServer? pipeServer = null)
    {
        Engine = engine;
        Client = client;
        TcpServer = tcpServer;
        PipeServer = pipeServer;
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync().ConfigureAwait(false);
        if (TcpServer is not null) await TcpServer.DisposeAsync().ConfigureAwait(false);
        if (PipeServer is not null) await PipeServer.DisposeAsync().ConfigureAwait(false);
        await Engine.DisposeAsync().ConfigureAwait(false);
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
        var engine = new BrokerEngine(opts.NodeId, storage, budget);
        await engine.StartAsync(ct).ConfigureAwait(false);

        var dispatcher = new BrokerRpcDispatcher(engine);
        async ValueTask<ReadOnlyMemory<byte>> HandleRpcBytes(ReadOnlyMemory<byte> reqBytes, CancellationToken token)
        {
            var req = JsonSerializer.Deserialize(reqBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcRequest)!;
            var resp = await dispatcher.DispatchAsync(req, token).ConfigureAwait(false);
            return JsonSerializer.SerializeToUtf8Bytes(resp, BrokerProtocolJsonContext.Default.BrokerRpcResponse);
        }

        switch (opts.TransportType)
        {
            case TransportBackendType.EmbeddedDirect:
                return new HybridMqRuntimeHost(engine, new EmbeddedHybridMqInstance(engine));

            case TransportBackendType.LocalInMemory:
            {
                var bus = new LocalInMemoryTransportBus();
                bus.RegisterEndpoint(opts.NodeId, (_, frame, token) => HandleRpcBytes(frame, token));
                var channel = new LocalTransportChannel(NodeId.NewId(), bus);
                return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId));
            }

            case TransportBackendType.LocalNamedPipe:
            {
                var pipeServer = new LocalNamedPipeServer(opts.LocalPipePrefix, opts.NodeId, HandleRpcBytes);
                pipeServer.Start();
                var channel = new LocalTransportChannel(NodeId.NewId(), opts.LocalPipePrefix);
                return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), pipeServer: pipeServer);
            }

            case TransportBackendType.Tcp:
            {
                var listenEp = opts.TcpListenEndPoint ?? new IPEndPoint(IPAddress.Loopback, 0);
                var tcpServer = new TcpBrokerServer(listenEp, HandleRpcBytes);
                tcpServer.Start();

                var channel = new TcpTransportChannel();
                var connectEp = opts.RemoteBrokerEndPoint ?? new IPEndPoint(IPAddress.Loopback, tcpServer.BoundPort);
                channel.RegisterNodeEndpoint(opts.NodeId, connectEp);
                return new HybridMqRuntimeHost(engine, new RemoteHybridMqInstance(channel, opts.NodeId), tcpServer: tcpServer);
            }

            default:
                throw new NotSupportedException($"Unsupported transport: {opts.TransportType}");
        }
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
