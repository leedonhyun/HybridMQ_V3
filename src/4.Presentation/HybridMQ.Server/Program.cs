using System.Net;
using HybridMQ.Hosting;

// CLI usage:
// HybridMQ.Server [file|postgres|mongo] [path_or_conn_string] [tcp_port]
string backend = args.Length > 0 ? args[0].ToLowerInvariant() : "file";
string target = args.Length > 1 ? args[1] : "./data/hybridmq-server";
int tcpPort = args.Length > 2 && int.TryParse(args[2], out var p) ? p : 5679;

Console.WriteLine($"Starting HybridMQ Standalone Server | Storage={backend} | TCP Port={tcpPort}...");

await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
{
    opts.StorageType = backend switch
    {
        "postgres" or "postgresql" => StorageBackendType.PostgreSql,
        "mongo" or "mongodb" => StorageBackendType.MongoDb,
        _ => StorageBackendType.File
    };
    if (opts.StorageType == StorageBackendType.File)
        opts.DataDirectory = target;
    else
        opts.ConnectionString = target;

    opts.TransportType = TransportBackendType.Tcp;
    opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Any, tcpPort);
});

Console.WriteLine($"HybridMQ Server Listening on TCP 0.0.0.0:{host.TcpServer!.BoundPort}. Press Ctrl+C to stop.");
await Task.Delay(Timeout.Infinite);
