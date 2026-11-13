using HybridMQ.Hosting;

using System.Net;

//// CLI usage:
//// HybridMQ.Server [file|postgres|mongo] [path_or_conn_string] [tcp_port]
//string backend = args.Length > 0 ? args[0].ToLowerInvariant() : "file";
//string target = args.Length > 1 ? args[1] : "./data/hybridmq-server";
//int tcpPort = args.Length > 2 && int.TryParse(args[2], out var p) ? p : 5679;

//Console.WriteLine($"Starting HybridMQ Standalone Server | Storage={backend} | TCP Port={tcpPort}...");

//await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
//{
//    opts.StorageType = backend switch
//    {
//        "postgres" or "postgresql" => StorageBackendType.PostgreSql,
//        "mongo" or "mongodb" => StorageBackendType.MongoDb,
//        _ => StorageBackendType.File
//    };
//    if (opts.StorageType == StorageBackendType.File)
//        opts.DataDirectory = target;
//    else
//        opts.ConnectionString = target;

//    opts.TransportType = TransportBackendType.Tcp;
//    opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Any, tcpPort);
//});

//Console.WriteLine($"HybridMQ Server Listening on TCP 0.0.0.0:{host.TcpServer!.BoundPort}. Press Ctrl+C to stop.");
//await Task.Delay(Timeout.Infinite);


using System.Net;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;

// 실행 예:
// HybridMQ.Server.exe --node broker-1 --port 5679 --storage postgres --target "Host=localhost;Database=hmq;..."
// HybridMQ.Server.exe --node broker-2 --port 5680 --seeds 127.0.0.1:5679 --storage postgres --target "..."

using System.Net;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;

// 실행 예:
// 노드 1: HybridMQ.Server.exe --node broker-1 --port 5679
// 노드 2: HybridMQ.Server.exe --node broker-2 --port 5680 --seeds 127.0.0.1:5679
// 노드 3: HybridMQ.Server.exe --node broker-3 --port 5681 --seeds 127.0.0.1:5679

string nodeId = GetArg(args, "--node") ?? "broker-1";
int tcpPort = int.TryParse(GetArg(args, "--port"), out var p) ? p : 5679;
string backend = (GetArg(args, "--storage") ?? "file").ToLowerInvariant();
string target = GetArg(args, "--target") ?? $"./data/{nodeId}";
string[] seeds = (GetArg(args, "--seeds") ?? "")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

Console.WriteLine($"Starting HybridMQ Cluster Node | Node={nodeId} | Port={tcpPort} | Seeds=[{string.Join(", ", seeds)}]");

await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
{
    opts.NodeId = new NodeId(nodeId);
    opts.AdvertisedEndpoint = $"127.0.0.1:{tcpPort}";
    opts.SeedPeers = seeds;

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

// 피어 합류 및 이탈 실시간 로그 출력
if (host.Membership is not null)
{
    host.Membership.OnPeerJoined += peer => Console.WriteLine($"[CLUSTER] Peer Joined: {peer.Value}");
    host.Membership.OnPeerDied += peer => Console.WriteLine($"[CLUSTER] Peer Died  : {peer.Value}");
}

Console.WriteLine($"Cluster Node [{nodeId}] Listening on 0.0.0.0:{host.TcpServer!.BoundPort}. Press Ctrl+C to stop.");
await Task.Delay(Timeout.Infinite);

static string? GetArg(string[] args, string key)
{
    int idx = Array.IndexOf(args, key);
    return (idx >= 0 && idx + 1 < args.Length) ? args[idx + 1] : null;
}