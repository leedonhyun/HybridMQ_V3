using HybridMQ.Client;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;

using System.Globalization;
using System.Net;
//터미널 1: dotnet run -- broker-1 5679

//터미널 2: dotnet run -- broker-2 5680

//터미널 3: dotnet run -- broker-3 5681
// 실행 예: dotnet run -- broker-1 5679
//string nodeIdStr = args.Length > 0 ? args[0] : "broker-1";
//int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 5679;

//string dataDir = Path.Combine(AppContext.BaseDirectory, "cluster-data", nodeIdStr);
//Directory.CreateDirectory(dataDir);

//await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
//{
//    // 1. 노드 식별자 및 스토리지 격리 (필수: 노드마다 폴더가 달라야 함)
//    opts.NodeId = new NodeId(nodeIdStr);
//    opts.StorageType = StorageBackendType.File;
//    opts.DataDirectory = dataDir;

//    // 2. 원격 접속을 위한 TCP 리스너 설정
//    opts.TransportType = TransportBackendType.Tcp;
//    opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Any, port);
//});
string nodeId = args.Length > 0 ? args[0] : "broker-1";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 5679;
string dataDir = Path.Combine(AppContext.BaseDirectory, "data", nodeId);

Console.WriteLine($"Starting HybridMQ Server | Node={nodeId} | TCP Port={port}...");

await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
{
    opts.NodeId = new NodeId(nodeId);
    opts.StorageType = StorageBackendType.File;
    opts.DataDirectory = dataDir;
    opts.TransportType = TransportBackendType.Tcp;
    opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Any, port);
});

await Task.Delay(Timeout.Infinite);
Console.WriteLine("==========================================");
Console.WriteLine($"[Broker Started] NodeId : {nodeId}");
Console.WriteLine($"[Broker Started] Listen : 0.0.0.0:{port}");
Console.WriteLine($"[Broker Started] DataDir: {dataDir}");
Console.WriteLine("==========================================");

// Ctrl+C 종료 대기
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine($"[{nodeIdStr}] Shutting down gracefully...");
}