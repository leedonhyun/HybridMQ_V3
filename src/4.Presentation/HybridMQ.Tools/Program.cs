using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Protocol;
using HybridMQ.Domain.Routing;
using HybridMQ.Hosting;
using HybridMQ.Storage.File;
using HybridMQ.Storage.File.IO;
using HybridMQ.Transport.Tcp;

if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    PrintHelp();
    return 0;
}

string command = args[0].ToLowerInvariant();
var options = ParseArgs(args.Skip(1).ToArray());

try
{
    switch (command)
    {
        case "ping":
            await RunPingAsync(options);
            break;
        case "topics":
            await RunListTopicsAsync(options);
            break;
        case "topic-create":
            await RunCreateTopicAsync(options);
            break;
        case "bind":
            await RunAddBindingAsync(options);
            break;
        case "produce":
            await RunProduceAsync(options);
            break;
        case "tail":
            await RunTailStreamAsync(options);
            break;
        case "dlq-inspect":
            await RunDlqInspectAsync(options);
            break;
        case "dlq-replay":
            await RunDlqReplayAsync(options);
            break;
        case "inspect-segment":
            await RunInspectSegmentAsync(options);
            break;
        case "recover-tail":
            await RunRecoverTailAsync(options);
            break;
        case "bench":
            await RunBenchmarkAsync(options);
            break;
        default:
            Console.Error.WriteLine($"Unknown command: {command}");
            PrintHelp();
            return 1;
    }
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[ERROR] {ex.Message}");
    return 1;
}

static async Task RunPingAsync(Dictionary<string, string> opts)
{
    var ep = GetTcpEndPoint(opts);
    var nodeId = new NodeId(opts.GetValueOrDefault("node", "broker-primary"));
    await using var channel = new TcpTransportChannel();
    channel.RegisterNodeEndpoint(nodeId, ep);

    var sw = Stopwatch.StartNew();
    var req = new BrokerRpcRequest(Guid.NewGuid().ToString("N"), BrokerCommandType.Ping);
    var respBytes = await channel.RequestAsync(nodeId, JsonSerializer.SerializeToUtf8Bytes(req, BrokerProtocolJsonContext.Default.BrokerRpcRequest));
    var resp = JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse)!;
    sw.Stop();

    Console.WriteLine(resp.Success
        ? $"PONG from {ep} ({nodeId}) in {sw.Elapsed.TotalMilliseconds:F2} ms"
        : $"PING FAILED: {resp.Error}");
}

static async Task RunListTopicsAsync(Dictionary<string, string> opts)
{
    if (opts.ContainsKey("data-dir") || opts.ContainsKey("storage"))
    {
        await using var storage = OpenDirectStorage(opts);
        await storage.InitializeAsync();
        var topics = await storage.ListTopicsAsync();
        Console.WriteLine($"{"TOPIC",-32} {"PARTITIONS",10} {"HIGH_WATERMARK",16}");
        Console.WriteLine(new string('-', 62));
        foreach (var t in topics)
        {
            long hwmSum = 0;
            for (int p = 0; p < t.PartitionCount; p++)
                hwmSum += (await storage.GetHighWatermarkAsync(new PartitionId(t.Name, p))).Value;
            Console.WriteLine($"{t.Name.Value,-32} {t.PartitionCount,10} {hwmSum,16}");
        }
        return;
    }

    var ep = GetTcpEndPoint(opts);
    var nodeId = new NodeId(opts.GetValueOrDefault("node", "broker-primary"));
    await using var channel = new TcpTransportChannel();
    channel.RegisterNodeEndpoint(nodeId, ep);

    var req = new BrokerRpcRequest(Guid.NewGuid().ToString("N"), BrokerCommandType.ListTopics);
    var respBytes = await channel.RequestAsync(nodeId, JsonSerializer.SerializeToUtf8Bytes(req, BrokerProtocolJsonContext.Default.BrokerRpcRequest));
    var resp = JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse)!;

    if (!resp.Success) throw new InvalidOperationException(resp.Error);

    Console.WriteLine($"{"TOPIC",-32} {"PARTITIONS",10} {"HIGH_WATERMARK",16}");
    Console.WriteLine(new string('-', 62));
    foreach (var t in resp.Topics ?? [])
    {
        Console.WriteLine($"{t.Name,-32} {t.PartitionCount,10} {t.HighWatermark,16}");
    }
}

static async Task RunCreateTopicAsync(Dictionary<string, string> opts)
{
    string topic = Require(opts, "topic");
    int partitions = int.Parse(opts.GetValueOrDefault("partitions", "1"));

    await using var client = await OpenClientAsync(opts);
    await client.Admin.CreateTopicAsync(topic, partitions);
    Console.WriteLine($"Topic '{topic}' created (partitions={partitions}).");
}

static async Task RunAddBindingAsync(Dictionary<string, string> opts)
{
    string exchange = Require(opts, "exchange");
    string pattern = Require(opts, "pattern");
    string topic = Require(opts, "topic");
    var exType = Enum.Parse<ExchangeType>(opts.GetValueOrDefault("type", "Topic"), ignoreCase: true);

    await using var client = await OpenClientAsync(opts);
    await client.Admin.AddBindingAsync(exchange, exType, pattern, topic);
    Console.WriteLine($"Binding added: Exchange='{exchange}' ({exType}) Pattern='{pattern}' -> Topic='{topic}'");
}

static async Task RunProduceAsync(Dictionary<string, string> opts)
{
    string topic = opts.GetValueOrDefault("topic", "");
    string? exchange = opts.GetValueOrDefault("exchange");
    string? routingKey = opts.GetValueOrDefault("routing-key");
    string? key = opts.GetValueOrDefault("key");
    string message = Require(opts, "message");
    int count = int.Parse(opts.GetValueOrDefault("count", "1"));
    int delayMs = int.Parse(opts.GetValueOrDefault("delay-ms", "0"));

    await using var client = await OpenClientAsync(opts);
    byte[] payload = Encoding.UTF8.GetBytes(message);
    TimeSpan? delay = delayMs > 0 ? TimeSpan.FromMilliseconds(delayMs) : null;

    for (int i = 0; i < count; i++)
    {
        var results = await client.Producer.PublishAsync(
            topic: topic,
            message: payload,
            key: key,
            routingKey: routingKey,
            exchange: exchange,
            delay: delay);

        foreach (var r in results)
        {
            Console.WriteLine($"Published [{i + 1}/{count}] -> {r.Partition} @ Offset {r.BaseOffset.Value}");
        }
    }
}

static async Task RunTailStreamAsync(Dictionary<string, string> opts)
{
    string topic = Require(opts, "topic");
    int partition = int.Parse(opts.GetValueOrDefault("partition", "0"));
    long from = long.Parse(opts.GetValueOrDefault("from", "0"));
    int limit = int.Parse(opts.GetValueOrDefault("limit", "50"));

    await using var client = await OpenClientAsync(opts);
    int printed = 0;
    await foreach (var rec in client.Consumer.ReadStreamAsync(topic, partition, new Offset(from), limit))
    {
        printed++;
        string keyStr = rec.Key.IsEmpty ? "-" : Encoding.UTF8.GetString(rec.Key.Span);
        string body = TryFormatUtf8(rec.Payload.Span);
        var ts = DateTimeOffset.FromUnixTimeMilliseconds(rec.TimestampUnixMs).ToString("HH:mm:ss.fff");
        Console.WriteLine($"[Offset={rec.Offset.Value} | {ts} | Flags={rec.Flags} | Key={keyStr}] {body}");
    }
    Console.WriteLine($"--- Total {printed} record(s) read ---");
}

static async Task RunDlqInspectAsync(Dictionary<string, string> opts)
{
    string topic = Require(opts, "topic");
    string group = Require(opts, "group");
    int partition = int.Parse(opts.GetValueOrDefault("partition", "0"));
    int limit = int.Parse(opts.GetValueOrDefault("limit", "50"));
    string dlqTopic = $"{topic}.{group}.dlq";

    await using var client = await OpenClientAsync(opts);
    int count = 0;
    await foreach (var rec in client.Consumer.ReadStreamAsync(dlqTopic, partition, Offset.Earliest, limit))
    {
        count++;
        string headers = string.Join(", ", rec.Headers.Select(h => $"{h.Key}={h.Value}"));
        string body = TryFormatUtf8(rec.Payload.Span);
        Console.WriteLine($"[DLQ Offset={rec.Offset.Value} | Headers: {headers}] {body}");
    }
    Console.WriteLine($"--- {count} dead-letter record(s) in '{dlqTopic}' ---");
}

static async Task RunDlqReplayAsync(Dictionary<string, string> opts)
{
    string topic = Require(opts, "topic");
    string group = Require(opts, "group");
    int partition = int.Parse(opts.GetValueOrDefault("partition", "0"));
    long fromOffset = long.Parse(opts.GetValueOrDefault("from", "0"));
    int limit = int.Parse(opts.GetValueOrDefault("limit", "100"));
    string dlqTopic = $"{topic}.{group}.dlq";

    await using var client = await OpenClientAsync(opts);
    int replayed = 0;
    await foreach (var rec in client.Consumer.ReadStreamAsync(dlqTopic, partition, new Offset(fromOffset), limit))
    {
        string? keyStr = rec.Key.IsEmpty ? null : Encoding.UTF8.GetString(rec.Key.Span);
        await client.Producer.PublishAsync(topic, rec.Payload.ToArray(), key: keyStr);
        replayed++;
    }
    Console.WriteLine($"Replayed {replayed} message(s) from '{dlqTopic}' back into '{topic}'.");
}

static async Task RunInspectSegmentAsync(Dictionary<string, string> opts)
{
    string filePath = Require(opts, "file");
    if (!File.Exists(filePath))
        throw new FileNotFoundException($"Segment file not found: {filePath}");

    byte[] bytes = await File.ReadAllBytesAsync(filePath);
    Console.WriteLine($"Inspecting binary segment: {filePath} ({bytes.Length:N0} bytes)");

    int cursor = 0;
    int validRecords = 0;
    while (cursor < bytes.Length)
    {
        try
        {
            if (BinaryRecordCodec.TryDecode(bytes.AsSpan(cursor), out var record, out int consumed) && record is not null)
            {
                validRecords++;
                Console.WriteLine($"  @{cursor,8} | Offset={record.Offset.Value,-6} | FrameBytes={consumed,-6} | Flags={record.Flags,-14} | PayloadBytes={record.Payload.Length}");
                cursor += consumed;
            }
            else
            {
                Console.WriteLine($"  @{cursor,8} | [INCOMPLETE TAIL] Remaining={bytes.Length - cursor} bytes");
                break;
            }
        }
        catch (HybridMQ.Domain.Exceptions.DataCorruptionException ex)
        {
            Console.WriteLine($"  @{cursor,8} | [CORRUPT RECORD] {ex.Message} (Remaining={bytes.Length - cursor} bytes)");
            break;
        }
    }
    Console.WriteLine($"Summary: {validRecords} valid record(s), clean end offset = {cursor} / {bytes.Length} bytes.");
}

static async Task RunRecoverTailAsync(Dictionary<string, string> opts)
{
    string dataDir = Require(opts, "data-dir");
    string topic = Require(opts, "topic");
    int partition = int.Parse(opts.GetValueOrDefault("partition", "0"));

    await using var provider = new FileStorageProvider(dataDir);
    await provider.InitializeAsync();
    int truncatedBytes = await provider.RecoverAndTruncateCorruptTailAsync(new PartitionId(new TopicName(topic), partition));
    Console.WriteLine($"Recovery completed for {topic}[{partition}]. Truncated {truncatedBytes} corrupt tail byte(s).");
}

static async Task RunBenchmarkAsync(Dictionary<string, string> opts)
{
    int count = int.Parse(opts.GetValueOrDefault("count", "10000"));
    int payloadSize = int.Parse(opts.GetValueOrDefault("size", "256"));
    int batchSize = int.Parse(opts.GetValueOrDefault("batch", "100"));
    string topic = opts.GetValueOrDefault("topic", "bench-topic");

    await using var client = await OpenClientAsync(opts);
    await client.Admin.CreateTopicAsync(topic, partitions: 1);

    byte[] payload = new byte[payloadSize];
    Random.Shared.NextBytes(payload);

    Console.WriteLine($"Running benchmark: Messages={count:N0}, Payload={payloadSize}B, Concurrency={batchSize}...");
    var sw = Stopwatch.StartNew();

    int sent = 0;
    while (sent < count)
    {
        int chunk = Math.Min(batchSize, count - sent);
        var tasks = new Task[chunk];
        for (int i = 0; i < chunk; i++)
        {
            tasks[i] = client.Producer.PublishAsync(topic, payload, durability: Durability.Memory).AsTask();
        }
        await Task.WhenAll(tasks);
        sent += chunk;
    }

    sw.Stop();
    double rps = count / sw.Elapsed.TotalSeconds;
    double mbps = (count * (double)payloadSize) / (1024 * 1024) / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"Completed {count:N0} msgs in {sw.Elapsed.TotalSeconds:F2}s -> {rps:N0} msg/s ({mbps:F2} MB/s)");
}

static async Task<IHybridMqInstance> OpenClientAsync(Dictionary<string, string> opts)
{
    if (opts.ContainsKey("host"))
    {
        var ep = GetTcpEndPoint(opts);
        var nodeId = new NodeId(opts.GetValueOrDefault("node", "broker-primary"));
        return HybridMqFactory.ConnectRemoteTcp(ep, nodeId);
    }

    string storageStr = opts.GetValueOrDefault("storage", "file").ToLowerInvariant();
    return await HybridMqFactory.CreateEmbeddedAsync(o =>
    {
        o.StorageType = storageStr switch
        {
            "postgres" or "postgresql" => StorageBackendType.PostgreSql,
            "mongo" or "mongodb" => StorageBackendType.MongoDb,
            _ => StorageBackendType.File
        };
        o.DataDirectory = opts.GetValueOrDefault("data-dir", "./data/hybridmq");
        o.ConnectionString = opts.GetValueOrDefault("conn");
        o.Profile = ResourceProfile.Low;
    });
}

static IStorageProvider OpenDirectStorage(Dictionary<string, string> opts)
{
    string storageStr = opts.GetValueOrDefault("storage", "file").ToLowerInvariant();
    var mqOpts = new HybridMqOptions
    {
        StorageType = storageStr switch
        {
            "postgres" or "postgresql" => StorageBackendType.PostgreSql,
            "mongo" or "mongodb" => StorageBackendType.MongoDb,
            _ => StorageBackendType.File
        },
        DataDirectory = opts.GetValueOrDefault("data-dir", "./data/hybridmq"),
        ConnectionString = opts.GetValueOrDefault("conn")
    };
    return HybridMqFactory.CreateStorageProvider(mqOpts);
}

static IPEndPoint GetTcpEndPoint(Dictionary<string, string> opts)
{
    string host = opts.GetValueOrDefault("host", "127.0.0.1");
    int port = int.Parse(opts.GetValueOrDefault("port", "5679"));
    return new IPEndPoint(IPAddress.Parse(host), port);
}

static string Require(Dictionary<string, string> opts, string key)
{
    if (!opts.TryGetValue(key, out var val) || string.IsNullOrWhiteSpace(val))
        throw new ArgumentException($"Missing required option: --{key}");
    return val;
}

static string TryFormatUtf8(ReadOnlySpan<byte> data)
{
    if (data.IsEmpty) return "<empty>";
    string text = Encoding.UTF8.GetString(data);
    return text.Any(c => char.IsControl(c) && c is not ('\r' or '\n' or '\t'))
        ? $"<binary {data.Length}B: {Convert.ToHexString(data[..Math.Min(16, data.Length)])}...>"
        : text;
}

static Dictionary<string, string> ParseArgs(string[] args)
{
    var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (int i = 0; i < args.Length; i++)
    {
        string arg = args[i];
        if (arg.StartsWith("--"))
        {
            string key = arg[2..];
            int eqIdx = key.IndexOf('=');
            if (eqIdx > 0)
            {
                dict[key[..eqIdx]] = key[(eqIdx + 1)..];
            }
            else if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                dict[key] = args[++i];
            }
            else
            {
                dict[key] = "true";
            }
        }
    }
    return dict;
}

static void PrintHelp()
{
    Console.WriteLine("""
HybridMQ Admin & Diagnostics CLI (hmq-cli)

Connection Options (Remote TCP or Direct Storage):
  --host <ip> --port <port>              Connect to running TCP Broker (default port: 5679)
  --storage <file|postgres|mongo>        Open storage directly in offline/embedded mode
  --data-dir <path>                      Directory path for File storage (default: ./data/hybridmq)
  --conn <connection-string>             Connection string for PostgreSQL / MongoDB

Commands:
  ping                                   Check TCP broker connectivity and RTT latency
  topics                                 List all topics, partition counts, and high watermarks
  topic-create --topic <name> [--partitions <n>]
  bind --exchange <ex> --type <Direct|Fanout|Topic|Headers> --pattern <pat> --topic <target>
  produce --topic <name> --message <text> [--key <k>] [--count <n>] [--delay-ms <ms>]
  tail --topic <name> [--partition <p>] [--from <offset>] [--limit <n>]
  dlq-inspect --topic <name> --group <grp> [--partition <p>] [--limit <n>]
  dlq-replay --topic <name> --group <grp> [--from <offset>] [--limit <n>]
  inspect-segment --file <path.log>      Inspect binary WAL segment records & verify CRC32 integrity
  recover-tail --data-dir <path> --topic <name> [--partition <p>]
  bench [--count <n>] [--size <bytes>] [--batch <n>]
""");
}
