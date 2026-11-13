using System.Text;
using System.Text.Json.Serialization;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Hosting;

Console.WriteLine("=== HybridMQ Console Client Interactive & Scenario Demo ===");

string dataDir = Path.Combine(Path.GetTempPath(), $"hybridmq_console_demo_{Guid.NewGuid():N}");

await using var mq = await HybridMqFactory.CreateEmbeddedAsync(opts =>
{
    opts.DataDirectory = dataDir;
    opts.Profile = ResourceProfile.Low; // 32MB 경량 프로파일
});

// 1. 토픽 및 Exchange 바인딩 구성
await mq.Admin.CreateTopicAsync("orders.created", partitions: 2);
await mq.Admin.CreateTopicAsync("audit.logs", partitions: 1);
await mq.Admin.AddBindingAsync("commerce.events", ExchangeType.Topic, "order.#", "audit.logs");
await mq.Admin.AddBindingAsync("commerce.events", ExchangeType.Direct, "order.created", "orders.created");

Console.WriteLine("[1] Topics & Topic/Direct Exchange bindings configured.");

// 2. Push 방식 WorkQueue 구독자 시작 (NativeAOT JsonSerializerContext 사용)
var receivedCount = 0;
var allReceivedTcs = new TaskCompletionSource();

await using var subscription = await mq.Consumer.SubscribeAsync(
    topic: "orders.created",
    group: "payment-worker-group",
    options: new SubscribeOptions
    {
        PrefetchCount = 10,
        AckTimeout = TimeSpan.FromSeconds(15),
        OrderedByKey = true, // 동일 CustomerId 기준 순차 처리 보장
        MaxAttempts = 3,
        Partition = 0
    },
    handler: async (ctx, order) =>
    {
        Console.WriteLine($"  -> [Consumer] Offset={ctx.Offset} | Order={order.OrderId} | Customer={order.CustomerId} | Amount={order.Amount:N0} KRW");

        if (order.SimulateTransientError && Interlocked.Increment(ref order.AttemptCounter) == 1)
        {
            Console.WriteLine($"  !! [Consumer] Transient error on {order.OrderId}, triggering Nack/Retry...");
            await ctx.NackAsync(reason: "SimulatedPaymentGatewayTimeout");
            return;
        }

        await ctx.AckAsync();
        if (Interlocked.Increment(ref receivedCount) >= 3)
            allReceivedTcs.TrySetResult();
    },
    typeInfo: SampleJsonContext.Default.OrderDto);

// 3. 메시지 발행 (Exchange 라우팅 + 지연 메시지 포함)
Console.WriteLine("[2] Publishing sample orders via Exchange...");
var orders = new[]
{
    new OrderDto("ORD-1001", "CUST-A", 45000m),
    new OrderDto("ORD-1002", "CUST-A", 12000m),
    new OrderDto("ORD-1003", "CUST-B", 89000m)
};

foreach (var ord in orders)
{
    await mq.Producer.PublishAsync(
        topic: "",
        message: ord,
        typeInfo: SampleJsonContext.Default.OrderDto,
        key: ord.CustomerId,
        routingKey: "order.created",
        exchange: "commerce.events");
}

// 4. 지연 메시지(Delayed Message) 발행 예시 (1.5초 후 전달)
await mq.Producer.PublishAsync(
    topic: "audit.logs",
    message: Encoding.UTF8.GetBytes("Delayed Audit Checkpoint"),
    delay: TimeSpan.FromMilliseconds(1500));

await allReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
Console.WriteLine("[3] All primary orders processed and acknowledged!");

// 5. Stream 방식(Offset 직접 읽기)으로 Audit Log 조회
Console.WriteLine("[4] Reading audit.logs stream from Offset 0:");
await Task.Delay(1700); // 지연 메시지 도래 대기
await foreach (var record in mq.Consumer.ReadStreamAsync("audit.logs", partition: 0, from: Offset.Earliest, maxCount: 20))
{
    Console.WriteLine($"  -> [AuditStream] Offset={record.Offset} | Ts={record.TimestampUnixMs} | Bytes={record.Payload.Length}");
}

Console.WriteLine("=== Demo Completed Successfully ===");

if (Directory.Exists(dataDir))
    Directory.Delete(dataDir, recursive: true);

public sealed record OrderDto(string OrderId, string CustomerId, decimal Amount, bool SimulateTransientError = false)
{
    [JsonIgnore]
    public int AttemptCounter;
}

[JsonSerializable(typeof(OrderDto))]
internal partial class SampleJsonContext : JsonSerializerContext;
