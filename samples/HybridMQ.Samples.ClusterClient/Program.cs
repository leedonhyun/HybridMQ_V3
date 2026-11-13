using System.Net;
using System.Text.Json.Serialization;

using HybridMQ.Client;
using HybridMQ.Client.Abstractions;

// 1. 클러스터 엔드포인트 목록 정의
var clusterEndPoints = new[]
{
    new IPEndPoint(IPAddress.Loopback, 5679),
    new IPEndPoint(IPAddress.Loopback, 5680),
    new IPEndPoint(IPAddress.Loopback, 5681)
};

// 2. 살아있는 브로커 노드에 연결 (Failover 헬퍼)
await using var client = await ConnectToAvailableBrokerAsync(clusterEndPoints);

// 3. 토픽 생성 (파티션 3개로 분산)
const string topicName = "cluster-orders";
await client.Admin.CreateTopicAsync(topicName, partitions: 3);
Console.WriteLine($"Topic '{topicName}' created.");

// 4. 컨슈머 구독 시작
var receivedCount = 0;
var allReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

await using var subscription = await client.Consumer.SubscribeAsync(
    topicName,
    "order-processor-group",
    new SubscribeOptions
    {
        PrefetchCount = 5,
        AckTimeout = TimeSpan.FromSeconds(10),
        MaxAttempts = 3
    },
    async (ctx, order) =>
    {
        int current = Interlocked.Increment(ref receivedCount);
        Console.WriteLine($"[Consumed #{current}] Partition={ctx.Partition}, Offset={ctx.Offset}, OrderId={order.OrderId}, Amount={order.Amount:N0}원");

        if (current >= 5)
            allReceivedTcs.TrySetResult();

        await ValueTask.CompletedTask;
    },
    AppJsonContext.Default.OrderCreatedEvent);

// 5. 프로듀서 메시지 5건 발행 (Key를 지정해 파티션 분산 및 순서 보장)
for (int i = 1; i <= 5; i++)
{
    string orderId = $"ORD-2026-{i:D3}";
    var evt = new OrderCreatedEvent(orderId, i * 10_000m);

    var results = await client.Producer.PublishAsync(
        topicName,
        evt,
        AppJsonContext.Default.OrderCreatedEvent,
        key: $"customer-{i % 3}"); // 고객 키 기준으로 파티션 라우팅

    foreach (var r in results)
    {
        Console.WriteLine($"[Published] {orderId} -> Partition={r.PartitionId.Partition}, Offset={r.BaseOffset}");
    }
}

// 6. 모든 메시지 소비 완료 대기
await allReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
Console.WriteLine("All cluster messages processed successfully!");

// --- 헬퍼 메서드 및 모델 정의 ---

static async Task<IHybridMqInstance> ConnectToAvailableBrokerAsync(IEnumerable<IPEndPoint> endpoints)
{
    Exception? lastEx = null;

    foreach (var ep in endpoints)
    {
        try
        {
            Console.WriteLine($"Connecting to broker {ep}...");
            var instance = HybridMqFactory.ConnectRemoteTcp(ep);

            // 실제 통신 가능 여부 헬스체크 (연결이 지연 초기화 방식일 경우 대비)
            await instance.Admin.CreateTopicAsync("__healthcheck", partitions: 1);
            Console.WriteLine($"Connected to active broker: {ep}");
            return instance;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to connect to {ep}: {ex.Message}");
            lastEx = ex;
        }
    }

    throw new InvalidOperationException("No available broker nodes in the cluster.", lastEx);
}

public sealed record OrderCreatedEvent(string OrderId, decimal Amount);

[JsonSerializable(typeof(OrderCreatedEvent))]
internal partial class AppJsonContext : JsonSerializerContext;
