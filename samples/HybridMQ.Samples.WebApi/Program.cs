using System.Text.Json.Serialization;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;

var builder = WebApplication.CreateSlimBuilder(args);

// NativeAOT 호환 JSON 직렬화 컨텍스트 등록
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, WebApiJsonContext.Default);
});

// HybridMQ 내장 브로커 DI 등록
builder.Services.AddHybridMq(opts =>
{
    opts.DataDirectory = builder.Configuration["HybridMQ:DataDirectory"] ?? "./data/webapi-mq";
    opts.Profile = ResourceProfile.Standard;
    opts.OverrideMaxMemoryMb = 128;
});

// 백그라운드 컨슈머 워커 등록
builder.Services.AddHostedService<OrderProcessingWorker>();

var app = builder.Build();

// 앱 시작 시 기본 토픽 생성
var admin = app.Services.GetRequiredService<IAdminClient>();
await admin.CreateTopicAsync("web-orders", partitions: 1);

// 1. 주문 발행 엔드포인트 (Producer)
app.MapPost("/api/orders", async (CreateOrderRequest req, IProducer producer, CancellationToken ct) =>
{
    var evt = new WebOrderEvent(Guid.NewGuid().ToString("N"), req.CustomerId, req.ItemName, req.Quantity, DateTimeOffset.UtcNow);
    TimeSpan? delay = req.DelaySeconds > 0 ? TimeSpan.FromSeconds(req.DelaySeconds) : null;

    var results = await producer.PublishAsync(
        topic: "web-orders",
        message: evt,
        typeInfo: WebApiJsonContext.Default.WebOrderEvent,
        key: req.CustomerId,
        delay: delay,
        ct: ct);

    return Results.Accepted($"/api/orders/{evt.OrderId}", new PublishResponse(evt.OrderId, results[0].BaseOffset.Value, delay is not null));
});

// 2. 토픽 스트림 조회 엔드포인트 (Stream Reader)
app.MapGet("/api/orders/stream", async (IConsumer consumer, long fromOffset = 0, int limit = 50, CancellationToken ct = default) =>
{
    var items = new List<StreamRecordDto>();
    await foreach (var rec in consumer.ReadStreamAsync("web-orders", partition: 0, new Offset(fromOffset), limit, ct))
    {
        items.Add(new StreamRecordDto(
            rec.Offset.Value,
            rec.TimestampUnixMs,
            System.Text.Encoding.UTF8.GetString(rec.Payload.Span)));
    }
    return Results.Ok(items);
});

// 3. DLQ(Dead Letter Queue) 조회 엔드포인트
app.MapGet("/api/orders/dlq", async (IConsumer consumer, int limit = 50, CancellationToken ct = default) =>
{
    var deadLetters = new List<StreamRecordDto>();
    await foreach (var rec in consumer.ReadStreamAsync("web-orders.web-order-workers.dlq", partition: 0, Offset.Earliest, limit, ct))
    {
        deadLetters.Add(new StreamRecordDto(
            rec.Offset.Value,
            rec.TimestampUnixMs,
            System.Text.Encoding.UTF8.GetString(rec.Payload.Span)));
    }
    return Results.Ok(deadLetters);
});

app.Run();

// 백그라운드 구독 워커
public sealed class OrderProcessingWorker(IConsumer consumer, ILogger<OrderProcessingWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // 토픽 초기화 대기
        await Task.Delay(200, stoppingToken);

        await using var sub = await consumer.SubscribeAsync(
            topic: "web-orders",
            group: "web-order-workers",
            options: new SubscribeOptions
            {
                PrefetchCount = 16,
                AckTimeout = TimeSpan.FromSeconds(30),
                OrderedByKey = true,
                MaxAttempts = 3
            },
            handler: async (ctx, order) =>
            {
                logger.LogInformation(
                    "Processed Order {OrderId} for Customer {CustomerId} (Item: {Item}, Qty: {Qty}) at Offset {Offset}",
                    order.OrderId, order.CustomerId, order.ItemName, order.Quantity, ctx.Offset);

                if (order.ItemName.Equals("FAIL_TEST", StringComparison.OrdinalIgnoreCase))
                {
                    await ctx.NackAsync(reason: "SimulatedFailureForDlqTest", ct: stoppingToken);
                    return;
                }

                await ctx.AckAsync(stoppingToken);
            },
            typeInfo: WebApiJsonContext.Default.WebOrderEvent,
            ct: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

public sealed record CreateOrderRequest(string CustomerId, string ItemName, int Quantity, int DelaySeconds = 0);
public sealed record WebOrderEvent(string OrderId, string CustomerId, string ItemName, int Quantity, DateTimeOffset CreatedAt);
public sealed record PublishResponse(string OrderId, long AssignedOffset, bool IsDelayed);
public sealed record StreamRecordDto(long Offset, long TimestampUnixMs, string RawJson);

[JsonSerializable(typeof(CreateOrderRequest))]
[JsonSerializable(typeof(WebOrderEvent))]
[JsonSerializable(typeof(PublishResponse))]
[JsonSerializable(typeof(StreamRecordDto))]
[JsonSerializable(typeof(List<StreamRecordDto>))]
internal partial class WebApiJsonContext : JsonSerializerContext;
