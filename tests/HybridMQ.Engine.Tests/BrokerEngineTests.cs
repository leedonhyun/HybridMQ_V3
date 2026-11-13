namespace HybridMQ.Engine.Tests;

using System.Text;
using System.Text.Json.Serialization;
using HybridMQ.Client;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Reliability;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using HybridMQ.Storage.File;
using FluentAssertions;
using Xunit;

public sealed record OrderCreatedEvent(string OrderId, decimal Amount);

[JsonSerializable(typeof(OrderCreatedEvent))]
internal partial class TestJsonContext : JsonSerializerContext;

public sealed class BrokerEngineTests : IAsyncDisposable
{
    private readonly string _tempDir;
    private readonly FakeTimeProvider _clock;
    private readonly FileStorageProvider _storage;
    private readonly BrokerEngine _engine;
    private readonly EmbeddedHybridMqInstance _client;

    public BrokerEngineTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"hmq_test_{Guid.NewGuid():N}");
        _clock = new FakeTimeProvider(new DateTimeOffset(2026, 11, 13, 9, 0, 0, TimeSpan.Zero));
        _storage = new FileStorageProvider(_tempDir);
        _engine = new BrokerEngine(
            NodeId.NewId(),
            _storage,
            new MemoryBudgetManager(ResourceProfile.Low),
            timeProvider: _clock);
        _client = new EmbeddedHybridMqInstance(_engine);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task AotJson_Publish_And_PushSubscribe_EndToEnd()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("orders", partitions: 1);

        var received = new List<OrderCreatedEvent>();
        var tcs = new TaskCompletionSource();

        await using var sub = await _client.Consumer.SubscribeAsync(
            "orders",
            "billing-service",
            new SubscribeOptions { PrefetchCount = 10 },
            async (ctx, evt) =>
            {
                received.Add(evt);
                if (received.Count == 2) tcs.TrySetResult();
                await ValueTask.CompletedTask;
            },
            TestJsonContext.Default.OrderCreatedEvent);

        await _client.Producer.PublishAsync("orders", new OrderCreatedEvent("ORD-1", 15000m), TestJsonContext.Default.OrderCreatedEvent);
        await _client.Producer.PublishAsync("orders", new OrderCreatedEvent("ORD-2", 32000m), TestJsonContext.Default.OrderCreatedEvent);

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        received.Should().HaveCount(2);
        received[0].OrderId.Should().Be("ORD-1");
        received[1].Amount.Should().Be(32000m);
    }

    [Fact]
    public async Task Exchange_TopicRouting_FansOut_To_Matching_Queues()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("audit-all", 1);
        await _client.Admin.CreateTopicAsync("kr-payments", 1);

        await _client.Admin.AddBindingAsync("events-ex", ExchangeType.Topic, "payment.#", "audit-all");
        await _client.Admin.AddBindingAsync("events-ex", ExchangeType.Topic, "payment.*.kr", "kr-payments");

        var results = await _client.Producer.PublishAsync(
            topic: "",
            message: new byte[] { 10, 20 },
            routingKey: "payment.card.kr",
            exchange: "events-ex");

        results.Should().HaveCount(2);

        var auditMsgs = await _engine.PollWorkQueueAsync(new SubscriptionId("g1", new TopicName("audit-all"), 0), 10, TimeSpan.FromSeconds(10));
        var krMsgs = await _engine.PollWorkQueueAsync(new SubscriptionId("g2", new TopicName("kr-payments"), 0), 10, TimeSpan.FromSeconds(10));

        auditMsgs.Should().HaveCount(1);
        krMsgs.Should().HaveCount(1);
    }

    [Fact]
    public async Task Delayed_And_Ttl_Messages_Respect_VirtualClock()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("scheduled", 1);

        // 1. 10초 뒤 전달되는 지연 메시지
        await _client.Producer.PublishAsync("scheduled", new byte[] { 1 }, delay: TimeSpan.FromSeconds(10));
        // 2. 유효기간(TTL) 5초짜리 메시지
        await _client.Producer.PublishAsync("scheduled", new byte[] { 2 }, ttl: TimeSpan.FromSeconds(5));

        var subId = new SubscriptionId("sched-group", new TopicName("scheduled"), 0);

        // T=0: 지연 메시지는 보류되고, TTL 메시지는 유효하므로 즉시 수신됨
        var batch0 = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(30));
        batch0.Should().HaveCount(1);
        batch0[0].Payload.ToArray().Should().BeEquivalentTo(new byte[] { 2 });
        await _engine.AckAsync(subId, batch0[0].Offset);

        // T=+6초: 아직 지연 메시지(10초)는 도달 전
        _clock.Advance(TimeSpan.FromSeconds(6));
        var batch6 = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(30));
        batch6.Should().BeEmpty();

        // T=+11초: 지연 메시지 전달 시점 도래
        _clock.Advance(TimeSpan.FromSeconds(5));
        var batch11 = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(30));
        batch11.Should().HaveCount(1);
        batch11[0].Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1 });
    }

    [Fact]
    public async Task VisibilityTimeout_Redelivers_Unacked_Message_And_DeadLetters_On_MaxAttempts()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("tasks", 1);
        await _client.Producer.PublishAsync("tasks", new byte[] { 77 });

        var subId = new SubscriptionId("workers", new TopicName("tasks"), 0);

        // 1차 수신 후 Ack 없이 타임아웃 경과
        var firstPoll = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(5));
        firstPoll.Should().HaveCount(1);

        _clock.Advance(TimeSpan.FromSeconds(6));

        // 2차 재전달 수신 후 Nack -> MaxAttempts=1 초과로 DLQ 이동
        var secondPoll = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(5));
        secondPoll.Should().HaveCount(1);

        await _engine.NackAsync(subId, secondPoll[0].Offset, new RetryPolicy(MaxAttempts: 1), "WorkerCrash");

        var dlqRecords = new List<Record>();
        await foreach (var rec in _client.Consumer.ReadStreamAsync("tasks.workers.dlq", 0, Offset.Earliest))
            dlqRecords.Add(rec);

        dlqRecords.Should().HaveCount(1);
        dlqRecords[0].Flags.HasFlag(RecordFlags.IsDeadLetter).Should().BeTrue();
    }

    [Fact]
    public async Task OrderedByKey_Blocks_SameKey_While_Allowing_DifferentKeys()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("keyed-jobs", 1);

        await _client.Producer.PublishAsync("keyed-jobs", Encoding.UTF8.GetBytes("A1"), key: "user-A");
        await _client.Producer.PublishAsync("keyed-jobs", Encoding.UTF8.GetBytes("A2"), key: "user-A");
        await _client.Producer.PublishAsync("keyed-jobs", Encoding.UTF8.GetBytes("B1"), key: "user-B");

        var subId = new SubscriptionId("ordered-workers", new TopicName("keyed-jobs"), 0);

        // OrderedByKey=true: user-A의 첫 메시지(A1)와 user-B의 첫 메시지(B1)만 동시 전달, A2는 보류
        var batch1 = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(30), orderedByKey: true);
        batch1.Select(r => Encoding.UTF8.GetString(r.Payload.Span)).Should().BeEquivalentTo(["A1", "B1"]);

        // A1 Ack 완료 후 A2가 잠금 해제되어 전달됨
        var a1Offset = batch1.First(r => Encoding.UTF8.GetString(r.Payload.Span) == "A1").Offset;
        await _engine.AckAsync(subId, a1Offset);

        var batch2 = await _engine.PollWorkQueueAsync(subId, 10, TimeSpan.FromSeconds(30), orderedByKey: true);
        batch2.Select(r => Encoding.UTF8.GetString(r.Payload.Span)).Should().BeEquivalentTo(["A2"]);
    }

    [Fact]
    public async Task Duplicate_Producer_Sequence_Is_Rejected_Idempotently()
    {
        await _engine.StartAsync();
        await _client.Admin.CreateTopicAsync("dedup-topic", 1);

        var env = new PublishEnvelope(
            new TopicName("dedup-topic"),
            null,
            null,
            ReadOnlyMemory<byte>.Empty,
            new byte[] { 1, 2, 3 },
            ProducerId: 999,
            Sequence: 1);

        var res1 = await _engine.PublishAsync(env);
        var res2 = await _engine.PublishAsync(env);

        res1[0].IsDuplicate.Should().BeFalse();
        res2[0].IsDuplicate.Should().BeTrue();

        var hw = await _storage.GetHighWatermarkAsync(new PartitionId(new TopicName("dedup-topic"), 0));
        hw.Value.Should().Be(1);
    }
}
