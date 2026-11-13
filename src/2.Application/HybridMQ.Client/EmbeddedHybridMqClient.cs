namespace HybridMQ.Client;

using System.Text;
using System.Text.Json.Serialization.Metadata;
using HybridMQ.Client.Abstractions;
using HybridMQ.Client.Serialization;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Reliability;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using HybridMQ.Engine.Pipelines;

public sealed class EmbeddedHybridMqInstance : IHybridMqInstance, IProducer, IConsumer, IAdminClient
{
    private readonly BrokerEngine _engine;
    private readonly IMessageSerializer _serializer;
    private readonly long _producerId = Random.Shared.NextInt64(1, long.MaxValue);
    private int _sequence;

    public IProducer Producer => this;
    public IConsumer Consumer => this;
    public IAdminClient Admin => this;

    public EmbeddedHybridMqInstance(BrokerEngine engine, IMessageSerializer? serializer = null)
    {
        _engine = engine;
        _serializer = serializer ?? AotJsonMessageSerializer.Default;
    }

    public ValueTask CreateTopicAsync(string topic, int partitions = 1, RetentionPolicy? retention = null, CancellationToken ct = default)
        => _engine.CreateTopicAsync(topic, partitions, retention, ct);

    public ValueTask AddBindingAsync(string exchange, ExchangeType type, string pattern, string targetTopic, CancellationToken ct = default)
        => _engine.AddBindingAsync(exchange, type, pattern, targetTopic, ct);

    public ValueTask<IReadOnlyList<AppendResult>> PublishAsync<T>(
        string topic,
        T message,
        JsonTypeInfo<T>? typeInfo = null,
        string? key = null,
        string? routingKey = null,
        string? exchange = null,
        Durability durability = Durability.DiskSync,
        TimeSpan? delay = null,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        var payload = _serializer.Serialize(message, typeInfo);
        var keyBytes = string.IsNullOrEmpty(key) ? ReadOnlyMemory<byte>.Empty : Encoding.UTF8.GetBytes(key);
        int seq = Interlocked.Increment(ref _sequence);

        var env = new PublishEnvelope(
            new TopicName(topic),
            routingKey,
            exchange,
            keyBytes,
            payload,
            Durability: durability,
            ProducerId: _producerId,
            Sequence: seq,
            Delay: delay,
            Ttl: ttl);

        return _engine.PublishAsync(env, ct);
    }

    public IAsyncEnumerable<Record> ReadStreamAsync(string topic, int partition, Offset from, int maxCount = 100, CancellationToken ct = default)
        => _engine.ReadStreamAsync(topic, partition, from, maxCount, ct);

    public ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        string topic,
        string group,
        SubscribeOptions options,
        Func<IMessageContext, T, ValueTask> handler,
        JsonTypeInfo<T>? typeInfo = null,
        CancellationToken ct = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var subId = new SubscriptionId(group, new TopicName(topic), options.Partition);
        var retryPolicy = new RetryPolicy(MaxAttempts: options.MaxAttempts);

        var workerTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var batch = await _engine.PollWorkQueueAsync(
                    subId,
                    options.PrefetchCount,
                    options.AckTimeout,
                    options.OrderedByKey,
                    cts.Token).ConfigureAwait(false);

                if (batch.Count == 0)
                {
                    await Task.Delay(25, cts.Token).ConfigureAwait(false);
                    continue;
                }

                foreach (var rec in batch)
                {
                    var ctx = new MessageContext(_engine, subId, rec, retryPolicy);
                    try
                    {
                        var obj = _serializer.Deserialize(rec.Payload, typeInfo)!;
                        await handler(ctx, obj).ConfigureAwait(false);
                        if (!ctx.IsSettled)
                            await ctx.AckAsync(cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!cts.Token.IsCancellationRequested)
                    {
                        if (!ctx.IsSettled)
                            await ctx.NackAsync(reason: ex.Message, ct: cts.Token).ConfigureAwait(false);
                    }
                }
            }
        }, cts.Token);

        return ValueTask.FromResult<IAsyncDisposable>(new SubscriptionHandle(cts, workerTask));
    }

    public ValueTask DisposeAsync() => _engine.DisposeAsync();

    private sealed class MessageContext(BrokerEngine engine, SubscriptionId subId, Record record, RetryPolicy retryPolicy) : IMessageContext
    {
        public bool IsSettled { get; private set; }
        public Offset Offset => record.Offset;
        public int Partition => subId.Partition;
        public ReadOnlyMemory<byte> Key => record.Key;
        public IReadOnlyList<MessageHeader> Headers => record.Headers;

        public async ValueTask AckAsync(CancellationToken ct = default)
        {
            if (IsSettled) return;
            IsSettled = true;
            await engine.AckAsync(subId, record.Offset, ct).ConfigureAwait(false);
        }

        public async ValueTask NackAsync(TimeSpan? delay = null, string? reason = null, CancellationToken ct = default)
        {
            if (IsSettled) return;
            IsSettled = true;
            await engine.NackAsync(subId, record.Offset, retryPolicy, reason, ct).ConfigureAwait(false);
        }
    }

    private sealed class SubscriptionHandle(CancellationTokenSource cts, Task worker) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try { await worker.ConfigureAwait(false); } catch (OperationCanceledException) { }
            cts.Dispose();
        }
    }
}
