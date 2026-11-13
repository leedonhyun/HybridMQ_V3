namespace HybridMQ.Client.Abstractions;

using System.Text.Json.Serialization.Metadata;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Topics;

public sealed record SubscribeOptions
{
    public int MaxAttempts { get; init; } = 5;
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public int PrefetchCount { get; init; } = 32;
    public bool OrderedByKey { get; init; } = false;
    public int Partition { get; init; } = 0;
}

public interface IMessageContext
{
    Offset Offset { get; }
    int Partition { get; }
    ReadOnlyMemory<byte> Key { get; }
    IReadOnlyList<MessageHeader> Headers { get; }
    ValueTask AckAsync(CancellationToken ct = default);
    ValueTask NackAsync(TimeSpan? delay = null, string? reason = null, CancellationToken ct = default);
}

public interface IProducer
{
    ValueTask<IReadOnlyList<AppendResult>> PublishAsync<T>(
        string topic,
        T message,
        JsonTypeInfo<T>? typeInfo = null,
        string? key = null,
        string? routingKey = null,
        string? exchange = null,
        Durability durability = Durability.DiskSync,
        TimeSpan? delay = null,
        TimeSpan? ttl = null,
        CancellationToken ct = default);
}

public interface IConsumer
{
    ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        string topic,
        string group,
        SubscribeOptions options,
        Func<IMessageContext, T, ValueTask> handler,
        JsonTypeInfo<T>? typeInfo = null,
        CancellationToken ct = default);

    IAsyncEnumerable<Record> ReadStreamAsync(
        string topic,
        int partition,
        Offset from,
        int maxCount = 100,
        CancellationToken ct = default);
}

public interface IAdminClient
{
    ValueTask CreateTopicAsync(string topic, int partitions = 1, RetentionPolicy? retention = null, CancellationToken ct = default);
    ValueTask AddBindingAsync(string exchange, ExchangeType type, string pattern, string targetTopic, CancellationToken ct = default);
}

public interface IHybridMqInstance : IAsyncDisposable
{
    IProducer Producer { get; }
    IConsumer Consumer { get; }
    IAdminClient Admin { get; }
}
