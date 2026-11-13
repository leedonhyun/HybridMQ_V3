namespace HybridMQ.Domain.Ports;

using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;

public interface IStorageProvider : IAsyncDisposable
{
    ValueTask InitializeAsync(CancellationToken ct = default);

    ValueTask CreateTopicAsync(TopicMetadata metadata, CancellationToken ct = default);
    ValueTask<TopicMetadata?> GetTopicAsync(TopicName topic, CancellationToken ct = default);
    ValueTask<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken ct = default);
    ValueTask AddBindingAsync(BindingRule binding, CancellationToken ct = default);
    ValueTask<IReadOnlyList<BindingRule>> GetBindingsAsync(string exchangeName, CancellationToken ct = default);

    ValueTask<AppendResult> AppendAsync(
        PartitionId partition,
        ReadOnlyMemory<Record> batch,
        Durability durability,
        FencingToken? fencingToken = null,
        CancellationToken ct = default);

    IAsyncEnumerable<Record> ReadAsync(
        PartitionId partition,
        Offset fromOffset,
        int maxCount,
        CancellationToken ct = default);

    ValueTask<Offset> GetHighWatermarkAsync(PartitionId partition, CancellationToken ct = default);

    ValueTask<SubscriptionState> LoadStateAsync(SubscriptionId subscription, CancellationToken ct = default);
    ValueTask CommitStateAsync(SubscriptionId subscription, StateChange change, CancellationToken ct = default);

    ValueTask<LeaseResult> TryAcquireLeaseAsync(PartitionId partition, NodeId node, TimeSpan ttl, CancellationToken ct = default);
    ValueTask ReleaseLeaseAsync(PartitionId partition, NodeId node, CancellationToken ct = default);

    ValueTask<int> RecoverAndTruncateCorruptTailAsync(PartitionId partition, CancellationToken ct = default);
}

public interface IPeerMembershipProvider : IAsyncDisposable
{
    NodeId LocalNodeId { get; }
    event Action<NodeId>? OnPeerDied;
    ValueTask RegisterSelfAsync(CancellationToken ct = default);
    ValueTask<IReadOnlyList<NodeId>> GetActivePeersAsync(CancellationToken ct = default);
}

public interface ITransportChannel : IAsyncDisposable
{
    ValueTask SendAsync(NodeId targetNode, ReadOnlyMemory<byte> frame, CancellationToken ct = default);
    ValueTask<ReadOnlyMemory<byte>> RequestAsync(NodeId targetNode, ReadOnlyMemory<byte> requestFrame, CancellationToken ct = default);
}
