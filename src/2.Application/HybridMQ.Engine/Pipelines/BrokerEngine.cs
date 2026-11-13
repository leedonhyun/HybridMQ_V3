namespace HybridMQ.Engine.Pipelines;

using System.Collections.Concurrent;
using System.IO.Hashing;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Reliability;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using HybridMQ.Engine.Diagnostics;
using HybridMQ.Engine.Memory;

public sealed class BrokerEngine : IAsyncDisposable
{
    private readonly IStorageProvider _storage;
    private readonly IPeerMembershipProvider? _membership;
    private readonly MemoryBudgetManager _memoryBudget;
    private readonly IFaultInjectionHooks _faultHooks;
    private readonly TimeProvider _timeProvider;
    private readonly IdempotencyWindow _producerDedup = new();
    private readonly ConcurrentDictionary<SubscriptionId, SubscriptionState> _subStates = new();
    private readonly ConcurrentDictionary<PartitionId, FencingToken> _ownedPartitions = new();

    public NodeId NodeId { get; }

    public BrokerEngine(
        NodeId nodeId,
        IStorageProvider storage,
        MemoryBudgetManager memoryBudget,
        IPeerMembershipProvider? membership = null,
        IFaultInjectionHooks? faultHooks = null,
        TimeProvider? timeProvider = null)
    {
        NodeId = nodeId;
        _storage = storage;
        _memoryBudget = memoryBudget;
        _membership = membership;
        _faultHooks = faultHooks ?? NoOpFaultInjectionHooks.Instance;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_membership is not null)
        {
            _membership.OnPeerDied += HandlePeerDied;
        }
    }

    private long NowUnixMs => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    public async ValueTask StartAsync(CancellationToken ct = default)
    {
        await _storage.InitializeAsync(ct).ConfigureAwait(false);
        if (_membership is not null)
        {
            await _membership.RegisterSelfAsync(ct).ConfigureAwait(false);
        }
    }

    public ValueTask CreateTopicAsync(string name, int partitions = 1, RetentionPolicy? retention = null, CancellationToken ct = default)
    {
        var meta = new TopicMetadata(
            new TopicName(name),
            Math.Max(1, partitions),
            retention ?? new RetentionPolicy(),
            NowUnixMs);
        return _storage.CreateTopicAsync(meta, ct);
    }

    public ValueTask<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken ct = default)
        => _storage.ListTopicsAsync(ct);

    public ValueTask<Offset> GetHighWatermarkAsync(string topic, int partition, CancellationToken ct = default)
        => _storage.GetHighWatermarkAsync(new PartitionId(new TopicName(topic), partition), ct);

    public ValueTask AddBindingAsync(string exchange, ExchangeType type, string pattern, string targetTopic, CancellationToken ct = default)
    {
        return _storage.AddBindingAsync(new BindingRule(exchange, type, pattern, new TopicName(targetTopic)), ct);
    }

    public void RegisterPartitionLease(PartitionId partition, FencingToken token)
        => _ownedPartitions[partition] = token;

    public void RevokePartitionLease(PartitionId partition)
        => _ownedPartitions.TryRemove(partition, out _);

    public bool IsPartitionLeader(PartitionId partition)
        => _ownedPartitions.ContainsKey(partition);

    public IReadOnlyCollection<PartitionId> OwnedPartitions => _ownedPartitions.Keys.ToArray();

    public async ValueTask<IReadOnlyList<AppendResult>> PublishAsync(PublishEnvelope envelope, CancellationToken ct = default)
    {
        long nowMs = NowUnixMs;

        if (envelope.ProducerId > 0)
        {
            string dedupKey = $"{envelope.ProducerId}:{envelope.Sequence}";
            if (!_producerDedup.TryRegister(dedupKey, nowMs))
            {
                return [new AppendResult(new PartitionId(envelope.Topic, 0), Offset.Invalid, 0, IsDuplicate: true)];
            }
        }

        var targetTopics = new List<TopicName>();
        if (!string.IsNullOrEmpty(envelope.Exchange))
        {
            var bindings = await _storage.GetBindingsAsync(envelope.Exchange, ct).ConfigureAwait(false);
            foreach (var b in bindings)
            {
                if (RoutingMatcher.IsMatch(b.Type, b.Pattern, envelope.RoutingKey))
                    targetTopics.Add(b.TargetTopic);
            }
        }
        else
        {
            targetTopics.Add(envelope.Topic);
        }

        if (targetTopics.Count == 0)
            return Array.Empty<AppendResult>();

        int payloadBytes = envelope.Payload.Length + envelope.Key.Length + 64;
        await _memoryBudget.AcquireAsync(payloadBytes, ct).ConfigureAwait(false);

        try
        {
            var results = new List<AppendResult>(targetTopics.Count);
            foreach (var targetTopic in targetTopics.Distinct())
            {
                var topicMeta = await _storage.GetTopicAsync(targetTopic, ct).ConfigureAwait(false)
                    ?? throw new InvalidOperationException($"Topic '{targetTopic}' does not exist.");

                int partitionIndex = SelectPartition(envelope.Key.Span, topicMeta.PartitionCount);
                var partitionId = new PartitionId(targetTopic, partitionIndex);

                var token = await EnsurePartitionOwnershipAsync(partitionId, ct).ConfigureAwait(false);

                var headers = envelope.Headers?.ToList() ?? new List<MessageHeader>();
                if (envelope.Delay is { } delay && delay > TimeSpan.Zero)
                    headers.Add(new MessageHeader("x-deliver-after-ms", (nowMs + (long)delay.TotalMilliseconds).ToString()));
                if (envelope.Ttl is { } ttl && ttl > TimeSpan.Zero)
                    headers.Add(new MessageHeader("x-expires-at-ms", (nowMs + (long)ttl.TotalMilliseconds).ToString()));

                var record = new Record(
                    Offset.Invalid,
                    nowMs,
                    envelope.ProducerId,
                    envelope.Sequence,
                    headers.Count > 0 ? RecordFlags.HasHeaders : RecordFlags.None,
                    envelope.Key,
                    headers,
                    envelope.Payload);

                await _faultHooks.BeforeFsyncAsync(partitionId.ToString(), ct).ConfigureAwait(false);

                var appendRes = await _storage.AppendAsync(
                    partitionId,
                    new[] { record },
                    envelope.Durability,
                    token,
                    ct).ConfigureAwait(false);

                results.Add(appendRes);
            }

            return results;
        }
        finally
        {
            _memoryBudget.Release(payloadBytes);
        }
    }

    public IAsyncEnumerable<Record> ReadStreamAsync(string topic, int partition, Offset fromOffset, int maxCount = 100, CancellationToken ct = default)
    {
        return _storage.ReadAsync(new PartitionId(new TopicName(topic), partition), fromOffset, maxCount, ct);
    }

    public async ValueTask<SubscriptionState> GetOrLoadSubscriptionAsync(SubscriptionId subId, CancellationToken ct = default)
    {
        if (_subStates.TryGetValue(subId, out var existing))
            return existing;

        var loaded = await _storage.LoadStateAsync(subId, ct).ConfigureAwait(false);
        return _subStates.GetOrAdd(subId, loaded);
    }

    public async ValueTask<IReadOnlyList<Record>> PollWorkQueueAsync(
        SubscriptionId subId,
        int maxBatch,
        TimeSpan visibilityTimeout,
        bool orderedByKey = false,
        CancellationToken ct = default)
    {
        var state = await GetOrLoadSubscriptionAsync(subId, ct).ConfigureAwait(false);
        long nowMs = NowUnixMs;

        foreach (var expired in state.GetExpiredInFlight(nowMs))
        {
            var timeoutChange = new StateChange(StateChangeType.TimedOut, expired.Offset, nowMs, Attempt: expired.Attempt, Reason: "VisibilityTimeout");
            state.Apply(timeoutChange);
            await _storage.CommitStateAsync(subId, timeoutChange, ct).ConfigureAwait(false);
        }

        var candidates = new List<(Offset Offset, int Attempt)>();
        foreach (var due in state.GetDueRetries(nowMs).Take(maxBatch))
        {
            candidates.Add((due.Offset, due.Attempt));
        }

        int remaining = maxBatch - candidates.Count;
        if (remaining > 0)
        {
            await foreach (var rec in _storage.ReadAsync(new PartitionId(subId.Topic, subId.Partition), state.NextFetchOffset, remaining, ct))
            {
                candidates.Add((rec.Offset, 1));
            }
        }

        var deliveredRecords = new List<Record>(candidates.Count);
        foreach (var item in candidates)
        {
            await foreach (var rec in _storage.ReadAsync(new PartitionId(subId.Topic, subId.Partition), item.Offset, 1, ct))
            {
                if (rec.ExpiresAtUnixMs is { } expMs && expMs <= nowMs)
                {
                    var skipChange = new StateChange(StateChangeType.SkippedExpired, rec.Offset, nowMs, Reason: "MessageTTLExpired");
                    state.Apply(skipChange);
                    await _storage.CommitStateAsync(subId, skipChange, ct).ConfigureAwait(false);
                    continue;
                }

                if (rec.DeliverAfterUnixMs is { } deliverAfterMs && deliverAfterMs > nowMs)
                {
                    var delayChange = new StateChange(
                        StateChangeType.Nacked,
                        rec.Offset,
                        nowMs,
                        NextRetryUnixMs: deliverAfterMs,
                        Attempt: item.Attempt,
                        Reason: "DelayedDelivery");
                    state.Apply(delayChange);
                    await _storage.CommitStateAsync(subId, delayChange, ct).ConfigureAwait(false);
                    continue;
                }

                string? keyHash = rec.Key.Length > 0 ? Convert.ToHexString(XxHash64.Hash(rec.Key.Span)) : null;

                if (orderedByKey && state.IsKeyLocked(keyHash))
                {
                    var holdChange = new StateChange(
                        StateChangeType.Nacked,
                        rec.Offset,
                        nowMs,
                        NextRetryUnixMs: nowMs,
                        Attempt: item.Attempt,
                        Reason: "KeyOrderedHold",
                        KeyHash: keyHash);
                    state.Apply(holdChange);
                    await _storage.CommitStateAsync(subId, holdChange, ct).ConfigureAwait(false);
                    continue;
                }

                var deliveredChange = new StateChange(
                    StateChangeType.Delivered,
                    rec.Offset,
                    nowMs,
                    Owner: NodeId,
                    ExpiresAtUnixMs: nowMs + (long)visibilityTimeout.TotalMilliseconds,
                    Attempt: item.Attempt,
                    KeyHash: keyHash);

                state.Apply(deliveredChange);
                await _storage.CommitStateAsync(subId, deliveredChange, ct).ConfigureAwait(false);
                deliveredRecords.Add(rec);
            }
        }

        return deliveredRecords;
    }

    public async ValueTask AckAsync(SubscriptionId subId, Offset offset, CancellationToken ct = default)
    {
        var state = await GetOrLoadSubscriptionAsync(subId, ct).ConfigureAwait(false);
        var change = new StateChange(StateChangeType.Acked, offset, NowUnixMs);
        state.Apply(change);
        await _storage.CommitStateAsync(subId, change, ct).ConfigureAwait(false);
    }

    public async ValueTask NackAsync(SubscriptionId subId, Offset offset, RetryPolicy policy, string? reason = null, CancellationToken ct = default)
    {
        var state = await GetOrLoadSubscriptionAsync(subId, ct).ConfigureAwait(false);
        long nowMs = NowUnixMs;

        int currentAttempt = state.InFlight.TryGetValue(offset.Value, out var entry) ? entry.Attempt : 1;
        if (currentAttempt >= policy.MaxAttempts)
        {
            var dlqChange = new StateChange(StateChangeType.MovedToDlq, offset, nowMs, Attempt: currentAttempt, Reason: reason ?? "MaxAttemptsExceeded");
            state.Apply(dlqChange);
            await _storage.CommitStateAsync(subId, dlqChange, ct).ConfigureAwait(false);

            var dlqTopicName = $"{subId.Topic.Value}.{subId.Group}.dlq";
            if (await _storage.GetTopicAsync(new TopicName(dlqTopicName), ct).ConfigureAwait(false) is null)
            {
                await CreateTopicAsync(dlqTopicName, 1, ct: ct).ConfigureAwait(false);
            }

            await foreach (var orig in _storage.ReadAsync(new PartitionId(subId.Topic, subId.Partition), offset, 1, ct))
            {
                var headers = orig.Headers.ToList();
                headers.Add(new MessageHeader("x-dlq-reason", reason ?? "MaxAttemptsExceeded"));
                headers.Add(new MessageHeader("x-dlq-original-offset", offset.Value.ToString()));
                var dlqRec = orig with { Offset = Offset.Invalid, Flags = orig.Flags | RecordFlags.IsDeadLetter | RecordFlags.HasHeaders, Headers = headers };
                await _storage.AppendAsync(new PartitionId(new TopicName(dlqTopicName), 0), new[] { dlqRec }, Durability.DiskSync, ct: ct).ConfigureAwait(false);
            }
        }
        else
        {
            var delay = policy.ComputeDelay(currentAttempt);
            var retryChange = new StateChange(
                StateChangeType.Nacked,
                offset,
                nowMs,
                NextRetryUnixMs: nowMs + (long)delay.TotalMilliseconds,
                Attempt: currentAttempt + 1,
                Reason: reason);
            state.Apply(retryChange);
            await _storage.CommitStateAsync(subId, retryChange, ct).ConfigureAwait(false);
        }
    }

    private async ValueTask<FencingToken> EnsurePartitionOwnershipAsync(PartitionId partitionId, CancellationToken ct)
    {
        if (_ownedPartitions.TryGetValue(partitionId, out var token))
            return token;

        await _faultHooks.BeforeOwnershipTakeoverAsync(partitionId.ToString(), ct).ConfigureAwait(false);
        var lease = await _storage.TryAcquireLeaseAsync(partitionId, NodeId, TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
        if (!lease.Acquired)
            throw new InvalidOperationException($"Partition {partitionId} is currently owned by node {lease.Owner}.");

        await _storage.RecoverAndTruncateCorruptTailAsync(partitionId, ct).ConfigureAwait(false);
        _ownedPartitions[partitionId] = lease.Token;
        return lease.Token;
    }

    private void HandlePeerDied(NodeId deadNode)
    {
        long nowMs = NowUnixMs;
        foreach (var (subId, state) in _subStates)
        {
            foreach (var orphaned in state.GetInFlightByOwner(deadNode))
            {
                var change = new StateChange(StateChangeType.TimedOut, orphaned.Offset, nowMs, Attempt: orphaned.Attempt, Reason: $"PeerDied:{deadNode}");
                state.Apply(change);
                _ = _storage.CommitStateAsync(subId, change);
            }
        }
    }

    private static int SelectPartition(ReadOnlySpan<byte> key, int partitionCount)
    {
        if (partitionCount <= 1) return 0;
        if (key.IsEmpty) return Random.Shared.Next(0, partitionCount);
        uint hash = XxHash32.HashToUInt32(key);
        return (int)(hash % (uint)partitionCount);
    }

    public async ValueTask DisposeAsync()
    {
        if (_membership is not null)
        {
            _membership.OnPeerDied -= HandlePeerDied;
            await _membership.DisposeAsync().ConfigureAwait(false);
        }
        foreach (var p in _ownedPartitions.Keys)
        {
            await _storage.ReleaseLeaseAsync(p, NodeId).ConfigureAwait(false);
        }
        await _storage.DisposeAsync().ConfigureAwait(false);
    }
}
