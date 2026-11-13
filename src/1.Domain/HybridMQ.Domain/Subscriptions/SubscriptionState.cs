namespace HybridMQ.Domain.Subscriptions;

using HybridMQ.Domain.Primitives;

public sealed record InFlightEntry(
    Offset Offset,
    NodeId ConsumerOwner,
    long DeliveredAtUnixMs,
    long ExpiresAtUnixMs,
    int Attempt,
    string? PartitionKeyHash = null);

public sealed record RetryEntry(
    Offset Offset,
    long NextAttemptUnixMs,
    int Attempt,
    string? LastError);

public sealed record DlqEntry(
    Offset OriginalOffset,
    long MovedAtUnixMs,
    int TotalAttempts,
    string Reason);

public enum StateChangeType
{
    Delivered,
    Acked,
    Nacked,
    TimedOut,
    MovedToDlq,
    SkippedExpired,
    ResetOffset
}

public sealed record StateChange(
    StateChangeType Type,
    Offset Offset,
    long TimestampUnixMs,
    NodeId? Owner = null,
    long? ExpiresAtUnixMs = null,
    long? NextRetryUnixMs = null,
    int Attempt = 1,
    string? Reason = null,
    string? KeyHash = null);

public sealed class SubscriptionState
{
    private readonly SortedDictionary<long, InFlightEntry> _inFlight = new();
    private readonly SortedDictionary<long, RetryEntry> _retries = new();
    private readonly List<DlqEntry> _dlq = new();
    private readonly HashSet<string> _lockedKeys = new(StringComparer.Ordinal);

    public SubscriptionId Id { get; }
    public Offset CommitOffset { get; private set; }
    public Offset NextFetchOffset { get; private set; }
    public IReadOnlyDictionary<long, InFlightEntry> InFlight => _inFlight;
    public IReadOnlyDictionary<long, RetryEntry> Retries => _retries;
    public IReadOnlyList<DlqEntry> DeadLetters => _dlq;

    public SubscriptionState(SubscriptionId id, Offset initialCommitOffset)
    {
        Id = id;
        CommitOffset = initialCommitOffset;
        NextFetchOffset = initialCommitOffset;
    }

    public bool IsKeyLocked(string? keyHash) =>
        !string.IsNullOrEmpty(keyHash) && _lockedKeys.Contains(keyHash);

    public void Apply(StateChange change)
    {
        long off = change.Offset.Value;
        switch (change.Type)
        {
            case StateChangeType.Delivered:
                _retries.Remove(off);
                _inFlight[off] = new InFlightEntry(
                    change.Offset,
                    change.Owner ?? new NodeId("local"),
                    change.TimestampUnixMs,
                    change.ExpiresAtUnixMs ?? (change.TimestampUnixMs + 30_000),
                    change.Attempt,
                    change.KeyHash);
                if (!string.IsNullOrEmpty(change.KeyHash))
                    _lockedKeys.Add(change.KeyHash);
                if (off >= NextFetchOffset.Value)
                    NextFetchOffset = new Offset(off + 1);
                break;

            case StateChangeType.Acked:
                if (_inFlight.Remove(off, out var ackedEntry) && !string.IsNullOrEmpty(ackedEntry.PartitionKeyHash))
                    _lockedKeys.Remove(ackedEntry.PartitionKeyHash);
                _retries.Remove(off);
                AdvanceCommitOffset();
                break;

            case StateChangeType.Nacked:
            case StateChangeType.TimedOut:
                if (_inFlight.Remove(off, out var nackedEntry) && !string.IsNullOrEmpty(nackedEntry.PartitionKeyHash))
                    _lockedKeys.Remove(nackedEntry.PartitionKeyHash);
                _retries[off] = new RetryEntry(
                    change.Offset,
                    change.NextRetryUnixMs ?? change.TimestampUnixMs,
                    change.Attempt,
                    change.Reason);
                AdvanceCommitOffset();
                break;

            case StateChangeType.MovedToDlq:
                if (_inFlight.Remove(off, out var dlqEntry) && !string.IsNullOrEmpty(dlqEntry.PartitionKeyHash))
                    _lockedKeys.Remove(dlqEntry.PartitionKeyHash);
                _retries.Remove(off);
                _dlq.Add(new DlqEntry(change.Offset, change.TimestampUnixMs, change.Attempt, change.Reason ?? "MaxAttemptsExceeded"));
                AdvanceCommitOffset();
                break;

            case StateChangeType.SkippedExpired:
                if (_inFlight.Remove(off, out var expEntry) && !string.IsNullOrEmpty(expEntry.PartitionKeyHash))
                    _lockedKeys.Remove(expEntry.PartitionKeyHash);
                _retries.Remove(off);
                if (off >= NextFetchOffset.Value)
                    NextFetchOffset = new Offset(off + 1);
                AdvanceCommitOffset();
                break;

            case StateChangeType.ResetOffset:
                _inFlight.Clear();
                _retries.Clear();
                _lockedKeys.Clear();
                CommitOffset = change.Offset;
                NextFetchOffset = change.Offset;
                break;
        }
    }

    public IReadOnlyList<InFlightEntry> GetExpiredInFlight(long nowUnixMs) =>
        _inFlight.Values.Where(x => x.ExpiresAtUnixMs <= nowUnixMs).ToList();

    public IReadOnlyList<InFlightEntry> GetInFlightByOwner(NodeId deadNode) =>
        _inFlight.Values.Where(x => x.ConsumerOwner == deadNode).ToList();

    public IReadOnlyList<RetryEntry> GetDueRetries(long nowUnixMs) =>
        _retries.Values.Where(x => x.NextAttemptUnixMs <= nowUnixMs).ToList();

    private void AdvanceCommitOffset()
    {
        if (_inFlight.Count == 0 && _retries.Count == 0 && CommitOffset.Value < NextFetchOffset.Value)
        {
            CommitOffset = NextFetchOffset;
            return;
        }

        long minPending = NextFetchOffset.Value;
        if (_inFlight.Count > 0)
            minPending = Math.Min(minPending, _inFlight.Keys.First());
        if (_retries.Count > 0)
            minPending = Math.Min(minPending, _retries.Keys.First());

        if (minPending > CommitOffset.Value)
            CommitOffset = new Offset(minPending);
    }
}
