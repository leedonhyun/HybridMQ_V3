namespace HybridMQ.Domain.Reliability;

public sealed record RetryPolicy(
    int MaxAttempts = 5,
    TimeSpan InitialDelay = default,
    TimeSpan MaxDelay = default,
    double BackoffMultiplier = 2.0,
    double JitterFactor = 0.2)
{
    public TimeSpan InitialDelayOrDefault => InitialDelay == default ? TimeSpan.FromMilliseconds(200) : InitialDelay;
    public TimeSpan MaxDelayOrDefault => MaxDelay == default ? TimeSpan.FromSeconds(30) : MaxDelay;

    public TimeSpan ComputeDelay(int attempt)
    {
        var baseMs = InitialDelayOrDefault.TotalMilliseconds * Math.Pow(BackoffMultiplier, Math.Max(0, attempt - 1));
        var cappedMs = Math.Min(baseMs, MaxDelayOrDefault.TotalMilliseconds);
        var jitter = (Random.Shared.NextDouble() * 2.0 - 1.0) * JitterFactor * cappedMs;
        return TimeSpan.FromMilliseconds(Math.Max(10, cappedMs + jitter));
    }
}

public sealed class IdempotencyWindow
{
    private readonly TimeSpan _window;
    private readonly int _maxEntries;
    private readonly Dictionary<string, long> _seen = new(StringComparer.Ordinal);
    private readonly Queue<(string Key, long ExpiresAtMs)> _expiryQueue = new();
    private readonly Lock _lock = new();

    public IdempotencyWindow(TimeSpan? window = null, int maxEntries = 100_000)
    {
        _window = window ?? TimeSpan.FromMinutes(10);
        _maxEntries = maxEntries;
    }

    public bool TryRegister(string key, long nowUnixMs)
    {
        lock (_lock)
        {
            EvictExpired(nowUnixMs);
            if (_seen.ContainsKey(key))
                return false;

            if (_seen.Count >= _maxEntries && _expiryQueue.TryDequeue(out var oldest))
            {
                _seen.Remove(oldest.Key);
            }

            long expiresAt = nowUnixMs + (long)_window.TotalMilliseconds;
            _seen[key] = expiresAt;
            _expiryQueue.Enqueue((key, expiresAt));
            return true;
        }
    }

    private void EvictExpired(long nowUnixMs)
    {
        while (_expiryQueue.Count > 0 && _expiryQueue.Peek().ExpiresAtMs <= nowUnixMs)
        {
            var expired = _expiryQueue.Dequeue();
            if (_seen.TryGetValue(expired.Key, out var exp) && exp <= nowUnixMs)
                _seen.Remove(expired.Key);
        }
    }
}
