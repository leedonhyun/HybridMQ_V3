namespace HybridMQ.Engine.Memory;

using HybridMQ.Domain.Cluster;

public sealed class MemoryBudgetManager
{
    private readonly long _maxBytes;
    private long _allocatedBytes;
    private readonly SemaphoreSlim _backpressureGate = new(1, 1);

    public long MaxBytes => _maxBytes;
    public long CurrentBytes => Volatile.Read(ref _allocatedBytes);

    public MemoryBudgetManager(ResourceProfile profile, int? overrideMaxMemoryMb = null)
    {
        int mb = overrideMaxMemoryMb ?? profile switch
        {
            ResourceProfile.Low => 32,
            ResourceProfile.Standard => 256,
            ResourceProfile.High => 1024,
            _ => 128
        };
        _maxBytes = mb * 1024L * 1024L;
    }

    public async ValueTask AcquireAsync(int bytes, CancellationToken ct = default)
    {
        if (bytes > _maxBytes)
            throw new InvalidOperationException($"Single allocation ({bytes} bytes) exceeds max memory budget ({_maxBytes} bytes).");

        while (Interlocked.Add(ref _allocatedBytes, bytes) > _maxBytes)
        {
            Interlocked.Add(ref _allocatedBytes, -bytes);
            await _backpressureGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (Volatile.Read(ref _allocatedBytes) + bytes <= _maxBytes)
                    continue;
                await Task.Delay(5, ct).ConfigureAwait(false);
            }
            finally
            {
                _backpressureGate.Release();
            }
        }
    }

    public void Release(int bytes)
    {
        var updated = Interlocked.Add(ref _allocatedBytes, -bytes);
        if (updated < 0) Interlocked.Exchange(ref _allocatedBytes, 0);
    }
}
