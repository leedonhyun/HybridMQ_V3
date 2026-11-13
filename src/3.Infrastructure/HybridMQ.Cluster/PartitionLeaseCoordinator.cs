namespace HybridMQ.Cluster;

using System.Collections.Concurrent;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Engine.Pipelines;

/// <summary>
/// 공유 스토리지(PostgreSQL/MongoDB/Shared File) 기반 파티션 리더 선출, 리스 자동 갱신(Heartbeat),
/// 및 Split-Brain 방지를 위한 Fencing Token 생명주기 코디네이터
/// </summary>
public sealed class PartitionLeaseCoordinator : IAsyncDisposable
{
    private readonly NodeId _localNodeId;
    private readonly IStorageProvider _storage;
    private readonly BrokerEngine _engine;
    private readonly TimeSpan _leaseTtl;
    private readonly ConcurrentDictionary<PartitionId, LeaseResult> _activeLeases = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _renewalLoop;

    public event Action<PartitionId, FencingToken>? OnLeadershipAcquired;
    public event Action<PartitionId>? OnLeadershipLost;

    public PartitionLeaseCoordinator(
        NodeId localNodeId,
        IStorageProvider storage,
        BrokerEngine engine,
        TimeSpan? leaseTtl = null)
    {
        _localNodeId = localNodeId;
        _storage = storage;
        _engine = engine;
        _leaseTtl = leaseTtl ?? TimeSpan.FromSeconds(10);
    }

    public void Start()
    {
        _renewalLoop ??= Task.Run(RenewalLoopAsync, _cts.Token);
    }

    public async ValueTask<LeaseResult> TryAcquireOrRenewAsync(PartitionId partition, CancellationToken ct = default)
    {
        var result = await _storage.TryAcquireLeaseAsync(partition, _localNodeId, _leaseTtl, ct).ConfigureAwait(false);
        if (result.Acquired)
        {
            bool wasLeader = _activeLeases.ContainsKey(partition);
            _activeLeases[partition] = result;
            _engine.RegisterPartitionLease(partition, result.Token);

            if (!wasLeader)
            {
                await _storage.RecoverAndTruncateCorruptTailAsync(partition, ct).ConfigureAwait(false);
                OnLeadershipAcquired?.Invoke(partition, result.Token);
            }
        }
        else
        {
            if (_activeLeases.TryRemove(partition, out _))
            {
                _engine.RevokePartitionLease(partition);
                OnLeadershipLost?.Invoke(partition);
            }
        }
        return result;
    }

    public async ValueTask RebalancePartitionsAsync(CancellationToken ct = default)
    {
        var topics = await _storage.ListTopicsAsync(ct).ConfigureAwait(false);
        foreach (var topic in topics)
        {
            for (int p = 0; p < topic.PartitionCount; p++)
            {
                var pid = new PartitionId(topic.Name, p);
                await TryAcquireOrRenewAsync(pid, ct).ConfigureAwait(false);
            }
        }
    }

    public bool TryGetLease(PartitionId partition, out LeaseResult? lease)
        => _activeLeases.TryGetValue(partition, out lease);

    private async Task RenewalLoopAsync()
    {
        var interval = TimeSpan.FromMilliseconds(Math.Max(250, _leaseTtl.TotalMilliseconds / 3));
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, _cts.Token).ConfigureAwait(false);
                foreach (var pid in _activeLeases.Keys.ToArray())
                {
                    await TryAcquireOrRenewAsync(pid, _cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // 일시적 스토리지 오류 시 다음 주기에 재시도하며, TTL 만료 시 자동 Step-down 처리
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_renewalLoop is not null)
        {
            try { await _renewalLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }

        foreach (var pid in _activeLeases.Keys.ToArray())
        {
            await _storage.ReleaseLeaseAsync(pid, _localNodeId).ConfigureAwait(false);
            _engine.RevokePartitionLease(pid);
        }
        _activeLeases.Clear();
        _cts.Dispose();
    }
}
