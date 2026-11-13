namespace HybridMQ.Cluster;

using System.Collections.Concurrent;

using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;

public sealed record PeerNodeInfo(
    NodeId NodeId,
    string Endpoint,
    long LastHeartbeatUnixMs,
    bool IsAlive);

/// <summary>
/// 다중 브로커 노드 간 멤버십 관리 및 Heartbeat 기반 장애 감지(Failure Detector)
/// </summary>
public sealed class ClusterMembershipManager : IPeerMembershipProvider
{
    private readonly ConcurrentDictionary<NodeId, PeerNodeInfo> _peers = new();
    private readonly TimeSpan _heartbeatTimeout;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _cts = new();
    private Task? _monitorLoop;

    public NodeId LocalNodeId { get; }
    public string LocalEndpoint { get; }

    public event Action<NodeId>? OnPeerDied;
    public event Action<NodeId>? OnPeerJoined;
    private int _disposed; // 추가: 중복 Dispose 방지 플래그
    public ClusterMembershipManager(
        NodeId localNodeId,
        string localEndpoint = "127.0.0.1:5679",
        TimeSpan? heartbeatTimeout = null,
        TimeProvider? timeProvider = null)
    {
        LocalNodeId = localNodeId;
        LocalEndpoint = localEndpoint;
        _heartbeatTimeout = heartbeatTimeout ?? TimeSpan.FromSeconds(10);
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    private long NowUnixMs => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    public ValueTask RegisterSelfAsync(CancellationToken ct = default)
    {
        RecordHeartbeat(LocalNodeId, LocalEndpoint);
        _monitorLoop ??= Task.Run(MonitorPeersAsync, _cts.Token);
        return ValueTask.CompletedTask;
    }

    public void RecordHeartbeat(NodeId nodeId, string endpoint = "")
    {
        long now = NowUnixMs;
        bool isNewOrRecovered = false;

        _peers.AddOrUpdate(
            nodeId,
            _ =>
            {
                isNewOrRecovered = true;
                return new PeerNodeInfo(nodeId, endpoint, now, IsAlive: true);
            },
            (_, existing) =>
            {
                if (!existing.IsAlive) isNewOrRecovered = true;
                string ep = string.IsNullOrEmpty(endpoint) ? existing.Endpoint : endpoint;
                return existing with { Endpoint = ep, LastHeartbeatUnixMs = now, IsAlive = true };
            });

        if (isNewOrRecovered && nodeId != LocalNodeId)
        {
            OnPeerJoined?.Invoke(nodeId);
        }
    }

    public ValueTask<IReadOnlyList<NodeId>> GetActivePeersAsync(CancellationToken ct = default)
    {
        RecordHeartbeat(LocalNodeId, LocalEndpoint);
        var active = _peers.Values
            .Where(p => p.IsAlive)
            .Select(p => p.NodeId)
            .ToList();
        return ValueTask.FromResult<IReadOnlyList<NodeId>>(active);
    }

    public IReadOnlyList<PeerNodeInfo> GetClusterSnapshot() => _peers.Values.ToList();

    public void EvaluatePeerHealth()
    {
        long now = NowUnixMs;
        long timeoutMs = (long)_heartbeatTimeout.TotalMilliseconds;

        foreach (var (nodeId, info) in _peers)
        {
            if (nodeId == LocalNodeId)
            {
                RecordHeartbeat(LocalNodeId, LocalEndpoint);
                continue;
            }

            if (info.IsAlive && (now - info.LastHeartbeatUnixMs) > timeoutMs)
            {
                var deadInfo = info with { IsAlive = false };
                if (_peers.TryUpdate(nodeId, deadInfo, info))
                {
                    OnPeerDied?.Invoke(nodeId);
                }
            }
        }
    }

    private async Task MonitorPeersAsync()
    {
        var interval = TimeSpan.FromMilliseconds(Math.Max(200, _heartbeatTimeout.TotalMilliseconds / 3));
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, _timeProvider, _cts.Token).ConfigureAwait(false);
                EvaluatePeerHealth();
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        //await _cts.CancelAsync().ConfigureAwait(false);
        //if (_monitorLoop is not null)
        //{
        //    try { await _monitorLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        //}
        //_cts.Dispose();

        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _cts.CancelAsync().ConfigureAwait(false);
        if (_monitorLoop is not null)
        {
            try { await _monitorLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
        _cts.Dispose();
    }
}