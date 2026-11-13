namespace HybridMQ.Cluster;

using System.Text.Json;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Protocol;
using HybridMQ.Engine.Pipelines;

/// <summary>
/// 클러스터 환경에서 팔로워 노드로 인입된 쓰기/소비 요청을 파티션 리더 노드로 투명하게 포워딩하는 라우터
/// </summary>
public sealed class ClusterRpcRouter(
    NodeId localNodeId,
    BrokerRpcDispatcher localDispatcher,
    PartitionLeaseCoordinator leaseCoordinator,
    ITransportChannel peerTransport)
{
    public async ValueTask<BrokerRpcResponse> RouteAndDispatchAsync(BrokerRpcRequest request, CancellationToken ct = default)
    {
        if (request.Command is BrokerCommandType.Publish or BrokerCommandType.PollWorkQueue or BrokerCommandType.Ack or BrokerCommandType.Nack
            && !string.IsNullOrEmpty(request.Topic))
        {
            var pid = new PartitionId(new TopicName(request.Topic), request.Partition);
            if (!leaseCoordinator.TryGetLease(pid, out _))
            {
                var lease = await leaseCoordinator.TryAcquireOrRenewAsync(pid, ct).ConfigureAwait(false);
                if (!lease.Acquired && lease.Owner != localNodeId && !string.IsNullOrEmpty(lease.Owner.Value)
                    && lease.Owner.Value is not ("remote-owner" or "other-node" or "other-process"))
                {
                    byte[] forwardBytes = JsonSerializer.SerializeToUtf8Bytes(request, BrokerProtocolJsonContext.Default.BrokerRpcRequest);
                    var respBytes = await peerTransport.RequestAsync(lease.Owner, forwardBytes, ct).ConfigureAwait(false);
                    return JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse)!;
                }
            }
        }

        return await localDispatcher.DispatchAsync(request, ct).ConfigureAwait(false);
    }
}
