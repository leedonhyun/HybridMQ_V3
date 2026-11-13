namespace HybridMQ.Engine.Pipelines;

using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Protocol;
using HybridMQ.Domain.Reliability;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;

public sealed class BrokerRpcDispatcher(BrokerEngine engine)
{
    public async ValueTask<BrokerRpcResponse> DispatchAsync(BrokerRpcRequest req, CancellationToken ct = default)
    {
        try
        {
            switch (req.Command)
            {
                case BrokerCommandType.Ping:
                    return new BrokerRpcResponse(req.CorrelationId, true);

                case BrokerCommandType.CreateTopic:
                    await engine.CreateTopicAsync(req.Topic!, req.Partitions, new RetentionPolicy(), ct).ConfigureAwait(false);
                    return new BrokerRpcResponse(req.CorrelationId, true);

                case BrokerCommandType.AddBinding:
                    await engine.AddBindingAsync(req.Exchange!, req.ExchangeType, req.Pattern!, req.Topic!, ct).ConfigureAwait(false);
                    return new BrokerRpcResponse(req.CorrelationId, true);

                case BrokerCommandType.Publish:
                {
                    var env = new PublishEnvelope(
                        new TopicName(req.Topic ?? string.Empty),
                        req.RoutingKey,
                        req.Exchange,
                        req.Key ?? ReadOnlyMemory<byte>.Empty,
                        req.Payload ?? ReadOnlyMemory<byte>.Empty,
                        Durability: req.Durability,
                        ProducerId: req.ProducerId,
                        Sequence: req.Sequence,
                        Delay: req.DelayMs is > 0 ? TimeSpan.FromMilliseconds(req.DelayMs.Value) : null,
                        Ttl: req.TtlMs is > 0 ? TimeSpan.FromMilliseconds(req.TtlMs.Value) : null);

                    var res = await engine.PublishAsync(env, ct).ConfigureAwait(false);
                    var wireRes = res.Select(r => new AppendResultWireDto(
                        r.Partition.Topic.Value,
                        r.Partition.Partition,
                        r.BaseOffset.Value,
                        r.Count,
                        r.IsDuplicate)).ToList();

                    return new BrokerRpcResponse(req.CorrelationId, true, AppendResults: wireRes);
                }

                case BrokerCommandType.PollWorkQueue:
                {
                    var subId = new SubscriptionId(req.Group!, new TopicName(req.Topic!), req.Partition);
                    var recs = await engine.PollWorkQueueAsync(
                        subId,
                        req.MaxBatch,
                        TimeSpan.FromMilliseconds(req.VisibilityTimeoutMs),
                        req.OrderedByKey,
                        ct).ConfigureAwait(false);

                    var wireRecs = recs.Select(ToWireDto).ToList();
                    return new BrokerRpcResponse(req.CorrelationId, true, Records: wireRecs);
                }

                case BrokerCommandType.Ack:
                {
                    var subId = new SubscriptionId(req.Group!, new TopicName(req.Topic!), req.Partition);
                    await engine.AckAsync(subId, new Offset(req.Offset), ct).ConfigureAwait(false);
                    return new BrokerRpcResponse(req.CorrelationId, true);
                }

                case BrokerCommandType.Nack:
                {
                    var subId = new SubscriptionId(req.Group!, new TopicName(req.Topic!), req.Partition);
                    await engine.NackAsync(subId, new Offset(req.Offset), new RetryPolicy(MaxAttempts: req.MaxAttempts), req.Reason, ct).ConfigureAwait(false);
                    return new BrokerRpcResponse(req.CorrelationId, true);
                }

                case BrokerCommandType.ReadStream:
                {
                    var list = new List<RecordWireDto>();
                    await foreach (var r in engine.ReadStreamAsync(req.Topic!, req.Partition, new Offset(req.Offset), req.MaxBatch, ct))
                    {
                        list.Add(ToWireDto(r));
                    }
                    return new BrokerRpcResponse(req.CorrelationId, true, Records: list);
                }

                case BrokerCommandType.ListTopics:
                {
                    var topics = await engine.ListTopicsAsync(ct).ConfigureAwait(false);
                    var summaries = new List<TopicSummaryWireDto>(topics.Count);
                    foreach (var t in topics)
                    {
                        long totalHwm = 0;
                        for (int p = 0; p < t.PartitionCount; p++)
                        {
                            var hwm = await engine.GetHighWatermarkAsync(t.Name.Value, p, ct).ConfigureAwait(false);
                            totalHwm += hwm.Value;
                        }
                        summaries.Add(new TopicSummaryWireDto(t.Name.Value, t.PartitionCount, t.CreatedAtUnixMs, totalHwm));
                    }
                    return new BrokerRpcResponse(req.CorrelationId, true, Topics: summaries);
                }

                case BrokerCommandType.GetPartitionWatermark:
                {
                    var hwm = await engine.GetHighWatermarkAsync(req.Topic!, req.Partition, ct).ConfigureAwait(false);
                    return new BrokerRpcResponse(req.CorrelationId, true, HighWatermark: hwm.Value);
                }

                default:
                    return new BrokerRpcResponse(req.CorrelationId, false, $"Unsupported command: {req.Command}");
            }
        }
        catch (Exception ex)
        {
            return new BrokerRpcResponse(req.CorrelationId, false, ex.Message);
        }
    }

    private static RecordWireDto ToWireDto(Record r) =>
        new(r.Offset.Value, r.TimestampUnixMs, r.ProducerId, r.Sequence, r.Flags, r.Key.ToArray(), r.Headers.ToList(), r.Payload.ToArray());
}
