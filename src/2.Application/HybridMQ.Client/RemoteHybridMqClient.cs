namespace HybridMQ.Client;

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HybridMQ.Client.Abstractions;
using HybridMQ.Client.Serialization;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Protocol;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Topics;

public sealed class RemoteHybridMqInstance : IHybridMqInstance, IProducer, IConsumer, IAdminClient
{
    private readonly ITransportChannel _transport;
    private readonly NodeId _targetNode;
    private readonly IMessageSerializer _serializer;
    private readonly long _producerId = Random.Shared.NextInt64(1, long.MaxValue);
    private int _sequence;

    public IProducer Producer => this;
    public IConsumer Consumer => this;
    public IAdminClient Admin => this;

    public RemoteHybridMqInstance(ITransportChannel transport, NodeId targetNode, IMessageSerializer? serializer = null)
    {
        _transport = transport;
        _targetNode = targetNode;
        _serializer = serializer ?? AotJsonMessageSerializer.Default;
    }

    private async ValueTask<BrokerRpcResponse> SendRpcAsync(BrokerRpcRequest req, CancellationToken ct)
    {
        byte[] reqBytes = JsonSerializer.SerializeToUtf8Bytes(req, BrokerProtocolJsonContext.Default.BrokerRpcRequest);
        var respBytes = await _transport.RequestAsync(_targetNode, reqBytes, ct).ConfigureAwait(false);
        var resp = JsonSerializer.Deserialize(respBytes.Span, BrokerProtocolJsonContext.Default.BrokerRpcResponse)
            ?? throw new InvalidOperationException("Invalid empty RPC response from broker.");

        if (!resp.Success)
            throw new InvalidOperationException($"Broker RPC failed ({req.Command}): {resp.Error}");

        return resp;
    }

    public async ValueTask CreateTopicAsync(string topic, int partitions = 1, RetentionPolicy? retention = null, CancellationToken ct = default)
    {
        await SendRpcAsync(new BrokerRpcRequest(Guid.NewGuid().ToString("N"), BrokerCommandType.CreateTopic, Topic: topic, Partitions: partitions), ct).ConfigureAwait(false);
    }

    public async ValueTask AddBindingAsync(string exchange, ExchangeType type, string pattern, string targetTopic, CancellationToken ct = default)
    {
        await SendRpcAsync(new BrokerRpcRequest(
            Guid.NewGuid().ToString("N"),
            BrokerCommandType.AddBinding,
            Topic: targetTopic,
            Exchange: exchange,
            ExchangeType: type,
            Pattern: pattern), ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<AppendResult>> PublishAsync<T>(
        string topic,
        T message,
        JsonTypeInfo<T>? typeInfo = null,
        string? key = null,
        string? routingKey = null,
        string? exchange = null,
        Durability durability = Durability.DiskSync,
        TimeSpan? delay = null,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        var payload = _serializer.Serialize(message, typeInfo);
        byte[]? keyBytes = string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key);
        int seq = Interlocked.Increment(ref _sequence);

        var resp = await SendRpcAsync(new BrokerRpcRequest(
            Guid.NewGuid().ToString("N"),
            BrokerCommandType.Publish,
            Topic: topic,
            RoutingKey: routingKey,
            Exchange: exchange,
            Key: keyBytes,
            Payload: payload.ToArray(),
            Durability: durability,
            ProducerId: _producerId,
            Sequence: seq,
            DelayMs: delay is { } d ? (long)d.TotalMilliseconds : null,
            TtlMs: ttl is { } t ? (long)t.TotalMilliseconds : null), ct).ConfigureAwait(false);

        return (resp.AppendResults ?? [])
            .Select(r => new AppendResult(new PartitionId(new TopicName(r.Topic), r.Partition), new Offset(r.BaseOffset), r.Count, r.IsDuplicate))
            .ToList();
    }

    public async IAsyncEnumerable<Record> ReadStreamAsync(
        string topic,
        int partition,
        Offset from,
        int maxCount = 100,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var resp = await SendRpcAsync(new BrokerRpcRequest(
            Guid.NewGuid().ToString("N"),
            BrokerCommandType.ReadStream,
            Topic: topic,
            Partition: partition,
            Offset: from.Value,
            MaxBatch: maxCount), ct).ConfigureAwait(false);

        foreach (var dto in resp.Records ?? [])
        {
            yield return ToRecord(dto);
        }
    }

    public ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        string topic,
        string group,
        SubscribeOptions options,
        Func<IMessageContext, T, ValueTask> handler,
        JsonTypeInfo<T>? typeInfo = null,
        CancellationToken ct = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var workerTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var pollResp = await SendRpcAsync(new BrokerRpcRequest(
                    Guid.NewGuid().ToString("N"),
                    BrokerCommandType.PollWorkQueue,
                    Topic: topic,
                    Group: group,
                    Partition: options.Partition,
                    MaxBatch: options.PrefetchCount,
                    VisibilityTimeoutMs: (long)options.AckTimeout.TotalMilliseconds,
                    OrderedByKey: options.OrderedByKey), cts.Token).ConfigureAwait(false);

                var records = pollResp.Records ?? [];
                if (records.Count == 0)
                {
                    await Task.Delay(25, cts.Token).ConfigureAwait(false);
                    continue;
                }

                foreach (var dto in records)
                {
                    var rec = ToRecord(dto);
                    var ctx = new RemoteMessageContext(this, topic, group, options.Partition, options.MaxAttempts, rec);
                    try
                    {
                        var obj = _serializer.Deserialize(rec.Payload, typeInfo)!;
                        await handler(ctx, obj).ConfigureAwait(false);
                        if (!ctx.IsSettled)
                            await ctx.AckAsync(cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!cts.Token.IsCancellationRequested)
                    {
                        if (!ctx.IsSettled)
                            await ctx.NackAsync(reason: ex.Message, ct: cts.Token).ConfigureAwait(false);
                    }
                }
            }
        }, cts.Token);

        return ValueTask.FromResult<IAsyncDisposable>(new RemoteSubHandle(cts, workerTask));
    }

    private static Record ToRecord(RecordWireDto d) =>
        new(new Offset(d.Offset), d.TimestampUnixMs, d.ProducerId, d.Sequence, d.Flags, d.Key, d.Headers, d.Payload);

    public ValueTask DisposeAsync() => _transport.DisposeAsync();

    private sealed class RemoteMessageContext(
        RemoteHybridMqInstance client,
        string topic,
        string group,
        int partition,
        int maxAttempts,
        Record record) : IMessageContext
    {
        public bool IsSettled { get; private set; }
        public Offset Offset => record.Offset;
        public int Partition => partition;
        public ReadOnlyMemory<byte> Key => record.Key;
        public IReadOnlyList<MessageHeader> Headers => record.Headers;

        public async ValueTask AckAsync(CancellationToken ct = default)
        {
            if (IsSettled) return;
            IsSettled = true;
            await client.SendRpcAsync(new BrokerRpcRequest(
                Guid.NewGuid().ToString("N"),
                BrokerCommandType.Ack,
                Topic: topic,
                Group: group,
                Partition: partition,
                Offset: record.Offset.Value), ct).ConfigureAwait(false);
        }

        public async ValueTask NackAsync(TimeSpan? delay = null, string? reason = null, CancellationToken ct = default)
        {
            if (IsSettled) return;
            IsSettled = true;
            await client.SendRpcAsync(new BrokerRpcRequest(
                Guid.NewGuid().ToString("N"),
                BrokerCommandType.Nack,
                Topic: topic,
                Group: group,
                Partition: partition,
                Offset: record.Offset.Value,
                MaxAttempts: maxAttempts,
                Reason: reason), ct).ConfigureAwait(false);
        }
    }

    private sealed class RemoteSubHandle(CancellationTokenSource cts, Task worker) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try { await worker.ConfigureAwait(false); } catch (OperationCanceledException) { }
            cts.Dispose();
        }
    }
}
