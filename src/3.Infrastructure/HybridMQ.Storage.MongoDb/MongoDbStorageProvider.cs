namespace HybridMQ.Storage.MongoDb;

using System.Runtime.CompilerServices;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Exceptions;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

public sealed class MongoDbStorageProvider : IStorageProvider
{
    private readonly IMongoClient _client;
    private readonly IMongoDatabase _db;
    private readonly bool _ownsClient;

    private IMongoCollection<TopicDoc> Topics => _db.GetCollection<TopicDoc>("topics");
    private IMongoCollection<BindingDoc> Bindings => _db.GetCollection<BindingDoc>("bindings");
    private IMongoCollection<PartitionHeadDoc> Heads => _db.GetCollection<PartitionHeadDoc>("partition_heads");
    private IMongoCollection<RecordDoc> Records => _db.GetCollection<RecordDoc>("records");
    private IMongoCollection<SubEventDoc> SubEvents => _db.GetCollection<SubEventDoc>("subscription_events");
    private IMongoCollection<LeaseDoc> Leases => _db.GetCollection<LeaseDoc>("leases");

    public MongoDbStorageProvider(string connectionString, string databaseName = "hybridmq")
    {
        _client = new MongoClient(connectionString);
        _db = _client.GetDatabase(databaseName);
        _ownsClient = true;
    }

    public MongoDbStorageProvider(IMongoDatabase database)
    {
        _client = database.Client;
        _db = database;
        _ownsClient = false;
    }

    public async ValueTask InitializeAsync(CancellationToken ct = default)
    {
        await Records.Indexes.CreateOneAsync(
            new CreateIndexModel<RecordDoc>(
                Builders<RecordDoc>.IndexKeys.Ascending(x => x.Topic).Ascending(x => x.Partition).Ascending(x => x.Offset),
                new CreateIndexOptions { Unique = true }),
            cancellationToken: ct).ConfigureAwait(false);

        await SubEvents.Indexes.CreateOneAsync(
            new CreateIndexModel<SubEventDoc>(
                Builders<SubEventDoc>.IndexKeys.Ascending(x => x.Group).Ascending(x => x.Topic).Ascending(x => x.Partition)),
            cancellationToken: ct).ConfigureAwait(false);
    }

    public async ValueTask CreateTopicAsync(TopicMetadata metadata, CancellationToken ct = default)
    {
        var filter = Builders<TopicDoc>.Filter.Eq(x => x.Id, metadata.Name.Value);
        if (await Topics.Find(filter).AnyAsync(ct).ConfigureAwait(false))
            return;

        await Topics.InsertOneAsync(new TopicDoc
        {
            Id = metadata.Name.Value,
            PartitionCount = metadata.PartitionCount,
            CreatedAtUnixMs = metadata.CreatedAtUnixMs,
            MetadataSequence = metadata.MetadataSequence
        }, cancellationToken: ct).ConfigureAwait(false);

        for (int p = 0; p < metadata.PartitionCount; p++)
        {
            string headId = $"{metadata.Name.Value}:{p}";
            await Heads.UpdateOneAsync(
                Builders<PartitionHeadDoc>.Filter.Eq(x => x.Id, headId),
                Builders<PartitionHeadDoc>.Update.SetOnInsert(x => x.NextOffset, 0L),
                new UpdateOptions { IsUpsert = true },
                ct).ConfigureAwait(false);
        }
    }

    public async ValueTask<TopicMetadata?> GetTopicAsync(TopicName topic, CancellationToken ct = default)
    {
        var doc = await Topics.Find(x => x.Id == topic.Value).FirstOrDefaultAsync(ct).ConfigureAwait(false);
        if (doc is null) return null;

        return new TopicMetadata(
            new TopicName(doc.Id),
            doc.PartitionCount,
            new RetentionPolicy(),
            doc.CreatedAtUnixMs,
            doc.MetadataSequence);
    }

    public async ValueTask<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken ct = default)
    {
        var docs = await Topics.Find(FilterDefinition<TopicDoc>.Empty).ToListAsync(ct).ConfigureAwait(false);
        return docs.Select(d => new TopicMetadata(new TopicName(d.Id), d.PartitionCount, new RetentionPolicy(), d.CreatedAtUnixMs, d.MetadataSequence)).ToList();
    }

    public async ValueTask AddBindingAsync(BindingRule binding, CancellationToken ct = default)
    {
        await Bindings.InsertOneAsync(new BindingDoc
        {
            ExchangeName = binding.ExchangeName,
            ExchangeType = (int)binding.Type,
            Pattern = binding.Pattern,
            TargetTopic = binding.TargetTopic.Value
        }, cancellationToken: ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<BindingRule>> GetBindingsAsync(string exchangeName, CancellationToken ct = default)
    {
        var docs = await Bindings.Find(x => x.ExchangeName == exchangeName).ToListAsync(ct).ConfigureAwait(false);
        return docs.Select(d => new BindingRule(d.ExchangeName, (ExchangeType)d.ExchangeType, d.Pattern, new TopicName(d.TargetTopic))).ToList();
    }

    public async ValueTask<AppendResult> AppendAsync(
        PartitionId partition,
        ReadOnlyMemory<Record> batch,
        Durability durability,
        FencingToken? fencingToken = null,
        CancellationToken ct = default)
    {
        string partKey = $"{partition.Topic.Value}:{partition.Partition}";

        if (fencingToken is { } token)
        {
            var lease = await Leases.Find(x => x.Id == partKey).FirstOrDefaultAsync(ct).ConfigureAwait(false);
            if (lease is not null && lease.FencingToken > token.Value)
                throw new FencingTokenMismatchException(partKey, lease.FencingToken, token.Value);
        }

        // Atomic FindOneAndUpdate increment reserves contiguous offset range
        var updatedHead = await Heads.FindOneAndUpdateAsync<PartitionHeadDoc>(
            Builders<PartitionHeadDoc>.Filter.Eq(x => x.Id, partKey),
            Builders<PartitionHeadDoc>.Update.Inc(x => x.NextOffset, (long)batch.Length),
            new FindOneAndUpdateOptions<PartitionHeadDoc> { ReturnDocument = ReturnDocument.Before },
            ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Partition {partKey} not initialized.");

        long baseOffset = updatedHead.NextOffset;
        var docs = new List<RecordDoc>(batch.Length);
        for (int i = 0; i < batch.Length; i++)
        {
            var r = batch.Span[i];
            long off = baseOffset + i;
            docs.Add(new RecordDoc
            {
                Id = $"{partKey}:{off}",
                Topic = partition.Topic.Value,
                Partition = partition.Partition,
                Offset = off,
                TimestampUnixMs = r.TimestampUnixMs,
                ProducerId = r.ProducerId,
                Sequence = r.Sequence,
                Flags = (int)r.Flags,
                Key = r.Key.ToArray(),
                Headers = r.Headers.Select(h => new HeaderSubDoc { Key = h.Key, Value = h.Value }).ToList(),
                Payload = r.Payload.ToArray()
            });
        }

        var collection = durability >= Durability.Replicated
            ? Records.WithWriteConcern(WriteConcern.WMajority)
            : Records;

        await collection.InsertManyAsync(docs, cancellationToken: ct).ConfigureAwait(false);
        return new AppendResult(partition, new Offset(baseOffset), batch.Length);
    }

    public async IAsyncEnumerable<Record> ReadAsync(
        PartitionId partition,
        Offset fromOffset,
        int maxCount,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var filter = Builders<RecordDoc>.Filter.And(
            Builders<RecordDoc>.Filter.Eq(x => x.Topic, partition.Topic.Value),
            Builders<RecordDoc>.Filter.Eq(x => x.Partition, partition.Partition),
            Builders<RecordDoc>.Filter.Gte(x => x.Offset, fromOffset.Value));

        var docs = await Records.Find(filter)
            .SortBy(x => x.Offset)
            .Limit(maxCount)
            .ToListAsync(ct).ConfigureAwait(false);

        foreach (var d in docs)
        {
            yield return new Record(
                new Offset(d.Offset),
                d.TimestampUnixMs,
                d.ProducerId,
                d.Sequence,
                (RecordFlags)d.Flags,
                d.Key,
                d.Headers.Select(h => new MessageHeader(h.Key, h.Value)).ToList(),
                d.Payload);
        }
    }

    public async ValueTask<Offset> GetHighWatermarkAsync(PartitionId partition, CancellationToken ct = default)
    {
        string partKey = $"{partition.Topic.Value}:{partition.Partition}";
        var head = await Heads.Find(x => x.Id == partKey).FirstOrDefaultAsync(ct).ConfigureAwait(false);
        return head is not null ? new Offset(head.NextOffset) : Offset.Earliest;
    }

    public async ValueTask<SubscriptionState> LoadStateAsync(SubscriptionId subscription, CancellationToken ct = default)
    {
        var state = new SubscriptionState(subscription, Offset.Earliest);
        var filter = Builders<SubEventDoc>.Filter.And(
            Builders<SubEventDoc>.Filter.Eq(x => x.Group, subscription.Group),
            Builders<SubEventDoc>.Filter.Eq(x => x.Topic, subscription.Topic.Value),
            Builders<SubEventDoc>.Filter.Eq(x => x.Partition, subscription.Partition));

        var events = await SubEvents.Find(filter).SortBy(x => x.Id).ToListAsync(ct).ConfigureAwait(false);
        foreach (var ev in events)
        {
            state.Apply(new StateChange(
                Enum.Parse<StateChangeType>(ev.ChangeType),
                new Offset(ev.Offset),
                ev.TimestampUnixMs,
                ev.OwnerNode is not null ? new NodeId(ev.OwnerNode) : null,
                ev.ExpiresAtUnixMs,
                ev.NextRetryUnixMs,
                ev.Attempt,
                ev.Reason,
                ev.KeyHash));
        }
        return state;
    }

    public async ValueTask CommitStateAsync(SubscriptionId subscription, StateChange change, CancellationToken ct = default)
    {
        await SubEvents.InsertOneAsync(new SubEventDoc
        {
            Group = subscription.Group,
            Topic = subscription.Topic.Value,
            Partition = subscription.Partition,
            ChangeType = change.Type.ToString(),
            Offset = change.Offset.Value,
            TimestampUnixMs = change.TimestampUnixMs,
            OwnerNode = change.Owner?.Value,
            ExpiresAtUnixMs = change.ExpiresAtUnixMs,
            NextRetryUnixMs = change.NextRetryUnixMs,
            Attempt = change.Attempt,
            Reason = change.Reason,
            KeyHash = change.KeyHash
        }, cancellationToken: ct).ConfigureAwait(false);
    }

    public async ValueTask<LeaseResult> TryAcquireLeaseAsync(PartitionId partition, NodeId node, TimeSpan ttl, CancellationToken ct = default)
    {
        string partKey = $"{partition.Topic.Value}:{partition.Partition}";
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long newExp = nowMs + (long)ttl.TotalMilliseconds;

        var filter = Builders<LeaseDoc>.Filter.And(
            Builders<LeaseDoc>.Filter.Eq(x => x.Id, partKey),
            Builders<LeaseDoc>.Filter.Or(
                Builders<LeaseDoc>.Filter.Eq(x => x.OwnerNode, node.Value),
                Builders<LeaseDoc>.Filter.Lte(x => x.ExpiresAtUnixMs, nowMs)));

        try
        {
            var doc = await Leases.FindOneAndUpdateAsync<LeaseDoc>(
                filter,
                Builders<LeaseDoc>.Update
                    .Set(x => x.OwnerNode, node.Value)
                    .Set(x => x.ExpiresAtUnixMs, newExp)
                    .Inc(x => x.FencingToken, 1L),
                new FindOneAndUpdateOptions<LeaseDoc> { IsUpsert = true, ReturnDocument = ReturnDocument.After },
                ct).ConfigureAwait(false);

            return new LeaseResult(true, new NodeId(doc.OwnerNode), new FencingToken(doc.FencingToken), doc.ExpiresAtUnixMs);
        }
        catch (MongoCommandException)
        {
            return new LeaseResult(false, new NodeId("other-node"), new FencingToken(0), 0);
        }
    }

    public async ValueTask ReleaseLeaseAsync(PartitionId partition, NodeId node, CancellationToken ct = default)
    {
        string partKey = $"{partition.Topic.Value}:{partition.Partition}";
        await Leases.DeleteOneAsync(x => x.Id == partKey && x.OwnerNode == node.Value, ct).ConfigureAwait(false);
    }

    public ValueTask<int> RecoverAndTruncateCorruptTailAsync(PartitionId partition, CancellationToken ct = default)
        => ValueTask.FromResult(0);

    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
            _client.Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class TopicDoc
    {
        [BsonId] public string Id { get; set; } = string.Empty;
        public int PartitionCount { get; set; }
        public long CreatedAtUnixMs { get; set; }
        public long MetadataSequence { get; set; }
    }

    private sealed class BindingDoc
    {
        [BsonId] public ObjectId Id { get; set; }
        public string ExchangeName { get; set; } = string.Empty;
        public int ExchangeType { get; set; }
        public string Pattern { get; set; } = string.Empty;
        public string TargetTopic { get; set; } = string.Empty;
    }

    private sealed class PartitionHeadDoc
    {
        [BsonId] public string Id { get; set; } = string.Empty;
        public long NextOffset { get; set; }
    }

    private sealed class RecordDoc
    {
        [BsonId] public string Id { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public long Offset { get; set; }
        public long TimestampUnixMs { get; set; }
        public long ProducerId { get; set; }
        public int Sequence { get; set; }
        public int Flags { get; set; }
        public byte[] Key { get; set; } = [];
        public List<HeaderSubDoc> Headers { get; set; } = [];
        public byte[] Payload { get; set; } = [];
    }

    private sealed class HeaderSubDoc
    {
        public string Key { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
    }

    private sealed class SubEventDoc
    {
        [BsonId] public ObjectId Id { get; set; }
        public string Group { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public string ChangeType { get; set; } = string.Empty;
        public long Offset { get; set; }
        public long TimestampUnixMs { get; set; }
        public string? OwnerNode { get; set; }
        public long? ExpiresAtUnixMs { get; set; }
        public long? NextRetryUnixMs { get; set; }
        public int Attempt { get; set; }
        public string? Reason { get; set; }
        public string? KeyHash { get; set; }
    }

    private sealed class LeaseDoc
    {
        [BsonId] public string Id { get; set; } = string.Empty;
        public string OwnerNode { get; set; } = string.Empty;
        public long FencingToken { get; set; }
        public long ExpiresAtUnixMs { get; set; }
    }
}
