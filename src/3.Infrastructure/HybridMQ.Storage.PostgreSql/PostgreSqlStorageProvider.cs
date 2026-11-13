namespace HybridMQ.Storage.PostgreSql;

using System.Runtime.CompilerServices;
using System.Text.Json;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Exceptions;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using Npgsql;

public sealed class PostgreSqlStorageProvider : IStorageProvider
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly bool _ownsDataSource;

    public PostgreSqlStorageProvider(string connectionString, string schema = "hybridmq")
    {
        _dataSource = NpgsqlDataSource.Create(connectionString);
        _schema = schema;
        _ownsDataSource = true;
    }

    public PostgreSqlStorageProvider(NpgsqlDataSource dataSource, string schema = "hybridmq")
    {
        _dataSource = dataSource;
        _schema = schema;
        _ownsDataSource = false;
    }

    public async ValueTask InitializeAsync(CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            CREATE SCHEMA IF NOT EXISTS {_schema};

            CREATE TABLE IF NOT EXISTS {_schema}.topics (
                name TEXT PRIMARY KEY,
                partition_count INT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                metadata_seq BIGINT NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS {_schema}.bindings (
                id BIGSERIAL PRIMARY KEY,
                exchange_name TEXT NOT NULL,
                exchange_type INT NOT NULL,
                pattern TEXT NOT NULL,
                target_topic TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS {_schema}.partition_heads (
                topic TEXT NOT NULL,
                partition INT NOT NULL,
                next_offset BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (topic, partition)
            );

            CREATE TABLE IF NOT EXISTS {_schema}.records (
                topic TEXT NOT NULL,
                partition INT NOT NULL,
                msg_offset BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                producer_id BIGINT NOT NULL,
                sequence INT NOT NULL,
                flags INT NOT NULL,
                msg_key BYTEA NOT NULL,
                headers_json TEXT NOT NULL,
                payload BYTEA NOT NULL,
                PRIMARY KEY (topic, partition, msg_offset)
            );

            CREATE TABLE IF NOT EXISTS {_schema}.subscription_events (
                id BIGSERIAL PRIMARY KEY,
                sub_group TEXT NOT NULL,
                topic TEXT NOT NULL,
                partition INT NOT NULL,
                change_type TEXT NOT NULL,
                msg_offset BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                owner_node TEXT NULL,
                expires_at_ms BIGINT NULL,
                next_retry_ms BIGINT NULL,
                attempt INT NOT NULL DEFAULT 1,
                reason TEXT NULL,
                key_hash TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_{_schema}_sub_events ON {_schema}.subscription_events (sub_group, topic, partition, id);

            CREATE TABLE IF NOT EXISTS {_schema}.leases (
                topic TEXT NOT NULL,
                partition INT NOT NULL,
                owner_node TEXT NOT NULL,
                fencing_token BIGINT NOT NULL,
                expires_at_ms BIGINT NOT NULL,
                PRIMARY KEY (topic, partition)
            );
        ");
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask CreateTopicAsync(TopicMetadata metadata, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

        await using (var cmd = new NpgsqlCommand($@"
            INSERT INTO {_schema}.topics (name, partition_count, created_at_ms, metadata_seq)
            VALUES (@name, @parts, @created, @seq)
            ON CONFLICT (name) DO NOTHING;", conn, tx))
        {
            cmd.Parameters.AddWithValue("name", metadata.Name.Value);
            cmd.Parameters.AddWithValue("parts", metadata.PartitionCount);
            cmd.Parameters.AddWithValue("created", metadata.CreatedAtUnixMs);
            cmd.Parameters.AddWithValue("seq", metadata.MetadataSequence);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        for (int p = 0; p < metadata.PartitionCount; p++)
        {
            await using var pCmd = new NpgsqlCommand($@"
                INSERT INTO {_schema}.partition_heads (topic, partition, next_offset)
                VALUES (@topic, @part, 0)
                ON CONFLICT (topic, partition) DO NOTHING;", conn, tx);
            pCmd.Parameters.AddWithValue("topic", metadata.Name.Value);
            pCmd.Parameters.AddWithValue("part", p);
            await pCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask<TopicMetadata?> GetTopicAsync(TopicName topic, CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT name, partition_count, created_at_ms, metadata_seq
            FROM {_schema}.topics WHERE name = @name;");
        cmd.Parameters.AddWithValue("name", topic.Value);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;

        return new TopicMetadata(
            new TopicName(reader.GetString(0)),
            reader.GetInt32(1),
            new RetentionPolicy(),
            reader.GetInt64(2),
            reader.GetInt64(3));
    }

    public async ValueTask<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken ct = default)
    {
        var list = new List<TopicMetadata>();
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT name, partition_count, created_at_ms, metadata_seq FROM {_schema}.topics;");
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            list.Add(new TopicMetadata(
                new TopicName(reader.GetString(0)),
                reader.GetInt32(1),
                new RetentionPolicy(),
                reader.GetInt64(2),
                reader.GetInt64(3)));
        }
        return list;
    }

    public async ValueTask AddBindingAsync(BindingRule binding, CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            INSERT INTO {_schema}.bindings (exchange_name, exchange_type, pattern, target_topic)
            VALUES (@ex, @type, @pat, @target);");
        cmd.Parameters.AddWithValue("ex", binding.ExchangeName);
        cmd.Parameters.AddWithValue("type", (int)binding.Type);
        cmd.Parameters.AddWithValue("pat", binding.Pattern);
        cmd.Parameters.AddWithValue("target", binding.TargetTopic.Value);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<BindingRule>> GetBindingsAsync(string exchangeName, CancellationToken ct = default)
    {
        var list = new List<BindingRule>();
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT exchange_name, exchange_type, pattern, target_topic
            FROM {_schema}.bindings WHERE exchange_name = @ex;");
        cmd.Parameters.AddWithValue("ex", exchangeName);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            list.Add(new BindingRule(
                reader.GetString(0),
                (ExchangeType)reader.GetInt32(1),
                reader.GetString(2),
                new TopicName(reader.GetString(3))));
        }
        return list;
    }

    public async ValueTask<AppendResult> AppendAsync(
        PartitionId partition,
        ReadOnlyMemory<Record> batch,
        Durability durability,
        FencingToken? fencingToken = null,
        CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

        if (fencingToken is { } token)
        {
            await using var fenceCmd = new NpgsqlCommand($@"
                SELECT fencing_token FROM {_schema}.leases WHERE topic = @t AND partition = @p;", conn, tx);
            fenceCmd.Parameters.AddWithValue("t", partition.Topic.Value);
            fenceCmd.Parameters.AddWithValue("p", partition.Partition);
            var currentTokenObj = await fenceCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (currentTokenObj is long currentToken && currentToken > token.Value)
                throw new FencingTokenMismatchException(partition.ToString(), currentToken, token.Value);
        }

        // Row-level lock on partition_heads guarantees strictly contiguous, gap-free offset assignment across processes
        long baseOffset;
        await using (var headCmd = new NpgsqlCommand($@"
            SELECT next_offset FROM {_schema}.partition_heads
            WHERE topic = @t AND partition = @p FOR UPDATE;", conn, tx))
        {
            headCmd.Parameters.AddWithValue("t", partition.Topic.Value);
            headCmd.Parameters.AddWithValue("p", partition.Partition);
            var scalar = await headCmd.ExecuteScalarAsync(ct).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"Partition {partition} does not exist.");
            baseOffset = (long)scalar;
        }

        for (int i = 0; i < batch.Length; i++)
        {
            var rec = batch.Span[i];
            long assignedOffset = baseOffset + i;
            string headersJson = JsonSerializer.Serialize(rec.Headers.ToList(), PgJsonContext.Default.ListMessageHeader);

            await using var insCmd = new NpgsqlCommand($@"
                INSERT INTO {_schema}.records
                (topic, partition, msg_offset, timestamp_ms, producer_id, sequence, flags, msg_key, headers_json, payload)
                VALUES (@t, @p, @off, @ts, @prod, @seq, @flags, @key, @hdr, @pay);", conn, tx);
            insCmd.Parameters.AddWithValue("t", partition.Topic.Value);
            insCmd.Parameters.AddWithValue("p", partition.Partition);
            insCmd.Parameters.AddWithValue("off", assignedOffset);
            insCmd.Parameters.AddWithValue("ts", rec.TimestampUnixMs);
            insCmd.Parameters.AddWithValue("prod", rec.ProducerId);
            insCmd.Parameters.AddWithValue("seq", rec.Sequence);
            insCmd.Parameters.AddWithValue("flags", (int)rec.Flags);
            insCmd.Parameters.AddWithValue("key", rec.Key.ToArray());
            insCmd.Parameters.AddWithValue("hdr", headersJson);
            insCmd.Parameters.AddWithValue("pay", rec.Payload.ToArray());
            await insCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var updCmd = new NpgsqlCommand($@"
            UPDATE {_schema}.partition_heads SET next_offset = @next
            WHERE topic = @t AND partition = @p;", conn, tx))
        {
            updCmd.Parameters.AddWithValue("next", baseOffset + batch.Length);
            updCmd.Parameters.AddWithValue("t", partition.Topic.Value);
            updCmd.Parameters.AddWithValue("p", partition.Partition);
            await updCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);
        return new AppendResult(partition, new Offset(baseOffset), batch.Length);
    }

    public async IAsyncEnumerable<Record> ReadAsync(
        PartitionId partition,
        Offset fromOffset,
        int maxCount,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT msg_offset, timestamp_ms, producer_id, sequence, flags, msg_key, headers_json, payload
            FROM {_schema}.records
            WHERE topic = @t AND partition = @p AND msg_offset >= @from
            ORDER BY msg_offset ASC
            LIMIT @lim;");
        cmd.Parameters.AddWithValue("t", partition.Topic.Value);
        cmd.Parameters.AddWithValue("p", partition.Partition);
        cmd.Parameters.AddWithValue("from", fromOffset.Value);
        cmd.Parameters.AddWithValue("lim", maxCount);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var headers = JsonSerializer.Deserialize(reader.GetString(6), PgJsonContext.Default.ListMessageHeader) ?? [];
            yield return new Record(
                new Offset(reader.GetInt64(0)),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt32(3),
                (RecordFlags)reader.GetInt32(4),
                (byte[])reader[5],
                headers,
                (byte[])reader[7]);
        }
    }

    public async ValueTask<Offset> GetHighWatermarkAsync(PartitionId partition, CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT next_offset FROM {_schema}.partition_heads WHERE topic = @t AND partition = @p;");
        cmd.Parameters.AddWithValue("t", partition.Topic.Value);
        cmd.Parameters.AddWithValue("p", partition.Partition);
        var res = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return res is long val ? new Offset(val) : Offset.Earliest;
    }

    public async ValueTask<SubscriptionState> LoadStateAsync(SubscriptionId subscription, CancellationToken ct = default)
    {
        var state = new SubscriptionState(subscription, Offset.Earliest);
        await using var cmd = _dataSource.CreateCommand($@"
            SELECT change_type, msg_offset, timestamp_ms, owner_node, expires_at_ms, next_retry_ms, attempt, reason, key_hash
            FROM {_schema}.subscription_events
            WHERE sub_group = @g AND topic = @t AND partition = @p
            ORDER BY id ASC;");
        cmd.Parameters.AddWithValue("g", subscription.Group);
        cmd.Parameters.AddWithValue("t", subscription.Topic.Value);
        cmd.Parameters.AddWithValue("p", subscription.Partition);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            state.Apply(new StateChange(
                Enum.Parse<StateChangeType>(reader.GetString(0)),
                new Offset(reader.GetInt64(1)),
                reader.GetInt64(2),
                reader.IsDBNull(3) ? null : new NodeId(reader.GetString(3)),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }
        return state;
    }

    public async ValueTask CommitStateAsync(SubscriptionId subscription, StateChange change, CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            INSERT INTO {_schema}.subscription_events
            (sub_group, topic, partition, change_type, msg_offset, timestamp_ms, owner_node, expires_at_ms, next_retry_ms, attempt, reason, key_hash)
            VALUES (@g, @t, @p, @type, @off, @ts, @owner, @exp, @retry, @att, @reason, @kh);");
        cmd.Parameters.AddWithValue("g", subscription.Group);
        cmd.Parameters.AddWithValue("t", subscription.Topic.Value);
        cmd.Parameters.AddWithValue("p", subscription.Partition);
        cmd.Parameters.AddWithValue("type", change.Type.ToString());
        cmd.Parameters.AddWithValue("off", change.Offset.Value);
        cmd.Parameters.AddWithValue("ts", change.TimestampUnixMs);
        cmd.Parameters.AddWithValue("owner", (object?)change.Owner?.Value ?? DBNull.Value);
        cmd.Parameters.AddWithValue("exp", (object?)change.ExpiresAtUnixMs ?? DBNull.Value);
        cmd.Parameters.AddWithValue("retry", (object?)change.NextRetryUnixMs ?? DBNull.Value);
        cmd.Parameters.AddWithValue("att", change.Attempt);
        cmd.Parameters.AddWithValue("reason", (object?)change.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("kh", (object?)change.KeyHash ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask<LeaseResult> TryAcquireLeaseAsync(PartitionId partition, NodeId node, TimeSpan ttl, CancellationToken ct = default)
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long newExp = nowMs + (long)ttl.TotalMilliseconds;

        await using var cmd = _dataSource.CreateCommand($@"
            INSERT INTO {_schema}.leases (topic, partition, owner_node, fencing_token, expires_at_ms)
            VALUES (@t, @p, @node, 1, @exp)
            ON CONFLICT (topic, partition) DO UPDATE
            SET owner_node = EXCLUDED.owner_node,
                fencing_token = {_schema}.leases.fencing_token + 1,
                expires_at_ms = EXCLUDED.expires_at_ms
            WHERE {_schema}.leases.owner_node = @node OR {_schema}.leases.expires_at_ms <= @now
            RETURNING owner_node, fencing_token, expires_at_ms;");
        cmd.Parameters.AddWithValue("t", partition.Topic.Value);
        cmd.Parameters.AddWithValue("p", partition.Partition);
        cmd.Parameters.AddWithValue("node", node.Value);
        cmd.Parameters.AddWithValue("exp", newExp);
        cmd.Parameters.AddWithValue("now", nowMs);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return new LeaseResult(
                true,
                new NodeId(reader.GetString(0)),
                new FencingToken(reader.GetInt64(1)),
                reader.GetInt64(2));
        }

        return new LeaseResult(false, new NodeId("remote-owner"), new FencingToken(0), 0);
    }

    public async ValueTask ReleaseLeaseAsync(PartitionId partition, NodeId node, CancellationToken ct = default)
    {
        await using var cmd = _dataSource.CreateCommand($@"
            DELETE FROM {_schema}.leases WHERE topic = @t AND partition = @p AND owner_node = @node;");
        cmd.Parameters.AddWithValue("t", partition.Topic.Value);
        cmd.Parameters.AddWithValue("p", partition.Partition);
        cmd.Parameters.AddWithValue("node", node.Value);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public ValueTask<int> RecoverAndTruncateCorruptTailAsync(PartitionId partition, CancellationToken ct = default)
        => ValueTask.FromResult(0); // ACID transactional storage does not suffer torn tails

    public async ValueTask DisposeAsync()
    {
        if (_ownsDataSource)
            await _dataSource.DisposeAsync().ConfigureAwait(false);
    }
}

[System.Text.Json.Serialization.JsonSerializable(typeof(List<MessageHeader>))]
internal partial class PgJsonContext : System.Text.Json.Serialization.JsonSerializerContext;
