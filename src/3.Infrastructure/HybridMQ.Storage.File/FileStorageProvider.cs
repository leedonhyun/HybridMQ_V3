namespace HybridMQ.Storage.File;

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.IO;

using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Exceptions;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using HybridMQ.Storage.File.IO;

public sealed class FileStorageProvider : IStorageProvider
{
    private readonly string _dataDir;
    private readonly ConcurrentDictionary<PartitionId, FileStream> _partitionLocks = new();
    private readonly ConcurrentDictionary<PartitionId, long> _nextOffsets = new();
    private readonly ConcurrentDictionary<PartitionId, SemaphoreSlim> _writeLocks = new();
    private readonly ConcurrentDictionary<SubscriptionId, SemaphoreSlim> _stateLocks = new();
    private readonly List<BindingRule> _bindings = new();

    public FileStorageProvider(string dataDir)
    {
        _dataDir = Path.GetFullPath(dataDir);
    }

    public ValueTask InitializeAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(_dataDir);
        Directory.CreateDirectory(Path.Combine(_dataDir, "meta"));
        Directory.CreateDirectory(Path.Combine(_dataDir, "topics"));
        Directory.CreateDirectory(Path.Combine(_dataDir, "subs"));
        Directory.CreateDirectory(Path.Combine(_dataDir, "locks", "partitions"));
        return ValueTask.CompletedTask;
    }

    public async ValueTask CreateTopicAsync(TopicMetadata metadata, CancellationToken ct = default)
    {
        string topicDir = Path.Combine(_dataDir, "topics", metadata.Name.Value);
        Directory.CreateDirectory(topicDir);

        for (int p = 0; p < metadata.PartitionCount; p++)
        {
            string partDir = Path.Combine(topicDir, p.ToString());
            Directory.CreateDirectory(partDir);
            string segPath = Path.Combine(partDir, "00000000000000000000.log");
            if (!File.Exists(segPath))
                await File.WriteAllBytesAsync(segPath, Array.Empty<byte>(), ct).ConfigureAwait(false);
        }

        string metaFile = Path.Combine(_dataDir, "meta", $"topic_{metadata.Name.Value}.json");
        var dto = new TopicMetaDto(metadata.Name.Value, metadata.PartitionCount, metadata.CreatedAtUnixMs, metadata.MetadataSequence);
        await File.WriteAllTextAsync(metaFile, JsonSerializer.Serialize(dto), ct).ConfigureAwait(false);
    }

    public async ValueTask<TopicMetadata?> GetTopicAsync(TopicName topic, CancellationToken ct = default)
    {
        string metaFile = Path.Combine(_dataDir, "meta", $"topic_{topic.Value}.json");
        if (!File.Exists(metaFile)) return null;

        string json = await File.ReadAllTextAsync(metaFile, ct).ConfigureAwait(false);
        var dto = JsonSerializer.Deserialize<TopicMetaDto>(json);
        if (dto is null) return null;

        return new TopicMetadata(new TopicName(dto.Name), dto.PartitionCount, new RetentionPolicy(), dto.CreatedAtUnixMs, dto.MetadataSequence);
    }

    public async ValueTask<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken ct = default)
    {
        var list = new List<TopicMetadata>();
        string metaDir = Path.Combine(_dataDir, "meta");
        if (!Directory.Exists(metaDir)) return list;

        foreach (var file in Directory.GetFiles(metaDir, "topic_*.json"))
        {
            string name = Path.GetFileNameWithoutExtension(file)["topic_".Length..];
            var meta = await GetTopicAsync(new TopicName(name), ct).ConfigureAwait(false);
            if (meta is not null) list.Add(meta);
        }
        return list;
    }

    public ValueTask AddBindingAsync(BindingRule binding, CancellationToken ct = default)
    {
        lock (_bindings) _bindings.Add(binding);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<BindingRule>> GetBindingsAsync(string exchangeName, CancellationToken ct = default)
    {
        lock (_bindings)
        {
            var matched = _bindings.Where(b => string.Equals(b.ExchangeName, exchangeName, StringComparison.Ordinal)).ToList();
            return ValueTask.FromResult<IReadOnlyList<BindingRule>>(matched);
        }
    }

    public async ValueTask<AppendResult> AppendAsync(
        PartitionId partition,
        ReadOnlyMemory<Record> batch,
        Durability durability,
        FencingToken? fencingToken = null,
        CancellationToken ct = default)
    {
        var gate = _writeLocks.GetOrAdd(partition, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            string segPath = GetActiveSegmentPath(partition);
            long baseOff = await GetOrScanNextOffsetAsync(partition, segPath, ct).ConfigureAwait(false);
            long currentOff = baseOff;

            using var ms = new MemoryStream();
            for (int i = 0; i < batch.Length; i++)
            {
                byte[] encoded = BinaryRecordCodec.Encode(batch.Span[i], new Offset(currentOff));
                ms.Write(encoded, 0, encoded.Length);
                currentOff++;
            }

            using var fs = new FileStream(segPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, useAsync: true);
            long startPos = fs.Position;
            fs.Seek(0, SeekOrigin.End);
            await fs.WriteAsync(ms.GetBuffer().AsMemory(0, (int)ms.Length), ct).ConfigureAwait(false);
            if (durability >= Durability.DiskSync)
                fs.Flush(true);
            else
                await fs.FlushAsync(ct).ConfigureAwait(false);
            //await fs.FlushAsync(flushToDisk: durability >= Durability.DiskSync).ConfigureAwait(false);

            string idxPath = Path.ChangeExtension(segPath, ".idx");
            byte[] idxEntry = new byte[16];
            BinaryPrimitives.WriteInt64LittleEndian(idxEntry.AsSpan(0, 8), baseOff);
            BinaryPrimitives.WriteInt64LittleEndian(idxEntry.AsSpan(8, 8), startPos);
            await using var idxFs = new FileStream(idxPath, FileMode.Append, FileAccess.Write, FileShare.Read);
            await idxFs.WriteAsync(idxEntry, ct).ConfigureAwait(false);

            _nextOffsets[partition] = currentOff;
            return new AppendResult(partition, new Offset(baseOff), batch.Length);
        }
        finally
        {
            gate.Release();
        }
    }

    public async IAsyncEnumerable<Record> ReadAsync(
        PartitionId partition,
        Offset fromOffset,
        int maxCount,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        string segPath = GetActiveSegmentPath(partition);
        if (!File.Exists(segPath)) yield break;

        //byte[] data = await File.ReadAllBytesAsync(segPath, ct).ConfigureAwait(false);
        byte[] data = await ReadAllBytesSharedAsync(segPath, ct).ConfigureAwait(false);
        int pos = await LookupIndexOffsetAsync(segPath, fromOffset.Value, ct).ConfigureAwait(false);
        int yielded = 0;

        while (pos < data.Length && yielded < maxCount)
        {
            ct.ThrowIfCancellationRequested();
            if (!BinaryRecordCodec.TryDecode(data.AsSpan(pos), out var record, out int consumed) || record is null)
                break;

            pos += consumed;
            if (record.Offset.Value >= fromOffset.Value)
            {
                yielded++;
                yield return record;
            }
        }
    }
    // 2. 클래스 하단(닫는 중괄호 '}' 바로 위)에 헬퍼 메서드 추가
    private static async ValueTask<byte[]> ReadAllBytesSharedAsync(string path, CancellationToken ct)
    {
        await using var fs = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite, // 쓰기 핸들과 동시 접근 허용
            bufferSize: 4096,
            useAsync: true);

        var buffer = new byte[fs.Length];
        await fs.ReadExactlyAsync(buffer, ct).ConfigureAwait(false);
        return buffer;
    }
    private static async ValueTask<int> LookupIndexOffsetAsync(string segPath, long targetOffset, CancellationToken ct)
    {
        if (targetOffset <= 0) return 0;
        string idxPath = Path.ChangeExtension(segPath, ".idx");
        if (!File.Exists(idxPath)) return 0;

        byte[] idxData = await File.ReadAllBytesAsync(idxPath, ct).ConfigureAwait(false);
        int bestPos = 0;
        for (int i = 0; i + 16 <= idxData.Length; i += 16)
        {
            long off = BinaryPrimitives.ReadInt64LittleEndian(idxData.AsSpan(i, 8));
            long filePos = BinaryPrimitives.ReadInt64LittleEndian(idxData.AsSpan(i + 8, 8));
            if (off <= targetOffset)
                bestPos = (int)filePos;
            else
                break;
        }
        return bestPos;
    }

    public async ValueTask<Offset> GetHighWatermarkAsync(PartitionId partition, CancellationToken ct = default)
    {
        string segPath = GetActiveSegmentPath(partition);
        long next = await GetOrScanNextOffsetAsync(partition, segPath, ct).ConfigureAwait(false);
        return new Offset(next);
    }

    public async ValueTask<SubscriptionState> LoadStateAsync(SubscriptionId subscription, CancellationToken ct = default)
    {
        var gate = _stateLocks.GetOrAdd(subscription, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var state = new SubscriptionState(subscription, Offset.Earliest);
            string stateLogPath = GetSubscriptionStateLogPath(subscription);
            if (!File.Exists(stateLogPath)) return state;

            foreach (var line in await File.ReadAllLinesAsync(stateLogPath, ct).ConfigureAwait(false))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var dto = JsonSerializer.Deserialize<StateChangeDto>(line);
                if (dto is not null)
                {
                    state.Apply(new StateChange(
                        Enum.Parse<StateChangeType>(dto.Type),
                        new Offset(dto.Offset),
                        dto.TimestampUnixMs,
                        dto.Owner is not null ? new NodeId(dto.Owner) : null,
                        dto.ExpiresAtUnixMs,
                        dto.NextRetryUnixMs,
                        dto.Attempt,
                        dto.Reason,
                        dto.KeyHash));
                }
            }
            return state;
        }
        finally
        {
            gate.Release();
        }
    }

    public async ValueTask CommitStateAsync(SubscriptionId subscription, StateChange change, CancellationToken ct = default)
    {
        var gate = _stateLocks.GetOrAdd(subscription, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            string stateLogPath = GetSubscriptionStateLogPath(subscription);
            Directory.CreateDirectory(Path.GetDirectoryName(stateLogPath)!);

            var dto = new StateChangeDto(
                change.Type.ToString(),
                change.Offset.Value,
                change.TimestampUnixMs,
                change.Owner?.Value,
                change.ExpiresAtUnixMs,
                change.NextRetryUnixMs,
                change.Attempt,
                change.Reason,
                change.KeyHash);

            string line = JsonSerializer.Serialize(dto) + Environment.NewLine;
            await File.AppendAllTextAsync(stateLogPath, line, ct).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public ValueTask<LeaseResult> TryAcquireLeaseAsync(PartitionId partition, NodeId node, TimeSpan ttl, CancellationToken ct = default)
    {
        string lockPath = Path.Combine(_dataDir, "locks", "partitions", $"{partition.Topic.Value}-{partition.Partition}.lock");
        if (_partitionLocks.ContainsKey(partition))
        {
            return ValueTask.FromResult(new LeaseResult(true, node, new FencingToken(1), DateTimeOffset.UtcNow.Add(ttl).ToUnixTimeMilliseconds()));
        }

        try
        {
            var fs = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            _partitionLocks[partition] = fs;
            return ValueTask.FromResult(new LeaseResult(true, node, new FencingToken(1), DateTimeOffset.UtcNow.Add(ttl).ToUnixTimeMilliseconds()));
        }
        catch (IOException)
        {
            return ValueTask.FromResult(new LeaseResult(false, new NodeId("other-process"), new FencingToken(0), 0));
        }
    }

    public ValueTask ReleaseLeaseAsync(PartitionId partition, NodeId node, CancellationToken ct = default)
    {
        if (_partitionLocks.TryRemove(partition, out var fs))
        {
            fs.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    public async ValueTask<int> RecoverAndTruncateCorruptTailAsync(PartitionId partition, CancellationToken ct = default)
    {
        string segPath = GetActiveSegmentPath(partition);
        if (!File.Exists(segPath)) return 0;

        byte[] data = await File.ReadAllBytesAsync(segPath, ct).ConfigureAwait(false);
        int validBytes = 0;
        long lastValidOffset = -1;

        while (validBytes < data.Length)
        {
            try
            {
                if (!BinaryRecordCodec.TryDecode(data.AsSpan(validBytes), out var rec, out int consumed) || rec is null)
                    break;

                validBytes += consumed;
                lastValidOffset = rec.Offset.Value;
            }
            catch (DataCorruptionException)
            {
                break;
            }
        }

        int truncatedBytes = data.Length - validBytes;
        if (truncatedBytes > 0)
        {
            await using var fs = new FileStream(segPath, FileMode.Open, FileAccess.Write, FileShare.None);
            fs.SetLength(validBytes);
            //await fs.FlushAsync(flushToDisk: true).ConfigureAwait(false);
            fs.Flush(true);
        }

        _nextOffsets[partition] = lastValidOffset + 1;
        return truncatedBytes;
    }

    private async ValueTask<long> GetOrScanNextOffsetAsync(PartitionId partition, string segPath, CancellationToken ct)
    {
        if (_nextOffsets.TryGetValue(partition, out long cached))
            return cached;

        await RecoverAndTruncateCorruptTailAsync(partition, ct).ConfigureAwait(false);
        return _nextOffsets.GetValueOrDefault(partition, 0);
    }

    private string GetActiveSegmentPath(PartitionId p) =>
        Path.Combine(_dataDir, "topics", p.Topic.Value, p.Partition.ToString(), "00000000000000000000.log");

    private string GetSubscriptionStateLogPath(SubscriptionId s) =>
        Path.Combine(_dataDir, "subs", s.Group, s.Topic.Value, s.Partition.ToString(), "state.log");

    public ValueTask DisposeAsync()
    {
        foreach (var fs in _partitionLocks.Values) fs.Dispose();
        _partitionLocks.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed record TopicMetaDto(string Name, int PartitionCount, long CreatedAtUnixMs, long MetadataSequence);
    private sealed record StateChangeDto(string Type, long Offset, long TimestampUnixMs, string? Owner, long? ExpiresAtUnixMs, long? NextRetryUnixMs, int Attempt, string? Reason, string? KeyHash);
}
