namespace HybridMQ.Domain.Messaging;

using HybridMQ.Domain.Primitives;

public enum Durability : byte
{
    Memory = 0,
    Disk = 1,
    DiskSync = 2,
    Replicated = 3
}

[Flags]
public enum RecordFlags : ushort
{
    None = 0,
    CompressedLz4 = 1 << 0,
    CompressedZstd = 1 << 1,
    IsDeadLetter = 1 << 2,
    HasHeaders = 1 << 3
}

public readonly record struct MessageHeader(string Key, string Value);

public sealed record Record(
    Offset Offset,
    long TimestampUnixMs,
    long ProducerId,
    int Sequence,
    RecordFlags Flags,
    ReadOnlyMemory<byte> Key,
    IReadOnlyList<MessageHeader> Headers,
    ReadOnlyMemory<byte> Payload,
    uint Crc32C = 0)
{
    public string? MessageId => Headers.FirstOrDefault(h => h.Key == "x-message-id").Value;
    public long? DeliverAfterUnixMs => TryGetLongHeader("x-deliver-after-ms");
    public long? ExpiresAtUnixMs => TryGetLongHeader("x-expires-at-ms");

    private long? TryGetLongHeader(string key)
    {
        var val = Headers.FirstOrDefault(h => h.Key == key).Value;
        return long.TryParse(val, out var parsed) ? parsed : null;
    }
}

public sealed record PublishEnvelope(
    TopicName Topic,
    string? RoutingKey,
    string? Exchange,
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Payload,
    IReadOnlyList<MessageHeader>? Headers = null,
    Durability Durability = Durability.DiskSync,
    long ProducerId = 0,
    int Sequence = 0,
    TimeSpan? Delay = null,
    TimeSpan? Ttl = null,
    byte Priority = 0);

public readonly record struct AppendResult(
    PartitionId Partition,
    Offset BaseOffset,
    int Count,
    bool IsDuplicate = false);
