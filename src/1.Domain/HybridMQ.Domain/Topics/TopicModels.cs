namespace HybridMQ.Domain.Topics;

using HybridMQ.Domain.Primitives;

public sealed record RetentionPolicy(
    TimeSpan? MaxAge = null,
    long? MaxBytes = null,
    bool EnableCompaction = false);

public sealed record TopicMetadata(
    TopicName Name,
    int PartitionCount,
    RetentionPolicy Retention,
    long CreatedAtUnixMs,
    long MetadataSequence = 1);
