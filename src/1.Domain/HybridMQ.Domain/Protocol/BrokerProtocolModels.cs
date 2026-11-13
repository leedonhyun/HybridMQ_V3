namespace HybridMQ.Domain.Protocol;

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Routing;
using HybridMQ.Domain.Topics;

public enum BrokerCommandType : byte
{
    Ping = 1,
    CreateTopic = 2,
    AddBinding = 3,
    Publish = 4,
    PollWorkQueue = 5,
    Ack = 6,
    Nack = 7,
    ReadStream = 8,
    ListTopics = 9,
    GetPartitionWatermark = 10
}

public sealed record BrokerRpcRequest(
    string CorrelationId,
    BrokerCommandType Command,
    string? Topic = null,
    int Partitions = 1,
    string? Exchange = null,
    ExchangeType ExchangeType = ExchangeType.Direct,
    string? Pattern = null,
    string? RoutingKey = null,
    byte[]? Key = null,
    byte[]? Payload = null,
    Durability Durability = Durability.DiskSync,
    long ProducerId = 0,
    int Sequence = 0,
    long? DelayMs = null,
    long? TtlMs = null,
    string? Group = null,
    int Partition = 0,
    long Offset = 0,
    int MaxBatch = 32,
    long VisibilityTimeoutMs = 30_000,
    bool OrderedByKey = false,
    int MaxAttempts = 5,
    string? Reason = null);

public sealed record RecordWireDto(
    long Offset,
    long TimestampUnixMs,
    long ProducerId,
    int Sequence,
    RecordFlags Flags,
    byte[] Key,
    List<MessageHeader> Headers,
    byte[] Payload);

public sealed record AppendResultWireDto(
    string Topic,
    int Partition,
    long BaseOffset,
    int Count,
    bool IsDuplicate);

public sealed record TopicSummaryWireDto(
    string Name,
    int PartitionCount,
    long CreatedAtUnixMs,
    long HighWatermark);

public sealed record BrokerRpcResponse(
    string CorrelationId,
    bool Success,
    string? Error = null,
    List<AppendResultWireDto>? AppendResults = null,
    List<RecordWireDto>? Records = null,
    List<TopicSummaryWireDto>? Topics = null,
    long HighWatermark = 0);

[JsonSerializable(typeof(BrokerRpcRequest))]
[JsonSerializable(typeof(BrokerRpcResponse))]
[JsonSerializable(typeof(RecordWireDto))]
[JsonSerializable(typeof(AppendResultWireDto))]
[JsonSerializable(typeof(TopicSummaryWireDto))]
[JsonSerializable(typeof(MessageHeader))]
public partial class BrokerProtocolJsonContext : JsonSerializerContext;
