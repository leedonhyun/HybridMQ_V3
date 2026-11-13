namespace HybridMQ.Storage.File.Tests;

using HybridMQ.Domain.Exceptions;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Storage.File.IO;
using FluentAssertions;
using Xunit;

public class BinaryRecordCodecTests
{
    [Fact]
    public void Encode_And_Decode_RoundTrip_Preserves_All_Fields()
    {
        var original = new Domain.Messaging.Record(
            Offset.Invalid,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            123456789L,
            42,
            RecordFlags.HasHeaders,
            new byte[] { 0xAA, 0xBB },
            new[] { new MessageHeader("trace-id", "abc-123"), new MessageHeader("region", "ap-northeast-2") },
            new byte[] { 1, 2, 3, 4, 5 }
        );

        var assignedOffset = new Offset(1024);
        var encoded = BinaryRecordCodec.Encode(original, assignedOffset);

        var success = BinaryRecordCodec.TryDecode(encoded, out var decoded, out var consumed);

        success.Should().BeTrue();
        consumed.Should().Be(encoded.Length);
        decoded.Should().NotBeNull();
        decoded!.Offset.Should().Be(assignedOffset);
        decoded.TimestampUnixMs.Should().Be(original.TimestampUnixMs);
        decoded.ProducerId.Should().Be(original.ProducerId);
        decoded.Sequence.Should().Be(original.Sequence);
        decoded.Flags.Should().Be(original.Flags);
        decoded.Key.ToArray().Should().BeEquivalentTo(original.Key.ToArray());
        decoded.Payload.ToArray().Should().BeEquivalentTo(original.Payload.ToArray());
        decoded.Headers.Should().BeEquivalentTo(original.Headers);
    }

    [Fact]
    public void BitFlip_In_Payload_Throws_DataCorruptionException()
    {
        var original = new Domain.Messaging.Record(Offset.Invalid, 1000, 1, 1, RecordFlags.None, ReadOnlyMemory<byte>.Empty, Array.Empty<MessageHeader>(), new byte[] { 10, 20, 30 });
        var encoded = BinaryRecordCodec.Encode(original, new Offset(0));

        // Flip 1 bit in payload
        encoded[^1] ^= 0x01;

        var act = () => BinaryRecordCodec.TryDecode(encoded, out _, out _);
        act.Should().Throw<DataCorruptionException>();
    }

    [Fact]
    public void Partial_Frame_Returns_False_Without_Throwing()
    {
        var original = new Domain.Messaging.Record(Offset.Invalid, 1000, 1, 1, RecordFlags.None, ReadOnlyMemory<byte>.Empty, Array.Empty<MessageHeader>(), new byte[] { 10, 20, 30 });
        var encoded = BinaryRecordCodec.Encode(original, new Offset(0));

        BinaryRecordCodec.TryDecode(encoded.AsSpan(0, encoded.Length - 2), out var decoded, out var consumed).Should().BeFalse();
        decoded.Should().BeNull();
        consumed.Should().Be(0);
    }
}
