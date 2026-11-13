namespace HybridMQ.Storage.File.IO;

using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using HybridMQ.Domain.Exceptions;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;

public static class BinaryRecordCodec
{
    public const int FixedHeaderSize = 4 + 4 + 8 + 8 + 8 + 4 + 2; // 38 bytes

    public static byte[] Encode(Record record, Offset assignedOffset)
    {
        int headersBytes = 2;
        foreach (var h in record.Headers)
        {
            headersBytes += 2 + Encoding.UTF8.GetByteCount(h.Key) + 2 + Encoding.UTF8.GetByteCount(h.Value);
        }

        int bodySize = (FixedHeaderSize - 8) + 4 + record.Key.Length + headersBytes + 4 + record.Payload.Length;
        int totalSize = 8 + bodySize;
        byte[] buffer = new byte[totalSize];
        Span<byte> span = buffer;

        BinaryPrimitives.WriteInt32LittleEndian(span[..4], totalSize);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(8, 8), assignedOffset.Value);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(16, 8), record.TimestampUnixMs);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(24, 8), record.ProducerId);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(32, 4), record.Sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(36, 2), (ushort)record.Flags);

        int pos = 38;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(pos, 4), record.Key.Length);
        pos += 4;
        record.Key.Span.CopyTo(span.Slice(pos, record.Key.Length));
        pos += record.Key.Length;

        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(pos, 2), (ushort)record.Headers.Count);
        pos += 2;
        foreach (var h in record.Headers)
        {
            int kLen = Encoding.UTF8.GetBytes(h.Key, span.Slice(pos + 2));
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(pos, 2), (ushort)kLen);
            pos += 2 + kLen;

            int vLen = Encoding.UTF8.GetBytes(h.Value, span.Slice(pos + 2));
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(pos, 2), (ushort)vLen);
            pos += 2 + vLen;
        }

        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(pos, 4), record.Payload.Length);
        pos += 4;
        record.Payload.Span.CopyTo(span.Slice(pos, record.Payload.Length));

        uint crc = Crc32.HashToUInt32(span[8..]);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4, 4), crc);

        return buffer;
    }

    public static bool TryDecode(ReadOnlySpan<byte> frame, out Record? record, out int bytesConsumed)
    {
        record = null;
        bytesConsumed = 0;

        if (frame.Length < FixedHeaderSize) return false;
        int totalLength = BinaryPrimitives.ReadInt32LittleEndian(frame[..4]);
        if (totalLength < FixedHeaderSize || frame.Length < totalLength) return false;

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(4, 4));
        uint computedCrc = Crc32.HashToUInt32(frame.Slice(8, totalLength - 8));
        if (storedCrc != computedCrc)
            throw new DataCorruptionException($"CRC32C mismatch: stored=0x{storedCrc:X8}, computed=0x{computedCrc:X8}");

        long offset = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(8, 8));
        long ts = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(16, 8));
        long prodId = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(24, 8));
        int seq = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(32, 4));
        var flags = (RecordFlags)BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(36, 2));

        int pos = 38;
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(pos, 4));
        pos += 4;
        if (keyLen < 0 || pos + keyLen > totalLength)
            throw new DataCorruptionException("Invalid key length.");
        byte[] key = frame.Slice(pos, keyLen).ToArray();
        pos += keyLen;

        ushort headerCount = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(pos, 2));
        pos += 2;
        var headers = new List<MessageHeader>(headerCount);
        for (int i = 0; i < headerCount; i++)
        {
            if (pos + 2 > totalLength) throw new DataCorruptionException("Truncated header key.");
            ushort kLen = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(pos, 2));
            pos += 2;
            string k = Encoding.UTF8.GetString(frame.Slice(pos, kLen));
            pos += kLen;

            if (pos + 2 > totalLength) throw new DataCorruptionException("Truncated header value.");
            ushort vLen = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(pos, 2));
            pos += 2;
            string v = Encoding.UTF8.GetString(frame.Slice(pos, vLen));
            pos += vLen;

            headers.Add(new MessageHeader(k, v));
        }

        if (pos + 4 > totalLength) throw new DataCorruptionException("Truncated payload length.");
        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(pos, 4));
        pos += 4;
        if (payloadLen < 0 || pos + payloadLen != totalLength)
            throw new DataCorruptionException("Payload length mismatch with frame total length.");
        byte[] payload = frame.Slice(pos, payloadLen).ToArray();

        record = new Record(new Offset(offset), ts, prodId, seq, flags, key, headers, payload, storedCrc);
        bytesConsumed = totalLength;
        return true;
    }
}
