namespace HybridMQ.Client.Serialization;

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

public interface IMessageSerializer
{
    ReadOnlyMemory<byte> Serialize<T>(T value, JsonTypeInfo<T>? typeInfo = null);
    T? Deserialize<T>(ReadOnlyMemory<byte> data, JsonTypeInfo<T>? typeInfo = null);
}

public sealed class AotJsonMessageSerializer : IMessageSerializer
{
    public static readonly AotJsonMessageSerializer Default = new();

    public ReadOnlyMemory<byte> Serialize<T>(T value, JsonTypeInfo<T>? typeInfo = null)
    {
        if (value is ReadOnlyMemory<byte> rawMem) return rawMem;
        if (value is byte[] rawArr) return rawArr;

        if (typeInfo is not null)
            return JsonSerializer.SerializeToUtf8Bytes(value, typeInfo);

        throw new InvalidOperationException(
            $"NativeAOT safety: JsonTypeInfo<{typeof(T).Name}> must be provided via JsonSerializerContext.");
    }

    public T? Deserialize<T>(ReadOnlyMemory<byte> data, JsonTypeInfo<T>? typeInfo = null)
    {
        if (typeof(T) == typeof(ReadOnlyMemory<byte>))
            return (T)(object)data;
        if (typeof(T) == typeof(byte[]))
            return (T)(object)data.ToArray();

        if (typeInfo is not null)
            return JsonSerializer.Deserialize(data.Span, typeInfo);

        throw new InvalidOperationException(
            $"NativeAOT safety: JsonTypeInfo<{typeof(T).Name}> must be provided via JsonSerializerContext.");
    }
}
