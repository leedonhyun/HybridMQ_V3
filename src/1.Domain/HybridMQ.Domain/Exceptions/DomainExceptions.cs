namespace HybridMQ.Domain.Exceptions;

public class HybridMqException(string message, Exception? inner = null) : Exception(message, inner);

public sealed class FencingTokenMismatchException(string resource, long expected, long actual)
    : HybridMqException($"Fencing token mismatch on {resource}: expected >= {expected}, actual {actual}");

public sealed class StorageFullException(string path)
    : HybridMqException($"Storage capacity threshold reached at '{path}'. Write rejected to prevent corruption.");

public sealed class DataCorruptionException(string detail)
    : HybridMqException($"Data integrity check failed: {detail}");
