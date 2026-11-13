namespace HybridMQ.Engine.Diagnostics;

public interface IFaultInjectionHooks
{
    ValueTask BeforeFsyncAsync(string partitionKey, CancellationToken ct = default);
    ValueTask AfterIndexUpdateAsync(string partitionKey, long offset, CancellationToken ct = default);
    ValueTask BeforeOwnershipTakeoverAsync(string partitionKey, CancellationToken ct = default);
}

public sealed class NoOpFaultInjectionHooks : IFaultInjectionHooks
{
    public static readonly NoOpFaultInjectionHooks Instance = new();
    public ValueTask BeforeFsyncAsync(string partitionKey, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask AfterIndexUpdateAsync(string partitionKey, long offset, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask BeforeOwnershipTakeoverAsync(string partitionKey, CancellationToken ct = default) => ValueTask.CompletedTask;
}
