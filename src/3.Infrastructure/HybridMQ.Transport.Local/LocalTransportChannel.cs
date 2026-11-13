namespace HybridMQ.Transport.Local;

using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Buffers.Binary;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;

/// <summary>
/// 동일 프로세스 내 초저지연 인메모리 버스(In-Process Bus) 또는 동일 머신 프로세스 간 NamedPipe 전송을 지원하는 로컬 트랜스포트
/// </summary>
public sealed class LocalInMemoryTransportBus
{
    private readonly ConcurrentDictionary<NodeId, Func<NodeId, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>> _handlers = new();

    public void RegisterEndpoint(NodeId nodeId, Func<NodeId, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler)
    {
        _handlers[nodeId] = handler;
    }

    public void UnregisterEndpoint(NodeId nodeId) => _handlers.TryRemove(nodeId, out _);

    internal ValueTask<ReadOnlyMemory<byte>> DispatchAsync(NodeId source, NodeId target, ReadOnlyMemory<byte> frame, CancellationToken ct)
    {
        if (!_handlers.TryGetValue(target, out var handler))
            throw new InvalidOperationException($"Local endpoint for node '{target}' is not registered.");

        return handler(source, frame, ct);
    }
}

public sealed class LocalTransportChannel : ITransportChannel
{
    private readonly NodeId _localNodeId;
    private readonly LocalInMemoryTransportBus? _bus;
    private readonly string? _pipeNamePrefix;

    public LocalTransportChannel(NodeId localNodeId, LocalInMemoryTransportBus bus)
    {
        _localNodeId = localNodeId;
        _bus = bus;
    }

    public LocalTransportChannel(NodeId localNodeId, string pipeNamePrefix)
    {
        _localNodeId = localNodeId;
        _pipeNamePrefix = pipeNamePrefix;
    }

    public async ValueTask SendAsync(NodeId targetNode, ReadOnlyMemory<byte> frame, CancellationToken ct = default)
    {
        _ = await RequestAsync(targetNode, frame, ct).ConfigureAwait(false);
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(NodeId targetNode, ReadOnlyMemory<byte> requestFrame, CancellationToken ct = default)
    {
        if (_bus is not null)
        {
            return await _bus.DispatchAsync(_localNodeId, targetNode, requestFrame, ct).ConfigureAwait(false);
        }

        string pipeName = $"{_pipeNamePrefix}_{targetNode.Value}";
        await using var client = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        await client.ConnectAsync(5000, ct).ConfigureAwait(false);

        byte[] lenPrefix = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, requestFrame.Length);
        await client.WriteAsync(lenPrefix, ct).ConfigureAwait(false);
        await client.WriteAsync(requestFrame, ct).ConfigureAwait(false);
        await client.FlushAsync(ct).ConfigureAwait(false);

        await client.ReadExactlyAsync(lenPrefix, ct).ConfigureAwait(false);
        int respLen = BinaryPrimitives.ReadInt32LittleEndian(lenPrefix);
        byte[] respBuf = new byte[respLen];
        await client.ReadExactlyAsync(respBuf, ct).ConfigureAwait(false);
        return respBuf;
    }

    public ValueTask DisposeAsync()
    {
        _bus?.UnregisterEndpoint(_localNodeId);
        return ValueTask.CompletedTask;
    }
}

public sealed class LocalNamedPipeServer : IAsyncDisposable
{
    private readonly string _pipeName;
    private readonly Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _handler;
    private readonly CancellationTokenSource _cts = new();
    private Task? _acceptLoop;

    public LocalNamedPipeServer(string pipeNamePrefix, NodeId nodeId, Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler)
    {
        _pipeName = $"{pipeNamePrefix}_{nodeId.Value}";
        _handler = handler;
    }

    public void Start()
    {
        _acceptLoop = Task.Run(AcceptLoopAsync, _cts.Token);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            var server = new NamedPipeServerStream(_pipeName, PipeDirection.InOut, NamedPipeServerStream.MaxAllowedServerInstances, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            try
            {
                await server.WaitForConnectionAsync(_cts.Token).ConfigureAwait(false);
                _ = HandleConnectionAsync(server, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                await server.DisposeAsync().ConfigureAwait(false);
                break;
            }
        }
    }

    private async Task HandleConnectionAsync(NamedPipeServerStream stream, CancellationToken ct)
    {
        await using (stream)
        {
            byte[] lenBuf = new byte[4];
            await stream.ReadExactlyAsync(lenBuf, ct).ConfigureAwait(false);
            int reqLen = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);
            byte[] reqBuf = new byte[reqLen];
            await stream.ReadExactlyAsync(reqBuf, ct).ConfigureAwait(false);

            var resp = await _handler(reqBuf, ct).ConfigureAwait(false);
            BinaryPrimitives.WriteInt32LittleEndian(lenBuf, resp.Length);
            await stream.WriteAsync(lenBuf, ct).ConfigureAwait(false);
            await stream.WriteAsync(resp, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
        _cts.Dispose();
    }
}
