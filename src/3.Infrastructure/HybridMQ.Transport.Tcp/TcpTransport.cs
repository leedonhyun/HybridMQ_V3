namespace HybridMQ.Transport.Tcp;

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using HybridMQ.Domain.Ports;
using HybridMQ.Domain.Primitives;

/// <summary>
/// Length-Prefixed Binary Framing:
/// [TotalLen: 4B][FrameType: 1B (0=OneWay, 1=Request, 2=Response)][RequestId: 8B][Payload: N B]
/// </summary>
public sealed class TcpTransportChannel : ITransportChannel
{
    private readonly ConcurrentDictionary<NodeId, IPEndPoint> _nodeEndpoints = new();
    private readonly ConcurrentDictionary<NodeId, NodeConnection> _connections = new();
    private long _nextRequestId;

    public void RegisterNodeEndpoint(NodeId nodeId, IPEndPoint endpoint) => _nodeEndpoints[nodeId] = endpoint;

    public async ValueTask SendAsync(NodeId targetNode, ReadOnlyMemory<byte> frame, CancellationToken ct = default)
    {
        var conn = await GetOrConnectAsync(targetNode, ct).ConfigureAwait(false);
        long reqId = Interlocked.Increment(ref _nextRequestId);
        await conn.SendFrameAsync(frameType: 0, reqId, frame, ct).ConfigureAwait(false);
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(NodeId targetNode, ReadOnlyMemory<byte> requestFrame, CancellationToken ct = default)
    {
        var conn = await GetOrConnectAsync(targetNode, ct).ConfigureAwait(false);
        long reqId = Interlocked.Increment(ref _nextRequestId);
        return await conn.RequestAsync(reqId, requestFrame, ct).ConfigureAwait(false);
    }

    private async ValueTask<NodeConnection> GetOrConnectAsync(NodeId targetNode, CancellationToken ct)
    {
        if (_connections.TryGetValue(targetNode, out var existing) && existing.IsConnected)
            return existing;

        if (!_nodeEndpoints.TryGetValue(targetNode, out var ep))
            throw new InvalidOperationException($"No TCP endpoint registered for node '{targetNode}'.");

        var tcp = new TcpClient { NoDelay = true };
        await tcp.ConnectAsync(ep.Address, ep.Port, ct).ConfigureAwait(false);
        var conn = new NodeConnection(tcp);
        conn.StartReceiveLoop();
        _connections[targetNode] = conn;
        return conn;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var conn in _connections.Values)
            await conn.DisposeAsync().ConfigureAwait(false);
        _connections.Clear();
    }

    private sealed class NodeConnection(TcpClient client) : IAsyncDisposable
    {
        private readonly NetworkStream _stream = client.GetStream();
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly ConcurrentDictionary<long, TaskCompletionSource<ReadOnlyMemory<byte>>> _pending = new();
        private readonly CancellationTokenSource _cts = new();
        private Task? _readLoop;

        public bool IsConnected => client.Connected;

        public void StartReceiveLoop() => _readLoop = Task.Run(ReadLoopAsync, _cts.Token);

        public async ValueTask SendFrameAsync(byte frameType, long reqId, ReadOnlyMemory<byte> payload, CancellationToken ct)
        {
            int totalLen = 1 + 8 + payload.Length;
            byte[] header = new byte[4 + 1 + 8];
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), totalLen);
            header[4] = frameType;
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(5, 8), reqId);

            await _writeLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(header, ct).ConfigureAwait(false);
                await _stream.WriteAsync(payload, ct).ConfigureAwait(false);
                await _stream.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(long reqId, ReadOnlyMemory<byte> payload, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[reqId] = tcs;
            using var reg = ct.Register(() =>
            {
                if (_pending.TryRemove(reqId, out var removed))
                    removed.TrySetCanceled(ct);
            });

            await SendFrameAsync(frameType: 1, reqId, payload, ct).ConfigureAwait(false);
            return await tcs.Task.ConfigureAwait(false);
        }

        private async Task ReadLoopAsync()
        {
            byte[] header = new byte[13];
            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    await _stream.ReadExactlyAsync(header, _cts.Token).ConfigureAwait(false);
                    int totalLen = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                    byte frameType = header[4];
                    long reqId = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(5, 8));
                    int payloadLen = totalLen - 9;

                    byte[] payload = new byte[payloadLen];
                    if (payloadLen > 0)
                        await _stream.ReadExactlyAsync(payload, _cts.Token).ConfigureAwait(false);

                    if (frameType == 2 && _pending.TryRemove(reqId, out var tcs))
                    {
                        tcs.TrySetResult(payload);
                    }
                }
            }
            catch (Exception ex)
            {
                foreach (var kv in _pending)
                {
                    if (_pending.TryRemove(kv.Key, out var tcs))
                        tcs.TrySetException(ex);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _cts.CancelAsync().ConfigureAwait(false);
            client.Dispose();
            if (_readLoop is not null)
            {
                try { await _readLoop.ConfigureAwait(false); } catch { }
            }
            _writeLock.Dispose();
            _cts.Dispose();
        }
    }
}

public sealed class TcpBrokerServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _requestHandler;
    private readonly CancellationTokenSource _cts = new();
    private Task? _acceptLoop;

    public int BoundPort => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public TcpBrokerServer(IPEndPoint listenEndPoint, Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> requestHandler)
    {
        _listener = new TcpListener(listenEndPoint);
        _requestHandler = requestHandler;
    }

    public void Start()
    {
        _listener.Start();
        _acceptLoop = Task.Run(AcceptLoopAsync, _cts.Token);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                client.NoDelay = true;
                _ = HandleClientAsync(client, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        await using var stream = client.GetStream();
        var writeLock = new SemaphoreSlim(1, 1);
        byte[] header = new byte[13];

        try
        {
            while (!ct.IsCancellationRequested)
            {
                await stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
                int totalLen = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
                byte frameType = header[4];
                long reqId = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(5, 8));
                int payloadLen = totalLen - 9;

                byte[] payload = new byte[payloadLen];
                if (payloadLen > 0)
                    await stream.ReadExactlyAsync(payload, ct).ConfigureAwait(false);

                _ = Task.Run(async () =>
                {
                    var respPayload = await _requestHandler(payload, ct).ConfigureAwait(false);
                    if (frameType == 1)
                    {
                        byte[] respHeader = new byte[13];
                        BinaryPrimitives.WriteInt32LittleEndian(respHeader.AsSpan(0, 4), 9 + respPayload.Length);
                        respHeader[4] = 2;
                        BinaryPrimitives.WriteInt64LittleEndian(respHeader.AsSpan(5, 8), reqId);

                        await writeLock.WaitAsync(ct).ConfigureAwait(false);
                        try
                        {
                            await stream.WriteAsync(respHeader, ct).ConfigureAwait(false);
                            await stream.WriteAsync(respPayload, ct).ConfigureAwait(false);
                            await stream.FlushAsync(ct).ConfigureAwait(false);
                        }
                        finally
                        {
                            writeLock.Release();
                        }
                    }
                }, ct);
            }
        }
        catch { /* Connection closed */ }
        finally
        {
            client.Dispose();
            writeLock.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.ConfigureAwait(false); } catch { }
        }
        _cts.Dispose();
    }
}
