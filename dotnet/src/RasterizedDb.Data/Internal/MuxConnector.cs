using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>
/// One <c>rastcp/2</c> connection shared by any number of callers: every frame carries an id the client
/// chose, so many requests are in flight at once and the server answers them in any order.
/// </summary>
/// <remarks>
/// A frame is <c>[u32 length][u32 id][u8 kind][payload]</c>, the length counting the payload only. Kind
/// 0 is a request or its answer, 1 says the server's handler timed out, 2 that it failed (the payload a
/// message). Such a connection holds no transaction block: its statements belong to unrelated callers,
/// and the server refuses transaction control on it.
/// </remarks>
internal sealed class MuxConnector : IDisposable
{
    public const int Header = 9;

    private readonly Connector _connector;
    private readonly Stream _stream;
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<Response>> _pending = new();
    private readonly CancellationTokenSource _closing = new();
    private int _nextId;
    private int _inFlight;
    private volatile RasterizedDbException? _failure;

    public long LastUsed = Environment.TickCount64;

    public bool Broken => _failure is not null;

    public int InFlight => Volatile.Read(ref _inFlight);

    public long Id => _connector.Id;

    /// <summary>The connection underneath, for what an answer's reader needs of it (its notices, its cancel sequence).</summary>
    public Connector Connector => _connector;

    private MuxConnector(Connector connector)
    {
        _connector = connector;
        _stream = connector.Stream;
        _ = Task.Run(ReadLoop);
    }

    public static async Task<MuxConnector> Open(ConnectionSettings settings, CancellationToken cancellation)
    {
        // Logged in and set up one request at a time; from here on, many at once.
        var connector = await Connector.Open(settings, async: true, cancellation, multiplexed: true).ConfigureAwait(false);
        return new MuxConnector(connector);
    }

    private async Task ReadLoop()
    {
        var header = new byte[Header];
        try
        {
            while (true)
            {
                await _stream.ReadExactlyAsync(header, _closing.Token).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadUInt32LittleEndian(header);
                var id = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4));
                var kind = header[8];
                if (length > (uint)_connector.Settings.MaxMessageSize)
                {
                    // Its body is not read, so the stream cannot be followed past it: the one request
                    // is told why, the others that the connection is gone.
                    var tooLarge = new RasterizedDbException(
                        RasterizedDbErrorKind.Transport,
                        $"the answer is {length} bytes, past the limit of {_connector.Settings.MaxMessageSize} (Max Message Size in the connection string); raise it, or page the read");
                    if (_pending.TryRemove(id, out var owner))
                    {
                        owner.TrySetException(tooLarge);
                    }

                    throw tooLarge;
                }

                var size = (int)length;
                var body = ArrayPool<byte>.Shared.Rent(Math.Max(size, 16));
                try
                {
                    await _stream.ReadExactlyAsync(body.AsMemory(0, size), _closing.Token).ConfigureAwait(false);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(body);
                    throw;
                }

                LastUsed = Environment.TickCount64;
                if (!_pending.TryRemove(id, out var waiter))
                {
                    // Its caller gave up waiting.
                    ArrayPool<byte>.Shared.Return(body);
                    continue;
                }

                switch (kind)
                {
                    case 0:
                        if (!waiter.TrySetResult(new Response(body, size)))
                        {
                            ArrayPool<byte>.Shared.Return(body);
                        }

                        break;
                    case 1:
                        ArrayPool<byte>.Shared.Return(body);
                        waiter.TrySetException(new RasterizedDbException(RasterizedDbErrorKind.Timeout, "the server's handler did not finish within its timeout"));
                        break;
                    default:
                        var message = Encoding.UTF8.GetString(body, 0, size);
                        ArrayPool<byte>.Shared.Return(body);
                        waiter.TrySetException(new RasterizedDbException(RasterizedDbErrorKind.Server, $"the server could not answer the request: {message}"));
                        break;
                }
            }
        }
        catch (Exception e)
        {
            Fail(e as RasterizedDbException
                 ?? new RasterizedDbException(
                     RasterizedDbErrorKind.Transport,
                     e is EndOfStreamException or OperationCanceledException ? "the connection is closed" : $"the connection failed: {e.Message}",
                     inner: e));
        }
    }

    private void Fail(RasterizedDbException failure)
    {
        _failure ??= failure;
        foreach (var id in _pending.Keys)
        {
            if (_pending.TryRemove(id, out var waiter))
            {
                // Whether the statement ran is unknown; it is not sent again.
                waiter.TrySetException(_failure);
            }
        }

        _connector.Break();
    }

    /// <summary>
    /// Sends a request and waits for its answer. <paramref name="request"/> starts with
    /// <see cref="Header"/> bytes this fills in.
    /// </summary>
    public async ValueTask<Response> Exchange(ByteWriter request, int timeoutSeconds, CancellationToken cancellation)
    {
        if (_failure is { } failure)
        {
            throw failure;
        }

        var id = unchecked((uint)Interlocked.Increment(ref _nextId));
        request.PatchUInt32(0, (uint)(request.Length - Header));
        request.PatchUInt32(4, id);
        request.UnderlyingArray[8] = 0;
        var waiter = new TaskCompletionSource<Response>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = waiter;
        Interlocked.Increment(ref _inFlight);
        LastUsed = Environment.TickCount64;
        try
        {
            await _write.WaitAsync(cancellation).ConfigureAwait(false);
            try
            {
                // Written whole under the lock: frames of two requests must not interleave. Not
                // cancellable part-way, which would leave half a frame on the stream.
                await _stream.WriteAsync(request.WrittenMemory, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not RasterizedDbException)
            {
                var broken = new RasterizedDbException(RasterizedDbErrorKind.Transport, $"the connection failed: {e.Message}", inner: e);
                Fail(broken);
                throw broken;
            }
            finally
            {
                _write.Release();
            }

            if (_failure is { } late && _pending.TryRemove(id, out _))
            {
                throw late;
            }

            try
            {
                return timeoutSeconds <= 0
                    ? await waiter.Task.WaitAsync(cancellation).ConfigureAwait(false)
                    : await waiter.Task.WaitAsync(TimeSpan.FromMilliseconds(timeoutSeconds * 1000L + Connector.AnswerGraceMs), cancellation).ConfigureAwait(false);
            }
            catch (TimeoutException e)
            {
                // The connection goes on: the answer, if it ever comes, finds nobody and is dropped.
                Abandon(id, waiter);
                throw new RasterizedDbException(
                    RasterizedDbErrorKind.Timeout,
                    $"no answer came within {timeoutSeconds} s (Command Timeout); whether the statement ran is unknown",
                    inner: e);
            }
            catch (OperationCanceledException)
            {
                Abandon(id, waiter);
                throw;
            }
        }
        catch
        {
            _pending.TryRemove(id, out _);
            throw;
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    private void Abandon(uint id, TaskCompletionSource<Response> waiter)
    {
        _pending.TryRemove(id, out _);
        // An answer that arrived in the same instant: its buffer goes back.
        if (!waiter.TrySetCanceled() && waiter.Task.IsCompletedSuccessfully)
        {
            waiter.Task.Result.Return();
        }
    }

    public void Dispose()
    {
        Fail(new RasterizedDbException(RasterizedDbErrorKind.Transport, "the connection is closed"));
        _closing.Cancel();
        _connector.Dispose();
        _closing.Dispose();
        _write.Dispose();
    }
}
