using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>An answer's bytes, rented from the array pool; whoever holds it gives it back.</summary>
internal struct Response(byte[] buffer, int length)
{
    public byte[]? Buffer = buffer;
    public readonly int Length = length;

    public readonly ReadOnlySpan<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => new(Buffer, 0, Length);
    }

    public void Return()
    {
        var buffer = Buffer;
        Buffer = null;
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}

/// <summary>
/// One physical connection: a <c>rastcp/1</c> stream (TLS over TCP, or a unix socket), logged in and
/// set up, on which requests and answers strictly alternate.
/// </summary>
/// <remarks>
/// A frame is four bytes of little-endian length and then that many bytes. <c>rastcp/1</c> is the
/// protocol whose connection carries one caller, which is what an ADO.NET connection is: it is the one
/// that can hold a transaction block, temporary tables and sequence state.
/// </remarks>
internal sealed class Connector : IDisposable
{
    private const int MaxAuthMessage = 64 * 1024;
    private const int ReadAhead = 16 * 1024;

    /// <summary>What a statement's wait is extended by, so the server's own timeout answer (57014) arrives first.</summary>
    public const int AnswerGraceMs = 2000;

    /// <summary>The server closes a native connection quiet for this long, rolling back its transaction.</summary>
    public const long ServerIdleCloseMs = 30_000;

    private static long _nextId;

    public readonly ConnectionSettings Settings;
    public readonly long Id = Interlocked.Increment(ref _nextId);

    /// <summary>The request being built. Its first four bytes are the frame's length, patched when it is sent.</summary>
    public readonly ByteWriter Writer = new(2048);

    private readonly Stream _stream;
    private readonly Socket _socket;
    private readonly byte[] _readBuffer;
    private CancellationTokenSource _timeoutSource = new();
    private int _socketTimeoutMs = -2;
    private long _cancelSequence;
    private bool _disposed;

    public bool Broken { get; private set; }

    /// <summary>Whether a transaction block is open on this connection, as far as its answers have said.</summary>
    public bool InBlock;

    /// <summary>Whether the block is in the failed state (25P02 until it is rolled back).</summary>
    public bool BlockFailed;

    /// <summary><see cref="Environment.TickCount64"/> when the connection was last sent on or answered.</summary>
    public long LastUsed;

    /// <summary>When the connection was put back in its pool.</summary>
    public long Returned;

    public bool NoticesEnabled { get; private set; }

    /// <summary>Where this connection's notices go; set by whoever holds it.</summary>
    public Action<RasterizedDbNotice>? NoticeSink;

    private Connector(ConnectionSettings settings, Socket socket, Stream stream)
    {
        Settings = settings;
        _socket = socket;
        _stream = stream;
        _readBuffer = ArrayPool<byte>.Shared.Rent(ReadAhead);
        LastUsed = Environment.TickCount64;
    }

    public long IdleMs => Environment.TickCount64 - LastUsed;

    // ------------------------------------------------------------------ opening

    public static async ValueTask<Connector> Open(ConnectionSettings settings, bool async, CancellationToken cancellation)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        if (settings.ConnectTimeoutMs != Timeout.Infinite)
        {
            timeout.CancelAfter(settings.ConnectTimeoutMs);
        }

        var token = timeout.Token;
        Socket? socket = null;
        Stream? stream = null;
        Connector? connector = null;
        try
        {
            byte[]? binding;
            if (settings.IsUnix)
            {
                socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                await Connect(socket, new UnixDomainSocketEndPoint(settings.SocketPath!), async, token).ConfigureAwait(false);
                stream = new NetworkStream(socket, ownsSocket: true);
                // The protocol's name, which a TLS handshake would have carried in ALPN.
                var preamble = "rastcp/1"u8.ToArray();
                if (async)
                {
                    await stream.WriteAsync(preamble, token).ConfigureAwait(false);
                }
                else
                {
                    stream.Write(preamble);
                }

                binding = Tls.UnixBinding.ToArray();
            }
            else
            {
                socket = await ConnectTcp(settings, async, token).ConfigureAwait(false);
                // Small request/response exchanges over a long-lived connection otherwise meet the
                // delayed-ACK timer between turns.
                socket.NoDelay = true;
                (stream, binding) = await Tls.Open(socket, settings, settings.Login == RasterizedDbLogin.Password, async, token).ConfigureAwait(false);
            }

            connector = new Connector(settings, socket, stream);
            await connector.Authenticate(binding, async, token).ConfigureAwait(false);
            await connector.SetUp(async, token).ConfigureAwait(false);
            connector.LastUsed = Environment.TickCount64;
            return connector;
        }
        catch (Exception e)
        {
            if (connector is not null)
            {
                connector.Dispose();
            }
            else
            {
                stream?.Dispose();
                socket?.Dispose();
            }

            if (e is OperationCanceledException && !cancellation.IsCancellationRequested)
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Timeout, $"connecting to {settings.DataSourceName} timed out", inner: e);
            }

            if (e is RasterizedDbException or OperationCanceledException)
            {
                throw;
            }

            throw new RasterizedDbException(RasterizedDbErrorKind.Transport, $"could not connect to {settings.DataSourceName}: {e.Message}", inner: e);
        }
    }

    private static async ValueTask Connect(Socket socket, EndPoint endPoint, bool async, CancellationToken token)
    {
        if (async)
        {
            await socket.ConnectAsync(endPoint, token).ConfigureAwait(false);
        }
        else
        {
            // The connect timeout is the token's; a blocking connect has none of its own.
            socket.ConnectAsync(endPoint, token).AsTask().GetAwaiter().GetResult();
        }
    }

    private static async ValueTask<Socket> ConnectTcp(ConnectionSettings settings, bool async, CancellationToken token)
    {
        IPAddress[] addresses;
        if (IPAddress.TryParse(settings.Host, out var literal))
        {
            addresses = [literal];
        }
        else
        {
            addresses = async
                ? await Dns.GetHostAddressesAsync(settings.Host!, token).ConfigureAwait(false)
                : Dns.GetHostAddresses(settings.Host!);
        }

        Exception? last = null;
        foreach (var address in addresses)
        {
            var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await Connect(socket, new IPEndPoint(address, settings.Port), async, token).ConfigureAwait(false);
                return socket;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                socket.Dispose();
                last = e;
            }
        }

        throw last ?? new SocketException((int)SocketError.HostNotFound);
    }

    // ------------------------------------------------------------------- login

    private const byte ClientHello = 0x01;
    private const byte ClientSaslInitial = 0x02;
    private const byte ClientSaslFinal = 0x03;
    private const byte ServerOk = 0x00;
    private const byte ServerSasl = 0x01;
    private const byte ServerContinue = 0x02;
    private const byte ServerError = 0xFF;

    private static RasterizedDbException AuthFailed(string message) => new(RasterizedDbErrorKind.Authentication, message);

    private async ValueTask Authenticate(byte[]? binding, bool async, CancellationToken token)
    {
        var settings = Settings;
        if (settings.Login == RasterizedDbLogin.Trust)
        {
            return;
        }

        // The hello: [0x01][version][u16 user][user], and from version 2 [u16 database][database].
        var user = Encoding.UTF8.GetBytes(settings.Username!);
        var database = settings.Database is null ? null : Encoding.UTF8.GetBytes(settings.Database);
        if (user.Length > ushort.MaxValue || database?.Length > ushort.MaxValue)
        {
            throw AuthFailed("the user or database name is too long");
        }

        BeginRequest();
        Writer.WriteByte(ClientHello);
        Writer.WriteByte(database is null ? (byte)1 : (byte)2);
        Writer.WriteUInt16((ushort)user.Length);
        Writer.Write(user);
        if (database is not null)
        {
            Writer.WriteUInt16((ushort)database.Length);
            Writer.Write(database);
        }

        await Send(async, token).ConfigureAwait(false);
        byte tag;
        string text;
        try
        {
            (tag, text) = await AuthReply(async, token).ConfigureAwait(false);
        }
        catch (RasterizedDbException e) when (e.Kind == RasterizedDbErrorKind.Transport)
        {
            throw AuthFailed(database is not null
                ? $"the server closed the connection at the login, which named database \"{settings.Database}\": it may predate database selection over the native protocol, or ask for no login at all (a unix socket under --rdb-socket-auth trust; connect with Login=Trust then)"
                : "the server did not take part in the login (it closed the connection): it most likely asks for none, as a unix socket under --rdb-socket-auth trust does; connect with Login=Trust if that server is meant");
        }

        if (tag is not (ServerOk or ServerSasl or ServerError))
        {
            throw AuthFailed($"the server did not take part in the login (it answered with a message of kind {tag}): it most likely asks for none; connect with Login=Trust if that server is meant");
        }

        if (settings.Login == RasterizedDbLogin.Peer)
        {
            switch (tag)
            {
                case ServerOk:
                    return;
                case ServerSasl:
                    throw AuthFailed("the server asks for a password: peer login needs a unix socket under --rdb-socket-auth peer");
                default:
                    throw AuthFailed(text);
            }
        }

        switch (tag)
        {
            case ServerSasl:
                break;
            case ServerOk:
                throw AuthFailed("the server let the connection in without a password exchange, so it never proved who it is; log in with Login=Trust if that server is meant");
            default:
                throw AuthFailed(text);
        }

        var scram = new ScramClient(settings.Username!, settings.Password!, binding ?? throw AuthFailed("this connection has no channel binding"));
        BeginRequest();
        Writer.WriteByte(ClientSaslInitial);
        Writer.WriteUtf8(scram.ClientFirst);
        await Send(async, token).ConfigureAwait(false);

        (tag, text) = await AuthReply(async, token).ConfigureAwait(false);
        if (tag != ServerContinue)
        {
            throw AuthFailed(text);
        }

        // PBKDF2 over up to a million rounds: off the caller's thread pool turn when it can be.
        var serverFirst = text;
        var clientFinal = async ? await Task.Run(() => scram.Respond(serverFirst), token).ConfigureAwait(false) : scram.Respond(serverFirst);
        BeginRequest();
        Writer.WriteByte(ClientSaslFinal);
        Writer.WriteUtf8(clientFinal);
        await Send(async, token).ConfigureAwait(false);

        (tag, text) = await AuthReply(async, token).ConfigureAwait(false);
        if (tag != ServerOk)
        {
            throw AuthFailed(text);
        }

        scram.Verify(text);
    }

    private async ValueTask<(byte Tag, string Text)> AuthReply(bool async, CancellationToken token)
    {
        var reply = await ReadFrame(async, MaxAuthMessage, token).ConfigureAwait(false);
        try
        {
            if (reply.Length == 0)
            {
                throw AuthFailed("the server sent an empty authentication message");
            }

            return (reply.Span[0], Encoding.UTF8.GetString(reply.Span[1..]));
        }
        finally
        {
            reply.Return();
        }
    }

    /// <summary>
    /// The requests a connection sends before any statement: the database or role of a connection that
    /// ran no exchange, then the request for notices.
    /// </summary>
    private async ValueTask SetUp(bool async, CancellationToken token)
    {
        if (Settings.SelectionRequest is { } selection)
        {
            BeginRequest();
            Writer.Write(selection);
            await Send(async, token).ConfigureAwait(false);
            var answer = await ReadFrame(async, Settings.MaxMessageSize, token).ConfigureAwait(false);
            try
            {
                var what = Settings.Role is not null ? $"name role \"{Settings.Role}\" on a trust connection" : $"select database \"{Settings.Database}\" over the native protocol";
                var span = answer.Span;
                if (span.Length == 0 || span[0] != Answer.Simple)
                {
                    var detail = span.Length > 1 && span[0] == Answer.Error ? Encoding.UTF8.GetString(span[1..]) : "an answer of another kind";
                    throw AuthFailed($"this server cannot {what} (it predates the request), so the connection was not used: {detail}");
                }

                if (SimpleAnswer.FirstError(span[1..]) is { } error)
                {
                    throw error.SqlState == "0A000"
                        ? AuthFailed($"this server cannot {what}, so the connection was not used: {error.Message}")
                        : new RasterizedDbException(RasterizedDbErrorKind.Authentication, error.Message, error.SqlState);
                }
            }
            finally
            {
                answer.Return();
            }
        }

        if (Settings.Notices)
        {
            BeginRequest();
            Writer.Write("\u0001NOTICES;"u8);
            await Send(async, token).ConfigureAwait(false);
            var answer = await ReadFrame(async, Settings.MaxMessageSize, token).ConfigureAwait(false);
            // A server that predates notices refuses the request without running anything, and simply
            // sends none.
            NoticesEnabled = answer.Length > 0 && answer.Span[0] == Answer.Simple && SimpleAnswer.FirstError(answer.Span[1..]) is null;
            answer.Return();
        }

        Writer.Reset();
    }

    // ---------------------------------------------------------------- exchanges

    /// <summary>Starts a request: clears the writer and leaves room for the frame's length.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void BeginRequest()
    {
        Writer.Clear();
        Writer.Advance(4);
    }

    /// <summary>A token unique to this request, for <c>\x01SIMPLE:cancel=…</c> and the CANCEL that names it.</summary>
    public long NextCancelSequence() => Interlocked.Increment(ref _cancelSequence);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask Send(bool async, CancellationToken token)
    {
        var length = Writer.Length - 4;
        Writer.PatchUInt32(0, (uint)length);
        if (async)
        {
            await _stream.WriteAsync(Writer.WrittenMemory, token).ConfigureAwait(false);
        }
        else
        {
            _stream.Write(Writer.WrittenSpan);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Response> ReadFrame(bool async, int maxMessageSize, CancellationToken token)
    {
        // One read usually brings the prefix and the whole of a small answer.
        var buffer = _readBuffer;
        int have;
        try
        {
            have = async
                ? await _stream.ReadAtLeastAsync(buffer.AsMemory(0, ReadAhead), 4, throwOnEndOfStream: true, token).ConfigureAwait(false)
                : _stream.ReadAtLeast(buffer.AsSpan(0, ReadAhead), 4, throwOnEndOfStream: true);
        }
        catch (EndOfStreamException e)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Transport, "the server closed the connection", inner: e);
        }

        var length = BinaryPrimitives.ReadUInt32LittleEndian(buffer);
        if (length > (uint)maxMessageSize)
        {
            throw new RasterizedDbException(
                RasterizedDbErrorKind.Transport,
                $"the answer is {length} bytes, past the limit of {maxMessageSize} (Max Message Size in the connection string); raise it, or page the read");
        }

        var size = (int)length;
        have -= 4;
        if (have > size)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "the server sent more than one answer to a request");
        }

        var body = ArrayPool<byte>.Shared.Rent(Math.Max(size, 16));
        try
        {
            Unsafe.CopyBlockUnaligned(ref body[0], ref buffer[4], (uint)have);
            if (have < size)
            {
                if (async)
                {
                    await _stream.ReadExactlyAsync(body.AsMemory(have, size - have), token).ConfigureAwait(false);
                }
                else
                {
                    _stream.ReadExactly(body.AsSpan(have, size - have));
                }
            }
        }
        catch (Exception e)
        {
            ArrayPool<byte>.Shared.Return(body);
            if (e is EndOfStreamException)
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Transport, "the server closed the connection inside an answer", inner: e);
            }

            throw;
        }

        return new Response(body, size);
    }

    /// <summary>
    /// Sends the request in <see cref="Writer"/> and returns its answer. Any failure here leaves the
    /// stream in an unknown position, so the connection is closed: a request is never sent twice,
    /// because whether it ran is unknown. A timeout of 0 waits indefinitely; otherwise the wait is the
    /// timeout plus <see cref="AnswerGraceMs"/>.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<Response> Exchange(bool async, int timeoutSeconds, CancellationToken cancellation)
    {
        if (Broken)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Transport, "the connection is closed");
        }

        var timeoutMs = timeoutSeconds <= 0 ? Timeout.Infinite : checked(timeoutSeconds * 1000 + AnswerGraceMs);
        CancellationToken token = default;
        CancellationTokenRegistration registration = default;
        if (async)
        {
            if (timeoutMs != Timeout.Infinite)
            {
                _timeoutSource.CancelAfter(timeoutMs);
                token = _timeoutSource.Token;
                if (cancellation.CanBeCanceled)
                {
                    registration = cancellation.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _timeoutSource);
                }
            }
            else
            {
                token = cancellation;
            }
        }
        else if (_socketTimeoutMs != timeoutMs)
        {
            _socketTimeoutMs = timeoutMs;
            _socket.ReceiveTimeout = timeoutMs == Timeout.Infinite ? 0 : timeoutMs;
            _socket.SendTimeout = timeoutMs == Timeout.Infinite ? 0 : timeoutMs;
        }

        LastUsed = Environment.TickCount64;
        try
        {
            await Send(async, token).ConfigureAwait(false);
            var response = await ReadFrame(async, Settings.MaxMessageSize, token).ConfigureAwait(false);
            LastUsed = Environment.TickCount64;
            return response;
        }
        catch (Exception e)
        {
            Break();
            if (e is OperationCanceledException)
            {
                if (cancellation.IsCancellationRequested)
                {
                    throw new OperationCanceledException("the statement was cancelled; its connection was closed, and whether it ran is unknown", e, cancellation);
                }

                throw TimedOut(timeoutSeconds, e);
            }

            if (e is IOException { InnerException: SocketException { SocketErrorCode: SocketError.TimedOut } })
            {
                throw TimedOut(timeoutSeconds, e);
            }

            if (e is RasterizedDbException)
            {
                throw;
            }

            throw new RasterizedDbException(RasterizedDbErrorKind.Transport, $"the connection failed: {e.Message}", inner: e);
        }
        finally
        {
            if (async && timeoutMs != Timeout.Infinite)
            {
                registration.Dispose();
                _timeoutSource.CancelAfter(Timeout.Infinite);
                if (!_timeoutSource.TryReset())
                {
                    _timeoutSource.Dispose();
                    _timeoutSource = new CancellationTokenSource();
                }
            }

            Writer.Reset();
        }
    }

    private static RasterizedDbException TimedOut(int seconds, Exception inner) =>
        new(RasterizedDbErrorKind.Timeout, $"no answer came within {seconds} s (Command Timeout); the connection was closed, and whether the statement ran is unknown", inner: inner);

    /// <summary>Sends an empty simple query: keeps a quiet connection open, and says whether it still is.</summary>
    public async ValueTask<bool> Ping(bool async)
    {
        try
        {
            BeginRequest();
            Writer.Write("\u0001SIMPLE;"u8);
            var response = await Exchange(async, 10, default).ConfigureAwait(false);
            response.Return();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// Splits the notices off the front of an answer and hands them to <see cref="NoticeSink"/>.
    /// Returns where the answer itself starts.
    /// </summary>
    public int TakeNotices(ReadOnlySpan<byte> answer)
    {
        if (answer.Length == 0 || answer[0] != Answer.Notices)
        {
            return 0;
        }

        return TakeNoticesSlow(answer);
    }

    private int TakeNoticesSlow(ReadOnlySpan<byte> answer)
    {
        if (answer.Length < 5)
        {
            throw Truncated();
        }

        var count = BinaryPrimitives.ReadUInt32LittleEndian(answer[1..]);
        var at = 5;
        var sink = NoticeSink;
        // Bounded by the answer, never by the count it claims.
        for (uint i = 0; i < count; i++)
        {
            var severity = Field(answer, ref at);
            var sqlState = Field(answer, ref at);
            var message = Field(answer, ref at);
            sink?.Invoke(new RasterizedDbNotice(severity, sqlState, message));
        }

        if (at >= answer.Length)
        {
            throw Truncated();
        }

        return at;

        static string Field(ReadOnlySpan<byte> answer, ref int at)
        {
            var rest = answer[at..];
            var end = rest.IndexOf((byte)0);
            if (end < 0)
            {
                throw Truncated();
            }

            at += end + 1;
            return Encoding.UTF8.GetString(rest[..end]);
        }

        static RasterizedDbException Truncated() => new(RasterizedDbErrorKind.Protocol, "an answer's notices are truncated");
    }

    public void Break()
    {
        if (!Broken)
        {
            Broken = true;
            try
            {
                _stream.Dispose();
            }
            catch (Exception)
            {
                // Closing a failed stream may fail again.
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        Break();
        _socket.Dispose();
        _timeoutSource.Dispose();
        Writer.Dispose();
        ArrayPool<byte>.Shared.Return(_readBuffer);
        NoticeSink = null;
    }
}

/// <summary>The first byte of an answer.</summary>
internal static class Answer
{
    /// <summary>An acknowledgement, optionally followed by a <c>u64</c> count.</summary>
    public const byte Ack = 0;
    /// <summary>A <c>u64</c> count of rows affected.</summary>
    public const byte Count = 1;
    /// <summary>Rows, back to back.</summary>
    public const byte Rows = 2;
    /// <summary>An error's message, ending in <c> (SQLSTATE xxxxx)</c> when the server knew a code.</summary>
    public const byte Error = 3;
    /// <summary>PostgreSQL backend messages, ending with ReadyForQuery: the simple-query form.</summary>
    public const byte Simple = 4;
    /// <summary>Notices, then one of the answers above.</summary>
    public const byte Notices = 5;
}
