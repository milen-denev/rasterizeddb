using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;

namespace RasterizedDb.Messaging;

/// <summary>
/// A connection to RasterizedDB's message broker (<c>rdbmq/1</c>): queues and topics, producing,
/// subscriptions with credits and acknowledgements, exchanges. One connection carries any number of
/// requests and subscriptions at once; it is safe to use from many tasks.
/// </summary>
/// <remarks>
/// <para>
/// <b>Pre-production.</b> The protocol is pinned to the broker at commit <c>ee960c71</c> and is not
/// frozen; the broker itself has no tests yet.
/// </para>
/// <para>
/// A frame is <c>[u32 length][u8 op][u8 flags][u16 reserved][u32 corr][body]</c>, the length counting
/// what follows it. A reply carries its request's <c>corr</c>; a delivery the server pushes carries its
/// subscription's handle there. Strings are <c>u16</c> length + UTF-8, every integer little endian.
/// </para>
/// <para>
/// The connection does not reconnect by itself. When it is lost, every pending request fails, every
/// subscription ends with the error, and <see cref="ConnectionLost"/> is raised: connect again and
/// subscribe again. Messages delivered and not acknowledged are delivered again by the broker.
/// </para>
/// </remarks>
public sealed class RasterizedDbBroker : IAsyncDisposable
{
    private const ushort Version = 1;
    private const int FrameHeader = 12;
    private const int MaxFrame = (1 << 30) + 4096;
    private const byte FlagReply = 1;
    private const byte FlagError = 2;
    private const byte FlagPush = 4;

    internal static class Op
    {
        public const byte Hello = 1;
        public const byte Auth = 2;
        public const byte Ping = 3;
        public const byte TopicCreate = 10;
        public const byte TopicDelete = 11;
        public const byte TopicList = 12;
        public const byte Resolve = 14;
        public const byte Produce = 20;
        public const byte Fetch = 21;
        public const byte Subscribe = 30;
        public const byte Credit = 31;
        public const byte Ack = 32;
        public const byte Nack = 33;
        public const byte Commit = 34;
        public const byte Unsubscribe = 35;
        public const byte SubDelete = 36;
        public const byte ExchangeDeclare = 40;
        public const byte ExchangeDelete = 41;
        public const byte Bind = 42;
        public const byte Unbind = 43;
        public const byte Publish = 44;
        public const byte QueueDeclare = 45;
        public const byte QueueDelete = 46;
        public const byte Deliver = 0x80;
        public const byte Assign = 0x81;
        public const byte Closed = 0x82;
    }

    /// <summary>The partition of a produce that lets the message's key decide.</summary>
    public const uint ByKey = uint.MaxValue;

    /// <summary>The partition of a produce that lets the broker choose, round robin.</summary>
    public const uint AnyPartition = uint.MaxValue - 1;

    private readonly Stream _stream;
    private readonly TimeSpan _timeout;
    private readonly Channel<byte[]> _outgoing = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
    private readonly ConcurrentDictionary<uint, Waiter> _pending = new();
    private readonly ConcurrentDictionary<uint, BrokerSubscription> _subscriptions = new();
    private readonly ConcurrentDictionary<string, BrokerTopic> _topics = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _closing = new();
    private int _nextCorrelation;
    private BrokerException? _failure;

    private RasterizedDbBroker(Stream stream, TimeSpan timeout)
    {
        _stream = stream;
        _timeout = timeout;
    }

    /// <summary>Raised once, when the connection is lost (not when it is disposed).</summary>
    public event Action<BrokerException>? ConnectionLost;

    public bool IsConnected => _failure is null;

    public static Task<RasterizedDbBroker> ConnectAsync(string connectionString, CancellationToken cancellationToken = default) =>
        ConnectAsync(RasterizedDbBrokerOptions.Parse(connectionString), cancellationToken);

    public static async Task<RasterizedDbBroker> ConnectAsync(RasterizedDbBrokerOptions options, CancellationToken cancellationToken = default)
    {
        Socket socket;
        if (options.Socket is { } path)
        {
            socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(path), cancellationToken).ConfigureAwait(false);
        }
        else
        {
            socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            await socket.ConnectAsync(options.Host!, options.Port, cancellationToken).ConfigureAwait(false);
        }

        Stream stream = new NetworkStream(socket, ownsSocket: true);
        try
        {
            if (options.Tls && options.Socket is null)
            {
                var tls = new SslStream(stream, leaveInnerStreamOpen: false);
                stream = tls;
                await tls.AuthenticateAsClientAsync(
                    new SslClientAuthenticationOptions
                    {
                        TargetHost = options.Host,
                        RemoteCertificateValidationCallback = options.TrustServerCertificate ? static (_, _, _, _) => true : null,
                    },
                    cancellationToken).ConfigureAwait(false);
            }

            var broker = new RasterizedDbBroker(stream, options.Timeout);
            _ = Task.Run(broker.WriteLoop, CancellationToken.None);
            _ = Task.Run(broker.ReadLoop, CancellationToken.None);
            try
            {
                var hello = await broker.Call(Op.Hello, w =>
                {
                    w.UInt16(Version);
                    w.String(options.ClientName);
                }, cancellationToken).ConfigureAwait(false);
                var reader = new BodyReader(hello);
                reader.UInt16();
                reader.String();
                reader.UInt32();
                reader.UInt32();
                var authRequired = reader.Bool();
                if (options.Username is { } user)
                {
                    await broker.Call(Op.Auth, w =>
                    {
                        w.String(user);
                        w.String(options.Password ?? "");
                    }, cancellationToken).ConfigureAwait(false);
                }
                else if (authRequired)
                {
                    throw new BrokerException("the broker wants a login: set Username and Password", BrokerErrorCode.AuthRequired);
                }

                return broker;
            }
            catch
            {
                await broker.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch (Exception e) when (e is not BrokerException and not OperationCanceledException)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw new BrokerException($"could not connect to the broker: {e.Message}", inner: e);
        }
    }

    // ------------------------------------------------------------------ frames

    internal sealed class BodyWriter
    {
        private byte[] _buffer = new byte[256];
        private int _length = FrameHeader;

        private Span<byte> Take(int count)
        {
            if (_buffer.Length - _length < count)
            {
                Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, _length + count));
            }

            var span = _buffer.AsSpan(_length, count);
            _length += count;
            return span;
        }

        public void Byte(byte value) => Take(1)[0] = value;

        public void Bool(bool value) => Byte(value ? (byte)1 : (byte)0);

        public void UInt16(ushort value) => BinaryPrimitives.WriteUInt16LittleEndian(Take(2), value);

        public void UInt32(uint value) => BinaryPrimitives.WriteUInt32LittleEndian(Take(4), value);

        public void UInt64(ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(Take(8), value);

        public void Int64(long value) => BinaryPrimitives.WriteInt64LittleEndian(Take(8), value);

        public void String(string value)
        {
            var count = Encoding.UTF8.GetByteCount(value);
            if (count > ushort.MaxValue)
            {
                throw new ArgumentException("a name is at most 65535 bytes");
            }

            UInt16((ushort)count);
            Encoding.UTF8.GetBytes(value, Take(count));
        }

        public void Blob(ReadOnlySpan<byte> value)
        {
            UInt32((uint)value.Length);
            value.CopyTo(Take(value.Length));
        }

        public void Raw(ReadOnlySpan<byte> value) => value.CopyTo(Take(value.Length));

        /// <summary>The finished frame: the header filled in, the array cut to its length.</summary>
        public byte[] Frame(byte op, uint correlation)
        {
            var frame = _buffer.Length == _length ? _buffer : _buffer.AsSpan(0, _length).ToArray();
            BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)(_length - 4));
            frame[4] = op;
            frame[5] = 0;
            frame[6] = 0;
            frame[7] = 0;
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(8), correlation);
            return frame;
        }
    }

    internal ref struct BodyReader(ReadOnlySpan<byte> body)
    {
        private ReadOnlySpan<byte> _rest = body;

        private ReadOnlySpan<byte> Take(int count)
        {
            if (_rest.Length < count)
            {
                throw new BrokerException("malformed frame: body too short");
            }

            var taken = _rest[..count];
            _rest = _rest[count..];
            return taken;
        }

        public int Remaining => _rest.Length;

        public byte Byte() => Take(1)[0];

        public bool Bool() => Byte() != 0;

        public ushort UInt16() => BinaryPrimitives.ReadUInt16LittleEndian(Take(2));

        public uint UInt32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));

        public ulong UInt64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));

        public long Int64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));

        public string String() => Encoding.UTF8.GetString(Take(UInt16()));
    }

    /// <summary>A request waiting for its reply. <see cref="OnReply"/> runs on the read loop, before the next frame is read.</summary>
    private sealed class Waiter(Action<byte[]>? onReply) : TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        public readonly Action<byte[]>? OnReply = onReply;
    }

    private static BrokerException Closed() => new("the broker connection is closed");

    private uint NextCorrelation()
    {
        while (true)
        {
            var value = unchecked((uint)Interlocked.Increment(ref _nextCorrelation));
            if (value != 0)
            {
                return value;
            }
        }
    }

    /// <summary>Queues a frame nobody waits for the reply of (an acknowledgement, a credit).</summary>
    internal void Send(byte op, uint correlation, Action<BodyWriter> body)
    {
        if (_failure is { } failure)
        {
            throw failure;
        }

        var writer = new BodyWriter();
        body(writer);
        if (!_outgoing.Writer.TryWrite(writer.Frame(op, correlation)))
        {
            throw _failure ?? Closed();
        }
    }

    internal async Task<byte[]> Call(byte op, Action<BodyWriter> body, CancellationToken cancellation, Action<byte[]>? onReply = null)
    {
        if (_failure is { } failure)
        {
            throw failure;
        }

        var correlation = NextCorrelation();
        var writer = new BodyWriter();
        body(writer);
        var waiter = new Waiter(onReply);
        _pending[correlation] = waiter;
        try
        {
            if (!_outgoing.Writer.TryWrite(writer.Frame(op, correlation)))
            {
                throw _failure ?? Closed();
            }

            return await waiter.Task.WaitAsync(_timeout, cancellation).ConfigureAwait(false);
        }
        catch (TimeoutException e)
        {
            throw new BrokerException($"no reply within {_timeout.TotalSeconds:0.#} s", inner: e);
        }
        finally
        {
            _pending.TryRemove(correlation, out _);
        }
    }

    private async Task WriteLoop()
    {
        try
        {
            var reader = _outgoing.Reader;
            var batch = new MemoryStream(64 * 1024);
            while (await reader.WaitToReadAsync(_closing.Token).ConfigureAwait(false))
            {
                // Whatever is waiting leaves in one write.
                batch.SetLength(0);
                while (batch.Length < 1 << 20 && reader.TryRead(out var frame))
                {
                    batch.Write(frame);
                }

                await _stream.WriteAsync(batch.GetBuffer().AsMemory(0, (int)batch.Length), _closing.Token).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            Fail(e);
        }
    }

    private async Task ReadLoop()
    {
        var header = new byte[FrameHeader];
        try
        {
            while (true)
            {
                await _stream.ReadExactlyAsync(header, _closing.Token).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadUInt32LittleEndian(header);
                if (length < FrameHeader - 4 || length > MaxFrame)
                {
                    throw new BrokerException("malformed frame: length out of range");
                }

                var body = new byte[length - (FrameHeader - 4)];
                await _stream.ReadExactlyAsync(body, _closing.Token).ConfigureAwait(false);
                Route(header[4], header[5], BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(8)), body);
            }
        }
        catch (Exception e)
        {
            Fail(e);
        }
    }

    private static BrokerException ErrorOf(byte[] body)
    {
        try
        {
            var reader = new BodyReader(body);
            var code = reader.UInt16();
            return new BrokerException(reader.String(), Enum.IsDefined((BrokerErrorCode)code) ? (BrokerErrorCode)code : BrokerErrorCode.Unknown);
        }
        catch (BrokerException e)
        {
            return e;
        }
    }

    private void Route(byte op, byte flags, uint correlation, byte[] body)
    {
        if ((flags & FlagPush) != 0)
        {
            if (!_subscriptions.TryGetValue(correlation, out var subscription))
            {
                return;
            }

            switch (op)
            {
                case Op.Deliver:
                    subscription.Deliver(body);
                    break;
                case Op.Assign:
                    subscription.Assign(body);
                    break;
                case Op.Closed:
                    _subscriptions.TryRemove(correlation, out _);
                    subscription.End(ErrorOf(body));
                    break;
            }

            return;
        }

        if ((flags & FlagReply) == 0)
        {
            return;
        }

        var failed = (flags & FlagError) != 0;
        if (_pending.TryRemove(correlation, out var waiter))
        {
            if (failed)
            {
                waiter.TrySetException(ErrorOf(body));
            }
            else
            {
                try
                {
                    waiter.OnReply?.Invoke(body);
                    waiter.TrySetResult(body);
                }
                catch (Exception e)
                {
                    waiter.TrySetException(e);
                }
            }
        }
        else if (failed && _subscriptions.TryRemove(correlation, out var subscription))
        {
            // A frame nobody waited for (an acknowledgement, a credit) that failed: its subscription is told.
            subscription.End(ErrorOf(body));
        }
    }

    private void Fail(Exception cause)
    {
        var disposing = _closing.IsCancellationRequested;
        var failure = cause as BrokerException
            ?? new BrokerException(disposing ? "the broker connection is closed" : $"the broker connection was lost: {cause.Message}", inner: cause);
        if (Interlocked.CompareExchange(ref _failure, failure, null) is not null)
        {
            return;
        }

        _outgoing.Writer.TryComplete();
        foreach (var correlation in _pending.Keys)
        {
            if (_pending.TryRemove(correlation, out var waiter))
            {
                waiter.TrySetException(failure);
            }
        }

        foreach (var handle in _subscriptions.Keys)
        {
            if (_subscriptions.TryRemove(handle, out var subscription))
            {
                subscription.End(failure);
            }
        }

        try
        {
            _stream.Dispose();
        }
        catch (Exception)
        {
            // Closing a failed stream may fail again.
        }

        if (!disposing)
        {
            ConnectionLost?.Invoke(failure);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_closing.IsCancellationRequested)
        {
            await _closing.CancelAsync().ConfigureAwait(false);
            Fail(Closed());
        }
    }

    public async Task PingAsync(CancellationToken cancellationToken = default) => await Call(Op.Ping, static _ => { }, cancellationToken).ConfigureAwait(false);

    // ------------------------------------------------------------ topics, queues

    private static void Write(BodyWriter w, BrokerTopicConfig config)
    {
        w.UInt32(config.Partitions);
        w.Byte((byte)config.Durability);
        w.Int64(config.Retention is { } retention ? (long)retention.TotalMilliseconds : -1);
        w.Int64(config.RetentionBytes);
        w.UInt64(config.SegmentBytes);
        w.UInt32(config.MaxBatchBytes);
        w.UInt64(config.MemoryBytes);
    }

    private static BrokerTopicConfig ReadTopicConfig(ref BodyReader r)
    {
        var partitions = r.UInt32();
        var durability = (BrokerDurability)r.Byte();
        var retention = r.Int64();
        return new BrokerTopicConfig
        {
            Partitions = partitions,
            Durability = durability,
            Retention = retention < 0 ? null : TimeSpan.FromMilliseconds(retention),
            RetentionBytes = r.Int64(),
            SegmentBytes = r.UInt64(),
            MaxBatchBytes = r.UInt32(),
            MemoryBytes = r.UInt64(),
        };
    }

    /// <summary>The topic or queue of a name, by the id the broker gave it. Asked once per name and kept.</summary>
    public async Task<BrokerTopic> ResolveAsync(string name, CancellationToken cancellationToken = default)
    {
        if (_topics.TryGetValue(name, out var known))
        {
            return known;
        }

        var reply = await Call(Op.Resolve, w => w.String(name), cancellationToken).ConfigureAwait(false);
        var reader = new BodyReader(reply);
        var topic = new BrokerTopic(name, reader.UInt32(), reader.UInt32(), (BrokerDurability)reader.Byte(), reader.Bool());
        _topics[name] = topic;
        return topic;
    }

    public async Task<BrokerTopic> CreateTopicAsync(string name, BrokerTopicConfig? config = null, bool ifNotExists = true, CancellationToken cancellationToken = default)
    {
        await Call(Op.TopicCreate, w =>
        {
            w.String(name);
            Write(w, config ?? new BrokerTopicConfig());
            w.Bool(ifNotExists);
        }, cancellationToken).ConfigureAwait(false);
        _topics.TryRemove(name, out _);
        return await ResolveAsync(name, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteTopicAsync(string name, bool ifExists = true, CancellationToken cancellationToken = default)
    {
        var reply = await Call(Op.TopicDelete, w =>
        {
            w.String(name);
            w.Bool(ifExists);
        }, cancellationToken).ConfigureAwait(false);
        _topics.TryRemove(name, out _);
        return new BodyReader(reply).Bool();
    }

    /// <summary>Every topic and queue, with its depth: for monitoring.</summary>
    public async Task<IReadOnlyList<BrokerTopicInfo>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        var reply = await Call(Op.TopicList, static _ => { }, cancellationToken).ConfigureAwait(false);
        var reader = new BodyReader(reply);
        var count = reader.UInt32();
        var topics = new List<BrokerTopicInfo>((int)Math.Min(count, 4096));
        for (uint i = 0; i < count; i++)
        {
            var id = reader.UInt32();
            var name = reader.String();
            var config = ReadTopicConfig(ref reader);
            topics.Add(new BrokerTopicInfo(id, name, config, reader.Bool(), reader.UInt64(), reader.UInt64()));
        }

        return topics;
    }

    /// <summary>
    /// Declares a queue: a one-partition topic and a shared subscription of the same name. Declared
    /// again with other settings, it is refused (<see cref="BrokerErrorCode.Mismatch"/>).
    /// </summary>
    public async Task<BrokerTopic> DeclareQueueAsync(string name, BrokerQueueConfig? config = null, bool ifNotExists = true, CancellationToken cancellationToken = default)
    {
        config ??= new BrokerQueueConfig();
        await Call(Op.QueueDeclare, w =>
        {
            w.String(name);
            w.Byte((byte)config.Durability);
            w.UInt32((uint)config.AckTimeout.TotalMilliseconds);
            w.UInt32(config.MaxDeliveries);
            w.String(config.DeadLetter);
            w.Int64(config.MessageTtl is { } ttl ? (long)ttl.TotalMilliseconds : -1);
            w.Int64(config.MaxLength);
            w.Bool(ifNotExists);
        }, cancellationToken).ConfigureAwait(false);
        _topics.TryRemove(name, out _);
        return await ResolveAsync(name, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteQueueAsync(string name, bool ifExists = true, CancellationToken cancellationToken = default)
    {
        var reply = await Call(Op.QueueDelete, w =>
        {
            w.String(name);
            w.Bool(ifExists);
        }, cancellationToken).ConfigureAwait(false);
        _topics.TryRemove(name, out _);
        return new BodyReader(reply).Bool();
    }

    // ---------------------------------------------------------------- producing

    /// <summary>
    /// Produces a sealed batch. With <see cref="BrokerAck.None"/> nothing is waited for and the result's
    /// offset is <see cref="ulong.MaxValue"/>.
    /// </summary>
    public async Task<BrokerProduceResult> ProduceAsync(
        BrokerTopic topic,
        ReadOnlyMemory<byte> batch,
        uint partition = ByKey,
        BrokerAck ack = BrokerAck.Durable,
        CancellationToken cancellationToken = default)
    {
        void Body(BodyWriter w)
        {
            w.UInt32(topic.Id);
            w.UInt32(partition);
            w.Byte((byte)ack);
            w.Raw(batch.Span);
        }

        if (ack == BrokerAck.None)
        {
            Send(Op.Produce, NextCorrelation(), Body);
            return new BrokerProduceResult(partition, ulong.MaxValue, false);
        }

        var reply = await Call(Op.Produce, Body, cancellationToken).ConfigureAwait(false);
        var reader = new BodyReader(reply);
        return new BrokerProduceResult(reader.UInt32(), reader.UInt64(), reader.Bool());
    }

    /// <summary>Produces one message to a topic or queue by name; durable unless <paramref name="ack"/> says otherwise.</summary>
    public async Task<BrokerProduceResult> ProduceAsync(
        string topic,
        ReadOnlyMemory<byte> value,
        IReadOnlyList<BrokerHeader>? headers = null,
        ReadOnlyMemory<byte>? key = null,
        BrokerAck ack = BrokerAck.Durable,
        CancellationToken cancellationToken = default)
    {
        var resolved = await ResolveAsync(topic, cancellationToken).ConfigureAwait(false);
        using var builder = new BrokerBatchBuilder();
        builder.Add(value.Span, key is { } k ? k.Span : default, key is not null, headers);
        // A message without a key has no partition of its own: the broker chooses.
        return await ProduceAsync(resolved, builder.Seal(), key is null ? AnyPartition : ByKey, ack, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The stored messages of one partition from <paramref name="offset"/> on, without a subscription:
    /// up to <paramref name="maxBytes"/>, waiting up to <paramref name="maxWait"/> for any to arrive.
    /// </summary>
    public async Task<(IReadOnlyList<BrokerMessage> Messages, ulong NextOffset, ulong LogStart)> FetchAsync(
        BrokerTopic topic,
        uint partition,
        ulong offset,
        int maxBytes = 1 << 20,
        TimeSpan maxWait = default,
        CancellationToken cancellationToken = default)
    {
        var reply = await Call(Op.Fetch, w =>
        {
            w.UInt32(topic.Id);
            w.UInt32(partition);
            w.UInt64(offset);
            w.UInt32((uint)maxBytes);
            w.UInt32((uint)Math.Min(maxWait.TotalMilliseconds, uint.MaxValue));
            w.UInt32(1);
        }, cancellationToken).ConfigureAwait(false);
        var reader = new BodyReader(reply);
        var from = reader.UInt32();
        var next = reader.UInt64();
        var start = reader.UInt64();
        var messages = new List<BrokerMessage>();
        // A batch may start before the offset asked for: its earlier records are skipped.
        BrokerBatch.Read(reply.AsMemory(reply.Length - reader.Remaining), from, o => o >= offset, messages);
        return (messages, messages.Count > 0 ? messages[^1].Offset + 1 : next, start);
    }

    // ------------------------------------------------------------- subscriptions

    /// <summary>
    /// Subscribes to a topic. The broker pushes up to <paramref name="credits"/> messages and then
    /// waits; the subscription grants more as messages are taken from it, so about that many are ever
    /// held here unread.
    /// </summary>
    public async Task<BrokerSubscription> SubscribeAsync(
        string topic,
        string subscriptionName,
        BrokerSubscriptionConfig? config = null,
        int credits = 256,
        CancellationToken cancellationToken = default)
    {
        config ??= new BrokerSubscriptionConfig();
        ArgumentOutOfRangeException.ThrowIfLessThan(credits, 1);
        var resolved = await ResolveAsync(topic, cancellationToken).ConfigureAwait(false);
        BrokerSubscription? subscription = null;
        await Call(
            Op.Subscribe,
            w =>
            {
                w.UInt32(resolved.Id);
                w.String(subscriptionName);
                w.Byte((byte)config.Mode);
                w.Byte(config.Start.Kind);
                w.Int64(config.Start.Value);
                w.UInt32((uint)config.AckTimeout.TotalMilliseconds);
                w.UInt32(config.MaxDeliveries);
                w.String(config.DeadLetter);
                w.UInt32((uint)credits);
            },
            cancellationToken,
            reply =>
            {
                // On the read loop: the subscription is known before the frame after the reply is
                // read, which may already be its first delivery.
                var reader = new BodyReader(reply);
                var handle = reader.UInt32();
                var assigned = new uint[Math.Min(reader.UInt32(), (uint)reader.Remaining / 4)];
                for (var i = 0; i < assigned.Length; i++)
                {
                    assigned[i] = reader.UInt32();
                }

                subscription = new BrokerSubscription(this, handle, assigned, credits);
                _subscriptions[handle] = subscription;
            }).ConfigureAwait(false);
        // Lost in the instant after the reply.
        if (_failure is { } failure && _subscriptions.TryRemove(subscription!.Handle, out _))
        {
            subscription.End(failure);
        }

        return subscription!;
    }

    /// <summary>Consumes a queue: the shared subscription a queue is declared with.</summary>
    public Task<BrokerSubscription> ConsumeAsync(string queue, int credits = 256, CancellationToken cancellationToken = default) =>
        SubscribeAsync(queue, queue, new BrokerSubscriptionConfig { Mode = BrokerSubscriptionMode.Shared }, credits, cancellationToken);

    internal void Forget(uint handle) => _subscriptions.TryRemove(handle, out _);

    public async Task<bool> DeleteSubscriptionAsync(string topic, string subscriptionName, bool ifExists = true, CancellationToken cancellationToken = default)
    {
        var resolved = await ResolveAsync(topic, cancellationToken).ConfigureAwait(false);
        var reply = await Call(Op.SubDelete, w =>
        {
            w.UInt32(resolved.Id);
            w.String(subscriptionName);
            w.Bool(ifExists);
        }, cancellationToken).ConfigureAwait(false);
        return new BodyReader(reply).Bool();
    }

    // ---------------------------------------------------------------- exchanges

    public async Task DeclareExchangeAsync(string name, BrokerExchangeKind kind, bool durable = true, string? alternate = null, CancellationToken cancellationToken = default) =>
        await Call(Op.ExchangeDeclare, w =>
        {
            w.String(name);
            w.Byte((byte)kind);
            w.Bool(durable);
            w.String(alternate ?? "");
        }, cancellationToken).ConfigureAwait(false);

    public async Task<bool> DeleteExchangeAsync(string name, bool ifExists = true, CancellationToken cancellationToken = default)
    {
        var reply = await Call(Op.ExchangeDelete, w =>
        {
            w.String(name);
            w.Bool(ifExists);
        }, cancellationToken).ConfigureAwait(false);
        return new BodyReader(reply).Bool();
    }

    private async Task<bool> Binding(byte op, string exchange, string topic, string key, bool matchAny, IReadOnlyList<BrokerHeader>? arguments, CancellationToken cancellation)
    {
        var reply = await Call(op, w =>
        {
            w.String(exchange);
            w.String(topic);
            w.String(key);
            w.Byte(matchAny ? (byte)1 : (byte)0);
            w.UInt16((ushort)(arguments?.Count ?? 0));
            if (arguments is not null)
            {
                foreach (var argument in arguments)
                {
                    w.String(argument.Name);
                    w.Blob(argument.Value.Span);
                }
            }
        }, cancellation).ConfigureAwait(false);
        return new BodyReader(reply).Bool();
    }

    /// <summary>Binds a topic or queue to an exchange under a binding key (for a headers exchange, the header values to match).</summary>
    public Task<bool> BindAsync(string exchange, string topic, string key = "", bool matchAny = false, IReadOnlyList<BrokerHeader>? arguments = null, CancellationToken cancellationToken = default) =>
        Binding(Op.Bind, exchange, topic, key, matchAny, arguments, cancellationToken);

    public Task<bool> UnbindAsync(string exchange, string topic, string key = "", bool matchAny = false, IReadOnlyList<BrokerHeader>? arguments = null, CancellationToken cancellationToken = default) =>
        Binding(Op.Unbind, exchange, topic, key, matchAny, arguments, cancellationToken);

    /// <summary>
    /// Publishes a sealed batch through an exchange; returns how many topics took it. With
    /// <paramref name="mandatory"/>, a batch no binding routes anywhere is refused
    /// (<see cref="BrokerErrorCode.Unroutable"/>).
    /// </summary>
    public async Task<uint> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> batch, BrokerAck ack = BrokerAck.Durable, bool mandatory = false, CancellationToken cancellationToken = default)
    {
        void Body(BodyWriter w)
        {
            w.String(exchange);
            w.String(routingKey);
            w.Byte((byte)ack);
            w.Bool(mandatory);
            w.Raw(batch.Span);
        }

        if (ack == BrokerAck.None)
        {
            Send(Op.Publish, NextCorrelation(), Body);
            return 0;
        }

        return new BodyReader(await Call(Op.Publish, Body, cancellationToken).ConfigureAwait(false)).UInt32();
    }
}

/// <summary>
/// A subscription: messages as the broker pushes them, and the acknowledgements that settle them.
/// Read it with <c>await foreach</c>. It ends when it is closed, when the broker closes it, or when
/// the connection is lost; the last two throw from the enumeration.
/// </summary>
public sealed class BrokerSubscription : IAsyncEnumerable<BrokerMessage>, IAsyncDisposable
{
    private readonly RasterizedDbBroker _broker;
    private readonly Channel<byte[]> _deliveries = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleWriter = true });
    private readonly uint _window;
    private uint _consumed;
    private volatile uint[] _assigned;
    private int _closed;

    internal BrokerSubscription(RasterizedDbBroker broker, uint handle, uint[] assigned, int credits)
    {
        _broker = broker;
        Handle = handle;
        _assigned = assigned;
        _window = (uint)credits;
    }

    /// <summary>The subscription's handle on this connection.</summary>
    public uint Handle { get; }

    /// <summary>The partitions this member reads, for an ordered subscription; the broker may change them.</summary>
    public IReadOnlyList<uint> AssignedPartitions => _assigned;

    /// <summary>Raised when the subscription ends other than by <see cref="CloseAsync"/>.</summary>
    public event Action<BrokerException>? Closed;

    internal void Deliver(byte[] body) => _deliveries.Writer.TryWrite(body);

    internal void Assign(byte[] body)
    {
        var reader = new RasterizedDbBroker.BodyReader(body);
        reader.UInt64();
        var partitions = new uint[reader.UInt32()];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = reader.UInt32();
        }

        _assigned = partitions;
    }

    internal void End(BrokerException error)
    {
        if (_deliveries.Writer.TryComplete(error) && Volatile.Read(ref _closed) == 0)
        {
            Closed?.Invoke(error);
        }
    }

    /// <summary>
    /// The messages, in the order the broker pushed them. Taking messages is what grants the broker
    /// more credits: once half the window has been taken, that many are granted again.
    /// </summary>
    public async IAsyncEnumerator<BrokerMessage> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var messages = new List<BrokerMessage>();
        await foreach (var body in _deliveries.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            // partition u32 | range count u32 | (partition u32, offset u64, count u32)… | stored batches
            if (body.Length < 8 || BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(4)) > (uint)(body.Length - 8) / 16)
            {
                throw new BrokerException("malformed delivery: body too short");
            }

            var partition = BinaryPrimitives.ReadUInt32LittleEndian(body);
            var rangeCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(4));
            var ranges = new (ulong Offset, uint Count)[rangeCount];
            uint delivered = 0;
            for (var i = 0; i < rangeCount; i++)
            {
                var at = 8 + i * 16;
                ranges[i] = (BinaryPrimitives.ReadUInt64LittleEndian(body.AsSpan(at + 4)), BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(at + 12)));
                delivered += ranges[i].Count;
            }

            messages.Clear();
            // Records outside the ranges are another member's, or before this one's position.
            BrokerBatch.Read(
                body.AsMemory(8 + rangeCount * 16),
                partition,
                offset =>
                {
                    foreach (var (start, count) in ranges)
                    {
                        if (offset >= start && offset < start + count)
                        {
                            return true;
                        }
                    }

                    return false;
                },
                messages);

            _consumed += delivered;
            if (_consumed >= Math.Max(1, _window / 2))
            {
                var grant = _consumed;
                _consumed = 0;
                try
                {
                    _broker.Send(RasterizedDbBroker.Op.Credit, Handle, w =>
                    {
                        w.UInt32(Handle);
                        w.UInt32(grant);
                    });
                }
                catch (BrokerException)
                {
                    // The connection is gone; the enumeration ends with its error.
                }
            }

            foreach (var message in messages)
            {
                yield return message;
            }
        }
    }

    private void Settle(byte op, uint delayMs, IReadOnlyList<(uint Partition, ulong Offset, uint Count)> ranges) =>
        _broker.Send(op, Handle, w =>
        {
            w.UInt32(Handle);
            if (op == RasterizedDbBroker.Op.Nack)
            {
                w.UInt32(delayMs);
            }

            w.UInt32((uint)ranges.Count);
            foreach (var (partition, offset, count) in ranges)
            {
                w.UInt32(partition);
                w.UInt64(offset);
                w.UInt32(count);
            }
        });

    /// <summary>Messages as the fewest ranges that cover them: neighbours in a partition become one range.</summary>
    private static List<(uint Partition, ulong Offset, uint Count)> Ranges(IEnumerable<BrokerMessage> messages)
    {
        var ranges = new List<(uint Partition, ulong Offset, uint Count)>();
        foreach (var message in messages.OrderBy(m => m.Partition).ThenBy(m => m.Offset))
        {
            if (ranges.Count > 0 && ranges[^1] is var last && last.Partition == message.Partition && last.Offset + last.Count == message.Offset)
            {
                ranges[^1] = (last.Partition, last.Offset, last.Count + 1);
            }
            else if (ranges.Count == 0 || ranges[^1].Partition != message.Partition || ranges[^1].Offset + ranges[^1].Count <= message.Offset)
            {
                ranges.Add((message.Partition, message.Offset, 1));
            }
        }

        return ranges;
    }

    /// <summary>Acknowledges a message: it is done, and is not delivered again. Not waited for.</summary>
    public void Ack(BrokerMessage message) => Settle(RasterizedDbBroker.Op.Ack, 0, [(message.Partition, message.Offset, 1)]);

    /// <summary>Acknowledges several messages in one frame.</summary>
    public void Ack(IEnumerable<BrokerMessage> messages) => Settle(RasterizedDbBroker.Op.Ack, 0, Ranges(messages));

    /// <summary>
    /// Gives a message back: it is delivered again, after <paramref name="delay"/> if one is given, and
    /// counts towards the subscription's maximum deliveries.
    /// </summary>
    public void Nack(BrokerMessage message, TimeSpan delay = default) =>
        Settle(RasterizedDbBroker.Op.Nack, (uint)Math.Min(delay.TotalMilliseconds, uint.MaxValue), [(message.Partition, message.Offset, 1)]);

    public void Nack(IEnumerable<BrokerMessage> messages, TimeSpan delay = default) =>
        Settle(RasterizedDbBroker.Op.Nack, (uint)Math.Min(delay.TotalMilliseconds, uint.MaxValue), Ranges(messages));

    /// <summary>For an ordered subscription: records that everything before <paramref name="offset"/> in a partition is done.</summary>
    public async Task CommitAsync(uint partition, ulong offset, CancellationToken cancellationToken = default) =>
        await _broker.Call(RasterizedDbBroker.Op.Commit, w =>
        {
            w.UInt32(Handle);
            w.UInt32(partition);
            w.UInt64(offset);
        }, cancellationToken).ConfigureAwait(false);

    /// <summary>Ends the subscription. Messages delivered and not acknowledged go to another member, or wait.</summary>
    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return;
        }

        _broker.Forget(Handle);
        _deliveries.Writer.TryComplete();
        if (_broker.IsConnected)
        {
            try
            {
                await _broker.Call(RasterizedDbBroker.Op.Unsubscribe, w => w.UInt32(Handle), cancellationToken).ConfigureAwait(false);
            }
            catch (BrokerException)
            {
                // The connection went first, which ended the subscription too.
            }
        }
    }

    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);
}
