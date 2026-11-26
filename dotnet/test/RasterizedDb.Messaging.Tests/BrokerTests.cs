using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RasterizedDb.Messaging;

namespace RasterizedDb.Messaging.Tests;

public class BatchTests
{
    [Fact]
    public void A_sealed_batch_reads_back_with_keys_and_headers()
    {
        using var builder = new BrokerBatchBuilder();
        builder.Add("one"u8);
        builder.Add("two"u8, "k"u8, [new BrokerHeader("trace", "abc"), new BrokerHeader("n", new byte[] { 1, 2, 3 })]);
        builder.Add(ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty, hasKey: true);
        var sealedBatch = builder.Seal().ToArray();

        Assert.Equal(0, sealedBatch.Length % 8);
        var messages = new List<BrokerMessage>();
        BrokerBatch.Read(sealedBatch, 7, null, messages);

        Assert.Equal(3, messages.Count);
        Assert.Equal("one", Encoding.UTF8.GetString(messages[0].Value.Span));
        Assert.False(messages[0].HasKey);
        Assert.Empty(messages[0].Headers);
        Assert.Equal(7u, messages[0].Partition);
        Assert.Equal(0ul, messages[0].Offset);

        Assert.True(messages[1].HasKey);
        Assert.Equal("k", Encoding.UTF8.GetString(messages[1].Key.Span));
        Assert.Equal("two", Encoding.UTF8.GetString(messages[1].Value.Span));
        Assert.Equal("abc", messages[1].Header("trace"));
        Assert.Equal(new byte[] { 1, 2, 3 }, messages[1].Headers[1].Value.ToArray());
        Assert.Null(messages[1].Header("absent"));
        Assert.Equal(1ul, messages[1].Offset);

        Assert.True(messages[2].HasKey);
        Assert.Equal(0, messages[2].Key.Length);
        Assert.Equal(0, messages[2].Value.Length);
    }

    [Fact]
    public void Offsets_follow_the_base_offset_and_the_filter_drops_records()
    {
        using var builder = new BrokerBatchBuilder();
        builder.Add("a").Add("b").Add("c");
        var batch = Stamp(builder.Seal().ToArray(), 100, 1_700_000_000_000_000);

        var messages = new List<BrokerMessage>();
        BrokerBatch.Read(batch, 0, o => o != 101, messages);

        Assert.Equal([100ul, 102ul], messages.Select(m => m.Offset));
        Assert.Equal(DateTime.UnixEpoch.AddTicks(1_700_000_000_000_000 * 10), messages[0].Time);
        Assert.Equal(DateTimeKind.Utc, messages[0].Time.Kind);
    }

    [Fact]
    public void Two_batches_back_to_back_are_both_read()
    {
        using var builder = new BrokerBatchBuilder();
        var first = Stamp(builder.Add("a").Seal().ToArray(), 5, 0);
        builder.Reset();
        var second = Stamp(builder.Add("b").Add("c").Seal().ToArray(), 6, 0);

        var messages = new List<BrokerMessage>();
        BrokerBatch.Read(first.Concat(second).ToArray(), 0, null, messages);
        Assert.Equal([5ul, 6ul, 7ul], messages.Select(m => m.Offset));
    }

    [Fact]
    public void A_damaged_batch_is_refused()
    {
        using var builder = new BrokerBatchBuilder();
        var batch = builder.Add("payload").Seal().ToArray();

        var records = (byte[])batch.Clone();
        records[^9] ^= 1;
        Assert.Contains("records CRC", Assert.Throws<BrokerException>(() => BrokerBatch.Read(records, 0, null, [])).Message);

        var header = (byte[])batch.Clone();
        header[BrokerBatch.OCount] ^= 1;
        Assert.Contains("header CRC", Assert.Throws<BrokerException>(() => BrokerBatch.Read(header, 0, null, [])).Message);

        var magic = (byte[])batch.Clone();
        magic[0] = (byte)'X';
        Assert.Contains("magic", Assert.Throws<BrokerException>(() => BrokerBatch.Read(magic, 0, null, [])).Message);
    }

    [Fact]
    public void A_builder_is_reusable_and_refuses_what_it_cannot_hold()
    {
        using var builder = new BrokerBatchBuilder();
        Assert.Throws<InvalidOperationException>(() => builder.Seal());
        builder.Add("x");
        var one = builder.Seal().ToArray();
        Assert.Throws<InvalidOperationException>(() => builder.Add("y"));
        builder.Reset();
        Assert.Equal(0, builder.Count);
        builder.Add("x");
        Assert.Equal(one, builder.Seal().ToArray());
    }

    [Fact]
    public void A_connection_string_is_parsed_and_checked()
    {
        var options = RasterizedDbBrokerOptions.Parse("Host=mq.internal;Port=7200;Username=app;Password=p;Timeout=5;Tls=true");
        Assert.Equal("mq.internal", options.Host);
        Assert.Equal(7200, options.Port);
        Assert.Equal("app", options.Username);
        Assert.Equal(TimeSpan.FromSeconds(5), options.Timeout);
        Assert.True(options.Tls);

        Assert.Equal("/run/mq.sock", RasterizedDbBrokerOptions.Parse("Host=unix:/run/mq.sock").Socket);
        Assert.Equal(RasterizedDbBrokerOptions.DefaultPort, RasterizedDbBrokerOptions.Parse("Host=h").Port);
        Assert.Throws<ArgumentException>(() => RasterizedDbBrokerOptions.Parse("Port=1"));
        Assert.Throws<ArgumentException>(() => RasterizedDbBrokerOptions.Parse("Host=h;Nonsense=1"));
    }

    /// <summary>What the broker does on append: gives the batch its offset and time, and seals the header again.</summary>
    internal static byte[] Stamp(byte[] batch, ulong baseOffset, long baseTimeMicros)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(batch.AsSpan(BrokerBatch.OBaseOffset), baseOffset);
        BinaryPrimitives.WriteInt64LittleEndian(batch.AsSpan(BrokerBatch.OBaseTime), baseTimeMicros);
        BinaryPrimitives.WriteUInt32LittleEndian(batch.AsSpan(BrokerBatch.OCrc), System.IO.Hashing.Crc32.HashToUInt32(batch.AsSpan(BrokerBatch.OFlags, BrokerBatch.Header - BrokerBatch.OFlags)));
        return batch;
    }
}

/// <summary>
/// A broker that is a socket and a script: it answers what the client under test sends, frame by frame,
/// and records what it was sent.
/// </summary>
internal sealed class FakeBroker : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private Socket? _socket;
    private readonly SemaphoreSlim _write = new(1, 1);

    public readonly System.Collections.Concurrent.BlockingCollection<(byte Op, uint Corr, byte[] Body)> Received = new();
    public bool AuthRequired;
    public Func<byte, uint, byte[], Task>? OnFrame;
    public readonly Dictionary<string, List<byte[]>> Queues = new();
    public ulong NextOffset = 10;

    public FakeBroker()
    {
        _listener.Start();
        _ = Task.Run(Serve);
    }

    public string ConnectionString => $"Host=127.0.0.1;Port={((IPEndPoint)_listener.LocalEndpoint).Port};Timeout=5";

    public static byte[] Body(Action<BinaryWriter> write)
    {
        var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        write(writer);
        writer.Flush();
        return stream.ToArray();
    }

    public static void Str(BinaryWriter w, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        w.Write((ushort)bytes.Length);
        w.Write(bytes);
    }

    public static string Str(byte[] body, ref int at)
    {
        var length = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(at));
        var value = Encoding.UTF8.GetString(body, at + 2, length);
        at += 2 + length;
        return value;
    }

    public async Task Send(byte op, byte flags, uint corr, byte[] body)
    {
        var frame = new byte[12 + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)(8 + body.Length));
        frame[4] = op;
        frame[5] = flags;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(8), corr);
        body.CopyTo(frame, 12);
        await _write.WaitAsync();
        try
        {
            await _socket!.SendAsync(frame);
        }
        finally
        {
            _write.Release();
        }
    }

    public Task Reply(byte op, uint corr, byte[] body) => Send(op, 1, corr, body);

    public Task Error(byte op, uint corr, ushort code, string message) => Send(op, 3, corr, Body(w =>
    {
        w.Write(code);
        Str(w, message);
    }));

    /// <summary>A delivery of one stored batch whose every record is the subscriber's.</summary>
    public Task Deliver(uint handle, uint partition, byte[] batch, ulong offset, uint count) => Send(0x80, 4, handle, Body(w =>
    {
        w.Write(partition);
        w.Write(1u);
        w.Write(partition);
        w.Write(offset);
        w.Write(count);
        w.Write(batch);
    }));

    public void Drop() => _socket?.Close();

    private async Task Serve()
    {
        try
        {
            _socket = await _listener.AcceptSocketAsync(_stop.Token);
            _socket.NoDelay = true;
            var header = new byte[12];
            while (true)
            {
                await ReadExactly(header);
                var body = new byte[BinaryPrimitives.ReadUInt32LittleEndian(header) - 8];
                await ReadExactly(body);
                var op = header[4];
                var corr = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(8));
                Received.Add((op, corr, body));
                if (OnFrame is not null)
                {
                    await OnFrame(op, corr, body);
                }
                else
                {
                    await Default(op, corr, body);
                }
            }
        }
        catch (Exception)
        {
            // The test ended, or dropped the connection on purpose.
        }
    }

    private async Task ReadExactly(byte[] buffer)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await _socket!.ReceiveAsync(buffer.AsMemory(read), _stop.Token);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }

            read += n;
        }
    }

    /// <summary>The broker's ordinary answers, enough for a queue to be declared, produced to and consumed.</summary>
    public async Task Default(byte op, uint corr, byte[] body)
    {
        switch (op)
        {
            case 1:
                await Reply(op, corr, Body(w =>
                {
                    w.Write((ushort)1);
                    Str(w, "fake");
                    w.Write(1u << 20);
                    w.Write(1u << 20);
                    w.Write(AuthRequired);
                }));
                break;
            case 2:
            {
                var at = 0;
                var user = Str(body, ref at);
                var password = Str(body, ref at);
                if (user == "app" && password == "secret")
                {
                    await Reply(op, corr, []);
                }
                else
                {
                    await Error(op, corr, 4, "wrong password");
                }

                break;
            }
            case 3:
                await Reply(op, corr, []);
                break;
            case 45:
            {
                var at = 0;
                Queues.TryAdd(Str(body, ref at), []);
                await Reply(op, corr, []);
                break;
            }
            case 14:
            {
                var at = 0;
                var name = Str(body, ref at);
                if (!Queues.ContainsKey(name))
                {
                    await Error(op, corr, 6, $"no topic {name}");
                    break;
                }

                await Reply(op, corr, Body(w =>
                {
                    w.Write(42u);
                    w.Write(1u);
                    w.Write((byte)0);
                    w.Write(true);
                }));
                break;
            }
            case 20:
            {
                var offset = NextOffset;
                var batch = body[9..];
                NextOffset += BinaryPrimitives.ReadUInt32LittleEndian(batch.AsSpan(BrokerBatch.OCount));
                Queues.Values.First().Add(BatchTests.Stamp(batch, offset, 1_000_000));
                if (body[8] != 0)
                {
                    await Reply(op, corr, Body(w =>
                    {
                        w.Write(0u);
                        w.Write(offset);
                        w.Write(false);
                    }));
                }

                break;
            }
            case 30:
                await Reply(op, corr, Body(w =>
                {
                    w.Write(9u);
                    w.Write(1u);
                    w.Write(0u);
                }));
                // At once, on the heels of the reply: the first delivery must not be lost.
                foreach (var batch in Queues.Values.First())
                {
                    await Deliver(9, 0, batch, BinaryPrimitives.ReadUInt64LittleEndian(batch.AsSpan(BrokerBatch.OBaseOffset)), BinaryPrimitives.ReadUInt32LittleEndian(batch.AsSpan(BrokerBatch.OCount)));
                }

                break;
            case 35:
                await Reply(op, corr, []);
                break;
        }
    }

    public (byte Op, uint Corr, byte[] Body) Next(byte op)
    {
        while (true)
        {
            if (!Received.TryTake(out var frame, TimeSpan.FromSeconds(5)))
            {
                throw new TimeoutException($"the client never sent op {op}");
            }

            if (frame.Op == op)
            {
                return frame;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _socket?.Dispose();
        _listener.Stop();
    }
}

public class BrokerClientTests
{
    private static async Task<List<BrokerMessage>> Take(BrokerSubscription subscription, int count)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var taken = new List<BrokerMessage>();
        await foreach (var message in subscription.WithCancellation(limit.Token))
        {
            taken.Add(message);
            if (taken.Count == count)
            {
                break;
            }
        }

        return taken;
    }

    [Fact]
    public async Task A_queue_is_declared_produced_to_and_consumed()
    {
        await using var fake = new FakeBroker();
        await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString);

        var queue = await broker.DeclareQueueAsync("jobs", new BrokerQueueConfig { AckTimeout = TimeSpan.FromSeconds(7), MaxDeliveries = 3, DeadLetter = "jobs.dead" });
        Assert.Equal(42u, queue.Id);
        Assert.True(queue.IsQueue);

        var declare = fake.Next(45).Body;
        var at = 0;
        Assert.Equal("jobs", FakeBroker.Str(declare, ref at));
        Assert.Equal(0, declare[at]);
        Assert.Equal(7000u, BinaryPrimitives.ReadUInt32LittleEndian(declare.AsSpan(at + 1)));
        Assert.Equal(3u, BinaryPrimitives.ReadUInt32LittleEndian(declare.AsSpan(at + 5)));
        at += 9;
        Assert.Equal("jobs.dead", FakeBroker.Str(declare, ref at));
        Assert.Equal(-1, BinaryPrimitives.ReadInt64LittleEndian(declare.AsSpan(at)));
        Assert.Equal(-1, BinaryPrimitives.ReadInt64LittleEndian(declare.AsSpan(at + 8)));
        Assert.Equal(1, declare[at + 16]);
        Assert.Equal(at + 17, declare.Length);

        var first = await broker.ProduceAsync("jobs", "one"u8.ToArray(), [new BrokerHeader("kind", "email")]);
        var second = await broker.ProduceAsync("jobs", "two"u8.ToArray(), key: "k"u8.ToArray());
        Assert.Equal(10ul, first.BaseOffset);
        Assert.Equal(11ul, second.BaseOffset);

        var produce = fake.Next(20).Body;
        Assert.Equal(42u, BinaryPrimitives.ReadUInt32LittleEndian(produce));
        Assert.Equal(RasterizedDbBroker.AnyPartition, BinaryPrimitives.ReadUInt32LittleEndian(produce.AsSpan(4)));
        Assert.Equal((byte)BrokerAck.Durable, produce[8]);
        Assert.Equal(RasterizedDbBroker.ByKey, BinaryPrimitives.ReadUInt32LittleEndian(fake.Next(20).Body.AsSpan(4)));

        await using var subscription = await broker.ConsumeAsync("jobs", credits: 2);
        Assert.Equal(9u, subscription.Handle);
        Assert.Equal([0u], subscription.AssignedPartitions);

        var subscribe = fake.Next(30).Body;
        at = 4;
        Assert.Equal("jobs", FakeBroker.Str(subscribe, ref at));
        Assert.Equal((byte)BrokerSubscriptionMode.Shared, subscribe[at]);
        Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(subscribe.AsSpan(subscribe.Length - 4)));

        var messages = await Take(subscription, 2);
        Assert.Equal(["one", "two"], messages.Select(m => Encoding.UTF8.GetString(m.Value.Span)));
        Assert.Equal([10ul, 11ul], messages.Select(m => m.Offset));
        Assert.Equal("email", messages[0].Header("kind"));
        Assert.True(messages[1].HasKey);

        // Half the window taken: that many credits go back, with the handle as the frame's corr.
        var credit = fake.Next(31);
        Assert.Equal(9u, credit.Corr);
        Assert.Equal(9u, BinaryPrimitives.ReadUInt32LittleEndian(credit.Body));
        Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(credit.Body.AsSpan(4)));

        subscription.Ack(messages);
        var ack = fake.Next(32).Body;
        Assert.Equal(24, ack.Length);
        Assert.Equal(9u, BinaryPrimitives.ReadUInt32LittleEndian(ack));
        Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(ack.AsSpan(4)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(ack.AsSpan(8)));
        Assert.Equal(10ul, BinaryPrimitives.ReadUInt64LittleEndian(ack.AsSpan(12)));
        Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(ack.AsSpan(20)));

        subscription.Nack(messages[1], TimeSpan.FromSeconds(2));
        var nack = fake.Next(33).Body;
        Assert.Equal(2000u, BinaryPrimitives.ReadUInt32LittleEndian(nack.AsSpan(4)));
        Assert.Equal(11ul, BinaryPrimitives.ReadUInt64LittleEndian(nack.AsSpan(16)));

        await subscription.CloseAsync();
        Assert.Equal(9u, BinaryPrimitives.ReadUInt32LittleEndian(fake.Next(35).Body));
    }

    [Fact]
    public async Task A_login_is_sent_when_given_and_asked_for_when_missing()
    {
        await using (var fake = new FakeBroker { AuthRequired = true })
        {
            var refused = await Assert.ThrowsAsync<BrokerException>(() => RasterizedDbBroker.ConnectAsync(fake.ConnectionString));
            Assert.Equal(BrokerErrorCode.AuthRequired, refused.Code);
        }

        await using (var fake = new FakeBroker { AuthRequired = true })
        {
            var wrong = await Assert.ThrowsAsync<BrokerException>(() => RasterizedDbBroker.ConnectAsync(fake.ConnectionString + ";Username=app;Password=no"));
            Assert.Equal(BrokerErrorCode.AuthFailed, wrong.Code);
            Assert.False(wrong.IsConnectionLost);
        }

        await using (var fake = new FakeBroker { AuthRequired = true })
        {
            await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString + ";Username=app;Password=secret");
            await broker.PingAsync();
        }
    }

    [Fact]
    public async Task An_error_reply_carries_its_code_and_the_connection_goes_on()
    {
        await using var fake = new FakeBroker();
        await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString);

        var missing = await Assert.ThrowsAsync<BrokerException>(() => broker.ResolveAsync("nope"));
        Assert.Equal(BrokerErrorCode.NoTopic, missing.Code);
        Assert.Contains("no topic nope", missing.Message);
        await broker.PingAsync();
        Assert.True(broker.IsConnected);
    }

    [Fact]
    public async Task A_lost_connection_ends_requests_and_subscriptions()
    {
        await using var fake = new FakeBroker();
        await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString);
        await broker.DeclareQueueAsync("jobs");
        var subscription = await broker.ConsumeAsync("jobs");

        var lost = new TaskCompletionSource<BrokerException>();
        broker.ConnectionLost += e => lost.TrySetResult(e);
        var closed = new TaskCompletionSource<BrokerException>();
        subscription.Closed += e => closed.TrySetResult(e);

        fake.Drop();

        Assert.True((await lost.Task.WaitAsync(TimeSpan.FromSeconds(5))).IsConnectionLost);
        await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<BrokerException>(() => Take(subscription, 1));
        await Assert.ThrowsAsync<BrokerException>(() => broker.PingAsync());
        Assert.False(broker.IsConnected);
        Assert.Throws<BrokerException>(() => subscription.Ack(Array.Empty<BrokerMessage>()));
    }

    [Fact]
    public async Task A_subscription_the_broker_closes_ends_with_its_error()
    {
        await using var fake = new FakeBroker();
        await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString);
        await broker.DeclareQueueAsync("jobs");
        var subscription = await broker.ConsumeAsync("jobs");

        await fake.Send(0x82, 4, 9, FakeBroker.Body(w =>
        {
            w.Write((ushort)12);
            FakeBroker.Str(w, "the subscription was deleted");
        }));

        var ended = await Assert.ThrowsAsync<BrokerException>(() => Take(subscription, 1));
        Assert.Equal(BrokerErrorCode.NoSubscription, ended.Code);
        await broker.PingAsync();
    }

    [Fact]
    public async Task A_request_without_a_reply_times_out_and_a_disposed_connection_refuses()
    {
        await using var fake = new FakeBroker();
        var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString.Replace("Timeout=5", "Timeout=0.3"));
        fake.OnFrame = (_, _, _) => Task.CompletedTask;

        var late = await Assert.ThrowsAsync<BrokerException>(() => broker.PingAsync());
        Assert.Contains("no reply", late.Message);

        var raised = false;
        broker.ConnectionLost += _ => raised = true;
        await broker.DisposeAsync();
        await Assert.ThrowsAsync<BrokerException>(() => broker.PingAsync());
        Assert.False(raised);
    }

    [Fact]
    public async Task Many_requests_at_once_each_get_their_own_reply()
    {
        await using var fake = new FakeBroker();
        await using var broker = await RasterizedDbBroker.ConnectAsync(fake.ConnectionString);
        await broker.DeclareQueueAsync("jobs");

        var produced = await Task.WhenAll(Enumerable.Range(0, 200).Select(i => broker.ProduceAsync("jobs", Encoding.UTF8.GetBytes($"m{i}"))));
        Assert.Equal(200, produced.Select(p => p.BaseOffset).Distinct().Count());

        // Nothing is waited for, nothing comes back.
        var unacknowledged = await broker.ProduceAsync("jobs", "x"u8.ToArray(), ack: BrokerAck.None);
        Assert.Equal(ulong.MaxValue, unacknowledged.BaseOffset);
        await broker.PingAsync();
    }
}
