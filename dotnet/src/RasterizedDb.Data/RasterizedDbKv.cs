using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// The server's Redis-compatible key/value cache (<c>KV …</c> statements). From
/// <see cref="RasterizedDbConnection.Kv"/>.
/// </summary>
/// <remarks>
/// Every argument is sent as hex (<c>x'…'</c>) and values come back as bytes (<c>KV BINARY</c>), so any
/// byte string is a key or a value. A key or value given as a <see cref="string"/> is its UTF-8. No
/// command here is atomic with another: the native protocol has no <c>MULTI</c>. Where a call takes two
/// commands it says so. <c>Map</c> is a typed map under a key prefix.
/// </remarks>
public readonly partial struct RasterizedDbKv
{
    private const int MaxScanRounds = 1_000_000;

    private readonly RasterizedDbConnection _connection;

    // Set on the copy the synchronous calls run on: its commands are sent with blocking I/O, so the
    // task of any call on it is complete when it is returned.
    private readonly bool _sync;

    internal RasterizedDbKv(RasterizedDbConnection connection, bool sync = false)
    {
        _connection = connection;
        _sync = sync;
    }

    internal RasterizedDbKv Blocking => new(_connection, sync: true);

    // ------------------------------------------------------------------ request

    /// <summary>
    /// A command's text, built straight into bytes the connection keeps for this: <c>KV BINARY SET
    /// x'6b' x'76'</c>. Nothing is scanned (there is nothing in it to find) and no string is made.
    /// </summary>
    internal readonly struct Request
    {
        private readonly ByteWriter _writer;

        internal Request(ByteWriter writer, string command)
        {
            _writer = writer;
            writer.Clear();
            writer.Write("KV BINARY "u8);
            writer.WriteAscii(command);
        }

        public Request Arg(ReadOnlySpan<byte> bytes)
        {
            _writer.Write(" x'"u8);
            _writer.WriteHex(bytes);
            _writer.WriteByte((byte)'\'');
            return this;
        }

        public Request Arg(string text)
        {
            var max = Encoding.UTF8.GetMaxByteCount(text.Length);
            if (max <= 512)
            {
                Span<byte> utf8 = stackalloc byte[max];
                return Arg(utf8[..Encoding.UTF8.GetBytes(text, utf8)]);
            }

            var rented = ArrayPool<byte>.Shared.Rent(max);
            try
            {
                return Arg(rented.AsSpan(0, Encoding.UTF8.GetBytes(text, rented)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public Request Arg(long number)
        {
            Span<byte> text = stackalloc byte[20];
            System.Buffers.Text.Utf8Formatter.TryFormat(number, text, out var written);
            return Arg(text[..written]);
        }

        public Request Arg(double number)
        {
            Span<byte> text = stackalloc byte[32];
            number.TryFormat(text, out var written, default, CultureInfo.InvariantCulture);
            return Arg(text[..written]);
        }

        public Request Ttl(TimeSpan? ttl) => ttl is { } t ? Arg("PX"u8).Arg(Math.Clamp((long)t.TotalMilliseconds, 1, long.MaxValue)) : this;
    }

    internal Request Begin(string command)
    {
        if (command.Length == 0)
        {
            throw new ArgumentException("a KV command needs a name");
        }

        foreach (var c in command)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
            {
                throw new ArgumentException($"\"{command}\" is not a KV command name");
            }
        }

        return new Request(_connection.KvScratch ??= new ByteWriter(256), command);
    }

    /// <summary>Sends the request <see cref="Begin"/> started. Its bytes are in the connection's scratch buffer until then.</summary>
    internal Task<RasterizedDbDataReader> Send(CancellationToken cancellation)
    {
        var command = _connection.KvCommand ??= new RasterizedDbCommand { ResultFormat = RasterizedDbResultFormat.Binary };
        command.Connection = _connection;
        command.RawStatement = _connection.KvScratch!.WrittenMemory;
        return _sync ? Task.FromResult(command.ExecuteReader()) : command.ExecuteReaderAsync(cancellation);
    }

    private async Task<long> Int(CancellationToken cancellation, string command)
    {
        await using var reader = await Send(cancellation).ConfigureAwait(false);
        return reader.Read() ? reader.GetInt64(0) : throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, $"KV {command} answered no row");
    }

    private async Task<byte[]?> One(CancellationToken cancellation)
    {
        await using var reader = await Send(cancellation).ConfigureAwait(false);
        return reader.Read() && !reader.IsDBNull(0) ? reader.GetFieldValue<byte[]>(0) : null;
    }

    private async Task<List<byte[]?>> Column(CancellationToken cancellation)
    {
        await using var reader = await Send(cancellation).ConfigureAwait(false);
        var list = new List<byte[]?>();
        while (reader.Read())
        {
            list.Add(reader.IsDBNull(0) ? null : reader.GetFieldValue<byte[]>(0));
        }

        return list;
    }

    private async Task Done(CancellationToken cancellation)
    {
        await using var reader = await Send(cancellation).ConfigureAwait(false);
    }

    private async Task<bool> Any(CancellationToken cancellation)
    {
        await using var reader = await Send(cancellation).ConfigureAwait(false);
        return reader.HasRows;
    }

    private static async Task<bool> Positive(Task<long> count) => await count.ConfigureAwait(false) > 0;

    private static async Task<bool> IsOne(Task<long> count) => await count.ConfigureAwait(false) == 1;

    /// <summary>Any command by name, each argument sent as hex: <c>CommandAsync("GETRANGE", key, "0"u8.ToArray(), "3"u8.ToArray())</c>.</summary>
    public Task<RasterizedDbDataReader> CommandAsync(string command, IEnumerable<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken = default)
    {
        var request = Begin(command);
        foreach (var argument in arguments)
        {
            request.Arg(argument.Span);
        }

        return Send(cancellationToken);
    }

    // ------------------------------------------------------------------ strings

    public Task<byte[]?> GetAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("GET").Arg(key);
        return One(cancellationToken);
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Begin("GET").Arg(key);
        return One(cancellationToken);
    }

    public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken = default) =>
        await GetAsync(key, cancellationToken).ConfigureAwait(false) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    public Task SetAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Begin("SET").Arg(key).Arg(value).Ttl(ttl);
        return Done(cancellationToken);
    }

    public Task SetAsync(string key, ReadOnlySpan<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Begin("SET").Arg(key).Arg(value).Ttl(ttl);
        return Done(cancellationToken);
    }

    public Task SetAsync(string key, string value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Begin("SET").Arg(key).Arg(value).Ttl(ttl);
        return Done(cancellationToken);
    }

    /// <summary>Sets the key only if it has no value (<c>SET … NX</c>); whether it was set.</summary>
    public Task<bool> SetIfAbsentAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Begin("SET").Arg(key).Arg(value).Arg("NX"u8).Ttl(ttl);
        return Any(cancellationToken);
    }

    public Task<bool> SetIfAbsentAsync(string key, string value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Begin("SET").Arg(key).Arg(value).Arg("NX"u8).Ttl(ttl);
        return Any(cancellationToken);
    }

    public Task<List<byte[]?>> GetManyAsync(IReadOnlyList<byte[]> keys, CancellationToken cancellationToken = default)
    {
        if (keys.Count == 0)
        {
            return Task.FromResult(new List<byte[]?>());
        }

        var request = Begin("MGET");
        foreach (var key in keys)
        {
            request.Arg(key);
        }

        return Column(cancellationToken);
    }

    public Task SetManyAsync(IReadOnlyList<KeyValuePair<byte[], byte[]>> pairs, CancellationToken cancellationToken = default)
    {
        if (pairs.Count == 0)
        {
            return Task.CompletedTask;
        }

        var request = Begin("MSET");
        foreach (var (key, value) in pairs)
        {
            request.Arg(key).Arg(value);
        }

        return Done(cancellationToken);
    }

    public Task<long> DeleteAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("DEL").Arg(key);
        return Int(cancellationToken, "DEL");
    }

    public Task<long> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        Begin("DEL").Arg(key);
        return Int(cancellationToken, "DEL");
    }

    public Task<long> DeleteAsync(IReadOnlyList<byte[]> keys, CancellationToken cancellationToken = default)
    {
        if (keys.Count == 0)
        {
            return Task.FromResult(0L);
        }

        var request = Begin("DEL");
        foreach (var key in keys)
        {
            request.Arg(key);
        }

        return Int(cancellationToken, "DEL");
    }

    public Task<bool> ExistsAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("EXISTS").Arg(key);
        return Positive(Int(cancellationToken, "EXISTS"));
    }

    public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        Begin("EXISTS").Arg(key);
        return Positive(Int(cancellationToken, "EXISTS"));
    }

    public Task<long> IncrementAsync(ReadOnlySpan<byte> key, long by = 1, CancellationToken cancellationToken = default)
    {
        Begin("INCRBY").Arg(key).Arg(by);
        return Int(cancellationToken, "INCRBY");
    }

    public Task<long> IncrementAsync(string key, long by = 1, CancellationToken cancellationToken = default)
    {
        Begin("INCRBY").Arg(key).Arg(by);
        return Int(cancellationToken, "INCRBY");
    }

    /// <summary>Gives the key a time to live; false when it does not exist.</summary>
    public Task<bool> ExpireAsync(ReadOnlySpan<byte> key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        Begin("PEXPIRE").Arg(key).Arg(Math.Clamp((long)ttl.TotalMilliseconds, 1, long.MaxValue));
        return IsOne(Int(cancellationToken, "PEXPIRE"));
    }

    public Task<bool> ExpireAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        ExpireAsync(Encoding.UTF8.GetBytes(key), ttl, cancellationToken);

    /// <summary>Removes the key's time to live.</summary>
    public Task<bool> PersistAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("PERSIST").Arg(key);
        return IsOne(Int(cancellationToken, "PERSIST"));
    }

    /// <summary>The key's remaining time to live; null when it has none, or does not exist.</summary>
    public Task<TimeSpan?> TtlAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("PTTL").Arg(key);
        return Ttl(Int(cancellationToken, "PTTL"));

        static async Task<TimeSpan?> Ttl(Task<long> ms) => await ms.ConfigureAwait(false) is >= 0 and var value ? TimeSpan.FromMilliseconds(value) : null;
    }

    public Task<TimeSpan?> TtlAsync(string key, CancellationToken cancellationToken = default) => TtlAsync(Encoding.UTF8.GetBytes(key), cancellationToken);

    /// <summary>Every key matching a glob pattern, by <c>SCAN</c> in rounds of 500 (one command per round).</summary>
    public async Task<List<byte[]>> KeysAsync(ReadOnlyMemory<byte> pattern, CancellationToken cancellationToken = default)
    {
        var keys = new List<byte[]>();
        var cursor = "0";
        // A server's cursor walks a finite keyspace; one that never returns to "0" would loop forever.
        for (var round = 0; round < MaxScanRounds; round++)
        {
            Begin("SCAN").Arg(cursor).Arg("MATCH"u8).Arg(pattern.Span).Arg("COUNT"u8).Arg("500"u8);
            string? next = null;
            await using (var reader = await Send(cancellationToken).ConfigureAwait(false))
            {
                while (reader.Read())
                {
                    next = reader.GetString(0);
                    if (reader.FieldCount > 1 && !reader.IsDBNull(1))
                    {
                        keys.Add(reader.GetFieldValue<byte[]>(1));
                    }
                }
            }

            cursor = next ?? "0";
            if (cursor == "0")
            {
                return keys;
            }
        }

        throw new RasterizedDbException(RasterizedDbErrorKind.Unsupported, $"SCAN did not finish after {MaxScanRounds} rounds ({keys.Count} keys so far); the server's cursor does not end");
    }

    public Task<List<byte[]>> KeysAsync(string pattern, CancellationToken cancellationToken = default) => KeysAsync(Encoding.UTF8.GetBytes(pattern), cancellationToken);

    // ------------------------------------------------------------------- hashes

    public Task<long> HashSetAsync(ReadOnlySpan<byte> key, IReadOnlyList<KeyValuePair<byte[], byte[]>> fields, CancellationToken cancellationToken = default)
    {
        if (fields.Count == 0)
        {
            return Task.FromResult(0L);
        }

        var request = Begin("HSET").Arg(key);
        foreach (var (field, value) in fields)
        {
            request.Arg(field).Arg(value);
        }

        return Int(cancellationToken, "HSET");
    }

    public Task<long> HashSetAsync(string key, string field, string value, CancellationToken cancellationToken = default)
    {
        Begin("HSET").Arg(key).Arg(field).Arg(value);
        return Int(cancellationToken, "HSET");
    }

    public Task<byte[]?> HashGetAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> field, CancellationToken cancellationToken = default)
    {
        Begin("HGET").Arg(key).Arg(field);
        return One(cancellationToken);
    }

    public Task<byte[]?> HashGetAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        Begin("HGET").Arg(key).Arg(field);
        return One(cancellationToken);
    }

    public Task<List<KeyValuePair<byte[], byte[]>>> HashGetAllAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("HGETALL").Arg(key);
        return Pairs(this, cancellationToken);

        static async Task<List<KeyValuePair<byte[], byte[]>>> Pairs(RasterizedDbKv kv, CancellationToken cancellation)
        {
            await using var reader = await kv.Send(cancellation).ConfigureAwait(false);
            var list = new List<KeyValuePair<byte[], byte[]>>();
            while (reader.Read())
            {
                list.Add(new(reader.GetFieldValue<byte[]>(0), reader.GetFieldValue<byte[]>(1)));
            }

            return list;
        }
    }

    public Task<List<KeyValuePair<byte[], byte[]>>> HashGetAllAsync(string key, CancellationToken cancellationToken = default) =>
        HashGetAllAsync(Encoding.UTF8.GetBytes(key), cancellationToken);

    public Task<long> HashDeleteAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> fields, CancellationToken cancellationToken = default)
    {
        if (fields.Count == 0)
        {
            return Task.FromResult(0L);
        }

        var request = Begin("HDEL").Arg(key);
        foreach (var field in fields)
        {
            request.Arg(field);
        }

        return Int(cancellationToken, "HDEL");
    }

    public Task<long> HashIncrementAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> field, long by = 1, CancellationToken cancellationToken = default)
    {
        Begin("HINCRBY").Arg(key).Arg(field).Arg(by);
        return Int(cancellationToken, "HINCRBY");
    }

    // -------------------------------------------------------------------- lists

    private Task<long> KeyAndMany(string command, ReadOnlySpan<byte> key, IReadOnlyList<byte[]> values, CancellationToken cancellation)
    {
        var request = Begin(command).Arg(key);
        foreach (var value in values)
        {
            request.Arg(value);
        }

        return Int(cancellation, command);
    }

    public Task<long> ListRightPushAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> values, CancellationToken cancellationToken = default) =>
        KeyAndMany("RPUSH", key, values, cancellationToken);

    public Task<long> ListLeftPushAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> values, CancellationToken cancellationToken = default) =>
        KeyAndMany("LPUSH", key, values, cancellationToken);

    public Task<byte[]?> ListLeftPopAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("LPOP").Arg(key);
        return One(cancellationToken);
    }

    public Task<byte[]?> ListRightPopAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("RPOP").Arg(key);
        return One(cancellationToken);
    }

    public Task<List<byte[]>> ListRangeAsync(ReadOnlySpan<byte> key, long start, long stop, CancellationToken cancellationToken = default)
    {
        Begin("LRANGE").Arg(key).Arg(start).Arg(stop);
        return NotNull(Column(cancellationToken));
    }

    private static async Task<List<byte[]>> NotNull(Task<List<byte[]?>> column)
    {
        var values = await column.ConfigureAwait(false);
        values.RemoveAll(static v => v is null);
        return values!;
    }

    public Task<long> ListLengthAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("LLEN").Arg(key);
        return Int(cancellationToken, "LLEN");
    }

    // --------------------------------------------------------------------- sets

    public Task<long> SetAddAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members, CancellationToken cancellationToken = default) =>
        KeyAndMany("SADD", key, members, cancellationToken);

    public Task<long> SetRemoveAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members, CancellationToken cancellationToken = default) =>
        KeyAndMany("SREM", key, members, cancellationToken);

    public Task<bool> SetContainsAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member, CancellationToken cancellationToken = default)
    {
        Begin("SISMEMBER").Arg(key).Arg(member);
        return IsOne(Int(cancellationToken, "SISMEMBER"));
    }

    public Task<List<byte[]>> SetMembersAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("SMEMBERS").Arg(key);
        return NotNull(Column(cancellationToken));
    }

    public Task<long> SetCountAsync(ReadOnlySpan<byte> key, CancellationToken cancellationToken = default)
    {
        Begin("SCARD").Arg(key);
        return Int(cancellationToken, "SCARD");
    }

    // -------------------------------------------------------------- sorted sets

    public Task<long> SortedSetAddAsync(ReadOnlySpan<byte> key, IReadOnlyList<KeyValuePair<byte[], double>> members, CancellationToken cancellationToken = default)
    {
        var request = Begin("ZADD").Arg(key);
        foreach (var (member, score) in members)
        {
            request.Arg(score).Arg(member);
        }

        return Int(cancellationToken, "ZADD");
    }

    public Task<double?> SortedSetScoreAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member, CancellationToken cancellationToken = default)
    {
        Begin("ZSCORE").Arg(key).Arg(member);
        return Score(this, cancellationToken);

        static async Task<double?> Score(RasterizedDbKv kv, CancellationToken cancellation)
        {
            await using var reader = await kv.Send(cancellation).ConfigureAwait(false);
            return reader.Read() && !reader.IsDBNull(0) ? reader.GetDouble(0) : null;
        }
    }

    public Task<List<KeyValuePair<byte[], double>>> SortedSetRangeAsync(ReadOnlySpan<byte> key, long start, long stop, bool descending = false, CancellationToken cancellationToken = default)
    {
        var request = Begin("ZRANGE").Arg(key).Arg(start).Arg(stop);
        if (descending)
        {
            request.Arg("REV"u8);
        }

        request.Arg("WITHSCORES"u8);
        return Range(this, cancellationToken);

        static async Task<List<KeyValuePair<byte[], double>>> Range(RasterizedDbKv kv, CancellationToken cancellation)
        {
            await using var reader = await kv.Send(cancellation).ConfigureAwait(false);
            var list = new List<KeyValuePair<byte[], double>>();
            while (reader.Read())
            {
                list.Add(new(reader.GetFieldValue<byte[]>(0), reader.GetDouble(1)));
            }

            return list;
        }
    }

    public Task<long> SortedSetRemoveAsync(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members, CancellationToken cancellationToken = default) =>
        KeyAndMany("ZREM", key, members, cancellationToken);

    public Task<double> SortedSetIncrementAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member, double by, CancellationToken cancellationToken = default)
    {
        Begin("ZINCRBY").Arg(key).Arg(by).Arg(member);
        return Score(this, cancellationToken);

        static async Task<double> Score(RasterizedDbKv kv, CancellationToken cancellation)
        {
            await using var reader = await kv.Send(cancellation).ConfigureAwait(false);
            return reader.Read() ? reader.GetDouble(0) : throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "KV ZINCRBY answered no row");
        }
    }

    // -------------------------------------------------------------------- typed

    /// <summary>
    /// A typed map under a key prefix (<c>sessions:&lt;key&gt;</c>). Keys and scalar values are stored as
    /// their text (<c>42</c>, <c>t</c>, a UUID's text, UTF-8 for a string, the bytes of a
    /// <c>byte[]</c>); any other value type is stored as JSON.
    /// </summary>
    /// <param name="prefix">The prefix; empty for the whole keyspace.</param>
    /// <param name="jsonOptions">How a non-scalar value is serialized.</param>
    public RasterizedDbKvMap<TKey, TValue> Map<TKey, TValue>(string prefix, JsonSerializerOptions? jsonOptions = null)
        where TKey : notnull =>
        new(this, prefix, jsonOptions, null);

    /// <summary>
    /// Objects of <typeparamref name="T"/> stored as hashes, one field per public property, under
    /// <c>&lt;prefix&gt;:&lt;key&gt;</c>: a field can be read or changed without the rest.
    /// </summary>
    /// <param name="key">The property that identifies an object.</param>
    /// <param name="prefix">The key prefix; the type's name in lower case when left out.</param>
    public RasterizedDbKvRepository<T, TKey> Repository<T, TKey>(Func<T, TKey> key, string? prefix = null)
        where T : class, new()
        where TKey : notnull =>
        new(this, key, prefix ?? typeof(T).Name.ToLowerInvariant());

    /// <summary><see cref="Map{TKey,TValue}(string,JsonSerializerOptions?)"/> with source-generated JSON metadata, for trimmed and AOT applications.</summary>
    public RasterizedDbKvMap<TKey, TValue> Map<TKey, TValue>(string prefix, JsonTypeInfo<TValue> jsonTypeInfo)
        where TKey : notnull =>
        new(this, prefix, null, jsonTypeInfo);
}

/// <summary>A typed map over the key/value cache. See <see cref="RasterizedDbKv.Map{TKey,TValue}(string,JsonSerializerOptions?)"/>.</summary>
public readonly partial struct RasterizedDbKvMap<TKey, TValue>
    where TKey : notnull
{
    private readonly RasterizedDbKv _kv;
    private readonly byte[] _prefix;
    private readonly JsonSerializerOptions? _options;
    private readonly JsonTypeInfo<TValue>? _typeInfo;

    internal RasterizedDbKvMap(RasterizedDbKv kv, string prefix, JsonSerializerOptions? options, JsonTypeInfo<TValue>? typeInfo)
    {
        _kv = kv;
        _prefix = prefix.Length == 0 ? [] : Encoding.UTF8.GetBytes(prefix + ":");
        _options = options;
        _typeInfo = typeInfo;
    }

    private RasterizedDbKvMap(RasterizedDbKv kv, byte[] prefix, JsonSerializerOptions? options, JsonTypeInfo<TValue>? typeInfo)
    {
        _kv = kv;
        _prefix = prefix;
        _options = options;
        _typeInfo = typeInfo;
    }

    internal RasterizedDbKvMap<TKey, TValue> Blocking => new(_kv.Blocking, _prefix, _options, _typeInfo);

    internal RasterizedDbKv Kv => _kv;

    internal byte[] Prefix => _prefix;

    internal TValue DecodeValue(byte[] bytes) => Decode(bytes);

    /// <summary>A query over the map's entries: by key prefix or glob, filtered here by key or by entry.</summary>
    public RasterizedDbKvQuery<TKey, TValue> Query() => new(this);

    /// <summary>The key as stored: the prefix, a colon, and the key's text.</summary>
    public byte[] FullKey(TKey key)
    {
        using var writer = new ByteWriter(64);
        writer.Write(_prefix);
        KvCodec.WriteText(writer, key);
        return writer.WrittenSpan.ToArray();
    }

    private byte[] Encode(TValue value)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value), "a NULL is not stored as a value: remove the key instead");
        }

        if (KvCodec.IsScalar<TValue>())
        {
            using var writer = new ByteWriter(64);
            KvCodec.WriteText(writer, value);
            return writer.WrittenSpan.ToArray();
        }

        return _typeInfo is not null ? JsonSerializer.SerializeToUtf8Bytes(value, _typeInfo) : JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    private TValue Decode(byte[] bytes)
    {
        if (KvCodec.IsScalar<TValue>())
        {
            return ValueReader.Read<TValue>(Tag.String, bytes);
        }

        return (_typeInfo is not null ? JsonSerializer.Deserialize(bytes, _typeInfo) : JsonSerializer.Deserialize<TValue>(bytes, _options))!;
    }

    /// <summary>The value, and whether the key had one.</summary>
    public async Task<(bool Found, TValue Value)> TryGetAsync(TKey key, CancellationToken cancellationToken = default) =>
        await _kv.GetAsync(FullKey(key), cancellationToken).ConfigureAwait(false) is { } bytes ? (true, Decode(bytes)) : (false, default!);

    /// <summary>The value, or the type's default when the key has none.</summary>
    public async Task<TValue?> GetAsync(TKey key, CancellationToken cancellationToken = default) =>
        await _kv.GetAsync(FullKey(key), cancellationToken).ConfigureAwait(false) is { } bytes ? Decode(bytes) : default;

    public Task SetAsync(TKey key, TValue value, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        _kv.SetAsync(FullKey(key), Encode(value), ttl, cancellationToken);

    public Task<bool> SetIfAbsentAsync(TKey key, TValue value, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        _kv.SetIfAbsentAsync(FullKey(key), Encode(value), ttl, cancellationToken);

    public async Task<bool> RemoveAsync(TKey key, CancellationToken cancellationToken = default) =>
        await _kv.DeleteAsync(FullKey(key), cancellationToken).ConfigureAwait(false) > 0;

    public Task<bool> ContainsAsync(TKey key, CancellationToken cancellationToken = default) => _kv.ExistsAsync(FullKey(key), cancellationToken);

    public Task<bool> ExpireAsync(TKey key, TimeSpan ttl, CancellationToken cancellationToken = default) => _kv.ExpireAsync(FullKey(key), ttl, cancellationToken);

    public Task<TimeSpan?> TtlAsync(TKey key, CancellationToken cancellationToken = default) => _kv.TtlAsync(FullKey(key), cancellationToken);

    public Task<long> IncrementAsync(TKey key, long by = 1, CancellationToken cancellationToken = default) => _kv.IncrementAsync(FullKey(key), by, cancellationToken);

    /// <summary>Several values in one command; a key with none gives the type's default.</summary>
    public async Task<List<TValue?>> GetManyAsync(IReadOnlyList<TKey> keys, CancellationToken cancellationToken = default)
    {
        var full = new byte[keys.Count][];
        for (var i = 0; i < full.Length; i++)
        {
            full[i] = FullKey(keys[i]);
        }

        var raw = await _kv.GetManyAsync(full, cancellationToken).ConfigureAwait(false);
        var values = new List<TValue?>(raw.Count);
        foreach (var bytes in raw)
        {
            values.Add(bytes is null ? default : Decode(bytes));
        }

        return values;
    }

    private byte[] Pattern()
    {
        // The prefix with glob characters escaped, then *.
        var pattern = new List<byte>(_prefix.Length + 2);
        foreach (var b in _prefix)
        {
            if (b is (byte)'*' or (byte)'?' or (byte)'[' or (byte)']' or (byte)'\\')
            {
                pattern.Add((byte)'\\');
            }

            pattern.Add(b);
        }

        pattern.Add((byte)'*');
        return [.. pattern];
    }

    /// <summary>The map's keys, read as <typeparamref name="TKey"/> (one <c>SCAN</c> per 500 keys).</summary>
    public async Task<List<TKey>> KeysAsync(CancellationToken cancellationToken = default)
    {
        var raw = await _kv.KeysAsync(Pattern(), cancellationToken).ConfigureAwait(false);
        var keys = new List<TKey>(raw.Count);
        foreach (var key in raw)
        {
            keys.Add(ValueReader.Read<TKey>(Tag.String, key.AsSpan(Math.Min(_prefix.Length, key.Length))));
        }

        return keys;
    }

    /// <summary>Removes every key of the map (a <c>SCAN</c>, then a <c>DEL</c> per 500 keys; not atomic).</summary>
    public async Task<long> ClearAsync(CancellationToken cancellationToken = default)
    {
        if (_prefix.Length == 0)
        {
            throw new NotSupportedException("ClearAsync on the whole keyspace; use KV FLUSHDB if that is meant");
        }

        var keys = await _kv.KeysAsync(Pattern(), cancellationToken).ConfigureAwait(false);
        long removed = 0;
        for (var at = 0; at < keys.Count; at += 500)
        {
            removed += await _kv.DeleteAsync(keys.GetRange(at, Math.Min(500, keys.Count - at)), cancellationToken).ConfigureAwait(false);
        }

        return removed;
    }
}

/// <summary>A value's text form as the cache stores it: what the SQL literal holds, without the quoting.</summary>
internal static class KvCodec
{
    public static bool IsScalar<T>()
    {
        var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal) || type == typeof(Guid) || type == typeof(byte[])
               || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly) || type == typeof(TimeOnly)
               || type == typeof(TimeSpan) || type == typeof(Int128) || type == typeof(System.Numerics.BigInteger) || type == typeof(RasterizedDbInterval);
    }

    public static void WriteText<T>(ByteWriter writer, T value)
    {
        switch (value)
        {
            case null:
                throw new ArgumentNullException(nameof(value), "a NULL cannot be a key or a value");
            case string text:
                writer.WriteUtf8(text);
                break;
            case byte[] bytes:
                writer.Write(bytes);
                break;
            case bool flag:
                writer.WriteByte(flag ? (byte)'t' : (byte)'f');
                break;
            case int number:
                writer.WriteInt64Text(number);
                break;
            case long number:
                writer.WriteInt64Text(number);
                break;
            case Guid id:
                writer.WriteFormatted(id);
                break;
            case char c:
                writer.WriteUtf8(new ReadOnlySpan<char>(in c));
                break;
            case Enum e:
                writer.WriteAscii(Convert.ToInt64(e, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture));
                break;
            default:
            {
                // The literal, with what makes it one taken off: quotes, a negative number's
                // parentheses, a real's cast.
                using var literal = new ByteWriter(64);
                LiteralWriter.WriteObject(literal, value);
                var span = literal.WrittenSpan;
                if (span.EndsWith("::real"u8))
                {
                    span = span[..^6];
                }

                if (span.Length >= 2 && ((span[0] == '\'' && span[^1] == '\'') || (span[0] == '(' && span[^1] == ')')))
                {
                    span = span[1..^1];
                }

                if (span.EndsWith("+00"u8) && value is DateTime or DateTimeOffset)
                {
                    span = span[..^3];
                }

                writer.Write(span);
                break;
            }
        }
    }
}

/// <summary>
/// A query over a typed map: the keys are found by <c>SCAN</c> (the pattern is matched by the server),
/// the values fetched 500 at a time, and the filters run here. Not a snapshot: an entry removed between
/// the scan and its fetch is simply not returned.
/// </summary>
public sealed partial class RasterizedDbKvQuery<TKey, TValue>
    where TKey : notnull
{
    private RasterizedDbKvMap<TKey, TValue> _map;

    private RasterizedDbKvQuery<TKey, TValue> Blocking
    {
        get
        {
            var copy = (RasterizedDbKvQuery<TKey, TValue>)MemberwiseClone();
            copy._map = _map.Blocking;
            return copy;
        }
    }

    private byte[] _suffix = [];
    private Func<TKey, bool>? _keyFilter;
    private Func<TKey, TValue, bool>? _filter;
    private int? _limit;
    private bool _lenient;

    internal RasterizedDbKvQuery(RasterizedDbKvMap<TKey, TValue> map) => _map = map;

    private static byte[] GlobEscape(ReadOnlySpan<byte> bytes)
    {
        var escaped = new List<byte>(bytes.Length + 2);
        foreach (var b in bytes)
        {
            if (b is (byte)'*' or (byte)'?' or (byte)'[' or (byte)']' or (byte)'\\')
            {
                escaped.Add((byte)'\\');
            }

            escaped.Add(b);
        }

        return [.. escaped];
    }

    /// <summary>Only keys starting with <paramref name="prefix"/> (after the map's own prefix).</summary>
    public RasterizedDbKvQuery<TKey, TValue> Prefix(string prefix)
    {
        _suffix = [.. GlobEscape(Encoding.UTF8.GetBytes(prefix)), (byte)'*'];
        return this;
    }

    /// <summary>Only keys matching a glob (<c>*</c>, <c>?</c>, <c>[a-z]</c>), after the map's own prefix.</summary>
    public RasterizedDbKvQuery<TKey, TValue> Pattern(string glob)
    {
        _suffix = Encoding.UTF8.GetBytes(glob);
        return this;
    }

    /// <summary>Keeps the keys <paramref name="keep"/> accepts, before any value is fetched.</summary>
    public RasterizedDbKvQuery<TKey, TValue> WhereKey(Func<TKey, bool> keep)
    {
        _keyFilter = keep;
        return this;
    }

    public RasterizedDbKvQuery<TKey, TValue> Where(Func<TKey, TValue, bool> keep)
    {
        _filter = keep;
        return this;
    }

    public RasterizedDbKvQuery<TKey, TValue> Take(int count)
    {
        _limit = count;
        return this;
    }

    /// <summary>Skips keys and values that do not read as the query's types, where the default is an error naming one.</summary>
    public RasterizedDbKvQuery<TKey, TValue> Lenient()
    {
        _lenient = true;
        return this;
    }

    private async Task<List<(byte[] Full, TKey Key)>> MatchingKeys(CancellationToken cancellation)
    {
        var prefix = _map.Prefix;
        byte[] pattern = [.. GlobEscape(prefix), .. _suffix.Length == 0 ? "*"u8.ToArray() : _suffix];
        var matches = new List<(byte[], TKey)>();
        foreach (var full in await _map.Kv.KeysAsync(pattern, cancellation).ConfigureAwait(false))
        {
            TKey key;
            try
            {
                key = ValueReader.Read<TKey>(Tag.String, full.AsSpan(Math.Min(prefix.Length, full.Length)));
            }
            catch (Exception e) when (e is InvalidCastException or FormatException or OverflowException)
            {
                if (_lenient)
                {
                    continue;
                }

                throw new InvalidCastException($"key \"{Encoding.UTF8.GetString(full)}\" does not read as {typeof(TKey).Name} ({e.Message}); Lenient() skips such keys", e);
            }

            if (_keyFilter is null || _keyFilter(key))
            {
                matches.Add((full, key));
            }
        }

        return matches;
    }

    /// <summary>The matching entries.</summary>
    public async Task<List<KeyValuePair<TKey, TValue>>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var entries = new List<KeyValuePair<TKey, TValue>>();
        var keys = await MatchingKeys(cancellationToken).ConfigureAwait(false);
        for (var at = 0; at < keys.Count; at += 500)
        {
            var chunk = keys.GetRange(at, Math.Min(500, keys.Count - at));
            var values = await _map.Kv.GetManyAsync(chunk.ConvertAll(k => k.Full), cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < chunk.Count && i < values.Count; i++)
            {
                // Deleted since the scan, or not a string value (a hash, a list): not an entry of this map.
                if (values[i] is not { } bytes)
                {
                    continue;
                }

                TValue value;
                try
                {
                    value = _map.DecodeValue(bytes);
                }
                catch (Exception e) when (e is InvalidCastException or FormatException or OverflowException or JsonException)
                {
                    if (_lenient)
                    {
                        continue;
                    }

                    throw new InvalidCastException(
                        $"the value at \"{Encoding.UTF8.GetString(chunk[i].Full)}\" does not read as {typeof(TValue).Name} ({e.Message}); Lenient() skips such entries", e);
                }

                if (_filter is null || _filter(chunk[i].Key, value))
                {
                    entries.Add(new(chunk[i].Key, value));
                    if (entries.Count >= _limit)
                    {
                        return entries;
                    }
                }
            }
        }

        return entries;
    }

    /// <summary>The matching keys; no value is fetched unless an entry filter needs it.</summary>
    public async Task<List<TKey>> KeysAsync(CancellationToken cancellationToken = default)
    {
        if (_filter is not null)
        {
            return (await ToListAsync(cancellationToken).ConfigureAwait(false)).ConvertAll(e => e.Key);
        }

        var keys = (await MatchingKeys(cancellationToken).ConfigureAwait(false)).ConvertAll(k => k.Key);
        return _limit is { } limit && keys.Count > limit ? keys.GetRange(0, limit) : keys;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        _filter is null && _limit is null
            ? (await MatchingKeys(cancellationToken).ConfigureAwait(false)).Count
            : (await ToListAsync(cancellationToken).ConfigureAwait(false)).Count;

    /// <summary>Removes the matching entries (a <c>DEL</c> per 500 keys; not atomic). Returns how many were removed.</summary>
    public async Task<long> RemoveAsync(CancellationToken cancellationToken = default)
    {
        List<byte[]> keys;
        if (_filter is null && _limit is null)
        {
            keys = (await MatchingKeys(cancellationToken).ConfigureAwait(false)).ConvertAll(k => k.Full);
        }
        else
        {
            keys = (await ToListAsync(cancellationToken).ConfigureAwait(false)).ConvertAll(e => _map.FullKey(e.Key));
        }

        long removed = 0;
        for (var at = 0; at < keys.Count; at += 500)
        {
            removed += await _map.Kv.DeleteAsync(keys.GetRange(at, Math.Min(500, keys.Count - at)), cancellationToken).ConfigureAwait(false);
        }

        return removed;
    }
}

/// <summary>
/// Objects stored as hashes: one field per public read-write property, each as its text (a property
/// that is not a scalar as JSON). See <see cref="RasterizedDbKv.Repository{T,TKey}"/>.
/// </summary>
public sealed partial class RasterizedDbKvRepository<T, TKey>
    where T : class, new()
    where TKey : notnull
{
    private static readonly System.Reflection.PropertyInfo[] Properties =
        [.. typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(p => p is { CanRead: true, CanWrite: true } && p.GetIndexParameters().Length == 0)];

    private RasterizedDbKv _kv;
    private readonly Func<T, TKey> _key;
    private readonly byte[] _prefix;

    private RasterizedDbKvRepository<T, TKey> Blocking
    {
        get
        {
            var copy = (RasterizedDbKvRepository<T, TKey>)MemberwiseClone();
            copy._kv = _kv.Blocking;
            return copy;
        }
    }

    internal RasterizedDbKvRepository(RasterizedDbKv kv, Func<T, TKey> key, string prefix)
    {
        _kv = kv;
        _key = key;
        _prefix = Encoding.UTF8.GetBytes(prefix + ":");
    }

    private byte[] FullKey(TKey key)
    {
        using var writer = new ByteWriter(64);
        writer.Write(_prefix);
        KvCodec.WriteText(writer, key);
        return writer.WrittenSpan.ToArray();
    }

    private static bool Scalar(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal) || type == typeof(Guid) || type == typeof(byte[])
               || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(TimeSpan);
    }

    private static byte[] FieldValue(System.Reflection.PropertyInfo property, object value)
    {
        if (!Scalar(property.PropertyType))
        {
            return JsonSerializer.SerializeToUtf8Bytes(value, property.PropertyType);
        }

        using var writer = new ByteWriter(64);
        KvCodec.WriteText(writer, value);
        return writer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Writes every property as a field. A null property's field is removed, so a reader sees it as
    /// null. Two commands when a TTL is given or a property is null; not atomic.
    /// </summary>
    public async Task SaveAsync(T item, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        var key = FullKey(_key(item));
        var fields = new List<KeyValuePair<byte[], byte[]>>(Properties.Length);
        List<byte[]>? cleared = null;
        foreach (var property in Properties)
        {
            var name = Encoding.UTF8.GetBytes(property.Name);
            if (property.GetValue(item) is { } value)
            {
                fields.Add(new(name, FieldValue(property, value)));
            }
            else
            {
                (cleared ??= []).Add(name);
            }
        }

        await _kv.HashSetAsync(key, fields, cancellationToken).ConfigureAwait(false);
        if (cleared is not null)
        {
            await _kv.HashDeleteAsync(key, cleared, cancellationToken).ConfigureAwait(false);
        }

        if (ttl is { } time)
        {
            await _kv.ExpireAsync(key, time, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>The object stored under <paramref name="key"/>, or null when there is none.</summary>
    public async Task<T?> GetAsync(TKey key, CancellationToken cancellationToken = default)
    {
        var fields = await _kv.HashGetAllAsync(FullKey(key), cancellationToken).ConfigureAwait(false);
        if (fields.Count == 0)
        {
            return null;
        }

        var item = new T();
        foreach (var (name, bytes) in fields)
        {
            var property = Array.Find(Properties, p => Encoding.UTF8.GetString(name) == p.Name);
            if (property is null)
            {
                // A field of a newer or older shape of the type.
                continue;
            }

            property.SetValue(
                item,
                Scalar(property.PropertyType)
                    ? ValueReader.ReadBoxed(property.PropertyType, Tag.String, bytes)
                    : JsonSerializer.Deserialize(bytes, property.PropertyType));
        }

        return item;
    }

    /// <summary>Changes one field, leaving the others as they are.</summary>
    public Task SetFieldAsync(TKey key, string property, object value, CancellationToken cancellationToken = default)
    {
        var info = Array.Find(Properties, p => p.Name == property) ?? throw new ArgumentException($"{typeof(T).Name} has no property \"{property}\"");
        return _kv.HashSetAsync(FullKey(key), [new(Encoding.UTF8.GetBytes(property), FieldValue(info, value))], cancellationToken);
    }

    /// <summary>Adds to a numeric field in place (<c>HINCRBY</c>) and returns its new value.</summary>
    public Task<long> IncrementAsync(TKey key, string property, long by = 1, CancellationToken cancellationToken = default) =>
        _kv.HashIncrementAsync(FullKey(key), Encoding.UTF8.GetBytes(property), by, cancellationToken);

    public async Task<bool> RemoveAsync(TKey key, CancellationToken cancellationToken = default) =>
        await _kv.DeleteAsync(FullKey(key), cancellationToken).ConfigureAwait(false) > 0;

    public Task<bool> ContainsAsync(TKey key, CancellationToken cancellationToken = default) => _kv.ExistsAsync(FullKey(key), cancellationToken);

    public Task<bool> ExpireAsync(TKey key, TimeSpan ttl, CancellationToken cancellationToken = default) => _kv.ExpireAsync(FullKey(key), ttl, cancellationToken);
}
