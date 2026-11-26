// Generated from the asynchronous calls of RasterizedDbKv.cs: one synchronous twin for each.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace RasterizedDb.Data;

/// <content>The synchronous calls: each is its <c>Async</c> twin, sent with blocking I/O on the calling thread.</content>
public readonly partial struct RasterizedDbKv
{
    public RasterizedDbDataReader Command(string command, IEnumerable<ReadOnlyMemory<byte>> arguments) =>
        Blocking.CommandAsync(command, arguments).GetAwaiter().GetResult();

    public byte[]? Get(ReadOnlySpan<byte> key) =>
        Blocking.GetAsync(key).GetAwaiter().GetResult();

    public byte[]? Get(string key) =>
        Blocking.GetAsync(key).GetAwaiter().GetResult();

    public string? GetString(string key) =>
        Blocking.GetStringAsync(key).GetAwaiter().GetResult();

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TimeSpan? ttl = null) =>
        Blocking.SetAsync(key, value, ttl).GetAwaiter().GetResult();

    public void Set(string key, ReadOnlySpan<byte> value, TimeSpan? ttl = null) =>
        Blocking.SetAsync(key, value, ttl).GetAwaiter().GetResult();

    public void Set(string key, string value, TimeSpan? ttl = null) =>
        Blocking.SetAsync(key, value, ttl).GetAwaiter().GetResult();

    public bool SetIfAbsent(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TimeSpan? ttl = null) =>
        Blocking.SetIfAbsentAsync(key, value, ttl).GetAwaiter().GetResult();

    public bool SetIfAbsent(string key, string value, TimeSpan? ttl = null) =>
        Blocking.SetIfAbsentAsync(key, value, ttl).GetAwaiter().GetResult();

    public List<byte[]?> GetMany(IReadOnlyList<byte[]> keys) =>
        Blocking.GetManyAsync(keys).GetAwaiter().GetResult();

    public void SetMany(IReadOnlyList<KeyValuePair<byte[], byte[]>> pairs) =>
        Blocking.SetManyAsync(pairs).GetAwaiter().GetResult();

    public long Delete(ReadOnlySpan<byte> key) =>
        Blocking.DeleteAsync(key).GetAwaiter().GetResult();

    public long Delete(string key) =>
        Blocking.DeleteAsync(key).GetAwaiter().GetResult();

    public long Delete(IReadOnlyList<byte[]> keys) =>
        Blocking.DeleteAsync(keys).GetAwaiter().GetResult();

    public bool Exists(ReadOnlySpan<byte> key) =>
        Blocking.ExistsAsync(key).GetAwaiter().GetResult();

    public bool Exists(string key) =>
        Blocking.ExistsAsync(key).GetAwaiter().GetResult();

    public long Increment(ReadOnlySpan<byte> key, long by = 1) =>
        Blocking.IncrementAsync(key, by).GetAwaiter().GetResult();

    public long Increment(string key, long by = 1) =>
        Blocking.IncrementAsync(key, by).GetAwaiter().GetResult();

    public bool Expire(ReadOnlySpan<byte> key, TimeSpan ttl) =>
        Blocking.ExpireAsync(key, ttl).GetAwaiter().GetResult();

    public bool Expire(string key, TimeSpan ttl) =>
        Blocking.ExpireAsync(key, ttl).GetAwaiter().GetResult();

    public bool Persist(ReadOnlySpan<byte> key) =>
        Blocking.PersistAsync(key).GetAwaiter().GetResult();

    public TimeSpan? Ttl(ReadOnlySpan<byte> key) =>
        Blocking.TtlAsync(key).GetAwaiter().GetResult();

    public TimeSpan? Ttl(string key) =>
        Blocking.TtlAsync(key).GetAwaiter().GetResult();

    public List<byte[]> Keys(ReadOnlyMemory<byte> pattern) =>
        Blocking.KeysAsync(pattern).GetAwaiter().GetResult();

    public List<byte[]> Keys(string pattern) =>
        Blocking.KeysAsync(pattern).GetAwaiter().GetResult();

    public long HashSet(ReadOnlySpan<byte> key, IReadOnlyList<KeyValuePair<byte[], byte[]>> fields) =>
        Blocking.HashSetAsync(key, fields).GetAwaiter().GetResult();

    public long HashSet(string key, string field, string value) =>
        Blocking.HashSetAsync(key, field, value).GetAwaiter().GetResult();

    public byte[]? HashGet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> field) =>
        Blocking.HashGetAsync(key, field).GetAwaiter().GetResult();

    public byte[]? HashGet(string key, string field) =>
        Blocking.HashGetAsync(key, field).GetAwaiter().GetResult();

    public List<KeyValuePair<byte[], byte[]>> HashGetAll(ReadOnlySpan<byte> key) =>
        Blocking.HashGetAllAsync(key).GetAwaiter().GetResult();

    public List<KeyValuePair<byte[], byte[]>> HashGetAll(string key) =>
        Blocking.HashGetAllAsync(key).GetAwaiter().GetResult();

    public long HashDelete(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> fields) =>
        Blocking.HashDeleteAsync(key, fields).GetAwaiter().GetResult();

    public long HashIncrement(ReadOnlySpan<byte> key, ReadOnlySpan<byte> field, long by = 1) =>
        Blocking.HashIncrementAsync(key, field, by).GetAwaiter().GetResult();

    public long ListRightPush(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> values) =>
        Blocking.ListRightPushAsync(key, values).GetAwaiter().GetResult();

    public long ListLeftPush(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> values) =>
        Blocking.ListLeftPushAsync(key, values).GetAwaiter().GetResult();

    public byte[]? ListLeftPop(ReadOnlySpan<byte> key) =>
        Blocking.ListLeftPopAsync(key).GetAwaiter().GetResult();

    public byte[]? ListRightPop(ReadOnlySpan<byte> key) =>
        Blocking.ListRightPopAsync(key).GetAwaiter().GetResult();

    public List<byte[]> ListRange(ReadOnlySpan<byte> key, long start, long stop) =>
        Blocking.ListRangeAsync(key, start, stop).GetAwaiter().GetResult();

    public long ListLength(ReadOnlySpan<byte> key) =>
        Blocking.ListLengthAsync(key).GetAwaiter().GetResult();

    public long SetAdd(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members) =>
        Blocking.SetAddAsync(key, members).GetAwaiter().GetResult();

    public long SetRemove(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members) =>
        Blocking.SetRemoveAsync(key, members).GetAwaiter().GetResult();

    public bool SetContains(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member) =>
        Blocking.SetContainsAsync(key, member).GetAwaiter().GetResult();

    public List<byte[]> SetMembers(ReadOnlySpan<byte> key) =>
        Blocking.SetMembersAsync(key).GetAwaiter().GetResult();

    public long SetCount(ReadOnlySpan<byte> key) =>
        Blocking.SetCountAsync(key).GetAwaiter().GetResult();

    public long SortedSetAdd(ReadOnlySpan<byte> key, IReadOnlyList<KeyValuePair<byte[], double>> members) =>
        Blocking.SortedSetAddAsync(key, members).GetAwaiter().GetResult();

    public double? SortedSetScore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member) =>
        Blocking.SortedSetScoreAsync(key, member).GetAwaiter().GetResult();

    public List<KeyValuePair<byte[], double>> SortedSetRange(ReadOnlySpan<byte> key, long start, long stop, bool descending = false) =>
        Blocking.SortedSetRangeAsync(key, start, stop, descending).GetAwaiter().GetResult();

    public long SortedSetRemove(ReadOnlySpan<byte> key, IReadOnlyList<byte[]> members) =>
        Blocking.SortedSetRemoveAsync(key, members).GetAwaiter().GetResult();

    public double SortedSetIncrement(ReadOnlySpan<byte> key, ReadOnlySpan<byte> member, double by) =>
        Blocking.SortedSetIncrementAsync(key, member, by).GetAwaiter().GetResult();
}

/// <content>The synchronous calls: each is its <c>Async</c> twin, sent with blocking I/O on the calling thread.</content>
public readonly partial struct RasterizedDbKvMap<TKey, TValue>
    where TKey : notnull
{
    public (bool Found, TValue Value) TryGet(TKey key) =>
        Blocking.TryGetAsync(key).GetAwaiter().GetResult();

    public TValue? Get(TKey key) =>
        Blocking.GetAsync(key).GetAwaiter().GetResult();

    public void Set(TKey key, TValue value, TimeSpan? ttl = null) =>
        Blocking.SetAsync(key, value, ttl).GetAwaiter().GetResult();

    public bool SetIfAbsent(TKey key, TValue value, TimeSpan? ttl = null) =>
        Blocking.SetIfAbsentAsync(key, value, ttl).GetAwaiter().GetResult();

    public bool Remove(TKey key) =>
        Blocking.RemoveAsync(key).GetAwaiter().GetResult();

    public bool Contains(TKey key) =>
        Blocking.ContainsAsync(key).GetAwaiter().GetResult();

    public bool Expire(TKey key, TimeSpan ttl) =>
        Blocking.ExpireAsync(key, ttl).GetAwaiter().GetResult();

    public TimeSpan? Ttl(TKey key) =>
        Blocking.TtlAsync(key).GetAwaiter().GetResult();

    public long Increment(TKey key, long by = 1) =>
        Blocking.IncrementAsync(key, by).GetAwaiter().GetResult();

    public List<TValue?> GetMany(IReadOnlyList<TKey> keys) =>
        Blocking.GetManyAsync(keys).GetAwaiter().GetResult();

    public List<TKey> Keys() =>
        Blocking.KeysAsync().GetAwaiter().GetResult();

    public long Clear() =>
        Blocking.ClearAsync().GetAwaiter().GetResult();
}

/// <content>The synchronous calls: each is its <c>Async</c> twin, sent with blocking I/O on the calling thread.</content>
public sealed partial class RasterizedDbKvQuery<TKey, TValue>
    where TKey : notnull
{
    public List<KeyValuePair<TKey, TValue>> ToList() =>
        Blocking.ToListAsync().GetAwaiter().GetResult();

    public List<TKey> Keys() =>
        Blocking.KeysAsync().GetAwaiter().GetResult();

    public int Count() =>
        Blocking.CountAsync().GetAwaiter().GetResult();

    public long Remove() =>
        Blocking.RemoveAsync().GetAwaiter().GetResult();
}

/// <content>The synchronous calls: each is its <c>Async</c> twin, sent with blocking I/O on the calling thread.</content>
public sealed partial class RasterizedDbKvRepository<T, TKey>
    where T : class, new()
    where TKey : notnull
{
    public void Save(T item, TimeSpan? ttl = null) =>
        Blocking.SaveAsync(item, ttl).GetAwaiter().GetResult();

    public T? Get(TKey key) =>
        Blocking.GetAsync(key).GetAwaiter().GetResult();

    public void SetField(TKey key, string property, object value) =>
        Blocking.SetFieldAsync(key, property, value).GetAwaiter().GetResult();

    public long Increment(TKey key, string property, long by = 1) =>
        Blocking.IncrementAsync(key, property, by).GetAwaiter().GetResult();

    public bool Remove(TKey key) =>
        Blocking.RemoveAsync(key).GetAwaiter().GetResult();

    public bool Contains(TKey key) =>
        Blocking.ContainsAsync(key).GetAwaiter().GetResult();

    public bool Expire(TKey key, TimeSpan ttl) =>
        Blocking.ExpireAsync(key, ttl).GetAwaiter().GetResult();
}
