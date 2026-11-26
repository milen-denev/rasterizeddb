using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Text;

namespace RasterizedDb.Messaging;

/// <summary>
/// The broker's one message format, <c>RQB1</c>: a 64-byte header and records, each on an 8-byte
/// boundary, every integer little endian. What a producer sends is what the broker stores and what a
/// consumer receives.
/// </summary>
/// <remarks>
/// <code>
/// header: 0 magic "RQB1" | 4 length | 8 crc (of bytes 12..64) | 12 flags u16 | 14 producer_epoch u16
///         16 base_offset u64 (the server's) | 24 base_time i64 µs (the server's, unless producer_time)
///         32 count | 36 max_time_delta | 40 producer_id u64 | 48 base_sequence | 52 records_len
///         56 raw_len | 60 records_crc (of bytes 64..length)
/// record: 0 length (padding included) | 4 time_delta µs | 8 key_len (0xFFFFFFFF = none) | 12 value_len
///         16 header_count u16 | 18 reserved | 20 key, value, headers (name_len u16, value_len u32, name, value)…
///         zero padding to 8
/// </code>
/// </remarks>
internal static class BrokerBatch
{
    public const int Header = 64;
    public const int RecordHeader = 20;
    public const uint NoKey = uint.MaxValue;
    public const int MaxBatch = 1 << 30;

    public const int OLength = 4;
    public const int OCrc = 8;
    public const int OFlags = 12;
    public const int OBaseOffset = 16;
    public const int OBaseTime = 24;
    public const int OCount = 32;
    public const int OMaxDelta = 36;
    public const int ORecordsLength = 52;
    public const int ORawLength = 56;
    public const int ORecordsCrc = 60;

    public const ushort CodecMask = 0b111;
    public const ushort FlagKeys = 1 << 3;
    public const ushort FlagHeaders = 1 << 4;
    public const ushort FlagUniform = 1 << 5;
    public const ushort FlagProducerTime = 1 << 8;

    public static ReadOnlySpan<byte> Magic => "RQB1"u8;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Pad8(int n) => (n + 7) & ~7;

    public static BrokerHeader[] ParseHeaders(ReadOnlyMemory<byte> bytes, int count)
    {
        if (count == 0)
        {
            return [];
        }

        var headers = new BrokerHeader[count];
        var span = bytes.Span;
        var at = 0;
        for (var i = 0; i < count; i++)
        {
            var nameLength = BinaryPrimitives.ReadUInt16LittleEndian(span[at..]);
            var valueLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(span[(at + 2)..]);
            headers[i] = new BrokerHeader(Encoding.UTF8.GetString(span.Slice(at + 6, nameLength)), bytes.Slice(at + 6 + nameLength, valueLength));
            at += 6 + nameLength + valueLength;
        }

        return headers;
    }

    /// <summary>
    /// The messages of the batches in <paramref name="data"/> whose offsets <paramref name="wanted"/>
    /// accepts, as slices of it. Each batch is checked (lengths, both CRCs) before a record of it is read.
    /// </summary>
    public static void Read(ReadOnlyMemory<byte> data, uint partition, Func<ulong, bool>? wanted, List<BrokerMessage> into)
    {
        var at = 0;
        while (data.Length - at >= Header)
        {
            var span = data.Span[at..];
            if (!span[..4].SequenceEqual(Magic))
            {
                throw new BrokerException("not a batch: bad magic");
            }

            var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(span[OLength..]);
            if (length < Header + RecordHeader || length % 8 != 0 || length > span.Length)
            {
                throw new BrokerException($"implausible batch length {length}");
            }

            span = span[..length];
            if (Crc32.HashToUInt32(span[OFlags..Header]) != BinaryPrimitives.ReadUInt32LittleEndian(span[OCrc..]))
            {
                throw new BrokerException("batch header CRC mismatch");
            }

            if (Crc32.HashToUInt32(span[Header..]) != BinaryPrimitives.ReadUInt32LittleEndian(span[ORecordsCrc..]))
            {
                throw new BrokerException("batch records CRC mismatch");
            }

            var flags = BinaryPrimitives.ReadUInt16LittleEndian(span[OFlags..]);
            if ((flags & CodecMask) != 0)
            {
                throw new BrokerException("the batch is compressed (zstd), which this client does not read yet");
            }

            var baseOffset = BinaryPrimitives.ReadUInt64LittleEndian(span[OBaseOffset..]);
            var baseTime = BinaryPrimitives.ReadInt64LittleEndian(span[OBaseTime..]);
            var count = BinaryPrimitives.ReadUInt32LittleEndian(span[OCount..]);
            var p = Header;
            for (uint i = 0; i < count; i++)
            {
                if (length - p < RecordHeader)
                {
                    throw new BrokerException($"record {i}: truncated header");
                }

                var record = span[p..];
                var recordLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(record);
                var keyLength = BinaryPrimitives.ReadUInt32LittleEndian(record[8..]);
                var valueLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(record[12..]);
                var headerCount = BinaryPrimitives.ReadUInt16LittleEndian(record[16..]);
                var hasKey = keyLength != NoKey;
                var keyBytes = hasKey ? (int)keyLength : 0;
                if (recordLength < RecordHeader || recordLength % 8 != 0 || recordLength > record.Length
                    || (long)RecordHeader + keyBytes + valueLength > recordLength)
                {
                    throw new BrokerException($"record {i}: bad length");
                }

                var offset = baseOffset + i;
                if (wanted is null || wanted(offset))
                {
                    var start = at + p + RecordHeader;
                    var headersAt = start + keyBytes + valueLength;
                    into.Add(new BrokerMessage(
                        partition,
                        offset,
                        DateTime.UnixEpoch.AddTicks((baseTime + BinaryPrimitives.ReadUInt32LittleEndian(record[4..])) * 10),
                        hasKey,
                        data.Slice(start, keyBytes),
                        data.Slice(start + keyBytes, valueLength),
                        data.Slice(headersAt, at + p + recordLength - headersAt),
                        headerCount));
                }

                p += recordLength;
            }

            at += length;
        }
    }
}

/// <summary>
/// Builds one batch of messages to produce. Reusable: <see cref="Seal"/> finishes the batch, and
/// <see cref="Reset"/> starts the next in the same buffer.
/// </summary>
public sealed class BrokerBatchBuilder : IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(4096);
    private int _length = BrokerBatch.Header;
    private uint _count;
    private ushort _flags;
    private int _uniformLength;
    private bool _sealed;

    /// <summary>Messages added so far.</summary>
    public int Count => (int)_count;

    /// <summary>The batch's size in bytes so far, its header included.</summary>
    public int Length => _length;

    private void Ensure(int more)
    {
        if (_buffer.Length - _length >= more)
        {
            return;
        }

        var next = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _length + more));
        _buffer.AsSpan(0, _length).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }

    /// <summary>Adds a message: its value, an optional key (which decides its partition), and headers.</summary>
    public BrokerBatchBuilder Add(ReadOnlySpan<byte> value, ReadOnlySpan<byte> key = default, bool hasKey = false, IReadOnlyList<BrokerHeader>? headers = null)
    {
        if (_sealed)
        {
            throw new InvalidOperationException("the batch is sealed; Reset starts the next");
        }

        long size = BrokerBatch.RecordHeader + (hasKey ? key.Length : 0) + value.Length;
        var headerCount = headers?.Count ?? 0;
        if (headerCount > ushort.MaxValue)
        {
            throw new ArgumentException("a message has at most 65535 headers");
        }

        for (var i = 0; i < headerCount; i++)
        {
            var nameBytes = Encoding.UTF8.GetByteCount(headers![i].Name);
            if (nameBytes > ushort.MaxValue)
            {
                throw new ArgumentException("a header's name is at most 65535 bytes");
            }

            size += 6 + nameBytes + headers[i].Value.Length;
        }

        size = (size + 7) & ~7L;
        if (_length + size > BrokerBatch.MaxBatch)
        {
            throw new BrokerException($"batch would pass {BrokerBatch.MaxBatch} bytes");
        }

        var recordLength = (int)size;
        Ensure(recordLength);
        var record = _buffer.AsSpan(_length, recordLength);
        record.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(record, (uint)recordLength);
        BinaryPrimitives.WriteUInt32LittleEndian(record[8..], hasKey ? (uint)key.Length : BrokerBatch.NoKey);
        BinaryPrimitives.WriteUInt32LittleEndian(record[12..], (uint)value.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(record[16..], (ushort)headerCount);
        var at = BrokerBatch.RecordHeader;
        if (hasKey)
        {
            key.CopyTo(record[at..]);
            at += key.Length;
            _flags |= BrokerBatch.FlagKeys;
        }

        value.CopyTo(record[at..]);
        at += value.Length;
        for (var i = 0; i < headerCount; i++)
        {
            var header = headers![i];
            var nameBytes = Encoding.UTF8.GetBytes(header.Name, record[(at + 6)..]);
            BinaryPrimitives.WriteUInt16LittleEndian(record[at..], (ushort)nameBytes);
            BinaryPrimitives.WriteUInt32LittleEndian(record[(at + 2)..], (uint)header.Value.Length);
            header.Value.Span.CopyTo(record[(at + 6 + nameBytes)..]);
            at += 6 + nameBytes + header.Value.Length;
        }

        if (headerCount > 0)
        {
            _flags |= BrokerBatch.FlagHeaders;
        }

        _uniformLength = _count == 0 ? recordLength : _uniformLength == recordLength ? recordLength : 0;
        _length += recordLength;
        _count++;
        return this;
    }

    public BrokerBatchBuilder Add(ReadOnlySpan<byte> value, ReadOnlySpan<byte> key, IReadOnlyList<BrokerHeader>? headers = null) =>
        Add(value, key, hasKey: true, headers);

    public BrokerBatchBuilder Add(string value, string? key = null, IReadOnlyList<BrokerHeader>? headers = null) =>
        key is null ? Add(Encoding.UTF8.GetBytes(value), default, false, headers) : Add(Encoding.UTF8.GetBytes(value), Encoding.UTF8.GetBytes(key), true, headers);

    /// <summary>
    /// Finishes the batch: writes its header and both checksums. The bytes are the builder's own until
    /// <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public ReadOnlyMemory<byte> Seal()
    {
        if (_count == 0)
        {
            throw new InvalidOperationException("a batch holds at least one message");
        }

        if (!_sealed)
        {
            var b = _buffer.AsSpan(0, _length);
            var header = b[..BrokerBatch.Header];
            header.Clear();
            BrokerBatch.Magic.CopyTo(header);
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.OLength..], (uint)_length);
            BinaryPrimitives.WriteUInt16LittleEndian(header[BrokerBatch.OFlags..], (ushort)(_flags | (_count > 1 && _uniformLength != 0 ? BrokerBatch.FlagUniform : 0)));
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.OCount..], _count);
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.ORecordsLength..], (uint)(_length - BrokerBatch.Header));
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.ORawLength..], (uint)(_length - BrokerBatch.Header));
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.ORecordsCrc..], Crc32.HashToUInt32(b[BrokerBatch.Header..]));
            // Last: it covers the header's other fields, the records' checksum among them.
            BinaryPrimitives.WriteUInt32LittleEndian(header[BrokerBatch.OCrc..], Crc32.HashToUInt32(header[BrokerBatch.OFlags..]));
            _sealed = true;
        }

        return _buffer.AsMemory(0, _length);
    }

    /// <summary>Empties the builder for the next batch, keeping its buffer.</summary>
    public void Reset()
    {
        _length = BrokerBatch.Header;
        _count = 0;
        _flags = 0;
        _uniformLength = 0;
        _sealed = false;
    }

    public void Dispose()
    {
        var buffer = _buffer;
        _buffer = [];
        if (buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
