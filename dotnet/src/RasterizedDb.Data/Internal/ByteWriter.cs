using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>
/// A growable byte buffer over <see cref="ArrayPool{T}"/>, kept by a connection and reused for every
/// request it builds: after the first few statements nothing is allocated to send one.
/// </summary>
internal sealed class ByteWriter : IDisposable
{
    private byte[] _buffer;
    private int _length;

    /// <summary>A buffer that has grown past this is given back to the pool after the request.</summary>
    private const int KeepLimit = 256 * 1024;

    public ByteWriter(int initialCapacity = 1024)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    public int Length
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _length;
    }

    public ReadOnlySpan<byte> WrittenSpan
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => new(_buffer, 0, _length);
    }

    public ReadOnlyMemory<byte> WrittenMemory => new(_buffer, 0, _length);

    public byte[] UnderlyingArray => _buffer;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => _length = 0;

    /// <summary>Forgets what was written and drops an oversized buffer.</summary>
    public void Reset()
    {
        _length = 0;
        if (_buffer.Length > KeepLimit)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(1024);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint)
    {
        if (_buffer.Length - _length < sizeHint)
        {
            Grow(sizeHint);
        }

        return _buffer.AsSpan(_length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count) => _length += count;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int sizeHint)
    {
        var needed = checked(_length + sizeHint);
        var size = Math.Max(needed, _buffer.Length * 2);
        var next = ArrayPool<byte>.Shared.Rent(size);
        Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetArrayDataReference(next), ref MemoryMarshal.GetArrayDataReference(_buffer), (uint)_length);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteByte(byte value)
    {
        if (_length == _buffer.Length)
        {
            Grow(1);
        }

        Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), _length++) = value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(GetSpan(bytes.Length));
        _length += bytes.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(GetSpan(2), value);
        _length += 2;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(GetSpan(4), value);
        _length += 4;
    }

    /// <summary>Overwrites four bytes written earlier: a length known only once its payload is.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PatchUInt32(int at, uint value) => BinaryPrimitives.WriteUInt32LittleEndian(_buffer.AsSpan(at, 4), value);

    public void WriteUtf8(ReadOnlySpan<char> text)
    {
        var span = GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        _length += Encoding.UTF8.GetBytes(text, span);
    }

    /// <summary>ASCII known to be ASCII: one narrowing pass, no encoder.</summary>
    public void WriteAscii(ReadOnlySpan<char> text)
    {
        var span = GetSpan(text.Length);
        Encoding.ASCII.GetBytes(text, span);
        _length += text.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt64Text(long value)
    {
        Utf8Formatter.TryFormat(value, GetSpan(20), out var written);
        _length += written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUInt64Text(ulong value)
    {
        Utf8Formatter.TryFormat(value, GetSpan(20), out var written);
        _length += written;
    }

    /// <summary>Anything that formats itself as UTF-8 (every numeric type, Guid, the date types).</summary>
    public void WriteFormatted<T>(T value, ReadOnlySpan<char> format = default) where T : IUtf8SpanFormattable
    {
        var hint = 64;
        while (true)
        {
            if (value.TryFormat(GetSpan(hint), out var written, format, System.Globalization.CultureInfo.InvariantCulture))
            {
                _length += written;
                return;
            }

            hint *= 2;
        }
    }

    public void WriteHex(ReadOnlySpan<byte> bytes)
    {
        var span = GetSpan(bytes.Length * 2);
        ref var table = ref MemoryMarshal.GetReference("0123456789abcdef"u8);
        ref var dst = ref MemoryMarshal.GetReference(span);
        for (var i = 0; i < bytes.Length; i++)
        {
            var b = bytes[i];
            Unsafe.Add(ref dst, i * 2) = Unsafe.Add(ref table, b >> 4);
            Unsafe.Add(ref dst, i * 2 + 1) = Unsafe.Add(ref table, b & 0xF);
        }

        _length += bytes.Length * 2;
    }

    public void Dispose()
    {
        var buffer = _buffer;
        _buffer = [];
        _length = 0;
        if (buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
