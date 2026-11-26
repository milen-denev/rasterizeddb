using System.Buffers.Binary;
using System.Buffers.Text;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>The one-byte type tag that ends every native cell (<c>core/row/row.rs</c>).</summary>
internal static class Tag
{
    public const byte I8 = 1;
    public const byte I16 = 2;
    public const byte I32 = 3;
    public const byte I64 = 4;
    public const byte I128 = 5;
    public const byte U8 = 6;
    public const byte U16 = 7;
    public const byte U32 = 8;
    public const byte U64 = 9;
    public const byte U128 = 10;
    public const byte F32 = 11;
    public const byte F64 = 12;
    public const byte Char = 13;
    public const byte String = 14;
    /// <summary><c>timestamptz</c>: little-endian i64 microseconds since the Unix epoch.</summary>
    public const byte DateTime = 15;
    public const byte Bool = 16;
    public const byte Uuid = 17;
    public const byte Inet = 18;
    public const byte Decimal = 19;
    public const byte Bytea = 20;
    public const byte Array = 21;
    public const byte Jsonb = 22;
    public const byte Json = 23;
    public const byte Interval = 24;
    public const byte Date = 29;
    public const byte Time = 30;
    /// <summary><c>timestamp</c> (no zone): the same eight bytes as <see cref="DateTime"/>.</summary>
    public const byte Timestamp = 46;
    public const byte JsonText = 47;
    public const byte Null = 253;

    /// <summary>
    /// Not a server tag: a cell of a simple-query answer, which is PostgreSQL's text for its type
    /// whatever the type (a <c>bytea</c> is <c>\x…</c> there, not its bytes).
    /// </summary>
    public const byte PgText = 0xF0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsText(byte tag) => tag is String or PgText or Char or Json or JsonText;
}

internal delegate T CellReader<T>(byte tag, ReadOnlySpan<byte> raw);

/// <summary>
/// Cells as CLR values. A native cell is read by its tag; every type also reads from text, so a column
/// selected as <c>col::text</c>, and every cell of a simple-query answer, reads back as its own type.
/// </summary>
internal static class ValueReader
{
    private const long UnixEpochTicks = 621_355_968_000_000_000;
    private const long MaxTicks = 3_155_378_975_999_999_999;
    /// <summary>Days from 1970-01-01 to 2000-01-01, PostgreSQL's date epoch.</summary>
    private const int PgEpochDays = 10_957;
    private const int UnixEpochDayNumber = 719_162;

    private const string NumericNoScale =
        "a NUMERIC arrived without its scale (an older server sends none for a NUMERIC(p,s) column or sum() over bigint), " +
        "so its value cannot be known: select it as col::text or cast it, e.g. sum(x)::bigint";

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException Bad(byte tag, string what) =>
        new(tag == Tag.PgText ? $"the cell is not {what}" : $"a cell with type tag {tag} is not {what}");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException BadText(ReadOnlySpan<byte> raw, string what) =>
        new($"\"{Encoding.UTF8.GetString(raw.Length > 64 ? raw[..64] : raw)}\" is not {what}");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException BadSize(byte tag, int expected, int got) =>
        new($"a cell with type tag {tag} needs {expected} byte(s), it has {got}");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Size(byte tag, ReadOnlySpan<byte> raw, int expected)
    {
        if (raw.Length != expected)
        {
            throw BadSize(tag, expected, raw.Length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T Le<T>(ReadOnlySpan<byte> raw) where T : unmanaged
    {
        // Every native integer and float is little endian, which is every platform .NET runs on today;
        // the swap keeps a big-endian one right.
        var value = Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(raw));
        return BitConverter.IsLittleEndian ? value : Reverse(value);
    }

    private static T Reverse<T>(T value) where T : unmanaged
    {
        var bytes = MemoryMarshal.AsBytes(new Span<T>(ref value));
        bytes.Reverse();
        return value;
    }

    private static ReadOnlySpan<byte> Trim(ReadOnlySpan<byte> raw) => raw.Trim(" \t\r\n"u8);

    // ------------------------------------------------------------------ integers

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(byte tag, ReadOnlySpan<byte> raw)
    {
        if (tag == Tag.I64 && raw.Length == 8)
        {
            return Le<long>(raw);
        }

        if (tag == Tag.I32 && raw.Length == 4)
        {
            return Le<int>(raw);
        }

        return ReadInt64Slow(tag, raw);
    }

    private static long ReadInt64Slow(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.I8: Size(tag, raw, 1); return (sbyte)raw[0];
            case Tag.I16: Size(tag, raw, 2); return Le<short>(raw);
            case Tag.I32: Size(tag, raw, 4); return Le<int>(raw);
            case Tag.I64: Size(tag, raw, 8); return Le<long>(raw);
            case Tag.U8: Size(tag, raw, 1); return raw[0];
            case Tag.U16: Size(tag, raw, 2); return Le<ushort>(raw);
            case Tag.U32: Size(tag, raw, 4); return Le<uint>(raw);
            case Tag.U64: Size(tag, raw, 8); return checked((long)Le<ulong>(raw));
            case Tag.I128 or Tag.U128 or Tag.Decimal: return checked((long)ReadInt128(tag, raw));
            case Tag.Bool: Size(tag, raw, 1); return raw[0] != 0 ? 1 : 0;
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (Utf8Parser.TryParse(text, out long value, out var consumed) && consumed == text.Length)
            {
                return value;
            }

            // "12.000" from a numeric cast to text.
            return checked((long)ReadInt128(tag, raw));
        }

        throw Bad(tag, "an integer");
    }

    public static Int128 ReadInt128(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.I128: Size(tag, raw, 16); return Le<Int128>(raw);
            case Tag.U128: Size(tag, raw, 16); return checked((Int128)Le<UInt128>(raw));
            case Tag.U64: Size(tag, raw, 8); return Le<ulong>(raw);
            case Tag.Decimal:
            {
                var (unscaled, scale) = NumericParts(raw);
                if (scale == 0)
                {
                    return unscaled;
                }

                if (scale > 38)
                {
                    return unscaled == 0 ? 0 : throw new InvalidCastException($"{DecimalText(unscaled, scale)} is not a whole number");
                }

                var factor = Pow10(scale);
                var (quotient, remainder) = Int128.DivRem(unscaled, factor);
                return remainder == 0 ? quotient : throw new InvalidCastException($"{DecimalText(unscaled, scale)} is not a whole number");
            }
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (Int128.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value))
            {
                return value;
            }

            if (decimal.TryParse(text, NumberStyles.Number & ~NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var number) && decimal.Truncate(number) == number)
            {
                return (Int128)number;
            }

            throw BadText(raw, "an integer");
        }

        return ReadInt64Slow(tag, raw);
    }

    private static Int128 Pow10(int scale)
    {
        Int128 value = 1;
        for (var i = 0; i < scale; i++)
        {
            value *= 10;
        }

        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(byte tag, ReadOnlySpan<byte> raw) =>
        tag == Tag.I32 && raw.Length == 4 ? Le<int>(raw) : checked((int)ReadInt64(tag, raw));

    public static bool ReadBoolean(byte tag, ReadOnlySpan<byte> raw)
    {
        if (tag == Tag.Bool && raw.Length == 1)
        {
            return raw[0] != 0;
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (text.Length == 1)
            {
                switch (text[0])
                {
                    case (byte)'t' or (byte)'1': return true;
                    case (byte)'f' or (byte)'0': return false;
                }
            }

            if (Ascii.EqualsIgnoreCase(text, "true"u8) || Ascii.EqualsIgnoreCase(text, "yes"u8) || Ascii.EqualsIgnoreCase(text, "on"u8))
            {
                return true;
            }

            if (Ascii.EqualsIgnoreCase(text, "false"u8) || Ascii.EqualsIgnoreCase(text, "no"u8) || Ascii.EqualsIgnoreCase(text, "off"u8))
            {
                return false;
            }

            throw BadText(raw, "a boolean");
        }

        if (tag is Tag.I8 or Tag.I16 or Tag.I32 or Tag.I64 or Tag.U8 or Tag.U16 or Tag.U32 or Tag.U64)
        {
            return ReadInt64(tag, raw) != 0;
        }

        throw Bad(tag, "a boolean");
    }

    // -------------------------------------------------------------------- floats

    public static double ReadDouble(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.F64: Size(tag, raw, 8); return Le<double>(raw);
            case Tag.F32: Size(tag, raw, 4); return Le<float>(raw);
            case Tag.Decimal:
            {
                var (unscaled, scale) = NumericParts(raw);
                return double.Parse(DecimalText(unscaled, scale), CultureInfo.InvariantCulture);
            }
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (Utf8Parser.TryParse(text, out double value, out var consumed) && consumed == text.Length)
            {
                return value;
            }

            if (text.SequenceEqual("NaN"u8))
            {
                return double.NaN;
            }

            if (text.SequenceEqual("Infinity"u8))
            {
                return double.PositiveInfinity;
            }

            if (text.SequenceEqual("-Infinity"u8))
            {
                return double.NegativeInfinity;
            }

            throw BadText(raw, "a number");
        }

        return (double)ReadInt128(tag, raw);
    }

    public static float ReadSingle(byte tag, ReadOnlySpan<byte> raw)
    {
        if (tag == Tag.F32)
        {
            Size(tag, raw, 4);
            return Le<float>(raw);
        }

        return (float)ReadDouble(tag, raw);
    }

    // ------------------------------------------------------------------- numeric

    private static (Int128 Unscaled, int Scale) NumericParts(ReadOnlySpan<byte> raw) => raw.Length switch
    {
        17 => (Le<Int128>(raw), raw[16]),
        16 => throw new InvalidCastException(NumericNoScale),
        _ => throw new InvalidCastException($"a NUMERIC cell of {raw.Length} bytes is not one this driver reads"),
    };

    internal static string DecimalText(Int128 unscaled, int scale)
    {
        var negative = unscaled < 0;
        var magnitude = negative ? (UInt128)(-unscaled) : (UInt128)unscaled;
        var digits = magnitude.ToString(CultureInfo.InvariantCulture);
        if (scale == 0)
        {
            return negative ? "-" + digits : digits;
        }

        var builder = new StringBuilder(digits.Length + scale + 3);
        if (negative)
        {
            builder.Append('-');
        }

        if (digits.Length > scale)
        {
            builder.Append(digits, 0, digits.Length - scale).Append('.').Append(digits, digits.Length - scale, scale);
        }
        else
        {
            builder.Append("0.").Append('0', scale - digits.Length).Append(digits);
        }

        return builder.ToString();
    }

    private static decimal ToDecimal(Int128 unscaled, int scale)
    {
        var negative = unscaled < 0;
        var magnitude = negative ? (UInt128)(-unscaled) : (UInt128)unscaled;
        // A decimal holds 96 bits at a scale of at most 28: trailing zeros of the fraction are shed to fit.
        while (scale > 28 || (magnitude >> 96) != UInt128.Zero)
        {
            var (quotient, remainder) = UInt128.DivRem(magnitude, 10);
            if (scale == 0 || remainder != UInt128.Zero)
            {
                throw new OverflowException(
                    $"{DecimalText(unscaled, scale)} does not fit System.Decimal exactly; read it as a string or BigInteger");
            }

            magnitude = quotient;
            scale--;
        }

        return new decimal((int)(uint)magnitude, (int)(uint)(magnitude >> 32), (int)(uint)(magnitude >> 64), negative, (byte)scale);
    }

    public static decimal ReadDecimal(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.Decimal:
            {
                var (unscaled, scale) = NumericParts(raw);
                return ToDecimal(unscaled, scale);
            }
            case Tag.F32 or Tag.F64:
                return (decimal)ReadDouble(tag, raw);
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (Utf8Parser.TryParse(text, out decimal value, out var consumed) && consumed == text.Length)
            {
                return value;
            }

            if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            {
                return value;
            }

            throw BadText(raw, "a number that fits System.Decimal");
        }

        var whole = ReadInt128(tag, raw);
        return ToDecimal(whole, 0);
    }

    // ---------------------------------------------------------------------- text

    public static string ReadString(byte tag, ReadOnlySpan<byte> raw)
    {
        if (Tag.IsText(tag))
        {
            return raw.IsEmpty ? string.Empty : Encoding.UTF8.GetString(raw);
        }

        return tag switch
        {
            Tag.Jsonb => throw JsonbDocument(),
            Tag.Decimal => DecimalTextOf(raw),
            Tag.Bytea => string.Create(raw.Length * 2 + 2, raw.ToArray(), static (span, bytes) =>
            {
                span[0] = '\\';
                span[1] = 'x';
                Convert.ToHexString(bytes).AsSpan().CopyTo(span[2..]);
                for (var i = 2; i < span.Length; i++)
                {
                    span[i] = char.ToLowerInvariant(span[i]);
                }
            }),
            Tag.Bool => ReadBoolean(tag, raw) ? "t" : "f",
            Tag.Uuid => ReadGuid(tag, raw).ToString("D"),
            Tag.F32 => ReadSingle(tag, raw).ToString(CultureInfo.InvariantCulture),
            Tag.F64 => ReadDouble(tag, raw).ToString(CultureInfo.InvariantCulture),
            Tag.DateTime => ReadDateTime(tag, raw).ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture) + "+00",
            Tag.Timestamp => ReadDateTime(tag, raw).ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
            Tag.Date => ReadDate(tag, raw).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            Tag.Time => ReadTime(tag, raw).ToString("HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
            Tag.Interval => ReadInterval(tag, raw).ToString(),
            Tag.Inet => InetText(raw),
            Tag.Array => ArrayText(tag, raw),
            Tag.I128 or Tag.U128 => ReadInt128(tag, raw).ToString(CultureInfo.InvariantCulture),
            Tag.U64 => Le<ulong>(raw).ToString(CultureInfo.InvariantCulture),
            _ => ReadInt64(tag, raw).ToString(CultureInfo.InvariantCulture),
        };
    }

    private static string DecimalTextOf(ReadOnlySpan<byte> raw)
    {
        var (unscaled, scale) = NumericParts(raw);
        return DecimalText(unscaled, scale);
    }

    private static InvalidCastException JsonbDocument() =>
        new("this is a stored JSONB document, not JSON text: select the column as col::text");

    private static string ArrayText(byte tag, ReadOnlySpan<byte> raw)
    {
        var elements = ReadArray<string?>(tag, raw);
        using var w = new ByteWriter(64);
        LiteralWriter.WriteArrayBody(w, elements);
        return Encoding.UTF8.GetString(w.WrittenSpan);
    }

    private static string InetText(ReadOnlySpan<byte> raw)
    {
        // [family][address, 16 bytes big endian][prefix]
        if (raw.Length != 18)
        {
            throw BadSize(Tag.Inet, 18, raw.Length);
        }

        var v4 = raw[0] == 4 || raw[0] == 2;
        var address = v4 ? new System.Net.IPAddress(raw.Slice(13, 4)) : new System.Net.IPAddress(raw.Slice(1, 16));
        var full = v4 ? 32 : 128;
        return raw[17] == full ? address.ToString() : $"{address}/{raw[17]}";
    }

    public static byte[] ReadBytes(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.Bytea or Tag.String or Tag.Char or Tag.Uuid or Tag.Jsonb or Tag.Array or Tag.Inet:
                return raw.ToArray();
            case Tag.PgText:
                if (raw.Length >= 2 && raw[0] == '\\' && raw[1] == 'x')
                {
                    var hex = raw[2..];
                    if ((hex.Length & 1) != 0)
                    {
                        throw BadText(raw, "a bytea");
                    }

                    var bytes = GC.AllocateUninitializedArray<byte>(hex.Length / 2);
                    for (var i = 0; i < bytes.Length; i++)
                    {
                        var hi = HexValue(hex[i * 2]);
                        var lo = HexValue(hex[i * 2 + 1]);
                        if ((hi | lo) < 0)
                        {
                            throw BadText(raw, "a bytea");
                        }

                        bytes[i] = (byte)(hi << 4 | lo);
                    }

                    return bytes;
                }

                return raw.ToArray();
            default:
                throw Bad(tag, "bytes");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int HexValue(byte c) => c switch
    {
        >= (byte)'0' and <= (byte)'9' => c - '0',
        >= (byte)'a' and <= (byte)'f' => c - 'a' + 10,
        >= (byte)'A' and <= (byte)'F' => c - 'A' + 10,
        _ => -1,
    };

    public static Guid ReadGuid(byte tag, ReadOnlySpan<byte> raw)
    {
        if (tag == Tag.Uuid)
        {
            Size(tag, raw, 16);
            return new Guid(raw, bigEndian: true);
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (Utf8Parser.TryParse(text, out Guid value, out var consumed) && consumed == text.Length)
            {
                return value;
            }

            throw BadText(raw, "a UUID");
        }

        throw Bad(tag, "a UUID");
    }

    // ----------------------------------------------------------------- date/time

    private static DateTime FromUnixMicros(long micros, DateTimeKind kind)
    {
        long ticks;
        try
        {
            ticks = checked(micros * 10 + UnixEpochTicks);
        }
        catch (OverflowException)
        {
            ticks = -1;
        }

        if ((ulong)ticks > MaxTicks)
        {
            throw new InvalidCastException($"a timestamp {micros} µs from the Unix epoch is outside System.DateTime's years 1 to 9999");
        }

        return new DateTime(ticks, kind);
    }

    /// <summary>
    /// A <c>timestamptz</c> is an instant and comes back as UTC; a <c>timestamp</c> is a wall-clock time
    /// and comes back unspecified.
    /// </summary>
    public static DateTime ReadDateTime(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.DateTime: Size(tag, raw, 8); return FromUnixMicros(Le<long>(raw), DateTimeKind.Utc);
            case Tag.Timestamp: Size(tag, raw, 8); return FromUnixMicros(Le<long>(raw), DateTimeKind.Unspecified);
            case Tag.Date: return ReadDate(tag, raw).ToDateTime(default);
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (!TryParseTimestamp(text, out var ticks, out var offsetSeconds, out var hasZone))
            {
                throw BadText(raw, "a timestamp");
            }

            return hasZone
                ? new DateTime(ticks - offsetSeconds * TimeSpan.TicksPerSecond, DateTimeKind.Utc)
                : new DateTime(ticks, DateTimeKind.Unspecified);
        }

        throw Bad(tag, "a timestamp");
    }

    public static DateTimeOffset ReadDateTimeOffset(byte tag, ReadOnlySpan<byte> raw)
    {
        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (!TryParseTimestamp(text, out var ticks, out var offsetSeconds, out _))
            {
                throw BadText(raw, "a timestamp");
            }

            return new DateTimeOffset(ticks, TimeSpan.FromSeconds(offsetSeconds));
        }

        var value = ReadDateTime(tag, raw);
        return new DateTimeOffset(value.Ticks, TimeSpan.Zero);
    }

    public static DateOnly ReadDate(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.Date:
            {
                Size(tag, raw, 4);
                var day = (long)BinaryPrimitives.ReadInt32BigEndian(raw) + PgEpochDays + UnixEpochDayNumber;
                if ((ulong)day > (ulong)DateOnly.MaxValue.DayNumber)
                {
                    throw new InvalidCastException("a date outside System.DateOnly's years 1 to 9999");
                }

                return DateOnly.FromDayNumber((int)day);
            }
            case Tag.DateTime or Tag.Timestamp:
                return DateOnly.FromDateTime(ReadDateTime(tag, raw));
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (TryParseTimestamp(text, out var ticks, out _, out _))
            {
                return DateOnly.FromDateTime(new DateTime(ticks));
            }

            throw BadText(raw, "a date");
        }

        throw Bad(tag, "a date");
    }

    public static TimeOnly ReadTime(byte tag, ReadOnlySpan<byte> raw)
    {
        switch (tag)
        {
            case Tag.Time:
            {
                Size(tag, raw, 8);
                var micros = BinaryPrimitives.ReadInt64BigEndian(raw);
                // 24:00:00 is a valid PostgreSQL time and not a TimeOnly.
                if ((ulong)micros >= 86_400_000_000UL)
                {
                    throw new InvalidCastException($"a time of {micros} µs is not a System.TimeOnly");
                }

                return new TimeOnly(micros * 10);
            }
            case Tag.DateTime or Tag.Timestamp:
                return TimeOnly.FromDateTime(ReadDateTime(tag, raw));
        }

        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            var at = 0;
            if (TryParseClock(text, ref at, out var ticks) && (ulong)ticks < (ulong)TimeSpan.TicksPerDay)
            {
                return new TimeOnly(ticks);
            }

            throw BadText(raw, "a time");
        }

        throw Bad(tag, "a time");
    }

    public static RasterizedDbInterval ReadInterval(byte tag, ReadOnlySpan<byte> raw)
    {
        if (tag == Tag.Interval)
        {
            Size(tag, raw, 16);
            return new RasterizedDbInterval(
                BinaryPrimitives.ReadInt32BigEndian(raw[12..]),
                BinaryPrimitives.ReadInt32BigEndian(raw[8..]),
                BinaryPrimitives.ReadInt64BigEndian(raw));
        }

        if (Tag.IsText(tag))
        {
            if (TryParseInterval(Trim(raw), out var value))
            {
                return value;
            }

            throw BadText(raw, "an interval");
        }

        throw Bad(tag, "an interval");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Digits(ReadOnlySpan<byte> text, ref int at, int min, int max, out int value)
    {
        value = 0;
        var start = at;
        while (at < text.Length && at - start < max && (uint)(text[at] - '0') <= 9)
        {
            value = value * 10 + (text[at] - '0');
            at++;
        }

        return at - start >= min;
    }

    /// <summary><c>HH:MM[:SS[.ffffff]]</c> at <paramref name="at"/>, as ticks.</summary>
    private static bool TryParseClock(ReadOnlySpan<byte> text, ref int at, out long ticks)
    {
        ticks = 0;
        if (!Digits(text, ref at, 1, 4, out var hours) || at >= text.Length || text[at] != ':')
        {
            return false;
        }

        at++;
        if (!Digits(text, ref at, 2, 2, out var minutes))
        {
            return false;
        }

        var seconds = 0;
        long fraction = 0;
        if (at < text.Length && text[at] == ':')
        {
            at++;
            if (!Digits(text, ref at, 2, 2, out seconds))
            {
                return false;
            }

            if (at < text.Length && text[at] == '.')
            {
                at++;
                var start = at;
                var scale = 1_000_000L;
                while (at < text.Length && (uint)(text[at] - '0') <= 9)
                {
                    if (at - start < 6)
                    {
                        scale /= 10;
                        fraction += (text[at] - '0') * scale;
                    }

                    at++;
                }

                if (at == start)
                {
                    return false;
                }
            }
        }

        if (minutes > 59 || seconds > 60)
        {
            return false;
        }

        ticks = ((hours * 60L + minutes) * 60 + seconds) * TimeSpan.TicksPerSecond + fraction * 10;
        return true;
    }

    /// <summary>
    /// PostgreSQL's timestamp text: <c>YYYY-MM-DD[ HH:MM:SS[.ffffff]][±HH[:MM[:SS]]|Z]</c>, with
    /// <c>T</c> accepted between date and time.
    /// </summary>
    private static bool TryParseTimestamp(ReadOnlySpan<byte> text, out long ticks, out int offsetSeconds, out bool hasZone)
    {
        ticks = 0;
        offsetSeconds = 0;
        hasZone = false;
        var at = 0;
        if (!Digits(text, ref at, 4, 4, out var year) || at >= text.Length || text[at++] != '-'
            || !Digits(text, ref at, 2, 2, out var month) || at >= text.Length || text[at++] != '-'
            || !Digits(text, ref at, 2, 2, out var day))
        {
            return false;
        }

        if (year is < 1 or > 9999 || month is < 1 or > 12 || day < 1 || day > DateTime.DaysInMonth(year, month))
        {
            return false;
        }

        ticks = new DateTime(year, month, day).Ticks;
        if (at == text.Length)
        {
            return true;
        }

        if (text[at] is (byte)' ' or (byte)'T')
        {
            at++;
            if (!TryParseClock(text, ref at, out var clock))
            {
                return false;
            }

            ticks += clock;
        }

        if (at == text.Length)
        {
            return true;
        }

        if (text[at] == ' ')
        {
            at++;
        }

        if (at < text.Length && text[at] is (byte)'Z' or (byte)'z')
        {
            hasZone = true;
            return at + 1 == text.Length;
        }

        if (at < text.Length && text[at] is (byte)'+' or (byte)'-')
        {
            var negative = text[at++] == '-';
            if (!Digits(text, ref at, 1, 2, out var hours))
            {
                return false;
            }

            var minutes = 0;
            var seconds = 0;
            if (at < text.Length && text[at] == ':')
            {
                at++;
                if (!Digits(text, ref at, 2, 2, out minutes))
                {
                    return false;
                }

                if (at < text.Length && text[at] == ':')
                {
                    at++;
                    if (!Digits(text, ref at, 2, 2, out seconds))
                    {
                        return false;
                    }
                }
            }

            offsetSeconds = (hours * 60 + minutes) * 60 + seconds;
            if (negative)
            {
                offsetSeconds = -offsetSeconds;
            }

            hasZone = true;
            return at == text.Length;
        }

        return false;
    }

    /// <summary>
    /// PostgreSQL's interval output (<c>1 year 2 mons 3 days 04:05:06.5</c>, a sign on any part) and the
    /// form this driver writes (<c>14 mons -3 days 5 microseconds</c>).
    /// </summary>
    private static bool TryParseInterval(ReadOnlySpan<byte> text, out RasterizedDbInterval value)
    {
        value = default;
        long months = 0, days = 0, micros = 0;
        var at = 0;
        var any = false;
        while (at < text.Length)
        {
            while (at < text.Length && text[at] == ' ')
            {
                at++;
            }

            if (at == text.Length)
            {
                break;
            }

            var negative = false;
            if (text[at] is (byte)'-' or (byte)'+')
            {
                negative = text[at] == '-';
                at++;
            }

            // A clock: digits, then ':'.
            var probe = at;
            while (probe < text.Length && (uint)(text[probe] - '0') <= 9)
            {
                probe++;
            }

            if (probe == at)
            {
                return false;
            }

            if (probe < text.Length && text[probe] == ':')
            {
                if (!TryParseClock(text, ref at, out var ticks))
                {
                    return false;
                }

                micros += negative ? -(ticks / 10) : ticks / 10;
                any = true;
                continue;
            }

            if (!Utf8Parser.TryParse(text[at..], out decimal amount, out var consumed))
            {
                return false;
            }

            at += consumed;
            if (negative)
            {
                amount = -amount;
            }

            while (at < text.Length && text[at] == ' ')
            {
                at++;
            }

            var unitStart = at;
            while (at < text.Length && (uint)((text[at] | 0x20) - 'a') <= 'z' - 'a')
            {
                at++;
            }

            var unit = text[unitStart..at];
            if (unit.Length > 1 && (unit[^1] | 0x20) == 's' && !Ascii.EqualsIgnoreCase(unit, "us"u8) && !Ascii.EqualsIgnoreCase(unit, "ms"u8))
            {
                unit = unit[..^1];
            }

            if (Ascii.EqualsIgnoreCase(unit, "year"u8) || Ascii.EqualsIgnoreCase(unit, "yr"u8) || Ascii.EqualsIgnoreCase(unit, "y"u8))
            {
                months += (long)(amount * 12);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "mon"u8) || Ascii.EqualsIgnoreCase(unit, "month"u8))
            {
                months += (long)amount;
            }
            else if (Ascii.EqualsIgnoreCase(unit, "week"u8) || Ascii.EqualsIgnoreCase(unit, "w"u8))
            {
                days += (long)(amount * 7);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "day"u8) || Ascii.EqualsIgnoreCase(unit, "d"u8))
            {
                days += (long)amount;
            }
            else if (Ascii.EqualsIgnoreCase(unit, "hour"u8) || Ascii.EqualsIgnoreCase(unit, "hr"u8) || Ascii.EqualsIgnoreCase(unit, "h"u8))
            {
                micros += (long)(amount * 3_600_000_000m);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "minute"u8) || Ascii.EqualsIgnoreCase(unit, "min"u8) || Ascii.EqualsIgnoreCase(unit, "m"u8))
            {
                micros += (long)(amount * 60_000_000m);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "second"u8) || Ascii.EqualsIgnoreCase(unit, "sec"u8) || Ascii.EqualsIgnoreCase(unit, "s"u8))
            {
                micros += (long)(amount * 1_000_000m);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "millisecond"u8) || Ascii.EqualsIgnoreCase(unit, "ms"u8))
            {
                micros += (long)(amount * 1_000m);
            }
            else if (Ascii.EqualsIgnoreCase(unit, "microsecond"u8) || Ascii.EqualsIgnoreCase(unit, "us"u8))
            {
                micros += (long)amount;
            }
            else
            {
                return false;
            }

            any = true;
        }

        if (!any || months is < int.MinValue or > int.MaxValue || days is < int.MinValue or > int.MaxValue)
        {
            return false;
        }

        value = new RasterizedDbInterval((int)months, (int)days, micros);
        return true;
    }

    // -------------------------------------------------------------------- arrays

    private delegate void ElementSink<TState>(ref TState state, bool isNull, ReadOnlySpan<byte> text);

    /// <summary>
    /// An array's elements, each read as <typeparamref name="T"/> from its text: from an array literal
    /// (<c>{1,2,NULL}</c>, a column selected as <c>col::text</c> or a simple-query cell) or from the
    /// engine's stored form (PostgreSQL's binary array framing around text elements).
    /// </summary>
    public static T[] ReadArray<T>(byte tag, ReadOnlySpan<byte> raw)
    {
        var list = new List<T>();
        if (Tag.IsText(tag) && raw.Length > 0 && (raw[0] == '{' || raw[0] == '['))
        {
            if (!ParseArrayText(raw, ref list, static (ref List<T> state, bool isNull, ReadOnlySpan<byte> text) => state.Add(Element<T>(isNull, text, state.Count))))
            {
                throw BadText(raw, "an array literal");
            }

            return list.ToArray();
        }

        if (tag is Tag.Array or Tag.String)
        {
            switch (ParseBinaryArray(raw, ref list, static (ref List<T> state, bool isNull, ReadOnlySpan<byte> text) => state.Add(Element<T>(isNull, text, state.Count))))
            {
                case 1:
                    return list.ToArray();
                case 2:
                    throw new InvalidCastException("a multi-dimensional array: select it as col::text and read it as an array of arrays");
            }
        }

        throw Bad(tag, "an array");
    }

    private static T Element<T>(bool isNull, ReadOnlySpan<byte> text, int index)
    {
        if (isNull)
        {
            return default(T) is null ? default! : throw new InvalidCastException($"element {index + 1} is NULL; read the array with a nullable element type");
        }

        return Read<T>(Tag.PgText, text);
    }

    private static bool ParseArrayText<TState>(ReadOnlySpan<byte> text, ref TState state, ElementSink<TState> sink)
    {
        var body = Trim(text);
        // `[1:3]={…}`: explicit bounds; the elements are what matters here.
        if (body.Length > 0 && body[0] == '[')
        {
            var eq = body.IndexOf("={"u8);
            if (eq < 0)
            {
                return false;
            }

            body = body[(eq + 1)..];
        }

        if (body.Length < 2 || body[0] != '{' || body[^1] != '}')
        {
            return false;
        }

        var inner = body[1..^1];
        if (Trim(inner).IsEmpty)
        {
            return true;
        }

        using var scratch = new ByteWriter(64);
        var i = 0;
        while (true)
        {
            while (i < inner.Length && inner[i] is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\r')
            {
                i++;
            }

            if (i >= inner.Length)
            {
                return false;
            }

            if (inner[i] == '"')
            {
                scratch.Clear();
                i++;
                while (true)
                {
                    if (i >= inner.Length)
                    {
                        return false;
                    }

                    var b = inner[i];
                    if (b == '\\')
                    {
                        if (i + 1 >= inner.Length)
                        {
                            return false;
                        }

                        scratch.WriteByte(inner[i + 1]);
                        i += 2;
                    }
                    else if (b == '"')
                    {
                        i++;
                        break;
                    }
                    else
                    {
                        scratch.WriteByte(b);
                        i++;
                    }
                }

                sink(ref state, false, scratch.WrittenSpan);
            }
            else if (inner[i] == '{')
            {
                var start = i;
                var depth = 0;
                var quoted = false;
                while (i < inner.Length)
                {
                    var b = inner[i];
                    if (b == '\\' && quoted)
                    {
                        i++;
                    }
                    else if (b == '"')
                    {
                        quoted = !quoted;
                    }
                    else if (b == '{' && !quoted)
                    {
                        depth++;
                    }
                    else if (b == '}' && !quoted)
                    {
                        depth--;
                        if (depth == 0)
                        {
                            i++;
                            break;
                        }
                    }

                    i++;
                }

                if (depth != 0)
                {
                    return false;
                }

                sink(ref state, false, inner[start..i]);
            }
            else
            {
                scratch.Clear();
                while (i < inner.Length && inner[i] != ',')
                {
                    if (inner[i] == '\\')
                    {
                        if (i + 1 >= inner.Length)
                        {
                            return false;
                        }

                        scratch.WriteByte(inner[i + 1]);
                        i += 2;
                        continue;
                    }

                    scratch.WriteByte(inner[i]);
                    i++;
                }

                var value = scratch.WrittenSpan.TrimEnd(" \t\r\n"u8);
                var isNull = Ascii.EqualsIgnoreCase(value, "NULL"u8);
                sink(ref state, isNull, isNull ? default : value);
            }

            while (i < inner.Length && inner[i] is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\r')
            {
                i++;
            }

            if (i >= inner.Length)
            {
                return true;
            }

            if (inner[i] != ',')
            {
                return false;
            }

            i++;
        }
    }

    /// <returns>0: not a stored array; 1: read; 2: more than one dimension.</returns>
    private static int ParseBinaryArray<TState>(ReadOnlySpan<byte> raw, ref TState state, ElementSink<TState> sink)
    {
        if (raw.Length < 12)
        {
            return 0;
        }

        var dimensions = BinaryPrimitives.ReadInt32BigEndian(raw);
        var flags = BinaryPrimitives.ReadInt32BigEndian(raw[4..]);
        if (dimensions is < 0 or > 6 || flags is < 0 or > 1)
        {
            return 0;
        }

        if (dimensions == 0)
        {
            return raw.Length == 12 ? 1 : 0;
        }

        if (dimensions > 1)
        {
            return 2;
        }

        if (raw.Length < 20)
        {
            return 0;
        }

        var count = BinaryPrimitives.ReadInt32BigEndian(raw[12..]);
        if (count < 0)
        {
            return 0;
        }

        // Validated whole before any element is handed out, so a text cell that only looks like an
        // array yields nothing.
        var at = 20;
        for (var i = 0; i < count; i++)
        {
            if (at + 4 > raw.Length)
            {
                return 0;
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(raw[at..]);
            at += 4;
            if (length < 0)
            {
                continue;
            }

            if (length > raw.Length - at)
            {
                return 0;
            }

            at += length;
        }

        if (at != raw.Length)
        {
            return 0;
        }

        at = 20;
        for (var i = 0; i < count; i++)
        {
            var length = BinaryPrimitives.ReadInt32BigEndian(raw[at..]);
            at += 4;
            if (length < 0)
            {
                sink(ref state, true, default);
                continue;
            }

            sink(ref state, false, raw.Slice(at, length));
            at += length;
        }

        return 1;
    }

    // ------------------------------------------------------------------- generic

    /// <summary>A cell as <typeparamref name="T"/>. The type tests are JIT-time constants for value types.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T Read<T>(byte tag, ReadOnlySpan<byte> raw)
    {
        if (typeof(T) == typeof(int)) { var v = ReadInt32(tag, raw); return Unsafe.As<int, T>(ref v); }
        if (typeof(T) == typeof(long)) { var v = ReadInt64(tag, raw); return Unsafe.As<long, T>(ref v); }
        if (typeof(T) == typeof(bool)) { var v = ReadBoolean(tag, raw); return Unsafe.As<bool, T>(ref v); }
        if (typeof(T) == typeof(string)) { return (T)(object)ReadString(tag, raw); }
        if (typeof(T) == typeof(Guid)) { var v = ReadGuid(tag, raw); return Unsafe.As<Guid, T>(ref v); }
        if (typeof(T) == typeof(DateTime)) { var v = ReadDateTime(tag, raw); return Unsafe.As<DateTime, T>(ref v); }
        if (typeof(T) == typeof(decimal)) { var v = ReadDecimal(tag, raw); return Unsafe.As<decimal, T>(ref v); }
        if (typeof(T) == typeof(double)) { var v = ReadDouble(tag, raw); return Unsafe.As<double, T>(ref v); }
        if (typeof(T) == typeof(float)) { var v = ReadSingle(tag, raw); return Unsafe.As<float, T>(ref v); }
        if (typeof(T) == typeof(short)) { var v = checked((short)ReadInt64(tag, raw)); return Unsafe.As<short, T>(ref v); }
        if (typeof(T) == typeof(byte)) { var v = checked((byte)ReadInt64(tag, raw)); return Unsafe.As<byte, T>(ref v); }
        if (typeof(T) == typeof(DateTimeOffset)) { var v = ReadDateTimeOffset(tag, raw); return Unsafe.As<DateTimeOffset, T>(ref v); }
        if (typeof(T) == typeof(DateOnly)) { var v = ReadDate(tag, raw); return Unsafe.As<DateOnly, T>(ref v); }
        if (typeof(T) == typeof(TimeOnly)) { var v = ReadTime(tag, raw); return Unsafe.As<TimeOnly, T>(ref v); }
        if (typeof(T) == typeof(TimeSpan)) { var v = ReadInterval(tag, raw).ToTimeSpan(); return Unsafe.As<TimeSpan, T>(ref v); }
        if (typeof(T) == typeof(byte[])) { return (T)(object)ReadBytes(tag, raw); }
        return ReadOther<T>(tag, raw);
    }

    private static T ReadOther<T>(byte tag, ReadOnlySpan<byte> raw)
    {
        if (typeof(T) == typeof(object)) { return (T)ReadObject(tag, raw, 0); }
        if (typeof(T) == typeof(sbyte)) { var v = checked((sbyte)ReadInt64(tag, raw)); return Unsafe.As<sbyte, T>(ref v); }
        if (typeof(T) == typeof(ushort)) { var v = checked((ushort)ReadInt64(tag, raw)); return Unsafe.As<ushort, T>(ref v); }
        if (typeof(T) == typeof(uint)) { var v = checked((uint)ReadInt64(tag, raw)); return Unsafe.As<uint, T>(ref v); }
        if (typeof(T) == typeof(ulong)) { var v = checked((ulong)ReadInt128(tag, raw)); return Unsafe.As<ulong, T>(ref v); }
        if (typeof(T) == typeof(Int128)) { var v = ReadInt128(tag, raw); return Unsafe.As<Int128, T>(ref v); }
        if (typeof(T) == typeof(BigInteger)) { var v = BigInteger.Parse(ReadInt128Text(tag, raw), CultureInfo.InvariantCulture); return Unsafe.As<BigInteger, T>(ref v); }
        if (typeof(T) == typeof(char)) { var s = ReadString(tag, raw); var v = s.Length == 1 ? s[0] : throw new InvalidCastException($"\"{s}\" is not a single character"); return Unsafe.As<char, T>(ref v); }
        if (typeof(T) == typeof(RasterizedDbInterval)) { var v = ReadInterval(tag, raw); return Unsafe.As<RasterizedDbInterval, T>(ref v); }
        if (typeof(T) == typeof(ReadOnlyMemory<byte>)) { ReadOnlyMemory<byte> v = ReadBytes(tag, raw); return Unsafe.As<ReadOnlyMemory<byte>, T>(ref v); }
        if (typeof(T) == typeof(System.Text.Json.JsonDocument)) { return (T)(object)System.Text.Json.JsonDocument.Parse(JsonBytes(tag, raw).ToArray()); }
        if (typeof(T) == typeof(System.Text.Json.JsonElement)) { using var doc = System.Text.Json.JsonDocument.Parse(JsonBytes(tag, raw).ToArray()); var v = doc.RootElement.Clone(); return Unsafe.As<System.Text.Json.JsonElement, T>(ref v); }
        var reader = Composite<T>.Reader ?? throw new InvalidCastException($"this driver has no reader for {typeof(T)}");
        return reader(tag, raw);
    }

    private static ReadOnlySpan<byte> JsonBytes(byte tag, ReadOnlySpan<byte> raw) =>
        tag == Tag.Jsonb ? throw JsonbDocument() : Tag.IsText(tag) ? raw : throw Bad(tag, "JSON");

    private static string ReadInt128Text(byte tag, ReadOnlySpan<byte> raw) =>
        Tag.IsText(tag) ? Encoding.ASCII.GetString(Trim(raw)) : ReadInt128(tag, raw).ToString(CultureInfo.InvariantCulture);

    /// <summary>Readers for the types that take reflection to build: nullables, enums, arrays, lists.</summary>
    private static class Composite<T>
    {
        public static readonly CellReader<T>? Reader = Build();

        private static CellReader<T>? Build()
        {
            var type = typeof(T);
            MethodInfo? method = null;
            if (Nullable.GetUnderlyingType(type) is { } underlying)
            {
                method = typeof(ValueReader).GetMethod(nameof(ReadNullable), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(underlying);
            }
            else if (type.IsEnum)
            {
                method = typeof(ValueReader).GetMethod(nameof(ReadEnum), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type);
            }
            else if (type.IsArray && type.GetArrayRank() == 1)
            {
                method = typeof(ValueReader).GetMethod(nameof(ReadArray), BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(type.GetElementType()!);
            }
            else if (type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(List<>) || type.GetGenericTypeDefinition() == typeof(IList<>)
                         || type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>) || type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                         || type.GetGenericTypeDefinition() == typeof(ICollection<>) || type.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>)))
            {
                method = typeof(ValueReader).GetMethod(nameof(ReadList), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type.GetGenericArguments()[0]);
                var list = method.CreateDelegate(typeof(CellReader<>).MakeGenericType(typeof(List<>).MakeGenericType(type.GetGenericArguments()[0])));
                return (CellReader<T>)typeof(ValueReader).GetMethod(nameof(Upcast), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(list.GetType().GetGenericArguments()[0], type).Invoke(null, [list])!;
            }

            return method is null ? null : (CellReader<T>)method.CreateDelegate(typeof(CellReader<T>));
        }
    }

    private static CellReader<TTo> Upcast<TFrom, TTo>(CellReader<TFrom> reader) where TFrom : TTo => (tag, raw) => reader(tag, raw);

    private static T? ReadNullable<T>(byte tag, ReadOnlySpan<byte> raw) where T : struct => Read<T>(tag, raw);

    private static List<T> ReadList<T>(byte tag, ReadOnlySpan<byte> raw) => [.. ReadArray<T>(tag, raw)];

    private static T ReadEnum<T>(byte tag, ReadOnlySpan<byte> raw) where T : struct, Enum
    {
        if (Tag.IsText(tag))
        {
            var text = Trim(raw);
            if (!Utf8Parser.TryParse(text, out long _, out var consumed) || consumed != text.Length)
            {
                return Enum.Parse<T>(Encoding.UTF8.GetString(text), ignoreCase: true);
            }
        }

        var number = ReadInt64(tag, raw);
        return (T)Enum.ToObject(typeof(T), number);
    }

    // --------------------------------------------------------------------- boxed

    private delegate object? BoxedReader(byte tag, ReadOnlySpan<byte> raw);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, BoxedReader> BoxedReaders = new();

    /// <summary>A cell as a type known only at run time (a property found by reflection).</summary>
    public static object? ReadBoxed(Type type, byte tag, ReadOnlySpan<byte> raw) =>
        BoxedReaders.GetOrAdd(
            type,
            static t => (BoxedReader)typeof(ValueReader).GetMethod(nameof(Boxed), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(t).CreateDelegate(typeof(BoxedReader)))(tag, raw);

    private static object? Boxed<T>(byte tag, ReadOnlySpan<byte> raw) => Read<T>(tag, raw);

    // -------------------------------------------------------------------- object

    /// <summary>
    /// A cell as the CLR type its column type maps to: by the native tag, or for a simple-query cell by
    /// the column's PostgreSQL type OID.
    /// </summary>
    public static object ReadObject(byte tag, ReadOnlySpan<byte> raw, uint oid)
    {
        if (tag == Tag.PgText)
        {
            return oid switch
            {
                PgOid.Bool => ReadBoolean(tag, raw),
                PgOid.Int2 => checked((short)ReadInt64(tag, raw)),
                PgOid.Int4 => ReadInt32(tag, raw),
                PgOid.Int8 => ReadInt64(tag, raw),
                PgOid.Oid => checked((uint)ReadInt64(tag, raw)),
                PgOid.Float4 => ReadSingle(tag, raw),
                PgOid.Float8 => ReadDouble(tag, raw),
                PgOid.Numeric => NumericObject(tag, raw),
                PgOid.Bytea => ReadBytes(tag, raw),
                PgOid.Uuid => ReadGuid(tag, raw),
                PgOid.Date => ReadDate(tag, raw),
                PgOid.Time => ReadTime(tag, raw),
                PgOid.Timestamp or PgOid.TimestampTz => ReadDateTime(tag, raw),
                PgOid.Interval => IntervalObject(tag, raw),
                PgOid.BoolArray => ReadArray<bool?>(tag, raw),
                PgOid.Int2Array => ReadArray<short?>(tag, raw),
                PgOid.Int4Array => ReadArray<int?>(tag, raw),
                PgOid.Int8Array => ReadArray<long?>(tag, raw),
                PgOid.Float4Array => ReadArray<float?>(tag, raw),
                PgOid.Float8Array => ReadArray<double?>(tag, raw),
                PgOid.NumericArray => ReadArray<decimal?>(tag, raw),
                PgOid.UuidArray => ReadArray<Guid?>(tag, raw),
                PgOid.TextArray or PgOid.VarcharArray or PgOid.BpcharArray => ReadArray<string?>(tag, raw),
                _ => ReadString(tag, raw),
            };
        }

        switch (tag)
        {
            case Tag.String or Tag.Char or Tag.Json or Tag.JsonText: return ReadString(tag, raw);
            case Tag.I32 or Tag.U16: return ReadInt32(tag, raw);
            case Tag.I64 or Tag.U32: return ReadInt64(tag, raw);
            case Tag.Bool: return ReadBoolean(tag, raw);
            case Tag.I8 or Tag.I16 or Tag.U8: return checked((short)ReadInt64(tag, raw));
            case Tag.U64: return Le<ulong>(raw);
            case Tag.I128 or Tag.U128: return ReadInt128(tag, raw);
            case Tag.F32: return ReadSingle(tag, raw);
            case Tag.F64: return ReadDouble(tag, raw);
            case Tag.Decimal: return NumericObject(tag, raw);
            case Tag.Uuid: return ReadGuid(tag, raw);
            case Tag.DateTime or Tag.Timestamp: return ReadDateTime(tag, raw);
            case Tag.Date: return ReadDate(tag, raw);
            case Tag.Time: return ReadTime(tag, raw);
            case Tag.Interval: return IntervalObject(tag, raw);
            case Tag.Bytea: return raw.ToArray();
            case Tag.Array: return ReadArray<string?>(tag, raw);
            case Tag.Inet: return InetText(raw);
            case Tag.Jsonb: throw JsonbDocument();
            default: throw new InvalidCastException($"type tag {tag} has no decoder in this driver");
        }
    }

    /// <summary>A decimal where it fits one; otherwise the exact digits as text.</summary>
    private static object NumericObject(byte tag, ReadOnlySpan<byte> raw)
    {
        try
        {
            return ReadDecimal(tag, raw);
        }
        catch (Exception e) when (e is OverflowException || (e is InvalidCastException && Tag.IsText(tag)))
        {
            return ReadString(tag, raw);
        }
    }

    private static object IntervalObject(byte tag, ReadOnlySpan<byte> raw)
    {
        var interval = ReadInterval(tag, raw);
        return interval.Months == 0 ? interval.ToTimeSpan() : interval;
    }

    /// <summary>The CLR type <see cref="ReadObject"/> returns for a column.</summary>
    public static Type FieldType(byte tag, uint oid)
    {
        if (tag == Tag.PgText)
        {
            return oid switch
            {
                PgOid.Bool => typeof(bool),
                PgOid.Int2 => typeof(short),
                PgOid.Int4 => typeof(int),
                PgOid.Int8 => typeof(long),
                PgOid.Oid => typeof(uint),
                PgOid.Float4 => typeof(float),
                PgOid.Float8 => typeof(double),
                PgOid.Numeric => typeof(decimal),
                PgOid.Bytea => typeof(byte[]),
                PgOid.Uuid => typeof(Guid),
                PgOid.Date => typeof(DateOnly),
                PgOid.Time => typeof(TimeOnly),
                PgOid.Timestamp or PgOid.TimestampTz => typeof(DateTime),
                PgOid.Interval => typeof(TimeSpan),
                PgOid.BoolArray => typeof(bool?[]),
                PgOid.Int2Array => typeof(short?[]),
                PgOid.Int4Array => typeof(int?[]),
                PgOid.Int8Array => typeof(long?[]),
                PgOid.Float4Array => typeof(float?[]),
                PgOid.Float8Array => typeof(double?[]),
                PgOid.NumericArray => typeof(decimal?[]),
                PgOid.UuidArray => typeof(Guid?[]),
                PgOid.TextArray or PgOid.VarcharArray or PgOid.BpcharArray => typeof(string[]),
                _ => typeof(string),
            };
        }

        return tag switch
        {
            Tag.I32 or Tag.U16 => typeof(int),
            Tag.I64 or Tag.U32 => typeof(long),
            Tag.Bool => typeof(bool),
            Tag.I8 or Tag.I16 or Tag.U8 => typeof(short),
            Tag.U64 => typeof(ulong),
            Tag.I128 or Tag.U128 => typeof(Int128),
            Tag.F32 => typeof(float),
            Tag.F64 => typeof(double),
            Tag.Decimal => typeof(decimal),
            Tag.Uuid => typeof(Guid),
            Tag.DateTime or Tag.Timestamp => typeof(DateTime),
            Tag.Date => typeof(DateOnly),
            Tag.Time => typeof(TimeOnly),
            Tag.Interval => typeof(TimeSpan),
            Tag.Bytea => typeof(byte[]),
            Tag.Array => typeof(string[]),
            _ => typeof(string),
        };
    }

    public static string TypeName(byte tag, uint oid)
    {
        if (tag == Tag.PgText)
        {
            return PgOid.Name(oid);
        }

        return tag switch
        {
            Tag.I8 or Tag.I16 or Tag.U8 => "smallint",
            Tag.I32 or Tag.U16 => "integer",
            Tag.I64 or Tag.U32 => "bigint",
            Tag.U64 or Tag.I128 or Tag.U128 or Tag.Decimal => "numeric",
            Tag.F32 => "real",
            Tag.F64 => "double precision",
            Tag.Bool => "boolean",
            Tag.Uuid => "uuid",
            Tag.DateTime => "timestamp with time zone",
            Tag.Timestamp => "timestamp without time zone",
            Tag.Date => "date",
            Tag.Time => "time without time zone",
            Tag.Interval => "interval",
            Tag.Bytea => "bytea",
            Tag.Array => "text[]",
            Tag.Json or Tag.Jsonb => "jsonb",
            Tag.JsonText => "json",
            Tag.Inet => "inet",
            Tag.Char => "character",
            _ => "text",
        };
    }
}

/// <summary>The PostgreSQL type OIDs a simple-query answer names its columns' types by.</summary>
internal static class PgOid
{
    public const uint Bool = 16;
    public const uint Bytea = 17;
    public const uint Name_ = 19;
    public const uint Int8 = 20;
    public const uint Int2 = 21;
    public const uint Int4 = 23;
    public const uint Text = 25;
    public const uint Oid = 26;
    public const uint Json = 114;
    public const uint Float4 = 700;
    public const uint Float8 = 701;
    public const uint BoolArray = 1000;
    public const uint Int2Array = 1005;
    public const uint Int4Array = 1007;
    public const uint TextArray = 1009;
    public const uint BpcharArray = 1014;
    public const uint VarcharArray = 1015;
    public const uint Int8Array = 1016;
    public const uint Float4Array = 1021;
    public const uint Float8Array = 1022;
    public const uint Bpchar = 1042;
    public const uint Varchar = 1043;
    public const uint Date = 1082;
    public const uint Time = 1083;
    public const uint Timestamp = 1114;
    public const uint TimestampTz = 1184;
    public const uint Interval = 1186;
    public const uint NumericArray = 1231;
    public const uint Numeric = 1700;
    public const uint Uuid = 2950;
    public const uint UuidArray = 2951;
    public const uint Jsonb = 3802;

    public static string Name(uint oid) => oid switch
    {
        Bool => "boolean",
        Bytea => "bytea",
        Name_ => "name",
        Int8 => "bigint",
        Int2 => "smallint",
        Int4 => "integer",
        Text => "text",
        Oid => "oid",
        Json => "json",
        Float4 => "real",
        Float8 => "double precision",
        Bpchar => "character",
        Varchar => "character varying",
        Date => "date",
        Time => "time without time zone",
        Timestamp => "timestamp without time zone",
        TimestampTz => "timestamp with time zone",
        Interval => "interval",
        Numeric => "numeric",
        Uuid => "uuid",
        Jsonb => "jsonb",
        BoolArray => "boolean[]",
        Int2Array => "smallint[]",
        Int4Array => "integer[]",
        Int8Array => "bigint[]",
        Float4Array => "real[]",
        Float8Array => "double precision[]",
        NumericArray => "numeric[]",
        UuidArray => "uuid[]",
        TextArray => "text[]",
        VarcharArray => "character varying[]",
        BpcharArray => "character[]",
        _ => "text",
    };
}
