using System.Buffers.Text;
using System.Collections;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace RasterizedDb.Data.Internal;

/// <summary>
/// Writes a parameter's value as the SQL literal the server reads in its place, straight into the
/// request's bytes.
/// </summary>
/// <remarks>
/// The native protocol carries a statement's text and nothing else, so a value is a literal. The rules
/// are the client crate's (<c>params.rs</c>, <c>push_param</c>), which are the ones the server's lexer
/// reads by: text is single-quoted with <c>'</c> doubled, bytes are a <c>'\x…'</c> hex literal, a
/// negative number is parenthesised so it cannot follow a <c>-</c> and start a comment, and a NUL is
/// refused. A value can never become syntax.
/// </remarks>
internal static class LiteralWriter
{
    private const long TicksPerMicrosecond = 10;

    /// <summary>Writes <paramref name="value"/> typed, with no boxing for the common value types.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write<T>(ByteWriter w, T value)
    {
        // Each comparison is a JIT-time constant for a value type T: one branch survives.
        if (typeof(T) == typeof(int)) { WriteInt(w, Unsafe.As<T, int>(ref value)); return; }
        if (typeof(T) == typeof(long)) { WriteInt(w, Unsafe.As<T, long>(ref value)); return; }
        if (typeof(T) == typeof(short)) { WriteInt(w, Unsafe.As<T, short>(ref value)); return; }
        if (typeof(T) == typeof(bool)) { WriteBool(w, Unsafe.As<T, bool>(ref value)); return; }
        if (typeof(T) == typeof(double)) { WriteDouble(w, Unsafe.As<T, double>(ref value)); return; }
        if (typeof(T) == typeof(float)) { WriteSingle(w, Unsafe.As<T, float>(ref value)); return; }
        if (typeof(T) == typeof(decimal)) { WriteDecimal(w, Unsafe.As<T, decimal>(ref value)); return; }
        if (typeof(T) == typeof(Guid)) { WriteGuid(w, Unsafe.As<T, Guid>(ref value)); return; }
        if (typeof(T) == typeof(DateTime)) { WriteDateTime(w, Unsafe.As<T, DateTime>(ref value)); return; }
        if (typeof(T) == typeof(DateTimeOffset)) { WriteDateTimeOffset(w, Unsafe.As<T, DateTimeOffset>(ref value)); return; }
        if (typeof(T) == typeof(DateOnly)) { WriteDate(w, Unsafe.As<T, DateOnly>(ref value)); return; }
        if (typeof(T) == typeof(TimeOnly)) { WriteTime(w, Unsafe.As<T, TimeOnly>(ref value)); return; }
        if (typeof(T) == typeof(TimeSpan)) { WriteInterval(w, RasterizedDbInterval.FromTimeSpan(Unsafe.As<T, TimeSpan>(ref value))); return; }
        if (typeof(T) == typeof(byte)) { w.WriteInt64Text(Unsafe.As<T, byte>(ref value)); return; }
        if (typeof(T) == typeof(sbyte)) { WriteInt(w, Unsafe.As<T, sbyte>(ref value)); return; }
        if (typeof(T) == typeof(ushort)) { w.WriteInt64Text(Unsafe.As<T, ushort>(ref value)); return; }
        if (typeof(T) == typeof(uint)) { w.WriteInt64Text(Unsafe.As<T, uint>(ref value)); return; }
        if (typeof(T) == typeof(ulong)) { w.WriteUInt64Text(Unsafe.As<T, ulong>(ref value)); return; }
        WriteObject(w, value);
    }

    /// <summary>Writes a boxed value, by its runtime type.</summary>
    public static void WriteObject(ByteWriter w, object? value)
    {
        switch (value)
        {
            case null or DBNull:
                w.Write("NULL"u8);
                break;
            case string s:
                WriteText(w, s);
                break;
            case int v:
                WriteInt(w, v);
                break;
            case long v:
                WriteInt(w, v);
                break;
            case bool v:
                WriteBool(w, v);
                break;
            case Guid v:
                WriteGuid(w, v);
                break;
            case DateTime v:
                WriteDateTime(w, v);
                break;
            case decimal v:
                WriteDecimal(w, v);
                break;
            case double v:
                WriteDouble(w, v);
                break;
            case short v:
                WriteInt(w, v);
                break;
            case float v:
                WriteSingle(w, v);
                break;
            case byte[] v:
                WriteBytes(w, v);
                break;
            case DateTimeOffset v:
                WriteDateTimeOffset(w, v);
                break;
            case DateOnly v:
                WriteDate(w, v);
                break;
            case TimeOnly v:
                WriteTime(w, v);
                break;
            case TimeSpan v:
                WriteInterval(w, RasterizedDbInterval.FromTimeSpan(v));
                break;
            case RasterizedDbInterval v:
                WriteInterval(w, v);
                break;
            case byte v:
                w.WriteInt64Text(v);
                break;
            case sbyte v:
                WriteInt(w, v);
                break;
            case ushort v:
                w.WriteInt64Text(v);
                break;
            case uint v:
                w.WriteInt64Text(v);
                break;
            case ulong v:
                w.WriteUInt64Text(v);
                break;
            case Int128 v:
                WriteParenthesisedIfNegative(w, v, v < 0);
                break;
            case UInt128 v:
                w.WriteFormatted(v);
                break;
            case BigInteger v:
                WriteBigInteger(w, v);
                break;
            case char v:
                WriteText(w, new ReadOnlySpan<char>(in v));
                break;
            case ReadOnlyMemory<byte> v:
                WriteBytes(w, v.Span);
                break;
            case Memory<byte> v:
                WriteBytes(w, v.Span);
                break;
            case ArraySegment<byte> v:
                WriteBytes(w, v);
                break;
            case ReadOnlyMemory<char> v:
                WriteText(w, v.Span);
                break;
            case char[] v:
                WriteText(w, v);
                break;
            case JsonDocument v:
                WriteText(w, v.RootElement.GetRawText());
                break;
            case JsonElement v:
                WriteText(w, v.GetRawText());
                break;
            case JsonNode v:
                WriteText(w, v.ToJsonString());
                break;
            case System.Net.IPAddress v:
                WriteText(w, v.ToString());
                break;
            case Enum v:
                WriteObject(w, Convert.ChangeType(v, Enum.GetUnderlyingType(v.GetType()), CultureInfo.InvariantCulture));
                break;
            case IEnumerable v:
                WriteArray(w, v);
                break;
            default:
                throw new NotSupportedException(
                    $"a parameter of type {value.GetType()} has no SQL literal in this driver; convert it to a supported type (text, a number, bool, Guid, a date/time type, byte[], or an array of those)");
        }
    }

    public static void WriteBool(ByteWriter w, bool value) => w.Write(value ? "true"u8 : "false"u8);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt(ByteWriter w, long value)
    {
        if (value < 0)
        {
            w.WriteByte((byte)'(');
            w.WriteInt64Text(value);
            w.WriteByte((byte)')');
        }
        else
        {
            w.WriteInt64Text(value);
        }
    }

    private static void WriteParenthesisedIfNegative<T>(ByteWriter w, T value, bool negative) where T : IUtf8SpanFormattable
    {
        if (negative)
        {
            w.WriteByte((byte)'(');
            w.WriteFormatted(value);
            w.WriteByte((byte)')');
        }
        else
        {
            w.WriteFormatted(value);
        }
    }

    private static void WriteBigInteger(ByteWriter w, BigInteger value)
    {
        var text = value.ToString(CultureInfo.InvariantCulture);
        if (value.Sign < 0)
        {
            w.WriteByte((byte)'(');
            w.WriteAscii(text);
            w.WriteByte((byte)')');
        }
        else
        {
            w.WriteAscii(text);
        }
    }

    public static void WriteDecimal(ByteWriter w, decimal value) => WriteParenthesisedIfNegative(w, value, value < 0);

    public static void WriteDouble(ByteWriter w, double value)
    {
        if (double.IsNaN(value))
        {
            w.Write("'NaN'"u8);
        }
        else if (double.IsInfinity(value))
        {
            w.Write(value < 0 ? "'-Infinity'"u8 : "'Infinity'"u8);
        }
        else
        {
            var negative = double.IsNegative(value);
            if (negative)
            {
                w.WriteByte((byte)'(');
            }

            WriteFloatText(w, value);
            if (negative)
            {
                w.WriteByte((byte)')');
            }
        }
    }

    /// <summary>
    /// Typed: a bare <c>0.1</c> is a numeric literal, which a <c>real</c> column's 0.1f (widened,
    /// 0.10000000149…) never equals, as in PostgreSQL.
    /// </summary>
    public static void WriteSingle(ByteWriter w, float value)
    {
        if (float.IsNaN(value))
        {
            w.Write("'NaN'"u8);
        }
        else if (float.IsInfinity(value))
        {
            w.Write(value < 0 ? "'-Infinity'"u8 : "'Infinity'"u8);
        }
        else
        {
            var negative = float.IsNegative(value);
            if (negative)
            {
                w.WriteByte((byte)'(');
            }

            Span<byte> text = stackalloc byte[32];
            value.TryFormat(text, out var written, default, CultureInfo.InvariantCulture);
            WriteNormalisedFloat(w, text[..written]);
            if (negative)
            {
                w.WriteByte((byte)')');
            }
        }

        w.Write("::real"u8);
    }

    private static void WriteFloatText(ByteWriter w, double value)
    {
        Span<byte> text = stackalloc byte[32];
        value.TryFormat(text, out var written, default, CultureInfo.InvariantCulture);
        WriteNormalisedFloat(w, text[..written]);
    }

    /// <summary>
    /// The shortest round-trip digits .NET writes, in the shape the client crate sends (Rust's
    /// <c>{:?}</c>): <c>1E+21</c> becomes <c>1e21</c>, <c>1E-07</c> becomes <c>1e-7</c>, and a whole
    /// number keeps a <c>.0</c> so it stays a float literal.
    /// </summary>
    private static void WriteNormalisedFloat(ByteWriter w, ReadOnlySpan<byte> text)
    {
        var e = text.IndexOf((byte)'E');
        if (e < 0)
        {
            w.Write(text);
            if (text.IndexOf((byte)'.') < 0)
            {
                w.Write(".0"u8);
            }

            return;
        }

        w.Write(text[..e]);
        w.WriteByte((byte)'e');
        var exponent = text[(e + 1)..];
        if (exponent.Length > 0 && exponent[0] == '-')
        {
            w.WriteByte((byte)'-');
        }

        exponent = exponent.TrimStart("+-"u8).TrimStart((byte)'0');
        if (exponent.IsEmpty)
        {
            w.WriteByte((byte)'0');
        }
        else
        {
            w.Write(exponent);
        }
    }

    public static void WriteGuid(ByteWriter w, Guid value)
    {
        var span = w.GetSpan(38);
        span[0] = (byte)'\'';
        Utf8Formatter.TryFormat(value, span[1..], out var written);
        span[1 + written] = (byte)'\'';
        w.Advance(written + 2);
    }

    public static void WriteBytes(ByteWriter w, ReadOnlySpan<byte> value)
    {
        w.Write("'\\x"u8);
        w.WriteHex(value);
        w.WriteByte((byte)'\'');
    }

    public static void WriteText(ByteWriter w, ReadOnlySpan<char> value)
    {
        if (value.Contains('\0'))
        {
            throw new ArgumentException("a text value cannot contain a NUL character");
        }

        w.WriteByte((byte)'\'');
        while (true)
        {
            var quote = value.IndexOf('\'');
            if (quote < 0)
            {
                break;
            }

            w.WriteUtf8(value[..(quote + 1)]);
            w.WriteByte((byte)'\'');
            value = value[(quote + 1)..];
        }

        w.WriteUtf8(value);
        w.WriteByte((byte)'\'');
    }

    private static void CheckYear(int year, string what)
    {
        if (year is < 1 or > 9999)
        {
            throw new ArgumentOutOfRangeException(what, $"a {what} in year {year} has no literal here: years 1 to 9999 are written");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Write2(Span<byte> span, int value)
    {
        span[0] = (byte)('0' + value / 10);
        span[1] = (byte)('0' + value % 10);
    }

    private static int WriteYmd(Span<byte> span, int year, int month, int day)
    {
        span[0] = (byte)('0' + year / 1000);
        span[1] = (byte)('0' + year / 100 % 10);
        span[2] = (byte)('0' + year / 10 % 10);
        span[3] = (byte)('0' + year % 10);
        span[4] = (byte)'-';
        Write2(span[5..], month);
        span[7] = (byte)'-';
        Write2(span[8..], day);
        return 10;
    }

    /// <summary><c>HH:MM:SS[.ffffff]</c>, the fraction without trailing zeros.</summary>
    private static int WriteClock(Span<byte> span, long ticksOfDay)
    {
        var micros = ticksOfDay / TicksPerMicrosecond;
        var hours = (int)(micros / 3_600_000_000);
        var minutes = (int)(micros % 3_600_000_000 / 60_000_000);
        var seconds = (int)(micros % 60_000_000 / 1_000_000);
        var fraction = (int)(micros % 1_000_000);
        Write2(span, hours);
        span[2] = (byte)':';
        Write2(span[3..], minutes);
        span[5] = (byte)':';
        Write2(span[6..], seconds);
        if (fraction == 0)
        {
            return 8;
        }

        span[8] = (byte)'.';
        var at = 14;
        var digits = 6;
        while (fraction % 10 == 0)
        {
            fraction /= 10;
            at--;
            digits--;
        }

        for (var i = 0; i < digits; i++)
        {
            span[at - i] = (byte)('0' + fraction % 10);
            fraction /= 10;
        }

        return 9 + digits;
    }

    /// <summary>
    /// A UTC or local instant is written in UTC with <c>+00</c>; an unspecified one is a wall-clock time
    /// and is written with no zone, which a <c>timestamp</c> column takes as it is.
    /// </summary>
    public static void WriteDateTime(ByteWriter w, DateTime value) => WriteDateTimeBody(w, value, quote: true);

    private static void WriteDateTimeBody(ByteWriter w, DateTime value, bool quote)
    {
        var zone = value.Kind != DateTimeKind.Unspecified;
        if (value.Kind == DateTimeKind.Local)
        {
            value = value.ToUniversalTime();
        }

        CheckYear(value.Year, "timestamp");
        var span = w.GetSpan(34);
        var at = 0;
        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        at += WriteYmd(span[at..], value.Year, value.Month, value.Day);
        span[at++] = (byte)' ';
        at += WriteClock(span[at..], value.TimeOfDay.Ticks);
        if (zone)
        {
            span[at++] = (byte)'+';
            span[at++] = (byte)'0';
            span[at++] = (byte)'0';
        }

        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        w.Advance(at);
    }

    public static void WriteDateTimeOffset(ByteWriter w, DateTimeOffset value) => WriteDateTime(w, value.UtcDateTime);

    public static void WriteDate(ByteWriter w, DateOnly value) => WriteDateBody(w, value, quote: true);

    private static void WriteDateBody(ByteWriter w, DateOnly value, bool quote)
    {
        CheckYear(value.Year, "date");
        var span = w.GetSpan(12);
        var at = 0;
        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        at += WriteYmd(span[at..], value.Year, value.Month, value.Day);
        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        w.Advance(at);
    }

    public static void WriteTime(ByteWriter w, TimeOnly value) => WriteTimeBody(w, value, quote: true);

    private static void WriteTimeBody(ByteWriter w, TimeOnly value, bool quote)
    {
        var span = w.GetSpan(18);
        var at = 0;
        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        at += WriteClock(span[at..], value.Ticks);
        if (quote)
        {
            span[at++] = (byte)'\'';
        }

        w.Advance(at);
    }

    /// <summary><c>'&lt;months&gt; mons &lt;days&gt; days &lt;micros&gt; microseconds'</c>, each part only when not zero.</summary>
    public static void WriteInterval(ByteWriter w, RasterizedDbInterval value)
    {
        w.WriteByte((byte)'\'');
        WriteIntervalBody(w, value);
        w.WriteByte((byte)'\'');
    }

    internal static void WriteIntervalBody(ByteWriter w, RasterizedDbInterval value)
    {
        var any = false;
        if (value.Months != 0)
        {
            w.WriteInt64Text(value.Months);
            w.Write(" mons"u8);
            any = true;
        }

        if (value.Days != 0)
        {
            if (any)
            {
                w.WriteByte((byte)' ');
            }

            w.WriteInt64Text(value.Days);
            w.Write(" days"u8);
            any = true;
        }

        if (value.Microseconds != 0 || !any)
        {
            if (any)
            {
                w.WriteByte((byte)' ');
            }

            w.WriteInt64Text(value.Microseconds);
            w.Write(" microseconds"u8);
        }
    }

    // ------------------------------------------------------------------ arrays

    /// <summary>
    /// An array literal: <c>'{1,2,NULL}'</c>. The body is built apart and then quoted as text, so a
    /// <c>'</c> inside an element is doubled like any other.
    /// </summary>
    public static void WriteArray(ByteWriter w, IEnumerable values)
    {
        using var body = new ByteWriter(256);
        WriteArrayBody(body, values);
        w.WriteByte((byte)'\'');
        var span = body.WrittenSpan;
        while (true)
        {
            var quote = span.IndexOf((byte)'\'');
            if (quote < 0)
            {
                break;
            }

            w.Write(span[..(quote + 1)]);
            w.WriteByte((byte)'\'');
            span = span[(quote + 1)..];
        }

        w.Write(span);
        w.WriteByte((byte)'\'');
    }

    internal static void WriteArrayBody(ByteWriter w, IEnumerable values)
    {
        w.WriteByte((byte)'{');
        var first = true;
        foreach (var element in values)
        {
            if (!first)
            {
                w.WriteByte((byte)',');
            }

            first = false;
            WriteArrayElement(w, element);
        }

        w.WriteByte((byte)'}');
    }

    private static void WriteArrayElement(ByteWriter w, object? element)
    {
        switch (element)
        {
            case null or DBNull:
                w.Write("NULL"u8);
                return;
            case bool v:
                w.WriteByte(v ? (byte)'t' : (byte)'f');
                return;
            case sbyte or short or int or long:
                w.WriteInt64Text(Convert.ToInt64(element, CultureInfo.InvariantCulture));
                return;
            case byte or ushort or uint or ulong:
                w.WriteUInt64Text(Convert.ToUInt64(element, CultureInfo.InvariantCulture));
                return;
            case Int128 v:
                w.WriteFormatted(v);
                return;
            case BigInteger v:
                w.WriteAscii(v.ToString(CultureInfo.InvariantCulture));
                return;
            case decimal v:
                w.WriteFormatted(v);
                return;
            case float v:
                WriteArrayFloat(w, v);
                return;
            case double v:
                WriteArrayFloat(w, v);
                return;
            case string or char or Guid or DateTime or DateTimeOffset or DateOnly or TimeOnly or TimeSpan or RasterizedDbInterval or byte[] or JsonDocument or JsonElement or JsonNode:
                WriteQuotedElement(w, element);
                return;
            case Enum v:
                WriteArrayElement(w, Convert.ChangeType(v, Enum.GetUnderlyingType(v.GetType()), CultureInfo.InvariantCulture));
                return;
            case IEnumerable inner:
                WriteArrayBody(w, inner);
                return;
            default:
                throw new NotSupportedException($"an array element of type {element.GetType()} has no SQL literal in this driver");
        }
    }

    private static void WriteArrayFloat(ByteWriter w, double value)
    {
        if (double.IsNaN(value))
        {
            w.Write("NaN"u8);
        }
        else if (double.IsInfinity(value))
        {
            w.Write(value < 0 ? "-Infinity"u8 : "Infinity"u8);
        }
        else
        {
            WriteFloatText(w, value);
        }
    }

    private static void WriteArrayFloat(ByteWriter w, float value)
    {
        if (float.IsNaN(value) || float.IsInfinity(value))
        {
            WriteArrayFloat(w, (double)value);
            return;
        }

        Span<byte> text = stackalloc byte[32];
        value.TryFormat(text, out var written, default, CultureInfo.InvariantCulture);
        WriteNormalisedFloat(w, text[..written]);
    }

    private static void WriteQuotedElement(ByteWriter w, object element)
    {
        using var text = new ByteWriter(64);
        switch (element)
        {
            case string v:
                if (v.Contains('\0'))
                {
                    throw new ArgumentException("a text value cannot contain a NUL character");
                }

                text.WriteUtf8(v);
                break;
            case char v:
                text.WriteUtf8(new ReadOnlySpan<char>(in v));
                break;
            case Guid v:
                text.WriteFormatted(v);
                break;
            case DateTime v:
                WriteDateTimeBody(text, v, quote: false);
                break;
            case DateTimeOffset v:
                WriteDateTimeBody(text, v.UtcDateTime, quote: false);
                break;
            case DateOnly v:
                WriteDateBody(text, v, quote: false);
                break;
            case TimeOnly v:
                WriteTimeBody(text, v, quote: false);
                break;
            case TimeSpan v:
                WriteIntervalBody(text, RasterizedDbInterval.FromTimeSpan(v));
                break;
            case RasterizedDbInterval v:
                WriteIntervalBody(text, v);
                break;
            case byte[] v:
                text.Write("\\x"u8);
                text.WriteHex(v);
                break;
            case JsonDocument v:
                text.WriteUtf8(v.RootElement.GetRawText());
                break;
            case JsonElement v:
                text.WriteUtf8(v.GetRawText());
                break;
            case JsonNode v:
                text.WriteUtf8(v.ToJsonString());
                break;
        }

        w.WriteByte((byte)'"');
        foreach (var b in text.WrittenSpan)
        {
            if (b is (byte)'"' or (byte)'\\')
            {
                w.WriteByte((byte)'\\');
            }

            w.WriteByte(b);
        }

        w.WriteByte((byte)'"');
    }

    // --------------------------------------------------------------- identifiers

    /// <summary>
    /// An identifier, quoted only when it has to be: lower-case letters, digits and <c>_</c> that are
    /// not a keyword pass as written.
    /// </summary>
    public static string QuoteIdentifier(string name)
    {
        if (string.IsNullOrEmpty(name) || name.Contains('\0'))
        {
            throw new ArgumentException($"\"{name}\" is not a usable identifier");
        }

        var plain = true;
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (!(c == '_' || c is >= 'a' and <= 'z' || (i > 0 && c is >= '0' and <= '9')))
            {
                plain = false;
                break;
            }
        }

        if (plain && Array.BinarySearch(Keywords, name, StringComparer.Ordinal) < 0)
        {
            return name;
        }

        return string.Concat("\"", name.Replace("\"", "\"\""), "\"");
    }

    public static string QuoteText(string value)
    {
        if (value.Contains('\0'))
        {
            throw new ArgumentException("a text value cannot contain a NUL character");
        }

        return string.Concat("'", value.Replace("'", "''"), "'");
    }

    // Sorted (ordinal): searched with BinarySearch.
    private static readonly string[] Keywords =
    [
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "between", "both", "by",
        "case", "cast", "check", "collate", "column", "constraint", "create", "current_date", "current_role",
        "current_time", "current_timestamp", "current_user", "default", "deferrable", "delete", "desc", "distinct",
        "do", "else", "end", "except", "exists", "false", "fetch", "filter", "for", "foreign", "from", "grant",
        "group", "having", "ilike", "in", "index", "initially", "inner", "insert", "intersect", "into", "is", "join",
        "key", "lateral", "leading", "left", "like", "limit", "localtime", "localtimestamp", "not", "null", "offset",
        "on", "only", "or", "order", "outer", "partition", "placing", "primary", "references", "returning", "right",
        "select", "session_user", "set", "some", "symmetric", "table", "then", "to", "trailing", "true", "union",
        "unique", "update", "user", "using", "values", "variadic", "when", "where", "window", "with",
    ];
}
