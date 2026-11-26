using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>Where one placeholder sits in a statement's UTF-8 text, and which parameter fills it.</summary>
internal readonly struct PlaceholderSpan(int start, int end, int parameter)
{
    public readonly int Start = start;
    public readonly int End = end;
    public readonly int Parameter = parameter;
}

internal enum TransactionVerb : byte
{
    None,
    Begin,
    End,
    /// <summary>SAVEPOINT, RELEASE, ROLLBACK TO: inside a block, and leaves it open.</summary>
    Savepoint,
}

/// <summary>
/// A statement's text scanned once: its UTF-8 bytes with every placeholder written as <c>$n</c>, where
/// the placeholders are, and what the text is (one statement or several, transaction control).
/// Cached by text, so a statement an application sends repeatedly is scanned once.
/// </summary>
/// <remarks>
/// The scanner is the client crate's (<c>rasterizeddb_client/src/params.rs</c>): strings, <c>E''</c>
/// strings, quoted names, dollar quotes and both comment forms are skipped, so a placeholder inside one
/// is left alone. On top of <c>$n</c> it reads ADO.NET's <c>@name</c>.
/// </remarks>
internal sealed class ParsedSql
{
    public readonly byte[] Utf8;
    public readonly PlaceholderSpan[] Spans;
    /// <summary>For <c>@name</c> placeholders, the name of parameter <c>i</c>; null when the text uses <c>$n</c>.</summary>
    public readonly string[]? Names;
    public readonly int ParameterCount;
    /// <summary>Whether some placeholder names parameter <c>i</c> (with <c>$1</c> and <c>$3</c>, parameter 2 is not).</summary>
    public readonly bool[] Used;
    public readonly bool MultipleStatements;
    public readonly TransactionVerb Verb;
    /// <summary>Byte offset of the <c>;</c> a second statement follows, for the error that names it.</summary>
    public readonly int SecondStatementAt;

    private ParsedSql(byte[] utf8, PlaceholderSpan[] spans, string[]? names, int parameterCount, bool multiple, int secondAt, TransactionVerb verb)
    {
        Utf8 = utf8;
        Spans = spans;
        Names = names;
        ParameterCount = parameterCount;
        Used = new bool[parameterCount];
        foreach (var span in spans)
        {
            Used[span.Parameter] = true;
        }

        MultipleStatements = multiple;
        SecondStatementAt = secondAt;
        Verb = verb;
    }

    /// <summary>What stands in for the scan of a statement the driver built itself.</summary>
    public static readonly ParsedSql Raw = new([], [], null, 0, false, -1, TransactionVerb.None);

    private const int CacheLimit = 4096;
    private static readonly ConcurrentDictionary<string, ParsedSql> Cache = new(StringComparer.Ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ParsedSql Get(string sql)
    {
        return Cache.TryGetValue(sql, out var parsed) ? parsed : Add(sql);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ParsedSql Add(string sql)
    {
        var parsed = Parse(sql);
        // Text with values written into it never repeats; past the limit such statements are parsed
        // each time instead of filling the cache.
        if (Cache.Count < CacheLimit)
        {
            Cache.TryAdd(sql, parsed);
        }

        return parsed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsIdentByte(byte b) => (uint)((b | 0x20) - 'a') <= 'z' - 'a' || (uint)(b - '0') <= 9 || b == '_' || b == '$' || b >= 0x80;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsIdentStart(byte b) => (uint)((b | 0x20) - 'a') <= 'z' - 'a' || b == '_' || b >= 0x80;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDigit(byte b) => (uint)(b - '0') <= 9;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSpace(byte b) => b == ' ' || (uint)(b - '\t') <= '\r' - '\t';

    public static ParsedSql Parse(string sql)
    {
        if (sql.AsSpan().Contains('\0'))
        {
            throw new ArgumentException("the statement contains a NUL character");
        }

        var max = Encoding.UTF8.GetMaxByteCount(sql.Length);
        var rented = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            var length = Encoding.UTF8.GetBytes(sql, rented);
            return Parse(rented.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static ParsedSql Parse(ReadOnlySpan<byte> bytes)
    {
        List<PlaceholderSpan>? spans = null;
        List<string>? names = null;
        // Set when a named placeholder is rewritten to `$n`: the text then differs from the input.
        ByteWriter? rewritten = null;
        var copied = 0;
        var positional = 0;
        var multiple = false;
        var secondAt = -1;

        try
        {
            var i = 0;
            while (i < bytes.Length)
            {
                // Everything a placeholder or a statement end can start with; the rest is skipped in one
                // vectorised search.
                var next = bytes[i..].IndexOfAny("'\"-/;$@"u8);
                if (next < 0)
                {
                    break;
                }

                i += next;
                var b = bytes[i];
                switch (b)
                {
                    case (byte)'\'':
                        i = SkipQuoted(bytes, i, (byte)'\'', EscapeStringAt(bytes, i));
                        break;
                    case (byte)'"':
                        i = SkipQuoted(bytes, i, (byte)'"', false);
                        break;
                    case (byte)'-' when i + 1 < bytes.Length && bytes[i + 1] == '-':
                        i = SkipLineComment(bytes, i);
                        break;
                    case (byte)'/' when i + 1 < bytes.Length && bytes[i + 1] == '*':
                        i = SkipBlockComment(bytes, i);
                        break;
                    case (byte)';':
                        if (!multiple && !OnlyTriviaAfter(bytes, i + 1))
                        {
                            multiple = true;
                            secondAt = i;
                        }

                        i++;
                        break;
                    case (byte)'$' when i == 0 || !IsIdentByte(bytes[i - 1]):
                    {
                        var digits = 0;
                        while (i + 1 + digits < bytes.Length && IsDigit(bytes[i + 1 + digits]))
                        {
                            digits++;
                        }

                        if (digits > 0)
                        {
                            if (names is not null)
                            {
                                throw new ArgumentException("the statement mixes $n and @name placeholders; use one form");
                            }

                            var n = 0;
                            for (var d = 0; d < digits; d++)
                            {
                                n = checked(n * 10 + (bytes[i + 1 + d] - '0'));
                            }

                            if (n == 0)
                            {
                                throw new ArgumentException("$0 is not a placeholder: parameters are numbered from $1");
                            }

                            (spans ??= []).Add(new PlaceholderSpan(i, i + 1 + digits, n - 1));
                            positional = Math.Max(positional, n);
                            i += 1 + digits;
                        }
                        else
                        {
                            var end = DollarQuoteEnd(bytes, i);
                            i = end < 0 ? i + 1 : end;
                        }

                        break;
                    }
                    case (byte)'@' when i + 1 < bytes.Length && IsIdentStart(bytes[i + 1]) && (i == 0 || (!IsIdentByte(bytes[i - 1]) && bytes[i - 1] != '@')):
                    {
                        if (positional > 0)
                        {
                            throw new ArgumentException("the statement mixes $n and @name placeholders; use one form");
                        }

                        var end = i + 1;
                        while (end < bytes.Length && IsIdentByte(bytes[end]) && bytes[end] != '$')
                        {
                            end++;
                        }

                        var name = Encoding.UTF8.GetString(bytes[(i + 1)..end]);
                        names ??= [];
                        var index = -1;
                        for (var k = 0; k < names.Count; k++)
                        {
                            if (string.Equals(names[k], name, StringComparison.OrdinalIgnoreCase))
                            {
                                index = k;
                                break;
                            }
                        }

                        if (index < 0)
                        {
                            index = names.Count;
                            names.Add(name);
                        }

                        rewritten ??= new ByteWriter(bytes.Length + 16);
                        rewritten.Write(bytes[copied..i]);
                        var start = rewritten.Length;
                        rewritten.WriteByte((byte)'$');
                        rewritten.WriteInt64Text(index + 1);
                        (spans ??= []).Add(new PlaceholderSpan(start, rewritten.Length, index));
                        copied = end;
                        i = end;
                        break;
                    }
                    default:
                        i++;
                        break;
                }
            }

            byte[] utf8;
            if (rewritten is not null)
            {
                rewritten.Write(bytes[copied..]);
                utf8 = rewritten.WrittenSpan.ToArray();
            }
            else
            {
                utf8 = bytes.ToArray();
            }

            return new ParsedSql(
                utf8,
                spans?.ToArray() ?? [],
                names?.ToArray(),
                names?.Count ?? positional,
                multiple,
                secondAt,
                VerbOf(bytes));
        }
        finally
        {
            rewritten?.Dispose();
        }
    }

    private static bool EscapeStringAt(ReadOnlySpan<byte> bytes, int quote) =>
        quote >= 1 && (bytes[quote - 1] | 0x20) == 'e' && (quote == 1 || !IsIdentByte(bytes[quote - 2]));

    private static int SkipQuoted(ReadOnlySpan<byte> bytes, int start, byte delimiter, bool backslashEscapes)
    {
        var i = start + 1;
        while (i < bytes.Length)
        {
            var b = bytes[i];
            if (backslashEscapes && b == '\\')
            {
                i += 2;
                continue;
            }

            if (b == delimiter)
            {
                if (i + 1 < bytes.Length && bytes[i + 1] == delimiter)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return bytes.Length;
    }

    private static int SkipLineComment(ReadOnlySpan<byte> bytes, int start)
    {
        var n = bytes[start..].IndexOf((byte)'\n');
        return n < 0 ? bytes.Length : start + n + 1;
    }

    private static int SkipBlockComment(ReadOnlySpan<byte> bytes, int start)
    {
        var depth = 0;
        var i = start;
        while (i + 1 < bytes.Length)
        {
            if (bytes[i] == '/' && bytes[i + 1] == '*')
            {
                depth++;
                i += 2;
            }
            else if (bytes[i] == '*' && bytes[i + 1] == '/')
            {
                depth--;
                i += 2;
                if (depth == 0)
                {
                    return i;
                }
            }
            else
            {
                i++;
            }
        }

        return bytes.Length;
    }

    private static int DollarQuoteEnd(ReadOnlySpan<byte> bytes, int start)
    {
        var rest = bytes[(start + 1)..];
        var tagLength = rest.IndexOf((byte)'$');
        if (tagLength < 0)
        {
            return -1;
        }

        var tag = rest[..tagLength];
        if (tag.Length > 0 && !IsIdentStart(tag[0]))
        {
            return -1;
        }

        foreach (var b in tag)
        {
            if (!(IsIdentByte(b) && b != '$'))
            {
                return -1;
            }
        }

        var delimiter = bytes.Slice(start, tagLength + 2);
        var body = start + tagLength + 2;
        var close = bytes[body..].IndexOf(delimiter);
        return close < 0 ? bytes.Length : body + close + delimiter.Length;
    }

    private static bool OnlyTriviaAfter(ReadOnlySpan<byte> bytes, int from)
    {
        var i = from;
        while (i < bytes.Length)
        {
            var b = bytes[i];
            if (IsSpace(b) || b == ';')
            {
                i++;
            }
            else if (b == '-' && i + 1 < bytes.Length && bytes[i + 1] == '-')
            {
                i = SkipLineComment(bytes, i);
            }
            else if (b == '/' && i + 1 < bytes.Length && bytes[i + 1] == '*')
            {
                i = SkipBlockComment(bytes, i);
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    private static int SkipTrivia(ReadOnlySpan<byte> bytes, int i, bool parens)
    {
        while (i < bytes.Length)
        {
            var b = bytes[i];
            if (IsSpace(b) || (parens && b == '('))
            {
                i++;
            }
            else if (b == '-' && i + 1 < bytes.Length && bytes[i + 1] == '-')
            {
                i = SkipLineComment(bytes, i);
            }
            else if (b == '/' && i + 1 < bytes.Length && bytes[i + 1] == '*')
            {
                i = SkipBlockComment(bytes, i);
            }
            else
            {
                break;
            }
        }

        return i;
    }

    private static ReadOnlySpan<byte> Word(ReadOnlySpan<byte> bytes, int at)
    {
        var end = at;
        while (end < bytes.Length && (uint)((bytes[end] | 0x20) - 'a') <= 'z' - 'a')
        {
            end++;
        }

        return bytes[at..end];
    }

    private static bool Is(ReadOnlySpan<byte> word, ReadOnlySpan<byte> upper) =>
        word.Length == upper.Length && System.Text.Ascii.EqualsIgnoreCase(word, upper);

    /// <summary>What a statement does to the connection's transaction block, by its first words.</summary>
    private static TransactionVerb VerbOf(ReadOnlySpan<byte> bytes)
    {
        var at = SkipTrivia(bytes, 0, parens: true);
        var word = Word(bytes, at);
        if (word.Length is < 3 or > 9)
        {
            return TransactionVerb.None;
        }

        if (Is(word, "BEGIN"u8) || Is(word, "START"u8))
        {
            return TransactionVerb.Begin;
        }

        if (Is(word, "COMMIT"u8) || Is(word, "END"u8) || Is(word, "ABORT"u8))
        {
            return TransactionVerb.End;
        }

        if (Is(word, "SAVEPOINT"u8) || Is(word, "RELEASE"u8))
        {
            return TransactionVerb.Savepoint;
        }

        if (Is(word, "ROLLBACK"u8))
        {
            // ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] name keeps the block open.
            var i = SkipTrivia(bytes, at + word.Length, parens: false);
            var second = Word(bytes, i);
            if (Is(second, "WORK"u8) || Is(second, "TRANSACTION"u8))
            {
                i = SkipTrivia(bytes, i + second.Length, parens: false);
                second = Word(bytes, i);
            }

            return Is(second, "TO"u8) ? TransactionVerb.Savepoint : TransactionVerb.End;
        }

        return TransactionVerb.None;
    }
}
