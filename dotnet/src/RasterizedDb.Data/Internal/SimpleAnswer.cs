using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;

namespace RasterizedDb.Data.Internal;

internal readonly record struct ServerError(string Message, string? SqlState);

/// <summary>Where one row-returning result sits in a simple-query answer.</summary>
internal readonly struct ResultInfo(int descriptionAt, int descriptionLength, int rowsAt)
{
    /// <summary>The body of its RowDescription.</summary>
    public readonly int DescriptionAt = descriptionAt;
    public readonly int DescriptionLength = descriptionLength;
    /// <summary>The first message after the description: its first DataRow, or its CommandComplete.</summary>
    public readonly int RowsAt = rowsAt;
}

/// <summary>What a pass over a simple-query answer found.</summary>
internal struct SimpleScan
{
    public ServerError? Error;
    /// <summary><c>I</c> idle, <c>T</c> in a transaction block, <c>E</c> in a failed one; 0 when the answer never said.</summary>
    public byte Status;
    /// <summary>Rows the answer's INSERT, UPDATE, DELETE and MERGE statements reported, or -1 when it ran none.</summary>
    public long Affected;
    /// <summary>The last command tag, for a caller that asked one statement.</summary>
    public string? LastTag;
}

/// <summary>
/// The simple-query form (<c>\x01SIMPLE;</c>): the answer pgwire would give, as PostgreSQL backend
/// messages. Each is a one-byte kind, a big-endian <c>i32</c> length that counts itself, and a body:
/// <c>T</c> RowDescription, <c>D</c> DataRow, <c>C</c> CommandComplete, <c>E</c> ErrorResponse, <c>N</c>
/// NoticeResponse, <c>Z</c> ReadyForQuery.
/// </summary>
internal static class SimpleAnswer
{
    private static RasterizedDbException Damaged(string what) => new(RasterizedDbErrorKind.Protocol, $"a simple-query answer is damaged: {what}");

    /// <summary>Reads the message at <paramref name="at"/>, returning its kind and body and moving past it.</summary>
    public static byte Next(ReadOnlySpan<byte> messages, ref int at, out int bodyAt, out int bodyLength)
    {
        if (messages.Length - at < 5)
        {
            throw Damaged("a message is cut short");
        }

        var kind = messages[at];
        var length = BinaryPrimitives.ReadInt32BigEndian(messages[(at + 1)..]) - 4;
        if (length < 0 || length > messages.Length - at - 5)
        {
            throw Damaged("a message's length runs past the answer");
        }

        bodyAt = at + 5;
        bodyLength = length;
        at = bodyAt + length;
        return kind;
    }

    public static ServerError? FirstError(ReadOnlySpan<byte> messages)
    {
        var at = 0;
        while (at < messages.Length)
        {
            if (Next(messages, ref at, out var bodyAt, out var bodyLength) == (byte)'E')
            {
                return ErrorFields(messages.Slice(bodyAt, bodyLength));
            }
        }

        return null;
    }

    private static ServerError ErrorFields(ReadOnlySpan<byte> body) => Fields(body, out _);

    private static ServerError Fields(ReadOnlySpan<byte> body, out string severity)
    {
        string? code = null;
        var message = string.Empty;
        severity = "NOTICE";
        var at = 0;
        while (at < body.Length && body[at] != 0)
        {
            var field = body[at++];
            var end = body[at..].IndexOf((byte)0);
            if (end < 0)
            {
                throw Damaged("a string has no end");
            }

            var value = body.Slice(at, end);
            at += end + 1;
            switch (field)
            {
                case (byte)'C':
                    code = Encoding.ASCII.GetString(value);
                    break;
                case (byte)'M':
                    message = Encoding.UTF8.GetString(value);
                    break;
                case (byte)'S':
                    severity = Encoding.ASCII.GetString(value);
                    break;
            }
        }

        return new ServerError(message, string.IsNullOrEmpty(code) ? null : code);
    }

    /// <summary>
    /// One pass over the whole answer: finds its row-returning results, sums what its writes affected,
    /// hands out its notices, and keeps the error that stopped it. The message bodies of rows are
    /// stepped over, not read.
    /// </summary>
    public static SimpleScan Scan(ReadOnlySpan<byte> messages, List<ResultInfo>? results, Action<RasterizedDbNotice>? notices)
    {
        var scan = new SimpleScan { Affected = -1 };
        var at = 0;
        var ready = false;
        var description = -1;
        var descriptionLength = 0;
        var rowsAt = 0;
        while (at < messages.Length)
        {
            var kind = Next(messages, ref at, out var bodyAt, out var bodyLength);
            switch (kind)
            {
                case (byte)'D':
                    break;
                case (byte)'T':
                    description = bodyAt;
                    descriptionLength = bodyLength;
                    rowsAt = at;
                    break;
                case (byte)'C':
                {
                    var tag = messages.Slice(bodyAt, bodyLength);
                    var end = tag.IndexOf((byte)0);
                    if (end >= 0)
                    {
                        tag = tag[..end];
                    }

                    if (description >= 0)
                    {
                        results?.Add(new ResultInfo(description, descriptionLength, rowsAt));
                        description = -1;
                    }
                    else if (TagCount(tag, out var count))
                    {
                        scan.Affected = scan.Affected < 0 ? count : scan.Affected + count;
                    }

                    scan.LastTag = Encoding.ASCII.GetString(tag);
                    break;
                }
                case (byte)'E':
                {
                    // The first is the one that stopped the request; a later one adds to it.
                    var error = ErrorFields(messages.Slice(bodyAt, bodyLength));
                    scan.Error = scan.Error is { } first ? first with { Message = $"{first.Message}; then: {error.Message}" } : error;
                    description = -1;
                    break;
                }
                case (byte)'N':
                    if (notices is not null)
                    {
                        var notice = Fields(messages.Slice(bodyAt, bodyLength), out var severity);
                        notices(new RasterizedDbNotice(severity, notice.SqlState ?? "00000", notice.Message));
                    }

                    break;
                case (byte)'Z':
                    scan.Status = bodyLength > 0 ? messages[bodyAt] : (byte)'I';
                    ready = true;
                    break;
            }
        }

        if (!ready && scan.Error is null)
        {
            throw Damaged("it ends before ReadyForQuery");
        }

        return scan;
    }

    /// <summary>The count a write's command tag ends with (<c>INSERT 0 3</c>, <c>UPDATE 2</c>).</summary>
    private static bool TagCount(ReadOnlySpan<byte> tag, out long count)
    {
        count = 0;
        var space = tag.IndexOf((byte)' ');
        if (space < 0)
        {
            return false;
        }

        var verb = tag[..space];
        if (!(verb.SequenceEqual("INSERT"u8) || verb.SequenceEqual("UPDATE"u8) || verb.SequenceEqual("DELETE"u8)
              || verb.SequenceEqual("MERGE"u8) || verb.SequenceEqual("COPY"u8)))
        {
            return false;
        }

        var last = tag.LastIndexOf((byte)' ');
        return Utf8Parser.TryParse(tag[(last + 1)..], out count, out var consumed) && consumed == tag.Length - last - 1;
    }
}
