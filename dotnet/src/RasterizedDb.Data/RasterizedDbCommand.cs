using System.Buffers.Binary;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// A statement to run. Placeholders are <c>$1</c>…<c>$n</c> or <c>@name</c>; their values are written
/// into the request as SQL literals (see <see cref="RasterizedDbParameter"/>).
/// </summary>
/// <remarks>
/// <see cref="ResultFormat"/> decides how it is sent. <see cref="RasterizedDbResultFormat.Binary"/> is
/// the native path: one statement, the engine's typed cells, no column names; where the server knows
/// the form, the statement's text and its values travel apart (<c>\x01EXEC;</c>), so the server answers
/// a repeated statement from the parse it learned for its shape.
/// <see cref="RasterizedDbResultFormat.Text"/> is the simple-query form: what a PostgreSQL client's
/// simple query gets, with names and types, for any number of statements.
/// </remarks>
public sealed class RasterizedDbCommand : DbCommand
{
    private const int MaxRendered = 8 * 1024 * 1024;

    private string _text = string.Empty;
    private ParsedSql? _parsed;
    private RasterizedDbConnection? _connection;
    private RasterizedDbParameterCollection? _parameters;
    private int? _timeout;

    // The request in flight, for Cancel.
    private long _cancelConnector;
    private long _cancelSequence;

    public RasterizedDbCommand()
    {
    }

    public RasterizedDbCommand(string commandText, RasterizedDbConnection? connection = null, RasterizedDbTransaction? transaction = null)
    {
        _text = commandText;
        _connection = connection;
        Transaction = transaction;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _text;
        set
        {
            value ??= string.Empty;
            if (!ReferenceEquals(value, _text))
            {
                _text = value;
                _parsed = null;
            }
        }
    }

    /// <summary>Seconds to wait for the answer; the connection string's <c>Command Timeout</c> unless set. 0 waits indefinitely.</summary>
    public override int CommandTimeout
    {
        get => _timeout ?? _connection?.DefaultCommandTimeout ?? 30;
        set => _timeout = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
    }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
            {
                throw new NotSupportedException("RasterizedDB commands are statement text; call a function with SELECT f(…)");
            }
        }
    }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;

    /// <summary>How the rows are asked for; the connection's setting unless set.</summary>
    public RasterizedDbResultFormat ResultFormat { get; set; }

    public new RasterizedDbConnection? Connection
    {
        get => _connection;
        set => _connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (RasterizedDbConnection?)value;
    }

    public new RasterizedDbParameterCollection Parameters => _parameters ??= new RasterizedDbParameterCollection();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction? DbTransaction { get; set; }

    protected override DbParameter CreateDbParameter() => new RasterizedDbParameter();

    public new RasterizedDbParameter CreateParameter() => new();

    /// <summary>Nothing to do: there is no server-side prepared statement. The statement's text is scanned once and cached either way.</summary>
    public override void Prepare() => _ = Parsed();

    private ParsedSql Parsed() => _parsed ??= ParsedSql.Get(_text);

    /// <summary>
    /// A statement the driver built itself, already UTF-8 and with nothing to scan or fill in (a KV
    /// command): sent as it is, on the native path, in place of <see cref="CommandText"/>.
    /// </summary>
    internal ReadOnlyMemory<byte> RawStatement;

    // ------------------------------------------------------------------ request

    private RasterizedDbParameter ParameterFor(ParsedSql parsed, int index)
    {
        var items = _parameters?.Items;
        if (parsed.Names is { } names)
        {
            var name = names[index];
            if (items is not null)
            {
                // Usually declared in the order they are used.
                if ((uint)index < (uint)items.Count && string.Equals(items[index].TrimmedName, name, StringComparison.OrdinalIgnoreCase))
                {
                    return items[index];
                }

                foreach (var item in items)
                {
                    if (string.Equals(item.TrimmedName, name, StringComparison.OrdinalIgnoreCase))
                    {
                        return item;
                    }
                }
            }

            throw new InvalidOperationException($"@{name} has no value: the command has no parameter of that name");
        }

        if (items is null || (uint)index >= (uint)items.Count)
        {
            throw new InvalidOperationException($"${index + 1} has no value: {items?.Count ?? 0} parameter(s) were given");
        }

        return items[index];
    }

    /// <summary>The statement as text, each placeholder replaced by its value's literal.</summary>
    private void WriteRendered(ByteWriter writer, ParsedSql parsed)
    {
        var sql = parsed.Utf8.AsSpan();
        var copied = 0;
        foreach (ref readonly var span in parsed.Spans.AsSpan())
        {
            writer.Write(sql[copied..span.Start]);
            ParameterFor(parsed, span.Parameter).WriteLiteral(writer);
            copied = span.End;
        }

        writer.Write(sql[copied..]);
    }

    /// <summary>
    /// The EXEC request: the text with its placeholders, each parameter's literal once, and where each
    /// placeholder sits. All integers little endian:
    /// <code>
    /// u32 sql_len, sql
    /// u16 n_literals, per literal: u32 len, bytes
    /// u16 n_spans,    per span:    u32 start, u32 end, u16 literal
    /// </code>
    /// </summary>
    /// <returns>False when the statement cannot travel this way (too many parameters, or it renders past what a server reads): send it as text.</returns>
    private bool TryWriteExec(ByteWriter writer, ParsedSql parsed)
    {
        var spans = parsed.Spans;
        var count = parsed.ParameterCount;
        if (count > ushort.MaxValue || spans.Length > ushort.MaxValue)
        {
            return false;
        }

        var sql = parsed.Utf8;
        writer.Write("\u0001EXEC;"u8);
        writer.WriteUInt32((uint)sql.Length);
        writer.Write(sql);
        writer.WriteUInt16((ushort)count);

        Span<int> lengths = count <= 64 ? stackalloc int[count] : new int[count];
        var used = parsed.Used;
        for (var i = 0; i < count; i++)
        {
            var lengthAt = writer.Length;
            writer.GetSpan(4);
            writer.Advance(4);
            var start = writer.Length;
            // A parameter no placeholder names is sent empty.
            if (used[i])
            {
                ParameterFor(parsed, i).WriteLiteral(writer);
            }

            lengths[i] = writer.Length - start;
            writer.PatchUInt32(lengthAt, (uint)lengths[i]);
        }

        long rendered = sql.Length;
        writer.WriteUInt16((ushort)spans.Length);
        foreach (ref readonly var span in spans.AsSpan())
        {
            var target = writer.GetSpan(10);
            BinaryPrimitives.WriteUInt32LittleEndian(target, (uint)span.Start);
            BinaryPrimitives.WriteUInt32LittleEndian(target[4..], (uint)span.End);
            BinaryPrimitives.WriteUInt16LittleEndian(target[8..], (ushort)span.Parameter);
            writer.Advance(10);
            rendered += lengths[span.Parameter] - (span.End - span.Start);
        }

        return rendered <= MaxRendered;
    }

    private static RasterizedDbException SecondStatement(ParsedSql parsed) => new(
        RasterizedDbErrorKind.Unsupported,
        $"the text holds more than one statement (a second one starts after the ';' at byte {parsed.SecondStatementAt}), and the binary " +
        "result format runs one statement per command. Send each on its own, or run the command with ResultFormat = Text, which takes several.");

    private async ValueTask Build(Connector connector, RasterizedDbDataSource source, ParsedSql parsed, bool text, bool async, CancellationToken cancellation)
    {
        var writer = connector.Writer;
        connector.BeginRequest();
        if (text)
        {
            // \x01SIMPLE:cancel=<token>;<statements>
            _cancelConnector = connector.Id;
            _cancelSequence = connector.NextCancelSequence();
            writer.Write("\u0001SIMPLE:cancel=c"u8);
            writer.WriteInt64Text(_cancelConnector);
            writer.WriteByte((byte)'-');
            writer.WriteInt64Text(_cancelSequence);
            writer.WriteByte((byte)';');
            WriteRendered(writer, parsed);
            return;
        }

        if (!RawStatement.IsEmpty)
        {
            writer.Write(RawStatement.Span);
            return;
        }

        if (parsed.MultipleStatements)
        {
            throw SecondStatement(parsed);
        }

        if (parsed.Spans.Length == 0)
        {
            writer.Write(parsed.Utf8);
            return;
        }

        var exec = source.ExecSupported;
        if (exec is null && !connector.InBlock)
        {
            exec = await Probe(connector, source, async, cancellation).ConfigureAwait(false);
            connector.BeginRequest();
        }

        if (exec == true)
        {
            if (TryWriteExec(writer, parsed))
            {
                return;
            }

            connector.BeginRequest();
        }

        WriteRendered(writer, parsed);
    }

    /// <summary>
    /// Asks the server once whether it knows the EXEC form, with <c>SELECT 1</c>: a server without it
    /// refuses the request without running anything, and statements are then rendered as text.
    /// </summary>
    private static async ValueTask<bool> Probe(Connector connector, RasterizedDbDataSource source, bool async, CancellationToken cancellation)
    {
        var writer = connector.Writer;
        connector.BeginRequest();
        writer.Write("\u0001EXEC;"u8);
        writer.WriteUInt32(8);
        writer.Write("SELECT 1"u8);
        writer.WriteUInt16(0);
        writer.WriteUInt16(0);
        var response = await connector.Exchange(async, 30, cancellation).ConfigureAwait(false);
        var span = response.Span;
        var at = connector.TakeNotices(span);
        var supported = span.Length > at && span[at] == Answer.Rows;
        response.Return();
        source.SetExecSupported(supported);
        return supported;
    }

    // ---------------------------------------------------------------- execution

    private enum Mode : byte
    {
        Reader,
        NonQuery,
        Scalar,
    }

    private struct Outcome
    {
        public RasterizedDbDataReader? Reader;
        public long Affected;
        public object? Scalar;
    }

    private RasterizedDbConnection RequireConnection()
    {
        var connection = _connection ?? throw new InvalidOperationException("the command has no connection");
        if (_text.Length == 0 && RawStatement.IsEmpty)
        {
            throw new InvalidOperationException("the command has no text");
        }

        return connection;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Outcome> Run(bool async, Mode mode, CommandBehavior behavior, CancellationToken cancellation)
    {
        var connection = RequireConnection();
        var connector = await connection.Ready(async, cancellation).ConfigureAwait(false);
        var source = connection.Source!;
        var raw = !RawStatement.IsEmpty;
        var parsed = raw ? ParsedSql.Raw : Parsed();
        var format = ResultFormat == RasterizedDbResultFormat.Default ? connection.DefaultResultFormat : ResultFormat;
        var text = !raw && format == RasterizedDbResultFormat.Text;
        var wasInBlock = connector.InBlock;

        try
        {
            await Build(connector, source, parsed, text, async, cancellation).ConfigureAwait(false);
        }
        catch
        {
            connector.Writer.Reset();
            throw;
        }

        Response response;
        try
        {
            response = await connector.Exchange(async, CommandTimeout, cancellation).ConfigureAwait(false);
        }
        catch (RasterizedDbException e) when (wasInBlock && e.Kind is RasterizedDbErrorKind.Transport or RasterizedDbErrorKind.Timeout)
        {
            // The request may have been a COMMIT the server carried out before the connection went.
            var unknown = text || parsed.Verb == TransactionVerb.End;
            connection.TransactionLost();
            throw new RasterizedDbException(
                RasterizedDbErrorKind.TransactionLost,
                unknown
                    ? $"the connection failed inside a transaction, on a request that may have committed it: {e.Message}"
                    : $"the connection failed inside a transaction, which the server rolled back: {e.Message}",
                inner: e,
                commitOutcomeUnknown: unknown);
        }
        finally
        {
            _cancelSequence = 0;
        }

        try
        {
            return text
                ? ReadText(connection, connector, ref response, mode, behavior)
                : ReadBinary(connection, connector, parsed, ref response, mode, behavior);
        }
        catch
        {
            response.Return();
            throw;
        }
    }

    private static Outcome ReadBinary(RasterizedDbConnection connection, Connector connector, ParsedSql parsed, ref Response response, Mode mode, CommandBehavior behavior)
    {
        var span = response.Span;
        var at = connector.TakeNotices(span);
        if (span.Length <= at)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "the server sent an empty answer");
        }

        var kind = span[at];
        if (kind == Answer.Error)
        {
            var error = RasterizedDbException.FromServer(Encoding.UTF8.GetString(span[(at + 1)..]));
            if (connector.InBlock)
            {
                // A failed COMMIT has ended the block; any other failure leaves it open, and failed.
                connector.InBlock = parsed.Verb != TransactionVerb.End;
                connector.BlockFailed = connector.InBlock;
            }

            throw error;
        }

        switch (parsed.Verb)
        {
            case TransactionVerb.Begin:
                connector.InBlock = true;
                connector.BlockFailed = false;
                break;
            case TransactionVerb.End:
                connector.InBlock = false;
                connector.BlockFailed = false;
                break;
        }

        var outcome = new Outcome { Affected = -1 };
        switch (kind)
        {
            case Answer.Ack:
                if (span.Length - at >= 9)
                {
                    outcome.Affected = (long)BinaryPrimitives.ReadUInt64LittleEndian(span[(at + 1)..]);
                }

                break;
            case Answer.Count:
                if (span.Length - at < 9)
                {
                    throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "a row count with no count in it");
                }

                outcome.Affected = (long)BinaryPrimitives.ReadUInt64LittleEndian(span[(at + 1)..]);
                break;
            case Answer.Rows:
            {
                var reader = connection.TakeReader();
                reader.InitBinary(connection, behavior, response, at + 1, -1);
                response = default;
                if (mode == Mode.Reader)
                {
                    outcome.Reader = reader;
                    return outcome;
                }

                try
                {
                    if (mode == Mode.Scalar)
                    {
                        outcome.Scalar = reader.Read() && reader.FieldCount > 0 ? reader.GetValue(0) : null;
                    }
                    else
                    {
                        // What a write with RETURNING affected is the rows it returned.
                        long rows = 0;
                        while (reader.Read())
                        {
                            rows++;
                        }

                        outcome.Affected = rows;
                    }
                }
                finally
                {
                    reader.Close();
                }

                return outcome;
            }
            case Answer.Simple:
                throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "a statement was answered in the simple-query form it was not sent in");
            default:
                throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, $"unknown answer tag {kind}");
        }

        response.Return();
        if (mode == Mode.Reader)
        {
            var reader = connection.TakeReader();
            reader.InitEmpty(connection, behavior, outcome.Affected);
            outcome.Reader = reader;
        }

        return outcome;
    }

    private static Outcome ReadText(RasterizedDbConnection connection, Connector connector, ref Response response, Mode mode, CommandBehavior behavior)
    {
        var span = response.Span;
        if (span.Length == 0)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "the server sent an empty answer");
        }

        if (span[0] != Answer.Simple)
        {
            // The refusal of a server that predates the form: the prefix made the request two
            // statements, so nothing ran.
            throw span[0] == Answer.Error
                ? new RasterizedDbException(
                    RasterizedDbErrorKind.Unsupported,
                    "this server does not answer the simple-query form over the native protocol (it predates it), and ran nothing; " +
                    $"use ResultFormat = Binary: {Encoding.UTF8.GetString(span[1..])}")
                : new RasterizedDbException(RasterizedDbErrorKind.Protocol, $"a simple query was answered with tag {span[0]}, not as a simple query");
        }

        var reader = connection.TakeReader();
        // Shifted by one: the scan's offsets are into the messages, which start after the tag byte.
        var messages = span[1..];
        var results = reader.TextResults;
        SimpleScan scan;
        try
        {
            scan = SimpleAnswer.Scan(messages, results, connector.NoticeSink);
        }
        catch
        {
            results.Clear();
            throw;
        }

        if (scan.Status != 0)
        {
            connector.InBlock = scan.Status != (byte)'I';
            connector.BlockFailed = scan.Status == (byte)'E';
        }

        if (scan.Error is { } error)
        {
            results.Clear();
            throw new RasterizedDbException(RasterizedDbErrorKind.Server, error.Message, error.SqlState);
        }

        var outcome = new Outcome { Affected = scan.Affected };
        // The scan's offsets count from the first message; the reader's from the answer's first byte.
        for (var i = 0; i < results.Count; i++)
        {
            var r = results[i];
            results[i] = new ResultInfo(r.DescriptionAt + 1, r.DescriptionLength, r.RowsAt + 1);
        }

        reader.InitText(connection, behavior, response, scan.Affected);
        response = default;
        if (mode == Mode.Reader)
        {
            outcome.Reader = reader;
            return outcome;
        }

        try
        {
            if (mode == Mode.Scalar)
            {
                outcome.Scalar = reader.Read() && reader.FieldCount > 0 ? reader.GetValue(0) : null;
            }
        }
        finally
        {
            reader.Close();
        }

        return outcome;
    }

    private static int Clamp(long affected) => affected > int.MaxValue ? int.MaxValue : (int)affected;

    public override int ExecuteNonQuery() => Clamp(Run(false, Mode.NonQuery, CommandBehavior.Default, default).GetAwaiter().GetResult().Affected);

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
        Clamp((await Run(true, Mode.NonQuery, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).Affected);

    public override object? ExecuteScalar() => Run(false, Mode.Scalar, CommandBehavior.Default, default).GetAwaiter().GetResult().Scalar;

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) =>
        (await Run(true, Mode.Scalar, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).Scalar;

    public new RasterizedDbDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

    public new RasterizedDbDataReader ExecuteReader(CommandBehavior behavior) => Run(false, Mode.Reader, behavior, default).GetAwaiter().GetResult().Reader!;

    public new Task<RasterizedDbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    public new async Task<RasterizedDbDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default) =>
        (await Run(true, Mode.Reader, behavior, cancellationToken).ConfigureAwait(false)).Reader!;

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        await ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Asks the server to stop the statement this command is running, from another thread; it then
    /// fails with SQLSTATE 57014 and its connection goes on. Works for the text result format, whose
    /// requests carry a cancel token; a binary-format statement is not interrupted (it ends at the
    /// server's statement timeout).
    /// </summary>
    public override void Cancel()
    {
        var sequence = _cancelSequence;
        var source = _connection?.Source;
        if (sequence == 0 || source is null)
        {
            return;
        }

        try
        {
            _ = RasterizedDbConnection.SendCancel(source, $"c{_cancelConnector}-{sequence}", async: false, default).GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            // Best effort, as DbCommand.Cancel is.
        }
    }

    protected override void Dispose(bool disposing)
    {
        _connection = null;
        base.Dispose(disposing);
    }
}
