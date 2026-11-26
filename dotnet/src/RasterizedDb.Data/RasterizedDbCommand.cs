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

    // The request in flight, for Cancel: 0 when none is, or when it carries no cancel token.
    private long _cancelConnector;
    private long _cancelSequence;

    /// <summary>
    /// Part of every cancel token of this process. A token is unique among a user's running
    /// statements, and connection numbers alone repeat from one process to the next.
    /// </summary>
    private static readonly ulong ProcessTag = BitConverter.ToUInt64(System.Security.Cryptography.RandomNumberGenerator.GetBytes(8));

    /// <summary>How long a cancelled statement is given to answer before its connection is closed instead.</summary>
    private const int CancelGraceMs = 5000;

    /// <summary>
    /// Whether a binary-format statement is sent so that <see cref="Cancel"/> can stop it. Statements
    /// run with a <see cref="CancellationToken"/> that can be cancelled, and every text-format
    /// statement, already are; set this for a synchronous call another thread may want to cancel.
    /// Needs a server that knows cancelable requests; against an older one it changes nothing.
    /// </summary>
    public bool Cancelable { get; set; }

    private static string CancelToken(long connector, long sequence) => $"c{ProcessTag:x}-{connector}-{sequence}";

    /// <summary>Writes the token of the request being built, and remembers it for <see cref="Cancel"/>.</summary>
    private void WriteCancelToken(ByteWriter writer, Connector connector)
    {
        _cancelConnector = connector.Id;
        var sequence = connector.NextCancelSequence();
        Volatile.Write(ref _cancelSequence, sequence);
        writer.WriteByte((byte)'c');
        writer.WriteFormatted(ProcessTag, "x");
        writer.WriteByte((byte)'-');
        writer.WriteInt64Text(_cancelConnector);
        writer.WriteByte((byte)'-');
        writer.WriteInt64Text(sequence);
    }

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
        get => _timeout ?? _connection?.DefaultCommandTimeout ?? DataSource?.Settings.CommandTimeoutSeconds ?? 30;
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
    /// <summary>The data source a command with no connection runs on (<see cref="RasterizedDbDataSource.CreateCommand"/>).</summary>
    internal RasterizedDbDataSource? DataSource;

    internal ReadOnlyMemory<byte> RawStatement;

    /// <summary>Whether <see cref="RawStatement"/> is a simple query (several statements, text answer) and not one native statement.</summary>
    internal bool RawIsText;

    // ------------------------------------------------------------------ request

    private RasterizedDbParameter ParameterFor(ParsedSql parsed, int index) => ParameterFor(parsed, index, _parameters?.Items);

    internal static RasterizedDbParameter ParameterFor(ParsedSql parsed, int index, List<RasterizedDbParameter>? items)
    {
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
    private void WriteRendered(ByteWriter writer, ParsedSql parsed) => WriteRendered(writer, parsed, _parameters?.Items);

    internal static void WriteRendered(ByteWriter writer, ParsedSql parsed, List<RasterizedDbParameter>? items)
    {
        var sql = parsed.Utf8.AsSpan();
        var copied = 0;
        foreach (ref readonly var span in parsed.Spans.AsSpan())
        {
            writer.Write(sql[copied..span.Start]);
            ParameterFor(parsed, span.Parameter, items).WriteLiteral(writer);
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

    /// <summary>
    /// Writes the request after the <paramref name="header"/> bytes its frame starts with: the simple-query
    /// form for the text format; for the binary one the statement alone, the EXEC form when the server
    /// knows it (<paramref name="exec"/>), or the statement with its values written in.
    /// </summary>
    private void BuildInto(ByteWriter writer, int header, bool? exec, ParsedSql parsed, bool text, Connector connector, bool cancelable = false)
    {
        writer.Clear();
        writer.GetSpan(header);
        writer.Advance(header);
        if (text)
        {
            // \x01SIMPLE:cancel=<token>;<statements>
            writer.Write("\u0001SIMPLE:cancel="u8);
            WriteCancelToken(writer, connector);
            writer.WriteByte((byte)';');
            if (RawStatement.IsEmpty)
            {
                WriteRendered(writer, parsed);
            }
            else
            {
                writer.Write(RawStatement.Span);
            }

            return;
        }

        if (parsed.MultipleStatements && RawStatement.IsEmpty)
        {
            throw SecondStatement(parsed);
        }

        if (cancelable)
        {
            // \x01CANCELABLE:<token>;<the request as it would have been alone>
            writer.Write("\u0001CANCELABLE:"u8);
            WriteCancelToken(writer, connector);
            writer.WriteByte((byte)';');
        }

        var start = writer.Length;
        if (!RawStatement.IsEmpty)
        {
            writer.Write(RawStatement.Span);
            return;
        }

        if (parsed.Spans.Length == 0)
        {
            writer.Write(parsed.Utf8);
            return;
        }

        if (exec == true)
        {
            if (TryWriteExec(writer, parsed))
            {
                return;
            }

            // Past what an EXEC request may render to: sent as text, after the same prefix.
            writer.Clear();
            writer.Advance(start);
        }

        WriteRendered(writer, parsed);
    }

    /// <summary>Whether this statement's request depends on knowing if the server takes the EXEC form.</summary>
    private bool NeedsExecAnswer(ParsedSql parsed, bool text) => !text && RawStatement.IsEmpty && parsed.Spans.Length > 0 && !parsed.MultipleStatements;

    private static void WriteProbe(ByteWriter writer, int header)
    {
        writer.Clear();
        writer.GetSpan(header);
        writer.Advance(header);
        writer.Write("\u0001EXEC;"u8);
        writer.WriteUInt32(8);
        writer.Write("SELECT 1"u8);
        writer.WriteUInt16(0);
        writer.WriteUInt16(0);
    }

    /// <summary>Builds the request; returns whether it carries a cancel token.</summary>
    private async ValueTask<bool> Build(Connector connector, RasterizedDbDataSource source, ParsedSql parsed, bool text, bool async, CancellationToken cancellation)
    {
        var exec = source.ExecSupported;
        if (exec is null && !connector.InBlock && NeedsExecAnswer(parsed, text))
        {
            exec = await Probe(connector, source, async, cancellation).ConfigureAwait(false);
        }

        var cancelable = false;
        if (!text && (Cancelable || cancellation.CanBeCanceled))
        {
            // Asked once per data source, and not inside a block, which a failed probe would not tell
            // from an old server: there the statement goes uncancelable, as it did before.
            var known = source.CancelableSupported;
            if (known is null && !connector.InBlock)
            {
                WriteCancelableProbe(connector.Writer, connector.FrameHeader);
                var response = await connector.Exchange(async, 30, cancellation).ConfigureAwait(false);
                var span = response.Span;
                var at = connector.TakeNotices(span);
                known = span.Length > at && span[at] == Answer.Rows;
                response.Return();
                source.SetCancelableSupported(known.Value);
            }

            cancelable = known == true;
        }

        BuildInto(connector.Writer, connector.FrameHeader, exec, parsed, text, connector, cancelable);
        return text || cancelable;
    }

    /// <summary>
    /// <c>SELECT 1</c> as a cancelable request: a server that knows the form answers its row, one that
    /// does not refuses the request without running anything.
    /// </summary>
    private static void WriteCancelableProbe(ByteWriter writer, int header)
    {
        writer.Clear();
        writer.GetSpan(header);
        writer.Advance(header);
        writer.Write("\u0001CANCELABLE:p"u8);
        writer.WriteFormatted(ProcessTag, "x");
        writer.WriteByte((byte)'-');
        writer.WriteInt64Text(Interlocked.Increment(ref _probeSequence));
        writer.Write(";SELECT 1"u8);
    }

    private static long _probeSequence;

    /// <summary>
    /// The caller's token was cancelled while the statement runs on a connection of its own: the server
    /// is asked to stop it, again while it has not answered (the first ask may arrive before the
    /// statement has started), and the connection is closed only if it never does.
    /// </summary>
    private async Task StopOnServer(RasterizedDbDataSource source, Connector connector, long sequence)
    {
        var token = CancelToken(connector.Id, sequence);
        var started = Environment.TickCount64;
        var wait = 20;
        try
        {
            while (Volatile.Read(ref _cancelSequence) == sequence && Environment.TickCount64 - started < CancelGraceMs)
            {
                await RasterizedDbConnection.SendCancel(source, token, async: true, default).ConfigureAwait(false);
                await Task.Delay(wait).ConfigureAwait(false);
                wait = Math.Min(wait * 2, 500);
            }
        }
        catch (Exception)
        {
            // The server cannot be asked: the connection is closed below, as before cancel tokens.
        }

        if (Volatile.Read(ref _cancelSequence) == sequence)
        {
            connector.Break();
        }
    }

    private static OperationCanceledException Stopped(Exception inner, CancellationToken cancellation) =>
        new("the statement was cancelled: the server stopped it, and its connection goes on", inner, cancellation);

    /// <summary>
    /// Asks the server once whether it knows the EXEC form, with <c>SELECT 1</c>: a server without it
    /// refuses the request without running anything, and statements are then rendered as text.
    /// </summary>
    private static async ValueTask<bool> Probe(Connector connector, RasterizedDbDataSource source, bool async, CancellationToken cancellation)
    {
        WriteProbe(connector.Writer, connector.FrameHeader);
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
        if (_connection is null && DataSource is { } shared)
        {
            return await RunShared(shared, mode, behavior, cancellation).ConfigureAwait(false);
        }

        var connection = RequireConnection();
        var connector = await connection.Ready(async, cancellation).ConfigureAwait(false);
        var source = connection.Source!;
        var raw = !RawStatement.IsEmpty;
        var parsed = raw ? ParsedSql.Raw : Parsed();
        var format = ResultFormat == RasterizedDbResultFormat.Default ? connection.DefaultResultFormat : ResultFormat;
        var text = raw ? RawIsText : format == RasterizedDbResultFormat.Text;
        var wasInBlock = connector.InBlock;

        bool stoppable;
        try
        {
            stoppable = await Build(connector, source, parsed, text, async, cancellation).ConfigureAwait(false) && cancellation.CanBeCanceled;
        }
        catch
        {
            connector.Writer.Reset();
            throw;
        }

        Response response;
        // A statement that carries a cancel token is not abandoned when the caller's token is
        // cancelled: the server is asked to stop it, and its answer (57014) is waited for, so the
        // connection, and the transaction it may hold, go on.
        CancellationTokenRegistration stop = default;
        if (stoppable)
        {
            _stopSource = source;
            _stopConnector = connector;
            stop = cancellation.UnsafeRegister(
                static state =>
                {
                    var command = (RasterizedDbCommand)state!;
                    var sequence = command.InFlight();
                    if (sequence != 0 && command._stopSource is { } source && command._stopConnector is { } connector)
                    {
                        _ = Task.Run(() => command.StopOnServer(source, connector, sequence));
                    }
                },
                this);
        }

        try
        {
            response = await connector.Exchange(async, CommandTimeout, stoppable ? default : cancellation).ConfigureAwait(false);
        }
        catch (RasterizedDbException e) when (stoppable && !wasInBlock && cancellation.IsCancellationRequested && e.Kind == RasterizedDbErrorKind.Transport)
        {
            // The server never answered the cancel, and the connection was closed instead.
            throw new OperationCanceledException("the statement was cancelled; its connection was closed, and whether it ran is unknown", e, cancellation);
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
            Volatile.Write(ref _cancelSequence, 0);
            if (stoppable)
            {
                stop.Dispose();
                _stopSource = null;
                _stopConnector = null;
            }
        }

        try
        {
            return text
                ? ReadText(connection, connector, ref response, mode, behavior)
                : ReadBinary(connection, connector, parsed, ref response, mode, behavior);
        }
        catch (RasterizedDbException e) when (stoppable && e.SqlState == "57014" && cancellation.IsCancellationRequested)
        {
            response.Return();
            throw Stopped(e, cancellation);
        }
        catch
        {
            response.Return();
            throw;
        }
    }

    private long InFlight() => Volatile.Read(ref _cancelSequence);

    private RasterizedDbDataSource? _stopSource;
    private Connector? _stopConnector;

    /// <summary>
    /// A command with no connection of its own: sent on the data source's shared multiplexed
    /// connection, where it is in flight beside every other caller's. It commits on its own;
    /// transaction control is refused there, and a transaction takes a <see cref="RasterizedDbConnection"/>.
    /// </summary>
    private async ValueTask<Outcome> RunShared(RasterizedDbDataSource source, Mode mode, CommandBehavior behavior, CancellationToken cancellation)
    {
        var raw = !RawStatement.IsEmpty;
        if (_text.Length == 0 && !raw)
        {
            throw new InvalidOperationException("the command has no text");
        }

        var parsed = raw ? ParsedSql.Raw : Parsed();
        var format = ResultFormat == RasterizedDbResultFormat.Default ? source.Settings.ResultFormat : ResultFormat;
        var text = raw ? RawIsText : format == RasterizedDbResultFormat.Text;
        if (parsed.Verb != TransactionVerb.None)
        {
            throw new RasterizedDbException(
                RasterizedDbErrorKind.Unsupported,
                "transaction control needs a connection of its own: a data source's commands share one multiplexed connection. " +
                "Open a RasterizedDbConnection and use BeginTransaction.");
        }

        var mux = await source.Mux(cancellation).ConfigureAwait(false);
        var timeout = _timeout ?? source.Settings.CommandTimeoutSeconds;
        Response response;
        var writer = new ByteWriter(512);
        try
        {
            var exec = source.ExecSupported;
            if (exec is null && NeedsExecAnswer(parsed, text))
            {
                WriteProbe(writer, MuxConnector.Header);
                var probe = await mux.Exchange(writer, 30, cancellation).ConfigureAwait(false);
                var answer = probe.Span;
                var at = mux.Connector.TakeNotices(answer);
                exec = answer.Length > at && answer[at] == Answer.Rows;
                probe.Return();
                source.SetExecSupported(exec.Value);
            }

            var cancelable = false;
            if (!text && (Cancelable || cancellation.CanBeCanceled))
            {
                var known = source.CancelableSupported;
                if (known is null)
                {
                    WriteCancelableProbe(writer, MuxConnector.Header);
                    var probe = await mux.Exchange(writer, 30, cancellation).ConfigureAwait(false);
                    var answer = probe.Span;
                    var at = mux.Connector.TakeNotices(answer);
                    known = answer.Length > at && answer[at] == Answer.Rows;
                    probe.Return();
                    source.SetCancelableSupported(known.Value);
                }

                cancelable = known == true;
            }

            BuildInto(writer, MuxConnector.Header, exec, parsed, text, mux.Connector, cancelable);
            try
            {
                response = await mux.Exchange(writer, timeout, cancellation).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when ((text || cancelable) && cancellation.IsCancellationRequested)
            {
                // The caller stops waiting at once (the shared connection goes on without it); the
                // server is told too, so the statement does not run on for nobody.
                var token = CancelToken(_cancelConnector, Volatile.Read(ref _cancelSequence));
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await RasterizedDbConnection.SendCancel(source, token, async: true, default).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Best effort: the statement then ends at the server's statement timeout.
                    }
                });
                throw;
            }
        }
        finally
        {
            writer.Dispose();
            Volatile.Write(ref _cancelSequence, 0);
        }

        try
        {
            return text
                ? ReadText(null, mux.Connector, ref response, mode, behavior)
                : ReadBinary(null, mux.Connector, parsed, ref response, mode, behavior, source);
        }
        catch
        {
            response.Return();
            throw;
        }
    }

    private static RasterizedDbDataReader NewReader(RasterizedDbConnection? connection) => connection?.TakeReader() ?? new RasterizedDbDataReader();

    private static Outcome ReadBinary(RasterizedDbConnection? connection, Connector connector, ParsedSql parsed, ref Response response, Mode mode, CommandBehavior behavior, RasterizedDbDataSource? source = null)
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
                var reader = NewReader(connection);
                reader.InitBinary(connection, behavior, response, at + 1, -1, parsed, source);
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
            var reader = NewReader(connection);
            reader.InitEmpty(connection, behavior, outcome.Affected);
            outcome.Reader = reader;
        }

        return outcome;
    }

    private static Outcome ReadText(RasterizedDbConnection? connection, Connector connector, ref Response response, Mode mode, CommandBehavior behavior)
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

        var reader = NewReader(connection);
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

    /// <summary>
    /// The synchronous calls. On a connection every step under them is synchronous, so the result is
    /// there when the call returns; a data source's command shares a connection other callers are
    /// reading, so there the call waits for its answer.
    /// </summary>
    private Outcome RunSync(Mode mode, CommandBehavior behavior)
    {
        var run = Run(false, mode, behavior, default);
        return run.IsCompleted ? run.GetAwaiter().GetResult() : run.AsTask().GetAwaiter().GetResult();
    }

    public override int ExecuteNonQuery() => Clamp(RunSync(Mode.NonQuery, CommandBehavior.Default).Affected);

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
        Clamp((await Run(true, Mode.NonQuery, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).Affected);

    public override object? ExecuteScalar() => RunSync(Mode.Scalar, CommandBehavior.Default).Scalar;

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) =>
        (await Run(true, Mode.Scalar, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).Scalar;

    public new RasterizedDbDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

    public new RasterizedDbDataReader ExecuteReader(CommandBehavior behavior) => RunSync(Mode.Reader, behavior).Reader!;

    public new Task<RasterizedDbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    public new async Task<RasterizedDbDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default) =>
        (await Run(true, Mode.Reader, behavior, cancellationToken).ConfigureAwait(false)).Reader!;

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        await ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Asks the server to stop the statement this command is running, from another thread; it then
    /// fails with SQLSTATE 57014 and its connection goes on. Works for a statement that carries a
    /// cancel token: every text-format one, and a binary-format one run with a cancellable
    /// <see cref="CancellationToken"/> or with <see cref="Cancelable"/> set (against a server that
    /// knows cancelable requests). Any other is not interrupted: it ends at the server's statement
    /// timeout.
    /// </summary>
    public override void Cancel()
    {
        var sequence = Volatile.Read(ref _cancelSequence);
        var source = _connection?.Source ?? DataSource;
        if (sequence == 0 || source is null)
        {
            return;
        }

        try
        {
            _ = RasterizedDbConnection.SendCancel(source, CancelToken(_cancelConnector, sequence), async: false, default).GetAwaiter().GetResult();
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
