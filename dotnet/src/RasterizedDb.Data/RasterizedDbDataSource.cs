using System.Collections.Concurrent;
using System.Data.Common;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// Everything connections to one server share: the parsed connection string and the pool of open,
/// logged-in connections. Create one per connection string and keep it for the life of the
/// application; a <see cref="RasterizedDbConnection"/> made from a connection string alone uses a
/// shared one for that string.
/// </summary>
public sealed class RasterizedDbDataSource : DbDataSource
{
    private static readonly ConcurrentDictionary<string, RasterizedDbDataSource> Shared = new(StringComparer.Ordinal);

    internal readonly ConnectionSettings Settings;

    private readonly object _gate = new();
    private readonly Connector?[] _idle;
    private int _idleCount;
    private readonly SemaphoreSlim _slots;
    private Timer? _timer;
    private int _maintaining;
    private bool _disposed;

    private const int ExecUnknown = 0;
    private const int ExecYes = 1;
    private const int ExecNo = 2;

    /// <summary>
    /// Whether the server takes a statement and its values apart (<c>\x01EXEC;</c>): unknown until the
    /// first statement with values asks it.
    /// </summary>
    private volatile int _exec;

    public RasterizedDbDataSource(string connectionString)
    {
        Settings = new ConnectionSettings(connectionString ?? throw new ArgumentNullException(nameof(connectionString)));
        _idle = new Connector?[Settings.MaxPoolSize];
        _slots = new SemaphoreSlim(Settings.MaxPoolSize, Settings.MaxPoolSize);
    }

    public static RasterizedDbDataSource Create(string connectionString) => new(connectionString);

    /// <summary>The data source every connection made from this exact connection string shares.</summary>
    internal static RasterizedDbDataSource ForConnectionString(string connectionString) =>
        Shared.TryGetValue(connectionString, out var source) ? source : Shared.GetOrAdd(connectionString, static s => new RasterizedDbDataSource(s));

    /// <summary>The connection string, without its password.</summary>
    public override string ConnectionString => Settings.PublicConnectionString;

    protected override DbConnection CreateDbConnection() => new RasterizedDbConnection(this);

    /// <summary>
    /// A command with no connection: it runs on a connection every such command of this data source
    /// shares, many statements in flight at once (<c>rastcp/2</c>). The cheapest way to run statements
    /// that each commit on their own; one that needs a transaction, a temporary table or a sequence's
    /// <c>currval</c> takes a <see cref="RasterizedDbConnection"/>.
    /// </summary>
    public new RasterizedDbCommand CreateCommand(string? commandText = null) => new(commandText ?? string.Empty) { DataSource = this };

    protected override DbCommand CreateDbCommand(string? commandText = null) => CreateCommand(commandText);

    private MuxConnector? _mux;
    private readonly SemaphoreSlim _muxGate = new(1, 1);

    private bool Usable(MuxConnector mux) =>
        !mux.Broken && (Settings.IdleReconnectMs == 0 || mux.InFlight > 0 || Environment.TickCount64 - mux.LastUsed < Settings.IdleReconnectMs);

    /// <summary>
    /// The shared connection, opened on first use and reopened when it has failed or gone quiet (the
    /// server closes one idle for 30 s). One with a statement in flight is never reopened under it.
    /// </summary>
    internal ValueTask<MuxConnector> Mux(CancellationToken cancellation)
    {
        var mux = _mux;
        return mux is not null && Usable(mux) ? new ValueTask<MuxConnector>(mux) : OpenMux(cancellation);
    }

    private async ValueTask<MuxConnector> OpenMux(CancellationToken cancellation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _muxGate.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            var mux = _mux;
            if (mux is not null && Usable(mux))
            {
                return mux;
            }

            var fresh = await MuxConnector.Open(Settings, cancellation).ConfigureAwait(false);
            _mux = fresh;
            // Callers that sent on the old one in the instant before have their answers failed, which
            // is what a connection the server was closing would have given them.
            mux?.Dispose();
            return fresh;
        }
        finally
        {
            _muxGate.Release();
        }
    }

    public new RasterizedDbConnection CreateConnection() => new(this);

    public new RasterizedDbConnection OpenConnection()
    {
        var connection = CreateConnection();
        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    public new async ValueTask<RasterizedDbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a transaction: commits when it returns, rolls back when it
    /// throws, and after a serialization failure or a deadlock (<see cref="DbException.IsTransient"/>)
    /// runs it again in a new transaction, up to <paramref name="retries"/> times. The body may therefore
    /// run more than once: keep what it does outside the database to after it returns. A COMMIT whose
    /// connection failed is never retried: whether it committed is unknown.
    /// </summary>
    public async Task<T> ExecuteInTransactionAsync<T>(
        Func<RasterizedDbConnection, RasterizedDbTransaction, CancellationToken, Task<T>> body,
        System.Data.IsolationLevel isolationLevel = System.Data.IsolationLevel.Unspecified,
        int retries = 2,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        while (true)
        {
            RasterizedDbException failure;
            await using (var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                var transaction = (RasterizedDbTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
                try
                {
                    var value = await body(connection, transaction, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return value;
                }
                catch (RasterizedDbException e) when (e.IsTransient && attempt < retries)
                {
                    failure = e;
                }
                finally
                {
                    await transaction.DisposeAsync().ConfigureAwait(false);
                }
            }

            _ = failure;
            attempt++;
            var basis = 5 << Math.Min(attempt, 6);
            await Task.Delay(basis + Random.Shared.Next(basis + 1), cancellationToken).ConfigureAwait(false);
        }
    }

    public Task ExecuteInTransactionAsync(
        Func<RasterizedDbConnection, RasterizedDbTransaction, CancellationToken, Task> body,
        System.Data.IsolationLevel isolationLevel = System.Data.IsolationLevel.Unspecified,
        int retries = 2,
        CancellationToken cancellationToken = default) =>
        ExecuteInTransactionAsync<object?>(
            async (connection, transaction, token) =>
            {
                await body(connection, transaction, token).ConfigureAwait(false);
                return null;
            },
            isolationLevel,
            retries,
            cancellationToken);

    // ---------------------------------------------------------------- EXEC form

    internal bool? ExecSupported => _exec switch { ExecYes => true, ExecNo => false, _ => null };

    internal void SetExecSupported(bool supported) => _exec = supported ? ExecYes : ExecNo;

    // 0 unknown, 1 yes, 2 no: whether the server knows cancelable requests (\x01CANCELABLE:<token>;).
    private volatile int _cancelable;

    internal bool? CancelableSupported => _cancelable switch { 1 => true, 2 => false, _ => null };

    internal void SetCancelableSupported(bool supported) => _cancelable = supported ? 1 : 2;

    // ----------------------------------------------------------------- describe

    private readonly ConcurrentDictionary<ParsedSql, string[]> _columnNames = new();
    private volatile bool _describeUnsupported;

    /// <summary>
    /// The names of a statement's result columns, for a binary-format answer, which carries none. Asked
    /// of the server once per statement text (<c>\x01DESCRIBE;</c>, which runs nothing) and kept. Empty
    /// when the server does not know the request, or cannot describe the statement.
    /// </summary>
    internal string[] ColumnNames(ParsedSql parsed, RasterizedDbConnection? connection)
    {
        if (_describeUnsupported || parsed.Utf8.Length == 0)
        {
            return [];
        }

        if (_columnNames.TryGetValue(parsed, out var known))
        {
            return known;
        }

        string[] names = [];
        try
        {
            Response response;
            if (connection is { State: System.Data.ConnectionState.Open })
            {
                var connector = connection.Ready(false, default).GetAwaiter().GetResult();
                connector.BeginRequest();
                connector.Writer.Write("\u0001DESCRIBE;"u8);
                connector.Writer.Write(parsed.Utf8);
                response = connector.Exchange(false, 30, default).GetAwaiter().GetResult();
            }
            else
            {
                var mux = Mux(default).AsTask().GetAwaiter().GetResult();
                using var writer = new ByteWriter(parsed.Utf8.Length + 32);
                writer.GetSpan(MuxConnector.Header);
                writer.Advance(MuxConnector.Header);
                writer.Write("\u0001DESCRIBE;"u8);
                writer.Write(parsed.Utf8);
                response = mux.Exchange(writer, 30, default).AsTask().GetAwaiter().GetResult();
            }

            try
            {
                var answer = response.Span;
                if (answer.Length == 0 || answer[0] != Answer.Simple)
                {
                    // A server before marked requests refuses the text as two statements.
                    _describeUnsupported = true;
                    return [];
                }

                names = SimpleAnswer.ColumnNames(answer[1..], out var error);
                if (error is { SqlState: "0A000" })
                {
                    _describeUnsupported = true;
                    return [];
                }
            }
            finally
            {
                response.Return();
            }
        }
        catch (RasterizedDbException)
        {
            // Names are a convenience here; the rows themselves have been read.
            return [];
        }

        if (_columnNames.Count < 4096)
        {
            _columnNames.TryAdd(parsed, names);
        }

        return names;
    }

    // --------------------------------------------------------------------- pool

    /// <summary>
    /// Whether a pooled connection can still be used: not failed, and not so quiet that the server may
    /// be closing it (it closes one idle for 30 s).
    /// </summary>
    private bool Usable(Connector connector) =>
        !connector.Broken && !connector.InBlock && (Settings.IdleReconnectMs == 0 || connector.IdleMs < Settings.IdleReconnectMs);

    internal async ValueTask<Connector> Rent(bool async, CancellationToken cancellation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!Settings.Pooling)
        {
            return await Connector.Open(Settings, async, cancellation).ConfigureAwait(false);
        }

        if (!_slots.Wait(0, CancellationToken.None))
        {
            var entered = async
                ? await _slots.WaitAsync(Settings.ConnectTimeoutMs, cancellation).ConfigureAwait(false)
                : _slots.Wait(Settings.ConnectTimeoutMs, cancellation);
            if (!entered)
            {
                throw new RasterizedDbException(
                    RasterizedDbErrorKind.Timeout,
                    $"the pool's {Settings.MaxPoolSize} connections are all in use and none was returned within the connect timeout (Maximum Pool Size)");
            }
        }

        try
        {
            while (true)
            {
                Connector? connector = null;
                lock (_gate)
                {
                    if (_idleCount > 0)
                    {
                        // The most recently used: the one least likely to have gone quiet.
                        connector = _idle[--_idleCount];
                        _idle[_idleCount] = null;
                    }
                }

                if (connector is null)
                {
                    break;
                }

                if (Usable(connector))
                {
                    return connector;
                }

                connector.Dispose();
            }

            return await Connector.Open(Settings, async, cancellation).ConfigureAwait(false);
        }
        catch
        {
            _slots.Release();
            throw;
        }
    }

    internal void Return(Connector connector)
    {
        connector.NoticeSink = null;
        if (!Settings.Pooling)
        {
            connector.Dispose();
            return;
        }

        var keep = !_disposed && Usable(connector);
        if (keep)
        {
            connector.Returned = Environment.TickCount64;
            lock (_gate)
            {
                if (_idleCount < _idle.Length)
                {
                    _idle[_idleCount++] = connector;
                }
                else
                {
                    keep = false;
                }

                _timer ??= new Timer(static state => ((RasterizedDbDataSource)state!).Maintain(), this, 5000, 5000);
            }
        }

        if (!keep)
        {
            connector.Dispose();
        }

        _slots.Release();
    }

    /// <summary>
    /// Every few seconds: closes pooled connections nobody has asked for in a long while, and pings the
    /// others so the server does not close them for being quiet.
    /// </summary>
    private void Maintain()
    {
        if (Interlocked.Exchange(ref _maintaining, 1) != 0)
        {
            return;
        }

        try
        {
            var now = Environment.TickCount64;
            List<Connector>? ping = null;
            List<Connector>? close = null;
            lock (_gate)
            {
                var kept = 0;
                for (var i = 0; i < _idleCount; i++)
                {
                    var connector = _idle[i]!;
                    if (connector.Broken || (now - connector.Returned >= Settings.IdleLifetimeMs && kept >= Settings.MinPoolSize))
                    {
                        (close ??= []).Add(connector);
                    }
                    else if (Settings.KeepaliveMs > 0 && connector.IdleMs >= Settings.KeepaliveMs)
                    {
                        // Out of the pool while it is pinged: nobody may rent it mid-exchange.
                        (ping ??= []).Add(connector);
                    }
                    else
                    {
                        _idle[kept++] = connector;
                    }
                }

                Array.Clear(_idle, kept, _idleCount - kept);
                _idleCount = kept;
            }

            if (close is not null)
            {
                foreach (var connector in close)
                {
                    connector.Dispose();
                }
            }

            if (ping is not null)
            {
                _ = PingAll(ping);
            }
            else
            {
                Volatile.Write(ref _maintaining, 0);
            }
        }
        catch (Exception)
        {
            Volatile.Write(ref _maintaining, 0);
        }
    }

    private async Task PingAll(List<Connector> connectors)
    {
        try
        {
            foreach (var connector in connectors)
            {
                var returned = connector.Returned;
                var alive = await connector.Ping(async: true).ConfigureAwait(false);
                var back = false;
                if (alive && !_disposed)
                {
                    connector.Returned = returned;
                    lock (_gate)
                    {
                        if (_idleCount < _idle.Length)
                        {
                            _idle[_idleCount++] = connector;
                            back = true;
                        }
                    }
                }

                if (!back)
                {
                    connector.Dispose();
                }
            }
        }
        finally
        {
            Volatile.Write(ref _maintaining, 0);
        }
    }

    /// <summary>Closes every pooled connection. Connections in use are closed when they are returned.</summary>
    public void Clear()
    {
        Connector?[] taken;
        lock (_gate)
        {
            taken = _idle.AsSpan(0, _idleCount).ToArray();
            Array.Clear(_idle, 0, _idleCount);
            _idleCount = 0;
        }

        foreach (var connector in taken)
        {
            connector?.Dispose();
        }
    }

    /// <summary>Closes the pooled connections of every data source made from a connection string alone.</summary>
    public static void ClearAllPools()
    {
        foreach (var source in Shared.Values)
        {
            source.Clear();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            lock (_gate)
            {
                _timer?.Dispose();
                _timer = null;
            }

            Clear();
            Interlocked.Exchange(ref _mux, null)?.Dispose();
        }
    }

    protected override ValueTask DisposeAsyncCore()
    {
        Dispose(true);
        return default;
    }
}
