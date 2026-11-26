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
        }
    }

    protected override ValueTask DisposeAsyncCore()
    {
        Dispose(true);
        return default;
    }
}
