using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// A connection to a RasterizedDB server's native protocol (rastcp): TLS 1.3 over TCP, or a unix
/// socket. Opening takes a logged-in connection from the pool; closing gives it back.
/// </summary>
/// <remarks>
/// <para>
/// The server closes a native connection that has been quiet for 30 s. An open connection that has been
/// quiet for <c>Idle Reconnect</c> seconds (20) is therefore swapped for a fresh one before the next
/// statement, unless a transaction is open on it: nothing is ever sent on a connection the server may
/// be closing, and nothing is ever sent twice. What lived on the old connection (temporary tables,
/// sequence <c>currval</c>) does not carry over. A transaction left quiet for 30 s is lost, and says so
/// (<see cref="RasterizedDbErrorKind.TransactionLost"/>).
/// </para>
/// <para>
/// A statement whose connection fails throws, and is not retried: whether it ran is unknown.
/// </para>
/// </remarks>
public sealed class RasterizedDbConnection : DbConnection
{
    private string _connectionString = string.Empty;
    private RasterizedDbDataSource? _source;
    private readonly bool _ownsSource;
    private Connector? _connector;
    private ConnectionState _state = ConnectionState.Closed;
    private RasterizedDbTransaction? _transaction;
    private RasterizedDbDataReader? _cachedReader;
    private RasterizedDbCommand? _internal;
    private string? _serverVersion;
    internal RasterizedDbCommand? KvCommand;
    internal ByteWriter? KvScratch;
    private readonly Action<RasterizedDbNotice> _noticeSink;

    public RasterizedDbConnection()
    {
        _noticeSink = OnNotice;
    }

    public RasterizedDbConnection(string? connectionString)
        : this()
    {
        _connectionString = connectionString ?? string.Empty;
    }

    internal RasterizedDbConnection(RasterizedDbDataSource source)
        : this()
    {
        _source = source;
        _ownsSource = true;
        _connectionString = source.Settings.PublicConnectionString;
    }

    /// <summary>Raised for every notice the server sends with this connection's answers (<c>RAISE NOTICE</c>, <c>DROP TABLE IF EXISTS</c> naming no table, …).</summary>
    public event EventHandler<RasterizedDbNoticeEventArgs>? Notice;

    private void OnNotice(RasterizedDbNotice notice) => Notice?.Invoke(this, new RasterizedDbNoticeEventArgs(notice));

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_state != ConnectionState.Closed)
            {
                throw new InvalidOperationException("the connection string cannot change while the connection is open");
            }

            if (_ownsSource)
            {
                throw new InvalidOperationException("this connection belongs to a data source, whose connection string is fixed");
            }

            _connectionString = value ?? string.Empty;
            _source = null;
        }
    }

    internal RasterizedDbDataSource? Source => _source;

    internal int DefaultCommandTimeout => (_source?.Settings ?? Settings()).CommandTimeoutSeconds;

    internal RasterizedDbResultFormat DefaultResultFormat => _source!.Settings.ResultFormat;

    private ConnectionSettings Settings() => (_source ??= RasterizedDbDataSource.ForConnectionString(_connectionString)).Settings;

    public override string Database => Settings().Database ?? string.Empty;

    public override string DataSource => Settings().DataSourceName;

    public override ConnectionState State => _state;

    public override int ConnectionTimeout => Settings().ConnectTimeoutMs == Timeout.Infinite ? 0 : Settings().ConnectTimeoutMs / 1000;

    public override string ServerVersion
    {
        get
        {
            if (_serverVersion is null)
            {
                RequireOpen();
                try
                {
                    using var command = new RasterizedDbCommand("SHOW server_version", this) { ResultFormat = RasterizedDbResultFormat.Text };
                    _serverVersion = Convert.ToString(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
                }
                catch (RasterizedDbException)
                {
                    _serverVersion = string.Empty;
                }
            }

            return _serverVersion;
        }
    }

    public override bool CanCreateBatch => true;

    protected override DbBatch CreateDbBatch() => new RasterizedDbBatch { Connection = this };

    public new RasterizedDbBatch CreateBatch() => new() { Connection = this };

    protected override DbProviderFactory DbProviderFactory => RasterizedDbFactory.Instance;

    // ------------------------------------------------------------- open / close

    public override void Open() => OpenCore(false, default).GetAwaiter().GetResult();

    public override Task OpenAsync(CancellationToken cancellationToken) => OpenCore(true, cancellationToken).AsTask();

    private async ValueTask OpenCore(bool async, CancellationToken cancellation)
    {
        if (_state != ConnectionState.Closed)
        {
            throw new InvalidOperationException("the connection is already open");
        }

        var source = _source ??= RasterizedDbDataSource.ForConnectionString(_connectionString);
        _state = ConnectionState.Connecting;
        try
        {
            var connector = await source.Rent(async, cancellation).ConfigureAwait(false);
            connector.NoticeSink = _noticeSink;
            _connector = connector;
        }
        catch
        {
            _state = ConnectionState.Closed;
            throw;
        }

        _state = ConnectionState.Open;
        OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
    }

    public override void Close() => CloseCore(false).GetAwaiter().GetResult();

    public override Task CloseAsync() => CloseCore(true).AsTask();

    private async ValueTask CloseCore(bool async)
    {
        if (_state == ConnectionState.Closed)
        {
            return;
        }

        var connector = _connector;
        _connector = null;
        var transaction = _transaction;
        _transaction = null;
        transaction?.Forget();
        if (_cachedReader is { IsClosed: false } reader)
        {
            reader.Close();
        }

        if (connector is not null)
        {
            if (connector is { InBlock: true, Broken: false })
            {
                // A pooled connection must go back with no block open: the server would run the next
                // caller's statements inside it.
                try
                {
                    connector.BeginRequest();
                    connector.Writer.Write("ROLLBACK"u8);
                    var response = await connector.Exchange(async, 10, default).ConfigureAwait(false);
                    var answer = response.Span;
                    var at = connector.TakeNotices(answer);
                    var ok = answer.Length > at && answer[at] != Answer.Error;
                    response.Return();
                    connector.InBlock = false;
                    connector.BlockFailed = false;
                    if (!ok)
                    {
                        connector.Break();
                    }
                }
                catch (Exception)
                {
                    connector.Break();
                }
            }

            _source!.Return(connector);
        }

        _state = ConnectionState.Closed;
        OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseCore(true).ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void RequireOpen()
    {
        if (_state != ConnectionState.Open || _connector is null)
        {
            throw new InvalidOperationException("the connection is not open");
        }
    }

    /// <summary>
    /// The connection a statement is about to be sent on: the one held, or a fresh one in its place when
    /// that one has failed or gone quiet outside a transaction.
    /// </summary>
    internal ValueTask<Connector> Ready(bool async, CancellationToken cancellation)
    {
        var connector = _connector;
        if (connector is null || _state != ConnectionState.Open)
        {
            throw new InvalidOperationException("the connection is not open");
        }

        var idle = connector.IdleMs;
        if (!connector.Broken && (connector.InBlock ? idle < Connector.ServerIdleCloseMs : idle < IdleLimit(connector)))
        {
            return new ValueTask<Connector>(connector);
        }

        return Replace(connector, idle, async, cancellation);
    }

    private static long IdleLimit(Connector connector) =>
        connector.Settings.IdleReconnectMs == 0 ? long.MaxValue : connector.Settings.IdleReconnectMs;

    private async ValueTask<Connector> Replace(Connector connector, long idle, bool async, CancellationToken cancellation)
    {
        if (connector.InBlock || _transaction is not null)
        {
            var why = connector.Broken
                ? "the transaction's connection is closed"
                : $"the transaction was idle for {idle / 1000.0:F1} s, and the server closes a connection idle for {Connector.ServerIdleCloseMs / 1000} s, rolling back its transaction";
            connector.Break();
            TransactionLost();
            throw new RasterizedDbException(RasterizedDbErrorKind.TransactionLost, why);
        }

        _connector = null;
        var source = _source!;
        connector.Break();
        source.Return(connector);
        try
        {
            var fresh = await source.Rent(async, cancellation).ConfigureAwait(false);
            fresh.NoticeSink = _noticeSink;
            _connector = fresh;
            return fresh;
        }
        catch
        {
            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
            throw;
        }
    }

    /// <summary>The transaction's connection is gone, and the server has rolled it back (or will, with the connection).</summary>
    internal void TransactionLost()
    {
        var transaction = _transaction;
        _transaction = null;
        transaction?.Forget();
        if (_connector is { } connector)
        {
            connector.InBlock = false;
            connector.BlockFailed = false;
        }
    }

    internal RasterizedDbDataReader TakeReader()
    {
        var reader = _cachedReader;
        if (reader is { IsClosed: true })
        {
            return reader;
        }

        // The cached one is still being read: a second result at once is fine, each holds its own answer.
        reader = new RasterizedDbDataReader();
        _cachedReader ??= reader;
        return reader;
    }

    internal void ReaderClosed(RasterizedDbDataReader reader, bool closeConnection)
    {
        if (closeConnection)
        {
            Close();
        }
    }

    // ------------------------------------------------------------- transactions

    /// <summary>Whether a transaction block is open on the connection, as its answers have said.</summary>
    public bool InTransaction => _connector?.InBlock ?? false;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        BeginCore(isolationLevel, readOnly: false, false, default).GetAwaiter().GetResult();

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken) =>
        await BeginCore(isolationLevel, readOnly: false, true, cancellationToken).ConfigureAwait(false);

    public new RasterizedDbTransaction BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

    public new RasterizedDbTransaction BeginTransaction(IsolationLevel isolationLevel) =>
        BeginCore(isolationLevel, readOnly: false, false, default).GetAwaiter().GetResult();

    /// <summary>Opens a transaction that may be <c>READ ONLY</c>.</summary>
    public ValueTask<RasterizedDbTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, bool readOnly, CancellationToken cancellationToken = default) =>
        BeginCore(isolationLevel, readOnly, true, cancellationToken);

    private async ValueTask<RasterizedDbTransaction> BeginCore(IsolationLevel isolationLevel, bool readOnly, bool async, CancellationToken cancellation)
    {
        RequireOpen();
        if (_transaction is not null)
        {
            throw new InvalidOperationException("a transaction is already open on this connection; nest with savepoints (DbTransaction.Save)");
        }

        var level = isolationLevel switch
        {
            IsolationLevel.Unspecified => null,
            IsolationLevel.ReadUncommitted or IsolationLevel.ReadCommitted => "READ COMMITTED",
            IsolationLevel.RepeatableRead or IsolationLevel.Snapshot => "REPEATABLE READ",
            IsolationLevel.Serializable => "SERIALIZABLE",
            _ => throw new NotSupportedException($"isolation level {isolationLevel} is not one RasterizedDB has"),
        };
        var sql = (level, readOnly) switch
        {
            (null, false) => "BEGIN",
            (null, true) => "BEGIN READ ONLY",
            (_, false) => "BEGIN ISOLATION LEVEL " + level,
            _ => "BEGIN ISOLATION LEVEL " + level + " READ ONLY",
        };
        await ExecuteInternal(sql, async, cancellation).ConfigureAwait(false);
        return _transaction = new RasterizedDbTransaction(this, isolationLevel == IsolationLevel.Unspecified ? IsolationLevel.ReadCommitted : isolationLevel);
    }

    internal void TransactionEnded(RasterizedDbTransaction transaction)
    {
        if (ReferenceEquals(_transaction, transaction))
        {
            _transaction = null;
        }
    }

    /// <summary>A statement of the driver's own (BEGIN, COMMIT, a savepoint), on the native path.</summary>
    internal async ValueTask<int> ExecuteInternal(string sql, bool async, CancellationToken cancellation)
    {
        var command = _internal ??= new RasterizedDbCommand { Connection = this, ResultFormat = RasterizedDbResultFormat.Binary };
        command.Connection = this;
        command.CommandText = sql;
        return async ? await command.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false) : command.ExecuteNonQuery();
    }

    // -------------------------------------------------------------------- other

    public new RasterizedDbCommand CreateCommand() => new() { Connection = this };

    /// <summary>A command with its text.</summary>
    public RasterizedDbCommand CreateCommand(string commandText) => new(commandText, this);

    protected override DbCommand CreateDbCommand() => CreateCommand();

    /// <summary>
    /// A connection's database is fixed once it has run anything, so this closes the connection and
    /// opens one to <paramref name="databaseName"/>.
    /// </summary>
    public override void ChangeDatabase(string databaseName)
    {
        if (_ownsSource)
        {
            throw new InvalidOperationException("this connection belongs to a data source, whose database is fixed; create a data source for the other database");
        }

        var open = _state == ConnectionState.Open;
        if (open)
        {
            Close();
        }

        var builder = new RasterizedDbConnectionStringBuilder(_source?.Settings.ConnectionString ?? _connectionString) { Database = databaseName };
        _connectionString = builder.ConnectionString;
        _source = null;
        if (open)
        {
            Open();
        }
    }

    /// <summary>The server's administrative statements as typed calls: stores, placement, indexes, partitions, roles, backups, configuration.</summary>
    public RasterizedDbAdmin Admin => new(this);

    /// <summary>The server's Redis-compatible key/value cache, typed.</summary>
    public RasterizedDbKv Kv => new(this);

    /// <summary>
    /// Inserts many rows with as few statements as they fit in: multi-row <c>INSERT</c>s of up to
    /// <paramref name="rowsPerStatement"/> rows (and about a megabyte) each, written straight into the
    /// request with no SQL string or parameter objects in between. Each statement commits on its own
    /// unless a transaction is open; open one to make the whole load atomic.
    /// </summary>
    /// <returns>The number of rows the server reported inserted.</returns>
    public async Task<long> BulkInsertAsync(
        string table,
        IReadOnlyList<string> columns,
        IEnumerable<IReadOnlyList<object?>> rows,
        int rowsPerStatement = 1000,
        CancellationToken cancellationToken = default)
    {
        RequireOpen();
        if (columns.Count == 0)
        {
            throw new ArgumentException("a bulk insert needs at least one column", nameof(columns));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(rowsPerStatement, 1);
        const int StatementBytes = 1024 * 1024;
        using var statement = new ByteWriter(64 * 1024);
        var header = new StringBuilder("INSERT INTO ").Append(LiteralWriter.QuoteIdentifier(table)).Append(" (");
        for (var i = 0; i < columns.Count; i++)
        {
            header.Append(i == 0 ? "" : ", ").Append(LiteralWriter.QuoteIdentifier(columns[i]));
        }

        var prefix = Encoding.UTF8.GetBytes(header.Append(") VALUES ").ToString());
        using var command = new RasterizedDbCommand { Connection = this, ResultFormat = RasterizedDbResultFormat.Binary };
        long inserted = 0;
        var pending = 0;

        async Task Flush()
        {
            command.RawStatement = statement.WrittenMemory;
            inserted += Math.Max(0, await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false));
            statement.Clear();
            pending = 0;
        }

        foreach (var row in rows)
        {
            if (row.Count != columns.Count)
            {
                throw new ArgumentException($"a row has {row.Count} value(s), {columns.Count} column(s) were named", nameof(rows));
            }

            statement.Write(pending == 0 ? prefix : ", "u8);
            statement.WriteByte((byte)'(');
            for (var i = 0; i < row.Count; i++)
            {
                if (i > 0)
                {
                    statement.Write(", "u8);
                }

                LiteralWriter.WriteObject(statement, row[i]);
            }

            statement.WriteByte((byte)')');
            pending++;
            if (pending >= rowsPerStatement || statement.Length >= StatementBytes)
            {
                await Flush().ConfigureAwait(false);
            }
        }

        if (pending > 0)
        {
            await Flush().ConfigureAwait(false);
        }

        return inserted;
    }

    /// <summary>
    /// Stops the statement a text-format command sent with <paramref name="cancelToken"/> is running
    /// (see <see cref="RasterizedDbCommand.Cancel"/>, which does this for its own command).
    /// </summary>
    internal static async ValueTask<bool> SendCancel(RasterizedDbDataSource source, string cancelToken, bool async, CancellationToken cancellation)
    {
        // On a connection of its own: the one running the statement is waiting for its answer.
        var connector = await source.Rent(async, cancellation).ConfigureAwait(false);
        try
        {
            connector.BeginRequest();
            connector.Writer.Write("\u0001CANCEL;"u8);
            connector.Writer.WriteAscii(cancelToken);
            var response = await connector.Exchange(async, 10, cancellation).ConfigureAwait(false);
            try
            {
                var span = response.Span;
                if (span.Length == 0 || span[0] != Answer.Simple)
                {
                    throw new RasterizedDbException(RasterizedDbErrorKind.Unsupported, "this server cannot cancel a native query (it predates cancel tokens)");
                }

                var scan = SimpleAnswer.Scan(span[1..], null, null);
                if (scan.Error is { } error)
                {
                    throw new RasterizedDbException(RasterizedDbErrorKind.Server, error.Message, error.SqlState);
                }

                return scan.LastTag == "CANCEL 1";
            }
            finally
            {
                response.Return();
            }
        }
        finally
        {
            source.Return(connector);
        }
    }

    /// <summary>
    /// Describes the server's objects: <c>Tables</c> and <c>Columns</c> (from <c>information_schema</c>),
    /// besides the collections every provider lists.
    /// </summary>
    public override DataTable GetSchema() => GetSchema(DbMetaDataCollectionNames.MetaDataCollections, []);

    public override DataTable GetSchema(string collectionName) => GetSchema(collectionName, []);

    public override DataTable GetSchema(string collectionName, string?[] restrictionValues)
    {
        if (collectionName.Equals(DbMetaDataCollectionNames.MetaDataCollections, StringComparison.OrdinalIgnoreCase))
        {
            var table = new DataTable(DbMetaDataCollectionNames.MetaDataCollections);
            table.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
            table.Columns.Add(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int));
            table.Columns.Add(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int));
            table.Rows.Add(DbMetaDataCollectionNames.MetaDataCollections, 0, 0);
            table.Rows.Add("Tables", 1, 1);
            table.Rows.Add("Columns", 2, 2);
            return table;
        }

        RequireOpen();
        var sql = new StringBuilder();
        string? name = restrictionValues.Length > 0 ? restrictionValues[^1] : null;
        if (collectionName.Equals("Tables", StringComparison.OrdinalIgnoreCase))
        {
            sql.Append("SELECT table_schema, table_name, table_type FROM information_schema.tables");
            if (name is not null)
            {
                sql.Append(" WHERE table_name = ").Append(LiteralWriter.QuoteText(name));
            }

            sql.Append(" ORDER BY table_name");
        }
        else if (collectionName.Equals("Columns", StringComparison.OrdinalIgnoreCase))
        {
            sql.Append("SELECT table_name, column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns");
            var table = restrictionValues.Length > 0 ? restrictionValues[0] : null;
            if (table is not null)
            {
                sql.Append(" WHERE table_name = ").Append(LiteralWriter.QuoteText(table));
                if (restrictionValues.Length > 1 && restrictionValues[1] is { } column)
                {
                    sql.Append(" AND column_name = ").Append(LiteralWriter.QuoteText(column));
                }
            }

            sql.Append(" ORDER BY table_name, ordinal_position");
        }
        else
        {
            throw new ArgumentException($"\"{collectionName}\" is not a schema collection this provider has", nameof(collectionName));
        }

        using var command = new RasterizedDbCommand(sql.ToString(), this) { ResultFormat = RasterizedDbResultFormat.Text };
        using var reader = command.ExecuteReader();
        var result = new DataTable(collectionName);
        result.Load(reader);
        return result;
    }
}
