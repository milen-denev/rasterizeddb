using System.Data;
using System.Data.Common;

namespace RasterizedDb.Data;

/// <summary>
/// A transaction block on its connection: the block PostgreSQL clients get, with savepoints, isolation
/// levels and DDL that rolls back. Disposing one that was neither committed nor rolled back rolls it
/// back.
/// </summary>
/// <remarks>
/// A statement that fails inside the block fails the block (SQLSTATE 25P02 until it is rolled back, or
/// rolled back to a savepoint). A connection lost inside it throws
/// <see cref="RasterizedDbErrorKind.TransactionLost"/>: the server has rolled the block back, unless the
/// request was the COMMIT (<see cref="RasterizedDbException.CommitOutcomeUnknown"/>).
/// </remarks>
public sealed class RasterizedDbTransaction : DbTransaction
{
    private RasterizedDbConnection? _connection;
    private bool _done;

    internal RasterizedDbTransaction(RasterizedDbConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        IsolationLevel = isolationLevel;
    }

    public override IsolationLevel IsolationLevel { get; }

    protected override DbConnection? DbConnection => _connection;

    public new RasterizedDbConnection? Connection => _connection;

    public override bool SupportsSavepoints => true;

    /// <summary>The connection is gone, or closed under the transaction: there is nothing left to end.</summary>
    internal void Forget()
    {
        _done = true;
        _connection = null;
    }

    private RasterizedDbConnection Live()
    {
        if (_done || _connection is null)
        {
            throw new InvalidOperationException("the transaction has ended");
        }

        return _connection;
    }

    private async ValueTask End(string sql, bool async, CancellationToken cancellation)
    {
        var connection = Live();
        try
        {
            await connection.ExecuteInternal(sql, async, cancellation).ConfigureAwait(false);
        }
        finally
        {
            // Ended whatever the answer: a COMMIT the server refused has rolled the block back.
            _done = true;
            connection.TransactionEnded(this);
            _connection = null;
        }
    }

    public override void Commit() => End("COMMIT", false, default).GetAwaiter().GetResult();

    public override Task CommitAsync(CancellationToken cancellationToken = default) => End("COMMIT", true, cancellationToken).AsTask();

    public override void Rollback() => End("ROLLBACK", false, default).GetAwaiter().GetResult();

    public override Task RollbackAsync(CancellationToken cancellationToken = default) => End("ROLLBACK", true, cancellationToken).AsTask();

    private static string Checked(string name)
    {
        var ok = name.Length is > 0 and <= 63 && (char.IsAsciiLetter(name[0]) || name[0] == '_');
        foreach (var c in name)
        {
            ok &= char.IsAsciiLetterOrDigit(c) || c == '_';
        }

        return ok
            ? name
            : throw new ArgumentException($"\"{name}\" is not a usable savepoint name: use letters, digits and '_', starting with a letter or '_' (at most 63)");
    }

    public override void Save(string savepointName) =>
        Live().ExecuteInternal("SAVEPOINT " + Checked(savepointName), false, default).GetAwaiter().GetResult();

    public override Task SaveAsync(string savepointName, CancellationToken cancellationToken = default) =>
        Live().ExecuteInternal("SAVEPOINT " + Checked(savepointName), true, cancellationToken).AsTask();

    public override void Rollback(string savepointName) =>
        Live().ExecuteInternal("ROLLBACK TO SAVEPOINT " + Checked(savepointName), false, default).GetAwaiter().GetResult();

    public override Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default) =>
        Live().ExecuteInternal("ROLLBACK TO SAVEPOINT " + Checked(savepointName), true, cancellationToken).AsTask();

    public override void Release(string savepointName) =>
        Live().ExecuteInternal("RELEASE SAVEPOINT " + Checked(savepointName), false, default).GetAwaiter().GetResult();

    public override Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default) =>
        Live().ExecuteInternal("RELEASE SAVEPOINT " + Checked(savepointName), true, cancellationToken).AsTask();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_done && _connection is not null)
        {
            try
            {
                Rollback();
            }
            catch (Exception)
            {
                // A lost connection has rolled it back already.
                Forget();
            }
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_done && _connection is not null)
        {
            try
            {
                await RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                Forget();
            }
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>The provider factory: <c>DbProviderFactories.RegisterFactory("RasterizedDb.Data", RasterizedDbFactory.Instance)</c>.</summary>
public sealed class RasterizedDbFactory : DbProviderFactory
{
    public static readonly RasterizedDbFactory Instance = new();

    private RasterizedDbFactory()
    {
    }

    public override DbCommand CreateCommand() => new RasterizedDbCommand();

    public override DbConnection CreateConnection() => new RasterizedDbConnection();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new RasterizedDbConnectionStringBuilder();

    public override DbParameter CreateParameter() => new RasterizedDbParameter();

    public override DbDataSource CreateDataSource(string connectionString) => new RasterizedDbDataSource(connectionString);

    public override bool CanCreateBatch => false;
}
