using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>What a migrator found or did.</summary>
/// <param name="Applied">The steps this call applied (or, for a verification, none).</param>
/// <param name="Pending">The steps not yet applied when the call ended.</param>
public sealed record RasterizedDbMigrationReport(IReadOnlyList<string> Applied, IReadOnlyList<string> Pending);

/// <summary>
/// Schema migrations without an ORM: named steps of SQL statements, applied once each, in order, and
/// recorded in a table of the database.
/// </summary>
/// <remarks>
/// <code>
/// var migrator = new RasterizedDbMigrator()
///     .Step("001_users", "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL)")
///     .Step("002_users_email", "ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)")
///     .Down("ALTER TABLE users DROP CONSTRAINT users_email_key");
/// await migrator.ApplyAsync(dataSource);        // at start-up
/// await migrator.VerifyAsync(dataSource);       // or: refuse to start with anything pending
/// </code>
/// <para>
/// Every statement of every step is checked before anything runs: a string holding two statements, or
/// transaction control, is refused up front, not after the steps before it have committed. Each step
/// runs in a transaction with the row that records it, so a step is applied whole or not at all.
/// </para>
/// <para>
/// A step is identified by its name and remembered with a checksum of its statements. Changing an
/// applied step's text is an error (write a new step); so is an applied step the migrator no longer
/// knows, and a pending step ordered before an applied one.
/// </para>
/// </remarks>
public sealed class RasterizedDbMigrator
{
    private sealed record StepDefinition(string Name, string[] Up)
    {
        public string[] DownStatements { get; set; } = [];
    }

    private readonly List<StepDefinition> _steps = [];
    private string _table = "rdb_migrations";

    /// <summary>The table applied steps are recorded in (<c>rdb_migrations</c>).</summary>
    public RasterizedDbMigrator HistoryTable(string name)
    {
        _table = name;
        return this;
    }

    /// <summary>Adds a step: a unique name and the statements that apply it, one statement per string.</summary>
    public RasterizedDbMigrator Step(string name, params string[] statements)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("a migration step needs a name", nameof(name));
        }

        if (_steps.Exists(s => s.Name == name))
        {
            throw new ArgumentException($"two migration steps are named \"{name}\"", nameof(name));
        }

        if (statements.Length == 0)
        {
            throw new ArgumentException($"migration step \"{name}\" has no statements", nameof(statements));
        }

        foreach (var statement in statements)
        {
            Check(name, statement);
        }

        _steps.Add(new StepDefinition(name, statements));
        return this;
    }

    /// <summary>The statements that undo the step added last, for <see cref="RollbackAsync"/>.</summary>
    public RasterizedDbMigrator Down(params string[] statements)
    {
        if (_steps.Count == 0)
        {
            throw new InvalidOperationException("Down follows the Step it undoes");
        }

        foreach (var statement in statements)
        {
            Check(_steps[^1].Name, statement);
        }

        _steps[^1].DownStatements = statements;
        return this;
    }

    private static void Check(string step, string statement)
    {
        var parsed = ParsedSql.Parse(statement);
        if (parsed.MultipleStatements)
        {
            throw new ArgumentException($"migration step \"{step}\": a string holds more than one statement; give each statement its own string");
        }

        if (parsed.Verb != TransactionVerb.None)
        {
            throw new ArgumentException($"migration step \"{step}\": transaction control is the migrator's; a step is applied in a transaction of its own");
        }

        if (parsed.ParameterCount != 0)
        {
            throw new ArgumentException($"migration step \"{step}\": a statement has a placeholder and no value to fill it");
        }
    }

    /// <summary>FNV-1a over the statements, each ended by a NUL so that moving text between two of them changes it.</summary>
    private static long Checksum(string[] statements)
    {
        var hash = 14695981039346656037UL;
        foreach (var statement in statements)
        {
            foreach (var b in Encoding.UTF8.GetBytes(statement.Trim()))
            {
                hash = (hash ^ b) * 1099511628211UL;
            }

            hash *= 1099511628211UL;
        }

        return unchecked((long)hash);
    }

    private string Table => LiteralWriter.QuoteIdentifier(_table);

    private static async Task<int> Run(RasterizedDbConnection connection, string sql, CancellationToken cancellation)
    {
        await using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        return await command.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
    }

    private async Task<Dictionary<string, long>> Recorded(RasterizedDbConnection connection, bool create, CancellationToken cancellation)
    {
        if (create)
        {
            await Run(
                connection,
                $"CREATE TABLE IF NOT EXISTS {Table} (name TEXT NOT NULL, checksum BIGINT NOT NULL, applied_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (name))",
                cancellation).ConfigureAwait(false);
        }
        else
        {
            await using var exists = new RasterizedDbCommand("SELECT tablename FROM pg_tables WHERE tablename = $1", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
            exists.Parameters.AddWithValue(_table);
            if (await exists.ExecuteScalarAsync(cancellation).ConfigureAwait(false) is null)
            {
                return [];
            }
        }

        var recorded = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var command = new RasterizedDbCommand($"SELECT name, checksum FROM {Table}", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        await using var reader = await command.ExecuteReaderAsync(cancellation).ConfigureAwait(false);
        while (reader.Read())
        {
            recorded[reader.GetString(0)] = reader.GetInt64(1);
        }

        return recorded;
    }

    /// <summary>The steps not applied yet, after checking what is recorded against what this migrator holds.</summary>
    private List<StepDefinition> Pending(Dictionary<string, long> recorded)
    {
        foreach (var name in recorded.Keys)
        {
            if (!_steps.Exists(s => s.Name == name))
            {
                throw new InvalidOperationException(
                    $"the database has applied migration step \"{name}\", which this program does not know: it is older than the database, or the step was removed");
            }
        }

        var pending = new List<StepDefinition>();
        foreach (var step in _steps)
        {
            if (recorded.TryGetValue(step.Name, out var checksum))
            {
                if (checksum != Checksum(step.Up))
                {
                    throw new InvalidOperationException(
                        $"migration step \"{step.Name}\" was applied with other statements than it holds now; an applied step is not edited, add a new step");
                }

                if (pending.Count > 0)
                {
                    throw new InvalidOperationException(
                        $"migration step \"{pending[0].Name}\" is not applied but \"{step.Name}\", which comes after it, is: steps are applied in order");
                }
            }
            else
            {
                pending.Add(step);
            }
        }

        return pending;
    }

    /// <summary>Applies every pending step, in order, each in its own transaction.</summary>
    public async Task<RasterizedDbMigrationReport> ApplyAsync(RasterizedDbConnection connection, CancellationToken cancellationToken = default)
    {
        var pending = Pending(await Recorded(connection, create: true, cancellationToken).ConfigureAwait(false));
        var applied = new List<string>(pending.Count);
        foreach (var step in pending)
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            foreach (var statement in step.Up)
            {
                try
                {
                    await Run(connection, statement, cancellationToken).ConfigureAwait(false);
                }
                catch (RasterizedDbException e) when (e.Kind == RasterizedDbErrorKind.Server)
                {
                    throw new InvalidOperationException($"migration step \"{step.Name}\" failed and was rolled back: {e.Message}", e);
                }
            }

            await using (var record = new RasterizedDbCommand($"INSERT INTO {Table} (name, checksum, applied_at) VALUES ($1, $2, $3)", connection) { ResultFormat = RasterizedDbResultFormat.Binary })
            {
                record.Parameters.AddWithValue(step.Name);
                record.Parameters.AddWithValue(Checksum(step.Up));
                record.Parameters.AddWithValue(DateTime.UtcNow);
                await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            applied.Add(step.Name);
        }

        return new RasterizedDbMigrationReport(applied, []);
    }

    public async Task<RasterizedDbMigrationReport> ApplyAsync(RasterizedDbDataSource dataSource, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await ApplyAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>What is pending, changing nothing (not even creating the history table).</summary>
    public async Task<RasterizedDbMigrationReport> StatusAsync(RasterizedDbConnection connection, CancellationToken cancellationToken = default)
    {
        var pending = Pending(await Recorded(connection, create: false, cancellationToken).ConfigureAwait(false));
        return new RasterizedDbMigrationReport([], pending.ConvertAll(s => s.Name));
    }

    /// <summary>Throws unless every step is applied: for a program that must not start against a database behind it.</summary>
    public async Task VerifyAsync(RasterizedDbConnection connection, CancellationToken cancellationToken = default)
    {
        var status = await StatusAsync(connection, cancellationToken).ConfigureAwait(false);
        if (status.Pending.Count > 0)
        {
            throw new InvalidOperationException($"{status.Pending.Count} migration step(s) are pending, first \"{status.Pending[0]}\"");
        }
    }

    public async Task VerifyAsync(RasterizedDbDataSource dataSource, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await VerifyAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Undoes the last <paramref name="steps"/> applied steps, newest first, each in its own
    /// transaction. A step with no <see cref="Down"/> statements is an error, before anything is undone.
    /// </summary>
    public async Task<IReadOnlyList<string>> RollbackAsync(RasterizedDbConnection connection, int steps = 1, CancellationToken cancellationToken = default)
    {
        var recorded = await Recorded(connection, create: false, cancellationToken).ConfigureAwait(false);
        Pending(recorded);
        var applied = _steps.FindAll(s => recorded.ContainsKey(s.Name));
        var undo = applied.GetRange(Math.Max(0, applied.Count - steps), Math.Min(steps, applied.Count));
        undo.Reverse();
        foreach (var step in undo)
        {
            if (step.DownStatements.Length == 0)
            {
                throw new InvalidOperationException($"migration step \"{step.Name}\" has no Down statements to undo it with");
            }
        }

        var undone = new List<string>(undo.Count);
        foreach (var step in undo)
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            foreach (var statement in step.DownStatements)
            {
                await Run(connection, statement, cancellationToken).ConfigureAwait(false);
            }

            await using (var forget = new RasterizedDbCommand($"DELETE FROM {Table} WHERE name = $1", connection) { ResultFormat = RasterizedDbResultFormat.Binary })
            {
                forget.Parameters.AddWithValue(step.Name);
                await forget.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            undone.Add(step.Name);
        }

        return undone;
    }
}
