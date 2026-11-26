using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using RasterizedDb.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Migrations;

/// <summary>Annotation names the provider puts on a model.</summary>
public static class RasterizedDbAnnotationNames
{
    /// <summary>On a column: its values come from a sequence of its own (<c>serial</c>, <c>bigserial</c>, <c>smallserial</c>).</summary>
    public const string Serial = "RasterizedDb:Serial";

    /// <summary>On a property: the sequence its keys are taken from a block at a time (<c>UseHiLo</c>).</summary>
    public const string HiLoSequence = "RasterizedDb:HiLoSequence";

    public const string HiLoSequenceSchema = "RasterizedDb:HiLoSequenceSchema";
}

/// <summary>Marks integer columns generated on insert as <c>serial</c>, so migrations create them that way.</summary>
public class RasterizedDbAnnotationProvider(RelationalAnnotationProviderDependencies dependencies) : RelationalAnnotationProvider(dependencies)
{
    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
    {
        if (!designTime)
        {
            yield break;
        }

        foreach (var mapping in column.PropertyMappings)
        {
            var property = mapping.Property;
            var type = (property.GetTypeMapping().Converter?.ProviderClrType ?? property.ClrType).UnwrapNullable();
            if (property.ValueGenerated == ValueGenerated.OnAdd
                && (type == typeof(int) || type == typeof(long) || type == typeof(short))
                && property.GetDefaultValueSql() is null
                && property[RasterizedDbAnnotationNames.HiLoSequence] is null
                && !property.TryGetDefaultValue(out _)
                && property.GetComputedColumnSql() is null
                && property.FindContainingPrimaryKey() is not null
                && !property.IsForeignKey())
            {
                yield return new Annotation(RasterizedDbAnnotationNames.Serial, true);
                yield break;
            }
        }
    }
}

internal static class TypeExtensions
{
    public static Type UnwrapNullable(this Type type) => Nullable.GetUnderlyingType(type) ?? type;
}

/// <summary>DDL in the dialect RasterizedDB reads: PostgreSQL's.</summary>
public class RasterizedDbMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies) : MigrationsSqlGenerator(dependencies)
{
    public override IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
    {
        // DDL, and whatever a migration's Sql(…) holds, runs as a simple query: the text format.
        using var _ = RasterizedDbCommandFormat.UseText();
        return base.Generate(operations, model, options);
    }

    private string Name(string name, string? schema = null) => Dependencies.SqlGenerationHelper.DelimitIdentifier(name, schema);

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        if (operation.ComputedColumnSql is not null)
        {
            throw new NotSupportedException($"column \"{name}\" of \"{table}\" is computed, and RasterizedDB has no generated columns");
        }

        var type = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;
        if (operation[RasterizedDbAnnotationNames.Serial] is true)
        {
            type = type.ToLowerInvariant() switch
            {
                "bigint" or "int8" => "bigserial",
                "smallint" or "int2" => "smallserial",
                _ => "serial",
            };
        }

        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name)).Append(" ").Append(type);
        if (operation.Collation is not null)
        {
            builder.Append(" COLLATE ").Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Collation));
        }

        builder.Append(operation.IsNullable ? " NULL" : " NOT NULL");
        DefaultValue(operation.DefaultValue, operation.DefaultValueSql, type, builder);
    }

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.NewSchema is not null && operation.NewSchema != operation.Schema)
        {
            builder.Append("ALTER TABLE ").Append(Name(operation.Name, operation.Schema)).Append(" SET SCHEMA ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewSchema))
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }

        if (operation.NewName is not null && operation.NewName != operation.Name)
        {
            builder.Append("ALTER TABLE ").Append(Name(operation.Name, operation.NewSchema ?? operation.Schema)).Append(" RENAME TO ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER TABLE ").Append(Name(operation.Table, operation.Schema)).Append(" RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name)).Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER INDEX ").Append(Name(operation.Name, operation.Schema)).Append(" RENAME TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.NewName is not null && operation.NewName != operation.Name)
        {
            builder.Append("ALTER SEQUENCE ").Append(Name(operation.Name, operation.Schema)).Append(" RENAME TO ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.Append("ALTER SEQUENCE ").Append(Name(operation.Name, operation.Schema)).Append(" RESTART");
        if (operation.StartValue is { } start)
        {
            builder.Append(" WITH ").Append(start.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("DROP INDEX ").Append(Name(operation.Name, operation.Schema));
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.Name == "public")
        {
            return;
        }

        builder.Append("CREATE SCHEMA IF NOT EXISTS ").Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(DropTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        builder.Append("DROP TABLE ").Append(Name(operation.Name, operation.Schema));
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// A changed column, as the separate ALTER COLUMN clauses PostgreSQL has: its type, whether it
    /// takes NULL, and its default.
    /// </summary>
    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;
        var table = Name(operation.Table, operation.Schema);
        var column = helper.DelimitIdentifier(operation.Name);
        var type = operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model)!;
        var oldType = operation.OldColumn.ColumnType
            ?? (operation.OldColumn.ClrType is not null ? GetColumnType(operation.Schema, operation.Table, operation.Name, operation.OldColumn, model) : null);

        if (!string.Equals(type, oldType, StringComparison.OrdinalIgnoreCase))
        {
            builder.Append("ALTER TABLE ").Append(table).Append(" ALTER COLUMN ").Append(column).Append(" TYPE ").Append(type)
                .AppendLine(helper.StatementTerminator);
            EndStatement(builder);
        }

        if (operation.IsNullable != operation.OldColumn.IsNullable)
        {
            builder.Append("ALTER TABLE ").Append(table).Append(" ALTER COLUMN ").Append(column)
                .Append(operation.IsNullable ? " DROP NOT NULL" : " SET NOT NULL")
                .AppendLine(helper.StatementTerminator);
            EndStatement(builder);
        }

        if (!Equals(operation.DefaultValue, operation.OldColumn.DefaultValue) || operation.DefaultValueSql != operation.OldColumn.DefaultValueSql)
        {
            builder.Append("ALTER TABLE ").Append(table).Append(" ALTER COLUMN ").Append(column);
            if (operation.DefaultValue is null && operation.DefaultValueSql is null)
            {
                builder.Append(" DROP DEFAULT");
            }
            else
            {
                builder.Append(" SET");
                DefaultValue(operation.DefaultValue, operation.DefaultValueSql, type, builder);
            }

            builder.AppendLine(helper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // Nothing a RasterizedDB database has to alter.
    }
}

/// <summary>The <c>__EFMigrationsHistory</c> table.</summary>
public class RasterizedDbHistoryRepository(HistoryRepositoryDependencies dependencies) : HistoryRepository(dependencies)
{
    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

    // The server keeps names that start with "__" out of its catalogue views (they are where it keeps
    // its own tables), so the table is not looked up there: it is read, and a refusal naming an
    // undefined table (42P01) is the answer "no".
    protected override string ExistsSql => $"SELECT COUNT(*) FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)}";

    protected override bool InterpretExistsResult(object? value) => value is not null and not DBNull;

    private static bool Undefined(RasterizedDb.Data.RasterizedDbException e) =>
        e.SqlState == "42P01" || (e.SqlState is null && e.Message.Contains("not found", StringComparison.OrdinalIgnoreCase));

    public override bool Exists()
    {
        try
        {
            Dependencies.RawSqlCommandBuilder.Build(ExistsSql).ExecuteScalar(Parameters);
            return true;
        }
        catch (RasterizedDb.Data.RasterizedDbException e) when (Undefined(e))
        {
            return false;
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await Dependencies.RawSqlCommandBuilder.Build(ExistsSql).ExecuteScalarAsync(Parameters, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (RasterizedDb.Data.RasterizedDbException e) when (Undefined(e))
        {
            return false;
        }
    }

    public override string GetCreateIfNotExistsScript()
    {
        var script = GetCreateScript();
        const string create = "CREATE TABLE ";
        var at = script.IndexOf(create, StringComparison.Ordinal);
        return at < 0 ? script : script.Insert(at + create.Length, "IF NOT EXISTS ");
    }

    // An idempotent script wraps each migration in a DO block that looks in the history table first.
    private string IfHistory(string migrationId, bool exists) =>
        $"""

        DO $EF$
        BEGIN
            IF {(exists ? "" : "NOT ")}EXISTS(SELECT 1 FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {Dependencies.TypeMappingSource.GetMapping(typeof(string)).GenerateSqlLiteral(migrationId)}) THEN
        """;

    public override string GetBeginIfNotExistsScript(string migrationId) => IfHistory(migrationId, exists: false);

    public override string GetBeginIfExistsScript(string migrationId) => IfHistory(migrationId, exists: true);

    public override string GetEndIfScript() =>
        """
            END IF;
        END $EF$;
        """;

    private const string LockTable = "__EFMigrationsLock";

    private RelationalCommandParameterObject Parameters =>
        new(Dependencies.Connection, null, null, Dependencies.CurrentContext.Context, Dependencies.CommandLogger, CommandSource.Migrations);

    private string LockName => SqlGenerationHelper.DelimitIdentifier(LockTable);

    private string CreateLockSql => $"CREATE TABLE IF NOT EXISTS {LockName} (\"Id\" integer NOT NULL, \"Timestamp\" text NOT NULL, PRIMARY KEY (\"Id\"))";

    private string TakeLockSql => $"INSERT INTO {LockName} (\"Id\", \"Timestamp\") VALUES (1, '{DateTimeOffset.UtcNow:O}')";

    private static readonly TimeSpan LockWait = TimeSpan.FromMinutes(2);

    private InvalidOperationException LockHeld() =>
        new($"another migration holds the lock (a row in {LockTable}) and did not release it within {LockWait.TotalSeconds:0} s; " +
            $"if no migration is running, it was left by one that died: DELETE FROM {LockName}");

    /// <summary>
    /// The server has no advisory locks, so the lock is a row: whoever inserts row 1 of
    /// <c>__EFMigrationsLock</c> holds it (its primary key refuses a second), and deletes it when done.
    /// </summary>
    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        Dependencies.RawSqlCommandBuilder.Build(CreateLockSql).ExecuteNonQuery(Parameters);
        var waited = System.Diagnostics.Stopwatch.StartNew();
        var delay = 100;
        while (true)
        {
            try
            {
                Dependencies.RawSqlCommandBuilder.Build(TakeLockSql).ExecuteNonQuery(Parameters);
                return new RowLock(this);
            }
            catch (RasterizedDb.Data.RasterizedDbException e) when (e.IsUniqueViolation)
            {
                if (waited.Elapsed > LockWait)
                {
                    throw LockHeld();
                }
            }

            Thread.Sleep(delay);
            delay = Math.Min(delay * 2, 2000);
        }
    }

    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
    {
        await Dependencies.RawSqlCommandBuilder.Build(CreateLockSql).ExecuteNonQueryAsync(Parameters, cancellationToken).ConfigureAwait(false);
        var waited = System.Diagnostics.Stopwatch.StartNew();
        var delay = 100;
        while (true)
        {
            try
            {
                await Dependencies.RawSqlCommandBuilder.Build(TakeLockSql).ExecuteNonQueryAsync(Parameters, cancellationToken).ConfigureAwait(false);
                return new RowLock(this);
            }
            catch (RasterizedDb.Data.RasterizedDbException e) when (e.IsUniqueViolation)
            {
                if (waited.Elapsed > LockWait)
                {
                    throw LockHeld();
                }
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, 2000);
        }
    }

    private sealed class RowLock(RasterizedDbHistoryRepository repository) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => repository;

        public void Dispose() =>
            repository.Dependencies.RawSqlCommandBuilder.Build($"DELETE FROM {repository.LockName}").ExecuteNonQuery(repository.Parameters);

        public async ValueTask DisposeAsync() =>
            await repository.Dependencies.RawSqlCommandBuilder.Build($"DELETE FROM {repository.LockName}")
                .ExecuteNonQueryAsync(repository.Parameters).ConfigureAwait(false);
    }
}
