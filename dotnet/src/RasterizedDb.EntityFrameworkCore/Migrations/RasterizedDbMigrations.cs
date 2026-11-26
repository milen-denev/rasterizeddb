using Microsoft.EntityFrameworkCore;
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

    protected override string ExistsSql =>
        "SELECT COUNT(*) FROM pg_tables WHERE tablename = "
        + Dependencies.TypeMappingSource.GetMapping(typeof(string)).GenerateSqlLiteral(TableName);

    protected override bool InterpretExistsResult(object? value) =>
        value is not null and not DBNull && Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture) > 0;

    public override string GetCreateIfNotExistsScript()
    {
        var script = GetCreateScript();
        const string create = "CREATE TABLE ";
        var at = script.IndexOf(create, StringComparison.Ordinal);
        return at < 0 ? script : script.Insert(at + create.Length, "IF NOT EXISTS ");
    }

    public override string GetBeginIfNotExistsScript(string migrationId) =>
        throw new NotSupportedException("idempotent migration scripts need procedural SQL, which this provider does not generate");

    public override string GetBeginIfExistsScript(string migrationId) =>
        throw new NotSupportedException("idempotent migration scripts need procedural SQL, which this provider does not generate");

    public override string GetEndIfScript() =>
        throw new NotSupportedException("idempotent migration scripts need procedural SQL, which this provider does not generate");

    /// <summary>
    /// No lock is taken: run migrations from one place at a time. (The server has no advisory locks to
    /// take one with.)
    /// </summary>
    public override IMigrationsDatabaseLock AcquireDatabaseLock() => new NoLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IMigrationsDatabaseLock>(new NoLock(this));

    private sealed class NoLock(IHistoryRepository historyRepository) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => historyRepository;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }
}
