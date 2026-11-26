using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;
using RasterizedDb.Data;
using RasterizedDb.EntityFrameworkCore.Infrastructure;

namespace RasterizedDb.EntityFrameworkCore.Storage;

/// <summary>
/// Which result format the commands being built right now are sent in.
/// </summary>
/// <remarks>
/// The provider reads the rows of the SQL it generates by position, so that SQL runs in the binary
/// format: the native path, with typed cells. SQL it did not write (raw SQL, a migration's
/// <c>Sql(…)</c>) may hold several statements and may be read by column name, so it runs in the text
/// format, which is what a PostgreSQL client's simple query gets.
/// </remarks>
public static class RasterizedDbCommandFormat
{
    [ThreadStatic]
    private static int _text;

    internal static bool TextRequested => _text > 0;

    /// <summary>Commands built until the returned scope is disposed run in the text format.</summary>
    public static Scope UseText()
    {
        _text++;
        return default;
    }

    public readonly struct Scope : IDisposable
    {
        public void Dispose() => _text--;
    }
}

public class RasterizedDbCommandBuilderFactory(RelationalCommandBuilderDependencies dependencies) : RelationalCommandBuilderFactory(dependencies)
{
    public override IRelationalCommandBuilder Create() => new RasterizedDbCommandBuilder(Dependencies);
}

public class RasterizedDbCommandBuilder(RelationalCommandBuilderDependencies dependencies) : RelationalCommandBuilder(dependencies)
{
    /// <summary>Whether the command runs in the text format (see <see cref="RasterizedDbCommandFormat"/>).</summary>
    public bool Text { get; set; }

    public override IRelationalCommand Build() =>
        new RasterizedDbRelationalCommand(Dependencies, ToString(), Parameters) { Text = Text || RasterizedDbCommandFormat.TextRequested };
}

public class RasterizedDbRelationalCommand(RelationalCommandBuilderDependencies dependencies, string commandText, IReadOnlyList<IRelationalParameter> parameters)
#if EF10
    : RelationalCommand(dependencies, commandText, commandText, parameters)
#else
    : RelationalCommand(dependencies, commandText, parameters)
#endif
{
    public bool Text { get; set; }

    public override DbCommand CreateDbCommand(RelationalCommandParameterObject parameterObject, Guid commandId, DbCommandMethod commandMethod)
    {
        var command = base.CreateDbCommand(parameterObject, commandId, commandMethod);
        if (command is RasterizedDbCommand native)
        {
            native.ResultFormat = Text ? RasterizedDbResultFormat.Text : RasterizedDbResultFormat.Binary;
        }

        return command;
    }

    public override void PopulateFrom(IRelationalCommandTemplate commandTemplate)
    {
        base.PopulateFrom(commandTemplate);
        if (commandTemplate is RasterizedDbRelationalCommand template)
        {
            Text = template.Text;
        }
    }
}

/// <summary>Raw SQL (<c>ExecuteSql</c>, the history table's statements) runs in the text format.</summary>
public class RasterizedDbRawSqlCommandBuilder(
    IRelationalCommandBuilderFactory relationalCommandBuilderFactory,
    ISqlGenerationHelper sqlGenerationHelper,
    IParameterNameGeneratorFactory parameterNameGeneratorFactory,
    IRelationalTypeMappingSource typeMappingSource)
    : RawSqlCommandBuilder(relationalCommandBuilderFactory, sqlGenerationHelper, parameterNameGeneratorFactory, typeMappingSource)
{
    public override IRelationalCommand Build(string sql)
    {
        using var _ = RasterizedDbCommandFormat.UseText();
        return base.Build(sql);
    }

    public override RawSqlCommand Build(string sql, IEnumerable<object?> parameters)
    {
        using var _ = RasterizedDbCommandFormat.UseText();
        return base.Build(sql, parameters);
    }

    public override RawSqlCommand Build(string sql, IEnumerable<object?> parameters, IModel? model)
    {
        using var _ = RasterizedDbCommandFormat.UseText();
        return base.Build(sql, parameters, model);
    }
}

public class RasterizedDbRelationalConnection : RelationalConnection
{
    private readonly RasterizedDbDataSource? _dataSource;

    public RasterizedDbRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        _dataSource = dependencies.ContextOptions.FindExtension<RasterizedDbOptionsExtension>()?.DataSource;
    }

    protected override DbConnection CreateDbConnection() =>
        _dataSource is not null ? _dataSource.CreateConnection() : new RasterizedDbConnection(GetValidatedConnectionString());

    protected override bool SupportsAmbientTransactions => false;

    /// <summary>A connection to the server's default database, for CREATE and DROP DATABASE.</summary>
    public virtual RasterizedDbConnection CreateAdminConnection()
    {
        var builder = new RasterizedDbConnectionStringBuilder(GetValidatedConnectionString()) { Pooling = false };
        builder.Remove("Database");
        return new RasterizedDbConnection(builder.ConnectionString);
    }
}

public class RasterizedDbDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies, IRawSqlCommandBuilder rawSqlCommandBuilder)
    : RelationalDatabaseCreator(dependencies)
{
    private string DatabaseName => new RasterizedDbConnectionStringBuilder(Dependencies.Connection.ConnectionString).Database
        ?? throw new InvalidOperationException("the connection string names no database to create or delete");

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    public override bool Exists()
    {
        try
        {
            using var connection = CloneConnection();
            connection.Open();
            return true;
        }
        catch (RasterizedDbException)
        {
            return false;
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = CloneConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (RasterizedDbException)
        {
            return false;
        }
    }

    // A connection of its own, so the probe leaves the context's connection as it was.
    private RasterizedDbConnection CloneConnection()
    {
        var builder = new RasterizedDbConnectionStringBuilder(Dependencies.Connection.ConnectionString) { Pooling = false };
        return new RasterizedDbConnection(builder.ConnectionString);
    }

    private const string HasTablesSql = "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'";

    public override bool HasTables() =>
        Dependencies.ExecutionStrategy.Execute(
            Dependencies.Connection,
            connection => Convert.ToInt64(
                rawSqlCommandBuilder.Build(HasTablesSql).ExecuteScalar(
                    new RelationalCommandParameterObject(connection, null, null, Dependencies.CurrentContext.Context, Dependencies.CommandLogger)),
                System.Globalization.CultureInfo.InvariantCulture) > 0,
            null);

    public override Task<bool> HasTablesAsync(CancellationToken cancellationToken = default) =>
        Dependencies.ExecutionStrategy.ExecuteAsync(
            Dependencies.Connection,
            async (connection, token) => Convert.ToInt64(
                await rawSqlCommandBuilder.Build(HasTablesSql).ExecuteScalarAsync(
                    new RelationalCommandParameterObject(connection, null, null, Dependencies.CurrentContext.Context, Dependencies.CommandLogger),
                    token).ConfigureAwait(false),
                System.Globalization.CultureInfo.InvariantCulture) > 0,
            null,
            cancellationToken);

    private void Admin(string sql)
    {
        using var connection = ((RasterizedDbRelationalConnection)Dependencies.Connection).CreateAdminConnection();
        connection.Open();
        using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = RasterizedDbResultFormat.Text };
        command.ExecuteNonQuery();
    }

    private async Task AdminAsync(string sql, CancellationToken cancellationToken)
    {
        await using var connection = ((RasterizedDbRelationalConnection)Dependencies.Connection).CreateAdminConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = RasterizedDbResultFormat.Text };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Create() => Admin($"CREATE DATABASE {Quote(DatabaseName)}");

    public override Task CreateAsync(CancellationToken cancellationToken = default) => AdminAsync($"CREATE DATABASE {Quote(DatabaseName)}", cancellationToken);

    public override void Delete()
    {
        RasterizedDbDataSource.ClearAllPools();
        Admin($"DROP DATABASE {Quote(DatabaseName)}");
    }

    public override Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        RasterizedDbDataSource.ClearAllPools();
        return AdminAsync($"DROP DATABASE {Quote(DatabaseName)}", cancellationToken);
    }
}

/// <summary>
/// Retries a unit of work after a serialization failure (40001) or a deadlock (40P01). Nothing else is
/// retried: a statement whose connection failed may have run.
/// </summary>
public class RasterizedDbRetryingExecutionStrategy : ExecutionStrategy
{
    public RasterizedDbRetryingExecutionStrategy(ExecutionStrategyDependencies dependencies, int maxRetryCount = 2, TimeSpan? maxRetryDelay = null)
        : base(dependencies, maxRetryCount, maxRetryDelay ?? TimeSpan.FromSeconds(5))
    {
    }

    public RasterizedDbRetryingExecutionStrategy(DbContext context, int maxRetryCount = 2, TimeSpan? maxRetryDelay = null)
        : base(context, maxRetryCount, maxRetryDelay ?? TimeSpan.FromSeconds(5))
    {
    }

    protected override bool ShouldRetryOn(Exception exception) => exception is RasterizedDbException { IsTransient: true };
}
