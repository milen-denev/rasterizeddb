using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.Extensions.DependencyInjection;
using RasterizedDb.Data;

[assembly: DesignTimeProviderServices("RasterizedDb.EntityFrameworkCore.Scaffolding.RasterizedDbDesignTimeServices")]

namespace RasterizedDb.EntityFrameworkCore.Scaffolding;

/// <summary>
/// Reads an existing database's tables, columns, keys and indexes, for <c>dotnet ef dbcontext
/// scaffold</c>. Everything comes from <c>information_schema</c> and <c>pg_indexes</c>, one view per
/// statement, joined here.
/// </summary>
public class RasterizedDbDatabaseModelFactory : DatabaseModelFactory
{
    private const string Schema = "public";

    public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using var connection = new RasterizedDbConnection(connectionString);
        return Create(connection, options);
    }

    public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        var opened = connection.State != ConnectionState.Open;
        if (opened)
        {
            connection.Open();
        }

        try
        {
            return Read((RasterizedDbConnection)connection, options);
        }
        finally
        {
            if (opened)
            {
                connection.Close();
            }
        }
    }

    private static List<string?[]> Rows(RasterizedDbConnection connection, string sql)
    {
        using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = RasterizedDbResultFormat.Text };
        using var reader = command.ExecuteReader();
        var rows = new List<string?[]>();
        while (reader.Read())
        {
            var row = new string?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetString(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>A view an older server may not have, or have fewer columns of: nothing, in place of an error.</summary>
    private static List<string?[]>? TryRows(RasterizedDbConnection connection, string sql)
    {
        try
        {
            return Rows(connection, sql);
        }
        catch (RasterizedDbException)
        {
            return null;
        }
    }

    private static DatabaseModel Read(RasterizedDbConnection connection, DatabaseModelFactoryOptions options)
    {
        // No schema on the model or its tables: the server has the one, and a scaffolded context that
        // named it (HasDefaultSchema("public")) would qualify every table in every statement for nothing.
        var model = new DatabaseModel { DatabaseName = connection.Database };
        var wanted = new HashSet<string>(options.Tables.Select(t => t[(t.LastIndexOf('.') + 1)..].Trim('"')), StringComparer.Ordinal);
        if (options.Schemas.Any() && !options.Schemas.Contains(Schema))
        {
            return model;
        }

        var tables = new Dictionary<string, DatabaseTable>(StringComparer.Ordinal);
        foreach (var row in Rows(
                     connection,
                     $"SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '{Schema}' ORDER BY table_name"))
        {
            var name = row[0]!;
            var view = string.Equals(row[1], "VIEW", StringComparison.OrdinalIgnoreCase);
            if (!view && !string.Equals(row[1], "BASE TABLE", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            // The migrations history table, and the server's own catalogue tables, are not the application's.
            if (name == HistoryRepository.DefaultTableName || name.StartsWith("__rdb_", StringComparison.Ordinal) || (wanted.Count > 0 && !wanted.Contains(name)))
            {
                continue;
            }

            // A view scaffolds as a keyless entity over it.
            var table = view ? new DatabaseView { Database = model, Name = name } : new DatabaseTable { Database = model, Name = name };
            tables[name] = table;
            model.Tables.Add(table);
        }

        ReadColumns(connection, tables);
        ReadConstraints(connection, tables);
        ReadIndexes(connection, tables);
        return model;
    }

    private static void ReadColumns(RasterizedDbConnection connection, Dictionary<string, DatabaseTable> tables)
    {
        var rows = TryRows(
                       connection,
                       "SELECT table_name, column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, udt_name " +
                       $"FROM information_schema.columns WHERE table_schema = '{Schema}' ORDER BY table_name, ordinal_position")
                   ?? Rows(
                       connection,
                       $"SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '{Schema}' ORDER BY table_name, ordinal_position");
        foreach (var row in rows)
        {
            if (!tables.TryGetValue(row[0]!, out var table))
            {
                continue;
            }

            string? At(int i) => i < row.Length ? row[i] : null;
            var type = row[2]!;
            var lower = type.ToLowerInvariant();
            if (lower == "array")
            {
                // udt_name is the element's internal name with an underscore in front: _int4, _text.
                type = At(8) is { Length: > 1 } udt && udt[0] == '_' ? ElementName(udt[1..]) + "[]" : "text[]";
            }
            else if (lower is "character varying" or "character" && At(5) is { } length)
            {
                type = $"{type}({length})";
            }
            else if (lower == "numeric" && At(6) is { } precision)
            {
                type = At(7) is { } scale and not "0" ? $"numeric({precision},{scale})" : $"numeric({precision})";
            }

            var defaultSql = At(4);
            var serial = defaultSql is not null && defaultSql.Contains("nextval(", StringComparison.OrdinalIgnoreCase);
            table.Columns.Add(new DatabaseColumn
            {
                Table = table,
                Name = row[1]!,
                StoreType = type,
                IsNullable = string.Equals(row[3], "YES", StringComparison.OrdinalIgnoreCase),
                DefaultValueSql = serial || string.IsNullOrEmpty(defaultSql) ? null : defaultSql,
                ValueGenerated = serial ? ValueGenerated.OnAdd : null,
            });
        }
    }

    private static string ElementName(string internalName) => internalName switch
    {
        "int2" => "smallint",
        "int4" => "integer",
        "int8" => "bigint",
        "float4" => "real",
        "float8" => "double precision",
        "bool" => "boolean",
        "varchar" => "character varying",
        "bpchar" => "character",
        "timestamptz" => "timestamp with time zone",
        "timestamp" => "timestamp without time zone",
        _ => internalName,
    };

    private static void ReadConstraints(RasterizedDbConnection connection, Dictionary<string, DatabaseTable> tables)
    {
        var constraints = TryRows(
            connection,
            $"SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_schema = '{Schema}'");
        var usage = TryRows(
            connection,
            $"SELECT constraint_name, table_name, column_name, ordinal_position FROM information_schema.key_column_usage WHERE table_schema = '{Schema}'");
        if (constraints is null || usage is null)
        {
            return;
        }

        // (table, constraint) -> its columns, in key order. A constraint's name is unique within its table.
        var columnsOf = usage
            .GroupBy(r => (Table: r[1]!, Name: r[0]!))
            .ToDictionary(
                g => g.Key,
                g => g.OrderBy(r => int.Parse(r[3] ?? "0", CultureInfo.InvariantCulture)).Select(r => r[2]!).ToList());

        List<DatabaseColumn>? Columns(DatabaseTable table, string constraint)
        {
            if (!columnsOf.TryGetValue((table.Name, constraint), out var names))
            {
                return null;
            }

            var columns = new List<DatabaseColumn>(names.Count);
            foreach (var name in names)
            {
                if (table.Columns.FirstOrDefault(c => c.Name == name) is not { } column)
                {
                    return null;
                }

                columns.Add(column);
            }

            return columns;
        }

        foreach (var row in constraints)
        {
            if (!tables.TryGetValue(row[1]!, out var table) || Columns(table, row[0]!) is not { Count: > 0 } columns)
            {
                continue;
            }

            switch (row[2])
            {
                case "PRIMARY KEY":
                {
                    var key = new DatabasePrimaryKey { Table = table, Name = row[0] };
                    foreach (var column in columns)
                    {
                        key.Columns.Add(column);
                    }

                    table.PrimaryKey = key;
                    break;
                }
                case "UNIQUE":
                {
                    var unique = new DatabaseUniqueConstraint { Table = table, Name = row[0] };
                    foreach (var column in columns)
                    {
                        unique.Columns.Add(column);
                    }

                    table.UniqueConstraints.Add(unique);
                    break;
                }
            }
        }

        var references = TryRows(
            connection,
            $"SELECT constraint_name, unique_constraint_name, delete_rule FROM information_schema.referential_constraints WHERE constraint_schema = '{Schema}'");
        if (references is null)
        {
            return;
        }

        foreach (var row in constraints.Where(r => r[2] == "FOREIGN KEY"))
        {
            if (!tables.TryGetValue(row[1]!, out var table) || Columns(table, row[0]!) is not { Count: > 0 } columns)
            {
                continue;
            }

            var reference = references.FirstOrDefault(r => r[0] == row[0]);
            if (reference is null)
            {
                continue;
            }

            // The key it points at: the PRIMARY KEY or UNIQUE constraint of that name.
            var target = constraints.FirstOrDefault(r => r[0] == reference[1] && r[2] is "PRIMARY KEY" or "UNIQUE" && tables.ContainsKey(r[1]!));
            if (target is null || Columns(tables[target[1]!], target[0]!) is not { } principalColumns || principalColumns.Count != columns.Count)
            {
                continue;
            }

            var foreignKey = new DatabaseForeignKey
            {
                Table = table,
                PrincipalTable = tables[target[1]!],
                Name = row[0],
                OnDelete = reference[2]?.ToUpperInvariant() switch
                {
                    "CASCADE" => ReferentialAction.Cascade,
                    "SET NULL" => ReferentialAction.SetNull,
                    "SET DEFAULT" => ReferentialAction.SetDefault,
                    "RESTRICT" => ReferentialAction.Restrict,
                    _ => ReferentialAction.NoAction,
                },
            };
            foreach (var column in columns)
            {
                foreignKey.Columns.Add(column);
            }

            foreach (var column in principalColumns)
            {
                foreignKey.PrincipalColumns.Add(column);
            }

            table.ForeignKeys.Add(foreignKey);
        }
    }

    private static void ReadIndexes(RasterizedDbConnection connection, Dictionary<string, DatabaseTable> tables)
    {
        var rows = TryRows(connection, "SELECT tablename, indexname, indexdef FROM pg_indexes");
        if (rows is null)
        {
            return;
        }

        foreach (var row in rows)
        {
            if (!tables.TryGetValue(row[0]!, out var table) || row[1] is not { } name || row[2] is not { } definition)
            {
                continue;
            }

            // A key's index is the key, which the model already has.
            if (table.PrimaryKey?.Name == name || table.UniqueConstraints.Any(u => u.Name == name))
            {
                continue;
            }

            // CREATE [UNIQUE] INDEX name ON table [USING method] (col [opclass], …)
            var open = definition.LastIndexOf('(');
            var close = definition.LastIndexOf(')');
            if (open < 0 || close < open)
            {
                continue;
            }

            var index = new DatabaseIndex
            {
                Table = table,
                Name = name,
                IsUnique = definition.StartsWith("CREATE UNIQUE", StringComparison.OrdinalIgnoreCase),
            };
            var complete = true;
            foreach (var part in definition[(open + 1)..close].Split(','))
            {
                var columnName = part.Trim().Split(' ')[0].Trim('"');
                if (table.Columns.FirstOrDefault(c => c.Name == columnName) is { } column)
                {
                    index.Columns.Add(column);
                }
                else
                {
                    // An expression index: nothing a model can say.
                    complete = false;
                }
            }

            if (complete && index.Columns.Count > 0)
            {
                table.Indexes.Add(index);
            }
        }
    }
}

/// <summary>What <c>dotnet ef</c> loads from this assembly at design time.</summary>
public class RasterizedDbDesignTimeServices : IDesignTimeServices
{
    public virtual void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkRasterizedDb();
        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IAnnotationCodeGenerator, AnnotationCodeGenerator>()
            .TryAdd<IDatabaseModelFactory, RasterizedDbDatabaseModelFactory>()
            .TryAdd<IProviderConfigurationCodeGenerator, RasterizedDbCodeGenerator>()
            .TryAddCoreServices();
    }
}

/// <summary>Writes <c>.UseRasterizedDb("…")</c> into a scaffolded context.</summary>
public class RasterizedDbCodeGenerator(ProviderCodeGeneratorDependencies dependencies) : ProviderCodeGenerator(dependencies)
{
    private static readonly System.Reflection.MethodInfo UseRasterizedDb = typeof(RasterizedDbDbContextOptionsExtensions).GetMethod(
        nameof(RasterizedDbDbContextOptionsExtensions.UseRasterizedDb),
        [typeof(DbContextOptionsBuilder), typeof(string), typeof(Action<Infrastructure.RasterizedDbDbContextOptionsBuilder>)])!;

    public override MethodCallCodeFragment GenerateUseProvider(string connectionString, MethodCallCodeFragment? providerOptions) =>
        new(
            UseRasterizedDb,
            providerOptions is null
                ? [connectionString]
                : [connectionString, new NestedClosureCodeFragment("x", providerOptions)]);
}
