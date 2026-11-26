using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Query;

public class RasterizedDbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public RasterizedDbMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sql = dependencies.SqlExpressionFactory;
        AddTranslators(
        [
            new RasterizedDbStringMethodTranslator(sql),
            new RasterizedDbMathTranslator(sql),
            new RasterizedDbMiscMethodTranslator(sql, dependencies.RelationalTypeMappingSource),
        ]);
    }
}

public class RasterizedDbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public RasterizedDbMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new RasterizedDbMemberTranslator(dependencies.SqlExpressionFactory)]);
    }
}

/// <summary><see cref="string"/> methods as PostgreSQL's string functions.</summary>
public class RasterizedDbStringMethodTranslator(ISqlExpressionFactory sql) : IMethodCallTranslator
{
    private SqlExpression Function(string name, Type type, params SqlExpression[] arguments) =>
        sql.Function(name, arguments, nullable: true, argumentsPropagateNullability: arguments.Select(_ => true).ToArray(), type);

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(string))
        {
            return null;
        }

        if (instance is null)
        {
            switch (method.Name)
            {
                case nameof(string.IsNullOrEmpty) when arguments.Count == 1:
                    return sql.OrElse(sql.IsNull(arguments[0]), sql.Equal(arguments[0], sql.Constant(string.Empty)));
                case nameof(string.IsNullOrWhiteSpace) when arguments.Count == 1:
                    return sql.OrElse(
                        sql.IsNull(arguments[0]),
                        sql.Equal(Function("btrim", typeof(string), arguments[0]), sql.Constant(string.Empty)));
                case nameof(string.Concat) when arguments.Count is 2 or 3 or 4 && arguments.All(a => a.Type == typeof(string)):
                {
                    var result = arguments[0];
                    for (var i = 1; i < arguments.Count; i++)
                    {
                        result = sql.Add(result, arguments[i]);
                    }

                    return result;
                }
            }

            return null;
        }

        var mapping = instance.TypeMapping;
        switch (method.Name)
        {
            case nameof(string.ToUpper) or nameof(string.ToUpperInvariant) when arguments.Count == 0:
                return sql.Function("upper", [instance], true, [true], typeof(string), mapping);
            case nameof(string.ToLower) or nameof(string.ToLowerInvariant) when arguments.Count == 0:
                return sql.Function("lower", [instance], true, [true], typeof(string), mapping);
            case nameof(string.Trim) when arguments.Count == 0:
                return sql.Function("btrim", [instance], true, [true], typeof(string), mapping);
            case nameof(string.TrimStart) when arguments.Count == 0:
                return sql.Function("ltrim", [instance], true, [true], typeof(string), mapping);
            case nameof(string.TrimEnd) when arguments.Count == 0:
                return sql.Function("rtrim", [instance], true, [true], typeof(string), mapping);
            case nameof(string.Replace) when arguments.Count == 2 && arguments[0].Type == typeof(string):
            {
                var from = sql.ApplyTypeMapping(arguments[0], mapping);
                var to = sql.ApplyTypeMapping(arguments[1], mapping);
                return sql.Function("replace", [instance, from, to], true, [true, true, true], typeof(string), mapping);
            }
            case nameof(string.Substring) when arguments.Count == 1:
                return sql.Function("substr", [instance, sql.Add(arguments[0], sql.Constant(1))], true, [true, true], typeof(string), mapping);
            case nameof(string.Substring) when arguments.Count == 2:
                return sql.Function("substr", [instance, sql.Add(arguments[0], sql.Constant(1)), arguments[1]], true, [true, true, true], typeof(string), mapping);
            case nameof(string.IndexOf) when arguments.Count == 1 && arguments[0].Type == typeof(string):
                return sql.Subtract(
                    Function("strpos", typeof(int), instance, sql.ApplyTypeMapping(arguments[0], mapping)),
                    sql.Constant(1));
            case nameof(string.Contains) when arguments.Count == 1 && arguments[0].Type == typeof(string):
            {
                var pattern = sql.ApplyTypeMapping(arguments[0], mapping);
                if (pattern is SqlConstantExpression { Value: string constant })
                {
                    return constant.Length == 0 ? sql.Constant(true) : Like(instance, "%" + Escape(constant) + "%");
                }

                // strpos finds a substring literally; an empty one is found in anything.
                return sql.GreaterThan(Function("strpos", typeof(int), instance, pattern), sql.Constant(0));
            }
            case nameof(string.StartsWith) when arguments.Count == 1 && arguments[0].Type == typeof(string):
            {
                var pattern = sql.ApplyTypeMapping(arguments[0], mapping);
                if (pattern is SqlConstantExpression { Value: string constant })
                {
                    return constant.Length == 0 ? sql.Constant(true) : Like(instance, Escape(constant) + "%");
                }

                return sql.Equal(
                    sql.Function("left", [instance, Function("length", typeof(int), pattern)], true, [true, true], typeof(string), mapping),
                    pattern);
            }
            case nameof(string.EndsWith) when arguments.Count == 1 && arguments[0].Type == typeof(string):
            {
                var pattern = sql.ApplyTypeMapping(arguments[0], mapping);
                if (pattern is SqlConstantExpression { Value: string constant })
                {
                    return constant.Length == 0 ? sql.Constant(true) : Like(instance, "%" + Escape(constant));
                }

                return sql.Equal(
                    sql.Function("right", [instance, Function("length", typeof(int), pattern)], true, [true, true], typeof(string), mapping),
                    pattern);
            }
        }

        return null;
    }

    private SqlExpression Like(SqlExpression match, string pattern) =>
        pattern.Contains('\\') ? sql.Like(match, sql.Constant(pattern), sql.Constant("\\")) : sql.Like(match, sql.Constant(pattern));

    private static string Escape(string literal) => literal.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");
}

public class RasterizedDbMathTranslator(ISqlExpressionFactory sql) : IMethodCallTranslator
{
    private static readonly Dictionary<string, string> Functions = new()
    {
        [nameof(Math.Abs)] = "abs",
        [nameof(Math.Floor)] = "floor",
        [nameof(Math.Ceiling)] = "ceil",
        [nameof(Math.Truncate)] = "trunc",
        [nameof(Math.Sqrt)] = "sqrt",
        [nameof(Math.Pow)] = "power",
        [nameof(Math.Max)] = "greatest",
        [nameof(Math.Min)] = "least",
        [nameof(Math.Round)] = "round",
    };

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if ((method.DeclaringType != typeof(Math) && method.DeclaringType != typeof(MathF)) || !Functions.TryGetValue(method.Name, out var name))
        {
            return null;
        }

        if (arguments.Count is 0 or > 2 || (method.Name == nameof(Math.Round) && arguments.Count == 2 && arguments[1].Type != typeof(int)))
        {
            return null;
        }

        var mapping = ExpressionExtensions.InferTypeMapping([.. arguments.Where(a => a.Type == method.ReturnType)]);
        return sql.Function(
            name,
            arguments.Select(a => a.Type == method.ReturnType ? sql.ApplyTypeMapping(a, mapping) : a),
            nullable: true,
            argumentsPropagateNullability: arguments.Select(_ => true),
            method.ReturnType,
            mapping);
    }
}

/// <summary><c>Guid.NewGuid()</c>, <c>ToString()</c>, and <c>EF.Functions.ILike</c>.</summary>
public class RasterizedDbMiscMethodTranslator(ISqlExpressionFactory sql, IRelationalTypeMappingSource typeMappingSource) : IMethodCallTranslator
{
    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType == typeof(Guid) && method.Name == nameof(Guid.NewGuid))
        {
            return sql.Function("gen_random_uuid", [], nullable: false, argumentsPropagateNullability: [], typeof(Guid));
        }

        if (method.Name == nameof(ToString) && arguments.Count == 0 && instance is not null && instance.Type != typeof(string)
            && (instance.Type.IsPrimitive || instance.Type == typeof(decimal) || instance.Type == typeof(Guid)))
        {
            return sql.Convert(instance, typeof(string), typeMappingSource.FindMapping(typeof(string)));
        }

        if (method.DeclaringType == typeof(RasterizedDbDbFunctionsExtensions) && method.Name == nameof(RasterizedDbDbFunctionsExtensions.ILike))
        {
            // Case-insensitive LIKE, as lower() on both sides: the same rows as ILIKE.
            var mapping = ExpressionExtensions.InferTypeMapping(arguments[1], arguments[2]);
            var match = sql.Function("lower", [sql.ApplyTypeMapping(arguments[1], mapping)], true, [true], typeof(string), mapping);
            var pattern = sql.Function("lower", [sql.ApplyTypeMapping(arguments[2], mapping)], true, [true], typeof(string), mapping);
            return sql.Like(match, pattern);
        }

        return null;
    }
}

public class RasterizedDbMemberTranslator(ISqlExpressionFactory sql) : IMemberTranslator
{
    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var declaring = member.DeclaringType;
        if (declaring == typeof(string) && member.Name == nameof(string.Length) && instance is not null)
        {
            return sql.Convert(sql.Function("length", [instance], true, [true], typeof(long)), typeof(int));
        }

        if (declaring == typeof(DateTime) || declaring == typeof(DateTimeOffset))
        {
            if (instance is null)
            {
                return member.Name switch
                {
                    nameof(DateTime.Now) or nameof(DateTime.UtcNow) => sql.Function("now", [], nullable: false, argumentsPropagateNullability: [], returnType),
                    _ => null,
                };
            }

            var part = member.Name switch
            {
                nameof(DateTime.Year) => "year",
                nameof(DateTime.Month) => "month",
                nameof(DateTime.Day) => "day",
                nameof(DateTime.Hour) => "hour",
                nameof(DateTime.Minute) => "minute",
                nameof(DateTime.Second) => "second",
                _ => null,
            };
            if (part is not null)
            {
                return sql.Convert(
                    sql.Function("date_part", [sql.Constant(part), instance], true, [false, true], typeof(double)),
                    typeof(int));
            }
        }

        return null;
    }
}

/// <summary>Functions for LINQ queries against RasterizedDB.</summary>
public static class RasterizedDbDbFunctionsExtensions
{
    /// <summary>A case-insensitive <c>LIKE</c>.</summary>
    public static bool ILike(this DbFunctions _, string matchExpression, string pattern) =>
        throw new InvalidOperationException("ILike is for LINQ queries; it has no client implementation");
}
