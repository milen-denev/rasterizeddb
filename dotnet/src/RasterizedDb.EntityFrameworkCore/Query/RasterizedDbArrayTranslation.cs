using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using RasterizedDb.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Query;

/// <summary>
/// LINQ over an array column: <c>tags.Contains(x)</c> is <c>x = ANY (tags)</c>, <c>tags.Length</c>,
/// <c>Count</c> and <c>Count()</c> are <c>cardinality(tags)</c>, and <c>tags.Any()</c> asks whether that
/// is above zero.
/// </summary>
/// <remarks>
/// A list held in a parameter needs none of this: EF Core expands it into <c>IN (…)</c> itself.
/// </remarks>
public class RasterizedDbArrayTranslator(ISqlExpressionFactory sql) : IMethodCallTranslator, IMemberTranslator
{
    internal static bool IsArrayColumn(SqlExpression? expression) =>
        expression is { TypeMapping: RasterizedDbArrayTypeMapping } and not SqlParameterExpression and not SqlConstantExpression;

    internal static SqlExpression Cardinality(ISqlExpressionFactory sql, SqlExpression array) =>
        sql.Convert(sql.Function("cardinality", [array], nullable: true, argumentsPropagateNullability: [true], typeof(long)), typeof(int));

    private SqlExpression Any(SqlExpression item, SqlExpression array)
    {
        item = sql.ApplyTypeMapping(item, ((RasterizedDbArrayTypeMapping)array.TypeMapping!).Element);
        // `item = ANY (array)`: ANY is written as a function of the array, which is how it reads.
        return sql.Equal(item, sql.Function("ANY", [array], nullable: true, argumentsPropagateNullability: [true], item.Type, item.TypeMapping));
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType == typeof(Enumerable) && arguments.Count >= 1 && IsArrayColumn(arguments[0]))
        {
            switch (method.Name)
            {
                case nameof(Enumerable.Contains) when arguments.Count == 2:
                    return Any(arguments[1], arguments[0]);
                case nameof(Enumerable.Count) when arguments.Count == 1:
                    return Cardinality(sql, arguments[0]);
                case nameof(Enumerable.LongCount) when arguments.Count == 1:
                    return sql.Function("cardinality", [arguments[0]], true, [true], typeof(long));
                case nameof(Enumerable.Any) when arguments.Count == 1:
                    return sql.GreaterThan(Cardinality(sql, arguments[0]), sql.Constant(0));
            }

            return null;
        }

        // List<T>.Contains, ICollection<T>.Contains.
        if (instance is not null && IsArrayColumn(instance) && method.Name == nameof(IList.Contains) && arguments.Count == 1)
        {
            return Any(arguments[0], instance);
        }

        return null;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger) =>
        instance is not null && IsArrayColumn(instance) && member.Name is nameof(ICollection.Count) or nameof(Array.Length)
            ? Cardinality(sql, instance)
            : null;
}

public class RasterizedDbSqlTranslatingExpressionVisitorFactory(RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public virtual RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor) =>
        new RasterizedDbSqlTranslatingExpressionVisitor(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor);
}

/// <summary>Adds <c>array.Length</c>, which reaches the translator as an expression of its own and not as a member.</summary>
public class RasterizedDbSqlTranslatingExpressionVisitor(
    RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext,
    QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    : RelationalSqlTranslatingExpressionVisitor(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
{
    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        if (unaryExpression.NodeType == ExpressionType.ArrayLength && unaryExpression.Operand.Type != typeof(byte[])
            && Visit(unaryExpression.Operand) is SqlExpression array && RasterizedDbArrayTranslator.IsArrayColumn(array))
        {
            return RasterizedDbArrayTranslator.Cardinality(Dependencies.SqlExpressionFactory, array);
        }

        return base.VisitUnary(unaryExpression);
    }
}
