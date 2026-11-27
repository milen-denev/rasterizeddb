using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Query;

/// <summary>One element of an array column: <c>array[index]</c>, the index counted from 1 as SQL counts it.</summary>
public class RasterizedDbArrayIndexExpression : SqlExpression
{
    private static readonly ConstructorInfo Constructor = typeof(RasterizedDbArrayIndexExpression).GetConstructor(
        [typeof(SqlExpression), typeof(SqlExpression), typeof(Type), typeof(RelationalTypeMapping)])!;

    public RasterizedDbArrayIndexExpression(SqlExpression array, SqlExpression index, Type type, RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Array = array;
        Index = index;
    }

    public virtual SqlExpression Array { get; }

    public virtual SqlExpression Index { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor) =>
        Update((SqlExpression)visitor.Visit(Array), (SqlExpression)visitor.Visit(Index));

    public virtual RasterizedDbArrayIndexExpression Update(SqlExpression array, SqlExpression index) =>
        ReferenceEquals(array, Array) && ReferenceEquals(index, Index) ? this : new RasterizedDbArrayIndexExpression(array, index, Type, TypeMapping);

    public override Expression Quote() =>
        New(Constructor, Array.Quote(), Index.Quote(), Constant(Type), RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Array);
        expressionPrinter.Append("[");
        expressionPrinter.Visit(Index);
        expressionPrinter.Append("]");
    }

    public override bool Equals(object? obj) =>
        obj is RasterizedDbArrayIndexExpression other && base.Equals(other) && Array.Equals(other.Array) && Index.Equals(other.Index);

    public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Array, Index);
}

/// <summary>
/// Tells EF Core's NULL reasoning about the provider's own expressions: an array element may be NULL,
/// and a regular-expression match is NULL when either side is.
/// </summary>
public class RasterizedDbSqlNullabilityProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters)
    : SqlNullabilityProcessor(dependencies, parameters)
{
    protected override SqlExpression VisitCustomSqlExpression(SqlExpression sqlExpression, bool allowOptimizedExpansion, out bool nullable)
    {
        if (sqlExpression is RasterizedDbArrayIndexExpression index)
        {
            var array = Visit(index.Array, out _);
            var position = Visit(index.Index, out _);
            // Past the array's end, or a NULL element.
            nullable = true;
            return index.Update(array, position);
        }

        if (sqlExpression is RasterizedDbRegexMatchExpression regex)
        {
            var match = Visit(regex.Match, out var matchNullable);
            var pattern = Visit(regex.Pattern, out var patternNullable);
            nullable = matchNullable || patternNullable;
            return regex.Update(match, pattern);
        }

        return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
    }
}

public class RasterizedDbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters)
    : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
#if EF10
    protected override Expression ProcessSqlNullability(Expression queryExpression, ParametersCacheDecorator decorator) =>
        new RasterizedDbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, decorator);
#else
    protected override Expression ProcessSqlNullability(Expression queryExpression, IReadOnlyDictionary<string, object?> parametersValues, out bool canCache) =>
        new RasterizedDbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersValues, out canCache);
#endif
}

public class RasterizedDbParameterBasedSqlProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
    : IRelationalParameterBasedSqlProcessorFactory
{
    public virtual RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters) =>
        new RasterizedDbParameterBasedSqlProcessor(dependencies, parameters);
}
