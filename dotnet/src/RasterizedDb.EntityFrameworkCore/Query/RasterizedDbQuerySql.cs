using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using RasterizedDb.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Query;

public class RasterizedDbQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies) : IQuerySqlGeneratorFactory
{
    public virtual QuerySqlGenerator Create() => new RasterizedDbQuerySqlGenerator(dependencies);
}

/// <summary>SELECT statements in the dialect RasterizedDB reads: PostgreSQL's.</summary>
public class RasterizedDbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : QuerySqlGenerator(dependencies)
{
    protected override void GenerateRootCommand(Expression queryExpression)
    {
        // Raw SQL that is the whole query is read by column name, which only the text format carries.
        if (queryExpression is SelectExpression select && select.IsNonComposedFromSql() && Sql is RasterizedDbCommandBuilder builder)
        {
            builder.Text = true;
        }

        base.GenerateRootCommand(queryExpression);
    }

    protected override void GenerateTop(SelectExpression selectExpression)
    {
        // LIMIT, after the ORDER BY.
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset is not null)
        {
            if (selectExpression.Limit is null)
            {
                Sql.AppendLine();
            }
            else
            {
                Sql.Append(" ");
            }

            Sql.Append("OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression) =>
        binaryExpression.OperatorType == ExpressionType.Add && binaryExpression.Type == typeof(string)
            ? " || "
            : base.GetOperator(binaryExpression);

    protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
    {
        Sql.Append("JOIN LATERAL ");
        Visit(crossApplyExpression.Table);
        Sql.Append(" ON TRUE");
        return crossApplyExpression;
    }

    protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
    {
        Sql.Append("LEFT JOIN LATERAL ");
        Visit(outerApplyExpression.Table);
        Sql.Append(" ON TRUE");
        return outerApplyExpression;
    }

    protected override void GenerateSetOperationOperand(SetOperationBase setOperation, SelectExpression operand)
    {
        // An operand with its own ORDER BY or LIMIT has to stand in parentheses.
        if (operand.Orderings.Count > 0 || operand.Limit is not null || operand.Offset is not null)
        {
            Sql.AppendLine("(");
            using (Sql.Indent())
            {
                Visit(operand);
            }

            Sql.AppendLine().Append(")");
            return;
        }

        base.GenerateSetOperationOperand(setOperation, operand);
    }
}
