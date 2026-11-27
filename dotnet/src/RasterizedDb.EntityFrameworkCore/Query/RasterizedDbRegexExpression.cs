using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Query;

/// <summary>
/// A regular-expression match, <c>match ~ pattern</c> (or <c>~*</c>, ignoring case): what
/// <c>Regex.IsMatch</c> becomes. The pattern is read by the server, as PostgreSQL reads one.
/// </summary>
public class RasterizedDbRegexMatchExpression : SqlExpression
{
    private static readonly ConstructorInfo Constructor = typeof(RasterizedDbRegexMatchExpression).GetConstructor(
        [typeof(SqlExpression), typeof(SqlExpression), typeof(bool), typeof(RelationalTypeMapping)])!;

    public RasterizedDbRegexMatchExpression(SqlExpression match, SqlExpression pattern, bool ignoreCase, RelationalTypeMapping? typeMapping)
        : base(typeof(bool), typeMapping)
    {
        Match = match;
        Pattern = pattern;
        IgnoreCase = ignoreCase;
    }

    public virtual SqlExpression Match { get; }

    public virtual SqlExpression Pattern { get; }

    public virtual bool IgnoreCase { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor) =>
        Update((SqlExpression)visitor.Visit(Match), (SqlExpression)visitor.Visit(Pattern));

    public virtual RasterizedDbRegexMatchExpression Update(SqlExpression match, SqlExpression pattern) =>
        ReferenceEquals(match, Match) && ReferenceEquals(pattern, Pattern) ? this : new RasterizedDbRegexMatchExpression(match, pattern, IgnoreCase, TypeMapping);

    public override Expression Quote() =>
        New(Constructor, Match.Quote(), Pattern.Quote(), Constant(IgnoreCase), RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Match);
        expressionPrinter.Append(IgnoreCase ? " ~* " : " ~ ");
        expressionPrinter.Visit(Pattern);
    }

    public override bool Equals(object? obj) =>
        obj is RasterizedDbRegexMatchExpression other && base.Equals(other) && IgnoreCase == other.IgnoreCase && Match.Equals(other.Match) && Pattern.Equals(other.Pattern);

    public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Match, Pattern, IgnoreCase);
}
