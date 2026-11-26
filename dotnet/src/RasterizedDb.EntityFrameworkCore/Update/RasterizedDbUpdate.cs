using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace RasterizedDb.EntityFrameworkCore.Update;

/// <summary>
/// INSERT, UPDATE and DELETE with <c>RETURNING</c>: generated values come back from the statement that
/// wrote them, and an UPDATE or DELETE returns a row per row it changed, which is how a concurrency
/// conflict is seen.
/// </summary>
public class RasterizedDbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : UpdateSqlGenerator(dependencies)
{
    /// <summary>
    /// A change inside a JSON column is written in place: <c>jsonb_set(col, '{a,b,0}', value)</c>, the
    /// path being EF Core's <c>$.a.b[0]</c>. A change to the whole document is the column's new value.
    /// </summary>
    protected override void AppendUpdateColumnValue(
        ISqlGenerationHelper updateSqlGeneratorHelper,
        IColumnModification columnModification,
        StringBuilder stringBuilder,
        string name,
        string? schema)
    {
        if (columnModification.JsonPath is null or "$")
        {
            base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);
            return;
        }

        stringBuilder.Append("jsonb_set(").Append(updateSqlGeneratorHelper.DelimitIdentifier(columnModification.ColumnName)).Append(", '{");
        AppendPath(stringBuilder, columnModification.JsonPath);
        stringBuilder.Append("}', ");
        if (columnModification.Value is null or DBNull)
        {
            stringBuilder.Append("'null'::jsonb");
        }
        else if (columnModification.Property is { } property)
        {
            // One scalar: its SQL value, typed, made into JSON.
            stringBuilder.Append("to_jsonb(CAST(");
            base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);
            stringBuilder.Append(" AS ").Append(property.GetRelationalTypeMapping().StoreType).Append("))");
        }
        else
        {
            // A sub-document, already JSON text.
            stringBuilder.Append("CAST(");
            base.AppendUpdateColumnValue(updateSqlGeneratorHelper, columnModification, stringBuilder, name, schema);
            stringBuilder.Append(" AS jsonb)");
        }

        stringBuilder.Append(')');
    }

    /// <summary><c>$.a.b[0]</c> as the elements of a PostgreSQL path array: <c>a,b,0</c>.</summary>
    private static void AppendPath(StringBuilder builder, string jsonPath)
    {
        var first = true;
        var at = jsonPath.StartsWith('$') ? 1 : 0;
        while (at < jsonPath.Length)
        {
            var c = jsonPath[at];
            if (c is '.' or '[')
            {
                at++;
                continue;
            }

            var end = at;
            while (end < jsonPath.Length && jsonPath[end] is not ('.' or '[' or ']'))
            {
                end++;
            }

            if (!first)
            {
                builder.Append(',');
            }

            first = false;
            var element = jsonPath[at..end];
            // Quoted inside the array literal, whose own quotes are the statement's.
            builder.Append('"').Append(element.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("'", "''")).Append('"');
            at = end < jsonPath.Length && jsonPath[end] == ']' ? end + 1 : end;
        }
    }
}

/// <summary>
/// One statement per request: the native protocol runs one statement per request, so each command of a
/// <c>SaveChanges</c> is its own round trip, inside one transaction when there are several.
/// </summary>
public class RasterizedDbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies) : IModificationCommandBatchFactory
{
    public virtual ModificationCommandBatch Create() => new SingularModificationCommandBatch(dependencies);
}
