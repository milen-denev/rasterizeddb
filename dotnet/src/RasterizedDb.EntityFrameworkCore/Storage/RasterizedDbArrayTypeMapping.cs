using System.Collections;
using System.Text;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Storage;

/// <summary>
/// An array column (<c>integer[]</c>, <c>text[]</c>, …) for a CLR array or list of a mapped type. The
/// value travels as an array literal and reads back element by element.
/// </summary>
public class RasterizedDbArrayTypeMapping : RelationalTypeMapping
{
    public RasterizedDbArrayTypeMapping(Type clrType, RelationalTypeMapping elementMapping)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(clrType, comparer: Comparer(clrType, elementMapping), elementMapping: elementMapping),
                elementMapping.StoreType + "[]"))
    {
    }

    protected RasterizedDbArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbArrayTypeMapping(parameters);

    /// <summary>The element type of an array or list type this mapping can hold; null for anything else.</summary>
    public static Type? ElementType(Type type)
    {
        if (type.IsArray)
        {
            return type.GetArrayRank() == 1 && type != typeof(byte[]) ? type.GetElementType() : null;
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            if (definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(IReadOnlyList<>)
                || definition == typeof(ICollection<>) || definition == typeof(IReadOnlyCollection<>) || definition == typeof(IEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }
        }

        return null;
    }

    private static ValueComparer? Comparer(Type clrType, RelationalTypeMapping elementMapping)
    {
        var element = ElementType(clrType)!;
        var underlying = Nullable.GetUnderlyingType(element);
        var comparerType = underlying is not null
            ? typeof(ListOfNullableValueTypesComparer<,>).MakeGenericType(clrType, underlying)
            : element.IsValueType
                ? typeof(ListOfValueTypesComparer<,>).MakeGenericType(clrType, element)
                : typeof(ListOfReferenceTypesComparer<,>).MakeGenericType(clrType, element);
        try
        {
            return (ValueComparer?)Activator.CreateInstance(comparerType, elementMapping.Comparer);
        }
        catch (Exception)
        {
            // An interface type the list comparers cannot build: the default comparer, by reference.
            return null;
        }
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var element = (RelationalTypeMapping)ElementTypeMapping!;
        var body = new StringBuilder("{");
        var first = true;
        foreach (var item in (IEnumerable)value)
        {
            if (!first)
            {
                body.Append(',');
            }

            first = false;
            if (item is null)
            {
                body.Append("NULL");
                continue;
            }

            // The element's own literal, without its quotes or type prefix, quoted as an array element.
            var literal = element.GenerateSqlLiteral(item);
            var open = literal.IndexOf('\'');
            if (open >= 0 && literal.EndsWith('\''))
            {
                var text = literal[(open + 1)..^1].Replace("''", "'");
                body.Append('"').Append(text.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
            }
            else
            {
                body.Append(literal.Trim('(', ')').Replace("::real", ""));
            }
        }

        return "'" + body.Append('}').ToString().Replace("'", "''") + "'";
    }
}
