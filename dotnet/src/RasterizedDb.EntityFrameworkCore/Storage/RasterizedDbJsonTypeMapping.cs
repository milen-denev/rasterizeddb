using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Storage;

/// <summary>
/// The <c>jsonb</c> column an owned type mapped with <c>ToJson()</c> lives in. It is read as its JSON
/// text, which EF Core then walks.
/// </summary>
public class RasterizedDbJsonTypeMapping : JsonTypeMapping
{
    private static readonly MethodInfo GetStringMethod = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)])!;
    private static readonly PropertyInfo Utf8Property = typeof(Encoding).GetProperty(nameof(Encoding.UTF8))!;
    private static readonly MethodInfo GetBytesMethod = typeof(Encoding).GetMethod(nameof(Encoding.GetBytes), [typeof(string)])!;
    private static readonly ConstructorInfo MemoryStreamConstructor = typeof(MemoryStream).GetConstructor([typeof(byte[])])!;

    public RasterizedDbJsonTypeMapping(string storeType = "jsonb")
        : base(storeType, typeof(JsonElement), System.Data.DbType.String)
    {
    }

    protected RasterizedDbJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbJsonTypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod() => GetStringMethod;

    /// <summary>The column's text, as the UTF-8 stream EF Core's JSON reader takes.</summary>
    public override Expression CustomizeDataReaderExpression(Expression expression) =>
        Expression.New(MemoryStreamConstructor, Expression.Call(Expression.Property(null, Utf8Property), GetBytesMethod, expression));

    protected override string GenerateNonNullSqlLiteral(object value) => "'" + ((string)value).Replace("'", "''") + "'";
}
