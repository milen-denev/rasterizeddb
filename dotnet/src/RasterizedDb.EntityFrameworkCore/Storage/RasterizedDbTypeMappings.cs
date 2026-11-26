using System.Data;
using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Storage;

/// <summary><c>boolean</c>: its literals are <c>TRUE</c> and <c>FALSE</c>, not 1 and 0.</summary>
public class RasterizedDbBoolTypeMapping : BoolTypeMapping
{
    public RasterizedDbBoolTypeMapping()
        : base("boolean", System.Data.DbType.Boolean)
    {
    }

    protected RasterizedDbBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => (bool)value ? "TRUE" : "FALSE";
}

/// <summary><c>bytea</c>: a hex literal, <c>'\x…'</c>.</summary>
public class RasterizedDbByteArrayTypeMapping : ByteArrayTypeMapping
{
    public RasterizedDbByteArrayTypeMapping()
        : base("bytea", System.Data.DbType.Binary)
    {
    }

    protected RasterizedDbByteArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbByteArrayTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => "'\\x" + Convert.ToHexString((byte[])value).ToLowerInvariant() + "'";
}

/// <summary>
/// <c>double precision</c> and <c>real</c>: the shortest digits that read back as the same value, with
/// the non-finite values quoted.
/// </summary>
public class RasterizedDbDoubleTypeMapping : DoubleTypeMapping
{
    public RasterizedDbDoubleTypeMapping()
        : base("double precision", System.Data.DbType.Double)
    {
    }

    protected RasterizedDbDoubleTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbDoubleTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => Literal(Convert.ToDouble(value, CultureInfo.InvariantCulture), "double precision");

    internal static string Literal(double value, string type)
    {
        if (double.IsNaN(value))
        {
            return $"'NaN'::{type}";
        }

        if (double.IsInfinity(value))
        {
            return (value < 0 ? "'-Infinity'::" : "'Infinity'::") + type;
        }

        var text = value.ToString("R", CultureInfo.InvariantCulture).Replace("E+", "e").Replace("E", "e");
        if (!text.Contains('.') && !text.Contains('e'))
        {
            text += ".0";
        }

        return value < 0 ? $"({text})" : text;
    }
}

public class RasterizedDbFloatTypeMapping : FloatTypeMapping
{
    public RasterizedDbFloatTypeMapping()
        : base("real", System.Data.DbType.Single)
    {
    }

    protected RasterizedDbFloatTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbFloatTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var single = Convert.ToSingle(value, CultureInfo.InvariantCulture);
        if (float.IsNaN(single) || float.IsInfinity(single))
        {
            return RasterizedDbDoubleTypeMapping.Literal(single, "real");
        }

        var text = single.ToString("R", CultureInfo.InvariantCulture).Replace("E+", "e").Replace("E", "e");
        if (!text.Contains('.') && !text.Contains('e'))
        {
            text += ".0";
        }

        return (single < 0 ? $"({text})" : text) + "::real";
    }
}

internal static class DateTimeLiterals
{
    public static string Stamp(DateTime value)
    {
        var text = new StringBuilder(32);
        text.Append(value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        var fraction = value.Ticks % TimeSpan.TicksPerSecond / 10;
        if (fraction != 0)
        {
            text.Append('.').Append(fraction.ToString("000000", CultureInfo.InvariantCulture).TrimEnd('0'));
        }

        return text.ToString();
    }

    public static string Clock(long ticks)
    {
        var time = new TimeSpan(ticks);
        var text = new StringBuilder(16);
        text.Append(CultureInfo.InvariantCulture, $"{(int)time.TotalHours:00}:{time.Minutes:00}:{time.Seconds:00}");
        var fraction = ticks % TimeSpan.TicksPerSecond / 10;
        if (fraction != 0)
        {
            text.Append('.').Append(fraction.ToString("000000", CultureInfo.InvariantCulture).TrimEnd('0'));
        }

        return text.ToString();
    }
}

/// <summary>
/// <c>timestamp with time zone</c> for a <see cref="DateTime"/>: an instant. A UTC or local value is
/// written in UTC; an unspecified one is taken to be UTC already. It reads back as UTC.
/// </summary>
public class RasterizedDbTimestampTzTypeMapping : DateTimeTypeMapping
{
    public RasterizedDbTimestampTzTypeMapping()
        : base("timestamp with time zone", System.Data.DbType.DateTimeOffset)
    {
    }

    protected RasterizedDbTimestampTzTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbTimestampTzTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var stamp = (DateTime)value;
        if (stamp.Kind == DateTimeKind.Local)
        {
            stamp = stamp.ToUniversalTime();
        }

        return $"TIMESTAMPTZ '{DateTimeLiterals.Stamp(stamp)}+00'";
    }
}

/// <summary><c>timestamp without time zone</c>: a wall-clock time, written and read with no zone.</summary>
public class RasterizedDbTimestampTypeMapping : DateTimeTypeMapping
{
    public RasterizedDbTimestampTypeMapping()
        : base("timestamp without time zone", System.Data.DbType.DateTime2)
    {
    }

    protected RasterizedDbTimestampTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbTimestampTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => $"TIMESTAMP '{DateTimeLiterals.Stamp((DateTime)value)}'";
}

public class RasterizedDbDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
{
    public RasterizedDbDateTimeOffsetTypeMapping()
        : base("timestamp with time zone", System.Data.DbType.DateTimeOffset)
    {
    }

    protected RasterizedDbDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbDateTimeOffsetTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => $"TIMESTAMPTZ '{DateTimeLiterals.Stamp(((DateTimeOffset)value).UtcDateTime)}+00'";
}

public class RasterizedDbDateOnlyTypeMapping : DateOnlyTypeMapping
{
    public RasterizedDbDateOnlyTypeMapping()
        : base("date", System.Data.DbType.Date)
    {
    }

    protected RasterizedDbDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbDateOnlyTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => $"DATE '{(DateOnly)value:yyyy-MM-dd}'";
}

public class RasterizedDbTimeOnlyTypeMapping : TimeOnlyTypeMapping
{
    public RasterizedDbTimeOnlyTypeMapping()
        : base("time without time zone", System.Data.DbType.Time)
    {
    }

    protected RasterizedDbTimeOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbTimeOnlyTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) => $"TIME '{DateTimeLiterals.Clock(((TimeOnly)value).Ticks)}'";
}

/// <summary><c>interval</c> for a <see cref="TimeSpan"/>.</summary>
public class RasterizedDbIntervalTypeMapping : TimeSpanTypeMapping
{
    public RasterizedDbIntervalTypeMapping()
        : base("interval", dbType: null)
    {
    }

    protected RasterizedDbIntervalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbIntervalTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) =>
        $"INTERVAL '{RasterizedDb.Data.RasterizedDbInterval.FromTimeSpan((TimeSpan)value)}'";
}

/// <summary>Text types: <c>text</c>, <c>character varying(n)</c>, <c>character(n)</c>, and the JSON types read as their text.</summary>
public class RasterizedDbStringTypeMapping : StringTypeMapping
{
    public RasterizedDbStringTypeMapping(string storeType, int? size = null, bool fixedLength = false)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(string), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonStringReaderWriter.Instance),
                storeType,
                size is null ? StoreTypePostfix.None : StoreTypePostfix.Size,
                System.Data.DbType.String,
                unicode: true,
                size: size,
                fixedLength: fixedLength))
    {
    }

    protected RasterizedDbStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbStringTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var text = (string)value;
        if (text.Contains('\0'))
        {
            throw new InvalidOperationException("a text value cannot contain a NUL character");
        }

        return "'" + text.Replace("'", "''") + "'";
    }
}

public class RasterizedDbDecimalTypeMapping : DecimalTypeMapping
{
    public RasterizedDbDecimalTypeMapping(int? precision = null, int? scale = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(decimal), jsonValueReaderWriter: Microsoft.EntityFrameworkCore.Storage.Json.JsonDecimalReaderWriter.Instance),
                "numeric",
                precision is null ? StoreTypePostfix.None : StoreTypePostfix.PrecisionAndScale,
                System.Data.DbType.Decimal,
                precision: precision,
                scale: scale))
    {
    }

    protected RasterizedDbDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => new RasterizedDbDecimalTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // A constant EF Core made of another numeric type (the 0 of COALESCE(SUM(x), 0)) arrives as that type.
        var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        var text = number.ToString(CultureInfo.InvariantCulture);
        return number < 0 ? $"({text})" : text;
    }
}

/// <summary>Maps CLR types to RasterizedDB's (PostgreSQL's) column types.</summary>
public class RasterizedDbTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly RasterizedDbBoolTypeMapping Bool = new();
    private static readonly ShortTypeMapping Short = new("smallint", DbType.Int16);
    private static readonly IntTypeMapping Int = new("integer", DbType.Int32);
    private static readonly LongTypeMapping Long = new("bigint", DbType.Int64);
    private static readonly ByteTypeMapping Byte = new("smallint", DbType.Byte);
    private static readonly RasterizedDbFloatTypeMapping Float = new();
    private static readonly RasterizedDbDoubleTypeMapping Double = new();
    private static readonly RasterizedDbDecimalTypeMapping Decimal = new();
    private static readonly RasterizedDbStringTypeMapping Text = new("text");
    private static readonly RasterizedDbStringTypeMapping Jsonb = new("jsonb");
    private static readonly RasterizedDbStringTypeMapping Json = new("json");
    private static readonly CharTypeMapping Char = new("character(1)", DbType.String);
    private static readonly RasterizedDbByteArrayTypeMapping Bytea = new();
    private static readonly GuidTypeMapping Uuid = new("uuid", DbType.Guid);
    private static readonly RasterizedDbTimestampTzTypeMapping TimestampTz = new();
    private static readonly RasterizedDbTimestampTypeMapping Timestamp = new();
    private static readonly RasterizedDbDateTimeOffsetTypeMapping TimestampTzOffset = new();
    private static readonly RasterizedDbDateOnlyTypeMapping Date = new();
    private static readonly RasterizedDbTimeOnlyTypeMapping Time = new();
    private static readonly RasterizedDbIntervalTypeMapping Interval = new();

    private static readonly Dictionary<Type, RelationalTypeMapping> ByClrType = new()
    {
        [typeof(bool)] = Bool,
        [typeof(short)] = Short,
        [typeof(int)] = Int,
        [typeof(long)] = Long,
        [typeof(byte)] = Byte,
        [typeof(float)] = Float,
        [typeof(double)] = Double,
        [typeof(decimal)] = Decimal,
        [typeof(string)] = Text,
        [typeof(char)] = Char,
        [typeof(byte[])] = Bytea,
        [typeof(Guid)] = Uuid,
        [typeof(DateTime)] = TimestampTz,
        [typeof(DateTimeOffset)] = TimestampTzOffset,
        [typeof(DateOnly)] = Date,
        [typeof(TimeOnly)] = Time,
        [typeof(TimeSpan)] = Interval,
    };

    // The first mapping of each list is the one a column of that type gets when no CLR type is asked for.
    private static readonly Dictionary<string, RelationalTypeMapping[]> ByStoreType = new(StringComparer.OrdinalIgnoreCase)
    {
        ["boolean"] = [Bool],
        ["bool"] = [Bool],
        ["smallint"] = [Short, Byte],
        ["int2"] = [Short, Byte],
        ["smallserial"] = [Short],
        ["integer"] = [Int],
        ["int"] = [Int],
        ["int4"] = [Int],
        ["serial"] = [Int],
        ["bigint"] = [Long],
        ["int8"] = [Long],
        ["bigserial"] = [Long],
        ["real"] = [Float],
        ["float4"] = [Float],
        ["double precision"] = [Double],
        ["float8"] = [Double],
        ["float"] = [Double],
        ["numeric"] = [Decimal],
        ["decimal"] = [Decimal],
        ["text"] = [Text],
        ["character varying"] = [Text],
        ["varchar"] = [Text],
        ["character"] = [Text, Char],
        ["char"] = [Text, Char],
        ["bpchar"] = [Text, Char],
        ["jsonb"] = [Jsonb],
        ["json"] = [Json],
        ["bytea"] = [Bytea],
        ["uuid"] = [Uuid],
        ["timestamp with time zone"] = [TimestampTz, TimestampTzOffset],
        ["timestamptz"] = [TimestampTz, TimestampTzOffset],
        ["timestamp without time zone"] = [Timestamp],
        ["timestamp"] = [Timestamp],
        ["date"] = [Date],
        ["time without time zone"] = [Time],
        ["time"] = [Time],
        ["interval"] = [Interval],
    };

    public RasterizedDbTypeMappingSource(TypeMappingSourceDependencies dependencies, RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    private static readonly RasterizedDbJsonTypeMapping JsonContainer = new();

    private static string StoreBase(string storeType)
    {
        var paren = storeType.IndexOf('(');
        return (paren < 0 ? storeType : storeType[..paren]).Trim();
    }

    /// <summary>
    /// The mapping of a value in a query (a parameter, a constant). A list there is one EF Core expands
    /// into its elements, so it is told what an element is.
    /// </summary>
    public override RelationalTypeMapping? FindMapping(Type type, IModel model, CoreTypeMapping? elementMapping = null)
    {
        if (RasterizedDbArrayTypeMapping.ElementType(type) is { } elementType
            && (elementMapping as RelationalTypeMapping ?? FindMapping(elementType, model)) is { } element)
        {
            return new RasterizedDbArrayTypeMapping(type, element, queryable: true);
        }

        return base.FindMapping(type, model, elementMapping);
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeType = mappingInfo.StoreTypeName;
        // The column an owned type mapped with ToJson() lives in.
#if EF10
        if (clrType == typeof(JsonTypePlaceholder) || clrType == typeof(System.Text.Json.JsonElement))
#else
        if (clrType == typeof(System.Text.Json.JsonElement))
#endif
        {
            return storeType is null || storeType.Equals("jsonb", StringComparison.OrdinalIgnoreCase) ? JsonContainer : new RasterizedDbJsonTypeMapping(storeType);
        }

        if (storeType is not null && storeType.EndsWith("[]", StringComparison.Ordinal))
        {
            // integer[], text[]: the element's mapping, as an array of it.
            var elementClr = clrType is null ? null : RasterizedDbArrayTypeMapping.ElementType(clrType);
            if (clrType is not null && elementClr is null)
            {
                return null;
            }

            var element = FindMapping(new RelationalTypeMappingInfo(elementClr, storeTypeName: storeType[..^2], storeTypeNameBase: StoreBase(storeType[..^2])));
            return element is null ? null : new RasterizedDbArrayTypeMapping(clrType ?? element.ClrType.MakeArrayType(), element);
        }

        if (storeType is null && clrType is not null && RasterizedDbArrayTypeMapping.ElementType(clrType) is { } elementType
            && FindMapping(new RelationalTypeMappingInfo(elementType)) is { } elementMapping)
        {
            return new RasterizedDbArrayTypeMapping(clrType, elementMapping);
        }

        if (storeType is not null)
        {
            var baseName = mappingInfo.StoreTypeNameBase ?? storeType;
            if (ByStoreType.TryGetValue(baseName, out var candidates))
            {
                RelationalTypeMapping? chosen = null;
                foreach (var candidate in candidates)
                {
                    if (clrType is null || candidate.ClrType == clrType)
                    {
                        chosen = candidate;
                        break;
                    }
                }

                if (chosen is null)
                {
                    return null;
                }

                // The column's type as written (varchar(40), numeric(18,2)) is what migrations emit.
                return chosen switch
                {
                    RasterizedDbStringTypeMapping => new RasterizedDbStringTypeMapping(
                        storeType, null, baseName.StartsWith("char", StringComparison.OrdinalIgnoreCase) || baseName.Equals("bpchar", StringComparison.OrdinalIgnoreCase)),
                    RasterizedDbDecimalTypeMapping when mappingInfo.Precision is not null =>
                        new RasterizedDbDecimalTypeMapping(mappingInfo.Precision, mappingInfo.Scale ?? 0),
                    _ => chosen.StoreType.Equals(storeType, StringComparison.OrdinalIgnoreCase) ? chosen : chosen.WithStoreTypeAndSize(storeType, null),
                };
            }

            // A type this provider does not know by name (a domain, citext): the CLR type's mapping, under that name.
            if (clrType is not null && ByClrType.TryGetValue(clrType, out var fallback))
            {
                return fallback is RasterizedDbStringTypeMapping ? new RasterizedDbStringTypeMapping(storeType) : fallback.WithStoreTypeAndSize(storeType, null);
            }

            return null;
        }

        if (clrType is null)
        {
            return null;
        }

        if (clrType == typeof(string))
        {
            if (mappingInfo.Size is { } size)
            {
                return mappingInfo.IsFixedLength == true
                    ? new RasterizedDbStringTypeMapping("character", size, fixedLength: true)
                    : new RasterizedDbStringTypeMapping("character varying", size);
            }

            return Text;
        }

        if (clrType == typeof(decimal) && mappingInfo.Precision is { } precision)
        {
            return new RasterizedDbDecimalTypeMapping(precision, mappingInfo.Scale ?? 0);
        }

        return ByClrType.GetValueOrDefault(clrType);
    }
}
