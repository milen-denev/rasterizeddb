using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// A statement's value. The native protocol has no Bind message, so a value reaches the server as the
/// SQL literal for it, written by this driver under the server's own quoting rules: it can never become
/// syntax. A placeholder is <c>$1</c>…<c>$n</c> (by position in the collection) or <c>@name</c>.
/// </summary>
public class RasterizedDbParameter : DbParameter
{
    private string _name = string.Empty;
    private string _sourceColumn = string.Empty;
    private object? _value;
    private DbType? _dbType;

    public RasterizedDbParameter()
    {
    }

    public RasterizedDbParameter(string parameterName, object? value)
    {
        ParameterName = parameterName;
        _value = value;
    }

    public RasterizedDbParameter(object? value)
    {
        _value = value;
    }

    [AllowNull]
    public override string ParameterName
    {
        get => _name;
        set
        {
            _name = value ?? string.Empty;
            TrimmedName = _name.Length > 0 && _name[0] is '@' or ':' or '$' ? _name[1..] : _name;
        }
    }

    /// <summary>The name without its <c>@</c>.</summary>
    internal string TrimmedName { get; private set; } = string.Empty;

    public override object? Value
    {
        get => _value;
        set => _value = value;
    }

    public override DbType DbType
    {
        get => _dbType ?? Infer();
        set => _dbType = value;
    }

    private DbType Infer() => _value switch
    {
        int => DbType.Int32,
        long => DbType.Int64,
        short => DbType.Int16,
        bool => DbType.Boolean,
        decimal => DbType.Decimal,
        double => DbType.Double,
        float => DbType.Single,
        Guid => DbType.Guid,
        DateTime => DbType.DateTime2,
        DateTimeOffset => DbType.DateTimeOffset,
        DateOnly => DbType.Date,
        TimeOnly or TimeSpan => DbType.Time,
        byte[] => DbType.Binary,
        byte => DbType.Byte,
        _ => DbType.String,
    };

    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
            {
                throw new NotSupportedException("RasterizedDB statements take input parameters only");
            }
        }
    }

    public override bool IsNullable { get; set; }

    public override int Size { get; set; }

    public override byte Precision { get; set; }

    public override byte Scale { get; set; }

    [AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    public override bool SourceColumnNullMapping { get; set; }

    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

    public override void ResetDbType() => _dbType = null;

    /// <summary>Writes the value as an SQL literal. <see cref="DbType"/>, when set, picks among the forms a CLR type has.</summary>
    internal virtual void WriteLiteral(ByteWriter writer)
    {
        var value = _value;
        if (_dbType is { } type && value is not null and not DBNull)
        {
            switch (type)
            {
                case DbType.Date when value is DateTime date:
                    LiteralWriter.WriteDate(writer, DateOnly.FromDateTime(date));
                    return;
                case DbType.Time when value is DateTime time:
                    LiteralWriter.WriteTime(writer, TimeOnly.FromDateTime(time));
                    return;
                case DbType.Time when value is TimeSpan span && span >= TimeSpan.Zero && span < TimeSpan.FromDays(1):
                    LiteralWriter.WriteTime(writer, TimeOnly.FromTimeSpan(span));
                    return;
                case DbType.DateTime or DbType.DateTime2 when value is DateTime { Kind: not DateTimeKind.Unspecified } stamp:
                    // A timestamp without time zone: the wall-clock time, with no zone written.
                    LiteralWriter.WriteDateTime(writer, DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified));
                    return;
                case DbType.DateTimeOffset when value is DateTime { Kind: DateTimeKind.Unspecified } instant:
                    LiteralWriter.WriteDateTime(writer, DateTime.SpecifyKind(instant, DateTimeKind.Utc));
                    return;
            }
        }

        LiteralWriter.WriteObject(writer, value);
    }

    internal RasterizedDbParameter CloneParameter() => (RasterizedDbParameter)MemberwiseClone();
}

/// <summary>A parameter whose value is held unboxed: nothing is allocated to send an <c>int</c> or a <c>Guid</c>.</summary>
public sealed class RasterizedDbParameter<T> : RasterizedDbParameter
{
    public RasterizedDbParameter()
    {
    }

    public RasterizedDbParameter(string parameterName, T value)
    {
        ParameterName = parameterName;
        TypedValue = value;
    }

    public RasterizedDbParameter(T value)
    {
        TypedValue = value;
    }

    public T? TypedValue { get; set; }

    public override object? Value
    {
        get => TypedValue;
        set => TypedValue = value is null or DBNull ? default : (T)value;
    }

    internal override void WriteLiteral(ByteWriter writer)
    {
        if (TypedValue is null)
        {
            writer.Write("NULL"u8);
        }
        else
        {
            LiteralWriter.Write(writer, TypedValue);
        }
    }
}

public sealed class RasterizedDbParameterCollection : DbParameterCollection
{
    private readonly List<RasterizedDbParameter> _items = [];

    internal List<RasterizedDbParameter> Items => _items;

    public override int Count => _items.Count;

    public override object SyncRoot => ((ICollection)_items).SyncRoot;

    public new RasterizedDbParameter this[int index]
    {
        get => _items[index];
        set => _items[index] = value;
    }

    public new RasterizedDbParameter this[string parameterName]
    {
        get => _items[Require(parameterName)];
        set => _items[Require(parameterName)] = value;
    }

    public RasterizedDbParameter Add(RasterizedDbParameter parameter)
    {
        _items.Add(parameter);
        return parameter;
    }

    public RasterizedDbParameter AddWithValue(string parameterName, object? value) => Add(new RasterizedDbParameter(parameterName, value));

    /// <summary>A positional value, for <c>$n</c>.</summary>
    public RasterizedDbParameter AddWithValue(object? value) => Add(new RasterizedDbParameter(value));

    public RasterizedDbParameter<T> Add<T>(string parameterName, T value)
    {
        var parameter = new RasterizedDbParameter<T>(parameterName, value);
        _items.Add(parameter);
        return parameter;
    }

    public override int Add(object value)
    {
        _items.Add(Cast(value));
        return _items.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
        {
            _items.Add(Cast(value));
        }
    }

    public override void Clear() => _items.Clear();

    public override bool Contains(object value) => value is RasterizedDbParameter p && _items.Contains(p);

    public override bool Contains(string value) => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index) => ((ICollection)_items).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _items.GetEnumerator();

    public override int IndexOf(object value) => value is RasterizedDbParameter p ? _items.IndexOf(p) : -1;

    public override int IndexOf(string parameterName)
    {
        var name = parameterName.AsSpan();
        if (name.Length > 0 && name[0] is '@' or ':' or '$')
        {
            name = name[1..];
        }

        var items = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_items);
        for (var i = 0; i < items.Length; i++)
        {
            if (name.Equals(items[i].TrimmedName, StringComparison.Ordinal))
            {
                return i;
            }
        }

        for (var i = 0; i < items.Length; i++)
        {
            if (name.Equals(items[i].TrimmedName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    public override void Insert(int index, object value) => _items.Insert(index, Cast(value));

    public override void Remove(object value) => _items.Remove(Cast(value));

    public override void RemoveAt(int index) => _items.RemoveAt(index);

    public override void RemoveAt(string parameterName) => _items.RemoveAt(Require(parameterName));

    protected override DbParameter GetParameter(int index) => _items[index];

    protected override DbParameter GetParameter(string parameterName) => _items[Require(parameterName)];

    protected override void SetParameter(int index, DbParameter value) => _items[index] = Cast(value);

    protected override void SetParameter(string parameterName, DbParameter value) => _items[Require(parameterName)] = Cast(value);

    private int Require(string parameterName)
    {
        var index = IndexOf(parameterName);
        return index >= 0 ? index : throw new ArgumentException($"no parameter is named \"{parameterName}\"");
    }

    private static RasterizedDbParameter Cast(object? value) =>
        value as RasterizedDbParameter ?? throw new InvalidCastException("a RasterizedDbCommand takes RasterizedDbParameter values");
}
