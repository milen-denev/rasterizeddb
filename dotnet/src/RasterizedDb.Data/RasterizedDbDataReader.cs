using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// Reads a command's rows. The whole answer has arrived before the reader exists (the native protocol
/// answers a request with one frame), so <see cref="Read"/> never waits, and a cell is decoded from the
/// answer's own bytes when it is asked for: nothing is copied or boxed until a getter needs to.
/// </summary>
/// <remarks>
/// In <see cref="RasterizedDbResultFormat.Binary"/> the server sends no column names: columns are read
/// by position, <see cref="GetName"/> answers <c>?column?</c> and <see cref="GetOrdinal"/> cannot find
/// anything. In <see cref="RasterizedDbResultFormat.Text"/> names and types are there, and a command
/// holding several statements has one result per row-returning statement.
/// </remarks>
public sealed class RasterizedDbDataReader : DbDataReader
{
    [StructLayout(LayoutKind.Sequential)]
    private struct Cell
    {
        public int Start;
        public int Length;
        public byte Tag;
    }

    private sealed class Column(string name, uint oid, int typeModifier)
    {
        public readonly string Name = name;
        public readonly uint Oid = oid;
        public readonly int TypeModifier = typeModifier;
    }

    private const int RowHeader = 4;
    private const int ColumnOverhead = 8 + 8 + 1;

    private RasterizedDbConnection? _connection;
    private CommandBehavior _behavior;
    private Response _response;
    private byte[] _buffer = [];
    private bool _text;
    private bool _closed = true;

    // Where the rows of the current result are, and how far Read has come.
    private int _position;
    private int _end;
    private bool _onRow;
    private bool _hasRows;
    private long _recordsAffected = -1;

    private Cell[] _cells = [];
    private int _fieldCount;

    // Text format.
    private readonly List<ResultInfo> _results = [];
    private int _resultIndex;
    private Column[]? _columns;

    // Binary format: the statement, to ask the server for its column names if anyone wants them.
    private ParsedSql? _statement;
    private RasterizedDbDataSource? _source;
    private string[]? _names;

    internal RasterizedDbDataReader()
    {
    }

    // ------------------------------------------------------------------- set up

    /// <summary>A native answer's rows (<paramref name="start"/> is just past the answer's tag byte).</summary>
    internal void InitBinary(RasterizedDbConnection? connection, CommandBehavior behavior, Response response, int start, long recordsAffected, ParsedSql? statement = null, RasterizedDbDataSource? source = null)
    {
        Init(connection, behavior, response);
        _statement = statement;
        _source = source ?? connection?.Source;
        _text = false;
        _position = start;
        _end = response.Length;
        _recordsAffected = recordsAffected;
        _hasRows = _position < _end;
        _fieldCount = _hasRows ? CountBinaryCells(_position) : 0;
        EnsureCells(_fieldCount);
        if (_hasRows)
        {
            // The first row's tags, for GetFieldType before the first Read.
            var first = _position;
            ReadBinaryRow();
            _position = first;
            _onRow = false;
        }
    }

    /// <summary>An answer with no rows at all: a write's count, or an acknowledgement.</summary>
    internal void InitEmpty(RasterizedDbConnection? connection, CommandBehavior behavior, long recordsAffected)
    {
        Init(connection, behavior, default);
        _text = false;
        _position = _end = 0;
        _hasRows = false;
        _fieldCount = 0;
        _recordsAffected = recordsAffected;
    }

    /// <summary>A simple-query answer, already scanned into <see cref="TextResults"/>.</summary>
    internal void InitText(RasterizedDbConnection? connection, CommandBehavior behavior, Response response, long recordsAffected)
    {
        Init(connection, behavior, response);
        _text = true;
        _end = response.Length;
        _recordsAffected = recordsAffected;
        _resultIndex = -1;
        if (!MoveToResult(0))
        {
            _fieldCount = 0;
            _hasRows = false;
            _position = _end;
        }
    }

    /// <summary>The list <see cref="SimpleAnswer.Scan"/> fills before <see cref="InitText"/>.</summary>
    internal List<ResultInfo> TextResults
    {
        get
        {
            _results.Clear();
            return _results;
        }
    }

    private void Init(RasterizedDbConnection? connection, CommandBehavior behavior, Response response)
    {
        _connection = connection;
        _behavior = behavior;
        _response = response;
        _buffer = response.Buffer ?? [];
        _closed = false;
        _onRow = false;
        _columns = null;
        _statement = null;
        _source = null;
        _names = null;
    }

    private void EnsureCells(int count)
    {
        if (_cells.Length < count)
        {
            _cells = new Cell[Math.Max(count, 8)];
        }
    }

    private static RasterizedDbException Truncated() =>
        new(RasterizedDbErrorKind.Protocol, "a row in the answer is truncated or its lengths disagree");

    private int CountBinaryCells(int at)
    {
        var buffer = _buffer.AsSpan(0, _end);
        if (buffer.Length - at < RowHeader)
        {
            throw Truncated();
        }

        var payload = BinaryPrimitives.ReadUInt32LittleEndian(buffer[at..]);
        if (payload > (uint)(buffer.Length - at - RowHeader))
        {
            throw Truncated();
        }

        var end = at + RowHeader + (int)payload;
        var p = at + RowHeader;
        var count = 0;
        while (p < end)
        {
            if (end - p < ColumnOverhead)
            {
                throw Truncated();
            }

            var size = BinaryPrimitives.ReadUInt64LittleEndian(buffer[p..]);
            if (size > (ulong)(end - p - ColumnOverhead))
            {
                throw Truncated();
            }

            p += 16 + (int)size + 1;
            count++;
        }

        return count;
    }

    // ------------------------------------------------------------------- moving

    public override bool Read()
    {
        if (_position >= _end)
        {
            _onRow = false;
            return false;
        }

        return _text ? ReadTextRow() : ReadBinaryRow();
    }

    /// <summary>
    /// A row is a little-endian <c>u32</c> payload length, then per column a <c>u64</c> data size, a
    /// <c>u64</c> schema id (unused here), the data, and a one-byte type tag.
    /// </summary>
    private bool ReadBinaryRow()
    {
        ref var start = ref MemoryMarshal.GetArrayDataReference(_buffer);
        var at = _position;
        if (_end - at < RowHeader)
        {
            throw Truncated();
        }

        var payload = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref start, at));
        if (!BitConverter.IsLittleEndian)
        {
            payload = BinaryPrimitives.ReverseEndianness(payload);
        }

        if (payload > (uint)(_end - at - RowHeader))
        {
            throw Truncated();
        }

        var end = at + RowHeader + (int)payload;
        var p = at + RowHeader;
        var cells = _cells;
        var count = _fieldCount;
        var i = 0;
        while (p < end)
        {
            if (end - p < ColumnOverhead || (uint)i >= (uint)cells.Length || i >= count)
            {
                throw Truncated();
            }

            var size = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref start, p));
            if (!BitConverter.IsLittleEndian)
            {
                size = BinaryPrimitives.ReverseEndianness(size);
            }

            if (size > (ulong)(end - p - ColumnOverhead))
            {
                throw Truncated();
            }

            var data = p + 16;
            ref var cell = ref cells[i++];
            cell.Start = data;
            cell.Length = (int)size;
            cell.Tag = Unsafe.Add(ref start, data + (int)size);
            p = data + (int)size + 1;
        }

        if (i != count)
        {
            throw Truncated();
        }

        _position = end;
        _onRow = true;
        return true;
    }

    private bool ReadTextRow()
    {
        var span = _buffer.AsSpan(0, _end);
        var at = _position;
        if (span[at] != (byte)'D')
        {
            // The result's CommandComplete: its rows are over.
            _onRow = false;
            return false;
        }

        SimpleAnswer.Next(span, ref at, out var bodyAt, out var bodyLength);
        var body = span.Slice(bodyAt, bodyLength);
        if (body.Length < 2 || BinaryPrimitives.ReadInt16BigEndian(body) != _fieldCount)
        {
            throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "a simple-query answer is damaged: a row's width differs from its description");
        }

        var p = 2;
        var cells = _cells;
        for (var i = 0; i < _fieldCount; i++)
        {
            if (body.Length - p < 4)
            {
                throw Truncated();
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(body[p..]);
            p += 4;
            ref var cell = ref cells[i];
            if (length < 0)
            {
                cell.Start = 0;
                cell.Length = 0;
                cell.Tag = Tag.Null;
            }
            else
            {
                if (length > body.Length - p)
                {
                    throw Truncated();
                }

                cell.Start = bodyAt + p;
                cell.Length = length;
                cell.Tag = Tag.PgText;
                p += length;
            }
        }

        _position = at;
        _onRow = true;
        return true;
    }

    private bool MoveToResult(int index)
    {
        if (index >= _results.Count)
        {
            return false;
        }

        _resultIndex = index;
        var info = _results[index];
        var description = _buffer.AsSpan(info.DescriptionAt, info.DescriptionLength);
        var count = BinaryPrimitives.ReadInt16BigEndian(description);
        var columns = new Column[count];
        var at = 2;
        for (var i = 0; i < count; i++)
        {
            var end = description[at..].IndexOf((byte)0);
            if (end < 0 || description.Length - at - end - 1 < 18)
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "a simple-query answer is damaged: a column description is cut short");
            }

            var name = Encoding.UTF8.GetString(description.Slice(at, end));
            at += end + 1;
            // table oid (4), attribute (2), type oid (4), size (2), modifier (4), format (2)
            var oid = BinaryPrimitives.ReadUInt32BigEndian(description[(at + 6)..]);
            var modifier = BinaryPrimitives.ReadInt32BigEndian(description[(at + 12)..]);
            at += 18;
            columns[i] = new Column(name, oid, modifier);
        }

        _columns = columns;
        _fieldCount = count;
        EnsureCells(count);
        _position = info.RowsAt;
        _onRow = false;
        _hasRows = _position < _end && _buffer[_position] == (byte)'D';
        return true;
    }

    public override bool NextResult()
    {
        _onRow = false;
        if (_text && (_behavior & CommandBehavior.SingleResult) == 0 && MoveToResult(_resultIndex + 1))
        {
            return true;
        }

        _position = _end;
        _hasRows = false;
        return false;
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Read() ? TrueTask : FalseTask;

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => NextResult() ? TrueTask : FalseTask;

    private static readonly Task<bool> TrueTask = Task.FromResult(true);
    private static readonly Task<bool> FalseTask = Task.FromResult(false);

    public override int FieldCount => _fieldCount;

    public override bool HasRows => _hasRows;

    public override int Depth => 0;

    public override bool IsClosed => _closed;

    /// <summary>
    /// Rows the command's writes affected, or -1 when it ran none. In the binary format a statement that
    /// returns rows reports none.
    /// </summary>
    public override int RecordsAffected => _recordsAffected > int.MaxValue ? int.MaxValue : (int)_recordsAffected;

    /// <summary><see cref="RecordsAffected"/> without the 32-bit limit.</summary>
    public long RowsAffected => _recordsAffected;

    public override int VisibleFieldCount => _fieldCount;

    // -------------------------------------------------------------------- cells

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref readonly Cell CellAt(int ordinal)
    {
        if (!_onRow)
        {
            ThrowNoRow();
        }

        if ((uint)ordinal >= (uint)_fieldCount)
        {
            ThrowOrdinal(ordinal);
        }

        return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), ordinal);
    }

    /// <summary>A cell's bytes; bounds were checked when the row was read.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> Raw(in Cell cell) =>
        MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), cell.Start), cell.Length);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref readonly Cell ValueAt(int ordinal)
    {
        ref readonly var cell = ref CellAt(ordinal);
        if (cell.Tag == Tag.Null)
        {
            ThrowNull(ordinal);
        }

        return ref cell;
    }

    [DoesNotReturn]
    private void ThrowNoRow() =>
        throw new InvalidOperationException(_closed ? "the reader is closed" : "no row is being read: call Read() first, and stop when it returns false");

    [DoesNotReturn]
    private void ThrowOrdinal(int ordinal) =>
        throw new IndexOutOfRangeException($"column {ordinal} is out of range: the row has {_fieldCount} column(s)");

    [DoesNotReturn]
    private static void ThrowNull(int ordinal) =>
        throw new InvalidCastException($"column {ordinal} is NULL; check IsDBNull first, or read it as a nullable type");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private InvalidCastException Explain(int ordinal, Exception inner) =>
        new($"column {ordinal}{(_columns is { } c && (uint)ordinal < (uint)c.Length ? $" (\"{c[ordinal].Name}\")" : "")}: {inner.Message}", inner);

    public override bool IsDBNull(int ordinal) => CellAt(ordinal).Tag == Tag.Null;

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => IsDBNull(ordinal) ? TrueTask : FalseTask;

    public override int GetInt32(int ordinal)
    {
        ref readonly var cell = ref ValueAt(ordinal);
        // The common case decoded here, with no call.
        if (cell.Tag == Tag.I32 && cell.Length == 4 && BitConverter.IsLittleEndian)
        {
            return Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), cell.Start));
        }

        try
        {
            return ValueReader.ReadInt32(cell.Tag, Raw(cell));
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException)
        {
            throw Explain(ordinal, e);
        }
    }

    public override long GetInt64(int ordinal)
    {
        ref readonly var cell = ref ValueAt(ordinal);
        if (cell.Tag == Tag.I64 && cell.Length == 8 && BitConverter.IsLittleEndian)
        {
            return Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), cell.Start));
        }

        try
        {
            return ValueReader.ReadInt64(cell.Tag, Raw(cell));
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException)
        {
            throw Explain(ordinal, e);
        }
    }

    public override string GetString(int ordinal)
    {
        ref readonly var cell = ref ValueAt(ordinal);
        try
        {
            return ValueReader.ReadString(cell.Tag, Raw(cell));
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException)
        {
            throw Explain(ordinal, e);
        }
    }

    public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);

    public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);

    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);

    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);

    public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);

    public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);

    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);

    public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);

    public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);

    public DateTimeOffset GetDateTimeOffset(int ordinal) => GetFieldValue<DateTimeOffset>(ordinal);

    public DateOnly GetDateOnly(int ordinal) => GetFieldValue<DateOnly>(ordinal);

    public TimeOnly GetTimeOnly(int ordinal) => GetFieldValue<TimeOnly>(ordinal);

    public TimeSpan GetTimeSpan(int ordinal) => GetFieldValue<TimeSpan>(ordinal);

    public RasterizedDbInterval GetInterval(int ordinal) => GetFieldValue<RasterizedDbInterval>(ordinal);

    /// <summary>
    /// The cell as <typeparamref name="T"/>. A NULL reads as <c>null</c> for a nullable value type and as
    /// <see cref="DBNull"/> for <see cref="object"/>, and is an <see cref="InvalidCastException"/> otherwise.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        ref readonly var cell = ref CellAt(ordinal);
        if (cell.Tag == Tag.Null)
        {
            return NullAs<T>(ordinal);
        }

        try
        {
            return ValueReader.Read<T>(cell.Tag, Raw(cell));
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException)
        {
            throw Explain(ordinal, e);
        }
    }

    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => Task.FromResult(GetFieldValue<T>(ordinal));

    private static T NullAs<T>(int ordinal)
    {
        if (typeof(T) == typeof(object))
        {
            return (T)(object)DBNull.Value;
        }

        if (default(T) is null && typeof(T).IsValueType)
        {
            return default!;
        }

        ThrowNull(ordinal);
        return default;
    }

    public override object GetValue(int ordinal)
    {
        ref readonly var cell = ref CellAt(ordinal);
        if (cell.Tag == Tag.Null)
        {
            return DBNull.Value;
        }

        try
        {
            return ValueReader.ReadObject(cell.Tag, Raw(cell), _columns is { } columns ? columns[ordinal].Oid : 0);
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException)
        {
            throw Explain(ordinal, e);
        }
    }

    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, _fieldCount);
        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }

        return count;
    }

    /// <summary>
    /// The cell's bytes as they arrived, with no copy: UTF-8 for text (and for every cell of the text
    /// format), the value's bytes otherwise. Valid until the reader moves on or is closed.
    /// </summary>
    public ReadOnlySpan<byte> GetRawSpan(int ordinal)
    {
        ref readonly var cell = ref ValueAt(ordinal);
        return Raw(cell);
    }

    /// <summary>The server's one-byte type tag of a cell (binary format); 253 for a NULL.</summary>
    public byte GetTypeTag(int ordinal) => CellAt(ordinal).Tag;

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        ref readonly var cell = ref ValueAt(ordinal);
        ReadOnlySpan<byte> bytes = cell.Tag == Tag.PgText ? ValueReader.ReadBytes(cell.Tag, Raw(cell)) : Raw(cell);
        if (buffer is null)
        {
            return bytes.Length;
        }

        if (dataOffset < 0 || dataOffset > bytes.Length)
        {
            return 0;
        }

        var count = Math.Min(length, bytes.Length - (int)dataOffset);
        bytes.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset));
        return count;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var text = GetString(ordinal);
        if (buffer is null)
        {
            return text.Length;
        }

        if (dataOffset < 0 || dataOffset > text.Length)
        {
            return 0;
        }

        var count = Math.Min(length, text.Length - (int)dataOffset);
        text.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }

    public override Stream GetStream(int ordinal) => new MemoryStream(GetFieldValue<byte[]>(ordinal), writable: false);

    public override TextReader GetTextReader(int ordinal) => new StringReader(GetString(ordinal));

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    // ----------------------------------------------------------------- metadata

    /// <summary>The column's name; <c>?column?</c> in the binary format, which carries none.</summary>
    public override string GetName(int ordinal)
    {
        if ((uint)ordinal >= (uint)_fieldCount)
        {
            ThrowOrdinal(ordinal);
        }

        return _columns is { } columns ? columns[ordinal].Name : BinaryNames()[ordinal];
    }

    /// <summary>
    /// The binary format's answer carries no names, so they are asked for (once per statement text,
    /// with a request that runs nothing) the first time something wants one. <c>?column?</c> when the
    /// server cannot say.
    /// </summary>
    private string[] BinaryNames()
    {
        if (_names is { } names)
        {
            return names;
        }

        var described = _statement is { } statement && _source is { } source ? source.ColumnNames(statement, _connection) : [];
        if (described.Length != _fieldCount)
        {
            described = new string[_fieldCount];
            Array.Fill(described, "?column?");
        }

        return _names = described;
    }

    public override int GetOrdinal(string name)
    {
        var names = _columns is { } columns ? null : BinaryNames();
        var count = _fieldCount;
        for (var i = 0; i < count; i++)
        {
            if (string.Equals(names is null ? _columns![i].Name : names[i], name, StringComparison.Ordinal))
            {
                return i;
            }
        }

        for (var i = 0; i < count; i++)
        {
            if (string.Equals(names is null ? _columns![i].Name : names[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        if (names is not null && count > 0 && names[0] == "?column?" && Array.TrueForAll(names, n => n == "?column?"))
        {
            throw new IndexOutOfRangeException(
                $"column \"{name}\" cannot be looked up: the binary result format carries no column names and this server could not describe the " +
                "statement. Read by position, or run the command with ResultFormat = Text.");
        }

        throw new IndexOutOfRangeException($"the result has no column named \"{name}\"");
    }

    private byte TagOf(int ordinal)
    {
        if ((uint)ordinal >= (uint)_fieldCount)
        {
            ThrowOrdinal(ordinal);
        }

        if (_text)
        {
            return Tag.PgText;
        }

        // The binary format types cells, not columns: the type is the current row's (or the first's).
        return _cells[ordinal].Tag;
    }

    public override string GetDataTypeName(int ordinal) => ValueReader.TypeName(TagOf(ordinal), _columns is { } c ? c[ordinal].Oid : 0);

    public override Type GetFieldType(int ordinal)
    {
        var tag = TagOf(ordinal);
        return tag == Tag.Null ? typeof(object) : ValueReader.FieldType(tag, _columns is { } c ? c[ordinal].Oid : 0);
    }

    /// <summary>The column's PostgreSQL type OID (text format); 0 in the binary format.</summary>
    public uint GetDataTypeOid(int ordinal)
    {
        if ((uint)ordinal >= (uint)_fieldCount)
        {
            ThrowOrdinal(ordinal);
        }

        return _columns is { } c ? c[ordinal].Oid : 0;
    }

    public ReadOnlyCollection<DbColumn> GetColumnSchema()
    {
        var list = new List<DbColumn>(_fieldCount);
        for (var i = 0; i < _fieldCount; i++)
        {
            list.Add(new SchemaColumn(GetName(i), i, GetFieldType(i), GetDataTypeName(i)));
        }

        return new ReadOnlyCollection<DbColumn>(list);
    }

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
        table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
        table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
        table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
        table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
        table.Columns.Add("DataTypeName", typeof(string));
        for (var i = 0; i < _fieldCount; i++)
        {
            table.Rows.Add(GetName(i), i, -1, GetFieldType(i), true, GetDataTypeName(i));
        }

        return table;
    }

    private sealed class SchemaColumn : DbColumn
    {
        public SchemaColumn(string name, int ordinal, Type type, string typeName)
        {
            ColumnName = name;
            ColumnOrdinal = ordinal;
            DataType = type;
            DataTypeName = typeName;
            AllowDBNull = true;
        }
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    // ------------------------------------------------------------------ closing

    public override void Close()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        _onRow = false;
        _position = _end = 0;
        _fieldCount = 0;
        _buffer = [];
        _columns = null;
        _statement = null;
        _source = null;
        _names = null;
        _results.Clear();
        _response.Return();
        var connection = _connection;
        _connection = null;
        connection?.ReaderClosed(this, (_behavior & CommandBehavior.CloseConnection) != 0);
    }

    public override Task CloseAsync()
    {
        Close();
        return Task.CompletedTask;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
    }

    public override ValueTask DisposeAsync()
    {
        Close();
        return default;
    }
}
