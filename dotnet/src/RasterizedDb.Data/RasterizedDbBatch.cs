using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// Several statements in one round trip. They are sent together as one simple query (the text result
/// format), which the server runs as one implicit transaction: all of them take effect, or none does.
/// The reader has one result per row-returning statement (<see cref="DbDataReader.NextResult"/>).
/// </summary>
public sealed class RasterizedDbBatch : DbBatch
{
    private readonly RasterizedDbBatchCommandCollection _commands = new();
    private readonly RasterizedDbCommand _command = new();
    private readonly ByteWriter _text = new(1024);
    private RasterizedDbConnection? _connection;

    public new RasterizedDbBatchCommandCollection BatchCommands => _commands;

    protected override DbBatchCommandCollection DbBatchCommands => _commands;

    /// <summary>Seconds to wait for the answer; the connection string's <c>Command Timeout</c> unless set.</summary>
    public override int Timeout
    {
        get => _command.CommandTimeout;
        set => _command.CommandTimeout = value;
    }

    public new RasterizedDbConnection? Connection
    {
        get => _connection;
        set => _connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (RasterizedDbConnection?)value;
    }

    protected override DbTransaction? DbTransaction { get; set; }

    protected override DbBatchCommand CreateDbBatchCommand() => new RasterizedDbBatchCommand();

    /// <summary>The statements, each with its values written in, one after another.</summary>
    private RasterizedDbCommand Prepared()
    {
        if (_commands.Count == 0)
        {
            throw new InvalidOperationException("the batch has no commands");
        }

        _text.Clear();
        var first = true;
        foreach (var command in _commands.Items)
        {
            if (!first)
            {
                _text.Write(";\n"u8);
            }

            first = false;
            var parsed = ParsedSql.Get(command.CommandText);
            RasterizedDbCommand.WriteRendered(_text, parsed, command.ParameterItems);
        }

        _command.Connection = _connection ?? throw new InvalidOperationException("the batch has no connection");
        _command.RawStatement = _text.WrittenMemory;
        _command.RawIsText = true;
        return _command;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => Prepared().ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        await Prepared().ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    public override int ExecuteNonQuery() => Prepared().ExecuteNonQuery();

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) => Prepared().ExecuteNonQueryAsync(cancellationToken);

    public override object? ExecuteScalar() => Prepared().ExecuteScalar();

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default) => Prepared().ExecuteScalarAsync(cancellationToken);

    public override void Prepare()
    {
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public override void Cancel() => _command.Cancel();

    public override void Dispose()
    {
        _text.Dispose();
        _command.Dispose();
        base.Dispose();
    }
}

public sealed class RasterizedDbBatchCommand : DbBatchCommand
{
    private RasterizedDbParameterCollection? _parameters;
    private string _text = string.Empty;

    public RasterizedDbBatchCommand()
    {
    }

    public RasterizedDbBatchCommand(string commandText) => _text = commandText;

    [AllowNull]
    public override string CommandText
    {
        get => _text;
        set => _text = value ?? string.Empty;
    }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
            {
                throw new NotSupportedException("RasterizedDB commands are statement text");
            }
        }
    }

    /// <summary>Not reported per statement; the reader's <see cref="DbDataReader.RecordsAffected"/> is the batch's total.</summary>
    public override int RecordsAffected => -1;

    public new RasterizedDbParameterCollection Parameters => _parameters ??= new RasterizedDbParameterCollection();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    internal List<RasterizedDbParameter>? ParameterItems => _parameters?.Items;

#if NET8_0_OR_GREATER
    public override bool CanCreateParameter => true;

    public override DbParameter CreateParameter() => new RasterizedDbParameter();
#endif
}

public sealed class RasterizedDbBatchCommandCollection : DbBatchCommandCollection
{
    internal List<RasterizedDbBatchCommand> Items { get; } = [];

    public override int Count => Items.Count;

    public override bool IsReadOnly => false;

    public new RasterizedDbBatchCommand this[int index]
    {
        get => Items[index];
        set => Items[index] = value;
    }

    /// <summary>Adds a statement and returns its command, to give it parameters.</summary>
    public RasterizedDbBatchCommand Add(string commandText)
    {
        var command = new RasterizedDbBatchCommand(commandText);
        Items.Add(command);
        return command;
    }

    private static RasterizedDbBatchCommand Cast(DbBatchCommand item) =>
        item as RasterizedDbBatchCommand ?? throw new InvalidCastException("a RasterizedDbBatch takes RasterizedDbBatchCommand items");

    public override void Add(DbBatchCommand item) => Items.Add(Cast(item));

    public override void Clear() => Items.Clear();

    public override bool Contains(DbBatchCommand item) => item is RasterizedDbBatchCommand command && Items.Contains(command);

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex) => ((ICollection)Items).CopyTo(array, arrayIndex);

    public override IEnumerator<DbBatchCommand> GetEnumerator() => Items.GetEnumerator();

    public override int IndexOf(DbBatchCommand item) => item is RasterizedDbBatchCommand command ? Items.IndexOf(command) : -1;

    public override void Insert(int index, DbBatchCommand item) => Items.Insert(index, Cast(item));

    public override bool Remove(DbBatchCommand item) => item is RasterizedDbBatchCommand command && Items.Remove(command);

    public override void RemoveAt(int index) => Items.RemoveAt(index);

    protected override DbBatchCommand GetBatchCommand(int index) => Items[index];

    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Items[index] = Cast(batchCommand);
}
