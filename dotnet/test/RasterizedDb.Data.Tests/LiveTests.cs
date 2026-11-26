using System.Data;

namespace RasterizedDb.Data.Tests;

/// <summary>The driver against a real server (see <see cref="ServerFixture"/>).</summary>
[Collection("server")]
public class LiveTests(ServerFixture servers)
{
    private static int _table;

    private static string Table(string name) => $"{name}_{Interlocked.Increment(ref _table)}";

    private async Task<RasterizedDbConnection?> Open(string extra = "")
    {
        if (servers.Trust(extra) is not { } connectionString)
        {
            return null;
        }

        var connection = new RasterizedDbConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<int> Run(RasterizedDbConnection connection, string sql, RasterizedDbResultFormat format = RasterizedDbResultFormat.Binary)
    {
        await using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = format };
        return await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Binary_rows_read_as_their_types()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("types");
        await Run(
            connection,
            $"CREATE TABLE {table} (id BIGINT NOT NULL, i INTEGER, s SMALLINT, b BOOLEAN, f REAL, d DOUBLE PRECISION, n NUMERIC(12,2), t TEXT, " +
            "raw BYTEA, u UUID, ts TIMESTAMP, tz TIMESTAMPTZ, dt DATE, tm TIME, PRIMARY KEY (id))");

        var uuid = Guid.NewGuid();
        var instant = new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc).AddTicks(1234560);
        await using (var insert = new RasterizedDbCommand(
                         $"INSERT INTO {table} (id, i, s, b, f, d, n, t, raw, u, ts, tz, dt, tm) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                         connection) { ResultFormat = RasterizedDbResultFormat.Binary })
        {
            insert.Parameters.AddWithValue(1L);
            insert.Parameters.AddWithValue(-42);
            insert.Parameters.AddWithValue((short)7);
            insert.Parameters.AddWithValue(true);
            insert.Parameters.AddWithValue(0.1f);
            insert.Parameters.AddWithValue(-2.5);
            insert.Parameters.AddWithValue(-1234.56m);
            insert.Parameters.AddWithValue("it's a 'quoted' -- text; with $1 and \\ in it");
            insert.Parameters.AddWithValue(new byte[] { 0, 1, 254, 255 });
            insert.Parameters.AddWithValue(uuid);
            insert.Parameters.AddWithValue(new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(instant);
            insert.Parameters.AddWithValue(new DateOnly(2024, 2, 29));
            insert.Parameters.AddWithValue(new TimeOnly(1, 2, 3));
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await Run(connection, $"INSERT INTO {table} (id) VALUES (2)");

        await using var select = new RasterizedDbCommand(
            $"SELECT id, i, s, b, f, d, n, t, raw, u, ts, tz, dt, tm FROM {table} ORDER BY id", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(reader.HasRows);
        Assert.Equal(14, reader.FieldCount);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(-42, reader.GetInt32(1));
        Assert.Equal((short)7, reader.GetInt16(2));
        Assert.True(reader.GetBoolean(3));
        Assert.Equal(0.1f, reader.GetFloat(4));
        Assert.Equal(-2.5, reader.GetDouble(5));
        Assert.Equal(-1234.56m, reader.GetDecimal(6));
        Assert.Equal("it's a 'quoted' -- text; with $1 and \\ in it", reader.GetString(7));
        Assert.Equal(new byte[] { 0, 1, 254, 255 }, reader.GetFieldValue<byte[]>(8));
        Assert.Equal(uuid, reader.GetGuid(9));
        Assert.Equal(new DateTime(2024, 2, 29, 13, 14, 15), reader.GetDateTime(10));
        Assert.Equal(instant, reader.GetDateTime(11));
        Assert.Equal(DateTimeKind.Utc, reader.GetDateTime(11).Kind);
        Assert.Equal(new DateOnly(2024, 2, 29), reader.GetFieldValue<DateOnly>(12));
        Assert.Equal(new TimeOnly(1, 2, 3), reader.GetFieldValue<TimeOnly>(13));
        Assert.IsType<long>(reader.GetValue(0));
        Assert.IsType<decimal>(reader.GetValue(6));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        for (var i = 1; i < 14; i++)
        {
            Assert.True(reader.IsDBNull(i), $"column {i}");
        }

        Assert.Null(reader.GetFieldValue<int?>(1));
        Assert.Throws<InvalidCastException>(() => reader.GetInt32(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Text_format_carries_names_types_and_several_results()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("named");
        await using var command = new RasterizedDbCommand(
            $"CREATE TABLE {table} (id INTEGER NOT NULL, label TEXT, amount NUMERIC(10,2), PRIMARY KEY (id)); " +
            $"INSERT INTO {table} VALUES (1, 'one', 1.50), (2, NULL, 2.25); " +
            $"SELECT id, label, amount FROM {table} ORDER BY id; SELECT count(*) AS total FROM {table}",
            connection) { ResultFormat = RasterizedDbResultFormat.Text };
        await using var reader = await command.ExecuteReaderAsync();
        Assert.Equal(2, reader.RecordsAffected);
        Assert.Equal(["id", "label", "amount"], [reader.GetName(0), reader.GetName(1), reader.GetName(2)]);
        Assert.Equal(1, reader.GetOrdinal("label"));
        Assert.Equal(typeof(int), reader.GetFieldType(0));
        Assert.Equal(typeof(decimal), reader.GetFieldType(2));
        Assert.True(reader.Read());
        Assert.Equal(1, reader["id"]);
        Assert.Equal("one", reader["label"]);
        Assert.Equal(1.50m, reader.GetDecimal(2));
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(1));
        Assert.Equal(DBNull.Value, reader["label"]);
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.Equal("total", reader.GetName(0));
        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(reader.NextResult());

        var loaded = new DataTable();
        await using (var again = new RasterizedDbCommand($"SELECT id, label FROM {table} ORDER BY id", connection) { ResultFormat = RasterizedDbResultFormat.Text })
        await using (var rows = await again.ExecuteReaderAsync())
        {
            loaded.Load(rows);
        }

        Assert.Equal(2, loaded.Rows.Count);
        Assert.Equal("label", loaded.Columns[1].ColumnName);
    }

    [Fact]
    public async Task Named_and_repeated_parameters_fill_in()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        foreach (var format in (RasterizedDbResultFormat[])[RasterizedDbResultFormat.Binary, RasterizedDbResultFormat.Text])
        {
            await using var command = new RasterizedDbCommand("SELECT @a + @b, @a, @text", connection) { ResultFormat = format };
            command.Parameters.Add("@b", 40);
            command.Parameters.Add("a", 2);
            command.Parameters.AddWithValue("@text", "x'y");
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(reader.Read());
            Assert.Equal(42, reader.GetInt32(0));
            Assert.Equal(2, reader.GetInt32(1));
            Assert.Equal("x'y", reader.GetString(2));
        }

        await using var missing = new RasterizedDbCommand("SELECT @nope", connection);
        await Assert.ThrowsAsync<InvalidOperationException>(() => missing.ExecuteScalarAsync());
    }

    [Fact]
    public async Task A_binary_command_takes_one_statement()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        await using var command = new RasterizedDbCommand("SELECT 1; SELECT 2", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        var error = await Assert.ThrowsAsync<RasterizedDbException>(() => command.ExecuteScalarAsync());
        Assert.Equal(RasterizedDbErrorKind.Unsupported, error.Kind);
    }

    [Fact]
    public async Task A_server_error_carries_its_sqlstate_and_the_connection_goes_on()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("dupes");
        await Run(connection, $"CREATE TABLE {table} (id INTEGER NOT NULL, PRIMARY KEY (id))");
        await Run(connection, $"INSERT INTO {table} VALUES (1)");
        foreach (var format in (RasterizedDbResultFormat[])[RasterizedDbResultFormat.Binary, RasterizedDbResultFormat.Text])
        {
            var error = await Assert.ThrowsAsync<RasterizedDbException>(() => Run(connection, $"INSERT INTO {table} VALUES (1)", format));
            Assert.Equal(RasterizedDbErrorKind.Server, error.Kind);
            Assert.True(error.IsUniqueViolation, error.Message);
            Assert.Equal(1L, await new RasterizedDbCommand($"SELECT count(*) FROM {table}", connection) { ResultFormat = format }.ExecuteScalarAsync());
        }

        var missing = await Assert.ThrowsAsync<RasterizedDbException>(() => Run(connection, "SELECT * FROM no_such_table_here", RasterizedDbResultFormat.Text));
        Assert.Equal("42P01", missing.SqlState);
    }

    [Fact]
    public async Task Transactions_commit_roll_back_and_keep_savepoints()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("tx");
        await Run(connection, $"CREATE TABLE {table} (id INTEGER NOT NULL, PRIMARY KEY (id))");

        await using (var transaction = await connection.BeginTransactionAsync())
        {
            Assert.True(connection.InTransaction);
            await Run(connection, $"INSERT INTO {table} VALUES (1)");
            await transaction.SaveAsync("before_two");
            await Run(connection, $"INSERT INTO {table} VALUES (2)", RasterizedDbResultFormat.Text);
            await transaction.RollbackAsync("before_two");
            await Run(connection, $"INSERT INTO {table} VALUES (3)");
            await transaction.CommitAsync();
        }

        Assert.False(connection.InTransaction);
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable))
        {
            await Run(connection, $"INSERT INTO {table} VALUES (4)");
            // Disposed without a commit: rolled back.
        }

        Assert.False(connection.InTransaction);
        await using var select = new RasterizedDbCommand($"SELECT id FROM {table} ORDER BY id", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        await using var reader = await select.ExecuteReaderAsync();
        var ids = new List<int>();
        while (reader.Read())
        {
            ids.Add(reader.GetInt32(0));
        }

        Assert.Equal([1, 3], ids);
    }

    [Fact]
    public async Task A_failed_statement_fails_its_block_until_it_is_rolled_back()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        await using var transaction = await connection.BeginTransactionAsync();
        await Assert.ThrowsAsync<RasterizedDbException>(() => Run(connection, "SELECT * FROM no_such_table_here"));
        var failed = await Assert.ThrowsAsync<RasterizedDbException>(() => Run(connection, "SELECT 1"));
        Assert.Equal("25P02", failed.SqlState);
        await transaction.RollbackAsync();
        Assert.Equal(1, Convert.ToInt32(await new RasterizedDbCommand("SELECT 1", connection).ExecuteScalarAsync()));
    }

    [Fact]
    public async Task A_closed_connection_gives_its_block_back_rolled_back()
    {
        if (servers.Trust("Maximum Pool Size=1") is not { } connectionString)
        {
            return;
        }

        var table = Table("pooled");
        await using (var first = new RasterizedDbConnection(connectionString))
        {
            await first.OpenAsync();
            await Run(first, $"CREATE TABLE {table} (id INTEGER NOT NULL, PRIMARY KEY (id))");
            await Run(first, "BEGIN");
            Assert.True(first.InTransaction);
            await Run(first, $"INSERT INTO {table} VALUES (1)");
        }

        // The pool's one connection again: no block may be open on it.
        await using var second = new RasterizedDbConnection(connectionString);
        await second.OpenAsync();
        Assert.False(second.InTransaction);
        Assert.Equal(0L, await new RasterizedDbCommand($"SELECT count(*) FROM {table}", second) { ResultFormat = RasterizedDbResultFormat.Binary }.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Returning_gives_rows_and_writes_report_counts()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("serials");
        await Run(connection, $"CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
        await using var insert = new RasterizedDbCommand($"INSERT INTO {table} (name) VALUES ($1) RETURNING id", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        var name = insert.Parameters.Add("name", "a");
        var first = Convert.ToInt64(await insert.ExecuteScalarAsync());
        name.TypedValue = "b";
        var second = Convert.ToInt64(await insert.ExecuteScalarAsync());
        Assert.True(second > first);

        Assert.Equal(2, await Run(connection, $"UPDATE {table} SET name = name || '!'"));
        Assert.Equal(1, await Run(connection, $"DELETE FROM {table} WHERE id = {first}"));
        Assert.Equal(1, await Run(connection, $"DELETE FROM {table} WHERE id = {second}", RasterizedDbResultFormat.Text));
    }

    [Fact]
    public async Task Notices_reach_the_connection()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var notices = new List<RasterizedDbNotice>();
        connection.Notice += (_, e) => notices.Add(e.Notice);
        await Run(connection, "DROP TABLE IF EXISTS never_was_here_binary");
        await Run(connection, "DROP TABLE IF EXISTS never_was_here_text", RasterizedDbResultFormat.Text);
        Assert.Contains(notices, n => n.Message.Contains("never_was_here_binary"));
        Assert.Contains(notices, n => n.Message.Contains("never_was_here_text"));
    }

    [Fact]
    public async Task Many_statements_run_side_by_side_on_pooled_connections()
    {
        if (servers.Trust("Maximum Pool Size=4") is not { } connectionString)
        {
            return;
        }

        await using var source = new RasterizedDbDataSource(connectionString);
        var table = Table("parallel");
        await using (var setup = await source.OpenConnectionAsync())
        {
            await Run(setup, $"CREATE TABLE {table} (id INTEGER NOT NULL, v TEXT, PRIMARY KEY (id))");
        }

        await Task.WhenAll(Enumerable.Range(0, 64).Select(async i =>
        {
            await using var connection = await source.OpenConnectionAsync();
            await using var command = new RasterizedDbCommand($"INSERT INTO {table} VALUES ($1, $2)", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
            command.Parameters.Add("id", i);
            command.Parameters.Add("v", $"row {i}");
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }));

        await using var check = await source.OpenConnectionAsync();
        Assert.Equal(64L, await new RasterizedDbCommand($"SELECT count(*) FROM {table}", check) { ResultFormat = RasterizedDbResultFormat.Binary }.ExecuteScalarAsync());
    }

    [Fact]
    public async Task The_retrying_transaction_commits()
    {
        if (servers.Trust() is not { } connectionString)
        {
            return;
        }

        await using var source = new RasterizedDbDataSource(connectionString);
        var table = Table("retry");
        var total = await source.ExecuteInTransactionAsync(async (connection, _, token) =>
        {
            await Run(connection, $"CREATE TABLE IF NOT EXISTS {table} (id INTEGER NOT NULL, PRIMARY KEY (id))");
            await Run(connection, $"INSERT INTO {table} VALUES (1), (2)");
            return Convert.ToInt64(await new RasterizedDbCommand($"SELECT count(*) FROM {table}", connection) { ResultFormat = RasterizedDbResultFormat.Binary }.ExecuteScalarAsync(token));
        });
        Assert.Equal(2, total);
    }

    [Fact]
    public async Task The_key_value_cache_stores_bytes_and_typed_values()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var kv = connection.Kv;
        var key = $"k:{Guid.NewGuid():N}";
        Assert.Null(await kv.GetAsync(key));
        await kv.SetAsync(key, "värde");
        Assert.Equal("värde", await kv.GetStringAsync(key));
        Assert.True(await kv.ExistsAsync(key));
        Assert.False(await kv.SetIfAbsentAsync(key, "other"));
        Assert.True(await kv.ExpireAsync(key, TimeSpan.FromMinutes(5)));
        Assert.InRange((await kv.TtlAsync(key))!.Value, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(5));
        Assert.Equal(1, await kv.DeleteAsync(key));

        Assert.Equal(5, await kv.IncrementAsync(key + ":n", 5));
        Assert.Equal(7, await kv.IncrementAsync(key + ":n", 2));

        var hits = kv.Map<string, long>($"hits{Guid.NewGuid():N}");
        await hits.SetAsync("/index", 41);
        Assert.Equal(42, await hits.IncrementAsync("/index"));
        Assert.Equal(42, await hits.GetAsync("/index"));
        Assert.Equal(["/index"], await hits.KeysAsync());

        var sessions = kv.Map<Guid, Session>($"sessions{Guid.NewGuid():N}");
        var id = Guid.NewGuid();
        await sessions.SetAsync(id, new Session(7, "ada"), TimeSpan.FromMinutes(1));
        Assert.Equal(new Session(7, "ada"), await sessions.GetAsync(id));
        Assert.Equal((false, null), await sessions.TryGetAsync(Guid.NewGuid()));
        Assert.Equal(1, await sessions.ClearAsync());

        Assert.Equal(1, await kv.HashSetAsync(key + ":h", "field", "value"));
        Assert.Equal("value"u8.ToArray(), await kv.HashGetAsync(key + ":h", "field"));
        Assert.Single(await kv.HashGetAllAsync(key + ":h"));
    }

    private sealed record Session(long UserId, string Name);

    [Fact]
    public async Task Admin_calls_read_the_catalogue()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("described");
        await Run(connection, $"CREATE TABLE {table} (id BIGINT NOT NULL, email TEXT NOT NULL, note TEXT, PRIMARY KEY (id))");
        var admin = connection.Admin;
        Assert.Contains(await admin.TablesAsync(), t => t.Name == table);
        var columns = await admin.ColumnsAsync(table);
        Assert.Equal(["id", "email", "note"], columns.Select(c => c.Name));
        Assert.True(columns[0].PrimaryKey);
        Assert.True(columns[2].Nullable);
        await admin.CreateIndexAsync(new RasterizedDbIndex(table, "email").Unique().IfNotExists());
        Assert.True((await admin.ColumnsAsync(table))[1].Unique);
        Assert.NotEmpty(await admin.ConfigAsync());
        Assert.NotEmpty(await admin.DatabaseStatsAsync());

        var schema = connection.GetSchema("Columns", [table]);
        Assert.Equal(3, schema.Rows.Count);
    }

    [Fact]
    public async Task A_password_logs_in_over_a_socket_and_over_tls()
    {
        if (servers.Scram() is not { } server)
        {
            return;
        }

        foreach (var connectionString in (string[])[server.Socket, server.Tcp, server.Tcp + "Tls Engine=Managed", server.Tcp + "Result Format=Binary"])
        {
            await using var connection = new RasterizedDbConnection(connectionString);
            await connection.OpenAsync();
            Assert.Equal(1, Convert.ToInt32(await new RasterizedDbCommand("SELECT 1", connection).ExecuteScalarAsync()));
        }

        var wrong = new RasterizedDbConnectionStringBuilder(server.Tcp) { Password = "hunter3", Pooling = false };
        await using (var connection = new RasterizedDbConnection(wrong.ConnectionString))
        {
            var error = await Assert.ThrowsAsync<RasterizedDbException>(() => connection.OpenAsync());
            Assert.Equal(RasterizedDbErrorKind.Authentication, error.Kind);
            Assert.DoesNotContain("hunter", error.Message);
        }

        var nobody = new RasterizedDbConnectionStringBuilder(server.Socket) { Username = "bob", Pooling = false };
        await using (var connection = new RasterizedDbConnection(nobody.ConnectionString))
        {
            await Assert.ThrowsAsync<RasterizedDbException>(() => connection.OpenAsync());
        }
    }

    [Fact]
    public async Task A_bulk_insert_loads_rows_in_few_statements()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("bulk");
        await Run(connection, $"CREATE TABLE {table} (id INTEGER NOT NULL, name TEXT, at TIMESTAMPTZ, PRIMARY KEY (id))");
        var rows = Enumerable.Range(0, 2500).Select(i => (IReadOnlyList<object?>)[i, i % 7 == 0 ? null : $"n'{i}", DateTime.UnixEpoch.AddSeconds(i)]);
        Assert.Equal(2500, await connection.BulkInsertAsync(table, ["id", "name", "at"], rows));
        await using var check = new RasterizedDbCommand($"SELECT count(*), count(name), max(id) FROM {table}", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        await using var reader = await check.ExecuteReaderAsync();
        Assert.True(reader.Read());
        Assert.Equal(2500, reader.GetInt64(0));
        Assert.Equal(2500 - 358, reader.GetInt64(1));
        Assert.Equal(2499, reader.GetInt64(2));
    }

    [Fact]
    public void The_synchronous_calls_work_too()
    {
        if (servers.Trust() is not { } connectionString)
        {
            return;
        }

        using var connection = new RasterizedDbConnection(connectionString);
        connection.Open();
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand("SELECT $1::bigint + 1");
        command.ResultFormat = RasterizedDbResultFormat.Binary;
        command.Parameters.AddWithValue(41L);
        Assert.Equal(42L, Convert.ToInt64(command.ExecuteScalar()));
        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(42L, reader.GetInt64(0));
        }

        transaction.Commit();
        Assert.False(connection.InTransaction);
    }
}
