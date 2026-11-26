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

    private sealed class Visitor
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public long Visits { get; set; }
        public DateTime? LastSeen { get; set; }
        public List<string> Roles { get; set; } = [];
    }

    [Fact]
    public async Task Key_value_queries_and_hash_repositories()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var kv = connection.Kv;
        var scores = kv.Map<string, int>($"scores{Guid.NewGuid():N}");
        foreach (var (name, score) in new[] { ("ann", 10), ("bob", 25), ("amy", 40), ("a*b", 7) })
        {
            await scores.SetAsync(name, score);
        }

        Assert.Equal(4, await scores.Query().CountAsync());
        Assert.Equal(["a*b", "amy", "ann"], (await scores.Query().Prefix("a").KeysAsync()).Order());
        Assert.Equal(["a*b"], await scores.Query().Prefix("a*").KeysAsync());
        Assert.Equal(["amy", "bob"], (await scores.Query().Where((_, score) => score > 20).ToListAsync()).Select(e => e.Key).Order());
        Assert.Single(await scores.Query().WhereKey(k => k.Length == 3).Take(1).ToListAsync());
        Assert.Equal(2, await scores.Query().Where((_, score) => score <= 10).RemoveAsync());
        Assert.Equal(2, await scores.Query().CountAsync());

        var visitors = kv.Repository<Visitor, Guid>(v => v.Id, $"visitors{Guid.NewGuid():N}");
        var id = Guid.NewGuid();
        Assert.Null(await visitors.GetAsync(id));
        var seen = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
        await visitors.SaveAsync(new Visitor { Id = id, Name = "o'neil", Visits = 3, LastSeen = seen, Roles = ["admin", "dev"] }, TimeSpan.FromMinutes(5));
        var loaded = await visitors.GetAsync(id);
        Assert.NotNull(loaded);
        Assert.Equal((id, "o'neil", 3L), (loaded.Id, loaded.Name, loaded.Visits));
        Assert.Equal(seen, loaded.LastSeen!.Value);
        Assert.Equal(["admin", "dev"], loaded.Roles);
        Assert.Equal(5, await visitors.IncrementAsync(id, nameof(Visitor.Visits), 2));
        await visitors.SetFieldAsync(id, nameof(Visitor.Name), "renamed");
        loaded!.LastSeen = null;
        loaded.Name = "saved";
        loaded.Visits = 5;
        await visitors.SaveAsync(loaded);
        var again = await visitors.GetAsync(id);
        Assert.Equal(("saved", 5L, null), (again!.Name, again.Visits, again.LastSeen));
        Assert.True(await visitors.RemoveAsync(id));
        Assert.False(await visitors.ContainsAsync(id));
    }

    [Fact]
    public async Task The_migrator_applies_verifies_and_rolls_back()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("migrated");
        var history = Table("history");
        var first = new RasterizedDbMigrator().HistoryTable(history)
            .Step("001_create", $"CREATE TABLE {table} (id INTEGER NOT NULL, PRIMARY KEY (id))")
            .Down($"DROP TABLE {table}");
        Assert.Equal(["001_create"], (await first.StatusAsync(connection)).Pending);
        await Assert.ThrowsAsync<InvalidOperationException>(() => first.VerifyAsync(connection));
        Assert.Equal(["001_create"], (await first.ApplyAsync(connection)).Applied);
        Assert.Empty((await first.ApplyAsync(connection)).Applied);
        await first.VerifyAsync(connection);

        var second = new RasterizedDbMigrator().HistoryTable(history)
            .Step("001_create", $"CREATE TABLE {table} (id INTEGER NOT NULL, PRIMARY KEY (id))")
            .Down($"DROP TABLE {table}")
            .Step("002_fill", $"ALTER TABLE {table} ADD COLUMN name TEXT", $"INSERT INTO {table} (id, name) VALUES (1, 'a')")
            .Down($"DELETE FROM {table}", $"ALTER TABLE {table} DROP COLUMN name");
        Assert.Equal(["002_fill"], (await second.ApplyAsync(connection)).Applied);
        Assert.Equal("a", await new RasterizedDbCommand($"SELECT name FROM {table} WHERE id = 1", connection).ExecuteScalarAsync());

        // An applied step whose text changed, a step that fails (and leaves nothing behind), a string of two statements.
        var edited = new RasterizedDbMigrator().HistoryTable(history).Step("001_create", $"CREATE TABLE {table} (id BIGINT NOT NULL, PRIMARY KEY (id))");
        await Assert.ThrowsAsync<InvalidOperationException>(() => edited.StatusAsync(connection));
        var failing = new RasterizedDbMigrator().HistoryTable(Table("history"))
            .Step("001", $"CREATE TABLE {table}_x (id INTEGER NOT NULL, PRIMARY KEY (id))", "INSERT INTO no_such_table_here VALUES (1)");
        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.ApplyAsync(connection));
        Assert.False(connection.InTransaction);
        Assert.Equal(["001"], (await failing.StatusAsync(connection)).Pending);
        Assert.Throws<ArgumentException>(() => new RasterizedDbMigrator().Step("x", "SELECT 1; SELECT 2"));
        Assert.Throws<ArgumentException>(() => new RasterizedDbMigrator().Step("x", "COMMIT"));

        Assert.Equal(["002_fill", "001_create"], await second.RollbackAsync(connection, steps: 2));
        Assert.Equal(2, (await second.StatusAsync(connection)).Pending.Count);
    }

    [Fact]
    public async Task A_batch_sends_its_statements_in_one_request()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("batched");
        await Run(connection, $"CREATE TABLE {table} (id INTEGER NOT NULL, name TEXT, PRIMARY KEY (id))");
        await using var batch = connection.CreateBatch();
        for (var i = 1; i <= 3; i++)
        {
            var insert = batch.BatchCommands.Add($"INSERT INTO {table} (id, name) VALUES (@id, @name)");
            insert.Parameters.Add("id", i);
            insert.Parameters.AddWithValue("name", $"row; {i} 'quoted'");
        }

        batch.BatchCommands.Add($"SELECT id, name FROM {table} WHERE id > $1 ORDER BY id").Parameters.AddWithValue(1);
        batch.BatchCommands.Add($"SELECT count(*) AS total FROM {table}");
        await using (var reader = await batch.ExecuteReaderAsync())
        {
            Assert.Equal(3, reader.RecordsAffected);
            Assert.True(reader.Read());
            Assert.Equal((2, "row; 2 'quoted'"), (reader.GetInt32(0), reader.GetString(1)));
            Assert.True(reader.NextResult());
            Assert.True(reader.Read());
            Assert.Equal(3L, reader["total"]);
        }

        // One implicit transaction: the second statement's failure undoes the first.
        await using var failing = connection.CreateBatch();
        failing.BatchCommands.Add($"INSERT INTO {table} (id) VALUES (10)");
        failing.BatchCommands.Add($"INSERT INTO {table} (id) VALUES (1)");
        await Assert.ThrowsAsync<RasterizedDbException>(() => failing.ExecuteNonQueryAsync());
        Assert.Equal(3L, await new RasterizedDbCommand($"SELECT count(*) FROM {table}", connection).ExecuteScalarAsync());
        Assert.False(await connection.Admin.DropStoreIfExistsAsync("no_such_store"));
    }

    [Fact]
    public async Task Data_source_commands_share_one_multiplexed_connection()
    {
        if (servers.Trust() is not { } connectionString)
        {
            return;
        }

        await using var source = new RasterizedDbDataSource(connectionString);
        var table = Table("shared");
        await using (var create = source.CreateCommand($"CREATE TABLE {table} (id INTEGER NOT NULL, v TEXT, PRIMARY KEY (id))"))
        {
            await create.ExecuteNonQueryAsync();
        }

        // 200 statements in flight at once on the one connection, answered in any order.
        var written = await Task.WhenAll(Enumerable.Range(0, 200).Select(async i =>
        {
            await using var insert = source.CreateCommand($"INSERT INTO {table} VALUES ($1, $2)");
            insert.ResultFormat = RasterizedDbResultFormat.Binary;
            insert.Parameters.Add("id", i);
            insert.Parameters.Add("v", $"row {i}");
            return await insert.ExecuteNonQueryAsync();
        }));
        Assert.Equal(200, written.Sum());

        var read = await Task.WhenAll(Enumerable.Range(0, 200).Select(async i =>
        {
            await using var select = source.CreateCommand($"SELECT v FROM {table} WHERE id = @id");
            select.ResultFormat = i % 2 == 0 ? RasterizedDbResultFormat.Binary : RasterizedDbResultFormat.Text;
            select.Parameters.Add("id", i);
            return (string?)await select.ExecuteScalarAsync();
        }));
        Assert.Equal(Enumerable.Range(0, 200).Select(i => $"row {i}"), read);

        await using (var named = source.CreateCommand($"SELECT id, v FROM {table} WHERE id < 2 ORDER BY id"))
        await using (var reader = await named.ExecuteReaderAsync())
        {
            Assert.Equal("v", reader.GetName(1));
            Assert.True(reader.Read());
        }

        // An error on one request leaves the others, and the connection, alone.
        await using (var bad = source.CreateCommand("SELECT * FROM no_such_table_here"))
        {
            await Assert.ThrowsAsync<RasterizedDbException>(() => bad.ExecuteScalarAsync());
        }

        await using (var begin = source.CreateCommand("BEGIN"))
        {
            Assert.Equal(RasterizedDbErrorKind.Unsupported, (await Assert.ThrowsAsync<RasterizedDbException>(() => begin.ExecuteNonQueryAsync())).Kind);
        }

        using var sync = source.CreateCommand($"SELECT count(*) FROM {table}");
        sync.ResultFormat = RasterizedDbResultFormat.Binary;
        Assert.Equal(200L, sync.ExecuteScalar());
    }

    [Fact]
    public async Task Binary_results_get_their_column_names_from_a_describe()
    {
        await using var connection = await Open();
        if (connection is null)
        {
            return;
        }

        var table = Table("described_binary");
        await Run(connection, $"CREATE TABLE {table} (id INTEGER NOT NULL, \"Label\" TEXT, price NUMERIC(10,2), PRIMARY KEY (id))");
        await Run(connection, $"INSERT INTO {table} VALUES (1, 'one', 1.50)");
        await using var command = new RasterizedDbCommand($"SELECT id, \"Label\", price AS cost FROM {table} WHERE id = @id", connection) { ResultFormat = RasterizedDbResultFormat.Binary };
        command.Parameters.Add("id", 1);
        for (var round = 0; round < 2; round++)
        {
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(reader.Read());
            Assert.Equal(["id", "Label", "cost"], [reader.GetName(0), reader.GetName(1), reader.GetName(2)]);
            Assert.Equal(2, reader.GetOrdinal("cost"));
            Assert.Equal("one", reader["Label"]);
            Assert.Equal(1.50m, reader["cost"]);
        }

        // A write with RETURNING is described, never run a second time.
        await using (var insert = new RasterizedDbCommand($"INSERT INTO {table} (id, \"Label\") VALUES (2, 'two') RETURNING id AS new_id", connection) { ResultFormat = RasterizedDbResultFormat.Binary })
        await using (var reader = await insert.ExecuteReaderAsync())
        {
            Assert.True(reader.Read());
            Assert.Equal("new_id", reader.GetName(0));
        }

        Assert.Equal(2L, await new RasterizedDbCommand($"SELECT count(*) FROM {table}", connection) { ResultFormat = RasterizedDbResultFormat.Binary }.ExecuteScalarAsync());

        // Inside a failed block the names still come, and the block is left as it was.
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var select = new RasterizedDbCommand($"SELECT id AS first, price FROM {table} ORDER BY id", connection) { ResultFormat = RasterizedDbResultFormat.Binary })
        await using (var reader = await select.ExecuteReaderAsync())
        {
            await Assert.ThrowsAsync<RasterizedDbException>(() => Run(connection, "SELECT * FROM no_such_table_here"));
            Assert.Equal("first", reader.GetName(0));
            Assert.True(connection.InTransaction);
        }

        await transaction.RollbackAsync();
        var loaded = new DataTable();
        await using (var all = new RasterizedDbCommand($"SELECT id, \"Label\" FROM {table} ORDER BY id", connection) { ResultFormat = RasterizedDbResultFormat.Binary })
        await using (var rows = await all.ExecuteReaderAsync())
        {
            loaded.Load(rows);
        }

        Assert.Equal("Label", loaded.Columns[1].ColumnName);
        Assert.Equal(2, loaded.Rows.Count);
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
