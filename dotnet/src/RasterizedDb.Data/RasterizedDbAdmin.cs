using System.Globalization;
using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data;

/// <summary>
/// The server's administrative statements as typed calls: stores, table placement, indexes, partitions,
/// roles, backups, configuration. From <see cref="RasterizedDbConnection.Admin"/>.
/// </summary>
/// <remarks>
/// Native rows carry no column names, so each introspection statement is read by position. A server
/// that adds or reorders a column of one of them needs the matching change here.
/// </remarks>
public readonly struct RasterizedDbAdmin
{
    private readonly RasterizedDbConnection _connection;

    internal RasterizedDbAdmin(RasterizedDbConnection connection) => _connection = connection;

    // ------------------------------------------------------------------ helpers

    private RasterizedDbCommand Command(string sql) => new(sql, _connection) { ResultFormat = RasterizedDbResultFormat.Binary };

    private async Task<long> Run(string sql, CancellationToken cancellation)
    {
        using var command = Command(sql);
        await using var reader = await command.ExecuteReaderAsync(cancellation).ConfigureAwait(false);
        return Math.Max(0, reader.RowsAffected);
    }

    private async Task<List<T>> Query<T>(string sql, Func<RasterizedDbDataReader, T> read, CancellationToken cancellation)
    {
        using var command = Command(sql);
        await using var reader = await command.ExecuteReaderAsync(cancellation).ConfigureAwait(false);
        var list = new List<T>();
        while (reader.Read())
        {
            list.Add(read(reader));
        }

        return list;
    }

    private Task<List<string>> Lines(string sql, CancellationToken cancellation) => Query(sql, static r => r.GetString(0), cancellation);

    private static string Text(RasterizedDbDataReader r, int i) => r.IsDBNull(i) ? string.Empty : r.GetString(i);

    private static string? Optional(RasterizedDbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

    private static string? Blank(RasterizedDbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i) is { Length: > 0 } s ? s : null;

    internal static string BareWord(string name, string what)
    {
        var ok = name.Length is > 0 and <= 128 && (char.IsAsciiLetter(name[0]) || name[0] == '_');
        foreach (var c in name)
        {
            ok &= char.IsAsciiLetterOrDigit(c) || c is '_' or '-';
        }

        return ok ? name : throw new ArgumentException($"{what} \"{name}\" must be letters, digits, '_' and '-', starting with a letter or '_'");
    }

    internal static string Ident(string name) => LiteralWriter.QuoteIdentifier(name);

    internal static string OptionValue(string key, string value)
    {
        if (value.AsSpan().IndexOfAny("\n\r\t\0") >= 0)
        {
            throw new ArgumentException($"the value of {key} holds a control character, which the store grammar cannot carry");
        }

        if (!value.Contains('\''))
        {
            return $"'{value}'";
        }

        if (!value.Contains('"'))
        {
            return $"\"{value}\"";
        }

        throw new ArgumentException($"the value of {key} holds both ' and \", which the store grammar cannot quote");
    }

    internal static string RoleName(string name)
    {
        if (name.Length == 0)
        {
            throw new ArgumentException("a role name must not be empty");
        }

        foreach (var c in name)
        {
            if (char.IsWhiteSpace(c) || c is '"' or '\'' or '\0')
            {
                throw new ArgumentException($"role name \"{name}\" must not hold whitespace or quotes");
            }
        }

        return name;
    }

    // ------------------------------------------------------------------- stores

    /// <summary>Registers a store (<c>STORAGE ADD STORE</c>).</summary>
    public Task<List<string>> AddStoreAsync(RasterizedDbStore store, CancellationToken cancellationToken = default) => Lines(store.ToSql(), cancellationToken);

    /// <summary>Changes a store's settings (<c>STORAGE ALTER STORE … SET</c>).</summary>
    public Task<List<string>> AlterStoreAsync(string name, IEnumerable<KeyValuePair<string, string>> settings, CancellationToken cancellationToken = default)
    {
        BareWord(name, "a store name");
        var list = new List<string>();
        foreach (var (key, value) in settings)
        {
            BareWord(key, "a store setting");
            // The SET list is split on every comma and found by its last " SET ", neither quote-aware.
            if (value.Contains(',') || value.Contains(" SET ", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"the value of {key} holds a ',' or ' SET ', which ALTER STORE cannot carry");
            }

            list.Add($"{key} = {OptionValue(key, value)}");
        }

        if (list.Count == 0)
        {
            throw new ArgumentException("ALTER STORE needs at least one setting");
        }

        return Lines($"STORAGE ALTER STORE {name} SET {string.Join(", ", list)}", cancellationToken);
    }

    public Task<List<string>> RotateStoreCredentialsAsync(string name, string accessKeyId, string secretAccessKey, CancellationToken cancellationToken = default) =>
        AlterStoreAsync(name, [new("ACCESS_KEY_ID", accessKeyId), new("SECRET_ACCESS_KEY", secretAccessKey)], cancellationToken);

    public Task<List<string>> DropStoreAsync(string name, CancellationToken cancellationToken = default) =>
        Lines($"STORAGE DROP STORE {BareWord(name, "a store name")}", cancellationToken);

    /// <summary>Drops a store if it is registered; whether it was. Two statements: the listing, then the drop.</summary>
    public async Task<bool> DropStoreIfExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        if (await StoreAsync(name, cancellationToken).ConfigureAwait(false) is null)
        {
            return false;
        }

        await DropStoreAsync(name, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<List<RasterizedDbStoreInfo>> StoresAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW STORAGE STORES",
            static r => new RasterizedDbStoreInfo(
                Text(r, 0), Text(r, 1), Text(r, 2), Text(r, 3), Text(r, 4), Text(r, 5), Text(r, 6), Text(r, 7), Text(r, 8),
                r.GetInt64(9), r.GetBoolean(10), r.GetBoolean(11)),
            cancellationToken);

    public async Task<RasterizedDbStoreInfo?> StoreAsync(string name, CancellationToken cancellationToken = default) =>
        (await StoresAsync(cancellationToken).ConfigureAwait(false)).Find(s => s.Name == name);

    /// <summary>Where a table that does not exist yet will be created (<c>STORAGE PLACE TABLE</c>).</summary>
    public Task<List<string>> PlaceTableAsync(string table, RasterizedDbPlacement placement, CancellationToken cancellationToken = default) =>
        Lines($"STORAGE PLACE TABLE {BareWord(table, "a table name")} AT {placement.Spec()}", cancellationToken);

    /// <summary>Moves an existing table (<c>STORAGE MOVE TABLE</c>).</summary>
    public Task<List<string>> MoveTableAsync(string table, RasterizedDbPlacement placement, CancellationToken cancellationToken = default) =>
        Lines($"STORAGE MOVE TABLE {BareWord(table, "a table name")} TO {placement.Spec()}", cancellationToken);

    public Task<List<RasterizedDbLocationInfo>> StorageLocationsAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW STORAGE LOCATIONS",
            static r => new RasterizedDbLocationInfo(Text(r, 0), Text(r, 1), r.GetInt64(2), r.GetBoolean(3), r.GetBoolean(4), r.GetInt64(5), r.GetInt64(6), r.GetInt64(7)),
            cancellationToken);

    public Task<List<RasterizedDbMetric>> StorageStatsAsync(CancellationToken cancellationToken = default) => Metrics("SHOW STORAGE STATS", cancellationToken);

    // ------------------------------------------------------------------ indexes

    public Task CreateIndexAsync(RasterizedDbIndex index, CancellationToken cancellationToken = default) => Run(index.ToSql(), cancellationToken);

    public Task DropIndexAsync(string name, bool ifExists = false, CancellationToken cancellationToken = default) =>
        Run($"DROP INDEX {(ifExists ? "IF EXISTS " : "")}{Ident(name)}", cancellationToken);

    /// <summary>Writes a table's persistent indexes to disk now (<c>rdb_index_checkpoint</c>).</summary>
    public async Task<string> IndexCheckpointAsync(string table, CancellationToken cancellationToken = default)
    {
        var lines = await Lines($"SELECT rdb_index_checkpoint({LiteralWriter.QuoteText(table)})", cancellationToken).ConfigureAwait(false);
        return lines.Count > 0 ? lines[0] : throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "rdb_index_checkpoint answered no row");
    }

    // --------------------------------------------------------------- partitions

    public Task PartitionByHashAsync(string table, IReadOnlyList<string> columns, int? partitions = null, CancellationToken cancellationToken = default)
    {
        if (columns.Count is 0 or > 3)
        {
            throw new ArgumentException("a hash partition key has one to three columns");
        }

        var sql = new StringBuilder($"ALTER TABLE {Ident(table)} PARTITION BY HASH ({string.Join(", ", columns.Select(Ident))})");
        if (partitions is { } n)
        {
            sql.Append(CultureInfo.InvariantCulture, $" PARTITIONS {n}");
        }

        return Run(sql.ToString(), cancellationToken);
    }

    public Task<List<RasterizedDbPartitionInfo>> PartitionsAsync(string table, CancellationToken cancellationToken = default) =>
        Query(
            $"SHOW PARTITIONS {Ident(table)}",
            static r => new RasterizedDbPartitionInfo(r.GetInt64(0), Text(r, 1), Text(r, 2), Optional(r, 3), Text(r, 4), r.GetInt64(5), r.GetInt64(6)),
            cancellationToken);

    // -------------------------------------------------------------- maintenance

    /// <summary><c>VACUUM [FULL] [table]</c>; returns the count it reported.</summary>
    public Task<long> VacuumAsync(string? table = null, bool full = false, CancellationToken cancellationToken = default) =>
        Run($"VACUUM{(full ? " FULL" : "")}{(table is null ? "" : " " + Ident(table))}", cancellationToken);

    public Task<List<RasterizedDbTableInfo>> TablesAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW TABLES",
            static r => new RasterizedDbTableInfo(
                Text(r, 0), Text(r, 1), r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.GetInt64(5), r.GetInt64(6), r.GetInt64(7), r.GetInt64(8),
                r.GetBoolean(9), r.GetBoolean(10), Text(r, 11)),
            cancellationToken);

    public Task<List<RasterizedDbColumnInfo>> ColumnsAsync(string table, CancellationToken cancellationToken = default) =>
        Query(
            $"DESCRIBE {Ident(table)}",
            static r => new RasterizedDbColumnInfo(
                r.GetInt64(0), Text(r, 1), Text(r, 2), r.GetInt64(3), r.GetInt64(4), r.GetBoolean(5), r.GetBoolean(6), r.GetBoolean(7),
                Blank(r, 8), r.GetBoolean(9), r.GetBoolean(10), Blank(r, 11), r.GetBoolean(12)),
            cancellationToken);

    public Task<List<RasterizedDbConfigEntry>> ConfigAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW CONFIG",
            static r => new RasterizedDbConfigEntry(Text(r, 0), Text(r, 1), Blank(r, 2), r.GetBoolean(3), Text(r, 4), Text(r, 5), Blank(r, 6), Blank(r, 7), Blank(r, 8)),
            cancellationToken);

    /// <summary><c>SET CONFIG section.key = value</c>; returns what the server said about it.</summary>
    public async Task<string> SetConfigAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        var dot = key.IndexOf('.');
        if (dot < 0)
        {
            throw new ArgumentException($"\"{key}\" is not section.key");
        }

        BareWord(key[..dot], "a config section");
        BareWord(key[(dot + 1)..], "a config key");
        // The value is read unquoted, to the end of the statement.
        var ok = value.Length > 0;
        foreach (var c in value)
        {
            ok &= char.IsAsciiLetterOrDigit(c) || "._:-+/".Contains(c);
        }

        if (!ok)
        {
            throw new ArgumentException($"config value \"{value}\" must be letters, digits and . _ : - + /");
        }

        return string.Join('\n', await Lines($"SET CONFIG {key} = {value}", cancellationToken).ConfigureAwait(false));
    }

    public Task<List<RasterizedDbMetric>> DatabaseStatsAsync(CancellationToken cancellationToken = default) => Metrics("SHOW STATS", cancellationToken);

    public Task<List<RasterizedDbMetric>> TableStatsAsync(string table, CancellationToken cancellationToken = default) =>
        Metrics($"SHOW TABLE STATS {Ident(table)}", cancellationToken);

    private Task<List<RasterizedDbMetric>> Metrics(string sql, CancellationToken cancellation) =>
        Query(
            sql,
            static r => new RasterizedDbMetric(Text(r, 0), r.IsDBNull(1) ? null : Convert.ToString(r.GetValue(1), CultureInfo.InvariantCulture), Optional(r, 2)),
            cancellation);

    // -------------------------------------------------------------------- roles

    public Task CreateRoleAsync(RasterizedDbRole role, CancellationToken cancellationToken = default) =>
        Run($"CREATE ROLE {RoleName(role.Name)}{role.Options()}", cancellationToken);

    public Task AlterRoleAsync(RasterizedDbRole role, CancellationToken cancellationToken = default) =>
        Run($"ALTER ROLE {RoleName(role.Name)}{role.Options()}", cancellationToken);

    public Task RenameRoleAsync(string from, string to, CancellationToken cancellationToken = default) =>
        Run($"ALTER ROLE {RoleName(from)} RENAME TO {RoleName(to)}", cancellationToken);

    /// <summary><c>ALTER ROLE … SET setting = value</c>, or <c>RESET setting</c> when <paramref name="value"/> is null.</summary>
    public Task SetRoleSettingAsync(string name, string setting, string? value, CancellationToken cancellationToken = default)
    {
        BareWord(setting.StartsWith("rdb.", StringComparison.Ordinal) ? setting[4..].Trim() : setting.Trim(), "setting");
        return Run(
            value is null
                ? $"ALTER ROLE {RoleName(name)} RESET {setting}"
                : $"ALTER ROLE {RoleName(name)} SET {setting} = {LiteralWriter.QuoteText(value)}",
            cancellationToken);
    }

    public Task GrantRoleAsync(string role, string member, bool withAdminOption = false, CancellationToken cancellationToken = default) =>
        Run($"GRANT {RoleName(role)} TO {RoleName(member)}{(withAdminOption ? " WITH ADMIN OPTION" : "")}", cancellationToken);

    public Task RevokeRoleAsync(string role, string member, CancellationToken cancellationToken = default) =>
        Run($"REVOKE {RoleName(role)} FROM {RoleName(member)}", cancellationToken);

    public Task GrantAsync(RasterizedDbGrant grant, CancellationToken cancellationToken = default) => Run(grant.ToSql(grant: true), cancellationToken);

    public Task RevokeAsync(RasterizedDbGrant grant, CancellationToken cancellationToken = default) => Run(grant.ToSql(grant: false), cancellationToken);

    public Task ReassignOwnedAsync(string from, string to, CancellationToken cancellationToken = default) =>
        Run($"REASSIGN OWNED BY {RoleName(from)} TO {RoleName(to)}", cancellationToken);

    public Task<List<RasterizedDbRoleInfo>> RolesAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SELECT rolname, rolsuper, rolcanlogin, rolcreatedb, rolcreaterole, rolbypassrls, rolconnlimit, " +
            "rolvaliduntil::text, array_to_string(rolconfig, ',') FROM pg_catalog.pg_roles ORDER BY rolname",
            static r => new RasterizedDbRoleInfo(
                Text(r, 0), r.GetBoolean(1), r.GetBoolean(2), r.GetBoolean(3), r.GetBoolean(4), r.GetBoolean(5), r.GetInt64(6), Optional(r, 7),
                Optional(r, 8)?.Split(',', StringSplitOptions.RemoveEmptyEntries) ?? []),
            cancellationToken);

    public Task<List<RasterizedDbGrantInfo>> TableGrantsAsync(string table, CancellationToken cancellationToken = default) =>
        Query(
            "SELECT grantor, grantee, table_name, privilege_type, is_grantable FROM information_schema.table_privileges " +
            $"WHERE table_name = {LiteralWriter.QuoteText(table)} ORDER BY grantee, privilege_type",
            static r => new RasterizedDbGrantInfo(Text(r, 0), Text(r, 1), Text(r, 2), Text(r, 3), Text(r, 4) == "YES"),
            cancellationToken);

    /// <summary>Sets a role's password; null removes it.</summary>
    public Task SetRolePasswordAsync(string name, string? password, CancellationToken cancellationToken = default) =>
        Run($"ALTER ROLE {RoleName(name)} PASSWORD {(password is null ? "NULL" : LiteralWriter.QuoteText(password))}", cancellationToken);

    public Task DropRoleAsync(string name, bool ifExists = false, CancellationToken cancellationToken = default) =>
        Run($"DROP ROLE {(ifExists ? "IF EXISTS " : "")}{RoleName(name)}", cancellationToken);

    // ------------------------------------------------------------------ backups

    public Task<long> BackupDatabaseAsync(CancellationToken cancellationToken = default) => Run("BACKUP DATABASE", cancellationToken);

    public Task<long> BackupWalAsync(CancellationToken cancellationToken = default) => Run("BACKUP WAL", cancellationToken);

    public Task<long> BackupPruneAsync(CancellationToken cancellationToken = default) => Run("BACKUP PRUNE", cancellationToken);

    public Task<List<RasterizedDbBackupInfo>> BackupsAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW BACKUPS",
            static r => new RasterizedDbBackupInfo(Text(r, 0), Text(r, 1), Text(r, 2), r.GetInt64(3), r.GetInt64(4), r.GetInt64(5), r.GetInt64(6), Optional(r, 7)),
            cancellationToken);

    public Task<List<RasterizedDbMetric>> BackupStatusAsync(CancellationToken cancellationToken = default) => Metrics("SHOW BACKUP STATUS", cancellationToken);

    /// <summary>
    /// Restores a backup into <paramref name="toDirectory"/>, a directory on the server.
    /// <paramref name="target"/> is <c>LATEST</c>, <c>FROM SNAPSHOT 'id'</c>, <c>AT LSN n</c> or <c>AT TIME '…'</c>:
    /// see <see cref="RasterizedDbRestoreTarget"/>.
    /// </summary>
    public async Task<RasterizedDbRestoreReport> RestoreAsync(RasterizedDbRestoreTarget target, string toDirectory, CancellationToken cancellationToken = default)
    {
        var rows = await Query(
            $"RESTORE DATABASE {target.Sql} TO {LiteralWriter.QuoteText(toDirectory)}",
            static r => new RasterizedDbRestoreReport(Text(r, 0), Text(r, 1), Text(r, 2), r.GetInt64(3), r.GetInt64(4), r.GetInt64(5), r.GetInt64(6), r.GetInt64(7)),
            cancellationToken).ConfigureAwait(false);
        return rows.Count > 0 ? rows[0] : throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "RESTORE answered no row");
    }

    // -------------------------------------------------------------- replication

    public Task<List<RasterizedDbMetric>> ReplicationStatsAsync(CancellationToken cancellationToken = default) => Metrics("SHOW REPLICATION STATS", cancellationToken);

    /// <summary>The columns of <see cref="ReplicationPeersAsync"/>'s rows, in order.</summary>
    public static readonly IReadOnlyList<string> ReplicationPeerColumns =
    [
        "peer", "address", "remote_node", "connected", "paused", "acked_seq", "lag", "queued", "records_sent", "batches_sent", "dropped",
        "codec", "last_ratio", "last_error", "state",
    ];

    /// <summary><c>SHOW REPLICATION PEER STATUS</c>: one dictionary per peer, keyed by <see cref="ReplicationPeerColumns"/>.</summary>
    public Task<List<Dictionary<string, object?>>> ReplicationPeersAsync(CancellationToken cancellationToken = default) =>
        Query(
            "SHOW REPLICATION PEER STATUS",
            static r =>
            {
                var record = new Dictionary<string, object?>(r.FieldCount);
                for (var i = 0; i < r.FieldCount && i < ReplicationPeerColumns.Count; i++)
                {
                    record[ReplicationPeerColumns[i]] = r.IsDBNull(i) ? null : r.GetValue(i);
                }

                return record;
            },
            cancellationToken);

    /// <summary>Runs <c>REPLICATION &lt;statement&gt;</c> and returns its lines.</summary>
    public Task<List<string>> ReplicationAsync(string statement, CancellationToken cancellationToken = default) => Lines("REPLICATION " + statement, cancellationToken);
}

// ----------------------------------------------------------------------- types

public sealed record RasterizedDbStoreInfo(
    string Name, string Kind, string Endpoint, string Region, string Bucket, string Prefix, string Addressing, string Settings, string Credentials,
    long Tables, bool Opened, bool Supported);

public sealed record RasterizedDbLocationInfo(
    string Location, string Role, long Weight, bool Enabled, bool IsDatabaseDirectory, long Tables, long AvailableBytes, long TotalBytes);

public sealed record RasterizedDbTableInfo(
    string Name, string Storage, long RowSlots, long Columns, long DataBytes, long PointerBytes, long IndexBytes, long TotalBytes, long ReclaimableBytes,
    bool FiltersReady, bool Immutable, string PrimaryKey);

public sealed record RasterizedDbColumnInfo(
    long Position, string Name, string DataType, long Size, long Offset, bool Nullable, bool Unique, bool PrimaryKey, string? Default,
    bool Indexed, bool ZoneBounds, string? JsonTable, bool Dropped);

public sealed record RasterizedDbConfigEntry(
    string Section, string Key, string? Value, bool HotReload, string SetWith, string Help, string? Pending, string? Options, string? Source);

public sealed record RasterizedDbMetric(string Metric, string? Value, string? Detail);

public sealed record RasterizedDbPartitionInfo(long Pid, string Lo, string Hi, string? Location, string State, long RowSlots, long TotalBytes);

public sealed record RasterizedDbBackupInfo(
    string Kind, string Id, string CreatedAt, long StartLsn, long EndLsn, long UncompressedBytes, long CompressedBytes, string? Detail);

public sealed record RasterizedDbRestoreReport(
    string SnapshotId, string TargetDirectory, string RecoveredToLsn, long FilesRestored, long BytesRestored, long SegmentsReplayed,
    long RecordsApplied, long RecordsDiscarded);

public sealed record RasterizedDbRoleInfo(
    string Name, bool Superuser, bool Login, bool CreateDb, bool CreateRole, bool BypassRls, long ConnectionLimit, string? ValidUntil,
    IReadOnlyList<string> Config);

public sealed record RasterizedDbGrantInfo(string Grantor, string Grantee, string Table, string Privilege, bool Grantable);

/// <summary>What a restore recovers to.</summary>
public readonly struct RasterizedDbRestoreTarget
{
    internal readonly string Sql;

    private RasterizedDbRestoreTarget(string sql) => Sql = sql;

    public static RasterizedDbRestoreTarget Latest => new("LATEST");

    public static RasterizedDbRestoreTarget Snapshot(string id) => new("FROM SNAPSHOT " + LiteralWriter.QuoteText(id));

    public static RasterizedDbRestoreTarget Lsn(ulong lsn) => new("AT LSN " + lsn.ToString(CultureInfo.InvariantCulture));

    public static RasterizedDbRestoreTarget Time(string timestamp) => new("AT TIME " + LiteralWriter.QuoteText(timestamp));
}

/// <summary>Where a table's data lives.</summary>
public readonly struct RasterizedDbPlacement
{
    private readonly string _spec;

    private RasterizedDbPlacement(string spec) => _spec = spec;

    internal string Spec() => _spec ?? "'memory:'";

    public static RasterizedDbPlacement Memory => new("'memory:'");

    /// <summary>A registered key/value store.</summary>
    public static RasterizedDbPlacement Kv(string store) => new(LiteralWriter.QuoteText("kv:" + RasterizedDbAdmin.BareWord(store, "a store name")));

    public static RasterizedDbPlacement Directory(string path) =>
        path.StartsWith("kv:", StringComparison.Ordinal) || path.StartsWith("memory", StringComparison.Ordinal)
            ? throw new ArgumentException($"\"{path}\" would be read as a store or memory spec, not a directory")
            : new(LiteralWriter.QuoteText(path));
}

/// <summary>A store to register: an S3 bucket, or a directory.</summary>
public sealed class RasterizedDbStore
{
    private readonly string _name;
    private readonly bool _s3;
    private readonly List<KeyValuePair<string, string>> _options = [];
    private string? _accessKeyId;
    private string? _secretAccessKey;
    private string? _sessionToken;
    private bool _ifNotExists;

    private RasterizedDbStore(string name, bool s3)
    {
        _name = name;
        _s3 = s3;
    }

    public static RasterizedDbStore S3(string name, string endpoint, string bucket) =>
        new RasterizedDbStore(name, true).Option("ENDPOINT", endpoint).Option("BUCKET", bucket);

    public static RasterizedDbStore Directory(string name, string path) => new RasterizedDbStore(name, false).Option("PATH", path);

    /// <summary>Any other setting of the store grammar, by its name.</summary>
    public RasterizedDbStore Option(string key, string value)
    {
        _options.Add(new(RasterizedDbAdmin.BareWord(key, "a store option"), value));
        return this;
    }

    public RasterizedDbStore Region(string region) => Option("REGION", region);

    public RasterizedDbStore Prefix(string prefix) => Option("PREFIX", prefix);

    /// <summary><c>virtual</c> (bucket in the host name) or <c>path</c> addressing.</summary>
    public RasterizedDbStore PathAddressing(bool path = true) => Option("ADDRESSING", path ? "path" : "virtual");

    public RasterizedDbStore Credentials(string accessKeyId, string secretAccessKey)
    {
        _accessKeyId = accessKeyId;
        _secretAccessKey = secretAccessKey;
        return this;
    }

    public RasterizedDbStore SessionToken(string token)
    {
        _sessionToken = token;
        return this;
    }

    public RasterizedDbStore ChunkSize(ulong bytes) => Option("CHUNK_SIZE", bytes.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore PageSize(ulong bytes) => Option("PAGE_SIZE", bytes.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore CacheBytes(ulong bytes) => Option("CACHE_BYTES", bytes.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore SpillBytes(ulong bytes) => Option("SPILL_BYTES", bytes.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore CompactChunkBytes(ulong bytes) => Option("COMPACT_CHUNK_BYTES", bytes.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore ChunkFetchPages(ulong pages) => Option("CHUNK_FETCH_PAGES", pages.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore ReadaheadChunks(ulong chunks) => Option("READAHEAD_CHUNKS", chunks.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore MaxDeltas(ulong deltas) => Option("MAX_DELTAS", deltas.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore FlushIntervalMs(ulong milliseconds) => Option("FLUSH_INTERVAL_MS", milliseconds.ToString(CultureInfo.InvariantCulture));

    public RasterizedDbStore IfNotExists()
    {
        _ifNotExists = true;
        return this;
    }

    public string ToSql()
    {
        RasterizedDbAdmin.BareWord(_name, "a store name");
        if (_accessKeyId is null != _secretAccessKey is null || (_sessionToken is not null && _accessKeyId is null))
        {
            throw new ArgumentException("a store's access key id and secret go together, and a session token needs both");
        }

        if (!_s3 && _accessKeyId is not null)
        {
            throw new ArgumentException("a directory store takes no credentials");
        }

        var sql = new StringBuilder("STORAGE ADD STORE ");
        if (_ifNotExists)
        {
            sql.Append("IF NOT EXISTS ");
        }

        sql.Append(_name).Append(_s3 ? " s3" : " dir");
        var entries = new List<string>();
        foreach (var (key, value) in _options)
        {
            entries.Add($"{key} = {RasterizedDbAdmin.OptionValue(key, value)}");
        }

        foreach (var (key, value) in (ReadOnlySpan<(string, string?)>)[("ACCESS_KEY_ID", _accessKeyId), ("SECRET_ACCESS_KEY", _secretAccessKey), ("SESSION_TOKEN", _sessionToken)])
        {
            if (value is null)
            {
                continue;
            }

            if (value.Length == 0)
            {
                throw new ArgumentException($"{key} is empty");
            }

            entries.Add($"{key} = {RasterizedDbAdmin.OptionValue(key, value)}");
        }

        if (entries.Count > 0)
        {
            sql.Append(" WITH (").AppendJoin(", ", entries).Append(')');
        }

        return sql.ToString();
    }

    /// <summary>The store without its secrets.</summary>
    public override string ToString() => $"store {_name} ({(_s3 ? "s3" : "dir")})";
}

public enum RasterizedDbIndexMethod
{
    BTree,
    Hash,
    Gin,
    /// <summary>GIN with <c>jsonb_path_ops</c>.</summary>
    GinPathOps,
}

/// <summary>An index to create: <c>new RasterizedDbIndex("users", "email").Unique()</c>.</summary>
public sealed class RasterizedDbIndex(string table, params string[] columns)
{
    private string? _name;
    private RasterizedDbIndexMethod _method;
    private bool _unique;
    private bool _ifNotExists;

    public RasterizedDbIndex Name(string name)
    {
        _name = name;
        return this;
    }

    public RasterizedDbIndex Using(RasterizedDbIndexMethod method)
    {
        _method = method;
        return this;
    }

    /// <summary>A unique index is a UNIQUE constraint.</summary>
    public RasterizedDbIndex Unique()
    {
        _unique = true;
        return this;
    }

    public RasterizedDbIndex IfNotExists()
    {
        _ifNotExists = true;
        return this;
    }

    public string IndexName => _name ?? $"{table}_{string.Join('_', columns)}_{(_unique ? "key" : "idx")}";

    public string ToSql()
    {
        if (columns.Length == 0)
        {
            throw new ArgumentException("an index needs at least one column");
        }

        var list = string.Join(", ", columns.Select(RasterizedDbAdmin.Ident));
        var exists = _ifNotExists ? "IF NOT EXISTS " : "";
        if (_unique)
        {
            if (_method != RasterizedDbIndexMethod.BTree)
            {
                throw new NotSupportedException("a unique index is a UNIQUE constraint, which has no method");
            }

            return $"ALTER TABLE {RasterizedDbAdmin.Ident(table)} ADD CONSTRAINT {exists}{RasterizedDbAdmin.Ident(IndexName)} UNIQUE ({list})";
        }

        var (method, opclass) = _method switch
        {
            RasterizedDbIndexMethod.Hash => ("hash", ""),
            RasterizedDbIndexMethod.Gin => ("gin", ""),
            RasterizedDbIndexMethod.GinPathOps => ("gin", " jsonb_path_ops"),
            _ => ("btree", ""),
        };
        return $"CREATE INDEX {exists}{RasterizedDbAdmin.Ident(IndexName)} ON {RasterizedDbAdmin.Ident(table)} USING {method} ({list}{opclass})";
    }
}

/// <summary>A role to create or alter: <c>RasterizedDbRole.User("app").Password("…")</c>.</summary>
public sealed class RasterizedDbRole
{
    private string? _password;
    private readonly bool _login;
    private bool _superuser;
    private bool _createDb;
    private bool _createRole;
    private bool _bypassRls;
    private long? _connectionLimit;
    private string? _validUntil;

    private RasterizedDbRole(string name, bool login)
    {
        Name = name;
        _login = login;
    }

    public string Name { get; }

    /// <summary>A role that may log in.</summary>
    public static RasterizedDbRole User(string name) => new(name, true);

    /// <summary>A role that holds privileges for its members and cannot log in.</summary>
    public static RasterizedDbRole Group(string name) => new(name, false);

    public RasterizedDbRole Password(string password)
    {
        _password = password;
        return this;
    }

    public RasterizedDbRole Superuser()
    {
        _superuser = true;
        return this;
    }

    public RasterizedDbRole CreateDb()
    {
        _createDb = true;
        return this;
    }

    public RasterizedDbRole CreateRole()
    {
        _createRole = true;
        return this;
    }

    public RasterizedDbRole BypassRls()
    {
        _bypassRls = true;
        return this;
    }

    public RasterizedDbRole ConnectionLimit(long limit)
    {
        _connectionLimit = limit;
        return this;
    }

    public RasterizedDbRole ValidUntil(string timestamp)
    {
        _validUntil = timestamp;
        return this;
    }

    internal string Options()
    {
        var sql = new StringBuilder();
        sql.Append(_login ? " LOGIN" : " NOLOGIN").Append(_superuser ? " SUPERUSER" : " NOSUPERUSER");
        if (_createDb)
        {
            sql.Append(" CREATEDB");
        }

        if (_createRole)
        {
            sql.Append(" CREATEROLE");
        }

        if (_bypassRls)
        {
            sql.Append(" BYPASSRLS");
        }

        if (_connectionLimit is { } limit)
        {
            sql.Append(CultureInfo.InvariantCulture, $" CONNECTION LIMIT {limit}");
        }

        if (_validUntil is not null)
        {
            sql.Append(" VALID UNTIL ").Append(LiteralWriter.QuoteText(_validUntil));
        }

        if (_password is not null)
        {
            sql.Append(" PASSWORD ").Append(LiteralWriter.QuoteText(_password));
        }

        return sql.ToString();
    }

    /// <summary>The role without its password.</summary>
    public override string ToString() => $"role {Name}";
}

/// <summary>A grant or revoke: <c>RasterizedDbGrant.Privileges("SELECT", "INSERT").OnTable("orders").To("app")</c>.</summary>
public sealed class RasterizedDbGrant
{
    private readonly List<string> _privileges = [];
    private readonly List<string> _grantees = [];
    private string? _kind;
    private List<string>? _objects;
    private bool _grantOption;
    private bool _cascade;

    public static RasterizedDbGrant Privileges(params string[] privileges)
    {
        var grant = new RasterizedDbGrant();
        foreach (var privilege in privileges)
        {
            grant._privileges.Add(privilege.ToUpperInvariant());
        }

        return grant;
    }

    /// <summary>A privilege on some columns only: <c>Columns("UPDATE", "name", "email")</c>.</summary>
    public static RasterizedDbGrant Columns(string privilege, params string[] columns)
    {
        var grant = new RasterizedDbGrant();
        grant._privileges.Add($"{privilege.ToUpperInvariant()} ({string.Join(", ", columns.Select(RasterizedDbAdmin.Ident))})");
        return grant;
    }

    private RasterizedDbGrant On(string kind, string[]? objects)
    {
        _kind = kind;
        _objects = objects?.ToList();
        return this;
    }

    public RasterizedDbGrant OnTable(params string[] tables) => On("TABLE", tables);

    public RasterizedDbGrant OnAllTables() => On("ALL TABLES IN SCHEMA public", null);

    public RasterizedDbGrant OnSequences(params string[] sequences) => On("SEQUENCE", sequences);

    public RasterizedDbGrant OnFunctions(params string[] functions) => On("FUNCTION", functions);

    public RasterizedDbGrant To(string grantee)
    {
        _grantees.Add(grantee);
        return this;
    }

    public RasterizedDbGrant WithGrantOption()
    {
        _grantOption = true;
        return this;
    }

    public RasterizedDbGrant Cascade()
    {
        _cascade = true;
        return this;
    }

    public string ToSql(bool grant)
    {
        if (_kind is null)
        {
            throw new ArgumentException("a grant needs an object: OnTable, OnSequences, …");
        }

        if (_privileges.Count == 0 || _grantees.Count == 0)
        {
            throw new ArgumentException("a grant needs privileges and at least one grantee");
        }

        var on = _objects is null ? _kind : $"{_kind} {string.Join(", ", _objects.Select(RasterizedDbAdmin.Ident))}";
        var grantees = string.Join(", ", _grantees.Select(g => g.Equals("public", StringComparison.OrdinalIgnoreCase) ? "PUBLIC" : RasterizedDbAdmin.RoleName(g)));
        var privileges = string.Join(", ", _privileges);
        return grant
            ? $"GRANT {privileges} ON {on} TO {grantees}{(_grantOption ? " WITH GRANT OPTION" : "")}"
            : $"REVOKE {privileges} ON {on} FROM {grantees}{(_cascade ? " CASCADE" : "")}";
    }
}
