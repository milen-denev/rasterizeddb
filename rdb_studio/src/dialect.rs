//! The two vocabularies, behind one interface.
//!
//! Every screen asks for the same six things — database stats, table stats, a
//! table list, a column list, the schema map, the configuration — and gets back
//! the same [`crate::model`] types. What differs is the SQL sent and, for
//! PostgreSQL, quite a lot of arithmetic to make `pg_catalog` produce the same
//! columns.
//!
//! # Why the column names are identical on both sides
//!
//! The parsing below is written once and shared. The alternative — a parser per
//! dialect — means every new metric is added in two places and the screens grow
//! branches on which server they are talking to. Making the *SQL* responsible
//! for producing `(metric, value, detail)` keeps the branch at the edge, where
//! it is one string constant.
//!
//! # What PostgreSQL cannot answer
//!
//! Two figures have no equivalent and are marked rather than faked:
//!
//! - **`reclaimable_bytes`** is estimated from `n_dead_tup` against the table's
//!   average row width. PostgreSQL does not track dead bytes, only dead tuples,
//!   and an estimate labelled as one is more use than a zero.
//! - **`filters_ready`** describes RasterizedDB's own index catching up with
//!   the rows. Nothing in PostgreSQL is ever behind in that sense, so it reads
//!   `true`.

use crate::model::{
    BackupEntry, ColumnInfo, ConfigEntry, DatabaseInfo, Flavor, Grid, PeerStatus, Relationship,
    RestoreReport, RestoreTarget, Stat, StorageLocation, TableInfo, TablePlacement,
};

// ---------------------------------------------------------------------------
// The statements
// ---------------------------------------------------------------------------

/// What to send for the whole-database statistics.
pub fn database_stats_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW DATABASE STATS",
        Flavor::Postgres => PG_DATABASE_STATS,
    }
}

/// What to send for the table list.
pub fn tables_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW TABLES",
        Flavor::Postgres => PG_TABLES,
    }
}

/// What to send for one table's statistics.
pub fn table_stats_sql(flavor: Flavor, table: &str) -> String {
    match flavor {
        Flavor::Rasterized => format!("SHOW TABLE STATS {}", quote_ident(table)),
        Flavor::Postgres => PG_TABLE_STATS.replace("$TABLE$", &quote_literal(table)),
    }
}

/// What to send for one table's columns.
pub fn columns_sql(flavor: Flavor, table: &str) -> String {
    match flavor {
        Flavor::Rasterized => format!("SHOW COLUMNS FROM {}", quote_ident(table)),
        Flavor::Postgres => PG_COLUMNS.replace("$TABLE$", &quote_literal(table)),
    }
}

/// What to send for the relationship graph.
pub fn schema_map_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW SCHEMA MAP",
        Flavor::Postgres => PG_SCHEMA_MAP,
    }
}

/// What to send for the runtime configuration.
pub fn config_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW CONFIG",
        Flavor::Postgres => PG_CONFIG,
    }
}

/// What to send for the database list.
pub fn databases_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW DATABASES",
        Flavor::Postgres => PG_DATABASES,
    }
}

/// The statement that changes one knob, or an explanation of why there is none.
///
/// PostgreSQL splits this in two and the split matters: `SET` lasts for the
/// session and `ALTER SYSTEM SET` is written to `postgresql.auto.conf` and
/// survives a restart but needs a reload to take effect. A console that sent
/// the first while showing the second would report a change that vanishes when
/// the tab is closed, so the persistent form is used and the reload is chained
/// onto it.
pub fn set_config_sql(flavor: Flavor, entry: &ConfigEntry, value: &str) -> Result<String, String> {
    if !entry.hot {
        return Err(format!(
            "{} is fixed at start-up on this server. Set it with: {}",
            entry.qualified(),
            entry.set_with
        ));
    }
    Ok(match flavor {
        Flavor::Rasterized => {
            format!("SET CONFIG {} = '{}'", entry.qualified(), escape_literal(value))
        }
        Flavor::Postgres => format!(
            "ALTER SYSTEM SET {} = '{}'; SELECT pg_reload_conf()",
            entry.key,
            escape_literal(value)
        ),
    })
}

/// A page of a table's rows, ordered so that paging is stable.
///
/// Ordering matters more than it looks. Without an `ORDER BY` neither engine
/// promises the same order twice, so page 2 can repeat a row from page 1 and
/// skip another — and an editor that then writes back by row position would
/// write to the wrong row. The key is the primary key where there is one; where
/// there is not, the console says so rather than paging on nothing.
pub fn page_sql(table: &str, order_by: Option<&str>, limit: usize, offset: usize) -> String {
    let mut sql = format!("SELECT * FROM {}", quote_ident(table));
    if let Some(key) = order_by {
        sql.push_str(&format!(" ORDER BY {}", quote_ident(key)));
    }
    sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
    sql
}

/// The exact row count, which both engines answer by scanning.
pub fn count_sql(table: &str) -> String {
    format!("SELECT COUNT(*) AS n FROM {}", quote_ident(table))
}

// ---------------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------------

/// Whether this dialect can *manage* backups, as opposed to describe them.
///
/// RasterizedDB backs up to S3 from SQL. PostgreSQL does not, and no amount of
/// catalogue reading changes that: `pg_basebackup`, WAL archiving and whatever
/// tool wraps them live outside the server's SQL surface. The console shows
/// PostgreSQL what it *can* see — whether archiving is on and whether it is
/// working — and disables the buttons rather than offering actions that would
/// fail.
pub fn backups_are_manageable(flavor: Flavor) -> bool {
    matches!(flavor, Flavor::Rasterized)
}

/// The state of the backup destination, as `(metric, value, detail)`.
pub fn backup_status_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW BACKUP STATUS",
        Flavor::Postgres => PG_BACKUP_STATUS,
    }
}

/// The catalogue of stored backups, or `None` where there is not one.
pub fn backups_sql(flavor: Flavor) -> Option<&'static str> {
    match flavor {
        Flavor::Rasterized => Some("SHOW BACKUPS"),
        // `pg_stat_archiver` counts archived files; it does not enumerate them,
        // and nothing in the server knows what a base backup is. There is no
        // list to show, so none is invented.
        Flavor::Postgres => None,
    }
}

/// Take a full snapshot now.
pub fn backup_now_sql() -> &'static str {
    "BACKUP DATABASE"
}

/// Cut the log and upload it — the point-in-time barrier.
pub fn archive_wal_sql() -> &'static str {
    "BACKUP WAL"
}

/// Apply the retention policy. **Deletes objects.**
pub fn prune_sql() -> &'static str {
    "BACKUP PRUNE"
}

/// Take a fresh full snapshot and delete every earlier generation.
/// **Deletes objects**, but only after the new snapshot has landed.
pub fn rollover_sql() -> &'static str {
    "BACKUP ROLLOVER"
}

/// Rebuild the database into `target_dir`, stopping where `target` says.
///
/// The directory is a string literal rather than an identifier: it is a path,
/// and quoting it as an identifier would make `"/var/lib/x"` a relation name.
pub fn restore_sql(target_dir: &str, target: &RestoreTarget) -> Result<String, String> {
    let dir = target_dir.trim();
    if dir.is_empty() {
        return Err("a restore needs a destination directory".to_string());
    }

    let mut sql = match target {
        RestoreTarget::Snapshot(id) if id.trim().is_empty() => {
            return Err("choose a snapshot, or pick a different recovery target".to_string());
        }
        RestoreTarget::Snapshot(id) => {
            format!("RESTORE DATABASE FROM SNAPSHOT {}", quote_literal(id.trim()))
        }
        _ => "RESTORE DATABASE FROM S3".to_string(),
    };

    sql.push_str(&format!(" TO {}", quote_literal(dir)));

    match target {
        RestoreTarget::Latest | RestoreTarget::Snapshot(_) => {}
        RestoreTarget::Lsn(lsn) => sql.push_str(&format!(" AT LSN {}", lsn)),
        RestoreTarget::Time(instant) if instant.trim().is_empty() => {
            return Err("a point-in-time restore needs an instant, as in 2026-08-20T10:00:00Z"
                .to_string());
        }
        RestoreTarget::Time(instant) => {
            sql.push_str(&format!(" AT TIME {}", quote_literal(instant.trim())))
        }
    }

    Ok(sql)
}

/// The command that replaces the *live* database, which no statement can do.
///
/// Shown rather than run: swapping the files under a server that has tables
/// open leaves every reader looking at storage that no longer describes what it
/// thinks it does, so the engine refuses it from SQL and the operation belongs
/// to start-up. A console that only said "not supported" would leave the
/// operator to find this in the documentation during an incident.
pub fn in_place_restore_command(status: &[Stat], target: &RestoreTarget) -> String {
    let value = |metric: &str| {
        status
            .iter()
            .find(|s| s.metric == metric)
            .map(|s| s.value.clone())
            .unwrap_or_default()
    };

    let mut command = format!(
        "rasterizeddb_core --location {}",
        shell_quote(&value("source_directory"))
    );
    if !value("bucket").is_empty() {
        command.push_str(&format!(" \\\n  --s3-bucket {}", shell_quote(&value("bucket"))));
    }
    let prefix = value("prefix");
    if !prefix.is_empty() {
        command.push_str(&format!(" --s3-prefix {}", shell_quote(prefix.trim_end_matches('/'))));
    }
    if !value("region").is_empty() {
        command.push_str(&format!(" --s3-region {}", shell_quote(&value("region"))));
    }
    command.push_str(" \\\n  --restore-from-s3 --restore-replace");
    match target {
        RestoreTarget::Latest => {}
        RestoreTarget::Snapshot(id) if !id.trim().is_empty() => {
            command.push_str(&format!(" --restore-snapshot {}", shell_quote(id.trim())));
        }
        RestoreTarget::Snapshot(_) => {}
        RestoreTarget::Lsn(lsn) => command.push_str(&format!(" --restore-at-lsn {}", lsn)),
        RestoreTarget::Time(instant) if !instant.trim().is_empty() => {
            command.push_str(&format!(" --restore-at-time {}", shell_quote(instant.trim())));
        }
        RestoreTarget::Time(_) => {}
    }
    command
}

/// Single-quotes a value for a shell, doubling nothing and escaping the one
/// character that matters.
fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-/:".contains(c))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn parse_backups(grid: &Grid) -> Vec<BackupEntry> {
    (0..grid.rows.len())
        .map(|row| BackupEntry {
            kind: grid.value(row, "kind").unwrap_or_default().to_string(),
            id: grid.value(row, "id").unwrap_or_default().to_string(),
            created_at: grid.value(row, "created_at").unwrap_or_default().to_string(),
            start_lsn: grid.number(row, "start_lsn").unwrap_or(0),
            end_lsn: grid.number(row, "end_lsn").unwrap_or(0),
            uncompressed_bytes: grid.number(row, "uncompressed_bytes").unwrap_or(0),
            compressed_bytes: grid.number(row, "compressed_bytes").unwrap_or(0),
            detail: grid.value(row, "detail").unwrap_or_default().to_string(),
        })
        .collect()
}

pub fn parse_restore_report(grid: &Grid) -> Option<RestoreReport> {
    if grid.rows.is_empty() {
        return None;
    }
    Some(RestoreReport {
        snapshot_id: grid.value(0, "snapshot_id").unwrap_or_default().to_string(),
        target_dir: grid.value(0, "target_dir").unwrap_or_default().to_string(),
        recovered_to_lsn: grid.value(0, "recovered_to_lsn").unwrap_or_default().to_string(),
        files_restored: grid.number(0, "files_restored").unwrap_or(0),
        bytes_restored: grid.number(0, "bytes_restored").unwrap_or(0),
        segments_replayed: grid.number(0, "segments_replayed").unwrap_or(0),
        records_applied: grid.number(0, "records_applied").unwrap_or(0),
        records_discarded: grid.number(0, "records_discarded").unwrap_or(0),
    })
}

/// What PostgreSQL can say about its own archiving.
///
/// Shaped as `(metric, value, detail)` so the same panel renders it, and named
/// so the console's health checks — `configured`, `archiving_suspended` — find
/// something meaningful on both sides. `archiving_suspended` is derived: an
/// archiver whose last *failure* is more recent than its last success is not
/// archiving, which is the same silent loss of point-in-time coverage the
/// RasterizedDB flag reports.
const PG_BACKUP_STATUS: &str = "\
SELECT metric, value, detail FROM (
  SELECT 1 AS ord, 'feature_compiled' AS metric, 'true' AS value,
         'PostgreSQL always has WAL archiving; whether it is on is the next row' AS detail
  UNION ALL SELECT 2, 'configured',
    (current_setting('archive_mode') NOT IN ('off'))::text,
    'archive_mode; backups themselves are taken outside the server'
  UNION ALL SELECT 3, 'archive_mode', current_setting('archive_mode'), ''
  UNION ALL SELECT 4, 'archive_command', coalesce(nullif(current_setting('archive_command'), ''), '(unset)'),
    'where archived WAL is sent'
  UNION ALL SELECT 5, 'archive_timeout', current_setting('archive_timeout'),
    'how long a partly-filled segment waits before being archived'
  UNION ALL SELECT 6, 'wal_level', current_setting('wal_level'), ''
  UNION ALL SELECT 7, 'source_directory', current_setting('data_directory'), ''
  UNION ALL SELECT 10, 'archived_count',
    (SELECT archived_count::text FROM pg_stat_archiver), 'WAL files archived successfully'
  UNION ALL SELECT 11, 'last_archived_wal',
    (SELECT coalesce(last_archived_wal, '(none)') FROM pg_stat_archiver), ''
  UNION ALL SELECT 12, 'last_archived_time',
    (SELECT coalesce(last_archived_time::text, '(never)') FROM pg_stat_archiver), ''
  UNION ALL SELECT 13, 'failed_count',
    (SELECT failed_count::text FROM pg_stat_archiver), 'archive_command failures'
  UNION ALL SELECT 14, 'last_failed_wal',
    (SELECT coalesce(last_failed_wal, '(none)') FROM pg_stat_archiver), ''
  UNION ALL SELECT 15, 'last_failed_time',
    (SELECT coalesce(last_failed_time::text, '(never)') FROM pg_stat_archiver), ''
  UNION ALL SELECT 16, 'archiving_suspended',
    (SELECT (last_failed_time IS NOT NULL
             AND (last_archived_time IS NULL OR last_failed_time > last_archived_time))::text
       FROM pg_stat_archiver),
    'the last archive attempt failed, so point-in-time coverage stops there'
  UNION ALL SELECT 20, 'in_recovery', pg_is_in_recovery()::text,
    'this server is a standby or is replaying a restore'
  UNION ALL SELECT 21, 'current_wal_lsn',
    CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text
         ELSE pg_current_wal_lsn()::text END, ''
) s ORDER BY ord";

// ---------------------------------------------------------------------------
// Importing another database
// ---------------------------------------------------------------------------

/// Where an import reads from, and what it should leave out.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportRequest {
    pub host: String,
    pub port: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub sslmode: SslMode,
    pub schema: String,
    /// Comma-separated. Empty means every table.
    pub exclude: String,
    pub target: String,
    /// Work out what would happen and report it, creating nothing.
    pub dry_run: bool,
    /// Leave primary keys, unique constraints and foreign keys off the target.
    pub skip_constraints: bool,
}

impl Default for ImportRequest {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: "5432".to_string(),
            database: String::new(),
            user: String::new(),
            password: String::new(),
            sslmode: SslMode::Prefer,
            schema: "public".to_string(),
            exclude: String::new(),
            target: String::new(),
            // On by default. An import creates a database and streams every
            // row of another one into it; the first thing anyone should do is
            // find out what it plans to do, and making them opt *out* of that
            // is the wrong way round.
            dry_run: true,
            skip_constraints: false,
        }
    }
}

/// What the source server does about TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
    VerifyFull,
}

impl SslMode {
    pub const ALL: [SslMode; 4] =
        [SslMode::Disable, SslMode::Prefer, SslMode::Require, SslMode::VerifyFull];

    pub fn label(&self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyFull => "verify-full",
        }
    }

    /// What it does and does not protect, for the line under the picker.
    ///
    /// `require` is worth being explicit about: it is libpq's meaning, which
    /// stops a passive eavesdropper and nothing else. An import sends a
    /// password and then every row of a database, so the difference between
    /// `require` and `verify-full` across an untrusted network is the whole
    /// database.
    pub fn caveat(&self) -> &'static str {
        match self {
            SslMode::Disable => {
                "The password and every row cross the network in cleartext."
            }
            SslMode::Prefer => {
                "Encrypts if the source offers it, otherwise cleartext. Nothing is verified."
            }
            SslMode::Require => {
                "Encrypted, but the source is not identified — anyone able to intercept the                  connection can present their own certificate and read the password and every                  row. Use verify-full across a network you do not trust."
            }
            SslMode::VerifyFull => "Encrypted, and the source's certificate is checked.",
        }
    }
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Whether this server can import from PostgreSQL at all.
pub fn import_is_supported(flavor: Flavor) -> bool {
    matches!(flavor, Flavor::Rasterized)
}

/// Builds the `IMPORT DATABASE` statement.
///
/// Every value goes in as a quoted literal, so a password with a quote in it
/// survives and cannot end the statement early.
pub fn import_sql(request: &ImportRequest) -> Result<String, String> {
    let host = request.host.trim();
    let database = request.database.trim();
    let user = request.user.trim();
    let target = request.target.trim();

    if host.is_empty() {
        return Err("the source needs a host".to_string());
    }
    if database.is_empty() {
        return Err("the source needs a database name".to_string());
    }
    if user.is_empty() {
        return Err("the source needs a user".to_string());
    }
    if target.is_empty() {
        return Err("name the database to import into".to_string());
    }
    let port: u16 = request
        .port
        .trim()
        .parse()
        .map_err(|_| format!("{:?} is not a port", request.port.trim()))?;

    let mut sql = format!(
        "IMPORT DATABASE FROM POSTGRES HOST {} PORT {} DATABASE {} USER {} PASSWORD {} SSLMODE {}",
        quote_literal(host),
        port,
        quote_literal(database),
        quote_literal(user),
        quote_literal(&request.password),
        quote_literal(request.sslmode.label()),
    );

    let schema = request.schema.trim();
    if !schema.is_empty() {
        sql.push_str(&format!(" SCHEMA {}", quote_literal(schema)));
    }

    let excluded: Vec<&str> = request
        .exclude
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    if !excluded.is_empty() {
        sql.push_str(&format!(" EXCLUDE TABLES ({})", excluded.join(", ")));
    }

    if request.skip_constraints {
        sql.push_str(" WITHOUT CONSTRAINTS");
    }
    if request.dry_run {
        sql.push_str(" DRY RUN");
    }

    sql.push_str(&format!(" INTO {}", target));
    Ok(sql)
}

/// The report an import returns, one line per row of its result.
/// Reads a result whose rows are lines of prose rather than fields.
///
/// The first column, whatever it is called. Several surfaces answer this way —
/// a rebalance plan, a filter tuning statement — and each names its one column
/// after itself, so matching on the name would need a list of them.
pub fn parse_report_lines(grid: &Grid) -> Vec<String> {
    (0..grid.rows.len())
        .filter_map(|row| grid.rows[row].first().and_then(|value| value.clone()))
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
}

pub fn parse_import_report(grid: &Grid) -> Vec<String> {
    (0..grid.rows.len())
        .map(|row| {
            grid.value(row, "import")
                .or_else(|| grid.rows[row].first().and_then(|v| v.as_deref()))
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

/// Node state and counters, as `(metric, value, detail)`.
pub fn replication_stats_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW REPLICATION STATS",
        Flavor::Postgres => PG_REPLICATION_STATS,
    }
}

/// A row per peer.
///
/// The two servers replicate in opposite directions and the page says so.
/// RasterizedDB is multi-master: every node accepts writes and ships them to
/// the others, so a "peer" is somewhere this node sends to. PostgreSQL streams
/// from a primary to standbys, so a row of `pg_stat_replication` is a standby
/// *receiving* from here. The columns are the same because the questions are —
/// who is it, is it connected, how far behind — but the direction is not, which
/// is why the page labels it rather than leaving it implied.
pub fn replication_peers_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW REPLICATION PEER STATUS",
        Flavor::Postgres => PG_REPLICATION_PEERS,
    }
}

/// Whether this dialect lets the console change replication from here.
pub fn replication_is_manageable(flavor: Flavor) -> bool {
    matches!(flavor, Flavor::Rasterized)
}

pub fn replication_enable_sql(enable: bool) -> &'static str {
    if enable { "REPLICATION ENABLE" } else { "REPLICATION DISABLE" }
}

pub fn peer_pause_sql(peer: &str, pause: bool) -> String {
    format!(
        "REPLICATION {} PEER {}",
        if pause { "PAUSE" } else { "RESUME" },
        quote_literal(peer)
    )
}

/// Re-sends a peer's whole stream from the beginning of what is retained.
pub fn peer_resync_sql(peer: &str) -> String {
    format!("REPLICATION RESYNC PEER {}", quote_literal(peer))
}

pub fn peer_drop_sql(peer: &str) -> String {
    format!("REPLICATION DROP PEER {}", quote_literal(peer))
}

pub fn peer_add_sql(peer: &str, address: &str) -> Result<String, String> {
    if peer.trim().is_empty() {
        return Err("a peer needs a name".to_string());
    }
    if address.trim().is_empty() {
        return Err("a peer needs an address, as in 10.0.0.2:61171".to_string());
    }
    Ok(format!(
        "REPLICATION ADD PEER {} ADDRESS {}",
        quote_literal(peer.trim()),
        quote_literal(address.trim())
    ))
}

// ---------------------------------------------------------------------------
// Storage placement
// ---------------------------------------------------------------------------
//
// RasterizedDB's storage pool and PostgreSQL's tablespaces answer the same
// question — which directory does this table's data live in, and how much room
// is left there — so the page renders both with the same columns. What differs
// is who decides: RasterizedDB has a placement *policy* that picks a directory
// per `CREATE TABLE` and a rebalance that moves tables afterwards, while
// PostgreSQL places a table where the statement or `default_tablespace` says
// and never moves one on its own. So the panel is read-write against one and
// read-only against the other, which the page says rather than leaving the
// buttons there and failing.

/// Pool-wide state, as `(metric, value, detail)`.
pub fn storage_stats_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW STORAGE STATS",
        Flavor::Postgres => PG_STORAGE_STATS,
    }
}

/// A row per directory.
pub fn storage_locations_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW STORAGE LOCATIONS",
        Flavor::Postgres => PG_STORAGE_LOCATIONS,
    }
}

/// A row per table, saying which directory holds it.
pub fn storage_placements_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Rasterized => "SHOW STORAGE PLACEMENTS",
        Flavor::Postgres => PG_STORAGE_PLACEMENTS,
    }
}

/// Whether this dialect lets the console change placement from here.
pub fn storage_is_manageable(flavor: Flavor) -> bool {
    matches!(flavor, Flavor::Rasterized)
}

pub fn storage_add_location_sql(
    path: &str,
    role: &str,
    weight: &str,
) -> Result<String, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("a location needs a path".to_string());
    }
    // Checked here as well as by the server, because the server's refusal
    // arrives as a failed statement and this arrives as the user types.
    if !path.starts_with('/') && !path.contains(":\\") {
        return Err(format!(
            "{} is relative; a storage location must be an absolute path",
            path
        ));
    }

    let weight = weight.trim();
    let weight: u32 = if weight.is_empty() {
        1
    } else {
        weight.parse().map_err(|_| "weight must be a whole number".to_string())?
    };
    if weight == 0 {
        return Err("weight must be at least 1; disable a location to take it out".to_string());
    }

    Ok(format!(
        "STORAGE ADD LOCATION {} WITH (ROLE = {}, WEIGHT = {})",
        quote_literal(path),
        quote_literal(role),
        weight
    ))
}

pub fn storage_drop_location_sql(path: &str) -> String {
    format!("STORAGE DROP LOCATION {}", quote_literal(path))
}

pub fn storage_enable_location_sql(path: &str, enable: bool) -> String {
    format!(
        "STORAGE {} LOCATION {}",
        if enable { "ENABLE" } else { "DISABLE" },
        quote_literal(path)
    )
}

pub fn storage_set_role_sql(path: &str, role: &str) -> String {
    format!(
        "STORAGE ALTER LOCATION {} SET ROLE = {}",
        quote_literal(path),
        quote_literal(role)
    )
}

pub fn storage_set_weight_sql(path: &str, weight: u32) -> String {
    format!("STORAGE ALTER LOCATION {} SET WEIGHT = {}", quote_literal(path), weight)
}

pub fn storage_set_policy_sql(policy: &str) -> String {
    format!("STORAGE SET POLICY = {}", quote_literal(policy))
}

pub fn storage_set_min_free_sql(size: &str) -> Result<String, String> {
    let size = size.trim();
    if size.is_empty() {
        return Err("a floor needs a size, as in 2GiB".to_string());
    }
    Ok(format!("STORAGE SET MIN_FREE = {}", quote_literal(size)))
}

pub fn storage_move_table_sql(table: &str, path: &str) -> String {
    format!("STORAGE MOVE TABLE {} TO {}", quote_ident(table), quote_literal(path))
}

pub fn storage_rebalance_sql(dry_run: bool) -> &'static str {
    if dry_run { "STORAGE REBALANCE DRY RUN" } else { "STORAGE REBALANCE" }
}

pub fn parse_storage_locations(grid: &Grid) -> Vec<StorageLocation> {
    (0..grid.rows.len())
        .map(|row| StorageLocation {
            path: grid.value(row, "location").unwrap_or_default().to_string(),
            role: grid.value(row, "role").unwrap_or_default().to_string(),
            weight: grid.number(row, "weight").unwrap_or(1),
            enabled: grid.flag(row, "enabled").unwrap_or(true),
            is_root: grid.flag(row, "is_database_directory").unwrap_or(false),
            tables: grid.number(row, "tables").unwrap_or(0),
            available_bytes: grid.number(row, "available_bytes").unwrap_or(0),
            total_bytes: grid.number(row, "total_bytes").unwrap_or(0),
        })
        .collect()
}

pub fn parse_storage_placements(grid: &Grid) -> Vec<TablePlacement> {
    (0..grid.rows.len())
        .map(|row| TablePlacement {
            table: grid.value(row, "table_name").unwrap_or_default().to_string(),
            location: grid.value(row, "location").unwrap_or_default().to_string(),
            mirrored: grid.flag(row, "mirrored").unwrap_or(false),
        })
        .collect()
}

/// PostgreSQL's tablespaces, named to match so the same panel renders them.
///
/// `policy` is reported as the `default_tablespace` setting, because that is
/// genuinely what decides where a `CREATE TABLE` with no `TABLESPACE` clause
/// puts its data — the same role the placement policy plays. An empty setting
/// means `pg_default`, which is what the `COALESCE` says rather than leaving
/// the field blank.
const PG_STORAGE_STATS: &str = "\
SELECT metric, value, detail FROM (
  SELECT 1 AS ord, 'policy' AS metric,
    COALESCE(NULLIF(current_setting('default_tablespace'), ''), 'pg_default') AS value,
    'where a CREATE TABLE with no TABLESPACE clause puts its data' AS detail
  UNION ALL SELECT 2, 'min_free_bytes', '0',
    'PostgreSQL has no free-space floor; it writes until the filesystem refuses'
  UNION ALL SELECT 3, 'root',
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'data_directory'), ''),
    'the data directory'
  UNION ALL SELECT 4, 'locations', (SELECT count(*)::text FROM pg_tablespace),
    'tablespaces on this server'
  UNION ALL SELECT 5, 'primaries', (SELECT count(*)::text FROM pg_tablespace),
    'every tablespace can hold tables'
  UNION ALL SELECT 6, 'backups', '0',
    'PostgreSQL does not mirror a tablespace; that is what replication is for'
  UNION ALL SELECT 7, 'disabled', '0', 'a tablespace is never taken out of service'
  UNION ALL SELECT 8, 'tables',
    (SELECT count(*)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')),
    'ordinary tables in this database'
  UNION ALL SELECT 9, 'next_placement',
    COALESCE(NULLIF(current_setting('default_tablespace'), ''), 'pg_default'),
    'where a table created now would go'
) s ORDER BY ord";

/// One row per tablespace.
///
/// Free space is not reported: PostgreSQL exposes no per-tablespace figure, and
/// working one out would mean resolving each location to a filesystem from
/// inside the server. Zero here is what the page reads as "not known", which is
/// the honest answer rather than a wrong number.
const PG_STORAGE_LOCATIONS: &str = "\
SELECT
  COALESCE(NULLIF(pg_catalog.pg_tablespace_location(t.oid), ''),
           (SELECT setting FROM pg_settings WHERE name = 'data_directory'),
           t.spcname) AS location,
  'primary'::text AS role,
  1::bigint AS weight,
  true AS enabled,
  (t.spcname = 'pg_default') AS is_database_directory,
  (SELECT count(*) FROM pg_class c
     WHERE c.relkind IN ('r','p')
       AND COALESCE(NULLIF(c.reltablespace, 0), (SELECT dattablespace FROM pg_database
                                                 WHERE datname = current_database())) = t.oid
  )::bigint AS tables,
  0::bigint AS available_bytes,
  0::bigint AS total_bytes
FROM pg_tablespace t
ORDER BY (t.spcname = 'pg_default') DESC, t.spcname";

/// One row per table, resolving the tablespace a table inherits when its own is
/// unset — which is how PostgreSQL records "the database's default" and reads
/// as no tablespace at all if taken literally.
const PG_STORAGE_PLACEMENTS: &str = "\
SELECT
  c.relname AS table_name,
  COALESCE(NULLIF(pg_catalog.pg_tablespace_location(
             COALESCE(NULLIF(c.reltablespace, 0),
                      (SELECT dattablespace FROM pg_database WHERE datname = current_database()))
           ), ''),
           (SELECT setting FROM pg_settings WHERE name = 'data_directory')) AS location,
  false AS mirrored
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.relname";

pub fn parse_peers(grid: &Grid) -> Vec<PeerStatus> {
    (0..grid.rows.len())
        .map(|row| PeerStatus {
            name: grid.value(row, "peer").unwrap_or_default().to_string(),
            address: grid.value(row, "address").unwrap_or_default().to_string(),
            remote_node: grid.number(row, "remote_node").unwrap_or(0),
            connected: grid.flag(row, "connected").unwrap_or(false),
            paused: grid.flag(row, "paused").unwrap_or(false),
            acked_seq: grid.number(row, "acked_seq").unwrap_or(0),
            lag: grid.number(row, "lag").unwrap_or(0),
            queued: grid.number(row, "queued").unwrap_or(0),
            records_sent: grid.number(row, "records_sent").unwrap_or(0),
            batches_sent: grid.number(row, "batches_sent").unwrap_or(0),
            dropped: grid.number(row, "dropped").unwrap_or(0),
            codec: grid.value(row, "codec").unwrap_or_default().to_string(),
            last_ratio: grid.value(row, "last_ratio").unwrap_or_default().to_string(),
            last_error: grid.value(row, "last_error").unwrap_or_default().to_string(),
        })
        .collect()
}

/// PostgreSQL's side, named to match so the same panel renders it.
///
/// `active` means something different here and the detail says so: a
/// PostgreSQL primary is "replicating" when a standby is streaming from it,
/// which is a property of who connected rather than a switch on this server.
const PG_REPLICATION_STATS: &str = "\
SELECT metric, value, detail FROM (
  SELECT 1 AS ord, 'active' AS metric,
    (EXISTS (SELECT 1 FROM pg_stat_replication) OR pg_is_in_recovery())::text AS value,
    'a standby is streaming from here, or this server is one' AS detail
  UNION ALL SELECT 2, 'role', CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END, ''
  UNION ALL SELECT 3, 'node_id', (SELECT system_identifier::text FROM pg_control_system()),
    'the cluster identifier this server was initialised with'
  UNION ALL SELECT 4, 'cluster', coalesce(nullif(current_setting('cluster_name'), ''), '(unset)'), ''
  UNION ALL SELECT 5, 'listen',
    current_setting('listen_addresses') || ':' || current_setting('port'), ''
  UNION ALL SELECT 6, 'mode', current_setting('synchronous_commit'),
    'how long a commit waits for its standbys'
  UNION ALL SELECT 7, 'synchronous_standby_names',
    coalesce(nullif(current_setting('synchronous_standby_names'), ''), '(none)'), ''
  UNION ALL SELECT 8, 'wal_level', current_setting('wal_level'), ''
  UNION ALL SELECT 10, 'peers', (SELECT count(*)::text FROM pg_stat_replication),
    'standbys streaming from this server'
  UNION ALL SELECT 11, 'peers_connected',
    (SELECT count(*) FILTER (WHERE state = 'streaming')::text FROM pg_stat_replication), ''
  UNION ALL SELECT 12, 'replication_slots',
    (SELECT count(*)::text FROM pg_replication_slots), ''
  UNION ALL SELECT 13, 'inactive_slots',
    (SELECT count(*) FILTER (WHERE NOT active)::text FROM pg_replication_slots),
    'a slot nobody is using still holds WAL, and will fill the disk'
  UNION ALL SELECT 20, 'local_sequence',
    CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text
         ELSE pg_current_wal_lsn()::text END, ''
  UNION ALL SELECT 21, 'max_peer_lag',
    coalesce((SELECT max(pg_wal_lsn_diff(sent_lsn, replay_lsn))::bigint::text
                FROM pg_stat_replication), '0'),
    'bytes the furthest-behind standby has not replayed'
  UNION ALL SELECT 30, 'max_wal_senders', current_setting('max_wal_senders'), ''
  UNION ALL SELECT 31, 'wal_keep_size', current_setting('wal_keep_size'),
    'history kept for a standby that falls behind'
) s ORDER BY ord";

/// One row per streaming standby. Column names match the RasterizedDB form.
///
/// `lag` is in **bytes of WAL** here and in **transactions** there — the same
/// question with different units, which the page labels rather than silently
/// mixing.
const PG_REPLICATION_PEERS: &str = "\
SELECT
  coalesce(nullif(application_name, ''), client_addr::text, 'standby')::text AS peer,
  coalesce(client_addr::text || ':' || client_port::text, '(local)')         AS address,
  pid::text                                                                  AS remote_node,
  (state = 'streaming')::text                                                AS connected,
  (state = 'backup' OR state = 'startup')::text                              AS paused,
  coalesce(pg_wal_lsn_diff(replay_lsn, '0/0')::bigint, 0)::text              AS acked_seq,
  coalesce(pg_wal_lsn_diff(sent_lsn, replay_lsn)::bigint, 0)::text           AS lag,
  coalesce(pg_wal_lsn_diff(sent_lsn, flush_lsn)::bigint, 0)::text            AS queued,
  coalesce(pg_wal_lsn_diff(sent_lsn, '0/0')::bigint, 0)::text                AS records_sent,
  '0'                                                                        AS batches_sent,
  '0'                                                                        AS dropped,
  coalesce(state, '')                                                        AS codec,
  coalesce(sync_state, '')                                                   AS last_ratio,
  coalesce(reply_time::text, '')                                             AS last_error
FROM pg_stat_replication
ORDER BY peer";

// ---------------------------------------------------------------------------
// Parsing — one implementation, because the column names agree
// ---------------------------------------------------------------------------

pub fn parse_stats(grid: &Grid) -> Vec<Stat> {
    (0..grid.rows.len())
        .map(|row| Stat {
            metric: grid.value(row, "metric").unwrap_or_default().to_string(),
            value: grid.value(row, "value").unwrap_or_default().to_string(),
            detail: grid.value(row, "detail").unwrap_or_default().to_string(),
        })
        .collect()
}

pub fn parse_tables(grid: &Grid) -> Vec<TableInfo> {
    (0..grid.rows.len())
        .map(|row| TableInfo {
            name: grid.value(row, "table_name").unwrap_or_default().to_string(),
            row_slots: grid.number(row, "row_slots").unwrap_or(0),
            columns: grid.number(row, "columns").unwrap_or(0),
            data_bytes: grid.number(row, "data_bytes").unwrap_or(0),
            index_bytes: grid.number(row, "index_bytes").unwrap_or(0),
            total_bytes: grid.number(row, "total_bytes").unwrap_or(0),
            reclaimable_bytes: grid.number(row, "reclaimable_bytes").unwrap_or(0),
            primary_key: grid.value(row, "primary_key").unwrap_or_default().to_string(),
            immutable: grid.flag(row, "immutable").unwrap_or(false),
            indexed: grid.flag(row, "filters_ready").unwrap_or(true),
        })
        .collect()
}

pub fn parse_columns(grid: &Grid) -> Vec<ColumnInfo> {
    (0..grid.rows.len())
        .filter(|row| !grid.flag(*row, "is_dropped").unwrap_or(false))
        .map(|row| ColumnInfo {
            position: grid.number(row, "position").unwrap_or(0),
            name: grid.value(row, "column_name").unwrap_or_default().to_string(),
            data_type: grid.value(row, "data_type").unwrap_or_default().to_string(),
            nullable: grid.flag(row, "nullable").unwrap_or(true),
            unique: grid.flag(row, "is_unique").unwrap_or(false),
            primary_key: grid.flag(row, "is_primary_key").unwrap_or(false),
            default: grid.value(row, "default_expression").unwrap_or_default().to_string(),
            indexed: grid.flag(row, "is_indexed").unwrap_or(false),
            json_table: grid.value(row, "json_table").unwrap_or_default().to_string(),
        })
        .collect()
}

pub fn parse_relationships(grid: &Grid) -> Vec<Relationship> {
    (0..grid.rows.len())
        .map(|row| Relationship {
            kind: grid.value(row, "kind").unwrap_or("fk").to_string(),
            from_table: grid.value(row, "from_table").unwrap_or_default().to_string(),
            from_column: grid.value(row, "from_column").unwrap_or_default().to_string(),
            to_table: grid.value(row, "to_table").unwrap_or_default().to_string(),
            to_column: grid.value(row, "to_column").unwrap_or_default().to_string(),
            constraint: grid.value(row, "constraint_name").unwrap_or_default().to_string(),
            on_delete: grid.value(row, "on_delete").unwrap_or_default().to_string(),
        })
        .collect()
}

pub fn parse_config(grid: &Grid) -> Vec<ConfigEntry> {
    (0..grid.rows.len())
        .map(|row| ConfigEntry {
            section: grid.value(row, "section").unwrap_or_default().to_string(),
            key: grid.value(row, "key").unwrap_or_default().to_string(),
            value: grid.value(row, "value").unwrap_or_default().to_string(),
            hot: grid.flag(row, "hot_reload").unwrap_or(false),
            set_with: grid.value(row, "set_with").unwrap_or_default().to_string(),
            help: grid.value(row, "help").unwrap_or_default().to_string(),
        })
        .collect()
}

pub fn parse_databases(grid: &Grid) -> Vec<DatabaseInfo> {
    (0..grid.rows.len())
        .map(|row| DatabaseInfo {
            name: grid.value(row, "database_name").unwrap_or_default().to_string(),
            location: grid.value(row, "location").unwrap_or_default().to_string(),
            tables: grid.number(row, "tables").unwrap_or(0),
            sessions: grid.number(row, "sessions").unwrap_or(0),
            is_current: grid.flag(row, "is_current").unwrap_or(false),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Quoting
// ---------------------------------------------------------------------------

/// Quotes an identifier for either dialect.
///
/// Everything a console builds around a name a *server* gave it still goes
/// through here. The name came back from the catalogue, so it is not attacker
/// input in the usual sense — but a table called `orders"; DROP TABLE users; --`
/// is a legal PostgreSQL identifier, and a console that concatenates one
/// unquoted turns a listing into an execution.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quotes a value as a SQL string literal.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", escape_literal(value))
}

/// Doubles every quote, which is how both dialects escape one.
///
/// Backslash escapes are deliberately not used: PostgreSQL only honours them
/// under `standard_conforming_strings = off`, which has been off by default
/// since 9.1, so a `\'` would be two characters rather than an escape and the
/// literal would run on into the rest of the statement.
pub fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// A value as it should be written into an `INSERT` or `UPDATE`.
///
/// `None` is a genuine `NULL` and must be written unquoted; everything else is
/// a quoted literal, whatever the column's type, because both engines cast a
/// string literal to the column type and neither accepts a bare `12` for a
/// `TEXT` column.
pub fn sql_value(value: Option<&str>) -> String {
    match value {
        None => "NULL".to_string(),
        Some(text) => quote_literal(text),
    }
}

// ---------------------------------------------------------------------------
// The PostgreSQL side
// ---------------------------------------------------------------------------

/// Whole-database statistics, shaped as `(metric, value, detail)`.
///
/// `ord` keeps the sections together; without it the `UNION ALL` order is not
/// guaranteed and the overview reshuffles between refreshes.
const PG_DATABASE_STATS: &str = "\
SELECT metric, value, detail FROM (
  SELECT 1 AS ord, 'name' AS metric, current_database()::text AS value,
         'database this session is attached to' AS detail
  UNION ALL SELECT 2, 'server_version', current_setting('server_version'), ''
  UNION ALL SELECT 3, 'location', current_setting('data_directory'), 'data directory'
  UNION ALL SELECT 4, 'tables',
         (SELECT count(*)::text FROM pg_stat_user_tables), 'user tables'
  UNION ALL SELECT 5, 'columns',
         (SELECT count(*)::text FROM information_schema.columns
          WHERE table_schema NOT IN ('pg_catalog','information_schema')), ''
  UNION ALL SELECT 6, 'row_slots',
         (SELECT coalesce(sum(n_live_tup + n_dead_tup),0)::text FROM pg_stat_user_tables),
         'live plus dead tuples; an upper bound on live rows'
  UNION ALL SELECT 7, 'total_bytes',
         pg_database_size(current_database())::text, 'everything this database occupies'
  UNION ALL SELECT 8, 'data_bytes',
         (SELECT coalesce(sum(pg_table_size(relid)),0)::text FROM pg_stat_user_tables), ''
  UNION ALL SELECT 9, 'index_bytes',
         (SELECT coalesce(sum(pg_indexes_size(relid)),0)::text FROM pg_stat_user_tables), ''
  UNION ALL SELECT 10, 'reclaimable_bytes',
         (SELECT coalesce(sum(
             n_dead_tup * (pg_table_size(relid) / greatest(n_live_tup + n_dead_tup, 1))
          ),0)::text FROM pg_stat_user_tables),
         'estimated from dead tuples; PostgreSQL does not track dead bytes'
  UNION ALL SELECT 11, 'foreign_keys',
         (SELECT count(*)::text FROM pg_constraint WHERE contype = 'f'), ''
  UNION ALL SELECT 12, 'unique_constraints',
         (SELECT count(*)::text FROM pg_constraint WHERE contype IN ('u','p')), ''
  UNION ALL SELECT 20, 'transaction_next_xid', txid_current()::text, ''
  UNION ALL SELECT 21, 'connection_backends',
         (SELECT count(*)::text FROM pg_stat_activity
          WHERE datname = current_database()), 'sessions on this database'
  UNION ALL SELECT 22, 'connection_max', current_setting('max_connections'), ''
  UNION ALL SELECT 30, 'cache_hit_ratio',
         (SELECT coalesce(round(100.0 * blks_hit / greatest(blks_hit + blks_read, 1), 2), 0)::text
          FROM pg_stat_database WHERE datname = current_database()),
         'percent of block reads served from shared buffers'
  UNION ALL SELECT 31, 'cache_shared_buffers', current_setting('shared_buffers'), ''
  UNION ALL SELECT 40, 'query_commits',
         (SELECT xact_commit::text FROM pg_stat_database WHERE datname = current_database()), ''
  UNION ALL SELECT 41, 'query_rollbacks',
         (SELECT xact_rollback::text FROM pg_stat_database WHERE datname = current_database()), ''
  UNION ALL SELECT 42, 'query_tuples_returned',
         (SELECT tup_returned::text FROM pg_stat_database WHERE datname = current_database()), ''
  UNION ALL SELECT 50, 'wal_level', current_setting('wal_level'), ''
  UNION ALL SELECT 51, 'checkpoint_timeout', current_setting('checkpoint_timeout'), ''
  UNION ALL SELECT 60, 'autovacuum_enabled', current_setting('autovacuum'), ''
  UNION ALL SELECT 70, 'server_uptime',
         (SELECT date_trunc('second', now() - pg_postmaster_start_time())::text), ''
) s ORDER BY ord";

/// The table list. Column names match `SHOW TABLES` exactly.
const PG_TABLES: &str = "\
SELECT
  t.relname::text                                        AS table_name,
  (s.n_live_tup + s.n_dead_tup)::text                    AS row_slots,
  (SELECT count(*) FROM pg_attribute a
    WHERE a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped)::text AS columns,
  pg_table_size(t.oid)::text                             AS data_bytes,
  (pg_total_relation_size(t.oid)
     - pg_table_size(t.oid) - pg_indexes_size(t.oid))::text AS pointer_bytes,
  pg_indexes_size(t.oid)::text                           AS index_bytes,
  pg_total_relation_size(t.oid)::text                    AS total_bytes,
  (s.n_dead_tup * (pg_table_size(t.oid)
     / greatest(s.n_live_tup + s.n_dead_tup, 1)))::text   AS reclaimable_bytes,
  'true'                                                 AS filters_ready,
  'false'                                                AS immutable,
  coalesce((SELECT string_agg(a.attname, ', ' ORDER BY k.ord)
     FROM pg_constraint c
     CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
     JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
    WHERE c.conrelid = t.oid AND c.contype = 'p'), '')   AS primary_key
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = t.oid
WHERE t.relkind IN ('r','p')
  AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY t.relname";

/// One table's statistics, as `(metric, value, detail)`.
const PG_TABLE_STATS: &str = "\
SELECT metric, value, detail FROM (
  SELECT 1 AS ord, 'table' AS metric, c.relname::text AS value, '' AS detail
  FROM pg_class c WHERE c.oid = $TABLE$::regclass
  UNION ALL SELECT 2, 'columns',
    (SELECT count(*)::text FROM pg_attribute
      WHERE attrelid = $TABLE$::regclass AND attnum > 0 AND NOT attisdropped), ''
  UNION ALL SELECT 3, 'dropped_columns',
    (SELECT count(*)::text FROM pg_attribute
      WHERE attrelid = $TABLE$::regclass AND attnum > 0 AND attisdropped), ''
  UNION ALL SELECT 10, 'row_slots',
    (SELECT (n_live_tup + n_dead_tup)::text FROM pg_stat_user_tables
      WHERE relid = $TABLE$::regclass), 'live plus dead tuples'
  UNION ALL SELECT 11, 'live_tuples',
    (SELECT n_live_tup::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 12, 'dead_tuples',
    (SELECT n_dead_tup::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 20, 'data_bytes', pg_table_size($TABLE$::regclass)::text, ''
  UNION ALL SELECT 21, 'index_bytes', pg_indexes_size($TABLE$::regclass)::text, ''
  UNION ALL SELECT 22, 'total_bytes', pg_total_relation_size($TABLE$::regclass)::text, ''
  UNION ALL SELECT 23, 'reclaimable_bytes',
    (SELECT (n_dead_tup * (pg_table_size(relid) / greatest(n_live_tup + n_dead_tup, 1)))::text
       FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass),
    'estimated from dead tuples'
  UNION ALL SELECT 30, 'index_scans',
    (SELECT coalesce(idx_scan,0)::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 31, 'sequential_scans',
    (SELECT coalesce(seq_scan,0)::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 32, 'rows_inserted',
    (SELECT n_tup_ins::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 33, 'rows_updated',
    (SELECT n_tup_upd::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 34, 'rows_deleted',
    (SELECT n_tup_del::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 35, 'hot_updates',
    (SELECT n_tup_hot_upd::text FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass),
    'updates that stayed on the same page'
  UNION ALL SELECT 40, 'vacuum_runs',
    (SELECT (coalesce(vacuum_count,0) + coalesce(autovacuum_count,0))::text
       FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), 'manual plus autovacuum'
  UNION ALL SELECT 41, 'vacuum_last',
    (SELECT coalesce(greatest(last_vacuum, last_autovacuum)::text, 'never')
       FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 42, 'vacuum_last_analyze',
    (SELECT coalesce(greatest(last_analyze, last_autoanalyze)::text, 'never')
       FROM pg_stat_user_tables WHERE relid = $TABLE$::regclass), ''
  UNION ALL SELECT 50, 'primary_key',
    coalesce((SELECT string_agg(a.attname, ', ' ORDER BY k.ord)
       FROM pg_constraint c
       CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
       JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
      WHERE c.conrelid = $TABLE$::regclass AND c.contype = 'p'), ''), ''
  UNION ALL SELECT 51, 'indexes',
    (SELECT count(*)::text FROM pg_index WHERE indrelid = $TABLE$::regclass), ''
) s ORDER BY ord";

/// One table's columns. Column names match `SHOW COLUMNS FROM`.
const PG_COLUMNS: &str = "\
SELECT
  a.attnum::text                                              AS position,
  a.attname::text                                             AS column_name,
  format_type(a.atttypid, a.atttypmod)                        AS data_type,
  greatest(a.attlen, 0)::text                                 AS size,
  '0'                                                         AS offset,
  (NOT a.attnotnull)::text                                    AS nullable,
  EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid
            AND i.indisunique AND a.attnum = ANY (i.indkey))::text AS is_unique,
  EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid
            AND i.indisprimary AND a.attnum = ANY (i.indkey))::text AS is_primary_key,
  coalesce(pg_get_expr(d.adbin, d.adrelid), '')               AS default_expression,
  EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid
            AND a.attnum = ANY (i.indkey))::text              AS is_indexed,
  'false'                                                     AS zone_bounds,
  ''                                                          AS json_table,
  a.attisdropped::text                                        AS is_dropped
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = $TABLE$::regclass AND a.attnum > 0
ORDER BY a.attnum";

/// The relationship graph. `json` edges have no PostgreSQL equivalent — a
/// `jsonb` column is one value, not a reference — so only `fk` edges appear.
const PG_SCHEMA_MAP: &str = "\
SELECT
  'fk'                                    AS kind,
  child.relname::text                     AS from_table,
  ca.attname::text                        AS from_column,
  parent.relname::text                    AS to_table,
  pa.attname::text                        AS to_column,
  c.conname::text                         AS constraint_name,
  CASE c.confdeltype
    WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
    WHEN 'n' THEN 'SET NULL'  WHEN 'd' THEN 'SET DEFAULT' ELSE '' END AS on_delete,
  CASE c.confupdtype
    WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
    WHEN 'n' THEN 'SET NULL'  WHEN 'd' THEN 'SET DEFAULT' ELSE '' END AS on_update
FROM pg_constraint c
JOIN pg_class child  ON child.oid  = c.conrelid
JOIN pg_class parent ON parent.oid = c.confrelid
CROSS JOIN LATERAL unnest(c.conkey)  WITH ORDINALITY AS ck(attnum, ord)
CROSS JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS pk(attnum, ord)
JOIN pg_attribute ca ON ca.attrelid = c.conrelid  AND ca.attnum = ck.attnum
JOIN pg_attribute pa ON pa.attrelid = c.confrelid AND pa.attnum = pk.attnum
WHERE c.contype = 'f' AND ck.ord = pk.ord
ORDER BY child.relname, c.conname, ck.ord";

/// The runtime configuration.
///
/// `hot_reload` is derived from `pg_settings.context`, which is exactly the
/// question being asked: `internal` and `postmaster` need a restart, everything
/// else does not.
const PG_CONFIG: &str = "\
SELECT
  coalesce(category, 'general')                    AS section,
  name                                             AS key,
  coalesce(setting, '') || coalesce(' ' || unit, '') AS value,
  (context NOT IN ('internal','postmaster'))::text AS hot_reload,
  CASE context
    WHEN 'internal'   THEN 'compiled in; not settable'
    WHEN 'postmaster' THEN 'postgresql.conf, then restart'
    WHEN 'sighup'     THEN 'ALTER SYSTEM SET, then pg_reload_conf()'
    ELSE 'ALTER SYSTEM SET, then pg_reload_conf()'
  END                                              AS set_with,
  coalesce(short_desc, '')                         AS help
FROM pg_settings
ORDER BY category, name";

const PG_DATABASES: &str = "\
SELECT
  d.datname::text                                  AS database_name,
  coalesce(t.spcname::text, 'default tablespace')  AS location,
  (SELECT count(*) FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','p')
      AND n.nspname NOT IN ('pg_catalog','information_schema'))::text AS tables,
  (SELECT count(*) FROM pg_stat_activity a WHERE a.datname = d.datname)::text AS sessions,
  (d.datname = 'postgres')::text                   AS is_default,
  (d.datname = current_database())::text           AS is_current
FROM pg_database d
LEFT JOIN pg_tablespace t ON t.oid = d.dattablespace
WHERE NOT d.datistemplate
ORDER BY d.datname";

#[cfg(test)]
mod tests {
    use super::*;

    // -- storage placement -----------------------------------------------

    /// A directory path is user input that reaches the server inside a literal,
    /// and a path is one of the few places a quote legitimately appears.
    #[test]
    fn a_quote_in_a_directory_path_cannot_end_the_statement() {
        let sql = storage_add_location_sql("/mnt/it's here", "primary", "1").unwrap();
        assert_eq!(
            sql,
            "STORAGE ADD LOCATION '/mnt/it''s here' WITH (ROLE = 'primary', WEIGHT = 1)"
        );

        let sql = storage_drop_location_sql("/mnt/x'; DROP TABLE users; --");
        assert_eq!(sql, "STORAGE DROP LOCATION '/mnt/x''; DROP TABLE users; --'");

        // The table name is an identifier, not a literal, and takes the other
        // quoting.
        assert_eq!(
            storage_move_table_sql("orders", "/mnt/b"),
            "STORAGE MOVE TABLE \"orders\" TO '/mnt/b'"
        );
    }

    #[test]
    fn a_location_form_refuses_what_the_server_would_refuse() {
        assert!(storage_add_location_sql("", "primary", "1").is_err());
        assert!(storage_add_location_sql("   ", "primary", "1").is_err());
        // Relative: the server's working directory is not part of its
        // configuration, and the console says so before the round trip.
        assert!(storage_add_location_sql("data/second", "primary", "1").is_err());
        assert!(storage_add_location_sql("/mnt/b", "primary", "0").is_err());
        assert!(storage_add_location_sql("/mnt/b", "primary", "plenty").is_err());
        // A blank weight is the field's default rather than an error.
        assert!(storage_add_location_sql("/mnt/b", "backup", "").unwrap().contains("WEIGHT = 1"));
        // Windows paths are absolute too.
        assert!(storage_add_location_sql("D:\\data", "primary", "1").is_ok());
    }

    #[test]
    fn a_floor_needs_a_size() {
        assert!(storage_set_min_free_sql("").is_err());
        assert_eq!(storage_set_min_free_sql(" 2GiB ").unwrap(), "STORAGE SET MIN_FREE = '2GiB'");
    }

    #[test]
    fn the_locations_grid_is_read_the_same_from_either_server() {
        let grid = Grid {
            columns: vec![
                "location".into(),
                "role".into(),
                "weight".into(),
                "enabled".into(),
                "is_database_directory".into(),
                "tables".into(),
                "available_bytes".into(),
                "total_bytes".into(),
            ],
            rows: vec![
                vec![
                    Some("/var/lib/rdb".into()),
                    Some("primary".into()),
                    Some("1".into()),
                    Some("t".into()),
                    Some("t".into()),
                    Some("4".into()),
                    Some("1024".into()),
                    Some("4096".into()),
                ],
                vec![
                    Some("/mnt/backup".into()),
                    Some("backup".into()),
                    Some("1".into()),
                    Some("f".into()),
                    Some("f".into()),
                    Some("0".into()),
                    Some("0".into()),
                    Some("0".into()),
                ],
            ],
            ..Grid::default()
        };

        let parsed = parse_storage_locations(&grid);
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].is_root);
        assert!(parsed[0].enabled);
        assert_eq!(parsed[0].tables, 4);
        assert!(parsed[0].space_known());
        assert!(parsed[1].is_backup());
        assert!(!parsed[1].enabled);
        // Zero total is the server saying it does not know, not a full disk.
        assert!(!parsed[1].space_known());
        assert_eq!(parsed[1].used_fraction(), None);
    }

    /// A report is prose in whatever column the surface named after itself, so
    /// it is read positionally. Reading it by name would need a list of every
    /// statement that answers this way.
    #[test]
    fn a_report_is_read_from_the_first_column_whatever_it_is_called() {
        let grid = Grid {
            columns: vec!["storage".into()],
            rows: vec![
                vec![Some("2 table(s), 4.4 KiB would move:".into())],
                vec![Some("  ".into())],
                vec![None],
                vec![Some("orders (3.5 KiB) /a -> /b: its location no longer takes tables".into())],
            ],
            ..Grid::default()
        };
        let lines = parse_report_lines(&grid);
        assert_eq!(lines.len(), 2, "blank and null rows are not plan lines: {lines:?}");
        assert!(lines[1].starts_with("orders"));
    }

    /// An identifier from the catalogue is still concatenated into SQL, and a
    /// legal PostgreSQL name can contain a quote.
    #[test]
    fn a_quote_in_an_identifier_cannot_end_the_identifier() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(
            quote_ident("orders\"; DROP TABLE users; --"),
            "\"orders\"\"; DROP TABLE users; --\""
        );
    }

    /// Backslashes are not escapes under `standard_conforming_strings`, so a
    /// value containing one must come through unchanged while the quote is
    /// doubled.
    #[test]
    fn a_literal_doubles_quotes_and_leaves_backslashes_alone() {
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_literal("C:\\data"), "'C:\\data'");
        assert_eq!(quote_literal("'; DROP TABLE users; --"), "'''; DROP TABLE users; --'");
    }

    /// A `NULL` written as `'NULL'` is the four-character string, which is a
    /// silently wrong write rather than an error.
    #[test]
    fn a_null_is_written_unquoted() {
        assert_eq!(sql_value(None), "NULL");
        assert_eq!(sql_value(Some("NULL")), "'NULL'");
        assert_eq!(sql_value(Some("")), "''");
    }

    /// Both dialects have to name their result columns identically, or the
    /// shared parsers read `None` out of every row.
    #[test]
    fn both_dialects_produce_the_same_column_names() {
        for column in ["table_name", "row_slots", "columns", "data_bytes", "total_bytes",
                       "reclaimable_bytes", "filters_ready", "immutable", "primary_key"] {
            assert!(PG_TABLES.contains(&format!("AS {}", column))
                        || PG_TABLES.contains(&format!("AS {} ", column)),
                    "PG_TABLES does not produce `{}`", column);
        }
        for column in ["position", "column_name", "data_type", "nullable", "is_unique",
                       "is_primary_key", "default_expression", "is_indexed", "json_table",
                       "is_dropped"] {
            assert!(PG_COLUMNS.contains(&format!("AS {}", column)),
                    "PG_COLUMNS does not produce `{}`", column);
        }
        for column in ["kind", "from_table", "from_column", "to_table", "to_column",
                       "constraint_name", "on_delete", "on_update"] {
            assert!(PG_SCHEMA_MAP.contains(&format!("AS {}", column)),
                    "PG_SCHEMA_MAP does not produce `{}`", column);
        }
        for column in ["section", "key", "value", "hot_reload", "set_with", "help"] {
            assert!(PG_CONFIG.contains(&format!("AS {}", column)),
                    "PG_CONFIG does not produce `{}`", column);
        }
    }

    /// The table name reaches PostgreSQL as a literal cast to `regclass`, not
    /// as an identifier, so it has to be quoted as one.
    #[test]
    fn the_postgres_table_statements_bind_the_name_as_a_literal() {
        let sql = table_stats_sql(Flavor::Postgres, "users");
        assert!(sql.contains("'users'::regclass"));
        assert!(!sql.contains("$TABLE$"), "a placeholder was left unsubstituted");

        let sql = columns_sql(Flavor::Postgres, "o'brien");
        assert!(sql.contains("'o''brien'::regclass"));
    }

    #[test]
    fn a_cold_setting_is_refused_rather_than_sent() {
        let entry = ConfigEntry {
            section: "server".into(),
            key: "batch_size".into(),
            value: "16000".into(),
            hot: false,
            set_with: "--batch-size N (restart)".into(),
            help: String::new(),
        };
        let error = set_config_sql(Flavor::Rasterized, &entry, "32000").unwrap_err();
        assert!(error.contains("--batch-size"), "the error must say what to do instead");
    }

    #[test]
    fn a_restore_names_its_target_exactly() {
        assert_eq!(
            restore_sql("/tmp/copy", &RestoreTarget::Latest).unwrap(),
            "RESTORE DATABASE FROM S3 TO '/tmp/copy'"
        );
        assert_eq!(
            restore_sql("/tmp/copy", &RestoreTarget::Lsn(918273)).unwrap(),
            "RESTORE DATABASE FROM S3 TO '/tmp/copy' AT LSN 918273"
        );
        assert_eq!(
            restore_sql("/tmp/copy", &RestoreTarget::Time("2026-08-20T10:00:00Z".into())).unwrap(),
            "RESTORE DATABASE FROM S3 TO '/tmp/copy' AT TIME '2026-08-20T10:00:00Z'"
        );
        assert_eq!(
            restore_sql("/tmp/copy", &RestoreTarget::Snapshot("snap-1".into())).unwrap(),
            "RESTORE DATABASE FROM SNAPSHOT 'snap-1' TO '/tmp/copy'"
        );
    }

    /// A path is a literal, not an identifier. Quoting it the other way would
    /// make `/var/lib/x` a relation name, and a path containing a quote would
    /// end the statement early.
    #[test]
    fn a_restore_path_is_quoted_as_a_literal() {
        let sql = restore_sql("/tmp/o'brien", &RestoreTarget::Latest).unwrap();
        assert!(sql.contains("TO '/tmp/o''brien'"), "{sql}");
    }

    /// A half-filled form must not produce a statement that restores something
    /// other than what the dialog described.
    #[test]
    fn an_incomplete_restore_is_refused_before_it_is_sent() {
        assert!(restore_sql("", &RestoreTarget::Latest).is_err());
        assert!(restore_sql("   ", &RestoreTarget::Latest).is_err());
        assert!(restore_sql("/tmp/x", &RestoreTarget::Snapshot(String::new())).is_err());
        assert!(restore_sql("/tmp/x", &RestoreTarget::Time("  ".into())).is_err());
    }

    /// Only `Latest` replays everything; the warning hangs off this.
    #[test]
    fn every_target_but_latest_discards_commits() {
        assert!(!RestoreTarget::Latest.discards_later_commits());
        assert!(RestoreTarget::Lsn(1).discards_later_commits());
        assert!(RestoreTarget::Time("x".into()).discards_later_commits());
        assert!(RestoreTarget::Snapshot("x".into()).discards_later_commits());
    }

    /// The in-place command is copied into a shell during an incident, so it
    /// has to be correct and it has to quote what a shell would otherwise eat.
    #[test]
    fn the_in_place_command_is_built_from_the_destination() {
        let status = vec![
            Stat { metric: "source_directory".into(), value: "/var/lib/rdb".into(), detail: String::new() },
            Stat { metric: "bucket".into(), value: "my-backups".into(), detail: String::new() },
            Stat { metric: "prefix".into(), value: "prod/orders/".into(), detail: String::new() },
            Stat { metric: "region".into(), value: "eu-central-1".into(), detail: String::new() },
        ];

        let command = in_place_restore_command(&status, &RestoreTarget::Latest);
        assert!(command.contains("--location /var/lib/rdb"), "{command}");
        assert!(command.contains("--s3-bucket my-backups"), "{command}");
        // The trailing slash the status reports is not part of the flag value.
        assert!(command.contains("--s3-prefix prod/orders"), "{command}");
        assert!(!command.contains("prod/orders/ "), "{command}");
        assert!(command.contains("--restore-from-s3 --restore-replace"), "{command}");

        let at_time = in_place_restore_command(
            &status,
            &RestoreTarget::Time("2026-08-20 09:55:00".into()),
        );
        assert!(
            at_time.contains("--restore-at-time '2026-08-20 09:55:00'"),
            "a value with a space has to survive the shell: {at_time}"
        );
    }

    /// PostgreSQL has no SQL-visible backup catalogue, and inventing one would
    /// mean showing an empty list where the honest answer is "not here".
    #[test]
    fn only_rasterizeddb_has_a_catalogue_to_list() {
        assert!(backups_sql(Flavor::Rasterized).is_some());
        assert!(backups_sql(Flavor::Postgres).is_none());
        assert!(backups_are_manageable(Flavor::Rasterized));
        assert!(!backups_are_manageable(Flavor::Postgres));
    }

    /// Both dialects have to answer the health questions the page branches on,
    /// or PostgreSQL renders as "not configured" whatever its archiver says.
    #[test]
    fn both_dialects_answer_the_backup_health_questions() {
        for metric in ["feature_compiled", "configured", "archiving_suspended", "source_directory"] {
            assert!(
                PG_BACKUP_STATUS.contains(&format!("'{}'", metric)),
                "PG_BACKUP_STATUS does not report `{}`",
                metric
            );
        }
    }

    #[test]
    fn an_import_statement_quotes_every_value() {
        let request = ImportRequest {
            host: "db.internal".into(),
            port: "5432".into(),
            database: "shop".into(),
            user: "migrator".into(),
            password: "it's secret".into(),
            sslmode: SslMode::VerifyFull,
            schema: "public".into(),
            exclude: " audit_log , sessions ".into(),
            target: "shop".into(),
            dry_run: true,
            skip_constraints: false,
        };
        let sql = import_sql(&request).unwrap();

        assert!(sql.contains("HOST 'db.internal' PORT 5432"), "{sql}");
        assert!(sql.contains("PASSWORD 'it''s secret'"), "a quote must not end it: {sql}");
        assert!(sql.contains("SSLMODE 'verify-full'"), "{sql}");
        assert!(sql.contains("EXCLUDE TABLES (audit_log, sessions)"), "{sql}");
        assert!(sql.ends_with("DRY RUN INTO shop"), "{sql}");
    }

    /// The options are clauses, and each has to appear only when asked for.
    #[test]
    fn the_optional_clauses_are_optional() {
        let request = ImportRequest {
            database: "shop".into(),
            user: "u".into(),
            target: "shop".into(),
            schema: String::new(),
            dry_run: false,
            ..ImportRequest::default()
        };
        let sql = import_sql(&request).unwrap();
        assert!(!sql.contains("DRY RUN"), "{sql}");
        assert!(!sql.contains("SCHEMA"), "{sql}");
        assert!(!sql.contains("EXCLUDE"), "{sql}");
        assert!(!sql.contains("WITHOUT CONSTRAINTS"), "{sql}");
        assert!(sql.ends_with("INTO shop"), "{sql}");
    }

    /// Finding out what an import would do, before it does it, is the default.
    #[test]
    fn an_import_previews_by_default() {
        assert!(ImportRequest::default().dry_run);
    }

    #[test]
    fn an_incomplete_import_is_refused_before_it_is_sent() {
        let good = ImportRequest {
            database: "shop".into(),
            user: "u".into(),
            target: "shop".into(),
            ..ImportRequest::default()
        };
        assert!(import_sql(&good).is_ok());

        for broken in [
            ImportRequest { host: String::new(), ..good.clone() },
            ImportRequest { database: String::new(), ..good.clone() },
            ImportRequest { user: String::new(), ..good.clone() },
            ImportRequest { target: String::new(), ..good.clone() },
            ImportRequest { port: "not-a-port".into(), ..good.clone() },
        ] {
            assert!(import_sql(&broken).is_err());
        }
    }

    /// Paging without an order is paging on nothing: the same row can appear on
    /// two pages and another on none.
    #[test]
    fn a_page_orders_by_the_key_when_there_is_one() {
        assert_eq!(
            page_sql("users", Some("id"), 50, 100),
            "SELECT * FROM \"users\" ORDER BY \"id\" LIMIT 50 OFFSET 100"
        );
        assert_eq!(
            page_sql("users", None, 50, 0),
            "SELECT * FROM \"users\" LIMIT 50 OFFSET 0"
        );
    }
}
