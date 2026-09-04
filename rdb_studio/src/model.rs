//! What the console holds about a server, independent of which server it is.
//!
//! Every screen renders one of these. The dialect layer's whole job is to fill
//! them in from either `SHOW …` (RasterizedDB) or `pg_catalog` (PostgreSQL), so
//! nothing below this line has to know which it is talking to.

use std::collections::{BTreeMap, VecDeque};

/// Which SQL a connection is served.
///
/// Not a guess: both speak the PostgreSQL wire protocol, so the transport
/// cannot tell them apart, and a RasterizedDB server answers a `pg_catalog`
/// query with "relation does not exist" rather than with something wrong. The
/// user names it when they save the connection, and [`Flavor::detect`] offers a
/// default by asking the server one question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Flavor {
    Rasterized,
    Postgres,
}

impl Flavor {
    pub const ALL: [Flavor; 2] = [Flavor::Rasterized, Flavor::Postgres];

    pub fn label(&self) -> &'static str {
        match self {
            Flavor::Rasterized => "RasterizedDB",
            Flavor::Postgres => "PostgreSQL",
        }
    }
}

impl std::fmt::Display for Flavor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// How to reach a server, as saved on disk.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Connection {
    pub name: String,
    pub flavor: Flavor,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    /// Kept in the profile only when the user asks for it. A console that
    /// silently persists passwords is a credential store nobody audited.
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub save_password: bool,
    pub tls: TlsMode,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            name: "local".to_string(),
            flavor: Flavor::Rasterized,
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            user: "rasterized".to_string(),
            password: String::new(),
            save_password: false,
            tls: TlsMode::Prefer,
        }
    }
}

/// What to do about transport encryption.
///
/// `Prefer` is the default rather than `Require` because RasterizedDB generates
/// a self-signed certificate when none is configured, and `Require` with
/// verification would refuse the server the user just started. See
/// [`crate::conn`] for what verification actually happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TlsMode {
    Disable,
    Prefer,
    Require,
}

impl TlsMode {
    pub const ALL: [TlsMode; 3] = [TlsMode::Disable, TlsMode::Prefer, TlsMode::Require];

    pub fn label(&self) -> &'static str {
        match self {
            TlsMode::Disable => "disable",
            TlsMode::Prefer => "prefer",
            TlsMode::Require => "require",
        }
    }
}

impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// A result set, exactly as the wire delivered it.
///
/// Values are `Option<String>` because every column arrives as text over the
/// simple query protocol and `None` is a genuine SQL `NULL` — which a console
/// must not render as an empty string, since a `TEXT` column can hold one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Grid {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    /// What the server tagged the statement with, e.g. `SELECT 12`.
    pub tag: String,
}

impl Grid {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.eq_ignore_ascii_case(name))
    }

    pub fn value(&self, row: usize, column: &str) -> Option<&str> {
        let index = self.column_index(column)?;
        self.rows.get(row)?.get(index)?.as_deref()
    }

    pub fn number(&self, row: usize, column: &str) -> Option<u64> {
        self.value(row, column)?.trim().parse().ok()
    }

    /// A column of milliseconds, or `None` when the server left it blank.
    ///
    /// Separate from [`Grid::number`] rather than folded into it: a duration
    /// that read as an integer would round every sub-millisecond statement to
    /// zero, and most statements are sub-millisecond.
    pub fn real(&self, row: usize, column: &str) -> Option<f64> {
        self.value(row, column)?.trim().parse().ok()
    }

    pub fn flag(&self, row: usize, column: &str) -> Option<bool> {
        Some(matches!(
            self.value(row, column)?.trim(),
            "t" | "true" | "TRUE" | "on" | "1" | "yes"
        ))
    }
}

/// One `(metric, value, detail)` line of a stats result.
#[derive(Debug, Clone, PartialEq)]
pub struct Stat {
    pub metric: String,
    pub value: String,
    pub detail: String,
}

/// A table, as the sidebar and the table list show it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    /// What is holding the table, in the server's own vocabulary — `file`,
    /// `memory`, `s3`, `mirrored`, `kv-directory`, `kv-memory` from
    /// RasterizedDB, and `file` / `file-unlogged` / `temporary` from
    /// PostgreSQL. Empty from a server too old to say.
    pub storage: String,
    pub row_slots: u64,
    pub columns: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub total_bytes: u64,
    pub reclaimable_bytes: u64,
    pub primary_key: String,
    pub immutable: bool,
    /// Whether the engine's own index has caught up. Always true against
    /// PostgreSQL, which has no equivalent state to be behind in.
    pub indexed: bool,
}

impl TableInfo {
    pub fn durable(&self) -> bool {
        storage_is_durable(&self.storage)
    }
}

/// Whether a table's contents outlive the process serving them.
///
/// Strict on purpose. PostgreSQL's `UNLOGGED` and temporary tables are files on
/// a disk, and both are *emptied* after a crash — so for the question this
/// answers, "would this table still be here", they belong with the in-memory
/// backends and not with the durable ones.
///
/// Anything unrecognised counts as durable, including the empty string a server
/// too old to have the column returns. The alternative would mark every table
/// on such a server as ephemeral, which is a warning about nothing — and this
/// is used to colour a badge, where a false alarm costs more than a missing one.
pub fn storage_is_durable(kind: &str) -> bool {
    !matches!(kind, "memory" | "kv-memory" | "temporary" | "file-unlogged")
}

/// A column of a table.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub position: u64,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub default: String,
    pub indexed: bool,
    /// The shadow table a JSON column's documents live in. Empty for everything
    /// else, and only ever set by RasterizedDB.
    pub json_table: String,
}

impl ColumnInfo {
    /// Whether values of this column are right-aligned and monospaced in the
    /// grid, which is what makes a column of figures comparable down the page.
    pub fn is_numeric(&self) -> bool {
        let t = self.data_type.to_ascii_uppercase();
        [
            "INT", "SERIAL", "NUMERIC", "DECIMAL", "REAL", "DOUBLE", "FLOAT", "MONEY",
        ]
        .iter()
        .any(|k| t.contains(k))
    }
}

/// The column the data browser orders and addresses rows by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowKey {
    pub column: String,
    pub source: KeySource,
}

/// Why a column was chosen as the row key.
///
/// The distinction is not pedantry. A declared primary key is the table saying
/// "this identifies a row". A `UNIQUE NOT NULL` column *also* identifies a row —
/// the constraint is enforced, so an `UPDATE` matching on it touches at most one
/// — but the table never said it was the key, so the console says which it
/// found rather than claiming the table has a primary key it does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    PrimaryKey,
    /// A `UNIQUE NOT NULL` column. Addresses exactly one row, which is all the
    /// browser needs — and it is what makes tables created before the engine
    /// recorded primary keys editable at all.
    UniqueColumn,
}

impl RowKey {
    /// Picks the key for a table, from what the server said about it.
    ///
    /// Three sources in order of authority: the table's declared primary key,
    /// a column flagged as the primary key, then any `UNIQUE NOT NULL` column.
    /// A composite key is deliberately not used — the browser addresses rows
    /// with one predicate, and half of a two-column key identifies a set.
    pub fn choose(declared: &str, columns: &[ColumnInfo]) -> Option<RowKey> {
        let declared = declared.trim();
        if !declared.is_empty() && !declared.contains(',') {
            return Some(RowKey {
                column: declared.to_string(),
                source: KeySource::PrimaryKey,
            });
        }

        if let Some(column) = columns.iter().find(|c| c.primary_key) {
            return Some(RowKey {
                column: column.name.clone(),
                source: KeySource::PrimaryKey,
            });
        }

        // A nullable unique column is not a key: SQL lets it hold any number of
        // NULLs, so `WHERE c IS NULL` can match many rows.
        columns
            .iter()
            .find(|c| c.unique && !c.nullable)
            .map(|column| RowKey {
                column: column.name.clone(),
                source: KeySource::UniqueColumn,
            })
    }

    /// How the browser describes what it is addressing rows by.
    pub fn describe(&self) -> String {
        match self.source {
            KeySource::PrimaryKey => format!("ordered and addressed by {} (primary key)", self.column),
            KeySource::UniqueColumn => format!(
                "ordered and addressed by {} — a UNIQUE NOT NULL column, since this table \
                 declares no primary key",
                self.column
            ),
        }
    }
}

/// One edge of the schema map.
#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    /// `fk` for a declared foreign key, `json` for a column mapped onto a
    /// shadow table.
    pub kind: String,
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
    pub constraint: String,
    pub on_delete: String,
}

/// The schema, as a graph to draw.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaGraph {
    pub tables: Vec<TableNode>,
    pub edges: Vec<Relationship>,
}

/// A table on the schema canvas, with the position the layout gave it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableNode {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub rows: u64,
    /// Canvas coordinates. Mutable: the user drags these.
    pub x: f32,
    pub y: f32,
}

impl TableNode {
    /// Height the node draws at, which the layout needs before anything is
    /// drawn so that two nodes do not overlap.
    pub fn height(&self) -> f32 {
        Self::HEADER + self.visible_columns() as f32 * Self::ROW + 6.0
    }

    pub const WIDTH: f32 = 208.0;
    pub const HEADER: f32 = 26.0;
    pub const ROW: f32 = 16.0;
    /// A wide table would draw a column list taller than the canvas, so it is
    /// truncated with a count of what was left out.
    pub const MAX_COLUMNS: usize = 14;

    pub fn visible_columns(&self) -> usize {
        self.columns.len().min(Self::MAX_COLUMNS) + usize::from(self.columns.len() > Self::MAX_COLUMNS)
    }
}

/// One runtime knob, as the configuration screen shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigEntry {
    pub section: String,
    pub key: String,
    pub value: String,
    /// Whether a running server can change it. The screen greys out the rest
    /// and says what flag to restart with instead of offering an edit that
    /// would fail.
    pub hot: bool,
    pub set_with: String,
    pub help: String,
}

impl ConfigEntry {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.section, self.key)
    }
}

/// A database on the server.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseInfo {
    pub name: String,
    pub location: String,
    pub tables: u64,
    pub sessions: u64,
    pub is_current: bool,
}

/// Everything one statement produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Rows(Grid),
    /// A statement that reported a count rather than rows.
    Affected { tag: String, rows: u64 },
}

/// Groups stats by their prefix, so the overview can lay them out in sections
/// rather than as one long list.
///
/// The prefix is everything before the first `_`, which is how the metric names
/// on both sides are already organised — `filter_enabled`, `vacuum_runs`,
/// `wal_checkpoint_bytes`. A metric with no `_` goes under `general`.
pub fn group_stats(stats: &[Stat]) -> BTreeMap<String, Vec<Stat>> {
    let mut out: BTreeMap<String, Vec<Stat>> = BTreeMap::new();
    for stat in stats {
        let section = match stat.metric.split_once('_') {
            Some((head, _)) if KNOWN_SECTIONS.contains(&head) => head.to_string(),
            _ => "general".to_string(),
        };
        out.entry(section).or_default().push(stat.clone());
    }
    out
}

/// Prefixes that name a section rather than being the first word of a metric.
/// Without this list `data_bytes` and `total_bytes` would each invent a section
/// of one.
const KNOWN_SECTIONS: &[&str] = &[
    "filter",
    "query",
    "replication",
    "transaction",
    "vacuum",
    "wal",
    "cache",
    "autovacuum",
    "checkpoint",
    "connection",
    "index",
    "server",
];

/// A byte count, in the largest unit that keeps it under four digits.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", n)
    } else if value < 10.0 {
        format!("{:.2} {}", value, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// A count, with thin separators every three digits.
pub fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push('\u{202F}');
        }
        out.push(ch);
    }
    out
}

/// Whether a metric's value is a byte count, decided from its name.
///
/// The alternative is a units column on the wire, which would have to be filled
/// in by both dialects for every metric; the suffix convention is already
/// there and is what both sides already follow.
///
/// `_size` is deliberately **not** a match. It looks like the same convention
/// and is not: `batch_size` is a number of rows, and matching it rendered
/// `10000` as `9.77 KiB` — a plausible figure in the wrong unit, which is worse
/// than an unformatted one because nothing about it looks wrong.
pub fn is_byte_metric(metric: &str) -> bool {
    metric.ends_with("_bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(path: &str, role: &str, enabled: bool, free: u64, total: u64) -> StorageLocation {
        StorageLocation {
            path: path.into(),
            role: role.into(),
            weight: 1,
            enabled,
            is_root: path == "/root",
            tables: 0,
            available_bytes: free,
            total_bytes: total,
        }
    }

    fn storage_snapshot(locations: Vec<StorageLocation>, floor: u64) -> StorageSnapshot {
        StorageSnapshot {
            stats: vec![Stat {
                metric: "min_free_bytes".into(),
                value: floor.to_string(),
                detail: String::new(),
            }],
            supported: true,
            locations,
            placements: Vec::new(),
        }
    }

    /// A draining location and a backup are both doing what they were told.
    /// Colouring either as a fault is how people learn to ignore the colour
    /// that means something.
    #[test]
    fn only_a_location_out_of_room_reads_as_a_fault() {
        let floor = 1000;
        assert_eq!(
            location("/a", "primary", true, 5000, 10_000).health(floor),
            LocationHealth::Healthy
        );
        assert_eq!(
            location("/a", "primary", true, 10, 10_000).health(floor),
            LocationHealth::Full
        );
        assert_eq!(
            location("/a", "primary", false, 5000, 10_000).health(floor),
            LocationHealth::Draining
        );
        assert_eq!(
            location("/b", "backup", true, 5000, 10_000).health(floor),
            LocationHealth::Backup
        );
        // A disabled location is draining whatever else is true of it: that is
        // the thing an operator is doing to it.
        assert_eq!(
            location("/b", "backup", false, 5000, 10_000).health(floor),
            LocationHealth::Draining
        );
    }

    /// A server that cannot report free space sends zero, and zero free out of
    /// zero total is not a full disk — it is no answer. Reading it as "full"
    /// would put a warning banner on every PostgreSQL connection.
    #[test]
    fn unknown_free_space_is_not_a_full_disk() {
        let unknown = location("/a", "primary", true, 0, 0);
        assert!(!unknown.space_known());
        assert_eq!(unknown.health(1000), LocationHealth::Healthy);
        assert_eq!(unknown.used_fraction(), None);
        assert!(storage_snapshot(vec![unknown], 1000).tight().is_empty());

        let real = location("/a", "primary", true, 2500, 10_000);
        assert!(real.space_known());
        assert_eq!(real.used_fraction(), Some(0.75));
    }

    #[test]
    fn only_enabled_primaries_count_as_short_of_room() {
        let snapshot = storage_snapshot(
            vec![
                location("/root", "primary", true, 10, 10_000),
                location("/drained", "primary", false, 10, 10_000),
                location("/backup", "backup", true, 10, 10_000),
                location("/fine", "primary", true, 9_000, 10_000),
            ],
            1000,
        );
        let tight: Vec<&str> = snapshot.tight().iter().map(|l| l.path.as_str()).collect();
        assert_eq!(
            tight,
            vec!["/root"],
            "a drained location takes no new tables and a backup holds no table's only copy"
        );
        assert!(snapshot.is_pooled());
    }

    /// A database with no backup location is not a database whose tables are
    /// unprotected — it is one that was never asked for a backup. Reporting
    /// every table as "not mirrored" there would be a permanent false alarm.
    #[test]
    fn tables_are_only_unmirrored_when_there_is_a_backup_to_miss() {
        let placements = vec![
            TablePlacement { table: "a".into(), location: "/root".into(), mirrored: false },
            TablePlacement { table: "b".into(), location: "/root".into(), mirrored: true },
        ];

        let mut without = storage_snapshot(vec![location("/root", "primary", true, 1, 2)], 0);
        without.placements = placements.clone();
        assert!(without.unmirrored().is_empty());

        let mut with = storage_snapshot(
            vec![
                location("/root", "primary", true, 1, 2),
                location("/backup", "backup", true, 1, 2),
            ],
            0,
        );
        with.placements = placements;
        let missing: Vec<&str> = with.unmirrored().iter().map(|p| p.table.as_str()).collect();
        assert_eq!(missing, vec!["a"]);
    }

    fn column(name: &str, primary: bool, unique: bool, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            position: 1,
            name: name.into(),
            data_type: "TEXT".into(),
            nullable,
            unique,
            primary_key: primary,
            default: String::new(),
            indexed: false,
            json_table: String::new(),
        }
    }

    /// A declared primary key wins, and says so.
    #[test]
    fn the_declared_key_is_preferred() {
        let key = RowKey::choose("id", &[column("id", true, true, false)]).unwrap();
        assert_eq!(key.column, "id");
        assert_eq!(key.source, KeySource::PrimaryKey);
    }

    /// The case that made a whole database read-only: a table created before
    /// the engine recorded primary keys reports none, but its `UNIQUE NOT NULL`
    /// column still addresses exactly one row.
    #[test]
    fn a_unique_not_null_column_is_a_usable_key() {
        let columns = [
            column("name", false, false, true),
            column("email", false, true, false),
        ];
        let key = RowKey::choose("", &columns).unwrap();
        assert_eq!(key.column, "email");
        assert_eq!(key.source, KeySource::UniqueColumn);
        assert!(
            key.describe().contains("declares no primary key"),
            "the browser must not claim this is the primary key"
        );
    }

    /// A nullable unique column is not a key: SQL lets it hold any number of
    /// NULLs, so matching on one can hit many rows.
    #[test]
    fn a_nullable_unique_column_is_not_a_key() {
        assert!(RowKey::choose("", &[column("email", false, true, true)]).is_none());
    }

    /// Half of a two-column key identifies a set, not a row.
    #[test]
    fn a_composite_key_is_not_used() {
        assert!(RowKey::choose("tenant_id, id", &[]).is_none());
    }

    #[test]
    fn a_table_with_nothing_unique_has_no_key() {
        assert!(RowKey::choose("", &[column("a", false, false, true)]).is_none());
    }

    fn snapshot(status: Vec<(&str, &str)>, supported: bool, catalogue: Result<Vec<BackupEntry>, String>) -> BackupSnapshot {
        BackupSnapshot {
            status: status
                .into_iter()
                .map(|(metric, value)| Stat {
                    metric: metric.into(),
                    value: value.into(),
                    detail: String::new(),
                })
                .collect(),
            status_supported: supported,
            catalogue,
        }
    }

    fn entry() -> BackupEntry {
        BackupEntry {
            kind: "snapshot".into(),
            id: "snap-1".into(),
            created_at: "2026-08-21T00:00:00Z".into(),
            start_lsn: 1,
            end_lsn: 2,
            uncompressed_bytes: 100,
            compressed_bytes: 10,
            detail: String::new(),
        }
    }

    /// The bug this state machine exists for. A server built before
    /// `SHOW BACKUP STATUS` answers it as a runtime parameter — one empty
    /// column — which parses to a `Stat` with no metric. Reading `configured`
    /// out of that gave "false", and the page told an operator with a working
    /// destination and a full catalogue that they had none.
    #[test]
    fn a_server_that_cannot_answer_is_not_a_server_without_backups() {
        let state = snapshot(vec![], false, Ok(vec![entry()])).state();
        assert_eq!(state, BackupState::ConfiguredWithoutStatus);

        // Even with nothing stored yet: the catalogue answered, so a
        // destination exists.
        let state = snapshot(vec![], false, Ok(vec![])).state();
        assert_eq!(state, BackupState::ConfiguredWithoutStatus);
    }

    /// The engine's refusal names itself, and that is what distinguishes an
    /// unconfigured server from one that merely could not be read.
    #[test]
    fn the_refusal_is_what_says_nothing_is_configured() {
        let state = snapshot(
            vec![],
            false,
            Err("no backup destination is configured. Start the server with --s3-bucket".into()),
        )
        .state();
        assert_eq!(state, BackupState::NotConfigured);
    }

    /// A permission or connection failure is not a statement about backups.
    #[test]
    fn an_unrelated_failure_is_reported_as_itself() {
        let state = snapshot(vec![], false, Err("permission denied for pg_stat_archiver".into()))
            .state();
        assert_eq!(
            state,
            BackupState::Unreadable("permission denied for pg_stat_archiver".into())
        );
    }

    #[test]
    fn a_reporting_server_is_taken_at_its_word() {
        assert_eq!(
            snapshot(vec![("feature_compiled", "true"), ("configured", "true")], true, Ok(vec![]))
                .state(),
            BackupState::Configured
        );
        assert_eq!(
            snapshot(vec![("feature_compiled", "true"), ("configured", "false")], true, Ok(vec![]))
                .state(),
            BackupState::NotConfigured
        );
        assert_eq!(
            snapshot(vec![("feature_compiled", "false"), ("configured", "false")], true, Ok(vec![]))
                .state(),
            BackupState::NotCompiled
        );
    }

    /// A "supported" result that names no `configured` is not a status result,
    /// and must fall back rather than read as a no.
    #[test]
    fn a_status_without_the_answer_falls_back_to_the_catalogue() {
        let state = snapshot(vec![("something_else", "1")], true, Ok(vec![entry()])).state();
        assert_eq!(state, BackupState::ConfiguredWithoutStatus);
    }

    /// Sub-millisecond work is real on a local database; rounding it away
    /// throws out the only interesting thing about it.
    #[test]
    fn a_duration_keeps_its_precision() {
        use std::time::Duration;
        assert_eq!(duration(Duration::from_micros(430)), "430 µs");
        assert_eq!(duration(Duration::from_micros(4300)), "4.30 ms");
        assert_eq!(duration(Duration::from_millis(43)), "43.0 ms");
        assert_eq!(duration(Duration::from_millis(4300)), "4.30 s");
        assert_eq!(duration(Duration::from_secs(75)), "1m 15s");
    }

    #[test]
    fn an_outcome_says_what_happened() {
        assert_eq!(
            RunOutcome::Ok { statements: 1, rows: 12, affected: 0 }.summary(),
            "12 rows"
        );
        assert_eq!(
            RunOutcome::Ok { statements: 1, rows: 1, affected: 0 }.summary(),
            "1 row"
        );
        assert_eq!(
            RunOutcome::Ok { statements: 1, rows: 0, affected: 3 }.summary(),
            "3 affected"
        );
        assert_eq!(RunOutcome::Ok { statements: 1, rows: 0, affected: 0 }.summary(), "ok");
        assert_eq!(
            RunOutcome::Ok { statements: 3, rows: 4, affected: 0 }.summary(),
            "4 rows · 3 statements"
        );
        assert!(RunOutcome::Failed("boom".into()).failed());
    }

    /// The badge answers "would this table still be here", so anything that a
    /// crash empties belongs with the in-memory backends however it is stored.
    #[test]
    fn durability_is_about_surviving_a_crash_not_about_being_a_file() {
        for durable in ["file", "mirrored", "s3", "kv-directory"] {
            assert!(storage_is_durable(durable), "{durable} should read as durable");
        }
        for ephemeral in ["memory", "kv-memory", "temporary", "file-unlogged"] {
            assert!(!storage_is_durable(ephemeral), "{ephemeral} outlives nothing");
        }
        // A server that does not report the column, and one that reports
        // something this console has never heard of, both get the benefit of
        // the doubt: the badge is a warning, and a false one costs more than a
        // missing one.
        assert!(storage_is_durable(""));
        assert!(storage_is_durable("something-new"));
    }

    #[test]
    fn bytes_scale_to_a_readable_unit() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(2048), "2.00 KiB");
        assert_eq!(bytes(1024 * 1024 * 15), "15.0 MiB");
    }

    /// `batch_size` is a row count. Formatting it as bytes produced a
    /// believable figure in the wrong unit.
    #[test]
    fn only_a_bytes_suffix_means_bytes() {
        assert!(is_byte_metric("total_bytes"));
        assert!(is_byte_metric("wal_checkpoint_bytes"));
        assert!(!is_byte_metric("batch_size"));
        assert!(!is_byte_metric("page_size"));
    }

    #[test]
    fn counts_are_grouped_in_threes() {
        assert_eq!(count(7), "7");
        assert_eq!(count(1234), "1\u{202F}234");
        assert_eq!(count(1234567), "1\u{202F}234\u{202F}567");
    }

    /// A `NULL` and an empty string are different values, and a console that
    /// renders them the same makes a column of one look like a column of the
    /// other.
    #[test]
    fn a_null_is_not_an_empty_string() {
        let grid = Grid {
            columns: vec!["a".into()],
            rows: vec![vec![None], vec![Some(String::new())]],
            tag: String::new(),
        };
        assert_eq!(grid.value(0, "a"), None);
        assert_eq!(grid.value(1, "a"), Some(""));
    }

    #[test]
    fn stats_group_by_their_prefix() {
        let stats = vec![
            Stat { metric: "name".into(), value: "db".into(), detail: String::new() },
            Stat { metric: "filter_enabled".into(), value: "true".into(), detail: String::new() },
            Stat { metric: "total_bytes".into(), value: "10".into(), detail: String::new() },
        ];
        let grouped = group_stats(&stats);
        assert_eq!(grouped["general"].len(), 2, "total_bytes must not invent a section");
        assert_eq!(grouped["filter"].len(), 1);
    }
}

// ---------------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------------

/// One object in the backup catalogue: a full snapshot, or a bundle of
/// write-ahead log segments.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupEntry {
    /// `snapshot` or `wal`.
    pub kind: String,
    pub id: String,
    pub created_at: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub detail: String,
}

impl BackupEntry {
    pub fn is_snapshot(&self) -> bool {
        self.kind.eq_ignore_ascii_case("snapshot")
    }

    /// What the compression achieved, as a ratio of original to stored.
    pub fn ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            return 0.0;
        }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }
}

// ---------------------------------------------------------------------------
// Statement history
// ---------------------------------------------------------------------------

/// One statement this session sent, and what came back.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEntry {
    pub sql: String,
    /// Round-trip time: sent to answered. Includes the network, which is why
    /// it is never presented as "the server took this long".
    pub elapsed: std::time::Duration,
    /// When it ran, for the relative age in the list. An `Instant` rather than
    /// a wall clock: the list only ever says "4m ago", which needs no timezone
    /// and cannot be wrong because the machine's clock moved.
    pub at: std::time::Instant,
    pub outcome: RunOutcome,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    Ok {
        statements: usize,
        /// Rows returned across every result set.
        rows: usize,
        /// Rows reported affected across every statement that reported a count.
        affected: u64,
    },
    Failed(String),
}

impl RunOutcome {
    pub fn failed(&self) -> bool {
        matches!(self, RunOutcome::Failed(_))
    }

    /// What the history list shows beside the statement.
    pub fn summary(&self) -> String {
        match self {
            RunOutcome::Failed(_) => "failed".to_string(),
            RunOutcome::Ok { rows, affected, statements } => {
                let mut parts = Vec::new();
                if *rows > 0 {
                    parts.push(format!("{} row{}", count(*rows as u64), plural(*rows as u64)));
                }
                if *affected > 0 {
                    parts.push(format!("{} affected", count(*affected)));
                }
                if parts.is_empty() {
                    parts.push("ok".to_string());
                }
                if *statements > 1 {
                    parts.push(format!("{} statements", statements));
                }
                parts.join(" · ")
            }
        }
    }
}

fn plural(n: u64) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// A duration, in whatever unit makes it readable.
///
/// Sub-millisecond work is real on a local database, and rounding it to `0 ms`
/// throws away the only interesting thing about it.
pub fn duration(elapsed: std::time::Duration) -> String {
    let micros = elapsed.as_micros();
    if micros < 1_000 {
        format!("{} µs", micros)
    } else if micros < 10_000 {
        format!("{:.2} ms", micros as f64 / 1000.0)
    } else if micros < 1_000_000 {
        format!("{:.1} ms", micros as f64 / 1000.0)
    } else if elapsed.as_secs() < 60 {
        format!("{:.2} s", elapsed.as_secs_f64())
    } else {
        format!("{}m {:02}s", elapsed.as_secs() / 60, elapsed.as_secs() % 60)
    }
}

/// How long ago something happened, coarsely.
pub fn age(since: std::time::Instant) -> String {
    let seconds = since.elapsed().as_secs();
    if seconds < 5 {
        "just now".to_string()
    } else if seconds < 60 {
        format!("{}s ago", seconds)
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else {
        format!("{}h ago", seconds / 3600)
    }
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

/// One peer, as the replication page shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerStatus {
    pub name: String,
    pub address: String,
    pub remote_node: u64,
    pub connected: bool,
    pub paused: bool,
    pub acked_seq: u64,
    /// Transactions this peer has not confirmed. The number an operator
    /// actually watches.
    pub lag: u64,
    pub queued: u64,
    pub records_sent: u64,
    pub batches_sent: u64,
    /// Records that could not be queued. Not lost — the sender backfills from
    /// the log — but a peer that is dropping is a peer that is behind.
    pub dropped: u64,
    pub codec: String,
    pub last_ratio: String,
    pub last_error: String,
}

impl PeerStatus {
    /// How this peer should read at a glance.
    pub fn health(&self) -> PeerHealth {
        if self.paused {
            PeerHealth::Paused
        } else if !self.connected {
            PeerHealth::Down
        } else if self.lag > 0 || self.dropped > 0 {
            PeerHealth::Behind
        } else {
            PeerHealth::Healthy
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealth {
    Healthy,
    /// Connected, but not caught up.
    Behind,
    /// Deliberately stopped. Not a fault, and must not be coloured as one.
    Paused,
    Down,
}

impl PeerHealth {
    pub fn label(&self) -> &'static str {
        match self {
            PeerHealth::Healthy => "IN SYNC",
            PeerHealth::Behind => "BEHIND",
            PeerHealth::Paused => "PAUSED",
            PeerHealth::Down => "DOWN",
        }
    }
}

/// Everything the replication page reads, in one round trip.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationSnapshot {
    pub stats: Vec<Stat>,
    /// Whether the server understood `SHOW REPLICATION STATS`. Same trap as the
    /// backup page: an older build answers an unknown `SHOW x` as a runtime
    /// parameter rather than refusing it, so silence has to be distinguished
    /// from a "no".
    pub supported: bool,
    pub peers: Vec<PeerStatus>,
}

impl ReplicationSnapshot {
    pub fn metric(&self, name: &str) -> Option<&str> {
        self.stats
            .iter()
            .find(|s| s.metric == name)
            .map(|s| s.value.trim())
    }

    pub fn flag(&self, name: &str) -> Option<bool> {
        Some(matches!(self.metric(name)?, "true" | "t" | "on" | "1" | "yes"))
    }

    pub fn number(&self, name: &str) -> Option<u64> {
        self.metric(name)?.parse().ok()
    }

    /// Whether replication is switched on. `None` when the server would not
    /// say — which is not the same as off.
    pub fn active(&self) -> Option<bool> {
        self.flag("active")
    }
}

// ---------------------------------------------------------------------------
// Storage placement
// ---------------------------------------------------------------------------

/// One directory the database may write into.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageLocation {
    pub path: String,
    /// `primary` (holds tables) or `backup` (receives a mirrored copy).
    pub role: String,
    pub weight: u64,
    pub enabled: bool,
    /// The database's own directory, which cannot be removed from the pool.
    pub is_root: bool,
    pub tables: u64,
    /// Zero when the server could not determine it, which the page has to show
    /// as unknown rather than as "full" — the column is a number and has
    /// nowhere else to put a "don't know".
    pub available_bytes: u64,
    pub total_bytes: u64,
}

impl StorageLocation {
    pub fn is_backup(&self) -> bool {
        self.role.eq_ignore_ascii_case("backup")
    }

    /// Whether free space is actually known. See `available_bytes`.
    pub fn space_known(&self) -> bool {
        self.total_bytes > 0
    }

    pub fn used_fraction(&self) -> Option<f32> {
        if !self.space_known() {
            return None;
        }
        let used = self.total_bytes.saturating_sub(self.available_bytes);
        Some(used as f32 / self.total_bytes as f32)
    }

    /// How this location should read at a glance.
    pub fn health(&self, min_free_bytes: u64) -> LocationHealth {
        if !self.enabled {
            LocationHealth::Draining
        } else if self.space_known() && self.available_bytes < min_free_bytes {
            LocationHealth::Full
        } else if self.is_backup() {
            LocationHealth::Backup
        } else {
            LocationHealth::Healthy
        }
    }
}

/// The four states a location can be in, separated so that only the one which
/// needs attention gets a warning colour.
///
/// A disabled location is being drained on purpose and a backup is doing its
/// job; colouring either of them red is how people learn to ignore red.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationHealth {
    Healthy,
    /// Below the free-space floor: it will not be chosen for new tables, and
    /// the tables already on it have nowhere to grow.
    Full,
    /// Deliberately taking no new tables.
    Draining,
    Backup,
}

impl LocationHealth {
    pub fn label(&self) -> &'static str {
        match self {
            LocationHealth::Healthy => "OK",
            LocationHealth::Full => "LOW SPACE",
            LocationHealth::Draining => "DRAINING",
            LocationHealth::Backup => "BACKUP",
        }
    }
}

/// Which directory one table's files are in.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePlacement {
    pub table: String,
    pub location: String,
    /// Whether this table's writes are actually reaching a backup copy — asked
    /// of the table, not of the pool. A backup location added while the server
    /// was running does not apply to tables already open, and a column that
    /// reported the pool's intent would say a table was protected when it is
    /// not.
    pub mirrored: bool,
}

/// Everything the storage page reads, in one round trip.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageSnapshot {
    pub stats: Vec<Stat>,
    /// Whether the server understood `SHOW STORAGE STATS`.
    ///
    /// The same trap the backup and replication pages have: a server built
    /// without the feature — or from before it existed — does not refuse an
    /// unknown `SHOW x`. The wire layer answers it as a runtime parameter, with
    /// one empty column named after the statement. That parses into a snapshot
    /// full of nothing that looks exactly like a database with no extra
    /// directories, which is a different and much more common thing.
    pub supported: bool,
    pub locations: Vec<StorageLocation>,
    pub placements: Vec<TablePlacement>,
}

impl StorageSnapshot {
    pub fn metric(&self, name: &str) -> Option<&str> {
        self.stats.iter().find(|s| s.metric == name).map(|s| s.value.trim())
    }

    pub fn number(&self, name: &str) -> Option<u64> {
        self.metric(name)?.parse().ok()
    }

    pub fn policy(&self) -> &str {
        self.metric("policy").unwrap_or("primary")
    }

    pub fn min_free_bytes(&self) -> u64 {
        self.number("min_free_bytes").unwrap_or(0)
    }

    /// Whether this database writes anywhere but its own directory. A pool of
    /// one is what every database has always had, and the page says so rather
    /// than rendering an empty-looking control panel.
    pub fn is_pooled(&self) -> bool {
        self.locations.len() > 1
    }

    pub fn primaries(&self) -> impl Iterator<Item = &StorageLocation> {
        self.locations.iter().filter(|l| !l.is_backup())
    }

    pub fn backups(&self) -> impl Iterator<Item = &StorageLocation> {
        self.locations.iter().filter(|l| l.is_backup())
    }

    /// Enabled primaries below the free-space floor. The one figure worth
    /// putting a banner on.
    pub fn tight(&self) -> Vec<&StorageLocation> {
        let floor = self.min_free_bytes();
        self.primaries()
            .filter(|l| l.enabled && l.space_known() && l.available_bytes < floor)
            .collect()
    }

    /// Tables whose writes are not reaching a backup, when the pool has one.
    ///
    /// Empty when there is no backup at all — that is not an anomaly, it is a
    /// database that was never asked for one.
    pub fn unmirrored(&self) -> Vec<&TablePlacement> {
        if self.backups().next().is_none() {
            return Vec::new();
        }
        self.placements.iter().filter(|p| !p.mirrored).collect()
    }

    /// Where a table created now would go, as the server worked it out.
    pub fn next_placement(&self) -> Option<&str> {
        self.metric("next_placement").filter(|value| !value.is_empty())
    }
}

/// One statement the server has answered, as the activity page lists it.
///
/// `rows` and `rows_scanned` are optional because PostgreSQL cannot answer
/// them for a running statement: `pg_stat_activity` says what a backend is
/// executing, not what it has read. A figure nobody can produce renders as a
/// dash rather than as a zero, which would read as "it scanned nothing".
#[derive(Debug, Clone, PartialEq)]
pub struct StatementRecord {
    pub started_at: String,
    pub duration_ms: f64,
    pub rows: Option<u64>,
    pub rows_scanned: Option<u64>,
    /// Relations this statement had to build before it could answer — one per
    /// CTE and per derived table — and the time that took. `None` from
    /// PostgreSQL, which has no equivalent because it does not build them.
    pub materialised: Option<u64>,
    pub materialised_ms: Option<f64>,
    /// `ok` or `error` on RasterizedDB; the backend's state on PostgreSQL.
    pub status: String,
    pub database: String,
    pub username: String,
    pub statement: String,
    pub error: String,
}

impl StatementRecord {
    pub fn failed(&self) -> bool {
        !self.error.is_empty() || self.status.eq_ignore_ascii_case("error")
    }

    pub fn heavy(&self) -> bool {
        scan_is_heavy(self.rows, self.rows_scanned)
    }
}

/// Every execution of one statement shape, folded together.
///
/// `rows_scanned`, `errors` and `last_seen` are `None` against PostgreSQL,
/// whose `pg_stat_statements` has no counterpart for any of them.
#[derive(Debug, Clone, PartialEq)]
pub struct StatementShape {
    pub queryid: String,
    pub calls: u64,
    pub total_ms: f64,
    pub mean_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub stddev_ms: f64,
    pub rows: Option<u64>,
    pub rows_scanned: Option<u64>,
    /// Totals across every execution of this shape. See [`StatementRecord`].
    pub materialised: Option<u64>,
    pub materialised_ms: Option<f64>,
    pub errors: Option<u64>,
    pub last_seen: String,
    pub statement: String,
}

impl StatementShape {
    pub fn heavy(&self) -> bool {
        scan_is_heavy(self.rows, self.rows_scanned)
    }
}

/// Rows a scan has to read before it is worth pointing at.
///
/// The engine scans any table below `filter.min_table_rows` — 10,000 by
/// default — without consulting its index at all, on the measured argument that
/// the mask costs more than the read it would save. Flagging those would flag
/// every statement against a small table, which is every statement on a small
/// database.
pub const SCAN_HEAVY_ROWS: u64 = 10_000;

/// How many rows a statement may read per row returned before the ratio is the
/// interesting thing about it.
pub const SCAN_HEAVY_RATIO: u64 = 100;

/// Whether a statement read far more than it had to.
///
/// This is the reading the `rows_scanned` column exists for: a hundred rows
/// read for each one returned, over a table large enough for the index to have
/// been consulted, is a predicate nothing indexes. A statement that returned
/// nothing is judged as though it had returned one row — a full scan that found
/// no match is the same missing index, and dividing by zero would otherwise
/// hide the worst case.
pub fn scan_is_heavy(rows: Option<u64>, scanned: Option<u64>) -> bool {
    let (Some(rows), Some(scanned)) = (rows, scanned) else { return false };
    scanned >= SCAN_HEAVY_ROWS && scanned / rows.max(1) >= SCAN_HEAVY_RATIO
}

/// Everything the activity page reads, gathered in one round trip.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivitySnapshot {
    pub recent: Vec<StatementRecord>,
    /// Whether the server answered the recent-statement question at all.
    ///
    /// The same trap as the backup, replication and storage pages: a server
    /// built without `query_stats` — or from before it existed — does not
    /// refuse `SHOW QUERIES`. The wire layer answers an unknown `SHOW x` as a
    /// runtime parameter, with one empty column named after it, which parses
    /// into an empty list that looks exactly like a server that has run
    /// nothing.
    pub recent_supported: bool,
    pub shapes: Vec<StatementShape>,
    /// Whether the aggregate answered. On PostgreSQL this is false until
    /// somebody runs `CREATE EXTENSION pg_stat_statements`.
    pub shapes_supported: bool,
}

impl ActivitySnapshot {
    pub fn failures(&self) -> usize {
        self.recent.iter().filter(|r| r.failed()).count()
    }

    /// The slowest statement in the ring, which is the one an operator opened
    /// this page to find.
    pub fn slowest(&self) -> Option<&StatementRecord> {
        self.recent
            .iter()
            .max_by(|a, b| a.duration_ms.total_cmp(&b.duration_ms))
    }

    /// Rows read and rows returned across everything the ring holds.
    ///
    /// `None` when the server cannot answer either, rather than a ratio of
    /// zeroes.
    pub fn scan_totals(&self) -> Option<(u64, u64)> {
        let mut any = false;
        let mut scanned = 0u64;
        let mut returned = 0u64;
        for record in &self.recent {
            if let (Some(r), Some(s)) = (record.rows, record.rows_scanned) {
                any = true;
                returned += r;
                scanned += s;
            }
        }
        any.then_some((scanned, returned))
    }
}

/// Everything the backup page needs, gathered in one round trip.
///
/// One message rather than two because the page's first decision — which of
/// four screens to show — depends on *both* answers, and a view that renders
/// between them flickers through a wrong one.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupSnapshot {
    pub status: Vec<Stat>,
    /// Whether the server understood `SHOW BACKUP STATUS` at all.
    ///
    /// A server built before the statement existed does not report an error:
    /// the wire layer treats an unrecognised `SHOW x` as a request for a
    /// runtime parameter and answers with one empty column named after it. That
    /// parses into a `Stat` with no metric, and reading `configured` out of it
    /// yields "false" — so the page confidently said "no backup destination is
    /// configured" about a server that had one. Absence of the answer is not
    /// the answer.
    pub status_supported: bool,
    /// The catalogue, or why it could not be read. An error here is
    /// informative rather than fatal: "no backup destination is configured" is
    /// exactly what an unconfigured server says, and is the fallback signal
    /// when the status statement is not available.
    pub catalogue: Result<Vec<BackupEntry>, String>,
}

impl BackupSnapshot {
    fn metric(&self, name: &str) -> Option<&str> {
        self.status
            .iter()
            .find(|s| s.metric == name)
            .map(|s| s.value.trim())
    }

    fn flag(&self, name: &str) -> Option<bool> {
        Some(matches!(self.metric(name)?, "true" | "t" | "on" | "1" | "yes"))
    }

    /// Which of the four screens the backup page should show.
    pub fn state(&self) -> BackupState {
        if self.status_supported {
            if !self.flag("feature_compiled").unwrap_or(true) {
                return BackupState::NotCompiled;
            }
            return match self.flag("configured") {
                Some(true) => BackupState::Configured,
                // A status result that answered but named no `configured` is
                // not a status result. Treated as unknown rather than as a no.
                Some(false) => BackupState::NotConfigured,
                None => self.infer_from_catalogue(),
            };
        }
        self.infer_from_catalogue()
    }

    /// What the catalogue implies when the status statement is unavailable.
    ///
    /// A catalogue that lists objects proves a destination exists, whatever
    /// else the server will or will not say. The engine's refusal names itself,
    /// which is what distinguishes "nothing configured" from "configured and
    /// empty" — and from a third thing, a permission or network failure, which
    /// must not be reported as either.
    fn infer_from_catalogue(&self) -> BackupState {
        match &self.catalogue {
            Ok(_) => BackupState::ConfiguredWithoutStatus,
            Err(e) if e.contains("no backup destination is configured") => {
                BackupState::NotConfigured
            }
            Err(e) => BackupState::Unreadable(e.clone()),
        }
    }
}

/// Which of the four backup screens applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupState {
    /// The binary has no backup code in it.
    NotCompiled,
    /// It can back up and has not been told where to.
    NotConfigured,
    /// Fully reporting.
    Configured,
    /// Backups exist, but this server predates `SHOW BACKUP STATUS`, so the
    /// health panel has nothing to read. The catalogue and the actions still
    /// work; only the warnings are missing.
    ConfiguredWithoutStatus,
    /// Neither question could be answered — a permission error, a broken
    /// connection, something that is not a state of the backup system.
    Unreadable(String),
}

/// Where a restore should stop.
///
/// The variants are not interchangeable ways of saying the same thing:
/// `Latest` replays everything archived, and each of the other three
/// **deliberately leaves out** every commit after the point it names. The
/// console says so before running one, because "restore" reads to most people
/// as "get my data back" rather than "get my data back as of then".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreTarget {
    Latest,
    Snapshot(String),
    Lsn(u64),
    /// An RFC 3339 instant, as typed. Parsed by the server, which owns the
    /// grammar and the error message for it.
    Time(String),
}

impl RestoreTarget {
    /// Whether this target discards commits that were successfully archived.
    pub fn discards_later_commits(&self) -> bool {
        !matches!(self, RestoreTarget::Latest)
    }

    pub fn label(&self) -> &'static str {
        match self {
            RestoreTarget::Latest => "everything archived",
            RestoreTarget::Snapshot(_) => "one snapshot, no log replay past it",
            RestoreTarget::Lsn(_) => "through a commit sequence",
            RestoreTarget::Time(_) => "through a point in time",
        }
    }
}

/// Which recovery target a form is offering, without its value — for the
/// selector, which has to name the choices before one is filled in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreKind {
    Latest,
    Snapshot,
    Lsn,
    Time,
}

impl RestoreKind {
    pub const ALL: [RestoreKind; 4] = [
        RestoreKind::Latest,
        RestoreKind::Snapshot,
        RestoreKind::Lsn,
        RestoreKind::Time,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            RestoreKind::Latest => "Latest",
            RestoreKind::Snapshot => "Snapshot",
            RestoreKind::Lsn => "At commit (LSN)",
            RestoreKind::Time => "At time",
        }
    }
}

impl std::fmt::Display for RestoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// What a finished restore reported.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreReport {
    pub snapshot_id: String,
    pub target_dir: String,
    pub recovered_to_lsn: String,
    pub files_restored: u64,
    pub bytes_restored: u64,
    pub segments_replayed: u64,
    pub records_applied: u64,
    /// Records the restore deliberately did not apply, because they were past
    /// the recovery target. **This is the count of commits the restored copy
    /// does not have**, and the reason a restore report is shown at all rather
    /// than a "done".
    pub records_discarded: u64,
}

// ---------------------------------------------------------------------------
// Live metrics
// ---------------------------------------------------------------------------

/// How a series' numbers should be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesKind {
    /// A level that goes up and down. Plotted as it arrives.
    Bytes,
    Count,
    /// A counter that only goes up. Plotted as its **per-second change**,
    /// because the counter itself is a straight line with no information in it
    /// — what anyone wants from `xact_commit` is commits per second.
    Rate,
    /// Already a percentage.
    Percent,
}

impl SeriesKind {
    pub fn format(&self, value: f64) -> String {
        match self {
            SeriesKind::Bytes => bytes(value.max(0.0) as u64),
            SeriesKind::Count => count(value.max(0.0) as u64),
            SeriesKind::Rate => {
                if value >= 100.0 {
                    format!("{}/s", count(value as u64))
                } else if value >= 10.0 {
                    format!("{:.1}/s", value)
                } else {
                    format!("{:.2}/s", value)
                }
            }
            SeriesKind::Percent => format!("{:.1}%", value),
        }
    }
}

/// One chart on the dashboard.
///
/// `metric` is the name the stats result uses. A series whose metric the server
/// does not report simply does not appear — which is how one catalogue serves
/// both dialects without either having to know about the other's counters.
pub struct SeriesSpec {
    pub label: &'static str,
    pub metric: &'static str,
    pub kind: SeriesKind,
    pub help: &'static str,
}

/// Every series the dashboard knows how to draw, in the order it draws them.
///
/// Deliberately a fixed list rather than "chart every numeric metric": most
/// metrics are constants — `batch_size`, `wal_checkpoint_bytes` — and a wall of
/// flat lines is worse than six moving ones. Each entry here is something that
/// actually changes while a database is being used.
pub const SERIES: &[SeriesSpec] = &[
    SeriesSpec {
        label: "TOTAL SIZE",
        metric: "total_bytes",
        kind: SeriesKind::Bytes,
        help: "everything this database occupies",
    },
    SeriesSpec {
        label: "ROW SLOTS",
        metric: "row_slots",
        kind: SeriesKind::Count,
        help: "an upper bound on live rows",
    },
    SeriesSpec {
        label: "RECLAIMABLE",
        metric: "reclaimable_bytes",
        kind: SeriesKind::Bytes,
        help: "dead space a VACUUM would return",
    },
    SeriesSpec {
        label: "DATA",
        metric: "data_bytes",
        kind: SeriesKind::Bytes,
        help: "row data, without indexes",
    },
    SeriesSpec {
        label: "INDEX",
        metric: "index_bytes",
        kind: SeriesKind::Bytes,
        help: "what the indexes cost",
    },
    // RasterizedDB: the transaction id is a counter, so its slope is the
    // transaction rate.
    SeriesSpec {
        label: "TRANSACTIONS",
        metric: "next_transaction_id",
        kind: SeriesKind::Rate,
        help: "transactions started per second",
    },
    SeriesSpec {
        label: "OPEN TRANSACTIONS",
        metric: "transaction_bookkeeping",
        kind: SeriesKind::Count,
        help: "outcomes still being tracked",
    },
    SeriesSpec {
        label: "CACHE HITS",
        metric: "query_cache_hits",
        kind: SeriesKind::Rate,
        help: "queries served from a known offset, per second",
    },
    SeriesSpec {
        label: "CACHE MISSES",
        metric: "query_cache_misses",
        kind: SeriesKind::Rate,
        help: "queries that had to scan, per second",
    },
    // PostgreSQL.
    SeriesSpec {
        label: "COMMITS",
        metric: "query_commits",
        kind: SeriesKind::Rate,
        help: "transactions committed per second",
    },
    SeriesSpec {
        label: "ROLLBACKS",
        metric: "query_rollbacks",
        kind: SeriesKind::Rate,
        help: "transactions rolled back per second",
    },
    SeriesSpec {
        label: "TUPLES RETURNED",
        metric: "query_tuples_returned",
        kind: SeriesKind::Rate,
        help: "rows handed to clients per second",
    },
    SeriesSpec {
        label: "BUFFER HIT RATIO",
        metric: "cache_hit_ratio",
        kind: SeriesKind::Percent,
        help: "block reads served from shared buffers",
    },
    SeriesSpec {
        label: "BACKENDS",
        metric: "connection_backends",
        kind: SeriesKind::Count,
        help: "sessions attached to this database",
    },
];

/// How many samples a series keeps.
///
/// At the two-second default that is twenty minutes of history, which is long
/// enough to see a vacuum run or a load test start and short enough that the
/// whole thing is a few kilobytes.
pub const HISTORY: usize = 600;

/// The samples taken so far, and what is needed to turn a counter into a rate.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Plotted values, oldest first, one entry per metric name.
    pub series: BTreeMap<String, VecDeque<f64>>,
    /// The last raw counter reading, for the next rate.
    previous: BTreeMap<String, f64>,
    /// Samples taken, which is what tells the charts whether a rate series is
    /// simply waiting for its second reading rather than reporting zero.
    pub samples: u64,
}

impl Metrics {
    /// Folds one reading of the whole stats result into the history.
    ///
    /// `interval` is how long since the previous sample, in seconds; a rate is
    /// meaningless without it, and using the *configured* interval rather than
    /// the measured one would report a spike every time a slow query delayed a
    /// tick.
    pub fn sample(&mut self, stats: &[Stat], interval: f64) {
        for spec in SERIES {
            let Some(stat) = stats.iter().find(|s| s.metric == spec.metric) else {
                continue;
            };
            let Ok(raw) = stat.value.trim().parse::<f64>() else {
                continue;
            };

            let value = match spec.kind {
                SeriesKind::Rate => {
                    let previous = self.previous.insert(spec.metric.to_string(), raw);
                    match previous {
                        // The first reading of a counter establishes the
                        // baseline and plots nothing: there is no earlier
                        // reading to subtract, and plotting the counter itself
                        // would put one enormous point at the head of the
                        // series and flatten everything after it.
                        None => continue,
                        // A counter that went backwards means the server
                        // restarted or the statistics were reset. Zero is the
                        // truthful reading for that interval; a negative rate
                        // is not a thing.
                        Some(before) if raw < before => 0.0,
                        Some(before) if interval > 0.0 => (raw - before) / interval,
                        Some(_) => 0.0,
                    }
                }
                _ => raw,
            };

            let points = self.series.entry(spec.metric.to_string()).or_default();
            points.push_back(value);
            while points.len() > HISTORY {
                points.pop_front();
            }
        }
        self.samples += 1;
    }

    pub fn points(&self, metric: &str) -> Option<&VecDeque<f64>> {
        self.series.get(metric).filter(|points| !points.is_empty())
    }

    pub fn clear(&mut self) {
        self.series.clear();
        self.previous.clear();
        self.samples = 0;
    }
}
