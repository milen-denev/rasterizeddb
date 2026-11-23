//! The server's administrative statements as typed calls.
//!
//! Each method writes the statement the server's own grammar reads, with every
//! value quoted or checked so that it cannot change the statement, and reads
//! the answer into a struct. Most of these need a superuser login (the
//! server's rule): over a unix socket under `--rdb-socket-auth trust` every
//! connection is one.
//!
//! ```no_run
//! # async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
//! use rasterizeddb_client::admin::{Addressing, Placement, Store};
//!
//! client
//!     .admin()
//!     .add_store(
//!         &Store::s3("telemetry", "https://s3.example.com", "my-bucket")
//!             .region("eu-central-1")
//!             .prefix("pop-1/telemetry")
//!             .addressing(Addressing::Virtual)
//!             .credentials("AKIA…", "secret")
//!             .if_not_exists(),
//!     )
//!     .await?;
//! client.admin().place_table("events", &Placement::kv("telemetry")).await?;
//! # Ok(()) }
//! ```

use std::fmt;

use crate::client::Client;
use crate::params::{push_ident, push_text, quote_text};
use crate::row::Row;
use crate::value::Value;
use crate::{Error, Result};

/// The administrative calls of one [`Client`]; see [`Client::admin`].
pub struct Admin<'c> {
    client: &'c Client,
}

// ------------------------------------------------------------------- helpers

/// A name the storage and replication surfaces read as one bare word: they
/// split on whitespace and know no quoting.
fn bare_word<'n>(name: &'n str, what: &str) -> Result<&'n str> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(name)
    } else {
        Err(Error::Param(format!(
            "{what} {name:?} must be letters, digits, '_' and '-', starting with a letter or '_'"
        )))
    }
}

/// A table name for a statement that also takes a quoted identifier.
fn ident(name: &str) -> Result<String> {
    let mut out = String::new();
    push_ident(&mut out, name)?;
    Ok(out)
}

/// A value in a store's `WITH (…)` or `SET` list. The surface strips one layer
/// of `'…'` or `"…"` and knows no escape, so a value is wrapped in whichever
/// quote it does not contain. Never echoes the value: it may be a secret.
fn option_value(key: &str, value: &str) -> Result<String> {
    if value.contains(['\n', '\r', '\t', '\0']) {
        return Err(Error::Param(format!(
            "the value of {key} holds a control character, which the store grammar cannot carry"
        )));
    }
    if !value.contains('\'') {
        Ok(format!("'{value}'"))
    } else if !value.contains('"') {
        Ok(format!("\"{value}\""))
    } else {
        Err(Error::Param(format!(
            "the value of {key} holds both ' and \", which the store grammar cannot quote"
        )))
    }
}

fn text_lines(rows: crate::Rows) -> Result<Vec<String>> {
    rows.iter().map(|r| r.get::<String>(0)).collect()
}

fn ack_count(rows: &crate::Rows) -> u64 {
    rows.rows_affected().unwrap_or(0)
}

/// A row's column `i`, as `T`, for the fixed-shape `SHOW` answers.
macro_rules! col {
    ($row:expr, $i:expr) => {
        $row.get($i)?
    };
}

// -------------------------------------------------------------------- stores

/// How an S3 store addresses its bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addressing {
    /// `https://bucket.endpoint/key` (the server's default).
    Virtual,
    /// `https://endpoint/bucket/key`, as MinIO and most self-hosted stores
    /// want.
    Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreKind {
    Directory,
    S3,
}

/// A key/value store definition, for [`Admin::add_store`].
///
/// Credentials go to the server's secrets file (mode 0600), never to its
/// store catalogue, and are redacted from the server log; `Debug` here
/// redacts them too. Without credentials the store signs with the server's
/// `AWS_*` environment.
#[derive(Clone)]
pub struct Store {
    name: String,
    kind: StoreKind,
    options: Vec<(&'static str, String)>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    if_not_exists: bool,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("options", &self.options)
            .field(
                "credentials",
                &self.access_key_id.as_ref().map(|_| "<redacted>"),
            )
            .field("if_not_exists", &self.if_not_exists)
            .finish()
    }
}

impl Store {
    fn new(name: &str, kind: StoreKind) -> Self {
        Store {
            name: name.to_string(),
            kind,
            options: Vec::new(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            if_not_exists: false,
        }
    }

    /// An S3-compatible object store: its endpoint URL and bucket.
    pub fn s3(name: &str, endpoint: &str, bucket: &str) -> Self {
        Store::new(name, StoreKind::S3)
            .option("ENDPOINT", endpoint)
            .option("BUCKET", bucket)
    }

    /// A directory on the server's disks.
    pub fn directory(name: &str, path: &str) -> Self {
        Store::new(name, StoreKind::Directory).option("PATH", path)
    }

    fn option(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.options.retain(|(k, _)| *k != key);
        self.options.push((key, value.into()));
        self
    }

    /// The S3 region (the server's default is `us-east-1`).
    pub fn region(self, region: &str) -> Self {
        self.option("REGION", region)
    }

    /// The key prefix the store's objects or files sit under (default `rdb`).
    pub fn prefix(self, prefix: &str) -> Self {
        self.option("PREFIX", prefix)
    }

    /// `ADDRESSING`: how the bucket is addressed (the server's default is virtual-hosted).
    pub fn addressing(self, addressing: Addressing) -> Self {
        self.option(
            "ADDRESSING",
            match addressing {
                Addressing::Virtual => "virtual",
                Addressing::Path => "path",
            },
        )
    }

    /// The store's own access key, instead of the server's environment.
    pub fn credentials(mut self, access_key_id: &str, secret_access_key: &str) -> Self {
        self.access_key_id = Some(access_key_id.to_string());
        self.secret_access_key = Some(secret_access_key.to_string());
        self
    }

    /// A session token, with [`credentials`](Self::credentials).
    pub fn session_token(mut self, token: &str) -> Self {
        self.session_token = Some(token.to_string());
        self
    }

    /// Bytes per chunk object. Must be a multiple of the page size.
    pub fn chunk_size(self, bytes: u64) -> Self {
        self.option("CHUNK_SIZE", bytes.to_string())
    }

    /// Bytes per page within a chunk.
    pub fn page_size(self, bytes: u64) -> Self {
        self.option("PAGE_SIZE", bytes.to_string())
    }

    /// Bytes of the store's read cache.
    pub fn cache_bytes(self, bytes: u64) -> Self {
        self.option("CACHE_BYTES", bytes.to_string())
    }

    /// Bytes a write may hold in memory before it spills.
    pub fn spill_bytes(self, bytes: u64) -> Self {
        self.option("SPILL_BYTES", bytes.to_string())
    }

    /// `COMPACT_CHUNK_BYTES`: the chunk size compaction rewrites to.
    pub fn compact_chunk_bytes(self, bytes: u64) -> Self {
        self.option("COMPACT_CHUNK_BYTES", bytes.to_string())
    }

    /// `CHUNK_FETCH_PAGES`: pages fetched together when a chunk is read.
    pub fn chunk_fetch_pages(self, pages: u64) -> Self {
        self.option("CHUNK_FETCH_PAGES", pages.to_string())
    }

    /// `READAHEAD_CHUNKS`: chunks read ahead of a sequential scan.
    pub fn readahead_chunks(self, chunks: u64) -> Self {
        self.option("READAHEAD_CHUNKS", chunks.to_string())
    }

    /// `MAX_DELTAS`: delta objects a chunk may gather before it is compacted.
    pub fn max_deltas(self, deltas: u64) -> Self {
        self.option("MAX_DELTAS", deltas.to_string())
    }

    /// `FLUSH_INTERVAL_MS`: how often buffered writes are flushed to the store.
    pub fn flush_interval_ms(self, ms: u64) -> Self {
        self.option("FLUSH_INTERVAL_MS", ms.to_string())
    }

    /// Succeed when a store of this name already exists with the same
    /// definition (an error names the fields when it differs). Credentials
    /// given are applied either way, which is how keys are rotated by a
    /// deployment that re-adds its stores on every start.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// The statement, as it will be sent. It holds the credentials in the
    /// clear: do not log it.
    pub fn to_sql(&self) -> Result<String> {
        let name = bare_word(&self.name, "a store name")?;
        if self.access_key_id.is_some() != self.secret_access_key.is_some()
            || (self.session_token.is_some() && self.access_key_id.is_none())
        {
            return Err(Error::Param(
                "a store's access key id and secret go together, and a session token needs both"
                    .to_string(),
            ));
        }
        if self.kind == StoreKind::Directory && self.access_key_id.is_some() {
            return Err(Error::Param(
                "a directory store takes no credentials".to_string(),
            ));
        }
        let mut sql = String::from("STORAGE ADD STORE ");
        if self.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(name);
        sql.push_str(match self.kind {
            StoreKind::Directory => " dir",
            StoreKind::S3 => " s3",
        });
        let secrets = [
            ("ACCESS_KEY_ID", &self.access_key_id),
            ("SECRET_ACCESS_KEY", &self.secret_access_key),
            ("SESSION_TOKEN", &self.session_token),
        ];
        let mut entries = Vec::new();
        for (key, value) in &self.options {
            entries.push(format!("{key} = {}", option_value(key, value)?));
        }
        for (key, value) in secrets {
            if let Some(value) = value {
                if value.is_empty() {
                    return Err(Error::Param(format!("{key} is empty")));
                }
                entries.push(format!("{key} = {}", option_value(key, value)?));
            }
        }
        if !entries.is_empty() {
            sql.push_str(" WITH (");
            sql.push_str(&entries.join(", "));
            sql.push(')');
        }
        Ok(sql)
    }
}

/// One row of `SHOW STORAGE STORES`.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreInfo {
    /// The store's name, as tables are placed on it (`kv:<name>`).
    pub name: String,
    /// `dir`, `s3`, or the name of a kind this server build cannot open.
    pub kind: String,
    /// An S3 store's endpoint; a directory store's path.
    pub endpoint: String,
    /// An S3 store's region; empty for a directory store.
    pub region: String,
    /// An S3 store's bucket; empty for a directory store.
    pub bucket: String,
    /// The key prefix (S3) or subdirectory (directory store) its data sits under.
    pub prefix: String,
    /// `path` or `virtual` (S3 only).
    pub addressing: String,
    /// Its tunables, as the server prints them.
    pub settings: String,
    /// `store` when it has its own credentials, `environment` otherwise.
    pub credentials: String,
    /// Tables placed on it.
    pub tables: i64,
    /// Whether the server has it open (a table on it has been used).
    pub opened: bool,
    /// Whether this server build can open a store of its kind.
    pub supported: bool,
}

impl StoreInfo {
    fn read(row: &Row<'_>) -> Result<Self> {
        let text = |i| row.get::<Option<String>>(i).map(Option::unwrap_or_default);
        Ok(StoreInfo {
            name: text(0)?,
            kind: text(1)?,
            endpoint: text(2)?,
            region: text(3)?,
            bucket: text(4)?,
            prefix: text(5)?,
            addressing: text(6)?,
            settings: text(7)?,
            credentials: text(8)?,
            tables: col!(row, 9),
            opened: col!(row, 10),
            supported: col!(row, 11),
        })
    }
}

/// Where a table's rows live, for [`Admin::place_table`] and
/// [`Admin::move_table`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// In memory only: gone at restart.
    Memory,
    /// On a key/value store added with [`Admin::add_store`].
    Kv(String),
    /// In a directory on the server.
    Directory(String),
}

impl Placement {
    /// On the key/value store `store`.
    pub fn kv(store: &str) -> Self {
        Placement::Kv(store.to_string())
    }

    /// In the directory `path` on the server.
    pub fn directory(path: &str) -> Self {
        Placement::Directory(path.to_string())
    }

    fn spec(&self) -> Result<String> {
        match self {
            Placement::Memory => Ok("'memory:'".to_string()),
            Placement::Kv(store) => {
                quote_text(&format!("kv:{}", bare_word(store, "a store name")?))
            }
            Placement::Directory(path) => {
                if path.starts_with("kv:") || path.starts_with("memory") {
                    return Err(Error::Param(format!(
                        "{path:?} would be read as a store or memory spec, not a directory"
                    )));
                }
                quote_text(path)
            }
        }
    }
}

// ------------------------------------------------------------------ indexes

/// How an index is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexMethod {
    /// A B-tree: equality and ranges (`=`, `<`, `BETWEEN`, `ORDER BY`).
    #[default]
    BTree,
    /// A hash index: equality only, smaller and faster for it.
    Hash,
    /// Over a `jsonb` column, with the `jsonb_ops` operator class.
    Gin,
    /// Over a `jsonb` column, with `jsonb_path_ops`.
    GinPathOps,
}

/// An index definition, for [`Admin::create_index`].
#[derive(Debug, Clone)]
pub struct Index {
    table: String,
    columns: Vec<String>,
    name: Option<String>,
    method: IndexMethod,
    unique: bool,
    if_not_exists: bool,
}

impl Index {
    /// An index on `table` over `columns`, in order.
    pub fn on(table: &str, columns: &[&str]) -> Self {
        Index {
            table: table.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            name: None,
            method: IndexMethod::BTree,
            unique: false,
            if_not_exists: false,
        }
    }

    /// The index's name (otherwise `<table>_<columns>_idx`, or `_key` when unique).
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// How the index is built (B-tree when not set).
    pub fn using(mut self, method: IndexMethod) -> Self {
        self.method = method;
        self
    }

    /// A unique index. Created as a `UNIQUE` constraint, which is what the
    /// server enforces (it reads `CREATE UNIQUE INDEX` only over pgwire).
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Succeed without doing anything when an index of this name exists.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    /// The index's name: as given, or `<table>_<columns>_idx` (`_key` for a
    /// unique one), as PostgreSQL names them.
    pub fn index_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| {
            let suffix = if self.unique { "key" } else { "idx" };
            format!("{}_{}_{suffix}", self.table, self.columns.join("_"))
        })
    }

    /// The statement [`Admin::create_index`] sends.
    pub fn to_sql(&self) -> Result<String> {
        if self.columns.is_empty() {
            return Err(Error::Param(
                "an index needs at least one column".to_string(),
            ));
        }
        let columns = self
            .columns
            .iter()
            .map(|c| ident(c))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let name = ident(&self.index_name())?;
        let table = ident(&self.table)?;
        let exists = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };
        if self.unique {
            if self.method != IndexMethod::BTree {
                return Err(Error::Unsupported(
                    "a unique index is a UNIQUE constraint, which has no method".to_string(),
                ));
            }
            return Ok(format!(
                "ALTER TABLE {table} ADD CONSTRAINT {exists}{name} UNIQUE ({columns})"
            ));
        }
        let (method, opclass) = match self.method {
            IndexMethod::BTree => ("btree", ""),
            IndexMethod::Hash => ("hash", ""),
            IndexMethod::Gin => ("gin", ""),
            IndexMethod::GinPathOps => ("gin", " jsonb_path_ops"),
        };
        Ok(format!(
            "CREATE INDEX {exists}{name} ON {table} USING {method} ({columns}{opclass})"
        ))
    }
}

// ----------------------------------------------------------- introspection

/// One row of `SHOW TABLES`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    /// The table's name.
    pub name: String,
    /// `file`, `memory`, `kv`, …
    pub storage: String,
    /// Row slots in use, live rows and those vacuum has yet to reclaim.
    pub row_slots: i64,
    /// Columns, dropped ones included.
    pub columns: i64,
    /// Bytes of row data.
    pub data_bytes: i64,
    /// Bytes of row pointers.
    pub pointer_bytes: i64,
    /// Bytes of index data.
    pub index_bytes: i64,
    /// All of the table's bytes.
    pub total_bytes: i64,
    /// Bytes vacuum could give back.
    pub reclaimable_bytes: i64,
    /// Whether the filter index has caught up with the table.
    pub filters_ready: bool,
    /// Whether the table is read-only.
    pub immutable: bool,
    /// The primary key's columns, comma-separated; empty for none.
    pub primary_key: String,
}

/// One row of `DESCRIBE <table>`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    /// The column's position, from 1.
    pub position: i64,
    /// The column's name.
    pub name: String,
    /// As the engine names it: `BIGINT`, `TEXT`, `NUMERIC(10,2)`, …
    pub data_type: String,
    /// Its stored width in bytes (0 for a variable-width type).
    pub size: i64,
    /// Its offset within a row, for a fixed-width column.
    pub offset: i64,
    /// Whether it may hold NULL.
    pub nullable: bool,
    /// Whether a unique constraint covers it alone.
    pub unique: bool,
    /// Whether it is (part of) the primary key.
    pub primary_key: bool,
    /// Its `DEFAULT` expression, if it has one.
    pub default: Option<String>,
    /// Whether the filter index covers it.
    pub indexed: bool,
    /// Whether per-zone minimum and maximum are kept for it.
    pub zone_bounds: bool,
    /// For a shredded JSON column, the table holding its keys.
    pub json_table: Option<String>,
    /// Whether it was dropped (a dropped column keeps its slot until a rewrite).
    pub dropped: bool,
}

/// One row of `SHOW CONFIG`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigEntry {
    /// The setting's section (`wal`, `filter`, `vacuum`, …).
    pub section: String,
    /// The setting's name within its section.
    pub key: String,
    /// Its current value, if it has one to show.
    pub value: Option<String>,
    /// Whether `SET CONFIG` changes it without a restart.
    pub hot_reload: bool,
    /// How to change it: a `SET CONFIG` statement or a flag.
    pub set_with: String,
    /// What it does.
    pub help: String,
    /// A value set for the next start, if one is waiting.
    pub pending: Option<String>,
    /// The values it takes, when they are a list.
    pub options: Option<String>,
    /// Where its value came from (a flag, the config file, `SET CONFIG`).
    pub source: Option<String>,
}

/// A `metric, value, detail` row: the `STATS` and `STATUS` forms.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    /// What is measured.
    pub metric: String,
    /// Its value, as text.
    pub value: Option<String>,
    /// What the value means, or where it came from.
    pub detail: Option<String>,
}

/// One row of `SHOW PARTITIONS <table>`.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionInfo {
    /// The partition's id.
    pub pid: i64,
    /// The hash range's bounds, in hex.
    pub lo: String,
    /// The hash range's upper bound, in hex.
    pub hi: String,
    /// Where it is stored, when not in the table's own directory.
    pub location: Option<String>,
    /// Its state (`open`, `closed`, `moving`, …).
    pub state: String,
    /// Row slots in use.
    pub row_slots: i64,
    /// Its bytes on disk.
    pub total_bytes: i64,
}

/// One row of `SHOW BACKUPS`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupInfo {
    /// `snapshot` or `wal`.
    pub kind: String,
    /// The backup's id, as `RESTORE … FROM SNAPSHOT` takes it.
    pub id: String,
    /// When it was taken.
    pub created_at: String,
    /// The first log position it covers.
    pub start_lsn: i64,
    /// The last log position it covers.
    pub end_lsn: i64,
    /// Its size before compression.
    pub uncompressed_bytes: i64,
    /// Its size as stored.
    pub compressed_bytes: i64,
    /// Anything else the server reports about it.
    pub detail: Option<String>,
}

/// One row of `SHOW STORAGE LOCATIONS`.
#[derive(Debug, Clone, PartialEq)]
pub struct LocationInfo {
    /// The directory.
    pub location: String,
    /// `primary` or `backup`.
    pub role: String,
    /// Its share of new tables under a weighted policy.
    pub weight: i64,
    /// Whether new tables may be placed in it.
    pub enabled: bool,
    /// Whether it is the database's own directory.
    pub is_database_directory: bool,
    /// Tables stored in it.
    pub tables: i64,
    /// Free bytes on its filesystem.
    pub available_bytes: i64,
    /// Total bytes of its filesystem.
    pub total_bytes: i64,
}

/// A row whose columns this client names but does not type, for answers
/// whose shape is less settled (`SHOW REPLICATION PEER STATUS`).
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The columns' names, in order.
    pub columns: &'static [&'static str],
    /// The values, in the order of [`columns`](Self::columns).
    pub values: Vec<Value>,
}

impl Record {
    /// The value of the column named `column`.
    pub fn get(&self, column: &str) -> Option<&Value> {
        let i = self.columns.iter().position(|c| *c == column)?;
        self.values.get(i)
    }
}

/// Where [`Admin::restore`] recovers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreTarget {
    /// The newest state the backups hold.
    Latest,
    /// One snapshot, by the id `SHOW BACKUPS` lists.
    Snapshot(String),
    /// A log position.
    Lsn(u64),
    /// A point in time, RFC 3339 (`2026-11-16T12:00:00Z`).
    Time(String),
}

/// What a restore did.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreReport {
    /// The snapshot restored from.
    pub snapshot_id: String,
    /// The directory the database was restored into.
    pub target_dir: String,
    /// The log position recovery reached.
    pub recovered_to_lsn: String,
    /// Files restored from the snapshot.
    pub files_restored: i64,
    /// Bytes restored from the snapshot.
    pub bytes_restored: i64,
    /// Log segments replayed on top of it.
    pub segments_replayed: i64,
    /// Log records applied.
    pub records_applied: i64,
    /// Log records past the target, not applied.
    pub records_discarded: i64,
}

/// A role definition, for [`Admin::create_role`].
#[derive(Clone)]
pub struct Role {
    name: String,
    password: Option<String>,
    login: bool,
    superuser: bool,
}

impl fmt::Debug for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Role")
            .field("name", &self.name)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("login", &self.login)
            .field("superuser", &self.superuser)
            .finish()
    }
}

impl Role {
    /// A role that can log in (PostgreSQL's `CREATE USER`).
    pub fn user(name: &str) -> Self {
        Role {
            name: name.to_string(),
            password: None,
            login: true,
            superuser: false,
        }
    }

    /// A role that cannot log in until given LOGIN.
    pub fn group(name: &str) -> Self {
        Role {
            name: name.to_string(),
            password: None,
            login: false,
            superuser: false,
        }
    }

    /// Its password, turned into a SCRAM-SHA-256 verifier by the server.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Make it a superuser.
    pub fn superuser(mut self) -> Self {
        self.superuser = true;
        self
    }
}

/// A role name the role grammar reads: one whitespace-free token, quotes
/// stripped, no escapes.
fn role_name(name: &str) -> Result<&str> {
    if name.is_empty()
        || name
            .chars()
            .any(|c| c.is_whitespace() || c == '"' || c == '\'' || c == '\0')
    {
        return Err(Error::Param(format!(
            "role name {name:?} must not be empty or hold whitespace or quotes"
        )));
    }
    Ok(name)
}

// --------------------------------------------------------------------- calls

impl<'c> Admin<'c> {
    pub(crate) fn new(client: &'c Client) -> Self {
        Admin { client }
    }

    // ------------------------------------------------------------- stores

    /// Adds a key/value store. Returns the server's report, one line per row
    /// (what was added, how it signs, how to place a table on it).
    pub async fn add_store(&self, store: &Store) -> Result<Vec<String>> {
        text_lines(self.client.run(&store.to_sql()?).await?)
    }

    /// Changes a store's settings: `ALTER STORE <name> SET k = v, …`. The
    /// server refuses an address change while tables sit on the store, and
    /// applies credentials at its next start.
    pub async fn alter_store(&self, name: &str, settings: &[(&str, &str)]) -> Result<Vec<String>> {
        let name = bare_word(name, "a store name")?;
        if settings.is_empty() {
            return Err(Error::Param(
                "ALTER STORE needs at least one setting".to_string(),
            ));
        }
        let mut list = Vec::new();
        for (key, value) in settings {
            let key = bare_word(key, "a store setting")?;
            // The SET list is split on every comma and found by its last
            // " SET ", neither quote-aware.
            if value.contains(',') || value.to_ascii_uppercase().contains(" SET ") {
                return Err(Error::Param(format!(
                    "the value of {key} holds a ',' or ' SET ', which ALTER STORE cannot carry"
                )));
            }
            list.push(format!("{key} = {}", option_value(key, value)?));
        }
        text_lines(
            self.client
                .run(&format!(
                    "STORAGE ALTER STORE {name} SET {}",
                    list.join(", ")
                ))
                .await?,
        )
    }

    /// Replaces a store's credentials.
    pub async fn rotate_store_credentials(
        &self,
        name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<Vec<String>> {
        self.alter_store(
            name,
            &[
                ("ACCESS_KEY_ID", access_key_id),
                ("SECRET_ACCESS_KEY", secret_access_key),
            ],
        )
        .await
    }

    /// Drops a store. Refused while tables sit on it; its objects are not
    /// deleted.
    pub async fn drop_store(&self, name: &str) -> Result<Vec<String>> {
        let name = bare_word(name, "a store name")?;
        text_lines(
            self.client
                .run(&format!("STORAGE DROP STORE {name}"))
                .await?,
        )
    }

    /// Drops a store if there is one (the grammar has no `IF EXISTS`, so this
    /// looks first). Whether one was dropped.
    pub async fn drop_store_if_exists(&self, name: &str) -> Result<bool> {
        if self.store(name).await?.is_none() {
            return Ok(false);
        }
        self.drop_store(name).await?;
        Ok(true)
    }

    /// Every key/value store: `SHOW STORAGE STORES`.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>> {
        let rows = self.client.run("SHOW STORAGE STORES").await?;
        rows.iter().map(|r| StoreInfo::read(&r)).collect()
    }

    /// The store named `name`, if there is one.
    pub async fn store(&self, name: &str) -> Result<Option<StoreInfo>> {
        Ok(self.stores().await?.into_iter().find(|s| s.name == name))
    }

    /// Where a table not yet created will be placed: `STORAGE PLACE TABLE`.
    pub async fn place_table(&self, table: &str, placement: &Placement) -> Result<Vec<String>> {
        let table = bare_word(table, "a table name")?;
        text_lines(
            self.client
                .run(&format!(
                    "STORAGE PLACE TABLE {table} AT {}",
                    placement.spec()?
                ))
                .await?,
        )
    }

    /// Moves a table's rows to another backend: `STORAGE MOVE TABLE`. The
    /// report says which file-backed features the table gains or loses.
    pub async fn move_table(&self, table: &str, placement: &Placement) -> Result<Vec<String>> {
        let table = bare_word(table, "a table name")?;
        text_lines(
            self.client
                .run(&format!(
                    "STORAGE MOVE TABLE {table} TO {}",
                    placement.spec()?
                ))
                .await?,
        )
    }

    /// The directories tables are stored in: `SHOW STORAGE LOCATIONS`.
    pub async fn storage_locations(&self) -> Result<Vec<LocationInfo>> {
        let rows = self.client.run("SHOW STORAGE LOCATIONS").await?;
        rows.iter()
            .map(|r| {
                Ok(LocationInfo {
                    location: col!(r, 0),
                    role: col!(r, 1),
                    weight: col!(r, 2),
                    enabled: col!(r, 3),
                    is_database_directory: col!(r, 4),
                    tables: col!(r, 5),
                    available_bytes: col!(r, 6),
                    total_bytes: col!(r, 7),
                })
            })
            .collect()
    }

    /// Storage figures: `SHOW STORAGE STATS`.
    pub async fn storage_stats(&self) -> Result<Vec<Metric>> {
        self.metrics("SHOW STORAGE STATS").await
    }

    // ------------------------------------------------------------ indexes

    /// Creates an index (a unique one as a `UNIQUE` constraint).
    pub async fn create_index(&self, index: &Index) -> Result<()> {
        self.client.run(&index.to_sql()?).await.map(drop)
    }

    /// Drops an index by name (`IF EXISTS` when `if_exists`).
    pub async fn drop_index(&self, name: &str, if_exists: bool) -> Result<()> {
        let exists = if if_exists { "IF EXISTS " } else { "" };
        self.client
            .run(&format!("DROP INDEX {exists}{}", ident(name)?))
            .await
            .map(drop)
    }

    /// Seals a table's indexes to disk now, so a restart loads them instead
    /// of rebuilding: `SELECT rdb_index_checkpoint('t')`. The server's report
    /// (`"<clean> of <file sets> clean"`, then any that were not).
    pub async fn index_checkpoint(&self, table: &str) -> Result<String> {
        let rows = self
            .client
            .run(&format!(
                "SELECT rdb_index_checkpoint({})",
                quote_text(table)?
            ))
            .await?;
        rows.get(0)
            .ok_or_else(|| Error::Protocol("rdb_index_checkpoint answered no row".to_string()))?
            .get(0)
    }

    // --------------------------------------------------------- partitions

    /// Converts a plain table to hash partitioning on `columns` (one to
    /// three), with `partitions` partitions (the server's default is 64).
    pub async fn partition_by_hash(
        &self,
        table: &str,
        columns: &[&str],
        partitions: Option<u32>,
    ) -> Result<()> {
        if columns.is_empty() || columns.len() > 3 {
            return Err(Error::Param(
                "a hash partition key has one to three columns".to_string(),
            ));
        }
        let key = columns
            .iter()
            .map(|c| ident(c))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut sql = format!("ALTER TABLE {} PARTITION BY HASH ({key})", ident(table)?);
        if let Some(n) = partitions {
            sql.push_str(&format!(" PARTITIONS {n}"));
        }
        self.client.run(&sql).await.map(drop)
    }

    /// A partitioned table's partitions: `SHOW PARTITIONS`.
    pub async fn partitions(&self, table: &str) -> Result<Vec<PartitionInfo>> {
        let rows = self
            .client
            .run(&format!("SHOW PARTITIONS {}", ident(table)?))
            .await?;
        rows.iter()
            .map(|r| {
                Ok(PartitionInfo {
                    pid: col!(r, 0),
                    lo: col!(r, 1),
                    hi: col!(r, 2),
                    location: col!(r, 3),
                    state: col!(r, 4),
                    row_slots: col!(r, 5),
                    total_bytes: col!(r, 6),
                })
            })
            .collect()
    }

    // -------------------------------------------------------- maintenance

    /// Vacuums one table, or every table. `full` rewrites to give space back
    /// to the disk. The bytes reclaimed.
    pub async fn vacuum(&self, table: Option<&str>, full: bool) -> Result<u64> {
        let mut sql = String::from("VACUUM");
        if full {
            sql.push_str(" FULL");
        }
        if let Some(table) = table {
            sql.push(' ');
            sql.push_str(&ident(table)?);
        }
        Ok(ack_count(&self.client.run(&sql).await?))
    }

    /// Every table, with its size and state: `SHOW TABLES`.
    pub async fn tables(&self) -> Result<Vec<TableInfo>> {
        let rows = self.client.run("SHOW TABLES").await?;
        rows.iter()
            .map(|r| {
                Ok(TableInfo {
                    name: col!(r, 0),
                    storage: col!(r, 1),
                    row_slots: col!(r, 2),
                    columns: col!(r, 3),
                    data_bytes: col!(r, 4),
                    pointer_bytes: col!(r, 5),
                    index_bytes: col!(r, 6),
                    total_bytes: col!(r, 7),
                    reclaimable_bytes: col!(r, 8),
                    filters_ready: col!(r, 9),
                    immutable: col!(r, 10),
                    primary_key: r.get::<Option<String>>(11)?.unwrap_or_default(),
                })
            })
            .collect()
    }

    /// A table's columns as the engine describes them, including dropped ones
    /// (see [`ColumnInfo::dropped`]).
    pub async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let rows = self
            .client
            .run(&format!("DESCRIBE {}", ident(table)?))
            .await?;
        let blank = |v: Option<String>| v.filter(|s| !s.is_empty());
        rows.iter()
            .map(|r| {
                Ok(ColumnInfo {
                    position: col!(r, 0),
                    name: col!(r, 1),
                    data_type: col!(r, 2),
                    size: col!(r, 3),
                    offset: col!(r, 4),
                    nullable: col!(r, 5),
                    unique: col!(r, 6),
                    primary_key: col!(r, 7),
                    default: blank(col!(r, 8)),
                    indexed: col!(r, 9),
                    zone_bounds: col!(r, 10),
                    json_table: blank(col!(r, 11)),
                    dropped: col!(r, 12),
                })
            })
            .collect()
    }

    /// Every setting: `SHOW CONFIG`.
    pub async fn config(&self) -> Result<Vec<ConfigEntry>> {
        let rows = self.client.run("SHOW CONFIG").await?;
        let blank = |v: Option<String>| v.filter(|s| !s.is_empty());
        rows.iter()
            .map(|r| {
                Ok(ConfigEntry {
                    section: col!(r, 0),
                    key: col!(r, 1),
                    value: blank(col!(r, 2)),
                    hot_reload: col!(r, 3),
                    set_with: r.get::<Option<String>>(4)?.unwrap_or_default(),
                    help: r.get::<Option<String>>(5)?.unwrap_or_default(),
                    pending: blank(col!(r, 6)),
                    options: blank(col!(r, 7)),
                    source: blank(col!(r, 8)),
                })
            })
            .collect()
    }

    /// `SET CONFIG section.key = value`. A setting that is not hot-reloadable
    /// is saved for the next start. The server's answer.
    pub async fn set_config(&self, key: &str, value: &str) -> Result<String> {
        let (section, name) = key
            .split_once('.')
            .ok_or_else(|| Error::Param(format!("{key:?} is not section.key")))?;
        bare_word(section, "a config section")?;
        bare_word(name, "a config key")?;
        // The value is read unquoted, to the end of the statement.
        if value.is_empty()
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:-+/".contains(&b))
        {
            return Err(Error::Param(format!(
                "config value {value:?} must be letters, digits and . _ : - + /"
            )));
        }
        let rows = self
            .client
            .run(&format!("SET CONFIG {key} = {value}"))
            .await?;
        Ok(text_lines(rows)?.join("\n"))
    }

    /// Database-wide figures: `SHOW STATS`.
    pub async fn database_stats(&self) -> Result<Vec<Metric>> {
        self.metrics("SHOW STATS").await
    }

    /// One table's figures: `SHOW TABLE STATS`.
    pub async fn table_stats(&self, table: &str) -> Result<Vec<Metric>> {
        self.metrics(&format!("SHOW TABLE STATS {}", ident(table)?))
            .await
    }

    async fn metrics(&self, sql: &str) -> Result<Vec<Metric>> {
        let rows = self.client.run(sql).await?;
        rows.iter()
            .map(|r| {
                Ok(Metric {
                    metric: col!(r, 0),
                    value: r.get::<Option<Value>>(1)?.map(|v| v.to_string()),
                    detail: col!(r, 2),
                })
            })
            .collect()
    }

    // -------------------------------------------------------------- roles

    /// `CREATE ROLE`. The password is turned into a SCRAM verifier by the
    /// server and never stored as written.
    pub async fn create_role(&self, role: &Role) -> Result<()> {
        let mut sql = format!("CREATE ROLE {}", role_name(&role.name)?);
        sql.push_str(if role.login { " LOGIN" } else { " NOLOGIN" });
        sql.push_str(if role.superuser {
            " SUPERUSER"
        } else {
            " NOSUPERUSER"
        });
        if let Some(password) = &role.password {
            sql.push_str(" PASSWORD ");
            push_text(&mut sql, password)?;
        }
        self.client.run(&sql).await.map(drop)
    }

    /// Sets a role's password, or removes it (`None`).
    pub async fn set_role_password(&self, name: &str, password: Option<&str>) -> Result<()> {
        let mut sql = format!("ALTER ROLE {} PASSWORD ", role_name(name)?);
        match password {
            Some(p) => push_text(&mut sql, p)?,
            None => sql.push_str("NULL"),
        }
        self.client.run(&sql).await.map(drop)
    }

    /// Drops a role (`IF EXISTS` when `if_exists`).
    pub async fn drop_role(&self, name: &str, if_exists: bool) -> Result<()> {
        let exists = if if_exists { "IF EXISTS " } else { "" };
        self.client
            .run(&format!("DROP ROLE {exists}{}", role_name(name)?))
            .await
            .map(drop)
    }

    // ------------------------------------------------------------ backups

    /// Takes a snapshot to the configured backup destination. The compressed
    /// bytes written.
    pub async fn backup_database(&self) -> Result<u64> {
        Ok(ack_count(&self.client.run("BACKUP DATABASE").await?))
    }

    /// Ships the write-ahead log written since the last shipment.
    pub async fn backup_wal(&self) -> Result<u64> {
        Ok(ack_count(&self.client.run("BACKUP WAL").await?))
    }

    /// Removes backups past the retention policy. The objects removed.
    pub async fn backup_prune(&self) -> Result<u64> {
        Ok(ack_count(&self.client.run("BACKUP PRUNE").await?))
    }

    /// Every backup at the destination: `SHOW BACKUPS`.
    pub async fn backups(&self) -> Result<Vec<BackupInfo>> {
        let rows = self.client.run("SHOW BACKUPS").await?;
        rows.iter()
            .map(|r| {
                Ok(BackupInfo {
                    kind: col!(r, 0),
                    id: col!(r, 1),
                    created_at: col!(r, 2),
                    start_lsn: col!(r, 3),
                    end_lsn: col!(r, 4),
                    uncompressed_bytes: col!(r, 5),
                    compressed_bytes: col!(r, 6),
                    detail: col!(r, 7),
                })
            })
            .collect()
    }

    /// The backup subsystem's state: `SHOW BACKUP STATUS`.
    pub async fn backup_status(&self) -> Result<Vec<Metric>> {
        self.metrics("SHOW BACKUP STATUS").await
    }

    /// Restores the backups into a new directory, `to_dir`, which a server
    /// can then be started on. The running database is not touched.
    pub async fn restore(&self, target: &RestoreTarget, to_dir: &str) -> Result<RestoreReport> {
        let from = match target {
            RestoreTarget::Latest => "LATEST".to_string(),
            RestoreTarget::Snapshot(id) => format!("FROM SNAPSHOT {}", quote_text(id)?),
            RestoreTarget::Lsn(lsn) => format!("AT LSN {lsn}"),
            RestoreTarget::Time(t) => format!("AT TIME {}", quote_text(t)?),
        };
        let rows = self
            .client
            .run(&format!(
                "RESTORE DATABASE {from} TO {}",
                quote_text(to_dir)?
            ))
            .await?;
        let r = rows
            .get(0)
            .ok_or_else(|| Error::Protocol("RESTORE answered no row".to_string()))?;
        Ok(RestoreReport {
            snapshot_id: col!(r, 0),
            target_dir: col!(r, 1),
            recovered_to_lsn: col!(r, 2),
            files_restored: col!(r, 3),
            bytes_restored: col!(r, 4),
            segments_replayed: col!(r, 5),
            records_applied: col!(r, 6),
            records_discarded: col!(r, 7),
        })
    }

    // -------------------------------------------------------- replication

    /// Replication figures: `SHOW REPLICATION STATS`.
    pub async fn replication_stats(&self) -> Result<Vec<Metric>> {
        self.metrics("SHOW REPLICATION STATS").await
    }

    /// One record per peer: `peer`, `address`, `remote_node`, `connected`,
    /// `paused`, `acked_seq`, `lag`, `queued`, `records_sent`,
    /// `batches_sent`, `dropped`, `codec`, `last_ratio`, `last_error`,
    /// `state`.
    pub async fn replication_peers(&self) -> Result<Vec<Record>> {
        const COLUMNS: &[&str] = &[
            "peer",
            "address",
            "remote_node",
            "connected",
            "paused",
            "acked_seq",
            "lag",
            "queued",
            "records_sent",
            "batches_sent",
            "dropped",
            "codec",
            "last_ratio",
            "last_error",
            "state",
        ];
        let rows = self.client.run("SHOW REPLICATION PEER STATUS").await?;
        rows.iter()
            .map(|r| {
                Ok(Record {
                    columns: COLUMNS,
                    values: r.values()?,
                })
            })
            .collect()
    }

    /// Any `REPLICATION …` statement, for the verbs not typed here. The
    /// server's report lines.
    pub async fn replication(&self, statement: &str) -> Result<Vec<String>> {
        let sql = format!("REPLICATION {statement}");
        text_lines(self.client.run(&sql).await?)
    }

    // ----------------------------------------------------------------- kv

    /// The key/value cache (`KV …`), binary-safe: the same as
    /// [`Client::kv`].
    pub fn kv(&self) -> crate::kv::Kv<'c> {
        crate::kv::Kv::new(self.client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_s3_store_statement() {
        let store = Store::s3("telemetry", "https://s3.example.com", "bucket")
            .region("eu-central-1")
            .prefix("pop-1/it's")
            .addressing(Addressing::Virtual)
            .credentials("AKIA", "se,cr=et)")
            .if_not_exists();
        assert_eq!(
            store.to_sql().unwrap(),
            "STORAGE ADD STORE IF NOT EXISTS telemetry s3 WITH (ENDPOINT = 'https://s3.example.com', \
             BUCKET = 'bucket', REGION = 'eu-central-1', PREFIX = \"pop-1/it's\", \
             ADDRESSING = 'virtual', ACCESS_KEY_ID = 'AKIA', SECRET_ACCESS_KEY = 'se,cr=et)')"
        );
        assert!(
            !format!("{store:?}").contains("se,cr"),
            "Debug redacts the secret"
        );
    }

    #[test]
    fn store_values_that_cannot_be_written_are_refused_without_echo() {
        let both = Store::s3("s", "e", "b").credentials("k", "a'b\"c");
        let e = both.to_sql().unwrap_err().to_string();
        assert!(!e.contains("a'b"), "{e}");
        assert!(
            Store::s3("s", "e", "b")
                .credentials("k", "x\ny")
                .to_sql()
                .is_err()
        );
        assert!(Store::s3("bad name", "e", "b").to_sql().is_err());
        assert!(Store::s3("s;drop", "e", "b").to_sql().is_err());
        assert!(
            Store::directory("d", "/x")
                .credentials("k", "s")
                .to_sql()
                .is_err()
        );
        assert!(
            Store::s3("s", "e", "b")
                .session_token("t")
                .to_sql()
                .is_err()
        );
        assert_eq!(
            Store::directory("local", "/var/kv")
                .prefix("p")
                .to_sql()
                .unwrap(),
            "STORAGE ADD STORE local dir WITH (PATH = '/var/kv', PREFIX = 'p')"
        );
    }

    #[test]
    fn placements() {
        assert_eq!(Placement::Memory.spec().unwrap(), "'memory:'");
        assert_eq!(Placement::kv("telemetry").spec().unwrap(), "'kv:telemetry'");
        assert_eq!(
            Placement::directory("/data/o'k").spec().unwrap(),
            "'/data/o''k'"
        );
        assert!(Placement::kv("x y").spec().is_err());
        assert!(Placement::directory("kv:x").spec().is_err());
    }

    #[test]
    fn indexes() {
        assert_eq!(
            Index::on("users", &["email"])
                .unique()
                .if_not_exists()
                .to_sql()
                .unwrap(),
            "ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS users_email_key UNIQUE (email)"
        );
        assert_eq!(
            Index::on("users", &["name", "order"])
                .using(IndexMethod::Hash)
                .to_sql()
                .unwrap(),
            "CREATE INDEX users_name_order_idx ON users USING hash (name, \"order\")"
        );
        assert_eq!(
            Index::on("docs", &["body"])
                .name("docs_body")
                .using(IndexMethod::GinPathOps)
                .to_sql()
                .unwrap(),
            "CREATE INDEX docs_body ON docs USING gin (body jsonb_path_ops)"
        );
        assert!(Index::on("t", &[]).to_sql().is_err());
        assert!(
            Index::on("t", &["a"])
                .unique()
                .using(IndexMethod::Hash)
                .to_sql()
                .is_err()
        );
    }

    #[test]
    fn role_names_are_one_token() {
        assert!(role_name("app").is_ok());
        assert!(role_name("a b").is_err());
        assert!(role_name("a\"b").is_err());
        assert!(role_name("").is_err());
        assert!(!format!("{:?}", Role::user("u").password("hunter2")).contains("hunter2"));
    }
}
