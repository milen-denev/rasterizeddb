//! `SHOW TABLES`, `SHOW … STATS`, `SHOW SCHEMA MAP`, `SHOW/SET CONFIG` — the
//! introspection surface a database console needs.
//!
//! ```text
//! SHOW DATABASE STATS          -- one metric per row, for the whole database
//! SHOW TABLES                  -- a row per table, with its sizes
//! SHOW TABLE STATS <table>     -- one metric per row, for one table
//! SHOW PARTITIONS <table>      -- a row per partition of a partitioned table
//! SHOW COLUMNS FROM <table>    -- a row per column  (also: DESCRIBE <table>)
//! SHOW QUERIES [n]             -- the last n statements, newest first
//! SHOW QUERY STATS             -- one row per statement shape, with its totals
//! RESET QUERY STATS            -- forget both
//! SHOW SCHEMA MAP              -- a row per relationship, for drawing a graph
//! SHOW CONFIG                  -- every runtime knob, its value and how to change it
//! SHOW CONFIG KEYS             -- what SET CONFIG accepts
//! SET CONFIG <key> = <value>   -- change one, where the engine can do it live
//! ```
//!
//! # Why this is not `information_schema`
//!
//! PostgreSQL's catalogue is a set of tables, so a client introspects by
//! selecting from them. Nothing here is stored that way: the schema lives in a
//! per-table sidecar, the sizes live in the filesystem, and the tunables live in
//! process memory. Emulating `pg_class` would mean materialising all three into
//! rows a query planner then filters — the whole cost of the catalogue, to
//! answer questions that are a field read each.
//!
//! So these are statements rather than relations. They take no `WHERE`, they
//! return a fixed shape, and the shape is the one a console renders directly.
//! `rdb_studio` speaks this dialect natively and falls back to `pg_catalog`
//! queries when it is pointed at a real PostgreSQL server.
//!
//! # What the sizes mean
//!
//! Every figure here is read from the storage layer rather than counted, so
//! `SHOW TABLES` over a hundred tables is a hundred `stat` calls and not a
//! hundred scans. The consequence is that **`row_slots` counts pointer records,
//! not live rows**: a deleted row keeps its slot until a vacuum reclaims it, and
//! an updated row that had to move leaves one behind. It is an upper bound, and
//! it is named `row_slots` rather than `rows` so nothing reads it as the latter.
//! A console that wants the exact figure runs `SELECT COUNT(*)`, which scans.

use std::sync::atomic::Ordering;

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::storage_providers::traits::StorageIO;
use crate::memory_pool::MEMORY_POOL;

/// One parsed introspection statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Introspect {
    DatabaseStats,
    Tables,
    TableStats(String),
    /// One row per partition of a partitioned table.
    Partitions(String),
    Columns(String),
    SchemaMap,
    BackupStatus,
    /// The last `n` statements this server answered, newest first. `None`
    /// takes whatever the ring holds.
    RecentQueries(Option<u64>),
    /// Every statement shape and what it has cost in total.
    QueryStats,
    /// Empties both of the above.
    ResetQueryStats,
    Config,
    ConfigKeys,
    SetConfig {
        key: String,
        value: String,
    },
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Whether this statement belongs to the introspection surface.
///
/// Checked ahead of the DDL and DML prefixes, and *after* the filter,
/// replication and backup surfaces, each of which owns a longer `SHOW` phrase
/// of its own. `SHOW FILTER` must not be read here as a `SHOW` of something
/// called `FILTER`.
pub fn is_introspect_statement(sql: &str) -> bool {
    parse(sql).is_some()
}

/// Recognises an introspection statement, or `None` when it is something else.
///
/// A statement that starts like one of these and then goes wrong returns
/// `Some(Err(..))` rather than `None`, so the caller reports which clause was
/// unusable instead of a bare "unsupported query".
pub fn parse(sql: &str) -> Option<Result<Introspect, String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();
    let original: Vec<&str> = trimmed.split_whitespace().collect();

    // `DESCRIBE t` / `DESC t`, which every console offers and which is the
    // shortest way to ask what a table holds.
    if matches!(words.first().copied(), Some("DESCRIBE") | Some("DESC")) {
        return match original.get(1) {
            Some(name) if original.len() == 2 => {
                Some(Ok(Introspect::Columns(normalize_name(name))))
            }
            Some(_) => Some(Err(format!("{} takes one table name", words[0]))),
            None => Some(Err(format!("{} names no table", words[0]))),
        };
    }

    // `RESET QUERY STATS`. The only `RESET` this surface owns; every other one
    // is a runtime parameter the wire protocol answers for itself.
    if words.first().copied() == Some("RESET") && words.get(1).copied() == Some("QUERY") {
        return match (words.get(2).copied(), words.len()) {
            (Some("STATS"), 3) => Some(Ok(Introspect::ResetQueryStats)),
            _ => Some(Err("the statement is `RESET QUERY STATS`".to_string())),
        };
    }

    if words.first().copied() == Some("SET") && words.get(1).copied() == Some("CONFIG") {
        // Everything after `SET CONFIG`, from the original so the value keeps
        // its case: a path or a peer address is not case-insensitive.
        // Both keywords are ASCII, and `words` already matched them, so
        // slicing past them cannot land inside a character.
        let rest = trimmed["SET".len()..].trim_start();
        let rest = rest["CONFIG".len()..].trim_start();
        let Some((key, value)) = rest.split_once('=') else {
            return Some(Err("SET CONFIG needs `key = value`".to_string()));
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        if key.is_empty() {
            return Some(Err("SET CONFIG names no key".to_string()));
        }
        return Some(Ok(Introspect::SetConfig { key, value }));
    }

    if words.first().copied() != Some("SHOW") {
        return None;
    }

    match words.get(1).copied() {
        Some("TABLES") => {
            if words.len() > 2 {
                return Some(Err(format!(
                    "SHOW TABLES takes no arguments; unexpected {:?}",
                    original[2]
                )));
            }
            Some(Ok(Introspect::Tables))
        }
        // `SHOW STATS` is the short spelling of `SHOW DATABASE STATS`.
        Some("STATS") if words.len() == 2 => Some(Ok(Introspect::DatabaseStats)),
        Some("DATABASE") if words.get(2).copied() == Some("STATS") => {
            if words.len() > 3 {
                return Some(Err(format!(
                    "SHOW DATABASE STATS takes no arguments; unexpected {:?}",
                    original[3]
                )));
            }
            Some(Ok(Introspect::DatabaseStats))
        }
        Some("TABLE") if words.get(2).copied() == Some("STATS") => match original.get(3) {
            Some(name) if original.len() == 4 => {
                Some(Ok(Introspect::TableStats(normalize_name(name))))
            }
            Some(_) => Some(Err("SHOW TABLE STATS takes one table name".to_string())),
            None => Some(Err("SHOW TABLE STATS names no table".to_string())),
        },
        Some("PARTITIONS") => match original.get(2) {
            Some(name) if original.len() == 3 => {
                Some(Ok(Introspect::Partitions(normalize_name(name))))
            }
            Some(_) => Some(Err("SHOW PARTITIONS takes one table name".to_string())),
            None => Some(Err("SHOW PARTITIONS names no table".to_string())),
        },
        Some("COLUMNS") => {
            // `SHOW COLUMNS FROM t`, and the bare `SHOW COLUMNS t`.
            let index = if words.get(2).copied() == Some("FROM") {
                3
            } else {
                2
            };
            match original.get(index) {
                Some(name) if original.len() == index + 1 => {
                    Some(Ok(Introspect::Columns(normalize_name(name))))
                }
                Some(_) => Some(Err("SHOW COLUMNS takes one table name".to_string())),
                None => Some(Err("SHOW COLUMNS names no table".to_string())),
            }
        }
        // `SHOW BACKUPS` belongs to the backup surface and is left to it; this
        // is the *status* of that surface, which is a different question and
        // one that has to be answerable even when there is no catalogue to
        // list — including when the server was built without the feature at
        // all. A console cannot tell "no backups yet" from "backups are not
        // configured" from "this binary cannot do backups" otherwise, and those
        // want three different things done about them.
        Some("BACKUP") if words.get(2).copied() == Some("STATUS") => {
            if words.len() > 3 {
                return Some(Err(format!(
                    "SHOW BACKUP STATUS takes no arguments; unexpected {:?}",
                    original[3]
                )));
            }
            Some(Ok(Introspect::BackupStatus))
        }
        // `SHOW QUERIES` and `SHOW QUERIES 50`. The count is a cap on what is
        // returned, not on what is kept — the ring's size is
        // `SET CONFIG query_stats.recent`.
        Some("QUERIES") => match original.get(2) {
            None => Some(Ok(Introspect::RecentQueries(None))),
            Some(count) if original.len() == 3 => match count.parse::<u64>() {
                Ok(n) => Some(Ok(Introspect::RecentQueries(Some(n)))),
                Err(_) => Some(Err(format!(
                    "SHOW QUERIES takes a row count, and `{}` is not one",
                    count
                ))),
            },
            Some(_) => Some(Err("SHOW QUERIES takes at most a row count".to_string())),
        },
        Some("QUERY") if words.get(2).copied() == Some("STATS") => {
            if words.len() > 3 {
                return Some(Err(format!(
                    "SHOW QUERY STATS takes no arguments; unexpected {:?}",
                    original[3]
                )));
            }
            Some(Ok(Introspect::QueryStats))
        }
        Some("SCHEMA") if words.get(2).copied() == Some("MAP") => {
            if words.len() > 3 {
                return Some(Err(format!(
                    "SHOW SCHEMA MAP takes no arguments; unexpected {:?}",
                    original[3]
                )));
            }
            Some(Ok(Introspect::SchemaMap))
        }
        Some("CONFIG") => match words.get(2).copied() {
            None => Some(Ok(Introspect::Config)),
            Some("KEYS") if words.len() == 3 => Some(Ok(Introspect::ConfigKeys)),
            Some(other) => Some(Err(format!(
                "unknown SHOW CONFIG form `{}`: try SHOW CONFIG or SHOW CONFIG KEYS",
                other
            ))),
        },
        _ => None,
    }
}

/// Strips quoting and any schema qualifier from a relation reference.
///
/// Consoles quote every identifier, so the name arrives as `"users"` or
/// `"public"."users"` rather than `users`.
fn normalize_name(raw: &str) -> String {
    let raw = raw.trim().trim_end_matches(|c| c == ',' || c == ';');
    raw.split('.')
        .next_back()
        .unwrap_or(raw)
        .trim()
        .trim_matches('"')
        .to_string()
}

// ---------------------------------------------------------------------------
// Result shapes
// ---------------------------------------------------------------------------

/// The columns each statement's result set carries.
///
/// The wire protocol describes a row before it sends one, and these rows belong
/// to no relation, so their names come from the statement rather than a schema.
pub fn result_fields(statement: &Introspect) -> Option<Vec<(String, DbType)>> {
    let text = |name: &str| (name.to_string(), DbType::STRING);
    let number = |name: &str| (name.to_string(), DbType::I64);
    let flag = |name: &str| (name.to_string(), DbType::BOOL);
    let real = |name: &str| (name.to_string(), DbType::F64);
    let signed = |name: &str| (name.to_string(), DbType::I64);

    Some(match statement {
        // A metric per row rather than a column per metric: the set of metrics
        // grows, and a console that renders `(name, value)` keeps working when
        // it does, where one keyed on a fixed column list does not.
        Introspect::DatabaseStats | Introspect::TableStats(_) | Introspect::BackupStatus => {
            vec![text("metric"), text("value"), text("detail")]
        }
        Introspect::Tables => vec![
            text("table_name"),
            // Second, not last: it is an identity, not a measurement, and a
            // console reading the list wants to know what a table *is* before
            // it reads how big it is.
            text("storage"),
            number("row_slots"),
            number("columns"),
            number("data_bytes"),
            number("pointer_bytes"),
            number("index_bytes"),
            number("total_bytes"),
            number("reclaimable_bytes"),
            flag("filters_ready"),
            flag("immutable"),
            text("primary_key"),
        ],
        // `lo` and `hi` are text: the hash space is `[0, 2^64)`, so the last
        // partition's `hi` does not fit a BIGINT, and a `lo` past 2^63 would
        // read as negative. Hex, as the map log and the design write them.
        Introspect::Partitions(_) => vec![
            number("pid"),
            text("lo"),
            text("hi"),
            text("location"),
            text("state"),
            number("row_slots"),
            number("total_bytes"),
        ],
        Introspect::Columns(_) => vec![
            number("position"),
            text("column_name"),
            text("data_type"),
            number("size"),
            number("offset"),
            flag("nullable"),
            flag("is_unique"),
            flag("is_primary_key"),
            text("default_expression"),
            flag("is_indexed"),
            flag("zone_bounds"),
            text("json_table"),
            flag("is_dropped"),
        ],
        // What ran, what it cost, and how much of the table it had to read.
        // `rows_scanned` against `rows` is the ratio that says why a statement
        // is slow *when it is scan-bound*; `materialised` is the other cost
        // centre, and a statement can be expensive for either reason or both.
        // They are separate columns rather than one because they are different
        // kinds of number — `rows_scanned` means something against `rows` as a
        // ratio, a materialisation count means something on its own and has no
        // denominator. See `core::query_stats` for what is and is not counted.
        Introspect::RecentQueries(_) => vec![
            text("started_at"),
            real("duration_ms"),
            number("rows"),
            number("rows_scanned"),
            number("materialised"),
            real("materialised_ms"),
            text("status"),
            text("database"),
            text("username"),
            text("statement"),
            text("error"),
        ],
        Introspect::QueryStats => vec![
            signed("queryid"),
            number("calls"),
            real("total_ms"),
            real("mean_ms"),
            real("min_ms"),
            real("max_ms"),
            real("stddev_ms"),
            number("rows"),
            number("rows_scanned"),
            number("materialised"),
            real("materialised_ms"),
            number("errors"),
            text("last_seen"),
            text("statement"),
        ],
        Introspect::ResetQueryStats => vec![text("result")],
        Introspect::SchemaMap => vec![
            text("kind"),
            text("from_table"),
            text("from_column"),
            text("to_table"),
            text("to_column"),
            text("constraint_name"),
            text("on_delete"),
            text("on_update"),
        ],
        Introspect::Config | Introspect::ConfigKeys => vec![
            text("section"),
            text("key"),
            text("value"),
            flag("hot_reload"),
            text("set_with"),
            text("help"),
        ],
        // Reports what it changed, so a console can echo it.
        Introspect::SetConfig { .. } => vec![text("result")],
    })
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

pub async fn execute(statement: &Introspect, database: &Arc<Database>) -> Vec<u8> {
    match statement {
        Introspect::DatabaseStats => match database_stats(database).await {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::Tables => match tables(database).await {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::TableStats(name) => match table_stats(database, name).await {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::Partitions(name) => match partitions(database, name).await {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::Columns(name) => match columns(database, name) {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::SchemaMap => match schema_map(database) {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::RecentQueries(limit) => match recent_queries(*limit) {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::QueryStats => match query_stats() {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::ResetQueryStats => match reset_query_stats() {
            Ok(message) => rows(vec![row(&[Cell::Text(message)])]),
            Err(e) => failure(&e),
        },
        Introspect::BackupStatus => rows(backup_status(database).await),
        Introspect::Config => rows(config(database)),
        Introspect::ConfigKeys => rows(config_keys()),
        Introspect::SetConfig { key, value } => match set_config(database, key, value).await {
            Ok(message) => rows(vec![row(&[Cell::Text(message)])]),
            Err(e) => failure(&e),
        },
    }
}

/// Every file a table owns, and how large each is.
#[derive(Default)]
struct TableSizes {
    data: u64,
    pointers: u64,
    schema: u64,
    filters: u64,
    sequence: u64,
    /// A partitioned table's map log; 0 for a plain table.
    map: u64,
}

impl TableSizes {
    fn total(&self) -> u64 {
        self.data + self.pointers + self.schema + self.filters + self.sequence + self.map
    }

    /// Pointer records written, which bounds the live rows from above. See the
    /// module docs for why this is not a row count.
    fn row_slots(&self) -> u64 {
        self.pointers / ROW_POINTER_RECORD_LEN as u64
    }
}

/// Errs on a partition shell, whose files are its partitions'.
async fn sizes_of<S: StorageIO>(table: &crate::core::row::table::Table<S>) -> Result<TableSizes, String> {
    let data = table.data().map_err(|e| e.to_string())?;
    Ok(TableSizes {
        data: data.io_rows.get_len().await,
        pointers: data.io_pointers.get_len().await,
        schema: table.io_schema.get_len().await,
        // The index-store files since B14 retired `_flt.db` (§10, RV21).
        filters: table.index_store_bytes().map_err(|e| e.to_string())?,
        sequence: data.ids.io.get_len().await,
        map: 0,
    })
}

/// Every table's name: those in `tables`, and any partitioned table whose
/// schema could not be read, which has no shell there. Leaving it out would
/// list a subset; listed, its figures refuse with the reason.
fn table_names(database: &Database) -> Vec<String> {
    database.relation_names()
}

type DbTable = crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>;

/// What the reclamation service has done, summed over a partitioned table's
/// partitions. Each `last_*` is then the sum of every partition's own most
/// recent run.
#[derive(Default)]
struct VacuumFigures {
    runs: u64,
    compactions: u64,
    deferrals: u64,
    full_untruncated: u64,
    last_live_bytes: u64,
    last_dead_bytes: u64,
    last_reclaimed_bytes: u64,
    total_reclaimed_bytes: u64,
    panics: u64,
}

impl VacuumFigures {
    fn add(&mut self, stats: &crate::core::vacuum::VacuumStats) {
        self.runs += stats.runs.load(Ordering::Relaxed);
        self.compactions += stats.compactions.load(Ordering::Relaxed);
        self.deferrals += stats.deferrals.load(Ordering::Relaxed);
        self.full_untruncated += stats.full_untruncated.load(Ordering::Relaxed);
        self.last_live_bytes += stats.last_live_bytes.load(Ordering::Relaxed);
        self.last_dead_bytes += stats.last_dead_bytes.load(Ordering::Relaxed);
        self.last_reclaimed_bytes += stats.last_reclaimed_bytes.load(Ordering::Relaxed);
        self.total_reclaimed_bytes += stats.total_reclaimed_bytes.load(Ordering::Relaxed);
        self.panics += stats.panics.load(Ordering::Relaxed);
    }
}

/// A partitioned table's own figures, for `SHOW TABLE STATS`.
struct PartitionFigures {
    key: Vec<String>,
    declared: u32,
    materialized: u32,
    epoch: u64,
    route_v: u16,
}

/// Where a table's rows are and what they cost: a plain table's own files
/// and counters, or a partitioned table's summed over its materialized
/// partitions, with its parent files counted once. These are phase 2a's
/// running totals: one atomic load per file per partition, never a scan.
struct RowFigures {
    sizes: TableSizes,
    kind: String,
    location: String,
    last_row_id: u64,
    reclaimable: u64,
    filters_ready: bool,
    vacuum_enabled: bool,
    vacuum: VacuumFigures,
    in_place_updates: u64,
    /// A compaction holds the table, or for a partitioned table any partition.
    hard_locked: bool,
    /// A plain table's rows provider, for what only a provider can say (its
    /// key/value family, its mirrors). `None` for a partitioned table.
    plain_rows: Option<Arc<crate::core::storage_providers::backend::DbStorageProvider>>,
    partitioned: Option<PartitionFigures>,
}

/// The figures of `relation`. Errs when it cannot serve data (a broken
/// partitioned table, or a shell with no partitioned entry) rather than
/// reporting it empty.
async fn figures(relation: &Relation) -> Result<RowFigures, String> {
    let e = |e: std::io::Error| e.to_string();
    match relation {
        Relation::Plain(table) => {
            let sizes = sizes_of(table).await?;
            let data = table.data().map_err(e)?;
            let mut vacuum = VacuumFigures::default();
            vacuum.add(&data.vacuum_stats);
            Ok(RowFigures {
                sizes,
                kind: data.io_rows.kind().to_string(),
                location: data.io_rows.location(),
                last_row_id: data.ids.last_row_id.load(Ordering::Relaxed),
                reclaimable: data.free_space.free_bytes(),
                filters_ready: table.filters_ready().map_err(e)?,
                vacuum_enabled: data.free_space.is_enabled(),
                vacuum,
                in_place_updates: table.in_place_column_updates(),
                hard_locked: table.hard_locked.load(Ordering::Relaxed),
                plain_rows: Some(data.io_rows.clone()),
                partitioned: None,
            })
        }
        Relation::Partitioned(table) => {
            let totals = table.totals().await.map_err(e)?;
            let epoch = table.epoch().map_err(e)?;
            let parts = table
                .materialized(&epoch, &crate::core::partition::Route::All)
                .map_err(e)?;
            let mut reclaimable = 0;
            let mut filters_ready = true;
            let mut vacuum_enabled = true;
            let mut vacuum = VacuumFigures::default();
            let mut in_place_updates = 0;
            let mut hard_locked = false;
            for (_, part) in &parts {
                in_place_updates += part.in_place_column_updates();
                hard_locked |= part.hard_locked.load(Ordering::Relaxed);
                reclaimable += part.reclaimable_bytes().map_err(e)?;
                filters_ready &= part.filters_ready().map_err(e)?;
                vacuum_enabled &= part.vacuum_enabled().map_err(e)?;
                vacuum.add(part.vacuum_stats().map_err(e)?);
            }
            if parts.is_empty() {
                // What a partition materialized now would get.
                vacuum_enabled = crate::vacuum_enabled();
            }
            let schema = &table.shell().schema;
            let key = table
                .spec()
                .map_err(e)?
                .levels()
                .iter()
                .map(|level| {
                    schema
                        .fields
                        .iter()
                        .find(|f| !f.is_deleted && f.write_order as u32 == level.write_order)
                        .map(|f| f.name.clone())
                        .unwrap_or_else(|| format!("#{}", level.write_order))
                })
                .collect();
            let io_schema = &table.shell().io_schema;
            Ok(RowFigures {
                sizes: TableSizes {
                    data: totals.data,
                    pointers: totals.pointers,
                    schema: totals.schema,
                    filters: totals.filters,
                    sequence: totals.ids,
                    map: totals.map,
                },
                kind: io_schema.kind().to_string(),
                location: io_schema.location(),
                last_row_id: totals.ids_handed_out,
                reclaimable,
                filters_ready,
                vacuum_enabled,
                vacuum,
                in_place_updates,
                hard_locked,
                plain_rows: None,
                partitioned: Some(PartitionFigures {
                    key,
                    declared: totals.declared,
                    materialized: totals.materialized,
                    epoch: epoch.epoch,
                    route_v: table.route_v().await.map_err(e)?,
                }),
            })
        }
    }
}

async fn tables(database: &Arc<Database>) -> Result<Vec<Row>, String> {
    // Names are copied out first so no shard guard is held across the awaits
    // below: a `get_len` on a slow filesystem would otherwise block every
    // CREATE/DROP that hashes to the same shard for as long as it takes.
    // Engine-internal relations are left out. A `__cte_…`/`__cat_…` scratch
    // table exists only while one statement runs, and a `__json_…` shadow
    // table holds the shredded fields of a JSON column — none is a relation
    // anybody wrote or can use. Listed, they appeared and vanished between
    // refreshes of a client's table list, and a client that clicked one
    // queried a table that was already gone.
    let mut names: Vec<String> = table_names(database)
        .into_iter()
        .filter(|name| !name.starts_with("__"))
        .collect();
    names.sort();

    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let Some(relation) = database.relation(&name) else {
            continue;
        };
        let figures = figures(&relation).await?;
        let sizes = &figures.sizes;
        let schema = relation.schema();
        let visible = schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .count();

        out.push(row(&[
            Cell::Text(name),
            // The backend holding the row data. Every file of a table is opened
            // from one seed, so this is the table's answer and not just this
            // file's — `SHOW TABLE STATS` reports where it is.
            Cell::Text(figures.kind.clone()),
            Cell::Number(sizes.row_slots()),
            Cell::Number(visible as u64),
            Cell::Number(sizes.data),
            Cell::Number(sizes.pointers),
            Cell::Number(sizes.filters),
            Cell::Number(sizes.total()),
            Cell::Number(figures.reclaimable),
            Cell::Flag(figures.filters_ready),
            Cell::Flag(schema.is_immutable),
            Cell::Text(schema.primary_key.clone().unwrap_or_default()),
        ]));
    }
    Ok(out)
}

/// The whole database, as one metric per row.
///
/// # What this deliberately does not report
///
/// **Settings.** `wal_enabled`, `batch_size`, `filter_min_pruning_ratio` and
/// the rest of the knobs used to be here. They are not measurements: they say
/// what this server was told to do, they do not change while it runs, and every
/// one of them is already reported by `SHOW CONFIG` — which is also the only
/// place they can be *changed*. Listed here as well they were most of the rows,
/// which pushed the figures that actually move off the page, and a console
/// showing a constant beside the sizes invites reading it as a statistic.
///
/// **Internal ids and internal relations.** The database's own id and the
/// replication node id name this process to the engine; nothing a client can
/// do depends on either. And the `__…` tables — a JSON column's shadow table, a
/// CTE's temporary relation — are counted by neither `tables` nor `columns`,
/// because `SHOW TABLES` does not list them and a front page that disagreed
/// with the table list about how many tables exist is a front page nobody
/// trusts.
///
/// The byte totals still cover **every** file the database owns, shadow tables
/// included. That is what the disk actually holds, and a size that left them
/// out would be wrong in the direction that matters.
async fn database_stats(database: &Arc<Database>) -> Result<Vec<Row>, String> {
    let mut names: Vec<String> = table_names(database);
    names.sort();

    let mut total = TableSizes::default();
    let mut reclaimable = 0u64;
    let mut vacuum_panics = 0u64;
    let mut foreign_keys = 0usize;
    let mut unique_keys = 0usize;
    let mut json_columns = 0usize;
    let mut column_count = 0usize;
    let mut table_count = 0usize;

    for name in &names {
        let Some(relation) = database.relation(name) else {
            continue;
        };
        let figures = figures(&relation).await?;
        let sizes = &figures.sizes;
        // Bytes are counted for every table; the countable *things* are counted
        // only for the ones a client can name. See the note above the function.
        total.data += sizes.data;
        total.pointers += sizes.pointers;
        total.schema += sizes.schema;
        total.filters += sizes.filters;
        total.sequence += sizes.sequence;
        total.map += sizes.map;
        reclaimable += figures.reclaimable;
        vacuum_panics += figures.vacuum.panics;

        if name.starts_with("__") {
            continue;
        }
        let schema = relation.schema();
        table_count += 1;
        foreign_keys += schema
            .foreign_keys
            .iter()
            .filter(|f| !f.is_deleted)
            .count();
        unique_keys += schema
            .unique_keys
            .iter()
            .filter(|k| !k.is_deleted)
            .count();
        json_columns += schema
            .json_columns
            .iter()
            .filter(|c| !c.is_deleted)
            .count();
        column_count += schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .count();
    }

    let mut out = Vec::new();
    let mut push = |metric: &str, value: String, detail: &str| {
        out.push(row(&[
            Cell::Text(metric.to_string()),
            Cell::Text(value),
            Cell::Text(detail.to_string()),
        ]));
    };

    push(
        "name",
        database.name.clone(),
        "what clients connect to this database as",
    );
    push(
        "server_version",
        format!("rasterizeddb {}", env!("CARGO_PKG_VERSION")),
        "the build serving this database",
    );
    push(
        "location",
        database
            .location
            .clone()
            .unwrap_or_else(|| "(in memory)".to_string()),
        "directory holding this database's files",
    );
    // What is under the whole database, in the same vocabulary the table list
    // uses. `location` above is the directory it was opened with, which for a
    // key/value database is not where the bytes are.
    push(
        "storage",
        database.db_io_rows.kind().to_string(),
        "memory | file | mirrored | s3 | kv-directory | kv-memory",
    );
    push(
        "storage_location",
        database.db_io_rows.location(),
        "where those bytes actually are",
    );
    push(
        "tables",
        table_count.to_string(),
        "tables a client can name; the same set SHOW TABLES lists",
    );
    push(
        "columns",
        column_count.to_string(),
        "declared columns across those tables",
    );
    push(
        "row_slots",
        (total.pointers / ROW_POINTER_RECORD_LEN as u64).to_string(),
        "pointer records; an upper bound on live rows",
    );

    push("data_bytes", total.data.to_string(), "row data");
    push(
        "pointer_bytes",
        total.pointers.to_string(),
        "row pointer records",
    );
    push(
        "schema_bytes",
        total.schema.to_string(),
        "schemas and their sidecars",
    );
    push(
        "index_bytes",
        total.filters.to_string(),
        "persisted filter checkpoints",
    );
    push(
        "sequence_bytes",
        total.sequence.to_string(),
        "row-id sequences",
    );
    push(
        "partition_map_bytes",
        total.map.to_string(),
        "partitioned tables' map logs",
    );
    push(
        "total_bytes",
        total.total().to_string(),
        "every file the database owns",
    );
    push(
        "reclaimable_bytes",
        reclaimable.to_string(),
        "dead space a VACUUM would return",
    );

    push(
        "foreign_keys",
        foreign_keys.to_string(),
        "enforced references",
    );
    push(
        "unique_constraints",
        unique_keys.to_string(),
        "table-level UNIQUE constraints",
    );
    push(
        "json_columns",
        json_columns.to_string(),
        "columns mapped onto a shadow table",
    );

    let transactions = &database.transactions;
    push(
        "next_transaction_id",
        transactions.next_id().to_string(),
        "id the next transaction takes",
    );
    push(
        "oldest_active_transaction",
        transactions.oldest_active().to_string(),
        "what pins the reclamation horizon",
    );
    push(
        "transaction_bookkeeping",
        transactions.bookkeeping_entries().to_string(),
        "outcomes still being tracked",
    );
    push(
        "quiescent",
        transactions.quiescent().to_string(),
        "no transaction is running",
    );

    for (metric, value, detail) in durability_metrics(&durability_locations(database)) {
        push(&metric, value, &detail);
    }
    push(
        "vacuum_panics",
        vacuum_panics.to_string(),
        "background reclamation ticks that panicked and were caught",
    );

    // The page cache in front of an object store, which is the difference
    // between a scan that reads memory and one that pays for a request per
    // chunk. Reported for the database because the cache is the family's and is
    // shared by every table in it.
    #[cfg(feature = "kv_storage")]
    if let Some(family) = database.db_io_rows.kv_family() {
        let (hits, misses, resident) = family.cache_stats();
        push(
            "kv_store",
            family.store().describe(),
            "the store behind this database",
        );
        push(
            "kv_cache_hits",
            hits.to_string(),
            "chunks served without a request",
        );
        push(
            "kv_cache_misses",
            misses.to_string(),
            "each one is a request to the store",
        );
        push(
            "kv_cache_bytes",
            resident.to_string(),
            "chunks resident in memory",
        );
    }

    #[cfg(feature = "query_stats")]
    {
        let (recorded, shapes) = crate::core::query_stats::counts();
        push(
            "statements_recorded",
            recorded.to_string(),
            "kept for SHOW QUERIES",
        );
        push(
            "statement_shapes",
            shapes.to_string(),
            "distinct statements in the aggregate",
        );
    }

    let (hits, misses, entries) = crate::cache::query_cache::stats();
    push(
        "query_cache_hits",
        hits.to_string(),
        "queries served from a known offset",
    );
    push(
        "query_cache_misses",
        misses.to_string(),
        "queries that had to scan",
    );
    push(
        "query_cache_entries",
        entries.to_string(),
        "offsets currently remembered",
    );

    let replication = &database.replication;
    push(
        "replication_peers",
        replication.peer_count().to_string(),
        "peers this node ships transactions to",
    );

    Ok(out)
}

/// Every directory with a write-ahead log of its own, root first.
///
/// A storage pool gives each extra location its own log (see
/// `StorageManager::prepare_locations`), and a commit that touched a table
/// there is refused when *that* log is poisoned, so reading only the root's
/// reported a healthy database whose second disk refused every commit.
fn durability_locations(database: &Database) -> Vec<String> {
    let mut locations: Vec<String> = database.location.iter().cloned().collect();
    #[cfg(feature = "storage_pools")]
    for path in database.storage.all_paths() {
        let key = crate::core::wal::dir_key(&path);
        if !locations
            .iter()
            .any(|known| crate::core::wal::dir_key(known) == key)
        {
            locations.push(path);
        }
    }
    locations
}

/// Whether this database's commits can still be trusted, for `SHOW STATS`.
///
/// Each of these used to be visible only as one `error!` line at the moment it
/// happened. A poisoned log refuses every commit for the rest of the process,
/// and a log whose checkpoints keep declining or failing never shrinks, so
/// both are states an operator needs to be able to ask about afterwards.
///
/// `locations` is root first. The long-standing metric names are totals over
/// all of them — any poisoned, counts summed — so a client that reads
/// `wal_poisoned` cannot be told "false" while a second disk's log is
/// poisoned. With more than one location the text values name the directory
/// they came from, and a `wal_location:<path>` row per location says which
/// one is in trouble. A single-directory database reports exactly what it
/// always did.
fn durability_metrics(locations: &[String]) -> Vec<(String, String, String)> {
    let labelled = locations.len() > 1;
    let mut poisoned = false;
    let mut declined = 0u64;
    let mut failed = 0u64;
    let mut poison_reasons = Vec::new();
    let mut checkpoint_errors = Vec::new();
    let mut flush_failures = Vec::new();
    let mut per_location = Vec::new();

    let label = |path: &str, text: String| {
        if labelled { format!("{}: {}", path, text) } else { text }
    };

    for (index, path) in locations.iter().enumerate() {
        let wal = crate::core::wal::wal_for(path);
        let here_poisoned = wal.as_ref().is_some_and(|w| w.is_poisoned());
        let (here_declined, here_failed) =
            wal.as_ref().map(|w| w.checkpoint_counts()).unwrap_or((0, 0));
        let flush_failure =
            crate::core::storage_providers::file_sync::directory_flush_failure(path);

        poisoned |= here_poisoned;
        declined += here_declined;
        failed += here_failed;
        if let Some(reason) = wal.as_ref().and_then(|w| w.poison_reason()) {
            poison_reasons.push(label(path, reason));
        }
        if let Some(error) = wal.as_ref().and_then(|w| w.last_checkpoint_error()) {
            checkpoint_errors.push(label(path, error));
        }
        let flush_failed = flush_failure.is_some();
        if let Some(failure) = flush_failure {
            flush_failures.push(label(path, failure));
        }

        if labelled {
            let state = if here_poisoned {
                "poisoned"
            } else if flush_failed {
                "data flush failed"
            } else if wal.is_none() {
                "no log"
            } else {
                "ok"
            };
            per_location.push((
                format!("wal_location:{}", path),
                state.to_string(),
                format!(
                    "{}checkpoints declined {}, failed {}",
                    if index == 0 { "root; " } else { "" },
                    here_declined,
                    here_failed
                ),
            ));
        }
    }

    // Poison is per directory: each location has its own log, and a commit is
    // refused only by the logs it touches, so with several the old wording
    // told an operator the whole database had stopped when one disk had.
    let poisoned_detail = if labelled {
        "a flush, apply or log write failed in at least one location; commits \
         touching that location are refused until restart (see wal_location rows)"
    } else {
        "a flush, apply or log write failed; every commit is refused until restart"
    };
    let mut out = vec![
        (
            "wal_poisoned".to_string(),
            poisoned.to_string(),
            poisoned_detail.to_string(),
        ),
        (
            "wal_poison_reason".to_string(),
            poison_reasons.join("; "),
            String::new(),
        ),
        (
            "wal_checkpoints_declined".to_string(),
            declined.to_string(),
            "skipped while a transaction was open or another checkpoint ran".to_string(),
        ),
        (
            "wal_checkpoint_failures".to_string(),
            failed.to_string(),
            String::new(),
        ),
        (
            "wal_last_checkpoint_error".to_string(),
            checkpoint_errors.join("; "),
            String::new(),
        ),
        (
            "data_flush_failure".to_string(),
            flush_failures.join("; "),
            "a data file here failed a flush; commits here are refused until restart".to_string(),
        ),
    ];
    out.extend(per_location);
    out
}

async fn table_stats(database: &Arc<Database>, name: &str) -> Result<Vec<Row>, String> {
    let relation = database
        .relation(name)
        .ok_or_else(|| format!("relation `{}` does not exist", name))?;
    // A partitioned table's shell carries the schema and the per-table
    // counters below that are not about rows.
    let table = match &relation {
        Relation::Plain(table) => table.clone(),
        Relation::Partitioned(table) => table.shell().clone(),
    };
    let figures = figures(&relation).await?;
    let sizes = &figures.sizes;
    let schema = &table.schema;
    let vacuum = &figures.vacuum;

    let mut out = Vec::new();
    let mut push = |metric: &str, value: String, detail: &str| {
        out.push(row(&[
            Cell::Text(metric.to_string()),
            Cell::Text(value),
            Cell::Text(detail.to_string()),
        ]));
    };

    let visible: Vec<_> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .collect();
    let nullable = schema
        .fields
        .iter()
        .filter(|f| crate::core::rql::nullable::is_hidden(&f.name) && !f.is_deleted)
        .count();

    push("table", schema.name.clone(), "");
    // What is holding this table, and where. First, because it changes how
    // every figure below it should be read: a scan of an S3-backed table is a
    // bill, and a memory-backed one does not survive a restart at all.
    push(
        "storage",
        figures.kind.clone(),
        "memory | file | mirrored | s3 | kv-directory | kv-memory",
    );
    push(
        "storage_location",
        figures.location.clone(),
        "where those bytes actually are",
    );
    if let Some(p) = &figures.partitioned {
        push("partition_key", p.key.join(", "), "PARTITION BY HASH columns");
        push(
            "partitions_declared",
            p.declared.to_string(),
            "partitions the map declares",
        );
        push(
            "partitions_materialized",
            p.materialized.to_string(),
            "partitions with files; the figures below sum these",
        );
        push("map_epoch", p.epoch.to_string(), "the partition map's current epoch");
        push("route_v", p.route_v.to_string(), "the routing hash's version");
    }
    // The page cache in front of the store, reported here as well as on the
    // database because a table can be placed on a key/value store while the
    // database it belongs to is on files — `STORAGE PLACE TABLE t AT
    // 'kv:<store>'` — and then the database-level figures do not exist. The
    // counters are the store's, shared by every table placed on it, and the
    // detail says so rather than letting them read as this table's own.
    #[cfg(feature = "kv_storage")]
    if let Some(family) = figures.plain_rows.as_ref().and_then(|rows| rows.kv_family()) {
        let (hits, misses, resident) = family.cache_stats();
        push(
            "kv_store",
            family.store().describe(),
            "shared by every table on this store",
        );
        push(
            "kv_cache_hits",
            hits.to_string(),
            "chunks served without a request",
        );
        push(
            "kv_cache_misses",
            misses.to_string(),
            "each one is a request to the store",
        );
        push(
            "kv_cache_bytes",
            resident.to_string(),
            "chunks resident in memory",
        );
    }
    #[cfg(feature = "storage_pools")]
    if let Some(rows) = &figures.plain_rows {
        let copies = rows.backup_count();
        if copies > 0 {
            push(
                "backup_copies",
                copies.to_string(),
                "extra directories every write also goes to",
            );
        }
    }
    push(
        "columns",
        visible.len().to_string(),
        "declared and not dropped",
    );
    push(
        "nullable_columns",
        nullable.to_string(),
        "each carries a companion flag column",
    );
    push(
        "dropped_columns",
        schema
            .fields
            .iter()
            .filter(|f| f.is_deleted)
            .count()
            .to_string(),
        "slots kept rather than rewritten",
    );
    push(
        "fixed_row_width",
        visible
            .iter()
            .map(|f| f.size as u64)
            .sum::<u64>()
            .to_string(),
        "bytes a row occupies before variable-width columns",
    );

    push(
        "row_slots",
        sizes.row_slots().to_string(),
        "pointer records; an upper bound on live rows",
    );
    push(
        "last_row_id",
        figures.last_row_id.to_string(),
        "highest id handed out",
    );

    push("data_bytes", sizes.data.to_string(), "row data");
    // The measured average, where `fixed_row_width` above is the declared
    // minimum: the difference between them is what the variable-width columns
    // are actually costing, which is the figure capacity planning needs and the
    // one nothing here reported.
    push(
        "avg_row_bytes",
        (sizes.data / sizes.row_slots().max(1)).to_string(),
        "row data divided by pointer records",
    );
    push(
        "pointer_bytes",
        sizes.pointers.to_string(),
        "row pointer records",
    );
    push(
        "schema_bytes",
        sizes.schema.to_string(),
        "schema and its sidecars",
    );
    push(
        "index_bytes",
        sizes.filters.to_string(),
        "persisted filter checkpoint",
    );
    push(
        "sequence_bytes",
        sizes.sequence.to_string(),
        "row-id sequence",
    );
    if figures.partitioned.is_some() {
        push("partition_map_bytes", sizes.map.to_string(), "the partition map log");
    }
    push(
        "total_bytes",
        sizes.total().to_string(),
        "every file this table owns",
    );
    push(
        "reclaimable_bytes",
        figures.reclaimable.to_string(),
        "dead space a VACUUM would return",
    );
    // The ratio rather than the two figures, because the decision it informs —
    // is a VACUUM worth taking — is about the proportion and not the size. A
    // gigabyte of dead space in a terabyte table is noise; the same gigabyte in
    // a two-gigabyte table is half the disk.
    push(
        "dead_ratio",
        format!("{:.1}%", percent(figures.reclaimable, sizes.data)),
        "reclaimable against row data",
    );

    push(
        "vacuum_enabled",
        figures.vacuum_enabled.to_string(),
        "reclamation runs for this table; SHOW CONFIG can only say `per table`",
    );
    push(
        "vacuum_runs",
        vacuum.runs.to_string(),
        "",
    );
    push(
        "vacuum_compactions",
        vacuum.compactions.to_string(),
        "runs that actually reclaimed something",
    );
    push(
        "vacuum_deferrals",
        vacuum.deferrals.to_string(),
        "runs held off by an open snapshot",
    );
    push(
        "vacuum_full_untruncated",
        vacuum.full_untruncated.to_string(),
        "VACUUM FULL runs that compacted but could not return space (log busy)",
    );
    push(
        "vacuum_last_live_bytes",
        vacuum.last_live_bytes.to_string(),
        "",
    );
    push(
        "vacuum_last_dead_bytes",
        vacuum.last_dead_bytes.to_string(),
        "",
    );
    push(
        "vacuum_last_reclaimed_bytes",
        vacuum.last_reclaimed_bytes.to_string(),
        "what the most recent run actually returned",
    );
    push(
        "vacuum_total_reclaimed_bytes",
        vacuum.total_reclaimed_bytes.to_string(),
        "",
    );
    push(
        "vacuum_panics",
        vacuum.panics.to_string(),
        "background ticks that panicked and were caught",
    );

    push(
        "filters_ready",
        figures.filters_ready.to_string(),
        "the index has caught up with the rows",
    );
    push(
        "filter_enabled_for_table",
        crate::filter_settings()
            .table_enabled(&schema.name)
            .to_string(),
        "",
    );
    push(
        "in_place_column_updates",
        figures.in_place_updates.to_string(),
        "updates that rewrote only the assigned columns",
    );
    push(
        "hard_locked",
        figures.hard_locked.to_string(),
        "a compaction holds the table",
    );
    push("immutable", schema.is_immutable.to_string(), "");
    push(
        "primary_key",
        schema.primary_key.clone().unwrap_or_default(),
        "",
    );

    // The declarations that cost something on every write, counted before they
    // are listed: a table with forty checks is a table whose inserts are doing
    // forty evaluations, and that is worth seeing without counting rows.
    push(
        "check_constraints",
        schema
            .checks
            .iter()
            .filter(|c| !c.is_deleted)
            .count()
            .to_string(),
        "evaluated on every insert and update",
    );
    push(
        "generated_columns",
        schema
            .generated
            .iter()
            .filter(|g| !g.is_deleted)
            .count()
            .to_string(),
        "computed from the row as it is stored",
    );
    push(
        "default_expressions",
        schema
            .defaults
            .iter()
            .filter(|d| !d.is_deleted)
            .count()
            .to_string(),
        "",
    );
    push(
        "path_indexes",
        schema
            .path_indexes
            .iter()
            .filter(|p| !p.is_deleted)
            .count()
            .to_string(),
        "document paths carried by the filter index",
    );

    for key in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
        push(
            "unique_constraint",
            format!("{} ({})", key.name, key.columns.join(", ")),
            "",
        );
    }
    for fk in schema.foreign_keys.iter().filter(|f| !f.is_deleted) {
        push(
            "foreign_key",
            format!(
                "{} : {} -> {}.{}",
                fk.name, fk.column, fk.parent_table, fk.parent_column
            ),
            &format!("ON DELETE {}", action_name(&fk.on_delete)),
        );
    }
    for json in schema.json_columns.iter().filter(|c| !c.is_deleted) {
        push(
            "json_column",
            format!("{} -> {}", json.column, json.shadow_table),
            "document fields live in the shadow table",
        );
    }
    for check in schema.checks.iter().filter(|c| !c.is_deleted) {
        push(
            "check_constraint",
            format!("{} : {}", check.name, check.expression),
            "",
        );
    }
    for generated in schema.generated.iter().filter(|g| !g.is_deleted) {
        push(
            "generated_column",
            format!("{} = {}", generated.column, generated.expression),
            "",
        );
    }
    for index in schema.path_indexes.iter().filter(|p| !p.is_deleted) {
        push("path_index", format!("{} : {}", index.name, index.path), "");
    }

    Ok(out)
}

/// `SHOW PARTITIONS t`: one row per declared partition, by pid, with its
/// `[lo, hi)` range of the hash space, where its files are, whether it has
/// any, and its sizes (zero until it is materialized). Refused for a plain
/// table, and for a partitioned table that cannot be opened.
async fn partitions(database: &Arc<Database>, name: &str) -> Result<Vec<Row>, String> {
    let table = match database.relation(name) {
        None => return Err(format!("relation `{}` does not exist", name)),
        Some(Relation::Plain(_)) => return Err(format!("table `{}` is not partitioned", name)),
        Some(Relation::Partitioned(table)) => table,
    };
    let infos = table.partitions().await.map_err(|e| e.to_string())?;
    Ok(infos
        .into_iter()
        .map(|p| {
            row(&[
                Cell::Number(p.pid as u64),
                Cell::Text(format!("{:#018x}", p.lo)),
                Cell::Text(format!("{:#018x}", p.hi)),
                Cell::Text(p.location.unwrap_or_default()),
                Cell::Text(p.state.to_string()),
                Cell::Number(p.slots),
                Cell::Number(p.bytes),
            ])
        })
        .collect())
}

fn columns(database: &Arc<Database>, name: &str) -> Result<Vec<Row>, String> {
    let table = database
        .schema_view(name)
        .ok_or_else(|| match database.relation(name) {
            // Its schema could not be read; an empty column list would be a
            // wrong answer.
            Some(Relation::Partitioned(t)) => t.broken().unwrap_or("its schema is unavailable").to_string(),
            _ => format!("relation `{}` does not exist", name),
        })?;
    let schema = table.schema();
    let settings = crate::filter_settings();

    let mut out = Vec::new();
    let mut position = 0u64;

    for field in schema.fields.iter() {
        // Companion flag columns are engine-owned. They are reported through
        // the `nullable` flag of the column they belong to rather than as
        // columns of their own, which is what a client declared.
        if crate::core::rql::nullable::is_hidden(&field.name) {
            continue;
        }
        position += 1;

        let nullable =
            crate::core::rql::nullable::flag_field(&field.name, &schema.fields).is_some();
        let is_primary = schema.primary_key.as_deref() == Some(field.name.as_str());
        let in_unique_key = schema.unique_keys.iter().any(|k| {
            !k.is_deleted
                && k.columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&field.name))
        });
        let default = schema
            .defaults
            .iter()
            .find(|d| !d.is_deleted && d.column.eq_ignore_ascii_case(&field.name))
            .map(|d| d.expression.clone())
            .unwrap_or_default();
        let json_table = schema
            .json_columns
            .iter()
            .find(|c| !c.is_deleted && c.column.eq_ignore_ascii_case(&field.name))
            .map(|c| c.shadow_table.clone())
            .unwrap_or_default();
        let resolved = settings.resolve(&schema.name, &field.name);

        // A JSON column stores a `UUID` referencing the row its document was
        // mapped to. The client declared it `JSONB` and is handed the document
        // back, so reporting the storage type here would name something no
        // statement can use: `WHERE body = <uuid>` is not what anyone means.
        let declared = if json_table.is_empty() {
            sql_type_name(&field.db_type).to_string()
        } else {
            "JSONB".to_string()
        };

        out.push(row(&[
            Cell::Number(position),
            Cell::Text(field.name.clone()),
            Cell::Text(declared),
            Cell::Number(field.size as u64),
            Cell::Number(field.offset),
            Cell::Flag(nullable),
            Cell::Flag(field.is_unique || in_unique_key),
            Cell::Flag(is_primary),
            Cell::Text(default),
            Cell::Flag(resolved.indexed),
            Cell::Flag(resolved.zone_bounds),
            Cell::Text(json_table),
            Cell::Flag(field.is_deleted),
        ]));
    }

    Ok(out)
}

/// Every relationship in the database, as edges a console can draw.
///
/// Two kinds share the shape. A `fk` edge is a declared foreign key. A `json`
/// edge points a column at the shadow table its documents are mapped onto —
/// which is a real relationship here, not an implementation detail, because
/// that table is where a document's fields are queried from.
///
/// Errs on a partitioned table whose schema could not be read: its edges are
/// unknown, and leaving them out would draw a map with them missing.
fn schema_map(database: &Arc<Database>) -> Result<Vec<Row>, String> {
    let mut names: Vec<String> = table_names(database);
    names.sort();

    let mut out = Vec::new();
    for name in names {
        let Some(table) = database.schema_view(&name) else {
            if let Some(Relation::Partitioned(t)) = database.relation(&name) {
                return Err(t.broken().unwrap_or("its schema is unavailable").to_string());
            }
            continue;
        };
        let schema = table.schema();

        for fk in schema.foreign_keys.iter().filter(|f| !f.is_deleted) {
            out.push(row(&[
                Cell::Text("fk".to_string()),
                Cell::Text(schema.name.clone()),
                Cell::Text(fk.column.clone()),
                Cell::Text(fk.parent_table.clone()),
                Cell::Text(fk.parent_column.clone()),
                Cell::Text(fk.name.clone()),
                Cell::Text(action_name(&fk.on_delete).to_string()),
                Cell::Text(action_name(&fk.on_update).to_string()),
            ]));
        }

        for json in schema.json_columns.iter().filter(|c| !c.is_deleted) {
            out.push(row(&[
                Cell::Text("json".to_string()),
                Cell::Text(schema.name.clone()),
                Cell::Text(json.column.clone()),
                Cell::Text(json.shadow_table.clone()),
                Cell::Text(String::new()),
                Cell::Text(String::new()),
                Cell::Text(String::new()),
                Cell::Text(String::new()),
            ]));
        }
    }
    Ok(out)
}

/// Whether this server can back up at all, where it backs up to, and whether
/// the archiving that makes point-in-time recovery possible is actually
/// running.
///
/// Three states a console has to be able to tell apart, because each wants
/// something different done about it:
///
/// - **not built with the feature** — the binary has no backup code in it, so
///   nothing an operator does at runtime will help.
/// - **built but not configured** — start the server with `--s3-bucket`.
/// - **configured** — and then the interesting question is whether it is
///   *healthy*, which is mostly `archiving_suspended`.
///
/// That last one is the failure this statement exists for. When uploads start
/// failing, segments pile up in the staging directory; past the ceiling the
/// service goes back to discarding the log so a full disk cannot stop the
/// database. Point-in-time coverage is silently gone from that moment, and
/// nothing in `SHOW BACKUPS` says so — the catalogue simply stops growing.
async fn backup_status(database: &Arc<Database>) -> Vec<Row> {
    let mut out = Vec::new();
    let mut push = |metric: &str, value: String, detail: &str| {
        out.push(row(&[
            Cell::Text(metric.to_string()),
            Cell::Text(value),
            Cell::Text(detail.to_string()),
        ]));
    };

    push(
        "feature_compiled",
        cfg!(feature = "s3_backup").to_string(),
        "whether this binary was built with --features s3_backup",
    );

    #[cfg(not(feature = "s3_backup"))]
    {
        let _ = database;
        push(
            "configured",
            "false".to_string(),
            "rebuild with --features s3_backup to back up to S3",
        );
        return out;
    }

    #[cfg(feature = "s3_backup")]
    {
        let _ = database;
        let Some(service) = crate::core::backup::service() else {
            push(
                "configured",
                "false".to_string(),
                "start the server with --s3-bucket (and --s3-region / --s3-endpoint) to enable backups",
            );
            return out;
        };

        let config = service.config();
        push("configured", "true".to_string(), "");
        push("bucket", config.bucket.clone(), "");
        push(
            "prefix",
            config.normalized_prefix(),
            "one database per prefix",
        );
        push("region", config.region.clone(), "");
        push(
            "endpoint",
            format!(
                "{}://{}:{}",
                if config.endpoint.tls { "https" } else { "http" },
                config.endpoint.host,
                config.endpoint.port
            ),
            "S3, or whatever speaks its API",
        );
        push(
            "source_directory",
            service.location().to_string(),
            "the database these backups are of",
        );
        push(
            "instance_id",
            service.instance_id().to_string(),
            "identifies this database in the prefix",
        );

        push(
            "snapshot_interval_secs",
            config.snapshot_interval_secs.to_string(),
            if config.snapshot_interval_secs == 0 {
                "0 means snapshots are taken only when asked for"
            } else {
                ""
            },
        );
        push(
            "wal_flush_secs",
            config.wal_flush_secs.to_string(),
            if config.wal_flush_secs == 0 {
                "0 means the log is archived only when asked for, so there is no automatic point-in-time coverage"
            } else {
                "how far back recovery can reach between flushes"
            },
        );
        push(
            "wal_bundle_bytes",
            config.wal_bundle_bytes.to_string(),
            "staged bytes that trigger a flush without waiting for the interval",
        );
        push(
            "retain_snapshots",
            config.retain_snapshots.to_string(),
            if config.retain_snapshots == 0 {
                "0 keeps every snapshot; BACKUP PRUNE deletes nothing"
            } else {
                "older snapshots, and the segments only they needed, are deleted by BACKUP PRUNE"
            },
        );

        let staged = service.staged_bytes();
        let suspended = service.archiving_is_suspended();
        push(
            "staged_bytes",
            staged.to_string(),
            "written but not yet uploaded",
        );
        push(
            "staging_max_bytes",
            config.staging_max_bytes.to_string(),
            "0 is unlimited",
        );
        push(
            "archiving_suspended",
            suspended.to_string(),
            if suspended {
                "ARCHIVING IS SUSPENDED: staged log exceeded the ceiling, so the log is being \
                 discarded and point-in-time recovery has a hole in it from now until uploads \
                 work again"
            } else {
                ""
            },
        );

        let manifest = service.manifest_snapshot().await;
        push("snapshots", manifest.snapshots.len().to_string(), "");
        push("wal_bundles", manifest.segments.len().to_string(), "");
        push(
            "catalogue_updated_at",
            crate::core::backup::timeutil::format_rfc3339(manifest.updated_at),
            "",
        );

        match manifest.snapshots_newest_first().first().copied() {
            None => push(
                "newest_snapshot",
                "none".to_string(),
                "nothing to restore from yet; run BACKUP DATABASE",
            ),
            Some(snapshot) => {
                push(
                    "newest_snapshot",
                    snapshot.id.clone(),
                    &crate::core::backup::timeutil::format_rfc3339(snapshot.finished_at),
                );
                push("newest_snapshot_end_lsn", snapshot.end_lsn.to_string(), "");
            }
        }

        // The furthest a point-in-time restore can currently reach. It is the
        // question `SHOW BACKUPS` makes you work out by reading every row.
        let recoverable = manifest
            .segments
            .iter()
            .map(|segment| segment.last_lsn)
            .max()
            .unwrap_or(0);
        push(
            "recoverable_to_lsn",
            recoverable.to_string(),
            "the highest commit any archived log bundle covers",
        );

        out
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// One knob `SHOW CONFIG` reports and `SET CONFIG` may accept.
struct ConfigKey {
    section: &'static str,
    key: &'static str,
    /// Whether a running server can change it. A `false` here is not a missing
    /// feature: the value is read once at start-up into a `OnceLock` that later
    /// code assumes is stable, and making it mutable would mean auditing every
    /// reader for what a mid-flight change does.
    hot: bool,
    /// The statement or flag that changes it, quoted verbatim so a console can
    /// offer it.
    set_with: &'static str,
    help: &'static str,
}

const CONFIG_KEYS: &[ConfigKey] = &[
    ConfigKey {
        section: "server",
        key: "batch_size",
        hot: false,
        set_with: "--batch-size N (restart)",
        help: "row pointers fetched per batch",
    },
    ConfigKey {
        section: "server",
        key: "concurrent_threads",
        hot: false,
        set_with: "--concurrent-threads N (restart)",
        help: "workers a query may use",
    },
    ConfigKey {
        section: "wal",
        key: "enabled",
        hot: false,
        // No flag sets it: the log-off mode was never reachable or tested,
        // and later durability work (in-place writes, the outcome log)
        // assumes the log. Advertising `--wal` named a flag that does not
        // exist, so a server configured by this row would not start.
        set_with: "fixed: always on",
        help: "route durable writes through the log",
    },
    ConfigKey {
        section: "wal",
        key: "checkpoint_bytes",
        hot: false,
        set_with: "--wal-checkpoint-bytes N (restart)",
        help: "log bytes between checkpoints",
    },
    ConfigKey {
        section: "wal",
        key: "synchronous_commit",
        hot: true,
        set_with: "SET CONFIG wal.synchronous_commit = off",
        help: "commits wait for their fdatasync; off risks the last writes, not the database",
    },
    ConfigKey {
        section: "wal",
        key: "writer_delay_ms",
        hot: true,
        set_with: "SET CONFIG wal.writer_delay_ms = 200",
        help: "how long an asynchronous commit may go unsynced",
    },
    ConfigKey {
        section: "cache",
        key: "enabled",
        hot: false,
        set_with: "--enable-cache BOOL (restart)",
        help: "serve repeated queries from a known offset",
    },
    ConfigKey {
        section: "filter",
        key: "enabled",
        hot: true,
        set_with: "SET CONFIG filter.enabled = on",
        help: "master switch for the filter index",
    },
    ConfigKey {
        section: "filter",
        key: "backfill_on_open",
        hot: true,
        set_with: "SET CONFIG filter.backfill_on_open = on",
        help: "index pre-existing rows at open",
    },
    ConfigKey {
        section: "filter",
        key: "min_pruning_ratio",
        hot: true,
        set_with: "SET CONFIG filter.min_pruning_ratio = 0.9",
        help: "skip the mask when it prunes less than this (0..1)",
    },
    ConfigKey {
        section: "filter",
        key: "checkpoint_interval_secs",
        hot: true,
        set_with: "SET CONFIG filter.checkpoint_interval_secs = 60",
        help: "how often the maintenance task checkpoints",
    },
    ConfigKey {
        section: "filter",
        key: "min_f",
        hot: true,
        set_with: "SET CONFIG filter.min_f = 8",
        help: "floor on fingerprint bits (4..16)",
    },
    ConfigKey {
        section: "filter",
        key: "default_f",
        hot: true,
        set_with: "SET CONFIG filter.default_f = 12",
        help: "fingerprint bits before cardinality is known",
    },
    ConfigKey {
        section: "filter",
        key: "max_f",
        hot: true,
        set_with: "SET CONFIG filter.max_f = 16",
        help: "ceiling on fingerprint bits (4..16)",
    },
    ConfigKey {
        section: "filter",
        key: "min_selectivity",
        hot: true,
        set_with: "SET CONFIG filter.min_selectivity = 0.1",
        help: "largest row fraction a predicate may match and still be indexed",
    },
    ConfigKey {
        section: "filter",
        key: "min_table_rows",
        hot: true,
        set_with: "SET CONFIG filter.min_table_rows = 10000",
        help: "tables smaller than this are scanned without consulting the index",
    },
    ConfigKey {
        section: "filter",
        key: "zone_bounds",
        hot: true,
        set_with: "SET CONFIG filter.zone_bounds = on",
        help: "keep per-zone min/max for range pruning",
    },
    ConfigKey {
        section: "filter",
        key: "bloom",
        hot: true,
        set_with: "SET CONFIG filter.bloom = off",
        help: "keep a per-superzone span summary",
    },
    ConfigKey {
        section: "replication",
        key: "active",
        hot: true,
        set_with: "REPLICATION ENABLE | REPLICATION DISABLE",
        help: "ship committed transactions to the peers",
    },
    ConfigKey {
        section: "replication",
        key: "compression",
        hot: true,
        set_with: "SET CONFIG replication.compression = 'auto:zstd:1'",
        help: "none | zstd[:level] | auto[:zstd[:level]]",
    },
    ConfigKey {
        section: "replication",
        key: "mode",
        hot: true,
        set_with: "SET CONFIG replication.mode = 'quorum:1'",
        help: "async | sync | quorum:N | all",
    },
    ConfigKey {
        section: "replication",
        key: "conflict",
        hot: true,
        set_with: "SET CONFIG replication.conflict = 'last-writer-wins'",
        help: "abort | skip | last-writer-wins",
    },
    ConfigKey {
        section: "replication",
        key: "ack_timeout_ms",
        hot: true,
        set_with: "SET CONFIG replication.ack_timeout_ms = 2000",
        help: "how long a synchronous commit waits for peers",
    },
    ConfigKey {
        section: "replication",
        key: "batch_max_records",
        hot: true,
        set_with: "SET CONFIG replication.batch_max_records = 256",
        help: "records in one outbound batch",
    },
    ConfigKey {
        section: "replication",
        key: "batch_linger_us",
        hot: true,
        set_with: "SET CONFIG replication.batch_linger_us = 500",
        help: "how long a sender waits for more records",
    },
    ConfigKey {
        section: "query_stats",
        key: "enabled",
        hot: true,
        set_with: "SET CONFIG query_stats.enabled = off",
        help: "record what each statement cost, for SHOW QUERIES and pg_stat_statements",
    },
    ConfigKey {
        section: "query_stats",
        key: "recent",
        hot: true,
        set_with: "SET CONFIG query_stats.recent = 256",
        help: "statements kept for SHOW QUERIES; the per-shape totals are kept regardless",
    },
    ConfigKey {
        section: "vacuum",
        key: "enabled",
        hot: true,
        set_with: "SET CONFIG vacuum.enabled = on",
        help: "background reclamation, for every table in this database",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "idle_checkpoint_secs",
        hot: true,
        set_with: "SET CONFIG index_snapshot.idle_checkpoint_secs = 300",
        help: "seconds a table goes unwritten before its index snapshot is taken; 0 turns the idle checkpoint off",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "seal_timeout_ms",
        hot: true,
        set_with: "SET CONFIG index_snapshot.seal_timeout_ms = 5000",
        help: "how long a mandatory snapshot (shutdown, rdb_index_checkpoint) waits for writers in flight",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "shutdown_deadline_secs",
        hot: true,
        set_with: "SET CONFIG index_snapshot.shutdown_deadline_secs = 120",
        help: "the whole of shutdown's snapshots; keep it below the service manager's stop timeout",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "writeback_mb",
        hot: true,
        set_with: "SET CONFIG index_snapshot.writeback_mb = 64",
        help: "MiB of a snapshot written between two writeback starts",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "index_storage",
        hot: true,
        set_with: "SET CONFIG index_snapshot.index_storage = snapshot",
        help: "rebuild: indexes are built from rows at every open; snapshot: seals write B-tree, hash and path indexes, and an open loads them",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "max_concurrent_loads",
        hot: true,
        set_with: "SET CONFIG index_snapshot.max_concurrent_loads = 4",
        help: "index loads at open running at once, costliest first; 0 is half the cores",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "load_wait_ms",
        hot: true,
        set_with: "SET CONFIG index_snapshot.load_wait_ms = 600",
        help: "how long a query waits for an index load at open before it scans",
    },
    ConfigKey {
        section: "index_snapshot",
        key: "audit_rate",
        hot: true,
        set_with: "SET CONFIG index_snapshot.audit_rate = 64",
        help: "1 in this many index loads is checked against a copy read from rows in the background; 0 checks none",
    },
];

fn config_keys() -> Vec<Row> {
    CONFIG_KEYS
        .iter()
        .map(|k| {
            row(&[
                Cell::Text(k.section.to_string()),
                Cell::Text(k.key.to_string()),
                Cell::Text(String::new()),
                Cell::Flag(k.hot),
                Cell::Text(k.set_with.to_string()),
                Cell::Text(k.help.to_string()),
            ])
        })
        .collect()
}

fn config(database: &Arc<Database>) -> Vec<Row> {
    let filters = crate::filter_settings();
    let resolved = filters.resolve("", "");
    let replication = database.replication.config();

    let value_of = |section: &str, key: &str| -> String {
        match (section, key) {
            ("server", "batch_size") => crate::batch_size().to_string(),
            ("server", "concurrent_threads") => crate::max_permits_threads().to_string(),
            ("wal", "enabled") => crate::wal_enabled().to_string(),
            ("wal", "checkpoint_bytes") => crate::wal_checkpoint_bytes().to_string(),
            // `on`/`off`, not `true`/`false`: it carries a PostgreSQL setting's
            // name, its help and `set_with` both spell it that way, and it is
            // what someone types back into a console's edit box.
            ("wal", "synchronous_commit") => if crate::synchronous_commit() {
                "on"
            } else {
                "off"
            }
            .to_string(),
            ("wal", "writer_delay_ms") => crate::wal_writer_delay_ms().to_string(),
            ("cache", "enabled") => crate::cache::query_cache::enabled().to_string(),
            ("filter", "enabled") => filters.enabled().to_string(),
            ("filter", "backfill_on_open") => filters.backfill_on_open().to_string(),
            ("filter", "min_pruning_ratio") => filters.min_pruning_ratio().to_string(),
            ("filter", "checkpoint_interval_secs") => {
                filters.checkpoint_interval_secs().to_string()
            }
            ("filter", "min_f") => resolved.cfg.min_f.to_string(),
            ("filter", "default_f") => resolved.cfg.default_f.to_string(),
            ("filter", "max_f") => resolved.cfg.max_f.to_string(),
            ("filter", "min_selectivity") => resolved.cfg.min_selectivity.to_string(),
            ("filter", "min_table_rows") => resolved.cfg.min_table_rows.to_string(),
            ("filter", "zone_bounds") => resolved.zone_bounds.to_string(),
            ("filter", "bloom") => resolved.bloom.to_string(),
            ("replication", "active") => database.replication.is_active().to_string(),
            ("replication", "compression") => format!("{:?}", replication.compression),
            ("replication", "mode") => format!("{:?}", replication.sync_mode),
            ("replication", "conflict") => format!("{:?}", replication.conflict),
            ("replication", "ack_timeout_ms") => replication.ack_timeout.as_millis().to_string(),
            ("replication", "batch_max_records") => replication.batch_max_records.to_string(),
            ("replication", "batch_linger_us") => replication.batch_linger.as_micros().to_string(),
            // Reclamation is per table, and the whole point of reporting it
            // here is that a console can turn it off for a database. It reads
            // as on unless every table has it off.
            #[cfg(feature = "query_stats")]
            ("query_stats", "enabled") => if crate::core::query_stats::enabled() {
                "on"
            } else {
                "off"
            }
            .to_string(),
            #[cfg(feature = "query_stats")]
            ("query_stats", "recent") => crate::core::query_stats::capacity().to_string(),
            #[cfg(not(feature = "query_stats"))]
            ("query_stats", _) => "not built".to_string(),
            ("vacuum", "enabled") => "per table".to_string(),
            ("index_snapshot", "idle_checkpoint_secs") => {
                crate::core::index_store::seal::settings::idle_checkpoint_secs().to_string()
            }
            ("index_snapshot", "seal_timeout_ms") => {
                crate::core::index_store::seal::settings::seal_timeout().as_millis().to_string()
            }
            ("index_snapshot", "shutdown_deadline_secs") => {
                crate::core::index_store::seal::settings::shutdown_deadline().as_secs().to_string()
            }
            ("index_snapshot", "writeback_mb") => crate::core::index_store::seal::settings::writeback_mb().to_string(),
            ("index_snapshot", "index_storage") => {
                crate::core::index_store::load::settings::index_storage().name().to_string()
            }
            ("index_snapshot", "max_concurrent_loads") => {
                crate::core::index_store::load::settings::max_concurrent_loads().to_string()
            }
            ("index_snapshot", "load_wait_ms") => {
                crate::core::index_store::load::settings::load_wait().as_millis().to_string()
            }
            ("index_snapshot", "audit_rate") => crate::core::index_store::audit::settings::audit_rate().to_string(),
            _ => String::new(),
        }
    };

    CONFIG_KEYS
        .iter()
        .map(|k| {
            row(&[
                Cell::Text(k.section.to_string()),
                Cell::Text(k.key.to_string()),
                Cell::Text(value_of(k.section, k.key)),
                Cell::Flag(k.hot),
                Cell::Text(k.set_with.to_string()),
                Cell::Text(k.help.to_string()),
            ])
        })
        .collect()
}

/// Changes one knob, where the engine can do it without a restart.
///
/// Every hot key is delegated to the subsystem that already owns it — the
/// filter surface, the replication surface, the tables themselves — rather than
/// reimplemented. That is what keeps `SET CONFIG filter.max_f = 20` and
/// `SET FILTER max_f = 20` from drifting into meaning different things.
async fn set_config(database: &Arc<Database>, key: &str, value: &str) -> Result<String, String> {
    let (section, name) = key.split_once('.').ok_or_else(|| {
        format!(
            "`{}` is not a config key; they are `section.key`, and SHOW CONFIG KEYS lists them",
            key
        )
    })?;

    let spec = CONFIG_KEYS
        .iter()
        .find(|k| k.section == section && k.key == name)
        .ok_or_else(|| format!("unknown config key `{}` (try SHOW CONFIG KEYS)", key))?;

    if !spec.hot {
        return Err(format!(
            "`{}` is read once at start-up and cannot be changed on a running server; set it with `{}`",
            key, spec.set_with
        ));
    }

    match section {
        "wal" => match name {
            "synchronous_commit" => {
                let on = crate::parse_synchronous_commit(value).ok_or_else(|| {
                    format!(
                        "`{}` is not a value for synchronous_commit (use on/off)",
                        value
                    )
                })?;
                crate::set_synchronous_commit(on);
                // Turning it back on says nothing about what was committed while
                // it was off. Flushing that backlog here makes the guarantee
                // start from this statement rather than from the writer's next
                // round, which is what an operator turning it on expects.
                if on {
                    crate::core::log_writer::flush_now();
                }
                Ok(format!(
                    "synchronous_commit is now {}{}",
                    if on { "on" } else { "off" },
                    if on {
                        "; everything committed so far is durable"
                    } else {
                        ""
                    }
                ))
            }
            "writer_delay_ms" => {
                let ms: u64 = value
                    .trim()
                    .parse()
                    .map_err(|_| format!("`{}` is not a number of milliseconds", value))?;
                crate::set_wal_writer_delay_ms(ms);
                Ok(format!(
                    "an asynchronous commit is now at most {} ms from the disk",
                    crate::wal_writer_delay_ms()
                ))
            }
            _ => Err(format!("`{}` has no live setter", key)),
        },
        "filter" => {
            let lines =
                crate::core::filter::sql::execute(&format!("SET FILTER {} = {}", name, value))?;
            Ok(lines.join("; "))
        }
        "replication" => {
            if name == "active" {
                let statement = if parse_bool(value)? {
                    "REPLICATION ENABLE"
                } else {
                    "REPLICATION DISABLE"
                };
                let lines = crate::replication::sql::execute(statement, database).await?;
                return Ok(lines.join("; "));
            }
            let lines = crate::replication::sql::execute(
                &format!("REPLICATION SET {} = '{}'", name, value),
                database,
            )
            .await?;
            Ok(lines.join("; "))
        }
        "query_stats" => {
            #[cfg(not(feature = "query_stats"))]
            {
                let _ = value;
                Err(NO_QUERY_STATS.to_string())
            }
            #[cfg(feature = "query_stats")]
            match name {
                "enabled" => {
                    let on = parse_bool(value)?;
                    crate::core::query_stats::set_enabled(on);
                    // Turning it off is not the same as forgetting: what was
                    // recorded stays readable, which is what someone turning it
                    // off *because* of what they just saw would expect.
                    Ok(format!(
                        "statement statistics are now {}{}",
                        if on { "on" } else { "off" },
                        if on {
                            ""
                        } else {
                            "; what was already recorded is still readable"
                        }
                    ))
                }
                "recent" => {
                    let n: usize = value
                        .trim()
                        .parse()
                        .map_err(|_| format!("`{}` is not a number of statements", value))?;
                    crate::core::query_stats::set_capacity(n);
                    Ok(format!(
                        "SHOW QUERIES now keeps {} statement{}",
                        crate::core::query_stats::capacity(),
                        if crate::core::query_stats::capacity() == 1 {
                            ""
                        } else {
                            "s"
                        }
                    ))
                }
                _ => Err(format!("`{}` has no live setter", key)),
            }
        }
        "index_snapshot" => {
            use crate::core::index_store::seal::settings;
            if name == "index_storage" {
                use crate::core::index_store::load::settings::{IndexStorage, set_index_storage};
                let s = IndexStorage::parse(value)
                    .ok_or_else(|| format!("`{}` is not rebuild or snapshot for {}", value, key))?;
                set_index_storage(s);
                return Ok(format!("{key} is now {}", s.name()));
            }
            let n: u64 = value
                .trim()
                .trim_matches('\'')
                .parse()
                .map_err(|_| format!("`{}` is not a whole number for {}", value, key))?;
            match name {
                "idle_checkpoint_secs" => settings::set_idle_checkpoint_secs(n),
                "seal_timeout_ms" => settings::set_seal_timeout_ms(n),
                "shutdown_deadline_secs" => settings::set_shutdown_deadline_secs(n),
                "writeback_mb" => settings::set_writeback_mb(n),
                "max_concurrent_loads" => crate::core::index_store::load::settings::set_max_concurrent_loads(n),
                "load_wait_ms" => crate::core::index_store::load::settings::set_load_wait_ms(n),
                "audit_rate" => crate::core::index_store::audit::settings::set_audit_rate(n),
                _ => return Err(format!("`{}` has no live setter", key)),
            }
            Ok(format!("{key} is now {n}"))
        }
        "vacuum" => {
            let enabled = parse_bool(value)?;
            // Every target is found before any is changed: a table that cannot
            // serve data refuses the whole statement rather than leave it
            // reported applied to rows it never reached. A partitioned table's
            // targets are its materialized partitions; one materialized later
            // takes the server default, as a table created later does.
            let names = table_names(database);
            let mut targets: Vec<Arc<DbTable>> = Vec::new();
            let mut touched = 0usize;
            let mut partitions = 0usize;
            for name in names {
                match database.relation(&name) {
                    None => continue,
                    Some(Relation::Plain(table)) => {
                        table.data().map_err(|e| e.to_string())?;
                        targets.push(table);
                    }
                    Some(Relation::Partitioned(table)) => {
                        let epoch = table.epoch().map_err(|e| e.to_string())?;
                        for (_, part) in table
                            .materialized(&epoch, &crate::core::partition::Route::All)
                            .map_err(|e| e.to_string())?
                        {
                            targets.push(part);
                            partitions += 1;
                        }
                    }
                }
                touched += 1;
            }
            for table in &targets {
                table.set_vacuum_enabled(enabled).map_err(|e| e.to_string())?;
            }
            Ok(format!(
                "background reclamation {} for {} table{}{}",
                if enabled { "enabled" } else { "disabled" },
                touched,
                if touched == 1 { "" } else { "s" },
                if partitions > 0 {
                    format!(" ({} partition{})", partitions, if partitions == 1 { "" } else { "s" })
                } else {
                    String::new()
                }
            ))
        }
        _ => Err(format!("`{}` has no live setter", key)),
    }
}

/// A boolean as `SET CONFIG` takes one; also what `--query-stats` parses.
pub fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" | "t" => Ok(true),
        "false" | "off" | "no" | "0" | "f" => Ok(false),
        other => Err(format!(
            "`{}` is not a boolean (use on/off, true/false, 1/0)",
            other
        )),
    }
}

// ---------------------------------------------------------------------------
// Naming and encoding
// ---------------------------------------------------------------------------

/// The SQL type name a column would be declared with.
///
/// Fuller than `json_map::sql_type_name`, which only has to name the types JSON
/// inference produces; a console shows whatever the schema actually holds.
pub fn sql_type_name(db_type: &DbType) -> &'static str {
    match db_type {
        DbType::JSONB => "JSONB",
        DbType::I8 => "TINYINT",
        DbType::I16 => "SMALLINT",
        DbType::I32 => "INTEGER",
        DbType::I64 => "BIGINT",
        DbType::I128 => "HUGEINT",
        DbType::U8 => "UTINYINT",
        DbType::U16 => "USMALLINT",
        DbType::U32 => "UINTEGER",
        DbType::U64 => "UBIGINT",
        DbType::U128 => "UHUGEINT",
        DbType::F32 => "REAL",
        DbType::F64 => "DOUBLE PRECISION",
        DbType::BOOL => "BOOLEAN",
        DbType::CHAR => "CHAR",
        DbType::STRING => "TEXT",
        DbType::DATETIME => "TIMESTAMPTZ",
        DbType::UUID => "UUID",
        DbType::INET => "INET",
        DbType::BYTEA => "BYTEA",
        DbType::ARRAY => "ARRAY",
        DbType::DECIMAL => "NUMERIC",
        DbType::NULL => "NULL",
        _ => "UNKNOWN",
    }
}

fn action_name(action: &crate::core::rql::foreign_key::ReferentialAction) -> &'static str {
    use crate::core::rql::foreign_key::ReferentialAction::*;
    match action {
        NoAction => "NO ACTION",
        Restrict => "RESTRICT",
        Cascade => "CASCADE",
        SetNull => "SET NULL",
        SetDefault => "SET DEFAULT",
    }
}

// ---------------------------------------------------------------------------
// Query statistics
// ---------------------------------------------------------------------------

/// Why there is nothing to report, when the binary cannot report it.
///
/// An error rather than an empty result set, for the same reason `SHOW BACKUP
/// STATUS` says so: "nothing has run yet" and "this build does not record what
/// runs" want two different things done about them.
#[cfg(not(feature = "query_stats"))]
const NO_QUERY_STATS: &str = "this server was built without the `query_stats` feature, so it records nothing about      what it runs; rebuild with `--features query_stats`";

#[cfg(not(feature = "query_stats"))]
fn recent_queries(_limit: Option<u64>) -> Result<Vec<Row>, String> {
    Err(NO_QUERY_STATS.to_string())
}

#[cfg(not(feature = "query_stats"))]
fn query_stats() -> Result<Vec<Row>, String> {
    Err(NO_QUERY_STATS.to_string())
}

#[cfg(not(feature = "query_stats"))]
fn reset_query_stats() -> Result<String, String> {
    Err(NO_QUERY_STATS.to_string())
}

/// The last statements this server answered, newest first.
///
/// The text is what the client sent, minus comments and minus anything after a
/// `PASSWORD` keyword. It is **not** filtered by who is asking: any session
/// that can run this sees every session's statements, which is the same
/// exposure `SHOW CONFIG` already carries and the reason
/// [`crate::core::query_stats`] redacts before storing rather than before
/// printing.
#[cfg(feature = "query_stats")]
fn recent_queries(limit: Option<u64>) -> Result<Vec<Row>, String> {
    let limit = limit.map(|n| n as usize).unwrap_or(usize::MAX);
    Ok(crate::core::query_stats::recent(limit)
        .into_iter()
        .map(|record| {
            let status = if record.error.is_some() {
                "error"
            } else {
                "ok"
            };
            row(&[
                Cell::Text(crate::pg_params::format_timestamp_us(
                    record.started_unix_us,
                )),
                Cell::Real(record.duration_us as f64 / 1_000.0),
                Cell::Number(record.rows),
                Cell::Number(record.rows_scanned),
                Cell::Number(record.materialised),
                Cell::Real(record.materialised_us as f64 / 1_000.0),
                Cell::Text(status.to_string()),
                Cell::Text(record.database),
                Cell::Text(record.username),
                Cell::Text(record.sql),
                Cell::Text(record.error.unwrap_or_default()),
            ])
        })
        .collect())
}

/// One row per statement shape, the most expensive in total first.
#[cfg(feature = "query_stats")]
fn query_stats() -> Result<Vec<Row>, String> {
    Ok(crate::core::query_stats::aggregated()
        .into_iter()
        .map(|agg| {
            row(&[
                // The hash uses the whole 64-bit range, so it is reported
                // signed — as PostgreSQL reports `queryid` — rather than as a
                // count that happens to be enormous.
                Cell::Signed(agg.queryid as i64),
                Cell::Number(agg.calls),
                Cell::Real(agg.total_us as f64 / 1_000.0),
                Cell::Real(agg.mean_us() / 1_000.0),
                Cell::Real(agg.min_us as f64 / 1_000.0),
                Cell::Real(agg.max_us as f64 / 1_000.0),
                Cell::Real(agg.stddev_us() / 1_000.0),
                Cell::Number(agg.rows),
                Cell::Number(agg.rows_scanned),
                Cell::Number(agg.materialised),
                Cell::Real(agg.materialised_us as f64 / 1_000.0),
                Cell::Number(agg.errors),
                Cell::Text(crate::pg_params::format_timestamp_us(agg.last_seen_unix_us)),
                Cell::Text(agg.query),
            ])
        })
        .collect())
}

#[cfg(feature = "query_stats")]
fn reset_query_stats() -> Result<String, String> {
    let (recent, shapes) = crate::core::query_stats::reset();
    Ok(format!(
        "forgot {} recent statement{} and {} statement shape{}",
        recent,
        if recent == 1 { "" } else { "s" },
        shapes,
        if shapes == 1 { "" } else { "s" }
    ))
}

/// Two of these are only ever constructed by the statement-statistics rows, so
/// a build without that feature has them declared and unused.
#[cfg_attr(not(feature = "query_stats"), allow(dead_code))]
enum Cell {
    Text(String),
    Number(u64),
    Flag(bool),
    /// A duration in milliseconds. Rendered rather than rounded to whole
    /// microseconds: a statement that took 340 µs reads as `0.34`, and a
    /// console that only ever saw `0` could not tell it from an instant one.
    Real(f64),
    /// A signed 64-bit integer, for the one figure here that is a hash rather
    /// than a count and so uses the whole range including the negative half.
    Signed(i64),
}

fn row(cells: &[Cell]) -> Row {
    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            Cell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            Cell::Number(n) => (
                crate::core::value_codec::count_as_bigint(*n).to_le_bytes().to_vec(),
                DbType::I64,
            ),
            Cell::Flag(b) => (vec![u8::from(*b)], DbType::BOOL),
            Cell::Real(v) => (v.to_le_bytes().to_vec(), DbType::F64),
            Cell::Signed(v) => (v.to_le_bytes().to_vec(), DbType::I64),
        };
        let mut block = MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(Column {
            schema_id: crate::core::row::row::schema_id_of(index as u64),
            data: block,
            column_type,
        });
    }

    let length = columns.iter().map(|c| c.data.len()).sum::<usize>();
    Row {
        position: 0,
        columns,
        #[cfg(feature = "enable_long_row")]
        length: length as u64,
        #[cfg(not(feature = "enable_long_row"))]
        length: length as u32,
    }
}

/// `part` as a percentage of `whole`, and zero rather than a division by it.
fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 * 100.0 / whole as f64
}

fn rows(rows: Vec<Row>) -> Vec<u8> {
    let mut out = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut out);
    }
    out
}

fn failure(message: &str) -> Vec<u8> {
    let mut out = vec![3u8];
    out.extend_from_slice(message.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> Introspect {
        parse(sql).expect("recognised").expect("parsed")
    }

    #[test]
    fn the_statements_parse() {
        assert_eq!(ok("SHOW TABLES"), Introspect::Tables);
        assert_eq!(ok("show tables;"), Introspect::Tables);
        assert_eq!(ok("SHOW STATS"), Introspect::DatabaseStats);
        assert_eq!(ok("SHOW DATABASE STATS"), Introspect::DatabaseStats);
        assert_eq!(ok("SHOW SCHEMA MAP"), Introspect::SchemaMap);
        assert_eq!(ok("SHOW CONFIG"), Introspect::Config);
        assert_eq!(ok("SHOW CONFIG KEYS"), Introspect::ConfigKeys);
        assert_eq!(ok("SHOW BACKUP STATUS"), Introspect::BackupStatus);
        assert_eq!(ok("show backup status;"), Introspect::BackupStatus);
        assert_eq!(
            ok("SHOW TABLE STATS users"),
            Introspect::TableStats("users".into())
        );
        assert_eq!(
            ok("SHOW COLUMNS FROM users"),
            Introspect::Columns("users".into())
        );
        assert_eq!(
            ok("SHOW COLUMNS users"),
            Introspect::Columns("users".into())
        );
        assert_eq!(ok("DESCRIBE users"), Introspect::Columns("users".into()));
        assert_eq!(ok("DESC users"), Introspect::Columns("users".into()));
        assert_eq!(ok("SHOW QUERIES"), Introspect::RecentQueries(None));
        assert_eq!(ok("show queries;"), Introspect::RecentQueries(None));
        assert_eq!(ok("SHOW QUERIES 20"), Introspect::RecentQueries(Some(20)));
        assert_eq!(ok("SHOW QUERY STATS"), Introspect::QueryStats);
        assert_eq!(ok("RESET QUERY STATS"), Introspect::ResetQueryStats);
        assert_eq!(ok("reset query stats;"), Introspect::ResetQueryStats);
    }

    /// A statement that starts like one of these and then goes wrong says which
    /// clause was unusable, rather than falling through to "unsupported query"
    /// — which would send `SHOW QUERIES x` to the engine to be parsed as SQL.
    #[test]
    fn the_statistics_statements_report_their_own_mistakes() {
        let err = |sql: &str| parse(sql).expect("recognised").expect_err("rejected");
        assert!(err("SHOW QUERIES x").contains("row count"));
        assert!(err("SHOW QUERIES 10 20").contains("at most a row count"));
        assert!(err("SHOW QUERY STATS extra").contains("no arguments"));
        assert!(err("RESET QUERY").contains("RESET QUERY STATS"));
        // `RESET` of anything else is not this surface's business: the wire
        // protocol answers `RESET search_path` for itself, and claiming it here
        // would turn a parameter reset into an error.
        assert!(parse("RESET search_path").is_none());
        assert!(parse("RESET ALL").is_none());
    }

    /// A console quotes and schema-qualifies every identifier, so the name
    /// arrives as `"public"."users"` rather than `users`.
    #[test]
    fn a_quoted_and_qualified_name_resolves_to_the_table() {
        assert_eq!(
            ok("SHOW TABLE STATS \"users\""),
            Introspect::TableStats("users".into())
        );
        assert_eq!(
            ok("SHOW COLUMNS FROM \"public\".\"users\""),
            Introspect::Columns("users".into())
        );
    }

    #[test]
    fn set_config_keeps_the_value_case_and_strips_quotes() {
        assert_eq!(
            ok("SET CONFIG replication.mode = 'quorum:1'"),
            Introspect::SetConfig {
                key: "replication.mode".into(),
                value: "quorum:1".into()
            }
        );
        assert_eq!(
            ok("SET CONFIG filter.max_f = 20"),
            Introspect::SetConfig {
                key: "filter.max_f".into(),
                value: "20".into()
            }
        );
    }

    /// Three other surfaces own a longer `SHOW` phrase. Reading one of them as
    /// a `SHOW` of something called `FILTER` would take the statement away from
    /// the module that can answer it.
    #[test]
    fn the_other_show_surfaces_are_left_alone() {
        for sql in [
            "SHOW FILTER",
            "SHOW FILTER KEYS",
            "SHOW REPLICATION",
            "SHOW REPLICATION PEERS",
            "SHOW BACKUPS",
            "SHOW DATABASES",
            "SELECT * FROM users",
            "SET FILTER max_f = 20",
        ] {
            assert!(
                parse(sql).is_none(),
                "{:?} should not be an introspection statement",
                sql
            );
        }
    }

    /// A statement that starts like one of these and then goes wrong is
    /// reported as that statement being wrong, not as an unknown query.
    #[test]
    fn a_malformed_statement_says_what_was_wrong() {
        assert!(parse("SHOW TABLE STATS").unwrap().is_err());
        assert!(parse("SHOW COLUMNS").unwrap().is_err());
        assert!(parse("SHOW TABLES extra").unwrap().is_err());
        assert!(parse("SET CONFIG filter.max_f").unwrap().is_err());
        assert!(parse("SHOW CONFIG NONSENSE").unwrap().is_err());
        assert!(parse("SHOW BACKUP STATUS extra").unwrap().is_err());
    }

    /// Every key `SHOW CONFIG` reports has to be one `SET CONFIG` can look up,
    /// or a console would offer a setting that then fails to resolve.
    #[test]
    fn every_config_key_is_addressable() {
        for key in CONFIG_KEYS {
            let qualified = format!("{}.{}", key.section, key.key);
            assert!(
                CONFIG_KEYS
                    .iter()
                    .any(|k| format!("{}.{}", k.section, k.key) == qualified),
                "{} is not addressable",
                qualified
            );
            assert!(
                !key.set_with.is_empty(),
                "{} says nothing about how to change it",
                qualified
            );
        }
    }

    /// The metric names `SHOW DATABASE STATS` reports, in order.
    async fn metrics(database: &Arc<Database>) -> Vec<String> {
        database_stats(database)
            .await
            .expect("database stats")
            .iter()
            .map(|row| {
                String::from_utf8(row.columns[0].data.into_slice().to_vec())
                    .expect("a metric name is text")
            })
            .collect()
    }

    /// Settings live on `SHOW CONFIG`, which is also the only place they can be
    /// changed. Reported here as well they are most of the list, they never
    /// move, and the two surfaces drift the moment one of them is edited.
    ///
    /// Written against `CONFIG_KEYS` rather than against a list of names, so a
    /// knob added to the configuration surface tomorrow is covered without
    /// anybody remembering this test exists. That is the point of it: adding a
    /// setting here would be a reasonable-looking one-line change, and nothing
    /// about the result would look wrong afterwards.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_statistics_report_no_settings_and_no_internal_ids() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);

        let dir = tempfile::tempdir().expect("tempdir");
        let database = Arc::new(Database::new(dir.path().to_str().expect("a UTF-8 path")).await);
        let metrics = metrics(&database).await;

        for key in CONFIG_KEYS {
            // Both spellings a knob could take here: `wal.enabled` was
            // `wal_enabled`, and `server.batch_size` was the bare `batch_size`.
            for spelling in [format!("{}_{}", key.section, key.key), key.key.to_string()] {
                assert!(
                    !metrics.contains(&spelling),
                    "`{}` is a SHOW CONFIG knob and is reported as a statistic",
                    spelling
                );
            }
        }

        for internal in ["database_id", "replication_node_id"] {
            assert!(
                !metrics.contains(&internal.to_string()),
                "`{}` names this process to the engine; no client can act on it",
                internal
            );
        }

        // What a console reads by name. Losing one of these silently empties a
        // tile on the front page.
        for kept in ["name", "tables", "columns", "row_slots", "total_bytes"] {
            assert!(
                metrics.contains(&kept.to_string()),
                "`{}` went missing",
                kept
            );
        }

        // A single-directory database reports what it always did. The pool
        // manager lists the root among its locations too, so without the
        // de-duplication the root is counted twice and every value gains a
        // path prefix.
        assert_eq!(durability_locations(&database).len(), 1);
        assert!(
            !metrics.iter().any(|m| m.starts_with("wal_location:")),
            "a single-directory database grew per-location rows: {:?}",
            metrics
        );
    }

    /// The count has to be the set `SHOW TABLES` lists, or a console's front
    /// page and its table list disagree about how many tables exist.
    #[tokio::test(flavor = "multi_thread")]
    async fn internal_relations_are_not_counted_as_tables() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);

        let dir = tempfile::tempdir().expect("tempdir");
        let database = Arc::new(Database::new(dir.path().to_str().expect("a UTF-8 path")).await);

        for sql in [
            "CREATE TABLE orders (id INT, amount INT)",
            // The spelling every internal relation uses: a JSON column's
            // shadow table, a CTE's temporary one, a materialised catalogue
            // view.
            "CREATE TABLE __json_orders_doc (id INT, k VARCHAR)",
        ] {
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
                .unwrap_or_else(|| panic!("unrecognised: {sql}"));
            let response = crate::core::rql::executor::execute(purpose, database.clone()).await;
            assert_ne!(response.first(), Some(&3u8), "{sql} failed");
        }

        let stats = database_stats(&database).await.expect("database stats");
        let value_of = |metric: &str| -> String {
            let index = stats
                .iter()
                .position(|row| row.columns[0].data.into_slice() == metric.as_bytes())
                .unwrap_or_else(|| panic!("no `{}` row", metric));
            String::from_utf8(stats[index].columns[1].data.into_slice().to_vec())
                .expect("a value is text")
        };

        assert_eq!(value_of("tables"), "1", "the shadow table was counted");
        assert!(
            database.table_count() >= 2,
            "the fixture did not actually create the shadow table"
        );
    }

    /// A poisoned log has to show up in `SHOW STATS`, with its reason and the
    /// checkpoint it broke; until it did, the one place it was visible was a
    /// log line from the moment it happened. `/dev/null` fails `fdatasync`.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_poisoned_log_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();
        let metric = |name: &str| {
            durability_metrics(std::slice::from_ref(&location))
                .into_iter()
                .find(|(m, _, _)| *m == name)
                .map(|(_, v, _)| v)
                .unwrap_or_else(|| panic!("{} is not reported", name))
        };

        let wal = std::sync::Arc::new(
            crate::core::wal::Wal::open(dir.path().join(crate::core::wal::DEFAULT_WAL_NAME))
                .unwrap(),
        );
        crate::core::wal::install_for(&location, wal.clone());
        assert_eq!(metric("wal_poisoned"), "false");
        assert_eq!(metric("data_flush_failure"), "");

        // A checkpoint syncs the files records name, so one names it.
        wal.register_file(7, "/dev/null").expect("register the file");
        wal.log(wal.begin(), 7, 0, b"x").unwrap();
        assert!(wal.checkpoint().is_err());

        let poisoned = metric("wal_poisoned");
        let reason = metric("wal_poison_reason");
        let failures = metric("wal_checkpoint_failures");
        let last_error = metric("wal_last_checkpoint_error");
        let detail = durability_metrics(std::slice::from_ref(&location))
            .into_iter()
            .find(|(m, _, _)| m == "wal_poisoned")
            .map(|(_, _, d)| d)
            .expect("wal_poisoned is reported");
        crate::core::wal::remove_for(&location);

        assert_eq!(poisoned, "true");
        assert!(reason.contains("/dev/null"), "the reason names the file: {:?}", reason);
        assert_eq!(failures, "1");
        assert!(!last_error.is_empty());
        // One directory, one log: here a poisoned log does stop every commit.
        assert_eq!(detail, "a flush, apply or log write failed; every commit is refused until restart");
    }

    /// Forgets the logs and flush failures a test registered for these
    /// directories, on panic too: both registries are process-global, and a
    /// poisoned entry left behind describes a deleted directory to every test
    /// that runs after.
    struct Registered(Vec<String>);

    impl Drop for Registered {
        fn drop(&mut self) {
            for location in &self.0 {
                crate::core::wal::remove_for(location);
                crate::core::storage_providers::file_sync::forget_directory_failure(location);
            }
        }
    }

    /// Every location's trouble reaches the totals. Each count is summed, not
    /// overwritten or maxed, and each text value collects every location,
    /// labelled with it — a total taken from one location reads healthy while
    /// another disk refuses commits, which is the bug these metrics fix.
    ///
    /// root: poisoned, one decline and a recorded flush failure (poison wins);
    /// `poisoned`: poisoned; `flushed`: two declines and a flush failure;
    /// `bare`: no log at all.
    #[cfg(target_os = "linux")]
    #[test]
    fn every_location_is_summed_and_labelled() {
        let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
        let paths: Vec<String> =
            dirs.iter().map(|d| d.path().to_str().unwrap().to_string()).collect();
        let _registered = Registered(paths.clone());
        let (root, poisoned, flushed, bare) = (&paths[0], &paths[1], &paths[2], &paths[3]);

        let install = |path: &String| {
            let wal = std::sync::Arc::new(
                crate::core::wal::Wal::open(
                    std::path::Path::new(path).join(crate::core::wal::DEFAULT_WAL_NAME),
                )
                .unwrap(),
            );
            crate::core::wal::install_for(path, wal.clone());
            wal
        };
        // `/dev/null` fails `fdatasync`, and a checkpoint syncs what records name.
        let poison = |wal: &crate::core::wal::Wal| {
            wal.register_file(0xDEAD_BEEF, "/dev/null").expect("register the file");
            wal.log(wal.begin(), 0xDEAD_BEEF, 0, b"x").unwrap();
            assert!(wal.checkpoint().is_err());
        };
        // A pending overwrite holds the log, so a checkpoint declines.
        let decline = |wal: &crate::core::wal::Wal, path: &String| {
            let data = std::path::Path::new(path).join("t.db");
            std::fs::write(&data, b"").unwrap();
            let id = crate::core::wal::file_id_for_name("t.db");
            wal.register_file(id, &data).unwrap();
            let txn = wal.begin();
            wal.log_in_place(txn, id, 0, b"held").unwrap();
            assert!(!wal.checkpoint().unwrap(), "the checkpoint was not declined");
            wal.abort_txn(txn);
        };

        let root_wal = install(root);
        decline(&root_wal, root);
        poison(&root_wal);
        poison(&install(poisoned));
        let flushed_wal = install(flushed);
        decline(&flushed_wal, flushed);
        decline(&flushed_wal, flushed);
        use crate::core::storage_providers::file_sync::record_directory_failure;
        record_directory_failure(root, "root flush".to_string());
        record_directory_failure(flushed, "flushed flush".to_string());

        let metrics = durability_metrics(&paths);
        let value_of = |name: &str| {
            metrics
                .iter()
                .find(|(m, _, _)| m == name)
                .map(|(_, v, _)| v.clone())
                .unwrap_or_else(|| panic!("{} is not reported", name))
        };

        assert_eq!(value_of("wal_poisoned"), "true");
        assert_eq!(value_of("wal_checkpoints_declined"), "3");
        assert_eq!(value_of("wal_checkpoint_failures"), "2");

        let reason = value_of("wal_poison_reason");
        let parts: Vec<&str> = reason.split("; ").collect();
        assert_eq!(parts.len(), 2, "one reason per poisoned location: {:?}", reason);
        assert!(parts[0].starts_with(&format!("{}: ", root)), "{:?}", reason);
        assert!(parts[1].starts_with(&format!("{}: ", poisoned)), "{:?}", reason);

        let errors = value_of("wal_last_checkpoint_error");
        assert!(errors.starts_with(&format!("{}: ", root)), "{:?}", errors);
        assert!(errors.contains(&format!("; {}: ", poisoned)), "{:?}", errors);

        assert_eq!(
            value_of("data_flush_failure"),
            format!("{}: root flush; {}: flushed flush", root, flushed)
        );

        assert_eq!(value_of(&format!("wal_location:{}", root)), "poisoned");
        assert_eq!(value_of(&format!("wal_location:{}", poisoned)), "poisoned");
        assert_eq!(value_of(&format!("wal_location:{}", flushed)), "data flush failed");
        assert_eq!(value_of(&format!("wal_location:{}", bare)), "no log");

        // Poison is scoped to a location's log; the summary must not claim the
        // whole database stopped.
        let detail = metrics
            .iter()
            .find(|(m, _, _)| m == "wal_poisoned")
            .map(|(_, _, d)| d.clone())
            .unwrap();
        assert!(!detail.contains("every commit"), "{:?}", detail);
    }

    /// A database with no pool reports its root once. The pool manager seeds
    /// its locations with the root, so without the de-duplication one root
    /// failure is counted twice and every value gains a path prefix — a wrong
    /// number on every database, with nothing looking wrong.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_single_directory_database_reports_its_root_once() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().to_str().unwrap().to_string();
        let database = Arc::new(Database::new(&root_path).await);
        let _registered = Registered(vec![root_path.clone()]);

        let value_of = |stats: &[Row], metric: &str| -> String {
            let index = stats
                .iter()
                .position(|row| row.columns[0].data.into_slice() == metric.as_bytes())
                .unwrap_or_else(|| panic!("no `{}` row", metric));
            String::from_utf8(stats[index].columns[1].data.into_slice().to_vec())
                .expect("a value is text")
        };

        let wal = crate::core::wal::wal_for(&root_path).expect("the root has a log");
        wal.register_file(0xDEAD_BEEF, "/dev/null").expect("register the file");
        wal.log(wal.begin(), 0xDEAD_BEEF, 0, b"x").unwrap();
        assert!(wal.checkpoint().is_err());

        let stats = database_stats(&database).await.expect("database stats");
        let names: Vec<String> = stats
            .iter()
            .map(|row| String::from_utf8(row.columns[0].data.into_slice().to_vec()).unwrap())
            .collect();
        assert!(
            !names.iter().any(|m| m.starts_with("wal_location:")),
            "a single-directory database grew per-location rows: {:?}",
            names
        );
        assert_eq!(value_of(&stats, "wal_poisoned"), "true");
        assert_eq!(value_of(&stats, "wal_checkpoint_failures"), "1", "the root was counted twice");
        let reason = value_of(&stats, "wal_poison_reason");
        assert!(
            reason.contains("/dev/null") && !reason.contains(&format!("{}: ", root_path)),
            "a single-directory reason is unlabelled: {:?}",
            reason
        );
    }

    /// A storage location has a log of its own, and a commit that touched a
    /// table there is refused when that log is poisoned. `SHOW STATS` read only
    /// the root's, so it reported `wal_poisoned = false` for a database whose
    /// second disk refused every write.
    #[cfg(all(target_os = "linux", feature = "storage_pools"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_poisoned_log_on_a_second_location_is_reported() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);

        let root = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::new(root.path().to_str().unwrap()).await);

        let run = |sql: String| {
            let database = database.clone();
            async move {
                let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
                    .unwrap_or_else(|| panic!("unrecognised: {sql}"));
                let response = crate::core::rql::executor::execute(purpose, database).await;
                assert_ne!(response.first(), Some(&3u8), "{sql} failed");
            }
        };
        let second_path = second.path().to_str().unwrap().to_string();
        run(format!("STORAGE ADD LOCATION '{}'", second_path)).await;
        run("CREATE TABLE remote (id INT, label VARCHAR)".to_string()).await;
        run(format!("STORAGE MOVE TABLE remote TO '{}'", second_path)).await;
        for id in 0..8 {
            run(format!("INSERT INTO remote (id, label) VALUES ({id}, 'row {id}')")).await;
        }
        database.sync_durable().expect("sync");

        let located = database
            .storage
            .location_of("remote")
            .expect("the table was placed on the second location");
        assert!(
            std::path::Path::new(&located).join("remote.db").exists(),
            "the writes did not land in {located}"
        );
        let _registered = Registered(vec![root.path().to_str().unwrap().to_string(), located.clone()]);
        let wal = crate::core::wal::wal_for(&located).expect("the second location has a log");
        assert!(
            crate::core::wal::wal_for(root.path().to_str().unwrap()).is_some(),
            "the root has a log of its own"
        );
        // The root once, however the pool manager spells it.
        assert_eq!(durability_locations(&database).len(), 2);

        let stats = || async {
            let stats = database_stats(&database).await.expect("database stats");
            move |metric: &str| -> String {
                let index = stats
                    .iter()
                    .position(|row| row.columns[0].data.into_slice() == metric.as_bytes())
                    .unwrap_or_else(|| panic!("no `{}` row", metric));
                String::from_utf8(stats[index].columns[1].data.into_slice().to_vec())
                    .expect("a value is text")
            }
        };
        let location_row = format!("wal_location:{}", located);
        let healthy = stats().await;
        assert_eq!(healthy("wal_poisoned"), "false");
        assert_eq!(healthy(&location_row), "ok");

        // Poisons only the second location's log; the root's stays healthy.
        // A checkpoint syncs the files records name, and `/dev/null` fails
        // `fdatasync`.
        wal.register_file(0xDEAD_BEEF, "/dev/null").expect("register the file");
        wal.log(wal.begin(), 0xDEAD_BEEF, 0, b"x").unwrap();
        assert!(wal.checkpoint().is_err());

        let value_of = stats().await;
        let poisoned = value_of("wal_poisoned");
        let reason = value_of("wal_poison_reason");
        let failures = value_of("wal_checkpoint_failures");
        let second_state = value_of(&location_row);
        let root_state = value_of(&format!(
            "wal_location:{}",
            crate::core::storage::normalize(root.path().to_str().unwrap())
        ));

        assert_eq!(poisoned, "true", "a poisoned pool log was reported healthy");
        assert!(
            reason.starts_with(&format!("{}: ", located)) && reason.contains("/dev/null"),
            "the reason names the location and the file: {:?}",
            reason
        );
        assert_eq!(failures, "1");
        assert_eq!(second_state, "poisoned");
        assert_eq!(root_state, "ok");
    }

    /// Result metadata is sent before the rows, so a mismatch is a protocol
    /// error rather than a wrong-looking column.
    #[test]
    fn every_statement_describes_its_own_columns() {
        for statement in [
            Introspect::DatabaseStats,
            Introspect::Tables,
            Introspect::TableStats("t".into()),
            Introspect::Columns("t".into()),
            Introspect::SchemaMap,
            Introspect::BackupStatus,
            Introspect::Config,
            Introspect::ConfigKeys,
            Introspect::SetConfig {
                key: "k".into(),
                value: "v".into(),
            },
            Introspect::RecentQueries(None),
            Introspect::QueryStats,
            Introspect::ResetQueryStats,
        ] {
            let fields = result_fields(&statement).expect("described");
            assert!(!fields.is_empty(), "{:?} describes no columns", statement);
        }
    }
}
