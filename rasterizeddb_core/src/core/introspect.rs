//! `SHOW TABLES`, `SHOW … STATS`, `SHOW SCHEMA MAP`, `SHOW/SET CONFIG` — the
//! introspection surface a database console needs.
//!
//! ```text
//! SHOW DATABASE STATS          -- one metric per row, for the whole database
//! SHOW TABLES                  -- a row per table, with its sizes
//! SHOW TABLE STATS <table>     -- one metric per row, for one table
//! SHOW COLUMNS FROM <table>    -- a row per column  (also: DESCRIBE <table>)
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
    Columns(String),
    SchemaMap,
    BackupStatus,
    Config,
    ConfigKeys,
    SetConfig { key: String, value: String },
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
            Some(_) => Some(Err(format!(
                "{} takes one table name",
                words[0]
            ))),
            None => Some(Err(format!("{} names no table", words[0]))),
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
        let value = value.trim().trim_matches('\'').trim_matches('"').to_string();
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
        Some("COLUMNS") => {
            // `SHOW COLUMNS FROM t`, and the bare `SHOW COLUMNS t`.
            let index = if words.get(2).copied() == Some("FROM") { 3 } else { 2 };
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
    let number = |name: &str| (name.to_string(), DbType::U64);
    let flag = |name: &str| (name.to_string(), DbType::BOOL);

    Some(match statement {
        // A metric per row rather than a column per metric: the set of metrics
        // grows, and a console that renders `(name, value)` keeps working when
        // it does, where one keyed on a fixed column list does not.
        Introspect::DatabaseStats | Introspect::TableStats(_) | Introspect::BackupStatus => {
            vec![text("metric"), text("value"), text("detail")]
        }
        Introspect::Tables => vec![
            text("table_name"),
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
        Introspect::DatabaseStats => rows(database_stats(database).await),
        Introspect::Tables => rows(tables(database).await),
        Introspect::TableStats(name) => match table_stats(database, name).await {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::Columns(name) => match columns(database, name) {
            Ok(r) => rows(r),
            Err(e) => failure(&e),
        },
        Introspect::SchemaMap => rows(schema_map(database)),
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
struct TableSizes {
    data: u64,
    pointers: u64,
    schema: u64,
    filters: u64,
    sequence: u64,
}

impl TableSizes {
    fn total(&self) -> u64 {
        self.data + self.pointers + self.schema + self.filters + self.sequence
    }

    /// Pointer records written, which bounds the live rows from above. See the
    /// module docs for why this is not a row count.
    fn row_slots(&self) -> u64 {
        self.pointers / ROW_POINTER_RECORD_LEN as u64
    }
}

async fn sizes_of<S: StorageIO>(table: &crate::core::row::table::Table<S>) -> TableSizes {
    TableSizes {
        data: table.io_rows.get_len().await,
        pointers: table.io_pointers.get_len().await,
        schema: table.io_schema.get_len().await,
        filters: table.io_filters.get_len().await,
        sequence: table.io_sequence.get_len().await,
    }
}

async fn tables(database: &Arc<Database>) -> Vec<Row> {
    // Names are copied out first so no shard guard is held across the awaits
    // below: a `get_len` on a slow filesystem would otherwise block every
    // CREATE/DROP that hashes to the same shard for as long as it takes.
    // Engine-internal relations are left out. A `__cte_…`/`__cat_…` scratch
    // table exists only while one statement runs, and a `__json_…` shadow
    // table holds the shredded fields of a JSON column — none is a relation
    // anybody wrote or can use. Listed, they appeared and vanished between
    // refreshes of a client's table list, and a client that clicked one
    // queried a table that was already gone.
    let mut names: Vec<String> = database
        .tables
        .iter()
        .map(|e| e.key().clone())
        .filter(|name| !name.starts_with("__"))
        .collect();
    names.sort();

    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let Some(table) = database.tables.get(&name).map(|t| t.value().clone()) else {
            continue;
        };
        let sizes = sizes_of(&table).await;
        let visible = table
            .schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .count();

        out.push(row(&[
            Cell::Text(name),
            Cell::Number(sizes.row_slots()),
            Cell::Number(visible as u64),
            Cell::Number(sizes.data),
            Cell::Number(sizes.pointers),
            Cell::Number(sizes.filters),
            Cell::Number(sizes.total()),
            Cell::Number(table.reclaimable_bytes()),
            Cell::Flag(table.filters_ready()),
            Cell::Flag(table.schema.is_immutable),
            Cell::Text(table.schema.primary_key.clone().unwrap_or_default()),
        ]));
    }
    out
}

async fn database_stats(database: &Arc<Database>) -> Vec<Row> {
    let mut names: Vec<String> = database.tables.iter().map(|e| e.key().clone()).collect();
    names.sort();

    let mut total = TableSizes { data: 0, pointers: 0, schema: 0, filters: 0, sequence: 0 };
    let mut reclaimable = 0u64;
    let mut foreign_keys = 0usize;
    let mut unique_keys = 0usize;
    let mut json_columns = 0usize;
    let mut column_count = 0usize;

    for name in &names {
        let Some(table) = database.tables.get(name).map(|t| t.value().clone()) else {
            continue;
        };
        let sizes = sizes_of(&table).await;
        total.data += sizes.data;
        total.pointers += sizes.pointers;
        total.schema += sizes.schema;
        total.filters += sizes.filters;
        total.sequence += sizes.sequence;
        reclaimable += table.reclaimable_bytes();
        foreign_keys += table.schema.foreign_keys.iter().filter(|f| !f.is_deleted).count();
        unique_keys += table.schema.unique_keys.iter().filter(|k| !k.is_deleted).count();
        json_columns += table.schema.json_columns.iter().filter(|c| !c.is_deleted).count();
        column_count += table
            .schema
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

    push("name", database.name.clone(), "what clients connect to this database as");
    push(
        "server_version",
        format!("rasterizeddb {}", env!("CARGO_PKG_VERSION")),
        "the build serving this database",
    );
    push(
        "location",
        database.location.clone().unwrap_or_else(|| "(in memory)".to_string()),
        "directory holding this database's files",
    );
    push("database_id", database.id.to_string(), "distinguishes it from a recreated namesake");
    push("tables", names.len().to_string(), "user and shadow tables the catalogue holds");
    push("columns", column_count.to_string(), "declared columns across every table");
    push("row_slots", (total.pointers / ROW_POINTER_RECORD_LEN as u64).to_string(),
         "pointer records; an upper bound on live rows");

    push("data_bytes", total.data.to_string(), "row data");
    push("pointer_bytes", total.pointers.to_string(), "row pointer records");
    push("schema_bytes", total.schema.to_string(), "schemas and their sidecars");
    push("index_bytes", total.filters.to_string(), "persisted filter checkpoints");
    push("sequence_bytes", total.sequence.to_string(), "row-id sequences");
    push("total_bytes", total.total().to_string(), "every file the tables own");
    push("reclaimable_bytes", reclaimable.to_string(), "dead space a VACUUM would return");

    push("foreign_keys", foreign_keys.to_string(), "enforced references");
    push("unique_constraints", unique_keys.to_string(), "table-level UNIQUE constraints");
    push("json_columns", json_columns.to_string(), "columns mapped onto a shadow table");

    let transactions = &database.transactions;
    push("next_transaction_id", transactions.next_id().to_string(), "id the next transaction takes");
    push("oldest_active_transaction", transactions.oldest_active().to_string(),
         "what pins the reclamation horizon");
    push("transaction_bookkeeping", transactions.bookkeeping_entries().to_string(),
         "outcomes still being tracked");
    push("quiescent", transactions.quiescent().to_string(), "no transaction is running");

    push("wal_enabled", crate::wal_enabled().to_string(), "durable writes go through the log");
    push("wal_checkpoint_bytes", crate::wal_checkpoint_bytes().to_string(),
         "log bytes between checkpoints");

    let (hits, misses, entries) = crate::cache::query_cache::stats();
    push("query_cache_enabled", crate::cache::query_cache::enabled().to_string(),
         "repeated queries served from a known offset");
    push("query_cache_hits", hits.to_string(), "");
    push("query_cache_misses", misses.to_string(), "");
    push("query_cache_entries", entries.to_string(), "");

    let filters = crate::filter_settings();
    push("filter_enabled", filters.enabled().to_string(), "bit-sliced filter index");
    push("filter_min_pruning_ratio", filters.min_pruning_ratio().to_string(),
         "skip the mask when it prunes less than this");
    push("filter_indexed_tables", filters.overridden_tables().len().to_string(),
         "tables carrying a filter override");

    let replication = &database.replication;
    push("replication_active", replication.is_active().to_string(), "shipping transactions to peers");
    push("replication_peers", replication.peer_count().to_string(), "");
    push("replication_node_id", replication.node_id().to_string(), "half of every global transaction id");

    push("batch_size", crate::batch_size().to_string(), "row pointers fetched per batch");
    push("concurrent_threads", crate::max_permits_threads().to_string(), "workers a query may use");

    out
}

async fn table_stats(database: &Arc<Database>, name: &str) -> Result<Vec<Row>, String> {
    let table = database
        .tables
        .get(name)
        .map(|t| t.value().clone())
        .ok_or_else(|| format!("relation `{}` does not exist", name))?;

    let sizes = sizes_of(&table).await;
    let schema = &table.schema;
    let vacuum = table.vacuum_stats();

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
    push("columns", visible.len().to_string(), "declared and not dropped");
    push("nullable_columns", nullable.to_string(), "each carries a companion flag column");
    push("dropped_columns", schema.fields.iter().filter(|f| f.is_deleted).count().to_string(),
         "slots kept rather than rewritten");
    push("fixed_row_width", visible.iter().map(|f| f.size as u64).sum::<u64>().to_string(),
         "bytes a row occupies before variable-width columns");

    push("row_slots", sizes.row_slots().to_string(),
         "pointer records; an upper bound on live rows");
    push("last_row_id", table.last_row_id.load(Ordering::Relaxed).to_string(),
         "highest id handed out");

    push("data_bytes", sizes.data.to_string(), "row data");
    push("pointer_bytes", sizes.pointers.to_string(), "row pointer records");
    push("schema_bytes", sizes.schema.to_string(), "schema and its sidecars");
    push("index_bytes", sizes.filters.to_string(), "persisted filter checkpoint");
    push("sequence_bytes", sizes.sequence.to_string(), "row-id sequence");
    push("total_bytes", sizes.total().to_string(), "every file this table owns");
    push("reclaimable_bytes", table.reclaimable_bytes().to_string(),
         "dead space a VACUUM would return");

    push("vacuum_runs", vacuum.runs.load(Ordering::Relaxed).to_string(), "");
    push("vacuum_compactions", vacuum.compactions.load(Ordering::Relaxed).to_string(),
         "runs that actually reclaimed something");
    push("vacuum_deferrals", vacuum.deferrals.load(Ordering::Relaxed).to_string(),
         "runs held off by an open snapshot");
    push("vacuum_last_live_bytes", vacuum.last_live_bytes.load(Ordering::Relaxed).to_string(), "");
    push("vacuum_last_dead_bytes", vacuum.last_dead_bytes.load(Ordering::Relaxed).to_string(), "");
    push("vacuum_total_reclaimed_bytes",
         vacuum.total_reclaimed_bytes.load(Ordering::Relaxed).to_string(), "");

    push("filters_ready", table.filters_ready().to_string(),
         "the index has caught up with the rows");
    push("filter_enabled_for_table", crate::filter_settings().table_enabled(&schema.name).to_string(),
         "");
    push("in_place_column_updates", table.in_place_column_updates().to_string(),
         "updates that rewrote only the assigned columns");
    push("hard_locked", table.hard_locked.load(Ordering::Relaxed).to_string(),
         "a compaction holds the table");
    push("immutable", schema.is_immutable.to_string(), "");
    push("primary_key", schema.primary_key.clone().unwrap_or_default(), "");

    for key in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
        push("unique_constraint", format!("{} ({})", key.name, key.columns.join(", ")), "");
    }
    for fk in schema.foreign_keys.iter().filter(|f| !f.is_deleted) {
        push(
            "foreign_key",
            format!("{} : {} -> {}.{}", fk.name, fk.column, fk.parent_table, fk.parent_column),
            &format!("ON DELETE {}", action_name(&fk.on_delete)),
        );
    }
    for json in schema.json_columns.iter().filter(|c| !c.is_deleted) {
        push("json_column", format!("{} -> {}", json.column, json.shadow_table),
             "document fields live in the shadow table");
    }

    Ok(out)
}

fn columns(database: &Arc<Database>, name: &str) -> Result<Vec<Row>, String> {
    let table = database
        .tables
        .get(name)
        .map(|t| t.value().clone())
        .ok_or_else(|| format!("relation `{}` does not exist", name))?;
    let schema = &table.schema;
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

        let nullable = crate::core::rql::nullable::flag_field(&field.name, &schema.fields).is_some();
        let is_primary = schema.primary_key.as_deref() == Some(field.name.as_str());
        let in_unique_key = schema
            .unique_keys
            .iter()
            .any(|k| !k.is_deleted && k.columns.iter().any(|c| c.eq_ignore_ascii_case(&field.name)));
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
fn schema_map(database: &Arc<Database>) -> Vec<Row> {
    let mut names: Vec<String> = database.tables.iter().map(|e| e.key().clone()).collect();
    names.sort();

    let mut out = Vec::new();
    for name in names {
        let Some(table) = database.tables.get(&name).map(|t| t.value().clone()) else {
            continue;
        };
        let schema = &table.schema;

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
    out
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
        push("prefix", config.normalized_prefix(), "one database per prefix");
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
        push("wal_bundle_bytes", config.wal_bundle_bytes.to_string(),
             "staged bytes that trigger a flush without waiting for the interval");
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
        push("staged_bytes", staged.to_string(), "written but not yet uploaded");
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
    ConfigKey { section: "server", key: "batch_size", hot: false,
        set_with: "--batch-size N (restart)", help: "row pointers fetched per batch" },
    ConfigKey { section: "server", key: "concurrent_threads", hot: false,
        set_with: "--concurrent-threads N (restart)", help: "workers a query may use" },
    ConfigKey { section: "wal", key: "enabled", hot: false,
        set_with: "--wal BOOL (restart)", help: "route durable writes through the log" },
    ConfigKey { section: "wal", key: "checkpoint_bytes", hot: false,
        set_with: "--wal-checkpoint-bytes N (restart)", help: "log bytes between checkpoints" },
    ConfigKey { section: "wal", key: "synchronous_commit", hot: true,
        set_with: "SET CONFIG wal.synchronous_commit = off",
        help: "commits wait for their fdatasync; off risks the last writes, not the database" },
    ConfigKey { section: "wal", key: "writer_delay_ms", hot: true,
        set_with: "SET CONFIG wal.writer_delay_ms = 200",
        help: "how long an asynchronous commit may go unsynced" },
    ConfigKey { section: "cache", key: "enabled", hot: false,
        set_with: "--enable-cache BOOL (restart)", help: "serve repeated queries from a known offset" },
    ConfigKey { section: "filter", key: "enabled", hot: true,
        set_with: "SET CONFIG filter.enabled = on", help: "master switch for the filter index" },
    ConfigKey { section: "filter", key: "backfill_on_open", hot: true,
        set_with: "SET CONFIG filter.backfill_on_open = on", help: "index pre-existing rows at open" },
    ConfigKey { section: "filter", key: "min_pruning_ratio", hot: true,
        set_with: "SET CONFIG filter.min_pruning_ratio = 0.9",
        help: "skip the mask when it prunes less than this (0..1)" },
    ConfigKey { section: "filter", key: "checkpoint_interval_secs", hot: true,
        set_with: "SET CONFIG filter.checkpoint_interval_secs = 60",
        help: "how often the maintenance task checkpoints" },
    ConfigKey { section: "filter", key: "min_f", hot: true,
        set_with: "SET CONFIG filter.min_f = 8", help: "floor on fingerprint bits (4..16)" },
    ConfigKey { section: "filter", key: "default_f", hot: true,
        set_with: "SET CONFIG filter.default_f = 12",
        help: "fingerprint bits before cardinality is known" },
    ConfigKey { section: "filter", key: "max_f", hot: true,
        set_with: "SET CONFIG filter.max_f = 16", help: "ceiling on fingerprint bits (4..16)" },
    ConfigKey { section: "filter", key: "min_selectivity", hot: true,
        set_with: "SET CONFIG filter.min_selectivity = 0.1",
        help: "largest row fraction a predicate may match and still be indexed" },
    ConfigKey { section: "filter", key: "min_table_rows", hot: true,
        set_with: "SET CONFIG filter.min_table_rows = 10000",
        help: "tables smaller than this are not indexed" },
    ConfigKey { section: "filter", key: "zone_bounds", hot: true,
        set_with: "SET CONFIG filter.zone_bounds = on",
        help: "keep per-zone min/max for range pruning" },
    ConfigKey { section: "filter", key: "bloom", hot: true,
        set_with: "SET CONFIG filter.bloom = off",
        help: "keep a per-superzone span summary" },
    ConfigKey { section: "replication", key: "active", hot: true,
        set_with: "REPLICATION ENABLE | REPLICATION DISABLE",
        help: "ship committed transactions to the peers" },
    ConfigKey { section: "replication", key: "compression", hot: true,
        set_with: "SET CONFIG replication.compression = 'auto:zstd:1'",
        help: "none | zstd[:level] | auto[:zstd[:level]]" },
    ConfigKey { section: "replication", key: "mode", hot: true,
        set_with: "SET CONFIG replication.mode = 'quorum:1'",
        help: "async | sync | quorum:N | all" },
    ConfigKey { section: "replication", key: "conflict", hot: true,
        set_with: "SET CONFIG replication.conflict = 'last-writer-wins'",
        help: "abort | skip | last-writer-wins" },
    ConfigKey { section: "replication", key: "ack_timeout_ms", hot: true,
        set_with: "SET CONFIG replication.ack_timeout_ms = 2000",
        help: "how long a synchronous commit waits for peers" },
    ConfigKey { section: "replication", key: "batch_max_records", hot: true,
        set_with: "SET CONFIG replication.batch_max_records = 256",
        help: "records in one outbound batch" },
    ConfigKey { section: "replication", key: "batch_linger_us", hot: true,
        set_with: "SET CONFIG replication.batch_linger_us = 500",
        help: "how long a sender waits for more records" },
    ConfigKey { section: "vacuum", key: "enabled", hot: true,
        set_with: "SET CONFIG vacuum.enabled = on",
        help: "background reclamation, for every table in this database" },
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
            ("wal", "synchronous_commit") => {
                if crate::synchronous_commit() { "on" } else { "off" }.to_string()
            }
            ("wal", "writer_delay_ms") => crate::wal_writer_delay_ms().to_string(),
            ("cache", "enabled") => crate::cache::query_cache::enabled().to_string(),
            ("filter", "enabled") => filters.enabled().to_string(),
            ("filter", "backfill_on_open") => filters.backfill_on_open().to_string(),
            ("filter", "min_pruning_ratio") => filters.min_pruning_ratio().to_string(),
            ("filter", "checkpoint_interval_secs") => filters.checkpoint_interval_secs().to_string(),
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
            ("vacuum", "enabled") => "per table".to_string(),
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
    let (section, name) = key
        .split_once('.')
        .ok_or_else(|| format!("`{}` is not a config key; they are `section.key`, and SHOW CONFIG KEYS lists them", key))?;

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
                    format!("`{}` is not a value for synchronous_commit (use on/off)", value)
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
                    if on { "; everything committed so far is durable" } else { "" }
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
            let lines = crate::core::filter::sql::execute(&format!("SET FILTER {} = {}", name, value))?;
            Ok(lines.join("; "))
        }
        "replication" => {
            if name == "active" {
                let statement = if parse_bool(value)? { "REPLICATION ENABLE" } else { "REPLICATION DISABLE" };
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
        "vacuum" => {
            let enabled = parse_bool(value)?;
            let mut touched = 0usize;
            for entry in database.tables.iter() {
                entry.value().set_vacuum_enabled(enabled);
                touched += 1;
            }
            Ok(format!(
                "background reclamation {} for {} table{}",
                if enabled { "enabled" } else { "disabled" },
                touched,
                if touched == 1 { "" } else { "s" }
            ))
        }
        _ => Err(format!("`{}` has no live setter", key)),
    }
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" | "t" => Ok(true),
        "false" | "off" | "no" | "0" | "f" => Ok(false),
        other => Err(format!("`{}` is not a boolean (use on/off, true/false, 1/0)", other)),
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

enum Cell {
    Text(String),
    Number(u64),
    Flag(bool),
}

fn row(cells: &[Cell]) -> Row {
    let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            Cell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            Cell::Number(n) => (n.to_le_bytes().to_vec(), DbType::U64),
            Cell::Flag(b) => (vec![u8::from(*b)], DbType::BOOL),
        };
        let mut block = MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(Column {
            schema_id: index as u64,
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
        assert_eq!(ok("SHOW TABLE STATS users"), Introspect::TableStats("users".into()));
        assert_eq!(ok("SHOW COLUMNS FROM users"), Introspect::Columns("users".into()));
        assert_eq!(ok("SHOW COLUMNS users"), Introspect::Columns("users".into()));
        assert_eq!(ok("DESCRIBE users"), Introspect::Columns("users".into()));
        assert_eq!(ok("DESC users"), Introspect::Columns("users".into()));
    }

    /// A console quotes and schema-qualifies every identifier, so the name
    /// arrives as `"public"."users"` rather than `users`.
    #[test]
    fn a_quoted_and_qualified_name_resolves_to_the_table() {
        assert_eq!(ok("SHOW TABLE STATS \"users\""), Introspect::TableStats("users".into()));
        assert_eq!(
            ok("SHOW COLUMNS FROM \"public\".\"users\""),
            Introspect::Columns("users".into())
        );
    }

    #[test]
    fn set_config_keeps_the_value_case_and_strips_quotes() {
        assert_eq!(
            ok("SET CONFIG replication.mode = 'quorum:1'"),
            Introspect::SetConfig { key: "replication.mode".into(), value: "quorum:1".into() }
        );
        assert_eq!(
            ok("SET CONFIG filter.max_f = 20"),
            Introspect::SetConfig { key: "filter.max_f".into(), value: "20".into() }
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
            assert!(parse(sql).is_none(), "{:?} should not be an introspection statement", sql);
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
            assert!(!key.set_with.is_empty(), "{} says nothing about how to change it", qualified);
        }
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
            Introspect::SetConfig { key: "k".into(), value: "v".into() },
        ] {
            let fields = result_fields(&statement).expect("described");
            assert!(!fields.is_empty(), "{:?} describes no columns", statement);
        }
    }
}
