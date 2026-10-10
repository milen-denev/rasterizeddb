//! Running an import.
//!
//! The order is the whole design:
//!
//! 1. Connect, and pin the settings that decide how PostgreSQL renders a value
//!    into text — encoding, date style, time zone, float digits, `bytea`
//!    output. Everything after this converts *text*, so an unpinned setting
//!    would be a silently different import on a differently-configured server.
//! 2. Read the catalogue and plan. Both are pure reads; nothing exists on this
//!    side yet, so a schema this engine cannot hold costs nothing to discover.
//! 3. Create the target database and every table, keys included, then drop the
//!    keys. See [`super::plan`] for why they come off before the data.
//! 4. Stream each table in with `COPY`, a batch of rows to a transaction.
//! 5. Put the keys back — which validates every row in one pass — and add the
//!    foreign keys.
//! 6. Make it all durable, once.
//!
//! A failure at any point after step 3 drops the target database. A database
//! that holds some of a table is indistinguishable, once the process has
//! exited, from one that holds all of it; the only safe thing to leave behind
//! is nothing.
//!
//! That last fact is what step 6 rests on. Nothing between steps 3 and 6 is
//! promised to anyone, so nothing in between is made durable: the target
//! directory is registered with [`crate::core::bulk_load`], under which an
//! append is a `write` and no more — no log record, no flush — and a batch's
//! commit is an in-memory outcome that no one waits on. A row costs the two
//! writes it takes to exist. The price is paid in full at step 6: every file
//! synced, every outcome flushed, and only then is the import reported done.
//! A crash before that leaves exactly what a crash always left — a half-built
//! database to drop — having spent far less getting there.

use std::time::{Duration, Instant};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::database::catalog::DatabaseCatalog;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::copy;
use super::pg_client::{ConnectParams, PgConnection};
use super::plan::{ImportOptions, Plan, TablePlan, plan};
use super::source::read_schema;
use super::types;

/// What one table cost.
#[derive(Debug, Clone)]
pub struct TableReport {
    pub name: String,
    /// Rows copied. On a dry run this is the source's estimate instead, and
    /// `None` when the source has no estimate — `reltuples` is -1 on a table
    /// that has never been analysed, and reporting that as "0 rows" for a table
    /// about to yield a million is a report nobody would trust twice.
    pub rows: Option<u64>,
    pub elapsed: Duration,
    /// Keys put back after the data went in.
    pub constraints: usize,
    pub foreign_keys: usize,
}

/// What an import did.
#[derive(Debug, Clone)]
pub struct ImportReport {
    /// The source, with the password removed.
    pub source: String,
    pub target: String,
    pub server_version: String,
    pub tables: Vec<TableReport>,
    /// Everything the import could not carry over exactly.
    pub notes: Vec<String>,
    pub elapsed: Duration,
    pub dry_run: bool,
}

impl ImportReport {
    /// Rows copied across every table.
    pub fn rows(&self) -> u64 {
        self.tables.iter().filter_map(|t| t.rows).sum()
    }

    /// The report as lines of text, for a terminal.
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.push(format!(
            "{} {} from {} into {:?}",
            if self.dry_run {
                "Would import"
            } else {
                "Imported"
            },
            plural(self.tables.len() as u64, "table", "tables"),
            self.source,
            self.target
        ));
        if !self.server_version.is_empty() {
            out.push(format!(
                "  source server: PostgreSQL {}",
                self.server_version
            ));
        }
        for table in &self.tables {
            let count = match (table.rows, self.dry_run) {
                (Some(n), true) => format!("about {} rows", n),
                (Some(n), false) => format!("{} rows", n),
                (None, _) => "row count unknown (never analysed)".to_string(),
            };
            out.push(format!(
                "  {:<32} {:>34}  {:>8.1}s{}",
                table.name,
                count,
                table.elapsed.as_secs_f64(),
                match (table.constraints, table.foreign_keys) {
                    (0, 0) => String::new(),
                    (k, 0) => format!("  ({})", plural(k as u64, "key", "keys")),
                    (0, f) => format!("  ({})", plural(f as u64, "foreign key", "foreign keys")),
                    (k, f) => format!(
                        "  ({}, {})",
                        plural(k as u64, "key", "keys"),
                        plural(f as u64, "foreign key", "foreign keys")
                    ),
                }
            ));
        }
        out.push(if self.dry_run {
            format!("  about {} to copy", plural(self.rows(), "row", "rows"))
        } else {
            format!(
                "  {} in {:.1}s",
                plural(self.rows(), "row", "rows"),
                self.elapsed.as_secs_f64()
            )
        });
        if !self.notes.is_empty() {
            out.push(String::new());
            out.push(format!(
                "{} that did not come across unchanged:",
                plural(self.notes.len() as u64, "One thing", "Things")
            ));
            for note in &self.notes {
                out.push(format!("  - {}", note));
            }
        }
        out
    }
}

fn plural(n: u64, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{} {}", n, one)
    } else {
        format!("{} {}", n, many)
    }
}

/// Imports a PostgreSQL database into a new database in `catalog`.
pub async fn import(
    catalog: &Arc<DatabaseCatalog>,
    source: &ConnectParams,
    target: &str,
    options: &ImportOptions,
) -> Result<ImportReport, String> {
    let started = Instant::now();

    crate::core::database::catalog::validate_name(target)?;
    if catalog.get(target).is_some() {
        return Err(format!(
            "a database called {:?} is already here. An import builds a database from nothing, so \
             it will not write into one that exists — drop it first, or import under another name.",
            target
        ));
    }

    let mut connection = connect(source).await?;
    let server_version = connection
        .parameter("server_version")
        .unwrap_or_default()
        .to_string();

    log::info!(
        "Reading the schema of {} (PostgreSQL {})",
        connection.description(),
        server_version
    );

    let schema = read_schema(
        &mut connection,
        &options.schema,
        &options.include,
        &options.exclude,
    )
    .await?;

    let plan = plan(&schema, options)?;

    let mut report = ImportReport {
        source: connection.description().to_string(),
        target: target.to_string(),
        server_version,
        tables: Vec::with_capacity(plan.tables.len()),
        notes: plan.notes.clone(),
        elapsed: Duration::ZERO,
        dry_run: options.dry_run,
    };

    if options.dry_run {
        for table in &plan.tables {
            report.tables.push(TableReport {
                name: table.name.clone(),
                rows: table.estimated_rows.map(|n| n.max(0) as u64),
                elapsed: Duration::ZERO,
                constraints: table.add_constraints.len(),
                foreign_keys: table.foreign_keys.len(),
            });
            report.notes.extend(table.notes.iter().cloned());
        }
        report.elapsed = started.elapsed();
        connection.close().await;
        return Ok(report);
    }

    let database = catalog
        .create(target)
        .await
        .map_err(|e| format!("could not create the database {:?}: {}", target, e))?;

    // Closed to clients until every row is in and the source's keys are back.
    // A client writing into the gap is not merely early — its rows are written
    // while nothing constrains them, and the keys then cannot be restored. An
    // application recorded two page hits into a half-imported table and the
    // source's `UNIQUE (endpoint)` could no longer be added, which failed the
    // whole import after it had already copied a million rows.
    let _loading = catalog.begin_loading(target);

    // Everything written from here until `finish` is unlogged and unsynced.
    // The guard is held across the whole load, keys and foreign keys included,
    // and dropped before the final sync so that nothing appended after the
    // sync could be missed by it.
    let bulk = database
        .location
        .as_deref()
        .map(crate::core::bulk_load::begin);

    // From here on the target exists, so every failure has to take it away
    // again: half a database that looks like a whole one is the one outcome
    // worse than a failed import.
    let outcome = match load(&mut connection, &database, &plan, options, &mut report).await {
        Ok(()) => {
            drop(bulk);
            finish(&database).await
        }
        Err(e) => {
            drop(bulk);
            Err(e)
        }
    };
    match outcome {
        Ok(()) => {}
        Err(e) => {
            log::error!("The import failed: {}", e);
            let removed = catalog
                .drop_database(target, catalog.default_name())
                .await
                .is_ok();
            connection.close().await;
            return Err(format!(
                "{}\n\nThe half-built database {:?} {}.",
                e,
                target,
                if removed {
                    "was removed, so nothing is left that could be mistaken for a finished import"
                } else {
                    "could NOT be removed — drop it before trying again, because it holds only \
                     part of the source"
                }
            ));
        }
    }

    connection.close().await;
    report.elapsed = started.elapsed();
    Ok(report)
}

/// Opens the connection and pins everything that decides how a value is
/// rendered.
async fn connect(source: &ConnectParams) -> Result<PgConnection, String> {
    let mut connection = PgConnection::connect(source).await?;

    // `client_encoding`, `DateStyle`, `TimeZone` and `extra_float_digits` are
    // set in the start-up packet. `bytea_output` is not a start-up parameter,
    // so it is set here — and it matters: under the `escape` setting a `bytea`
    // arrives as octal escapes rather than hex, which the converter refuses
    // rather than storing as text.
    connection
        .simple_query("SET bytea_output = 'hex'")
        .await
        .map_err(|e| format!("could not set bytea_output on the source: {}", e))?;

    // A read-only, repeatable-read transaction would give every table the same
    // snapshot. It is deliberately not opened: it would hold one for the whole
    // import, which on a busy source means an hours-long transaction pinning
    // dead tuples. An import is a point-in-time copy per table, and a source
    // still taking writes should be quiesced by whoever is migrating it.
    Ok(connection)
}

async fn load(
    connection: &mut PgConnection,
    database: &Arc<Database>,
    plan: &Plan,
    options: &ImportOptions,
    report: &mut ImportReport,
) -> Result<(), String> {
    // ---- schema ----------------------------------------------------------

    for table in &plan.tables {
        log::info!("Creating {}", table.name);
        // Logged so a table that is created differently from how its rows are
        // built can be compared against them. A "schema mismatch" on row 1 says
        // the two disagree and nothing about how.
        log::debug!("import DDL: {}", table.create);
        run_ddl(database, &table.create).await?;
        for statement in &table.drop_constraints {
            run_ddl(database, statement).await?;
        }
        report.notes.extend(table.notes.iter().cloned());
    }

    // ---- data ------------------------------------------------------------

    for table in &plan.tables {
        let started = Instant::now();
        let rows = copy_table(connection, database, table, options).await?;
        report.tables.push(TableReport {
            name: table.name.clone(),
            rows: Some(rows),
            elapsed: started.elapsed(),
            constraints: 0,
            foreign_keys: 0,
        });
    }

    // ---- keys ------------------------------------------------------------

    for table in &plan.tables {
        for statement in &table.add_constraints {
            log::info!("{}", statement);
            run_ddl(database, statement).await.map_err(|e| {
                format!(
                    "{}\n\nThe rows are in, but a key the source enforced could not be put back. \
                     That means the source held values this constraint forbids, which should be \
                     impossible — or that the import changed one.",
                    e
                )
            })?;
            if let Some(entry) = report.tables.iter_mut().find(|t| t.name == table.name) {
                entry.constraints += 1;
            }
        }
    }

    // Foreign keys last of all: they point at other tables, which have to hold
    // their rows first.
    for table in &plan.tables {
        for statement in &table.foreign_keys {
            log::info!("{}", statement);
            run_ddl(database, statement).await?;
            if let Some(entry) = report.tables.iter_mut().find(|t| t.name == table.name) {
                entry.foreign_keys += 1;
            }
        }
    }

    Ok(())
}

/// Streams one table across.
async fn copy_table(
    connection: &mut PgConnection,
    database: &Arc<Database>,
    table: &TablePlan,
    options: &ImportOptions,
) -> Result<u64, String> {
    match table.estimated_rows {
        Some(n) => log::info!("Copying {} (about {} rows)", table.name, n),
        None => log::info!("Copying {}", table.name),
    }

    connection.copy_out(&table.copy).await?;

    let mut loader = TableLoader::new(database, table, options);
    loop {
        let line = match connection.copy_row().await {
            Ok(line) => line,
            Err(e) => {
                loader.abort();
                return Err(format!("reading {}: {}", table.source_qualified, e));
            }
        };
        let Some(line) = line else { break };
        if copy::is_terminator(&line) {
            continue;
        }
        loader.row(&line).await?;
    }
    loader.finish().await
}

/// Loads one table's `COPY` rows, a batch to a transaction.
///
/// Separate from the connection that feeds it so the load itself — the part
/// that decides what is stored — can be driven by a synthetic stream in tests
/// and benchmarks.
pub(crate) struct TableLoader<'a> {
    database: &'a Arc<Database>,
    table: &'a TablePlan,
    /// `INSERT INTO t (a, b, c) VALUES (` — the same for every row, so it is
    /// built once and the values appended to a buffer that is reused.
    prefix: String,
    statement: String,
    batch_rows: usize,
    copied: u64,
    in_batch: usize,
    batch_bytes: usize,
    transaction: Option<crate::core::mvcc::TxId>,
    last_logged: Instant,
    /// Write each row through [`executor::insert_loaded_row`] rather than as
    /// a statement. Off only in tests, as the reference the direct write is
    /// compared against.
    direct: bool,
    /// Rows the direct write took, as opposed to handing them back.
    direct_rows: u64,
}

impl<'a> TableLoader<'a> {
    pub(crate) fn new(
        database: &'a Arc<Database>,
        table: &'a TablePlan,
        options: &ImportOptions,
    ) -> Self {
        let prefix = format!(
            "INSERT INTO {} ({}) VALUES (",
            table.name,
            table
                .columns
                .iter()
                .map(|c| c.source.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        TableLoader {
            database,
            table,
            statement: String::with_capacity(prefix.len() + 256),
            prefix,
            batch_rows: options.batch_rows.max(1),
            copied: 0,
            in_batch: 0,
            batch_bytes: 0,
            transaction: None,
            last_logged: Instant::now(),
            direct: true,
            direct_rows: 0,
        }
    }

    /// Rolls back the open batch, if there is one. The import fails behind it.
    pub(crate) fn abort(&mut self) {
        if let Some(tx) = self.transaction.take() {
            abort(self.database, tx);
        }
    }

    /// Stores one `COPY` row. On an error the open batch is rolled back.
    pub(crate) async fn row(&mut self, line: &[u8]) -> Result<(), String> {
        let (database, table) = (self.database, self.table);
        let result = build_statement(table, &self.prefix, line, &mut self.statement);
        let Ok(()) = result else {
            self.abort();
            return Err(format!(
                "{} row {}: {}",
                table.source_qualified,
                self.copied + 1,
                result.unwrap_err()
            ));
        };

        let tx = match self.transaction {
            Some(tx) => tx,
            None => {
                let tx = database.transactions.begin();
                self.transaction = Some(tx);
                tx
            }
        };

        // The row as the statement would write it, minus the statement: no
        // classifying parse, no retry wrapper, no probes for shapes an import
        // never builds. Anything that path cannot do exactly — a JSON column, a
        // generated one, a refusal — comes back `None` and runs as the
        // statement below, which then has the last word.
        if self.direct
            && executor::insert_loaded_row(database, &table.name, &self.statement, tx)
                .await
                .is_some()
        {
            self.direct_rows += 1;
            self.note_row();
            return self.end_batch_if_full(tx).await;
        }

        let Some(purpose) = recognize_query_purpose(&self.statement) else {
            self.abort();
            return Err(format!(
                "{} row {}: the statement built for this row could not be parsed back",
                table.source_qualified,
                self.copied + 1
            ));
        };

        let response = executor::execute_in_txn(purpose, database.clone(), tx).await;
        if response.first() == Some(&3) {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            self.abort();
            // The statement is named, truncated. Some refusals — "Cannot insert
            // row due to schema mismatch" above all — say nothing about which
            // value the engine could not take, and without the text there is
            // nothing to reproduce from: the row is one of millions streaming
            // past and the copy has already been rolled back.
            const SHOWN: usize = 600;
            let statement = &self.statement;
            let excerpt: String = if statement.chars().count() > SHOWN {
                let head: String = statement.chars().take(SHOWN).collect();
                format!("{head}… [{} chars]", statement.chars().count())
            } else {
                statement.clone()
            };
            return Err(format!(
                "{} row {}: {}\n  statement: {}",
                table.source_qualified,
                self.copied + 1,
                message,
                excerpt
            ));
        }

        self.note_row();
        self.end_batch_if_full(tx).await
    }

    fn note_row(&mut self) {
        self.copied += 1;
        self.in_batch += 1;
        self.batch_bytes += self.statement.len();
    }

    async fn end_batch_if_full(&mut self, tx: crate::core::mvcc::TxId) -> Result<(), String> {
        let (database, table) = (self.database, self.table);
        // A batch ends on whichever limit it reaches first, which is what makes
        // the size adaptive: narrow rows reach the row count and batch deeply,
        // wide ones reach the byte budget and batch shallowly. A row count
        // alone is not a bound on anything that matters — 5,000 rows of a few
        // bytes and 5,000 rows carrying a megabyte of JSON each are four
        // orders of magnitude apart in what one open transaction has to hold.
        if self.in_batch >= self.batch_rows || self.batch_bytes >= MAX_BATCH_BYTES {
            self.transaction = None;
            self.in_batch = 0;
            self.batch_bytes = 0;
            commit(database, tx).await?;

            if self.last_logged.elapsed() >= Duration::from_secs(10) {
                log::info!("  {}: {} rows", table.name, self.copied);
                self.last_logged = Instant::now();
            }
        }
        Ok(())
    }

    /// Commits the last batch and reports how many rows went in.
    pub(crate) async fn finish(mut self) -> Result<u64, String> {
        if let Some(tx) = self.transaction.take() {
            commit(self.database, tx).await?;
        }
        log::info!("Copied {} rows into {}", self.copied, self.table.name);
        Ok(self.copied)
    }
}

/// How much one import batch may accumulate before it is committed.
///
/// The batch is also capped by a row count, and ends at whichever comes first.
/// This is the limit that matters for wide rows: it bounds what a single open
/// transaction holds regardless of how large the rows turn out to be, which a
/// row count cannot do because it is fixed before any row has been seen.
///
/// A batch no longer pays a sync of its own — see [`commit`] — so this bounds
/// only what one open transaction holds: its captured statements for
/// replication, and the row locks and versions a reader has to resolve
/// against. Large enough that the per-batch commit and publish are noise.
const MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Builds one row's `INSERT` into `statement`.
fn build_statement(
    table: &TablePlan,
    prefix: &str,
    line: &[u8],
    statement: &mut String,
) -> Result<(), String> {
    let values = copy::split_row(line)?;
    if values.len() != table.columns.len() {
        return Err(format!(
            "PostgreSQL sent {} values for {} columns",
            values.len(),
            table.columns.len()
        ));
    }

    statement.clear();
    statement.push_str(prefix);
    for (index, (column, value)) in table.columns.iter().zip(values.iter()).enumerate() {
        if index > 0 {
            statement.push_str(", ");
        }
        let literal = types::literal(&column.mapping, value.as_deref()).map_err(|e| {
            format!(
                "column {} ({}): {}",
                column.source.name, column.source.formatted_type, e
            )
        })?;
        statement.push_str(&literal);
    }
    statement.push(')');
    Ok(())
}

/// Runs one DDL statement, failing the import if it does not work.
async fn run_ddl(database: &Arc<Database>, sql: &str) -> Result<(), String> {
    let Some(purpose) = recognize_query_purpose(sql) else {
        return Err(format!(
            "this engine did not recognise the statement: {}",
            sql
        ));
    };
    let response = executor::execute(purpose, database.clone()).await;
    if response.first() == Some(&3) {
        return Err(format!(
            "{}\n  while running: {}",
            String::from_utf8_lossy(&response[1..]),
            sql
        ));
    }
    Ok(())
}

async fn commit(database: &Arc<Database>, tx: crate::core::mvcc::TxId) -> Result<(), String> {
    // Nothing here touches the device. The batch's rows were appended under
    // the bulk-load guard — no log record, no flush — and its outcome is
    // recorded in memory only; `finish` makes all of it durable at once when
    // the whole import has succeeded. A batch that is visible but not durable
    // is no hazard in a database nobody has been handed yet: a crash before
    // `finish` loses the import whole, exactly as it would have anyway.
    //
    // Publishing to peers is the one thing a batch still does, so a replica
    // keeps pace with the load rather than receiving it all at the end.
    let family = database.commit_transaction(tx);
    database.publish_transaction(family).await;
    Ok(())
}

/// Makes the finished import durable, in the order that survives a crash
/// between any two steps.
///
/// Files before outcomes: a crash after the files are on disk but before the
/// outcomes are leaves rows whose transactions read as unfinished — debris,
/// which is the right answer for an import that did not complete. The other
/// order could leave committed outcomes pointing at rows that never reached
/// the disk, and a restart would believe them.
async fn finish(database: &Arc<Database>) -> Result<(), String> {
    let Some(location) = database.location.as_deref() else {
        return Ok(()); // in memory: nothing to make durable
    };
    log::info!("Making the import durable");
    crate::core::bulk_load::sync_all(location)
        .map_err(|e| format!("the imported rows could not be made durable: {}", e))?;
    // Whatever went through the log rather than around it — the constraint
    // sidecars, schema writes — is covered by the ordinary path.
    database
        .sync_durable()
        .map_err(|e| format!("the import's metadata could not be made durable: {}", e))?;
    database
        .transactions
        .flush()
        .map_err(|e| format!("the import's transactions could not be made durable: {}", e))?;
    Ok(())
}

fn abort(database: &Arc<Database>, tx: crate::core::mvcc::TxId) {
    // The flush is skipped for the same reason `commit` skips it: an aborted
    // batch fails the import, and the database is dropped behind it.
    database.abort_transaction(tx);
}

/// The one column an `IMPORT` reports through.
///
/// A report is meant to be read: what mapped to what, what did not come across,
/// how many rows each table took. Splitting that into typed columns would make
/// it harder to read in a terminal and no easier to act on.
pub fn result_fields() -> Vec<(String, crate::core::db_type::DbType)> {
    vec![("import".to_string(), crate::core::db_type::DbType::STRING)]
}

#[cfg(test)]
mod load_tests {
    //! The load step driven by a synthetic `COPY` stream: no PostgreSQL needed.

    use super::*;
    use crate::core::migrate::source::{SourceSchema, SourceTable};
    use crate::core::migrate::types::SourceColumn;

    fn column(name: &str, type_name: &str, category: char, modifier: i32) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            attnum: 1,
            type_name: type_name.to_string(),
            type_kind: 'b',
            type_category: category,
            element_type_name: (category == 'A').then(|| "text".to_string()),
            type_modifier: modifier,
            not_null: name == "id",
            default: None,
            formatted_type: type_name.to_string(),
            json_shreddable: Some(true),
        }
    }

    /// One table holding every conversion the import has: integers, NUMERIC
    /// with a declared scale and without, floats, booleans, text with every
    /// character `COPY` escapes, timestamps and dates, UUID, INET, BYTEA,
    /// arrays, JSON/JSONB, and PostgreSQL types carried as text.
    fn plan_for(name: &str) -> TablePlan {
        // numeric(12,2): atttypmod = ((precision << 16) | scale) + 4.
        let numeric_12_2 = ((12 << 16) | 2) + 4;
        let columns = vec![
            column("id", "int4", 'N', -1),
            column("big", "int8", 'N', -1),
            column("small", "int2", 'N', -1),
            column("name", "text", 'S', -1),
            column("amount", "numeric", 'N', numeric_12_2),
            column("plain_num", "numeric", 'N', -1),
            column("ratio", "float8", 'N', -1),
            column("real4", "float4", 'N', -1),
            column("ok", "bool", 'B', -1),
            column("created", "timestamptz", 'D', -1),
            column("day", "date", 'D', -1),
            column("uid", "uuid", 'U', -1),
            column("addr", "inet", 'I', -1),
            column("blob", "bytea", 'U', -1),
            column("tags", "_text", 'A', -1),
            column("doc", "jsonb", 'U', -1),
            column("span", "interval", 'T', -1),
            column("mac", "macaddr", 'U', -1),
        ];
        let schema = SourceSchema {
            tables: vec![SourceTable {
                schema: "public".to_string(),
                name: name.to_string(),
                oid: 1,
                columns,
                primary_key: vec!["id".to_string()],
                unique_keys: Vec::new(),
                foreign_keys: Vec::new(),
                estimated_rows: None,
                notes: Vec::new(),
            }],
            notes: Vec::new(),
        };
        plan(&schema, &ImportOptions::default())
            .expect("plan")
            .tables
            .remove(0)
    }

    /// Row `i` as PostgreSQL's text `COPY` renders it, with NULLs, escapes and
    /// edge values spread across the rows.
    fn copy_line(i: u64) -> Vec<u8> {
        let null = |k: u64| (i + k) % 7 == 0;
        let f = |k: u64, v: String| if null(k) { "\\N".to_string() } else { v };
        let fields = [
            i.to_string(),
            f(1, format!("{}", (i as i64) * 1_000_003 - 7_000_000_000)),
            f(2, format!("{}", (i % 60_000) as i64 - 30_000)),
            f(3, match i % 5 {
                0 => format!("plain {i}"),
                1 => format!("tab\\there {i}"),
                2 => format!("quote ' and back\\\\slash {i}"),
                3 => format!("new\\nline é {i}"),
                _ => String::new(),
            }),
            f(4, format!("{}.{:02}", i as i64 - 500, i % 100)),
            f(5, format!("{}.{}", i % 1000, i % 7)),
            f(6, match i % 4 {
                0 => "NaN".to_string(),
                1 => "-Infinity".to_string(),
                2 => format!("{}", (i as f64) / 3.0),
                _ => "-0".to_string(),
            }),
            f(7, format!("{}", (i as f32) * 0.25)),
            f(8, if i % 2 == 0 { "t" } else { "f" }.to_string()),
            f(9, format!("2026-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00", i % 12 + 1, i % 28 + 1, i % 24, i % 60, (i * 7) % 60, i % 1_000_000)),
            f(10, format!("{}-{:02}-{:02}", 1990 + i % 40, i % 12 + 1, i % 28 + 1)),
            f(11, format!("{:08x}-0000-4000-8000-{:012x}", i, i * 31)),
            f(12, format!("10.{}.{}.{}", i % 256, (i / 256) % 256, i % 200)),
            f(13, format!("\\\\x{:02x}{:02x}00ff", i % 256, (i * 3) % 256)),
            f(14, format!("{{a,\"b c\",{i}}}")),
            f(15, format!("{{\"k\": {i}, \"s\": \"v{i}\", \"arr\": [1, 2]}}")),
            f(16, format!("{} days {:02}:00:00", i % 30, i % 24)),
            f(17, format!("08:00:2b:01:02:{:02x}", i % 256)),
        ];
        fields.join("\t").into_bytes()
    }

    struct Target {
        dir: String,
        database: Arc<Database>,
    }

    impl Drop for Target {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A fresh on-disk database holding the planned table, keys dropped, as
    /// `load` leaves it before the first row.
    async fn target(table: &TablePlan, tag: &str) -> Target {
        let dir = format!(
            "/var/tmp/rdb_import_tests/{}_{}_{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the target directory");
        let database = Arc::new(Database::new(&dir).await);
        run_ddl(&database, &table.create).await.expect("create");
        for statement in &table.drop_constraints {
            run_ddl(&database, statement).await.expect("drop keys");
        }
        Target { dir, database }
    }

    /// Feeds `lines` through the loader under the bulk-load guard, then makes
    /// the load durable, as `import` does.
    /// Returns how many rows the direct write took.
    async fn load_lines_via(
        target: &Target,
        table: &TablePlan,
        lines: &[Vec<u8>],
        batch: usize,
        direct: bool,
    ) -> u64 {
        let options = ImportOptions {
            batch_rows: batch,
            ..ImportOptions::default()
        };
        let bulk = crate::core::bulk_load::begin(&target.dir);
        let mut loader = TableLoader::new(&target.database, table, &options);
        loader.direct = direct;
        for line in lines {
            loader.row(line).await.expect("row");
        }
        let direct_rows = loader.direct_rows;
        let n = loader.finish().await.expect("finish");
        assert_eq!(n, lines.len() as u64);
        drop(bulk);
        finish(&target.database).await.expect("durable");
        direct_rows
    }

    async fn select(database: &Arc<Database>, sql: &str) -> Vec<u8> {
        let purpose = recognize_query_purpose(sql).expect("parse");
        executor::execute(purpose, database.clone()).await
    }

    /// Every value the import converts, stored by the direct write and by the
    /// statement it replaced, must read back identically — NULLs, NUMERIC
    /// scales, floats' edge values, escapes, JSON, arrays, BYTEA and the
    /// PostgreSQL types carried as text. Read back two ways: the whole row, and
    /// each column on its own with its nullness, so a NULL that became a zero
    /// cannot hide behind a renderer that prints both the same.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_direct_write_stores_what_the_statement_stored() {
        let table = plan_for("differential");
        let lines: Vec<Vec<u8>> = (0..3_000).map(copy_line).collect();
        let reference = target(&table, "reference").await;
        load_lines_via(&reference, &table, &lines, 700, false).await;
        let direct = target(&table, "direct").await;
        let taken = load_lines_via(&direct, &table, &lines, 700, true).await;
        // Otherwise this compares the statement path with itself.
        assert_eq!(taken, lines.len() as u64, "the direct write handed rows back");

        let all = "SELECT * FROM differential ORDER BY id";
        let a = select(&reference.database, all).await;
        let b = select(&direct.database, all).await;
        assert_eq!(a.first(), Some(&2), "the reference read failed: {:?}", String::from_utf8_lossy(&a));
        assert!(a.len() > 3_000 * 20, "suspiciously little data: {} bytes", a.len());
        assert!(a == b, "SELECT * differs between the two writes");
        for column in &table.columns {
            let name = &column.source.name;
            let sql = format!(
                "SELECT id, {name}, {name} IS NULL FROM differential ORDER BY id"
            );
            assert_eq!(
                select(&reference.database, &sql).await,
                select(&direct.database, &sql).await,
                "column {name} differs"
            );
        }
        // Some of every column is NULL, so the companions were exercised.
        let nulls = select(
            &direct.database,
            "SELECT id FROM differential WHERE name IS NULL",
        )
        .await;
        assert_eq!(nulls.first(), Some(&2));
    }

    /// A row the engine refuses is refused with the same words either way —
    /// the direct write hands it to the statement, which then decides.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_row_fails_the_same_way_through_both_writes() {
        let table = plan_for("refusals");
        let mut bad = copy_line(1);
        // `id` is NOT NULL.
        let tab = bad.iter().position(|b| *b == b'\t').unwrap();
        bad.splice(..tab, b"\\N".iter().copied());
        let mut errors = Vec::new();
        for direct in [false, true] {
            let target = target(&table, "refusal").await;
            let options = ImportOptions::default();
            let mut loader = TableLoader::new(&target.database, &table, &options);
            loader.direct = direct;
            loader.row(&copy_line(0)).await.expect("a good row");
            errors.push(loader.row(&bad).await.expect_err("the bad row is refused"));
        }
        assert_eq!(errors[0], errors[1]);
        assert!(errors[0].contains("row 2"), "{}", errors[0]);
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// import_load_bench --nocapture`. Rows/s through the load step on an
    /// on-disk database under /var/tmp, from a synthetic `COPY` stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn import_load_bench() {
        let rows: u64 = std::env::var("RDB_IMPORT_BENCH_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);
        let table = plan_for("bench_rows");
        let lines: Vec<Vec<u8>> = (0..rows).map(copy_line).collect();
        for round in 0..3 {
            for direct in [false, true] {
                let target = target(&table, "bench").await;
                let started = Instant::now();
                load_lines_via(&target, &table, &lines, 5_000, direct).await;
                let elapsed = started.elapsed();
                println!(
                    "round {round} {}: {rows} rows in {elapsed:.2?} = {:.0} rows/s",
                    if direct { "direct   " } else { "statement" },
                    rows as f64 / elapsed.as_secs_f64()
                );
            }
        }
    }
}
