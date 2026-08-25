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
            if self.dry_run { "Would import" } else { "Imported" },
            plural(self.tables.len() as u64, "table", "tables"),
            self.source,
            self.target
        ));
        if !self.server_version.is_empty() {
            out.push(format!("  source server: PostgreSQL {}", self.server_version));
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
    let bulk = database.location.as_deref().map(crate::core::bulk_load::begin);

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

    // `INSERT INTO t (a, b, c) VALUES (` — the same for every row, so it is
    // built once and the values appended to a buffer that is reused.
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

    connection.copy_out(&table.copy).await?;

    let batch_rows = options.batch_rows.max(1);
    let mut statement = String::with_capacity(prefix.len() + 256);
    let mut copied: u64 = 0;
    let mut in_batch = 0usize;
    let mut batch_bytes = 0usize;
    let mut transaction: Option<crate::core::mvcc::TxId> = None;
    let mut last_logged = Instant::now();

    loop {
        let line = match connection.copy_row().await {
            Ok(line) => line,
            Err(e) => {
                if let Some(tx) = transaction.take() {
                    abort(database, tx);
                }
                return Err(format!("reading {}: {}", table.source_qualified, e));
            }
        };

        let Some(line) = line else { break };
        if copy::is_terminator(&line) {
            continue;
        }

        let result = build_statement(table, &prefix, &line, &mut statement);
        let Ok(()) = result else {
            if let Some(tx) = transaction.take() {
                abort(database, tx);
            }
            return Err(format!(
                "{} row {}: {}",
                table.source_qualified,
                copied + 1,
                result.unwrap_err()
            ));
        };

        let tx = match transaction {
            Some(tx) => tx,
            None => {
                let tx = database.transactions.begin();
                transaction = Some(tx);
                tx
            }
        };

        let Some(purpose) = recognize_query_purpose(&statement) else {
            abort(database, tx);
            return Err(format!(
                "{} row {}: the statement built for this row could not be parsed back",
                table.source_qualified,
                copied + 1
            ));
        };

        let response = executor::execute_in_txn(purpose, database.clone(), tx).await;
        if response.first() == Some(&3) {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            abort(database, tx);
            // The statement is named, truncated. Some refusals — "Cannot insert
            // row due to schema mismatch" above all — say nothing about which
            // value the engine could not take, and without the text there is
            // nothing to reproduce from: the row is one of millions streaming
            // past and the copy has already been rolled back.
            const SHOWN: usize = 600;
            let excerpt: String = if statement.chars().count() > SHOWN {
                let head: String = statement.chars().take(SHOWN).collect();
                format!("{head}… [{} chars]", statement.chars().count())
            } else {
                statement.clone()
            };
            return Err(format!(
                "{} row {}: {}\n  statement: {}",
                table.source_qualified,
                copied + 1,
                message,
                excerpt
            ));
        }

        copied += 1;
        in_batch += 1;
        batch_bytes += statement.len();

        // A batch ends on whichever limit it reaches first, which is what makes
        // the size adaptive: narrow rows reach the row count and batch deeply,
        // wide ones reach the byte budget and batch shallowly. A row count
        // alone is not a bound on anything that matters — 5,000 rows of a few
        // bytes and 5,000 rows carrying a megabyte of JSON each are four
        // orders of magnitude apart in what one open transaction has to hold.
        if in_batch >= batch_rows || batch_bytes >= MAX_BATCH_BYTES {
            commit(database, tx).await?;
            transaction = None;
            in_batch = 0;
            batch_bytes = 0;

            if last_logged.elapsed() >= Duration::from_secs(10) {
                log::info!("  {}: {} rows", table.name, copied);
                last_logged = Instant::now();
            }
        }
    }

    if let Some(tx) = transaction {
        commit(database, tx).await?;
    }

    log::info!("Copied {} rows into {}", copied, table.name);
    Ok(copied)
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
            format!("column {} ({}): {}", column.source.name, column.source.formatted_type, e)
        })?;
        statement.push_str(&literal);
    }
    statement.push(')');
    Ok(())
}

/// Runs one DDL statement, failing the import if it does not work.
async fn run_ddl(database: &Arc<Database>, sql: &str) -> Result<(), String> {
    let Some(purpose) = recognize_query_purpose(sql) else {
        return Err(format!("this engine did not recognise the statement: {}", sql));
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
