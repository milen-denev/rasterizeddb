//! Applying committed Raft entries to a shard's tables (sharding-plan.md
//! B3): the group's [`StateMachine`].
//!
//! - **The leader's own entry** (its origin transaction is still waiting in
//!   `commit_written_transaction`, [`register_origin`]): nothing to write;
//!   the transaction already holds the rows and commits once this returns.
//! - **Anyone else's, or the leader's after a restart** (its origin
//!   transaction died with the process): the images are written under a
//!   fresh transaction at the storage layer, by primary key, with capture
//!   off for it, and no defaults, foreign-key actions, triggers or sequences
//!   (the leader's statement already ran them, and their effects are in the
//!   entry).
//! - **Exactly once, across restarts**: each database keeps the highest
//!   index applied to it in the engine-owned `__rdb_shard_applied`, written
//!   in the entry's own transaction. An entry at or below it is skipped.
//!
//! A failure halts the replica (the group stops serving): applying less, or
//! something else, than a quorum committed is divergence.

use std::collections::HashMap;

use dashmap::DashSet;
use rasterizeddb_raft::{Index, SnapshotMeta};
use rclite::Arc;

use super::capture::{self, Command, Image, Op};
use super::raft_group::StateMachine;
use crate::core::database::Database;
use crate::core::database::catalog::DatabaseCatalog;
use crate::core::database::relation::Relation;
use crate::core::mvcc::TxId;

const APPLIED_TABLE: &str = "__rdb_shard_applied";

/// `(origin node, database, origin transaction)` of the leader's commits
/// waiting for their entry.
static ORIGINS: std::sync::LazyLock<DashSet<(u64, String, u64)>> = std::sync::LazyLock::new(DashSet::new);
/// Apply transactions, whose writes capture must not record.
static APPLYING: std::sync::LazyLock<DashSet<(u64, TxId)>> = std::sync::LazyLock::new(DashSet::new);

/// The running group, this replica's id, and the catalogue (for the
/// commit path, which holds a `&Database`).
static RAFT: std::sync::OnceLock<(super::raft_group::Group, u64, Arc<DatabaseCatalog>)> = std::sync::OnceLock::new();

/// Installs the running group; once, at start.
pub fn install(group: super::raft_group::Group, node: u64, catalog: Arc<DatabaseCatalog>) {
    let _ = RAFT.set((group, node, catalog));
    capture::activate();
}

/// `Err` when this replica must not serve sessions: it runs a Raft group
/// and is not its leader, so what it holds may lag what a quorum committed.
/// Sessions go to the leader, whose reads see every committed write (and
/// whose check-quorum steps it down when it loses the majority).
pub fn serving() -> Result<(), String> {
    let Some((group, _, _)) = RAFT.get() else {
        return Ok(());
    };
    let s = group.status();
    if let Some(why) = s.halted {
        return Err(format!("57P03: this shard replica halted: {why}"));
    }
    match s.role {
        Some(rasterizeddb_raft::Role::Leader) => Ok(()),
        _ => Err(format!(
            "57P03: this shard replica is a follower of its Raft group (leader: {}); connect to the leader",
            s.leader.map_or("unknown".to_string(), |l| l.to_string())
        )),
    }
}

/// Proposes `database`'s committing transaction `tx` (sharding-plan.md B3,
/// "propose instead of commit"): its family's captured ops as one entry,
/// waited for until it is applied here; then the applied index is written
/// into `tx` itself, so the commit that follows carries it. `Ok(())` with
/// nothing done when the family captured nothing (a read, temp tables, an
/// apply). An `Err` means the caller aborts: the write is not visible
/// anywhere unless a later leader commits the entry, in which case every
/// replica writes its images then.
pub fn propose_commit(database: &Database, members: &[(TxId, bool)], tx: TxId) -> std::io::Result<()> {
    let Some((group, node, catalog)) = RAFT.get() else {
        return Ok(());
    };
    let ops = capture::take_family(database.id, members);
    if ops.is_empty() {
        return Ok(());
    }
    let command = Command { origin_node: *node, origin_tx: tx, database: database.name.clone(), ops };
    register_origin(*node, &database.name, tx);
    let proposed = run_block(group.propose(capture::encode(&command)));
    let index = match proposed {
        Ok(i) => i,
        Err(e) => {
            unregister_origin(*node, &database.name, tx);
            return Err(std::io::Error::other(match e {
                super::raft_group::GroupError::NotLeader { leader } => {
                    format!("57P03: this shard replica is not its group's leader (leader: {leader:?}); retry on the leader")
                }
                other => format!("40001: the write's replication outcome is unknown ({other:?}); it was rolled back here"),
            }));
        }
    };
    let db = catalog
        .connect(&database.name)
        .ok_or_else(|| std::io::Error::other(format!("database {} is gone", database.name)))?
        .database()
        .clone();
    run_block(async {
        if !db.has_table(APPLIED_TABLE) {
            exec(&db, &format!("CREATE TABLE {APPLIED_TABLE} (idx BIGINT NOT NULL)"), None).await?;
        }
        exec(&db, &format!("INSERT INTO {APPLIED_TABLE} (idx) VALUES ({index})"), Some(tx)).await
    })
    .map_err(|e| std::io::Error::other(format!("XX000: recording applied index {index}: {e}")))?;
    Ok(())
}

/// The leader registers its committing transaction before proposing.
pub fn register_origin(node: u64, database: &str, tx: u64) {
    ORIGINS.insert((node, database.to_string(), tx));
}

pub fn unregister_origin(node: u64, database: &str, tx: u64) {
    ORIGINS.remove(&(node, database.to_string(), tx));
}

/// Whether `tx` of database `db` is an apply's (capture skips it).
pub fn is_applying(db: u64, tx: TxId) -> bool {
    APPLYING.contains(&(db, tx))
}

pub struct ShardMachine {
    catalog: Arc<DatabaseCatalog>,
    node: u64,
    /// Highest index applied, per database, loaded on first use.
    applied: HashMap<String, u64>,
}

impl ShardMachine {
    pub fn new(catalog: Arc<DatabaseCatalog>, node: u64) -> ShardMachine {
        ShardMachine { catalog, node, applied: HashMap::new() }
    }
}

fn run_block<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

async fn exec(db: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<Vec<u8>, String> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("could not parse: {sql}"))?;
    let out = match tx {
        Some(tx) => executor::execute_in_txn(purpose, db.clone(), tx).await,
        None => executor::execute(purpose, db.clone()).await,
    };
    if out.first() == Some(&3) {
        return Err(String::from_utf8_lossy(&out[1..]).to_string());
    }
    Ok(out)
}

/// The highest index applied to `db` (0 for none); makes the table.
async fn applied_of(db: &Arc<Database>) -> Result<u64, String> {
    if !db.has_table(APPLIED_TABLE) {
        exec(db, &format!("CREATE TABLE {APPLIED_TABLE} (idx BIGINT NOT NULL)"), None).await?;
        return Ok(0);
    }
    let out = exec(db, &format!("SELECT MAX(idx) FROM {APPLIED_TABLE}"), None).await?;
    let rows = crate::core::row::row::vec_into_rows(&out[1..]).map_err(|e| e.to_string())?;
    Ok(rows
        .first()
        .and_then(|r| r.columns.first())
        .and_then(|c| <[u8; 8]>::try_from(c.data.into_slice()).ok())
        .map_or(0, |b| i64::from_le_bytes(b).max(0) as u64))
}

fn column(wo: u32, ty: u8, bytes: &[u8]) -> crate::core::row::row::Column {
    crate::core::row::row::Column {
        schema_id: wo,
        data: crate::memory_pool::MemoryBlock::from_vec(bytes.to_vec()),
        column_type: crate::core::db_type::DbType::from_byte(ty),
    }
}

/// The image as a write, sized by the schema's fields.
fn write_of(schema: &crate::core::row::schema::TableSchema, img: &Image) -> Result<crate::core::row::row::RowWrite, String> {
    let mut cols: smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]> = smallvec::SmallVec::new();
    for (wo, ty, bytes) in img {
        let field = schema
            .fields
            .iter()
            .find(|f| f.write_order == *wo as u64)
            .ok_or_else(|| format!("{}: no column of write order {wo} (schemas diverged)", schema.name))?;
        cols.push(crate::core::row::row::ColumnWritePayload {
            data: crate::memory_pool::MemoryBlock::from_vec(bytes.clone()),
            write_order: *wo,
            column_type: crate::core::db_type::DbType::from_byte(*ty),
            size: field.size,
        });
    }
    cols.sort_by_key(|c| c.write_order);
    Ok(crate::core::row::row::RowWrite { columns_writing_data: cols })
}

/// The predicate naming the row `img` is, by the table's identifying key.
fn key_predicate(schema: &crate::core::row::schema::TableSchema, img: &Image) -> Result<String, String> {
    let key = crate::core::row::identifying_key::identifying_key(schema)
        .ok_or_else(|| format!("{} has no key a replica can address its rows by", schema.name))?;
    let cells: Vec<crate::core::row::row::Column> = key
        .fields
        .iter()
        .map(|f| {
            img.iter()
                .find(|(wo, _, _)| *wo as u64 == f.write_order)
                .map(|(wo, ty, b)| column(*wo, *ty, b))
                .ok_or_else(|| format!("{}: the image has no key column {}", schema.name, f.name))
        })
        .collect::<Result<_, _>>()?;
    let refs: Vec<&crate::core::row::row::Column> = cells.iter().collect();
    key.predicate(&refs)
}

async fn insert(db: &Arc<Database>, table: &str, img: &Image, tx: TxId) -> Result<(), String> {
    match db.relation(table) {
        Some(Relation::Plain(t)) => {
            let w = write_of(&t.schema, img)?;
            t.insert_row_in_txn(w, tx).await.map_err(|e| format!("{table}: {e}"))
        }
        Some(Relation::Partitioned(pt)) => {
            let w = write_of(Relation::Partitioned(pt.clone()).schema(), img)?;
            pt.insert_row_in_txn(w, tx).await.map_err(|e| format!("{table}: {e}"))
        }
        None => Err(format!("{table} does not exist here (schemas diverged)")),
    }
}

async fn delete(db: &Arc<Database>, table: &str, img: &Image, tx: TxId) -> Result<(), String> {
    let relation = db.relation(table).ok_or_else(|| format!("{table} does not exist here (schemas diverged)"))?;
    let schema = relation.schema().clone();
    let filter = key_predicate(&schema, img)?;
    let fetch = crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_full_row_fetch(&schema.fields);
    let n = match relation {
        Relation::Plain(t) => t.delete_rows_in_txn(&filter, fetch, Some(1), tx).await.map_err(|e| format!("{table}: {e}"))?,
        Relation::Partitioned(pt) => crate::core::rql::row_source::write_partitions_as_of(&pt, &filter, tx, None, None, |part, vis, _| {
            let (fetch, filter) = (fetch.clone(), filter.clone());
            async move {
                let n = part.delete_rows_as_of(&filter, fetch, Some(1), tx, vis, None).await?;
                Ok((n, Vec::new()))
            }
        })
        .await
        .map_err(|e| format!("{table}: {e}"))?,
    };
    if n != 1 {
        return Err(format!("{table}: the row {filter} the leader changed is not here ({n} found): the replica diverged"));
    }
    Ok(())
}

impl ShardMachine {
    async fn apply_command(&mut self, index: Index, c: Command) -> Result<(), String> {
        // The leader's own, still waiting: its transaction commits the rows.
        if ORIGINS.remove(&(c.origin_node, c.database.clone(), c.origin_tx)).is_some() && c.origin_node == self.node {
            return Ok(());
        }
        let guard = self.catalog.connect(&c.database).ok_or_else(|| format!("database {} does not exist here", c.database))?;
        let db = guard.database().clone();
        let done = match self.applied.get(&c.database) {
            Some(n) => *n,
            None => applied_of(&db).await?,
        };
        if index <= done {
            self.applied.insert(c.database.clone(), done);
            return Ok(());
        }
        let tx = db.transactions.begin();
        APPLYING.insert((db.id, tx));
        let result = async {
            for op in &c.ops {
                match op {
                    Op::Insert { table, row } => insert(&db, table, row, tx).await?,
                    Op::Delete { table, old } => delete(&db, table, old, tx).await?,
                    Op::Update { table, old, new } => {
                        delete(&db, table, old, tx).await?;
                        insert(&db, table, new, tx).await?;
                    }
                    Op::Ddl { sql } => {
                        exec(&db, sql, Some(tx)).await?;
                    }
                }
            }
            exec(&db, &format!("INSERT INTO {APPLIED_TABLE} (idx) VALUES ({index})"), Some(tx)).await?;
            db.commit_written_transaction(tx).map_err(|e| e.to_string())?;
            db.sync_own_location().map_err(|e| e.to_string())
        }
        .await;
        APPLYING.remove(&(db.id, tx));
        if let Err(e) = result {
            db.transactions.abort(tx);
            capture::discard(db.id, tx);
            return Err(format!("entry {index} of {}: {e}", c.database));
        }
        self.applied.insert(c.database.clone(), index);
        Ok(())
    }
}

impl StateMachine for ShardMachine {
    fn apply(&mut self, index: Index, command: &[u8]) -> Result<(), String> {
        let c = capture::decode(command)?;
        run_block(self.apply_command(index, c))
    }

    fn snapshot(&mut self) -> Result<Vec<u8>, String> {
        // A follower too far behind is relocated through the backup
        // snapshot routine (the plan's learner relocation), not shipped a
        // snapshot in-band.
        Err("in-band snapshots are not supported: relocate the replica from a backup snapshot".to_string())
    }

    fn restore(&mut self, _meta: &SnapshotMeta, _data: &[u8]) -> Result<(), String> {
        Err("in-band snapshots are not supported: relocate the replica from a backup snapshot".to_string())
    }
}
