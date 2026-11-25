//! Row-image capture for a shard's Raft group (sharding-plan.md B3).
//!
//! On the leader, every row a writing transaction stamps is recorded as an
//! op at the storage layer (insert: the new image; delete: the old image;
//! update: both), beside `shard_hooks::check_stamp`. At commit
//! ([`take_family`]) the family's ops (its own and every surviving
//! savepoint's, in the order they were stamped) become one command, which
//! the group proposes; the local commit waits for it to apply.
//!
//! Images are the stored values, companions (`__null_`, `__ci_`, `__cia_`)
//! included, so a follower writes what the leader wrote and recomputes
//! nothing. Temp relations are never captured: they are not replicated.
//!
//! Off unless the shard runs a Raft group ([`activate`]); then one relaxed
//! load per stamp when nothing is captured.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;

use crate::core::mvcc::TxId;

/// A row as stored: `(write_order, type byte, bytes)` per column.
pub type Image = Vec<(u32, u8, Vec<u8>)>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Insert { table: String, row: Image },
    Delete { table: String, old: Image },
    Update { table: String, old: Image, new: Image },
    /// A schema change, as its statement: replicas run it in log order, so
    /// their schemas and write orders stay the leader's.
    Ddl { sql: String },
}

/// One committed transaction's effects, as proposed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    /// The node and transaction it came from: the leader commits that
    /// transaction when the entry applies; any other replica, or a leader
    /// restarted since, writes the images.
    pub origin_node: u64,
    pub origin_tx: u64,
    pub database: String,
    pub ops: Vec<Op>,
}

static ACTIVE: AtomicBool = AtomicBool::new(false);
static SEQ: AtomicU64 = AtomicU64::new(0);
/// `(database id, writing transaction)` → its ops with their order.
static BUFFER: std::sync::LazyLock<DashMap<(u64, TxId), Vec<(u64, Op)>>> = std::sync::LazyLock::new(DashMap::new);

/// Turns capture on: this shard replicates through Raft.
pub fn activate() {
    ACTIVE.store(true, Ordering::Release);
}

#[inline]
pub fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// The database id a table's cache scope names (`"<db_id>\0…"`).
pub fn database_of_scope(scope: &str) -> Option<u64> {
    scope.split('\0').next()?.parse().ok()
}

/// Records `op`, stamped by transaction `tx` of database `database`.
pub fn record(database: u64, tx: TxId, op: Op) {
    if is_temp(&op) || super::raft_apply::is_applying(database, tx) {
        return;
    }
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    BUFFER.entry((database, tx)).or_default().push((seq, op));
}

fn is_temp(op: &Op) -> bool {
    let t = match op {
        Op::Insert { table, .. } | Op::Delete { table, .. } | Op::Update { table, .. } => table,
        Op::Ddl { .. } => return false,
    };
    t.starts_with("__cte_tmp_")
        || t.starts_with("__rdb_tmp_")
        || t.starts_with("__cat_")
        // The applied index is each replica's own.
        || t == "__rdb_shard_applied"
}

/// The ops of a committing family, in stamp order: every member's, those of
/// rolled-back members dropped. `members` pairs each id with whether its
/// writes survive. Every member's buffer is emptied either way.
pub fn take_family(database: u64, members: &[(TxId, bool)]) -> Vec<Op> {
    let mut all: Vec<(u64, Op)> = Vec::new();
    for &(tx, survived) in members {
        if let Some((_, ops)) = BUFFER.remove(&(database, tx))
            && survived
        {
            all.extend(ops);
        }
    }
    all.sort_by_key(|(seq, _)| *seq);
    all.into_iter().map(|(_, op)| op).collect()
}

/// Drops an aborted transaction's ops.
pub fn discard(database: u64, tx: TxId) {
    BUFFER.remove(&(database, tx));
}

// ------------------------------------------------------------------ codec

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    put_u32(b, v.len() as u32);
    b.extend_from_slice(v);
}

fn put_image(b: &mut Vec<u8>, img: &Image) {
    put_u32(b, img.len() as u32);
    for (wo, ty, bytes) in img {
        put_u32(b, *wo);
        b.push(*ty);
        put_bytes(b, bytes);
    }
}

/// The command's bytes: `version 1 ‖ origin_node ‖ origin_tx ‖ database ‖
/// n ‖ n × (tag ‖ table ‖ images)`.
pub fn encode(c: &Command) -> Vec<u8> {
    let mut b = vec![1u8];
    put_u64(&mut b, c.origin_node);
    put_u64(&mut b, c.origin_tx);
    put_bytes(&mut b, c.database.as_bytes());
    put_u32(&mut b, c.ops.len() as u32);
    for op in &c.ops {
        match op {
            Op::Insert { table, row } => {
                b.push(1);
                put_bytes(&mut b, table.as_bytes());
                put_image(&mut b, row);
            }
            Op::Delete { table, old } => {
                b.push(2);
                put_bytes(&mut b, table.as_bytes());
                put_image(&mut b, old);
            }
            Op::Update { table, old, new } => {
                b.push(3);
                put_bytes(&mut b, table.as_bytes());
                put_image(&mut b, old);
                put_image(&mut b, new);
            }
            Op::Ddl { sql } => {
                b.push(4);
                put_bytes(&mut b, sql.as_bytes());
            }
        }
    }
    b
}

struct R<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len()).ok_or("a truncated raft command")?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn text(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?).map_err(|_| "a raft command's name is not UTF-8".to_string())
    }
    fn image(&mut self) -> Result<Image, String> {
        let n = self.u32()? as usize;
        let mut img = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            img.push((self.u32()?, self.u8()?, self.bytes()?));
        }
        Ok(img)
    }
}

pub fn decode(bytes: &[u8]) -> Result<Command, String> {
    let mut r = R { b: bytes, at: 0 };
    if r.u8()? != 1 {
        return Err("a raft command of a version this build does not know".to_string());
    }
    let origin_node = r.u64()?;
    let origin_tx = r.u64()?;
    let database = r.text()?;
    let n = r.u32()? as usize;
    let mut ops = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        ops.push(match r.u8()? {
            1 => Op::Insert { table: r.text()?, row: r.image()? },
            2 => Op::Delete { table: r.text()?, old: r.image()? },
            3 => Op::Update { table: r.text()?, old: r.image()?, new: r.image()? },
            4 => Op::Ddl { sql: r.text()? },
            t => return Err(format!("a raft command op of tag {t}")),
        });
    }
    if r.at != bytes.len() {
        return Err("a raft command has trailing bytes".to_string());
    }
    Ok(Command { origin_node, origin_tx, database, ops })
}

/// Whether `sql` changes the schema (it travels as an [`Op::Ddl`]).
pub fn is_ddl(sql: &str) -> bool {
    let first = sql.trim_start().split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("");
    ["CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "RENAME"].iter().any(|k| first.eq_ignore_ascii_case(k))
}

/// A stored row as an image.
pub fn image_of_row(row: &crate::core::row::row::Row) -> Image {
    row.columns
        .iter()
        .map(|c| (c.schema_id as u32, c.column_type.to_byte(), c.data.into_slice().to_vec()))
        .collect()
}

/// A write as an image.
pub fn image_of_write(w: &crate::core::row::row::RowWrite) -> Image {
    w.columns_writing_data
        .iter()
        .map(|c| (c.write_order, c.column_type.to_byte(), c.data.into_slice().to_vec()))
        .collect()
}
