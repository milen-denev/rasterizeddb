//! Cluster DDL's shard side (sharding-plan.md B1a.10): the schema
//! fingerprint each shard acks after applying a `DdlApply`.
//!
//! The procedure (lock, apply on every shard idempotently, compare the
//! fingerprints, publish the next schema epoch or mark the table `PARTIAL`)
//! runs from the operator tool; a shard only applies and answers with
//! [`fingerprint`]. Two shards whose fingerprints differ would store the same
//! column under different write orders, and a statement routed by the
//! gateway would read garbage on one of them, so the table is refused until
//! `CLUSTER DDL RESUME` makes them agree.

use crate::core::row::schema::TableSchema;

/// Seeds of the fingerprint's two halves ("RDBFP" 1 and 2).
const SEED_HI: u64 = 0x5244_4246_5000_0001;
const SEED_LO: u64 = 0x5244_4246_5000_0002;

/// A 128-bit fingerprint of `schema`'s stored shape: every column record in
/// order (live and dropped, the engine's `__null_`, `__ci_` and `__cia_`
/// companions included) with its write order, type, size, offset and flags;
/// the primary key; and every unique key's name, columns, predicate and
/// kind. Encoded field by field, never through `Debug`, so caches inside the
/// schema (a unique key's compiled predicate) cannot make two equal schemas
/// differ.
pub fn fingerprint(schema: &TableSchema) -> u128 {
    let mut b = Vec::with_capacity(64 + schema.fields.len() * 48);
    put_str(&mut b, &schema.name);
    b.push(schema.is_immutable as u8);
    put_u64(&mut b, schema.fields.len() as u64);
    for f in schema.fields.iter() {
        put_str(&mut b, &f.name);
        put_str(&mut b, &format!("{}", f.db_type));
        put_u64(&mut b, f.write_order);
        put_u64(&mut b, f.size as u64);
        put_u64(&mut b, f.offset);
        b.push(f.is_unique as u8);
        b.push(f.is_deleted as u8);
        b.push(f.without_time_zone as u8);
        b.push(f.json_text as u8);
        match f.scale {
            Some(sc) => {
                b.push(1);
                put_u64(&mut b, sc as u64);
            }
            None => b.push(0),
        }
        put_str(&mut b, &format!("{:?}", f.typmod_packed));
    }
    match &schema.primary_key {
        Some(pk) => {
            b.push(1);
            put_str(&mut b, pk);
        }
        None => b.push(0),
    }
    put_u64(&mut b, schema.unique_keys.len() as u64);
    for k in &schema.unique_keys {
        put_str(&mut b, &k.name);
        put_u64(&mut b, k.columns.len() as u64);
        for c in &k.columns {
            put_str(&mut b, c);
        }
        b.push(k.is_deleted as u8);
        b.push(k.from_index as u8);
        b.push(k.primary as u8);
        match &k.predicate {
            Some(p) => {
                b.push(1);
                put_str(&mut b, p);
            }
            None => b.push(0),
        }
    }
    let hi = rasterizeddb_router::xxh64::xxh64(&b, SEED_HI);
    let lo = rasterizeddb_router::xxh64::xxh64(&b, SEED_LO);
    ((hi as u128) << 64) | lo as u128
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_str(b: &mut Vec<u8>, s: &str) {
    put_u64(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}

/// The tables a cluster DDL is changing right now (B4.6): set on every
/// shard before the DDL applies anywhere, cleared once every shard's
/// fingerprint agrees and the schema epoch is raised. While a table is
/// fenced, a statement that reads or writes it is refused with a
/// retryable 40001, so no statement runs against one shard's new schema
/// and another's old one. Kept in memory: a restarted shard is unfenced,
/// and the tool sets the fence again when it resumes.
static FENCE: std::sync::RwLock<Vec<String>> = std::sync::RwLock::new(Vec::new());
static FENCED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Replaces the fence with `tables` (comma separated; empty lifts it).
pub fn set_fence(tables: &str) {
    let list: Vec<String> = tables
        .split(',')
        .map(|t| t.trim().trim_matches('\'').trim_matches('"').to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    FENCED.store(!list.is_empty(), std::sync::atomic::Ordering::Release);
    *FENCE.write().unwrap_or_else(|e| e.into_inner()) = list;
}

/// The fence as `SHOW CONFIG` reports it.
pub fn fence() -> String {
    FENCE.read().unwrap_or_else(|e| e.into_inner()).join(",")
}

/// The refusal for `stmt` while it touches a fenced table; `None` for a
/// statement that does not, and for the DDL and session statements the
/// change itself is made with.
pub fn fenced_refusal(stmt: &str) -> Option<String> {
    if !FENCED.load(std::sync::atomic::Ordering::Acquire) {
        return None;
    }
    let head = stmt.trim_start().split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("").to_ascii_uppercase();
    if matches!(
        head.as_str(),
        "CREATE" | "ALTER" | "DROP" | "SET" | "SHOW" | "RESET" | "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT"
    ) {
        return None;
    }
    let fence = FENCE.read().unwrap_or_else(|e| e.into_inner());
    crate::core::rql::lock_set::relation_references(stmt).into_iter().find_map(|r| {
        let name = r.name.rsplit('.').next().unwrap_or(&r.name).trim_matches('"').to_ascii_lowercase();
        fence.contains(&name).then(|| {
            format!("40001: table {name} is having its schema changed across the cluster; retry the statement in a moment")
        })
    })
}

#[cfg(test)]
mod fence_tests {
    use super::*;

    #[test]
    fn b46_a_fenced_table_refuses_reads_and_writes_but_not_its_ddl() {
        set_fence("orders, \"Items\"");
        assert!(fenced_refusal("SELECT * FROM orders WHERE id = 1").is_some_and(|e| e.starts_with("40001")));
        assert!(fenced_refusal("UPDATE public.orders SET x = 1").is_some());
        assert!(fenced_refusal("SELECT 1 FROM other").is_none());
        assert!(fenced_refusal("ALTER TABLE orders ADD COLUMN y INT").is_none(), "the DDL itself runs");
        set_fence("");
        assert!(fenced_refusal("SELECT * FROM orders").is_none(), "lifted");
    }
}
