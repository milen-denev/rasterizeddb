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
