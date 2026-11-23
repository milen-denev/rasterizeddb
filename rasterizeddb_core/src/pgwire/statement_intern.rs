//! Prepared-statement text and described column lists, shared across sessions.
//!
//! A driver prepares the same statements on every connection of its pool —
//! sqlx keeps 100 per connection, pgx 512 — and each session used to hold its
//! own copy of every statement's text and of every column list it had been
//! described with, twice (once for the shape check, once in the describe
//! cache). A thousand pooled connections of one application therefore held a
//! thousand copies of one set of strings. Interned, each distinct text and
//! each distinct column list is held once, and a session's entry is a handful
//! of reference counts.
//!
//! # Bounded
//!
//! The tables never grow without limit. An entry no session holds any more —
//! its only reference is the table's own — is swept out when the table next
//! fills; if every entry is still in use when it is full, a new value is
//! handed out *uninterned*, which costs memory and nothing else.
//!
//! Interning changes no answer: a lookup returns a value equal to the one
//! asked for, so every key the describe cache compares is exactly as precise
//! as before.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};

use crate::core::db_type::DbType;

/// A described column list, shared.
pub(super) type Fields = Arc<[(String, DbType)]>;

/// Entries each table holds before it sweeps; also its hard limit.
const CAPACITY: usize = 16 * 1024;

/// One interning table: values bucketed by hash, so the value type needs no
/// `Hash` of its own (`DbType` has none).
struct Table<T: ?Sized> {
    buckets: HashMap<u64, Vec<Arc<T>>, ahash::RandomState>,
    len: usize,
    /// Size at which the next sweep runs. Doubles after a sweep that freed
    /// little, so a table full of live entries is not rescanned on every
    /// insert.
    sweep_at: usize,
}

impl<T: ?Sized> Table<T> {
    fn new() -> Self {
        Self {
            buckets: HashMap::default(),
            len: 0,
            sweep_at: CAPACITY / 4,
        }
    }

    fn intern(
        &mut self,
        hash: u64,
        equal: impl Fn(&T) -> bool,
        make: impl FnOnce() -> Arc<T>,
    ) -> Arc<T> {
        if let Some(found) = self
            .buckets
            .get(&hash)
            .and_then(|b| b.iter().find(|a| equal(a)))
        {
            return found.clone();
        }
        if self.len >= self.sweep_at {
            self.sweep();
        }
        let value = make();
        if self.len < CAPACITY {
            self.buckets.entry(hash).or_default().push(value.clone());
            self.len += 1;
        }
        value
    }

    /// Drops every entry nothing outside the table refers to.
    fn sweep(&mut self) {
        self.buckets.retain(|_, bucket| {
            bucket.retain(|a| Arc::strong_count(a) > 1);
            !bucket.is_empty()
        });
        self.len = self.buckets.values().map(Vec::len).sum();
        self.sweep_at = (self.len * 2).clamp(CAPACITY / 4, CAPACITY);
    }
}

fn hasher() -> &'static ahash::RandomState {
    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    STATE.get_or_init(ahash::RandomState::new)
}

fn sql_table() -> &'static Mutex<Table<str>> {
    static TABLE: OnceLock<Mutex<Table<str>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(Table::new()))
}

fn fields_table() -> &'static Mutex<Table<[(String, DbType)]>> {
    static TABLE: OnceLock<Mutex<Table<[(String, DbType)]>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(Table::new()))
}

/// The shared copy of a statement's text.
pub(super) fn sql(text: &str) -> Arc<str> {
    let hash = hasher().hash_one(text);
    sql_table()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .intern(hash, |t| t == text, || Arc::from(text))
}

/// The shared copy of a described column list.
pub(super) fn fields(list: &[(String, DbType)]) -> Fields {
    let mut h = hasher().build_hasher();
    list.len().hash(&mut h);
    for (name, ty) in list {
        name.hash(&mut h);
        ty.to_byte().hash(&mut h);
    }
    let hash = h.finish();
    fields_table()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .intern(hash, |f| f == list, || Arc::from(list))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_values_share_one_allocation() {
        let a = sql("SELECT intern_test_1 FROM t");
        let b = sql(&String::from("SELECT intern_test_1 FROM t"));
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &sql("SELECT intern_test_2 FROM t")));

        let f1 = fields(&[("a".to_string(), DbType::I32), ("b".to_string(), DbType::STRING)]);
        let f2 = fields(&[("a".to_string(), DbType::I32), ("b".to_string(), DbType::STRING)]);
        assert!(Arc::ptr_eq(&f1, &f2));
        // Same names, different type: a different list.
        let f3 = fields(&[("a".to_string(), DbType::I64), ("b".to_string(), DbType::STRING)]);
        assert!(!Arc::ptr_eq(&f1, &f3));
        assert_eq!(&*f3, &[("a".to_string(), DbType::I64), ("b".to_string(), DbType::STRING)][..]);
    }

    #[test]
    fn the_table_is_bounded_and_sweeps_unreferenced_entries() {
        let mut table: Table<str> = Table::new();
        let state = ahash::RandomState::new();
        let mut held = Vec::new();
        for i in 0..CAPACITY * 2 {
            let text = format!("statement {i}");
            let a = table.intern(state.hash_one(&text), |t| t == text, || Arc::from(text.as_str()));
            // Keep every other one alive.
            if i % 2 == 0 {
                held.push(a);
            }
            assert!(table.len <= CAPACITY);
        }
        table.sweep();
        assert!(table.len <= held.len());
        drop(held);
        table.sweep();
        assert_eq!(table.len, 0, "nothing is referenced any more");
    }
}
