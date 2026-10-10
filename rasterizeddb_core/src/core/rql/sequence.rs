//! Implicit sequences behind `SERIAL`, `SMALLSERIAL` and `BIGSERIAL`.
//!
//! PostgreSQL's `SERIAL` is not a type: it is an integer column, `NOT NULL`,
//! whose default calls `nextval` on a sequence created alongside the table.
//! That is exactly how it is built here, which is what lets it reuse the
//! defaults machinery instead of needing a path of its own — the column carries
//! a `DEFAULT nextval('t_c_seq')` in the same sidecar as every other default,
//! and [`crate::core::rql::column_default::evaluate`] resolves it.
//!
//! # Why the counter is seeded rather than persisted
//!
//! The obvious design writes the counter back on every allocation. That is a
//! sidecar write per inserted row, on the latency path, to persist a number
//! that the table's own contents already imply. Instead the counter lives in
//! memory and is seeded, once per process, from the largest value the column
//! already holds — [`seed_from_rows`]. A restart therefore resumes above every
//! stored key, which is the property that actually matters.
//!
//! The difference from PostgreSQL is visible in one place: values consumed by a
//! transaction that then rolled back are handed out again after a restart,
//! where PostgreSQL would leave the gap. A gap is not a guarantee `SERIAL`
//! makes, and no key is ever duplicated, which is the guarantee it does make.

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

/// Every sequence this process has issued a value from, by name.
static SEQUENCES: std::sync::LazyLock<DashMap<String, AtomicI64>> =
    std::sync::LazyLock::new(DashMap::new);

/// The name PostgreSQL gives the sequence behind `t.c`.
pub fn sequence_name(table: &str, column: &str) -> String {
    format!(
        "{}_{}_seq",
        table.to_ascii_lowercase(),
        column.to_ascii_lowercase()
    )
}

/// The sequence a `nextval('…')` expression names, or `None` when the
/// expression is not one.
pub fn parse_nextval(expression: &str) -> Option<String> {
    let trimmed = expression.trim();
    let open = trimmed.find('(')?;
    if !trimmed[..open].trim().eq_ignore_ascii_case("nextval") {
        return None;
    }
    let close = trimmed.rfind(')')?;
    if close < open {
        return None;
    }
    let inner = trimmed[open + 1..close].trim();
    // `nextval('t_c_seq'::regclass)` is what a `pg_dump` emits.
    let inner = inner.split("::").next().unwrap_or(inner).trim();
    Some(inner.trim_matches(['\'', '"']).to_string())
}

/// Whether a type name declares a serial column, and the integer type it means.
///
/// The width matters: `SMALLSERIAL` past 32767 is an error in PostgreSQL rather
/// than a wrap, and the column has to be declared with the type that says so.
pub fn serial_type(type_name: &str) -> Option<crate::core::db_type::DbType> {
    use crate::core::db_type::DbType;
    let base = type_name
        .split(['(', '['])
        .next()
        .unwrap_or(type_name)
        .trim()
        .to_ascii_uppercase();
    Some(match base.as_str() {
        "SERIAL" | "SERIAL4" => DbType::I32,
        "BIGSERIAL" | "SERIAL8" => DbType::I64,
        "SMALLSERIAL" | "SERIAL2" => DbType::I16,
        _ => return None,
    })
}

/// Whether this process has already worked out where `name` should resume.
pub fn is_seeded(name: &str) -> bool {
    SEQUENCES.contains_key(name)
}

/// Records where a sequence resumes, if it has no value yet.
///
/// `last` is the largest value already stored, so the next value handed out is
/// `last + 1`. A fresh table seeds with `0`.
pub fn seed(name: &str, last: i64) {
    SEQUENCES
        .entry(name.to_string())
        .or_insert_with(|| AtomicI64::new(last));
}

/// The next value of a sequence.
///
/// An unseeded sequence starts at one, which is what a table created in this
/// process wants and what a reopened one gets only after [`seed`] has run —
/// hence the seeding pass the insert path makes first.
pub fn next(name: &str) -> i64 {
    let entry = SEQUENCES
        .entry(name.to_string())
        .or_insert_with(|| AtomicI64::new(0));
    entry.value().fetch_add(1, Ordering::SeqCst) + 1
}

/// Forgets a sequence, for a table that has been dropped.
///
/// Without this a table dropped and recreated under the same name would keep
/// counting from where the old one stopped.
pub fn forget_table(table: &str) {
    let prefix = format!("{}_", table.to_ascii_lowercase());
    SEQUENCES.retain(|name, _| !name.starts_with(&prefix) || !name.ends_with("_seq"));
}

/// The largest value stored in a column, as the seed for its sequence.
///
/// `None` when the column holds no rows, which seeds at zero.
pub fn max_of_column(
    rows: &[crate::core::row::row::Row],
    write_order: u64,
    db_type: &crate::core::db_type::DbType,
) -> Option<i64> {
    use crate::core::db_type::DbType;
    let mut max: Option<i64> = None;
    for row in rows {
        let Some(column) = row.columns.iter().find(|c| u64::from(c.schema_id) == write_order) else {
            continue;
        };
        let bytes = column.data.into_slice();
        let value = match db_type {
            DbType::I16 => bytes
                .get(0..2)
                .and_then(|b| b.try_into().ok())
                .map(i16::from_le_bytes)
                .map(i64::from),
            DbType::I32 => bytes
                .get(0..4)
                .and_then(|b| b.try_into().ok())
                .map(i32::from_le_bytes)
                .map(i64::from),
            DbType::I64 => bytes
                .get(0..8)
                .and_then(|b| b.try_into().ok())
                .map(i64::from_le_bytes),
            _ => None,
        };
        if let Some(value) = value {
            max = Some(max.map_or(value, |m: i64| m.max(value)));
        }
    }
    max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nextval_expression_yields_its_sequence_name() {
        assert_eq!(
            parse_nextval("nextval('t_id_seq')").as_deref(),
            Some("t_id_seq")
        );
        assert_eq!(
            parse_nextval("nextval('t_id_seq'::regclass)").as_deref(),
            Some("t_id_seq")
        );
        assert_eq!(
            parse_nextval("NEXTVAL(\"t_id_seq\")").as_deref(),
            Some("t_id_seq")
        );
        assert!(parse_nextval("CURRENT_TIMESTAMP").is_none());
        assert!(parse_nextval("gen_random_uuid()").is_none());
    }

    #[test]
    fn serial_spellings_map_to_their_widths() {
        use crate::core::db_type::DbType;
        assert_eq!(serial_type("SERIAL"), Some(DbType::I32));
        assert_eq!(serial_type("bigserial"), Some(DbType::I64));
        assert_eq!(serial_type("SMALLSERIAL"), Some(DbType::I16));
        assert_eq!(serial_type("INTEGER"), None);
    }

    #[test]
    fn a_sequence_counts_up_from_its_seed() {
        let name = "seed_test_a_seq";
        seed(name, 41);
        assert_eq!(next(name), 42);
        assert_eq!(next(name), 43);
    }

    #[test]
    fn seeding_twice_does_not_move_a_live_sequence_backwards() {
        let name = "seed_test_b_seq";
        seed(name, 10);
        assert_eq!(next(name), 11);
        // A second seeding pass — a concurrent insert reaching the same table —
        // must not reset the counter and hand out 11 again.
        seed(name, 10);
        assert_eq!(next(name), 12);
    }

    #[test]
    fn a_dropped_table_forgets_its_sequences() {
        let name = sequence_name("droppable", "id");
        seed(&name, 5);
        assert_eq!(next(&name), 6);
        forget_table("droppable");
        assert!(!is_seeded(&name));
    }
}
