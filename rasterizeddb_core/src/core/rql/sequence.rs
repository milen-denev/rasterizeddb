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

/// Every sequence this process has issued a value from, by database and name.
///
/// The database is part of the key because a sequence belongs to one: keyed by
/// name alone, every database in the process counted on the same `t_id_seq`, so
/// a `tenant` table in a new database started at 51 because the default
/// database's `tenant` had used 1..50. Nothing failed — the keys were simply
/// another database's numbers, which breaks any foreign key assuming 1..N and
/// tells one database how many rows another holds.
///
/// See [`key`] for the layout.
static SEQUENCES: std::sync::LazyLock<DashMap<String, AtomicI64>> =
    std::sync::LazyLock::new(DashMap::new);

/// Where a database's sequence lives in [`SEQUENCES`].
///
/// `database` is [`crate::core::database::Database::id`]: process-wide and
/// never reused, which is exactly the lifetime of this map — a database dropped
/// and recreated under the same name gets a new id, and so fresh counters,
/// without anything having to retire the old ones first. The separator cannot
/// occur in an identifier, so no database and name pair can spell another's.
fn key(database: u64, name: &str) -> String {
    format!("{}\u{0}{}", database, name)
}

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

/// Whether this process has already worked out where `database`'s sequence
/// `name` should resume.
pub fn is_seeded(database: u64, name: &str) -> bool {
    SEQUENCES.contains_key(&key(database, name))
}

/// Records where a sequence resumes, if it has no value yet.
///
/// `last` is the largest value already stored, so the next value handed out is
/// `last + 1`. A fresh table seeds with `0`.
pub fn seed(database: u64, name: &str, last: i64) {
    SEQUENCES
        .entry(key(database, name))
        .or_insert_with(|| AtomicI64::new(last));
}

/// The next value of `database`'s sequence `name`.
///
/// An unseeded sequence starts at one, which is what a table created in this
/// process wants and what a reopened one gets only after [`seed`] has run —
/// hence the seeding pass the insert path makes first.
pub fn next(database: u64, name: &str) -> i64 {
    let entry = SEQUENCES
        .entry(key(database, name))
        .or_insert_with(|| AtomicI64::new(0));
    entry.value().fetch_add(1, Ordering::SeqCst) + 1
}

/// The last value `database`'s sequence `name` handed out (its seed, before
/// any), or `None` while it is unseeded: what a partitioned table's durable
/// reservation is checked against (`partition::serial`).
pub fn current(database: u64, name: &str) -> Option<i64> {
    SEQUENCES
        .get(&key(database, name))
        .map(|v| v.value().load(Ordering::SeqCst))
}

/// Forgets `database`'s sequences `names`, for a table that has been dropped.
///
/// Without this a table dropped and recreated under the same name would keep
/// counting from where the old one stopped.
///
/// By exact name rather than by the table's name as a prefix: the prefix
/// `t_` also matches `t_x_id_seq`, so dropping `t` used to forget table
/// `t_x`'s counter too — which then reseeds from the rows it can see, and a
/// row another transaction has inserted and not yet committed is not one of
/// them. See [`owned_by`] for which names a table takes with it.
pub fn forget(database: u64, names: &[String]) {
    for name in names {
        SEQUENCES.remove(&key(database, name));
    }
}

/// Forgets every sequence of a database that has been dropped.
///
/// Not needed for correctness — a recreated database has a new id and so never
/// reads these — but nothing else would ever release them.
pub fn forget_database(database: u64) {
    let prefix = key(database, "");
    SEQUENCES.retain(|name, _| !name.starts_with(&prefix));
}

/// The sequences a table owns, which go when it does.
///
/// PostgreSQL's rule: the sequence a `SERIAL` column created is `OWNED BY` that
/// column and is dropped with the table, while one a `DEFAULT nextval('…')`
/// merely names lives on. The owned ones are recognisable by name, as
/// `<table>_<column>_seq`.
///
/// Every column the table has ever had is a candidate, not only the ones whose
/// current default still reads that name. The default is not a reliable
/// witness: `DROP COLUMN` tombstones the field, and `DROP DEFAULT` removes the
/// default outright. Asking only live fields whose default reads the name
/// missed both, so the counter outlived the table and the next `CREATE TABLE`
/// of the same name started above 1.
///
/// Nor is a column's *name*: `RENAME COLUMN a TO c` moves the field and its
/// default to `c`, and the default goes on reading `nextval('t_a_seq')`, as it
/// does in PostgreSQL — so no column is called `a` any more. Every sequence of
/// the `<table>_…_seq` shape that one of this table's own defaults reads is a
/// candidate too.
///
/// What keeps a wide candidate set from taking another table's counter is
/// `named_elsewhere`: every sequence a `nextval` default of *another* table in
/// the same database names ([`named_by`], lowercased). Table `t` with a column
/// `x_id` proposes `t_x_id_seq`, which is table `t_x`'s; and a hand-written
/// `DEFAULT nextval('t_id_seq')` on another table makes that sequence shared.
/// Either way something still counts on it, so it stays. A candidate nothing
/// counts on is harmless to forget: a later reader seeds it afresh.
pub fn owned_by(
    table: &str,
    fields: &[crate::core::row::schema::SchemaField],
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    named_elsewhere: &std::collections::HashSet<String>,
) -> Vec<String> {
    let mut owned: Vec<String> = fields
        .iter()
        .map(|f| f.name.as_str())
        .chain(defaults.iter().map(|d| d.column.as_str()))
        .map(|column| sequence_name(table, column))
        .chain(
            defaults
                .iter()
                .filter(|d| !d.is_deleted)
                .filter_map(|d| parse_nextval(&d.expression))
                .map(|name| name.to_ascii_lowercase())
                .filter(|name| serial_column_of(table, name).is_some()),
        )
        .filter(|name| !named_elsewhere.contains(name))
        .collect();
    owned.sort_unstable();
    owned.dedup();
    owned
}

/// The `<column>` of a sequence spelled `<table>_<column>_seq`, the shape a
/// `SERIAL` on `table` creates, or `None` when `name` is not of that shape.
///
/// The column part is whatever the column was called when the `SERIAL` was
/// declared, which a later `RENAME COLUMN` does not change.
fn serial_column_of<'a>(table: &str, name: &'a str) -> Option<&'a str> {
    let rest = name.strip_prefix(table.to_ascii_lowercase().as_str())?;
    let column = rest.strip_prefix('_')?.strip_suffix("_seq")?;
    (!column.is_empty()).then_some(column)
}

/// Every sequence a `nextval` default in `defaults` names, lowercased as
/// [`sequence_name`] spells them.
pub fn named_by(
    defaults: &[crate::core::rql::column_default::ColumnDefault],
) -> impl Iterator<Item = String> + '_ {
    defaults
        .iter()
        .filter_map(|d| parse_nextval(&d.expression))
        .map(|name| name.to_ascii_lowercase())
}

/// The defaults a table renamed from `old_table` to `new_table` rewrites so
/// that its sequences follow it, each with the sequence name it leaves and the
/// one it takes.
///
/// Left as they were, a renamed table's `SERIAL` default went on reading
/// `nextval('<old>_<column>_seq')`, and a new table created under the old name
/// generated that same name: two tables on one counter. In one process their
/// keys interleaved; after a restart whichever table inserted first seeded the
/// counter from its own rows, and the other drew keys above *that* table's
/// maximum — which can be keys it already holds, stored without complaint when
/// nothing declares the column unique.
///
/// Only a default of the shape a `SERIAL` creates moves, and only when nothing
/// else names either sequence (`named_elsewhere`, as in [`owned_by`]): a
/// sequence another table also reads is shared on purpose, and a target name
/// already in use would make the rename create the very sharing it removes.
pub fn follow_rename(
    old_table: &str,
    new_table: &str,
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    named_elsewhere: &std::collections::HashSet<String>,
) -> Vec<(
    crate::core::rql::column_default::ColumnDefault,
    String,
    String,
)> {
    defaults
        .iter()
        .filter(|d| !d.is_deleted)
        .filter_map(|d| {
            let from = parse_nextval(&d.expression)?;
            // By the sequence's own spelling, not by the column the default
            // is filed under: a column renamed since its `SERIAL` was declared
            // still reads `<old_table>_<its first name>_seq`.
            let column = serial_column_of(old_table, &from)?;
            if named_elsewhere.contains(&from) {
                return None;
            }
            let to = sequence_name(new_table, column);
            if named_elsewhere.contains(&to) {
                log::warn!(
                    "sequence {} stays with renamed table {}: {} is already in use",
                    from,
                    new_table,
                    to
                );
                return None;
            }
            let rewritten = crate::core::rql::column_default::ColumnDefault {
                column: d.column.clone(),
                expression: format!("nextval('{}')", to),
                is_deleted: false,
            };
            Some((rewritten, from, to))
        })
        .collect()
}

/// Moves `database`'s counter `from` to the name `to`, for a sequence that
/// followed its table through a rename.
///
/// The larger of the two wins if `to` already counts, so no value either has
/// handed out is handed out again.
pub fn rename(database: u64, from: &str, to: &str) {
    let Some((_, counter)) = SEQUENCES.remove(&key(database, from)) else {
        return;
    };
    let last = counter.into_inner();
    SEQUENCES
        .entry(key(database, to))
        .or_insert_with(|| AtomicI64::new(last))
        .value()
        .fetch_max(last, Ordering::SeqCst);
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

    // Database ids are handed out from 1 upwards, so ids from the top of the
    // range never meet a real database another test in this process opened.
    const DB: u64 = u64::MAX;
    const OTHER_DB: u64 = u64::MAX - 1;

    #[test]
    fn a_sequence_counts_up_from_its_seed() {
        let name = "seed_test_a_seq";
        seed(DB, name, 41);
        assert_eq!(next(DB, name), 42);
        assert_eq!(next(DB, name), 43);
    }

    #[test]
    fn seeding_twice_does_not_move_a_live_sequence_backwards() {
        let name = "seed_test_b_seq";
        seed(DB, name, 10);
        assert_eq!(next(DB, name), 11);
        // A second seeding pass — a concurrent insert reaching the same table —
        // must not reset the counter and hand out 11 again.
        seed(DB, name, 10);
        assert_eq!(next(DB, name), 12);
    }

    #[test]
    fn a_dropped_table_forgets_its_sequences() {
        let name = sequence_name("droppable", "id");
        seed(DB, &name, 5);
        assert_eq!(next(DB, &name), 6);
        forget(DB, &[name.clone()]);
        assert!(!is_seeded(DB, &name));
    }

    /// The same sequence name in two databases is two sequences.
    ///
    /// Keyed by name alone, a `tenant` table created in a new database started
    /// at 51 because the default database's `tenant` had used 1..50.
    #[test]
    fn two_databases_count_a_same_named_sequence_apart() {
        let name = sequence_name("tenant_apart", "id");
        seed(DB, &name, 50);
        assert_eq!(next(DB, &name), 51);
        assert!(
            !is_seeded(OTHER_DB, &name),
            "one database's seed counted as another's"
        );
        assert_eq!(next(OTHER_DB, &name), 1);
        assert_eq!(next(DB, &name), 52);
        assert_eq!(next(OTHER_DB, &name), 2);
    }

    /// Dropping a table forgets its counters in its own database only, and only
    /// its own: not a same-named table's elsewhere, nor a table whose name it
    /// happens to prefix.
    #[test]
    fn a_drop_forgets_only_the_dropped_table_s_sequences() {
        let mine = sequence_name("dropper", "id");
        let prefixed = sequence_name("dropper_x", "id");
        seed(DB, &mine, 3);
        seed(DB, &prefixed, 7);
        seed(OTHER_DB, &mine, 9);

        forget(DB, &[mine.clone()]);

        assert!(!is_seeded(DB, &mine));
        assert!(
            is_seeded(DB, &prefixed),
            "a prefix-sharing table lost its counter"
        );
        assert!(
            is_seeded(OTHER_DB, &mine),
            "another database lost its counter"
        );
        assert_eq!(next(OTHER_DB, &mine), 10);
    }

    #[test]
    fn a_dropped_database_forgets_all_of_its_sequences() {
        let db = u64::MAX - 2;
        let survivor = u64::MAX - 3;
        let name = sequence_name("gone_with_db", "id");
        seed(db, &name, 4);
        seed(survivor, &name, 4);
        forget_database(db);
        assert!(!is_seeded(db, &name));
        assert!(is_seeded(survivor, &name));
    }

    fn field(name: &str, write_order: u64) -> crate::core::row::schema::SchemaField {
        crate::core::row::schema::SchemaField::new(
            name.to_string(),
            crate::core::db_type::DbType::I64,
            8,
            0,
            write_order,
            false,
        )
    }

    fn default(column: &str, expression: &str) -> crate::core::rql::column_default::ColumnDefault {
        crate::core::rql::column_default::ColumnDefault {
            column: column.to_string(),
            expression: expression.to_string(),
            is_deleted: false,
        }
    }

    fn nothing_elsewhere() -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }

    /// Only the sequences a `SERIAL` created go with the table; one a
    /// `DEFAULT nextval('…')` merely names lives on, as in PostgreSQL.
    #[test]
    fn a_table_owns_only_its_serial_sequences() {
        let fields = vec![field("id", 0), field("other", 1), field("n", 2)];
        let defaults = vec![
            default("id", "nextval('t_id_seq')"),
            default("other", "nextval('shared_seq')"),
            default("n", "0"),
        ];
        let owned = owned_by("t", &fields, &defaults, &nothing_elsewhere());
        assert!(owned.contains(&"t_id_seq".to_string()), "{:?}", owned);
        assert!(!owned.contains(&"shared_seq".to_string()), "{:?}", owned);
    }

    /// A `SERIAL` column altered so that no live default reads its sequence
    /// any more still takes the sequence with its table. Each of these used to
    /// leave the counter behind, so the next `CREATE TABLE t (id SERIAL)`
    /// started above 1.
    #[test]
    fn a_serial_column_altered_away_still_owns_its_sequence() {
        // `DROP COLUMN id`: the field is a tombstone, its default untouched.
        let mut dropped = field("id", 0);
        dropped.is_deleted = true;
        let fields = vec![dropped, field("n", 1)];
        let defaults = vec![default("id", "nextval('t_id_seq')")];
        assert!(
            owned_by("t", &fields, &defaults, &nothing_elsewhere())
                .contains(&"t_id_seq".to_string())
        );

        // `RENAME COLUMN id TO ident`: the default moves to `ident` and,
        // as in PostgreSQL, still names `t_id_seq` — which no column is called
        // any more, so only the `nextval` itself says the sequence is `t`'s.
        let fields = vec![field("ident", 0), field("n", 1)];
        let renamed = vec![default("ident", "nextval('t_id_seq')")];
        assert!(
            owned_by("t", &fields, &renamed, &nothing_elsewhere())
                .contains(&"t_id_seq".to_string())
        );

        // `ALTER COLUMN id DROP DEFAULT`: no default at all.
        let fields = vec![field("id", 0), field("n", 1)];
        assert!(
            owned_by("t", &fields, &[], &nothing_elsewhere()).contains(&"t_id_seq".to_string())
        );
    }

    /// A candidate another table still counts on is not this table's to take:
    /// `t`'s column `x_id` spells `t_x_id_seq`, which is table `t_x`'s.
    #[test]
    fn a_sequence_another_table_names_is_not_owned() {
        let fields = vec![field("id", 0), field("x_id", 1)];
        let defaults = vec![default("id", "nextval('t_id_seq')")];
        let elsewhere = named_by(&[default("id", "nextval('t_x_id_seq')")]).collect();
        let owned = owned_by("t", &fields, &defaults, &elsewhere);
        assert!(owned.contains(&"t_id_seq".to_string()), "{:?}", owned);
        assert!(
            !owned.contains(&"t_x_id_seq".to_string()),
            "dropping t would take t_x's counter: {:?}",
            owned
        );
    }

    /// A renamed table's `SERIAL` default follows it, and its counter moves
    /// with it; a hand-named sequence, or one another table shares, stays.
    #[test]
    fn a_renamed_table_takes_its_serial_sequences_along() {
        let defaults = vec![
            default("id", "nextval('t_id_seq')"),
            default("other", "nextval('shared_seq')"),
            default("k", "nextval('t_k_seq')"),
        ];
        let elsewhere = named_by(&[default("k", "nextval('t_k_seq')")]).collect();
        let moved = follow_rename("t", "u", &defaults, &elsewhere);
        assert_eq!(moved.len(), 1, "{:?}", moved);
        let (rewritten, from, to) = &moved[0];
        assert_eq!(rewritten.column, "id");
        assert_eq!(rewritten.expression, "nextval('u_id_seq')");
        assert_eq!((from.as_str(), to.as_str()), ("t_id_seq", "u_id_seq"));

        // A column renamed before the table: the default is filed under the
        // new column name and still reads the sequence its SERIAL created.
        let renamed = vec![default("ident", "nextval('t_id_seq')")];
        let moved = follow_rename("t", "u", &renamed, &nothing_elsewhere());
        assert_eq!(moved.len(), 1, "{:?}", moved);
        let (rewritten, from, to) = &moved[0];
        assert_eq!(rewritten.column, "ident");
        assert_eq!(rewritten.expression, "nextval('u_id_seq')");
        assert_eq!((from.as_str(), to.as_str()), ("t_id_seq", "u_id_seq"));
        // A sequence that only shares the prefix of a *longer* table name is
        // another table's spelling and stays where it is.
        let foreign = vec![default("k", "nextval('tx_k_seq')")];
        assert!(follow_rename("t", "u", &foreign, &nothing_elsewhere()).is_empty());

        let name = sequence_name("renamed_from", "id");
        let target = sequence_name("renamed_to", "id");
        seed(DB, &name, 7);
        rename(DB, &name, &target);
        assert!(!is_seeded(DB, &name), "the old name kept counting");
        assert_eq!(next(DB, &target), 8);
    }
}
