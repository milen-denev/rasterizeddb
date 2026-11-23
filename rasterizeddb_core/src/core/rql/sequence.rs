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
//! A sequence several tables read (a hand-written `DEFAULT nextval('…')` on a
//! second table) is seeded by whichever inserts first, and every other table
//! still raises it to its own floor before its first insert in the process
//! ([`seed`] keeps the larger value, [`is_floored`] records who has). Seeded
//! once per sequence instead, a table whose rows were higher than the first
//! inserter's drew values it already held, silently, on a column with no
//! unique key.
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

/// The tables that have raised a sequence to their floor in this process,
/// by [`floor_key`]. An entry is only meaningful while its sequence has a
/// counter: [`forget`], [`forget_database`] and [`rename`] drop a sequence's
/// entries with its counter, so the next reader seeds afresh.
static FLOORED: std::sync::LazyLock<DashMap<String, ()>> = std::sync::LazyLock::new(DashMap::new);

/// Where `table`'s floor of `database`'s sequence `name` is recorded in
/// [`FLOORED`]: the sequence's [`key`], a separator, the table. The separator
/// cannot occur in an identifier, so every entry of a sequence starts with
/// its key and a separator.
///
/// The table folded to lowercase: `INSERT INTO ACCT` marks the floor of the
/// table `Acct` (names resolve in any case, `Database::resolve`). Keyed by
/// the spelling, the mark escaped `forget_table_floors("Acct")` at a DROP or
/// RENAME, and a table later taking the name skipped raising its counters to
/// its own rows.
fn floor_key(database: u64, name: &str, table: &str) -> String {
    format!("{}\u{0}{}\u{0}{}", database, name, table.to_ascii_lowercase())
}

/// Drops every [`FLOORED`] entry of `database`'s sequence `name`.
fn forget_floors(database: u64, name: &str) {
    let prefix = format!("{}\u{0}", key(database, name));
    FLOORED.retain(|k, _| !k.starts_with(&prefix));
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
    let inner = inner.trim_matches('\'');
    // And `nextval('public.t_c_seq'::regclass)`, schema-qualified, since a
    // dump runs with an empty `search_path`: the sequence is `t_c_seq`, the
    // one its `CREATE SEQUENCE public.t_c_seq` made, as the `nextval` and
    // `setval` calls read it (`sequence_name_of`). Kept whole, the column
    // counted on a second, empty sequence named `public.t_c_seq`, and the
    // first insert after a restore collided with a restored row.
    let inner = crate::core::sql_scan::last_name_segment(inner);
    Some(inner.trim_matches('"').to_string())
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

/// Records where a sequence resumes: never below `last`.
///
/// `last` is the largest value a table already stores (its floor), so the next
/// value handed out is at least `last + 1`. A fresh table seeds with `0`.
///
/// Returns whether this call seeded it: `false` when the sequence already had
/// a counter (another table sharing it seeded it first). That counter is
/// raised to `last` if it is below it, and otherwise left as it was: seeded
/// from the other table's rows, it could sit below this table's, and the
/// next values would repeat keys this table holds.
pub fn seed(database: u64, name: &str, last: i64) -> bool {
    // An explicit sequence a `DEFAULT nextval('…')` reads: raised above the
    // rows the table holds, as an implicit one is, when it counts upward.
    if let Some(sequence) = explicit(database, name) {
        if sequence.increment > 0 && sequence.called.load(Ordering::SeqCst) {
            sequence.last.fetch_max(last.min(sequence.max), Ordering::SeqCst);
        } else if sequence.increment > 0 && last >= sequence.last.load(Ordering::SeqCst) {
            sequence.last.store(last.min(sequence.max), Ordering::SeqCst);
            sequence.called.store(true, Ordering::SeqCst);
        }
        return false;
    }
    let mut seeded = false;
    SEQUENCES
        .entry(key(database, name))
        .or_insert_with(|| {
            seeded = true;
            AtomicI64::new(last)
        })
        .value()
        .fetch_max(last, Ordering::SeqCst);
    seeded
}

/// Whether `table` has raised `database`'s sequence `name` to its floor in
/// this process ([`mark_floored`]): what an insert into `table` must make
/// true before it takes a value. Not whether the sequence has a counter:
/// another table sharing it may have seeded it from lower rows.
pub fn is_floored(database: u64, name: &str, table: &str) -> bool {
    FLOORED.contains_key(&floor_key(database, name, table))
}

/// Forgets every floor mark `table` holds in `database`, on any sequence:
/// for a table dropped or renamed. The marks are by name, so a table that
/// later takes the name (created, or renamed onto it) would otherwise find
/// the old table's marks and skip raising the counters to its own rows.
pub fn forget_table_floors(database: u64, table: &str) {
    let (prefix, suffix) = (key(database, ""), format!("\u{0}{}", table.to_ascii_lowercase()));
    FLOORED.retain(|k, _| !(k.starts_with(&prefix) && k.ends_with(&suffix)));
}

/// Records that `table`'s floor is in `database`'s counter `name`. After the
/// [`seed`] that put it there, never before: a concurrent insert that sees
/// the mark takes values straight away.
pub fn mark_floored(database: u64, name: &str, table: &str) {
    // A restart whose transaction may still roll back: seeded again by every
    // insert until it ends (`restart`).
    if !RESTARTED.is_empty() && RESTARTED.contains_key(&key(database, name)) {
        return;
    }
    FLOORED.insert(floor_key(database, name, table), ());
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

/// Sequences a `TRUNCATE … RESTART IDENTITY` restarted, by [`key`], and the
/// transaction that did, until an insert finds it ended
/// ([`settle_restarts`]). Meanwhile no floor is marked on them.
static RESTARTED: std::sync::LazyLock<DashMap<String, u64>> = std::sync::LazyLock::new(DashMap::new);

/// Starts `database`'s sequence `name` again for transaction `tx`: the next
/// value is 1, as after `TRUNCATE … RESTART IDENTITY`.
///
/// Undoable, as PostgreSQL's is. Its floor marks go, and none is marked
/// while `tx` may still end either way, so every insert seeds the counter
/// again from the rows it can see ([`seed`] only ever raises a counter).
/// After `tx` ends, the next insert seeds it once more and marks it as
/// usual: a TRUNCATE that committed left no rows, so the counter stays where
/// `tx`'s own inserts took it; one that rolled back left the rows, and the
/// counter is raised above them. A mark made by `tx`'s own insert would have
/// outlived the rollback and handed out a value a restored row holds.
pub fn restart(database: u64, name: &str, tx: u64) {
    SEQUENCES.insert(key(database, name), AtomicI64::new(0));
    forget_floors(database, name);
    RESTARTED.insert(key(database, name), tx);
}

/// Whether `tx` restarted `database`'s sequence `name` and has not been
/// found ended since: its own inserts then take values from the restarted
/// counter as they come, without seeding it (the seeding scan reads past
/// `tx`'s own deletes, so it would raise the counter back above the rows
/// the TRUNCATE removed). No other transaction inserts meanwhile: the
/// TRUNCATE holds ACCESS EXCLUSIVE on the table to `tx`'s end.
pub fn restarted_by(database: u64, name: &str, tx: u64) -> bool {
    !RESTARTED.is_empty() && RESTARTED.get(&key(database, name)).is_some_and(|by| *by == tx)
}

/// Ends the restarts of `database` whose transaction has `finished`, so
/// their sequences take floor marks again (see [`restart`]). Free when
/// nothing was restarted.
pub fn settle_restarts(database: u64, finished: impl Fn(u64) -> bool) {
    if RESTARTED.is_empty() {
        return;
    }
    let prefix = key(database, "");
    RESTARTED.retain(|k, tx| !(k.starts_with(&prefix) && finished(*tx)));
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
        forget_floors(database, name);
    }
}

/// Forgets every sequence of a database that has been dropped.
///
/// Not needed for correctness — a recreated database has a new id and so never
/// reads these — but nothing else would ever release them.
pub fn forget_database(database: u64) {
    let prefix = key(database, "");
    SEQUENCES.retain(|name, _| !name.starts_with(&prefix));
    FLOORED.retain(|name, _| !name.starts_with(&prefix));
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
/// maximum — which could be keys it already held, stored without complaint
/// when nothing declares the column unique. (Each table now raises a shared
/// counter to its own floor before its first insert, see [`seed`]; the
/// tables' keys still interleave, which is why the sharing is removed.)
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
                // An identity column stays one under its table's new name.
                identity: d.identity,
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
    // The tables that floored `from` go with it: a table that later reads
    // the name afresh seeds it from its own rows.
    forget_floors(database, from);
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

// ---------------------------------------------------------------------------
// Sequences created by CREATE SEQUENCE
// ---------------------------------------------------------------------------

/// A sequence `CREATE SEQUENCE` made: its parameters and its counter. Kept
/// apart from the implicit counters above, which a `SERIAL` seeds from its
/// column; one of these counts from its own `START` and keeps to its bounds.
///
/// Values are not transactional, as PostgreSQL's are not: one taken by a
/// transaction that rolls back is gone. The counter is persisted ahead of
/// itself (`reserved`), in the system table `sequence_catalog` keeps, so a
/// crash leaves a gap and never hands a value out twice.
#[derive(Debug)]
pub struct ExplicitSequence {
    pub data_type: String,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub start: i64,
    pub cache: i64,
    pub cycle: bool,
    /// The last value handed out, or `start - increment` before any.
    pub last: AtomicI64,
    /// Whether `last` has been handed out (`setval(…, false)` clears it).
    pub called: std::sync::atomic::AtomicBool,
    /// The value up to which the persisted row covers: a value past it must
    /// be written ahead before the process could lose it.
    pub reserved: AtomicI64,
}

/// Every explicit sequence of every database, by [`key`].
static EXPLICIT: std::sync::LazyLock<DashMap<String, std::sync::Arc<ExplicitSequence>>> =
    std::sync::LazyLock::new(DashMap::new);

/// How many values past the last one handed out a persisted reservation
/// covers.
pub const RESERVE_AHEAD: i64 = 32;

/// Registers (or replaces) `database`'s explicit sequence `name`.
pub fn register_explicit(database: u64, name: &str, sequence: ExplicitSequence) {
    EXPLICIT.insert(key(database, name), std::sync::Arc::new(sequence));
}

/// Forgets `database`'s explicit sequence `name`.
pub fn forget_explicit(database: u64, name: &str) {
    EXPLICIT.remove(&key(database, name));
}

/// `database`'s explicit sequence `name`, if there is one.
pub fn explicit(database: u64, name: &str) -> Option<std::sync::Arc<ExplicitSequence>> {
    EXPLICIT.get(&key(database, name)).map(|e| e.value().clone())
}

/// Every explicit sequence of `database`, by name.
pub fn explicit_all(database: u64) -> Vec<(String, std::sync::Arc<ExplicitSequence>)> {
    let prefix = key(database, "");
    EXPLICIT
        .iter()
        .filter_map(|e| e.key().strip_prefix(&prefix).map(|name| (name.to_string(), e.value().clone())))
        .collect()
}

/// The explicit sequences of `database` whose reservation a value has
/// passed: what `sequence_catalog::flush` writes ahead.
pub fn explicit_needing_reserve(database: u64) -> Vec<(String, i64)> {
    explicit_all(database)
        .into_iter()
        .filter_map(|(name, s)| {
            let last = s.last.load(Ordering::SeqCst);
            let reserved = s.reserved.load(Ordering::SeqCst);
            let ahead = s.increment.saturating_mul(RESERVE_AHEAD / 2);
            let short = if s.increment > 0 { last.saturating_add(ahead) > reserved } else { last.saturating_add(ahead) < reserved };
            short.then(|| (name, reserve_target(&s, last)))
        })
        .collect()
}

/// Where a reservation from `last` reaches: `RESERVE_AHEAD` steps on,
/// clamped to the sequence's bounds.
pub fn reserve_target(sequence: &ExplicitSequence, last: i64) -> i64 {
    last.saturating_add(sequence.increment.saturating_mul(RESERVE_AHEAD)).clamp(sequence.min.min(sequence.max), sequence.max.max(sequence.min))
}

/// The next value of an explicit sequence, as PostgreSQL's `nextval`:
/// `last + increment`, wrapping to the other bound when it cycles, and an
/// error at a bound when it does not (2200H).
fn next_explicit(name: &str, sequence: &ExplicitSequence) -> Result<i64, String> {
    loop {
        let called = sequence.called.load(Ordering::SeqCst);
        let last = sequence.last.load(Ordering::SeqCst);
        let candidate = if !called {
            last
        } else {
            match last.checked_add(sequence.increment) {
                Some(v) if v >= sequence.min && v <= sequence.max => v,
                _ if sequence.cycle => {
                    if sequence.increment > 0 { sequence.min } else { sequence.max }
                }
                _ => {
                    let (which, bound) =
                        if sequence.increment > 0 { ("maximum", sequence.max) } else { ("minimum", sequence.min) };
                    return Err(format!("nextval: reached {which} value of sequence \"{name}\" ({bound})"));
                }
            }
        };
        if !called {
            if sequence.called.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return Ok(candidate);
            }
            continue;
        }
        if sequence.last.compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            return Ok(candidate);
        }
    }
}

/// `nextval(name)` for `database`: an explicit sequence's next value, or an
/// implicit (`SERIAL`, identity) counter's. Records the value as this
/// session's `currval`/`lastval` when a session is in scope.
pub fn next_checked(database: u64, name: &str) -> Result<i64, String> {
    let value = match explicit(database, name) {
        Some(sequence) => next_explicit(name, &sequence)?,
        None => next(database, name),
    };
    session::record(name, value);
    Ok(value)
}

/// `setval(name, value, is_called)`. An explicit sequence's bounds hold
/// (22003); an implicit counter is set as it stands.
pub fn set_value(database: u64, name: &str, value: i64, is_called: bool) -> Result<i64, String> {
    match explicit(database, name) {
        Some(sequence) => {
            if value < sequence.min || value > sequence.max {
                return Err(format!(
                    "setval: value {value} is out of bounds for sequence \"{name}\" ({}..{})",
                    sequence.min, sequence.max
                ));
            }
            sequence.last.store(value, Ordering::SeqCst);
            sequence.called.store(is_called, Ordering::SeqCst);
            // Past the reservation: written ahead at the statement's end.
            if (sequence.increment > 0 && value > sequence.reserved.load(Ordering::SeqCst))
                || (sequence.increment < 0 && value < sequence.reserved.load(Ordering::SeqCst))
            {
                sequence.reserved.store(value, Ordering::SeqCst);
            }
        }
        None => {
            let last = if is_called { value } else { value - 1 };
            SEQUENCES.insert(key(database, name), AtomicI64::new(last));
        }
    }
    if is_called {
        session::record(name, value);
    }
    Ok(value)
}

/// `currval`/`lastval`: what this session's last `nextval` took, per
/// sequence, in a task-local the PostgreSQL listener scopes around each
/// statement (its map lives on the session).
pub mod session {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// One session's `nextval` results.
    #[derive(Debug, Default)]
    pub struct SessionSequences {
        pub by_name: HashMap<String, i64>,
        pub last: Option<i64>,
    }

    /// What a statement's expressions need of a sequence: the database's id
    /// and the session's values.
    #[derive(Debug, Clone)]
    pub struct Scope {
        pub database: u64,
        pub values: Arc<Mutex<SessionSequences>>,
    }

    tokio::task_local! {
        static SCOPE: Scope;
    }

    /// Runs `fut` with `scope` in reach of its expressions.
    pub async fn scoped<F: std::future::Future>(scope: Scope, fut: F) -> F::Output {
        SCOPE.scope(scope, fut).await
    }

    /// `fut`, to be spawned, under the scope of the statement spawning it
    /// (if any), as `statement_clock::inherit` carries the clock: a scan
    /// worker judging `WHERE n < nextval('q')` for its rows draws from the
    /// statement's sequences and records the values in its session. A
    /// task-local does not cross `task::spawn`, so the call failed there
    /// ("not available in this context").
    pub fn inherit<F: std::future::Future>(fut: F) -> impl std::future::Future<Output = F::Output> {
        let scope = SCOPE.try_with(Scope::clone).ok();
        async move {
            match scope {
                Some(scope) => SCOPE.scope(scope, fut).await,
                None => fut.await,
            }
        }
    }

    /// The scope's database, when a statement set one.
    pub fn database() -> Option<u64> {
        SCOPE.try_with(|s| s.database).ok()
    }

    pub(super) fn record(name: &str, value: i64) {
        let _ = SCOPE.try_with(|s| {
            if let Ok(mut values) = s.values.lock() {
                values.by_name.insert(name.to_string(), value);
                values.last = Some(value);
            }
        });
    }

    /// `currval(name)`: 55000 before this session took a value from it.
    pub fn current(name: &str) -> Result<i64, String> {
        SCOPE
            .try_with(|s| s.values.lock().ok().and_then(|v| v.by_name.get(name).copied()))
            .ok()
            .flatten()
            .ok_or_else(|| format!("currval of sequence \"{name}\" is not yet defined in this session"))
    }

    /// `lastval()`: 55000 before this session took any value.
    pub fn last() -> Result<i64, String> {
        SCOPE
            .try_with(|s| s.values.lock().ok().and_then(|v| v.last))
            .ok()
            .flatten()
            .ok_or_else(|| "lastval is not yet defined in this session".to_string())
    }
}

/// The sequence a `nextval`/`currval`/`setval` argument names, as PostgreSQL
/// reads a `regclass` from text: a `::regclass` cast dropped, a schema
/// qualifier dropped, a quoted name kept as written and an unquoted one
/// lowercased.
pub fn sequence_name_of(argument: &str) -> String {
    let text = argument.trim();
    let text = text.split("::").next().unwrap_or(text).trim();
    let last = crate::core::sql_scan::last_name_segment(text);
    if last.starts_with('"') && last.ends_with('"') && last.len() >= 2 {
        last[1..last.len() - 1].replace("\"\"", "\"")
    } else {
        last.to_ascii_lowercase()
    }
}

/// Reads a `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( options ) ]`
/// clause out of a column definition: the definition without it, and the
/// kind. `Ok((definition, None))` when the definition has none.
///
/// An identity column is built as a `SERIAL` is: `NOT NULL`, defaulting to
/// `nextval` of the sequence `<table>_<column>_seq`. The sequence options
/// PostgreSQL defaults to are accepted as written; any other is refused by
/// name, as the counter here always starts above the column's largest value
/// and steps by one.
pub fn split_identity_clause(
    definition: &str,
) -> Result<(String, Option<crate::core::rql::column_default::IdentityKind>), String> {
    use crate::core::rql::column_default::IdentityKind;
    use crate::core::rql::join::find_keyword;
    let Some(at) = find_keyword(definition, "GENERATED", 0) else {
        return Ok((definition.to_string(), None));
    };
    let mut words = WordReader { text: definition, at: at + "GENERATED".len() };
    let kind = if words.eat("ALWAYS") {
        IdentityKind::Always
    } else if words.eat("BY") {
        if !words.eat("DEFAULT") {
            return Ok((definition.to_string(), None));
        }
        IdentityKind::ByDefault
    } else {
        return Ok((definition.to_string(), None));
    };
    if !words.eat("AS") || !words.eat("IDENTITY") {
        // `GENERATED ALWAYS AS (…) STORED`, which `generated::split_clause`
        // reads.
        return Ok((definition.to_string(), None));
    }
    let mut end = words.at;
    let rest = &definition[end..];
    let skipped = rest.len() - rest.trim_start().len();
    if rest.trim_start().starts_with('(') {
        let open = end + skipped;
        let close = crate::core::sql_scan::matching_close(definition, open)
            .ok_or_else(|| "syntax error in the identity options".to_string())?;
        identity_options(&definition[open + 1..close])?;
        end = close + 1;
    }
    let mut out = String::with_capacity(definition.len());
    out.push_str(definition[..at].trim_end());
    out.push(' ');
    out.push_str(definition[end..].trim_start());
    Ok((out.trim_end().to_string(), Some(kind)))
}

/// Checks the options of an identity column's sequence: those that restate
/// what this sequence does are accepted, any other is refused by name.
fn identity_options(options: &str) -> Result<(), String> {
    let words: Vec<String> = options.split_whitespace().map(|w| w.to_ascii_uppercase()).collect();
    let refuse = |what: &str| -> Result<(), String> {
        Err(format!(
            "identity option {what} is not supported: an identity column here starts at 1 (above the \
             column's largest value) and increments by 1"
        ))
    };
    let number = |w: Option<&String>| -> Option<i64> { w.and_then(|w| w.parse::<i64>().ok()) };
    let mut i = 0;
    while i < words.len() {
        match words[i].as_str() {
            "START" => {
                i += 1;
                if words.get(i).map(String::as_str) == Some("WITH") {
                    i += 1;
                }
                if number(words.get(i)) != Some(1) {
                    return refuse(&format!("START WITH {}", words.get(i).map(String::as_str).unwrap_or("")));
                }
                i += 1;
            }
            "INCREMENT" => {
                i += 1;
                if words.get(i).map(String::as_str) == Some("BY") {
                    i += 1;
                }
                if number(words.get(i)) != Some(1) {
                    return refuse(&format!("INCREMENT BY {}", words.get(i).map(String::as_str).unwrap_or("")));
                }
                i += 1;
            }
            "MINVALUE" => {
                if number(words.get(i + 1)) != Some(1) {
                    return refuse(&format!("MINVALUE {}", words.get(i + 1).map(String::as_str).unwrap_or("")));
                }
                i += 2;
            }
            "NO" => match words.get(i + 1).map(String::as_str) {
                Some("MINVALUE" | "MAXVALUE" | "CYCLE") => i += 2,
                other => return refuse(&format!("NO {}", other.unwrap_or(""))),
            },
            // Values are handed out one at a time from memory; a cache is
            // only how many PostgreSQL reserves at once.
            "CACHE" if number(words.get(i + 1)).is_some_and(|n| n >= 1) => i += 2,
            "MAXVALUE" if number(words.get(i + 1)).is_some() => i += 2,
            other => return refuse(other),
        }
    }
    Ok(())
}

/// Whether a column type can be an identity column's: PostgreSQL allows the
/// three integer widths only.
pub fn identity_type_ok(db_type: &crate::core::db_type::DbType) -> bool {
    use crate::core::db_type::DbType;
    matches!(db_type, DbType::I16 | DbType::I32 | DbType::I64)
}

/// Reads keywords one after another, skipping whitespace.
struct WordReader<'a> {
    text: &'a str,
    at: usize,
}

impl WordReader<'_> {
    fn eat(&mut self, word: &str) -> bool {
        let rest = &self.text[self.at..];
        let start = self.at + (rest.len() - rest.trim_start().len());
        let tail = &self.text[start..];
        let end = start + word.len();
        let fits = tail.len() >= word.len() && tail.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes());
        let bounded = self
            .text
            .as_bytes()
            .get(end)
            .is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'));
        if fits && bounded {
            self.at = end;
            true
        } else {
            false
        }
    }
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
        assert_eq!(
            parse_nextval("nextval('public.t_id_seq'::regclass)").as_deref(),
            Some("t_id_seq")
        );
        assert_eq!(
            parse_nextval("nextval('public.\"T id\"'::regclass)").as_deref(),
            Some("T id")
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

    /// A floor marked under one spelling of a table is that table's in
    /// every spelling, and a forget under another spelling takes it.
    ///
    /// Mutation: `floor_key` or `forget_table_floors` keyed by the spelling
    /// (the forget misses the mark).
    #[test]
    fn a_floor_mark_is_the_tables_in_any_case() {
        let name = "floor_case_seq";
        mark_floored(DB, name, "ACCT");
        assert!(is_floored(DB, name, "Acct"));
        forget_table_floors(DB, "Acct");
        assert!(!is_floored(DB, name, "ACCT"), "a mark under another spelling outlived the table's forget");
    }

    /// A restart is its transaction's until an insert finds that ended: no
    /// floor is marked meanwhile, and afterwards marks are taken again (a
    /// sequence never settled would be seeded by a scan at every insert).
    ///
    /// Mutations: `settle_restarts` that never clears; `mark_floored`
    /// without its pending-restart guard.
    #[test]
    fn a_restart_is_pending_until_its_transaction_ends() {
        let name = "restart_test_seq";
        seed(DB, name, 40);
        mark_floored(DB, name, "rt");
        restart(DB, name, 7);
        assert_eq!(next(DB, name), 1, "the counter did not restart");
        assert!(!is_floored(DB, name, "rt"), "the old floor outlived the restart");
        assert!(restarted_by(DB, name, 7) && !restarted_by(DB, name, 8));
        mark_floored(DB, name, "rt");
        assert!(!is_floored(DB, name, "rt"), "a floor was marked while the restart could still roll back");
        settle_restarts(DB, |tx| tx != 7);
        assert!(restarted_by(DB, name, 7), "settled while its transaction was still running");
        settle_restarts(DB, |tx| tx == 7);
        assert!(!restarted_by(DB, name, 7));
        mark_floored(DB, name, "rt");
        assert!(is_floored(DB, name, "rt"), "no floor is marked after the restart ended");
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

    /// A counter another table seeded from lower rows is raised to the
    /// second table's floor, never lowered by a seed, and a forget or a
    /// rename takes every table's floor mark with the counter.
    ///
    /// Mutation: `seed` without the `fetch_max` (`next` hands out 1).
    #[test]
    fn a_second_tables_floor_raises_a_seeded_counter() {
        let name = "floor_test_seq";
        assert!(seed(DB, name, 0));
        mark_floored(DB, name, "sb");
        assert!(is_floored(DB, name, "sb"));
        assert!(!is_floored(DB, name, "sa"), "one table's floor counted as another's");
        assert!(!seed(DB, name, 5), "a seeded counter was reported as seeded again");
        mark_floored(DB, name, "sa");
        assert_eq!(next(DB, name), 6, "the counter stayed below the second table's floor");
        assert!(!seed(DB, name, 2));
        assert_eq!(next(DB, name), 7, "a lower floor moved the counter back");
        assert!(!is_floored(OTHER_DB, name, "sa"));

        rename(DB, name, "floor_test_renamed_seq");
        assert!(!is_floored(DB, name, "sa"), "a renamed-away counter kept its floor marks");
        assert_eq!(next(DB, "floor_test_renamed_seq"), 8);
        mark_floored(DB, "floor_test_renamed_seq", "sa");
        forget(DB, &["floor_test_renamed_seq".to_string()]);
        assert!(!is_floored(DB, "floor_test_renamed_seq", "sa"), "a forgotten counter kept its floor marks");
    }

    /// A dropped or renamed table's floor marks go, on every sequence, and
    /// only its own: not another table's, not one in another database, not
    /// a table whose name ends in its name.
    ///
    /// Mutation: `forget_table_floors` a no-op.
    #[test]
    fn a_tables_floor_marks_go_with_its_name() {
        let (one, two) = ("ftf_one_seq", "ftf_two_seq");
        for (db, seq, table) in [(DB, one, "a"), (DB, two, "a"), (DB, one, "xa"), (DB, one, "b"), (OTHER_DB, one, "a")] {
            seed(db, seq, 0);
            mark_floored(db, seq, table);
        }
        forget_table_floors(DB, "a");
        assert!(!is_floored(DB, one, "a") && !is_floored(DB, two, "a"), "a mark outlived its table's name");
        assert!(is_floored(DB, one, "xa"), "a table whose name ends in the name lost its mark");
        assert!(is_floored(DB, one, "b"));
        assert!(is_floored(OTHER_DB, one, "a"), "another database's table lost its mark");
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
            identity: None,
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
