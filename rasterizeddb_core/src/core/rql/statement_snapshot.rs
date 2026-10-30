//! One snapshot for every read a read-only statement makes.
//!
//! A statement is answered in pieces here: an `IN (SELECT …)`, `EXISTS` or
//! scalar subquery runs first and is substituted as literals, a CTE is
//! materialised into a temporary relation, and only then does the outer query
//! read. Each piece used to take a snapshot of its own, so under READ
//! COMMITTED a transaction committing between two pieces showed in one and
//! not the other: a row moved from `b` to `a` appeared in both, and the
//! statement reported a state the database was never in. PostgreSQL reads a
//! whole statement under one snapshot.
//!
//! Inside [`scope`], the first versioned read takes the snapshot and every
//! later read reuses it, whichever transaction id it reads under. They differ
//! within one statement: a subquery runs under the session's id (`FROZEN_TX`
//! outside a block) and the outer query under an autocommit id of its own,
//! so a snapshot kept per id was never shared. A read-only statement writes
//! nothing those ids would need to see as their own except a CTE's temporary
//! relation, which is written under the transaction whose body read came
//! first, so that read's snapshot sees it, however late it was written.
//!
//! A read-only statement is scoped whole ([`is_read_only`]). A write is
//! scoped per attempt, and only one that reads before it writes (an `UPDATE`
//! or `DELETE` with a subquery, a `FROM`/`USING` source or a CTE): its
//! reads share the attempt's snapshot as a query's do, and its target rows
//! are scanned under that same snapshot ([`kept`]). Its subqueries were
//! read under one snapshot and its rows scanned under a later one, so a
//! transaction committing between them made `UPDATE t … WHERE NOT EXISTS
//! (… u.tid = t.id)` write a row no single moment had match (27 of 300
//! statements under a writer moving a `t`/`u` pair; PostgreSQL 0 of 2,000).
//! A conflict retry is a new attempt and takes a new snapshot: one under
//! the first attempt's would find the same superseded versions and conflict
//! again. A write with nothing to read first takes its snapshot under the
//! table's write lock, as before.

use std::sync::Arc;

use crate::core::mvcc::Visibility;
use crate::core::mvcc::transaction::TransactionManager;

/// What one statement's reads hold: a snapshot per database (every
/// versioned table of one database shares its manager, so one serves them
/// all), and, inside a [`write_scope`], which tables were read under it.
#[derive(Default)]
struct Reads {
    snapshots: smallvec::SmallVec<[Visibility; 1]>,
    /// The `max_writer` of each plain table read, or `None` once a read
    /// could not name its table's (a partitioned table): then nothing is
    /// known about what changed since, and the target keeps the reads'
    /// snapshot ([`target_visibility`]).
    tables: Option<Vec<Arc<AtomicU64>>>,
}

use std::sync::atomic::{AtomicU64, Ordering};

tokio::task_local! {
    static READ: parking_lot::Mutex<Reads>;
    /// Present inside [`write_scope`]: only there is a write handed the
    /// snapshot its statement's reads took ([`target_visibility`]).
    static WRITE: ();
}

fn fresh_reads() -> parking_lot::Mutex<Reads> {
    parking_lot::Mutex::new(Reads { snapshots: smallvec::SmallVec::new(), tables: Some(Vec::new()) })
}

/// Runs `f` with one snapshot for all its reads.
///
/// Not an `async fn`, so `f` is stored once rather than once as an
/// argument and again where it is awaited: this wraps every statement, and
/// its future is copied whole each time it moves.
pub fn scope<F: std::future::Future>(f: F) -> impl std::future::Future<Output = F::Output> {
    READ.scope(fresh_reads(), f)
}

/// [`scope`] for an `UPDATE` or `DELETE` that reads before it writes: its
/// reads share one snapshot, and its target is scanned under it, or under a
/// later one when nothing it read can have changed since
/// ([`target_visibility`]). A write running inside a read-only statement's
/// [`scope`] (a test's hook between two partition reads, say) is not this
/// statement, and is never handed that statement's snapshot: scanned under
/// it, its rows conflicted with every version committed since.
pub fn write_scope<F: std::future::Future>(f: F) -> impl std::future::Future<Output = F::Output> {
    READ.scope(fresh_reads(), WRITE.scope((), f))
}

/// Whether the caller runs inside [`write_scope`].
pub(crate) fn in_write_scope() -> bool {
    WRITE.try_with(|_| ()).is_ok()
}

/// The statement's snapshot for a table whose transactions `manager` keeps,
/// taken with `take` on the first read. Outside [`scope`], `take`'s, every
/// time.
///
/// Only a snapshot is kept: `Visibility::All` is a table with no versions,
/// and whether the next table has them is its own question. A read of
/// another database's table (a role lookup, a catalogue copy) keeps that
/// database's snapshot beside this one's: kept one at a time, a first read
/// of another database left this one's reads unshared.
pub fn get_or_take(manager: &Arc<TransactionManager>, take: impl FnOnce() -> Visibility) -> Visibility {
    get_or_take_noting(manager, Noting::Unknown, take)
}

/// What a read tells a [`write_scope`] about its table.
pub enum Noting<'a> {
    /// A plain table, and its `max_writer`.
    Table(&'a Arc<AtomicU64>),
    /// A relation the statement materialised itself (a CTE or derived
    /// table's `__cte_` relation): its rows come from reads already noted.
    Derived,
    /// A table that cannot name its writers (a partitioned one).
    Unknown,
}

/// [`get_or_take`] saying what the read's table is ([`Noting`]): a
/// [`write_scope`] records a plain table's `max_writer` so its target can
/// tell whether anything it read was written since.
pub fn get_or_take_noting(
    manager: &Arc<TransactionManager>,
    noting: Noting<'_>,
    take: impl FnOnce() -> Visibility,
) -> Visibility {
    let writes = in_write_scope();
    let kept = READ.try_with(|read| {
        let mut read = read.lock();
        if writes {
            match (noting, read.tables.as_mut()) {
                (Noting::Table(writer), Some(tables)) => {
                    if !tables.iter().any(|t| Arc::ptr_eq(t, writer)) {
                        tables.push(writer.clone());
                    }
                }
                (Noting::Unknown, _) => read.tables = None,
                _ => {}
            }
        }
        read.snapshots.iter().find(|v| matches!(v, Visibility::AsOf { manager: m, .. } if Arc::ptr_eq(m, manager))).cloned()
    });
    match kept {
        Ok(Some(visibility)) => visibility,
        Ok(None) => {
            let visibility = take();
            if matches!(visibility, Visibility::AsOf { .. }) {
                let _ = READ.try_with(|read| read.lock().snapshots.push(visibility.clone()));
            }
            visibility
        }
        Err(_) => take(),
    }
}

/// The snapshot the enclosing [`write_scope`] holds for the database
/// `manager` keeps, if a read of the statement has taken one: what its
/// reads described. `None` outside a write scope (a read-only statement's
/// [`scope`] included), before any read, for another database's manager,
/// and for a write under a transaction the snapshot was not taken for:
/// another transaction run in the same task (a test's hook inside a
/// statement) scanned under it conflicted with every version committed
/// since.
pub(crate) fn kept(manager: &Arc<TransactionManager>, tx: crate::core::mvcc::TxId) -> Option<Visibility> {
    if !in_write_scope() {
        return None;
    }
    READ.try_with(|read| {
        read.lock()
            .snapshots
            .iter()
            .find(|v| matches!(v, Visibility::AsOf { manager: m, snapshot } if Arc::ptr_eq(m, manager) && snapshot.is_mine(tx)))
            .cloned()
    })
    .ok()
    .flatten()
}

/// The snapshot a write's target is scanned under when its statement read
/// first (`None`: it read nothing, or is not this scope's statement, and
/// takes its own under its lock).
///
/// `fresh`'s snapshot when nothing the statement read can have changed
/// since its reads' snapshot: every table it read was last written by a
/// transaction older than that snapshot's oldest running one, so no commit
/// since can show there, and the reads hold at the fresh snapshot as they
/// held at theirs. The fresh one is taken first and the writers are checked
/// after it: a commit it sees was preceded by its writer's `note_write`. So
/// a conflict on the target alone, an incrementer of the same row, replays
/// as it does for a plain write, without reading the subqueries again
/// (they were at 91-100% 40001 under a tight-loop incrementer). Otherwise
/// the reads' snapshot, which keeps the two halves one moment.
pub fn target_visibility(
    manager: &Arc<TransactionManager>,
    tx: crate::core::mvcc::TxId,
    fresh: impl FnOnce() -> Visibility,
) -> Option<Visibility> {
    let read = kept(manager, tx)?;
    let Visibility::AsOf { snapshot, .. } = &read else { return Some(read) };
    let tables = READ.try_with(|r| r.lock().tables.clone()).ok().flatten();
    let Some(tables) = tables else { return Some(read) };
    let now = fresh();
    if tables.iter().all(|writer| writer.load(Ordering::SeqCst) < snapshot.xmin) {
        Some(now)
    } else {
        Some(read)
    }
}

/// Whether the caller runs inside [`scope`]. For the tests that pin which
/// paths read a statement under one snapshot.
#[cfg(test)]
pub(crate) fn in_scope() -> bool {
    READ.try_with(|_| ()).is_ok()
}

/// Whether `sql` only reads, so its pieces may share one snapshot.
///
/// Conservative: a statement opening with `SELECT`, `VALUES`, `TABLE` or
/// `WITH`, with no `INSERT`, `UPDATE`, `DELETE`, `MERGE` or `INTO` and no
/// locking clause (`FOR UPDATE`, `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY
/// SHARE`) anywhere outside a quoted string or identifier. A column named
/// `update` costs only the sharing, never correctness.
pub fn is_read_only(sql: &str) -> bool {
    // The first word alone settles every statement that is not a read, which
    // is every write this asks about, without a `String` per word of it.
    let Some(first) = first_word_outside_quotes(sql) else {
        return false;
    };
    if !["SELECT", "VALUES", "TABLE", "WITH"].contains(&first.as_str()) {
        return false;
    }
    let words = words_outside_quotes(sql);
    let writes = ["INSERT", "UPDATE", "DELETE", "MERGE", "INTO"];
    if words.iter().any(|w| writes.contains(&w.as_str())) {
        return false;
    }
    !words
        .windows(2)
        .any(|w| w[0] == "FOR" && ["SHARE", "NO", "KEY"].contains(&w[1].as_str()))
}

/// Whether `sql` is an `UPDATE` or a `DELETE` part of whose reading the wire
/// does before the engine runs it: a CTE body, or a derived table (`FROM
/// (SELECT …) s`), each materialised first. The whole statement is then
/// scoped, so those reads and the write's own share one snapshot. A conflict
/// retry of the write runs under it as well, since the materialised rows
/// cannot be read again: replayed only when the transaction it met aborted,
/// and reported (40001) when that one committed, as a `REPEATABLE READ`
/// write's conflict is. A write whose reads all run inside the engine (a
/// subquery, a `FROM`/`USING` table) is scoped per attempt there instead.
pub fn is_write_read_at_the_wire(sql: &str) -> bool {
    // Neither a CTE body nor a derived table without the word SELECT; most
    // writes have none, and are answered without a scan.
    if !mentions_select(sql) {
        return false;
    }
    let with = crate::core::rql::cte::has_with_clause(sql);
    let split;
    let outer = if with {
        match crate::core::rql::cte::split_with(sql) {
            Ok(Some((_, outer))) => {
                split = outer;
                split.as_str()
            }
            _ => return false,
        }
    } else {
        sql
    };
    if !matches!(first_word_outside_quotes(outer).as_deref(), Some("UPDATE" | "DELETE")) {
        return false;
    }
    with || crate::core::rql::cte::next_derived_table(outer).is_some()
}

/// Whether `sql` holds the word `SELECT` anywhere (in any case, quoted or
/// not): without it, no subquery, CTE body or derived table. A cheap
/// filter in front of the scanners that look for those.
pub fn mentions_select(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    memchr::memchr2_iter(b's', b'S', bytes)
        .any(|at| bytes.get(at..at + 6).is_some_and(|w| w.eq_ignore_ascii_case(b"SELECT")))
}

/// The first of [`words_outside_quotes`], found without the rest.
fn first_word_outside_quotes(sql: &str) -> Option<String> {
    let mut word = String::new();
    let mut quote: Option<char> = None;
    for c in sql.chars() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            word.push(c.to_ascii_uppercase());
            continue;
        }
        if !word.is_empty() {
            return Some(word);
        }
        if c == '\'' || c == '"' {
            quote = Some(c);
        }
    }
    (!word.is_empty()).then_some(word)
}

/// The bare words of `sql`, upper-cased, skipping quoted strings and
/// identifiers.
fn words_outside_quotes(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    for c in sql.chars() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            word.push(c.to_ascii_uppercase());
            continue;
        }
        if !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        if c == '\'' || c == '"' {
            quote = Some(c);
        }
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kept_by(visibility: &Visibility) -> *const TransactionManager {
        match visibility {
            Visibility::AsOf { manager, .. } => Arc::as_ptr(manager),
            Visibility::All => std::ptr::null(),
        }
    }

    /// A statement's snapshot is one database's: a read of another
    /// database's table inside the same statement takes that database's own
    /// snapshot, and the statement's stays for its own tables.
    ///
    /// Mutation: the `Arc::ptr_eq` guard always true (the other database
    /// reads under this one's snapshot).
    #[tokio::test]
    async fn a_snapshot_serves_only_its_own_database() {
        let (here, there) = (Arc::new(TransactionManager::in_memory()), Arc::new(TransactionManager::in_memory()));
        let take = |m: &Arc<TransactionManager>| {
            let m = m.clone();
            move || Visibility::for_snapshot(m.snapshot(crate::core::mvcc::transaction::FROZEN_TX), m)
        };
        scope(async {
            let first = get_or_take(&here, take(&here));
            assert_eq!(kept_by(&first), Arc::as_ptr(&here));
            let other = get_or_take(&there, take(&there));
            assert_eq!(kept_by(&other), Arc::as_ptr(&there), "read another database under this one's snapshot");
            let again = get_or_take(&here, || panic!("the statement's snapshot was replaced"));
            assert_eq!(kept_by(&again), Arc::as_ptr(&here));
        })
        .await;
    }

    /// A write scans under the snapshot its statement's reads took, and
    /// only once one has: before any read, outside a scope, and for another
    /// database there is none, and the write takes its own.
    ///
    /// Mutation: `kept` returning the snapshot without the `Arc::ptr_eq`
    /// guard (another database's write scans under this one's).
    #[tokio::test]
    async fn a_write_is_handed_its_reads_snapshot_and_no_other() {
        let (here, there) = (Arc::new(TransactionManager::in_memory()), Arc::new(TransactionManager::in_memory()));
        let (tx, other) = (here.begin(), here.begin());
        let take = |m: &Arc<TransactionManager>| {
            let m = m.clone();
            move || Visibility::for_snapshot(m.snapshot(tx), m)
        };
        assert!(kept(&here, tx).is_none(), "outside a scope");
        scope(async {
            let _ = get_or_take(&here, take(&here));
            assert!(kept(&here, tx).is_none(), "a read-only statement's snapshot was handed to a write inside it");
        })
        .await;
        write_scope(async {
            assert!(kept(&here, tx).is_none(), "before any read");
            let read = get_or_take(&here, take(&here));
            let write = kept(&here, tx).expect("the read's snapshot");
            assert!(kept(&here, other).is_none(), "another transaction's write was handed this one's snapshot");
            match (&read, &write) {
                (Visibility::AsOf { snapshot: r, .. }, Visibility::AsOf { snapshot: w, .. }) => {
                    assert_eq!((r.xmin, r.xmax), (w.xmin, w.xmax), "the write scans under another snapshot");
                }
                _ => panic!("a versioned read"),
            }
            assert!(kept(&there, tx).is_none(), "another database's write was handed this one's snapshot");
        })
        .await;
    }

    /// The target is scanned at a fresh snapshot only when no table the
    /// statement read was written by a transaction its reads' snapshot could
    /// not see; otherwise, and when a read could not name its table, under
    /// the reads' own.
    ///
    /// Mutations: the writer check always passing (`all` -> true); a
    /// `Noting::Unknown` read not clearing the table list.
    #[tokio::test]
    async fn the_target_moves_on_only_when_nothing_read_was_written_since() {
        let m = Arc::new(TransactionManager::in_memory());
        let tx = m.begin();
        let take = |m: &Arc<TransactionManager>| {
            let m = m.clone();
            move || Visibility::for_snapshot(m.snapshot(tx), m)
        };
        let xmax = |v: &Visibility| match v {
            Visibility::AsOf { snapshot, .. } => snapshot.xmax,
            Visibility::All => 0,
        };
        let u = Arc::new(AtomicU64::new(1));
        write_scope(async {
            let read = get_or_take_noting(&m, Noting::Table(&u), take(&m));
            let later = m.begin();
            let moved = target_visibility(&m, tx, take(&m)).expect("a target snapshot");
            assert!(xmax(&moved) > xmax(&read), "nothing read changed, yet the target kept the reads' snapshot");
            // A writer of the table read that the reads' snapshot could not see.
            u.store(later, Ordering::SeqCst);
            let kept = target_visibility(&m, tx, take(&m)).expect("a target snapshot");
            assert_eq!(xmax(&kept), xmax(&read), "a table read was written since, yet the target moved on");
        })
        .await;
        let u = Arc::new(AtomicU64::new(1));
        write_scope(async {
            let read = get_or_take_noting(&m, Noting::Table(&u), take(&m));
            let _ = get_or_take_noting(&m, Noting::Unknown, take(&m));
            let _later = m.begin();
            let kept = target_visibility(&m, tx, take(&m)).expect("a target snapshot");
            assert_eq!(xmax(&kept), xmax(&read), "a read that named no table, yet the target moved on");
        })
        .await;
    }

    #[test]
    fn writes_read_at_the_wire_are_cte_or_derived_fed() {
        for sql in [
            "WITH p AS (SELECT tid FROM u) DELETE FROM t WHERE id IN (SELECT tid FROM p)",
            "with p as (select 1) update t set v = 1",
            "UPDATE t SET v = s.v FROM (SELECT id, v FROM u) s WHERE s.id = t.id",
            "DELETE FROM t USING u JOIN (SELECT id FROM w) d ON d.id = u.id WHERE u.id = t.id",
        ] {
            assert!(is_write_read_at_the_wire(sql), "{sql}");
        }
        for sql in [
            "UPDATE t SET v = 1 WHERE id IN (SELECT tid FROM u)",
            "DELETE FROM t USING u WHERE u.id = t.id",
            "WITH p AS (SELECT 1) SELECT * FROM p",
            "WITH p AS (SELECT 1) INSERT INTO t SELECT * FROM p",
            "SELECT * FROM (SELECT 1) s",
            "UPDATE t SET v = 1",
        ] {
            assert!(!is_write_read_at_the_wire(sql), "{sql}");
        }
    }

    #[test]
    fn reads_are_read_only() {
        for sql in [
            "SELECT id FROM a WHERE id IN (SELECT id FROM b)",
            "select 1",
            "WITH m AS (SELECT id FROM b) SELECT * FROM a JOIN m ON m.id = a.id",
            "VALUES (1), (2)",
            "SELECT 'insert into t' AS note, \"update\" FROM t",
            "SELECT * FROM t FOR_X",
        ] {
            assert!(is_read_only(sql), "{sql}");
        }
    }

    #[test]
    fn writes_and_locking_reads_are_not() {
        for sql in [
            "UPDATE t SET a = 1",
            "WITH d AS (DELETE FROM t RETURNING id) SELECT * FROM d",
            "WITH i AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM i",
            "SELECT * INTO copy FROM t",
            "SELECT * FROM t WHERE id = 1 FOR UPDATE",
            "SELECT * FROM t FOR NO KEY UPDATE",
            "SELECT * FROM t FOR SHARE SKIP LOCKED",
            "SELECT * FROM t FOR KEY SHARE",
            "CREATE TABLE t (a INT)",
            "",
        ] {
            assert!(!is_read_only(sql), "{sql}");
        }
    }

    #[test]
    fn the_first_word_is_the_first_of_the_words() {
        for sql in [
            "SELECT 1",
            "  update t set a = 1",
            "'quoted' SELECT",
            "\"ident\" with",
            "",
            "   ",
            "(SELECT 1)",
            "ünïcödé FROM t",
            "a",
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
        ] {
            assert_eq!(first_word_outside_quotes(sql), words_outside_quotes(sql).into_iter().next(), "{sql:?}");
        }
    }
}
