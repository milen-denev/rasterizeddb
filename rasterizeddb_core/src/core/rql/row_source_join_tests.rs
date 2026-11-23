//! Joins over partitioned tables (S19, `docs/partitioning.md` §8): every
//! relation and every partition read under the join's one snapshot (I7),
//! each partitioned relation routed only by its own pushed-down predicate
//! (I3), NULL-extended outer rows with their NULL companions, and a
//! partitioned source of `UPDATE … FROM` / `DELETE … USING` (a partitioned
//! target stays refused, phase 3).
//!
//! Seen through the fan-out's test hooks (`set_after_first`, `take_reads`)
//! rather than timing, and against plain twins holding the same rows.
//! Bounded: fixed row and statement counts.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicI64, Ordering};

use rclite::Arc;

use super::test_hooks;
use super::tests::{close, exec, exec_ok, materialized, open, partitioned, pid_of, rows};
use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::row::Row;
use crate::core::rql::lexer_s1::recognize_query_purpose;

const TENANTS: i64 = 16;
const OPENING: i64 = 100;
/// Owners with no account, for the outer joins.
const ORPHANS: [i64; 3] = [100, 101, 102];

/// A cell as the test reads it: `None` for NULL (by type, as the join
/// marks it), else the 8-byte integer it holds, or its text.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Null,
    Int(i64),
    Text(String),
}

fn cells(rows: &[Row]) -> Vec<Vec<Cell>> {
    rows.iter()
        .map(|r| {
            r.columns
                .iter()
                .map(|c| {
                    let bytes = c.data.into_slice();
                    if c.column_type == DbType::NULL {
                        Cell::Null
                    } else if c.column_type == DbType::STRING {
                        Cell::Text(String::from_utf8_lossy(bytes).into_owned())
                    } else if bytes.len() == 8 {
                        Cell::Int(i64::from_le_bytes(bytes.try_into().unwrap()))
                    } else if c.column_type == DbType::DECIMAL && bytes.len() == 16 {
                        // A SUM over BIGINT: the integer, unscaled.
                        Cell::Int(i128::from_le_bytes(bytes.try_into().unwrap()) as i64)
                    } else {
                        Cell::Text(format!("{:?}:{bytes:?}", c.column_type))
                    }
                })
                .collect()
        })
        .collect()
}

async fn table(db: &Arc<Database>, sql: &str) -> Vec<Vec<Cell>> {
    cells(&rows(db, sql).await)
}

/// The one row of an aggregate, as integers.
async fn ints_row(db: &Arc<Database>, sql: &str) -> Vec<i64> {
    let got = table(db, sql).await;
    assert_eq!(got.len(), 1, "{sql}: {got:?}");
    got[0]
        .iter()
        .map(|c| match c {
            Cell::Int(v) => *v,
            other => panic!("{sql}: not an integer: {other:?}"),
        })
        .collect()
}

/// `name`: one row per tenant `1..=TENANTS`, balance `OPENING`, and a
/// nullable `lim` that is NULL for odd tenants. Partitioned by tenant into
/// `partitions`, or plain when `partitions` is 0.
async fn accounts(db: &Arc<Database>, name: &str, partitions: u32) {
    let clause =
        if partitions == 0 { String::new() } else { format!(" PARTITION BY HASH (tenant_id) PARTITIONS {partitions}") };
    exec_ok(
        db,
        &format!("CREATE TABLE {name} (tenant_id BIGINT NOT NULL, bal BIGINT NOT NULL, lim BIGINT, PRIMARY KEY (tenant_id)){clause}"),
    )
    .await;
    let values: Vec<String> = (1..=TENANTS)
        .map(|t| format!("({t}, {OPENING}, {})", if t % 2 == 1 { "NULL".to_string() } else { (t * 10).to_string() }))
        .collect();
    exec_ok(db, &format!("INSERT INTO {name} (tenant_id, bal, lim) VALUES {}", values.join(", "))).await;
}

/// The plain `owners`: every tenant, plus [`ORPHANS`] that hold no account.
async fn owners(db: &Arc<Database>) {
    exec_ok(db, "CREATE TABLE owners (tenant_id BIGINT NOT NULL PRIMARY KEY, name TEXT)").await;
    let values: Vec<String> =
        (1..=TENANTS).chain(ORPHANS).map(|t| format!("({t}, 'owner {t}')")).collect();
    exec_ok(db, &format!("INSERT INTO owners (tenant_id, name) VALUES {}", values.join(", "))).await;
}

/// A debit tenant in the partition every fan-out over `pt` reads first, and
/// a credit tenant in another.
fn debit_and_credit(pt: &PartitionedTable) -> (i64, i64) {
    let parts = materialized(pt);
    assert!(parts.len() >= 2, "the tenants reach one partition only: {parts:?}");
    let debit = (1..=TENANTS).find(|t| pid_of(pt, *t) == parts[0]).unwrap();
    let credit = (1..=TENANTS).find(|t| pid_of(pt, *t) != parts[0]).unwrap();
    (debit, credit)
}

/// Runs `sqls` in one transaction and commits it.
async fn in_one_transaction(db: &Arc<Database>, sqls: &[String]) {
    let tx = db.transactions.begin();
    for sql in sqls {
        let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
        let out = crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx).await;
        assert!(out.first().is_some_and(|b| *b != 3), "{sql} failed: {}", String::from_utf8_lossy(&out));
    }
    db.commit_written_transaction(tx).expect("commit");
    db.transactions.flush().expect("flush the outcome");
}

/// I7 for joins, through `execute()`, so outside any `statement_snapshot`
/// scope: the join's own snapshot is the only thing holding its relations
/// together. Between the first and second partition reads of every fan-out
/// over `acct`, one transaction commits, every time:
///
/// - `acct`: 2 out of the debit tenant (in the partition read first), 1 into
///   the credit tenant (in one read after the hook);
/// - `bank`, a second partitioned table: 1 into the credit tenant.
///
/// So `acct` loses 1 and `bank` gains 1 per transfer. Under one snapshot:
/// `acct ⋈ owners` sums to the opening total less the transfers committed
/// before the statement; the self-join's two sides agree row for row; and
/// `acct ⋈ bank` keeps the two tables' combined total.
///
/// Mutations: `FanOut::plan_with` taking its own snapshot
/// (`read_visibility_for`) instead of the join's: the self-join's sides and
/// `acct ⋈ bank` disagree. A snapshot per partition (`FanOut::each`):
/// `acct ⋈ owners` sees the credit without the debit.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_never_sees_a_cross_partition_transfer_half_applied() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "join_i7").await;
    accounts(&db, "acct", 7).await;
    accounts(&db, "bank", 5).await;
    owners(&db).await;
    let pt = partitioned(&db, "acct");
    let (debit, credit) = debit_and_credit(&pt);

    let committed = StdArc::new(AtomicI64::new(0));
    let hook: test_hooks::Hook = {
        let (db, committed) = (db.clone(), committed.clone());
        StdArc::new(move || {
            let (db, committed) = (db.clone(), committed.clone());
            Box::pin(async move {
                // Each a routed, one-partition write: no fan-out of its own,
                // so the hook never fires inside itself.
                in_one_transaction(
                    &db,
                    &[
                        format!("UPDATE acct SET bal = bal - 2 WHERE tenant_id = {debit}"),
                        format!("UPDATE acct SET bal = bal + 1 WHERE tenant_id = {credit}"),
                        format!("UPDATE bank SET bal = bal + 1 WHERE tenant_id = {credit}"),
                    ],
                )
                .await;
                committed.fetch_add(1, Ordering::SeqCst);
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));

    let total = TENANTS * OPENING;
    for round in 0..6i64 {
        // Partitioned × plain, an aggregate over the join.
        let before = committed.load(Ordering::SeqCst);
        let got = ints_row(&db, "SELECT SUM(a.bal), COUNT(*) FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id").await;
        assert_eq!(got, vec![total - before, TENANTS], "round {round}: acct ⋈ owners saw a transfer in part");
        assert_eq!(committed.load(Ordering::SeqCst), before + 1, "round {round}: the hook did not run");

        // A self-join: two fan-outs over one table, one snapshot.
        let before = committed.load(Ordering::SeqCst);
        let got = table(&db, "SELECT a.tenant_id, a.bal, b.bal FROM acct a JOIN acct b ON a.tenant_id = b.tenant_id").await;
        assert_eq!(got.len(), TENANTS as usize, "round {round}: {got:?}");
        let torn: Vec<_> = got.iter().filter(|r| r[1] != r[2]).collect();
        assert!(torn.is_empty(), "round {round}: the self-join's two sides read two snapshots: {torn:?}");
        let sum: i64 = got.iter().map(|r| if let Cell::Int(v) = r[1] { v } else { panic!("{r:?}") }).sum();
        assert_eq!(sum, total - before, "round {round}: the self-join saw a transfer committed after it began");
        assert_eq!(committed.load(Ordering::SeqCst), before + 2, "round {round}: the hook did not run on both sides");

        // Partitioned × partitioned: the combined total holds.
        let before = committed.load(Ordering::SeqCst);
        let got =
            ints_row(&db, "SELECT SUM(a.bal), SUM(b.bal) FROM acct a JOIN bank b ON a.tenant_id = b.tenant_id").await;
        assert_eq!(got[0] + got[1], 2 * total, "round {round}: acct ⋈ bank read the two tables at two moments: {got:?}");
        assert_eq!(got, vec![total - before, total + before], "round {round}: acct ⋈ bank");
        assert_eq!(committed.load(Ordering::SeqCst), before + 1, "round {round}: the hook did not run");
    }
    test_hooks::set_after_first(pt.uid(), None);

    let n = committed.load(Ordering::SeqCst);
    let got = ints_row(&db, "SELECT SUM(a.bal), SUM(b.bal) FROM acct a JOIN bank b ON a.tenant_id = b.tenant_id").await;
    assert_eq!(got, vec![total - n, total + n]);
    drop(pt);
    close(db, dir.path());
}

/// I3 for joins, seen from the reads. A join whose `WHERE` pins the
/// partitioned side's key reads that one partition; a key pinned through an
/// `IN` list reads its partitions; a constant-false clause on it reads none.
/// A key pinned only on the *other* side (`o.tenant_id = 5`) routes nothing
/// on the partitioned side: no transitive inference in 2b, so it fans out
/// over every materialized partition (and still answers right).
///
/// Mutation: route the fan-out on an empty clause instead of the relation's
/// own pushed-down predicate: the pinned join reads every partition.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_reads_only_the_partitions_its_own_predicate_proves() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "join_i3").await;
    accounts(&db, "acct", 64).await;
    owners(&db).await;
    let pt = partitioned(&db, "acct");
    let uid = pt.uid();
    let parts = materialized(&pt);
    assert!(parts.len() > 2 && parts.len() < 64, "{parts:?}");
    test_hooks::take_reads(uid);

    let got = table(
        &db,
        "SELECT a.tenant_id, a.bal, o.name FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id WHERE a.tenant_id = 5",
    )
    .await;
    assert_eq!(got, vec![vec![Cell::Int(5), Cell::Int(OPENING), Cell::Text("owner 5".into())]]);
    assert_eq!(test_hooks::take_reads(uid), vec![pid_of(&pt, 5)], "a pinned join read other partitions");

    // The partitioned side second, an ON term pinning it, a LEFT JOIN.
    let got = table(
        &db,
        "SELECT o.tenant_id, a.bal FROM owners o LEFT JOIN acct a ON a.tenant_id = o.tenant_id AND a.tenant_id = 6 \
         WHERE o.tenant_id IN (6, 7)",
    )
    .await;
    let mut got = got;
    got.sort();
    assert_eq!(got, vec![vec![Cell::Int(6), Cell::Int(OPENING)], vec![Cell::Int(7), Cell::Null]]);
    assert_eq!(test_hooks::take_reads(uid), vec![pid_of(&pt, 6)], "an ON term pinning the key did not route");

    // An IN list: its tenants' partitions, by pid, once each.
    let got = table(
        &db,
        "SELECT a.tenant_id FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id WHERE a.tenant_id IN (3, 9, 3)",
    )
    .await;
    assert_eq!(got.len(), 2, "{got:?}");
    let mut want = vec![pid_of(&pt, 3), pid_of(&pt, 9)];
    want.sort();
    want.dedup();
    assert_eq!(test_hooks::take_reads(uid), want);

    // A constant-false clause: no partition, and the join still answers.
    let got = ints_row(
        &db,
        "SELECT COUNT(*) FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id WHERE a.tenant_id = 1 AND 1 = 0",
    )
    .await;
    assert_eq!(got, vec![0]);
    assert_eq!(test_hooks::take_reads(uid), Vec::<u32>::new(), "a constant-false clause read partitions");

    // Two equalities on the key: the route proves no more than either one
    // (as for a single table), so it reads their two partitions, no other.
    let got = ints_row(
        &db,
        "SELECT COUNT(*) FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id WHERE a.tenant_id = 1 AND a.tenant_id = 2",
    )
    .await;
    assert_eq!(got, vec![0]);
    let mut want = vec![pid_of(&pt, 1), pid_of(&pt, 2)];
    want.sort();
    want.dedup();
    assert_eq!(test_hooks::take_reads(uid), want);

    // The key pinned on the other side only: a fan-out, by design.
    let got = table(
        &db,
        "SELECT a.tenant_id, o.name FROM acct a JOIN owners o ON a.tenant_id = o.tenant_id WHERE o.tenant_id = 5",
    )
    .await;
    assert_eq!(got, vec![vec![Cell::Int(5), Cell::Text("owner 5".into())]]);
    assert_eq!(test_hooks::take_reads(uid), parts, "a join term naming the other side routed the partitioned one");

    // A self-join, each side pinned to its own tenant: two partitions.
    let got = table(
        &db,
        "SELECT a.tenant_id, b.tenant_id FROM acct a JOIN acct b ON a.bal = b.bal WHERE a.tenant_id = 2 AND b.tenant_id = 11",
    )
    .await;
    assert_eq!(got, vec![vec![Cell::Int(2), Cell::Int(11)]]);
    assert_eq!(test_hooks::take_reads(uid), vec![pid_of(&pt, 2), pid_of(&pt, 11)]);
    drop(pt);
    close(db, dir.path());
}

/// Outer joins with the partitioned side null-supplying, against a plain
/// twin holding the same rows. Owners without an account come back
/// NULL-extended; an account whose `lim` is NULL carries its NULL through
/// the join (its companion), so `COALESCE(a.lim, -1)` is `-1` there, never a
/// zero read as a value. A contradictory ON term on the partitioned side
/// NULL-extends every owner and reads only the partitions it names.
///
/// Mutation: drop the `__null_` companions from the partitioned relation's
/// requested columns: the NULL `lim`s read as present zeroes.
#[tokio::test(flavor = "multi_thread")]
async fn a_left_join_null_extends_and_keeps_null_companions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "join_left").await;
    accounts(&db, "acct", 7).await;
    accounts(&db, "acct_q", 0).await;
    owners(&db).await;
    let pt = partitioned(&db, "acct");
    let uid = pt.uid();

    let queries = [
        // Unmatched owners only.
        "SELECT o.tenant_id, o.name, a.bal, a.lim, a.tenant_id FROM owners o LEFT JOIN {a} a \
         ON a.tenant_id = o.tenant_id WHERE o.tenant_id >= 100",
        // Every owner, matched or not, with the NULL companions read.
        "SELECT o.tenant_id, a.lim, COALESCE(a.lim, -1), a.bal FROM owners o LEFT JOIN {a} a \
         ON a.tenant_id = o.tenant_id",
        // The anti-join.
        "SELECT o.tenant_id FROM owners o LEFT JOIN {a} a ON a.tenant_id = o.tenant_id WHERE a.tenant_id IS NULL",
        // Ordered and paged over the join, NULLs included.
        "SELECT o.tenant_id, a.lim FROM owners o LEFT JOIN {a} a ON a.tenant_id = o.tenant_id \
         ORDER BY a.lim DESC, o.tenant_id LIMIT 7 OFFSET 2",
        // Aggregates over the outer join.
        "SELECT COUNT(*), COUNT(a.lim), SUM(a.lim), MAX(a.bal) FROM owners o LEFT JOIN {a} a ON a.tenant_id = o.tenant_id",
    ];
    for q in queries {
        let p = table(&db, &q.replace("{a}", "acct")).await;
        let t = table(&db, &q.replace("{a}", "acct_q")).await;
        let ordered = q.contains("ORDER BY");
        let (mut ps, mut ts) = (p.clone(), t.clone());
        if !ordered {
            ps.sort();
            ts.sort();
        }
        assert_eq!(ps, ts, "`{q}`: the partitioned table answered unlike its twin");
        assert!(!p.is_empty(), "`{q}` answered nothing, so nothing was compared");
    }
    // The NULL companions, spelled out: an odd tenant's lim is NULL, and the
    // COALESCE sees it.
    let got = table(&db, "SELECT a.lim, COALESCE(a.lim, -1) FROM owners o LEFT JOIN acct a ON a.tenant_id = o.tenant_id WHERE o.tenant_id = 3").await;
    assert_eq!(got, vec![vec![Cell::Null, Cell::Int(-1)]], "a NULL lim did not come through the join as NULL");
    let got = table(&db, "SELECT a.lim FROM owners o LEFT JOIN acct a ON a.tenant_id = o.tenant_id WHERE o.tenant_id = 4").await;
    assert_eq!(got, vec![vec![Cell::Int(40)]]);

    // A contradiction on the null-supplying side: every owner
    // NULL-extended, reading at most the two partitions its equalities name.
    test_hooks::take_reads(uid);
    let got = table(
        &db,
        "SELECT o.tenant_id, a.bal, COALESCE(a.lim, -7) FROM owners o LEFT JOIN acct a \
         ON a.tenant_id = o.tenant_id AND a.tenant_id = 1 AND a.tenant_id = 2",
    )
    .await;
    assert_eq!(got.len(), TENANTS as usize + ORPHANS.len(), "{got:?}");
    assert!(got.iter().all(|r| r[1] == Cell::Null && r[2] == Cell::Int(-7)), "{got:?}");
    let read = test_hooks::take_reads(uid);
    assert!(
        read.iter().all(|p| *p == pid_of(&pt, 1) || *p == pid_of(&pt, 2)),
        "an ON term pinning the key read other partitions: {read:?}"
    );
    drop(pt);
    close(db, dir.path());
}

/// `UPDATE … FROM` / `DELETE … USING` with a partitioned source and a plain
/// target: the source is read by the join under its one snapshot, and the
/// target is written like the plain twin's.
///
/// - Against the twin: the same rows changed, the same tags.
/// - Under a transfer committing between two partition reads of the source
///   (debit tenant read first, credit after): the rows matched are those of
///   one snapshot, so exactly the credit tenant matches `bal = OPENING + k`.
///
/// Then a partitioned target, `UPDATE … FROM` and `DELETE … USING` both,
/// is written like the plain twin given the same rows (phase 3 P3; it was
/// refused with 0A000 until then).
///
/// Mutation: a snapshot per partition (`FanOut::each`): the update matches
/// no row (the credit seen one transfer late).
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_source_or_target_updates_and_deletes_like_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "join_upd").await;
    accounts(&db, "acct", 7).await;
    accounts(&db, "acct_q", 0).await;
    exec_ok(&db, "CREATE TABLE own_p (tenant_id BIGINT NOT NULL PRIMARY KEY, name TEXT, hits BIGINT)").await;
    exec_ok(&db, "CREATE TABLE own_q (tenant_id BIGINT NOT NULL PRIMARY KEY, name TEXT, hits BIGINT)").await;
    for t in ["own_p", "own_q"] {
        let values: Vec<String> = (1..=TENANTS).chain(ORPHANS).map(|k| format!("({k}, 'owner {k}', 0)")).collect();
        exec_ok(&db, &format!("INSERT INTO {t} (tenant_id, name, hits) VALUES {}", values.join(", "))).await;
    }
    let pt = partitioned(&db, "acct");

    // Against the twin.
    for (sql, tag) in [
        ("UPDATE {o} SET name = 'limited', hits = 1 FROM {a} a WHERE {o}.tenant_id = a.tenant_id AND a.lim IS NOT NULL", 0u8),
        ("UPDATE {o} SET hits = 5 FROM {a} a WHERE {o}.tenant_id = a.tenant_id AND a.tenant_id = 3 RETURNING {o}.tenant_id, {o}.hits", 2),
        ("DELETE FROM {o} USING {a} a WHERE {o}.tenant_id = a.tenant_id AND a.lim > 120", 0),
        ("DELETE FROM {o} USING {a} a WHERE {o}.tenant_id = a.tenant_id AND a.tenant_id = 15 RETURNING {o}.name", 2),
    ] {
        let p = exec(&db, &sql.replace("{o}", "own_p").replace("{a}", "acct")).await;
        let q = exec(&db, &sql.replace("{o}", "own_q").replace("{a}", "acct_q")).await;
        assert_eq!(p.first(), Some(&tag), "`{sql}`: {}", String::from_utf8_lossy(&p));
        assert_eq!(p, q, "`{sql}`: the partitioned source answered unlike the twin");
        let (mut sp, mut sq) = (
            table(&db, "SELECT tenant_id, name, hits FROM own_p").await,
            table(&db, "SELECT tenant_id, name, hits FROM own_q").await,
        );
        sp.sort();
        sq.sort();
        assert_eq!(sp, sq, "`{sql}`: the targets hold different rows");
    }
    let changed = table(&db, "SELECT tenant_id FROM own_p WHERE hits > 0").await;
    assert!(changed.len() >= 3, "the writes changed too little to compare: {changed:?}");

    // Under a transfer between two partition reads of the source.
    let (debit, credit) = debit_and_credit(&pt);
    let committed = StdArc::new(AtomicI64::new(0));
    let hook: test_hooks::Hook = {
        let (db, committed) = (db.clone(), committed.clone());
        StdArc::new(move || {
            let (db, committed) = (db.clone(), committed.clone());
            Box::pin(async move {
                in_one_transaction(
                    &db,
                    &[
                        format!("UPDATE acct SET bal = bal - 1 WHERE tenant_id = {debit}"),
                        format!("UPDATE acct SET bal = bal + 1 WHERE tenant_id = {credit}"),
                    ],
                )
                .await;
                committed.fetch_add(1, Ordering::SeqCst);
            })
        })
    };
    // Past the opening state, so `OPENING + k` names only the credit tenant.
    in_one_transaction(
        &db,
        &[
            format!("UPDATE acct SET bal = bal - 1 WHERE tenant_id = {debit}"),
            format!("UPDATE acct SET bal = bal + 1 WHERE tenant_id = {credit}"),
        ],
    )
    .await;
    committed.store(1, Ordering::SeqCst);
    test_hooks::set_after_first(pt.uid(), Some(hook));
    for round in 0..5i64 {
        let k = committed.load(Ordering::SeqCst);
        let out = exec(
            &db,
            &format!(
                "UPDATE own_p SET hits = hits + 100 FROM acct a WHERE own_p.tenant_id = a.tenant_id AND a.bal = {} \
                 RETURNING own_p.tenant_id",
                OPENING + k
            ),
        )
        .await;
        // Matching nothing, the plain path answers a count of 0 rather than
        // an empty row set; either way, no row.
        let got = match out.first() {
            Some(2) => cells(&crate::core::row::row::vec_into_rows(&out[1..]).unwrap()),
            Some(0) if out[1..] == 0u64.to_le_bytes() => Vec::new(),
            _ => panic!("round {round}: {}", String::from_utf8_lossy(&out)),
        };
        assert_eq!(
            got,
            vec![vec![Cell::Int(credit)]],
            "round {round}: the source was read at more than one moment (k = {k})"
        );
        assert_eq!(committed.load(Ordering::SeqCst), k + 1, "round {round}: the hook did not run");
    }
    test_hooks::set_after_first(pt.uid(), None);

    // A partitioned target (phase 3 P3): written back by its key, routed per
    // row, like the plain twin given the same rows.
    exec_ok(&db, "DELETE FROM acct_q").await;
    exec_ok(&db, "INSERT INTO acct_q (tenant_id, bal, lim) SELECT tenant_id, bal, lim FROM acct").await;
    let own_before = table(&db, "SELECT tenant_id, name, hits FROM own_q").await;
    let sorted = |out: &[u8]| -> (u8, Vec<Vec<Cell>>, Vec<u8>) {
        match out.first() {
            Some(2) => {
                let mut got = cells(&crate::core::row::row::vec_into_rows(&out[1..]).unwrap());
                got.sort();
                (2, got, Vec::new())
            }
            Some(&tag) => (tag, Vec::new(), out[1..].to_vec()),
            None => (255, Vec::new(), Vec::new()),
        }
    };
    for (sql, tag) in [
        ("UPDATE {a} SET bal = {a}.bal + 1 FROM own_q o WHERE {a}.tenant_id = o.tenant_id", 0u8),
        ("UPDATE {a} SET lim = 0 FROM own_q o WHERE {a}.tenant_id = o.tenant_id AND o.tenant_id < 5 RETURNING {a}.tenant_id, {a}.lim", 2),
        ("DELETE FROM {a} USING own_q o WHERE {a}.tenant_id = o.tenant_id AND o.tenant_id > 10", 0),
        ("DELETE FROM {a} USING own_q o WHERE {a}.tenant_id = o.tenant_id AND o.tenant_id = 2 RETURNING {a}.tenant_id, {a}.bal", 2),
    ] {
        let p = exec(&db, &sql.replace("{a}", "acct")).await;
        let q = exec(&db, &sql.replace("{a}", "acct_q")).await;
        assert_eq!(p.first(), Some(&tag), "`{sql}`: {}", String::from_utf8_lossy(&p));
        assert_eq!(sorted(&p), sorted(&q), "`{sql}`: the partitioned target answered unlike the twin");
        let (mut sp, mut sq) = (
            table(&db, "SELECT tenant_id, bal, lim FROM acct").await,
            table(&db, "SELECT tenant_id, bal, lim FROM acct_q").await,
        );
        sp.sort();
        sq.sort();
        assert_eq!(sp, sq, "`{sql}`: the targets hold different rows");
        assert_eq!(table(&db, "SELECT tenant_id, name, hits FROM own_q").await, own_before, "`{sql}` wrote the source");
    }
    drop(pt);
    close(db, dir.path());
}
