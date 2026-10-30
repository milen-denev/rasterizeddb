//! The prepared `SELECT` cache on partitioned tables (`docs/partitioning.md`
//! §4.1, S15), seen from inside the process: one pgwire session over a duplex
//! stream, the fan-out's read counter and between-partitions hook
//! (`row_source::test_hooks`), and the executor's per-table parse count
//! (`prepared::test_counters`).
//!
//! - **I11, late routing.** The parse is learned once and reused, but every
//!   execution routes the `WHERE` its own values render: two tenants in two
//!   partitions, alternating, each read only its own partition and get only
//!   its own rows, from the cached parse.
//! - **The flags are read per execution.** A tenant first written after the
//!   parse was learned is counted by the next execution.
//! - **One snapshot on the fast path** (I7, owner constraint 2). A learned
//!   execution reads inside `statement_snapshot::scope`, and a transfer that
//!   commits between two partition reads is seen whole or not at all.
//! - **Key spellings** (I2): a key bound as text and in binary, on an INT and
//!   a BIGINT key, routes to its point's partition from the cached parse.

use super::*;
use crate::core::database::relation::Relation;
use crate::core::partition::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::prepared::test_counters::{parses_of, take_learned_runs};
use crate::core::rql::row_source::test_hooks;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

// ---------------------------------------------------------------------------
// The database side.
// ---------------------------------------------------------------------------

async fn open(root: &std::path::Path, name: &str) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::new_named(&root.to_string_lossy(), false, name).await)
}

fn close(db: Arc<Database>, root: &std::path::Path) {
    drop(db);
    crate::core::wal::remove_for(&root.to_string_lossy());
}

/// Runs `sql` through the executor, outside any session, in a transaction of
/// its own.
async fn exec_ok(db: &Arc<Database>, sql: &str) {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    let out = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(out.first().is_some_and(|b| *b != 3), "{sql} failed: {}", String::from_utf8_lossy(&out));
}

fn partitioned(db: &Database, name: &str) -> rclite::Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not a partitioned table"),
    }
}

fn pid_of(pt: &PartitionedTable, key: &[u8]) -> u32 {
    let point = pt.spec().unwrap().point_of(&[key]).unwrap();
    pt.epoch().unwrap().map.pid_of(point)
}

fn materialized(pt: &PartitionedTable) -> Vec<u32> {
    pt.materialized_pids().unwrap()
}

fn session(db: &Arc<Database>) -> SessionState {
    SessionState::new(
        DatabaseCatalog::single(db.clone()),
        db.name.clone(),
        AuthConfig { method: crate::auth::AuthMethod::Trust, ..AuthConfig::default() },
        Principal { name: "rasterized".to_string(), superuser: false, authenticated: false },
        ConnectionLimits::default(),
    )
}

/// Runs `client` against one pgwire session on `db`, both on this task, and
/// ends the session when `client` returns.
async fn with_session<T>(
    db: &Arc<Database>,
    client: impl AsyncFnOnce(&mut Wire) -> T,
) -> T {
    let (mut server, client_end) = tokio::io::duplex(1 << 20);
    let mut state = session(db);
    let mut wire = Wire { s: client_end };
    let serve = Box::pin(run_message_loop_inner(db, &mut state, &mut server));
    let drive = async {
        let out = client(&mut wire).await;
        wire.send(b'X', &[]).await;
        out
    };
    let (served, out) = tokio::join!(serve, drive);
    served.expect("the session ended cleanly");
    out
}

// ---------------------------------------------------------------------------
// A minimal extended-protocol client.
// ---------------------------------------------------------------------------

struct Wire {
    s: DuplexStream,
}

#[derive(Debug, Default)]
struct Answer {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

#[derive(Clone)]
enum P {
    Text(String),
    Bin(Vec<u8>),
}

fn cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

impl Wire {
    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut buf = vec![tag];
        buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(body);
        self.s.write_all(&buf).await.unwrap();
    }

    /// Reads to ReadyForQuery; the first error is the outcome.
    async fn until_ready(&mut self) -> Result<Answer, String> {
        let mut answer = Answer::default();
        let mut error = None;
        loop {
            let tag = self.s.read_u8().await.unwrap();
            let len = self.s.read_i32().await.unwrap() as usize;
            let mut body = vec![0u8; len - 4];
            self.s.read_exact(&mut body).await.unwrap();
            match tag {
                b'T' => {
                    answer.columns.clear();
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut at = 2;
                    for _ in 0..n {
                        let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                        answer.columns.push(String::from_utf8_lossy(&body[at..end]).into_owned());
                        at = end + 1 + 18;
                    }
                }
                b'D' => {
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut at = 2;
                    let mut row = Vec::with_capacity(n);
                    for _ in 0..n {
                        let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                        at += 4;
                        if len < 0 {
                            row.push(None);
                        } else {
                            row.push(Some(String::from_utf8_lossy(&body[at..at + len as usize]).into_owned()));
                            at += len as usize;
                        }
                    }
                    answer.rows.push(row);
                }
                b'E' => {
                    if error.is_none() {
                        error = Some(String::from_utf8_lossy(&body).replace('\0', " "));
                    }
                }
                b'Z' => return error.map_or(Ok(answer), Err),
                _ => {}
            }
        }
    }

    /// Parse (with parameter types when `oids` is not empty), describe the
    /// statement, sync.
    async fn prepare(&mut self, name: &str, sql: &str, oids: &[i32]) -> Answer {
        let mut parse = Vec::new();
        cstr(&mut parse, name);
        cstr(&mut parse, sql);
        parse.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for oid in oids {
            parse.extend_from_slice(&oid.to_be_bytes());
        }
        self.send(b'P', &parse).await;
        let mut describe = vec![b'S'];
        cstr(&mut describe, name);
        self.send(b'D', &describe).await;
        self.send(b'S', &[]).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("prepare {sql}: {e}"))
    }

    /// A simple query: the text path, always.
    async fn simple(&mut self, sql: &str) -> Answer {
        let mut body = Vec::new();
        cstr(&mut body, sql);
        self.send(b'Q', &body).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// Bind `params` to statement `name`, execute the unnamed portal, sync.
    /// No Describe, as a client that never describes again.
    async fn run(&mut self, name: &str, params: &[P]) -> Answer {
        let mut bind = Vec::new();
        cstr(&mut bind, "");
        cstr(&mut bind, name);
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            bind.extend_from_slice(&(matches!(p, P::Bin(_)) as i16).to_be_bytes());
        }
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            let bytes = match p {
                P::Text(s) => s.as_bytes(),
                P::Bin(b) => b.as_slice(),
            };
            bind.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            bind.extend_from_slice(bytes);
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'B', &bind).await;
        let mut execute = Vec::new();
        cstr(&mut execute, "");
        execute.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &execute).await;
        self.send(b'S', &[]).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("execute {name}: {e}"))
    }
}

fn t(v: impl ToString) -> P {
    P::Text(v.to_string())
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

/// Two tenants in `4..=9` (single digits, so neither literal occurs inside the
/// other or inside a group number, and the template is learned from the
/// first two executions) whose rows live in different partitions.
fn two_tenants_apart(pt: &PartitionedTable) -> (i64, i64) {
    let a = 4i64;
    let b = (5..=9i64)
        .find(|b| pid_of(pt, &b.to_le_bytes()) != pid_of(pt, &a.to_le_bytes()))
        .expect("tenants 4..=9 all hash to one partition");
    (a, b)
}

// ---------------------------------------------------------------------------
// I11.
// ---------------------------------------------------------------------------

/// I11. A point `SELECT` and a `COUNT(*)`, prepared once and run ten times
/// alternating two tenants in different partitions: every execution returns
/// its own tenant's rows and reads only its own tenant's partition, and from
/// the third on none of them parses (the cached parse answered). The plain
/// twin learns in the same two executions.
///
/// Mutation: route every learned execution on the first one's `WHERE` (a
/// cached route): the second tenant reads the first one's partition and
/// gets nothing. Mutation: mark the partitioned `SELECT` rewritten again
/// (`note_rewritten`): every execution parses.
#[tokio::test(flavor = "multi_thread")]
async fn each_execution_routes_its_own_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_i11").await;
    let ddl = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, n BIGINT, PRIMARY KEY (tenant_id, grp))";
    exec_ok(&db, &format!("CREATE TABLE pr_i11 {ddl} PARTITION BY HASH (tenant_id) PARTITIONS 7")).await;
    exec_ok(&db, &format!("CREATE TABLE pr_i11_plain {ddl}")).await;
    let values: Vec<String> =
        (1..=16i64).flat_map(|t| (1..=2i64).map(move |g| format!("({t}, {g}, {})", t * 100 + g))).collect();
    for table in ["pr_i11", "pr_i11_plain"] {
        exec_ok(&db, &format!("INSERT INTO {table} (tenant_id, grp, n) VALUES {}", values.join(", "))).await;
    }
    let pt = partitioned(&db, "pr_i11");
    let (a, b) = two_tenants_apart(&pt);
    let home = |tenant: i64| pid_of(&pt, &tenant.to_le_bytes());

    let runs_before = PREPARED_RUNS.load(Ordering::Relaxed);
    with_session(&db, async |w: &mut Wire| {
        for table in ["pr_i11", "pr_i11_plain"] {
            w.prepare("pt", &format!("SELECT tenant_id, grp, n FROM {table} WHERE tenant_id = $1 AND grp = $2"), &[])
                .await;
            w.prepare("pc", &format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = $1"), &[]).await;
            let parses_before = parses_of(table);
            test_hooks::take_reads(pt.uid());
            for i in 0..10 {
                let (tenant, grp) = if i % 2 == 0 { (a, 1) } else { (b, 2) };
                let got = w.run("pt", &[t(tenant), t(grp)]).await;
                let n = (tenant * 100 + grp).to_string();
                assert_eq!(
                    got.rows,
                    vec![row(&[&tenant.to_string(), &grp.to_string(), &n])],
                    "{table} run {i}: tenant {tenant} got another tenant's rows"
                );
                let count = w.run("pc", &[t(tenant)]).await;
                assert_eq!(count.rows, vec![row(&["2"])], "{table} run {i}: COUNT for tenant {tenant}");
                if table == "pr_i11" {
                    let reads = test_hooks::take_reads(pt.uid());
                    assert_eq!(
                        reads,
                        vec![home(tenant), home(tenant)],
                        "run {i}: tenant {tenant} (partition {}) read other partitions",
                        home(tenant)
                    );
                }
            }
            // Two learning executions per statement, then the cached parse.
            assert_eq!(
                parses_of(table) - parses_before,
                4,
                "{table}: twenty executions of two statements did not stop parsing after learning"
            );
        }
    })
    .await;
    assert!(PREPARED_RUNS.load(Ordering::Relaxed) - runs_before >= 32, "the fast path never ran");
    drop(pt);
    close(db, dir.path());
}

/// The microbenchmark the exit measurement (S16) stands on: N executions of a
/// learned point `SELECT` parse twice, on a partitioned table as on a plain
/// one, and the time per execution is printed for the curious. Nothing here
/// is timed against a bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_learned_parse_is_not_parsed_again() {
    const N: usize = 200;
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_bench").await;
    let ddl = "(tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (tenant_id, id))";
    exec_ok(&db, &format!("CREATE TABLE pr_bench {ddl} PARTITION BY HASH (tenant_id) PARTITIONS 64")).await;
    exec_ok(&db, &format!("CREATE TABLE pr_bench_plain {ddl}")).await;
    // `id` never contains its tenant's digits, so the two learning
    // executions place each value unambiguously.
    let id = |t: i64| 3000 + 7 * t;
    let values: Vec<String> = (1..=64i64).map(|t| format!("({t}, {}, {t})", id(t))).collect();
    for table in ["pr_bench", "pr_bench_plain"] {
        exec_ok(&db, &format!("INSERT INTO {table} (tenant_id, id, n) VALUES {}", values.join(", "))).await;
    }
    with_session(&db, async |w: &mut Wire| {
        for table in ["pr_bench", "pr_bench_plain"] {
            w.prepare("p", &format!("SELECT n FROM {table} WHERE tenant_id = $1 AND id = $2"), &[]).await;
            let before = parses_of(table);
            let started = std::time::Instant::now();
            for i in 0..N {
                let k = 10 + (i as i64 % 50);
                let got = w.run("p", &[t(k), t(id(k))]).await;
                assert_eq!(got.rows, vec![row(&[&k.to_string()])], "{table}: key {k}");
            }
            let per = started.elapsed() / N as u32;
            assert_eq!(parses_of(table) - before, 2, "{table}: {N} executions, more than the two learning parses");
            eprintln!("{table}: {N} executions, 2 parses, {per:?} per execution (unmeasured box; no bound)");
        }
    })
    .await;
    close(db, dir.path());
}

/// §4.3/§4.4 on the fast path. A prepared `COUNT(*)` for a tenant whose
/// partition is not materialized learns its parse answering 0; that tenant's
/// first row is then written, materializing the partition, and the next
/// cached execution counts it. The flags are read per execution, after its
/// snapshot, never at learn time.
///
/// Mutation: keep each `WHERE`'s first materialized list (in
/// `FanOut::plan`): the new partition is never read and the count stays 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_first_written_after_learning_is_counted() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_mat").await;
    exec_ok(
        &db,
        "CREATE TABLE pr_mat (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 64",
    )
    .await;
    exec_ok(&db, "INSERT INTO pr_mat (tenant_id, id) VALUES (1, 1), (1, 2), (2, 1), (3, 1)").await;
    let pt = partitioned(&db, "pr_mat");
    let fresh = (100..1000i64)
        .find(|t| !materialized(&pt).contains(&pid_of(&pt, &t.to_le_bytes())))
        .expect("a tenant whose partition is not materialized");
    with_session(&db, async |w: &mut Wire| {
        w.prepare("c", "SELECT COUNT(*) FROM pr_mat WHERE tenant_id = $1", &[]).await;
        let before = parses_of("pr_mat");
        for _ in 0..3 {
            assert_eq!(w.run("c", &[t(fresh)]).await.rows, vec![row(&["0"])]);
            assert_eq!(w.run("c", &[t(1)]).await.rows, vec![row(&["2"])]);
        }
        assert_eq!(parses_of("pr_mat") - before, 2, "the parse was never learned");
        exec_ok(&db, &format!("INSERT INTO pr_mat (tenant_id, id) VALUES ({fresh}, 1)")).await;
        assert!(materialized(&pt).contains(&pid_of(&pt, &fresh.to_le_bytes())), "the first row materialized nothing");
        assert_eq!(
            w.run("c", &[t(fresh)]).await.rows,
            vec![row(&["1"])],
            "a cached COUNT missed a row in a partition materialized after it learned"
        );
        assert_eq!(parses_of("pr_mat") - before, 2, "the last execution did not run from the cached parse");
    })
    .await;
    drop(pt);
    close(db, dir.path());
}

// ---------------------------------------------------------------------------
// I7 on the fast path.
// ---------------------------------------------------------------------------

const ACCOUNTS: i64 = 16;
const OPENING: i64 = 1000;

/// I7 through the cached parse (owner constraint 2). A prepared, unrouted
/// `SELECT SUM(bal)` over every partition runs 20 times. Between its first
/// and second partition reads, every time, a transfer between a tenant in
/// the first partition and one in another commits (one statement, so one
/// transaction), and the hook records whether the reader is inside
/// `statement_snapshot::scope`. The sum is the opening total on every run,
/// the scope held on every run, and 18 of the 20 ran from the cached parse.
/// Bounded: 20 runs, under 5 s.
///
/// Mutation: drop the scope from `run_statement`'s learned arm: the scope
/// check fails on the cached runs. Mutation: give each partition read a
/// snapshot of its own (`FanOut::each`): the sum is off by a transfer.
#[tokio::test(flavor = "multi_thread")]
async fn the_fast_path_reads_one_snapshot_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_i7").await;
    exec_ok(
        &db,
        "CREATE TABLE accounts (tenant_id BIGINT NOT NULL, seq BIGINT NOT NULL, bal BIGINT NOT NULL, \
         PRIMARY KEY (tenant_id, seq)) PARTITION BY HASH (tenant_id) PARTITIONS 7",
    )
    .await;
    let opening: Vec<String> = (1..=ACCOUNTS).map(|t| format!("({t}, 0, {OPENING})")).collect();
    exec_ok(&db, &format!("INSERT INTO accounts (tenant_id, seq, bal) VALUES {}", opening.join(", "))).await;
    let pt = partitioned(&db, "accounts");
    let parts = materialized(&pt);
    assert!(parts.len() >= 2, "the tenants reach one partition only: {parts:?}");
    let first = parts[0];
    let debit = (1..=ACCOUNTS).find(|t| pid_of(&pt, &t.to_le_bytes()) == first).unwrap();
    let credit = (1..=ACCOUNTS).find(|t| pid_of(&pt, &t.to_le_bytes()) != first).unwrap();

    let transfers = StdArc::new(AtomicI64::new(0));
    let fired = StdArc::new(AtomicUsize::new(0));
    let unscoped = StdArc::new(AtomicBool::new(false));
    let hook: test_hooks::Hook = {
        let (db, transfers, fired, unscoped) = (db.clone(), transfers.clone(), fired.clone(), unscoped.clone());
        StdArc::new(move || {
            let (db, transfers, fired, unscoped) = (db.clone(), transfers.clone(), fired.clone(), unscoped.clone());
            Box::pin(async move {
                if !crate::core::rql::statement_snapshot::in_scope() {
                    unscoped.store(true, Ordering::SeqCst);
                }
                fired.fetch_add(1, Ordering::SeqCst);
                let n = transfers.load(Ordering::SeqCst) + 1;
                exec_ok(
                    &db,
                    &format!(
                        "INSERT INTO accounts (tenant_id, seq, bal) VALUES ({debit}, {n}, -{n}), ({credit}, {n}, {n})"
                    ),
                )
                .await;
                transfers.store(n, Ordering::SeqCst);
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));

    let total = ACCOUNTS * OPENING;
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        with_session(&db, async |w: &mut Wire| {
            w.prepare("s", "SELECT SUM(bal) FROM accounts WHERE seq < $1", &[]).await;
            let before = parses_of("accounts");
            for run in 0..20i64 {
                // Above every seq, and different each time, so the template
                // is learned from the first two runs.
                let bound = 1_000_000 + run;
                let got = w.run("s", &[t(bound)]).await;
                assert_eq!(
                    got.rows,
                    vec![row(&[&total.to_string()])],
                    "run {run}: a transfer committed between two partition reads was seen half applied"
                );
                assert!(
                    !unscoped.load(Ordering::SeqCst),
                    "run {run}: a prepared execution read partitions outside the statement's snapshot scope"
                );
            }
            assert_eq!(parses_of("accounts") - before, 2, "the cached parse never answered");
        }),
    )
    .await;
    test_hooks::set_after_first(pt.uid(), None);
    outcome.expect("twenty prepared reads took over 5 s");
    assert_eq!(fired.load(Ordering::SeqCst), 20, "the hook did not fall between partition reads on every run");
    assert_eq!(transfers.load(Ordering::SeqCst), 20, "a transfer failed to commit");
    drop(pt);
    close(db, dir.path());
}

/// I7 for a prepared join (S19, owner constraint 2). A join is never a
/// learned parse (only a plain single-table `SELECT` is), so its executions
/// take the text path, which `run_statement` runs inside
/// `statement_snapshot::scope` for a read-only statement. Two partitioned
/// tables, `jacct` (7 partitions) and `jbank` (5), one row per tenant. Between
/// the first and second partition reads of every fan-out over `jacct`, one
/// transaction commits: 2 out of a `jacct` tenant in the partition read
/// first, 1 into one read after it, and 1 into `jbank`. A prepared
/// `jacct ⋈ jbank` keeps the combined total and a prepared self-join's sides
/// agree on every run, and every hook saw the reader inside the scope.
/// Bounded: 2 × 8 runs, under 10 s.
///
/// Mutation: drop the scope from `run_statement`'s text arm: the scope check
/// fails. Mutation: a snapshot per partition (`FanOut::each`): the sums are
/// off by a transfer.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_join_reads_one_snapshot_across_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_join").await;
    for (table, partitions) in [("jacct", 7), ("jbank", 5)] {
        exec_ok(
            &db,
            &format!(
                "CREATE TABLE {table} (tenant_id BIGINT NOT NULL, bal BIGINT NOT NULL, PRIMARY KEY (tenant_id)) \
                 PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"
            ),
        )
        .await;
        let opening: Vec<String> = (1..=ACCOUNTS).map(|t| format!("({t}, {OPENING})")).collect();
        exec_ok(&db, &format!("INSERT INTO {table} (tenant_id, bal) VALUES {}", opening.join(", "))).await;
    }
    let pt = partitioned(&db, "jacct");
    let parts = materialized(&pt);
    assert!(parts.len() >= 2, "the tenants reach one partition only: {parts:?}");
    let debit = (1..=ACCOUNTS).find(|t| pid_of(&pt, &t.to_le_bytes()) == parts[0]).unwrap();
    let credit = (1..=ACCOUNTS).find(|t| pid_of(&pt, &t.to_le_bytes()) != parts[0]).unwrap();

    let transfers = StdArc::new(AtomicI64::new(0));
    let unscoped = StdArc::new(AtomicBool::new(false));
    let hook: test_hooks::Hook = {
        let (db, transfers, unscoped) = (db.clone(), transfers.clone(), unscoped.clone());
        StdArc::new(move || {
            let (db, transfers, unscoped) = (db.clone(), transfers.clone(), unscoped.clone());
            Box::pin(async move {
                if !crate::core::rql::statement_snapshot::in_scope() {
                    unscoped.store(true, Ordering::SeqCst);
                }
                // Routed, one-partition writes in one transaction: no
                // fan-out of their own, so the hook never fires inside itself.
                let tx = db.transactions.begin();
                for sql in [
                    format!("UPDATE jacct SET bal = bal - 2 WHERE tenant_id = {debit}"),
                    format!("UPDATE jacct SET bal = bal + 1 WHERE tenant_id = {credit}"),
                    format!("UPDATE jbank SET bal = bal + 1 WHERE tenant_id = {credit}"),
                ] {
                    let purpose = recognize_query_purpose(&sql).unwrap();
                    let out = crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx).await;
                    assert!(out.first().is_some_and(|b| *b != 3), "{sql}: {}", String::from_utf8_lossy(&out));
                }
                db.commit_written_transaction(tx).expect("commit");
                db.transactions.flush().expect("flush the outcome");
                transfers.fetch_add(1, Ordering::SeqCst);
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));

    let total = ACCOUNTS * OPENING;
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        with_session(&db, async |w: &mut Wire| {
            w.prepare(
                "both",
                "SELECT SUM(a.bal), SUM(b.bal) FROM jacct a JOIN jbank b ON a.tenant_id = b.tenant_id \
                 WHERE a.tenant_id > $1",
                &[],
            )
            .await;
            w.prepare(
                "selfj",
                "SELECT COUNT(*), SUM(a.bal) FROM jacct a JOIN jacct b ON a.tenant_id = b.tenant_id \
                 WHERE a.bal = b.bal AND a.tenant_id > $1",
                &[],
            )
            .await;
            for run in 0..8i64 {
                let before = transfers.load(Ordering::SeqCst);
                let got = w.run("both", &[t(-run)]).await;
                assert_eq!(
                    got.rows,
                    vec![row(&[&(total - before).to_string(), &(total + before).to_string()])],
                    "run {run}: a prepared join read its two tables at two moments"
                );
                assert_eq!(transfers.load(Ordering::SeqCst), before + 1, "run {run}: the hook did not run");

                let before = transfers.load(Ordering::SeqCst);
                let got = w.run("selfj", &[t(-run)]).await;
                assert_eq!(
                    got.rows,
                    vec![row(&[&ACCOUNTS.to_string(), &(total - before).to_string()])],
                    "run {run}: a prepared self-join's sides read two snapshots"
                );
                assert_eq!(transfers.load(Ordering::SeqCst), before + 2, "run {run}: the hook did not run on both sides");
                assert!(
                    !unscoped.load(Ordering::SeqCst),
                    "run {run}: a prepared join read partitions outside the statement's snapshot scope"
                );
            }
        }),
    )
    .await;
    test_hooks::set_after_first(pt.uid(), None);
    outcome.expect("sixteen prepared joins took over 10 s");
    assert_eq!(transfers.load(Ordering::SeqCst), 24, "a transfer failed to commit");
    drop(pt);
    close(db, dir.path());
}

/// The learned arm of `run_statement` scopes a parse whose `row_lock` is
/// empty; the text path scopes a statement `is_read_only` admits. They are the
/// same rule only if every locking clause `is_read_only` refuses is one the
/// parse records in `row_lock`, which this pins. (A learned parse is a plain
/// single-table `SELECT`, so a locking clause is all that can make it more
/// than a read.)
#[test]
fn every_locking_clause_is_recorded_in_row_lock() {
    use crate::core::rql::row_lock::split_locking_clause;
    use crate::core::rql::statement_snapshot::is_read_only;
    for tail in [
        "FOR UPDATE",
        "FOR NO KEY UPDATE",
        "FOR SHARE",
        "FOR KEY SHARE",
        "FOR UPDATE NOWAIT",
        "FOR SHARE SKIP LOCKED",
        "FOR UPDATE OF t",
        "for update",
    ] {
        let sql = format!("SELECT * FROM t WHERE k = 1 {tail}");
        assert!(!is_read_only(&sql), "{sql}");
        let (_, lock) = split_locking_clause(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(lock.is_some(), "{sql}: a locking clause the parse does not record would be scoped");
    }
    let sql = "SELECT * FROM t WHERE k = 1";
    assert!(is_read_only(sql));
    assert!(split_locking_clause(sql).unwrap().1.is_none());
}

/// The scope's allowlist, enumerated (memory `a-guard-is-an-allowlist`):
/// `run_statement` scopes a learned parse with no `row_lock` and no other.
/// A plain table's `SELECT … FOR UPDATE` is learned like any point read (the
/// premise of the guard, pinned here), and runs outside the scope, as its
/// text does; a plain read and a partitioned one run inside it.
///
/// Mutation: scope every learned parse: the locking read's runs are scoped.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_learned_read_without_a_lock_is_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_scope").await;
    exec_ok(&db, "CREATE TABLE pr_lock (k BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await;
    exec_ok(&db, "CREATE TABLE pr_lockp (k BIGINT NOT NULL, v BIGINT) PARTITION BY HASH (k) PARTITIONS 7").await;
    let rows: Vec<String> = (1..=20i64).map(|k| format!("({k}, {})", k * 10)).collect();
    for table in ["pr_lock", "pr_lockp"] {
        exec_ok(&db, &format!("INSERT INTO {table} (k, v) VALUES {}", rows.join(", "))).await;
    }
    let keys = [13i64, 15, 4, 7, 18, 9];
    let pt = partitioned(&db, "pr_lockp");
    with_session(&db, async |w: &mut Wire| {
        for (table, lock, block) in [("pr_lock", "", false), ("pr_lock", " FOR UPDATE", true), ("pr_lockp", "", false)] {
            let sql = format!("SELECT k, v FROM {table} WHERE k = $1{lock}");
            w.prepare("s", &sql, &[]).await;
            take_learned_runs(table);
            test_hooks::take_keyed(pt.uid());
            if block {
                w.simple("BEGIN").await;
            }
            for k in keys {
                let got = w.run("s", &[t(k)]).await;
                assert_eq!(got.rows, vec![row(&[&k.to_string(), &(k * 10).to_string()])], "{sql} ({k})");
            }
            if block {
                w.simple("COMMIT").await;
            }
            let scoped = take_learned_runs(table);
            assert_eq!(scoped.len(), keys.len() - 2, "{sql}: the parse was not learned in two executions");
            let want = lock.is_empty();
            assert!(
                scoped.iter().all(|s| *s == want),
                "{sql}: learned runs scoped {scoped:?}; a locking read must run outside the snapshot scope, every \
                 other inside it"
            );
            if table == "pr_lockp" {
                // The partitioned read's learned runs took the cached route
                // (S16b), inside the scope like every other learned read.
                assert_eq!(test_hooks::take_keyed(pt.uid()), scoped.len() as u64, "{sql}: not planned from a bound key");
            }
        }
    })
    .await;
    drop(pt);
    close(db, dir.path());
}

// ---------------------------------------------------------------------------
// Key spellings.
// ---------------------------------------------------------------------------

/// I2 on the fast path. A point `SELECT` on an INT key and on a BIGINT key,
/// prepared twice each (parameters bound as text with the type left to the
/// server, and in binary with it declared), every value run twice so most
/// executions are cached: each returns its row and reads only the partition
/// its point names.
#[tokio::test(flavor = "multi_thread")]
async fn text_and_binary_keys_route_alike_from_the_cached_parse() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_i2").await;
    exec_ok(&db, "CREATE TABLE pr_kb (k BIGINT NOT NULL, label TEXT) PARTITION BY HASH (k) PARTITIONS 64").await;
    exec_ok(&db, "CREATE TABLE pr_ki (k INT NOT NULL, label TEXT) PARTITION BY HASH (k) PARTITIONS 64").await;
    let values: [i64; 7] = [0, 7, 42, 1000, -1, i32::MAX as i64, i32::MIN as i64 + 1];
    for table in ["pr_kb", "pr_ki"] {
        let rows: Vec<String> = values.iter().map(|v| format!("({v}, 'v{v}')")).collect();
        exec_ok(&db, &format!("INSERT INTO {table} (k, label) VALUES {}", rows.join(", "))).await;
    }
    with_session(&db, async |w: &mut Wire| {
        for (table, oid) in [("pr_kb", 20i32), ("pr_ki", 23)] {
            let pt = partitioned(&db, table);
            let key = |v: i64| if oid == 20 { v.to_le_bytes().to_vec() } else { (v as i32).to_le_bytes().to_vec() };
            let wire = |v: i64| if oid == 20 { v.to_be_bytes().to_vec() } else { (v as i32).to_be_bytes().to_vec() };
            let sql = format!("SELECT k, label FROM {table} WHERE k = $1");
            w.prepare("text", &sql, &[]).await;
            w.prepare("bin", &sql, &[oid]).await;
            let before = parses_of(table);
            test_hooks::take_reads(pt.uid());
            let mut executions = 0;
            for _ in 0..2 {
                for v in values {
                    for (name, p) in [("text", t(v)), ("bin", P::Bin(wire(v)))] {
                        let got = w.run(name, &[p]).await;
                        executions += 1;
                        assert_eq!(got.rows, vec![row(&[&v.to_string(), &format!("v{v}")])], "{table} {name} {v}");
                        let reads = test_hooks::take_reads(pt.uid());
                        if v >= 0 {
                            assert_eq!(reads, vec![pid_of(&pt, &key(v))], "{table} {name} {v}: not routed to its point's partition");
                        } else {
                            // A signed value runs from its text, where `k = - 1`
                            // is not yet routed (docs/partitioning.md §4.1): a
                            // superset, never a miss.
                            assert!(reads.contains(&pid_of(&pt, &key(v))), "{table} {name} {v}: its partition was not read");
                        }
                    }
                }
            }
            // Two learning executions per statement; a signed value is never
            // learned (`literal_class`), so each of the two negative values
            // parses on every run, by both statements, both rounds.
            let negatives = values.iter().filter(|v| **v < 0).count() as u64;
            let parses = parses_of(table) - before;
            assert_eq!(
                parses,
                4 + negatives * 2 * 2,
                "{table}: {parses} parses in {executions} executions; the cache never answered, or answered a \
                 signed value"
            );
        }
    })
    .await;
    close(db, dir.path());
}

// ---------------------------------------------------------------------------
// Signed literals.
// ---------------------------------------------------------------------------

/// A signed value is never learned (`prepared::literal_class`): the text path
/// rewrites `k = -1` to `k = - 1`, so a signed literal does not pass through
/// the rewrites unchanged, which every admitted literal must. On a plain and
/// a partitioned table:
/// - a statement bound only to negative keys, five times with five values,
///   never runs from a cached parse, and answers as the same statement sent
///   as simple-query text does;
/// - once a statement learned on positive keys, a negative key still runs
///   from its text, and answers as the text does.
///
/// Mutation: admit a leading `-` in `literal_class` again: the learned
/// statement runs the negative keys from its cached parse.
#[tokio::test(flavor = "multi_thread")]
async fn a_signed_key_is_never_learned() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_signed").await;
    exec_ok(&db, "CREATE TABLE pr_sg (k BIGINT NOT NULL, label TEXT)").await;
    exec_ok(&db, "CREATE TABLE pr_sgp (k BIGINT NOT NULL, label TEXT) PARTITION BY HASH (k) PARTITIONS 64").await;
    let rows: Vec<String> = (-9..=9i64).map(|v| format!("({v}, 'v{v}')")).collect();
    for table in ["pr_sg", "pr_sgp"] {
        exec_ok(&db, &format!("INSERT INTO {table} (k, label) VALUES {}", rows.join(", "))).await;
    }
    with_session(&db, async |w: &mut Wire| {
        for table in ["pr_sg", "pr_sgp"] {
            let sql = format!("SELECT k, label FROM {table} WHERE k = $1");
            let text = async |w: &mut Wire, v: i64| w.simple(&format!("SELECT k, label FROM {table} WHERE k = {v}")).await;

            w.prepare("neg", &sql, &[]).await;
            take_learned_runs(table);
            let before = parses_of(table);
            for v in [-1i64, -2, -3, -4, -5] {
                let got = w.run("neg", &[t(v)]).await;
                assert_eq!(got.rows, vec![row(&[&v.to_string(), &format!("v{v}")])], "{table} {v}");
                assert_eq!(got.rows, text(w, v).await.rows, "{table} {v}: the prepared answer is not the text's");
            }
            assert_eq!(take_learned_runs(table), Vec::<bool>::new(), "{table}: a signed key ran from a cached parse");
            // Five prepared executions and five simple queries, all parsed.
            assert_eq!(parses_of(table) - before, 10, "{table}: a signed key skipped the parse");

            w.prepare("pos", &sql, &[]).await;
            for v in [3i64, 4, 5] {
                assert_eq!(w.run("pos", &[t(v)]).await.rows, vec![row(&[&v.to_string(), &format!("v{v}")])]);
            }
            assert_eq!(take_learned_runs(table).len(), 1, "{table}: the positive keys did not learn the parse");
            for v in [-1i64, -7, -1] {
                let got = w.run("pos", &[t(v)]).await;
                assert_eq!(got.rows, vec![row(&[&v.to_string(), &format!("v{v}")])], "{table} learned, {v}");
                assert_eq!(got.rows, text(w, v).await.rows, "{table} learned, {v}: not the text's answer");
            }
            assert_eq!(
                take_learned_runs(table),
                Vec::<bool>::new(),
                "{table}: a learned statement ran a signed key from its cached parse"
            );
            assert_eq!(w.run("pos", &[t(6)]).await.rows, vec![row(&["6", "v6"])]);
            assert_eq!(take_learned_runs(table).len(), 1, "{table}: the learned parse was lost to a signed key");
        }
    })
    .await;
    close(db, dir.path());
}

// ---------------------------------------------------------------------------
// S16b: the route derivation cached with the template.
// ---------------------------------------------------------------------------

impl Wire {
    /// Parse and Sync, no Describe: a client that never describes, so only
    /// the schema generation resets what the statement learned.
    async fn prepare_quiet(&mut self, name: &str, sql: &str) {
        let mut parse = Vec::new();
        cstr(&mut parse, name);
        cstr(&mut parse, sql);
        parse.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &parse).await;
        self.send(b'S', &[]).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
    }
}

/// The pid `point` falls in under `pt`'s current map.
fn pid_at(pt: &PartitionedTable, point: u64) -> u32 {
    pt.epoch().unwrap().map.pid_of(point)
}

/// I11 with the route cached (S16b). A point `SELECT` and a `COUNT(*)` by
/// the full key, prepared once and run twelve times alternating two tenants
/// in different partitions: from the third run on every execution plans its
/// fan-out from its own bound key (the keyed counter moves twice per run),
/// reads only its own tenant's partition, and returns its own rows.
///
/// Mutation: bind the first learning execution's values on every run (the
/// first execution's pid frozen): the second tenant reads the first one's
/// partition and gets nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_cached_route_binds_each_executions_own_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_i11").await;
    exec_ok(
        &db,
        "CREATE TABLE pr_ck (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, n BIGINT, PRIMARY KEY (tenant_id, grp)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 7",
    )
    .await;
    let values: Vec<String> =
        (1..=16i64).flat_map(|t| (1..=2i64).map(move |g| format!("({t}, {g}, {})", t * 100 + g))).collect();
    exec_ok(&db, &format!("INSERT INTO pr_ck (tenant_id, grp, n) VALUES {}", values.join(", "))).await;
    let pt = partitioned(&db, "pr_ck");
    let (a, b) = two_tenants_apart(&pt);
    let home = |tenant: i64| pid_of(&pt, &tenant.to_le_bytes());
    with_session(&db, async |w: &mut Wire| {
        w.prepare("pt", "SELECT tenant_id, grp, n FROM pr_ck WHERE tenant_id = $1 AND grp = $2", &[]).await;
        w.prepare("pc", "SELECT COUNT(*) FROM pr_ck WHERE tenant_id = $1", &[]).await;
        test_hooks::take_reads(pt.uid());
        test_hooks::take_keyed(pt.uid());
        for i in 0..12 {
            let (tenant, grp) = if i % 2 == 0 { (a, 1) } else { (b, 2) };
            let got = w.run("pt", &[t(tenant), t(grp)]).await;
            let n = (tenant * 100 + grp).to_string();
            assert_eq!(
                got.rows,
                vec![row(&[&tenant.to_string(), &grp.to_string(), &n])],
                "run {i}: tenant {tenant} got another tenant's rows"
            );
            let count = w.run("pc", &[t(tenant)]).await;
            assert_eq!(count.rows, vec![row(&["2"])], "run {i}: COUNT for tenant {tenant}");
            assert_eq!(
                test_hooks::take_reads(pt.uid()),
                vec![home(tenant), home(tenant)],
                "run {i}: tenant {tenant} (partition {}) read other partitions",
                home(tenant)
            );
            let keyed = test_hooks::take_keyed(pt.uid());
            assert_eq!(keyed, if i < 2 { 0 } else { 2 }, "run {i}: fan-outs planned from a bound key");
        }
    })
    .await;
    drop(pt);
    close(db, dir.path());
}

/// I2 for the cached derivation (S16b), as a property over the routing
/// functions themselves: for every key type a learned statement can bind
/// (SMALLINT, INT, BIGINT, TEXT, and a composite of BIGINT and TEXT) and
/// many values, edges and random, spelled as `pg_params::param_literal`
/// writes them, the partition the bound key names
/// (`RoutePlan::bind` → `keyed_range`) is the one `route_where` routes the
/// rendered `k = <literal>` to, and the one the insert path's point
/// (`KeySpec::point_of` over the stored bytes) lands in. A value the
/// derivation cannot read for certain (out of range, signed, a fraction, a
/// quote) binds no key, and only those: every unsigned in-range integer and
/// every plain string binds one. Signed values never reach the derivation
/// (`literal_class`); their text-path route is checked against the insert
/// point all the same.
///
/// Mutation: hash the literal's text instead of the value it names (bind a
/// number as a string): the property fails at the first integer.
#[tokio::test(flavor = "multi_thread")]
async fn the_cached_route_hashes_like_route_where_and_insert() {
    use crate::core::db_type::DbType;
    use crate::core::rql::prepared::RoutePlan;
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_i2").await;
    for (table, ty) in [("pr_h16", "SMALLINT"), ("pr_h32", "INT"), ("pr_h64", "BIGINT"), ("pr_hs", "TEXT")] {
        exec_ok(&db, &format!("CREATE TABLE {table} (k {ty} NOT NULL, v BIGINT) PARTITION BY HASH (k) PARTITIONS 64"))
            .await;
    }
    exec_ok(
        &db,
        "CREATE TABLE pr_hc (a BIGINT NOT NULL, s TEXT NOT NULL, v BIGINT) PARTITION BY HASH (a, s) PARTITIONS 64",
    )
    .await;

    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut ints: Vec<String> = [
        "0", "1", "9", "10", "007", "000", "127", "128", "255", "256", "32767", "32768", "65535", "65536",
        "2147483647", "2147483648", "4294967295", "4294967296", "9223372036854775807", "9223372036854775808",
        "18446744073709551615", "18446744073709551616", "1.5", "-1", "-32768", "-2147483648", "-9223372036854775808",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for _ in 0..300 {
        let r = next();
        ints.push(match r % 4 {
            0 => (r >> 2) % 32768,
            1 => (r >> 2) % (i32::MAX as u64 + 1),
            2 => (r >> 2) % (i64::MAX as u64 + 1),
            _ => r >> 1,
        }
        .to_string());
    }
    let mut strings: Vec<String> =
        ["''", "'abc'", "'42'", "'ünïcødé'", "'tenant-42'", "'it''s'", "'a b'"].iter().map(|s| s.to_string()).collect();
    for _ in 0..200 {
        let len = (next() % 12) as usize;
        let s: String = (0..len).map(|_| (b'a' + (next() % 26) as u8) as char).collect();
        strings.push(format!("'{s}'"));
    }

    // The stored bytes an INSERT of `lit` into a column of `ty` writes, by
    // the value the literal names; `None` when it holds no such value.
    let stored = |lit: &str, ty: &DbType| -> Option<Vec<u8>> {
        if *ty == DbType::STRING {
            let inner = lit.strip_prefix('\'')?.strip_suffix('\'')?;
            return Some(inner.replace("''", "'").into_bytes());
        }
        let v: i128 = lit.parse().ok()?;
        Some(match ty {
            DbType::I16 => i16::try_from(v).ok()?.to_le_bytes().to_vec(),
            DbType::I32 => i32::try_from(v).ok()?.to_le_bytes().to_vec(),
            DbType::I64 => i64::try_from(v).ok()?.to_le_bytes().to_vec(),
            _ => unreachable!(),
        })
    };
    // The one partition `route_where` reaches, or `None` for a wider route.
    let routed = |pt: &PartitionedTable, clause: &str| -> Option<u32> {
        let route = pt.route_where(clause).unwrap();
        let pids = route.pids(&pt.epoch().unwrap().map);
        (matches!(route, Route::Points(_)) && pids.len() == 1).then(|| pids[0])
    };
    // The one partition a bound key's range reaches.
    let keyed = |pt: &PartitionedTable, range: crate::core::partition::PointRange| -> u32 {
        let pids = pt.epoch().unwrap().map.pids_overlapping(range.lo, range.hi);
        assert_eq!(pids.len(), 1, "a full key's range spans {pids:?}");
        pids[0]
    };

    let mut bound = 0;
    for (table, ty) in [("pr_h16", DbType::I16), ("pr_h32", DbType::I32), ("pr_h64", DbType::I64), ("pr_hs", DbType::STRING)]
    {
        let pt = partitioned(&db, table);
        let level = &pt.spec().unwrap().levels()[0];
        assert_eq!(level.db_type, ty, "{table}: the declared key type");
        let plan = RoutePlan::derive(pt.uid(), &[(level.write_order, ty.clone(), "k")], &["k".to_string()]).unwrap();
        let lits = if ty == DbType::STRING { &strings } else { &ints };
        for lit in lits {
            let insert = stored(lit, &ty).map(|bytes| pid_at(&pt, pt.spec().unwrap().point_of(&[&bytes]).unwrap()));
            let text = routed(&pt, &format!("k = {lit}"));
            let cached = plan
                .bind(std::slice::from_ref(lit))
                .and_then(|key| pt.keyed_range(key.uid, &key.levels))
                .map(|range| keyed(&pt, range));
            if let Some(insert) = insert {
                if text.is_some() {
                    assert_eq!(text, Some(insert), "{table} {lit}: route_where and insert disagree");
                }
            }
            let certain = if ty == DbType::STRING {
                !lit[1..lit.len() - 1].contains('\'')
            } else {
                lit.bytes().all(|c| c.is_ascii_digit()) && insert.is_some()
            };
            assert_eq!(cached.is_some(), certain, "{table} {lit}: which values bind a key");
            if let Some(cached) = cached {
                bound += 1;
                assert_eq!(Some(cached), text, "{table} {lit}: the cached route is not route_where's");
                assert_eq!(Some(cached), insert, "{table} {lit}: the cached route is not the insert's");
            }
        }
    }

    // The composite key: both levels bound, in key order whatever the
    // parameter order.
    let pt = partitioned(&db, "pr_hc");
    let levels = pt.spec().unwrap().levels().to_vec();
    let plan = RoutePlan::derive(
        pt.uid(),
        &[(levels[0].write_order, DbType::I64, "a"), (levels[1].write_order, DbType::STRING, "s")],
        &["s".to_string(), "a".to_string()],
    )
    .unwrap();
    for (i, s) in strings.iter().enumerate().filter(|(_, s)| !s[1..s.len() - 1].contains('\'')) {
        let a = &ints[i % 20];
        let Some(a_bytes) = stored(a, &DbType::I64) else {
            continue;
        };
        let insert = pid_at(&pt, pt.spec().unwrap().point_of(&[&a_bytes, &stored(s, &DbType::STRING).unwrap()]).unwrap());
        let text = routed(&pt, &format!("s = {s} AND a = {a}"));
        let key = plan.bind(&[s.clone(), a.clone()]);
        if !a.bytes().all(|c| c.is_ascii_digit()) {
            assert!(key.is_none(), "pr_hc ({a}, {s}): a signed value bound a key");
            continue;
        }
        let cached = key.and_then(|key| pt.keyed_range(key.uid, &key.levels)).map(|range| keyed(&pt, range));
        assert!(cached.is_some(), "pr_hc ({a}, {s}): bound no key");
        bound += 1;
        assert_eq!(cached, text, "pr_hc ({a}, {s}): the cached route is not route_where's");
        assert_eq!(cached, Some(insert), "pr_hc ({a}, {s}): the cached route is not the insert's");
    }
    assert!(bound > 900, "only {bound} values exercised the cached route");
    close(db, dir.path());
}

/// §4.3/§4.4 with the route cached (S16b): a point `SELECT` and a
/// `COUNT(*)` for a tenant whose partition is not materialized learn their
/// parse and route answering nothing; that tenant's first row is written,
/// materializing the partition; the next executions, still planned from a
/// bound key, find it. The materialized flag is read per execution, after
/// its snapshot.
///
/// Mutation: keep the first answer per bound point (a memo in
/// `FanOut::plan`'s keyed branch): the new partition is never read.
#[tokio::test(flavor = "multi_thread")]
async fn a_cached_route_finds_a_partition_materialized_after_learning() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_mat").await;
    exec_ok(
        &db,
        "CREATE TABLE pr_cm (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 64",
    )
    .await;
    exec_ok(&db, "INSERT INTO pr_cm (tenant_id, id) VALUES (1, 1), (1, 2), (2, 1), (3, 1)").await;
    let pt = partitioned(&db, "pr_cm");
    let fresh = (100..1000i64)
        .find(|t| !materialized(&pt).contains(&pid_of(&pt, &t.to_le_bytes())))
        .expect("a tenant whose partition is not materialized");
    with_session(&db, async |w: &mut Wire| {
        w.prepare("c", "SELECT COUNT(*) FROM pr_cm WHERE tenant_id = $1", &[]).await;
        w.prepare("p", "SELECT id FROM pr_cm WHERE tenant_id = $1 AND id = $2", &[]).await;
        test_hooks::take_keyed(pt.uid());
        for _ in 0..3 {
            assert_eq!(w.run("c", &[t(fresh)]).await.rows, vec![row(&["0"])]);
            assert_eq!(w.run("c", &[t(1)]).await.rows, vec![row(&["2"])]);
            assert_eq!(w.run("p", &[t(fresh), t(5)]).await.rows, Vec::<Vec<Option<String>>>::new());
            assert_eq!(w.run("p", &[t(1), t(2)]).await.rows, vec![row(&["2"])]);
        }
        assert_eq!(test_hooks::take_keyed(pt.uid()), 8, "the cached route never answered");
        exec_ok(&db, &format!("INSERT INTO pr_cm (tenant_id, id) VALUES ({fresh}, 5)")).await;
        assert!(materialized(&pt).contains(&pid_of(&pt, &fresh.to_le_bytes())), "the first row materialized nothing");
        assert_eq!(
            w.run("c", &[t(fresh)]).await.rows,
            vec![row(&["1"])],
            "a cached route missed a row in a partition materialized after it learned"
        );
        assert_eq!(
            w.run("p", &[t(fresh), t(5)]).await.rows,
            vec![row(&["5"])],
            "a cached point route missed a partition materialized after it learned"
        );
        assert_eq!(test_hooks::take_keyed(pt.uid()), 2, "the last executions were not planned from a bound key");
    })
    .await;
    drop(pt);
    close(db, dir.path());
}

/// The cached derivation never outlives the table it was learned on
/// (S16b). One never-describing client (so only the schema generation
/// resets what a statement learned) prepares a point `SELECT` by `a` and
/// `b` once, and runs it across tables re-created under the same name with
/// another key column set each time: `HASH (a)`, `HASH (b)`, `HASH (a, b)`,
/// `HASH (a)` with `a` an INT, then a plain table, then `HASH (b)` again.
/// Every execution answers its own row and, on a partitioned table, reads
/// only the partition the live key names; the statement relearns, and its
/// route, on every table. An unrelated `CREATE TABLE` mid-run (a schema
/// change) relearns too, and the route stays right.
///
/// Mutation: drop both guards, the schema-generation reset in
/// `execute_watched` and the uid/key check in `keyed_range`: the old key's
/// route reads the wrong partition. Either guard alone holds (each was
/// mutated alone, and the test passed).
#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_table_never_routes_on_the_old_derivation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_ddl").await;
    // No `b` holds a digit of any `a`, so the first two executions place
    // every value and the template is learned from them.
    let rows: Vec<(i64, i64)> = [90i64, 99, 909, 990, 999, 9009, 9090, 9099]
        .iter()
        .enumerate()
        .map(|(i, b)| (i as i64 + 1, *b))
        .collect();
    let values: Vec<String> = rows.iter().map(|(a, b)| format!("({a}, {b}, {})", a * 1000 + b)).collect();
    let tables = [
        ("a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT", " PARTITION BY HASH (a) PARTITIONS 16"),
        ("a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT", " PARTITION BY HASH (b) PARTITIONS 16"),
        ("a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT", " PARTITION BY HASH (a, b) PARTITIONS 16"),
        ("a INT NOT NULL, b BIGINT NOT NULL, v BIGINT", " PARTITION BY HASH (a) PARTITIONS 16"),
        ("a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT", ""),
        ("a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT", " PARTITION BY HASH (b) PARTITIONS 16"),
    ];
    with_session(&db, async |w: &mut Wire| {
        w.prepare_quiet("p", "SELECT v FROM pr_dk WHERE a = $1 AND b = $2").await;
        for (n, (columns, partition)) in tables.iter().enumerate() {
            if n > 0 {
                exec_ok(&db, "DROP TABLE pr_dk").await;
            }
            exec_ok(&db, &format!("CREATE TABLE pr_dk ({columns}){partition}")).await;
            exec_ok(&db, &format!("INSERT INTO pr_dk (a, b, v) VALUES {}", values.join(", "))).await;
            let pt = match db.relation("pr_dk") {
                Some(Relation::Partitioned(pt)) => Some(pt),
                _ => None,
            };
            // Where a row's point puts it under this table's own key.
            let home = |pt: &PartitionedTable, a: i64, b: i64| {
                let spec = pt.spec().unwrap();
                let a_bytes = if columns.starts_with("a INT") { (a as i32).to_le_bytes().to_vec() } else { a.to_le_bytes().to_vec() };
                let key: Vec<Vec<u8>> = spec
                    .levels()
                    .iter()
                    .map(|l| if l.write_order == 0 { a_bytes.clone() } else { b.to_le_bytes().to_vec() })
                    .collect();
                let key: Vec<&[u8]> = key.iter().map(Vec::as_slice).collect();
                pid_at(pt, spec.point_of(&key).unwrap())
            };
            if let Some(pt) = &pt {
                test_hooks::take_reads(pt.uid());
                test_hooks::take_keyed(pt.uid());
            }
            for (i, (a, b)) in rows.iter().enumerate() {
                if n == 2 && i == 4 {
                    // A schema change that is not this table's.
                    exec_ok(&db, "CREATE TABLE pr_dk_other (x BIGINT)").await;
                }
                let got = w.run("p", &[t(a), t(b)]).await;
                assert_eq!(got.rows, vec![row(&[&(a * 1000 + b).to_string()])], "table {n} ({partition}): row ({a}, {b})");
                if let Some(pt) = &pt {
                    assert_eq!(
                        test_hooks::take_reads(pt.uid()),
                        vec![home(pt, *a, *b)],
                        "table {n} ({partition}): ({a}, {b}) read partitions its key does not name"
                    );
                }
            }
            if let Some(pt) = &pt {
                // Two learning executions, two more after the mid-run CREATE.
                let want = if n == 2 { 4 } else { 6 };
                assert_eq!(test_hooks::take_keyed(pt.uid()), want, "table {n} ({partition}): the route was not relearned");
            }
            if n == 2 {
                exec_ok(&db, "DROP TABLE pr_dk_other").await;
            }
        }
    })
    .await;
    close(db, dir.path());
}

/// Only the full-key point shape caches its route (S16b); every other
/// shape keeps routing its rendered `WHERE` per execution (S15), and all of
/// them answer as their text does. On a table keyed by `(a, b)`:
/// - `a = $1 AND b = $2`, and the same with the parameters swapped, cache
///   it;
/// - `a = $1` (part of the key), `a >= $1 AND b = $2` and
///   `a = $1 AND b >= $2` (a range on a key column), `a = $1 OR b = $2`, `a = $1 AND b = $2 OR a = $3`, and
///   `a = $1 AND b <> $2` do not.
///
/// Mutation: keep the derivation without checking it against `route_where`
/// on the verifying executions (`learn_route`): the range on `a` is bound
/// as an equality, reads one partition and misses rows.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_full_key_point_shape_caches_its_route() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_shapes").await;
    exec_ok(
        &db,
        "CREATE TABLE pr_sh (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT) PARTITION BY HASH (a, b) PARTITIONS 16",
    )
    .await;
    let values: Vec<String> =
        (1..=6i64).flat_map(|a| (1..=6i64).map(move |b| format!("({a}, {b}, {})", a * 10 + b))).collect();
    exec_ok(&db, &format!("INSERT INTO pr_sh (a, b, v) VALUES {}", values.join(", "))).await;
    let pt = partitioned(&db, "pr_sh");
    let shapes: [(&str, usize, bool); 8] = [
        ("a = $1 AND b = $2", 2, true),
        ("b = $2 AND a = $1", 2, true),
        ("a = $1", 1, false),
        ("a >= $1 AND b = $2", 2, false),
        ("a = $1 AND b >= $2", 2, false),
        ("a = $1 OR b = $2", 2, false),
        ("a = $1 AND b = $2 OR a = $3", 3, false),
        ("a = $1 AND b <> $2", 2, false),
    ];
    with_session(&db, async |w: &mut Wire| {
        for (clause, params, keyed) in shapes {
            let sql = format!("SELECT v FROM pr_sh WHERE {clause} ORDER BY v");
            w.prepare("s", &sql, &[]).await;
            take_learned_runs("pr_sh");
            test_hooks::take_keyed(pt.uid());
            for run in 0..6i64 {
                // Single digits, distinct within a run, different each run.
                let vals: Vec<i64> = (0..params as i64).map(|p| 1 + (run + 2 * p) % 6).collect();
                let lits: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
                if lits.iter().collect::<std::collections::HashSet<_>>().len() < lits.len() {
                    continue;
                }
                let got = w.run("s", &vals.iter().map(|v| t(v)).collect::<Vec<_>>()).await;
                let mut text_sql = format!("SELECT v FROM pr_sh WHERE {clause} ORDER BY v");
                for (i, lit) in lits.iter().enumerate().rev() {
                    text_sql = text_sql.replace(&format!("${}", i + 1), lit);
                }
                let want = w.simple(&text_sql).await;
                assert_eq!(got.rows, want.rows, "{clause} with {lits:?}: not the text's answer");
            }
            let learned = take_learned_runs("pr_sh").len() as u64;
            assert!(learned > 0, "{clause}: the parse was never learned");
            let planned = test_hooks::take_keyed(pt.uid());
            assert_eq!(
                planned,
                if keyed { learned } else { 0 },
                "{clause}: {planned} of {learned} learned runs were planned from a bound key"
            );
        }
    })
    .await;
    drop(pt);
    close(db, dir.path());
}

/// `keyed_range`, the executor's own guard on a bound key (S16b), refuses a
/// key bound for another table (another uid: a table re-created under the
/// name) or for another key (other columns, another type, another arity),
/// so a stale derivation routes the rendered `WHERE` even if the schema
/// generation reset that relearns it were missed. It accepts the key it was
/// bound for.
///
/// Mutation: drop the uid and key checks: a stale key is accepted.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_range_refuses_a_key_bound_for_another_table_or_key() {
    use crate::core::db_type::DbType;
    use crate::core::rql::prepared::RoutePlan;
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "prep_s16b_guard").await;
    exec_ok(&db, "CREATE TABLE pr_ga (a BIGINT NOT NULL, b BIGINT NOT NULL) PARTITION BY HASH (a) PARTITIONS 8").await;
    exec_ok(&db, "CREATE TABLE pr_gb (a BIGINT NOT NULL, b BIGINT NOT NULL) PARTITION BY HASH (a) PARTITIONS 8").await;
    exec_ok(&db, "CREATE TABLE pr_gc (a BIGINT NOT NULL, b BIGINT NOT NULL) PARTITION BY HASH (a, b) PARTITIONS 8").await;
    let (ga, gb, gc) = (partitioned(&db, "pr_ga"), partitioned(&db, "pr_gb"), partitioned(&db, "pr_gc"));
    let lits = ["7".to_string(), "9".to_string()];
    let columns = ["a".to_string(), "b".to_string()];
    let a_wo = ga.spec().unwrap().levels()[0].write_order;
    let key = RoutePlan::derive(ga.uid(), &[(a_wo, DbType::I64, "a")], &columns).unwrap().bind(&lits).unwrap();
    assert!(ga.keyed_range(key.uid, &key.levels).is_some(), "the key it was bound for");
    assert_eq!(gb.keyed_range(key.uid, &key.levels), None, "a key bound for another table's uid");
    // pr_ga's own uid, with keys that are not its key.
    let b_wo = gc.spec().unwrap().levels()[1].write_order;
    for (what, levels) in [
        ("another column", vec![(b_wo, DbType::I64, "a")]),
        ("another type", vec![(a_wo, DbType::I32, "a")]),
        ("another arity", vec![(a_wo, DbType::I64, "a"), (b_wo, DbType::I64, "b")]),
    ] {
        let Some(plan) = RoutePlan::derive(ga.uid(), &levels, &columns) else {
            panic!("{what}: derives no plan");
        };
        let key = plan.bind(&lits).unwrap();
        assert_eq!(ga.keyed_range(key.uid, &key.levels), None, "{what}");
    }
    drop((ga, gb, gc));
    close(db, dir.path());
}
