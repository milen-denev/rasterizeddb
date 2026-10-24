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
    let epoch = pt.epoch().unwrap();
    pt.materialized(&epoch, &Route::All).unwrap().into_iter().map(|(pid, _)| pid).collect()
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

    /// A simple query.
    async fn simple(&mut self, sql: &str) {
        let mut body = Vec::new();
        cstr(&mut body, sql);
        self.send(b'Q', &body).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
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
    with_session(&db, async |w: &mut Wire| {
        for (table, lock, block) in [("pr_lock", "", false), ("pr_lock", " FOR UPDATE", true), ("pr_lockp", "", false)] {
            let sql = format!("SELECT k, v FROM {table} WHERE k = $1{lock}");
            w.prepare("s", &sql, &[]).await;
            take_learned_runs(table);
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
        }
    })
    .await;
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
                        assert_eq!(
                            test_hooks::take_reads(pt.uid()),
                            vec![pid_of(&pt, &key(v))],
                            "{table} {name} {v}: not routed to its point's partition"
                        );
                    }
                }
            }
            let parses = parses_of(table) - before;
            assert!(parses <= 4, "{table}: {parses} parses in {executions} executions; the cache never answered");
        }
    })
    .await;
    close(db, dir.path());
}
