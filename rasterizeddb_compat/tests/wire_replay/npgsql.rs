//! npgsql's batching and auto-prepare, frame for frame (the owner's Q2
//! condition (b); §13 sequences `npgsql-batch`, `npgsql-batch-error`,
//! `npgsql-batch-lock` and `npgsql-auto-prepare`).
//!
//! Npgsql 8, defaults: a batch (`NpgsqlBatch`, or a command whose text holds
//! several statements) writes, per statement, Parse of the unnamed
//! statement with its parameters' types, Bind with every parameter and
//! every result column in binary (one result format code, 1), Describe of
//! the portal and Execute, then ONE Sync for the whole batch. An error
//! skips everything after it until that Sync. With `Max Auto Prepare` set, a
//! statement used often enough is prepared as `_autoN`: Parse, Describe of
//! the statement, Bind with binary results, Execute, Sync; from then on
//! Bind and Execute only, decoding by the cached description. The usage
//! threshold (`Auto Prepare Min Usages`, 5) only decides when the prepare
//! happens and sends nothing of its own, so one unprepared run stands for
//! the four.

use super::wire::{self, Param, Wire};
use super::{Target, Trace};

fn startup(t: &Target) -> Vec<(&str, &str)> {
    vec![("user", t.user), ("database", "postgres"), ("client_encoding", "UTF8")]
}

fn statement(sql: &str, oids: &[i32], params: &[Param]) -> Vec<wire::Frame> {
    vec![
        wire::parse("", sql, oids),
        wire::bind("", "", params, &[1]),
        wire::describe_p(""),
        wire::execute("", 0),
    ]
}

const INSERT: &str = "INSERT INTO npg_t (id, a, b) VALUES ($1, $2, $3)";
const BY_ID: &str = "SELECT a, b FROM npg_t WHERE id = $1";

fn insert(id: i32, a: &str, b: i32) -> Vec<wire::Frame> {
    statement(INSERT, &[23, 25, 23], &[Param::Int4(id), Param::BinText(a.to_string()), Param::Int4(b)])
}

/// The batches: a good one, one failing mid-way, one whose second statement
/// waits for a table lock past `lock_timeout`.
pub async fn batch(t: &Target) -> Trace {
    let mut admin = Wire::connect(&t.addr, &[("user", t.user), ("database", "postgres")]).await;
    for sql in ["DROP TABLE IF EXISTS npg_t", "DROP TABLE IF EXISTS npg_o"] {
        admin.simple(sql).await;
    }
    for sql in [
        "CREATE TABLE npg_t (id INT PRIMARY KEY, a TEXT, b INT)",
        "INSERT INTO npg_t (id, a, b) VALUES (1, 'x', 10)",
        "INSERT INTO npg_t (id, a, b) VALUES (2, 'y', 20)",
        "CREATE TABLE npg_o (id INT PRIMARY KEY)",
        "INSERT INTO npg_o (id) VALUES (1)",
    ] {
        admin.simple_ok(sql).await;
    }
    let mut n = Wire::connect(&t.addr, &startup(t)).await;
    let mut trace: Trace = Vec::new();

    // A good batch: an INSERT and a SELECT, one Sync.
    let mut frames = insert(3, "z", 30);
    frames.extend(statement(BY_ID, &[23], &[Param::Int4(3)]));
    frames.push(wire::sync());
    trace.push(("batch", wire::render(&n.exchange(&frames).await)));

    // An error mid-batch: the second INSERT violates the key; the SELECT
    // after it is skipped until the Sync.
    let mut frames = insert(4, "w", 40);
    frames.extend(insert(1, "dup", 0));
    frames.extend(statement("SELECT count(*) FROM npg_t", &[], &[]));
    frames.push(wire::sync());
    trace.push(("batch with an error", wire::render(&n.exchange(&frames).await)));
    // What the batch left: in PostgreSQL the statements before a Sync are
    // one implicit transaction, so the error rolled the first INSERT back.
    trace.push(("after the error", wire::render(&admin.simple("SELECT id FROM npg_t ORDER BY id").await)));

    // A lock wait mid-batch: another session holds ACCESS EXCLUSIVE on
    // npg_t, the batch's second statement reads it.
    let mut holder = Wire::connect(&t.addr, &[("user", t.user), ("database", "postgres")]).await;
    holder.simple_ok("BEGIN").await;
    holder.simple_ok("LOCK TABLE npg_t IN ACCESS EXCLUSIVE MODE").await;
    n.simple_ok("SET lock_timeout = '300ms'").await;
    let by_o = "SELECT id FROM npg_o WHERE id = $1";
    let mut frames = statement(by_o, &[23], &[Param::Int4(1)]);
    frames.extend(statement("SELECT a FROM npg_t WHERE id = $1", &[23], &[Param::Int4(1)]));
    frames.extend(statement(by_o, &[23], &[Param::Int4(1)]));
    frames.push(wire::sync());
    trace.push(("batch with a lock timeout", wire::render(&n.exchange(&frames).await)));
    holder.simple_ok("ROLLBACK").await;
    n.simple_ok("SET lock_timeout = 0").await;
    // Still in step after the skip: the next batch runs normally.
    let mut frames = statement(BY_ID, &[23], &[Param::Int4(1)]);
    frames.push(wire::sync());
    trace.push(("batch after the timeout", wire::render(&n.exchange(&frames).await)));

    for sql in ["DROP TABLE IF EXISTS npg_t", "DROP TABLE IF EXISTS npg_o"] {
        admin.simple(sql).await;
    }
    trace
}

const STAR: &str = "SELECT * FROM npg_a WHERE id = $1";

/// Auto-prepare across another session's ADD COLUMN. After the 0A000 the
/// replay re-prepares as `_auto1`, which is what a driver that answers it
/// by re-preparing sends (whether Npgsql does so by itself, or surfaces the
/// error once and re-prepares on the next use, is for the real-driver run).
pub async fn auto_prepare(t: &Target, error_mode: bool) -> Trace {
    let mut admin = Wire::connect(&t.addr, &[("user", t.user), ("database", "postgres")]).await;
    admin.simple("DROP TABLE IF EXISTS npg_a").await;
    for sql in [
        "CREATE TABLE npg_a (id INT PRIMARY KEY, a TEXT, b INT)",
        "INSERT INTO npg_a (id, a, b) VALUES (1, 'x', 10)",
        "INSERT INTO npg_a (id, a, b) VALUES (2, 'y', 20)",
    ] {
        admin.simple_ok(sql).await;
    }
    let mut params = startup(t);
    if error_mode {
        // PostgreSQL takes it as a placeholder setting and ignores it.
        params.push(("options", "-c rdb.shape_change=error"));
    }
    let mut n = Wire::connect(&t.addr, &params).await;
    let mut trace: Trace = Vec::new();
    let mut frames = statement(STAR, &[23], &[Param::Int4(1)]);
    frames.push(wire::sync());
    trace.push(("unprepared", wire::render(&n.exchange(&frames).await)));
    let prepare = |name: &str| {
        vec![
            wire::parse(name, STAR, &[23]),
            wire::describe_s(name),
            wire::bind("", name, &[Param::Int4(1)], &[1]),
            wire::execute("", 0),
            wire::sync(),
        ]
    };
    trace.push(("prepare _auto0", wire::render(&n.exchange(&prepare("_auto0")).await)));
    let run = |name: &str| {
        vec![
            wire::bind("", name, &[Param::Int4(2)], &[1]),
            wire::execute("", 0),
            wire::sync(),
        ]
    };
    trace.push(("run _auto0", wire::render(&n.exchange(&run("_auto0")).await)));
    admin.simple_ok("ALTER TABLE npg_a ADD COLUMN c INT").await;
    trace.push(("run _auto0 after ADD COLUMN", wire::render(&n.exchange(&run("_auto0")).await)));
    trace.push(("prepare _auto1", wire::render(&n.exchange(&prepare("_auto1")).await)));
    trace.push(("run _auto1", wire::render(&n.exchange(&run("_auto1")).await)));
    admin.simple("DROP TABLE IF EXISTS npg_a").await;
    trace
}

/// PostgreSQL 17's answers to [`batch`].
pub const PG17_BATCH: &[(&str, &str)] = &[
    ("batch", "1 2 n C(INSERT 0 1) 1 2 T[a/25/1,b/23/1] D['z'|0000001e] C(SELECT 1) Z(I)"),
    ("batch with an error", "1 2 n C(INSERT 0 1) 1 2 n E(23505) Z(I)"),
    ("after the error", "T[id/23/0] D['1'] D['2'] D['3'] C(SELECT 3) Z(I)"),
    ("batch with a lock timeout", "1 2 T[id/23/1] D[00000001] C(SELECT 1) E(55P03) Z(I)"),
    ("batch after the timeout", "1 2 T[a/25/1,b/23/1] D['x'|0000000a] C(SELECT 1) Z(I)"),
];

/// PostgreSQL 17's answers to [`auto_prepare`].
pub const PG17_AUTO_PREPARE: &[(&str, &str)] = &[
    ("unprepared", "1 2 T[id/23/1,a/25/1,b/23/1] D[00000001|'x'|0000000a] C(SELECT 1) Z(I)"),
    ("prepare _auto0", "1 t[23] T[id/23/0,a/25/0,b/23/0] 2 D[00000001|'x'|0000000a] C(SELECT 1) Z(I)"),
    ("run _auto0", "2 D[00000002|'y'|00000014] C(SELECT 1) Z(I)"),
    (
        "run _auto0 after ADD COLUMN",
        "E(0A000 M=cached plan must not change result type R=RevalidateCachedQuery) Z(I)",
    ),
    (
        "prepare _auto1",
        "1 t[23] T[id/23/0,a/25/0,b/23/0,c/23/0] 2 D[00000001|'x'|0000000a|NULL] C(SELECT 1) Z(I)",
    ),
    ("run _auto1", "2 D[00000002|'y'|00000014|NULL] C(SELECT 1) Z(I)"),
];

/// Documented differences in [`batch`]:
///
/// - **`batch with a lock timeout`**: PostgreSQL takes ACCESS SHARE at
///   Parse (parse analysis opens the table), so its Parse waits and fails
///   with 55P03; here Parse and Describe take no lock (§7 "Describe and
///   catalog reads take no lock: a deliberate deviation") and answer at
///   once, and the Execute waits and fails with the same 55P03. Either way
///   the error skips the rest of the batch until the Sync.
/// - **`after the error`**: the statements of one batch are NOT one
///   implicit transaction here; each Execute outside a block commits on its
///   own, so the INSERT before the failing one stays (`D['4']`). A known
///   divergence from PostgreSQL, not a lock-manager one (the simple
///   protocol's multi-statement string has the same gap, `pgwire.rs`
///   `handle_simple_query`); listed in §13 as found by B8 and not fixed.
pub const OURS_BATCH: &[(&str, &str)] = &[
    ("after the error", "T[id/23/0] D['1'] D['2'] D['3'] D['4'] C(SELECT 4) Z(I)"),
    (
        "batch with a lock timeout",
        "1 2 T[id/23/1] D[00000001] C(SELECT 1) 1 2 T[a/25/1] E(55P03) Z(I)",
    ),
];

/// The default `rowdescription` mode in [`auto_prepare`]: the stale Bind
/// runs and its rows come behind a RowDescription of the new columns (the
/// one result format applies to all). Npgsql decodes a prepared
/// statement's rows by its cached description and does not expect a
/// RowDescription there (from its source: not run).
pub const OURS_AUTO_PREPARE_ROWDESCRIPTION: &[(&str, &str)] = &[(
    "run _auto0 after ADD COLUMN",
    "2 T[id/23/1,a/25/1,b/23/1,c/23/1] D[00000002|'y'|00000014|NULL] C(SELECT 1) Z(I)",
)];

#[tokio::test]
async fn npgsql_batches_answer_as_postgresql_17() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, batch(&Target::ours(&server)))
        .await
        .expect("the replay hung");
    super::assert_trace("npgsql batch", &got, &super::with_differences(PG17_BATCH, OURS_BATCH));
}

#[tokio::test]
async fn npgsql_auto_prepare_in_error_mode_answers_as_postgresql_17() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, auto_prepare(&Target::ours(&server), true))
        .await
        .expect("the replay hung");
    super::assert_trace("npgsql auto-prepare, error mode", &got, &super::with_differences(PG17_AUTO_PREPARE, &[]));
}

#[tokio::test]
async fn npgsql_auto_prepare_in_rowdescription_mode_repairs_the_stale_run() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, auto_prepare(&Target::ours(&server), false))
        .await
        .expect("the replay hung");
    super::assert_trace(
        "npgsql auto-prepare, rowdescription mode",
        &got,
        &super::with_differences(PG17_AUTO_PREPARE, OURS_AUTO_PREPARE_ROWDESCRIPTION),
    );
}

use rasterizeddb_compat::TestServer;
