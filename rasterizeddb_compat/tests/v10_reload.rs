//! V10's reload cases 1–18 under `lock_timeout = 0` (slice B8, the W3 merge
//! gate; `docs/persistent-indexes.md` §13 V10, §7).
//!
//! Every session here runs with `lock_timeout = 0`, PostgreSQL's default and
//! this engine's (asserted by `unbounded`), so a missed wake-up or an edge
//! the graph cannot see is a hang. Each case runs under a harness bound
//! ([`BOUND`], 10 s), so a hang fails the test instead of the CI run.
//!
//! The cases built by earlier slices stay where they were written, and are
//! listed here so the gate reads in one place (test names are paths):
//!
//! | Case | Test |
//! |---|---|
//! | 1 own family | [`case_1_own_family_never_waits_on_itself`] |
//! | 2 R1, extended protocol | [`case_2_r1_over_the_extended_protocol`] |
//! | 3 Describe during a pending AE | [`case_3_describe_during_a_pending_access_exclusive`] |
//! | 4 prepared across ADD/DROP/retype, both modes | core `pgwire::shape_repair_tests::alter_between_describe_and_execute_repairs_every_execute`, `shape_change_error_mode_show_and_reset`; compat `prepared_shape_change` (all four tests), `driver_matrix::shape_change_mid_prepare_through_each_driver_in_both_modes` |
//! | 5 Describe during a rewrite; block INSERT survives | core `pgwire::ddl_rewrite_tests::a_block_insert_committed_while_the_alter_waits_survives`; [`case_5_describe_and_select_during_a_rewrite`] |
//! | 6 catalog views during an AE | core `pgwire::schema_view_tests::catalog_figures_answer_during_a_held_access_exclusive`; [`case_6_catalog_reads_during_a_pending_access_exclusive`] |
//! | 7 INSERT … SELECT against a block's CREATE INDEX | [`case_7_insert_select_against_a_blocks_create_index`] |
//! | 8 two blocks' CREATE INDEX, same and crossed tables | [`case_8_two_blocks_create_index_one_victim`] |
//! | 9 block UPDATE + CREATE INDEX vs autocommit UPDATE | core `pgwire::block_error_tests::a_cycle_through_a_conflict_retry_and_a_ddl_fails_only_the_ddl` |
//! | 10 autocommit join, crossed DDLs | [`case_10_autocommit_join_with_crossed_ddls`] |
//! | 11 shredded writers | core `tests/shredded_shadow_ddl.rs` (all four) |
//! | 12 block ALTER then ROLLBACK | core `pgwire::ddl_rewrite_tests::a_rewrite_keeps_every_versions_visibility` |
//! | 13 lock_timeout, statement_timeout, cancel, second DDL, shutdown | [`case_13_bounds_and_cancellation`], [`case_13_cancelled_ddl_declares_nothing_after_reopen`], [`case_13_shutdown_during_a_wait`]; compat `driver_matrix::a_lock_wait_is_cancelled_through_each_driver` |
//! | 14 the driver gate | compat `driver_matrix`, `wire_replay` |
//! | 15 one owner per statement | core `pgwire::owner_tests::every_piece_of_a_wire_statement_runs_under_one_owner`, `a_granted_statement_finishes_before_a_ddl_queued_behind_it` |
//! | 16 reader in a cycle with LOCK TABLE | [`case_16_reader_in_a_cycle_is_never_the_victim`] |
//! | 17 NUMERIC scale across DROP+CREATE | core `pgwire::shape_repair_tests::a_learned_parse_is_rechecked_after_the_lock` |
//! | 18 DROP of a shredded parent | [`case_18_drop_of_a_shredded_parent_waits_and_writes_nothing`] |
//!
//! Timing: the doc's "within 100 ms" for a Describe or a catalog read is
//! asserted as "answers while the DDL it would have waited behind is still
//! waiting, and within 2 s": the property is that it does not queue, and
//! this shared box's load makes 100 ms a flake.

#[path = "support/wire_frames.rs"]
mod wire;

use std::time::{Duration, Instant};

use rasterizeddb_compat::TestServer;
use wire::{Param, Reply, Wire};

const BOUND: Duration = Duration::from_secs(10);
/// How long "answers at once" may take on this box.
const PROMPT: Duration = Duration::from_secs(2);
/// How long a test watches a waiter to see that it waits.
const SETTLE: Duration = Duration::from_millis(400);

async fn bounded<F: std::future::Future>(what: &str, f: F) -> F::Output {
    tokio::time::timeout(BOUND, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: did not finish within {BOUND:?} (a hang under lock_timeout = 0)"))
}

fn addr(server: &TestServer) -> String {
    format!("127.0.0.1:{}", server.port())
}

/// A tokio-postgres session, checked to run under `lock_timeout = 0`.
async fn unbounded(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    let shown: Vec<String> = client
        .simple_query("SHOW lock_timeout")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(shown, ["0"], "a session does not start under lock_timeout = 0");
    client
}

/// A raw session (the extended protocol, frame by frame).
async fn raw(server: &TestServer) -> Wire {
    let mut w = Wire::connect(&addr(server), &[("user", "postgres"), ("database", "postgres")]).await;
    let r = w.simple_ok("SHOW lock_timeout").await;
    assert_eq!(wire::text_rows(&r), [[Some("0".to_string())]]);
    w
}

fn code(e: &tokio_postgres::Error) -> Option<String> {
    e.as_db_error().map(|d| d.code().code().to_string())
}

/// A statement on its own connection, spawned, so the test can watch it
/// wait.
fn spawn_sql(server: &TestServer, sql: &'static str) -> tokio::task::JoinHandle<Result<(), Option<String>>> {
    let url = server.url();
    tokio::spawn(async move {
        let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.expect("connect");
        tokio::spawn(conn);
        client.batch_execute(sql).await.map_err(|e| code(&e))
    })
}

/// The first column of every row, as text, over the simple protocol.
async fn text(c: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_string)),
            _ => None,
        })
        .collect()
}

async fn count(c: &tokio_postgres::Client, sql: &str) -> i64 {
    c.query_one(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

// ---------------------------------------------------------------------------
// Case 1: own family.
// ---------------------------------------------------------------------------

/// Case 1 (RV1, P2): a block's own locks never make its DDL wait. Two
/// INSERTs then CREATE INDEX; INSERT, SAVEPOINT, INSERT, CREATE INDEX, with
/// the plain, ROLLBACK TO and RELEASE variants; INSERT then ADD COLUMN. Each
/// block commits with exactly its surviving rows.
///
/// Mutation: an owner's own ROW EXCLUSIVE conflicts with its ACCESS
/// EXCLUSIVE (`TableLocks` grant check without the other-owner filter): the
/// first CREATE INDEX waits on itself and the bound fails.
#[tokio::test]
async fn case_1_own_family_never_waits_on_itself() {
    let server = TestServer::start().await.expect("server");
    let c = unbounded(&server).await;
    let variants: &[(&str, &[&str], i64)] = &[
        ("two inserts", &["INSERT INTO t (id, x) VALUES (1, 1)", "INSERT INTO t (id, x) VALUES (2, 2)", "CREATE INDEX t_i ON t (x)"], 2),
        (
            "savepoint",
            &["INSERT INTO t (id, x) VALUES (1, 1)", "SAVEPOINT s", "INSERT INTO t (id, x) VALUES (2, 2)", "CREATE INDEX t_i ON t (x)"],
            2,
        ),
        (
            "rollback to",
            &[
                "INSERT INTO t (id, x) VALUES (1, 1)",
                "SAVEPOINT s",
                "INSERT INTO t (id, x) VALUES (2, 2)",
                "ROLLBACK TO SAVEPOINT s",
                "CREATE INDEX t_i ON t (x)",
            ],
            1,
        ),
        (
            "release",
            &[
                "INSERT INTO t (id, x) VALUES (1, 1)",
                "SAVEPOINT s",
                "INSERT INTO t (id, x) VALUES (2, 2)",
                "RELEASE SAVEPOINT s",
                "CREATE INDEX t_i ON t (x)",
            ],
            2,
        ),
        ("add column", &["INSERT INTO t (id, x) VALUES (1, 1)", "ALTER TABLE t ADD COLUMN c INT"], 1),
    ];
    for (name, body, survivors) in variants {
        c.batch_execute("DROP TABLE IF EXISTS t").await.unwrap();
        c.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
        bounded(name, async {
            c.batch_execute("BEGIN").await.unwrap();
            for sql in *body {
                c.batch_execute(sql).await.unwrap_or_else(|e| panic!("{name}: {sql}: {e}"));
            }
            c.batch_execute("COMMIT").await.unwrap();
        })
        .await;
        assert_eq!(count(&c, "SELECT count(*) FROM t").await, *survivors, "{name}");
        // The index (or column) is there and the table answers through it.
        assert_eq!(count(&c, "SELECT count(*) FROM t WHERE x = 1").await, 1, "{name}");
    }
}

// ---------------------------------------------------------------------------
// Case 2: R1 over the extended protocol.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Style {
    /// Parse, Describe 'S', Sync; then Bind, Describe 'P', Execute, Sync.
    Separate,
    /// pgjdbc: Parse, Bind, Describe 'P', Execute, Sync in one write.
    Jdbc,
    /// sqlx: Parse, Describe 'S', Sync; then Bind, Execute, Sync.
    Sqlx,
}

/// One statement through `style`; returns (time to the Describe's answer,
/// every reply).
async fn run_style(w: &mut Wire, style: Style, name: &str, sql: &str, params: &[Param]) -> (Duration, Vec<Reply>) {
    let started = Instant::now();
    match style {
        Style::Jdbc => {
            w.write(&[
                wire::parse(name, sql, &[]),
                wire::bind("", name, params, &[]),
                wire::describe_p(""),
                wire::execute("", 0),
                wire::sync(),
            ])
            .await;
            // The Describe's answer is the first RowDescription or NoData.
            let mut replies = w.until(|r| matches!(r, Reply::RowDesc(_) | Reply::NoData | Reply::Error { .. })).await;
            let described = started.elapsed();
            if !matches!(replies.last(), Some(Reply::Error { .. })) {
                replies.extend(w.until_ready().await);
            } else {
                replies.extend(w.until_ready().await);
            }
            (described, replies)
        }
        Style::Separate | Style::Sqlx => {
            let mut replies = w.exchange(&[wire::parse(name, sql, &[]), wire::describe_s(name), wire::sync()]).await;
            let described = started.elapsed();
            let mut frames = vec![wire::bind("", name, params, &[])];
            if matches!(style, Style::Separate) {
                frames.push(wire::describe_p(""));
            }
            frames.push(wire::execute("", 0));
            frames.push(wire::sync());
            replies.extend(w.exchange(&frames).await);
            (described, replies)
        }
    }
}

/// Case 2 (R1): block A has written `t` through Parse/Bind/Execute; B's
/// autocommit DDL (CREATE INDEX, ADD COLUMN, DROP INDEX) waits on A's ROW
/// EXCLUSIVE; A then sends a new SELECT and a new `UPDATE t SET x = $1
/// WHERE id = $2`, each Described (the UPDATE's parameter types inferred).
/// Every Describe answers while B still waits, both statements finish
/// (rule (b): A holds the table), A commits, B finishes, and nobody gets
/// 40P01 or 55P03. In three framings: messages one by one, pgjdbc's
/// pipeline, sqlx's.
///
/// Mutation: rule (b) checks the waiter rather than the holder (A's new
/// SELECT queues behind B's pending ACCESS EXCLUSIVE): A and B form a
/// cycle and one of them gets 40P01.
#[tokio::test]
async fn case_2_r1_over_the_extended_protocol() {
    let ddls: &[(&str, &str)] = &[
        ("create index", "CREATE INDEX t_x ON t (x)"),
        ("add column", "ALTER TABLE t ADD COLUMN y INT"),
        ("drop index", "DROP INDEX t_old"),
    ];
    for (ddl_name, ddl) in ddls {
        for style in [Style::Separate, Style::Jdbc, Style::Sqlx] {
            let what = format!("{ddl_name} / {style:?}");
            let server = TestServer::start().await.expect("server");
            let admin = unbounded(&server).await;
            admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
            admin.batch_execute("CREATE INDEX t_old ON t (x)").await.unwrap();
            for k in 1..=3 {
                admin.batch_execute(&format!("INSERT INTO t (id, x) VALUES ({k}, {})", k * 10)).await.unwrap();
            }
            bounded(&what, async {
                let mut a = raw(&server).await;
                a.simple_ok("BEGIN").await;
                let r = a
                    .exchange(&[
                        wire::parse("", "INSERT INTO t (id, x) VALUES ($1, $2)", &[]),
                        wire::bind("", "", &[Param::Text("10".into()), Param::Text("100".into())], &[]),
                        wire::execute("", 0),
                        wire::sync(),
                    ])
                    .await;
                assert_eq!(wire::error_code(&r), None, "{what}: A's INSERT: {r:?}");
                let b = spawn_sql(&server, ddl);
                tokio::time::sleep(SETTLE).await;
                assert!(!b.is_finished(), "{what}: the DDL ran beside A's write");

                let (described, r) =
                    run_style(&mut a, style, "s1", "SELECT x FROM t WHERE id = $1", &[Param::Text("10".into())]).await;
                assert_eq!(wire::error_code(&r), None, "{what}: A's SELECT: {r:?}");
                assert!(described < PROMPT, "{what}: the SELECT's Describe took {described:?}");
                assert_eq!(wire::text_rows(&r), [[Some("100".to_string())]], "{what}: {r:?}");
                assert!(!b.is_finished(), "{what}: the DDL ran beside A's open block");

                let (described, r) = run_style(
                    &mut a,
                    style,
                    "u1",
                    "UPDATE t SET x = $1 WHERE id = $2",
                    &[Param::Text("7".into()), Param::Text("10".into())],
                )
                .await;
                assert_eq!(wire::error_code(&r), None, "{what}: A's UPDATE: {r:?}");
                assert!(described < PROMPT, "{what}: the UPDATE's Describe took {described:?}");
                if !matches!(style, Style::Jdbc) {
                    assert!(
                        r.contains(&Reply::ParamDesc(vec![23, 23])),
                        "{what}: the UPDATE's parameters were not inferred as int4: {r:?}"
                    );
                }
                assert!(r.contains(&Reply::Complete("UPDATE 1".into())), "{what}: {r:?}");
                assert!(!b.is_finished(), "{what}: the DDL ran beside A's open block");

                a.simple_ok("COMMIT").await;
                assert_eq!(b.await.unwrap(), Ok(()), "{what}: the DDL after A's COMMIT");
            })
            .await;
            assert_eq!(count(&admin, "SELECT count(*) FROM t WHERE id = 10 AND x = 7").await, 1, "{what}");
        }
    }
}

// ---------------------------------------------------------------------------
// Case 3: Describe during a pending ACCESS EXCLUSIVE.
// ---------------------------------------------------------------------------

/// Case 3: H holds ACCESS SHARE on `t` (an idle block), a DDL queues for
/// ACCESS EXCLUSIVE. A detached Describe, and a Describe from a block that
/// holds nothing on `t` (it holds `u`), both answer while the DDL waits,
/// with the old shape. Their Executes then wait; once H commits and the
/// DDL runs, each Execute carries a RowDescription of the new shape for ADD
/// COLUMN, and none for CREATE INDEX (it changes no shape). That is
/// `rdb.shape_change = rowdescription`; under the default, `auto` (B8b), the
/// ADD COLUMN Executes are each their statement's first stale use and are
/// refused with 0A000 after the build, no rows, as PostgreSQL refuses a
/// stale cached plan; CREATE INDEX sends rows alone in both.
///
/// Mutation: `reconcile_shape` always omits the description (the skip path
/// taken whatever the epoch): the ADD COLUMN Executes send rows with no
/// RowDescription, and the assertion on it fails.
#[tokio::test]
async fn case_3_describe_during_a_pending_access_exclusive() {
    for (ddl, changes_shape, mode) in [
        ("ALTER TABLE t ADD COLUMN c INT", true, Some("rowdescription")),
        ("CREATE INDEX t_x ON t (x)", false, Some("rowdescription")),
        ("ALTER TABLE t ADD COLUMN c INT", true, None),
        ("CREATE INDEX t_x ON t (x)", false, None),
    ] {
        let server = TestServer::start().await.expect("server");
        let admin = unbounded(&server).await;
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, x INT)",
            "CREATE TABLE u (id INT PRIMARY KEY)",
            "INSERT INTO t (id, x) VALUES (1, 10)",
            "INSERT INTO u (id) VALUES (1)",
        ] {
            admin.batch_execute(sql).await.unwrap();
        }
        bounded(ddl, async {
            let holder = unbounded(&server).await;
            holder.batch_execute("BEGIN").await.unwrap();
            holder.batch_execute("SELECT * FROM t").await.unwrap();
            let d = spawn_sql(&server, ddl);
            tokio::time::sleep(SETTLE).await;
            assert!(!d.is_finished(), "{ddl}: ran beside the reader");

            let mut detached = raw(&server).await;
            let mut block = raw(&server).await;
            if let Some(mode) = mode {
                detached.simple_ok(&format!("SET rdb.shape_change = {mode}")).await;
                block.simple_ok(&format!("SET rdb.shape_change = {mode}")).await;
            }
            block.simple_ok("BEGIN").await;
            block.simple_ok("SELECT * FROM u").await;
            let old = Reply::RowDesc(vec![("id".into(), 23, 0), ("x".into(), 23, 0)]);
            for (who, w) in [("detached", &mut detached), ("block", &mut block)] {
                let started = Instant::now();
                let r = w
                    .exchange(&[wire::parse("s", "SELECT * FROM t WHERE id = $1", &[]), wire::describe_s("s"), wire::sync()])
                    .await;
                assert!(started.elapsed() < PROMPT, "{ddl}: the {who} Describe waited");
                assert!(r.contains(&old), "{ddl}: the {who} Describe: {r:?}");
                assert!(!d.is_finished(), "{ddl}: the DDL finished before the {who} Describe was judged");
                w.write(&[wire::bind("", "s", &[Param::Text("1".into())], &[]), wire::execute("", 0), wire::sync()]).await;
            }
            // Both Executes wait behind the DDL: nothing comes back (the
            // BindComplete is held with the reply until the Execute ends).
            for (who, w) in [("detached", &mut detached), ("block", &mut block)] {
                assert_eq!(w.next_within(SETTLE).await, None, "{ddl}: the {who} Execute ran beside the pending DDL");
            }
            holder.batch_execute("COMMIT").await.unwrap();
            assert_eq!(d.await.unwrap(), Ok(()), "{ddl}");
            for (who, w) in [("detached", &mut detached), ("block", &mut block)] {
                let r = w.until_ready().await;
                if changes_shape && mode.is_none() {
                    assert_eq!(wire::error_code(&r).as_deref(), Some("0A000"), "{ddl}: {who}, default mode: {r:?}");
                    assert!(wire::text_rows(&r).is_empty(), "{ddl}: {who}: rows after the 0A000: {r:?}");
                    continue;
                }
                assert_eq!(wire::error_code(&r), None, "{ddl}: {who}: {r:?}");
                let described = r.iter().any(|m| matches!(m, Reply::RowDesc(_)));
                assert_eq!(described, changes_shape, "{ddl}: {who}: RowDescription sent = {described}: {r:?}");
                if changes_shape {
                    assert!(
                        r.contains(&Reply::RowDesc(vec![("id".into(), 23, 0), ("x".into(), 23, 0), ("c".into(), 23, 0)])),
                        "{ddl}: {who}: {r:?}"
                    );
                    assert_eq!(wire::text_rows(&r), [[Some("1".into()), Some("10".into()), None]], "{ddl}: {who}");
                } else {
                    assert_eq!(wire::text_rows(&r), [[Some("1".into()), Some("10".into())]], "{ddl}: {who}");
                }
            }
            // The block's 0A000 (default mode) ended it (R3): ROLLBACK.
            block.simple_ok(if changes_shape && mode.is_none() { "ROLLBACK" } else { "COMMIT" }).await;
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// Case 5: Describe and SELECT during a rewrite.
// ---------------------------------------------------------------------------

/// Case 5, over the wire: an ADD COLUMN that rewrites a populated table
/// waits behind a block's read; meanwhile a Describe answers with the old
/// shape and never 42P01, and a SELECT waits and then reads the new shape,
/// never 42P01. (The block INSERT that commits while the ALTER waits is
/// core `ddl_rewrite_tests::a_block_insert_committed_while_the_alter_waits_survives`.)
#[tokio::test]
async fn case_5_describe_and_select_during_a_rewrite() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
    for k in 1..=50 {
        admin.batch_execute(&format!("INSERT INTO t (id, x) VALUES ({k}, {k})")).await.unwrap();
    }
    bounded("case 5", async {
        let holder = unbounded(&server).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("SELECT * FROM t").await.unwrap();
        let alter = spawn_sql(&server, "ALTER TABLE t ADD COLUMN c INT DEFAULT 5");
        tokio::time::sleep(SETTLE).await;
        assert!(!alter.is_finished(), "the rewrite ran beside the reader");
        let mut w = raw(&server).await;
        let r = w.exchange(&[wire::parse("s", "SELECT * FROM t WHERE id = $1", &[]), wire::describe_s("s"), wire::sync()]).await;
        assert_eq!(wire::error_code(&r), None, "{r:?}");
        assert!(r.contains(&Reply::RowDesc(vec![("id".into(), 23, 0), ("x".into(), 23, 0)])), "{r:?}");
        // A simple-protocol `SELECT *` waits for the ALTER and then reads
        // the new shape, never 42P01. (Over the extended protocol, a Parse
        // naming the column being added answers 42703 at once instead of
        // waiting: Describe takes no lock, §7's deviation; PostgreSQL's
        // Parse would wait for the ALTER.)
        let reader = {
            let url = server.url();
            tokio::spawn(async move {
                let (c, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
                tokio::spawn(conn);
                c.simple_query("SELECT * FROM t WHERE id = 7").await.map_err(|e| code(&e)).map(|msgs| {
                    msgs.into_iter()
                        .filter_map(|m| match m {
                            tokio_postgres::SimpleQueryMessage::Row(r) => {
                                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect::<Vec<_>>())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
            })
        };
        tokio::time::sleep(SETTLE).await;
        assert!(!reader.is_finished(), "a SELECT ran beside the pending ALTER");
        holder.batch_execute("COMMIT").await.unwrap();
        assert_eq!(alter.await.unwrap(), Ok(()));
        assert_eq!(
            reader.await.unwrap(),
            Ok(vec![vec![Some("7".to_string()), Some("7".to_string()), Some("5".to_string())]]),
            "the waiting SELECT"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Case 6: catalog reads during a pending ACCESS EXCLUSIVE.
// ---------------------------------------------------------------------------

/// Case 6 (Q3): with a CREATE INDEX queued for ACCESS EXCLUSIVE behind an
/// idle-in-transaction reader, `pg_class`, `pg_stat_user_tables`,
/// `information_schema.columns` and sqlx's describe all answer while the
/// DDL still waits, with the old instance's figures.
///
/// Mutation: the catalog's figures read through a locking lookup
/// (`schema_view` taking ACCESS SHARE): each read queues behind the DDL and
/// the bound fails.
#[tokio::test]
async fn case_6_catalog_reads_during_a_pending_access_exclusive() {
    use sqlx::{Connection, Executor};
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
    for k in 1..=3 {
        admin.batch_execute(&format!("INSERT INTO t (id, x) VALUES ({k}, {k})")).await.unwrap();
    }
    bounded("case 6", async {
        let holder = unbounded(&server).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("SELECT * FROM t").await.unwrap();
        let d = spawn_sql(&server, "CREATE INDEX t_x ON t (x)");
        tokio::time::sleep(SETTLE).await;
        assert!(!d.is_finished(), "the CREATE INDEX ran beside the reader");

        let c = unbounded(&server).await;
        let started = Instant::now();
        let reltuples = text(&c, "SELECT reltuples FROM pg_class WHERE relname = 't'").await;
        assert_eq!(reltuples, [Some("3".to_string())], "pg_class");
        let live = text(&c, "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 't'").await;
        assert_eq!(live, [Some("3".to_string())], "pg_stat_user_tables");
        let cols = c
            .query("SELECT column_name FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position", &[])
            .await
            .expect("information_schema.columns");
        assert_eq!(cols.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>(), ["id", "x"]);
        let mut sx = sqlx::PgConnection::connect(&server.url()).await.unwrap();
        // sqlx describes a statement when it prepares it (Parse, Describe).
        let described = sx.prepare(sqlx::SqlStr::from_static("SELECT * FROM t WHERE id = $1")).await.expect("sqlx describe");
        assert_eq!(sqlx::Statement::columns(&described).len(), 2);
        assert!(started.elapsed() < PROMPT * 2, "the catalog reads took {:?}", started.elapsed());
        assert!(!d.is_finished(), "the DDL finished before the catalog reads were judged");
        holder.batch_execute("COMMIT").await.unwrap();
        assert_eq!(d.await.unwrap(), Ok(()));
    })
    .await;
}

// ---------------------------------------------------------------------------
// Case 7.
// ---------------------------------------------------------------------------

/// Case 7: block B has inserted key 1 into `t` (uncommitted); an autocommit
/// `INSERT INTO t SELECT id FROM u` meets that key and waits for B (a
/// conflict wait, an edge in the graph since B6), holding ACCESS SHARE on
/// `u`; B then runs CREATE INDEX on `u`, which waits for the INSERT's
/// ACCESS SHARE. Exactly one 40P01, to the DDL; B's block ends at once (R3),
/// its key 1 goes, and the INSERT … SELECT lands every row.
///
/// Mutation: the victim is the cycle's youngest member whatever its kind:
/// the INSERT … SELECT (O, minted after B's BEGIN) gets the 40P01.
#[tokio::test]
async fn case_7_insert_select_against_a_blocks_create_index() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY)").await.unwrap();
    admin.batch_execute("CREATE TABLE u (id INT, x INT)").await.unwrap();
    for k in 1..=20 {
        admin.batch_execute(&format!("INSERT INTO u (id, x) VALUES ({k}, {k})")).await.unwrap();
    }
    bounded("case 7", async {
        let b = unbounded(&server).await;
        b.batch_execute("BEGIN").await.unwrap();
        b.batch_execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
        let s = spawn_sql(&server, "INSERT INTO t (id) SELECT id FROM u");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!s.is_finished(), "the INSERT … SELECT did not meet B's key");
        let ddl = b.batch_execute("CREATE INDEX u_x ON u (x)").await.map_err(|e| code(&e));
        assert_eq!(ddl, Err(Some("40P01".to_string())), "B's CREATE INDEX");
        assert_eq!(s.await.unwrap(), Ok(()), "the INSERT … SELECT is never the victim");
        let e = b.batch_execute("SELECT 1").await.unwrap_err();
        assert_eq!(code(&e).as_deref(), Some("25P02"));
        b.batch_execute("ROLLBACK").await.expect("the victim's ROLLBACK");
    })
    .await;
    assert_eq!(count(&admin, "SELECT count(*) FROM t").await, 20);
}

// ---------------------------------------------------------------------------
// Case 8.
// ---------------------------------------------------------------------------

/// Case 8 (R3): two blocks each write, then each runs CREATE INDEX on what
/// the other holds: on one table, and on two crossed tables. One 40P01; the
/// victim's client then sends nothing, and the survivor finishes within
/// `deadlock_timeout` + 2 s anyway (the victim's locks went with its
/// error); the victim's next statement gets 25P02 and its ROLLBACK works.
///
/// Mutation: R3 off (the error only fails the block; its locks wait for
/// the client's ROLLBACK): the survivor waits for the victim's client and
/// misses its bound.
#[tokio::test]
async fn case_8_two_blocks_create_index_one_victim() {
    for crossed in [false, true] {
        let server = TestServer::start().await.expect("server");
        let admin = unbounded(&server).await;
        for sql in ["CREATE TABLE t1 (id INT PRIMARY KEY, x INT)", "CREATE TABLE t2 (id INT PRIMARY KEY, x INT)"] {
            admin.batch_execute(sql).await.unwrap();
        }
        let what = if crossed { "crossed tables" } else { "one table" };
        bounded(what, async {
            let a = std::sync::Arc::new(unbounded(&server).await);
            let b = std::sync::Arc::new(unbounded(&server).await);
            a.batch_execute("BEGIN").await.unwrap();
            a.batch_execute("INSERT INTO t1 (id, x) VALUES (1, 1)").await.unwrap();
            b.batch_execute("BEGIN").await.unwrap();
            let (b_writes, a_index, b_index) = if crossed {
                ("INSERT INTO t2 (id, x) VALUES (2, 2)", "CREATE INDEX t2_a ON t2 (x)", "CREATE INDEX t1_b ON t1 (x)")
            } else {
                ("INSERT INTO t1 (id, x) VALUES (2, 2)", "CREATE INDEX t1_a ON t1 (x)", "CREATE INDEX t1_b ON t1 (x)")
            };
            b.batch_execute(b_writes).await.unwrap();
            let a_ddl = {
                let a = a.clone();
                tokio::spawn(async move { a.batch_execute(a_index).await.map_err(|e| code(&e)) })
            };
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(!a_ddl.is_finished(), "{what}: A's CREATE INDEX ran beside B's write");
            let cycle_formed = Instant::now();
            let b_result = b.batch_execute(b_index).await.map_err(|e| code(&e));
            // Whichever was failed, the survivor's DDL finishes with nobody
            // sending ROLLBACK.
            let a_result = tokio::time::timeout(Duration::from_secs(3), a_ddl)
                .await
                .unwrap_or_else(|_| panic!("{what}: the survivor waited for the victim's client"))
                .unwrap();
            assert!(cycle_formed.elapsed() < Duration::from_secs(3), "{what}: the survivor took {:?}", cycle_formed.elapsed());
            let results = [&a_result, &b_result];
            assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1, "{what}: A {a_result:?}, B {b_result:?}");
            let (victim, survivor) = if a_result.is_err() { (&a, &b) } else { (&b, &a) };
            for r in results {
                if let Err(c) = r {
                    assert_eq!(c.as_deref(), Some("40P01"), "{what}");
                }
            }
            let e = victim.batch_execute("SELECT 1").await.unwrap_err();
            assert_eq!(code(&e).as_deref(), Some("25P02"), "{what}");
            victim.batch_execute("ROLLBACK").await.expect("the victim's ROLLBACK");
            survivor.batch_execute("COMMIT").await.expect("the survivor's COMMIT");
        })
        .await;
        assert_eq!(count(&admin, "SELECT count(*) FROM t1").await + count(&admin, "SELECT count(*) FROM t2").await, 1, "{what}");
    }
}

// ---------------------------------------------------------------------------
// Case 10.
// ---------------------------------------------------------------------------

/// Case 10: an autocommit join over `t1` and `t2` with crossed DDLs. H (a
/// block) reads `t2`; an autocommit CREATE INDEX on `t2` queues behind it;
/// the join takes ACCESS SHARE on `t1` and queues on `t2` behind that DDL;
/// then H runs CREATE INDEX on `t1`, which waits for the join. The cycle
/// H → join → DDL → H fails one of the DDLs with 40P01, never the join,
/// which returns its rows.
///
/// Mutation: an autocommit reader may be the victim (the youngest member
/// is picked): the join gets the 40P01.
#[tokio::test]
async fn case_10_autocommit_join_with_crossed_ddls() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    for sql in [
        "CREATE TABLE t1 (id INT PRIMARY KEY, x INT)",
        "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)",
        "INSERT INTO t1 (id, x) VALUES (1, 10)",
        "INSERT INTO t2 (id, t1_id) VALUES (5, 1)",
    ] {
        admin.batch_execute(sql).await.unwrap();
    }
    bounded("case 10", async {
        let h = unbounded(&server).await;
        h.batch_execute("BEGIN").await.unwrap();
        h.batch_execute("SELECT * FROM t2").await.unwrap();
        let d2 = spawn_sql(&server, "CREATE INDEX t2_x ON t2 (t1_id)");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let join = {
            let url = server.url();
            tokio::spawn(async move {
                let (c, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
                tokio::spawn(conn);
                c.query("SELECT t1.x, t2.id FROM t1 JOIN t2 ON t2.t1_id = t1.id", &[])
                    .await
                    .map(|rows| rows.iter().map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1))).collect::<Vec<_>>())
                    .map_err(|e| code(&e))
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!join.is_finished() && !d2.is_finished(), "the join or the DDL did not wait");
        let h_ddl = h.batch_execute("CREATE INDEX t1_x ON t1 (x)").await.map_err(|e| code(&e));
        if h_ddl.is_err() {
            let _ = h.batch_execute("ROLLBACK").await;
        } else {
            h.batch_execute("COMMIT").await.unwrap();
        }
        assert_eq!(join.await.unwrap(), Ok(vec![(10, 5)]), "the join");
        let d2_result = d2.await.unwrap();
        let victims: Vec<_> = [&h_ddl, &d2_result].into_iter().filter(|r| r.is_err()).collect();
        assert_eq!(victims.len(), 1, "H {h_ddl:?}, DDL {d2_result:?}");
        assert_eq!(victims[0], &Err(Some("40P01".to_string())));
    })
    .await;
}

// ---------------------------------------------------------------------------
// Case 13.
// ---------------------------------------------------------------------------

/// Case 13 (P4), the bounds: a queued Execute under `lock_timeout = 1s`
/// gets 55P03 while its Describe answered at once; a second DDL under
/// `lock_timeout = 5s` behind an unbounded first gets 55P03 and adds
/// nothing. (`statement_timeout` and a cancel of a lock wait are
/// `driver_matrix::a_lock_wait_is_cancelled_through_each_driver`.)
///
/// Mutation: the wait ignores the session's `lock_timeout`: the Execute
/// waits past the bound.
#[tokio::test]
async fn case_13_bounds_and_cancellation() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
    admin.batch_execute("INSERT INTO t (id, x) VALUES (1, 10)").await.unwrap();
    bounded("case 13: queued Execute", async {
        let holder = unbounded(&server).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("LOCK TABLE t IN ACCESS EXCLUSIVE MODE").await.unwrap();
        let mut w = Wire::connect(&addr(&server), &[("user", "postgres"), ("database", "postgres"), ("options", "-c lock_timeout=1000")]).await;
        let started = Instant::now();
        let r = w.exchange(&[wire::parse("s", "SELECT x FROM t WHERE id = $1", &[]), wire::describe_s("s"), wire::sync()]).await;
        assert!(started.elapsed() < PROMPT, "the Describe waited");
        assert!(r.contains(&Reply::RowDesc(vec![("x".into(), 23, 0)])), "{r:?}");
        let started = Instant::now();
        let r = w.exchange(&[wire::bind("", "s", &[Param::Text("1".into())], &[]), wire::execute("", 0), wire::sync()]).await;
        assert_eq!(wire::error_code(&r).as_deref(), Some("55P03"), "{r:?}");
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(900) && waited < Duration::from_secs(4), "waited {waited:?}");
        holder.batch_execute("COMMIT").await.unwrap();
    })
    .await;
    bounded("case 13: second DDL", async {
        let holder = unbounded(&server).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("SELECT * FROM t").await.unwrap();
        let first = spawn_sql(&server, "CREATE INDEX t_x ON t (x)");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let second = unbounded(&server).await;
        second.batch_execute("SET lock_timeout = '5s'").await.unwrap();
        let started = Instant::now();
        let e = second.batch_execute("ALTER TABLE t ADD COLUMN y INT").await.unwrap_err();
        assert_eq!(code(&e).as_deref(), Some("55P03"), "{e}");
        assert!(started.elapsed() >= Duration::from_millis(4500), "gave up after {:?}", started.elapsed());
        assert!(!first.is_finished(), "the unbounded DDL gave up");
        holder.batch_execute("COMMIT").await.unwrap();
        assert_eq!(first.await.unwrap(), Ok(()));
        let cols: i64 = count(&admin, "SELECT count(*) FROM information_schema.columns WHERE table_name = 't'").await;
        assert_eq!(cols, 2, "the timed-out ADD COLUMN added a column");
    })
    .await;
}

/// Case 13: a CancelRequest ends a CREATE INDEX stuck behind an
/// idle-in-transaction reader with 57014, and after a reopen of the
/// file-backed database the index is not declared. (`pg_cancel_backend` is
/// not implemented here; the CancelRequest is the wire's way to the same
/// cancel.)
///
/// Mutation: the wait ignores `cancel::tripped()`: the CREATE INDEX keeps
/// waiting after the cancel.
#[tokio::test]
async fn case_13_cancelled_ddl_declares_nothing_after_reopen() {
    let dir = tempfile_dir("v10_case13");
    let root = dir.to_string_lossy().to_string();
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let admin = unbounded(&server).await;
        admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").await.unwrap();
        admin.batch_execute("INSERT INTO t (id, x) VALUES (1, 10)").await.unwrap();
        bounded("case 13: cancel a DDL", async {
            let holder = unbounded(&server).await;
            holder.batch_execute("BEGIN").await.unwrap();
            holder.batch_execute("SELECT * FROM t").await.unwrap();
            let mut d = raw(&server).await;
            d.write(&[wire::query("CREATE INDEX t_x ON t (x)")]).await;
            assert_eq!(d.next_within(SETTLE).await, None, "the CREATE INDEX did not wait");
            d.cancel().await;
            let r = d.until_ready().await;
            assert_eq!(wire::error_code(&r).as_deref(), Some("57014"), "{r:?}");
            holder.batch_execute("COMMIT").await.unwrap();
        })
        .await;
        let n: i64 = count(&admin, "SELECT count(*) FROM pg_indexes WHERE indexname = 't_x'").await;
        assert_eq!(n, 0, "the cancelled CREATE INDEX declared its index");
        drop(admin);
        drop(server);
        // The log is registered per directory; a reopen in this process
        // must not find the old one.
        rasterizeddb_core::core::wal::remove_for(&root);
    }
    let server = TestServer::start_file_backed(&root).await.expect("reopen");
    let admin = unbounded(&server).await;
    let n: i64 = count(&admin, "SELECT count(*) FROM pg_indexes WHERE indexname = 't_x'").await;
    assert_eq!(n, 0, "the cancelled CREATE INDEX is declared after a reopen");
    assert_eq!(count(&admin, "SELECT count(*) FROM t").await, 1);
    drop(admin);
    drop(server);
    rasterizeddb_core::core::wal::remove_for(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Case 13: shutdown during a lock wait. There is no `shutdown_deadline`
/// setting; the server's runtime is stopped with its two-second shutdown
/// bound, so the harness drop returns promptly and the waiting client sees
/// its connection end rather than hang.
#[tokio::test]
async fn case_13_shutdown_during_a_wait() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE t (id INT PRIMARY KEY)").await.unwrap();
    let holder = unbounded(&server).await;
    holder.batch_execute("BEGIN").await.unwrap();
    holder.batch_execute("LOCK TABLE t IN ACCESS EXCLUSIVE MODE").await.unwrap();
    let waiter = spawn_sql(&server, "SELECT * FROM t");
    tokio::time::sleep(SETTLE).await;
    assert!(!waiter.is_finished());
    let started = Instant::now();
    tokio::task::spawn_blocking(move || drop(server)).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "shutdown took {:?}", started.elapsed());
    let r = tokio::time::timeout(BOUND, waiter).await.expect("the waiter hung past shutdown").unwrap();
    assert!(r.is_err(), "a read waiting on ACCESS EXCLUSIVE answered through a shutdown");
}

fn tempfile_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rdb_{tag}_{}_{}", std::process::id(), rand_suffix()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rand_suffix() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Case 16.
// ---------------------------------------------------------------------------

/// Case 16 (Q6): a reader in a cycle with a block's LOCK TABLE. B locks
/// `t2` ACCESS EXCLUSIVE; an autocommit join takes ACCESS SHARE on `t1` and
/// waits for `t2` holding it (a late or ordered wait holds its set, with
/// edges: `max_lock_restarts` is not built, so every reader is past it);
/// B then asks for `t1` ACCESS EXCLUSIVE. B gets the 40P01, the reader
/// never does and returns its rows.
///
/// Mutation: an autocommit reader may be the victim (the youngest member
/// is picked): the join gets the 40P01.
#[tokio::test]
async fn case_16_reader_in_a_cycle_is_never_the_victim() {
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    for sql in [
        "CREATE TABLE t1 (id INT PRIMARY KEY, x INT)",
        "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)",
        "INSERT INTO t1 (id, x) VALUES (1, 10)",
        "INSERT INTO t2 (id, t1_id) VALUES (5, 1)",
    ] {
        admin.batch_execute(sql).await.unwrap();
    }
    bounded("case 16", async {
        let b = unbounded(&server).await;
        b.batch_execute("BEGIN").await.unwrap();
        b.batch_execute("LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE").await.unwrap();
        let reader = {
            let url = server.url();
            tokio::spawn(async move {
                let (c, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
                tokio::spawn(conn);
                c.query("SELECT t1.x, t2.id FROM t1 JOIN t2 ON t2.t1_id = t1.id", &[])
                    .await
                    .map(|rows| rows.len())
                    .map_err(|e| code(&e))
            })
        };
        tokio::time::sleep(SETTLE).await;
        assert!(!reader.is_finished(), "the reader ran beside ACCESS EXCLUSIVE");
        let e = b.batch_execute("LOCK TABLE t1 IN ACCESS EXCLUSIVE MODE").await.unwrap_err();
        assert_eq!(code(&e).as_deref(), Some("40P01"), "the block's LOCK TABLE: {e}");
        assert_eq!(reader.await.unwrap(), Ok(1), "the reader");
        b.batch_execute("ROLLBACK").await.unwrap();
    })
    .await;
}

// ---------------------------------------------------------------------------
// Case 18.
// ---------------------------------------------------------------------------

/// Case 18 (Q8): DROP of a shredded parent while a block holds it (it wrote
/// a document, so the parent's ROW EXCLUSIVE covers its shadow): the DROP
/// waits in step 1, before anything is retired; `lock_timeout` (55P03) or a
/// cancel (57014) ends it having written nothing: the parent, its shadow
/// and the block's document all survive, and once the block commits a DROP
/// runs at once (nothing was left waiting).
///
/// Mutation: DROP retires the instance before its ACCESS EXCLUSIVE is
/// granted (`execute_drop_table` drops first, locks after): the timed-out
/// DROP has removed the table.
#[tokio::test]
async fn case_18_drop_of_a_shredded_parent_waits_and_writes_nothing() {
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    assert_eq!(rasterizeddb_core::JSON_SHADOW_TABLES.get(), Some(&true), "this binary must run shredded");
    let server = TestServer::start().await.expect("server");
    let admin = unbounded(&server).await;
    admin.batch_execute("CREATE TABLE p (id INT NOT NULL, doc JSONB NULL)").await.unwrap();
    admin.batch_execute(r#"INSERT INTO p (id, doc) VALUES (1, '{"a":1}')"#).await.unwrap();
    bounded("case 18", async {
        let w = unbounded(&server).await;
        w.batch_execute("BEGIN").await.unwrap();
        w.batch_execute(r#"INSERT INTO p (id, doc) VALUES (2, '{"a":2,"b":"two"}')"#).await.unwrap();

        let d = unbounded(&server).await;
        d.batch_execute("SET lock_timeout = '1s'").await.unwrap();
        let e = d.batch_execute("DROP TABLE p").await.unwrap_err();
        assert_eq!(code(&e).as_deref(), Some("55P03"), "{e}");

        let mut c = raw(&server).await;
        c.write(&[wire::query("DROP TABLE p")]).await;
        assert_eq!(c.next_within(SETTLE).await, None, "the DROP did not wait");
        c.cancel().await;
        let r = c.until_ready().await;
        assert_eq!(wire::error_code(&r).as_deref(), Some("57014"), "{r:?}");

        w.batch_execute("COMMIT").await.unwrap();
        let docs = text(&admin, "SELECT doc FROM p ORDER BY id").await;
        assert_eq!(docs.len(), 2, "{docs:?}");
        let second = docs[1].clone().unwrap_or_default();
        assert!(second.contains("\"b\"") && second.contains("two"), "{docs:?}");
        let started = Instant::now();
        admin.batch_execute("DROP TABLE p").await.expect("the DROP once the block ended");
        assert!(started.elapsed() < PROMPT, "the last DROP waited on something left behind");
    })
    .await;
}
