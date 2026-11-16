//! What a native-protocol statement goes through before the engine, and the
//! `ON CONFLICT` arbiter rule both protocols share.
//!
//! oxyredox, a proxy that reaches rdb over the native protocol alone, found
//! these on 2026-11-16: `CREATE UNIQUE INDEX` and `SELECT 1` were
//! "Unrecognized query"; `INSERT …; INSERT …` wrote the first row, reported
//! success and dropped the second; and `ON CONFLICT (rid) DO NOTHING`
//! reported 0 rows for a clash on a different unique column.

use rclite::Arc;

use super::*;

fn context(database: &Arc<Database>, authentication_required: bool) -> NativeContext {
    NativeContext { database: database.clone(), authentication_required }
}

async fn send(context: &NativeContext, superuser: bool, sql: &str) -> Vec<u8> {
    process_incoming_queries(
        context.clone(),
        std::sync::Arc::new(rastcp::session::Principal::new("svc", superuser)),
        sql.as_bytes().to_vec(),
    )
    .await
}

fn text(response: &[u8]) -> String {
    String::from_utf8_lossy(response.get(1..).unwrap_or_default()).to_string()
}

async fn ok(context: &NativeContext, sql: &str) -> Vec<u8> {
    let r = send(context, true, sql).await;
    assert_ne!(r.first(), Some(&3), "{sql}: {}", text(&r));
    r
}

/// The rows an acknowledgement says the statement affected.
async fn affected(context: &NativeContext, sql: &str) -> u64 {
    let r = ok(context, sql).await;
    assert!(matches!(r.first(), Some(0) | Some(1)) && r.len() >= 9, "{sql}: {:?}", r);
    u64::from_le_bytes(r[1..9].try_into().expect("8 bytes"))
}

async fn refused(context: &NativeContext, sql: &str) -> String {
    let r = send(context, true, sql).await;
    assert_eq!(r.first(), Some(&3), "{sql} must fail, got {:?}", r.first());
    text(&r)
}

async fn rows(context: &NativeContext, sql: &str) -> Vec<crate::core::row::row::Row> {
    let r = ok(context, sql).await;
    assert_eq!(r.first(), Some(&2), "{sql}: {:?}", r.first());
    crate::core::row::row::vec_into_rows(&r[1..]).expect("rows")
}

async fn database() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().expect("dir");
    let database = Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    (dir, database)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_unique_index_is_created_and_enforced_over_native() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE probe_t (id BIGINT PRIMARY KEY, c_email TEXT, c_n BIGINT)").await;
    ok(&c, "CREATE UNIQUE INDEX IF NOT EXISTS probe_t_email_uq ON probe_t (c_email)").await;
    // A re-run of a migration: tolerated, as the clause says.
    ok(&c, "CREATE UNIQUE INDEX IF NOT EXISTS probe_t_email_uq ON probe_t (c_email)").await;
    ok(&c, "CREATE UNIQUE INDEX probe_t_pair_uq ON probe_t (c_email, c_n)").await;
    // And the constraint form, with the engine's IF NOT EXISTS.
    ok(&c, "ALTER TABLE probe_t ADD CONSTRAINT IF NOT EXISTS probe_t_n_uq UNIQUE (c_n)").await;
    ok(&c, "ALTER TABLE probe_t ADD CONSTRAINT IF NOT EXISTS probe_t_n_uq UNIQUE (c_n)").await;

    assert_eq!(affected(&c, "INSERT INTO probe_t (id, c_email, c_n) VALUES (1, 'a@x', 1)").await, 1);
    let clash = refused(&c, "INSERT INTO probe_t (id, c_email, c_n) VALUES (2, 'a@x', 2)").await;
    assert!(clash.contains("duplicate"), "{clash}");
    let clash = refused(&c, "INSERT INTO probe_t (id, c_email, c_n) VALUES (3, 'b@x', 1)").await;
    assert!(clash.contains("duplicate"), "{clash}");
    let again = refused(&c, "CREATE UNIQUE INDEX probe_t_email_uq ON probe_t (c_email)").await;
    assert!(again.contains("already exists"), "{again}");

    // What a client checks at boot: every key, by name, through `pg_indexes`.
    let mut listed: Vec<String> = rows(&c, "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'probe_t'")
        .await
        .iter()
        .map(|r| String::from_utf8(r.columns[0].data.into_slice().to_vec()).expect("utf-8"))
        .collect();
    listed.sort();
    for name in ["probe_t_email_uq", "probe_t_n_uq", "probe_t_pair_uq", "probe_t_pkey"] {
        assert!(listed.iter().any(|l| l == name), "{name} missing from {listed:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_without_from_and_subqueries_answer_over_native() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE s (id BIGINT PRIMARY KEY, v TEXT)").await;
    ok(&c, "INSERT INTO s (id, v) VALUES (1, 'a')").await;

    assert_eq!(rows(&c, "SELECT 1").await.len(), 1);
    assert_eq!(rows(&c, "select 1;").await.len(), 1);
    assert_eq!(rows(&c, "SELECT now()").await.len(), 1);
    let yes = rows(&c, "SELECT EXISTS (SELECT 1 FROM s WHERE id = 1)").await;
    let no = rows(&c, "SELECT EXISTS (SELECT 1 FROM s WHERE id = 2)").await;
    assert_eq!((yes.len(), no.len()), (1, 1));
    assert_ne!(
        yes[0].columns[0].data.into_slice(),
        no[0].columns[0].data.into_slice(),
        "EXISTS must tell a present row from an absent one"
    );
    assert_eq!(rows(&c, "SELECT id FROM s WHERE id IN (SELECT id FROM s WHERE v = 'a')").await.len(), 1);
    // A comment is whitespace, wherever it sits.
    assert_eq!(rows(&c, "SELECT id FROM s -- the only row\n WHERE id = 1").await.len(), 1);
    assert_eq!(rows(&c, "SELECT id FROM s WHERE v = 'a' /* IN (SELECT ...) */").await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_statements_in_one_request_are_refused_and_none_runs() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)").await;

    let message = refused(&c, "INSERT INTO t (id) VALUES (10); INSERT INTO t (id) VALUES (11)").await;
    assert!(message.contains("one statement per request"), "{message}");
    refused(&c, "UPDATE t SET v = 'x' WHERE id = 10; DELETE FROM t WHERE id = 10").await;
    refused(&c, "SELECT 1; SELECT 2").await;
    assert!(rows(&c, "SELECT id FROM t").await.is_empty(), "nothing of a refused request may run");

    // One statement with a trailing `;`, or a `;` inside a string, is one.
    assert_eq!(affected(&c, "INSERT INTO t (id, v) VALUES (12, 'a;b');").await, 1);
    assert_eq!(rows(&c, "SELECT id FROM t WHERE v = 'a;b'").await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_needs_a_session_is_refused_by_name_over_native() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE t (id BIGINT PRIMARY KEY)").await;
    for sql in ["BEGIN", "begin transaction", "START TRANSACTION", "COMMIT", "ROLLBACK", "SAVEPOINT a"] {
        let m = refused(&c, sql).await;
        assert!(m.contains("transaction control"), "{sql}: {m}");
    }
    let m = refused(&c, "WITH x AS (SELECT id FROM t) SELECT id FROM x").await;
    assert!(m.contains("native protocol"), "{m}");
    let m = refused(&c, "SELECT id FROM (SELECT id FROM t) d").await;
    assert!(m.contains("native protocol"), "{m}");
    let m = refused(&c, "LOCK TABLE t IN ACCESS EXCLUSIVE MODE").await;
    assert!(m.contains("transaction blocks"), "{m}");
}

/// Administering the server takes a superuser over native as over pgwire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn administering_the_server_needs_a_superuser_over_native() {
    let (_dir, db) = database().await;
    let guarded = context(&db, true);
    for sql in ["BACKUP NOW", "RESTORE FROM 'x'", "SET CONFIG max_connections = 5", "KV FLUSHALL", "DROP DATABASE other"] {
        let r = send(&guarded, false, sql).await;
        assert_eq!(r.first(), Some(&3), "{sql}");
        assert!(text(&r).contains("permission denied"), "{sql}: {}", text(&r));
    }
    // Ordinary work is not gated.
    let r = send(&guarded, false, "CREATE TABLE mine (id BIGINT)").await;
    assert_ne!(r.first(), Some(&3), "{}", text(&r));
    // Nor is anything on a listener that asks nobody to authenticate.
    let open = context(&db, false);
    let r = send(&open, false, "SET CONFIG nonexistent_setting = 5").await;
    assert!(!text(&r).contains("permission denied"), "{}", text(&r));
}

/// `ON CONFLICT (k)` absorbs a clash on `k` alone; one on another unique key
/// is raised (PostgreSQL's 23505). Every path a row can take there: one row,
/// several, `INSERT … SELECT`, `ON CONSTRAINT`, `DO UPDATE`.
async fn the_arbiter_alone_is_absorbed_on(c: &NativeContext, t: &str) {
    let clash = |m: &str| m.contains("duplicate");
    // On the arbiter: absorbed.
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 1, 'a.example') ON CONFLICT (rid) DO NOTHING")).await, 1);
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 3, 'b.example') ON CONFLICT (rid) DO NOTHING")).await, 0);
    // On the arbiter and another key at once: still the arbiter's.
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 3, 'a.example') ON CONFLICT (rid) DO NOTHING")).await, 0);
    // The same when the arbiter is the key checked second: a check that
    // stopped at the first clash (the primary key) never saw it.
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 3, 'a.example') ON CONFLICT (c_domain) DO NOTHING")).await, 0);
    // On another key only: raised.
    let m = refused(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT (rid) DO NOTHING")).await;
    assert!(clash(&m), "{m}");
    // No target: every key is an arbiter. The other key named: absorbed.
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT DO NOTHING")).await, 0);
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT (c_domain) DO NOTHING")).await, 0);
    // By constraint name.
    assert_eq!(
        affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT ON CONSTRAINT {t}_domain_uq DO NOTHING")).await,
        0
    );
    let m = refused(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT ON CONSTRAINT {t}_pkey DO NOTHING")).await;
    assert!(clash(&m), "{m}");
    // Several rows: the clash on the other key fails the statement.
    let m = refused(
        c,
        &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id3', 4, 'c.example'), ('id4', 5, 'a.example') ON CONFLICT (rid) DO NOTHING"),
    )
    .await;
    assert!(clash(&m), "{m}");
    assert_eq!(
        affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 4, 'c.example'), ('id5', 5, 'e.example') ON CONFLICT (rid) DO NOTHING")).await,
        1
    );
    // From a SELECT.
    let m = refused(c, &format!("INSERT INTO {t} (rid, seq, c_domain) SELECT rid, seq, c_domain FROM {t}_src ON CONFLICT (rid) DO NOTHING")).await;
    assert!(clash(&m), "{m}");
    // DO UPDATE: the arbiter's row is updated; another key's clash is raised.
    let m = refused(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id6', 7, 'a.example') ON CONFLICT (rid) DO UPDATE SET seq = EXCLUDED.seq")).await;
    assert!(clash(&m), "{m}");
    assert_eq!(affected(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id1', 9, 'z.example') ON CONFLICT (rid) DO UPDATE SET seq = EXCLUDED.seq")).await, 1);

    let mut present: Vec<String> = rows(c, &format!("SELECT rid FROM {t}"))
        .await
        .iter()
        .map(|r| String::from_utf8(r.columns[0].data.into_slice().to_vec()).expect("utf-8"))
        .collect();
    present.sort();
    assert_eq!(present, vec!["id1".to_string(), "id5".to_string()]);
    let seq = rows(c, &format!("SELECT seq FROM {t} WHERE rid = 'id1'")).await;
    assert_eq!(seq[0].columns[0].data.into_slice(), &9i64.to_le_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_absorbs_its_arbiter_alone() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE t (rid TEXT PRIMARY KEY, seq BIGINT, c_domain TEXT)").await;
    ok(&c, "ALTER TABLE t ADD CONSTRAINT t_domain_uq UNIQUE (c_domain)").await;
    ok(&c, "CREATE TABLE t_src (rid TEXT, seq BIGINT, c_domain TEXT)").await;
    ok(&c, "INSERT INTO t_src (rid, seq, c_domain) VALUES ('id7', 6, 'a.example')").await;
    the_arbiter_alone_is_absorbed_on(&c, "t").await;
}

/// The same over a partitioned table, whose keys hold the partition key: the
/// row's one partition decides, and its violation carries the same keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_absorbs_its_arbiter_alone_on_a_partitioned_table() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE p (rid TEXT NOT NULL, seq BIGINT, c_domain TEXT, PRIMARY KEY (rid)) PARTITION BY HASH (rid) PARTITIONS 4").await;
    // Every unique key of a partitioned table holds the partition key, so
    // the second key here is (rid, c_domain): a clash on it alone needs the
    // same rid, which the primary key already catches. What is left to tell
    // apart is the arbiter named by its columns against the key by its name.
    assert_eq!(affected(&c, "INSERT INTO p (rid, seq, c_domain) VALUES ('id1', 1, 'a.example') ON CONFLICT (rid) DO NOTHING").await, 1);
    assert_eq!(affected(&c, "INSERT INTO p (rid, seq, c_domain) VALUES ('id1', 2, 'b.example') ON CONFLICT (rid) DO NOTHING").await, 0);
    assert_eq!(affected(&c, "INSERT INTO p (rid, seq, c_domain) VALUES ('id1', 2, 'b.example') ON CONFLICT ON CONSTRAINT p_pkey DO NOTHING").await, 0);
    assert_eq!(
        affected(&c, "INSERT INTO p (rid, seq, c_domain) VALUES ('id1', 3, 'c.example'), ('id2', 4, 'd.example') ON CONFLICT (rid) DO NOTHING").await,
        1
    );
    assert_eq!(rows(&c, "SELECT rid FROM p").await.len(), 2);
}

/// A boolean literal as a predicate, or as one side of one: what a query
/// builder writes for an empty `IN` list or an always-true filter. They
/// failed with "Unexpected end of token stream" from the planner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_literals_are_predicates() {
    let (_dir, db) = database().await;
    let c = context(&db, false);
    ok(&c, "CREATE TABLE s (id BIGINT PRIMARY KEY, v TEXT)").await;
    ok(&c, "INSERT INTO s (id, v) VALUES (1, 'a'), (2, 'b')").await;
    for (sql, expected) in [
        ("SELECT id FROM s WHERE false", 0),
        ("SELECT id FROM s WHERE true", 2),
        ("SELECT id FROM s WHERE FALSE", 0),
        ("SELECT id FROM s WHERE (id = 1) OR (false)", 1),
        ("SELECT id FROM s WHERE (id = 1) AND (true)", 1),
        ("SELECT id FROM s WHERE id = 1 OR false", 1),
        ("SELECT id FROM s WHERE NOT false", 2),
        ("SELECT id FROM s WHERE false OR v = 'b'", 1),
        ("SELECT id FROM s WHERE 1 = 0", 0),
    ] {
        assert_eq!(rows(&c, sql).await.len(), expected, "{sql}");
    }
    assert_eq!(affected(&c, "UPDATE s SET v = 'z' WHERE false").await, 0);
    assert_eq!(affected(&c, "DELETE FROM s WHERE false").await, 0);
    assert_eq!(rows(&c, "SELECT id FROM s").await.len(), 2);
}
