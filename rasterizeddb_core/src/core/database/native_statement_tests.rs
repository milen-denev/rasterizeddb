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
    NativeContext { database: database.clone(), authentication_required, catalog: None }
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
    // A name no constraint has is that error (42704), not the clash and not absorbed.
    for action in ["DO NOTHING", "DO UPDATE SET seq = 9"] {
        let m = refused(c, &format!("INSERT INTO {t} (rid, seq, c_domain) VALUES ('id2', 2, 'a.example') ON CONFLICT ON CONSTRAINT nope {action}")).await;
        assert!(m.contains("constraint \"nope\"") && m.contains("does not exist"), "{action}: {m}");
    }
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

/// A port another server holds is an error naming the way out, not a panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_taken_port_is_an_error_naming_the_way_out() {
    let holder = std::net::TcpListener::bind("127.0.0.1:0").expect("a port to hold");
    let port = holder.local_addr().unwrap().port();
    let listen = NativeListen {
        addr: "127.0.0.1".to_string(),
        port,
        ..NativeListen::default()
    };
    let error = match bind_native_listener(&listen, None).await {
        Ok(_) => panic!("bound a port another listener holds"),
        Err(e) => e,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    let message = error.to_string();
    for needle in ["--rdb-port", "--rdb-tcp off", &format!("grep {port}")] {
        assert!(message.contains(needle), "missing {needle:?}: {message}");
    }
    drop(holder);
}

// ---- the simple-query form (`NativeSession::run_simple`) -----------------

/// What a simple-query answer's PostgreSQL messages said.
#[derive(Debug, Default)]
struct SimpleSaid {
    /// Per result: its column names, its rows' text cells, its tag.
    results: Vec<(Vec<String>, Vec<Vec<Option<String>>>, String)>,
    errors: Vec<(String, String)>,
    /// The last `ReadyForQuery` status.
    status: u8,
}

fn read_simple(answer: &[u8]) -> SimpleSaid {
    assert_eq!(
        answer.first(),
        Some(&crate::pgwire::NativeSession::SIMPLE_ANSWER_FOR_TESTS),
        "{answer:?}"
    );
    let mut said = SimpleSaid::default();
    let (mut columns, mut rows) = (None::<Vec<String>>, Vec::new());
    let mut at = 1;
    let cstr = |b: &[u8], p: &mut usize| {
        let end = *p + b[*p..].iter().position(|c| *c == 0).expect("nul");
        let s = String::from_utf8_lossy(&b[*p..end]).to_string();
        *p = end + 1;
        s
    };
    while at < answer.len() {
        let tag = answer[at];
        let len = i32::from_be_bytes(answer[at + 1..at + 5].try_into().unwrap()) as usize;
        let body = &answer[at + 5..at + 1 + len];
        at += 1 + len;
        let mut p = 0;
        match tag {
            b'T' => {
                let n = i16::from_be_bytes(body[0..2].try_into().unwrap());
                p = 2;
                let mut names = Vec::new();
                for _ in 0..n {
                    names.push(cstr(body, &mut p));
                    p += 18;
                }
                columns = Some(names);
                rows = Vec::new();
            }
            b'D' => {
                let n = i16::from_be_bytes(body[0..2].try_into().unwrap());
                p = 2;
                let mut row = Vec::new();
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[p..p + 4].try_into().unwrap());
                    p += 4;
                    if l < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(
                            String::from_utf8_lossy(&body[p..p + l as usize]).to_string(),
                        ));
                        p += l as usize;
                    }
                }
                rows.push(row);
            }
            b'C' => {
                let tag = cstr(body, &mut p);
                said.results.push((
                    columns.take().unwrap_or_default(),
                    std::mem::take(&mut rows),
                    tag,
                ));
            }
            b'E' => {
                let (mut code, mut message) = (String::new(), String::new());
                while body[p] != 0 {
                    let field = body[p];
                    p += 1;
                    let value = cstr(body, &mut p);
                    match field {
                        b'C' => code = value,
                        b'M' => message = value,
                        _ => {}
                    }
                }
                said.errors.push((code, message));
            }
            b'Z' => said.status = body[0],
            _ => {}
        }
    }
    said
}

fn simple_request(sql: &str) -> Vec<u8> {
    let mut request = crate::pgwire::NATIVE_SIMPLE_QUERY_PREFIX.to_vec();
    request.extend_from_slice(sql.as_bytes());
    request
}

async fn simple(
    context: &NativeContext,
    session: Option<&crate::pgwire::NativeSession>,
    sql: &str,
) -> SimpleSaid {
    let answer = process_native_request(
        context.clone(),
        std::sync::Arc::new(rastcp::session::Principal::new("svc", true)),
        session,
        simple_request(sql),
    )
    .await;
    read_simple(&answer)
}

/// Counted on a connection of its own, outside any block under test.
async fn count(context: &NativeContext, table: &str) -> i64 {
    let session =
        crate::pgwire::NativeSession::new(context.database.clone(), "svc", true, false, true);
    let said = simple(
        context,
        Some(&session),
        &format!("SELECT count(*) AS n FROM {table}"),
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    said.results[0].1[0][0]
        .as_deref()
        .expect("a count")
        .parse()
        .expect("a number")
}

/// The form answers as pgwire would: column names (aliases included), text
/// cells, a NULL as NULL, a tag per statement, and several statements in one
/// request. A CTE runs, which the plain native path refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_simple_query_answers_with_names_text_and_tags() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let session = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, false);

    let said = simple(
        &context,
        Some(&session),
        "CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT, score NUMERIC(10,2)); \
         INSERT INTO notes (id, body, score) VALUES (1, 'one', 1.50), (2, NULL, 2.25)",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    assert_eq!(said.results.len(), 2, "{said:?}");
    assert_eq!(said.results[1].2, "INSERT 0 2");
    assert_eq!(said.status, b'I');

    let said = simple(
        &context,
        Some(&session),
        "SELECT id, body AS note, score FROM notes ORDER BY id",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    let (columns, rows, tag) = &said.results[0];
    assert_eq!(columns, &["id", "note", "score"]);
    assert_eq!(
        rows,
        &vec![
            vec![
                Some("1".to_string()),
                Some("one".to_string()),
                Some("1.50".to_string())
            ],
            vec![Some("2".to_string()), None, Some("2.25".to_string())],
        ]
    );
    assert_eq!(tag, "SELECT 2");

    let said = simple(
        &context,
        Some(&session),
        "WITH big AS (SELECT id FROM notes WHERE id > 1) SELECT count(*) AS n FROM big",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    assert_eq!(said.results[0].1, vec![vec![Some("1".to_string())]]);
}

/// Several statements are one implicit transaction, as over pgwire: a
/// failure in the second takes the first back, and the error carries its
/// SQLSTATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_simple_query_takes_its_earlier_statements_back() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let session = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    let said = simple(
        &context,
        Some(&session),
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);

    let said = simple(
        &context,
        Some(&session),
        "INSERT INTO t (id) VALUES (1); INSERT INTO no_such_table (id) VALUES (2)",
    )
    .await;
    assert_eq!(said.errors.len(), 1, "{said:?}");
    assert_eq!(said.errors[0].0, "42P01", "{said:?}");
    assert_eq!(
        count(&context, "t").await,
        0,
        "the first INSERT outlived the failed request"
    );
}

/// On rastcp/1 a `BEGIN` holds the connection's block across requests, of
/// either form; on rastcp/2 a block left open is rolled back and said so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_simple_query_block_belongs_to_a_dedicated_connection_only() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let dedicated = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, false);
    let said = simple(
        &context,
        Some(&dedicated),
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);

    let said = simple(
        &context,
        Some(&dedicated),
        "BEGIN; INSERT INTO t (id) VALUES (1)",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    assert_eq!(said.status, b'T');
    // The block is the connection's: a plain native request runs in it.
    let plain = process_native_request(
        context.clone(),
        std::sync::Arc::new(rastcp::session::Principal::new("svc", true)),
        Some(&dedicated),
        b"INSERT INTO t (id) VALUES (2)".to_vec(),
    )
    .await;
    assert_ne!(plain.first(), Some(&3), "{}", text(&plain));
    let said = simple(&context, Some(&dedicated), "ROLLBACK").await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    assert_eq!(said.status, b'I');
    assert_eq!(
        count(&context, "t").await,
        0,
        "the rolled-back block's rows stayed"
    );

    let shared = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    let said = simple(
        &context,
        Some(&shared),
        "BEGIN; INSERT INTO t (id) VALUES (3)",
    )
    .await;
    assert_eq!(said.errors.len(), 1, "{said:?}");
    assert_eq!(said.errors[0].0, "0A000");
    assert_eq!(said.status, b'I');
    assert_eq!(
        count(&context, "t").await,
        0,
        "a block left open on a shared connection was kept"
    );
    // A block that ends inside the request is fine there.
    let said = simple(
        &context,
        Some(&shared),
        "BEGIN; INSERT INTO t (id) VALUES (4); COMMIT",
    )
    .await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    assert_eq!(count(&context, "t").await, 1);
}

/// The prefix is what keeps a server without the form from running the text:
/// to the plain path it is two statements, which are refused whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_prefix_makes_a_marked_request_unrunnable_on_the_plain_path() {
    let request = simple_request("DELETE FROM t");
    let text = String::from_utf8_lossy(&request);
    assert!(crate::pgwire::split_statements(&text).len() > 1, "{text:?}");

    let (_dir, database) = database().await;
    let context = context(&database, false);
    // No session: refused in the form's own answer, not as a plain error.
    let said = simple(&context, None, "SELECT 1").await;
    assert_eq!(said.errors.len(), 1);
    assert_eq!(said.errors[0].0, "0A000");
}

/// The form skips the native path's own superuser gate and relies on
/// pgwire's, inside the statement's execution. So it must refuse what the
/// native path refuses: an authenticated user who is not a superuser may not
/// administer the server, nor slip the statement in after another one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_simple_query_keeps_the_superuser_gate() {
    let (_dir, database) = database().await;
    let context = context(&database, true);
    let user = crate::pgwire::NativeSession::new(database.clone(), "svc", false, true, true);
    for sql in [
        "SET CONFIG cache.size = 10",
        "CREATE ROLE intruder LOGIN PASSWORD 'x'",
        "SELECT 1; RESET QUERY STATS",
        "STORAGE REBALANCE DRY RUN",
    ] {
        let answer = process_native_request(
            context.clone(),
            std::sync::Arc::new(rastcp::session::Principal::new("svc", false)),
            Some(&user),
            simple_request(sql),
        )
        .await;
        let said = read_simple(&answer);
        assert_eq!(said.errors.len(), 1, "{sql}: {said:?}");
        assert_eq!(said.errors[0].0, "42501", "{sql}: {said:?}");
    }
}

/// A request as `user` on `session`, read as a simple-query answer.
async fn marked(
    context: &NativeContext,
    user: &str,
    session: Option<&crate::pgwire::NativeSession>,
    request: &[u8],
) -> SimpleSaid {
    let answer = process_native_request(
        context.clone(),
        std::sync::Arc::new(rastcp::session::Principal::new(user, true)),
        session,
        request.to_vec(),
    )
    .await;
    read_simple(&answer)
}

/// The header's options, and a header this server cannot read refused before
/// anything runs, so an option a client relies on is never dropped.
#[test]
fn a_simple_query_header_is_read_or_refused() {
    use crate::pgwire::{
        NativeSimpleOptions as SimpleOptions, native_parse_simple as parse_simple,
    };
    assert_eq!(parse_simple(b"SELECT 1"), None);
    assert_eq!(parse_simple(b"\x01SIMPLEX;SELECT 1"), None);
    let (options, text) = parse_simple(b"\x01SIMPLE;SELECT 1").unwrap().unwrap();
    assert_eq!((options, text), (SimpleOptions::default(), &b"SELECT 1"[..]));
    let (options, text) = parse_simple(b"\x01SIMPLE:cancel=a-B_9,rows=10;SELECT 1; SELECT 2")
        .unwrap()
        .unwrap();
    assert_eq!(options.cancel.as_deref(), Some("a-B_9"));
    assert_eq!(options.rows, Some(10));
    assert_eq!(text, b"SELECT 1; SELECT 2");
    for bad in [
        &b"\x01SIMPLE:rows=10"[..],
        b"\x01SIMPLE:rows=ten;SELECT 1",
        b"\x01SIMPLE:rows=1,rows=2;SELECT 1",
        b"\x01SIMPLE:cancel=a b;SELECT 1",
        b"\x01SIMPLE:cancel=;SELECT 1",
        b"\x01SIMPLE:timeout=5;SELECT 1",
        b"\x01SIMPLE:rows;SELECT 1",
    ] {
        assert!(
            parse_simple(bad).unwrap().is_err(),
            "{}",
            String::from_utf8_lossy(bad)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_option_runs_nothing() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let session =
        crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    simple(&context, Some(&session), "CREATE TABLE opt_t (id BIGINT)").await;
    let said = marked(
        &context,
        "svc",
        Some(&session),
        b"\x01SIMPLE:later=1;INSERT INTO opt_t (id) VALUES (1)",
    )
    .await;
    assert_eq!(said.errors.len(), 1, "{said:?}");
    assert_eq!(said.errors[0].0, "08P01");
    assert_eq!(count(&context, "opt_t").await, 0);
}

/// `rows=n` sends at most `n` rows of each result, and the tag still counts
/// them all. Mutation: the sink keeps every row (the first result has 5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_cap_keeps_the_first_rows_of_each_result_and_the_full_tag() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let session =
        crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    let said = simple(
        &context,
        Some(&session),
        "CREATE TABLE cap_t (id BIGINT, label TEXT); \
         INSERT INTO cap_t (id, label) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
    )
    .await;
    assert!(said.errors.is_empty(), "{said:?}");
    let said = marked(
        &context,
        "svc",
        Some(&session),
        b"\x01SIMPLE:rows=2;SELECT id, label FROM cap_t ORDER BY id; \
          SELECT id FROM cap_t WHERE id > 3 ORDER BY id; SELECT 7 AS seven",
    )
    .await;
    assert!(said.errors.is_empty(), "{said:?}");
    assert_eq!(said.results.len(), 3, "{said:?}");
    let (columns, rows, tag) = &said.results[0];
    assert_eq!(columns, &["id", "label"]);
    assert_eq!(
        rows,
        &[
            vec![Some("1".to_string()), Some("a".to_string())],
            vec![Some("2".to_string()), Some("b".to_string())],
        ]
    );
    assert_eq!(tag, "SELECT 5");
    assert_eq!(said.results[1].1.len(), 2);
    assert_eq!(said.results[1].2, "SELECT 2");
    assert_eq!(said.results[2].1, vec![vec![Some("7".to_string())]]);

    // A cap of 0 sends the descriptions and tags only.
    let said = marked(
        &context,
        "svc",
        Some(&session),
        b"\x01SIMPLE:rows=0;SELECT id FROM cap_t",
    )
    .await;
    assert_eq!(said.results[0].0, ["id"]);
    assert!(said.results[0].1.is_empty());
    assert_eq!(said.results[0].2, "SELECT 5");
}

/// A query sent with `cancel=<token>` is stopped by a CANCEL carrying the
/// token from another connection of the same user, with 57014; another
/// user's CANCEL finds nothing, and the token cannot be taken twice while
/// the query runs. Here the query waits for a table lock another block
/// holds, so it runs until something stops it.
///
/// Mutations: the query runs outside the token's scope (CANCEL 0, and the
/// wait outlasts the bound); the registry ignores the user (the stranger's
/// CANCEL stops it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_token_stops_its_own_users_query_only() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let holder =
        crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, false);
    let said = simple(
        &context,
        Some(&holder),
        "CREATE TABLE lock_t (id BIGINT); INSERT INTO lock_t (id) VALUES (1)",
    )
    .await;
    assert!(said.errors.is_empty(), "{said:?}");
    let said = simple(
        &context,
        Some(&holder),
        "BEGIN; LOCK TABLE lock_t IN ACCESS EXCLUSIVE MODE",
    )
    .await;
    assert!(said.errors.is_empty(), "{said:?}");
    assert_eq!(said.status, b'T');

    let waiter = std::sync::Arc::new(crate::pgwire::NativeSession::new(
        database.clone(),
        "svc",
        true,
        false,
        true,
    ));
    let started = std::time::Instant::now();
    let task = {
        let (context, waiter) = (context.clone(), waiter.clone());
        tokio::spawn(async move {
            marked(
                &context,
                "svc",
                Some(&waiter),
                b"\x01SIMPLE:cancel=tok-1;SELECT id FROM lock_t",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(!task.is_finished(), "the query did not wait for the lock");

    let other = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    let said = marked(
        &context,
        "svc",
        Some(&other),
        b"\x01SIMPLE:cancel=tok-1;SELECT 1",
    )
    .await;
    assert_eq!(said.errors.first().map(|e| e.0.as_str()), Some("08P01"), "{said:?}");

    let said = marked(&context, "stranger", None, b"\x01CANCEL;tok-1").await;
    assert_eq!(said.results[0].2, "CANCEL 0", "{said:?}");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!task.is_finished(), "another user's CANCEL stopped the query");

    let said = marked(&context, "svc", None, b"\x01CANCEL;tok-1").await;
    assert_eq!(said.results[0].2, "CANCEL 1", "{said:?}");
    let said = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("the cancelled query kept waiting")
        .unwrap();
    assert_eq!(said.errors.first().map(|e| e.0.as_str()), Some("57014"), "{said:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));

    // The token is free again once its query has ended.
    let said = marked(&context, "svc", None, b"\x01CANCEL;tok-1").await;
    assert_eq!(said.results[0].2, "CANCEL 0", "{said:?}");
    let said = simple(&context, Some(&holder), "COMMIT").await;
    assert!(said.errors.is_empty(), "{said:?}");
}

/// `SHOW COLUMNS` names each column's type as it was declared, length,
/// precision and element type included, in a form `CREATE TABLE` accepts
/// back: a table declared from the reported names reports the same names.
/// It read `TEXT` for a `VARCHAR(40)`, `NUMERIC` for a `NUMERIC(10,2)` and
/// `UNKNOWN` for a `DATE`, so a console editing a column offered a type the
/// column did not have.
///
/// Mutation: report the bare storage name (`TEXT`, `NUMERIC`, `UNKNOWN`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_columns_reports_the_declared_type_with_its_modifiers() {
    let (_dir, database) = database().await;
    let context = context(&database, false);
    let session =
        crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, true);
    let said = simple(
        &context,
        Some(&session),
        "CREATE TABLE typmod_t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(40), \
         code CHAR(3), price NUMERIC(10,2), plain NUMERIC, note TEXT, \
         born DATE, at TIMESTAMP(3), took INTERVAL, tags INTEGER[], \
         seen TIMESTAMPTZ)",
    )
    .await;
    assert!(said.errors.is_empty(), "{said:?}");

    async fn types(
        context: &NativeContext,
        session: &crate::pgwire::NativeSession,
        table: &str,
    ) -> Vec<(String, String)> {
        let request = format!("\x01SIMPLE;SHOW COLUMNS FROM {table}");
        let said = marked(context, "svc", Some(session), request.as_bytes()).await;
        assert!(said.errors.is_empty(), "{said:?}");
        let (columns, rows, _) = &said.results[0];
        let name = columns.iter().position(|c| c == "column_name").unwrap();
        let data_type = columns.iter().position(|c| c == "data_type").unwrap();
        rows.iter()
            .map(|r| (r[name].clone().unwrap(), r[data_type].clone().unwrap()))
            .collect()
    }

    let reported = types(&context, &session, "typmod_t").await;
    let expect = [
        ("id", "BIGINT"),
        ("name", "VARCHAR(40)"),
        ("code", "CHAR(3)"),
        ("price", "NUMERIC(10,2)"),
        ("plain", "NUMERIC"),
        ("note", "TEXT"),
        ("born", "DATE"),
        ("at", "TIMESTAMP(3)"),
        ("took", "INTERVAL"),
        ("tags", "INTEGER[]"),
        ("seen", "TIMESTAMPTZ"),
    ];
    let expect: Vec<(String, String)> =
        expect.iter().map(|(n, t)| (n.to_string(), t.to_string())).collect();
    assert_eq!(reported, expect);

    // Declared back from what was reported, the table reports the same.
    let ddl = reported
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ");
    let said = simple(&context, Some(&session), &format!("CREATE TABLE typmod_back ({ddl})")).await;
    assert!(said.errors.is_empty(), "{ddl}: {said:?}");
    assert_eq!(types(&context, &session, "typmod_back").await, expect);
}

/// The session's own values (`current_database()`, `current_user`,
/// `current_setting(…)`) answer over the plain native path as they do over
/// pgwire, where `session_values` puts them in the text. The native path did
/// not, so a native client could not ask which database it is connected to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_values_answer_over_the_plain_native_path() {
    let (_dir, database) = database().await;
    let context = context(&database, true);
    let session = crate::pgwire::NativeSession::new(database.clone(), "native_alice", true, true, true);
    let ask = |sql: &'static str| {
        let context = context.clone();
        let session = &session;
        async move {
            process_native_request(
                context,
                std::sync::Arc::new(rastcp::session::Principal::new("native_alice", true)),
                Some(session),
                sql.as_bytes().to_vec(),
            )
            .await
        }
    };
    let contains = |answer: &[u8], needle: &str| answer.windows(needle.len()).any(|w| w == needle.as_bytes());

    let answer = ask("SELECT current_database()").await;
    assert_eq!(answer.first(), Some(&2), "{}", text(&answer));
    assert!(contains(&answer, &database.name), "{} not in {:?}", database.name, text(&answer));

    let answer = ask("SELECT current_user, current_setting('server_version_num')").await;
    assert_eq!(answer.first(), Some(&2), "{}", text(&answer));
    assert!(contains(&answer, "native_alice"), "{:?}", text(&answer));
    assert!(contains(&answer, "160003"), "{:?}", text(&answer));

    // A parameter this server does not have is refused, not read as NULL.
    let answer = ask("SELECT current_setting('no_such_parameter')").await;
    assert_eq!(answer.first(), Some(&3), "{:?}", answer.first());
}

/// A native connection counts among its database's sessions in the server's
/// catalogue for as long as it is open, as a pgwire one does: `SHOW
/// DATABASES` said `sessions` 0 over either protocol while native clients
/// were connected, and `DROP DATABASE` did not wait for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_connection_counts_among_its_database_s_sessions() {
    let catalog = super::catalog::DatabaseCatalog::open(None, super::DEFAULT_DATABASE_NAME)
        .await
        .expect("a catalogue");
    let database = catalog.get(super::DEFAULT_DATABASE_NAME).expect("its default database");
    let context = NativeContext {
        database: database.clone(),
        authentication_required: false,
        catalog: Some(catalog.clone()),
    };
    let principal = std::sync::Arc::new(rastcp::session::Principal::new("svc", true));
    let sessions = || {
        catalog
            .listing()
            .into_iter()
            .find(|d| d.name == super::DEFAULT_DATABASE_NAME)
            .expect("listed")
            .sessions
    };
    assert_eq!(sessions(), 0);

    let one = native_session_for(&context, &principal, false);
    let two = native_session_for(&context, &principal, true);
    assert_eq!(sessions(), 2, "two native connections open");

    // Over native too, read through the connection's own session.
    let said = simple(&context, Some(&one), "SHOW DATABASES").await;
    assert!(said.errors.is_empty(), "{:?}", said.errors);
    let (columns, rows, _) = &said.results[0];
    let at = |name: &str| columns.iter().position(|c| c == name).expect(name);
    let row = rows
        .iter()
        .find(|r| r[at("database_name")].as_deref() == Some(super::DEFAULT_DATABASE_NAME))
        .expect("the database listed");
    assert_eq!(row[at("sessions")].as_deref(), Some("2"), "{said:?}");
    assert_eq!(row[at("is_current")].as_deref(), Some("t"), "{said:?}");

    drop(one);
    assert_eq!(sessions(), 1, "a closed connection is let go");
    drop(two);
    assert_eq!(sessions(), 0);

    // A listener serving a database of its own counts nothing anywhere.
    let alone = NativeContext { catalog: None, ..context.clone() };
    let _three = native_session_for(&alone, &principal, false);
    assert_eq!(sessions(), 0);
}

/// One request with the connection's session, which learns a parse for an
/// autocommit SELECT's template (`NativeSession::text_step`), and without
/// one, which learns nothing: the two answers, byte for byte.
async fn learned_and_plain(context: &NativeContext, session: &crate::pgwire::NativeSession, sql: &str) -> Vec<u8> {
    let principal = std::sync::Arc::new(rastcp::session::Principal::new("svc", true));
    let learned = process_native_request(context.clone(), principal.clone(), Some(session), sql.as_bytes().to_vec()).await;
    let plain = process_native_request(context.clone(), principal, None, sql.as_bytes().to_vec()).await;
    assert_eq!(learned, plain, "{sql}: the learned answer differs from the plain one ({})", text(&learned));
    learned
}

/// A native connection's learned SELECT answers exactly as the plain path:
/// while it is learned, from the learned parse, for a key that matches
/// nothing, after rows are added, after a NUMERIC column is added (its
/// scale typed by the plain path's retag), and for statements that never
/// learn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_native_select_answers_as_the_plain_path() {
    use std::sync::atomic::Ordering;
    let (_dir, database) = database().await;
    let context = context(&database, false);
    ok(&context, "CREATE TABLE lt (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)").await;
    for id in 1..=40 {
        ok(&context, &format!("INSERT INTO lt (id, v, s) VALUES ({id}, {}, 'n{id}')", id * 10)).await;
    }
    let session = crate::pgwire::NativeSession::new(database.clone(), "svc", true, false, false);
    let before = crate::pgwire::TEXT_RUNS.load(Ordering::Relaxed);
    for round in 0..3 {
        for id in [1, 2, 3, 5, 8, 13, 999] {
            learned_and_plain(&context, &session, &format!("SELECT id, v, s FROM lt WHERE id = {id}")).await;
        }
        for (a, b) in [(3, 9), (30, 45), (7, 7)] {
            learned_and_plain(&context, &session, &format!("SELECT id FROM lt WHERE id >= {a} AND id < {b} ORDER BY id")).await;
        }
        // Never learned: a LIKE, a NULL, a quote inside a string.
        learned_and_plain(&context, &session, "SELECT id FROM lt WHERE s LIKE 'n1%' ORDER BY id").await;
        learned_and_plain(&context, &session, "SELECT id FROM lt WHERE v = NULL").await;
        learned_and_plain(&context, &session, "SELECT id FROM lt WHERE s = 'it''s'").await;
        if round == 0 {
            for id in 41..=45 {
                ok(&context, &format!("INSERT INTO lt (id, v, s) VALUES ({id}, {}, 'n{id}')", id * 10)).await;
            }
        }
    }
    assert!(
        crate::pgwire::TEXT_RUNS.load(Ordering::Relaxed) > before,
        "nothing ran from a learned parse, so nothing was compared"
    );
    // A column added after the template was learned, with a scale of its own.
    ok(&context, "ALTER TABLE lt ADD COLUMN price NUMERIC(10, 2)").await;
    ok(&context, "UPDATE lt SET price = 1.25").await;
    for id in [7, 9, 44, 999] {
        let answer = learned_and_plain(&context, &session, &format!("SELECT id, v, s FROM lt WHERE id = {id}")).await;
        assert_ne!(answer.first(), Some(&3), "{}", text(&answer));
        learned_and_plain(&context, &session, &format!("SELECT * FROM lt WHERE id = {id}")).await;
        learned_and_plain(&context, &session, &format!("SELECT price, id FROM lt WHERE id = {id}")).await;
    }
}
