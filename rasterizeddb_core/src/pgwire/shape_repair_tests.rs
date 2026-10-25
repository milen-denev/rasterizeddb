//! Execute's shape repair, completed (slice B7, `docs/persistent-indexes.md`
//! §7 "The client's shape changes only at Describe", `shape_epoch`, the
//! per-session mode; findings L7, L10, L13; Q2, Q7).
//!
//! Each test speaks the extended protocol over a duplex stream to a real
//! session loop and records every message that comes back: the
//! RowDescription actually sent (column names and type OIDs), the DataRows
//! as text, and errors by sqlstate. What is asserted is what a driver would
//! decode, never just "no error".
//!
//! - A statement described, then ALTERed by another session, then executed:
//!   every Execute carries a RowDescription in the new shape ahead of rows
//!   that match it, until the client describes again.
//! - An ALTER landing while the Execute is parked before its lock (the
//!   window between what it read before the lock and the lock itself) is
//!   caught by the comparison made after the result is built (L7), on the
//!   text path and on a learned parse (`shape_epoch` re-checked after the
//!   lock).
//! - RETURNING goes through the same repair (L13).
//! - `shape_epoch` is one database-wide counter (Q7): a join whose two
//!   tables are ALTERed one after the other repairs both times, and a DROP
//!   and CREATE never repeats an epoch a learned parse was taken under.
//! - `rdb.shape_change = error` answers 0A000 with PostgreSQL's message;
//!   SHOW reports the mode and RESET restores the default (Q2).
//! - The default `auto` (slice B8b, Q1, finding L10) answers a stale
//!   statement's first use as `error` and its re-executes without a new
//!   Parse as `rowdescription`; a new Parse or Describe starts over.
//!
//! The B7 tests pin `rowdescription`, the repair they test
//! ([`Client::connect`]); the default's own tests connect with
//! [`Client::connect_default`].
//! - A partitioned table refuses ALTER, and a plain table joined with one
//!   still repairs, by logical name.

use super::*;
use crate::core::database::table_for::test_hooks;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

const BOUND: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------
// The database and sessions.
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

fn state(db: &Arc<Database>) -> SessionState {
    let mut st = default_state(db);
    st.shape_change = ShapeChangeMode::RowDescription;
    st
}

fn default_state(db: &Arc<Database>) -> SessionState {
    SessionState::new(
        DatabaseCatalog::single(db.clone()),
        db.name.clone(),
        AuthConfig { method: crate::auth::AuthMethod::Trust, ..AuthConfig::default() },
        Principal { name: "rasterized".to_string(), superuser: true, authenticated: false },
        ConnectionLimits::default(),
    )
}

/// One message the server sent.
#[derive(Debug, Clone, PartialEq)]
enum Msg {
    /// RowDescription: each column's name and type OID.
    T(Vec<(String, i32)>),
    /// DataRow, cells as text.
    D(Vec<Option<String>>),
    /// ErrorResponse: sqlstate and message.
    E(String, String),
    /// CommandComplete tag.
    C(String),
    /// NoData.
    N,
}

/// A client whose session loop runs on its own task.
struct Client {
    s: DuplexStream,
    server: Option<tokio::task::JoinHandle<()>>,
}

fn cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

impl Client {
    /// A session in `rdb.shape_change = rowdescription`, the repair B7's
    /// tests are about.
    fn connect(db: &Arc<Database>) -> Client {
        Client::connect_with(db, state)
    }

    /// A session in the server's default mode (`auto`).
    fn connect_default(db: &Arc<Database>) -> Client {
        Client::connect_with(db, default_state)
    }

    fn connect_with(db: &Arc<Database>, make: fn(&Arc<Database>) -> SessionState) -> Client {
        let (mut server, client) = tokio::io::duplex(1 << 20);
        let db = db.clone();
        let handle = tokio::spawn(async move {
            let mut st = make(&db);
            let _ = Box::pin(run_message_loop_inner(&db, &mut st, &mut server)).await;
        });
        Client { s: client, server: Some(handle) }
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut buf = vec![tag];
        buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(body);
        self.s.write_all(&buf).await.unwrap();
    }

    /// Every message up to ReadyForQuery.
    async fn until_ready(&mut self) -> Vec<Msg> {
        let mut out = Vec::new();
        loop {
            let tag = self.s.read_u8().await.unwrap();
            let len = self.s.read_i32().await.unwrap() as usize;
            let mut body = vec![0u8; len - 4];
            self.s.read_exact(&mut body).await.unwrap();
            match tag {
                b'T' => {
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut at = 2;
                    let mut cols = Vec::with_capacity(n);
                    for _ in 0..n {
                        let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                        let name = String::from_utf8_lossy(&body[at..end]).into_owned();
                        let oid_at = end + 1 + 4 + 2;
                        let oid = i32::from_be_bytes(body[oid_at..oid_at + 4].try_into().unwrap());
                        cols.push((name, oid));
                        at = end + 1 + 18;
                    }
                    out.push(Msg::T(cols));
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
                    out.push(Msg::D(row));
                }
                b'E' => {
                    let fields: Vec<&[u8]> = body.split(|b| *b == 0).collect();
                    let field = |k: u8| {
                        fields
                            .iter()
                            .find(|f| f.first() == Some(&k))
                            .map(|f| String::from_utf8_lossy(&f[1..]).into_owned())
                            .unwrap_or_default()
                    };
                    out.push(Msg::E(field(b'C'), field(b'M')));
                }
                b'C' => out.push(Msg::C(String::from_utf8_lossy(&body[..body.len() - 1]).into_owned())),
                b'n' => out.push(Msg::N),
                b'Z' => return out,
                _ => {}
            }
        }
    }

    async fn simple(&mut self, sql: &str) -> Vec<Msg> {
        let mut body = Vec::new();
        cstr(&mut body, sql);
        self.send(b'Q', &body).await;
        self.until_ready().await
    }

    /// A simple query that must succeed.
    async fn ok(&mut self, sql: &str) -> Vec<Msg> {
        let out = self.simple(sql).await;
        assert!(!out.iter().any(|m| matches!(m, Msg::E(..))), "{sql}: {out:?}");
        out
    }

    /// Parse, Describe the statement, Sync: what the client is told.
    async fn prepare(&mut self, name: &str, sql: &str) -> Vec<Msg> {
        let mut parse = Vec::new();
        cstr(&mut parse, name);
        cstr(&mut parse, sql);
        parse.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &parse).await;
        let mut describe = vec![b'S'];
        cstr(&mut describe, name);
        self.send(b'D', &describe).await;
        self.send(b'S', &[]).await;
        self.until_ready().await
    }

    /// Parse alone, Sync: a client that never describes (B3b's bench).
    async fn parse_only(&mut self, name: &str, sql: &str) -> Vec<Msg> {
        let mut parse = Vec::new();
        cstr(&mut parse, name);
        cstr(&mut parse, sql);
        parse.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &parse).await;
        self.send(b'S', &[]).await;
        self.until_ready().await
    }

    /// Describe the statement again (no Parse), Sync.
    async fn describe(&mut self, name: &str) -> Vec<Msg> {
        let mut describe = vec![b'S'];
        cstr(&mut describe, name);
        self.send(b'D', &describe).await;
        self.send(b'S', &[]).await;
        self.until_ready().await
    }

    /// Bind text values to `name`, Execute the unnamed portal, Sync. No
    /// Describe: a driver running its cached statement.
    async fn run(&mut self, name: &str, params: &[&str]) -> Vec<Msg> {
        self.send_run(name, params).await;
        self.until_ready().await
    }

    async fn send_run(&mut self, name: &str, params: &[&str]) {
        let mut bind = Vec::new();
        cstr(&mut bind, "");
        cstr(&mut bind, name);
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            bind.extend_from_slice(&(p.len() as i32).to_be_bytes());
            bind.extend_from_slice(p.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'B', &bind).await;
        let mut execute = Vec::new();
        cstr(&mut execute, "");
        execute.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &execute).await;
        self.send(b'S', &[]).await;
    }

    async fn close(mut self) {
        self.send(b'X', &[]).await;
        if let Some(h) = self.server.take() {
            let _ = tokio::time::timeout(BOUND, h).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Reading a trace.
// ---------------------------------------------------------------------------

const INT4: i32 = 23;
const INT8: i32 = 20;
const TEXT: i32 = 25;
const NUMERIC: i32 = 1700;

fn cols(spec: &[(&str, i32)]) -> Vec<(String, i32)> {
    spec.iter().map(|(n, o)| (n.to_string(), *o)).collect()
}

/// The RowDescription sent ahead of the first DataRow, if any.
fn description(trace: &[Msg]) -> Option<Vec<(String, i32)>> {
    let first_row = trace.iter().position(|m| matches!(m, Msg::D(_))).unwrap_or(trace.len());
    trace[..first_row].iter().rev().find_map(|m| match m {
        Msg::T(c) => Some(c.clone()),
        _ => None,
    })
}

fn rows(trace: &[Msg]) -> Vec<Vec<Option<String>>> {
    trace
        .iter()
        .filter_map(|m| match m {
            Msg::D(r) => Some(r.clone()),
            _ => None,
        })
        .collect()
}

fn error(trace: &[Msg]) -> Option<(String, String)> {
    trace.iter().find_map(|m| match m {
        Msg::E(c, msg) => Some((c.clone(), msg.clone())),
        _ => None,
    })
}

fn r(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|v| if *v == "NULL" { None } else { Some(v.to_string()) })
        .collect()
}

/// Asserts the trace succeeded, carried a RowDescription with exactly
/// `expected` ahead of its rows, and that every row has that many cells.
fn assert_repaired(what: &str, trace: &[Msg], expected: &[(String, i32)]) {
    assert_eq!(error(trace), None, "{what}: {trace:?}");
    assert_eq!(description(trace).as_deref(), Some(expected), "{what}: no fresh RowDescription in the new shape: {trace:?}");
    for row in rows(trace) {
        assert_eq!(row.len(), expected.len(), "{what}: a row does not match the RowDescription sent: {trace:?}");
    }
}

fn assert_plain(what: &str, trace: &[Msg]) {
    assert_eq!(error(trace), None, "{what}: {trace:?}");
    assert!(!trace.iter().any(|m| matches!(m, Msg::T(_))), "{what}: an Execute sent a RowDescription: {trace:?}");
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

/// Parse/Describe, ALTER TABLE ADD COLUMN in another session, then Execute
/// (three times): each Execute carries a RowDescription in the new shape,
/// with the new column's type, ahead of rows that match it and hold the new
/// column's values; a re-Describe stops them.
///
/// Mutations, each failing an assertion here: the comparison skipped
/// whenever the epoch moved (no RowDescription after the ALTER); Execute
/// taking the shape it sent as the client's (the second Execute after the
/// ALTER has none: sqlx would decode it against the old shape); the backstop
/// answering 0A000 in `rowdescription` mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_between_describe_and_execute_repairs_every_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_add").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7a (id INT, a TEXT, b INT)").await;
    b.ok("INSERT INTO b7a (id, a, b) VALUES (1, 'x', 10)").await;
    b.ok("INSERT INTO b7a (id, a, b) VALUES (2, 'y', 20)").await;

    let told = a.prepare("s", "SELECT * FROM b7a WHERE id >= $1 ORDER BY id").await;
    assert_eq!(description(&told), Some(cols(&[("id", INT4), ("a", TEXT), ("b", INT4)])), "{told:?}");
    let first = a.run("s", &["1"]).await;
    assert_plain("before the ALTER", &first);
    assert_eq!(rows(&first), vec![r(&["1", "x", "10"]), r(&["2", "y", "20"])]);

    b.ok("ALTER TABLE b7a ADD COLUMN c BIGINT").await;
    b.ok("UPDATE b7a SET c = id * 1000000000000").await;

    let new_shape = cols(&[("id", INT4), ("a", TEXT), ("b", INT4), ("c", INT8)]);
    for n in 1..=3 {
        let got = a.run("s", &["1"]).await;
        assert_repaired(&format!("execute {n} after the ALTER"), &got, &new_shape);
        assert_eq!(
            rows(&got),
            vec![r(&["1", "x", "10", "1000000000000"]), r(&["2", "y", "20", "2000000000000"])],
            "execute {n}"
        );
    }

    // Described again: the client knows the shape, and rows go alone.
    let again = a.prepare("s", "SELECT * FROM b7a WHERE id >= $1 ORDER BY id").await;
    assert_eq!(description(&again), Some(new_shape.clone()));
    let after = a.run("s", &["2"]).await;
    assert_plain("after a new Describe", &after);
    assert_eq!(rows(&after), vec![r(&["2", "y", "20", "2000000000000"])]);

    // An index changes no shape: no description, and the epoch did not move.
    let epoch = db.shape_epoch();
    b.ok("CREATE INDEX b7a_b ON b7a (b)").await;
    assert_eq!(db.shape_epoch(), epoch, "an index-only DDL moved the shape epoch");
    assert_plain("after CREATE INDEX", &a.run("s", &["1"]).await);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// Finding L7: the ALTER lands while the Execute is parked before its table
/// lock, after everything Execute and the text path read up front. The
/// comparison made after the result is built sees the moved epoch, the text
/// path describes the rows from the instance they came from, and the client
/// gets a RowDescription in the new shape with rows that match it.
///
/// Mutations, each failing an assertion here: the skip decided from the
/// epoch read when the Execute began (no RowDescription: rows of three
/// columns under a description of two); the text path's description not
/// recomputed after the build (the payload is described, and compared, in
/// the old shape).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_while_execute_waits_for_its_lock_is_caught_after_the_build() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_l7").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7l (id INT, a INT)").await;
    b.ok("INSERT INTO b7l (id, a) VALUES (1, 11)").await;
    b.ok("INSERT INTO b7l (id, a) VALUES (2, 22)").await;

    a.prepare("s", "SELECT * FROM b7l ORDER BY id").await;
    assert_plain("before", &a.run("s", &[]).await);

    let park = test_hooks::park_next("b7l");
    a.send_run("s", &[]).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the Execute never reached its lock");
    b.ok("ALTER TABLE b7l ADD COLUMN c INT DEFAULT 7").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the Execute never finished");
    assert_repaired(
        "an ALTER while the Execute waited",
        &got,
        &cols(&[("id", INT4), ("a", INT4), ("c", INT4)]),
    );
    assert_eq!(rows(&got), vec![r(&["1", "11", "7"]), r(&["2", "22", "7"])]);

    // The simple protocol's own description is the rows' too.
    let park = test_hooks::park_next("b7l");
    let mut body = Vec::new();
    cstr(&mut body, "SELECT * FROM b7l ORDER BY id");
    a.send(b'Q', &body).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the query never reached its lock");
    b.ok("ALTER TABLE b7l DROP COLUMN a").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the query never finished");
    assert_eq!(description(&got), Some(cols(&[("id", INT4), ("c", INT4)])), "{got:?}");
    assert_eq!(rows(&got), vec![r(&["1", "7"]), r(&["2", "7"])]);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// A learned parse (`PreparedSelect::Ready`) chosen before the Execute waits
/// for its lock, with the table reshaped while it waits: the executor
/// re-checks `shape_epoch` after the lock, nothing is read through the old
/// parse, and the text path answers in the same Execute, in the new shape.
/// Then case 17: DROP and CREATE the table with another NUMERIC scale while
/// a learned execution waits, and between executions: the values are right
/// both times, and the first execution after the change runs no learned
/// parse.
///
/// Mutations, each failing an assertion here: no re-check after the lock
/// (the old parse answers, in the old shape, with the old scale); a
/// per-instance epoch (every new slot starts at 0, so the recreated table's
/// epoch equals the one the parse was learned under and `12.3456` is read
/// at scale 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_parse_is_rechecked_after_the_lock() {
    use crate::core::rql::prepared::test_counters::take_learned_runs;
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_learned").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7p (id INT, a INT)").await;
    for k in 1..=4 {
        b.ok(&format!("INSERT INTO b7p (id, a) VALUES ({k}, {})", 10 * k)).await;
    }
    a.prepare("p", "SELECT * FROM b7p WHERE id = $1").await;
    // Two watched executions with different values, then the parse answers.
    assert_eq!(rows(&a.run("p", &["1"]).await), vec![r(&["1", "10"])]);
    assert_eq!(rows(&a.run("p", &["2"]).await), vec![r(&["2", "20"])]);
    take_learned_runs("b7p");
    assert_eq!(rows(&a.run("p", &["3"]).await), vec![r(&["3", "30"])]);
    assert_eq!(take_learned_runs("b7p").len(), 1, "the parse was never learned");

    let park = test_hooks::park_next("b7p");
    a.send_run("p", &["4"]).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the Execute never reached its lock");
    b.ok("ALTER TABLE b7p ADD COLUMN c INT DEFAULT 5").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the Execute never finished");
    assert_repaired("a learned parse across an ALTER", &got, &cols(&[("id", INT4), ("a", INT4), ("c", INT4)]));
    assert_eq!(rows(&got), vec![r(&["4", "40", "5"])]);

    // Case 17 (Q7): a NUMERIC table learned, then dropped and created again
    // with another scale. The names and types the client was told do not
    // change, so no description is due; the values must be the new table's.
    b.ok("CREATE TABLE b7n (id INT, v NUMERIC(10,2))").await;
    for k in 1..=3 {
        b.ok(&format!("INSERT INTO b7n (id, v) VALUES ({k}, {k}.25)")).await;
    }
    a.prepare("n", "SELECT * FROM b7n WHERE id = $1").await;
    assert_eq!(rows(&a.run("n", &["1"]).await), vec![r(&["1", "1.25"])]);
    assert_eq!(rows(&a.run("n", &["2"]).await), vec![r(&["2", "2.25"])]);
    take_learned_runs("b7n");
    assert_eq!(rows(&a.run("n", &["3"]).await), vec![r(&["3", "3.25"])]);
    assert_eq!(take_learned_runs("b7n").len(), 1, "the NUMERIC parse was never learned");

    let park = test_hooks::park_next("b7n");
    a.send_run("n", &["1"]).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the Execute never reached its lock");
    b.ok("DROP TABLE b7n").await;
    b.ok("CREATE TABLE b7n (id INT, v NUMERIC(10,4))").await;
    b.ok("INSERT INTO b7n (id, v) VALUES (1, 12.3456)").await;
    b.ok("INSERT INTO b7n (id, v) VALUES (2, 0.0001)").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the Execute never finished");
    assert_eq!(error(&got), None, "{got:?}");
    assert_eq!(rows(&got), vec![r(&["1", "12.3456"])], "the recreated table read at the old scale: {got:?}");
    for row in rows(&got) {
        assert_eq!(row.len(), 2);
    }

    // Between executions: the first one after the change runs no learned
    // parse, and reads the right value.
    b.ok("DROP TABLE b7n").await;
    b.ok("CREATE TABLE b7n (id INT, v NUMERIC(12,3))").await;
    b.ok("INSERT INTO b7n (id, v) VALUES (2, 7.125)").await;
    take_learned_runs("b7n");
    let got = a.run("n", &["2"]).await;
    assert_eq!(rows(&got), vec![r(&["2", "7.125"])], "{got:?}");
    assert!(take_learned_runs("b7n").is_empty(), "a parse learned on the dropped table ran");
    let _ = description(&got).map(|d| assert_eq!(d, cols(&[("id", INT4), ("v", NUMERIC)])));

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// Finding L13: UPDATE, INSERT and DELETE … RETURNING * prepared before an
/// ALTER get the fresh RowDescription too, with rows that match it.
///
/// Mutation, failing an assertion here: the comparison made for SELECTs
/// only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_across_an_alter_is_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_ret").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7r (id INT, a TEXT, b INT)").await;
    b.ok("INSERT INTO b7r (id, a, b) VALUES (1, 'x', 10)").await;
    b.ok("INSERT INTO b7r (id, a, b) VALUES (2, 'y', 20)").await;

    let told = a.prepare("u", "UPDATE b7r SET b = b + 1 WHERE id = $1 RETURNING *").await;
    assert_eq!(description(&told), Some(cols(&[("id", INT4), ("a", TEXT), ("b", INT4)])), "{told:?}");
    a.prepare("i", "INSERT INTO b7r (id, a, b) VALUES ($1, 'n', 0) RETURNING *").await;
    a.prepare("d", "DELETE FROM b7r WHERE id = $1 RETURNING *").await;
    let first = a.run("u", &["1"]).await;
    assert_plain("before", &first);
    assert_eq!(rows(&first), vec![r(&["1", "x", "11"])]);

    b.ok("ALTER TABLE b7r ADD COLUMN c INT DEFAULT 9").await;

    let new_shape = cols(&[("id", INT4), ("a", TEXT), ("b", INT4), ("c", INT4)]);
    for n in 1..=2 {
        let got = a.run("u", &["1"]).await;
        assert_repaired(&format!("UPDATE … RETURNING, execute {n}"), &got, &new_shape);
        assert_eq!(rows(&got), vec![r(&["1", "x", &(11 + n).to_string(), "9"])]);
    }
    let got = a.run("i", &["3"]).await;
    assert_repaired("INSERT … RETURNING", &got, &new_shape);
    assert_eq!(rows(&got), vec![r(&["3", "n", "0", "9"])]);
    let got = a.run("d", &["2"]).await;
    assert_repaired("DELETE … RETURNING", &got, &new_shape);
    assert_eq!(rows(&got), vec![r(&["2", "y", "20", "9"])]);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// Q7: one database-wide counter. A join over two tables, each retyped in
/// turn (a column dropped and added back under its name with another type,
/// as `ALTER COLUMN … TYPE` is refused), compares one number and repairs
/// both times, with the new type in the description and values decoded by
/// it; the counter moves for either table.
///
/// (The join names its columns: `SELECT *` over a join also returns the
/// engine's `__null_…` companion columns, in the description and the rows
/// alike, which is not this slice's.)
///
/// Mutation, failing an assertion here: a reshape moving only its own
/// slot's epoch, not the database counter (a per-table epoch): the join
/// never notices either change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_across_two_altered_tables_repairs_by_one_counter() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_join").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7j1 (id INT, a INT)").await;
    b.ok("CREATE TABLE b7j2 (id INT, b INT)").await;
    b.ok("INSERT INTO b7j1 (id, a) VALUES (1, 10)").await;
    b.ok("INSERT INTO b7j2 (id, b) VALUES (1, 100)").await;

    let sql = "SELECT b7j1.id, b7j1.a, b7j2.b FROM b7j1 JOIN b7j2 ON b7j1.id = b7j2.id";
    let told = a.prepare("j", sql).await;
    let before = description(&told).expect("the join is described");
    assert_eq!(before.iter().map(|c| c.1).collect::<Vec<_>>(), vec![INT4, INT4, INT4], "{told:?}");
    let first = a.run("j", &[]).await;
    assert_plain("before", &first);
    assert_eq!(rows(&first), vec![r(&["1", "10", "100"])]);

    let e0 = db.shape_epoch();
    b.ok("ALTER TABLE b7j1 DROP COLUMN a").await;
    b.ok("ALTER TABLE b7j1 ADD COLUMN a BIGINT").await;
    b.ok("UPDATE b7j1 SET a = 5000000000 WHERE id = 1").await;
    let e1 = db.shape_epoch();
    assert!(e1 > e0, "the first table's change did not move the database counter");
    let got = a.run("j", &[]).await;
    let mut after_first = before.clone();
    after_first[1].1 = INT8;
    assert_repaired("after the first table's change", &got, &after_first);
    assert_eq!(rows(&got), vec![r(&["1", "5000000000", "100"])]);

    // Told the new shape, then the other table changes.
    a.prepare("j", sql).await;
    assert_plain("re-described", &a.run("j", &[]).await);
    b.ok("ALTER TABLE b7j2 DROP COLUMN b").await;
    b.ok("ALTER TABLE b7j2 ADD COLUMN b TEXT").await;
    b.ok("UPDATE b7j2 SET b = 'hundred' WHERE id = 1").await;
    assert!(db.shape_epoch() > e1, "the second table's change did not move the database counter");
    let got = a.run("j", &[]).await;
    let mut after_second = after_first.clone();
    after_second[2].1 = TEXT;
    assert_repaired("after the second table's change", &got, &after_second);
    assert_eq!(rows(&got), vec![r(&["1", "5000000000", "hundred"])]);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// Q2: `rdb.shape_change`. SHOW reports `auto` by default (B8b); SET to
/// `error` makes the stale Execute fail with 0A000 and PostgreSQL's message,
/// before a write runs (the row is not updated); re-preparing heals it;
/// RESET restores `auto`; an unknown value is refused. In `error`
/// mode an ALTER landing while a SELECT waits for its lock still gets 0A000,
/// and one landing while an autocommit write waits gets the RowDescription
/// (the write has committed).
///
/// Mutation, failing an assertion here: no check before the run in `error`
/// mode (the UPDATE is applied, then answered with a RowDescription).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shape_change_error_mode_show_and_reset() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_mode").await;
    let mut a = Client::connect_default(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b7m (id INT, a INT)").await;
    b.ok("INSERT INTO b7m (id, a) VALUES (1, 10)").await;
    b.ok("INSERT INTO b7m (id, a) VALUES (2, 20)").await;

    let shown = |t: &[Msg]| rows(t).first().and_then(|r| r[0].clone());
    assert_eq!(shown(&a.ok("SHOW rdb.shape_change").await).as_deref(), Some("auto"));
    let refused = a.simple("SET rdb.shape_change = sometimes").await;
    assert_eq!(error(&refused).map(|e| e.0).as_deref(), Some("22023"), "{refused:?}");
    a.ok("SET rdb.shape_change = error").await;
    assert_eq!(shown(&a.ok("SHOW rdb.shape_change").await).as_deref(), Some("error"));

    a.prepare("s", "SELECT * FROM b7m ORDER BY id").await;
    a.prepare("u", "UPDATE b7m SET a = a + 1 WHERE id = $1 RETURNING *").await;
    assert_plain("before", &a.run("s", &[]).await);

    b.ok("ALTER TABLE b7m ADD COLUMN c INT DEFAULT 1").await;
    for stmt in ["s", "s"] {
        let got = a.run(stmt, &[]).await;
        assert_eq!(
            error(&got),
            Some(("0A000".to_string(), "cached plan must not change result type".to_string())),
            "{got:?}"
        );
        assert!(rows(&got).is_empty(), "rows after the 0A000: {got:?}");
    }
    let got = a.run("u", &["1"]).await;
    assert_eq!(error(&got).map(|e| e.0).as_deref(), Some("0A000"), "{got:?}");
    let check = b.ok("SELECT a FROM b7m WHERE id = 1").await;
    assert_eq!(rows(&check), vec![r(&["10"])], "the refused UPDATE was applied");

    // Re-prepared, as pgjdbc does on 0A000: the new shape, rows alone.
    a.prepare("s", "SELECT * FROM b7m ORDER BY id").await;
    let healed = a.run("s", &[]).await;
    assert_plain("re-prepared", &healed);
    assert_eq!(rows(&healed), vec![r(&["1", "10", "1"]), r(&["2", "20", "1"])]);

    // An ALTER while a SELECT waits for its lock: still 0A000, from the
    // comparison after the build.
    let park = test_hooks::park_next("b7m");
    a.send_run("s", &[]).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the Execute never reached its lock");
    b.ok("ALTER TABLE b7m ADD COLUMN d INT DEFAULT 2").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the Execute never finished");
    assert_eq!(error(&got).map(|e| e.0).as_deref(), Some("0A000"), "{got:?}");
    assert!(rows(&got).is_empty(), "{got:?}");

    // An autocommit write in the same window has committed by then: the
    // RowDescription, never an error inviting a second apply.
    a.prepare("u", "UPDATE b7m SET a = a + 1 WHERE id = $1 RETURNING *").await;
    let park = test_hooks::park_next("b7m");
    a.send_run("u", &["2"]).await;
    tokio::time::timeout(BOUND, park.arrived()).await.expect("the write never reached its lock");
    b.ok("ALTER TABLE b7m ADD COLUMN e INT DEFAULT 3").await;
    park.release();
    let got = tokio::time::timeout(BOUND, a.until_ready()).await.expect("the write never finished");
    assert_repaired(
        "an autocommit write in error mode",
        &got,
        &cols(&[("id", INT4), ("a", INT4), ("c", INT4), ("d", INT4), ("e", INT4)]),
    );
    assert_eq!(rows(&got), vec![r(&["2", "21", "1", "2", "3"])]);

    // RESET: back to the default, `auto`. `s` was refused (0A000) since it
    // was last described, so its next stale use, without a new Parse, gets
    // the RowDescription.
    a.ok("RESET rdb.shape_change").await;
    assert_eq!(shown(&a.ok("SHOW rdb.shape_change").await).as_deref(), Some("auto"));
    b.ok("ALTER TABLE b7m DROP COLUMN c").await;
    let got = a.run("s", &[]).await;
    assert_repaired("after RESET", &got, &cols(&[("id", INT4), ("a", INT4), ("d", INT4), ("e", INT4)]));
    a.ok("SET rdb.shape_change = rowdescription").await;
    assert_eq!(shown(&a.ok("SHOW rdb.shape_change").await).as_deref(), Some("rowdescription"));
    a.ok("SET rdb.shape_change TO DEFAULT").await;
    assert_eq!(shown(&a.ok("SHOW rdb.shape_change").await).as_deref(), Some("auto"));

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// The startup option spelling of Q2's mode.
#[test]
fn shape_change_from_the_startup_packet() {
    let p = |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<HashMap<_, _>>();
    assert_eq!(startup_shape_change(&p(&[])), None);
    assert_eq!(
        startup_shape_change(&p(&[("options", "-c rdb.shape_change=error")])),
        Some(ShapeChangeMode::Error)
    );
    assert_eq!(
        startup_shape_change(&p(&[("options", "-c statement_timeout=5s --rdb.shape_change=rowdescription")])),
        Some(ShapeChangeMode::RowDescription)
    );
    assert_eq!(startup_shape_change(&p(&[("rdb.shape_change", "ERROR")])), Some(ShapeChangeMode::Error));
    assert_eq!(startup_shape_change(&p(&[("options", "-c rdb.shape_change=bogus")])), None);
    assert_eq!(startup_shape_change(&p(&[("rdb.shape_change", "auto")])), Some(ShapeChangeMode::Auto));
    assert_eq!(ShapeChangeMode::default(), ShapeChangeMode::Auto);
}

/// The default, `auto` (slice B8b; Q1 as the owner decided it, finding L10):
/// a stale statement's first use is refused as PostgreSQL refuses it (0A000
/// at Bind, nothing run, a write not applied); executed again without a new
/// Parse, as sqlx re-executes its cached statement, it gets the fresh
/// RowDescription every time; a re-Describe or a new Parse starts over, so
/// the next change is refused first again.
///
/// Mutations, each failing an assertion here: `auto` resolved as `error`
/// always (the re-execute gets 0A000 again: sqlx never heals); as
/// `rowdescription` always (the first stale use runs: pgjdbc and npgsql get
/// a RowDescription they did not ask for); the refusal not remembered; a
/// Describe not starting over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_refuses_a_stale_statement_once_then_repairs_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b8b_auto").await;
    let mut a = Client::connect_default(&db);
    let mut b = Client::connect(&db);
    b.ok("CREATE TABLE b8ba (id INT, a INT)").await;
    b.ok("INSERT INTO b8ba (id, a) VALUES (1, 10)").await;
    b.ok("INSERT INTO b8ba (id, a) VALUES (2, 20)").await;
    let refused = |what: &str, got: &[Msg]| {
        assert_eq!(
            error(got),
            Some(("0A000".to_string(), "cached plan must not change result type".to_string())),
            "{what}: {got:?}"
        );
        assert!(rows(got).is_empty(), "{what}: rows after the 0A000: {got:?}");
    };

    a.prepare("s", "SELECT * FROM b8ba ORDER BY id").await;
    a.prepare("u", "UPDATE b8ba SET a = a + 1 WHERE id = $1 RETURNING *").await;
    assert_plain("before", &a.run("s", &[]).await);

    b.ok("ALTER TABLE b8ba ADD COLUMN c INT DEFAULT 1").await;
    let three = cols(&[("id", INT4), ("a", INT4), ("c", INT4)]);
    refused("the first stale use", &a.run("s", &[]).await);
    for n in 1..=2 {
        let got = a.run("s", &[]).await;
        assert_repaired(&format!("re-execute {n} without a new Parse"), &got, &three);
        assert_eq!(rows(&got), vec![r(&["1", "10", "1"]), r(&["2", "20", "1"])]);
    }

    // A write: refused first, before it runs, then applied exactly once.
    refused("the first stale write", &a.run("u", &["1"]).await);
    assert_eq!(rows(&b.ok("SELECT a FROM b8ba WHERE id = 1").await), vec![r(&["10"])], "the refused UPDATE was applied");
    let got = a.run("u", &["1"]).await;
    assert_repaired("the write re-executed", &got, &three);
    assert_eq!(rows(&got), vec![r(&["1", "11", "1"])]);

    // Described again (no Parse): the client caches the new shape, and the
    // next change is refused first again.
    assert_eq!(description(&a.describe("s").await), Some(three.clone()));
    assert_plain("described again", &a.run("s", &[]).await);
    b.ok("ALTER TABLE b8ba ADD COLUMN d INT DEFAULT 2").await;
    refused("after a re-Describe", &a.run("s", &[]).await);
    assert_repaired("and re-executed", &a.run("s", &[]).await, &cols(&[("id", INT4), ("a", INT4), ("c", INT4), ("d", INT4)]));

    // A new Parse of the same name is a new statement: refused first too.
    a.prepare("s", "SELECT * FROM b8ba ORDER BY id").await;
    assert_plain("re-prepared", &a.run("s", &[]).await);
    b.ok("ALTER TABLE b8ba DROP COLUMN d").await;
    refused("after a new Parse", &a.run("s", &[]).await);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// A partitioned table refuses ALTER, and its shape is reached by logical
/// name: a plain table joined with a partitioned one repairs when the plain
/// one is retyped, and a DROP and CREATE of the partitioned name with
/// another column type repairs too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_with_a_partitioned_table_repairs_by_logical_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b7_part").await;
    let mut a = Client::connect(&db);
    let mut b = Client::connect(&db);
    let ddl = |v: &str| {
        format!(
            "CREATE TABLE b7pt (tenant_id BIGINT NOT NULL, v {v}, PRIMARY KEY (tenant_id)) \
             PARTITION BY HASH (tenant_id) PARTITIONS 4"
        )
    };
    b.ok(&ddl("BIGINT")).await;
    b.ok("CREATE TABLE b7pl (id BIGINT, a INT)").await;
    b.ok("INSERT INTO b7pt (tenant_id, v) VALUES (1, 100)").await;
    b.ok("INSERT INTO b7pl (id, a) VALUES (1, 10)").await;

    let refused = b.simple("ALTER TABLE b7pt ADD COLUMN w INT").await;
    assert!(error(&refused).is_some(), "ALTER of a partitioned table was accepted: {refused:?}");

    let sql = "SELECT b7pl.id, b7pl.a, b7pt.v FROM b7pl JOIN b7pt ON b7pl.id = b7pt.tenant_id";
    let told = a.prepare("j", sql).await;
    let before = description(&told).expect("the join is described");
    assert_eq!(before.iter().map(|c| c.1).collect::<Vec<_>>(), vec![INT8, INT4, INT8], "{told:?}");
    let first = a.run("j", &[]).await;
    assert_plain("before", &first);
    assert_eq!(rows(&first), vec![r(&["1", "10", "100"])]);

    b.ok("ALTER TABLE b7pl DROP COLUMN a").await;
    b.ok("ALTER TABLE b7pl ADD COLUMN a TEXT").await;
    b.ok("UPDATE b7pl SET a = 'ten' WHERE id = 1").await;
    let got = a.run("j", &[]).await;
    let mut expected = before.clone();
    expected[1].1 = TEXT;
    assert_repaired("the plain side retyped", &got, &expected);
    assert_eq!(rows(&got), vec![r(&["1", "ten", "100"])]);

    // The partitioned name dropped and created with another type for `v`.
    a.prepare("j", sql).await;
    b.ok("DROP TABLE b7pt").await;
    b.ok(&ddl("TEXT")).await;
    b.ok("INSERT INTO b7pt (tenant_id, v) VALUES (1, 'two hundred')").await;
    let got = a.run("j", &[]).await;
    let mut expected2 = expected.clone();
    expected2[2].1 = TEXT;
    assert_repaired("the partitioned side recreated", &got, &expected2);
    assert_eq!(rows(&got), vec![r(&["1", "ten", "two hundred"])]);

    a.close().await;
    b.close().await;
    close(db, dir.path());
}

/// The B8b hot path over the wire, as the perf gate's bench drives it
/// (Parse once, then Bind/Execute/Sync, no Describe): a prepared point
/// SELECT, UPDATE and bigint-key INSERT each take one transaction id per
/// statement (O is the first attempt) and register no owner as outliving
/// its transaction (only a retry or a failure does).
///
/// Mutation, failing an assertion here: the commit path registering O
/// (`owner::attempt_ended` registering).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_prepared_hot_path_takes_one_id_and_registers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "b8b_hot").await;
    let mut a = Client::connect_default(&db);
    a.ok("CREATE TABLE b8bh (id BIGINT PRIMARY KEY, v BIGINT)").await;
    a.ok("INSERT INTO b8bh (id, v) VALUES (1, 10), (2, 20)").await;
    a.parse_only("sel", "SELECT v FROM b8bh WHERE id = $1").await;
    a.parse_only("upd", "UPDATE b8bh SET v = $1 WHERE id = $2").await;
    a.parse_only("ins", "INSERT INTO b8bh (id, v) VALUES ($1, $2)").await;
    // Warm: the learned parse needs two runs.
    for _ in 0..3 {
        assert_eq!(error(&a.run("sel", &["1"]).await), None);
    }
    let registered = db.transactions.outliving_total();
    for (stmt, params) in [("sel", vec!["2".to_string()]), ("upd", vec!["7".to_string(), "1".to_string()])]
        .into_iter()
        .chain((0..3).map(|i| ("ins", vec![(100 + i).to_string(), "5".to_string()])))
    {
        let params: Vec<&str> = params.iter().map(String::as_str).collect();
        let before = db.transactions.next_id();
        let got = a.run(stmt, &params).await;
        assert_eq!(error(&got), None, "{stmt}: {got:?}");
        assert_eq!(db.transactions.next_id() - before, 1, "{stmt}: ids per statement over the wire");
    }
    assert_eq!(
        db.transactions.outliving_total(),
        registered,
        "a committing statement over the wire registered its owner as outliving its transaction"
    );
    a.close().await;
    close(db, dir.path());
}
