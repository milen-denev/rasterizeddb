//! A prepared statement executed again after the table under it changed shape.
//!
//! A driver describes a statement once and caches the column list. When an
//! `ALTER TABLE` changes what the statement returns, PostgreSQL 17 refuses
//! every later execute with `0A000 cached plan must not change result type`.
//! sqlx does not re-prepare on that error (see sqlx-ignores-0a000), so the
//! server puts a fresh `RowDescription` in front of the rows instead.
//!
//! It used to do that on the first execute only, and then took the new shape
//! as what the client knew. sqlx applies a `RowDescription` that arrives with
//! the rows to that one execution, not to its statement cache, so from the
//! second execute on it decoded the rows against the shape it was described
//! with: a dropped column's neighbour read under the dropped column's name,
//! a renamed column under its old name. No error.
//!
//! The rule pinned here: the shape a client knows is the one it was last told
//! at `Describe`, and every `Execute` whose shape differs from it carries a
//! `RowDescription`, until the client describes again. That is
//! `rdb.shape_change = rowdescription`, which every session here asks for
//! ([`ROWDESCRIPTION`]); since B8b the default is `auto`, which answers a
//! stale statement's first use with 0A000 and its re-executes without a new
//! Parse this way (`sqlx_cached_returning_under_the_default_…` below, and
//! `driver_matrix.rs`).
//!
//! Every check compares a prepared statement's rows with an unprepared twin
//! (the same text as a simple query). The twins' answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Connection, Executor, PgConnection, Row, TypeInfo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// The protocol, spoken directly, to see exactly which messages come back.
// ---------------------------------------------------------------------------

fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

async fn read_msg(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut t = [0u8; 1];
    stream.read_exact(&mut t).await.expect("tag");
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("length");
    let n = i32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; n.saturating_sub(4)];
    if !body.is_empty() {
        stream.read_exact(&mut body).await.expect("body");
    }
    (t[0], body)
}

/// The startup option every session in this file connects with.
const ROWDESCRIPTION: &str = "-c rdb.shape_change=rowdescription";

/// `server.url()` asking for [`ROWDESCRIPTION`].
fn pinned_url(server: &TestServer) -> String {
    format!("{}&options=-c%20rdb.shape_change%3Drowdescription", server.url())
}

async fn connect(server: &TestServer) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
        .await
        .expect("connect");
    let mut body = 196608i32.to_be_bytes().to_vec();
    for s in ["user", "postgres", "database", "postgres", "options", ROWDESCRIPTION] {
        body.extend_from_slice(&cstr(s));
    }
    body.push(0);
    let mut out = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    stream.write_all(&out).await.expect("startup");
    loop {
        if read_msg(&mut stream).await.0 == b'Z' {
            break;
        }
    }
    stream
}

/// Everything up to ReadyForQuery.
async fn until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    loop {
        let m = read_msg(stream).await;
        let done = m.0 == b'Z';
        out.push(m);
        if done {
            return out;
        }
    }
}

async fn simple(stream: &mut TcpStream, sql: &str) {
    stream.write_all(&msg(b'Q', &cstr(sql))).await.expect("query");
    let replies = until_ready(stream).await;
    if let Some((_, body)) = replies.iter().find(|(t, _)| *t == b'E') {
        panic!("{sql}: {}", String::from_utf8_lossy(body));
    }
}

fn parse(name: &str, sql: &str) -> Vec<u8> {
    let mut p = cstr(name);
    p.extend_from_slice(&cstr(sql));
    p.extend_from_slice(&0i16.to_be_bytes());
    msg(b'P', &p)
}

fn bind(portal: &str, stmt: &str, values: &[&str]) -> Vec<u8> {
    let mut b = cstr(portal);
    b.extend_from_slice(&cstr(stmt));
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for v in values {
        b.extend_from_slice(&(v.len() as i32).to_be_bytes());
        b.extend_from_slice(v.as_bytes());
    }
    b.extend_from_slice(&0i16.to_be_bytes());
    msg(b'B', &b)
}

fn describe(kind: u8, name: &str) -> Vec<u8> {
    let mut d = vec![kind];
    d.extend_from_slice(&cstr(name));
    msg(b'D', &d)
}

fn execute(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut e = cstr(portal);
    e.extend_from_slice(&max_rows.to_be_bytes());
    msg(b'E', &e)
}

fn sync() -> Vec<u8> {
    msg(b'S', &[])
}

/// Column names of a RowDescription.
fn names(body: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2;
    let mut out = Vec::new();
    for _ in 0..count {
        let end = at + body[at..].iter().position(|b| *b == 0).expect("name");
        out.push(String::from_utf8(body[at..end].to_vec()).expect("utf8 name"));
        at = end + 1 + 18;
    }
    out
}

/// A DataRow's values as text (every Bind here asks for text results).
fn values(body: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2;
    let mut out = Vec::new();
    for _ in 0..count {
        let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
        at += 4;
        if len < 0 {
            out.push(None);
        } else {
            let len = len as usize;
            out.push(Some(String::from_utf8_lossy(&body[at..at + len]).into_owned()));
            at += len;
        }
    }
    out
}

/// What came back for one batch: each message as its tag, with a
/// RowDescription's columns and a DataRow's values written out, so a failure
/// shows the whole exchange.
fn trace(replies: &[(u8, Vec<u8>)]) -> Vec<String> {
    replies
        .iter()
        .map(|(t, body)| match t {
            b'T' => format!("T{:?}", names(body)),
            b'D' => format!("D{:?}", values(body)),
            b'E' => format!("E({})", String::from_utf8_lossy(body)),
            _ => (*t as char).to_string(),
        })
        .collect()
}

async fn send(stream: &mut TcpStream, batch: Vec<u8>) -> Vec<String> {
    stream.write_all(&batch).await.expect("send");
    trace(&until_ready(stream).await)
}

fn has_description(trace: &[String]) -> bool {
    trace.iter().any(|m| m.starts_with('T'))
}

/// A RowDescription before every DataRow it applies to, with these columns.
fn described_as(trace: &[String], columns: &[&str]) -> bool {
    let expected = format!("T{:?}", columns.iter().map(|c| c.to_string()).collect::<Vec<_>>());
    let first_row = trace.iter().position(|m| m.starts_with('D')).unwrap_or(trace.len());
    trace[..first_row].iter().any(|m| *m == expected)
}

/// Each schema change, with the table before and after it. The shape a
/// `SELECT *` returns moves under every one of them.
struct Change {
    name: &'static str,
    setup: &'static [&'static str],
    alter: &'static [&'static str],
    after: &'static [&'static str],
    /// `SELECT * FROM t ORDER BY id` after the change, as PostgreSQL 17
    /// answers it.
    rows: &'static [&'static [Option<&'static str>]],
}

const CHANGES: &[Change] = &[
    Change {
        name: "add column",
        setup: &[
            "CREATE TABLE t (id INT, a TEXT, b INT)",
            "INSERT INTO t (id, a, b) VALUES (1, 'x', 10)",
            "INSERT INTO t (id, a, b) VALUES (2, 'y', 20)",
        ],
        alter: &[
            "ALTER TABLE t ADD COLUMN c INT",
            "UPDATE t SET c = 7 WHERE id = 1",
            "UPDATE t SET c = 14 WHERE id = 2",
        ],
        after: &["id", "a", "b", "c"],
        rows: &[
            &[Some("1"), Some("x"), Some("10"), Some("7")],
            &[Some("2"), Some("y"), Some("20"), Some("14")],
        ],
    },
    Change {
        name: "drop column",
        setup: &[
            "CREATE TABLE t (id INT, a INT, b INT)",
            "INSERT INTO t (id, a, b) VALUES (1, 10, 100)",
            "INSERT INTO t (id, a, b) VALUES (2, 20, 200)",
        ],
        alter: &["ALTER TABLE t DROP COLUMN a"],
        after: &["id", "b"],
        rows: &[&[Some("1"), Some("100")], &[Some("2"), Some("200")]],
    },
    // A retype. `ALTER COLUMN … TYPE` to a different type is refused here
    // (it would rewrite every row), so the column is dropped and added back
    // under the same name and a wider type: same names, another type.
    Change {
        name: "retype",
        setup: &[
            "CREATE TABLE t (id INT, v INT)",
            "INSERT INTO t (id, v) VALUES (1, 5)",
            "INSERT INTO t (id, v) VALUES (2, 6)",
        ],
        alter: &[
            "ALTER TABLE t DROP COLUMN v",
            "ALTER TABLE t ADD COLUMN v BIGINT",
            "UPDATE t SET v = 5000000000 WHERE id = 1",
            "UPDATE t SET v = 10000000000 WHERE id = 2",
        ],
        after: &["id", "v"],
        rows: &[&[Some("1"), Some("5000000000")], &[Some("2"), Some("10000000000")]],
    },
    Change {
        name: "rename column",
        setup: &[
            "CREATE TABLE t (id INT, a INT)",
            "INSERT INTO t (id, a) VALUES (1, 11)",
            "INSERT INTO t (id, a) VALUES (2, 22)",
        ],
        alter: &["ALTER TABLE t RENAME COLUMN a TO z"],
        after: &["id", "z"],
        rows: &[&[Some("1"), Some("11")], &[Some("2"), Some("22")]],
    },
];

const STAR: &str = "SELECT * FROM t ORDER BY id";

fn rows_of(trace: &[String]) -> Vec<String> {
    trace.iter().filter(|m| m.starts_with('D')).cloned().collect()
}

fn expected_rows(change: &Change) -> Vec<String> {
    change
        .rows
        .iter()
        .map(|r| format!("D{:?}", r.iter().map(|v| v.map(str::to_string)).collect::<Vec<_>>()))
        .collect()
}

/// A named statement described once, executed four times after each change:
/// every one of the four carries the new description, and the rows are
/// PostgreSQL's. Once the client describes again, executes go back to
/// sending rows alone.
#[tokio::test]
async fn every_execute_after_a_change_carries_the_new_description() {
    let _counted = COUNTED.lock().await;
    for change in CHANGES {
        let server = TestServer::start().await.expect("server");
        let mut s = connect(&server).await;
        for sql in change.setup {
            simple(&mut s, sql).await;
        }
        let first = send(&mut s, [parse("s", STAR), describe(b'S', "s"), sync()].concat()).await;
        assert!(has_description(&first), "{}: {first:?}", change.name);
        let before = send(&mut s, [bind("", "s", &[]), execute("", 0), sync()].concat()).await;
        assert!(!has_description(&before), "{}: nothing changed yet: {before:?}", change.name);

        for sql in change.alter {
            simple(&mut s, sql).await;
        }
        for n in 1..=4 {
            let t = send(&mut s, [bind("", "s", &[]), execute("", 0), sync()].concat()).await;
            assert!(
                described_as(&t, change.after),
                "{}: execute {n} after the change must describe {:?}: {t:?}",
                change.name,
                change.after
            );
            assert_eq!(rows_of(&t), expected_rows(change), "{}: execute {n}: {t:?}", change.name);
        }

        // Described again: the client knows the shape now.
        let again = send(&mut s, [describe(b'S', "s"), sync()].concat()).await;
        assert!(described_as(&again, change.after), "{}: {again:?}", change.name);
        for n in 1..=2 {
            let t = send(&mut s, [bind("", "s", &[]), execute("", 0), sync()].concat()).await;
            assert!(!has_description(&t), "{}: execute {n} after re-describe: {t:?}", change.name);
            assert_eq!(rows_of(&t), expected_rows(change), "{}: {t:?}", change.name);
        }
    }
}

/// The unnamed statement, kept across executes without a new Parse, and a
/// pipeline of several Bind/Execute pairs under one Sync: each execute in it
/// carries the description.
#[tokio::test]
async fn unnamed_and_pipelined_executes_carry_the_description() {
    let _counted = COUNTED.lock().await;
    for change in CHANGES {
        let server = TestServer::start().await.expect("server");
        let mut s = connect(&server).await;
        for sql in change.setup {
            simple(&mut s, sql).await;
        }
        let first = send(
            &mut s,
            [parse("", STAR), describe(b'S', ""), bind("", "", &[]), execute("", 0), sync()].concat(),
        )
        .await;
        assert_eq!(first.iter().filter(|m| m.starts_with('T')).count(), 1, "{first:?}");

        for sql in change.alter {
            simple(&mut s, sql).await;
        }
        let pipelined = send(
            &mut s,
            [
                bind("", "", &[]),
                execute("", 0),
                bind("", "", &[]),
                execute("", 0),
                bind("", "", &[]),
                execute("", 0),
                sync(),
            ]
            .concat(),
        )
        .await;
        let descriptions = pipelined.iter().filter(|m| m.starts_with('T')).count();
        assert_eq!(descriptions, 3, "{}: one per execute: {pipelined:?}", change.name);
        for chunk in pipelined.split(|m| m == "C" || m.starts_with('C')) {
            if chunk.iter().any(|m| m.starts_with('D')) {
                assert!(described_as(chunk, change.after), "{}: {pipelined:?}", change.name);
            }
        }
        assert_eq!(
            rows_of(&pipelined).len(),
            3 * change.rows.len(),
            "{}: {pipelined:?}",
            change.name
        );

        // A new Parse is a new statement, described afresh in the same batch.
        let reparsed = send(
            &mut s,
            [parse("", STAR), describe(b'S', ""), bind("", "", &[]), execute("", 0), sync()].concat(),
        )
        .await;
        assert_eq!(reparsed.iter().filter(|m| m.starts_with('T')).count(), 1, "{reparsed:?}");
        assert_eq!(rows_of(&reparsed), expected_rows(change), "{}: {reparsed:?}", change.name);
    }
}

/// A portal described the way libpq's `PQexecParams` does it
/// (Parse/Bind/Describe portal/Execute), then executed again through a new
/// portal of the same statement without describing it: what the client last
/// learned is the portal's description, so the new shape has to be sent.
/// And a portal paged with a row limit sends it once, ahead of the first page.
#[tokio::test]
async fn portals_follow_what_they_were_told() {
    let _counted = COUNTED.lock().await;
    let change = &CHANGES[1];
    let server = TestServer::start().await.expect("server");
    let mut s = connect(&server).await;
    for sql in change.setup {
        simple(&mut s, sql).await;
    }
    let sql = "SELECT * FROM t WHERE id >= $1 ORDER BY id";
    let first = send(
        &mut s,
        [parse("p", sql), bind("", "p", &["1"]), describe(b'P', ""), execute("", 0), sync()].concat(),
    )
    .await;
    assert!(described_as(&first, &["id", "a", "b"]), "{first:?}");
    assert_eq!(first.iter().filter(|m| m.starts_with('T')).count(), 1, "{first:?}");

    for sql in change.alter {
        simple(&mut s, sql).await;
    }
    for n in 1..=3 {
        let t = send(&mut s, [bind("", "p", &["1"]), execute("", 0), sync()].concat()).await;
        assert!(described_as(&t, change.after), "execute {n}: {t:?}");
        assert_eq!(rows_of(&t), expected_rows(change), "execute {n}: {t:?}");
    }

    // A row limit: the description goes ahead of the first page only, and
    // the page that resumes the portal does not repeat it.
    let paged = send(
        &mut s,
        [bind("pg", "p", &["1"]), execute("pg", 1), execute("pg", 1), sync()].concat(),
    )
    .await;
    assert_eq!(paged.iter().filter(|m| m.starts_with('T')).count(), 1, "{paged:?}");
    assert!(described_as(&paged, change.after), "{paged:?}");
    assert_eq!(rows_of(&paged), expected_rows(change), "{paged:?}");

    // Described once more, through the portal: the Describe carries the shape
    // and the Execute behind it does not repeat it.
    let redescribed = send(
        &mut s,
        [bind("", "p", &["1"]), describe(b'P', ""), execute("", 0), sync()].concat(),
    )
    .await;
    assert_eq!(redescribed.iter().filter(|m| m.starts_with('T')).count(), 1, "{redescribed:?}");
    let t = send(&mut s, [bind("", "p", &["2"]), execute("", 0), sync()].concat()).await;
    assert!(!has_description(&t), "{t:?}");
    assert_eq!(rows_of(&t), expected_rows(change)[1..].to_vec(), "{t:?}");
}

/// A schema change that leaves this statement's shape alone sends nothing
/// extra. A strict client (tokio-postgres) refuses a RowDescription it did
/// not ask for, so an unneeded one would break a statement that is fine.
#[tokio::test]
async fn a_change_that_does_not_move_the_shape_sends_nothing() {
    let _counted = COUNTED.lock().await;
    let server = TestServer::start().await.expect("server");
    let mut s = connect(&server).await;
    simple(&mut s, "CREATE TABLE t (id INT, a INT NOT NULL)").await;
    simple(&mut s, "INSERT INTO t (id, a) VALUES (1, 10)").await;
    let statements = [
        ("cols", "SELECT id, a FROM t ORDER BY id", Vec::<&str>::new()),
        ("param", "SELECT * FROM t WHERE id = $1", vec!["1"]),
    ];
    for (name, sql, _) in &statements {
        let d = send(&mut s, [parse(name, sql), describe(b'S', name), sync()].concat()).await;
        assert!(has_description(&d), "{d:?}");
    }
    // Another table, a same-type restatement, a nullability change, and a
    // column the explicit list does not read.
    simple(&mut s, "CREATE TABLE other (x INT)").await;
    simple(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE INT").await;
    simple(&mut s, "ALTER TABLE t ALTER COLUMN a DROP NOT NULL").await;
    for (name, _, values) in &statements {
        for n in 1..=3 {
            let t = send(&mut s, [bind("", name, values), execute("", 0), sync()].concat()).await;
            assert!(!has_description(&t), "{name}: execute {n}: {t:?}");
            assert_eq!(rows_of(&t), vec![r#"D[Some("1"), Some("10")]"#.to_string()], "{name}: {t:?}");
        }
    }
    simple(&mut s, "ALTER TABLE t ADD COLUMN extra INT").await;
    for n in 1..=3 {
        let t = send(&mut s, [bind("", "cols", &[]), execute("", 0), sync()].concat()).await;
        assert!(!has_description(&t), "cols after ADD COLUMN: execute {n}: {t:?}");
    }
    for n in 1..=3 {
        let t = send(&mut s, [bind("", "param", &["1"]), execute("", 0), sync()].concat()).await;
        assert!(described_as(&t, &["id", "a", "extra"]), "param after ADD COLUMN: execute {n}: {t:?}");
    }
}

// ---------------------------------------------------------------------------
// The drivers.
// ---------------------------------------------------------------------------

/// A row as `name=value` cells, decoded by the type the row's own metadata
/// names, which is what an application reading it by type does. A decode the
/// driver refuses, or one that panics on a column the row does not have, is
/// written as such rather than ending the test, so a failure shows every
/// execute.
fn render(row: &sqlx::postgres::PgRow) -> String {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        row.columns()
            .iter()
            .map(|c| {
                let i = c.ordinal();
                let v = match c.type_info().name() {
                    "INT4" => row.try_get::<Option<i32>, _>(i).map(|v| format!("{v:?}")),
                    "INT8" => row.try_get::<Option<i64>, _>(i).map(|v| format!("{v:?}")),
                    _ => row.try_get::<Option<String>, _>(i).map(|v| format!("{v:?}")),
                };
                format!("{}={}", c.name(), v.unwrap_or_else(|e| format!("<{e}>")))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }))
    .unwrap_or_else(|_| "<panicked decoding>".to_string())
}

/// The twin: the same text as a simple query, with no statement and nothing
/// cached.
async fn twin(conn: &mut PgConnection, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("twin");
    rows.iter().map(render).collect()
}

/// One execute of `sql` through sqlx's statement cache, `$1` bound to 1.
async fn cached(conn: &mut PgConnection, sql: &str) -> Result<Vec<String>, String> {
    let q = sqlx::query(AssertSqlSafe(sql.to_string()));
    let q = if sql.contains("$1") { q.bind(1i32) } else { q };
    q.fetch_all(&mut *conn)
        .await
        .map(|rows| rows.iter().map(render).collect())
        .map_err(|e| e.to_string())
}

/// sqlx, statement cache on: the statement is prepared and described once,
/// and every later execute is Bind/Execute against the cached description.
/// Each of four executes after the change must decode exactly what the twin
/// does, names and values both.
#[tokio::test]
async fn sqlx_cached_statement_decodes_the_new_shape_every_time() {
    let _counted = COUNTED.lock().await;
    let sqls = [STAR, "SELECT * FROM t WHERE id >= $1 ORDER BY id"];
    let mut failures = Vec::new();
    for change in CHANGES {
        for sql in sqls {
            let server = TestServer::start().await.expect("server");
            let mut conn = PgConnection::connect(&pinned_url(&server)).await.expect("connect");
            for setup in change.setup {
                conn.execute(*setup).await.expect("setup");
            }
            cached(&mut conn, sql).await.expect("first execute");
            for alter in change.alter {
                conn.execute(*alter).await.expect("alter");
            }
            let expected = twin(&mut conn, STAR).await;
            for n in 1..=4 {
                let got = cached(&mut conn, sql).await;
                if got.as_ref() != Ok(&expected) {
                    failures.push(format!(
                        "{} / {sql}: execute {n}: got {got:?}, twin {expected:?}",
                        change.name
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// The twin's values, as PostgreSQL 17 returns them, so the twin itself is
/// checked too.
#[tokio::test]
async fn the_twins_are_postgresql_answers() {
    for change in CHANGES {
        let server = TestServer::start().await.expect("server");
        let mut conn = PgConnection::connect(&pinned_url(&server)).await.expect("connect");
        for sql in change.setup.iter().chain(change.alter) {
            conn.execute(*sql).await.expect("setup");
        }
        let rows = twin(&mut conn, STAR).await;
        let expected: Vec<String> = change
            .rows
            .iter()
            .map(|r| {
                change
                    .after
                    .iter()
                    .zip(r.iter())
                    .map(|(c, v)| match v {
                        Some(v) if v.parse::<i64>().is_ok() => format!("{c}=Some({v})"),
                        Some(v) => format!("{c}=Some({v:?})"),
                        None => format!("{c}=None"),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        assert_eq!(rows, expected, "{}", change.name);
    }
}

/// A client that never describes again gets a description on every execute,
/// so those executes still have to be able to run from the cached parse
/// (`PreparedSelect`), which is learned afresh after the change, and send
/// the description from there. The count of cached runs is global to the
/// process, so this test does not run beside the others that move it.
#[tokio::test]
async fn sqlx_point_lookups_after_a_change_run_from_the_cached_parse() {
    use rasterizeddb_core::pgwire::PREPARED_RUNS;
    use std::sync::atomic::Ordering;
    let _counted = COUNTED.lock().await;
    let server = TestServer::start().await.expect("server");
    let mut conn = PgConnection::connect(&pinned_url(&server)).await.expect("connect");
    conn.execute("CREATE TABLE t (id INT, a INT)").await.expect("create");
    for k in 1..=8 {
        conn.execute(AssertSqlSafe(format!("INSERT INTO t (id, a) VALUES ({k}, {})", 100 + k)))
            .await
            .expect("insert");
    }
    let point = "SELECT * FROM t WHERE id = $1";
    let lookup = |k: i32| sqlx::query(AssertSqlSafe(point.to_string())).bind(k);
    let rows = lookup(1).fetch_all(&mut conn).await.expect("first execute");
    assert_eq!(rows.iter().map(render).collect::<Vec<_>>(), ["id=Some(1) a=Some(101)"]);

    conn.execute("ALTER TABLE t ADD COLUMN c INT").await.expect("alter");
    conn.execute("UPDATE t SET c = id * 7").await.expect("fill");
    let before = PREPARED_RUNS.load(Ordering::Relaxed);
    for k in 1..=8 {
        let rows = lookup(k).fetch_all(&mut conn).await.expect("execute");
        // PostgreSQL 17: `SELECT * FROM t WHERE id = k` after the change.
        let expected = format!("id=Some({k}) a=Some({}) c=Some({})", 100 + k, 7 * k);
        assert_eq!(rows.iter().map(render).collect::<Vec<_>>(), [expected], "id {k}");
    }
    assert!(
        PREPARED_RUNS.load(Ordering::Relaxed) > before,
        "no execute after the change ran from the cached parse"
    );
}

/// Any prepared execute can move the count of cached runs, so every test here
/// that runs one takes this too.
static COUNTED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&pinned_url(server), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// tokio-postgres keeps the `Statement` it prepared and accepts nothing but
/// DataRows after a Bind, so a description arriving with the rows is an
/// error to it: "unexpected message from server", which is how PostgreSQL's
/// 0A000 reaches it too. What it must never do is succeed with the old
/// columns: every execute after the change either fails or returns exactly
/// the twin's names and values. The connection stays usable, and preparing
/// again reads the new shape.
#[tokio::test]
async fn tokio_postgres_never_reads_rows_under_the_old_columns() {
    let _counted = COUNTED.lock().await;
    for change in CHANGES {
        let server = TestServer::start().await.expect("server");
        let client = tp(&server).await;
        for sql in change.setup {
            client.batch_execute(sql).await.expect("setup");
        }
        let stmt = client.prepare(STAR).await.expect("prepare");
        client.query(&stmt, &[]).await.expect("first execute");
        for sql in change.alter {
            client.batch_execute(sql).await.expect("alter");
        }
        let expected = tp_twin(&client).await;
        for n in 1..=4 {
            match client.query(&stmt, &[]).await {
                Err(e) => assert!(
                    e.to_string().contains("unexpected message") || e.to_string().contains("0A000"),
                    "{}: execute {n}: {e}",
                    change.name
                ),
                Ok(rows) => {
                    let got: Vec<Vec<(String, Option<String>)>> = rows.iter().map(tp_render).collect();
                    assert_eq!(got, expected, "{}: execute {n} read the old columns", change.name);
                }
            }
        }
        // Still usable, and a fresh prepare reads the new shape.
        let fresh = client.prepare(STAR).await.expect("prepare again");
        let rows = client.query(&fresh, &[]).await.expect("fresh statement");
        let got: Vec<_> = rows.iter().map(tp_render).collect();
        assert_eq!(got, expected, "{}", change.name);
    }
}

fn tp_render(row: &tokio_postgres::Row) -> Vec<(String, Option<String>)> {
    use tokio_postgres::types::Type;
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let v = match *c.type_() {
                Type::INT4 => row.try_get::<_, Option<i32>>(i).map(|v| v.map(|v| v.to_string())),
                Type::INT8 => row.try_get::<_, Option<i64>>(i).map(|v| v.map(|v| v.to_string())),
                _ => row.try_get::<_, Option<String>>(i),
            };
            (c.name().to_string(), v.unwrap_or_else(|e| Some(format!("<{e}>"))))
        })
        .collect()
}

async fn tp_twin(client: &tokio_postgres::Client) -> Vec<Vec<(String, Option<String>)>> {
    let mut out = Vec::new();
    for m in client.simple_query(STAR).await.expect("twin") {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            out.push(
                r.columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name().to_string(), r.get(i).map(str::to_string)))
                    .collect(),
            );
        }
    }
    out
}

/// Finding L13 through a real driver: sqlx's cached `UPDATE … RETURNING *`,
/// described before an ADD COLUMN, decodes the new shape on every execute
/// after it, names and values both, as the twin (the row read back by a
/// simple query) does.
#[tokio::test]
async fn sqlx_cached_returning_decodes_the_new_shape_every_time() {
    let _counted = COUNTED.lock().await;
    let server = TestServer::start().await.expect("server");
    let mut conn = PgConnection::connect(&pinned_url(&server)).await.expect("connect");
    conn.execute("CREATE TABLE t (id INT, a INT)").await.expect("create");
    conn.execute("INSERT INTO t (id, a) VALUES (1, 10)").await.expect("insert");
    let sql = "UPDATE t SET a = a + 1 WHERE id = $1 RETURNING *";
    assert_eq!(cached(&mut conn, sql).await.expect("first execute"), ["id=Some(1) a=Some(11)"]);
    conn.execute("ALTER TABLE t ADD COLUMN c INT DEFAULT 7").await.expect("alter");
    let mut failures = Vec::new();
    for n in 1..=3 {
        let got = cached(&mut conn, sql).await;
        let expected = vec![format!("id=Some(1) a=Some({}) c=Some(7)", 11 + n)];
        if got.as_ref() != Ok(&expected) {
            failures.push(format!("execute {n}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    let twin_rows = twin(&mut conn, "SELECT * FROM t").await;
    assert_eq!(twin_rows, ["id=Some(1) a=Some(14) c=Some(7)"]);
}

/// The default, `auto` (slice B8b), through sqlx's cached `UPDATE …
/// RETURNING *`: the first execute after the ADD COLUMN is refused before the
/// write runs (PostgreSQL's 0A000; the row is not updated); sqlx executes
/// its cached statement again without a new Parse, and from then on every
/// execute is applied once and decodes the new shape, as the twin does.
///
/// Mutation, failing an assertion here: `auto` resolved as `error` always
/// (sqlx fails every execute: it never re-prepares).
#[tokio::test]
async fn sqlx_cached_returning_under_the_default_is_refused_once_then_decodes() {
    let _counted = COUNTED.lock().await;
    let server = TestServer::start().await.expect("server");
    let mut conn = PgConnection::connect(&server.url()).await.expect("connect");
    let shown = sqlx::query("SHOW rdb.shape_change").fetch_one(&mut conn).await.expect("show");
    assert_eq!(shown.get::<String, _>(0), "auto");
    conn.execute("CREATE TABLE t (id INT, a INT)").await.expect("create");
    conn.execute("INSERT INTO t (id, a) VALUES (1, 10)").await.expect("insert");
    let sql = "UPDATE t SET a = a + 1 WHERE id = $1 RETURNING *";
    assert_eq!(cached(&mut conn, sql).await.expect("first execute"), ["id=Some(1) a=Some(11)"]);
    conn.execute("ALTER TABLE t ADD COLUMN c INT DEFAULT 7").await.expect("alter");
    let refused = cached(&mut conn, sql).await.expect_err("the first stale execute ran");
    assert!(refused.contains("cached plan must not change result type"), "{refused}");
    assert_eq!(twin(&mut conn, "SELECT * FROM t").await, ["id=Some(1) a=Some(11) c=Some(7)"], "the refused write was applied");
    for n in 1..=3 {
        let got = cached(&mut conn, sql).await;
        let expected = vec![format!("id=Some(1) a=Some({}) c=Some(7)", 11 + n)];
        assert_eq!(got.as_ref(), Ok(&expected), "execute {n} after the refusal");
    }
    assert_eq!(twin(&mut conn, "SELECT * FROM t").await, ["id=Some(1) a=Some(14) c=Some(7)"]);
}

/// `rdb.shape_change = error` set in the startup packet (Q2), as a JVM or
/// .NET driver would be configured: tokio-postgres's kept statement gets
/// PostgreSQL's 0A000 after the change, every time, never rows under the old
/// columns; preparing again reads the new shape.
#[tokio::test]
async fn error_mode_from_the_startup_packet_answers_0a000() {
    let _counted = COUNTED.lock().await;
    for change in CHANGES {
        let server = TestServer::start().await.expect("server");
        let url = format!("{}&options=-c%20rdb.shape_change%3Derror", server.url());
        let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("tokio-postgres connects");
        tokio::spawn(conn);
        let mode = client.simple_query("SHOW rdb.shape_change").await.expect("show");
        let shown: Vec<String> = mode
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        assert_eq!(shown, ["error"], "{}", change.name);
        for sql in change.setup {
            client.batch_execute(sql).await.expect("setup");
        }
        let stmt = client.prepare(STAR).await.expect("prepare");
        client.query(&stmt, &[]).await.expect("first execute");
        for sql in change.alter {
            client.batch_execute(sql).await.expect("alter");
        }
        for n in 1..=2 {
            let e = client.query(&stmt, &[]).await.expect_err("a stale statement ran");
            let db = e.as_db_error().unwrap_or_else(|| panic!("{}: execute {n}: {e}", change.name));
            assert_eq!(db.code().code(), "0A000", "{}: execute {n}", change.name);
            assert_eq!(db.message(), "cached plan must not change result type", "{}", change.name);
        }
        let expected = tp_twin(&client).await;
        let fresh = client.prepare(STAR).await.expect("prepare again");
        let rows = client.query(&fresh, &[]).await.expect("fresh statement");
        let got: Vec<_> = rows.iter().map(tp_render).collect();
        assert_eq!(got, expected, "{}", change.name);
    }
}
