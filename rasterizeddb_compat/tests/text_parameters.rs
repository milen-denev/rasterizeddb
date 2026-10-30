//! Text-format parameters, sent as libpq's `PQexecParams` and psql's
//! `\bind` send them: Parse with the types the client declared (none, or
//! some), Bind with text values, no statement Describe. Each expectation is
//! PostgreSQL 17's (`docker run --rm postgres:17`).
//!
//! - An untyped value for a text column is stored as sent. `'007'` was
//!   stored as `7`, `'+15551234'` as `15551234`, `'1e99999'` (7 bytes) as a
//!   100,000-character number: the value was inlined unquoted because it looks
//!   like a number, and read as a numeric constant written in the statement.
//! - A value declared `timestamptz` (OID 1184) or `timestamp` (1114) is read
//!   as that type, whatever column or comparison meets it: a `timestamp`
//!   column dropped the zone of a `timestamptz` value (PostgreSQL converts
//!   it), and a `timestamptz` column applied the zone of a `timestamp` one
//!   (PostgreSQL drops it).

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rasterizeddb_compat::TestServer;

struct Raw {
    stream: TcpStream,
}

impl Raw {
    async fn connect(port: u16) -> Raw {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
        let mut conn = Raw { stream };
        let mut body = 196608i32.to_be_bytes().to_vec();
        for s in ["user", "rasterized", "database", "postgres"] {
            body.extend_from_slice(s.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        conn.stream.write_all(&msg).await.expect("startup");
        conn.until_ready().await.expect("startup");
        conn
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut msg = vec![tag];
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(body);
        self.stream.write_all(&msg).await.expect("send");
    }

    async fn read(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(30), async {
            let tag = self.stream.read_u8().await.expect("tag");
            let len = self.stream.read_i32().await.expect("length") as usize;
            let mut body = vec![0u8; len - 4];
            self.stream.read_exact(&mut body).await.expect("body");
            (tag, body)
        })
        .await
        .expect("the server stopped answering")
    }

    /// Up to ReadyForQuery: the rows, or the first error's SQLSTATE and text.
    async fn until_ready(&mut self) -> Result<Vec<Vec<Option<String>>>, String> {
        let (mut rows, mut error) = (Vec::new(), None);
        loop {
            let (tag, body) = self.read().await;
            match tag {
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
                    rows.push(row);
                }
                b'E' => {
                    let field = |k: u8| {
                        body.split(|b| *b == 0)
                            .find(|f| f.first() == Some(&k))
                            .map(|f| String::from_utf8_lossy(&f[1..]).into_owned())
                            .unwrap_or_default()
                    };
                    error.get_or_insert(format!("{} {}", field(b'C'), field(b'M')));
                }
                b'Z' => break,
                _ => {}
            }
        }
        error.map_or(Ok(rows), Err)
    }

    /// One statement over the simple protocol.
    async fn simple(&mut self, sql: &str) -> Vec<Vec<Option<String>>> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body).await;
        self.until_ready().await.unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// Parse (when `sql` is given) as `name` with `types`, then Bind the
    /// text `values`, Describe the portal, Execute, Sync: `PQexecParams`,
    /// or with a name, `PQprepare` + `PQexecPrepared`.
    async fn run(&mut self, name: &str, sql: Option<&str>, types: &[i32], values: &[Option<&str>]) -> Result<Vec<Vec<Option<String>>>, String> {
        if let Some(sql) = sql {
            let mut parse = name.as_bytes().to_vec();
            parse.push(0);
            parse.extend_from_slice(sql.as_bytes());
            parse.push(0);
            parse.extend_from_slice(&(types.len() as i16).to_be_bytes());
            for t in types {
                parse.extend_from_slice(&t.to_be_bytes());
            }
            self.send(b'P', &parse).await;
        }
        let mut bind = vec![0u8];
        bind.extend_from_slice(name.as_bytes());
        bind.push(0);
        bind.extend_from_slice(&0i16.to_be_bytes()); // all text
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            match v {
                None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(v) => {
                    bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    bind.extend_from_slice(v.as_bytes());
                }
            }
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'B', &bind).await;
        self.send(b'D', &[b'P', 0]).await;
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &execute).await;
        self.send(b'S', &[]).await;
        self.until_ready().await
    }
}

fn texts(rows: Vec<Vec<Option<String>>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|r| r.into_iter().map(|c| c.unwrap_or_else(|| "NULL".into())).collect())
        .collect()
}

/// `(sent, stored)`: PostgreSQL 17 stores every one as sent.
const SENT: &[&str] = &["007", "+15551234", ".5", "00", "-0", "0012", "1e5", "1E-5", "1e99999"];

#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_value_for_a_text_column_is_stored_as_sent() {
    let server = TestServer::start().await.expect("server");
    let mut c = Raw::connect(server.port()).await;
    c.simple("CREATE TABLE pp (id INT NOT NULL, v TEXT, w VARCHAR(20), n INT)").await;
    for (id, sent) in SENT.iter().enumerate() {
        let id = id.to_string();
        c.run("", Some("INSERT INTO pp (id, v, w) VALUES ($1, $2, $3)"), &[], &[Some(&id), Some(sent), Some(sent)])
            .await
            .unwrap_or_else(|e| panic!("{sent}: {e}"));
    }
    let want: Vec<Vec<String>> = SENT.iter().map(|s| vec![s.to_string(), s.to_string()]).collect();
    assert_eq!(texts(c.simple("SELECT v, w FROM pp ORDER BY id").await), want);

    // UPDATE, and a named statement run twice (`PQprepare`).
    c.run("", Some("UPDATE pp SET v = $1 WHERE id = $2"), &[], &[Some("0099"), Some("0")]).await.expect("update");
    c.run("s1", Some("UPDATE pp SET w = $1 WHERE id = $2"), &[], &[Some("+44"), Some("1")]).await.expect("prepared");
    c.run("s1", None, &[], &[Some("007.50"), Some("2")]).await.expect("prepared again");
    assert_eq!(
        texts(c.simple("SELECT v, w FROM pp WHERE id <= 2 ORDER BY id").await),
        [["0099", "007"], ["+15551234", "+44"], [".5", "007.50"]].map(|r| r.map(String::from).to_vec())
    );

    // An integer column still reads the text as its number.
    c.run("", Some("UPDATE pp SET n = $1 WHERE id = $2"), &[], &[Some("007"), Some("0")]).await.expect("int");
    assert_eq!(texts(c.simple("SELECT n FROM pp WHERE id = 0").await), [["7"]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_timestamp_value_is_read_as_its_declared_type() {
    let server = TestServer::start().await.expect("server");
    let mut c = Raw::connect(server.port()).await;
    c.simple("CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ)").await;
    let zoned = "2024-01-01 10:00:00+02";
    // a TIMESTAMP gets a timestamptz value, b TIMESTAMPTZ a timestamp one.
    c.run("", Some("INSERT INTO ts VALUES ($1, $2, $3)"), &[23, 1184, 1114], &[Some("1"), Some(zoned), Some(zoned)])
        .await
        .expect("insert");
    // An untyped value with a cast on its placeholder.
    c.run("", Some("INSERT INTO ts VALUES ($1, $2::timestamptz, $3::timestamp)"), &[], &[Some("2"), Some(zoned), Some(zoned)])
        .await
        .expect("insert with casts");
    // PostgreSQL 17: 08:00 | 10:00+00 for both.
    assert_eq!(
        texts(c.simple("SELECT k, a, b FROM ts ORDER BY k").await),
        [["1", "2024-01-01 08:00:00", "2024-01-01 10:00:00+00"], ["2", "2024-01-01 08:00:00", "2024-01-01 10:00:00+00"]]
            .map(|r| r.map(String::from).to_vec())
    );
    // A comparison reads it the same way: 08:00 rows, both.
    let found = c.run("", Some("SELECT k FROM ts WHERE a = $1 ORDER BY k"), &[1184], &[Some(zoned)]).await.expect("select");
    assert_eq!(texts(found), [["1"], ["2"]]);
    let found = c.run("", Some("SELECT k FROM ts WHERE b = $1 ORDER BY k"), &[1114], &[Some(zoned)]).await.expect("select");
    assert_eq!(texts(found), [["1"], ["2"]]);

    // Into a text column each is shown as PostgreSQL shows its type as
    // text: `+00` for timestamptz, no zone for timestamp.
    c.simple("CREATE TABLE tx (k INT NOT NULL, v TEXT)").await;
    c.run("", Some("INSERT INTO tx VALUES ($1, $2)"), &[23, 1184], &[Some("1"), Some(zoned)]).await.expect("1184");
    c.run("", Some("INSERT INTO tx VALUES ($1, $2)"), &[23, 1114], &[Some("2"), Some(zoned)]).await.expect("1114");
    assert_eq!(
        texts(c.simple("SELECT v FROM tx ORDER BY k").await),
        [["2024-01-01 08:00:00+00"], ["2024-01-01 10:00:00"]]
    );
}

/// Wherever an untyped value lands in a text column, it is stored as sent:
/// the shapes below place the parameter where its position does not name
/// the column (an alias, a CTE ahead of the INSERT, `INSERT … SELECT`), and
/// each stored `'007'` as `7` (PostgreSQL 17 and base: `007`). The value is
/// recognised as a bound parameter, not by where it stands.
#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_value_is_stored_as_sent_whatever_the_statements_shape() {
    let server = TestServer::start().await.expect("server");
    let mut c = Raw::connect(server.port()).await;
    c.simple("CREATE TABLE pq (id INT NOT NULL, v TEXT)").await;
    c.simple("CREATE TABLE \"Qt\" (id INT NOT NULL, \"Val\" TEXT)").await;
    for (id, sql) in [
        (1, "INSERT INTO pq AS q (id, v) VALUES ($1, $2)"),
        (2, "WITH c AS (SELECT 1 AS k) INSERT INTO pq (id, v) VALUES ($1, $2)"),
        (3, "INSERT INTO pq (id, v) SELECT $1, $2"),
        (4, "INSERT INTO pq (id, v) SELECT $1, $2 WHERE true"),
    ] {
        let id = id.to_string();
        c.run("", Some(sql), &[], &[Some(&id), Some("007")]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    c.run("", Some("INSERT INTO pq (id, v) SELECT 5, $1"), &[], &[Some("007")]).await.expect("SELECT 5, $1");
    c.run("", Some("INSERT INTO \"Qt\" AS q (id, \"Val\") VALUES ($1, $2)"), &[], &[Some("1"), Some("007")])
        .await
        .expect("quoted names");
    // Not PostgreSQL syntax (it wants AS); accepted here, it must still be exact.
    let _ = c.run("", Some("INSERT INTO pq q (id, v) VALUES ($1, $2)"), &[], &[Some("6"), Some("007")]).await;
    let stored = texts(c.simple("SELECT id, v FROM pq ORDER BY id").await);
    assert!(stored.iter().all(|r| r[1] == "007"), "{stored:?}");
    assert!(stored.len() >= 5, "{stored:?}");
    assert_eq!(texts(c.simple("SELECT \"Val\" FROM \"Qt\"").await), [["007"]]);

    // A constant written in the statement is still its value's text, beside
    // a bound parameter, and an integer column still reads the text as 7.
    c.run("", Some("INSERT INTO pq (id, v) VALUES ($1, 1e1)"), &[], &[Some("20")]).await.expect("constant");
    assert_eq!(texts(c.simple("SELECT v FROM pq WHERE id = 20").await), [["10"]]);
    c.run("", Some("INSERT INTO pq AS q (id, v) VALUES ($1, 'x')"), &[], &[Some("007")]).await.expect("int");
    assert_eq!(texts(c.simple("SELECT id FROM pq WHERE v = 'x'").await), [["7"]]);
}

// A parameter that is an operand inside a text column's value is typed by
// its expression, not by the column: `$1 + 1` with '007' is 8, as
// PostgreSQL stores it. Only the whole of the slot, parenthesised or a side
// of `||`, is the column's text. Typing every parameter in the slot as text
// refused `$1 + 1` and `-$1`, which the base stored.
#[tokio::test]
async fn a_parameter_inside_an_expression_is_typed_by_the_expression() {
    let server = TestServer::start().await.expect("server");
    let mut c = Raw::connect(server.port()).await;
    c.simple("CREATE TABLE pe (id INT NOT NULL, a TEXT)").await;
    for (sql, value) in [
        ("INSERT INTO pe (id, a) VALUES (10, $1 + 1)", "007"),
        ("INSERT INTO pe (id, a) VALUES (11, -$1)", "5"),
        ("INSERT INTO pe (id, a) VALUES (12, - $1)", "5"),
        ("INSERT INTO pe (id, a) VALUES (13, ($1))", "007"),
        ("INSERT INTO pe (id, a) VALUES (14, $1 || 'x')", "007"),
    ] {
        c.run("", Some(sql), &[], &[Some(value)]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        texts(c.simple("SELECT id, a FROM pe ORDER BY id").await),
        [["10", "8"], ["11", "-5"], ["12", "-5"], ["13", "007"], ["14", "007x"]]
    );
}
