//! `Describe` answers are remembered per prepared statement. These pin what
//! must make the server describe again, because a stale description is not a
//! label problem: the same column list types the payload, so a wrong type
//! there corrupts the values (see describe-and-payload-share-one-source).
//!
//! Talks the protocol directly: sqlx never sends a portal Describe.

use rasterizeddb_compat::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

async fn connect(server: &TestServer) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
        .await
        .expect("connect");
    let mut body = 196608i32.to_be_bytes().to_vec();
    for s in ["user", "postgres", "database", "postgres"] {
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

/// `(name, type oid)` of each field of a RowDescription.
fn fields(body: &[u8]) -> Vec<(String, i32)> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2;
    let mut out = Vec::new();
    for _ in 0..count {
        let end = at + body[at..].iter().position(|b| *b == 0).expect("name");
        let name = String::from_utf8(body[at..end].to_vec()).expect("utf8 name");
        at = end + 1;
        let oid = i32::from_be_bytes(body[at + 6..at + 10].try_into().unwrap());
        at += 18;
        out.push((name, oid));
    }
    out
}

fn parse(name: &str, sql: &str, oids: &[i32]) -> Vec<u8> {
    let mut p = cstr(name);
    p.extend_from_slice(&cstr(sql));
    p.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for oid in oids {
        p.extend_from_slice(&oid.to_be_bytes());
    }
    msg(b'P', &p)
}

fn bind_text(portal: &str, stmt: &str, values: &[&str]) -> Vec<u8> {
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

fn execute(portal: &str) -> Vec<u8> {
    let mut e = cstr(portal);
    e.extend_from_slice(&0i32.to_be_bytes());
    msg(b'E', &e)
}

fn sync() -> Vec<u8> {
    msg(b'S', &[])
}

/// Sends a batch and returns the field list of the first RowDescription.
async fn described(stream: &mut TcpStream, batch: Vec<u8>) -> Option<Vec<(String, i32)>> {
    stream.write_all(&batch).await.expect("send");
    let replies = until_ready(stream).await;
    if let Some((_, body)) = replies.iter().find(|(t, _)| *t == b'E') {
        panic!("{}", String::from_utf8_lossy(body));
    }
    replies
        .iter()
        .find(|(t, _)| *t == b'T')
        .map(|(_, body)| fields(body))
}

const INT4: i32 = 23;
const TEXT: i32 = 25;

fn names(f: &Option<Vec<(String, i32)>>) -> Vec<String> {
    f.as_ref()
        .map(|v| v.iter().map(|(n, _)| n.clone()).collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn a_statement_describe_follows_the_schema() {
    let server = TestServer::start().await.expect("server");
    let mut s = connect(&server).await;
    simple(&mut s, "CREATE TABLE dc (a INT, b TEXT)").await;

    let sql = "SELECT * FROM dc WHERE a = $1";
    let first = described(&mut s, [parse("s1", sql, &[]), describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(names(&first), ["a", "b"]);
    // Asked again with nothing changed: the same answer.
    let again = described(&mut s, [describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(again, first);

    // ALTER moves the schema generation.
    simple(&mut s, "ALTER TABLE dc ADD COLUMN c INT").await;
    let altered = described(&mut s, [describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(names(&altered), ["a", "b", "c"]);

    // DROP + CREATE under the same name with another shape, no ALTER at all.
    simple(&mut s, "DROP TABLE dc").await;
    simple(&mut s, "CREATE TABLE dc (a INT, z INT, y TEXT)").await;
    let recreated = described(&mut s, [describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(names(&recreated), ["a", "z", "y"]);
    // Re-parsing the identical text must not resurrect an older answer.
    let reparsed = described(&mut s, [parse("s1", sql, &[]), describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(names(&reparsed), ["a", "z", "y"]);

    // RENAME: the old name no longer resolves; the new one does.
    simple(&mut s, "ALTER TABLE dc RENAME TO dc2").await;
    simple(&mut s, "CREATE TABLE dc (q TEXT)").await;
    let renamed = described(&mut s, [describe(b'S', "s1"), sync()].concat()).await;
    assert_eq!(names(&renamed), ["q"]);
}

#[tokio::test]
async fn a_portal_describe_follows_the_bound_value() {
    let server = TestServer::start().await.expect("server");
    let mut s = connect(&server).await;
    simple(&mut s, "CREATE TABLE one (id INT)").await;
    simple(&mut s, "INSERT INTO one (id) VALUES (1)").await;

    // Untyped `$1`: what it describes as depends on what was bound.
    let sql = "SELECT $1 AS v FROM one";
    let number = described(
        &mut s,
        [parse("", sql, &[]), bind_text("", "", &["42"]), describe(b'P', ""), execute(""), sync()].concat(),
    )
    .await
    .expect("described");
    let word = described(
        &mut s,
        [parse("", sql, &[]), bind_text("", "", &["abc"]), describe(b'P', ""), execute(""), sync()].concat(),
    )
    .await
    .expect("described");
    assert_eq!(number.len(), 1);
    assert_eq!(word.len(), 1);
    assert_eq!(word[0].1, TEXT, "a bound word describes as text");
    assert_ne!(number[0].1, word[0].1, "a number and a word are not the same type");
    // And back: the earlier answer is not reused for a different value.
    let number_again = described(
        &mut s,
        [parse("", sql, &[]), bind_text("", "", &["42"]), describe(b'P', ""), execute(""), sync()].concat(),
    )
    .await
    .expect("described");
    assert_eq!(number_again, number);

    // Typed `$1`: a different parameter OID is a different statement.
    let as_int = described(
        &mut s,
        [parse("t", sql, &[INT4]), bind_text("", "t", &["7"]), describe(b'P', ""), execute(""), sync()].concat(),
    )
    .await
    .expect("described");
    let as_text = described(
        &mut s,
        [parse("t", sql, &[TEXT]), bind_text("", "t", &["7"]), describe(b'P', ""), execute(""), sync()].concat(),
    )
    .await
    .expect("described");
    assert_eq!(as_text[0].1, TEXT, "{as_text:?}");
    assert_ne!(as_int[0].1, as_text[0].1, "{as_int:?} vs {as_text:?}");
}

#[tokio::test]
async fn a_statement_describe_with_non_ascii_names_and_a_placeholder() {
    // The stand-in substitution re-encoded every byte above 0x7F as its own
    // character, so the table and column names no longer matched anything
    // and the statement was described as returning no columns.
    let server = TestServer::start().await.expect("server");
    let mut s = connect(&server).await;
    simple(&mut s, r#"CREATE TABLE "tëst" ("日本" INT, "naïve" TEXT)"#).await;
    let d = described(
        &mut s,
        [
            parse("s", r#"SELECT "naïve", "日本" FROM "tëst" WHERE "日本" = $1"#, &[]),
            describe(b'S', "s"),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_eq!(names(&d), ["naïve", "日本"]);
}
