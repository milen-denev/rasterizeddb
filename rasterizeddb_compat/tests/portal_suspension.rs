//! `Execute` with a nonzero `max_rows`: at most that many DataRows, then
//! `PortalSuspended`; the next Execute on the portal resumes; CommandComplete
//! only once the rows run out. It used to ignore the limit and send every row
//! with a CommandComplete, so a client paging through a cursor
//! (`query_portal`, JDBC `fetchSize`, psycopg named cursors) got the whole
//! result on the first fetch and nothing it asked for afterwards.

use rasterizeddb_compat::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

async fn seed(client: &tokio_postgres::Client, n: i32) {
    client
        .batch_execute("CREATE TABLE ps (id INT PRIMARY KEY, label TEXT NOT NULL)")
        .await
        .expect("create");
    for i in 1..=n {
        client
            .batch_execute(&format!("INSERT INTO ps (id, label) VALUES ({i}, 'row {i}')"))
            .await
            .expect("insert");
    }
}

#[tokio::test]
async fn query_portal_pages_through_the_result() {
    let server = TestServer::start().await.expect("server");
    let mut client = tp(&server).await;
    seed(&client, 7).await;

    let tx = client.transaction().await.expect("begin");
    let stmt = tx
        // No parameters: a statement describe answers ParameterDescription
        // with the OIDs the client declared, and tokio-postgres declares none,
        // so it would refuse to bind a value (a separate gap, not this one).
        .prepare("SELECT id, label FROM ps WHERE id > 0 ORDER BY id")
        .await
        .expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind");

    let mut seen = Vec::new();
    let mut pages = Vec::new();
    loop {
        let page = tx.query_portal(&portal, 3).await.expect("fetch a page");
        if page.is_empty() {
            break;
        }
        pages.push(page.len());
        for row in page {
            seen.push((row.get::<_, i32>(0), row.get::<_, String>(1)));
        }
        assert!(pages.len() < 10, "the portal never ran out: {pages:?}");
    }
    assert_eq!(pages, vec![3, 3, 1], "three at a time");
    assert_eq!(
        seen,
        (1..=7).map(|i| (i, format!("row {i}"))).collect::<Vec<_>>()
    );
    tx.commit().await.expect("commit");

    // The connection is still in step afterwards.
    let n: i64 = client
        .query_one("SELECT COUNT(*) FROM ps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 7);
}

#[tokio::test]
async fn a_limit_the_result_fits_in_completes_at_once() {
    let server = TestServer::start().await.expect("server");
    let mut client = tp(&server).await;
    seed(&client, 2).await;
    let tx = client.transaction().await.expect("begin");
    let stmt = tx.prepare("SELECT id FROM ps ORDER BY id").await.expect("prepare");
    let portal = tx.bind(&stmt, &[]).await.expect("bind");
    let page = tx.query_portal(&portal, 50).await.expect("fetch");
    assert_eq!(page.len(), 2);
    tx.commit().await.expect("commit");
}

// ---------------------------------------------------------------- raw protocol

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
    let mut body = vec![0u8; (i32::from_be_bytes(len) as usize).saturating_sub(4)];
    if !body.is_empty() {
        stream.read_exact(&mut body).await.expect("body");
    }
    (t[0], body)
}

async fn until_ready(stream: &mut TcpStream) -> Vec<u8> {
    let mut tags = Vec::new();
    loop {
        let (t, _) = read_msg(stream).await;
        tags.push(t);
        if t == b'Z' {
            return tags;
        }
    }
}

async fn raw(server: &TestServer) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", server.port())).await.expect("connect");
    let mut body = 196608i32.to_be_bytes().to_vec();
    for s in ["user", "postgres", "database", "postgres"] {
        body.extend_from_slice(&cstr(s));
    }
    body.push(0);
    let mut out = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    stream.write_all(&out).await.expect("startup");
    until_ready(&mut stream).await;
    stream
}

fn execute(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut e = cstr(portal);
    e.extend_from_slice(&max_rows.to_be_bytes());
    msg(b'E', &e)
}

fn count(tags: &[u8], tag: u8) -> usize {
    tags.iter().filter(|t| **t == tag).count()
}

#[tokio::test]
async fn raw_protocol_suspends_resumes_and_honours_zero() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    seed(&client, 5).await;
    let mut s = raw(&server).await;

    let mut parse = cstr("q");
    parse.extend_from_slice(&cstr("SELECT id FROM ps ORDER BY id"));
    parse.extend_from_slice(&0i16.to_be_bytes());
    let mut bind = cstr("p");
    bind.extend_from_slice(&cstr("q"));
    bind.extend_from_slice(&[0, 0, 0, 0, 0, 0]);

    // BEGIN so the portal outlives each Sync, as PostgreSQL requires.
    s.write_all(&msg(b'Q', &cstr("BEGIN"))).await.unwrap();
    until_ready(&mut s).await;

    let batch = [msg(b'P', &parse), msg(b'B', &bind), execute("p", 2), msg(b'S', &[])].concat();
    s.write_all(&batch).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!(count(&tags, b'D'), 2, "{:?}", tags);
    assert_eq!(count(&tags, b's'), 1, "PortalSuspended: {:?}", tags);
    assert_eq!(count(&tags, b'C'), 0, "no CommandComplete yet: {:?}", tags);

    s.write_all(&[execute("p", 2), msg(b'S', &[])].concat()).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!((count(&tags, b'D'), count(&tags, b's'), count(&tags, b'C')), (2, 1, 0), "{:?}", tags);

    // max_rows = 0 means "all the rest".
    s.write_all(&[execute("p", 0), msg(b'S', &[])].concat()).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!((count(&tags, b'D'), count(&tags, b's'), count(&tags, b'C')), (1, 0, 1), "{:?}", tags);

    // A fresh portal with max_rows = 0 sends everything at once.
    let batch = [msg(b'B', &bind), execute("p", 0), msg(b'S', &[])].concat();
    s.write_all(&batch).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!((count(&tags, b'D'), count(&tags, b's'), count(&tags, b'C')), (5, 0, 1), "{:?}", tags);

    // Close drops a suspended portal: executing it afterwards is an error.
    let batch = [msg(b'B', &bind), execute("p", 1), msg(b'S', &[])].concat();
    s.write_all(&batch).await.unwrap();
    until_ready(&mut s).await;
    let mut close = vec![b'P'];
    close.extend_from_slice(&cstr("p"));
    s.write_all(&[msg(b'C', &close), execute("p", 1), msg(b'S', &[])].concat()).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!(count(&tags, b'D'), 0, "{:?}", tags);
    assert_eq!(count(&tags, b'E'), 1, "a closed portal does not exist: {:?}", tags);

    s.write_all(&msg(b'Q', &cstr("ROLLBACK"))).await.unwrap();
    until_ready(&mut s).await;

    // Outside a block, Sync ends the suspended result: the next Execute runs
    // the statement afresh rather than resuming.
    let batch = [msg(b'B', &bind), execute("p", 2), msg(b'S', &[])].concat();
    s.write_all(&batch).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!(count(&tags, b'D'), 2, "{:?}", tags);
    s.write_all(&[execute("p", 0), msg(b'S', &[])].concat()).await.unwrap();
    let tags = until_ready(&mut s).await;
    assert_eq!(count(&tags, b'D'), 5, "rerun from the start, not resumed: {:?}", tags);
}
