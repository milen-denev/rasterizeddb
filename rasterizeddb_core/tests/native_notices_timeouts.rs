//! Three things a native (rastcp) client used to be missing, end to end:
//! notices, `SHOW` of a session parameter outside a block, and a statement
//! that runs past the request timeout without costing its connection.
//!
//! A real server (the native listener on a unix socket, over an on-disk
//! database) runs on a runtime of its own with the binary's 32 MiB stacks,
//! built as the server's own listeners are (`handler_timeout(None)`), with a
//! short request timeout so a "long" statement is a second or two. Clients
//! are `rasterizeddb_client` (which asks for notices) and raw rastcp
//! connections that do not (a client from before notices).
//!
//! Every wait is bounded, and every test's directory is a `TempDir`.

#![cfg(unix)]
// As the library and the server binary: proving this file's server future
// `Send` runs past the default 128 since the database gained the broker.
#![recursion_limit = "256"]

use std::future::Future;
use std::sync::{Arc as StdArc, Mutex};
use std::time::{Duration, Instant};

use rastcp::client::{TcpClient, TcpClientBuilder};
use rastcp::server::TcpServerBuilder;
use rasterizeddb_client::{Client, Executor, Notice, Options};
use rasterizeddb_core::core::database::{Database, serve_native};
use rclite::Arc;

const TEST_LIMIT: Duration = Duration::from_secs(90);

/// The server's request timeout: how long a statement may run, and how long
/// a connection may sit idle.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(1500);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// The server: its own thread and runtime, ended when this is dropped.
struct Harness {
    socket: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    // Last: removed once the server thread has ended.
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Harness {
        let dir = tempfile::Builder::new()
            .prefix("rdb_n6f")
            .tempdir()
            .expect("a temp dir");
        let socket_path = dir.path().join("n.sock");
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<()>();
        let path = socket_path.clone();
        let thread = std::thread::Builder::new()
            .name("native-6f-server".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("server runtime");
                runtime.block_on(async move {
                    let database = Arc::new(Database::new(data.to_str().unwrap()).await);
                    let server = TcpServerBuilder::unix(&path)
                        .request_timeout(REQUEST_TIMEOUT)
                        .handler_timeout(None)
                        .build()
                        .await
                        .expect("the native socket binds");
                    let _ = bound_tx.send(());
                    tokio::select! {
                        _ = serve_native(&server, database.clone(), false) => {}
                        _ = shutdown_rx => {}
                    }
                });
                runtime.shutdown_timeout(Duration::from_secs(2));
            })
            .expect("the server thread");
        tokio::time::timeout(Duration::from_secs(30), bound_rx)
            .await
            .expect("the server started in time")
            .expect("the server thread bound its socket");
        Harness {
            socket: format!("unix:{}", socket_path.display()),
            shutdown: Some(shutdown),
            thread: Some(thread),
            _dir: dir,
        }
    }

    async fn client(&self) -> Client {
        Client::connect(
            &self.socket,
            Options {
                timeout: Some(Duration::from_secs(20)),
                ..Options::default()
            },
        )
        .await
        .expect("the client connects")
    }

    /// A raw plain (`rastcp/1`) connection that never asks for notices: what
    /// a client from before them is.
    async fn raw(&self) -> TcpClient {
        let mut client = TcpClientBuilder::new(&self.socket)
            .timeout(Duration::from_secs(20))
            .build()
            .await
            .expect("a native connection");
        client.connect().await.expect("connects");
        client
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A handler that keeps every notice it is given.
fn collect(client: &Client) -> StdArc<Mutex<Vec<Notice>>> {
    let seen = StdArc::new(Mutex::new(Vec::new()));
    let keep = seen.clone();
    client.set_notice_handler(move |n| keep.lock().unwrap().push(n.clone()));
    seen
}

fn take(seen: &StdArc<Mutex<Vec<Notice>>>) -> Vec<Notice> {
    std::mem::take(&mut *seen.lock().unwrap())
}

fn messages(notices: &[Notice]) -> Vec<String> {
    notices.iter().map(|n| n.message.clone()).collect()
}

/// A client that asked for notices gets them on the answer and in its
/// handler: `RAISE NOTICE` in a `DO` block, `DROP TABLE IF EXISTS` naming no
/// table and `CREATE TABLE IF NOT EXISTS` naming one, outside a transaction
/// and inside one, with PostgreSQL's severities, codes and texts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notices_reach_a_native_client_in_autocommit_and_in_a_block() {
    bounded(async {
        let h = Harness::start().await;
        let client = h.client().await;
        let seen = collect(&client);

        let rows = client
            .run("DO $$ BEGIN RAISE NOTICE 'hello %', 42; END $$")
            .await
            .expect("a DO block runs over native");
        assert_eq!(messages(rows.notices()), vec!["hello 42"]);
        assert_eq!(rows.notices()[0].severity, "NOTICE");
        assert_eq!(rows.notices()[0].sqlstate, "00000");
        assert_eq!(take(&seen), rows.notices().to_vec(), "the handler saw the same");

        let rows = client.run("DROP TABLE IF EXISTS missing").await.unwrap();
        assert_eq!(messages(rows.notices()), vec!["table \"missing\" does not exist, skipping"]);
        assert_eq!(rows.notices()[0].sqlstate, "00000");

        client.run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)").await.unwrap();
        let rows = client
            .run("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, v BIGINT)")
            .await
            .unwrap();
        assert_eq!(messages(rows.notices()), vec!["relation \"t\" already exists, skipping"]);
        assert_eq!(rows.notices()[0].sqlstate, "42P07");

        // A statement that raises nothing carries nothing.
        let rows = client.run("SELECT id FROM t").await.unwrap();
        assert!(rows.notices().is_empty());
        take(&seen);

        // A notice before an error has no answer to ride on: the handler
        // gets it.
        let failed = client
            .run("DO $$ BEGIN RAISE NOTICE 'before'; RAISE EXCEPTION 'boom'; END $$")
            .await;
        assert!(failed.is_err(), "the DO block raised");
        assert_eq!(messages(&take(&seen)), vec!["before"]);

        // Inside a block.
        let tx = client.begin().await.unwrap();
        let rows = tx.run("DO $$ BEGIN RAISE WARNING 'careful'; END $$").await.unwrap();
        assert_eq!(messages(rows.notices()), vec!["careful"]);
        assert_eq!(rows.notices()[0].severity, "WARNING");
        assert_eq!(rows.notices()[0].sqlstate, "01000");
        let rows = tx.run("DROP TABLE IF EXISTS missing").await.unwrap();
        assert_eq!(messages(rows.notices()), vec!["table \"missing\" does not exist, skipping"]);
        tx.commit().await.unwrap();
        assert_eq!(
            messages(&take(&seen)),
            vec!["careful", "table \"missing\" does not exist, skipping"],
            "the transaction's notices reached the client's handler too"
        );
    })
    .await
}

/// A client that never asks for notices (one from before them) gets every
/// answer as it always did, on a plain connection and on a multiplexed one;
/// asking is what changes the answer's form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_does_not_ask_for_notices_gets_none() {
    bounded(async {
        let h = Harness::start().await;
        let raise = b"DO $$ BEGIN RAISE NOTICE 'hello'; END $$".to_vec();

        let mut plain = h.raw().await;
        let answer = plain.send(raise.clone()).await.unwrap();
        assert_eq!(answer.first(), Some(&0), "a plain acknowledgement, got {answer:?}");
        let answer = plain.send(b"DROP TABLE IF EXISTS missing".to_vec()).await.unwrap();
        assert_eq!(answer.first(), Some(&0), "got {answer:?}");

        let mux = TcpClientBuilder::new(&h.socket)
            .timeout(Duration::from_secs(20))
            .build_multiplexed()
            .await
            .unwrap();
        mux.connect().await.unwrap();
        let answer = mux.send(&raise).await.unwrap();
        assert_eq!(answer.first(), Some(&0), "got {answer:?}");

        // Asking changes the answers of that connection alone.
        let mut asked = h.raw().await;
        let set = asked.send(b"\x01NOTICES;".to_vec()).await.unwrap();
        assert_eq!(set.first(), Some(&4), "answered in the simple-query form: {set:?}");
        let answer = asked.send(raise.clone()).await.unwrap();
        assert_eq!(answer.first(), Some(&5), "an answer carrying notices, got {answer:?}");
        assert_eq!(&answer[1..5], &1u32.to_le_bytes());
        assert_eq!(answer.last(), Some(&0), "ends with the plain acknowledgement");
        let answer = plain.send(raise).await.unwrap();
        assert_eq!(answer.first(), Some(&0), "the other connection is unchanged: {answer:?}");
    })
    .await
}

/// `SHOW` of a session parameter answers outside a block as pgwire answers
/// a new session; a `SET` outside a block is still refused, since an
/// autocommit native statement has no session to keep it in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_answers_session_parameters_in_autocommit() {
    bounded(async {
        let h = Harness::start().await;
        let client = h.client().await;
        let show = |what: &'static str| {
            let client = &client;
            async move {
                client
                    .fetch_scalar::<String>(what, &[])
                    .await
                    .unwrap_or_else(|e| panic!("{what}: {e}"))
            }
        };
        assert_eq!(show("SHOW search_path").await, "\"$user\", public");
        assert_eq!(show("show SEARCH_PATH").await, "\"$user\", public");
        assert_eq!(show("SHOW TimeZone").await, "UTC");
        assert_eq!(show("SHOW transaction_isolation").await, "read committed");
        assert!(!show("SHOW server_version").await.is_empty());

        let unknown = client.run("SHOW no_such_parameter").await.unwrap_err();
        assert!(
            unknown.to_string().contains("unrecognized configuration parameter"),
            "{unknown}"
        );
        assert!(client.run("SET search_path = public").await.is_err());

        // Inside a block a SET is the block's, and SHOW reads it.
        let tx = client.begin().await.unwrap();
        tx.run("SET search_path = public").await.unwrap();
        assert_eq!(tx.fetch_scalar::<String>("SHOW search_path", &[]).await.unwrap(), "public");
        tx.rollback().await.unwrap();
        assert_eq!(show("SHOW search_path").await, "\"$user\", public");
    })
    .await
}

/// A transaction holding row 1 of `t`, kept busy (the server closes an idle
/// connection at the request timeout, which would roll it back) until
/// `release` fires, then committed.
async fn hold_row(client: &Client) -> (tokio::sync::oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let tx = client.begin().await.unwrap();
    tx.run("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();
    let (release, mut released) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut released => break,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    tx.run("SELECT 1").await.expect("the holder stays busy");
                }
            }
        }
        tx.commit().await.expect("the holder commits");
    });
    (release, task)
}

async fn table_t(client: &Client) {
    client
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client.run("INSERT INTO t (id, v) VALUES (1, 0)").await.unwrap();
}

/// A statement in a block that runs past the request timeout (here waiting
/// on a row another transaction holds) is stopped with 57014. The block is
/// failed (25P02 until it rolls back), and its connection is not lost: the
/// rollback is answered on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_past_the_timeout_fails_57014_and_keeps_its_block_connection() {
    bounded(async {
        let h = Harness::start().await;
        let client = h.client().await;
        table_t(&client).await;
        let (release, holder) = hold_row(&client).await;

        let tx = client.begin().await.unwrap();
        let started = Instant::now();
        let stopped = tx.run("UPDATE t SET v = 2 WHERE id = 1").await.unwrap_err();
        assert!(stopped.is_statement_timeout(), "expected 57014, got {stopped}");
        assert!(!stopped.is_transaction_lost(), "{stopped}");
        assert!(started.elapsed() >= REQUEST_TIMEOUT, "stopped early: {:?}", started.elapsed());
        let after = tx.run("SELECT 1").await.unwrap_err();
        assert_eq!(after.sqlstate(), Some("25P02"), "{after}");
        tx.rollback().await.expect("the rollback is answered on the same connection");

        let _ = release.send(());
        holder.await.unwrap();
        assert_eq!(client.fetch_scalar::<i64>("SELECT v FROM t WHERE id = 1", &[]).await.unwrap(), 1);
    })
    .await
}

/// `SET statement_timeout = 0` inside a block lifts the bound for it: a
/// statement there runs well past the request timeout and is answered, and
/// the block commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_can_run_a_statement_longer_than_the_request_timeout() {
    bounded(async {
        let h = Harness::start().await;
        let client = h.client().await;
        table_t(&client).await;
        let (release, holder) = hold_row(&client).await;

        let tx = client.begin().await.unwrap();
        tx.run("SET statement_timeout = 0").await.unwrap();
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(REQUEST_TIMEOUT * 2).await;
            let _ = release.send(());
        });
        let started = Instant::now();
        let updated = tx.execute("UPDATE t SET v = v + 10 WHERE id = 1", &[]).await;
        let took = started.elapsed();
        assert_eq!(updated.expect("the long statement is answered"), 1);
        assert!(took > REQUEST_TIMEOUT, "it did not outlive the old limit: {took:?}");
        tx.commit().await.expect("the block commits");
        releaser.await.unwrap();
        holder.await.unwrap();
        assert_eq!(client.fetch_scalar::<i64>("SELECT v FROM t WHERE id = 1", &[]).await.unwrap(), 11);
    })
    .await
}

/// Outside a block the same: 57014 (with its SQLSTATE, which a native
/// autocommit error otherwise lacks), on a multiplexed connection and on a
/// plain one, and each connection serves its next statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_autocommit_statement_past_the_timeout_fails_57014_and_keeps_its_connection() {
    bounded(async {
        let h = Harness::start().await;
        let client = h.client().await;
        table_t(&client).await;
        let (release, holder) = hold_row(&client).await;

        let stopped = client.run("UPDATE t SET v = 3 WHERE id = 1").await.unwrap_err();
        assert!(stopped.is_statement_timeout(), "expected 57014, got {stopped}");
        assert_eq!(client.fetch_scalar::<i64>("SELECT id FROM t", &[]).await.unwrap(), 1);

        let mut plain = h.raw().await;
        let answer = plain.send(b"UPDATE t SET v = 4 WHERE id = 1".to_vec()).await.unwrap();
        assert_eq!(answer.first(), Some(&3), "an error answer, got {answer:?}");
        assert!(
            answer.ends_with(b"(SQLSTATE 57014)"),
            "{:?}",
            String::from_utf8_lossy(&answer)
        );
        let answer = plain.send(b"SELECT id FROM t".to_vec()).await.expect("the plain connection goes on");
        assert_eq!(answer.first(), Some(&2));

        let _ = release.send(());
        holder.await.unwrap();
        assert_eq!(client.fetch_scalar::<i64>("SELECT v FROM t WHERE id = 1", &[]).await.unwrap(), 1);
    })
    .await
}
