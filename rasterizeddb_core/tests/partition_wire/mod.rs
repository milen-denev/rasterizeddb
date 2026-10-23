//! A pgwire server over one on-disk database, and a small blocking client
//! that speaks the simple and the extended protocol, for the partitioning
//! tests that need what only the wire layer does: sessions (`BEGIN` …
//! `ROLLBACK`), `$n` parameters, multi-row `INSERT` splitting, `ON CONFLICT`,
//! `RETURNING`, `COPY`, `FOR UPDATE`, and the SQLSTATE a refusal maps to.
//!
//! Shared by `partition_twin.rs` and `partition_shell_sites.rs`; not a test
//! target of its own (a directory module).

#![allow(dead_code)]

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    auth::AuthMethod,
    core::database::{Database, catalog::DatabaseCatalog},
    pgwire::{AuthConfig, TlsConfig, TlsMode, start_pgwire_with_catalog},
};
use rclite::Arc;

pub fn init_globals() {
    let _ = MAX_PERMITS_THREADS.set(4);
    let _ = BATCH_SIZE.set(1_000);
}

/// A pgwire server on its own thread and runtime (32 MiB stacks, as the
/// binary has), serving one database opened from `dir`. Dropping it, or
/// [`Server::stop`], ends the server and releases the database, so the same
/// directory can be opened again (a restart).
pub struct Server {
    pub port: u16,
    pub db: Arc<Database>,
    pub dir: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    pub fn start(dir: &str) -> Server {
        init_globals();
        let port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
            probe.local_addr().unwrap().port()
        };
        let (db_tx, db_rx) = std::sync::mpsc::channel::<Arc<Database>>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let owned_dir = dir.to_string();
        let handle = std::thread::Builder::new()
            .name("partition-test-server".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("server runtime");
                rt.block_on(async move {
                    let db = Arc::new(Database::new_with_backend(&owned_dir, false).await);
                    db_tx.send(db.clone()).expect("hand the database back");
                    let catalog = DatabaseCatalog::single(db);
                    let tls = TlsConfig { mode: TlsMode::Disable, cert_path: None, key_path: None };
                    let auth = AuthConfig {
                        method: AuthMethod::Trust,
                        bootstrap_user: "rasterized".to_string(),
                        bootstrap_password: None,
                    };
                    tokio::select! {
                        _ = start_pgwire_with_catalog(catalog, "127.0.0.1", port, tls, auth) => {}
                        _ = shutdown_rx => {}
                    }
                });
                rt.shutdown_timeout(Duration::from_secs(5));
            })
            .expect("spawn the server thread");
        let db = db_rx.recv_timeout(Duration::from_secs(120)).expect("the database opened");
        let server = Server { port, db, dir: dir.to_string(), shutdown: Some(shutdown_tx), handle: Some(handle) };
        // Wait until it accepts.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "the server never accepted");
            std::thread::sleep(Duration::from_millis(20));
        }
        server
    }

    pub fn connect(&self) -> Client {
        Client::connect(self.port, &self.db.name)
    }

    /// Ends the server and releases the database: every client must have
    /// been dropped first, or its session keeps the database alive.
    pub fn stop(mut self) {
        self.stop_inner();
    }

    fn stop_inner(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        rasterizeddb_core::core::wal::remove_for(&self.dir);
    }

    /// Stops this server and opens the same directory again.
    pub fn restart(self) -> Server {
        let dir = self.dir.clone();
        // Dropping the server stops its runtime (and with it every session's
        // handle) and then drops ours.
        drop(self);
        Server::start(&dir)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

/// A server's error: SQLSTATE and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    pub code: String,
    pub message: String,
}

impl WireError {
    /// A refusal of something not built yet: 0A000 and "not supported".
    pub fn is_unsupported(&self) -> bool {
        self.code == "0A000" && self.message.to_ascii_lowercase().contains("not supported")
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// What one statement (or one simple-query string) answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reply {
    /// Column names of the last row description.
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Every CommandComplete tag, in order.
    pub tags: Vec<String>,
    /// CopyOut data lines.
    pub copy_out: Vec<String>,
}

pub type Outcome = Result<Reply, WireError>;

pub struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    /// The transaction status of the last ReadyForQuery: I, T or E.
    pub status: u8,
}

fn frame(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
    buf.push(tag);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(body);
}

fn cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

fn error_fields(body: &[u8]) -> WireError {
    let mut code = String::new();
    let mut message = String::new();
    let mut at = 0;
    while at < body.len() && body[at] != 0 {
        let kind = body[at];
        at += 1;
        let end = at + body[at..].iter().position(|b| *b == 0).unwrap_or(body.len() - at);
        let value = String::from_utf8_lossy(&body[at..end]).into_owned();
        at = end + 1;
        match kind {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    WireError { code, message }
}

impl Client {
    pub fn connect(port: u16, database: &str) -> Client {
        let socket = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        socket.set_nodelay(true).unwrap();
        // A statement that hangs fails the test instead of wedging it.
        socket.set_read_timeout(Some(Duration::from_secs(300))).unwrap();
        let mut client = Client {
            writer: socket.try_clone().unwrap(),
            reader: BufReader::with_capacity(64 * 1024, socket),
            status: b'I',
        };
        let mut params = Vec::new();
        for (k, v) in [("user", "rasterized"), ("database", database)] {
            cstr(&mut params, k);
            cstr(&mut params, v);
        }
        params.push(0);
        let mut startup = Vec::new();
        startup.extend_from_slice(&((params.len() + 8) as i32).to_be_bytes());
        startup.extend_from_slice(&196_608i32.to_be_bytes());
        startup.extend_from_slice(&params);
        client.writer.write_all(&startup).unwrap();
        client.until_ready().expect("startup");
        client
    }

    fn message(&mut self) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        self.reader.read_exact(&mut head).expect("read a message header (server gone or hung?)");
        let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
        let mut body = vec![0u8; len];
        self.reader.read_exact(&mut body).expect("read a message body");
        (head[0], body)
    }

    /// Reads to ReadyForQuery. The first error is the outcome.
    fn until_ready(&mut self) -> Outcome {
        let mut reply = Reply::default();
        let mut error: Option<WireError> = None;
        loop {
            let (tag, body) = self.message();
            match tag {
                b'T' => {
                    reply.columns.clear();
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut at = 2;
                    for _ in 0..n {
                        let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                        reply.columns.push(String::from_utf8_lossy(&body[at..end]).into_owned());
                        at = end + 1 + 18;
                    }
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
                            let len = len as usize;
                            row.push(Some(String::from_utf8_lossy(&body[at..at + len]).into_owned()));
                            at += len;
                        }
                    }
                    reply.rows.push(row);
                }
                b'C' => {
                    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
                    reply.tags.push(String::from_utf8_lossy(&body[..end]).into_owned());
                }
                b'E' => {
                    if error.is_none() {
                        error = Some(error_fields(&body));
                    }
                }
                b'd' => reply.copy_out.push(String::from_utf8_lossy(&body).into_owned()),
                b'G' => {
                    // CopyInResponse with nothing to send: fail the copy so the
                    // exchange ends; the caller wanted copy_in for data.
                    let mut buf = Vec::new();
                    let mut msg = Vec::new();
                    cstr(&mut msg, "no data from this client");
                    frame(&mut buf, b'f', &msg);
                    self.writer.write_all(&buf).unwrap();
                }
                b'Z' => {
                    self.status = body.first().copied().unwrap_or(b'I');
                    return match error {
                        Some(e) => Err(e),
                        None => Ok(reply),
                    };
                }
                _ => {}
            }
        }
    }

    /// A simple query.
    pub fn simple(&mut self, sql: &str) -> Outcome {
        let mut body = Vec::new();
        cstr(&mut body, sql);
        let mut buf = Vec::new();
        frame(&mut buf, b'Q', &body);
        self.writer.write_all(&buf).unwrap();
        self.until_ready()
    }

    /// A simple query that must succeed.
    #[track_caller]
    pub fn ok(&mut self, sql: &str) -> Reply {
        match self.simple(sql) {
            Ok(r) => r,
            Err(e) => panic!("`{sql}` failed: {e}"),
        }
    }

    /// `COPY … FROM STDIN` with `data` as the stream.
    pub fn copy_in(&mut self, sql: &str, data: &str) -> Outcome {
        let mut body = Vec::new();
        cstr(&mut body, sql);
        let mut buf = Vec::new();
        frame(&mut buf, b'Q', &body);
        self.writer.write_all(&buf).unwrap();
        let mut reply = Reply::default();
        loop {
            let (tag, body) = self.message();
            match tag {
                b'G' => {
                    let mut buf = Vec::new();
                    frame(&mut buf, b'd', data.as_bytes());
                    frame(&mut buf, b'c', &[]);
                    self.writer.write_all(&buf).unwrap();
                    let rest = self.until_ready();
                    return rest.map(|mut r| {
                        r.tags.splice(0..0, reply.tags.drain(..));
                        r
                    });
                }
                b'E' => {
                    let e = error_fields(&body);
                    // Drain to ReadyForQuery.
                    let _ = self.until_ready();
                    return Err(e);
                }
                b'C' => {
                    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
                    reply.tags.push(String::from_utf8_lossy(&body[..end]).into_owned());
                }
                b'Z' => {
                    self.status = body.first().copied().unwrap_or(b'I');
                    return Ok(reply);
                }
                _ => {}
            }
        }
    }

    /// Parse a named statement (parameter types left to the server).
    pub fn prepare(&mut self, name: &str, sql: &str) -> Outcome {
        let mut buf = Vec::new();
        let mut parse = Vec::new();
        cstr(&mut parse, name);
        cstr(&mut parse, sql);
        parse.extend_from_slice(&0i16.to_be_bytes());
        frame(&mut buf, b'P', &parse);
        let mut describe = vec![b'S'];
        cstr(&mut describe, name);
        frame(&mut buf, b'D', &describe);
        frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready()
    }

    /// Bind `params` (text, `None` is NULL) to a named statement, describe and
    /// execute the unnamed portal, sync.
    pub fn execute_prepared(&mut self, name: &str, params: &[Option<String>]) -> Outcome {
        let mut buf = Vec::new();
        let mut bind = Vec::new();
        cstr(&mut bind, "");
        cstr(&mut bind, name);
        bind.extend_from_slice(&0i16.to_be_bytes()); // all text
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            match p {
                None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(v) => {
                    bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    bind.extend_from_slice(v.as_bytes());
                }
            }
        }
        bind.extend_from_slice(&0i16.to_be_bytes()); // results all text
        frame(&mut buf, b'B', &bind);
        let mut describe = vec![b'P'];
        cstr(&mut describe, "");
        frame(&mut buf, b'D', &describe);
        let mut execute = Vec::new();
        cstr(&mut execute, "");
        execute.extend_from_slice(&0i32.to_be_bytes());
        frame(&mut buf, b'E', &execute);
        frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready()
    }

    /// Close a named statement.
    pub fn close_prepared(&mut self, name: &str) {
        let mut buf = Vec::new();
        let mut close = vec![b'S'];
        cstr(&mut close, name);
        frame(&mut buf, b'C', &close);
        frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        let _ = self.until_ready();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let mut buf = Vec::new();
        frame(&mut buf, b'X', &[]);
        let _ = self.writer.write_all(&buf);
        let _ = self.writer.shutdown(std::net::Shutdown::Both);
    }
}

/// Files in `dir` whose names start with `prefix`, sorted.
pub fn files_starting(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .expect("read the database directory")
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with(prefix))
        .collect();
    out.sort();
    out
}

/// Files a plain table called `name` would have in `dir`: `<name>.db` and
/// `<name>_<part>.db`, with their `.tmp` companions. Not every file starting
/// with `name`: `site_p` is a prefix of `site_parent`. (A table named
/// `<name>_x` would match too; the tests make none.)
pub fn files_of_table(dir: &std::path::Path, name: &str) -> Vec<String> {
    files_starting(dir, name)
        .into_iter()
        .filter(|n| matches!(n.as_bytes().get(name.len()), Some(b'.') | Some(b'_')))
        .collect()
}

/// Runs `body` to completion on a multi-threaded runtime of its own, on a
/// thread with the engine's 32 MiB stack.
pub fn block_on_big<T: Send + 'static>(body: impl std::future::Future<Output = T> + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a runtime")
                .block_on(body)
        })
        .expect("a thread")
        .join()
        .expect("the thread panicked")
}

/// Writes rows straight into a partitioned table's partitions, under the
/// frozen transaction (visible to every snapshot), the way the catalog tests
/// do: `INSERT` statements are refused until S9, and a twin whose two sides
/// are both empty cannot tell an empty answer from a right one. `inserts`
/// are whole `INSERT INTO <table> (…) VALUES (…)` statements, one row each.
pub fn seed_partitioned(db: &Arc<Database>, table: &str, inserts: Vec<String>) {
    use rasterizeddb_core::core::mvcc::transaction::FROZEN_TX;
    use rasterizeddb_core::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
    let pt = db.partitioned.get(table).expect("a partitioned table").value().clone();
    let name = table.to_string();
    block_on_big(async move {
        for query in inserts {
            let qp = QueryPurpose::InsertRow(InsertRowData {
                table_name: name.clone(),
                query: query.clone(),
                on_conflict_do_nothing: false,
                on_conflict_update: None,
            });
            let row = rasterizeddb_core::core::rql::lexer_ir::insert_row_with_defaults(
                &qp,
                &pt.shell().schema.fields,
                &[],
                &[],
            )
            .unwrap_or_else(|| panic!("cannot build a row from {query}"));
            pt.insert_row_in_txn(row, FROZEN_TX).await.unwrap_or_else(|e| panic!("{query}: {e}"));
        }
    });
}
