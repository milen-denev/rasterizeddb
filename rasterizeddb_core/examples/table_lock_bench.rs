//! Per-statement cost of the table-lock path (B3b's perf gate,
//! `docs/persistent-indexes.md` §7 "Cost and behaviour", finding L12).
//!
//! Starts the pgwire server in-process over loopback on a database in a
//! temporary directory, loads one plain table `acc (id BIGINT PRIMARY KEY,
//! balance BIGINT)` with `rows` rows, then runs ONE workload `n` times from
//! a raw protocol client:
//!
//! - `select`:  prepared `SELECT balance FROM acc WHERE id = $1`
//!   (Bind/Execute/Sync; learned parse after two runs)
//! - `update`:  prepared `UPDATE acc SET balance = $1 WHERE id = $2`
//! - `insert`:  prepared `INSERT INTO ins (id, v) VALUES ($1, $2)`, a fresh
//!   bigint key each time
//! - `cte`:     simple `WITH x AS (SELECT balance FROM acc WHERE id = K)
//!   SELECT balance FROM x`
//! - `derived`: simple `SELECT d.balance FROM (SELECT balance FROM acc WHERE
//!   id = K) d`
//! - `catalog`: simple `SELECT relname FROM pg_class WHERE relname = 'acc'`
//!
//! It prints the wall-clock p50/p99 per statement, which on this shared box
//! is indicative only. The gate's number is **instructions per statement**:
//! run the binary under `perf stat -e instructions:u` at two values of `n`
//! and divide the difference by the difference in `n`, which cancels the
//! start-up and the load (`.claude/reviews/b3b-perf/` holds the driver).
//!
//! Usage: `table_lock_bench workload=<w> n=<count> [rows=<count>]`.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    auth::AuthMethod,
    core::{
        database::{DEFAULT_DATABASE_NAME, catalog::DatabaseCatalog},
        processor::concurrent_processor::ENABLE_CACHE,
    },
    pgwire::{AuthConfig, ConnectionLimits, TlsConfig, TlsMode, start_pgwire_on_listener},
};

fn main() {
    let mut workload = "select".to_string();
    let mut n: u64 = 20_000;
    let mut rows: u64 = 10_000;
    for arg in std::env::args().skip(1) {
        let (k, v) = arg.split_once('=').expect("arguments are key=value");
        match k {
            "workload" => workload = v.to_string(),
            "n" => n = v.parse().unwrap(),
            "rows" => rows = v.parse().unwrap(),
            _ => panic!("unknown argument {k}"),
        }
    }
    let _ = MAX_PERMITS_THREADS.set(4);
    let _ = BATCH_SIZE.set(512);
    let _ = ENABLE_CACHE.set(true);

    let dir = std::env::temp_dir().join(format!("rdb_table_lock_bench_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.to_string_lossy().into_owned();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    {
        let root = root.clone();
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let catalog = DatabaseCatalog::open(Some(&root), DEFAULT_DATABASE_NAME).await.unwrap();
                    let tls = TlsConfig { mode: TlsMode::Disable, cert_path: None, key_path: None };
                    let auth = AuthConfig {
                        method: AuthMethod::Trust,
                        bootstrap_user: "rasterized".to_string(),
                        bootstrap_password: None,
                    };
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let _ = start_pgwire_on_listener(catalog, listener, tls, auth, ConnectionLimits::default()).await;
                });
            })
            .unwrap();
    }
    let mut client = loop {
        if let Ok(c) = Client::connect(port) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    client.simple("CREATE TABLE acc (id BIGINT PRIMARY KEY, balance BIGINT)");
    client.simple("CREATE TABLE ins (id BIGINT PRIMARY KEY, v BIGINT)");
    let all: Vec<u64> = (1..=rows).collect();
    for chunk in all.chunks(1000) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i * 10)).collect();
        client.simple(&format!("INSERT INTO acc (id, balance) VALUES {}", values.join(",")));
    }

    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut insert_key = 0u64;
    let prepared = match workload.as_str() {
        "select" => Some("SELECT balance FROM acc WHERE id = $1"),
        "update" => Some("UPDATE acc SET balance = $1 WHERE id = $2"),
        "insert" => Some("INSERT INTO ins (id, v) VALUES ($1, $2)"),
        "cte" | "derived" | "catalog" => None,
        other => panic!("unknown workload {other}"),
    };
    if let Some(sql) = prepared {
        client.prepare("s", sql);
    }
    let mut one = |client: &mut Client| {
        let k = 1 + next() % rows;
        match workload.as_str() {
            "select" => assert_eq!(client.execute("s", &[&k.to_string()]), 1),
            "update" => {
                client.execute("s", &[&(k * 7).to_string(), &k.to_string()]);
            }
            "insert" => {
                insert_key += 1;
                client.execute("s", &[&insert_key.to_string(), &k.to_string()]);
            }
            "cte" => {
                let got = client
                    .simple(&format!("WITH x AS (SELECT balance FROM acc WHERE id = {k}) SELECT balance FROM x"))
                    .0;
                assert_eq!(got, 1);
            }
            "derived" => {
                let got = client
                    .simple(&format!("SELECT d.balance FROM (SELECT balance FROM acc WHERE id = {k}) d"))
                    .0;
                assert_eq!(got, 1);
            }
            "catalog" => {
                let got = client.simple("SELECT relname FROM pg_class WHERE relname = 'acc'").0;
                assert_eq!(got, 1);
            }
            _ => unreachable!(),
        }
    };
    for _ in 0..200 {
        one(&mut client);
    }
    let mut lat = Vec::with_capacity(n as usize);
    let started = Instant::now();
    for _ in 0..n {
        let t = Instant::now();
        one(&mut client);
        lat.push(t.elapsed().as_nanos() as u64);
    }
    let total = started.elapsed();
    lat.sort_unstable();
    let pct = |p: f64| lat[((lat.len() as f64 * p) as usize).min(lat.len() - 1)] as f64 / 1000.0;
    println!(
        "{workload:8} n={n} rows={rows} {:8.0} ops/s  p50 {:7.1} us  p99 {:7.1} us",
        n as f64 / total.as_secs_f64(),
        pct(0.50),
        pct(0.99)
    );
    drop(client);
    let _ = std::fs::remove_dir_all(&dir);
    rasterizeddb_core::core::wal::remove_for(&root);
}

struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn connect(port: u16) -> std::io::Result<Self> {
        let socket = TcpStream::connect(("127.0.0.1", port))?;
        socket.set_nodelay(true)?;
        let mut client = Client { writer: socket.try_clone()?, reader: BufReader::with_capacity(64 * 1024, socket) };
        let mut params = Vec::new();
        for (k, v) in [("user", "rasterized"), ("database", DEFAULT_DATABASE_NAME)] {
            params.extend_from_slice(k.as_bytes());
            params.push(0);
            params.extend_from_slice(v.as_bytes());
            params.push(0);
        }
        params.push(0);
        let mut startup = Vec::new();
        startup.extend_from_slice(&((params.len() + 8) as i32).to_be_bytes());
        startup.extend_from_slice(&196_608i32.to_be_bytes());
        startup.extend_from_slice(&params);
        client.writer.write_all(&startup)?;
        client.until_ready();
        Ok(client)
    }

    fn message(&mut self) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        self.reader.read_exact(&mut head).expect("read header");
        let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
        let mut body = vec![0u8; len];
        self.reader.read_exact(&mut body).expect("read body");
        (head[0], body)
    }

    /// Reads to ReadyForQuery; the DataRow count. Panics on an error.
    fn until_ready(&mut self) -> (usize, bool) {
        let mut rows = 0;
        let mut error = None;
        loop {
            let (tag, body) = self.message();
            match tag {
                b'D' => rows += 1,
                b'E' => error = Some(String::from_utf8_lossy(&body).into_owned()),
                b'Z' => {
                    if let Some(e) = error {
                        panic!("server error: {e}");
                    }
                    return (rows, false);
                }
                _ => {}
            }
        }
    }

    fn simple(&mut self, sql: &str) -> (usize, bool) {
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.writer.write_all(&msg).unwrap();
        self.until_ready()
    }

    fn frame(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
        buf.push(tag);
        buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(body);
    }

    fn prepare(&mut self, name: &str, sql: &str) {
        let mut buf = Vec::new();
        let mut parse = Vec::new();
        parse.extend_from_slice(name.as_bytes());
        parse.push(0);
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0i16.to_be_bytes());
        Self::frame(&mut buf, b'P', &parse);
        Self::frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready();
    }

    fn execute(&mut self, name: &str, params: &[&str]) -> usize {
        let mut buf = Vec::with_capacity(128);
        let mut bind = Vec::with_capacity(64);
        bind.push(0);
        bind.extend_from_slice(name.as_bytes());
        bind.push(0);
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            bind.extend_from_slice(&(p.len() as i32).to_be_bytes());
            bind.extend_from_slice(p.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        Self::frame(&mut buf, b'B', &bind);
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        Self::frame(&mut buf, b'E', &execute);
        Self::frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready().0
    }
}
