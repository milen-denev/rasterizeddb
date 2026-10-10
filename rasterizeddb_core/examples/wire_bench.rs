//! End-to-end pgwire output benchmark over a real loopback socket.
//!
//! Starts the pgwire server in-process on an in-memory database, loads a table,
//! and times result-heavy and statement-heavy workloads from a minimal raw
//! protocol client (no driver overhead in the measurement):
//!
//! - `select_text`:   simple-query `SELECT *` of ROWS rows, text format
//! - `select_binary`: Parse/Bind(binary results)/Execute/Sync of the same
//! - `select_1`:      round trips of a trivial statement, simple protocol
//! - `pipelined`:     Parse/Bind/Execute/Sync of `SELECT 1`, round trips
//! - `scan control`:  the same scan returning no rows, to separate engine time
//!
//! Usage: `cargo run --release -p rasterizeddb_core --example wire_bench [rounds]`
//! `rounds` = 0 does the setup only, so `strace -c -f` runs with two round
//! counts can be subtracted to get syscalls per statement.

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
    pgwire::{AuthConfig, TlsConfig, TlsMode, start_pgwire_with_catalog},
};

const ROWS: usize = 10_000;

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(15);
    let _ = MAX_PERMITS_THREADS.set(4);
    let _ = BATCH_SIZE.set(10_000);
    let _ = ENABLE_CACHE.set(false);

    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
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
                let catalog = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME)
                    .await
                    .unwrap();
                let tls = TlsConfig {
                    mode: TlsMode::Disable,
                    cert_path: None,
                    key_path: None,
                };
                let auth = AuthConfig {
                    method: AuthMethod::Trust,
                    bootstrap_user: "rasterized".to_string(),
                    bootstrap_password: None,
                };
                let _ = start_pgwire_with_catalog(catalog, "127.0.0.1", port, tls, auth).await;
            });
        })
        .unwrap();

    let mut client = loop {
        if let Ok(c) = Client::connect(port) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    client.simple(
        "CREATE TABLE wb (id INT PRIMARY KEY, name TEXT, amount NUMERIC(12,2), ts TIMESTAMP, flag BOOLEAN)",
    );
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1000) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| {
                format!(
                    "({i}, 'name-{i}-é', {}.{:02}, '2026-0{}-1{} 12:34:56.{:06}', {})",
                    i * 7,
                    i % 100,
                    1 + i % 9,
                    i % 10,
                    i * 37 % 1_000_000,
                    i % 2 == 0
                )
            })
            .collect();
        client.simple(&format!(
            "INSERT INTO wb (id, name, amount, ts, flag) VALUES {}",
            values.join(",")
        ));
    }
    let (rows, _) = client.simple("SELECT * FROM wb");
    assert_eq!(rows, ROWS, "the table must hold every row");
    if rounds == 0 {
        return;
    }

    let mut text = Vec::new();
    let mut binary = Vec::new();
    let mut select_1 = Vec::new();
    let mut pipelined = Vec::new();
    let mut control = Vec::new();
    for _ in 0..rounds {
        // The same scan with nothing to send: what the engine costs alone.
        let t = Instant::now();
        let (n, _) = client.simple("SELECT * FROM wb WHERE name = 'nope'");
        control.push(t.elapsed());
        assert_eq!(n, 0);

        let t = Instant::now();
        let (n, _) = client.simple("SELECT * FROM wb");
        text.push(t.elapsed());
        assert_eq!(n, ROWS);

        let t = Instant::now();
        let n = client.extended("SELECT * FROM wb", 1);
        binary.push(t.elapsed());
        assert_eq!(n, ROWS);

        let t = Instant::now();
        for _ in 0..200 {
            client.simple("SELECT 1");
        }
        select_1.push(t.elapsed() / 200);

        let t = Instant::now();
        for _ in 0..200 {
            client.extended("SELECT 1", 0);
        }
        pipelined.push(t.elapsed() / 200);
    }
    report("scan control (0 rows)  ", &mut control);
    report("select_text  (10k rows)", &mut text);
    report("select_binary(10k rows)", &mut binary);
    report("select_1 simple  (per stmt)", &mut select_1);
    report("select_1 extended(per stmt)", &mut pipelined);
}

fn report(name: &str, samples: &mut [Duration]) {
    samples.sort();
    let median = samples[samples.len() / 2];
    let min = samples[0];
    println!("{name}: median {median:?}  min {min:?}  n={}", samples.len());
}

struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn connect(port: u16) -> std::io::Result<Self> {
        let socket = TcpStream::connect(("127.0.0.1", port))?;
        socket.set_nodelay(true)?;
        let mut client = Client {
            writer: socket.try_clone()?,
            reader: BufReader::with_capacity(64 * 1024, socket),
        };
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

    /// Reads to ReadyForQuery; returns DataRow count and whether an error came.
    fn until_ready(&mut self) -> (usize, bool) {
        let mut rows = 0;
        let mut error = false;
        loop {
            let (tag, body) = self.message();
            match tag {
                b'D' => rows += 1,
                b'E' => {
                    error = true;
                    eprintln!("server error: {}", String::from_utf8_lossy(&body));
                }
                b'Z' => return (rows, error),
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
        let out = self.until_ready();
        assert!(!out.1, "statement failed: {sql}");
        out
    }

    /// Parse/Bind/Execute/Sync in one write, unnamed statement and portal.
    fn extended(&mut self, sql: &str, result_format: i16) -> usize {
        let mut buf = Vec::new();
        let mut frame = |tag: u8, body: &[u8]| {
            buf.push(tag);
            buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            buf.extend_from_slice(body);
        };
        let mut parse = vec![0u8]; // unnamed statement
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0i16.to_be_bytes());
        frame(b'P', &parse);
        let mut bind = vec![0u8, 0u8]; // unnamed portal, unnamed statement
        bind.extend_from_slice(&0i16.to_be_bytes()); // parameter formats
        bind.extend_from_slice(&0i16.to_be_bytes()); // parameters
        bind.extend_from_slice(&1i16.to_be_bytes()); // one result format, for all
        bind.extend_from_slice(&result_format.to_be_bytes());
        frame(b'B', &bind);
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        frame(b'E', &execute);
        frame(b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        let (rows, error) = self.until_ready();
        assert!(!error, "statement failed: {sql}");
        rows
    }
}
