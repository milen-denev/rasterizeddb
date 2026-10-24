//! Per-execution cost of a learned prepared read, plain table against a
//! `PARTITION BY HASH` one (`docs/partitioning.md` §15, S16/S16b).
//!
//! Starts the pgwire server in-process over loopback on a database in a
//! temporary directory, loads partbench's shape (`accounts`, TENANTS ×
//! ROWS, PARTS partitions) twice, once plain and once partitioned, and runs
//! partbench's two exit-criterion reads as named prepared statements from a
//! raw protocol client: Bind/Execute/Sync per execution, text parameters, no
//! Describe, so every timed execution after the first two runs from the
//! learned parse.
//!
//! - `point`:  `SELECT balance FROM t WHERE tenant_id = $1 AND id = $2`
//! - `tcount`: `SELECT count(*) FROM t WHERE tenant_id = $1`
//!
//! Rounds interleave plain and partitioned; medians and the
//! partitioned/plain ratio are printed. Nothing is asserted about time: this
//! box drifts ~15% between identical runs (memory `how-to-profile-this-repo`).
//!
//! Usage: `cargo run --release -p rasterizeddb_core --example
//! partition_prepared_bench -- [key=value ...]` with keys `tenants` (1000),
//! `rows` (100), `parts` (64), `n` (20000 executions per client per run),
//! `rounds` (3), `clients` (1), `only` (`both` | `plain` | `part`) and
//! `workload` (`both` | `point` | `tcount`). `only` and `workload` narrow the
//! timed loop so one `perf record` covers one path.

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

struct Opts {
    tenants: u64,
    rows: u64,
    parts: u64,
    n: u64,
    rounds: usize,
    clients: usize,
    only: String,
    workload: String,
}

fn opts() -> Opts {
    let mut o = Opts {
        tenants: 1000,
        rows: 100,
        parts: 64,
        n: 20_000,
        rounds: 3,
        clients: 1,
        only: "both".into(),
        workload: "both".into(),
    };
    for arg in std::env::args().skip(1) {
        let Some((k, v)) = arg.split_once('=') else {
            panic!("arguments are key=value, got {arg}");
        };
        match k {
            "tenants" => o.tenants = v.parse().unwrap(),
            "rows" => o.rows = v.parse().unwrap(),
            "parts" => o.parts = v.parse().unwrap(),
            "n" => o.n = v.parse().unwrap(),
            "rounds" => o.rounds = v.parse().unwrap(),
            "clients" => o.clients = v.parse().unwrap(),
            "only" => o.only = v.to_string(),
            "workload" => o.workload = v.to_string(),
            _ => panic!("unknown argument {k}"),
        }
    }
    o
}

fn main() {
    let o = opts();
    let _ = MAX_PERMITS_THREADS.set(4);
    let _ = BATCH_SIZE.set(512);
    // The server's default: partbench runs against it with the cache on.
    let _ = ENABLE_CACHE.set(true);

    let dir = std::env::temp_dir().join(format!("rdb_partition_prepared_bench_{}", std::process::id()));
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
                    .worker_threads(8)
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

    let ddl = "(tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, balance BIGINT, PRIMARY KEY (tenant_id, id))";
    client.simple(&format!("CREATE TABLE acc_plain {ddl}"));
    client.simple(&format!("CREATE TABLE acc_part {ddl} PARTITION BY HASH (tenant_id) PARTITIONS {}", o.parts));
    for table in ["acc_plain", "acc_part"] {
        let all: Vec<(u64, u64)> = (1..=o.tenants).flat_map(|t| (1..=o.rows).map(move |i| (t, i))).collect();
        for chunk in all.chunks(1000) {
            let values: Vec<String> = chunk.iter().map(|(t, i)| format!("({t}, {i}, {})", t * 1000 + i)).collect();
            client.simple(&format!("INSERT INTO {table} (tenant_id, id, balance) VALUES {}", values.join(",")));
        }
        let n = client.simple(&format!("SELECT count(*) FROM {table}")).0;
        assert_eq!(n, 1);
    }
    drop(client);
    println!(
        "# tenants={} rows={} parts={} n={} rounds={} clients={} only={} workload={}",
        o.tenants, o.rows, o.parts, o.n, o.rounds, o.clients, o.only, o.workload
    );

    let tables: Vec<&str> = match o.only.as_str() {
        "plain" => vec!["acc_plain"],
        "part" => vec!["acc_part"],
        _ => vec!["acc_plain", "acc_part"],
    };
    let workloads: Vec<&str> = match o.workload.as_str() {
        "point" => vec!["point"],
        "tcount" => vec!["tcount"],
        _ => vec!["point", "tcount"],
    };
    let mut results: Vec<(String, String, Vec<f64>)> = Vec::new();
    for _ in 0..o.rounds {
        for w in &workloads {
            for table in &tables {
                let ops = run(port, table, w, &o);
                match results.iter_mut().find(|(t, ww, _)| t == table && ww == w) {
                    Some((_, _, v)) => v.push(ops),
                    None => results.push((table.to_string(), w.to_string(), vec![ops])),
                }
            }
        }
    }
    let median = |v: &[f64]| {
        let mut v = v.to_vec();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    for w in &workloads {
        let mut plain = None;
        for (table, ww, v) in &results {
            if ww != w {
                continue;
            }
            let m = median(v);
            let runs: Vec<String> = v.iter().map(|x| format!("{x:.0}")).collect();
            println!(
                "{w:6} {table:9} c={} median {m:8.0} ops/s  {:6.2} us/op  runs [{}]",
                o.clients,
                1e6 * o.clients as f64 / m,
                runs.join(", ")
            );
            if table == "acc_plain" {
                plain = Some(m);
            } else if let Some(p) = plain {
                println!("{w:6} ratio partitioned/plain c={}: {:.3}", o.clients, m / p);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    rasterizeddb_core::core::wal::remove_for(&root);
}

/// `o.clients` connections each running `o.n` executions of `workload` on
/// `table`; ops/s over all of them.
fn run(port: u16, table: &str, workload: &str, o: &Opts) -> f64 {
    let sql = match workload {
        "point" => format!("SELECT balance FROM {table} WHERE tenant_id = $1 AND id = $2"),
        _ => format!("SELECT count(*) FROM {table} WHERE tenant_id = $1"),
    };
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(o.clients + 1));
    let handles: Vec<_> = (0..o.clients)
        .map(|c| {
            let (sql, barrier) = (sql.clone(), barrier.clone());
            let (tenants, rows, n, point) = (o.tenants, o.rows, o.n, workload == "point");
            std::thread::spawn(move || {
                let mut client = Client::connect(port).unwrap();
                client.prepare("s", &sql);
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (c as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407);
                let mut next = move || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng
                };
                let mut one = |client: &mut Client| {
                    let t = 1 + next() % tenants;
                    if point {
                        let i = 1 + next() % rows;
                        let got = client.execute("s", &[&t.to_string(), &i.to_string()]);
                        assert_eq!(got, 1, "point ({t}, {i})");
                    } else {
                        let got = client.execute("s", &[&t.to_string()]);
                        assert_eq!(got, 1, "tcount {t}");
                    }
                };
                // The two learning executions, and warm caches.
                for _ in 0..200 {
                    one(&mut client);
                }
                barrier.wait();
                for _ in 0..n {
                    one(&mut client);
                }
            })
        })
        .collect();
    barrier.wait();
    let started = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    (o.n * o.clients as u64) as f64 / started.elapsed().as_secs_f64()
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

    /// Parse `sql` as statement `name` (untyped parameters), Sync.
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

    /// Bind text `params` to `name`, Execute the unnamed portal, Sync, in one
    /// write: the DataRow count.
    fn execute(&mut self, name: &str, params: &[&str]) -> usize {
        let mut buf = Vec::with_capacity(128);
        let mut bind = Vec::with_capacity(64);
        bind.push(0); // unnamed portal
        bind.extend_from_slice(name.as_bytes());
        bind.push(0);
        bind.extend_from_slice(&0i16.to_be_bytes()); // all parameters text
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            bind.extend_from_slice(&(p.len() as i32).to_be_bytes());
            bind.extend_from_slice(p.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes()); // results text
        Self::frame(&mut buf, b'B', &bind);
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        Self::frame(&mut buf, b'E', &execute);
        Self::frame(&mut buf, b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready().0
    }
}
