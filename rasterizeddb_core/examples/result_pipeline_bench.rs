// Post-scan result pipeline benchmark: ORDER BY <expression> LIMIT, OFFSET
// paging, and the row decode every row-level step forces.
//
//   result_pipeline_bench write --dir /var/tmp/x --rows 1000000
//   result_pipeline_bench read  --dir /var/tmp/x --iters 5 [--only NAME]
//
// `write` creates `rp` and fills it through multi-row INSERTs with
// synchronous_commit off. `read` reopens the directory and times each shape,
// printing min/median wall clock and how far peak RSS grew. Build the binary
// before and after a change and interleave the runs: this box drifts ~15%.

use std::time::Instant;

use rclite::Arc;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

fn peak_rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            if let Some(kb) = rest.split_whitespace().next() {
                return kb.parse::<f64>().unwrap_or(0.0) / 1024.0;
            }
        }
    }
    0.0
}

/// End-to-end shapes for engine-wide changes (`Row`/`Column` layout): run each
/// with `--only` in its own process so its peak RSS is its own. `update_all`
/// rewrites every row, so it runs last, once, on a freshly written database.
const SUITE: &[(&str, &str)] = &[
    ("suite_scan_where", "SELECT * FROM rp WHERE entry_order < 50000"),
    (
        "suite_cte_500k",
        "WITH c AS (SELECT id, entry_order, label FROM rp WHERE entry_order < 50000) SELECT COUNT(*) FROM c WHERE entry_order >= 0",
    ),
    (
        "suite_window_sort",
        "SELECT id, entry_order, ROW_NUMBER() OVER (ORDER BY entry_order) AS rn FROM rp WHERE domain_id = 'd1'",
    ),
    (
        "suite_group_by",
        "SELECT domain_id, COUNT(*), SUM(entry_order), MAX(score) FROM rp GROUP BY domain_id",
    ),
    (
        "suite_join",
        "SELECT rp.id, rp_dim.name FROM rp JOIN rp_dim ON rp_dim.domain_id = rp.domain_id WHERE rp.entry_order < 20000",
    ),
    (
        "suite_subquery_in",
        "SELECT id, label FROM rp WHERE id IN (SELECT id FROM rp WHERE entry_order < 2000)",
    ),
    ("suite_update_all", "UPDATE rp SET score = score + 1"),
];

const QUERIES: &[(&str, &str)] = &[
    (
        "expr_limit_50",
        "SELECT * FROM rp ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST, entry_order ASC, id ASC LIMIT 50",
    ),
    (
        "expr_limit_5000",
        "SELECT * FROM rp ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST, entry_order ASC, id ASC LIMIT 5000",
    ),
    (
        "expr_limit_50_off_1000",
        "SELECT * FROM rp ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST, entry_order ASC, id ASC LIMIT 50 OFFSET 1000",
    ),
    (
        "expr_narrow_limit_50",
        "SELECT id FROM rp ORDER BY entry_order DESC NULLS LAST, id ASC LIMIT 50",
    ),
    (
        "expr_uncapped",
        "SELECT id, entry_order FROM rp ORDER BY entry_order DESC NULLS LAST, id ASC",
    ),
    // The scan's own sort over a stored integer column, uncapped.
    ("scan_sort_int_uncapped", "SELECT id, entry_order FROM rp ORDER BY entry_order DESC"),
    ("scan_sort_int_limit_5000", "SELECT id, entry_order FROM rp ORDER BY entry_order DESC LIMIT 5000"),
    ("offset_10000_limit_20", "SELECT * FROM rp LIMIT 20 OFFSET 10000"),
    ("offset_500000_limit_20", "SELECT id, entry_order FROM rp LIMIT 20 OFFSET 500000"),
    (
        "ordered_offset_10000_limit_20",
        "SELECT * FROM rp ORDER BY entry_order ASC LIMIT 20 OFFSET 10000",
    ),
    // Every row through the per-row loop: a computed item, and a computed
    // item plus DISTINCT. Run each with `--only` in its own process to read
    // its peak RSS.
    ("expr_slot_all", "SELECT id, entry_order + 1 AS e, label FROM rp"),
    ("expr_slot_distinct_page", "SELECT DISTINCT domain_id, entry_order + 1 AS e FROM rp LIMIT 100"),
    // CTEs: each materialises into a memory-backed temp relation that the
    // outer query then scans with every worker.
    (
        "cte_scan",
        "WITH c AS (SELECT id, entry_order, label FROM rp WHERE entry_order < 50000) SELECT COUNT(*) FROM c WHERE entry_order >= 0",
    ),
    (
        "cte_two",
        "WITH a AS (SELECT id, entry_order FROM rp WHERE entry_order < 30000), b AS (SELECT id FROM a WHERE entry_order >= 1000) SELECT COUNT(*) FROM b WHERE id >= 0",
    ),
    // A literal forces every row through `vec_into_rows`.
    ("decode_all_literal", "SELECT id, entry_order, 1 FROM rp"),
];

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut mode = String::new();
    let mut dir = String::new();
    let mut rows = 1_000_000usize;
    let mut iters = 5usize;
    let mut only: Option<String> = None;
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "write" | "read" | "decode" | "wire" | "sortkeys" => mode = argv[i].clone(),
            "--dir" => {
                i += 1;
                dir = argv[i].clone()
            }
            "--rows" => {
                i += 1;
                rows = argv[i].parse().unwrap()
            }
            "--iters" => {
                i += 1;
                iters = argv[i].parse().unwrap()
            }
            "--only" => {
                i += 1;
                only = Some(argv[i].clone())
            }
            other => panic!("unknown argument {other}"),
        }
        i += 1;
    }
    assert!(!dir.is_empty(), "--dir is required");

    let _ = MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = BATCH_SIZE.get_or_init(|| 4096);
    let _ = ENABLE_CACHE.get_or_init(|| false);
    rasterizeddb_core::set_synchronous_commit(false);

    if mode == "decode" {
        decode_bench(rows, iters);
        return;
    }
    if mode == "sortkeys" {
        sortkeys_bench(rows, iters);
        return;
    }
    if mode == "wire" {
        wire_bench(&dir, rows, iters, only.as_deref());
        return;
    }

    let db = Arc::new(Database::new(&dir).await);

    if mode == "write" {
        let r = exec(
            &db,
            "CREATE TABLE rp (id BIGINT NOT NULL, entry_order BIGINT NOT NULL, domain_id TEXT NOT NULL, label TEXT NOT NULL, score DOUBLE NOT NULL, data JSONB)",
        )
        .await;
        if r.first() == Some(&3u8) {
            panic!("create failed: {}", String::from_utf8_lossy(&r[1..]));
        }
        let t0 = Instant::now();
        let chunk = 1000;
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for start in (0..rows).step_by(chunk) {
            let mut sql = String::from(
                "INSERT INTO rp (id, entry_order, domain_id, label, score, data) VALUES ",
            );
            for id in start..(start + chunk).min(rows) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let day = state % 400;
                // One row in eight has an empty date and one in sixteen none.
                let doc = match state % 16 {
                    0 => r#"{"1": "x"}"#.to_string(),
                    1 | 2 => r#"{"1": "x", "2": ""}"#.to_string(),
                    _ => format!(
                        r#"{{"1": "x", "2": "2026-{:02}-{:02}"}}"#,
                        1 + (day % 12),
                        1 + (day % 28)
                    ),
                };
                if id > start {
                    sql.push(',');
                }
                sql.push_str(&format!(
                    "({}, {}, 'd{}', 'label number {}', {}, '{}')",
                    id,
                    (state >> 20) % 100_000,
                    id % 4,
                    id,
                    (id % 977) as f64 * 1.25,
                    doc
                ));
            }
            let r = exec(&db, &sql).await;
            if r.first() == Some(&3u8) {
                panic!("insert failed: {}", String::from_utf8_lossy(&r[1..]));
            }
        }
        for sql in [
            "CREATE TABLE rp_dim (domain_id TEXT NOT NULL, name TEXT NOT NULL)",
            "INSERT INTO rp_dim (domain_id, name) VALUES ('d0', 'zero'), ('d1', 'one'), ('d2', 'two'), ('d3', 'three')",
        ] {
            let r = exec(&db, sql).await;
            if r.first() == Some(&3u8) {
                panic!("{sql}: {}", String::from_utf8_lossy(&r[1..]));
            }
        }
        eprintln!("wrote {rows} rows in {:.2?}", t0.elapsed());
        return;
    }

    for (name, sql) in QUERIES.iter().chain(SUITE.iter()) {
        if let Some(o) = &only {
            if !name.contains(o.as_str()) {
                continue;
            }
        }
        let mut times = Vec::with_capacity(iters);
        let mut bytes = 0usize;
        let before = peak_rss_mib();
        for _ in 0..iters {
            let t = Instant::now();
            let r = exec_on_worker(&db, sql).await;
            times.push(t.elapsed());
            if r.first() == Some(&3u8) {
                println!("{name} failed: {}", String::from_utf8_lossy(&r[1..]));
                break;
            }
            bytes = r.len();
        }
        if times.len() < iters {
            continue;
        }
        times.sort();
        println!(
            "{name:<30} min {:>9.2?}  median {:>9.2?}  peak +{:>7.1} MiB  {} result bytes",
            times[0],
            times[times.len() / 2],
            peak_rss_mib() - before,
            bytes
        );
    }
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    executor::execute(purpose, db.clone()).await
}

/// The server runs statements on runtime workers, never on the main thread.
async fn exec_on_worker(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let db = db.clone();
    let sql = sql.to_string();
    tokio::spawn(async move {
        // A `WITH` is materialised into temporary relations first, as pgwire
        // does, then the rewritten statement runs and the temps are dropped.
        use rasterizeddb_core::core::{mvcc::transaction::FROZEN_TX, rql::cte};
        if cte::has_with_clause(&sql) {
            let m = cte::materialize(&sql, &db, FROZEN_TX)
                .await
                .expect("materialise")
                .expect("a WITH clause");
            let purpose = recognize_query_purpose(&m.rewritten).expect("recognize");
            let out = executor::execute(purpose, db.clone()).await;
            cte::drop_temps(&db, &m.temps, FROZEN_TX).await;
            return out;
        }
        let purpose = recognize_query_purpose(&sql).expect("recognize");
        executor::execute(purpose, db).await
    })
    .await
    .expect("query task")
}

/// `vec_into_rows` sized up front against the same decode pushed into a vector
/// that grows by doubling. `decode` mode; `--dir` is ignored.
fn decode_bench(rows: usize, iters: usize) {
    use rasterizeddb_core::core::db_type::DbType;
    use rasterizeddb_core::core::row::row::{
        Column, Row, rows_into_vec, serialized_rows, vec_into_rows,
    };
    use rasterizeddb_core::memory_pool::MEMORY_POOL;

    for cols in [2usize, 6] {
        let mut built = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut row = Row::default();
            for c in 0..cols {
                let bytes = ((r * 7 + c) as u64).to_le_bytes();
                let mut data = MEMORY_POOL.acquire(8);
                data.into_slice_mut().copy_from_slice(&bytes);
                row.columns.push(Column {
                    schema_id: rasterizeddb_core::core::row::row::schema_id_of(c as u64),
                    data,
                    column_type: DbType::I64,
                });
            }
            built.push(row);
        }
        let bytes = rows_into_vec(built);
        let mut presized = Vec::new();
        let mut growing = Vec::new();
        for _ in 0..iters {
            let t = Instant::now();
            let v = vec_into_rows(&bytes).unwrap();
            presized.push(t.elapsed());
            drop(v);
            let t = Instant::now();
            let mut v: Vec<Row> = Vec::new();
            for span in serialized_rows(&bytes) {
                v.push(Row::decode(span.unwrap()).unwrap());
            }
            growing.push(t.elapsed());
            drop(v);
        }
        presized.sort();
        growing.sort();
        println!(
            "decode {rows} rows x {cols} cols: presized min {:>9.2?} median {:>9.2?} | growing min {:>9.2?} median {:>9.2?}",
            presized[0],
            presized[iters / 2],
            growing[0],
            growing[iters / 2]
        );
    }
}

/// A million-row SELECT through the pgwire server, in process, over loopback
/// — `wire` mode. With `--dir` naming a directory `write` mode filled, the
/// server opens it as its catalogue root and `rp` is read from disk; with
/// `--dir mem` the run loads an in-memory table itself (slow to scan: the
/// in-memory provider dominates the time). `--only text|binary` runs one
/// shape, so its peak RSS is its own.
fn wire_bench(dir: &str, rows: usize, iters: usize, only: Option<&str>) {
    use rasterizeddb_core::auth::AuthMethod;
    use rasterizeddb_core::core::database::{DEFAULT_DATABASE_NAME, catalog::DatabaseCatalog};
    use rasterizeddb_core::pgwire::{AuthConfig, TlsConfig, TlsMode, start_pgwire_with_catalog};
    use std::time::Duration;

    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
    let dir = dir.to_string();
    let on_disk = dir != "mem";
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
                let root = (dir != "mem").then_some(dir.as_str());
                let catalog = DatabaseCatalog::open(root, DEFAULT_DATABASE_NAME).await.unwrap();
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
        if let Ok(c) = WireClient::connect(port) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let sql = if on_disk {
        "SELECT id, entry_order, domain_id, label, score FROM rp"
    } else {
        "SELECT id, entry_order, label, amount, note FROM rp"
    };
    if !on_disk {
    client.simple(
        "CREATE TABLE rp (id BIGINT NOT NULL, entry_order BIGINT NOT NULL, label TEXT NOT NULL, amount NUMERIC(12,2), note TEXT)",
    );
    for start in (0..rows).step_by(1000) {
        let values: Vec<String> = (start..(start + 1000).min(rows))
            .map(|i| {
                let note = if i % 3 == 0 { "NULL".to_string() } else { format!("'n{i}'") };
                format!("({i}, {}, 'label number {i}', {}.{:02}, {note})", (i * 7919) % 100_000, i % 9999, i % 100)
            })
            .collect();
        client.simple(&format!(
            "INSERT INTO rp (id, entry_order, label, amount, note) VALUES {}",
            values.join(",")
        ));
    }
    }
    for shape in ["text", "binary"] {
        if only.is_some_and(|o| o != shape) {
            continue;
        }
        let before = peak_rss_mib();
        let mut times = Vec::new();
        for _ in 0..iters {
            let t = Instant::now();
            let n = if shape == "text" {
                client.simple(sql)
            } else {
                client.extended(sql, 1)
            };
            times.push(t.elapsed());
            assert_eq!(n, rows);
        }
        times.sort();
        println!(
            "wire {shape:<6} {rows} rows  min {:>9.2?}  median {:>9.2?}  peak +{:>7.1} MiB (absolute {:.1} MiB)",
            times[0],
            times[times.len() / 2],
            peak_rss_mib() - before,
            peak_rss_mib()
        );
    }
}

struct WireClient {
    writer: std::net::TcpStream,
    reader: std::io::BufReader<std::net::TcpStream>,
}

impl WireClient {
    fn connect(port: u16) -> std::io::Result<Self> {
        use std::io::Write;
        let socket = std::net::TcpStream::connect(("127.0.0.1", port))?;
        socket.set_nodelay(true)?;
        let mut client = WireClient {
            writer: socket.try_clone()?,
            reader: std::io::BufReader::with_capacity(64 * 1024, socket),
        };
        let mut params = Vec::new();
        for (k, v) in [
            ("user", "rasterized"),
            ("database", rasterizeddb_core::core::database::DEFAULT_DATABASE_NAME),
        ] {
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

    fn until_ready(&mut self) -> usize {
        use std::io::Read;
        let mut rows = 0;
        let mut body = Vec::new();
        loop {
            let mut head = [0u8; 5];
            self.reader.read_exact(&mut head).expect("read header");
            let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
            body.resize(len, 0);
            self.reader.read_exact(&mut body).expect("read body");
            match head[0] {
                b'D' => rows += 1,
                b'E' => panic!("server error: {}", String::from_utf8_lossy(&body)),
                b'Z' => return rows,
                _ => {}
            }
        }
    }

    fn simple(&mut self, sql: &str) -> usize {
        use std::io::Write;
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.writer.write_all(&msg).unwrap();
        self.until_ready()
    }

    fn extended(&mut self, sql: &str, result_format: i16) -> usize {
        use std::io::Write;
        let mut buf = Vec::new();
        let mut frame = |tag: u8, body: &[u8]| {
            buf.push(tag);
            buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            buf.extend_from_slice(body);
        };
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0i16.to_be_bytes());
        frame(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&1i16.to_be_bytes());
        bind.extend_from_slice(&result_format.to_be_bytes());
        frame(b'B', &bind);
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        frame(b'E', &execute);
        frame(b'S', &[]);
        self.writer.write_all(&buf).unwrap();
        self.until_ready()
    }
}

/// Ordering 1M rows by one BIGINT key three ways — `sortkeys` mode:
///   compare: sort indices with a comparator that reads both rows (what
///            `sort_rows_indirect` + `PreparedSort` do),
///   pdq:     extract `(normalised u64, index)` once, `sort_unstable`,
///   radix:   the same pairs, LSD radix on the key bytes (stable).
/// Each is followed by the same cycle-walk permutation of the rows.
fn sortkeys_bench(rows: usize, iters: usize) {
    use rasterizeddb_core::core::db_type::DbType;
    use rasterizeddb_core::core::row::row::{Column, Row};
    use rasterizeddb_core::memory_pool::MEMORY_POOL;

    fn col(id: u64, bytes: &[u8]) -> Column {
        let mut data = MEMORY_POOL.acquire(bytes.len());
        data.into_slice_mut().copy_from_slice(bytes);
        Column { schema_id: rasterizeddb_core::core::row::row::schema_id_of(id), data, column_type: DbType::I64 }
    }
    fn build(rows: usize, modulo: u64) -> Vec<Row> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        (0..rows)
            .map(|i| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let k = (state % modulo) as i64 - (modulo as i64 / 2);
                let mut r = Row::default();
                r.columns.push(col(0, &(i as i64).to_le_bytes()));
                r.columns.push(col(1, &k.to_le_bytes()));
                r.columns.push(col(2, b"label text"));
                r
            })
            .collect()
    }
    fn key(r: &Row) -> i64 {
        let c = match r.columns.get(1) {
            Some(c) if c.schema_id == 1 => c,
            _ => r.columns.iter().find(|c| c.schema_id == 1).unwrap(),
        };
        i64::from_le_bytes(c.data.into_slice().try_into().unwrap())
    }
    fn permute(rows: &mut [Row], order: &mut [u32]) {
        for start in 0..order.len() {
            if order[start] as usize == start {
                continue;
            }
            let mut cur = start;
            loop {
                let next = order[cur] as usize;
                order[cur] = cur as u32;
                if next == start {
                    break;
                }
                rows.swap(cur, next);
                cur = next;
            }
        }
    }
    fn radix(pairs: &mut Vec<(u64, u32)>) {
        let mut tmp = vec![(0u64, 0u32); pairs.len()];
        for pass in 0..8 {
            let shift = pass * 8;
            let mut counts = [0usize; 257];
            for &(k, _) in pairs.iter() {
                counts[((k >> shift) & 0xff) as usize + 1] += 1;
            }
            if counts[1..].iter().any(|&c| c == pairs.len()) {
                continue;
            }
            for i in 1..257 {
                counts[i] += counts[i - 1];
            }
            for &p in pairs.iter() {
                let b = ((p.0 >> shift) & 0xff) as usize;
                tmp[counts[b]] = p;
                counts[b] += 1;
            }
            std::mem::swap(pairs, &mut tmp);
        }
    }

    for modulo in [100_000u64, u64::MAX / 4] {
        let mut t = [Vec::new(), Vec::new(), Vec::new()];
        let mut check: Vec<Vec<i64>> = Vec::new();
        for _ in 0..iters {
            for (m, times) in t.iter_mut().enumerate() {
                let mut rows_v = build(rows, modulo);
                let start = Instant::now();
                let mut order: Vec<u32> = match m {
                    0 => {
                        let mut o: Vec<u32> = (0..rows_v.len() as u32).collect();
                        o.sort_by(|&i, &j| key(&rows_v[i as usize]).cmp(&key(&rows_v[j as usize])));
                        o
                    }
                    _ => {
                        let mut pairs: Vec<(u64, u32)> = rows_v
                            .iter()
                            .enumerate()
                            .map(|(i, r)| ((key(r) as u64) ^ (1 << 63), i as u32))
                            .collect();
                        if m == 1 {
                            pairs.sort_unstable();
                        } else {
                            radix(&mut pairs);
                        }
                        pairs.into_iter().map(|(_, i)| i).collect()
                    }
                };
                permute(&mut rows_v, &mut order);
                times.push(start.elapsed());
                if check.len() < 3 {
                    check.push(rows_v.iter().map(|r| {
                        i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap())
                    }).collect());
                }
            }
        }
        assert!(check.windows(2).all(|w| w[0] == w[1]), "the three orders differ");
        let fmt = |v: &mut Vec<std::time::Duration>| {
            v.sort();
            format!("min {:>9.2?} median {:>9.2?}", v[0], v[v.len() / 2])
        };
        println!(
            "sort {rows} rows, key range {modulo}: compare {} | pdq {} | radix {}",
            fmt(&mut t[0]),
            fmt(&mut t[1]),
            fmt(&mut t[2])
        );
    }
}
