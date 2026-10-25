//! Open time of a table's indexes: rebuilt from rows, or loaded from the
//! index snapshot (docs/persistent-indexes.md §4 and §12, phase 0's (e)
//! and phase 2's gate; slice B18). Each mode is its own process, so an
//! `open` measures a fresh process against files the previous one left.
//!
//! ```text
//! index_rebuild_bench load  --dir D --rows N [--nopk]
//!     CREATE TABLE t (id BIGINT PRIMARY KEY, tenant_id BIGINT, email VARCHAR)
//!     ([--nopk]: no PRIMARY KEY), N rows appended under the bulk-load
//!     guard, made durable, then sealed under index_storage = snapshot
//!     (rdb_index_checkpoint), as a clean shutdown leaves it.
//! index_rebuild_bench ddl   --dir D --sql "CREATE INDEX ..." [--sql ...]
//!     run statements, wait for the indexes, seal again.
//! index_rebuild_bench open  --dir D [--cold] [--snapshot]
//!     (--cold: syncfs, then posix_fadvise(DONTNEED) on every file under D,
//!     and the pages still resident counted), then time Database::new and
//!     the wait until table t's indexes and filter are ready. --snapshot
//!     opens under index_storage = snapshot (the load); without it, under
//!     rebuild. One RESULT line: the origin, times, RSS, snapshot bytes.
//!
//! index_rebuild_bench ptload --dir D --parts P --rows-per R
//!     a table hash-partitioned P ways on tenant_id, P*R rows, primary key
//!     (tenant_id, id), sealed for shutdown.
//! index_rebuild_bench ptopen --dir D [--cold] [--snapshot]
//!     startup to every partition's indexes ready.
//!
//! index_rebuild_bench bumpload --dir D --big B --big-rows N --small-rows M
//!     B tables of N rows and one table `hot` of M rows, three indexes
//!     each, all sealed.
//! index_rebuild_bench bumpopen --dir D --max-loads L [--no-bump]
//!     opens under snapshot with L load slots and writes one row into `hot`
//!     at once: the time to that write's commit, with the priority bump
//!     (B18) or without it.
//!
//! index_rebuild_bench mb2 --dir D --secs S [--snapshots]
//!     MB2 (RV7, slice B15's open item): on a directory `load` and `ddl`
//!     prepared (table t, three indexes), autocommit single-row INSERTs into
//!     a bigint-PK table `w`, each waiting for its commit's sync, for S
//!     seconds; with --snapshots, every 30 s a row goes into t and t gets an
//!     optional seal (its whole snapshot captured, written and synced on
//!     the same device). Commit p50/p99/max, and each seal's outcome and
//!     time.
//!
//!   cargo run --release -p rasterizeddb_core --example index_rebuild_bench -- ...
//! ```
//!
//! `--log` turns on the load's own log lines: the proof's read of the
//! snapshot, each section's read and decode, each index's install.

use std::path::Path;
use std::time::{Duration, Instant};

use rclite::Arc;
use smallvec::SmallVec;

use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::database::relation::Relation;
use rasterizeddb_core::core::index_store::load::{self, Origin, settings::IndexStorage};
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::rql::{executor, lexer_s1::recognize_query_purpose};
use rasterizeddb_core::core::storage_providers::backend::DbStorageProvider;
use rasterizeddb_core::memory_pool::MEMORY_POOL;

type DbTable = Table<DbStorageProvider>;

#[derive(Default)]
struct Args {
    mode: String,
    dir: String,
    rows: u64,
    sql: Vec<String>,
    cold: bool,
    nopk: bool,
    snapshot: bool,
    log: bool,
    parts: u64,
    rows_per: u64,
    big: u64,
    big_rows: u64,
    small_rows: u64,
    max_loads: u64,
    no_bump: bool,
    secs: u64,
    snapshots: bool,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut a = Args::default();
    let mut i = 0;
    let num = |i: &mut usize| -> u64 {
        *i += 1;
        argv[*i].parse().unwrap()
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "load" | "ddl" | "open" | "ptload" | "ptopen" | "bumpload" | "bumpopen" | "mb2" => a.mode = argv[i].clone(),
            "--dir" => {
                i += 1;
                a.dir = argv[i].clone()
            }
            "--sql" => {
                i += 1;
                a.sql.push(argv[i].clone())
            }
            "--rows" => a.rows = num(&mut i),
            "--parts" => a.parts = num(&mut i),
            "--rows-per" => a.rows_per = num(&mut i),
            "--big" => a.big = num(&mut i),
            "--big-rows" => a.big_rows = num(&mut i),
            "--small-rows" => a.small_rows = num(&mut i),
            "--max-loads" => a.max_loads = num(&mut i),
            "--secs" => a.secs = num(&mut i),
            "--snapshots" => a.snapshots = true,
            "--cold" => a.cold = true,
            "--nopk" => a.nopk = true,
            "--snapshot" => a.snapshot = true,
            "--log" => a.log = true,
            "--no-bump" => a.no_bump = true,
            other => panic!("unknown argument {other}"),
        }
        i += 1;
    }
    assert!(!a.mode.is_empty() && !a.dir.is_empty(), "usage: see the header");
    a
}

fn main() {
    let args = parse_args();
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(16);
    let _ = rasterizeddb_core::BATCH_SIZE.set(10_000);
    let _ = rasterizeddb_core::cache::query_cache::ENABLE_CACHE.set(false);
    if args.log {
        let _ = env_logger::builder()
            .filter_module("rasterizeddb_core::core::index_store::load", log::LevelFilter::Debug)
            .filter_module("rasterizeddb_core::core::index_store::open_proof", log::LevelFilter::Debug)
            .try_init();
    }
    // Evict before anything in this process touches the files.
    if args.cold {
        let (files, bytes, resident) = evict(Path::new(&args.dir));
        eprintln!(
            "evict: {files} files, {:.1} MiB, {:.1} MiB still resident",
            bytes as f64 / (1 << 20) as f64,
            resident as f64 / (1 << 20) as f64
        );
    }
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                match args.mode.as_str() {
                    "load" => load_rows(&args).await,
                    "ddl" => ddl(&args).await,
                    "open" => open(&args).await,
                    "ptload" => pt_load(&args).await,
                    "ptopen" => pt_open(&args).await,
                    "bumpload" => bump_load(&args).await,
                    "bumpopen" => bump_open(&args).await,
                    "mb2" => mb2(&args).await,
                    _ => unreachable!(),
                }
            });
            rasterizeddb_core::shutdown::finalize();
            // Background maintenance never ends on its own; nothing is
            // sealed at exit, so the files stay as the last seal left them.
            std::process::exit(0);
        })
        .unwrap()
        .join()
        .unwrap();
}

/// syncfs, then DONTNEED on every file under `dir`; the bytes `mincore`
/// still finds resident afterwards (0 on a real disk, all of it on tmpfs,
/// whose pages are the storage).
fn evict(dir: &Path) -> (u64, u64, u64) {
    use std::os::fd::AsRawFd;
    if let Ok(d) = std::fs::File::open(dir) {
        unsafe { libc::syncfs(d.as_raw_fd()) };
    }
    let (mut files, mut bytes, mut resident) = (0u64, 0u64, 0u64);
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let meta = entry.metadata().unwrap();
        if meta.is_dir() {
            let (f, b, r) = evict(&path);
            files += f;
            bytes += b;
            resident += r;
        } else if meta.is_file() && meta.len() > 0 {
            let f = std::fs::File::open(&path).unwrap();
            let rc = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            assert_eq!(rc, 0, "fadvise {}", path.display());
            files += 1;
            bytes += meta.len();
            resident += resident_bytes(&f, meta.len());
        }
    }
    (files, bytes, resident)
}

fn resident_bytes(f: &std::fs::File, len: u64) -> u64 {
    use std::os::fd::AsRawFd;
    let page = 4096u64;
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len as usize, libc::PROT_READ, libc::MAP_SHARED, f.as_raw_fd(), 0);
        if p == libc::MAP_FAILED {
            return 0;
        }
        let pages = len.div_ceil(page) as usize;
        let mut vec = vec![0u8; pages];
        let rc = libc::mincore(p, len as usize, vec.as_mut_ptr());
        libc::munmap(p, len as usize);
        if rc != 0 {
            return 0;
        }
        vec.iter().filter(|b| **b & 1 == 1).count() as u64 * page
    }
}

async fn exec(db: &Arc<Database>, sql: &str) {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
    let out = executor::execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        panic!("{sql}: {}", String::from_utf8_lossy(&out[1..]));
    }
}

fn plain(db: &Database, name: &str) -> Arc<DbTable> {
    match db.relation(name) {
        Some(Relation::Plain(t)) => t,
        _ => panic!("{name} is not a plain table"),
    }
}

fn payload(bytes: &[u8], write_order: u32, f: &rasterizeddb_core::core::row::schema::SchemaField) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    ColumnWritePayload { data: mb, write_order, column_type: f.db_type.clone(), size: f.db_type.get_size() }
}

/// Waits until `t`'s indexes and filter are ready.
async fn ready(t: &DbTable) {
    t.index_load_settled().await;
    while t.indexes().unwrap().is_some_and(|i| !i.is_ready()) || !t.filters_ready().unwrap() {
        tokio::time::sleep(Duration::from_micros(500)).await;
    }
}

/// Seals `name` under snapshot: what a clean shutdown leaves.
async fn seal(db: &Arc<Database>, name: &str) {
    let report = db.index_checkpoint(name, None).await.expect("rdb_index_checkpoint");
    let (clean, of) = report.clean();
    assert_eq!(clean, of, "{name} was not sealed clean: {:?}", report.sealed);
}

fn make_durable(db: &Arc<Database>) {
    let location = db.location.clone().unwrap();
    rasterizeddb_core::core::bulk_load::sync_all(&location).expect("sync_all");
    db.sync_durable().expect("sync_durable");
    db.transactions.flush().expect("flush");
}

/// `t`'s row: id, tenant_id, email, in write order.
async fn append_rows(db: &Arc<Database>, table: &Arc<DbTable>, rows: u64) {
    let fields = table.schema.fields.clone();
    let field = |name: &str| fields.iter().find(|f| f.name == name).unwrap().clone();
    let (fid, ften, femail) = (field("id"), field("tenant_id"), field("email"));
    let mut order: Vec<(u32, &str)> = vec![
        (fid.write_order as u32, "id"),
        (ften.write_order as u32, "tenant_id"),
        (femail.write_order as u32, "email"),
    ];
    order.sort();
    let t0 = Instant::now();
    let mut tx = db.transactions.begin();
    let mut email = String::with_capacity(64);
    for i in 0..rows {
        let id = i as i64 + 1;
        let tenant = (i % 1000) as i64;
        email.clear();
        use std::fmt::Write;
        write!(email, "user{:010}@tenant{:04}.example", id, tenant).unwrap();
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        for (wo, name) in &order {
            rw.columns_writing_data.push(match *name {
                "id" => payload(&id.to_le_bytes(), *wo, &fid),
                "tenant_id" => payload(&tenant.to_le_bytes(), *wo, &ften),
                _ => payload(email.as_bytes(), *wo, &femail),
            });
        }
        table.insert_row_in_txn(rw, tx).await.expect("insert");
        if (i + 1) % 5_000 == 0 {
            let family = db.commit_transaction(tx);
            db.publish_transaction(family).await;
            tx = db.transactions.begin();
        }
        if (i + 1) % 1_000_000 == 0 {
            eprintln!("  {} rows, {:.1}s", i + 1, t0.elapsed().as_secs_f64());
        }
    }
    let family = db.commit_transaction(tx);
    db.publish_transaction(family).await;
}

async fn load_rows(args: &Args) {
    let _ = std::fs::remove_dir_all(&args.dir);
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let db = Arc::new(Database::new(&args.dir).await);
    exec(&db, if args.nopk {
        "CREATE TABLE t (id BIGINT, tenant_id BIGINT, email VARCHAR);"
    } else {
        "CREATE TABLE t (id BIGINT PRIMARY KEY, tenant_id BIGINT, email VARCHAR);"
    })
    .await;
    let table = plain(&db, "t");
    let t0 = Instant::now();
    {
        let _bulk = rasterizeddb_core::core::bulk_load::begin(&args.dir);
        append_rows(&db, &table, args.rows).await;
    }
    let loaded = t0.elapsed();
    make_durable(&db);
    ready(&table).await;
    seal(&db, "t").await;
    eprintln!(
        "load: {} rows in {:.1}s ({:.0} rows/s), sealed after {:.1}s",
        args.rows,
        loaded.as_secs_f64(),
        args.rows as f64 / loaded.as_secs_f64(),
        t0.elapsed().as_secs_f64()
    );
}

async fn ddl(args: &Args) {
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let db = Arc::new(Database::new(&args.dir).await);
    ready(&plain(&db, "t")).await;
    for sql in &args.sql {
        let t0 = Instant::now();
        exec(&db, sql).await;
        ready(&plain(&db, "t")).await;
        eprintln!("ddl: {sql} -> {:.2}s to ready", t0.elapsed().as_secs_f64());
    }
    make_durable(&db);
    seal(&db, "t").await;
    let t = plain(&db, "t");
    eprintln!("ddl: table t now has {} index(es), sealed", t.indexes().unwrap().map_or(0, |i| i.list().len()));
}

/// `VmRSS`, `RssAnon`, `RssFile`, `RssShmem`, `VmHWM` of this process, MiB.
fn memory() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(0.0, |kb| kb / 1024.0)
    };
    format!(
        "rss_mib={:.1} anon_mib={:.1} file_mib={:.1} shmem_mib={:.1} hwm_mib={:.1}",
        field("VmRSS:"),
        field("RssAnon:"),
        field("RssFile:"),
        field("RssShmem:"),
        field("VmHWM:")
    )
}

fn origin_name(o: Origin) -> &'static str {
    match o {
        Origin::Loaded => "loaded",
        Origin::Built => "built",
        Origin::FellBack => "fellback",
        Origin::Pending => "pending",
    }
}

async fn open(args: &Args) {
    load::settings::set_index_storage(if args.snapshot { IndexStorage::Snapshot } else { IndexStorage::Rebuild });
    let ixs = std::fs::metadata(Path::new(&args.dir).join("t.ixs")).map_or(0, |m| m.len());
    let t0 = Instant::now();
    let db = Arc::new(Database::new(&args.dir).await);
    let t_new = t0.elapsed();
    let t = plain(&db, "t");
    ready(&t).await;
    let t_ready = t0.elapsed();
    let n_ix = t.indexes().unwrap().map_or(0, |i| i.list().len());
    println!(
        "RESULT mode={} cold={} origin={} indexes={} new_s={:.3} ready_s={:.3} ixs_bytes={} {}",
        if args.snapshot { "snapshot" } else { "rebuild" },
        args.cold,
        origin_name(t.index_origin()),
        n_ix,
        t_new.as_secs_f64(),
        t_ready.as_secs_f64(),
        ixs,
        memory()
    );
}

async fn pt_load(args: &Args) {
    let _ = std::fs::remove_dir_all(&args.dir);
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let db = Arc::new(Database::new(&args.dir).await);
    exec(
        &db,
        &format!(
            "CREATE TABLE p (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
             PARTITION BY HASH (tenant_id) PARTITIONS {}",
            args.parts
        ),
    )
    .await;
    let total = args.parts * args.rows_per;
    let t0 = Instant::now();
    let mut id = 0u64;
    while id < total {
        let end = (id + 10_000).min(total);
        let values: Vec<String> = (id..end).map(|i| format!("({}, {i}, {})", i % (args.parts * 16), i * 3)).collect();
        exec(&db, &format!("INSERT INTO p (tenant_id, id, v) VALUES {}", values.join(", "))).await;
        id = end;
    }
    make_durable(&db);
    let pt = db.partitioned_table("p").unwrap();
    let epoch = pt.epoch().unwrap();
    let parts = pt.materialized(&epoch, &rasterizeddb_core::core::partition::Route::All).unwrap();
    for (_, part) in &parts {
        ready(part).await;
    }
    let report = db.seal_for_shutdown(Duration::from_secs(600)).await;
    assert!(report.skipped.is_empty(), "the shutdown seal skipped {:?}", report.skipped);
    eprintln!(
        "ptload: {} partitions, {total} rows in {:.1}s, {} file sets sealed",
        parts.len(),
        t0.elapsed().as_secs_f64(),
        report.sealed.len()
    );
}

async fn pt_open(args: &Args) {
    load::settings::set_index_storage(if args.snapshot { IndexStorage::Snapshot } else { IndexStorage::Rebuild });
    let t0 = Instant::now();
    let db = Arc::new(Database::new(&args.dir).await);
    let t_new = t0.elapsed();
    let pt = db.partitioned_table("p").unwrap();
    let epoch = pt.epoch().unwrap();
    let parts = pt.materialized(&epoch, &rasterizeddb_core::core::partition::Route::All).unwrap();
    for (_, part) in &parts {
        ready(part).await;
    }
    let t_ready = t0.elapsed();
    let loaded = parts.iter().filter(|(_, p)| p.index_origin() == Origin::Loaded).count();
    println!(
        "RESULT mode={} cold={} partitions={} loaded={} new_s={:.3} ready_s={:.3} {}",
        if args.snapshot { "snapshot" } else { "rebuild" },
        args.cold,
        parts.len(),
        loaded,
        t_new.as_secs_f64(),
        t_ready.as_secs_f64(),
        memory()
    );
}

async fn bump_load(args: &Args) {
    let _ = std::fs::remove_dir_all(&args.dir);
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let db = Arc::new(Database::new(&args.dir).await);
    let mut names: Vec<(String, u64)> = (0..args.big).map(|i| (format!("big{i}"), args.big_rows)).collect();
    names.push(("hot".into(), args.small_rows));
    for (name, rows) in &names {
        exec(&db, &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, tenant_id BIGINT, email VARCHAR)")).await;
        exec(&db, &format!("CREATE INDEX {name}_tid ON {name} (tenant_id, id)")).await;
        exec(&db, &format!("CREATE INDEX {name}_e ON {name} USING hash (email)")).await;
        let table = plain(&db, name);
        {
            let _bulk = rasterizeddb_core::core::bulk_load::begin(&args.dir);
            append_rows(&db, &table, *rows).await;
        }
        make_durable(&db);
        ready(&table).await;
        seal(&db, name).await;
    }
    eprintln!("bumpload: {} tables of {} rows and hot of {} rows, sealed", args.big, args.big_rows, args.small_rows);
}

async fn bump_open(args: &Args) {
    load::settings::set_index_storage(IndexStorage::Snapshot);
    load::settings::set_max_concurrent_loads(args.max_loads);
    load::settings::set_priority_bump(!args.no_bump);
    let t0 = Instant::now();
    let db = Arc::new(Database::new(&args.dir).await);
    let t_new = t0.elapsed();
    exec(&db, "INSERT INTO hot (id, tenant_id, email) VALUES (900000001, 7, 'first@write.example')").await;
    let t_write = t0.elapsed();
    let mut names = db.relation_names();
    names.sort();
    let mut loaded = 0;
    for name in &names {
        if let Some(Relation::Plain(t)) = db.relation(name) {
            ready(&t).await;
            loaded += usize::from(t.index_origin() == Origin::Loaded);
        }
    }
    let t_all = t0.elapsed();
    println!(
        "RESULT bump={} max_loads={} tables={} loaded={} new_s={:.3} first_write_s={:.3} all_ready_s={:.3} bumped={}",
        !args.no_bump,
        args.max_loads,
        names.len(),
        loaded,
        t_new.as_secs_f64(),
        t_write.as_secs_f64(),
        t_all.as_secs_f64(),
        load::bumped_total()
    );
}

async fn mb2(args: &Args) {
    use rasterizeddb_core::core::index_store::gate_word::SealKind;
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let db = Arc::new(Database::new(&args.dir).await);
    let t = plain(&db, "t");
    ready(&t).await;
    if db.relation("w").is_none() {
        exec(&db, "CREATE TABLE w (id BIGINT PRIMARY KEY, v BIGINT)").await;
    }
    let base: i64 = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64 * 16;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sealer = args.snapshots.then(|| {
        let (db, t, stop) = (db.clone(), t.clone(), stop.clone());
        tokio::spawn(async move {
            let mut seals = Vec::new();
            let mut k = 0i64;
            loop {
                for _ in 0..300 {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        return seals;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                k += 1;
                let id = 3_000_000_000i64 + base % 1_000_000_000 + k;
                let sql = format!("INSERT INTO t (id, tenant_id, email) VALUES ({id}, 1, 'seal{id}@x')");
                let purpose = recognize_query_purpose(&sql).unwrap();
                executor::execute_with_sync(purpose, db.clone(), true).await;
                let at = Instant::now();
                let outcome = t.seal_snapshot(SealKind::Optional).await;
                seals.push((at.elapsed().as_secs_f64(), outcome.describe()));
            }
        })
    });
    let started = Instant::now();
    let mut lat: Vec<f64> = Vec::new();
    let mut i = 0i64;
    while started.elapsed() < Duration::from_secs(args.secs) {
        i += 1;
        let sql = format!("INSERT INTO w (id, v) VALUES ({}, {i})", base + i);
        let purpose = recognize_query_purpose(&sql).unwrap();
        let at = Instant::now();
        let out = executor::execute_with_sync(purpose, db.clone(), true).await;
        lat.push(at.elapsed().as_secs_f64() * 1e3);
        assert_ne!(out.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&out));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let seals = match sealer {
        Some(h) => h.await.unwrap(),
        None => Vec::new(),
    };
    lat.sort_by(|a, b| a.total_cmp(b));
    let q = |p: f64| lat[((lat.len() as f64 - 1.0) * p) as usize];
    let seal_desc: Vec<String> = seals.iter().map(|(s, o)| format!("{s:.3}s:{o}")).collect();
    println!(
        "RESULT snapshots={} commits={} p50_ms={:.3} p99_ms={:.3} p999_ms={:.3} max_ms={:.3} seals=[{}]",
        args.snapshots,
        lat.len(),
        q(0.5),
        q(0.99),
        q(0.999),
        lat[lat.len() - 1],
        seal_desc.join(" ")
    );
}
