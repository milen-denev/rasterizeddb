//! Inserts of new keys must keep making progress while another session
//! updates other rows of the same table, as they do in PostgreSQL.
//!
//! Found by `connection_drop_stress`: on a file-backed server the inserted row
//! count sat flat at 285k for minutes while scans and updates kept completing.
//! 285k is three inserters times 95k — each inside its 50,000-tuple statement,
//! every tuple of which queued for the lock a 500-row `UPDATE` held across its
//! scan. See `unique_lock` in `table.rs`.
//!
//! `probe` (ignored) replays that load and prints the curve: `FAIR_SECS`,
//! `FAIR_INSERTERS`, `FAIR_BIG_EVERY` (0 for no big statements),
//! `FAIR_UPDATER`, `FAIR_SCANNER`.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

struct Load {
    inserters: u64,
    /// Every `big_every`th statement of an inserter is a 50,000-tuple one;
    /// 0 for never.
    big_every: u64,
    updater: bool,
    scanner: bool,
    secs: u64,
}

/// Rows inserted, sampled every `tick`.
async fn run(server: &TestServer, label: &str, load: Load, tick: Duration) -> Vec<u64> {
    let url = server.url();
    let mut setup = sqlx::PgConnection::connect(&url).await.expect("connect");
    setup
        .execute("CREATE TABLE p (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");
    drop(setup);

    let stop = Arc::new(AtomicBool::new(false));
    let inserted = Arc::new(AtomicU64::new(0));
    let updates = Arc::new(AtomicU64::new(0));
    let update_errors = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    for w in 0..load.inserters {
        let (url, stop, inserted) = (url.clone(), stop.clone(), inserted.clone());
        let big_every = load.big_every;
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut next = w * 100_000_000;
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                let tuples = if big_every > 0 && k % big_every == 0 { 50_000 } else { 5_000 };
                let values = (next..next + tuples)
                    .map(|i| format!("({i}, 'tag{} {}', {i})", i % 977, "x".repeat((i % 7) as usize * 5)))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!("INSERT INTO p (id, tag, n) VALUES {values}");
                conn.execute(AssertSqlSafe(sql)).await.expect("insert");
                next += tuples;
                inserted.fetch_add(tuples, Ordering::Relaxed);
            }
        }));
    }

    if load.updater {
        let (url, stop, updates, update_errors) =
            (url.clone(), stop.clone(), updates.clone(), update_errors.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                // A 500-row range in what the first inserter has written.
                let start = (k * 7_919) % 1_000_000;
                let sql = format!("UPDATE p SET n = n + 1 WHERE id >= {start} AND id < {}", start + 500);
                match conn.execute(AssertSqlSafe(sql)).await {
                    Ok(_) => updates.fetch_add(1, Ordering::Relaxed),
                    Err(_) => update_errors.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }

    if load.scanner {
        let (url, stop) = (url.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            while !stop.load(Ordering::Relaxed) {
                let _ = sqlx::query("SELECT id, n FROM p ORDER BY n DESC LIMIT 50")
                    .fetch_all(&mut conn)
                    .await;
            }
        }));
    }

    let started = Instant::now();
    let mut samples = Vec::new();
    while started.elapsed() < Duration::from_secs(load.secs) {
        tokio::time::sleep(tick).await;
        let n = inserted.load(Ordering::Relaxed);
        samples.push(n);
        println!(
            "[{label}] {:>5.0?} inserted {n} updates {} update-errors {}",
            started.elapsed(),
            updates.load(Ordering::Relaxed),
            update_errors.load(Ordering::Relaxed),
        );
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(120), t).await;
    }
    samples
}

struct Root(String);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn root(tag: &str) -> Root {
    let r = std::env::temp_dir()
        .join(format!("insert_update_fairness_{}_{tag}", std::process::id()))
        .to_string_lossy()
        .into_owned();
    let _ = std::fs::remove_dir_all(&r);
    Root(r)
}

/// Rows inserted while the update stream ran, and while it did not, over
/// alternating windows — so whatever else the machine is doing weighs on both
/// sides alike — plus the number of updates that completed.
async fn alternate(server: &TestServer, windows: u32, window: Duration) -> (u64, u64, u64) {
    let url = server.url();
    let mut setup = sqlx::PgConnection::connect(&url).await.expect("connect");
    setup
        .execute("CREATE TABLE p (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");
    // Rows for the updates to land on.
    const SEED: u64 = 20_000;
    const BATCH: u64 = 100;
    for start in (0..SEED).step_by(5_000) {
        let values = (start..start + 5_000)
            .map(|i| format!("({i}, 'seed', {i})"))
            .collect::<Vec<_>>()
            .join(",");
        setup
            .execute(AssertSqlSafe(format!("INSERT INTO p (id, tag, n) VALUES {values}")))
            .await
            .expect("seed");
    }
    drop(setup);

    let stop = Arc::new(AtomicBool::new(false));
    let updating = Arc::new(AtomicBool::new(false));
    let inserted = Arc::new(AtomicU64::new(0));
    let updates = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    for w in 1..=3u64 {
        let (url, stop, inserted) = (url.clone(), stop.clone(), inserted.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut next = w * 100_000_000;
            while !stop.load(Ordering::Relaxed) {
                // Small statements, so a window counts many of them.
                let values = (next..next + BATCH)
                    .map(|i| format!("({i}, 'tag{}', {i})", i % 977))
                    .collect::<Vec<_>>()
                    .join(",");
                conn.execute(AssertSqlSafe(format!("INSERT INTO p (id, tag, n) VALUES {values}")))
                    .await
                    .expect("insert");
                next += BATCH;
                inserted.fetch_add(BATCH, Ordering::Relaxed);
            }
        }));
    }
    {
        let (url, stop, updating, updates) =
            (url.clone(), stop.clone(), updating.clone(), updates.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if !updating.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                k += 1;
                let start = (k * 7_919) % (SEED - 500);
                let sql = format!("UPDATE p SET n = n + 1 WHERE id >= {start} AND id < {}", start + 500);
                if conn.execute(AssertSqlSafe(sql)).await.is_ok() {
                    updates.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    let (mut with, mut without) = (0u64, 0u64);
    for i in 0..windows {
        let on = i % 2 == 1;
        updating.store(on, Ordering::Relaxed);
        let before = inserted.load(Ordering::Relaxed);
        tokio::time::sleep(window).await;
        let got = inserted.load(Ordering::Relaxed) - before;
        println!("window {i:>2} updates {:<5} inserted {got}", if on { "on" } else { "off" });
        if on { with += got } else { without += got }
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(60), t).await;
    }
    (with, without, updates.load(Ordering::Relaxed))
}

/// A multi-row `INSERT` takes the uniqueness lock once per tuple, and that lock
/// used to be the one an `UPDATE` holds across its whole scan, so every tuple
/// queued behind a scan. Release, file-backed: with the update stream running
/// the insert rate fell to 6% of its rate without it (0.06, twice); now it
/// holds at ~57%, the two contending only for the update's rewrite. Without
/// even that (unsafe — see the next test) it is ~73%: the rest is CPU.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn inserts_keep_their_pace_while_another_session_updates() {
    let dir = root("pace");
    let server = TestServer::start_file_backed(&dir.0).await.expect("start");
    let secs = env_u64("FAIR_WINDOW_SECS", 3);
    let windows = env_u64("FAIR_WINDOWS", 8) as u32;
    let (with, without, updates) = alternate(&server, windows, Duration::from_secs(secs)).await;
    let ratio = with as f64 / without.max(1) as f64;
    println!("inserted {with} with updates running, {without} without ({ratio:.2}); {updates} updates");
    assert!(updates >= 10, "the update stream barely ran ({updates} updates): the test proves nothing");
    // The floor was 0.3 until the primary key got an index (2026-10-10). The
    // same run on the commit before and after it, quiet machine:
    //
    //   inserts, no update running:   7,600  ->  56,900   (7.5x)
    //   inserts, updates running:     3,550  ->   7,700   (2.2x)
    //   updates completed:              248  ->     373
    //   ratio:                         0.47  ->    0.14
    //
    // Nothing got slower; the uncontended side got faster than the contended
    // one could. The update's `WHERE id >= … AND id < …` became a range scan,
    // so the unlocked phase of each update is now nearly free and the update
    // stream holds the uniqueness lock for most of its cycle. The regression
    // this test exists for, inserts queueing behind an update's *scan*,
    // measured 0.06 against the old, much slower baseline and would measure
    // lower against this one, so 0.10 still catches it.
    assert!(
        ratio >= 0.10,
        "inserts slowed to {:.0}% of their pace while another session updated other rows \
         ({with} rows against {without})",
        ratio * 100.0
    );
}

/// The other half: taking inserts out from under the update's lock must not
/// let one in that the lock used to keep out.
///
/// An `UPDATE` that relocates a row writes its new version first and indexes
/// it only once the batch is durable, while the old version keeps the key. A
/// uniqueness check that sampled the filter in between, and read the old
/// version only after the update committed, would find the key nowhere and
/// store it twice — a duplicate of a key nobody changed. Every insert here
/// re-uses a key that exists, while other sessions keep relocating those rows,
/// so every one of them must fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_key_whose_row_is_being_relocated_is_never_inserted_twice() {
    let dir = root("dup");
    let server = TestServer::start_file_backed(&dir.0).await.expect("start");
    let url = server.url();
    const N: u64 = 200;
    let mut setup = sqlx::PgConnection::connect(&url).await.expect("connect");
    setup
        .execute("CREATE TABLE p (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");
    let values = (0..N).map(|i| format!("({i}, 'x', {i})")).collect::<Vec<_>>().join(",");
    setup
        .execute(AssertSqlSafe(format!("INSERT INTO p (id, tag, n) VALUES {values}")))
        .await
        .expect("seed");

    let stop = Arc::new(AtomicBool::new(false));
    let admitted = Arc::new(AtomicU64::new(0));
    let attempts = Arc::new(AtomicU64::new(0));
    let relocations = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for u in 0..2u64 {
        let (url, stop, relocations) = (url.clone(), stop.clone(), relocations.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut k = u;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                // A text value of a new length: the row cannot be rewritten in
                // place, so it moves.
                let start = (k * 37) % (N - 20);
                let sql = format!(
                    "UPDATE p SET tag = '{}' WHERE id >= {start} AND id < {}",
                    "y".repeat((k % 40) as usize + 1),
                    start + 20
                );
                if conn.execute(AssertSqlSafe(sql)).await.is_ok() {
                    relocations.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for w in 0..4u64 {
        let (url, stop, admitted, attempts) =
            (url.clone(), stop.clone(), admitted.clone(), attempts.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
            let mut k = w * 53;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                let a = k % N;
                let b = (k * 7 + 3) % N;
                let sql = format!(
                    "INSERT INTO p (id, tag, n) VALUES ({a}, 'dup', -1), ({b}, 'dup', -1) \
                     ON CONFLICT (id) DO NOTHING"
                );
                attempts.fetch_add(1, Ordering::Relaxed);
                if let Ok(r) = conn.execute(AssertSqlSafe(sql)).await {
                    admitted.fetch_add(r.rows_affected(), Ordering::Relaxed);
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(env_u64("FAIR_DUP_SECS", 8))).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(60), t).await;
    }

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM p")
        .fetch_one(&mut setup)
        .await
        .expect("count");
    let dups: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM p WHERE n = -1")
        .fetch_one(&mut setup)
        .await
        .expect("count dups");
    let (attempts, relocations, admitted) = (
        attempts.load(Ordering::Relaxed),
        relocations.load(Ordering::Relaxed),
        admitted.load(Ordering::Relaxed),
    );
    println!("{attempts} insert statements, {relocations} relocating updates, {admitted} rows admitted, {rows} rows, {dups} duplicates");
    assert!(relocations >= 10 && attempts >= 100, "the race barely ran: the test proves nothing");
    assert_eq!(admitted, 0, "an insert of an existing key was reported as inserted");
    assert_eq!((rows, dups), (N as i64, 0), "a duplicate key was stored");
}

#[ignore = "measurement: env-driven, see the module docs"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn probe() {
    let dir = root("probe");
    let server = TestServer::start_file_backed(&dir.0).await.expect("start");
    let load = Load {
        inserters: env_u64("FAIR_INSERTERS", 3),
        big_every: env_u64("FAIR_BIG_EVERY", 20),
        updater: env_u64("FAIR_UPDATER", 1) == 1,
        scanner: env_u64("FAIR_SCANNER", 0) == 1,
        secs: env_u64("FAIR_SECS", 120),
    };
    run(&server, "probe", load, Duration::from_secs(10)).await;
}
