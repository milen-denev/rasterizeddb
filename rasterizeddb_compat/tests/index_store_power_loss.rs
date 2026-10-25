//! Persistent indexes phase 1 over pgwire (slice B15, docs/persistent-indexes.md
//! §11): a file-backed server's filter through (a) a clean close and reopen
//! from its snapshot and (b) power cuts at the §11 failpoints that touch the
//! filter, in the database's root and on a second storage location with its
//! own WAL, plus bug (c)'s scenario under `synchronous_commit = off`.
//!
//! The failpoints and the crash-image recorder come from the core crate's
//! test-only `index_store_failpoints` feature, enabled here as a
//! dev-dependency feature: the server runs in this process, built without
//! `cfg(test)`. Every statement goes over the wire (sqlx); the engine is
//! touched only to read whether an open loaded its snapshot (the word's
//! `vouched`) and to set the table's filter to consult from the first row.
//!
//! **The twin** is a model of the table, never restarted, fed only the
//! statements the server acknowledged; each image's rows, read with no
//! predicate (nothing to prune), must equal one of the states the twin
//! passed through, and every filtered answer must then be that state's.
//!
//! **With INDEX sections** (slice B17): the same suites on a table with a
//! B-tree, a hash and a composite index, under `index_storage = snapshot`
//! (set process-wide in this binary; it changes nothing for a table with
//! no index). An image that proves its snapshot must load the INDEX
//! sections too, never the filter alone; every answer goes through the
//! loaded (or rebuilt) indexes, and a sample of every image's rows must be
//! found through each index (no false negatives).
//!
//! Bounded: ~1,000 rows, `RDB_POWER_LOSS_SEEDS` images per cut (default 8;
//! the doc's CI figure is 32, `RDB_POWER_LOSS_SEEDS=32`), data under the
//! shared tmpfs, every directory removed on every path.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::power_loss::failpoint::{self, Hit, Outcome};
use rasterizeddb_core::core::index_store::load::Origin;
use rasterizeddb_core::core::index_store::load::settings::{IndexStorage, set_index_storage};
use rasterizeddb_core::core::power_loss::image::{self, CrashPoint, Recorder};
use sqlx::{AssertSqlSafe, Row};

const ROWS: i64 = 1000;

fn seeds() -> u64 {
    std::env::var("RDB_POWER_LOSS_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8u64)
        .clamp(1, 1024)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    Root,
    Second,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Load,
    Rebuild,
    Either,
}

/// `<base>/root` for the server, `<base>/disk` for the second location:
/// siblings under one recorded directory.
struct Dirs {
    base: PathBuf,
}

impl Dirs {
    fn root(&self) -> PathBuf {
        self.base.join("root")
    }
    fn disk(&self) -> PathBuf {
        self.base.join("disk")
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ModelRow {
    a: i64,
    b: i64,
    s: String,
}

impl ModelRow {
    fn for_id(id: i64) -> ModelRow {
        ModelRow {
            a: (id * 7919) % 1000,
            b: id / 10,
            s: format!("s{}", id % 97),
        }
    }
}

type Model = BTreeMap<i64, ModelRow>;

#[derive(Debug, Clone)]
enum Pred {
    A(i64),
    B(i64),
    S(String),
    ARange(i64, i64),
    BA(i64, i64),
}

impl Pred {
    fn sql(&self) -> String {
        match self {
            Pred::A(v) => format!("a = {v}"),
            Pred::B(v) => format!("b = {v}"),
            Pred::S(v) => format!("s = '{v}'"),
            Pred::ARange(lo, hi) => format!("a BETWEEN {lo} AND {hi}"),
            Pred::BA(b, a) => format!("b = {b} AND a = {a}"),
        }
    }
    fn matches(&self, r: &ModelRow) -> bool {
        match self {
            Pred::A(v) => r.a == *v,
            Pred::B(v) => r.b == *v,
            Pred::S(v) => r.s == *v,
            Pred::ARange(lo, hi) => r.a >= *lo && r.a <= *hi,
            Pred::BA(b, a) => r.b == *b && r.a == *a,
        }
    }
}

fn answer(m: &Model, p: &Pred) -> Vec<i64> {
    m.iter().filter(|(_, r)| p.matches(r)).map(|(id, _)| *id).collect()
}

/// Equalities on every touched row's values, and a spread of others.
fn battery(states: &[&Model], touched: &[ModelRow]) -> Vec<Pred> {
    let mut out = Vec::new();
    for m in states {
        for (i, r) in m.values().enumerate().filter(|(i, _)| i % 97 == 3) {
            out.push(Pred::A(r.a));
            out.push(Pred::S(r.s.clone()));
            if i % 2 == 0 {
                out.push(Pred::B(r.b));
                out.push(Pred::ARange(r.a, r.a + 7));
            }
        }
    }
    for r in touched {
        out.push(Pred::A(r.a));
        out.push(Pred::B(r.b));
    }
    out.push(Pred::A(123_456_789));
    out
}

struct Subject {
    server: Option<TestServer>,
    pool: Option<sqlx::PgPool>,
    table: &'static str,
    /// The directories whose logs the server registered.
    logs: [PathBuf; 2],
}

impl Subject {
    async fn start(dirs: &Dirs, table: &'static str) -> Subject {
        let server = TestServer::start_file_backed(&dirs.root().to_string_lossy()).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        Subject {
            server: Some(server),
            pool: Some(pool),
            table,
            logs: [dirs.root(), dirs.disk()],
        }
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.pool.as_ref().unwrap()
    }

    async fn exec(&self, sql: &str) {
        sqlx::query(AssertSqlSafe(sql)).execute(self.pool()).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// The table's word says the open loaded its snapshot (once a load of
    /// its INDEX sections, which vouches when it succeeds, has settled).
    async fn vouched(&self) -> bool {
        let t = self.server.as_ref().unwrap().database().table_for_tests(self.table).expect("the table");
        tokio::time::timeout(std::time::Duration::from_secs(30), t.index_load_settled())
            .await
            .expect("the index load never settled");
        t.gate().vouched()
    }

    /// How the table's indexes came to be at this open, if it has any.
    fn origin(&self) -> Option<Origin> {
        let t = self.server.as_ref().unwrap().database().table_for_tests(self.table).expect("the table");
        let has = t.indexes().ok().flatten().is_some_and(|ix| !ix.list().is_empty());
        has.then(|| t.index_origin())
    }

    /// Waits for the filter (a rebuilt one backfills) and the indexes.
    async fn ready(&self) {
        let t = self.server.as_ref().unwrap().database().table_for_tests(self.table).expect("the table");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
            assert!(std::time::Instant::now() < deadline, "the filter or the indexes never became ready");
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    async fn checkpoint(&self) -> String {
        let row = sqlx::query(AssertSqlSafe(format!("SELECT rdb_index_checkpoint('{}')", self.table)))
            .fetch_one(self.pool())
            .await
            .expect("rdb_index_checkpoint");
        row.get::<String, _>(0)
    }

    async fn scan(&self) -> Model {
        let rows = sqlx::query(AssertSqlSafe(format!("SELECT id, a, b, s FROM {}", self.table)))
            .fetch_all(self.pool())
            .await
            .expect("scan");
        rows.iter()
            .map(|r| {
                (
                    r.get::<i64, _>(0),
                    ModelRow {
                        a: r.get::<i64, _>(1),
                        b: r.get::<i64, _>(2),
                        s: r.get::<String, _>(3),
                    },
                )
            })
            .collect()
    }

    async fn ids(&self, clause: &str) -> Vec<i64> {
        let mut v: Vec<i64> = sqlx::query(AssertSqlSafe(format!("SELECT id FROM {} WHERE {clause}", self.table)))
            .fetch_all(self.pool())
            .await
            .unwrap_or_else(|e| panic!("{clause}: {e}"))
            .iter()
            .map(|r| r.get::<i64, _>(0))
            .collect();
        v.sort_unstable();
        v
    }

    /// Stops the server and takes its logs out of the process-wide
    /// registry. Left there, the next server opened at the same path (the
    /// next image) would reuse the stopped one's `Wal`, its handles on
    /// files the image replaced and its notion of what is durable, and
    /// recover the wrong bytes: an acknowledged UPDATE came back lost.
    async fn stop(mut self) {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        drop(self.server.take());
        for dir in &self.logs {
            rasterizeddb_core::core::wal::remove_for(&dir.to_string_lossy());
        }
    }
}

/// Compares the subject with the state its rows match; returns whether it
/// loaded.
async fn check(s: &Subject, label: &str, states: &[Model], preds: &[Pred], expect: Expect, problems: &mut Vec<String>) -> bool {
    let loaded = s.vouched().await;
    match (expect, loaded) {
        (Expect::Load, false) => problems.push(format!("{label}: expected a load, the open rebuilt")),
        (Expect::Rebuild, true) => problems.push(format!("{label}: expected a rebuild, the open loaded")),
        _ => {}
    }
    match (s.origin(), loaded) {
        (Some(o), true) if o != Origin::Loaded => {
            problems.push(format!("{label}: the snapshot loaded, its INDEX sections did not ({o:?})"))
        }
        (Some(Origin::Loaded), false) => problems.push(format!("{label}: INDEX sections loaded from an unvouched snapshot")),
        _ => {}
    }
    s.ready().await;
    let got = s.scan().await;
    let Some(state) = states.iter().find(|m| **m == got) else {
        let diffs: Vec<String> = states
            .iter()
            .map(|m| {
                let changed: Vec<String> = got
                    .iter()
                    .filter(|(k, v)| m.get(k) != Some(v))
                    .take(6)
                    .map(|(k, v)| format!("{k}: got {v:?}, twin {:?}", m.get(k)))
                    .collect();
                let missing: Vec<i64> = m.keys().filter(|k| !got.contains_key(k)).take(6).copied().collect();
                format!("vs {} rows: changed {changed:?}, missing {missing:?}", m.len())
            })
            .collect();
        problems.push(format!(
            "{label}: the recovered rows ({}) match no state the twin passed through: {}",
            got.len(),
            diffs.join("; ")
        ));
        return loaded;
    };
    let mut preds = preds.to_vec();
    if s.origin().is_some() {
        // Every index asked for a sample of the image's own rows.
        for r in state.values().step_by(50) {
            preds.push(Pred::A(r.a));
            preds.push(Pred::S(r.s.clone()));
            preds.push(Pred::BA(r.b, r.a));
        }
    }
    for p in &preds {
        let want = answer(state, p);
        let have = s.ids(&p.sql()).await;
        if have != want {
            problems.push(format!(
                "{label} ({}): WHERE {}: {} ids, twin {}",
                if loaded { "loaded" } else { "rebuilt" },
                p.sql(),
                have.len(),
                want.len()
            ));
        }
    }
    loaded
}

/// A fresh subject with `ROWS` rows, some deleted and some updated in place;
/// `indexed`, with a B-tree on `a`, a hash on `s` and a B-tree on `(b, a)`,
/// under `index_storage = snapshot` (B17).
async fn populated(dirs: &Dirs, place: Place, table: &'static str, indexed: bool) -> (Subject, Model) {
    std::fs::create_dir_all(dirs.root()).unwrap();
    // The filter consulted from the first row, set before the table exists
    // so every open builds the configuration the snapshot's identity names.
    rasterizeddb_core::core::filter::settings::settings().update_table(table, |o| o.min_table_rows = Some(0));
    let s = Subject::start(dirs, table).await;
    s.exec(&format!("CREATE TABLE {table} (id BIGINT, a BIGINT, b BIGINT, s TEXT)")).await;
    if indexed {
        set_index_storage(IndexStorage::Snapshot);
        s.exec(&format!("CREATE INDEX {table}_a ON {table} (a)")).await;
        s.exec(&format!("CREATE INDEX {table}_s ON {table} USING hash (s)")).await;
        s.exec(&format!("CREATE INDEX {table}_ba ON {table} (b, a)")).await;
    }
    if place == Place::Second {
        std::fs::create_dir_all(dirs.disk()).unwrap();
        s.exec(&format!("STORAGE ADD LOCATION '{}'", dirs.disk().display())).await;
        s.exec(&format!("STORAGE MOVE TABLE {table} TO '{}'", dirs.disk().display())).await;
    }
    let mut model = Model::new();
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(250) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| {
                let r = ModelRow::for_id(*i);
                format!("({i}, {}, {}, '{}')", r.a, r.b, r.s)
            })
            .collect();
        s.exec(&format!("INSERT INTO {table} (id, a, b, s) VALUES {}", values.join(", "))).await;
        for i in chunk {
            model.insert(*i, ModelRow::for_id(*i));
        }
    }
    s.exec(&format!("DELETE FROM {table} WHERE id % 37 = 5")).await;
    model.retain(|id, _| id % 37 != 5);
    for id in [11i64, 222, 433, 644] {
        s.exec(&format!("UPDATE {table} SET a = {} WHERE id = {id}", 700_000 + id)).await;
        model.get_mut(&id).unwrap().a = 700_000 + id;
    }
    s.exec(&format!("VACUUM {table}")).await;
    s.ready().await;
    if place == Place::Second {
        let t = s.server.as_ref().unwrap().database().table_for_tests(table).unwrap();
        assert_eq!(t.gate().dir(), Some(dirs.disk().as_path()), "the table is not on the second location");
    }
    (s, model)
}

/// A directory under the temporary root, named by process and a counter,
/// removed on drop (a panic included).
struct TmpBase(PathBuf);

impl TmpBase {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tmp_base() -> TmpBase {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rdb_pi_compat_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a temporary directory");
    TmpBase(dir)
}

/// (a) A clean close and reopen: the filter loads from the snapshot (the
/// load counter moves), answers as the twin, and a write after the reopen
/// with an abandoned close rebuilds.
async fn clean_reopen(place: Place, table: &'static str, indexed: bool) {
    let tmp = tmp_base();
    let dirs = Dirs { base: tmp.path().join("subject") };
    let (s, mut model) = populated(&dirs, place, table, indexed).await;
    let answered = s.checkpoint().await;
    assert_eq!(answered, "1 of 1 clean", "rdb_index_checkpoint answered {answered}");
    s.stop().await;
    let loads = rasterizeddb_core::core::index_store::open_proof::loaded_total();
    let s = Subject::start(&dirs, table).await;
    let mut problems = Vec::new();
    let preds = battery(&[&model], &[]);
    let index_loads = rasterizeddb_core::core::index_store::load::loaded_total();
    let loaded = check(&s, &format!("{place:?} clean reopen"), &[model.clone()], &preds, Expect::Load, &mut problems).await;
    assert!(loaded && rasterizeddb_core::core::index_store::open_proof::loaded_total() > loads, "the reopen did not load");
    if indexed {
        // The load counter moved and this open built nothing.
        assert_eq!(s.origin(), Some(Origin::Loaded), "{place:?}: the clean reopen built its indexes");
        assert!(rasterizeddb_core::core::index_store::load::loaded_total() > index_loads, "the index load counter did not move");
    }
    s.exec(&format!("UPDATE {table} SET a = 999999 WHERE id = 20")).await;
    model.get_mut(&20).unwrap().a = 999_999;
    s.stop().await;
    let s = Subject::start(&dirs, table).await;
    let preds = battery(&[&model], &[model[&20].clone()]);
    check(&s, &format!("{place:?} reopen after a write"), &[model.clone()], &preds, Expect::Rebuild, &mut problems).await;
    s.stop().await;
    assert!(problems.is_empty(), "{place:?}: {}", problems.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_reopen_loads_the_filter_in_the_root() {
    clean_reopen(Place::Root, "pic_clean_root", false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_reopen_loads_the_filter_on_a_second_location() {
    clean_reopen(Place::Second, "pic_clean_disk", false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_reopen_loads_the_indexes_in_the_root() {
    clean_reopen(Place::Root, "pic_iclean_root", true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_reopen_loads_the_indexes_on_a_second_location() {
    clean_reopen(Place::Second, "pic_iclean_disk", true).await;
}

const SEAL_STEPS: &[(&str, Expect)] = &[
    ("snapshot_before_fsync", Expect::Rebuild),
    ("snapshot_after_fsync", Expect::Rebuild),
    ("snapshot_after_rename", Expect::Rebuild),
    ("snapshot_after_dir_fsync", Expect::Rebuild),
    ("marker_compaction_after_rename", Expect::Rebuild),
    ("clean_append_before_fsync", Expect::Either),
    ("clean_after_fsync", Expect::Load),
];

struct Point {
    cp: CrashPoint,
    states: Vec<Model>,
    preds: Vec<Pred>,
    expect: Expect,
}

fn arm_cuts(rec: &StdArc<Recorder>, names: &[&str], scope: &Path, sink: &image::CutSink) -> Vec<failpoint::Guard> {
    names.iter().map(|n| failpoint::arm(n, scope, 1, rec.cut_action(n.to_string(), sink.clone()))).collect()
}

fn collect(sink: &image::CutSink, states: &[&Model], preds: &[Pred], expect_of: impl Fn(&str) -> Expect, out: &mut Vec<Point>) {
    for cut in image::drain(sink) {
        match cut {
            Ok(cp) => {
                let expect = expect_of(&cp.label);
                out.push(Point { cp, states: states.iter().map(|m| (*m).clone()).collect(), preds: preds.to_vec(), expect });
            }
            Err(why) => eprintln!("{why}"),
        }
    }
}

/// (b) Power cuts over pgwire: every seal step of `rdb_index_checkpoint`,
/// the first write's `DIRTY` (unsynced and durable) and after it, a failed
/// `CLEAN` fsync followed by a length-preserving write, and, on the second
/// location, bug (c): a DELETE and its vacuum clear under
/// `synchronous_commit = off` sealed before the outcomes reach the root.
async fn power_cuts(place: Place, table: &'static str, indexed: bool) {
    let tmp = tmp_base();
    let dirs = Dirs { base: tmp.path().join("subject") };
    let (s, mut model) = populated(&dirs, place, table, indexed).await;
    let rec = Recorder::start(&dirs.base);
    let mut points: Vec<Point> = Vec::new();
    let sink = image::cut_sink();
    let step_names: Vec<&str> = SEAL_STEPS.iter().map(|(n, _)| *n).collect();
    let step_expect = |cut: &str| SEAL_STEPS.iter().find(|(n, _)| cut.starts_with(n)).map(|(_, e)| *e).unwrap_or(Expect::Either);

    // The first seal (no marker log yet).
    let preds = battery(&[&model], &[]);
    let guards = arm_cuts(&rec, &step_names, &dirs.base, &sink);
    assert_eq!(s.checkpoint().await, "1 of 1 clean");
    drop(guards);
    collect(&sink, &[&model], &preds, step_expect, &mut points);

    // The first write after it: an in-place UPDATE, cut in its DIRTY.
    let before = model.clone();
    let mut after = model.clone();
    after.get_mut(&30).unwrap().a = 812_345;
    let touched = vec![before[&30].clone(), after[&30].clone()];
    let preds = battery(&[&before, &after], &touched);
    let guards = arm_cuts(&rec, &["dirty_append_before_fsync", "dirty_after_fsync"], &dirs.base, &sink);
    s.exec(&format!("UPDATE {table} SET a = 812345 WHERE id = 30")).await;
    drop(guards);
    model = after.clone();
    collect(&sink, &[&before, &after], &preds, |c| if c.starts_with("dirty_after_fsync") { Expect::Rebuild } else { Expect::Either }, &mut points);
    if let Ok(cp) = rec.cut_quiescent("first_write_after_snapshot, after it returned") {
        points.push(Point { cp, states: vec![model.clone()], preds: preds.clone(), expect: Expect::Rebuild });
    }

    // A second seal over a log ending DIRTY and an old `.ixs`.
    let preds = battery(&[&model], &touched);
    let guards = arm_cuts(&rec, &step_names, &dirs.base, &sink);
    assert_eq!(s.checkpoint().await, "1 of 1 clean");
    drop(guards);
    collect(&sink, &[&model], &preds, step_expect, &mut points);
    if let Ok(cp) = rec.cut_quiescent("after the second seal") {
        points.push(Point { cp, states: vec![model.clone()], preds: preds.clone(), expect: Expect::Load });
    }

    // clean_fsync_fails, then a length-preserving write: the word was
    // Unknown, so the write rewrote the log first.
    s.exec(&format!("UPDATE {table} SET a = 812346 WHERE id = 31")).await;
    model.get_mut(&31).unwrap().a = 812_346;
    let fail = failpoint::arm(
        "clean_fsync_fails",
        &dirs.base,
        1,
        StdArc::new(|_: &Hit<'_>| Outcome::Fail(std::io::Error::from_raw_os_error(5))),
    );
    let answered = s.checkpoint().await;
    assert_eq!(fail.fired(), 1, "clean_fsync_fails never fired ({answered})");
    drop(fail);
    s.exec(&format!("UPDATE {table} SET a = 812347 WHERE id = 32")).await;
    model.get_mut(&32).unwrap().a = 812_347;
    let touched: Vec<ModelRow> = [31i64, 32].iter().map(|i| model[i].clone()).collect();
    let preds = battery(&[&model], &touched);
    if let Ok(cp) = rec.cut_quiescent("clean_fsync_fails, then a length-preserving write") {
        points.push(Point { cp, states: vec![model.clone()], preds: preds.clone(), expect: Expect::Rebuild });
    }

    if place == Place::Second {
        // Bug (c): synchronous_commit = off, a DELETE, the vacuum that clears
        // its codes, a seal: each image a prefix of the two states.
        assert_eq!(s.checkpoint().await, "1 of 1 clean");
        let mut conn = s.pool().acquire().await.unwrap();
        sqlx::query("SET synchronous_commit = off").execute(&mut *conn).await.unwrap();
        let s0 = model.clone();
        sqlx::query(AssertSqlSafe(format!("DELETE FROM {table} WHERE id IN (40, 41, 42, 43)")))
            .execute(&mut *conn)
            .await
            .unwrap();
        let mut s1 = model.clone();
        let touched: Vec<ModelRow> = [40i64, 41, 42, 43].iter().filter_map(|i| s1.remove(i)).collect();
        sqlx::query(AssertSqlSafe(format!("VACUUM {table}"))).execute(&mut *conn).await.unwrap();
        drop(conn);
        model = s1.clone();
        let preds = battery(&[&s0, &s1], &touched);
        let guards = arm_cuts(&rec, &step_names, &dirs.base, &sink);
        assert_eq!(s.checkpoint().await, "1 of 1 clean");
        drop(guards);
        collect(&sink, &[&s0, &s1], &preds, step_expect, &mut points);
    }
    s.stop().await;
    drop(rec);

    let mut problems = Vec::new();
    let (mut loads, mut images) = (0usize, 0usize);
    let img = Dirs { base: tmp.path().join("image") };
    for p in &points {
        for seed in 0..seeds() {
            let _ = std::fs::remove_dir_all(&img.base);
            p.cp.write_image(seed, &img.base);
            let s = Subject::start(&img, table).await;
            let label = format!("{place:?} {} seed {seed}", p.cp.label);
            loads += check(&s, &label, &p.states, &p.preds, p.expect, &mut problems).await as usize;
            s.stop().await;
            images += 1;
        }
    }
    let _ = std::fs::remove_dir_all(&img.base);
    eprintln!("{place:?}: {} points, {images} images, {loads} loaded", points.len());
    assert!(loads > 0 && loads < images, "{place:?}: the images missed one side ({loads} of {images} loaded)");
    assert!(
        problems.is_empty(),
        "{place:?}: {} problems:\n{}",
        problems.len(),
        problems.iter().take(30).cloned().collect::<Vec<_>>().join("\n")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_filter_survives_power_cuts_over_pgwire_in_the_root() {
    power_cuts(Place::Root, "pic_cuts_root", false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_filter_survives_power_cuts_over_pgwire_on_a_second_location() {
    power_cuts(Place::Second, "pic_cuts_disk", false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_indexes_survive_power_cuts_over_pgwire_in_the_root() {
    power_cuts(Place::Root, "pic_icuts_root", true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_indexes_survive_power_cuts_over_pgwire_on_a_second_location() {
    power_cuts(Place::Second, "pic_icuts_disk", true).await;
}

/// A server built with the test-only features refuses to start: the
/// binary checks this list first (`main.rs`).
#[test]
fn the_failpoint_feature_is_named_among_the_test_only_features() {
    assert!(rasterizeddb_core::TEST_ONLY_FEATURES.contains(&"index_store_failpoints"));
}
