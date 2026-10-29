//! Owner's invariant 8, against today's rebuild-at-open: the index twin
//! suites' core scenarios through (a) clean restarts and (b) crash images.
//!
//! Three workloads, each in the shape of an existing suite:
//! - `secondary`: `compat/tests/secondary_indexes.rs` (B-tree, hash,
//!   composite; inserts, in-place and relocating updates, deletes, rolled
//!   back blocks, `VACUUM`, `VACUUM FULL`);
//! - `json`: `compat/tests/json_indexes.rs` (gin `jsonb_ops` and
//!   `jsonb_path_ops`, path indexes, a hash over the document; numbers
//!   spelled several ways, `NULL` documents, arrays and bare scalars);
//! - `superset`: `index/tests.rs`'s randomized superset test, on a real
//!   table: B-tree, hash and composite layouts led by each type, `-0.0`
//!   beside `0.0`, random `AND`/`OR` trees.
//!
//! The database under test holds the indexed table. **The twin is a
//! separate database that is never restarted or imaged**, holding the same
//! table with no index, fed only statements the subject acknowledged. Every
//! check compares the subject's answers (indexes consulted, asserted through
//! `build_candidates`) with the twin's, and checks containment: every row a
//! fresh snapshot can see is under its key in every index (or unkeyed), and
//! a gin lookup of its own document finds it. Answers alone cannot show an
//! entry a query did not exercise.
//!
//! Cuts: between statements (nothing in flight), and inside a statement at
//! the `nth` hit of a `sync:<tag>` failpoint (the statement may or may not
//! survive). The twin's rows and answers are taken before and after each
//! cut statement; an image's rows must equal one of the two, and its answers
//! must then equal that state's exactly (one state per cut, never per
//! query). Every acknowledged statement before the cut must have survived.
//!
//! Bounded: a few hundred rows, `RDB_POWER_LOSS_SEEDS` images per cut
//! (default 5, one per seed kind), `RDB_POWER_LOSS_RUNS` workload seeds
//! (default 1), and the bytes a run holds are checked against a ceiling.

use std::path::{Path, PathBuf};

use rclite::Arc;

use super::failpoint;
use super::image::{CrashPoint, Recorder, Rng};
use crate::core::database::Database;
use crate::core::mvcc::status::read_reservation;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointerIterator};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

const TABLE: &str = "ix";
/// Ceiling on what a run may hold, crash points in memory and directories
/// on disk together. The images land in the shared tmpfs.
const MAX_RUN_BYTES: u64 = 256 * 1024 * 1024;
const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn env_count(name: &str, default: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
        .clamp(1, max)
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path, name: &str) -> Result<Arc<Database>, String> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Database::try_new_named(&path_str(dir), false, name).await.map(Arc::new)
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

async fn exec_in(db: &Arc<Database>, tx: crate::core::mvcc::TxId, sql: &str) -> Result<(), String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(())
}

fn decode(out: &[u8]) -> Result<Vec<Row>, String> {
    if out.first() != Some(&2) {
        return Err(format!("not a row set: {}", String::from_utf8_lossy(out)));
    }
    crate::core::row::row::vec_into_rows(&out[1..]).map_err(|e| e.to_string())
}

/// Every row, as its column bytes, sorted.
async fn rows(db: &Arc<Database>, select: &str) -> Result<Vec<Vec<Vec<u8>>>, String> {
    let mut out: Vec<Vec<Vec<u8>>> = decode(&exec(db, select).await?)?
        .iter()
        .map(|r| r.columns.iter().map(|c| c.data.into_slice().to_vec()).collect())
        .collect();
    out.sort();
    Ok(out)
}

/// The ids a predicate selects, sorted.
async fn ids(db: &Arc<Database>, clause: &str) -> Result<Vec<i64>, String> {
    let sql = format!("SELECT id FROM {TABLE} WHERE {clause}");
    let mut out: Vec<i64> = decode(&exec(db, &sql).await?)?
        .iter()
        .map(|r| {
            let b = r.columns[0].data.into_slice();
            i64::from_le_bytes(b.try_into().unwrap_or([0xFF; 8]))
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Secondary,
    Json,
    Superset,
}

/// One statement, the same on both databases.
#[derive(Debug, Clone)]
enum Stmt {
    Auto(String),
    /// Written in a block and rolled back: the versions exist, stay
    /// invisible, and vacuum later forgets them.
    RolledBack(Vec<String>),
}

struct Workload {
    kind: Kind,
    r: Rng,
    next_id: i64,
}

const STATUSES: [&str; 5] = ["new", "paid", "shipped", "delivered", "cancelled"];
const BIG: [&str; 12] = [
    "1e400", "10e399", "1.000000000000000000001", "1", "1.0", "9007199254740993", "9007199254740993.0",
    "9.007199254740993e15", "-0", "0.0", "1e60", "10e59",
];

impl Workload {
    fn new(kind: Kind, seed: u64) -> Workload {
        Workload {
            kind,
            r: Rng::new(seed),
            next_id: 0,
        }
    }

    fn below(&mut self, n: u64) -> u64 {
        self.r.below(n)
    }

    fn create(&self, indexed: bool) -> Vec<String> {
        let mut out = match (self.kind, indexed) {
            (Kind::Secondary, _) => vec![format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, n INTEGER NOT NULL, s TEXT NOT NULL)")],
            (Kind::Json, true) => vec![format!("CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, doc JSONB)")],
            (Kind::Json, false) => vec![format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, doc JSONB)")],
            (Kind::Superset, _) => vec![format!(
                "CREATE TABLE {TABLE} (id BIGINT NOT NULL, n BIGINT NOT NULL, s TEXT NOT NULL, f DOUBLE PRECISION NOT NULL)"
            )],
        };
        if !indexed {
            return out;
        }
        let ddl: &[&str] = match self.kind {
            Kind::Secondary => &[
                "CREATE INDEX ix_n ON ix (n)",
                "CREATE INDEX ix_s ON ix USING hash (s)",
                "CREATE INDEX ix_sn ON ix (s, n)",
                "CREATE INDEX ix_id ON ix (id)",
            ],
            Kind::Json => &[
                "CREATE INDEX ix_ops ON ix USING gin (doc)",
                "CREATE INDEX ix_path ON ix USING gin (doc jsonb_path_ops)",
                "CREATE INDEX ix_status ON ix ((doc->>'status'))",
                "CREATE INDEX ix_uname ON ix ((doc->'user'->>'name'))",
                "CREATE INDEX ix_label ON ix ((doc->>'label'))",
                "CREATE INDEX ix_whole ON ix USING hash (doc)",
                "CREATE INDEX ix_cat ON ix ((doc->>'cat'))",
                "CREATE INDEX ix_big ON ix ((doc->>'big'))",
            ],
            Kind::Superset => &[
                "CREATE INDEX ix_n ON ix (n)",
                "CREATE INDEX ix_s ON ix (s)",
                "CREATE INDEX ix_fh ON ix USING hash (f)",
                "CREATE INDEX ix_ns ON ix (n, s)",
                "CREATE INDEX ix_sn ON ix (s, n)",
                "CREATE INDEX ix_fs ON ix (f, s)",
                "CREATE INDEX ix_nsf ON ix USING hash (n, s, f)",
                "CREATE INDEX ix_nh ON ix USING hash (n)",
            ],
        };
        out.extend(ddl.iter().map(|s| s.to_string()));
        out
    }

    fn row_select(&self) -> String {
        match self.kind {
            Kind::Secondary => format!("SELECT id, n, s FROM {TABLE}"),
            Kind::Json => format!("SELECT id, doc FROM {TABLE}"),
            Kind::Superset => format!("SELECT id, n, s, f FROM {TABLE}"),
        }
    }

    // --- values -----------------------------------------------------------

    fn word(&mut self) -> String {
        let w = ["alpha", "beta", "gamma", "delta", "al", "b", ""];
        let i = self.below(w.len() as u64) as usize;
        format!("{}{}", w[i], self.below(4))
    }

    fn short(&mut self) -> String {
        let n = self.below(4) as usize;
        (0..n).map(|_| if self.below(2) == 0 { 'a' } else { 'b' }).collect()
    }

    fn float(&mut self) -> &'static str {
        ["0.0", "-0.0", "1.5", "-1.5", "2.0", "2.5"][self.below(6) as usize]
    }

    fn number(&mut self, n: u64) -> String {
        match self.below(6) {
            0 => format!("{n}"),
            1 => format!("{n}.0"),
            2 => format!("{n}e0"),
            3 => format!("{n}.00"),
            4 => format!("{}e-1", n * 10),
            _ => format!("0.{n}e1"),
        }
    }

    fn document(&mut self) -> String {
        match self.below(20) {
            0 => return "NULL".into(),
            1 => {
                let (t, n) = (self.below(6), self.below(4));
                return format!("'[\"t{t}\", {}]'", self.number(n));
            }
            2 => return format!("'\"{}\"'", STATUSES[self.below(5) as usize]),
            _ => {}
        }
        let status = STATUSES[self.below(5) as usize];
        let cat = self.below(10);
        let cat = self.number(cat);
        let (name, uid) = (self.below(30), self.below(30));
        let uid = self.number(uid);
        let mut fields = vec![
            format!("\"status\":\"{status}\""),
            format!("\"cat\":{cat}"),
            format!("\"user\":{{\"name\":\"u{name}\",\"id\":{uid}}}"),
        ];
        let tags: Vec<String> = (0..self.below(4)).map(|_| format!("\"t{}\"", self.below(6))).collect();
        fields.push(format!("\"tags\":[{}]", tags.join(",")));
        if self.below(10) == 0 {
            fields.push("\"promo\":true".into());
        }
        if self.below(3) == 0 {
            fields.push(format!("\"big\":{}", BIG[self.below(BIG.len() as u64) as usize]));
        }
        if self.below(8) == 0 {
            fields.push(format!("\"label\":{}", self.below(3)));
        } else if self.below(4) == 0 {
            fields.push("\"label\":null".into());
        }
        format!("'{{{}}}'", fields.join(","))
    }

    fn row_values(&mut self) -> String {
        self.next_id += 1;
        let id = self.next_id;
        match self.kind {
            Kind::Secondary => {
                let n = self.below(30) as i64 - 5;
                format!("({id}, {n}, '{}')", self.word())
            }
            Kind::Json => format!("({id}, {})", self.document()),
            Kind::Superset => {
                let n = self.below(41) as i64 - 20;
                let s = self.short();
                format!("({id}, {n}, '{s}', {})", self.float())
            }
        }
    }

    fn columns(&self) -> &'static str {
        match self.kind {
            Kind::Secondary => "(id, n, s)",
            Kind::Json => "(id, doc)",
            Kind::Superset => "(id, n, s, f)",
        }
    }

    fn insert(&mut self, n: usize) -> String {
        let rows: Vec<String> = (0..n).map(|_| self.row_values()).collect();
        format!("INSERT INTO {TABLE} {} VALUES {}", self.columns(), rows.join(", "))
    }

    // --- statements ---------------------------------------------------------

    fn step(&mut self) -> Stmt {
        let id = self.below(self.next_id as u64 + 1);
        let s = match (self.kind, self.below(12)) {
            (_, 0..=3) => {
                let n = 1 + self.below(4) as usize;
                self.insert(n)
            }
            (Kind::Secondary, 4 | 5) => {
                // Fixed width: in place when nothing else reads.
                format!("UPDATE {TABLE} SET n = {} WHERE id = {id}", self.below(30) as i64 - 5)
            }
            (Kind::Secondary, 6) => {
                let w = self.word();
                format!("UPDATE {TABLE} SET s = '{w}' WHERE n = {}", self.below(30) as i64 - 5)
            }
            (Kind::Json, 4..=6) => format!("UPDATE {TABLE} SET doc = {} WHERE id = {id}", self.document()),
            (Kind::Superset, 4 | 5) => format!("UPDATE {TABLE} SET n = {} WHERE id = {id}", self.below(41) as i64 - 20),
            (Kind::Superset, 6) => {
                let s = self.short();
                format!("UPDATE {TABLE} SET s = '{s}', f = {} WHERE id = {id}", self.float())
            }
            (_, 7) => format!("DELETE FROM {TABLE} WHERE id = {id}"),
            (Kind::Secondary, 8) => format!("DELETE FROM {TABLE} WHERE n = {}", self.below(30) as i64 - 5),
            (Kind::Superset, 8) => format!("DELETE FROM {TABLE} WHERE n = {}", self.below(41) as i64 - 20),
            (Kind::Json, 8) => format!("DELETE FROM {TABLE} WHERE doc @> '{{\"status\":\"{}\"}}'", STATUSES[self.below(5) as usize]),
            (_, 9) => {
                let ins = self.insert(1);
                let upd = match self.kind {
                    Kind::Json => format!("UPDATE {TABLE} SET doc = '{{\"status\":\"rolled\"}}' WHERE id = {id}"),
                    _ => format!("UPDATE {TABLE} SET n = 99 WHERE id = {id}"),
                };
                return Stmt::RolledBack(vec![ins, upd]);
            }
            (_, 10) => format!("VACUUM {TABLE}"),
            _ => {
                if self.below(6) == 0 {
                    format!("VACUUM FULL {TABLE}")
                } else {
                    self.insert(1)
                }
            }
        };
        Stmt::Auto(s)
    }

    // --- predicates ---------------------------------------------------------

    fn predicate(&mut self) -> String {
        match self.kind {
            Kind::Secondary => self.secondary_predicate(),
            Kind::Json => self.json_predicate(),
            Kind::Superset => self.superset_expr(3),
        }
    }

    fn secondary_atom(&mut self) -> String {
        let n = self.below(30) as i64 - 5;
        match self.below(16) {
            0 => format!("n = {n}"),
            1 => format!("n > {n}"),
            2 => format!("n <= {n}"),
            3 => format!("n BETWEEN {} AND {}", n, n + self.below(6) as i64),
            4 => format!("n IN ({}, {}, {})", self.below(30), self.below(30), self.below(30)),
            5 => format!("s = '{}'", self.word()),
            6 => format!("s >= '{}'", self.word()),
            7 => {
                let w = self.word();
                format!("s LIKE '{}%'", w.chars().next().map(String::from).unwrap_or_default())
            }
            8 => format!("n % 7 = {}", self.below(7)),
            9 => format!("s || 'x' = '{}x'", self.word()),
            10 => format!("NOT (n % 3 = {})", self.below(3)),
            11 => format!("(n, s) = ({n}, '{}')", self.word()),
            12 => format!("n IS NOT DISTINCT FROM {n}"),
            13 => format!("s = '{}' AND n = {n}", self.word()),
            _ => format!("id = {}", self.below(self.next_id as u64 + 2)),
        }
    }

    fn secondary_predicate(&mut self) -> String {
        match self.below(4) {
            0 => self.secondary_atom(),
            1 => format!("{} AND {}", self.secondary_atom(), self.secondary_atom()),
            2 => format!("({} OR {})", self.secondary_atom(), self.secondary_atom()),
            _ => format!(
                "{} AND ({} OR {})",
                self.secondary_atom(),
                self.secondary_atom(),
                self.secondary_atom()
            ),
        }
    }

    fn json_atom(&mut self, depth: u32) -> String {
        let st = STATUSES[self.below(5) as usize];
        match self.below(24) {
            0 if depth > 0 => format!("NOT ({})", self.json_atom(depth - 1)),
            0 | 1 => format!("doc @> '{{\"status\":\"{st}\"}}'"),
            2 => {
                let n = self.below(10);
                format!("doc @> '{{\"cat\":{}}}'", self.number(n))
            }
            3 => format!("doc @> '{{\"user\":{{\"name\":\"u{}\"}}}}'", self.below(30)),
            4 => format!("doc @> '{{\"tags\":[\"t{}\"]}}'", self.below(6)),
            5 => format!("doc @> '[\"t{}\"]'", self.below(6)),
            6 => format!("doc @> '\"t{}\"'", self.below(6)),
            7 => "doc ? 'promo'".into(),
            8 => "doc ?& array['status', 'promo']".into(),
            9 => format!("doc->>'status' = '{st}'"),
            10 => format!("doc->'user'->>'name' = 'u{}'", self.below(30)),
            11 => format!("doc->>'status' > '{st}'"),
            12 => format!("doc->>'label' = '{}'", self.below(3)),
            13 => format!("doc @> '{{\"big\":{}}}'", BIG[self.below(BIG.len() as u64) as usize]),
            14 => format!("doc->>'big' = '{}'", ["1", "1.0", "0", "-0", "1e400", "1e60"][self.below(6) as usize]),
            15 => {
                let n = self.below(10);
                format!("doc->>'cat' = '{}'", self.number(n))
            }
            16 => {
                let n = self.below(10);
                format!("doc->'cat' = '{}'", self.number(n))
            }
            17 => format!("doc = '\"{st}\"'"),
            18 => format!("doc->>'status' IN ('{st}', '{}')", STATUSES[self.below(5) as usize]),
            19 => format!(
                "{} IS {}NULL",
                ["doc->>'label'", "doc->'label'", "doc->>'promo'"][self.below(3) as usize],
                if self.below(2) == 0 { "NOT " } else { "" }
            ),
            20 => format!("doc->>'status' || '!' = '{st}!'"),
            21 => format!("doc->>'label' {} ''", ["=", "<>", ">="][self.below(3) as usize]),
            22 => format!("doc @> '{{\"status\":\"{st}\",\"cat\":{}}}'", self.below(10)),
            _ => format!("id < {}", self.below(self.next_id as u64 + 2)),
        }
    }

    fn json_predicate(&mut self) -> String {
        match self.below(4) {
            0 => format!("{} AND {}", self.json_atom(1), self.json_atom(1)),
            1 => format!("({} OR {})", self.json_atom(1), self.json_atom(1)),
            _ => self.json_atom(1),
        }
    }

    fn superset_pred(&mut self) -> String {
        let op = ["=", "=", ">", ">=", "<", "<="][self.below(6) as usize];
        match self.below(3) {
            0 => format!("n {op} {}", self.below(41) as i64 - 20),
            1 => format!("s {op} '{}'", self.short()),
            _ => format!("f {op} {}", self.float()),
        }
    }

    fn superset_expr(&mut self, depth: u32) -> String {
        if depth == 0 || self.below(3) == 0 {
            return self.superset_pred();
        }
        let n = 2 + self.below(2) as usize;
        let kids: Vec<String> = (0..n).map(|_| self.superset_expr(depth - 1)).collect();
        let joiner = if self.below(2) == 0 { " AND " } else { " OR " };
        format!("({})", kids.join(joiner))
    }
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// The twin's rows and answers at one instant. An answer is `None` for a
/// predicate the twin refused, which is then asked of nobody.
#[derive(Clone)]
struct Expect {
    rows: Vec<Vec<Vec<u8>>>,
    answers: Vec<(String, Option<Vec<i64>>)>,
}

async fn expect(twin: &Arc<Database>, w: &Workload, preds: &[String]) -> Expect {
    let rows = rows(twin, &w.row_select()).await.unwrap_or_else(|e| panic!("twin rows: {e}"));
    let mut answers = Vec::new();
    for p in preds {
        answers.push((p.clone(), ids(twin, p).await.ok()));
    }
    Expect { rows, answers }
}

async fn apply(db: &Arc<Database>, stmt: &Stmt) -> Result<(), String> {
    match stmt {
        Stmt::Auto(sql) => exec(db, sql).await.map(|_| ()),
        Stmt::RolledBack(sqls) => {
            let tx = db.transactions.begin();
            let mut out = Ok(());
            for sql in sqls {
                if let Err(e) = exec_in(db, tx, sql).await {
                    out = Err(e);
                    break;
                }
            }
            db.abort_transaction(tx);
            out
        }
    }
}

/// Every row a fresh snapshot sees, under its key in every index (or
/// unkeyed), and found by a gin lookup of its own document.
async fn containment(t: &Table<DbStorageProvider>) -> Result<Vec<String>, String> {
    let d = t.data().map_err(|e| e.to_string())?;
    let Some(ix) = d.indexes.as_ref() else {
        return Err("the table has no indexes".into());
    };
    let _read = d.readers.enter();
    let visibility = t.visibility_for(FROZEN_TX);
    let fetch = Table::<DbStorageProvider>::build_fetch_for(&t.schema.fields, &ix.columns());
    let mut it = RowPointerIterator::new(d.io_pointers.clone()).await.map_err(|e| format!("{e:?}"))?;
    let mut row = Row::default();
    let mut problems = Vec::new();
    let mut seen = 0usize;
    while let Some((offset, pointer)) = it.next_row_pointer_with_offset().await.map_err(|e| format!("{e:?}"))? {
        if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
            continue;
        }
        let slot = offset / ROW_POINTER_RECORD_LEN as u64;
        if pointer.fetch_row_reuse_async(&*d.io_rows, &fetch, &mut row).await.is_err() {
            problems.push(format!("visible slot {slot} could not be read"));
            continue;
        }
        seen += 1;
        let get = |wo: u32| row.columns.iter().find(|c| c.schema_id as u32 == wo).map(|c| c.data.into_slice());
        for li in &ix.list {
            let key = li.key_of(get);
            let listed = li.slots_for_key(key.as_deref().unwrap_or(&[]));
            if listed.binary_search(&slot).is_err() {
                problems.push(format!("index {} lost visible slot {slot} (key {key:?})", li.decl.name));
            }
        }
        for g in &ix.gin {
            let Some(doc) = get(g.column).filter(|d| !d.is_empty()) else { continue };
            let Some(query) = crate::core::index::gin::contains_query(doc, g.opclass) else { continue };
            if let Some(found) = g.lookup(&query, usize::MAX)
                && found.binary_search(&slot).is_err()
            {
                problems.push(format!("gin index {} lost visible slot {slot}", g.decl.name));
            }
        }
    }
    if seen == 0 {
        problems.push("containment saw no visible row".into());
    }
    Ok(problems)
}

/// Waits for the indexes, then compares rows, answers and containment with
/// whichever of `states` the rows match. Returns the predicates an index
/// narrowed.
async fn check(db: &Arc<Database>, w: &Workload, label: &str, states: &[&Expect], problems: &mut Vec<String>) -> usize {
    let Some(t) = db.table_for_tests(TABLE) else {
        problems.push(format!("{label}: the table is gone"));
        return 0;
    };
    let start = std::time::Instant::now();
    while !t.data().is_ok_and(|d| d.indexes.as_ref().is_some_and(|i| i.is_ready())) {
        if start.elapsed() > READY_TIMEOUT {
            problems.push(format!("{label}: the indexes never became ready"));
            return 0;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let got = match rows(db, &w.row_select()).await {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!("{label}: rows: {e}"));
            return 0;
        }
    };
    let Some(state) = states.iter().find(|s| s.rows == got) else {
        let id = |r: &Vec<Vec<u8>>| i64::from_le_bytes(r[0].as_slice().try_into().unwrap_or([0xFF; 8]));
        let diffs: Vec<String> = states
            .iter()
            .map(|s| {
                let extra: Vec<i64> = got.iter().filter(|r| !s.rows.contains(r)).map(id).collect();
                let missing: Vec<i64> = s.rows.iter().filter(|r| !got.contains(r)).map(id).collect();
                format!("vs {} twin rows: extra or changed ids {extra:?}, missing or changed ids {missing:?}", s.rows.len())
            })
            .collect();
        problems.push(format!(
            "{label}: the recovered rows ({}) match no state the twin passed through: {}",
            got.len(),
            diffs.join("; ")
        ));
        return 0;
    };
    let mut narrowed = 0;
    for (p, want) in &state.answers {
        let Some(want) = want else { continue };
        match ids(db, p).await {
            Ok(got) if &got == want => {}
            Ok(got) => problems.push(format!("{label}: WHERE {p}: indexed {got:?}, twin {want:?}")),
            Err(e) => problems.push(format!("{label}: WHERE {p}: {e}")),
        }
        if t.build_candidates(p, None).await.ok().flatten().is_some_and(|c| !c.is_nothing()) {
            narrowed += 1;
        }
    }
    match containment(&t).await {
        Ok(found) => problems.extend(found.into_iter().map(|f| format!("{label}: {f}"))),
        Err(e) => problems.push(format!("{label}: containment: {e}")),
    }
    narrowed
}

// ---------------------------------------------------------------------------
// A run
// ---------------------------------------------------------------------------

struct Point {
    cp: CrashPoint,
    before: Expect,
    after: Option<Expect>,
}

#[derive(Default, Debug)]
struct Tally {
    quiescent_cuts: usize,
    sync_cuts: usize,
    disturbed: usize,
    restarts: usize,
    images: usize,
    narrowed: usize,
    checked: usize,
}

const SYNC_SITES: &[&str] = &[
    "wal.log",
    "wal.log",
    "file.data",
    "file.data",
    "dir",
    "wal.checkpoint.data_file",
    "wal.truncate",
    "wal.keep_tail",
    "wal.lsn_mark",
];

fn dir_bytes(dir: &Path) -> u64 {
    super::image::walk(dir)
        .iter()
        .map(|r| std::fs::metadata(dir.join(r)).map(|m| m.len()).unwrap_or(0))
        .sum()
}

async fn run(kind: Kind, run_seed: u64) -> Tally {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let mut w = Workload::new(kind, 0x7769_6e00 ^ run_seed ^ ((kind as u64) << 40));
    let mut plan = Rng::new(run_seed.wrapping_add(kind as u64 + 1));
    let steps: usize = match kind {
        Kind::Secondary => 150,
        Kind::Json => 110,
        Kind::Superset => 110,
    };
    let pick = |plan: &mut Rng, n: usize| -> Vec<usize> {
        let mut v: Vec<usize> = (0..n).map(|_| 5 + plan.below(steps as u64 - 5) as usize).collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    let quiescent = pick(&mut plan, 3);
    let sync_cut = pick(&mut plan, 6);
    let restarts = pick(&mut plan, 2);
    eprintln!("{kind:?}: restarts at {restarts:?}, quiescent cuts at {quiescent:?}, sync cuts at {sync_cut:?}");
    // A cut inside a `VACUUM FULL`, the statement that renumbers slots.
    let full_at = 5 + plan.below(steps as u64 - 5) as usize;

    let base = tempfile::tempdir().unwrap();
    let dut_dir = base.path().join("subject");
    let twin_dir = base.path().join("twin");
    let name = format!("pl_{kind:?}").to_lowercase();
    let mut dut = open(&dut_dir, &name).await.unwrap();
    let twin = open(&twin_dir, &format!("{name}_twin")).await.unwrap();
    for sql in w.create(true) {
        exec(&dut, &sql).await.unwrap();
    }
    for sql in w.create(false) {
        exec(&twin, &sql).await.unwrap();
    }
    for _ in 0..8 {
        let ins = Stmt::Auto(w.insert(5));
        apply(&dut, &ins).await.unwrap();
        apply(&twin, &ins).await.unwrap();
    }
    dut.sync_durable().unwrap();
    let rec = Recorder::start(&dut_dir);

    let mut tally = Tally::default();
    let mut problems: Vec<String> = Vec::new();
    let mut points: Vec<Point> = Vec::new();
    let mut held = 0u64;

    for step in 0..steps {
        let stmt = if step == full_at {
            Stmt::Auto(format!("VACUUM FULL {TABLE}"))
        } else {
            w.step()
        };
        let cut_here = sync_cut.contains(&step) || step == full_at;
        if cut_here {
            let site = if step == full_at {
                ["file.data", "wal.checkpoint.data_file", "wal.log"][plan.below(3) as usize]
            } else {
                SYNC_SITES[plan.below(SYNC_SITES.len() as u64) as usize]
            };
            let nth = 1 + plan.below(3);
            let preds: Vec<String> = (0..16).map(|_| w.predicate()).collect();
            let before = expect(&twin, &w, &preds).await;
            let sink = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let label = format!("{kind:?} run {run_seed} step {step} `{stmt:?}`");
            let guard = failpoint::arm(&format!("sync:{site}"), &dut_dir, nth, rec.cut_action(label, sink.clone()));
            let applied = apply(&dut, &stmt).await;
            drop(guard);
            if let Err(e) = applied {
                problems.push(format!("step {step}: the subject refused {stmt:?}: {e}"));
                break;
            }
            apply(&twin, &stmt).await.unwrap_or_else(|e| panic!("twin refused {stmt:?}: {e}"));
            let after = expect(&twin, &w, &preds).await;
            for cut in std::mem::take(&mut *sink.lock()) {
                match cut {
                    Ok(cp) => {
                        tally.sync_cuts += 1;
                        held += cp.held_bytes();
                        points.push(Point {
                            cp,
                            before: before.clone(),
                            after: Some(after.clone()),
                        });
                    }
                    Err(why) => {
                        eprintln!("{why}");
                        tally.disturbed += 1;
                    }
                }
            }
        } else {
            if let Err(e) = apply(&dut, &stmt).await {
                problems.push(format!("step {step}: the subject refused {stmt:?}: {e}"));
                break;
            }
            apply(&twin, &stmt).await.unwrap_or_else(|e| panic!("twin refused {stmt:?}: {e}"));
        }

        if quiescent.contains(&step) {
            let preds: Vec<String> = (0..16).map(|_| w.predicate()).collect();
            let now = expect(&twin, &w, &preds).await;
            // The never-restarted subject first: the harness's own baseline.
            tally.narrowed += check(&dut, &w, &format!("live, step {step}"), &[&now], &mut problems).await;
            match rec.cut_quiescent(format!("{kind:?} run {run_seed} after step {step}")) {
                Ok(cp) => {
                    tally.quiescent_cuts += 1;
                    held += cp.held_bytes();
                    points.push(Point {
                        cp,
                        before: now,
                        after: None,
                    });
                }
                Err(why) => {
                    eprintln!("{why}");
                    tally.disturbed += 1;
                }
            }
        }

        if restarts.contains(&step) {
            close(&dut_dir, dut);
            dut = open(&dut_dir, &name).await.unwrap_or_else(|e| panic!("clean restart refused: {e}"));
            tally.restarts += 1;
            let preds: Vec<String> = (0..24).map(|_| w.predicate()).collect();
            let now = expect(&twin, &w, &preds).await;
            tally.narrowed += check(&dut, &w, &format!("clean restart after step {step}"), &[&now], &mut problems).await;
        }
        assert!(
            held + dir_bytes(base.path()) < MAX_RUN_BYTES,
            "the run holds {} bytes, past its bound",
            held + dir_bytes(base.path())
        );
    }

    // A last clean restart, then the images.
    close(&dut_dir, dut);
    drop(rec);
    let dut = open(&dut_dir, &name).await.unwrap_or_else(|e| panic!("clean restart refused: {e}"));
    tally.restarts += 1;
    let preds: Vec<String> = (0..24).map(|_| w.predicate()).collect();
    let now = expect(&twin, &w, &preds).await;
    tally.narrowed += check(&dut, &w, "final clean restart", &[&now], &mut problems).await;
    close(&dut_dir, dut);

    let img_dir: PathBuf = base.path().join("image");
    for point in &points {
        for seed in 0..seeds {
            point.cp.write_image(seed, &img_dir);
            let label = format!(
                "{} seed {seed} (in flight at the cut: {:?})",
                point.cp.label, point.cp.in_flight
            );
            match open(&img_dir, &name).await {
                Ok(db) => {
                    let mut states = vec![&point.before];
                    if let Some(after) = &point.after {
                        states.push(after);
                    }
                    let before = problems.len();
                    tally.narrowed += check(&db, &w, &label, &states, &mut problems).await;
                    if problems.len() > before && std::env::var_os("RDB_POWER_LOSS_DESCRIBE").is_some() {
                        eprintln!("{label}:{}", point.cp.describe());
                    }
                    close(&img_dir, db);
                }
                Err(e) => {
                    problems.push(format!("{label}: the image would not open: {e}"));
                    crate::core::wal::remove_for(&path_str(&img_dir));
                }
            }
            tally.images += 1;
            let _ = std::fs::remove_dir_all(&img_dir);
        }
    }
    close(&twin_dir, twin);
    tally.checked = tally.restarts + tally.images + tally.quiescent_cuts;
    assert!(
        problems.is_empty(),
        "{kind:?} run {run_seed}: {} problems ({tally:?}):\n{}",
        problems.len(),
        problems.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
    eprintln!("{kind:?} run {run_seed}: {tally:?}");
    tally
}

async fn suite(kind: Kind) {
    use crate::core::index::{GIN_LOOKUPS, KEY_LOOKUPS, PATH_LOOKUPS};
    use std::sync::atomic::Ordering::Relaxed;
    let counters = || (KEY_LOOKUPS.load(Relaxed), GIN_LOOKUPS.load(Relaxed), PATH_LOOKUPS.load(Relaxed));
    let (key0, gin0, path0) = counters();
    let runs = env_count("RDB_POWER_LOSS_RUNS", 1, 256);
    let mut total = Tally::default();
    for r in 0..runs {
        let t = run(kind, r).await;
        total.quiescent_cuts += t.quiescent_cuts;
        total.sync_cuts += t.sync_cuts;
        total.images += t.images;
        total.narrowed += t.narrowed;
        total.restarts += t.restarts;
    }
    // Each run must have tested something: images from both kinds of cut,
    // and indexes that actually answered.
    assert!(total.quiescent_cuts >= runs as usize, "{kind:?}: no quiescent cut was imaged ({total:?})");
    assert!(total.sync_cuts >= runs as usize, "{kind:?}: no failpoint cut fired ({total:?})");
    assert!(total.narrowed >= 20 * runs as usize, "{kind:?}: the indexes narrowed too few answers ({total:?})");
    // The engine's own counters agree an index answered (process-wide, so
    // only that they moved).
    let (key, gin, path) = counters();
    match kind {
        Kind::Secondary | Kind::Superset => assert!(key > key0, "{kind:?}: no key lookup was counted"),
        Kind::Json => assert!(gin > gin0 && path > path0, "{kind:?}: gin {gin0}->{gin}, path {path0}->{path}"),
    }
}

#[tokio::test]
async fn secondary_indexes_survive_restarts_and_power_cuts() {
    suite(Kind::Secondary).await;
}

#[tokio::test]
async fn json_indexes_survive_restarts_and_power_cuts() {
    suite(Kind::Json).await;
}

#[tokio::test]
async fn the_randomized_superset_survives_restarts_and_power_cuts() {
    suite(Kind::Superset).await;
}


/// A rolled-back block's `Aborted` is never flushed, so after a restart only
/// the id reservation keeps its id from being handed out again. Reissued,
/// the id's new owner saw the old row as its own and committed it: the
/// rolled-back insert came back after a clean restart. Ids now resume above
/// the durable reservation (`mvcc::status`), and the twin suites run with no
/// workaround for it.
#[tokio::test]
async fn a_rolled_back_insert_stays_rolled_back_across_a_clean_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "rolled").await.unwrap();
    exec(&db, &format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, n INTEGER NOT NULL)")).await.unwrap();
    exec(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES (1, 1), (2, 2)")).await.unwrap();
    let tx = db.transactions.begin();
    exec_in(&db, tx, &format!("INSERT INTO {TABLE} (id, n) VALUES (999, 9)")).await.unwrap();
    db.abort_transaction(tx);
    assert_eq!(ids(&db, "id > 0").await.unwrap(), vec![1, 2]);
    db.sync_durable().unwrap();
    close(&dir, db);
    let db = open(&dir, "rolled").await.unwrap();
    let after = ids(&db, "id > 0").await.unwrap();
    exec(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES (3, 3)")).await.unwrap();
    let later = ids(&db, "id > 0").await.unwrap();
    close(&dir, db);
    assert_eq!(after, vec![1, 2], "the rolled-back row is back after a clean restart");
    assert_eq!(later, vec![1, 2, 3], "the rolled-back row came back once its id was reused");
}

/// Recovery replays committed records into the data files, syncs them, and
/// then drops the log. Those syncs must be recorded under the data files'
/// paths: recorded under the log's, an image cut after a clean restart lost
/// every row the replay had made durable, and the twin suites failed on
/// rows no engine path had lost.
#[tokio::test]
async fn acknowledged_rows_survive_a_power_cut_after_a_clean_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let w = Workload::new(Kind::Secondary, 1);
    let db = open(&dir, "replayed").await.unwrap();
    for sql in w.create(true) {
        exec(&db, &sql).await.unwrap();
    }
    let insert = |from: i64, to: i64| {
        let rows: Vec<String> = (from..to).map(|i| format!("({i}, {}, 'w{i}')", i % 7)).collect();
        format!("INSERT INTO {TABLE} (id, n, s) VALUES {}", rows.join(", "))
    };
    exec(&db, &insert(0, 40)).await.unwrap();
    db.sync_durable().unwrap();
    let rec = Recorder::start(&dir);
    for k in 0..6 {
        exec(&db, &insert(40 + 10 * k, 50 + 10 * k)).await.unwrap();
    }
    let want = rows(&db, &w.row_select()).await.unwrap();
    assert_eq!(want.len(), 100);
    close(&dir, db);
    let db = open(&dir, "replayed").await.unwrap();
    assert_eq!(rows(&db, &w.row_select()).await.unwrap(), want, "the clean restart itself lost rows");
    let cp = rec.cut_quiescent("after a clean restart").unwrap();
    close(&dir, db);
    drop(rec);
    let img = base.path().join("image");
    for seed in 0..5 {
        cp.write_image(seed, &img);
        let db = open(&img, "replayed").await.unwrap();
        let got = rows(&db, &w.row_select()).await.unwrap();
        close(&img, db);
        let _ = std::fs::remove_dir_all(&img);
        assert_eq!(got.len(), want.len(), "seed {seed}: an image cut after a clean restart lost acknowledged rows");
        assert_eq!(got, want, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// The transaction id reservation through a power cut
// ---------------------------------------------------------------------------

/// Ids per raise of the reservation in [`a_lost_reservation_page_never_reissues_an_id`]:
/// small, so a short workload crosses many marks.
const RESERVATION_BLOCK: u64 = 4;
const STATUS_FILE: &str = "db_transactions.status";
/// Cuts taken inside log syncs, per run.
const MAX_LOG_SYNC_CUTS: usize = 12;
/// The row the write after each restart inserts.
const PROBE_ROW: i64 = 9_999_999;

/// Arms `name` for each of its first `hits` hits under `scope`; the action
/// decides for itself whether a hit is one to cut at.
fn arm_every(name: &str, scope: &Path, hits: u64, action: failpoint::Action) -> Vec<failpoint::Guard> {
    (1..=hits).map(|nth| failpoint::arm(name, scope, nth, action.clone())).collect()
}

/// The highest id a status file holds any outcome for, read from its bytes:
/// two bits an id, id 0 in the low bits of byte 0, `00` for none.
fn highest_with_outcome(status: &Path) -> u64 {
    let bytes = std::fs::read(status).unwrap_or_default();
    let mut highest = 0;
    for (i, byte) in bytes.iter().enumerate() {
        for slot in 0..4u64 {
            if (byte >> (slot * 2)) & 0b11 != 0 {
                highest = i as u64 * 4 + slot;
            }
        }
    }
    highest
}

/// Every row version's slot offset with the ids stamped on it, from the
/// pointer file: what is on disk, whatever any counter says. (Deleted slots
/// are skipped; nothing here vacuums.)
async fn stamped(db: &Arc<Database>) -> Result<Vec<(u64, u64, u64)>, String> {
    let t = db.table_for_tests(TABLE).ok_or("the table is gone")?;
    let mut it = RowPointerIterator::new(t.io_pointers().map_err(|e| format!("{e:?}"))?.clone()).await.map_err(|e| format!("{e:?}"))?;
    let mut out = Vec::new();
    while let Some((offset, p)) = it.next_row_pointer_with_offset().await.map_err(|e| format!("{e:?}"))? {
        out.push((offset, p.xmin, p.xmax));
    }
    Ok(out)
}

struct ReservationPoint {
    cp: CrashPoint,
    /// The mark the reservation file held in the page cache at the cut.
    live_mark: u64,
    /// The committed ids an image may hold: before and after the statement
    /// the cut fell in.
    states: Vec<Vec<i64>>,
}

#[derive(Default, Debug)]
struct ReservationTally {
    /// Cuts in the first log sync after the mark moved: the reservation's
    /// own record, written and not yet durable.
    at_reservation_sync: usize,
    /// Cuts in the log sync after that: usually a writer under the new
    /// block committing.
    after_reservation_sync: usize,
    /// Cuts in a checkpoint's sync of the reservation file itself.
    at_reservation_file_sync: usize,
    /// Quiescent cuts with a writer above every durable outcome still open,
    /// its rows durable.
    writer_open: usize,
    disturbed: usize,
    images: usize,
    /// Images whose reservation file held a lower mark than the page cache
    /// did at the cut: the unsynced page was dropped.
    page_dropped: usize,
    /// Images whose mark the log's replay raised.
    replay_raised: usize,
    /// Images whose pointer file ended inside a record before the open: a
    /// power cut can leave that, since a whole-record `write(2)` is not
    /// atomic across one. The open cuts it back before replay
    /// (`core::torn_tail`), and every check below holds for them as for the
    /// rest; counted to show the harness reaches the case.
    torn_pointer_tails: usize,
    marks_crossed: u64,
}

/// Opens one image and checks (a) it opens, (b) exactly the committed rows
/// are there, (c) ids resume above every id the crash left on disk, and (d)
/// a write commits, under an id nothing on disk carries, and changes nothing
/// else.
async fn check_reservation_image(
    img: &Path,
    name: &str,
    label: &str,
    point: &ReservationPoint,
    tally: &mut ReservationTally,
    problems: &mut Vec<String>,
) {
    let status = img.join(STATUS_FILE);
    let imaged = read_reservation(&status).unwrap_or(0);
    // Read before the replay below, which cuts a torn tail back.
    tally.torn_pointer_tails += std::fs::metadata(img.join(format!("{TABLE}_pointers.db")))
        .is_ok_and(|m| m.len() % ROW_POINTER_RECORD_LEN as u64 != 0) as usize;
    tally.page_dropped += (imaged < point.live_mark) as usize;
    // Replayed alone first, so the status file reads as the crash left it
    // plus its log, before an open records anything of its own.
    match crate::core::wal::open_and_recover(&path_str(img)) {
        Ok((wal, _)) => drop(wal),
        Err(e) => {
            problems.push(format!("{label}: (a) the log would not replay: {e}"));
            return;
        }
    }
    let replayed = read_reservation(&status).unwrap_or(0);
    tally.replay_raised += (replayed > imaged) as usize;
    let in_status = highest_with_outcome(&status);
    let db = match open(img, name).await {
        Ok(db) => db,
        Err(e) => {
            problems.push(format!("{label}: (a) the image would not open: {e}"));
            crate::core::wal::remove_for(&path_str(img));
            return;
        }
    };
    let marks = format!("reservation: {imaged} imaged, {replayed} after replay, {} at the cut", point.live_mark);
    let before = match stamped(&db).await {
        Ok(v) => v,
        Err(e) => {
            problems.push(format!("{label}: reading the row pointers: {e}"));
            close(img, db);
            return;
        }
    };
    let in_rows = before.iter().flat_map(|(_, xmin, xmax)| [*xmin, *xmax]).max().unwrap_or(0);
    let highest = in_rows.max(in_status);
    // (c) Every id from here on is at least the next one.
    let resume = db.transactions.next_id();
    if resume <= highest {
        problems.push(format!(
            "{label}: (c) ids resume at {resume}, and id {highest} is already on disk \
             (rows up to {in_rows}, status up to {in_status}; {marks})"
        ));
    }
    // (b)
    let got = ids(&db, "id > 0").await;
    let matched = match &got {
        Ok(got) => point.states.iter().find(|s| *s == got),
        Err(_) => None,
    };
    if matched.is_none() {
        problems.push(format!(
            "{label}: (b) the rows {got:?} are no committed state of the cut ({:?})",
            point.states.iter().map(|s| s.len()).collect::<Vec<_>>()
        ));
    }
    // (d) A write commits under an id nothing on disk carries. Taken over,
    // an id makes its new owner's commit commit the old writer's rows too:
    // its insert appears and its delete removes a committed row.
    if let Err(e) = exec(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES ({PROBE_ROW}, 0)")).await {
        problems.push(format!("{label}: (d) the write after the restart failed: {e}"));
    } else if let Some(state) = matched {
        match ids(&db, "id > 0").await {
            Ok(after) => {
                let appeared: Vec<i64> =
                    after.iter().filter(|id| **id != PROBE_ROW && !state.contains(id)).copied().collect();
                let vanished: Vec<i64> = state.iter().filter(|id| !after.contains(id)).copied().collect();
                if !appeared.is_empty() || !vanished.is_empty() {
                    problems.push(format!(
                        "{label}: (d) the write after the restart committed another transaction's \
                         work: rows {appeared:?} appeared and {vanished:?} vanished ({marks})"
                    ));
                }
                if !after.contains(&PROBE_ROW) {
                    problems.push(format!("{label}: (d) the write after the restart is not there ({marks})"));
                }
            }
            Err(e) => problems.push(format!("{label}: (d) reading the rows after the write: {e}")),
        }
    }
    // The write's own version, read back: stamped above everything that was
    // on disk.
    match stamped(&db).await {
        Ok(after) => {
            let old: std::collections::HashSet<u64> = before.iter().map(|(o, _, _)| *o).collect();
            let new: Vec<&(u64, u64, u64)> = after.iter().filter(|(o, _, _)| !old.contains(o)).collect();
            if new.is_empty() {
                problems.push(format!("{label}: (d) the write after the restart left no row version"));
            }
            for (offset, xmin, _) in new {
                if *xmin <= highest {
                    problems.push(format!(
                        "{label}: (c) the write after the restart stamped id {xmin} at {offset}, and \
                         ids up to {highest} were on disk before it ({marks})"
                    ));
                }
            }
        }
        Err(e) => problems.push(format!("{label}: reading the row pointers after the write: {e}")),
    }
    close(img, db);
}

async fn reservation_run(run_seed: u64, seeds: u64) -> ReservationTally {
    use parking_lot::Mutex;
    use std::sync::Arc as StdArc;

    let name = "lost_reservation";
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let status = dir.join(STATUS_FILE);
    let reservation_file = crate::core::mvcc::status::reservation_path(&status);
    let mut plan = Rng::new(0x7265_7365_7276 ^ run_seed);
    let insert = |id: i64| format!("INSERT INTO {TABLE} (id, n) VALUES ({id}, {})", id % 7);

    let db = open(&dir, name).await.unwrap();
    exec(&db, &format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, n INTEGER NOT NULL)")).await.unwrap();
    let mut committed: Vec<i64> = Vec::new();
    let mut next_row = 0i64;
    for _ in 0..4 {
        next_row += 1;
        exec(&db, &insert(next_row)).await.unwrap();
        committed.push(next_row);
    }
    // The open reserved a full block. Used up, so every mark from here on is
    // a small block above the last and the workload crosses many.
    db.transactions.set_reservation_block(RESERVATION_BLOCK);
    while db.transactions.next_id() < db.transactions.reserved_ids_below() {
        let id = db.transactions.begin();
        db.abort_transaction(id);
    }
    next_row += 1;
    exec(&db, &insert(next_row)).await.unwrap();
    committed.push(next_row);
    db.sync_durable().unwrap();
    let rec = Recorder::start(&dir);
    let baseline_mark = read_reservation(&status).expect("a mark at the baseline");

    let mut tally = ReservationTally::default();
    // (mark at the cut, which site, the cut)
    type Sink = StdArc<Mutex<Vec<(u64, u8, Result<CrashPoint, String>)>>>;
    let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
    let context = StdArc::new(Mutex::new(String::new()));

    // Every log sync: the first after the reservation file changed is, with
    // the reservation logged, the record's own sync in flight; the second is
    // the first writer under the new block committing.
    struct Seen {
        last: u64,
        since: u32,
        cuts: usize,
    }
    let seen = StdArc::new(Mutex::new(Seen {
        last: baseline_mark,
        since: u32::MAX,
        cuts: 0,
    }));
    let at_log_sync: failpoint::Action = {
        let (rec, sink, context, seen, status) = (rec.clone(), sink.clone(), context.clone(), seen.clone(), status.clone());
        StdArc::new(move |hit| {
            let mark = read_reservation(&status).unwrap_or(0);
            let since = {
                let mut s = seen.lock();
                if mark != s.last {
                    s.last = mark;
                    s.since = 0;
                } else {
                    s.since = s.since.saturating_add(1);
                }
                if s.since >= 2 || s.cuts >= MAX_LOG_SYNC_CUTS {
                    return failpoint::Outcome::Continue;
                }
                s.cuts += 1;
                s.since
            };
            let which = if since == 0 { "the log sync after" } else { "the second log sync after" };
            let label = format!("{}, {which} the mark moved to {mark} (hit {})", context.lock(), hit.nth);
            let cut = rec.cut_in_sync(label);
            sink.lock().push((mark, since as u8, cut));
            failpoint::Outcome::Continue
        })
    };
    // A checkpoint's sync of the reservation file itself.
    let at_file_sync: failpoint::Action = {
        let (rec, sink, context, status, file) =
            (rec.clone(), sink.clone(), context.clone(), status.clone(), reservation_file.clone());
        StdArc::new(move |hit| {
            if hit.path != file.as_path() {
                return failpoint::Outcome::Continue;
            }
            let mark = read_reservation(&status).unwrap_or(0);
            let label = format!("{}, a checkpoint syncing the reservation file at {mark}", context.lock());
            let cut = rec.cut_in_sync(label);
            sink.lock().push((mark, 2, cut));
            failpoint::Outcome::Continue
        })
    };
    let log_guards = arm_every("sync:wal.log", &dir, 1024, at_log_sync);

    let mut points: Vec<ReservationPoint> = Vec::new();
    let mut held = 0u64;
    let steps = 36;
    for step in 0..steps {
        let before = committed.clone();
        *context.lock() = format!("run {run_seed} step {step}");
        match (step % 9, plan.below(4)) {
            // A writer above every durable outcome, its rows (an insert and a
            // delete's stamp) made durable, open at the cut and then rolled
            // back. Its id is the one a lost reservation hands out again.
            (8, _) | (_, 0) if step >= 6 => {
                let w = db.transactions.begin();
                *context.lock() = format!("run {run_seed} step {step}, writer {w} open");
                exec_in(&db, w, &insert(2_000_000 + step as i64)).await.unwrap();
                let victim = committed[plan.below(committed.len() as u64) as usize];
                exec_in(&db, w, &format!("DELETE FROM {TABLE} WHERE id = {victim}")).await.unwrap();
                db.sync_durable().unwrap();
                let live_mark = read_reservation(&status).unwrap_or(0);
                match rec.cut_quiescent(format!("run {run_seed} step {step}: writer {w} open, rows durable")) {
                    Ok(cp) => {
                        tally.writer_open += 1;
                        held += cp.held_bytes();
                        points.push(ReservationPoint {
                            cp,
                            live_mark,
                            states: vec![committed.clone()],
                        });
                    }
                    Err(why) => {
                        eprintln!("{why}");
                        tally.disturbed += 1;
                    }
                }
                db.abort_transaction(w);
            }
            // A rolled-back block: an insert and a delete, never visible.
            (_, 1) => {
                let r = db.transactions.begin();
                exec_in(&db, r, &insert(1_000_000 + step as i64)).await.unwrap();
                let victim = committed[plan.below(committed.len() as u64) as usize];
                exec_in(&db, r, &format!("DELETE FROM {TABLE} WHERE id = {victim}")).await.unwrap();
                db.abort_transaction(r);
            }
            // A checkpoint, which syncs the reservation file and drops the
            // records that carried it.
            (4, _) => {
                let guards = arm_every("sync:wal.checkpoint.data_file", &dir, 256, at_file_sync.clone());
                let wal = crate::core::wal::wal_for(&path_str(&dir)).expect("the database has a log");
                wal.checkpoint().unwrap();
                drop(guards);
            }
            _ => {
                next_row += 1;
                exec(&db, &insert(next_row)).await.unwrap();
                committed.push(next_row);
            }
        }
        let after = committed.clone();
        for (live_mark, site, cut) in std::mem::take(&mut *sink.lock()) {
            match cut {
                Ok(cp) => {
                    match site {
                        0 => tally.at_reservation_sync += 1,
                        1 => tally.after_reservation_sync += 1,
                        _ => tally.at_reservation_file_sync += 1,
                    }
                    held += cp.held_bytes();
                    let mut states = vec![before.clone()];
                    if after != before {
                        states.push(after.clone());
                    }
                    points.push(ReservationPoint { cp, live_mark, states });
                }
                Err(why) => {
                    eprintln!("{why}");
                    tally.disturbed += 1;
                }
            }
        }
        assert!(
            held + dir_bytes(base.path()) < MAX_RUN_BYTES,
            "the run holds {} bytes, past its bound",
            held + dir_bytes(base.path())
        );
    }
    drop(log_guards);
    tally.marks_crossed = (db.transactions.reserved_ids_below() - baseline_mark) / RESERVATION_BLOCK;
    close(&dir, db);
    drop(rec);

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.cp.write_image(seed, &img);
            let label = format!("{} seed {seed} (in flight: {:?})", point.cp.label, point.cp.in_flight);
            check_reservation_image(&img, name, &label, point, &mut tally, &mut problems).await;
            tally.images += 1;
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(
        problems.is_empty(),
        "run {run_seed}: {} problems ({tally:?}):\n{}",
        problems.len(),
        problems.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
    eprintln!("lost reservation run {run_seed}: {tally:?}");
    tally
}

/// The id reservation is the one write whose loss hands ids out again: a
/// restart resumes at the mark, and an id below it whose writer left rows
/// on disk with no durable outcome goes to a new owner, who reads those rows
/// as its own and commits them. Only a power cut can lose it. A killed
/// process keeps the page cache, so a reservation written and never made
/// durable passes every clean restart and every `SIGKILL` test, and did.
///
/// Many small blocks are crossed while committing, rolling back, and holding
/// a writer open above every durable outcome with its rows durable. Cuts
/// fall in the reservation's own log sync, in the sync after it, in a
/// checkpoint's sync of the reservation file, and with that writer open; the
/// seeds drop or keep the reservation file's unsynced page and the log's
/// unsynced tail. Every image must open, hold exactly the committed rows,
/// resume ids above every id its rows and status file carry, and take a
/// write under an id of its own.
#[tokio::test]
async fn a_lost_reservation_page_never_reissues_an_id() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let runs = env_count("RDB_POWER_LOSS_RUNS", 1, 256);
    let mut total = ReservationTally::default();
    for r in 0..runs {
        let t = reservation_run(r, seeds).await;
        total.at_reservation_sync += t.at_reservation_sync;
        total.after_reservation_sync += t.after_reservation_sync;
        total.at_reservation_file_sync += t.at_reservation_file_sync;
        total.writer_open += t.writer_open;
        total.images += t.images;
        total.page_dropped += t.page_dropped;
        total.replay_raised += t.replay_raised;
        total.marks_crossed += t.marks_crossed;
        total.torn_pointer_tails += t.torn_pointer_tails;
        total.disturbed += t.disturbed;
    }
    eprintln!("lost reservation, {runs} runs of {seeds} seeds: {total:?}");
    // Each kind of cut was imaged, and some images lost the page the test is
    // about, so the property was asked where it can fail.
    let runs = runs as usize;
    assert!(total.marks_crossed >= 4 * runs as u64, "too few marks were crossed ({total:?})");
    assert!(total.at_reservation_sync >= runs, "no cut fell in a reservation's sync ({total:?})");
    assert!(total.after_reservation_sync >= runs, "no cut fell in the sync after one ({total:?})");
    assert!(total.at_reservation_file_sync >= 1, "no checkpoint synced the reservation file ({total:?})");
    assert!(total.writer_open >= runs, "no cut held an open writer ({total:?})");
    assert!(total.page_dropped >= runs, "no image dropped the reservation file's page ({total:?})");
    assert!(total.replay_raised >= runs, "no image needed the log to restore the mark ({total:?})");
    assert!(total.torn_pointer_tails >= runs, "no image tore the pointer file inside a record ({total:?})");
}

#[path = "rewrite_crash_tests.rs"]
mod rewrite_crash_tests;

#[path = "pddl_crash_tests.rs"]
mod pddl_crash_tests;

#[path = "rename_crash_tests.rs"]
mod rename_crash_tests;

#[path = "create_crash_tests.rs"]
mod create_crash_tests;
