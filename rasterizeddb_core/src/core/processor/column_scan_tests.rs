//! Folding an aggregate from cells may only change how long it takes. Every
//! test here runs a statement with the cell fold off (the fold over
//! serialized rows) and on, and requires the same bytes back; and it requires
//! each statement to have taken the path the test says it takes, so a
//! statement that quietly stopped folding from cells is not compared with
//! itself.

use rclite::Arc;

use super::column_scan::{self, FOLDS_FROM_CELLS};
use crate::core::database::Database;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;

/// Tests that flip the global switch run one at a time. Other tests may see
/// it off for a moment, which changes nothing they can observe.
static SWITCH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Runs `sql`, as `tx` when given. Returns the reply and how many folds from
/// cells the statement made (counted on its own task, so no other test's
/// statements are in it).
async fn run_in(db: &Arc<Database>, tx: Option<crate::core::mvcc::TxId>, sql: &str) -> (Vec<u8>, u32) {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
    FOLDS_FROM_CELLS
        .scope(std::cell::Cell::new(0), async {
            let reply = match tx {
                Some(tx) => Box::pin(executor::execute_with_context(purpose, db.clone(), 0, tx)).await,
                None => executor::execute(purpose, db.clone()).await,
            };
            (reply, FOLDS_FROM_CELLS.with(std::cell::Cell::get))
        })
        .await
}

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    run_in(db, None, sql).await.0
}

async fn must(db: &Arc<Database>, sql: &str) {
    let reply = run(db, sql).await;
    assert!(reply.first() != Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
}

/// `sql` with the cell fold off and on: the same reply, and `cells` says
/// whether the second run must have folded from cells. Returns the reply.
async fn same_both_ways(db: &Arc<Database>, tx: Option<crate::core::mvcc::TxId>, sql: &str, cells: bool, note: &str) -> Vec<u8> {
    column_scan::set_enabled(false);
    let (from_rows, folds) = run_in(db, tx, sql).await;
    assert_eq!(folds, 0, "{note}{sql}: folded from cells while that is off");
    column_scan::set_enabled(true);
    let (from_cells, folds) = run_in(db, tx, sql).await;
    assert!(
        from_cells == from_rows,
        "{note}{sql}: folded from cells {:?}, from rows {:?}",
        String::from_utf8_lossy(&from_cells),
        String::from_utf8_lossy(&from_rows)
    );
    assert_eq!(
        folds > 0,
        cells,
        "{note}{sql}: {}",
        if cells { "was not folded from cells, so nothing was compared" } else { "was folded from cells" }
    );
    from_cells
}

struct RemoveDir(String);
impl Drop for RemoveDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(name: &str) -> (String, RemoveDir) {
    let dir = std::env::temp_dir().join(format!("column_scan_{name}_{}", std::process::id()));
    let dir = dir.to_string_lossy().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    (dir.clone(), RemoveDir(dir))
}

/// xorshift64*, so a failing table reproduces.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn pick<'a>(&mut self, options: &[&'a str]) -> &'a str {
        options[(self.next() % options.len() as u64) as usize]
    }
}

const COLUMNS: &str = "id BIGINT NOT NULL, i8c TINYINT, u8c UTINYINT, i16c SMALLINT, \
    u16c USMALLINT, i32c INTEGER, u32c UINT, i64c BIGINT, u64c UBIGINT, r REAL, \
    f DOUBLE PRECISION, flag BOOLEAN, ts TIMESTAMPTZ, s TEXT, n NUMERIC(10,2), k INTEGER NOT NULL";
const COLUMN_NAMES: &str = "id, i8c, u8c, i16c, u16c, i32c, u32c, i64c, u64c, r, f, flag, ts, s, n, k";

/// One row's values. Mostly a narrow range, so groups have members; sometimes
/// a type's extreme; about one in eight NULL. The floats are quarters and
/// halves, so their sums are exact in whatever order they are added.
fn row(rng: &mut Rng, id: u64) -> String {
    fn cell(rng: &mut Rng, typical: impl Fn(&mut Rng) -> String, extremes: &[&str]) -> String {
        match rng.next() % 16 {
            0 | 1 => "NULL".to_string(),
            2 if !extremes.is_empty() => rng.pick(extremes).to_string(),
            _ => typical(rng),
        }
    }
    fn small(rng: &mut Rng) -> i64 {
        (rng.next() % 101) as i64 - 50
    }
    let unsigned = |rng: &mut Rng| (rng.next() % 101).to_string();
    let v = [
        cell(rng, |r| small(r).to_string(), &["-128", "127"]),
        cell(rng, |r| (r.next() % 256).to_string(), &["0", "255"]),
        cell(rng, |r| small(r).to_string(), &["-32768", "32767"]),
        cell(rng, unsigned, &["0", "65535"]),
        cell(rng, |r| small(r).to_string(), &["-2147483648", "2147483647"]),
        cell(rng, unsigned, &["0", "4294967295"]),
        cell(rng, |r| small(r).to_string(), &["-9223372036854775808", "9223372036854775807"]),
        cell(rng, unsigned, &["0", "18446744073709551615"]),
        cell(rng, |r| format!("{}", small(r) as f64 / 4.0), &["2.5", "-1024"]),
        cell(rng, |r| format!("{}", small(r) as f64 / 2.0), &["2.5", "1048576"]),
        cell(rng, |r| if r.next() % 2 == 0 { "true" } else { "false" }.to_string(), &[]),
        cell(rng, |r| format!("'2024-{:02}-15 12:00:00+00'", r.next() % 12 + 1), &[]),
        cell(rng, |r| format!("'x{}'", r.next() % 10), &["''"]),
        cell(rng, |r| format!("{:.2}", small(r) as f64 / 8.0), &["99999999.99", "-99999999.99"]),
        (rng.next() % 7).to_string(),
    ];
    format!("({id}, {})", v.join(", "))
}

async fn fill(db: &Arc<Database>, name: &str, rows: u64) {
    let mut rng = Rng(0xCE11_5EED);
    let mut id = 0;
    while id < rows {
        let chunk: Vec<String> = (id..(id + 500).min(rows)).map(|i| row(&mut rng, i)).collect();
        must(db, &format!("INSERT INTO {name} ({COLUMN_NAMES}) VALUES {}", chunk.join(", "))).await;
        id += 500;
    }
}

/// Aggregates the fold reads as cells: one column or none per aggregate,
/// plain columns of fixed width for keys.
const FROM_CELLS: &[&str] = &[
    "SELECT COUNT(*), SUM(i16c), MIN(f), MAX(u8c) FROM t",
    "SELECT COUNT(i32c), SUM(i32c), AVG(i32c), MIN(i32c), MAX(i32c) FROM t",
    "SELECT SUM(i64c), AVG(i64c), MIN(i64c), MAX(u64c), SUM(u64c) FROM t",
    "SELECT SUM(n), AVG(n), MIN(n), MAX(n), COUNT(n) FROM t",
    "SELECT SUM(f), AVG(f), MIN(r), MAX(r), SUM(r) FROM t",
    "SELECT COUNT(DISTINCT i8c), SUM(DISTINCT u8c), COUNT(DISTINCT n) FROM t",
    "SELECT bool_and(flag), bool_or(flag), COUNT(flag) FROM t",
    "SELECT MIN(ts), MAX(ts), COUNT(ts) FROM t",
    "SELECT stddev(i16c), variance(i8c), var_pop(u8c) FROM t WHERE i16c BETWEEN -50 AND 50",
    "SELECT 1, COUNT(*), MAX(k) FROM t",
    "SELECT MAX(i32c), MIN(u16c) FROM t WHERE i32c > 10",
    "SELECT COUNT(*), SUM(u32c) FROM t WHERE i64c < 0 OR i16c > 30",
    "SELECT COUNT(*), AVG(i8c) FROM t WHERE s = 'x5'",
    "SELECT COUNT(*), MIN(n) FROM t WHERE s = 'x5' AND i32c > 0",
    "SELECT COUNT(*), MAX(f) FROM t WHERE i32c IS NULL",
    "SELECT COUNT(*), SUM(i8c) FROM t WHERE i32c + 1 > 10 AND i32c < 1000",
    "SELECT COUNT(u8c), SUM(k) FROM t WHERE n > 1.5",
    // Nothing matches: one row of a zero and NULLs, or no group at all.
    "SELECT COUNT(*), SUM(i32c), MAX(f) FROM t WHERE k > 1000",
    "SELECT k, COUNT(*) FROM t WHERE k > 1000 GROUP BY k ORDER BY k",
    // Grouped: a one-byte key and its NULL group, a NOT NULL key, wide keys,
    // two keys, a key per row.
    "SELECT i8c, COUNT(*), SUM(i32c) FROM t GROUP BY i8c ORDER BY i8c",
    "SELECT flag, COUNT(*), AVG(n), MIN(ts) FROM t GROUP BY flag ORDER BY flag",
    "SELECT k, COUNT(*), SUM(f), MAX(i64c) FROM t GROUP BY k ORDER BY k",
    "SELECT k, COUNT(*), SUM(u8c) FROM t WHERE i32c > 0 GROUP BY k ORDER BY k",
    "SELECT u64c, COUNT(*) FROM t GROUP BY u64c ORDER BY u64c",
    "SELECT n, COUNT(*), SUM(i8c) FROM t GROUP BY n ORDER BY n",
    "SELECT ts, COUNT(*), COUNT(i16c) FROM t GROUP BY ts ORDER BY ts",
    "SELECT i32c, u16c, COUNT(*), MAX(k) FROM t GROUP BY i32c, u16c ORDER BY i32c, u16c",
    "SELECT id, COUNT(*), MAX(i8c) FROM t GROUP BY id ORDER BY id DESC LIMIT 25",
    "SELECT i8c, COUNT(*) FROM t GROUP BY i8c HAVING SUM(i32c) > 0 AND COUNT(u8c) > 2 ORDER BY i8c",
    "SELECT u8c, COUNT(*), SUM(k) FROM t GROUP BY u8c ORDER BY u8c LIMIT 5 OFFSET 3",
    "SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY COUNT(*) DESC, k LIMIT 3",
];

/// Aggregates that read more of a row than cells, or a string, and so fold
/// from rows whatever the switch says.
const FROM_ROWS: &[&str] = &[
    "SELECT s, COUNT(*), SUM(i8c) FROM t GROUP BY s ORDER BY s",
    "SELECT MAX(s), MIN(i32c) FROM t",
    "SELECT COUNT(*) FILTER (WHERE i32c > 0), SUM(i8c) FROM t",
    "SELECT SUM(i8c + 1), COUNT(*) FROM t",
    "SELECT COUNT(*) + 1, MAX(i8c) FROM t",
    "SELECT k + 1 AS g, COUNT(*) FROM t GROUP BY k + 1 ORDER BY g",
    // A computed key the statement does not return, and aggregates only its
    // HAVING reads that are more than a column.
    "SELECT COUNT(*), MAX(id) FROM t GROUP BY k + 1 ORDER BY MAX(id)",
    "SELECT i8c, COUNT(*) FROM t GROUP BY i8c HAVING SUM(u8c + 1) > 0 ORDER BY i8c",
    "SELECT i8c, COUNT(*) FROM t GROUP BY i8c HAVING COUNT(*) FILTER (WHERE u8c > 9) > 2 ORDER BY i8c",
    "SELECT array_agg(i8c ORDER BY id) FROM t WHERE id < 40",
];

async fn differential(in_memory: bool) {
    let _g = SWITCH.lock().await;
    let (dir, _cleanup) = scratch(if in_memory { "memory" } else { "files" });
    let db = Arc::new(Database::new_with_backend(&dir, in_memory).await);
    must(&db, &format!("CREATE TABLE t ({COLUMNS})")).await;
    // Two batches, the second partial.
    fill(&db, "t", 12_000).await;

    for round in 0..2 {
        let note = format!("round {round}, ");
        let mut answered = 0;
        for sql in FROM_CELLS {
            let reply = same_both_ways(&db, None, sql, true, &note).await;
            assert!(reply.first() != Some(&3), "{note}{sql}: {}", String::from_utf8_lossy(&reply));
            answered += usize::from(reply.len() > 16);
        }
        // Not every statement an error or an empty answer on both sides.
        assert!(answered >= FROM_CELLS.len() - 2, "{note}only {answered} statements answered with rows");
        for sql in FROM_ROWS {
            let reply = same_both_ways(&db, None, sql, false, &note).await;
            assert!(reply.first() != Some(&3), "{note}{sql}: {}", String::from_utf8_lossy(&reply));
        }
        // An error is the same error: a sum past its type.
        same_both_ways(&db, None, "SELECT SUM(u64c), SUM(i64c) FROM t WHERE u64c > 10 OR i64c > 10", true, &note).await;
        if round != 0 {
            break;
        }
        // Leave versions behind for the next round: rows rewritten (the old
        // version stays, stamped), rows deleted, rows made NULL.
        for change in [
            "UPDATE t SET i32c = 11, f = 2.5 WHERE u8c < 60",
            "DELETE FROM t WHERE i16c > 40",
            "UPDATE t SET i32c = NULL, n = NULL WHERE i8c = 3",
            "UPDATE t SET i64c = -7, flag = true, k = 6 WHERE i32c = 11 AND u16c < 30",
            "INSERT INTO t (id, i32c, i64c, f, k) VALUES (900001, 12, -1, 0.25, 3), (900002, NULL, 5, 0, 4)",
        ] {
            must(&db, change).await;
        }
    }
    column_scan::set_enabled(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cells_and_rows_fold_to_the_same_answer_file_backed() {
    differential(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cells_and_rows_fold_to_the_same_answer_in_memory() {
    differential(true).await
}

/// Past a hundred thousand rows the chunks are folded on several threads and
/// merged, unless the first chunk shows too many groups for that to pay.
/// Both ways, and the serial fold of the rows, give one answer: for a table
/// whose scan sends a chunk per worker, and for one of sixteen partitions,
/// which sends more chunks than there are threads to fold them, so a thread
/// folds several.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_input_folds_in_parts_to_the_same_answer() {
    let _g = SWITCH.lock().await;
    let (dir, _cleanup) = scratch("large");
    let db = Arc::new(Database::new_named(&dir, false, "columnscanlarge").await);
    must(&db, "CREATE TABLE w (id BIGINT NOT NULL, g INTEGER NOT NULL, v INTEGER, h SMALLINT)").await;
    must(
        &db,
        "CREATE TABLE wp (id BIGINT NOT NULL, g INTEGER NOT NULL, v INTEGER, h SMALLINT) \
         PARTITION BY HASH (id) PARTITIONS 16",
    )
    .await;
    let rows = 130_000u64;
    let mut rng = Rng(0xB16_F01D);
    let mut id = 0;
    while id < rows {
        let chunk: Vec<String> = (id..(id + 2_000).min(rows))
            .map(|i| {
                let v = match rng.next() % 9 {
                    0 => "NULL".to_string(),
                    n => ((rng.next() % 2001) as i64 - 1000 + n as i64).to_string(),
                };
                format!("({i}, {}, {v}, {})", rng.next() % 12, rng.next() % 300)
            })
            .collect();
        for table in ["w", "wp"] {
            must(&db, &format!("INSERT INTO {table} (id, g, v, h) VALUES {}", chunk.join(", "))).await;
        }
        id += 2_000;
    }
    let mut answers: Vec<Vec<Vec<u8>>> = Vec::new();
    for table in ["w", "wp"] {
        let mut replies = Vec::new();
        for sql in [
            // One group and twelve: folded in parts and merged.
            "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM {t}",
            "SELECT g, COUNT(*), SUM(v), COUNT(DISTINCT h), MIN(id), MAX(id) FROM {t} GROUP BY g ORDER BY g",
            "SELECT g, h, COUNT(*), SUM(v) FROM {t} WHERE v > -500 GROUP BY g, h ORDER BY g, h",
            // A group per row: the first chunk says so and the rest fold serially.
            "SELECT id, COUNT(*), SUM(v) FROM {t} GROUP BY id ORDER BY id LIMIT 40 OFFSET 99000",
        ] {
            let sql = sql.replace("{t}", table);
            let reply = same_both_ways(&db, None, &sql, true, "").await;
            assert!(reply.first() != Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
            replies.push(reply);
        }
        let total = run(&db, &format!("SELECT COUNT(*), MAX(id) FROM {table}")).await;
        assert_eq!(first_count(&total), rows as i64, "{table}: not every row was folded once");
        answers.push(replies);
    }
    // The same rows, so the same answers, however they are laid out.
    assert!(answers[0] == answers[1], "the partitioned table's answers differ from the plain table's");
    column_scan::set_enabled(true);
}

/// Which rows a statement's cells are read from is its snapshot's to say:
/// not the rows another transaction has inserted and not committed, still
/// the rows it has deleted and not committed, and for that transaction
/// itself the reverse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_fold_reads_what_its_snapshot_sees() {
    let _g = SWITCH.lock().await;
    let (dir, _cleanup) = scratch("snapshot");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    must(&db, &format!("CREATE TABLE t ({COLUMNS})")).await;
    fill(&db, "t", 12_000).await;

    const SEEN: &[&str] = &[
        "SELECT COUNT(*), SUM(i32c), MAX(i16c) FROM t",
        "SELECT COUNT(*), SUM(i64c) FROM t WHERE i32c > 10",
        "SELECT k, COUNT(*), SUM(u8c), COUNT(i32c) FROM t GROUP BY k ORDER BY k",
        "SELECT COUNT(*), MIN(id), MAX(id) FROM t WHERE i32c IS NULL",
    ];
    let mut committed = Vec::new();
    for sql in SEEN {
        committed.push(same_both_ways(&db, None, sql, true, "before, ").await);
    }

    let tx = db.transactions.begin();
    let fresh: Vec<String> = (0..400).map(|i| format!("({}, 20, -5, 40, 250, 5)", 800_000 + i)).collect();
    for change in [
        format!("INSERT INTO t (id, i32c, i64c, i16c, u8c, k) VALUES {}", fresh.join(", ")),
        "DELETE FROM t WHERE i32c > 30".to_string(),
        "UPDATE t SET i32c = NULL WHERE i8c = 3".to_string(),
    ] {
        let (reply, _) = run_in(&db, Some(tx), &change).await;
        assert!(reply.first() != Some(&3), "{change}: {}", String::from_utf8_lossy(&reply));
    }

    let mut own_view_differs = 0;
    for (sql, committed) in SEEN.iter().zip(&committed) {
        let outside = same_both_ways(&db, None, sql, true, "outside, ").await;
        assert!(&outside == committed, "{sql}: another transaction's uncommitted writes were folded");
        let inside = same_both_ways(&db, Some(tx), sql, true, "inside, ").await;
        own_view_differs += usize::from(&inside != committed);
    }
    assert_eq!(own_view_differs, SEEN.len(), "the transaction's writes changed {own_view_differs} of its own answers");

    db.abort_transaction(tx);
    for (sql, committed) in SEEN.iter().zip(&committed) {
        let after = same_both_ways(&db, None, sql, true, "aborted, ").await;
        assert!(&after == committed, "{sql}: an aborted transaction's writes were folded");
    }
    column_scan::set_enabled(true);
}

/// A partitioned table's cells are every routed partition's, under the one
/// snapshot the fan-out took.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_table_folds_from_every_partitions_cells() {
    let _g = SWITCH.lock().await;
    let (dir, _cleanup) = scratch("partitioned");
    let db = Arc::new(Database::new_named(&dir, false, "columnscan").await);
    must(&db, &format!("CREATE TABLE t ({COLUMNS}) PARTITION BY HASH (k)")).await;
    fill(&db, "t", 6_000).await;
    must(&db, "DELETE FROM t WHERE i16c > 40").await;
    must(&db, "UPDATE t SET i32c = NULL WHERE i8c = 3").await;

    for sql in [
        "SELECT COUNT(*), SUM(i32c), MIN(f), MAX(u8c) FROM t",
        "SELECT k, COUNT(*), SUM(i64c), AVG(n) FROM t GROUP BY k ORDER BY k",
        "SELECT i8c, COUNT(*), MAX(k) FROM t WHERE i32c > 0 GROUP BY i8c ORDER BY i8c",
        // Routed to one partition, and to none of the rows.
        "SELECT COUNT(*), SUM(i16c) FROM t WHERE k = 3",
        "SELECT COUNT(*), SUM(i16c) FROM t WHERE k = 3 AND k = 4",
    ] {
        let reply = same_both_ways(&db, None, sql, true, "").await;
        assert!(reply.first() != Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
    }
    // The whole table's count is every partition's: not one partition's
    // cells, and not one partition's counted seven times.
    let total = run(&db, "SELECT COUNT(*), COUNT(k) FROM t").await;
    let mut by_key = 0i64;
    for k in 0..7 {
        let one = run(&db, &format!("SELECT COUNT(*), MAX(k) FROM t WHERE k = {k}")).await;
        by_key += first_count(&one);
    }
    assert_eq!(first_count(&total), by_key, "the partitions' counts do not add up to the table's");
    assert!(by_key > 4_000, "{by_key} rows counted");
    column_scan::set_enabled(true);
}

/// The first column of a one-row reply, as the `BIGINT` a count is.
fn first_count(reply: &[u8]) -> i64 {
    assert_eq!(reply.first(), Some(&2), "{}", String::from_utf8_lossy(reply));
    let rows = crate::core::row::row::vec_into_rows(&reply[1..]).expect("rows");
    let cell = rows[0].columns[0].data.into_slice();
    i64::from_le_bytes(cell.try_into().expect("a count is eight bytes"))
}

const CACHE_CHILD_ENV: &str = "RDB_COLUMN_SCAN_CACHE_CHILD";

/// With the query cache on, a repeated aggregate is answered from the cached
/// pointers: their cells are read without a scan. The cache is one switch
/// per process, so this runs in a child.
#[test]
#[cfg(unix)]
fn a_cached_match_set_folds_from_its_cells() {
    if std::env::var(CACHE_CHILD_ENV).is_ok() {
        return;
    }
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args(["--exact", "core::processor::column_scan_tests::cache_child", "--nocapture", "--test-threads=1"])
        .env(CACHE_CHILD_ENV, "1")
        .output()
        .expect("spawn the cache child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "the cache child failed:\n{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "the cache child did not run:\n{stdout}\n{stderr}");
}

/// The child half of [`a_cached_match_set_folds_from_its_cells`]. A no-op
/// unless the parent set the environment variable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_child() {
    if std::env::var(CACHE_CHILD_ENV).is_err() {
        return;
    }
    crate::cache::query_cache::init(1024);
    let _ = crate::cache::query_cache::ENABLE_CACHE.set(true);
    assert!(crate::cache::query_cache::enabled(), "the child could not turn the cache on");

    let (dir, _cleanup) = scratch("cache");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    must(&db, &format!("CREATE TABLE t ({COLUMNS})")).await;
    fill(&db, "t", 12_000).await;
    must(&db, "DELETE FROM t WHERE i16c > 40").await;

    let hits = || crate::cache::query_cache::stats().0;
    for sql in [
        // Every row: more than one read of the entry's cells.
        "SELECT COUNT(*), SUM(i16c), MAX(k) FROM t",
        "SELECT COUNT(*), SUM(i64c), MAX(u8c) FROM t WHERE i32c > 10",
        "SELECT k, COUNT(*), SUM(i8c) FROM t WHERE s = 'x5' GROUP BY k ORDER BY k",
        "SELECT COUNT(*), MIN(f) FROM t WHERE k > 1000",
    ] {
        // Rows first, with the cache cold for this clause and then warm; then
        // cells from the warm entry.
        column_scan::set_enabled(false);
        let (scanned, _) = run_in(&db, None, sql).await;
        let (from_rows, _) = run_in(&db, None, sql).await;
        assert!(scanned == from_rows, "{sql}: the cached rows differ from the scanned ones");
        column_scan::set_enabled(true);
        let before = hits();
        let (from_cells, folds) = run_in(&db, None, sql).await;
        assert!(folds > 0, "{sql}: not folded from cells");
        assert!(hits() > before, "{sql}: the cell fold did not read the cached match set");
        assert!(
            from_cells == from_rows,
            "{sql}: cells from the cache {:?}, rows {:?}",
            String::from_utf8_lossy(&from_cells),
            String::from_utf8_lossy(&from_rows)
        );
    }
    // And a cell scan fills the cache for the rows that come after it.
    let sql = "SELECT COUNT(*), SUM(i16c) FROM t WHERE u8c > 200";
    let (from_cells, _) = run_in(&db, None, sql).await;
    let before = hits();
    let (again, _) = run_in(&db, None, sql).await;
    assert!(hits() > before, "{sql}: a cell scan stored no match set");
    assert!(again == from_cells, "{sql}: the cached cells differ from the scanned ones");
    // A write retires the entry: the next answer is the new one.
    must(&db, "DELETE FROM t WHERE u8c > 250").await;
    column_scan::set_enabled(false);
    let from_rows = run(&db, sql).await;
    column_scan::set_enabled(true);
    let after = run(&db, sql).await;
    assert!(after == from_rows, "{sql}: stale cells after a delete");
    assert!(after != from_cells, "{sql}: the delete changed nothing");

    // An entry names every version matching its clause, whoever may see it,
    // so an open transaction's rows are in it: each reader's snapshot says
    // which of the entry's cells are its to fold.
    let sql = "SELECT COUNT(*), SUM(i64c), MAX(u8c) FROM t WHERE i32c > 10";
    let committed = run(&db, sql).await;
    let tx = db.transactions.begin();
    let fresh: Vec<String> = (0..400).map(|i| format!("({}, 20, -5, 40, 250, 5)", 800_000 + i)).collect();
    for change in [
        format!("INSERT INTO t (id, i32c, i64c, i16c, u8c, k) VALUES {}", fresh.join(", ")),
        "DELETE FROM t WHERE i32c > 30".to_string(),
    ] {
        let (reply, _) = run_in(&db, Some(tx), &change).await;
        assert!(reply.first() != Some(&3), "{change}: {}", String::from_utf8_lossy(&reply));
    }
    let (scanned, _) = run_in(&db, None, sql).await;
    assert!(scanned == committed, "{sql}: a scan folded another transaction's uncommitted writes");
    let before = hits();
    let (cached, folds) = run_in(&db, None, sql).await;
    assert!(folds > 0 && hits() > before, "{sql}: not folded from the cached match set");
    assert!(cached == committed, "{sql}: the cached cells of another transaction's uncommitted writes were folded");
    let before = hits();
    let (inside, folds) = run_in(&db, Some(tx), sql).await;
    assert!(folds > 0 && hits() > before, "{sql}: the transaction did not fold from the cached match set");
    column_scan::set_enabled(false);
    let (inside_rows, _) = run_in(&db, Some(tx), sql).await;
    column_scan::set_enabled(true);
    assert!(inside == inside_rows, "{sql}: the transaction's cached cells differ from its rows");
    assert!(inside != committed, "{sql}: the transaction does not see its own writes");
    db.abort_transaction(tx);
    assert!(run(&db, sql).await == committed, "{sql}: an aborted transaction's writes were folded");
}

/// A batch read that fails loses only the rows that cannot be read, and a
/// strict read loses nothing it had.
#[tokio::test]
async fn a_row_that_cannot_be_read_is_the_only_one_left_out() {
    use crate::core::storage_providers::memory::MemoryStorageProvider;
    use crate::core::storage_providers::traits::StorageIO;
    use column_scan::{CellColumn, CellLayout, Gather};

    // Four rows of twelve bytes: an eight-byte value, a flag, three more.
    let io = MemoryStorageProvider::new("column_scan_gather");
    let mut data = Vec::new();
    for row in 0u64..4 {
        data.extend_from_slice(&(row * 1000 + 7).to_le_bytes());
        data.push(row as u8 % 2);
        data.extend_from_slice(&[0xAA, 0xBB, row as u8]);
    }
    io.append_data(&data, true).await;
    let layout = CellLayout {
        columns: vec![
            CellColumn { schema_id: 5, offset: 8, width: 1 },
            CellColumn { schema_id: 2, offset: 0, width: 8 },
            CellColumn { schema_id: 9, offset: 11, width: 1 },
        ],
    };
    assert_eq!(layout.position(2), Some(1));
    assert_eq!(layout.position(3), None);

    let mut gather = Gather::new(&layout);
    for position in [24u64, 0] {
        gather.note(position);
    }
    gather.flush(&io).await.expect("two rows inside the file");
    // A row past the end among good ones: the strict read takes none of them.
    for position in [12u64, 4000, 36] {
        gather.note(position);
    }
    assert!(gather.flush(&io).await.is_err(), "a read past the end succeeded");
    // The same rows for a scan: the two that can be read, in order.
    for position in [12u64, 4000, 36] {
        gather.note(position);
    }
    gather.flush_readable(&io).await;
    // Nothing is left noted: a second flush reads nothing.
    gather.flush_readable(&io).await;
    gather.flush(&io).await.expect("nothing to read");

    let chunk = gather.finish();
    assert_eq!(chunk.rows, 4);
    let values: Vec<u64> =
        (0..4).map(|r| u64::from_le_bytes(chunk.cell(1, 8, r).try_into().expect("eight bytes"))).collect();
    assert_eq!(values, [2007, 7, 1007, 3007]);
    assert_eq!(chunk.cols[0], [0, 0, 1, 1]);
    assert_eq!(chunk.cols[2], [2, 0, 1, 3]);
}

/// A fetch has a cell layout exactly when none of its columns is a string.
#[test]
fn a_fetch_naming_a_string_has_no_layout() {
    use crate::core::db_type::DbType;
    use crate::core::row::row::{ColumnFetchingData, RowFetch};
    use column_scan::CellLayout;

    let column = |column_type: DbType, size: u32, schema_id: u64| ColumnFetchingData {
        column_offset: schema_id as u32 * 16,
        column_type,
        size,
        schema_id,
    };
    let fixed = RowFetch {
        columns_fetching_data: [column(DbType::I64, 8, 1), column(DbType::BOOL, 1, 4)].into_iter().collect(),
    };
    let layout = CellLayout::of(&fixed).expect("fixed columns");
    assert_eq!(
        layout.columns.iter().map(|c| (c.schema_id, c.offset, c.width)).collect::<Vec<_>>(),
        [(1, 16, 8), (4, 64, 1)]
    );
    let with_string = RowFetch {
        columns_fetching_data: [column(DbType::I64, 8, 1), column(DbType::STRING, 12, 2)].into_iter().collect(),
    };
    assert!(CellLayout::of(&with_string).is_none());
    let empty = RowFetch { columns_fetching_data: Default::default() };
    assert!(CellLayout::of(&empty).is_some_and(|l| l.columns.is_empty()));
}
