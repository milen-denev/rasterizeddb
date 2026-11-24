//! `UPDATE … FROM` and `DELETE … USING` write their rows back a batch to a
//! statement (`JOIN_WRITE_BATCH`) rather than a statement per row, which paid
//! a log sync per row: about 3 ms a row, so a 9 600-row backfill from a
//! partitioned table took half a minute.
//!
//! Every test here compares a batched table with its twin written row by row
//! (`join_write_hooks::per_row_only`), the path these statements always took,
//! byte for byte: a batch chooses each row's value by a `CASE` cast to the
//! column's type, and a cast that assigned a value differently from the
//! row's own statement would be a silent wrong answer.

use rclite::Arc;

use super::tests::{close, exec, exec_ok, open, rows};
use crate::core::database::Database;
use crate::core::rql::executor::join_write_hooks;

/// Rows spanning three batches, so a batch boundary and a short last batch
/// are both crossed.
const ROWS: i64 = 300;

/// Every column of every row of `table`, ordered by `order`, as each cell's
/// type and bytes.
async fn cells(db: &Arc<Database>, table: &str, order: &str) -> Vec<Vec<(String, Vec<u8>)>> {
    rows(db, &format!("SELECT * FROM {table} ORDER BY {order}"))
        .await
        .iter()
        .map(|r| r.columns.iter().map(|c| (format!("{:?}", c.column_type), c.data.into_slice().to_vec())).collect())
        .collect()
}

fn affected(out: &[u8], sql: &str) -> u64 {
    assert_eq!(out.first(), Some(&0u8), "{sql}: {}", String::from_utf8_lossy(out));
    u64::from_le_bytes(out[1..9].try_into().unwrap())
}

fn failure(out: &[u8], sql: &str) -> String {
    assert_eq!(out.first(), Some(&3u8), "{sql} succeeded");
    String::from_utf8_lossy(&out[1..]).into_owned()
}

fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The source's columns, every type a batch casts, NULLs scattered through
/// each so a `CASE` meets a NULL branch beside typed ones, and values of
/// several scales in the unconstrained and the wide `NUMERIC`.
const SOURCE: &str = "CREATE TABLE src (k INT PRIMARY KEY, g INT NOT NULL, i INT, n2 NUMERIC(10,2), nu NUMERIC, \
     n15 NUMERIC(30,15), f8 DOUBLE PRECISION, f4 REAL, t TEXT, b BOOLEAN, u UUID, ts TIMESTAMPTZ, tsl TIMESTAMP, \
     d DATE, j JSONB, by BYTEA, sm SMALLINT, big BIGINT)";

async fn fill_source(db: &Arc<Database>) {
    exec_ok(db, SOURCE).await;
    let mut values = Vec::new();
    for k in 1..=ROWS {
        let null = |offset: i64, v: String| if (k + offset) % 7 == 0 { "NULL".to_string() } else { v };
        let nu = match k % 3 {
            0 => format!("{k}"),
            1 => format!("{k}.5"),
            _ => format!("-{k}.125"),
        };
        let f8 = match k % 4 {
            0 => "0.30000000000000004".to_string(),
            1 => "1e20".to_string(),
            2 => format!("{:?}", k as f64 / 7.0),
            _ => format!("-{k}.25"),
        };
        let row = [
            k.to_string(),
            (k % 13).to_string(),
            null(0, (k * 3 - 100).to_string()),
            null(1, format!("{}.{:02}", k * 7 % 1000 - 500, k % 100)),
            null(2, nu),
            // Two values a rounding through the engine's nine places would
            // round twice: to 0.005 and then 0.01, where once gives 0.00.
            match k {
                1 => "0.004999999995".to_string(),
                2 => "-0.004999999995".to_string(),
                _ => null(3, format!("{}.{:015}", k % 11, k * 4_999_999_999 % 1_000_000_000_000_000)),
            },
            null(4, f8),
            null(5, format!("{:?}", k as f32 / 3.0)),
            null(6, quoted(&format!("it's #{k}{}", if k % 2 == 0 { " é" } else { "" }))),
            null(0, (k % 2 == 0).to_string()),
            null(1, format!("'00000000-0000-4000-8000-{k:012x}'")),
            null(2, format!("'2024-01-{:02} 03:04:{:02}+02'", 1 + k % 28, k % 60)),
            null(3, format!("'2024-01-{:02} 03:04:{:02}'", 1 + k % 28, k % 60)),
            null(4, format!("'2024-03-{:02}'", 1 + k % 28)),
            null(5, quoted(&format!("{{\"k\": {k}, \"a\": [1, 2.50], \"s\": \"x\"}}"))),
            null(6, format!("'\\x{:04x}ff'", k)),
            null(0, (k % 300 - 150).to_string()),
            null(1, (k * 10_000_000_000).to_string()),
        ];
        values.push(format!("({})", row.join(", ")));
    }
    exec_ok(
        db,
        &format!(
            "INSERT INTO src (k, g, i, n2, nu, n15, f8, f4, t, b, u, ts, tsl, d, j, by, sm, big) VALUES {}",
            values.join(", ")
        ),
    )
    .await;
}

/// A target holding every assigned type, the row's values all different from
/// the source's so every assignment shows.
fn target(name: &str, partitioned: bool) -> String {
    let columns = "k INT NOT NULL, g INT NOT NULL, i INT, n2 NUMERIC(10,2), nu NUMERIC, n2w NUMERIC(10,2), \
         f8 DOUBLE PRECISION, f4 REAL, t TEXT, vc VARCHAR(40), b BOOLEAN, u UUID, ts TIMESTAMPTZ, tsl TIMESTAMP, \
         d DATE, j JSONB, by BYTEA, sm SMALLINT, big BIGINT, tf TEXT, cnt INT";
    if partitioned {
        format!("CREATE TABLE {name} ({columns}, PRIMARY KEY (k, g)) PARTITION BY HASH (g)")
    } else {
        format!("CREATE TABLE {name} ({columns}, PRIMARY KEY (k))")
    }
}

async fn fill_target(db: &Arc<Database>, name: &str, partitioned: bool) {
    exec_ok(db, &target(name, partitioned)).await;
    // A row the source has no match for (k = 0), one past it (ROWS + 1).
    let values: Vec<String> = (0..=ROWS + 1).map(|k| format!("({k}, {}, {k}, 'x', 'y')", k % 13)).collect();
    exec_ok(db, &format!("INSERT INTO {name} (k, g, cnt, t, vc) VALUES {}", values.join(", "))).await;
}

fn backfill(name: &str) -> String {
    format!(
        "UPDATE {name} SET i = s.i, n2 = s.n2, nu = s.nu, n2w = s.n15, f8 = s.f8, f4 = s.f4, t = s.t, vc = s.t, \
         b = s.b, u = s.u, ts = s.ts, tsl = s.tsl, d = s.d, j = s.j, by = s.by, sm = s.sm, big = s.big, tf = s.f8, \
         cnt = {name}.cnt + s.k FROM src s WHERE {name}.k = s.k AND s.k % 10 <> 3"
    )
}

/// Every type the batch casts, a computed assignment, a join that leaves
/// rows out, NULLs beside values: the batched table ends byte for byte as the
/// one written row by row, on a plain target and on a partitioned one (whose
/// key is composite, each row named by both columns).
#[tokio::test(flavor = "multi_thread")]
async fn update_from_writes_a_batch_as_its_rows_one_by_one() {
    for partitioned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), "joined_write_update").await;
        fill_source(&db).await;
        let (batched, oracle) = if partitioned { ("ufb_part", "ufo_part") } else { ("ufb_plain", "ufo_plain") };
        join_write_hooks::per_row_only(oracle);
        fill_target(&db, batched, partitioned).await;
        fill_target(&db, oracle, partitioned).await;

        let wal = crate::core::wal::wal_for(&dir.path().to_string_lossy()).expect("the database's log");
        let before = wal.sync_arrivals_for_tests();
        let by_batch = affected(&exec(&db, &backfill(batched)).await, batched);
        let batch_syncs = wal.sync_arrivals_for_tests() - before;
        let before = wal.sync_arrivals_for_tests();
        let by_row = affected(&exec(&db, &backfill(oracle)).await, oracle);
        let row_syncs = wal.sync_arrivals_for_tests() - before;

        assert_eq!(by_batch, by_row, "partitioned {partitioned}: the two counted differently");
        assert_eq!(by_batch, (ROWS - ROWS / 10) as u64, "partitioned {partitioned}");
        assert_eq!(
            cells(&db, batched, "k").await,
            cells(&db, oracle, "k").await,
            "partitioned {partitioned}: a batch wrote a value its row's own statement would not"
        );
        // The point of it: a sync per batch, not per row. Measured on the
        // plain target, whose statement's every write goes to one log.
        if !partitioned {
            assert!(row_syncs >= by_row, "row by row took {row_syncs} syncs for {by_row} rows");
            assert!(batch_syncs <= 10, "the batched statement took {batch_syncs} syncs for {by_batch} rows");
        }
        close(db, dir.path());
    }
}

/// The benchmark's shape: a plain target filled from a source partitioned by
/// a column the join does not name, so the join reads every partition, and a
/// recheck on the target's own column beside the key.
#[tokio::test(flavor = "multi_thread")]
async fn update_from_a_partitioned_source_writes_what_the_rows_would() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "joined_write_partitioned_source").await;
    exec_ok(&db, "CREATE TABLE step (id INT NOT NULL, case_id INT NOT NULL, title TEXT, PRIMARY KEY (id, case_id)) PARTITION BY HASH (case_id)").await;
    let steps: Vec<String> = (1..=40).map(|id| format!("({id}, {}, 'step {id}')", id % 9)).collect();
    exec_ok(&db, &format!("INSERT INTO step (id, case_id, title) VALUES {}", steps.join(", "))).await;
    for name in ["fb", "fo"] {
        exec_ok(&db, &format!("CREATE TABLE {name} (id INT PRIMARY KEY, step_id INT NOT NULL, case_id INT, note TEXT)")).await;
        let fields: Vec<String> = (1..=ROWS).map(|id| format!("({id}, {}, 'n')", 1 + id % 45)).collect();
        exec_ok(&db, &format!("INSERT INTO {name} (id, step_id, note) VALUES {}", fields.join(", "))).await;
    }
    join_write_hooks::per_row_only("fo");
    let sql = |name: &str| {
        format!(
            "UPDATE {name} SET case_id = s.case_id, note = s.title FROM step s WHERE {name}.step_id = s.id AND {name}.note = 'n'"
        )
    };
    let by_batch = affected(&exec(&db, &sql("fb")).await, "fb");
    let by_row = affected(&exec(&db, &sql("fo")).await, "fo");
    assert_eq!(by_batch, by_row);
    // Steps 41-45 do not exist: those fields stay unmatched.
    assert_eq!(by_batch, (1..=ROWS).filter(|id| 1 + id % 45 <= 40).count() as u64);
    assert_eq!(cells(&db, "fb", "id").await, cells(&db, "fo", "id").await);
    // Run again, the recheck (`note = 'n'`) now false for every row: nothing.
    assert_eq!(affected(&exec(&db, &sql("fb")).await, "fb"), 0);
    close(db, dir.path());
}

/// A batch that fails fails the statement with the error the rows one by one
/// give, and nothing of it is applied.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_batch_fails_as_its_row_would_and_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "joined_write_failure").await;
    exec_ok(&db, "CREATE TABLE fsrc (k INT PRIMARY KEY, t TEXT)").await;
    let values: Vec<String> = (1..=ROWS)
        .map(|k| format!("({k}, '{}')", if k == 200 { "much too long" } else { "ok" }))
        .collect();
    exec_ok(&db, &format!("INSERT INTO fsrc (k, t) VALUES {}", values.join(", "))).await;
    for name in ["vb", "vo"] {
        exec_ok(&db, &format!("CREATE TABLE {name} (k INT PRIMARY KEY, t VARCHAR(5))")).await;
        let rows: Vec<String> = (1..=ROWS).map(|k| format!("({k}, 'old')")).collect();
        exec_ok(&db, &format!("INSERT INTO {name} (k, t) VALUES {}", rows.join(", "))).await;
    }
    join_write_hooks::per_row_only("vo");
    let before = cells(&db, "vb", "k").await;
    let sql = |name: &str| format!("UPDATE {name} SET t = s.t FROM fsrc s WHERE {name}.k = s.k");
    let batched = failure(&exec(&db, &sql("vb")).await, "vb");
    let by_row = failure(&exec(&db, &sql("vo")).await, "vo");
    assert_eq!(batched.replace("vb", "vo"), by_row);
    assert_eq!(cells(&db, "vb", "k").await, before, "the failed statement left rows written");
    assert_eq!(cells(&db, "vo", "k").await, before);

    // The same for a delete: a foreign key refuses one row of the batch.
    exec_ok(&db, "CREATE TABLE child (id INT PRIMARY KEY, k INT REFERENCES vb (k))").await;
    exec_ok(&db, "INSERT INTO child (id, k) VALUES (1, 150)").await;
    let delete = failure(&exec(&db, "DELETE FROM vb USING fsrc s WHERE vb.k = s.k").await, "delete");
    assert!(!delete.is_empty());
    assert_eq!(cells(&db, "vb", "k").await, before, "the failed delete removed rows");
    close(db, dir.path());
}

/// `DELETE … USING` removes in batches exactly the rows its rows one by one
/// remove, on a plain target and a partitioned one.
#[tokio::test(flavor = "multi_thread")]
async fn delete_using_removes_a_batch_as_its_rows_one_by_one() {
    for partitioned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), "joined_write_delete").await;
        fill_source(&db).await;
        let (batched, oracle) = if partitioned { ("udb_part", "udo_part") } else { ("udb_plain", "udo_plain") };
        join_write_hooks::per_row_only(oracle);
        fill_target(&db, batched, partitioned).await;
        fill_target(&db, oracle, partitioned).await;
        let sql = |name: &str| format!("DELETE FROM {name} USING src s WHERE {name}.k = s.k AND s.i > 50 AND {name}.cnt % 4 <> 1");
        let by_batch = affected(&exec(&db, &sql(batched)).await, batched);
        let by_row = affected(&exec(&db, &sql(oracle)).await, oracle);
        assert_eq!(by_batch, by_row, "partitioned {partitioned}");
        assert!(by_batch > 100, "partitioned {partitioned}: {by_batch}");
        assert_eq!(cells(&db, batched, "k").await, cells(&db, oracle, "k").await, "partitioned {partitioned}");
        close(db, dir.path());
    }
}
