//! `GROUP BY` with no aggregate in the SELECT list.
//!
//! `SELECT city FROM t GROUP BY city` is a DISTINCT, and legal PostgreSQL. So
//! is keeping the only aggregate in the HAVING — `GROUP BY city HAVING
//! count(*) > 1` — which the SELECT list never mentions. Both were rejected
//! with "GROUP BY requires an aggregate in the SELECT list", because what
//! turned the grouped path on was an aggregate call rather than the GROUP BY
//! itself. sea-orm emits the first form for any `.distinct()` over a column.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        row::row::vec_into_rows,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
    },
};
use rclite::Arc;

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
}

async fn run(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    execute(purpose, db.clone()).await
}

/// The first column of every result row, as text.
///
/// Asserting on the rendered response would pass on almost anything, so the
/// rows are decoded and the group keys read out of them.
fn first_column_strings(response: &[u8]) -> Vec<String> {
    assert_eq!(
        response.first(),
        Some(&2u8),
        "not a row response: {:?}",
        response
    );
    let rows = vec_into_rows(&response[1..]).expect("rows");
    let mut out: Vec<String> = rows
        .iter()
        .filter_map(|row| row.columns.first())
        .map(|c| {
            String::from_utf8_lossy(c.data.into_slice())
                .trim()
                .to_string()
        })
        .collect();
    out.sort();
    out
}

fn first_column_ints(response: &[u8]) -> Vec<i64> {
    assert_eq!(
        response.first(),
        Some(&2u8),
        "not a row response: {:?}",
        response
    );
    let rows = vec_into_rows(&response[1..]).expect("rows");
    let mut out: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.columns.first())
        .map(|c| {
            let bytes = c.data.into_slice();
            let mut buf = [0u8; 8];
            let n = bytes.len().min(8);
            buf[..n].copy_from_slice(&bytes[..n]);
            i64::from_le_bytes(buf)
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_alone_collapses_duplicates() {
    init_globals();
    let dir = format!("/tmp/rdb_group_no_agg_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(
        &db,
        "CREATE TABLE g (id INT NOT NULL, city TEXT NOT NULL, n INT NOT NULL)",
    )
    .await;
    for (id, city, n) in [
        (1, "sofia", 1),
        (2, "sofia", 2),
        (3, "varna", 3),
        (4, "plovdiv", 4),
        (5, "varna", 5),
    ] {
        run(
            &db,
            &format!(
                "INSERT INTO g (id, city, n) VALUES ({}, '{}', {})",
                id, city, n
            ),
        )
        .await;
    }

    // One row per distinct city, not one per input row.
    let out = run(&db, "SELECT city FROM g GROUP BY city").await;
    assert_eq!(
        first_column_strings(&out),
        vec![
            "plovdiv".to_string(),
            "sofia".to_string(),
            "varna".to_string()
        ],
        "GROUP BY without an aggregate did not collapse the duplicates"
    );

    // The aggregate lives only in the HAVING, so the select list has none.
    let having = run(&db, "SELECT city FROM g GROUP BY city HAVING COUNT(*) > 1").await;
    assert_eq!(
        first_column_strings(&having),
        vec!["sofia".to_string(), "varna".to_string()],
        "HAVING was not applied to a select list without an aggregate"
    );

    // Two keys, to prove the grouping is on the tuple rather than the first
    // column alone.
    let pair = run(&db, "SELECT city, n FROM g GROUP BY city, n").await;
    assert_eq!(
        first_column_strings(&pair).len(),
        5,
        "grouping on two keys collapsed rows that differ in the second"
    );

    // A numeric key takes the same path.
    let nums = run(&db, "SELECT n FROM g GROUP BY n").await;
    assert_eq!(first_column_ints(&nums), vec![1, 2, 3, 4, 5]);

    // Still rejected: a column that is neither a key nor inside an aggregate
    // has no single value within the group.
    let bad = run(&db, "SELECT city, id FROM g GROUP BY city").await;
    assert_ne!(bad.first(), Some(&2u8), "an ungrouped column was allowed");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_alone_works_over_a_join() {
    init_globals();
    let dir = format!("/tmp/rdb_group_no_agg_join_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE p (id INT NOT NULL, city TEXT NOT NULL)").await;
    run(&db, "CREATE TABLE q (id INT NOT NULL, p_id INT NOT NULL)").await;
    for (id, city) in [(1, "sofia"), (2, "varna")] {
        run(
            &db,
            &format!("INSERT INTO p (id, city) VALUES ({}, '{}')", id, city),
        )
        .await;
    }
    // Two rows against `sofia`, so an ungrouped join would repeat it.
    for (id, p_id) in [(10, 1), (11, 1), (12, 2)] {
        run(
            &db,
            &format!("INSERT INTO q (id, p_id) VALUES ({}, {})", id, p_id),
        )
        .await;
    }

    let out = run(
        &db,
        "SELECT p.city FROM p JOIN q ON q.p_id = p.id GROUP BY p.city",
    )
    .await;
    assert_eq!(
        first_column_strings(&out),
        vec!["sofia".to_string(), "varna".to_string()],
        "the join repeated a group key"
    );

    let having = run(
        &db,
        "SELECT p.city FROM p JOIN q ON q.p_id = p.id GROUP BY p.city HAVING COUNT(*) > 1",
    )
    .await;
    assert_eq!(
        first_column_strings(&having),
        vec!["sofia".to_string()],
        "HAVING was not applied over the join"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
