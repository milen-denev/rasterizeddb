//! `COUNT(*) FILTER (WHERE …)` counts only the rows the clause admits.
//!
//! The clause belongs to one aggregate, not to the statement: a query pairs a
//! filtered aggregate with an unfiltered one, so moving the test into `WHERE`
//! would change both. Two ways this failed silently before: the filter's column
//! was not fetched, so the condition tested a column that was not there and
//! rejected every row; and a cast around the call — `COUNT(*) FILTER (…)::bigint`,
//! which is how it is usually written — sent the item down a path that dropped
//! the filter and counted everything.

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

/// The little-endian integers one result row carries, in output order.
///
/// The response is rows of bytes, not text, so an assertion on the rendered
/// string would pass on almost anything.
fn numbers(response: &[u8]) -> Vec<i64> {
    assert_eq!(
        response.first(),
        Some(&2u8),
        "not a row response: {:?}",
        response
    );
    let rows = vec_into_rows(&response[1..]).expect("rows");
    rows.first()
        .map(|row| {
            row.columns
                .iter()
                .map(|c| {
                    let bytes = c.data.into_slice();
                    let mut buf = [0u8; 8];
                    let n = bytes.len().min(8);
                    buf[..n].copy_from_slice(&bytes[..n]);
                    i64::from_le_bytes(buf)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_aggregate_counts_only_its_own_rows() {
    init_globals();
    let dir = format!("/tmp/rdb_aggregate_filter_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(
        &db,
        "CREATE TABLE a (id INT NOT NULL, status TEXT NOT NULL, tokens INT NOT NULL)",
    )
    .await;
    run(
        &db,
        "INSERT INTO a (id, status, tokens) VALUES (1, 'ok', 10)",
    )
    .await;
    run(
        &db,
        "INSERT INTO a (id, status, tokens) VALUES (2, 'failed', 20)",
    )
    .await;
    run(
        &db,
        "INSERT INTO a (id, status, tokens) VALUES (3, 'failed', 30)",
    )
    .await;

    // `status` is read by the filter alone — no aggregate names it.
    let out = run(
        &db,
        "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'failed') AS failed FROM a",
    )
    .await;
    assert_eq!(numbers(&out), vec![3, 2], "the filter was not applied");

    // The cast form, which is how an ORM writes it — and which used to send the
    // item down a path that dropped the filter and counted every row.
    let cast = run(
        &db,
        "SELECT COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed FROM a",
    )
    .await;
    assert_eq!(numbers(&cast), vec![2], "the cast form lost the filter");

    let summed = run(
        &db,
        "SELECT SUM(tokens) FILTER (WHERE status = 'failed') AS failed_tokens FROM a",
    )
    .await;
    assert_eq!(numbers(&summed), vec![50], "SUM ignored its filter");

    let _ = std::fs::remove_dir_all(&dir);
}
