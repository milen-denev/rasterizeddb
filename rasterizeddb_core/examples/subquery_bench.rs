// Existence- and membership-only subqueries: what narrowing the inner query
// (`SubqueryNeed`) and de-duplicating an IN list buy.
//
// Each measurement is interleaved A/B in one process:
//   A emulates the old path — the inner query run as written (every row, ORDER
//     BY included), decoded, and for IN rendered with every duplicate — then
//     the outer statement over that result;
//   B is the statement as the engine now runs it.
//
//   cargo run --release -p rasterizeddb_core --example subquery_bench -- /var/tmp/<dir>
//
// The directory is created and must be deleted by the caller afterwards.

use rclite::Arc;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

const BIG_ROWS: usize = 100_000;
const ROUNDS: usize = 5;
const ITERS: usize = 5;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let _ = MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = BATCH_SIZE.get_or_init(|| 1024 * 4);
    let _ = ENABLE_CACHE.get_or_init(|| false);

    let dir = std::env::args()
        .nth(1)
        .expect("usage: subquery_bench <database directory under /var/tmp>");
    std::fs::create_dir_all(&dir).expect("create the database directory");
    let database = Arc::new(Database::new(&dir).await);

    exec(&database, "CREATE TABLE big (id BIGINT, k INTEGER, v INTEGER)").await;
    exec(&database, "CREATE TABLE small (id INTEGER, k INTEGER)").await;
    let t = std::time::Instant::now();
    let mut batch = String::new();
    for i in 0..BIG_ROWS {
        if batch.is_empty() {
            batch.push_str("INSERT INTO big (id, k, v) VALUES ");
        } else {
            batch.push_str(", ");
        }
        batch.push_str(&format!(
            "({}, {}, {})",
            i,
            i % 1000,
            (i * 7919) % 100_003
        ));
        if (i + 1) % 1000 == 0 {
            exec(&database, &batch).await;
            batch.clear();
        }
    }
    for i in 0..2000 {
        exec(
            &database,
            &format!("INSERT INTO small (id, k) VALUES ({}, {})", i, i),
        )
        .await;
    }
    eprintln!("loaded {} rows in {:.2?}", BIG_ROWS, t.elapsed());

    // Sanity: both forms agree before anything is timed.
    let exists_b = exec(&database, "SELECT id FROM small WHERE NOT EXISTS (SELECT 1 FROM big)").await;
    let in_b = exec(
        &database,
        "SELECT id FROM small WHERE k IN (SELECT k FROM big ORDER BY v)",
    )
    .await;
    assert_eq!(exists_b.first(), Some(&2u8));
    assert_eq!(in_b.first(), Some(&2u8));
    let in_rows = rasterizeddb_core::core::row::row::vec_into_rows(&in_b[1..]).unwrap();
    assert_eq!(in_rows.len(), 1000, "small ids 0..999 have a k in big");

    for round in 0..ROUNDS {
        // --- uncorrelated NOT EXISTS ---
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            let inner = exec(&database, "SELECT 1 FROM big").await;
            let rows = rasterizeddb_core::core::row::row::vec_into_rows(&inner[1..]).unwrap();
            let predicate = if rows.is_empty() { "1 = 1" } else { "1 = 0" };
            std::hint::black_box(
                exec(&database, &format!("SELECT id FROM small WHERE {}", predicate)).await,
            );
        }
        let exists_a = t.elapsed() / ITERS as u32;
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(
                exec(&database, "SELECT id FROM small WHERE NOT EXISTS (SELECT 1 FROM big)").await,
            );
        }
        let exists_b = t.elapsed() / ITERS as u32;

        // --- IN over a large, ordered, duplicate-heavy subquery ---
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            let inner = exec(&database, "SELECT k FROM big ORDER BY v").await;
            let rows = rasterizeddb_core::core::row::row::vec_into_rows(&inner[1..]).unwrap();
            let mut parts = Vec::with_capacity(rows.len());
            for row in &rows {
                let c = &row.columns[0];
                parts.push(
                    rasterizeddb_core::core::rql::subquery::value_to_literal(
                        &c.column_type,
                        c.data.into_slice(),
                    )
                    .unwrap(),
                );
            }
            let sql = format!("SELECT id FROM small WHERE k IN ({})", parts.join(", "));
            std::hint::black_box(exec(&database, &sql).await);
        }
        let in_a = t.elapsed() / ITERS as u32;
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(
                exec(
                    &database,
                    "SELECT id FROM small WHERE k IN (SELECT k FROM big ORDER BY v)",
                )
                .await,
            );
        }
        let in_b = t.elapsed() / ITERS as u32;

        // --- scalar subquery with no LIMIT ---
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            let inner = exec(&database, "SELECT k FROM big WHERE id >= 0").await;
            let rows = rasterizeddb_core::core::row::row::vec_into_rows(&inner[1..]).unwrap();
            std::hint::black_box(rows.len() > 1);
        }
        let scalar_a = t.elapsed() / ITERS as u32;
        let t = std::time::Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(
                exec(
                    &database,
                    "SELECT id FROM small WHERE k = (SELECT k FROM big WHERE id >= 0)",
                )
                .await,
            );
        }
        let scalar_b = t.elapsed() / ITERS as u32;

        println!(
            "round {round}: NOT EXISTS old {:.2?} new {:.2?} | IN old {:.2?} new {:.2?} | scalar(>1 row error) old {:.2?} new {:.2?}",
            exists_a, exists_b, in_a, in_b, scalar_a, scalar_b
        );
    }
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    let out = executor::execute(purpose, db.clone()).await;
    if out.first() == Some(&3) && !sql.contains("WHERE k = (SELECT") {
        panic!("{}: {}", sql, String::from_utf8_lossy(&out[1..]));
    }
    out
}
