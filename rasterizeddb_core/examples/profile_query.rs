// Self-contained profiling driver for the query hot path.
// Inserts a moderate number of rows and runs a series of WHERE-filtered
// scans in a loop, suitable for `perf record` / `cargo flamegraph`.

use rclite::Arc;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

const ROWS: usize = 200_000;
const QUERY_ITERATIONS: usize = 20;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let _ = MAX_PERMITS_THREADS.get_or_init(|| 8);
    // BATCH_SIZE tuning: smaller batches keep more workers busy when the
    // total row count is modest (e.g., 200K rows / 64K batch = only 3 batches,
    // leaving most workers idle). 4K-8K is a sweet spot for this benchmark.
    let _ = BATCH_SIZE.get_or_init(|| 1024 * 4);
    let _ = ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_profile_")
        .tempdir()
        .expect("tempdir")
        .keep();
    let db_dir_str = db_dir.to_str().unwrap().to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    exec(
        &database,
        r##"CREATE TABLE bench (
            id UBIGINT,
            age INTEGER,
            salary DOUBLE
        );"##,
    )
    .await;

    let setup = std::time::Instant::now();
    eprintln!("Inserting {ROWS} rows...");
    for i in 0..ROWS {
        let id = (i as u64) + 1;
        let age = (i % 90) as i32;
        let salary = 30_000.0 + ((i % 200_000) as f64);
        let sql = format!(
            "INSERT INTO bench (id, age, salary) VALUES ({}, {}, {});",
            id, age, salary
        );
        exec(&database, &sql).await;
    }
    eprintln!("Insert took {:.2?}", setup.elapsed());

    // Warm up
    for _ in 0..2 {
        exec(
            &database,
            "SELECT id, age, salary FROM bench WHERE salary > 100000.0",
        )
        .await;
    }

    eprintln!(
        "Running {QUERY_ITERATIONS} iterations of full-scan WHERE-filtered query..."
    );
    let t0 = std::time::Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..QUERY_ITERATIONS {
        let r = exec(
            &database,
            "SELECT id, age, salary FROM bench WHERE salary > 100000.0",
        )
        .await;
        total_bytes += r.len();
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "Done. {QUERY_ITERATIONS} iterations, {total_bytes} bytes returned in {:.2?} ({:.2?} avg)",
        elapsed,
        elapsed / (QUERY_ITERATIONS as u32)
    );
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    executor::execute(purpose, db.clone()).await
}
