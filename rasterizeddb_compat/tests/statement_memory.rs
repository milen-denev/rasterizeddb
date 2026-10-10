//! Server memory held per pooled connection for its prepared statements.
//!
//! `cargo test --release -p rasterizeddb_compat --test statement_memory -- --ignored --nocapture`
//!
//! Measured as the process's resident size: the clients run in the same
//! process, so this is an A/B instrument (same client work both times), not an
//! absolute figure.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};

fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm.split_whitespace().nth(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    pages * 4096
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prepared_statements_across_many_connections() {
    const CONNECTIONS: usize = 200;
    const STATEMENTS: usize = 100;
    let server = TestServer::start().await.expect("server");
    let mut setup = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    setup
        .execute(AssertSqlSafe(
            "CREATE TABLE sm (id INT PRIMARY KEY, customer_name TEXT, order_total BIGINT, created_at TIMESTAMP, status TEXT, notes TEXT)",
        ))
        .await
        .unwrap();
    setup
        .execute(AssertSqlSafe("INSERT INTO sm (id, customer_name, order_total, status) VALUES (1, 'a', 1, 'x')"))
        .await
        .unwrap();

    let before = rss_bytes();
    let mut conns = Vec::new();
    for _ in 0..CONNECTIONS {
        let mut c = sqlx::PgConnection::connect(&server.url()).await.unwrap();
        for k in 0..STATEMENTS {
            let sql = format!(
                "SELECT id, customer_name, order_total, created_at, status, notes FROM sm \
                 WHERE id = $1 AND order_total >= {k} ORDER BY id"
            );
            sqlx::query(AssertSqlSafe(sql)).bind(1i32).fetch_all(&mut c).await.unwrap();
        }
        conns.push(c);
    }
    let after = rss_bytes();
    println!(
        "MEMORY {} connections x {} statements: RSS +{:.1} MiB ({:.1} KiB per connection)",
        CONNECTIONS,
        STATEMENTS,
        (after.saturating_sub(before)) as f64 / (1024.0 * 1024.0),
        (after.saturating_sub(before)) as f64 / 1024.0 / CONNECTIONS as f64
    );
}

/// Server memory an idle session keeps after returning one sizeable result:
/// its output buffer grows to 64 KiB for the result and used to keep it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn idle_connections_after_a_large_result() {
    const CONNECTIONS: usize = 200;
    let server = TestServer::start().await.expect("server");
    let mut setup = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    setup
        .execute(AssertSqlSafe("CREATE TABLE big_rows (id INT PRIMARY KEY, label TEXT NOT NULL)"))
        .await
        .unwrap();
    let values = (0..2000)
        .map(|i| format!("({i}, 'label number {i} padded out a little')"))
        .collect::<Vec<_>>()
        .join(",");
    setup
        .execute(AssertSqlSafe(format!("INSERT INTO big_rows (id, label) VALUES {values}")))
        .await
        .unwrap();

    let before = rss_bytes();
    let mut conns = Vec::new();
    for _ in 0..CONNECTIONS {
        let mut c = sqlx::PgConnection::connect(&server.url()).await.unwrap();
        let rows = sqlx::raw_sql(AssertSqlSafe("SELECT id, label FROM big_rows"))
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2000);
        drop(rows);
        conns.push(c);
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let after = rss_bytes();
    println!(
        "IDLE {} connections after a ~100 KB result: RSS +{:.1} MiB ({:.1} KiB per connection)",
        CONNECTIONS,
        (after.saturating_sub(before)) as f64 / (1024.0 * 1024.0),
        (after.saturating_sub(before)) as f64 / 1024.0 / CONNECTIONS as f64
    );
}
