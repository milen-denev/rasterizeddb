//! Runs a single SQL statement against a fresh server, for narrowing down a
//! specific failure: `cargo run -p rasterizeddb_compat --example probe_one -- "<sql>"`
use sqlx::AssertSqlSafe;
use rasterizeddb_compat::{TestServer, probes};
use sqlx::{Column, Row, ValueRef};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sql = std::env::args().nth(1).expect("usage: probe_one <sql>");
    let server = TestServer::start().await?;
    let pool = server.connect_sqlx().await?;
    probes::apply_schema(&pool).await?;

    let rows = sqlx::query(AssertSqlSafe(&*sql)).fetch_all(&pool).await?;
    println!("-- {} row(s)", rows.len());
    for r in &rows {
        let cells: Vec<String> = (0..r.columns().len())
            .map(|i| {
                let name = r.column(i).name().to_string();
                let v = match r.try_get_raw(i) {
                    Ok(v) if v.is_null() => "NULL".to_string(),
                    Ok(v) => v
                        .as_bytes()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_else(|_| "<binary>".into()),
                    Err(e) => format!("<err {}>", e),
                };
                format!("{}={:?}", name, v)
            })
            .collect();
        println!("   {}", cells.join("  "));
    }
    Ok(())
}
