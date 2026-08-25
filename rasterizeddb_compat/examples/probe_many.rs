//! Runs several statements in order against one fresh server, for exercising a
//! DDL sequence: `cargo run -p rasterizeddb_compat --example probe_many -- "stmt" "stmt" …`
use rasterizeddb_compat::{TestServer, probes};
use sqlx::{Column, Row, ValueRef};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let statements: Vec<String> = std::env::args().skip(1).collect();
    let server = TestServer::start().await?;
    let pool = server.connect_sqlx().await?;
    probes::apply_schema(&pool).await?;

    for sql in &statements {
        print!("-- {}\n", sql);
        match sqlx::query(sql).fetch_all(&pool).await {
            Err(e) => println!("   ERROR: {}", e.to_string().lines().next().unwrap_or("")),
            Ok(rows) => {
                println!("   ok, {} row(s)", rows.len());
                for r in rows.iter().take(5) {
                    let cells: Vec<String> = (0..r.columns().len())
                        .map(|i| {
                            let name = r.column(i).name();
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
                    println!("     {}", cells.join("  "));
                }
            }
        }
    }
    Ok(())
}
