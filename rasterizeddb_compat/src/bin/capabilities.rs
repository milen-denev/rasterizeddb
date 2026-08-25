//! Prints the PostgreSQL feature-compatibility matrix for RasterizedDB.
//!
//! `cargo run -p rasterizeddb_compat --bin capabilities`

use rasterizeddb_compat::{TestServer, probes};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = TestServer::start().await?;
    let pool = server.connect_sqlx().await?;

    println!("RasterizedDB / PostgreSQL compatibility matrix");
    println!("server: {}\n", server.url());

    match probes::apply_schema(&pool).await {
        Ok(()) => println!("schema: applied\n"),
        Err(e) => {
            println!("schema: FAILED\n  {}\n", e);
            println!("Every probe below runs against an incomplete schema.\n");
        }
    }

    let results = probes::run_all(&pool).await?;

    let mut area = "";
    let (mut pass, mut fail) = (0usize, 0usize);
    for r in &results {
        if r.probe.area != area {
            area = r.probe.area;
            println!("[{}]", area);
        }
        if r.passed() {
            pass += 1;
            println!("  PASS  {}", r.probe.name);
        } else {
            fail += 1;
            println!("  FAIL  {}  -- {}", r.probe.name, r.failure.as_deref().unwrap_or(""));
        }
    }

    let total = pass + fail;
    println!(
        "\n{} / {} probes pass ({:.0}%)",
        pass,
        total,
        if total == 0 { 0.0 } else { pass as f64 * 100.0 / total as f64 }
    );

    Ok(())
}
