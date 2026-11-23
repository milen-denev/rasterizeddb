//! Connect, write and read with plain SQL.
//!
//! `RDB_ADDR=unix:/tmp/rdb.sock cargo run --example quickstart`

use rasterizeddb_client::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "unix:/run/rdb/rdb.sock".into());
    let client = Client::connect(&addr, Options::default()).await?;

    client
        .run("CREATE TABLE IF NOT EXISTS quickstart (id BIGSERIAL, body TEXT NOT NULL, score DOUBLE PRECISION, PRIMARY KEY (id))")
        .await?;
    let id: i64 = client
        .fetch_scalar(
            "INSERT INTO quickstart (body, score) VALUES ($1, $2) RETURNING id",
            params!["it's a note", 4.5f64],
        )
        .await?;
    println!("inserted {id}");

    let rows: Vec<(i64, String, Option<f64>)> = client
        .fetch_all(
            "SELECT id, body, score FROM quickstart ORDER BY id DESC LIMIT $1",
            params![5i64],
        )
        .await?;
    for (id, body, score) in rows {
        println!("{id}: {body} ({score:?})");
    }
    Ok(())
}
