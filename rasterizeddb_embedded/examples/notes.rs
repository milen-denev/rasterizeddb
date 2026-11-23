//! An application with its database inside: `cargo run --example notes -- ./notes-data`.
//! Run it twice and the second run finds the first run's note.

use rasterizeddb_embedded::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./notes-data".to_string());
    let mut builder = Builder::open(&dir);
    if cfg!(not(unix)) {
        builder = builder.password("notes", "change-me");
    }
    let db = builder.start().await?;
    let client = db.connect().await?;

    client
        .execute("CREATE TABLE IF NOT EXISTS notes (id BIGSERIAL, body TEXT NOT NULL, PRIMARY KEY (id))", &[])
        .await?;
    client.execute("INSERT INTO notes (body) VALUES ('started once more')", &[]).await?;
    let rows = client.query("SELECT id, body FROM notes ORDER BY id", &[]).await?;
    for row in rows.iter() {
        println!("{}: {}", row.get::<i64>(0)?, row.get::<String>(1)?);
    }

    drop(client);
    db.shutdown().await?;
    Ok(())
}
