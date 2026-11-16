//! The key/value cache, typed.
//!
//! `RDB_ADDR=unix:/tmp/rdb.sock cargo run --example kv`

use std::time::Duration;

use rasterizeddb_client::prelude::*;

#[derive(Model, Debug, Clone)]
#[rdb(table = "session")]
struct Session {
    #[rdb(primary_key)]
    token: Uuid,
    user_id: i64,
    seen: Timestamp,
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "unix:/run/rdb/rdb.sock".into());
    let client = Client::connect(&addr, Options::default()).await?;
    let kv = client.kv();

    kv.set_as("Value 1", &41i32, None).await?;
    println!("Value 1 = {:?}", kv.get_as::<i32>("Value 1").await?);

    let hits = kv.map::<String, i64>("example_hits");
    for path in ["/", "/api/a", "/api/b", "/api/b"] {
        hits.incr_by(&path.to_string(), 1).await?;
    }
    let api = hits
        .query()
        .prefix("/api/")
        .filter(|_, n| *n > 1)
        .all()
        .await?;
    println!("busy api paths: {api:?}");

    let sessions = kv.repo::<Session>();
    let session = Session {
        token: Uuid::new_v4()?,
        user_id: 7,
        seen: Timestamp::now(),
    };
    sessions
        .save_with_ttl(&session, Duration::from_secs(60))
        .await?;
    println!("session: {:?}", sessions.get(&[&session.token]).await?);
    println!(
        "user 7's sessions: {}",
        sessions.filter(|s| s.user_id == 7).await?.len()
    );
    Ok(())
}
