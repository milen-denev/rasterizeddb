//! Migrations run when the client connects.
//!
//! `RDB_ADDR=unix:/tmp/rdb.sock cargo run --example migrations`

use rasterizeddb_client::orm::{AutoMigrate, Migrator, SyncPolicy};
use rasterizeddb_client::prelude::*;

#[derive(Model, Debug)]
#[rdb(table = "example_people")]
struct Person {
    #[rdb(primary_key, serial)]
    id: i64,
    name: String,
    #[rdb(default = "'active'")]
    status: String,
}

fn migrations() -> Migrator {
    Migrator::new()
        .table("example_migrations")
        .step(
            "0001_settings",
            &["CREATE TABLE IF NOT EXISTS example_settings (k TEXT NOT NULL, v TEXT, PRIMARY KEY (k))"],
        )
        .down(&["DROP TABLE IF EXISTS example_settings"])
        .code_step("0002_defaults", "v1", |c| {
            Box::pin(async move {
                c.exec(
                    "INSERT INTO example_settings (k, v) VALUES ($1, $2) ON CONFLICT (k) DO NOTHING",
                    params!["theme", "dark"],
                )
                .await?;
                Ok(())
            })
        })
        .model::<Person>()
        .sync_policy(SyncPolicy::default())
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "unix:/run/rdb/rdb.sock".into());
    let client = Client::connect(
        &addr,
        Options {
            migrations: Some(migrations()),
            auto_migrate: AutoMigrate::Apply,
            ..Options::default()
        },
    )
    .await?;
    for step in migrations().status(&client).await? {
        println!("{:<16} {:?} {:?}", step.name, step.state, step.applied_at);
    }
    let plan = migrations().plan(&client).await?;
    println!("up to date: {}", plan.is_up_to_date());
    Ok(())
}
