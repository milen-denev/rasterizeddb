//! A model: its table, CRUD and the query builder.
//!
//! `RDB_ADDR=unix:/tmp/rdb.sock cargo run --example orm`

use rasterizeddb_client::prelude::*;

#[derive(SqlEnum, Debug, Clone, Copy, PartialEq)]
enum Plan {
    Free,
    Pro,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "example_accounts")]
struct Account {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(unique)]
    email: String,
    name: Option<String>,
    plan: Plan,
    balance: Numeric,
    tags: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "unix:/run/rdb/rdb.sock".into());
    let client = Client::connect(&addr, Options::default()).await?;
    client.create_table::<Account>().await?;

    let ada = client
        .upsert(&Account {
            id: 1,
            email: "ada@example.com".into(),
            name: Some("Ada".into()),
            plan: Plan::Pro,
            balance: Numeric::new("120.50")?,
            tags: vec!["founder".into()],
        })
        .await?;
    println!("stored {ada:?}");

    let pros = Account::query()
        .filter(Account::plan.eq(Plan::Pro) & Account::balance.gt(Numeric::new("100")?))
        .order_by(Account::email.asc())
        .all(&client)
        .await?;
    println!("{} pro account(s) over 100", pros.len());

    let renamed = Account::query()
        .filter(Account::id.eq(ada.id))
        .update()
        .set(Account::name, "Ada L.")
        .run(&client)
        .await?;
    println!("renamed: {renamed:?}");
    Ok(())
}
