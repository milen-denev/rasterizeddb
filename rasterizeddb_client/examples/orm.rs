//! A model: its table, CRUD, the query builder, relations, aggregates,
//! keyset pages, upserts and bulk updates.
//!
//! `RDB_ADDR=unix:/tmp/rdb.sock cargo run --example orm`

use std::time::Duration;

use rasterizeddb_client::orm::Cursor;
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
    #[rdb(version)]
    version: i64,
    #[rdb(created_at)]
    created_at: Timestamp,
    #[rdb(updated_at)]
    updated_at: Timestamp,
    #[rdb(soft_delete)]
    deleted_at: Option<Timestamp>,
    #[rdb(has_many(Order, foreign_key = "account_id"))]
    orders: Vec<Order>,
    #[rdb(count_of = "orders")]
    orders_count: i64,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "example_orders")]
struct Order {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(index, references = "example_accounts(id)", on_delete = "cascade")]
    account_id: i64,
    total: i64,
    #[rdb(belongs_to(Account, foreign_key = "account_id"))]
    account: Option<Box<Account>>,
}

fn account(email: &str, plan: Plan, balance: &str) -> Result<Account> {
    Ok(Account {
        id: 0,
        email: email.into(),
        name: None,
        plan,
        balance: Numeric::new(balance)?,
        tags: vec![],
        version: 0,
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
        deleted_at: None,
        orders: vec![],
        orders_count: 0,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "unix:/run/rdb/rdb.sock".into());
    let client = Client::connect(&addr, Options::default()).await?;
    client.create_table::<Account>().await?;
    client.create_table::<Order>().await?;

    // An upsert by the unique email: the server assigns the id.
    let ada = client
        .upsert(&account("ada@example.com", Plan::Pro, "120.50")?)
        .on_conflict(Account::email)
        .await?;
    let bob = client
        .upsert(&account("bob@example.com", Plan::Free, "3")?)
        .on_conflict(Account::email)
        .await?;
    println!("stored {ada:?}");
    for (who, total) in [(&ada, 30), (&ada, 12), (&bob, 5)] {
        client
            .insert(&Order {
                id: 0,
                account_id: who.id,
                total,
                account: None,
            })
            .await?;
    }

    // The query builder, with each account's orders and their count loaded
    // in one statement per relation.
    let pros = Account::query()
        .filter(Account::plan.eq(Plan::Pro) & Account::balance.gt(Numeric::new("100")?))
        .with(Account::orders.order_by(Order::total.desc()))
        .with_count(Account::orders)
        .order_by(Account::email.asc())
        .all(&client)
        .await?;
    println!(
        "{} pro account(s) over 100, the first with {} order(s)",
        pros.len(),
        pros.first().map_or(0, |a| a.orders_count)
    );

    // Aggregates: an integer SUM is cast to BIGINT outside the SUM.
    let per_account: Vec<(i64, i64, Option<i64>)> = Order::query()
        .select((Order::account_id, count_rows(), Order::total.sum()))
        .group_by(Order::account_id)
        .order_by(Order::account_id.asc())
        .all(&client)
        .await?;
    println!("orders per account: {per_account:?}");
    let biggest = Order::query()
        .with(Order::account)
        .order_by(Order::total.desc())
        .first(&client)
        .await?;
    println!(
        "the biggest order is {:?}'s",
        biggest.and_then(|o| o.account).map(|a| a.email)
    );

    // Keyset pages, the cursor carried as a string.
    let mut cursor: Option<String> = None;
    loop {
        let q = Order::query().order_by(Order::total.desc()).limit(2);
        let page = match &cursor {
            Some(text) => q.after(&Cursor::decode(text)?).page(&client).await?,
            None => q.page(&client).await?,
        };
        println!(
            "page: {:?}",
            page.items.iter().map(|o| o.total).collect::<Vec<_>>()
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next.encode()?),
            None => break,
        }
    }

    // Optimistic locking: a stale copy is refused.
    let mut renamed = ada.clone();
    renamed.name = Some("Ada".into());
    let renamed = client.update(&renamed).await?;
    let mut stale = ada.clone();
    stale.name = Some("Lovelace".into());
    match client.update(&stale).await {
        Err(e) if e.is_conflict() => println!("stale write refused: {e}"),
        other => println!("unexpected: {other:?}"),
    }

    // Many rows in one all-or-nothing statement.
    let mut both = vec![renamed, bob];
    for a in &mut both {
        a.tags.push("migrated".into());
    }
    let both = client.update_many(&both).await?;
    println!("tagged {} account(s)", both.len());

    // A soft delete, a read-through cache.
    client.delete(&both[1]).await?;
    println!(
        "live accounts: {}, deleted: {}",
        Account::query().count(&client).await?,
        Account::query().only_deleted().count(&client).await?
    );
    let cached = client.cached::<Account>(Duration::from_secs(30));
    println!(
        "cached read: {:?}",
        cached.find(both[0].id).await?.map(|a| a.email)
    );
    Ok(())
}
