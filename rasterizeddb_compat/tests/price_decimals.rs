//! The seeded billing rates, stored and read back.
//!
//! Every value here is a real rate from the price seed. A decimal that comes
//! back wrong is not a rounding curiosity: `0.02` read as `0.20` is a tenfold
//! overcharge, and a rate that reads as `0` skips the charge silently.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};
use sqlx::types::Decimal;
use std::str::FromStr;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

const SEED: &[(&str, &str)] = &[
    ("Monthly_Subscription", "5.00"),
    ("License_Subscription_Tier_1", "15.00"),
    ("License_Subscription_Tier_2", "50.00"),
    ("Storage", "0.20"),
    ("Cold_Storage", "0.02"),
    ("Input_Tokens", "1.17"),
    ("Cached_Input_Tokens", "0.17"),
    ("Output_Tokens", "3.52"),
];

#[tokio::test]
async fn every_seeded_price_round_trips() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE prices (id INT PRIMARY KEY, price NUMERIC(38,16) NOT NULL, \"group\" TEXT NOT NULL, name TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    // Inserted as literals, the way the seed migration writes them.
    for (i, (name, price)) in SEED.iter().enumerate() {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO prices (id, price, \"group\", name) VALUES ({}, {}, 'general', '{}')",
            i, price, name
        )))
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", name, e));
    }

    // Read back through the extended protocol, as the application does.
    for (i, (name, price)) in SEED.iter().enumerate() {
        let got: Decimal = sqlx::query("SELECT price FROM prices WHERE id = $1")
            .bind(i as i32)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("select {}: {}", name, e))
            .try_get("price")
            .unwrap_or_else(|e| panic!("decode {}: {}", name, e));
        let want = Decimal::from_str(price).expect("literal parses");
        assert_eq!(got, want, "{} came back as {} not {}", name, got, price);
    }

    // The lookup the biller actually performs: by name and group.
    let got: Decimal = sqlx::query("SELECT price FROM prices WHERE name = $1 AND \"group\" = $2")
        .bind("Cold_Storage")
        .bind("general")
        .fetch_one(&pool)
        .await
        .expect("price lookup")
        .try_get("price")
        .expect("decode");
    assert_eq!(got, Decimal::from_str("0.02").unwrap(), "Cold_Storage");

    // And arithmetic over them, which is where a scale error would surface as
    // a wrong invoice rather than a wrong display.
    let total: Decimal = pool
        .fetch_one(sqlx::raw_sql("SELECT SUM(price) AS total FROM prices"))
        .await
        .expect("sum")
        .try_get("total")
        .expect("decode sum");
    // 5 + 15 + 50 + 0.20 + 0.02 + 1.17 + 0.17 + 3.52
    assert_eq!(total, Decimal::from_str("75.08").unwrap(), "sum of the rates");
}
