//! PostgreSQL 18's `uuidv7()`, as a column default and in a statement, plus
//! `uuid_extract_version` / `uuid_extract_timestamp`.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn uuidv7_defaults_are_time_ordered_and_readable() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE ev (id UUID NOT NULL DEFAULT uuidv7(), n INTEGER NOT NULL)")
        .await
        .expect("create");
    let before = chrono::Utc::now().timestamp_millis();
    for n in 0..200 {
        sqlx::query("INSERT INTO ev (n) VALUES ($1)")
            .bind(n)
            .execute(&pool)
            .await
            .expect("insert");
    }
    let after = chrono::Utc::now().timestamp_millis();

    let rows = sqlx::query("SELECT id, n FROM ev ORDER BY n")
        .fetch_all(&pool)
        .await
        .expect("read");
    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.get::<uuid::Uuid, _>(0)).collect();
    assert_eq!(ids.len(), 200);
    for id in &ids {
        assert_eq!(id.get_version_num(), 7, "{}", id);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122, "{}", id);
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "generated in order, so sorted in order");

    // Ordering by the id is insertion order.
    let by_id: Vec<i32> = sqlx::query("SELECT n FROM ev ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("order by id")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
    assert_eq!(by_id, (0..200).collect::<Vec<_>>());

    // The functions over a stored value.
    let row = sqlx::query(
        "SELECT uuid_extract_version(id), uuid_extract_timestamp(id) FROM ev WHERE n = 0",
    )
    .fetch_one(&pool)
    .await
    .expect("extract");
    assert_eq!(row.get::<i16, _>(0), 7);
    let ts = row.get::<chrono::DateTime<chrono::Utc>, _>(1).timestamp_millis();
    // A second of slack: the monotonic step may run the timestamp slightly
    // ahead of the clock.
    assert!((before..=after + 1000).contains(&ts), "{} not in {}..{}", ts, before, after);
}
