use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Executor;

#[tokio::test]
async fn isolate_null_into_jsonb() {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE p (id INT PRIMARY KEY, j JSONB)"))
        .await
        .expect("create");

    for (label, sql) in [
        ("plain NULL, no cast", "INSERT INTO p (id, j) VALUES (1, NULL)"),
        ("NULL with ::jsonb cast", "INSERT INTO p (id, j) VALUES (2, NULL::jsonb)"),
        ("document, no cast", "INSERT INTO p (id, j) VALUES (3, '{\"a\":1}')"),
        ("document with cast", "INSERT INTO p (id, j) VALUES (4, '{\"a\":1}'::jsonb)"),
    ] {
        let r = pool.execute(sqlx::raw_sql(sql)).await;
        println!("{:<26} -> {}", label, match r {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string().lines().next().unwrap_or("").to_string(),
        });
    }

    // And the bound forms.
    for (label, cast) in [("bound NULL, no cast", ""), ("bound NULL with cast", "::jsonb")] {
        let sql = format!("INSERT INTO p (id, j) VALUES ($1, $2{})", cast);
        let r = sqlx::query(AssertSqlSafe(&*sql))
            .bind(if cast.is_empty() { 10i32 } else { 11i32 })
            .bind(Option::<String>::None)
            .execute(&pool)
            .await;
        println!("{:<26} -> {}", label, match r {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string().lines().next().unwrap_or("").to_string(),
        });
    }
}
