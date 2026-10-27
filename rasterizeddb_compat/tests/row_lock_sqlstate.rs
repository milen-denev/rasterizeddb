//! A writer refused at a row another session holds `FOR UPDATE` answers
//! 55P03 (lock_not_available), as a NOWAIT does, not XX000.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_meeting_a_row_lock_gets_55p03() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE rl (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO rl VALUES (1, 1), (2, 2);"))
        .await
        .unwrap();
    let mut holder = pool.acquire().await.unwrap();
    holder.execute("BEGIN").await.unwrap();
    let held = sqlx::query("SELECT id FROM rl WHERE id = 1 FOR UPDATE").fetch_all(&mut *holder).await.unwrap();
    assert_eq!(held.len(), 1);
    for sql in ["UPDATE rl SET v = 9 WHERE id = 1", "DELETE FROM rl WHERE id = 1"] {
        let e = pool.execute(sql).await.expect_err(sql);
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("55P03"), "{sql}: {e}");
    }
    // A row nobody holds is written.
    pool.execute("UPDATE rl SET v = 7 WHERE id = 2").await.unwrap();
    holder.execute("COMMIT").await.unwrap();
    drop(holder);
    pool.execute("UPDATE rl SET v = 9 WHERE id = 1").await.expect("after the holder committed");
    let v: i64 = sqlx::query("SELECT v FROM rl WHERE id = 1").fetch_one(&pool).await.unwrap().get(0);
    assert_eq!(v, 9);
}
