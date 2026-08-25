use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row, Column, TypeInfo};

#[tokio::test]
async fn every_smallint_declaration_reports_int2() {
    let s = TestServer::start().await.expect("start");
    let pool = s.connect_sqlx().await.expect("connect");
    let forms = [
        ("plain",        "CREATE TABLE t1 (id INT PRIMARY KEY, case_state SMALLINT)"),
        ("not_null_def", "CREATE TABLE t2 (id INT PRIMARY KEY, case_state SMALLINT NOT NULL DEFAULT 0)"),
        ("int2",         "CREATE TABLE t3 (id INT PRIMARY KEY, case_state INT2)"),
        ("quoted",       "CREATE TABLE t4 (id INT PRIMARY KEY, \"case_state\" SMALLINT NOT NULL)"),
        ("smallserial",  "CREATE TABLE t5 (id INT PRIMARY KEY, case_state SMALLSERIAL)"),
        ("added",        "CREATE TABLE t6 (id INT PRIMARY KEY)"),
    ];
    for (name, ddl) in forms {
        pool.execute(sqlx::raw_sql(ddl)).await.unwrap_or_else(|e| panic!("{}: {}", name, e));
    }
    pool.execute(sqlx::raw_sql("ALTER TABLE t6 ADD COLUMN case_state SMALLINT NOT NULL DEFAULT 0")).await.unwrap();

    for t in ["t1", "t2", "t3", "t4", "t5", "t6"] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO {} (id, case_state) VALUES (1, 3)", t)))).await.unwrap_or_else(|e| panic!("{}: {}", t, e));
        let row = sqlx::query(AssertSqlSafe(format!("SELECT case_state FROM {} WHERE id = $1", t)))
            .bind(1i32).fetch_one(&pool).await.unwrap_or_else(|e| panic!("{}: {}", t, e));
        println!("{} -> {}", t, row.column(0).type_info().name());
    }
    // What an INTEGER column looks like, for contrast.
    pool.execute(sqlx::raw_sql("CREATE TABLE t7 (id INT PRIMARY KEY, case_state INTEGER)")).await.unwrap();
    pool.execute(sqlx::raw_sql("INSERT INTO t7 (id, case_state) VALUES (1, 3)")).await.unwrap();
    let row = sqlx::query("SELECT case_state FROM t7 WHERE id = $1").bind(1i32).fetch_one(&pool).await.unwrap();
    println!("t7 (INTEGER) -> {}", row.column(0).type_info().name());
}
