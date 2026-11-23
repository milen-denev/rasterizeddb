//! A schema-qualified `CREATE TABLE` creates the table every other statement
//! reaches by that name.
//!
//! The catalogue has one namespace, and every reader resolves `public.t` or
//! `"public"."t"` by its last segment. `CREATE TABLE` alone stored the name as
//! written, so `CREATE TABLE public.pq` made a table called `public.pq` and
//! `CREATE TABLE "public"."q1"` one called `public"."q1`, quotes included.
//! Both reported success, and nothing could read, write or even drop the
//! table afterwards. Prisma, Flyway and EF all create tables that way.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn names(pool: &sqlx::PgPool) -> Vec<String> {
    let mut out: Vec<String> = sqlx::query("SELECT table_name FROM information_schema.tables")
        .fetch_all(pool)
        .await
        .expect("information_schema.tables")
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qualified_create_makes_the_table_its_name_reaches() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");

    for (create, bare) in [
        ("CREATE TABLE public.qa (id INT PRIMARY KEY, v INT)", "qa"),
        ("CREATE TABLE \"public\".\"qb\" (id INT PRIMARY KEY, v INT)", "qb"),
        ("CREATE TABLE IF NOT EXISTS public.qc (id INT PRIMARY KEY, v INT)", "qc"),
        ("CREATE TABLE IF NOT EXISTS \"public\".\"Qd\" (id INT PRIMARY KEY, v INT)", "Qd"),
    ] {
        pool.execute(AssertSqlSafe(create)).await.unwrap_or_else(|e| panic!("{create}: {e}"));
        let quoted = format!("\"{bare}\"");
        for (insert, id) in [
            (format!("INSERT INTO {quoted} VALUES (1, 10)"), 1),
            (format!("INSERT INTO public.{quoted} VALUES (2, 20)"), 2),
        ] {
            pool.execute(AssertSqlSafe(insert.clone())).await.unwrap_or_else(|e| panic!("{insert}: {e}"));
            let read = format!("SELECT v FROM \"public\".{quoted} WHERE id = {id}");
            let v: i32 = sqlx::query(AssertSqlSafe(read.clone()))
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|e| panic!("{read}: {e}"))
                .get(0);
            assert_eq!(v, id * 10, "{create}: {read}");
        }
    }
    assert_eq!(names(&pool).await, vec!["Qd", "qa", "qb", "qc"]);

    // A second create of the same table under the other spelling is the same
    // table: refused, or skipped with IF NOT EXISTS.
    let again = pool.execute("CREATE TABLE qa (id INT)").await.expect_err("qa exists");
    assert_eq!(again.as_database_error().and_then(|d| d.code()).as_deref(), Some("42P07"), "{again}");
    pool.execute("CREATE TABLE IF NOT EXISTS \"public\".qa (id INT)").await.expect("skipped");

    // CREATE TABLE … AS goes through the same name.
    pool.execute("CREATE TABLE public.qe AS SELECT id, v FROM qa").await.expect("ctas");
    let n: i64 = sqlx::query("SELECT count(*) FROM qe").fetch_one(&pool).await.expect("read ctas").get(0);
    assert_eq!(n, 2);

    // And every one of them can be dropped by its qualified name.
    for drop in [
        "DROP TABLE public.qa",
        "DROP TABLE \"public\".\"qb\"",
        "DROP TABLE public.qc",
        "DROP TABLE \"public\".\"Qd\"",
        "DROP TABLE IF EXISTS public.qe",
    ] {
        pool.execute(drop).await.unwrap_or_else(|e| panic!("{drop}: {e}"));
    }
    assert!(names(&pool).await.is_empty(), "{:?}", names(&pool).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_in_a_declared_schema_is_reached_by_its_qualified_name() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE SCHEMA app").await.expect("schema");
    pool.execute("CREATE TABLE app.t (a INT)").await.expect("create");
    pool.execute("INSERT INTO app.t VALUES (5)").await.expect("insert");
    let a: i32 = sqlx::query("SELECT a FROM app.t").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(a, 5);
    pool.execute("DROP TABLE app.t").await.expect("drop");
    assert!(names(&pool).await.is_empty());
}
