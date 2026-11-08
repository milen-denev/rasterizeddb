//! `ALTER TABLE … ALTER COLUMN c TYPE t USING expr`: every row rewritten with
//! `c` set to `expr` over the row as it was. It was refused outright ("USING
//! is not supported yet; rewrite via an added column and an UPDATE"), which
//! left the migrations that change what a column means — text to a number
//! or a date, a flag to a boolean — to be done by hand. Every expected answer
//! here is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn run(c: &tokio_postgres::Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn each_column_takes_its_expression() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    run(
        &c,
        "CREATE TABLE au (id INT PRIMARY KEY, s TEXT, n INT, d TEXT, b TEXT, u TEXT, c NUMERIC(6,2) CHECK (c >= 0))",
    )
    .await;
    run(
        &c,
        "INSERT INTO au VALUES \
         (1, '12', 5, '02/01/2024', 'yes', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1.25), \
         (2, '7', NULL, '31/12/2023', 'f', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 2.50)",
    )
    .await;
    for sql in [
        "ALTER TABLE au ALTER COLUMN n TYPE TEXT USING n::text || '!'",
        "ALTER TABLE au ALTER COLUMN s TYPE INT USING s::int",
        "ALTER TABLE au ALTER COLUMN d TYPE DATE USING to_date(d, 'DD/MM/YYYY')",
        // The primary key, to another type, from its own value.
        "ALTER TABLE au ALTER COLUMN id TYPE BIGINT USING id * 10",
        "ALTER TABLE au ALTER COLUMN b TYPE BOOLEAN USING b::boolean",
        "ALTER TABLE au ALTER COLUMN u TYPE UUID USING u::uuid",
        // To a scale the value is rounded to, as an assignment rounds it.
        "ALTER TABLE au ALTER COLUMN c TYPE NUMERIC(6,1) USING c * 2",
    ] {
        run(&c, sql).await;
    }
    assert_eq!(
        rows(&c, "SELECT * FROM au ORDER BY id").await,
        [
            "10|12|5!|2024-01-02|t|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|2.5",
            "20|7|NULL|2023-12-31|f|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12|5.0",
        ]
    );
    // Typed as the new column: compared, filtered and written as one.
    assert_eq!(rows(&c, "SELECT id FROM au WHERE d > '2024-01-01' AND b AND s + 1 = 13").await, ["10"]);
    run(&c, "INSERT INTO au VALUES (30, 1, 'ab', '2024-05-05', true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 3)").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM au WHERE d >= '2023-12-31'").await, ["3"]);

    // The same type, and an expression over other columns and NULLs.
    run(&c, "ALTER TABLE au ALTER COLUMN s TYPE INT USING s * 2 + id").await;
    run(&c, "ALTER TABLE au ALTER COLUMN n TYPE TEXT USING coalesce(n, 'none')").await;
    assert_eq!(rows(&c, "SELECT id, s, n FROM au ORDER BY id").await, ["10|34|5!", "20|34|none", "30|32|ab"]);
    run(&c, "ALTER TABLE au ALTER COLUMN s TYPE INT USING NULL").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM au WHERE s IS NULL").await, ["3"]);
}

/// Refused as PostgreSQL refuses each, leaving the table as it was.
#[tokio::test(flavor = "multi_thread")]
async fn refusals_carry_postgresqls_codes_and_change_nothing() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    for sql in [
        "CREATE TABLE r (id INT PRIMARY KEY, v TEXT NOT NULL, w TEXT, c INT CHECK (c > 0))",
        "INSERT INTO r VALUES (1, 'x', 'abc', 5), (2, '', 'de', 6)",
        "CREATE TABLE pa (id INT PRIMARY KEY)",
        "CREATE TABLE ch (id INT PRIMARY KEY, pid INT REFERENCES pa(id))",
        "CREATE TABLE gc (id INT PRIMARY KEY, a INT, g INT GENERATED ALWAYS AS (a * 2) STORED)",
    ] {
        run(&c, sql).await;
    }
    for (sql, sqlstate) in [
        ("ALTER TABLE r ALTER COLUMN v TYPE INT USING v::int", "22P02"),
        // The first row's NULL, before the second's '' is read.
        ("ALTER TABLE r ALTER COLUMN v TYPE INT USING NULLIF(v, 'x')::int", "23502"),
        ("ALTER TABLE r ALTER COLUMN v TYPE TEXT USING NULL", "23502"),
        ("ALTER TABLE r ALTER COLUMN w TYPE VARCHAR(2) USING w", "22001"),
        ("ALTER TABLE r ALTER COLUMN c TYPE INT USING c - 5", "23514"),
        ("ALTER TABLE r ALTER COLUMN id TYPE INT USING 1", "23505"),
        ("ALTER TABLE r ALTER COLUMN c TYPE INT USING (SELECT 1)", "0A000"),
        ("ALTER TABLE r ALTER COLUMN c TYPE INT USING sum(c)", "42803"),
        ("ALTER TABLE r ALTER COLUMN c TYPE INT USING row_number() OVER ()", "42P20"),
        ("ALTER TABLE gc ALTER COLUMN a TYPE BIGINT USING a + 1", "0A000"),
        // PostgreSQL takes this; the rewrite does not yet re-check references.
        ("ALTER TABLE ch ALTER COLUMN pid TYPE BIGINT USING pid", "0A000"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT * FROM r ORDER BY id").await, ["1|x|abc|5", "2||de|6"]);
    // A string that reads like the words refused is a value, not a subquery.
    assert_eq!(code(&c, "ALTER TABLE r ALTER COLUMN w TYPE INT USING 'select'").await, "22P02");
}

/// An index over the column is rebuilt from the new values, so a lookup
/// through it finds them; one over another column still finds its rows.
#[tokio::test(flavor = "multi_thread")]
async fn indexes_are_rebuilt_from_the_new_values() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    run(&c, "CREATE TABLE ix (id INT PRIMARY KEY, v TEXT, k INT)").await;
    run(&c, "CREATE INDEX ix_v ON ix (v)").await;
    run(&c, "CREATE INDEX ix_k ON ix (k)").await;
    run(&c, "INSERT INTO ix SELECT g, (g % 7)::text, g % 3 FROM generate_series(1, 200) g").await;
    run(&c, "ALTER TABLE ix ALTER COLUMN v TYPE INT USING v::int * 100").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM ix WHERE v = 300").await, ["29"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM ix WHERE v > 450").await, ["56"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM ix WHERE k = 1").await, ["67"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM ix WHERE v = 3").await, ["0"]);
}

/// A partitioned table: a column other than the key rewritten in every
/// partition; a key column refused, since its values decide where each row
/// is stored.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_rewrites_every_partition() {
    struct Root(std::path::PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Root(std::env::temp_dir().join(format!("alter_using_pu_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    std::fs::create_dir_all(&root.0).expect("root");
    let server = TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server");
    let c = connect(&server).await;
    run(
        &c,
        "CREATE TABLE pu (k BIGINT NOT NULL, id INT NOT NULL, v TEXT, PRIMARY KEY (k, id)) \
         PARTITION BY HASH (k) PARTITIONS 8",
    )
    .await;
    run(&c, "INSERT INTO pu SELECT g % 5, g, (g * 3)::text FROM generate_series(1, 60) g").await;
    run(&c, "ALTER TABLE pu ALTER COLUMN v TYPE INT USING v::int + k").await;
    assert_eq!(rows(&c, "SELECT count(*), sum(v) FROM pu").await, ["60|5610"]);
    assert_eq!(rows(&c, "SELECT v FROM pu WHERE k = 2 AND id = 7").await, ["23"]);
    assert_eq!(code(&c, "ALTER TABLE pu ALTER COLUMN k TYPE BIGINT USING k + 1").await, "0A000");
    run(&c, "ALTER TABLE pu ALTER COLUMN id TYPE BIGINT USING id * 2").await;
    assert_eq!(rows(&c, "SELECT k, id, v FROM pu WHERE k = 2 AND id = 14").await, ["2|14|23"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM pu WHERE k = 2").await, ["12"]);
}
