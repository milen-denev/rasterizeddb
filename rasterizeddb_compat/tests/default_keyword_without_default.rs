//! `DEFAULT` written for a column that has no default is NULL, in an INSERT
//! and in an UPDATE, and a NOT NULL column refuses it (23502), for every
//! type. Each expectation is PostgreSQL 17's (`docker run --rm postgres:17`).
//!
//! `INSERT … VALUES (DEFAULT, …)` into a `TEXT NOT NULL PRIMARY KEY` with no
//! default stored `''` and reported success; a nullable column given
//! `DEFAULT` read back as `''`, `0` or the epoch rather than NULL; and
//! `UPDATE … SET c = DEFAULT` stored the word `DEFAULT` in a text column.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
    c.simple_query(sql).await.map(|_| ())
}

async fn rows(c: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect()),
            _ => None,
        })
        .collect()
}

fn code(e: &tokio_postgres::Error) -> Option<&str> {
    e.code().map(|c| c.code())
}

/// One column of each kind of storage: variable-width, fixed-width, and the
/// types with literal syntaxes of their own.
const TYPES: &[&str] = &[
    "TEXT", "VARCHAR(10)", "INT", "BIGINT", "SMALLINT", "NUMERIC(10,2)", "DOUBLE PRECISION", "BOOLEAN",
    "TIMESTAMPTZ", "TIMESTAMP", "DATE", "UUID", "BYTEA", "JSONB", "INTERVAL", "TEXT[]",
];

#[tokio::test(flavor = "multi_thread")]
async fn default_into_a_not_null_column_without_one_is_refused() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    // The reported case: a text primary key.
    run(&c, "CREATE TABLE dk (k TEXT NOT NULL PRIMARY KEY, n INT)").await.expect("create");
    let e = run(&c, "INSERT INTO dk VALUES (DEFAULT, 1)").await.expect_err("PostgreSQL: 23502");
    assert_eq!(code(&e), Some("23502"), "{e}");
    let e = run(&c, "INSERT INTO dk (k, n) VALUES ('a', 1), (DEFAULT, 2)").await.expect_err("in a multi-row VALUES");
    assert_eq!(code(&e), Some("23502"), "{e}");
    assert!(rows(&c, "SELECT k FROM dk").await.is_empty(), "nothing stored");

    for (i, ty) in TYPES.iter().enumerate() {
        let t = format!("nn{i}");
        run(&c, &format!("CREATE TABLE {t} (id INT NOT NULL, v {ty} NOT NULL)")).await.expect(ty);
        let e = run(&c, &format!("INSERT INTO {t} VALUES (1, DEFAULT)")).await.expect_err(ty);
        assert_eq!(code(&e), Some("23502"), "{ty}: {e}");
        assert!(rows(&c, &format!("SELECT id FROM {t}")).await.is_empty(), "{ty}: nothing stored");
    }

    // A default that exists is used, and is not refused.
    run(&c, "CREATE TABLE ok (k TEXT NOT NULL DEFAULT 'x', n INT NOT NULL DEFAULT 7)").await.expect("create");
    run(&c, "INSERT INTO ok VALUES (DEFAULT, DEFAULT)").await.expect("defaults");
    assert_eq!(rows(&c, "SELECT k, n FROM ok").await, [[Some("x".into()), Some("7".into())]]);

    // A constant that evaluates to NULL is NULL too (it was stored as 0).
    run(&c, "CREATE TABLE ex (id INT NOT NULL, n INT NOT NULL)").await.expect("create");
    let e = run(&c, "INSERT INTO ex VALUES (1, NULLIF(1, 1))").await.expect_err("PostgreSQL: 23502");
    assert_eq!(code(&e), Some("23502"), "{e}");
}

/// A generated column given `DEFAULT` is its expression's value, over the
/// inputs as written or defaulted. PostgreSQL 17: `(1, 3, 6)` and `(2, 7, 14)`.
#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_given_default_is_computed() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE g (id INT NOT NULL, n INT NOT NULL DEFAULT 7, g INT GENERATED ALWAYS AS (n * 2) STORED)")
        .await
        .expect("create");
    run(&c, "INSERT INTO g (id, n, g) VALUES (1, 3, DEFAULT)").await.expect("explicit input");
    run(&c, "INSERT INTO g (id, n, g) VALUES (2, DEFAULT, DEFAULT)").await.expect("defaulted input");
    assert_eq!(
        rows(&c, "SELECT id, n, g FROM g ORDER BY id").await,
        [["1", "3", "6"], ["2", "7", "14"]].map(|r| r.map(|v| Some(v.to_string())).to_vec())
    );
}

/// A constant that evaluates to NULL is NULL, as the keyword is.
#[tokio::test(flavor = "multi_thread")]
async fn an_expression_that_is_null_stores_null() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE dn (k INT NOT NULL, a TEXT, b INT)").await.expect("create");
    run(&c, "INSERT INTO dn VALUES (1, NULLIF('a', 'a'), NULLIF(1, 1))").await.expect("insert");
    run(&c, "INSERT INTO dn VALUES (2, 'x', 5)").await.expect("insert");
    run(&c, "UPDATE dn SET a = NULLIF('a', 'a'), b = NULLIF(1, 1) WHERE k = 2").await.expect("update");
    assert_eq!(
        rows(&c, "SELECT k, a IS NULL, b IS NULL FROM dn ORDER BY k").await,
        [["1", "t", "t"], ["2", "t", "t"]].map(|r| r.map(|v| Some(v.to_string())).to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn default_into_a_nullable_column_without_one_is_null() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (i, ty) in TYPES.iter().enumerate() {
        let t = format!("dn{i}");
        run(&c, &format!("CREATE TABLE {t} (id INT NOT NULL, v {ty})")).await.expect(ty);
        run(&c, &format!("INSERT INTO {t} VALUES (1, DEFAULT), (2, DEFAULT)")).await.expect(ty);
        run(&c, &format!("INSERT INTO {t} (id, v) VALUES (3, DEFAULT)")).await.expect(ty);
        // PostgreSQL 17: NULL, every row.
        assert_eq!(
            rows(&c, &format!("SELECT id, v IS NULL FROM {t} ORDER BY id")).await,
            (1..=3).map(|k| vec![Some(k.to_string()), Some("t".to_string())]).collect::<Vec<_>>(),
            "{ty}"
        );
        assert_eq!(rows(&c, &format!("SELECT v FROM {t} WHERE id = 1")).await, [[None]], "{ty}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn update_set_default_is_the_default_or_null() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE u (id INT NOT NULL, a TEXT, b INT, s TEXT NOT NULL DEFAULT 'pending', n INT NOT NULL, \
         m INT NOT NULL DEFAULT 5)",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO u VALUES (1, 'x', 9, 'done', 3, 6), (2, 'y', 8, 'done', 4, 7)").await.expect("insert");
    run(&c, "UPDATE u SET a = DEFAULT, b = DEFAULT, s = DEFAULT, m = DEFAULT WHERE id = 1").await.expect("update");
    // PostgreSQL 17: NULL | NULL | pending | 5.
    assert_eq!(
        rows(&c, "SELECT a, b, s, m FROM u WHERE id = 1").await,
        [[None, None, Some("pending".into()), Some("5".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT a, b, s, m FROM u WHERE id = 2").await,
        [[Some("y".into()), Some("8".into()), Some("done".into()), Some("7".into())]],
        "the other row is untouched"
    );
    let e = run(&c, "UPDATE u SET n = DEFAULT WHERE id = 2").await.expect_err("PostgreSQL: 23502");
    assert_eq!(code(&e), Some("23502"), "{e}");
    assert_eq!(rows(&c, "SELECT n FROM u WHERE id = 2").await, [[Some("4".into())]]);

    // RETURNING reports what was stored.
    assert_eq!(
        rows(&c, "UPDATE u SET a = DEFAULT, s = DEFAULT WHERE id = 2 RETURNING a, s").await,
        [[None, Some("pending".into())]]
    );
}
