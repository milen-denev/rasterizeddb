//! A `timestamp` (without time zone) column is described as `timestamp` (OID
//! 1114) and shown with no offset; a `timestamptz` one keeps `timestamptz`
//! (1184) and its `+00`. Each expectation is PostgreSQL 17's
//! (`docker run --rm postgres:17`, session `TimeZone` UTC).
//!
//! Both were described as `timestamptz` and shown as `2021-01-01 00:00:00+00`.
//! The description and the value come from one type (`reported_type`), so
//! they are checked together, through real drivers: psql cannot see an OID,
//! and a typed driver refuses a `NaiveDateTime` from a column that says
//! `timestamptz` (and a `DateTime<Utc>` from one that says `timestamp`).

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rasterizeddb_compat::TestServer;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

/// Every row's cells in the text format, NULL as `None`.
async fn text(c: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
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

fn naive(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, ms: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, mo, d).unwrap().and_hms_milli_opt(h, mi, s, ms).unwrap()
}

async fn table(c: &Client) {
    c.batch_execute(
        "CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ, n TEXT);
         INSERT INTO ts VALUES (1, '2021-01-01 00:00:00', '2021-01-01 00:00:00', 'x'),
                               (2, '2024-06-01 12:34:56.789', '2024-06-01 12:34:56.789+02', 'y')",
    )
    .await
    .expect("create");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_text_form_has_an_offset_only_for_timestamptz() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    table(&c).await;
    let s = |v: &str| Some(v.to_string());
    // PostgreSQL 17, statement by statement.
    for (sql, want) in [
        (
            "SELECT a, b FROM ts ORDER BY k",
            vec![
                vec![s("2021-01-01 00:00:00"), s("2021-01-01 00:00:00+00")],
                vec![s("2024-06-01 12:34:56.789"), s("2024-06-01 10:34:56.789+00")],
            ],
        ),
        (
            "SELECT min(a), max(a), min(b) FROM ts",
            vec![vec![s("2021-01-01 00:00:00"), s("2024-06-01 12:34:56.789"), s("2021-01-01 00:00:00+00")]],
        ),
        ("SELECT n, max(a) FROM ts GROUP BY n ORDER BY n", vec![vec![s("x"), s("2021-01-01 00:00:00")], vec![s("y"), s("2024-06-01 12:34:56.789")]]),
        ("SELECT t1.a FROM ts t1 JOIN ts t2 ON t1.k = t2.k WHERE t1.k = 1", vec![vec![s("2021-01-01 00:00:00")]]),
        ("WITH x AS (SELECT k, a FROM ts) SELECT a FROM x WHERE k = 2", vec![vec![s("2024-06-01 12:34:56.789")]]),
        ("SELECT a FROM (SELECT k, a FROM ts) s WHERE k = 1", vec![vec![s("2021-01-01 00:00:00")]]),
        ("SELECT a, row_number() OVER (ORDER BY a) FROM ts WHERE k = 1", vec![vec![s("2021-01-01 00:00:00"), s("1")]]),
        (
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ts' AND column_name IN ('a', 'b') ORDER BY ordinal_position",
            vec![vec![s("a"), s("timestamp without time zone")], vec![s("b"), s("timestamp with time zone")]],
        ),
    ] {
        assert_eq!(text(&c, sql).await, want, "{sql}");
    }
    assert_eq!(
        text(&c, "INSERT INTO ts (k, a, b) VALUES (3, '2030-01-01 00:00:00', '2030-01-01 00:00:00') RETURNING a, b").await,
        vec![vec![s("2030-01-01 00:00:00"), s("2030-01-01 00:00:00+00")]]
    );
    assert_eq!(text(&c, "UPDATE ts SET a = '2031-01-01 00:00:00' WHERE k = 3 RETURNING a").await, vec![vec![s("2031-01-01 00:00:00")]]);
}

/// The extended protocol, binary results: what the description says is what
/// the driver decodes, and both are right.
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_driver_reads_timestamp_as_naive_and_timestamptz_as_an_instant() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    table(&c).await;

    let st = c.prepare("SELECT a, b FROM ts ORDER BY k").await.expect("prepare");
    let types: Vec<Type> = st.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(types, [Type::TIMESTAMP, Type::TIMESTAMPTZ]);
    let rows = c.query(&st, &[]).await.expect("query");
    assert_eq!(rows[1].get::<_, NaiveDateTime>(0), naive(2024, 6, 1, 12, 34, 56, 789));
    assert_eq!(rows[1].get::<_, DateTime<Utc>>(1), naive(2024, 6, 1, 10, 34, 56, 789).and_utc());

    for sql in ["SELECT max(a) FROM ts", "WITH x AS (SELECT a FROM ts) SELECT max(a) FROM x"] {
        let st = c.prepare(sql).await.expect("prepare");
        assert_eq!(st.columns()[0].type_(), &Type::TIMESTAMP, "{sql}");
        let got: NaiveDateTime = c.query_one(&st, &[]).await.expect("query").get(0);
        assert_eq!(got, naive(2024, 6, 1, 12, 34, 56, 789), "{sql}");
    }

    // A parameter compared with or written into the column is a `timestamp`,
    // so a `NaiveDateTime` binds to it.
    let lookup = c.prepare("SELECT k FROM ts WHERE a = $1").await.expect("prepare");
    assert_eq!(lookup.params(), [Type::TIMESTAMP]);
    let k: i32 = c.query_one(&lookup, &[&naive(2021, 1, 1, 0, 0, 0, 0)]).await.expect("lookup").get(0);
    assert_eq!(k, 1);
    let insert = c.prepare("INSERT INTO ts (k, a) VALUES ($1, $2) RETURNING a").await.expect("prepare");
    assert_eq!(insert.params()[1], Type::TIMESTAMP);
    let back: NaiveDateTime =
        c.query_one(&insert, &[&5i32, &naive(1999, 12, 31, 23, 59, 59, 0)]).await.expect("insert").get(0);
    assert_eq!(back, naive(1999, 12, 31, 23, 59, 59, 0));
    assert_eq!(
        text(&c, "SELECT a FROM ts WHERE k = 5").await,
        vec![vec![Some("1999-12-31 23:59:59".to_string())]]
    );
}

/// sqlx checks a column's advertised type against the Rust type asked for:
/// `NaiveDateTime` is `TIMESTAMP` and `DateTime<Utc>` is `TIMESTAMPTZ`.
#[tokio::test(flavor = "multi_thread")]
async fn sqlx_decodes_each_column_as_its_own_type() {
    use sqlx::Row;
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    sqlx::raw_sql(
        "CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ);
         INSERT INTO ts VALUES (1, '2024-06-01 12:34:56', '2024-06-01 12:34:56+02')",
    )
    .execute(&pool)
    .await
    .expect("create");
    let row = sqlx::query("SELECT a, b FROM ts").fetch_one(&pool).await.expect("select");
    let a: NaiveDateTime = row.try_get(0).expect("a timestamp column decodes as NaiveDateTime");
    let b: DateTime<Utc> = row.try_get(1).expect("a timestamptz column decodes as DateTime<Utc>");
    assert_eq!(a, naive(2024, 6, 1, 12, 34, 56, 0));
    assert_eq!(b, naive(2024, 6, 1, 10, 34, 56, 0).and_utc());
    // And not the other way round, as PostgreSQL's types say.
    assert!(row.try_get::<DateTime<Utc>, _>(0).is_err());
    assert!(row.try_get::<NaiveDateTime, _>(1).is_err());

    sqlx::query("INSERT INTO ts (k, a, b) VALUES (2, $1, $2)")
        .bind(naive(2001, 2, 3, 4, 5, 6, 0))
        .bind(naive(2001, 2, 3, 4, 5, 6, 0).and_utc())
        .execute(&pool)
        .await
        .expect("bind");
    let row = sqlx::query("SELECT a, b FROM ts WHERE k = 2").fetch_one(&pool).await.expect("select");
    assert_eq!(row.get::<NaiveDateTime, _>(0), naive(2001, 2, 3, 4, 5, 6, 0));
    assert_eq!(row.get::<DateTime<Utc>, _>(1), naive(2001, 2, 3, 4, 5, 6, 0).and_utc());
}
