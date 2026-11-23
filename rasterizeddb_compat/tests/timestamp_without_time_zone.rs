//! `timestamp` (without time zone) against `timestamptz`, as PostgreSQL 17
//! answers each statement here (`docker run --rm postgres:17`, session
//! `TimeZone` UTC).
//!
//! The two were one type, so a zone written in a literal was applied to a
//! column that has none: `'2024-01-01 10:00:00+02'` into a `TIMESTAMP` column
//! stored 08:00, reported success, and a later `WHERE ts = '… 10:00:00'` found
//! nothing. PostgreSQL's `timestamp_in` discards the zone and stores 10:00; a
//! `timestamptz` keeps converting by it.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) -> Result<u64, tokio_postgres::Error> {
    let mut count = 0;
    for m in c.simple_query(sql).await? {
        if let SimpleQueryMessage::CommandComplete(n) = m {
            count = n;
        }
    }
    Ok(count)
}

/// Every row's cells as text, NULL as `None`.
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

/// The first column of every row.
async fn column(c: &Client, sql: &str) -> Vec<String> {
    rows(c, sql).await.into_iter().map(|r| r[0].clone().unwrap_or_else(|| "NULL".into())).collect()
}

/// A `timestamp` cell's wall-clock time, whatever suffix it is shown with:
/// what these tests check is the value stored, not how it is rendered.
fn wall(cell: &str) -> &str {
    cell.strip_suffix("+00").unwrap_or(cell)
}

fn walls(cells: Vec<String>) -> Vec<String> {
    cells.iter().map(|c| wall(c).to_string()).collect()
}

const TABLE: &str = "CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ, \
     c TIMESTAMP WITHOUT TIME ZONE, d TIMESTAMP WITH TIME ZONE, e TIMESTAMP(3))";

#[tokio::test(flavor = "multi_thread")]
async fn a_literals_zone_is_discarded_by_timestamp_and_applied_by_timestamptz() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, TABLE).await.expect("create");
    run(
        &c,
        "INSERT INTO ts VALUES (1, '2024-01-01 10:00:00+02', '2024-01-01 10:00:00+02', \
         '2024-01-01 10:00:00-05:30', '2024-01-01 10:00:00-05:30', '2024-01-01T10:00:00Z')",
    )
    .await
    .expect("insert");
    // A multi-row VALUES, which is executed row by row.
    run(&c, "INSERT INTO ts (k, a, b) VALUES (2, '2021-01-01 00:00:00+0530', '2021-01-01 00:00:00+0530'), (3, '2024-03-03 03:03:03+03', '2024-03-03 03:03:03+03')")
        .await
        .expect("multi-row insert");
    let got = rows(&c, "SELECT k, a, b, c, d, e FROM ts ORDER BY k").await;
    let cell = |r: usize, i: usize| got[r][i].clone().unwrap_or_default();
    // PostgreSQL 17: 10:00 | 08:00+00 | 10:00 | 15:30+00 | 10:00.
    assert_eq!(wall(&cell(0, 1)), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(cell(0, 2), "2024-01-01 08:00:00+00", "{got:?}");
    assert_eq!(wall(&cell(0, 3)), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(cell(0, 4), "2024-01-01 15:30:00+00", "{got:?}");
    assert_eq!(wall(&cell(0, 5)), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(wall(&cell(1, 1)), "2021-01-01 00:00:00", "{got:?}");
    assert_eq!(cell(1, 2), "2020-12-31 18:30:00+00", "{got:?}");
    assert_eq!(wall(&cell(2, 1)), "2024-03-03 03:03:03", "{got:?}");
    assert_eq!(cell(2, 2), "2024-03-03 00:03:03+00", "{got:?}");

    run(&c, "UPDATE ts SET a = '2024-06-01 12:00:00+03', b = '2024-06-01 12:00:00+03' WHERE k = 2")
        .await
        .expect("update");
    let got = rows(&c, "SELECT a, b FROM ts WHERE k = 2").await;
    assert_eq!(wall(got[0][0].as_deref().unwrap()), "2024-06-01 12:00:00", "{got:?}");
    assert_eq!(got[0][1].as_deref(), Some("2024-06-01 09:00:00+00"), "{got:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn defaults_and_upserts_keep_the_wall_clock_time() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE tsd (k INT, a TIMESTAMP DEFAULT '2024-01-01 10:00:00+02', \
         b TIMESTAMPTZ DEFAULT '2024-01-01 10:00:00+02')",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO tsd (k) VALUES (1)").await.expect("omitted");
    run(&c, "INSERT INTO tsd VALUES (2, DEFAULT, DEFAULT)").await.expect("DEFAULT");
    let got = rows(&c, "SELECT a, b FROM tsd ORDER BY k").await;
    for row in &got {
        assert_eq!(wall(row[0].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
        assert_eq!(row[1].as_deref(), Some("2024-01-01 08:00:00+00"), "{got:?}");
    }

    run(&c, "CREATE TABLE tsu (k INT PRIMARY KEY, a TIMESTAMP)").await.expect("create");
    let upsert = "INSERT INTO tsu VALUES (1, '2024-01-01 10:00:00+02') \
                  ON CONFLICT (k) DO UPDATE SET a = '2025-01-01 10:00:00+02'";
    run(&c, upsert).await.expect("insert");
    run(&c, upsert).await.expect("update");
    let many = |year: u32| {
        format!(
            "INSERT INTO tsu VALUES (2, '{year}-01-01 10:00:00+02'), (3, '{year}-01-01 11:00:00-02') \
             ON CONFLICT (k) DO UPDATE SET a = EXCLUDED.a"
        )
    };
    run(&c, &many(2024)).await.expect("insert");
    run(&c, &many(2026)).await.expect("update");
    assert_eq!(
        walls(column(&c, "SELECT a FROM tsu ORDER BY k").await),
        ["2025-01-01 10:00:00", "2026-01-01 10:00:00", "2026-01-01 11:00:00"]
    );

    // A key compares the wall-clock times: these are one value of a
    // `timestamp` key, and PostgreSQL refuses the second.
    run(&c, "CREATE TABLE tsk (a TIMESTAMP PRIMARY KEY, n INT)").await.expect("create");
    run(&c, "INSERT INTO tsk VALUES ('2024-01-01 10:00:00+02', 1)").await.expect("first");
    let dup = run(&c, "INSERT INTO tsk VALUES ('2024-01-01 10:00:00-07', 2)").await.expect_err("same key");
    assert_eq!(dup.code().map(|c| c.code()), Some("23505"), "{dup}");
}

#[tokio::test(flavor = "multi_thread")]
async fn predicates_read_a_literal_against_a_timestamp_column_without_its_zone() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ)").await.expect("create");
    run(
        &c,
        "INSERT INTO ts VALUES (1, '2024-01-01 10:00:00', '2024-01-01 10:00:00+02'), \
         (2, '2024-06-01 12:00:00', '2024-06-01 12:00:00'), (3, '2024-01-01 08:00:00', '2024-01-01 08:00:00')",
    )
    .await
    .expect("insert");
    // Each against PostgreSQL 17.
    for (sql, want) in [
        ("SELECT k FROM ts WHERE a = '2024-01-01 10:00:00+07'", vec!["1"]),
        ("SELECT k FROM ts WHERE '2024-01-01 10:00:00-09' = a", vec!["1"]),
        ("SELECT k FROM ts WHERE a <> '2024-01-01 10:00:00+07' ORDER BY k", vec!["2", "3"]),
        (
            "SELECT k FROM ts WHERE a BETWEEN '2024-01-01 09:59:00+05' AND '2024-01-01 10:01:00+05' ORDER BY k",
            vec!["1"],
        ),
        ("SELECT k FROM ts WHERE a IN ('2024-01-01 10:00:00+01', '2024-06-01 12:00:00Z') ORDER BY k", vec!["1", "2"]),
        ("SELECT k FROM ts WHERE a > '2024-01-01 09:00:00+09' ORDER BY k", vec!["1", "2"]),
        // A `timestamptz` column still converts: both name 08:00 UTC.
        ("SELECT k FROM ts WHERE b = '2024-01-01 10:00:00+02' ORDER BY k", vec!["1", "3"]),
        ("SELECT CASE WHEN a = '2024-01-01 10:00:00+02' THEN 'hit' ELSE 'miss' END FROM ts WHERE k = 1", vec!["hit"]),
    ] {
        assert_eq!(column(&c, sql).await, want, "{sql}");
    }
    assert_eq!(run(&c, "UPDATE ts SET k = 10 WHERE a = '2024-01-01 10:00:00+05'").await.expect("update"), 1);
    assert_eq!(run(&c, "DELETE FROM ts WHERE a = '2024-06-01 12:00:00-11'").await.expect("delete"), 1);
    assert_eq!(column(&c, "SELECT k FROM ts ORDER BY k").await, ["3", "10"]);
}

/// The flag is a field record's: a table rewrite (ADD COLUMN on a populated
/// table rebuilds it), ALTER COLUMN TYPE in either direction and a restart
/// each keep or change it as asked.
#[tokio::test(flavor = "multi_thread")]
async fn the_column_stays_a_timestamp_across_a_rewrite_an_alter_and_a_restart() {
    let root = format!("/var/tmp/timestamp_without_time_zone_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let c = client(&server).await;
        run(&c, "CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ)").await.expect("create");
        run(&c, "INSERT INTO ts VALUES (1, '2024-01-01 10:00:00+02', '2024-01-01 10:00:00+02')").await.expect("insert");
        // Rebuilds the populated table; the new column's default fills the
        // row already there.
        run(&c, "ALTER TABLE ts ADD COLUMN f TIMESTAMP DEFAULT '2024-01-01 10:00:00+02'").await.expect("add");
        run(&c, "INSERT INTO ts (k, a, b) VALUES (2, '2024-01-01 10:00:00+02', '2024-01-01 10:00:00+02')")
            .await
            .expect("insert after the rewrite");
        let got = rows(&c, "SELECT a, b, f FROM ts ORDER BY k").await;
        for row in &got {
            assert_eq!(wall(row[0].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
            assert_eq!(row[1].as_deref(), Some("2024-01-01 08:00:00+00"), "{got:?}");
            assert_eq!(wall(row[2].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
        }

        // `timestamptz` and back: no value moves (the session is UTC), and
        // each statement after reads its literal as the column now is.
        run(&c, "ALTER TABLE ts ALTER COLUMN a TYPE timestamptz").await.expect("to timestamptz");
        run(&c, "INSERT INTO ts (k, a) VALUES (3, '2024-01-01 10:00:00+02')").await.expect("insert");
        run(&c, "ALTER TABLE ts ALTER COLUMN a TYPE timestamp without time zone").await.expect("to timestamp");
        run(&c, "INSERT INTO ts (k, a) VALUES (4, '2024-01-01 10:00:00+02')").await.expect("insert");
        // Restated as it is: no change.
        run(&c, "ALTER TABLE ts ALTER COLUMN a TYPE timestamp").await.expect("same type");
        assert_eq!(
            walls(column(&c, "SELECT a FROM ts ORDER BY k").await),
            ["2024-01-01 10:00:00", "2024-01-01 10:00:00", "2024-01-01 08:00:00", "2024-01-01 10:00:00"]
        );
    }
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let c = client(&server).await;
    run(&c, "INSERT INTO ts (k, a, b, f) VALUES (5, '2024-01-01 10:00:00-03', '2024-01-01 10:00:00-03', '2024-01-01 10:00:00-03')")
        .await
        .expect("insert after the restart");
    let got = rows(&c, "SELECT a, b, f FROM ts WHERE k = 5").await;
    assert_eq!(wall(got[0][0].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(got[0][1].as_deref(), Some("2024-01-01 13:00:00+00"), "{got:?}");
    assert_eq!(wall(got[0][2].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(column(&c, "SELECT k FROM ts WHERE a = '2024-01-01 10:00:00+09' ORDER BY k").await, ["1", "2", "4", "5"]);
}

/// A prepared statement bound with text: the parse is learned on the second
/// execution and reused from the third (`rql::prepared`), and every one of
/// them reads the bound literal as the column does.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_lookup_reads_each_bound_literal_without_its_zone() {
    use sqlx::Row;
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    sqlx::query("CREATE TABLE ts (k INT NOT NULL, a TIMESTAMP)").execute(&pool).await.expect("create");
    for k in 0..6 {
        sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO ts VALUES ({k}, '2024-01-0{} 10:00:00')", k + 1)))
            .execute(&pool)
            .await
            .expect("insert");
    }
    for k in 0..6i32 {
        // A different zone each time; each names the row's wall-clock time.
        let literal = format!("2024-01-0{} 10:00:00+0{}", k + 1, k + 1);
        let found: Vec<i32> = sqlx::query("SELECT k FROM ts WHERE a = $1")
            .bind(&literal)
            .fetch_all(&pool)
            .await
            .expect("select")
            .iter()
            .map(|r| r.get::<i32, _>(0))
            .collect();
        assert_eq!(found, vec![k], "{literal}");
        sqlx::query("INSERT INTO ts (k, a) VALUES ($1, $2)")
            .bind(100 + k)
            .bind(&literal)
            .execute(&pool)
            .await
            .expect("insert");
    }
    let n: i64 = sqlx::query("SELECT count(*) FROM ts WHERE k >= 100 AND a = '2024-01-03 10:00:00'")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 1);
}

/// A cast or a type name on a literal says which type reads it, and every
/// reader agrees: the write, a predicate (with and without an index), a
/// stored DEFAULT and a CHECK. `::timestamptz` and `TIMESTAMPTZ '…'` convert
/// by the zone; `::timestamp` and `TIMESTAMP '…'` drop it. A row written with
/// one spelling is found by the same spelling, as PostgreSQL 17 finds it.
#[tokio::test(flavor = "multi_thread")]
async fn a_cast_or_type_name_on_a_literal_decides_how_its_zone_is_read() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tc (k INT NOT NULL, a TIMESTAMP, b TIMESTAMPTZ)").await.expect("create");
    run(
        &c,
        "INSERT INTO tc VALUES \
         (1, '2024-01-01 10:00:00+02'::timestamptz, '2024-01-01 10:00:00+02'::timestamp), \
         (2, TIMESTAMPTZ '2024-01-01 10:00:00+02', TIMESTAMP '2024-01-01 10:00:00+02'), \
         (3, CAST('2024-01-01 10:00:00+02' AS timestamptz), CAST('2024-01-01 10:00:00+02' AS timestamp)), \
         (4, '2024-01-01 07:30:00', NULL), (5, '2024-01-01 10:00:00', NULL), (6, NULL, NULL)",
    )
    .await
    .expect("insert");
    let got = rows(&c, "SELECT a, b FROM tc WHERE k <= 3 ORDER BY k").await;
    for row in &got {
        assert_eq!(wall(row[0].as_deref().unwrap()), "2024-01-01 08:00:00", "{got:?}");
        assert_eq!(row[1].as_deref(), Some("2024-01-01 10:00:00+00"), "{got:?}");
    }
    let queries = [
        ("SELECT k FROM tc WHERE a = '2024-01-01 10:00:00+02'::timestamptz ORDER BY k", vec!["1", "2", "3"]),
        ("SELECT k FROM tc WHERE a > '2024-01-01 09:45:00+02'::timestamptz ORDER BY k", vec!["1", "2", "3", "5"]),
        ("SELECT k FROM tc WHERE a = TIMESTAMP '2024-01-01 10:00:00+02' ORDER BY k", vec!["5"]),
        ("SELECT k FROM tc WHERE a = '2024-01-01 10:00:00+02'::timestamp ORDER BY k", vec!["5"]),
        ("SELECT k FROM tc WHERE b = '2024-01-01 10:00:00+02'::timestamp ORDER BY k", vec!["1", "2", "3"]),
    ];
    for (sql, want) in &queries {
        assert_eq!(&column(&c, sql).await, want, "{sql}");
    }
    // And through an index on the column.
    run(&c, "CREATE INDEX tc_a ON tc (a)").await.expect("index");
    for (sql, want) in &queries {
        assert_eq!(&column(&c, sql).await, want, "indexed: {sql}");
    }

    // A DEFAULT and a CHECK keep the type their literal was given.
    run(
        &c,
        "CREATE TABLE td (k INT, a TIMESTAMP DEFAULT '2024-01-01 10:00:00+02'::timestamptz, \
         c TIMESTAMP CHECK (c < '2024-01-01 09:00:00+05'::timestamptz))",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO td (k) VALUES (1)").await.expect("default");
    assert_eq!(walls(column(&c, "SELECT a FROM td").await), ["2024-01-01 08:00:00"]);
    let e = run(&c, "INSERT INTO td (k, c) VALUES (2, '2024-01-01 05:00:00')").await.expect_err("PostgreSQL: 23514");
    assert_eq!(e.code().map(|c| c.code()), Some("23514"), "{e}");
    run(&c, "INSERT INTO td (k, c) VALUES (3, '2024-01-01 03:00:00')").await.expect("within the check");
}

/// An expression's text assigned by UPDATE is read as the column reads a
/// literal: PostgreSQL 17 stores 10:00 for each.
#[tokio::test(flavor = "multi_thread")]
async fn an_updates_expression_reads_its_text_as_the_column_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tu (k INT NOT NULL, a TIMESTAMP)").await.expect("create");
    run(&c, "INSERT INTO tu VALUES (1, '2020-01-01 00:00:00'), (2, NULL)").await.expect("insert");
    run(&c, "UPDATE tu SET a = CASE WHEN k = 1 THEN '2024-05-05 10:00:00+02' ELSE a END").await.expect("case");
    run(&c, "UPDATE tu SET a = coalesce(a, '2024-06-06 10:00:00+02') WHERE k = 2").await.expect("coalesce");
    assert_eq!(walls(column(&c, "SELECT a FROM tu ORDER BY k").await), ["2024-05-05 10:00:00", "2024-06-06 10:00:00"]);
}

/// Flipping between `timestamp` and `timestamptz` re-reads the column's
/// DEFAULT and CHECK text under the new type, which would change a value a
/// zone was written into; PostgreSQL keeps the value. Refused until it is
/// restated without the zone; a column without one still flips.
#[tokio::test(flavor = "multi_thread")]
async fn a_flip_that_would_change_a_zoned_default_or_check_is_refused() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tf (k INT, a TIMESTAMPTZ DEFAULT '2024-01-01 09:00:00+05')").await.expect("create");
    let e = run(&c, "ALTER TABLE tf ALTER COLUMN a TYPE timestamp").await.expect_err("refused");
    assert_eq!(e.code().map(|c| c.code()), Some("0A000"), "{e}");
    run(&c, "INSERT INTO tf (k) VALUES (1)").await.expect("insert");
    assert_eq!(column(&c, "SELECT a FROM tf").await, ["2024-01-01 04:00:00+00"], "the default kept its value");

    run(&c, "CREATE TABLE tg (k INT, a TIMESTAMP CHECK (a > '2024-01-01 09:00:00+05'))").await.expect("create");
    let e = run(&c, "ALTER TABLE tg ALTER COLUMN a TYPE timestamptz").await.expect_err("refused");
    assert_eq!(e.code().map(|c| c.code()), Some("0A000"), "{e}");

    run(&c, "CREATE TABLE th (k INT, a TIMESTAMPTZ DEFAULT '2024-01-01 09:00:00')").await.expect("create");
    run(&c, "ALTER TABLE th ALTER COLUMN a TYPE timestamp").await.expect("no zone: flips");
}

/// A generated column reading the column blocks the flip, as PostgreSQL
/// refuses to alter the type of a column a generated column uses; and the
/// generated value compares a `timestamptz` as an instant, not as text.
/// PostgreSQL 17: `late` is t, f, then t, t; for the `timestamp` table f, t.
#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_reads_the_timestamp_as_its_type_and_blocks_the_flip() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE fl3 (k INT, a TIMESTAMPTZ, late BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01 09:00:00+05') STORED)",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO fl3 (k, a) VALUES (1, '2024-01-01 06:00:00+00'), (2, '2024-01-01 03:00:00+00')").await.expect("insert");
    assert_eq!(column(&c, "SELECT late FROM fl3 ORDER BY k").await, ["t", "f"]);
    run(&c, "UPDATE fl3 SET a = '2024-01-01 07:00:00+00' WHERE k = 2").await.expect("update");
    assert_eq!(column(&c, "SELECT late FROM fl3 ORDER BY k").await, ["t", "t"]);
    let e = run(&c, "ALTER TABLE fl3 ALTER COLUMN a TYPE timestamp").await.expect_err("refused");
    assert_eq!(e.code().map(|c| c.code()), Some("0A000"), "{e}");

    run(
        &c,
        "CREATE TABLE fl4 (k INT, a TIMESTAMP, late BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01 09:00:00+05') STORED)",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO fl4 (k, a) VALUES (1, '2024-01-01 06:00:00'), (2, '2024-01-01 10:00:00')").await.expect("insert");
    assert_eq!(column(&c, "SELECT late FROM fl4 ORDER BY k").await, ["f", "t"]);
}

/// A timestamp-typed literal written into a text column is shown as
/// PostgreSQL shows that type as text: `+00` for `timestamptz`, no zone for
/// `timestamp`; a parenthesised literal is cast as one. PostgreSQL 17 for
/// each.
#[tokio::test(flavor = "multi_thread")]
async fn a_timestamp_typed_literal_into_text_reads_as_postgresql_shows_it() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tt (k INT, v TEXT, a TIMESTAMP, b TIMESTAMPTZ)").await.expect("create");
    run(
        &c,
        "INSERT INTO tt (k, v) VALUES (1, '2024-01-01 10:00:00+02'::timestamptz), \
         (2, '2024-01-01 10:00:00+02'::timestamp), (3, '2024-01-01 10:00:00'::timestamptz), \
         (4, '2024-01-01 10:00:00+02')",
    )
    .await
    .expect("insert");
    run(&c, "INSERT INTO tt (k, a, b) VALUES (5, ('2024-01-01 10:00:00+02')::timestamp, ('2024-01-01 10:00:00+02')::timestamp)")
        .await
        .expect("parenthesised");
    run(&c, "INSERT INTO tt (k, v) VALUES (6, 'x')").await.expect("insert");
    run(&c, "UPDATE tt SET v = '2024-01-01 10:00:00+02'::timestamptz WHERE k = 6").await.expect("update");
    assert_eq!(
        column(&c, "SELECT v FROM tt WHERE k IN (1, 2, 3, 4, 6) ORDER BY k").await,
        [
            "2024-01-01 08:00:00+00",
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00+00",
            "2024-01-01 10:00:00+02",
            "2024-01-01 08:00:00+00"
        ]
    );
    let got = rows(&c, "SELECT a, b FROM tt WHERE k = 5").await;
    assert_eq!(wall(got[0][0].as_deref().unwrap()), "2024-01-01 10:00:00", "{got:?}");
    assert_eq!(got[0][1].as_deref(), Some("2024-01-01 10:00:00+00"), "{got:?}");
}

// PostgreSQL renders a timestamptz as text one way, whatever spelled the
// value: `CAST`, a parenthesised literal, an explicit `::text`, a column.
// Only the first spelling used to get the `+00`; the rest stored the bare
// wall-clock time, and a lookup by `'…'::timestamptz::text` found nothing.
#[tokio::test]
async fn every_spelling_of_a_timestamptz_into_text_gets_the_same_rendering() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tx (k INT, v TEXT)").await.expect("create");
    run(
        &c,
        "INSERT INTO tx (k, v) VALUES (1, CAST('2024-01-01 10:00:00+02' AS timestamptz)), \
         (2, ('2024-01-01 10:00:00+02')::timestamptz), (3, '2024-01-01 10:00:00+02'::timestamptz::text), \
         (4, '2024-01-01 10:00:00+02'::timestamp with time zone), (5, CAST('2024-01-01 10:00:00+02' AS timestamp)), \
         (6, ('2024-01-01 10:00:00+02')::timestamp), (7, '2024-01-01 10:00:00+02'::timestamp::text), \
         (8, (('2024-01-01 10:00:00+02'))::timestamp)",
    )
    .await
    .expect("insert");
    assert_eq!(
        column(&c, "SELECT v FROM tx ORDER BY k").await,
        [
            "2024-01-01 08:00:00+00",
            "2024-01-01 08:00:00+00",
            "2024-01-01 08:00:00+00",
            "2024-01-01 08:00:00+00",
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00"
        ]
    );
    assert_eq!(
        column(&c, "SELECT k FROM tx WHERE v = '2024-01-01 10:00:00+02'::timestamptz::text ORDER BY k").await,
        ["1", "2", "3", "4"]
    );
    assert_eq!(
        column(&c, "SELECT k FROM tx WHERE v = '2024-01-01 10:00:00+02'::timestamp::text ORDER BY k").await,
        ["5", "6", "7", "8"]
    );
    run(&c, "CREATE TABLE tc (a TIMESTAMP, b TIMESTAMPTZ)").await.expect("create");
    run(&c, "INSERT INTO tc VALUES ('2024-01-01 10:00:00', '2024-01-01 10:00:00+02')").await.expect("insert");
    // However deep the parentheses, `::timestamp` discards the literal's zone.
    run(&c, "CREATE TABLE tn (b TIMESTAMPTZ)").await.expect("create");
    run(&c, "INSERT INTO tn VALUES ((('2024-01-01 10:00:00+02'))::timestamp)").await.expect("insert nested");
    assert_eq!(column(&c, "SELECT b FROM tn").await, ["2024-01-01 10:00:00+00"]);
    let got = rows(&c, "SELECT a::text, b::text, a || '', b || '' FROM tc").await;
    assert_eq!(got[0][0].as_deref(), Some("2024-01-01 10:00:00"), "{got:?}");
    assert_eq!(got[0][1].as_deref(), Some("2024-01-01 08:00:00+00"), "{got:?}");
    assert_eq!(got[0][2].as_deref(), Some("2024-01-01 10:00:00"), "{got:?}");
    assert_eq!(got[0][3].as_deref(), Some("2024-01-01 08:00:00+00"), "{got:?}");

    // An expression's timestamp, folded in an INSERT or computed in an UPDATE.
    run(
        &c,
        "INSERT INTO tx (k, v) VALUES (9, CASE WHEN 1 = 1 THEN '2024-01-01 10:00:00+02'::timestamptz END), \
         (10, CASE WHEN 1 = 1 THEN '2024-01-01 10:00:00+02'::timestamp END), (11, 'x'), (12, 'x')",
    )
    .await
    .expect("insert case");
    run(&c, "UPDATE tx SET v = CASE WHEN k = 11 THEN '2024-01-01 10:00:00+02'::timestamptz END WHERE k = 11")
        .await
        .expect("update case");
    run(&c, "UPDATE tx SET v = CASE WHEN 1 = 1 THEN '2024-01-01 10:00:00+02'::timestamp END WHERE k = 12")
        .await
        .expect("update local case");
    assert_eq!(
        column(&c, "SELECT v FROM tx WHERE k >= 9 ORDER BY k").await,
        ["2024-01-01 08:00:00+00", "2024-01-01 10:00:00", "2024-01-01 08:00:00+00", "2024-01-01 10:00:00"]
    );
}

// A generated column over a TIMESTAMP compares it as a timestamp, as
// PostgreSQL does: any spelling of the literal, and a zone on it ignored.
// It used to compare the column's text with the literal's.
#[tokio::test]
async fn a_generated_column_compares_a_timestamp_as_a_timestamp() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE g4 (k INT, a TIMESTAMP, \
         late BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01 09:00:00+05') STORED, \
         iso BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01T09:00:00') STORED, \
         short BOOLEAN GENERATED ALWAYS AS (a > '2024-1-1 9:00') STORED, \
         frac BOOLEAN GENERATED ALWAYS AS (a = '2024-01-01 10:00:00.000') STORED, \
         other BOOLEAN GENERATED ALWAYS AS (a < '2024-01-01 10:00:00+00') STORED)",
    )
    .await
    .expect("create");
    run(
        &c,
        "INSERT INTO g4 (k, a) VALUES (1, '2024-01-01 10:00:00'), (2, '2024-01-01 08:00:00'), \
         (3, '2024-01-01 10:00:00.5'), (4, '2024-01-01 09:30:00+02')",
    )
    .await
    .expect("insert");
    let sql = "SELECT k, late, iso, short, frac, other FROM g4 ORDER BY k";
    let flags = |got: Vec<Vec<Option<String>>>| -> Vec<String> {
        got.into_iter().map(|r| r.into_iter().map(|v| v.unwrap_or_default()).collect::<Vec<_>>().join("|")).collect()
    };
    assert_eq!(flags(rows(&c, sql).await), ["1|t|t|t|t|f", "2|f|f|f|f|t", "3|t|t|t|f|f", "4|t|t|t|f|t"]);
    run(&c, "UPDATE g4 SET a = '2024-01-01 09:30:00+07' WHERE k = 2").await.expect("update");
    assert_eq!(flags(rows(&c, "SELECT k, late, iso, short, frac, other FROM g4 WHERE k = 2").await), ["2|t|t|t|f|t"]);
    run(
        &c,
        "CREATE TABLE g5 (k INT, a TIMESTAMPTZ, late BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01 09:00:00+05') STORED, \
         iso BOOLEAN GENERATED ALWAYS AS (a > '2024-01-01T07:00:00') STORED)",
    )
    .await
    .expect("create");
    run(&c, "INSERT INTO g5 (k, a) VALUES (1, '2024-01-01 10:00:00+02'), (2, '2024-01-01 03:00:00')").await.expect("insert");
    assert_eq!(flags(rows(&c, "SELECT k, late, iso FROM g5 ORDER BY k").await), ["1|t|t", "2|f|f"]);
}
