//! What a CTE or a derived table hands back must be what its query selected.
//!
//! Both are materialised into a temporary relation: the inner query runs, a
//! table is created for its rows, and the rows are written into it as
//! literals. Every way that can go wrong is silent, because the outer query
//! then reads a perfectly ordinary table:
//!
//! - The relation was typed by sampling the rows, and a scanned row reports
//!   the physical type, which is TEXT for a document, an array and a range.
//!   Their stored bytes were written back as text, so `'{"k":1}'` came back as
//!   its encoding, non-NULL, and `@>` refused the column as a string.
//! - DATE, TIME, MACADDR, POINT and most of the other wide types had no column
//!   type or no literal at all, and the statement was refused.
//! - A body whose `WHERE` held `IN (SELECT …)` lost its NUMERIC scale, so
//!   339.50 read as 0.000033950.
//! - A derived table inside a derived table was never materialised.
//! - `INSERT INTO t WITH … SELECT …` was not recognised as a CTE at all.
//!
//! Every expected answer is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row, ValueRef};

/// Simple-protocol fetch, so every value arrives as the text a client sees,
/// one row per string, columns joined with `|` and NULL spelled out.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>, String> {
    let fetched = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in fetched {
        let mut values = Vec::new();
        for i in 0..row.len() {
            let v = row.try_get_raw(i).map_err(|e| e.to_string())?;
            if v.is_null() {
                values.push("NULL".to_string());
            } else {
                values.push(v.as_str().map_err(|e| e.to_string())?.to_string());
            }
        }
        out.push(values.join("|"));
    }
    Ok(out)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Collects every disagreement, so one run shows the whole surface.
#[derive(Default)]
struct Wrong(Vec<String>);

impl Wrong {
    async fn check(&mut self, pool: &sqlx::PgPool, sql: &str, want: &[&str]) {
        match rows(pool, sql).await {
            Ok(got) if got == want => {}
            Ok(got) => self.0.push(format!("{sql}\n    got  {got:?}\n    want {want:?}")),
            Err(e) => self.0.push(format!("{sql}\n    error {e}\n    want  {want:?}")),
        }
    }

    fn assert_none(self) {
        assert!(self.0.is_empty(), "{} wrong:\n{}", self.0.len(), self.0.join("\n"));
    }
}

/// The reporter's 31-type table, one row of values and one of NULLs.
const MT: &str = "CREATE TABLE mt (id INT, n NUMERIC(12,2), m MONEY, h HUGEINT, uh UHUGEINT, \
    ub UBIGINT, ui UINT, us USMALLINT, ut UTINYINT, ti TINYINT, ts TIMESTAMPTZ, d DATE, t TIME, \
    ttz TIMETZ, iv INTERVAL, ip INET, ci CIDR, mac MACADDR, m8 MACADDR8, u UUID, b BIT(8), \
    vb VARBIT, pt POINT, r4 INT4RANGE, rd DATERANGE, by BYTEA, tg TEXT[], j JSONB, c CHAR, \
    c5 CHAR(5), f REAL, bo BOOLEAN)";
const MT_ROW: &str = r#"INSERT INTO mt VALUES (1, 1.5, 2.25, 3, 4, 5, 6, 7, 8, 9, '2025-01-01 00:00+00', '2025-01-02', '01:02:03', '01:02:03+02', '1 day', '10.0.0.1', '10.0.0.0/8', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05', '00000000-0000-4000-8000-000000000001', '10101010', '101', '(1,2)', '[1,5)', '[2025-01-01,2025-02-01)', '\xbeef', '{a,b}', '{"k":1}', 'Z', 'ABCDE', 1.5, true)"#;

/// The types the reporter's table does not cover.
const MX: &str = "CREATE TABLE mx (id INT, ln LINE, ls LSEG, bx BOX, pa PATH, pg POLYGON, \
    cr CIRCLE, r8 INT8RANGE, rn NUMRANGE, rt TSRANGE, rz TSTZRANGE, ia INT[], vc VARCHAR(10), \
    tx TEXT, sm SMALLINT, bi BIGINT, db DOUBLE PRECISION, tsn TIMESTAMP, n0 NUMERIC(10,0), \
    n4 NUMERIC(20,4), fr REAL)";
const MX_ROW: &str = "INSERT INTO mx VALUES (1, '{1,-1,0}', '[(0,0),(1,1)]', '((2,2),(0,0))', \
    '[(0,0),(1,1),(2,0)]', '((0,0),(1,1),(2,0))', '<(1,1),2>', '[1,10)', '[1.5,2.5)', \
    '[2025-01-01 00:00,2025-01-02 00:00)', '[2025-01-01 00:00+00,2025-01-02 00:00+00)', \
    '{1,2,3}', 'vee', 'tee', -7, 9000000000, 2.5, '2025-03-04 05:06:07', 12, 3.1416, 0.1)";

/// Each column and the value PostgreSQL 17 prints for it. The unsigned and
/// 128-bit types are this engine's own, so their answer is the value written.
const MT_VALUES: &[(&str, &str)] = &[
    ("n", "1.50"),
    ("h", "3"),
    ("uh", "4"),
    ("ub", "5"),
    ("ui", "6"),
    ("us", "7"),
    ("ut", "8"),
    ("ti", "9"),
    ("ts", "2025-01-01 00:00:00+00"),
    ("d", "2025-01-02"),
    ("t", "01:02:03"),
    ("ttz", "01:02:03+02"),
    ("iv", "1 day"),
    ("ip", "10.0.0.1"),
    ("ci", "10.0.0.0/8"),
    ("mac", "08:00:2b:01:02:03"),
    ("m8", "08:00:2b:01:02:03:04:05"),
    ("u", "00000000-0000-4000-8000-000000000001"),
    ("b", "10101010"),
    ("vb", "101"),
    ("pt", "(1,2)"),
    ("r4", "[1,5)"),
    ("rd", "[2025-01-01,2025-02-01)"),
    ("by", "\\xbeef"),
    ("tg", "{a,b}"),
    ("j", "{\"k\": 1}"),
    ("c", "Z"),
    ("c5", "ABCDE"),
    ("f", "1.5"),
    ("bo", "t"),
];

const MX_VALUES: &[(&str, &str)] = &[
    ("ln", "{1,-1,0}"),
    ("ls", "[(0,0),(1,1)]"),
    ("bx", "(2,2),(0,0)"),
    ("pa", "[(0,0),(1,1),(2,0)]"),
    ("pg", "((0,0),(1,1),(2,0))"),
    ("cr", "<(1,1),2>"),
    ("r8", "[1,10)"),
    ("rn", "[1.5,2.5)"),
    ("rt", "[\"2025-01-01 00:00:00\",\"2025-01-02 00:00:00\")"),
    ("ia", "{1,2,3}"),
    ("vc", "vee"),
    ("tx", "tee"),
    ("sm", "-7"),
    ("bi", "9000000000"),
    ("db", "2.5"),
    ("fr", "0.1"),
    ("n0", "12"),
    ("n4", "3.1416"),
];

/// Every shape a relation can be read back through, for one column.
fn shapes(table: &str, col: &str) -> Vec<String> {
    vec![
        format!("SELECT id, {col} FROM {table} ORDER BY id"),
        format!("WITH x AS (SELECT id, {col} FROM {table}) SELECT id, {col} FROM x ORDER BY id"),
        format!("SELECT id, {col} FROM (SELECT id, {col} FROM {table}) s ORDER BY id"),
        format!(
            "SELECT id, {col} FROM (SELECT id, {col} FROM (SELECT id, {col} FROM {table}) a) s ORDER BY id"
        ),
        // A CTE read through another, and one read twice.
        format!(
            "WITH x AS (SELECT id, {col} FROM {table}), y AS (SELECT id, {col} FROM x) \
             SELECT id, {col} FROM y ORDER BY id"
        ),
        format!(
            "WITH x AS (SELECT id, {col} FROM {table}) \
             SELECT a.id, b.{col} FROM x a JOIN x b ON b.id = a.id ORDER BY a.id"
        ),
        // A derived table over a join.
        format!(
            "SELECT id, {col} FROM (SELECT p.id, q.{col} FROM {table} p JOIN {table} q ON q.id = p.id) s ORDER BY id"
        ),
    ]
}

async fn round_trip(pool: &sqlx::PgPool, wrong: &mut Wrong, table: &str, col: &str, value: &str) {
    let want_row = format!("1|{value}");
    let want = [want_row.as_str(), "2|NULL"];
    for sql in shapes(table, col) {
        wrong.check(pool, &sql, &want).await;
    }
    // NULL stays NULL, and a value is not mistaken for one.
    wrong
        .check(
            pool,
            &format!("WITH x AS (SELECT id, {col} FROM {table}) SELECT id FROM x WHERE {col} IS NULL"),
            &["2"],
        )
        .await;
    wrong
        .check(
            pool,
            &format!("SELECT id FROM (SELECT id, {col} FROM {table}) s WHERE {col} IS NOT NULL"),
            &["1"],
        )
        .await;
    wrong
        .check(
            pool,
            &format!("WITH x AS (SELECT id, {col} FROM {table}) SELECT count({col}) FROM x"),
            &["1"],
        )
        .await;
}

/// #17 and #18: every type comes back value for value, NULL as NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_type_round_trips_through_a_materialised_relation() {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [MT, MT_ROW, "INSERT INTO mt (id) VALUES (2)", MX, MX_ROW, "INSERT INTO mx (id) VALUES (2)"] {
        run(&pool, sql).await;
    }
    let mut wrong = Wrong::default();
    for (col, value) in MT_VALUES {
        round_trip(&pool, &mut wrong, "mt", col, value).await;
    }
    for (col, value) in MX_VALUES {
        round_trip(&pool, &mut wrong, "mx", col, value).await;
    }
    // Three columns whose top-level text already differs from PostgreSQL's,
    // which is a display question outside materialisation: MONEY prints
    // `$2.25` there, and a TIMESTAMP and a TSTZRANGE print without and with
    // an offset. What matters here is that the relation hands back exactly
    // what the table does.
    for (table, col) in [("mt", "m"), ("mx", "tsn"), ("mx", "rz")] {
        let top = rows(&pool, &format!("SELECT id, {col} FROM {table} ORDER BY id"))
            .await
            .expect("top level");
        let top: Vec<&str> = top.iter().map(String::as_str).collect();
        for sql in shapes(table, col) {
            wrong.check(&pool, &sql, &top).await;
        }
    }
    // Every column at once, so the widths of a whole row are exercised too.
    let all = rows(&pool, "SELECT * FROM mt ORDER BY id").await.expect("all");
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    wrong.check(&pool, "WITH x AS (SELECT * FROM mt) SELECT * FROM x ORDER BY id", &all).await;
    wrong.check(&pool, "SELECT * FROM (SELECT * FROM mt) s ORDER BY id", &all).await;
    wrong.assert_none();
}

/// #17: the relation is typed by the declared types, so the operators of a
/// document, an array, a range and a date apply to its columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_materialised_column_keeps_its_declared_type() {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [MT, MT_ROW, "INSERT INTO mt (id) VALUES (2)"] {
        run(&pool, sql).await;
    }
    let mut wrong = Wrong::default();
    for (sql, want) in [
        (r#"WITH x AS (SELECT id, j FROM mt) SELECT count(*) FROM x WHERE j @> '{"k":1}'"#, &["1"][..]),
        (r#"SELECT count(*) FROM (SELECT id, j FROM mt) s WHERE j @> '{"k":1}'"#, &["1"]),
        ("WITH x AS (SELECT id, j FROM mt) SELECT id, j->>'k', j->'k' FROM x ORDER BY id", &["1|1|1", "2|NULL|NULL"]),
        ("SELECT count(*) FROM (SELECT id, j FROM mt) s WHERE j ? 'k'", &["1"]),
        ("WITH x AS (SELECT id, tg FROM mt) SELECT id FROM x WHERE tg @> '{a}'", &["1"]),
        ("SELECT id FROM mt WHERE r4 @> '[2,3)'", &["1"]),
        ("WITH x AS (SELECT id, r4 FROM mt) SELECT id FROM x WHERE r4 @> '[2,3)'", &["1"]),
        ("WITH x AS (SELECT id, d FROM mt) SELECT id FROM x WHERE d > '2025-01-01'", &["1"]),
        // An empty relation is typed too, so the operator still applies.
        (r#"WITH x AS (SELECT id, j FROM mt WHERE id = 99) SELECT count(*) FROM x WHERE j @> '{"k":1}'"#, &["0"]),
    ] {
        wrong.check(&pool, sql, want).await;
    }
    wrong.assert_none();

    // Over the extended protocol the column is described and decoded as a
    // document, not as text.
    let doc: serde_json::Value = sqlx::query("WITH x AS (SELECT id, j FROM mt) SELECT j FROM x WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("typed fetch")
        .get(0);
    assert_eq!(doc, serde_json::json!({"k": 1}));
}

async fn digits(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE digit (n INT, one INT)").await;
    run(
        pool,
        "INSERT INTO digit VALUES (0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1)",
    )
    .await;
}

/// #12: a derived table may hold another, to any depth, with joins inside.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_derived_tables_are_materialised() {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    digits(&pool).await;
    let mut wrong = Wrong::default();
    for (sql, want) in [
        (
            "SELECT k FROM (SELECT k FROM (SELECT a.n AS k, b.n AS m FROM digit a JOIN digit b \
             ON b.one = a.one WHERE a.n = 3 AND b.n < 2) x) s ORDER BY k",
            &["3", "3"][..],
        ),
        (
            "SELECT count(*) FROM (SELECT x.k FROM (SELECT a.n AS k, b.n AS m FROM digit a JOIN digit b \
             ON b.one = a.one) x WHERE x.m = 4) s",
            &["10"],
        ),
        (
            "SELECT s.k, s.m FROM (SELECT y.k, y.m FROM (SELECT x.k, x.m FROM (SELECT a.n AS k, b.n AS m \
             FROM digit a JOIN digit b ON b.one = a.one WHERE a.n = 2 AND b.n > 7) x) y) s ORDER BY s.m",
            &["2|8", "2|9"],
        ),
        (
            "SELECT s.n, d.n FROM (SELECT n FROM (SELECT n FROM digit WHERE n < 2) x) s \
             JOIN (SELECT n FROM (SELECT n FROM digit WHERE n > 8) y) d ON true ORDER BY s.n",
            &["0|9", "1|9"],
        ),
        (
            "WITH w AS (SELECT k FROM (SELECT k FROM (SELECT n AS k FROM digit WHERE n > 6) x) y) \
             SELECT sum(k) FROM w",
            &["24"],
        ),
        (
            "SELECT max(k) FROM (SELECT k FROM (SELECT n AS k FROM digit) x \
             WHERE k IN (SELECT n FROM digit WHERE n < 5)) s",
            &["4"],
        ),
    ] {
        wrong.check(&pool, sql, want).await;
    }
    wrong.assert_none();
    // Nothing a nested materialisation built is left behind.
    let tables = rows(&pool, "SELECT count(*) FROM pg_class WHERE relname LIKE '__cte_%'").await;
    assert_eq!(tables.as_deref().ok(), Some(&["0".to_string()][..]), "{tables:?}");
}
