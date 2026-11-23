//! A `double precision` or `real` value's text is PostgreSQL's: the shortest
//! digits that read back as the same float, with an exponent past fifteen
//! digits (six for `real`) or below 10^-4 — `1e+20`, `1e-07`, `3.4e+38` — and
//! `NaN`, `Infinity`, `-Infinity` by name.
//!
//! The engine wrote Rust's rendering, which never uses an exponent: `1e20` was
//! `100000000000000000000` and `3.4e38::real` a 39-digit string, on the wire
//! and in `::text`, so a `varchar(6)` that takes PostgreSQL's `1e+20` refused
//! the value; and `::text` of an infinity was `inf`. Every expectation is
//! PostgreSQL 17's (`docker run --rm postgres:17`), read as text.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn seeded(c: &Client) {
    for sql in [
        "CREATE TABLE fl (id INT PRIMARY KEY, f FLOAT8, r REAL, t TEXT, vc VARCHAR(6), c CHAR(6))",
        "INSERT INTO fl VALUES (1, 1e20, 3.4e38, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (2, 1e-7, 1e-7, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (3, '-0', 'NaN', 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (4, 'Infinity', '-Infinity', 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (5, 0.1, 0.1, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (6, 123456789012345, 1e6, 'x', 'x', 'x')",
    ] {
        run(c, sql).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn floats_read_as_postgresql_writes_them() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seeded(&c).await;
    // The wire, `::text`, a typmod cast and a concatenation all agree.
    assert_eq!(
        rows(&c, "SELECT id, f, r, f::text, r::text, f::varchar(6), 'v=' || f FROM fl ORDER BY id").await,
        [
            "1|1e+20|3.4e+38|1e+20|3.4e+38|1e+20|v=1e+20",
            "2|1e-07|1e-07|1e-07|1e-07|1e-07|v=1e-07",
            "3|-0|NaN|-0|NaN|-0|v=-0",
            "4|Infinity|-Infinity|Infinity|-Infinity|Infini|v=Infinity",
            "5|0.1|0.1|0.1|0.1|0.1|v=0.1",
            "6|123456789012345|1e+06|123456789012345|1e+06|123456|v=123456789012345",
        ]
    );
    assert_eq!(rows(&c, "SELECT id FROM fl WHERE f::text = '1e+20'").await, ["1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_float_stored_as_text_is_postgresqls_text() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seeded(&c).await;
    run(&c, "UPDATE fl SET t = f WHERE id > 0").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM fl ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
    run(&c, "UPDATE fl SET t = r WHERE id > 0").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM fl ORDER BY id").await,
        ["1|3.4e+38", "2|1e-07", "3|NaN", "4|-Infinity", "5|0.1", "6|1e+06"]
    );
    // Six characters, which `varchar(6)` and `char(6)` take.
    run(&c, "UPDATE fl SET vc = f, c = f WHERE id = 1").await;
    run(&c, "UPDATE fl SET vc = r WHERE id = 2").await;
    assert_eq!(rows(&c, "SELECT id, vc, c FROM fl WHERE id < 3 ORDER BY id").await, ["1|1e+20|1e+20 ", "2|1e-07|x     "]);
    run(&c, "CREATE TABLE ft (id INT PRIMARY KEY, t TEXT)").await;
    run(&c, "INSERT INTO ft SELECT id, f FROM fl").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM ft ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
    // And into a float column, the same floats.
    run(&c, "CREATE TABLE ff (id INT PRIMARY KEY, f FLOAT8, i INT)").await;
    run(&c, "INSERT INTO ff SELECT id, f, 2 FROM fl").await;
    assert_eq!(
        rows(&c, "SELECT id, f FROM ff ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
}

/// A float scalar subquery is a float: substituted back as a bare constant,
/// `(SELECT max(f) …)` over a float8 column became a `numeric` constant, so
/// the column was described as numeric (1700) where PostgreSQL 17 describes
/// float8 (701) and float4 (700), and a driver decoding f64 refused it.
/// (Read through the simple protocol: an extended-protocol Describe of a
/// statement holding a scalar subquery answers NoData, before and after.)
#[tokio::test(flavor = "multi_thread")]
async fn a_float_scalar_subquery_is_described_as_a_float() {
    use sqlx::{AssertSqlSafe, Row};
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT8, r REAL)").await;
    run(&c, "INSERT INTO t VALUES (1, 0.1, 0.25), (2, 1e20, 3.4e38)").await;
    let pool = server.connect_sqlx().await.expect("sqlx");
    let row = sqlx::raw_sql(AssertSqlSafe("SELECT (SELECT max(f) FROM t), (SELECT min(r) FROM t) FROM t WHERE id = 1".to_string()))
        .fetch_one(&pool)
        .await
        .expect("row");
    assert_eq!(row.try_get::<f64, _>(0).expect("described float8"), 1e20);
    assert_eq!(row.try_get::<f32, _>(1).expect("described float4"), 0.25);
    // And it still compares and stores as the float it is.
    assert_eq!(rows(&c, "SELECT id FROM t WHERE f > (SELECT min(f) FROM t)").await, ["2"]);
    run(&c, "UPDATE t SET f = (SELECT max(f) FROM t) WHERE id = 1").await;
    assert_eq!(rows(&c, "SELECT id, f FROM t ORDER BY id").await, ["1|1e+20", "2|1e+20"]);
}

/// A `real` stored into a NUMERIC(p, s) is its six-digit text rounded to
/// `s`, as PostgreSQL's `float4_numeric` reads it: 2.675 is 2.68. Rounded
/// from its binary value (2.67499995…) it stored 2.67 — through INSERT …
/// SELECT, a scalar subquery and a `::real` literal alike. PostgreSQL 17's
/// values.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_into_a_scaled_numeric_rounds_its_text() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, r REAL, f FLOAT8)").await;
    run(&c, "INSERT INTO rl VALUES (1, 2.675, 2.675), (2, 1.005, 1.005), (4, 1234.565, 1234.565), (6, 33.335, 33.335)").await;
    run(&c, "CREATE TABLE rn (id INT PRIMARY KEY, n2 NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rn SELECT id, r FROM rl").await;
    run(&c, "INSERT INTO rn SELECT id + 30, f FROM rl").await;
    run(&c, "INSERT INTO rn VALUES (61, 2.675::real), (62, '1.005'::real), (70, 0)").await;
    run(&c, "UPDATE rn SET n2 = (SELECT r FROM rl WHERE id = 6) WHERE id = 70").await;
    assert_eq!(
        rows(&c, "SELECT id, n2 FROM rn ORDER BY id").await,
        ["1|2.68", "2|1.01", "4|1234.56", "6|33.34", "31|2.68", "32|1.01", "34|1234.57", "36|33.34", "61|2.68", "62|1.01", "70|33.34"]
    );
}

/// An integer column compared with a float — a scalar subquery's value
/// (substituted as `'0.5'::float8`), or that literal written out — is
/// compared as PostgreSQL compares `int = float8`, over a table big enough
/// for the filter index. The quoted float under a cast was text to the
/// plan, which refused it (XX000).
#[tokio::test(flavor = "multi_thread")]
async fn an_integer_column_compares_with_a_float_subquery() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE iq (id INT PRIMARY KEY, b BIGINT, i INT, s SMALLINT)").await;
    for chunk in 0..10 {
        let values: Vec<String> = (0..500).map(|k| chunk * 500 + k).map(|g| format!("({g}, {g}, {g}, {g})")).collect();
        run(&c, &format!("INSERT INTO iq VALUES {}", values.join(","))).await;
    }
    run(&c, "CREATE TABLE fs (k INT PRIMARY KEY, f FLOAT8, r REAL)").await;
    run(&c, "INSERT INTO fs VALUES (1, 0.5, 0.5), (2, 2.0, 2.0), (3, 2.5, 2.5), (4, 1e20, 1e20)").await;
    // PostgreSQL 17's counts for each.
    for (predicate, pg) in [
        ("b > (SELECT f FROM fs WHERE k = 1)", "4999"),
        ("i >= (SELECT f FROM fs WHERE k = 3)", "4997"),
        ("b = (SELECT f FROM fs WHERE k = 2)", "1"),
        ("b < (SELECT f FROM fs WHERE k = 4)", "5000"),
        ("s < (SELECT r FROM fs WHERE k = 3)", "3"),
        ("i > '0.5'::float8", "4999"),
    ] {
        assert_eq!(rows(&c, &format!("SELECT count(*) FROM iq WHERE {predicate}")).await, [pg], "{predicate}");
    }
    run(&c, "DELETE FROM iq WHERE b < (SELECT f FROM fs WHERE k = 3)").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM iq").await, ["4997"]);
}

async fn ids(c: &Client, sql: &str) -> String {
    rows(c, sql).await.join(",")
}

async fn sqlstate(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| e.to_string())
}

/// A quoted float under a float cast in a WHERE clause is that float, read
/// as PostgreSQL 17 reads it: `.5` and `-.5` (refused as 42601 "unexpected
/// character '.'"), an exponent, a trailing point; a `real` is its widened
/// value (`f8 * 2 = '0.2'::real` matched a row PostgreSQL does not, and `f8
/// = '0.1'::real` a value that is not that float); and against text — a
/// text, `character` or `varchar` column, or a JSON path read as text — it
/// is 42883, as PostgreSQL has no such operator (`j->>'x' = '0.5'::float8`
/// matched nothing and reported success).
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_float_in_a_predicate_is_postgresqls() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE e (id INT PRIMARY KEY, f8 FLOAT8, f4 REAL, b BIGINT, t TEXT, c CHAR(4), v VARCHAR(10), j JSONB)").await;
    run(
        &c,
        "INSERT INTO e VALUES (1, 0.5, 0.1, 1, '0.5', '0.5', '0.5', '{\"x\": 0.5}'), \
         (2, 1.0, 0.5, 2, '1', '1', '1', '{\"x\": \"0.5\"}'), (3, 0.1, 0.25, 3, '0.1', '0.1', '0.1', '{\"x\": 1}')",
    )
    .await;
    // PostgreSQL 17's ids for each.
    for (predicate, pg) in [
        ("f8 = '.5'::float8", "1"),
        ("f8 > '-.5'::float8", "1,2,3"),
        ("f8 = '5E-1'::float8", "1"),
        ("f8 = '1.'::float8", "2"),
        ("b > '.5'::float8", "1,2,3"),
        ("f8 * 2 = '0.2'::real", ""),
        ("f8 = '0.1'::real", ""),
        ("f4 = '0.1'::real", "1"),
        ("f8 = '0.1'::float8", "3"),
    ] {
        assert_eq!(ids(&c, &format!("SELECT id FROM e WHERE {predicate} ORDER BY id")).await, pg, "{predicate}");
    }
    for predicate in [
        "j->>'x' = '0.5'::float8",
        "t = '0.5'::float8",
        "c = '0.5'::float8",
        "v = '0.5'::float8",
        "'0.5'::float8 = t",
    ] {
        assert_eq!(sqlstate(&c, &format!("SELECT id FROM e WHERE {predicate}")).await, "42883", "{predicate}");
    }
}

/// `UPDATE … SET u = f FROM …` reads a float into an unconstrained NUMERIC
/// as every other path does, as its fifteen-digit text: its seventeen
/// shortest digits, written back bare, stored 123456789012345678 as
/// 123456789012345680 and refused 0.30000000000000004 and
/// 1.0000000000000002. PostgreSQL 17 stores 123456789012346000, 0.3 and 1.
#[tokio::test(flavor = "multi_thread")]
async fn update_from_writes_a_float_as_its_text() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fs (k INT PRIMARY KEY, f FLOAT8)").await;
    run(&c, "INSERT INTO fs VALUES (1, 123456789012345678), (2, 0.30000000000000004), (3, 1.0000000000000002)").await;
    run(&c, "CREATE TABLE d (id INT PRIMARY KEY, u NUMERIC)").await;
    run(&c, "INSERT INTO d VALUES (1, 0), (2, 0), (3, 0)").await;
    run(&c, "UPDATE d SET u = fs.f FROM fs WHERE fs.k = d.id").await;
    assert_eq!(rows(&c, "SELECT id, u::text FROM d ORDER BY id").await, ["1|123456789012346000", "2|0.3", "3|1"]);
}

/// A `real` stored into a `double precision` column is that real, widened
/// exactly (0.1::real is 0.10000000149011612), as PostgreSQL 17 stores it —
/// through every path in: a typed literal (`::real`, `::float4`, `CAST(… AS
/// real)`), INSERT … SELECT, UPDATE SET (a literal, a REAL column, a REAL
/// subquery), UPDATE … FROM, ON CONFLICT … DO UPDATE (`excluded.f4`), a
/// DEFAULT and a generated column, and `::float8` in a select list. Each
/// stored the double nearest the real's digits (0.1), so `f8 = (SELECT f4 …)`
/// found none of the rows PostgreSQL finds.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_into_double_precision_is_widened() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for sql in [
        "CREATE TABLE src (id INT PRIMARY KEY, r REAL)",
        "INSERT INTO src VALUES (1, 0.1)",
        "CREATE TABLE x (id INT PRIMARY KEY, f8 FLOAT8 DEFAULT '0.1'::real, f4 REAL, g FLOAT8 GENERATED ALWAYS AS (f4) STORED)",
        "INSERT INTO x (id, f8, f4) VALUES (1, '0.1'::real, '0.1'::real)",
        "INSERT INTO x (id, f8, f4) VALUES (2, '0.1'::float4, 0.1)",
        "INSERT INTO x (id, f8, f4) VALUES (3, CAST('0.1' AS real), 0.1)",
        "INSERT INTO x (id, f8, f4) SELECT 4, r, r FROM src WHERE id = 1",
        "INSERT INTO x (id, f4) VALUES (5, 0.1)",
        "INSERT INTO x (id, f8, f4) VALUES (6, 0, 0.1), (7, 0, 0.1), (8, 0, 0.1), (9, 0, 0.1)",
        "UPDATE x SET f8 = '0.1'::real WHERE id = 6",
        "UPDATE x SET f8 = src.r FROM src WHERE src.id = 1 AND x.id = 7",
        "UPDATE x SET f8 = (SELECT r FROM src WHERE id = 1) WHERE id = 8",
        "UPDATE x SET f8 = f4 WHERE id = 9",
        "INSERT INTO x (id, f8, f4) VALUES (9, 0, 0.1) ON CONFLICT (id) DO UPDATE SET f8 = excluded.f4",
    ] {
        run(&c, sql).await;
    }
    let widened: Vec<String> = (1..=9).map(|id| format!("{id}|0.10000000149011612|0.1|0.10000000149011612")).collect();
    assert_eq!(rows(&c, "SELECT id, f8, f4, g FROM x ORDER BY id").await, widened);
    assert_eq!(ids(&c, "SELECT id FROM x WHERE f8 = (SELECT r FROM src WHERE id = 1) ORDER BY id").await, "1,2,3,4,5,6,7,8,9");
    // A REAL column itself is unchanged.
    assert_eq!(ids(&c, "SELECT id FROM x WHERE f4 = '0.1'::real ORDER BY id").await, "1,2,3,4,5,6,7,8,9");
    assert_eq!(
        rows(&c, "SELECT r::float8, CAST(r AS float8) FROM src").await,
        ["0.10000000149011612|0.10000000149011612"]
    );

    // q02's statement end to end: two rows written through a real, one not.
    run(&c, "CREATE TABLE q (id INT PRIMARY KEY, f8 FLOAT8, f4 REAL)").await;
    run(&c, "INSERT INTO q VALUES (1, 0.1, 0.1)").await;
    run(&c, "INSERT INTO q VALUES (2, '0.1'::real, '0.1'::real)").await;
    run(&c, "INSERT INTO q VALUES (3, 0.1, 0.1)").await;
    run(&c, "UPDATE q SET f8 = '0.1'::real WHERE id = 1").await;
    assert_eq!(ids(&c, "SELECT id FROM q WHERE f8 = (SELECT f4 FROM q WHERE id = 3) ORDER BY id").await, "1,2");
    assert_eq!(ids(&c, "SELECT id FROM q WHERE f8 = 0.1 ORDER BY id").await, "3");
    // A real into a NUMERIC(p, s) is still its six-digit text, rounded.
    run(&c, "CREATE TABLE n (id INT PRIMARY KEY, a NUMERIC(10,2))").await;
    run(&c, "INSERT INTO n SELECT 1, f4 FROM q WHERE id = 1").await;
    assert_eq!(rows(&c, "SELECT a FROM n").await, ["0.10"]);
}

/// Widened reals in a `double precision` column, over a table big enough for
/// the filter index and zone maps: no bound prunes a row it matches.
/// PostgreSQL 17's counts.
#[tokio::test(flavor = "multi_thread")]
async fn widened_reals_are_never_pruned() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fz (id INT PRIMARY KEY, f8 FLOAT8)").await;
    for chunk in 0..10 {
        let values: Vec<String> = (1..=500)
            .map(|k| chunk * 500 + k)
            .map(|g| if g % 2 == 0 { format!("({g}, '0.1'::real)") } else { format!("({g}, {g})") })
            .collect();
        run(&c, &format!("INSERT INTO fz VALUES {}", values.join(","))).await;
    }
    run(&c, "CREATE TABLE s (k INT PRIMARY KEY, r REAL)").await;
    run(&c, "INSERT INTO s VALUES (1, 0.1)").await;
    for (predicate, pg) in [
        ("f8 = (SELECT r FROM s)", "2500"),
        ("f8 >= '0.1'::real AND f8 < 0.2", "2500"),
        ("f8 > 0.1", "5000"),
        ("f8 < '0.1'::real", "0"),
    ] {
        assert_eq!(rows(&c, &format!("SELECT count(*) FROM fz WHERE {predicate}")).await, [pg], "{predicate}");
    }
}

/// A CHECK written with a `real` reads it as PostgreSQL does, the real
/// widened, against the widened values the column now holds: `f8 <>
/// '0.1'::real` refuses '0.1'::real whether it arrives by VALUES, INSERT …
/// SELECT of a REAL column or UPDATE, `0.1::float4` too, and `g <
/// '0.1'::real` refuses that real. They were admitted: the check compared the
/// double 0.1, or compared a double with a NUMERIC exactly where PostgreSQL
/// compares in `double precision`. PostgreSQL 17's 23514s and rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_check_with_a_real_reads_it_widened() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE ck (id INT PRIMARY KEY, f8 FLOAT8 CHECK (f8 <> '0.1'::real), g FLOAT8 CHECK (g < '0.1'::real), \
         k FLOAT8 CHECK (k <> 0.1::float4), d FLOAT8 CHECK (d <> 0.1))",
    )
    .await;
    run(&c, "CREATE TABLE rs (id INT PRIMARY KEY, r REAL)").await;
    run(&c, "INSERT INTO rs VALUES (1, 0.1)").await;
    run(&c, "INSERT INTO ck (id, f8) VALUES (3, 0.1)").await;
    run(&c, "INSERT INTO ck (id, g) VALUES (4, 0.1)").await;
    for sql in [
        "INSERT INTO ck (id, f8) VALUES (1, '0.1'::real)",
        "INSERT INTO ck (id, f8) SELECT 2, r FROM rs",
        "UPDATE ck SET f8 = '0.1'::real WHERE id = 3",
        "UPDATE ck SET f8 = (SELECT r FROM rs) WHERE id = 3",
        "INSERT INTO ck (id, g) VALUES (5, '0.1'::real)",
        "INSERT INTO ck (id, k) VALUES (6, '0.1'::real)",
        // A plain NUMERIC literal against a double, compared as a double.
        "INSERT INTO ck (id, d) VALUES (7, 0.1)",
    ] {
        assert_eq!(sqlstate(&c, sql).await, "23514", "{sql}");
    }
    assert_eq!(rows(&c, "SELECT id, f8, g FROM ck ORDER BY id").await, ["3|0.1|", "4||0.1"]);
}

/// `EXCLUDED.<column>` written `DEFAULT` in the VALUES is that column's
/// default, as PostgreSQL resolves it: a REAL default under `excluded.f4`
/// failed with 42703 once wrapped as a real, a FLOAT8 reading
/// `excluded.f4` stored NULL before that, and a NUMERIC default is that
/// default at its scale. PostgreSQL 17's rows.
#[tokio::test(flavor = "multi_thread")]
async fn excluded_default_is_the_columns_default() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE dd (id INT PRIMARY KEY, f4 REAL DEFAULT 0.25, f8 FLOAT8 DEFAULT 0.5, n NUMERIC(10,2) DEFAULT 1.5, t TEXT)").await;
    run(&c, "INSERT INTO dd VALUES (1, 1, 1, 1, 'a')").await;
    run(
        &c,
        "INSERT INTO dd VALUES (1, DEFAULT, DEFAULT, DEFAULT, 'b') ON CONFLICT (id) DO UPDATE \
         SET f4 = excluded.f4, f8 = excluded.f8, n = excluded.n, t = excluded.t",
    )
    .await;
    assert_eq!(rows(&c, "SELECT * FROM dd").await, ["1|0.25|0.5|1.50|b"]);
    run(&c, "INSERT INTO dd VALUES (1, DEFAULT, 9, 9, 'c') ON CONFLICT (id) DO UPDATE SET f8 = excluded.f4").await;
    assert_eq!(rows(&c, "SELECT * FROM dd").await, ["1|0.25|0.25|1.50|b"]);
}

/// An IN list built from a REAL subquery reads each real widened, so a
/// double holding the widened real matches and a double holding 0.1 does
/// not. Values from PostgreSQL 17.
#[tokio::test(flavor = "multi_thread")]
async fn an_in_list_of_reals_reads_them_widened() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE x (id INT PRIMARY KEY, f8 FLOAT8, f4 REAL)").await;
    run(&c, "INSERT INTO x VALUES (1, '0.1'::real, 0.1), (2, 0.1, 0.25)").await;
    run(&c, "CREATE TABLE rs (id INT PRIMARY KEY, r REAL)").await;
    run(&c, "INSERT INTO rs VALUES (1, 0.1), (2, 0.25)").await;
    assert_eq!(ids(&c, "SELECT id FROM x WHERE f8 IN (SELECT r FROM rs) ORDER BY id").await, "1");
    assert_eq!(ids(&c, "SELECT id FROM x WHERE f4 IN (SELECT r FROM rs) ORDER BY id").await, "1,2");
}

/// A NUMERIC or an integer against a double is PostgreSQL's float8
/// comparison: the exact value converted to its nearest double. Every
/// operator, the scan, IN lists and the hash join agree on it. Values from
/// PostgreSQL 17.
#[tokio::test(flavor = "multi_thread")]
async fn a_number_against_a_double_is_its_nearest_double() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE c9 (id INT PRIMARY KEY, n NUMERIC(30,9), f8 FLOAT8)").await;
    run(
        &c,
        "INSERT INTO c9 VALUES (1, 0.1, 0.1), (3, 1234567890.123456789, 1234567890.123456789), \
         (5, 9007199254740.123456789, 9007199254740.123456789), (8, 99999999999999999999.999999999, 1e20)",
    )
    .await;
    assert_eq!(
        rows(
            &c,
            "SELECT id, n = f8, n < f8, n > f8, CASE WHEN n = f8 THEN 'eq' WHEN n < f8 THEN 'lt' ELSE 'gt' END, \
             GREATEST(n, f8) = f8, LEAST(n, f8) = n, NULLIF(n, f8) IS NULL FROM c9 ORDER BY id"
        )
        .await,
        ["1|t|f|f|eq|t|t|t", "3|t|f|f|eq|t|t|t", "5|t|f|f|eq|t|t|t", "8|t|f|f|eq|t|t|t"]
    );
    assert_eq!(ids(&c, "SELECT id FROM c9 WHERE n IN (SELECT f8 FROM c9) ORDER BY id").await, "1,3,5,8");
    assert_eq!(ids(&c, "SELECT id FROM c9 WHERE n NOT IN (SELECT f8 FROM c9 WHERE id < 5) ORDER BY id").await, "5,8");
    assert_eq!(ids(&c, "SELECT id FROM c9 WHERE n = 1234567890.123456789::float8 ORDER BY id").await, "3");
    assert_eq!(ids(&c, "SELECT id FROM c9 WHERE n < '1e20'::float8 ORDER BY id").await, "1,3,5");
    assert_eq!(ids(&c, "SELECT id FROM c9 WHERE n >= '9007199254740.123456789'::float8 ORDER BY id").await, "5,8");
    assert_eq!(
        ids(&c, "SELECT id FROM c9 WHERE n IN ('1e20'::float8, '9007199254740.123456789'::float8) ORDER BY id").await,
        "5,8"
    );
    // A NUMERIC with more digits than the double's shortest text.
    assert_eq!(
        rows(&c, "SELECT id, 0.10000000000000001 = f8, 0.10000000000000001 < f8 FROM c9 WHERE id = 1").await,
        ["1|t|f"]
    );
    // The hash join and the projected comparison agree on every pair.
    assert_eq!(
        rows(
            &c,
            "SELECT x.id, y.id, x.n = y.f8, CASE WHEN x.n = y.f8 THEN 'eq' ELSE 'ne' END \
             FROM c9 x JOIN c9 y ON x.n = y.f8 ORDER BY 1, 2"
        )
        .await,
        ["1|1|t|eq", "3|3|t|eq", "5|5|t|eq", "8|8|t|eq"]
    );
    run(&c, "CREATE TABLE ci (id INT PRIMARY KEY, i BIGINT, f8 FLOAT8)").await;
    run(&c, "INSERT INTO ci VALUES (1, 9007199254740993, 9007199254740992), (2, 3, 2.5)").await;
    assert_eq!(rows(&c, "SELECT id, i = f8, i > f8 FROM ci ORDER BY id").await, ["1|t|f", "2|f|t"]);
    assert_eq!(ids(&c, "SELECT id FROM ci WHERE i = '9007199254740992'::float8 ORDER BY id").await, "1");
    assert_eq!(ids(&c, "SELECT id FROM ci WHERE i > 2.5::float8 ORDER BY id").await, "1,2");
    // A CHECK against a double: both NUMERICs whose nearest double it is.
    run(&c, "CREATE TABLE ck9b (id INT PRIMARY KEY, n NUMERIC(30,9) CHECK (n <> 1234567890.123456789::float8))").await;
    for sql in ["INSERT INTO ck9b VALUES (1, 1234567890.123456789)", "INSERT INTO ck9b VALUES (2, 1234567890.1234567)"] {
        assert_eq!(sqlstate(&c, sql).await, "23514", "{sql}");
    }
    run(&c, "INSERT INTO ck9b VALUES (3, 1234567890.123457)").await;
    assert_eq!(ids(&c, "SELECT id FROM ck9b ORDER BY id").await, "3");
}

/// An IN list of floats with a NULL among them: the floats compared as
/// PostgreSQL compares them (a NUMERIC(38,20) 0.10000000000000001 is the
/// double 0.1), the NULL keeping IN unknown without a match and NOT IN never
/// true. Values from PostgreSQL 17.
#[tokio::test(flavor = "multi_thread")]
async fn an_in_list_of_floats_with_a_null() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(10,2), v NUMERIC(38,20))").await;
    run(&c, "INSERT INTO t VALUES (1, 1.00, 0.1), (2, 2.00, 0.10000000000000001), (3, NULL, 2), (4, 4.00, 0.3)").await;
    run(&c, "CREATE TABLE fs (id INT PRIMARY KEY, f FLOAT8)").await;
    run(&c, "INSERT INTO fs VALUES (1, 0.1), (2, NULL), (3, 2)").await;
    for (sql, want) in [
        ("SELECT id FROM t WHERE v IN ('0.1'::float8, NULL, '2'::float8) ORDER BY id", "1,2,3"),
        ("SELECT id FROM t WHERE v NOT IN ('0.1'::float8, NULL) ORDER BY id", ""),
        ("SELECT id FROM t WHERE v NOT IN ('0.1'::float8, '2'::float8) ORDER BY id", "4"),
        ("SELECT id FROM t WHERE n IN ('1'::float8, NULL, '2'::float8) ORDER BY id", "1,2"),
        ("SELECT id FROM t WHERE n NOT IN ('1'::float8, NULL) ORDER BY id", ""),
        ("SELECT id FROM t WHERE NOT (v IN ('0.1'::float8, NULL)) ORDER BY id", ""),
        ("SELECT id FROM t WHERE v IN (SELECT f FROM fs) ORDER BY id", "1,2,3"),
        ("SELECT id FROM t WHERE n NOT IN (SELECT f FROM fs) ORDER BY id", ""),
    ] {
        assert_eq!(ids(&c, sql).await, want, "{sql}");
    }
}

/// A large IN list of floats over an integer and a NUMERIC column, mixing
/// floats only one value rounds to (an integer below 2^53, a NUMERIC(30,9)
/// 300.5), floats many values round to (a bigint past 2^53, a NUMERIC(30,9)
/// past 10^7) and floats nothing rounds to. Values from PostgreSQL 17.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_in_list_mixing_single_values_and_runs() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    let m_row = |k: u64| -> (u64, String) {
        if k <= 600 {
            (k * 1000 + 7, format!("{}.{}", k / 2, if k % 2 == 1 { 5 } else { 0 }))
        } else {
            (9_007_199_254_740_992 + (k - 600) * 3, format!("10000000.{:03}", k - 600))
        }
    };
    run(&c, "CREATE TABLE m (id INT PRIMARY KEY, i BIGINT, n NUMERIC(30,9))").await;
    run(&c, "CREATE TABLE src (id INT PRIMARY KEY, f FLOAT8, g FLOAT8)").await;
    let rows_m: Vec<String> = (1..=1200).map(|k| {
        let (i, n) = m_row(k);
        format!("({k}, {i}, {n})")
    }).collect();
    for chunk in rows_m.chunks(200) {
        run(&c, &format!("INSERT INTO m VALUES {}", chunk.join(", "))).await;
    }
    let rows_src: Vec<String> = (1..=1500u64).map(|j| {
        if j <= 1200 {
            let (i, n) = m_row(j);
            let f = if j % 10 == 0 { format!("{i}.5") } else { i.to_string() };
            let g = if j % 7 == 0 { format!("{n}1") } else { n };
            format!("({j}, {f}, {g})")
        } else {
            format!("({j}, {}.25, {j}.123456789123)", j * 1000 + 7)
        }
    }).collect();
    for chunk in rows_src.chunks(250) {
        run(&c, &format!("INSERT INTO src VALUES {}", chunk.join(", "))).await;
    }
    for (sql, want) in [
        ("SELECT count(*), sum(id) FROM m WHERE i IN (SELECT f FROM src)", "1140|702300"),
        ("SELECT count(*), sum(id) FROM m WHERE i NOT IN (SELECT f FROM src)", "60|18300"),
        ("SELECT count(*), sum(id) FROM m WHERE n IN (SELECT g FROM src)", "1029|617658"),
        ("SELECT count(*), sum(id) FROM m WHERE n NOT IN (SELECT g FROM src)", "171|102942"),
        ("SELECT count(*), sum(id) FROM m WHERE i IN (SELECT f FROM src WHERE id > 600)", "600|540300"),
        ("SELECT count(*), sum(id) FROM m WHERE n IN (SELECT g FROM src WHERE id > 600)", "514|462943"),
    ] {
        assert_eq!(rows(&c, sql).await, [want], "{sql}");
    }
}
