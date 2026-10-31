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
