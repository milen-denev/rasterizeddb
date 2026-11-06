//! `<expr>::cast AS alias`, and the aggregate arguments next to it.
//!
//! The alias splitter was not cast-aware: given a tail beginning `::float8 …`,
//! which is neither `AS` nor a name, it fell through to a whitespace split and
//! cut the item at its *first space*. So it only misfired when the expression
//! contained an internal space — `COUNT(*)::bigint AS n` worked by luck while
//! `(COUNT(*) + 0)::bigint AS n` did not, and the error named a type nobody
//! wrote: "cast to 'bigint AS n' is not supported yet".
//!
//! Two failures were tangled here and are separated below: the splitter, which
//! is fixed, and an aggregate whose *argument* is an expression, which is not.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE utc (id INT PRIMARY KEY, tid INT NOT NULL, column_order INT NOT NULL)",
        "CREATE TABLE ut (id INT PRIMARY KEY, nm TEXT NOT NULL)",
        "INSERT INTO ut (id, nm) VALUES (1, 'a')",
        "INSERT INTO utc (id, tid, column_order) VALUES (1, 1, 10)",
        "INSERT INTO utc (id, tid, column_order) VALUES (2, 1, 20)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cast_with_an_alias_survives_an_internal_space() {
    let (_server, mut conn) = seeded().await;

    for (label, sql, want) in [
        // No internal space: worked before, by luck.
        (
            "count",
            "SELECT c.tid AS tid, COUNT(*)::bigint AS n FROM utc c \
             JOIN ut t ON t.id = c.tid GROUP BY c.tid",
            2i64,
        ),
        (
            "sum of a column",
            "SELECT c.tid AS tid, SUM(c.column_order)::bigint AS n FROM utc c \
             JOIN ut t ON t.id = c.tid GROUP BY c.tid",
            30,
        ),
        // The minimal trigger: a space before the `::`.
        (
            "parenthesised arithmetic",
            "SELECT c.tid AS tid, (COUNT(*) + 0)::bigint AS n FROM utc c \
             JOIN ut t ON t.id = c.tid GROUP BY c.tid",
            2,
        ),
        (
            "single relation",
            "SELECT (COUNT(*) + 0)::bigint AS n FROM utc",
            2,
        ),
    ] {
        let got: i64 = sqlx::query(AssertSqlSafe(sql))
            .fetch_one(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{label}: {sql}: {e}"))
            .try_get("n")
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(got, want, "{label}");
    }
}

/// An aggregate over an expression argument — where it works, and where it is
/// still refused *loudly*.
///
/// On a single relation the schema is in hand when the aggregate is parsed, so
/// the argument's type is known at once, inside an expression too (a trailing
/// cast is one). A join projects the argument as a column whose type is known
/// only once the fetch lists are settled; the expression around the call is
/// parsed again then. With the placeholder type alone the column came back
/// **NULL**, so both are pinned to the value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expression_aggregate_argument() {
    let (_server, mut conn) = seeded().await;

    // Works: 10 + 20, each plus one.
    let got: i64 = sqlx::query(AssertSqlSafe("SELECT SUM(column_order + 1) AS n FROM utc"))
        .fetch_one(&mut conn)
        .await
        .expect("an expression argument on a single relation")
        .try_get("n")
        .expect("n");
    assert_eq!(got, 32);

    // A trailing cast makes it an expression around the aggregate: the same
    // value, not a NULL column.
    let got: i64 = sqlx::query(AssertSqlSafe("SELECT SUM(column_order + 1)::bigint AS n FROM utc"))
        .fetch_one(&mut conn)
        .await
        .expect("a cast around an expression argument on a single relation")
        .try_get("n")
        .expect("n");
    assert_eq!(got, 32);

    // Over a join too: the computed argument is projected as a column, and the
    // expression around the call is parsed again once its type is known. With
    // the placeholder type alone the slot came back a NULL column, which is
    // why this was refused; it must be the value.
    let got: i64 = sqlx::query(AssertSqlSafe(
        "SELECT c.tid AS tid, SUM(c.column_order + 1)::bigint AS n FROM utc c \
         JOIN ut t ON t.id = c.tid GROUP BY c.tid",
    ))
    .fetch_one(&mut conn)
    .await
    .expect("a cast around an expression argument over a join")
    .try_get("n")
    .expect("n");
    assert_eq!(got, 32);
}
