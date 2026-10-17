//! `UNION` and `UNION ALL` answered as PostgreSQL 17 answers them.
//!
//! Every expected answer below was produced by running the same statement
//! over the same rows on a real PostgreSQL 17.
//!
//! `UNION` removed duplicates by comparing the serialized rows, and every
//! cell there carries the position of the column it was read from, so `open`
//! read from `events.tag` (the third column) and `open` read from `t.tag` (the
//! second) were two rows. It also compared a cell's type tag and width, so an
//! `int` 1 and a `bigint` 1 were two rows where PostgreSQL unifies the column
//! to `bigint` and keeps one; and the `bigint` values, sent under the first
//! branch's `int` description, arrived as NULL. A trailing `ORDER BY` after
//! the last branch was never applied to the combined rows: it was left on the
//! last branch, which sorted that branch alone, and a `LIMIT` behind it was
//! then not recognised either. And a parenthesised branch was taken for a
//! scalar subquery and refused.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

/// The same values sit at different column positions in each table, and
/// `t.tag` is a `VARCHAR` where `events.tag` is `TEXT`. `events.n` is `INT`
/// and `t.n` is `BIGINT`. Each table has a NULL tag and a NULL `n`.
const SETUP: &[&str] = &[
    "CREATE TABLE events (id BIGINT PRIMARY KEY, doc JSONB, tag TEXT, n INT)",
    "CREATE TABLE t (id BIGINT PRIMARY KEY, tag VARCHAR(10), n BIGINT)",
    "CREATE TABLE u (tag TEXT, id BIGINT PRIMARY KEY)",
    r#"INSERT INTO events VALUES (1,'{"a":1}','open',1),(2,'{"a":2}','paid',2),(3,'[1,2]','shut',3),(4,'{"a":1.0}','open',NULL),(5,NULL,NULL,5)"#,
    "INSERT INTO t VALUES (1,'paid',2),(2,'void',7),(3,'open',1),(4,NULL,NULL),(5,'gone',5)",
    "INSERT INTO u VALUES ('open',1),('zeta',2),(NULL,3),('alpha',4)",
    "CREATE TABLE w (id BIGINT PRIMARY KEY, n BIGINT)",
    "INSERT INTO w VALUES (1, 5000000000), (2, -3)",
];

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in SETUP {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, pool)
}

/// Rows joined by `,`, cells by `:`, NULL as `NULL`, as the simple protocol
/// sent them; sorted first when the statement leaves the order open.
async fn flat(pool: &sqlx::PgPool, sql: &str, sort: bool) -> Result<String, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(|e| e.to_string())?;
    let mut rows: Vec<String> = rows
        .iter()
        .map(|r| {
            (0..r.len())
                .map(|i| {
                    r.try_get_unchecked::<Option<String>, _>(i)
                        .unwrap()
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect::<Vec<_>>()
                .join(":")
        })
        .collect();
    if sort {
        rows.sort();
    }
    Ok(rows.join(","))
}

/// PostgreSQL's answer, sorted the way `flat` sorts, for a statement whose
/// order is not defined.
fn sorted(want: &str) -> String {
    let mut rows: Vec<&str> = want.split(',').filter(|r| !r.is_empty()).collect();
    rows.sort();
    rows.join(",")
}

/// `(statement, PostgreSQL 17's rows in any order)`.
const UNORDERED: &[(&str, &str)] = &[
    // Equal values from columns at different positions are one row.
    ("SELECT tag FROM events UNION SELECT tag FROM t", "NULL,open,shut,void,paid,gone"),
    (
        "SELECT tag FROM events UNION ALL SELECT tag FROM t",
        "open,paid,shut,open,NULL,paid,void,open,NULL,gone",
    ),
    (
        "SELECT tag FROM events UNION SELECT tag FROM t UNION SELECT tag FROM u",
        "NULL,shut,paid,alpha,open,zeta,void,gone",
    ),
    // VARCHAR against TEXT, the column first in one table and second in the
    // other.
    ("SELECT tag FROM t UNION SELECT tag FROM u", "NULL,paid,alpha,open,zeta,void,gone"),
    // `int` and `bigint` are one `bigint` column.
    ("SELECT n FROM events UNION SELECT n FROM t", "NULL,2,3,5,7,1"),
    // Whole rows: two columns must both agree, NULLs equal to each other.
    (
        "SELECT id, tag FROM events UNION SELECT id, tag FROM t",
        "1:open,4:open,1:paid,2:paid,3:shut,5:gone,5:NULL,3:open,2:void,4:NULL",
    ),
    (
        "SELECT tag, id FROM u UNION SELECT tag, id FROM events",
        "NULL:5,zeta:2,paid:2,shut:3,NULL:3,open:4,open:1,alpha:4",
    ),
    // Set operators group from the left: the first `UNION` removes
    // duplicates of what it joins, and a `UNION ALL` after it keeps the
    // third branch's rows whole; a `UNION` after a `UNION ALL` removes
    // duplicates from everything before it.
    (
        "SELECT tag FROM events UNION SELECT tag FROM t UNION ALL SELECT tag FROM u",
        "NULL,open,shut,void,paid,gone,open,zeta,NULL,alpha",
    ),
    (
        "SELECT tag FROM events UNION ALL SELECT tag FROM t UNION SELECT tag FROM u",
        "NULL,shut,paid,alpha,open,zeta,void,gone",
    ),
    // A branch's own ORDER BY and LIMIT, inside its parentheses, pick that
    // branch's rows. UNION ALL keeps the branches in order.
    (
        "(SELECT tag FROM events WHERE tag IS NOT NULL ORDER BY tag DESC LIMIT 2) UNION ALL (SELECT tag FROM t WHERE tag IS NOT NULL ORDER BY tag LIMIT 1)",
        "shut,paid,gone",
    ),
];

/// `(statement, PostgreSQL 17's rows in its order)`: a trailing `ORDER BY`,
/// `LIMIT` and `OFFSET` apply to the combined rows.
const ORDERED: &[(&str, &str)] = &[
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY 1", "gone,open,paid,shut,void,NULL"),
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY 1 LIMIT 2", "gone,open"),
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY tag DESC", "NULL,void,shut,paid,open,gone"),
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY tag DESC LIMIT 2 OFFSET 1", "void,shut"),
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY tag NULLS FIRST LIMIT 3", "NULL,gone,open"),
    ("SELECT tag FROM events UNION SELECT tag FROM t ORDER BY 1 OFFSET 2", "paid,shut,void,NULL"),
    ("SELECT n FROM events UNION SELECT n FROM t ORDER BY n DESC", "NULL,7,5,3,2,1"),
    ("SELECT n FROM events UNION ALL SELECT n FROM t ORDER BY 1 LIMIT 4", "1,1,2,2"),
    (
        "SELECT id, tag FROM events UNION SELECT id, tag FROM t ORDER BY 2, 1",
        "5:gone,1:open,3:open,4:open,1:paid,2:paid,3:shut,2:void,4:NULL,5:NULL",
    ),
    (
        "SELECT id, tag FROM events UNION SELECT id, tag FROM t ORDER BY tag DESC, id DESC LIMIT 3",
        "5:NULL,4:NULL,2:void",
    ),
    // The output columns are named by the first branch.
    (
        "SELECT id AS k, tag FROM events UNION SELECT id, tag FROM t ORDER BY k DESC, tag LIMIT 4",
        "5:gone,5:NULL,4:open,4:NULL",
    ),
    ("SELECT tag FROM events UNION ALL SELECT tag FROM t ORDER BY tag LIMIT 3", "gone,open,open"),
    (
        "(SELECT tag FROM events WHERE tag IS NOT NULL ORDER BY tag DESC LIMIT 2) UNION (SELECT tag FROM t WHERE tag IS NOT NULL ORDER BY tag LIMIT 1) ORDER BY 1",
        "gone,paid,shut",
    ),
    (
        "SELECT tag FROM events UNION SELECT tag FROM t UNION SELECT tag FROM u ORDER BY 1 DESC LIMIT 3",
        "NULL,zeta,void",
    ),
    (
        "SELECT n FROM events WHERE n > 1 UNION SELECT n FROM t WHERE n > 1 ORDER BY 1 LIMIT 10 OFFSET 1",
        "3,5,7",
    ),
    // Documents are compared by value, `{"a":1}` and `{"a":1.0}` one row, and
    // sorted in jsonb's order.
    ("SELECT doc FROM events UNION SELECT doc FROM events ORDER BY 1", r#"[1, 2],{"a": 1},{"a": 2},NULL"#),
    // The `int` column is a `bigint` whichever branch has the rows: the values
    // of the wider branch were sent under the first branch's `int`
    // description, did not fit it, and arrived as NULL.
    ("SELECT n FROM events WHERE n > 100 UNION SELECT n FROM t ORDER BY 1", "1,2,5,7,NULL"),
    ("SELECT n FROM events UNION SELECT n FROM t WHERE n > 100 ORDER BY 1", "1,2,3,5,NULL"),
    ("SELECT n FROM events UNION SELECT n FROM w ORDER BY 1", "-3,1,2,3,5,5000000000,NULL"),
    (
        "SELECT id, n FROM w UNION ALL SELECT id, n FROM events ORDER BY n DESC, id",
        "4:NULL,1:5000000000,5:5,3:3,2:2,1:1,2:-3",
    ),
];

#[tokio::test(flavor = "multi_thread")]
async fn union_deduplicates_by_value_whatever_the_source_column() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (sql, want) in UNORDERED {
        let want = sorted(want);
        let got = flat(&pool, sql, true).await;
        if got.as_deref() != Ok(want.as_str()) {
            wrong.push(format!("{sql}\n    want {want}\n    got  {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), UNORDERED.len(), wrong.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trailing_order_by_and_limit_apply_to_the_whole_union() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (sql, want) in ORDERED {
        let got = flat(&pool, sql, false).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{sql}\n    want {want}\n    got  {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), ORDERED.len(), wrong.join("\n"));
}

/// Over the extended protocol the values are sent in binary under the
/// description, so the widened column has to be described as the `bigint`
/// it is sent as: PostgreSQL 17 describes it as `int8`, and a driver reading
/// an `int4` description refuses an `i64`.
#[tokio::test(flavor = "multi_thread")]
async fn a_widened_column_is_described_as_its_wider_type() {
    let (_s, pool) = server().await;
    for (sql, want) in [
        (
            "SELECT n FROM events UNION SELECT n FROM w ORDER BY 1",
            vec![Some(-3i64), Some(1), Some(2), Some(3), Some(5), Some(5_000_000_000), None],
        ),
        (
            "(SELECT n FROM events WHERE n IS NOT NULL ORDER BY n DESC LIMIT 1) UNION ALL (SELECT n FROM w ORDER BY n LIMIT 1)",
            vec![Some(5), Some(-3)],
        ),
    ] {
        let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got: Vec<Option<i64>> = rows
            .iter()
            .map(|r| r.try_get::<Option<i64>, _>(0).unwrap_or_else(|e| panic!("{sql}: {e}")))
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}
