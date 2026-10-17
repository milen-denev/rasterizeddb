//! What a select list evaluates to, item by item, against PostgreSQL 17.
//!
//! A silent wrong answer lived here:
//!
//! 1. An expression in a join's select list returned its first operand.
//!    `a.n * 10 + b.n AS k` came back as `a.n`, labelled `*`: the item was
//!    split on whitespace, so the column before the operator was read as the
//!    item and the operator as its alias. `INSERT … SELECT` over a join stored
//!    those values and reported success.
//!
//! Every answer below is PostgreSQL 17's, labels and announced types included:
//! the description is what a client decodes by, so a right value under a wrong
//! type is still a wrong answer.

use rasterizeddb_compat::TestServer;
use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, Column, Executor, Row, TypeInfo};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

const SETUP: &[&str] = &[
    "CREATE TABLE digit (n INT NOT NULL, one INT NOT NULL)",
    "INSERT INTO digit (n, one) VALUES (0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1)",
    "CREATE TABLE pa (id INT NOT NULL, n INT, t TEXT, j JSONB)",
    "CREATE TABLE pb (id INT NOT NULL, pa_id INT, m INT, u TEXT)",
    r#"INSERT INTO pa (id, n, t, j) VALUES (1, 3, 'x', '{"k":1,"s":"a"}'), (2, NULL, NULL, NULL), (3, 7, 'z', '{"k":2}')"#,
    "INSERT INTO pb (id, pa_id, m, u) VALUES (10, 1, 5, 'p'), (11, 2, NULL, 'q'), (12, 9, 4, NULL)",
    "CREATE TABLE d (id INT NOT NULL, v TEXT)",
    "INSERT INTO d (id, v) VALUES (1, 'a'), (2, NULL)",
];

async fn setup(pool: &sqlx::PgPool) {
    for sql in SETUP {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(*sql)))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// One cell as psql prints it unaligned, NULL as `<N>`, decoded by the type
/// the row description announced.
fn cell(row: &PgRow, i: usize) -> String {
    let ty = row.column(i).type_info().name().to_string();
    let text = match ty.as_str() {
        "INT2" => row.try_get::<Option<i16>, _>(i).map(|v| v.map(|v| v.to_string())),
        "INT4" => row.try_get::<Option<i32>, _>(i).map(|v| v.map(|v| v.to_string())),
        "INT8" => row.try_get::<Option<i64>, _>(i).map(|v| v.map(|v| v.to_string())),
        "BOOL" => row
            .try_get::<Option<bool>, _>(i)
            .map(|v| v.map(|v| if v { "t" } else { "f" }.to_string())),
        "JSONB" | "JSON" => row
            .try_get::<Option<serde_json::Value>, _>(i)
            .map(|v| v.map(|v| v.to_string())),
        _ => row.try_get::<Option<String>, _>(i),
    };
    match text {
        Ok(Some(v)) => v,
        Ok(None) => "<N>".to_string(),
        Err(e) => format!("<decode {ty}: {e}>"),
    }
}

/// `label:TYPE|…` for the description, then one `v|v|…` line per row.
fn render(rows: &[PgRow]) -> (String, Vec<String>) {
    let header = rows
        .first()
        .map(|r| {
            r.columns()
                .iter()
                .map(|c| format!("{}:{}", c.name(), c.type_info().name()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .unwrap_or_default();
    let body = rows
        .iter()
        .map(|r| (0..r.columns().len()).map(|i| cell(r, i)).collect::<Vec<_>>().join("|"))
        .collect();
    (header, body)
}

/// Runs `sql` over the extended protocol (a prepared statement, binary
/// results) or the simple one (text results).
async fn run(pool: &sqlx::PgPool, sql: &str, simple: bool) -> Result<(String, Vec<String>), String> {
    let rows = if simple {
        sqlx::raw_sql(AssertSqlSafe(sql)).fetch_all(pool).await
    } else {
        sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await
    };
    rows.map(|rows| render(&rows))
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// Checks every case over both protocols and reports all the wrong ones.
async fn check(pool: &sqlx::PgPool, cases: &[(&str, &str, &[&str])]) {
    check_over(pool, cases, &[false, true]).await
}

/// [`check`] over the protocols named: `true` is the simple one.
async fn check_over(pool: &sqlx::PgPool, cases: &[(&str, &str, &[&str])], protocols: &[bool]) {
    let mut wrong = Vec::new();
    for (sql, header, rows) in cases {
        for &simple in protocols {
            let protocol = if simple { "simple" } else { "extended" };
            let want: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
            match run(pool, sql, simple).await {
                Ok((got_header, got_rows)) if got_header == *header && got_rows == want => {}
                Ok((got_header, got_rows)) => wrong.push(format!(
                    "{sql} [{protocol}]\n    want {header} {want:?}\n    got  {got_header} {got_rows:?}"
                )),
                Err(e) => wrong.push(format!("{sql} [{protocol}]\n    error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Bug 1: every expression kind a join's select list can hold, with
/// PostgreSQL's labels — `?column?` for an operator, the function's name for a
/// call, the column's for a cast of one.
#[tokio::test(flavor = "multi_thread")]
async fn join_select_list_expressions_are_evaluated() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    check(
        &pool,
        &[
            // The report's statement.
            (
                "SELECT a.n AS an, b.n AS bn, a.n * 10 + b.n AS k, a.n || 'x' AS s FROM digit a \
                 JOIN digit b ON b.one = a.one WHERE a.n = 3 AND b.n < 3 ORDER BY bn",
                "an:INT4|bn:INT4|k:INT4|s:TEXT",
                &["3|0|30|3x", "3|1|31|3x", "3|2|32|3x"],
            ),
            // Unaliased, it is `?column?`, and a NULL operand makes it NULL.
            (
                "SELECT a.id, a.n * 10 + b.m FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|?column?:INT4",
                &["1|35", "2|<N>"],
            ),
            (
                "SELECT a.id, a.t || b.u AS s, a.t || '-' || b.u FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|s:TEXT|?column?:TEXT",
                &["1|xp|x-p", "2|<N>|<N>"],
            ),
            // A CASE is labelled by its ELSE when that names something.
            (
                "SELECT a.id, CASE WHEN a.n > b.m THEN 'gt' ELSE 'le' END AS c, \
                 CASE WHEN b.m IS NULL THEN 0 ELSE b.m END FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|c:TEXT|m:INT4",
                &["1|le|5", "2|le|0"],
            ),
            (
                "SELECT a.id, upper(b.u), length(a.t) AS len FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|upper:TEXT|len:INT4",
                &["1|P|1", "2|Q|<N>"],
            ),
            (
                "SELECT a.id, a.n::text AS nt, CAST(b.m AS text) AS mt, b.m::bigint, CAST(a.n AS bigint) \
                 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|nt:TEXT|mt:TEXT|m:INT8|n:INT8",
                &["1|3|5|5|3", "2|<N>|<N>|<N>|<N>"],
            ),
            (
                "SELECT a.id, COALESCE(a.n, b.m, 0) AS c, COALESCE(a.t, 'none') FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|c:INT4|coalesce:TEXT",
                &["1|3|x", "2|0|none"],
            ),
            (
                "SELECT a.id, a.j->>'s' AS s, a.j->'k' AS k, a.j->>'k' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|s:TEXT|k:JSONB|?column?:TEXT",
                &["1|a|1|1", "2|<N>|<N>|<N>"],
            ),
            // A comparison is a boolean, and NULL when either side is.
            (
                "SELECT a.id, a.n > b.m AS gt, a.n = 3 AS eq3, a.n < b.m, a.n IS NULL AS isn, b.m IS NOT NULL \
                 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|gt:BOOL|eq3:BOOL|?column?:BOOL|isn:BOOL|?column?:BOOL",
                &["1|f|t|t|f|t", "2|<N>|<N>|<N>|t|f"],
            ),
            (
                "SELECT a.id, b.m - a.n AS d, b.m / 2 AS h, a.n % 2 AS r, 0 - a.n AS neg FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|d:INT4|h:INT4|r:INT4|neg:INT4",
                &["1|2|2|1|-3", "2|<N>|<N>|<N>|<N>"],
            ),
            // The NULL-extended side of each outer join.
            (
                "SELECT a.id, b.id AS bid, a.n + b.m AS s, a.t || b.u AS c FROM pa a LEFT JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|bid:INT4|s:INT4|c:TEXT",
                &["1|10|8|xp", "2|11|<N>|<N>", "3|<N>|<N>|<N>"],
            ),
            (
                "SELECT a.id, b.id AS bid, COALESCE(a.n, -1) + b.m AS s FROM pa a RIGHT JOIN pb b ON b.pa_id = a.id ORDER BY b.id",
                "id:INT4|bid:INT4|s:INT4",
                &["1|10|8", "2|11|<N>", "<N>|12|3"],
            ),
            (
                "SELECT a.id, b.id AS bid, a.t || b.u AS s, a.n + b.m AS p FROM pa a FULL JOIN pb b ON b.pa_id = a.id ORDER BY a.id, b.id",
                "id:INT4|bid:INT4|s:TEXT|p:INT4",
                &["1|10|xp|8", "2|11|<N>|<N>", "3|<N>|<N>|<N>", "<N>|12|<N>|<N>"],
            ),
            // Three relations.
            (
                "SELECT a.id, a.n + b.m + c.n AS s, c.t || b.u AS cu FROM pa a JOIN pb b ON b.pa_id = a.id \
                 JOIN pa c ON c.id = a.id ORDER BY a.id",
                "id:INT4|s:INT4|cu:TEXT",
                &["1|11|xp", "2|<N>|<N>"],
            ),
            // An expression first, ahead of the relation's own columns, and
            // ordered by its alias.
            (
                "SELECT a.n * 10 + b.n AS k, a.one FROM digit a JOIN digit b ON b.one = a.one \
                 WHERE a.n = 1 AND b.n < 2 ORDER BY k",
                "k:INT4|one:INT4",
                &["10|1", "11|1"],
            ),
            (
                "SELECT a.id, a.id + 2 AS three, 'lit' AS l, NULL AS nl, NULL, NULL::int AS ni \
                 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|three:INT4|l:TEXT|nl:TEXT|?column?:TEXT|ni:INT4",
                &["1|3|lit|<N>|<N>|<N>", "2|4|lit|<N>|<N>|<N>"],
            ),
            // Ordered by position; a NULL sorts first descending.
            (
                "SELECT a.id, a.n * 2 AS dbl FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY 2 DESC",
                "id:INT4|dbl:INT4",
                &["2|<N>", "1|6"],
            ),
            // Unaliased items, named as PostgreSQL names them: a cast after
            // what it casts or else its type, a call after the function, a
            // CASE after a named ELSE or else `case`, parentheses ignored.
            (
                "SELECT a.n::text, CAST(b.m AS text), 'x'::text, NULL::int, upper(b.u), COALESCE(a.n, 0), \
                 NULLIF(a.n, 3), (a.n), (a.n + 1), CASE WHEN a.n > 1 THEN 1 ELSE 0 END, \
                 CASE WHEN a.n > 1 THEN a.n ELSE b.m END, a.n + 1::int, a.j->>'s', a.n IS NULL, 'lit', NULL, \
                 a.n::bigint::text, (a.n + 1)::text FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "n:TEXT|m:TEXT|text:TEXT|int4:INT4|upper:TEXT|coalesce:INT4|nullif:INT4|n:INT4|?column?:INT4|\
                 case:INT4|m:INT4|?column?:INT4|?column?:TEXT|?column?:BOOL|?column?:TEXT|?column?:TEXT|n:TEXT|text:TEXT",
                &[
                    "3|5|x|<N>|P|3|<N>|3|4|1|3|4|a|f|lit|<N>|3|4",
                    "<N>|<N>|x|<N>|Q|0|<N>|<N>|<N>|0|<N>|<N>|<N>|t|lit|<N>|<N>|<N>",
                ],
            ),
            // A nullary function is a column only when it is the whole item.
            (
                "SELECT a.id, now() IS NOT NULL AS has_now, now() IS NULL FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
                "id:INT4|has_now:BOOL|?column?:BOOL",
                &["1|t|f", "2|t|f"],
            ),
        ],
    )
    .await;
}

/// A comparison is a boolean in a single table's select list too, where it was
/// refused as a missing column.
#[tokio::test(flavor = "multi_thread")]
async fn a_comparison_in_a_select_list_is_a_boolean() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    check(
        &pool,
        &[(
            "SELECT id, n > 1 AS big, n = 3, t = 'x' AS isx, 'a' = 'b' AS ab FROM pa ORDER BY id",
            "id:INT4|big:BOOL|?column?:BOOL|isx:BOOL|ab:BOOL",
            &["1|t|t|t|f", "2|<N>|<N>|<N>|f", "3|t|f|f|f"],
        )],
    )
    .await;
}

/// Bug 1, through `INSERT … SELECT`: the stored values are the computed ones.
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_over_a_join_stores_the_computed_values() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    for sql in [
        "CREATE TABLE sink (k INT, s TEXT)",
        "INSERT INTO sink (k, s) SELECT a.n * 10 + b.n, a.n || 'x' FROM digit a JOIN digit b ON b.one = a.one",
        "CREATE TABLE sink2 (id INT, k INT, s TEXT)",
        "INSERT INTO sink2 (id, k, s) SELECT a.id, a.n * 10 + b.m, a.t || b.u FROM pa a JOIN pb b ON b.pa_id = a.id",
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    check(
        &pool,
        &[
            (
                "SELECT count(*), min(k), max(k), sum(k) FROM sink",
                "count:INT8|min:INT4|max:INT4|sum:INT8",
                &["100|0|99|4950"],
            ),
            ("SELECT k, s FROM sink WHERE k = 37", "k:INT4|s:TEXT", &["37|3x"]),
            (
                "SELECT id, k, s FROM sink2 ORDER BY id",
                "id:INT4|k:INT4|s:TEXT",
                &["1|35|xp", "2|<N>|<N>"],
            ),
        ],
    )
    .await;
}
