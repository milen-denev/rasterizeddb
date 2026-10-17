//! What a select list evaluates to, item by item, against PostgreSQL 17.
//!
//! Three silent wrong answers lived here:
//!
//! 1. An expression in a join's select list returned its first operand.
//!    `a.n * 10 + b.n AS k` came back as `a.n`, labelled `*`: the item was
//!    split on whitespace, so the column before the operator was read as the
//!    item and the operator as its alias. `INSERT … SELECT` over a join stored
//!    those values and reported success.
//! 2. A `NULL` written in a select list came back as an empty string. The
//!    constant was stored as empty text, which is a value, so the client saw
//!    `''` where PostgreSQL sends NULL. A scalar subquery that found no row,
//!    or found a NULL, is substituted as the same constant.
//! 3. The correlated `UPDATE … FROM` path compared a subquery's document with
//!    its constant as text, so `{"a": 1.0}` did not equal `{"a": 1}`, and a
//!    NULL result compared as the word `NULL`.
//!
//! And one more of the first kind, found in review:
//!
//! 4. An operator spelled as a word — `a.n BETWEEN 2 AND 4`, `s LIKE 'x%'`,
//!    `flag IS TRUE` — was its first operand, aliased by the keyword.
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
    "CREATE TABLE fl (id INT NOT NULL, flag BOOL)",
    "INSERT INTO fl (id, flag) VALUES (1, true), (2, false), (3, NULL)",
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

/// Bug 2: a NULL in a select list is a NULL, however it is written and
/// whichever path answers the statement. A bare NULL is described as `text`.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_in_a_select_list_is_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    check(
        &pool,
        &[
            ("SELECT id, NULL FROM d ORDER BY id", "id:INT4|?column?:TEXT", &["1|<N>", "2|<N>"]),
            (
                "SELECT id, NULL AS z, NULL::int AS ni, CAST(NULL AS text) AS nt FROM d ORDER BY id",
                "id:INT4|z:TEXT|ni:INT4|nt:TEXT",
                &["1|<N>|<N>|<N>", "2|<N>|<N>|<N>"],
            ),
            // Over a join.
            (
                "SELECT a.id, NULL AS z, NULL FROM d a JOIN d b ON b.id = a.id ORDER BY a.id",
                "id:INT4|z:TEXT|?column?:TEXT",
                &["1|<N>|<N>", "2|<N>|<N>"],
            ),
            // Through a CTE, written inside it and outside it.
            (
                "WITH c AS (SELECT id, NULL AS z FROM d) SELECT id, z FROM c ORDER BY id",
                "id:INT4|z:TEXT",
                &["1|<N>", "2|<N>"],
            ),
            (
                "WITH c AS (SELECT id FROM d) SELECT id, NULL AS z FROM c ORDER BY id",
                "id:INT4|z:TEXT",
                &["1|<N>", "2|<N>"],
            ),
            // A column whose first row is NULL types the CTE by a later one.
            (
                "WITH c AS (SELECT id, v FROM d ORDER BY id DESC) SELECT id, v FROM c ORDER BY id",
                "id:INT4|v:TEXT",
                &["1|a", "2|<N>"],
            ),
            // Each branch of a UNION, and a NULL meeting a NULL column.
            (
                "SELECT id, NULL AS z FROM d WHERE id = 1 UNION ALL SELECT id, NULL FROM d WHERE id = 2 ORDER BY id",
                "id:INT4|z:TEXT",
                &["1|<N>", "2|<N>"],
            ),
            (
                "SELECT id, NULL AS z FROM d WHERE id = 1 UNION ALL SELECT id, v FROM d WHERE id = 2 ORDER BY id",
                "id:INT4|z:TEXT",
                &["1|<N>", "2|<N>"],
            ),
            // Grouped, and de-duplicated.
            ("SELECT NULL AS z, count(*) AS c FROM d", "z:TEXT|c:INT8", &["<N>|2"]),
            ("SELECT DISTINCT NULL AS z FROM d", "z:TEXT", &["<N>"]),
            // An empty string is still a value, not a NULL.
            ("SELECT id, '' AS e FROM d ORDER BY id", "id:INT4|e:TEXT", &["1|", "2|"]),
        ],
    )
    .await;
    // A scalar subquery with no row, and one whose row holds NULL. Simple
    // protocol only: over the extended one a select-list scalar subquery is
    // described with no columns at all, whatever it returns — a separate
    // fault in the describe path, not this one.
    check_over(
        &pool,
        &[(
            "SELECT id, (SELECT v FROM d WHERE id = 99) AS sq, (SELECT v FROM d WHERE id = 2) AS sn FROM d ORDER BY id",
            "id:INT4|sq:TEXT|sn:TEXT",
            &["1|<N>|<N>", "2|<N>|<N>"],
        )],
        &[true],
    )
    .await;
}

/// Bug 3: the correlated `UPDATE … FROM` term `(SELECT doc …) <op> '<json>'`
/// compares documents by value, as `jsonb` does, and a NULL result matches
/// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn correlated_update_from_compares_documents_by_value() {
    let (_s, pool) = server().await;
    for sql in [
        "CREATE TABLE ut (id INT PRIMARY KEY, k INT NOT NULL, flag INT NOT NULL)",
        "CREATE TABLE us (id INT NOT NULL, k INT NOT NULL)",
        "CREATE TABLE ue (id INT NOT NULL, doc JSONB)",
        "INSERT INTO ut (id, k, flag) VALUES (1, 1, 0), (2, 2, 0), (3, 3, 0), (4, 4, 0), (5, 5, 0)",
        "INSERT INTO us (id, k) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
        r#"INSERT INTO ue (id, doc) VALUES (1, '{"a":1}'), (2, '{"a":1.0}'), (3, '{"b":2,"a":1}'), (4, NULL), (5, '[1, 2]')"#,
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let mut wrong = Vec::new();
    for (condition, want_count, want_ids) in [
        (r#"= '{"a": 1}'"#, 2u64, "1,2"),
        (r#"<> '{"a":1}'"#, 2, "3,5"),
        (r#"> '{"a":1}'"#, 1, "3"),
        ("= '[1.0, 2]'", 1, "5"),
        (r#"= '{"a":1}'::jsonb"#, 2, "1,2"),
    ] {
        pool.execute("UPDATE ut SET flag = 0").await.expect("reset");
        let sql = format!(
            "UPDATE ut SET flag = 1 FROM us WHERE us.k = ut.k AND \
             (SELECT doc FROM ue WHERE ue.id = ut.id) {condition}"
        );
        let count = match pool.execute(AssertSqlSafe(sql.as_str())).await {
            Ok(done) => done.rows_affected(),
            Err(e) => {
                wrong.push(format!("{sql}\n    error {e}"));
                continue;
            }
        };
        let ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM ut WHERE flag = 1 ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("read back");
        let ids = ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        if count != want_count || ids != want_ids {
            wrong.push(format!(
                "{sql}\n    want UPDATE {want_count} [{want_ids}]\n    got  UPDATE {count} [{ids}]"
            ));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An operator spelled as a word — `BETWEEN`, `LIKE`, `AND`, `IS TRUE` — over
/// a join and over one table.
///
/// The alias split read the first word as the item and the keyword as its
/// alias and dropped the rest, so `a.n BETWEEN 2 AND 4` came back as the
/// integer `a.n` in a column labelled `BETWEEN`, and `flag IS TRUE` as `flag`.
/// The `IS` tests are evaluated. The rest are refused for now: the condition
/// grammar has no `LIKE`, `BETWEEN` or three-valued `NOT`, and a refusal is
/// honest where the first operand is not. Each may answer only as PostgreSQL
/// does, so the day one of them is evaluated it is held to PostgreSQL's value.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyword_operator_in_a_select_list_is_never_its_first_operand() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    check(
        &pool,
        &[
            (
                "SELECT a.id, a.flag IS TRUE FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
                "id:INT4|?column?:BOOL",
                &["1|t", "2|f", "3|f"],
            ),
            (
                "SELECT a.id, a.flag IS NOT FALSE AS nf FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
                "id:INT4|nf:BOOL",
                &["1|t", "2|f", "3|t"],
            ),
            (
                "SELECT id, flag IS TRUE FROM fl ORDER BY id",
                "id:INT4|?column?:BOOL",
                &["1|t", "2|f", "3|f"],
            ),
            (
                "SELECT id, flag IS NOT TRUE AS nt FROM fl ORDER BY id",
                "id:INT4|nt:BOOL",
                &["1|f", "2|t", "3|t"],
            ),
            // A bare alias still names the column, and the window value.
            (
                "SELECT a.id, a.flag x FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
                "id:INT4|x:BOOL",
                &["1|t", "2|f", "3|<N>"],
            ),
            (
                "SELECT id, rank() OVER (ORDER BY id) r FROM pa ORDER BY id",
                "id:INT4|r:INT8",
                &["1|1", "2|2", "3|3"],
            ),
        ],
    )
    .await;

    let refusable: &[(&str, &str, &[&str])] = &[
        (
            "SELECT a.id, a.flag AND b.flag FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|f", "3|<N>"],
        ),
        (
            "SELECT a.id, a.flag OR b.flag FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|f", "3|<N>"],
        ),
        (
            "SELECT a.id, NOT a.flag FROM fl a JOIN fl b ON b.id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|f", "2|t", "3|<N>"],
        ),
        (
            "SELECT a.id, a.t LIKE 'x%' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.t ILIKE 'X%' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.t NOT LIKE 'x%' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|f", "2|<N>"],
        ),
        (
            "SELECT a.id, a.t SIMILAR TO 'x' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.n BETWEEN 2 AND 4 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.n NOT BETWEEN 2 AND 4 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|f", "2|<N>"],
        ),
        (
            "SELECT a.id, a.n BETWEEN 2 AND 4 AS bt FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|bt:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.n IN (3, 4) FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, a.n IS DISTINCT FROM b.m FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|f"],
        ),
        (
            "SELECT a.id, a.n ISNULL FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|f", "2|t"],
        ),
        (
            "SELECT a.id, upper(a.t) LIKE 'X%' FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, 'x' LIKE a.t FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        (
            "SELECT a.id, (a.n)::int BETWEEN 2 AND 4 FROM pa a JOIN pb b ON b.pa_id = a.id ORDER BY a.id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>"],
        ),
        // One table.
        ("SELECT id, t LIKE 'x%' FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|<N>", "3|f"]),
        ("SELECT id, t ILIKE 'X%' AS il FROM pa ORDER BY id", "id:INT4|il:BOOL", &["1|t", "2|<N>", "3|f"]),
        ("SELECT id, n BETWEEN 2 AND 4 FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|<N>", "3|f"]),
        ("SELECT id, n IN (3, 4) FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|<N>", "3|f"]),
        ("SELECT id, n IS DISTINCT FROM 3 FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|f", "2|t", "3|t"]),
        ("SELECT id, n ISNULL FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|f", "2|t", "3|f"]),
        ("SELECT id, upper(t) LIKE 'X%' FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|<N>", "3|f"]),
        ("SELECT id, 'x' LIKE t FROM pa ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|<N>", "3|f"]),
        (
            "SELECT id, (n)::int BETWEEN 2 AND 4 FROM pa ORDER BY id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|<N>", "3|f"],
        ),
        ("SELECT id, flag AND true FROM fl ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|f", "3|<N>"]),
        ("SELECT id, flag OR false FROM fl ORDER BY id", "id:INT4|?column?:BOOL", &["1|t", "2|f", "3|<N>"]),
        ("SELECT id, NOT flag FROM fl ORDER BY id", "id:INT4|?column?:BOOL", &["1|f", "2|t", "3|<N>"]),
        // After a window call, whose own splitter read the operator as the
        // alias the same way.
        (
            "SELECT id, rank() OVER (ORDER BY id) BETWEEN 1 AND 2 FROM pa ORDER BY id",
            "id:INT4|?column?:BOOL",
            &["1|t", "2|t", "3|f"],
        ),
        (
            "SELECT id, rank() OVER (ORDER BY id) + 1 AS r FROM pa ORDER BY id",
            "id:INT4|r:INT8",
            &["1|2", "2|3", "3|4"],
        ),
    ];
    let mut wrong = Vec::new();
    for (sql, header, rows) in refusable {
        for simple in [false, true] {
            let want: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
            if let Ok((got_header, got_rows)) = run(&pool, sql, simple).await {
                if got_header != *header || got_rows != want {
                    wrong.push(format!(
                        "{sql} [simple={simple}]\n    want {header} {want:?} or a refusal\n    got  {got_header} {got_rows:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));

    // `INSERT … SELECT` over a join stores PostgreSQL's booleans or nothing.
    // A text column takes whatever arrives, which is how the operand got in;
    // PostgreSQL stores `true` and `false` there.
    for sql in ["CREATE TABLE bsink (id INT, b BOOL)", "CREATE TABLE tsink (id INT, b TEXT)"] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for (sink, want) in [
        ("bsink", ["0|f", "1|f", "2|t", "3|t", "4|t", "5|f"]),
        ("tsink", ["0|false", "1|false", "2|true", "3|true", "4|true", "5|false"]),
    ] {
        let insert = format!(
            "INSERT INTO {sink} (id, b) SELECT a.n, a.n BETWEEN 2 AND 4 FROM digit a \
             JOIN digit b ON b.one = a.one WHERE b.n = 0 AND a.n < 6"
        );
        let stored = pool.execute(sqlx::raw_sql(AssertSqlSafe(insert.as_str()))).await;
        let (_, rows) = run(&pool, &format!("SELECT id, b FROM {sink} ORDER BY id"), true)
            .await
            .expect("read back");
        match stored {
            Ok(_) => assert_eq!(rows, want, "{insert}"),
            Err(_) => assert!(rows.is_empty(), "{insert} failed but stored {rows:?}"),
        }
    }
}
