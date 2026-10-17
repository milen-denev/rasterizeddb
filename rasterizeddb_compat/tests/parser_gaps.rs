//! Statements PostgreSQL accepts that the parser used to refuse: `DELETE`
//! with no `WHERE`, `CASE` branches holding arbitrary expressions, `IS [NOT]
//! DISTINCT FROM` against a boolean literal, and row-value equality.
//!
//! Every expected answer was taken from PostgreSQL 17 running the same
//! statements over the same rows, and is written out here rather than
//! recomputed, so a test cannot agree with a wrong engine by construction.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

fn first_line(e: sqlx::Error) -> String {
    e.to_string().lines().next().unwrap_or("").to_string()
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<u64, String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map(|r| r.rows_affected())
        .map_err(first_line)
}

/// The first column of every row, as the text PostgreSQL would print, with
/// `~` for NULL. Read over the simple protocol, so every value arrives as
/// text whatever its type.
async fn column(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(first_line)?;
    Ok(rows
        .iter()
        .map(|r| {
            r.try_get_unchecked::<Option<String>, _>(0)
                .expect("text")
                .unwrap_or_else(|| "~".to_string())
        })
        .collect())
}

async fn joined(pool: &sqlx::PgPool, sql: &str) -> Result<String, String> {
    column(pool, sql).await.map(|v| v.join(","))
}

/// Runs each `(sql, expected)` and returns one line per wrong answer.
async fn wrong_answers(pool: &sqlx::PgPool, cases: &[(&str, &str)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        match joined(pool, sql).await {
            Ok(got) if got == *want => {}
            Ok(got) => wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}")),
            Err(e) => wrong.push(format!("{sql}\n    error {e}\n    want  {want:?}")),
        }
    }
    wrong
}

// ---------------------------------------------------------------- #10 DELETE

/// `DELETE FROM t` with no `WHERE` was "Cannot parse DELETE query". It deletes
/// every row, reports the count in its command tag, rolls back with its
/// transaction, and takes `RETURNING` and an alias like any other `DELETE`.
#[tokio::test(flavor = "multi_thread")]
async fn delete_without_where_removes_every_row() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE dd (id INT NOT NULL, v TEXT)").await.unwrap();
    let fill = "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b'),(3,NULL),(4,'d'),(5,'e')";
    run(&pool, fill).await.unwrap();

    // Inside a transaction that rolls back: gone inside, all back after.
    let mut tx = pool.begin().await.unwrap();
    let deleted = sqlx::query("DELETE FROM dd").execute(&mut *tx).await.expect("DELETE FROM dd");
    assert_eq!(deleted.rows_affected(), 5, "PostgreSQL's tag is DELETE 5");
    let inside: i64 = sqlx::query("SELECT count(*) FROM dd")
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        .get(0);
    assert_eq!(inside, 0);
    tx.rollback().await.unwrap();
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "5");

    // RETURNING reports every row as it was.
    let mut returned: Vec<(i32, Option<String>)> =
        sqlx::query("DELETE FROM dd RETURNING id, v")
            .fetch_all(&pool)
            .await
            .expect("DELETE … RETURNING")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    returned.sort();
    assert_eq!(
        returned,
        vec![
            (1, Some("a".into())),
            (2, Some("b".into())),
            (3, None),
            (4, Some("d".into())),
            (5, Some("e".into())),
        ]
    );
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "0");

    // An empty table: DELETE 0, not an error.
    assert_eq!(run(&pool, "DELETE FROM dd").await, Ok(0));

    // Lower case, a quoted name and an alias, as PostgreSQL accepts them.
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b')").await.unwrap();
    assert_eq!(run(&pool, "delete from \"dd\" as x").await, Ok(2));
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b'),(3,'c')").await.unwrap();
    assert_eq!(run(&pool, "DELETE FROM dd x").await, Ok(3));
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a')").await.unwrap();
    assert_eq!(run(&pool, "DELETE FROM dd;").await, Ok(1));

    // Text after the table that is neither an alias nor a clause is refused,
    // never read as "no condition": a statement the parser misread must not
    // become one that empties the table.
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b')").await.unwrap();
    assert!(run(&pool, "DELETE FROM dd x y").await.is_err());
    assert!(run(&pool, "DELETE FROM dd WHERE").await.is_err());
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "2");
}

/// A whole-table delete of a parent runs each child's `ON DELETE` action for
/// every row it removes, and a blocking action refuses the whole statement.
#[tokio::test(flavor = "multi_thread")]
async fn delete_without_where_follows_foreign_keys() {
    let (_s, pool) = server().await;
    for ddl in [
        "CREATE TABLE par (id INT PRIMARY KEY)",
        "CREATE TABLE kid_c (id INT, pid INT REFERENCES par(id) ON DELETE CASCADE)",
        "CREATE TABLE kid_n (id INT, pid INT REFERENCES par(id) ON DELETE SET NULL)",
        "INSERT INTO par (id) VALUES (1),(2),(3)",
        "INSERT INTO kid_c (id, pid) VALUES (10,1),(11,2),(12,NULL)",
        "INSERT INTO kid_n (id, pid) VALUES (20,1),(21,3)",
    ] {
        run(&pool, ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    assert_eq!(run(&pool, "DELETE FROM par").await, Ok(3));
    assert_eq!(joined(&pool, "SELECT count(*) FROM par").await.unwrap(), "0");
    // The child row with no parent stays; the two that had one cascade.
    assert_eq!(joined(&pool, "SELECT id FROM kid_c ORDER BY id").await.unwrap(), "12");
    assert_eq!(
        joined(&pool, "SELECT pid FROM kid_n ORDER BY id").await.unwrap(),
        "~,~"
    );

    for ddl in [
        "CREATE TABLE par2 (id INT PRIMARY KEY)",
        "CREATE TABLE kid_r (id INT, pid INT REFERENCES par2(id) ON DELETE RESTRICT)",
        "INSERT INTO par2 (id) VALUES (1),(2)",
        "INSERT INTO kid_r (id, pid) VALUES (30, 2)",
    ] {
        run(&pool, ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    // PostgreSQL: 23503, and the unreferenced row 1 is not deleted either.
    let err = sqlx::query("DELETE FROM par2").execute(&pool).await.expect_err("RESTRICT");
    let code = err.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
    assert_eq!(code.as_deref(), Some("23503"), "{err}");
    assert_eq!(joined(&pool, "SELECT count(*) FROM par2").await.unwrap(), "2");
    assert_eq!(run(&pool, "DELETE FROM kid_r").await, Ok(1));
    assert_eq!(run(&pool, "DELETE FROM par2").await, Ok(2));
}

// -------------------------------------------- #13 IS DISTINCT FROM, row values

async fn flags(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE fl (id INT NOT NULL, is_flagged BOOLEAN, other BOOLEAN, nn BOOLEAN NOT NULL, country TEXT, grade TEXT, k INT)",
    )
    .await
    .unwrap();
    run(
        pool,
        "INSERT INTO fl (id, is_flagged, other, nn, country, grade, k) VALUES \
         (1, true, true, true, 'DE', 'A', 1), (2, false, NULL, false, 'DE', 'B', -1), \
         (3, NULL, NULL, true, 'FR', 'A', NULL), (4, NULL, false, false, NULL, 'A', 4), \
         (5, false, false, true, 'DE', NULL, -1)",
    )
    .await
    .unwrap();
}

/// `IS [NOT] DISTINCT FROM` a boolean literal was "unknown identifier
/// 'false'": the literal reached the tokenizer unconverted.
#[tokio::test(flavor = "multi_thread")]
async fn is_distinct_from_literals_and_columns() {
    let (_s, pool) = server().await;
    flags(&pool).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM fl WHERE is_flagged IS NOT DISTINCT FROM false ORDER BY id", "2,5"),
        ("SELECT id FROM fl WHERE is_flagged IS NOT DISTINCT FROM true ORDER BY id", "1"),
        ("SELECT id FROM fl WHERE is_flagged IS DISTINCT FROM true ORDER BY id", "2,3,4,5"),
        ("SELECT id FROM fl WHERE is_flagged IS DISTINCT FROM FALSE ORDER BY id", "1,3,4"),
        ("SELECT id FROM fl WHERE is_flagged IS NOT DISTINCT FROM NULL ORDER BY id", "3,4"),
        ("SELECT id FROM fl WHERE is_flagged IS DISTINCT FROM NULL ORDER BY id", "1,2,5"),
        ("SELECT id FROM fl WHERE is_flagged IS DISTINCT FROM other ORDER BY id", "2,4"),
        ("SELECT id FROM fl WHERE is_flagged IS NOT DISTINCT FROM other ORDER BY id", "1,3,5"),
        ("SELECT id FROM fl WHERE nn IS DISTINCT FROM false ORDER BY id", "1,3,5"),
        ("SELECT id FROM fl WHERE nn IS NOT DISTINCT FROM is_flagged ORDER BY id", "1,2"),
        ("SELECT id FROM fl WHERE k IS NOT DISTINCT FROM -1 ORDER BY id", "2,5"),
        ("SELECT id FROM fl WHERE k IS DISTINCT FROM -1 ORDER BY id", "1,3,4"),
        ("SELECT id FROM fl WHERE country IS DISTINCT FROM 'DE' ORDER BY id", "3,4"),
        ("SELECT id FROM fl WHERE id > 1 AND is_flagged IS NOT DISTINCT FROM false ORDER BY id", "2,5"),
        ("SELECT id FROM fl WHERE NOT (is_flagged IS DISTINCT FROM false) ORDER BY id", "2,5"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// `(a, b) = ('DE', 'A')` was "expected closing parenthesis" whenever the
/// right-hand row held only strings: the constant folder read `'DE', 'A'` as
/// one literal. Row `IN` lists were not read at all. PostgreSQL: a row equals
/// another when every element does, is unequal when any element is, and is
/// NULL otherwise — so a NULL element keeps a row out of both answers.
#[tokio::test(flavor = "multi_thread")]
async fn row_value_equality_inequality_and_in() {
    let (_s, pool) = server().await;
    flags(&pool).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM fl WHERE (country, grade) = ('DE', 'A') ORDER BY id", "1"),
        ("SELECT id FROM fl WHERE (country, grade) <> ('DE', 'A') ORDER BY id", "2,3"),
        ("SELECT id FROM fl WHERE (country, grade) != ('DE', 'B') ORDER BY id", "1,3,4"),
        ("SELECT id FROM fl WHERE (grade, country) = ('A', 'FR') ORDER BY id", "3"),
        ("SELECT id FROM fl WHERE (country, grade) IN (('DE', 'A'), ('FR', 'A')) ORDER BY id", "1,3"),
        ("SELECT id FROM fl WHERE (country, grade) NOT IN (('DE', 'A'), ('FR', 'A')) ORDER BY id", "2"),
        ("SELECT id FROM fl WHERE (country, k) IN (('DE', -1), ('XX', 4)) ORDER BY id", "2,5"),
        ("SELECT id FROM fl WHERE (id, grade) IN ((1, 'A'), (4, 'A'), (5, 'A')) ORDER BY id", "1,4"),
        ("SELECT id FROM fl WHERE (country, grade) = ('DE', NULL) ORDER BY id", ""),
        ("SELECT id FROM fl WHERE (country, grade) <> ('DE', NULL) ORDER BY id", "3"),
        (
            "SELECT id FROM fl WHERE id < 5 AND (country, grade) = ('DE', 'A') OR (country, grade) = ('FR', 'A') ORDER BY id",
            "1,3",
        ),
        ("SELECT id FROM fl WHERE NOT ((country, grade) = ('DE', 'A')) ORDER BY id", "2,3"),
        ("SELECT id FROM fl WHERE (country, grade) IN (('DE', 'A')) ORDER BY id", "1"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // The same shapes pick the rows an UPDATE and a DELETE touch.
    assert_eq!(run(&pool, "UPDATE fl SET k = 99 WHERE (country, grade) = ('DE', 'A')").await, Ok(1));
    assert_eq!(
        run(&pool, "DELETE FROM fl WHERE (country, grade) IN (('FR', 'A'), ('DE', 'B'))").await,
        Ok(2)
    );
    assert_eq!(joined(&pool, "SELECT id FROM fl ORDER BY id").await.unwrap(), "1,4,5");
    assert_eq!(joined(&pool, "SELECT k FROM fl WHERE id = 1").await.unwrap(), "99");
}
