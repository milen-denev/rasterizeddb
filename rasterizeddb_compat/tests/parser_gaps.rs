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

async fn digits(pool: &sqlx::PgPool, name: &str) {
    run(pool, &format!("CREATE TABLE {name} (n INT, one INT)")).await.unwrap();
    run(
        pool,
        &format!(
            "INSERT INTO {name} (n, one) VALUES (0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1)"
        ),
    )
    .await
    .unwrap();
}

/// A table with NULLs in both columns and a negative number, for the
/// three-valued answers and for `%` on a negative operand.
async fn nullable_numbers(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE pn (id INT NOT NULL, n INT, s TEXT)").await.unwrap();
    run(
        pool,
        "INSERT INTO pn (id, n, s) VALUES (1, 1, 'a'), (2, 2, NULL), (3, NULL, 'c'), (4, 7, 'dd'), (5, 12, 'e'), (6, -3, 'f')",
    )
    .await
    .unwrap();
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

// ------------------------------------------------------------------ #11 CASE

/// `CASE` branches hold any expression: an unparenthesised `||` chain, `%`
/// (binding tighter than `||`, so `'r' || n % 8` is `'r' || (n % 8)`), a
/// bit-string literal, a nested `CASE`, and arithmetic or concatenation around
/// the block itself. The first two were refused as "function 'CASE WHEN … ||'
/// is not supported yet" and "uses CASE WHEN, which is not supported yet".
#[tokio::test(flavor = "multi_thread")]
async fn case_branches_hold_any_expression_in_the_select_list() {
    let (_s, pool) = server().await;
    digits(&pool, "digit").await;
    nullable_numbers(&pool).await;

    let cases: &[(&str, &str)] = &[
        (
            "SELECT CASE WHEN n % 3 = 0 THEN NULL ELSE 'r' || (n % 8) END FROM digit ORDER BY n",
            "~,r1,r2,~,r4,r5,~,r7,r0,~",
        ),
        (
            "SELECT CASE WHEN n % 3 = 0 THEN NULL ELSE 'r' || n % 8 END FROM digit ORDER BY n",
            "~,r1,r2,~,r4,r5,~,r7,r0,~",
        ),
        (
            "SELECT CASE WHEN n > 4 THEN B'1010' ELSE B'0101' END FROM digit ORDER BY n",
            "0101,0101,0101,0101,0101,1010,1010,1010,1010,1010",
        ),
        (
            "SELECT CASE WHEN n < 3 THEN 'lo' || '-' || n ELSE 'hi' || n * 2 END FROM digit ORDER BY n",
            "lo-0,lo-1,lo-2,hi6,hi8,hi10,hi12,hi14,hi16,hi18",
        ),
        (
            "SELECT CASE WHEN n > 4 THEN CASE WHEN n > 7 THEN 'huge' ELSE 'big' END ELSE 'small' END FROM digit ORDER BY n",
            "small,small,small,small,small,big,big,big,huge,huge",
        ),
        (
            "SELECT upper(CASE WHEN n % 2 = 0 THEN 'e' || n ELSE 'o' || n END) FROM digit ORDER BY n",
            "E0,O1,E2,O3,E4,O5,E6,O7,E8,O9",
        ),
        (
            "SELECT length(CASE WHEN n > 4 THEN 'xx' || n ELSE 'y' END) FROM digit ORDER BY n",
            "1,1,1,1,1,3,3,3,3,3",
        ),
        (
            "SELECT coalesce(CASE WHEN n > 5 THEN 'x' || n END, 'none') FROM digit ORDER BY n",
            "none,none,none,none,none,none,x6,x7,x8,x9",
        ),
        (
            "SELECT CASE WHEN n % 2 = 0 THEN n ELSE 0 END + 1 FROM digit ORDER BY n",
            "1,1,3,1,5,1,7,1,9,1",
        ),
        (
            "SELECT CASE WHEN n > 5 THEN n ELSE 0 END * 2 + 1 FROM digit ORDER BY n",
            "1,1,1,1,1,1,13,15,17,19",
        ),
        (
            "SELECT CASE WHEN n > 4 THEN 'b' ELSE 's' END || n FROM digit ORDER BY n",
            "s0,s1,s2,s3,s4,b5,b6,b7,b8,b9",
        ),
        (
            "SELECT '<' || CASE WHEN n > 4 THEN 'b' ELSE 's' END || '>' FROM digit ORDER BY n",
            "<s>,<s>,<s>,<s>,<s>,<b>,<b>,<b>,<b>,<b>",
        ),
        (
            "SELECT CASE WHEN n % 4 = 0 THEN n % 4 + 10 WHEN n % 4 = 1 THEN n * 100 % 7 ELSE 0 - n END FROM digit ORDER BY n",
            "10,2,-2,-3,10,3,-6,-7,10,4",
        ),
        (
            "SELECT CASE WHEN n > 4 THEN 1 ELSE 0 END + CASE WHEN n % 2 = 0 THEN 10 ELSE 0 END FROM digit ORDER BY n",
            "10,0,10,0,10,1,11,1,11,1",
        ),
        (
            "SELECT CASE WHEN (CASE WHEN n > 4 THEN 1 ELSE 0 END) = 1 THEN 'y' ELSE 'n' END FROM digit ORDER BY n",
            "n,n,n,n,n,y,y,y,y,y",
        ),
        (
            "SELECT CASE WHEN n IS NULL THEN 'null' WHEN n % 2 = 0 THEN 'even' ELSE 'odd' END FROM pn ORDER BY id",
            "odd,even,null,odd,even,odd",
        ),
        ("SELECT 'r' || n % 8 FROM pn ORDER BY id", "r1,r2,~,r7,r4,r-3"),
        (
            "SELECT CASE WHEN s IS NULL THEN 'none' ELSE s || '/' || id END FROM pn ORDER BY id",
            "a/1,none,c/3,dd/4,e/5,f/6",
        ),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// The same expressions in a `WHERE`: a `CASE` compared with a value, a
/// boolean `CASE` standing alone, `%` and `||` outside any `CASE`, under
/// `NOT`, and inside a function call. Each was refused before ("unknown column
/// or unsupported keyword 'CASE'", "unexpected character '|'", "unexpected
/// character '%'"). The NULL rows pin three-valued logic: a `CASE` with no
/// `ELSE` is NULL, and neither it nor its negation holds.
#[tokio::test(flavor = "multi_thread")]
async fn case_and_operators_in_where() {
    let (_s, pool) = server().await;
    digits(&pool, "digit").await;
    nullable_numbers(&pool).await;

    let cases: &[(&str, &str)] = &[
        ("SELECT n FROM digit WHERE CASE WHEN n % 2 = 0 THEN 'e' || n ELSE 'o' END = 'e4' ORDER BY n", "4"),
        ("SELECT n FROM digit WHERE CASE WHEN n % 3 = 0 THEN NULL ELSE 'r' || n % 8 END = 'r1' ORDER BY n", "1"),
        (
            "SELECT n FROM digit WHERE CASE WHEN n > 4 THEN B'1010' ELSE B'0101' END = B'1010' ORDER BY n",
            "5,6,7,8,9",
        ),
        ("SELECT n FROM digit WHERE n % 3 = 1 ORDER BY n", "1,4,7"),
        ("SELECT n FROM digit WHERE 'r' || n % 8 = 'r1' ORDER BY n", "1,9"),
        (
            "SELECT n FROM digit WHERE NOT (CASE WHEN n > 4 THEN 'big' ELSE 'small' END = 'big') ORDER BY n",
            "0,1,2,3,4",
        ),
        ("SELECT n FROM digit WHERE (CASE WHEN n > 4 THEN 'x' END) = 'x' ORDER BY n", "5,6,7,8,9"),
        (
            "SELECT n FROM digit WHERE n > 2 AND CASE WHEN n % 2 = 0 THEN true ELSE false END ORDER BY n",
            "4,6,8",
        ),
        (
            "SELECT n FROM digit WHERE upper(CASE WHEN n % 2 = 0 THEN 'e' || n ELSE 'o' END) = 'E4' ORDER BY n",
            "4",
        ),
        ("SELECT count(*) FROM digit WHERE CASE WHEN n < 5 THEN n ELSE 10 - n END >= 3", "5"),
        (
            "SELECT n FROM digit WHERE CASE WHEN n > 4 THEN CASE WHEN n > 7 THEN 'huge' ELSE 'big' END ELSE 'small' END = 'big' ORDER BY n",
            "5,6,7",
        ),
        ("SELECT n FROM digit WHERE CASE WHEN n % 2 = 0 THEN n ELSE 0 END + 1 > 5 ORDER BY n", "6,8"),
        (
            "SELECT id FROM pn WHERE CASE WHEN n IS NULL THEN 'null' ELSE 'v' || n END <> 'null' ORDER BY id",
            "1,2,4,5,6",
        ),
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN 'big' END = 'big' ORDER BY id", "4,5"),
        ("SELECT id FROM pn WHERE NOT (CASE WHEN n > 5 THEN 'big' END = 'big') ORDER BY id", ""),
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN 'big' END <> 'big' ORDER BY id", ""),
        ("SELECT id FROM pn WHERE s || 'x' = 'ax' ORDER BY id", "1"),
        ("SELECT id FROM pn WHERE n % 2 = 1 OR s = 'c' ORDER BY id", "1,3,4"),
        ("SELECT id FROM pn WHERE n % 2 = -1 ORDER BY id", "6"),
        ("SELECT id FROM pn WHERE NOT (n % 2 = 0) ORDER BY id", "1,4,6"),
        ("SELECT id FROM pn WHERE s || '-' || id LIKE 'd%' ORDER BY id", "4"),
        ("SELECT id FROM pn WHERE id > 1 AND (s || 'z' = 'cz' OR n % 5 = 2) ORDER BY id", "2,3,4,5"),
        ("SELECT n FROM digit WHERE n % 2 = 0 AND n % 3 = 0 ORDER BY n", "0,6"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// The forms around a computed operand: `IS [NOT] NULL`, `[NOT] IN`, `[NOT]
/// BETWEEN`, `[NOT] LIKE`, the truth tests, and a column on the other side.
/// Each is spelled out into comparisons on the operand, and each keeps
/// PostgreSQL's NULL answers: `IS NOT TRUE` holds for a NULL, `NOT IN` a list
/// holding NULL holds for nothing.
#[tokio::test(flavor = "multi_thread")]
async fn forms_over_a_computed_where_operand() {
    let (_s, pool) = server().await;
    digits(&pool, "digit").await;
    nullable_numbers(&pool).await;
    run(&pool, "CREATE TABLE sn (s TEXT NOT NULL)").await.unwrap();
    run(&pool, "INSERT INTO sn (s) VALUES ('a'),('b'),('c'),('d'),('e')").await.unwrap();

    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN 'big' END IS NULL ORDER BY id", "1,2,3,6"),
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN 'big' END IS NOT NULL ORDER BY id", "4,5"),
        ("SELECT id FROM pn WHERE n % 2 IS NULL ORDER BY id", "3"),
        ("SELECT n FROM digit WHERE n % 3 IN (0, 2) ORDER BY n", "0,2,3,5,6,8,9"),
        ("SELECT n FROM digit WHERE n % 3 NOT IN (0, 2) ORDER BY n", "1,4,7"),
        ("SELECT id FROM pn WHERE n % 3 NOT IN (0, NULL) ORDER BY id", ""),
        ("SELECT n FROM digit WHERE n * 3 % 10 BETWEEN 2 AND 6 ORDER BY n", "1,2,4,5,8"),
        ("SELECT n FROM digit WHERE n * 3 % 10 NOT BETWEEN 2 AND 6 ORDER BY n", "0,3,6,7,9"),
        ("SELECT id FROM pn WHERE s || 'x' NOT LIKE 'a%' ORDER BY id", "3,4,5,6"),
        ("SELECT id FROM pn WHERE s || 'x' ILIKE 'D%' ORDER BY id", "4"),
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN true END IS NOT TRUE ORDER BY id", "1,2,3,6"),
        ("SELECT id FROM pn WHERE CASE WHEN n > 5 THEN true END IS TRUE ORDER BY id", "4,5"),
        (
            "SELECT id FROM pn WHERE CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS FALSE ORDER BY id",
            "1,6",
        ),
        (
            "SELECT id FROM pn WHERE CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS NOT FALSE ORDER BY id",
            "2,3,4,5",
        ),
        (
            "SELECT id FROM pn WHERE CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS UNKNOWN ORDER BY id",
            "2,3",
        ),
        ("SELECT id FROM pn WHERE NOT (CASE WHEN n > 5 THEN true END) ORDER BY id", ""),
        (
            "SELECT id FROM pn WHERE NOT CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END ORDER BY id",
            "1,6",
        ),
        ("SELECT id FROM pn WHERE (n % 2 = 0) IS NOT TRUE ORDER BY id", "1,3,4,6"),
        // `IS TRUE` and `IS FALSE` are never NULL, so a NOT over one keeps
        // the rows whose operand is NULL, and so does a test of the group's
        // falsity. Spelled as the bare comparison, a NULL operand was false
        // negated or not, and these dropped rows 2 and 3.
        ("SELECT id FROM pn WHERE NOT (CASE WHEN n > 5 THEN true END IS TRUE) ORDER BY id", "1,2,3,6"),
        (
            "SELECT id FROM pn WHERE NOT (CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS FALSE) ORDER BY id",
            "2,3,4,5",
        ),
        ("SELECT id FROM pn WHERE NOT CASE WHEN n > 5 THEN true END IS TRUE ORDER BY id", "1,2,3,6"),
        (
            "SELECT id FROM pn WHERE NOT CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS FALSE ORDER BY id",
            "2,3,4,5",
        ),
        ("SELECT id FROM pn WHERE (CASE WHEN n > 5 THEN true END IS TRUE) IS FALSE ORDER BY id", "1,2,3,6"),
        (
            "SELECT id FROM pn WHERE (CASE WHEN n > 5 THEN true WHEN n < 2 THEN false END IS FALSE) IS FALSE ORDER BY id",
            "2,3,4,5",
        ),
        ("SELECT id FROM pn WHERE (CASE WHEN n > 5 THEN true END IS TRUE) IS NOT TRUE ORDER BY id", "1,2,3,6"),
        ("SELECT id FROM pn WHERE (CASE WHEN n > 5 THEN true END IS NOT TRUE) IS FALSE ORDER BY id", "4,5"),
        ("SELECT id FROM pn WHERE NOT (CASE WHEN n > 5 THEN true END IS NOT TRUE) ORDER BY id", "4,5"),
        ("SELECT id FROM pn WHERE (CASE WHEN n > 5 THEN 'x' END IS NULL) IS FALSE ORDER BY id", "4,5"),
        (
            "SELECT id FROM pn WHERE id > 1 AND NOT (CASE WHEN n > 5 THEN true END IS TRUE OR s = 'f') ORDER BY id",
            "3",
        ),
        ("SELECT n FROM digit WHERE n = CASE WHEN n > 5 THEN n END ORDER BY n", "6,7,8,9"),
        ("SELECT n FROM digit WHERE CASE WHEN n > 5 THEN 1 ELSE 0 END = one ORDER BY n", "6,7,8,9"),
        (
            "SELECT n FROM digit WHERE (n = CASE WHEN n > 7 THEN n END OR n = CASE WHEN n < 1 THEN n END \
             OR n = CASE WHEN n = 4 THEN n END OR n = CASE WHEN n = 5 THEN 5 END OR n = 2 + 0 * one) ORDER BY n",
            "0,2,4,5,8,9",
        ),
        // A run of `col = <expression>` making up one group over a NOT NULL
        // text column: the shape the planner folds into a hashed set when the
        // right-hand sides are constants. Folded here, it would test the
        // placeholder each computed side stands on.
        (
            "SELECT s FROM sn WHERE (s = CASE WHEN s > 'c' THEN s END OR s = CASE WHEN s = 'a' THEN 'a' END \
             OR s = CASE WHEN s = 'b' THEN s END OR s = CASE WHEN s < 'a' THEN s END) ORDER BY s",
            "a,b,d,e",
        ),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // Not a boolean: PostgreSQL's "argument of WHERE must be type boolean".
    let err = joined(&pool, "SELECT n FROM digit WHERE n % 2").await.expect_err("integer WHERE");
    assert!(err.contains("must be type boolean"), "{err}");
}

/// A parameter inside a computed operand, bound over the extended protocol
/// with different values in turn. A prepared statement's parse is reused for
/// new values by finding each value in the rewritten clause, so this pins that
/// a value inside a lifted operand is never found there and swapped.
#[tokio::test(flavor = "multi_thread")]
async fn parameters_inside_a_computed_where_operand() {
    let (_s, pool) = server().await;
    digits(&pool, "digit").await;
    for (modulus, label, want) in [
        (2, "e", vec![0, 2, 4, 6, 8]),
        (3, "e", vec![0, 3, 6, 9]),
        (3, "o", vec![1, 2, 4, 5, 7, 8]),
        (5, "e", vec![0, 5]),
        (2, "o", vec![1, 3, 5, 7, 9]),
        (4, "e", vec![0, 4, 8]),
    ] {
        let got: Vec<i32> = sqlx::query(
            "SELECT n FROM digit WHERE CASE WHEN n % $1 = 0 THEN 'e' ELSE 'o' END = $2 ORDER BY n",
        )
        .bind(modulus)
        .bind(label)
        .fetch_all(&pool)
        .await
        .unwrap_or_else(|e| panic!("% {modulus} = {label}: {e}"))
        .iter()
        .map(|r| r.get(0))
        .collect();
        assert_eq!(got, want, "n % {modulus} labelled {label}");
    }
}

/// `UPDATE` and `DELETE` read their `WHERE` through the same machinery, so
/// they take the same shapes and touch exactly the rows a `SELECT` returns.
#[tokio::test(flavor = "multi_thread")]
async fn case_and_operators_in_update_and_delete_where() {
    let (_s, pool) = server().await;
    digits(&pool, "dg").await;
    assert_eq!(
        run(&pool, "UPDATE dg SET one = 2 WHERE CASE WHEN n % 2 = 0 THEN 'e' ELSE 'o' END = 'e'").await,
        Ok(5)
    );
    assert_eq!(
        joined(&pool, "SELECT n FROM dg WHERE one = 2 ORDER BY n").await.unwrap(),
        "0,2,4,6,8"
    );
    assert_eq!(run(&pool, "DELETE FROM dg WHERE n % 3 = 0").await, Ok(4));
    assert_eq!(joined(&pool, "SELECT n FROM dg ORDER BY n").await.unwrap(), "1,2,4,5,7,8");
    assert_eq!(run(&pool, "DELETE FROM dg WHERE 'x' || n = 'x5' RETURNING n").await, Ok(1));
    assert_eq!(joined(&pool, "SELECT n FROM dg ORDER BY n").await.unwrap(), "1,2,4,7,8");
    // A NOT over a truth test holds where the operand is NULL, in a DELETE
    // too: the CASE is NULL for 1, 2 and 4, and PostgreSQL deletes them.
    assert_eq!(
        run(&pool, "DELETE FROM dg WHERE NOT (CASE WHEN n > 6 THEN true END IS TRUE)").await,
        Ok(3)
    );
    assert_eq!(joined(&pool, "SELECT n FROM dg ORDER BY n").await.unwrap(), "7,8");
}

/// A computed operand meets a column of another type only where PostgreSQL
/// has an operator for the pair. A `CASE` over quoted literals is text, and
/// PostgreSQL has no `date = text` or `boolean = text`. Compared anyway, the
/// text's first bytes were read as the date's, so `=` matched nothing and `<>`
/// matched every row.
#[tokio::test(flavor = "multi_thread")]
async fn a_computed_operand_and_a_column_of_another_type() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE pd (k INT NOT NULL, d DATE, b BOOL, t TEXT, c CHAR(3), v VARCHAR(5), nm NUMERIC(10,2), i8 BIGINT, f FLOAT8)",
    )
    .await
    .unwrap();
    run(
        &pool,
        "INSERT INTO pd VALUES (1, '2024-01-01', true, 'x', 'ab', 'ab', 1.00, 1, 1.0), \
         (2, '2024-02-01', false, 'y', 'cd', 'cd', 2.50, 2, 2.5), (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    )
    .await
    .unwrap();
    run(&pool, "CREATE TABLE pe (k INT NOT NULL, c1 CHAR, bt BIT(4), vb VARBIT, j JSONB)").await.unwrap();
    run(
        &pool,
        "INSERT INTO pe VALUES (1, 'a', B'1010', B'101', '{\"a\":1}'), (2, 'b', B'0101', B'11', '{\"a\":2}'), \
         (3, NULL, NULL, NULL, NULL)",
    )
    .await
    .unwrap();

    let mut wrong = Vec::new();
    for (sql, want) in [
        (
            "SELECT k FROM pd WHERE d = CASE WHEN k = 1 THEN '2024-01-01' ELSE '2024-02-01' END",
            "operator does not exist: date = text",
        ),
        (
            "SELECT k FROM pd WHERE CASE WHEN k = 1 THEN '2024-01-01' ELSE '2024-02-01' END < d",
            "operator does not exist: text < date",
        ),
        (
            "SELECT k FROM pd WHERE d <> 'x' || ''",
            "operator does not exist: date <> text",
        ),
        (
            "SELECT k FROM pd WHERE b = CASE WHEN k = 1 THEN 't' ELSE 'f' END",
            "operator does not exist: boolean = text",
        ),
        ("SELECT k FROM pd WHERE t = k % 3", "operator does not exist: text = integer"),
        ("SELECT k FROM pe WHERE bt = k % 2", "operator does not exist: bit = integer"),
        // Pairs PostgreSQL does compare and the engine cannot yet. Its
        // answers, in order: 1, '' and 1,2. The `<` over a `character`
        // column answered 1 before, and the rest failed per row with an
        // operand-size error.
        (
            "SELECT k FROM pe WHERE c1 = CASE WHEN k = 1 THEN 'a' ELSE 'z' END ORDER BY k",
            "is not supported yet",
        ),
        (
            "SELECT k FROM pe WHERE CASE WHEN k = 1 THEN 'a' ELSE 'z' END < c1 ORDER BY k",
            "is not supported yet",
        ),
        (
            "SELECT k FROM pe WHERE (j->>'a')::bigint = k % 3 ORDER BY k",
            "comparing a bigint value with a computed integer value is not supported yet",
        ),
    ] {
        match joined(&pool, sql).await {
            Err(e) if e.contains(want) => {}
            got => wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}")),
        }
    }

    // The pairs PostgreSQL compares, and the engine with it.
    let cases: &[(&str, &str)] = &[
        // A NUMERIC column against a computed integer or NUMERIC, which were
        // refused as not supported: the integer is promoted, as PostgreSQL
        // promotes it.
        ("SELECT k FROM pd WHERE nm = CASE WHEN k = 1 THEN 1 ELSE 3 END ORDER BY k", "1"),
        ("SELECT k FROM pd WHERE nm <> CASE WHEN k = 1 THEN 1 ELSE 3 END ORDER BY k", "2"),
        ("SELECT k FROM pd WHERE nm = CASE WHEN k = 1 THEN 1.0 ELSE 3.0 END ORDER BY k", "1"),
        ("SELECT k FROM pd WHERE nm > k % 3 ORDER BY k", "2"),
        ("SELECT k FROM pd WHERE k % 3 < nm ORDER BY k", "2"),
        ("SELECT k FROM pd WHERE c = CASE WHEN k = 1 THEN 'ab' ELSE 'zz' END ORDER BY k", "1"),
        ("SELECT k FROM pd WHERE v = CASE WHEN k = 1 THEN 'ab' ELSE 'zz' END ORDER BY k", "1"),
        ("SELECT k FROM pd WHERE c = t || '' ORDER BY k", ""),
        ("SELECT k FROM pd WHERE f = CASE WHEN k = 1 THEN 1 ELSE 3 END ORDER BY k", "1"),
        ("SELECT k FROM pd WHERE i8 = k % 3 ORDER BY k", "1,2"),
        ("SELECT k FROM pe WHERE bt = CASE WHEN k = 1 THEN B'1010' ELSE B'1111' END ORDER BY k", "1"),
        ("SELECT k FROM pe WHERE vb = CASE WHEN k = 1 THEN B'101' ELSE B'1' END ORDER BY k", "1"),
        ("SELECT k FROM pe WHERE j->>'a' = k || '' ORDER BY k", "1,2"),
    ];
    wrong.extend(wrong_answers(&pool, cases).await);
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // With PostgreSQL's SQLSTATE, undefined_function.
    let err = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(
            "SELECT k FROM pd WHERE b = CASE WHEN k = 1 THEN 't' ELSE 'f' END".to_string(),
        )))
        .await
        .expect_err("boolean = text");
    assert_eq!(err.as_database_error().and_then(|e| e.code()).as_deref(), Some("42883"), "{err}");
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
        // Elements the expression evaluator computes: the row is split into
        // its comparisons before any of them is lifted.
        ("SELECT id FROM fl WHERE (country, k % 2) IN (('DE', 1), ('FR', 0)) ORDER BY id", "1"),
        ("SELECT id FROM fl WHERE (country, k % 2) = ('DE', -1) ORDER BY id", "2,5"),
        ("SELECT id FROM fl WHERE (grade || '!', id) <> ('A!', 1) ORDER BY id", "2,3,4,5"),
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

// ------------------------------------------------------- #15b column casts

/// Order lines, one all NULL, one dated before 2000.
async fn order_lines(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE od (id INT NOT NULL, qty INT, discount_pct NUMERIC(5,2), unit_price NUMERIC(12,2), \
         big BIGINT, f FLOAT8, nm4 NUMERIC(10,4), t TEXT, d DATE, ts TIMESTAMP)",
    )
    .await
    .unwrap();
    run(
        pool,
        "INSERT INTO od VALUES (1, 5, 2.50, 10.25, 5, 2.5, 1.5000, '5', '2024-01-05', '2024-01-05 10:00:00'), \
         (2, 12, 4.00, 3.10, 12, 4.0, 4.0000, '12', '2024-02-01', '2024-02-01 00:00:00'), \
         (3, 7, 0.00, 1.00, 7, 0.0, 0.0000, '7', '2024-03-01', '2024-03-01 12:30:00'), \
         (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         (5, 20, 6.75, 2.00, 20, 6.75, 6.7500, '20', '2023-12-31', '2023-12-31 23:59:59'), \
         (6, 3, 1.25, 0.50, 3, 0.5, 0.2500, '3', '1999-12-31', '1999-06-15 08:00:00')",
    )
    .await
    .unwrap();
}

/// A `::` cast on a column in a `WHERE`, on either side, to a number, text, a
/// date or a timestamp, converts as the select list's cast does.
/// `qty::int = 5` failed to plan with "expected comparison operator, got
/// Cast(I32)": the rewrite carried the cast on the column's name and then
/// emitted the cast again after it. Under that, the per-row conversion read a
/// NUMERIC's stored integer at the wrong scale, and `ts::date` as the whole
/// timestamp, so `d = ts::date` matched nothing.
#[tokio::test(flavor = "multi_thread")]
async fn column_casts_in_where() {
    let (_s, pool) = server().await;
    order_lines(&pool).await;
    let cases: &[(&str, &str)] = &[
        // Integer targets.
        ("SELECT id FROM od WHERE qty::int = 5 ORDER BY id", "1"),
        ("SELECT id FROM od WHERE qty ::int = 5 ORDER BY id", "1"),
        ("SELECT id FROM od WHERE (qty)::int = 5 ORDER BY id", "1"),
        ("SELECT id FROM od WHERE qty::bigint > 6 ORDER BY id", "2,3,5"),
        ("SELECT id FROM od WHERE 6 < qty::bigint ORDER BY id", "2,3,5"),
        ("SELECT id FROM od WHERE qty::smallint <= 5 ORDER BY id", "1,6"),
        ("SELECT id FROM od WHERE CAST(qty AS bigint) = 12 ORDER BY id", "2"),
        // A NUMERIC into an integer rounds half away from zero: 2.50 is 3.
        ("SELECT id FROM od WHERE discount_pct::int = 3 ORDER BY id", "1"),
        ("SELECT id FROM od WHERE discount_pct::int = qty % 4 ORDER BY id", ""),
        ("SELECT id FROM od WHERE unit_price::bigint > big ORDER BY id", "1"),
        // Text into an integer, against a column on either side.
        ("SELECT id FROM od WHERE t::int > 6 ORDER BY id", "2,3,5"),
        ("SELECT id FROM od WHERE t::int = qty ORDER BY id", "1,2,3,5,6"),
        ("SELECT id FROM od WHERE qty = t::int ORDER BY id", "1,2,3,5,6"),
        // NUMERIC and float targets.
        ("SELECT id FROM od WHERE qty::numeric = 7 ORDER BY id", "3"),
        ("SELECT id FROM od WHERE qty::numeric(10,2) >= 7.5 ORDER BY id", "2,5"),
        ("SELECT id FROM od WHERE discount_pct::numeric(4,1) = 6.8 ORDER BY id", "5"),
        ("SELECT id FROM od WHERE f::numeric(10,1) = 6.8 ORDER BY id", "5"),
        ("SELECT id FROM od WHERE t::numeric > 6.5 ORDER BY id", "2,3,5"),
        ("SELECT id FROM od WHERE t::numeric(6,2) = discount_pct * 2 ORDER BY id", "1"),
        ("SELECT id FROM od WHERE discount_pct::float8 > 2.4 ORDER BY id", "1,2,5"),
        // Text targets, rendered as a client sees them: 2.50 keeps its scale.
        ("SELECT id FROM od WHERE qty::text = '12' ORDER BY id", "2"),
        ("SELECT id FROM od WHERE qty::varchar = '7' ORDER BY id", "3"),
        ("SELECT id FROM od WHERE qty::text > '3' ORDER BY id", "1,3"),
        ("SELECT id FROM od WHERE discount_pct::text = '2.50' ORDER BY id", "1"),
        ("SELECT id FROM od WHERE nm4::text LIKE '%.2500' ORDER BY id", "6"),
        ("SELECT id FROM od WHERE qty::text SIMILAR TO '(1|2)%' ORDER BY id", "2,5"),
        ("SELECT id FROM od WHERE qty::text NOT LIKE '1%' ORDER BY id", "1,3,5,6"),
        ("SELECT id FROM od WHERE d::text LIKE '2024-0%' ORDER BY id", "1,2,3"),
        ("SELECT id FROM od WHERE ts::text LIKE '%12:30:00' ORDER BY id", "3"),
        // The same rendering in the select list, which trimmed the zeros: 2.5.
        ("SELECT discount_pct::text FROM od WHERE id = 1", "2.50"),
        ("SELECT (discount_pct * 2)::text FROM od WHERE id = 1", "5.00"),
        ("SELECT nm4::text || 'x' FROM od WHERE id = 6", "0.2500x"),
        ("SELECT CAST(nm4 AS varchar) FROM od WHERE id = 1", "1.5000"),
        // Date targets: the day, not the timestamp.
        ("SELECT id FROM od WHERE ts::date = '2024-01-05' ORDER BY id", "1"),
        ("SELECT id FROM od WHERE ts::date = d ORDER BY id", "1,2,3,5"),
        ("SELECT id FROM od WHERE d = ts::date ORDER BY id", "1,2,3,5"),
        ("SELECT id FROM od WHERE ts::date > d ORDER BY id", ""),
        ("SELECT id FROM od WHERE ts::date < d ORDER BY id", "6"),
        ("SELECT id FROM od WHERE ts::date < '2000-01-01' ORDER BY id", "6"),
        ("SELECT id FROM od WHERE ts::date >= '2024-01-01' ORDER BY id", "1,2,3"),
        ("SELECT id FROM od WHERE ts::date BETWEEN '2023-12-31' AND '2024-02-01' ORDER BY id", "1,2,5"),
        ("SELECT id FROM od WHERE CAST(ts AS date) = '2024-03-01' ORDER BY id", "3"),
        ("SELECT id FROM od WHERE ts::date::text = '2024-01-05' ORDER BY id", "1"),
        // Timestamp targets.
        ("SELECT id FROM od WHERE d::timestamp < '2024-02-01 00:00:01' ORDER BY id", "1,2,5,6"),
        ("SELECT id FROM od WHERE d::timestamp = '2024-02-01 00:00:00' ORDER BY id", "2"),
        ("SELECT id FROM od WHERE d::timestamp <= ts ORDER BY id", "1,2,3,5"),
        ("SELECT id FROM od WHERE ts > d::timestamp ORDER BY id", "1,3,5"),
        // The forms around a cast operand, with PostgreSQL's NULL answers.
        ("SELECT id FROM od WHERE qty::int IS NULL ORDER BY id", "4"),
        ("SELECT id FROM od WHERE qty::int IS NOT NULL ORDER BY id", "1,2,3,5,6"),
        ("SELECT id FROM od WHERE qty::int IN (5, 7) ORDER BY id", "1,3"),
        ("SELECT id FROM od WHERE qty::int NOT IN (5, 7) ORDER BY id", "2,5,6"),
        ("SELECT id FROM od WHERE qty::int BETWEEN 6 AND 12 ORDER BY id", "2,3"),
        ("SELECT id FROM od WHERE NOT (qty::int = 5) ORDER BY id", "2,3,5,6"),
        ("SELECT id FROM od WHERE NOT (qty::int = 5 OR ts::date = '2024-02-01') ORDER BY id", "3,5,6"),
        ("SELECT id FROM od WHERE qty::int > 4 AND d::text LIKE '2024%' ORDER BY id", "1,2,3"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // An UPDATE and a DELETE pick their rows the same way.
    assert_eq!(run(&pool, "UPDATE od SET qty = 0 WHERE ts::date = '2024-02-01'").await, Ok(1));
    assert_eq!(run(&pool, "DELETE FROM od WHERE discount_pct::int = 3").await, Ok(1));
    assert_eq!(joined(&pool, "SELECT id FROM od WHERE qty::int = 0 ORDER BY id").await.unwrap(), "2");
    assert_eq!(joined(&pool, "SELECT id FROM od ORDER BY id").await.unwrap(), "2,3,4,5,6");
}

// ------------------------------------- what a rewrite renders and lexes again

/// Rows whose text holds the characters the rewrites split and re-lex on: a
/// quote, `%`, ` AND `, a backslash, parentheses; and a quoted column name.
async fn awkward_text(pool: &sqlx::PgPool) {
    run(pool, r#"CREATE TABLE au (id INT NOT NULL, t TEXT, n INT, "MixedCase" TEXT, q NUMERIC(6,2), d DATE)"#)
        .await
        .unwrap();
    run(
        pool,
        r#"INSERT INTO au VALUES (1, 'it''s', 1, 'A', 1.50, '2024-01-01'), (2, 'a%b', 2, 'B', 2.25, '2024-01-02'),
           (3, 'x AND y', 3, 'C', NULL, '2024-01-03'), (4, 'a\b', 4, NULL, 4.00, NULL),
           (5, '(p)', 5, 'E', 0.10, '2024-01-05'), (6, NULL, NULL, 'F', 6.00, '2024-01-06')"#,
    )
    .await
    .unwrap();
}

/// The `WHERE` rewrite lexes the clause, renders it back to text, and the
/// tokenizer lexes that again, several times over. What the first lex read has
/// to survive each pass. Two things did not:
///
/// - A cast on a literal is dropped, the column it is compared with saying the
///   type. An arithmetic operand is compared with nothing, so the cast was the
///   type: `n / 2::numeric > 1.2` divided integers and dropped the row where
///   `n` is 3, and `q > '1.5'::numeric * 2` read the literal as text and was
///   refused. A join's relations dropped it the same way, and refused `%`,
///   `||` and a column cast, because their scans did not lift what only the
///   evaluator computes.
/// - `au."MixedCase"`, a bare qualifier on a quoted column, was split into
///   the qualifier and the name, and the qualifier refused as a column.
///
/// The rest pin what already survived: quotes, escapes, dollar quoting and
/// keywords inside literals, and parentheses the rewrite must not drop.
#[tokio::test(flavor = "multi_thread")]
async fn rewrites_keep_what_the_first_lex_read() {
    let (_s, pool) = server().await;
    awkward_text(&pool).await;
    let cases: &[(&str, &str)] = &[
        // A literal's cast in arithmetic is its type.
        ("SELECT id FROM au WHERE n / 2::numeric > 1.2 ORDER BY id", "3,4,5"),
        ("SELECT id FROM au WHERE n / 2::numeric = 1.5 ORDER BY id", "3"),
        ("SELECT id FROM au WHERE NOT (n / 2::numeric > 1.2) ORDER BY id", "1,2"),
        ("SELECT id FROM au WHERE n / '2'::numeric = 2.5 ORDER BY id", "5"),
        ("SELECT id FROM au WHERE n * 1::numeric / 4 = 0.75 ORDER BY id", "3"),
        ("SELECT id FROM au WHERE q * '2'::int > 4 ORDER BY id", "2,4,6"),
        ("SELECT id FROM au WHERE n + '1'::int = 3 ORDER BY id", "2"),
        ("SELECT id FROM au WHERE q > '1.5'::numeric * 2 ORDER BY id", "4,6"),
        ("SELECT id FROM au WHERE d = '2024-01-01'::date + 1 ORDER BY id", "2"),
        ("SELECT id FROM au WHERE d > '2024-01-01'::date + 2 ORDER BY id", "5,6"),
        // Compared directly, the column's type is the literal's, as before.
        ("SELECT id FROM au WHERE n / 2 > 1.2 ORDER BY id", "4,5"),
        ("SELECT id FROM au WHERE n = 2::bigint ORDER BY id", "2"),
        ("SELECT id FROM au WHERE n = '2'::bigint ORDER BY id", "2"),
        ("SELECT id FROM au WHERE q = 2.25::numeric(6,2) ORDER BY id", "2"),
        // Quoted column names, qualified or cast.
        (r#"SELECT id FROM au WHERE au."MixedCase" = 'A' ORDER BY id"#, "1"),
        (r#"SELECT id FROM au WHERE "au"."MixedCase" = 'A' ORDER BY id"#, "1"),
        (r#"SELECT id FROM au WHERE "q"::int = 2 ORDER BY id"#, "1,2"),
        (r#"SELECT id FROM au WHERE "MixedCase"::text || '!' = 'E!' ORDER BY id"#, "5"),
        (r#"SELECT id FROM au WHERE NOT ("MixedCase"::text = 'E') ORDER BY id"#, "1,2,3,6"),
        (r#"SELECT id FROM au WHERE ("MixedCase", n) = ('A', 1) ORDER BY id"#, "1"),
        // Quotes, escapes and dollar quoting through a lift and a NOT.
        (r#"SELECT id FROM au WHERE t = E'it\'s' AND n % 2 = 1 ORDER BY id"#, "1"),
        (r#"SELECT id FROM au WHERE NOT (t = E'it\'s' OR n = 2) ORDER BY id"#, "3,4,5"),
        (r#"SELECT id FROM au WHERE NOT (t = E'a\\b') ORDER BY id"#, "1,2,3,5"),
        (r#"SELECT id FROM au WHERE t = $$it's$$ AND n % 2 = 1 ORDER BY id"#, "1"),
        (r#"SELECT id FROM au WHERE t = $q$x AND y$q$ ORDER BY id"#, "3"),
        ("SELECT id FROM au WHERE t = 'x AND y' OR n % 2 = 0 ORDER BY id", "2,3,4"),
        ("SELECT id FROM au WHERE t || '::int' = 'a%b::int' ORDER BY id", "2"),
        ("SELECT id FROM au WHERE (t || 'x', n) = ('it''sx', 1) ORDER BY id", "1"),
        // Parentheses that hold.
        ("SELECT id FROM au WHERE (n + 1)::text = '3' ORDER BY id", "2"),
        ("SELECT id FROM au WHERE ('a' || t)::text = 'a(p)' ORDER BY id", "5"),
        ("SELECT id FROM au WHERE NOT ((n + 1) * 2 = 6) ORDER BY id", "1,3,4,5"),
        ("SELECT id FROM au WHERE (10 - n) % 4 = 1 ORDER BY id", "1,5"),
        // One relation's conditions in a join.
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.n / 2::numeric = 1.5 ORDER BY a.id", "3"),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE b.d = '2024-01-01'::date + 1 ORDER BY a.id", "2"),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.q::int = 2 ORDER BY a.id", "1,2"),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.n % 2 = 0 ORDER BY a.id", "2,4"),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE b.t || 'x' = 'x AND yx' ORDER BY a.id", "3"),
        (r#"SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a."MixedCase" = 'B' ORDER BY a.id"#, "2"),
        (r#"SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.t = E'it\'s' ORDER BY a.id"#, "1"),
        (
            "SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE NOT (a.t = 'x AND y') AND b.n % 2 = 1 ORDER BY a.id",
            "1,5",
        ),
        // A condition across both relations.
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.n / 2::numeric = b.q ORDER BY a.id", ""),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.n / 2::numeric > b.q ORDER BY a.id", "5"),
        ("SELECT a.id FROM au a JOIN au b ON b.id = a.id WHERE a.n * 1.5::numeric = b.q * 1 ORDER BY a.id", "1"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // A CHECK is compiled from the same rewrite, and dropping the cast
    // divided integers: 5 / 2 = 2 passed `< 2.2` and the row was stored.
    run(&pool, "CREATE TABLE ck (id INT, n INT CHECK (n / 2::numeric < 2.2))").await.unwrap();
    assert_eq!(run(&pool, "INSERT INTO ck VALUES (1, 4)").await, Ok(1));
    let err = run(&pool, "INSERT INTO ck VALUES (2, 5)").await.expect_err("5 / 2.0 is 2.5");
    assert!(err.contains("violates check constraint"), "{err}");
}

/// Text the join, CTE and generated-column rewrites copy into a new statement
/// keeps its multi-byte characters. Each copied the statement a byte at a
/// time and pushed every byte as a `char` of its own, so `'é'` arrived as
/// `'Ã©'`: through a CTE `t = 'é'` matched nothing and `t || 'ß'` returned
/// `éÃ\u{9f}`, a join's second pass sliced inside the mangled character and
/// closed the connection, and a generated column over `naïve` panicked.
#[tokio::test(flavor = "multi_thread")]
async fn non_ascii_text_survives_the_rewrites() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ux (id INT NOT NULL, t TEXT, n INT)").await.unwrap();
    run(&pool, "INSERT INTO ux VALUES (1, 'é', 1), (2, 'naïve', 2), (3, 'x', 3)").await.unwrap();
    let cases: &[(&str, &str)] = &[
        ("SELECT a.t || 'é' FROM ux a JOIN ux b ON b.id = a.id ORDER BY a.id", "éé,naïveé,xé"),
        ("SELECT a.id FROM ux a JOIN ux b ON b.id = a.id WHERE a.t = 'é' ORDER BY a.id", "1"),
        ("SELECT a.id FROM ux a JOIN ux b ON b.id = a.id WHERE a.t || b.t = 'éé' ORDER BY a.id", "1"),
        (
            "SELECT CASE WHEN a.t = 'naïve' THEN 'ü' ELSE 'ö' END FROM ux a JOIN ux b ON b.id = a.id ORDER BY a.id",
            "ö,ü,ö",
        ),
        ("WITH c AS (SELECT id, t FROM ux WHERE t = 'naïve') SELECT t FROM c", "naïve"),
        ("WITH c AS (SELECT id, t FROM ux) SELECT id FROM c WHERE t = 'é'", "1"),
        ("WITH c AS (SELECT id, t FROM ux) SELECT c.t || 'ß' FROM c ORDER BY c.id", "éß,naïveß,xß"),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    run(
        &pool,
        "CREATE TABLE gn (id INT, naïve INT, g INT GENERATED ALWAYS AS (naïve * 2) STORED, \
         s TEXT GENERATED ALWAYS AS ('é' || naïve) STORED)",
    )
    .await
    .unwrap();
    assert_eq!(run(&pool, "INSERT INTO gn (id, naïve) VALUES (1, 21)").await, Ok(1));
    assert_eq!(joined(&pool, "SELECT g FROM gn").await.unwrap(), "42");
    assert_eq!(joined(&pool, "SELECT s FROM gn").await.unwrap(), "é21");
}

/// A correlated `EXISTS` over the outer query's own table, its relation
/// aliased: `FROM ev e2 WHERE e2.id = ev.id`. The rewrite counted `ev` among
/// the subquery's own names as well as `e2`, so `ev.id` read as the inner
/// row's, the `EXISTS` as uncorrelated, and it held for every row. An alias
/// hides its table's name, as PostgreSQL scopes it. An outer column compared
/// by anything but `=` was left inside the subquery the rewrite runs once,
/// and matched nothing; it is refused until it can be answered.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_exists_over_its_own_table() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ev (id INT NOT NULL, k TEXT, v INT, t TEXT)").await.unwrap();
    run(
        &pool,
        r#"INSERT INTO ev VALUES (1, 'a', 10, E'x\ny'), (2, 'a', 20, 'p'), (3, 'b', 5, 'q'), (4, 'c', 7, 'r'), (5, 'c', 9, NULL)"#,
    )
    .await
    .unwrap();
    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.id = ev.id AND e2.k = 'c') ORDER BY id", "4,5"),
        ("SELECT id FROM ev WHERE NOT EXISTS (SELECT 1 FROM ev e2 WHERE e2.id = ev.id AND e2.k = 'c') ORDER BY id", "1,2,3"),
        (
            "SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev AS e2 WHERE ev.id = e2.id AND e2.v > 8) ORDER BY id",
            "1,2,5",
        ),
        (
            r#"SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.id = ev.id AND e2.t = E'x\ny') ORDER BY id"#,
            "1",
        ),
    ];
    let wrong = wrong_answers(&pool, cases).await;
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // PostgreSQL's answers are 1,4 and 1,2,4,5.
    for sql in [
        "SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.k = ev.k AND e2.v > ev.v) ORDER BY id",
        "SELECT o.id FROM ev o WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.k = o.k AND e2.id <> o.id) ORDER BY o.id",
    ] {
        let err = joined(&pool, sql).await.expect_err(sql);
        assert!(err.contains("not supported yet"), "{sql}: {err}");
    }
}
