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
        // answers, in order: 1, 1, 2, 1, '' and 1,2. The `<` over a
        // `character` column answered 1 before, and the rest failed per row
        // with an operand-size error.
        (
            "SELECT k FROM pd WHERE nm = CASE WHEN k = 1 THEN 1 ELSE 3 END ORDER BY k",
            "comparing a numeric value with a computed integer value is not supported yet",
        ),
        (
            "SELECT k FROM pd WHERE nm = CASE WHEN k = 1 THEN 1.0 ELSE 3.0 END ORDER BY k",
            "which a WHERE clause cannot compare yet",
        ),
        (
            "SELECT k FROM pd WHERE nm > k % 3 ORDER BY k",
            "comparing a numeric value with a computed integer value is not supported yet",
        ),
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
