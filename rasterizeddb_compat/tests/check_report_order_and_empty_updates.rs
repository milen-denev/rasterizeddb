//! Two things about CHECK constraints, each as PostgreSQL 17 answers it:
//!
//! - An `UPDATE` that writes no row breaks no check. `UPDATE t SET status =
//!   99 WHERE id = 0` was refused for its check with no row to break it: a
//!   check whose every column the statement assigns a constant was judged
//!   before the scan, on the assignments alone. PostgreSQL answers `UPDATE 0`.
//! - A row that breaks several checks is reported under the first by name
//!   (byte order: `t_a_check` before `t_check`, `"Mid"` before `aa`), a
//!   domain's before the table's. It was reported under the first written,
//!   and on `UPDATE` under whichever phase judged it.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in the
//! same order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>, (String, String)> {
    match sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()),
        Err(e) => {
            let db = e.as_database_error().unwrap_or_else(|| panic!("`{sql}`: no database error: {e}"));
            Err((db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string()))
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_of_no_row_breaks_no_check_and_the_first_check_by_name_is_reported() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    // Tables, one row each, then UPDATEs whose WHERE matches nothing: each
    // would break a check if it wrote a row, and writes none.
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ck2 (a int CHECK (a > 0), b int, CHECK (a > b), CHECK (b > a - 100), CHECK (b < 1000))"#, vec![]),
        (r#"CREATE TABLE co (z int, a int, CONSTRAINT zz CHECK (z > 0), CONSTRAINT "Mid" CHECK (z < 100 AND a > 0), CONSTRAINT aa CHECK (a > 0), CONSTRAINT bb CHECK (a + z > 0))"#, vec![]),
        (r#"CREATE DOMAIN posint AS int CHECK (VALUE > 0)"#, vec![]),
        (r#"CREATE TABLE cd (id int, p posint, n int, CONSTRAINT a_first CHECK (n > 0))"#, vec![]),
        (r#"CREATE TABLE gen (q int, p int, tot int GENERATED ALWAYS AS (q * p) STORED, CONSTRAINT tot_pos CHECK (tot >= 0), CONSTRAINT q_small CHECK (q < 100))"#, vec![]),
        (r#"INSERT INTO co VALUES (5, 5)"#, vec![]),
        (r#"INSERT INTO ck2 VALUES (1, 0)"#, vec![]),
        (r#"INSERT INTO cd VALUES (1, 5, 5)"#, vec![]),
        (r#"INSERT INTO gen (q, p) VALUES (2, 3)"#, vec![]),
        (r#"UPDATE co SET a = -9 WHERE z = 12345"#, vec![]),
        (r#"UPDATE co SET a = -9 WHERE false"#, vec![]),
        (r#"UPDATE ck2 SET a = -1 WHERE b = 777"#, vec![]),
        (r#"UPDATE co SET z = -1, a = -1 WHERE z > 1000 RETURNING z"#, vec![]),
        (r#"UPDATE cd SET p = -1 WHERE id = 99"#, vec![]),
        (r#"UPDATE gen SET q = 500 WHERE p = 99"#, vec![]),
        (r#"SELECT * FROM co"#, vec![r#"5|5"#]),
        (r#"SELECT * FROM ck2"#, vec![r#"1|0"#]),
        (r#"SELECT * FROM gen"#, vec![r#"2|3|6"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(run(&pool, sql).await, Ok(expected.iter().map(|r| r.to_string()).collect()), "{sql}");
    }

    // Rows that break one check or several, on INSERT and on UPDATE, with
    // constants, expressions of the row, a domain and a generated column.
    let cases: Vec<(&str, Result<Vec<&str>, &str>)> = vec![
        (r#"INSERT INTO ck2 VALUES (5, 2000)"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_b_check""#)),
        (r#"INSERT INTO ck2 VALUES (5000, 2000)"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_b_check""#)),
        (r#"INSERT INTO ck2 VALUES (-5, 5000)"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_a_check""#)),
        (r#"INSERT INTO ck2 VALUES (1, 0), (2, 1), (-1, 3000)"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_a_check""#)),
        (r#"INSERT INTO co VALUES (-1, -1)"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"INSERT INTO co VALUES (500, -1)"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"INSERT INTO co VALUES (-1, 5)"#, Err(r#"new row for relation "co" violates check constraint "zz""#)),
        (r#"INSERT INTO co VALUES (5, -1)"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"INSERT INTO cd VALUES (1, -1, -1)"#, Err(r#"value for domain posint violates check constraint "posint_check""#)),
        (r#"INSERT INTO cd VALUES (1, 5, -1)"#, Err(r#"new row for relation "cd" violates check constraint "a_first""#)),
        (r#"UPDATE co SET z = -1, a = -1"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"UPDATE co SET a = -9"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"UPDATE co SET z = z - 600, a = a - 10"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"UPDATE co SET z = 500, a = a - 10"#, Err(r#"new row for relation "co" violates check constraint "Mid""#)),
        (r#"UPDATE co SET z = -3"#, Err(r#"new row for relation "co" violates check constraint "zz""#)),
        (r#"UPDATE ck2 SET a = -5, b = 5000"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_a_check""#)),
        (r#"UPDATE ck2 SET b = b + 5000"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_b_check""#)),
        (r#"UPDATE ck2 SET a = 5000"#, Err(r#"new row for relation "ck2" violates check constraint "ck2_check1""#)),
        (r#"UPDATE cd SET p = -1, n = -1"#, Err(r#"value for domain posint violates check constraint "posint_check""#)),
        (r#"UPDATE cd SET n = -1"#, Err(r#"new row for relation "cd" violates check constraint "a_first""#)),
        (r#"UPDATE gen SET q = 500, p = -1"#, Err(r#"new row for relation "gen" violates check constraint "q_small""#)),
        (r#"UPDATE gen SET p = -1"#, Err(r#"new row for relation "gen" violates check constraint "tot_pos""#)),
        (r#"UPDATE gen SET q = -4"#, Err(r#"new row for relation "gen" violates check constraint "tot_pos""#)),
        (r#"UPDATE co SET a = 7 RETURNING z, a"#, Ok(vec![r#"5|7"#])),
        (r#"UPDATE ck2 SET a = 50, b = 20 RETURNING a, b"#, Ok(vec![r#"50|20"#])),
        (r#"UPDATE gen SET q = 9 RETURNING tot"#, Ok(vec![r#"27"#])),
        (r#"SELECT * FROM co"#, Ok(vec![r#"5|7"#])),
        (r#"SELECT * FROM ck2"#, Ok(vec![r#"50|20"#])),
        (r#"SELECT * FROM cd"#, Ok(vec![r#"1|5|5"#])),
        (r#"SELECT * FROM gen"#, Ok(vec![r#"9|3|27"#])),
    ];
    for (sql, expected) in cases {
        match (run(&pool, sql).await, expected) {
            (Ok(rows), Ok(expected)) => assert_eq!(rows, expected, "{sql}"),
            (Err((code, message)), Err(expected)) => {
                assert_eq!(message, expected, "{sql}");
                assert_eq!(code, "23514", "{sql}");
            }
            (got, expected) => panic!("`{sql}`: got {got:?}, PostgreSQL answers {expected:?}"),
        }
    }
}

/// A check on a constant assignment is judged with the first row written
/// and not again: a statement over many rows still fails as a whole when it
/// breaks, and writes every row when it holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_constant_check_holds_or_fails_for_every_row_of_the_statement() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE many (id int PRIMARY KEY, status int CHECK (status BETWEEN 1 AND 10), lo int, hi int, CHECK (lo < hi))",
        "INSERT INTO many SELECT g, 1, g, g + 5 FROM generate_series(1, 500) g",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    let broken = Err(("23514".to_string(), r#"new row for relation "many" violates check constraint "many_status_check""#.to_string()));
    assert_eq!(run(&pool, "UPDATE many SET status = 99").await, broken);
    assert_eq!(run(&pool, "SELECT count(*), min(status), max(status) FROM many").await, Ok(vec!["500|1|1".to_string()]));
    // The constant holds; the other check is each row's own, and the rows
    // from 400 on break it (`lo` is the id, against a `hi` of 400). Nothing
    // of the statement stays: row 395 had that `hi` before.
    assert_eq!(run(&pool, "UPDATE many SET status = 7 WHERE id <= 300").await, Ok(vec![]));
    assert_eq!(run(&pool, "SELECT count(*) FROM many WHERE status = 7").await, Ok(vec!["300".to_string()]));
    let broken = Err(("23514".to_string(), r#"new row for relation "many" violates check constraint "many_check""#.to_string()));
    assert_eq!(run(&pool, "UPDATE many SET status = 3, hi = 400").await, broken);
    assert_eq!(run(&pool, "SELECT count(*), count(*) FILTER (WHERE hi = 400) FROM many WHERE status = 3 OR hi = 400").await, Ok(vec!["1|1".to_string()]));
    assert_eq!(run(&pool, "SELECT count(*) FROM many WHERE status = 3").await, Ok(vec!["0".to_string()]));
    assert_eq!(run(&pool, "UPDATE many SET status = 3, hi = 400 WHERE id < 400").await, Ok(vec![]));
    assert_eq!(run(&pool, "SELECT count(*), max(hi) FROM many WHERE status = 3").await, Ok(vec!["399|400".to_string()]));
    // Nothing matches: no check is broken, whatever the assignment.
    assert_eq!(run(&pool, "UPDATE many SET status = 99, hi = -1 WHERE id > 100000").await, Ok(vec![]));
}
