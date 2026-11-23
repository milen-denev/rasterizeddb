//! Engine bugs the client crate's ORM work found (2026-11-17), each checked
//! against PostgreSQL 17 and pinned here through a pgwire session.

use super::owner_tests::{Session, ok, session};
use super::*;
use tokio::io::AsyncReadExt;

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

fn sqlstate_of(error: &str) -> &str {
    error
        .split('\0')
        .find_map(|field| field.strip_prefix('C'))
        .unwrap_or("")
}

type Rows = Vec<Vec<Option<String>>>;

/// Runs `sql` and returns its rows as text (`None` for NULL), or the error.
async fn rows(db: &Arc<Database>, s: &mut Session, sql: &str) -> Result<Rows, String> {
    Box::pin(execute_recorded(
        db,
        &mut s.state,
        &mut s.server,
        sql,
        &[],
        false,
        None,
    ))
    .await
    .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        row.push(None);
                    } else {
                        let l = l as usize;
                        row.push(Some(
                            String::from_utf8_lossy(&body[at..at + l]).into_owned(),
                        ));
                        at += l;
                    }
                }
                out.push(row);
            }
            b'C' => return Ok(out),
            b'E' => return Err(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
}

async fn one(db: &Arc<Database>, s: &mut Session, sql: &str) -> Rows {
    rows(db, s, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn r(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_string)).collect()
}

/// A per-row NULL assigned to a `NOT NULL` column is 23502, whatever
/// produced it. Only a constant NULL was refused; `SET n = m`, `n + m`,
/// `NULLIF` and a `CASE` that is NULL for one row stored the type's zero
/// (or `''`) and reported success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_null_into_a_not_null_column_is_refused() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE n1 (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, m BIGINT, s TEXT NOT NULL, t TEXT)").await;
    ok(
        &db,
        &mut s,
        "INSERT INTO n1 VALUES (1, 5, NULL, 'a', NULL), (2, 6, 7, 'b', 'x')",
    )
    .await;

    for sql in [
        "UPDATE n1 SET n = m WHERE id = 1",
        "UPDATE n1 SET n = n + m",
        "UPDATE n1 SET n = NULLIF(n, 5)",
        "UPDATE n1 SET n = CASE WHEN id = 1 THEN NULL ELSE n END",
        "UPDATE n1 SET s = t",
        "UPDATE n1 SET s = CASE WHEN id = 1 THEN NULL ELSE s END",
    ] {
        let e = rows(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), "23502", "{sql}: {e}");
    }
    // Nothing of a refused statement was written, not even the rows before
    // the one that failed.
    assert_eq!(
        one(&db, &mut s, "SELECT id, n, s FROM n1 ORDER BY id").await,
        vec![
            r(&[Some("1"), Some("5"), Some("a")]),
            r(&[Some("2"), Some("6"), Some("b")])
        ]
    );

    // ON CONFLICT … DO UPDATE goes the same way.
    let e = rows(
        &db,
        &mut s,
        "INSERT INTO n1 VALUES (1, 1, 1, 'z', NULL) ON CONFLICT (id) DO UPDATE SET n = n1.m",
    )
    .await
    .expect_err("conflict update to NULL");
    assert_eq!(sqlstate_of(&e), "23502", "{e}");

    // A nullable column still takes a computed NULL, and a NOT NULL one a
    // computed value.
    assert_eq!(
        one(&db, &mut s, "UPDATE n1 SET t = CASE WHEN id = 2 THEN NULL END, n = COALESCE(m, 0) + 1 RETURNING id, n, t").await.len(),
        2
    );
    assert_eq!(
        one(&db, &mut s, "SELECT id, n, t FROM n1 ORDER BY id").await,
        vec![
            r(&[Some("1"), Some("1"), None]),
            r(&[Some("2"), Some("8"), None])
        ]
    );
}

/// A literal reaching an UPDATE through an expression (a `CASE` branch, a
/// `COALESCE` fallback) is read by the column's type, as the same literal
/// written directly is. A bytea stored the escape's characters (`'\x0909'`
/// as six bytes) and an array column read back as NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_literal_through_an_expression_is_read_as_the_column_type() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE b2 (id BIGINT PRIMARY KEY, b BYTEA, a TEXT[], ia INT[], ip INET, r INT4RANGE)").await;
    ok(&db, &mut s, "INSERT INTO b2 (id) VALUES (1), (2)").await;
    ok(
        &db,
        &mut s,
        "UPDATE b2 SET b = CASE WHEN id = 1 THEN '\\x0909' ELSE b END, a = CASE WHEN id = 1 THEN '{x,y}' ELSE a END, \
         ia = CASE WHEN id = 1 THEN '{1,2,3}' ELSE ia END, ip = CASE WHEN id = 1 THEN '10.0.0.1/8' ELSE ip END, \
         r = CASE WHEN id = 1 THEN '[1,5)' ELSE r END",
    )
    .await;
    assert_eq!(
        one(
            &db,
            &mut s,
            "SELECT id, length(b), b, a[2], ia[3], ip, r FROM b2 ORDER BY id"
        )
        .await,
        vec![
            r(&[
                Some("1"),
                Some("2"),
                Some("\\x0909"),
                Some("y"),
                Some("3"),
                Some("10.0.0.1/8"),
                Some("[1,5)")
            ]),
            r(&[Some("2"), None, None, None, None, None, None]),
        ]
    );
    ok(
        &db,
        &mut s,
        "UPDATE b2 SET b = COALESCE(b, '\\x01'), a = COALESCE(a, '{q}')",
    )
    .await;
    assert_eq!(
        one(&db, &mut s, "SELECT b, a FROM b2 WHERE id = 2").await,
        vec![r(&[Some("\\x01"), Some("{q}")])]
    );
    // What the type's input refuses is refused, and nothing is written.
    for (sql, state) in [
        (
            "UPDATE b2 SET b = CASE WHEN id = 1 THEN '\\xzz' ELSE b END",
            "22023",
        ),
        (
            "UPDATE b2 SET ia = CASE WHEN id = 1 THEN '{1,x}' ELSE ia END",
            "22P02",
        ),
    ] {
        let e = rows(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), state, "{sql}: {e}");
    }
    assert_eq!(
        one(&db, &mut s, "SELECT b, ia[1] FROM b2 WHERE id = 1").await,
        vec![r(&[Some("\\x0909"), Some("1")])]
    );
}

/// `ADD CONSTRAINT IF NOT EXISTS name FOREIGN KEY …` adds the key under its
/// own name, and a second run is a no-op. `IF NOT EXISTS` was taken out only
/// for UNIQUE: a foreign key read `IF` as its name, so a live key called "if"
/// was stored and the second run failed as a duplicate of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_constraint_if_not_exists_names_a_foreign_key_and_a_check_properly() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE p3 (id BIGINT PRIMARY KEY)").await;
    ok(
        &db,
        &mut s,
        "CREATE TABLE c3 (id BIGINT PRIMARY KEY, p BIGINT, v BIGINT)",
    )
    .await;
    ok(&db, &mut s, "INSERT INTO p3 VALUES (1)").await;
    for _ in 0..2 {
        ok(&db, &mut s, "ALTER TABLE c3 ADD CONSTRAINT IF NOT EXISTS c3_p_fk FOREIGN KEY (p) REFERENCES p3 (id)").await;
        ok(
            &db,
            &mut s,
            "ALTER TABLE c3 ADD CONSTRAINT IF NOT EXISTS c3_v_chk CHECK (v > 0)",
        )
        .await;
    }
    // Without IF NOT EXISTS, the name is taken.
    let e = rows(
        &db,
        &mut s,
        "ALTER TABLE c3 ADD CONSTRAINT c3_p_fk FOREIGN KEY (p) REFERENCES p3 (id)",
    )
    .await
    .expect_err("a second key under the name");
    assert_eq!(sqlstate_of(&e), "42710", "{e}");

    let e = rows(&db, &mut s, "INSERT INTO c3 VALUES (1, 99, 1)")
        .await
        .expect_err("an orphan");
    assert_eq!(sqlstate_of(&e), "23503", "{e}");
    assert!(e.contains("\"c3_p_fk\""), "{e}");
    let e = rows(&db, &mut s, "INSERT INTO c3 VALUES (2, 1, -1)")
        .await
        .expect_err("a failed check");
    assert_eq!(sqlstate_of(&e), "23514", "{e}");
    assert!(e.contains("\"c3_v_chk\""), "{e}");
    ok(&db, &mut s, "INSERT INTO c3 VALUES (3, 1, 1)").await;

    // There is no constraint called "if", and each one drops by its name.
    let e = rows(&db, &mut s, "ALTER TABLE c3 DROP CONSTRAINT \"if\"")
        .await
        .expect_err("no such constraint");
    assert_eq!(sqlstate_of(&e), "42704", "{e}");
    ok(&db, &mut s, "ALTER TABLE c3 DROP CONSTRAINT c3_p_fk").await;
    ok(&db, &mut s, "ALTER TABLE c3 DROP CONSTRAINT c3_v_chk").await;
    ok(&db, &mut s, "INSERT INTO c3 VALUES (4, 99, -1)").await;
}

/// A key the block wrote itself, written again inside a savepoint, is a
/// duplicate (23505). The uniqueness check compared the row's writer with
/// the savepoint's id, took the block's own row for another transaction's
/// write in flight, and answered 40001, which a retrying caller replayed for
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_of_the_blocks_own_key_inside_a_savepoint_is_23505() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE b4 (id BIGINT PRIMARY KEY, u BIGINT UNIQUE, a BIGINT, b BIGINT, UNIQUE (a, b))").await;
    for (setup, duplicate) in [
        ("SAVEPOINT s", "INSERT INTO b4 VALUES (1, 2, 2, 2)"),
        (
            "SAVEPOINT s; INSERT INTO b4 VALUES (2, 2, 2, 2); ROLLBACK TO s",
            "INSERT INTO b4 VALUES (1, 3, 3, 3)",
        ),
        (
            "SAVEPOINT s; INSERT INTO b4 VALUES (2, 2, 2, 2); RELEASE s",
            "INSERT INTO b4 VALUES (2, 3, 3, 3)",
        ),
        (
            "SAVEPOINT s; SAVEPOINT t",
            "INSERT INTO b4 VALUES (4, 1, 4, 4)",
        ),
        ("SAVEPOINT s", "INSERT INTO b4 VALUES (3, 3, 1, 1)"),
    ] {
        ok(&db, &mut s, "BEGIN").await;
        ok(&db, &mut s, "INSERT INTO b4 VALUES (1, 1, 1, 1)").await;
        for sql in setup.split("; ") {
            ok(&db, &mut s, sql).await;
        }
        let e = rows(&db, &mut s, duplicate).await.expect_err(duplicate);
        assert_eq!(sqlstate_of(&e), "23505", "{setup}; {duplicate}: {e}");
        ok(&db, &mut s, "ROLLBACK").await;
    }

    // A key a rolled-back savepoint wrote is free again, and ON CONFLICT
    // inside a savepoint sees the block's own row.
    ok(&db, &mut s, "BEGIN").await;
    ok(&db, &mut s, "INSERT INTO b4 VALUES (1, 1, 1, 1)").await;
    ok(&db, &mut s, "SAVEPOINT s").await;
    ok(&db, &mut s, "INSERT INTO b4 VALUES (2, 2, 2, 2)").await;
    ok(&db, &mut s, "ROLLBACK TO s").await;
    ok(&db, &mut s, "INSERT INTO b4 VALUES (2, 2, 2, 2)").await;
    ok(
        &db,
        &mut s,
        "INSERT INTO b4 VALUES (1, 6, 6, 6) ON CONFLICT (id) DO UPDATE SET u = 70",
    )
    .await;
    ok(&db, &mut s, "COMMIT").await;
    assert_eq!(
        one(&db, &mut s, "SELECT id, u FROM b4 ORDER BY id").await,
        vec![r(&[Some("1"), Some("70")]), r(&[Some("2"), Some("2")])]
    );
}

/// `ROLLBACK TO` or `RELEASE` of a savepoint that does not exist fails the
/// block (3B001), as in PostgreSQL; it was answered and the block went on, so
/// its COMMIT committed. In a failed block `SAVEPOINT` and `RELEASE` are
/// refused (25P02): a RELEASE run there merged away the savepoint a ROLLBACK
/// TO would have revived the block from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_savepoint_fails_the_block() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE b5 (id BIGINT PRIMARY KEY)").await;

    for unknown in [
        "ROLLBACK TO nosuch",
        "RELEASE nosuch",
        "RELEASE SAVEPOINT nosuch",
    ] {
        ok(&db, &mut s, "BEGIN").await;
        ok(&db, &mut s, "INSERT INTO b5 VALUES (1)").await;
        let e = rows(&db, &mut s, unknown).await.expect_err(unknown);
        assert_eq!(sqlstate_of(&e), "3B001", "{unknown}: {e}");
        let e = rows(&db, &mut s, "SELECT 1")
            .await
            .expect_err("a failed block");
        assert_eq!(sqlstate_of(&e), "25P02", "{unknown}: {e}");
        ok(&db, &mut s, "COMMIT").await;
        assert_eq!(
            one(&db, &mut s, "SELECT count(*) FROM b5").await,
            vec![r(&[Some("0")])],
            "{unknown}"
        );
    }

    // Under a savepoint, ROLLBACK TO it revives the block.
    ok(&db, &mut s, "BEGIN").await;
    ok(&db, &mut s, "INSERT INTO b5 VALUES (1)").await;
    ok(&db, &mut s, "SAVEPOINT a").await;
    ok(&db, &mut s, "INSERT INTO b5 VALUES (2)").await;
    rows(&db, &mut s, "RELEASE nosuch")
        .await
        .expect_err("unknown");
    for refused in ["RELEASE a", "SAVEPOINT b"] {
        let e = rows(&db, &mut s, refused).await.expect_err(refused);
        assert_eq!(sqlstate_of(&e), "25P02", "{refused}: {e}");
    }
    ok(&db, &mut s, "ROLLBACK TO a").await;
    ok(&db, &mut s, "COMMIT").await;
    assert_eq!(
        one(&db, &mut s, "SELECT id FROM b5").await,
        vec![r(&[Some("1")])]
    );

    // Outside a block, each is 25P01.
    for sql in ["SAVEPOINT a", "ROLLBACK TO a", "RELEASE a"] {
        let e = rows(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), "25P01", "{sql}: {e}");
    }
}

/// `now()` over the native protocol is the statement's start under
/// autocommit, one instant for every call: with no statement clock set,
/// each call read the wall clock, so `INSERT … VALUES (now(), now())` stored
/// two different times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_now_is_one_instant_per_statement() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE t6 (id BIGINT PRIMARY KEY, a TIMESTAMPTZ, b TIMESTAMPTZ, c TIMESTAMPTZ DEFAULT now())").await;
    for id in 1..=20 {
        let out = crate::core::database::run_native_query_for_tests(
            &format!("INSERT INTO t6 (id, a, b) VALUES ({id}, now(), now())"),
            db.clone(),
        )
        .await;
        assert_ne!(out.first(), Some(&3), "{}", String::from_utf8_lossy(&out));
    }
    assert_eq!(
        one(&db, &mut s, "SELECT count(*) FROM t6 WHERE a = b AND a = c").await,
        vec![r(&[Some("20")])]
    );
}

/// A scalar subquery folded into a constant keeps its type in a select list:
/// written as quoted text, `(SELECT now()) = now()` compared a string with a
/// timestamp and was false, a date or interval could not take part in
/// arithmetic, a boolean was the number 1, and a `timestamp` was refused.
/// `CURRENT_DATE` is a `date` (it was a midnight timestamptz), and
/// `interval + timestamp` is a timestamp, as the other order was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folded_subquery_keeps_its_type_and_current_date_is_a_date() {
    let db = db().await;
    let mut s = session(&db);
    let t = Some("t");
    for sql in [
        "SELECT (SELECT now()) = now()",
        "SELECT (SELECT now()) - now() = interval '0 seconds'",
        "SELECT (SELECT now()::timestamp) = now()::timestamp",
        "SELECT (SELECT '2024-01-01'::date) = '2024-01-01'::date",
        "SELECT (SELECT current_date) + 1 = current_date + 1",
        "SELECT (SELECT interval '1 day') + now() > now()",
        "SELECT (SELECT true) AND true",
        "SELECT (SELECT '10:00'::time) < '11:00'::time",
        "SELECT current_date = now()::date",
        "SELECT '1 day'::interval + '2024-01-01 00:00+00'::timestamptz = '2024-01-02 00:00+00'::timestamptz",
    ] {
        assert_eq!(one(&db, &mut s, sql).await, vec![r(&[t])], "{sql}");
    }
    assert_eq!(
        one(
            &db,
            &mut s,
            "SELECT pg_typeof((SELECT now())), pg_typeof(current_date), pg_typeof((SELECT true))"
        )
        .await,
        vec![r(&[
            Some("timestamp with time zone"),
            Some("date"),
            Some("boolean")
        ])]
    );
    // A stored `timestamp` is the same eight bytes as a `timestamptz`: read
    // through a subquery it stays a `timestamp`, shown with no offset, bare,
    // under max/min, and through a join (left untyped there).
    ok(
        &db,
        &mut s,
        "CREATE TABLE tz (id BIGINT PRIMARY KEY, t TIMESTAMP, z TIMESTAMPTZ)",
    )
    .await;
    ok(
        &db,
        &mut s,
        "INSERT INTO tz VALUES (1, '2024-01-01 10:00', '2024-01-01 10:00+00')",
    )
    .await;
    assert_eq!(
        one(
            &db,
            &mut s,
            "SELECT (SELECT t FROM tz), (SELECT max(t) FROM tz), (SELECT max(z) FROM tz), \
             (SELECT a.t FROM tz a JOIN tz b ON a.id = b.id), pg_typeof((SELECT min(t) FROM tz))"
        )
        .await,
        vec![r(&[
            Some("2024-01-01 10:00:00"),
            Some("2024-01-01 10:00:00"),
            Some("2024-01-01 10:00:00+00"),
            Some("2024-01-01 10:00:00"),
            Some("timestamp without time zone"),
        ])]
    );
    // A predicate's subquery stays an untyped constant, read as the column.
    ok(
        &db,
        &mut s,
        "CREATE TABLE t9 (id BIGINT PRIMARY KEY, d DATE, ts TIMESTAMPTZ, b BOOLEAN)",
    )
    .await;
    ok(&db, &mut s, "INSERT INTO t9 VALUES (1, '2024-01-01', '2024-01-01 10:00+00', true), (2, '2024-02-01', '2024-02-01 10:00+00', false)").await;
    for (sql, want) in [
        ("SELECT id FROM t9 WHERE d = (SELECT max(d) FROM t9)", "2"),
        ("SELECT id FROM t9 WHERE ts < (SELECT max(ts) FROM t9)", "1"),
        (
            "SELECT id FROM t9 WHERE b = (SELECT b FROM t9 WHERE id = 1)",
            "1",
        ),
        (
            "SELECT (SELECT max(d) FROM t9) - d FROM t9 WHERE id = 1",
            "31",
        ),
    ] {
        assert_eq!(one(&db, &mut s, sql).await, vec![r(&[Some(want)])], "{sql}");
    }
}

/// A constant NULL test or a comparison with NULL anywhere in a WHERE
/// clause answers as PostgreSQL does. The optional-filter pattern `($1 IS
/// NULL OR col = $1)`, once a parameter is inlined, was refused for a NULL
/// value (0A000 "NULL literals are not supported yet") and a syntax error at
/// `IS` for any other. Found by the MySQL/SQL Server front ends.
#[tokio::test(flavor = "multi_thread")]
async fn null_constants_in_a_where_clause_answer_as_postgresql_does() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE pn (id INT PRIMARY KEY, customer TEXT, n INT)").await;
    ok(&db, &mut s, "INSERT INTO pn VALUES (1,'a',1),(2,NULL,NULL),(3,'b',3)").await;
    // Each count checked against PostgreSQL 17's answer for these rows.
    for (condition, expected) in [
        ("(customer = NULL OR (NULL IS NULL))", "3"),
        ("NULL IS NULL", "3"),
        ("NULL IS NOT NULL", "0"),
        ("('a' IS NULL OR customer = 'a')", "1"),
        ("('a' IS NOT NULL AND customer = 'a')", "1"),
        ("(5 IS NULL OR n = 5)", "0"),
        ("(NULL IS NULL OR n = NULL)", "3"),
        ("(NULL::text IS NULL OR customer = NULL::text)", "3"),
        ("(NULL::int4 IS NULL OR n = NULL::int4)", "3"),
        ("('a'::text IS NOT NULL AND customer = 'a'::text)", "1"),
        ("customer IS NULL OR NULL IS NOT NULL", "1"),
        ("NOT (NULL IS NULL)", "0"),
        ("NOT NULL IS NULL", "0"),
        ("id = 1 AND NOT ('a' IS NULL)", "1"),
        ("(NULL IS NULL) IS TRUE", "3"),
        // Comparisons with NULL are unknown, whichever side and operator.
        ("NULL = customer", "0"),
        ("NULL = NULL", "0"),
        ("n > NULL", "0"),
        ("n >= NULL OR id = 1", "1"),
        ("customer = NULL::text", "0"),
        ("NOT (NULL = customer)", "0"),
        ("NOT (customer = NULL::text)", "0"),
        ("(customer = NULL) IS UNKNOWN", "3"),
        ("NULL", "0"),
        // NULL as a value, not a condition, is left to what reads it.
        ("customer IN (NULL)", "0"),
        ("customer IS NOT NULL", "2"),
        ("coalesce(customer, NULL) = 'a'", "1"),
        ("customer = 'x NULL IS NULL'", "0"),
    ] {
        let sql = format!("SELECT count(*) FROM pn WHERE {condition}");
        assert_eq!(one(&db, &mut s, &sql).await, vec![r(&[Some(expected)])], "{sql}");
    }
    // The same through UPDATE and DELETE.
    ok(&db, &mut s, "UPDATE pn SET n = 9 WHERE (NULL IS NULL OR customer = NULL) AND id = 3").await;
    assert_eq!(one(&db, &mut s, "SELECT n FROM pn WHERE id = 3").await, vec![r(&[Some("9")])]);
    ok(&db, &mut s, "DELETE FROM pn WHERE NULL::text IS NOT NULL OR id = 99").await;
    assert_eq!(one(&db, &mut s, "SELECT count(*) FROM pn").await, vec![r(&[Some("3")])]);
}

/// An unknown column under an alias is 42703 naming the column, as without
/// one: `nope AS "Nope"` was 42601 naming `nope AS "Nope"`.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_under_an_alias_is_undefined_column() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE ua (id INT PRIMARY KEY)").await;
    for (sql, name) in [
        // In PostgreSQL's words (`error_position::pg_wording`).
        ("SELECT nope AS \"Nope\" FROM ua", "column \"nope\" does not exist"),
        ("SELECT id, nope AS x FROM ua", "column \"nope\" does not exist"),
        ("SELECT nope x FROM ua", "column \"nope\" does not exist"),
        ("SELECT ua.nope AS x FROM ua", "column ua.nope does not exist"),
        ("SELECT nope FROM ua", "column \"nope\" does not exist"),
    ] {
        let e = rows(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), "42703", "{sql}: {e}");
        assert!(e.contains(name) && !e.contains(" AS ") && !e.contains("nope x"), "{sql}: {e}");
    }
}

/// An optional filter with a NULL parameter, after the first conjunct:
/// `x AND (col = NULL OR …)` failed to compile ("unexpected RPar after the
/// end of the expression", 42601). SqlClient's `GetSchema("Tables")` sends
/// exactly this shape with its NULL restrictions.
#[tokio::test(flavor = "multi_thread")]
async fn an_optional_filter_after_the_first_conjunct_compiles() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE nt (a TEXT, b TEXT)").await;
    ok(&db, &mut s, "INSERT INTO nt VALUES ('x', 'p'), ('y', NULL)").await;
    for (condition, expected) in [
        ("a = 'x' and (a = NULL or a = 'x')", "1"),
        ("(a = NULL or (1 = 1)) and (a = NULL or (1 = 1))", "2"),
        ("(a = NULL or (NULL is null)) and (a = NULL or (NULL is null))", "2"),
        ("(a = NULL or a = 'x') and a = 'x'", "1"),
        ("(a = NULL or a = 'x') and (a = NULL or a = 'x')", "1"),
        ("(a = 'x' or (NULL is null)) and (a = 'x' or (NULL is null))", "2"),
        ("(a = NULL or (NULL is null)) and (b = NULL or (NULL is null)) and (a = 'y' or ('y' is null))", "1"),
        ("(a = 'x' or ('x' is null)) and (b = NULL or (NULL is null))", "1"),
    ] {
        let sql = format!("SELECT count(*) FROM nt WHERE {condition}");
        assert_eq!(one(&db, &mut s, &sql).await, vec![r(&[Some(expected)])], "{sql}");
    }
}

/// A correlated scalar subquery inside a derived table or a CTE body is
/// evaluated per outer row, as at the top level. Its text reached the
/// select-list parser instead, which read it as an expression over the
/// outer row: `'t'` for every row, `max(ms.id)` under an aggregate, the
/// outer ids under `string_agg` — no error. Found by the SQL Server front
/// end. Each expected value is PostgreSQL 17's for these rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_subquery_inside_a_derived_table_reads_each_row() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE mt (id int PRIMARY KEY, v text)").await;
    ok(&db, &mut s, "INSERT INTO mt VALUES (1,'A'),(2,'gone')").await;
    ok(&db, &mut s, "CREATE TABLE ms (id int, v text)").await;
    ok(&db, &mut s, "INSERT INTO ms VALUES (1,'A'),(3,'C')").await;
    let cases: [(&str, Rows); 9] = [
        (
            "SELECT j FROM (SELECT (SELECT max(mt.v) FROM mt WHERE mt.id = ms.id) AS j FROM ms) AS z ORDER BY j",
            vec![r(&[Some("A")]), r(&[None])],
        ),
        (
            "SELECT j FROM (SELECT max((SELECT max(mt.v) FROM mt WHERE mt.id = ms.id)) AS j FROM ms) AS z",
            vec![r(&[Some("A")])],
        ),
        (
            "SELECT j FROM (SELECT string_agg('{' || (SELECT string_agg(mt.v, ',') FROM mt WHERE mt.id = ms.id) || '}', ',') AS j FROM ms) AS z",
            vec![r(&[Some("{A}")])],
        ),
        (
            "WITH z AS (SELECT (SELECT max(mt.v) FROM mt WHERE mt.id = ms.id) AS j FROM ms) SELECT j FROM z ORDER BY j",
            vec![r(&[Some("A")]), r(&[None])],
        ),
        (
            "SELECT z.id, z.j FROM (SELECT ms.id, (SELECT count(*) FROM mt WHERE mt.id = ms.id) AS j FROM ms) z ORDER BY z.id",
            vec![r(&[Some("1"), Some("1")]), r(&[Some("3"), Some("0")])],
        ),
        (
            "SELECT * FROM (SELECT * FROM (SELECT ms.id, (SELECT max(mt.v) FROM mt WHERE mt.id = ms.id) AS j FROM ms) a) b ORDER BY id",
            vec![r(&[Some("1"), Some("A")]), r(&[Some("3"), None])],
        ),
        // A derived table inside EXISTS or IN: refused as a comparison that
        // was not `=`, or "Table not found", before.
        (
            "SELECT ms.id FROM ms WHERE EXISTS (SELECT 1 FROM (SELECT id FROM mt) AS t WHERE t.id = ms.id)",
            vec![r(&[Some("1")])],
        ),
        (
            "SELECT ms.id FROM ms WHERE NOT EXISTS (SELECT 1 FROM (VALUES (1),(2)) AS t(id) WHERE t.id = ms.id)",
            vec![r(&[Some("3")])],
        ),
        (
            "SELECT ms.id FROM ms WHERE ms.id IN (SELECT t.id FROM (SELECT id FROM mt) AS t)",
            vec![r(&[Some("1")])],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(one(&db, &mut s, sql).await, expected, "{sql}");
    }
    // Grouped by the column it reads: one value per group (refused before).
    assert_eq!(
        one(&db, &mut s, "SELECT j FROM (SELECT (SELECT max(mt.v) FROM mt WHERE mt.id = ms.id) AS j FROM ms GROUP BY ms.id) z ORDER BY j")
            .await,
        vec![r(&[Some("A")]), r(&[None])]
    );
}

/// A correlated scalar subquery in a grouped SELECT (refused, 0A000,
/// before): once per group in the select list, `HAVING` and `ORDER BY`,
/// where it may read only the outer columns the query is grouped by;
/// once per row in the `WHERE`, an aggregate's argument and a `FILTER`.
/// Every answer is PostgreSQL 17's.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_subquery_in_a_grouped_select_answers_as_postgresql_does() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE gt (id int PRIMARY KEY, k int, d text)").await;
    ok(&db, &mut s, "INSERT INTO gt VALUES (1,1,'a'),(2,1,'b'),(3,2,'a'),(4,3,'c'),(5,NULL,'a')").await;
    ok(&db, &mut s, "CREATE TABLE gu (k int, v int)").await;
    ok(&db, &mut s, "INSERT INTO gu VALUES (1,10),(1,20),(2,5),(4,7)").await;
    let cases: [(&str, Rows); 9] = [
        // A key-only HAVING term moves into the WHERE, beside one with OR.
        (
            "SELECT k FROM gt WHERE id > 1 OR id < 0 GROUP BY k \
             HAVING (SELECT max(v) FROM gu WHERE gu.k = gt.k) > 6 AND count(*) > 0 ORDER BY k",
            vec![r(&[Some("1")])],
        ),
        (
            "SELECT k, count(*), (SELECT max(v) FROM gu WHERE gu.k = gt.k) FROM gt GROUP BY k ORDER BY k",
            vec![
                r(&[Some("1"), Some("2"), Some("20")]),
                r(&[Some("2"), Some("1"), Some("5")]),
                r(&[Some("3"), Some("1"), None]),
                r(&[None, Some("1"), None]),
            ],
        ),
        (
            "SELECT gt.k, (SELECT sum(v) FROM gu WHERE gu.k = gt.k) AS s FROM gt GROUP BY gt.k \
             HAVING (SELECT count(*) FROM gu WHERE gu.k = gt.k) > 0 ORDER BY gt.k",
            vec![r(&[Some("1"), Some("30")]), r(&[Some("2"), Some("5")])],
        ),
        (
            "SELECT d, sum((SELECT max(v) FROM gu WHERE gu.k = gt.k)) FROM gt GROUP BY d ORDER BY d",
            vec![r(&[Some("a"), Some("25")]), r(&[Some("b"), Some("20")]), r(&[Some("c"), None])],
        ),
        (
            "SELECT d, count(*) FROM gt WHERE (SELECT max(v) FROM gu WHERE gu.k = gt.k) > 6 GROUP BY d ORDER BY d",
            vec![r(&[Some("a"), Some("1")]), r(&[Some("b"), Some("1")])],
        ),
        (
            "SELECT k, d, (SELECT count(*) FROM gu WHERE gu.k = gt.k) AS c FROM gt GROUP BY k, d ORDER BY k, d",
            vec![
                r(&[Some("1"), Some("a"), Some("2")]),
                r(&[Some("1"), Some("b"), Some("2")]),
                r(&[Some("2"), Some("a"), Some("1")]),
                r(&[Some("3"), Some("c"), Some("0")]),
                r(&[None, Some("a"), Some("0")]),
            ],
        ),
        (
            "SELECT k, (SELECT max(v) FROM gu WHERE gu.k = gt.k) AS m FROM gt GROUP BY k ORDER BY m NULLS FIRST, k",
            vec![
                r(&[Some("3"), None]),
                r(&[None, None]),
                r(&[Some("2"), Some("5")]),
                r(&[Some("1"), Some("20")]),
            ],
        ),
        (
            "SELECT k, max(id), (SELECT max(v) FROM gu WHERE gu.k = gt.k) AS m FROM gt GROUP BY k \
             HAVING count(*) >= 1 ORDER BY k",
            vec![
                r(&[Some("1"), Some("2"), Some("20")]),
                r(&[Some("2"), Some("3"), Some("5")]),
                r(&[Some("3"), Some("4"), None]),
                r(&[None, Some("5"), None]),
            ],
        ),
        (
            "SELECT d, count(*) FILTER (WHERE (SELECT max(v) FROM gu WHERE gu.k = gt.k) > 6) FROM gt GROUP BY d ORDER BY d",
            vec![r(&[Some("a"), Some("1")]), r(&[Some("b"), Some("1")]), r(&[Some("c"), Some("0")])],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(one(&db, &mut s, sql).await, expected, "{sql}");
    }
    // Reading a column the query is not grouped by: 42803, as PostgreSQL.
    for sql in [
        "SELECT d, (SELECT max(v) FROM gu WHERE gu.k = gt.k) FROM gt GROUP BY d",
        "SELECT count(*), (SELECT max(v) FROM gu WHERE gu.k = gt.k) FROM gt",
    ] {
        let e = rows(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), "42803", "{sql}: {e}");
    }
    // Refused, not guessed: the value inside a select-list expression
    // (PostgreSQL: 21, 6, NULL, NULL for k = 1, 2, 3, NULL).
    let e = rows(&db, &mut s, "SELECT k, (SELECT max(v) FROM gu WHERE gu.k = gt.k) + 1 FROM gt GROUP BY k")
        .await
        .expect_err("refused");
    assert_eq!(sqlstate_of(&e), "0A000", "{e}");
    // Refused, not guessed: ordered by the value without selecting it
    // (PostgreSQL: 3, NULL, 2, 1), which a grouped join cannot.
    let e = rows(&db, &mut s, "SELECT k FROM gt GROUP BY k ORDER BY (SELECT max(v) FROM gu WHERE gu.k = gt.k) NULLS FIRST, k")
        .await
        .expect_err("refused");
    assert_eq!(sqlstate_of(&e), "0A000", "{e}");
    // Refused, not guessed: an aggregate compared with the value in one
    // HAVING term (PostgreSQL: (1,2),(2,1),(3,1),(NULL,1)).
    let e = rows(
        &db,
        &mut s,
        "SELECT k, count(*) FROM gt GROUP BY k HAVING count(*) >= (SELECT count(*) FROM gu WHERE gu.k = gt.k) ORDER BY k",
    )
    .await
    .expect_err("refused");
    assert_eq!(sqlstate_of(&e), "0A000", "{e}");
}

/// `x [NOT] BETWEEN … AND …` with a constant NULL bound, and `BETWEEN
/// SYMMETRIC`, answer as PostgreSQL 17 does. A NULL bound reached the planner
/// as a column named NULL (42703 "unknown identifier 'NULL'") and SYMMETRIC
/// was a syntax error; `WHERE NOT NULL` was refused. Each expansion keeps
/// three-valued logic: `v NOT BETWEEN 5 AND NULL` holds where `v < 5`.
#[tokio::test(flavor = "multi_thread")]
async fn between_with_a_null_bound_or_symmetric_answers_as_postgresql_does() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE bt (id INT PRIMARY KEY, v INT)").await;
    ok(&db, &mut s, "INSERT INTO bt VALUES (1, 1), (2, 2), (3, NULL), (4, 7)").await;
    // The ids each keeps, PostgreSQL 17's answer for these rows.
    for (condition, expected) in [
        ("v BETWEEN 1 AND NULL", ""),
        ("v BETWEEN NULL AND 5", ""),
        ("v NOT BETWEEN 5 AND NULL", "1,2"),
        ("v NOT BETWEEN NULL AND 1", "2,4"),
        ("NOT (v BETWEEN 2 AND NULL)", "1"),
        ("NOT (v NOT BETWEEN 2 AND NULL)", ""),
        ("id = 4 OR v BETWEEN 1 AND NULL::int", "4"),
        ("v + 1 BETWEEN 2 AND NULL OR id BETWEEN 3 AND 4", "3,4"),
        ("v BETWEEN SYMMETRIC 2 AND 1", "1,2"),
        ("v NOT BETWEEN SYMMETRIC 7 AND 2", "1"),
        ("NOT (v BETWEEN SYMMETRIC 7 AND 2)", "1"),
        ("v BETWEEN 1 AND 2", "1,2"),
        ("NOT NULL", ""),
        ("NOT NULL OR id = 2", "2"),
    ] {
        let sql = format!("SELECT string_agg(id::text, ',' ORDER BY id) FROM bt WHERE {condition}");
        let want = if expected.is_empty() { None } else { Some(expected) };
        assert_eq!(one(&db, &mut s, &sql).await, vec![r(&[want])], "{sql}");
    }
    // The same through UPDATE and DELETE.
    ok(&db, &mut s, "UPDATE bt SET v = 0 WHERE v NOT BETWEEN 5 AND NULL").await;
    assert_eq!(
        one(&db, &mut s, "SELECT string_agg(id::text, ',' ORDER BY id) FROM bt WHERE v = 0").await,
        vec![r(&[Some("1,2")])]
    );
    ok(&db, &mut s, "DELETE FROM bt WHERE v BETWEEN NULL AND 100 OR id = 4").await;
    assert_eq!(one(&db, &mut s, "SELECT count(*) FROM bt").await, vec![r(&[Some("3")])]);
}

/// A statement refused with a foreign-key violation (23503).
async fn refused_23503(db: &Arc<Database>, s: &mut Session, sql: &str) {
    let e = rows(db, s, sql).await.expect_err(sql);
    assert_eq!(sqlstate_of(&e), "23503", "{sql}: {e}");
}

/// `id, p` of every row of `table`, in id order, `p` as text or "-".
async fn id_p(db: &Arc<Database>, s: &mut Session, table: &str) -> String {
    let sql = format!("SELECT id, p FROM {table} ORDER BY id");
    one(db, s, &sql)
        .await
        .iter()
        .map(|row| format!("{}:{}", row[0].as_deref().unwrap_or("-"), row[1].as_deref().unwrap_or("-")))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A table's foreign key onto itself is checked at the end of the statement,
/// once every row of it is in, as PostgreSQL 17 checks a key that is not
/// deferrable: a row may be its own parent, and rows of one `INSERT` may be
/// each other's. Each row's check ran before its row was written, so `(1,
/// 1)` and `(2, 3), (3, 2)` were refused (23503). What PostgreSQL refuses
/// still is, and leaves nothing behind. Every answer is PostgreSQL 17's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_referencing_foreign_key_is_checked_at_the_end_of_an_insert() {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE s (id int PRIMARY KEY, p int REFERENCES s (id))").await;
        ok(&db, &mut s, "INSERT INTO s VALUES (1, 1)").await;
        ok(&db, &mut s, "INSERT INTO s VALUES (2, 3), (3, 2)").await;
        refused_23503(&db, &mut s, "INSERT INTO s VALUES (4, 99)").await;
        // A row of the statement found, the other not: neither is kept.
        refused_23503(&db, &mut s, "INSERT INTO s VALUES (5, 6), (6, 98)").await;
        ok(&db, &mut s, "INSERT INTO s VALUES (7, NULL), (8, 7)").await;
        ok(&db, &mut s, "INSERT INTO s SELECT id + 10, p + 10 FROM s WHERE id IN (2, 3)").await;
        refused_23503(&db, &mut s, "INSERT INTO s SELECT id + 20, p + 50 FROM s WHERE id = 2").await;
        assert_eq!(
            one(&db, &mut s, "INSERT INTO s VALUES (40, 41), (41, 42), (42, 40) RETURNING id, p").await,
            vec![r(&[Some("40"), Some("41")]), r(&[Some("41"), Some("42")]), r(&[Some("42"), Some("40")])]
        );
        assert_eq!(id_p(&db, &mut s, "s").await, "1:1 2:3 3:2 7:- 8:7 12:13 13:12 40:41 41:42 42:40");

        // A key onto a UNIQUE column that is not the primary key.
        ok(&db, &mut s, "CREATE TABLE nd (id int PRIMARY KEY, code text UNIQUE, p text REFERENCES nd (code))").await;
        ok(&db, &mut s, "INSERT INTO nd VALUES (1, 'a', 'b'), (2, 'b', 'a')").await;
        refused_23503(&db, &mut s, "INSERT INTO nd VALUES (3, 'c', 'zz')").await;
        ok(&db, &mut s, "INSERT INTO nd VALUES (4, 'd', 'd')").await;
        assert_eq!(one(&db, &mut s, "SELECT count(*) FROM nd").await, vec![r(&[Some("3")])]);
    })
    .await
    .expect("bounded");
}

/// `ON CONFLICT`: a row the clause skips or turns into an update is not
/// checked as inserted (PostgreSQL checks inserted rows only), and the
/// update of `DO UPDATE` is checked at the end of the statement too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_referencing_foreign_key_under_on_conflict() {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE oc (id int PRIMARY KEY, p int REFERENCES oc (id))").await;
        ok(&db, &mut s, "INSERT INTO oc VALUES (1, 1) ON CONFLICT DO NOTHING").await;
        ok(&db, &mut s, "INSERT INTO oc VALUES (1, 99) ON CONFLICT (id) DO NOTHING").await;
        ok(&db, &mut s, "INSERT INTO oc VALUES (1, 99) ON CONFLICT DO NOTHING").await;
        ok(&db, &mut s, "INSERT INTO oc VALUES (2, 3), (3, 2) ON CONFLICT (id) DO NOTHING").await;
        ok(&db, &mut s, "INSERT INTO oc VALUES (1, 9), (9, 9) ON CONFLICT (id) DO UPDATE SET p = EXCLUDED.p").await;
        refused_23503(&db, &mut s, "INSERT INTO oc VALUES (1, 97) ON CONFLICT (id) DO UPDATE SET p = EXCLUDED.p").await;
        refused_23503(&db, &mut s, "INSERT INTO oc VALUES (5, 95) ON CONFLICT (id) DO UPDATE SET p = EXCLUDED.p").await;
        assert_eq!(id_p(&db, &mut s, "oc").await, "1:9 2:3 3:2 9:9");
    })
    .await
    .expect("bounded");
}

/// `UPDATE` and `DELETE` of a table that references itself: both sides of
/// the key are judged on the statement's end state. A row that is its own
/// parent may move its key with its reference; a key still referenced by a
/// row the statement does not repoint is refused; under `RESTRICT` too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_referencing_foreign_key_is_checked_at_the_end_of_an_update_or_delete() {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE u (id int PRIMARY KEY, p int REFERENCES u (id))").await;
        ok(&db, &mut s, "INSERT INTO u VALUES (1, 1)").await;
        ok(&db, &mut s, "UPDATE u SET id = 10, p = 10 WHERE id = 1").await;
        refused_23503(&db, &mut s, "UPDATE u SET p = 96 WHERE id = 10").await;
        ok(&db, &mut s, "INSERT INTO u VALUES (2, 10), (3, 2)").await;
        // Row 2 still references 10.
        refused_23503(&db, &mut s, "UPDATE u SET id = 20, p = 20 WHERE id = 10").await;
        ok(&db, &mut s, "UPDATE u SET id = id + 100, p = p + 100").await;
        ok(&db, &mut s, "UPDATE u SET p = id + 1 WHERE id = 102").await;
        ok(&db, &mut s, "UPDATE u SET p = id WHERE id = 102").await;
        assert_eq!(id_p(&db, &mut s, "u").await, "102:102 103:102 110:110");

        ok(&db, &mut s, "CREATE TABLE d (id int PRIMARY KEY, p int REFERENCES d (id))").await;
        ok(&db, &mut s, "INSERT INTO d VALUES (1, 1), (2, 3), (3, 2), (4, 1)").await;
        ok(&db, &mut s, "DELETE FROM d WHERE id IN (2, 3)").await;
        refused_23503(&db, &mut s, "DELETE FROM d WHERE id = 1").await;
        ok(&db, &mut s, "DELETE FROM d WHERE id IN (1, 4)").await;
        assert_eq!(id_p(&db, &mut s, "d").await, "");

        ok(
            &db,
            &mut s,
            "CREATE TABLE rs (id int PRIMARY KEY, p int REFERENCES rs (id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .await;
        ok(&db, &mut s, "INSERT INTO rs VALUES (1, 1), (2, 3), (3, 2), (4, 4), (5, 4)").await;
        ok(&db, &mut s, "UPDATE rs SET id = 10, p = 10 WHERE id = 1").await;
        // Row 5 still references 4.
        refused_23503(&db, &mut s, "UPDATE rs SET id = 40, p = 40 WHERE id = 4").await;
        ok(&db, &mut s, "DELETE FROM rs WHERE id IN (2, 3)").await;
        refused_23503(&db, &mut s, "DELETE FROM rs WHERE id = 4").await;
        ok(&db, &mut s, "DELETE FROM rs").await;
        assert_eq!(id_p(&db, &mut s, "rs").await, "");
    })
    .await
    .expect("bounded");
}

/// Inside a block each statement is checked at its own end: a violation
/// fails the statement, and `ROLLBACK TO SAVEPOINT` takes its rows back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_referencing_foreign_key_inside_a_block() {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE blk (id int PRIMARY KEY, p int REFERENCES blk (id))").await;
        ok(&db, &mut s, "BEGIN").await;
        ok(&db, &mut s, "INSERT INTO blk VALUES (20, 21), (21, 20)").await;
        ok(&db, &mut s, "INSERT INTO blk VALUES (22, 22)").await;
        ok(&db, &mut s, "UPDATE blk SET id = 23, p = 23 WHERE id = 22").await;
        ok(&db, &mut s, "COMMIT").await;
        ok(&db, &mut s, "BEGIN").await;
        refused_23503(&db, &mut s, "INSERT INTO blk VALUES (30, 95)").await;
        ok(&db, &mut s, "ROLLBACK").await;
        ok(&db, &mut s, "BEGIN").await;
        ok(&db, &mut s, "SAVEPOINT a").await;
        refused_23503(&db, &mut s, "INSERT INTO blk VALUES (31, 32), (32, 94)").await;
        ok(&db, &mut s, "ROLLBACK TO SAVEPOINT a").await;
        ok(&db, &mut s, "INSERT INTO blk VALUES (33, 33)").await;
        ok(&db, &mut s, "COMMIT").await;
        assert_eq!(id_p(&db, &mut s, "blk").await, "20:21 21:20 23:23 33:33");
    })
    .await
    .expect("bounded");
}

/// Runs `COPY table FROM STDIN` with `data` (text format) as the client's
/// rows, and returns its tag or its error.
async fn copy_in(db: &Arc<Database>, s: &mut Session, table: &str, data: &str) -> Result<String, String> {
    use tokio::io::AsyncWriteExt;
    // The client's CopyData and CopyDone, ahead of the statement that reads
    // them (the duplex holds them).
    let mut frames = Vec::new();
    for chunk in data.as_bytes().chunks(8192) {
        frames.push(b'd');
        frames.extend_from_slice(&((chunk.len() + 4) as i32).to_be_bytes());
        frames.extend_from_slice(chunk);
    }
    frames.push(b'c');
    frames.extend_from_slice(&4i32.to_be_bytes());
    s.client.write_all(&frames).await.unwrap();
    let sql = format!("COPY {table} FROM STDIN");
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, &sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'C' => return Ok(String::from_utf8_lossy(&body).trim_end_matches('\0').to_string()),
            b'E' => return Err(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
}

/// `COPY` is one statement: a row may reference a row of a later batch
/// (it inserts 500 rows a statement), and one missing parent anywhere
/// refuses the whole copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_referencing_foreign_key_across_a_copy() {
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE cp (id int PRIMARY KEY, p int REFERENCES cp (id))").await;
        assert_eq!(copy_in(&db, &mut s, "cp", "1\t2\n2\t1\n3\t3\n").await.as_deref(), Ok("COPY 3"));
        let e = copy_in(&db, &mut s, "cp", "4\t93\n").await.expect_err("an orphan");
        assert_eq!(sqlstate_of(&e), "23503", "{e}");
        assert_eq!(id_p(&db, &mut s, "cp").await, "1:2 2:1 3:3");

        ok(&db, &mut s, "CREATE TABLE big (id int PRIMARY KEY, p int REFERENCES big (id))").await;
        // Row i references i + 1, the last the first: every reference but
        // the last points forward, most into a later batch.
        let rows: String = (1..=1200).map(|i| format!("{i}\t{}\n", i % 1200 + 1)).collect();
        assert_eq!(copy_in(&db, &mut s, "big", &rows).await.as_deref(), Ok("COPY 1200"));
        let rows: String =
            (1201..=2400).map(|i| format!("{i}\t{}\n", if i < 2400 { i + 1 } else { 99999 })).collect();
        let e = copy_in(&db, &mut s, "big", &rows).await.expect_err("an orphan in the last batch");
        assert_eq!(sqlstate_of(&e), "23503", "{e}");
        assert_eq!(
            one(&db, &mut s, "SELECT count(*), min(id), max(id) FROM big").await,
            vec![r(&[Some("1200"), Some("1"), Some("1200")])]
        );
    })
    .await
    .expect("bounded");
}

/// Runs `sql`: its rows as `a|b` text (`NULL` for a NULL) and its command
/// tag, or the error's SQLSTATE.
async fn answer(db: &Arc<Database>, s: &mut Session, sql: &str) -> Result<(Vec<String>, String), String> {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        row.push("NULL".to_string());
                    } else {
                        row.push(String::from_utf8_lossy(&body[at..at + l as usize]).into_owned());
                        at += l as usize;
                    }
                }
                out.push(row.join("|"));
            }
            b'C' => return Ok((out, String::from_utf8_lossy(&body).trim_end_matches('\0').to_string())),
            b'E' => return Err(sqlstate_of(&String::from_utf8_lossy(&body)).to_string()),
            _ => {}
        }
    }
}

/// PostgreSQL checks a foreign key on the row it inserted, after the row is
/// in: a row `ON CONFLICT` skips, or turns into an update of the row it met,
/// never has its proposed key checked, and a CHECK or a uniqueness violation
/// is the error before a missing parent is. Each row's key was checked first
/// here, so all of these were 23503. Every answer is PostgreSQL 17's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_foreign_key_is_checked_on_the_row_the_insert_writes() {
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let db = db().await;
        let mut s = session(&db);
        for sql in [
            "CREATE TABLE parn (id int PRIMARY KEY)",
            "INSERT INTO parn VALUES (1), (2)",
            "CREATE TABLE chn (id int PRIMARY KEY, pn int REFERENCES parn(id), c int CHECK (c > 0))",
            "INSERT INTO chn VALUES (1, 1)",
            "CREATE TABLE ch2 (id int PRIMARY KEY, u int UNIQUE, pn int REFERENCES parn(id))",
            "INSERT INTO ch2 VALUES (1, 10, 1)",
            "CREATE SEQUENCE s1",
            "CREATE TABLE cs (id int PRIMARY KEY, k bigint DEFAULT nextval('s1'), pn int REFERENCES parn(id))",
            "INSERT INTO cs VALUES (1, 100, 1)",
            "CREATE TABLE src (id int, pn int)",
            "INSERT INTO src VALUES (1, 81), (7, 2), (8, 1)",
            "CREATE TABLE ck (a char(3), b int, pn int REFERENCES parn(id), PRIMARY KEY (a, b))",
            "INSERT INTO ck VALUES ('x', 1, 1)",
        ] {
            ok(&db, &mut s, sql).await;
        }
        let ins = |rows: &[&str], tag: &str| -> Result<(Vec<String>, String), String> {
            Ok((rows.iter().map(|r| r.to_string()).collect(), tag.to_string()))
        };
        let err = |code: &str| -> Result<(Vec<String>, String), String> { Err(code.to_string()) };
        for (sql, expected) in [
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT (id) DO UPDATE SET pn = 2 RETURNING pn", ins(&["2"], "INSERT 0 1")),
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT (id) DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT ON CONSTRAINT chn_pkey DO NOTHING", ins(&[], "INSERT 0 0")),
            (
                "INSERT INTO chn VALUES (1, 81) ON CONFLICT ON CONSTRAINT chn_pkey DO UPDATE SET pn = 1 RETURNING id, pn",
                ins(&["1|1"], "INSERT 0 1"),
            ),
            // No conflict: the row is inserted, and its key is missing.
            ("INSERT INTO chn VALUES (20, 81) ON CONFLICT (id) DO NOTHING", err("23503")),
            ("INSERT INTO chn VALUES (20, 81) ON CONFLICT (id) DO UPDATE SET pn = 2", err("23503")),
            // The update's own key is checked.
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT (id) DO UPDATE SET pn = EXCLUDED.pn", err("23503")),
            ("INSERT INTO chn VALUES (1, 81) ON CONFLICT (id) DO UPDATE SET pn = 99", err("23503")),
            // A row of a multi-row insert.
            ("INSERT INTO chn VALUES (1, 81), (2, 1), (3, 82) ON CONFLICT (id) DO NOTHING", err("23503")),
            ("INSERT INTO chn VALUES (1, 81), (2, 1), (5, 2) ON CONFLICT (id) DO NOTHING", ins(&[], "INSERT 0 2")),
            (
                "INSERT INTO chn VALUES (1, 81), (2, 77), (6, 2) ON CONFLICT (id) DO UPDATE SET pn = 1 RETURNING id, pn",
                ins(&["1|1", "2|1", "6|2"], "INSERT 0 3"),
            ),
            ("SELECT id, pn FROM chn ORDER BY id", ins(&["1|1", "2|1", "5|2", "6|2"], "SELECT 4")),
            // Without the clause, or a clash on another key: 23505 first.
            ("INSERT INTO chn VALUES (1, 81)", err("23505")),
            ("INSERT INTO ch2 VALUES (2, 10, 81) ON CONFLICT (id) DO NOTHING", err("23505")),
            ("INSERT INTO ch2 VALUES (2, 10, 81) ON CONFLICT (u) DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO ch2 VALUES (2, 10, 81) ON CONFLICT DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO ch2 VALUES (1, 11, 81) ON CONFLICT (u) DO NOTHING", err("23505")),
            // A CHECK is judged before either.
            ("INSERT INTO chn VALUES (1, 81, -1) ON CONFLICT DO NOTHING", err("23514")),
            ("INSERT INTO chn VALUES (30, 81, -1)", err("23514")),
            // A default computed once per row, the skipped row's too.
            ("INSERT INTO cs (id, pn) VALUES (1, 81) ON CONFLICT DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO cs (id, pn) VALUES (1, 81) ON CONFLICT (id) DO UPDATE SET pn = 2 RETURNING id, k, pn", ins(&["1|100|2"], "INSERT 0 1")),
            ("SELECT nextval('s1')", ins(&["3"], "SELECT 1")),
            // INSERT … SELECT.
            ("INSERT INTO chn (id, pn) SELECT id, pn FROM src ON CONFLICT DO NOTHING", ins(&[], "INSERT 0 2")),
            (
                "INSERT INTO chn (id, pn) SELECT id, pn FROM src ON CONFLICT (id) DO UPDATE SET c = 5 RETURNING id, pn, c",
                ins(&["1|1|5", "7|2|5", "8|1|5"], "INSERT 0 3"),
            ),
            // A key compared as the column stores it.
            ("INSERT INTO ck VALUES ('x  ', 1, 81) ON CONFLICT DO NOTHING", ins(&[], "INSERT 0 0")),
            ("INSERT INTO ck VALUES ('x', 1, 81) ON CONFLICT (a, b) DO UPDATE SET pn = 2 RETURNING *", ins(&["x  |1|2"], "INSERT 0 1")),
            ("INSERT INTO ck VALUES ('x', 2, 81) ON CONFLICT (a, b) DO NOTHING", err("23503")),
        ] {
            assert_eq!(answer(&db, &mut s, sql).await, expected, "{sql}");
        }
        // In a block: the skipped row does not fail it.
        ok(&db, &mut s, "BEGIN").await;
        assert_eq!(answer(&db, &mut s, "INSERT INTO chn VALUES (1, 81) ON CONFLICT DO NOTHING").await, ins(&[], "INSERT 0 0"));
        assert_eq!(
            answer(&db, &mut s, "INSERT INTO chn VALUES (1, 81) ON CONFLICT (id) DO UPDATE SET pn = 2 RETURNING pn").await,
            ins(&["2"], "INSERT 0 1")
        );
        ok(&db, &mut s, "ROLLBACK").await;
        assert_eq!(answer(&db, &mut s, "SELECT pn FROM chn WHERE id = 1").await, ins(&["1"], "SELECT 1"));
    })
    .await
    .expect("bounded");
}

/// A volatile call in a condition is evaluated for each row it judges, as
/// PostgreSQL evaluates it: `nextval` in a WHERE was folded into one value
/// for the statement (`WHERE n < nextval('q') + 100000` over four rows drew
/// one value, not four), and over a scan's workers it had no sequence to draw
/// from. `clock_timestamp()` in a WHERE was refused (0A000), and `random()`
/// or `nextval` in a join's WHERE too ("references no column"). Every answer
/// is PostgreSQL 17's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volatile_call_in_a_condition_is_evaluated_per_row() {
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE t (id int PRIMARY KEY, n int)").await;
        ok(&db, &mut s, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, NULL)").await;
        ok(&db, &mut s, "CREATE SEQUENCE q START 1").await;
        let at = |rows: &[&str], tag: &str| -> Result<(Vec<String>, String), String> {
            Ok((rows.iter().map(|r| r.to_string()).collect(), tag.to_string()))
        };
        for (sql, expected) in [
            ("SELECT count(*) FROM t WHERE n < nextval('q') + 100000", at(&["3"], "SELECT 1")),
            ("SELECT nextval('q')", at(&["5"], "SELECT 1")),
            ("SELECT currval('q')", at(&["5"], "SELECT 1")),
            ("SELECT id FROM t WHERE nextval('q') < 10 ORDER BY id", at(&["1", "2", "3", "4"], "SELECT 4")),
            ("SELECT currval('q')", at(&["9"], "SELECT 1")),
            ("SELECT count(*) FROM t WHERE nextval('q') > 0", at(&["4"], "SELECT 1")),
            ("SELECT nextval('q')", at(&["14"], "SELECT 1")),
            ("UPDATE t SET n = n WHERE nextval('q') > 0", at(&[], "UPDATE 4")),
            ("SELECT nextval('q')", at(&["19"], "SELECT 1")),
            // Not a point lookup: the key is a new value for each row.
            ("UPDATE t SET n = 5 WHERE id = nextval('q') - 100", at(&[], "UPDATE 0")),
            ("DELETE FROM t WHERE id = nextval('q')", at(&[], "DELETE 0")),
            ("SELECT nextval('q')", at(&["28"], "SELECT 1")),
            ("SELECT count(*) FROM t a JOIN t b ON a.id = b.id WHERE nextval('q') > 0", at(&["4"], "SELECT 1")),
            ("SELECT nextval('q')", at(&["33"], "SELECT 1")),
            ("INSERT INTO t SELECT id + 10, n FROM t WHERE nextval('q') > 0", at(&[], "INSERT 0 4")),
            ("SELECT nextval('q')", at(&["38"], "SELECT 1")),
            // Eight values in a row: four of them even, whichever row takes which.
            ("DELETE FROM t WHERE nextval('q') % 2 = 0", at(&[], "DELETE 4")),
            ("SELECT count(*) FROM t", at(&["4"], "SELECT 1")),
            ("SELECT nextval('q')", at(&["47"], "SELECT 1")),
            ("SELECT count(*) FROM t WHERE setval('q', 100) > 0", at(&["4"], "SELECT 1")),
            ("SELECT nextval('q')", at(&["101"], "SELECT 1")),
            // The other volatile calls.
            ("SELECT count(*) FROM t WHERE clock_timestamp() > now() - interval '1 hour'", at(&["4"], "SELECT 1")),
            ("SELECT count(*) FROM t WHERE clock_timestamp() < now() - interval '1 hour'", at(&["0"], "SELECT 1")),
            ("SELECT count(*) FROM t WHERE random() >= 0 AND random() < 1", at(&["4"], "SELECT 1")),
            ("SELECT count(*) FROM t a JOIN t b ON a.id = b.id WHERE random() >= 0", at(&["4"], "SELECT 1")),
        ] {
            assert_eq!(answer(&db, &mut s, sql).await, expected, "{sql}");
        }
        // The `SET` of one conflict's update still draws once.
        ok(&db, &mut s, "INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0) ON CONFLICT DO NOTHING").await;
        ok(&db, &mut s, "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET n = nextval('q')").await;
        assert_eq!(answer(&db, &mut s, "SELECT n FROM t WHERE id = 1").await, at(&["102"], "SELECT 1"));
        assert_eq!(answer(&db, &mut s, "SELECT nextval('q')").await, at(&["103"], "SELECT 1"));
    })
    .await
    .expect("bounded");
}

/// `EXISTS` over a `SELECT` with no `FROM` is the truth of its condition (one
/// row when it holds, none otherwise). Every `EXISTS` path wanted a relation
/// (42601, "the SELECT inside EXISTS has no FROM clause"), in a select list,
/// in a `WHERE` with or without a table, and over a parameter. Every answer
/// is PostgreSQL 17's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exists_over_a_select_without_from_answers_as_postgresql_does() {
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let db = db().await;
        let mut s = session(&db);
        ok(&db, &mut s, "CREATE TABLE t (id int PRIMARY KEY, n int)").await;
        ok(&db, &mut s, "INSERT INTO t VALUES (1, 1), (2, 7), (3, NULL)").await;
        let at = |rows: &[&str], tag: &str| -> Result<(Vec<String>, String), String> {
            Ok((rows.iter().map(|r| r.to_string()).collect(), tag.to_string()))
        };
        for (sql, expected) in [
            ("SELECT EXISTS (SELECT 1)", at(&["t"], "SELECT 1")),
            ("SELECT EXISTS (SELECT 1 WHERE false)", at(&["f"], "SELECT 1")),
            ("SELECT NOT EXISTS (SELECT 1)", at(&["f"], "SELECT 1")),
            ("SELECT EXISTS (SELECT 1 WHERE NULL)", at(&["f"], "SELECT 1")),
            ("SELECT NOT EXISTS (SELECT 1 WHERE NULL)", at(&["t"], "SELECT 1")),
            ("SELECT 1 WHERE EXISTS (SELECT 1)", at(&["1"], "SELECT 1")),
            ("SELECT 1 WHERE EXISTS (SELECT 1 WHERE false)", at(&[], "SELECT 0")),
            ("SELECT 1 WHERE NOT EXISTS (SELECT 1 WHERE 1 = 2)", at(&["1"], "SELECT 1")),
            ("SELECT 1 WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT 1 WHERE 1 = 1))", at(&["1"], "SELECT 1")),
            ("SELECT id FROM t WHERE EXISTS (SELECT 1) AND n IS NULL", at(&["3"], "SELECT 1")),
            ("SELECT id FROM t WHERE EXISTS (SELECT 1 WHERE false) ORDER BY id", at(&[], "SELECT 0")),
            ("SELECT id FROM t WHERE NOT EXISTS (SELECT 1) ORDER BY id", at(&[], "SELECT 0")),
            ("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 WHERE false) ORDER BY id", at(&["1", "2", "3"], "SELECT 3")),
            // The outer row's columns, qualified or bare.
            ("SELECT id FROM t WHERE EXISTS (SELECT 1 WHERE t.n > 5) ORDER BY id", at(&["2"], "SELECT 1")),
            ("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 WHERE t.n > 5) ORDER BY id", at(&["1", "3"], "SELECT 2")),
            ("SELECT id FROM t WHERE EXISTS (SELECT 1 WHERE n > 5) ORDER BY id", at(&["2"], "SELECT 1")),
            ("SELECT id, EXISTS (SELECT 1 WHERE t.n > 5) FROM t ORDER BY id", at(&["1|f", "2|t", "3|f"], "SELECT 3")),
            ("SELECT count(*) FROM t a JOIN t b ON a.id = b.id WHERE EXISTS (SELECT 1 WHERE a.n > 1)", at(&["1"], "SELECT 1")),
            // Aggregates yield a row whatever the condition.
            ("SELECT id FROM t WHERE EXISTS (SELECT count(*) WHERE false) ORDER BY id", at(&["1", "2", "3"], "SELECT 3")),
            ("SELECT id FROM t WHERE EXISTS (SELECT 1 LIMIT 0) ORDER BY id", at(&[], "SELECT 0")),
            (
                "SELECT 1 WHERE NOT EXISTS (SELECT 1 WHERE 1 = 2) AND EXISTS (SELECT 1 FROM t)",
                at(&["1"], "SELECT 1"),
            ),
            ("SELECT 'é' AS x WHERE EXISTS (SELECT 'ü')", at(&["é"], "SELECT 1")),
            ("DELETE FROM t WHERE NOT EXISTS (SELECT 1 WHERE false) AND id = 3 RETURNING id", at(&["3"], "DELETE 1")),
            ("UPDATE t SET n = 9 WHERE EXISTS (SELECT 1) AND id = 1 RETURNING id, n", at(&["1|9"], "UPDATE 1")),
        ] {
            assert_eq!(answer(&db, &mut s, sql).await, expected, "{sql}");
        }
        // A parameter, prepared and executed.
        ok(&db, &mut s, "PREPARE p(int) AS SELECT id FROM t WHERE NOT EXISTS (SELECT 1 WHERE $1 = 2) ORDER BY id").await;
        assert_eq!(answer(&db, &mut s, "EXECUTE p(2)").await, at(&[], "SELECT 0"));
        assert_eq!(answer(&db, &mut s, "EXECUTE p(3)").await, at(&["1", "2"], "SELECT 2"));
    })
    .await
    .expect("bounded");
}
