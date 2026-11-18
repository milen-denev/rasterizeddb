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
        ("SELECT nope AS \"Nope\" FROM ua", "'nope'"),
        ("SELECT id, nope AS x FROM ua", "'nope'"),
        ("SELECT nope x FROM ua", "'nope'"),
        ("SELECT ua.nope AS x FROM ua", "'ua.nope'"),
        ("SELECT nope FROM ua", "'nope'"),
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
    // Still refused, not guessed: the join would change the grouping.
    let e = rows(&db, &mut s, "SELECT j FROM (SELECT (SELECT max(mt.v) FROM mt WHERE mt.id = ms.id) AS j FROM ms GROUP BY ms.id) z")
        .await
        .expect_err("refused");
    assert_eq!(sqlstate_of(&e), "0A000", "{e}");
}
