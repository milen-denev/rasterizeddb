//! A name that names nothing, as PostgreSQL reports it.
//!
//! A bare name where a value belongs (`UPDATE t SET s = nosuch`, `INSERT …
//! VALUES (…, nosuch)`) was stored as its own text and the statement
//! reported success. An unknown function was 0A000 "not supported yet" and
//! an unknown column inside an expression a syntax error (42601), where a
//! driver looks for 42883 and 42703. Every code, message and position here
//! is PostgreSQL 17's for the same statement (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

async fn error(c: &tokio_postgres::Client, sql: &str) -> (String, String, Option<u32>) {
    let err = c.simple_query(sql).await.expect_err(sql);
    let db = err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}"));
    let position = match db.position() {
        Some(tokio_postgres::error::ErrorPosition::Original(p)) => Some(*p),
        _ => None,
    };
    (db.code().code().to_string(), db.message().to_string(), position)
}

async fn rows(c: &tokio_postgres::Client) -> Vec<(i32, Option<String>)> {
    c.query("SELECT id, s FROM un ORDER BY id", &[])
        .await
        .expect("rows")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bare_name_where_a_value_belongs_is_no_value() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE un (id int PRIMARY KEY, a int, s text); INSERT INTO un VALUES (1, 2, 'x')")
        .await
        .expect("setup");
    let cases: &[(&str, &str, &str, u32)] = &[
        ("UPDATE un SET s = nosuch WHERE id = 1", "42703", "column \"nosuch\" does not exist", 19),
        ("UPDATE un SET s = \"nosuch\" WHERE id = 1", "42703", "column \"nosuch\" does not exist", 19),
        ("UPDATE un SET (s, a) = (nosuch, 1) WHERE id = 1", "42703", "column \"nosuch\" does not exist", 25),
        ("INSERT INTO un VALUES (2, 1, nosuch)", "42703", "column \"nosuch\" does not exist", 30),
        ("INSERT INTO un VALUES (2, 1, \"nosuch\")", "42703", "column \"nosuch\" does not exist", 30),
        // The table's own column: VALUES sees none.
        ("INSERT INTO un VALUES (2, 1, s)", "42703", "column \"s\" does not exist", 30),
        ("INSERT INTO un VALUES (2, 1, x.y)", "42P01", "missing FROM-clause entry for table \"x\"", 30),
        (
            "INSERT INTO un VALUES (1, 1, 'z') ON CONFLICT (id) DO UPDATE SET s = nosuch",
            "42703",
            "column \"nosuch\" does not exist",
            70,
        ),
    ];
    for (sql, code, message, position) in cases {
        assert_eq!(error(&c, sql).await, (code.to_string(), message.to_string(), Some(*position)), "{sql}");
    }
    // Nothing was written by any of them.
    assert_eq!(rows(&c).await, vec![(1, Some("x".to_string()))]);

    // The value words and a column of the row still are values.
    c.batch_execute(
        "INSERT INTO un VALUES (2, DEFAULT, current_user); \
         UPDATE un SET s = NULL, a = DEFAULT WHERE id = 1; \
         UPDATE un SET s = a WHERE id = 1",
    )
    .await
    .expect("value words");
    let user: String = c.query_one("SELECT current_user::text", &[]).await.expect("user").get(0);
    assert_eq!(rows(&c).await, vec![(1, None), (2, Some(user))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_functions_and_columns_in_expressions_have_postgresql_codes() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE fx (id int PRIMARY KEY, a int, s text); CREATE TABLE fo (id int PRIMARY KEY, name text); \
         INSERT INTO fx VALUES (1, 2, 'x'); INSERT INTO fo VALUES (1, 'one')",
    )
    .await
    .expect("setup");
    let cases: &[(&str, &str, &str, u32)] = &[
        ("SELECT nosuchfn(a, 'x', 1.5, s) FROM fx", "42883", "function nosuchfn(integer, unknown, numeric, text) does not exist", 8),
        ("SELECT a FROM fx WHERE nosuchfn(a) = 1", "42883", "function nosuchfn(integer) does not exist", 24),
        ("SELECT nosuchagg(a) FROM fx GROUP BY s", "42883", "function nosuchagg(integer) does not exist", 8),
        ("UPDATE fx SET a = nosuchfn(1)", "42883", "function nosuchfn(integer) does not exist", 19),
        ("INSERT INTO fx VALUES (3, nosuchfn(1), 'z')", "42883", "function nosuchfn(integer) does not exist", 27),
        ("SELECT public.nosuchfn(1)", "42883", "function public.nosuchfn(integer) does not exist", 8),
        ("SELECT abs('x'::text) FROM fx", "42883", "function abs(text) does not exist", 8),
        ("SELECT length(1, 2)", "42883", "function length(integer, integer) does not exist", 8),
        // A value that does not cast to text implicitly has no lower/upper.
        ("SELECT lower(a) FROM fx", "42883", "function lower(integer) does not exist", 8),
        ("SELECT id FROM fx WHERE upper(a) = '2'", "42883", "function upper(integer) does not exist", 25),
        ("SELECT lower(1.5)", "42883", "function lower(numeric) does not exist", 8),
        ("SELECT upper(true)", "42883", "function upper(boolean) does not exist", 8),
        ("SELECT lower(now())", "42883", "function lower(timestamp with time zone) does not exist", 8),
        ("SELECT lower(interval '1 day')", "42883", "function lower(interval) does not exist", 8),
        ("SELECT upper(date '2024-01-01')", "42883", "function upper(date) does not exist", 8),
        // An argument's unknown column is found before its function.
        ("SELECT nosuchfn(nosuch) FROM fx", "42703", "column \"nosuch\" does not exist", 17),
        ("SELECT nosuch + 1 FROM fx", "42703", "column \"nosuch\" does not exist", 8),
        ("SELECT s || nosuch FROM fx", "42703", "column \"nosuch\" does not exist", 13),
        ("SELECT CASE WHEN nosuch THEN 1 END FROM fx", "42703", "column \"nosuch\" does not exist", 18),
        ("SELECT a FROM fx HAVING nosuch > 1", "42703", "column \"nosuch\" does not exist", 25),
        ("SELECT sum(a) FROM fx GROUP BY s HAVING nosuch > 1", "42703", "column \"nosuch\" does not exist", 41),
        ("SELECT a FROM fx ORDER BY a + nosuch", "42703", "column \"nosuch\" does not exist", 31),
        ("SELECT id FROM fx WHERE id IN (SELECT nosuch FROM fx)", "42703", "column \"nosuch\" does not exist", 39),
        ("SELECT a FROM fx WHERE EXISTS (SELECT 1 WHERE nosuch)", "42703", "column \"nosuch\" does not exist", 47),
        ("SELECT a FROM fx f JOIN fx g ON f.id = g.nosuch", "42703", "column g.nosuch does not exist", 40),
        ("SELECT f.a + g.nosuch FROM fx f JOIN fx g ON f.id = g.id", "42703", "column g.nosuch does not exist", 14),
        ("SELECT a FROM fx f WHERE f.nosuch = 1", "42703", "column f.nosuch does not exist", 26),
        ("SELECT * FROM fx WHERE s = nosuch", "42703", "column \"nosuch\" does not exist", 28),
        // An output name is a column of ORDER BY alone, not of an expression in it.
        ("SELECT a + 1 AS b FROM fx ORDER BY b + nosuch", "42703", "column \"b\" does not exist", 36),
    ];
    for (sql, code, message, position) in cases {
        assert_eq!(error(&c, sql).await, (code.to_string(), message.to_string(), Some(*position)), "{sql}");
    }
    // A failure whose names all resolve keeps its own code.
    for (sql, code) in [
        ("SELECT a FROM fx f JOIN fx g ON f.id = g.id", "42702"),
        ("SELECT sum(a) FROM fx GROUP BY s HAVING a > 1", "42803"),
        ("SELECT a, count(*) FROM fx", "42803"),
        ("SELECT 1 / 0 FROM fx", "22012"),
        // A function PostgreSQL has and the engine lacks stays unsupported.
        ("SELECT to_ascii(s) FROM fx", "0A000"),
    ] {
        let (got, message, _) = error(&c, sql).await;
        assert_eq!(got, code, "{sql}: {message}");
    }
    let row = c
        .query_one("SELECT lower(s), upper(a::text), lower('AB'), upper(NULL::text) FROM fx", &[])
        .await
        .expect("text lowercases");
    let got: (String, String, String, Option<String>) = (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!(got, ("x".to_string(), "2".to_string(), "ab".to_string(), None));
}

/// `ON CONFLICT … DO UPDATE` is read with the statement, as PostgreSQL reads
/// it: a name in its `SET` or `WHERE` that names nothing was refused only
/// when a row conflicted, so the first run inserted and reported success.
#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_do_update_names_nothing_before_any_row_is_written() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE oc (id int PRIMARY KEY, n int, s text, b bool); INSERT INTO oc VALUES (1, 1, 'a', true)")
        .await
        .expect("setup");
    let cases: &[(&str, &str, &str)] = &[
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET s = nosuch", "42703", "column \"nosuch\" does not exist"),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = nosuch + 1", "42703", "column \"nosuch\" does not exist"),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET s = upper(nosuch)", "42703", "column \"nosuch\" does not exist"),
        (
            "INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET nosuch = 1",
            "42703",
            "column \"nosuch\" of relation \"oc\" does not exist",
        ),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = excluded.nosuch", "42703", "column excluded.nosuch does not exist"),
        ("INSERT INTO oc AS o (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = o.nosuch + 1", "42703", "column o.nosuch does not exist"),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = x.n", "42P01", "missing FROM-clause entry for table \"x\""),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = 1 WHERE oc.nosuch > 1", "42703", "column oc.nosuch does not exist"),
        ("INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = 1 WHERE nosuch", "42703", "column \"nosuch\" does not exist"),
        (
            "INSERT INTO oc (id, n) VALUES (2, 1), (3, 1) ON CONFLICT (id) DO UPDATE SET s = nosuch",
            "42703",
            "column \"nosuch\" does not exist",
        ),
        (
            "INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET (n, s) = (1)",
            "42601",
            "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression",
        ),
        (
            "INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET (n, s) = (1, 2, 3)",
            "42601",
            "number of columns does not match number of values",
        ),
    ];
    for (sql, code, message) in cases {
        let (got_code, got_message, _) = error(&c, sql).await;
        assert_eq!((got_code.as_str(), got_message.as_str()), (*code, *message), "{sql}");
    }
    // In a transaction block too, and nothing was written by any of them.
    let err = c
        .batch_execute("BEGIN; INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET s = nosuch; COMMIT")
        .await
        .expect_err("in a block");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "42703");
    c.batch_execute("ROLLBACK").await.expect("rollback");
    let count: i64 = c.query_one("SELECT count(*) FROM oc", &[]).await.expect("count").get(0);
    assert_eq!(count, 1);

    // What names something still runs, conflicting or not; a row-form SET
    // too, which reached the conflict's UPDATE whole and failed there.
    for sql in [
        "INSERT INTO oc VALUES (1, 5, 'y', false) ON CONFLICT (id) DO UPDATE SET n = oc.n + excluded.n, s = excluded.s || oc.s, b = NOT excluded.b",
        "INSERT INTO oc AS o (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = o.n + 1 WHERE o.b IS FALSE AND excluded.n > 0",
        "INSERT INTO oc (id, n) VALUES (2, 1) ON CONFLICT (id) DO UPDATE SET n = (SELECT max(n) FROM oc) + 1",
        "INSERT INTO oc (id, n, s) VALUES (2, 9, 'k') ON CONFLICT (id) DO UPDATE SET (n, s) = (excluded.n, excluded.s)",
        "INSERT INTO oc (id, n) VALUES (3, 4) ON CONFLICT (id) DO UPDATE SET (n) = ROW(excluded.n + 1)",
        "INSERT INTO oc (id, n) VALUES (3, 4) ON CONFLICT (id) DO UPDATE SET (n) = ROW(excluded.n + 1)",
    ] {
        c.batch_execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let rows: Vec<(i32, Option<i32>, Option<String>)> = c
        .query("SELECT id, n, s FROM oc ORDER BY id", &[])
        .await
        .expect("rows")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, Some(6), Some("ya".to_string())), (2, Some(9), Some("k".to_string())), (3, Some(5), None)]
    );
}
