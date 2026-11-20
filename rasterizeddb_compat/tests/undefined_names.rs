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
}
