//! `CREATE DOMAIN` / `DROP DOMAIN`, refused before ("syntax error or
//! unsupported query", then "column type 'POSINT' is not supported yet").
//! Each answer is PostgreSQL 17's (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn error(c: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let err = c.simple_query(sql).await.expect_err(sql);
    let db = err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}"));
    (db.code().code().to_string(), db.message().to_string())
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn a_domain_lives_its_life() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE DOMAIN posint AS int CHECK (VALUE > 0);
         CREATE DOMAIN email AS text CONSTRAINT has_at CHECK (VALUE LIKE '%@%') NOT NULL;
         CREATE DOMAIN five AS int DEFAULT 5;
         CREATE TABLE gd (id int, p posint, e email, f five);
         INSERT INTO gd (id, p, e) VALUES (1, 3, 'a@b')",
    )
    .await
    .expect("setup");
    // The domain's default, and its checks with PostgreSQL's 23514 and names.
    assert_eq!(rows(&c, "SELECT * FROM gd").await, ["1|3|a@b|5"]);
    assert_eq!(
        error(&c, "INSERT INTO gd (id, p, e) VALUES (2, -1, 'a@b')").await,
        ("23514".to_string(), "value for domain posint violates check constraint \"posint_check\"".to_string())
    );
    assert_eq!(
        error(&c, "INSERT INTO gd (id, p, e) VALUES (3, 1, 'nope')").await,
        ("23514".to_string(), "value for domain email violates check constraint \"has_at\"".to_string())
    );
    assert_eq!(error(&c, "UPDATE gd SET p = 0").await.0, "23514");
    // NOT NULL holds (23502, in the column's words).
    assert_eq!(error(&c, "INSERT INTO gd (id, p) VALUES (4, 1)").await.0, "23502");
    // A cast to a domain is one to its base type.
    assert_eq!(rows(&c, "SELECT 7::posint, CAST('9' AS posint) + 1").await, ["7|10"]);
    // ALTER TABLE ADD, and a default after another clause.
    c.batch_execute("ALTER TABLE gd ADD COLUMN q posint; CREATE TABLE gd2 (x five PRIMARY KEY, y int); INSERT INTO gd2 (y) VALUES (1)")
        .await
        .expect("add");
    assert_eq!(error(&c, "INSERT INTO gd (id, p, e, q) VALUES (5, 1, 'x@y', -2)").await.0, "23514");
    assert_eq!(rows(&c, "SELECT * FROM gd2").await, ["5|1"]);
    // Names and dependencies.
    assert_eq!(error(&c, "CREATE DOMAIN posint AS int").await.0, "42710");
    assert_eq!(error(&c, "DROP DOMAIN posint").await.0, "2BP01");
    assert_eq!(error(&c, "DROP DOMAIN nosuchdom").await.0, "42704");
    c.batch_execute("DROP DOMAIN IF EXISTS nosuchdom").await.expect("if exists");
    c.batch_execute("DROP TABLE gd2; DROP TABLE gd; DROP DOMAIN posint, email, five").await.expect("drop");
    assert_eq!(error(&c, "SELECT 1::posint").await.0.len(), 5);
    assert_eq!(error(&c, "ALTER DOMAIN five ADD CHECK (VALUE < 9)").await.0, "0A000");
}

/// A domain created in a block that rolls back is gone with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_domain_is_gone() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("BEGIN; CREATE DOMAIN rbd AS int CHECK (VALUE > 0); CREATE TABLE rbt (x rbd); ROLLBACK")
        .await
        .expect("block");
    c.batch_execute("CREATE DOMAIN rbd AS text").await.expect("the name is free again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cast_to_a_domain_checks_its_constraints() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE DOMAIN posint AS int CHECK (VALUE > 0);
         CREATE DOMAIN nn AS text NOT NULL;
         CREATE DOMAIN code AS varchar(5) CONSTRAINT short CHECK (length(VALUE) < 4) CHECK (VALUE <> 'x');
         CREATE TABLE dt (id int PRIMARY KEY, a int, s text);
         INSERT INTO dt VALUES (1, 3, 'ab'), (2, -1, 'abcde')",
    )
    .await
    .expect("setup");
    for (sql, want) in [
        ("SELECT 5::posint, '7'::posint + 1, NULL::posint IS NULL", "5|8|t"),
        ("SELECT 'ab'::code, s::text::code, upper(s)::code FROM dt WHERE id = 1", "ab|ab|AB"),
        ("SELECT (a + 1)::posint, CAST(a AS posint) FROM dt WHERE id = 1", "4|3"),
    ] {
        assert_eq!(rows(&c, sql).await, [want], "{sql}");
    }
    let check = |domain: &str, name: &str| {
        ("23514".to_string(), format!("value for domain {domain} violates check constraint \"{name}\""))
    };
    for (sql, want) in [
        ("SELECT (-5)::posint", check("posint", "posint_check")),
        ("SELECT CAST(-1 AS posint)", check("posint", "posint_check")),
        ("SELECT 'abcd'::code", check("code", "short")),
        ("SELECT 'x'::code", check("code", "code_check")),
        ("SELECT 'it''s'::code", check("code", "short")),
        // Every row's value, as PostgreSQL casts each.
        ("SELECT id, a::posint FROM dt ORDER BY id", check("posint", "posint_check")),
        ("SELECT id FROM dt WHERE a::posint > 2", check("posint", "posint_check")),
        ("INSERT INTO dt VALUES (3, (-4)::posint, 'q')", check("posint", "posint_check")),
        ("SELECT NULL::nn", ("23502".to_string(), "domain nn does not allow null values".to_string())),
    ] {
        assert_eq!(error(&c, sql).await, want, "{sql}");
    }
    // Over the extended protocol too: a bound value cast whole, the cast on
    // the placeholder itself kept, and the column named for the domain.
    for sql in ["SELECT $1::int::posint", "SELECT $1::posint"] {
        let err = c.query(sql, &[&-3i32]).await.expect_err(sql);
        assert_eq!(err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err:?}")).code().code(), "23514", "{sql}");
    }
    let row = c.query_one("SELECT $1::int::posint", &[&4i32]).await.expect("extended");
    assert_eq!(row.columns()[0].name(), "posint");
    assert_eq!(row.get::<_, i32>(0), 4);
    assert_eq!(rows(&c, "SELECT count(*) FROM dt").await, ["2"]);
}
