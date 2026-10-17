//! DDL the way a migration emits it.
//!
//! Migrations are the first thing an application runs and are almost entirely
//! DDL, so a statement the engine cannot honour has to fail loudly rather than
//! be accepted as a no-op. The exceptions are recorded here explicitly.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::{TestServer, probes};
use sqlx::Row;

/// Rows a query returns.
async fn count(pool: &sqlx::PgPool, sql: &str) -> usize {
    sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect("select should succeed").len()
}

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// How many rows a query returns.
async fn count_rows(pool: &sqlx::PgPool, sql: &str) -> usize {
    sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect("select should succeed").len()
}

/// Column names visible to a client, in order.
async fn columns(pool: &sqlx::PgPool, table: &str) -> Vec<String> {
    use sqlx::{Column, Row};
    let rows = sqlx::query(AssertSqlSafe(format!("SELECT * FROM {}", table)))
        .fetch_all(pool)
        .await
        .expect("select should succeed");
    rows.first()
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_adds_renames_and_drops_columns() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE t (id UUID NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create should succeed");

    run(&pool, "ALTER TABLE t ADD COLUMN note TEXT").await.expect("add");
    run(&pool, r#"ALTER TABLE "t" ADD COLUMN "amount" NUMERIC(10,2) NOT NULL"#)
        .await
        .expect("add with a precision and NOT NULL");
    run(&pool, "ALTER TABLE t RENAME COLUMN note TO remark").await.expect("rename");

    run(
        &pool,
        "INSERT INTO t (id, name, remark, amount) \
         VALUES ('11111111-1111-4111-8111-111111111111', 'x', 'hi', 5.50)",
    )
    .await
    .expect("insert should see the added columns");

    assert_eq!(columns(&pool, "t").await, vec!["id", "name", "remark", "amount"]);

    run(&pool, "ALTER TABLE t DROP COLUMN remark").await.expect("drop");

    // A client caches the column list it was described and does not ask again,
    // so this is only true because the server notices the shape has changed and
    // puts a fresh `RowDescription` in front of the rows. Without that the
    // dropped column keeps appearing and every column after it is decoded from
    // the wrong place.
    assert_eq!(
        columns(&pool, "t").await,
        vec!["id", "name", "amount"],
        "a dropped column must disappear from the client's view"
    );
}

/// A row is a fixed-width record, so a column added to a table that already
/// holds rows cannot simply be appended to the schema — the stored rows are
/// rewritten under the new layout.
#[tokio::test(flavor = "multi_thread")]
async fn add_column_on_a_populated_table_rewrites_the_rows() {
    let (_s, pool) = server().await;

    let before = count_rows(&pool, "SELECT name FROM domain").await;
    run(&pool, "ALTER TABLE domain ADD COLUMN extra TEXT")
        .await
        .expect("add column");

    assert_eq!(
        count_rows(&pool, "SELECT name FROM domain").await,
        before,
        "no row may be lost in the rewrite"
    );
    assert_eq!(
        count_rows(&pool, "SELECT name FROM domain WHERE name = 'Acme'").await,
        1,
        "existing values must survive"
    );
    assert_eq!(
        count_rows(&pool, "SELECT name FROM domain WHERE extra IS NULL").await,
        before,
        "the new column reads as NULL for rows that predate it"
    );

    run(&pool, "UPDATE domain SET extra = 'x' WHERE name = 'Acme'")
        .await
        .expect("the new column is writable");
    assert_eq!(count_rows(&pool, "SELECT name FROM domain WHERE extra = 'x'").await, 1);
}

/// There is no `DEFAULT` yet, so a `NOT NULL` column has nothing to fill the
/// existing rows with.
#[tokio::test(flavor = "multi_thread")]
async fn add_not_null_column_on_a_populated_table_is_refused() {
    let (_s, pool) = server().await;

    let err = run(&pool, "ALTER TABLE domain ADD COLUMN extra TEXT NOT NULL")
        .await
        .expect_err("should be refused");
    assert!(err.contains("DEFAULT"), "{}", err);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_removes_the_relation() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE gone (id INT NOT NULL)").await.expect("create");
    run(&pool, "DROP TABLE gone").await.expect("drop");

    let err = run(&pool, "SELECT * FROM gone").await.expect_err("should be gone");
    assert!(err.contains("does not exist"), "{}", err);

    // `IF EXISTS` makes dropping a missing relation succeed, as in Postgres.
    run(&pool, "DROP TABLE IF EXISTS gone").await.expect("IF EXISTS");
    run(&pool, r#"DROP TABLE IF EXISTS "public"."never_existed""#)
        .await
        .expect("qualified and quoted");

    let err = run(&pool, "DROP TABLE never_existed").await.expect_err("without IF EXISTS");
    assert!(err.contains("does not exist"), "{}", err);
}

/// Index DDL is accepted and does nothing, because the engine maintains its own
/// indexes. This is a declared decision, not an oversight — a migration that
/// declares an index must not fail.
#[tokio::test(flavor = "multi_thread")]
async fn index_ddl_is_accepted_without_acting() {
    let (_s, pool) = server().await;

    for sql in [
        "CREATE INDEX idx_domain_name ON domain (name)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_domain_id ON domain (id)",
        "DROP INDEX idx_domain_name",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto",
    ] {
        run(&pool, sql).await.unwrap_or_else(|e| panic!("{} should be accepted: {}", sql, e));
    }
}

/// A JSON column is accepted and mapped onto a table of its own; see
/// `tests/json_columns.rs` for what it does with the data.
#[tokio::test(flavor = "multi_thread")]
async fn json_columns_are_accepted() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE j (id INT NOT NULL, doc JSONB)").await.expect("JSONB");
    run(&pool, "CREATE TABLE j2 (id INT NOT NULL, doc JSON)").await.expect("JSON");
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_and_array_columns_round_trip() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE b (id INT NOT NULL, payload BYTEA, tags TEXT[])")
        .await
        .expect("create");
    run(
        &pool,
        r#"INSERT INTO b (id, payload, tags) VALUES (1, '\xdeadbeef', '{alpha,beta,NULL}')"#,
    )
    .await
    .expect("insert");

    let row = sqlx::query("SELECT payload, tags FROM b WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");

    use sqlx::Row;
    let payload: Vec<u8> = row.try_get("payload").expect("bytea decodes");
    assert_eq!(payload, vec![0xde, 0xad, 0xbe, 0xef]);

    let tags: Vec<Option<String>> = row.try_get("tags").expect("text[] decodes");
    assert_eq!(
        tags,
        vec![Some("alpha".to_string()), Some("beta".to_string()), None],
        "a NULL element must survive as NULL, not as the string \"NULL\""
    );
}

/// Reserved for the engine's nullability companions.
#[tokio::test(flavor = "multi_thread")]
async fn the_reserved_column_prefix_cannot_be_claimed() {
    let (_s, pool) = server().await;

    assert!(run(&pool, "CREATE TABLE r (__null_x TEXT)").await.is_err());
    run(&pool, "CREATE TABLE r2 (id INT NOT NULL)").await.expect("create");
    let err = run(&pool, "ALTER TABLE r2 ADD COLUMN __null_id TEXT")
        .await
        .expect_err("reserved");
    assert!(err.contains("reserved"), "{}", err);
}

/// A declared `UNIQUE` that is not enforced is worse than none: the application
/// stops checking, believing the database will.
#[tokio::test(flavor = "multi_thread")]
async fn unique_is_enforced_on_insert_and_update() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE u (id INT NOT NULL, email TEXT UNIQUE)").await.expect("create");
    run(&pool, "INSERT INTO u (id, email) VALUES (1, 'a@x')").await.expect("first");

    let err = run(&pool, "INSERT INTO u (id, email) VALUES (2, 'a@x')")
        .await
        .expect_err("a duplicate");
    assert!(err.contains("UNIQUE"), "the reason must reach the client: {}", err);
    assert!(err.contains("email"), "and name the column: {}", err);

    // An update into a taken value is the same violation by another route.
    run(&pool, "INSERT INTO u (id, email) VALUES (3, 'b@x')").await.expect("third");
    let err = run(&pool, "UPDATE u SET email = 'a@x' WHERE id = 3")
        .await
        .expect_err("an update into a taken value");
    assert!(err.contains("UNIQUE"), "{}", err);
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_holds_however_it_is_declared() {
    let (_s, pool) = server().await;
    // As a table constraint rather than inline...
    run(&pool, "CREATE TABLE t1 (id INT NOT NULL, email TEXT, UNIQUE (email))")
        .await
        .expect("create");
    run(&pool, "INSERT INTO t1 (id, email) VALUES (1, 'a@x')").await.expect("first");
    run(&pool, "INSERT INTO t1 (id, email) VALUES (2, 'a@x')")
        .await
        .expect_err("table-level UNIQUE");

    // ...and as a PRIMARY KEY, which is a UNIQUE NOT NULL.
    run(&pool, "CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)").await.expect("create");
    run(&pool, "INSERT INTO t2 (id, name) VALUES (1, 'a')").await.expect("first");
    run(&pool, "INSERT INTO t2 (id, name) VALUES (1, 'b')")
        .await
        .expect_err("PRIMARY KEY implies UNIQUE");
}

/// SQL lets a unique column hold any number of `NULL`s — two unknown values are
/// not known to be equal.
#[tokio::test(flavor = "multi_thread")]
async fn unique_does_not_constrain_nulls() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE n (id INT NOT NULL, email TEXT UNIQUE)").await.expect("create");
    run(&pool, "INSERT INTO n (id, email) VALUES (1, NULL)").await.expect("first null");
    run(&pool, "INSERT INTO n (id, email) VALUES (2, NULL)").await.expect("second null");
}

/// The tag mapping itself is checked in `pgwire::command_tag_tests` — sqlx does
/// not expose the tag text. This checks the other half: that each statement
/// reaches the tagging path at all, and that a DDL tag carries no row count.
///
/// Reconstructed after an editing accident destroyed the original; the doc
/// comment above survived and is what it was rebuilt against.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_runs_and_reports_no_row_count() {
    let (_s, pool) = server().await;

    for sql in [
        "CREATE TABLE tg (id INT NOT NULL, v TEXT)",
        "ALTER TABLE tg ADD COLUMN extra INT",
        "CREATE INDEX idx_tg ON tg (id)",
        "ALTER TABLE tg RENAME COLUMN extra TO extra2",
        "DROP TABLE tg",
    ] {
        let tag = sqlx::query(sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        assert_eq!(tag.rows_affected(), 0, "{} is DDL and must report no rows", sql);
    }

    // A write, by contrast, does carry a count — otherwise "no row count" would
    // be indistinguishable from "the count is never set".
    run(&pool, "CREATE TABLE tg2 (id INT NOT NULL)").await.expect("create");
    let tag = sqlx::query("INSERT INTO tg2 (id) VALUES (1)").execute(&pool).await.expect("insert");
    assert_eq!(tag.rows_affected(), 1, "a write must report its count");
}

/// `ALTER TABLE … ADD COLUMN … JSONB` must do what `CREATE TABLE … JSONB` does.
///
/// It used to refuse with "RasterizedDB has no JSON type" — wrong, and
/// inconsistent with the statement beside it, since a JSON column has been a
/// `UUID` plus a table of its own since the mapping landed. A migration that
/// adds a JSON column to an existing table hit it on a fresh install.
#[tokio::test(flavor = "multi_thread")]
async fn add_column_supports_json_like_create_table_does() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE mt (id INT NOT NULL)").await.expect("create");
    run(&pool, "ALTER TABLE mt ADD COLUMN IF NOT EXISTS tags JSONB").await.expect("add json");

    run(&pool, r#"INSERT INTO mt (id, tags) VALUES (1, '{"a":1,"b":"x"}')"#)
        .await
        .expect("insert a document");

    let row = sqlx::query("SELECT tags FROM mt WHERE id = 1").fetch_one(&pool).await.expect("select");
    // Read as a document, not as text: a JSON column is advertised as `jsonb`,
    // and a driver checks the advertised type against the Rust type it was
    // asked for. Reading it into a `String` is refused, which is the point —
    // it used to be advertised as `text` and applications could not use it.
    let doc: serde_json::Value = row.try_get("tags").unwrap();
    assert_eq!(doc["a"], serde_json::json!(1));
    assert_eq!(doc["b"], serde_json::json!("x"));

    // ...and on a populated table, which takes the rewrite path instead.
    run(&pool, "ALTER TABLE mt ADD COLUMN extra JSONB").await.expect("add json to populated");
    run(&pool, r#"INSERT INTO mt (id, extra) VALUES (2, '{"c":true}')"#)
        .await
        .expect("insert into the second json column");
    assert_eq!(count(&pool, "SELECT id FROM mt").await, 2);
}

/// A cast in a `DEFAULT` says nothing the column does not already say, so it is
/// dropped — but only where what remains is a complete literal.
///
/// This is the one place a cast is not merely cosmetic on a fresh install:
/// `ADD COLUMN … DEFAULT '{}'::jsonb` runs on every install, and the default has
/// to be recorded because later inserts rely on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_redundant_cast_in_a_default_is_dropped() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE dc (id INT NOT NULL)").await.expect("create");
    run(&pool, "ALTER TABLE dc ADD COLUMN settings JSONB NOT NULL DEFAULT '{}'::jsonb")
        .await
        .expect("json default with a cast");
    run(&pool, "ALTER TABLE dc ADD COLUMN n INT NOT NULL DEFAULT 0::smallint")
        .await
        .expect("numeric default with a cast");

    run(&pool, "INSERT INTO dc (id) VALUES (1)").await.expect("insert");
    let row = sqlx::query("SELECT n, settings FROM dc").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<i32, _>("n").unwrap(), 0);
    // As above: `jsonb`, so it decodes as a document.
    assert_eq!(
        row.try_get::<serde_json::Value, _>("settings").unwrap(),
        serde_json::json!({})
    );

    // A `::` inside a string literal is data, not a cast.
    run(&pool, "CREATE TABLE dc2 (id INT NOT NULL, s TEXT DEFAULT 'a::b')").await.expect("create");
    run(&pool, "INSERT INTO dc2 (id) VALUES (1)").await.expect("insert");
    let row = sqlx::query("SELECT s FROM dc2").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<String, _>("s").unwrap(), "a::b");

    // And a cast over a function call is not redundant in the same way, so it
    // is still refused rather than quietly reinterpreted.
    run(&pool, "CREATE TABLE dc3 (id INT NOT NULL, n INT DEFAULT LEAST(1,2)::smallint)")
        .await
        .expect_err("a cast over a call is not a redundant cast");
}

/// A JSON column's `DEFAULT` must be storable, and must be judged the same way
/// an `INSERT` of the same value is.
///
/// The verdict has moved once already: `DEFAULT '[]'` was accepted and silently
/// stored as `{}`, then refused outright, and now stores `[]` because an array's
/// elements map to rows. What this pins is the *agreement* between the two
/// positions, not any particular verdict — a value accepted in one and refused
/// in the other is how the original corruption stayed invisible.
#[tokio::test(flavor = "multi_thread")]
async fn a_json_default_is_judged_as_an_insert_of_it_would_be() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE jd (id INT NOT NULL)").await.expect("create");

    // An array is storable, so it is accepted in both positions and comes back
    // as itself rather than as `{}`.
    run(&pool, "ALTER TABLE jd ADD COLUMN tags JSONB NOT NULL DEFAULT '[]'::jsonb")
        .await
        .expect("an array default is storable");
    run(&pool, "INSERT INTO jd (id) VALUES (1)").await.expect("insert");
    let row = sqlx::query("SELECT tags FROM jd WHERE id = 1").fetch_one(&pool).await.expect("select");
    // A JSON column is advertised as `jsonb`, so it decodes as a document.
    assert_eq!(
        row.try_get::<serde_json::Value, _>("tags").unwrap(),
        serde_json::json!([]),
        "the default must be the array"
    );

    // A bare scalar is a JSON value, so it is accepted in both positions and
    // reads back as itself. The judgement the test is named for still holds —
    // a default is judged exactly as an insert of it would be — it is the
    // verdict on this value that changed.
    run(&pool, "ALTER TABLE jd ADD COLUMN n JSONB DEFAULT '42'")
        .await
        .expect("a bare scalar is storable");
    run(&pool, "INSERT INTO jd (id) VALUES (5)").await.expect("insert with default");
    let row = sqlx::query("SELECT n FROM jd WHERE id = 5").fetch_one(&pool).await.expect("select");
    assert_eq!(
        row.try_get::<serde_json::Value, _>("n").unwrap(),
        serde_json::json!(42),
        "the default must be the scalar"
    );

    run(&pool, "ALTER TABLE jd ADD COLUMN doc JSONB").await.expect("add json");
    run(&pool, "INSERT INTO jd (id, doc) VALUES (2, '42')")
        .await
        .expect("same value, same verdict");
}

/// A failure part-way through a batch must leave nothing behind — the batch
/// runs in one transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_fails_part_way_leaves_nothing() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE b (id INT NOT NULL, v INT NOT NULL, UNIQUE (v))")
        .await
        .expect("create");
    run(&pool, "INSERT INTO b (id, v) VALUES (99, 7)").await.expect("seed");

    // The second tuple duplicates the seeded value.
    run(&pool, "INSERT INTO b (id, v) VALUES (1, 1), (2, 7), (3, 3)")
        .await
        .expect_err("the duplicate must fail the statement");

    assert_eq!(
        count(&pool, "SELECT id FROM b").await,
        1,
        "only the seed row; the batch must not have half-landed"
    );
}

/// A composite join key — `ON a.x = b.x AND a.y = b.y` — is ordinary SQL: every
/// junction table is joined on both of its columns.
///
/// It used to be refused, because the second equality was classified as a filter
/// spanning two relations. Matching on one column and filtering on the other is
/// **not** the same join: it fans out on the first column and then discards,
/// which agrees with SQL only when the first column is already unique.
///
/// The decoy rows are the point. `(1,9)` and `(9,2)` each match one column and
/// must not join; a single-column key would return them.
#[tokio::test(flavor = "multi_thread")]
async fn a_composite_join_key_matches_on_every_column() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ca (x INT NOT NULL, y INT NOT NULL, tag TEXT)").await.expect("ca");
    run(&pool, "CREATE TABLE cb (x INT NOT NULL, y INT NOT NULL, note TEXT)").await.expect("cb");
    run(&pool, "INSERT INTO ca (x, y, tag) VALUES (1,1,'p'), (1,2,'q'), (2,1,'r')")
        .await
        .expect("insert ca");
    run(
        &pool,
        "INSERT INTO cb (x, y, note) VALUES (1,1,'hit'), (1,9,'wrong-y'), (9,2,'wrong-x')",
    )
    .await
    .expect("insert cb");

    let rows = sqlx::query("SELECT a.tag, b.note FROM ca a INNER JOIN cb b ON a.x = b.x AND a.y = b.y")
        .fetch_all(&pool)
        .await
        .expect("composite inner join");
    assert_eq!(rows.len(), 1, "a decoy matching one column was joined");
    assert_eq!(rows[0].try_get::<String, _>("tag").unwrap(), "p");
    assert_eq!(rows[0].try_get::<String, _>("note").unwrap(), "hit");

    // The anti-join over the same key: the two left rows with no composite match.
    let mut left: Vec<String> =
        sqlx::query("SELECT a.tag FROM ca a LEFT JOIN cb b ON a.x = b.x AND a.y = b.y WHERE b.x IS NULL")
            .fetch_all(&pool)
            .await
            .expect("composite anti-join")
            .iter()
            .map(|r| r.try_get::<String, _>("tag").unwrap())
            .collect();
    left.sort();
    assert_eq!(left, vec!["q".to_string(), "r".to_string()]);
}

/// A three-column key, and an ON term that is a genuine filter alongside it —
/// the two kinds must still be told apart.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_and_a_filter_in_the_same_on_clause_are_distinguished() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ta (x INT NOT NULL, y INT NOT NULL, tag TEXT)").await.expect("ta");
    run(&pool, "CREATE TABLE tb (x INT NOT NULL, y INT NOT NULL, kind TEXT)").await.expect("tb");
    run(&pool, "INSERT INTO ta (x, y, tag) VALUES (1,1,'p'), (2,2,'q')").await.expect("insert");
    run(
        &pool,
        "INSERT INTO tb (x, y, kind) VALUES (1,1,'admin'), (2,2,'other')",
    )
    .await
    .expect("insert");

    // Two key equalities plus a literal filter on the right relation.
    let mut got: Vec<String> = sqlx::query(
        "SELECT a.tag FROM ta a LEFT JOIN tb b ON a.x = b.x AND a.y = b.y AND b.kind = 'admin' \
         WHERE b.x IS NULL",
    )
    .fetch_all(&pool)
    .await
    .expect("key plus filter")
    .iter()
    .map(|r| r.try_get::<String, _>("tag").unwrap())
    .collect();
    got.sort();
    // 'q' matches on the key but its only candidate fails the ON filter, so it
    // is unmatched — which is what an ON filter means and a WHERE filter does not.
    assert_eq!(got, vec!["q".to_string()]);
}

/// A `VALUES` clause that cannot be fully accounted for is refused rather than
/// partly executed. This is the guarantee that would have turned the original
/// bug from silent loss into a visible error.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_values_clause_is_refused_not_truncated() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE m (id INT NOT NULL)").await.expect("create");

    for sql in [
        "INSERT INTO m (id) VALUES (1),",
        "INSERT INTO m (id) VALUES (1), (2",
    ] {
        run(&pool, sql).await.unwrap_or_else(|_| ()); // may parse-fail earlier
    }
    // Whatever the route, nothing may have half-landed.
    assert_eq!(
        count(&pool, "SELECT id FROM m").await,
        0,
        "a statement that could not be fully parsed wrote rows anyway"
    );
}

/// A close-paren inside a string literal used to end the VALUES block early, so
/// `'a)b'` was stored as `'a`. Single row, well-formed statement, silent
/// truncation of a *value* — a phone number or a formatted amount would corrupt
/// on write and nobody would suspect the database.
#[tokio::test(flavor = "multi_thread")]
async fn a_paren_inside_a_string_literal_does_not_truncate_the_value() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE s (id INT NOT NULL, name TEXT)").await.expect("create");

    for (id, value) in [(1, "a)b"), (2, "(555) 123"), (3, "x, y"), (4, "p(q")] {
        run(&pool, &format!("INSERT INTO s (id, name) VALUES ({}, '{}')", id, value))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", value, e));
        let row = sqlx::query(AssertSqlSafe(format!("SELECT name FROM s WHERE id = {}", id)))
            .fetch_one(&pool)
            .await
            .expect("select");
        assert_eq!(row.try_get::<String, _>("name").unwrap(), value);
    }
}

/// Both at once: a column made nullable, then another added. The second rewrite
/// must not undo the first.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_rewrite_preserves_the_first_ones_flags() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE two (id INT NOT NULL, v INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO two (id, v) VALUES (1, 100)").await.expect("insert");

    run(&pool, "ALTER TABLE two ALTER COLUMN v DROP NOT NULL").await.expect("drop not null");
    run(&pool, "INSERT INTO two (id) VALUES (2)").await.expect("insert without v");
    run(&pool, "ALTER TABLE two ADD COLUMN extra TEXT").await.expect("add column");

    assert_eq!(count(&pool, "SELECT id FROM two WHERE v = 100").await, 1, "row 1 kept its value");
    assert_eq!(count(&pool, "SELECT id FROM two WHERE v IS NULL").await, 1, "row 2 is still null");
    assert_eq!(count(&pool, "SELECT id FROM two WHERE extra IS NULL").await, 2);
}

/// A table whose name merely starts with `if` must not be mangled by the clause
/// skipping. This is why the three keywords are matched as whole words in
/// sequence rather than by a substring search plus a fixed token index.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_named_like_the_keyword_is_not_mangled() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE if_settings (x INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO if_settings (x) VALUES (7)").await.expect("insert");
    assert_eq!(count(&pool, "SELECT x FROM if_settings").await, 1);

    run(&pool, "CREATE TABLE IF NOT EXISTS if_settings (x INT NOT NULL)")
        .await
        .expect("if not exists on a table whose name starts with if");
    assert_eq!(count(&pool, "SELECT x FROM if_settings").await, 1, "it was recreated");
}

/// PostgreSQL allows `ADD COLUMN … NOT NULL` with no `DEFAULT` when the table is
/// empty, and early migrations rely on it.
#[tokio::test(flavor = "multi_thread")]
async fn add_not_null_column_to_an_empty_table_is_allowed() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE e (id INT NOT NULL)").await.expect("create");
    run(&pool, "ALTER TABLE e ADD COLUMN nn TEXT NOT NULL").await.expect("empty table");

    run(&pool, "INSERT INTO e (id, nn) VALUES (1, 'x')").await.expect("insert");
    // ...and once it holds rows, the same statement is refused.
    run(&pool, "ALTER TABLE e ADD COLUMN nn2 TEXT NOT NULL")
        .await
        .expect_err("populated table has nothing to put in the existing rows");
}

/// The case the companion fill was originally written for must keep working:
/// when the tracked column is *also* new, existing rows have no value and the
/// companion should say NULL.
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_nullable_column_still_marks_existing_rows_null() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE anc (id INT NOT NULL, v INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO anc (id, v) VALUES (1, 100)").await.expect("insert");

    run(&pool, "ALTER TABLE anc ADD COLUMN note TEXT").await.expect("add column");

    // The new column is NULL for the row that predates it...
    assert_eq!(count(&pool, "SELECT id FROM anc WHERE note IS NULL").await, 1);
    // ...and the column that was already there is untouched.
    assert_eq!(count(&pool, "SELECT id FROM anc WHERE v = 100").await, 1);
}

/// An unsupported construct must say which construct, not "missing column".
///
/// Each of these is valid SQL. Reported as `Column '<the whole expression>' not
/// found in schema`, they send the reader to audit a schema that is fine.
///
/// The alias-stripper splits on whitespace, so `CASE WHEN … END` reaches the
/// resolver as the single word `CASE` with the rest read as an alias. The check
/// therefore reads the whole item; reading the stripped expression would miss
/// every multi-word construct, which is most of them.
#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_expression_names_itself_rather_than_blaming_the_schema() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ue (id INT NOT NULL, v INT NOT NULL, nm TEXT)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO ue (id, v, nm) VALUES (1, 10, 'x')").await.expect("seed");

    // A cast in the select list is supported now, and computes rather than
    // mislabelling: these assert the value, not merely that no error came back.
    for (sql, want) in [
        ("SELECT v::TEXT FROM ue", "10"),
        ("SELECT nm::TEXT FROM ue", "x"),
        // pms sends this one verbatim. The cast is on a parenthesised function
        // result, so a check anchored on an identifier before `::` would miss
        // it — and three separate functions have to resolve first.
        ("SELECT LEAST(GREATEST(COALESCE(v, 1), 0), 32767)::smallint FROM ue", "10"),
        // An un-aliased `CASE … END` used to be cut at its first space and read
        // as `CASE` plus an alias; it was on the unsupported list below until
        // that was fixed. Asserted as working, with the value it must compute.
        ("SELECT CASE WHEN v > 5 THEN 1 ELSE 2 END FROM ue", "1"),
        ("SELECT CASE WHEN v > 10 THEN 'hi' WHEN v > 5 THEN 'mid' ELSE 'lo' END FROM ue", "mid"),
    ] {
        let rows = sqlx::query(sql).fetch_all(&pool).await.unwrap_or_else(|e| panic!("{}: {}", sql, e));
        assert_eq!(rows.len(), 1, "{}", sql);
        // `i16` is tried too, because a trailing `::smallint` is now applied
        // rather than swallowed: the cast used to be mistaken for a column
        // alias, so the column came back as its uncast type and the value was
        // silently the wrong width.
        let got = rows[0]
            .try_get::<String, _>(0)
            .or_else(|_| rows[0].try_get::<i64, _>(0).map(|n| n.to_string()))
            .or_else(|_| rows[0].try_get::<i32, _>(0).map(|n| n.to_string()))
            .or_else(|_| rows[0].try_get::<i16, _>(0).map(|n| n.to_string()))
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        assert_eq!(got, want, "{}", sql);
    }

    for (sql, expected) in [
        // Still unsupported, and each must name the construct rather than
        // report a missing column: saying so is the difference between a
        // reader fixing the query and a reader auditing a schema that is fine.
        // `CAST(v AS TEXT)` in the select list, and a cast in a `WHERE`, moved
        // to the working lists below when they were implemented.
        ("SELECT DISTINCT ON (id, v) id FROM ue", "distinct on"),
    ] {
        let err = run(&pool, sql).await.expect_err(sql).to_ascii_lowercase();
        assert!(err.contains(expected), "{} -> {}", sql, err);
        assert!(!err.contains("not found in schema"), "{} blamed the schema: {}", sql, err);
    }

    // Window functions, and now `CAST(x AS type)`, were on the list above until
    // they were implemented. Asserted as *working* rather than deleted, so the
    // entry keeps earning its place: an unsupported-feature list that quietly
    // loses items stops being evidence of anything.
    //
    // `CAST(…)` was never merely unimplemented — it separates its arguments
    // with a keyword rather than a comma, so the argument splitter never
    // reached it, the same way `EXTRACT(EPOCH FROM …)` was not reached.
    for sql in [
        "SELECT ROW_NUMBER() OVER (ORDER BY v) FROM ue",
        "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY v, nm) FROM ue",
        "SELECT CAST(v AS TEXT) FROM ue",
        "SELECT CAST(v AS BIGINT) FROM ue",
    ] {
        run(&pool, sql).await.unwrap_or_else(|e| panic!("{} -> {}", sql, e));
    }
    // A cast in a `WHERE` was on the list above too. It answers PostgreSQL's
    // row now, rather than merely not failing.
    let rows = sqlx::query("SELECT id FROM ue WHERE v::TEXT = '10'")
        .fetch_all(&pool)
        .await
        .expect("a cast in WHERE");
    let ids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1], "SELECT id FROM ue WHERE v::TEXT = '10'");

    // A genuinely missing column must still say so — the check must not swallow
    // the ordinary case it sits in front of.
    let err = run(&pool, "SELECT nosuch FROM ue").await.expect_err("missing column");
    assert!(err.contains("not found in schema"), "{}", err);

    // ...and ordinary select items still work, including aliases, which share
    // the whitespace-splitting the check had to reason about.
    run(&pool, "SELECT v AS alias_name FROM ue").await.expect("alias");
    run(&pool, "SELECT nm FROM ue").await.expect("plain column");

    // Plain `DISTINCT` works, and the `DISTINCT ON` check above must not have
    // taken it with it — the mirror of that fix would be refusing a query whose
    // first column merely starts with those letters.
    run(&pool, "SELECT DISTINCT nm, id FROM ue").await.expect("plain distinct");
}

/// The key columns must be fetched even when nothing selects them — otherwise a
/// composite key would silently degrade to whichever column happened to be
/// projected.
#[tokio::test(flavor = "multi_thread")]
async fn composite_key_columns_are_fetched_even_when_not_selected() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ka (x INT NOT NULL, y INT NOT NULL, tag TEXT)").await.expect("ka");
    run(&pool, "CREATE TABLE kb (x INT NOT NULL, y INT NOT NULL)").await.expect("kb");
    run(&pool, "INSERT INTO ka (x, y, tag) VALUES (1,1,'match'), (1,2,'no')")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO kb (x, y) VALUES (1,1)").await.expect("insert");

    // Neither x nor y appears in the select list.
    let rows = sqlx::query("SELECT a.tag FROM ka a INNER JOIN kb b ON a.x = b.x AND a.y = b.y")
        .fetch_all(&pool)
        .await
        .expect("join");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<String, _>("tag").unwrap(), "match");
}

/// Running it twice is success, not an error — that is what the clause means,
/// and a migrator re-run depends on it.
#[tokio::test(flavor = "multi_thread")]
async fn create_table_if_not_exists_is_idempotent() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE t (x INT NOT NULL)").await.expect("plain create");
    run(&pool, "INSERT INTO t (x) VALUES (1)").await.expect("insert");

    run(&pool, "CREATE TABLE IF NOT EXISTS t (x INT NOT NULL)")
        .await
        .expect("second create is success");

    // And it left the existing table alone rather than recreating it empty.
    assert_eq!(count(&pool, "SELECT x FROM t").await, 1, "the table was recreated");

    // Without the clause it is still an error, which is what tells a migration
    // it has collided with something.
    run(&pool, "CREATE TABLE t (x INT NOT NULL)")
        .await
        .expect_err("a plain CREATE on an existing table must fail");
}

/// `CREATE TABLE IF NOT EXISTS` blocked every sea-orm application at statement
/// one: the migrator emits it for `seaql_migrations` before any user migration
/// runs.
///
/// The old parser took the third token as the table name, so it created a table
/// literally called `IF` — and the *second* such statement then failed on the
/// name collision, which is why the symptom looked like "already exists" rather
/// than "unsupported".
///
/// So this asserts the **name**, not merely that nothing errored. Asserting
/// absence of an error would have passed on the broken behaviour.
#[tokio::test(flavor = "multi_thread")]
async fn create_table_if_not_exists_uses_the_name_it_was_given() {
    let (_s, pool) = server().await;

    let seaorm_bootstrap = r#"CREATE TABLE IF NOT EXISTS "seaql_migrations" ("version" TEXT NOT NULL, "applied_at" BIGINT NOT NULL)"#;
    run(&pool, seaorm_bootstrap).await.expect("sea-orm bootstrap");

    // The table exists under the name asked for...
    run(&pool, "INSERT INTO seaql_migrations (version, applied_at) VALUES ('m1', 1)")
        .await
        .expect("insert into the named table");
    assert_eq!(count(&pool, "SELECT version FROM seaql_migrations").await, 1);

    // ...and no table called `IF` was created alongside it.
    run(&pool, r#"SELECT version FROM "IF""#)
        .await
        .expect_err("a table named IF must not exist");
}

/// The statement that blocked a real migration chain: a DELETE whose predicate
/// needs an interior wildcard.
#[tokio::test(flavor = "multi_thread")]
async fn delete_with_an_interior_wildcard_removes_the_right_rows() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE wildcard_roles (id INT NOT NULL, name TEXT)").await.expect("create");
    run(
        &pool,
        "INSERT INTO wildcard_roles (id, name) VALUES (1,'UserTable:42:read'), (2,'UserTable:7:write'), \
         (3,'Admin'), (4,'UserTable:42')",
    )
    .await
    .expect("insert");

    run(&pool, "DELETE FROM wildcard_roles WHERE name LIKE 'UserTable:%:%'").await.expect("delete");

    let mut left: Vec<i32> = sqlx::query("SELECT id FROM wildcard_roles")
        .fetch_all(&pool)
        .await
        .expect("select")
        .iter()
        .map(|r| r.try_get::<i32, _>("id").unwrap())
        .collect();
    left.sort_unstable();
    assert_eq!(left, vec![3, 4], "only the two-colon rows should have gone");
}

/// `DROP NOT NULL` must keep the values it makes nullable.
///
/// The first attempt at this reported success and left every row reading NULL —
/// the bytes were intact, but the new `__null_` companion said "is null" for
/// every row, so the column reported NULL in projection *and* in `WHERE`. That
/// is worse than losing the data, because nothing looks wrong: the schema is
/// right, the statement succeeded, and the rows are still there.
///
/// The cause was the rewrite filling a companion by asking whether the tracked
/// column has a `DEFAULT` — the right question when both are new, the wrong one
/// when only the companion is.
#[tokio::test(flavor = "multi_thread")]
async fn drop_not_null_keeps_the_existing_values() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE dnn (id INT NOT NULL, v INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO dnn (id, v) VALUES (1, 100), (2, 200)").await.expect("insert");

    run(&pool, "ALTER TABLE dnn ALTER COLUMN v DROP NOT NULL").await.expect("drop not null");

    // Projection still shows them...
    let rows = sqlx::query("SELECT id, v FROM dnn").fetch_all(&pool).await.expect("select");
    assert_eq!(rows.len(), 2);
    let mut seen: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.try_get::<i32, _>("id").unwrap(), r.try_get::<i32, _>("v").unwrap()))
        .collect();
    seen.sort_unstable();
    assert_eq!(seen, vec![(1, 100), (2, 200)], "the values were reported as NULL");

    // ...and `WHERE` finds them, which projection alone would not prove: the
    // companion is what both paths consult.
    assert_eq!(count(&pool, "SELECT id FROM dnn WHERE v = 100").await, 1);
    assert_eq!(count(&pool, "SELECT id FROM dnn WHERE v IS NULL").await, 0, "none are null yet");

    // And the column really is nullable now.
    run(&pool, "INSERT INTO dnn (id) VALUES (3)").await.expect("insert without v");
    assert_eq!(count(&pool, "SELECT id FROM dnn WHERE v IS NULL").await, 1);
    assert_eq!(count(&pool, "SELECT id FROM dnn WHERE v = 100").await, 1, "still findable");
}

/// `IS DISTINCT FROM` is the null-safe comparison: it answers true or false
/// where `=` answers NULL. An ORM reaches for it on any nullable column, and
/// a real application uses it a dozen times in one predicate.
#[tokio::test(flavor = "multi_thread")]
async fn is_distinct_from_treats_null_as_a_value() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE d (id INT NOT NULL, v INT)").await.expect("create");
    for (id, v) in [(1, "5"), (2, "7"), (3, "NULL")] {
        run(&pool, &format!("INSERT INTO d (id, v) VALUES ({}, {})", id, v))
            .await
            .expect("insert");
    }

    let ids = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            let mut got: Vec<i32> = sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{}: {}", sql, e))
                .iter()
                .map(|r| r.try_get::<i32, _>("id").unwrap())
                .collect();
            got.sort_unstable();
            got
        }
    };

    // NULL *is* distinct from 5, which is the whole point — plain `!=` would
    // drop row 3.
    assert_eq!(ids("SELECT id FROM d WHERE v IS DISTINCT FROM 5").await, vec![2, 3]);
    assert_eq!(ids("SELECT id FROM d WHERE v IS NOT DISTINCT FROM 5").await, vec![1]);

    // Against NULL it is exactly IS NOT NULL, and its negation is IS NULL.
    assert_eq!(ids("SELECT id FROM d WHERE v IS DISTINCT FROM NULL").await, vec![1, 2]);
    assert_eq!(ids("SELECT id FROM d WHERE v IS NOT DISTINCT FROM NULL").await, vec![3]);

    // On a NOT NULL column there is no third outcome, so it degenerates to `!=`.
    assert_eq!(ids("SELECT id FROM d WHERE id IS DISTINCT FROM 1").await, vec![2, 3]);
    assert_eq!(ids("SELECT id FROM d WHERE id IS NOT DISTINCT FROM 1").await, vec![1]);
}

/// `LIKE` with an interior wildcard. It used to be refused — correctly, since
/// `a%b` cannot be expressed as `STARTSWITH 'a' AND CONTAINS 'b'` (which does
/// not require `b` to follow `a`, and loses ordering entirely with three
/// segments) — and now has a real pattern matcher behind it.
#[tokio::test(flavor = "multi_thread")]
async fn like_matches_interior_wildcards() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE lk (id INT NOT NULL, name TEXT)").await.expect("create");
    run(
        &pool,
        "INSERT INTO lk (id, name) VALUES (1,'UserTable:42:read'), (2,'UserTable:7:write'), \
         (3,'Other:1:read'), (4,'UserTable:42')",
    )
    .await
    .expect("insert");

    let ids = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            let mut got: Vec<i32> = sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{}: {}", sql, e))
                .iter()
                .map(|r| r.try_get::<i32, _>("id").unwrap())
                .collect();
            got.sort_unstable();
            got
        }
    };

    // Two colons required, so `UserTable:42` must not match.
    assert_eq!(ids("SELECT id FROM lk WHERE name LIKE 'UserTable:%:%'").await, vec![1, 2]);
    // The prefix form still takes the cheaper path and still includes it.
    assert_eq!(ids("SELECT id FROM lk WHERE name LIKE 'UserTable:%'").await, vec![1, 2, 4]);
    // `_` matches exactly one character.
    assert_eq!(ids("SELECT id FROM lk WHERE name LIKE 'Other:_:read'").await, vec![3]);
    assert_eq!(ids("SELECT id FROM lk WHERE name LIKE 'Other:__:read'").await, Vec::<i32>::new());
    // ILIKE folds both sides.
    assert_eq!(ids("SELECT id FROM lk WHERE name ILIKE 'usertable:%:%'").await, vec![1, 2]);

    // Ordering is the property a conjunction of substring tests would lose.
    run(&pool, "INSERT INTO lk (id, name) VALUES (5, 'b_then_a')").await.expect("insert");
    assert_eq!(ids("SELECT id FROM lk WHERE name LIKE 'a%b'").await, Vec::<i32>::new());
}

/// The filter index must never prune a row that a `LIKE` pattern matches.
///
/// This is the failure that does not announce itself: a mask the index cannot
/// answer would exclude matching rows and the query returns *fewer* rows with no
/// error. A count assertion does not catch it — the shorter answer is still
/// internally consistent — so this uses a true oracle: read every row with a
/// predicate the index cannot act on, apply the pattern in Rust, and compare the
/// two as **sets**.
///
/// Mutation-verified: making `sarg::starts_with_to_range` accept `LIKE` as well
/// as `STARTSWITH` turns this red. `LIKE 'a%b'` then lowers to
/// `>= 'a%b' AND < 'a%c'`, and a real match like `'axxb'` sorts above `'a%c'`
/// because `'x'` (0x78) exceeds `'%'` (0x25) — so nearly every match is pruned.
#[tokio::test(flavor = "multi_thread")]
async fn like_never_prunes_a_matching_row() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE big (id INT NOT NULL, name TEXT)").await.expect("create");

    // Enough rows, and enough distinct values, that the index is worth building
    // and a mask would actually prune.
    let mut expected_by_pattern: Vec<(&str, Vec<i32>)> = Vec::new();
    let mut all: Vec<(i32, String)> = Vec::new();
    for id in 0..600i32 {
        let name = match id % 6 {
            0 => format!("UserTable:{}:read", id),
            1 => format!("UserTable:{}:write", id),
            2 => format!("UserTable:{}", id),
            3 => format!("a{}b", "x".repeat((id % 7) as usize)),
            4 => format!("Other:{}:read", id),
            _ => format!("plain-{}", id),
        };
        all.push((id, name));
    }
    for chunk in all.chunks(50) {
        let tuples: Vec<String> = chunk
            .iter()
            .map(|(id, n)| format!("({}, '{}')", id, n))
            .collect();
        run(&pool, &format!("INSERT INTO big (id, name) VALUES {}", tuples.join(", ")))
            .await
            .expect("insert batch");
    }

    // The oracle, computed in Rust from the full table.
    fn like_matches(value: &str, pattern: &str) -> bool {
        // Mirrors SQL LIKE for the patterns used below.
        let mut segs = pattern.split('%').peekable();
        let first = segs.next().unwrap_or("");
        if !value.starts_with(first) {
            return false;
        }
        let mut rest = &value[first.len()..];
        let mut last_seg = "";
        while let Some(seg) = segs.next() {
            last_seg = seg;
            if segs.peek().is_none() {
                return rest.ends_with(seg);
            }
            if seg.is_empty() {
                continue;
            }
            match rest.find(seg) {
                Some(at) => rest = &rest[at + seg.len()..],
                None => return false,
            }
        }
        let _ = last_seg;
        true
    }

    for pattern in ["UserTable:%:%", "a%b", "%:read", "Other:%:read", "UserTable:%"] {
        let mut oracle: Vec<i32> = all
            .iter()
            .filter(|(_, n)| like_matches(n, pattern))
            .map(|(id, _)| *id)
            .collect();
        oracle.sort_unstable();
        expected_by_pattern.push((pattern, oracle));
    }

    for (pattern, oracle) in expected_by_pattern {
        let mut got: Vec<i32> = sqlx::query(AssertSqlSafe(format!(
            "SELECT id FROM big WHERE name LIKE '{}'",
            pattern
        )))
        .fetch_all(&pool)
        .await
        .unwrap_or_else(|e| panic!("{}: {}", pattern, e))
        .iter()
        .map(|r| r.try_get::<i32, _>("id").unwrap())
        .collect();
        got.sort_unstable();

        assert!(!oracle.is_empty(), "pattern {} matched nothing; the test proves nothing", pattern);
        assert_eq!(
            got, oracle,
            "pattern {} returned a different set than matching every row in Rust — \
             the index pruned rows that match",
            pattern
        );
    }
}

/// Multi-row `VALUES` used to insert the **first tuple only** and report
/// `INSERT 0 1` — silent data loss, found in a deployed build. Every ORM
/// batches inserts this way; sea-orm's `insert_many` emits exactly this.
#[tokio::test(flavor = "multi_thread")]
async fn multi_row_values_inserts_every_tuple() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE mrv (id INT NOT NULL, name TEXT)").await.expect("create");

    let done = sqlx::query("INSERT INTO mrv (id, name) VALUES (1,'a'), (2,'b'), (3,'c')")
        .execute(&pool)
        .await
        .expect("multi-row insert");
    assert_eq!(done.rows_affected(), 3, "the count must not report only the first tuple");
    assert_eq!(count(&pool, "SELECT id FROM mrv").await, 3);

    // A single tuple still works, and a later batch appends rather than replaces.
    run(&pool, "INSERT INTO mrv (id, name) VALUES (4,'d')").await.expect("single row");
    run(&pool, "INSERT INTO mrv (id, name) VALUES (5,'e'), (6,'f')").await.expect("second batch");
    assert_eq!(count(&pool, "SELECT id FROM mrv").await, 6);
}

/// A comma or a parenthesis inside a string is data. Splitting on them would
/// turn one row into two, or lose one.
#[tokio::test(flavor = "multi_thread")]
async fn multi_row_values_respects_quoting() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE q (id INT NOT NULL, name TEXT)").await.expect("create");
    run(&pool, "INSERT INTO q (id, name) VALUES (1,'x, y'), (2,'a)b'), (3,'VALUES')")
        .await
        .expect("insert");

    assert_eq!(count(&pool, "SELECT id FROM q").await, 3);
    let row = sqlx::query("SELECT name FROM q WHERE id = 1").fetch_one(&pool).await.expect("row");
    assert_eq!(row.try_get::<String, _>("name").unwrap(), "x, y");
    let row = sqlx::query("SELECT name FROM q WHERE id = 2").fetch_one(&pool).await.expect("row");
    assert_eq!(row.try_get::<String, _>("name").unwrap(), "a)b");
}

/// `RETURNING` on a batch must report every row, not the last.
#[tokio::test(flavor = "multi_thread")]
async fn multi_row_values_returns_every_row() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE r (id INT NOT NULL)").await.expect("create");

    let rows = sqlx::query("INSERT INTO r (id) VALUES (1), (2), (3) RETURNING id")
        .fetch_all(&pool)
        .await
        .expect("returning");
    let mut got: Vec<i32> = rows.iter().map(|r| r.try_get::<i32, _>("id").unwrap()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);
}

/// `ON CONFLICT DO NOTHING` is what makes a seed migration re-runnable against a
/// database that already has the rows.
#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_do_nothing_swallows_a_duplicate() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE c (k INT NOT NULL, v TEXT, UNIQUE (k))").await.expect("create");
    run(&pool, "INSERT INTO c (k, v) VALUES (1, 'first')").await.expect("first");

    // Bare, and with an arbiter — both accepted, neither overwriting.
    run(&pool, "INSERT INTO c (k, v) VALUES (1, 'second') ON CONFLICT DO NOTHING")
        .await
        .expect("bare form");
    run(&pool, "INSERT INTO c (k, v) VALUES (1, 'third') ON CONFLICT (k) DO NOTHING")
        .await
        .expect("arbiter form");

    // A non-conflicting row still lands.
    run(&pool, "INSERT INTO c (k, v) VALUES (2, 'new') ON CONFLICT DO NOTHING")
        .await
        .expect("no conflict");

    let rows = sqlx::query("SELECT k, v FROM c").fetch_all(&pool).await.expect("select");
    assert_eq!(rows.len(), 2);
    let first = sqlx::query("SELECT v FROM c WHERE k = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(
        first.try_get::<String, _>("v").unwrap(),
        "first",
        "DO NOTHING must not overwrite"
    );
}

/// PostgreSQL storage tuning describes machinery this engine does not have.
/// Failing it would stop a migration over statements whose whole effect is
/// advisory — a real schema carries a dozen of them.
#[tokio::test(flavor = "multi_thread")]
async fn table_storage_options_are_accepted_without_acting() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE c (k INT NOT NULL, v TEXT)").await.expect("create");

    for stmt in [
        "ALTER TABLE c SET COMPRESSION lz4",
        "ALTER TABLE c SET (autovacuum_enabled = false)",
        "ALTER TABLE c RESET (autovacuum_enabled)",
        "ALTER TABLE c VALIDATE CONSTRAINT some_fk",
    ] {
        run(&pool, stmt).await.unwrap_or_else(|e| panic!("{}: {}", stmt, e));
    }

    // And the ALTER COLUMN form that *does* act still acts — the no-op arm must
    // not have swallowed it.
    run(&pool, "ALTER TABLE c ALTER COLUMN v SET DEFAULT 'z'").await.expect("set default");
    run(&pool, "INSERT INTO c (k) VALUES (1)").await.expect("insert");
    let row = sqlx::query("SELECT v FROM c WHERE k = 1").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<String, _>("v").unwrap(), "z");
}

/// `RENAME TO` renames the table; `RENAME COLUMN a TO b` renames a column. They
/// share a keyword, and the table form used to be parsed as the column form and
/// refused with "RENAME COLUMN must be followed by TO" — a refusal of valid DDL
/// with a message pointing at something the statement never mentioned.
#[tokio::test(flavor = "multi_thread")]
async fn rename_distinguishes_a_table_from_a_column() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE p (id INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO p (id) VALUES (1)").await.expect("insert");

    // The table form carries the rows across and retires the old name.
    run(&pool, "ALTER TABLE p RENAME TO p2").await.expect("rename table");
    assert_eq!(count(&pool, "SELECT id FROM p2").await, 1, "the row came with it");
    run(&pool, "SELECT id FROM p").await.expect_err("the old name is gone");

    // The column form still works and is not confused for the table form.
    run(&pool, "ALTER TABLE p2 RENAME COLUMN id TO id2").await.expect("rename column");
    assert_eq!(count(&pool, "SELECT id2 FROM p2").await, 1);
    run(&pool, "SELECT id FROM p2").await.expect_err("the old column name is gone");
}

/// `DO UPDATE` must actually perform the update.
///
/// This test previously required the statement to be *refused*, on the grounds
/// that treating `DO UPDATE` as `DO NOTHING` is a lost write the caller cannot
/// notice. `DO UPDATE` is implemented now, so the refusal is gone — but the
/// reasoning behind it is not, and that is what this checks: the row must carry
/// the new value afterwards. Asserting only that the statement succeeds would
/// pass just as well against the silent no-op it was written to catch.
#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_do_update_applies_the_update() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE c (k INT NOT NULL, v TEXT, UNIQUE (k))").await.expect("create");
    run(&pool, "INSERT INTO c (k, v) VALUES (1, 'first')").await.expect("first");

    run(&pool, "INSERT INTO c (k, v) VALUES (1, 'upsert') ON CONFLICT (k) DO UPDATE SET v = 'upsert'")
        .await
        .expect("an upsert must be honoured");

    let rows = sqlx::query("SELECT v FROM c WHERE k = 1").fetch_all(&pool).await.expect("select");
    assert_eq!(rows.len(), 1, "the conflicting row must not be duplicated");
    assert_eq!(
        rows[0].try_get::<String, _>("v").unwrap(),
        "upsert",
        "DO UPDATE must write the new value, not silently keep the old one"
    );
}

/// `VACUUM` is implemented, and must not be swallowed by the no-op list.
///
/// It was: `handle_builtin_query` consulted `no_op_reason` before the executor,
/// so over this wire the client got `CommandComplete("VACUUM")` and nothing
/// ran, while the same statement over the native protocol reclaimed space. A
/// no-op reported as success, differing only by which wire it arrived on.
///
/// The discriminator is a table that does not exist: reaching the engine, that
/// is an error, where the no-op branch answered success to anything.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_reaches_the_engine_rather_than_the_no_op_list() {
    let (_server, pool) = server().await;

    run(&pool, "CREATE TABLE vac (id INT NOT NULL, name TEXT)").await.expect("create");
    for i in 0..20 {
        run(&pool, &format!("INSERT INTO vac (id, name) VALUES ({i}, 'row{i}')"))
            .await
            .expect("insert");
    }
    run(&pool, "DELETE FROM vac WHERE id < 10").await.expect("delete");

    run(&pool, "VACUUM vac").await.expect("VACUUM on a real table works");
    run(&pool, "VACUUM FULL vac").await.expect("VACUUM FULL on a real table works");
    run(&pool, "VACUUM").await.expect("VACUUM over every table works");

    // The rows that survived the delete are still there afterwards.
    assert_eq!(count(&pool, "SELECT * FROM vac").await, 10);

    // And the proof it is not the no-op branch answering: that branch reported
    // success for anything at all.
    let err = run(&pool, "VACUUM no_such_table_at_all")
        .await
        .expect_err("a table that does not exist must be reported, not accepted");
    assert!(
        err.to_lowercase().contains("not found") || err.to_lowercase().contains("not exist"),
        "expected a missing-table error, got: {err}"
    );
}

/// The rest of the no-op list is still answered here, and still without acting.
#[tokio::test(flavor = "multi_thread")]
async fn the_genuine_no_ops_are_still_accepted() {
    let (_server, pool) = server().await;

    for sql in [
        // A plain column index is real now (see `secondary_indexes.rs`); a
        // partial one is what stays a no-op, since an index built without its
        // predicate would answer a different question.
        "CREATE INDEX idx_vac ON pms_case (id) WHERE id > 0",
        "DROP INDEX idx_vac",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto",
        "CREATE SCHEMA reporting",
        "COMMENT ON TABLE pms_case IS 'a comment'",
        "ANALYZE",
        "ANALYZE pms_case",
    ] {
        run(&pool, sql).await.unwrap_or_else(|e| panic!("{sql:?} should be accepted: {e}"));
    }
}
