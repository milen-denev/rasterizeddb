//! Importing a real PostgreSQL database, end to end.
//!
//! Skipped unless `RDB_PG_IMPORT_TEST_DSN` names a server, so an ordinary
//! `cargo test` needs no network service. To run them:
//!
//! ```text
//! docker run -d --rm --name rdb-migrate-pg \
//!     -e POSTGRES_PASSWORD=migr8pw -e POSTGRES_USER=migruser -e POSTGRES_DB=sourcedb \
//!     -p 55444:5432 postgres:17
//!
//! RDB_PG_IMPORT_TEST_DSN='postgresql://migruser:migr8pw@127.0.0.1:55444/sourcedb' \
//!     cargo test -p rasterizeddb_core --features pg_import --test pg_import_end_to_end
//! ```
//!
//! What these cover that the unit tests cannot: that the hand-rolled frontend
//! protocol authenticates against a real server, that the catalogue queries
//! return what this crate believes they return, and — the point of the whole
//! exercise — that a value read back out of RasterizedDB is the value
//! PostgreSQL held. Every assertion below is on a value, never on the absence
//! of an error: the characteristic failure here is a statement that reports
//! success and stores something else.
//!
//! The tests work in a schema of their own and drop it first, so they cannot
//! disturb anything else in the database they are pointed at.

#![cfg(feature = "pg_import")]

use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::migrate::pg_client::{ConnectParams, PgConnection};
use rasterizeddb_core::core::migrate::plan::ImportOptions;
use rasterizeddb_core::core::migrate::run::import;
use rasterizeddb_core::core::row::row::{Row, vec_into_rows};
use rasterizeddb_core::core::rql::executor::execute;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

/// The schema name the fixture is written in terms of. Each test substitutes
/// its own, so that `cargo test`'s default parallelism does not have one test
/// dropping the schema another is reading — which it did, and which looked
/// exactly like the import failing to carry a constraint over.
const TEMPLATE_SCHEMA: &str = "rdb_import_test";

/// The connection to test against, or `None` when the suite should skip.
fn source() -> Option<ConnectParams> {
    let dsn = std::env::var("RDB_PG_IMPORT_TEST_DSN").ok()?;
    if dsn.trim().is_empty() {
        return None;
    }
    Some(ConnectParams::parse_uri(&dsn).expect("RDB_PG_IMPORT_TEST_DSN is a connection URI"))
}

macro_rules! skip_without_postgres {
    () => {
        match source() {
            Some(params) => params,
            None => {
                eprintln!("skipping: set RDB_PG_IMPORT_TEST_DSN to run this");
                return;
            }
        }
    };
}

/// Builds the source schema. Everything in one statement stream so a failure
/// names the line that failed.
const FIXTURE: &str = r#"
DROP SCHEMA IF EXISTS rdb_import_test CASCADE;
CREATE SCHEMA rdb_import_test;
SET search_path TO rdb_import_test;

CREATE TYPE rdb_import_test.mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN rdb_import_test.positive_int AS integer CHECK (VALUE > 0);

CREATE TABLE users (
    id           bigserial PRIMARY KEY,
    email        varchar(255) NOT NULL UNIQUE,
    display_name text,
    age          smallint,
    balance      numeric(12,2) NOT NULL DEFAULT 0,
    ratio        double precision,
    is_active    boolean NOT NULL DEFAULT true,
    joined_at    timestamptz,
    birthday     date,
    token        uuid,
    avatar       bytea,
    last_ip      inet,
    tags         text[],
    settings     jsonb,
    feeling      rdb_import_test.mood,
    lucky        rdb_import_test.positive_int
);

CREATE TABLE orders (
    id        integer PRIMARY KEY,
    user_id   bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code      text NOT NULL,
    placed_at timestamp NOT NULL,
    CONSTRAINT orders_code_uq UNIQUE (code)
);

CREATE TABLE line_items (
    order_id integer NOT NULL,
    line_no  integer NOT NULL,
    sku      text NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
CREATE UNIQUE INDEX line_items_sku_uq ON line_items (sku);
CREATE UNIQUE INDEX line_items_partial ON line_items (order_id) WHERE line_no > 100;

CREATE TABLE empty_table (id integer PRIMARY KEY, thing text);
CREATE VIEW active_users AS SELECT id, email FROM users WHERE is_active;

INSERT INTO users (email, display_name, age, balance, ratio, is_active, joined_at, birthday,
                   token, avatar, last_ip, tags, settings, feeling, lucky)
VALUES
 ('a@example.com', 'Ada Lovelace', 36, 1234.56, 1.5, true,
  '2026-08-21 10:11:12.123456+00', '1815-12-10',
  '550e8400-e29b-41d4-a716-446655440000', '\x48656c6c6f', '192.168.1.7',
  ARRAY['a','b'], '{"theme":"dark"}', 'happy', 7),
 ('b@example.com', 'Grace ünïcødé 😀', NULL, -0.05, 'NaN', false,
  '1970-01-01 00:00:00+00', NULL, NULL, '\x00ff', '2001:db8::1',
  ARRAY['has ''quote''','has,comma'], '{}', 'sad', 1),
 ('c@example.com', E'tab\there and a )paren, and -- a comment', 0, 0, '-Infinity', true,
  '2262-04-11 00:00:00+00', '0001-01-02', '00000000-0000-0000-0000-000000000000',
  '\x', '255.255.255.255', ARRAY[]::text[], '[1,2,3]', 'ok', 42);

INSERT INTO orders (id, user_id, code, placed_at) VALUES
 (1, 1, 'ORD-1', '2026-01-01 00:00:00'),
 (2, 1, 'ORD-2', '2026-01-02 12:34:56'),
 (3, 2, 'a),b(--x', '2026-01-03 00:00:00');

INSERT INTO line_items (order_id, line_no, sku) VALUES
 (1, 1, 'SKU-A'), (1, 2, 'SKU-B'), (2, 1, 'SKU-C');
"#;

async fn build_fixture(params: &ConnectParams, schema: &str) {
    let mut connection = PgConnection::connect(params)
        .await
        .expect("could not connect to the test PostgreSQL");
    // One simple-query message carrying several statements: PostgreSQL runs
    // them in one implicit transaction, so the fixture is all there or none.
    connection
        .simple_query(&FIXTURE.replace(TEMPLATE_SCHEMA, schema))
        .await
        .expect("could not build the fixture schema");
    connection.close().await;
}

/// Imports the fixture into a fresh directory and hands back the database.
async fn imported(
    params: &ConnectParams,
    options: ImportOptions,
) -> (tempfile::TempDir, Arc<rasterizeddb_core::core::database::Database>, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let catalog = DatabaseCatalog::open(Some(dir.path().to_str().unwrap()), "postgres")
        .await
        .expect("a fresh catalogue opens");

    let report = import(&catalog, params, "shop", &options)
        .await
        .expect("the import runs");

    let database = catalog.get("shop").expect("the imported database is there");
    (dir, database, report.notes)
}

async fn run(database: &Arc<rasterizeddb_core::core::database::Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

#[track_caller]
fn decode(response: &[u8], sql: &str) -> Vec<Row> {
    match response.first() {
        Some(2) => vec_into_rows(&response[1..]).expect("a decodable result set"),
        Some(3) => panic!("`{}` failed: {}", sql, String::from_utf8_lossy(&response[1..])),
        other => panic!("`{}` returned status {:?}, not rows", sql, other),
    }
}

async fn rows(
    database: &Arc<rasterizeddb_core::core::database::Database>,
    sql: &str,
) -> Vec<Row> {
    let response = run(database, sql).await;
    decode(&response, sql)
}

fn bytes<'a>(row: &'a Row, column: usize) -> &'a [u8] {
    row.columns[column].data.into_slice()
}

fn text(row: &Row, column: usize) -> String {
    String::from_utf8_lossy(row.columns[column].data.into_slice()).to_string()
}

fn i64_of(row: &Row, column: usize) -> i64 {
    let b = row.columns[column].data.into_slice();
    match b.len() {
        2 => i16::from_le_bytes(b.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(b.try_into().unwrap()),
        n => panic!("unexpected integer width {}", n),
    }
}

/// Whether a column came back as SQL null.
///
/// Tested on the column's *type*, not on its bytes: a null fixed-width column
/// still carries its width, filled with zeroes, so testing for emptiness would
/// call a genuine zero a null and vice versa.
/// The stored fixed-point value of a DECIMAL: the number scaled by
/// `DECIMAL_SCALE` decimal places.
fn decimal(row: &Row, column: usize) -> i128 {
    let raw = row.columns[column].data.into_slice();
    i128::from_le_bytes(raw.try_into().expect("a DECIMAL is sixteen bytes"))
}

fn float(row: &Row, column: usize) -> Option<f64> {
    let raw = row.columns[column].data.into_slice();
    match raw.len() {
        4 => Some(f64::from(f32::from_le_bytes(raw.try_into().unwrap()))),
        8 => Some(f64::from_le_bytes(raw.try_into().unwrap())),
        _ => None,
    }
}

fn is_null(row: &Row, column: usize) -> bool {
    row.columns[column].column_type == rasterizeddb_core::core::db_type::DbType::NULL
}

/// The elements of an array, out of PostgreSQL's binary array representation.
fn elements(row: &Row, column: usize) -> Vec<Option<String>> {
    let raw = row.columns[column].data.into_slice();
    if raw.len() < 20 {
        return Vec::new();
    }
    let count = i32::from_be_bytes(raw[12..16].try_into().unwrap()) as usize;
    let mut at = 20;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(raw[at..at + 4].try_into().unwrap());
        at += 4;
        if length < 0 {
            out.push(None);
            continue;
        }
        let length = length as usize;
        out.push(Some(String::from_utf8_lossy(&raw[at..at + length]).to_string()));
        at += length;
    }
    out
}

fn options(schema: &str) -> ImportOptions {
    ImportOptions {
        schema: schema.to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn every_value_arrives_as_the_value_postgres_held() {
    const SCHEMA: &str = "rdb_import_values";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;
    let (_dir, database, _notes) = imported(&params, options(SCHEMA)).await;

    let found = rows(
        &database,
        "SELECT id, email, display_name, age, balance, ratio, is_active, joined_at, birthday, \
         token, avatar, last_ip, tags, settings, feeling, lucky FROM users",
    )
    .await;
    assert_eq!(found.len(), 3, "three rows went in, so three come out");

    let mut by_id: Vec<&Row> = found.iter().collect();
    by_id.sort_by_key(|r| i64_of(r, 0));

    // --- the ordinary row ---
    let ada = by_id[0];
    assert_eq!(i64_of(ada, 0), 1);
    assert_eq!(text(ada, 1), "a@example.com");
    assert_eq!(text(ada, 2), "Ada Lovelace");
    assert_eq!(i64_of(ada, 3), 36);
    assert_eq!(text(ada, 14), "happy", "an enum keeps its label");
    assert_eq!(i64_of(ada, 15), 7, "a domain is imported as what it is built on");
    assert_eq!(
        elements(ada, 12),
        vec![Some("a".to_string()), Some("b".to_string())]
    );
    assert_eq!(text(ada, 13), "{\"theme\": \"dark\"}");
    assert_eq!(bytes(ada, 10), b"Hello", "bytea arrives as its bytes");

    // --- the row full of things that break parsers ---
    let grace = by_id[1];
    assert_eq!(
        text(grace, 2),
        "Grace ünïcødé 😀",
        "non-ASCII text is not re-encoded on the way through"
    );
    assert!(is_null(grace, 3), "a null stays a null");
    assert!(is_null(grace, 8));
    assert!(is_null(grace, 9));
    assert_eq!(
        elements(grace, 12),
        vec![
            Some("has 'quote'".to_string()),
            Some("has,comma".to_string())
        ],
        "a quote inside an array element is one quote, not two"
    );
    // Not valid UTF-8, which is the point: a bytea is bytes, and the two that
    // went in are the two that come out.
    assert_eq!(bytes(grace, 10), &[0x00u8, 0xff]);

    let carol = by_id[2];
    assert_eq!(
        text(carol, 2),
        "tab\there and a )paren, and -- a comment",
        "punctuation that is structural in SQL is data here"
    );
    assert_eq!(bytes(carol, 10), b"", "an empty bytea is empty, not null");
    assert!(!is_null(carol, 10));

    // --- the numbers ---
    // Read as the stored fixed-point value, so the check is on the number and
    // not on how it is rendered: this engine keeps one scale for every DECIMAL
    // and trims trailing zeroes, so `1234.56` is 1234.56 exactly and prints
    // without the declared two places.
    assert_eq!(decimal(ada, 4), 1_234_560_000_000, "1234.56");
    assert_eq!(decimal(grace, 4), -50_000_000, "-0.05");
    assert_eq!(decimal(carol, 4), 0);

    // The floats, including the two values that are not numbers.
    assert_eq!(float(ada, 5), Some(1.5));
    assert!(float(grace, 5).is_some_and(f64::is_nan), "NaN did not survive");
    assert_eq!(float(carol, 5), Some(f64::NEG_INFINITY));

    // A predicate the engine evaluates itself, so the value is checked through
    // the read path as well as through its bytes.
    let matched = rows(&database, "SELECT id FROM users WHERE balance = 1234.56").await;
    assert_eq!(matched.len(), 1);
    assert_eq!(i64_of(&matched[0], 0), 1);
}

#[tokio::test]
async fn every_row_of_every_table_comes_across() {
    const SCHEMA: &str = "rdb_import_rows";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;
    let (_dir, database, _notes) = imported(&params, options(SCHEMA)).await;

    for (sql, expected) in [
        ("SELECT id FROM users", 3usize),
        ("SELECT id FROM orders", 3),
        ("SELECT order_id FROM line_items", 3),
        ("SELECT id FROM empty_table", 0),
    ] {
        let found = rows(&database, sql).await;
        assert_eq!(found.len(), expected, "`{}` returned the wrong number of rows", sql);
    }

    // A view is not a table and is not imported, so asking for it is an error
    // rather than an empty answer that could be mistaken for one.
    let response = run(&database, "SELECT id FROM active_users").await;
    assert_eq!(response.first(), Some(&3), "a view should not have been imported");
}

#[tokio::test]
async fn the_keys_the_source_enforced_are_enforced_here() {
    const SCHEMA: &str = "rdb_import_keys";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;
    let (_dir, database, _notes) = imported(&params, options(SCHEMA)).await;

    // Primary key.
    let response = run(&database, "INSERT INTO users (id, email) VALUES (1, 'x@y.z')").await;
    assert_eq!(response.first(), Some(&3), "a duplicate primary key was accepted");

    // Unique constraint.
    let response = run(
        &database,
        "INSERT INTO users (id, email) VALUES (500, 'a@example.com')",
    )
    .await;
    assert_eq!(response.first(), Some(&3), "a duplicate email was accepted");

    // Composite primary key.
    let response = run(
        &database,
        "INSERT INTO line_items (order_id, line_no, sku) VALUES (1, 1, 'SKU-Z')",
    )
    .await;
    assert_eq!(response.first(), Some(&3), "a duplicate composite key was accepted");

    // A unique index with no constraint behind it in the source.
    let response = run(
        &database,
        "INSERT INTO line_items (order_id, line_no, sku) VALUES (9, 9, 'SKU-A')",
    )
    .await;
    assert_eq!(response.first(), Some(&3), "a duplicate sku was accepted");

    // Foreign key.
    let response = run(
        &database,
        "INSERT INTO orders (id, user_id, code, placed_at) VALUES (99, 4242, 'ORD-99', \
         '2026-01-01 00:00:00')",
    )
    .await;
    assert_eq!(response.first(), Some(&3), "a dangling foreign key was accepted");

    // And a row that breaks nothing still goes in, so the checks above are not
    // simply refusing everything.
    let response = run(
        &database,
        "INSERT INTO orders (id, user_id, code, placed_at) VALUES (98, 1, 'ORD-98', \
         '2026-01-01 00:00:00')",
    )
    .await;
    assert_ne!(
        response.first(),
        Some(&3),
        "a valid row was refused: {}",
        String::from_utf8_lossy(&response[1..])
    );
}

#[tokio::test]
async fn the_defaults_that_are_constants_come_across() {
    const SCHEMA: &str = "rdb_import_defaults";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;
    let (_dir, database, _notes) = imported(&params, options(SCHEMA)).await;

    let response = run(&database, "INSERT INTO users (id, email) VALUES (700, 'd@x.y')").await;
    assert_ne!(response.first(), Some(&3));

    let found = rows(&database, "SELECT balance, is_active FROM users WHERE id = 700").await;
    assert_eq!(found.len(), 1);
    let balance = rows(&database, "SELECT id FROM users WHERE id = 700 AND balance = 0").await;
    assert_eq!(balance.len(), 1, "DEFAULT 0 did not come across");
    assert_eq!(found[0].columns[1].data.into_slice(), &[1u8], "DEFAULT true did not come across");
}

#[tokio::test]
async fn a_table_list_is_honoured_and_a_typo_in_it_is_not_ignored() {
    const SCHEMA: &str = "rdb_import_lists";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;

    let (_dir, database, _notes) = imported(
        &params,
        ImportOptions {
            include: vec!["users".to_string()],
            ..options(SCHEMA)
        },
    )
    .await;
    assert_eq!(rows(&database, "SELECT id FROM users").await.len(), 3);
    assert_eq!(
        run(&database, "SELECT id FROM orders").await.first(),
        Some(&3),
        "a table that was not asked for was imported anyway"
    );

    // A named table that is not there is a typo, and importing everything else
    // in silence would leave someone believing they had it.
    let dir = tempfile::tempdir().unwrap();
    let catalog = DatabaseCatalog::open(Some(dir.path().to_str().unwrap()), "postgres")
        .await
        .unwrap();
    let error = import(
        &catalog,
        &params,
        "shop",
        &ImportOptions {
            include: vec!["users".to_string(), "nosuchtable".to_string()],
            ..options(SCHEMA)
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("nosuchtable"), "{}", error);
}

#[tokio::test]
async fn a_dry_run_creates_nothing() {
    const SCHEMA: &str = "rdb_import_dryrun";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;

    let dir = tempfile::tempdir().unwrap();
    let catalog = DatabaseCatalog::open(Some(dir.path().to_str().unwrap()), "postgres")
        .await
        .unwrap();
    let report = import(
        &catalog,
        &params,
        "shop",
        &ImportOptions { dry_run: true, ..options(SCHEMA) },
    )
    .await
    .unwrap();

    assert!(report.dry_run);
    assert_eq!(report.tables.len(), 4);
    assert!(catalog.get("shop").is_none(), "a dry run created a database");
    assert!(
        report.notes.iter().any(|n| n.contains("view")),
        "the report does not mention the view it skipped: {:?}",
        report.notes
    );
}

#[tokio::test]
async fn importing_over_an_existing_database_is_refused() {
    const SCHEMA: &str = "rdb_import_exists";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;

    let dir = tempfile::tempdir().unwrap();
    let catalog = DatabaseCatalog::open(Some(dir.path().to_str().unwrap()), "postgres")
        .await
        .unwrap();
    catalog.create("shop").await.unwrap();

    let error = import(&catalog, &params, "shop", &options(SCHEMA))
        .await
        .unwrap_err();
    assert!(error.contains("already here"), "{}", error);
}

#[tokio::test]
async fn a_wrong_password_says_so_rather_than_hanging() {
    let mut params = skip_without_postgres!();
    params.password = Some("not the password".to_string());

    let error = match PgConnection::connect(&params).await {
        Err(e) => e,
        Ok(connection) => {
            connection.close().await;
            panic!("the wrong password was accepted");
        }
    };
    assert!(
        error.to_lowercase().contains("password")
            || error.to_lowercase().contains("authentication"),
        "{}",
        error
    );
    assert!(
        !error.contains("not the password"),
        "the password reached the error message: {}",
        error
    );
}

#[tokio::test]
async fn the_report_names_what_did_not_come_across_unchanged() {
    const SCHEMA: &str = "rdb_import_report";
    let params = skip_without_postgres!();
    build_fixture(&params, SCHEMA).await;
    let (_dir, _database, notes) = imported(&params, options(SCHEMA)).await;

    // Each of these is a real difference between the two databases, and the
    // report is the only place anyone will be told about it.
    for expected in [
        "line_items_partial", // a partial unique index, not carried over
        "mood became TEXT",
        "date became TIMESTAMP",
        "sequence",
        "NOT NULL",
    ] {
        assert!(
            notes.iter().any(|n| n.contains(expected)),
            "the report never mentions {:?}: {:#?}",
            expected,
            notes
        );
    }
}
