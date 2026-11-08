//! The `pg_catalog` and `information_schema` relations, as a driver reads them.
//!
//! These are what schema introspection runs on, and the extended query protocol
//! asks what a statement returns *before* running it. A catalogue view does not
//! exist until it is materialised, so the describe path could not resolve one
//! and answered `NoData` — then `Execute` sent rows anyway. sqlx surfaced that
//! as rows whose columns could not be read by name: the values were right and
//! `try_get("data_type")` was `ColumnNotFound`, so any ORM introspecting the
//! schema broke on it.
//!
//! The expected column names here are written out rather than read from the
//! engine's own list. They are fixed by PostgreSQL, so an independent spelling
//! is the oracle — a shared list would agree with itself even when both are
//! wrong.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Column, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE docs (id INT NOT NULL, body TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    (server, pool)
}

/// The column names a `SELECT *` announces, in order.
async fn described(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect("query should succeed");
    let first = rows.first().expect("expected at least one row");
    first.columns().iter().map(|c| c.name().to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_catalogue_view_announces_its_columns() {
    let (_server, pool) = server().await;

    for (sql, expected) in [
        (
            "SELECT * FROM information_schema.columns WHERE table_name = 'docs'",
            vec![
                "table_catalog", "table_schema", "table_name", "column_name",
                "ordinal_position", "is_nullable", "data_type", "character_maximum_length",
                "numeric_precision", "numeric_precision_radix", "numeric_scale",
                "datetime_precision", "udt_name",
            ],
        ),
        (
            "SELECT * FROM information_schema.tables WHERE table_name = 'docs'",
            vec!["table_catalog", "table_schema", "table_name", "table_type"],
        ),
        (
            "SELECT * FROM pg_class",
            vec!["oid", "relname", "relnamespace", "relkind", "relsize", "reltuples"],
        ),
        ("SELECT * FROM pg_namespace", vec!["oid", "nspname"]),
        (
            "SELECT * FROM pg_stat_user_tables",
            vec!["relid", "schemaname", "relname", "n_live_tup", "n_dead_tup"],
        ),
        (
            "SELECT * FROM pg_database",
            vec![
                "oid", "datname", "datdba", "encoding", "datcollate", "datctype",
                "datistemplate", "datallowconn", "datconnlimit",
            ],
        ),
    ] {
        assert_eq!(described(&pool, sql).await, expected, "for {sql:?}");
    }
}

/// The point of the description: a field can be read by name. This is the
/// assertion a schema-introspecting ORM actually makes.
#[tokio::test(flavor = "multi_thread")]
async fn introspection_columns_can_be_read_by_name() {
    let (_server, pool) = server().await;

    let rows = sqlx::query(
        "SELECT * FROM information_schema.columns WHERE table_name = 'docs' \
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("query");
    assert_eq!(rows.len(), 2, "docs has two columns");

    let names: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("column_name").expect("read by name"))
        .collect();
    assert_eq!(names, vec!["id", "body"]);

    let types: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("data_type").expect("read by name"))
        .collect();
    assert_eq!(types, vec!["integer", "text"]);

    // `id` is NOT NULL and `body` is not.
    let nullable: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("is_nullable").expect("read by name"))
        .collect();
    assert_eq!(nullable, vec!["NO", "YES"]);

    // information_schema.tables, the relation a schema dump lists first.
    let row = sqlx::query(
        "SELECT table_name, table_type FROM information_schema.tables \
         WHERE table_name = 'docs'",
    )
    .fetch_one(&pool)
    .await
    .expect("tables");
    assert_eq!(row.try_get::<String, _>("table_name").expect("by name"), "docs");
    assert_eq!(row.try_get::<String, _>("table_type").expect("by name"), "BASE TABLE");
}

/// An explicit column list is described as written, not as the whole relation.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_column_list_is_described_as_written() {
    let (_server, pool) = server().await;

    assert_eq!(
        described(&pool, "SELECT relname, relkind FROM pg_class").await,
        vec!["relname", "relkind"]
    );
    assert_eq!(
        described(
            &pool,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'docs'"
        )
        .await,
        vec!["column_name"]
    );
}

/// A select list over a catalogue view is described as a table's is: each
/// item under its alias, an expression as one column of its own type.
///
/// The view's describe arm read the fields the statement *fetches*, so
/// `n_live_tup AS live` kept its column's name, and `n_live_tup + n_dead_tup
/// AS slots` was announced as two columns ahead of a one-field DataRow, which
/// every driver refuses as a protocol error. Only the extended protocol
/// describes, so `psql` never saw it; sqlx and every ORM did.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalogue_select_list_is_described_as_its_items() {
    let (_server, pool) = server().await;
    let typed = |rows: &[sqlx::postgres::PgRow]| -> Vec<String> {
        rows[0]
            .columns()
            .iter()
            .map(|c| format!("{}:{}", c.name(), sqlx::TypeInfo::name(c.type_info())))
            .collect()
    };
    for (sql, expected) in [
        ("SELECT n_live_tup AS live FROM pg_stat_user_tables WHERE relname = 'docs'", vec!["live:INT8"]),
        (
            "SELECT relname AS name, n_live_tup + n_dead_tup AS slots FROM pg_stat_user_tables WHERE relname = 'docs'",
            vec!["name:TEXT", "slots:INT8"],
        ),
        ("SELECT reltuples >= 0 AS counted FROM pg_class WHERE relname = 'docs'", vec!["counted:BOOL"]),
        ("SELECT COUNT(*) AS n FROM pg_class c", vec!["n:INT8"]),
    ] {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql:?} should run: {e}"));
        assert_eq!(typed(&rows), expected, "for {sql:?}");
    }
    let counted: bool = sqlx::query("SELECT reltuples >= 0 AS counted FROM pg_class WHERE relname = 'docs'")
        .fetch_one(&pool)
        .await
        .expect("counted")
        .get("counted");
    assert!(counted);
}

/// "tables" and "columns" are ordinary words, so they are matched only when
/// qualified. A user relation named `tables` must still be its own table.
#[tokio::test(flavor = "multi_thread")]
async fn a_user_table_named_tables_is_not_the_catalogue_view() {
    let (_server, pool) = server().await;

    sqlx::query("CREATE TABLE tables (id INT NOT NULL, label TEXT)")
        .execute(&pool)
        .await
        .expect("a user table may be called tables");
    sqlx::query("INSERT INTO tables (id, label) VALUES (1, 'mine')")
        .execute(&pool)
        .await
        .expect("insert");

    let row = sqlx::query("SELECT id, label FROM tables").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<String, _>("label").expect("by name"), "mine");
}

/// `information_schema.columns` as PostgreSQL 17 fills it (`docker run --rm
/// postgres:17`), for every type the engine stores: the standard's type
/// name, `udt_name`, and the declared length, precision, scale and
/// fractional-second digits. Every type a short list did not name was
/// `text`, `date` and `character varying` among them, so an ORM read those
/// columns as text. `table_catalog` is the database, not the schema.
///
/// Four rows differ from PostgreSQL's, for what the engine does not record:
/// a `json` column is stored and advertised as `jsonb` and reads so; a
/// `bit(n)` or `bit varying(n)` length is not kept (`NULL`, where PostgreSQL
/// says 3 and 5); and an array's element type is not kept, so `int[]` reads
/// `_text`, as the wire's `text[]` says.
#[tokio::test(flavor = "multi_thread")]
async fn information_schema_columns_match_postgresql() {
    let server = TestServer::start().await.expect("server should start");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.simple_query("CREATE TABLE ty (a SMALLINT, b INT, c BIGINT, d REAL, e DOUBLE PRECISION, f NUMERIC, g NUMERIC(10,2), h NUMERIC(5), i TEXT, j VARCHAR, k VARCHAR(20), l CHAR(3), m BOOLEAN, n DATE, o TIME, p TIMETZ, q TIMESTAMP, r TIMESTAMPTZ, s INTERVAL, t UUID, u JSON, v JSONB, w BYTEA, x INET, y MACADDR, z MACADDR8, aa BIT(3), ab VARBIT(5), ac POINT, ad LINE, ae LSEG, af BOX, ag PATH, ah POLYGON, ai CIRCLE, aj INT4RANGE, ak INT8RANGE, al NUMRANGE, am TSRANGE, an TSTZRANGE, ao DATERANGE, ap TEXT[], aq INT[], ar SERIAL NOT NULL, ass CHAR, at TIMESTAMP(3), au BPCHAR)").await.expect("create");
    let got: Vec<String> = c
        .simple_query(
            "SELECT column_name, data_type, udt_name, character_maximum_length, numeric_precision, \
             numeric_scale, numeric_precision_radix, datetime_precision, is_nullable \
             FROM information_schema.columns WHERE table_name = 'ty' ORDER BY ordinal_position",
        )
        .await
        .expect("columns")
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect();
    let want = [
        "a|smallint|int2|NULL|16|0|2|NULL|YES",
        "b|integer|int4|NULL|32|0|2|NULL|YES",
        "c|bigint|int8|NULL|64|0|2|NULL|YES",
        "d|real|float4|NULL|24|NULL|2|NULL|YES",
        "e|double precision|float8|NULL|53|NULL|2|NULL|YES",
        "f|numeric|numeric|NULL|NULL|NULL|10|NULL|YES",
        "g|numeric|numeric|NULL|10|2|10|NULL|YES",
        "h|numeric|numeric|NULL|5|0|10|NULL|YES",
        "i|text|text|NULL|NULL|NULL|NULL|NULL|YES",
        "j|character varying|varchar|NULL|NULL|NULL|NULL|NULL|YES",
        "k|character varying|varchar|20|NULL|NULL|NULL|NULL|YES",
        "l|character|bpchar|3|NULL|NULL|NULL|NULL|YES",
        "m|boolean|bool|NULL|NULL|NULL|NULL|NULL|YES",
        "n|date|date|NULL|NULL|NULL|NULL|0|YES",
        "o|time without time zone|time|NULL|NULL|NULL|NULL|6|YES",
        "p|time with time zone|timetz|NULL|NULL|NULL|NULL|6|YES",
        "q|timestamp without time zone|timestamp|NULL|NULL|NULL|NULL|6|YES",
        "r|timestamp with time zone|timestamptz|NULL|NULL|NULL|NULL|6|YES",
        "s|interval|interval|NULL|NULL|NULL|NULL|6|YES",
        "t|uuid|uuid|NULL|NULL|NULL|NULL|NULL|YES",
        "u|json|json|NULL|NULL|NULL|NULL|NULL|YES",
        "v|jsonb|jsonb|NULL|NULL|NULL|NULL|NULL|YES",
        "w|bytea|bytea|NULL|NULL|NULL|NULL|NULL|YES",
        "x|inet|inet|NULL|NULL|NULL|NULL|NULL|YES",
        "y|macaddr|macaddr|NULL|NULL|NULL|NULL|NULL|YES",
        "z|macaddr8|macaddr8|NULL|NULL|NULL|NULL|NULL|YES",
        "aa|bit|bit|NULL|NULL|NULL|NULL|NULL|YES",
        "ab|bit varying|varbit|NULL|NULL|NULL|NULL|NULL|YES",
        "ac|point|point|NULL|NULL|NULL|NULL|NULL|YES",
        "ad|line|line|NULL|NULL|NULL|NULL|NULL|YES",
        "ae|lseg|lseg|NULL|NULL|NULL|NULL|NULL|YES",
        "af|box|box|NULL|NULL|NULL|NULL|NULL|YES",
        "ag|path|path|NULL|NULL|NULL|NULL|NULL|YES",
        "ah|polygon|polygon|NULL|NULL|NULL|NULL|NULL|YES",
        "ai|circle|circle|NULL|NULL|NULL|NULL|NULL|YES",
        "aj|int4range|int4range|NULL|NULL|NULL|NULL|NULL|YES",
        "ak|int8range|int8range|NULL|NULL|NULL|NULL|NULL|YES",
        "al|numrange|numrange|NULL|NULL|NULL|NULL|NULL|YES",
        "am|tsrange|tsrange|NULL|NULL|NULL|NULL|NULL|YES",
        "an|tstzrange|tstzrange|NULL|NULL|NULL|NULL|NULL|YES",
        "ao|daterange|daterange|NULL|NULL|NULL|NULL|NULL|YES",
        "ap|ARRAY|_text|NULL|NULL|NULL|NULL|NULL|YES",
        "aq|ARRAY|_text|NULL|NULL|NULL|NULL|NULL|YES",
        "ar|integer|int4|NULL|32|0|2|NULL|NO",
        "ass|character|bpchar|1|NULL|NULL|NULL|NULL|YES",
        "at|timestamp without time zone|timestamp|NULL|NULL|NULL|NULL|3|YES",
        "au|character|bpchar|NULL|NULL|NULL|NULL|NULL|YES",
    ];
    assert_eq!(got, want);
    let catalog = c
        .simple_query("SELECT table_catalog, table_schema FROM information_schema.tables WHERE table_name = 'ty'")
        .await
        .expect("tables");
    let row = catalog.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some((r.get(0).unwrap().to_string(), r.get(1).unwrap().to_string())),
        _ => None,
    });
    assert_eq!(row, Some(("postgres".to_string(), "public".to_string())));
}
