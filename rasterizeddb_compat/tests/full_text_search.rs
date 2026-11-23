//! Full-text search against PostgreSQL 17's answers.
//!
//! `to_tsvector(…) @@ to_tsquery(…)` in a `WHERE` was refused ("unknown
//! column or unsupported keyword"), and `text_column @@ 'word'` matched
//! nothing at all: the column's text was read as an already-made vector. The
//! stemmer was classic Porter, not the Snowball English PostgreSQL runs
//! (`day` became `dai`); `<->` was matched as `&`; a prefix `'qui':*` and a
//! stop word in a query matched nothing; and `simple` dropped English stop
//! words it has none of.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE ft (id int PRIMARY KEY, body text, notes text);
         INSERT INTO ft VALUES (1, 'The quick brown fox jumps', 'urgent delivery'),
                               (2, 'Lazy dogs sleep all day', 'fragile urgent'), (3, 'Foxes are quick', NULL);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_match_filters_rows_as_postgresql_does() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox') ORDER BY id", &["1", "3"]),
        // The one-argument forms read the default configuration, english.
        ("SELECT id FROM ft WHERE to_tsvector(body) @@ to_tsquery('quick & fox') ORDER BY id", &["1", "3"]),
        (
            "SELECT id FROM ft WHERE to_tsvector('english', COALESCE(notes, '')) @@ to_tsquery('english', 'urgent & !fragile') \
             ORDER BY id",
            &["1"],
        ),
        // A text column against a query, and against text (plainto's reading).
        ("SELECT id FROM ft WHERE body @@ to_tsquery('fox') ORDER BY id", &["1", "3"]),
        ("SELECT id FROM ft WHERE notes @@ 'urgent' ORDER BY id", &["1", "2"]),
        ("SELECT id FROM ft WHERE body @@ 'foxes' ORDER BY id", &["1", "3"]),
        ("SELECT id FROM ft WHERE notes @@ 'urgent & !fragile' ORDER BY id", &["2"]),
        ("SELECT id FROM ft WHERE to_tsquery('english', 'fox') @@ to_tsvector('english', body) ORDER BY id", &["1", "3"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'quick fox') ORDER BY id", &["1", "3"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ websearch_to_tsquery('english', 'quick -lazy') ORDER BY id", &["1", "3"]),
        (
            "SELECT id FROM ft WHERE to_tsvector('english', body || ' ' || COALESCE(notes, '')) @@ to_tsquery('urgent') ORDER BY id",
            &["1", "2"],
        ),
        ("SELECT id FROM ft WHERE to_tsvector('simple', body) @@ to_tsquery('simple', 'foxes') ORDER BY id", &["3"]),
        // A prefix, and phrases by position.
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'qui:*') ORDER BY id", &["1", "3"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'quick <-> brown') ORDER BY id", &["1"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ phraseto_tsquery('english', 'quick brown') ORDER BY id", &["1"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'quick <2> fox') ORDER BY id", &["1"]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'brown <-> quick') ORDER BY id", &[]),
        // A query of stop words only matches nothing.
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'the') ORDER BY id", &[]),
        ("SELECT id FROM ft WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox') AND id > 1 ORDER BY id", &["3"]),
        ("SELECT count(*) FROM ft WHERE NOT (to_tsvector('english', body) @@ to_tsquery('english', 'fox'))", &["1"]),
        ("SELECT id, body @@ to_tsquery('english', 'fox') FROM ft ORDER BY id", &["1|t", "2|f", "3|t"]),
        // In a select list too, a query on the left is read the other way round.
        (
            "SELECT id, to_tsquery('english', 'fox') @@ to_tsvector('english', body) FROM ft ORDER BY id",
            &["1|t", "2|f", "3|t"],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn vectors_and_queries_print_as_postgresql_prints_them() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT to_tsvector('english', body) FROM ft ORDER BY id").await,
        vec!["'brown':3 'fox':4 'jump':5 'quick':2", "'day':5 'dog':2 'lazi':1 'sleep':3", "'fox':1 'quick':3"]
    );
    assert_eq!(rows(&pool, "SELECT to_tsvector('simple', 'The Fox and the hound')").await, vec!["'and':3 'fox':2 'hound':5 'the':1,4"]);
    let cases: &[(&str, &str)] = &[
        ("SELECT to_tsquery('english', 'urgent & !fragile')", "'urgent' & !'fragil'"),
        ("SELECT to_tsquery('english', 'the & fox')", "'fox'"),
        ("SELECT to_tsquery('english', 'quick <-> the <-> fox')", "'quick' <2> 'fox'"),
        ("SELECT to_tsquery('english', 'run:* & !jumping')", "'run':* & !'jump'"),
        ("SELECT plainto_tsquery('english', 'The Quick foxes')", "'quick' & 'fox'"),
        ("SELECT phraseto_tsquery('english', 'quick the brown fox')", "'quick' <2> 'brown' <-> 'fox'"),
        ("SELECT websearch_to_tsquery('english', '\"quick brown\" or lazy -dogs')", "'quick' <-> 'brown' | 'lazi' & !'dog'"),
        ("SELECT websearch_to_tsquery('english', 'fox jumps')", "'fox' & 'jump'"),
        ("SELECT to_tsquery('simple', 'a | (b | c)')", "'a' | 'b' | 'c'"),
        ("SELECT to_tsquery('simple', '(a | b) & c')", "( 'a' | 'b' ) & 'c'"),
        ("SELECT to_tsquery('simple', 'a <-> (b <-> c)')", "'a' <-> ( 'b' <-> 'c' )"),
        ("SELECT to_tsquery('simple', '(a & b) <-> c')", "( 'a' & 'b' ) <-> 'c'"),
        ("SELECT to_tsquery('simple', '!(a <-> b)')", "!( 'a' <-> 'b' )"),
        ("SELECT to_tsquery('simple', 'a <2> b <-> c')", "'a' <2> 'b' <-> 'c'"),
        ("SELECT to_tsquery('simple', 'a & (b <-> c)')", "'a' & 'b' <-> 'c'"),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, vec![want.to_string()], "{sql}");
    }
}
