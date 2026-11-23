//! `ts_rank`, against PostgreSQL 17's answers to the last digit.
//!
//! It was not implemented ("function 'ts_rank' is not supported yet"), so a
//! search ordered by relevance failed. It is PostgreSQL's `calc_rank`, its
//! single- and double-precision steps kept, so the `real` prints the same:
//! 32,000 ranks over random documents and queries agreed when this was
//! written.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

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
async fn ts_rank_answers_as_postgresql_does() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE rk (id int, body text);
         INSERT INTO rk VALUES (1, 'The quick brown fox jumps over the lazy dog'), (2, 'fox fox fox quick quick dog'),
                               (3, 'nothing to see here'),
                               (4, 'quick brown foxes and quick brown dogs run quickly past the quiet fox den');",
    )
    .execute(&pool)
    .await
    .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'fox')) FROM rk ORDER BY id", &["1|0.06079271", "2|0.082745634", "3|0", "4|0.075990885"]),
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'quick & fox')) FROM rk ORDER BY id", &["1|0.098500855", "2|0.46028033", "3|1e-20", "4|0.38445088"]),
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'quick | dog')) FROM rk ORDER BY id", &["1|0.06079271", "2|0.0683918", "3|0", "4|0.07176917"]),
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'quick <-> brown'), 1) FROM rk ORDER BY id", &["1|0.03530128", "2|3.562072e-21", "3|6.3092975e-21", "4|0.12024908"]),
        ("SELECT id, ts_rank('{0.1,0.2,0.4,1.0}', to_tsvector('english', body), to_tsquery('english', 'qui:*'), 32) FROM rk ORDER BY id", &["1|0.057308756", "2|0.0706241", "3|0", "4|0.12552123"]),
        ("SELECT id, ts_rank('{0.5,0.2,0.4,1.0}', to_tsvector('english', body), to_tsquery('english', 'fox & dog')) FROM rk ORDER BY id", &["1|0.45744994", "2|0.85415685", "3|1e-20", "4|0.69960207"]),
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'fox & !lazy'), 2|8|16) FROM rk ORDER BY id", &["1|0.00094239553", "2|2.7777777e-22", "3|1.5773244e-21", "4|3.2860926e-23"]),
        ("SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', 'quick & brown & fox & dog'), 4) FROM rk ORDER BY id", &["1|0.43826994", "2|0.6747738", "3|1e-20", "4|0.87459457"]),
        ("SELECT id FROM rk WHERE to_tsvector('english', body) @@ to_tsquery('english', 'fox') ORDER BY ts_rank(to_tsvector('english', body), to_tsquery('english', 'fox')) DESC, id", &["2", "4", "1"]),
        ("SELECT id, ts_rank(to_tsvector(body), plainto_tsquery('quick fox')), ts_rank(to_tsvector(body), websearch_to_tsquery('\"quick brown\" or dog')) FROM rk ORDER BY id", &["1|0.098500855|0.06079271", "2|0.46028033|0.045594532", "3|1e-20|0", "4|0.38445088|0.07317641"]),
        ("SELECT ts_rank(to_tsvector('fox'), 'fox')", &["0.06079271"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
    let refused: &[(&str, &str, &str)] = &[
        ("SELECT ts_rank('{1,2}', to_tsvector('fox'), 'fox')", "2202E", "array of weight is too short"),
        ("SELECT ts_rank('{0.1,0.2,0.4,1.5}', to_tsvector('fox'), 'fox')", "22023", "weight out of range"),
    ];
    for (sql, code, message) in refused {
        let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(&pool).await.expect_err(sql);
        let d = e.as_database_error().expect("a database error");
        assert_eq!((d.code().as_deref(), d.message()), (Some(*code), *message), "{sql}");
    }
}
