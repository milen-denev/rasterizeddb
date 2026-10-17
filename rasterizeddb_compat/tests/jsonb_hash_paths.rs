//! `#>` and `#>>` read each path element by the container it meets, as
//! PostgreSQL does: in an object it is the key with exactly that text, in an
//! array the position it spells. An element that looked like a number used
//! to be read only as a position, so the key `"1"` was unreachable and every
//! such predicate matched nothing, with an index or without.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn ids(pool: &sqlx::PgPool, table: &str, clause: &str) -> String {
    let sql = format!("SELECT id FROM {table} WHERE {clause} ORDER BY id");
    sqlx::query(AssertSqlSafe(sql.clone()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i64, _>("id").to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Every answer is PostgreSQL 17's for the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_hash_path_element_is_a_key_in_an_object_and_a_position_in_an_array() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for t in ["p", "pi"] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc JSONB)"))))
            .await
            .expect("create");
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            r#"INSERT INTO {t} VALUES (1,'{{"arr":{{"1":5}}}}'),(2,'{{"1":7}}'),(3,'{{"arr":{{"-1":6}}}}'),(4,'{{"arr":{{"01":4}}}}'),(5,'{{"arr":[3,9]}}')"#
        ))))
        .await
        .expect("seed");
    }
    // The twin with an expression index on the very path asked, which must
    // answer alike.
    pool.execute(sqlx::raw_sql("CREATE INDEX pi_arr1 ON pi ((doc #>> '{arr,1}'))")).await.expect("index");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut wrong = Vec::new();
    for (clause, want) in [
        ("doc #>> '{arr,1}' = '5'", "1"),
        ("doc #>> '{arr,1}' = '9'", "5"),
        ("doc #> '{1}' = '7'::jsonb", "2"),
        (r#"doc #>> '{"arr","1"}' = '5'"#, "1"),
        ("doc #>> '{arr,-1}' = '6'", "3"),
        ("doc #>> '{arr,-1}' = '9'", "5"),
        ("doc #>> '{arr,01}' = '4'", "4"),
        ("doc #>> '{arr,01}' = '9'", "5"),
        ("NOT (doc #>> '{arr,1}' = 'zzz')", "1,5"),
        ("doc #>> '{arr,1}' IS NULL", "2,3,4"),
        ("doc->'arr'->>'1' = '5'", "1"),
        ("doc->'arr'->>1 = '9'", "5"),
    ] {
        for t in ["p", "pi"] {
            let got = ids(&pool, t, clause).await;
            if got != want {
                wrong.push(format!("{t}: {clause}: got [{got}], PostgreSQL [{want}]"));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));

    // What the path reads back, as PostgreSQL prints it.
    let rows = sqlx::query("SELECT id, doc #>> '{arr,1}' AS v FROM p ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("select");
    let got: Vec<Option<String>> = rows.iter().map(|r| r.get::<Option<String>, _>("v")).collect();
    assert_eq!(got, vec![Some("5".into()), None, None, None, Some("9".into())]);
}
