//! `ORDER BY <expression> … LIMIT n OFFSET m` cuts to the page before the
//! per-row work, and `OFFSET` alone pages without decoding the rows it drops.
//!
//! Both are only licensed by giving exactly the rows the uncapped query gives
//! at those positions, so every case here compares the capped query against
//! the same ordering uncapped and sliced. Every ordering ends in `id`, which is
//! unique: with a non-unique last key the two queries may legitimately order
//! ties differently, because the scan hands rows over in worker order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

const ROWS: i32 = 400;

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query(
        "CREATE TABLE ec (id INTEGER NOT NULL, a BIGINT NOT NULL, v DOUBLE PRECISION, s TEXT, n BIGINT, doc JSONB)",
    )
    .execute(&pool)
    .await
    .expect("create");

    let floats = [0.0f64, -0.0, 1.5, -1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
    let strings = ["", "a", "b", "ab", "B"];
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move |m: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % m
    };
    for id in 0..ROWS {
        // BIGINT like `n`, so `COALESCE(n, a)` has one type.
        let a = next(5) as i64;
        let v = if next(6) == 0 { None } else { Some(floats[next(7) as usize]) };
        let s = if next(6) == 0 { None } else { Some(strings[next(5) as usize].to_string()) };
        let n = if next(4) == 0 { None } else { Some(next(3) as i64) };
        let doc = match next(4) {
            0 => r#"{}"#.to_string(),
            1 => r#"{"k": ""}"#.to_string(),
            _ => format!(r#"{{"k": "2026-0{}-1{}"}}"#, 1 + next(9), next(9)),
        };
        sqlx::query("INSERT INTO ec (id, a, v, s, n, doc) VALUES ($1, $2, $3, $4, $5, $6::jsonb)")
            .bind(id)
            .bind(a)
            .bind(v)
            .bind(s)
            .bind(n)
            .bind(doc)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }
    (server, pool)
}

/// Each row as text, so the comparison covers every column the page carries
/// and not just the identity.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
        .iter()
        .map(|r| {
            (0..r.len())
                .map(|i| {
                    r.try_get::<Option<i32>, _>(i)
                        .map(|v| format!("{v:?}"))
                        .or_else(|_| r.try_get::<Option<i64>, _>(i).map(|v| format!("{v:?}")))
                        .or_else(|_| r.try_get::<Option<f64>, _>(i).map(|v| format!("{v:?}")))
                        .or_else(|_| r.try_get::<Option<String>, _>(i).map(|v| format!("{v:?}")))
                        .unwrap_or_else(|_| "<other>".to_string())
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

const ORDERINGS: &[&str] = &[
    "ORDER BY NULLIF(s, '') ASC NULLS FIRST, a DESC, id ASC",
    "ORDER BY v DESC NULLS LAST, id ASC",
    "ORDER BY v ASC NULLS FIRST, id DESC",
    "ORDER BY n ASC NULLS FIRST, v ASC, id ASC",
    "ORDER BY COALESCE(n, a) DESC, NULLIF(doc->>'k', '')::date ASC NULLS LAST, id ASC",
    "ORDER BY NULLIF(doc->>'k', '')::date DESC NULLS FIRST, s DESC NULLS LAST, id ASC",
];

const PAGES: &[(u32, u32)] = &[
    (1, 0),
    (5, 0),
    (5, 3),
    (50, 100),
    (0, 0),
    (0, 10),
    (10, 395),
    (10, 400),
    (10, 1000),
    (1000, 0),
    (399, 1),
];

#[tokio::test(flavor = "multi_thread")]
async fn a_capped_expression_sort_is_a_slice_of_the_uncapped_one() {
    let (_server, pool) = seeded().await;
    for select in [
        "SELECT id FROM ec",
        // Every column, a constant and a computed item: the per-row loop's
        // work runs on the page only, and must produce the same values.
        "SELECT id, a, v, s, n, 7 AS seven, COALESCE(n, a) AS c FROM ec",
        "SELECT * FROM ec WHERE a <> 3",
    ] {
        for ordering in ORDERINGS {
            let full = rows(&pool, &format!("{select} {ordering}")).await;
            if select.contains("WHERE") {
                assert!(full.len() < ROWS as usize && !full.is_empty());
            } else {
                assert_eq!(full.len(), ROWS as usize, "{select} {ordering}");
            }
            for (limit, offset) in PAGES {
                let sql = format!("{select} {ordering} LIMIT {limit} OFFSET {offset}");
                let got = rows(&pool, &sql).await;
                let from = (*offset as usize).min(full.len());
                let to = (from + *limit as usize).min(full.len());
                assert_eq!(got, full[from..to], "{sql}");
            }
            // OFFSET without LIMIT runs uncapped and must still page.
            let sql = format!("{select} {ordering} OFFSET 17");
            assert_eq!(rows(&pool, &sql).await, full[17.min(full.len())..], "{sql}");
        }
    }
}

/// `OFFSET` as the only post-scan step walks past the rows it drops by their
/// length headers. The page must be the same rows, whole.
#[tokio::test(flavor = "multi_thread")]
async fn offset_alone_pages_without_decoding() {
    let (_server, pool) = seeded().await;
    for select in ["SELECT id FROM ec", "SELECT id, a FROM ec", "SELECT * FROM ec"] {
        let full = rows(&pool, &format!("{select} ORDER BY id")).await;
        assert_eq!(full.len(), ROWS as usize);
        for (limit, offset) in PAGES {
            let sql = format!("{select} ORDER BY id LIMIT {limit} OFFSET {offset}");
            let got = rows(&pool, &sql).await;
            let from = (*offset as usize).min(full.len());
            let to = (from + *limit as usize).min(full.len());
            assert_eq!(got, full[from..to], "{sql}");
        }
        for offset in [0usize, 1, 399, 400, 401] {
            let sql = format!("{select} ORDER BY id OFFSET {offset}");
            assert_eq!(rows(&pool, &sql).await, full[offset.min(full.len())..], "{sql}");
        }
    }

    // Unordered: which rows is unspecified, how many is not, and they must be
    // distinct rows of the table.
    let got = rows(&pool, "SELECT id FROM ec LIMIT 30 OFFSET 20").await;
    assert_eq!(got.len(), 30);
    let mut unique = got.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 30);
    assert_eq!(rows(&pool, "SELECT id FROM ec OFFSET 390").await.len(), 10);
    assert!(rows(&pool, "SELECT id FROM ec OFFSET 5000").await.is_empty());
}
