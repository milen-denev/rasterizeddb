//! Inverted (`gin`) and path (`((doc->>'k'))`) indexes answer exactly like
//! an unindexed twin.
//!
//! Two tables get the same random documents and the same writes: inserts,
//! replacements, deletes, `NULL` documents, documents that are arrays or bare
//! scalars, numbers spelled several ways (`3`, `3.0`, `3e0`). Only one has
//! indexes. Every query must return the same ids from both, through
//! `VACUUM`, `VACUUM FULL` and a restart that rebuilds the indexes from the
//! rows. An index may only ever return too much; a row missing from the
//! indexed table's answer is the bug this exists to catch.
//!
//! The indexes must also actually have been consulted, which query results
//! cannot show by design: the engine's lookup counters are read for that.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::index::{GIN_LOOKUPS, PATH_LOOKUPS};
use sqlx::{AssertSqlSafe, Row};
use std::sync::atomic::Ordering;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const STATUSES: [&str; 5] = ["new", "paid", "shipped", "delivered", "cancelled"];

fn number(r: &mut Lcg, n: u64) -> String {
    match r.below(3) {
        0 => format!("{n}"),
        1 => format!("{n}.0"),
        _ => format!("{n}e0"),
    }
}

/// Whether documents may be `NULL`: they match no JSON operator, indexed or
/// not.
const NULL_DOCS: bool = true;

/// A random document, or `NULL`.
fn document(r: &mut Lcg) -> String {
    match r.below(20) {
        0 if NULL_DOCS => return "NULL".into(),
        0 => return "'{}'".into(),
        1 => return format!("'[\"t{}\", {}]'", r.below(6), { let n = r.below(4); number(r, n) }),
        2 => return format!("'\"{}\"'", STATUSES[r.below(5) as usize]),
        _ => {}
    }
    let mut fields = vec![
        format!("\"status\":\"{}\"", STATUSES[r.below(5) as usize]),
        format!("\"cat\":{}", { let n = r.below(10); number(r, n) }),
        format!(
            "\"user\":{{\"name\":\"u{}\",\"id\":{}}}",
            r.below(30),
            { let n = r.below(30); number(r, n) }
        ),
    ];
    let tags: Vec<String> = (0..r.below(4)).map(|_| format!("\"t{}\"", r.below(6))).collect();
    fields.push(format!("\"tags\":[{}]", tags.join(",")));
    if r.below(10) == 0 {
        fields.push("\"promo\":true".into());
    }
    if r.below(8) == 0 {
        // A key whose value is not a string: `->>` renders it as text.
        fields.push(format!("\"label\":{}", r.below(3)));
    } else if r.below(4) == 0 {
        fields.push("\"label\":null".into());
    }
    format!("'{{{}}}'", fields.join(","))
}

/// A random predicate over the document.
fn predicate(r: &mut Lcg) -> String {
    let atom = |r: &mut Lcg| -> String {
        match r.below(14) {
            0 => format!("doc @> '{{\"status\":\"{}\"}}'", STATUSES[r.below(5) as usize]),
            1 => format!("doc @> '{{\"cat\":{}}}'", { let n = r.below(10); number(r, n) }),
            2 => format!("doc @> '{{\"user\":{{\"name\":\"u{}\"}}}}'", r.below(30)),
            3 => format!("doc @> '{{\"tags\":[\"t{}\"]}}'", r.below(6)),
            4 => format!("doc @> '[\"t{}\"]'", r.below(6)),
            5 => format!("doc @> '\"t{}\"'", r.below(6)),
            6 => "doc ? 'promo'".into(),
            7 => format!("doc ?| array['promo', 't{}']", r.below(6)),
            8 => "doc ?& array['status', 'promo']".into(),
            9 => format!("doc->>'status' = '{}'", STATUSES[r.below(5) as usize]),
            10 => format!("doc->'user'->>'name' = 'u{}'", r.below(30)),
            11 => format!("doc->>'status' > '{}'", STATUSES[r.below(5) as usize]),
            12 => format!("doc->>'label' = '{}'", r.below(3)),
            _ => format!("id < {}", r.below(400)),
        }
    };
    match r.below(4) {
        0 => format!("{} AND {}", atom(r), atom(r)),
        1 => format!("({} OR {})", atom(r), atom(r)),
        _ => atom(r),
    }
}

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn ids(pool: &sqlx::PgPool, table: &str, clause: &str) -> Vec<i64> {
    let sql = format!("SELECT id FROM {table} WHERE {clause}");
    let mut v: Vec<i64> = sqlx::raw_sql(AssertSqlSafe(sql.clone()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| row.try_get_unchecked::<String, _>(0).unwrap().parse().unwrap())
        .collect();
    v.sort_unstable();
    v
}

async fn compare(pool: &sqlx::PgPool, r: &mut Lcg, rounds: usize, stage: &str) {
    for _ in 0..rounds {
        let p = predicate(r);
        let indexed = ids(pool, "ix", &p).await;
        let plain = ids(pool, "tw", &p).await;
        assert_eq!(indexed, plain, "{stage}: `{p}` answered differently with indexes");
    }
}

const INDEXES: [&str; 4] = [
    "CREATE INDEX ix_ops ON ix USING gin (doc)",
    "CREATE INDEX ix_path ON ix USING gin (doc jsonb_path_ops)",
    "CREATE INDEX ix_status ON ix ((doc->>'status'))",
    "CREATE INDEX ix_uname ON ix ((doc->'user'->>'name'))",
];

#[tokio::test(flavor = "multi_thread")]
async fn indexed_documents_answer_exactly_like_their_unindexed_twin() {
    // /tmp is a tmpfs here; a restart needs a real directory.
    let root = format!("/var/tmp/json_indexes_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let mut r = Lcg(0x6a73_6f6e);
    let (gin_before, path_before) = (GIN_LOOKUPS.load(Ordering::Relaxed), PATH_LOOKUPS.load(Ordering::Relaxed));
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        for t in ["ix", "tw"] {
            exec(&pool, &format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc JSONB)")).await;
        }
        // Half the rows before the indexes exist, so the build reads them
        // back, and half after, so the write path adds them.
        let mut next_id = 0i64;
        for phase in 0..2 {
            if phase == 1 {
                for ddl in INDEXES {
                    exec(&pool, ddl).await;
                }
            }
            for _ in 0..200 {
                let d = document(&mut r);
                for t in ["ix", "tw"] {
                    exec(&pool, &format!("INSERT INTO {t} (id, doc) VALUES ({next_id}, {d})")).await;
                }
                next_id += 1;
            }
        }
        compare(&pool, &mut r, 150, "after inserts").await;

        // Replacements (relocating and, when nothing else reads, in place)
        // and deletes, the same on both.
        for _ in 0..150 {
            let id = r.below(next_id as u64);
            let stmt = if r.below(4) == 0 {
                format!("DELETE FROM {{t}} WHERE id = {id}")
            } else {
                format!("UPDATE {{t}} SET doc = {} WHERE id = {id}", document(&mut r))
            };
            for t in ["ix", "tw"] {
                exec(&pool, &stmt.replace("{t}", t)).await;
            }
        }
        compare(&pool, &mut r, 150, "after updates and deletes").await;

        exec(&pool, "VACUUM ix").await;
        exec(&pool, "VACUUM tw").await;
        compare(&pool, &mut r, 100, "after VACUUM").await;
        exec(&pool, "VACUUM FULL ix").await;
        compare(&pool, &mut r, 100, "after VACUUM FULL").await;

        // `@> '{}'` cannot be narrowed; it is answered, not refused.
        assert_eq!(ids(&pool, "ix", "doc @> '{}'").await, ids(&pool, "tw", "doc @> '{}'").await);
        pool.close().await;
        drop(server);
    }
    // Rebuilt from the rows at open.
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    compare(&pool, &mut r, 100, "after a restart").await;

    let (gin, path) = (
        GIN_LOOKUPS.load(Ordering::Relaxed) - gin_before,
        PATH_LOOKUPS.load(Ordering::Relaxed) - path_before,
    );
    assert!(gin > 100, "the gin indexes answered only {gin} lookups");
    assert!(path > 30, "the path indexes answered only {path} lookups");
}

#[tokio::test(flavor = "multi_thread")]
async fn gin_ddl_is_validated_and_listed() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE d (id BIGINT PRIMARY KEY, doc JSONB, note TEXT)").await;
    exec(&pool, "CREATE INDEX d_gin ON d USING gin (doc jsonb_path_ops)").await;
    exec(&pool, "CREATE INDEX d_status ON d ((doc->>'status'))").await;
    for (sql, why) in [
        ("CREATE INDEX d_bad ON d USING gin (note)", "one jsonb column"),
        ("CREATE INDEX d_bad ON d USING gin (doc, note)", "one jsonb column"),
        ("CREATE INDEX d_bad ON d USING gin (doc gin_trgm_ops)", "operator class"),
    ] {
        let e = sqlx::raw_sql(AssertSqlSafe(sql)).execute(&pool).await.unwrap_err();
        assert!(e.to_string().contains(why), "{sql}: {e}");
    }
    let defs: Vec<String> = sqlx::raw_sql("SELECT indexdef FROM pg_indexes WHERE tablename = 'd'")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get_unchecked::<String, _>(0).unwrap())
        .collect();
    assert!(defs.iter().any(|d| d.contains("USING gin (doc jsonb_path_ops)")), "{defs:?}");
    assert!(defs.iter().any(|d| d.contains("doc->>'status'") || d.contains("doc ->> 'status'")), "{defs:?}");
    exec(&pool, "DROP INDEX d_gin").await;
    exec(&pool, "DROP INDEX d_status").await;
}
