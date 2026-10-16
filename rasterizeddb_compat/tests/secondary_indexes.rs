//! `CREATE INDEX`: B-tree and hash indexes over table columns.
//!
//! The load-bearing test is the model one: an indexed table and an
//! unindexed twin take the same random stream of inserts, updates (in place
//! and relocating), deletes, rolled-back transactions and vacuums, and every
//! read must return the same rows from both. An index that misses a
//! write path returns fewer rows from the indexed table and fails it, which
//! is the silent wrong answer an index can produce and nothing else would
//! report.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i64> {
    let mut v: Vec<i64> = sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect();
    v.sort_unstable();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn index_ddl() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE t (id BIGINT NOT NULL, n INTEGER NOT NULL, s TEXT NOT NULL, u UUID)").await;
    exec(&pool, "CREATE INDEX t_n ON t (n)").await;
    exec(&pool, "CREATE INDEX t_s_h ON t USING hash (s)").await;
    exec(&pool, "CREATE INDEX ON t (n, s)").await;
    exec(&pool, "CREATE INDEX t_u ON t (u)").await;
    // The name is taken; IF NOT EXISTS makes that fine.
    let dup = sqlx::query("CREATE INDEX t_n ON t (s)").execute(&pool).await;
    assert!(dup.is_err(), "a second index of the same name was accepted");
    exec(&pool, "CREATE INDEX IF NOT EXISTS t_n ON t (s)").await;
    // Refusals say why.
    let bad = sqlx::query("CREATE INDEX t_x ON t (nope)").execute(&pool).await.unwrap_err();
    assert!(bad.to_string().contains("does not exist"), "{bad}");
    // gin indexes documents only.
    let gin = sqlx::query("CREATE INDEX t_g ON t USING gin (s)").execute(&pool).await.unwrap_err();
    assert!(gin.to_string().contains("one jsonb column"), "{gin}");
    let brin = sqlx::query("CREATE INDEX t_b ON t USING brin (s)").execute(&pool).await.unwrap_err();
    assert!(brin.to_string().contains("btree or hash"), "{brin}");
    // Partial and expression indexes are still accepted and not built.
    exec(&pool, "CREATE INDEX t_partial ON t (n) WHERE n > 0").await;
    exec(&pool, "CREATE INDEX t_expr ON t (lower(s))").await;
    // A migration tool sees what exists: every declared index, with the
    // method chosen for it.
    let listed: Vec<(String, String)> = sqlx::query(
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 't'",
    )
    .fetch_all(&pool)
    .await
    .expect("pg_indexes")
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect();
    let def = |name: &str| listed.iter().find(|(n, _)| n == name).map(|(_, d)| d.clone());
    assert_eq!(def("t_n").as_deref(), Some("CREATE INDEX t_n ON public.t USING btree (n)"));
    assert_eq!(def("t_s_h").as_deref(), Some("CREATE INDEX t_s_h ON public.t USING hash (s)"));
    assert_eq!(def("t_n_s_idx").as_deref(), Some("CREATE INDEX t_n_s_idx ON public.t USING btree (n, s)"));
    // A UUID alone gets a hash: nothing asks a range of one.
    assert_eq!(def("t_u").as_deref(), Some("CREATE INDEX t_u ON public.t USING hash (u)"));
    assert!(def("t_partial").is_none(), "a partial index is not built and not listed");

    // Dropping, and dropping something never built (an older no-op index).
    exec(&pool, "DROP INDEX t_n").await;
    exec(&pool, "CREATE INDEX t_n ON t (n)").await;
    exec(&pool, "DROP INDEX never_existed").await;
    exec(&pool, "DROP INDEX IF EXISTS also_never").await;
}

/// The rows of `table` matching `clause`.
async fn select(pool: &sqlx::PgPool, table: &str, clause: &str) -> Vec<i64> {
    ids(pool, &format!("SELECT id FROM {table} WHERE {clause}")).await
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn word(r: &mut Rng) -> String {
    let w = ["alpha", "beta", "gamma", "delta", "al", "b", ""];
    format!("{}{}", w[r.below(w.len() as u64) as usize], r.below(4))
}

fn predicate(r: &mut Rng) -> String {
    let n = r.below(30) as i64 - 5;
    let atom = |r: &mut Rng| -> String {
        match r.below(9) {
            0 => format!("n = {}", r.below(30) as i64 - 5),
            1 => format!("n > {}", r.below(30) as i64 - 5),
            2 => format!("n <= {}", r.below(30) as i64 - 5),
            3 => format!("n BETWEEN {} AND {}", n, n + r.below(6) as i64),
            4 => format!("n IN ({}, {}, {})", r.below(30), r.below(30), r.below(30)),
            5 => format!("s = '{}'", word(r)),
            6 => format!("s >= '{}'", word(r)),
            7 => {
                let w = word(r);
                format!("s LIKE '{}%'", w.chars().next().map(String::from).unwrap_or_default())
            }
            _ => format!("id = {}", r.below(600)),
        }
    };
    match r.below(4) {
        0 => atom(r),
        1 => format!("{} AND {}", atom(r), atom(r)),
        2 => format!("({} OR {})", atom(r), atom(r)),
        _ => format!("{} AND ({} OR {})", atom(r), atom(r), atom(r)),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_indexed_table_answers_exactly_like_its_unindexed_twin() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for t in ["ix", "plain"] {
        exec(&pool, &format!("CREATE TABLE {t} (id BIGINT NOT NULL, n INTEGER NOT NULL, s TEXT NOT NULL)")).await;
    }
    // Every planner path: B-tree, hash, composite, and the implicit ones a
    // unique constraint brings (on the indexed twin only).
    exec(&pool, "CREATE INDEX ix_n ON ix (n)").await;
    exec(&pool, "CREATE INDEX ix_s ON ix USING hash (s)").await;
    exec(&pool, "CREATE INDEX ix_sn ON ix (s, n)").await;
    exec(&pool, "CREATE INDEX ix_id ON ix (id)").await;

    let mut r = Rng(0x5EED_1234);
    let mut next_id = 0u64;
    let pool = &pool;
    for step in 0..900 {
        // The same statement against both tables.
        let both = |sql: String| async move {
            for t in ["ix", "plain"] {
                exec(&pool, &sql.replace("{t}", t)).await;
            }
        };
        match r.below(12) {
            0..=3 => {
                let rows: Vec<String> = (0..1 + r.below(4))
                    .map(|_| {
                        next_id += 1;
                        format!("({}, {}, '{}')", next_id, r.below(30) as i64 - 5, word(&mut r))
                    })
                    .collect();
                both(format!("INSERT INTO {{t}} (id, n, s) VALUES {}", rows.join(", "))).await;
            }
            4 | 5 => {
                // Fixed-width only: the in-place path when nothing else runs.
                both(format!("UPDATE {{t}} SET n = {} WHERE id = {}", r.below(30) as i64 - 5, r.below(next_id + 1))).await;
            }
            6 => {
                // A string: the row relocates.
                both(format!("UPDATE {{t}} SET s = '{}' WHERE n = {}", word(&mut r), r.below(30) as i64 - 5)).await;
            }
            7 => both(format!("DELETE FROM {{t}} WHERE id = {}", r.below(next_id + 1))).await,
            8 => both(format!("DELETE FROM {{t}} WHERE n = {}", r.below(30) as i64 - 5)).await,
            9 => {
                // Written and rolled back: the versions exist and must stay
                // invisible, and vacuum must later forget them.
                let mut tx = pool.begin().await.expect("begin");
                let _ = &tx;
                for t in ["ix", "plain"] {
                    sqlx::query(AssertSqlSafe(format!(
                        "INSERT INTO {t} (id, n, s) VALUES ({}, 7, 'rolled')",
                        900_000 + step
                    )))
                    .execute(&mut *tx)
                    .await
                    .expect("insert in tx");
                    sqlx::query(AssertSqlSafe(format!("UPDATE {t} SET n = 99 WHERE n = 3")))
                        .execute(&mut *tx)
                        .await
                        .expect("update in tx");
                }
                tx.rollback().await.expect("rollback");
            }
            10 => both("VACUUM {t}".into()).await,
            _ => {
                if r.below(8) == 0 {
                    both("VACUUM FULL {t}".into()).await;
                }
            }
        }
        if step % 15 == 14 {
            for _ in 0..12 {
                let clause = predicate(&mut r);
                let (a, b) = (select(&pool, "ix", &clause).await, select(&pool, "plain", &clause).await);
                assert_eq!(a, b, "step {step}: WHERE {clause}: indexed {a:?} vs unindexed {b:?}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_is_found_by_its_index_as_soon_as_its_insert_returns() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE ev (id BIGINT NOT NULL, k TEXT NOT NULL)").await;
    exec(&pool, "CREATE INDEX ev_k ON ev (k)").await;
    let mut tasks = Vec::new();
    for w in 0..6 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..150 {
                let key = format!("w{w}-{i}");
                exec(&pool, &format!("INSERT INTO ev (id, k) VALUES ({}, '{key}')", w * 1000 + i)).await;
                let found = ids(&pool, &format!("SELECT id FROM ev WHERE k = '{key}'")).await;
                assert_eq!(found, vec![(w * 1000 + i) as i64], "{key} not found right after its insert");
                if i % 3 == 0 {
                    exec(&pool, &format!("UPDATE ev SET k = '{key}-moved' WHERE k = '{key}'")).await;
                    let moved = ids(&pool, &format!("SELECT id FROM ev WHERE k = '{key}-moved'")).await;
                    assert_eq!(moved.len(), 1, "{key} lost by its update");
                    assert!(ids(&pool, &format!("SELECT id FROM ev WHERE k = '{key}'")).await.is_empty());
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn indexes_survive_a_restart_and_an_alter() {
    // /tmp is a tmpfs here; the table needs a real directory either way.
    let root = format!("/var/tmp/secondary_indexes_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        exec(&pool, "CREATE TABLE p (id BIGINT NOT NULL, n INTEGER NOT NULL)").await;
        exec(&pool, "CREATE INDEX p_n ON p (n)").await;
        let rows: Vec<String> = (0..2000).map(|i| format!("({i}, {})", i % 50)).collect();
        exec(&pool, &format!("INSERT INTO p (id, n) VALUES {}", rows.join(", "))).await;
        assert_eq!(ids(&pool, "SELECT id FROM p WHERE n = 7").await.len(), 40);
        pool.close().await;
        drop(server);
    }
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    // Built again from the rows at open.
    for _ in 0..50 {
        if ids(&pool, "SELECT id FROM p WHERE n = 7").await.len() == 40 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(ids(&pool, "SELECT id FROM p WHERE n = 7").await.len(), 40);
    // An ALTER that rewrites the table keeps the index and its answers.
    exec(&pool, "ALTER TABLE p ADD COLUMN note TEXT").await;
    assert_eq!(ids(&pool, "SELECT id FROM p WHERE n = 7").await.len(), 40);
    let again = sqlx::query("CREATE INDEX p_n ON p (id)").execute(&pool).await;
    assert!(again.is_err(), "the index did not survive the rewrite: its name was free again");
}
