//! Full-text (`gin (to_tsvector(…))`) indexes answer exactly like an
//! unindexed twin.
//!
//! Two tables get the same random documents and the same writes: inserts
//! before and after the indexes exist, updates of either column, deletes,
//! `NULL`s, stop words, inflected forms and punctuation. Only one has
//! indexes: an `english` one over `title` and `body` together and a `simple`
//! one over `body`. Every `@@` query (each `to_tsquery` operator, the plain,
//! phrase and web-search parsers, `text @@ text`, a document over one column
//! or both) must return the same ids from both, through `VACUUM`, `VACUUM
//! FULL` and a restart that rebuilds the indexes from the rows. An index may
//! only ever return too much; a row missing from the indexed answer is the
//! bug this exists to catch. The lookup counter shows the indexes answered.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::index::GIN_LOOKUPS;
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
    fn pick<'a>(&mut self, from: &[&'a str]) -> &'a str {
        from[self.below(from.len() as u64) as usize]
    }
}

const WORDS: [&str; 24] = [
    "fox", "foxes", "Fox", "run", "runs", "running", "ran", "dog", "dogs", "quick", "lazy", "jumps", "jumped", "the",
    "a", "and", "sleep", "sleeping", "brown", "o'clock", "e-mail", "42", "über", "café",
];

fn text(r: &mut Lcg) -> String {
    if r.below(8) == 0 {
        return "NULL".to_string();
    }
    let n = r.below(7);
    let mut out = String::new();
    for k in 0..n {
        if k > 0 {
            out.push_str(["  ", " ", ", ", ". ", "-", " / "][r.below(6) as usize]);
        }
        out.push_str(r.pick(&WORDS));
    }
    format!("'{}'", out.replace('\'', "''"))
}

fn query(r: &mut Lcg) -> String {
    let term = |r: &mut Lcg| -> String {
        let w = ["fox", "run", "dog", "quick", "lazi", "jump", "sleep", "brown", "the", "42", "über", "foxes"];
        let word = w[r.below(w.len() as u64) as usize];
        match r.below(6) {
            0 => format!("{word}:*"),
            1 => format!("!{word}"),
            _ => word.to_string(),
        }
    };
    let mut q = term(r);
    for _ in 0..r.below(3) {
        let op = ["&", "|", "<->", "<2>"][r.below(4) as usize];
        q = format!("{q} {op} {}", term(r));
    }
    if r.below(4) == 0 {
        q = format!("({q}) & {}", term(r));
    }
    q
}

/// A random `@@` condition over the two tables' columns.
fn condition(r: &mut Lcg) -> String {
    let both = "coalesce(title, '') || ' ' || coalesce(body, '')";
    let (cfg, document) = match r.below(5) {
        0 | 1 => ("english", both),
        2 => ("english", "body"),
        3 => ("simple", "body"),
        _ => ("english", "title"),
    };
    let phrase = format!("{} {}", r.pick(&WORDS), r.pick(&WORDS)).replace('\'', "''");
    let q = match r.below(6) {
        0 | 1 => format!("to_tsquery('{cfg}', '{}')", query(r).replace('\'', "''")),
        2 => format!("plainto_tsquery('{cfg}', '{phrase}')"),
        3 => format!("phraseto_tsquery('{cfg}', '{phrase}')"),
        4 => format!("websearch_to_tsquery('{cfg}', '\"{phrase}\" or {}')", r.pick(&WORDS).replace('\'', "''")),
        _ => return format!("body @@ '{}'", r.pick(&WORDS).replace('\'', "''")),
    };
    let c = format!("to_tsvector('{cfg}', {document}) @@ {q}");
    match r.below(5) {
        0 => format!("{c} AND id % 3 <> 0"),
        1 => format!("({c}) OR id = 7"),
        _ => c,
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
        let c = condition(r);
        let indexed = ids(pool, "fi", &c).await;
        let plain = ids(pool, "fp", &c).await;
        assert_eq!(indexed, plain, "{stage}: `{c}` answered differently with indexes");
    }
}

const INDEXES: [&str; 2] = [
    "CREATE INDEX fi_both ON fi USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '')))",
    "CREATE INDEX fi_body ON fi USING gin (to_tsvector('simple', body))",
];

#[tokio::test(flavor = "multi_thread")]
async fn full_text_indexes_answer_exactly_like_their_unindexed_twin() {
    // /tmp is a tmpfs here; a restart needs a real directory.
    let root = format!("/var/tmp/full_text_index_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let mut r = Lcg(0x7473_7665);
    let before = GIN_LOOKUPS.load(Ordering::Relaxed);
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        for t in ["fi", "fp"] {
            exec(&pool, &format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, title TEXT, body TEXT)")).await;
        }
        // Half the rows before the indexes exist (the build reads them
        // back), half after (the write path adds them).
        let mut next_id = 0i64;
        for phase in 0..2 {
            if phase == 1 {
                for ddl in INDEXES {
                    exec(&pool, ddl).await;
                }
            }
            for _ in 0..150 {
                let (title, body) = (text(&mut r), text(&mut r));
                for t in ["fi", "fp"] {
                    exec(&pool, &format!("INSERT INTO {t} (id, title, body) VALUES ({next_id}, {title}, {body})")).await;
                }
                next_id += 1;
            }
        }
        compare(&pool, &mut r, 150, "after inserts").await;

        for _ in 0..120 {
            let id = r.below(next_id as u64);
            let stmt = match r.below(4) {
                0 => format!("DELETE FROM {{t}} WHERE id = {id}"),
                1 => format!("UPDATE {{t}} SET title = {} WHERE id = {id}", text(&mut r)),
                _ => format!("UPDATE {{t}} SET body = {} WHERE id = {id}", text(&mut r)),
            };
            for t in ["fi", "fp"] {
                exec(&pool, &stmt.replace("{t}", t)).await;
            }
        }
        compare(&pool, &mut r, 150, "after updates and deletes").await;

        exec(&pool, "VACUUM fi").await;
        exec(&pool, "VACUUM fp").await;
        compare(&pool, &mut r, 80, "after VACUUM").await;
        exec(&pool, "VACUUM FULL fi").await;
        compare(&pool, &mut r, 80, "after VACUUM FULL").await;

        // As pg_indexes lists it, and as pg_dump would send it back.
        let def: String = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE indexname = 'fi_both'")
            .fetch_one(&pool)
            .await
            .expect("pg_indexes");
        assert_eq!(
            def,
            "CREATE INDEX fi_both ON public.fi USING gin (to_tsvector('english'::regconfig, \
             COALESCE(title, ''::text) || ' '::text || COALESCE(body, ''::text)))"
        );
        exec(&pool, &def.replace("fi_both", "fi_again")).await;
        exec(&pool, "DROP INDEX fi_again").await;
        pool.close().await;
        drop(server);
    }
    // Rebuilt from the rows at open.
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    compare(&pool, &mut r, 100, "after a restart").await;

    let gin = GIN_LOOKUPS.load(Ordering::Relaxed) - before;
    assert!(gin > 100, "the full-text indexes answered only {gin} lookups");
}

#[tokio::test(flavor = "multi_thread")]
async fn full_text_index_ddl_refuses_what_it_cannot_key() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE ft (id INT PRIMARY KEY, title TEXT, body TEXT, n INT)").await;
    for (sql, want) in [
        ("CREATE INDEX x ON ft USING gin (to_tsvector(body))", "IMMUTABLE"),
        ("CREATE INDEX x ON ft USING gin (to_tsvector('english', title || body))", "no separator"),
        ("CREATE INDEX x ON ft USING gin (to_tsvector('english', n))", "reads text columns"),
        ("CREATE INDEX x ON ft USING gin (to_tsvector('english', missing))", "does not exist"),
    ] {
        let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(&pool).await.expect_err(sql).to_string();
        assert!(e.to_lowercase().contains(&want.to_lowercase()), "{sql}: {e}");
    }
    // The default name is PostgreSQL's for an expression index.
    exec(&pool, "CREATE INDEX ON ft USING gin (to_tsvector('english', body))").await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE indexname = 'ft_to_tsvector_idx'")
        .fetch_one(&pool)
        .await
        .expect("pg_indexes");
    assert_eq!(n, 1);
}
