//! Partial unique indexes: `CREATE UNIQUE INDEX … ON t (cols) WHERE pred`.
//!
//! The statement used to be accepted and ignored, so a table that declared
//! "one live account per email" stored any number of them and reported
//! success, and `ON CONFLICT … DO NOTHING` inserted the duplicate it was
//! written to skip. Uniqueness now holds among the rows that satisfy the
//! predicate, and a row that leaves the predicate (a soft delete) frees its
//! key.
//!
//! Every expected answer here was taken from PostgreSQL 17, by running the
//! same statements against it. The shapes are the ones ORMs emit: a soft
//! delete (`WHERE deleted_at IS NULL`), a tenant-scoped slug (`WHERE active`),
//! and predicates on a status column and on booleans.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) -> u64 {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) => d.code().map(|c| c.to_string()),
        _ => None,
    }
}

/// Runs a statement PostgreSQL rejects as a duplicate, and checks this engine
/// rejects it the same way.
async fn refused(pool: &sqlx::PgPool, sql: &str) {
    match sqlx::query(AssertSqlSafe(sql)).execute(pool).await {
        Ok(done) => panic!(
            "{sql}: PostgreSQL rejects this with 23505, but it succeeded ({} rows)",
            done.rows_affected()
        ),
        Err(e) => assert_eq!(sqlstate(&e).as_deref(), Some("23505"), "{sql}: {e}"),
    }
}

/// `(id, column)` pairs, ordered by id. The column is read as text so one
/// helper serves every table here.
async fn pairs(pool: &sqlx::PgPool, sql: &str) -> Vec<(i32, Option<String>)> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (r.get::<i32, _>(0), r.get::<Option<String>, _>(1)))
        .collect()
}

fn owned(expected: &[(i32, Option<&str>)]) -> Vec<(i32, Option<String>)> {
    expected.iter().map(|(i, s)| (*i, s.map(str::to_string))).collect()
}

/// `UNIQUE (email) WHERE deleted_at IS NULL`, the soft-delete shape.
#[tokio::test(flavor = "multi_thread")]
async fn soft_delete_frees_the_key() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT, deleted_at TIMESTAMPTZ)").await;
    exec(&pool, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;

    exec(&pool, "INSERT INTO users VALUES (1, 'a', 'one', NULL)").await;
    refused(&pool, "INSERT INTO users VALUES (2, 'a', 'two', NULL)").await;
    // Rows outside the predicate repeat the key freely.
    exec(&pool, "INSERT INTO users VALUES (3, 'a', 'three', '2024-01-01 00:00:00+00')").await;
    exec(&pool, "INSERT INTO users VALUES (4, 'a', 'four', '2024-01-02 00:00:00+00')").await;
    // NULLS DISTINCT, as for any unique index.
    exec(&pool, "INSERT INTO users VALUES (10, NULL, 'n1', NULL)").await;
    exec(&pool, "INSERT INTO users VALUES (11, NULL, 'n2', NULL)").await;
    // The soft delete frees the key for a new live row.
    assert_eq!(exec(&pool, "UPDATE users SET deleted_at = '2024-02-01 00:00:00+00' WHERE id = 1").await, 1);
    exec(&pool, "INSERT INTO users VALUES (5, 'a', 'five', NULL)").await;
    // An undelete brings the row back into the predicate, and collides.
    refused(&pool, "UPDATE users SET deleted_at = NULL WHERE id = 3").await;
    // So does moving a live row onto a live key.
    exec(&pool, "INSERT INTO users VALUES (6, 'b', 'six', NULL)").await;
    refused(&pool, "UPDATE users SET email = 'a' WHERE id = 6").await;

    // Without a target, DO NOTHING skips a violation of any unique index.
    assert_eq!(exec(&pool, "INSERT INTO users VALUES (7, 'a', 'seven', NULL) ON CONFLICT DO NOTHING").await, 0);
    // With the partial index named as the arbiter.
    assert_eq!(
        exec(&pool, "INSERT INTO users VALUES (8, 'a', 'eight', NULL) ON CONFLICT (email) WHERE deleted_at IS NULL DO NOTHING").await,
        0
    );
    // DO UPDATE changes the live row that conflicted, and none of the
    // soft-deleted rows sharing its email.
    assert_eq!(
        exec(
            &pool,
            "INSERT INTO users VALUES (9, 'a', 'nine', NULL) ON CONFLICT (email) WHERE deleted_at IS NULL \
             DO UPDATE SET name = EXCLUDED.name"
        )
        .await,
        1
    );
    // The parenthesised spelling of the arbiter's predicate.
    assert_eq!(
        exec(
            &pool,
            "INSERT INTO users VALUES (14, 'a', 'fourteen', NULL) ON CONFLICT (email) WHERE (deleted_at IS NULL) \
             DO UPDATE SET name = EXCLUDED.name"
        )
        .await,
        1
    );
    // No conflict: an ordinary insert.
    assert_eq!(
        exec(
            &pool,
            "INSERT INTO users VALUES (12, 'c', 'twelve', NULL) ON CONFLICT (email) WHERE deleted_at IS NULL \
             DO UPDATE SET name = EXCLUDED.name"
        )
        .await,
        1
    );

    // PostgreSQL:
    //   1|a|one|f  3|a|three|f  4|a|four|f  5|a|fourteen|t  6|b|six|t
    //   10|<null>|n1|t  11|<null>|n2|t  12|c|twelve|t
    assert_eq!(
        pairs(&pool, "SELECT id, name FROM users ORDER BY id").await,
        owned(&[
            (1, Some("one")),
            (3, Some("three")),
            (4, Some("four")),
            (5, Some("fourteen")),
            (6, Some("six")),
            (10, Some("n1")),
            (11, Some("n2")),
            (12, Some("twelve")),
        ])
    );
    assert_eq!(
        pairs(&pool, "SELECT id, email FROM users WHERE deleted_at IS NULL ORDER BY id").await,
        owned(&[(5, Some("a")), (6, Some("b")), (10, None), (11, None), (12, Some("c"))])
    );
    // The index must not answer a query whose predicate does not imply its
    // own: every row holding the email is found, live or not.
    assert_eq!(
        pairs(&pool, "SELECT id, name FROM users WHERE email = 'a' ORDER BY id").await,
        owned(&[(1, Some("one")), (3, Some("three")), (4, Some("four")), (5, Some("fourteen"))])
    );
    // Nor does the key pick out one row for an update: PostgreSQL: UPDATE 4.
    assert_eq!(exec(&pool, "UPDATE users SET name = 'x' WHERE email = 'a'").await, 4);
    assert_eq!(
        pairs(&pool, "SELECT id, name FROM users WHERE email = 'a' ORDER BY id").await,
        owned(&[(1, Some("x")), (3, Some("x")), (4, Some("x")), (5, Some("x"))])
    );
}

/// A soft delete and the insert that replaces it, in one transaction: the
/// transaction sees its own delete, so the key is free to it.
#[tokio::test(flavor = "multi_thread")]
async fn replace_inside_one_transaction() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, deleted_at TIMESTAMPTZ)").await;
    exec(&pool, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;
    exec(&pool, "INSERT INTO users VALUES (1, 'a', NULL)").await;
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("UPDATE users SET deleted_at = '2024-01-01 00:00:00+00' WHERE id = 1").await.expect("soft delete");
    tx.execute("INSERT INTO users VALUES (2, 'a', NULL)").await.expect("PostgreSQL accepts the replacement");
    tx.commit().await.expect("commit");
    // PostgreSQL: 2|a
    assert_eq!(
        pairs(&pool, "SELECT id, email FROM users WHERE deleted_at IS NULL ORDER BY id").await,
        owned(&[(2, Some("a"))])
    );
    // The same with a hard delete, and with a duplicate the transaction did
    // not free, which PostgreSQL refuses.
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DELETE FROM users WHERE id = 2").await.expect("delete");
    tx.execute("INSERT INTO users VALUES (3, 'a', NULL)").await.expect("PostgreSQL accepts the replacement");
    let e = tx.execute("INSERT INTO users VALUES (4, 'a', NULL)").await.expect_err("a second live 'a'");
    assert_eq!(sqlstate(&e).as_deref(), Some("23505"), "{e}");
    drop(tx);
    assert_eq!(
        pairs(&pool, "SELECT id, email FROM users WHERE deleted_at IS NULL ORDER BY id").await,
        owned(&[(2, Some("a"))])
    );
}

/// `UNIQUE (tenant_id, slug) WHERE active`: a composite key and a bare boolean.
#[tokio::test(flavor = "multi_thread")]
async fn tenant_slug_where_active() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE pages (id INT, tenant_id INT, slug TEXT, active BOOLEAN NOT NULL)").await;
    exec(&pool, "CREATE UNIQUE INDEX pages_slug ON pages (tenant_id, slug) WHERE active").await;
    exec(
        &pool,
        "INSERT INTO pages VALUES (1, 1, 'home', true), (2, 2, 'home', true), (3, 1, 'home', false), (4, 1, 'home', false)",
    )
    .await;
    refused(&pool, "INSERT INTO pages VALUES (5, 1, 'home', true)").await;
    refused(&pool, "UPDATE pages SET active = true WHERE id = 3").await;
    exec(&pool, "UPDATE pages SET active = false WHERE id = 1").await;
    exec(&pool, "UPDATE pages SET active = true WHERE id = 3").await;
    // PostgreSQL: 1|1|home|f  2|2|home|t  3|1|home|t  4|1|home|f
    assert_eq!(
        pairs(&pool, "SELECT id, slug FROM pages WHERE active = true ORDER BY id").await,
        owned(&[(2, Some("home")), (3, Some("home"))])
    );
    assert_eq!(
        pairs(&pool, "SELECT id, slug FROM pages ORDER BY id").await,
        owned(&[(1, Some("home")), (2, Some("home")), (3, Some("home")), (4, Some("home"))])
    );
}

/// A predicate on a status column (how an ORM stores an enum) and on booleans.
#[tokio::test(flavor = "multi_thread")]
async fn status_and_boolean_predicates() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE subs (id INT, user_id INT, status TEXT)").await;
    exec(&pool, "CREATE UNIQUE INDEX subs_one_active ON subs (user_id) WHERE status = 'active'").await;
    exec(
        &pool,
        "INSERT INTO subs VALUES (1, 1, 'active'), (2, 1, 'canceled'), (3, 1, 'canceled'), (4, 2, 'active')",
    )
    .await;
    refused(&pool, "INSERT INTO subs VALUES (5, 1, 'active')").await;
    // A NULL status is not 'active', so the row is outside the index.
    exec(&pool, "INSERT INTO subs VALUES (6, 1, NULL)").await;
    refused(&pool, "UPDATE subs SET status = 'active' WHERE id = 2").await;
    exec(&pool, "UPDATE subs SET status = 'canceled' WHERE id = 1").await;
    exec(&pool, "UPDATE subs SET status = 'active' WHERE id = 2").await;
    // PostgreSQL: 1|1|canceled  2|1|active  3|1|canceled  4|2|active  6|1|<null>
    assert_eq!(
        pairs(&pool, "SELECT id, status FROM subs ORDER BY id").await,
        owned(&[
            (1, Some("canceled")),
            (2, Some("active")),
            (3, Some("canceled")),
            (4, Some("active")),
            (6, None),
        ])
    );

    exec(&pool, "CREATE TABLE flags (id INT, owner INT, is_primary BOOLEAN, archived BOOLEAN)").await;
    exec(&pool, "CREATE UNIQUE INDEX flags_primary ON flags (owner) WHERE is_primary = true").await;
    exec(&pool, "CREATE UNIQUE INDEX flags_live ON flags (owner, id) WHERE NOT archived").await;
    exec(&pool, "INSERT INTO flags VALUES (1, 1, true, false), (2, 1, false, false), (3, 1, NULL, false)").await;
    refused(&pool, "INSERT INTO flags VALUES (4, 1, true, false)").await;
    // `NOT archived`: a NULL is not true, so these are outside the index and
    // may repeat; a live one may not.
    exec(&pool, "INSERT INTO flags VALUES (2, 1, false, true), (2, 1, false, NULL)").await;
    refused(&pool, "INSERT INTO flags VALUES (2, 1, false, false)").await;
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM flags")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 5);
}

/// Creating the index over rows that already break it fails as PostgreSQL's
/// does, and leaves nothing behind; `DROP INDEX` lifts the constraint.
#[tokio::test(flavor = "multi_thread")]
async fn create_over_violating_rows_and_drop() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE acct (id INT, email TEXT, deleted_at TIMESTAMPTZ)").await;
    exec(
        &pool,
        "INSERT INTO acct VALUES (1, 'x', NULL), (2, 'x', NULL), (3, 'y', NULL), (4, 'y', '2024-01-01 00:00:00+00')",
    )
    .await;
    // PostgreSQL: ERROR 23505 could not create unique index "acct_live"
    let e = sqlx::query("CREATE UNIQUE INDEX acct_live ON acct (email) WHERE deleted_at IS NULL")
        .execute(&pool)
        .await
        .expect_err("PostgreSQL refuses a unique index the rows already break");
    assert_eq!(sqlstate(&e).as_deref(), Some("23505"), "{e}");
    // Nothing was recorded, so the duplicate is still accepted.
    exec(&pool, "INSERT INTO acct VALUES (5, 'y', NULL)").await;

    exec(&pool, "DELETE FROM acct WHERE id IN (2, 5)").await;
    exec(&pool, "CREATE UNIQUE INDEX acct_live ON acct (email) WHERE deleted_at IS NULL").await;
    // The name is a relation now; IF NOT EXISTS makes a re-run harmless.
    exec(&pool, "CREATE UNIQUE INDEX IF NOT EXISTS acct_live ON acct (email) WHERE deleted_at IS NULL").await;
    refused(&pool, "INSERT INTO acct VALUES (6, 'x', NULL)").await;

    exec(&pool, "DROP INDEX acct_live").await;
    exec(&pool, "INSERT INTO acct VALUES (6, 'x', NULL)").await;
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM acct WHERE email = 'x'")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 2);
}

/// What a migration tool reads back, in PostgreSQL's spelling.
#[tokio::test(flavor = "multi_thread")]
async fn pg_indexes_lists_the_predicate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE users (id INT, email TEXT, deleted_at TIMESTAMPTZ)").await;
    exec(&pool, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;
    exec(&pool, "CREATE TABLE pages (id INT, tenant_id INT, slug TEXT, active BOOLEAN)").await;
    exec(&pool, "CREATE UNIQUE INDEX pages_slug ON pages (tenant_id, slug) WHERE active").await;
    let def = |name: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(AssertSqlSafe(format!("SELECT indexdef FROM pg_indexes WHERE indexname = '{name}'")))
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"))
                .get::<String, _>(0)
        }
    };
    // PostgreSQL 17, verbatim.
    assert_eq!(
        def("users_email_live").await,
        "CREATE UNIQUE INDEX users_email_live ON public.users USING btree (email) WHERE (deleted_at IS NULL)"
    );
    assert_eq!(
        def("pages_slug").await,
        "CREATE UNIQUE INDEX pages_slug ON public.pages USING btree (tenant_id, slug) WHERE active"
    );
}

/// The declaration is on disk: it outlives a restart, and an `ALTER` that
/// rewrites the table carries it across.
#[tokio::test(flavor = "multi_thread")]
async fn survives_a_restart_and_a_rewrite() {
    // /tmp is a tmpfs here; the table needs a real directory either way.
    let root = format!("/var/tmp/partial_unique_{}", std::process::id());
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
        exec(&pool, "CREATE TABLE users (id INT, email TEXT, deleted_at TIMESTAMPTZ)").await;
        exec(&pool, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;
        exec(&pool, "INSERT INTO users VALUES (1, 'a', NULL), (2, 'a', '2024-01-01 00:00:00+00')").await;
        refused(&pool, "INSERT INTO users VALUES (3, 'a', NULL)").await;
        pool.close().await;
        drop(server);
    }
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    refused(&pool, "INSERT INTO users VALUES (3, 'a', NULL)").await;
    exec(&pool, "INSERT INTO users VALUES (3, 'a', '2024-01-02 00:00:00+00')").await;

    // A rewrite drops and recreates the table; the predicate has to come back
    // with the key, or the constraint would turn into a full UNIQUE (refusing
    // the soft-deleted rows already there) or vanish.
    exec(&pool, "ALTER TABLE users ADD COLUMN note TEXT").await;
    refused(&pool, "INSERT INTO users VALUES (4, 'a', NULL, 'x')").await;
    exec(&pool, "INSERT INTO users VALUES (4, 'a', '2024-01-03 00:00:00+00', 'x')").await;
    exec(&pool, "INSERT INTO users VALUES (5, 'b', NULL, 'x')").await;
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM users")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 5);
    let def: String = sqlx::query("SELECT indexdef FROM pg_indexes WHERE indexname = 'users_email_live'")
        .fetch_one(&pool)
        .await
        .expect("listed after the rewrite")
        .get(0);
    assert!(def.ends_with("WHERE (deleted_at IS NULL)"), "{def}");
}

/// Several connections upsert the same emails into a table that already holds
/// soft-deleted rows for half of them. Exactly one live row per email may
/// result, and the deleted ones must all survive.
///
/// This is the race `unique_under_concurrency.rs` describes for plain UNIQUE:
/// without the in-flight rule a row another transaction has written but not
/// committed is invisible to the check, and concurrent `DO NOTHING` inserts of
/// one key each find nothing and each write. Each statement inserts every key
/// of a round, because one statement is one transaction: the rows it has
/// written stay uncommitted until its last one, which is the window the rule
/// closes. Single-row statements commit too quickly to show it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_upserts_keep_one_live_row_per_key() {
    const CLIENTS: usize = 6;
    const KEYS: usize = 120;
    const ROUNDS: usize = 3;

    let server = TestServer::start().await.expect("server");
    let setup = server.connect_sqlx().await.expect("connect");
    exec(&setup, "CREATE TABLE users (email TEXT NOT NULL, owner INT NOT NULL, deleted_at TIMESTAMPTZ)").await;
    exec(&setup, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;
    let deleted: Vec<String> = (0..ROUNDS)
        .flat_map(|r| (0..KEYS).step_by(2).map(move |k| format!("('r{r}e{k}', -1, '2024-01-01 00:00:00+00')")))
        .collect();
    exec(&setup, &format!("INSERT INTO users VALUES {}", deleted.join(", "))).await;

    let url = server.url();
    let mut clients = Vec::with_capacity(CLIENTS);
    for c in 0..CLIENTS {
        let url = url.clone();
        clients.push(tokio::spawn(async move {
            let mut conn = PgConnection::connect(&url).await.map_err(|e| e.to_string())?;
            for round in 0..ROUNDS {
                // Every client writes the keys in the same order. Rotated
                // orders make two statements each hold a key the other needs,
                // a deadlock that only ends when the replays run out, and the
                // test then measures retry budgets rather than uniqueness.
                let values: Vec<String> = (0..KEYS).map(|k| format!("('r{round}e{k}', {c}, NULL)")).collect();
                let sql = format!(
                    "INSERT INTO users VALUES {} ON CONFLICT (email) WHERE deleted_at IS NULL DO NOTHING",
                    values.join(", ")
                );
                let mut attempt = 0;
                loop {
                    match conn.execute(AssertSqlSafe(sql.clone())).await {
                        Ok(_) => break,
                        // A conflict the server could not wait out is the
                        // client's to retry, as with any serialization failure.
                        Err(e) if matches!(sqlstate(&e).as_deref(), Some("40001") | Some("40P01")) && attempt < 50 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                        Err(e) => return Err(format!("{sql}: {e}")),
                    }
                }
            }
            conn.close().await.map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        for client in clients {
            client.await.expect("client task").unwrap_or_else(|e| panic!("a client failed: {e}"));
        }
    })
    .await
    .expect("the clients did not finish in 120 s");

    let rows = sqlx::query("SELECT email FROM users WHERE deleted_at IS NULL")
        .fetch_all(&setup)
        .await
        .expect("live rows");
    let mut live: Vec<String> = rows.iter().map(|r| r.get::<String, _>(0)).collect();
    let total = live.len();
    live.sort();
    live.dedup();
    assert_eq!(total, live.len(), "the partial unique index did not hold: {total} live rows for {} emails", live.len());
    assert_eq!(live.len(), KEYS * ROUNDS, "some emails never got a live row");
    let gone: i64 = sqlx::query("SELECT COUNT(*) FROM users WHERE owner = -1")
        .fetch_one(&setup)
        .await
        .expect("deleted rows")
        .get(0);
    assert_eq!(gone as usize, KEYS / 2 * ROUNDS, "soft-deleted rows were lost");
}

/// Concurrent undeletes: every email has several soft-deleted copies, and the
/// clients race to bring copies back. An undelete is an `UPDATE` keyed by the
/// primary key that assigns no key column, which is the point-update path:
/// it holds the primary key's lock stripe, and has to take the email's too or
/// two undeletes of one email would each find the other still deleted. Each
/// runs in a transaction held open a moment past the write, so another
/// client's check meets it uncommitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_undeletes_restore_one_copy_per_key() {
    const CLIENTS: usize = 6;
    const KEYS: usize = 40;
    const COPIES: usize = 4;

    let server = TestServer::start().await.expect("server");
    let setup = server.connect_sqlx().await.expect("connect");
    exec(&setup, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, deleted_at TIMESTAMPTZ)").await;
    exec(&setup, "CREATE UNIQUE INDEX users_email_live ON users (email) WHERE deleted_at IS NULL").await;
    let seed: Vec<String> = (0..KEYS * COPIES)
        .map(|id| format!("({id}, 'e{}', '2024-01-01 00:00:00+00')", id % KEYS))
        .collect();
    exec(&setup, &format!("INSERT INTO users VALUES {}", seed.join(", "))).await;

    let url = server.url();
    let mut clients = Vec::with_capacity(CLIENTS);
    for c in 0..CLIENTS {
        let url = url.clone();
        clients.push(tokio::spawn(async move {
            let mut conn = PgConnection::connect(&url).await.map_err(|e| e.to_string())?;
            let mut restored = 0u64;
            // Each client takes every copy, in its own order.
            for i in 0..KEYS * COPIES {
                let id = (i * 7 + c * 13) % (KEYS * COPIES);
                let sql = format!("UPDATE users SET deleted_at = NULL WHERE id = {id} AND deleted_at IS NOT NULL");
                let mut attempt = 0;
                loop {
                    conn.execute("BEGIN").await.map_err(|e| e.to_string())?;
                    let outcome = match conn.execute(AssertSqlSafe(sql.clone())).await {
                        Ok(done) => {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            conn.execute("COMMIT").await.map(|_| done.rows_affected())
                        }
                        Err(e) => Err(e),
                    };
                    match outcome {
                        Ok(n) => {
                            restored += n;
                            break;
                        }
                        Err(e) => {
                            let _ = conn.execute("ROLLBACK").await;
                            match sqlstate(&e).as_deref() {
                                Some("23505") => break,
                                Some("40001") | Some("40P01") if attempt < 50 => {
                                    attempt += 1;
                                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                                }
                                _ => return Err(format!("{sql}: {e}")),
                            }
                        }
                    }
                }
            }
            conn.close().await.map_err(|e| e.to_string())?;
            Ok::<u64, String>(restored)
        }));
    }
    let restored: u64 = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let mut total = 0;
        for client in clients {
            total += client.await.expect("client task").unwrap_or_else(|e| panic!("a client failed: {e}"));
        }
        total
    })
    .await
    .expect("the clients did not finish in 120 s");

    let rows = sqlx::query("SELECT email FROM users WHERE deleted_at IS NULL")
        .fetch_all(&setup)
        .await
        .expect("live rows");
    let mut live: Vec<String> = rows.iter().map(|r| r.get::<String, _>(0)).collect();
    let total = live.len();
    live.sort();
    live.dedup();
    assert_eq!(total, live.len(), "the partial unique index did not hold: {total} live rows for {} emails", live.len());
    // Every email was tried, so each has exactly one copy back, and the
    // replies agree with the table.
    assert_eq!(total, KEYS);
    assert_eq!(restored as usize, KEYS, "the clients were told of {restored} undeletes");
    let all: i64 = sqlx::query("SELECT COUNT(*) FROM users")
        .fetch_one(&setup)
        .await
        .expect("count")
        .get(0);
    assert_eq!(all as usize, KEYS * COPIES);
}

/// `UPDATE … FROM` writes each joined row back by a column that identifies
/// it. A partial key does not: the soft-deleted copies share its value. Picked
/// as the identifier, it would have rewritten them all.
#[tokio::test(flavor = "multi_thread")]
async fn update_from_does_not_write_back_by_a_partial_key() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE subs (id INT, user_id INT, status TEXT)").await;
    exec(&pool, "CREATE UNIQUE INDEX subs_one_active ON subs (user_id) WHERE status = 'active'").await;
    exec(&pool, "INSERT INTO subs VALUES (1, 1, 'canceled'), (2, 1, 'active'), (3, 1, 'canceled'), (4, 2, 'active')").await;
    exec(&pool, "CREATE TABLE picks (uid INT)").await;
    exec(&pool, "INSERT INTO picks VALUES (1)").await;
    let result = sqlx::query(
        "UPDATE subs SET status = 'paused' FROM picks WHERE subs.user_id = picks.uid AND subs.status = 'active'",
    )
    .execute(&pool)
    .await;
    // PostgreSQL: UPDATE 1, and only row 2 changes. This engine writes back
    // by a unique column, and with none but the partial key it refuses the
    // statement, as it does for any table without one. Refusing is honest;
    // rewriting the canceled rows is the bug this guards.
    match &result {
        Ok(done) => assert_eq!(done.rows_affected(), 1),
        Err(e) => assert!(e.to_string().contains("needs a unique column"), "{e}"),
    }
    let expected = if result.is_ok() { "paused" } else { "active" };
    assert_eq!(
        pairs(&pool, "SELECT id, status FROM subs ORDER BY id").await,
        owned(&[(1, Some("canceled")), (2, Some(expected)), (3, Some("canceled")), (4, Some("active"))])
    );
}
