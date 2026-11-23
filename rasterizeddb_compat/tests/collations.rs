//! Nondeterministic (case- and accent-insensitive) collations, as
//! PostgreSQL's ICU `und-u-ks-level2` / `ks-level1` collations and `citext`
//! behave: comparisons, uniqueness, `ON CONFLICT`, indexes and DDL follow
//! the collation, a column's value is returned as written, and what cannot
//! follow it is refused rather than answered by the bytes.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    let mut v: Vec<i32> = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.try_get_unchecked::<String, _>(0).unwrap().parse().unwrap())
        .collect();
    v.sort_unstable();
    v
}

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    match sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await {
        Ok(_) => panic!("`{sql}` should fail"),
        Err(sqlx::Error::Database(e)) => e.code().map(|c| c.to_string()).unwrap_or_default(),
        Err(e) => panic!("{sql}: {e}"),
    }
}

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(
        &pool,
        "CREATE TABLE people (id int PRIMARY KEY, name text COLLATE rdb_ci NOT NULL UNIQUE, \
         email citext, place text COLLATE \"und-u-ks-level1\", note text)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO people (id, name, email, place, note) VALUES \
         (1, 'Bob', 'Bob@Example.com', 'Café', 'X'), (2, 'ann', 'ann@x.org', 'Zürich', 'x'), \
         (3, 'Ännchen', NULL, 'cafe', NULL), (4, 'ZOE', 'zoe@X.org', NULL, 'y')",
    )
    .await;
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn comparisons_follow_the_collation() {
    let (_server, pool) = server().await;
    // The value comes back as written; the key column stays hidden.
    let row = sqlx::raw_sql("SELECT * FROM people WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(row.len(), 5);
    assert_eq!(row.try_get_unchecked::<String, _>(1).unwrap(), "Bob");
    for (clause, want) in [
        ("name = 'BOB'", vec![1]),
        ("name <> 'bob'", vec![2, 3, 4]),
        ("name IN ('ANN', 'zoe')", vec![2, 4]),
        ("name LIKE 'b%'", vec![1]),
        ("name NOT LIKE 'b%'", vec![2, 3, 4]),
        ("name BETWEEN 'A' AND 'C'", vec![1, 2]),
        ("name > 'b' AND name < 'zz'", vec![1, 4]),
        ("email = 'BOB@example.COM'", vec![1]),
        ("email IS NULL", vec![3]),
        // Case and accents.
        ("place = 'CAFE'", vec![1, 3]),
        ("place = 'zurich'", vec![2]),
        // Case only: the accent counts.
        ("name = 'annchen'", vec![]),
        ("name = 'ÄNNCHEN'", vec![3]),
        // A column without one compares by the bytes.
        ("note = 'x'", vec![2]),
        ("NOT (name = 'bob' OR note = 'y')", vec![2]),
        ("'zoe' = name", vec![4]),
    ] {
        assert_eq!(ids(&pool, &format!("SELECT id FROM people WHERE {clause}")).await, want, "{clause}");
    }
    // A parameter, as a driver binds it.
    let got: Vec<i32> = sqlx::query_scalar("SELECT id FROM people WHERE name = $1")
        .bind("ZoE")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(got, vec![4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn uniqueness_and_upserts_follow_the_collation() {
    let (_server, pool) = server().await;
    assert_eq!(code(&pool, "INSERT INTO people (id, name) VALUES (9, 'BOB')").await, "23505");
    exec(&pool, "INSERT INTO people (id, name, note) VALUES (9, 'bOb', 'z') ON CONFLICT (name) DO UPDATE SET note = 'upserted'").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE note = 'upserted'").await, vec![1]);
    exec(&pool, "INSERT INTO people (id, name) VALUES (10, 'ANN') ON CONFLICT DO NOTHING").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name = 'ann'").await, vec![2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_keep_the_key_current() {
    let (_server, pool) = server().await;
    exec(&pool, "UPDATE people SET name = 'Robert' WHERE name = 'bob'").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name = 'ROBERT'").await, vec![1]);
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name = 'bob'").await, Vec::<i32>::new());
    exec(&pool, "UPDATE people SET email = NULL WHERE id = 2").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE email IS NULL").await, vec![2, 3]);
    exec(&pool, "DELETE FROM people WHERE name = 'robert'").await;
    assert_eq!(ids(&pool, "SELECT id FROM people").await, vec![2, 3, 4]);
    // INSERT … SELECT computes the key of every row.
    exec(&pool, "INSERT INTO people (id, name) SELECT id + 100, name || '!' FROM people WHERE id < 4").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name = 'ANN!'").await, vec![102]);
}

#[tokio::test(flavor = "multi_thread")]
async fn indexes_and_ddl_follow_the_column() {
    let (_server, pool) = server().await;
    exec(&pool, "CREATE INDEX people_name ON people (name)").await;
    exec(&pool, "CREATE INDEX people_place ON people USING hash (place)").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name = 'ZoE'").await, vec![4]);
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE place = 'CAFÉ'").await, vec![1, 3]);
    exec(&pool, "ALTER TABLE people RENAME COLUMN name TO full_name").await;
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE full_name = 'ann'").await, vec![2]);
    assert_eq!(code(&pool, "INSERT INTO people (id, full_name) VALUES (20, 'Ann')").await, "23505");
    exec(&pool, "ALTER TABLE people DROP COLUMN email").await;
    let row = sqlx::raw_sql("SELECT * FROM people WHERE id = 2").fetch_one(&pool).await.unwrap();
    assert_eq!(row.len(), 4);
    exec(&pool, "ALTER TABLE people ADD COLUMN email text").await;
    exec(&pool, "UPDATE people SET email = 'A@B' WHERE id = 2").await;
    // The new column is a plain one: its old collation went with the drop.
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE email = 'a@b'").await, Vec::<i32>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn what_cannot_follow_the_collation_is_refused() {
    let (_server, pool) = server().await;
    assert_eq!(code(&pool, "SELECT id FROM people WHERE name ~ 'b'").await, "0A000");
    assert_eq!(code(&pool, "SELECT id FROM people WHERE name = place").await, "42P22");
    assert_eq!(code(&pool, "CREATE TABLE bad1 (a text COLLATE \"no such\")").await, "42704");
    assert_eq!(code(&pool, "CREATE TABLE bad2 (a int COLLATE rdb_ci)").await, "42804");
    // An uncorrelated subquery's values are compared under the column's
    // collation (PostgreSQL: the default collation yields to it).
    assert_eq!(ids(&pool, "SELECT id FROM people WHERE name IN (SELECT upper(name) FROM people WHERE id = 2)").await, vec![2]);
    // A deterministic collation is the bytes, as before.
    exec(&pool, "CREATE TABLE plain (a text COLLATE \"C\", b text COLLATE \"en_US\")").await;
    exec(&pool, "INSERT INTO plain VALUES ('A', 'B')").await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM plain WHERE a = 'a' OR b = 'b'").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 0);
}

/// A primary key with a case-insensitive collation: point reads, updates
/// and deletes by it go through the key, not a fast path over the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_primary_key_follows_the_collation() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE codes (code text COLLATE rdb_ci PRIMARY KEY, v int)").await;
    exec(&pool, "INSERT INTO codes VALUES ('ABC', 1), ('Def', 2)").await;
    assert_eq!(code(&pool, "INSERT INTO codes VALUES ('abc', 3)").await, "23505");
    let v: Vec<i32> = sqlx::query_scalar("SELECT v FROM codes WHERE code = $1").bind("aBc").fetch_all(&pool).await.unwrap();
    assert_eq!(v, vec![1]);
    exec(&pool, "UPDATE codes SET v = 10 WHERE code = 'abc'").await;
    exec(&pool, "DELETE FROM codes WHERE code = 'DEF'").await;
    let rows: Vec<(String, i32)> = sqlx::query_as("SELECT code, v FROM codes ORDER BY code").fetch_all(&pool).await.unwrap();
    assert_eq!(rows, vec![("ABC".to_string(), 10)]);
    exec(&pool, "INSERT INTO codes VALUES ('xyz', 5) ON CONFLICT (code) DO UPDATE SET v = 99").await;
    exec(&pool, "INSERT INTO codes VALUES ('XYZ', 6) ON CONFLICT (code) DO UPDATE SET v = 99").await;
    let v: Vec<i32> = sqlx::query_scalar("SELECT v FROM codes WHERE code = 'Xyz'").fetch_all(&pool).await.unwrap();
    assert_eq!(v, vec![99]);
}

/// ORDER BY sorts by the collation; what would compare by the bytes is refused.
#[tokio::test(flavor = "multi_thread")]
async fn sorting_follows_the_collation_and_grouping_is_refused() {
    let (_server, pool) = server().await;
    exec(&pool, "INSERT INTO people (id, name) VALUES (5, 'alice'), (6, 'Charlie'), (7, 'bobby')").await;
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM people ORDER BY name").fetch_all(&pool).await.unwrap();
    assert_eq!(names, ["alice", "ann", "Bob", "bobby", "Charlie", "ZOE", "Ännchen"]);
    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM people WHERE id > 1 ORDER BY name DESC LIMIT 2").fetch_all(&pool).await.unwrap();
    assert_eq!(names, ["Ännchen", "ZOE"]);
    for sql in [
        "SELECT name, count(*) FROM people GROUP BY name",
        "SELECT DISTINCT name FROM people",
        "SELECT DISTINCT * FROM people",
        "SELECT min(name) FROM people",
        "SELECT count(DISTINCT name) FROM people",
        "SELECT name FROM people UNION SELECT note FROM people",
        "SELECT p.id FROM people p JOIN people q ON p.id = q.id WHERE p.name = 'bob'",
    ] {
        assert_eq!(code(&pool, sql).await, "0A000", "{sql}");
    }
    // What does not compare it is fine.
    let n: i64 = sqlx::query_scalar("SELECT count(name) FROM people").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 7);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT name FROM people UNION ALL SELECT name FROM people) u")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 14);
    let rows: Vec<(i32, String)> =
        sqlx::query_as("SELECT p.id, q.name FROM people p JOIN people q ON p.id = q.id WHERE p.id = 2").fetch_all(&pool).await.unwrap();
    assert_eq!(rows, vec![(2, "ann".to_string())]);
}

/// The collation is the companion column and its generation record, both
/// ordinary schema: a restart reads them back as they were.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_the_collation() {
    // /tmp is a tmpfs here; a restart needs a real directory.
    let root = format!("/var/tmp/collations_{}", std::process::id());
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
        exec(&pool, "CREATE TABLE r (id int PRIMARY KEY, name citext UNIQUE)").await;
        exec(&pool, "INSERT INTO r VALUES (1, 'Mixed'), (2, 'lower')").await;
        pool.close().await;
        drop(server);
    }
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(ids(&pool, "SELECT id FROM r WHERE name = 'MIXED'").await, vec![1]);
    assert_eq!(code(&pool, "INSERT INTO r VALUES (3, 'LOWER')").await, "23505");
    exec(&pool, "INSERT INTO r VALUES (3, 'Third')").await;
    assert_eq!(ids(&pool, "SELECT id FROM r WHERE name = 'third'").await, vec![3]);
}
