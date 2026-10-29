//! A word or a qualifier the statement cannot place is an error, never
//! ignored.
//!
//! The single-table readers took the first word after `FROM` as the relation
//! and the next as its alias, and dropped everything up to the first clause
//! keyword they knew: `SELECT … FROM t WHER id = 1` returned every row, as did
//! `FROM t LIMT 5` and `FROM t ODER BY id`. And they read `q.col` as `col`
//! whatever `q` was, so `DELETE FROM t WHERE nosuch.id = 1` deleted t's row 1
//! and `WHERE nosuch.v > 0 OR id = 2` read t's own `v`. PostgreSQL refuses
//! all of them: 42601 for the word, 42P01 ("missing FROM-clause entry") for
//! the qualifier. Each is checked through the simple and the extended
//! protocol and inside a CTE, and the table is checked untouched.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn rows(pool: &sqlx::PgPool) -> Vec<(i32, i32)> {
    sqlx::query("SELECT id, v FROM t ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_words_and_qualifiers_are_refused() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("setup");
    let before = rows(&pool).await;

    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM t WHER id = 1", "42601"),
        ("SELECT id FROM t x WHER x.id = 1", "42601"),
        ("SELECT id FROM t LIMT 5", "42601"),
        ("SELECT id FROM t ODER BY id", "42601"),
        ("SELECT id FROM t WHERE id = 1 ODER BY id", "42601"),
        ("SELECT id FROM t WHERE id = 1 ANDD v = 2", "42601"),
        ("SELECT id FROM t GROUPBY id", "42601"),
        ("SELECT id FROM t WHERE nosuch.id = 1", "42P01"),
        ("SELECT id FROM t WHERE nosuch.v > 0 OR id = 2", "42P01"),
        ("SELECT nosuch.id FROM t", "42P01"),
        ("SELECT id FROM t ORDER BY nosuch.id", "42P01"),
        ("SELECT id FROM t AS q WHERE t.id = 1", "42P01"),
        ("WITH c AS (SELECT id FROM t WHER id = 1) SELECT count(*) FROM c", "42601"),
        ("WITH c AS (SELECT id FROM t WHERE nosuch.id = 1) SELECT count(*) FROM c", "42P01"),
        ("DELETE FROM t WHER id = 1", "42601"),
        ("DELETE FROM t WHERE nosuch.id = 1", "42P01"),
        ("DELETE FROM t AS x WHERE t.id = 1", "42P01"),
        ("UPDATE t SET v = 0 WHERE nosuch.id = 2", "42P01"),
        ("UPDATE t SET v = nosuch.v WHERE id = 2", "42P01"),
    ];
    for (sql, want) in cases {
        let simple = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await;
        let extended = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await;
        for (protocol, result) in [("simple", simple), ("extended", extended)] {
            match result {
                Ok(rows) => panic!("{protocol}: {sql} answered {} rows", rows.len()),
                Err(e) => assert_eq!(code(&e), *want, "{protocol}: {sql}: {e}"),
            }
        }
    }
    assert_eq!(rows(&pool).await, before, "a refused statement changed a row");

    // What is right still runs: the table's name, its alias, quoted and
    // schema-qualified names, a subquery's own relation.
    for (sql, want) in [
        ("SELECT t.id FROM t WHERE t.v > 10 ORDER BY t.id", vec![2, 3]),
        ("SELECT x.id FROM t AS x WHERE x.v > 10 ORDER BY x.id", vec![2, 3]),
        ("SELECT x.id FROM t x WHERE x.v > 10 ORDER BY x.id", vec![2, 3]),
        (r#"SELECT "t"."id" FROM "t" WHERE "t"."v" > 10 ORDER BY "t"."id""#, vec![2, 3]),
        ("SELECT id FROM public.t WHERE t.id = 2", vec![2]),
        ("SELECT id FROM t WHERE id IN (SELECT o.id FROM t o WHERE o.v > 10) ORDER BY id", vec![2, 3]),
    ] {
        let got: Vec<i32> = sqlx::query_scalar(AssertSqlSafe(sql.to_string()))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got, want, "{sql}");
    }
    pool.execute("UPDATE t AS x SET v = x.v + 1 WHERE x.id = 2").await.expect("aliased update");
    pool.execute("DELETE FROM t AS x WHERE x.id = 3").await.expect("aliased delete");
    assert_eq!(rows(&pool).await, vec![(1, 10), (2, 21)]);
}

/// Every qualifier PostgreSQL accepts for a single relation is accepted:
/// the relation in the default schema (`public.t.id` beside a bare `FROM t`),
/// quoted or not; in `ON CONFLICT DO UPDATE`, the INSERT's alias (Ecto:
/// `INSERT INTO t AS t0 … SET v = t0.v + 1`), the schema-qualified target
/// (Prisma's upsert) and `EXCLUDED` bare or quoted as `"excluded"` (sea-orm). Each was 42P01
/// once qualifiers were checked, where PostgreSQL stores the row. And the
/// rest of the refusals: a keyword typo after a condition on any column is
/// 42601, an unknown column under a known qualifier 42703, and the clauses
/// not implemented 0A000.
#[tokio::test(flavor = "multi_thread")]
async fn every_qualifier_postgres_accepts_is_accepted() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, x INT); INSERT INTO t VALUES (1, 10, 0), (2, 20, 0), (3, 30, 0), (4, 40, 0)")
        .await
        .expect("setup");
    // Over both protocols: the second run of each statement is extended.
    for extended in [false, true] {
        let exec = |sql: String| {
            let pool = pool.clone();
            async move {
                if extended {
                    sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.map(|_| ())
                } else {
                    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.map(|_| ())
                }
            }
        };
        pool.execute("UPDATE t SET v = id * 10").await.expect("reset");
        for sql in [
            "SELECT id FROM t WHERE public.t.id = 1",
            "SELECT public.t.id FROM t WHERE t.id = 1",
            r#"SELECT "public"."t"."id" FROM "public"."t" WHERE "public"."t"."id" = 1"#,
            "SELECT id FROM public.t WHERE public.t.id = 1 AND t.id = 1",
            "UPDATE t SET v = v + 1 WHERE public.t.id = 1",
            "DELETE FROM t WHERE public.t.id = 3",
            r#"INSERT INTO "public"."t" ("id", "v", "x") VALUES (4, 0, 0) ON CONFLICT ("id") DO UPDATE SET "v" = "excluded"."v" + 1 WHERE ("public"."t"."id" = 4 AND 1=1)"#,
            "INSERT INTO t AS t0 (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = t0.v + 1",
            "INSERT INTO t AS t0 (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = t0.v + 1 WHERE t0.v < 1000",
            "INSERT INTO t AS x (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = x.v + EXCLUDED.v WHERE x.v < 1000",
            r#"INSERT INTO t (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = t.v + "excluded"."v""#,
        ] {
            exec(sql.to_string()).await.unwrap_or_else(|e| panic!("extended={extended}: {sql}: {e}"));
        }
        // 1: 10 + 1; 2: 20 + 1 + 1 + 5 + 5; 3 deleted, then put back; 4: 0 + 1.
        assert_eq!(rows3(&pool).await, vec![(1, 11), (2, 32), (4, 1)], "extended={extended}");
        pool.execute("INSERT INTO t VALUES (3, 30, 0)").await.expect("restore");
    }

    for (sql, want) in [
        ("SELECT id FROM t WHERE nosuch.t.id = 1", "42P01"),
        ("SELECT id FROM t AS q WHERE public.t.id = 1", "42P01"),
        ("INSERT INTO t AS t0 (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = t9.v + 1", "42P01"),
        // Quoted, a name is its exact spelling: the proposed row is
        // `excluded`, which `"EXCLUDED"` is not (PostgreSQL's 42P01 too).
        (r#"INSERT INTO t (id, v, x) VALUES (2, 5, 0) ON CONFLICT (id) DO UPDATE SET v = t.v + "EXCLUDED"."v""#, "42P01"),
        // `x` is not the key: the non-key path through the predicate tokenizer.
        ("SELECT id FROM t WHERE x = 1 LIMT 1", "42601"),
        ("SELECT id FROM t WHERE x = 1 ODER BY id", "42601"),
        ("SELECT id FROM t WHERE x = 1 ANDD v = 2", "42601"),
        ("SELECT id FROM t WHERE x = 0 GROUP BY id HAVNG id > 1", "42601"),
        ("SELECT id FROM t WHERE x = 1 0x10", "42601"),
        ("SELECT id FROM t WHERE t.nosuch = 1", "42703"),
        ("SELECT id FROM t INTERSECT SELECT id FROM t", "0A000"),
        ("SELECT id FROM t EXCEPT SELECT id FROM t", "0A000"),
        ("SELECT id FROM t TABLESAMPLE SYSTEM (10)", "0A000"),
        ("SELECT id FROM t WINDOW w AS (ORDER BY id)", "0A000"),
    ] {
        let simple = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await;
        let extended = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await;
        for (protocol, result) in [("simple", simple), ("extended", extended)] {
            match result {
                Ok(rows) => panic!("{protocol}: {sql} answered {} rows", rows.len()),
                Err(e) => assert_eq!(code(&e), want, "{protocol}: {sql}: {e}"),
            }
        }
    }
}

async fn rows3(pool: &sqlx::PgPool) -> Vec<(i32, i32)> {
    sqlx::query("SELECT id, v FROM t ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// The joins, the join writes, `RETURNING` and an upsert check their
/// qualifiers too. `UPDATE t SET v = 0 FROM u WHERE nosuch.id = t.id` and
/// `DELETE FROM t USING u WHERE nosuch.tid = t.id` dropped the unknown
/// qualifier and its condition with it, and wrote every row of t; an aliased
/// target named by its table (`UPDATE t x … WHERE u.tid = t.id`) did the
/// same. A join read `t.id` under `FROM t a` as `a.id`; `RETURNING nosuch.id`
/// committed and answered; a bare `x` named an upsert's `AS "X"`. Each is
/// 42P01 in PostgreSQL and leaves the table as it was. A correlated subquery
/// in SET, which nothing here evaluates per row, is refused (0A000) where its
/// text reached the value parser (22P02) or, in a DO UPDATE, read the inner
/// table's own column and stored NULL. And what PostgreSQL accepts runs:
/// `RETURNING t.*`, `SELECT x.*`, a target in `"public"`.
#[tokio::test(flavor = "multi_thread")]
async fn joins_join_writes_returning_and_upserts_check_their_qualifiers() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 10), (2, 20), (3, 30); \
         CREATE TABLE u (id INT PRIMARY KEY, tid INT, w INT); INSERT INTO u VALUES (1, 2, 100), (2, 1, 200)",
    )
    .await
    .expect("setup");
    let before = rows(&pool).await;
    let cases: &[(&str, &str)] = &[
        ("UPDATE t SET v = 0 FROM u WHERE nosuch.id = t.id", "42P01"),
        ("UPDATE t SET v = nosuch.w FROM u WHERE u.tid = t.id", "42P01"),
        ("UPDATE t x SET v = 0 FROM u WHERE u.tid = t.id", "42P01"),
        ("UPDATE t SET v = 0 FROM u WHERE u.tid = t.id RETURNING nosuch.id", "42P01"),
        ("DELETE FROM t USING u WHERE nosuch.tid = t.id", "42P01"),
        ("DELETE FROM t x USING u WHERE u.tid = t.id", "42P01"),
        ("SELECT a.id FROM t a JOIN u b ON b.tid = t.id", "42P01"),
        ("SELECT a.id FROM t a JOIN u b ON b.tid = a.id WHERE nosuch.w > 0", "42P01"),
        ("UPDATE t SET v = 0 WHERE id = 1 RETURNING nosuch.id", "42P01"),
        ("DELETE FROM t WHERE id = 1 RETURNING nosuch.id", "42P01"),
        ("INSERT INTO t VALUES (9, 9) RETURNING nosuch.id", "42P01"),
        (r#"INSERT INTO t AS "X" VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = x.v + 1"#, "42P01"),
        (r#"SELECT "T".id FROM t"#, "42P01"),
        ("SELECT id FROM t WHERE id=1AND v=10", "42601"),
        ("UPDATE t SET v = 0, WHERE id = 1", "42601"),
        ("UPDATE t SET v = (SELECT w FROM u WHERE u.tid = t.id)", "0A000"),
        ("INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = (SELECT w FROM u WHERE u.tid = t.id)", "0A000"),
    ];
    // Every case is run and every wrong answer listed.
    let mut wrong: Vec<String> = Vec::new();
    for (sql, want) in cases {
        let simple = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await;
        let extended = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await;
        for (protocol, result) in [("simple", simple), ("extended", extended)] {
            match result {
                Ok(rows) => wrong.push(format!("{protocol}: {sql} answered {} rows", rows.len())),
                Err(e) if code(&e) != *want => wrong.push(format!("{protocol}: {sql}: {e} (want {want})")),
                Err(_) => {}
            }
        }
        let now = rows(&pool).await;
        if now != before {
            wrong.push(format!("{sql}: a refused statement changed t to {now:?}"));
            pool.execute("DELETE FROM t; INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").await.expect("reset");
        }
    }

    for (i, (sql, want)) in [
        ("INSERT INTO t VALUES (4, 40) RETURNING t.*", vec![(4, 40)]),
        ("INSERT INTO t VALUES (4, 41) ON CONFLICT DO NOTHING RETURNING t.*", vec![]),
        ("UPDATE t SET v = 41 WHERE id = 4 RETURNING t.*", vec![(4, 41)]),
        ("UPDATE t SET v = 42 WHERE public.t.id = 4 RETURNING public.t.*", vec![(4, 42)]),
        (r#"UPDATE "public"."t" SET v = 43 WHERE "public"."t".id = 4 RETURNING id, v"#, vec![(4, 43)]),
        (r#"INSERT INTO t AS "X" VALUES (4, 1) ON CONFLICT (id) DO UPDATE SET v = "X".v + 1 RETURNING "X".id, "X".v"#, vec![(4, 44)]),
        ("SELECT x.* FROM t x WHERE x.id = 4", vec![(4, 44)]),
        ("SELECT public.t.* FROM t WHERE id = 4", vec![(4, 44)]),
        (r#"DELETE FROM "public"."t" WHERE id = 4 RETURNING t.*"#, vec![(4, 44)]),
    ]
    .into_iter()
    .enumerate()
    {
        // A read runs over both protocols; a write once, the protocols
        // alternating down the list.
        let protocols: &[bool] = if sql.starts_with("SELECT") { &[false, true] } else if i % 2 == 0 { &[false] } else { &[true] };
        for &extended in protocols {
            let got: Result<Vec<(i32, i32)>, sqlx::Error> = if extended {
                sqlx::query_as(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await
            } else {
                pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
                    .await
                    .map(|rows| rows.iter().map(|r| (r.get(0), r.get(1))).collect())
            };
            match got {
                Ok(got) if got == want => {}
                got => wrong.push(format!("extended={extended}: {sql}: {got:?}, want {want:?}")),
            }
        }
    }
    if rows(&pool).await != before {
        wrong.push(format!("the accepted statements left t as {:?}", rows(&pool).await));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));

    // `EXCLUDED.v` inside a string is text, not the proposed row's value.
    pool.execute("CREATE TABLE n (id INT PRIMARY KEY, name TEXT); INSERT INTO n VALUES (1, 'a')")
        .await
        .expect("n");
    pool.execute(
        "INSERT INTO n VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET name = 'was ' || EXCLUDED.name || ' excluded.name'",
    )
    .await
    .expect("upsert");
    pool.execute("INSERT INTO n VALUES (1, 'c') ON CONFLICT (id) DO UPDATE SET name = n.name || $$ EXCLUDED.name$$")
        .await
        .expect("upsert");
    let name: String = sqlx::query_scalar("SELECT name FROM n WHERE id = 1").fetch_one(&pool).await.expect("n");
    assert_eq!(name, "was b excluded.name EXCLUDED.name");
}

/// A correlated EXISTS in an UPDATE or a DELETE runs only under a pinned
/// snapshot (REPEATABLE READ, SERIALIZABLE), and is refused (0A000) under
/// READ COMMITTED, autocommit or an explicit block.
///
/// Its subquery is read once, under a snapshot taken before the scan of the
/// rows it then writes: with a writer between the two it touched rows
/// PostgreSQL's does not (27 of 300 statements; PostgreSQL 0 of 2000). Under
/// a pinned snapshot the two are one, and Django's forms run as in
/// PostgreSQL 17: the statement's relation (`UPDATE "t" … WHERE EXISTS (… =
/// ("t"."id") …)`), a parenthesised predicate, `LIMIT 1`, a schema-qualified
/// target, and an EXISTS inside an `IN` subquery correlating with that
/// subquery's own alias. One inside an `IN` that reads the target itself is
/// refused, as is a correlation the rewrite cannot place, both as 0A000.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_exists_in_an_update_or_delete_needs_a_pinned_snapshot() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40); \
         CREATE TABLE u (id INT PRIMARY KEY, tid INT, w INT); INSERT INTO u VALUES (1, 1, 100), (2, 2, 200), (3, 4, 50)",
    )
    .await
    .expect("setup");
    let statements = [
        r#"UPDATE "t" SET "v" = 2 WHERE EXISTS(SELECT 1 AS "a" FROM "u" U0 WHERE (U0."tid" = ("t"."id") AND U0."w" < 150) LIMIT 1)"#,
        "UPDATE public.t SET v = v + 1 WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        r#"DELETE FROM "t" WHERE EXISTS(SELECT 1 AS "a" FROM "u" U0 WHERE (U0."tid" = ("t"."id") AND U0."w" > 150) LIMIT 1)"#,
        r#"DELETE FROM "t" WHERE "t"."id" IN (SELECT "t0"."id" FROM "t" AS "t0" WHERE EXISTS (SELECT 1 FROM "u" WHERE "u"."tid" = "t0"."id" AND "u"."w" < 60))"#,
    ];
    let before = rows(&pool).await;
    // READ COMMITTED: autocommit, and an explicit block.
    for begin in [None, Some("BEGIN"), Some("BEGIN ISOLATION LEVEL READ COMMITTED")] {
        for sql in statements {
            let mut conn = pool.acquire().await.expect("conn");
            if let Some(begin) = begin {
                conn.execute(begin).await.expect("begin");
            }
            let e = conn.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
            assert_eq!(code(&e), "0A000", "{begin:?} {sql}: {e}");
            if begin.is_some() {
                conn.execute("ROLLBACK").await.expect("rollback");
            }
        }
    }
    assert_eq!(rows(&pool).await, before, "a refused statement changed a row");
    // A pinned snapshot: each runs, as in PostgreSQL 17.
    for begin in ["BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN ISOLATION LEVEL SERIALIZABLE"] {
        pool.execute("DELETE FROM t; INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)").await.expect("reset");
        let mut conn = pool.acquire().await.expect("conn");
        conn.execute(begin).await.expect("begin");
        for sql in statements {
            conn.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{begin}: {sql}: {e}"));
        }
        conn.execute("COMMIT").await.expect("commit");
        drop(conn);
        // 1: 10 -> 2; 2: deleted (w 200); 3: 30 + 1 (no u row); 4: 40 -> 2, then deleted (w 50).
        assert_eq!(rows(&pool).await, vec![(1, 2), (3, 31)], "{begin}");
    }
    // Refused under any snapshot: an IN subquery reading the target, and an
    // inequality the rewrite cannot place.
    let mut conn = pool.acquire().await.expect("conn");
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    for sql in [
        "UPDATE t SET v = 0 WHERE id IN (SELECT m.id FROM u m WHERE EXISTS (SELECT 1 FROM u x WHERE x.tid = t.id))",
        "UPDATE t SET v = 0 WHERE EXISTS (SELECT 1 FROM u WHERE u.tid > t.id)",
    ] {
        let e = conn.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
        assert_eq!(code(&e), "0A000", "{sql}: {e}");
        conn.execute("ROLLBACK").await.expect("rollback");
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    }
    conn.execute("ROLLBACK").await.expect("rollback");
}

/// Correlations PostgreSQL places that the rewrite placed wrong, and two it
/// cannot, each with its proper answer: a schema-qualified outer column
/// (Prisma's `"public"."t"."id"`) and an EXISTS beside an `UPDATE … FROM`
/// source correlating with the target run (both were 42P01, "missing
/// FROM-clause entry"); an outer column under arithmetic (`u.x = t.x + 1`,
/// was 42601) and an EXISTS used as a value (`x - EXISTS (…)::int`, was
/// XX000) are refused as 0A000.
///
/// Mutations: `qualifier_of` taking the first segment as the qualifier;
/// the UPDATE target left out of an `UPDATE … FROM`'s outer names;
/// `exists_used_as_value` always false.
#[tokio::test(flavor = "multi_thread")]
async fn correlations_are_placed_or_refused_with_their_code() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(
        "CREATE TABLE t2 (id INT PRIMARY KEY, x INT, y INT, v INT); INSERT INTO t2 VALUES (1, 1, 1, 0), (2, 5, 2, 0); \
         CREATE TABLE u2 (id INT PRIMARY KEY, tid INT, x INT); INSERT INTO u2 VALUES (1, 1, 2); \
         CREATE TABLE w2 (id INT PRIMARY KEY, y INT); INSERT INTO w2 VALUES (1, 1)",
    )
    .await
    .expect("setup");
    let mut conn = pool.acquire().await.expect("conn");
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    for sql in [
        "UPDATE t2 SET v = 0 WHERE EXISTS (SELECT 1 FROM u2 WHERE u2.x = t2.x + 1)",
        "UPDATE t2 SET v = x - EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id)::int",
    ] {
        let e = conn.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
        assert_eq!(code(&e), "0A000", "{sql}: {e}");
        conn.execute("ROLLBACK").await.expect("rollback");
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    }
    for sql in [
        r#"UPDATE "public"."t2" SET v = 7 WHERE EXISTS (SELECT 1 FROM u2 WHERE u2.tid = "public"."t2"."id")"#,
        "UPDATE t2 SET v = 9 FROM u2 WHERE u2.tid = t2.id AND EXISTS (SELECT 1 FROM w2 WHERE w2.y = t2.y)",
    ] {
        conn.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    conn.execute("COMMIT").await.expect("commit");
    drop(conn);
    let got: Vec<(i32, i32)> = pool
        .fetch_all("SELECT id, v FROM t2 ORDER BY id")
        .await
        .expect("t2")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(got, vec![(1, 9), (2, 0)]);
}
