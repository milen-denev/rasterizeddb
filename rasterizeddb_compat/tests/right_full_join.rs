//! `RIGHT JOIN` and `FULL OUTER JOIN`, checked against the rows PostgreSQL
//! returns for the same data.
//!
//! Every expectation below was worked out from the definition — pair what
//! matches, then NULL-pad whatever side found no partner — and the data is
//! chosen to hit the places an implementation goes quietly wrong: a NULL join
//! key (which equals nothing, not even another NULL), a key of `0` next to a
//! NULL (a NULL is stored as its type's zeroes, so comparing bytes would pair
//! them), duplicate keys on both sides, an empty relation, an `ON` term on the
//! preserved side (which must leave a rejected row in the result, padded, not
//! delete it), and NULL companion columns in the output.
//!
//! Results are compared as sorted multisets: PostgreSQL promises no order
//! without `ORDER BY`, and NULL placement in a sort is not what these test.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn exec(pool: &sqlx::PgPool, sql: &'static str) {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

async fn setup(pool: &sqlx::PgPool) {
    exec(pool, "CREATE TABLE l (id INT PRIMARY KEY, k INT, name TEXT)").await;
    exec(pool, "CREATE TABLE r (id INT PRIMARY KEY, k INT, tag TEXT, n INT)").await;
    exec(pool, "CREATE TABLE m (id INT PRIMARY KEY, rk INT)").await;
    exec(pool, "CREATE TABLE e (id INT PRIMARY KEY, k INT)").await;
    for sql in [
        "INSERT INTO l (id, k, name) VALUES (1, 1, 'a')",
        "INSERT INTO l (id, k, name) VALUES (2, 1, 'b')",
        "INSERT INTO l (id, k, name) VALUES (3, 2, 'c')",
        "INSERT INTO l (id, k, name) VALUES (4, NULL, 'd')",
        "INSERT INTO l (id, k, name) VALUES (5, 0, 'e')",
        "INSERT INTO l (id, k, name) VALUES (6, 9, 'f')",
        "INSERT INTO r (id, k, tag, n) VALUES (10, 1, 'x', 100)",
        "INSERT INTO r (id, k, tag, n) VALUES (11, 1, 'y', NULL)",
        "INSERT INTO r (id, k, tag, n) VALUES (12, 3, 'x', 300)",
        "INSERT INTO r (id, k, tag, n) VALUES (13, NULL, 'x', 400)",
        "INSERT INTO r (id, k, tag, n) VALUES (14, 0, 'z', NULL)",
        "INSERT INTO r (id, k, tag, n) VALUES (15, 2, NULL, 500)",
        "INSERT INTO m (id, rk) VALUES (100, 10)",
        "INSERT INTO m (id, rk) VALUES (101, 12)",
        "INSERT INTO m (id, rk) VALUES (102, NULL)",
        "INSERT INTO m (id, rk) VALUES (103, 15)",
    ] {
        exec(pool, sql).await;
    }
}

type Pair = (Option<i32>, Option<i32>);

async fn pairs(pool: &sqlx::PgPool, sql: &'static str) -> Vec<Pair> {
    let rows = sqlx::query(sql)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let mut out: Vec<Pair> = rows
        .iter()
        .map(|row| {
            (
                row.try_get::<Option<i32>, _>(0).expect("first column"),
                row.try_get::<Option<i32>, _>(1).expect("second column"),
            )
        })
        .collect();
    out.sort();
    out
}

fn sorted(mut v: Vec<Pair>) -> Vec<Pair> {
    v.sort();
    v
}

#[tokio::test]
async fn right_join_keeps_every_right_row_and_never_matches_a_null_key() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let want = sorted(vec![
        (Some(1), Some(10)),
        (Some(2), Some(10)),
        (Some(1), Some(11)),
        (Some(2), Some(11)),
        (None, Some(12)),
        // NULL key: equals nothing.
        (None, Some(13)),
        // Key 0 matches the real 0, and not l's NULL.
        (Some(5), Some(14)),
        (Some(3), Some(15)),
    ]);
    for sql in [
        "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k",
        "SELECT l.id, r.id FROM l RIGHT OUTER JOIN r ON l.k = r.k",
    ] {
        assert_eq!(pairs(&pool, sql).await, want, "{}", sql);
    }
}

#[tokio::test]
async fn full_join_keeps_both_sides() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let want = sorted(vec![
        (Some(1), Some(10)),
        (Some(2), Some(10)),
        (Some(1), Some(11)),
        (Some(2), Some(11)),
        (None, Some(12)),
        (None, Some(13)),
        (Some(5), Some(14)),
        (Some(3), Some(15)),
        (Some(4), None),
        (Some(6), None),
    ]);
    for sql in [
        "SELECT l.id, r.id FROM l FULL JOIN r ON r.k = l.k",
        "SELECT l.id, r.id FROM l FULL OUTER JOIN r ON r.k = l.k",
    ] {
        assert_eq!(pairs(&pool, sql).await, want, "{}", sql);
    }
}

/// The inner join must agree: NULL never pairs with NULL, nor with a stored 0.
#[tokio::test]
async fn an_inner_join_never_pairs_a_null_key() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let want = sorted(vec![
        (Some(1), Some(10)),
        (Some(2), Some(10)),
        (Some(1), Some(11)),
        (Some(2), Some(11)),
        (Some(5), Some(14)),
        (Some(3), Some(15)),
    ]);
    assert_eq!(
        pairs(&pool, "SELECT l.id, r.id FROM l JOIN r ON r.k = l.k").await,
        want
    );
    let left = pairs(&pool, "SELECT l.id, r.id FROM l LEFT JOIN r ON r.k = l.k").await;
    assert!(left.contains(&(Some(4), None)), "l.4 has a NULL key: {:?}", left);
    assert_eq!(left.len(), 8, "{:?}", left);
}

/// An `ON` term on the preserved side decides what *matches*; it does not
/// filter that side. A rejected right row still comes back, NULL-padded.
#[tokio::test]
async fn an_on_term_on_the_preserved_side_pads_instead_of_dropping() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let right = sorted(vec![
        (Some(1), Some(10)),
        (Some(2), Some(10)),
        // tag 'y': rejected by the ON term, kept unmatched.
        (None, Some(11)),
        (None, Some(12)),
        (None, Some(13)),
        (None, Some(14)),
        // tag NULL: `NULL = 'x'` is unknown, so unmatched.
        (None, Some(15)),
    ]);
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k AND r.tag = 'x'"
        )
        .await,
        right
    );
    let mut full = right.clone();
    full.extend([(Some(3), None), (Some(4), None), (Some(5), None), (Some(6), None)]);
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l FULL JOIN r ON r.k = l.k AND r.tag = 'x'"
        )
        .await,
        sorted(full)
    );
}

/// A `WHERE` term on the preserved side filters after the join.
#[tokio::test]
async fn where_on_the_preserved_side_and_the_anti_join() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k WHERE r.n > 150"
        )
        .await,
        sorted(vec![(None, Some(12)), (None, Some(13)), (Some(3), Some(15))])
    );
    // `l.id` is a primary key, so IS NULL means "no partner".
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k WHERE l.id IS NULL"
        )
        .await,
        sorted(vec![(None, Some(12)), (None, Some(13))])
    );
    // Filtering the null-supplying side is refused, not pushed into its scan
    // (which would change which rows count as unmatched).
    let refused = sqlx::query("SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k WHERE l.name = 'a'")
        .fetch_all(&pool)
        .await;
    assert!(refused.is_err(), "{:?}", refused.map(|r| r.len()));
}

#[tokio::test]
async fn an_empty_side() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let all_r: Vec<Pair> = [10, 11, 12, 13, 14, 15]
        .into_iter()
        .map(|id| (None, Some(id)))
        .collect();
    assert_eq!(
        pairs(&pool, "SELECT e.id, r.id FROM e RIGHT JOIN r ON r.k = e.k").await,
        all_r
    );
    assert_eq!(
        pairs(&pool, "SELECT e.id, r.id FROM e FULL JOIN r ON r.k = e.k").await,
        all_r
    );
    assert_eq!(
        pairs(&pool, "SELECT r.id, e.id FROM r RIGHT JOIN e ON e.k = r.k").await,
        Vec::<Pair>::new()
    );
    let full: Vec<Pair> = [10, 11, 12, 13, 14, 15]
        .into_iter()
        .map(|id| (Some(id), None))
        .collect();
    assert_eq!(
        pairs(&pool, "SELECT r.id, e.id FROM r FULL JOIN e ON e.k = r.k").await,
        full
    );
}

/// Unmatched sides arrive as NULL, and a matched row's own NULLs stay NULL.
#[tokio::test]
async fn null_companion_columns_survive_the_outer_join() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let rows = sqlx::query(
        "SELECT r.id AS rid, l.name AS name, r.tag AS tag, r.n AS n \
         FROM l RIGHT JOIN r ON r.k = l.k",
    )
    .fetch_all(&pool)
    .await
    .expect("right join");
    let mut got: Vec<(i32, Option<String>, Option<String>, Option<i32>)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<i32, _>("rid"),
                row.get::<Option<String>, _>("name"),
                row.get::<Option<String>, _>("tag"),
                row.get::<Option<i32>, _>("n"),
            )
        })
        .collect();
    got.sort();
    let s = |v: &str| Some(v.to_string());
    let mut want = vec![
        (10, s("a"), s("x"), Some(100)),
        (10, s("b"), s("x"), Some(100)),
        (11, s("a"), s("y"), None),
        (11, s("b"), s("y"), None),
        (12, None, s("x"), Some(300)),
        (13, None, s("x"), Some(400)),
        (14, s("e"), s("z"), None),
        (15, s("c"), None, Some(500)),
    ];
    want.sort();
    assert_eq!(got, want);
}

/// `(l JOIN r) RIGHT JOIN m`: m's rows survive, and a row of m whose partner
/// was dropped by the inner join is unmatched, not lost.
#[tokio::test]
async fn a_three_way_chain() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let rows = sqlx::query(
        "SELECT l.id, r.id, m.id FROM l JOIN r ON r.k = l.k RIGHT JOIN m ON m.rk = r.id",
    )
    .fetch_all(&pool)
    .await
    .expect("three-way");
    let mut got: Vec<(Option<i32>, Option<i32>, Option<i32>)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    got.sort();
    let mut want = vec![
        (Some(1), Some(10), Some(100)),
        (Some(2), Some(10), Some(100)),
        (None, None, Some(101)),
        (None, None, Some(102)),
        (Some(3), Some(15), Some(103)),
    ];
    want.sort();
    assert_eq!(got, want);

    // And FULL at the end: r rows the inner join kept but m never named are
    // returned too.
    let rows = sqlx::query(
        "SELECT l.id, r.id, m.id FROM l JOIN r ON r.k = l.k FULL JOIN m ON m.rk = r.id",
    )
    .fetch_all(&pool)
    .await
    .expect("three-way full");
    let mut got: Vec<(Option<i32>, Option<i32>, Option<i32>)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    got.sort();
    want.extend([
        (Some(1), Some(11), None),
        (Some(2), Some(11), None),
        (Some(5), Some(14), None),
    ]);
    want.sort();
    assert_eq!(got, want);
}

/// A fold over a right join counts only the rows that matched, and groups the
/// NULL key on its own.
#[tokio::test]
async fn aggregate_over_a_right_join() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let rows = sqlx::query(
        "SELECT r.k AS k, COUNT(l.id) AS c, COUNT(*) AS n FROM l RIGHT JOIN r ON r.k = l.k GROUP BY r.k",
    )
    .fetch_all(&pool)
    .await
    .expect("grouped right join");
    let mut got: Vec<(Option<i32>, i64, i64)> = rows
        .iter()
        .map(|row| (row.get("k"), row.get("c"), row.get("n")))
        .collect();
    got.sort();
    let mut want = vec![
        (Some(1), 4, 4),
        (Some(3), 0, 1),
        (None, 0, 1),
        (Some(0), 1, 1),
        (Some(2), 1, 1),
    ];
    want.sort();
    assert_eq!(got, want);
}
