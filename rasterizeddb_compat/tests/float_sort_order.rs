//! `ORDER BY` over a float column holding NaN, infinities, signed zeros and
//! NULL, in PostgreSQL's order.
//!
//! PostgreSQL orders floats totally: `NaN = NaN`, NaN is greater than every
//! other value (infinity included), `-0 = +0`, and NULL — larger still by
//! default — comes after NaN ascending and before it descending. The engine
//! used to treat NaN as *equal to every value*, which is not an order at all:
//! the capped (top-N) and uncapped paths could return different rows, and the
//! result depended on the input order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

const VALUES: [(i32, Option<f64>); 9] = [
    (1, Some(1.5)),
    (2, Some(f64::NAN)),
    (3, None),
    (4, Some(-0.0)),
    (5, Some(0.0)),
    (6, Some(f64::INFINITY)),
    (7, Some(f64::NEG_INFINITY)),
    (8, Some(f64::NAN)),
    (9, Some(-2.0)),
];

/// What PostgreSQL returns for `ORDER BY v, id` and `ORDER BY v DESC, id`.
const ASC: [i32; 9] = [7, 9, 4, 5, 1, 6, 2, 8, 3];
const DESC: [i32; 9] = [3, 2, 8, 6, 1, 4, 5, 9, 7];

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE fo (id INTEGER NOT NULL, v DOUBLE PRECISION)")
        .execute(&pool)
        .await
        .expect("create");
    // Inserted in reverse so the expected order is not the insertion order.
    for (id, v) in VALUES.iter().rev() {
        sqlx::query("INSERT INTO fo (id, v) VALUES ($1, $2)")
            .bind(*id)
            .bind(*v)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, tail: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!("SELECT id FROM fo {}", tail)))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", tail, e))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn nan_null_and_signed_zeros_sort_as_postgres_does() {
    let (_s, pool) = seeded().await;

    // The values went in as written: NaN stays NaN and -0 keeps its sign.
    let stored: Vec<(i32, Option<f64>)> = sqlx::query("SELECT id, v FROM fo ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| (r.get::<i32, _>(0), r.get::<Option<f64>, _>(1)))
        .collect();
    for ((id, want), (got_id, got)) in VALUES.iter().zip(stored.iter()) {
        assert_eq!(id, got_id);
        match (want, got) {
            (Some(w), Some(g)) if w.is_nan() => assert!(g.is_nan(), "id {id}: {g}"),
            (Some(w), Some(g)) => {
                assert_eq!(w.to_bits(), g.to_bits(), "id {id}: {w} vs {g}")
            }
            (None, None) => {}
            other => panic!("id {id}: {other:?}"),
        }
    }

    // Uncapped, then capped at several limits: the scan's top-N and the final
    // sort have to agree on one order.
    for (order, expect) in [("v, id", &ASC), ("v DESC, id", &DESC)] {
        assert_eq!(
            ids(&pool, &format!("ORDER BY {order}")).await,
            expect.to_vec(),
            "ORDER BY {order}"
        );
        for limit in [1usize, 3, 7, 8] {
            assert_eq!(
                ids(&pool, &format!("ORDER BY {order} LIMIT {limit}")).await,
                expect[..limit].to_vec(),
                "ORDER BY {order} LIMIT {limit}"
            );
        }
    }

    // Explicit NULLS placement routes to the expression sort, which has its
    // own comparator; it must order the floats the same way.
    assert_eq!(
        ids(&pool, "ORDER BY v NULLS FIRST, id").await,
        [3, 7, 9, 4, 5, 1, 6, 2, 8].to_vec()
    );
    assert_eq!(
        ids(&pool, "ORDER BY v DESC NULLS LAST, id LIMIT 4").await,
        [2, 8, 6, 1].to_vec()
    );

    // And a WHERE that excludes the NULL leaves NaN last.
    assert_eq!(
        ids(&pool, "WHERE v IS NOT NULL ORDER BY v, id").await,
        ASC[..8].to_vec()
    );
}

/// `MAX` of a column holding NaN is NaN, and `MIN` is unaffected by it —
/// PostgreSQL's answers, and order-independent ones.
#[tokio::test(flavor = "multi_thread")]
async fn min_and_max_follow_the_same_order() {
    let (_s, pool) = seeded().await;
    let row = sqlx::query("SELECT MIN(v), MAX(v) FROM fo")
        .fetch_one(&pool)
        .await
        .expect("min/max");
    assert_eq!(row.get::<Option<f64>, _>(0), Some(f64::NEG_INFINITY));
    assert!(row.get::<Option<f64>, _>(1).is_some_and(f64::is_nan));
}
