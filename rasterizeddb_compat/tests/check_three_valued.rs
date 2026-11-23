//! A `CHECK` refuses a row only when it is FALSE: SQL's three values, as
//! PostgreSQL evaluates them.
//!
//! `CHECK (x IS NULL OR x > 0)` refused a NULL `x` (23514), on plain and
//! partitioned tables alike. The check was compiled against the table without
//! its nullability companions, so the rewriter read every column as NOT NULL:
//! `x IS NULL` became a constant FALSE and `x IS NOT NULL` a constant TRUE,
//! and a guess about NULL inputs covered only half of that. `CHECK
//! (COALESCE(x, 1) > 0)` was stored and never enforced, and `UPDATE … SET x =
//! NULL` was refused by a check NULL satisfies. Every outcome here is what
//! PostgreSQL 17 gives, and the table is read back, again after a restart.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("check_3vl_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
    code(&e)
}

async fn rows(pool: &sqlx::PgPool, table: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(format!("SELECT id, x, y FROM {table} ORDER BY id")))
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| {
            let show = |v: Option<i32>| v.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
            format!("{} {} {}", r.get::<i32, _>("id"), show(r.get("x")), show(r.get("y")))
        })
        .collect()
}

/// Each check, and for each (x, y) whether PostgreSQL stores the row.
const CASES: &[(&str, &[(Option<i32>, Option<i32>, bool)])] = &[
    ("x > 0", &[(None, None, true), (Some(1), None, true), (Some(0), None, false)]),
    ("x IS NULL OR x > 0", &[(None, None, true), (Some(2), None, true), (Some(-1), None, false)]),
    ("x IS NOT NULL", &[(None, None, false), (Some(-5), None, true)]),
    ("x IS NOT NULL AND x > 0", &[(None, None, false), (Some(3), None, true), (Some(-3), None, false)]),
    ("x > 0 AND x < 10", &[(None, None, true), (Some(11), None, false), (Some(5), None, true)]),
    ("NOT (x > 5)", &[(None, None, true), (Some(6), None, false), (Some(5), None, true)]),
    ("COALESCE(x, 1) > 0", &[(None, None, true), (Some(-3), None, false), (Some(4), None, true)]),
    ("COALESCE(x, -1) > 0", &[(None, None, false), (Some(4), None, true)]),
    // Read as written (the rewriter cannot), and still UNKNOWN for a NULL x.
    ("COALESCE(y, 1) > x", &[(None, None, true), (Some(5), None, false), (Some(0), None, true), (None, Some(-4), true)]),
    // Two columns: FALSE OR FALSE refuses; UNKNOWN passes.
    ("x < y", &[(None, Some(1), true), (Some(2), Some(1), false), (Some(1), Some(2), true)]),
    ("x IS NOT NULL OR y > 0", &[(None, Some(-1), false), (None, None, true), (None, Some(5), true), (Some(-9), Some(-9), true)]),
    ("x IS NULL AND y IS NULL OR x < y", &[(None, None, true), (None, Some(3), true), (Some(4), Some(3), false)]),
];

fn lit(v: Option<i32>) -> String {
    v.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into())
}

async fn three_valued(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let mut want: Vec<(String, Vec<String>)> = Vec::new();
    {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        for (n, (check, rows_in)) in CASES.iter().enumerate() {
            // Declared at CREATE TABLE on even cases, by ALTER … ADD on odd.
            let table = format!("k{n}");
            if n % 2 == 0 {
                run(&pool, &format!("CREATE TABLE {table} (id INT PRIMARY KEY, x INT, y INT, CHECK ({check})){partitioning}")).await;
            } else {
                run(&pool, &format!("CREATE TABLE {table} (id INT PRIMARY KEY, x INT, y INT){partitioning}")).await;
                run(&pool, &format!("ALTER TABLE {table} ADD CONSTRAINT {table}_c CHECK ({check})")).await;
            }
            let mut kept = Vec::new();
            for (i, (x, y, ok)) in rows_in.iter().enumerate() {
                let sql = format!("INSERT INTO {table} VALUES ({i}, {}, {})", lit(*x), lit(*y));
                if *ok {
                    run(&pool, &sql).await;
                    kept.push(format!("{i} {} {}", lit(*x), lit(*y)));
                } else {
                    assert_eq!(refused(&pool, &sql).await, "23514", "{partitioning} CHECK ({check}): {sql}");
                }
            }
            // UPDATE: each refused pair assigned to the first stored row, as
            // constants and then as an expression of the row, is refused the
            // same way; each accepted pair is taken.
            let target = rows_in.iter().position(|r| r.2).unwrap();
            for (x, y, ok) in rows_in.iter() {
                let set = format!("UPDATE {table} SET x = {}, y = {} WHERE id = {target}", lit(*x), lit(*y));
                let computed = format!(
                    "UPDATE {table} SET x = {} + 0 * id, y = {} + 0 * id WHERE id = {target}",
                    lit(*x),
                    lit(*y)
                );
                for sql in [&set, &computed] {
                    if *ok {
                        run(&pool, sql).await;
                    } else {
                        assert_eq!(refused(&pool, sql).await, "23514", "{partitioning} CHECK ({check}): {sql}");
                    }
                }
            }
            // The target ends holding the last accepted pair.
            let (lx, ly, _) = rows_in.iter().rev().find(|r| r.2).unwrap();
            kept[0] = format!("{target} {} {}", lit(*lx), lit(*ly));
            assert_eq!(rows(&pool, &table).await, kept, "{partitioning} CHECK ({check})");
            want.push((table, kept));
        }

        // ADD CONSTRAINT judges the stored rows three-valued too: a NULL row
        // passes `x > 0`, and a row the check finds FALSE refuses it.
        run(&pool, &format!("CREATE TABLE v (id INT PRIMARY KEY, x INT, y INT){partitioning}")).await;
        run(&pool, "INSERT INTO v VALUES (1, NULL, NULL), (2, 4, NULL)").await;
        run(&pool, "ALTER TABLE v ADD CONSTRAINT v_pos CHECK (x > 0)").await;
        run(&pool, "ALTER TABLE v ADD CONSTRAINT v_or CHECK (x IS NULL OR x > 1)").await;
        assert_eq!(refused(&pool, "ALTER TABLE v ADD CONSTRAINT v_nn CHECK (x IS NOT NULL)").await, "23514");
        assert_eq!(refused(&pool, "ALTER TABLE v ADD CONSTRAINT v_co CHECK (COALESCE(x, 0) > 0)").await, "23514");
        run(&pool, "ALTER TABLE v ADD CONSTRAINT v_co CHECK (COALESCE(x, 1) > 0)").await;
        run(&pool, "INSERT INTO v VALUES (3, NULL, NULL)").await;
        assert_eq!(refused(&pool, "INSERT INTO v VALUES (4, 1, NULL)").await, "23514");
        want.push(("v".into(), vec!["1 NULL NULL".into(), "2 4 NULL".into(), "3 NULL NULL".into()]));
        for (table, rows_want) in &want {
            assert_eq!(&rows(&pool, table).await, rows_want, "{partitioning} {table}");
        }
    }
    // The checks reload as they were enforced.
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    for (table, rows_want) in &want {
        assert_eq!(&rows(&pool, table).await, rows_want, "{partitioning} {table} after a restart");
    }
    run(&pool, "INSERT INTO k1 VALUES (100, NULL, NULL)").await;
    assert_eq!(refused(&pool, "INSERT INTO k1 VALUES (101, -1, NULL)").await, "23514");
    assert_eq!(refused(&pool, "INSERT INTO k2 VALUES (100, NULL, NULL)").await, "23514");
    assert_eq!(refused(&pool, "INSERT INTO k6 VALUES (100, -3, NULL)").await, "23514");
    run(&pool, "INSERT INTO k6 VALUES (101, NULL, NULL)").await;
    run(&pool, "INSERT INTO k8 VALUES (100, NULL, 7)").await;
    assert_eq!(refused(&pool, "INSERT INTO k8 VALUES (101, 3, 2)").await, "23514");
}

#[tokio::test(flavor = "multi_thread")]
async fn check_constraints_are_three_valued_on_a_plain_table() {
    three_valued("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn check_constraints_are_three_valued_on_a_hash_partitioned_table() {
    three_valued(" PARTITION BY HASH (id) PARTITIONS 4").await;
}

/// A check neither the rewriter nor the evaluator can read is refused where
/// it is declared, rather than stored and never enforced.
#[tokio::test(flavor = "multi_thread")]
async fn an_uncompilable_check_is_refused_at_create_table() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let e = refused(&pool, "CREATE TABLE u (id INT PRIMARY KEY, x INT CHECK (no_such_fn(x) > 0))").await;
    assert_eq!(e, "0A000");
    // Nothing was created.
    assert_eq!(refused(&pool, "SELECT * FROM u").await, "42P01");
}
