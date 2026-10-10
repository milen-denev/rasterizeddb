//! Joins wide enough, or shaped enough, to reach the corners of the join path.
//!
//! The post-join predicate runs over one flattened row holding every fetched
//! column of every relation, and the predicate engine addresses that row
//! through a `u8` position map. Two 140-column tables make a 280-column row:
//! position 256 wrapped to 0 and read another column's bytes, and position 255
//! was the "absent" sentinel. Both answered — a filter comparing the wrong
//! columns returns rows, or none, rather than an error.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};

const WIDTH: usize = 140;

async fn wide_pair() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for table in ["wa", "wb"] {
        let columns: Vec<String> = (1..WIDTH).map(|k| format!("c{} BIGINT NOT NULL", k)).collect();
        let sql = format!("CREATE TABLE {} (id INT NOT NULL, {})", table, columns.join(", "));
        conn.execute(AssertSqlSafe(sql.clone())).await.expect(&sql);
    }
    // wa.cK = id*1000 + K; wb.cK = id*1000 + K + 500, so every wb column is
    // larger than every wa column of the same row and no two columns agree.
    for id in 1..=3i64 {
        for (table, bump) in [("wa", 0i64), ("wb", 500i64)] {
            let names: Vec<String> = (1..WIDTH).map(|k| format!("c{}", k)).collect();
            let values: Vec<String> = (1..WIDTH)
                .map(|k| (id * 1000 + k as i64 + bump).to_string())
                .collect();
            let sql = format!(
                "INSERT INTO {} (id, {}) VALUES ({}, {})",
                table,
                names.join(", "),
                id,
                values.join(", ")
            );
            conn.execute(AssertSqlSafe(sql.clone())).await.expect("insert");
        }
    }
    (server, conn)
}

fn select_all() -> String {
    let mut items = vec!["a.id".to_string()];
    items.extend((1..WIDTH).map(|k| format!("a.c{}", k)));
    items.push("b.id".to_string());
    items.extend((1..WIDTH).map(|k| format!("b.c{}", k)));
    items.join(", ")
}

/// A cross-relation predicate on the last columns of a 280-column joined row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_post_join_filter_past_column_255_reads_its_own_column() {
    let (_server, mut conn) = wide_pair().await;
    let base = format!("SELECT {} FROM wa a JOIN wb b ON a.id = b.id", select_all());

    for (predicate, expected) in [
        // True for every row: wb's columns are wa's plus 500.
        ("b.c139 > a.c139", 3usize),
        ("b.c116 > a.c139", 3),
        ("b.c115 = a.c115 + 500", 3),
        // False for every row.
        ("b.c139 < a.c1", 0),
        ("b.c116 = a.c116", 0),
        // One row.
        ("b.c139 = 3639", 1),
    ] {
        let sql = format!("{} WHERE {} ORDER BY a.id", base, predicate);
        let rows = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", predicate, e));
        assert_eq!(rows.len(), expected, "WHERE {} returned {} rows", predicate, rows.len());
        for row in &rows {
            let id: i32 = row.get(0);
            let last: i64 = row.get(2 * WIDTH - 1);
            assert_eq!(last, id as i64 * 1000 + 139 + 500, "b.c139 of row {}", id);
        }
    }
}

async fn pools_and_tasks() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE pool (id INT NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE task (id INT NOT NULL, pool_id INT, v INT NOT NULL, amount NUMERIC(10,2) NOT NULL)",
        "INSERT INTO pool (id, name) VALUES (1, 'alpha')",
        "INSERT INTO pool (id, name) VALUES (2, 'beta')",
        "INSERT INTO pool (id, name) VALUES (3, 'gamma')",
        // pool 1: three tasks, v sums to 3. pool 2: one task, v = 10.
        // pool 3: none. Two tasks with no pool at all.
        "INSERT INTO task (id, pool_id, v, amount) VALUES (10, 1, 1, 1.25)",
        "INSERT INTO task (id, pool_id, v, amount) VALUES (11, 1, 1, 2.50)",
        "INSERT INTO task (id, pool_id, v, amount) VALUES (12, 1, 1, 100.00)",
        "INSERT INTO task (id, pool_id, v, amount) VALUES (13, 2, 10, 0.05)",
        "INSERT INTO task (id, pool_id, v, amount) VALUES (14, NULL, 7, 3.00)",
        "INSERT INTO task (id, pool_id, v, amount) VALUES (15, NULL, 8, 4.00)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

/// A compound `HAVING` over a join took everything after the first operator as
/// the literal, could not read it, and so kept every group. It must either be
/// answered correctly or refused — never answered with every group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compound_having_over_a_join_is_answered_or_refused() {
    let (_server, mut conn) = pools_and_tasks().await;
    for (having, expected) in [
        // pool 1 has 3 tasks but SUM(v) = 3; pool 2 has 1 task: nobody passes.
        ("COUNT(*) > 2 AND SUM(t.v) > 5", Vec::<i32>::new()),
        ("COUNT(*) + 1 > 3", vec![1]),
        ("MIN(p.name) = 'alpha' AND COUNT(*) > 5", vec![]),
    ] {
        let sql = format!(
            "SELECT p.id FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id HAVING {} ORDER BY p.id",
            having
        );
        match sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&mut conn).await {
            Ok(rows) => {
                let got: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
                assert_eq!(got, expected, "HAVING {} answered wrongly", having);
            }
            Err(e) => assert!(
                e.to_string().contains("HAVING"),
                "HAVING {}: unexpected error {}",
                having,
                e
            ),
        }
    }
    // The simple form still works.
    let rows = sqlx::query(
        "SELECT p.id FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id HAVING COUNT(*) > 2",
    )
    .fetch_all(&mut conn)
    .await
    .expect("a simple HAVING over a join");
    assert_eq!(rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>(), vec![1]);
}

/// A `NUMERIC(10,2)` column read, summed, averaged and tested through a join
/// keeps its declared scale: the fold's positional field used to drop it, so
/// a sum stored at two places was described and compared at nine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scaled_numeric_keeps_its_scale_through_a_join() {
    use rust_decimal::Decimal;
    let (_server, mut conn) = pools_and_tasks().await;
    let d = |s: &str| s.parse::<Decimal>().unwrap();
    let mut wrong = Vec::new();
    for (sql, want) in [
        (
            "SELECT t.amount FROM task t JOIN pool p ON p.id = t.pool_id ORDER BY t.id",
            vec![d("1.25"), d("2.50"), d("100.00"), d("0.05")],
        ),
        (
            "SELECT SUM(t.amount), p.id FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id ORDER BY p.id",
            vec![d("103.75"), d("0.05")],
        ),
        (
            "SELECT MAX(t.amount), p.id FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id ORDER BY p.id",
            vec![d("100.00"), d("0.05")],
        ),
        (
            "SELECT AVG(t.amount), p.id FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id ORDER BY p.id",
            vec![d("34.5833333333333333"), d("0.05")],
        ),
        (
            "SELECT SUM(t.amount) FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id HAVING SUM(t.amount) > 100",
            vec![d("103.75")],
        ),
        (
            "SELECT SUM(t.amount) FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id HAVING SUM(t.amount) < 0.06",
            vec![d("0.05")],
        ),
    ] {
        for protocol in ["binary", "text"] {
            let fetch = async {
                if protocol == "binary" {
                    sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await
                } else {
                    sqlx::raw_sql(AssertSqlSafe(sql)).fetch_all(&mut conn).await
                }
            };
            let Ok(got) = tokio::time::timeout(std::time::Duration::from_secs(20), fetch).await else {
                panic!("{protocol} {sql}: no answer in 20 s");
            };
            match got {
                Ok(rows) => {
                    let got: Vec<Result<Decimal, String>> =
                        rows.iter().map(|r| r.try_get::<Decimal, _>(0).map_err(|e| e.to_string())).collect();
                    let ok = got.len() == want.len()
                        && got.iter().zip(&want).all(|(g, w)| g.as_ref().is_ok_and(|g| g == w));
                    if !ok {
                        wrong.push(format!("{protocol:6} {sql}: got {got:?}, want {want:?}"));
                    }
                }
                Err(e) => wrong.push(format!("{protocol:6} {sql}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// A join the planner refuses must leave the connection usable, over either
/// protocol, for the statement after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_join_leaves_the_connection_usable() {
    let (_server, mut conn) = pools_and_tasks().await;
    let bad = "SELECT SUM(t.amount) FROM task t JOIN pool p ON p.id = t.pool_id GROUP BY p.id ORDER BY p.id";
    let good = "SELECT COUNT(*) FROM task t JOIN pool p ON p.id = t.pool_id";
    for (first, second) in [("text", "binary"), ("binary", "binary"), ("text", "text"), ("binary", "text")] {
        let run = |protocol: &'static str, sql: &'static str, conn: &mut sqlx::PgConnection| {
            let conn: *mut sqlx::PgConnection = conn;
            async move {
                // SAFETY: each future is awaited before the next is made.
                let conn = unsafe { &mut *conn };
                let fut = async {
                    if protocol == "binary" {
                        sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut *conn).await
                    } else {
                        sqlx::raw_sql(AssertSqlSafe(sql)).fetch_all(&mut *conn).await
                    }
                };
                tokio::time::timeout(std::time::Duration::from_secs(20), fut)
                    .await
                    .unwrap_or_else(|_| panic!("{protocol} {sql}: no answer in 20 s"))
            }
        };
        let _ = run(first, bad, &mut conn).await;
        let rows = run(second, good, &mut conn)
            .await
            .unwrap_or_else(|e| panic!("{first} error then {second} query: {e}"));
        assert_eq!(rows.len(), 1);
    }
}

async fn ids(conn: &mut sqlx::PgConnection, sql: &str) -> Result<Vec<i32>, String> {
    let fut = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&mut *conn);
    match tokio::time::timeout(std::time::Duration::from_secs(20), fut).await {
        Err(_) => panic!("{sql}: no answer in 20 s"),
        Ok(Err(e)) => Err(e.to_string()),
        Ok(Ok(rows)) => Ok(rows.iter().map(|r| r.get::<i32, _>(0)).collect()),
    }
}

/// `LEFT JOIN r ON r.k = l.x WHERE r.k IS NULL` on a *nullable* join key is an
/// anti-join: a NULL key never matches, so a present right row's key is never
/// NULL. It used to be refused outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_anti_join_on_a_nullable_key() {
    let (_server, mut conn) = pools_and_tasks().await;
    // A third pool-less task and a pool matched twice, so the probe sees
    // fan-out and NULL keys on both sides.
    for sql in [
        "INSERT INTO task (id, pool_id, v, amount) VALUES (16, NULL, 1, 1.00)",
        "CREATE TABLE tag (id INT NOT NULL, task_pool INT)",
        "INSERT INTO tag (id, task_pool) VALUES (1, 1)",
        "INSERT INTO tag (id, task_pool) VALUES (2, NULL)",
        "INSERT INTO tag (id, task_pool) VALUES (3, 9)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    for (sql, want) in [
        // Nullable key on the right: pool 3 has no task.
        ("SELECT p.id FROM pool p LEFT JOIN task t ON t.pool_id = p.id WHERE t.pool_id IS NULL ORDER BY p.id", vec![3]),
        ("SELECT p.id FROM pool p LEFT JOIN task t ON p.id = t.pool_id WHERE t.pool_id IS NULL ORDER BY p.id", vec![3]),
        // Nullable key on the left: the pool-less tasks find nothing.
        ("SELECT t.id FROM task t LEFT JOIN pool p ON p.id = t.pool_id WHERE p.id IS NULL ORDER BY t.id", vec![14, 15, 16]),
        // Both sides nullable.
        ("SELECT g.id FROM tag g LEFT JOIN task t ON t.pool_id = g.task_pool WHERE t.pool_id IS NULL ORDER BY g.id", vec![2, 3]),
        // The anti-join followed by an inner join on the left relation.
        ("SELECT p.id FROM pool p LEFT JOIN task t ON t.pool_id = p.id JOIN tag g ON g.task_pool = p.id WHERE t.pool_id IS NULL ORDER BY p.id", vec![]),
        // The ordinary LEFT JOIN is unchanged.
        ("SELECT t.id FROM pool p LEFT JOIN task t ON t.pool_id = p.id WHERE p.id = 1 ORDER BY t.id", vec![10, 11, 12]),
    ] {
        assert_eq!(ids(&mut conn, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")), want, "{sql}");
    }
    // A FULL join keeps the right side's own NULL-keyed rows *present*, so
    // `IS NULL` there is not "unmatched". Refused or answered right — never
    // answered with only the unmatched pool.
    let sql = "SELECT p.id FROM pool p FULL JOIN task t ON t.pool_id = p.id WHERE t.pool_id IS NULL";
    if let Ok(got) = ids(&mut conn, sql).await {
        panic!("{sql}: answered {got:?}; PostgreSQL also returns the three pool-less tasks");
    }
}

/// Every join kind over nullable keys, through a real scan, against the
/// definition computed here. The scan drops NULL-keyed rows on every side that
/// is not preserved; this is what proves it drops none that are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joins_over_nullable_keys_match_the_definition() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    let mut rng: u64 = 0x9e3779b97f4a7c15;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut generate = |n: usize| -> Vec<(i32, Option<i32>)> {
        (0..n)
            .map(|i| (i as i32, if next() % 4 == 0 { None } else { Some((next() % 5) as i32) }))
            .collect()
    };
    let (a, b, c) = (generate(30), generate(25), generate(20));
    for (table, rows) in [("na", &a), ("nb", &b), ("nc", &c)] {
        conn.execute(AssertSqlSafe(format!("CREATE TABLE {} (id INT NOT NULL, k INT)", table)))
            .await
            .expect("create");
        for (id, k) in rows.iter() {
            let k = k.map_or("NULL".to_string(), |k| k.to_string());
            conn.execute(AssertSqlSafe(format!("INSERT INTO {} (id, k) VALUES ({}, {})", table, id, k)))
                .await
                .expect("insert");
        }
    }
    type T = (Option<i32>, Option<i32>);
    let step = |kind: &str, left: &[(i32, Option<i32>)], right: &[(i32, Option<i32>)]| -> Vec<T> {
        let mut out = Vec::new();
        let mut hit = vec![false; right.len()];
        for (lid, lk) in left {
            let mut any = false;
            for (r, (rid, rk)) in right.iter().enumerate() {
                if lk.is_some() && lk == rk {
                    any = true;
                    hit[r] = true;
                    out.push((Some(*lid), Some(*rid)));
                }
            }
            if !any && matches!(kind, "LEFT" | "FULL") {
                out.push((Some(*lid), None));
            }
        }
        if matches!(kind, "RIGHT" | "FULL") {
            for (r, (rid, _)) in right.iter().enumerate() {
                if !hit[r] {
                    out.push((None, Some(*rid)));
                }
            }
        }
        out.sort();
        out
    };
    let mut wrong = Vec::new();
    for kind in ["INNER", "LEFT", "RIGHT", "FULL"] {
        let sql = format!("SELECT x.id, y.id FROM na x {} JOIN nb y ON y.k = x.k", kind);
        let mut got: Vec<T> = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        got.sort();
        let want = step(kind, &a, &b);
        if got != want {
            wrong.push(format!("{sql}: got {} rows, want {}", got.len(), want.len()));
        }
        // Three-way: z keys off y, so y's NULL keys meet a second join.
        for kind2 in ["INNER", "LEFT"] {
            let sql = format!(
                "SELECT x.id, y.id, z.id FROM na x {} JOIN nb y ON y.k = x.k {} JOIN nc z ON z.k = y.k",
                kind, kind2
            );
            let mut got: Vec<(Option<i32>, Option<i32>, Option<i32>)> = sqlx::query(AssertSqlSafe(sql.clone()))
                .fetch_all(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect();
            got.sort();
            let mut want = Vec::new();
            for (x, y) in step(kind, &a, &b) {
                let yk = y.and_then(|y| b.iter().find(|r| r.0 == y).unwrap().1);
                let matches: Vec<i32> = c.iter().filter(|(_, zk)| yk.is_some() && *zk == yk).map(|r| r.0).collect();
                if matches.is_empty() {
                    if kind2 == "LEFT" {
                        want.push((x, y, None));
                    }
                } else {
                    for z in matches {
                        want.push((x, y, Some(z)));
                    }
                }
            }
            want.sort();
            if got != want {
                wrong.push(format!("{sql}: got {} rows, want {}", got.len(), want.len()));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// `UNION` de-duplicates on the whole row — a NULL is not `''` — `UNION ALL`
/// keeps every row, and a trailing `LIMIT` caps the combined result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_dedups_whole_rows_and_limits_the_combination() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE ua (id INT NOT NULL, label TEXT)",
        "CREATE TABLE ub (id INT NOT NULL, label TEXT)",
        "INSERT INTO ua (id, label) VALUES (1, 'x')",
        "INSERT INTO ua (id, label) VALUES (1, 'x')",
        "INSERT INTO ua (id, label) VALUES (2, '')",
        "INSERT INTO ub (id, label) VALUES (2, NULL)",
        "INSERT INTO ub (id, label) VALUES (1, 'x')",
        "INSERT INTO ub (id, label) VALUES (3, 'y')",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    let x = || Some("x".to_string());
    assert_eq!(
        union_rows(&mut conn, "SELECT id, label FROM ua UNION SELECT id, label FROM ub").await,
        vec![(1, x()), (2, None), (2, Some(String::new())), (3, Some("y".into()))]
    );
    assert_eq!(union_rows(&mut conn, "SELECT id, label FROM ua UNION ALL SELECT id, label FROM ub").await.len(), 6);
    assert_eq!(union_rows(&mut conn, "SELECT id, label FROM ua UNION SELECT id, label FROM ub LIMIT 2").await.len(), 2);
    assert_eq!(union_rows(&mut conn, "SELECT id, label FROM ua UNION ALL SELECT id, label FROM ub LIMIT 4").await.len(), 4);
}

async fn union_rows(conn: &mut sqlx::PgConnection, sql: &str) -> Vec<(i32, Option<String>)> {
    let mut got: Vec<(i32, Option<String>)> = sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    got.sort();
    got
}

/// An `ON` term that reads only the left relation restricts which left rows
/// may match, for every join kind, with PostgreSQL's meaning: INNER drops a
/// left row that fails it (as WHERE would), LEFT and FULL keep it NULL-padded,
/// and RIGHT and FULL return the right rows only it could have matched as
/// unmatched. It used to be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_terms_over_the_left_relation_match_the_definition() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    let a: Vec<(i32, Option<i32>, i32)> = (0..24)
        .map(|i| (i, if i % 5 == 0 { None } else { Some(i % 4) }, i % 3))
        .collect();
    let b: Vec<(i32, Option<i32>)> = (0..12).map(|i| (100 + i, if i % 6 == 0 { None } else { Some(i % 5) })).collect();
    for sql in [
        "CREATE TABLE la (id INT NOT NULL, k INT, flag INT NOT NULL)".to_string(),
        "CREATE TABLE lb (id INT NOT NULL, k INT)".to_string(),
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect("create");
    }
    for (id, k, flag) in &a {
        let k = k.map_or("NULL".to_string(), |k| k.to_string());
        conn.execute(AssertSqlSafe(format!("INSERT INTO la (id, k, flag) VALUES ({id}, {k}, {flag})")))
            .await
            .expect("insert");
    }
    for (id, k) in &b {
        let k = k.map_or("NULL".to_string(), |k| k.to_string());
        conn.execute(AssertSqlSafe(format!("INSERT INTO lb (id, k) VALUES ({id}, {k})")))
            .await
            .expect("insert");
    }
    let mut wrong = Vec::new();
    for kind in ["INNER", "LEFT", "RIGHT", "FULL"] {
        for (term, passes) in [
            ("x.flag = 1", (|f: i32, _id: i32| f == 1) as fn(i32, i32) -> bool),
            ("x.flag <> 2 AND x.id > 5", |f, id| f != 2 && id > 5),
        ] {
            let sql = format!(
                "SELECT x.id, y.id FROM la x {kind} JOIN lb y ON y.k = x.k AND {term}"
            );
            let got = sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&mut conn).await;
            let mut got: Vec<(Option<i32>, Option<i32>)> = match got {
                Ok(rows) => rows.iter().map(|r| (r.get(0), r.get(1))).collect(),
                Err(e) => {
                    wrong.push(format!("{sql}: error {e}"));
                    continue;
                }
            };
            got.sort();
            let mut want = Vec::new();
            let mut hit = vec![false; b.len()];
            for (id, k, flag) in &a {
                let mut any = false;
                for (r, (bid, bk)) in b.iter().enumerate() {
                    if k.is_some() && k == bk && passes(*flag, *id) {
                        any = true;
                        hit[r] = true;
                        want.push((Some(*id), Some(*bid)));
                    }
                }
                if !any && matches!(kind, "LEFT" | "FULL") {
                    want.push((Some(*id), None));
                }
            }
            if matches!(kind, "RIGHT" | "FULL") {
                for (r, (bid, _)) in b.iter().enumerate() {
                    if !hit[r] {
                        want.push((None, Some(*bid)));
                    }
                }
            }
            want.sort();
            if got != want {
                wrong.push(format!("{sql}: got {} rows, want {}", got.len(), want.len()));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// `GROUP BY p.id` over a join, with `p`'s other columns selected — the
/// Django/Rails shape. `p.id` is `p`'s primary key, so they are functionally
/// dependent on it. A key that is not unique (or is nullable) still requires
/// them to be grouped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouping_by_a_joined_primary_key_admits_its_columns() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE fp (id INT PRIMARY KEY, name TEXT NOT NULL, region TEXT)",
        "CREATE TABLE ft (id INT NOT NULL, pool_id INT NOT NULL, v INT NOT NULL)",
        "INSERT INTO fp (id, name, region) VALUES (1, 'alpha', 'eu')",
        "INSERT INTO fp (id, name, region) VALUES (2, 'beta', NULL)",
        "INSERT INTO fp (id, name, region) VALUES (3, 'alpha', 'us')",
        "INSERT INTO ft (id, pool_id, v) VALUES (10, 1, 1)",
        "INSERT INTO ft (id, pool_id, v) VALUES (11, 1, 2)",
        "INSERT INTO ft (id, pool_id, v) VALUES (12, 2, 5)",
        "INSERT INTO ft (id, pool_id, v) VALUES (13, 3, 7)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    let rows = sqlx::query(
        "SELECT p.id, p.name, p.region, COUNT(*), SUM(t.v) FROM fp p JOIN ft t ON t.pool_id = p.id GROUP BY p.id ORDER BY p.id",
    )
    .fetch_all(&mut conn)
    .await
    .expect("columns dependent on a grouped primary key");
    let got: Vec<(i32, String, Option<String>, i64, i64)> =
        rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))).collect();
    assert_eq!(
        got,
        vec![
            (1, "alpha".into(), Some("eu".into()), 2, 3),
            (2, "beta".into(), None, 1, 5),
            (3, "alpha".into(), Some("us".into()), 1, 7),
        ]
    );
    // `t.pool_id` is not unique in `ft`, so `t.v` is not dependent on it;
    // `p.name` is not unique in `fp`.
    for sql in [
        "SELECT t.pool_id, t.v, COUNT(*) FROM fp p JOIN ft t ON t.pool_id = p.id GROUP BY t.pool_id",
        "SELECT p.name, p.region, COUNT(*) FROM fp p JOIN ft t ON t.pool_id = p.id GROUP BY p.name",
    ] {
        let err = sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await.expect_err(sql);
        assert!(err.to_string().contains("GROUP BY"), "{sql}: {err}");
    }
}
