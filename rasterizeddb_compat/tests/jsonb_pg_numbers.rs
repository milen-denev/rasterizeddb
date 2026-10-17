//! JSON numbers and `jsonb` text, answered as PostgreSQL 17 answers them.
//!
//! A JSON number in PostgreSQL is a `numeric`: every digit kept, the scale as
//! written (`100.00` stays `100.00`), an exponent written out (`1e0` is `1`),
//! no negative zero, and a range far past a `f64` (`1e400` is stored). The
//! engine used to read every fraction and exponent through a `f64`, so
//! `1.000000000000000000001` was stored as `1.0` and then contained in `1`,
//! `9007199254740993.0` became `...994.0`, and `1e400` was refused. Every
//! INSERT reported success and the value read back was a different one.
//!
//! The text of a container is PostgreSQL's too: a space after `:` and `,`,
//! and keys shorter first, then bytewise. `->>` of an object compared with
//! `=` or `LIKE` sees that text, so the compact form returned other rows.
//!
//! Every expected value here was produced by running the same statement on
//! PostgreSQL 17 (`docker run postgres:17`); none needs a server to check.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const ROWS: &[(i64, &str)] = &[
    (1, r#"{"a":100.00}"#),
    (2, r#"{"a":1e0}"#),
    (3, r#"{"a":-0}"#),
    (4, r#"{"a":1.23456789012345678901}"#),
    (5, r#"{"a":123456789012345678901234567890}"#),
    (6, r#"{"a":-9223372036854775809}"#),
    (7, r#"{"a":1e400}"#),
    (8, r#"{"a":1.5E-3}"#),
    (9, r#"{"a":{"y":1,"it''s":2}}"#),
    (10, r#"{"a":[1,"x",{"bb":1e2,"y":1.50}]}"#),
    (11, "1.000000000000000000001"),
    (12, "[1.000000000000000000001]"),
    (13, "1"),
    (14, r#"{"a":9007199254740993.0}"#),
    (15, r#"{"a":9.007199254740993e15}"#),
    (16, r#"{"a":9007199254740993}"#),
];

/// `1e400` as `numeric` prints it.
fn e400() -> String {
    format!("1{}", "0".repeat(400))
}

/// PostgreSQL's `doc->>'a'` (and `doc#>>'{a}'`, `(doc->'a')::text`,
/// `doc->'a'`, which print the same here) per row; `None` is SQL NULL.
fn pg_member(id: i64) -> Option<String> {
    Some(match id {
        1 => "100.00".into(),
        2 => "1".into(),
        3 => "0".into(),
        4 => "1.23456789012345678901".into(),
        5 => "123456789012345678901234567890".into(),
        6 => "-9223372036854775809".into(),
        7 => e400(),
        8 => "0.0015".into(),
        9 => r#"{"y": 1, "it's": 2}"#.into(),
        10 => r#"[1, "x", {"y": 1.50, "bb": 100}]"#.into(),
        14 => "9007199254740993.0".into(),
        15 | 16 => "9007199254740993".into(),
        _ => return None,
    })
}

/// PostgreSQL's `SELECT doc` and `doc::text` per row.
fn pg_document(id: i64) -> String {
    match id {
        11 => "1.000000000000000000001".into(),
        12 => "[1.000000000000000000001]".into(),
        13 => "1".into(),
        _ => format!("{{\"a\": {}}}", pg_member(id).unwrap()),
    }
}

async fn seeded(indexed: bool) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE n1 (id BIGINT PRIMARY KEY, doc JSONB)"))
        .await
        .expect("create");
    for (id, doc) in ROWS {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO n1 VALUES ({id}, '{doc}')"
        ))))
        .await
        .unwrap_or_else(|e| panic!("seed {id} {doc}: {e}"));
    }
    if indexed {
        for ddl in [
            "CREATE INDEX n1_gin ON n1 USING gin (doc)",
            "CREATE INDEX n1_path ON n1 ((doc->>'a'))",
        ] {
            pool.execute(sqlx::raw_sql(ddl)).await.expect(ddl);
        }
    }
    (server, pool)
}

/// `(id, text)` for each row of a two-column query, read as the text the
/// server sent: the simple protocol sends every value as text, and the check
/// on the column's type is skipped because a `jsonb` column is not `TEXT`.
async fn texts(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<(i64, Option<String>)>, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|r| {
            let id: i64 = r.try_get_unchecked::<String, _>(0).unwrap().parse().unwrap();
            (id, r.try_get_unchecked::<Option<String>, _>(1).unwrap())
        })
        .collect())
}

async fn ids(pool: &sqlx::PgPool, predicate: &str) -> Result<Vec<i64>, String> {
    let sql = format!("SELECT id FROM n1 WHERE {predicate} ORDER BY id");
    let rows = sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows.iter().map(|r| r.get::<i64, _>(0)).collect())
}

/// The stored document and every way of reading a member back as text.
#[tokio::test(flavor = "multi_thread")]
async fn numbers_and_containers_read_back_as_postgres_prints_them() {
    let (_server, pool) = seeded(false).await;
    let mut wrong = Vec::new();
    for (label, sql, want) in [
        ("SELECT doc", "SELECT id, doc FROM n1 ORDER BY id", pg_document as fn(i64) -> String),
        ("doc::text", "SELECT id, doc::text FROM n1 ORDER BY id", pg_document),
    ] {
        match texts(&pool, sql).await {
            Ok(rows) => {
                assert_eq!(rows.len(), ROWS.len(), "{label}");
                for (id, got) in rows {
                    if got.as_deref() != Some(want(id).as_str()) {
                        wrong.push(format!("{label} row {id}: got {got:?}, PostgreSQL {:?}", want(id)));
                    }
                }
            }
            Err(e) => wrong.push(format!("{label}: {e}")),
        }
    }
    for (label, expr) in [
        ("->>", "doc->>'a'"),
        ("#>>", "doc#>>'{a}'"),
        ("->", "doc->'a'"),
        ("(->)::text", "(doc->'a')::text"),
    ] {
        match texts(&pool, &format!("SELECT id, {expr} FROM n1 ORDER BY id")).await {
            Ok(rows) => {
                assert_eq!(rows.len(), ROWS.len(), "{label}");
                for (id, got) in rows {
                    if got != pg_member(id) {
                        wrong.push(format!("{label} row {id}: got {got:?}, PostgreSQL {:?}", pg_member(id)));
                    }
                }
            }
            Err(e) => wrong.push(format!("{label}: {e}")),
        }
    }
    // length() of the text, which PostgreSQL reports as 6, 1, 1, 22, 30, 20,
    // 401, 6, 19, 32, then NULL for the three rows with no "a", 18, 16, 16.
    match texts(&pool, "SELECT id, length(doc->>'a') FROM n1 ORDER BY id").await {
        Ok(rows) => {
            for (id, got) in rows {
                let want = pg_member(id).map(|t| t.chars().count().to_string());
                if got != want {
                    wrong.push(format!("length row {id}: got {got:?}, PostgreSQL {want:?}"));
                }
            }
        }
        Err(e) => wrong.push(format!("length: {e}")),
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Predicates over the text and over the value, with and without the
/// indexes that answer them, which must agree with the scan and with
/// PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
async fn predicates_on_numbers_select_the_rows_postgres_selects() {
    let cases: &[(&str, &[i64])] = &[
        ("doc->>'a' = '100.00'", &[1]),
        ("doc->>'a' = '100.0'", &[]),
        ("doc->>'a' = '1'", &[2]),
        ("doc->>'a' = '1.0'", &[]),
        ("doc->>'a' = '0'", &[3]),
        ("doc->>'a' = '-0.0'", &[]),
        (r#"doc->>'a' = '{"y": 1, "it''s": 2}'"#, &[9]),
        (r#"doc->>'a' = '{"it''s":2,"y":1}'"#, &[]),
        (r#"doc->>'a' = '[1, "x", {"y": 1.50, "bb": 100}]'"#, &[10]),
        ("doc->>'a' LIKE '%, %'", &[9, 10]),
        ("doc @> '1'", &[13]),
        ("doc <@ '1.000000000000000000001'", &[11]),
        ("doc @> '[1]'", &[]),
        (r#"doc @> '{"a":9007199254740993.0}'"#, &[14, 15, 16]),
        (r#"doc @> '{"a":9007199254740992}'"#, &[]),
        (r#"doc @> '{"a":1e400}'"#, &[7]),
        (r#"doc @> '{"a":10e399}'"#, &[7]),
        (r#"doc @> '{"a":1e399}'"#, &[]),
        (r#"doc @> '{"a":0.0015}'"#, &[8]),
        (r#"doc @> '{"a":1e-400}'"#, &[]),
        (r#"doc @> '{"a":100}'"#, &[1]),
        (r#"doc @> '{"a":-0.0}'"#, &[3]),
        (r#"doc @> '{"a":1.2345678901234567}'"#, &[]),
        (r#"doc @> '{"a":[{"bb":100.0}]}'"#, &[10]),
    ];
    for indexed in [false, true] {
        let (_server, pool) = seeded(indexed).await;
        let mut wrong = Vec::new();
        for (predicate, want) in cases {
            match ids(&pool, predicate).await {
                Ok(got) if got == *want => {}
                Ok(got) => wrong.push(format!("{predicate}: got {got:?}, PostgreSQL {want:?}")),
                Err(e) => wrong.push(format!("{predicate}: {e}")),
            }
        }
        assert!(
            wrong.is_empty(),
            "indexed={indexed}: {} wrong:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }
}

/// `numeric`'s limits: PostgreSQL refuses both of these with "value
/// overflows numeric format", and stores `1e131071` (131072 digits).
#[tokio::test(flavor = "multi_thread")]
async fn numbers_past_numeric_are_refused_and_the_rest_are_kept() {
    let (_server, pool) = seeded(false).await;
    for doc in ["1e131072", r#"{"a":1e-16384}"#] {
        let err = pool
            .execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO n1 VALUES (100, '{doc}')"))))
            .await
            .expect_err(doc)
            .to_string();
        assert!(err.contains("value overflows numeric format"), "{doc}: {err}");
    }
    pool.execute(sqlx::raw_sql("INSERT INTO n1 VALUES (101, '[1e131071, 1e-16383]')"))
        .await
        .expect("PostgreSQL stores both");
    let rows = texts(&pool, "SELECT id, doc->>0 FROM n1 WHERE id = 101").await.unwrap();
    assert_eq!(rows[0].1.as_deref().map(str::len), Some(131_072));
    let rows = texts(&pool, "SELECT id, doc->>1 FROM n1 WHERE id = 101").await.unwrap();
    assert_eq!(rows[0].1, Some(format!("0.{}1", "0".repeat(16_382))));
}

/// A document carried through a derived table is written back as a literal
/// and read again; it must come out with its digits.
#[tokio::test(flavor = "multi_thread")]
async fn a_document_through_a_subquery_keeps_its_digits() {
    let (_server, pool) = seeded(false).await;
    let rows = texts(
        &pool,
        "SELECT id, doc FROM (SELECT id, doc FROM n1 WHERE id IN (4, 7, 9) ORDER BY id LIMIT 3) s ORDER BY id",
    )
    .await
    .unwrap();
    let got: Vec<_> = rows.into_iter().map(|(id, t)| (id, t.unwrap())).collect();
    assert_eq!(got, vec![(4, pg_document(4)), (7, pg_document(7)), (9, pg_document(9))]);
}

/// Numbers whose positional text is long are stored in a short exponent
/// form and written out only when printed, as PostgreSQL does: it stores
/// `1e131071` in 22 bytes. Each must still print, compare and index as the
/// value it is, and two spellings of one value must still be one document.
/// Every expected answer is PostgreSQL 17's for the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn long_numbers_are_stored_short_and_read_back_whole() {
    let zeros = |n: usize| "0".repeat(n);
    let e60 = format!("1{}", zeros(60));
    let docs = [
        (1, r#"{"a":1e60}"#.to_string()),
        (2, format!(r#"{{"a":{e60}}}"#)),
        (3, r#"{"a":1.000e-45}"#.to_string()),
        (4, r#"{"a":0e-45}"#.to_string()),
        (5, r#"{"a":-3.10e-30}"#.to_string()),
        (6, "[1e131071, 0e-16383]".to_string()),
    ];
    let printed = |id: i64| match id {
        1 | 2 => format!(r#"{{"a": {e60}}}"#),
        3 => format!(r#"{{"a": 0.{}1000}}"#, zeros(44)),
        4 => format!(r#"{{"a": 0.{}}}"#, zeros(45)),
        5 => format!(r#"{{"a": -0.{}310}}"#, zeros(29)),
        _ => format!("[1{}, 0.{}]", zeros(131_071), zeros(16_383)),
    };
    let cases: Vec<(String, Vec<i64>)> = vec![
        (r#"doc @> '{"a":1e60}'"#.into(), vec![1, 2]),
        (r#"doc @> '{"a":10e59}'"#.into(), vec![1, 2]),
        (r#"doc = '{"a":1e60}'"#.into(), vec![1, 2]),
        (format!("doc->>'a' = '{e60}'"), vec![1, 2]),
        (r#"doc @> '{"a":1.0e-45}'"#.into(), vec![3]),
        (format!("doc->>'a' LIKE '0.{}1000'", zeros(44)), vec![3]),
        (r#"doc @> '{"a":0}'"#.into(), vec![4]),
        (format!("doc->>'a' = '0.{}'", zeros(45)), vec![4]),
        (format!("doc->>'a' = '-0.{}310'", zeros(29)), vec![5]),
        ("doc @> '[1e131071]'".into(), vec![6]),
        ("doc @> '[0]'".into(), vec![6]),
        ("doc @> '[1e131070]'".into(), vec![]),
    ];
    for indexed in [false, true] {
        let server = TestServer::start().await.expect("server should start");
        let pool = server.connect_sqlx().await.expect("should connect");
        pool.execute(sqlx::raw_sql("CREATE TABLE n1 (id BIGINT PRIMARY KEY, doc JSONB)"))
            .await
            .expect("create");
        for (id, doc) in &docs {
            pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO n1 VALUES ({id}, '{doc}')"))))
                .await
                .unwrap_or_else(|e| panic!("seed {id}: {e}"));
        }
        if indexed {
            for ddl in [
                "CREATE INDEX n1_gin ON n1 USING gin (doc)",
                "CREATE INDEX n1_path ON n1 ((doc->>'a'))",
            ] {
                pool.execute(sqlx::raw_sql(ddl)).await.expect(ddl);
            }
        }
        let mut wrong = Vec::new();
        for (predicate, want) in &cases {
            match ids(&pool, predicate).await {
                Ok(got) if got == *want => {}
                Ok(got) => wrong.push(format!("{predicate}: got {got:?}, PostgreSQL {want:?}")),
                Err(e) => wrong.push(format!("{predicate}: {e}")),
            }
        }
        for (id, got) in texts(&pool, "SELECT id, doc::text FROM n1 ORDER BY id").await.unwrap() {
            if got.as_deref() != Some(printed(id).as_str()) {
                let head = got.map(|t| t.chars().take(80).collect::<String>());
                wrong.push(format!("doc::text of {id}: got {head:?}..."));
            }
        }
        // PostgreSQL: 131072 and 16385 characters, and a document of 147461
        // after a derived table writes it back as a literal.
        let rows = texts(&pool, "SELECT id, doc->>0 FROM n1 WHERE id = 6").await.unwrap();
        assert_eq!(rows[0].1.as_deref().map(str::len), Some(131_072));
        let rows = texts(&pool, "SELECT id, doc->>1 FROM n1 WHERE id = 6").await.unwrap();
        assert_eq!(rows[0].1.as_deref().map(str::len), Some(16_385));
        let rows = texts(
            &pool,
            "SELECT id, doc FROM (SELECT id, doc FROM n1 WHERE id IN (1, 6) ORDER BY id LIMIT 2) s ORDER BY id",
        )
        .await
        .unwrap();
        let got: Vec<_> = rows.into_iter().map(|(id, t)| (id, t.map(|t| t.len()))).collect();
        assert_eq!(got, vec![(1, Some(68)), (6, Some(147_461))]);
        assert!(wrong.is_empty(), "indexed={indexed}: {} wrong:\n{}", wrong.len(), wrong.join("\n"));
    }
}
