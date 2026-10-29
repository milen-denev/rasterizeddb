//! NUMERIC constants with more places than the engine's nine, and NUMERIC
//! columns of different scales meeting in one result.
//!
//! A constant past nine places was carried as a float: `c + 43e-17` was the
//! float 44812.65 (described `float8`), `INSERT … SELECT c + 43e-17` stored
//! 44812.65000000000000000000, and a `CASE` with one such branch became a
//! float, so `SET t9 = CASE … ELSE n2 - 1.0000000001 END` stored
//! 67268942325258700 for 67268942325258704.12. A NUMERIC(38,20) copied by
//! `INSERT … SELECT` lost its places past nine; a `UNION` of NUMERIC(10,2)
//! and NUMERIC(12,4) read 3.1415 as 314.15; a CHECK comparing with a
//! twelve-place constant refused every row; a join comparing a NUMERIC(10,2)
//! with a NUMERIC(38,20) failed XX000. Every one but the last reported
//! success. The expected text is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Every row's cells as PostgreSQL's text, `|`-joined; NULL as `NULL`.
async fn text(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, String> {
    let messages = client.simple_query(sql).await.map_err(|e| {
        e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string())
    })?;
    Ok(messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len()).map(|i| row.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"),
            ),
            _ => None,
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn wide_constants_and_mixed_scales_are_postgres_own() {
    let dir = std::env::temp_dir().join(format!("numeric_wide_constants_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("root");
    let root = Root(dir.to_string_lossy().into_owned());
    let server = TestServer::start_file_backed(&root.0).await.expect("server");
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE r (id BIGINT PRIMARY KEY, c NUMERIC(10,2), b NUMERIC(38,20), i BIGINT, \
                 n2 NUMERIC(20,2), t9 NUMERIC(27,9), d NUMERIC(12,4)); \
             INSERT INTO r VALUES (1, 44812.65, -613300910, 3, 67268942325258703.12, NULL, 3.1415); \
             INSERT INTO r VALUES (2, -2.55, 1.12345678912345678912, 7, 1234567890123456.78, NULL, -0.0001); \
             CREATE TABLE r2 (id BIGINT PRIMARY KEY, z NUMERIC(38,20)); \
             CREATE TABLE ck (id BIGINT PRIMARY KEY, a NUMERIC(20,12) CHECK (a <= 1.123456789123)); \
             CREATE TABLE jb (id BIGINT PRIMARY KEY, a NUMERIC(38,20)); \
             INSERT INTO jb VALUES (1, 1.12345678912345678912), (2, -0.00000000000000000005); \
             CREATE TABLE u2 (id BIGINT PRIMARY KEY, c NUMERIC(10,2), d NUMERIC(12,4)); \
             INSERT INTO u2 VALUES (1, 1.25, 1.2500)",
        )
        .await
        .expect("setup");

    let mut wrong: Vec<String> = Vec::new();
    let mut expect = |sql: &str, got: Result<Vec<String>, String>, want: &[&str]| {
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        if got.as_ref() != Ok(&want) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}"));
        }
    };
    for (sql, want) in [
        // Constants past nine places, written either way.
        ("SELECT c + 43e-17 FROM r WHERE id = 1", &["44812.65000000000000043"][..]),
        ("SELECT c + 0.00000000000000000043 FROM r WHERE id = 1", &["44812.65000000000000000043"]),
        ("SELECT c + 1e-12 FROM r WHERE id = 1", &["44812.650000000001"]),
        ("SELECT b - 67e-18 FROM r WHERE id = 1", &["-613300910.00000000000000006700"]),
        ("SELECT i * 1.5e-11 FROM r WHERE id = 1", &["0.000000000045"]),
        ("SELECT id FROM r ORDER BY c + 43e-17 LIMIT 1", &["2"]),
        // A CASE with one such branch is NUMERIC, not a float.
        ("SELECT CASE WHEN i > 0 THEN n2 + 1 ELSE n2 - 1.0000000001 END FROM r ORDER BY id", &[
            "67268942325258704.12",
            "1234567890123457.78",
        ]),
        ("SELECT greatest(n2, 0.0000000001), least(c, 0.0000000001) FROM r WHERE id = 1", &[
            "67268942325258703.12|0.0000000001",
        ]),
        // Stored, then read back.
        ("UPDATE r SET t9 = CASE WHEN i > 0 THEN n2 + 1 ELSE n2 - 1.0000000001 END WHERE id = 1 RETURNING t9", &[
            "67268942325258704.120000000",
        ]),
        ("UPDATE r SET b = b - 67e-18 WHERE id = 1 RETURNING b, b - 67e-18", &[
            "-613300910.00000000000000006700|-613300910.00000000000000013400",
        ]),
        ("INSERT INTO r2 SELECT id, c + 43e-17 FROM r WHERE id = 1 RETURNING z", &["44812.65000000000000043000"]),
        // A wide column copied by INSERT … SELECT, from each kind of source.
        ("INSERT INTO r2 SELECT id + 10, b FROM r WHERE id = 2", &[]),
        ("INSERT INTO r2 SELECT x.id + 20, y.a FROM r x JOIN jb y ON y.id = x.id WHERE x.id = 2", &[]),
        ("INSERT INTO r2 SELECT s.id + 30, s.b FROM (SELECT id, b FROM r) s WHERE s.id = 2", &[]),
        ("INSERT INTO r2 WITH w AS (SELECT id, b FROM r) SELECT id + 40, b FROM w WHERE id = 2", &[]),
        ("INSERT INTO r2 SELECT 50, min(a) FROM jb", &[]),
        ("SELECT id, z FROM r2 ORDER BY id", &[
            "1|44812.65000000000000043000",
            "12|1.12345678912345678912",
            "22|-0.00000000000000000005",
            "32|1.12345678912345678912",
            "42|1.12345678912345678912",
            "50|-0.00000000000000000005",
        ]),
        // A UNION of NUMERICs at different scales, and of a NUMERIC and an
        // integer: each value at its own scale.
        ("SELECT c FROM r UNION ALL SELECT d FROM r ORDER BY 1", &["-2.55", "-0.0001", "3.1415", "44812.65"]),
        ("SELECT d FROM r UNION ALL SELECT c FROM r ORDER BY 1 DESC", &["44812.65", "3.1415", "-0.0001", "-2.55"]),
        ("SELECT c FROM r UNION ALL SELECT i FROM r ORDER BY 1", &["-2.55", "3", "7", "44812.65"]),
        ("SELECT c FROM r UNION SELECT d FROM r ORDER BY 1", &["-2.55", "-0.0001", "3.1415", "44812.65"]),
        ("SELECT c FROM u2 UNION SELECT d FROM u2", &["1.25"]),
        ("SELECT count(*) FROM (SELECT c AS x FROM r UNION ALL SELECT d FROM r) s WHERE x < 0", &["2"]),
        // Compared with a constant or a column of another scale.
        ("INSERT INTO ck VALUES (1, 1.123456789123)", &[]),
        ("INSERT INTO ck VALUES (3, 1)", &[]),
        ("SELECT id FROM ck WHERE a > 1.12345678912 ORDER BY id", &["1"]),
        ("SELECT r.id FROM r JOIN jb ON jb.id = r.id WHERE r.c > jb.a ORDER BY r.id", &["1"]),
        ("SELECT id FROM jb WHERE a > 1.123456789123", &["1"]),
        // A constant past the column's places, rounded on the way in.
        ("INSERT INTO r2 VALUES (60, 1.25E-19), (61, 1.25e-21) RETURNING z", &[
            "0.00000000000000000013",
            "0.00000000000000000000",
        ]),
    ] {
        let got = text(&client, sql).await;
        expect(sql, got, want);
    }
    // Refused, never answered without the places: a CHECK's violation, an
    // integer first branch over a NUMERIC one, and an aggregate's argument.
    for (sql, code) in [
        ("INSERT INTO ck VALUES (2, 1.123456789124)", "23514"),
        ("SELECT i FROM r UNION ALL SELECT c FROM r", "0A000"),
        ("SELECT sum(c + 43e-17) FROM r", "0A000"),
    ] {
        let got = text(&client, sql).await;
        if got != Err(code.to_string()) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want Err({code})"));
        }
    }
    // Described as PostgreSQL describes it: `numeric`.
    for sql in ["SELECT c + 43e-17 FROM r", "SELECT CASE WHEN i > 0 THEN n2 ELSE 0.0000000001 END FROM r"] {
        match client.prepare(sql).await {
            Ok(statement) if statement.columns()[0].type_() == &tokio_postgres::types::Type::NUMERIC => {}
            other => wrong.push(format!("{sql}: described {:?}", other.map(|s| s.columns()[0].type_().clone()))),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
