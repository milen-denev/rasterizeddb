//! The result type of `CASE`, `COALESCE`, `NULLIF`, `GREATEST` and `LEAST`,
//! resolved as PostgreSQL's `select_common_type` does — checked by the OID the
//! row description announces *and* by the value decoded under it.
//!
//! An integer literal is `int4` (`int8` only when it does not fit); mixing
//! `int4` with `int8` gives `int8`, a numeric literal or column makes it
//! `numeric`, a float beats both, and quoted literals alone resolve to `text`.
//! `CASE … THEN 1` came out `int8`, so a client asking for `i32` refused it.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Column, Executor, Row, TypeInfo};
use std::str::FromStr;

#[derive(Debug, PartialEq)]
enum V {
    I4(Option<i32>),
    I8(Option<i64>),
    N(Option<Decimal>),
    F8(Option<f64>),
    T(Option<String>),
}

#[tokio::test(flavor = "multi_thread")]
async fn branches_resolve_to_one_postgres_type() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(
        "CREATE TABLE t (id INT NOT NULL, i INT, b BIGINT, f DOUBLE PRECISION, n NUMERIC(10,2), s TEXT)",
    )
    .await
    .unwrap();
    pool.execute(
        "INSERT INTO t (id, i, b, f, n, s) VALUES (1, 5, 7000000000, 2.5, 3.25, 'x'), (2, NULL, NULL, NULL, NULL, NULL)",
    )
    .await
    .unwrap();
    let d = |s: &str| Some(Decimal::from_str(s).unwrap());
    let cases: Vec<(&str, &str, Vec<V>)> = vec![
        ("CASE WHEN id = 1 THEN 1 ELSE 2 END", "INT4", vec![V::I4(Some(1)), V::I4(Some(2))]),
        ("CASE WHEN id = 1 THEN 1 END", "INT4", vec![V::I4(Some(1)), V::I4(None)]),
        ("CASE WHEN id = 1 THEN 1 ELSE 5000000000 END", "INT8", vec![V::I8(Some(1)), V::I8(Some(5_000_000_000))]),
        ("CASE WHEN id = 1 THEN i ELSE b END", "INT8", vec![V::I8(Some(5)), V::I8(None)]),
        ("CASE WHEN id = 1 THEN 1 ELSE 1.5 END", "NUMERIC", vec![V::N(d("1")), V::N(d("1.5"))]),
        ("CASE WHEN id = 1 THEN f ELSE 1 END", "FLOAT8", vec![V::F8(Some(2.5)), V::F8(Some(1.0))]),
        ("CASE WHEN id = 1 THEN 'a' ELSE 'b' END", "TEXT", vec![V::T(Some("a".into())), V::T(Some("b".into()))]),
        ("COALESCE(i, 0)", "INT4", vec![V::I4(Some(5)), V::I4(Some(0))]),
        ("COALESCE(b, 0)", "INT8", vec![V::I8(Some(7_000_000_000)), V::I8(Some(0))]),
        ("COALESCE(i, b)", "INT8", vec![V::I8(Some(5)), V::I8(None)]),
        ("COALESCE(n, 0)", "NUMERIC", vec![V::N(d("3.25")), V::N(d("0"))]),
        ("COALESCE(f, 0)", "FLOAT8", vec![V::F8(Some(2.5)), V::F8(Some(0.0))]),
        ("COALESCE(s, 'none')", "TEXT", vec![V::T(Some("x".into())), V::T(Some("none".into()))]),
        ("NULLIF(id, 2)", "INT4", vec![V::I4(Some(1)), V::I4(None)]),
        ("NULLIF(i, 5)", "INT4", vec![V::I4(None), V::I4(None)]),
        ("GREATEST(i, 10)", "INT4", vec![V::I4(Some(10)), V::I4(Some(10))]),
        ("LEAST(b, 1)", "INT8", vec![V::I8(Some(1)), V::I8(Some(1))]),
        ("GREATEST(i, 1.5)", "NUMERIC", vec![V::N(d("5")), V::N(d("1.5"))]),
    ];
    let mut wrong = Vec::new();
    for (expr, oid, want) in cases {
        let sql = format!("SELECT id, {expr} AS v FROM t ORDER BY id");
        let rows = match sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await {
            Ok(rows) => rows,
            Err(e) => {
                wrong.push(format!("{expr}: error {e}"));
                continue;
            }
        };
        let got_oid = rows[0].column(1).type_info().name().to_string();
        if got_oid != oid {
            wrong.push(format!("{expr}: announced {got_oid}, want {oid}"));
            continue;
        }
        let got: Vec<V> = rows
            .iter()
            .map(|r| match oid {
                "INT4" => V::I4(r.get(1)),
                "INT8" => V::I8(r.get(1)),
                "NUMERIC" => V::N(r.get::<Option<Decimal>, _>(1).map(|v| v.normalize())),
                "FLOAT8" => V::F8(r.get(1)),
                _ => V::T(r.get(1)),
            })
            .collect();
        let want: Vec<V> = want
            .into_iter()
            .map(|v| match v {
                V::N(x) => V::N(x.map(|v| v.normalize())),
                other => other,
            })
            .collect();
        if got != want {
            wrong.push(format!("{expr}: got {got:?}, want {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
