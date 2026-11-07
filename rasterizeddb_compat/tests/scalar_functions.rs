//! PostgreSQL's everyday scalar functions (`rql::pg_functions`) and the
//! regular-expression operators: each was "function '…' is not supported
//! yet" (or, for `~`, "unexpected character"), though `left`, `substring`,
//! `trim`, `concat`, `floor`, `to_char` and `json_build_object` are in most
//! applications' queries. Every expected answer is PostgreSQL 17's
//! (`.claude/reviews/fn/p3.sql`, `p4.sql`).

use rasterizeddb_compat::TestServer;
use sqlx::{Connection, Row};
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

async fn server_with_rows() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, s TEXT, n NUMERIC(8,3), f DOUBLE PRECISION, d DATE, ts TIMESTAMP)",
        "INSERT INTO t VALUES (1, 'Hello World', 12.345, -2.5, '2024-02-29', '2024-02-29 13:45:30.25')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

/// Each function, one value each, against one row.
#[tokio::test(flavor = "multi_thread")]
async fn each_function_answers_as_postgresql_does() {
    let (_server, c) = server_with_rows().await;
    let cases: &[(&str, &str)] = &[
        ("left(s, 3)", "Hel"),
        ("right(s, -2)", "llo World"),
        ("left(s, 0)", ""),
        ("left(NULL, 2)", "NULL"),
        ("substr(s, 0, 3)", "He"),
        ("substr(s, -1, 4)", "He"),
        ("substring(s from 2 for 3)", "ell"),
        ("substring(s for 4)", "Hell"),
        ("position('o' in s)", "5"),
        ("strpos(s, 'World')", "7"),
        ("reverse(s)", "dlroW olleH"),
        ("repeat('ab', 3)", "ababab"),
        ("trim(both 'x' from 'xxaxx')", "a"),
        ("trim(leading 'x' from 'xxaxx')", "axx"),
        ("trim('  x  ')", "x"),
        ("concat('a', NULL, 'b', 1, 2.50, true)", "ab12.50t"),
        ("concat_ws('-', 'a', NULL, 'b', 3)", "a-b-3"),
        ("concat_ws(NULL, 'a')", "NULL"),
        ("split_part('a,b,c', ',', -1)", "c"),
        ("split_part('a,b,c', ',', 5)", ""),
        ("translate('abcabc', 'ab', 'x')", "xcxc"),
        ("ascii('A')", "65"),
        ("chr(233)", "é"),
        ("starts_with(s, 'He')", "t"),
        ("initcap('hello wORLD foo-bar 3rd')", "Hello World Foo-Bar 3rd"),
        ("md5(s)", "b10a8db164e0754105b7a99be72e3fe5"),
        ("to_hex(-1)", "ffffffff"),
        ("to_hex(9223372036854775807)", "7fffffffffffffff"),
        ("quote_literal('it''s')", "'it''s'"),
        ("format('%I.%L', 'my table', 'it''s')", "\"my table\".'it''s'"),
        ("format('%2$s %1$s', 'a', 'b')", "b a"),
        ("format('%s/%L', NULL, NULL)", "/NULL"),
        ("regexp_replace(s, 'o', '0', 'g')", "Hell0 W0rld"),
        ("regexp_replace(s, '(l+)', '[\\1]', 'g')", "He[ll]o Wor[l]d"),
        ("regexp_replace(s, 'O', '0', 'gi')", "Hell0 W0rld"),
        ("ceil(n)", "13"),
        ("ceil(f)", "-2"),
        ("floor(n)", "12"),
        ("floor(-0.5)", "-1"),
        ("sign(n)", "1"),
        ("sign(f)", "-1"),
        ("power(2, 10)", "1024"),
        ("sqrt(2.0)", "1.414213562373095"),
        ("sqrt(16)", "4"),
        ("ln(10)", "2.302585092994046"),
        ("log(100)", "2"),
        ("pi()", "3.141592653589793"),
        ("degrees(pi())", "180"),
        ("div(-7, 2)", "-3"),
        ("gcd(12, 18)", "6"),
        ("lcm(4, 6)", "12"),
        ("make_date(2024, 2, 29)", "2024-02-29"),
        ("make_timestamp(2024, 1, 2, 3, 4, 5.5)", "2024-01-02 03:04:05.5"),
        ("to_char(ts, 'YYYY-MM-DD HH24:MI:SS')", "2024-02-29 13:45:30"),
        ("to_char(ts, 'DD Mon YYYY HH12:MI AM')", "29 Feb 2024 01:45 PM"),
        ("to_char(ts, 'FMMonth FMDD, YYYY')", "February 29, 2024"),
        ("to_char(d, 'Dy DD/MM/YY')", "Thu 29/02/24"),
        ("to_char(n, 'FM999.00')", "12.35"),
        ("to_char(1234567.891, 'FM9,999,999.99')", "1,234,567.89"),
        ("to_char(42, '000')", " 042"),
        ("to_char(-5.5, 'FM990.0')", "-5.5"),
        ("to_date('06/05/2024', 'DD/MM/YYYY')", "2024-05-06"),
        ("to_timestamp('2024-05-06 07:08:09', 'YYYY-MM-DD HH24:MI:SS')::timestamp", "2024-05-06 07:08:09"),
        ("age(timestamp '2024-03-01', timestamp '2024-01-15 10:00')", "1 mon 16 days 14:00:00"),
        ("age(timestamp '2024-01-31', timestamp '2023-02-28')", "11 mons 3 days"),
        // The borrowed days are the earlier date's month's: February's 29.
        ("age(timestamp '2024-03-01', timestamp '2024-02-15')", "15 days"),
        ("age(timestamp '2024-02-15', timestamp '2024-03-01')", "-15 days"),
        ("encode('abc'::bytea, 'base64')", "YWJj"),
        ("decode('616263', 'hex')", "\\x616263"),
        ("jsonb_build_object('b', 2, 'a', n)", r#"{"a": 12.345, "b": 2}"#),
        ("json_build_array(1, 'x', NULL, 2.5)", r#"[1, "x", null, 2.5]"#),
        ("to_jsonb(d)", r#""2024-02-29""#),
        ("to_json(ts)", r#""2024-02-29T13:45:30.25""#),
        ("jsonb_array_length('[1,2,[3]]'::jsonb)", "3"),
        ("num_nonnulls(1, NULL, 'a')", "2"),
        ("string_to_array('a,b,,c', ',')", "{a,b,\"\",c}"),
        ("array_to_string(string_to_array('a,b,c', ','), '/')", "a/b/c"),
        ("s ~ '^H'", "t"),
        ("s !~* 'W'", "f"),
        ("pg_typeof(ceil(5))", "double precision"),
        ("pg_typeof(floor(n))", "numeric"),
        ("pg_typeof(gcd(12, 18))", "integer"),
        ("pg_typeof(to_timestamp(0))", "timestamp with time zone"),
    ];
    let mut wrong = Vec::new();
    for (expr, want) in cases {
        let sql = format!("SELECT {expr} FROM t");
        match c.simple_query(&sql).await {
            Ok(messages) => {
                let got: Vec<String> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("NULL").to_string()),
                        _ => None,
                    })
                    .collect();
                if got != [want.to_string()] {
                    wrong.push(format!("{expr}: got {got:?}, want {want:?}"));
                }
            }
            Err(e) => wrong.push(format!("{expr}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));

    // What PostgreSQL refuses, and what this engine refuses rather than
    // answer otherwise than PostgreSQL would.
    for (sql, sqlstate) in [
        ("SELECT split_part(s, ',', 0) FROM t", "22023"),
        ("SELECT substr(s, 1, -1) FROM t", "22011"),
        ("SELECT chr(0) FROM t", "54000"),
        ("SELECT power(n, 2) FROM t", "0A000"),
        ("SELECT regexp_replace(s, 'a|b', 'x') FROM t", "0A000"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
}

/// The functions wherever a query uses them: `WHERE` (the regular
/// expression operators too), `GROUP BY`, `ORDER BY`, a join, an aggregate's
/// argument, `HAVING`, `UPDATE … SET`, and over the extended protocol.
#[tokio::test(flavor = "multi_thread")]
async fn every_clause_reads_them() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, s TEXT, n NUMERIC(8,3), ts TIMESTAMP, g TEXT)",
        "INSERT INTO t VALUES (1, 'Hello World', 12.345, '2024-02-29 13:45:30', 'x'), (2, 'hey there', 2.5, \
         '2024-03-05 08:00:00', 'y'), (3, 'Goodbye', -1.25, '2023-12-31 23:59:59', 'x'), (4, NULL, NULL, NULL, 'y')",
        "CREATE TABLE u (id INT PRIMARY KEY, tid INT, tag TEXT)",
        "INSERT INTO u VALUES (1, 1, 'alpha'), (2, 1, 'beta'), (3, 2, 'gamma')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id FROM t WHERE lower(left(s, 2)) = 'he' ORDER BY id", &["1", "2"]),
        ("SELECT id FROM t WHERE to_char(ts, 'YYYY') = '2024' ORDER BY id", &["1", "2"]),
        ("SELECT id FROM t WHERE floor(n) > 2 ORDER BY id", &["1"]),
        ("SELECT id FROM t WHERE s ~* '^h' ORDER BY id", &["1", "2"]),
        ("SELECT id FROM t WHERE s !~ 'o' ORDER BY id", &["2"]),
        ("SELECT id FROM t WHERE starts_with(s, 'G') OR split_part(s, ' ', 2) = 'there' ORDER BY id", &["2", "3"]),
        (
            "SELECT to_char(ts, 'YYYY-MM') AS m, count(*) FROM t GROUP BY to_char(ts, 'YYYY-MM') ORDER BY m",
            &["2023-12|1", "2024-02|1", "2024-03|1", "NULL|1"],
        ),
        (
            "SELECT left(g, 1), count(*), string_agg(initcap(s), ', ' ORDER BY id) FROM t GROUP BY left(g, 1) ORDER BY 1",
            &["x|2|Hello World, Goodbye", "y|2|Hey There"],
        ),
        ("SELECT id, upper(left(s, 3)) FROM t ORDER BY length(s) DESC NULLS LAST, id", &["1|HEL", "2|HEY", "3|GOO", "4|NULL"]),
        (
            "SELECT t.id, upper(u.tag), concat(t.g, '-', u.tag) FROM t JOIN u ON u.tid = t.id ORDER BY u.id",
            &["1|ALPHA|x-alpha", "1|BETA|x-beta", "2|GAMMA|y-gamma"],
        ),
        ("SELECT t.id FROM t JOIN u ON u.tid = t.id WHERE u.tag ~ 'a$' ORDER BY u.id", &["1", "1", "2"]),
        ("SELECT g, sum(ceil(n)) FROM t GROUP BY g ORDER BY g", &["x|12", "y|3"]),
        ("SELECT g FROM t GROUP BY g HAVING max(length(s)) > 8 ORDER BY g", &["x", "y"]),
        ("SELECT coalesce(nullif(trim(s), ''), '-') FROM t ORDER BY id", &["Hello World", "hey there", "Goodbye", "-"]),
        ("UPDATE t SET s = initcap(s) WHERE id = 2 RETURNING s", &["Hey There"]),
        ("SELECT id, s ~ 'o' AS has_o, id > 2 AS big FROM t ORDER BY id", &["1|t|f", "2|f|f", "3|t|t", "4|NULL|t"]),
        ("DELETE FROM u WHERE tag ~ '^g' RETURNING id", &["3"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }

    let mut x = sqlx::PgConnection::connect(&server.url()).await.expect("sqlx");
    let row = sqlx::query("SELECT left(s, 3), strpos(s, 'o'), floor(n), s ~ 'H', make_date(2024, 1, 31) FROM t WHERE id = $1")
        .bind(1i32)
        .fetch_one(&mut x)
        .await
        .expect("extended");
    assert_eq!(row.get::<String, _>(0), "Hel");
    assert_eq!(row.get::<i32, _>(1), 5);
    assert_eq!(row.get::<rust_decimal::Decimal, _>(2).to_string(), "12");
    assert!(row.get::<bool, _>(3));
    assert_eq!(row.get::<chrono::NaiveDate, _>(4).to_string(), "2024-01-31");
}
