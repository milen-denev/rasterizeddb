//! The PostgreSQL 17 differential round of 2026-11-09 (`.claude/reviews/pgdiff`):
//! random statements run on this engine and on `postgres:17`, results
//! compared. Each case below answered differently before its fix, most of
//! them silently: a boolean cast to text was '1', `'x' || true` was 'x',
//! `c::numeric(12,3) > 1` sent the number, `avg(numeric) OVER ()` had sixteen
//! places, `'-1' || doc->>'t'` concatenated where `||` and `->>` group left to
//! right, `(x::int)::numeric / 3` had nine places, `mod()` refused a numeric,
//! `'-5' < 3` was false, `sum(interval)` was NULL, an interval's epoch took a
//! year as 360 days and its text dropped the `+` after a negative part, and
//! `NOT (s IN ('日本', …))` panicked mid-character. Others were refused:
//! `id = a - 3` over a nullable `a`, `IS DISTINCT FROM` in a select list,
//! `LIKE` and `ANY (ARRAY[…])` as values, `CASE x WHEN`, a boolean GROUP BY,
//! `ntile`, `lead`'s default, an outer join's WHERE on the padded side, a
//! non-equality ON, `smallint - 7 / -4`, a constant beside an aggregate.
//! Every expected answer is PostgreSQL 17's, read from it beside this engine.

use rasterizeddb_compat::TestServer;
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

async fn server() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        r#"CREATE TABLE p1 (id INT PRIMARY KEY, a INT, sm SMALLINT, c NUMERIC(10,2), d DOUBLE PRECISION, s TEXT, f BOOLEAN, j JSONB, iv INTERVAL)"#,
        r#"INSERT INTO p1 VALUES (1, 5, 32767, 1.25, 2.5, 'abc', true, '{"k": 2, "t": "x"}', '1 day 03:00:00'), (2, NULL, -3, 7.50, NULL, 'Äpfel', false, '{"k": 1.5}', '2 years 3 mons'), (3, -2, 0, NULL, -1, NULL, NULL, '[1, "two"]', '-5 days 01:00:00'), (4, 3, 100, 0.01, 0.25, '日本', true, NULL, NULL)"#,
        r#"CREATE TABLE p2 (id INT PRIMARY KEY, p1_id INT, n INT)"#,
        r#"INSERT INTO p2 VALUES (1, 1, 4), (2, 1, NULL), (3, 2, -1), (4, 9, 7)"#,
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

#[tokio::test]
async fn statements_answer_as_postgresql_17_does() {
    let (_s, c) = server().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id, f::text, 'x' || f, length(f::text) FROM p1 ORDER BY id"#, &[r#"1|true|xtrue|4"#, r#"2|false|xfalse|5"#, r#"3|NULL|NULL|NULL"#, r#"4|true|xtrue|4"#]),
        (r#"SELECT ' 7 '::int, '0x1A'::int, '1_000'::int, 2.5::int, (-2.5)::int, '2.5'::numeric::int"#, &[r#"7|26|1000|3|-3|3"#]),
        (r#"SELECT id, c::numeric(12,3) > 1, 0 / -4::numeric(12,3) <> 1.5 FROM p1 ORDER BY id"#, &[r#"1|t|t"#, r#"2|t|t"#, r#"3|NULL|t"#, r#"4|f|t"#]),
        (r#"SELECT id FROM p1 WHERE id = a - 3 ORDER BY id"#, &[]),
        (r#"SELECT id FROM p1 WHERE id < a + 3 ORDER BY id"#, &[r#"1"#, r#"4"#]),
        (r#"SELECT id, sm IS DISTINCT FROM a, a IS NOT DISTINCT FROM NULL FROM p1 ORDER BY id"#, &[r#"1|t|f"#, r#"2|t|t"#, r#"3|t|f"#, r#"4|t|f"#]),
        (r#"SELECT id, avg(c) OVER () FROM p1 ORDER BY id"#, &[r#"1|2.9200000000000000"#, r#"2|2.9200000000000000"#, r#"3|2.9200000000000000"#, r#"4|2.9200000000000000"#]),
        (r#"SELECT id, '-1' || j->>'t' FROM p1 ORDER BY id"#, &[r#"1|NULL"#, r#"2|NULL"#, r#"3|NULL"#, r#"4|NULL"#]),
        (r#"SELECT id, (x::int)::numeric / 3 FROM (SELECT id, a AS x FROM p1) s ORDER BY id"#, &[r#"1|1.6666666666666667"#, r#"2|NULL"#, r#"3|-0.66666666666666666667"#, r#"4|1.00000000000000000000"#]),
        (r#"SELECT id, s LIKE '_bc', s ILIKE 'ä%', s NOT LIKE '%\%%' FROM p1 ORDER BY id"#, &[r#"1|t|f|t"#, r#"2|f|t|t"#, r#"3|NULL|NULL|NULL"#, r#"4|f|f|t"#]),
        (r#"SELECT id, a = ANY(ARRAY[5, NULL]), a > ALL(ARRAY[0, 4]) FROM p1 ORDER BY id"#, &[r#"1|t|t"#, r#"2|NULL|NULL"#, r#"3|NULL|f"#, r#"4|NULL|f"#]),
        (r#"SELECT count(*) FROM p1 WHERE a = ANY(ARRAY[5, 3])"#, &[r#"2"#]),
        (r#"SELECT count(*) FROM p1 WHERE (s LIKE 'a%') IS NOT TRUE"#, &[r#"3"#]),
        (r#"SELECT id, CASE a WHEN 5 THEN 'five' WHEN 3 THEN 'three' ELSE 'other' END FROM p1 ORDER BY id"#, &[r#"1|five"#, r#"2|other"#, r#"3|other"#, r#"4|three"#]),
        (r#"SELECT sm > 0, count(*) FROM p1 GROUP BY sm > 0 ORDER BY 1"#, &[r#"f|2"#, r#"t|2"#]),
        (r#"SELECT id, ntile(3) OVER (ORDER BY id), lead(a, 2, -99) OVER (ORDER BY id) FROM p1 ORDER BY id"#, &[r#"1|1|-2"#, r#"2|1|3"#, r#"3|2|-99"#, r#"4|3|-99"#]),
        (r#"SELECT width_bucket(5, 0, 10, 4), width_bucket(2.5, 10, 0, 4), factorial(5)"#, &[r#"3|4|120"#]),
        (r#"SELECT id, mod(c, 3) FROM p1 ORDER BY id"#, &[r#"1|1.25"#, r#"2|1.50"#, r#"3|NULL"#, r#"4|0.01"#]),
        (r#"SELECT count(*) FROM p1 WHERE mod(c, 3) > 0.5"#, &[r#"2"#]),
        (r#"SELECT count(*) FROM p1 WHERE sm - 7 / -4 > 0"#, &[r#"3"#]),
        (r#"SELECT count(*) FROM p1 WHERE c = a OR c < a"#, &[r#"2"#]),
        (r#"SELECT count(*) FROM p1 WHERE '-5' < 3"#, &[r#"4"#]),
        (r#"SELECT count(*) FROM p1 WHERE ('-1' >= 10) IS NOT TRUE"#, &[r#"4"#]),
        (r#"SELECT count(*) FROM p1 WHERE a + 1 > '5'"#, &[r#"1"#]),
        (r#"SELECT count(*) FROM p1 WHERE -sm < a"#, &[r#"2"#]),
        (r#"SELECT id, sum(iv) OVER (ORDER BY id), iv * 2, iv / 3, -iv, extract(epoch FROM iv) FROM p1 ORDER BY id"#, &[r#"1|1 day 03:00:00|2 days 06:00:00|09:00:00|-1 days -03:00:00|97200.000000"#, r#"2|2 years 3 mons 1 day 03:00:00|4 years 6 mons|9 mons|-2 years -3 mons|70891200.000000"#, r#"3|2 years 3 mons -4 days +04:00:00|-10 days +02:00:00|-1 days -15:40:00|5 days -01:00:00|-428400.000000"#, r#"4|2 years 3 mons -4 days +04:00:00|NULL|NULL|NULL|NULL"#]),
        (r#"SELECT sum(iv), avg(iv), justify_hours(interval '50 hours'), justify_interval(interval '-1 mon 40 days') FROM p1"#, &[r#"2 years 3 mons -4 days +04:00:00|9 mons -1 days -06:40:00|2 days 02:00:00|10 days"#]),
        (r#"SELECT p1.id, p2.id FROM p1 LEFT JOIN p2 ON p1.id = p2.p1_id WHERE p2.n > 0 ORDER BY 1, 2"#, &[r#"1|1"#]),
        (r#"SELECT p1.id, p2.id FROM p1 LEFT JOIN p2 ON p1.id = p2.p1_id WHERE p2.n IS NULL ORDER BY 1, 2"#, &[r#"1|2"#, r#"3|NULL"#, r#"4|NULL"#]),
        (r#"SELECT a.id, b.id FROM p1 a JOIN p1 b ON a.f = b.f AND a.id < b.id ORDER BY 1, 2"#, &[r#"1|4"#]),
        (r#"SELECT n FROM p2 EXCEPT SELECT a FROM p1 WHERE f ORDER BY 1"#, &[r#"-1"#, r#"4"#, r#"7"#, r#"NULL"#]),
        (r#"SELECT id, NOT (s IN ('日本', 'émile')) FROM p1 ORDER BY id"#, &[r#"1|t"#, r#"2|t"#, r#"3|NULL"#, r#"4|f"#]),
        (r#"SELECT sum(a), upper('x'), 1 + 2 FROM p1"#, &[r#"6|X|3"#]),
    ];
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        let got = rows(&c, sql).await;
        if got != *want {
            wrong.push(format!("{sql}\n  want {want:?}\n  got  {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// What PostgreSQL refuses, refused: a fraction is not integer input, and an
/// integer has no `~~`, nor text a `->>`.
#[tokio::test]
async fn what_postgresql_refuses_is_refused() {
    let (_s, c) = server().await;
    for (sql, code) in [
        ("SELECT '1.5'::int", "22P02"),
        ("SELECT count(*) FROM p1 WHERE '2.5' > 2", "22P02"),
        ("SELECT count(*) FROM p1 WHERE (a LIKE 'x%') IS FALSE", "42883"),
        ("SELECT s || j->>'t' FROM p1", "42883"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!(db.code().code(), code, "{sql}: {}", db.message());
    }
}

/// Through the extended protocol, as a driver sends it: a select list with a
/// scalar subquery is described (it answered NoData, then sent rows), and an
/// untyped parameter beside a text call or another unknown is text.
#[tokio::test]
async fn bound_statements_are_described_and_typed_as_postgresql_does() {
    let (_s, c) = server().await;
    let sql = "SELECT id, (SELECT max(n) FROM p2) AS mx FROM p1 WHERE id < $1 ORDER BY id";
    let st = c.prepare(sql).await.expect("prepare");
    let types: Vec<_> = st.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(types, vec![tokio_postgres::types::Type::INT4, tokio_postgres::types::Type::INT4], "max(int) is an integer");
    let got = c
        .query(sql, &[&3i32])
        .await
        .expect("a scalar subquery in the select list is described");
    let pairs: Vec<(i32, i32)> = got.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(pairs, vec![(1, 7), (2, 7)]);
    let st = c.prepare("SELECT count(*) FROM p1 WHERE md5(s) = $1").await.expect("prepare");
    assert_eq!(st.params(), &[tokio_postgres::types::Type::TEXT], "beside md5(), text");
    let n: i64 = c.query_one(&st, &[&"9"]).await.expect("md5(s) = $1").get(0);
    assert_eq!(n, 0);
    let st = c.prepare("SELECT count(*) FROM p1 WHERE 'Abc' >= $1").await.expect("prepare");
    assert_eq!(st.params(), &[tokio_postgres::types::Type::TEXT], "beside an unknown, text");
    let n: i64 = c.query_one(&st, &[&"-1"]).await.expect("'Abc' >= $1").get(0);
    assert_eq!(n, 4, "'Abc' >= '-1' compares text and holds for every row");
    // Round 12: a text call in parentheses, a concatenation, and format(),
    // each beside a parameter; it was described as an integer, and the
    // statement refused as `text < integer`.
    for (sql, value, want) in [
        ("SELECT count(*) FROM p1 WHERE (lower(s)) < $1", "b", 1i64),
        ("SELECT count(*) FROM p1 WHERE (s || id) >= $1", "abc1", 3),
        ("SELECT count(*) FROM p1 WHERE (format('%s|%s', s, id)) <> $1", "1", 4),
    ] {
        let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(st.params(), &[tokio_postgres::types::Type::TEXT], "{sql}");
        let n: i64 = c.query_one(&st, &[&value]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).get(0);
        assert_eq!(n, want, "{sql}");
    }
}


/// Round 12 (2026-11-09, `gen3.py`: the function surface). The one silent
/// wrong answer was `timestamp - timestamp` reported in hours (`8121:57:11`)
/// where PostgreSQL justifies whole days out of it (`338 days 08:27:11`),
/// which `extract(day …)` and `extract(hour …)` read. Refused before:
/// `array_agg(DISTINCT x % 3 ORDER BY x % 3)` (the key compared as text
/// with its parentheses), a bare boolean aggregate in HAVING, `DISTINCT ON
/// (a > 0) … ORDER BY a > 0`, and `IS [NOT] DISTINCT FROM` with an
/// expression on the left, an interval comparison in WHERE. Lifting the
/// first then showed `array_agg(DISTINCT …)` dropping its NULL, a column's
/// as much as an expression's. Every expected answer is PostgreSQL 17's.
#[tokio::test]
async fn round_12_answers_as_postgresql_17_does() {
    let (_s, c) = server().await;
    for sql in [
        "CREATE TABLE p3 (id INT PRIMARY KEY, a INT, sm SMALLINT, c NUMERIC(10,2), s TEXT, f BOOLEAN, ts TIMESTAMP)",
        "INSERT INTO p3 VALUES (1, 5, 4, 1.25, 'abc', true, '2024-12-04 08:27:11'), (2, NULL, -3, 7.50, 'b', false, '2000-06-20 05:43:46'), (3, -2, 7, NULL, NULL, NULL, '2024-01-01 10:00:00'), (4, 3, 4, 0.01, 'a', true, NULL), (5, 3, 5, 2.00, 'b', NULL, '2023-12-31 23:00:00')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, ts - TIMESTAMP '2024-01-01' FROM p3 ORDER BY id", &["1|338 days 08:27:11", "2|-8594 days -18:16:14", "3|10:00:00", "4|NULL", "5|-01:00:00"]),
        ("SELECT id FROM p3 WHERE ts - TIMESTAMP '2024-01-01' > interval '30 days' ORDER BY id", &["1"]),
        ("SELECT id, extract(day from ts - TIMESTAMP '2024-01-01'), extract(hour from ts - TIMESTAMP '2024-01-01') FROM p3 ORDER BY id", &["1|338|8", "2|-8594|-18", "3|0|10", "4|NULL|NULL", "5|0|-1"]),
        ("SELECT array_agg(DISTINCT sm % 3 ORDER BY sm % 3) FROM p3", &["{0,1,2}"]),
        ("SELECT f, string_agg(DISTINCT upper(s), ',' ORDER BY upper(s)) FROM p3 GROUP BY f ORDER BY f", &["f|B", "t|A,ABC", "NULL|B"]),
        ("SELECT a > 0 AS k, count(*) FROM p3 GROUP BY 1 HAVING bool_or(f) ORDER BY 1", &["t|3"]),
        ("SELECT sm % 3 AS k, count(*) FROM p3 GROUP BY 1 HAVING bool_and(f) OR count(*) > 1 ORDER BY 1", &["1|3"]),
        ("SELECT DISTINCT ON (a > 0) a > 0 AS k, id FROM p3 ORDER BY a > 0, id DESC", &["f|3", "t|5", "NULL|2"]),
        ("SELECT id FROM p3 WHERE (rtrim(s)) IS NOT DISTINCT FROM 'a' ORDER BY id", &["4"]),
        ("SELECT id FROM p3 WHERE (c / 3) IS DISTINCT FROM c ORDER BY id", &["1", "2", "4", "5"]),
        ("SELECT id FROM p3 WHERE upper(s) IS NOT DISTINCT FROM NULL ORDER BY id", &["3"]),
        // A DISTINCT array_agg or json_agg keeps one NULL, last ascending
        // and first descending; string_agg and count skip it.
        ("SELECT array_agg(DISTINCT a), array_agg(DISTINCT a ORDER BY a DESC), string_agg(DISTINCT s, ',' ORDER BY s), count(DISTINCT a) FROM p3", &["{-2,3,5,NULL}|{NULL,5,3,-2}|a,abc,b|3"]),
        ("SELECT f, array_agg(DISTINCT sm % 3 ORDER BY sm % 3) FROM p3 GROUP BY f ORDER BY f", &["f|{0}", "t|{1}", "NULL|{1,2}"]),
        ("SELECT json_agg(DISTINCT f) FROM p3", &["[false, true, null]"]),
    ];
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        match c.simple_query(sql).await {
            Ok(_) => {
                let got = rows(&c, sql).await;
                if got != *want {
                    wrong.push(format!("{sql}\n  want {want:?}\n  got  {got:?}"));
                }
            }
            Err(e) => wrong.push(format!("{sql}\n  want {want:?}\n  error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} of {} differ from PostgreSQL 17:\n{}", wrong.len(), cases.len(), wrong.join("\n"));
}

/// Round 15 (writes, gen4.py): a second assignment to one column is
/// PostgreSQL's 42601, in every form of UPDATE. The first used to be written
/// and the second dropped: `SET a = a - 2, a = a - 5` stored `a - 2`.
#[tokio::test]
async fn a_second_assignment_to_one_column_is_refused() {
    let (_s, c) = server().await;
    for sql in [
        "UPDATE p1 SET a = a - 2, a = a - 5 WHERE id = 1",
        "UPDATE p1 SET a = 1, s = 'x', a = 2 WHERE id = 1",
        "UPDATE p1 SET a = p2.n, a = 1 FROM p2 WHERE p2.p1_id = p1.id",
        "INSERT INTO p1 (id, a) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET a = 7, a = 8",
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!(db.code().code(), "42601", "{sql}: {}", db.message());
    }
    assert_eq!(rows(&c, "SELECT a, s FROM p1 WHERE id = 1").await, ["5|abc"]);
}

/// Round 16 (gen5.py: windows, dates and times, jsonb, arrays). A template
/// pattern `to_char` did not know was copied as text: `WW` came back as `WW`,
/// `J` as `J`, `IYYY` as `I` and a 3-digit year, and every all-lower-case
/// pattern as itself, so `to_date('2024-05-06', 'yyyy-mm-dd')` was
/// 0001-01-01. A boolean in an array printed `true`, where PostgreSQL
/// prints `t`. Expected values are PostgreSQL 17's.
#[tokio::test]
async fn round_16_answers_as_postgresql_17_does() {
    let (_s, c) = server().await;
    for (sql, want) in [
        (
            "SELECT to_char(TIMESTAMP '2020-12-31 23:59:59.123456', 'IYYY-IW ID IDDD J WW W CC Y,YYY FF3 SSSS RM DDth')",
            "2020-53 4 368 2459215 53 5 21 2,020 123 86399 XII  31st",
        ),
        ("SELECT to_char(DATE '2021-01-03', 'iyyy-iw yyyy-mm-dd')", "2020-53 2021-01-03"),
        ("SELECT to_char(DATE '2024-02-29', 'TMMonth|FMMonth|Month|Dy|FMDDth')", "February|February|February |Thu|29th"),
        ("SELECT to_date('2024-05-06', 'yyyy-mm-dd')", "2024-05-06"),
        ("SELECT to_timestamp('2024-05-06 01:14:15 pm', 'yyyy-mm-dd hh12:mi:ss am')::timestamp", "2024-05-06 13:14:15"),
        ("SELECT array_agg(f ORDER BY id) FROM p1", "{t,f,NULL,t}"),
        ("SELECT array_agg(DISTINCT f) FROM p1", "{f,t,NULL}"),
    ] {
        assert_eq!(rows(&c, sql).await, [want], "{sql}");
    }
}
