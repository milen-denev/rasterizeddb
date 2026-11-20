//! The range functions and operators, as PostgreSQL 17 answers each
//! (`.claude/reviews/pgdiff`).
//!
//! `lower(r)` and `upper(r)` lowercased the range's binary form as text and
//! answered an empty string; `'[5,1)'::int4range` stored `empty` where
//! PostgreSQL refuses it; one NULL range in a table failed every structural
//! test in a `WHERE`. The constructors, the bound tests, the set operators
//! and the positional ones were not read at all.

use rasterizeddb_compat::TestServer;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

/// The first row of `sql` as psql prints it (`|`-joined, NULL as `NULL`).
async fn row(c: &tokio_postgres::Client, sql: &str) -> String {
    let messages = c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = messages.iter().filter_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
        _ => None,
    });
    let r = rows.next().unwrap_or_else(|| panic!("{sql}: no row"));
    (0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|")
}

async fn ids(c: &tokio_postgres::Client, sql: &str) -> Vec<i32> {
    c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).iter().map(|r| r.get(0)).collect()
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let err = c.simple_query(sql).await.expect_err(sql);
    let db = err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}"));
    (db.code().code().to_string(), db.message().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn range_functions_answer_as_postgresql() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT lower('[1,5)'::int4range), upper('[1,5)'::int4range)", "1|5"),
        ("SELECT lower('(,5)'::int4range) IS NULL, upper('empty'::int4range) IS NULL", "t|t"),
        ("SELECT lower('[2024-01-01,2024-02-01)'::daterange), upper('[1.5,2.5)'::numrange)", "2024-01-01|2.5"),
        ("SELECT lower('[2024-01-01 10:00,2024-02-01)'::tsrange)", "2024-01-01 10:00:00"),
        ("SELECT lower('[3000000000,3000000005)'::int8range)", "3000000000"),
        (
            "SELECT isempty('[1,1)'::int4range), lower_inc('[1,5]'::int4range), upper_inc('[1,5]'::int4range), \
             lower_inf('(,5)'::int4range), upper_inf('[1,)'::int4range)",
            "t|t|f|t|t",
        ),
        ("SELECT int4range(1, 5), int4range(1, 5, '[]'), int4range(1, 5, '()'), int4range(NULL, 5)", "[1,5)|[1,6)|[2,5)|(,5)"),
        ("SELECT numrange(1.5, 2.5), numrange(1.5, 2.5, '(]')", "[1.5,2.5)|(1.5,2.5]"),
        ("SELECT daterange('2024-01-01', '2024-02-01', '[]')", "[2024-01-01,2024-02-02)"),
        ("SELECT tsrange('2024-01-01', '2024-01-02')", "[\"2024-01-01 00:00:00\",\"2024-01-02 00:00:00\")"),
        (
            "SELECT '[1,5)'::int4range * '[4,9)'::int4range, '[1,5)'::int4range + '[4,9)'::int4range, \
             '[1,5)'::int4range - '[4,9)'::int4range, '[1,5)'::int4range * '[6,9)'::int4range",
            "[4,5)|[1,9)|[1,4)|empty",
        ),
        ("SELECT '[1.5,2.5)'::numrange * '[2,3]'::numrange, '[1,3]'::numrange - '(2,4]'::numrange", "[2,2.5)|[1,2]"),
        ("SELECT range_merge('[1,2)'::int4range, '[5,6)'::int4range)", "[1,6)"),
        ("SELECT '[1,5)'::int4range << '[6,9)'::int4range, '[1,5)'::int4range >> '[6,9)'::int4range", "t|f"),
        ("SELECT '[1,5)'::int4range &< '[4,9)'::int4range, '[1,5)'::int4range &> '[4,9)'::int4range", "t|f"),
        (
            "SELECT '[1,5)'::int4range -|- '[5,9)'::int4range, '[1.5,2.5)'::numrange -|- '[2.5,3)'::numrange, \
             '[1.5,2.5]'::numrange -|- '[2.5,3)'::numrange",
            "t|t|f",
        ),
        ("SELECT '[5,5)'::int4range, '(5,5]'::int4range, '[5,5]'::int4range", "empty|empty|[5,6)"),
    ];
    for (sql, want) in cases {
        assert_eq!(row(&c, sql).await, *want, "{sql}");
    }
    for (sql, want_code, want_message) in [
        ("SELECT '[5,1)'::int4range", "22000", "range lower bound must be less than or equal to range upper bound"),
        ("SELECT int4range(5, 1)", "22000", "range lower bound must be less than or equal to range upper bound"),
        ("SELECT int4range(1, 5, 'xx')", "42601", "invalid range bound flags"),
        ("SELECT '[1,5)'::int4range + '[7,9)'::int4range", "22000", "result of range union would not be contiguous"),
        ("SELECT '[1,9)'::int4range - '[4,5)'::int4range", "22000", "result of range difference would not be contiguous"),
    ] {
        assert_eq!(code(&c, sql).await, (want_code.to_string(), want_message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_range_column_with_nulls_is_read_by_every_operator() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE rg (id int PRIMARY KEY, r int4range); \
         INSERT INTO rg VALUES (1, '[1,10)'), (2, '[20,30)'), (3, NULL), (4, 'empty')",
    )
    .await
    .expect("setup");
    let err = c.simple_query("INSERT INTO rg VALUES (5, '[9,2)')").await.expect_err("crossed");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "22000");

    for (sql, want) in [
        ("SELECT id FROM rg WHERE r @> 25 ORDER BY id", vec![2]),
        ("SELECT id FROM rg WHERE r && '[5,25)' ORDER BY id", vec![1, 2]),
        ("SELECT id FROM rg WHERE r <@ '[0,100)' ORDER BY id", vec![1, 2, 4]),
        ("SELECT id FROM rg WHERE r << '[15,16)' ORDER BY id", vec![1]),
        ("SELECT id FROM rg WHERE r >> '[15,16)' ORDER BY id", vec![2]),
        ("SELECT id FROM rg WHERE r &< '[15,16)' ORDER BY id", vec![1]),
        ("SELECT id FROM rg WHERE r &> '[15,16)' ORDER BY id", vec![2]),
        ("SELECT id FROM rg WHERE r -|- '[10,12)' ORDER BY id", vec![1]),
        // NULL is no match either way; an empty range is in no position.
        ("SELECT id FROM rg WHERE NOT (r << '[15,16)') ORDER BY id", vec![2, 4]),
        ("SELECT id FROM rg WHERE NOT (r @> 25) ORDER BY id", vec![1, 4]),
        ("SELECT id FROM rg WHERE upper(r) > 15 ORDER BY id", vec![2]),
    ] {
        assert_eq!(ids(&c, sql).await, want, "{sql}");
    }
    let rows = c
        .simple_query("SELECT id, lower(r), upper(r), isempty(r), lower_inf(r), r * int4range(5, 25) FROM rg ORDER BY id")
        .await
        .expect("bounds");
    let shown: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        shown,
        vec!["1|1|10|f|f|[5,10)", "2|20|30|f|f|[20,25)", "3|NULL|NULL|NULL|NULL|NULL", "4|NULL|NULL|t|f|empty"]
    );
}

/// A range the row computes, compared in a `WHERE`: refused before ("yields
/// a INT4RANGE value, which a WHERE clause cannot compare yet"), and `NOT
/// (range @> x)` read as a document ("invalid input syntax for type json").
/// Each answer is PostgreSQL 17's for the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_computed_range_is_compared_in_a_where_clause() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE rw (id int PRIMARY KEY, lo int, hi int, r int4range, d date); \
         INSERT INTO rw VALUES (1, 1, 10, '[1,10)', '2024-01-05'), (2, 20, 30, '[20,30)', '2024-02-01'), \
         (3, NULL, 5, NULL, NULL), (4, 5, 5, 'empty', '2024-03-01')",
    )
    .await
    .expect("setup");
    for (predicate, want) in [
        ("int4range(lo, hi) @> 5", vec![1]),
        ("r * '[5,25)'::int4range = '[5,10)'::int4range", vec![1]),
        ("int4range(lo, hi) && '[8,22)'", vec![1, 2]),
        ("r + '[10,20)'::int4range = '[1,20)'::int4range", vec![1]),
        ("'[0,100)'::int4range @> r", vec![1, 2, 4]),
        ("int4range(lo, hi, '[]') @> hi", vec![1, 2, 3, 4]),
        ("daterange(d, d + 7) @> '2024-01-08'::date", vec![1, 3]),
        ("upper(r * '[5,25)') > 8", vec![1, 2]),
        ("int4range(lo, hi) = r", vec![1, 2, 4]),
        ("NOT (int4range(lo, hi) @> 3)", vec![2, 4]),
        ("int4range(lo, hi) << int4range(15, 16)", vec![1, 3]),
        ("5 <@ int4range(lo, hi)", vec![1]),
        ("int4range(lo, hi) <> r", vec![]),
        ("(id = 2 OR int4range(lo, hi) @> 5) AND hi > 0", vec![1, 2]),
        ("NOT (int4range(lo, hi) && '[8,22)') OR id = 3", vec![3, 4]),
        ("r -|- int4range(hi, 40)", vec![1, 2]),
        ("NOT (r && int4range(lo + 5, hi + 5))", vec![4]),
        ("range_merge(r, '[40,50)') @> 35", vec![1, 2]),
        ("CASE WHEN int4range(lo, hi) @> 5 THEN true ELSE false END", vec![1]),
        // A column against a literal stays the scan's own comparison.
        ("r @> 5", vec![1]),
        ("NOT (r @> 5)", vec![2, 4]),
    ] {
        assert_eq!(ids(&c, &format!("SELECT id FROM rw WHERE {predicate} ORDER BY id")).await, want, "{predicate}");
    }
    assert_eq!(
        code(&c, "SELECT id FROM rw WHERE r -|- int4range(lo, 1)").await,
        ("22000".to_string(), "range lower bound must be less than or equal to range upper bound".to_string())
    );
    assert_eq!(
        row(&c, "SELECT range_merge(r, '[40,50)'), NOT (int4range(lo, hi) @> 3) FROM rw WHERE id = 2").await,
        "[20,50)|t"
    );
}
