//! Casts to an array type, and `LATERAL` function items over array and
//! float columns, against PostgreSQL 17's answers.
//!
//! `x::int[]` and `CAST(x AS float8[])` were refused in every expression
//! ("cast to 'int[]' is not supported yet"); only a few shapes the predicate
//! and table-function code read for themselves worked. The table functions
//! then took a `CAST(… AS T[])` argument for a column of another relation,
//! so `LATERAL unnest(t.arr)` — whose outer value is substituted as exactly
//! such a cast — failed, and a bare `unnest(arr)` ended as `FROM LATERAL
//! unnest(arr)` and "LATERAL outside a FROM list". A `LATERAL` item over a
//! `double precision` column cast its value to `DOUBLE`, a type no cast
//! takes. And a column declared `float8[][]` was an untyped array, its
//! elements kept as written (`1.0`), where PostgreSQL's `float8[][]` is
//! `float8[]`.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE tl (id INT PRIMARY KEY, r DOUBLE PRECISION[], m DOUBLE PRECISION[][]);
         INSERT INTO tl VALUES (1, '{80.5,90.25,42.0}', '{{1.0,2.0},{3.0,4.0}}'), (2, '{10.0,20.0}', '{{5.0,6.0}}');
         CREATE TABLE tf (id INT PRIMARY KEY, f DOUBLE PRECISION, s SMALLINT);
         INSERT INTO tf VALUES (1, 1.5, 3), (2, 2.5, 4);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn error_of(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    let d = e.as_database_error().expect("a database error");
    (d.code().map(|c| c.to_string()).unwrap_or_default(), d.message().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn casts_to_an_array_type_read_each_element_as_that_type() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &str)] = &[
        ("SELECT '{a,b}'::text[]", "{a,b}"),
        ("SELECT CAST('{1.5,2}' AS double precision[])", "{1.5,2}"),
        ("SELECT CAST(NULL AS int[]) IS NULL", "t"),
        ("SELECT ('{1,2,3}'::int[])[2] + 1", "3"),
        ("SELECT '{{1,2},{3,4}}'::int[][]", "{{1,2},{3,4}}"),
        ("SELECT '{t,false}'::boolean[]", "{t,f}"),
        ("SELECT '{2026-01-02}'::date[]", "{2026-01-02}"),
        ("SELECT array_length('{1,2,3}'::int[], 1)", "3"),
        // An array of another type converts element by element, each as its
        // own cast rounds: numeric halves away from zero, float to even.
        ("SELECT ARRAY[1,2]::text[]", "{1,2}"),
        ("SELECT ARRAY[1.5,2.25]::int[]", "{2,2}"),
        ("SELECT ARRAY[2.5,3.5]::int[]", "{3,4}"),
        ("SELECT ARRAY[2.5::float8,3.5::float8]::int[]", "{2,4}"),
        ("SELECT '{1.5,2.5}'::numeric[]::int[]", "{2,3}"),
        ("SELECT ARRAY[ARRAY[1.5,2.5],ARRAY[3.5,4.5]]::int[]", "{{2,3},{4,5}}"),
        ("SELECT ARRAY[true,false]::int[]", "{1,0}"),
        ("SELECT ARRAY[1,0]::boolean[]", "{t,f}"),
        ("SELECT ARRAY[1.5,NULL]::float8[]", "{1.5,NULL}"),
        // A boolean's text is `true`, not the `t` an array prints.
        ("SELECT ARRAY[true,false]::text[]", "{true,false}"),
        ("SELECT '{t,f}'::bool[]::text[]", "{true,false}"),
        ("SELECT ARRAY[1.50]::text[]", "{1.50}"),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, vec![want.to_string()], "{sql}");
    }
    assert_eq!(rows(&pool, "SELECT r::int[] FROM tl ORDER BY id").await, vec!["{80,90,42}", "{10,20}"]);
    assert_eq!(rows(&pool, "SELECT m::text[] FROM tl ORDER BY id").await, vec!["{{1,2},{3,4}}", "{{5,6}}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_array_cast_fails_as_postgresql_fails_it() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &str, &str)] = &[
        ("SELECT '{1,x}'::int[]", "22P02", "invalid input syntax for type integer: \"x\""),
        ("SELECT 'abc'::int[]", "22P02", "malformed array literal: \"abc\""),
        ("SELECT '{1,2}'::foo[]", "42704", "type \"foo[]\" does not exist"),
        ("SELECT '{1,2}'::double[]", "42704", "type \"double[]\" does not exist"),
        ("SELECT ARRAY[70000]::smallint[]", "22003", "smallint out of range"),
        // PostgreSQL rounds and cuts each element to the modifier; kept as
        // written here, it is refused rather than answered wrong.
        ("SELECT '{1.234}'::numeric(5,2)[]", "0A000", "a cast to an array of numeric(5,2) is not supported yet"),
        ("SELECT '{abcdef}'::varchar(3)[]", "0A000", "a cast to an array of varchar(3) is not supported yet"),
    ];
    for (sql, code, message) in cases {
        assert_eq!(error_of(&pool, sql).await, (code.to_string(), message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_two_bracket_declaration_is_the_typed_array() {
    let (_s, pool) = server().await;
    // `float8[][]` is `float8[]`: the elements are held as float8s print,
    // not as written.
    assert_eq!(rows(&pool, "SELECT m FROM tl ORDER BY id").await, vec!["{{1,2},{3,4}}", "{{5,6}}"]);
    assert_eq!(rows(&pool, "SELECT array_ndims(m), m[2][1] FROM tl ORDER BY id").await, vec!["2|3", "2|NULL"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unnest_takes_a_cast_array_argument() {
    let (_s, pool) = server().await;
    assert_eq!(rows(&pool, "SELECT * FROM unnest(CAST('{1.5,2}' AS double precision[])) AS g").await, vec!["1.5", "2"]);
    assert_eq!(rows(&pool, "SELECT * FROM unnest(ARRAY[1,2]::text[]) g").await, vec!["1", "2"]);
    assert_eq!(rows(&pool, "SELECT * FROM unnest('{t,f}'::bool[]) g").await, vec!["t", "f"]);
    assert_eq!(rows(&pool, "SELECT * FROM unnest('{1,2}'::smallint[]) g").await, vec!["1", "2"]);
    assert!(rows(&pool, "SELECT * FROM unnest(CAST(NULL AS int[])) g").await.is_empty());
    // Each element of the array's type: dates add, a timestamptz keeps its
    // zone, a numeric its places.
    assert_eq!(rows(&pool, "SELECT g + 1 FROM unnest('{2026-01-02}'::date[]) g").await, vec!["2026-01-03"]);
    assert_eq!(
        rows(&pool, "SELECT * FROM unnest('{\"2026-01-02 10:00:00+00\"}'::timestamptz[]) g").await,
        vec!["2026-01-02 10:00:00+00"]
    );
    assert_eq!(rows(&pool, "SELECT * FROM unnest('{1.50,2}'::numeric[]) g").await, vec!["1.50", "2"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lateral_function_reads_an_earlier_items_column() {
    let (_s, pool) = server().await;
    let flat = ["1|42", "1|80.5", "1|90.25", "2|10", "2|20"];
    for sql in [
        "SELECT id, v FROM tl, LATERAL unnest(r) AS a(v) ORDER BY 1, 2",
        "SELECT t.id, g FROM tl t CROSS JOIN LATERAL unnest(t.r) g ORDER BY 1, 2",
        "SELECT t.id, g FROM tl t, unnest(t.r) g ORDER BY 1, 2",
        "SELECT id, g FROM tl, unnest(r) g ORDER BY 1, 2",
    ] {
        assert_eq!(rows(&pool, sql).await, flat, "{sql}");
    }
    assert_eq!(
        rows(&pool, "SELECT id, v, n FROM tl, LATERAL unnest(r) WITH ORDINALITY AS a(v, n) ORDER BY 1, 2").await,
        vec!["1|42|3", "1|80.5|1", "1|90.25|2", "2|10|1", "2|20|2"]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT id, array_length(r, 1), v, n FROM tl, LATERAL unnest(r) WITH ORDINALITY AS arr(v, n) \
             WHERE array_length(r, 1) IS NOT NULL AND v > 75.5 AND m[1][1] IS NOT NULL ORDER BY 1, 3"
        )
        .await,
        vec!["1|3|80.5|1", "1|3|90.25|2"]
    );
    // A multidimensional array unnests flat.
    assert_eq!(
        rows(&pool, "SELECT id, g FROM tl, unnest(m) g ORDER BY 1, 2").await,
        vec!["1|1", "1|2", "1|3", "1|4", "2|5", "2|6"]
    );
    // A bare name is the earlier item's that has it, with others between.
    assert_eq!(
        rows(&pool, "SELECT tl.id, v FROM tl, tf, unnest(r) v WHERE tf.id = 1 ORDER BY 1, 2").await,
        flat
    );
    assert_eq!(
        rows(&pool, "SELECT id, x FROM tf, generate_series(1, s) x ORDER BY 1, 2").await,
        vec!["1|1", "1|2", "1|3", "2|1", "2|2", "2|3", "2|4"]
    );
    assert_eq!(
        rows(&pool, "SELECT id, x FROM tf, generate_series(1, CAST(s AS int)) x ORDER BY 1, 2").await,
        vec!["1|1", "1|2", "1|3", "2|1", "2|2", "2|3", "2|4"]
    );
    assert_eq!(
        rows(&pool, "SELECT id, x FROM tf, LATERAL (VALUES (f), (s)) v(x) ORDER BY 1, 2").await,
        vec!["1|1.5", "1|3", "2|2.5", "2|4"]
    );
    assert_eq!(
        rows(&pool, "SELECT id, x FROM tf, unnest(ARRAY[f, f*2]) x ORDER BY 1, 2").await,
        vec!["1|1.5", "1|3", "2|2.5", "2|5"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lateral_item_reads_a_double_precision_column() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT t.id, x FROM tf t CROSS JOIN LATERAL (SELECT t.f * 2 AS x) s ORDER BY 1").await,
        vec!["1|3", "2|5"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_no_earlier_item_has_fails_as_postgresql_fails_it() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &str, &str)] = &[
        ("SELECT * FROM tl, unnest(zz) g", "42703", "column \"zz\" does not exist"),
        ("SELECT * FROM unnest(r) g", "42703", "column \"r\" does not exist"),
        ("SELECT * FROM unnest(t.r) g", "42P01", "missing FROM-clause entry for table \"t\""),
        ("SELECT x FROM tf a, tf b, generate_series(1, s) x", "42702", "column reference \"s\" is ambiguous"),
    ];
    for (sql, code, message) in cases {
        assert_eq!(error_of(&pool, sql).await, (code.to_string(), message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_array_declaration_postgresql_reads_is_an_array() {
    let (_s, pool) = server().await;
    // Each was created as a scalar `integer` (or `text`) column: the words
    // after the type were read as constraint text, which is matched by
    // substring, and dropped.
    sqlx::raw_sql(
        "CREATE TABLE d1 (a int ARRAY);
         CREATE TABLE d2 (a int ARRAY[3]);
         CREATE TABLE d3 (a int [ ]);
         CREATE TABLE d8 (a text ARRAY NOT NULL);
         CREATE TABLE d9 (a double precision ARRAY [2]);
         CREATE TABLE d10 (a numeric(10, 2) [ 3 ] NOT NULL, b varchar(5) ARRAY);
         INSERT INTO d1 VALUES ('{1,2}');
         INSERT INTO d3 VALUES ('{01,2}');
         INSERT INTO d8 VALUES ('{x,y}');
         INSERT INTO d9 VALUES ('{1.0,2}');
         INSERT INTO d10 VALUES ('{1.50}', '{ab,cd}');",
    )
    .execute(&pool)
    .await
    .expect("declarations");
    assert_eq!(rows(&pool, "SELECT a, a[2] FROM d1").await, vec!["{1,2}|2"]);
    assert_eq!(rows(&pool, "SELECT a FROM d3").await, vec!["{1,2}"]);
    assert_eq!(rows(&pool, "SELECT a[2] FROM d8").await, vec!["y"]);
    assert_eq!(rows(&pool, "SELECT a FROM d9").await, vec!["{1,2}"]);
    assert_eq!(rows(&pool, "SELECT a, b FROM d10").await, vec!["{1.50}|{ab,cd}"]);
    assert_eq!(
        error_of(&pool, "INSERT INTO d2 VALUES ('{1,x}')").await,
        ("22P02".to_string(), "invalid input syntax for type integer: \"x\"".to_string())
    );
    // And by ALTER TABLE … ADD COLUMN, which reads the type the same way.
    sqlx::raw_sql("ALTER TABLE d1 ADD COLUMN b int ARRAY; ALTER TABLE d1 ADD COLUMN e float8[][];")
        .execute(&pool)
        .await
        .expect("add columns");
    sqlx::raw_sql("INSERT INTO d1 (a, b, e) VALUES ('{3}', '{4,5}', '{{1.0}}')").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT a, b, e FROM d1 ORDER BY a").await, vec!["{1,2}|NULL|NULL", "{3}|{4,5}|{{1}}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_word_after_a_column_type_that_is_no_constraint_is_a_syntax_error() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &str)] = &[
        ("CREATE TABLE e1 (a int foo)", "foo"),
        ("CREATE TABLE e2 (a int ARRAY[3][4])", "["),
        ("CREATE TABLE e3 (a int ARRAY[])", "]"),
        ("CREATE TABLE e4 (a int ARRAY [ ])", "]"),
        ("ALTER TABLE tf ADD COLUMN c int foo", "foo"),
    ];
    for (sql, near) in cases {
        assert_eq!(
            error_of(&pool, sql).await,
            ("42601".to_string(), format!("syntax error at or near \"{near}\"")),
            "{sql}"
        );
    }
    // The constraint words, a split modifier and the multi-word types all
    // still read.
    sqlx::raw_sql(
        "CREATE TABLE ok1 (a timestamp with time zone[], b interval hour to minute, c int NOT NULL DEFAULT 1 UNIQUE);
         CREATE TABLE ok2 (a varchar (3), c timestamp (0), d numeric (5, 2) [], e int CONSTRAINT c1 NOT NULL);
         CREATE TABLE ok3 (a int, b text COLLATE \"C\", c serial PRIMARY KEY, d bigint GENERATED ALWAYS AS IDENTITY);",
    )
    .execute(&pool)
    .await
    .expect("valid declarations");
    assert_eq!(
        error_of(&pool, "INSERT INTO ok2 (a, e) VALUES ('abcd', 1)").await,
        ("22001".to_string(), "value too long for type character varying(3)".to_string())
    );
    sqlx::raw_sql("INSERT INTO ok2 (a, c, e) VALUES ('ab', '2026-01-01 10:00:00.6', 1)").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT c::text FROM ok2").await, vec!["2026-01-01 10:00:01"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_modifier_written_apart_from_its_type_is_kept() {
    let (_s, pool) = server().await;
    // `numeric (5, 2)` kept every value at the default scale, and
    // `timestamp (0) with time zone` was a timestamp without one.
    sqlx::raw_sql(
        "CREATE TABLE n1 (id int, a numeric (5, 2), b numeric(5,2), c timestamp (0) with time zone, \
         d time (0) without time zone, e varchar (2));
         INSERT INTO n1 VALUES (1, 1.234, 1.234, '2026-01-01 10:00:00.6+02', '10:00:00.6', 'ab');",
    )
    .execute(&pool)
    .await
    .expect("setup");
    assert_eq!(
        rows(&pool, "SELECT a, b, c, d, e FROM n1").await,
        vec!["1.23|1.23|2026-01-01 08:00:01+00|10:00:01|ab"]
    );
    assert_eq!(error_of(&pool, "INSERT INTO n1 (id, a) VALUES (2, 12345.6)").await.0, "22003");
}
