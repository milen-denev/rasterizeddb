//! Bitwise operators over integers and bit strings, against PostgreSQL 17's
//! answers.
//!
//! `&`, `|`, `#`, `~`, `<<` and `>>` were not read at all: `SELECT 5 & 3`
//! looked for a column named "5 & 3", and `WHERE (flags & 1) = 1` was "neither
//! a column nor a value". In a `WHERE` `>>` was network containment only, so
//! `flags >> 1 = 3` failed to plan; and a prefix `~` after `AND` was taken
//! for a regular-expression match with `AND` as its operand.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE bf (id int PRIMARY KEY, flags int, s smallint, b bigint, bits bit(4), vb varbit, ip inet);
         INSERT INTO bf VALUES (1, 5, 3, 12, B'1010', B'101', '10.0.0.5'), (2, 6, 1, 1, B'0110', B'11', '192.168.1.9'),
                               (3, NULL, 2, 4, B'1111', B'1', '10.1.2.3');",
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
async fn integer_operators_group_and_type_as_postgresql_does() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &str)] = &[
        // One precedence with `||`, looser than arithmetic, left to right; a
        // prefix `~` takes the arithmetic after it.
        ("SELECT ~1 + 1, ~5 & 3, 1 + 2 & 3, 2 * 3 | 1, 1 << 2 + 1, 6 # 3 & 1, 1 | 2 || 'x'", "-3|2|3|7|8|1|3x"),
        (
            "SELECT 1::int2 & 1::int2, pg_typeof(1::int2 & 1::int2), pg_typeof(1::int2 & 1), pg_typeof(1 & 1::int8), \
             pg_typeof(1::int8 << 2), pg_typeof(1::int2 << 2)",
            "1|smallint|integer|bigint|bigint|smallint",
        ),
        // A shift wraps at the type's width, and keeps its sign.
        (
            "SELECT 1 << 33, -16 >> 2, ~0::int2, 1::int8 << 63, (-1) >> 1, 2147483647 << 1, (-2147483648) >> 31",
            "2|-4|-1|-9223372036854775808|-1|-2|-1",
        ),
        ("SELECT '6' & 3", "2"),
        // The wider operand's width, in the value too.
        ("SELECT 1::int2 | 65536, 70000 & 1::int2, 1::int2 # 65536::int8", "65537|0|65537"),
        ("SELECT NULL::int & 1, 5 & NULL", "NULL|NULL"),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, vec![want.to_string()], "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bit_strings_combine_bit_by_bit() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(
            &pool,
            "SELECT B'1010' & B'0110', B'1010' | B'0110', B'1010' # B'0110', ~B'1010', B'1010' << 1, B'1010' >> 1, \
             B'1010' << -1, B'10101' << 3"
        )
        .await,
        vec!["0010|1110|1100|0101|0100|0101|0101|01000"]
    );
    let cases: &[(&str, &str, &str)] = &[
        ("SELECT B'10' & B'1'", "22026", "cannot AND bit strings of different sizes"),
        ("SELECT 1.5 & 1", "42883", "operator does not exist: numeric & integer"),
        ("SELECT B'1010' & 1", "42883", "operator does not exist: bit & integer"),
        ("SELECT 'a' & 1", "22P02", "invalid input syntax for type integer: \"a\""),
        // One precedence with `||`, left to right: `('a' || 1) | 2`. Read
        // with `||` first, it answered 'a3'.
        ("SELECT 'a' || 1 | 2", "42883", "operator does not exist: text | integer"),
    ];
    for (sql, code, message) in cases {
        assert_eq!(error_of(&pool, sql).await, (code.to_string(), message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bitwise_operators_read_columns_everywhere() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(
            &pool,
            "SELECT id, flags & 1, flags | s, flags # b, ~flags, flags << 1, b >> 1, s & s, pg_typeof(s & s), \
             bits & B'0011', ~bits, bits << 1 FROM bf ORDER BY id"
        )
        .await,
        vec![
            "1|1|7|9|-6|10|6|3|smallint|0010|0101|0100",
            "2|0|7|7|-7|12|0|1|smallint|0010|1001|1100",
            "3|NULL|NULL|NULL|NULL|NULL|2|2|smallint|0011|0000|1110",
        ]
    );
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id FROM bf WHERE (flags & 1) = 1 ORDER BY id", &["1"]),
        ("SELECT id FROM bf WHERE flags & 4 = 4 ORDER BY id", &["1", "2"]),
        ("SELECT id FROM bf WHERE flags & 4 <> 0 AND s > 1 ORDER BY id", &["1"]),
        ("SELECT id FROM bf WHERE (bits & B'0010') = B'0010' ORDER BY id", &["1", "2", "3"]),
        // A shift beside a number, or compared after: not containment.
        ("SELECT id FROM bf WHERE flags >> 1 = 3 ORDER BY id", &["2"]),
        ("SELECT id FROM bf WHERE (b << s) > 10 ORDER BY id", &["1", "3"]),
        ("SELECT id FROM bf WHERE flags # 1 = 4 OR flags | 1 = 7 ORDER BY id", &["1", "2"]),
        // A prefix `~` is NOT, after `AND` or `=` too; a `~` with an operand
        // before it is still a match.
        ("SELECT id FROM bf WHERE ~flags = -6", &["1"]),
        ("SELECT id FROM bf WHERE id > 0 AND ~flags = -7", &["2"]),
        ("SELECT id FROM bf WHERE -7 = ~flags", &["2"]),
        ("SELECT id FROM bf WHERE CAST(flags AS text) ~ '5' ORDER BY id", &["1"]),
        ("SELECT id FROM bf WHERE CAST(flags AS text) !~ '5' ORDER BY id", &["2"]),
        // And containment still is.
        ("SELECT id FROM bf WHERE ip << inet '10.0.0.0/8' ORDER BY id", &["1", "3"]),
        ("SELECT id FROM bf WHERE ip <<= inet '10.0.0.0/8' ORDER BY id", &["1", "3"]),
        // In a group, which the evaluator reads: containment there too.
        ("SELECT id FROM bf WHERE (ip << inet '10.0.0.0/8') ORDER BY id", &["1", "3"]),
        ("SELECT id FROM bf WHERE (ip << inet '10.0.0.0/8') AND flags & 1 = 1 ORDER BY id", &["1"]),
        ("SELECT id FROM bf ORDER BY flags & 2, id", &["1", "2", "3"]),
        ("SELECT sum(flags & 1) FROM bf", &["1"]),
        ("SELECT id, flags & 3 AS f FROM bf GROUP BY id, flags & 3 ORDER BY id", &["1|1", "2|2", "3|NULL"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
    sqlx::raw_sql("UPDATE bf SET flags = flags | 8 WHERE id = 1").execute(&pool).await.expect("update");
    assert_eq!(rows(&pool, "SELECT flags FROM bf WHERE id = 1").await, vec!["13"]);
    assert_eq!(
        rows(&pool, "UPDATE bf SET flags = flags & ~8 WHERE id = 1 RETURNING flags, flags << 2").await,
        vec!["5|20"]
    );
    assert_eq!(rows(&pool, "SELECT a << 2 > 3 FROM (SELECT 1 AS a) t").await, vec!["t"]);
    // `<<` / `>>` between two inets the evaluator reads are containment.
    assert_eq!(
        rows(&pool, "SELECT id, ip << '10.0.0.0/8'::inet, '10.0.0.0/8'::inet >> ip FROM bf ORDER BY id").await,
        vec!["1|t|t", "2|f|f", "3|t|t"]
    );
    assert_eq!(rows(&pool, "SELECT id FROM bf WHERE (ip << '10.0.0.0/8'::inet) = true ORDER BY id").await, vec!["1", "3"]);
}
