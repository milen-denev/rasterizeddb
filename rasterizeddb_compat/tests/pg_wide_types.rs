//! The PostgreSQL types the engine gained beyond its original scalar set.
//!
//! Each test writes a value as its PostgreSQL literal and reads it back as
//! text, so it asserts the *whole* path: the DDL parser, the literal encoder,
//! storage, and both wire renderings. Reading back only "not an error" would
//! have passed while `CHAR(5)` silently kept four characters, which is what
//! these are written to catch.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Row, TypeInfo};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// Writes `literal` into a column of `declared_type` and reads it back as text.
///
/// Read through `::text` rather than as the column's own type. A driver
/// decoding a `point` wants a point, and asking it for a `String` is refused —
/// correctly, and by the OID the server advertised, which
/// [`wire_type`] asserts separately. The cast is what lets one helper compare
/// every type against the text PostgreSQL would print.
async fn round_trip(pool: &sqlx::PgPool, table: &str, declared_type: &str, literal: &str) -> String {
    run(
        pool,
        &format!("CREATE TABLE {} (id INTEGER NOT NULL, v {})", table, declared_type),
    )
    .await
    .unwrap_or_else(|e| panic!("CREATE with type {} failed: {}", declared_type, e));

    run(pool, &format!("INSERT INTO {} (id, v) VALUES (1, {})", table, literal))
        .await
        .unwrap_or_else(|e| panic!("INSERT of {} into {} failed: {}", literal, declared_type, e));

    let row = sqlx::query(AssertSqlSafe(format!("SELECT v::text FROM {}", table)))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("SELECT of {} failed: {}", declared_type, e));
    row.try_get::<Option<String>, _>(0)
        .unwrap_or_else(|e| panic!("decoding {} as text failed: {}", declared_type, e))
        .unwrap_or_default()
}

/// The type OID the server advertises for a column, as its driver-side name.
async fn wire_type(pool: &sqlx::PgPool, table: &str) -> String {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT v FROM {}", table)))
        .fetch_one(pool)
        .await
        .expect("select should succeed");
    row.columns()[0].type_info().name().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interval_round_trips_through_its_text_form() {
    let (_s, pool) = server().await;
    for (i, (literal, expected)) in [
        ("INTERVAL '1 hour'", "01:00:00"),
        ("INTERVAL '3 days 4 hours'", "3 days 04:00:00"),
        ("INTERVAL '1 year 2 mons'", "1 year 2 mons"),
        ("INTERVAL '30 minutes'", "00:30:00"),
        ("INTERVAL '90 seconds'", "00:01:30"),
        ("'P1Y2M3DT4H5M6S'", "1 year 2 mons 3 days 04:05:06"),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("iv{}", i);
        assert_eq!(round_trip(&pool, &table, "INTERVAL", literal).await, expected, "{}", literal);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interval_advertises_the_interval_oid() {
    let (_s, pool) = server().await;
    round_trip(&pool, "ivt", "INTERVAL", "INTERVAL '1 day'").await;
    assert_eq!(wire_type(&pool, "ivt").await, "INTERVAL");
}

#[tokio::test(flavor = "multi_thread")]
async fn mac_addresses_normalise_to_the_canonical_form() {
    let (_s, pool) = server().await;
    // Every separator style PostgreSQL accepts reaches the same six octets.
    for (i, literal) in
        ["'08:00:2b:01:02:03'", "'08-00-2b-01-02-03'", "'08002b:010203'", "'08002b010203'"]
            .into_iter()
            .enumerate()
    {
        let table = format!("mac{}", i);
        assert_eq!(
            round_trip(&pool, &table, "MACADDR", literal).await,
            "08:00:2b:01:02:03",
            "{}",
            literal
        );
    }
    assert_eq!(
        round_trip(&pool, "mac8", "MACADDR8", "'08:00:2b:01:02:03:04:05'").await,
        "08:00:2b:01:02:03:04:05"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bit_strings_keep_their_bits_and_their_width() {
    let (_s, pool) = server().await;
    assert_eq!(round_trip(&pool, "b1", "BIT(8)", "B'00000001'").await, "00000001");
    assert_eq!(round_trip(&pool, "b2", "BIT VARYING(32)", "B'1010'").await, "1010");
    // The leading zeroes are the point: a bit string is not a number, and
    // storing it as one would lose them.
    assert_eq!(round_trip(&pool, "b3", "BIT(8)", "B'00001111'").await, "00001111");
}

#[tokio::test(flavor = "multi_thread")]
async fn date_time_and_timetz_are_their_own_types() {
    let (_s, pool) = server().await;

    assert_eq!(round_trip(&pool, "d1", "DATE", "'2026-08-26'").await, "2026-08-26");
    assert_eq!(round_trip(&pool, "t1", "TIME", "'13:45:00'").await, "13:45:00");
    assert_eq!(round_trip(&pool, "z1", "TIMETZ", "'13:45:00+02'").await, "13:45:00+02");

    // The OIDs are the reason these were split out of DATETIME: advertised as
    // `timestamptz`, a driver asked for a NaiveDate refused the column.
    assert_eq!(wire_type(&pool, "d1").await, "DATE");
    assert_eq!(wire_type(&pool, "t1").await, "TIME");
    assert_eq!(wire_type(&pool, "z1").await, "TIMETZ");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timetz_keeps_the_offset_it_was_given() {
    let (_s, pool) = server().await;
    // Normalising to UTC would make this `11:45:00+00` — the same instant, and
    // not what PostgreSQL prints.
    assert_eq!(round_trip(&pool, "tz1", "TIMETZ", "'13:45:00+02'").await, "13:45:00+02");
    assert_eq!(round_trip(&pool, "tz2", "TIMETZ", "'08:15:00+00'").await, "08:15:00+00");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_geometric_types_round_trip() {
    let (_s, pool) = server().await;
    for (i, (declared, literal, expected)) in [
        ("POINT", "'(5,5)'", "(5,5)"),
        ("LSEG", "'[(0,0),(1,1)]'", "[(0,0),(1,1)]"),
        ("BOX", "'((0,0),(10,10))'", "(10,10),(0,0)"),
        ("PATH", "'((0,0),(1,1),(2,2))'", "((0,0),(1,1),(2,2))"),
        ("PATH", "'[(5,5),(6,6)]'", "[(5,5),(6,6)]"),
        ("POLYGON", "'((0,0),(0,10),(10,10),(10,0))'", "((0,0),(0,10),(10,10),(10,0))"),
        ("CIRCLE", "'<(5,5),3>'", "<(5,5),3>"),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("geo{}", i);
        assert_eq!(
            round_trip(&pool, &table, declared, literal).await,
            expected,
            "{} {}",
            declared,
            literal
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_box_normalises_its_corners() {
    let (_s, pool) = server().await;
    // PostgreSQL stores the upper-right corner first, so two boxes written
    // corner-swapped are the same box.
    let a = round_trip(&pool, "bx1", "BOX", "'((0,0),(10,10))'").await;
    let b = round_trip(&pool, "bx2", "BOX", "'((10,10),(0,0))'").await;
    assert_eq!(a, b);
    assert_eq!(a, "(10,10),(0,0)");
}

#[tokio::test(flavor = "multi_thread")]
async fn ranges_round_trip_and_canonicalise() {
    let (_s, pool) = server().await;
    assert_eq!(round_trip(&pool, "r1", "INT4RANGE", "'[50,150)'").await, "[50,150)");
    // A discrete range is stored `[)`, so `[1,10]` and `[1,11)` are one range.
    assert_eq!(round_trip(&pool, "r2", "INT4RANGE", "'[1,10]'").await, "[1,11)");
    assert_eq!(round_trip(&pool, "r3", "INT4RANGE", "'(0,10)'").await, "[1,10)");
    assert_eq!(round_trip(&pool, "r4", "INT8RANGE", "'[1,100)'").await, "[1,100)");
    assert_eq!(round_trip(&pool, "r5", "DATERANGE", "'[2026-01-01,2026-02-01)'").await,
               "[2026-01-01,2026-02-01)");
    // A range whose bounds cross holds nothing, and says so.
    assert_eq!(round_trip(&pool, "r6", "INT4RANGE", "'empty'").await, "empty");
    assert_eq!(round_trip(&pool, "r7", "INT4RANGE", "'[5,5)'").await, "empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_char_column_keeps_every_character_it_was_given() {
    let (_s, pool) = server().await;
    // `CHAR(5)` used to map to the engine's one-codepoint type, which stored
    // the first character and reported success: 'ABCDE' came back 'ABCD'.
    assert_eq!(round_trip(&pool, "c5", "CHAR(5)", "'ABCDE'").await, "ABCDE");
    assert_eq!(round_trip(&pool, "c3", "CHAR(3)", "'USD'").await, "USD");
    assert_eq!(round_trip(&pool, "c64", "VARCHAR(64)", "'ORD-0001'").await, "ORD-0001");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_word_type_carrying_array_brackets_is_an_array() {
    let (_s, pool) = server().await;
    // `DOUBLE PRECISION[][]` failed the two-word test, fell back to `DOUBLE`,
    // and left `PRECISION[][]` to be read as a constraint — so the column
    // became a scalar and the array-ness was lost with nothing reported.
    assert_eq!(
        round_trip(&pool, "m1", "DOUBLE PRECISION[][]", "'{1.0,2.0}'").await,
        "{1.0,2.0}"
    );
    assert_eq!(round_trip(&pool, "m2", "REAL[]", "'{80.5,90.25}'").await, "{80.5,90.25}");
    assert_eq!(round_trip(&pool, "m3", "TEXT[]", "'{express,gift}'").await, "{express,gift}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_serial_column_numbers_the_rows_it_is_not_given() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE s (reading_id BIGSERIAL PRIMARY KEY, note TEXT)")
        .await
        .expect("create");

    for note in ["a", "b", "c"] {
        run(&pool, &format!("INSERT INTO s (note) VALUES ('{}')", note))
            .await
            .unwrap_or_else(|e| panic!("insert of {} failed: {}", note, e));
    }

    let ids: Vec<i64> =
        sqlx::query(AssertSqlSafe("SELECT reading_id FROM s ORDER BY reading_id"))
            .fetch_all(&pool)
            .await
            .expect("select")
            .iter()
            .map(|r| r.get::<i64, _>(0))
            .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_value_does_not_advance_the_sequence() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE s2 (id BIGSERIAL PRIMARY KEY, note TEXT)").await.expect("create");

    run(&pool, "INSERT INTO s2 (id, note) VALUES (100, 'explicit')").await.expect("insert");
    run(&pool, "INSERT INTO s2 (note) VALUES ('next')").await.expect("insert");

    // PostgreSQL's behaviour: writing a value by hand does not move the
    // sequence, so the next generated key is 1 and not 101. Asserted rather
    // than assumed, because the opposite is the intuitive guess.
    let ids: Vec<i64> = sqlx::query(AssertSqlSafe("SELECT id FROM s2 ORDER BY id"))
        .fetch_all(&pool)
        .await
        .expect("select")
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect();
    assert_eq!(ids, vec![1, 100]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wall_clock_defaults_are_understood() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE clocks (
            id INTEGER NOT NULL,
            a TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
            b DATE NOT NULL DEFAULT CURRENT_DATE,
            c TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            d TIME NOT NULL DEFAULT CURRENT_TIME
        )",
    )
    .await
    .expect("create should succeed");
    run(&pool, "INSERT INTO clocks (id) VALUES (1)").await.expect("insert");

    let row = sqlx::query(AssertSqlSafe(
        "SELECT a::text, b::text, c::text, d::text FROM clocks",
    ))
        .fetch_one(&pool)
        .await
        .expect("select");
    for i in 0..4 {
        let value: Option<String> = row.try_get(i).expect("decodable as text");
        assert!(value.is_some_and(|v| !v.is_empty()), "column {} was empty", i);
    }
}
