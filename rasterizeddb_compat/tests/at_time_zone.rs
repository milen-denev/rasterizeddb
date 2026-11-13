//! `AT TIME ZONE`, against PostgreSQL 17's answers (`docker run --rm
//! postgres:17`, TimeZone UTC). It was not read at all: the value and its
//! zone were looked up as one function name.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

/// Each row as `a|b|…`, a NULL as `NULL`.
async fn rows(c: &Client, sql: &str) -> Vec<String> {
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

/// A timestamptz becomes the zone's wall-clock timestamp and a timestamp
/// the zone's instant; named zones follow the tz database (DST, any case),
/// POSIX offsets count west (`'+02'` is two hours behind), an interval
/// counts east, a skipped or repeated wall-clock time reads at the later
/// instant, a date is its UTC midnight, and an unknown literal is a
/// timestamptz. In a select list, a WHERE, under a cast, arithmetic and
/// date_trunc.
#[tokio::test(flavor = "multi_thread")]
async fn values_move_between_zones_as_postgresql_moves_them() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (sql, want) in [
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE '+02'", "2024-07-01 10:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'UTC+3'", "2024-07-01 09:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'GMT-2'", "2024-07-01 14:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE '-03:30'", "2024-07-01 15:30:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE '+167'", "2024-06-24 13:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE '+1:2'", "2024-07-01 10:58:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'Europe/Sofia'", "2024-07-01 15:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'europe/sofia'", "2024-07-01 15:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'PST8PDT'", "2024-07-01 05:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE INTERVAL '02:00'", "2024-07-01 14:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'UTC'", "2024-07-01 12:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'Z'", "2024-07-01 12:00:00"),
        ("SELECT '2024-07-01 12:00+00'::timestamptz AT TIME ZONE NULL", "NULL"),
        ("SELECT '2024-07-01 12:00'::timestamp AT TIME ZONE 'Europe/Sofia'", "2024-07-01 09:00:00+00"),
        ("SELECT '2024-03-10 02:30'::timestamp AT TIME ZONE 'America/New_York'", "2024-03-10 07:30:00+00"),
        ("SELECT '2024-11-03 01:30'::timestamp AT TIME ZONE 'America/New_York'", "2024-11-03 06:30:00+00"),
        ("SELECT date '2024-07-01' AT TIME ZONE 'UTC'", "2024-07-01 00:00:00"),
        ("SELECT '2024-01-15 23:30+00' AT TIME ZONE 'Asia/Tokyo'", "2024-01-16 08:30:00"),
        ("SELECT ('2024-07-01 12:00+00'::timestamptz AT TIME ZONE 'Europe/Sofia') AT TIME ZONE 'Europe/Sofia'", "2024-07-01 12:00:00+00"),
        ("SELECT pg_typeof(now() AT TIME ZONE 'UTC')", "timestamp without time zone"),
        ("SELECT pg_typeof(localtimestamp AT TIME ZONE 'UTC')", "timestamp with time zone"),
    ] {
        assert_eq!(rows(&c, sql).await, [want], "{sql}");
    }
    c.simple_query("CREATE TABLE tzt (id INT PRIMARY KEY, tz TIMESTAMPTZ, ts TIMESTAMP)").await.expect("create");
    c.simple_query("INSERT INTO tzt VALUES (1, '2024-07-01 12:00+00', '2024-07-01 12:00'), (2, '2024-12-31 23:30+00', '2024-12-31 23:30'), (3, NULL, NULL)")
        .await
        .expect("insert");
    for (sql, want) in [
        (
            "SELECT id, tz AT TIME ZONE 'Europe/Sofia', ts AT TIME ZONE 'Europe/Sofia' FROM tzt ORDER BY id",
            vec!["1|2024-07-01 15:00:00|2024-07-01 09:00:00+00", "2|2025-01-01 01:30:00|2024-12-31 21:30:00+00", "3|NULL|NULL"],
        ),
        ("SELECT id FROM tzt WHERE (tz AT TIME ZONE 'America/New_York')::date = '2024-12-31' ORDER BY id", vec!["2"]),
        ("SELECT id FROM tzt WHERE tz AT TIME ZONE 'UTC' > '2024-08-01' ORDER BY id", vec!["2"]),
        (
            "SELECT id, tz AT TIME ZONE 'Asia/Kolkata' + interval '1 hour', date_trunc('day', tz AT TIME ZONE 'Asia/Tokyo') FROM tzt ORDER BY id",
            vec!["1|2024-07-01 18:30:00|2024-07-01 00:00:00", "2|2025-01-01 06:00:00|2025-01-01 00:00:00", "3|NULL|NULL"],
        ),
    ] {
        assert_eq!(rows(&c, sql).await, want, "{sql}");
    }
}

/// A zone nobody knows is PostgreSQL's 22023. An abbreviation is refused:
/// PostgreSQL reads `CET` as a fixed +1 (13:00 here) where the tz database's
/// zone `CET` keeps summer time (14:00), so answering from the zone would be
/// an hour off. An interval with days is refused, as there.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_zones_and_abbreviations_are_refused() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (sql, needle) in [
        ("SELECT now() AT TIME ZONE 'Mars/Base'", "not recognized"),
        ("SELECT now() AT TIME ZONE 'CET'", "abbreviations are not supported"),
        ("SELECT now() AT TIME ZONE 'ist'", "abbreviations are not supported"),
        ("SELECT now() AT TIME ZONE INTERVAL '1 day'", "must not include months or days"),
        ("SELECT now() AT TIME ZONE '+168'", "not recognized"),
        ("SELECT now() AT TIME ZONE '+5:60'", "not recognized"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let text = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string());
        assert!(text.contains(needle), "{sql}: {text}");
    }
}
