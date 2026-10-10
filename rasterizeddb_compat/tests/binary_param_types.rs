//! Binary-format parameters for every type the engine stores whose binary
//! form the Bind path did not decode: date, time, timetz, interval, inet,
//! macaddr, bit strings, the geometric types and the ranges.
//!
//! Each case is differential: the same value bound as a binary parameter and
//! written as a text literal must store the same bytes and read back as the
//! same text. The statement describe must also report the column's own type,
//! which is what makes a client send the binary form in the first place.

use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use rasterizeddb_compat::TestServer;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// A parameter given as its binary wire bytes.
#[derive(Debug)]
struct Raw(Vec<u8>);

impl ToSql for Raw {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    to_sql_checked!();
}

/// A result column as its binary wire bytes.
struct RawOut(Vec<u8>);

impl<'a> FromSql<'a> for RawOut {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawOut(raw.to_vec()))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

fn f64s(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_be_bytes()).collect()
}

/// A range's binary form: flags, then each present bound as length + bytes.
fn range(flags: u8, bounds: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![flags];
    for b in bounds {
        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
        out.extend_from_slice(b);
    }
    out
}

const LB_INC: u8 = 0x02;
const UB_INC: u8 = 0x04;
const UB_INF: u8 = 0x10;

/// Days from 2000-01-01, and microseconds from 2000-01-01 00:00.
fn pg_days(y: i32, m: u32, d: u32) -> i32 {
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
    (date - chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()).num_days() as i32
}

struct Case {
    column: &'static str,
    ty: Type,
    text: &'static str,
    binary: Vec<u8>,
}

fn cases() -> Vec<Case> {
    let mut numeric_1_5 = Vec::new();
    for v in [2i16, 0, 0, 1, 1, 5000] {
        numeric_1_5.extend_from_slice(&v.to_be_bytes());
    }
    let mut numeric_2_5 = Vec::new();
    for v in [2i16, 0, 0, 1, 2, 5000] {
        numeric_2_5.extend_from_slice(&v.to_be_bytes());
    }
    let ts = |days: i32, secs: i64| (days as i64 * 86_400_000_000 + secs * 1_000_000).to_be_bytes();
    vec![
        Case {
            column: "TIMETZ",
            ty: Type::TIMETZ,
            text: "12:34:56+02",
            binary: [45_296_000_000i64.to_be_bytes().to_vec(), (-7200i32).to_be_bytes().to_vec()].concat(),
        },
        Case {
            column: "INTERVAL",
            ty: Type::INTERVAL,
            text: "1 year 2 mons 4 days 01:02:03",
            binary: [
                3_723_000_000i64.to_be_bytes().to_vec(),
                4i32.to_be_bytes().to_vec(),
                14i32.to_be_bytes().to_vec(),
            ]
            .concat(),
        },
        Case {
            column: "INET",
            ty: Type::INET,
            text: "10.0.0.0/8",
            binary: vec![2, 8, 0, 4, 10, 0, 0, 0],
        },
        Case {
            column: "INET",
            ty: Type::INET,
            text: "2001:db8::1",
            binary: [vec![3, 128, 0, 16], "2001:db8::1".parse::<std::net::Ipv6Addr>().unwrap().octets().to_vec()].concat(),
        },
        Case {
            column: "MACADDR",
            ty: Type::MACADDR,
            text: "08:00:2b:01:02:03",
            binary: vec![8, 0, 0x2b, 1, 2, 3],
        },
        Case {
            column: "MACADDR8",
            ty: Type::MACADDR8,
            text: "08:00:2b:01:02:03:04:05",
            binary: vec![8, 0, 0x2b, 1, 2, 3, 4, 5],
        },
        Case {
            column: "BIT(4)",
            ty: Type::BIT,
            text: "1010",
            binary: [4i32.to_be_bytes().to_vec(), vec![0xA0]].concat(),
        },
        Case {
            column: "VARBIT",
            ty: Type::VARBIT,
            text: "10110",
            binary: [5i32.to_be_bytes().to_vec(), vec![0xB0]].concat(),
        },
        Case { column: "POINT", ty: Type::POINT, text: "(1.5,-2)", binary: f64s(&[1.5, -2.0]) },
        Case { column: "LSEG", ty: Type::LSEG, text: "[(1,2),(3,4)]", binary: f64s(&[1.0, 2.0, 3.0, 4.0]) },
        Case { column: "BOX", ty: Type::BOX, text: "(3,4),(1,2)", binary: f64s(&[3.0, 4.0, 1.0, 2.0]) },
        Case { column: "LINE", ty: Type::LINE, text: "{1,-1,0}", binary: f64s(&[1.0, -1.0, 0.0]) },
        Case { column: "CIRCLE", ty: Type::CIRCLE, text: "<(1,2),3>", binary: f64s(&[1.0, 2.0, 3.0]) },
        Case {
            column: "PATH",
            ty: Type::PATH,
            text: "((0,0),(1,1),(2,0))",
            binary: [vec![1u8], 3i32.to_be_bytes().to_vec(), f64s(&[0.0, 0.0, 1.0, 1.0, 2.0, 0.0])].concat(),
        },
        Case {
            column: "POLYGON",
            ty: Type::POLYGON,
            text: "((0,0),(1,1),(1,0))",
            binary: [3i32.to_be_bytes().to_vec(), f64s(&[0.0, 0.0, 1.0, 1.0, 1.0, 0.0])].concat(),
        },
        Case {
            column: "INT4RANGE",
            ty: Type::INT4_RANGE,
            text: "[1,10)",
            binary: range(LB_INC, &[&1i32.to_be_bytes(), &10i32.to_be_bytes()]),
        },
        Case {
            column: "INT8RANGE",
            ty: Type::INT8_RANGE,
            text: "[5,)",
            binary: range(LB_INC | UB_INF, &[&5i64.to_be_bytes()]),
        },
        Case {
            column: "NUMRANGE",
            ty: Type::NUM_RANGE,
            text: "[1.5,2.5]",
            binary: range(LB_INC | UB_INC, &[&numeric_1_5, &numeric_2_5]),
        },
        Case {
            column: "DATERANGE",
            ty: Type::DATE_RANGE,
            text: "[2026-01-01,2026-02-01)",
            binary: range(
                LB_INC,
                &[&pg_days(2026, 1, 1).to_be_bytes(), &pg_days(2026, 2, 1).to_be_bytes()],
            ),
        },
        Case {
            column: "TSRANGE",
            ty: Type::TS_RANGE,
            text: "[\"2026-01-01 00:00:00\",\"2026-01-01 12:00:00\")",
            binary: range(
                LB_INC,
                &[&ts(pg_days(2026, 1, 1), 0), &ts(pg_days(2026, 1, 1), 12 * 3600)],
            ),
        },
    ]
}

#[tokio::test]
async fn binary_and_text_binds_store_the_same_value() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    let mut failures = Vec::new();

    for (n, case) in cases().into_iter().enumerate() {
        let table = format!("bp{n}");
        client
            .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, v {})", case.column))
            .await
            .unwrap_or_else(|e| panic!("create {}: {e}", case.column));

        let insert = client
            .prepare(&format!("INSERT INTO {table} (id, v) VALUES ($1, $2)"))
            .await
            .expect("prepare");
        if insert.params()[1] != case.ty {
            failures.push(format!("{}: described as {:?}", case.column, insert.params()[1]));
            continue;
        }
        if let Err(e) = client.execute(&insert, &[&1i32, &Raw(case.binary.clone())]).await {
            failures.push(format!("{}: binary bind refused: {e:?}", case.column));
            continue;
        }
        client
            .batch_execute(&format!(
                "INSERT INTO {table} (id, v) VALUES (2, '{}')",
                case.text.replace('\'', "''")
            ))
            .await
            .unwrap_or_else(|e| panic!("text insert {}: {e}", case.column));

        // Stored bytes, as the binary result path sends them.
        let read = |id: i32| {
            let client = &client;
            let table = table.clone();
            async move {
                client
                    .query_one(&format!("SELECT v FROM {table} WHERE id = $1"), &[&id])
                    .await
                    .map(|r| r.get::<_, RawOut>(0).0)
            }
        };
        let (binary, text) = (read(1).await, read(2).await);
        match (binary, text) {
            (Ok(b), Ok(t)) if b == t => {}
            (b, t) => {
                failures.push(format!(
                    "{}: binary bind stored {:?}, text stored {:?}",
                    case.column,
                    b.map_err(|e| e.to_string()),
                    t.map_err(|e| e.to_string())
                ));
                continue;
            }
        }
        // And the text both read back as.
        let rows = client
            .simple_query(&format!("SELECT v FROM {table} ORDER BY id"))
            .await
            .expect("simple read");
        let texts: Vec<String> = rows
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        if texts.len() != 2 || texts[0] != texts[1] {
            failures.push(format!("{}: read back as {:?}", case.column, texts));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Where tokio-postgres has a native type, the same through it.
#[tokio::test]
async fn native_date_time_and_inet_types_bind() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    client
        .batch_execute("CREATE TABLE nt (id INT PRIMARY KEY, d DATE, t TIME, ip INET)")
        .await
        .expect("create");
    let date = chrono::NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();
    let time = chrono::NaiveTime::from_hms_micro_opt(12, 34, 56, 789_000).unwrap();
    let ip: std::net::IpAddr = "192.168.1.5".parse().unwrap();

    let insert = client
        .prepare("INSERT INTO nt (id, d, t, ip) VALUES ($1, $2, $3, $4)")
        .await
        .expect("prepare");
    assert_eq!(insert.params(), &[Type::INT4, Type::DATE, Type::TIME, Type::INET]);
    client.execute(&insert, &[&1i32, &date, &time, &ip]).await.expect("binary insert");
    client
        .batch_execute("INSERT INTO nt (id, d, t, ip) VALUES (2, '2026-10-08', '12:34:56.789', '192.168.1.5')")
        .await
        .expect("text insert");

    let rows = client
        .query("SELECT d, t, ip FROM nt WHERE d = $1 AND t = $2 AND ip = $3 ORDER BY id", &[&date, &time, &ip])
        .await
        .expect("select by the same values");
    assert_eq!(rows.len(), 2, "both rows match the binary-bound values");
    for row in &rows {
        assert_eq!(row.get::<_, chrono::NaiveDate>(0), date);
        assert_eq!(row.get::<_, chrono::NaiveTime>(1), time);
        assert_eq!(row.get::<_, std::net::IpAddr>(2), ip);
    }
}
