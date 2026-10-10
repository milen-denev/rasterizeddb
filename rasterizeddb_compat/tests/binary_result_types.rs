//! Binary-format *results* for every type the engine returns.
//!
//! A client that asks for binary results must get PostgreSQL's binary
//! encoding under the advertised OID — never the text rendering under a binary
//! format code, which is what `INET` used to send (tokio-postgres then failed
//! with "error deserializing column"). Each value here is inserted as a text
//! literal and read back both ways: the text protocol's rendering, and the
//! binary payload, which is checked against the bytes PostgreSQL's own send
//! function produces for that value — or, where tokio-postgres has a native
//! type, decoded natively and compared with the text rendering parsed by the
//! same type.

use postgres_types::{FromSql, Type};
use rasterizeddb_compat::TestServer;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// A result column as its binary wire bytes, whatever its type.
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

fn range(flags: u8, bounds: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![flags];
    for b in bounds {
        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
        out.extend_from_slice(b);
    }
    out
}

fn pg_days(y: i32, m: u32, d: u32) -> i32 {
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
    (date - chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()).num_days() as i32
}

/// Decodes PostgreSQL's binary `numeric` to a plain decimal string with no
/// trailing fractional zeros, independently of the server's codec.
fn numeric_text(raw: &[u8]) -> String {
    let be16 = |at: usize| i16::from_be_bytes([raw[at], raw[at + 1]]);
    let ndigits = be16(0) as usize;
    let weight = be16(2) as i32;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let digits: Vec<i16> = (0..ndigits).map(|i| be16(8 + 2 * i)).collect();
    let mut int = String::new();
    let mut frac = String::new();
    for (i, d) in digits.iter().enumerate() {
        let position = weight - i as i32;
        let group = format!("{:04}", d);
        if position >= 0 {
            int.push_str(&group);
        } else {
            // Leading zero groups between the point and this digit.
            while (frac.len() as i32) < (-position - 1) * 4 {
                frac.push_str("0000");
            }
            frac.push_str(&group);
        }
    }
    // Integer groups the digits stop short of.
    for _ in 0..(weight + 1 - digits.len() as i32).max(0) {
        int.push_str("0000");
    }
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(if int.is_empty() { "0" } else { int });
    if !frac.is_empty() {
        out.push('.');
        out.push_str(frac);
    }
    out
}

fn trim_decimal(text: &str) -> String {
    if !text.contains('.') {
        return text.to_string();
    }
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

enum Expect {
    /// PostgreSQL's binary encoding of the value, exactly.
    Bytes(Vec<u8>),
    /// A numeric: the binary payload must decode to the text rendering.
    Numeric,
    /// Decode natively; compare with the text parsed by the same type.
    Native(fn(&tokio_postgres::Row, &str) -> Result<(), String>),
}

struct Case {
    column: &'static str,
    literal: &'static str,
    ty: Type,
    expect: Expect,
}

fn native<T>(row: &tokio_postgres::Row, text: &str) -> Result<(), String>
where
    T: for<'a> FromSql<'a> + std::str::FromStr + PartialEq + std::fmt::Debug,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let binary: T = row.try_get(0).map_err(|e| format!("binary decode: {e}"))?;
    let parsed: T = text.parse().map_err(|e| format!("text {text:?} does not parse: {e:?}"))?;
    if binary == parsed {
        Ok(())
    } else {
        Err(format!("binary {binary:?} vs text {parsed:?}"))
    }
}

fn cases() -> Vec<Case> {
    use Expect::*;
    vec![
        Case { column: "SMALLINT", literal: "-12", ty: Type::INT2, expect: Native(native::<i16>) },
        Case { column: "INT", literal: "-123456", ty: Type::INT4, expect: Native(native::<i32>) },
        Case { column: "BIGINT", literal: "-9000000000", ty: Type::INT8, expect: Native(native::<i64>) },
        Case { column: "TINYINT", literal: "-7", ty: Type::INT2, expect: Native(native::<i16>) },
        Case { column: "UTINYINT", literal: "200", ty: Type::INT8, expect: Native(native::<i64>) },
        Case { column: "USMALLINT", literal: "60000", ty: Type::INT8, expect: Native(native::<i64>) },
        Case { column: "UINT", literal: "4000000000", ty: Type::INT8, expect: Native(native::<i64>) },
        // Wider than `bigint`, so `numeric` at scale 0, as in PostgreSQL;
        // the extremes are in `wide_integer_numeric.rs`.
        Case { column: "UBIGINT", literal: "9000000000", ty: Type::NUMERIC, expect: Numeric },
        Case { column: "REAL", literal: "1.5", ty: Type::FLOAT4, expect: Native(native::<f32>) },
        Case { column: "DOUBLE PRECISION", literal: "-2.25", ty: Type::FLOAT8, expect: Native(native::<f64>) },
        Case { column: "BOOLEAN", literal: "true", ty: Type::BOOL, expect: Bytes(vec![1]) },
        Case { column: "TEXT", literal: "héllo, wörld", ty: Type::TEXT, expect: Native(native::<String>) },
        Case { column: "VARCHAR(20)", literal: "abc", ty: Type::TEXT, expect: Native(native::<String>) },
        Case { column: "BYTEA", literal: "\\x00ab10ff", ty: Type::BYTEA, expect: Bytes(vec![0x00, 0xab, 0x10, 0xff]) },
        Case {
            column: "UUID",
            literal: "11111111-2222-3333-4444-555555555555",
            ty: Type::UUID,
            expect: Native(native::<uuid::Uuid>),
        },
        Case {
            column: "TIMESTAMPTZ",
            literal: "2026-10-08 12:34:56.789",
            ty: Type::TIMESTAMPTZ,
            expect: Bytes(
                ((pg_days(2026, 10, 8) as i64 * 86_400 + 45_296) * 1_000_000 + 789_000)
                    .to_be_bytes()
                    .to_vec(),
            ),
        },
        Case { column: "NUMERIC(10,2)", literal: "1234.50", ty: Type::NUMERIC, expect: Numeric },
        Case { column: "NUMERIC(38,16)", literal: "-1.0000000000000001", ty: Type::NUMERIC, expect: Numeric },
        Case { column: "NUMERIC(12,0)", literal: "120000", ty: Type::NUMERIC, expect: Numeric },
        Case { column: "NUMERIC", literal: "0.000123", ty: Type::NUMERIC, expect: Numeric },
        Case {
            column: "JSONB",
            literal: "{\"k\": [1, \"two\"]}",
            ty: Type::JSONB,
            expect: Native(|row, text| {
                let binary: serde_json::Value = row.try_get(0).map_err(|e| e.to_string())?;
                let parsed: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
                (binary == parsed).then_some(()).ok_or(format!("{binary} vs {parsed}"))
            }),
        },
        Case {
            column: "TEXT[]",
            literal: "{a,\"b c\",NULL}",
            ty: Type::TEXT_ARRAY,
            expect: Native(|row, text| {
                let binary: Vec<Option<String>> = row.try_get(0).map_err(|e| e.to_string())?;
                let want = vec![Some("a".to_string()), Some("b c".to_string()), None];
                (binary == want && text.starts_with('{'))
                    .then_some(())
                    .ok_or(format!("{binary:?} from text {text}"))
            }),
        },
        Case { column: "DATE", literal: "2026-10-08", ty: Type::DATE, expect: Native(native::<chrono::NaiveDate>) },
        Case { column: "TIME", literal: "12:34:56.789", ty: Type::TIME, expect: Native(native::<chrono::NaiveTime>) },
        Case {
            column: "TIMETZ",
            literal: "12:34:56+02",
            ty: Type::TIMETZ,
            expect: Bytes([45_296_000_000i64.to_be_bytes().to_vec(), (-7200i32).to_be_bytes().to_vec()].concat()),
        },
        Case {
            column: "INTERVAL",
            literal: "1 year 2 mons 4 days 01:02:03",
            ty: Type::INTERVAL,
            expect: Bytes(
                [3_723_000_000i64.to_be_bytes().to_vec(), 4i32.to_be_bytes().to_vec(), 14i32.to_be_bytes().to_vec()]
                    .concat(),
            ),
        },
        Case { column: "INET", literal: "192.168.1.5", ty: Type::INET, expect: Native(native::<std::net::IpAddr>) },
        Case { column: "INET", literal: "10.0.0.0/8", ty: Type::INET, expect: Bytes(vec![2, 8, 0, 4, 10, 0, 0, 0]) },
        Case { column: "MACADDR", literal: "08:00:2b:01:02:03", ty: Type::MACADDR, expect: Bytes(vec![8, 0, 0x2b, 1, 2, 3]) },
        Case {
            column: "MACADDR8",
            literal: "08:00:2b:01:02:03:04:05",
            ty: Type::MACADDR8,
            expect: Bytes(vec![8, 0, 0x2b, 1, 2, 3, 4, 5]),
        },
        Case { column: "BIT(4)", literal: "1010", ty: Type::BIT, expect: Bytes([4i32.to_be_bytes().to_vec(), vec![0xA0]].concat()) },
        Case { column: "VARBIT", literal: "10110", ty: Type::VARBIT, expect: Bytes([5i32.to_be_bytes().to_vec(), vec![0xB0]].concat()) },
        Case { column: "POINT", literal: "(1.5,-2)", ty: Type::POINT, expect: Bytes(f64s(&[1.5, -2.0])) },
        Case { column: "LSEG", literal: "[(1,2),(3,4)]", ty: Type::LSEG, expect: Bytes(f64s(&[1.0, 2.0, 3.0, 4.0])) },
        Case { column: "BOX", literal: "(1,2),(3,4)", ty: Type::BOX, expect: Bytes(f64s(&[3.0, 4.0, 1.0, 2.0])) },
        Case { column: "LINE", literal: "{1,-1,0}", ty: Type::LINE, expect: Bytes(f64s(&[1.0, -1.0, 0.0])) },
        Case { column: "CIRCLE", literal: "<(1,2),3>", ty: Type::CIRCLE, expect: Bytes(f64s(&[1.0, 2.0, 3.0])) },
        Case {
            column: "PATH",
            literal: "[(0,0),(1,1)]",
            ty: Type::PATH,
            expect: Bytes([vec![0u8], 2i32.to_be_bytes().to_vec(), f64s(&[0.0, 0.0, 1.0, 1.0])].concat()),
        },
        Case {
            column: "POLYGON",
            literal: "((0,0),(1,1),(1,0))",
            ty: Type::POLYGON,
            expect: Bytes([3i32.to_be_bytes().to_vec(), f64s(&[0.0, 0.0, 1.0, 1.0, 1.0, 0.0])].concat()),
        },
        Case {
            column: "INT4RANGE",
            literal: "[1,10)",
            ty: Type::INT4_RANGE,
            expect: Bytes(range(0x02, &[&1i32.to_be_bytes(), &10i32.to_be_bytes()])),
        },
        Case {
            column: "INT8RANGE",
            literal: "[5,)",
            ty: Type::INT8_RANGE,
            expect: Bytes(range(0x12, &[&5i64.to_be_bytes()])),
        },
        Case {
            column: "DATERANGE",
            literal: "[2026-01-01,2026-02-01)",
            ty: Type::DATE_RANGE,
            expect: Bytes(range(0x02, &[&pg_days(2026, 1, 1).to_be_bytes(), &pg_days(2026, 2, 1).to_be_bytes()])),
        },
        Case { column: "INT4RANGE", literal: "empty", ty: Type::INT4_RANGE, expect: Bytes(vec![0x01]) },
    ]
}

#[tokio::test]
async fn every_type_reads_back_the_same_in_binary_and_in_text() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    let mut failures = Vec::new();

    for (n, case) in cases().into_iter().enumerate() {
        let table = format!("br{n}");
        if let Err(e) = client
            .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, v {})", case.column))
            .await
        {
            failures.push(format!("{}: create: {e}", case.column));
            continue;
        }
        if let Err(e) = client
            .batch_execute(&format!(
                "INSERT INTO {table} (id, v) VALUES (1, '{}')",
                case.literal.replace('\'', "''")
            ))
            .await
        {
            failures.push(format!("{}: insert: {e}", case.column));
            continue;
        }

        let text = client
            .simple_query(&format!("SELECT v FROM {table}"))
            .await
            .expect("text read")
            .into_iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .unwrap_or_default();

        // tokio-postgres asks for every result column in binary.
        let row = match client.query_one(&format!("SELECT v FROM {table}"), &[]).await {
            Ok(row) => row,
            Err(e) => {
                failures.push(format!("{}: binary read: {e:?}", case.column));
                continue;
            }
        };
        let advertised = row.columns()[0].type_().clone();
        if advertised != case.ty {
            failures.push(format!("{}: advertised {advertised:?}, expected {:?}", case.column, case.ty));
            continue;
        }
        let raw = row.get::<_, RawOut>(0).0;
        let outcome = match &case.expect {
            Expect::Bytes(want) => (raw == *want)
                .then_some(())
                .ok_or(format!("binary {raw:02x?}, PostgreSQL sends {want:02x?} (text {text:?})")),
            Expect::Numeric => {
                let decoded = numeric_text(&raw);
                (decoded == trim_decimal(&text))
                    .then_some(())
                    .ok_or(format!("binary decodes to {decoded}, text is {text}"))
            }
            Expect::Native(check) => check(&row, &text),
        };
        if let Err(e) = outcome {
            failures.push(format!("{} ({}): {e}", case.column, case.literal));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
