//! `UBIGINT`, `HUGEINT` and `UHUGEINT` are `numeric` on the wire.
//!
//! PostgreSQL uses `numeric` for any integer wider than `bigint`, and so does
//! this server: advertised as `int8`, a `UBIGINT` above `i64::MAX` reached a
//! binary client as a negative number, and a 128-bit integer went out as text
//! under a text OID. Each extreme here is written and read back over both
//! protocols. The binary payload is decoded by an independent decoder (the
//! one in `binary_result_types.rs`), and bound parameters are encoded by an
//! independent encoder, so the server's codec is not checking itself.

use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use rasterizeddb_compat::TestServer;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
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

/// Decodes PostgreSQL's binary `numeric` to a plain decimal string.
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
            while (frac.len() as i32) < (-position - 1) * 4 {
                frac.push_str("0000");
            }
            frac.push_str(&group);
        }
    }
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

/// A decimal string as PostgreSQL's binary `numeric`, as a client sends it.
#[derive(Debug)]
struct Num(&'static str);

impl ToSql for Num {
    fn to_sql(&self, _: &Type, out: &mut bytes::BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let (negative, body) = match self.0.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, self.0),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        // Integer digits padded on the left, fraction on the right, to whole
        // base-10000 groups.
        let int_pad = (4 - int.len() % 4) % 4;
        let int_digits = format!("{}{}", "0".repeat(int_pad), int);
        let frac_pad = (4 - frac.len() % 4) % 4;
        let frac_digits = format!("{}{}", frac, "0".repeat(frac_pad));
        let mut groups: Vec<i16> = int_digits
            .as_bytes()
            .chunks(4)
            .chain(frac_digits.as_bytes().chunks(4))
            .map(|c| std::str::from_utf8(c).unwrap().parse().unwrap())
            .collect();
        let mut weight = (int_digits.len() / 4) as i16 - 1;
        while groups.first() == Some(&0) && groups.len() > 1 {
            groups.remove(0);
            weight -= 1;
        }
        while groups.last() == Some(&0) && !groups.is_empty() {
            groups.pop();
        }
        out.extend_from_slice(&(groups.len() as i16).to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&(if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        out.extend_from_slice(&(frac.len() as i16).to_be_bytes());
        for g in groups {
            out.extend_from_slice(&g.to_be_bytes());
        }
        Ok(IsNull::No)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    to_sql_checked!();
}

const U64_MAX: &str = "18446744073709551615";
const I128_MIN: &str = "-170141183460469231731687303715884105728";
const I128_MAX: &str = "170141183460469231731687303715884105727";
const U128_MAX: &str = "340282366920938463463374607431768211455";

async fn text_row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    client
        .simple_query(sql)
        .await
        .expect("text read")
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            _ => None,
        })
        .expect("a row")
}

async fn binary_row(client: &tokio_postgres::Client, sql: &str) -> (Vec<Type>, Vec<String>) {
    let row = client.query_one(sql, &[]).await.expect("binary read");
    let types = row.columns().iter().map(|c| c.type_().clone()).collect();
    let values = (0..row.len()).map(|i| numeric_text(&row.get::<_, RawOut>(i).0)).collect();
    (types, values)
}

#[tokio::test]
async fn the_extremes_round_trip_as_numeric_in_text_and_binary() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    client
        .batch_execute("CREATE TABLE wide (id INT PRIMARY KEY, u UBIGINT, h HUGEINT, uh UHUGEINT)")
        .await
        .expect("create");
    client
        .batch_execute(&format!(
            "INSERT INTO wide (id, u, h, uh) VALUES (1, {U64_MAX}, {I128_MIN}, {U128_MAX}); \
             INSERT INTO wide (id, u, h, uh) VALUES (2, 0, {I128_MAX}, 0)"
        ))
        .await
        .expect("insert literals");

    for (id, want) in [(1, [U64_MAX, I128_MIN, U128_MAX]), (2, ["0", I128_MAX, "0"])] {
        let sql = format!("SELECT u, h, uh FROM wide WHERE id = {id}");
        let text = text_row(&client, &sql).await;
        assert_eq!(text, want.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>(), "text, id {id}");
        let (types, binary) = binary_row(&client, &sql).await;
        assert_eq!(types, vec![Type::NUMERIC; 3], "advertised types");
        assert_eq!(binary, want.to_vec(), "binary, id {id}");
    }

    // A prepared statement's describe says the same as the row it returns.
    let prepared = client.prepare("SELECT u, h, uh FROM wide").await.expect("prepare");
    let described: Vec<Type> = prepared.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(described, vec![Type::NUMERIC; 3]);

    // The same through a CTE, which materialises the columns into a
    // temporary table of its own.
    let sql = "WITH w AS (SELECT u, h, uh FROM wide WHERE id = 1) SELECT u, h, uh FROM w";
    let (types, binary) = binary_row(&client, sql).await;
    assert_eq!(types, vec![Type::NUMERIC; 3], "CTE types");
    assert_eq!(binary, vec![U64_MAX, I128_MIN, U128_MAX], "CTE values");

    // MIN/MAX keep the argument's type; SUM was already `numeric`.
    let (types, binary) = binary_row(&client, "SELECT MAX(u), MIN(h), MAX(uh) FROM wide").await;
    assert_eq!(types, vec![Type::NUMERIC; 3], "aggregate types");
    assert_eq!(binary, vec![U64_MAX, I128_MIN, U128_MAX], "aggregate values");

    // information_schema agrees with the wire.
    let rows = client
        .simple_query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'wide' ORDER BY ordinal_position",
        )
        .await
        .expect("information_schema");
    let types: Vec<(String, String)> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((r.get(0).unwrap().to_string(), r.get(1).unwrap().to_string()))
            }
            _ => None,
        })
        .collect();
    for col in ["u", "h", "uh"] {
        assert!(
            types.contains(&(col.to_string(), "numeric".to_string())),
            "{col} in {types:?}"
        );
    }
}

#[tokio::test]
async fn numeric_parameters_bind_the_full_range_and_refuse_what_does_not_fit() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    client
        .batch_execute("CREATE TABLE wp (id INT PRIMARY KEY, u UBIGINT, h HUGEINT, uh UHUGEINT)")
        .await
        .expect("create");

    let insert = client
        .prepare("INSERT INTO wp (id, u, h, uh) VALUES ($1, $2, $3, $4)")
        .await
        .expect("prepare insert");
    assert_eq!(insert.params(), &[Type::INT4, Type::NUMERIC, Type::NUMERIC, Type::NUMERIC]);

    client
        .execute(&insert, &[&1i32, &Num(U64_MAX), &Num(I128_MIN), &Num(U128_MAX)])
        .await
        .expect("bind the maxima");
    client
        .execute(&insert, &[&2i32, &Num("0"), &Num(I128_MAX), &Num("0")])
        .await
        .expect("bind zero and i128::MAX");

    for (id, want) in [(1, [U64_MAX, I128_MIN, U128_MAX]), (2, ["0", I128_MAX, "0"])] {
        let sql = format!("SELECT u, h, uh FROM wp WHERE id = {id}");
        let text = text_row(&client, &sql).await;
        assert_eq!(text, want.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>(), "id {id}");
        let (_, binary) = binary_row(&client, &sql).await;
        assert_eq!(binary, want.to_vec(), "binary, id {id}");
    }

    // A numeric parameter in a predicate finds the row.
    let select = client.prepare("SELECT id FROM wp WHERE u = $1").await.expect("prepare select");
    assert_eq!(select.params(), &[Type::NUMERIC]);
    let rows = client.query(&select, &[&Num(U64_MAX)]).await.expect("select by u64::MAX");
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), vec![1]);

    // Out of range is 22003, a fraction is 22P02, and neither stores a row.
    let refused: [(&str, [Num; 3], &str); 6] = [
        ("u64::MAX + 1", [Num("18446744073709551616"), Num("0"), Num("0")], "22003"),
        ("negative UBIGINT", [Num("-1"), Num("0"), Num("0")], "22003"),
        ("fractional UBIGINT", [Num("1.5"), Num("0"), Num("0")], "22P02"),
        ("i128::MAX + 1", [Num("0"), Num("170141183460469231731687303715884105728"), Num("0")], "22003"),
        ("u128::MAX + 1", [Num("0"), Num("0"), Num("340282366920938463463374607431768211456")], "22003"),
        ("fractional UHUGEINT", [Num("0"), Num("0"), Num("2.25")], "22P02"),
    ];
    let mut failures = Vec::new();
    for (n, (label, [u, h, uh], code)) in refused.iter().enumerate() {
        let id = 100 + n as i32;
        match client.execute(&insert, &[&id, u, h, uh]).await {
            Ok(_) => failures.push(format!("{label}: accepted")),
            Err(e) => {
                let got = e.code().map(|c| c.code().to_string()).unwrap_or_default();
                if got != *code {
                    failures.push(format!("{label}: {got} ({e:?}), expected {code}"));
                }
            }
        }
        let left = client
            .query(&format!("SELECT id FROM wp WHERE id = {id}"), &[])
            .await
            .expect("check");
        if !left.is_empty() {
            failures.push(format!("{label}: a row was stored"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
