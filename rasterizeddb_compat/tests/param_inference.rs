//! Parameter types for clients that declare none at Parse.
//!
//! tokio-postgres prepares with every parameter OID left at 0 and learns the
//! types from the statement describe, then encodes each value (in binary) as
//! the type it was told. The describe used to echo the declared list — empty —
//! so the client saw a statement with no parameters and refused to bind any:
//! `Parameters(1, 0)`. The server now infers each type from where the
//! placeholder stands, as PostgreSQL does.

use postgres_types::{IsNull, ToSql, Type, to_sql_checked};
use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// A `numeric` parameter in PostgreSQL's binary form, from its decimal text.
/// (Written out here so the test needs no decimal crate's postgres feature.)
#[derive(Debug)]
struct Numeric(&'static str);

impl ToSql for Numeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let (negative, text) = match self.0.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, self.0),
        };
        let (int, frac) = text.split_once('.').unwrap_or((text, ""));
        let dscale = frac.len() as i16;
        // Pad the integer part on the left and the fraction on the right to
        // whole base-10000 digits.
        let int_pad = (4 - int.len() % 4) % 4;
        let frac_pad = (4 - frac.len() % 4) % 4;
        let digits_text = format!("{}{}{}{}", "0".repeat(int_pad), int, frac, "0".repeat(frac_pad));
        let digits: Vec<i16> = digits_text
            .as_bytes()
            .chunks(4)
            .map(|c| std::str::from_utf8(c).unwrap().parse().unwrap())
            .collect();
        let weight = ((int.len() + int_pad) / 4) as i16 - 1;
        out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&(if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        out.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            out.extend_from_slice(&d.to_be_bytes());
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

const CREATE: &str = "CREATE TABLE pi (id INT PRIMARY KEY, big BIGINT, amount NUMERIC(12,2), \
    label TEXT, ref UUID, at TIMESTAMPTZ, active BOOL, doc JSONB, blob BYTEA)";

#[tokio::test]
async fn every_common_type_binds_in_where_insert_update_and_limit() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    client.batch_execute(CREATE).await.expect("create");

    let reference = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let at = chrono::DateTime::parse_from_rfc3339("2026-10-08T12:34:56Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let doc = serde_json::json!({"k": "v", "n": [1, 2]});
    let blob: Vec<u8> = vec![0, 1, 0xab, 0xff];

    // INSERT, every column a parameter.
    let insert = client
        .prepare(
            "INSERT INTO pi (id, big, amount, label, ref, at, active, doc, blob) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .await
        .expect("prepare insert");
    let types: Vec<Type> = insert.params().to_vec();
    assert_eq!(
        types,
        vec![
            Type::INT4,
            Type::INT8,
            Type::NUMERIC,
            Type::TEXT,
            Type::UUID,
            Type::TIMESTAMPTZ,
            Type::BOOL,
            Type::JSONB,
            Type::BYTEA
        ]
    );
    for (id, big) in [(1i32, 10i64), (2, 20), (3, 30)] {
        client
            .execute(
                &insert,
                &[&id, &big, &Numeric("1234.50"), &format!("label {id}"), &reference, &at, &(id != 2), &doc, &blob],
            )
            .await
            .expect("insert");
    }

    // WHERE, a parameter per type.
    let row = client
        .query_one(
            "SELECT id, big, label, ref, at, active, doc, blob FROM pi \
             WHERE big = $1 AND label = $2 AND ref = $3 AND at = $4 AND active = $5 AND amount = $6",
            &[&20i64, &"label 2", &reference, &at, &false, &Numeric("1234.50")],
        )
        .await
        .expect("select by every type");
    assert_eq!(row.get::<_, i32>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 20);
    assert_eq!(row.get::<_, String>(2), "label 2");
    assert_eq!(row.get::<_, uuid::Uuid>(3), reference);
    assert_eq!(row.get::<_, chrono::DateTime<chrono::Utc>>(4), at);
    assert!(!row.get::<_, bool>(5));
    assert_eq!(row.get::<_, serde_json::Value>(6), doc);
    assert_eq!(row.get::<_, Vec<u8>>(7), blob);

    // IN, BETWEEN, LIKE, a parameter on the left, LIMIT and OFFSET.
    let ids: Vec<i32> = client
        .query(
            "SELECT id FROM pi WHERE id IN ($1, $2) AND big BETWEEN $3 AND $4 \
             AND label LIKE $5 AND $6 < big ORDER BY id LIMIT $7 OFFSET $8",
            &[&1i32, &3i32, &5i64, &100i64, &"label%", &0i64, &10i64, &0i64],
        )
        .await
        .expect("query")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![1, 3]);

    // UPDATE … SET col = $n WHERE …, and a cast on a placeholder.
    let changed = client
        .execute(
            "UPDATE pi SET label = $1, big = $2, doc = $3 WHERE id = $4::int4",
            &[&"renamed", &99i64, &serde_json::json!({"x": 1}), &3i32],
        )
        .await
        .expect("update");
    assert_eq!(changed, 1);
    let row = client
        .query_one("SELECT label, big, doc FROM pi WHERE id = $1", &[&3i32])
        .await
        .expect("read back");
    assert_eq!(row.get::<_, String>(0), "renamed");
    assert_eq!(row.get::<_, i64>(1), 99);
    assert_eq!(row.get::<_, serde_json::Value>(2), serde_json::json!({"x": 1}));

    // DELETE … WHERE.
    let gone = client
        .execute("DELETE FROM pi WHERE ref = $1 AND id = $2", &[&reference, &1i32])
        .await
        .expect("delete");
    assert_eq!(gone, 1);

    // Multi-row INSERT.
    client
        .execute(
            "INSERT INTO pi (id, label) VALUES ($1, $2), ($3, $4)",
            &[&10i32, &"ten", &11i32, &"eleven"],
        )
        .await
        .expect("multi-row insert");
    let n: i64 = client
        .query_one("SELECT COUNT(*) FROM pi WHERE id >= $1", &[&10i32])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 2);
}

#[tokio::test]
async fn declared_types_win_and_unknown_positions_are_text() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    client.batch_execute(CREATE).await.expect("create");
    // Declared: int8 for a column that is int4 — the declaration stands.
    let stmt = client
        .prepare_typed("SELECT id FROM pi WHERE id = $1", &[Type::INT8])
        .await
        .expect("prepare");
    assert_eq!(stmt.params(), &[Type::INT8]);
    // A placeholder with no column to take a type from resolves to text.
    let stmt = client
        .prepare("SELECT id FROM pi WHERE label = $1 OR $2 = 'x'")
        .await
        .expect("prepare");
    assert_eq!(stmt.params(), &[Type::TEXT, Type::TEXT]);
}

/// sqlx declares its types: nothing changes for it.
#[tokio::test]
async fn sqlx_is_unaffected() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("pool");
    sqlx::query(AssertSqlSafe(CREATE)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO pi (id, big, label) VALUES ($1, $2, $3)")
        .bind(1i32)
        .bind(5i64)
        .bind("x")
        .execute(&pool)
        .await
        .expect("insert");
    let (id, label): (i32, String) = sqlx::query_as("SELECT id, label FROM pi WHERE big = $1 LIMIT $2")
        .bind(5i64)
        .bind(1i64)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!((id, label.as_str()), (1, "x"));
}
