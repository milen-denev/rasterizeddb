//! Array columns of a declared element type, against PostgreSQL 17's answers.
//!
//! Every array column was untyped text underneath: `'{1,x}'` was stored in an
//! `INT[]` column, `'{01,+2}'` was kept as written, and `ORDER BY` and `max`
//! compared the stored bytes, so `{10}` sorted before `{9}` and, worse, the
//! element count came first (`{z}` before `{a,b}`). Each expectation here is
//! what PostgreSQL 17 answers for the same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), (String, String)> {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map(|_| ()).map_err(|e| {
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default();
        (code, e.to_string())
    })
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    if let Err((code, e)) = run(pool, sql).await {
        panic!("`{sql}` must succeed, failed {code}: {e}");
    }
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str, words: &str) {
    match run(pool, sql).await {
        Ok(()) => panic!("`{sql}` was accepted; PostgreSQL refuses it with {code}"),
        Err((got, e)) => {
            assert_eq!(got, code, "`{sql}`: {e}");
            assert!(e.contains(words), "`{sql}`: {e}");
        }
    }
}

/// Each row's columns as text, `NULL` for a NULL, over the simple protocol.
async fn text_rows(pool: &sqlx::PgPool, sql: &str) -> Vec<Vec<String>> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    let v: Option<String> = row.try_get_unchecked(i).expect("text");
                    v.unwrap_or_else(|| "NULL".to_string())
                })
                .collect()
        })
        .collect()
}

async fn column(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    text_rows(pool, sql).await.into_iter().map(|r| r.join("|")).collect()
}

async fn table(pool: &sqlx::PgPool) {
    ok(
        pool,
        "CREATE TABLE ta (id INT, i INT[], b BOOLEAN[], n NUMERIC[], f FLOAT8[], d DATE[], t TEXT[], \
         ts TIMESTAMP[])",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_element_the_type_refuses_is_refused() {
    let (_s, pool) = server().await;
    table(&pool).await;
    refused(&pool, "INSERT INTO ta (id, i) VALUES (1, '{1,x}')", "22P02", "invalid input syntax for type integer: \"x\"").await;
    refused(&pool, "INSERT INTO ta (id, b) VALUES (2, '{maybe}')", "22P02", "invalid input syntax for type boolean: \"maybe\"").await;
    refused(&pool, "INSERT INTO ta (id, d) VALUES (3, '{2020-13-01}')", "22008", "date/time field value out of range").await;
    refused(&pool, "INSERT INTO ta (id, i) VALUES (4, '{99999999999}')", "22003", "out of range for type integer").await;
    assert_eq!(column(&pool, "SELECT count(*) FROM ta").await, ["0"]);

    ok(&pool, "INSERT INTO ta (id, i) VALUES (5, '{5}')").await;
    refused(&pool, "UPDATE ta SET i = '{q}' WHERE id = 5", "22P02", "invalid input syntax for type integer: \"q\"").await;
    assert_eq!(column(&pool, "SELECT i FROM ta WHERE id = 5").await, ["{5}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn elements_are_kept_as_the_type_writes_them() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(
        &pool,
        "INSERT INTO ta VALUES (4, '{01,+2,NULL}', '{yes,off,NULL}', '{1.50,01.0,-0}', '{1.5,1e20,-0}', \
         '{2020-01-02}', '{b,\"a b\",NULL}', '{\"2020-01-02 03:04:05\"}')",
    )
    .await;
    assert_eq!(
        text_rows(&pool, "SELECT i, b, n, f, d, t, ts FROM ta WHERE id = 4").await,
        [["{1,2,NULL}", "{t,f,NULL}", "{1.50,1.0,0}", "{1.5,1e+20,-0}", "{2020-01-02}", "{b,\"a b\",NULL}", "{\"2020-01-02 03:04:05\"}"]]
    );
    ok(&pool, "UPDATE ta SET i = '{07, 8 }' WHERE id = 4").await;
    assert_eq!(column(&pool, "SELECT i FROM ta WHERE id = 4").await, ["{7,8}"]);
    // Equal values are equal once kept alike.
    ok(&pool, "INSERT INTO ta (id, i) VALUES (5, '{7,08}')").await;
    assert_eq!(column(&pool, "SELECT count(DISTINCT i) FROM ta WHERE id IN (4, 5)").await, ["1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn arrays_order_element_by_element_as_their_type() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(
        &pool,
        "INSERT INTO ta (id, i) VALUES (4, '{1,2,NULL}'), (5, '{10}'), (6, '{9}'), (7, '{9,1}'), (8, '{}'), \
         (9, '{NULL}'), (10, '{-3}')",
    )
    .await;
    ok(&pool, "INSERT INTO ta (id, t) VALUES (11, '{z}'), (12, '{a,b}'), (13, '{10}'), (14, '{9}')").await;
    assert_eq!(
        column(&pool, "SELECT id FROM ta WHERE i IS NOT NULL ORDER BY i, id").await,
        ["8", "10", "4", "6", "7", "5", "9"]
    );
    assert_eq!(column(&pool, "SELECT id FROM ta WHERE t IS NOT NULL ORDER BY t, id").await, ["13", "14", "12", "11"]);
    assert_eq!(text_rows(&pool, "SELECT max(i), min(i) FROM ta").await, [["{NULL}", "{}"]]);
    assert_eq!(text_rows(&pool, "SELECT max(t), min(t) FROM ta").await, [["{z}", "{10}"]]);
    assert_eq!(column(&pool, "SELECT id FROM ta WHERE i IS NOT NULL ORDER BY i DESC, id LIMIT 2").await, ["9", "5"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn array_agg_orders_as_its_element_type() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE g (k INT, v INT)").await;
    ok(&pool, "INSERT INTO g VALUES (1, 10), (2, 9), (3, 100)").await;
    assert_eq!(
        column(&pool, "SELECT x FROM (SELECT array_agg(v) AS x FROM g GROUP BY k) s ORDER BY x").await,
        ["{9}", "{10}", "{100}"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_array_is_described_and_sent_as_its_type() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(&pool, "INSERT INTO ta (id, i, b, n, t) VALUES (1, '{01,2}', '{yes,NULL}', '{1.50,-2}', '{x,NULL}')").await;
    // sqlx asks for binary results and checks each column's described type
    // against the Rust type: `integer[]` decodes as Vec<i32>, and so on.
    let row = sqlx::query("SELECT i, b, n, t FROM ta WHERE id = 1").fetch_one(&pool).await.expect("select");
    let i: Vec<i32> = row.try_get(0).expect("integer[] as Vec<i32>");
    let b: Vec<Option<bool>> = row.try_get(1).expect("boolean[] as Vec<Option<bool>>");
    let n: Vec<rust_decimal::Decimal> = row.try_get(2).expect("numeric[] as Vec<Decimal>");
    let t: Vec<Option<String>> = row.try_get(3).expect("text[] as Vec<Option<String>>");
    assert_eq!(i, [1, 2]);
    assert_eq!(b, [Some(true), None]);
    assert_eq!(n.iter().map(|d| d.to_string()).collect::<Vec<_>>(), ["1.50", "-2"]);
    assert_eq!(t, [Some("x".to_string()), None]);
    // The element OID in the bytes is the described type's.
    for (at, oid) in [(0usize, 23i32), (1, 16), (2, 1700), (3, 25)] {
        let raw = row.try_get_raw(at).expect("raw");
        let bytes = raw.as_bytes().expect("binary bytes");
        assert_eq!(&bytes[8..12], &oid.to_be_bytes(), "column {at}'s element OID");
    }
    // The same column through a join, RETURNING and a derived table.
    ok(&pool, "CREATE TABLE other (id INT, label TEXT)").await;
    ok(&pool, "INSERT INTO other VALUES (1, 'one')").await;
    let joined: (Vec<i32>, String) =
        sqlx::query_as("SELECT ta.i, other.label FROM ta JOIN other ON ta.id = other.id").fetch_one(&pool).await.expect("join");
    assert_eq!(joined, (vec![1, 2], "one".to_string()));
    let returned: (Vec<i32>,) = sqlx::query_as("UPDATE ta SET i = '{7,8}' WHERE id = 1 RETURNING i").fetch_one(&pool).await.expect("returning");
    assert_eq!(returned.0, [7, 8]);
    let derived: (Vec<i32>,) = sqlx::query_as("SELECT x FROM (SELECT i AS x FROM ta) s").fetch_one(&pool).await.expect("derived");
    assert_eq!(derived.0, [7, 8]);
    // A parameter bound as integer[] in binary.
    ok(&pool, "CREATE TABLE pb (xs INT[])").await;
    sqlx::query("INSERT INTO pb VALUES ($1)").bind(vec![3i32, 4]).execute(&pool).await.expect("bind int[]");
    let back: (Vec<i32>,) = sqlx::query_as("SELECT xs FROM pb").fetch_one(&pool).await.expect("read back");
    assert_eq!(back.0, [3, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_element_type_survives_a_restart() {
    let root = std::env::temp_dir().join(format!("typed_arrays_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    {
        let server = TestServer::start_file_backed(&root_text).await.expect("server");
        let pool = server.connect_sqlx().await.expect("should connect");
        table(&pool).await;
        ok(&pool, "INSERT INTO ta (id, i) VALUES (1, '{2}')").await;
        pool.close().await;
    }
    let server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("should connect");
    refused(&pool, "INSERT INTO ta (id, i) VALUES (2, '{y}')", "22P02", "invalid input syntax for type integer").await;
    ok(&pool, "INSERT INTO ta (id, i) VALUES (3, '{+10}')").await;
    assert_eq!(column(&pool, "SELECT i FROM ta ORDER BY i").await, ["{2}", "{10}"]);
    pool.close().await;
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_added_typed_column_is_typed_too() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE ad (id INT)").await;
    ok(&pool, "ALTER TABLE ad ADD COLUMN xs BIGINT[]").await;
    refused(&pool, "INSERT INTO ad VALUES (1, '{1.5}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "INSERT INTO ad VALUES (2, '{-007}')").await;
    assert_eq!(column(&pool, "SELECT xs FROM ad").await, ["{-7}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_element_type_follows_the_column_through_ddl() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE lf (id INT, a INT[], b INT)").await;
    // A rename keeps it.
    ok(&pool, "ALTER TABLE lf RENAME COLUMN a TO aa").await;
    refused(&pool, "INSERT INTO lf (id, aa) VALUES (1, '{x}')", "22P02", "invalid input syntax for type integer").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (2, '{02}')").await;
    assert_eq!(column(&pool, "SELECT aa FROM lf").await, ["{2}"]);
    // A column added under a dropped one's name has its own type only.
    ok(&pool, "ALTER TABLE lf DROP COLUMN aa").await;
    ok(&pool, "ALTER TABLE lf ADD COLUMN aa TEXT[]").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (3, '{x}')").await;
    // An untyped array column with rows is not read again as another type
    // without `USING`, as in PostgreSQL.
    refused(&pool, "ALTER TABLE lf ALTER COLUMN aa TYPE INT[]", "42804", "cannot be cast automatically").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (4, '{y}')").await;
    // On an empty table the element type changes with the declaration.
    ok(&pool, "CREATE TABLE em (x INT[])").await;
    ok(&pool, "ALTER TABLE em ALTER COLUMN x TYPE BIGINT[]").await;
    refused(&pool, "INSERT INTO em VALUES ('{1.5}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "ALTER TABLE em ALTER COLUMN x TYPE TEXT[]").await;
    ok(&pool, "INSERT INTO em VALUES ('{1.5}')").await;
}

/// `ALTER COLUMN … TYPE` between array types on a table with rows, as
/// PostgreSQL 17 converts it: every element read again as the new type, all
/// or nothing, and the column ordered as the new type afterwards. It was
/// refused (0A000), and before that recorded without converting the rows.
#[tokio::test(flavor = "multi_thread")]
async fn an_array_column_with_rows_converts_to_another_element_type() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE r1 (x INT[])").await;
    ok(&pool, "INSERT INTO r1 VALUES ('{10}'), ('{9}'), ('{1,2}')").await;
    ok(&pool, "ALTER TABLE r1 ALTER COLUMN x TYPE BIGINT[]").await;
    assert_eq!(column(&pool, "SELECT x FROM r1 ORDER BY x").await, ["{1,2}", "{9}", "{10}"]);
    let wide: (Vec<i64>,) = sqlx::query_as("SELECT x FROM r1 ORDER BY x LIMIT 1").fetch_one(&pool).await.expect("bigint[]");
    assert_eq!(wide.0, [1, 2]);
    // To text[]: the same text, ordered as text now.
    ok(&pool, "ALTER TABLE r1 ALTER COLUMN x TYPE TEXT[]").await;
    assert_eq!(column(&pool, "SELECT x FROM r1 ORDER BY x").await, ["{1,2}", "{10}", "{9}"]);
    ok(&pool, "INSERT INTO r1 VALUES ('{y}')").await;

    // From untyped: PostgreSQL asks for USING, and converts with it.
    ok(&pool, "CREATE TABLE r2 (t TEXT[])").await;
    ok(&pool, "INSERT INTO r2 VALUES ('{10}'), ('{09}'), ('{+3}')").await;
    refused(&pool, "ALTER TABLE r2 ALTER COLUMN t TYPE INT[]", "42804", "cannot be cast automatically to type integer[]").await;
    ok(&pool, "ALTER TABLE r2 ALTER COLUMN t TYPE INT[] USING t::int[]").await;
    assert_eq!(column(&pool, "SELECT t FROM r2 ORDER BY t").await, ["{3}", "{9}", "{10}"]);
    refused(&pool, "INSERT INTO r2 VALUES ('{z}')", "22P02", "invalid input syntax for type integer: \"z\"").await;

    // An element the new type refuses refuses the whole change.
    ok(&pool, "CREATE TABLE r3 (t TEXT[])").await;
    ok(&pool, "INSERT INTO r3 VALUES ('{1,x}')").await;
    refused(&pool, "ALTER TABLE r3 ALTER COLUMN t TYPE INT[] USING t::int[]", "22P02", "invalid input syntax for type integer: \"x\"").await;
    assert_eq!(column(&pool, "SELECT t FROM r3").await, ["{1,x}"]);
    ok(&pool, "INSERT INTO r3 VALUES ('{still,text}')").await;
    ok(&pool, "CREATE TABLE r4 (b BIGINT[])").await;
    ok(&pool, "INSERT INTO r4 VALUES ('{99999999999}')").await;
    refused(&pool, "ALTER TABLE r4 ALTER COLUMN b TYPE INT[]", "22003", "out of range for type integer").await;
    assert_eq!(column(&pool, "SELECT b FROM r4").await, ["{99999999999}"]);
    // A USING that computes is not supported for arrays yet, and says so.
    refused(&pool, "ALTER TABLE r3 ALTER COLUMN t TYPE INT[] USING ARRAY[1]", "0A000", "not supported yet").await;
}

/// An untyped array column holds text: an array of integers written into it
/// (`array_agg`) is relabelled text's and orders as text, as `text[]` does.
#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_array_column_orders_what_it_holds_as_text() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE src (k INT, v INT)").await;
    ok(&pool, "INSERT INTO src VALUES (1, 10), (2, 9)").await;
    ok(&pool, "CREATE TABLE tx (t TEXT[])").await;
    ok(&pool, "INSERT INTO tx SELECT array_agg(v) FROM src GROUP BY k").await;
    assert_eq!(column(&pool, "SELECT t FROM tx ORDER BY t").await, ["{10}", "{9}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_keeps_the_element_type() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE src (x BIGINT[])").await;
    ok(&pool, "CREATE TABLE empty_copy AS SELECT x FROM src").await;
    refused(&pool, "INSERT INTO empty_copy VALUES ('{z}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "INSERT INTO src VALUES ('{10}'), ('{9}')").await;
    ok(&pool, "CREATE TABLE full_copy AS SELECT x FROM src").await;
    assert_eq!(column(&pool, "SELECT x FROM full_copy ORDER BY x").await, ["{9}", "{10}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_column_keeps_its_precision() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE pr (ts TIMESTAMP(0))").await;
    ok(&pool, "ALTER TABLE pr RENAME COLUMN ts TO t2").await;
    ok(&pool, "INSERT INTO pr VALUES ('2020-01-01 00:00:05.7')").await;
    assert_eq!(column(&pool, "SELECT t2 FROM pr").await, ["2020-01-01 00:00:06"]);
}

/// Every shape a typed array column is read through, in binary: the
/// description (`integer[]`) and the cells come from two computations on the
/// extended protocol, and where they disagreed a client decoded the stored
/// text elements as integers (`{7,8}` read as `[55, 56]`) — no error, wrong
/// numbers. Each shape is decoded as Vec<i32> and compared.
#[tokio::test(flavor = "multi_thread")]
async fn every_query_shape_sends_a_typed_array_as_described() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE sh (id INT PRIMARY KEY, i INT[], g INT)").await;
    ok(&pool, "INSERT INTO sh VALUES (1, '{7,8}', 1), (2, '{9}', 1), (3, '{10,0}', 2)").await;
    ok(&pool, "CREATE TABLE sh2 (id INT, label TEXT)").await;
    ok(&pool, "INSERT INTO sh2 VALUES (1, 'one')").await;
    let one = |v: &[i32]| vec![v.to_vec()];
    let cases: Vec<(&str, Vec<Vec<i32>>)> = vec![
        ("SELECT i FROM sh WHERE id = 1", one(&[7, 8])),
        ("SELECT i FROM sh ORDER BY i", vec![vec![7, 8], vec![9], vec![10, 0]]),
        ("SELECT i FROM sh ORDER BY id LIMIT 1", one(&[7, 8])),
        ("SELECT i AS renamed FROM sh WHERE id = 2", one(&[9])),
        ("SELECT id, i FROM sh WHERE id = 3", one(&[10, 0])),
        ("SELECT max(i) FROM sh", one(&[10, 0])),
        ("SELECT min(i) FROM sh WHERE g = 1", one(&[7, 8])),
        ("SELECT i FROM sh GROUP BY i ORDER BY i DESC LIMIT 1", one(&[10, 0])),
        ("SELECT DISTINCT i FROM sh WHERE id = 1", one(&[7, 8])),
        ("SELECT sh.i FROM sh JOIN sh2 ON sh.id = sh2.id", one(&[7, 8])),
        ("SELECT s.i FROM sh2 JOIN sh s ON s.id = sh2.id", one(&[7, 8])),
        ("WITH c AS (SELECT i FROM sh WHERE id = 1) SELECT i FROM c", one(&[7, 8])),
        ("SELECT x FROM (SELECT i AS x FROM sh WHERE id = 2) s", one(&[9])),
        ("SELECT i FROM sh WHERE id = 1 UNION ALL SELECT i FROM sh WHERE id = 2", vec![vec![7, 8], vec![9]]),
        ("SELECT i FROM sh WHERE id IN (SELECT id FROM sh2)", one(&[7, 8])),
        ("UPDATE sh SET g = 5 WHERE id = 1 RETURNING i", one(&[7, 8])),
        ("INSERT INTO sh VALUES (4, '{4}', 3) RETURNING i", one(&[4])),
        ("DELETE FROM sh WHERE id = 4 RETURNING i", one(&[4])),
    ];
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        let rows = match sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&pool).await {
            Ok(rows) => rows,
            Err(e) => {
                wrong.push(format!("{sql}: {e}"));
                continue;
            }
        };
        let col = if sql.starts_with("SELECT id, i") { 1 } else { 0 };
        let got: Result<Vec<Vec<i32>>, String> =
            rows.iter().map(|r| r.try_get::<Vec<i32>, _>(col).map_err(|e| e.to_string())).collect();
        match got {
            Ok(got) if got == want => {}
            Ok(got) => wrong.push(format!("{sql}: got {got:?}, want {want:?}")),
            Err(e) => wrong.push(format!("{sql}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // An expression over one has no element type here: described `text[]`
    // (PostgreSQL says `integer[]`), and sent as `text[]` to match, so a
    // client asking for integers is refused rather than misled.
    let row = sqlx::query("SELECT COALESCE(i, '{}') FROM sh WHERE id = 2").fetch_one(&pool).await.expect("coalesce");
    assert!(row.try_get::<Vec<i32>, _>(0).is_err());
    assert_eq!(row.try_get::<Vec<String>, _>(0).expect("as text[]"), ["9"]);
}
