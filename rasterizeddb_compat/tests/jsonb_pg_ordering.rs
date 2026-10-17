//! `jsonb` ordered, grouped and matched as PostgreSQL 17 orders, groups and
//! matches it.
//!
//! Every expected answer below was produced by running the same statement
//! over the same rows on a real PostgreSQL 17, in a database created with
//! `LC_COLLATE 'C'`: PostgreSQL compares strings inside a document by the
//! database's collation, and collations are not modelled here.
//!
//! Documents used to be ordered by their encoded bytes and grouped by them.
//! That was stable, but it was not PostgreSQL's order — `ORDER BY doc` and
//! `doc < '...'` answered questions about the encoding — and two documents
//! `=` calls equal (`{"a":1}` and `{"a":1.0}`) were two groups, two `DISTINCT`
//! rows, two `UNION` rows and two join buckets.
//!
//! The last test is about soundness rather than PostgreSQL: with the order
//! now PostgreSQL's, nothing that prunes rows may still read documents in byte
//! order. An indexed table and an unindexed twin get the same random documents
//! and must answer every range and every `ORDER BY … LIMIT` alike, and both
//! must agree with the engine's own comparator.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::jsonb;
use sqlx::{AssertSqlSafe, Executor, Row};

/// Respellings of one value sit side by side (`{"a":1}` and `{"a":1.0}`,
/// `[1,2]` and `[1.0,2e0]`, `0` and `-0.0`), so every grouping and matching
/// case has a pair `=` calls equal and the bytes do not. Row 13 is SQL NULL.
const D_ROWS: &str = r#"
 (1,'{"a":1}'),(2,'{"a":1.0}'),(3,'{"a":2}'),(4,'{"b":0}'),(5,'{"aa":0}'),
 (6,'[1,2]'),(7,'[1.0,2e0]'),(8,'[]'),(9,'1'),(10,'1.0'),
 (11,'"a"'),(12,'null'),(13,NULL),(14,'true'),(15,'{"a":{"b":1},"c":2}'),
 (16,'{"c":2.0,"a":{"b":1e0}}'),(17,'[{}]'),(18,'"é"'),(19,'{"a":1e400}'),(20,'{}'),
 (21,'{"a":[1,2]}'),(22,'{"a":"x"}'),(23,'[1]'),(24,'-0.0'),(25,'0')"#;

/// The other side of the join, the `UNION` and the `IN (subquery)`: more
/// respellings of `d`'s values, and some that match nothing.
const E_ROWS: &str = r#"
 (101,'{"a":1e0}'),(102,'[1,2.00]'),(103,'1'),(104,'{"a":{"b":1.0},"c":2}'),(105,'"é"'),
 (106,'{"b":0.0}'),(107,'[2,1]'),(108,NULL),(109,'0e3'),(110,'{"a":3}')"#;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, rows) in [("d", D_ROWS), ("e", E_ROWS)] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSONB)"
        ))))
        .await
        .expect("create");
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO {table} VALUES {rows}"))))
            .await
            .expect("seed");
    }
    (server, pool)
}

/// Every cell of every row as the text the server sent: the simple protocol
/// sends values as text, and the type check is skipped so a `jsonb` cell
/// reads as the document PostgreSQL would print.
async fn cells(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|r| {
            (0..r.len())
                .map(|i| r.try_get_unchecked::<Option<String>, _>(i).unwrap())
                .collect()
        })
        .collect())
}

/// Rows joined by `,`, cells by `:`, NULL as `NULL`; sorted first when the
/// statement has no order of its own.
async fn flat(pool: &sqlx::PgPool, sql: &str, sort: bool) -> Result<String, String> {
    let mut rows: Vec<String> = cells(pool, sql)
        .await?
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|c| c.unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join(":")
        })
        .collect();
    if sort {
        let first = |r: &String| r.split(':').next().and_then(|n| n.parse::<i64>().ok());
        rows.sort_by_key(|r| first(r).unwrap_or(i64::MAX));
    }
    Ok(rows.join(","))
}

/// `(statement, sort the rows by their first cell first, PostgreSQL 17's
/// answer)`.
const CASES: &[(&str, bool, &str)] = &[
    // ORDER BY: kinds, the top-level scalar between `[]` and `[x]`, pair
    // count before keys, keys bytewise once counts agree, numbers by value
    // past `f64`, and NULL above every value.
    (
        "SELECT id FROM d ORDER BY doc, id",
        false,
        "8,12,11,18,24,25,9,10,14,23,17,6,7,20,22,1,2,3,19,21,5,4,15,16,13",
    ),
    (
        "SELECT id FROM d ORDER BY doc DESC, id",
        false,
        "13,15,16,4,5,21,19,3,1,2,22,20,6,7,17,23,14,9,10,24,25,18,11,12,8",
    ),
    (
        "SELECT id FROM d ORDER BY doc NULLS FIRST, id",
        false,
        "13,8,12,11,18,24,25,9,10,14,23,17,6,7,20,22,1,2,3,19,21,5,4,15,16",
    ),
    (
        "SELECT id FROM d ORDER BY doc DESC NULLS LAST, id",
        false,
        "15,16,4,5,21,19,3,1,2,22,20,6,7,17,23,14,9,10,24,25,18,11,12,8,13",
    ),
    // A bounded sort keeps its rows by the same order.
    ("SELECT id FROM d ORDER BY doc LIMIT 4", false, "8,12,11,18"),
    ("SELECT id FROM d ORDER BY doc DESC LIMIT 1", false, "13"),
    ("SELECT id FROM d ORDER BY doc DESC NULLS LAST, id LIMIT 3", false, "15,16,4"),
    (
        "SELECT id FROM d ORDER BY doc->'a', id",
        false,
        "22,1,2,3,19,21,15,16,4,5,6,7,8,9,10,11,12,13,14,17,18,20,23,24,25",
    ),
    // GROUP BY: equal values are one group whatever their spelling; NULL is
    // a group of its own.
    (
        "SELECT min(id), count(*) FROM d GROUP BY doc",
        true,
        "1:2,3:1,4:1,5:1,6:2,8:1,9:2,11:1,12:1,13:1,14:1,15:2,17:1,18:1,19:1,20:1,21:1,22:1,23:1,24:2",
    ),
    (
        "SELECT min(id), count(*) FROM d GROUP BY doc->'a'",
        true,
        "1:2,3:1,4:17,15:2,19:1,21:1,22:1",
    ),
    // A join on documents matches by value.
    (
        "SELECT d.id, e.id FROM d JOIN e ON d.doc = e.doc ORDER BY d.id, e.id",
        false,
        "1:101,2:101,4:106,6:102,7:102,9:103,10:103,15:104,16:104,18:105,24:109,25:109",
    ),
    // Membership by value: a subquery's set, a list long enough to be hashed,
    // and `= ANY`.
    (
        "SELECT id FROM d WHERE doc IN (SELECT doc FROM e) ORDER BY id",
        false,
        "1,2,4,6,7,9,10,15,16,18,24,25",
    ),
    (
        r#"SELECT id FROM d WHERE doc IN ('1', '{"a":1.0}', '[1,2]', '"a"', 'true', '0') ORDER BY id"#,
        false,
        "1,2,6,7,9,10,11,14,24,25",
    ),
    (
        r#"SELECT id FROM d WHERE doc = ANY('{1,2.0,"[1,2]"}') ORDER BY id"#,
        false,
        "6,7,9,10",
    ),
    // Windows partition by value and rank in PostgreSQL's order.
    (
        "SELECT id, count(*) OVER (PARTITION BY doc) FROM d ORDER BY id",
        false,
        "1:2,2:2,3:1,4:1,5:1,6:2,7:2,8:1,9:2,10:2,11:1,12:1,13:1,14:1,15:2,16:2,17:1,18:1,19:1,20:1,21:1,22:1,23:1,24:2,25:2",
    ),
    (
        "SELECT id, rank() OVER (ORDER BY doc) FROM d ORDER BY id",
        false,
        "1:16,2:16,3:18,4:22,5:21,6:12,7:12,8:1,9:7,10:7,11:3,12:2,13:25,14:9,15:23,16:23,17:11,18:4,19:19,20:14,21:20,22:15,23:10,24:5,25:5",
    ),
];

/// `(predicate, ids PostgreSQL 17 returned)`, each run as `SELECT id FROM d
/// WHERE … ORDER BY id`.
const PREDICATES: &[(&str, &str)] = &[
    (r#"doc < '{"a":2}'"#, "1,2,6,7,8,9,10,11,12,14,17,18,20,22,23,24,25"),
    ("doc > '[1,2]'", "1,2,3,4,5,15,16,19,20,21,22"),
    ("doc <= '1'", "8,9,10,11,12,18,24,25"),
    (r#"doc >= '"a"'"#, "1,2,3,4,5,6,7,9,10,11,14,15,16,17,18,19,20,21,22,23,24,25"),
    ("doc < '[]'", ""),
    ("doc > '1'", "1,2,3,4,5,6,7,14,15,16,17,19,20,21,22,23"),
    ("doc < '[1]'", "8,9,10,11,12,14,18,24,25"),
    (r#"doc > '{"a":1}'"#, "3,4,5,15,16,19,21"),
    (r#"doc >= '{"a":1}'"#, "1,2,3,4,5,15,16,19,21"),
    (r#"doc < '{"aa":0}'"#, "1,2,3,6,7,8,9,10,11,12,14,17,18,19,20,21,22,23,24,25"),
    ("doc > '1e300'", "1,2,3,4,5,6,7,14,15,16,17,19,20,21,22,23"),
    ("doc->'a' > '1'", "3,15,16,19,21"),
    ("doc->'a' >= '1.0'", "1,2,3,15,16,19,21"),
    ("doc->'a' < '{}'", "1,2,3,19,21,22"),
    (r#"doc->'a' < '"x"'"#, ""),
    ("doc->'a' <= '[1,2]'", "1,2,3,19,21,22"),
    ("doc #> '{a,b}' >= '1'", "15,16"),
    (r#"'{"a":1}' < doc"#, "3,4,5,15,16,19,21"),
    (r#"doc BETWEEN '"a"' AND '1'"#, "9,10,11,18,24,25"),
];

#[tokio::test(flavor = "multi_thread")]
async fn documents_order_group_and_match_as_postgresql_does() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (sql, sort, want) in CASES {
        let got = flat(&pool, sql, *sort).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{sql}\n    want {want}\n    got  {got:?}"));
        }
    }
    for (predicate, want) in PREDICATES {
        let sql = format!("SELECT id FROM d WHERE {predicate} ORDER BY id");
        let got = flat(&pool, &sql, false).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{sql}\n    want {want}\n    got  {got:?}"));
        }
    }
    let total = CASES.len() + PREDICATES.len();
    assert!(wrong.is_empty(), "{} of {total} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// PostgreSQL's groups of `d` in its order, each as the texts its members
/// print as: `SELECT string_agg(doc::text, '|') … GROUP BY doc ORDER BY doc`.
/// A group shows one of its members, and which one is not defined, so a
/// shown document is checked to be one of its group's.
const GROUPS: &[&[&str]] = &[
    &["[]"],
    &["null"],
    &[r#""a""#],
    &[r#""é""#],
    &["0", "0.0"],
    &["1", "1.0"],
    &["true"],
    &["[1]"],
    &["[{}]"],
    &["[1, 2]", "[1.0, 2]"],
    &["{}"],
    &[r#"{"a": "x"}"#],
    &[r#"{"a": 1}"#, r#"{"a": 1.0}"#],
    &[r#"{"a": 2}"#],
    &["{\"a\": 1e400}"],
    &[r#"{"a": [1, 2]}"#],
    &[r#"{"aa": 0}"#],
    &[r#"{"b": 0}"#],
    &[r#"{"a": {"b": 1}, "c": 2}"#, r#"{"a": {"b": 1}, "c": 2.0}"#],
];

/// `1e400` is printed in full, 401 digits: written out here rather than
/// spelled in the table.
fn printed(text: &str) -> String {
    text.replace("1e400", &format!("1{}", "0".repeat(400)))
}

/// `GROUP BY doc … ORDER BY doc` and `SELECT DISTINCT doc … ORDER BY doc`:
/// one row per value, in PostgreSQL's order, NULL last, each showing a member
/// of its group; and `UNION` keeps one row per value across both tables.
#[tokio::test(flavor = "multi_thread")]
async fn grouped_and_distinct_documents_are_one_row_per_value() {
    let (_s, pool) = server().await;
    let counts = "1,1,1,1,2,2,1,1,1,2,1,1,2,1,1,1,1,1,2,1";
    for (label, sql, count_column) in [
        ("GROUP BY", "SELECT doc, count(*) FROM d GROUP BY doc ORDER BY doc", true),
        ("DISTINCT", "SELECT DISTINCT doc FROM d ORDER BY doc", false),
    ] {
        let rows = cells(&pool, sql).await.unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(rows.len(), GROUPS.len() + 1, "{label}: one row per value and one for NULL");
        for (row, group) in rows.iter().zip(GROUPS) {
            let shown = row[0].clone().unwrap_or_default();
            assert!(
                group.iter().any(|member| printed(member) == shown),
                "{label}: {shown} is not one of {group:?}"
            );
        }
        assert_eq!(rows.last().unwrap()[0], None, "{label}: NULL sorts last");
        if count_column {
            let got: Vec<String> = rows.iter().map(|r| r[1].clone().unwrap_or_default()).collect();
            assert_eq!(got.join(","), counts, "{label}: the group sizes");
        }
    }
    // 20 values in `d`, and `e` adds `[2,1]` and `{"a":3}`.
    let union = cells(&pool, "SELECT doc FROM d UNION SELECT doc FROM e").await.expect("UNION");
    assert_eq!(union.len(), 22, "UNION keeps one row per value");
    // Over a join, too: twelve matches of seven values, PostgreSQL says.
    for sql in [
        "SELECT DISTINCT d.doc FROM d JOIN e ON d.doc = e.doc",
        "SELECT DISTINCT d.doc, e.doc FROM d JOIN e ON d.doc = e.doc",
    ] {
        let rows = cells(&pool, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows.len(), 7, "{sql}");
    }
}

/// A unique document column refuses a respelling of a value it holds, as
/// PostgreSQL's unique index does, and so does adding the constraint over
/// rows that hold two. PostgreSQL 17: the second insert fails with
/// `duplicate key value violates unique constraint "u_doc_key"`, ids 1 and 4
/// remain; the composite constraint refuses `(1, '[1.0]')` as
/// `"w_k_doc_key"` and keeps ids 1 and 3; and the `ALTER` fails with `could
/// not create unique index "v_doc_key"`.
///
/// Before this, such a column held the very same document twice: the check
/// read the stored value by its declared type rather than as stored, and so
/// never found it.
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_document_column_compares_values() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let run = |sql: &'static str| {
        let pool = pool.clone();
        async move { pool.execute(sqlx::raw_sql(sql)).await.map(|_| ()).map_err(|e| e.to_string()) }
    };
    run("CREATE TABLE u (id BIGINT PRIMARY KEY, doc JSONB UNIQUE)").await.expect("create");
    run(r#"INSERT INTO u VALUES (1, '{"a":1}')"#).await.expect("first insert");
    assert!(
        run(r#"INSERT INTO u VALUES (2, '{"a":1.0}')"#).await.is_err(),
        "a respelling of a held value is a duplicate"
    );
    assert!(
        run(r#"INSERT INTO u VALUES (3, '{"a":1}')"#).await.is_err(),
        "the same document twice"
    );
    run("INSERT INTO u VALUES (4, '[1,2]')").await.expect("a new value");
    assert_eq!(flat(&pool, "SELECT id FROM u ORDER BY id", false).await.as_deref(), Ok("1,4"));

    // A constraint over several columns, one a document, checks it the same
    // way: `(1, '[1]')` and `(1, '[1.0]')` are one tuple, `(2, '[1]')`
    // another.
    run("CREATE TABLE w (id BIGINT PRIMARY KEY, k INT, doc JSONB, UNIQUE (k, doc))")
        .await
        .expect("create");
    run("INSERT INTO w VALUES (1, 1, '[1]')").await.expect("first insert");
    assert!(run("INSERT INTO w VALUES (2, 1, '[1.0]')").await.is_err(), "the same tuple");
    run("INSERT INTO w VALUES (3, 2, '[1]')").await.expect("another tuple");
    assert_eq!(flat(&pool, "SELECT id FROM w ORDER BY id", false).await.as_deref(), Ok("1,3"));

    run("CREATE TABLE v (id BIGINT PRIMARY KEY, doc JSONB)").await.expect("create");
    run("INSERT INTO v VALUES (1, '[1]'), (2, '[1.0]')").await.expect("seed");
    assert!(
        run("ALTER TABLE v ADD CONSTRAINT v_doc_key UNIQUE (doc)").await.is_err(),
        "the rows already hold one value twice"
    );
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A random document's text: every kind, numbers spelled several ways and
/// past `f64`, keys whose length order and byte order disagree.
fn document(r: &mut Lcg, depth: u32) -> String {
    const SCALARS: &[&str] = &[
        "null", "true", "false", "0", "-0.0", "1", "1.0", "1e0", "2.50", "-3", "1e400", "-1e400",
        "1.000000000000000000001", r#""""#, r#""a""#, r#""ab""#, r#""b""#, r#""é""#,
    ];
    if depth == 0 || r.below(3) == 0 {
        return SCALARS[r.below(SCALARS.len() as u64) as usize].to_string();
    }
    let n = r.below(4) as usize;
    if r.below(2) == 0 {
        let items: Vec<String> = (0..n).map(|_| document(r, depth - 1)).collect();
        format!("[{}]", items.join(","))
    } else {
        const KEYS: &[&str] = &["a", "b", "aa", "ab", "é"];
        let items: Vec<String> = (0..n)
            .map(|_| {
                let key = KEYS[r.below(KEYS.len() as u64) as usize];
                format!("\"{key}\":{}", document(r, depth - 1))
            })
            .collect();
        format!("{{{}}}", items.join(","))
    }
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The indexed table and its unindexed twin answer every document range and
/// every bounded document sort alike, and agree with the engine's comparator.
///
/// `ix` carries a hash index and both gin indexes on `doc`, and a path index
/// on `doc->>'a'`; the filter index covers `id` as it covers every integer
/// column. None of them may prune by a byte order: a hash index answers
/// equality only, a gin index containment only, and a document column has no
/// zone bounds, so a range over documents must reach every row. A missing row
/// in `ix`'s answer is what this exists to catch.
#[tokio::test(flavor = "multi_thread")]
async fn indexed_documents_answer_ranges_and_sorts_like_their_twin() {
    use std::cmp::Ordering;
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for t in ["ix", "tw"] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc JSONB)"
        ))))
        .await
        .expect("create");
    }
    for ddl in [
        "CREATE INDEX ix_hash ON ix USING hash (doc)",
        "CREATE INDEX ix_gin ON ix USING gin (doc)",
        "CREATE INDEX ix_gin_path ON ix USING gin (doc jsonb_path_ops)",
        "CREATE INDEX ix_a ON ix ((doc->>'a'))",
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(ddl.to_string())))
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    // A B-tree would keep documents in its key encoding's byte order, which is
    // not the order ranges are now answered in, so it stays refused.
    let btree = pool
        .execute(sqlx::raw_sql("CREATE INDEX ix_btree ON ix USING btree (doc)"))
        .await;
    assert!(btree.is_err(), "a B-tree over documents must be refused");

    let mut r = Lcg(0x6f72_6465);
    // The documents the reference reads, encoded as the engine encodes them.
    let mut docs: Vec<(i64, Option<Vec<u8>>)> = Vec::new();
    for batch in 0..4 {
        let mut values = Vec::new();
        for i in 0..100 {
            let id = batch * 100 + i;
            if r.below(15) == 0 {
                values.push(format!("({id}, NULL)"));
                docs.push((id, None));
            } else {
                let text = document(&mut r, 3);
                let encoded = jsonb::encode_text(&text).expect("a generated document encodes");
                docs.push((id, Some(encoded)));
                values.push(format!("({id}, {})", quoted(&text)));
            }
        }
        for t in ["ix", "tw"] {
            pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
                "INSERT INTO {t} (id, doc) VALUES {}",
                values.join(", ")
            ))))
            .await
            .expect("insert");
        }
    }

    let ids = |rows: Vec<Vec<Option<String>>>| -> Vec<i64> {
        rows.iter().map(|row| row[0].as_deref().unwrap().parse().unwrap()).collect()
    };
    let mut checked = 0;
    for round in 0..120 {
        let literal = document(&mut r, 2);
        let lit = jsonb::encode_text(&literal).unwrap();
        let (op, wanted): (&str, fn(Ordering) -> bool) = match round % 4 {
            0 => ("<", Ordering::is_lt),
            1 => ("<=", Ordering::is_le),
            2 => (">", Ordering::is_gt),
            _ => (">=", Ordering::is_ge),
        };
        let floor = r.below(400) as i64;
        let mut want: Vec<i64> = docs
            .iter()
            .filter(|(id, doc)| {
                doc.as_ref().is_some_and(|d| wanted(jsonb::order(d, &lit)))
                    && (round % 3 != 0 || *id >= floor)
            })
            .map(|(id, _)| *id)
            .collect();
        want.sort_unstable();
        let clause = if round % 3 == 0 {
            format!("id >= {floor} AND doc {op} {}", quoted(&literal))
        } else {
            format!("doc {op} {}", quoted(&literal))
        };
        for t in ["ix", "tw"] {
            let sql = format!("SELECT id FROM {t} WHERE {clause} ORDER BY id");
            let got = ids(cells(&pool, &sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")));
            assert_eq!(got, want, "{sql}");
        }
        checked += want.len();

        // A bounded sort, both directions, with and without a tie-breaker.
        let n = 1 + r.below(40) as usize;
        let descending = round % 2 == 1;
        let mut sorted: Vec<&(i64, Option<Vec<u8>>)> = docs.iter().collect();
        sorted.sort_by(|(ia, a), (ib, b)| {
            // NULL above every value, as PostgreSQL sorts it.
            let by_doc = match (a, b) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(a), Some(b)) => jsonb::order(a, b),
            };
            (if descending { by_doc.reverse() } else { by_doc }).then(ia.cmp(ib))
        });
        let direction = if descending { "DESC" } else { "ASC" };
        let want_ids: Vec<i64> = sorted.iter().take(n).map(|(id, _)| *id).collect();
        for t in ["ix", "tw"] {
            let sql = format!("SELECT id FROM {t} ORDER BY doc {direction}, id LIMIT {n}");
            let got = ids(cells(&pool, &sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")));
            assert_eq!(got, want_ids, "{sql}");
            // Without the tie-breaker the ids among equal values may come in
            // any order, so the documents are compared by value instead.
            let sql = format!("SELECT doc FROM {t} ORDER BY doc {direction} LIMIT {n}");
            let got = cells(&pool, &sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(got.len(), n.min(docs.len()), "{sql}");
            for (at, (row, (_, expected))) in got.iter().zip(sorted.iter()).enumerate() {
                let shown =
                    row[0].as_deref().map(|text| jsonb::encode_text(text).expect("a document"));
                let same = match (&shown, expected) {
                    (None, None) => true,
                    (Some(a), Some(b)) => jsonb::compare_values(a, b) == Some(Ordering::Equal),
                    _ => false,
                };
                assert!(same, "{sql}: row {at} is {:?}", row[0]);
            }
        }
    }
    assert!(checked > 5_000, "too few matching rows to test ({checked})");
}
