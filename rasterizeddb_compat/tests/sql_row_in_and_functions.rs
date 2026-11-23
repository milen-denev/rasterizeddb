//! A row compared with a subquery, `(a, b) [NOT] IN (SELECT x, y …)`, and
//! `overlay(… PLACING … FROM … FOR …)`, `jsonb_insert`, `jsonb_pretty`.
//! The row form was refused ("a subquery used with IN must select exactly
//! one column") and the functions were unknown. The answers are PostgreSQL
//! 17's (`.claude/reviews/pgdiff`), NULLs on either side included: a NULL
//! makes a row comparison UNKNOWN, so `NOT IN` keeps only the rows that
//! differ for certain.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn error(c: &tokio_postgres::Client, sql: &str) -> String {
    match c.simple_query(sql).await {
        Ok(_) => panic!("{sql}: succeeded"),
        Err(e) => e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string()),
    }
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_compared_with_a_subquery_is_three_valued() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE rt (a int, b text); CREATE TABLE ru (x int, y text);
         INSERT INTO rt VALUES (1,'p'),(1,'q'),(2,'q'),(NULL,'p'),(2,NULL);
         INSERT INTO ru VALUES (1,'p'),(2,'q'),(3,NULL)",
    )
    .await
    .expect("setup");

    assert_eq!(rows(&c, "SELECT a,b FROM rt WHERE (a,b) IN (SELECT x,y FROM ru) ORDER BY a,b").await, ["1|p", "2|q"]);
    // (2, NULL) and (NULL, 'p') may equal (3, NULL) or a row they half match:
    // UNKNOWN, so NOT IN drops them as IN does.
    assert_eq!(rows(&c, "SELECT a,b FROM rt WHERE (a,b) NOT IN (SELECT x,y FROM ru) ORDER BY a,b").await, ["1|q"]);
    assert_eq!(
        rows(&c, "SELECT a,b,(a,b) IN (SELECT x,y FROM ru) FROM rt ORDER BY a,b").await,
        ["1|p|t", "1|q|f", "2|q|t", "2|NULL|NULL", "NULL|p|NULL"]
    );
    assert_eq!(
        rows(&c, "SELECT a,b FROM rt WHERE NOT (a,b) IN (SELECT x,y FROM ru WHERE x < 3) ORDER BY a,b").await,
        ["1|q"]
    );
    assert_eq!(rows(&c, "SELECT count(*) FROM rt WHERE (a+1, b) IN (SELECT x, y FROM ru)").await, ["1"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM rt WHERE b = 'q' AND (a,b) IN (SELECT x,y FROM ru)").await, ["1"]);

    // No rows: IN is false and NOT IN true, for a row holding NULLs too.
    assert!(rows(&c, "SELECT a,b FROM rt WHERE (a,b) IN (SELECT x,y FROM ru WHERE x > 9)").await.is_empty());
    assert_eq!(
        rows(&c, "SELECT a,b FROM rt WHERE (a,b) NOT IN (SELECT x,y FROM ru WHERE x > 9) ORDER BY a,b").await,
        ["1|p", "1|q", "2|q", "2|NULL", "NULL|p"]
    );

    assert_eq!(error(&c, "SELECT count(*) FROM rt WHERE (a,b) IN (SELECT x FROM ru)").await, "subquery has too few columns");
    assert_eq!(
        error(&c, "SELECT count(*) FROM rt WHERE (a,b) IN (SELECT x,y,x FROM ru)").await,
        "subquery has too many columns"
    );

    // A write reads it the same way.
    c.batch_execute("DELETE FROM rt WHERE (a,b) IN (SELECT x,y FROM ru)").await.expect("delete");
    assert_eq!(rows(&c, "SELECT a,b FROM rt ORDER BY a,b").await, ["1|q", "2|NULL", "NULL|p"]);
    c.batch_execute("UPDATE rt SET b = 'r' WHERE (a,b) NOT IN (SELECT x,y FROM ru)").await.expect("update");
    assert_eq!(rows(&c, "SELECT a,b FROM rt ORDER BY a,b").await, ["1|r", "2|NULL", "NULL|p"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_jsonb_insert_and_jsonb_pretty() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    assert_eq!(
        rows(
            &c,
            "SELECT overlay('Txxxxas' placing 'hom' from 2 for 4), overlay('abc' placing 'XY' from 2), \
             overlay('abc' placing 'XY' from 2 for 0), overlay('abc','Z',1,1)"
        )
        .await,
        ["Thomas|aXY|aXYbc|Zbc"]
    );
    assert_eq!(rows(&c, "SELECT overlay('\\x010203'::bytea placing '\\xff'::bytea from 2)").await, ["\\x01ff03"]);
    assert_eq!(error(&c, "SELECT overlay('abc' placing 'XY' from 0)").await, "negative substring length not allowed");

    assert_eq!(
        rows(
            &c,
            "SELECT jsonb_insert('{\"a\":[0,1,2]}', '{a,1}', '\"new\"'), \
             jsonb_insert('{\"a\":[0,1,2]}', '{a,1}', '\"new\"', true), \
             jsonb_insert('{\"a\":{\"x\":1}}', '{a,y}', '2')"
        )
        .await,
        ["{\"a\": [0, \"new\", 1, 2]}|{\"a\": [0, 1, \"new\", 2]}|{\"a\": {\"x\": 1, \"y\": 2}}"]
    );
    assert_eq!(error(&c, "SELECT jsonb_insert('{\"a\":1}', '{a}', '2')").await, "cannot replace existing key");

    c.batch_execute("CREATE TABLE jp (id int, doc jsonb); INSERT INTO jp VALUES (1, '{\"a\":1,\"bb\":[1,{}],\"c\":\"x\"}')")
        .await
        .expect("setup");
    assert_eq!(
        rows(&c, "SELECT jsonb_pretty(doc) FROM jp").await,
        ["{\n    \"a\": 1,\n    \"c\": \"x\",\n    \"bb\": [\n        1,\n        {\n        }\n    ]\n}"]
    );
    assert_eq!(rows(&c, "SELECT id, jsonb_insert(doc, '{d}', '1') ? 'd' FROM jp").await, ["1|t"]);
}

/// A subquery naming an outer column bare (`WHERE customer_id = id`) reads
/// it as PostgreSQL scopes it: the subquery's own relations first, then the
/// enclosing query's. It was "unknown identifier 'id'".
#[tokio::test(flavor = "multi_thread")]
async fn a_subquery_reads_an_outer_column_named_bare() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE cu (id int, name text); CREATE TABLE ord (oid int, customer_id int, total int);
         INSERT INTO cu VALUES (1,'a'),(2,'b'),(3,'c');
         INSERT INTO ord VALUES (10,1,5),(11,1,7),(12,3,1)",
    )
    .await
    .expect("setup");

    assert_eq!(rows(&c, "SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = id) ORDER BY 1").await, ["a", "c"]);
    assert_eq!(rows(&c, "SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM ord o WHERE o.customer_id = id) ORDER BY 1").await, ["a", "c"]);
    assert_eq!(rows(&c, "SELECT name FROM cu WHERE NOT EXISTS (SELECT 1 FROM ord WHERE customer_id = id) ORDER BY 1").await, ["b"]);
    assert_eq!(
        rows(&c, "SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE oid = 10 AND customer_id = id) ORDER BY 1").await,
        ["a"]
    );
    assert_eq!(
        rows(&c, "SELECT name, (SELECT count(*) FROM ord WHERE customer_id = id) FROM cu ORDER BY 1").await,
        ["a|2", "b|0", "c|1"]
    );
    // Through a derived table that renames its columns.
    assert_eq!(
        rows(
            &c,
            "SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT customer_id, total FROM ord) AS s(k, t) WHERE s.k = id) ORDER BY 1"
        )
        .await,
        ["a", "c"]
    );
    // A write's target.
    c.batch_execute("DELETE FROM cu WHERE NOT EXISTS (SELECT 1 FROM ord WHERE customer_id = id)").await.expect("delete");
    assert_eq!(rows(&c, "SELECT name FROM cu ORDER BY 1").await, ["a", "c"]);
}
