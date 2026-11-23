//! `JOIN … USING`, `NATURAL JOIN`, `ROLLUP`/`CUBE`/`GROUPING SETS`, a
//! correlated subquery in a UNION's branches, and the small gaps beside them
//! (`current_time`, interval units, `time ± interval`, `sha256`,
//! `convert_to`). Each was refused, and `interval '5 ms'` was five minutes.
//! The expected answers are PostgreSQL 17's (`.claude/reviews/pgdiff`).

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

async fn setup(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE sa (id INT, k INT, v TEXT); CREATE TABLE sb (id INT, k INT, w TEXT); \
         INSERT INTO sa VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'); \
         INSERT INTO sb VALUES (1,10,'x'),(2,99,'y'),(4,40,'z'); \
         CREATE TABLE g (a TEXT, b TEXT, n INT); \
         INSERT INTO g VALUES ('x','p',1),('x','q',2),('y','p',3),('y','p',4); \
         CREATE TABLE ge (a TEXT, n INT)",
    )
    .await
    .expect("setup");
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn using_and_natural_joins_merge_their_columns() {
    let server = TestServer::start().await.expect("server");
    let c = setup(&server).await;
    assert_eq!(
        rows(&c, "SELECT id, v, w FROM sa JOIN sb USING (id) ORDER BY id").await,
        ["1|a|x", "2|b|y"]
    );
    assert_eq!(
        rows(&c, "SELECT * FROM sa JOIN sb USING (id, k)").await,
        ["1|10|a|x"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, v, w FROM sa LEFT JOIN sb USING (id) ORDER BY id"
        )
        .await,
        ["1|a|x", "2|b|y", "3|c|NULL"]
    );
    assert_eq!(
        rows(&c, "SELECT id FROM sa FULL JOIN sb USING (id) ORDER BY id").await,
        ["1", "2", "3", "4"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, v FROM sa RIGHT JOIN sb USING (id) ORDER BY id"
        )
        .await,
        ["1|a", "2|b", "4|NULL"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT sa.id, sb.id, id FROM sa FULL JOIN sb USING (id) ORDER BY 3"
        )
        .await,
        ["1|1|1", "2|2|2", "3|NULL|3", "NULL|4|4"]
    );
    assert_eq!(
        rows(&c, "SELECT * FROM sa NATURAL JOIN sb").await,
        ["1|10|a|x"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, count(*) FROM sa JOIN sb USING (id) WHERE id > 1 GROUP BY id ORDER BY id"
        )
        .await,
        ["2|1"]
    );
    let e = c
        .simple_query("SELECT * FROM sa JOIN sb USING (nope)")
        .await
        .expect_err("unknown column");
    assert_eq!(e.code().map(|s| s.code()), Some("42703"));
}

#[tokio::test(flavor = "multi_thread")]
async fn grouping_sets_group_by_each_set() {
    let server = TestServer::start().await.expect("server");
    let c = setup(&server).await;
    assert_eq!(
        rows(
            &c,
            "SELECT k, count(*) FROM sa GROUP BY ROLLUP (k) ORDER BY k"
        )
        .await,
        ["10|1", "20|1", "30|1", "NULL|3"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT a, b, sum(n) FROM g GROUP BY ROLLUP (a, b) ORDER BY a, b"
        )
        .await,
        [
            "x|p|1",
            "x|q|2",
            "x|NULL|3",
            "y|p|7",
            "y|NULL|7",
            "NULL|NULL|10"
        ]
    );
    assert_eq!(
        rows(&c, "SELECT a, b, sum(n) FROM g WHERE n > 1 GROUP BY ROLLUP (a, b) HAVING sum(n) > 2 ORDER BY a, b").await,
        ["y|p|7", "y|NULL|7", "NULL|NULL|9"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT a, b, count(*), GROUPING(a, b) FROM g GROUP BY CUBE (a, b) ORDER BY 4, a, b"
        )
        .await,
        [
            "x|p|1|0",
            "x|q|1|0",
            "y|p|2|0",
            "x|NULL|2|1",
            "y|NULL|2|1",
            "NULL|p|3|2",
            "NULL|q|1|2",
            "NULL|NULL|4|3"
        ]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT k, v, count(*) FROM sa GROUP BY GROUPING SETS ((k), (v), ()) ORDER BY 1, 2"
        )
        .await,
        [
            "10|NULL|1",
            "20|NULL|1",
            "30|NULL|1",
            "NULL|a|1",
            "NULL|b|1",
            "NULL|c|1",
            "NULL|NULL|3"
        ]
    );
    // The empty set's row comes even from no rows.
    assert_eq!(
        rows(&c, "SELECT a, count(*) FROM ge GROUP BY ROLLUP (a)").await,
        ["NULL|0"]
    );
    assert_eq!(
        rows(&c, "SELECT a, sum(n) AS total FROM g GROUP BY GROUPING SETS (a, ()) ORDER BY total DESC LIMIT 2").await,
        ["NULL|10", "y|7"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_subqueries_in_union_branches_and_small_gaps() {
    let server = TestServer::start().await.expect("server");
    let c = setup(&server).await;
    assert_eq!(
        rows(&c, "SELECT id, (SELECT max(w) FROM sb WHERE sb.id = sa.id) FROM sa UNION ALL SELECT 9, 'q' ORDER BY 1").await,
        ["1|x", "2|y", "3|NULL", "9|q"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, (SELECT max(w) FROM sb WHERE sb.id = sa.id) FROM sa UNION \
             SELECT id, (SELECT max(v) FROM sa x WHERE x.id = sb.id) FROM sb ORDER BY 1, 2",
        )
        .await,
        ["1|a", "1|x", "2|b", "2|y", "3|NULL", "4|NULL"]
    );
    assert_eq!(
        rows(&c, "SELECT current_time IS NOT NULL, localtime IS NOT NULL").await,
        ["t|t"]
    );
    assert_eq!(
        rows(&c, "SELECT pg_typeof(current_time), pg_typeof(localtime)").await,
        ["time with time zone|time without time zone"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT interval '0 s', interval '90 s', interval '5 ms', interval '3 us'"
        )
        .await,
        ["00:00:00|00:01:30|00:00:00.005|00:00:00.000003"]
    );
    assert_eq!(
        rows(&c, "SELECT time '10:00' + interval '1 hour', time '23:30' + interval '1 hour', time '10:00' - time '08:30', time '01:00' - interval '2 hours'").await,
        ["11:00:00|00:30:00|01:30:00|23:00:00"]
    );
    assert_eq!(
        rows(&c, "SELECT encode(sha256('abc'::bytea), 'hex')").await,
        ["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT convert_to('é', 'UTF8'), convert_from('\\xc3a9'::bytea, 'UTF8')"
        )
        .await,
        ["\\xc3a9|é"]
    );
    // ARRAY(subquery), correlated or not, and the empty array for no rows.
    assert_eq!(
        rows(&c, "SELECT ARRAY(SELECT w FROM sb ORDER BY w)").await,
        ["{x,y,z}"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id, ARRAY(SELECT w FROM sb WHERE sb.id <= sa.id ORDER BY w) FROM sa ORDER BY id"
        )
        .await,
        ["1|{x}", "2|{x,y}", "3|{x,y}"]
    );
    assert_eq!(
        rows(&c, "SELECT ARRAY(SELECT w FROM sb WHERE id > 100)").await,
        ["{}"]
    );
    // A comparison in RETURNING is the boolean it is.
    assert_eq!(
        rows(
            &c,
            "UPDATE sa SET k = k WHERE id = 1 RETURNING id, k > 5, v IS NULL"
        )
        .await,
        ["1|t|f"]
    );
}
