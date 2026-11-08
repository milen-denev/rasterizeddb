//! `LATERAL`: a `FROM` item that reads the items before it (`core::rql::lateral`).
//! Every form was refused — a subquery as "a correlated scalar subquery … is
//! not supported", a function as a relation named `LATERAL` — though the
//! top-N-per-group query is written with one. Every expected answer is
//! PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
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

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

async fn server() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE lc (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE lo (id INT PRIMARY KEY, cid INT, total INT, placed DATE)",
        "CREATE TABLE ln2 (id INT PRIMARY KEY, cid INT, tag TEXT)",
        "INSERT INTO lc VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "INSERT INTO lo VALUES (1, 1, 10, '2024-01-01'), (2, 1, 30, '2024-02-01'), (3, 1, 20, '2024-03-01'), \
         (4, 2, 5, '2024-01-05'), (5, 2, 50, '2024-01-06')",
        "INSERT INTO ln2 VALUES (1, NULL, 'x'), (2, 1, NULL), (3, NULL, 'y')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

#[tokio::test(flavor = "multi_thread")]
async fn each_form_answers_as_postgresql_does() {
    let (_server, c) = server().await;
    let cases: &[(&str, &[&str])] = &[
        // Top N per group.
        (
            "SELECT c.name, o.total FROM lc c CROSS JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id \
             ORDER BY total DESC LIMIT 2) o ORDER BY c.name, o.total",
            &["a|20", "a|30", "b|5", "b|50"],
        ),
        // A LEFT join keeps the outer row its item has nothing for.
        (
            "SELECT c.name, o.total FROM lc c LEFT JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id \
             ORDER BY total DESC LIMIT 1) o ON true ORDER BY c.name",
            &["a|30", "b|50", "c|NULL"],
        ),
        (
            "SELECT c.name, o.total FROM lc c LEFT JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id) o \
             ON o.total > 25 ORDER BY 1, 2",
            &["a|30", "b|50", "c|NULL"],
        ),
        // An aggregate over nothing is a row, joined back to its outer row.
        (
            "SELECT c.name, s.n FROM lc c, LATERAL (SELECT count(*) AS n FROM lo WHERE lo.cid = c.id) s ORDER BY c.name",
            &["a|3", "b|2", "c|0"],
        ),
        // A NULL outer value is a value too.
        (
            "SELECT n.id, s.k FROM ln2 n CROSS JOIN LATERAL (SELECT count(*) AS k FROM lo WHERE lo.cid = n.cid) s ORDER BY 1",
            &["1|0", "2|3", "3|0"],
        ),
        (
            "SELECT n.id, s.t FROM ln2 n CROSS JOIN LATERAL (SELECT coalesce(n.tag, 'none') AS t) s ORDER BY 1",
            &["1|x", "2|none", "3|y"],
        ),
        // Functions, VALUES, a column list, JOIN … ON.
        ("SELECT c.name, x FROM lc c, LATERAL generate_series(1, c.id) x ORDER BY 1, 2", &["a|1", "b|1", "b|2", "c|1", "c|2", "c|3"]),
        (
            "SELECT c.id, g FROM lc c CROSS JOIN LATERAL unnest(ARRAY[c.id, c.id + 1]) g ORDER BY 1, 2",
            &["1|1", "1|2", "2|2", "2|3", "3|3", "3|4"],
        ),
        (
            "SELECT c.name, t.v FROM lc c CROSS JOIN LATERAL (VALUES (c.id * 10), (c.id * 100)) t(v) ORDER BY 1, 2",
            &["a|10", "a|100", "b|20", "b|200", "c|30", "c|300"],
        ),
        (
            "SELECT c.name, o.a FROM lc c CROSS JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id LIMIT 1) o(a) ORDER BY 1",
            &["a|10", "b|5"],
        ),
        (
            "SELECT c.name, o.total FROM lc c JOIN LATERAL (SELECT * FROM lo WHERE lo.cid = c.id) o ON o.total > 15 ORDER BY 1, 2",
            &["a|20", "a|30", "b|50"],
        ),
        // Over a derived table, a CTE and another LATERAL item; grouped.
        (
            "SELECT x.name, y.total FROM (SELECT * FROM lc WHERE id > 1) x CROSS JOIN LATERAL (SELECT total FROM lo \
             WHERE lo.cid = x.id ORDER BY total LIMIT 1) y ORDER BY 1",
            &["b|5"],
        ),
        (
            "WITH w AS (SELECT id, name FROM lc WHERE id <> 2) SELECT w.name, o.total FROM w CROSS JOIN LATERAL \
             (SELECT total FROM lo WHERE lo.cid = w.id ORDER BY total DESC LIMIT 1) o ORDER BY 1",
            &["a|30"],
        ),
        (
            "SELECT c.name, o.total, d.dbl FROM lc c CROSS JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id \
             ORDER BY total DESC LIMIT 1) o CROSS JOIN LATERAL (SELECT o.total * 2 AS dbl) d ORDER BY 1",
            &["a|30|60", "b|50|100"],
        ),
        (
            "SELECT c.name, sum(o.total) FROM lc c LEFT JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id \
             ORDER BY total DESC LIMIT 2) o ON true GROUP BY c.name ORDER BY 1",
            &["a|50", "b|55", "c|NULL"],
        ),
        // Two items keyed on one outer relation.
        (
            "SELECT c.name, s.n, g FROM lc c LEFT JOIN LATERAL (SELECT count(*) AS n FROM lo WHERE lo.cid = c.id) s \
             ON true CROSS JOIN LATERAL generate_series(1, c.id) g ORDER BY 1, 3",
            &["a|3|1", "b|2|1", "b|2|2", "c|0|1", "c|0|2", "c|0|3"],
        ),
        // No outer column and no rows: a LEFT join keeps every outer row, an
        // inner one none.
        (
            "SELECT c.name, s.total FROM lc c LEFT JOIN LATERAL (SELECT total FROM lo WHERE total > 1000) s ON true ORDER BY 1",
            &["a|NULL", "b|NULL", "c|NULL"],
        ),
        ("SELECT c.name, s.total FROM lc c JOIN LATERAL (SELECT total FROM lo WHERE total > 1000) s ON true ORDER BY 1", &[]),
        // A function in FROM reads the items before it without the word,
        // as PostgreSQL's does; WITH ORDINALITY numbers each call's rows.
        ("SELECT c.name, g FROM lc c, generate_series(1, c.id) g ORDER BY 1, 2", &["a|1", "b|1", "b|2", "c|1", "c|2", "c|3"]),
        (
            "SELECT c.id, u.v, u.n FROM lc c CROSS JOIN LATERAL unnest(ARRAY[c.id * 10, c.id * 20]) WITH ORDINALITY \
             AS u(v, n) WHERE c.id < 3 ORDER BY 1, 3",
            &["1|10|1", "1|20|2", "2|20|1", "2|40|2"],
        ),
        // Reading no outer column: an ordinary derived table.
        ("SELECT c.name, s.m FROM lc c CROSS JOIN LATERAL (SELECT max(total) AS m FROM lo) s ORDER BY 1", &["a|50", "b|50", "c|50"]),
    ];
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        match c.simple_query(sql).await {
            Ok(messages) => {
                let got: Vec<String> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(r) => Some(
                            (0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"),
                        ),
                        _ => None,
                    })
                    .collect();
                if got != *want {
                    wrong.push(format!("{sql}\n   got {got:?}\n  want {want:?}"));
                }
            }
            Err(e) => wrong.push(format!("{sql}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
    // Every statement's temporaries are gone.
    assert!(rows(&c, "SHOW TABLES").await.iter().all(|t| !t.contains("__cte_")));
}

/// What is refused: `*` (it would show the keys), RIGHT and FULL joins, two
/// outer items; and an item's own error is PostgreSQL's.
#[tokio::test(flavor = "multi_thread")]
async fn refusals_and_errors() {
    let (_server, c) = server().await;
    for (sql, sqlstate) in [
        ("SELECT * FROM lc c CROSS JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id) o", "0A000"),
        ("SELECT c.name FROM lc c RIGHT JOIN LATERAL (SELECT 1 AS x) s ON true", "0A000"),
        (
            "SELECT 1 FROM lc c, lo l, LATERAL (SELECT 1 AS x FROM ln2 WHERE ln2.cid = c.id AND ln2.id = l.id) s",
            "0A000",
        ),
        ("SELECT c.name, o.v FROM lc c CROSS JOIN LATERAL (SELECT 1 / (c.id - 2) AS v) o", "22012"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
    assert!(rows(&c, "SHOW TABLES").await.iter().all(|t| !t.contains("__cte_")));
}

/// Over the extended protocol, which describes before it runs: the item's
/// columns are announced with their types and the rows decode as them.
#[tokio::test(flavor = "multi_thread")]
async fn the_extended_protocol_describes_and_reads_them() {
    let (_server, c) = server().await;
    let statement = c
        .prepare(
            "SELECT c.name, o.total, o.placed FROM lc c CROSS JOIN LATERAL (SELECT total, placed FROM lo \
             WHERE lo.cid = c.id ORDER BY total DESC LIMIT 1) o ORDER BY c.name",
        )
        .await
        .expect("prepare");
    let types: Vec<&str> = statement.columns().iter().map(|col| col.type_().name()).collect();
    assert_eq!(types, ["text", "int4", "date"]);
    let got: Vec<(String, i32)> =
        c.query(&statement, &[]).await.expect("query").iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, [("a".to_string(), 30), ("b".to_string(), 50)]);

    let statement = c
        .prepare(
            "SELECT c.name, s.n, g FROM lc c LEFT JOIN LATERAL (SELECT count(*) AS n FROM lo WHERE lo.cid = c.id) s \
             ON true CROSS JOIN LATERAL generate_series(1, c.id) g WHERE c.id = $1 ORDER BY g",
        )
        .await
        .expect("prepare");
    let types: Vec<&str> = statement.columns().iter().map(|col| col.type_().name()).collect();
    assert_eq!(types, ["text", "int8", "int4"]);
    let got: Vec<(String, i64, i32)> =
        c.query(&statement, &[&2i32]).await.expect("query").iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect();
    assert_eq!(got, [("b".to_string(), 2, 1), ("b".to_string(), 2, 2)]);
}
