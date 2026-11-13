//! A parameter compared with a CTE's or a derived table's column is typed by
//! that column, as PostgreSQL 17 types it (`pg_prepared_statements.
//! parameter_types`, `docker run --rm postgres:17`). It was described as
//! `text`, so tokio-postgres refused to bind an `i32` or `i64` there
//! (`WrongType`) before the statement was ever sent.

use rasterizeddb_compat::TestServer;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Client;

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn parameters_take_the_type_of_a_cte_or_derived_column() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE cat (id INT PRIMARY KEY, parent INT, name TEXT);
         INSERT INTO cat VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'a1'), (5, 4, 'a1x'), (6, NULL, 'other');",
    )
    .await
    .expect("setup");

    let (i4, i8) = (Type::INT4, Type::INT8);
    // (statement, PostgreSQL's parameter types, arguments, PostgreSQL's rows)
    let cases: Vec<(&str, Vec<Type>, Vec<Box<dyn ToSql + Sync>>, Vec<&str>)> = vec![
        // A renamed column.
        ("WITH c AS (SELECT id AS k, name FROM cat) SELECT name FROM c WHERE k = $1", vec![i4.clone()], vec![Box::new(1i32)], vec!["root"]),
        // A column list.
        ("WITH t(a, b) AS (SELECT id, name FROM cat) SELECT b FROM t WHERE a = $1", vec![i4.clone()], vec![Box::new(2i32)], vec!["a"]),
        // The CTE's `id` is a count: bigint, not the table's integer `id`
        // its body reads.
        (
            "WITH c AS (SELECT parent, count(*) AS id FROM cat GROUP BY parent) SELECT parent FROM c WHERE id = $1 ORDER BY parent",
            vec![i8.clone()],
            vec![Box::new(2i64)],
            vec!["1", "NULL"],
        ),
        // A derived table, and one inside another.
        ("SELECT d.n FROM (SELECT id * 10 AS n FROM cat) d WHERE d.n > $1 ORDER BY d.n", vec![i4.clone()], vec![Box::new(30i32)], vec!["40", "50", "60"]),
        ("SELECT x FROM (SELECT n AS x FROM (SELECT count(*) AS n FROM cat) a) b WHERE x = $1", vec![i8.clone()], vec![Box::new(6i64)], vec!["6"]),
        // A recursive CTE, typed by its anchor; the body's parameter by the
        // table it reads.
        (
            "WITH RECURSIVE t(a, b) AS (SELECT id, name FROM cat WHERE id = $1 UNION ALL SELECT c.id, c.name FROM cat c JOIN t ON c.parent = t.a) SELECT b FROM t WHERE a > $2 ORDER BY a",
            vec![i4.clone(), i4.clone()],
            vec![Box::new(2i32), Box::new(2i32)],
            vec!["a1", "a1x"],
        ),
        // BETWEEN and IN.
        (
            "WITH c AS (SELECT id AS k FROM cat) SELECT k FROM c WHERE k BETWEEN $1 AND $2 OR k IN ($3, $4) ORDER BY k",
            vec![i4.clone(), i4.clone(), i4.clone(), i4.clone()],
            vec![Box::new(2i32), Box::new(3i32), Box::new(5i32), Box::new(6i32)],
            vec!["2", "3", "5", "6"],
        ),
        // Inside the body, the table's `id`; outside, the CTE's.
        (
            "WITH c AS (SELECT count(*) AS id FROM cat WHERE id > $1) SELECT id FROM c WHERE id = $2",
            vec![i4.clone(), i8.clone()],
            vec![Box::new(3i32), Box::new(3i64)],
            vec!["3"],
        ),
        // Qualified by the derived table's alias, on either side.
        (
            "SELECT s.total FROM (SELECT parent, sum(id) AS total FROM cat GROUP BY parent) AS s WHERE s.parent = $1 AND $2 < s.total",
            vec![i4.clone(), i8.clone()],
            vec![Box::new(1i32), Box::new(4i64)],
            vec!["5"],
        ),
    ];
    for (sql, types, args, want) in cases {
        let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(st.params(), types.as_slice(), "{sql}");
        let args: Vec<&(dyn ToSql + Sync)> = args.iter().map(|a| a.as_ref()).collect();
        let rows = c.query(&st, &args).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got: Vec<String> = rows
            .iter()
            .map(|r| match r.columns()[0].type_() {
                t if *t == Type::TEXT => r.get::<_, Option<String>>(0),
                t if *t == Type::INT8 => r.get::<_, Option<i64>>(0).map(|v| v.to_string()),
                t if *t == Type::NUMERIC => r.try_get::<_, Option<String>>(0).ok().flatten(),
                _ => r.get::<_, Option<i32>>(0).map(|v| v.to_string()),
            }
            .unwrap_or_else(|| "NULL".into()))
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// A derived table inside another, or beside one, is described with its
/// columns. Only the first was resolved, by the plain resolver: these were
/// described as `NoData`, and the rows sent anyway were refused by the client
/// ("DataRow field count does not match the number of columns").
#[tokio::test(flavor = "multi_thread")]
async fn nested_and_joined_derived_tables_are_described() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE cat (id INT PRIMARY KEY, parent INT, name TEXT);
         INSERT INTO cat VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b');",
    )
    .await
    .expect("setup");
    for (sql, types, want) in [
        ("SELECT x FROM (SELECT n AS x FROM (SELECT count(*) AS n FROM cat) a) b", vec![Type::INT8], "3"),
        (
            "SELECT a.n, b.m FROM (SELECT count(*) AS n FROM cat) a CROSS JOIN (SELECT max(name) AS m FROM cat) b",
            vec![Type::INT8, Type::TEXT],
            "3|root",
        ),
        (
            "SELECT a.id, b.name FROM (SELECT id FROM cat WHERE parent = 1) a JOIN (SELECT id, name FROM cat) b ON a.id = b.id ORDER BY a.id",
            vec![Type::INT4, Type::TEXT],
            "2|a",
        ),
    ] {
        let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let described: Vec<Type> = st.columns().iter().map(|c| c.type_().clone()).collect();
        assert_eq!(described, types, "{sql}");
        let rows = c.query(&st, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let first = rows.first().map(|r| {
            (0..r.len())
                .map(|i| match r.columns()[i].type_() {
                    t if *t == Type::TEXT => r.get::<_, String>(i),
                    t if *t == Type::INT8 => r.get::<_, i64>(i).to_string(),
                    _ => r.get::<_, i32>(i).to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        });
        assert_eq!(first.as_deref(), Some(want), "{sql}");
    }
}
