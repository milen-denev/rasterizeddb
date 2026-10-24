//! `*` over a join lists each relation's declared columns and nothing else:
//! never the hidden `__null_<col>` companion a nullable column is stored
//! with, never a dropped column. `t.*` expands one relation, and `*` may sit
//! beside other items. PostgreSQL 17's answers.
//!
//! `SELECT * FROM a JOIN b` used to send every companion as a column of its
//! own (ten columns for six, over both protocols, plain and partitioned
//! alike), so a client reading by position read the wrong values; and `a.*`
//! was refused with 42601. Found by rasterizeddb-eb.

use rasterizeddb_compat::TestServer;
use sqlx::{Column, Executor, Row};

/// A file-backed server's root, removed on drop (after the server, which is
/// declared after it). Partitioned tables need a file-backed database.
struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn names(row: &sqlx::postgres::PgRow) -> Vec<String> {
    row.columns().iter().map(|c| c.name().to_string()).collect()
}

/// Every cell of every row as text, NULL as `NULL`, each decoded by the type
/// its column was described with.
fn cells(rows: &[sqlx::postgres::PgRow]) -> Vec<Vec<String>> {
    use sqlx::TypeInfo;
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(i, column)| {
                    let text = match column.type_info().name() {
                        "INT4" => row.try_get::<Option<i32>, _>(i).map(|v| v.map(|v| v.to_string())),
                        "INT8" => row.try_get::<Option<i64>, _>(i).map(|v| v.map(|v| v.to_string())),
                        "BOOL" => row.try_get::<Option<bool>, _>(i).map(|v| v.map(|v| v.to_string())),
                        _ => row.try_get::<Option<String>, _>(i),
                    };
                    match text {
                        Ok(Some(v)) => v,
                        Ok(None) => "NULL".to_string(),
                        Err(e) => format!("<{e}>"),
                    }
                })
                .collect()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn star_over_a_join_lists_only_declared_columns() {
    let dir = std::env::temp_dir().join(format!("join_star_columns_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("root");
    let root = Root(dir.to_string_lossy().into_owned());
    let server = TestServer::start_file_backed(&root.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE ja (id INT PRIMARY KEY, x INT, s TEXT);
         CREATE TABLE jb (id INT PRIMARY KEY, a_id INT, gone INT, y INT);
         INSERT INTO ja (id, x, s) VALUES (1, 10, 'one'), (2, NULL, NULL);
         INSERT INTO jb (id, a_id, gone, y) VALUES (7, 1, 5, 70), (8, 2, 6, NULL);
         ALTER TABLE jb DROP COLUMN gone;
         CREATE TABLE jp (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 4;
         CREATE TABLE jt (id BIGINT PRIMARY KEY, w BIGINT);
         INSERT INTO jp (tenant_id, id, v) VALUES (1, 1, 10), (2, 2, NULL);
         INSERT INTO jt (id, w) VALUES (1, 100), (2, NULL);
         CREATE TABLE oa (id INT PRIMARY KEY, n INT);
         CREATE TABLE ob (id INT PRIMARY KEY, a_id INT);
         INSERT INTO oa (id, n) VALUES (1, 10), (2, 20);
         INSERT INTO ob (id, a_id) VALUES (5, 2), (6, 1);",
    ))
    .await
    .expect("schema");

    let r = |row: &[&str]| row.iter().map(|c| c.to_string()).collect::<Vec<_>>();
    let cases: Vec<(&str, Vec<&str>, Vec<Vec<String>>)> = vec![
        (
            "SELECT * FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY ja.id",
            vec!["id", "x", "s", "id", "a_id", "y"],
            vec![r(&["1", "10", "one", "7", "1", "70"]), r(&["2", "NULL", "NULL", "8", "2", "NULL"])],
        ),
        (
            "SELECT * FROM jp JOIN jt ON jp.id = jt.id ORDER BY jt.id",
            vec!["tenant_id", "id", "v", "id", "w"],
            vec![r(&["1", "1", "10", "1", "100"]), r(&["2", "2", "NULL", "2", "NULL"])],
        ),
        (
            "SELECT ja.*, jb.y FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY ja.id",
            vec!["id", "x", "s", "y"],
            vec![r(&["1", "10", "one", "70"]), r(&["2", "NULL", "NULL", "NULL"])],
        ),
        (
            // Column 3 is `ja.x`, and DESC puts its NULL first.
            "SELECT jb.y, ja.* FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY 3 DESC",
            vec!["y", "id", "x", "s"],
            vec![r(&["NULL", "2", "NULL", "NULL"]), r(&["70", "1", "10", "one"])],
        ),
        (
            "SELECT *, jb.y AS again FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY ja.id",
            vec!["id", "x", "s", "id", "a_id", "y", "again"],
            vec![r(&["1", "10", "one", "7", "1", "70", "70"]), r(&["2", "NULL", "NULL", "8", "2", "NULL", "NULL"])],
        ),
        (
            // Column 1 is `ja.id`; both relations have an `id`.
            "SELECT * FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY 1 DESC",
            vec!["id", "x", "s", "id", "a_id", "y"],
            vec![r(&["2", "NULL", "NULL", "8", "2", "NULL"]), r(&["1", "10", "one", "7", "1", "70"])],
        ),
        (
            // Column 3 is `ob.id`, which runs against `oa.id`: an ordinal
            // naming the second `id` must sort by that one.
            "SELECT * FROM oa JOIN ob ON oa.id = ob.a_id ORDER BY 3",
            vec!["id", "n", "id", "a_id"],
            vec![r(&["2", "20", "5", "2"]), r(&["1", "10", "6", "1"])],
        ),
        (
            "SELECT * FROM ja JOIN jb ON ja.id = jb.a_id ORDER BY 5 DESC",
            vec!["id", "x", "s", "id", "a_id", "y"],
            vec![r(&["2", "NULL", "NULL", "8", "2", "NULL"]), r(&["1", "10", "one", "7", "1", "70"])],
        ),
    ];
    let mut wrong = Vec::new();
    for (sql, want_names, want_cells) in &cases {
        // Extended protocol (Parse, Describe, Execute), then simple query.
        for (protocol, rows) in [
            ("extended", sqlx::query(sqlx::AssertSqlSafe(*sql)).fetch_all(&pool).await),
            ("simple", sqlx::raw_sql(sqlx::AssertSqlSafe(*sql)).fetch_all(&pool).await),
        ] {
            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => {
                    wrong.push(format!("{protocol} {sql}: refused: {e}"));
                    continue;
                }
            };
            let got = names(&rows[0]);
            if got != *want_names {
                wrong.push(format!("{protocol} {sql}\n    names {got:?}\n    want  {want_names:?}"));
            }
            let got = cells(&rows);
            if got != *want_cells {
                wrong.push(format!("{protocol} {sql}\n    cells {got:?}\n    want  {want_cells:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), cases.len() * 2, wrong.join("\n"));
}
