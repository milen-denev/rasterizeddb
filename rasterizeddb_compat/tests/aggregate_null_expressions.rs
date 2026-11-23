//! An expression over an aggregate that is NULL — a group whose values are
//! all NULL, or no rows at all — is NULL wherever SQL says it is:
//! `NULL || '|'` is NULL, and so is `upper(NULL)` and `NULL::text`.
//!
//! The fold evaluated such an expression correctly and then emitted its
//! NULL as empty bytes typed as the expression. For a number that happened
//! to read back as NULL, since no fixed-width value is zero bytes long; for
//! text it is the value `''`. So `min(s) || '|'` over an all-NULL group
//! answered `''`, sorted as `''` under `NULLS LAST`, matched `s = ''` from a
//! scalar subquery, and an `INSERT … SELECT` stored `''` in place of NULL.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement.

use rasterizeddb_compat::TestServer;

async fn client(server: &TestServer) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

/// The rows of `sql` (simple protocol, text), one string per row with the
/// cells joined by `|` and NULL as `∅`, or the SQLSTATE it failed with.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, String> {
    match client.simple_query(sql).await {
        Ok(messages) => Ok(messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                    (0..r.len())
                        .map(|i| r.get(i).map(|v| format!("[{v}]")).unwrap_or_else(|| "∅".to_string()))
                        .collect::<Vec<_>>()
                        .join("|"),
                ),
                _ => None,
            })
            .collect()),
        Err(e) => Err(e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string())),
    }
}

/// Runs each `(sql, want)` and reports every answer that differs. `want` is
/// the rows joined by `;`, or `ERR <sqlstate>`.
async fn check(client: &tokio_postgres::Client, cases: &[(&str, &str)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        let got = match rows(client, sql).await {
            Ok(rows) => rows.join(";"),
            Err(code) => format!("ERR {code}"),
        };
        if got != *want {
            wrong.push(format!("{sql}\n    got  {got}\n    want {want}"));
        }
    }
    wrong
}

const SETUP: &str = "CREATE TABLE t (id INT, g INT, s TEXT, n NUMERIC(10,2), i INT, b BIGINT); \
     INSERT INTO t VALUES (1, 1, NULL, NULL, NULL, NULL), (2, 1, NULL, NULL, NULL, NULL), \
     (3, 2, 'b', 1.50, 3, 30), (4, 2, NULL, NULL, NULL, NULL), (5, 2, 'a', 2.25, 4, 40)";

/// Group 1 is all NULL; group 2 has values. min/max/sum/avg over text,
/// numeric and integers, under `||`, functions, casts, arithmetic,
/// `coalesce` and CASE, grouped and not, and in `ORDER BY … NULLS LAST`.
///
/// Mutation: `emit_group` typing a NULL computed result as the expression's
/// type again (every text case here answers `[]`; the numeric ones still
/// pass, which is why the bug hid).
#[tokio::test(flavor = "multi_thread")]
async fn an_expression_over_a_null_aggregate_is_null() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(SETUP).await.expect("setup");
    let wrong = check(
        &c,
        &[
            ("SELECT g, min(s) || '|' FROM t GROUP BY g ORDER BY g", "[1]|∅;[2]|[a|]"),
            ("SELECT g, max(s) || '|', '|' || min(s) FROM t GROUP BY g ORDER BY g", "[1]|∅|∅;[2]|[b|]|[|a]"),
            ("SELECT g, max(s) || min(s) || '|' FROM t GROUP BY g ORDER BY g", "[1]|∅;[2]|[ba|]"),
            ("SELECT g, upper(max(s)), length(min(s)) FROM t GROUP BY g ORDER BY g", "[1]|∅|∅;[2]|[B]|[1]"),
            ("SELECT g, min(s)::text, max(i)::text FROM t GROUP BY g ORDER BY g", "[1]|∅|∅;[2]|[a]|[4]"),
            ("SELECT g, min(n) + 1, max(n) * 2, sum(n) - 1 FROM t WHERE g = 1 GROUP BY g", "[1]|∅|∅|∅"),
            (
                "SELECT g, min(i) + 1, max(i) * 2, sum(i) - 1, max(b) - min(b), -sum(i) FROM t GROUP BY g ORDER BY g",
                "[1]|∅|∅|∅|∅|∅;[2]|[4]|[8]|[6]|[10]|[-7]",
            ),
            ("SELECT g, avg(i) + 1, avg(n) * 2 FROM t WHERE g = 1 GROUP BY g", "[1]|∅|∅"),
            (
                "SELECT g, coalesce(min(s), 'x'), coalesce(max(s), min(s), 'y'), coalesce(sum(i), -1) FROM t GROUP BY g ORDER BY g",
                "[1]|[x]|[y]|[-1];[2]|[a]|[b]|[7]",
            ),
            (
                "SELECT g, coalesce(min(s) || '|', 'z'), coalesce(sum(i) + 1, -5) FROM t GROUP BY g ORDER BY g",
                "[1]|[z]|[-5];[2]|[a|]|[8]",
            ),
            (
                "SELECT g, CASE WHEN min(s) IS NULL THEN 'none' ELSE min(s) END, \
                 CASE WHEN max(i) > 3 THEN 'big' ELSE 'other' END, \
                 CASE WHEN sum(n) IS NULL THEN 0 ELSE 1 END, \
                 CASE WHEN avg(i) IS NULL THEN 'none' ELSE 'some' END FROM t GROUP BY g ORDER BY g",
                "[1]|[none]|[other]|[0]|[none];[2]|[a]|[big]|[1]|[some]",
            ),
            (
                "SELECT g, CASE WHEN max(i) > 3 THEN max(s) || '!' END FROM t GROUP BY g ORDER BY g",
                "[1]|∅;[2]|[b!]",
            ),
            // No GROUP BY: over the all-NULL rows, and over no rows at all.
            (
                "SELECT min(s) || '|', sum(i) + 1, coalesce(max(s), 'x'), max(n) * 2, avg(i) + 1 FROM t WHERE g = 1",
                "∅|∅|[x]|∅|∅",
            ),
            ("SELECT min(s) || '|', sum(i) + 1, coalesce(max(s), 'x'), max(b) + 1 FROM t WHERE id > 100", "∅|∅|[x]|∅"),
            ("SELECT min(s) || '|' FROM t WHERE g = 1 HAVING count(*) > 0", "∅"),
            // NULL sorts last; `''` sorted first.
            ("SELECT g, min(s) || '|' AS m FROM t GROUP BY g ORDER BY m NULLS LAST, g", "[2]|[a|];[1]|∅"),
            ("SELECT g, min(s) || '|' AS m FROM t GROUP BY g ORDER BY m DESC NULLS LAST", "[2]|[a|];[1]|∅"),
            // A NULL from a scalar subquery matches nothing; `''` matched the
            // empty string.
            ("INSERT INTO t VALUES (6, 3, '', NULL, 0, NULL)", ""),
            ("SELECT id FROM t WHERE s = (SELECT min(s) || '' FROM t WHERE g = 1) ORDER BY id", ""),
            ("SELECT id FROM t WHERE s IN (SELECT min(s) || '' FROM t GROUP BY g) ORDER BY id", "[5];[6]"),
            // Over a join, which folds through the same emit.
            (
                "SELECT a.g, min(b.s) || '|', coalesce(max(b.s), 'x'), sum(b.i) + 1 FROM t a JOIN t b ON a.id = b.id \
                 WHERE a.g < 3 GROUP BY a.g ORDER BY a.g",
                "[1]|∅|[x]|∅;[2]|[a|]|[b]|[8]",
            ),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `INSERT … SELECT` of such an expression stores NULL, as read back after
/// a restart; it stored `''`. `UPDATE … SET` from a scalar subquery over
/// the all-NULL group stores NULL too, and both are checked by `IS NULL`
/// rather than by how the value prints.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_aggregate_expression_is_stored_as_null() {
    let root = std::env::temp_dir().join(format!("agg_null_expr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let root_text = root.to_string_lossy().into_owned();
    {
        let server = TestServer::start_file_backed(&root_text).await.expect("server");
        let c = client(&server).await;
        c.batch_execute(SETUP).await.expect("setup");
        c.batch_execute(
            "CREATE TABLE u (id INT, s TEXT, k INT); \
             INSERT INTO u SELECT g, min(s) || '|', max(i) * 2 FROM t GROUP BY g; \
             INSERT INTO u SELECT 9, coalesce(max(s), 'none') || '|', sum(i) + 1 FROM t WHERE g = 1; \
             INSERT INTO u VALUES (10, 'keep', 1); \
             UPDATE u SET s = (SELECT min(s) FROM t WHERE g = 1) || '|' WHERE id = 10",
        )
        .await
        .expect("write");
    }
    let server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let c = client(&server).await;
    let wrong = check(
        &c,
        &[
            (
                "SELECT id, s, s IS NULL, k IS NULL FROM u ORDER BY id",
                "[1]|∅|[t]|[t];[2]|[a|]|[f]|[f];[9]|[none|]|[f]|[t];[10]|∅|[t]|[f]",
            ),
            ("SELECT count(*) FROM u WHERE s = ''", "[0]"),
            ("SELECT count(s), count(*) FROM u", "[2]|[4]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
