//! A join hands a small relation's keys to the scan of the relation it joins
//! (`join::key_restriction`), so a point lookup joined to its parent reads one
//! parent through the index and not the whole table. The restriction may only
//! remove rows that reach no output tuple, so every join must answer exactly
//! as it does with the pushdown off — which is the join every other suite
//! holds to PostgreSQL.
//!
//! One test, in a binary of its own: the switch is process-wide.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::rql::join::{key_pushdowns, set_key_pushdown};
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

/// Every row of the answer, cells as text and NULL as `∅`, sorted unless the
/// statement orders them itself.
async fn rows(c: &Client, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut out: Vec<Vec<String>> = c
        .simple_query(sql)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("∅").to_string()).collect())
            }
            _ => None,
        })
        .collect();
    if !sql.contains("ORDER BY") {
        out.sort();
    }
    Ok(out)
}

async fn run(c: &Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Parents 1..=6000 (and -3), children 1..=9000 whose parent is NULL for every
/// eleventh and names a parent that does not exist for some, tags 1..=300
/// naming children the same way. The two large tables are past the filter's
/// `min_table_rows`, so a key on a column with no index is narrowed by a mask.
async fn seed(c: &Client) {
    run(c, "CREATE TABLE kp_parent (id INT PRIMARY KEY, grp INT, name TEXT NOT NULL)").await;
    run(c, "CREATE TABLE kp_child (id BIGINT PRIMARY KEY, parent INT, alt INT, qty INT NOT NULL)").await;
    run(c, "CREATE TABLE kp_tag (id INT PRIMARY KEY, child BIGINT, label TEXT NOT NULL)").await;
    for chunk in (1..=6000).collect::<Vec<i64>>().chunks(500) {
        let values: Vec<String> = chunk
            .iter()
            .map(|id| {
                let grp = if id % 17 == 0 { "NULL".to_string() } else { (id % 50).to_string() };
                format!("({id}, {grp}, 'n{id}')")
            })
            .collect();
        run(c, &format!("INSERT INTO kp_parent (id, grp, name) VALUES {}", values.join(", "))).await;
    }
    run(c, "INSERT INTO kp_parent (id, grp, name) VALUES (-3, 1, 'minus')").await;
    for chunk in (1..=9000).collect::<Vec<i64>>().chunks(500) {
        let values: Vec<String> = chunk
            .iter()
            .map(|id| {
                let parent = if id % 11 == 0 { "NULL".to_string() } else { ((id * 7) % 6500).to_string() };
                let alt = if id % 13 == 0 { "NULL".to_string() } else { (((id * 7) % 6500) % 50).to_string() };
                format!("({id}, {parent}, {alt}, {})", id % 9)
            })
            .collect();
        run(c, &format!("INSERT INTO kp_child (id, parent, alt, qty) VALUES {}", values.join(", "))).await;
    }
    run(c, "INSERT INTO kp_child (id, parent, alt, qty) VALUES (9001, -3, 1, 4), (9002, -3, 2, 5)").await;
    let values: Vec<String> = (1..=300i64)
        .map(|id| {
            let child = if id % 7 == 0 { "NULL".to_string() } else { ((id * 31) % 9500).to_string() };
            format!("({id}, {child}, 'l{}')", id % 5)
        })
        .collect();
    run(c, &format!("INSERT INTO kp_tag (id, child, label) VALUES {}", values.join(", "))).await;
}

/// `(statement, whether a scan must have been restricted by keys)`.
const STATEMENTS: &[(&str, bool)] = &[
    // The shape this exists for: one child, its parent through the index.
    ("SELECT c.id, c.qty, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = 42", true),
    // Several children: a NULL parent (11, 22), a parent that is not there.
    ("SELECT c.id, p.id, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id IN (1, 2, 3, 11, 22, 900, 8999, 9001)", true),
    // Written the other way round: the parent is attached first in the text.
    ("SELECT p.id, c.id FROM kp_parent p JOIN kp_child c ON c.parent = p.id WHERE c.id = 42", true),
    // The key lands on a column with no index: the filter's mask narrows it.
    ("SELECT p.id, c.id FROM kp_parent p JOIN kp_child c ON c.parent = p.id WHERE p.id = 7", false),
    ("SELECT p.id, c.id, c.qty FROM kp_parent p JOIN kp_child c ON c.parent = p.id WHERE p.id IN (7, 8, 9, -3, 5999)", false),
    // LEFT: the attached side is restricted, the preserved side never.
    ("SELECT c.id, p.id FROM kp_child c LEFT JOIN kp_parent p ON p.id = c.parent WHERE c.id IN (1, 11, 900, 929, 8999)", true),
    ("SELECT p.id, c.id FROM kp_parent p LEFT JOIN kp_child c ON c.parent = p.id WHERE p.id IN (5, 5999, 6000) AND c.qty > 3", false),
    ("SELECT p.id, c.id FROM kp_parent p LEFT JOIN kp_child c ON c.parent = p.id AND c.qty > 3 WHERE p.id IN (5, 5999, 6000)", false),
    // The anti-join: parents with no child.
    ("SELECT p.id FROM kp_parent p LEFT JOIN kp_child c ON c.parent = p.id WHERE p.id BETWEEN 100 AND 160 AND c.id IS NULL", false),
    ("SELECT c.id FROM kp_child c LEFT JOIN kp_parent p ON p.id = c.parent WHERE c.id BETWEEN 900 AND 960 AND p.id IS NULL", true),
    // RIGHT and FULL: a preserved side is scanned whole.
    ("SELECT p.id, c.id FROM kp_parent p RIGHT JOIN kp_child c ON c.parent = p.id WHERE c.id IN (1, 11, 900, 929)", false),
    ("SELECT p.id, c.id FROM kp_parent p RIGHT JOIN kp_child c ON c.parent = p.id WHERE p.id = 7", false),
    ("SELECT count(*), count(p.id), count(c.id) FROM kp_parent p FULL JOIN kp_child c ON c.parent = p.id", false),
    ("SELECT p.id, c.id FROM kp_parent p FULL JOIN kp_child c ON c.parent = p.id WHERE c.id IN (1, 11) OR p.id = 5999", false),
    // Three relations, the small one at either end.
    ("SELECT t.id, c.id, p.id FROM kp_tag t JOIN kp_child c ON c.id = t.child JOIN kp_parent p ON p.id = c.parent WHERE t.id IN (1, 2, 3, 4, 5, 7)", true),
    ("SELECT p.id, c.id, t.id FROM kp_parent p JOIN kp_child c ON c.parent = p.id JOIN kp_tag t ON t.child = c.id WHERE t.id = 9", true),
    ("SELECT p.id, c.id, t.label FROM kp_parent p LEFT JOIN kp_child c ON c.parent = p.id JOIN kp_tag t ON t.id = p.id WHERE t.id < 4", true),
    // A relation an outer join preserves, then joined inner to a small one.
    ("SELECT p.id, c.id, t.id FROM kp_parent p RIGHT JOIN kp_child c ON c.parent = p.id JOIN kp_tag t ON t.child = c.id WHERE t.id < 12", true),
    ("SELECT p.id, c.id, t.id FROM kp_parent p FULL JOIN kp_child c ON c.parent = p.id JOIN kp_tag t ON t.child = c.id WHERE t.id < 12", true),
    ("SELECT p.id, c.id, t.id FROM kp_parent p JOIN kp_child c ON c.parent = p.id LEFT JOIN kp_tag t ON t.child = c.id WHERE p.id IN (217, 434)", false),
    // Nothing on the small side: nothing can pair.
    ("SELECT c.id, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = -5", true),
    ("SELECT c.id, p.name FROM kp_child c LEFT JOIN kp_parent p ON p.id = c.parent WHERE c.id = 11", true),
    // A composite key: one term per column.
    ("SELECT c.id, p.id FROM kp_child c JOIN kp_parent p ON p.id = c.parent AND p.grp = c.alt WHERE c.id < 40", true),
    // The restricted relation has a predicate of its own, with an OR in it.
    ("SELECT c.id, p.id FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id IN (1, 2, 3, 4, 5, 6) AND (p.grp = 7 OR p.name = 'n14' OR p.grp = 21)", true),
    // An aggregate, an ordering and a limit over the join.
    ("SELECT count(*), sum(c.qty), min(c.id), max(c.id) FROM kp_parent p JOIN kp_child c ON c.parent = p.id WHERE p.id IN (1, 2, 3)", false),
    ("SELECT c.id, p.grp FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE p.grp = 7 ORDER BY c.id DESC LIMIT 5", false),
    ("SELECT p.grp, count(*) FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id < 60 GROUP BY p.grp ORDER BY p.grp", true),
    // A relation joined to itself, and one whose key is of another width.
    ("SELECT a.id, b.id FROM kp_child a JOIN kp_child b ON b.parent = a.parent WHERE a.id = 42", false),
    ("SELECT t.id, c.qty FROM kp_tag t JOIN kp_child c ON c.id = t.id WHERE t.id BETWEEN 10 AND 20", true),
    // A derived relation as the small side.
    ("WITH x AS (SELECT id, parent FROM kp_child WHERE id < 6) SELECT x.id, p.name FROM x JOIN kp_parent p ON p.id = x.parent", true),
    // The preserved side of an outer join is never restricted, however small
    // the other side came back: every parent is in each of these answers.
    ("SELECT count(*), count(t.id) FROM kp_parent p LEFT JOIN kp_tag t ON t.id = p.id AND t.id < 4", false),
    ("WITH x AS (SELECT id FROM kp_tag WHERE id < 4) SELECT count(*), count(x.id) FROM x RIGHT JOIN kp_parent p ON p.id = x.id", false),
    ("WITH x AS (SELECT id FROM kp_tag WHERE id < 4) SELECT count(*), count(x.id), count(p.id) FROM x FULL JOIN kp_parent p ON p.id = x.id", false),
    ("WITH x AS (SELECT id FROM kp_tag WHERE id < 4) SELECT count(*), count(x.id), count(c.id) FROM x RIGHT JOIN kp_parent p ON p.id = x.id LEFT JOIN kp_child c ON c.parent = p.id AND c.id < 3", false),
    // More keys than are handed over: scanned whole, as before.
    ("SELECT count(*) FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id < 2000", false),
];

#[tokio::test(flavor = "multi_thread")]
async fn a_join_answers_the_same_with_its_keys_pushed_down() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seed(&c).await;

    let mut wrong: Vec<String> = Vec::new();
    for (sql, must_push) in STATEMENTS {
        set_key_pushdown(false);
        let before = key_pushdowns();
        let plain = rows(&c, sql).await;
        assert_eq!(key_pushdowns(), before, "{sql}: restricted a scan with the pushdown off");

        set_key_pushdown(true);
        let pushed = rows(&c, sql).await;
        let fired = key_pushdowns() > before;
        if plain != pushed {
            wrong.push(format!("{sql}\n   plain:  {plain:?}\n   pushed: {pushed:?}"));
        }
        if *must_push && !fired {
            wrong.push(format!("{sql}\n   no scan was restricted by a key"));
        }
        if let Ok(plain) = &plain {
            assert!(plain.len() < 20_000, "{sql}: an unexpectedly large answer");
        }
    }

    // Inside a transaction the restricted scan sees the transaction's own
    // rows, as the plain one does.
    for on in [false, true] {
        set_key_pushdown(on);
        run(&c, "BEGIN").await;
        run(&c, "INSERT INTO kp_parent (id, grp, name) VALUES (70001, 3, 'mine')").await;
        run(&c, "INSERT INTO kp_child (id, parent, alt, qty) VALUES (70001, 70001, 3, 1)").await;
        let seen = rows(&c, "SELECT c.id, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = 70001").await;
        run(&c, "ROLLBACK").await;
        if seen != Ok(vec![vec!["70001".to_string(), "mine".to_string()]]) {
            wrong.push(format!("own writes through a join, pushdown {on}: {seen:?}"));
        }
        let gone = rows(&c, "SELECT c.id, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = 70001").await;
        if gone != Ok(Vec::new()) {
            wrong.push(format!("rolled-back rows through a join, pushdown {on}: {gone:?}"));
        }
    }
    set_key_pushdown(true);

    // A few answers by value, so "the same" is not "the same wrong thing".
    for (sql, expected) in [
        ("SELECT c.id, c.qty, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = 42", vec![vec!["42", "6", "n294"]]),
        ("SELECT c.id, p.id FROM kp_child c LEFT JOIN kp_parent p ON p.id = c.parent WHERE c.id IN (1, 11, 9001)", vec![vec!["1", "7"], vec!["11", "∅"], vec!["9001", "-3"]]),
        ("SELECT p.id, c.id FROM kp_parent p JOIN kp_child c ON c.parent = p.id WHERE p.id = -3", vec![vec!["-3", "9001"], vec!["-3", "9002"]]),
        ("SELECT c.id, p.name FROM kp_child c JOIN kp_parent p ON p.id = c.parent WHERE c.id = -5", vec![]),
        ("SELECT count(*), count(t.id) FROM kp_parent p LEFT JOIN kp_tag t ON t.id = p.id AND t.id < 4", vec![vec!["6001", "3"]]),
        ("WITH x AS (SELECT id FROM kp_tag WHERE id < 4) SELECT count(*), count(x.id) FROM x RIGHT JOIN kp_parent p ON p.id = x.id", vec![vec!["6001", "3"]]),
    ] {
        let got = rows(&c, sql).await;
        let expected: Vec<Vec<String>> = expected.iter().map(|r| r.iter().map(|v| v.to_string()).collect()).collect();
        if got != Ok(expected.clone()) {
            wrong.push(format!("{sql}\n   expected: {expected:?}\n   got:      {got:?}"));
        }
    }

    assert!(wrong.is_empty(), "{} disagreements:\n{}", wrong.len(), wrong.join("\n"));
}
