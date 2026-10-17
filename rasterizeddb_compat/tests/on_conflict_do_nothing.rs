//! `INSERT … ON CONFLICT DO NOTHING` with no conflict target, against
//! PostgreSQL 17's answers.
//!
//! PostgreSQL skips a row that would violate *any* unique constraint, and the
//! command tag counts only the rows actually inserted. Here an insert that
//! named no column list (`INSERT INTO t VALUES (…)`) lost the clause: the
//! column list was supplied by rewriting the statement and parsing it again,
//! and the clause had already been split off the text being re-parsed. So the
//! statement raised 23505 for the very conflict it asked to skip, and a
//! multi-row one inserted nothing. `DO UPDATE` was lost the same way.
//!
//! Two neighbours of that form failed before reaching the clause at all:
//! `INSERT INTO t SELECT …` with no column list was refused as a schema
//! mismatch, and a prepared `INSERT INTO t VALUES ($1, $2)` described every
//! parameter as `text`, so a client that binds by the described type could
//! not send an integer.
//!
//! Every expected row and every expected tag below is what PostgreSQL 17
//! returned for the same statements.
//!
//! A partial unique index (`CREATE UNIQUE INDEX … WHERE …`) is not covered.
//! The engine accepts one as a declared no-op, so it enforces nothing with or
//! without the clause, and PostgreSQL's `INSERT 0 0` there is a missing
//! constraint rather than anything `ON CONFLICT` decides.

use rasterizeddb_compat::TestServer;
use std::collections::HashMap;

async fn client(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// One statement over the simple query protocol: its rows as text, and the
/// count from its command tag.
async fn simple(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<(Vec<Vec<Option<String>>>, u64), tokio_postgres::Error> {
    let mut rows = Vec::new();
    let mut tag = None;
    for message in client.simple_query(sql).await? {
        match message {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                rows.push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => tag = Some(n),
            _ => {}
        }
    }
    Ok((rows, tag.expect("every statement completes with a tag")))
}

/// An error as the server worded it; `Display` alone says only "db error".
fn why(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!("{}: {}", d.code().code(), d.message()),
        None => e.to_string(),
    }
}

/// The command tag's count for a statement that must succeed.
async fn tag(client: &tokio_postgres::Client, sql: &str) -> u64 {
    match simple(client, sql).await {
        Ok((_, n)) => n,
        Err(e) => panic!("{sql}: {}", why(&e)),
    }
}

/// A query's rows rendered `a|b`, as psql's unaligned output prints them.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let (rows, _) = simple(client, sql).await.unwrap_or_else(|e| panic!("{sql}: {}", why(&e)));
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| v.unwrap_or_default())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// Every insert form over the simple protocol, with no conflict target.
#[tokio::test(flavor = "multi_thread")]
async fn no_target_skips_every_unique_violation_over_the_simple_protocol() {
    let server = TestServer::start().await.expect("server should start");
    let c = client(&server).await;
    for sql in [
        "CREATE TABLE tu (code TEXT UNIQUE, n INT)",
        "CREATE TABLE tp (id INT PRIMARY KEY, v TEXT)",
        "CREATE TABLE tc (a INT, b TEXT, n INT, UNIQUE (a, b))",
        "CREATE TABLE tm (id INT PRIMARY KEY, email TEXT UNIQUE, n INT)",
        "CREATE TABLE ti (k TEXT, n INT)",
        "CREATE UNIQUE INDEX ti_k ON ti (k)",
        "CREATE TABLE src (code TEXT, n INT)",
    ] {
        c.batch_execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // A UNIQUE text column: single row with and without a column list, then
    // multi-row with conflicts against the table and within the statement
    // itself. PostgreSQL keeps the first of two equal rows in one statement.
    for (sql, expected) in [
        ("INSERT INTO tu (code, n) VALUES ('a', 1)", 1),
        ("INSERT INTO tu (code, n) VALUES ('a', 2) ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tu VALUES ('a', 3) ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tu (code, n) VALUES ('a', 4), ('b', 5), ('a', 6) ON CONFLICT DO NOTHING", 1),
        ("INSERT INTO tu VALUES ('c', 7), ('c', 8), ('d', 9) ON CONFLICT DO NOTHING", 2),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT code, n FROM tu ORDER BY code").await, ["a|1", "b|5", "c|7", "d|9"]);

    // A primary key, and RETURNING, which returns only the rows inserted.
    for (sql, expected) in [
        ("INSERT INTO tp VALUES (1, 'x')", 1),
        ("INSERT INTO tp VALUES (1, 'y') ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tp (id, v) VALUES (1, 'y'), (2, 'z') ON CONFLICT DO NOTHING", 1),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    let (returned, n) = simple(&c, "INSERT INTO tp VALUES (2, 'q'), (3, 'r') ON CONFLICT DO NOTHING RETURNING id, v")
        .await
        .expect("multi-row RETURNING");
    assert_eq!(returned, vec![vec![Some("3".to_string()), Some("r".to_string())]]);
    assert_eq!(n, 1);
    let (returned, n) = simple(&c, "INSERT INTO tp VALUES (3, 'q') ON CONFLICT DO NOTHING RETURNING id")
        .await
        .expect("single-row RETURNING");
    assert!(returned.is_empty(), "a skipped row was returned: {returned:?}");
    assert_eq!(n, 0);
    assert_eq!(rows(&c, "SELECT id, v FROM tp ORDER BY id").await, ["1|x", "2|z", "3|r"]);

    // A composite unique key.
    for (sql, expected) in [
        ("INSERT INTO tc VALUES (1, 'x', 0)", 1),
        ("INSERT INTO tc VALUES (1, 'x', 1) ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tc (a, b, n) VALUES (1, 'x', 2), (1, 'y', 3), (2, 'x', 4) ON CONFLICT DO NOTHING", 2),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT a, b, n FROM tc ORDER BY a, b").await, ["1|x|0", "1|y|3", "2|x|4"]);

    // Two constraints: a conflict on either one skips the row, which is what
    // naming no target means.
    for (sql, expected) in [
        ("INSERT INTO tm VALUES (1, 'e1', 0)", 1),
        ("INSERT INTO tm VALUES (2, 'e1', 1) ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tm VALUES (1, 'e2', 2) ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tm VALUES (2, 'e2', 3) ON CONFLICT DO NOTHING", 1),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT id, email, n FROM tm ORDER BY id").await, ["1|e1|0", "2|e2|3"]);

    // A unique index created on its own rather than declared with the table.
    assert_eq!(tag(&c, "INSERT INTO ti VALUES ('k', 1)").await, 1);
    assert_eq!(tag(&c, "INSERT INTO ti VALUES ('k', 2) ON CONFLICT DO NOTHING").await, 0);
    assert_eq!(rows(&c, "SELECT k, n FROM ti ORDER BY k").await, ["k|1"]);

    // INSERT … SELECT, with and without a column list. The source repeats
    // 'e', so the second 'e' conflicts with the first one this statement wrote.
    assert_eq!(tag(&c, "INSERT INTO src VALUES ('a', 10), ('e', 11), ('e', 12), ('f', 13)").await, 4);
    assert_eq!(
        tag(&c, "INSERT INTO tu (code, n) SELECT code, n FROM src ORDER BY n ON CONFLICT DO NOTHING").await,
        2
    );
    assert_eq!(tag(&c, "INSERT INTO tu SELECT code, n FROM src ON CONFLICT DO NOTHING").await, 0);
    assert_eq!(
        rows(&c, "SELECT code, n FROM tu ORDER BY code").await,
        ["a|1", "b|5", "c|7", "d|9", "e|11", "f|13"]
    );

    // With no column list the query fills the leading columns, NULLs travel
    // as NULLs, and a query with no FROM is one row. `ORDER BY b` puts the
    // NULL last, so of the two rows with `a = 1` it is 'dup' that lands.
    for sql in [
        "CREATE TABLE s (a INT, b TEXT, c INT)",
        "CREATE TABLE d (a INT UNIQUE, b TEXT, c INT)",
        "INSERT INTO s VALUES (1, 'x', NULL), (2, NULL, 5), (1, 'dup', 6)",
    ] {
        c.batch_execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {}", why(&e)));
    }
    for (sql, expected) in [
        ("INSERT INTO d SELECT * FROM s ORDER BY b ON CONFLICT DO NOTHING", 2),
        ("INSERT INTO d SELECT a + 10, b FROM s WHERE a = 2 ON CONFLICT DO NOTHING", 1),
        ("INSERT INTO d SELECT 20, 'k' ON CONFLICT DO NOTHING", 1),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    let (returned, n) = simple(&c, "INSERT INTO d SELECT 20, 'k' ON CONFLICT DO NOTHING RETURNING a")
        .await
        .expect("constant SELECT, skipped");
    assert!(returned.is_empty() && n == 0, "skipped: {returned:?}, tag {n}");
    let (returned, n) = simple(&c, "INSERT INTO d SELECT 21, 'k' ON CONFLICT DO NOTHING RETURNING a")
        .await
        .expect("constant SELECT, inserted");
    assert_eq!((returned, n), (vec![vec![Some("21".to_string())]], 1));
    assert_eq!(
        rows(&c, "SELECT a, b, c FROM d ORDER BY a").await,
        ["1|dup|6", "2||5", "12||", "20|k|", "21|k|"]
    );

    // Inside an explicit block: the skipped row must not abort it, and a row
    // the block wrote itself is a conflict too.
    c.batch_execute("BEGIN").await.unwrap();
    for (sql, expected) in [
        ("INSERT INTO tp VALUES (10, 'a')", 1),
        ("INSERT INTO tp VALUES (10, 'b') ON CONFLICT DO NOTHING", 0),
        ("INSERT INTO tp VALUES (1, 'b'), (11, 'c') ON CONFLICT DO NOTHING", 1),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    c.batch_execute("COMMIT").await.expect("the block commits");
    assert_eq!(rows(&c, "SELECT id, v FROM tp ORDER BY id").await, ["1|x", "2|z", "3|r", "10|a", "11|c"]);
}

/// The same over the extended protocol, with parameters bound.
#[tokio::test(flavor = "multi_thread")]
async fn no_target_skips_unique_violations_over_the_extended_protocol() {
    let server = TestServer::start().await.expect("server should start");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE xp (id INT PRIMARY KEY, v TEXT)").await.unwrap();

    let p1 = "INSERT INTO xp VALUES ($1, $2) ON CONFLICT DO NOTHING";
    assert_eq!(c.execute(p1, &[&1i32, &"a"]).await.expect("first"), 1);
    assert_eq!(c.execute(p1, &[&1i32, &"b"]).await.expect("conflicting"), 0);

    let p2 = "INSERT INTO xp (id, v) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING id, v";
    let skipped = c.query(p2, &[&1i32, &"c"]).await.expect("conflicting RETURNING");
    assert!(skipped.is_empty(), "a skipped row was returned");
    let inserted = c.query(p2, &[&2i32, &"d"]).await.expect("RETURNING");
    assert_eq!(inserted.len(), 1);
    assert_eq!((inserted[0].get::<_, i32>(0), inserted[0].get::<_, String>(1)), (2, "d".to_string()));

    let p3 = "INSERT INTO xp VALUES ($1, $2), ($3, $4) ON CONFLICT DO NOTHING";
    assert_eq!(c.execute(p3, &[&2i32, &"e", &3i32, &"f"]).await.expect("multi-row"), 1);

    assert_eq!(rows(&c, "SELECT id, v FROM xp ORDER BY id").await, ["1|a", "2|d", "3|f"]);
}

/// `DO UPDATE` was lost from a column-list-less insert by the same re-parse.
#[tokio::test(flavor = "multi_thread")]
async fn do_update_without_a_column_list_keeps_its_clause() {
    let server = TestServer::start().await.expect("server should start");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE xu (id INT PRIMARY KEY, n INT)").await.unwrap();
    for (sql, expected) in [
        ("INSERT INTO xu VALUES (1, 1)", 1),
        ("INSERT INTO xu VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = xu.n + EXCLUDED.n", 1),
        ("INSERT INTO xu VALUES (1, 5), (2, 7) ON CONFLICT (id) DO UPDATE SET n = xu.n + EXCLUDED.n", 2),
    ] {
        assert_eq!(tag(&c, sql).await, expected, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT id, n FROM xu ORDER BY id").await, ["1|11", "2|7"]);
}

/// A `DO NOTHING` insert of a key another transaction holds uncommitted.
///
/// PostgreSQL waits for the holder: if it commits the insert does nothing
/// (`INSERT 0 0`), and if it rolls back the insert goes ahead (`INSERT 0 1`).
/// Neither outcome is an error. This engine replays the statement rather than
/// waiting, so what is pinned is the answer after the replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_held_by_an_open_transaction_is_waited_for() {
    let server = TestServer::start().await.expect("server should start");
    let holder = client(&server).await;
    holder
        .batch_execute("CREATE TABLE hk (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    // (how the holder ends, whether the waiter is in a block, the key)
    for (commits, waiter_in_block, k) in [(true, false, 1), (false, false, 2), (true, true, 3), (false, true, 4)] {
        holder.batch_execute("BEGIN").await.unwrap();
        assert_eq!(tag(&holder, &format!("INSERT INTO hk VALUES ({k}, 'holder')")).await, 1);

        let url = server.url();
        let waiter = tokio::spawn(async move {
            let (c, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
            tokio::spawn(conn);
            if waiter_in_block {
                c.batch_execute("BEGIN").await.unwrap();
            }
            let outcome = simple(&c, &format!("INSERT INTO hk VALUES ({k}, 'waiter') ON CONFLICT DO NOTHING"))
                .await
                .map(|(_, n)| n)
                .map_err(|e| why(&e));
            if waiter_in_block {
                let _ = c.batch_execute(if outcome.is_ok() { "COMMIT" } else { "ROLLBACK" }).await;
            }
            outcome
        });
        // Long enough for the waiter to have met the uncommitted row, and
        // well inside the time it keeps replaying for.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        holder
            .batch_execute(if commits { "COMMIT" } else { "ROLLBACK" })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), waiter)
            .await
            .expect("the waiting insert must finish")
            .unwrap();
        let (expected_tag, expected_row) = if commits { (0, "holder") } else { (1, "waiter") };
        assert_eq!(
            outcome,
            Ok(expected_tag),
            "holder {}, waiter {}",
            if commits { "commits" } else { "rolls back" },
            if waiter_in_block { "in a block" } else { "in autocommit" }
        );
        assert_eq!(rows(&holder, &format!("SELECT v FROM hk WHERE id = {k}")).await, [expected_row]);
    }
}

const CLIENTS: i32 = 12;
const KEYS: i32 = 30;

/// How one client writes key `k` into `table`: the shapes rotate so each key
/// is contended by every form at once.
fn statement(table: &str, form: i32, k: i32, me: i32) -> String {
    let row = match table {
        "cp" => format!("({k}, {me})"),
        "cu" => format!("('k{k}', {me})"),
        _ => format!("({k}, 'b{k}', {me})"),
    };
    let columns = match table {
        "cc" => "(a, b, v)",
        "cu" => "(code, v)",
        _ => "(id, v)",
    };
    match form % 3 {
        // No column list: the form whose clause was lost.
        0 => format!("INSERT INTO {table} VALUES {row} ON CONFLICT DO NOTHING"),
        1 => format!("INSERT INTO {table} {columns} VALUES {row} ON CONFLICT DO NOTHING"),
        // The same key twice in one statement, which can insert at most once.
        _ => format!("INSERT INTO {table} VALUES {row}, {row} ON CONFLICT DO NOTHING"),
    }
}

/// One client's run: for each statement, the table, the key and the count its
/// tag reported. Returned with the client's id, because a `JoinSet` hands
/// results back in the order the tasks finish, not the order they started.
async fn contend(url: String, me: i32) -> (i32, Result<Vec<(String, i32, u64)>, String>) {
    (me, contend_as(url, me).await)
}

async fn contend_as(url: String, me: i32) -> Result<Vec<(String, i32, u64)>, String> {
    let (c, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(conn);
    let mut won = Vec::new();
    // Every client walks the keys in the same order, so each key is written
    // by all of them at about the same moment rather than in turn.
    for k in 0..KEYS {
        for table in ["cp", "cu", "cc"] {
            let sql = statement(table, me + k, k, me);
            // A quarter of the clients wrap each statement in a block that
            // commits, so the in-transaction path is contended too, and a
            // quarter in one that rolls back. A rolled-back row must not make
            // anyone else skip the key: if every other writer had taken it
            // for a conflict, the key would end up stored by nobody.
            let in_block = me % 2 == 0;
            let rolls_back = me % 4 == 0;
            let result = async {
                if in_block {
                    c.batch_execute("BEGIN").await?;
                }
                let n = simple(&c, &sql).await?.1;
                if rolls_back {
                    c.batch_execute("ROLLBACK").await?;
                } else if in_block {
                    c.batch_execute("COMMIT").await?;
                }
                Ok::<u64, tokio_postgres::Error>(n)
            }
            .await;
            match result {
                Ok(_) if rolls_back => {}
                Ok(n) => won.push((table.to_string(), k, n)),
                Err(e) => {
                    if in_block {
                        let _ = c.batch_execute("ROLLBACK").await;
                    }
                    return Err(format!("client {me}: {sql}: {}", why(&e)));
                }
            }
        }
    }
    Ok(won)
}

/// Concurrent `DO NOTHING` inserts of one key: exactly one statement inserts
/// it, every other reports zero rows, none errors, and the row stored is the
/// winner's.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_of_one_key_insert_it_once() {
    let server = TestServer::start().await.expect("server should start");
    let c = client(&server).await;
    for sql in [
        "CREATE TABLE cp (id INT PRIMARY KEY, v INT)",
        "CREATE TABLE cu (code TEXT UNIQUE, v INT)",
        "CREATE TABLE cc (a INT, b TEXT, v INT, UNIQUE (a, b))",
    ] {
        c.batch_execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let mut set = tokio::task::JoinSet::new();
    for me in 1..=CLIENTS {
        set.spawn(contend(server.url(), me));
    }
    let outcomes = tokio::time::timeout(std::time::Duration::from_secs(120), set.join_all())
        .await
        .expect("the contended inserts must finish within two minutes");

    // (table, key) -> the clients whose statement reported a row inserted.
    let mut winners: HashMap<(String, i32), Vec<i32>> = HashMap::new();
    let mut errors = Vec::new();
    for (me, outcome) in outcomes {
        match outcome {
            Ok(won) => {
                for (table, k, n) in won {
                    assert!(n <= 1, "client {me} reported {n} rows for one key of {table}");
                    let entry = winners.entry((table, k)).or_default();
                    if n == 1 {
                        entry.push(me);
                    }
                }
            }
            Err(e) => errors.push(e),
        }
    }
    assert!(errors.is_empty(), "DO NOTHING raised errors:\n{}", errors.join("\n"));

    for (table, key) in [("cp", "id"), ("cu", "code"), ("cc", "a")] {
        let stored = rows(&c, &format!("SELECT {key}, v FROM {table} ORDER BY {key}")).await;
        assert_eq!(stored.len(), KEYS as usize, "{table} holds {} rows for {KEYS} keys: {stored:?}", stored.len());
        for line in stored {
            let (k, v) = line.split_once('|').unwrap();
            let k: i32 = k.trim_start_matches('k').parse().unwrap();
            let v: i32 = v.parse().unwrap();
            let won = &winners[&(table.to_string(), k)];
            assert_eq!(won, &vec![v], "{table} key {k}: stored client {v}'s row, but these reported inserting it: {won:?}");
        }
    }
}
