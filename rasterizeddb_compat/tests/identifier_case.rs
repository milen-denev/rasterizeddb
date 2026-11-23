//! Table names in any case, as PostgreSQL folds an unquoted identifier.
//!
//! `CREATE TABLE Acct` and then `FROM acct` name one table in PostgreSQL;
//! here every later statement was 42P01 ("Table not found"). A name now
//! resolves exactly first, then to the one table equal to it ignoring ASCII
//! case (`Database::resolve`); the collision guard never lets two tables
//! differ only by case, so the match is unique.
//!
//! The hazard a fold in one place brings is a lock keyed by the spelling:
//! `UPDATE Acct` and `UPDATE acct` would lock two entries, and a second
//! writer would not wait for the first. Row and table locks are keyed by the
//! folded name (`mvcc::fold_lock_name`); the second test holds a lock under
//! one spelling and writes under another.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
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

#[tokio::test(flavor = "multi_thread")]
async fn an_unquoted_name_reaches_its_table_in_any_case() {
    let root = std::env::temp_dir().join(format!("identifier_case_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE Acct (id INT PRIMARY KEY, owner TEXT, bal INT)",
        "INSERT INTO acct VALUES (1, 'a', 10), (2, 'b', 20)",
        "INSERT INTO ACCT VALUES (3, 'c', 30)",
        "UPDATE aCCT SET bal = bal + 1 WHERE id = 1",
        "DELETE FROM acct WHERE id = 3",
        "CREATE TABLE Tx (id INT PRIMARY KEY, acct_id INT REFERENCES acct(id))",
        "INSERT INTO tx VALUES (1, 1)",
        "ALTER TABLE acct ADD COLUMN note TEXT",
        "UPDATE ACCT SET note = 'n' WHERE id = 2",
        "CREATE INDEX acct_owner ON ACCT (owner)",
        // It exists, under another spelling: success with nothing done.
        "CREATE TABLE IF NOT EXISTS acct (id INT)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, owner, bal, note FROM acct ORDER BY id", &["1|a|11|NULL", "2|b|20|n"]),
        ("SELECT count(*) FROM \"Acct\"", &["2"]),
        ("SELECT a.owner, t.id FROM ACCT a JOIN tx t ON a.id = t.acct_id", &["a|1"]),
        ("SELECT owner FROM acct WHERE owner = 'b'", &["b"]),
        ("SELECT id FROM acct WHERE id IN (SELECT acct_id FROM TX)", &["1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    // The foreign key holds whichever spelling each side uses.
    assert_eq!(code(&c, "INSERT INTO TX VALUES (2, 99)").await, "23503");
    assert_eq!(code(&c, "DELETE FROM ACCT WHERE id = 1").await, "23503");
    // A second table of the same name in another case is the same name.
    assert_eq!(code(&c, "CREATE TABLE acct (id INT)").await, "42P07");
    c.simple_query("DROP TABLE tx").await.expect("drop tx");
    c.simple_query("DROP TABLE acct").await.expect("drop acct");
    assert_eq!(code(&c, "SELECT * FROM Acct").await, "42P01");
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

async fn connect(server: &TestServer) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(&server.url()).await.expect("connect")
}

async fn exec(conn: &mut sqlx::PgConnection, sql: &str) -> Result<(), String> {
    conn.execute(AssertSqlSafe(sql.to_string())).await.map(|_| ()).map_err(|e| e.to_string())
}

/// A row held `FOR UPDATE` under one spelling is not overwritten under
/// another, and a table lock under one spelling conflicts with a request
/// under another.
#[tokio::test(flavor = "multi_thread")]
async fn locks_taken_under_one_spelling_hold_against_another() {
    let server = TestServer::start().await.expect("server");
    let mut a = connect(&server).await;
    exec(&mut a, "CREATE TABLE Queue (id INT PRIMARY KEY, state TEXT NOT NULL)").await.expect("create");
    exec(&mut a, "INSERT INTO queue (id, state) VALUES (1, 'pending')").await.expect("insert");

    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "SELECT id FROM QUEUE WHERE id = 1 FOR UPDATE").await.expect("lock");
    let mut b = connect(&server).await;
    let outcome = exec(&mut b, "UPDATE queue SET state = 'stolen' WHERE id = 1").await;
    exec(&mut a, "COMMIT").await.expect("commit");
    // Refused as a lock conflict, not as a table it could not find.
    let e = outcome.expect_err("an UPDATE under another spelling overwrote a row held FOR UPDATE");
    assert!(!e.contains("not found") && !e.contains("does not exist"), "{e}");
    assert!(e.contains("lock") || e.contains("serializ") || e.contains("concurrent"), "{e}");
    let state: String = sqlx::query("SELECT state FROM Queue WHERE id = 1")
        .fetch_one(&mut a)
        .await
        .expect("select")
        .get(0);
    assert_eq!(state, "pending");

    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "LOCK TABLE Queue IN ACCESS EXCLUSIVE MODE").await.expect("lock table");
    exec(&mut b, "BEGIN").await.expect("begin b");
    let refused = exec(&mut b, "LOCK TABLE queue IN SHARE MODE NOWAIT").await;
    exec(&mut b, "ROLLBACK").await.expect("rollback b");
    let e = refused.expect_err("a table lock under another spelling did not conflict");
    assert!(e.contains("could not obtain lock"), "{e}");
    exec(&mut a, "COMMIT").await.expect("commit");
    exec(&mut b, "BEGIN").await.expect("begin b");
    exec(&mut b, "LOCK TABLE queue IN SHARE MODE NOWAIT").await.expect("free once released");
    exec(&mut b, "COMMIT").await.expect("commit b");
}

fn files_under(dir: &std::path::Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            files_under(&path, out);
        } else {
            out.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
}

/// Every DDL form, written with a spelling other than the table's own, acts
/// on that table: no 42P01, no collision refusal, and no copy of the table
/// filed under the statement's spelling. A restart reads it all back (a
/// wrongly keyed catalogue entry or file only shows on reopen).
#[tokio::test(flavor = "multi_thread")]
async fn every_ddl_form_acts_on_the_table_under_any_spelling() {
    for (kind, trailer) in [("plain", ""), ("part", " PARTITION BY HASH (id) PARTITIONS 4")] {
        let root = std::env::temp_dir().join(format!("identifier_case_ddl_{kind}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let root_text = root.to_string_lossy().into_owned();
        let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
        let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
        tokio::spawn(conn);
        let create = format!("CREATE TABLE Acct (id INT PRIMARY KEY, owner TEXT, bal INT){trailer}");
        for sql in [
            create.as_str(),
            "INSERT INTO acct VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
            "ALTER TABLE acct ADD COLUMN note TEXT",
            "UPDATE ACCT SET note = 'n' WHERE id = 2",
            "ALTER TABLE ACCT RENAME COLUMN note TO memo",
            "ALTER TABLE aCcT ALTER COLUMN owner TYPE VARCHAR(20)",
            "ALTER TABLE acct ALTER COLUMN owner SET DEFAULT 'x'",
            "ALTER TABLE acct ALTER COLUMN owner SET NOT NULL",
            "ALTER TABLE ACCT ADD CONSTRAINT bal_pos CHECK (bal >= 0)",
            "CREATE INDEX acct_owner ON acct (owner)",
            "DROP INDEX acct_owner",
            "CREATE INDEX acct_bal ON ACCT (bal)",
            "COMMENT ON TABLE acct IS 'accounts'",
            "VACUUM acct",
            "ANALYZE ACCT",
            "DELETE FROM acct WHERE id = 3",
            "ALTER TABLE acct DROP COLUMN memo",
            "ALTER TABLE acct ADD COLUMN memo TEXT",
            "UPDATE acct SET memo = 'm' WHERE id = 1",
            "ALTER TABLE acct ADD CONSTRAINT id_owner_u UNIQUE (id, owner)",
            "ALTER TABLE ACCT ALTER COLUMN owner DROP DEFAULT",
            "ALTER TABLE acct ALTER COLUMN owner DROP NOT NULL",
            "ALTER TABLE acct ALTER COLUMN owner SET DEFAULT 'x'",
            "CREATE TABLE Child (id INT PRIMARY KEY, a INT)",
            "INSERT INTO child VALUES (1, 1)",
            "ALTER TABLE CHILD ADD CONSTRAINT child_a FOREIGN KEY (a) REFERENCES ACCT (id)",
            "DELETE FROM CHILD",
            "INSERT INTO cHiLd VALUES (2, 2)",
            "ALTER TABLE acct RENAME TO Ledger",
            "INSERT INTO LEDGER (id, bal) VALUES (4, 40)",
        ] {
            c.simple_query(sql).await.unwrap_or_else(|e| panic!("{kind}: {sql}: {e}"));
        }
        assert_eq!(
            code(&c, "INSERT INTO ledger (id, bal) VALUES (5, -1)").await,
            "23514",
            "{kind}: the CHECK added under another spelling"
        );
        assert_eq!(code(&c, "SELECT * FROM acct").await, "42P01", "{kind}: the old name after RENAME TO");
        assert_eq!(code(&c, "INSERT INTO CHILD VALUES (3, 99)").await, "23503", "{kind}: the FK added under other spellings");
        assert_eq!(code(&c, "DELETE FROM ledger WHERE id = 2").await, "23503", "{kind}: the FK after RENAME TO");
        let want = ["1|a|10|m", "2|b|20|NULL", "4|x|40|NULL"];
        let read = "SELECT id, owner, bal, memo FROM ledger ORDER BY id";
        assert_eq!(rows(&c, read).await, want, "{kind}");
        drop(c);
        drop(server);
        server = TestServer::start_file_backed(&root_text).await.expect("restart");
        let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("reconnect");
        tokio::spawn(conn);
        assert_eq!(rows(&c, read).await, want, "{kind}: after a restart");
        assert_eq!(rows(&c, "SELECT count(*) FROM LEDGER WHERE bal = 20").await, ["1"], "{kind}: the index");
        assert_eq!(code(&c, "SELECT * FROM acct").await, "42P01", "{kind}: after a restart");
        assert_eq!(rows(&c, "SELECT id, a FROM Child").await, ["2|2"], "{kind}: DELETE under another spelling");
        assert_eq!(code(&c, "INSERT INTO child VALUES (3, 99)").await, "23503", "{kind}: the FK after a restart");
        c.simple_query("DROP TABLE CHILD").await.unwrap_or_else(|e| panic!("{kind}: drop child: {e}"));
        let names: Vec<String> = rows(
            &c,
            "SELECT table_name FROM information_schema.tables WHERE lower(table_name) IN ('acct', 'ledger')",
        )
        .await;
        assert_eq!(names, ["ledger"], "{kind}: one catalogue entry, under its folded name");
        c.simple_query("DROP TABLE ledger").await.unwrap_or_else(|e| panic!("{kind}: drop: {e}"));
        assert_eq!(code(&c, "SELECT * FROM Ledger").await, "42P01", "{kind}: after DROP");
        let mut files = Vec::new();
        files_under(&root, &mut files);
        let strays: Vec<_> = files
            .iter()
            .filter(|f| {
                let f = f.to_ascii_lowercase();
                f.starts_with("acct") || f.starts_with("ledger") || f.starts_with("child")
            })
            .collect();
        assert!(strays.is_empty(), "{kind}: files left under a spelling: {strays:?}");
        drop(c);
        drop(server);
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A column named in another case is that column, in an INSERT's list as
/// everywhere else (the list was matched exactly: `INSERT INTO t (ID)` was
/// 42703). And a plain column's alias is its label under the simple query
/// protocol as under Describe: `SELECT id AS k` came back labelled `id` over
/// a simple query.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_in_another_case_is_that_column() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, label TEXT, n INT DEFAULT 7)",
        "INSERT INTO t (ID, LABEL) VALUES (1, 'a')",
        "INSERT INTO T (Id, Label) VALUES (2, 'b'), (3, 'c')",
        "INSERT INTO t (ID, Label) SELECT ID + 10, LABEL FROM t WHERE id = 1",
        "INSERT INTO t (ID, LABEL) VALUES (1, 'z') ON CONFLICT (ID) DO UPDATE SET LABEL = EXCLUDED.LABEL",
        "INSERT INTO t (iD, \"LABEL\") VALUES (4, 'd') ON CONFLICT DO NOTHING",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(&c, "SELECT id, label, n FROM t ORDER BY id").await,
        ["1|z|7", "2|b|7", "3|c|7", "4|d|7", "11|a|7"]
    );
    assert_eq!(code(&c, "INSERT INTO t (ID, nope) VALUES (5, 1)").await, "42703");

    let sql = "SELECT id AS k, label AS \"L\", n FROM t WHERE id = 2";
    let simple: Vec<String> = c
        .simple_query(sql)
        .await
        .expect("simple")
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.columns().iter().map(|c| c.name().to_string()).collect()),
            _ => None,
        })
        .expect("a row");
    let described: Vec<String> =
        c.prepare(sql).await.expect("prepare").columns().iter().map(|c| c.name().to_string()).collect();
    assert_eq!(simple, ["k", "L", "n"], "simple query labels");
    assert_eq!(described, simple, "Describe and the rows disagree");
}

async fn labels(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let simple: Vec<String> = c
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.columns().iter().map(|c| c.name().to_string()).collect()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no row"));
    let described: Vec<String> =
        c.prepare(sql).await.expect("prepare").columns().iter().map(|c| c.name().to_string()).collect();
    assert_eq!(described, simple, "{sql}: Describe and the rows disagree");
    simple
}

/// Unquoted names fold to lowercase, as PostgreSQL's lexer folds them: what
/// a table and its columns are declared as, the labels a query's columns go
/// out under, an index's name, a CTE's. A client written against PostgreSQL
/// reads `row["name"]` after `CREATE TABLE t (Name text)`; it found `Name`.
/// Quoted names keep their case. Names stored in another case before the
/// fold (here made with quotes) still resolve unquoted: more lenient than
/// PostgreSQL, so existing databases keep working.
#[tokio::test(flavor = "multi_thread")]
async fn unquoted_names_fold_to_lowercase() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE Acct (Id INT PRIMARY KEY, Name TEXT NOT NULL, \"Quoted\" INT, Bal INT DEFAULT 0 CHECK (Bal >= 0))",
        "INSERT INTO ACCT (ID, NAME, \"Quoted\") VALUES (1, 'Ann', 7), (2, 'Bob', 8)",
        "CREATE INDEX Acct_Name ON Acct (Name)",
        "ALTER TABLE Acct ADD COLUMN Extra TEXT",
        "ALTER TABLE ACCT RENAME COLUMN Extra TO Memo",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(&c, "SELECT table_name FROM information_schema.tables WHERE table_name ILIKE 'acct'").await,
        ["acct"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'acct' ORDER BY ordinal_position"
        )
        .await,
        ["id", "name", "Quoted", "bal", "memo"]
    );
    let cases: &[(&str, &[&str])] = &[
        ("SELECT * FROM Acct WHERE Id = 1", &["id", "name", "Quoted", "bal", "memo"]),
        ("SELECT ID, Name FROM acct WHERE id = 1", &["id", "name"]),
        ("SELECT ID AS Ident, Name AS \"Who\" FROM acct WHERE id = 1", &["ident", "Who"]),
        ("SELECT count(*) AS Total, MAX(Bal) AS Top FROM acct", &["total", "top"]),
        ("SELECT UPPER(Name) FROM acct WHERE id = 1", &["upper"]),
        ("SELECT a.Name AS N FROM acct a JOIN Acct b ON a.ID = B.id WHERE a.id = 1", &["n"]),
        ("SELECT Ident FROM (SELECT ID AS Ident FROM acct) S WHERE IDENT = 1", &["ident"]),
        ("WITH Totals AS (SELECT count(*) AS N FROM acct) SELECT N FROM TOTALS", &["n"]),
        ("SELECT Name, count(*) AS C FROM acct GROUP BY Name ORDER BY NAME LIMIT 1", &["name", "c"]),
    ];
    for (sql, want) in cases {
        assert_eq!(labels(&c, sql).await, *want, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT Ident FROM (SELECT ID AS Ident FROM acct) S WHERE IDENT = 2").await, ["2"]);
    assert_eq!(rows(&c, "SELECT N FROM (SELECT Name AS N FROM acct) s ORDER BY n DESC").await, ["Bob", "Ann"]);
    assert_eq!(code(&c, "INSERT INTO acct (id, name, bal) VALUES (3, 'C', -1)").await, "23514");
    c.simple_query("DROP INDEX ACCT_NAME").await.expect("the index under another spelling");
    assert_eq!(code(&c, "CREATE TABLE ACCT (id INT)").await, "42P07");

    // Stored in another case (here quoted, as an older database stored an
    // unquoted name): reached unquoted, for reads and for DDL.
    for sql in [
        "CREATE TABLE \"Legacy\" (\"Id\" INT PRIMARY KEY, \"Col\" TEXT)",
        "INSERT INTO Legacy (Id, Col) VALUES (1, 'x')",
        "ALTER TABLE Legacy ADD COLUMN Extra INT",
        "UPDATE legacy SET Extra = 5 WHERE ID = 1",
        "CREATE INDEX Legacy_Col ON Legacy (Col)",
        "ALTER TABLE Legacy RENAME COLUMN Col TO Kol",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(rows(&c, "SELECT Id, Kol, Extra FROM LEGACY").await, ["1|x|5"]);
    // A label is the stored name: `Id` kept its quoted case, `Kol` was
    // folded when the rename declared it.
    assert_eq!(labels(&c, "SELECT ID, Kol FROM legacy").await, ["Id", "kol"]);
    c.simple_query("ALTER TABLE legacy DROP COLUMN extra").await.expect("drop column");
    c.simple_query("DROP TABLE Legacy").await.expect("drop");
}
