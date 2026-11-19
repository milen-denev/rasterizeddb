//! Enum types, `CREATE TYPE … AS ENUM`, as an ORM's migration writes them
//! (a type and a table using it in one block, a quoted name, an array of
//! it) and reads them (casts, `ORDER BY`, `pg_type` / `pg_enum`). They were
//! refused ("CREATE TYPE is not supported"). The answers are PostgreSQL
//! 17's (`.claude/reviews/pgdiff`); where they differ it is said below.

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

async fn error(c: &tokio_postgres::Client, sql: &str) -> (String, String) {
    match c.simple_query(sql).await {
        Ok(_) => panic!("{sql}: succeeded"),
        Err(e) => {
            let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
            (db.code().code().to_string(), db.message().to_string())
        }
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
async fn an_enum_type_lives_its_life() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "BEGIN; CREATE TYPE \"Status\" AS ENUM ('NEW', 'DONE');
         CREATE TABLE tk (id int, st \"Status\" NOT NULL DEFAULT 'NEW', tags \"Status\"[]); COMMIT",
    )
    .await
    .expect("create in one block");
    c.batch_execute("INSERT INTO tk (id, tags) VALUES (1, '{DONE,NULL}'); INSERT INTO tk VALUES (2, 'DONE', NULL), (3, 'NEW', '{NEW}')")
        .await
        .expect("insert");

    // A value no label is 22P02, by a cast or in a column (PostgreSQL's
    // message also names the value, which the column's check does not
    // carry here).
    assert_eq!(error(&c, "SELECT 'BAD'::\"Status\"").await, ("22P02".into(), "invalid input value for enum \"Status\": \"BAD\"".into()));
    assert_eq!(error(&c, "INSERT INTO tk VALUES (4, 'BAD')").await.0, "22P02");
    assert_eq!(error(&c, "INSERT INTO tk (id, tags) VALUES (4, '{BAD}')").await.0, "22P02");
    assert_eq!(rows(&c, "SELECT id, st FROM tk WHERE st = 'DONE'::\"Status\" ORDER BY id").await, ["2|DONE"]);

    // Sorted by the labels' order, which ADD VALUE … BEFORE places.
    c.batch_execute("ALTER TYPE \"Status\" ADD VALUE 'LATE' BEFORE 'DONE'").await.expect("add value");
    c.batch_execute("INSERT INTO tk VALUES (5, 'LATE', '{LATE}')").await.expect("new label");
    assert_eq!(
        rows(&c, "SELECT id, st, tags FROM tk ORDER BY st, id").await,
        ["1|NEW|{DONE,NULL}", "3|NEW|{NEW}", "5|LATE|{LATE}", "2|DONE|NULL"]
    );
    assert_eq!(rows(&c, "SELECT id FROM tk ORDER BY st DESC, id").await, ["2", "5", "1", "3"]);

    // RENAME VALUE changes what the rows hold.
    c.batch_execute("ALTER TYPE \"Status\" RENAME VALUE 'NEW' TO 'OPEN'").await.expect("rename value");
    assert_eq!(
        rows(&c, "SELECT id, st, tags FROM tk ORDER BY id").await,
        ["1|OPEN|{DONE,NULL}", "2|DONE|NULL", "3|OPEN|{OPEN}", "5|LATE|{LATE}"]
    );

    // What an ORM's introspection reads.
    assert_eq!(
        rows(
            &c,
            "SELECT t.typname, t.typtype, e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid \
             WHERE t.typname = 'Status' ORDER BY e.enumsortorder"
        )
        .await,
        ["Status|e|OPEN", "Status|e|LATE", "Status|e|DONE"]
    );

    assert_eq!(error(&c, "ALTER TYPE \"Status\" ADD VALUE 'X' AFTER 'NOPE'").await.0, "22023");
    assert_eq!(error(&c, "ALTER TYPE \"Status\" ADD VALUE 'DONE'").await.0, "42710");
    assert_eq!(error(&c, "CREATE TYPE \"Status\" AS ENUM ('x')").await.0, "42710");
    assert_eq!(error(&c, "DROP TYPE \"Status\"").await.0, "2BP01");
    c.batch_execute("DROP TABLE tk; DROP TYPE \"Status\"").await.expect("drop");
    assert_eq!(error(&c, "DROP TYPE \"Status\";").await.0, "42704");
    c.batch_execute("DROP TYPE IF EXISTS \"Status\"").await.expect("if exists");
}

/// `ALTER TYPE` outside a block is one statement: when one column's
/// constraint cannot be replaced, nothing changed, the label list included.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_alter_type_changes_nothing() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TYPE ab AS ENUM ('a', 'b'); CREATE TABLE at1 (id int, s ab, t ab[]);
         ALTER TABLE at1 DROP CONSTRAINT \"__enum_ab__t\";
         ALTER TABLE at1 ADD CONSTRAINT \"__enum_ab__t\" CHECK (cardinality(array_remove(array_remove(array_remove(array_remove(\"t\", NULL), 'a'), 'b'), 'zz')) = 0);
         INSERT INTO at1 VALUES (1, 'a', '{zz}')",
    )
    .await
    .expect("setup");
    let _ = error(&c, "ALTER TYPE ab ADD VALUE 'c'").await;
    assert_eq!(
        rows(&c, "SELECT enumlabel FROM pg_enum e JOIN pg_type ty ON ty.oid = e.enumtypid WHERE ty.typname = 'ab' ORDER BY enumsortorder").await,
        ["a", "b"]
    );
    assert_eq!(error(&c, "INSERT INTO at1 VALUES (2, 'c', NULL)").await.0, "22P02");
}
