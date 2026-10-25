//! The statements of one extended-query batch (several Execute before one
//! Sync) and of one multi-statement simple query are ONE implicit
//! transaction, as in PostgreSQL: they commit together, and a failure rolls
//! back the ones before it too. PostgreSQL 17's answers throughout.
//!
//! Each statement used to commit on its own, so a batch or a string whose
//! later statement failed kept the earlier ones (npgsql batching, a
//! migration sent as one string). Found by rasterizeddb-eb's B8 replay of
//! npgsql's batches.

#[path = "support/wire_frames.rs"]
mod wire;

use rasterizeddb_compat::TestServer;
use wire::{Frame, Reply, Wire};

/// One unnamed statement with no parameters, text results.
fn statement(sql: &str) -> Vec<Frame> {
    vec![wire::parse("", sql, &[]), wire::bind("", "", &[], &[0]), wire::describe_p(""), wire::execute("", 0)]
}

fn batch(sqls: &[&str]) -> Vec<Frame> {
    let mut frames: Vec<Frame> = sqls.iter().flat_map(|sql| statement(sql)).collect();
    frames.push(wire::sync());
    frames
}

fn ready_status(replies: &[Reply]) -> char {
    match replies.last() {
        Some(Reply::Ready(s)) => *s as char,
        other => panic!("the batch did not end with ReadyForQuery: {other:?}"),
    }
}

async fn ids(admin: &mut Wire) -> Vec<String> {
    wire::text_rows(&admin.simple_ok("SELECT id FROM ib ORDER BY id").await)
        .into_iter()
        .map(|row| row[0].clone().unwrap_or_default())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_and_a_multi_statement_string_are_one_transaction() {
    let server = TestServer::start().await.expect("server");
    let addr = format!("127.0.0.1:{}", server.port());
    let params = [("user", "postgres"), ("database", "postgres")];
    let mut admin = Wire::connect(&addr, &params).await;
    admin.simple_ok("CREATE TABLE ib (id INT PRIMARY KEY)").await;
    admin.simple_ok("INSERT INTO ib (id) VALUES (1)").await;
    let mut c = Wire::connect(&addr, &params).await;

    // A good batch commits every statement at its Sync.
    let r = c.exchange(&batch(&["INSERT INTO ib (id) VALUES (2)", "INSERT INTO ib (id) VALUES (3)"])).await;
    assert_eq!(wire::error_code(&r), None, "{}", wire::render(&r));
    assert_eq!(ready_status(&r), 'I');
    assert_eq!(ids(&mut admin).await, ["1", "2", "3"]);

    // A failure rolls back the batch's earlier statements and skips the
    // rest, and the session is idle again after the Sync.
    let r = c
        .exchange(&batch(&[
            "INSERT INTO ib (id) VALUES (4)",
            "INSERT INTO ib (id) VALUES (1)",
            "INSERT INTO ib (id) VALUES (5)",
        ]))
        .await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23505"), "{}", wire::render(&r));
    assert_eq!(ready_status(&r), 'I');
    assert_eq!(ids(&mut admin).await, ["1", "2", "3"], "a failed batch kept its earlier statements");
    // In step afterwards: the next batch runs normally.
    let r = c.exchange(&batch(&["INSERT INTO ib (id) VALUES (4)"])).await;
    assert_eq!(wire::error_code(&r), None, "{}", wire::render(&r));
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4"]);

    // A batch that opens with BEGIN is that block: still open after its
    // Sync, and its ROLLBACK undoes it.
    let r = c.exchange(&batch(&["BEGIN", "INSERT INTO ib (id) VALUES (6)"])).await;
    assert_eq!(wire::error_code(&r), None, "{}", wire::render(&r));
    assert_eq!(ready_status(&r), 'T', "the batch's BEGIN did not leave its block open");
    c.simple_ok("ROLLBACK").await;
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4"]);

    // A BEGIN after a statement makes the implicit block explicit: what ran
    // before it is the block's, and ends at its COMMIT, not at the Sync.
    let r = c
        .exchange(&batch(&["INSERT INTO ib (id) VALUES (7)", "BEGIN", "INSERT INTO ib (id) VALUES (8)"]))
        .await;
    assert_eq!(wire::error_code(&r), None, "{}", wire::render(&r));
    assert_eq!(ready_status(&r), 'T', "BEGIN inside a batch did not make its block explicit");
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4"], "the block's rows were visible before its COMMIT");
    c.simple_ok("COMMIT").await;
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4", "7", "8"]);

    // A lone statement and its Sync: committed as it always was.
    let r = c.exchange(&batch(&["INSERT INTO ib (id) VALUES (9)"])).await;
    assert_eq!(ready_status(&r), 'I');
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4", "7", "8", "9"]);

    // A batch the client abandons before its Sync is rolled back: nothing
    // it wrote is ever visible, and its keys are free once it has gone (a
    // transaction left open would hold them in flight).
    let mut gone = Wire::connect(&addr, &params).await;
    let mut frames: Vec<Frame> = statement("INSERT INTO ib (id) VALUES (10)");
    frames.extend(statement("INSERT INTO ib (id) VALUES (11)"));
    gone.write(&frames).await;
    let _ = gone.next_within(std::time::Duration::from_millis(300)).await;
    drop(gone);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4", "7", "8", "9"], "an abandoned batch committed");
    admin.simple_ok("INSERT INTO ib (id) VALUES (10)").await;
    admin.simple_ok("DELETE FROM ib WHERE id = 10").await;

    // A multi-statement simple query: a failure rolls back the statements
    // before it and skips the rest.
    let r = c
        .simple("INSERT INTO ib (id) VALUES (12); INSERT INTO ib (id) VALUES (1); INSERT INTO ib (id) VALUES (13)")
        .await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23505"), "{}", wire::render(&r));
    assert_eq!(ready_status(&r), 'I');
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4", "7", "8", "9"], "a failed string kept its earlier statements");
    // And a good one commits all of them.
    c.simple_ok("INSERT INTO ib (id) VALUES (12); INSERT INTO ib (id) VALUES (13)").await;
    assert_eq!(ids(&mut admin).await, ["1", "2", "3", "4", "7", "8", "9", "12", "13"]);
    // A string with its own transaction control keeps its own boundaries:
    // the explicit COMMIT keeps the first INSERT whatever follows.
    let r = c
        .simple("BEGIN; INSERT INTO ib (id) VALUES (14); COMMIT; INSERT INTO ib (id) VALUES (1)")
        .await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23505"), "{}", wire::render(&r));
    assert!(ids(&mut admin).await.iter().any(|id| id == "14"), "an explicitly committed statement was rolled back");
}
