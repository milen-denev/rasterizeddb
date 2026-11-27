//! The role system's R2/R3 surface, driven as a session runs it
//! (`execute_recorded`): a superuser sets things up, then the session acts
//! as an ordinary role. Each test checks a refusal and the grant that lifts
//! it, so a check that refused everything, or nothing, fails one half.

use super::owner_tests::{Session, ok, send, session};
use super::*;

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

/// Acts as `role` (not a superuser) from now on; `None` is the superuser.
fn act_as(s: &mut Session, role: Option<&str>) {
    s.state.session_user = None;
    s.state.principal = match role {
        Some(name) => Principal { name: name.to_string(), superuser: false, authenticated: true },
        None => Principal { name: "rasterized".to_string(), superuser: true, authenticated: false },
    };
}

/// The error of `sql`, which must fail.
async fn refused(db: &Arc<Database>, s: &mut Session, sql: &str) -> String {
    match send(db, s, sql).await {
        Ok(rows) => panic!("{sql}: expected a refusal, got {rows} rows"),
        Err(e) => e,
    }
}

/// The text of the first cell of the first row `sql` returns.
async fn first_cell(db: &Arc<Database>, s: &mut Session, sql: &str) -> Option<String> {
    use tokio::io::AsyncReadExt;
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None)).await.unwrap();
    let mut cell = None;
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' if cell.is_none() => {
                let n = i32::from_be_bytes(body[2..6].try_into().unwrap());
                cell = Some((n >= 0).then(|| String::from_utf8_lossy(&body[6..6 + n as usize]).to_string()));
            }
            b'C' => return cell.flatten(),
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

async fn roles(db: &Arc<Database>, s: &mut Session, names: &[&str]) {
    for n in names {
        ok(db, s, &format!("CREATE ROLE {n} LOGIN PASSWORD 'pw'")).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_reassign_owned_moves_ownership_and_drop_owned_needs_it_first() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["alice", "bob"]).await;
    ok(&db, &mut s, "GRANT CREATE ON SCHEMA public TO alice").await;
    act_as(&mut s, Some("alice"));
    ok(&db, &mut s, "CREATE TABLE ta (id INT PRIMARY KEY)").await;
    act_as(&mut s, None);
    let e = refused(&db, &mut s, "DROP OWNED BY alice").await;
    assert!(e.contains("still own"), "{e}");
    let e = refused(&db, &mut s, "DROP ROLE alice").await;
    assert!(e.contains("objects depend on it"), "{e}");
    ok(&db, &mut s, "REASSIGN OWNED BY alice TO bob").await;
    ok(&db, &mut s, "DROP OWNED BY alice").await;
    ok(&db, &mut s, "DROP ROLE alice").await;
    // bob owns it now: bob may drop it.
    act_as(&mut s, Some("bob"));
    ok(&db, &mut s, "DROP TABLE ta").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_drop_role_refuses_while_it_owns_a_function() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["fowner"]).await;
    act_as(&mut s, Some("fowner"));
    ok(&db, &mut s, "CREATE FUNCTION one() RETURNS int LANGUAGE sql AS 'SELECT 1'").await;
    act_as(&mut s, None);
    let e = refused(&db, &mut s, "DROP ROLE fowner").await;
    assert!(e.contains("function one"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_revoking_admin_option_keeps_the_membership() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["grp", "member1"]).await;
    ok(&db, &mut s, "GRANT grp TO member1 WITH ADMIN OPTION").await;
    ok(&db, &mut s, "REVOKE ADMIN OPTION FOR grp FROM member1").await;
    act_as(&mut s, Some("member1"));
    assert_eq!(first_cell(&db, &mut s, "SELECT pg_has_role('grp', 'MEMBER')").await.as_deref(), Some("t"));
    // The ADMIN option itself is gone: member1 may no longer grant grp.
    roles_as_superuser(&db, &mut s, &["other"]).await;
    let e = refused(&db, &mut s, "GRANT grp TO other").await;
    assert!(e.contains("ADMIN OPTION"), "{e}");
}

async fn roles_as_superuser(db: &Arc<Database>, s: &mut Session, names: &[&str]) {
    let was = s.state.principal.clone();
    act_as(s, None);
    roles(db, s, names).await;
    s.state.principal = was;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_revoke_with_dependents_needs_cascade() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["u1", "u2"]).await;
    ok(&db, &mut s, "CREATE TABLE gt (id INT PRIMARY KEY)").await;
    ok(&db, &mut s, "GRANT SELECT ON gt TO u1 WITH GRANT OPTION").await;
    act_as(&mut s, Some("u1"));
    ok(&db, &mut s, "GRANT SELECT ON gt TO u2").await;
    act_as(&mut s, None);
    let e = refused(&db, &mut s, "REVOKE SELECT ON gt FROM u1").await;
    assert!(e.contains("dependent privileges"), "{e}");
    ok(&db, &mut s, "REVOKE SELECT ON gt FROM u1 CASCADE").await;
    act_as(&mut s, Some("u2"));
    refused(&db, &mut s, "SELECT id FROM gt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_column_privileges_allow_only_the_granted_columns() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["cu"]).await;
    ok(&db, &mut s, "CREATE TABLE ct (id INT PRIMARY KEY, pub_col TEXT, secret TEXT)").await;
    ok(&db, &mut s, "INSERT INTO ct (id, pub_col, secret) VALUES (1, 'p', 's')").await;
    act_as(&mut s, Some("cu"));
    refused(&db, &mut s, "SELECT pub_col FROM ct").await;
    act_as(&mut s, None);
    ok(&db, &mut s, "GRANT SELECT (id, pub_col) ON ct TO cu").await;
    act_as(&mut s, Some("cu"));
    assert_eq!(ok(&db, &mut s, "SELECT pub_col FROM ct WHERE id = 1").await, 1);
    refused(&db, &mut s, "SELECT secret FROM ct").await;
    refused(&db, &mut s, "SELECT * FROM ct").await;
    refused(&db, &mut s, "SELECT DISTINCT * FROM ct").await;
    // count(*) reads no column.
    assert_eq!(ok(&db, &mut s, "SELECT count(*) FROM ct").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_sequence_calls_need_their_privileges() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["su1"]).await;
    ok(&db, &mut s, "CREATE SEQUENCE sq").await;
    act_as(&mut s, Some("su1"));
    let e = refused(&db, &mut s, "SELECT nextval('sq')").await;
    assert!(e.contains("permission denied for sequence sq"), "{e}");
    act_as(&mut s, None);
    ok(&db, &mut s, "GRANT USAGE ON SEQUENCE sq TO su1").await;
    act_as(&mut s, Some("su1"));
    assert_eq!(ok(&db, &mut s, "SELECT nextval('sq')").await, 1);
    // setval needs UPDATE, which USAGE is not.
    refused(&db, &mut s, "SELECT setval('sq', 10)").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_execute_revoked_from_public_and_granted_back() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["eu"]).await;
    ok(&db, &mut s, "CREATE FUNCTION two() RETURNS int LANGUAGE sql AS 'SELECT 2'").await;
    act_as(&mut s, Some("eu"));
    assert_eq!(first_cell(&db, &mut s, "SELECT two()").await.as_deref(), Some("2"));
    act_as(&mut s, None);
    ok(&db, &mut s, "REVOKE EXECUTE ON FUNCTION two() FROM PUBLIC").await;
    act_as(&mut s, Some("eu"));
    let e = refused(&db, &mut s, "SELECT two()").await;
    assert!(e.contains("permission denied for function two"), "{e}");
    act_as(&mut s, None);
    ok(&db, &mut s, "GRANT EXECUTE ON FUNCTION two TO eu").await;
    act_as(&mut s, Some("eu"));
    assert_eq!(first_cell(&db, &mut s, "SELECT two()").await.as_deref(), Some("2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_security_definer_reads_with_its_owners_rights() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["downer", "caller"]).await;
    ok(&db, &mut s, "CREATE TABLE vault (id INT PRIMARY KEY, v INT)").await;
    ok(&db, &mut s, "INSERT INTO vault (id, v) VALUES (1, 42)").await;
    ok(&db, &mut s, "GRANT SELECT ON vault TO downer").await;
    act_as(&mut s, Some("downer"));
    ok(&db, &mut s, "CREATE FUNCTION peek() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT v FROM vault WHERE id = 1'").await;
    ok(&db, &mut s, "CREATE FUNCTION peek_invoker() RETURNS int LANGUAGE sql AS 'SELECT v FROM vault WHERE id = 1'").await;
    act_as(&mut s, Some("caller"));
    refused(&db, &mut s, "SELECT v FROM vault").await;
    refused(&db, &mut s, "SELECT peek_invoker()").await;
    assert_eq!(first_cell(&db, &mut s, "SELECT peek()").await.as_deref(), Some("42"));
    // Its owner losing the right closes it too.
    act_as(&mut s, None);
    ok(&db, &mut s, "REVOKE SELECT ON vault FROM downer").await;
    act_as(&mut s, Some("caller"));
    refused(&db, &mut s, "SELECT peek()").await;
}

async fn rls_table(db: &Arc<Database>, s: &mut Session) {
    roles(db, s, &["ra", "rb"]).await;
    ok(db, s, "CREATE TABLE docs (id INT PRIMARY KEY, owner_name TEXT, body TEXT)").await;
    ok(db, s, "INSERT INTO docs (id, owner_name, body) VALUES (1, 'ra', 'x')").await;
    ok(db, s, "INSERT INTO docs (id, owner_name, body) VALUES (2, 'rb', 'y')").await;
    ok(db, s, "INSERT INTO docs (id, owner_name, body) VALUES (3, 'ra', 'z')").await;
    ok(db, s, "GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO ra, rb").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_rls_shows_a_role_its_rows_only() {
    let db = db().await;
    let mut s = session(&db);
    rls_table(&db, &mut s).await;
    act_as(&mut s, Some("ra"));
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs").await, 3, "before RLS, every row");
    act_as(&mut s, None);
    ok(&db, &mut s, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    act_as(&mut s, Some("ra"));
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs").await, 0, "RLS on and no policy: nothing");
    act_as(&mut s, None);
    ok(&db, &mut s, "CREATE POLICY own ON docs USING (owner_name = current_user)").await;
    act_as(&mut s, Some("ra"));
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs").await, 2);
    assert_eq!(ok(&db, &mut s, "SELECT d.id FROM docs d WHERE d.id > 0").await, 2, "aliased");
    act_as(&mut s, Some("rb"));
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs").await, 1);
    // The owner (the superuser here) is exempt.
    act_as(&mut s, None);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs").await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_rls_checks_new_rows_and_limits_updates() {
    let db = db().await;
    let mut s = session(&db);
    rls_table(&db, &mut s).await;
    ok(&db, &mut s, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    ok(&db, &mut s, "CREATE POLICY own ON docs USING (owner_name = current_user)").await;
    act_as(&mut s, Some("rb"));
    let e = refused(&db, &mut s, "INSERT INTO docs (id, owner_name, body) VALUES (4, 'ra', 'forged')").await;
    assert!(e.contains("violates row-level security"), "{e}");
    ok(&db, &mut s, "INSERT INTO docs (id, owner_name, body) VALUES (5, 'rb', 'mine')").await;
    // rb's UPDATE of every row changes only rb's.
    ok(&db, &mut s, "UPDATE docs SET body = 'edited'").await;
    act_as(&mut s, None);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs WHERE body = 'edited'").await, 2);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM docs WHERE id = 4").await, 0, "the forged row was not kept");
    // An UPDATE may not move a row out of the policy.
    act_as(&mut s, Some("rb"));
    let e = refused(&db, &mut s, "UPDATE docs SET owner_name = 'ra' WHERE id = 2").await;
    assert!(e.contains("violates row-level security"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_privilege_functions_answer_from_the_acl() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["pf"]).await;
    ok(&db, &mut s, "CREATE TABLE pt (id INT PRIMARY KEY)").await;
    act_as(&mut s, Some("pf"));
    assert_eq!(first_cell(&db, &mut s, "SELECT has_table_privilege('pt', 'SELECT')").await.as_deref(), Some("f"));
    act_as(&mut s, None);
    ok(&db, &mut s, "GRANT SELECT ON pt TO pf").await;
    act_as(&mut s, Some("pf"));
    assert_eq!(first_cell(&db, &mut s, "SELECT has_table_privilege('pt', 'SELECT')").await.as_deref(), Some("t"));
    assert_eq!(first_cell(&db, &mut s, "SELECT has_table_privilege('pt', 'INSERT')").await.as_deref(), Some("f"));
    assert_eq!(first_cell(&db, &mut s, "SELECT has_table_privilege('pf', 'pt', 'SELECT, INSERT')").await.as_deref(), Some("t"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_createrole_manages_ordinary_roles_only() {
    let db = db().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE ROLE manager LOGIN CREATEROLE PASSWORD 'pw'").await;
    ok(&db, &mut s, "CREATE ROLE boss LOGIN SUPERUSER PASSWORD 'pw'").await;
    act_as(&mut s, Some("manager"));
    ok(&db, &mut s, "CREATE ROLE worker LOGIN PASSWORD 'pw'").await;
    ok(&db, &mut s, "ALTER ROLE worker CONNECTION LIMIT 3").await;
    refused(&db, &mut s, "CREATE ROLE escalate SUPERUSER").await;
    refused(&db, &mut s, "ALTER ROLE boss PASSWORD 'x'").await;
    refused(&db, &mut s, "ALTER ROLE worker BYPASSRLS").await;
    act_as(&mut s, Some("worker"));
    // Its own password, but not another's.
    ok(&db, &mut s, "ALTER ROLE worker PASSWORD 'new'").await;
    refused(&db, &mut s, "ALTER ROLE manager PASSWORD 'x'").await;
}

/// The rdb_roles escalation (reported on 1.0.1): a login role must not
/// write the role catalogue nor read a verifier, by any spelling or path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2r3_rdb_roles_is_closed_to_every_path() {
    let db = db().await;
    let mut s = session(&db);
    roles(&db, &mut s, &["mallory", "victim"]).await;
    ok(&db, &mut s, "CREATE TABLE notes (id INT PRIMARY KEY)").await;
    ok(&db, &mut s, "GRANT SELECT, INSERT, UPDATE ON notes TO mallory").await;
    act_as(&mut s, Some("mallory"));
    for sql in [
        "UPDATE rdb_roles SET is_superuser = true WHERE name = 'mallory'",
        "UPDATE \"rdb_roles\" SET is_superuser = 1",
        "UPDATE public.rdb_roles SET is_superuser = 1",
        "DELETE FROM rdb_roles WHERE name = 'victim'",
        "INSERT INTO rdb_roles (name, password, can_login, is_superuser) VALUES ('evil', NULL, 1, 1)",
        "SELECT password FROM rdb_roles",
        "SELECT * FROM rdb_roles",
        "SELECT * FROM \"rdb_roles\"",
        "SELECT * FROM public.rdb_roles",
        "SELECT * FROM (SELECT password FROM rdb_roles) AS r",
        "WITH r AS (SELECT * FROM rdb_roles) SELECT * FROM r",
        "SELECT id FROM notes WHERE id IN (SELECT 1 FROM rdb_roles WHERE is_superuser = 1)",
        "INSERT INTO notes (id) SELECT 1 FROM rdb_roles",
        "COPY rdb_roles TO STDOUT",
        "CREATE VIEW leak AS SELECT * FROM rdb_roles",
        "PREPARE p AS SELECT password FROM rdb_roles",
        "SELECT * FROM __rdb_acl",
        "UPDATE __rdb_acl SET grantee = 'mallory'",
        "SELECT rolpassword FROM pg_authid",
    ] {
        let e = refused(&db, &mut s, sql).await;
        assert!(e.contains("42501") || e.contains("permission denied"), "{sql}: {e}");
    }
    // A prepared statement learned on a granted table, then pointed at
    // nothing new, keeps the guard: EXECUTE of a refused PREPARE has
    // nothing to run.
    refused(&db, &mut s, "EXECUTE p").await;
    act_as(&mut s, None);
    assert_eq!(first_cell(&db, &mut s, "SELECT rolsuper FROM pg_roles WHERE rolname = 'mallory'").await.as_deref(), Some("f"));
    assert_eq!(ok(&db, &mut s, "SELECT rolname FROM pg_roles WHERE rolname = 'victim'").await, 1);
    assert_eq!(ok(&db, &mut s, "SELECT rolname FROM pg_roles WHERE rolname = 'evil'").await, 0);
}

/// B4.2 on one shard: two tagged transactions, one waiting on the other's
/// row lock, are reported as a wait between their global ids; ending the
/// waiter fails its statement with 40P01.
#[cfg(feature = "sharding")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b42_tagged_waits_are_reported_and_a_victim_ends_with_40p01() {
    let db = db().await;
    let mut a = session(&db);
    ok(&db, &mut a, "CREATE TABLE dl (id INT PRIMARY KEY, v INT)").await;
    ok(&db, &mut a, "INSERT INTO dl (id, v) VALUES (1, 0)").await;
    ok(&db, &mut a, "BEGIN").await;
    ok(&db, &mut a, "SET LOCAL rdb.global_txn = 'gw-1-1'").await;
    ok(&db, &mut a, "UPDATE dl SET v = 1 WHERE id = 1").await;

    let db2 = db.clone();
    let waiter = tokio::spawn(async move {
        // As a connection runs: inside its registered session, which is
        // what an abort stops.
        let registration = crate::core::cancel::register_session([7, 7, 7, 7]);
        SESSION_CANCEL
            .scope(registration.session.clone(), async move {
                let mut b = session(&db2);
                ok(&db2, &mut b, "BEGIN").await;
                ok(&db2, &mut b, "SET LOCAL rdb.global_txn = 'gw-1-2'").await;
                // Blocks on a's row lock until ended.
                send(&db2, &mut b, "UPDATE dl SET v = 2 WHERE id = 1").await
            })
            .await
    });

    let catalog = crate::core::database::catalog::DatabaseCatalog::single(db.clone());
    let mut edges = Vec::new();
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        edges = crate::core::shard::deadlock::waits(&catalog);
        if !edges.is_empty() {
            break;
        }
    }
    assert_eq!(edges, vec![("gw-1-2".to_string(), "gw-1-1".to_string())]);
    assert!(crate::core::shard::deadlock::abort("gw-1-2"));
    let ended = tokio::time::timeout(std::time::Duration::from_secs(10), waiter).await.expect("the waiter ended").unwrap();
    let e = ended.expect_err("the victim's statement fails");
    assert!(e.contains("40P01") || e.contains("deadlock detected"), "{e}");
    ok(&db, &mut a, "COMMIT").await;
}
