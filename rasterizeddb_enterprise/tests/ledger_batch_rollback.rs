//! A failed request of several statements takes all of them with it, its
//! vault and ledger DDL included, and says so; a vault whose key provider
//! does not open is never made; a key whose row rolled back is kept, never
//! minted again over the columns that use it. One test function: the
//! vault's settings are read from the environment once per process.

use rasterizeddb_embedded::{Builder, Client};

async fn col(c: &Client, sql: &str) -> Vec<Option<String>> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<Option<String>>(0).unwrap()).collect()
}

async fn run(c: &Client, sql: &str) {
    c.execute(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(c: &Client, sql: &str) -> String {
    match c.query(sql, &[]).await {
        Ok(rows) => panic!("{sql} was answered ({} rows)", rows.len()),
        Err(e) => e.to_string(),
    }
}

async fn table_exists(c: &Client, name: &str) -> bool {
    !col(c, &format!("SELECT table_name FROM information_schema.tables WHERE table_name = '{name}'")).await.is_empty()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_batch_takes_its_vault_and_ledger_ddl_with_it() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key = root.path().join("kek");
    std::fs::write(&key, "33".repeat(32)).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();

    // --- a key provider that does not open: no vault is made -----------------
    let missing = root.path().join("no-such-dir").join("vault.key");
    let e = refused(&admin, &format!("CREATE VAULT broken KEY PROVIDER 'mlkem:{};create'", missing.display())).await;
    assert!(e.contains("No such file"), "{e}");
    assert!(!table_exists(&admin, "broken").await, "a vault whose key provider failed was made");
    let e = refused(&admin, "CREATE VAULT broken2 KEY PROVIDER 'pkcs11:module=/nonexistent/libnone.so;token=t;key=k'").await;
    assert!(e.contains("PIN"), "{e}");
    assert!(!table_exists(&admin, "broken2").await);

    // --- one request, failing at its last statement ---------------------------
    let setup = format!(
        "CREATE ROLE \"batch-role\" LOGIN PASSWORD 'pw'; \
         CREATE VAULT bv KEY PROVIDER 'file:{}'; \
         VAULT bv CREATE KEY 'k' TYPE 'aes256-gcm'; \
         CREATE LEDGER TABLE bl (id BIGINT NOT NULL, p TEXT ENCRYPTED WITH VAULT bv KEY 'k' NOT NULL) \
           NO DELETE UNTIL 1 HOUR AFTER INSERT NO DROP UNTIL 90 DAYS IDLE; \
         INSERT INTO bl (id, p) VALUES (1, 'secret'); \
         GRANT SELECT ON bl TO \"batch-role\"",
        key.display()
    );
    let session = admin.simple_session().await.unwrap();
    let failed = session.query(&format!("{setup}; SELECT * FROM no_such_table")).await;
    assert!(failed.is_err(), "the last statement fails");
    let notices = session.failure_notices();
    assert!(
        notices.iter().any(|n| n.contains("the 6 statements before the one that failed were rolled back with it")),
        "{notices:?}"
    );
    // Every one of them, the DDL with the rest.
    assert!(!table_exists(&admin, "bl").await, "the ledger table outlived its rollback");
    assert!(!table_exists(&admin, "bv").await, "the vault outlived its rollback");
    assert!(col(&admin, "SELECT rolname FROM pg_roles WHERE rolname = 'batch-role'").await.is_empty());
    let show = col(&admin, "SHOW LEDGER").await;
    assert!(!show.iter().flatten().any(|l| l.contains("encrypted column bl.")), "{show:?}");

    // Nothing is left in the way: the same request, whole, now runs.
    session.query(&setup).await.unwrap_or_else(|e| panic!("the request again: {e}"));
    assert!(session.failure_notices().is_empty());
    assert_eq!(col(&admin, "SELECT p FROM bl").await, vec![Some("secret".into())]);
    assert_eq!(col(&admin, "SELECT rolname FROM pg_roles WHERE rolname = 'batch-role'").await, vec![Some("batch-role".into())]);
    // A lone statement that fails rolls back nothing else: no note.
    assert!(session.query("SELECT * FROM no_such_table").await.is_err());
    assert!(session.failure_notices().is_empty(), "{:?}", session.failure_notices());

    // --- a key whose row rolled back is kept ----------------------------------
    // The keyring (a file) keeps the material the rolled-back statement made;
    // a column declared since wraps its data key with it.
    assert!(session.query("VAULT bv CREATE KEY 'k2'; SELECT * FROM no_such_table").await.is_err());
    run(&admin, "CREATE TABLE kc (id BIGINT, s TEXT ENCRYPTED WITH VAULT bv KEY 'k2')").await;
    run(&admin, "INSERT INTO kc (id, s) VALUES (1, 'kept')").await;
    // Of another kind than the keyring holds: refused, not replaced.
    let e = refused(&admin, "VAULT bv CREATE KEY 'k2' TYPE 'hsm-aes256-gcm'").await;
    assert!(e.ends_with("(SQLSTATE 42710)"), "{e}");
    // A code with a letter in it reaches the client as itself, not XX000.
    let e = refused(&admin, "SELECT id FROM kc WHERE s = 'kept'").await;
    assert!(e.ends_with("(SQLSTATE 0A000)"), "{e}");
    let made = col(&admin, "VAULT bv CREATE KEY 'k2'").await;
    assert!(made.iter().flatten().any(|l| l.contains("kept")), "{made:?}");
    assert_eq!(col(&admin, "SELECT s FROM kc").await, vec![Some("kept".into())], "the column's key was replaced");
    // Its row is back: a second CREATE finds it.
    let e = refused(&admin, "VAULT bv CREATE KEY 'k2'").await;
    assert!(e.contains("exists"), "{e}");

    // --- a vault's changes are audited at their commit -------------------------
    // A SET a ROLLBACK undid is not in the audit; a committed one is, chained.
    session.query("BEGIN; VAULT bv SET SECRET 'undone' = 'x'; ROLLBACK").await.unwrap();
    session.query("BEGIN; VAULT bv SET SECRET 'kept' = 'y'; COMMIT").await.unwrap();
    let audit = col(&admin, "VAULT bv AUDIT").await;
    let audit: Vec<String> = audit.into_iter().flatten().collect();
    assert!(!audit.iter().any(|l| l.contains("'undone'")), "a rolled-back SET was audited: {audit:?}");
    assert!(audit.iter().any(|l| l.starts_with("chained") && l.contains("SET bv 'kept' v1")), "{audit:?}");
    // A read is audited whatever becomes of its transaction: it disclosed.
    session.query("BEGIN; VAULT bv GET SECRET 'kept'; ROLLBACK").await.unwrap();
    let audit: Vec<String> = col(&admin, "VAULT bv AUDIT").await.into_iter().flatten().collect();
    assert!(audit.iter().any(|l| l.starts_with("chained") && l.contains("GET bv 'kept' v1")), "{audit:?}");
    // Nothing is left unchained.
    assert!(!audit.iter().any(|l| l.starts_with("UNCHAINED")), "{audit:?}");

    // PURGE and ROTATE KEY change keys at once, past any ROLLBACK: refused in
    // a block, and a batch holding one runs statement by statement.
    run(&admin, "VAULT bv DELETE SECRET 'kept'").await;
    for sql in ["VAULT bv PURGE SECRET 'kept'", "VAULT bv ROTATE KEY"] {
        let e = session.query(&format!("BEGIN; {sql}")).await.expect_err(sql).to_string();
        assert!(e.ends_with("(SQLSTATE 25001)"), "{sql}: {e}");
        session.query("ROLLBACK").await.unwrap();
    }
    // Statement by statement: the RECOVER commits on its own, and the PURGE
    // then finds the secret active (55000), not a block (25001).
    let e = session.query("VAULT bv RECOVER SECRET 'kept'; VAULT bv PURGE SECRET 'kept'").await.expect_err("not deleted");
    assert!(e.to_string().ends_with("(SQLSTATE 55000)"), "{e}");
    assert_eq!(col(&admin, "VAULT bv GET SECRET 'kept'").await, vec![Some("y".into())], "the RECOVER did not stay");

    // --- CREATE VAULT IF NOT EXISTS over one that is there ------------------
    // Changes nothing and opens no provider: no key file is made.
    let unused = root.path().join("unused.key");
    run(&admin, &format!("CREATE VAULT IF NOT EXISTS bv KEY PROVIDER 'mlkem:{};create'", unused.display())).await;
    assert!(!unused.exists(), "IF NOT EXISTS over an existing vault made a key file");

    // --- a vault with no NO DROP clause drops ---------------------------------
    run(&admin, &format!("CREATE VAULT dv KEY PROVIDER 'file:{}'", key.display())).await;
    run(&admin, "DROP TABLE dv").await;
    assert!(!table_exists(&admin, "dv").await);

    // --- a table dropped and made again in a block that rolls back ------------
    // The dropped one is set aside under a hidden name and comes back with
    // its rows; its encrypted columns' keys must come back with it, whether
    // the new table declares encrypted columns or none.
    for again in [
        "CREATE TABLE kc (id BIGINT, s TEXT)",
        "CREATE TABLE kc (id BIGINT, s TEXT ENCRYPTED WITH VAULT bv KEY 'k2')",
    ] {
        session
            .query(&format!("BEGIN; DROP TABLE kc; {again}; ROLLBACK"))
            .await
            .unwrap_or_else(|e| panic!("{again}: {e}"));
        assert_eq!(col(&admin, "SELECT s FROM kc").await, vec![Some("kept".into())], "after {again}");
    }
    // And a committed one is the new table: plain, so filtered on freely.
    session.query("BEGIN; DROP TABLE kc; CREATE TABLE kc (id BIGINT, s TEXT); COMMIT").await.unwrap();
    run(&admin, "INSERT INTO kc (id, s) VALUES (2, 'plain')").await;
    assert_eq!(col(&admin, "SELECT s FROM kc WHERE s = 'plain'").await, vec![Some("plain".into())]);

    drop(session);
    drop(admin);
    db.shutdown().await.unwrap();
}
