//! What two reviews of the ledger, vault and encrypted columns found, each
//! pinned: a decryption oracle over the native protocol, plaintext stored
//! through a qualified INSERT, sorts on ciphertext, panics a client could
//! cause, ledger tables converted, dropped or dropped with their database
//! inside retention, a hold lost to a rewrite, a versioned row's versions
//! chained out of order, anchors unchecked after a restart, vault states
//! and versions. One test function: the vault's settings are read from the
//! environment once per process.

use rasterizeddb_embedded::{Builder, Client, Login, Options};

async fn col(c: &Client, sql: &str) -> Vec<Option<String>> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<Option<String>>(0).unwrap()).collect()
}

async fn lines(c: &Client, sql: &str) -> Vec<String> {
    col(c, sql).await.into_iter().flatten().collect()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reviewed_holes_stay_closed() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key_a = root.path().join("kek-a");
    let key_b = root.path().join("kek-b");
    for (k, byte) in [(&key_a, "66"), (&key_b, "77")] {
        std::fs::write(k, byte.repeat(32)).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(k, <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600)).unwrap();
    }
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();

    run(&admin, &format!("CREATE VAULT hv KEY PROVIDER 'file:{}' RETAIN DELETED 0 SECONDS", key_a.display())).await;
    lines(&admin, "VAULT hv CREATE KEY 'pii'").await;
    run(&admin, "CREATE TABLE pt (id BIGINT, ssn TEXT ENCRYPTED WITH VAULT hv KEY 'pii')").await;
    run(&admin, "INSERT INTO pt (id, ssn) VALUES (1, '111-11-1111')").await;
    run(&admin, "CREATE ROLE clerk LOGIN PASSWORD 'clerkpw'").await;
    run(&admin, "GRANT SELECT, INSERT ON pt TO clerk").await;
    let clerk = db.connect_with(Options { login: Login::password("clerk", "clerkpw"), ..db.options() }).await.unwrap();

    // --- the native protocol is no decryption oracle -------------------------------
    let cipher = col(&clerk, "SELECT ssn FROM pt WHERE id = 1").await;
    let cipher = cipher[0].clone().unwrap();
    assert!(cipher.starts_with("rdbenc1:"), "a role without the vault's SELECT read {cipher}");
    // The same cell copied into a plain table, which names no encrypted
    // column: the statement takes the native protocol's direct path.
    run(&admin, "CREATE TABLE copied (c TEXT)").await;
    run(&admin, &format!("INSERT INTO copied (c) VALUES ('{cipher}')")).await;
    run(&admin, "GRANT SELECT ON copied TO clerk").await;
    assert_eq!(col(&clerk, "SELECT c FROM copied").await, vec![Some(cipher.clone())], "decrypted for a role without the grant");
    assert_eq!(col(&clerk, &format!("SELECT '{cipher}'")).await, vec![Some(cipher.clone())], "decrypted for a role without the grant");

    // --- a qualified INSERT encrypts too -----------------------------------------
    // Without the vault's grant it is refused; with it, the value is sealed.
    let e = refused(&clerk, "INSERT INTO public.pt VALUES (2, '222-22-2222')").await;
    assert!(e.contains("42501"), "{e}");
    run(&admin, "INSERT INTO public.pt VALUES (3, '333-33-3333')").await;
    let stored = col(&clerk, "SELECT ssn FROM pt WHERE id = 3").await;
    assert!(stored[0].as_deref().is_some_and(|v| v.starts_with("rdbenc1:")), "stored as plaintext: {stored:?}");

    // --- no position after the first sort or group key -----------------------------
    for sql in ["SELECT id, ssn FROM pt ORDER BY id, 2", "SELECT id, ssn FROM pt GROUP BY id, 2"] {
        let e = refused(&admin, sql).await;
        assert!(e.ends_with("(SQLSTATE 0A000)"), "{sql}: {e}");
    }
    // --- no panic a client can cause ------------------------------------------------
    let _ = clerk.query("SELECT.ssn FROM pt", &[]).await;
    let _ = admin.query("CREATE TABLE px (encrypted WITH VAULT hv KEY 'pii')", &[]).await;
    assert_eq!(col(&clerk, "SELECT 'alive'").await, vec![Some("alive".into())], "the connection did not survive");
    // A column named `encrypted` is an ordinary column.
    run(&admin, "CREATE TABLE plainenc (id INT, encrypted BOOLEAN)").await;

    // --- the secret verbs do not apply to keys --------------------------------------
    for verb in ["DISABLE", "DELETE", "PURGE", "GET"] {
        let e = refused(&admin, &format!("VAULT hv {verb} SECRET 'key:pii'")).await;
        assert!(e.contains("42602"), "{verb}: {e}");
    }

    // --- secret versions and the purged state ----------------------------------------
    let session = admin.simple_session().await.unwrap();
    session.query("BEGIN; VAULT hv SET SECRET 's' = 'one'; ROLLBACK").await.unwrap();
    let set = lines(&admin, "VAULT hv SET SECRET 's' = 'two'").await;
    assert!(set.iter().any(|l| l.contains("s version 2")), "a version a rollback used was taken again: {set:?}");
    run(&admin, "VAULT hv DELETE SECRET 's'").await;
    lines(&admin, "VAULT hv PURGE SECRET 's'").await;
    let e = refused(&admin, "VAULT hv PURGE SECRET 's'").await;
    assert!(e.contains("55000"), "a second PURGE: {e}");
    let set = lines(&admin, "VAULT hv SET SECRET 's' = 'three'").await;
    assert!(set.iter().any(|l| l.contains("s version 3")), "SET after a purge: {set:?}");
    assert_eq!(col(&admin, "VAULT hv GET SECRET 's'").await, vec![Some("three".into())]);

    // --- ROTATE KEY TO another provider, across a restart --------------------------------
    lines(&admin, &format!("VAULT hv ROTATE KEY TO 'file:{}'", key_b.display())).await;
    assert_eq!(col(&admin, "VAULT hv GET SECRET 's'").await, vec![Some("three".into())]);

    // --- ledger tables: no conversion, no drop inside retention or under a hold --------
    run(&admin, "CREATE LEDGER TABLE lt (id BIGINT NOT NULL) NO DELETE UNTIL 1 DAY AFTER INSERT").await;
    run(&admin, "INSERT INTO lt (id) VALUES (1)").await;
    let e = refused(&admin, "ALTER TABLE lt PARTITION BY HASH (id) PARTITIONS 4").await;
    assert!(e.contains("0A000"), "converted: {e}");
    let e = refused(&admin, "DROP TABLE lt").await;
    assert!(e.contains("55000") && e.contains("retention"), "dropped with a row inside its retention: {e}");
    run(&admin, "CREATE LEDGER TABLE ht (id BIGINT NOT NULL) NO DELETE UNTIL 0 SECONDS AFTER INSERT").await;
    lines(&admin, "ALTER LEDGER TABLE ht SET LEGAL HOLD 'case'").await;
    let e = refused(&admin, "DROP TABLE ht").await;
    assert!(e.contains("legal hold"), "dropped under a hold: {e}");

    // A hold survives a rewriting ALTER and a restart.
    run(&admin, "INSERT INTO ht (id) VALUES (1)").await;
    run(&admin, "ALTER TABLE ht ADD COLUMN note TEXT").await;

    // --- a versioned row's versions around a savepoint verify ------------------------
    run(&admin, "CREATE LEDGER TABLE vt (id BIGINT NOT NULL, x BIGINT) VERSIONED NO DELETE UNTIL 1 DAY AFTER INSERT").await;
    run(&admin, "INSERT INTO vt (id, x) VALUES (1, 0)").await;
    session
        .query(
            "BEGIN; UPDATE vt SET x = 1 WHERE id = 1; SAVEPOINT s; UPDATE vt SET x = 2 WHERE id = 1; RELEASE s; \
             UPDATE vt SET x = 3 WHERE id = 1; COMMIT",
        )
        .await
        .unwrap();
    let verify = lines(&admin, "VERIFY LEDGER vt").await;
    assert!(!verify.iter().any(|l| l.starts_with("TAMPERED")), "{verify:?}");

    // --- a database holding a ledger table is not dropped --------------------------------
    run(&admin, "CREATE DATABASE ldb").await;
    {
        let other = db.connect_with(Options { database: Some("ldb".into()), ..db.options() }).await.unwrap();
        run(&other, "CREATE LEDGER TABLE kept (id BIGINT NOT NULL) NO DELETE UNTIL 1 DAY AFTER INSERT").await;
        run(&other, "INSERT INTO kept (id) VALUES (1)").await;
    }
    // Through the session path, which handles database statements.
    let e = session.query("DROP DATABASE ldb").await.expect_err("DROP DATABASE").to_string();
    assert!(e.contains("retention"), "a database was dropped with ledger rows inside their retention: {e}");

    // --- anchors are checked after a restart, before any new row -----------------------
    let anchors = root.path().join("anchors");
    std::fs::create_dir_all(&anchors).unwrap();
    run(&admin, &format!("CREATE LEDGER TABLE at (id BIGINT NOT NULL) NO DELETE UNTIL 1 DAY AFTER INSERT ANCHOR TO 'dir:{}'", anchors.display())).await;
    run(&admin, "INSERT INTO at (id) VALUES (1)").await;

    drop(session);
    drop(clerk);
    drop(admin);
    db.shutdown().await.unwrap();
    rasterizeddb_enterprise::columns::forget_cached_keys();
    // A new process knows only what is on disk: the hold must come back
    // from the sidecar the rewrite wrote, not this process's memory.
    rasterizeddb_enterprise::ledger::forget_process_state();

    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();
    // The hold came through the rewrite and the restart.
    let e = refused(&admin, "DELETE FROM ht").await;
    assert!(e.contains("legal hold"), "the hold was lost: {e}");
    // The rotated keyring unwraps under the provider it names.
    assert_eq!(col(&admin, "VAULT hv GET SECRET 's'").await, vec![Some("three".into())]);
    assert_eq!(col(&admin, "SELECT ssn FROM pt WHERE id = 1").await, vec![Some("111-11-1111".into())]);
    // The table's own anchor is checked, though no row came since the start.
    let verify = lines(&admin, "VERIFY LEDGER at").await;
    assert!(verify.iter().any(|l| l.contains("anchor dir:")), "the table's anchor was not checked: {verify:?}");
    assert!(!verify.iter().any(|l| l.starts_with("TAMPERED")), "{verify:?}");
    drop(admin);
    db.shutdown().await.unwrap();
}
