//! The three items left open by the ledger's stabilization: an encrypted
//! column's plaintext kept in the statement log, a legal hold released by
//! the table's owner, and a policy record cut from the end of its sidecar
//! read as an interrupted append. One test function: the vault's settings
//! are read from the environment once per process.

use rasterizeddb_embedded::{Builder, Client, Login, Options};

async fn col(c: &Client, sql: &str) -> Vec<Option<String>> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<Option<String>>(0).unwrap()).collect()
}

async fn lines(c: &Client, sql: &str) -> Vec<String> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter()
        .map(|r| (0..r.len()).filter_map(|i| r.get::<Option<String>>(i).ok().flatten()).collect::<Vec<_>>().join(" | "))
        .collect()
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
async fn the_open_items_are_closed() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key = root.path().join("kek");
    std::fs::write(&key, "88".repeat(32)).unwrap();
    {
        #[cfg(unix)]
        std::fs::set_permissions(&key, <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600)).unwrap();
    }
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();

    // --- an encrypted column's plaintext is not kept in the statement log ------
    run(&admin, &format!("CREATE VAULT fv KEY PROVIDER 'file:{}'", key.display())).await;
    lines(&admin, "VAULT fv CREATE KEY 'pii'").await;
    run(&admin, "CREATE TABLE secrets_t (id BIGINT, ssn TEXT ENCRYPTED WITH VAULT fv KEY 'pii')").await;
    run(&admin, "INSERT INTO secrets_t (id, ssn) VALUES (1, '987-65-4321')").await;
    run(&admin, "UPDATE secrets_t SET ssn = '555-12-3434' WHERE id = 1").await;
    let shown = lines(&admin, "SHOW QUERIES").await;
    assert!(shown.iter().any(|l| l.contains("INSERT INTO secrets_t") && l.contains("'<redacted>'")), "not recorded: {shown:#?}");
    assert!(!shown.iter().any(|l| l.contains("987-65-4321") || l.contains("555-12-3434")), "plaintext in SHOW QUERIES: {shown:#?}");
    // A statement about no encrypted table keeps its literals.
    run(&admin, "CREATE TABLE open_t (v TEXT)").await;
    run(&admin, "INSERT INTO open_t (v) VALUES ('visible-literal')").await;
    let shown = lines(&admin, "SHOW QUERIES").await;
    assert!(shown.iter().any(|l| l.contains("'visible-literal'")), "{shown:#?}");

    // --- a legal hold is released only by a superuser ----------------------------
    run(&admin, "CREATE ROLE keeper LOGIN PASSWORD 'keeperpw'").await;
    run(&admin, "CREATE LEDGER TABLE held (id BIGINT NOT NULL) NO DELETE UNTIL 0 SECONDS AFTER INSERT").await;
    run(&admin, "ALTER TABLE held OWNER TO keeper").await;
    let keeper = db.connect_with(Options { login: Login::password("keeper", "keeperpw"), ..db.options() }).await.unwrap();
    lines(&keeper, "ALTER LEDGER TABLE held SET LEGAL HOLD 'case'").await;
    let e = refused(&keeper, "ALTER LEDGER TABLE held RELEASE LEGAL HOLD 'case'").await;
    assert!(e.contains("42501") && e.contains("superuser"), "the owner released a hold: {e}");
    lines(&admin, "ALTER LEDGER TABLE held RELEASE LEGAL HOLD 'case'").await;
    drop(keeper);

    // --- a policy record cut from the sidecar is found -----------------------------
    run(&admin, "CREATE LEDGER TABLE cut_t (id BIGINT NOT NULL) NO DELETE UNTIL 0 SECONDS AFTER INSERT").await;
    run(&admin, "INSERT INTO cut_t (id) VALUES (1)").await;
    // The table's own sidecar: the one that grows with its ALTER.
    let sidecars: Vec<(std::path::PathBuf, u64)> = {
        let mut all = Vec::new();
        let mut stack = vec![data.clone()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.to_string_lossy().ends_with("LEDGER.db") {
                    all.push((p.clone(), std::fs::metadata(&p).unwrap().len()));
                }
            }
        }
        all
    };
    lines(&admin, "ALTER LEDGER TABLE cut_t SET LEGAL HOLD 'kept'").await;
    let verify = lines(&admin, "VERIFY LEDGER cut_t").await;
    assert!(!verify.iter().any(|l| l.starts_with("TAMPERED")), "{verify:?}");
    let (path, before) = sidecars
        .iter()
        .find(|(p, len)| std::fs::metadata(p).unwrap().len() > *len)
        .cloned()
        .expect("the ALTER grew no policy sidecar");
    // Past a checkpoint, so the log no longer holds the record's write to
    // replay at the next open.
    rasterizeddb_core::core::wal::checkpoint_all().expect("checkpoint");
    drop(admin);
    db.shutdown().await.unwrap();

    // Cut the hold's record off the end, as someone with the files would.
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(before).unwrap();
    drop(file);
    // A new process knows only what is on disk.
    rasterizeddb_enterprise::ledger::forget_process_state();

    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();
    // The cut took the hold with it: a DELETE now passes the guard, which
    // is why VERIFY must say so.
    let cut = admin.execute("DELETE FROM cut_t", &[]).await;
    assert!(cut.is_ok(), "the hold is still in force, so nothing was cut: {cut:?}");
    let verify = lines(&admin, "VERIFY LEDGER cut_t").await;
    assert!(
        verify.iter().any(|l| l.contains("cut from it")),
        "a policy record cut from the sidecar went unnoticed: {verify:?}"
    );
    drop(admin);
    db.shutdown().await.unwrap();
}
