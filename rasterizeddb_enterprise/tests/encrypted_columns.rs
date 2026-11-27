//! Encrypted columns end to end (docs/ledger-and-vault-plan.md §4.7), in the
//! enterprise build with the engine in-process: values stored as ciphertext,
//! decrypted only for a role holding the vault's SELECT, and every statement
//! that would compute on ciphertext refused rather than answered. One test
//! function: the vault's settings are read from the environment once per
//! process.

use rasterizeddb_embedded::{Builder, Client, Login, Options};

async fn col(c: &Client, sql: &str) -> Vec<Option<String>> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<Option<String>>(0).unwrap()).collect()
}

async fn run(c: &Client, sql: &str) {
    c.execute(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(c: &Client, sql: &str) -> String {
    match c.query(sql, &[]).await {
        Ok(rows) => panic!("{sql} was answered ({} rows) over ciphertext", rows.len()),
        Err(e) => e.to_string(),
    }
}

/// Whether `needle` is in any file under `dir`.
fn on_disk(dir: &std::path::Path, needle: &[u8]) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p).is_ok_and(|b| b.windows(needle.len()).any(|w| w == needle)) {
                found.push(p);
            }
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_columns() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key = root.path().join("kek");
    std::fs::write(&key, "22".repeat(32)).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();

    // --- a vault key, and a table with an encrypted column ------------------
    admin.execute(&format!("CREATE VAULT hr KEY PROVIDER 'file:{}'", key.display()), &[]).await.unwrap();
    col(&admin, "VAULT hr CREATE KEY 'pii' TYPE 'aes256-gcm'").await;
    run(&admin, "CREATE TABLE patients (id BIGINT, name TEXT, ssn VARCHAR(11) ENCRYPTED WITH VAULT hr KEY 'pii')").await;
    run(&admin, "INSERT INTO patients (id, name, ssn) VALUES (1, 'ann', '123-45-6789'), (2, 'bob', '987-65-4321'), (3, 'cy', NULL)").await;
    run(&admin, "UPDATE patients SET ssn = '555-55-5555' WHERE id = 2").await;

    // Read back whole: the plaintext, NULL as NULL.
    assert_eq!(
        col(&admin, "SELECT ssn FROM patients ORDER BY id").await,
        vec![Some("123-45-6789".into()), Some("555-55-5555".into()), None]
    );
    let star = admin.query("SELECT * FROM patients WHERE id = 1", &[]).await.unwrap();
    assert_eq!(star.get(0).unwrap().get::<String>(2).unwrap(), "123-45-6789");
    assert_eq!(col(&admin, "DELETE FROM patients WHERE id = 3 RETURNING ssn").await, vec![None]);

    // The value is too long for VARCHAR(11): refused before it is sealed.
    let long = refused(&admin, "INSERT INTO patients (id, ssn) VALUES (9, '123-45-67890')").await;
    assert!(long.contains("too long"), "{long}");

    // --- nothing that would compute on ciphertext runs ------------------------
    for sql in [
        "SELECT id FROM patients WHERE ssn = '123-45-6789'",
        "SELECT id FROM patients WHERE ssn LIKE '123%'",
        "SELECT id FROM patients WHERE ssn IS NOT NULL",
        "SELECT id FROM patients ORDER BY ssn",
        "SELECT ssn, count(*) FROM patients GROUP BY ssn",
        "SELECT DISTINCT ssn FROM patients",
        "SELECT count(DISTINCT ssn) FROM patients",
        "SELECT max(ssn) FROM patients",
        "SELECT upper(ssn) FROM patients",
        "SELECT ssn FROM patients ORDER BY 1",
        "SELECT ssn AS s FROM patients ORDER BY s",
        "SELECT a.id FROM patients a JOIN patients b ON a.ssn = b.ssn",
        "SELECT id FROM patients WHERE id IN (SELECT id FROM patients WHERE ssn = 'x')",
        "SELECT ssn FROM patients UNION SELECT ssn FROM patients",
        "CREATE INDEX ix_ssn ON patients (ssn)",
        "UPDATE patients SET name = 'z' WHERE ssn = '123-45-6789'",
        "UPDATE patients SET ssn = upper(ssn)",
        "DELETE FROM patients WHERE ssn = '123-45-6789'",
        "INSERT INTO patients (id, ssn) SELECT id, ssn FROM patients",
        "INSERT INTO patients VALUES (7, 'x', '000-00-0000')",
        "CREATE TABLE copy AS SELECT * FROM patients",
        "ALTER TABLE patients RENAME COLUMN ssn TO s",
    ] {
        let e = refused(&admin, sql).await;
        assert!(e.contains("encrypted") || e.contains("0A000"), "{sql}: {e}");
    }
    // Bound parameters meet the same guard: a filter by $1 is refused, and
    // a value bound for the column is sealed like a literal.
    let e = admin.query("SELECT id FROM patients WHERE ssn = $1", &["123-45-6789".into()]).await.expect_err("a bound filter");
    assert!(e.to_string().contains("encrypted"), "{e}");
    admin
        .execute("INSERT INTO patients (id, name, ssn) VALUES ($1, $2, $3)", &[4i64.into(), "dee".into(), "444-44-4444".into()])
        .await
        .expect("a bound value for an encrypted column");
    assert_eq!(col(&admin, "SELECT ssn FROM patients WHERE id = 4").await, vec![Some("444-44-4444".into())]);

    // --- stored as ciphertext: no plaintext in any file -----------------------
    for secret in ["123-45-6789", "555-55-5555", "444-44-4444"] {
        let found = on_disk(&data, secret.as_bytes());
        assert!(found.is_empty(), "{secret} is stored in the clear in {found:?}");
    }

    // --- only a role holding the vault's SELECT reads plaintext -----------------
    run(&admin, "CREATE ROLE app LOGIN PASSWORD 'apppw'").await;
    run(&admin, "GRANT SELECT, INSERT ON patients TO app").await;
    let app = db.connect_with(Options { login: Login::password("app", "apppw"), ..db.options() }).await.unwrap();
    let sealed = col(&app, "SELECT ssn FROM patients WHERE id = 1").await;
    let sealed = sealed[0].clone().unwrap();
    assert!(sealed.starts_with("rdbenc1:"), "a role without the vault's SELECT read {sealed}");
    let e = app.execute("INSERT INTO patients (id, ssn) VALUES (8, '888-88-8888')", &[]).await.expect_err("encrypting without the key");
    assert!(e.to_string().contains("permission denied for vault"), "{e}");
    run(&admin, "GRANT SELECT ON hr TO app").await;
    assert_eq!(col(&app, "SELECT ssn FROM patients WHERE id = 1").await, vec![Some("123-45-6789".into())]);
    app.execute("INSERT INTO patients (id, ssn) VALUES (8, '888-88-8888')", &[]).await.unwrap();
    // A ciphertext copied elsewhere is still its column's: it decrypts
    // wherever it is read, for a role holding that column's vault, and for
    // no one else. The key is the column's, not the place's.
    run(&admin, "CREATE TABLE notes (id BIGINT, body TEXT)").await;
    admin.execute(&format!("INSERT INTO notes VALUES (1, '{sealed}')"), &[]).await.unwrap();
    assert_eq!(col(&admin, "SELECT body FROM notes").await, vec![Some("123-45-6789".into())], "the same column's value, read where it was copied");

    // --- a ledger table with an encrypted column --------------------------------
    run(&admin, "CREATE LEDGER TABLE cards (id BIGINT, pan TEXT ENCRYPTED WITH VAULT hr KEY 'pii') NO DELETE UNTIL 1 DAY AFTER INSERT").await;
    run(&admin, "INSERT INTO cards (id, pan) VALUES (1, '4111111111111111')").await;
    assert_eq!(col(&admin, "SELECT pan FROM cards").await, vec![Some("4111111111111111".into())]);
    let verify = col(&admin, "VERIFY LEDGER cards").await;
    assert!(verify.iter().flatten().all(|l| !l.starts_with("TAMPERED") && !l.starts_with("UNKNOWN")), "{verify:?}");
    assert!(on_disk(&data, b"4111111111111111").is_empty());
    let show = col(&admin, "SHOW LEDGER").await;
    assert!(show.iter().flatten().any(|l| l.starts_with("encrypted column patients.ssn: vault hr key 'pii'")), "{show:?}");
    assert!(show.iter().flatten().any(|l| l.starts_with("encrypted column cards.pan")), "{show:?}");

    // --- across a restart: the keys are unwrapped again, through the vault ------
    drop(app);
    drop(admin);
    db.shutdown().await.unwrap();
    rasterizeddb_enterprise::columns::forget_cached_keys();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();
    assert_eq!(col(&admin, "SELECT ssn FROM patients WHERE id = 8").await, vec![Some("888-88-8888".into())]);
    assert!(refused(&admin, "SELECT id FROM patients WHERE ssn = '888-88-8888'").await.contains("encrypted"));

    // --- a dropped table's columns are forgotten ---------------------------------
    run(&admin, "DROP TABLE patients").await;
    run(&admin, "CREATE TABLE patients (id BIGINT, ssn TEXT)").await;
    run(&admin, "INSERT INTO patients VALUES (1, 'plain')").await;
    assert_eq!(col(&admin, "SELECT ssn FROM patients WHERE ssn = 'plain'").await, vec![Some("plain".into())]);
}
