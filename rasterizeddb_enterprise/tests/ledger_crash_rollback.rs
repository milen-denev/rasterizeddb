//! A block that created a vault and a ledger table, then crashed before its
//! COMMIT: the next start drops both, past their NO DROP UNTIL … IDLE (the
//! block made them; nothing of them committed), and forgets the encrypted
//! columns it declared. A real crash: the test runs itself as a child
//! process, which aborts with the block open.

use rasterizeddb_embedded::{Builder, Client};

const CHILD: &str = "RDB_LEDGER_CRASH_CHILD";

async fn col(c: &Client, sql: &str) -> Vec<Option<String>> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<Option<String>>(0).unwrap()).collect()
}

async fn table_exists(c: &Client, name: &str) -> bool {
    !col(c, &format!("SELECT table_name FROM information_schema.tables WHERE table_name = '{name}'")).await.is_empty()
}

fn key_file(root: &std::path::Path) -> std::path::PathBuf {
    let key = root.join("kek");
    if !key.exists() {
        std::fs::write(&key, "44".repeat(32)).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    key
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_inside_a_block_takes_its_vault_and_ledger_table_with_it() {
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    rasterizeddb_enterprise::install_always().unwrap();

    // --- the child: open a block, make the vault and the table, abort ------------
    if let Ok(root) = std::env::var(CHILD) {
        let root = std::path::PathBuf::from(root);
        let key = key_file(&root);
        let db = Builder::open(root.join("data")).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
        let admin = db.connect().await.unwrap();
        let session = admin.simple_session().await.unwrap();
        session
            .query(&format!(
                "BEGIN; \
                 CREATE VAULT cv KEY PROVIDER 'file:{}'; \
                 VAULT cv CREATE KEY 'k'; \
                 CREATE LEDGER TABLE cl (id BIGINT NOT NULL, p TEXT ENCRYPTED WITH VAULT cv KEY 'k' NOT NULL) \
                   NO DELETE UNTIL 1 DAY AFTER INSERT NO DROP UNTIL 90 DAYS IDLE; \
                 INSERT INTO cl (id, p) VALUES (1, 'never committed')",
                key.display()
            ))
            .await
            .expect("the block's statements");
        // The crash: no COMMIT, no ROLLBACK, no shutdown.
        std::process::abort();
    }

    // --- the parent ------------------------------------------------------------
    let root = tempfile::tempdir().unwrap();
    let key = key_file(root.path());
    {
        // A committed table first, which the crash must leave alone.
        let db = Builder::open(root.path().join("data")).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
        let admin = db.connect().await.unwrap();
        admin
            .execute(&format!("CREATE VAULT keep KEY PROVIDER 'file:{}'", key.display()), &[])
            .await
            .unwrap();
        drop(admin);
        db.shutdown().await.unwrap();
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "a_crash_inside_a_block_takes_its_vault_and_ledger_table_with_it", "--nocapture", "--test-threads", "1"])
        .env(CHILD, root.path())
        .status()
        .unwrap();
    assert!(!status.success(), "the child was to abort with its block open");

    let db = Builder::open(root.path().join("data")).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();
    assert!(!table_exists(&admin, "cl").await, "the crashed block's ledger table survived the restart");
    assert!(!table_exists(&admin, "cv").await, "the crashed block's vault survived the restart");
    assert!(table_exists(&admin, "keep").await, "a committed vault went with the crashed block");
    let show = col(&admin, "SHOW LEDGER").await;
    assert!(!show.iter().flatten().any(|l| l.contains("encrypted column cl.")), "{show:?}");
    // The names are free again.
    admin.execute(&format!("CREATE VAULT cv KEY PROVIDER 'file:{}'", key.display()), &[]).await.unwrap();
    admin
        .execute("CREATE LEDGER TABLE cl (id BIGINT NOT NULL) NO DELETE UNTIL 1 DAY AFTER INSERT", &[])
        .await
        .unwrap();
    admin.execute("INSERT INTO cl (id) VALUES (7)", &[]).await.unwrap();
    assert_eq!(col(&admin, "SELECT id::TEXT FROM cl").await, vec![Some("7".into())]);
    drop(admin);
    db.shutdown().await.unwrap();
}
