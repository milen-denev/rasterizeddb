//! A ledger table and a vault replicate as what they are: the replication
//! log carries the statement as written (`CREATE LEDGER TABLE …`, `CREATE
//! VAULT …`), not the plain `CREATE TABLE` the engine runs for it, which a
//! replica made an ordinary, unguarded table of.

use rasterizeddb_embedded::{Builder, Client};

async fn run(c: &Client, sql: &str) -> Vec<String> {
    let rows = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().filter_map(|r| r.get::<Option<String>>(0).ok().flatten()).collect()
}

/// The files under `dir` holding `needle`.
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
async fn a_ledger_table_and_a_vault_replicate_as_themselves() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine starts, in the only test of this process.
    unsafe {
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key = root.path().join("kek");
    std::fs::write(&key, "55".repeat(32)).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).password("admin", "adminpw").start().await.unwrap();
    let admin = db.connect().await.unwrap();

    run(&admin, "REPLICATION SET node_id = 1").await;
    run(&admin, "REPLICATION ENABLE").await;
    let ledger = "CREATE LEDGER TABLE rl (id BIGINT NOT NULL) NO DELETE UNTIL 1 DAY AFTER INSERT NO DROP UNTIL 30 DAYS IDLE";
    admin.execute(ledger, &[]).await.unwrap();
    let vault = format!("CREATE VAULT rv KEY PROVIDER 'file:{}'", key.display());
    admin.execute(&vault, &[]).await.unwrap();
    run(&admin, "REPLICATION FLUSH").await;

    // The statements as written, in the replication log.
    assert!(!on_disk(&data, ledger.as_bytes()).is_empty(), "the ledger table's CREATE was not captured as written");
    assert!(!on_disk(&data, vault.as_bytes()).is_empty(), "the vault's CREATE was not captured as written");
    // And not the plain CREATE TABLE a replica would make an ordinary table of.
    assert!(on_disk(&data, b"CREATE TABLE rl ").is_empty(), "a plain CREATE TABLE of the ledger table was captured");
    assert!(on_disk(&data, b"CREATE TABLE rv ").is_empty(), "a plain CREATE TABLE of the vault was captured");

    drop(admin);
    db.shutdown().await.unwrap();
}
