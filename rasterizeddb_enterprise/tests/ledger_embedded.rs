//! Ledger tables and secret vaults end to end, in the enterprise build
//! (this crate's hooks installed before the embedded engine starts, as an
//! application installs them)
//! (docs/ledger-and-vault-plan.md): through the native client, against a
//! directory, with anchors on another directory. One test function: the
//! ledger's settings are read from the environment once per process.

use rasterizeddb_embedded::Builder;

async fn lines(client: &rasterizeddb_embedded::Client, sql: &str) -> Vec<String> {
    let rows = client.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter().map(|r| r.get::<String>(0).unwrap()).collect()
}

async fn refused(client: &rasterizeddb_embedded::Client, sql: &str) -> String {
    match client.execute(sql, &[]).await {
        Ok(n) => panic!("{sql} was not refused ({n} rows)"),
        Err(e) => e.to_string(),
    }
}

fn intact(report: &[String]) -> bool {
    report.iter().all(|l| !l.starts_with("TAMPERED") && !l.starts_with("UNKNOWN"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ledger_tables_and_vaults() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let ledger = root.path().join("ledger");
    let anchors = root.path().join("anchors");
    std::fs::create_dir_all(&data).unwrap();
    // SAFETY: set before the engine (and anything reading them) starts, in
    // the only test of this process.
    unsafe {
        std::env::set_var("RDB_LEDGER_DIR", &ledger);
        std::env::set_var("RDB_LEDGER_ANCHORS", format!("dir:{}", anchors.display()));
        std::env::set_var("RDB_LEDGER_ANCHOR_MODE", "sync");
        std::env::set_var("RDB_VAULT_ALLOW_FILE_KEY", "1");
    }
    let key = root.path().join("kek");
    std::fs::write(&key, "11".repeat(32)).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    // As an application does: the licensed hooks before the engine opens.
    rasterizeddb_enterprise::install_always().unwrap();
    let db = Builder::open(&data).worker_threads(2).start().await.unwrap();
    let c = db.connect().await.unwrap();

    // --- a ledger table ------------------------------------------------------
    c.execute("CREATE LEDGER TABLE pay (id BIGINT, amount BIGINT) NO DELETE UNTIL 2 SECONDS AFTER INSERT", &[])
        .await
        .unwrap();
    for i in 1..=3i64 {
        c.execute(&format!("INSERT INTO pay (id, amount) VALUES ({i}, {})", i * 100), &[]).await.unwrap();
    }
    let rows = c.query("SELECT id, amount FROM pay ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);

    // Nothing changes a ledger row.
    assert!(refused(&c, "UPDATE pay SET amount = 0 WHERE id = 1").await.contains("append-only"));
    let t = refused(&c, "TRUNCATE pay").await;
    assert!(t.contains("ledger"), "{t}");
    let a = refused(&c, "ALTER TABLE pay DROP COLUMN amount").await;
    assert!(a.contains("ledger"), "{a}");
    // Not before its retention.
    let d = refused(&c, "DELETE FROM pay WHERE id = 1").await;
    assert!(d.contains("retention"), "{d}");

    let report = lines(&c, "VERIFY LEDGER pay").await;
    assert!(intact(&report), "{report:#?}");
    assert!(report.iter().any(|l| l.starts_with("OK        pay: 3 row(s)")), "{report:#?}");
    assert!(report.iter().any(|l| l.starts_with("OK        anchor")), "{report:#?}");

    // Past its retention, a row goes, and the chain records it.
    tokio::time::sleep(std::time::Duration::from_millis(2300)).await;
    assert_eq!(c.execute("DELETE FROM pay WHERE id = 1", &[]).await.unwrap(), 1);
    let report = lines(&c, "VERIFY LEDGER pay").await;
    assert!(report.iter().any(|l| l.starts_with("OK        pay: 2 row(s)")), "{report:#?}");

    // A legal hold stops even an expired row.
    lines(&c, "ALTER LEDGER TABLE pay SET LEGAL HOLD 'case1'").await;
    assert!(refused(&c, "DELETE FROM pay WHERE id = 2").await.contains("legal hold"));
    lines(&c, "ALTER LEDGER TABLE pay RELEASE LEGAL HOLD 'case1'").await;
    // A retention is only raised.
    let lowered = c.query("ALTER LEDGER TABLE pay SET RETENTION 1 SECOND", &[]).await;
    assert!(lowered.is_err(), "a retention was lowered");
    lines(&c, "ALTER LEDGER TABLE pay SET RETENTION 1 DAY").await;
    assert!(refused(&c, "DELETE FROM pay WHERE id = 2").await.contains("retention"));

    let show = lines(&c, "SHOW LEDGER pay").await;
    assert!(show.iter().any(|l| l.starts_with("pay: ledger")), "{show:#?}");

    // A plain table is untouched by all of this.
    c.execute("CREATE TABLE plain (id BIGINT)", &[]).await.unwrap();
    c.execute("INSERT INTO plain VALUES (1)", &[]).await.unwrap();
    c.execute("UPDATE plain SET id = 2", &[]).await.unwrap();

    // --- a vault -------------------------------------------------------------
    c.execute(&format!("CREATE VAULT sec KEY PROVIDER 'file:{}' RETAIN DELETED 1 SECONDS", key.display()), &[])
        .await
        .unwrap();
    assert_eq!(lines(&c, "VAULT sec SET SECRET 'db' = 'hunter2'").await, vec!["db version 1"]);
    assert_eq!(lines(&c, "VAULT sec GET SECRET 'db'").await, vec!["hunter2"]);
    lines(&c, "VAULT sec SET SECRET 'app/url' = 'https://x'").await;
    let all = lines(&c, "VAULT sec GET ALL").await;
    assert_eq!(all.len(), 2, "{all:#?}");
    assert!(all.contains(&r#"{"name":"db","version":1,"value":"hunter2"}"#.to_string()), "{all:#?}");
    assert_eq!(lines(&c, "VAULT sec GET ALL PREFIX 'app/'").await.len(), 1);
    lines(&c, "VAULT sec SET SECRET 'db' = 'correct horse' CONTENT TYPE 'text/plain'").await;
    assert_eq!(lines(&c, "VAULT sec GET SECRET 'db'").await, vec!["correct horse"]);
    assert_eq!(lines(&c, "VAULT sec GET SECRET 'db' VERSION 1").await, vec!["hunter2"]);
    assert!(lines(&c, "VAULT sec LIST").await.iter().any(|l| l.starts_with("db v2 active")));
    // The ciphertext, never the value, is what the table holds.
    let stored = c.query("SELECT value FROM sec", &[]).await.unwrap();
    for r in stored.iter() {
        let v = r.get::<String>(0).unwrap();
        assert!(!v.contains("hunter2") && !v.contains("horse"));
    }
    // A client's own INSERT is refused.
    let forged = refused(
        &c,
        "INSERT INTO sec (name, version, value, state, content_type, not_before, expires_at, tags, created_by, created_at) \
         VALUES ('db', 3, 'x', 'active', '', 0, 0, '', 'me', 0)",
    )
    .await;
    assert!(forged.contains("vault"), "{forged}");

    // Soft delete, recover, delete, purge.
    lines(&c, "VAULT sec DELETE SECRET 'db'").await;
    assert!(c.query("VAULT sec GET SECRET 'db'", &[]).await.is_err());
    lines(&c, "VAULT sec RECOVER SECRET 'db'").await;
    assert_eq!(lines(&c, "VAULT sec GET SECRET 'db'").await, vec!["correct horse"]);
    lines(&c, "VAULT sec DELETE SECRET 'db'").await;
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    lines(&c, "VAULT sec PURGE SECRET 'db'").await;
    lines(&c, "VAULT sec RECOVER SECRET 'db'").await;
    let gone = c.query("VAULT sec GET SECRET 'db'", &[]).await.unwrap_err().to_string();
    assert!(gone.contains("purged"), "{gone}");

    // Keys used in place.
    lines(&c, "VAULT sec CREATE KEY 'k' TYPE 'aes256-gcm'").await;
    let ct = lines(&c, "VAULT sec ENCRYPT KEY 'k' VALUE 'payload'").await.remove(0);
    assert_ne!(ct, "payload");
    assert_eq!(lines(&c, &format!("VAULT sec DECRYPT KEY 'k' VALUE '{ct}'")).await, vec!["payload"]);

    // Every read and change is in the audit, and chained.
    let audit = lines(&c, "VAULT sec AUDIT").await;
    assert!(audit.len() >= 8, "{audit:#?}");
    assert!(audit.iter().all(|l| l.starts_with("chained")), "{audit:#?}");
    assert!(audit.iter().any(|l| l.contains("REFUSED")), "{audit:#?}");

    let report = lines(&c, "VERIFY LEDGER").await;
    assert!(intact(&report), "{report:#?}");

    // --- tampering -------------------------------------------------------------
    drop(c);
    db.shutdown().await.unwrap();
    let chain = std::fs::read_dir(&ledger)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path().join("chain.log"))
        .find(|p| p.exists())
        .expect("a chain file");
    let dirs: Vec<_> = std::fs::read_dir(&ledger).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).collect();
    eprintln!("ledger dir holds {dirs:?}; corrupting {}", chain.display());
    let mut bytes = std::fs::read(&chain).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x40;
    std::fs::write(&chain, &bytes).unwrap();

    let db = Builder::open(&data).worker_threads(2).start().await.unwrap();
    let c = db.connect().await.unwrap();
    eprintln!("after tampering: {:#?}", c.query("VERIFY LEDGER pay", &[]).await.map(|r| r.iter().map(|x| x.get::<String>(0).unwrap()).collect::<Vec<_>>()));
    // A broken chain stops ledger writes and says so.
    let broken = refused(&c, "INSERT INTO pay (id, amount) VALUES (9, 9)").await;
    assert!(broken.contains("broken"), "{broken}");
    assert!(c.query("VERIFY LEDGER pay", &[]).await.is_err() || !intact(&lines(&c, "VERIFY LEDGER pay").await));
    drop(c);
    db.shutdown().await.unwrap();
}
