//! The ORM's features against a real server: optimistic locking, relations,
//! projections, keyset pages, timestamps and soft delete, upserts,
//! hooks, partitioning, the read-through cache and bulk updates.
//!
//! Needs `RDB_SERVER_BIN`, as `sdk_live.rs` does; without it each test says
//! so and passes.
#![cfg(feature = "derive")]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::prelude::*;
use tokio::process::{Child, Command};

struct Server {
    child: Child,
    dir: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn server(name: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_orm_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
        ])
        .args(["--log-level", "off"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server {
        child,
        dir,
        socket: format!("unix:{}", socket.display()),
    };
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

async fn connect(server: &Server) -> Client {
    Client::connect(
        &server.socket,
        Options {
            timeout: Some(Duration::from_secs(30)),
            ..Options::default()
        },
    )
    .await
    .expect("connect")
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(300), f)
            .await
            .expect("the test did not finish within 300 s")
    }
}

// --------------------------------------------------------- optimistic locking

#[derive(Model, Debug, Clone, PartialEq)]
struct Doc {
    #[rdb(primary_key)]
    id: i64,
    body: String,
    #[rdb(version)]
    version: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_version_column_catches_lost_updates() {
    bounded(async {
        let Some(server) = server("version").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Doc>().await.unwrap();

        let read = c
            .insert(&Doc {
                id: 1,
                body: "first".into(),
                version: 9,
            })
            .await
            .unwrap();
        assert_eq!(read.version, 1, "an insert starts at 1");

        let mut mine = read.clone();
        mine.body = "mine".into();
        let stored = c.update(&mine).await.unwrap();
        assert_eq!((stored.body.as_str(), stored.version), ("mine", 2));

        // The copy read before that update is stale now.
        let mut theirs = read.clone();
        theirs.body = "theirs".into();
        let e = c.update(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        assert!(
            e.to_string().contains("version 1") && e.to_string().contains("version 2"),
            "{e}"
        );
        let e = c.delete(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        let e = c.upsert(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        let now: Doc = c.find(1i64).await.unwrap().unwrap();
        assert_eq!(now, stored, "no stale write landed");

        // The current copy goes through, and moves the version each time.
        let mut current = now.clone();
        current.body = "upserted".into();
        let up = c.upsert(&current).await.unwrap();
        assert_eq!((up.body.as_str(), up.version), ("upserted", 3));
        let fresh = c
            .upsert(&Doc {
                id: 2,
                body: "new".into(),
                version: 0,
            })
            .await
            .unwrap();
        assert_eq!(fresh.version, 1);

        // A bulk update moves it too, so the copy above is stale after it.
        let bumped = Doc::query()
            .filter(Doc::id.eq(1i64))
            .update()
            .set(Doc::body, "bulk")
            .run(&c)
            .await
            .unwrap();
        assert_eq!(bumped[0].version, 4);
        assert!(c.update(&up).await.unwrap_err().is_conflict());

        // Gone is not a conflict: an update says no row, a delete false.
        let mut gone = fresh.clone();
        gone.id = 99;
        assert!(matches!(
            c.update(&gone).await,
            Err(Error::RowCount { got: 0, .. })
        ));
        assert!(!c.delete(&gone).await.unwrap());
        assert!(c.delete(&bumped[0]).await.unwrap());
        assert!(!c.delete(&bumped[0]).await.unwrap(), "already gone");

        // Two writers holding the same version: exactly one wins.
        let base = c
            .insert(&Doc {
                id: 3,
                body: "race".into(),
                version: 0,
            })
            .await
            .unwrap();
        let c = std::sync::Arc::new(c);
        let mut tasks = Vec::new();
        for n in 0..8 {
            let c = c.clone();
            let mut doc = base.clone();
            doc.body = format!("writer {n}");
            tasks.push(tokio::spawn(async move { c.update(&doc).await }));
        }
        let mut won = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(doc) => {
                    won += 1;
                    assert_eq!(doc.version, 2);
                }
                Err(e) => assert!(e.is_conflict(), "{e}"),
            }
        }
        assert_eq!(won, 1, "one writer per version");
        let last: Doc = c.find(3i64).await.unwrap().unwrap();
        assert_eq!(last.version, 2);
    })
    .await
}
