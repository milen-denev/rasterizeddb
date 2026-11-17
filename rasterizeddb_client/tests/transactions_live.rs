//! Transactions against a real server.
//!
//! Needs `RDB_SERVER_BIN`, as `sdk_live.rs` does (a server built with native
//! transactions); without it each test says so and passes.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rasterizeddb_client::{
    Client, Error, Executor, Isolation, Options, Transaction, TransactionOptions, params,
};
use tokio::process::{Child, Command};

struct Server {
    bin: String,
    child: Child,
    dir: PathBuf,
    socket_path: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn(bin: &str, dir: &std::path::Path, socket: &std::path::Path) -> Child {
    Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
        ])
        .args(["--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server")
}

async fn wait_for(socket: &std::path::Path) {
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(socket).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

async fn server(name: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_tx_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket_path = dir.join("s.sock");
    let child = spawn(&bin, &dir, &socket_path);
    wait_for(&socket_path).await;
    Some(Server {
        bin,
        child,
        socket: format!("unix:{}", socket_path.display()),
        socket_path,
        dir,
    })
}

impl Server {
    /// Kills the server (no shutdown: the connections just die).
    async fn kill(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    /// Starts it again on the same socket and data.
    async fn restart(&mut self) {
        self.child = spawn(&self.bin, &self.dir, &self.socket_path);
        wait_for(&self.socket_path).await;
    }
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

/// `t (id BIGINT PRIMARY KEY, v BIGINT)` with ids 1..=rows, v = 0.
async fn table(c: &Client, rows: i64) {
    c.run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    for id in 1..=rows {
        c.exec("INSERT INTO t (id, v) VALUES ($1, 0)", params![id])
            .await
            .unwrap();
    }
}

async fn v(c: &Client, id: i64) -> i64 {
    c.fetch_scalar("SELECT v FROM t WHERE id = $1", params![id])
        .await
        .unwrap()
}

async fn count(c: &Client) -> usize {
    c.run("SELECT id FROM t").await.unwrap().len()
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_is_seen_and_rollback_is_not() {
    bounded(async {
        let Some(server) = server("commit").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 1).await;

        let tx = c.begin().await.unwrap();
        tx.exec(
            "INSERT INTO t (id, v) VALUES ($1, $2)",
            params![2i64, 20i64],
        )
        .await
        .unwrap();
        tx.exec("UPDATE t SET v = $1 WHERE id = 1", params![5i64])
            .await
            .unwrap();
        assert_eq!(
            tx.fetch_scalar::<i64>("SELECT v FROM t WHERE id = 1", params![])
                .await
                .unwrap(),
            5,
            "the transaction reads its own writes"
        );
        assert_eq!(count(&c).await, 1, "nobody else sees them before COMMIT");
        assert_eq!(v(&c, 1).await, 0);
        tx.commit().await.unwrap();
        assert_eq!(count(&c).await, 2);
        assert_eq!(v(&c, 1).await, 5);

        let tx = c.begin().await.unwrap();
        tx.exec("INSERT INTO t (id, v) VALUES (3, 0)", params![])
            .await
            .unwrap();
        tx.exec("UPDATE t SET v = 6 WHERE id = 1", params![])
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(count(&c).await, 2);
        assert_eq!(v(&c, 1).await, 5);

        // A failed statement fails the transaction: COMMIT then rolls back,
        // and says so.
        let tx = c.begin().await.unwrap();
        tx.exec("INSERT INTO t (id, v) VALUES (4, 0)", params![])
            .await
            .unwrap();
        let dup = tx
            .exec("INSERT INTO t (id, v) VALUES (1, 0)", params![])
            .await
            .unwrap_err();
        assert_eq!(dup.sqlstate(), Some("23505"), "{dup}");
        assert!(dup.is_unique_violation(), "{dup}");
        let refused = tx.run("SELECT id FROM t").await.unwrap_err();
        assert_eq!(refused.sqlstate(), Some("25P02"), "{refused}");
        let commit = tx.commit().await.unwrap_err();
        assert_eq!(commit.sqlstate(), Some("25P02"), "{commit}");
        assert_eq!(
            count(&c).await,
            2,
            "nothing of the failed transaction landed"
        );

        // Options: read only, and an isolation level.
        let tx = c
            .begin_with(TransactionOptions {
                read_only: true,
                ..TransactionOptions::default()
            })
            .await
            .unwrap();
        let e = tx.run("DELETE FROM t").await.unwrap_err();
        assert_eq!(e.sqlstate(), Some("25006"), "{e}");
        tx.rollback().await.unwrap();

        let tx = c
            .begin_with(TransactionOptions {
                isolation: Some(Isolation::RepeatableRead),
                ..TransactionOptions::default()
            })
            .await
            .unwrap();
        let level: String = tx
            .fetch_scalar("SHOW transaction_isolation", params![])
            .await
            .unwrap();
        assert_eq!(level, "repeatable read");
        assert_eq!(tx.run("SELECT id FROM t").await.unwrap().len(), 2);
        c.exec("INSERT INTO t (id, v) VALUES (9, 0)", params![])
            .await
            .unwrap();
        assert_eq!(
            tx.run("SELECT id FROM t").await.unwrap().len(),
            2,
            "repeatable read keeps its snapshot"
        );
        tx.commit().await.unwrap();
        assert_eq!(count(&c).await, 3);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_transaction_rolls_it_back_and_frees_its_rows() {
    bounded(async {
        let Some(server) = server("drop").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 1).await;

        let tx = c.begin().await.unwrap();
        tx.run("UPDATE t SET v = 99 WHERE id = 1").await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (2, 0)").await.unwrap();
        drop(tx);

        // Would wait on the dropped transaction's row lock if it were held.
        tokio::time::timeout(
            Duration::from_secs(10),
            c.run("UPDATE t SET v = 7 WHERE id = 1"),
        )
        .await
        .expect("the dropped transaction's lock was released")
        .unwrap();
        assert_eq!(v(&c, 1).await, 7);
        assert_eq!(count(&c).await, 1);

        // The closure form rolls back on Err.
        let e = c
            .transaction(|tx| {
                Box::pin(async move {
                    tx.run("UPDATE t SET v = 50 WHERE id = 1").await?;
                    Err::<(), _>(Error::Param("the caller changed its mind".into()))
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Param(_)), "{e}");
        assert_eq!(v(&c, 1).await, 7);
    })
    .await
}

/// A serialization failure in the closure form runs the closure again, in a
/// new transaction; with no retries it is the caller's.
#[tokio::test(flavor = "multi_thread")]
async fn the_closure_form_retries_a_serialization_failure() {
    bounded(async {
        let Some(server) = server("retry").await else {
            return;
        };
        let c = Arc::new(connect(&server).await);
        table(&c, 1).await;

        let repeatable = TransactionOptions {
            isolation: Some(Isolation::RepeatableRead),
            ..TransactionOptions::default()
        };
        let attempts = Arc::new(AtomicU32::new(0));
        let total = c
            .transaction_with(repeatable.clone(), |tx| {
                let c = c.clone();
                let attempts = attempts.clone();
                Box::pin(async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    let before: i64 = tx
                        .fetch_scalar("SELECT v FROM t WHERE id = 1", params![])
                        .await?;
                    if attempt == 1 {
                        // Another writer commits after this snapshot: the
                        // write below then conflicts (40001).
                        c.run("UPDATE t SET v = v + 100 WHERE id = 1").await?;
                    }
                    tx.exec("UPDATE t SET v = $1 WHERE id = 1", params![before + 1])
                        .await?;
                    Ok(before + 1)
                })
            })
            .await
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "one failure, one retry");
        assert_eq!(total, 101);
        assert_eq!(
            v(&c, 1).await,
            101,
            "the retry read the other writer's commit"
        );

        // No retries: the failure is the caller's, and nothing landed.
        let e = c
            .transaction_with(
                TransactionOptions {
                    retries: 0,
                    ..repeatable.clone()
                },
                |tx| {
                    let c = c.clone();
                    Box::pin(async move {
                        let before: i64 = tx
                            .fetch_scalar("SELECT v FROM t WHERE id = 1", params![])
                            .await?;
                        c.run("UPDATE t SET v = v + 1000 WHERE id = 1").await?;
                        tx.exec("UPDATE t SET v = $1 WHERE id = 1", params![before - 1])
                            .await?;
                        Ok(())
                    })
                },
            )
            .await
            .unwrap_err();
        assert!(e.is_serialization_failure(), "{e}");
        assert!(e.is_retryable());
        assert_eq!(v(&c, 1).await, 1101);

        // A closure that swallows a statement's failure and returns Ok does
        // not commit: COMMIT reports the rollback.
        let e = c
            .transaction(|tx| {
                Box::pin(async move {
                    tx.run("UPDATE t SET v = 0 WHERE id = 1").await?;
                    let _ = tx.run("INSERT INTO t (id, v) VALUES (1, 0)").await;
                    Ok(())
                })
            })
            .await
            .unwrap_err();
        assert_eq!(e.sqlstate(), Some("25P02"), "{e}");
        assert_eq!(v(&c, 1).await, 1101);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn savepoints() {
    bounded(async {
        let Some(server) = server("savepoints").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 1).await;

        let tx = c.begin().await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (10, 0)")
            .await
            .unwrap();
        tx.savepoint("a").await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (11, 0)")
            .await
            .unwrap();
        tx.rollback_to("a").await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (12, 0)")
            .await
            .unwrap();
        tx.release("a").await.unwrap();

        // Recovering from a failed statement without losing the transaction.
        tx.savepoint("b").await.unwrap();
        let e = tx
            .run("INSERT INTO t (id, v) VALUES (1, 0)")
            .await
            .unwrap_err();
        assert_eq!(e.sqlstate(), Some("23505"), "{e}");
        assert_eq!(
            tx.run("SELECT id FROM t").await.unwrap_err().sqlstate(),
            Some("25P02")
        );
        tx.rollback_to("b").await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (13, 0)")
            .await
            .unwrap();

        let e = tx.rollback_to("nosuch").await.unwrap_err();
        assert_eq!(e.sqlstate(), Some("3B001"), "{e}");
        assert!(matches!(
            tx.savepoint("not a name").await,
            Err(Error::Param(_))
        ));
        tx.commit().await.unwrap();

        let ids: Vec<(i64,)> = c
            .fetch_all("SELECT id FROM t ORDER BY id", params![])
            .await
            .unwrap();
        assert_eq!(ids, vec![(1,), (10,), (12,), (13,)]);
    })
    .await
}

/// Code written against `E: Executor` runs on a client (each statement
/// committing on its own) and inside a transaction alike.
async fn transfer<E: Executor>(
    e: &E,
    from: i64,
    to: i64,
    amount: i64,
) -> rasterizeddb_client::Result<()> {
    e.exec(
        "UPDATE t SET v = v - $1 WHERE id = $2",
        params![amount, from],
    )
    .await?;
    e.exec("UPDATE t SET v = v + $1 WHERE id = $2", params![amount, to])
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_executor_runs_inside_a_transaction() {
    bounded(async {
        let Some(server) = server("executor").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 2).await;

        transfer(&c, 1, 2, 5).await.unwrap();
        assert_eq!((v(&c, 1).await, v(&c, 2).await), (-5, 5));

        let tx = c.begin().await.unwrap();
        transfer(&tx, 2, 1, 3).await.unwrap();
        transfer(&&tx, 2, 1, 1).await.unwrap();
        let rows: Vec<(i64, i64)> = tx
            .fetch_all("SELECT id, v FROM t ORDER BY id", params![])
            .await
            .unwrap();
        assert_eq!(rows, vec![(1, -1), (2, 1)]);
        let one: Option<(i64,)> = tx
            .fetch_optional("SELECT v FROM t WHERE id = $1", params![2i64])
            .await
            .unwrap();
        assert_eq!(one, Some((1,)));
        assert_eq!(
            (v(&c, 1).await, v(&c, 2).await),
            (-5, 5),
            "not yet committed"
        );
        tx.rollback().await.unwrap();
        assert_eq!((v(&c, 1).await, v(&c, 2).await), (-5, 5));

        // Several tasks on one transaction: their statements queue, one at a
        // time, on its one connection.
        let tx = c.begin().await.unwrap();
        let send = |id: i64| {
            let tx: &Transaction = &tx;
            async move {
                tx.exec("INSERT INTO t (id, v) VALUES ($1, 0)", params![id])
                    .await
                    .unwrap();
            }
        };
        tokio::join!(send(100), send(101), send(102), send(103));
        tx.commit().await.unwrap();
        assert_eq!(count(&c).await, 6);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_control_is_refused_as_a_statement() {
    bounded(async {
        let Some(server) = server("refused").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 0).await;
        for sql in [
            "BEGIN",
            "commit",
            "ROLLBACK",
            "SAVEPOINT a",
            "START TRANSACTION",
        ] {
            match c.run(sql).await {
                Err(Error::Unsupported(m)) => assert!(m.contains("client.begin()"), "{sql}: {m}"),
                other => panic!("{sql}: {other:?}"),
            }
        }
        let tx = c.begin().await.unwrap();
        for sql in ["COMMIT", "ROLLBACK", "BEGIN", "RELEASE a"] {
            match tx.run(sql).await {
                Err(Error::Unsupported(m)) => assert!(m.contains("tx.commit()"), "{sql}: {m}"),
                other => panic!("{sql}: {other:?}"),
            }
            assert!(matches!(
                tx.query(sql, &[]).await,
                Err(Error::Unsupported(_))
            ));
        }
        // Still usable after the refusals, which sent nothing.
        tx.run("INSERT INTO t (id, v) VALUES (1, 0)").await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(count(&c).await, 1);
    })
    .await
}

/// The server going away mid-transaction ends it with an error. Nothing is
/// resent: a server back on the same socket never sees the transaction's
/// later statements, and a COMMIT in flight reports its outcome unknown.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_connection_ends_the_transaction_with_an_error() {
    bounded(async {
        let Some(mut server) = server("lost").await else {
            return;
        };
        {
            let c = connect(&server).await;
            table(&c, 1).await;
        }

        let c = connect(&server).await;
        let tx = c.begin().await.unwrap();
        tx.run("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();
        server.kill().await;
        server.restart().await;

        let e = tx
            .run("INSERT INTO t (id, v) VALUES (2, 0)")
            .await
            .unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");
        assert!(e.is_transport());
        assert!(!e.is_commit_outcome_unknown(), "{e}");
        // Every later call fails the same way, COMMIT included, without
        // reconnecting.
        let e = tx.run("SELECT id FROM t").await.unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");
        let e = tx.commit().await.unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");

        let c = connect(&server).await;
        assert_eq!(count(&c).await, 1, "the statement after the loss never ran");
        assert_eq!(v(&c, 1).await, 0, "the transaction was rolled back");

        // A COMMIT whose connection dies under it: outcome unknown.
        let tx = c.begin().await.unwrap();
        tx.run("UPDATE t SET v = 2 WHERE id = 1").await.unwrap();
        server.kill().await;
        let e = tx.commit().await.unwrap_err();
        assert!(e.is_commit_outcome_unknown(), "{e}");
        assert!(e.to_string().contains("may or may not"), "{e}");
        server.restart().await;

        // A new transaction works, on a new connection: the kept ones died
        // with the server.
        let c = connect(&server).await;
        c.transaction(|tx| {
            Box::pin(async move {
                tx.run("UPDATE t SET v = 3 WHERE id = 1").await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(v(&c, 1).await, 3);
    })
    .await
}

/// A transaction idle past the server's 30 s limit is over: its next call
/// fails, saying why, and the server has rolled it back.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_idle_past_the_server_limit_is_lost() {
    bounded(async {
        let Some(server) = server("idle").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 1).await;

        let tx = c.begin().await.unwrap();
        tx.run("UPDATE t SET v = 42 WHERE id = 1").await.unwrap();
        tokio::time::sleep(Duration::from_secs(31)).await;

        let e = tx.run("SELECT id FROM t").await.unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");
        assert!(e.to_string().contains("idle"), "{e}");
        let e = tx.commit().await.unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");
        assert!(!e.is_commit_outcome_unknown(), "nothing was sent: {e}");

        tokio::time::timeout(
            Duration::from_secs(10),
            c.run("UPDATE t SET v = 1 WHERE id = 1"),
        )
        .await
        .expect("the idle transaction's lock was released")
        .unwrap();
        assert_eq!(v(&c, 1).await, 1);
    })
    .await
}

/// A statement whose future is dropped before its answer (a timeout around
/// it) ends the transaction: its late answer would otherwise be read as the
/// next statement's.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_statement_future_ends_the_transaction() {
    bounded(async {
        let Some(server) = server("cancelled").await else {
            return;
        };
        let c = connect(&server).await;
        table(&c, 1).await;

        let holder = c.begin().await.unwrap();
        holder.run("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();

        let tx = c.begin().await.unwrap();
        tx.run("INSERT INTO t (id, v) VALUES (2, 0)").await.unwrap();
        // Waits on the holder's row lock; given up on after 300 ms.
        let waited = tokio::time::timeout(
            Duration::from_millis(300),
            tx.run("UPDATE t SET v = 2 WHERE id = 1"),
        )
        .await;
        assert!(waited.is_err(), "the update should still be waiting");

        let e = tx.run("SELECT id FROM t").await.unwrap_err();
        assert!(e.is_transaction_lost(), "{e}");
        assert!(e.to_string().contains("dropped"), "{e}");
        assert!(tx.commit().await.unwrap_err().is_transaction_lost());

        holder.commit().await.unwrap();
        // The abandoned update then runs on the server, inside the block its
        // closed connection rolls back.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(v(&c, 1).await, 1);
        assert_eq!(count(&c).await, 1);
        tokio::time::timeout(
            Duration::from_secs(10),
            c.run("UPDATE t SET v = 3 WHERE id = 1"),
        )
        .await
        .expect("nothing of the abandoned transaction holds the row")
        .unwrap();
    })
    .await
}
