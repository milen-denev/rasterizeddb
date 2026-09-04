//! A closed client connection must leave no socket behind on the server.
//!
//! Reported from the deployment: an ordinary sqlx pool cycling idle
//! connections leaked ~16 server-side sockets a minute, all stuck in
//! CLOSE-WAIT — the client had closed and the server never called `close`.
//! After about four hours the engine held 105 of them and every background
//! worker failed with "connection pool timed out".

use sqlx::{AssertSqlSafe, Connection, Executor};
use rasterizeddb_compat::TestServer;

/// Sockets the OS still holds for `port`, by TCP state.
fn states_for(port: u16) -> Vec<String> {
    let out = std::process::Command::new("ss")
        .args(["-tan", "state", "all"])
        .output()
        .expect("ss should run");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains(&format!(":{port} ")))
        .map(|l| {
            l.split_whitespace()
                .next()
                .unwrap_or("?")
                .to_string()
        })
        .collect()
}

fn count(port: u16, state: &str) -> usize {
    states_for(port).iter().filter(|s| s.as_str() == state).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_a_connection_reaps_the_server_side_socket() {
    let server = TestServer::start().await.expect("start");
    let port = server.port();

    // Open, use, and close connections one at a time, the way a pool cycles
    // an idle one out.
    for _ in 0..20 {
        let mut conn = sqlx::PgConnection::connect(&server.url())
            .await
            .expect("connect");
        conn.execute(AssertSqlSafe("SELECT 1")).await.expect("query");
        conn.close().await.expect("close");
    }

    // The server's half closes asynchronously; give it a moment before judging.
    for _ in 0..40 {
        if count(port, "CLOSE-WAIT") == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let lingering = count(port, "CLOSE-WAIT");
    let established = count(port, "ESTAB");
    assert_eq!(
        lingering, 0,
        "{lingering} socket(s) left in CLOSE-WAIT after 20 clean closes \
         ({established} still ESTAB) — the server is not reaping its half"
    );
}

/// The same, but the client vanishes without a polite close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_connection_reaps_the_server_side_socket() {
    let server = TestServer::start().await.expect("start");
    let port = server.port();

    for _ in 0..20 {
        let mut conn = sqlx::PgConnection::connect(&server.url())
            .await
            .expect("connect");
        conn.execute(AssertSqlSafe("SELECT 1")).await.expect("query");
        // Dropped, not closed: no Terminate message, just a FIN.
        drop(conn);
    }

    for _ in 0..40 {
        if count(port, "CLOSE-WAIT") == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let lingering = count(port, "CLOSE-WAIT");
    assert_eq!(
        lingering, 0,
        "{lingering} socket(s) left in CLOSE-WAIT after 20 dropped connections"
    );
}
