//! A connection the server is about to close for idleness is reopened before
//! a statement goes out on it, never under a statement in flight.
//!
//! Against an in-process rastcp server whose request timeout (the idle limit)
//! is short, so the race happens in seconds rather than at the real 30 s.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rastcp::server::TcpServerBuilder;
use rasterizeddb_client::{Client, Options};

/// The setup request the client sends on every connection it opens, asking
/// for notices (`notice::NOTICES_REQUEST`): not a statement.
const NOTICES_SETUP: &[u8] = b"\x01NOTICES;";

/// A server that answers every request with an acknowledgement after
/// `delay`, counts the statements it was asked (not a connection's setup
/// request, which every open and reopen sends once), and closes connections
/// idle for `idle_limit`.
async fn server(idle_limit: Duration, delay: Duration) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = asked.clone();
    let server = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .request_timeout(idle_limit)
        .build()
        .await
        .unwrap();
    tokio::spawn(async move {
        server
            .run(move |request| {
                let counter = counter.clone();
                async move {
                    if request != NOTICES_SETUP {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    tokio::time::sleep(delay).await;
                    vec![0]
                }
            })
            .await
            .unwrap();
    });
    (format!("127.0.0.1:{port}"), asked)
}

fn options(idle_reconnect: Option<Duration>) -> Options {
    Options {
        connections: 1,
        timeout: Some(Duration::from_secs(10)),
        idle_reconnect,
        ..Options::default()
    }
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(120), f)
            .await
            .expect("the test did not finish within 120 s")
    }
}

/// Quiet spells straddling the server's 1 s limit, closer and closer to it:
/// with the reconnect at half the limit, no statement is sent on a dying
/// connection, every one is answered, and each reaches the server once.
#[tokio::test(flavor = "multi_thread")]
async fn statements_after_a_quiet_spell_at_the_servers_limit_are_answered() {
    bounded(async {
        let limit = Duration::from_millis(1000);
        let (addr, asked) = server(limit, Duration::ZERO).await;
        let client = Client::connect(&addr, options(Some(limit / 2)))
            .await
            .unwrap();
        let mut sent = 0;
        for quiet_ms in [600u64, 900, 980, 995, 1000, 1005, 1010, 1020, 1100, 1500] {
            client
                .run("SELECT 1")
                .await
                .expect("before the quiet spell");
            sent += 1;
            tokio::time::sleep(Duration::from_millis(quiet_ms)).await;
            let after = client.run("SELECT 1").await;
            sent += 1;
            assert!(after.is_ok(), "after {quiet_ms} ms quiet: {after:?}");
        }
        assert_eq!(
            asked.load(Ordering::SeqCst),
            sent,
            "each statement reached the server once"
        );
    })
    .await
}

/// A statement in flight past the reconnect limit is not cut off by another
/// statement arriving on the same connection: in-flight work is not
/// idleness.
#[tokio::test(flavor = "multi_thread")]
async fn a_statement_in_flight_is_never_reopened_under() {
    bounded(async {
        let (addr, asked) = server(Duration::from_secs(5), Duration::from_millis(800)).await;
        let client = Arc::new(
            Client::connect(&addr, options(Some(Duration::from_millis(100))))
                .await
                .unwrap(),
        );
        // Quiet past the limit, so the next send would reopen if it could.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let slow = {
            let client = client.clone();
            tokio::spawn(async move { client.run("SELECT 1").await })
        };
        // While the slow one is in flight, past the limit again.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let second = client.run("SELECT 2").await;
        assert!(second.is_ok(), "{second:?}");
        assert!(
            slow.await.unwrap().is_ok(),
            "the statement in flight survived"
        );
        assert_eq!(asked.load(Ordering::SeqCst), 2);
    })
    .await
}

/// With the reconnect turned off, statements still work across quiet spells
/// longer than the limit (the client reopens a connection it finds closed);
/// what the reconnect removes is only the instant of the close itself.
#[tokio::test(flavor = "multi_thread")]
async fn without_the_reconnect_a_closed_connection_is_still_reopened() {
    bounded(async {
        let (addr, _) = server(Duration::from_millis(300), Duration::ZERO).await;
        let client = Client::connect(&addr, options(None)).await.unwrap();
        client.run("SELECT 1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(client.run("SELECT 1").await.is_ok());
    })
    .await
}
