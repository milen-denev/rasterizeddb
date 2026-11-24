//! `TcpServerBuilder::handler_timeout`: how long the server waits for a
//! handler, apart from how long a connection may sit idle.
//!
//! Unset, a handler has the request timeout, and on a plain (`rastcp/1`)
//! connection one that runs past it costs the connection: a plain frame has
//! no tag to say "timed out" with. With `None` the server waits for every
//! answer, so a slow request is answered and its connection goes on, while
//! an idle connection is still closed at the request timeout.

use rastcp::client::TcpClientBuilder;
use rastcp::server::{TcpServer, TcpServerBuilder};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

const TEST_LIMIT: Duration = Duration::from_secs(60);

/// The request timeout every server here has: also its idle limit.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(400);

/// How long a request starting with [`SLOW`] takes: well past
/// [`REQUEST_TIMEOUT`].
const SLOW_FOR: Duration = Duration::from_millis(1200);
const SLOW: u8 = 1;

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// An echo server, slow for a request starting with [`SLOW`]. `handler`
/// is what `handler_timeout` is given; `None` here leaves it unset.
async fn serve(handler: Option<Option<Duration>>) -> (String, Arc<TcpServer>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut builder = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .request_timeout(REQUEST_TIMEOUT);
    if let Some(limit) = handler {
        builder = builder.handler_timeout(limit);
    }
    let server = Arc::new(builder.build().await.unwrap());
    let running = server.clone();
    tokio::spawn(async move {
        running
            .run(|data: Vec<u8>| async move {
                if data.first() == Some(&SLOW) {
                    tokio::time::sleep(SLOW_FOR).await;
                }
                data
            })
            .await
    });
    (addr, server)
}

fn client(addr: &str) -> TcpClientBuilder {
    TcpClientBuilder::new(addr)
        .verify_certificate(false)
        .timeout(Duration::from_secs(10))
}

#[tokio::test]
async fn the_handler_timeout_defaults_to_the_request_timeout() {
    bounded(async {
        let (_, server) = serve(None).await;
        assert_eq!(server.request_timeout(), REQUEST_TIMEOUT);
        assert_eq!(server.handler_timeout(), Some(REQUEST_TIMEOUT));

        let (_, unbounded) = serve(Some(None)).await;
        assert_eq!(unbounded.handler_timeout(), None);
        let (_, own) = serve(Some(Some(Duration::from_secs(3)))).await;
        assert_eq!(own.handler_timeout(), Some(Duration::from_secs(3)));
    })
    .await
}

/// Unchanged by default: a plain connection's slow handler is dropped at
/// the request timeout, and the connection with it.
#[tokio::test]
async fn by_default_a_slow_handler_costs_a_plain_connection() {
    bounded(async {
        let (addr, _server) = serve(None).await;
        let mut plain = client(&addr).build().await.unwrap();
        plain.connect().await.unwrap();
        let sent = plain.send(vec![SLOW, 7]).await;
        assert!(sent.is_err(), "the slow request should have cost the connection, got {sent:?}");
    })
    .await
}

/// With no handler timeout, a plain connection's slow request is answered,
/// and the connection serves the next one.
#[tokio::test]
async fn without_a_handler_timeout_a_plain_connection_outlives_a_slow_request() {
    bounded(async {
        let (addr, _server) = serve(Some(None)).await;
        let mut plain = client(&addr).build().await.unwrap();
        plain.connect().await.unwrap();

        let started = Instant::now();
        let answer = plain.send(vec![SLOW, 7]).await.expect("the slow request is answered");
        assert_eq!(answer, vec![SLOW, 7]);
        assert!(started.elapsed() >= SLOW_FOR, "answered too soon to have been slow");

        assert_eq!(plain.send(vec![9]).await.expect("the connection goes on"), vec![9]);
        assert!(plain.is_connected());

        // The idle limit is still the request timeout.
        tokio::time::sleep(REQUEST_TIMEOUT * 3).await;
        assert!(
            plain.send(vec![3]).await.is_err(),
            "an idle connection should still be closed at the request timeout"
        );
    })
    .await
}

/// The same on a multiplexed connection: the slow request is answered rather
/// than timed out, and a quick one sent meanwhile is answered first.
#[tokio::test]
async fn without_a_handler_timeout_a_multiplexed_request_is_answered() {
    bounded(async {
        let (addr, _server) = serve(Some(None)).await;
        let mux = client(&addr).build_multiplexed().await.unwrap();
        mux.connect().await.unwrap();

        let slow = {
            let mux = mux.clone();
            tokio::spawn(async move { mux.send(&[SLOW, 1]).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(mux.send(&[2]).await.expect("a quick request"), vec![2]);
        let slow = slow.await.unwrap().expect("the slow request is answered, not timed out");
        assert_eq!(slow, vec![SLOW, 1]);
    })
    .await
}

/// And with the default, a multiplexed request past the request timeout is
/// answered as timed out, the connection kept.
#[tokio::test]
async fn by_default_a_slow_multiplexed_request_is_answered_as_timed_out() {
    bounded(async {
        let (addr, _server) = serve(None).await;
        let mux = client(&addr).build_multiplexed().await.unwrap();
        mux.connect().await.unwrap();
        let slow = mux.send(&[SLOW, 1]).await;
        assert!(
            matches!(slow, Err(rastcp::error::RastcpError::HandlerTimeout)),
            "got {slow:?}"
        );
        assert_eq!(mux.send(&[4]).await.expect("the connection goes on"), vec![4]);
    })
    .await
}
