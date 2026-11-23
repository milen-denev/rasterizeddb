//! Connection-scoped sessions (`TcpServer::run_with_session`).
//!
//! What has to hold: one session per connection, never shared with another
//! connection; every request of the connection sees the same one; and the
//! session ends only once the connection has ended and none of its requests
//! is still running, so whatever its end does cannot happen under one of them.

use rastcp::client::TcpClientBuilder;
use rastcp::server::{ConnectionInfo, TcpServer, TcpServerBuilder};
use rastcp::session::Principal;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// How long a `SLOW` request takes.
const SLOW_FOR: Duration = Duration::from_millis(600);
const SLOW: u8 = 1;

/// What the test sees of a session's life.
#[derive(Debug)]
enum Event {
    Opened { id: u64, multiplexed: bool },
    HandlerDone { id: u64, at: Instant },
    Dropped { id: u64, at: Instant },
}

struct Session {
    id: u64,
    events: mpsc::UnboundedSender<Event>,
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.events.send(Event::Dropped {
            id: self.id,
            at: Instant::now(),
        });
    }
}

struct Running {
    addr: String,
    server: Arc<TcpServer>,
    events: mpsc::UnboundedReceiver<Event>,
}

/// A server whose every answer is the id of the session that served it.
async fn serve(request_timeout: Duration) -> Running {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(
        TcpServerBuilder::from_listener(listener)
            .unwrap()
            .request_timeout(request_timeout)
            .build()
            .await
            .unwrap(),
    );
    let (sender, events) = mpsc::unbounded_channel();
    let next = Arc::new(AtomicU64::new(1));

    let running = server.clone();
    tokio::spawn(async move {
        running
            .run_with_session(
                (sender, next),
                |(sender, next): &(mpsc::UnboundedSender<Event>, Arc<AtomicU64>),
                 _principal: &Arc<Principal>,
                 connection: &ConnectionInfo| {
                    let id = next.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(Event::Opened {
                        id,
                        multiplexed: connection.multiplexed,
                    });
                    Session {
                        id,
                        events: sender.clone(),
                    }
                },
                |(sender, _), _principal, session: Arc<Session>, data: Vec<u8>| async move {
                    if data.first() == Some(&SLOW) {
                        sleep(SLOW_FOR).await;
                    }
                    let _ = sender.send(Event::HandlerDone {
                        id: session.id,
                        at: Instant::now(),
                    });
                    session.id.to_le_bytes().to_vec()
                },
            )
            .await
    });

    Running {
        addr,
        server,
        events,
    }
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("no session event within 10 s")
        .expect("the server stopped")
}

/// The next event that is not a handler finishing.
async fn next_lifecycle(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    loop {
        match next_event(events).await {
            Event::HandlerDone { .. } => continue,
            other => return other,
        }
    }
}

fn id_of(response: &[u8]) -> u64 {
    u64::from_le_bytes(response.try_into().expect("an 8-byte session id"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_session_per_connection_shared_by_its_requests() {
    bounded(async {
        let mut running = serve(Duration::from_secs(30)).await;

        let mut first = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();
        let mut second = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();

        let a1 = id_of(&first.send(b"x".to_vec()).await.unwrap());
        let a2 = id_of(&first.send(b"y".to_vec()).await.unwrap());
        let b1 = id_of(&second.send(b"x".to_vec()).await.unwrap());
        let a3 = id_of(&first.send(b"z".to_vec()).await.unwrap());

        assert_eq!(a1, a2, "one connection's requests share its session");
        assert_eq!(a1, a3);
        assert_ne!(a1, b1, "another connection never gets that session");

        // Opened once per connection, as plain connections.
        let mut opened = Vec::new();
        while opened.len() < 2 {
            if let Event::Opened { id, multiplexed } = next_lifecycle(&mut running.events).await {
                assert!(!multiplexed, "a rastcp/1 connection reports itself plain");
                opened.push(id);
            }
        }
        opened.sort();
        let mut expected = vec![a1, b1];
        expected.sort();
        assert_eq!(opened, expected);
        running.server.shutdown();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_ends_when_its_connection_closes() {
    bounded(async {
        let mut running = serve(Duration::from_secs(30)).await;

        let mut client = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();
        let id = id_of(&client.send(b"x".to_vec()).await.unwrap());
        assert!(matches!(next_lifecycle(&mut running.events).await, Event::Opened { id: o, .. } if o == id));

        client.close().await.unwrap();
        drop(client);

        match next_lifecycle(&mut running.events).await {
            Event::Dropped { id: dropped, .. } => assert_eq!(dropped, id),
            other => panic!("expected the session to end, got {other:?}"),
        }
        running.server.shutdown();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_connection_closed_by_the_request_timeout_ends_its_session() {
    bounded(async {
        let mut running = serve(Duration::from_millis(400)).await;

        let mut client = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();
        let id = id_of(&client.send(b"x".to_vec()).await.unwrap());
        let started = Instant::now();
        assert!(matches!(
            next_lifecycle(&mut running.events).await,
            Event::Opened { .. }
        ));

        // The client stays connected and says nothing: the server's idle
        // close is what ends the session.
        match next_lifecycle(&mut running.events).await {
            Event::Dropped { id: dropped, .. } => assert_eq!(dropped, id),
            other => panic!("expected the session to end, got {other:?}"),
        }
        assert!(started.elapsed() >= Duration::from_millis(300));
        drop(client);
        running.server.shutdown();
    })
    .await
}

/// A `rastcp/2` connection that closes leaves its running handlers to finish
/// (they are detached, not cancelled). The session must outlive them: a
/// session whose end rolls a transaction back must not do it under a
/// statement that then writes into, or after, the rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multiplexed_session_outlives_its_running_handlers() {
    bounded(async {
        let mut running = serve(Duration::from_secs(30)).await;

        let client = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(10))
            .build_multiplexed()
            .await
            .unwrap();
        let id = id_of(&client.send(b"x").await.unwrap());
        match next_lifecycle(&mut running.events).await {
            Event::Opened {
                id: opened,
                multiplexed,
            } => {
                assert_eq!(opened, id);
                assert!(
                    multiplexed,
                    "a rastcp/2 connection reports itself multiplexed"
                );
            }
            other => panic!("{other:?}"),
        }

        // A slow request in flight, then the connection closed under it.
        let slow = {
            let client = client.clone();
            tokio::spawn(async move { client.send(&[SLOW]).await })
        };
        sleep(Duration::from_millis(100)).await;
        client.close().await;
        let _ = slow.await;

        let mut handler_done = None;
        let dropped_at = loop {
            match next_event(&mut running.events).await {
                Event::HandlerDone { id: done, at } if done == id => handler_done = Some(at),
                Event::Dropped { id: dropped, at } if dropped == id => break at,
                other => panic!("unexpected {other:?}"),
            }
        };
        let handler_done =
            handler_done.expect("the slow handler ran to its end, and before the session ended");
        assert!(
            dropped_at >= handler_done,
            "the session ended before the handler still running on it"
        );
        running.server.shutdown();
    })
    .await
}
