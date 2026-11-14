//! Many callers on one connection (`rastcp/2`).
//!
//! What has to hold however the requests interleave: a caller gets the
//! response to its own request or an error, one request's failure is not
//! another's, and nothing waits behind a request it has nothing to do with.

use rastcp::client::TcpClientBuilder;
use rastcp::error::RastcpError;
use rastcp::mux::MuxClient;
use rastcp::server::{TcpServer, TcpServerBuilder};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// What a request asks the test server to do before echoing it back: the
/// first byte is the instruction, the rest is payload.
const ECHO: u8 = 0;
const SLOW: u8 = 1;
const PANIC: u8 = 2;

/// How long a `SLOW` request takes.
const SLOW_FOR: Duration = Duration::from_millis(400);

async fn respond(data: Vec<u8>) -> Vec<u8> {
    match data.first().copied() {
        Some(SLOW) => sleep(SLOW_FOR).await,
        Some(PANIC) => {
            // After a wait, so the panic happens in the request's own task
            // rather than on the connection's.
            sleep(Duration::from_millis(5)).await;
            panic!("the handler was asked to panic");
        }
        _ => {}
    }
    data
}

struct Running {
    addr: String,
    server: Arc<TcpServer>,
    task: tokio::task::JoinHandle<Result<(), RastcpError>>,
}

async fn serve_with<F, Fut>(
    configure: impl FnOnce(TcpServerBuilder) -> TcpServerBuilder,
    handler: F,
) -> Running
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Vec<u8>> + Send + 'static,
{
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let builder = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .max_message_size(64 * 1024 * 1024);
    let server = Arc::new(configure(builder).build().await.unwrap());

    let running = server.clone();
    let task = tokio::spawn(async move { running.run(handler).await });

    Running { addr, server, task }
}

async fn serve() -> Running {
    serve_with(|builder| builder, respond).await
}

async fn mux(addr: &str, timeout: Duration) -> MuxClient {
    TcpClientBuilder::new(addr)
        .timeout(timeout)
        .max_message_size(64 * 1024 * 1024)
        .build_multiplexed()
        .await
        .unwrap()
}

/// A request unlike any other in a test: the instruction, then bytes that
/// depend on who is asking.
fn request(kind: u8, seed: usize, len: usize) -> Vec<u8> {
    let mut request = vec![kind];
    request.extend((0..len).map(|i| (i.wrapping_mul(17).wrapping_add(seed * 131)) as u8));
    request
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if condition() {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
    panic!("never happened: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_tasks_share_one_connection_and_each_gets_its_own_response() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;

        let mut tasks = Vec::new();
        for task in 0..64usize {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..200usize {
                    let seed = task * 10_000 + round;
                    // Sizes on both sides of every threshold in the framing,
                    // mixed, so large and small frames interleave on the wire.
                    let len = [0, 1, 8, 63, 900, 16 * 1024 - 5, 16 * 1024 + 1, 70_000][seed % 8];
                    let request = request(ECHO, seed, len);
                    let response = client.send(&request).await.unwrap();
                    assert!(
                        response == request,
                        "task {task} round {round} got another's response"
                    );
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(
            running.server.active_connections(),
            1,
            "64 tasks should be one connection"
        );
        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_request_does_not_wait_behind_a_slow_one() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;
        client.connect().await.unwrap();

        let slow_client = client.clone();
        let slow = tokio::spawn(async move {
            let request = request(SLOW, 1, 32);
            let response = slow_client.send(&request).await.unwrap();
            assert!(response == request);
            Instant::now()
        });

        // Sent after the slow one, on the same connection.
        sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        for round in 0..20 {
            let request = request(ECHO, round, 32);
            assert!(client.send(&request).await.unwrap() == request);
        }
        let fast_done = Instant::now();

        assert!(
            fast_done - started < SLOW_FOR / 2,
            "twenty fast requests took {:?} behind a slow one",
            fast_done - started
        );
        assert!(
            slow.await.unwrap() > fast_done,
            "the slow request finished first"
        );

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_that_times_out_fails_alone_and_its_late_answer_reaches_nobody() {
    bounded(async {
        // A request that outlives the client's timeout is failed here while
        // the server is still working on it, and its answer arrives later.
        // By then the client has moved on, and the danger is that the id the
        // late answer carries now names some other request, which would be
        // handed it.
        //
        // An id given back is only taken again when more requests are in
        // flight than before, so that is what happens here: once the overdue
        // requests have been failed, eight more callers start, and need eight
        // more ids. Whatever ids the client thinks are free are then in
        // constant use, each by a request that takes a while, when the late
        // answers arrive.
        const LATE: Duration = Duration::from_millis(1500);
        const TIMEOUT: Duration = Duration::from_millis(500);
        const BRIEF: Duration = Duration::from_millis(40);

        let running = serve_with(
            |builder| builder,
            |data: Vec<u8>| async move {
                sleep(if data.first() == Some(&SLOW) {
                    LATE
                } else {
                    BRIEF
                })
                .await;
                data
            },
        )
        .await;
        let client = mux(&running.addr, TIMEOUT).await;

        let began = Instant::now();
        let caller_of = |caller: usize| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut round = 0usize;
                // Across every late answer's arrival, and a little beyond.
                while began.elapsed() < LATE * 2 {
                    let request = request(ECHO, caller * 100_000 + round, 1 + round % 50);
                    let response = client
                        .send(&request)
                        .await
                        .unwrap_or_else(|e| panic!("caller {caller} request {round} failed: {e}"));
                    assert!(
                        response == request,
                        "caller {caller} request {round} was given another request's answer"
                    );
                    round += 1;
                }
                round
            })
        };

        // Traffic from the start: a client that hears nothing at all for a
        // whole timeout gives the connection up, which is another test.
        let mut chatter: Vec<_> = (0..8).map(caller_of).collect();

        // Four overdue requests, a little apart, so four late answers arrive
        // at four different moments.
        let mut overdue = Vec::new();
        for seed in 0..4usize {
            sleep(Duration::from_millis(70)).await;
            let client = client.clone();
            overdue.push(tokio::spawn(async move {
                let started = Instant::now();
                let outcome = client.send(&request(SLOW, seed, 16)).await;
                (outcome, started.elapsed())
            }));
        }

        for task in overdue {
            let (outcome, took) = task.await.unwrap();
            assert!(
                matches!(outcome, Err(RastcpError::ConnectionTimeout)),
                "got {outcome:?}"
            );
            assert!(took >= TIMEOUT, "timed out early, after {took:?}");
            assert!(took < LATE, "the timeout took {took:?}");
        }

        // All four have been failed and none has been answered.
        assert!(
            began.elapsed() < LATE,
            "the test is too slow to test anything"
        );
        chatter.extend((8..16).map(caller_of));

        for task in chatter {
            assert!(task.await.unwrap() > 20, "the other requests stalled");
        }
        assert!(
            client.is_connected(),
            "slow requests cost everyone the connection"
        );
        assert_eq!(running.server.active_connections(), 1);

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_that_goes_silent_costs_the_connection_and_the_next_request_reopens_it() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, SLOW_FOR / 4).await;

        // Nothing else in flight: not one frame comes back for a whole
        // timeout, which is what a dead peer looks like.
        let outcome = client.send(&request(SLOW, 1, 16)).await;
        assert!(
            matches!(outcome, Err(RastcpError::ConnectionTimeout)),
            "got {outcome:?}"
        );

        until("the silent connection is given up", || {
            !client.is_connected()
        })
        .await;

        let request = request(ECHO, 2, 16);
        assert!(client.send(&request).await.unwrap() == request);
        assert!(client.is_connected());

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_the_server_times_out_fails_that_request_only() {
    bounded(async {
        let running = serve_with(|builder| builder.request_timeout(SLOW_FOR / 4), respond).await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;

        let slow_client = client.clone();
        let slow = tokio::spawn(async move { slow_client.send(&request(SLOW, 1, 16)).await });

        for round in 0..50 {
            let request = request(ECHO, round, 16);
            assert!(client.send(&request).await.unwrap() == request);
            sleep(Duration::from_millis(4)).await;
        }

        let outcome = slow.await.unwrap();
        assert!(
            matches!(outcome, Err(RastcpError::HandlerTimeout)),
            "got {outcome:?}"
        );
        assert!(client.is_connected());
        assert_eq!(running.server.active_connections(), 1);

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_that_panics_fails_that_request_only() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;

        let outcome = client.send(&request(PANIC, 1, 16)).await;
        assert!(
            matches!(outcome, Err(RastcpError::ProtocolError(_))),
            "got {outcome:?}"
        );

        // The connection, and the server behind it, are still serving.
        for round in 0..20 {
            let request = request(ECHO, round, 16);
            assert!(client.send(&request).await.unwrap() == request);
        }
        assert!(client.is_connected());
        assert_eq!(running.server.active_connections(), 1);

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_fails_what_is_in_flight_and_the_next_request_reconnects() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;

        let mut in_flight = Vec::new();
        for seed in 0..8 {
            let client = client.clone();
            in_flight.push(tokio::spawn(async move {
                client.send(&request(SLOW, seed, 16)).await
            }));
        }

        until("the requests reach the server", || {
            running.server.active_connections() == 1
        })
        .await;
        sleep(Duration::from_millis(50)).await;
        client.close().await;

        for task in in_flight {
            let outcome = task.await.unwrap();
            assert!(
                matches!(outcome, Err(RastcpError::ConnectionClosed)),
                "got {outcome:?}"
            );
        }
        assert!(!client.is_connected());

        let request = request(ECHO, 99, 16);
        assert!(client.send(&request).await.unwrap() == request);

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_closes_with_the_last_handle() {
    bounded(async {
        let running = serve().await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;
        let other = client.clone();

        assert!(client.send(&request(ECHO, 1, 8)).await.is_ok());
        assert_eq!(running.server.active_connections(), 1);

        drop(client);
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            running.server.active_connections(),
            1,
            "a clone was still in use"
        );
        assert!(other.send(&request(ECHO, 2, 8)).await.is_ok());

        drop(other);
        until("the server sees the connection close", || {
            running.server.active_connections() == 0
        })
        .await;

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neither_side_runs_more_at_once_than_its_limit() {
    bounded(async {
        let running_now = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicU64::new(0));

        let (now, peak, count) = (running_now.clone(), most.clone(), handled.clone());
        let running = serve_with(
            |builder| builder.max_in_flight(3),
            move |data: Vec<u8>| {
                let (now, peak, count) = (now.clone(), peak.clone(), count.clone());
                async move {
                    let current = now.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    sleep(Duration::from_millis(20)).await;
                    now.fetch_sub(1, Ordering::SeqCst);
                    count.fetch_add(1, Ordering::SeqCst);
                    data
                }
            },
        )
        .await;

        // The client would send sixteen at once; the server takes three.
        let client = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(30))
            .max_in_flight(16)
            .build_multiplexed()
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for seed in 0..48usize {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                let request = request(ECHO, seed, 24);
                assert!(client.send(&request).await.unwrap() == request);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(handled.load(Ordering::SeqCst), 48);
        let peak = most.load(Ordering::SeqCst);
        assert!(
            peak <= 3,
            "the server ran {peak} requests of one connection at once"
        );
        assert!(peak >= 2, "the requests of one connection never overlapped");

        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_server_answers_what_is_in_flight_before_it_closes() {
    bounded(async {
        let running = serve_with(
            |builder| builder.shutdown_grace(Duration::from_secs(20)),
            respond,
        )
        .await;
        let client = mux(&running.addr, Duration::from_secs(30)).await;

        let mut in_flight = Vec::new();
        for seed in 0..6 {
            let client = client.clone();
            in_flight.push(tokio::spawn(async move {
                let request = request(SLOW, seed, 16);
                client
                    .send(&request)
                    .await
                    .map(|response| response == request)
            }));
        }

        sleep(SLOW_FOR / 4).await;
        running.server.shutdown();

        for task in in_flight {
            assert!(
                matches!(task.await.unwrap(), Ok(true)),
                "a request in flight was dropped by the stop"
            );
        }

        let stopped = running.task.await.unwrap();
        assert!(stopped.is_ok(), "the server did not stop cleanly");
        assert_eq!(running.server.active_connections(), 0);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_and_multiplexed_clients_are_served_side_by_side() {
    bounded(async {
        let running = serve().await;
        let multiplexed = mux(&running.addr, Duration::from_secs(30)).await;
        let mut plain = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(30))
            .build()
            .await
            .unwrap();

        for round in 0..50 {
            let request = request(ECHO, round, 40);
            assert!(multiplexed.send(&request).await.unwrap() == request);
            assert!(plain.send(request.clone()).await.unwrap() == request);
        }

        let batch: Vec<Vec<u8>> = (0..20).map(|i| request(ECHO, 1000 + i, 30)).collect();
        assert!(plain.send_batch(&batch).await.unwrap() == batch);

        assert_eq!(running.server.active_connections(), 2);
        running.task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_that_panics_at_once_does_not_keep_its_connection_slot() {
    bounded(async {
        // A panic before the handler's first wait unwinds the task serving
        // the connection, on either protocol. The connection is lost; the
        // count of open connections must not go on including it, or enough
        // such panics leave the server refusing everyone.
        let running = serve_with(
            |builder| builder,
            |data: Vec<u8>| async move {
                if data.first() == Some(&PANIC) {
                    panic!("the handler was asked to panic");
                }
                data
            },
        )
        .await;

        let mut plain = TcpClientBuilder::new(&running.addr)
            .timeout(Duration::from_secs(5))
            .build()
            .await
            .unwrap();
        assert!(plain.send(request(ECHO, 1, 8)).await.is_ok());
        assert_eq!(running.server.active_connections(), 1);
        assert!(plain.send(request(PANIC, 1, 8)).await.is_err());

        until("the plain connection's slot is given back", || {
            running.server.active_connections() == 0
        })
        .await;

        let multiplexed = mux(&running.addr, Duration::from_secs(5)).await;
        assert!(multiplexed.send(&request(ECHO, 2, 8)).await.is_ok());
        assert!(multiplexed.send(&request(PANIC, 2, 8)).await.is_err());

        until("the multiplexed connection's slot is given back", || {
            running.server.active_connections() == 0
        })
        .await;

        // And the server is still serving.
        assert!(multiplexed.send(&request(ECHO, 3, 8)).await.is_ok());

        running.task.abort();
    })
    .await
}
