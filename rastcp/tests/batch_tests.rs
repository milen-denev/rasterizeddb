//! Several requests sent before any response is read.
//!
//! The server answers them with one read and one write where it can, and the
//! client writes and reads at once so a large batch cannot wedge. What must
//! not change is what a caller can observe: every request is handled once, in
//! the order given, and its response comes back in that position.

use rastcp::client::{TcpClient, TcpClientBuilder};
use rastcp::error::RastcpError;
use rastcp::server::TcpServerBuilder;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;

/// Nothing here should take seconds. A test that does has wedged, and should
/// fail rather than hold the run.
const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

async fn serve<F, Fut>(
    request_timeout: Duration,
    max_message_size: usize,
    handler: F,
) -> (String, tokio::task::JoinHandle<()>)
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Vec<u8>> + Send + 'static,
{
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .request_timeout(request_timeout)
        .max_message_size(max_message_size)
        .build()
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        server.run(handler).await.unwrap();
    });

    (addr, handle)
}

async fn client(addr: &str, timeout: Duration) -> TcpClient {
    TcpClientBuilder::new(addr)
        .timeout(timeout)
        .max_message_size(64 * 1024 * 1024)
        .build()
        .await
        .unwrap()
}

/// A request whose bytes depend on its position in the batch, so a response
/// returned in the wrong slot does not compare equal.
fn request(index: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(13).wrapping_add(index * 101)) as u8)
        .collect()
}

/// A handler that stamps each request with the order it was handled in.
fn stamping(counter: Arc<AtomicU64>) -> impl Fn(Vec<u8>) -> std::future::Ready<Vec<u8>> + Clone {
    move |data| {
        let order = counter.fetch_add(1, Ordering::SeqCst);
        let mut response = order.to_le_bytes().to_vec();
        response.extend_from_slice(&data);
        std::future::ready(response)
    }
}

#[tokio::test]
async fn a_batch_is_answered_in_order_whatever_the_sizes() {
    bounded(async {
        let handled = Arc::new(AtomicU64::new(0));
        let (addr, server) = serve(
            Duration::from_secs(30),
            64 * 1024 * 1024,
            stamping(handled.clone()),
        )
        .await;
        let mut client = client(&addr, Duration::from_secs(30)).await;

        // Empty, tiny, either side of the size at which a frame stops being
        // copied behind its neighbours, and larger than the read-ahead buffer
        // — next to each other, so every boundary between the paths is
        // crossed in both directions.
        let sizes = [
            0,
            1,
            3,
            4,
            64,
            16 * 1024 - 8,
            16 * 1024,
            16 * 1024 + 1,
            5,
            200_000,
            0,
            7,
            70_000,
            2,
        ];
        let requests: Vec<Vec<u8>> = sizes
            .iter()
            .enumerate()
            .map(|(i, len)| request(i, *len))
            .collect();

        let responses = client.send_batch(&requests).await.unwrap();

        assert_eq!(responses.len(), requests.len());
        for (i, (request, response)) in requests.iter().zip(&responses).enumerate() {
            assert_eq!(
                &response[..8],
                &(i as u64).to_le_bytes(),
                "request {i} was handled out of order"
            );
            assert_eq!(
                &response[8..],
                &request[..],
                "response {i} is not for request {i}"
            );
        }
        assert_eq!(handled.load(Ordering::SeqCst), requests.len() as u64);

        // The connection is still a connection: a plain request follows a
        // batch, and a batch follows a plain request.
        let single = client.send(b"after".to_vec()).await.unwrap();
        assert_eq!(&single[8..], b"after");
        let again = client
            .send_batch(&[b"x".to_vec(), b"y".to_vec()])
            .await
            .unwrap();
        assert_eq!(&again[0][8..], b"x");
        assert_eq!(&again[1][8..], b"y");
        assert!(client.is_connected());

        server.abort();
    })
    .await
}

#[tokio::test]
async fn an_empty_batch_sends_nothing() {
    bounded(async {
        let handled = Arc::new(AtomicU64::new(0));
        let (addr, server) =
            serve(Duration::from_secs(30), 1 << 20, stamping(handled.clone())).await;
        let mut client = client(&addr, Duration::from_secs(30)).await;

        let none: [Vec<u8>; 0] = [];
        assert!(client.send_batch(&none).await.unwrap().is_empty());
        assert_eq!(handled.load(Ordering::SeqCst), 0);

        server.abort();
    })
    .await
}

#[tokio::test]
async fn a_batch_far_larger_than_any_buffer_does_not_wedge() {
    bounded(async {
        // 48 MiB of requests and 48 MiB of responses through socket buffers a
        // fraction of that. A client that wrote everything before reading
        // anything would stop here with both ends waiting to write.
        let (addr, server) =
            serve(Duration::from_secs(30), 1 << 20, |data| async move { data }).await;
        let mut client = client(&addr, Duration::from_secs(30)).await;

        let requests: Vec<Vec<u8>> = (0..6000).map(|i| request(i, 8 * 1024)).collect();
        let responses = client.send_batch(&requests).await.unwrap();

        assert_eq!(responses.len(), requests.len());
        assert!(
            requests == responses,
            "a response did not match its request"
        );

        server.abort();
    })
    .await
}

#[tokio::test]
async fn a_batch_of_slow_requests_is_still_answered_in_order() {
    bounded(async {
        // Handlers that have to wait take the other path through the server:
        // replies leave as each next handler blocks, not all at the end.
        let handled = Arc::new(AtomicU64::new(0));
        let counter = handled.clone();
        let (addr, server) = serve(Duration::from_secs(30), 1 << 20, move |data: Vec<u8>| {
            let counter = counter.clone();
            async move {
                // The later a request is in the batch, the sooner it would
                // finish if the server ran them side by side.
                let wait = 40u64.saturating_sub(data[0] as u64 * 4);
                sleep(Duration::from_millis(wait)).await;
                let order = counter.fetch_add(1, Ordering::SeqCst);
                let mut response = order.to_le_bytes().to_vec();
                response.extend_from_slice(&data);
                response
            }
        })
        .await;
        let mut client = client(&addr, Duration::from_secs(30)).await;

        let requests: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 16]).collect();
        let responses = client.send_batch(&requests).await.unwrap();

        for (i, (request, response)) in requests.iter().zip(&responses).enumerate() {
            assert_eq!(
                &response[..8],
                &(i as u64).to_le_bytes(),
                "request {i} was handled out of order"
            );
            assert_eq!(&response[8..], &request[..]);
        }

        server.abort();
    })
    .await
}

#[tokio::test]
async fn the_timeout_of_a_batch_is_per_response_not_for_the_whole() {
    bounded(async {
        let (addr, server) = serve(Duration::from_secs(30), 1 << 20, |data| async move {
            sleep(Duration::from_millis(100)).await;
            data
        })
        .await;

        // A second of work in all, against a timeout of 400 ms: each response
        // arrives well inside the timeout of the one before.
        let mut patient = client(&addr, Duration::from_millis(400)).await;
        let requests: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 8]).collect();
        let responses = patient.send_batch(&requests).await.unwrap();
        assert!(requests == responses);

        // And it is still a timeout: one response slower than it ends the
        // batch and the connection.
        let mut hasty = client(&addr, Duration::from_millis(30)).await;
        let result = hasty.send_batch(&requests).await;
        assert!(
            matches!(result, Err(RastcpError::ConnectionTimeout)),
            "got {result:?}"
        );
        assert!(!hasty.is_connected());

        server.abort();
    })
    .await
}

#[tokio::test]
async fn a_request_the_server_refuses_ends_the_batch_after_the_ones_before_it() {
    bounded(async {
        let handled = Arc::new(AtomicU64::new(0));
        let (addr, server) = serve(Duration::from_secs(30), 1000, stamping(handled.clone())).await;
        let mut client = client(&addr, Duration::from_secs(5)).await;

        // The third is over the server's limit of 1000 bytes.
        let requests = vec![vec![1u8; 10], vec![2u8; 10], vec![3u8; 1001], vec![4u8; 10]];
        let result = client.send_batch(&requests).await;

        assert!(
            result.is_err(),
            "a batch with a refused request was reported as answered"
        );
        assert!(!client.is_connected());

        // The two before it were handled, the refused one and the one after
        // it were not: the server stops at the frame it cannot read.
        for _ in 0..200 {
            if handled.load(Ordering::SeqCst) >= 2 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        sleep(Duration::from_millis(50)).await;
        assert_eq!(handled.load(Ordering::SeqCst), 2);

        server.abort();
    })
    .await
}

#[tokio::test]
async fn many_connections_batching_at_once_keep_their_own_responses() {
    bounded(async {
        let (addr, server) =
            serve(Duration::from_secs(30), 1 << 20, |data| async move { data }).await;

        let mut tasks = Vec::new();
        for connection in 0..16usize {
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut client = client(&addr, Duration::from_secs(30)).await;
                for round in 0..20usize {
                    let requests: Vec<Vec<u8>> = (0..50)
                        .map(|i| {
                            request(
                                connection * 1_000_000 + round * 1000 + i,
                                1 + (i * 37) % 300,
                            )
                        })
                        .collect();
                    let responses = client.send_batch(&requests).await.unwrap();
                    assert!(
                        requests == responses,
                        "connection {connection} round {round}"
                    );
                }
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        server.abort();
    })
    .await
}

#[tokio::test]
async fn a_stopping_server_finishes_the_request_in_hand_and_starts_no_other() {
    bounded(async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = Arc::new(
            TcpServerBuilder::from_listener(listener)
                .unwrap()
                .shutdown_grace(Duration::from_secs(10))
                .build()
                .await
                .unwrap(),
        );

        let started = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let (starts, finishes) = (started.clone(), finished.clone());
        let running = server.clone();
        let serving = tokio::spawn(async move {
            running
                .run(move |data| {
                    let (starts, finishes) = (starts.clone(), finishes.clone());
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(200)).await;
                        finishes.fetch_add(1, Ordering::SeqCst);
                        data
                    }
                })
                .await
        });

        // Five requests arrive together; the stop lands while the first is
        // being handled and the other four are read but not begun.
        let batch = tokio::spawn(async move {
            let mut client = client(&addr, Duration::from_secs(10)).await;
            let requests: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 8]).collect();
            client.send_batch(&requests).await
        });

        while started.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(2)).await;
        }
        server.shutdown();

        let stopped = serving.await.unwrap();
        assert!(stopped.is_ok(), "the server did not stop cleanly");

        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "the request in hand was not finished"
        );
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "a stopping server began another request"
        );
        assert!(
            batch.await.unwrap().is_err(),
            "a batch cut short was reported as answered"
        );
    })
    .await
}
