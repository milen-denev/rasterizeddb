//! Requests on the wire before their answers are in (`TcpClient::pipeline`).
//!
//! What has to hold: answers come back in the order the requests went out and
//! each is its own request's; the server still handles them one at a time and
//! in order; large requests and answers in both directions do not wedge the
//! connection; a handle dropped with answers still due does not leave them to
//! be read as someone else's; and the waiting overlaps, which is the point.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rastcp::client::TcpClientBuilder;
use rastcp::error::RastcpError;
use rastcp::server::TcpServerBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test).await.expect("the test wedged")
}

async fn serve<F, Fut>(
    request_timeout: Duration,
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
        .max_message_size(64 * 1024 * 1024)
        .build()
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        let _ = server.run(handler).await;
    });
    (addr, task)
}

/// A handler that answers each request with the order it was handled in,
/// then the request itself, and checks it is never running two at once.
fn ordered(handled: Arc<AtomicU64>, running: Arc<AtomicUsize>) -> impl Fn(Vec<u8>) -> std::pin::Pin<Box<dyn Future<Output = Vec<u8>> + Send>> + Clone {
    move |data| {
        let (handled, running) = (handled.clone(), running.clone());
        Box::pin(async move {
            assert_eq!(running.fetch_add(1, Ordering::SeqCst), 0, "two requests of one connection ran at once");
            // Yields, so a second request would get its chance to start.
            tokio::time::sleep(Duration::from_millis(1)).await;
            let order = handled.fetch_add(1, Ordering::SeqCst);
            running.fetch_sub(1, Ordering::SeqCst);
            let mut answer = order.to_le_bytes().to_vec();
            answer.extend_from_slice(&data);
            answer
        })
    }
}

fn request(index: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(7).wrapping_add(index * 31)) as u8).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_come_back_in_order_and_requests_run_one_at_a_time() {
    bounded(async {
        let handled = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicUsize::new(0));
        let (addr, task) = serve(Duration::from_secs(30), ordered(handled.clone(), running)).await;
        let mut client = TcpClientBuilder::new(&addr).timeout(Duration::from_secs(10)).build().await.unwrap();

        let mut pipeline = client.pipeline().await.unwrap();
        let mut sent = 0usize;
        let mut received = 0usize;
        // A window of eight, kept full: send while there is room, then take
        // the oldest answer.
        while received < 200 {
            while sent < 200 && pipeline.in_flight() < 8 {
                pipeline.send(&request(sent, 1 + sent % 300)).await.unwrap();
                sent += 1;
            }
            let answer = pipeline.recv().await.unwrap();
            assert_eq!(&answer[..8], &(received as u64).to_le_bytes(), "answer {received} is out of order");
            assert_eq!(&answer[8..], &request(received, 1 + received % 300)[..], "answer {received} is another's");
            received += 1;
        }
        assert_eq!(pipeline.in_flight(), 0);
        drop(pipeline);

        // The connection is a connection again afterwards.
        let after = client.send(b"after".to_vec()).await.unwrap();
        assert_eq!(&after[8..], b"after");
        assert!(client.is_connected());

        task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_requests_and_answers_both_ways_do_not_wedge() {
    bounded(async {
        // 1 MiB each way, sixteen in flight: far more than the socket buffers
        // hold in either direction. A client that only wrote while it had
        // something to send would stop with the server blocked on answers.
        let (addr, task) = serve(Duration::from_secs(30), |data| async move { data }).await;
        let mut client = TcpClientBuilder::new(&addr)
            .timeout(Duration::from_secs(30))
            .max_message_size(8 * 1024 * 1024)
            .build()
            .await
            .unwrap();

        let mut pipeline = client.pipeline().await.unwrap();
        for i in 0..16 {
            pipeline.send(&request(i, 1024 * 1024)).await.unwrap();
        }
        for i in 0..16 {
            let answer = pipeline.recv().await.unwrap();
            assert!(answer == request(i, 1024 * 1024), "answer {i} is not its request's");
        }

        task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handle_dropped_with_answers_due_does_not_hand_them_to_the_next_request() {
    bounded(async {
        let (addr, task) = serve(Duration::from_secs(30), |data| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            data
        })
        .await;
        let mut client = TcpClientBuilder::new(&addr).timeout(Duration::from_secs(10)).build().await.unwrap();

        {
            let mut pipeline = client.pipeline().await.unwrap();
            pipeline.send(b"first").await.unwrap();
            pipeline.send(b"second").await.unwrap();
            // Dropped with both answers still to come.
        }
        assert!(!client.is_connected(), "a connection with answers still due was kept");

        // On a fresh connection, its own answer.
        assert_eq!(client.send(b"third".to_vec()).await.unwrap(), b"third".to_vec());

        task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_answer_times_out_and_costs_the_connection() {
    bounded(async {
        let (addr, task) = serve(Duration::from_secs(30), |data| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            data
        })
        .await;
        let mut client = TcpClientBuilder::new(&addr).timeout(Duration::from_millis(100)).build().await.unwrap();

        let mut pipeline = client.pipeline().await.unwrap();
        pipeline.send(b"slow").await.unwrap();
        let outcome = pipeline.recv().await;
        assert!(matches!(outcome, Err(RastcpError::ConnectionTimeout)), "got {outcome:?}");
        assert_eq!(pipeline.in_flight(), 0);
        drop(pipeline);
        assert!(!client.is_connected());

        // Asking for an answer to nothing is an error, not a wait.
        let mut client = TcpClientBuilder::new(&addr).timeout(Duration::from_secs(5)).build().await.unwrap();
        let mut pipeline = client.pipeline().await.unwrap();
        assert!(pipeline.recv().await.is_err());

        task.abort();
    })
    .await
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelining_works_on_a_unix_socket_too() {
    bounded(async {
        let path = std::path::PathBuf::from(format!("/tmp/rastcp-pipe-{}.sock", std::process::id()));
        let server = TcpServerBuilder::unix(&path).build().await.unwrap();
        let task = tokio::spawn(async move {
            let _ = server.run(|data| async move { data }).await;
        });
        let mut client = TcpClientBuilder::new(&format!("unix:{}", path.display())).build().await.unwrap();

        let mut pipeline = client.pipeline().await.unwrap();
        for i in 0..32 {
            pipeline.send(&request(i, 5000)).await.unwrap();
        }
        for i in 0..32 {
            assert!(pipeline.recv().await.unwrap() == request(i, 5000));
        }
        drop(pipeline);

        task.abort();
        let _ = task.await;
    })
    .await
}

/// A TCP proxy that holds every chunk for `delay` before passing it on, in
/// each direction: a long link, in-process.
async fn delaying_proxy(target: String, delay: Duration) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let target = target.clone();
            tokio::spawn(async move {
                let Ok(server) = tokio::net::TcpStream::connect(&target).await else { return };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(carry(client_read, server_write, delay));
                tokio::spawn(carry(server_read, client_write, delay));
            });
        }
    });
    addr
}

async fn carry(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let delivering = tokio::spawn(async move {
        while let Some((due, chunk)) = rx.recv().await {
            tokio::time::sleep_until(due.into()).await;
            if to.write_all(&chunk).await.is_err() {
                return;
            }
        }
    });
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let _ = tx.send((Instant::now() + delay, buf[..n].to_vec()));
            }
        }
    }
    drop(tx);
    let _ = delivering.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_round_trips_overlap() {
    bounded(async {
        // 20 ms each way: a round trip of 40 ms. Twenty requests one after
        // another take at least 800 ms; eight in flight at a time take a few
        // round trips.
        let (addr, task) = serve(Duration::from_secs(30), |data| async move { data }).await;
        let far = delaying_proxy(addr, Duration::from_millis(20)).await;
        let mut client = TcpClientBuilder::new(&far).timeout(Duration::from_secs(10)).build().await.unwrap();
        client.send(b"warm".to_vec()).await.unwrap();

        let started = Instant::now();
        for i in 0..20 {
            client.send(request(i, 100)).await.unwrap();
        }
        let one_at_a_time = started.elapsed();

        let started = Instant::now();
        let mut pipeline = client.pipeline().await.unwrap();
        let (mut sent, mut received) = (0, 0);
        while received < 20 {
            while sent < 20 && pipeline.in_flight() < 8 {
                pipeline.send(&request(sent, 100)).await.unwrap();
                sent += 1;
            }
            assert!(pipeline.recv().await.unwrap() == request(received, 100));
            received += 1;
        }
        let pipelined = started.elapsed();

        assert!(one_at_a_time >= Duration::from_millis(800), "the delay is not being applied: {one_at_a_time:?}");
        assert!(
            pipelined * 3 < one_at_a_time,
            "pipelining saved too little: {pipelined:?} against {one_at_a_time:?} one at a time"
        );

        task.abort();
    })
    .await
}
