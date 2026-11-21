use rastcp::{client::TcpClientBuilder, server::TcpServerBuilder};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// A loopback listener on a port of the kernel's choosing, and its address.
///
/// The server is handed the listener itself, never a port number to bind
/// again: a fixed port can be held by anything else on the machine (these
/// tests failed whenever another program had 8090 or 8095), and a port that
/// is let go can be handed to a test running in parallel before it is bound.
fn loopback() -> (std::net::TcpListener, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let addr = listener.local_addr().expect("a bound address").to_string();
    (listener, addr)
}

// Helper function to create a test server, and the address it serves
async fn create_test_server() -> (tokio::task::JoinHandle<()>, Arc<AtomicUsize>, String) {
    let message_count = Arc::new(AtomicUsize::new(0));
    let message_count_clone = message_count.clone();

    let (listener, addr) = loopback();
    let server = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .max_connections(20)
        .build()
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        server
            .run(move |data| {
                message_count_clone.fetch_add(1, Ordering::SeqCst);
                async move { data }
            })
            .await
            .unwrap();
    });

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    (handle, message_count, addr)
}

#[tokio::test]
async fn test_client_reconnection() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server_handle, message_count, server_addr) = create_test_server().await;

    // Create a client
    let mut client = TcpClientBuilder::new(&server_addr)
        .timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap();

    // Initial connection
    client.connect().await.unwrap();
    assert!(client.is_connected());

    // Send a message
    let message = b"Test message 1".to_vec();
    let response = client.send(message.clone()).await.unwrap();
    assert_eq!(response, message);

    // Force reconnection
    client.reconnect().await.unwrap();
    assert!(client.is_connected());

    // Send another message
    let message2 = b"Test message 2".to_vec();
    let response2 = client.send(message2.clone()).await.unwrap();
    assert_eq!(response2, message2);

    // Close connection
    client.close().await.unwrap();
    assert!(!client.is_connected());

    // Auto-reconnect when sending
    let message3 = b"Test message 3".to_vec();
    let response3 = client.send(message3.clone()).await.unwrap();
    assert_eq!(response3, message3);
    assert!(client.is_connected());

    // Check message count
    assert_eq!(message_count.load(Ordering::SeqCst), 3);

    timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

#[tokio::test]
async fn test_large_messages() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server_handle, _, server_addr) = create_test_server().await;

    // Create a client
    let mut client = TcpClientBuilder::new(&server_addr)
        .timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap();

    // Test with small message
    let small_msg = vec![0u8; 100]; // 100 bytes
    let small_response = client.send(small_msg.clone()).await.unwrap();
    assert_eq!(small_response.len(), small_msg.len());

    // Test with medium message
    let medium_msg = vec![1u8; 1024 * 10]; // 10 KB
    let medium_response = client.send(medium_msg.clone()).await.unwrap();
    assert_eq!(medium_response.len(), medium_msg.len());

    // Test with large message
    let large_msg = vec![2u8; 1024 * 1024]; // 1 MB
    let large_response = client.send(large_msg.clone()).await.unwrap();
    assert_eq!(large_response.len(), large_msg.len());

    timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

#[tokio::test]
async fn test_concurrent_connections() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server_handle, message_count, server_addr) = create_test_server().await;

    // Number of concurrent clients
    let client_count = 50;
    // Number of messages per client
    let messages_per_client = 10;

    let mut handles = Vec::with_capacity(client_count);

    // Start concurrent clients
    for client_id in 0..client_count {
        let server_addr = server_addr.clone();
        let handle = tokio::spawn(async move {
            // Create a client
            let mut client = TcpClientBuilder::new(&server_addr)
                .timeout(Duration::from_secs(10))
                .build()
                .await
                .unwrap();

            for msg_id in 0..messages_per_client {
                // Create a unique message for this client/message
                let message = format!("Client-{}-Message-{}", client_id, msg_id).into_bytes();
                let response = client.send(message.clone()).await.unwrap();
                assert_eq!(response, message);

                // Add some randomized delay between messages
                if msg_id % 3 == 0 {
                    sleep(Duration::from_millis(10)).await;
                }
            }

            client.close().await.unwrap();
        });

        handles.push(handle);
    }

    // Wait for all clients to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Allow time for server to process all connections
    sleep(Duration::from_millis(500)).await;

    // Check total message count
    assert_eq!(
        message_count.load(Ordering::SeqCst),
        client_count * messages_per_client
    );

    timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

#[tokio::test]
async fn test_connection_timeout() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Try to connect to a non-existent server with short timeout. The port is
    // bound but never listened on, and held for the whole test, so nothing can
    // answer on it (a fixed port such as 9999 may be another program's).
    let unheard = tokio::net::TcpSocket::new_v4().expect("a socket");
    unheard
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("a loopback port");
    let unheard_addr = unheard.local_addr().expect("a bound address").to_string();
    let result = TcpClientBuilder::new(&unheard_addr)
        .timeout(Duration::from_millis(100))
        .build()
        .await
        .unwrap()
        .connect()
        .await;

    match result {
        Err(rastcp::error::RastcpError::ConnectionTimeout) => {
            // This is the expected outcome
        }
        Err(e) => {
            // Other errors might be acceptable depending on OS (like connection refused)
            println!("Got error: {:?}", e);
        }
        Ok(_) => {
            panic!("Expected connection to timeout but it succeeded");
        }
    }
}

#[tokio::test]
async fn test_client_handler_errors() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (listener, server_addr) = loopback();

    // Create a server with an "echo uppercase" handler
    let server = TcpServerBuilder::from_listener(listener)
        .unwrap()
        .build()
        .await
        .unwrap();

    let server_handle = tokio::spawn(async move {
        server
            .run(|data| async move {
                // Convert request to string, uppercase it, and send back
                match String::from_utf8(data) {
                    Ok(s) => s.to_uppercase().into_bytes(),
                    Err(_) => b"Invalid UTF-8".to_vec(),
                }
            })
            .await
            .unwrap();
    });

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    // Create a client
    let mut client = TcpClientBuilder::new(&server_addr)
        .build()
        .await
        .unwrap();

    // Test valid UTF-8
    let message = b"hello world".to_vec();
    let response = client.send(message).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&response), "HELLO WORLD");

    // Test invalid UTF-8
    let invalid_utf8 = vec![0xFF, 0x90, 0x80];
    let response = client.send(invalid_utf8).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&response), "Invalid UTF-8");

    timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

// Simple benchmark test to measure throughput
#[tokio::test]
async fn test_throughput_benchmark() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server_handle, _, server_addr) = create_test_server().await;

    // Create a client
    let mut client = TcpClientBuilder::new(&server_addr)
        .build()
        .await
        .unwrap();

    // Message size
    let msg_size = 1024; // 1 KB
    let message = vec![42u8; msg_size];

    // Number of messages for benchmark
    let msg_count = 1000;

    // Measure time for sending messages
    let start = std::time::Instant::now();

    for _ in 0..msg_count {
        client.send(message.clone()).await.unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = (msg_count * msg_size) as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!("Throughput: {:.2} MB/s", throughput);
    println!(
        "Average latency: {:.2} ms",
        elapsed.as_millis() as f64 / msg_count as f64
    );

    // No specific assertion, just for information

    timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}
