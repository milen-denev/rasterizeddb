use rastcp::{
    client::TcpClientBuilder,
    common::{DEFAULT_MAX_MESSAGE_SIZE, read_message, write_message},
    error::RastcpError,
    server::TcpServerBuilder,
};
use tokio::io::duplex;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

// Test custom message protocol
#[tokio::test]
async fn test_custom_protocol_handler() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server_addr = "127.0.0.1";

    // Create a server that expects JSON protocol
    let server = TcpServerBuilder::new(server_addr, 8097)
        .build()
        .await
        .unwrap();

    // Protocol handler that expects JSON and validates it
    let server_handle = tokio::spawn(async move {
        server
            .run(|data| async move {
                // Try to parse as JSON
                match serde_json::from_slice::<serde_json::Value>(&data) {
                    Ok(json) => {
                        // Check if it has a "command" field
                        if let Some(cmd) = json.get("command") {
                            if cmd == "echo" {
                                if let Some(text) = json.get("text") {
                                    return serde_json::json!({
                                        "status": "success",
                                        "result": text
                                    })
                                    .to_string()
                                    .into_bytes();
                                }
                            }
                        }

                        // Invalid command or missing fields
                        serde_json::json!({
                            "status": "error",
                            "message": "Invalid command format"
                        })
                        .to_string()
                        .into_bytes()
                    }
                    Err(_) => {
                        // Not valid JSON
                        serde_json::json!({
                            "status": "error",
                            "message": "Invalid JSON"
                        })
                        .to_string()
                        .into_bytes()
                    }
                }
            })
            .await
            .unwrap();
    });

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    // Create a client
    let mut client = TcpClientBuilder::new(&format!("{}:8097", server_addr))
        .build()
        .await
        .unwrap();

    // Send valid JSON command
    let valid_cmd = serde_json::json!({
        "command": "echo",
        "text": "Hello JSON protocol!"
    })
    .to_string()
    .into_bytes();

    let response = client.send(valid_cmd).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_eq!(response_json["status"], "success");
    assert_eq!(response_json["result"], "Hello JSON protocol!");

    // Send invalid JSON command
    let invalid_cmd = serde_json::json!({
        "command": "unknown",
    })
    .to_string()
    .into_bytes();

    let response = client.send(invalid_cmd).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_eq!(response_json["status"], "error");

    // Send completely invalid JSON
    let invalid_json = b"{not valid json".to_vec();

    let response = client.send(invalid_json).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_eq!(response_json["status"], "error");
    assert_eq!(response_json["message"], "Invalid JSON");

    tokio::time::timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

// Test for server handling clients that disconnect abruptly
#[tokio::test]
async fn test_client_disconnect_handling() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server_addr = "127.0.0.1";

    // Message tracking
    let message_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let message_count_clone = message_count.clone();

    // Create server
    let server = TcpServerBuilder::new(server_addr, 8098)
        .build()
        .await
        .unwrap();

    let server_handle = tokio::spawn(async move {
        server
            .run(move |data| {
                let count = message_count_clone.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // Slow response to ensure client can disconnect before response
                    sleep(Duration::from_millis(50)).await;
                    data
                }
            })
            .await
            .unwrap();
    });

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    // Create multiple clients and test abrupt disconnection
    for i in 0..5 {
        let mut client = TcpClientBuilder::new(&format!("{}:8098", server_addr))
            .build()
            .await
            .unwrap();

        // Send a message
        let message = format!("Message {}", i).into_bytes();
        client.send(message).await.unwrap();

        // Don't wait for more responses, just drop the client
        // This tests the server's ability to handle client disconnection
    }

    // Allow time for server to process
    sleep(Duration::from_millis(200)).await;

    // Verify server processed the messages
    assert_eq!(message_count.load(std::sync::atomic::Ordering::SeqCst), 5);

    tokio::time::timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

// Test custom low-level protocol implementation and error cases
#[tokio::test]
async fn test_custom_protocol_implementation() {
    let _ = env_logger::builder().is_test(true).try_init();

    // For this test, we'll use a simple TCP server without TLS to test the protocol logic
    let listener = TcpListener::bind("127.0.0.1:8099").await.unwrap();

    // Spawn a server that uses our message protocol directly
    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // Handle one message with our protocol
        let data = read_message(&mut socket).await.unwrap();
        assert_eq!(data, b"test message");

        // Reply
        write_message(&mut socket, b"test response").await.unwrap();

        // Now test protocol error - write invalid data
        socket
            .write_all(b"invalid protocol data without length prefix")
            .await
            .unwrap();
    });

    // Allow server to start
    sleep(Duration::from_millis(100)).await;

    // Connect as a client
    let mut stream = TcpStream::connect("127.0.0.1:8099").await.unwrap();

    // Send valid message
    write_message(&mut stream, b"test message").await.unwrap();

    // Read response
    let response = read_message(&mut stream).await.unwrap();
    assert_eq!(response, b"test response");

    // Now try to read invalid data - should fail with protocol error
    let result = read_message(&mut stream).await;
    assert!(result.is_err());

    tokio::time::timeout(Duration::from_millis(100), server_handle)
        .await
        .ok();
}

#[tokio::test]
async fn test_read_message_rejects_oversized_frame() {
    let (mut writer, mut reader) = duplex(64);
    let oversized_len = (DEFAULT_MAX_MESSAGE_SIZE as u32).saturating_add(1);

    tokio::spawn(async move {
        writer.write_all(&oversized_len.to_le_bytes()).await.unwrap();
    });

    let result = read_message(&mut reader).await;

    match result {
        Err(RastcpError::MessageTooLarge { len, max }) => {
            assert_eq!(len, oversized_len as usize);
            assert_eq!(max, DEFAULT_MAX_MESSAGE_SIZE);
        }
        other => panic!("expected oversized frame rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn test_client_send_returns_timeout_instead_of_panicking() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = TcpServerBuilder::new("127.0.0.1", 8100)
        .request_timeout(Duration::from_secs(1))
        .build()
        .await
        .unwrap();

    let server_handle = tokio::spawn(async move {
        server
            .run(|data| async move {
                sleep(Duration::from_millis(250)).await;
                data
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClientBuilder::new("127.0.0.1:8100")
        .timeout(Duration::from_millis(50))
        .build()
        .await
        .unwrap();

    let result = client.send(b"slow request".to_vec()).await;

    assert!(matches!(result, Err(RastcpError::ConnectionTimeout)));
    assert!(!client.is_connected());

    server_handle.abort();
}

#[tokio::test]
async fn test_graceful_shutdown_drains_and_frees_the_port() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = Arc::new(
        TcpServerBuilder::new("127.0.0.1", 8101)
            .shutdown_grace(Duration::from_secs(5))
            .build()
            .await
            .unwrap(),
    );

    let shutdown = server.shutdown_handle();
    let running = server.clone();

    let server_handle = tokio::spawn(async move { running.run(|data| async move { data }).await });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClientBuilder::new("127.0.0.1:8101").build().await.unwrap();
    let echoed = client.send(b"ping".to_vec()).await.unwrap();
    assert_eq!(echoed, b"ping".to_vec());

    // The connection is now idle but still open; a shutdown has to close it
    // rather than wait out the whole grace period.
    shutdown.shutdown();

    let stopped = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    assert!(
        matches!(stopped, Ok(Ok(Ok(())))),
        "server did not stop cleanly: {:?}",
        stopped.map(|joined| joined.map(|run| run.is_ok()))
    );
    assert_eq!(server.active_connections(), 0);

    // Once the listener is dropped the port takes a new bind straight away,
    // which is what a restart right after Ctrl+C does.
    drop(server);

    TcpServerBuilder::new("127.0.0.1", 8101)
        .build()
        .await
        .expect("port should be free immediately after a graceful shutdown");
}
