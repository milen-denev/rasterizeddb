use rastcp::{client::TcpClientBuilder, server::TcpServerBuilder};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use futures_util::lock::Mutex;
use compio::runtime::{spawn, RuntimeBuilder};
use compio::time::{sleep, timeout};

fn run_async<F>(future: F)
where
    F: Future<Output = ()>,
{
    _ = RuntimeBuilder::new()
        .build()
        .expect("failed to build compio runtime")
        .block_on(future);
}

#[test]
fn test_client_server_communication() {
    run_async(async {
        let _ = env_logger::builder().is_test(true).try_init();

        // Start a server
        let server = TcpServerBuilder::new("127.0.0.1", 8081)
            .max_connections(10)
            .build()
            .await
            .unwrap();

        let server_handle = spawn(async move {
            server
                .run(|data| async move {
                    println!(
                        "[SERVER HANDLER] Received data: {}",
                        String::from_utf8_lossy(&data)
                    );
                    // Echo with prefix
                    let mut response = b"Echo: ".to_vec();
                    response.extend_from_slice(&data);
                    println!(
                        "[SERVER HANDLER] Sending response: {}",
                        String::from_utf8_lossy(&response)
                    );
                    response
                })
                .await
                .unwrap();
        });

        // Allow server to start
        sleep(Duration::from_millis(100)).await;

        // Create a client
        let mut client = TcpClientBuilder::new("127.0.0.1:8081")
            .timeout(Duration::from_secs(5))
            .verify_certificate(false)
            .build()
            .await
            .unwrap();

        println!("[TEST] Connecting client...");
        client.connect().await.unwrap();
        println!("[TEST] Client connected. Sending message...");
        let message = b"Hello, test world!".to_vec();
        let response = client.send(message).await.unwrap();
        println!(
            "[TEST] Message sent. Got response: {}",
            String::from_utf8_lossy(&response)
        );

        assert_eq!(
            String::from_utf8_lossy(&response),
            "Echo: Hello, test world!"
        );

        // Close the client connection
        client.close().await.unwrap();

        // Cleanup - this will actually never complete since server.run is infinite,
        // but in a real scenario we'd have a server.shutdown() method
        timeout(Duration::from_millis(100), server_handle).await.ok();
    });
}

#[test]
fn test_multiple_clients() {
    run_async(async {
        let _ = env_logger::builder().is_test(true).try_init();

        // Start a server
        let server = TcpServerBuilder::new("127.0.0.1", 8082)
            .max_connections(10)
            .build()
            .await
            .unwrap();

        let message_count = Arc::new(Mutex::new(0));
        let message_count_clone = message_count.clone();

        let server_handle = spawn(async move {
            server
                .run(move |data| {
                    let message_count = message_count_clone.clone();
                    async move {
                        println!(
                            "[SERVER HANDLER] Received data: {}",
                            String::from_utf8_lossy(&data)
                        );
                        let mut count = message_count.lock().await;
                        *count += 1;
                        println!(
                            "[SERVER HANDLER] Message count incremented: {}",
                            *count
                        );
                        println!(
                            "[SERVER HANDLER] Sending response: {}",
                            String::from_utf8_lossy(&data)
                        );
                        data // Simple echo
                    }
                })
                .await
                .unwrap();
        });

        // Allow server to start
        sleep(Duration::from_millis(100)).await;

        // Create multiple clients
        let client_count = 5;
        let message_per_client = 3;

        let mut handles = vec![];

        for i in 0..client_count {
            let handle = spawn(async move {
                let mut client = TcpClientBuilder::new("127.0.0.1:8082")
                    .timeout(Duration::from_secs(5))
                    .build()
                    .await
                    .unwrap();

                println!("[TEST] Client {} connecting...", i);
                client.connect().await.unwrap();
                println!("[TEST] Client {} connected.", i);
                for j in 0..message_per_client {
                    let message = format!("Message {}-{}", i, j).into_bytes();
                    println!("[TEST] Client {} sending message {}...", i, j);
                    let response = client.send(message.clone()).await.unwrap();
                    println!("[TEST] Client {} got response for message {}.", i, j);
                    assert_eq!(response, message);
                }

                client.close().await.unwrap();
            });

            handles.push(handle);
        }

        // Wait for all clients to finish
        for handle in handles {
            handle.await.unwrap();
        }

        // Check message count
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            *message_count.lock().await,
            client_count * message_per_client
        );

        // Cleanup
        timeout(Duration::from_millis(100), server_handle).await.ok();
    });
}
