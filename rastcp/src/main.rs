use rastcp::{client::TcpClient, server::TcpServerBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    
    env_logger::init();

    _ = compio::runtime::RuntimeBuilder::new()
        .build()
        .unwrap()
        .block_on(async_main())
        .unwrap();

    Ok(())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    // Spawn the server task and detach
    compio::runtime::spawn(async move {
        println!("Starting server...");
        let server = TcpServerBuilder::new("127.0.0.1", 8080)
            .max_connections(100)
            .session_cache_size(2048)
            .connection_backoff_ms(50)
            .handshake_timeout(std::time::Duration::from_secs(5))
            .build()
            .await
            .expect("Failed to build server");

        server
            .run(|data| {
                println!("Received data: {} bytes", data.len());
                async move { data }
            })
            .await
            .expect("Server run failed");
    }).detach();

    // Give the server a moment to start
    compio::time::sleep(std::time::Duration::from_millis(100)).await;

    println!("Starting client with insecure certificate verification...");
    let mut client = TcpClient::new("127.0.0.1:8080")
        .await
        .map_err(|e| format!("Failed to create client: {e}"))?;

    client.connect().await.map_err(|e| format!("Failed to connect to server: {e}"))?;

    let response1 = client.send(b"Hello, secure world! (1)".to_vec()).await?;
    println!("Response 1: {}", String::from_utf8_lossy(&response1));

    let response2 = client.send(b"Hello, secure world! (2)".to_vec()).await?;
    println!("Response 2: {}", String::from_utf8_lossy(&response2));

    client.close().await?;
    Ok(())
}
