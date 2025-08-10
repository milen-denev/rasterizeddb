use std::env;

use rastcp::{client::TcpClient, server::TcpServerBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe { env::set_var("RUST_LOG", "debug"); }

    env_logger::init();
    
    tokio::spawn(async move { 
        println!("Starting server...");
        
        // Use the builder with improved settings
        let server = TcpServerBuilder::new("127.0.0.1", 8080)
            .max_connections(100) // Set an appropriate limit
            .session_cache_size(2048) // Increase session cache size
            .connection_backoff_ms(50) // Reduce backoff time for testing
            .handshake_timeout(std::time::Duration::from_secs(5))
            .build()
            .await
            .unwrap();
            
        server.run(|data| {
            println!("Received data: {} bytes", data.len());
            async move {
                // Echo the data back
                data
            }
        }).await.unwrap(); 
    });

    println!("Starting client with insecure certificate verification...");
    // Create a client that accepts any server certificate
    let mut client = TcpClient::new("127.0.0.1:8080").await.expect("Failed to create client");
    
    // Connect explicitly (optional, as send will connect if needed)
    client.connect().await.expect("Failed to connect to server");
    
    // Send multiple messages over the same connection
    let response1 = client.send(b"Hello, secure world! (1)".to_vec()).await?;
    println!("Response 1: {}", String::from_utf8_lossy(&response1));
    
    let response2 = client.send(b"Hello, secure world! (2)".to_vec()).await?;
    println!("Response 2: {}", String::from_utf8_lossy(&response2));
    
    // Explicitly close the connection when done
    client.close().await?;

    Ok(())
}
