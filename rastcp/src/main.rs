use rastcp::{client::TcpClient, server::TcpServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    tokio::spawn(async move { 
        println!("Starting server...");
        let server = TcpServer::new("127.0.0.1:8080").await.unwrap();
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
    let mut client = TcpClient::new("127.0.0.1:8080").await?;
    
    // Connect explicitly (optional, as send will connect if needed)
    client.connect().await?;
    
    // Send multiple messages over the same connection
    let response1 = client.send(b"Hello, secure world! (1)".to_vec()).await?;
    println!("Response 1: {}", String::from_utf8_lossy(&response1));
    
    let response2 = client.send(b"Hello, secure world! (2)".to_vec()).await?;
    println!("Response 2: {}", String::from_utf8_lossy(&response2));
    
    // Explicitly close the connection when done
    client.close().await?;

    Ok(())
}
