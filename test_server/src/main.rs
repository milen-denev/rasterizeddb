use tokio::net::UdpSocket;
use tokio::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Bind the UDP socket to an address and port
    let socket = UdpSocket::bind("127.0.0.1:8080").await?;
    println!("Server is running on 127.0.0.1:8080");

    let mut buf = vec![0u8; 1024];

    loop {
        // Receive data from a client
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let received_data = String::from_utf8_lossy(&buf[..len]);
        println!("Received '{}' from {}", received_data, addr);

        // Process the received data and create a response
        let response = match received_data.trim() {
            "hello" => "Hello, client!",
            "ping" => "pong",
            _ => "Unknown command",
        };

        // Send the response back to the client
        socket.send_to(response.as_bytes(), &addr).await?;
        println!("Sent '{}' to {}", response, addr);
    }
}