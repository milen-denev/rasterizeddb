use receiver::Receiver;
use tokio::net::UdpSocket;
use tokio::io;
use std::collections::HashMap;
use std::sync::Arc;

pub mod receiver;
pub mod sender;

static RECEIVER: async_lazy::Lazy<Arc<Receiver>> = async_lazy::Lazy::const_new(|| Box::pin(async {
    let receiver = Receiver::new("127.0.0.1:8080").await.unwrap();
    Arc::new(receiver)
}));


#[tokio::main]
async fn main() -> io::Result<()> {
    tokio::spawn(async { 
        let receiver = RECEIVER.force().await;
        let receiver_clone = receiver.clone(); 
        receiver_clone.start_processing_messages(test).await; 
    });

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    #[allow(unreachable_code)]


    let socket = UdpSocket::bind("127.0.0.1:8080").await?;
    println!("Server is running on 127.0.0.1:8080");

    let mut buf = vec![0u8; 1500];
    let mut message_buffers: HashMap<u64, Vec<(usize, Vec<u8>)>> = HashMap::new();

    loop {
        if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
            let received_data = &buf[..len];

            // Parse sequence number and total parts
            let header = received_data[..12].to_vec();
            let sequence_number = u16::from_be_bytes([header[0], header[1]]) as usize;
            let total_parts = u16::from_be_bytes([header[2], header[3]]) as usize;
            let session_id = u64::from_be_bytes(
                [
                    header[4], 
                    header[5], 
                    header[6],
                    header[7], 
                    header[8], 
                    header[9],
                    header[10],
                    header[11]
                ]);

            let payload = received_data[8..].to_vec();

            // println!(
            //     "Received chunk {}/{} from session {} from {}",
            //     sequence_number, total_parts, session_id, addr
            // );

            // Add the chunk to the message buffer
            let buffer = message_buffers
                .entry(session_id)
                .or_insert_with(Vec::new);

            buffer.push((sequence_number, payload));

            // If all chunks are received, reassemble the message
            if buffer.len() == total_parts {
                buffer.sort_by_key(|k| k.0);
                let message: Vec<u8> = buffer.iter().flat_map(|(_, data)| data.clone()).collect();

                println!(
                    "Reassembled message from session {}: {}",
                    session_id,
                    String::from_utf8_lossy(&message)
                );

                //GET MESSAGE
                _ = String::from_utf8_lossy(&message);

                message_buffers.remove(&session_id);

                // Acknowledge the received chunk
                let ack = format!("ACK {}", session_id);
                _ = socket.send_to(ack.as_bytes(), addr).await;
            }
        }
    }
}

async fn test(buf: Vec<u8>) {
    println!("Hello, world! len: {}", buf.len());
}