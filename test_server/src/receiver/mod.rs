use std::{future::Future, sync::Arc};

use dashmap::DashMap;
use tokio::{io, net::UdpSocket};

#[derive(Clone)]
pub struct Receiver {
    receiver: Arc<UdpSocket>,
    message_buffers: Arc<DashMap<u64, Vec<(usize, Vec<u8>)>>>
}

impl Receiver {
    pub async fn new(local_addr: &str) -> io::Result<Self> {
        let socket = UdpSocket::bind(local_addr).await?;

        Ok(Self {
            receiver: Arc::new(socket),
            message_buffers: Arc::new(DashMap::new())
        })
    }

    pub async fn start_processing_messages<F, Fut>(&self, function: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send {
        let receiver = self.receiver.clone();
        let message_buffers = self.message_buffers.clone();
        let mut buf = vec![0u8; 1500];
        
        loop {
            if let Ok((len, addr)) = receiver.recv_from(&mut buf).await {
                let received_data = &buf[..len];

                // Parse sequence number and total parts
                let header = &received_data[..12];
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

                println!(
                    "Received chunk {}/{} from session {} from {}",
                    sequence_number, total_parts, session_id, addr
                );

                // Add the chunk to the message buffer
                let mut buffer = message_buffers
                    .entry(session_id.clone())
                    .or_insert_with(Vec::new);

                buffer.push((sequence_number, payload));

                // If all chunks are received, reassemble the message
                if sequence_number == total_parts {
                    buffer.sort_by_key(|k| k.0);
                    let message: Vec<u8> = buffer.iter().flat_map(|(_, data)| data.clone()).collect();
                    
                    drop(buffer);

                    println!(
                        "Reassembled message from session {}: {}",
                        session_id,
                        String::from_utf8_lossy(&message)
                    );

                    //GET MESSAGE
                    //_ = String::from_utf8_lossy(&message);
                    function(message).await;

                    message_buffers.remove(&session_id);

                    // Acknowledge the received chunk
                    let ack = format!("ACK {}", session_id);
                    _ = receiver.send_to(ack.as_bytes(), addr).await;
                }
            }
        }
    }
}