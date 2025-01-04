use std::{net::SocketAddr, sync::Arc};

use tokio::{io, net::UdpSocket};

use rand::prelude::*;

const CHUNK_SIZE: u16 = 1488;

pub struct Sender {
    server_addr: String,
    socket: UdpSocket
}

impl Sender {
    pub async fn new(server_addr: String) -> io::Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;

        Ok(Self {
            server_addr,
            socket
        })
    }

    pub async fn send_message(&self, message: &[u8]) -> io::Result<()> {
        let total_chunks = (message.len() + CHUNK_SIZE as usize - 1) / CHUNK_SIZE as usize;

        let mut rng = rand::thread_rng();
        let session_id = rng.gen::<u64>();
        let session_id_bytes = session_id.to_be_bytes();
        let total_parts = (total_chunks as u16).to_be_bytes();
    
        for (i, chunk) in message.chunks(CHUNK_SIZE as usize).enumerate() {
            let sequence_number = (i as u16 + 1).to_be_bytes();
    
            // Prepare the header: [sequence_number (2 bytes), total_parts (2 bytes), session_id (8 bytes)]
            let mut packet = Vec::new();
            packet.extend_from_slice(&sequence_number);
            packet.extend_from_slice(&total_parts);
            packet.extend_from_slice(&session_id_bytes);
            packet.extend_from_slice(chunk);
    
            self.socket.send_to(&packet, self.server_addr.as_str()).await?;

            // println!(
            //     "Sent chunk {}/{} to {}",
            //     i + 1,
            //     total_chunks,
            //     server_addr
            // );
        }

        let mut buf = vec![0u8; 12];
        let (len, _) = self.socket.recv_from(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..len]);

        if response == format!("ACK {}", session_id) {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Invalid acknowledgment"))
        }
    }   

    pub async fn send_message_external(message: &[u8], socket: Arc<UdpSocket>, addr: SocketAddr) -> io::Result<()> {
        let total_chunks = (message.len() + CHUNK_SIZE as usize - 1) / CHUNK_SIZE as usize;

        let mut rng = rand::thread_rng();
        let session_id = rng.gen::<u64>();
        let session_id_bytes = session_id.to_be_bytes();
        let total_parts = (total_chunks as u16).to_be_bytes();
    
        for (i, chunk) in message.chunks(CHUNK_SIZE as usize).enumerate() {
            let sequence_number = (i as u16 + 1).to_be_bytes();
    
            // Prepare the header: [sequence_number (2 bytes), total_parts (2 bytes), session_id (8 bytes)]
            let mut packet = Vec::new();
            packet.extend_from_slice(&sequence_number);
            packet.extend_from_slice(&total_parts);
            packet.extend_from_slice(&session_id_bytes);
            packet.extend_from_slice(chunk);
    
            socket.send_to(&packet, addr).await?;

            // println!(
            //     "Sent chunk {}/{} to {}",
            //     i + 1,
            //     total_chunks,
            //     server_addr
            // );
        }

        Ok(())
    }
}