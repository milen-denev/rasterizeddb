use std::{net::SocketAddr, sync::Arc};

use tokio::{io, net::UdpSocket, sync::RwLock};

use rand::prelude::*;

use crate::{message_status::MessageStatus, MESSAGE_SIZE};

// Session Id (8 bytes)
// Sequence Number (2 bytes)
// Total Parts (2 bytes)
// Protocol Status (1 byte) 
// Compress (1 byte)

pub struct Sender {
    server_addr: String,
    socket: UdpSocket,
    _secure: bool,
    compress: bool,
    message_status: Arc<RwLock<MessageStatus>>,
    _public_key: Option<String>,
    _private_key: Option<String>
}

impl Sender {
    pub async fn new(server_addr: String, secure: bool, compress: bool) -> io::Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;

        Ok(Self {
            server_addr,
            socket,
            _secure: secure,
            compress: compress,
            message_status: Arc::new(RwLock::new(MessageStatus::NotSecured)),
            _public_key: None,
            _private_key: None
        })
    }

    pub async fn send_message(&self, message: &[u8]) -> io::Result<()> {
        let total_chunks = (message.len() + MESSAGE_SIZE as usize - 1) / MESSAGE_SIZE as usize;

        let session_id = rand::thread_rng().gen_range(0..u64::MAX);
        let session_id_bytes = session_id.to_be_bytes();
        let total_parts = (total_chunks as u16).to_be_bytes();
        let compressed_bytes = [self.compress as u8];
        let message_status = [self.message_status.read().await.to_u8()];

        for (i, chunk) in message.chunks(MESSAGE_SIZE as usize).enumerate() {
            let sequence_number = (i as u16 + 1).to_be_bytes();
    
            let mut packet = Vec::new();

            packet.extend_from_slice(&sequence_number);
            packet.extend_from_slice(&total_parts);
            packet.extend_from_slice(&session_id_bytes);

            packet.extend_from_slice(&message_status);
            packet.extend_from_slice(&compressed_bytes);

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

    pub async fn send_message_external(
        message: &[u8], 
        socket: Arc<UdpSocket>, 
        addr: SocketAddr,
        compress: bool,
        _secure: bool,
        _public_key: Option<&String>) -> io::Result<()> {
        let total_chunks = (message.len() + MESSAGE_SIZE as usize - 1) / MESSAGE_SIZE as usize;

        let mut rng = rand::thread_rng();
        let session_id = rng.gen::<u64>();
        let session_id_bytes = session_id.to_be_bytes();
        let total_parts = (total_chunks as u16).to_be_bytes();
        let compress_bytes = [compress as u8];
        let message_status = [MessageStatus::NotSecured.to_u8()];

        for (i, chunk) in message.chunks(MESSAGE_SIZE as usize).enumerate() {
            let sequence_number = (i as u16 + 1).to_be_bytes();
    
            // Prepare the header: [sequence_number (2 bytes), total_parts (2 bytes), session_id (8 bytes)]
            let mut packet = Vec::new();
            packet.extend_from_slice(&sequence_number);
            packet.extend_from_slice(&total_parts);
            packet.extend_from_slice(&session_id_bytes);
            packet.extend_from_slice(&message_status);
            packet.extend_from_slice(&compress_bytes);
            packet.extend_from_slice(chunk);
    
            socket.send_to(&packet, addr).await?;
        }

        Ok(())
    }
}
