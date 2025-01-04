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
}

async fn test(buf: Vec<u8>) {
    println!("Hello, world! len: {}", buf.len());
}