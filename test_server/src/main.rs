use test_server::{receiver::Receiver, sender::Sender};
use tokio::io;
use std::sync::Arc;

static RECEIVER: async_lazy::Lazy<Arc<Receiver>> = async_lazy::Lazy::const_new(|| Box::pin(async {
    let receiver = Receiver::new("127.0.0.1:8080", false, false).await.unwrap();
    Arc::new(receiver)
}));

static SENDER: async_lazy::Lazy<Arc<Sender>> = async_lazy::Lazy::const_new(|| Box::pin(async {
    let sender = Sender::new("127.0.0.1:8080".to_string(), false, false).await.unwrap();
    Arc::new(sender)
}));

#[tokio::main]
async fn main() -> io::Result<()> {
    
    tokio::spawn(async { 
        let receiver = RECEIVER.force().await;
        let receiver_clone = receiver.clone(); 
        receiver_clone.start_processing_messages(test).await; 
        let sender = SENDER.force().await;
        _ = sender.send_message(b"").await;
    });

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn test(buf: Vec<u8>) {
    //println!("Hello, world! len: {}", buf.len());
}