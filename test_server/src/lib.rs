pub mod receiver;
pub mod sender;
pub mod message_status;

// Session Id (8 bytes)
// Sequence Number (2 bytes)
// Total Parts (2 bytes)
// Protocol Status (1 byte) 
// Compress (1 byte)
pub const MESSAGE_SIZE: u16 = 1486;
pub const HEADER_SIZE: usize = 1500 - MESSAGE_SIZE as usize;