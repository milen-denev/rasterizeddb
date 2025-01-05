pub enum MessageStatus {
    NotSecured,
    RequestKey,
    Secured
}

impl MessageStatus {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => MessageStatus::NotSecured,
            1 => MessageStatus::RequestKey,
            2 => MessageStatus::Secured,
            _ => panic!("Invalid message status")
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            MessageStatus::NotSecured => 0,
            MessageStatus::RequestKey => 1,
            MessageStatus::Secured => 2
        }
    }
}