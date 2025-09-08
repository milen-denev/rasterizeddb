use std::error::Error;
use std::fmt;
use std::io;

/// Custom error type for row operations
#[derive(Debug)]
pub enum RowError {
    /// IO errors that might occur during storage operations
    Io(io::Error),
    /// Deserialization errors
    Deserialize(String),
    /// Serialization errors
    Serialize(String),
    /// Validation errors (e.g., row constraints)
    Validation(String),
    /// Row not found or deleted
    NotFound(String),
    /// Memory allocation errors
    MemoryAllocation(String),
    /// Position errors (invalid position in storage)
    InvalidPosition(String),
    /// Buffer overflow errors
    BufferOverflow(String),
    /// Missing data errors
    MissingData(String),
    /// Invalid data errors
    InvalidData(String),
    /// Invalid type errors
    InvalidType(String),
    /// Invalid length errors
    InvalidLength(String),
    /// Invalid column errors
    InvalidColumn(String),
    /// Deleted row errors
    DeletedRow(String),
    /// Saving failed errors
    SavingFailed(String),
    /// General errors with custom message
    Other(String),
}

impl fmt::Display for RowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowError::Io(err) => write!(f, "IO error: {}", err),
            RowError::Deserialize(msg) => write!(f, "Deserialization error: {}", msg),
            RowError::Serialize(msg) => write!(f, "Serialization error: {}", msg),
            RowError::Validation(msg) => write!(f, "Validation error: {}", msg),
            RowError::NotFound(msg) => write!(f, "Not found: {}", msg),
            RowError::MemoryAllocation(msg) => write!(f, "Memory allocation error: {}", msg),
            RowError::InvalidPosition(msg) => write!(f, "Invalid position: {}", msg),
            RowError::BufferOverflow(msg) => write!(f, "Buffer overflow: {}", msg),
            RowError::MissingData(msg) => write!(f, "Missing data: {}", msg),
            RowError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            RowError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
            RowError::InvalidLength(msg) => write!(f, "Invalid length: {}", msg),
            RowError::InvalidColumn(msg) => write!(f, "Invalid column: {}", msg),
            RowError::DeletedRow(msg) => write!(f, "Deleted row: {}", msg),
            RowError::SavingFailed(msg) => write!(f, "Saving failed: {}", msg),
            RowError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for RowError {}

// Implement From trait for easy conversion from IO errors
impl From<io::Error> for RowError {
    fn from(error: io::Error) -> Self {
        RowError::Io(error)
    }
}

/// Custom Result type for row operations
pub type Result<T> = std::result::Result<T, RowError>;
