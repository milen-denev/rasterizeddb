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

impl From<RowError> for io::Error {
    fn from(error: RowError) -> Self {
        match error {
            // The original error carries the kind a caller may be matching on,
            // so unwrap rather than re-wrap.
            RowError::Io(error) => error,
            RowError::NotFound(msg) => io::Error::new(io::ErrorKind::NotFound, msg),
            RowError::Validation(msg)
            | RowError::InvalidData(msg)
            | RowError::InvalidType(msg)
            | RowError::InvalidLength(msg)
            | RowError::InvalidColumn(msg)
            | RowError::InvalidPosition(msg) => io::Error::new(io::ErrorKind::InvalidData, msg),
            other => io::Error::other(other.to_string()),
        }
    }
}

/// Custom Result type for row operations
pub type Result<T> = std::result::Result<T, RowError>;

/// A failure while evaluating a WHERE expression against a row.
///
/// Deliberately `Copy` and allocation-free: this is produced on the per-row
/// evaluation path, so it must not cost anything in the overwhelmingly common
/// case where nothing goes wrong.
///
/// These abort the whole query rather than skipping the offending row. Skipping
/// would hand the caller a short result set that looks like a complete one —
/// `SELECT ... WHERE salary / bonus > 100` would silently omit every employee
/// with no bonus instead of reporting that the expression is undefined for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalError {
    /// Integer or float division with a zero divisor.
    DivisionByZero,
    /// `Root` with a zero degree, i.e. an infinite exponent.
    RootByZero,
    /// An operand's byte width does not match the column type it is being
    /// evaluated as. Reading it as that type would run off the end of the slice.
    OperandSizeMismatch,
    /// An integer add/subtract/multiply left the range of its type.
    ArithmeticOverflow,
    /// The column's type has no implementation for the requested operation.
    ///
    /// Reported rather than panicked: the type comes from a client's query, so
    /// `WHERE is_active = true` against an unimplemented type must fail the
    /// query, not the worker evaluating it.
    UnsupportedType,
}

impl EvalError {
    pub const fn as_str(&self) -> &'static str {
        match self {
            EvalError::DivisionByZero => "division by zero",
            EvalError::RootByZero => "root by zero",
            EvalError::OperandSizeMismatch => {
                "operand size does not match the column type being evaluated"
            }
            EvalError::ArithmeticOverflow => "integer out of range",
            EvalError::UnsupportedType => {
                "operation is not supported for this column type"
            }
        }
    }

    /// Non-zero discriminant, so query workers can report a failure through a
    /// single shared atomic instead of a lock. 0 is reserved for "no error".
    pub const fn to_code(&self) -> u8 {
        match self {
            EvalError::DivisionByZero => 1,
            EvalError::RootByZero => 2,
            EvalError::OperandSizeMismatch => 3,
            EvalError::ArithmeticOverflow => 4,
            EvalError::UnsupportedType => 5,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(EvalError::DivisionByZero),
            2 => Some(EvalError::RootByZero),
            3 => Some(EvalError::OperandSizeMismatch),
            4 => Some(EvalError::ArithmeticOverflow),
            5 => Some(EvalError::UnsupportedType),
            _ => None,
        }
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Error for EvalError {}

impl From<EvalError> for RowError {
    fn from(error: EvalError) -> Self {
        RowError::Validation(error.as_str().to_string())
    }
}

impl From<EvalError> for io::Error {
    fn from(error: EvalError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, error.as_str())
    }
}
