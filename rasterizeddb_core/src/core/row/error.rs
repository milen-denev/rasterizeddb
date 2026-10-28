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
    /// The statement outlived `statement_timeout`.
    ///
    /// Not an evaluation failure like the others, but it travels the same way:
    /// the scan reports it through the shared atomic and the statement fails
    /// with it. That is the whole point — a cancelled scan must surface an
    /// error, because the alternative is returning the rows it happened to
    /// reach as though they were the complete answer.
    StatementTimeout,
    /// The client that asked for this statement is gone.
    ClientWentAway,
    /// A `CancelRequest` with this session's key asked for it to stop.
    UserCanceled,
    /// The `WHERE` clause has more comparisons than the compiled boolean
    /// program can index.
    ///
    /// Reported rather than approximated. The index used to be cast to `u16`,
    /// so a clause with more than 65,535 comparisons wrapped its indices to
    /// 0, 1, 2 … and produced a *valid-looking* program over the wrong
    /// comparisons — the predicate silently stopped discriminating and the
    /// query returned every row. A correlated `EXISTS` materialises one
    /// comparison per inner row, so any such subquery over a table above that
    /// size was quietly ignored. The width is now `u32`; this exists so that
    /// exceeding even that fails the query instead of answering it wrongly.
    PredicateTooComplex,
    /// A row's storage was handed to a writer between the scan reading its
    /// sort key and reading the rest of it.
    RowMovedUnderScan,
    /// A `SIMILAR TO` pattern the matcher cannot use. Refused when the
    /// predicate is planned; this is the safety net at evaluation time.
    InvalidPattern,
    /// Matching one value against a `SIMILAR TO` pattern would cost more than
    /// the matcher allows. PostgreSQL's `54001`.
    PatternTooComplex,
    /// An expression evaluated per row in a `WHERE` — a `CASE`, a `||`, a
    /// `%` — failed for a reason none of the variants above names.
    ExpressionFailed,
    /// A `NUMERIC(p, s)` value with more places than the engine's nine, met
    /// in `WHERE`-clause arithmetic, which runs at nine: refused rather than
    /// computed without its last digits. PostgreSQL's 0A000 by its words.
    InexactNumeric,
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
            EvalError::UnsupportedType => "operation is not supported for this column type",
            EvalError::StatementTimeout => "canceling statement due to statement timeout",
            EvalError::ClientWentAway => {
                "canceling statement because the client closed the connection"
            }
            EvalError::PredicateTooComplex => {
                "the WHERE clause has too many comparisons to evaluate"
            }
            EvalError::RowMovedUnderScan => {
                "a row was reclaimed while the query was reading it; retry the statement"
            }
            EvalError::InvalidPattern => "invalid regular expression in SIMILAR TO",
            EvalError::PatternTooComplex => crate::core::similar_to::TOO_COMPLEX,
            EvalError::UserCanceled => "canceling statement due to user request",
            EvalError::ExpressionFailed => "an expression in the WHERE clause could not be evaluated",
            EvalError::InexactNumeric => {
                "a NUMERIC value with more than 9 decimal places cannot be carried exactly by arithmetic \
                 in a WHERE clause (not supported)"
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
            EvalError::StatementTimeout => 6,
            EvalError::ClientWentAway => 7,
            EvalError::PredicateTooComplex => 8,
            EvalError::RowMovedUnderScan => 9,
            EvalError::InvalidPattern => 10,
            EvalError::PatternTooComplex => 11,
            EvalError::UserCanceled => 12,
            EvalError::ExpressionFailed => 13,
            EvalError::InexactNumeric => 14,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(EvalError::DivisionByZero),
            2 => Some(EvalError::RootByZero),
            3 => Some(EvalError::OperandSizeMismatch),
            4 => Some(EvalError::ArithmeticOverflow),
            5 => Some(EvalError::UnsupportedType),
            6 => Some(EvalError::StatementTimeout),
            7 => Some(EvalError::ClientWentAway),
            8 => Some(EvalError::PredicateTooComplex),
            9 => Some(EvalError::RowMovedUnderScan),
            10 => Some(EvalError::InvalidPattern),
            11 => Some(EvalError::PatternTooComplex),
            12 => Some(EvalError::UserCanceled),
            13 => Some(EvalError::ExpressionFailed),
            14 => Some(EvalError::InexactNumeric),
            _ => None,
        }
    }
}

impl EvalError {
    /// The variant for an expression evaluator's message, so a division by
    /// zero inside a `CASE` reports as one rather than as a generic failure.
    /// The evaluator's errors are text, and only one code crosses the shared
    /// atomic the scan workers report through.
    pub fn from_expression(message: &str) -> Self {
        let lower = message.to_ascii_lowercase();
        if lower.contains("division by zero") {
            EvalError::DivisionByZero
        } else if lower.contains("cannot carry exactly") {
            EvalError::InexactNumeric
        } else if lower.contains("out of range") || lower.contains("overflow") {
            EvalError::ArithmeticOverflow
        } else {
            EvalError::ExpressionFailed
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
