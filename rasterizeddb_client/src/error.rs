use std::fmt;

use rastcp::error::RastcpError;

/// Everything a call can fail with.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The connection failed: it could not be opened, it closed, or the answer
    /// did not come within the timeout. The statement may or may not have
    /// run; nothing is sent again.
    Transport(RastcpError),
    /// The server ran the statement, or tried to, and refused it. The text is
    /// the server's own message.
    Server(String),
    /// The server's answer could not be read: an unknown result tag, or row
    /// bytes whose lengths do not add up.
    Protocol(String),
    /// A statement's parameters could not be rendered: a placeholder with no
    /// value, or a value no literal can carry (text holding a NUL).
    Param(String),
    /// A column could not be read as the type asked for.
    Column { index: usize, message: String },
    /// Refused before anything was sent: a statement the native protocol
    /// cannot run as written (a second statement in one text, transaction
    /// control), or an operation the model or argument does not allow.
    Unsupported(String),
    /// A statement expected to return exactly one row, or at most one,
    /// returned some other number of rows.
    RowCount { expected: &'static str, got: usize },
}

impl Error {
    pub(crate) fn column(index: usize, message: impl Into<String>) -> Self {
        Error::Column {
            index,
            message: message.into(),
        }
    }

    /// Whether the connection failed, as opposed to the server answering with
    /// an error. Only then can the statement's fate be unknown.
    pub fn is_transport(&self) -> bool {
        matches!(self, Error::Transport(_))
    }

    /// The server's message, when the server refused the statement.
    pub fn server_message(&self) -> Option<&str> {
        match self {
            Error::Server(message) => Some(message),
            _ => None,
        }
    }

    /// Whether the server refused a write for breaking a unique or primary
    /// key. The native protocol carries no SQLSTATE, so this reads the
    /// server's message.
    pub fn is_unique_violation(&self) -> bool {
        self.server_message().is_some_and(|m| {
            let m = m.to_ascii_lowercase();
            m.contains("duplicate") && (m.contains("unique") || m.contains("primary key"))
        })
    }

    /// Whether a `fetch_one` found no row.
    pub fn is_no_rows(&self) -> bool {
        matches!(self, Error::RowCount { got: 0, .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "transport: {e}"),
            Error::Server(message) => write!(f, "server: {message}"),
            Error::Protocol(message) => write!(f, "protocol: {message}"),
            Error::Param(message) => write!(f, "parameter: {message}"),
            Error::Column { index, message } => write!(f, "column {index}: {message}"),
            Error::Unsupported(message) => write!(f, "unsupported: {message}"),
            Error::RowCount { expected, got } => {
                write!(f, "expected {expected} row(s), the statement returned {got}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(e) => Some(e),
            _ => None,
        }
    }
}

impl From<RastcpError> for Error {
    fn from(e: RastcpError) -> Self {
        Error::Transport(e)
    }
}
