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
    Column {
        /// The column's position, from 0.
        index: usize,
        /// What was wrong with it.
        message: String,
    },
    /// Refused before anything was sent: a statement the native protocol
    /// cannot run as written (a second statement in one text, transaction
    /// control), or an operation the model or argument does not allow.
    Unsupported(String),
    /// A statement expected to return exactly one row, or at most one,
    /// returned some other number of rows.
    RowCount {
        /// How many were expected: `"exactly 1"` or `"at most 1"`.
        expected: &'static str,
        /// How many there were.
        got: usize,
    },
    /// A [`Transaction`](crate::Transaction)'s connection failed, was closed
    /// by the server (it closes a connection idle for 30 s), or was left
    /// unusable by a statement whose future was dropped before its answer
    /// came. The transaction is over and nothing is sent again: the server
    /// rolls a transaction back when its connection ends, so it did not
    /// commit, unless `commit_outcome_unknown` says the failure came during
    /// COMMIT, after which it may or may not have.
    TransactionLost {
        /// The connection failed while COMMIT was in flight: the server may
        /// have committed before it went, and nothing on this side can tell.
        commit_outcome_unknown: bool,
        /// What happened.
        message: String,
    },
    /// A write lost a race it was told to detect: an ORM write of a model
    /// with a `#[rdb(version)]` column found the row at another version
    /// than the one it was read at, or an `update_many` found a row gone.
    /// Nothing was written. Read the rows again and retry, or give up.
    Conflict(String),
    /// A model's [`ModelHooks::validate`](crate::orm::ModelHooks::validate)
    /// refused the row: nothing was sent. The text names the table and says
    /// what was wrong.
    Invalid(String),
}

impl Error {
    pub(crate) fn column(index: usize, message: impl Into<String>) -> Self {
        Error::Column {
            index,
            message: message.into(),
        }
    }

    /// Whether the connection failed, as opposed to the server answering with
    /// an error. Only then can the statement's fate be unknown. A
    /// transaction whose connection failed ([`Error::TransactionLost`]) is
    /// one too.
    pub fn is_transport(&self) -> bool {
        matches!(self, Error::Transport(_) | Error::TransactionLost { .. })
    }

    /// Whether a transaction's connection was lost ([`Error::TransactionLost`]).
    pub fn is_transaction_lost(&self) -> bool {
        matches!(self, Error::TransactionLost { .. })
    }

    /// Whether a COMMIT's connection failed before its answer came, so the
    /// transaction may or may not have committed. Never retried by
    /// [`Client::transaction`](crate::Client::transaction).
    pub fn is_commit_outcome_unknown(&self) -> bool {
        matches!(
            self,
            Error::TransactionLost {
                commit_outcome_unknown: true,
                ..
            }
        )
    }

    /// Whether the server abandoned the statement at its request timeout
    /// (`--rdb-request-timeout`, 30 s by default). Whether a write had taken
    /// effect is unknown, as for any [`Error::Transport`].
    pub fn is_server_timeout(&self) -> bool {
        matches!(self, Error::Transport(RastcpError::HandlerTimeout))
    }

    /// Whether [`connect`](crate::Client::connect) failed because it named a
    /// database ([`Options::database`](crate::Options::database)) and the
    /// server may predate database selection over the native protocol. Such
    /// a server serves only its default database there; a caller that can
    /// live with that may connect again naming none, and check which
    /// database it got (`SELECT current_database()`).
    pub fn may_predate_database_selection(&self) -> bool {
        match self {
            Error::Transport(RastcpError::AuthenticationError(message)) => {
                message.contains(crate::login::PREDATES_DATABASE_SELECTION)
            }
            _ => false,
        }
    }

    /// The SQLSTATE the server gave, when it gave one.
    ///
    /// Statements run inside a transaction are answered by the server's
    /// transaction machinery, which ends each error message with
    /// `(SQLSTATE xxxxx)`; that code is returned. A statement outside a
    /// transaction carries no code (the native protocol has no field for
    /// it), and this is `None`.
    pub fn sqlstate(&self) -> Option<&str> {
        let message = self.server_message()?;
        let rest = message.strip_suffix(')')?;
        let at = rest.rfind("(SQLSTATE ")?;
        let code = &rest[at + "(SQLSTATE ".len()..];
        (code.len() == 5 && code.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(code)
    }

    /// Whether the server refused a statement for a conflict with a
    /// concurrent transaction (`40001`, serialization_failure): the whole
    /// transaction may succeed if run again.
    pub fn is_serialization_failure(&self) -> bool {
        self.sqlstate() == Some("40001")
    }

    /// Whether the server chose this transaction as a deadlock's victim
    /// (`40P01`): the whole transaction may succeed if run again.
    pub fn is_deadlock(&self) -> bool {
        self.sqlstate() == Some("40P01")
    }

    /// Whether running the whole transaction again may succeed: a
    /// serialization failure or a deadlock. What
    /// [`Client::transaction`](crate::Client::transaction) retries on.
    pub fn is_retryable(&self) -> bool {
        self.is_serialization_failure() || self.is_deadlock()
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

    /// Whether a versioned write found the row changed: [`Error::Conflict`].
    pub fn is_conflict(&self) -> bool {
        matches!(self, Error::Conflict(_))
    }

    /// Whether a model's validation refused the row: [`Error::Invalid`].
    pub fn is_invalid(&self) -> bool {
        matches!(self, Error::Invalid(_))
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
            Error::TransactionLost {
                commit_outcome_unknown,
                message,
            } => {
                if *commit_outcome_unknown {
                    write!(
                        f,
                        "transaction lost during COMMIT, which may or may not have committed: {message}"
                    )
                } else {
                    write!(f, "transaction lost (the server rolled it back): {message}")
                }
            }
            Error::Conflict(message) => write!(f, "conflict: {message}"),
            Error::Invalid(message) => write!(f, "invalid: {message}"),
            Error::RowCount { expected, got } => {
                write!(
                    f,
                    "expected {expected} row(s), the statement returned {got}"
                )
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
