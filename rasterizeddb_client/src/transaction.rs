//! [`Transaction`]: statements that commit or roll back together.
//!
//! See the [transactions chapter](crate::guide::transactions) of the guide.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rastcp::client::{TcpClient, TcpClientBuilder};
use rastcp::session::ClientAuthenticator;

use crate::client::decode_response;
use crate::executor::Executor;
use crate::row::Rows;
use crate::{Error, Result};

/// How long the server lets a native connection sit idle before it closes it
/// (rastcp's request timeout), and with it rolls back the transaction it
/// holds.
pub const SERVER_IDLE_CLOSE: Duration = Duration::from_secs(30);

/// How long a finished transaction's connection is kept for the next one: well
/// inside [`SERVER_IDLE_CLOSE`], so a kept connection is never one the server
/// is closing.
const SPARE_FOR: Duration = Duration::from_secs(20);

/// How many finished transactions' connections are kept for the next ones.
const SPARES_KEPT: usize = 8;

/// [`TransactionOptions::retries`]' default.
pub const DEFAULT_TRANSACTION_RETRIES: u32 = 2;

/// A transaction's isolation level.
///
/// The server has two behaviours: `ReadCommitted` takes a fresh snapshot per
/// statement and waits for a concurrent writer of the same row, then goes on
/// with what it committed; `RepeatableRead` reads one snapshot for the whole
/// transaction, and a write to a row another transaction changed after that
/// snapshot fails with `40001` ([`Error::is_serialization_failure`]).
/// `Serializable` is accepted and runs as `RepeatableRead`: write skew
/// between two transactions is not detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// The server's default.
    ReadCommitted,
    /// One snapshot for the transaction.
    RepeatableRead,
    /// Accepted; runs as [`Isolation::RepeatableRead`].
    Serializable,
}

impl Isolation {
    fn sql(self) -> &'static str {
        match self {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }
}

/// How a transaction is opened ([`Client::begin_with`](crate::Client::begin_with),
/// [`Client::transaction_with`](crate::Client::transaction_with)).
///
/// ```
/// use rasterizeddb_client::{Isolation, TransactionOptions};
/// let options = TransactionOptions {
///     isolation: Some(Isolation::RepeatableRead),
///     ..TransactionOptions::default()
/// };
/// # let _ = options;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOptions {
    /// The isolation level; `None` takes the server's default (read
    /// committed).
    pub isolation: Option<Isolation>,
    /// `BEGIN READ ONLY`: a write in the transaction fails with `25006`.
    pub read_only: bool,
    /// How many times [`Client::transaction_with`](crate::Client::transaction_with)
    /// runs its closure again after a serialization failure or a deadlock
    /// ([`Error::is_retryable`]), each time in a new transaction. Default
    /// [`DEFAULT_TRANSACTION_RETRIES`]. Not used by `begin_with`, whose
    /// caller decides.
    pub retries: u32,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        TransactionOptions {
            isolation: None,
            read_only: false,
            retries: DEFAULT_TRANSACTION_RETRIES,
        }
    }
}

impl TransactionOptions {
    fn begin_statement(&self) -> String {
        let mut sql = String::from("BEGIN");
        if let Some(isolation) = self.isolation {
            sql.push_str(" ISOLATION LEVEL ");
            sql.push_str(isolation.sql());
        }
        if self.read_only {
            sql.push_str(" READ ONLY");
        }
        sql
    }
}

/// What a client needs to open a transaction's connection.
pub(crate) struct Dial {
    pub(crate) addr: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) max_message_size: usize,
    pub(crate) authenticator: Option<Arc<dyn ClientAuthenticator>>,
    pub(crate) trust: crate::Trust,
}

/// A finished transaction's connection, kept for the next.
struct Spare {
    tcp: TcpClient,
    since: Instant,
}

/// The connections transactions run on: dialled for one, and kept after it
/// for the next when it ended cleanly.
pub(crate) struct Connections {
    dial: Dial,
    spares: std::sync::Mutex<Vec<Spare>>,
}

impl Connections {
    pub(crate) fn new(dial: Dial) -> Arc<Self> {
        Arc::new(Connections {
            dial,
            spares: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// A plain (`rastcp/1`) connection of its own: the server handles its
    /// requests one at a time and in order, which is what lets it hold a
    /// transaction block. Never reopened behind the caller's back.
    pub(crate) async fn open(&self) -> Result<TcpClient> {
        let mut builder =
            crate::client::with_trust(TcpClientBuilder::new(&self.dial.addr), &self.dial.trust)
                .max_message_size(self.dial.max_message_size);
        builder = match self.dial.timeout {
            Some(timeout) => builder.timeout(timeout),
            None => builder.no_timeout(),
        };
        if let Some(authenticator) = &self.dial.authenticator {
            builder = builder.authenticator(authenticator.clone());
        }
        let mut tcp = builder.build().await?;
        tcp.connect().await?;
        Ok(tcp)
    }

    /// A kept connection quiet for less than [`SPARE_FOR`], if there is one.
    fn spare(&self) -> Option<TcpClient> {
        let mut spares = self.spares.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(spare) = spares.pop() {
            if spare.since.elapsed() < SPARE_FOR && spare.tcp.is_connected() {
                return Some(spare.tcp);
            }
        }
        None
    }

    /// Closes the kept connections.
    pub(crate) fn forget_spares(&self) {
        self.spares
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    fn keep(&self, tcp: TcpClient) {
        if !tcp.is_connected() {
            return;
        }
        let mut spares = self.spares.lock().unwrap_or_else(|e| e.into_inner());
        spares.retain(|s| s.since.elapsed() < SPARE_FOR);
        if spares.len() < SPARES_KEPT {
            spares.push(Spare {
                tcp,
                since: Instant::now(),
            });
        }
    }

    /// Opens a transaction: `BEGIN` on a kept connection or a new one.
    pub(crate) async fn begin(
        self: &Arc<Self>,
        options: &TransactionOptions,
    ) -> Result<Transaction> {
        let begin = options.begin_statement();
        if let Some(tcp) = self.spare() {
            let transaction = Transaction::new(tcp, self.clone());
            match transaction.exchange(&begin, Step::Begin).await {
                Ok(_) => return Ok(transaction),
                // The kept connection had gone. Nothing was open on it (a
                // BEGIN whose answer was lost opened a block the server rolls
                // back with the connection), so a new one is tried.
                Err(e) if e.is_transaction_lost() => {}
                Err(e) => return Err(e),
            }
        }
        let transaction = Transaction::new(self.open().await?, self.clone());
        transaction.exchange(&begin, Step::Begin).await?;
        Ok(transaction)
    }
}

/// Which statement an exchange carries, for what its failure means.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    Begin,
    Statement,
    Commit,
    Rollback,
}

/// The transaction's connection, and why it can no longer be used.
struct State {
    /// `None` once the transaction is over or its connection is lost.
    tcp: Option<TcpClient>,
    /// Why `tcp` is gone, for every later call.
    lost: Option<String>,
    /// When the last answer came.
    last_used: Instant,
    /// A statement was sent and its answer not read: its future was dropped
    /// part-way. The connection's next answer would be that one's.
    in_flight: bool,
}

/// A transaction: statements that commit together or not at all.
///
/// Opened by [`Client::begin`](crate::Client::begin) or
/// [`Client::begin_with`](crate::Client::begin_with), on a connection used by
/// nothing else, which is never reopened behind its back. Statements are sent
/// through [`Executor`] (`tx.run`, `tx.query`, `tx.exec`, `tx.fetch_all`, …),
/// so anything written against `E: Executor` runs inside it. They are sent
/// one at a time, in order, each after the answer to the one before; calls
/// from several tasks on one transaction queue behind each other.
///
/// It ends with [`commit`](Self::commit) or [`rollback`](Self::rollback).
/// **Dropped without either, it rolls back**: its connection is closed, and
/// the server rolls back a transaction whose connection ends.
///
/// # When the connection fails
///
/// A transaction lives and dies with its connection. A transport failure
/// (the server gone, a timeout, the server closing a connection idle for
/// 30 s) ends it, and every later call fails with
/// [`Error::TransactionLost`]: nothing is resent, and no statement of it
/// ever runs on another connection, outside the transaction. The server
/// rolled it back, except when the failure came during COMMIT, when whether
/// it committed is unknown ([`Error::is_commit_outcome_unknown`]).
///
/// **The idle limit.** The server closes a native connection that has been
/// idle for 30 s ([`SERVER_IDLE_CLOSE`]), and rolls back the transaction on
/// it. Nothing keeps a transaction's connection alive on its own: a
/// transaction forgotten by its owner must not hold its locks for ever. A
/// statement sent after 30 s of quiet fails with [`Error::TransactionLost`]
/// without being sent.
///
/// # Errors inside it
///
/// After a statement fails, the server refuses every later one (SQLSTATE
/// `25P02`) until the transaction ends or rolls back to a savepoint taken
/// before the failure; COMMIT then rolls it back and fails. A transaction's
/// errors carry their SQLSTATE ([`Error::sqlstate`]).
pub struct Transaction {
    state: tokio::sync::Mutex<State>,
    connections: Arc<Connections>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction").finish_non_exhaustive()
    }
}

impl Transaction {
    fn new(tcp: TcpClient, connections: Arc<Connections>) -> Self {
        Transaction {
            state: tokio::sync::Mutex::new(State {
                tcp: Some(tcp),
                lost: None,
                last_used: Instant::now(),
                in_flight: false,
            }),
            connections,
        }
    }

    /// Sends one statement on the transaction's connection and reads its
    /// answer.
    async fn exchange(&self, sql: &str, step: Step) -> Result<Rows> {
        let mut state = self.state.lock().await;
        let commit = step == Step::Commit;
        let lost = |message: String| Error::TransactionLost {
            commit_outcome_unknown: false,
            message,
        };

        if let Some(why) = &state.lost {
            return Err(lost(why.clone()));
        }
        if state.in_flight {
            // The connection's next answer belongs to a statement nobody is
            // waiting for: it cannot be used, and the transaction with it.
            let why = "a statement's future was dropped before its answer came, so the \
                       connection cannot be read in step; the server rolls the transaction back"
                .to_string();
            state.tcp = None;
            state.lost = Some(why.clone());
            return Err(lost(why));
        }
        let idle = state.last_used.elapsed();
        if step != Step::Begin && idle >= SERVER_IDLE_CLOSE {
            let why = format!(
                "the transaction was idle for {:.1} s, and the server closes a connection idle for \
                 {} s, rolling back its transaction",
                idle.as_secs_f64(),
                SERVER_IDLE_CLOSE.as_secs()
            );
            state.tcp = None;
            state.lost = Some(why.clone());
            return Err(lost(why));
        }
        let Some(tcp) = state.tcp.as_mut() else {
            return Err(lost("the transaction has ended".to_string()));
        };
        if !tcp.is_connected() {
            // A send would dial a new connection: one without the block.
            let why = "the transaction's connection is closed".to_string();
            state.tcp = None;
            state.lost = Some(why.clone());
            return Err(lost(why));
        }

        state.in_flight = true;
        let sent = state
            .tcp
            .as_mut()
            .expect("checked above")
            .send_borrowed(sql.as_bytes())
            .await;
        state.in_flight = false;
        match sent {
            Ok(response) => {
                state.last_used = Instant::now();
                decode_response(response)
            }
            Err(e) => {
                let mut why = format!("its connection failed: {e}");
                if idle >= SERVER_IDLE_CLOSE - Duration::from_secs(5) {
                    why.push_str(&format!(
                        " (it had been idle for {:.1} s; the server closes a connection idle for {} s)",
                        idle.as_secs_f64(),
                        SERVER_IDLE_CLOSE.as_secs()
                    ));
                }
                state.tcp = None;
                state.lost = Some(why.clone());
                Err(Error::TransactionLost {
                    commit_outcome_unknown: commit,
                    message: why,
                })
            }
        }
    }

    /// Ends the transaction with `sql`; on a clean answer its connection is
    /// kept for the next transaction.
    async fn end(self, sql: &str, step: Step) -> Result<()> {
        let result = self.exchange(sql, step).await;
        let tcp = self.state.lock().await.tcp.take();
        match (&result, tcp) {
            (Ok(_), Some(tcp)) => self.connections.keep(tcp),
            // A server error ends the block too, but the connection is not
            // worth keeping on a path that rare.
            _ => {}
        }
        result.map(|_| ())
    }

    /// Commits the transaction.
    ///
    /// Fails if a statement of it had failed (the server then rolls it back:
    /// SQLSTATE `25P02`), or the commit could not be made durable. If the
    /// connection fails before the answer comes, the outcome is unknown:
    /// [`Error::is_commit_outcome_unknown`].
    pub async fn commit(self) -> Result<()> {
        self.end("COMMIT", Step::Commit).await
    }

    /// Rolls the transaction back.
    pub async fn rollback(self) -> Result<()> {
        self.end("ROLLBACK", Step::Rollback).await
    }

    /// `SAVEPOINT name`: a point [`rollback_to`](Self::rollback_to) can
    /// return to without ending the transaction. A name may be reused; the
    /// newest shadows the older.
    pub async fn savepoint(&self, name: &str) -> Result<()> {
        check_savepoint_name(name)?;
        self.exchange(&format!("SAVEPOINT {name}"), Step::Statement)
            .await
            .map(|_| ())
    }

    /// `ROLLBACK TO SAVEPOINT name`: undoes everything since the savepoint,
    /// which stays, and clears a failure that came after it, so the
    /// transaction can go on.
    pub async fn rollback_to(&self, name: &str) -> Result<()> {
        check_savepoint_name(name)?;
        self.exchange(&format!("ROLLBACK TO SAVEPOINT {name}"), Step::Statement)
            .await
            .map(|_| ())
    }

    /// `RELEASE SAVEPOINT name`: keeps what was done since the savepoint and
    /// forgets the savepoint (and any taken after it).
    pub async fn release(&self, name: &str) -> Result<()> {
        check_savepoint_name(name)?;
        self.exchange(&format!("RELEASE SAVEPOINT {name}"), Step::Statement)
            .await
            .map(|_| ())
    }

    /// Whether the transaction can still be used: its connection has not
    /// failed. A transaction whose statement failed is still usable (for
    /// ROLLBACK, or ROLLBACK TO a savepoint).
    pub async fn is_usable(&self) -> bool {
        let state = self.state.lock().await;
        state.lost.is_none()
            && !state.in_flight
            && state.tcp.as_ref().is_some_and(|t| t.is_connected())
    }
}

impl Executor for Transaction {
    fn send_checked(&self, sql: &str) -> impl Future<Output = Result<Rows>> + Send {
        self.exchange(sql, Step::Statement)
    }
}

/// A savepoint name goes into the statement as written, so only a plain
/// identifier is accepted.
fn check_savepoint_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if first_ok && name.len() <= 63 && chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(Error::Param(format!(
            "{name:?} is not a usable savepoint name: use letters, digits and '_', starting with a \
             letter or '_' (at most 63)"
        )))
    }
}

/// The future a [`Client::transaction`](crate::Client::transaction) closure
/// returns: `Box::pin(async move { … })`.
pub type TransactionFuture<'t, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 't>>;

/// Runs `body` in a transaction, committing on `Ok` and rolling back on `Err`,
/// and running it again in a new transaction after a serialization failure
/// or a deadlock, up to `options.retries` times.
pub(crate) async fn run<T, F>(
    connections: &Arc<Connections>,
    options: TransactionOptions,
    mut body: F,
) -> Result<T>
where
    T: Send,
    F: for<'t> FnMut(&'t Transaction) -> TransactionFuture<'t, T> + Send,
{
    let mut attempt: u32 = 0;
    loop {
        let transaction = connections.begin(&options).await?;
        let outcome = body(&transaction).await;
        let failure = match outcome {
            Ok(value) => match transaction.commit().await {
                Ok(()) => return Ok(value),
                Err(e) => e,
            },
            Err(e) => {
                // Rolled back whatever the rollback says: a lost connection
                // rolls it back too.
                let _ = transaction.rollback().await;
                e
            }
        };
        if failure.is_retryable() && attempt < options.retries {
            attempt += 1;
            tokio::time::sleep(backoff(attempt)).await;
            continue;
        }
        return Err(failure);
    }
}

/// A short, growing, jittered pause before attempt `attempt` runs again, so
/// two transactions that conflicted do not meet again in step.
fn backoff(attempt: u32) -> Duration {
    let base = 5u64 << attempt.min(6);
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 % (base + 1))
        .unwrap_or(0);
    Duration::from_millis(base + jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_statements() {
        assert_eq!(TransactionOptions::default().begin_statement(), "BEGIN");
        assert_eq!(
            TransactionOptions {
                isolation: Some(Isolation::RepeatableRead),
                read_only: true,
                ..TransactionOptions::default()
            }
            .begin_statement(),
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
        );
        assert_eq!(
            TransactionOptions {
                isolation: Some(Isolation::Serializable),
                ..TransactionOptions::default()
            }
            .begin_statement(),
            "BEGIN ISOLATION LEVEL SERIALIZABLE"
        );
    }

    #[test]
    fn savepoint_names_are_plain_identifiers() {
        for ok in ["a", "_x", "sp_1", "A1"] {
            assert!(check_savepoint_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "1a", "a b", "a;b", "\"q\"", "a-b", &"x".repeat(64)] {
            assert!(check_savepoint_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn retries_default_to_a_few() {
        assert_eq!(
            TransactionOptions::default().retries,
            DEFAULT_TRANSACTION_RETRIES
        );
        assert!(backoff(1) >= Duration::from_millis(10));
        assert!(backoff(30) <= Duration::from_millis(640));
    }
}
