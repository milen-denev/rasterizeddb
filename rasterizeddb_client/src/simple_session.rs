//! [`SimpleSession`]: a connection of its own for simple queries, where a
//! transaction block lasts from one call to the next.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rastcp::client::TcpClient;

use crate::simple::{SimpleAnswer, SimpleOptions, TransactionStatus, decode_status, request};
use crate::{Error, Result};

/// A connection of its own for [simple queries](crate::simple), opened by
/// [`Client::simple_session`](crate::Client::simple_session): what a console
/// gives its editor.
///
/// A [`Client`](crate::Client)'s connections are shared, so a `BEGIN` sent
/// with [`Client::simple_query`](crate::Client::simple_query) has to end in the
/// same call. Here the connection is this session's alone (`rastcp/1`, whose
/// requests the server takes one at a time and in order), so a block opened
/// by one call is still open for the next, as it is in `psql`.
///
/// Calls are taken one at a time; a second waits for the first. Outside a
/// block a connection that has gone (or has been quiet long enough that the
/// server may be closing it) is replaced before the next call, which is safe
/// because nothing is lost with it. Inside one it is not: the server rolls a
/// block back with its connection, so the call fails with
/// [`Error::TransactionLost`] and the session is back outside any block.
///
/// The server also closes a connection that has been quiet for its request
/// timeout (30 s by default, `--rdb-request-timeout`), and with it rolls back
/// the block it holds. A program that keeps a block open while its user
/// thinks calls [`ping`](Self::ping) more often than that.
pub struct SimpleSession {
    connections: Arc<crate::transaction::Connections>,
    idle_reconnect: Option<Duration>,
    link: tokio::sync::Mutex<Link>,
    /// The last `ReadyForQuery` status, readable without waiting for a call.
    status: std::sync::Mutex<TransactionStatus>,
}

struct Link {
    tcp: Option<TcpClient>,
    last_used: Instant,
    /// A call's future was dropped between its request and its answer, so
    /// the next answer on the connection is that call's.
    in_flight: bool,
}

impl SimpleSession {
    pub(crate) async fn open(
        connections: Arc<crate::transaction::Connections>,
        idle_reconnect: Option<Duration>,
    ) -> Result<Self> {
        let tcp = connections.open().await?;
        Ok(SimpleSession {
            connections,
            idle_reconnect,
            link: tokio::sync::Mutex::new(Link {
                tcp: Some(tcp),
                last_used: Instant::now(),
                in_flight: false,
            }),
            status: std::sync::Mutex::new(TransactionStatus::Idle),
        })
    }

    /// Where the session's transaction stood after its last call.
    pub fn transaction(&self) -> TransactionStatus {
        *self.status.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_transaction(&self, status: TransactionStatus) {
        *self.status.lock().unwrap_or_else(|e| e.into_inner()) = status;
    }

    /// Runs `sql` as [`Client::simple_query`](crate::Client::simple_query)
    /// does, on this session's connection.
    pub async fn query(&self, sql: &str) -> Result<SimpleAnswer> {
        self.query_with(sql, &SimpleOptions::default()).await
    }

    /// [`query`](Self::query), with a cancel token or a row cap.
    pub async fn query_with(&self, sql: &str, options: &SimpleOptions) -> Result<SimpleAnswer> {
        let request = request(sql, options)?;
        self.exchange(&request).await
    }

    /// Sends nothing to run, which keeps the connection (and a block open on
    /// it) from being closed as idle by the server.
    pub async fn ping(&self) -> Result<()> {
        self.exchange(&request("", &SimpleOptions::default())?)
            .await
            .map(|_| ())
    }

    async fn exchange(&self, request: &[u8]) -> Result<SimpleAnswer> {
        let mut link = self.link.lock().await;
        let status = self.transaction();
        if link.in_flight {
            link.in_flight = false;
            link.tcp = None;
        }
        let usable = link.tcp.as_ref().is_some_and(TcpClient::is_connected);
        if status == TransactionStatus::Idle {
            let quiet = self
                .idle_reconnect
                .is_some_and(|limit| link.last_used.elapsed() >= limit);
            if !usable || quiet {
                link.tcp = Some(self.connections.open().await?);
            }
        } else if !usable {
            link.tcp = None;
            self.set_transaction(TransactionStatus::Idle);
            return Err(Error::TransactionLost {
                commit_outcome_unknown: false,
                message: "the session's connection closed inside a transaction block, which the \
                          server rolled back"
                    .to_string(),
            });
        }

        link.in_flight = true;
        let sent = link
            .tcp
            .as_mut()
            .expect("opened above")
            .send_borrowed(request)
            .await;
        link.in_flight = false;
        match sent {
            Ok(response) => {
                link.last_used = Instant::now();
                let (result, status) = decode_status(response);
                if let Some(status) = status {
                    self.set_transaction(status);
                }
                result
            }
            Err(e) => {
                link.tcp = None;
                if status == TransactionStatus::Idle {
                    return Err(e.into());
                }
                self.set_transaction(TransactionStatus::Idle);
                Err(Error::TransactionLost {
                    // The request may have been a COMMIT the server carried
                    // out before the connection went.
                    commit_outcome_unknown: true,
                    message: format!(
                        "the session's connection failed inside a transaction block ({e}); the \
                         server rolls the block back unless the request committed it"
                    ),
                })
            }
        }
    }
}
