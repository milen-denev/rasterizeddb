//! Transaction blocks over the native protocol, run by this listener's own
//! session machinery.
//!
//! A native connection is one [`NativeSession`] (rastcp's
//! `run_with_session`). Outside a block it is not consulted: a statement
//! takes the native protocol's autocommit path exactly as before. `BEGIN` (and
//! the rest of transaction control), and every statement while a block is
//! open, run here instead: through a [`SessionState`] and
//! [`execute_recorded`], the path a PostgreSQL client's simple query takes.
//! So a block over native is the block pgwire runs, with nothing written
//! twice: the aborted state and its 25P02, savepoints, isolation levels,
//! DDL that rolls back, deadlock and lock-timeout codes, and the commit's
//! durability steps (`commit_block`).
//!
//! What the PostgreSQL messages carry is translated back into the native
//! answer: the engine's own rows where the engine produced them
//! (`SessionState::native_capture`), text rows otherwise (`SHOW`), and an
//! error as the message followed by ` (SQLSTATE xxxxx)`, since the native
//! protocol has no field for the code and a client deciding whether to retry
//! needs it.
//!
//! **One request at a time.** A block belongs to a connection, so only a
//! connection that carries one caller may hold one: `rastcp/1`, whose
//! requests the server handles strictly in order. A `rastcp/2` connection is
//! shared by unrelated callers, and a block on it would take in whichever of
//! their statements came next, so transaction control is refused there. The
//! session's state is also only ever taken with `try_lock`: a second request
//! finding it held is refused, never queued into the block.
//!
//! **Settings live as long as the block.** The session's state is made at
//! `BEGIN` and let go when the block closes. Outside a block the native path
//! reads no session setting, so a `SET` made inside one cannot outlive it and
//! decide a later transaction on the same (pooled) connection.
//!
//! **The end of the connection rolls the block back**, as the end of a
//! PostgreSQL session does: when the session is dropped, which rastcp does
//! once the connection has ended and none of its requests is still running.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// What every refusal of this module ends with, so a client can read the code.
fn native_error(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut answer = Vec::with_capacity(1 + message.len() + 18);
    answer.push(3u8);
    answer.extend_from_slice(message.as_bytes());
    answer.extend_from_slice(b" (SQLSTATE ");
    answer.extend_from_slice(sqlstate.as_bytes());
    answer.push(b')');
    answer
}

/// `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`,
/// `SAVEPOINT`, `RELEASE`: what opens, closes or changes a block, and so is
/// handled by the session even outside one.
pub(crate) fn is_transaction_control(query: &str) -> bool {
    let first = query
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    [
        "begin",
        "start",
        "commit",
        "end",
        "rollback",
        "abort",
        "savepoint",
        "release",
    ]
    .iter()
    .any(|w| first.eq_ignore_ascii_case(w))
}

/// One native-protocol connection's session.
pub(crate) struct NativeSession {
    database: Arc<Database>,
    /// Who is connected, as pgwire's gates read it.
    user: String,
    superuser: bool,
    authenticated: bool,
    /// The connection is `rastcp/2`, which may not hold a block.
    multiplexed: bool,
    /// Whether a block is open: read without the lock, so a statement
    /// outside one pays an atomic load and nothing else.
    in_block: AtomicBool,
    /// The connection's block was ended by the server, not by the client
    /// (an internal error): every later request is refused, so a client
    /// that still believes it is inside the block never has a statement
    /// autocommit.
    poisoned: AtomicBool,
    /// The block's session, made at `BEGIN` and let go when the block
    /// closes. Only ever taken with `try_lock`.
    state: tokio::sync::Mutex<Option<SessionState>>,
    /// The one-database catalogue a block's statements resolve against,
    /// made the first time a block opens.
    catalog: std::sync::OnceLock<Arc<DatabaseCatalog>>,
}

impl NativeSession {
    pub(crate) fn new(
        database: Arc<Database>,
        user: &str,
        superuser: bool,
        authenticated: bool,
        multiplexed: bool,
    ) -> Self {
        Self {
            database,
            user: user.to_string(),
            superuser,
            authenticated,
            multiplexed,
            in_block: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            state: tokio::sync::Mutex::new(None),
            catalog: std::sync::OnceLock::new(),
        }
    }

    /// Whether `query` is this session's to run rather than the native
    /// autocommit path's: a block is open, or the statement is transaction
    /// control, or the session refuses everything.
    pub(crate) fn takes(&self, query: &str) -> bool {
        self.in_block.load(Ordering::Acquire)
            || self.poisoned.load(Ordering::Acquire)
            || is_transaction_control(query)
    }

    /// Runs one statement (already known to be one) of this connection.
    pub(crate) async fn run(&self, query: &str) -> Vec<u8> {
        if self.poisoned.load(Ordering::Acquire) {
            return native_error(
                "08006",
                "this connection's transaction was rolled back by the server after an internal error; \
                 open a new connection",
            );
        }
        if self.multiplexed {
            return native_error(
                "0A000",
                "transaction control needs a connection of its own: a multiplexed (rastcp/2) connection \
                 is shared by many callers, and a transaction on it would take in their statements too. \
                 Open a rastcp/1 connection for the transaction (rasterizeddb_client: Client::begin)",
            );
        }
        let Ok(mut slot) = self.state.try_lock() else {
            return native_error(
                "08P01",
                "another request on this connection is still running inside its transaction: a \
                 transaction's statements must be sent one at a time, each after the answer to the last",
            );
        };

        let state = slot.get_or_insert_with(|| self.new_state());
        let answer = self.run_in(state, query).await;

        let open = state.transaction.is_some();
        if !open {
            // The block's settings end with it (see the module's notes).
            *slot = None;
        }
        self.in_block.store(open, Ordering::Release);
        answer
    }

    /// One autocommit statement through pgwire's whole path, in a session
    /// made for it alone: for a statement that needs what the native path
    /// does not have (a correlated subquery's `LATERAL` join, run in the
    /// statement transaction pgwire opens around it). Nothing of the session
    /// outlives the statement and no lock is shared with the connection's
    /// block, so a multiplexed connection may use it too. Never called with
    /// a block open (`takes` routes every statement of one to `run`).
    pub(crate) async fn run_alone(&self, query: &str) -> Vec<u8> {
        let mut state = self.new_state();
        let answer = Box::pin(self.run_in(&mut state, query)).await;
        // Not reachable for a statement that is not transaction control, but
        // a block left open here would hold its locks with no client to end
        // it.
        if let Some(t) = state.transaction.take() {
            state.end_block_settings(&t);
            self.database.block_ddl_abort(t.id).await;
            t.abort_unless_aborted(&self.database);
            t.owner.release();
        }
        answer
    }

    async fn run_in(&self, state: &mut SessionState, query: &str) -> Vec<u8> {
        state.message_us = crate::core::statement_clock::wall_us();
        state.error_in_batch = false;
        state.native_capture = true;
        state.native_result = None;
        let mut sink = NativeSink::default();

        // As a simple query reads its text (`handle_simple_query`): escape
        // and dollar-quoted strings normalised before anything splits it,
        // and a text that cannot be fails the block it was sent in.
        let normalized = match crate::core::sql_scan::normalize_string_literals(query) {
            Ok(text) => text,
            Err(e) => {
                let _ = fail_statement(state, &mut sink, "22025", &e).await;
                state.statement_failed = false;
                return native_error("22025", &e);
            }
        };
        let statements = split_statements(&normalized);
        let stmt = match statements.as_slice() {
            [one] => one.as_str(),
            [] => return native_error("42601", "an empty statement"),
            _ => {
                return native_error(
                    "0A000",
                    "the native protocol runs one statement per request: send each statement on its own",
                );
            }
        };

        let ran = execute_recorded(&self.database, state, &mut sink, stmt, &[], false, None).await;
        let engine = state.native_result.take();
        state.statement_failed = false;

        match ran {
            Ok(()) => translate(stmt, &sink.out, engine),
            Err(e) => {
                // pgwire ends the session on such an error (a commit whose
                // durability failed outside a block, a write to the client
                // that failed): the block goes with it. Here the connection
                // stays, so it is poisoned instead: a client still inside
                // what it thinks is its block must not have its next
                // statement autocommit.
                if let Some(t) = state.transaction.take() {
                    log::warn!(
                        "a native transaction's statement failed internally ({e}); rolling back transaction {}",
                        t.id
                    );
                    state.end_block_settings(&t);
                    self.database.block_ddl_abort(t.id).await;
                    t.abort_unless_aborted(&self.database);
                    t.owner.release();
                    self.poisoned.store(true, Ordering::Release);
                }
                native_error("XX000", &e.to_string())
            }
        }
    }

    fn new_state(&self) -> SessionState {
        let catalog = self
            .catalog
            .get_or_init(|| DatabaseCatalog::single_unmanaged(self.database.clone()))
            .clone();
        SessionState::new(
            catalog,
            self.database.name.clone(),
            AuthConfig::default(),
            Principal {
                name: self.user.clone(),
                superuser: self.superuser,
                authenticated: self.authenticated,
            },
            ConnectionLimits::default(),
        )
    }
}

impl Drop for NativeSession {
    /// The connection is gone and nothing of it is running: an open block is
    /// rolled back, as `run_message_loop` rolls back a PostgreSQL session's.
    fn drop(&mut self) {
        let Some(mut state) = self.state.get_mut().take() else {
            return;
        };
        let Some(t) = state.transaction.take() else {
            return;
        };
        log::info!(
            "native connection closed with transaction {} open; rolling it back",
            t.id
        );
        let database = self.database.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    database.block_ddl_abort(t.id).await;
                    t.abort_unless_aborted(&database);
                    t.owner.release();
                });
            }
            Err(_) => {
                // No runtime to undo the block's DDL on (the process is
                // ending). The rows go with the abort, and the DDL's intent
                // files are what the next open finishes from.
                log::warn!(
                    "no runtime to roll back transaction {}'s DDL; its intents remain for the next start",
                    t.id
                );
                t.abort_unless_aborted(&database);
                t.owner.release();
            }
        }
    }
}

/// Where the statement's PostgreSQL messages go, to be read back.
#[derive(Default)]
pub(super) struct NativeSink {
    pub(super) out: Vec<u8>,
}

impl AsyncRead for NativeSink {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // Nothing is read on this path; an empty read is the end of input.
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for NativeSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.out.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// What one statement's PostgreSQL messages said.
#[derive(Default, Debug)]
pub(super) struct Said {
    pub(super) error: Option<(String, String)>,
    pub(super) tag: Option<String>,
    /// Each column's type OID, when the statement described rows.
    pub(super) columns: Option<Vec<i32>>,
    /// Each column's name, alongside `columns`.
    pub(super) names: Vec<String>,
    pub(super) rows: Vec<Vec<Option<Vec<u8>>>>,
}

fn cstring(bytes: &[u8], at: &mut usize) -> Option<String> {
    let end = bytes.get(*at..)?.iter().position(|b| *b == 0)? + *at;
    let text = String::from_utf8_lossy(&bytes[*at..end]).into_owned();
    *at = end + 1;
    Some(text)
}

fn be_i16(bytes: &[u8], at: &mut usize) -> Option<i16> {
    let value = i16::from_be_bytes(bytes.get(*at..*at + 2)?.try_into().ok()?);
    *at += 2;
    Some(value)
}

fn be_i32(bytes: &[u8], at: &mut usize) -> Option<i32> {
    let value = i32::from_be_bytes(bytes.get(*at..*at + 4)?.try_into().ok()?);
    *at += 4;
    Some(value)
}

/// Reads the messages back. `None` when they do not parse, which is this
/// module's bug, not the client's.
pub(super) fn read_messages(out: &[u8]) -> Option<Said> {
    let mut said = Said::default();
    let mut at = 0usize;
    while at < out.len() {
        let tag = out[at];
        let len = i32::from_be_bytes(out.get(at + 1..at + 5)?.try_into().ok()?);
        let body = out.get(at + 5..at + 1 + len as usize)?;
        at += 1 + len as usize;
        match tag {
            b'E' if said.error.is_none() => {
                let (mut code, mut message) = (String::new(), String::new());
                let mut p = 0usize;
                while p < body.len() && body[p] != 0 {
                    let field = body[p];
                    p += 1;
                    let value = cstring(body, &mut p)?;
                    match field {
                        b'C' => code = value,
                        b'M' => message = value,
                        _ => {}
                    }
                }
                said.error = Some((code, message));
            }
            b'C' => {
                let mut p = 0usize;
                said.tag = Some(cstring(body, &mut p)?);
            }
            b'T' => {
                let mut p = 0usize;
                let count = be_i16(body, &mut p)?;
                let mut columns = Vec::with_capacity(count.max(0) as usize);
                said.names.clear();
                for _ in 0..count {
                    said.names.push(cstring(body, &mut p)?);
                    be_i32(body, &mut p)?; // table
                    be_i16(body, &mut p)?; // attribute
                    columns.push(be_i32(body, &mut p)?);
                    be_i16(body, &mut p)?; // size
                    be_i32(body, &mut p)?; // modifier
                    be_i16(body, &mut p)?; // format
                }
                said.columns = Some(columns);
            }
            b'D' => {
                let mut p = 0usize;
                let count = be_i16(body, &mut p)?;
                let mut row = Vec::with_capacity(count.max(0) as usize);
                for _ in 0..count {
                    let len = be_i32(body, &mut p)?;
                    if len < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(body.get(p..p + len as usize)?.to_vec()));
                        p += len as usize;
                    }
                }
                said.rows.push(row);
            }
            // Notices (a warning, `there is no transaction in progress`):
            // the native protocol has nowhere to put them.
            _ => {}
        }
    }
    Some(said)
}

/// Whether `stmt` asks for the block to be committed.
fn is_commit(stmt: &str) -> bool {
    let first = stmt
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    first.eq_ignore_ascii_case("commit") || first.eq_ignore_ascii_case("end")
}

/// The native answer to one statement run in the session.
fn translate(stmt: &str, out: &[u8], engine: Option<Vec<u8>>) -> Vec<u8> {
    let Some(said) = read_messages(out) else {
        log::error!(
            "a native transaction's statement produced messages this module could not read"
        );
        return native_error(
            "XX000",
            "internal error: the statement's answer could not be read",
        );
    };
    if let Some((code, message)) = said.error {
        return native_error(&code, &message);
    }
    if let Some(engine) = engine {
        return engine;
    }
    if let Some(columns) = said.columns {
        return text_rows(&columns, &said.rows);
    }
    match said.tag.as_deref() {
        // A COMMIT of a block that had failed rolls it back, and PostgreSQL
        // says so only in the command tag, which the native protocol does not
        // carry: answered as success, the client would believe its writes
        // had landed. An error, and the block is closed either way.
        Some("ROLLBACK") if is_commit(stmt) => native_error(
            "25P02",
            "the transaction was rolled back, not committed: a statement in it had failed",
        ),
        Some(tag) => {
            let count = tag
                .rsplit(' ')
                .next()
                .filter(|_| {
                    [
                        "INSERT ", "UPDATE ", "DELETE ", "SELECT ", "MERGE ", "COPY ",
                    ]
                    .iter()
                    .any(|verb| tag.starts_with(verb))
                })
                .and_then(|n| n.parse::<u64>().ok());
            match count {
                Some(n) => {
                    let mut answer = vec![1u8];
                    answer.extend_from_slice(&n.to_le_bytes());
                    answer
                }
                None => vec![0u8],
            }
        }
        None => native_error("XX000", "internal error: the statement produced no answer"),
    }
}

/// Text rows (`SHOW`, a builtin function) as native rows, each cell typed by
/// its column's OID. A type with no native counterpart here is refused rather
/// than sent as text a client would read as something else.
fn text_rows(columns: &[i32], rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
    use crate::core::row::row::{Column, Row};

    fn cell(column_type: DbType, bytes: &[u8]) -> Column {
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(bytes);
        Column {
            schema_id: 0,
            data: block,
            column_type,
        }
    }

    let mut answer = vec![2u8];
    for row in rows {
        let mut out = Row {
            position: 0,
            columns: smallvec::SmallVec::new(),
            length: 0,
        };
        for (index, value) in row.iter().enumerate() {
            let oid = columns.get(index).copied().unwrap_or(25);
            let Some(value) = value else {
                out.columns.push(cell(DbType::NULL, &[]));
                continue;
            };
            let text = String::from_utf8_lossy(value);
            let parsed = match oid {
                // text, varchar, name, char, bpchar, unknown
                25 | 1043 | 19 | 18 | 1042 | 705 => Some(cell(DbType::STRING, value)),
                16 => match text.as_ref() {
                    "t" | "true" => Some(cell(DbType::BOOL, &[1])),
                    "f" | "false" => Some(cell(DbType::BOOL, &[0])),
                    _ => None,
                },
                20 => text
                    .parse::<i64>()
                    .ok()
                    .map(|v| cell(DbType::I64, &v.to_le_bytes())),
                23 => text
                    .parse::<i32>()
                    .ok()
                    .map(|v| cell(DbType::I32, &v.to_le_bytes())),
                21 => text
                    .parse::<i16>()
                    .ok()
                    .map(|v| cell(DbType::I16, &v.to_le_bytes())),
                701 => text
                    .parse::<f64>()
                    .ok()
                    .map(|v| cell(DbType::F64, &v.to_le_bytes())),
                700 => text
                    .parse::<f32>()
                    .ok()
                    .map(|v| cell(DbType::F32, &v.to_le_bytes())),
                _ => {
                    return native_error(
                        "0A000",
                        &format!(
                            "this statement's result (a column of type OID {oid}) cannot be sent over the \
                             native protocol inside a transaction; select it as ::text"
                        ),
                    );
                }
            };
            match parsed {
                Some(column) => out.columns.push(column),
                None => {
                    return native_error(
                        "XX000",
                        &format!("internal error: {text:?} is not a value of type OID {oid}"),
                    );
                }
            }
        }
        out.extend_serialized(&mut answer);
    }
    answer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_control_is_recognised_by_its_first_word() {
        for sql in [
            "BEGIN",
            "  begin isolation level repeatable read",
            "START TRANSACTION",
            "commit",
            "END",
            "ROLLBACK TO SAVEPOINT a",
            "abort",
            "SAVEPOINT a",
            "RELEASE a",
        ] {
            assert!(is_transaction_control(sql), "{sql}");
        }
        for sql in [
            "SELECT begin FROM t",
            "INSERT INTO commits VALUES (1)",
            "LOCK TABLE t",
            "SET x = 1",
        ] {
            assert!(!is_transaction_control(sql), "{sql}");
        }
    }

    #[test]
    fn an_error_carries_its_sqlstate() {
        let answer = native_error("40001", "could not serialize access");
        assert_eq!(answer[0], 3);
        assert_eq!(&answer[1..], b"could not serialize access (SQLSTATE 40001)");
    }

    #[test]
    fn a_commit_answered_rollback_is_an_error() {
        let mut out = Vec::new();
        let tag = b"ROLLBACK\0";
        out.push(b'C');
        out.extend_from_slice(&((4 + tag.len()) as i32).to_be_bytes());
        out.extend_from_slice(tag);
        let answer = translate("COMMIT", &out, None);
        assert_eq!(answer[0], 3, "{}", String::from_utf8_lossy(&answer));
        assert!(String::from_utf8_lossy(&answer).contains("25P02"));
        // A ROLLBACK answered ROLLBACK is what it asked for.
        assert_eq!(translate("ROLLBACK", &out, None), vec![0u8]);
    }

    #[test]
    fn a_write_tag_becomes_a_count() {
        let mut out = Vec::new();
        let tag = b"INSERT 0 3\0";
        out.push(b'C');
        out.extend_from_slice(&((4 + tag.len()) as i32).to_be_bytes());
        out.extend_from_slice(tag);
        let answer = translate("INSERT INTO t VALUES (1), (2), (3)", &out, None);
        assert_eq!(answer[0], 1);
        assert_eq!(u64::from_le_bytes(answer[1..9].try_into().unwrap()), 3);
    }
}
