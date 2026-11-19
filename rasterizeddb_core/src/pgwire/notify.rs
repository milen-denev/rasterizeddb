//! `LISTEN`, `UNLISTEN`, `NOTIFY` and `pg_notify`: PostgreSQL's notifications
//! between sessions of one database.
//!
//! A notification is sent when the transaction that raised it commits (at
//! once outside a block), to every session listening on its channel, itself
//! included, as a `NotificationResponse`; a block that rolls back sends none
//! (its queue goes with its `SessionTransaction`). A session receives them
//! while it waits for its next message, as drivers' listen loops expect, and
//! between statements. Each was "syntax error or unsupported query".

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Mutex;

use rclite::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::{SessionState, fail_statement, send_command_complete, send_message};
use crate::core::database::Database;

/// One notification as a listener receives it.
#[derive(Debug, Clone)]
pub(crate) struct Note {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

type Listeners = HashMap<(u64, String), Vec<(u64, UnboundedSender<Note>)>>;

/// Every listening session, by database and channel.
static HUB: Mutex<Option<Listeners>> = Mutex::new(None);

fn hub<R>(f: impl FnOnce(&mut Listeners) -> R) -> R {
    let mut guard = HUB.lock().unwrap_or_else(|e| e.into_inner());
    f(guard.get_or_insert_with(HashMap::new))
}

/// Sends `channel`'s notification to every session of `database` listening
/// on it. A session gone since is dropped from the list.
pub(crate) fn publish(database: u64, pid: i32, channel: &str, payload: &str) {
    let note = Note {
        pid,
        channel: channel.to_string(),
        payload: payload.to_string(),
    };
    hub(|listeners| {
        if let Some(sessions) = listeners.get_mut(&(database, channel.to_string())) {
            sessions.retain(|(_, tx)| tx.send(note.clone()).is_ok());
        }
    });
}

/// A session's channels and the queue its notifications arrive on.
pub(crate) struct Listener {
    id: u64,
    database: u64,
    tx: UnboundedSender<Note>,
    pub(super) rx: UnboundedReceiver<Note>,
    channels: HashSet<String>,
}

impl Listener {
    pub(crate) fn new(database: u64) -> Listener {
        let (tx, rx) = unbounded_channel();
        Listener {
            id: fastrand::u64(..),
            database,
            tx,
            rx,
            channels: HashSet::new(),
        }
    }

    fn listen(&mut self, channel: &str) {
        if self.channels.insert(channel.to_string()) {
            let entry = (self.id, self.tx.clone());
            hub(|listeners| {
                listeners
                    .entry((self.database, channel.to_string()))
                    .or_default()
                    .push(entry)
            });
        }
    }

    fn unlisten(&mut self, channel: &str) {
        if self.channels.remove(channel) {
            let id = self.id;
            hub(|listeners| {
                if let Some(sessions) = listeners.get_mut(&(self.database, channel.to_string())) {
                    sessions.retain(|(s, _)| *s != id);
                }
            });
        }
    }

    fn unlisten_all(&mut self) {
        for channel in std::mem::take(&mut self.channels) {
            self.channels.insert(channel.clone());
            self.unlisten(&channel);
        }
    }
}

impl Drop for Listener {
    /// The session is gone: none of its channels hears for it any more.
    fn drop(&mut self) {
        self.unlisten_all();
    }
}

/// A notification statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NotifyStatement {
    Listen(String),
    Unlisten(Option<String>),
    Notify(String, String),
}

/// An identifier as PostgreSQL reads one: quoted kept as written, bare
/// lowercased.
fn identifier(text: &str) -> Option<String> {
    let t = text.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        return Some(t[1..t.len() - 1].replace("\"\"", "\""));
    }
    (!t.is_empty()
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80)
        && !t.as_bytes()[0].is_ascii_digit())
    .then(|| t.to_ascii_lowercase())
}

/// A single-quoted string literal's value.
fn string_literal(text: &str) -> Option<String> {
    let t = text.trim();
    (t.len() >= 2 && t.starts_with('\'') && t.ends_with('\''))
        .then(|| t[1..t.len() - 1].replace("''", "'"))
}

/// PostgreSQL's payload limit.
const PAYLOAD_LIMIT: usize = 8000;

/// `LISTEN`, `UNLISTEN`, `NOTIFY` or `SELECT pg_notify('chan', 'payload')`;
/// `None` for anything else.
pub(crate) fn parse_notify_statement(
    sql: &str,
) -> Option<Result<NotifyStatement, (&'static str, String)>> {
    let t = sql.trim().trim_end_matches(';').trim_end();
    let word = |w: &str| {
        t.len() > w.len()
            && t[..w.len()].eq_ignore_ascii_case(w)
            && t.as_bytes()[w.len()].is_ascii_whitespace()
    };
    let syntax = |what: &str| Some(Err(("42601", format!("syntax error in {what}"))));
    if word("LISTEN") {
        return Some(
            identifier(&t[6..])
                .map(NotifyStatement::Listen)
                .ok_or(("42601", "syntax error in LISTEN".to_string())),
        );
    }
    if word("UNLISTEN") {
        let rest = t[8..].trim();
        if rest == "*" {
            return Some(Ok(NotifyStatement::Unlisten(None)));
        }
        return Some(
            identifier(rest)
                .map(|c| NotifyStatement::Unlisten(Some(c)))
                .ok_or(("42601", "syntax error in UNLISTEN".to_string())),
        );
    }
    if word("NOTIFY") {
        let rest = t[6..].trim();
        let (channel, payload) = match rest.split_once(',') {
            Some((c, p)) => match string_literal(p) {
                Some(p) => (c, p),
                None => return syntax("NOTIFY"),
            },
            None => (rest, String::new()),
        };
        let Some(channel) = identifier(channel) else {
            return syntax("NOTIFY");
        };
        return Some(checked(channel, payload));
    }
    // `SELECT pg_notify('chan', 'payload')`, the function form.
    let lower = t.to_ascii_lowercase();
    let call = lower.strip_prefix("select")?.trim_start();
    let call = call.strip_prefix("pg_catalog.").unwrap_or(call);
    if !call
        .strip_prefix("pg_notify")?
        .trim_start()
        .starts_with('(')
        || !t.ends_with(')')
    {
        return None;
    }
    // The arguments from the original text, so the payload keeps its case:
    // from the first `(` (the call's: nothing before it can hold one) to the
    // closing one, the statement's last byte.
    let open = t.find('(')?;
    let args = &t[open + 1..t.len() - 1];
    let parts = crate::core::sql_scan::split_top_level(args, b',');
    let [channel, payload] = parts.as_slice() else {
        return None;
    };
    let (Some(channel), Some(payload)) = (string_literal(channel), string_literal(payload)) else {
        return None;
    };
    Some(checked(channel, payload))
}

fn checked(channel: String, payload: String) -> Result<NotifyStatement, (&'static str, String)> {
    if channel.is_empty() {
        return Err(("22023", "channel name cannot be empty".to_string()));
    }
    if payload.len() >= PAYLOAD_LIMIT {
        return Err(("22023", "payload string too long".to_string()));
    }
    Ok(NotifyStatement::Notify(channel, payload))
}

/// Runs a notification statement.
pub(super) async fn run<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    statement: NotifyStatement,
    function_form: bool,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    match statement {
        NotifyStatement::Listen(channel) => {
            state
                .listener
                .get_or_insert_with(|| Listener::new(database.id))
                .listen(&channel);
            send_command_complete(stream, "LISTEN").await
        }
        NotifyStatement::Unlisten(channel) => {
            if let Some(listener) = state.listener.as_mut() {
                match channel {
                    Some(c) => listener.unlisten(&c),
                    None => listener.unlisten_all(),
                }
            }
            send_command_complete(stream, "UNLISTEN").await
        }
        NotifyStatement::Notify(channel, payload) => {
            if state.transaction.as_ref().is_some_and(|t| t.failed) {
                return fail_statement(
                    state,
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                )
                .await;
            }
            match state.transaction.as_mut() {
                // Sent when the block commits; one of each, as PostgreSQL
                // folds duplicates within a transaction.
                Some(t) => {
                    if !t
                        .notifies
                        .iter()
                        .any(|(c, p)| *c == channel && *p == payload)
                    {
                        t.notifies.push((channel, payload));
                    }
                }
                None => publish(database.id, state.backend_pid, &channel, &payload),
            }
            if function_form {
                // `pg_notify` returns void, whose text is empty: one row, one
                // empty column, described as text (as `Describe` describes it).
                if !omit_description {
                    let mut description = Vec::new();
                    description.extend_from_slice(&1i16.to_be_bytes());
                    description.extend_from_slice(b"pg_notify\0");
                    description.extend_from_slice(&0i32.to_be_bytes());
                    description.extend_from_slice(&0i16.to_be_bytes());
                    description.extend_from_slice(&25i32.to_be_bytes());
                    description.extend_from_slice(&(-1i16).to_be_bytes());
                    description.extend_from_slice(&(-1i32).to_be_bytes());
                    description.extend_from_slice(&0i16.to_be_bytes());
                    send_message(stream, b'T', &description).await?;
                }
                let mut row = Vec::new();
                row.extend_from_slice(&1i16.to_be_bytes());
                row.extend_from_slice(&0i32.to_be_bytes());
                send_message(stream, b'D', &row).await?;
                return send_command_complete(stream, "SELECT 1").await;
            }
            send_command_complete(stream, "NOTIFY").await
        }
    }
}

/// A `NotificationResponse`.
pub(super) async fn send_note<S>(stream: &mut S, note: &Note) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(note.channel.len() + note.payload.len() + 6);
    payload.extend_from_slice(&note.pid.to_be_bytes());
    payload.extend_from_slice(note.channel.as_bytes());
    payload.push(0);
    payload.extend_from_slice(note.payload.as_bytes());
    payload.push(0);
    send_message(stream, b'A', &payload).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_parse_as_postgresql_reads_them() {
        assert_eq!(
            parse_notify_statement("LISTEN Jobs").unwrap().unwrap(),
            NotifyStatement::Listen("jobs".into())
        );
        assert_eq!(
            parse_notify_statement("listen \"Jobs\";").unwrap().unwrap(),
            NotifyStatement::Listen("Jobs".into())
        );
        assert_eq!(
            parse_notify_statement("UNLISTEN *").unwrap().unwrap(),
            NotifyStatement::Unlisten(None)
        );
        assert_eq!(
            parse_notify_statement("NOTIFY jobs, 'it''s done'")
                .unwrap()
                .unwrap(),
            NotifyStatement::Notify("jobs".into(), "it's done".into())
        );
        assert_eq!(
            parse_notify_statement("NOTIFY jobs").unwrap().unwrap(),
            NotifyStatement::Notify("jobs".into(), String::new())
        );
        assert_eq!(
            parse_notify_statement("SELECT pg_notify('Jobs', 'Payload X')")
                .unwrap()
                .unwrap(),
            NotifyStatement::Notify("Jobs".into(), "Payload X".into())
        );
        assert!(parse_notify_statement("SELECT pg_notify(c, 'x') FROM t").is_none());
        assert!(parse_notify_statement("SELECT 1").is_none());
        assert!(parse_notify_statement("LISTENER").is_none());
        assert_eq!(
            parse_notify_statement(&format!("NOTIFY j, '{}'", "x".repeat(8000)))
                .unwrap()
                .unwrap_err()
                .0,
            "22023"
        );
    }

    #[test]
    fn a_listener_hears_its_database_and_channel_alone() {
        let mut a = Listener::new(9_000_001);
        let mut b = Listener::new(9_000_002);
        a.listen("ch");
        b.listen("ch");
        publish(9_000_001, 7, "ch", "one");
        publish(9_000_001, 7, "other", "two");
        assert_eq!(a.rx.try_recv().unwrap().payload, "one");
        assert!(a.rx.try_recv().is_err());
        assert!(b.rx.try_recv().is_err());
        a.unlisten("ch");
        publish(9_000_001, 7, "ch", "three");
        assert!(a.rx.try_recv().is_err());
        drop(b);
        // A dropped listener is gone from the hub.
        assert!(hub(|l| l
            .get(&(9_000_002, "ch".to_string()))
            .is_none_or(Vec::is_empty)));
    }
}
