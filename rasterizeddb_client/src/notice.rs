//! Notices: what the server tells a client about a statement without
//! failing it.
//!
//! PostgreSQL sends them as `NoticeResponse` messages: `RAISE NOTICE` in a
//! `DO` block, `DROP TABLE IF EXISTS` naming a table that is not there,
//! `CREATE TABLE IF NOT EXISTS` naming one that is, a `BEGIN` inside a
//! transaction. A native connection asks for them when it opens (a setup
//! request, `\x01NOTICES;`), and the server then carries them in front of
//! the answer they belong to. A server that predates notices refuses the
//! request without running anything, and its answers are read as they always
//! were: no notices, and nothing else changed.
//!
//! They are on each answer ([`Rows::notices`](crate::Rows::notices)), and
//! every one, including those of a statement that failed afterwards, goes to
//! a client's handler ([`Client::set_notice_handler`](crate::Client::set_notice_handler)).

use std::fmt;
use std::sync::Arc;

use rastcp::error::RastcpError;
use rastcp::session::ConnectionSetup;

use crate::{Error, Result};

/// One notice: its severity, SQLSTATE and message, as PostgreSQL's
/// `NoticeResponse` carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Notice {
    /// `NOTICE`, `WARNING`, `INFO`, `LOG` or `DEBUG`.
    pub severity: String,
    /// The five-character SQLSTATE: `00000` for a plain notice, `01000` for
    /// a warning, `42P07` for `CREATE TABLE IF NOT EXISTS` naming a table that
    /// exists, `25001` for a `BEGIN` inside a transaction, …
    pub sqlstate: String,
    /// The text.
    pub message: String,
}

impl fmt::Display for Notice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} (SQLSTATE {})", self.severity, self.message, self.sqlstate)
    }
}

/// What [`Client::set_notice_handler`](crate::Client::set_notice_handler)
/// is given: called once per notice, in the order the server raised them,
/// before the call that sent the statement returns.
pub type NoticeHandler = Arc<dyn Fn(&Notice) + Send + Sync>;

/// A client's handler, shared by its connections and its transactions'.
#[derive(Default)]
pub(crate) struct NoticeHook(std::sync::RwLock<Option<NoticeHandler>>);

impl NoticeHook {
    pub(crate) fn set(&self, handler: Option<NoticeHandler>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = handler;
    }

    /// Hands `notices` to the handler, if there is one.
    pub(crate) fn deliver(&self, notices: &[Notice]) {
        if notices.is_empty() {
            return;
        }
        let handler = self.0.read().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(handler) = handler {
            for notice in notices {
                handler(notice);
            }
        }
    }
}

/// The server's `pgwire::native_session::NOTICES_PREFIX`.
const NOTICES_REQUEST: &[u8] = b"\x01NOTICES;";

/// The first byte of an answer carrying notices (the server's
/// `NOTICES_ANSWER`): a `u32` count, each notice as three NUL-terminated
/// strings, then the answer.
pub(crate) const NOTICES_ANSWER: u8 = 5;

/// Asks a connection for notices: a setup request of every connection the
/// client opens. Whatever the answer, the connection is used: a server that
/// refuses it (one that predates notices) simply sends none.
#[derive(Debug)]
pub(crate) struct NoticesSetup;

impl ConnectionSetup for NoticesSetup {
    fn request(&self) -> Vec<u8> {
        NOTICES_REQUEST.to_vec()
    }

    fn accept(&self, _answer: &[u8]) -> std::result::Result<(), RastcpError> {
        Ok(())
    }
}

pub(crate) fn setup() -> Arc<dyn ConnectionSetup> {
    Arc::new(NoticesSetup)
}

/// Splits the notices off the front of an answer: none, and the answer
/// whole, for one that carries none.
pub(crate) fn split(answer: Vec<u8>) -> Result<(Vec<Notice>, Vec<u8>)> {
    if answer.first() != Some(&NOTICES_ANSWER) {
        return Ok((Vec::new(), answer));
    }
    let bad = || Error::Protocol("an answer's notices are truncated".to_string());
    let count = answer.get(1..5).ok_or_else(bad)?;
    let count = u32::from_le_bytes(count.try_into().expect("four bytes"));
    let mut at = 5usize;
    let field = |at: &mut usize| -> Result<String> {
        let rest = answer.get(*at..).ok_or_else(bad)?;
        let end = rest.iter().position(|b| *b == 0).ok_or_else(bad)?;
        let text = String::from_utf8_lossy(&rest[..end]).into_owned();
        *at += end + 1;
        Ok(text)
    };
    // Bounded by the answer, never by the count it claims.
    let mut notices = Vec::new();
    for _ in 0..count {
        let severity = field(&mut at)?;
        let sqlstate = field(&mut at)?;
        let message = field(&mut at)?;
        notices.push(Notice {
            severity,
            sqlstate,
            message,
        });
    }
    let rest = answer.get(at..).ok_or_else(bad)?.to_vec();
    if rest.is_empty() {
        return Err(bad());
    }
    Ok((notices, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped(notices: &[(&str, &str, &str)], answer: &[u8]) -> Vec<u8> {
        let mut out = vec![NOTICES_ANSWER];
        out.extend_from_slice(&(notices.len() as u32).to_le_bytes());
        for (s, c, m) in notices {
            for f in [s, c, m] {
                out.extend_from_slice(f.as_bytes());
                out.push(0);
            }
        }
        out.extend_from_slice(answer);
        out
    }

    #[test]
    fn an_answer_without_notices_is_untouched() {
        let (notices, answer) = split(vec![0]).unwrap();
        assert!(notices.is_empty());
        assert_eq!(answer, vec![0]);
    }

    #[test]
    fn notices_come_off_the_front_in_order() {
        let raw = wrapped(
            &[("NOTICE", "00000", "one"), ("WARNING", "01000", "two")],
            b"\x03boom",
        );
        let (notices, answer) = split(raw).unwrap();
        assert_eq!(notices.len(), 2);
        assert_eq!(notices[0].message, "one");
        assert_eq!(notices[1].severity, "WARNING");
        assert_eq!(notices[1].sqlstate, "01000");
        assert_eq!(answer, b"\x03boom");
    }

    #[test]
    fn a_truncated_or_bodiless_answer_is_refused() {
        assert!(split(vec![NOTICES_ANSWER, 1, 0]).is_err());
        let mut raw = wrapped(&[("NOTICE", "00000", "x")], b"");
        assert!(split(raw.clone()).is_err(), "no answer after the notices");
        raw.truncate(raw.len() - 1);
        assert!(split(raw).is_err());
        let mut huge = vec![NOTICES_ANSWER];
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        huge.push(0);
        assert!(split(huge).is_err());
    }
}
