//! The simple-query form: a statement run as a PostgreSQL client's simple
//! query is, answered in text with its column names.
//!
//! A native answer is the engine's binary cells and nothing else: no column
//! names, and a type tag that says `text` for a `jsonb` document or an array.
//! That suits an application, which knows what it selected. It does not suit
//! a console, an export, or anything else that shows a result it did not
//! write the statement for. [`Client::simple_query`](crate::Client::simple_query)
//! asks the server for the answer pgwire would give instead: each column's
//! name and type OID, every cell as PostgreSQL's text for it, each
//! statement's command tag, and the notices. The text may hold several
//! statements, which run as one implicit transaction as they do over pgwire,
//! and `WITH` and derived tables work.
//!
//! The server runs it through its PostgreSQL path, so this needs a server
//! that knows the form; an older one refuses it without running anything, and
//! that is [`Error::Unsupported`].

use crate::{Error, Result};

/// What a request starts with to be answered in this form; the server's
/// `pgwire::native_session::SIMPLE_QUERY_PREFIX`. A server without the form
/// reads it as two statements and refuses the request whole.
pub(crate) const PREFIX: &[u8] = b"\x01SIMPLE;";

/// The first byte of an answer in this form.
const ANSWER: u8 = 4;

/// One column of a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleColumn {
    /// Its name, as the statement gave it (`count`, or the alias).
    pub name: String,
    /// Its PostgreSQL type OID (25 `text`, 20 `int8`, 3802 `jsonb`, …).
    pub type_oid: u32,
}

/// What one statement of a simple query answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimpleResult {
    /// A statement that returned rows (possibly none).
    Rows {
        /// The columns, in order.
        columns: Vec<SimpleColumn>,
        /// Each row's cells as text; `None` is SQL NULL.
        rows: Vec<Vec<Option<String>>>,
        /// The command tag (`SELECT 3`).
        tag: String,
    },
    /// A statement that returned no rows.
    Command {
        /// The command tag (`INSERT 0 2`, `CREATE TABLE`).
        tag: String,
        /// The count the tag carries, for the statements whose tag has one.
        rows: Option<u64>,
    },
}

impl SimpleResult {
    /// The command tag.
    pub fn tag(&self) -> &str {
        match self {
            SimpleResult::Rows { tag, .. } | SimpleResult::Command { tag, .. } => tag,
        }
    }

    /// How many rows the statement returned or touched, as its tag counts
    /// them: for a result sent under [`SimpleOptions::max_rows`], every row,
    /// not only those sent. `None` for a tag without a count.
    pub fn row_count(&self) -> Option<u64> {
        tag_count(self.tag())
    }
}

/// How a simple query is sent ([`Client::simple_query_with`](crate::Client::simple_query_with)).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleOptions {
    /// Registered for the length of the query, so
    /// [`Client::cancel`](crate::Client::cancel) with the same token, from
    /// any connection of the same user, stops the statement it is running.
    pub cancel: Option<CancelToken>,
    /// At most this many rows of each result are sent; the rest are counted
    /// by the result's tag ([`SimpleResult::row_count`]) and dropped by the
    /// server. A console's guard against a `SELECT *` it cannot show.
    pub max_rows: Option<u64>,
}

impl SimpleOptions {
    /// The header these options put in front of the statement text.
    fn header(&self) -> Vec<u8> {
        let mut options = Vec::new();
        if let Some(token) = &self.cancel {
            options.push(format!("cancel={}", token.0));
        }
        if let Some(rows) = self.max_rows {
            options.push(format!("rows={rows}"));
        }
        if options.is_empty() {
            return PREFIX.to_vec();
        }
        let mut header = PREFIX[..PREFIX.len() - 1].to_vec();
        header.push(b':');
        header.extend_from_slice(options.join(",").as_bytes());
        header.push(b';');
        header
    }
}

/// What a simple query is cancelled by: a secret the client chooses and
/// sends with the query, since the native protocol hands out no key before a
/// statement runs. The server scopes it by user, so only a connection of the
/// same user can cancel with it. Use a fresh one per query: a token is
/// refused while a query of the same user still holds it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CancelToken(String);

impl CancelToken {
    /// A token of 128 random bits.
    pub fn new() -> Self {
        let mut bytes = [0u8; 16];
        if crate::types::random_bytes(&mut bytes).is_err() {
            // Unique is what matters here; secrecy only guards against other
            // connections of the same user.
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default();
            bytes[..8].copy_from_slice(&t.to_be_bytes());
            bytes[8..].copy_from_slice(&n.to_be_bytes());
        }
        CancelToken(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }

    /// The token as sent.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

/// The request [`Client::cancel`](crate::Client::cancel) sends.
pub(crate) fn cancel_request(token: &CancelToken) -> Vec<u8> {
    let mut request = b"\x01CANCEL;".to_vec();
    request.extend_from_slice(token.0.as_bytes());
    request
}

/// Reads the answer to a cancel: whether a statement was stopped.
pub(crate) fn decode_cancel(response: Vec<u8>) -> Result<bool> {
    let answer = decode(response).map_err(|e| match e {
        Error::Unsupported(_) => Error::Unsupported(
            "this server cannot cancel a native query (it predates cancel tokens)".to_string(),
        ),
        e => e,
    })?;
    match answer.results.first().map(SimpleResult::tag) {
        Some("CANCEL 1") => Ok(true),
        Some("CANCEL 0") => Ok(false),
        other => Err(Error::Protocol(format!(
            "a cancel was answered with {other:?}"
        ))),
    }
}

/// The request text for `sql` under `options`.
pub(crate) fn request(sql: &str, options: &SimpleOptions) -> Result<Vec<u8>> {
    if sql.as_bytes().contains(&0) {
        return Err(Error::Param("the statement contains a NUL byte".into()));
    }
    let mut request = options.header();
    request.extend_from_slice(sql.as_bytes());
    Ok(request)
}

/// Whether the connection is inside a transaction block once the request has
/// run (PostgreSQL's `ReadyForQuery` status).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    /// Not in a block.
    #[default]
    Idle,
    /// In a block.
    InBlock,
    /// In a block that has failed, which refuses everything until it ends.
    Failed,
}

/// The answer to a simple query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SimpleAnswer {
    /// One per statement, in order.
    pub results: Vec<SimpleResult>,
    /// The notices the statements raised, as their messages.
    pub notices: Vec<String>,
    /// Where the connection's transaction stands afterwards.
    pub transaction: TransactionStatus,
}

/// Reads an answer to a request in this form.
pub(crate) fn decode(response: Vec<u8>) -> Result<SimpleAnswer> {
    decode_status(response).0
}

/// [`decode`], and where the connection's transaction stands afterwards
/// whatever the answer was, error or not: `None` only when the answer did not
/// say (a damaged one, or a refusal).
pub(crate) fn decode_status(response: Vec<u8>) -> (Result<SimpleAnswer>, Option<TransactionStatus>) {
    match response.first() {
        Some(&ANSWER) => match read_messages(&response[1..]) {
            Ok((answer, None)) => {
                let status = answer.transaction;
                (Ok(answer), Some(status))
            }
            Ok((answer, Some(error))) => (Err(error), Some(answer.transaction)),
            Err(e) => (Err(e), None),
        },
        _ => (refusal(&response), None),
    }
}

fn refusal(response: &[u8]) -> Result<SimpleAnswer> {
    match response.first() {
        // The refusal of a server that predates the form: the prefix made the
        // request two statements, so nothing ran.
        Some(3) => Err(Error::Unsupported(format!(
            "this server does not answer the simple-query form over the native protocol (it predates \
             it), and ran nothing: {}",
            String::from_utf8_lossy(&response[1..])
        ))),
        Some(other) => Err(Error::Protocol(format!(
            "a simple query was answered with tag {other}, not as a simple query"
        ))),
        None => Err(Error::Protocol(
            "the server sent an empty answer".to_string(),
        )),
    }
}

fn damaged(what: &str) -> Error {
    Error::Protocol(format!("a simple-query answer is damaged: {what}"))
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| damaged("a field runs past its message"))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn cstring(&mut self) -> Result<String> {
        let rest = &self.bytes[self.at..];
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| damaged("a string has no end"))?;
        let text = String::from_utf8_lossy(&rest[..end]).into_owned();
        self.at += end + 1;
        Ok(text)
    }
}

/// The count a command tag carries, as libpq's `PQcmdTuples` reads it.
fn tag_count(tag: &str) -> Option<u64> {
    let verb = tag.split(' ').next()?;
    if !matches!(
        verb,
        "INSERT" | "UPDATE" | "DELETE" | "SELECT" | "MERGE" | "MOVE" | "FETCH" | "COPY"
    ) {
        return None;
    }
    tag.rsplit(' ').next()?.parse().ok()
}

/// An `ErrorResponse` or `NoticeResponse` body: its message, and its SQLSTATE.
fn fields(body: &[u8]) -> Result<(String, String)> {
    let mut r = Reader { bytes: body, at: 0 };
    let (mut code, mut message) = (String::new(), String::new());
    while r.at < body.len() {
        let field = r.take(1)?[0];
        if field == 0 {
            break;
        }
        let value = r.cstring()?;
        match field {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    Ok((code, message))
}

/// The answer's messages: what they said, and the error that stopped the
/// request, if one did. `Err` only for a damaged answer.
fn read_messages(bytes: &[u8]) -> Result<(SimpleAnswer, Option<Error>)> {
    let mut answer = SimpleAnswer::default();
    let mut current: Option<(Vec<SimpleColumn>, Vec<Vec<Option<String>>>)> = None;
    let mut error: Option<Error> = None;
    let mut ready = false;
    let mut r = Reader { bytes, at: 0 };

    while r.at < bytes.len() {
        let tag = r.take(1)?[0];
        let len = r.i32()?;
        let len = usize::try_from(len)
            .ok()
            .and_then(|l| l.checked_sub(4))
            .ok_or_else(|| damaged("a message's length is negative"))?;
        let body = r.take(len)?;
        let mut m = Reader { bytes: body, at: 0 };
        match tag {
            b'T' => {
                let count = m.i16()?;
                let mut columns = Vec::with_capacity(count.max(0) as usize);
                for _ in 0..count {
                    let name = m.cstring()?;
                    m.take(4 + 2)?; // table, attribute
                    let type_oid = m.i32()? as u32;
                    m.take(2 + 4 + 2)?; // size, modifier, format
                    columns.push(SimpleColumn { name, type_oid });
                }
                current = Some((columns, Vec::new()));
            }
            b'D' => {
                let Some((columns, rows)) = current.as_mut() else {
                    return Err(damaged("a row arrived with no description before it"));
                };
                let count = m.i16()?;
                if count as usize != columns.len() {
                    return Err(damaged("a row's width differs from its description"));
                }
                let mut row = Vec::with_capacity(columns.len());
                for _ in 0..count {
                    let len = m.i32()?;
                    if len < 0 {
                        row.push(None);
                    } else {
                        let cell = m.take(len as usize)?;
                        row.push(Some(String::from_utf8_lossy(cell).into_owned()));
                    }
                }
                rows.push(row);
            }
            b'C' => {
                let tag = m.cstring()?;
                answer.results.push(match current.take() {
                    Some((columns, rows)) => SimpleResult::Rows { columns, rows, tag },
                    None => SimpleResult::Command {
                        rows: tag_count(&tag),
                        tag,
                    },
                });
            }
            b'E' => {
                // The first is the one that stopped the request; a later one
                // (a block left open on a shared connection) adds to it.
                let (code, message) = fields(body)?;
                let text = if code.is_empty() {
                    message
                } else {
                    format!("{message} (SQLSTATE {code})")
                };
                error = Some(match error.take() {
                    Some(Error::Server(first)) => Error::Server(format!("{first}; then: {text}")),
                    _ => Error::Server(text),
                });
                current = None;
            }
            b'N' => answer.notices.push(fields(body)?.1),
            b'Z' => {
                answer.transaction = match m.take(1)?[0] {
                    b'T' => TransactionStatus::InBlock,
                    b'E' => TransactionStatus::Failed,
                    _ => TransactionStatus::Idle,
                };
                ready = true;
            }
            // EmptyQueryResponse, ParameterStatus, and anything later
            // versions add: nothing a result is made of.
            _ => {}
        }
    }
    if !ready {
        // The error says more than the missing end does.
        return Err(error.unwrap_or_else(|| damaged("it ends before ReadyForQuery")));
    }
    Ok((answer, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn description(columns: &[(&str, u32)]) -> Vec<u8> {
        let mut body = (columns.len() as i16).to_be_bytes().to_vec();
        for (name, oid) in columns {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&[0; 6]);
            body.extend_from_slice(&oid.to_be_bytes());
            body.extend_from_slice(&[0; 8]);
        }
        message(b'T', &body)
    }

    fn row(cells: &[Option<&str>]) -> Vec<u8> {
        let mut body = (cells.len() as i16).to_be_bytes().to_vec();
        for cell in cells {
            match cell {
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(text) => {
                    body.extend_from_slice(&(text.len() as i32).to_be_bytes());
                    body.extend_from_slice(text.as_bytes());
                }
            }
        }
        message(b'D', &body)
    }

    fn complete(tag: &str) -> Vec<u8> {
        let mut body = tag.as_bytes().to_vec();
        body.push(0);
        message(b'C', &body)
    }

    fn error(code: &str, text: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (f, v) in [(b'S', "ERROR"), (b'C', code), (b'M', text)] {
            body.push(f);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        message(b'E', &body)
    }

    fn answer(parts: &[Vec<u8>], status: u8) -> Vec<u8> {
        let mut out = vec![ANSWER];
        for part in parts {
            out.extend_from_slice(part);
        }
        out.extend_from_slice(&message(b'Z', &[status]));
        out
    }

    #[test]
    fn rows_and_commands_in_order() {
        let got = decode(answer(
            &[
                complete("INSERT 0 2"),
                description(&[("id", 20), ("note", 25)]),
                row(&[Some("1"), None]),
                row(&[Some("2"), Some("")]),
                complete("SELECT 2"),
                complete("CREATE TABLE"),
            ],
            b'I',
        ))
        .unwrap();
        assert_eq!(
            got.results,
            vec![
                SimpleResult::Command {
                    tag: "INSERT 0 2".into(),
                    rows: Some(2)
                },
                SimpleResult::Rows {
                    columns: vec![
                        SimpleColumn {
                            name: "id".into(),
                            type_oid: 20
                        },
                        SimpleColumn {
                            name: "note".into(),
                            type_oid: 25
                        },
                    ],
                    rows: vec![
                        vec![Some("1".into()), None],
                        vec![Some("2".into()), Some(String::new())],
                    ],
                    tag: "SELECT 2".into(),
                },
                SimpleResult::Command {
                    tag: "CREATE TABLE".into(),
                    rows: None
                },
            ]
        );
        assert_eq!(got.transaction, TransactionStatus::Idle);
    }

    #[test]
    fn an_error_carries_its_sqlstate_and_the_block_status_is_read() {
        match decode(answer(
            &[error("42P01", "relation \"x\" does not exist")],
            b'I',
        )) {
            Err(e) => assert_eq!(e.sqlstate(), Some("42P01"), "{e}"),
            Ok(a) => panic!("{a:?}"),
        }
        let open = decode(answer(&[complete("BEGIN")], b'T')).unwrap();
        assert_eq!(open.transaction, TransactionStatus::InBlock);
    }

    #[test]
    fn an_older_server_refusal_is_unsupported_not_a_statement_error() {
        let mut refusal = vec![3u8];
        refusal.extend_from_slice(b"the native protocol runs one statement per request");
        assert!(matches!(decode(refusal), Err(Error::Unsupported(_))));
    }

    #[test]
    fn a_damaged_answer_is_an_error_never_a_shorter_one() {
        let mut whole = answer(
            &[
                description(&[("a", 25)]),
                row(&[Some("x")]),
                complete("SELECT 1"),
            ],
            b'I',
        );
        // Cut inside the row, and with no ReadyForQuery.
        for cut in [whole.len() - 1, whole.len() - 6, 12] {
            assert!(decode(whole[..cut].to_vec()).is_err(), "cut at {cut}");
        }
        // A row wider than its description.
        whole = answer(
            &[description(&[("a", 25)]), row(&[Some("x"), Some("y")])],
            b'I',
        );
        assert!(decode(whole).is_err());
    }

    #[test]
    fn tag_counts() {
        assert_eq!(tag_count("UPDATE 7"), Some(7));
        assert_eq!(tag_count("INSERT 0 3"), Some(3));
        assert_eq!(tag_count("CREATE TABLE"), None);
        assert_eq!(tag_count("SET 5"), None);
    }

    #[test]
    fn options_put_a_header_in_front_of_the_text() {
        assert_eq!(
            request("SELECT 1", &SimpleOptions::default()).unwrap(),
            b"\x01SIMPLE;SELECT 1"
        );
        let token = CancelToken("ab-1".into());
        let options = SimpleOptions {
            cancel: Some(token.clone()),
            max_rows: Some(500),
        };
        assert_eq!(
            request("SELECT 1; SELECT 2", &options).unwrap(),
            b"\x01SIMPLE:cancel=ab-1,rows=500;SELECT 1; SELECT 2"
        );
        let rows_only = SimpleOptions {
            cancel: None,
            max_rows: Some(0),
        };
        assert_eq!(request("", &rows_only).unwrap(), b"\x01SIMPLE:rows=0;");
        assert_eq!(cancel_request(&token), b"\x01CANCEL;ab-1");
        assert!(request("SELECT '\0'", &options).is_err());
    }

    #[test]
    fn a_token_is_fresh_and_sendable() {
        let (a, b) = (CancelToken::new(), CancelToken::new());
        assert_ne!(a, b);
        assert_eq!(a.as_str().len(), 32);
        assert!(a.as_str().bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn a_cancel_answer_says_whether_it_stopped_one() {
        assert!(decode_cancel(answer(&[complete("CANCEL 1")], b'I')).unwrap());
        assert!(!decode_cancel(answer(&[complete("CANCEL 0")], b'I')).unwrap());
        let mut old = vec![3u8];
        old.extend_from_slice(b"the native protocol runs one statement per request");
        assert!(matches!(decode_cancel(old), Err(Error::Unsupported(_))));
        assert!(matches!(
            decode_cancel(answer(&[complete("SELECT 1")], b'I')),
            Err(Error::Protocol(_))
        ));
    }

    /// An error still says where the block stands, which a session keeping a
    /// block between calls must know.
    #[test]
    fn a_failed_answer_still_carries_its_transaction_status() {
        let (result, status) = decode_status(answer(
            &[complete("BEGIN"), error("42P01", "no such table")],
            b'E',
        ));
        assert!(matches!(result, Err(Error::Server(m)) if m.contains("42P01")));
        assert_eq!(status, Some(TransactionStatus::Failed));
        let (result, status) = decode_status(vec![3u8, b'x']);
        assert!(result.is_err());
        assert_eq!(status, None);
    }

    #[test]
    fn a_capped_result_counts_every_row_in_its_tag() {
        let decoded = decode(answer(
            &[description(&[("id", 20)]), row(&[Some("1")]), complete("SELECT 40")],
            b'I',
        ))
        .unwrap();
        assert_eq!(decoded.results[0].row_count(), Some(40));
        assert_eq!(decoded.results[0].tag(), "SELECT 40");
    }
}
