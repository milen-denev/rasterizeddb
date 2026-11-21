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

/// What a request starts with to be answered in the simple-query form
/// ([`NativeSession::run_simple`]); the statement text follows it.
///
/// Chosen so that a server without this form can never run the text: from
/// 8087ed2e on, the `;` makes the request two statements, which is refused
/// whole before anything reads it; before that, only the first fragment ran,
/// and `\x01SIMPLE` is not a statement. No statement text starts with `\x01`.
/// `rasterizeddb_client` holds the same bytes.
pub(crate) const SIMPLE_QUERY_PREFIX: &[u8] = b"\x01SIMPLE;";

/// The first byte of a simple-query answer: PostgreSQL backend messages
/// follow, ending with `ReadyForQuery`. The native answers are 0 to 3.
pub(crate) const SIMPLE_ANSWER: u8 = 4;

/// The byte every marked request starts with ([`answer_marked`]). No
/// statement text starts with it.
pub(crate) const MARK: u8 = 1;

/// A cancel request: `\x01CANCEL;<token>` stops the statement a simple query
/// of the same user, sent with `cancel=<token>`, is running. Two statements
/// to a server without it, so refused there without anything running.
const CANCEL_PREFIX: &[u8] = b"\x01CANCEL;";

/// What a simple query's header asked for: `\x01SIMPLE:key=value,…;text`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SimpleOptions {
    /// `cancel=<token>`: registered for the length of the query, so a
    /// `CANCEL` request carrying the token (on any connection of the same
    /// user) stops it.
    pub(crate) cancel: Option<String>,
    /// `rows=<n>`: at most `n` rows of each result are sent. The command tag
    /// still counts them all, so a client can say how many it was not sent.
    pub(crate) rows: Option<u64>,
}

/// A cancel token as a client may choose it: 1 to 64 of `[A-Za-z0-9_-]`.
fn valid_token(token: &str) -> bool {
    (1..=64).contains(&token.len())
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Splits a simple query's header from its text: `None` when `request` is not
/// in the simple-query form, and an error for a header this server does not
/// understand, so an option a client relies on is never silently ignored.
pub(crate) fn parse_simple(request: &[u8]) -> Option<Result<(SimpleOptions, &[u8]), String>> {
    // The prefix without its `;`, which a header replaces with `:`.
    let rest = request.strip_prefix(&SIMPLE_QUERY_PREFIX[..SIMPLE_QUERY_PREFIX.len() - 1])?;
    if let Some(text) = rest.strip_prefix(b";") {
        return Some(Ok((SimpleOptions::default(), text)));
    }
    let rest = rest.strip_prefix(b":")?;
    let Some(end) = rest.iter().position(|b| *b == b';') else {
        return Some(Err("the simple-query header has no ';' to end it".to_string()));
    };
    let Ok(header) = std::str::from_utf8(&rest[..end]) else {
        return Some(Err("the simple-query header is not valid UTF-8".to_string()));
    };
    let mut options = SimpleOptions::default();
    for option in header.split(',') {
        let Some((key, value)) = option.split_once('=') else {
            return Some(Err(format!("the simple-query option '{option}' has no '='")));
        };
        match key {
            "cancel" if valid_token(value) => {
                if options.cancel.replace(value.to_string()).is_some() {
                    return Some(Err("the cancel option is given twice".to_string()));
                }
            }
            "cancel" => {
                return Some(Err(
                    "a cancel token is 1 to 64 letters, digits, '_' or '-'".to_string(),
                ));
            }
            "rows" => match value.parse::<u64>() {
                Ok(n) if options.rows.is_none() => options.rows = Some(n),
                Ok(_) => return Some(Err("the rows option is given twice".to_string())),
                Err(_) => {
                    return Some(Err(format!(
                        "the rows option takes a count, not '{value}'"
                    )));
                }
            },
            other => {
                return Some(Err(format!(
                    "this server does not know the simple-query option '{other}'"
                )));
            }
        }
    }
    Some(Ok((options, &rest[end + 1..])))
}

/// Answers a request starting with [`MARK`]: a simple query (with or without
/// a header), or a cancel. `user` is who the connection authenticated as;
/// `session` is the connection's, which a simple query needs.
pub(crate) async fn answer_marked(
    session: Option<&NativeSession>,
    user: &str,
    request: &[u8],
) -> Vec<u8> {
    if let Some(token) = request.strip_prefix(CANCEL_PREFIX) {
        let token = String::from_utf8_lossy(token);
        if !valid_token(&token) {
            return simple_error(
                "22023",
                "a cancel token is 1 to 64 letters, digits, '_' or '-'",
                b'I',
            );
        }
        let stopped = crate::core::cancel::cancel_token(user, &token);
        log::debug!("native CANCEL for a query of {user}: {stopped}");
        return simple_complete(if stopped { "CANCEL 1" } else { "CANCEL 0" });
    }
    let Some(parsed) = parse_simple(request) else {
        return simple_error(
            "0A000",
            "this server does not know the marked request (a request starting with byte 1)",
            b'I',
        );
    };
    let (options, text) = match parsed {
        Ok(parsed) => parsed,
        Err(e) => return simple_error("08P01", &e, b'I'),
    };
    let Ok(text) = std::str::from_utf8(text) else {
        return simple_error("22021", "the statement is not valid UTF-8", b'I');
    };
    match session {
        Some(session) => session.run_simple(text, &options).await,
        None => simple_error(
            "0A000",
            "the simple-query form needs a connection session, which this caller does not have",
            b'I',
        ),
    }
}

/// A simple-query answer holding one `CommandComplete` and an idle
/// `ReadyForQuery`.
fn simple_complete(tag: &str) -> Vec<u8> {
    let mut answer = Vec::with_capacity(tag.len() + 13);
    answer.push(SIMPLE_ANSWER);
    answer.push(b'C');
    answer.extend_from_slice(&((tag.len() + 5) as i32).to_be_bytes());
    answer.extend_from_slice(tag.as_bytes());
    answer.push(0);
    answer.push(b'Z');
    answer.extend_from_slice(&5i32.to_be_bytes());
    answer.push(b'I');
    answer
}

/// A simple-query answer holding one `ErrorResponse` and a `ReadyForQuery`
/// with `status`.
pub(crate) fn simple_error(sqlstate: &str, message: &str, status: u8) -> Vec<u8> {
    let mut body = Vec::with_capacity(message.len() + 24);
    for (field, value) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut answer = Vec::with_capacity(body.len() + 12);
    answer.push(SIMPLE_ANSWER);
    answer.push(b'E');
    answer.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    answer.extend_from_slice(&body);
    answer.push(b'Z');
    answer.extend_from_slice(&5i32.to_be_bytes());
    answer.push(status);
    answer
}

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
    /// made the first time a block opens, or given by [`Self::attach`].
    catalog: std::sync::OnceLock<Arc<DatabaseCatalog>>,
    /// The connection counted among its database's sessions in the server's
    /// catalogue, as a pgwire connection is, until it closes.
    attached: Option<crate::core::database::catalog::SessionGuard>,
    /// The connection's `nextval` results, for `currval` and `lastval`,
    /// across its statements (each statement's session reads them).
    sequence_values:
        StdArc<std::sync::Mutex<crate::core::rql::sequence::session::SessionSequences>>,
    /// The connection's temporary tables, handed to each statement's session
    /// and taken back after it (`temp_tables`).
    temp_tables: std::sync::Mutex<Vec<super::temp_tables::TempTable>>,
    temp_session: u64,
    /// Learned parses of this connection's autocommit `SELECT`s, by template
    /// (`pgwire::text_template`), as a pgwire session keeps them for simple
    /// queries. A lock held only to decide and to learn, never across a
    /// statement: a multiplexed connection's requests run at once.
    text_shapes: std::sync::Mutex<std::collections::HashMap<StdArc<str>, super::TextShape>>,
}

/// What [`NativeSession::text_step`] decided for an autocommit statement.
pub(crate) enum NativeTextStep {
    /// The plain native path, with nothing learned.
    Plain,
    /// The learned parse, with this statement's values, and the description
    /// its rows were learned under (what a retag reads).
    Run {
        template: String,
        rows: Box<crate::core::rql::prepared::PreparedRows>,
        fields: Option<super::LearnedFields>,
        scales: Option<super::LearnedScales>,
        /// What the statement is recorded under (`query_stats::note_shape`).
        #[cfg(feature = "query_stats")]
        stats: StdArc<crate::core::query_stats::ShapeStats>,
    },
    /// The plain native path, watched: hand its capture to
    /// [`NativeSession::text_learn`].
    Watch(NativeWatch),
}

/// A watched statement's template and values, for [`NativeSession::text_learn`].
pub(crate) struct NativeWatch {
    /// The statement itself, described while it is learned.
    query: String,
    template: String,
    literals: Vec<String>,
    classes: Vec<u8>,
    generation: u64,
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
            attached: None,
            sequence_values: StdArc::default(),
            temp_tables: std::sync::Mutex::new(Vec::new()),
            temp_session: fastrand::u64(..),
            text_shapes: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// How an autocommit statement on the native path runs: from a parse
    /// learned for its template, watched while one is learned, or plainly.
    /// The same rules as a pgwire session's simple query (`watch_step`).
    pub(crate) fn text_step(&self, query: &str) -> NativeTextStep {
        use crate::core::rql::prepared as pr;
        if !super::text_shapes_on().load(Ordering::Relaxed) {
            return NativeTextStep::Plain;
        }
        let Some((template, literals)) = super::text_template(query) else {
            return NativeTextStep::Plain;
        };
        let generation = self.database.schema_generation.load(Ordering::Acquire);
        let classes: Option<Vec<u8>> = literals.iter().map(|x| pr::literal_class(x)).collect();
        let literals = Some(literals);
        let mut shapes = self.text_shapes.lock().unwrap_or_else(|e| e.into_inner());
        let prepared = super::text_shape_slot(&mut shapes, &self.database, &template);
        match super::watch_step(prepared, &template, generation, &literals, &classes) {
            super::WatchStep::Text => NativeTextStep::Plain,
            super::WatchStep::Run(run) => {
                NativeTextStep::Run {
                    template,
                    rows: run.rows,
                    fields: run.fields,
                    scales: run.scales,
                    #[cfg(feature = "query_stats")]
                    stats: run.stats,
                }
            }
            super::WatchStep::Watch => match (literals, classes) {
                (Some(literals), Some(classes)) => {
                    NativeTextStep::Watch(NativeWatch { query: query.to_string(), template, literals, classes, generation })
                }
                _ => NativeTextStep::Plain,
            },
        }
    }

    /// What a watched statement's plain run teaches its template. The
    /// native path types its rows from the statement's description only
    /// when a cell needs it, so the description is worked out here, once
    /// per watched run, and learned with the parse: a learned run retags
    /// from it rather than describing the statement again every time (a
    /// table with a NUMERIC, a document or a timestamp needs a retag on every
    /// answer). The verifying run compares it as it compares the rest.
    ///
    /// When the statement just became ready with an index derivation to
    /// check, answers it, for the caller to hand to [`Self::text_check_index`]
    /// once the lock is released.
    pub(crate) fn text_learn(
        &self,
        watch: NativeWatch,
        mut capture: crate::core::rql::prepared::Capture,
    ) -> Option<(String, super::IndexCheck)> {
        let Some(purpose) = crate::core::rql::lexer_s1::recognize_query_purpose(&watch.query) else {
            return None;
        };
        let Ok(description) = super::result_description(&self.database, &purpose) else {
            return None;
        };
        capture.description = Some(description);
        let mut shapes = self.text_shapes.lock().unwrap_or_else(|e| e.into_inner());
        let Some(shape) = shapes.get_mut(watch.template.as_str()) else {
            return None;
        };
        if !std::ptr::eq(shape.generations.as_ptr(), StdArc::as_ptr(&self.database.schema_generation)) {
            return None;
        }
        super::learn_into(
            &self.database,
            &mut shape.prepared,
            &watch.template,
            watch.generation,
            watch.literals,
            watch.classes,
            capture,
        );
        super::pending_index(&shape.prepared).map(|check| (watch.template, check))
    }

    /// Checks a template's pending index derivation (`text_learn`'s answer)
    /// with no lock held, and records the answer if the template still
    /// holds that derivation.
    pub(crate) async fn text_check_index(&self, template: String, check: super::IndexCheck) {
        let ok = Box::pin(super::check_index(&self.database, &check)).await;
        let mut shapes = self.text_shapes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(shape) = shapes.get_mut(template.as_str()) {
            super::settle_index(&self.database, &mut shape.prepared, &check, ok);
        }
    }

    /// A learned parse met a table reshaped since (`prepared::STALE_PLAN`):
    /// its template is learned again.
    pub(crate) fn text_forget(&self, template: &str) {
        let mut shapes = self.text_shapes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(shape) = shapes.get_mut(template) {
            shape.prepared = super::PreparedSelect::Unknown;
        }
    }

    /// This connection, counted in `catalog` (the server's) among its
    /// database's sessions for as long as it is open, and resolving its
    /// statements against a view of that database which shares the count.
    /// Without it a native connection was never counted: `SHOW DATABASES`
    /// said `sessions` 0 for a database native clients were using, and
    /// `DROP DATABASE` did not see them.
    pub(crate) fn attach(mut self, catalog: &DatabaseCatalog) -> Self {
        let name = self.database.name.clone();
        if let (Some(guard), Some(view)) = (catalog.connect(&name), catalog.view_of(&name)) {
            self.attached = Some(guard);
            let _ = self.catalog.set(view);
        }
        self
    }

    /// Whether `query` needs pgwire's statement path, not the native
    /// autocommit one: a CTE or derived table, a view, a statement of the
    /// objects kept above the engine (views, sequences, routines, triggers,
    /// MERGE, temporary tables, grants), a call of a user's function or a
    /// sequence function, a write to a table with triggers, a temporary
    /// table, or a correlated subquery. Such a statement runs through
    /// [`Self::run_alone`].
    pub(crate) async fn needs_session_path(&self, query: &str) -> bool {
        let database = &self.database;
        let stmt = query.trim();
        // The schema-object statements below each begin with CREATE, DROP,
        // ALTER, CALL, MERGE, GRANT or REVOKE, so a statement that begins
        // with a reading or writing verb is none of them. Each parser
        // tokenized or uppercased the whole text before it looked at its
        // first word: an eighth of a native point query.
        let schema_object = || {
            crate::core::rql::views::parse_view_statement(stmt).is_some()
                || crate::core::rql::sequence_catalog::parse_sequence_statement(stmt).is_some()
                || crate::core::rql::functions::parse_routine_statement(stmt).is_some()
                || crate::core::rql::triggers::parse_trigger_statement(stmt).is_some()
                || crate::core::rql::merge::parse_merge(stmt).is_some()
                || crate::core::rql::privileges::parse_privilege_statement(stmt).is_some()
        };
        // A correlated scalar subquery and a derived table are both written
        // in parentheses: a statement with none has neither, and is not
        // searched for them keyword by keyword.
        let parenthesised = stmt.contains('(');
        if (parenthesised && crate::core::rql::correlated::rewrite_select(stmt).is_some())
            || crate::core::rql::cte::has_with_clause(stmt)
            || (parenthesised && crate::core::rql::cte::next_derived_table(stmt).is_some())
            || (!begins_with_data_verb(stmt) && schema_object())
            // `current_database()`, `current_user`, `current_setting(…)`:
            // read from the session, which the native path has none of.
            || super::session_values::names_any(stmt)
        {
            return true;
        }
        // The first forty bytes, cut back to a character boundary: only its
        // first word and short ASCII prefixes (`CREATE TEMP`) are read.
        let mut head_end = stmt.len().min(40);
        while !stmt.is_char_boundary(head_end) {
            head_end -= 1;
        }
        let upper_head = stmt[..head_end].to_ascii_uppercase();
        if upper_head.starts_with("CREATE TEMP")
            || upper_head.starts_with("CREATE LOCAL TEMP")
            || upper_head.starts_with("CREATE GLOBAL TEMP")
        {
            return true;
        }
        let has = |needle: &[u8]| crate::core::sql_scan::contains_ignore_ascii_case(stmt, needle);
        // The four sequence functions all hold `val`: one scan of a statement
        // that names none of them, where there were five.
        if (has(b"val") && (has(b"nextval") || has(b"currval") || has(b"setval") || has(b"lastval")))
            || has(b"pg_get_serial_sequence")
        {
            return true;
        }
        {
            let temps = self.temp_tables.lock().unwrap_or_else(|e| e.into_inner());
            if super::temp_tables::rewrite(&temps, stmt).is_some() {
                return true;
            }
        }
        if crate::core::rql::views::may_name_a_view(database, stmt).await
            || crate::core::rql::functions::may_call_a_routine(database, stmt).await
        {
            return true;
        }
        // A privilege check (a non-superuser's statement), or the owner a
        // new relation is given. A superuser's other statements stay on the
        // native path.
        if database.has_table(crate::core::rql::privileges::ACL_TABLE)
            && ((self.authenticated && !self.superuser) || upper_head.starts_with("CREATE"))
        {
            return true;
        }
        if database.has_table(crate::core::rql::triggers::TRIGGER_TABLE) {
            let head = upper_head.split_whitespace().next().unwrap_or("");
            if matches!(head, "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE") {
                for r in crate::core::rql::lock_set::relation_references(stmt) {
                    if r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare
                        && crate::core::rql::triggers::may_have_triggers(database, &r.name).await
                    {
                        return true;
                    }
                }
                if head == "TRUNCATE" {
                    return true;
                }
            }
        }
        false
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
        self.keep_temp_tables(state);

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
        self.keep_temp_tables(&state);
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

    /// [`SIMPLE_ANSWER`], for the native statement tests.
    #[cfg(test)]
    pub(crate) const SIMPLE_ANSWER_FOR_TESTS: u8 = SIMPLE_ANSWER;

    /// A request in the simple-query form ([`SIMPLE_QUERY_PREFIX`]): `query`
    /// runs exactly as a PostgreSQL client's simple query does
    /// (`handle_simple_query`), and the answer is [`SIMPLE_ANSWER`] followed
    /// by the PostgreSQL messages it wrote, `ReadyForQuery` included. So the
    /// caller gets what pgwire would have sent it: each result's column names
    /// and type OIDs, every cell as PostgreSQL's text, each statement's
    /// command tag, errors with their SQLSTATE, notices, and several
    /// statements in one request as one implicit transaction.
    ///
    /// On `rastcp/1` the request is the connection's: a `BEGIN` in it opens
    /// the connection's block, which later requests of either form run in.
    /// On `rastcp/2`, which unrelated callers share, the request gets a
    /// session of its own, so a block must end within it; one left open is
    /// rolled back and the answer ends with an error saying so.
    pub(crate) async fn run_simple(&self, query: &str, options: &SimpleOptions) -> Vec<u8> {
        if self.poisoned.load(Ordering::Acquire) {
            return simple_error(
                "08006",
                "this connection's transaction was rolled back by the server after an internal error; \
                 open a new connection",
                b'I',
            );
        }
        // Held to the end of the query, so a CANCEL finds it for exactly that
        // long.
        let registration = match &options.cancel {
            Some(token) => match crate::core::cancel::register_token(&self.user, token) {
                Some(registration) => Some(registration),
                None => {
                    return simple_error(
                        "08P01",
                        "another query of this user is still running under the same cancel token: \
                         choose a fresh token for each query",
                        if self.in_block.load(Ordering::Acquire) { b'T' } else { b'I' },
                    );
                }
            },
            None => None,
        };
        let run = SimpleRun {
            cancel: registration.as_ref().map(|r| r.session()),
            rows: options.rows,
        };
        if self.multiplexed {
            let mut state = self.new_state();
            let mut answer = Box::pin(self.simple_in(&mut state, query, &run)).await;
            if let Some(t) = state.transaction.take() {
                state.end_block_settings(&t);
                self.database.block_ddl_abort(t.id).await;
                t.abort_unless_aborted(&self.database);
                t.owner.release();
                answer.extend_from_slice(&simple_error(
                    "0A000",
                    "the transaction was rolled back: a multiplexed (rastcp/2) connection is shared by many \
                     callers, so a transaction must end in the request that began it. Open a rastcp/1 \
                     connection to keep one open between requests",
                    b'I',
                )[1..]);
            }
            return answer;
        }
        let Ok(mut slot) = self.state.try_lock() else {
            return simple_error(
                "08P01",
                "another request on this connection is still running inside its transaction: a \
                 transaction's statements must be sent one at a time, each after the answer to the last",
                b'T',
            );
        };
        let state = slot.get_or_insert_with(|| self.new_state());
        let answer = Box::pin(self.simple_in(state, query, &run)).await;
        let open = state.transaction.is_some();
        if !open {
            *slot = None;
        }
        self.in_block.store(open, Ordering::Release);
        answer
    }

    async fn simple_in(&self, state: &mut SessionState, query: &str, run: &SimpleRun) -> Vec<u8> {
        // As the message loop sets them for a `Query` message.
        state.message_us = crate::core::statement_clock::wall_us();
        state.error_in_batch = false;
        state.native_capture = false;
        state.native_result = None;
        let mut sink = NativeSink::simple(run.rows);
        let ran = match &run.cancel {
            // Each statement publishes itself in the slot as it starts
            // (`execute_recorded`), as a pgwire session's statements do.
            Some(cancel) => {
                super::SESSION_CANCEL
                    .scope(
                        cancel.clone(),
                        handle_simple_query(&self.database, state, &mut sink, query),
                    )
                    .await
            }
            None => handle_simple_query(&self.database, state, &mut sink, query).await,
        };
        state.error_in_batch = false;
        state.statement_failed = false;
        match ran {
            Ok(()) => sink.out,
            Err(e) => {
                // What ends a pgwire session (see `run_in`) poisons this
                // connection, with the messages so far kept and the failure
                // after them.
                if let Some(t) = state.transaction.take() {
                    log::warn!(
                        "a native simple query failed internally ({e}); rolling back transaction {}",
                        t.id
                    );
                    state.end_block_settings(&t);
                    self.database.block_ddl_abort(t.id).await;
                    t.abort_unless_aborted(&self.database);
                    t.owner.release();
                    self.poisoned.store(true, Ordering::Release);
                }
                sink.out
                    .extend_from_slice(&simple_error("XX000", &e.to_string(), b'I')[1..]);
                sink.out
            }
        }
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
        let mut state = SessionState::new(
            catalog,
            self.database.name.clone(),
            AuthConfig::default(),
            Principal {
                name: self.user.clone(),
                superuser: self.superuser,
                authenticated: self.authenticated,
            },
            ConnectionLimits::default(),
        );
        // The connection's, not the statement's.
        state.sequence_values = self.sequence_values.clone();
        state.temp_tables = self
            .temp_tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        state.temp_session = self.temp_session;
        state
    }

    /// The connection's sequence values, for a statement on the native
    /// autocommit path.
    pub(crate) fn sequence_scope(&self) -> crate::core::rql::sequence::session::Scope {
        crate::core::rql::sequence::session::Scope {
            database: self.database.id,
            values: self.sequence_values.clone(),
        }
    }

    /// Takes back the temporary tables a statement's session made or dropped.
    fn keep_temp_tables(&self, state: &SessionState) {
        *self.temp_tables.lock().unwrap_or_else(|e| e.into_inner()) = state.temp_tables.clone();
    }
}

impl Drop for NativeSession {
    /// The connection is gone and nothing of it is running: an open block is
    /// rolled back, as `run_message_loop` rolls back a PostgreSQL session's.
    fn drop(&mut self) {
        // The connection's temporary tables go with it.
        let temps = std::mem::take(
            self.temp_tables
                .get_mut()
                .unwrap_or_else(|e| e.into_inner()),
        );
        if !temps.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let database = self.database.clone();
            runtime.spawn(async move {
                for t in temps {
                    let sql = format!(
                        "DROP TABLE IF EXISTS {}",
                        crate::core::rql::views::quote_ident(&t.real)
                    );
                    let _ = crate::core::rql::executor::run_statement(
                        &sql,
                        &database,
                        crate::core::mvcc::transaction::FROZEN_TX,
                    )
                    .await;
                }
            });
        }
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
    /// A simple query's row cap: past it, a result's `DataRow`s are dropped
    /// as they are written. `None` keeps everything.
    cap: Option<u64>,
    /// Where the next message not yet looked at starts in `out`.
    scan: usize,
    /// The current result's rows kept so far.
    kept: u64,
}

/// What a simple query was sent with, past its header.
struct SimpleRun {
    cancel: Option<std::sync::Arc<crate::core::cancel::SessionCancel>>,
    rows: Option<u64>,
}

impl NativeSink {
    /// A sink for a simple-query answer: [`SIMPLE_ANSWER`], then the messages,
    /// with at most `cap` rows kept of each result.
    fn simple(cap: Option<u64>) -> Self {
        NativeSink {
            out: vec![SIMPLE_ANSWER],
            cap,
            scan: 1,
            kept: 0,
        }
    }

    /// Walks the complete messages written since the last call, dropping the
    /// rows past the cap. A message split across two writes waits for the
    /// rest.
    fn trim(&mut self) {
        let Some(cap) = self.cap else { return };
        while self.out.len() >= self.scan + 5 {
            let tag = self.out[self.scan];
            let len = i32::from_be_bytes(self.out[self.scan + 1..self.scan + 5].try_into().unwrap());
            let total = 1 + len.max(4) as usize;
            if self.out.len() < self.scan + total {
                return;
            }
            match tag {
                b'T' => {
                    self.kept = 0;
                    self.scan += total;
                }
                b'D' if self.kept >= cap => {
                    self.out.drain(self.scan..self.scan + total);
                }
                b'D' => {
                    self.kept += 1;
                    self.scan += total;
                }
                _ => self.scan += total,
            }
        }
    }
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
        self.trim();
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
    /// Notices and warnings, by message. Not sent on the native protocol;
    /// read by pgwire's own sessions and tests.
    pub(super) notices: Vec<String>,
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
            // the native protocol has nowhere to put them, but its reader
            // keeps them for the callers that do.
            b'N' => {
                let mut p = 0usize;
                while p < body.len() && body[p] != 0 {
                    let field = body[p];
                    p += 1;
                    let value = cstring(body, &mut p)?;
                    if field == b'M' {
                        said.notices.push(value);
                    }
                }
            }
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

/// Whether `stmt`'s first word is a verb that reads or writes rows
/// (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `WITH`, `VALUES`, `TABLE`), which
/// no schema-object statement begins with. Anything else, a leading comment
/// or parenthesis included, answers false and is looked at in full.
pub(crate) fn begins_with_data_verb(stmt: &str) -> bool {
    let word = stmt
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "TABLE"]
        .iter()
        .any(|verb| word.eq_ignore_ascii_case(verb))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What lets `needs_session_path` skip the schema-object parsers: none
    /// of them recognises a statement that begins with a data verb, and
    /// every statement one of them recognises is looked at in full.
    #[test]
    fn no_schema_object_statement_begins_with_a_data_verb() {
        let recognised = |sql: &str| {
            crate::core::rql::views::parse_view_statement(sql).is_some()
                || crate::core::rql::sequence_catalog::parse_sequence_statement(sql).is_some()
                || crate::core::rql::functions::parse_routine_statement(sql).is_some()
                || crate::core::rql::triggers::parse_trigger_statement(sql).is_some()
                || crate::core::rql::merge::parse_merge(sql).is_some()
                || crate::core::rql::privileges::parse_privilege_statement(sql).is_some()
        };
        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "create or replace view v as select a from t",
            "DROP MATERIALIZED VIEW IF EXISTS v",
            "CREATE SEQUENCE s START 5",
            "ALTER SEQUENCE s RESTART",
            "DROP SEQUENCE s",
            "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
            "DROP PROCEDURE p",
            "CALL p(1)",
            "CREATE TRIGGER x BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
            "DROP TRIGGER IF EXISTS x ON t",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "GRANT SELECT ON t TO r",
            "REVOKE ALL ON t FROM r",
            "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO r",
        ] {
            assert!(recognised(sql), "the list is stale: {sql} is not a schema-object statement");
            assert!(!begins_with_data_verb(sql), "{sql}");
        }
        for sql in [
            "SELECT * FROM t WHERE id = 1",
            "select merge, grant from t",
            "  Select(1)",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "VALUES (1), (2)",
            "TABLE t",
        ] {
            assert!(begins_with_data_verb(sql), "{sql}");
            assert!(!recognised(sql), "{sql} is a schema-object statement after all");
        }
        // Anything not plainly a verb is looked at in full.
        for sql in ["/* note */ CREATE VIEW v AS SELECT 1", "(SELECT 1)", "", "SELECTED"] {
            assert!(!begins_with_data_verb(sql), "{sql}");
        }
    }

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
