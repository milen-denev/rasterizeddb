//! One SQL Server connection: PRELOGIN, TLS, LOGIN7, then requests — SQL
//! batches, RPC calls and transaction-manager requests.
//!
//! **Errors.** In SQL Server an error ends the statement, not the batch or
//! the transaction (unless `XACT_ABORT` is on): the batch goes on with its
//! next statement. Inside a transaction each statement therefore runs under a
//! savepoint, rolled back to if it fails; a deadlock rolls the whole
//! transaction back, as it does there.
//!
//! **Transactions.** `BEGIN TRAN` nests (`@@TRANCOUNT`); only the outermost
//! COMMIT commits, and ROLLBACK ends them all. The client is told when a
//! transaction begins and ends with ENVCHANGE tokens carrying its descriptor,
//! which drivers track.

use std::collections::HashMap;
use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;

use rclite::Arc;
use tokio::io::AsyncWriteExt;

use crate::core::database::Database;
use crate::pgwire::proxied::{
    BridgeOptions, OpenRefused, PgBridge, PgField, PgNotice, ProxiedLogin, data_row_cells,
    parse_row_description, tag_count,
};
use crate::sqlcompat::{Tok, Token, find_top, ident_sql, kw, split_commas};

use super::lex;
use super::routines::{self, ProcDef};
use super::tds::{self, ColType, Column, Message, ParamValue, Reader, TdsStream, Wire};
use super::translate::{self, Env, Forward, Kind, ParamSlot, Stmt, TsqlError, Value};
use super::{MsAuth, MsTls, Shared};

const SERVER_NAME: &str = "rasterizeddb";
const STATEMENT_SAVEPOINT: &str = "rdb_tds_statement";

const VERSION_TEXT: &str =
    "Microsoft SQL Server 2022 (RTM) - 16.0.4000.0 (X64) \n\tRasterizedDB (SQL Server protocol)";

/// SQL Server's system databases are the default database here: a client
/// connects to `master` to create or drop its own.
fn resolve_database(shared: &Shared, name: &str) -> String {
    if name.is_empty()
        || matches!(
            name.to_ascii_lowercase().as_str(),
            "master" | "model" | "msdb" | "tempdb"
        )
    {
        return shared.catalog.default_name().to_string();
    }
    // SQL Server compares database names without case, and `CREATE DATABASE
    // [DenevColors]` made `denevcolors`: a login naming `DenevColors` must
    // open it.
    if shared.catalog.get(name).is_some() {
        return name.to_string();
    }
    shared
        .catalog
        .names()
        .into_iter()
        .find(|n| n.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| name.to_string())
}

/// What a statement leaves the batch doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flow {
    Next,
    Return,
    Break,
    Continue,
    /// XACT_ABORT, or an error that ends the batch.
    Abort,
}

/// A declared variable.
#[derive(Clone, Debug)]
struct Var {
    value: Value,
    string: bool,
    /// The declared type, for converting assigned values.
    numeric: bool,
}

/// The parameters of an `sp_executesql` (or prepared) statement.
/// The spellings tables and columns were declared with, read from
/// `translate::NAMES_TABLE`: per relation its own (`""`) and each column's,
/// all keyed folded. Read again after this session's DDL, on another
/// database, or after `NAMES_TTL` (another session's DDL).
#[derive(Default)]
struct NameCache {
    database: usize,
    read: Option<std::time::Instant>,
    dirty: bool,
    relations: HashMap<String, HashMap<String, String>>,
    /// A column's spelling over every relation, where they agree (`None`
    /// where two relations spell it differently).
    columns: HashMap<String, Option<String>>,
    /// A constraint's or index's spelling, by the engine's name for it.
    objects: HashMap<String, String>,
}

/// `translate::NAMES_TABLE`'s `rel` for databases' names, in the default
/// database.
const DATABASES_REL: &str = "#databases";

const NAMES_TTL: std::time::Duration = std::time::Duration::from_secs(5);

impl NameCache {
    /// A result column's name as declared: an all-lower-case name the
    /// engine reports (`SELECT *`, a plain column) shown with the spelling
    /// every relation declared it with; any other name as it is.
    fn column_shown(&self, name: &str) -> String {
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            return name.to_string();
        }
        match self.columns.get(name) {
            Some(Some(shown)) => shown.clone(),
            _ => name.to_string(),
        }
    }

    fn shown(&self, relation: &str, column: &str) -> Option<String> {
        self.relations.get(relation)?.get(column).cloned()
    }

    /// A relation, constraint or column name an engine message quotes, as
    /// declared: its dots real ones (`lex::NAME_DOT`).
    fn table_shown(&self, relation: &str) -> String {
        self.shown(relation, "").unwrap_or_else(|| lex::decode_dots(relation).into_owned())
    }

    fn object_shown(&self, name: &str) -> String {
        self.objects.get(name).cloned().unwrap_or_else(|| lex::decode_dots(name).into_owned())
    }

    fn column_of(&self, relation: &str, column: &str) -> String {
        self.shown(relation, column).unwrap_or_else(|| lex::decode_dots(column).into_owned())
    }
}

/// A declared parameter: its name (without @), whether it is a string, the
/// engine type sent for it (0: inferred), and whether it is a number.
type Decl = (String, bool, u32, bool);

struct ParamCtx {
    /// (name without @, string type, engine type)
    decls: Vec<Decl>,
    values: Vec<ParamValue>,
    /// The declarations the statement being translated used, in `$n` order.
    used: parking_lot::Mutex<Vec<usize>>,
}

/// A prepared handle's statement.
#[derive(Clone)]
struct Prepared {
    sql: String,
    decls: Vec<Decl>,
}

/// What ends with a batch, a piece of dynamic SQL or a procedure.
#[derive(Default)]
struct LocalScope {
    /// Whether `#tables` made here are dropped when it ends (dynamic SQL and
    /// procedures); a batch's last for the connection.
    owns_temps: bool,
    /// Temporary tables to drop at the end: table variables, and the
    /// `#tables` made here when it owns them.
    drop: Vec<String>,
    /// `@name` → the temporary table holding it.
    table_vars: HashMap<String, String>,
    local_cursors: Vec<String>,
}

/// A declared cursor; its rows are read when it is opened.
struct Cursor {
    select: Vec<Token>,
    scroll: bool,
    open: Option<CursorRows>,
}

struct CursorRows {
    fields: Vec<PgField>,
    rows: Vec<Vec<Option<String>>>,
    /// -1 before the first row, `rows.len()` after the last.
    pos: i64,
}

/// An argument of EXEC: its parameter name if named, its value (`None` for
/// DEFAULT), and whether it asks for the parameter's value back (OUTPUT).
struct ProcArg {
    name: Option<String>,
    value: Option<Value>,
    output: bool,
}

struct ProcOutcome {
    status: i32,
    /// (argument index, value) of each OUTPUT argument.
    outputs: Vec<(usize, Value)>,
    flow: Flow,
}

enum Outcome {
    Rows {
        count: u64,
    },
    Done {
        tag: String,
        identity: Option<i64>,
    },
    /// The statement failed; `started` if its COLMETADATA had been sent.
    Failed(PgNotice, bool),
}

struct Session {
    wire: Wire,
    shared: StdArc<Shared>,
    login: ProxiedLogin,
    host: String,
    app: String,
    bridge: Option<PgBridge>,
    vars: HashMap<String, Var>,
    nocount: bool,
    xact_abort: bool,
    implicit_transactions: bool,
    trancount: u32,
    savepoints: Vec<String>,
    descriptor: u64,
    next_descriptor: u64,
    last_identity: Option<i64>,
    rowcount: i64,
    last_error: Option<TsqlError>,
    error_number: i32,
    prepared: HashMap<i32, Prepared>,
    next_handle: i32,
    /// Inside an RPC: statement results end with DONEINPROC.
    in_rpc: bool,
    /// Where the status of the last DONE written is, to clear its MORE bit
    /// when it turns out to be the batch's last.
    last_done: Option<usize>,
    try_depth: usize,
    peer_watch: Option<StdArc<crate::pgwire::PeerWatch>>,
    /// @@NESTLEVEL: how deep in procedure calls.
    nest_level: u32,
    /// What the running procedure's RETURN said.
    return_value: Option<i32>,
    scopes: Vec<LocalScope>,
    cursors: HashMap<String, Cursor>,
    fetch_status: i32,
    temp_seq: u64,
    /// `#tables` made at batch level: they last until the connection is
    /// reset or closed.
    session_temps: Vec<String>,
    /// The database's procedures (name, definition), read when a batch
    /// looks at the catalogue.
    routine_cache: Option<Vec<(String, String)>>,
    /// OUTPUT parameters of the RPC call running: (ordinal, name, engine
    /// type, value), sent after its return status.
    rpc_outputs: Vec<(u16, String, u32, Value)>,
    /// What this connection's `#tables` are named with.
    temp_prefix: String,
    /// SET FMTONLY ON: queries answer their columns and no rows; nothing
    /// else runs.
    fmtonly: bool,
    /// The table an `INSERT BULK` named, which the next bulk load fills.
    bulk_target: Option<String>,
    /// This connection, as the owner of its `sp_getapplock` locks.
    conn_id: u64,
    /// Whether each `$n` of the statement being forwarded is declared a
    /// number (`inline_params` writes those bare, everything else quoted).
    bound_numbers: Vec<bool>,
    /// The declared spellings of tables and columns (`translate::NAMES_TABLE`).
    names: NameCache,
    /// The statement being forwarded's explicit aliases
    /// (`Forward::aliases`): kept as typed in its COLMETADATA.
    aliases: Vec<String>,
    /// The statement being forwarded's columns read with their 100 ns
    /// digit (`Forward::ticks`): sent as their SQL Server time type.
    ticks: Vec<(String, &'static str)>,
    /// The statement being forwarded's verb (`INSERT`, `UPDATE`, …), as a
    /// constraint violation's message names it ([`Session::engine_error`]).
    verb: &'static str,
    /// The current database's name as CREATE DATABASE declared it
    /// (`Shared::database_names`); empty until read.
    database_shown: String,
    /// The database the login named, as it named it: a pooled connection's
    /// reset goes back to it, as SQL Server's does after a `USE`.
    login_shown: String,
}

impl Drop for Session {
    fn drop(&mut self) {
        super::applock::release_all(self.conn_id, None);
    }
}

pub(crate) async fn serve_connection(
    base: tds::Base,
    peer: String,
    shared: StdArc<Shared>,
    over_limit: bool,
) -> io::Result<()> {
    #[cfg(unix)]
    let watch = {
        use std::os::fd::AsFd;
        match &base {
            tds::Base::Tcp(s) => crate::pgwire::proxied::peer_watch_of(s.as_fd()),
            tds::Base::Unix(s) => crate::pgwire::proxied::peer_watch_of(s.as_fd()),
        }
    };
    #[cfg(not(unix))]
    let watch = None;
    let mut wire = Wire::new(TdsStream::Plain(tds::Shim::new(base, Vec::new())));
    let deadline = shared.config.limits.handshake_timeout;
    let started = tokio::time::timeout(deadline, handshake(&mut wire, &shared, over_limit)).await;
    let (login_packet, login) = match started {
        Ok(Ok(Some(done))) => done,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(()),
    };
    let database = resolve_database(&shared, &login_packet.database).to_string();
    let shown = if login_packet.database.is_empty() {
        database.clone()
    } else {
        login_packet.database.clone()
    };
    let conn_id = crate::sqlcompat::next_connection_id();
    let mut session = Session {
        wire,
        shared: shared.clone(),
        login,
        host: login_packet.host.clone(),
        app: login_packet.app.clone(),
        bridge: None,
        vars: HashMap::new(),
        nocount: false,
        xact_abort: false,
        implicit_transactions: false,
        trancount: 0,
        savepoints: Vec::new(),
        descriptor: 0,
        next_descriptor: 1,
        last_identity: None,
        rowcount: 0,
        last_error: None,
        error_number: 0,
        prepared: HashMap::new(),
        next_handle: 1,
        in_rpc: false,
        last_done: None,
        try_depth: 0,
        peer_watch: watch,
        nest_level: 0,
        return_value: None,
        scopes: Vec::new(),
        cursors: HashMap::new(),
        fetch_status: -1,
        temp_seq: 0,
        session_temps: Vec::new(),
        routine_cache: None,
        rpc_outputs: Vec::new(),
        temp_prefix: crate::sqlcompat::temp_table_prefix("tds", conn_id),
        fmtonly: false,
        bulk_target: None,
        conn_id,
        login_shown: shown.clone(),
        bound_numbers: Vec::new(),
        names: NameCache::default(),
        aliases: Vec::new(),
        ticks: Vec::new(),
        verb: "INSERT",
        database_shown: String::new(),
    };
    let packet_size = match login_packet.packet_size {
        0 => tds::DEFAULT_PACKET,
        n => (n as usize).clamp(512, tds::MAX_PACKET),
    };
    match session.open(&database).await {
        Ok(()) => {
            let out = session.wire.reply();
            tds::env_change_text(out, 1, &shown, "master");
            tds::message(
                out,
                false,
                5701,
                2,
                0,
                &format!("Changed database context to '{shown}'."),
                SERVER_NAME,
                1,
            );
            tds::env_change_bytes(out, 7, &tds::COLLATION, &[]);
            tds::env_change_text(out, 2, "us_english", "");
            tds::login_ack(out);
            tds::env_change_text(
                out,
                4,
                &packet_size.to_string(),
                &tds::DEFAULT_PACKET.to_string(),
            );
            if login_packet.wants_features {
                // FEATUREEXTACK: no feature acknowledged.
                out.extend_from_slice(&[0xae, 0xff]);
            }
            tds::done(out, tds::TOKEN_DONE, 0, 0, 0);
            session.wire.send(tds::PT_REPLY).await?;
            session.wire.packet_size = packet_size;
            session.wire.max_message = tds::MAX_MESSAGE;
        }
        Err(e) => {
            let out = session.wire.reply();
            tds::message(out, true, e.number, e.state, 14, &e.message, SERVER_NAME, 1);
            tds::done(out, tds::TOKEN_DONE, tds::DONE_ERROR, 0, 0);
            session.wire.send(tds::PT_REPLY).await?;
            return Ok(());
        }
    }
    let result = session.request_loop().await;
    session.drop_temps_at_close().await;
    if let Some(bridge) = session.bridge.take() {
        bridge.terminate().await;
    }
    let _ = session.wire.stream.shutdown().await;
    let _ = peer;
    result
}

/// PRELOGIN, TLS and LOGIN7. `None` when the client was refused (and told).
async fn handshake(
    wire: &mut Wire,
    shared: &StdArc<Shared>,
    over_limit: bool,
) -> io::Result<Option<(tds::Login, ProxiedLogin)>> {
    let tls_available = shared.acceptor.is_some();
    let first = wire.peek_byte().await?;
    if first == 0x16 {
        // TDS 8.0: TLS first, then PRELOGIN inside it.
        let Some(acceptor) = shared.acceptor.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TDS 8.0 client, but TLS is not configured",
            ));
        };
        upgrade(wire, acceptor, true).await?;
        let prelogin = wire.read().await?;
        if prelogin.kind != tds::PT_PRELOGIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PRELOGIN was expected",
            ));
        }
        wire.reply()
            .extend_from_slice(&tds::prelogin_reply(tds::ENCRYPT_ON));
        wire.send(tds::PT_REPLY).await?;
    } else {
        let prelogin = wire.read().await?;
        if prelogin.kind != tds::PT_PRELOGIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PRELOGIN was expected",
            ));
        }
        let client = tds::prelogin_encryption(&prelogin.payload);
        let (reply, encrypt) = if !tls_available || wire.stream.is_unix() {
            (tds::ENCRYPT_NOT_SUP, false)
        } else if client == tds::ENCRYPT_NOT_SUP {
            if shared.config.tls == MsTls::Require {
                (tds::ENCRYPT_REQ, false)
            } else {
                (tds::ENCRYPT_NOT_SUP, false)
            }
        } else if client == tds::ENCRYPT_OFF {
            // The client did not ask for encryption. Login-only encryption is
            // not offered, so under `require` the whole connection is
            // encrypted, and otherwise none of it (as pgwire's `prefer`).
            if shared.config.tls == MsTls::Require {
                (tds::ENCRYPT_REQ, true)
            } else {
                (tds::ENCRYPT_NOT_SUP, false)
            }
        } else {
            (tds::ENCRYPT_ON, true)
        };
        wire.reply().extend_from_slice(&tds::prelogin_reply(reply));
        wire.send(tds::PT_REPLY).await?;
        if reply == tds::ENCRYPT_REQ && !encrypt {
            return Ok(None);
        }
        if encrypt {
            upgrade(wire, shared.acceptor.clone().unwrap(), false).await?;
        }
    }

    let message = wire.read().await?;
    if message.kind != tds::PT_LOGIN7 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "LOGIN7 was expected",
        ));
    }
    let login = tds::parse_login7(&message.payload)?;
    let refuse = |wire: &mut Wire, number: i32, text: &str| {
        let out = wire.reply();
        tds::message(out, true, number, 1, 14, text, SERVER_NAME, 1);
        tds::done(out, tds::TOKEN_DONE, tds::DONE_ERROR, 0, 0);
    };
    if over_limit {
        refuse(
            wire,
            17809,
            "Could not connect because the maximum number of user connections has already been reached.",
        );
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    if shared.config.tls == MsTls::Require && !wire.stream.is_tls() && !wire.stream.is_unix() {
        refuse(
            wire,
            18456,
            "Login failed: this server requires an encrypted connection.",
        );
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    if login.integrated {
        refuse(
            wire,
            18452,
            "Login failed. Windows authentication is not supported; use SQL Server authentication.",
        );
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    let database_name = resolve_database(shared, &login.database).to_string();
    let Some(database) = shared.catalog.get(&database_name) else {
        refuse(
            wire,
            4060,
            &format!(
                "Cannot open database \"{database_name}\" requested by the login. The login failed."
            ),
        );
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    };
    let method = if wire.stream.is_unix() {
        shared
            .config
            .socket
            .as_ref()
            .and_then(|s| s.auth)
            .unwrap_or(shared.config.auth)
    } else {
        shared.config.auth
    };
    let principal = match method {
        // As the named role, with its attributes (`auth::trust_login`).
        MsAuth::Trust => match crate::auth::trust_login(&database, &login.user, Some(&shared.config.bootstrap_user)).await {
            Ok(superuser) => ProxiedLogin {
                user: login.user.clone(),
                superuser,
                authenticated: false,
                database: database_name.clone(),
                verifier: None,
                bootstrap_user: Some(shared.config.bootstrap_user.clone()),
                upstream_password: None,
            },
            Err(_) => {
                refuse(wire, 18456, &format!("Login failed for user '{}'.", login.user));
                wire.send(tds::PT_REPLY).await?;
                return Ok(None);
            }
        },
        // A front-end node (B4.7) holds no roles: the login's password goes
        // to the gateway, which checks it.
        MsAuth::Password if crate::pgwire::proxied::upstream().is_some() => ProxiedLogin {
            user: login.user.clone(),
            superuser: false,
            authenticated: true,
            database: database_name.clone(),
            verifier: None,
            bootstrap_user: None,
            upstream_password: Some(crate::pgwire::proxied::Secret(login.password.clone())),
        },
        MsAuth::Password => {
            let role = crate::auth::lookup_role(&database, &login.user).await;
            let verifier = role
                .as_ref()
                .and_then(|r| r.verifier.clone())
                .unwrap_or(crate::auth::Verifier::Plain(String::new()));
            let may = role.as_ref().is_some_and(|r| r.can_login);
            if !may || !crate::auth::verify_password(&verifier, &login.user, &login.password) {
                refuse(
                    wire,
                    18456,
                    &format!("Login failed for user '{}'.", login.user),
                );
                wire.send(tds::PT_REPLY).await?;
                return Ok(None);
            }
            // RD3: an md5 verifier's login is warned about.
            crate::auth::warn_if_md5_login(Some(&verifier), &login.user, &database_name, "tds");
            ProxiedLogin {
                user: login.user.clone(),
                superuser: role.as_ref().map(|r| r.is_superuser).unwrap_or(false),
                authenticated: true,
                database: database_name.clone(),
                verifier: role.and_then(|r| r.verifier).map(|v| v.to_stored()),
                bootstrap_user: None,
                upstream_password: None,
            }
        }
    };
    Ok(Some((login, principal)))
}

/// TLS over the connection: inside PRELOGIN packets for the handshake
/// (TDS 7.x), or directly (TDS 8.0).
async fn upgrade(
    wire: &mut Wire,
    acceptor: tokio_rustls::TlsAcceptor,
    strict: bool,
) -> io::Result<()> {
    let buffered = wire.take_buffered();
    let stream = std::mem::replace(&mut wire.stream, TdsStream::Upgrading);
    let TdsStream::Plain(mut shim) = stream else {
        return Err(io::Error::other("TLS asked for twice"));
    };
    if !buffered.is_empty() {
        shim = tds::Shim::new(shim.base, buffered);
    }
    shim.passthrough = strict;
    let mut tls = acceptor
        .accept(shim)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string()))?;
    tls.get_mut().0.passthrough = true;
    wire.stream = TdsStream::Tls(Box::new(tls));
    Ok(())
}

/// A session's view, for the translator: its variables, and the parameters
/// of the statement in hand.
struct Scope<'a> {
    s: &'a Session,
    params: Option<&'a ParamCtx>,
}

impl Env for Scope<'_> {
    fn shown(&self, relation: &str, column: Option<&str>) -> Option<String> {
        self.s.names.relations.get(relation)?.get(column.unwrap_or("")).cloned()
    }
    fn shown_object(&self, name: &str) -> Option<String> {
        self.s.names.objects.get(name).cloned()
    }
    fn case_insensitive(&self) -> bool {
        self.s.shared.config.case_insensitive
    }
    fn database(&self) -> &Arc<Database> {
        &self.s.bridge.as_ref().unwrap().database
    }
    fn database_name(&self) -> &str {
        if self.s.database_shown.is_empty() {
            &self.s.bridge.as_ref().unwrap().database_name
        } else {
            &self.s.database_shown
        }
    }
    fn user(&self) -> &str {
        &self.s.login.user
    }
    fn guid_order(&self) -> bool {
        self.s.shared.config.guid_order
    }
    fn default_database(&self) -> Option<String> {
        Some(self.s.shared.catalog.default_name().to_string())
    }
    fn databases(&self) -> Vec<String> {
        let ctx = crate::core::policy_hooks::SessionContext {
            role: &self.s.login.user,
            database: self.database_name(),
            protocol: "tds",
            superuser: self.s.login.superuser,
        };
        let visible = crate::core::policy_hooks::visible_databases(&ctx, self.s.shared.catalog.names());
        // Each as it was declared (`CREATE DATABASE [MyApp]`).
        let shown = self.s.shared.database_names.lock();
        visible.into_iter().map(|n| shown.as_ref().and_then(|m| m.get(&n).cloned()).unwrap_or(n)).collect()
    }
    fn variable(&self, name: &str) -> Option<Value> {
        self.s.vars.get(name).map(|v| v.value.clone())
    }
    fn param_text(&self, name: &str) -> Option<Option<String>> {
        let p = self.params?;
        let k = p.decls.iter().position(|d| d.0 == name)?;
        Some(match p.values.get(k)? {
            tds::ParamValue::Null => None,
            tds::ParamValue::Text(t) => Some(t.clone()),
            tds::ParamValue::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        })
    }
    fn param(&self, name: &str) -> Option<ParamSlot> {
        let p = self.params?;
        let k = p.decls.iter().position(|d| d.0 == name)?;
        let mut used = p.used.lock();
        let index = match used.iter().position(|u| *u == k) {
            Some(i) => i,
            None => {
                used.push(k);
                used.len() - 1
            }
        };
        Some(ParamSlot {
            index,
            string: p.decls[k].1,
        })
    }
    fn system(&self, name: &str) -> Option<Value> {
        let s = self.s;
        Some(match name {
            "version" => Value::Str(VERSION_TEXT.into()),
            "spid" => Value::Int(
                s.bridge
                    .as_ref()
                    .map(|b| b.pid as i64 & 0x7fff)
                    .unwrap_or(0),
            ),
            "rowcount" => Value::Int(s.rowcount),
            "identity" => s
                .last_identity
                .map(|v| Value::Num(v.to_string()))
                .unwrap_or(Value::Null),
            "trancount" => Value::Int(s.trancount as i64),
            "error" => Value::Int(s.error_number as i64),
            "servername" => Value::Str(SERVER_NAME.into()),
            "servicename" => Value::Str("MSSQLSERVER".into()),
            "language" => Value::Str("us_english".into()),
            "datefirst" => Value::Int(7),
            "max_precision" => Value::Int(38),
            "options" => Value::Int(5496),
            "lock_timeout" => Value::Int(-1),
            "textsize" => Value::Int(2147483647),
            "microsoftversion" => Value::Int(0x10000FA0),
            "langid" => Value::Int(0),
            "nestlevel" => Value::Int(s.nest_level as i64),
            "procid" => Value::Int(0),
            "fetch_status" => Value::Int(s.fetch_status as i64),
            "dbts" => Value::Bytes(vec![0; 8]),
            _ => return None,
        })
    }
    fn routines(&self) -> Vec<(String, String)> {
        self.s.routine_cache.clone().unwrap_or_default()
    }
    fn temp_name(&self, name: &str) -> String {
        self.s.temp_name(name)
    }
    fn function(&self, name: &str) -> Option<Value> {
        let s = self.s;
        let e = s.last_error.as_ref();
        Some(match name {
            "SCOPE_IDENTITY" | "IDENT_CURRENT" => s
                .last_identity
                .map(|v| Value::Num(v.to_string()))
                .unwrap_or(Value::Null),
            "ROWCOUNT_BIG" => Value::Int(s.rowcount),
            "HOST_NAME" => Value::Str(s.host.clone()),
            "APP_NAME" => Value::Str(s.app.clone()),
            "ERROR_MESSAGE" => e
                .map(|e| Value::Str(e.message.clone()))
                .unwrap_or(Value::Null),
            "ERROR_NUMBER" => e
                .map(|e| Value::Int(e.number as i64))
                .unwrap_or(Value::Null),
            "ERROR_SEVERITY" => e.map(|e| Value::Int(e.class as i64)).unwrap_or(Value::Null),
            "ERROR_STATE" => e.map(|e| Value::Int(e.state as i64)).unwrap_or(Value::Null),
            "ERROR_LINE" => e.map(|_| Value::Int(1)).unwrap_or(Value::Null),
            "ERROR_PROCEDURE" => Value::Null,
            "XACT_STATE" => Value::Int((s.trancount > 0) as i64),
            "IS_SRVROLEMEMBER" | "IS_MEMBER" => {
                Value::Int(s.login.superuser as i64)
            }
            "HAS_DBACCESS" => Value::Int(1),
            "SESSIONPROPERTY" => Value::Int(1),
            _ => return None,
        })
    }
}

/// A PostgreSQL error in SQL Server's numbering; the PostgreSQL code is kept.
fn error_from_engine(n: &PgNotice) -> TsqlError {
    let (number, class): (i32, u8) = match n.code.as_str() {
        "23505" => (2627, 14),
        "23502" => (515, 16),
        "23503" => (547, 16),
        "23514" => (547, 16),
        "42P01" => (208, 16),
        "42703" => (207, 16),
        "42601" => (102, 15),
        "42P07" => (2714, 16),
        "42701" => (2705, 16),
        "42883" => (195, 15),
        "42501" => (229, 14),
        "3D000" => (911, 16),
        "42P04" => (1801, 16),
        "40001" | "40P01" => (1205, 13),
        "55P03" => (1222, 16),
        "57014" => (3980, 16),
        "22001" => (2628, 16),
        "22003" => (8115, 16),
        "22012" => (8134, 16),
        "22P02" | "22007" | "22008" => (245, 16),
        "0A000" => (40000, 16),
        "25P02" | "25P03" => (3998, 16),
        _ => (50000, 16),
    };
    let mut message = n.message.clone();
    if let Some(d) = &n.detail {
        message.push_str(": ");
        message.push_str(d);
    }
    if !n.code.is_empty() {
        message.push_str(&format!(" (SQLSTATE {})", n.code));
    }
    TsqlError {
        number,
        class,
        state: 1,
        message,
    }
}

/// The `"…"`-quoted names of an engine message, in order.
fn quoted_names(message: &str) -> Vec<&str> {
    message.split('"').skip(1).step_by(2).collect()
}

/// A duplicate's values from the engine's `…: a = 'x', b = 2` tail, as
/// SQL Server lists them (`x, 2`).
fn duplicate_values(message: &str) -> Option<String> {
    let (_, tail) = message.split_once("): ")?;
    let tail = tail.split("; the row being updated is").next().unwrap_or(tail);
    let mut values = Vec::new();
    for part in tail.split(", ") {
        let (column, v) = part.split_once(" = ")?;
        // A case-insensitive key compares its folded companion, whose
        // value is not the one written: none shown rather than a wrong one.
        if column.starts_with("__ci_") {
            return None;
        }
        let v = v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')).unwrap_or(v);
        values.push(v.replace("''", "'"));
    }
    Some(values.join(", "))
}

/// A time parameter's type as bound: none (read as its text, as a literal
/// is), where `timestamp` rounded a `datetime2(7)` value to microseconds
/// before its seventh digit could be kept (`__rdb_t7_digit`).
fn untyped_time(oid: u32) -> u32 {
    if matches!(oid, 1114 | 1184 | 1083) { 0 } else { oid }
}

fn isolation_words(level: u8) -> Option<&'static str> {
    Some(match level {
        1 => "READ UNCOMMITTED",
        2 => "READ COMMITTED",
        3 => "REPEATABLE READ",
        4 | 5 => "SERIALIZABLE",
        _ => return None,
    })
}

/// A value fetched from the engine, typed by its column.
fn value_of(oid: u32, text: Option<String>) -> Value {
    match text {
        None => Value::Null,
        Some(t) => match oid {
            20 | 21 | 23 => t.parse().map(Value::Int).unwrap_or(Value::Str(t)),
            16 => Value::Int((t == "t" || t == "true") as i64),
            700 | 701 | 1700 => Value::Num(t),
            _ => Value::Str(t),
        },
    }
}

/// `@a int, @b nvarchar(10) OUTPUT` as (name, string, oid).
fn parse_declarations(text: &str) -> Result<Vec<Decl>, TsqlError> {
    let tokens = lex::lex(text).map_err(|e| TsqlError::syntax(e.0))?;
    let mut out = Vec::new();
    for part in split_commas(&tokens) {
        let Some(Tok::UserVar(name)) = part.first().map(|t| &t.tok) else {
            if part.is_empty() {
                continue;
            }
            return Err(TsqlError::syntax(format!(
                "Incorrect syntax in the parameter list '{text}'."
            )));
        };
        let ty = part.get(1).and_then(|t| t.upper()).unwrap_or_default();
        let string = matches!(
            ty.as_str(),
            "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "SYSNAME" | "XML"
        );
        let oid = match ty.as_str() {
            "VARBINARY" | "BINARY" | "IMAGE" => 17,
            _ => 0,
        };
        let number = matches!(
            ty.as_str(),
            "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "DECIMAL" | "NUMERIC" | "MONEY" | "SMALLMONEY" | "FLOAT" | "REAL"
        );
        out.push((name.clone(), string, oid, number));
    }
    Ok(out)
}

impl Session {
    fn bridge(&mut self) -> &mut PgBridge {
        self.bridge.as_mut().expect("an open session has a bridge")
    }

    fn bridge_options(&self, database: &str) -> BridgeOptions {
        let mut parameters = HashMap::new();
        parameters.insert("database".to_string(), database.to_string());
        BridgeOptions {
            login: self.login.clone(),
            limits: self.shared.config.limits,
            watch: self.peer_watch.clone(),
            parameters,
            lenient_booleans: false,
        }
    }

    /// `writes`, then `read`, on the default database: where databases'
    /// declared names are kept, whichever this session is in. Over a bridge
    /// of its own when the session is elsewhere. `None` when it could not
    /// be reached or the read failed.
    async fn on_default(&mut self, writes: &[String], read: Option<&str>) -> Option<Vec<Vec<Option<String>>>> {
        let default = self.shared.catalog.default_name().to_string();
        let away = self.bridge.as_ref().is_some_and(|b| b.database_name != default);
        let main = if away {
            let side = PgBridge::open(self.shared.catalog.clone(), &default, self.bridge_options(&default)).await.ok()?;
            Some(self.bridge.replace(side))
        } else {
            None
        };
        // The front end's own statements on its catalogue (`NAMES_TABLE`),
        // announced as such: a client's are refused there.
        for w in writes {
            if let Err(e) = self.silent_as(w, true).await {
                log::warn!("SQL Server session: recording a database's name failed: {}", e.message);
            }
        }
        let rows = match read {
            Some(sql) => self.query_bound_as(sql, &[], true).await.ok().map(|(_, rows, _)| rows),
            None => Some(Vec::new()),
        };
        if let Some(main) = main {
            if let Some(side) = std::mem::replace(&mut self.bridge, main) {
                side.terminate().await;
            }
        }
        rows
    }

    /// Databases' declared names (`Shared::database_names`), read from the
    /// default database the first time.
    async fn database_names(&mut self) -> HashMap<String, String> {
        if let Some(m) = self.shared.database_names.lock().clone() {
            return m;
        }
        let default = self.shared.catalog.default_name().to_string();
        let has = self.shared.catalog.get(&default).is_some_and(|d| d.has_table(translate::NAMES_TABLE));
        let mut map = HashMap::new();
        if has {
            let read = format!("SELECT col, shown FROM {} WHERE rel = '{DATABASES_REL}'", translate::NAMES_TABLE);
            let Some(rows) = self.on_default(&[], Some(&read)).await else {
                return map;
            };
            for row in rows {
                if let [Some(name), Some(shown)] = [row.first().cloned().flatten(), row.get(1).cloned().flatten()] {
                    map.insert(name, shown);
                }
            }
        }
        *self.shared.database_names.lock() = Some(map.clone());
        map
    }

    /// A CREATE DATABASE's spelling recorded, or a DROP's forgotten.
    async fn record_database(&mut self, name: &str, created: bool) {
        let _ = self.database_names().await;
        let key = name.to_lowercase();
        let names = translate::NAMES_TABLE;
        let writes = if created {
            vec![
                format!("CREATE TABLE IF NOT EXISTS {names} (rel text NOT NULL, col text NOT NULL, shown text NOT NULL, PRIMARY KEY (rel, col))"),
                format!(
                    "INSERT INTO {names} (rel, col, shown) VALUES ('{DATABASES_REL}', {}, {}) ON CONFLICT (rel, col) DO UPDATE SET shown = excluded.shown",
                    translate::sql_str(&key),
                    translate::sql_str(name)
                ),
            ]
        } else if self.shared.catalog.get(self.shared.catalog.default_name()).is_some_and(|d| d.has_table(names)) {
            vec![format!("DELETE FROM {names} WHERE rel = '{DATABASES_REL}' AND col = {}", translate::sql_str(&key))]
        } else {
            Vec::new()
        };
        if !writes.is_empty() {
            let _ = self.on_default(&writes, None).await;
        }
        if let Some(m) = self.shared.database_names.lock().as_mut() {
            if created {
                m.insert(key, name.to_string());
            } else {
                m.remove(&key);
            }
        }
    }

    async fn open(&mut self, database: &str) -> Result<(), TsqlError> {
        let options = self.bridge_options(database);
        match PgBridge::open(self.shared.catalog.clone(), database, options).await {
            Ok(b) => {
                // What this database made of the login (its own role's
                // attributes), for this front end's own checks.
                self.login = b.login.clone();
                if let Some(old) = self.bridge.replace(b) {
                    old.terminate().await;
                }
                Ok(())
            }
            Err(OpenRefused::NoDatabase) => Err(TsqlError::new(
                911,
                format!(
                    "Database '{database}' does not exist. Make sure that the name is entered correctly."
                ),
            )),
            Err(OpenRefused::Loading) => Err(TsqlError::new(
                922,
                format!(
                    "Database '{database}' is being recovered. Waiting until recovery is finished."
                ),
            )),
            Err(OpenRefused::Denied(why)) => Err(TsqlError::new(
                916,
                format!(
                    "The server principal \"{}\" is not able to access the database \"{database}\" under the current \
                     security context: {why}",
                    self.login.user
                ),
            )),
            Err(OpenRefused::Io(e)) => Err(TsqlError::new(50000, e.to_string())),
        }
    }

    async fn request_loop(&mut self) -> io::Result<()> {
        let mut stopping = self.shared.stopping.subscribe();
        loop {
            let message = tokio::select! {
                m = self.wire.read() => match m {
                    Ok(m) => m,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => return Err(e),
                },
                _ = stopping.wait_for(|s| *s) => return Ok(()),
            };
            self.last_done = None;
            if message.status & (tds::ST_RESET_CONNECTION | tds::ST_RESET_CONNECTION_SKIP_TRAN) != 0
            {
                self.reset(message.status & tds::ST_RESET_CONNECTION_SKIP_TRAN != 0)
                    .await;
                tds::env_change_bytes(self.wire.reply(), 18, &[], &[]);
            }
            match message.kind {
                tds::PT_SQL_BATCH => self.sql_batch(&message).await?,
                tds::PT_RPC => self.rpc(&message).await?,
                tds::PT_TM => self.transaction_manager(&message).await?,
                tds::PT_ATTENTION => {
                    tds::done(self.wire.reply(), tds::TOKEN_DONE, tds::DONE_ATTN, 0, 0);
                    self.wire.send(tds::PT_REPLY).await?;
                    continue;
                }
                tds::PT_BULK => {
                    self.in_rpc = false;
                    match self.bulk_load(&message).await {
                        Ok(n) => {
                            self.rowcount = n as i64;
                            self.statement_done(tds::DONE_COUNT, 0, n);
                        }
                        Err(e) => {
                            self.error_token(&e);
                            self.statement_done(tds::DONE_ERROR, 0, 0);
                        }
                    }
                    self.final_done();
                }
                tds::PT_SSPI => return Ok(()),
                other => {
                    self.error_token(&TsqlError::new(
                        50000,
                        format!("Unsupported TDS message type {other:#04x}"),
                    ));
                    self.final_done();
                }
            }
            // A transaction's application locks go with it.
            if self.trancount == 0 && !self.bridge().in_block() {
                super::applock::release_all(self.conn_id, Some(super::applock::Owner::Transaction));
            }
            self.wire.send(tds::PT_REPLY).await?;
            // An attention sent while the request ran is answered after it.
            if self.wire.attention_buffered() {
                let _ = self.wire.read().await?;
                tds::done(self.wire.reply(), tds::TOKEN_DONE, tds::DONE_ATTN, 0, 0);
                self.wire.send(tds::PT_REPLY).await?;
            }
        }
    }

    async fn reset(&mut self, keep_transaction: bool) {
        if !keep_transaction && self.bridge().in_block() {
            let _ = self.silent("ROLLBACK").await;
            self.trancount = 0;
            self.descriptor = 0;
            self.savepoints.clear();
        }
        self.vars.clear();
        self.cursors.clear();
        self.fetch_status = -1;
        for t in std::mem::take(&mut self.session_temps) {
            let _ = self
                .silent(&format!("DROP TABLE IF EXISTS {}", ident_sql(&t)))
                .await;
        }
        self.nocount = false;
        self.xact_abort = false;
        self.implicit_transactions = false;
        self.prepared.clear();
        self.last_identity = None;
        self.rowcount = 0;
        self.last_error = None;
        self.error_number = 0;
        if !self.bridge().in_block() {
            let _ = self.silent("RESET ALL").await;
            let login_db = resolve_database(&self.shared, &self.login_shown);
            let old = self.bridge().database_name.clone();
            if old != login_db && self.shared.catalog.get(&login_db).is_some() {
                self.prepared.clear();
                if self.open(&login_db).await.is_ok() {
                    let shown = self.login_shown.clone();
                    tds::env_change_text(self.wire.reply(), 1, &shown, &old);
                }
            }
        }
    }

    fn error_token(&mut self, e: &TsqlError) {
        tds::message(
            self.wire.reply(),
            true,
            e.number,
            e.state,
            e.class,
            &e.message,
            SERVER_NAME,
            1,
        );
        self.last_error = Some(e.clone());
        self.error_number = e.number;
    }

    fn info_token(&mut self, number: i32, text: &str) {
        tds::message(self.wire.reply(), false, number, 1, 0, text, SERVER_NAME, 1);
    }

    /// The DONE (or DONEINPROC) after a statement.
    fn statement_done(&mut self, status: u16, command: u16, rows: u64) {
        let token = if self.in_rpc {
            tds::TOKEN_DONEINPROC
        } else {
            tds::TOKEN_DONE
        };
        let mut status = status | tds::DONE_MORE;
        if self.trancount > 0 {
            status |= tds::DONE_INXACT;
        }
        let at = self.wire.mark() + 1;
        tds::done(self.wire.reply(), token, status, command, rows);
        self.last_done = Some(at);
    }

    /// Ends a batch: its last DONE loses MORE, or a final one is added.
    fn final_done(&mut self) {
        if let Some(at) = self.last_done.take() {
            if self.wire.clear_bits(at, tds::DONE_MORE as u8) {
                return;
            }
        }
        tds::done(self.wire.reply(), tds::TOKEN_DONE, 0, 0, 0);
    }

    // -----------------------------------------------------------------------
    // Running statements on the engine
    // -----------------------------------------------------------------------

    async fn silent(&mut self, sql: &str) -> Result<String, PgNotice> {
        self.silent_as(sql, false).await
    }

    /// [`Self::silent`], or with `system` this front end's own statement on
    /// its catalogue (`__rdb_routines`), announced so the session's
    /// catalogue guard lets it through (`PgBridge::system_query`). Never
    /// `system` for a client's text.
    async fn silent_as(&mut self, sql: &str, system: bool) -> Result<String, PgNotice> {
        let b = self.bridge();
        if system {
            b.system_query(sql);
        } else {
            b.query(sql);
        }
        let mut error = None;
        let mut tag = String::new();
        loop {
            let frame = b.next().await.map_err(|e| PgNotice {
                code: "08006".into(),
                message: e.to_string(),
                ..Default::default()
            })?;
            match frame.tag {
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'C' => {
                    tag =
                        String::from_utf8_lossy(frame.body.strip_suffix(&[0]).unwrap_or(frame.body))
                            .into_owned()
                }
                b'Z' => break,
                _ => {}
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(tag),
        }
    }

    /// A query whose rows are the session's (a condition, a variable).
    async fn fetch(
        &mut self,
        sql: &str,
        params: &[(i16, Option<Vec<u8>>, u32)],
    ) -> Result<(Vec<u32>, Vec<Vec<Option<String>>>), PgNotice> {
        let (fields, rows) = self.fetch_fields(sql, params).await?;
        Ok((fields.iter().map(|f| f.type_oid).collect(), rows))
    }

    /// [`Session::fetch`], with the columns' names and types.
    async fn fetch_fields(
        &mut self,
        sql: &str,
        params: &[(i16, Option<Vec<u8>>, u32)],
    ) -> Result<(Vec<PgField>, Vec<Vec<Option<String>>>), PgNotice> {
        self.query_bound(sql, params).await.map(|(f, r, _)| (f, r))
    }

    /// One statement's columns, rows and command tag.
    async fn query_bound(
        &mut self,
        sql: &str,
        params: &[(i16, Option<Vec<u8>>, u32)],
    ) -> Result<(Vec<PgField>, Vec<Vec<Option<String>>>, String), PgNotice> {
        self.query_bound_as(sql, params, false).await
    }

    /// [`Self::query_bound`]; with `system`, as [`Self::silent_as`] says,
    /// and with no parameters (the announcement covers a simple query).
    async fn query_bound_as(
        &mut self,
        sql: &str,
        params: &[(i16, Option<Vec<u8>>, u32)],
        system: bool,
    ) -> Result<(Vec<PgField>, Vec<Vec<Option<String>>>, String), PgNotice> {
        let b = self.bridge();
        if system {
            debug_assert!(params.is_empty(), "a catalogue statement carries its values as literals");
            b.system_query(sql);
        } else if params.is_empty() {
            b.query(sql);
        } else {
            let oids: Vec<u32> = params.iter().map(|p| p.2).collect();
            b.parse("", sql, &oids);
            let refs: Vec<(i16, Option<&[u8]>)> =
                params.iter().map(|(f, v, _)| (*f, v.as_deref())).collect();
            b.bind("", "", &refs, &[0]);
            b.describe(b'P', "");
            b.execute("", 0);
            b.sync();
        }
        let mut error = None;
        let mut fields = Vec::new();
        let mut rows = Vec::new();
        let mut tag = String::new();
        loop {
            let frame = b.next().await.map_err(|e| PgNotice {
                code: "08006".into(),
                message: e.to_string(),
                ..Default::default()
            })?;
            match frame.tag {
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'T' => fields = parse_row_description(frame.body).unwrap_or_default(),
                b'C' => {
                    tag =
                        String::from_utf8_lossy(frame.body.strip_suffix(&[0]).unwrap_or(frame.body))
                            .into_owned()
                }
                b'D' => {
                    if let Ok(cells) = data_row_cells(frame.body) {
                        rows.push(
                            cells
                                .iter()
                                .map(|c| c.map(|v| String::from_utf8_lossy(v).into_owned()))
                                .collect(),
                        );
                    }
                }
                b'Z' => break,
                _ => {}
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok((fields, rows, tag)),
        }
    }

    /// One statement on the engine, its rows streamed to the client.
    async fn exchange(
        &mut self,
        sql: &str,
        params: &[(i16, Option<Vec<u8>>, u32)],
        capture: bool,
        extra: Option<&(String, Vec<String>)>,
    ) -> io::Result<Outcome> {
        {
            let b = self.bridge.as_mut().unwrap();
            if params.is_empty() {
                b.query(sql);
            } else {
                let oids: Vec<u32> = params.iter().map(|p| p.2).collect();
                b.parse("", sql, &oids);
                let refs: Vec<(i16, Option<&[u8]>)> =
                    params.iter().map(|(f, v, _)| (*f, v.as_deref())).collect();
                b.bind("", "", &refs, &[0]);
                b.describe(b'P', "");
                b.execute("", 0);
                b.sync();
            }
        }
        let mut columns: Option<Vec<Column>> = None;
        // A bound SELECT whose rows came with no description: run again
        // with its values written in (`inline_params`), as nothing has
        // reached the client yet.
        let mut undescribed = false;
        // Numeric columns with no declared scale: rows held until their
        // scale is known.
        let mut held: Option<Vec<Vec<Option<Vec<u8>>>>> = None;
        let mut count = 0u64;
        let mut identity: Option<i64> = None;
        let mut error: Option<PgNotice> = None;
        let mut tag = String::new();
        let mut encode_error: Option<String> = None;
        loop {
            let b = self.bridge.as_mut().unwrap();
            let frame = b.next().await?;
            match frame.tag {
                b'T' => {
                    let fields = parse_row_description(frame.body)?;
                    let mut unknown = false;
                    let cols: Vec<Column> = fields
                        .iter()
                        .map(|f| {
                            // A time read with its 100 ns digit, as text;
                            // a COUNT(DISTINCT …) as SQL Server's int.
                            let tick = matches!(f.type_oid, 25 | 20)
                                .then(|| self.ticks.iter().find(|(n, _)| *n == f.name).map(|(_, t)| *t))
                                .flatten()
                                .filter(|t| (*t == "int") == (f.type_oid == 20));
                            let ty = match tick {
                                Some("int") => Some(tds::ColType::Int(4)),
                                Some("datetimeoffset") => Some(tds::ColType::DateTimeOffset),
                                Some("time") => Some(tds::ColType::Time),
                                Some(_) => Some(tds::ColType::DateTime2),
                                None => tds::column_type(f.type_oid, f.type_modifier),
                            };
                            if ty.is_none() {
                                unknown = true;
                            }
                            Column {
                                // SQL Server leaves an unaliased expression
                                // unnamed.
                                name: if f.name.starts_with("?column?") {
                                    String::new()
                                } else if f.table_oid != 0 && !self.aliases.iter().any(|a| *a == f.name) {
                                    // A table's column read as it is (`SELECT *`, an
                                    // unaliased column): its declared spelling. Never
                                    // an alias, which is the client's own.
                                    self.names.column_shown(&f.name)
                                } else {
                                    f.name.clone()
                                },
                                ty: ty.unwrap_or(ColType::Decimal {
                                    precision: 38,
                                    scale: 0,
                                }),
                                oid: f.type_oid,
                                identity: false,
                            }
                        })
                        .collect();
                    let mut cols = cols;
                    if let Some((name, _)) = extra {
                        cols.push(Column {
                            name: name.clone(),
                            ty: ColType::Int(4),
                            oid: 23,
                            identity: false,
                        });
                    }
                    if !capture {
                        if unknown {
                            held = Some(Vec::new());
                        } else {
                            tds::col_metadata(self.wire.reply(), &cols);
                        }
                    }
                    columns = Some(cols);
                }
                b'D' if columns.is_none() && !capture && !params.is_empty() => {
                    undescribed = true;
                }
                b'D' => {
                    let cells = data_row_cells(frame.body)?;
                    count += 1;
                    if capture {
                        if let Some(Some(v)) = cells.first() {
                            identity = std::str::from_utf8(v).ok().and_then(|s| s.parse().ok());
                        }
                        continue;
                    }
                    let extra_value: Option<Vec<u8>> = extra
                        .and_then(|(_, values)| values.get(count as usize - 1))
                        .map(|v| v.as_bytes().to_vec());
                    if let Some(rows) = held.as_mut() {
                        let mut row: Vec<Option<Vec<u8>>> =
                            cells.iter().map(|c| c.map(|v| v.to_vec())).collect();
                        if extra.is_some() {
                            row.push(extra_value);
                        }
                        rows.push(row);
                        continue;
                    }
                    if encode_error.is_some() {
                        continue;
                    }
                    let cols = columns.as_deref().unwrap_or(&[]);
                    let at = self.wire.reply_len();
                    let out = self.wire.reply();
                    out.push(0xd1);
                    let mut all: Vec<Option<&[u8]>> = cells.iter().copied().collect();
                    if extra.is_some() {
                        all.push(extra_value.as_deref());
                    }
                    for (k, cell) in all.iter().enumerate() {
                        let ty = cols
                            .get(k)
                            .map(|c| c.ty.clone())
                            .unwrap_or(ColType::NVarChar(None));
                        if let Err(e) = tds::put_value(out, &ty, *cell) {
                            out.truncate(at);
                            encode_error = Some(e);
                            break;
                        }
                    }
                    if self.wire.reply_len() > 64 * 1024 {
                        self.wire.flush_partial(tds::PT_REPLY).await?;
                    }
                }
                b'C' => {
                    tag =
                        String::from_utf8_lossy(frame.body.strip_suffix(&[0]).unwrap_or(frame.body))
                            .into_owned()
                }
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'N' => {
                    let n = PgNotice::parse(frame.body);
                    self.info_token(0, &n.message);
                }
                b'Z' => break,
                _ => {}
            }
        }
        if undescribed && error.is_none() {
            // The engine described the bound statement as rowless and then
            // returned rows: a client given rows with no COLMETADATA loses
            // the connection. The same statement as text is described, so
            // it runs again that way, once nothing it does can differ:
            // a plain read, no volatile call. Logged, as each such shape is
            // a describe gap to close.
            let rerun = is_plain_select(sql) && !calls_volatile(sql);
            if let Some(text) = rerun.then(|| inline_params(sql, params, &self.bound_numbers)).flatten() {
                log::warn!("SQL Server session: a bound SELECT was described as rowless and runs again with its values inlined: {sql}");
                return Box::pin(self.exchange(&text, &[], capture, extra)).await;
            }
            log::warn!("SQL Server session: a bound statement was described as rowless and returned rows: {sql}");
            return Ok(Outcome::Failed(
                PgNotice {
                    code: "0A000".into(),
                    message: "this statement's result columns could not be described with its parameters bound; \
                              it is not supported yet with parameters"
                        .into(),
                    ..Default::default()
                },
                false,
            ));
        }
        if let (Some(rows), Some(cols)) = (held, columns.as_mut()) {
            // The scale of each undeclared numeric: the most any value has.
            for (k, c) in cols.iter_mut().enumerate() {
                if c.oid == 1700
                    && matches!(
                        c.ty,
                        ColType::Decimal {
                            precision: 38,
                            scale: 0
                        }
                    )
                {
                    let (mut int, mut frac) = (1usize, 0usize);
                    for r in &rows {
                        if let Some(Some(v)) = r.get(k) {
                            let (i, f) = tds::decimal_shape(&String::from_utf8_lossy(v));
                            int = int.max(i);
                            frac = frac.max(f);
                        }
                    }
                    let frac = frac.min(38 - int.min(38));
                    c.ty = ColType::Decimal {
                        precision: (int + frac).clamp(1, 38) as u8,
                        scale: frac as u8,
                    };
                }
            }
            tds::col_metadata(self.wire.reply(), cols);
            for r in rows {
                let out = self.wire.reply();
                let at = out.len();
                out.push(0xd1);
                for (k, cell) in r.iter().enumerate() {
                    let ty = cols
                        .get(k)
                        .map(|c| c.ty.clone())
                        .unwrap_or(ColType::NVarChar(None));
                    if let Err(e) = tds::put_value(out, &ty, cell.as_deref()) {
                        out.truncate(at);
                        encode_error = Some(e);
                        break;
                    }
                }
                if encode_error.is_some() {
                    break;
                }
            }
        }
        let started = columns.is_some() && !capture;
        if let Some(e) = error {
            return Ok(Outcome::Failed(e, started));
        }
        if let Some(e) = encode_error {
            return Ok(Outcome::Failed(
                PgNotice {
                    code: "22003".into(),
                    message: e,
                    ..Default::default()
                },
                started,
            ));
        }
        if columns.is_some() && !capture {
            return Ok(Outcome::Rows { count });
        }
        Ok(Outcome::Done { tag, identity })
    }

    /// Tells the client of a transaction that began or ended in the engine.
    fn sync_transaction(&mut self, committed: bool) {
        let open = self.bridge.as_ref().is_some_and(|b| b.in_block());
        if open && self.descriptor == 0 {
            self.descriptor = self.next_descriptor;
            self.next_descriptor += 1;
            let d = self.descriptor.to_le_bytes();
            tds::env_change_bytes(self.wire.reply(), 8, &d, &[]);
        } else if !open && self.descriptor != 0 {
            let d = self.descriptor.to_le_bytes();
            tds::env_change_bytes(self.wire.reply(), if committed { 9 } else { 10 }, &[], &d);
            self.descriptor = 0;
            self.trancount = 0;
            self.savepoints.clear();
        }
    }

    async fn begin(&mut self, isolation: Option<&str>) -> Result<(), TsqlError> {
        if self.trancount == 0 {
            let sql = match isolation {
                Some(level) => format!("BEGIN ISOLATION LEVEL {level}"),
                None => "BEGIN".into(),
            };
            self.silent(&sql).await.map_err(|e| self.engine_error(&e))?;
        }
        self.trancount += 1;
        self.sync_transaction(true);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), TsqlError> {
        if self.trancount == 0 {
            return Err(TsqlError::new(
                3902,
                "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.",
            ));
        }
        self.trancount -= 1;
        if self.trancount == 0 {
            let tag = self
                .silent("COMMIT")
                .await
                .map_err(|e| self.engine_error(&e));
            let rolled_back = matches!(&tag, Ok(t) if t == "ROLLBACK");
            self.sync_transaction(!rolled_back);
            tag?;
            if rolled_back {
                return Err(TsqlError::new(
                    3998,
                    "The transaction was rolled back instead of committed: a statement in it had failed.",
                ));
            }
        }
        Ok(())
    }

    async fn rollback(&mut self, name: Option<&str>) -> Result<(), TsqlError> {
        if let Some(n) = name {
            let lower = n.to_lowercase();
            if let Some(pos) = self.savepoints.iter().rposition(|s| *s == lower) {
                self.savepoints.truncate(pos + 1);
                self.silent(&format!("ROLLBACK TO SAVEPOINT {}", ident_sql(&lower)))
                    .await
                    .map_err(|e| self.engine_error(&e))?;
                return Ok(());
            }
        }
        if self.trancount == 0 && !self.bridge().in_block() {
            return Err(TsqlError::new(
                3903,
                "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION.",
            ));
        }
        let r = self
            .silent("ROLLBACK")
            .await
            .map_err(|e| self.engine_error(&e));
        self.trancount = 0;
        self.sync_transaction(false);
        r.map(|_| ())
    }

    /// Runs a translated statement and answers for it. `Err` is the
    /// statement's error, not yet written.
    async fn forward(
        &mut self,
        forward: Forward,
        params: Option<&ParamCtx>,
    ) -> io::Result<Result<(), TsqlError>> {
        let ddl = forward.kind == Kind::Ddl;
        if ddl {
            self.names.dirty = true;
        }
        if self.bridge().status == b'E' {
            log::warn!("SQL Server session: a failed transaction was found open; rolling it back");
            let _ = self.silent("ROLLBACK").await;
            self.sync_transaction(false);
        }
        if self.implicit_transactions && self.trancount == 0 && !self.bridge().in_block() {
            if let Err(e) = self.begin(None).await {
                return Ok(Err(e));
            }
        }
        let guarded = self.bridge().in_block() && !self.xact_abort;
        if guarded {
            if let Err(e) = self
                .silent(&format!("SAVEPOINT {STATEMENT_SAVEPOINT}"))
                .await
            {
                return Ok(Err(self.engine_error(&e)));
            }
        }
        let bound: Vec<(i16, Option<Vec<u8>>, u32)> = match params {
            Some(p) => p
                .used
                .lock()
                .iter()
                .map(|&k| {
                    let oid = p.decls[k].2;
                    match p.values.get(k).cloned().unwrap_or(ParamValue::Null) {
                        ParamValue::Null => (0, None, oid),
                        ParamValue::Text(t) => {
                            (0, Some(t.into_bytes()), if oid == 17 { 0 } else { untyped_time(oid) })
                        }
                        ParamValue::Bytes(b) => (1, Some(b), 17),
                    }
                })
                .collect(),
            None => Vec::new(),
        };
        self.bound_numbers = match params {
            Some(p) => p.used.lock().iter().map(|&k| p.decls[k].3).collect(),
            None => Vec::new(),
        };
        let capture = matches!(forward.kind, Kind::Insert { identity: Some(_), .. });
        let mut last = None;
        let mut affected = 0u64;
        self.aliases = forward.aliases.clone();
        self.ticks = forward.ticks.clone();
        // The statement's verb, as SQL Server's messages name it.
        self.verb = forward
            .statements
            .iter()
            .rev()
            .find(|s| !s.is_empty())
            .and_then(|s| s.split_whitespace().next())
            .and_then(|w| ["INSERT", "UPDATE", "DELETE", "MERGE"].into_iter().find(|v| w.eq_ignore_ascii_case(v)))
            .unwrap_or("INSERT");
        for sql in forward.statements.iter().filter(|s| !s.is_empty()) {
            let outcome = self
                .exchange(sql, &bound, capture, forward.extra_column.as_ref())
                .await;
            let outcome = match outcome {
                Ok(o) => o,
                Err(e) => {
                    self.aliases.clear();
                    self.ticks.clear();
                    return Err(e);
                }
            };
            let failed = matches!(outcome, Outcome::Failed(..));
            if let Outcome::Done { tag, .. } = &outcome {
                affected += tag_count(tag).unwrap_or(0);
            }
            last = Some(outcome);
            if failed {
                break;
            }
        }
        self.aliases.clear();
        self.ticks.clear();
        // The DDL's record in this front end's catalogue (its declared
        // names), once the DDL has run: the front end's own statements,
        // past the session's catalogue guard.
        if !forward.system.is_empty() && !matches!(last, Some(Outcome::Failed(..))) {
            for sql in &forward.system {
                if let Err(notice) = self.silent_as(sql, true).await {
                    last = Some(Outcome::Failed(notice, false));
                    break;
                }
            }
        }
        let outcome = last.unwrap_or(Outcome::Done {
            tag: String::new(),
            identity: None,
        });
        match outcome {
            Outcome::Failed(n, started) => {
                let mut e = self.engine_error(&n);
                if started {
                    // The result had begun: it is ended here, with its
                    // error, before anything else is written.
                    if self.try_depth == 0 {
                        self.error_token(&e);
                    }
                    self.statement_done(tds::DONE_ERROR, 0xc1, 0);
                    e.state = 0;
                }
                if guarded {
                    if n.code == "40001" || n.code == "40P01" {
                        let _ = self.silent("ROLLBACK").await;
                    } else {
                        let _ = self
                            .silent(&format!("ROLLBACK TO SAVEPOINT {STATEMENT_SAVEPOINT}"))
                            .await;
                        let _ = self
                            .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                            .await;
                    }
                } else if self.xact_abort && self.bridge().in_block() {
                    let _ = self.silent("ROLLBACK").await;
                }
                self.sync_transaction(false);
                self.rowcount = 0;
                Ok(Err(e))
            }
            Outcome::Rows { count } => {
                if guarded {
                    let _ = self
                        .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                        .await;
                }
                self.rowcount = count as i64;
                self.sync_transaction(true);
                if self.nocount {
                    self.statement_done(0, 0xc1, 0);
                } else {
                    self.statement_done(tds::DONE_COUNT, 0xc1, count);
                }
                Ok(Ok(()))
            }
            Outcome::Done { tag, identity } => {
                if guarded {
                    let _ = self
                        .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                        .await;
                }
                if let Kind::Insert { written: Some((table, column)), .. } = &forward.kind {
                    self.advance_identity(table, column).await;
                }
                if let Some((name, created)) = &forward.database {
                    self.record_database(name, *created).await;
                }
                if let Some(id) = identity {
                    self.last_identity = Some(id);
                }
                let rows = if ddl {
                    0
                } else {
                    tag_count(&tag).unwrap_or(affected)
                };
                self.rowcount = rows as i64;
                self.sync_transaction(true);
                let command = match tag.split(' ').next().unwrap_or("") {
                    "INSERT" => 0xc3,
                    "DELETE" => 0xc4,
                    "UPDATE" => 0xc5,
                    "SELECT" => 0xc1,
                    _ => 0,
                };
                let counted = !ddl && command != 0 && !self.nocount;
                self.statement_done(
                    if counted { tds::DONE_COUNT } else { 0 },
                    command,
                    if counted { rows } else { 0 },
                );
                Ok(Ok(()))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Batches
    // -----------------------------------------------------------------------

    async fn sql_batch(&mut self, message: &Message) -> io::Result<()> {
        self.ensure_names().await;
        let mut r = Reader::new(&message.payload);
        if message.payload.len() >= 4 {
            let total = u32::from_le_bytes(message.payload[0..4].try_into().unwrap()) as usize;
            if total >= 4 && total <= message.payload.len() && total >= 22 {
                tds::all_headers(&mut r)?;
            }
        }
        let text = tds::utf16_text(&message.payload[r.pos..]);
        log::debug!("T-SQL batch: {text}");
        self.in_rpc = false;
        self.batch(&text, None).await?;
        self.final_done();
        Ok(())
    }

    /// Runs a batch's text. Errors are written as they happen; the batch
    /// goes on after a statement-level one.
    async fn batch(&mut self, text: &str, params: Option<&ParamCtx>) -> io::Result<Flow> {
        let tokens = match lex::lex(text) {
            Ok(t) => t,
            Err(e) => {
                let e = TsqlError::syntax(e.0);
                self.error_token(&e);
                self.statement_done(tds::DONE_ERROR, 0, 0);
                return Ok(Flow::Abort);
            }
        };
        self.refresh_routines_for(text).await;
        if let Some(r) = self.procedure_ddl(text, &tokens).await {
            return Ok(match r {
                Ok(()) => {
                    self.statement_done(0, 0, 0);
                    Flow::Next
                }
                Err(e) => {
                    self.error_token(&e);
                    self.statement_done(tds::DONE_ERROR, 0, 0);
                    Flow::Abort
                }
            });
        }
        let stmts = match translate::parse_batch(&tokens) {
            Ok(s) => s,
            Err(e) => {
                self.error_token(&e);
                self.statement_done(tds::DONE_ERROR, 0, 0);
                return Ok(Flow::Abort);
            }
        };
        self.scopes.push(LocalScope {
            owns_temps: params.is_some(),
            ..Default::default()
        });
        let r = self.run_list(text, &stmts, params).await;
        self.end_scope().await;
        match r? {
            Ok(flow) => Ok(flow),
            Err(e) => {
                self.error_token(&e);
                self.statement_done(tds::DONE_ERROR, 0, 0);
                Ok(Flow::Abort)
            }
        }
    }

    fn run_list<'a>(
        &'a mut self,
        text: &'a str,
        stmts: &'a [Stmt],
        params: Option<&'a ParamCtx>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Result<Flow, TsqlError>>> + Send + 'a>,
    > {
        Box::pin(async move {
            for s in stmts {
                match self.run(text, s, params).await? {
                    Ok(Flow::Next) => {}
                    Ok(flow) => return Ok(Ok(flow)),
                    Err(e) => {
                        if self.try_depth > 0 {
                            return Ok(Err(e));
                        }
                        // State 0: written already, where it happened.
                        if e.state != 0 {
                            self.error_token(&e);
                            self.statement_done(tds::DONE_ERROR, 0, 0);
                        }
                        if self.xact_abort || e.class >= 20 {
                            if self.bridge().in_block() {
                                let _ = self.silent("ROLLBACK").await;
                                self.sync_transaction(false);
                            }
                            return Ok(Ok(Flow::Abort));
                        }
                    }
                }
            }
            Ok(Ok(Flow::Next))
        })
    }

    fn run<'a>(
        &'a mut self,
        text: &'a str,
        stmt: &'a Stmt,
        params: Option<&'a ParamCtx>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Result<Flow, TsqlError>>> + Send + 'a>,
    > {
        Box::pin(async move {
            match stmt {
                Stmt::Simple(tokens) => self.simple(text, tokens, params).await,
                Stmt::Block(body) => self.run_list(text, body, params).await,
                Stmt::If {
                    condition,
                    then,
                    otherwise,
                } => {
                    let truth = match self.condition(condition, params).await {
                        Ok(t) => t,
                        Err(e) => return Ok(Err(e)),
                    };
                    if truth {
                        self.run(text, then, params).await
                    } else if let Some(o) = otherwise {
                        self.run(text, o, params).await
                    } else {
                        Ok(Ok(Flow::Next))
                    }
                }
                Stmt::While { condition, body } => {
                    let mut rounds = 0u64;
                    loop {
                        let truth = match self.condition(condition, params).await {
                            Ok(t) => t,
                            Err(e) => return Ok(Err(e)),
                        };
                        if !truth {
                            break;
                        }
                        rounds += 1;
                        if rounds > 1_000_000 {
                            return Ok(Err(TsqlError::new(
                                50000,
                                "a WHILE loop ran more than a million times; stopped",
                            )));
                        }
                        match self.run(text, body, params).await? {
                            Ok(Flow::Break) => break,
                            Ok(Flow::Next) | Ok(Flow::Continue) => {}
                            Ok(other) => return Ok(Ok(other)),
                            Err(e) => return Ok(Err(e)),
                        }
                    }
                    Ok(Ok(Flow::Next))
                }
                Stmt::Try { body, catch } => {
                    self.try_depth += 1;
                    let r = self.run_list(text, body, params).await?;
                    self.try_depth -= 1;
                    match r {
                        Ok(flow) => Ok(Ok(flow)),
                        Err(e) => {
                            self.last_error = Some(e.clone());
                            self.error_number = e.number;
                            self.run_list(text, catch, params).await
                        }
                    }
                }
            }
        })
    }

    async fn condition(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<bool, TsqlError> {
        let bound;
        let tokens = match self.bind_names(tokens) {
            Some(b) => {
                bound = b;
                &bound[..]
            }
            None => tokens,
        };
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let sql = translate::condition_query(tokens.to_vec(), &Scope { s: self, params })?;
        let bound = bound_values(params);
        let (_, rows) = self
            .fetch(&sql, &bound)
            .await
            .map_err(|e| self.engine_error(&e))?;
        Ok(rows
            .first()
            .and_then(|r| r.first().cloned().flatten())
            .as_deref()
            == Some("1"))
    }

    /// Evaluates an expression to a value.
    async fn evaluate(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Value, TsqlError> {
        let bound;
        let tokens = match self.bind_names(tokens) {
            Some(b) => {
                bound = b;
                &bound[..]
            }
            None => tokens,
        };
        match tokens {
            [t] => match &t.tok {
                Tok::Str(s) => return Ok(Value::Str(String::from_utf8_lossy(s).into_owned())),
                Tok::Num(n) => {
                    return Ok(n
                        .parse::<i64>()
                        .map(Value::Int)
                        .unwrap_or(Value::Num(n.clone())));
                }
                Tok::UserVar(name) if params.is_none() => {
                    return self.vars.get(name).map(|v| v.value.clone()).ok_or_else(|| {
                        TsqlError::new(
                            137,
                            format!("Must declare the scalar variable \"@{name}\"."),
                        )
                    });
                }
                Tok::Word(w) if w.eq_ignore_ascii_case("NULL") => return Ok(Value::Null),
                _ => {}
            },
            [m, t] if m.is_op("-") => {
                if let Tok::Num(n) = &t.tok {
                    return Ok(n
                        .parse::<i64>()
                        .map(|v| Value::Int(-v))
                        .unwrap_or(Value::Num(format!("-{n}"))));
                }
            }
            _ => {}
        }
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let sql = translate::value_query(tokens.to_vec(), &Scope { s: self, params })?;
        let bound = bound_values(params);
        let (oids, rows) = self
            .fetch(&sql, &bound)
            .await
            .map_err(|e| self.engine_error(&e))?;
        Ok(value_of(
            oids.first().copied().unwrap_or(25),
            rows.first().and_then(|r| r.first().cloned()).flatten(),
        ))
    }

    fn assign(&mut self, name: &str, value: Value) -> Result<(), TsqlError> {
        let var = self.vars.get_mut(name).ok_or_else(|| {
            TsqlError::new(
                137,
                format!("Must declare the scalar variable \"@{name}\"."),
            )
        })?;
        var.value = match (&value, var.numeric, var.string) {
            (Value::Str(s), true, _) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .unwrap_or_else(|_| Value::Num(s.trim().to_string())),
            (Value::Int(n), _, true) => Value::Str(n.to_string()),
            (Value::Num(n), _, true) => Value::Str(n.clone()),
            _ => value,
        };
        Ok(())
    }

    async fn simple(
        &mut self,
        text: &str,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> io::Result<Result<Flow, TsqlError>> {
        let bound;
        let tokens = match self.bind_names(tokens) {
            Some(b) => {
                bound = b;
                &bound[..]
            }
            None => tokens,
        };
        let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
        let next = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
        let made_temp = temp_table_made(tokens);
        // SET FMTONLY ON: a query answers its columns; nothing else runs.
        if self.fmtonly
            && !matches!(
                head.as_str(),
                "SET" | "SELECT" | "DECLARE" | "IF" | "WHILE" | "BEGIN" | "EXEC" | "EXECUTE"
            )
        {
            self.statement_done(0, 0, 0);
            return Ok(Ok(Flow::Next));
        }
        if self.fmtonly && head == "SELECT" {
            self.ensure_names().await;
            if let Some(columns) = self.declared_columns(tokens) {
                tds::col_metadata(self.wire.reply(), &columns);
                self.statement_done(if self.nocount { 0 } else { tds::DONE_COUNT }, 0xc1, 0);
                return Ok(Ok(Flow::Next));
            }
        }
        // Logins, users, roles and permissions, as the engine's role
        // statements (`accounts`).
        let account = match head.as_str() {
            "CREATE" | "ALTER" | "DROP" | "GRANT" | "REVOKE" | "DENY" => super::accounts::translate(tokens),
            _ => None,
        };
        let r: Result<Flow, TsqlError> = match head.as_str() {
            _ if account.is_some() => match account.expect("checked just above") {
                Ok(statements) => {
                    let mut out = Ok(Flow::Next);
                    for sql in statements {
                        if let Err(e) = self.silent(&sql).await {
                            out = Err(error_from_engine(&e));
                            break;
                        }
                    }
                    self.statement_done(0, 0, 0);
                    out
                }
                Err(e) => Err(e),
            },
            "INSERT" if next == "BULK" => self.insert_bulk(tokens),
            "DECLARE" => self.declare(tokens, params).await,
            "DROP" if matches!(next.as_str(), "PROC" | "PROCEDURE") => {
                self.drop_procedure(tokens).await
            }
            "OPEN" | "CLOSE" | "FETCH" | "DEALLOCATE" => {
                self.cursor_statement(&head, tokens, params).await
            }
            "MERGE" if !super::merge::is_insert_only(tokens) => self.merge(tokens, params).await,
            "SET" if matches!(tokens.get(1).map(|t| &t.tok), Some(Tok::UserVar(_))) => {
                let Some(Tok::UserVar(name)) = tokens.get(1).map(|t| &t.tok) else {
                    unreachable!()
                };
                let name = name.clone();
                let op = tokens.get(2).map(|t| t.tok.clone());
                match op {
                    Some(Tok::Op(o)) if o == "=" => match self.evaluate(&tokens[3..], params).await
                    {
                        Ok(v) => self.assign(&name, v).map(|_| Flow::Next),
                        Err(e) => Err(e),
                    },
                    Some(Tok::Op(o)) if o.len() == 2 && o.ends_with('=') => {
                        // SET @x += 1
                        let mut expr =
                            vec![Token::new(Tok::UserVar(name.clone())), Token::op(&o[..1])];
                        expr.push(Token::op("("));
                        expr.extend(tokens[3..].iter().cloned());
                        expr.push(Token::op(")"));
                        match self.evaluate(&expr, params).await {
                            Ok(v) => self.assign(&name, v).map(|_| Flow::Next),
                            Err(e) => Err(e),
                        }
                    }
                    _ => Err(TsqlError::syntax("SET @variable needs '='")),
                }
                .map(|f| {
                    self.statement_done(0, 0, 0);
                    f
                })
            }
            "SET" => self.set_option(tokens).await.map(|f| {
                self.statement_done(0, 0, 0);
                f
            }),
            "PRINT" => match self.evaluate(&tokens[1..], params).await {
                Ok(v) => {
                    let msg = v.text().unwrap_or_default();
                    self.info_token(0, &msg);
                    Ok(Flow::Next)
                }
                Err(e) => Err(e),
            },
            "RAISERROR" => self.raiserror(tokens, params).await,
            "THROW" => {
                if tokens.len() == 1 {
                    Err(self.last_error.clone().unwrap_or_else(|| TsqlError::new(10704, "To rethrow an error, a THROW statement must be used inside a CATCH block.")))
                } else {
                    let args = split_commas(&tokens[1..]);
                    let number = render_value(&args[0])
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(50000);
                    let message = match args.get(1) {
                        Some(a) => self
                            .evaluate(a, params)
                            .await
                            .map(|v| v.text().unwrap_or_default()),
                        None => Ok(String::new()),
                    };
                    match message {
                        Ok(m) => Err(TsqlError {
                            number,
                            class: 16,
                            state: 1,
                            message: m,
                        }),
                        Err(e) => Err(e),
                    }
                }
            }
            "RETURN" if tokens.len() > 1 => match self.evaluate(&tokens[1..], params).await {
                Ok(v) => {
                    let n = v
                        .text()
                        .and_then(|t| t.trim().parse::<f64>().ok())
                        .unwrap_or(0.0);
                    self.return_value = Some(n as i32);
                    Ok(Flow::Return)
                }
                Err(e) => Err(e),
            },
            "RETURN" => Ok(Flow::Return),
            "BREAK" => Ok(Flow::Break),
            "CONTINUE" => Ok(Flow::Continue),
            "USE" => {
                let db = tokens
                    .get(1)
                    .and_then(|t| t.ident())
                    .unwrap_or("")
                    .to_string();
                self.use_database(&db).await
            }
            "BEGIN" if matches!(next.as_str(), "TRAN" | "TRANSACTION" | "DISTRIBUTED") => {
                let r = self.begin(None).await;
                self.statement_done(0, 0, 0);
                r.map(|_| Flow::Next)
            }
            "COMMIT" => {
                let r = self.commit().await;
                self.statement_done(0, 0, 0);
                r.map(|_| Flow::Next)
            }
            "ROLLBACK" => {
                let mut name = None;
                let mut k = 1;
                if matches!(next.as_str(), "TRAN" | "TRANSACTION" | "WORK") {
                    k = 2;
                }
                if let Some(t) = tokens.get(k) {
                    name = t.ident().map(str::to_string);
                }
                let r = self.rollback(name.as_deref()).await;
                self.statement_done(0, 0, 0);
                r.map(|_| Flow::Next)
            }
            "SAVE" => {
                let name = tokens
                    .get(2)
                    .and_then(|t| t.ident())
                    .unwrap_or("")
                    .to_lowercase();
                if self.trancount == 0 {
                    Err(TsqlError::new(
                        628,
                        "Cannot issue SAVE TRANSACTION when there is no active transaction.",
                    ))
                } else {
                    let r = self
                        .silent(&format!("SAVEPOINT {}", ident_sql(&name)))
                        .await
                        .map_err(|e| self.engine_error(&e));
                    if r.is_ok() {
                        self.savepoints.push(name);
                    }
                    self.statement_done(0, 0, 0);
                    r.map(|_| Flow::Next)
                }
            }
            "EXEC" | "EXECUTE" => self.exec_statement(tokens, params).await,
            "WAITFOR" => {
                let delay = tokens.get(2).and_then(|t| match &t.tok {
                    Tok::Str(s) => Some(String::from_utf8_lossy(s).into_owned()),
                    _ => None,
                });
                match (next.as_str(), delay) {
                    ("DELAY", Some(d)) => {
                        let parts: Vec<f64> = d.split(':').filter_map(|p| p.parse().ok()).collect();
                        let secs = parts.iter().fold(0.0, |acc, p| acc * 60.0 + p);
                        tokio::time::sleep(std::time::Duration::from_secs_f64(secs.min(3600.0)))
                            .await;
                        Ok(Flow::Next)
                    }
                    _ => Err(TsqlError::unsupported("WAITFOR other than DELAY")),
                }
            }
            "GRANT" | "DENY" | "REVOKE" | "DBCC" | "BACKUP" | "RESTORE" | "GOTO" | "KILL" => {
                Err(TsqlError::unsupported(head))
            }
            "ALTER" if next == "DATABASE" => {
                // SET SINGLE_USER, RECOVERY, COLLATE …: storage and access
                // settings the engine does not have.
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "CHECKPOINT" => {
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "SELECT"
                if (matches!(tokens.get(1).map(|t| &t.tok), Some(Tok::UserVar(_)))
                    && tokens.get(2).is_some_and(|t| t.is_op("=")))
                    || (next == "DISTINCT"
                        && matches!(tokens.get(2).map(|t| &t.tok), Some(Tok::UserVar(_)))
                        && tokens.get(3).is_some_and(|t| t.is_op("="))) =>
            {
                self.assignment_select(text, tokens, params).await
            }
            _ => {
                if let Some(p) = params {
                    p.used.lock().clear();
                }
                let translated =
                    translate::translate(text, tokens.to_vec(), &Scope { s: self, params });
                match &translated {
                    Ok(f) => log::debug!(
                        "T-SQL {:?} -> {:?}",
                        crate::sqlcompat::render_text(tokens),
                        f.statements
                    ),
                    Err(e) => log::debug!(
                        "T-SQL {:?} refused: {}",
                        crate::sqlcompat::render_text(tokens),
                        e.message
                    ),
                }
                let translated = match translated {
                    Ok(mut f) if self.fmtonly && f.kind == Kind::Query => {
                        if let Some(last) = f.statements.last_mut() {
                            // LIMIT 0 on the query itself keeps its columns'
                            // declared types (a derived table loses their
                            // precision and length).
                            let lower = last.to_ascii_lowercase();
                            *last = if lower.contains(" limit ")
                                || lower.contains(" union ")
                                || lower.contains(" for update")
                            {
                                format!("SELECT * FROM ({last}) AS __fmtonly LIMIT 0")
                            } else {
                                format!("{last} LIMIT 0")
                            };
                        }
                        Ok(f)
                    }
                    other => other,
                };
                match translated {
                    Ok(forward) => match self.forward(forward, params).await? {
                        Ok(()) => Ok(Flow::Next),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
        };
        if let (Ok(_), Some(name)) = (&r, made_temp) {
            self.note_temp(name);
        }
        Ok(r)
    }

    async fn declare(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        if let Some(cursor) = routines::parse_cursor(tokens) {
            return self.declare_cursor(cursor?);
        }
        if matches!(tokens.get(1).map(|t| &t.tok), Some(Tok::UserVar(_)))
            && (crate::sqlcompat::kw(tokens, 2, "TABLE")
                || (crate::sqlcompat::kw(tokens, 2, "AS")
                    && crate::sqlcompat::kw(tokens, 3, "TABLE")))
        {
            return self.declare_table_var(tokens).await;
        }
        for part in split_commas(&tokens[1..]) {
            let Some(Tok::UserVar(name)) = part.first().map(|t| &t.tok) else {
                return Err(TsqlError::unsupported("DECLARE of a cursor"));
            };
            let name = name.clone();
            let mut k = 1;
            if part.get(k).is_some_and(|t| t.is_word("AS")) {
                k += 1;
            }
            let ty = part.get(k).and_then(|t| t.upper()).unwrap_or_default();
            if ty == "TABLE" {
                return Err(TsqlError::syntax("a table variable is declared on its own"));
            }
            if ty == "CURSOR" {
                return Err(TsqlError::unsupported("cursor variables"));
            }
            let string = matches!(
                ty.as_str(),
                "CHAR"
                    | "NCHAR"
                    | "VARCHAR"
                    | "NVARCHAR"
                    | "TEXT"
                    | "NTEXT"
                    | "SYSNAME"
                    | "XML"
                    | "UNIQUEIDENTIFIER"
            );
            let numeric = matches!(
                ty.as_str(),
                "INT"
                    | "BIGINT"
                    | "SMALLINT"
                    | "TINYINT"
                    | "BIT"
                    | "DECIMAL"
                    | "NUMERIC"
                    | "MONEY"
                    | "FLOAT"
                    | "REAL"
            );
            self.vars.insert(
                name.clone(),
                Var {
                    value: Value::Null,
                    string,
                    numeric,
                },
            );
            if let Some(eq) = part.iter().position(|t| t.is_op("=")) {
                let v = self.evaluate(&part[eq + 1..], params).await?;
                self.assign(&name, v)?;
            }
        }
        Ok(Flow::Next)
    }

    async fn assignment_select(
        &mut self,
        text: &str,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let Some((sql, names, accumulates)) =
            translate::assignment_select(text, tokens, &Scope { s: self, params })?
        else {
            return Err(TsqlError::syntax("an assignment SELECT was expected"));
        };
        let bound = bound_values(params);
        let (oids, rows) = self
            .fetch(&sql, &bound)
            .await
            .map_err(|e| self.engine_error(&e))?;
        if accumulates && rows.len() > 1 {
            return Err(TsqlError::unsupported(
                "SELECT @v = … @v … over more than one row (it folds row by row; use STRING_AGG or SUM)",
            ));
        }
        // The last row's values, as SQL Server leaves them.
        if let Some(row) = rows.last() {
            for (k, name) in names.iter().enumerate() {
                let v = value_of(
                    oids.get(k).copied().unwrap_or(25),
                    row.get(k).cloned().flatten(),
                );
                self.assign(name, v)?;
            }
        }
        self.rowcount = rows.len() as i64;
        self.statement_done(
            if self.nocount { 0 } else { tds::DONE_COUNT },
            0xc1,
            rows.len() as u64,
        );
        Ok(Flow::Next)
    }

    async fn raiserror(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        let mut inner = &tokens[1..];
        if inner.first().is_some_and(|t| t.is_op("(")) {
            let close = crate::sqlcompat::matching_paren(inner, 0).unwrap_or(inner.len() - 1);
            inner = &inner[1..close];
        }
        let args = split_commas(inner);
        if args.len() < 3 {
            return Err(TsqlError::syntax(
                "RAISERROR needs a message, a severity and a state",
            ));
        }
        let msg = self
            .evaluate(&args[0], params)
            .await?
            .text()
            .unwrap_or_default();
        let severity = render_value(&args[1])
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(16);
        let state = render_value(&args[2])
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(1);
        let mut values = Vec::new();
        for a in args.iter().skip(3) {
            values.push(
                self.evaluate(a, params)
                    .await?
                    .text()
                    .unwrap_or_else(|| "(null)".into()),
            );
        }
        // %s, %d, %i substituted in order.
        let mut text = String::new();
        let mut it = values.into_iter();
        let mut chars = msg.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '%' {
                let mut spec = String::new();
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if n.is_ascii_alphabetic() {
                        spec.push(n);
                        break;
                    }
                }
                if spec.is_empty() {
                    text.push('%');
                } else {
                    text.push_str(&it.next().unwrap_or_default());
                }
            } else {
                text.push(c);
            }
        }
        if severity <= 10 {
            self.info_token(50000, &text);
            Ok(Flow::Next)
        } else {
            Err(TsqlError {
                number: 50000,
                class: severity.min(18),
                state,
                message: text,
            })
        }
    }

    async fn use_database(&mut self, shown: &str) -> Result<Flow, TsqlError> {
        let db = resolve_database(&self.shared, shown).to_string();
        let db = db.as_str();
        if self.bridge().in_block() {
            return Err(TsqlError::new(
                574,
                "USE inside a transaction is not supported: COMMIT or ROLLBACK first.",
            ));
        }
        if self.shared.catalog.get(db).is_none() {
            return Err(TsqlError::new(
                911,
                format!(
                    "Database '{db}' does not exist. Make sure that the name is entered correctly."
                ),
            ));
        }
        let old = self.bridge().database_name.clone();
        if old != db {
            self.prepared.clear();
            self.open(db).await?;
            self.database_shown = self.database_names().await.get(db).cloned().unwrap_or_default();
        }
        tds::env_change_text(self.wire.reply(), 1, shown, &old);
        self.info_token(5701, &format!("Changed database context to '{shown}'."));
        self.statement_done(0, 0, 0);
        Ok(Flow::Next)
    }

    async fn set_option(&mut self, tokens: &[Token]) -> Result<Flow, TsqlError> {
        let words: Vec<String> = tokens.iter().skip(1).filter_map(|t| t.upper()).collect();
        let first = words.first().cloned().unwrap_or_default();
        let on = || words.last().map(|w| w == "ON").unwrap_or(false);
        match first.as_str() {
            "NOCOUNT" => self.nocount = on(),
            "XACT_ABORT" => self.xact_abort = on(),
            "IMPLICIT_TRANSACTIONS" => self.implicit_transactions = on(),
            "TRANSACTION" => {
                let at = words.iter().position(|w| w == "LEVEL").ok_or_else(|| {
                    TsqlError::syntax("SET TRANSACTION ISOLATION LEVEL was expected")
                })?;
                let level = match (
                    words.get(at + 1).map(String::as_str),
                    words.get(at + 2).map(String::as_str),
                ) {
                    (Some("READ"), Some("UNCOMMITTED")) => "READ UNCOMMITTED",
                    (Some("READ"), Some("COMMITTED")) => "READ COMMITTED",
                    (Some("REPEATABLE"), _) => "REPEATABLE READ",
                    (Some("SERIALIZABLE"), _) | (Some("SNAPSHOT"), _) => "SERIALIZABLE",
                    _ => return Err(TsqlError::syntax("an isolation level was expected")),
                };
                self.silent(&format!(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"
                ))
                .await
                .map_err(|e| self.engine_error(&e))?;
            }
            "LOCK_TIMEOUT" => {
                let ms: i64 = tokens
                    .get(2)
                    .and_then(|t| match &t.tok {
                        Tok::Num(n) => n.parse().ok(),
                        _ => None,
                    })
                    .or_else(|| {
                        if tokens.get(2).is_some_and(|t| t.is_op("-")) {
                            Some(-1)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(-1);
                let value = if ms < 0 { 0 } else { ms.max(1) };
                self.silent(&format!("SET lock_timeout = {value}"))
                    .await
                    .map_err(|e| self.engine_error(&e))?;
            }
            "ROWCOUNT" => {
                let n = render_value(&tokens[2..]).unwrap_or_default();
                if n != "0" {
                    return Err(TsqlError::unsupported("SET ROWCOUNT (use TOP)"));
                }
            }
            "FMTONLY" => self.fmtonly = on(),
            "NOEXEC" | "PARSEONLY" if on() => {
                return Err(TsqlError::unsupported(format!("SET {first} ON")));
            }
            "SHOWPLAN_ALL" | "SHOWPLAN_TEXT" | "SHOWPLAN_XML" if on() => {
                return Err(TsqlError::unsupported(format!("SET {first} ON")));
            }
            // Accepted, with no effect here: ANSI_NULLS, QUOTED_IDENTIFIER,
            // ANSI_WARNINGS, ANSI_PADDING, ARITHABORT, CONCAT_NULL_YIELDS_NULL,
            // TEXTSIZE, DATEFORMAT, DATEFIRST, LANGUAGE, DEADLOCK_PRIORITY,
            // STATISTICS, IDENTITY_INSERT, CONTEXT_INFO, ...
            _ => {}
        }
        Ok(Flow::Next)
    }

    /// EXEC: a string as a batch, or one of the system procedures.
    async fn exec_statement(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        let rest = &tokens[1..];
        if rest.first().is_some_and(|t| t.is_op("(")) {
            // EXEC('…') / EXEC(@sql)
            let close = crate::sqlcompat::matching_paren(rest, 0)
                .ok_or_else(|| TsqlError::syntax("unbalanced EXEC"))?;
            let sql = self
                .evaluate(&rest[1..close], params)
                .await?
                .text()
                .unwrap_or_default();
            return self.nested_batch(&sql, None).await;
        }
        // EXEC @rc = proc …
        let mut rest = rest;
        let mut status_var = None;
        if let (Some(Tok::UserVar(v)), Some(eq)) = (rest.first().map(|t| &t.tok), rest.get(1)) {
            if eq.is_op("=") {
                status_var = Some(v.clone());
                rest = &rest[2..];
            }
        }
        let mut k = 0;
        let mut name = String::new();
        while rest.get(k).is_some_and(|t| t.is_op(".")) {
            k += 1;
        }
        while let Some(t) = rest.get(k) {
            if let Some(i) = t.ident() {
                name = i.to_lowercase();
                k += 1;
                if rest.get(k).is_some_and(|d| d.is_op(".")) {
                    k += 1;
                    continue;
                }
            }
            break;
        }
        let args = split_commas(&rest[k..]);
        let mut values = Vec::new();
        let mut named: Vec<(Option<String>, Value)> = Vec::new();
        let mut proc_args = Vec::new();
        let mut out_vars: Vec<Option<String>> = Vec::new();
        for a in &args {
            if a.is_empty() {
                continue;
            }
            let mut a = &a[..];
            let mut output = false;
            if a.last()
                .is_some_and(|t| t.is_word("OUTPUT") || t.is_word("OUT"))
            {
                output = true;
                a = &a[..a.len() - 1];
            }
            let mut arg_name = None;
            if let (Some(Tok::UserVar(n)), Some(eq)) = (a.first().map(|t| &t.tok), a.get(1)) {
                if eq.is_op("=") {
                    arg_name = Some(n.clone());
                    a = &a[2..];
                }
            }
            let out_var = match (output, a) {
                (false, _) => None,
                (true, [t]) => match &t.tok {
                    Tok::UserVar(v) => Some(v.clone()),
                    _ => {
                        return Err(TsqlError::new(
                            179,
                            "Cannot use the OUTPUT option when passing a constant to a stored procedure.",
                        ));
                    }
                },
                (true, _) => {
                    return Err(TsqlError::new(
                        179,
                        "Cannot use the OUTPUT option when passing a constant to a stored procedure.",
                    ));
                }
            };
            let v = if matches!(a, [t] if t.is_word("DEFAULT")) {
                None
            } else {
                Some(self.evaluate(a, params).await?)
            };
            let plain = v.clone().unwrap_or(Value::Null);
            if arg_name.is_none() {
                values.push(plain.clone());
            }
            named.push((arg_name.clone(), plain));
            proc_args.push(ProcArg {
                name: arg_name,
                value: v,
                output,
            });
            out_vars.push(out_var);
        }
        match name.as_str() {
            "sp_executesql" => {
                let stmt = named
                    .first()
                    .and_then(|(_, v)| v.text())
                    .unwrap_or_default();
                let decl = named.get(1).and_then(|(_, v)| v.text()).unwrap_or_default();
                let decls = parse_declarations(&decl)?;
                let mut vals = vec![ParamValue::Null; decls.len()];
                let mut position = 0;
                for (n, v) in named.iter().skip(2) {
                    let k = match n {
                        Some(n) => decls.iter().position(|d| d.0 == *n).ok_or_else(|| {
                            TsqlError::new(
                                8145,
                                format!("@{n} is not a parameter for procedure sp_executesql."),
                            )
                        })?,
                        None => {
                            position += 1;
                            position - 1
                        }
                    };
                    if k < vals.len() {
                        vals[k] = match v {
                            Value::Null => ParamValue::Null,
                            Value::Bytes(b) => ParamValue::Bytes(b.clone()),
                            other => ParamValue::Text(other.text().unwrap_or_default()),
                        };
                    }
                }
                let ctx = ParamCtx {
                    decls,
                    values: vals,
                    used: parking_lot::Mutex::new(Vec::new()),
                };
                self.nested_batch(&stmt, Some(&ctx)).await
            }
            "sp_rename" => {
                let old = values.first().and_then(|v| v.text()).unwrap_or_default();
                let new = values.get(1).and_then(|v| v.text()).unwrap_or_default();
                let kind = values
                    .get(2)
                    .and_then(|v| v.text())
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let parts: Vec<String> = lex::name_parts(&old)
                    .into_iter()
                    .map(|p| p.to_lowercase())
                    .filter(|p| p != "dbo")
                    .collect();
                let new = lex::encode_dots(new.trim_matches(['[', ']']).to_string());
                // The declared spellings follow the rename (`NAMES_TABLE`).
                let record = translate::rename_records(&parts, &new, &kind);
                let sql = if kind == "COLUMN" && parts.len() >= 2 {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        ident_sql(&parts[parts.len() - 2]),
                        ident_sql(&parts[parts.len() - 1]),
                        ident_sql(&new.to_lowercase())
                    )
                } else if kind == "INDEX" && parts.len() >= 2 {
                    // Index names are stored per table (`<table>__<name>`).
                    let table = &parts[parts.len() - 2];
                    format!(
                        "ALTER INDEX {} RENAME TO {}",
                        ident_sql(&crate::sqlcompat::index_name(table, &parts[parts.len() - 1])),
                        ident_sql(&crate::sqlcompat::index_name(table, &new))
                    )
                } else if kind == "INDEX" {
                    return Err(TsqlError::syntax("sp_rename of an index needs N'table.index'"));
                } else {
                    format!(
                        "ALTER TABLE {} RENAME TO {}",
                        ident_sql(parts.last().map(String::as_str).unwrap_or("")),
                        ident_sql(&new.to_lowercase())
                    )
                };
                self.silent(&sql).await.map_err(|e| self.engine_error(&e))?;
                for r in &record {
                    // A rename of a table this protocol did not record
                    // (no names table yet) records it now.
                    let _ = self.silent_as(r, true).await;
                }
                self.names.dirty = true;
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "sp_rdb_record_names" => {
                // The declared names a script would have recorded, for a
                // database made before they were (`names_from_script`).
                let script = values
                    .first()
                    .or_else(|| named.iter().find(|(n, _)| n.as_deref() == Some("script")).map(|(_, v)| v))
                    .and_then(|v| v.text())
                    .unwrap_or_default();
                let statements = translate::names_from_script(&script, &Scope { s: self, params: None })?;
                for sql in &statements {
                    self.silent_as(sql, true).await.map_err(|e| self.engine_error(&e))?;
                }
                self.names.dirty = true;
                let recorded = statements.iter().filter(|s| s.starts_with("INSERT INTO")).count();
                self.info_token(0, &format!("Declared names recorded from {recorded} statement(s)."));
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "sp_tablecollations" | "sp_tablecollations_90" | "sp_tablecollations_100" => {
                let table = values
                    .first()
                    .and_then(|v| v.text())
                    .map(|t| {
                        routines::object_name_at(&lex::lex(&t).unwrap_or_default(), 0)
                            .map(|(n, _)| n)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                self.table_collations(&table).await
            }
            "sp_getapplock" | "sp_releaseapplock" | "applock_test" => {
                let arg = |n: &str, at: usize| -> Option<String> {
                    named
                        .iter()
                        .find(|(k, _)| k.as_deref() == Some(n))
                        .map(|(_, v)| v)
                        .or_else(|| values.get(at))
                        .and_then(|v| v.text())
                };
                let releasing = name == "sp_releaseapplock";
                let resource = arg("resource", 0).ok_or_else(|| {
                    TsqlError::new(201, &format!("Procedure or function '{name}' expects parameter '@Resource', which was not supplied."))
                })?;
                let (mode_at, owner_at) = if releasing { (usize::MAX, 1) } else { (1, 2) };
                let mode = arg("lockmode", mode_at).unwrap_or_default().to_ascii_lowercase();
                if !releasing && mode.is_empty() {
                    return Err(TsqlError::new(201, &format!("Procedure or function '{name}' expects parameter '@LockMode', which was not supplied.")));
                }
                let owner = match arg("lockowner", owner_at)
                    .unwrap_or_else(|| "Transaction".into())
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "session" => super::applock::Owner::Session,
                    "transaction" => super::applock::Owner::Transaction,
                    other => {
                        return Err(TsqlError::new(
                            1222,
                            &format!("The parameter @LockOwner '{other}' is not valid."),
                        ));
                    }
                };
                if owner == super::applock::Owner::Transaction
                    && self.trancount == 0
                    && !self.bridge().in_block()
                {
                    return Err(TsqlError::new(
                        1223,
                        &format!(
                            "Cannot {} the application lock (Database Principal: 'public', Resource: '{resource}') because there is not a current transaction.",
                            if releasing { "release" } else { "acquire" }
                        ),
                    ));
                }
                let db = Arc::as_ptr(&self.bridge().database) as usize;
                let status = if name == "applock_test" {
                    i32::from(!super::applock::holds(db, &resource, self.conn_id))
                } else if releasing {
                    let r = super::applock::release(db, &resource, self.conn_id, owner);
                    if r < 0 {
                        return Err(TsqlError::new(
                            1223,
                            &format!("Cannot release the application lock (Database Principal: 'public', Resource: '{resource}') because it is not currently held."),
                        ));
                    }
                    r
                } else {
                    let timeout = arg("locktimeout", 3)
                        .and_then(|t| t.trim().parse::<i64>().ok())
                        .filter(|t| *t >= 0)
                        .map(|t| std::time::Duration::from_millis(t as u64));
                    let shared = matches!(mode.as_str(), "shared" | "intentshared");
                    super::applock::get(db, &resource, self.conn_id, owner, shared, timeout).await
                };
                if let Some(var) = status_var {
                    self.assign(&var, Value::Int(status as i64))?;
                }
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "sp_reset_connection" | "sp_set_session_context" | "sp_setapprole" => {
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "" => Err(TsqlError::syntax("EXEC needs a procedure")),
            other => {
                let outcome = self.call_procedure(other, proc_args).await?;
                for (k, v) in outcome.outputs {
                    if let Some(Some(var)) = out_vars.get(k) {
                        self.assign(var, v)?;
                    }
                }
                if let Some(var) = status_var {
                    self.assign(&var, Value::Int(outcome.status as i64))?;
                }
                Ok(match outcome.flow {
                    Flow::Abort => Flow::Abort,
                    _ => Flow::Next,
                })
            }
        }
    }

    /// A batch inside this one (EXEC, sp_executesql): its errors are its
    /// own, and do not end the outer batch.
    async fn nested_batch(
        &mut self,
        sql: &str,
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        let tokens = lex::lex(sql).map_err(|e| TsqlError::syntax(e.0))?;
        self.refresh_routines_for(sql).await;
        if let Some(r) = self.procedure_ddl(sql, &tokens).await {
            r?;
            self.statement_done(0, 0, 0);
            return Ok(Flow::Next);
        }
        let stmts = translate::parse_batch(&tokens)?;
        let owned = sql.to_string();
        self.scopes.push(LocalScope {
            owns_temps: true,
            ..Default::default()
        });
        let r = self.run_list(&owned, &stmts, params).await;
        self.end_scope().await;
        match r {
            Ok(Ok(Flow::Return)) | Ok(Ok(Flow::Next)) => Ok(Flow::Next),
            Ok(Ok(Flow::Abort)) => Ok(Flow::Abort),
            Ok(Ok(other)) => Ok(other),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(TsqlError::new(50000, e.to_string())),
        }
    }

    // -----------------------------------------------------------------------
    // RPC
    // -----------------------------------------------------------------------

    async fn rpc(&mut self, message: &Message) -> io::Result<()> {
        self.ensure_names().await;
        let mut r = Reader::new(&message.payload);
        if message.payload.len() >= 4 {
            let total = u32::from_le_bytes(message.payload[0..4].try_into().unwrap()) as usize;
            if total >= 22 && total <= message.payload.len() {
                tds::all_headers(&mut r)?;
            }
        }
        self.in_rpc = true;
        loop {
            let name_len = r.u16()?;
            let proc_name = if name_len == 0xffff {
                match r.u16()? {
                    10 => "sp_executesql".to_string(),
                    11 => "sp_prepare".to_string(),
                    12 => "sp_execute".to_string(),
                    13 => "sp_prepexec".to_string(),
                    14 => "sp_prepexecrpc".to_string(),
                    15 => "sp_unprepare".to_string(),
                    other => format!("sp_cursor#{other}"),
                }
            } else {
                r.utf16(name_len as usize)?.to_lowercase()
            };
            let _flags = r.u16()?;
            let params = match tds::read_params(&mut r) {
                Ok(p) => p,
                Err(e) => {
                    self.error_token(&TsqlError::new(8016, format!("The incoming tabular data stream (TDS) remote procedure call (RPC) protocol stream is incorrect: {e}")));
                    tds::done(
                        self.wire.reply(),
                        tds::TOKEN_DONEPROC,
                        tds::DONE_ERROR,
                        0,
                        0,
                    );
                    self.in_rpc = false;
                    return Ok(());
                }
            };
            let more = r.remaining() > 0 && matches!(r.buf[r.pos], 0x80 | 0xff);
            self.rpc_outputs.clear();
            let (status, failed) = self.call(&proc_name, params).await?;
            tds::return_status(self.wire.reply(), status);
            for (ordinal, name, oid, value) in std::mem::take(&mut self.rpc_outputs) {
                let bytes = match value {
                    Value::Null => None,
                    Value::Bytes(b) => Some(b),
                    other => other.text().map(String::into_bytes),
                };
                tds::return_value(self.wire.reply(), ordinal, &name, oid, bytes.as_deref());
            }
            let mut done = if failed { tds::DONE_ERROR } else { 0 };
            if more {
                done |= tds::DONE_MORE;
            }
            if self.trancount > 0 {
                done |= tds::DONE_INXACT;
            }
            tds::done(self.wire.reply(), tds::TOKEN_DONEPROC, done, 0, 0);
            if !more {
                break;
            }
            r.pos += 1;
        }
        self.in_rpc = false;
        self.last_done = None;
        Ok(())
    }

    /// One RPC call; its return status.
    async fn call(&mut self, name: &str, params: Vec<tds::Param>) -> io::Result<(i32, bool)> {
        let text = |p: Option<&tds::Param>| match p.map(|p| &p.value) {
            Some(ParamValue::Text(t)) => t.clone(),
            Some(ParamValue::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => String::new(),
        };
        let int = |p: Option<&tds::Param>| match p.map(|p| &p.value) {
            Some(ParamValue::Text(t)) => t.parse::<i32>().ok(),
            _ => None,
        };
        let failed_before = self.error_number;
        self.error_number = 0;
        let mut proc_status = None;
        let result: Result<(), TsqlError> = match name {
            "sp_executesql" => {
                let sql = text(params.first());
                let decl = text(params.get(1));
                match parse_declarations(&decl) {
                    Ok(decls) => {
                        let values = bind_values(&decls, &params[2.min(params.len())..]);
                        self.run_rpc_batch(&sql, decls, values).await?
                    }
                    Err(e) => Err(e),
                }
            }
            "sp_prepare" | "sp_prepexec" => {
                let decl = text(params.get(1));
                let sql = text(params.get(2));
                match parse_declarations(&decl) {
                    Ok(decls) => {
                        let handle = self.next_handle;
                        self.next_handle += 1;
                        self.prepared.insert(
                            handle,
                            Prepared {
                                sql: sql.clone(),
                                decls: decls.clone(),
                            },
                        );
                        let r = if name == "sp_prepexec" {
                            let values = bind_values(&decls, &params[3.min(params.len())..]);
                            self.run_rpc_batch(&sql, decls, values).await?
                        } else {
                            Ok(())
                        };
                        tds::return_value_int(
                            self.wire.reply(),
                            0,
                            params.first().map(|p| p.name.as_str()).unwrap_or("@handle"),
                            handle,
                        );
                        r
                    }
                    Err(e) => Err(e),
                }
            }
            "sp_execute" => {
                match int(params.first()).and_then(|h| self.prepared.get(&h).cloned()) {
                    Some(p) => {
                        let values = bind_values(&p.decls, &params[1.min(params.len())..]);
                        self.run_rpc_batch(&p.sql, p.decls, values).await?
                    }
                    None => Err(TsqlError::new(
                        8179,
                        "Could not find prepared statement with handle.",
                    )),
                }
            }
            "sp_unprepare" => {
                if let Some(h) = int(params.first()) {
                    self.prepared.remove(&h);
                }
                Ok(())
            }
            "sp_reset_connection" => {
                self.reset(false).await;
                Ok(())
            }
            other if !procedure_name(other).starts_with("sp_") => {
                let other = procedure_name(other);
                let other = other.as_str();
                let args = params
                    .iter()
                    .map(|p| ProcArg {
                        name: (!p.name.is_empty())
                            .then(|| p.name.trim_start_matches('@').to_lowercase()),
                        value: Some(match &p.value {
                            ParamValue::Null => Value::Null,
                            ParamValue::Text(t) => value_of(p.oid, Some(t.clone())),
                            ParamValue::Bytes(b) => Value::Bytes(b.clone()),
                        }),
                        output: p.output,
                    })
                    .collect();
                match self.call_procedure(other, args).await {
                    Ok(outcome) => {
                        proc_status = Some(outcome.status);
                        for (k, v) in outcome.outputs {
                            let p = &params[k];
                            self.rpc_outputs.push((k as u16, p.name.clone(), p.oid, v));
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            other => {
                // A system procedure by name, run as EXEC would.
                let mut tokens = vec![Token::word("EXEC"), Token::raw(other.to_string())];
                for (k, p) in params.iter().enumerate() {
                    if k > 0 {
                        tokens.push(Token::op(","));
                    }
                    if !p.name.is_empty() {
                        tokens.push(Token::new(Tok::UserVar(
                            p.name.trim_start_matches('@').to_lowercase(),
                        )));
                        tokens.push(Token::op("="));
                    }
                    tokens.push(match &p.value {
                        ParamValue::Null => Token::word("NULL"),
                        ParamValue::Text(t) => Token::new(Tok::Str(t.clone().into_bytes())),
                        ParamValue::Bytes(b) => Token::new(Tok::Hex(b.clone())),
                    });
                }
                self.exec_statement(&tokens, None).await.map(|_| ())
            }
        };
        if let Err(e) = result {
            self.error_token(&e);
            self.statement_done(tds::DONE_ERROR, 0, 0);
        }
        let failed = self.error_number != 0;
        let status = match proc_status {
            Some(s) => s,
            None if failed => 1,
            None => 0,
        };
        if self.error_number == 0 {
            self.error_number = failed_before.min(0);
        }
        Ok((status, failed))
    }

    async fn run_rpc_batch(
        &mut self,
        sql: &str,
        decls: Vec<Decl>,
        values: Vec<ParamValue>,
    ) -> io::Result<Result<(), TsqlError>> {
        let ctx = ParamCtx {
            decls,
            values,
            used: parking_lot::Mutex::new(Vec::new()),
        };
        let tokens = match lex::lex(sql) {
            Ok(t) => t,
            Err(e) => return Ok(Err(TsqlError::syntax(e.0))),
        };
        self.refresh_routines_for(sql).await;
        if let Some(r) = self.procedure_ddl(sql, &tokens).await {
            if r.is_ok() {
                self.statement_done(0, 0, 0);
            }
            return Ok(r);
        }
        let stmts = match translate::parse_batch(&tokens) {
            Ok(s) => s,
            Err(e) => return Ok(Err(e)),
        };
        self.scopes.push(LocalScope {
            owns_temps: true,
            ..Default::default()
        });
        let r = self.run_list(sql, &stmts, Some(&ctx)).await;
        self.end_scope().await;
        match r? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    // -----------------------------------------------------------------------
    // Transaction manager requests
    // -----------------------------------------------------------------------

    async fn transaction_manager(&mut self, message: &Message) -> io::Result<()> {
        let mut r = Reader::new(&message.payload);
        tds::all_headers(&mut r)?;
        let request = r.u16()?;
        let result: Result<(), TsqlError> = match request {
            5 => {
                let isolation = r.u8().unwrap_or(0);
                let _name = r.b_varchar().unwrap_or_default();
                self.begin(isolation_words(isolation)).await
            }
            7 | 8 => {
                let name = r.b_varchar().unwrap_or_default();
                let flags = r.u8().unwrap_or(0);
                let mut res = if request == 7 {
                    // A TM commit ends the transaction whatever its nesting.
                    self.trancount = self.trancount.min(1);
                    self.commit().await
                } else {
                    self.rollback(if name.is_empty() { None } else { Some(&name) })
                        .await
                };
                if res.is_ok() && flags & 0x01 != 0 {
                    let isolation = r.u8().unwrap_or(0);
                    let _ = r.b_varchar();
                    res = self.begin(isolation_words(isolation)).await;
                }
                res
            }
            9 => {
                let name = r.b_varchar().unwrap_or_default().to_lowercase();
                let res = self
                    .silent(&format!("SAVEPOINT {}", ident_sql(&name)))
                    .await
                    .map_err(|e| self.engine_error(&e));
                if res.is_ok() {
                    self.savepoints.push(name);
                }
                res.map(|_| ())
            }
            other => Err(TsqlError::unsupported(format!(
                "transaction manager request {other}"
            ))),
        };
        let status = match result {
            Ok(()) => 0,
            Err(e) => {
                self.error_token(&e);
                tds::DONE_ERROR
            }
        };
        tds::done(self.wire.reply(), tds::TOKEN_DONE, status, 0, 0);
        Ok(())
    }
}

/// The `#table` a statement makes (`CREATE TABLE #t`, `SELECT … INTO #t`).
fn temp_table_made(tokens: &[Token]) -> Option<String> {
    let name_at = |i: usize| match tokens.get(i).map(|t| &t.tok) {
        Some(Tok::Quoted(q)) if q.starts_with(crate::sqlcompat::TEMP_PREFIX) => Some(q.clone()),
        _ => None,
    };
    if kw(tokens, 0, "CREATE") && kw(tokens, 1, "TABLE") {
        return name_at(2);
    }
    if kw(tokens, 0, "SELECT") {
        if let Some(into) = find_top(tokens, 1, &["INTO"]) {
            return name_at(into + 1);
        }
    }
    None
}

/// A procedure's name as an RPC request gives it (`dbo.p`, `[db].[dbo].[p]`):
/// its last part, lower case.
fn procedure_name(raw: &str) -> String {
    lex::lex(raw)
        .ok()
        .and_then(|t| routines::object_name_at(&t, 0).map(|(n, _)| n))
        .unwrap_or_else(|| raw.to_lowercase())
}

const ROUTINE_SAVEPOINT: &str = "rdb_tds_routine";

/// Procedures, table variables, cursors and the scopes they live in.
impl Session {
    fn note_temp(&mut self, name: String) {
        match self.scopes.last_mut() {
            Some(scope) if scope.owns_temps => scope.drop.push(name),
            _ => {
                if !self.session_temps.contains(&name) {
                    self.session_temps.push(name);
                }
            }
        }
    }

    /// Ends the innermost scope: its local cursors, table variables and the
    /// `#tables` it owns go.
    async fn end_scope(&mut self) {
        let Some(scope) = self.scopes.pop() else {
            return;
        };
        for c in scope.local_cursors {
            self.cursors.remove(&c);
        }
        for t in scope.drop {
            if let Err(e) = self
                .silent_guarded(&format!("DROP TABLE IF EXISTS {}", ident_sql(&t)))
                .await
            {
                log::debug!("dropping {t} at the end of its scope: {}", e.message);
            }
        }
    }

    /// The table a `#table` (`##table`: every connection's) is kept in.
    fn temp_name(&self, name: &str) -> String {
        let name = name.to_lowercase();
        match name.strip_prefix("##") {
            Some(global) => format!("{}tds_global_{global}", crate::sqlcompat::TEMP_PREFIX),
            None => format!("{}{}", self.temp_prefix, name.trim_start_matches('#')),
        }
    }

    /// The statement with each `#table` and table variable named as the
    /// table that holds it; `None` when it names none.
    fn bind_names(&self, tokens: &[Token]) -> Option<Vec<Token>> {
        let empty = HashMap::new();
        let vars = self.scopes.last().map(|s| &s.table_vars).unwrap_or(&empty);
        let mut hit = false;
        let out = tokens
            .iter()
            .enumerate()
            .map(|(k, t)| {
                let name = match &t.tok {
                    Tok::UserVar(n)
                        if vars.contains_key(n)
                            && !kw(tokens, k + 1, "TABLE")
                            && !(kw(tokens, k + 1, "AS") && kw(tokens, k + 2, "TABLE")) =>
                    {
                        vars[n].clone()
                    }
                    Tok::Quoted(q) if q.starts_with('#') => self.temp_name(q),
                    _ => return t.clone(),
                };
                hit = true;
                Token {
                    tok: Tok::Quoted(name),
                    space: t.space,
                    span: t.span,
                }
            })
            .collect();
        hit.then_some(out)
    }

    /// A connection's `#tables` and table variables go with it.
    async fn drop_temps_at_close(&mut self) {
        if self.bridge.is_none() {
            return;
        }
        let mut tables = std::mem::take(&mut self.session_temps);
        for scope in std::mem::take(&mut self.scopes) {
            tables.extend(scope.drop);
        }
        if tables.is_empty() {
            return;
        }
        if self.bridge().in_block() {
            let _ = self.silent("ROLLBACK").await;
        }
        for t in tables {
            let _ = self
                .silent(&format!("DROP TABLE IF EXISTS {}", ident_sql(&t)))
                .await;
        }
    }

    /// Translates T-SQL and runs it without answering the client.
    async fn run_quietly(&mut self, tokens: Vec<Token>) -> Result<(), TsqlError> {
        let forward = translate::translate(
            "",
            tokens,
            &Scope {
                s: self,
                params: None,
            },
        )?;
        for sql in forward.statements {
            self.silent_guarded(&sql)
                .await
                .map_err(|e| self.engine_error(&e))?;
        }
        Ok(())
    }

    /// A statement that, inside a transaction, must not fail it: run under a
    /// savepoint.
    async fn silent_guarded(&mut self, sql: &str) -> Result<String, PgNotice> {
        let guard = self.bridge().in_block();
        if guard {
            self.silent(&format!("SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await?;
        }
        let r = self.silent(sql).await;
        if guard {
            if r.is_err() {
                let _ = self
                    .silent(&format!("ROLLBACK TO SAVEPOINT {ROUTINE_SAVEPOINT}"))
                    .await;
            }
            let _ = self
                .silent(&format!("RELEASE SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await;
        }
        r
    }

    /// [`Self::silent_guarded`] for this front end's own statement on its
    /// catalogue ([`Self::silent_as`]).
    async fn system_guarded(&mut self, sql: &str) -> Result<String, PgNotice> {
        let guard = self.bridge().in_block();
        if guard {
            self.silent(&format!("SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await?;
        }
        let r = self.silent_as(sql, true).await;
        if guard {
            if r.is_err() {
                let _ = self
                    .silent(&format!("ROLLBACK TO SAVEPOINT {ROUTINE_SAVEPOINT}"))
                    .await;
            }
            let _ = self
                .silent(&format!("RELEASE SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await;
        }
        r
    }

    /// A savepoint-guarded query (as [`Self::silent_guarded`]) for this front end's own read or write of its
    /// catalogue ([`Self::silent_as`]): its values written into `sql`.
    async fn system_fetch_guarded(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<u32>, Vec<Vec<Option<String>>>), PgNotice> {
        let guard = self.bridge().in_block();
        if guard {
            self.silent(&format!("SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await?;
        }
        let r = self
            .query_bound_as(sql, &[], true)
            .await
            .map(|(fields, rows, _)| (fields.iter().map(|f| f.type_oid).collect(), rows));
        if guard {
            if r.is_err() {
                let _ = self
                    .silent(&format!("ROLLBACK TO SAVEPOINT {ROUTINE_SAVEPOINT}"))
                    .await;
            }
            let _ = self
                .silent(&format!("RELEASE SAVEPOINT {ROUTINE_SAVEPOINT}"))
                .await;
        }
        r
    }

    // -- table variables ----------------------------------------------------

    async fn declare_table_var(&mut self, tokens: &[Token]) -> Result<Flow, TsqlError> {
        let Some(Tok::UserVar(name)) = tokens.get(1).map(|t| &t.tok) else {
            return Err(TsqlError::syntax("DECLARE @table TABLE (…) was expected"));
        };
        let name = name.clone();
        let table_at = if kw(tokens, 2, "TABLE") { 2 } else { 3 };
        if self.scopes.is_empty() {
            self.scopes.push(LocalScope::default());
        }
        // Declared once: a DECLARE inside a loop does not make it again.
        if self
            .scopes
            .last()
            .is_some_and(|s| s.table_vars.contains_key(&name))
        {
            return Ok(Flow::Next);
        }
        if !tokens.get(table_at + 1).is_some_and(|t| t.is_op("(")) {
            return Err(TsqlError::syntax("a table variable needs a column list"));
        }
        self.temp_seq += 1;
        let table = self.temp_name(&format!("#tv{}_{}", self.temp_seq, name));
        let mut create = vec![
            Token::word("CREATE"),
            Token::word("TABLE"),
            Token::new(Tok::Quoted(table.clone())),
        ];
        create.extend(tokens[table_at + 1..].iter().cloned());
        self.run_quietly(create).await?;
        let scope = self.scopes.last_mut().expect("a scope");
        scope.table_vars.insert(name, table.clone());
        scope.drop.push(table);
        Ok(Flow::Next)
    }

    // -- cursors ------------------------------------------------------------

    fn declare_cursor(&mut self, decl: routines::CursorDecl) -> Result<Flow, TsqlError> {
        if self.cursors.contains_key(&decl.name) {
            return Err(TsqlError::new(
                16915,
                format!("A cursor with the name '{}' already exists.", decl.name),
            ));
        }
        if decl.local {
            if let Some(scope) = self.scopes.last_mut() {
                scope.local_cursors.push(decl.name.clone());
            }
        }
        self.cursors.insert(
            decl.name,
            Cursor {
                select: decl.select,
                scroll: decl.scroll,
                open: None,
            },
        );
        Ok(Flow::Next)
    }

    async fn cursor_statement(
        &mut self,
        head: &str,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        let mut k = 1;
        let mut how = "NEXT".to_string();
        let mut n = 0i64;
        if head == "FETCH" {
            if let Some(w) = tokens.get(k).and_then(|t| t.upper()) {
                if matches!(
                    w.as_str(),
                    "NEXT" | "PRIOR" | "FIRST" | "LAST" | "ABSOLUTE" | "RELATIVE"
                ) {
                    how = w;
                    k += 1;
                    if how == "ABSOLUTE" || how == "RELATIVE" {
                        let end = (k..tokens.len())
                            .find(|&j| kw(tokens, j, "FROM"))
                            .unwrap_or(tokens.len());
                        n = self
                            .evaluate(&tokens[k..end], params)
                            .await?
                            .text()
                            .and_then(|t| t.trim().parse::<f64>().ok())
                            .unwrap_or(0.0) as i64;
                        k = end;
                    }
                }
            }
            if kw(tokens, k, "FROM") {
                k += 1;
            }
        }
        if kw(tokens, k, "GLOBAL") {
            k += 1;
        }
        let name = tokens
            .get(k)
            .and_then(|t| t.ident())
            .map(str::to_lowercase)
            .ok_or_else(|| TsqlError::syntax(format!("{head} needs a cursor name")))?;
        k += 1;
        let missing = || {
            TsqlError::new(
                16916,
                format!("A cursor with the name '{name}' does not exist."),
            )
        };
        let not_open = || TsqlError::new(16917, "Cursor is not open.");
        match head {
            "OPEN" => {
                let cursor = self.cursors.get(&name).ok_or_else(missing)?;
                if cursor.open.is_some() {
                    return Err(TsqlError::new(16905, "The cursor is already open."));
                }
                let select = cursor.select.clone();
                let select = self.bind_names(&select).unwrap_or(select);
                if let Some(p) = params {
                    p.used.lock().clear();
                }
                let forward = translate::translate("", select, &Scope { s: self, params })?;
                let sql = forward
                    .statements
                    .last()
                    .cloned()
                    .ok_or_else(|| TsqlError::syntax("a cursor needs a query"))?;
                let bound = bound_values(params);
                let (fields, rows) = self
                    .fetch_fields(&sql, &bound)
                    .await
                    .map_err(|e| self.engine_error(&e))?;
                if let Some(c) = self.cursors.get_mut(&name) {
                    c.open = Some(CursorRows {
                        fields,
                        rows,
                        pos: -1,
                    });
                }
            }
            "CLOSE" => {
                let cursor = self.cursors.get_mut(&name).ok_or_else(missing)?;
                cursor.open.take().ok_or_else(not_open)?;
            }
            "DEALLOCATE" => {
                self.cursors.remove(&name).ok_or_else(missing)?;
                for scope in &mut self.scopes {
                    scope.local_cursors.retain(|c| *c != name);
                }
            }
            _ => {
                let mut into = Vec::new();
                if kw(tokens, k, "INTO") {
                    for part in split_commas(&tokens[k + 1..]) {
                        match part.as_slice() {
                            [t] => match &t.tok {
                                Tok::UserVar(v) => into.push(v.clone()),
                                _ => return Err(TsqlError::syntax("FETCH … INTO takes variables")),
                            },
                            _ => return Err(TsqlError::syntax("FETCH … INTO takes variables")),
                        }
                    }
                } else if k < tokens.len() {
                    return Err(TsqlError::syntax(format!(
                        "Incorrect syntax near '{}'.",
                        crate::sqlcompat::render_text(&tokens[k..k + 1])
                    )));
                }
                let cursor = self.cursors.get_mut(&name).ok_or_else(missing)?;
                if how != "NEXT" && !cursor.scroll {
                    return Err(TsqlError::new(
                        16911,
                        format!(
                            "fetch: The fetch type {how} cannot be used with forward only cursors."
                        ),
                    ));
                }
                let open = cursor.open.as_mut().ok_or_else(not_open)?;
                let len = open.rows.len() as i64;
                open.pos = routines::fetch_target(open.pos, len, &how, n);
                let row =
                    (open.pos >= 0 && open.pos < len).then(|| open.rows[open.pos as usize].clone());
                let fields = open.fields.clone();
                self.fetch_status = if row.is_some() { 0 } else { -1 };
                if into.is_empty() {
                    self.send_rows(&fields, row.into_iter().collect());
                    return Ok(Flow::Next);
                }
                if into.len() != fields.len() {
                    return Err(TsqlError::new(
                        16924,
                        "Cursorfetch: The number of variables declared in the INTO list must match that of selected columns.",
                    ));
                }
                if let Some(row) = row {
                    for (j, var) in into.iter().enumerate() {
                        let v = value_of(fields[j].type_oid, row[j].clone());
                        self.assign(var, v)?;
                    }
                }
            }
        }
        self.statement_done(0, 0, 0);
        Ok(Flow::Next)
    }

    /// Rows held here (a cursor's), sent as a result.
    fn send_rows(&mut self, fields: &[PgField], rows: Vec<Vec<Option<String>>>) {
        let cols: Vec<Column> = fields
            .iter()
            .enumerate()
            .map(|(k, f)| Column {
                name: f.name.clone(),
                ty: tds::column_type(f.type_oid, f.type_modifier).unwrap_or_else(|| {
                    let (mut int, mut frac) = (1usize, 0usize);
                    for r in &rows {
                        if let Some(Some(v)) = r.get(k) {
                            let (i, f) = tds::decimal_shape(v);
                            int = int.max(i);
                            frac = frac.max(f);
                        }
                    }
                    let frac = frac.min(38 - int.min(38));
                    ColType::Decimal {
                        precision: (int + frac).clamp(1, 38) as u8,
                        scale: frac as u8,
                    }
                }),
                oid: f.type_oid,
                identity: false,
            })
            .collect();
        tds::col_metadata(self.wire.reply(), &cols);
        for r in &rows {
            let out = self.wire.reply();
            let at = out.len();
            out.push(0xd1);
            for (k, cell) in r.iter().enumerate() {
                let ty = cols
                    .get(k)
                    .map(|c| c.ty.clone())
                    .unwrap_or(ColType::NVarChar(None));
                if tds::put_value(out, &ty, cell.as_deref().map(str::as_bytes)).is_err() {
                    out.truncate(at);
                    break;
                }
            }
        }
        self.rowcount = rows.len() as i64;
        self.statement_done(
            if self.nocount { 0 } else { tds::DONE_COUNT },
            0xc1,
            rows.len() as u64,
        );
    }

    // -- procedures ---------------------------------------------------------

    fn has_routine_table(&self) -> bool {
        self.bridge
            .as_ref()
            .is_some_and(|b| b.database.has_table(routines::ROUTINE_TABLE))
    }

    /// Reads the procedures when a batch looks at the catalogue (OBJECT_ID,
    /// sys.procedures, …), for the translation to see them.
    async fn refresh_routines_for(&mut self, text: &str) {
        let lower = text.to_ascii_lowercase();
        let looks = [
            "object_id",
            "object_name",
            "procedures",
            "routines",
            "objects",
            "sql_modules",
            "object_definition",
        ]
        .iter()
        .any(|w| lower.contains(w));
        if !looks {
            return;
        }
        let mut found = Vec::new();
        if self.has_routine_table() {
            if let Ok((_, rows)) = self
                .system_fetch_guarded(
                    "SELECT name, definition FROM __rdb_routines WHERE dialect = 'tsql' ORDER BY name",
                )
                .await
            {
                found = rows
                    .into_iter()
                    .filter_map(|r| {
                        let mut r = r.into_iter();
                        Some((r.next()??, r.next()??))
                    })
                    .collect();
            }
        }
        self.routine_cache = Some(found);
    }

    async fn load_procedure(&mut self, name: &str) -> Result<Option<String>, TsqlError> {
        if !self.has_routine_table() {
            return Ok(None);
        }
        let (_, rows) = self
            .system_fetch_guarded(&format!(
                "SELECT definition FROM __rdb_routines WHERE name = {} AND dialect = 'tsql'",
                crate::sqlcompat::string_sql(name.as_bytes())
            ))
            .await
            .map_err(|e| self.engine_error(&e))?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten()))
    }

    /// CREATE / ALTER PROCEDURE, which is its batch.
    async fn procedure_ddl(
        &mut self,
        text: &str,
        tokens: &[Token],
    ) -> Option<Result<(), TsqlError>> {
        let def = routines::parse_create(tokens)?;
        Some(match def {
            Ok(d) => self.define_procedure(text, tokens, d).await,
            Err(e) => Err(e),
        })
    }

    async fn define_procedure(
        &mut self,
        text: &str,
        tokens: &[Token],
        def: ProcDef,
    ) -> Result<(), TsqlError> {
        // A body that does not parse is refused now, as SQL Server does.
        translate::parse_batch(&tokens[def.body_start..])?;
        // Kept as CREATE PROCEDURE …, however it was made.
        let proc_at = match def.mode {
            routines::Mode::Create => 1,
            routines::Mode::CreateOrAlter => 3,
            routines::Mode::Alter => 1,
        };
        let definition = format!("CREATE {}", text[tokens[proc_at].span.0..].trim_end());
        let taken = || {
            TsqlError::new(
                2714,
                format!(
                    "There is already an object named '{}' in the database.",
                    def.name
                ),
            )
        };
        if self.bridge().database.has_table(&def.name) {
            return Err(taken());
        }
        if !self.has_routine_table() {
            self.system_guarded(routines::CREATE_ROUTINE_TABLE)
                .await
                .map_err(|e| self.engine_error(&e))?;
        }
        let (_, rows) = self
            .system_fetch_guarded(&format!(
                "SELECT dialect FROM __rdb_routines WHERE name = {}",
                crate::sqlcompat::string_sql(def.name.as_bytes())
            ))
            .await
            .map_err(|e| self.engine_error(&e))?;
        let existing = rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten());
        match (def.mode, existing.as_deref()) {
            (_, Some(d)) if d != "tsql" => return Err(taken()),
            (routines::Mode::Create, Some(_)) => return Err(taken()),
            (routines::Mode::Alter, None) => {
                return Err(TsqlError::new(
                    208,
                    format!("Invalid object name '{}'.", def.name),
                ));
            }
            _ => {}
        }
        let lit = |v: &str| crate::sqlcompat::string_sql(v.as_bytes());
        let sql = if existing.is_some() {
            format!("UPDATE __rdb_routines SET definition = {} WHERE name = {}", lit(&definition), lit(&def.name))
        } else {
            format!(
                "INSERT INTO __rdb_routines (name, dialect, definition) VALUES ({}, 'tsql', {})",
                lit(&def.name),
                lit(&definition)
            )
        };
        self.system_fetch_guarded(&sql)
            .await
            .map_err(|e| self.engine_error(&e))?;
        self.routine_cache = None;
        Ok(())
    }

    async fn drop_procedure(&mut self, tokens: &[Token]) -> Result<Flow, TsqlError> {
        let if_exists = kw(tokens, 2, "IF") && kw(tokens, 3, "EXISTS");
        let mut k = if if_exists { 4 } else { 2 };
        let mut names = Vec::new();
        loop {
            let (name, j) = routines::object_name_at(tokens, k)
                .ok_or_else(|| TsqlError::syntax("DROP PROCEDURE needs a name"))?;
            names.push(name);
            k = j;
            if tokens.get(k).is_some_and(|t| t.is_op(",")) {
                k += 1;
                continue;
            }
            break;
        }
        for name in names {
            let removed = if self.has_routine_table() {
                let tag = self
                    .system_guarded(&format!(
                        "DELETE FROM __rdb_routines WHERE name = {} AND dialect = 'tsql'",
                        crate::sqlcompat::string_sql(name.as_bytes())
                    ))
                    .await
                    .map_err(|e| self.engine_error(&e))?;
                tag_count(&tag).unwrap_or(0)
            } else {
                0
            };
            if removed == 0 && !if_exists {
                return Err(TsqlError::new(
                    3701,
                    format!(
                        "Cannot drop the procedure '{name}', because it does not exist or you do not have permission."
                    ),
                ));
            }
        }
        self.routine_cache = None;
        self.statement_done(0, 0, 0);
        Ok(Flow::Next)
    }

    /// Runs a stored procedure: its parameters are its variables, in a
    /// scope of their own.
    async fn call_procedure(
        &mut self,
        name: &str,
        args: Vec<ProcArg>,
    ) -> Result<ProcOutcome, TsqlError> {
        let definition = self.load_procedure(name).await?.ok_or_else(|| {
            TsqlError::new(2812, format!("Could not find stored procedure '{name}'."))
        })?;
        let tokens = lex::lex(&definition).map_err(|e| TsqlError::syntax(e.0))?;
        let def = routines::parse_create(&tokens)
            .ok_or_else(|| TsqlError::syntax("a stored definition is not a procedure"))??;
        if self.nest_level >= routines::MAX_NESTING {
            return Err(TsqlError::new(
                217,
                "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
            ));
        }
        // Arguments to parameters: positional first, then by name.
        let mut values: Vec<Option<Value>> = vec![None; def.params.len()];
        let mut supplied = vec![false; def.params.len()];
        let mut wants: Vec<Option<usize>> = vec![None; def.params.len()];
        let mut position = 0usize;
        for (a, arg) in args.iter().enumerate() {
            let k = match &arg.name {
                Some(n) => def
                    .params
                    .iter()
                    .position(|p| p.name == *n)
                    .ok_or_else(|| {
                        TsqlError::new(
                            8145,
                            format!("@{n} is not a parameter for procedure {name}."),
                        )
                    })?,
                None => {
                    if position >= def.params.len() {
                        return Err(TsqlError::new(
                            8144,
                            format!(
                                "Procedure or function {name} has too many arguments specified."
                            ),
                        ));
                    }
                    position += 1;
                    position - 1
                }
            };
            if arg.output {
                if !def.params[k].output {
                    return Err(TsqlError::new(
                        8162,
                        format!(
                            "The formal parameter \"@{}\" was not declared as an OUTPUT parameter, but the actual parameter passed in requested output.",
                            def.params[k].name
                        ),
                    ));
                }
                wants[k] = Some(a);
            }
            if let Some(v) = &arg.value {
                values[k] = Some(v.clone());
                supplied[k] = true;
            }
        }
        for (k, p) in def.params.iter().enumerate() {
            if supplied[k] {
                continue;
            }
            match &p.default {
                Some(d) => values[k] = Some(self.evaluate(d, None).await?),
                None => {
                    return Err(TsqlError::new(
                        201,
                        format!(
                            "Procedure or function '{name}' expects parameter '@{}', which was not supplied.",
                            p.name
                        ),
                    ));
                }
            }
        }
        let saved_vars = std::mem::take(&mut self.vars);
        let saved_return = self.return_value.take();
        let mut bound = Ok(());
        for (k, p) in def.params.iter().enumerate() {
            let (string, numeric) = routines::type_flags(&p.ty);
            self.vars.insert(
                p.name.clone(),
                Var {
                    value: Value::Null,
                    string,
                    numeric,
                },
            );
            if let Err(e) = self.assign(&p.name, values[k].take().unwrap_or(Value::Null)) {
                bound = Err(e);
                break;
            }
        }
        let run = match bound {
            Ok(()) => {
                self.nest_level += 1;
                self.scopes.push(LocalScope {
                    owns_temps: true,
                    ..Default::default()
                });
                let r = match translate::parse_batch(&tokens[def.body_start..]) {
                    Ok(stmts) => self.run_list(&definition, &stmts, None).await,
                    Err(e) => Ok(Err(e)),
                };
                self.end_scope().await;
                self.nest_level -= 1;
                r
            }
            Err(e) => Ok(Err(e)),
        };
        let outputs = def
            .params
            .iter()
            .enumerate()
            .filter_map(|(k, p)| {
                let a = wants[k]?;
                Some((
                    a,
                    self.vars
                        .get(&p.name)
                        .map(|v| v.value.clone())
                        .unwrap_or(Value::Null),
                ))
            })
            .collect();
        let status = self.return_value.take().unwrap_or(0);
        self.return_value = saved_return;
        self.vars = saved_vars;
        let flow = match run {
            Ok(Ok(flow)) => flow,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(TsqlError::new(50000, e.to_string())),
        };
        Ok(ProcOutcome {
            status,
            outputs,
            flow,
        })
    }
}

/// MERGE (see [`super::merge`]).
impl Session {
    /// T-SQL run as a statement of the session's, its rows kept here:
    /// (columns, rows, rows affected).
    async fn exec_tsql(
        &mut self,
        tokens: Vec<Token>,
        params: Option<&ParamCtx>,
    ) -> Result<(Vec<PgField>, Vec<Vec<Option<String>>>, u64), TsqlError> {
        let tokens = self.bind_names(&tokens).unwrap_or(tokens);
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let forward = translate::translate("", tokens, &Scope { s: self, params })?;
        let bound = bound_values(params);
        let mut last = (Vec::new(), Vec::new(), 0);
        for sql in forward.statements {
            log::debug!("MERGE step: {sql}");
            let (f, r, tag) = self
                .query_bound(&sql, &bound)
                .await
                .map_err(|e| self.engine_error(&e))?;
            last = (f, r, tag_count(&tag).unwrap_or(0));
        }
        Ok(last)
    }

    async fn merge(
        &mut self,
        tokens: &[Token],
        params: Option<&ParamCtx>,
    ) -> Result<Flow, TsqlError> {
        let m = super::merge::parse(tokens)?;
        let database = self.bridge().database.clone();
        if !database.has_table(&m.target_name) {
            return Err(TsqlError::new(
                208,
                format!("Invalid object name '{}'.", m.target_name),
            ));
        }
        let pk = database
            .schema_view(&m.target_name)
            .and_then(|v| v.schema().primary_key.clone());
        let outer = self.bridge().in_block();
        self.silent(if outer {
            "SAVEPOINT rdb_tds_merge"
        } else {
            "BEGIN"
        })
        .await
        .map_err(|e| self.engine_error(&e))?;
        let r = self.merge_steps(&m, pk.as_deref(), params).await;
        let end = match (&r, outer) {
            (Ok(_), true) => "RELEASE SAVEPOINT rdb_tds_merge",
            (Ok(_), false) => "COMMIT",
            (Err(_), true) => "ROLLBACK TO SAVEPOINT rdb_tds_merge",
            (Err(_), false) => "ROLLBACK",
        };
        let ended = self.silent(end).await;
        if outer && r.is_err() {
            let _ = self.silent("RELEASE SAVEPOINT rdb_tds_merge").await;
        }
        let n = r?;
        ended.map_err(|e| self.engine_error(&e))?;
        self.rowcount = n as i64;
        self.statement_done(if self.nocount { 0 } else { tds::DONE_COUNT }, 0xc1, n);
        Ok(Flow::Next)
    }

    async fn merge_steps(
        &mut self,
        m: &super::merge::Merge,
        pk: Option<&str>,
        params: Option<&ParamCtx>,
    ) -> Result<u64, TsqlError> {
        use super::merge::t;
        let alias = Token::new(Tok::Quoted(m.source_alias.clone()));
        let mut copied = None;
        let mut source = Vec::new();
        if m.derived_source() {
            // The engine correlates with a table, not a derived one.
            self.temp_seq += 1;
            let tmp = format!("#rdb_merge{}", self.temp_seq);
            let mut copy = t("SELECT * INTO");
            copy.push(Token::new(Tok::Quoted(tmp.clone())));
            copy.extend(t("FROM"));
            copy.extend(m.source.iter().cloned());
            copy.extend(t("AS"));
            copy.push(alias.clone());
            if let Some(cols) = &m.source_columns {
                copy.push(Token::op("("));
                copy.extend(cols.iter().cloned());
                copy.push(Token::op(")"));
            }
            self.exec_tsql(copy, params).await?;
            copied = Some(self.temp_name(&tmp));
            source.push(Token::new(Tok::Quoted(tmp)));
        } else {
            source.extend(m.source.iter().cloned());
        }
        source.extend(t("AS"));
        source.push(alias);
        let r = self.merge_run(m, &source, pk, params).await;
        if let Some(table) = copied {
            let _ = self
                .silent(&format!("DROP TABLE IF EXISTS {}", ident_sql(&table)))
                .await;
        }
        r
    }

    async fn merge_run(
        &mut self,
        m: &super::merge::Merge,
        source: &[Token],
        pk: Option<&str>,
        params: Option<&ParamCtx>,
    ) -> Result<u64, TsqlError> {
        let steps = super::merge::steps(m, source, pk)?;
        if let Some(d) = steps.duplicates {
            let (_, rows, _) = self.exec_tsql(d, params).await?;
            if !rows.is_empty() {
                return Err(TsqlError::new(
                    8672,
                    "The MERGE statement attempted to UPDATE or DELETE the same row more than once. This happens when a target row matches more than one source row. A MERGE statement cannot UPDATE/DELETE the same row of the target table multiple times. Refine the ON clause to ensure a target row matches at most one source row, or use the GROUP BY clause to group the source rows.",
                ));
            }
        }
        let inserting = match steps.to_insert {
            Some((select, columns, default)) => {
                let (fields, rows, _) = self.exec_tsql(select, params).await?;
                Some((fields, rows, columns, default))
            }
            None => None,
        };
        let mut total = 0u64;
        for w in steps.writes {
            total += self.exec_tsql(w, params).await?.2;
        }
        let Some((fields, rows, columns, default)) = inserting else {
            return Ok(total);
        };
        if rows.is_empty() {
            return Ok(total);
        }
        let table = ident_sql(&m.target_name);
        if default {
            for _ in &rows {
                let tag = self
                    .silent(&format!("INSERT INTO {table} DEFAULT VALUES"))
                    .await
                    .map_err(|e| self.engine_error(&e))?;
                total += tag_count(&tag).unwrap_or(0);
            }
            return Ok(total);
        }
        let columns = match columns {
            Some(c) => c,
            None => {
                // All the columns but an identity, in the table's order.
                let view = self
                    .bridge()
                    .database
                    .schema_view(&m.target_name)
                    .ok_or_else(|| {
                        TsqlError::new(208, format!("Invalid object name '{}'.", m.target_name))
                    })?;
                let schema = view.schema();
                schema
                    .fields
                    .iter()
                    .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                    .filter(|f| {
                        !schema.defaults.iter().any(|d| {
                            !d.is_deleted
                                && d.column == f.name
                                && d.expression
                                    .trim_start()
                                    .to_ascii_lowercase()
                                    .starts_with("nextval(")
                        })
                    })
                    .map(|f| f.name.clone())
                    .collect()
            }
        };
        if columns.len() != fields.len() {
            return Err(TsqlError::new(
                110,
                "There are fewer columns in the INSERT statement than values specified in the VALUES clause.",
            ));
        }
        let cols = columns
            .iter()
            .map(|c| ident_sql(c))
            .collect::<Vec<_>>()
            .join(", ");
        let per = (30_000 / columns.len().max(1)).max(1);
        for chunk in rows.chunks(per) {
            let mut values = Vec::with_capacity(chunk.len());
            let mut bound = Vec::with_capacity(chunk.len() * columns.len());
            for row in chunk {
                let mut slots = Vec::with_capacity(columns.len());
                for (k, cell) in row.iter().enumerate() {
                    bound.push((
                        0i16,
                        cell.as_ref().map(|c| c.as_bytes().to_vec()),
                        fields[k].type_oid,
                    ));
                    slots.push(format!("${}", bound.len()));
                }
                values.push(format!("({})", slots.join(", ")));
            }
            let sql = format!("INSERT INTO {table} ({cols}) VALUES {}", values.join(", "));
            let (_, _, tag) = self
                .query_bound(&sql, &bound)
                .await
                .map_err(|e| self.engine_error(&e))?;
            total += tag_count(&tag).unwrap_or(0);
        }
        Ok(total)
    }
}

/// Bulk loads (SqlBulkCopy, bcp): INSERT BULK names the table, then a bulk
/// load message carries the rows.
impl Session {
    fn insert_bulk(&mut self, tokens: &[Token]) -> Result<Flow, TsqlError> {
        let (name, _) = routines::object_name_at(tokens, 2)
            .ok_or_else(|| TsqlError::syntax("INSERT BULK needs a table"))?;
        let name = if name.starts_with('#') {
            self.temp_name(&name)
        } else {
            name
        };
        if !self.bridge().database.has_table(&name) {
            return Err(TsqlError::new(
                208,
                format!("Invalid object name '{name}'."),
            ));
        }
        self.bulk_target = Some(name);
        self.statement_done(0, 0, 0);
        Ok(Flow::Next)
    }

    async fn bulk_load(&mut self, message: &Message) -> Result<u64, TsqlError> {
        self.verb = "INSERT";
        let table = self.bulk_target.take().ok_or_else(|| {
            TsqlError::new(
                4022,
                "Bulk load data was expected but not sent. The batch will be terminated.",
            )
        })?;
        let (columns, rows) = tds::read_bulk(&message.payload).map_err(|e| {
            TsqlError::new(
                4815,
                format!("Received an invalid column length from the bcp client: {e}"),
            )
        })?;
        if rows.is_empty() {
            return Ok(0);
        }
        let per = (30_000 / columns.len().max(1)).max(1);
        // One transaction, as SQL Server's batch is (BatchSize 0).
        let outer = self.bridge().in_block();
        self.silent(if outer {
            "SAVEPOINT rdb_tds_bulk"
        } else {
            "BEGIN"
        })
        .await
        .map_err(|e| self.engine_error(&e))?;
        let mut total = 0u64;
        let mut failed = None;
        for chunk in rows.chunks(per) {
            let mut values = Vec::with_capacity(chunk.len());
            let mut bound = Vec::with_capacity(chunk.len() * columns.len());
            for row in chunk {
                let mut slots = Vec::with_capacity(row.len());
                for (value, oid) in row {
                    bound.push(match value {
                        ParamValue::Null => (0i16, None, 0u32),
                        ParamValue::Text(t) => (0, Some(t.clone().into_bytes()), untyped_time(*oid)),
                        ParamValue::Bytes(b) => (1, Some(b.clone()), 17),
                    });
                    slots.push(format!("${}", bound.len()));
                }
                values.push(slots);
            }
            // A column keeping a 100 ns digit: its values split into the
            // column's and the digit's (`translate::with_ticks`).
            let names: Vec<String> = columns.iter().map(|c| ident_sql(&c.name.to_lowercase())).collect();
            let texts: Vec<Vec<translate::KnownText>> = chunk
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(v, _)| match v {
                            ParamValue::Null => Some(None),
                            ParamValue::Text(t) => Some(Some(t.clone())),
                            ParamValue::Bytes(_) => None,
                        })
                        .collect()
                })
                .collect();
            let (names, values) = translate::with_ticks(&Scope { s: self, params: None }, &table, &names, values, &texts);
            let values: Vec<String> = values.iter().map(|r| format!("({})", r.join(", "))).collect();
            let cols = names.join(", ");
            let sql = format!(
                "INSERT INTO {} ({cols}) VALUES {}",
                ident_sql(&table),
                values.join(", ")
            );
            match self.query_bound(&sql, &bound).await {
                Ok((_, _, tag)) => total += tag_count(&tag).unwrap_or(0),
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
        }
        let end = match (&failed, outer) {
            (None, true) => "RELEASE SAVEPOINT rdb_tds_bulk",
            (None, false) => "COMMIT",
            (Some(_), true) => "ROLLBACK TO SAVEPOINT rdb_tds_bulk",
            (Some(_), false) => "ROLLBACK",
        };
        let ended = self.silent(end).await;
        if outer && failed.is_some() {
            let _ = self.silent("RELEASE SAVEPOINT rdb_tds_bulk").await;
        }
        if let Some(e) = failed {
            return Err(self.engine_error(&e));
        }
        ended.map_err(|e| self.engine_error(&e))?;
        // SqlBulkCopy with KeepIdentity (and bcp -E) write the identity
        // column: its counter moves past them, as SQL Server's does.
        let identity = translate::identity_column(&self.bridge().database, &table);
        if let Some(column) = identity.filter(|c| columns.iter().any(|b| b.name.eq_ignore_ascii_case(c))) {
            self.advance_identity(&table, &column).await;
        }
        Ok(total)
    }

    /// The declared spellings, read again when stale (`NameCache`).
    /// A failed statement's error, with a constraint violation worded and
    /// named as SQL Server's: error 547 for a foreign key (`The INSERT
    /// statement conflicted with the FOREIGN KEY constraint …`), 2627 for a
    /// primary or unique key, 2601 for a unique index, every name as it
    /// was declared. Applications match on the number; tools show the
    /// text, where the engine's own named `denevcloud․erp․kids`.
    fn engine_error(&self, n: &PgNotice) -> TsqlError {
        let verb = self.verb;
        let mut e = error_from_engine(n);
        let Some(bridge) = self.bridge.as_ref() else { return e };
        let database = &bridge.database;
        let db = if self.database_shown.is_empty() { bridge.database_name.clone() } else { self.database_shown.clone() };
        let names = &self.names;
        let quoted = quoted_names(&n.message);
        let dbo = |t: &str| format!("dbo.{}", names.table_shown(t));
        let foreign_key = |table: &str, name: &str| {
            database.schema_view(table).and_then(|v| {
                v.schema().foreign_keys.iter().find(|f| !f.is_deleted && f.name == name).cloned()
            })
        };
        let message = match n.code.as_str() {
            "23503" if n.message.starts_with("insert or update on table") && quoted.len() >= 3 => {
                let (child, fk, parent) = (quoted[0], quoted[1], quoted[quoted.len() - 1]);
                let column = foreign_key(child, fk)
                    .and_then(|f| f.parent_columns.first().cloned())
                    .map(|c| names.column_of(parent, &c));
                Some(format!(
                    "The {verb} statement conflicted with the FOREIGN KEY constraint \"{}\". The conflict occurred in database \"{db}\", table \"{}\"{}.",
                    names.object_shown(fk),
                    dbo(parent),
                    column.map(|c| format!(", column '{c}'")).unwrap_or_default()
                ))
            }
            "23503" if n.message.starts_with("update or delete on table") && quoted.len() >= 3 => {
                let (fk, child) = (quoted[1], quoted[2]);
                let column = foreign_key(child, fk)
                    .and_then(|f| f.columns.first().cloned())
                    .map(|c| names.column_of(child, &c));
                Some(format!(
                    "The {verb} statement conflicted with the REFERENCE constraint \"{}\". The conflict occurred in database \"{db}\", table \"{}\"{}.",
                    names.object_shown(fk),
                    dbo(child),
                    column.map(|c| format!(", column '{c}'")).unwrap_or_default()
                ))
            }
            "23505" if n.message.starts_with("duplicate key value violates unique constraint") && !quoted.is_empty() => {
                let key = quoted[0];
                // The table holding the key, and whether it is the primary
                // key, a constraint or an index.
                let owner = database.table_names().into_iter().find_map(|t| {
                    let view = database.schema_view(&t)?;
                    let schema = view.schema();
                    if crate::core::rql::unique_key::primary_key_of_named(schema, &t).is_some_and(|k| k.name == key) {
                        return Some((t, Some(true)));
                    }
                    let k = schema.unique_keys.iter().find(|k| !k.is_deleted && k.name == key)?;
                    Some((t, if k.from_index { None } else { Some(false) }))
                });
                owner.map(|(table, kind)| {
                    let values = duplicate_values(&n.message)
                        .map(|v| format!(" The duplicate key value is ({v})."))
                        .unwrap_or_default();
                    match kind {
                        None => {
                            e.number = self.shared.config.unique_index_error;
                            format!(
                                "Cannot insert duplicate key row in object '{}' with unique index '{}'.{values}",
                                dbo(&table),
                                names.object_shown(key)
                            )
                        }
                        Some(primary) => format!(
                            "Violation of {} constraint '{}'. Cannot insert duplicate key in object '{}'.{values}",
                            if primary { "PRIMARY KEY" } else { "UNIQUE KEY" },
                            names.object_shown(key),
                            dbo(&table)
                        ),
                    }
                })
            }
            _ => None,
        };
        if let Some(m) = message {
            e.message = format!("{m} (SQLSTATE {})", n.code);
        }
        e
    }

    async fn ensure_names(&mut self) {
        let Some(bridge) = self.bridge.as_ref() else { return };
        let database = Arc::as_ptr(&bridge.database) as usize;
        let has = bridge.database.has_table(translate::NAMES_TABLE);
        let fresh = self.names.database == database
            && !self.names.dirty
            && self.names.read.is_some_and(|r| r.elapsed() < NAMES_TTL);
        if fresh {
            return;
        }
        let mut cache = NameCache { database, read: Some(std::time::Instant::now()), ..Default::default() };
        if has {
            // Not inside the client's transaction: a read of committed names.
            if let Ok((_, rows, _)) = self.query_bound_as(&format!("SELECT rel, col, shown FROM {}", translate::NAMES_TABLE), &[], true).await {
                for row in rows {
                    let [Some(rel), Some(col), Some(shown)] = [row.first().cloned().flatten(), row.get(1).cloned().flatten(), row.get(2).cloned().flatten()]
                    else {
                        continue;
                    };
                    if let Some(object) = col.strip_prefix(translate::OBJECT_MARK) {
                        cache.objects.insert(object.to_string(), shown);
                        continue;
                    }
                    if !col.is_empty() {
                        cache
                            .columns
                            .entry(col.clone())
                            .and_modify(|e| {
                                if e.as_deref() != Some(shown.as_str()) {
                                    *e = None;
                                }
                            })
                            .or_insert_with(|| Some(shown.clone()));
                    }
                    cache.relations.entry(rel).or_default().insert(col, shown);
                }
            }
        }
        self.names = cache;
        let current = self.bridge.as_ref().map(|b| b.database_name.clone()).unwrap_or_default();
        self.database_shown = self.database_names().await.get(&current).cloned().unwrap_or_default();
    }

    /// Moves `table`'s identity counter past the largest value its
    /// `column` holds, never back: SQL Server's identity after rows were
    /// written with their own values. Left as it was the next generated id
    /// was 1, and a copied table failed its inserts on duplicate keys as
    /// soon as the counter reached the first copied row.
    async fn advance_identity(&mut self, table: &str, column: &str) {
        let lit = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let Ok((_, rows)) = self
            .fetch(&format!("SELECT pg_get_serial_sequence({}, {})", lit(table), lit(column)), &[])
            .await
        else {
            return;
        };
        let Some(Some(sequence)) = rows.first().and_then(|r| r.first()).cloned() else { return };
        let Ok((_, rows)) = self
            .fetch(&format!("SELECT max({}) FROM {}", ident_sql(column), ident_sql(table)), &[])
            .await
        else {
            return;
        };
        let Some(max) = rows.first().and_then(|r| r.first()).cloned().flatten().and_then(|m| m.parse::<i128>().ok()) else {
            return;
        };
        let Ok((_, rows)) = self.fetch(&format!("SELECT nextval({})", lit(&sequence)), &[]).await else { return };
        let Some(next) = rows.first().and_then(|r| r.first()).cloned().flatten().and_then(|m| m.parse::<i128>().ok()) else {
            return;
        };
        // `nextval` took one value; set the counter to the larger of it and
        // the column's maximum, so the next call answers past both.
        let value = max.max(next);
        if let Err(e) = self.silent(&format!("SELECT setval({}, {value})", lit(&sequence))).await {
            log::warn!("SQL Server session: could not move the identity of {table}.{column} past {max}: {}", e.message);
        }
    }

    /// sp_tablecollations_100: a row per column, the collation of its text
    /// (none: the engine compares bytes).
    async fn table_collations(&mut self, table: &str) -> Result<Flow, TsqlError> {
        self.ensure_names().await;
        let view = self
            .bridge()
            .database
            .schema_view(table)
            .ok_or_else(|| TsqlError::new(208, format!("Invalid object name '{table}'.")))?;
        let schema = view.schema();
        // Each column as declared, and for text its collation: the 5-byte
        // TDS collation SqlBulkCopy encodes a varchar/char/text value with,
        // and its name. Case-insensitive: SQL_Latin1_General_CP1_CI_AS
        // (LCID 1033, sort id 52); else by the bytes, Latin1_General_BIN2.
        let names: Vec<(String, Option<(String, String)>)> = schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .map(|f| {
                let shown = self.names.shown(table, &f.name).unwrap_or_else(|| f.name.clone());
                let text = matches!(f.db_type, crate::core::db_type::DbType::STRING | crate::core::db_type::DbType::BPCHAR(_))
                    && !f.json_text;
                let collation = text.then(|| {
                    if crate::core::rql::collation::of(&f.name, &schema.fields).is_binary() {
                        ("\\x0904000200".to_string(), "Latin1_General_BIN2".to_string())
                    } else {
                        ("\\x0904d00034".to_string(), "SQL_Latin1_General_CP1_CI_AS".to_string())
                    }
                });
                (shown, collation)
            })
            .collect();
        let field = |name: &str, oid: u32| PgField {
            name: name.to_string(),
            table_oid: 0,
            column: 0,
            type_oid: oid,
            type_size: -1,
            type_modifier: -1,
        };
        let fields = vec![
            field("colid", 23),
            field("name", 25),
            field("tds_collation_100", 17),
            field("collation_100", 25),
        ];
        let rows = names
            .into_iter()
            .enumerate()
            .map(|(k, (n, collation))| {
                let (bytes, name) = match collation {
                    Some((b, c)) => (Some(b), Some(c)),
                    None => (None, None),
                };
                vec![Some((k + 1).to_string()), Some(n), bytes, name]
            })
            .collect();
        self.send_rows(&fields, rows);
        Ok(Flow::Next)
    }
}

/// The declared types of a table's columns, which the engine's result
/// descriptions do not carry (a `decimal(18,2)` is described as `numeric`).
impl Session {
    /// `SELECT * | col, … FROM table` under SET FMTONLY ON (SqlBulkCopy's
    /// probe): the columns as declared.
    fn declared_columns(&self, tokens: &[Token]) -> Option<Vec<Column>> {
        let from = find_top(tokens, 1, &["FROM"])?;
        let (table, end) = routines::object_name_at(tokens, from + 1)?;
        if end != tokens.len() && !tokens[end..].iter().all(|t| t.is_op(";")) {
            return None;
        }
        let table = if table.starts_with('#') {
            self.temp_name(&table)
        } else {
            table
        };
        let view = self.bridge.as_ref()?.database.schema_view(&table)?;
        let identity = translate::identity_column(&self.bridge.as_ref()?.database, &table);
        let schema = view.schema();
        let fields: Vec<_> = schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .collect();
        let items = split_commas(&tokens[1..from]);
        let chosen: Vec<_> = if items.len() == 1 && items[0].len() == 1 && items[0][0].is_op("*") {
            fields
        } else {
            let mut chosen = Vec::new();
            for item in &items {
                let [t] = item.as_slice() else { return None };
                let name = t.ident()?;
                chosen.push(*fields.iter().find(|f| f.name.eq_ignore_ascii_case(name))?);
            }
            chosen
        };
        Some(
            chosen
                .into_iter()
                .map(|f| {
                    use crate::core::db_type::DbType as D;
                    use crate::core::rql::type_modifier::{TextTypmod, precision_of};
                    let oid = f.db_type.pg_oid();
                    let ty = match (&f.db_type, f.text_typmod()) {
                        (D::DECIMAL, _) => {
                            let scale = f
                                .scale
                                .unwrap_or(crate::core::value_codec::DECIMAL_SCALE)
                                .min(38);
                            let precision = precision_of(&f.name, &schema.type_modifiers)
                                .unwrap_or(38)
                                .clamp(scale.max(1), 38);
                            ColType::Decimal {
                                precision: precision as u8,
                                scale: scale as u8,
                            }
                        }
                        (D::STRING, Some(TextTypmod::Varchar(n) | TextTypmod::Bpchar(n)))
                            if n > 0 && n <= 4000 =>
                        {
                            ColType::NVarChar(Some(n as u16))
                        }
                        _ => match tds::column_type(oid, f.db_type.pg_typmod()).unwrap_or(ColType::NVarChar(None)) {
                            ColType::DateTimeOffset if f.without_time_zone => ColType::DateTime2,
                            other => other,
                        },
                    };
                    let oid = if oid == 1184 && f.without_time_zone {
                        1114
                    } else {
                        oid
                    };
                    Column {
                        name: self.names.shown(&table, &f.name).unwrap_or_else(|| f.name.clone()),
                        ty,
                        oid,
                        identity: identity.as_deref().is_some_and(|c| c.eq_ignore_ascii_case(&f.name)),
                    }
                })
                .collect(),
        )
    }
}

/// The values of a call's parameters, in the order of the declarations
/// (by name, or by position when unnamed).
fn bind_values(decls: &[Decl], params: &[tds::Param]) -> Vec<ParamValue> {
    let mut out = vec![ParamValue::Null; decls.len()];
    let mut position = 0;
    for p in params {
        let name = p.name.trim_start_matches('@').to_lowercase();
        let k = if name.is_empty() {
            position += 1;
            Some(position - 1)
        } else {
            decls.iter().position(|d| d.0 == name)
        };
        if let Some(k) = k {
            if k < out.len() {
                out[k] = p.value.clone();
            }
        }
    }
    out
}

/// The bind list for a statement translated with `params`.
fn bound_values(params: Option<&ParamCtx>) -> Vec<(i16, Option<Vec<u8>>, u32)> {
    let Some(p) = params else { return Vec::new() };
    p.used
        .lock()
        .iter()
        .map(
            |&k| match p.values.get(k).cloned().unwrap_or(ParamValue::Null) {
                ParamValue::Null => (0, None, 0),
                ParamValue::Text(t) => (0, Some(t.into_bytes()), 0),
                ParamValue::Bytes(b) => (1, Some(b), 17),
            },
        )
        .collect()
}

fn render_value(tokens: &[Token]) -> Option<String> {
    match tokens {
        [t] => match &t.tok {
            Tok::Num(n) => Some(n.clone()),
            Tok::Str(s) => Some(String::from_utf8_lossy(s).into_owned()),
            Tok::Word(w) => Some(w.clone()),
            _ => None,
        },
        [m, t] if m.is_op("-") => match &t.tok {
            Tok::Num(n) => Some(format!("-{n}")),
            _ => None,
        },
        _ => None,
    }
}

#[allow(dead_code)]
fn unused(_: &Arc<Database>, _: Ordering, _: Option<usize>) -> Option<usize> {
    find_top(&[], 0, &[])
}

/// Whether `sql` (engine SQL) only reads: re-running it changes nothing.
fn is_plain_select(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    if !(upper.starts_with("SELECT") || upper.starts_with('(') || upper.starts_with("WITH")) {
        return false;
    }
    // At any depth, outside quotes: a data-modifying CTE or subquery is a
    // write (`find_keyword` reads the top level only).
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if crate::core::sql_scan::is_identifier_byte(bytes[i]) {
            let start = i;
            while i < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[i]) {
                i += 1;
            }
            let word = &upper[start..i];
            if matches!(word, "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "INTO" | "SHARE" | "TRUNCATE" | "COPY") {
                return false;
            }
            continue;
        }
        i += 1;
    }
    true
}

/// `sql` with each `$n` (outside quotes) replaced by its bound value as a
/// typed literal; `None` when a value cannot be written so.
fn inline_params(sql: &str, params: &[(i16, Option<Vec<u8>>, u32)], numbers: &[bool]) -> Option<String> {
    let literal = |n: usize| -> Option<String> {
        let (format, value, oid) = params.get(n)?;
        let declared_number = numbers.get(n).copied().unwrap_or(false);
        let Some(v) = value else { return Some("NULL".into()) };
        if *format == 1 {
            let hex: String = v.iter().map(|b| format!("{b:02x}")).collect();
            return Some(format!("'\\x{hex}'::bytea"));
        }
        let raw = std::str::from_utf8(v).ok()?;
        // A NUL has no place in statement text: not written in.
        if raw.contains('\0') {
            return None;
        }
        // Numbers bare, as LIMIT and OFFSET take only a number.
        let oid = *oid;
        // A parameter declared a number (no engine type is sent for one,
        // which infers it): bare.
        let number = matches!(oid, 20 | 21 | 23 | 700 | 701 | 1700) || (oid == 0 && declared_number);
        if number && is_plain_number(raw) {
            return Some(raw.to_string());
        }
        if oid == 16 && matches!(raw, "t" | "f" | "true" | "false") {
            return Some(if raw.starts_with('t') { "TRUE".into() } else { "FALSE".into() });
        }
        let text = raw.replace('\'', "''");
        let ty = match oid {
            16 => "boolean",
            20 => "bigint",
            21 => "smallint",
            23 => "integer",
            25 | 1043 => "text",
            700 => "real",
            701 => "double precision",
            1700 => "numeric",
            1082 => "date",
            1083 => "time",
            1114 => "timestamp",
            1184 => "timestamptz",
            2950 => "uuid",
            17 => "bytea",
            _ => return Some(format!("'{text}'")),
        };
        Some(format!("CAST('{text}' AS {ty})"))
    };
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let mut i = 0;
    let mut copied = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'$' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)
            && (i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]))
        {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let n: usize = sql[i + 1..j].parse().ok()?;
            let value = literal(n.checked_sub(1)?)?;
            out.push_str(&sql[copied..i]);
            out.push_str(&value);
            copied = j;
            i = j;
            continue;
        }
        i += 1;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// A number as SQL writes one: `-12`, `3.50`, `1e-3`, nothing else.
fn is_plain_number(text: &str) -> bool {
    let body = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    (!int.is_empty() || !frac.is_empty())
        && digits(int)
        && digits(frac)
        && exponent.is_none_or(|e| {
            let e = e.strip_prefix(['-', '+']).unwrap_or(e);
            !e.is_empty() && digits(e)
        })
}

/// Whether `sql` calls a function whose answer differs between runs.
fn calls_volatile(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    ["nextval", "setval", "random", "gen_random_uuid", "uuidv4", "uuidv7", "uuid_generate", "clock_timestamp", "txid_current"]
        .iter()
        .any(|f| lower.contains(f))
}

#[cfg(test)]
mod inline_tests {
    use super::*;

    fn text(v: &str) -> (i16, Option<Vec<u8>>, u32) {
        (0, Some(v.as_bytes().to_vec()), 0)
    }

    /// A value is written as a quoted literal whose text is exactly the
    /// value: a quote doubled, a backslash as itself (standard strings),
    /// so no value can end the literal and add SQL of its own.
    #[test]
    fn values_are_written_as_literals_that_cannot_escape() {
        let sql = "SELECT a FROM t WHERE b = $1 AND c = $2 AND d = $3 AND e = $4 LIMIT $5";
        let params = [
            text("x'); DROP TABLE t; --"),
            text("Е'\\"),
            (0, None, 0),
            (1, Some(vec![0x00, 0xff, b'\'']), 17),
            text("10"),
        ];
        let got = inline_params(sql, &params, &[false, false, false, false, true]).unwrap();
        assert_eq!(
            got,
            "SELECT a FROM t WHERE b = 'x''); DROP TABLE t; --' AND c = 'Е''\\' AND d = NULL AND e = '\\x00ff27'::bytea LIMIT 10"
        );
        // A number is bare only when it is one and declared one.
        assert_eq!(inline_params("SELECT $1", &[text("10")], &[false]).unwrap(), "SELECT '10'");
        assert_eq!(inline_params("SELECT $1", &[text("1; DROP TABLE t")], &[true]).unwrap(), "SELECT '1; DROP TABLE t'");
        assert_eq!(inline_params("SELECT $1", &[text("-1.5e3")], &[true]).unwrap(), "SELECT -1.5e3");
        // A `$1` inside a string or a quoted name is text, not a parameter.
        assert_eq!(
            inline_params("SELECT '$1', \"$1\", $1", &[text("v")], &[false]).unwrap(),
            "SELECT '$1', \"$1\", 'v'"
        );
        // A NUL is not written in; a missing parameter is no statement.
        assert!(inline_params("SELECT $1", &[text("a\0b")], &[false]).is_none());
        assert!(inline_params("SELECT $2", &[text("a")], &[false]).is_none());
    }

    #[test]
    fn only_plain_reads_run_again() {
        assert!(is_plain_select("SELECT a FROM t"));
        assert!(!is_plain_select("SELECT a FROM t FOR UPDATE"));
        assert!(!is_plain_select("INSERT INTO t SELECT 1"));
        assert!(!is_plain_select("WITH x AS (DELETE FROM t RETURNING a) SELECT a FROM x"));
        assert!(calls_volatile("SELECT nextval('s'), a FROM t"));
        assert!(calls_volatile("SELECT gen_random_uuid()"));
        assert!(!calls_volatile("SELECT a FROM t"));
        for n in ["0", "-12", "3.50", ".5", "1e-3", "+7"] {
            assert!(is_plain_number(n), "{n}");
        }
        for n in ["", "-", "1e", "0x10", "1 OR 1=1", "NaN", "1.2.3"] {
            assert!(!is_plain_number(n), "{n}");
        }
    }
}
