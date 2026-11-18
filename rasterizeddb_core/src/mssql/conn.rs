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
use crate::pgwire::proxied::{BridgeOptions, OpenRefused, PgBridge, PgNotice, ProxiedLogin, data_row_cells, parse_row_description, tag_count};
use crate::sqlcompat::{Tok, Token, find_top, ident_sql, split_commas};

use super::lex;
use super::tds::{self, Column, ColType, Message, ParamValue, Reader, TdsStream, Wire};
use super::translate::{self, Env, Forward, Kind, ParamSlot, Stmt, TsqlError, Value};
use super::{MsAuth, MsTls, Shared};

const SERVER_NAME: &str = "rasterizeddb";
const STATEMENT_SAVEPOINT: &str = "rdb_tds_statement";

const VERSION_TEXT: &str = "Microsoft SQL Server 2022 (RTM) - 16.0.4000.0 (X64) \n\tRasterizedDB (SQL Server protocol)";

/// SQL Server's system databases are the default database here: a client
/// connects to `master` to create or drop its own.
fn resolve_database<'a>(shared: &'a Shared, name: &'a str) -> &'a str {
    if name.is_empty() || matches!(name.to_ascii_lowercase().as_str(), "master" | "model" | "msdb" | "tempdb") {
        shared.catalog.default_name()
    } else {
        name
    }
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
struct ParamCtx {
    /// (name without @, string type, engine type)
    decls: Vec<(String, bool, u32)>,
    values: Vec<ParamValue>,
    /// The declarations the statement being translated used, in `$n` order.
    used: parking_lot::Mutex<Vec<usize>>,
}

/// A prepared handle's statement.
#[derive(Clone)]
struct Prepared {
    sql: String,
    decls: Vec<(String, bool, u32)>,
}

enum Outcome {
    Rows { count: u64 },
    Done { tag: String, identity: Option<i64> },
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
}

pub(crate) async fn serve_connection(base: tds::Base, peer: String, shared: StdArc<Shared>, over_limit: bool) -> io::Result<()> {
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
    let shown = if login_packet.database.is_empty() { database.clone() } else { login_packet.database.clone() };
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
    };
    let packet_size = match login_packet.packet_size {
        0 => tds::DEFAULT_PACKET,
        n => (n as usize).clamp(512, tds::MAX_PACKET),
    };
    match session.open(&database).await {
        Ok(()) => {
            let out = session.wire.reply();
            tds::env_change_text(out, 1, &shown, "master");
            tds::message(out, false, 5701, 2, 0, &format!("Changed database context to '{shown}'."), SERVER_NAME, 1);
            tds::env_change_bytes(out, 7, &tds::COLLATION, &[]);
            tds::env_change_text(out, 2, "us_english", "");
            tds::login_ack(out);
            tds::env_change_text(out, 4, &packet_size.to_string(), &tds::DEFAULT_PACKET.to_string());
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
    if let Some(bridge) = session.bridge.take() {
        bridge.terminate().await;
    }
    let _ = session.wire.stream.shutdown().await;
    let _ = peer;
    result
}

/// PRELOGIN, TLS and LOGIN7. `None` when the client was refused (and told).
async fn handshake(wire: &mut Wire, shared: &StdArc<Shared>, over_limit: bool) -> io::Result<Option<(tds::Login, ProxiedLogin)>> {
    let tls_available = shared.acceptor.is_some();
    let first = wire.peek_byte().await?;
    if first == 0x16 {
        // TDS 8.0: TLS first, then PRELOGIN inside it.
        let Some(acceptor) = shared.acceptor.clone() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "TDS 8.0 client, but TLS is not configured"));
        };
        upgrade(wire, acceptor, true).await?;
        let prelogin = wire.read().await?;
        if prelogin.kind != tds::PT_PRELOGIN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "PRELOGIN was expected"));
        }
        wire.reply().extend_from_slice(&tds::prelogin_reply(tds::ENCRYPT_ON));
        wire.send(tds::PT_REPLY).await?;
    } else {
        let prelogin = wire.read().await?;
        if prelogin.kind != tds::PT_PRELOGIN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "PRELOGIN was expected"));
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
        return Err(io::Error::new(io::ErrorKind::InvalidData, "LOGIN7 was expected"));
    }
    let login = tds::parse_login7(&message.payload)?;
    let refuse = |wire: &mut Wire, number: i32, text: &str| {
        let out = wire.reply();
        tds::message(out, true, number, 1, 14, text, SERVER_NAME, 1);
        tds::done(out, tds::TOKEN_DONE, tds::DONE_ERROR, 0, 0);
    };
    if over_limit {
        refuse(wire, 17809, "Could not connect because the maximum number of user connections has already been reached.");
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    if shared.config.tls == MsTls::Require && !wire.stream.is_tls() && !wire.stream.is_unix() {
        refuse(wire, 18456, "Login failed: this server requires an encrypted connection.");
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    if login.integrated {
        refuse(wire, 18452, "Login failed. Windows authentication is not supported; use SQL Server authentication.");
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    }
    let database_name = resolve_database(shared, &login.database).to_string();
    let Some(database) = shared.catalog.get(&database_name) else {
        refuse(wire, 4060, &format!("Cannot open database \"{database_name}\" requested by the login. The login failed."));
        wire.send(tds::PT_REPLY).await?;
        return Ok(None);
    };
    let method = if wire.stream.is_unix() {
        shared.config.socket.as_ref().and_then(|s| s.auth).unwrap_or(shared.config.auth)
    } else {
        shared.config.auth
    };
    let principal = match method {
        MsAuth::Trust => ProxiedLogin {
            user: login.user.clone(),
            superuser: false,
            authenticated: false,
        },
        MsAuth::Password => {
            let role = crate::auth::lookup_role(&database, &login.user).await;
            let verifier = role.as_ref().and_then(|r| r.verifier.clone()).unwrap_or(crate::auth::Verifier::Plain(String::new()));
            let may = role.as_ref().is_some_and(|r| r.can_login);
            if !may || !crate::auth::verify_password(&verifier, &login.user, &login.password) {
                refuse(wire, 18456, &format!("Login failed for user '{}'.", login.user));
                wire.send(tds::PT_REPLY).await?;
                return Ok(None);
            }
            ProxiedLogin {
                user: login.user.clone(),
                superuser: role.map(|r| r.is_superuser).unwrap_or(false),
                authenticated: true,
            }
        }
    };
    Ok(Some((login, principal)))
}

/// TLS over the connection: inside PRELOGIN packets for the handshake
/// (TDS 7.x), or directly (TDS 8.0).
async fn upgrade(wire: &mut Wire, acceptor: tokio_rustls::TlsAcceptor, strict: bool) -> io::Result<()> {
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
    fn database(&self) -> &Arc<Database> {
        &self.s.bridge.as_ref().unwrap().database
    }
    fn database_name(&self) -> &str {
        &self.s.bridge.as_ref().unwrap().database_name
    }
    fn user(&self) -> &str {
        &self.s.login.user
    }
    fn databases(&self) -> Vec<String> {
        self.s.shared.catalog.names()
    }
    fn variable(&self, name: &str) -> Option<Value> {
        self.s.vars.get(name).map(|v| v.value.clone())
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
        Some(ParamSlot { index, string: p.decls[k].1 })
    }
    fn param_is_null(&self, name: &str) -> Option<bool> {
        let p = self.params?;
        let k = p.decls.iter().position(|d| d.0 == name)?;
        Some(matches!(p.values.get(k), None | Some(ParamValue::Null)))
    }
    fn system(&self, name: &str) -> Option<Value> {
        let s = self.s;
        Some(match name {
            "version" => Value::Str(VERSION_TEXT.into()),
            "spid" => Value::Int(s.bridge.as_ref().map(|b| b.pid as i64 & 0x7fff).unwrap_or(0)),
            "rowcount" => Value::Int(s.rowcount),
            "identity" => s.last_identity.map(|v| Value::Num(v.to_string())).unwrap_or(Value::Null),
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
            "nestlevel" => Value::Int(0),
            "procid" => Value::Int(0),
            "fetch_status" => Value::Int(-1),
            "dbts" => Value::Bytes(vec![0; 8]),
            _ => return None,
        })
    }
    fn function(&self, name: &str) -> Option<Value> {
        let s = self.s;
        let e = s.last_error.as_ref();
        Some(match name {
            "SCOPE_IDENTITY" | "IDENT_CURRENT" => s.last_identity.map(|v| Value::Num(v.to_string())).unwrap_or(Value::Null),
            "ROWCOUNT_BIG" => Value::Int(s.rowcount),
            "HOST_NAME" => Value::Str(s.host.clone()),
            "APP_NAME" => Value::Str(s.app.clone()),
            "ERROR_MESSAGE" => e.map(|e| Value::Str(e.message.clone())).unwrap_or(Value::Null),
            "ERROR_NUMBER" => e.map(|e| Value::Int(e.number as i64)).unwrap_or(Value::Null),
            "ERROR_SEVERITY" => e.map(|e| Value::Int(e.class as i64)).unwrap_or(Value::Null),
            "ERROR_STATE" => e.map(|e| Value::Int(e.state as i64)).unwrap_or(Value::Null),
            "ERROR_LINE" => e.map(|_| Value::Int(1)).unwrap_or(Value::Null),
            "ERROR_PROCEDURE" => Value::Null,
            "XACT_STATE" => Value::Int((s.trancount > 0) as i64),
            "IS_SRVROLEMEMBER" | "IS_MEMBER" => Value::Int((!s.login.authenticated || s.login.superuser) as i64),
            "HAS_DBACCESS" => Value::Int(1),
            "SESSIONPROPERTY" => Value::Int(1),
            _ => return None,
        })
    }
}

/// A PostgreSQL error in SQL Server's numbering; the PostgreSQL code is kept.
fn error_from_engine(n: &PgNotice) -> TsqlError {
    // The engine reports some unknown columns as a syntax error.
    let code = if n.message.contains("not found in schema") { "42703" } else { n.code.as_str() };
    let (number, class): (i32, u8) = match code {
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
fn parse_declarations(text: &str) -> Result<Vec<(String, bool, u32)>, TsqlError> {
    let tokens = lex::lex(text).map_err(|e| TsqlError::syntax(e.0))?;
    let mut out = Vec::new();
    for part in split_commas(&tokens) {
        let Some(Tok::UserVar(name)) = part.first().map(|t| &t.tok) else {
            if part.is_empty() {
                continue;
            }
            return Err(TsqlError::syntax(format!("Incorrect syntax in the parameter list '{text}'.")));
        };
        let ty = part.get(1).and_then(|t| t.upper()).unwrap_or_default();
        let string = matches!(ty.as_str(), "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "SYSNAME" | "XML");
        let oid = match ty.as_str() {
            "VARBINARY" | "BINARY" | "IMAGE" => 17,
            _ => 0,
        };
        out.push((name.clone(), string, oid));
    }
    Ok(out)
}

impl Session {
    fn bridge(&mut self) -> &mut PgBridge {
        self.bridge.as_mut().expect("an open session has a bridge")
    }

    async fn open(&mut self, database: &str) -> Result<(), TsqlError> {
        let mut parameters = HashMap::new();
        parameters.insert("database".to_string(), database.to_string());
        let options = BridgeOptions {
            login: self.login.clone(),
            limits: self.shared.config.limits,
            watch: self.peer_watch.clone(),
            parameters,
        };
        match PgBridge::open(self.shared.catalog.clone(), database, options).await {
            Ok(b) => {
                if let Some(old) = self.bridge.replace(b) {
                    old.terminate().await;
                }
                Ok(())
            }
            Err(OpenRefused::NoDatabase) => Err(TsqlError::new(911, format!("Database '{database}' does not exist. Make sure that the name is entered correctly."))),
            Err(OpenRefused::Loading) => Err(TsqlError::new(922, format!("Database '{database}' is being recovered. Waiting until recovery is finished."))),
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
            if message.status & (tds::ST_RESET_CONNECTION | tds::ST_RESET_CONNECTION_SKIP_TRAN) != 0 {
                self.reset(message.status & tds::ST_RESET_CONNECTION_SKIP_TRAN != 0).await;
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
                    self.error_token(&TsqlError::unsupported("bulk load (BULK INSERT / SqlBulkCopy)"));
                    self.final_done();
                }
                tds::PT_SSPI => return Ok(()),
                other => {
                    self.error_token(&TsqlError::new(50000, format!("Unsupported TDS message type {other:#04x}")));
                    self.final_done();
                }
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
        }
    }

    fn error_token(&mut self, e: &TsqlError) {
        tds::message(self.wire.reply(), true, e.number, e.state, e.class, &e.message, SERVER_NAME, 1);
        self.last_error = Some(e.clone());
        self.error_number = e.number;
    }

    fn info_token(&mut self, number: i32, text: &str) {
        tds::message(self.wire.reply(), false, number, 1, 0, text, SERVER_NAME, 1);
    }

    /// The DONE (or DONEINPROC) after a statement.
    fn statement_done(&mut self, status: u16, command: u16, rows: u64) {
        let token = if self.in_rpc { tds::TOKEN_DONEINPROC } else { tds::TOKEN_DONE };
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
        let b = self.bridge();
        b.query(sql);
        let mut error = None;
        let mut tag = String::new();
        loop {
            let frame = b.next().await.map_err(|e| PgNotice { code: "08006".into(), message: e.to_string(), ..Default::default() })?;
            match frame.tag {
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'C' => tag = String::from_utf8_lossy(frame.body.strip_suffix(&[0]).unwrap_or(frame.body)).into_owned(),
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
    async fn fetch(&mut self, sql: &str, params: &[(i16, Option<Vec<u8>>, u32)]) -> Result<(Vec<u32>, Vec<Vec<Option<String>>>), PgNotice> {
        let b = self.bridge();
        if params.is_empty() {
            b.query(sql);
        } else {
            let oids: Vec<u32> = params.iter().map(|p| p.2).collect();
            b.parse("", sql, &oids);
            let refs: Vec<(i16, Option<&[u8]>)> = params.iter().map(|(f, v, _)| (*f, v.as_deref())).collect();
            b.bind("", "", &refs, &[0]);
            b.describe(b'P', "");
            b.execute("", 0);
            b.sync();
        }
        let mut error = None;
        let mut oids = Vec::new();
        let mut rows = Vec::new();
        loop {
            let frame = b.next().await.map_err(|e| PgNotice { code: "08006".into(), message: e.to_string(), ..Default::default() })?;
            match frame.tag {
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'T' => oids = parse_row_description(frame.body).map(|f| f.iter().map(|x| x.type_oid).collect()).unwrap_or_default(),
                b'D' => {
                    if let Ok(cells) = data_row_cells(frame.body) {
                        rows.push(cells.iter().map(|c| c.map(|v| String::from_utf8_lossy(v).into_owned())).collect());
                    }
                }
                b'Z' => break,
                _ => {}
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok((oids, rows)),
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
                let refs: Vec<(i16, Option<&[u8]>)> = params.iter().map(|(f, v, _)| (*f, v.as_deref())).collect();
                b.bind("", "", &refs, &[0]);
                b.describe(b'P', "");
                b.execute("", 0);
                b.sync();
            }
        }
        let mut columns: Option<Vec<Column>> = None;
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
                            let ty = tds::column_type(f.type_oid, f.type_modifier);
                            if ty.is_none() {
                                unknown = true;
                            }
                            Column {
                                // SQL Server leaves an unaliased expression
                                // unnamed.
                                name: if f.name == "?column?" { String::new() } else { f.name.clone() },
                                ty: ty.unwrap_or(ColType::Decimal { precision: 38, scale: 0 }),
                                oid: f.type_oid,
                            }
                        })
                        .collect();
                    let mut cols = cols;
                    if let Some((name, _)) = extra {
                        cols.push(Column { name: name.clone(), ty: ColType::Int(4), oid: 23 });
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
                b'D' => {
                    let cells = data_row_cells(frame.body)?;
                    count += 1;
                    if capture {
                        if let Some(Some(v)) = cells.first() {
                            identity = std::str::from_utf8(v).ok().and_then(|s| s.parse().ok());
                        }
                        continue;
                    }
                    let extra_value: Option<Vec<u8>> = extra.and_then(|(_, values)| values.get(count as usize - 1)).map(|v| v.as_bytes().to_vec());
                    if let Some(rows) = held.as_mut() {
                        let mut row: Vec<Option<Vec<u8>>> = cells.iter().map(|c| c.map(|v| v.to_vec())).collect();
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
                        let ty = cols.get(k).map(|c| c.ty.clone()).unwrap_or(ColType::NVarChar(None));
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
                b'C' => tag = String::from_utf8_lossy(frame.body.strip_suffix(&[0]).unwrap_or(frame.body)).into_owned(),
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'N' => {
                    let n = PgNotice::parse(frame.body);
                    self.info_token(0, &n.message);
                }
                b'Z' => break,
                _ => {}
            }
        }
        if let (Some(rows), Some(cols)) = (held, columns.as_mut()) {
            // The scale of each undeclared numeric: the most any value has.
            for (k, c) in cols.iter_mut().enumerate() {
                if c.oid == 1700 && matches!(c.ty, ColType::Decimal { precision: 38, scale: 0 }) {
                    let (mut int, mut frac) = (1usize, 0usize);
                    for r in &rows {
                        if let Some(Some(v)) = r.get(k) {
                            let (i, f) = tds::decimal_shape(&String::from_utf8_lossy(v));
                            int = int.max(i);
                            frac = frac.max(f);
                        }
                    }
                    let frac = frac.min(38 - int.min(38));
                    c.ty = ColType::Decimal { precision: (int + frac).clamp(1, 38) as u8, scale: frac as u8 };
                }
            }
            tds::col_metadata(self.wire.reply(), cols);
            for r in rows {
                let out = self.wire.reply();
                let at = out.len();
                out.push(0xd1);
                for (k, cell) in r.iter().enumerate() {
                    let ty = cols.get(k).map(|c| c.ty.clone()).unwrap_or(ColType::NVarChar(None));
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
            return Ok(Outcome::Failed(PgNotice { code: "22003".into(), message: e, ..Default::default() }, started));
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
            self.silent(&sql).await.map_err(|e| error_from_engine(&e))?;
        }
        self.trancount += 1;
        self.sync_transaction(true);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), TsqlError> {
        if self.trancount == 0 {
            return Err(TsqlError::new(3902, "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION."));
        }
        self.trancount -= 1;
        if self.trancount == 0 {
            let tag = self.silent("COMMIT").await.map_err(|e| error_from_engine(&e));
            let rolled_back = matches!(&tag, Ok(t) if t == "ROLLBACK");
            self.sync_transaction(!rolled_back);
            tag?;
            if rolled_back {
                return Err(TsqlError::new(3998, "The transaction was rolled back instead of committed: a statement in it had failed."));
            }
        }
        Ok(())
    }

    async fn rollback(&mut self, name: Option<&str>) -> Result<(), TsqlError> {
        if let Some(n) = name {
            let lower = n.to_lowercase();
            if let Some(pos) = self.savepoints.iter().rposition(|s| *s == lower) {
                self.savepoints.truncate(pos + 1);
                self.silent(&format!("ROLLBACK TO SAVEPOINT {}", ident_sql(&lower))).await.map_err(|e| error_from_engine(&e))?;
                return Ok(());
            }
        }
        if self.trancount == 0 && !self.bridge().in_block() {
            return Err(TsqlError::new(3903, "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION."));
        }
        let r = self.silent("ROLLBACK").await.map_err(|e| error_from_engine(&e));
        self.trancount = 0;
        self.sync_transaction(false);
        r.map(|_| ())
    }

    /// Runs a translated statement and answers for it. `Err` is the
    /// statement's error, not yet written.
    async fn forward(&mut self, forward: Forward, params: Option<&ParamCtx>) -> io::Result<Result<(), TsqlError>> {
        let ddl = forward.kind == Kind::Ddl;
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
            if let Err(e) = self.silent(&format!("SAVEPOINT {STATEMENT_SAVEPOINT}")).await {
                return Ok(Err(error_from_engine(&e)));
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
                        ParamValue::Text(t) => (0, Some(t.into_bytes()), if oid == 17 { 0 } else { oid }),
                        ParamValue::Bytes(b) => (1, Some(b), 17),
                    }
                })
                .collect(),
            None => Vec::new(),
        };
        let capture = matches!(forward.kind, Kind::Insert { identity: Some(_) });
        let mut last = None;
        let mut affected = 0u64;
        for sql in forward.statements.iter().filter(|s| !s.is_empty()) {
            let outcome = self.exchange(sql, &bound, capture, forward.extra_column.as_ref()).await?;
            let failed = matches!(outcome, Outcome::Failed(..));
            if let Outcome::Done { tag, .. } = &outcome {
                affected += tag_count(tag).unwrap_or(0);
            }
            last = Some(outcome);
            if failed {
                break;
            }
        }
        let outcome = last.unwrap_or(Outcome::Done { tag: String::new(), identity: None });
        match outcome {
            Outcome::Failed(n, started) => {
                let mut e = error_from_engine(&n);
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
                        let _ = self.silent(&format!("ROLLBACK TO SAVEPOINT {STATEMENT_SAVEPOINT}")).await;
                        let _ = self.silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}")).await;
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
                    let _ = self.silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}")).await;
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
                    let _ = self.silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}")).await;
                }
                if let Some(id) = identity {
                    self.last_identity = Some(id);
                }
                let rows = if ddl { 0 } else { tag_count(&tag).unwrap_or(affected) };
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
                self.statement_done(if counted { tds::DONE_COUNT } else { 0 }, command, if counted { rows } else { 0 });
                Ok(Ok(()))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Batches
    // -----------------------------------------------------------------------

    async fn sql_batch(&mut self, message: &Message) -> io::Result<()> {
        let mut r = Reader::new(&message.payload);
        if message.payload.len() >= 4 {
            let total = u32::from_le_bytes(message.payload[0..4].try_into().unwrap()) as usize;
            if total >= 4 && total <= message.payload.len() && total >= 22 {
                tds::all_headers(&mut r)?;
            }
        }
        let text = tds::utf16_text(&message.payload[r.pos..]);
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
        let stmts = match translate::parse_batch(&tokens) {
            Ok(s) => s,
            Err(e) => {
                self.error_token(&e);
                self.statement_done(tds::DONE_ERROR, 0, 0);
                return Ok(Flow::Abort);
            }
        };
        match self.run_list(text, &stmts, params).await? {
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Result<Flow, TsqlError>>> + Send + 'a>> {
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Result<Flow, TsqlError>>> + Send + 'a>> {
        Box::pin(async move {
            match stmt {
                Stmt::Simple(tokens) => self.simple(text, tokens, params).await,
                Stmt::Block(body) => self.run_list(text, body, params).await,
                Stmt::If { condition, then, otherwise } => {
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
                            return Ok(Err(TsqlError::new(50000, "a WHILE loop ran more than a million times; stopped")));
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

    async fn condition(&mut self, tokens: &[Token], params: Option<&ParamCtx>) -> Result<bool, TsqlError> {
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let sql = translate::condition_query(tokens.to_vec(), &Scope { s: self, params })?;
        let bound = bound_values(params);
        let (_, rows) = self.fetch(&sql, &bound).await.map_err(|e| error_from_engine(&e))?;
        Ok(rows.first().and_then(|r| r.first().cloned().flatten()).as_deref() == Some("1"))
    }

    /// Evaluates an expression to a value.
    async fn evaluate(&mut self, tokens: &[Token], params: Option<&ParamCtx>) -> Result<Value, TsqlError> {
        match tokens {
            [t] => match &t.tok {
                Tok::Str(s) => return Ok(Value::Str(String::from_utf8_lossy(s).into_owned())),
                Tok::Num(n) => return Ok(n.parse::<i64>().map(Value::Int).unwrap_or(Value::Num(n.clone()))),
                Tok::UserVar(name) if params.is_none() => {
                    return self.vars.get(name).map(|v| v.value.clone()).ok_or_else(|| TsqlError::new(137, format!("Must declare the scalar variable \"@{name}\".")));
                }
                Tok::Word(w) if w.eq_ignore_ascii_case("NULL") => return Ok(Value::Null),
                _ => {}
            },
            [m, t] if m.is_op("-") => {
                if let Tok::Num(n) = &t.tok {
                    return Ok(n.parse::<i64>().map(|v| Value::Int(-v)).unwrap_or(Value::Num(format!("-{n}"))));
                }
            }
            _ => {}
        }
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let sql = translate::value_query(tokens.to_vec(), &Scope { s: self, params })?;
        let bound = bound_values(params);
        let (oids, rows) = self.fetch(&sql, &bound).await.map_err(|e| error_from_engine(&e))?;
        Ok(value_of(oids.first().copied().unwrap_or(25), rows.first().and_then(|r| r.first().cloned()).flatten()))
    }

    fn assign(&mut self, name: &str, value: Value) -> Result<(), TsqlError> {
        let var = self.vars.get_mut(name).ok_or_else(|| TsqlError::new(137, format!("Must declare the scalar variable \"@{name}\".")))?;
        var.value = match (&value, var.numeric, var.string) {
            (Value::Str(s), true, _) => s.trim().parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Num(s.trim().to_string())),
            (Value::Int(n), _, true) => Value::Str(n.to_string()),
            (Value::Num(n), _, true) => Value::Str(n.clone()),
            _ => value,
        };
        Ok(())
    }

    async fn simple(&mut self, text: &str, tokens: &[Token], params: Option<&ParamCtx>) -> io::Result<Result<Flow, TsqlError>> {
        let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
        let next = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
        let r: Result<Flow, TsqlError> = match head.as_str() {
            "DECLARE" => self.declare(tokens, params).await,
            "SET" if matches!(tokens.get(1).map(|t| &t.tok), Some(Tok::UserVar(_))) => {
                let Some(Tok::UserVar(name)) = tokens.get(1).map(|t| &t.tok) else { unreachable!() };
                let name = name.clone();
                let op = tokens.get(2).map(|t| t.tok.clone());
                match op {
                    Some(Tok::Op(o)) if o == "=" => match self.evaluate(&tokens[3..], params).await {
                        Ok(v) => self.assign(&name, v).map(|_| Flow::Next),
                        Err(e) => Err(e),
                    },
                    Some(Tok::Op(o)) if o.len() == 2 && o.ends_with('=') => {
                        // SET @x += 1
                        let mut expr = vec![Token::new(Tok::UserVar(name.clone())), Token::op(&o[..1])];
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
                    let number = render_value(&args[0]).and_then(|v| v.parse().ok()).unwrap_or(50000);
                    let message = match args.get(1) {
                        Some(a) => self.evaluate(a, params).await.map(|v| v.text().unwrap_or_default()),
                        None => Ok(String::new()),
                    };
                    match message {
                        Ok(m) => Err(TsqlError { number, class: 16, state: 1, message: m }),
                        Err(e) => Err(e),
                    }
                }
            }
            "RETURN" => Ok(Flow::Return),
            "BREAK" => Ok(Flow::Break),
            "CONTINUE" => Ok(Flow::Continue),
            "USE" => {
                let db = tokens.get(1).and_then(|t| t.ident()).unwrap_or("").to_string();
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
                let name = tokens.get(2).and_then(|t| t.ident()).unwrap_or("").to_lowercase();
                if self.trancount == 0 {
                    Err(TsqlError::new(628, "Cannot issue SAVE TRANSACTION when there is no active transaction."))
                } else {
                    let r = self.silent(&format!("SAVEPOINT {}", ident_sql(&name))).await.map_err(|e| error_from_engine(&e));
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
                        tokio::time::sleep(std::time::Duration::from_secs_f64(secs.min(3600.0))).await;
                        Ok(Flow::Next)
                    }
                    _ => Err(TsqlError::unsupported("WAITFOR other than DELAY")),
                }
            }
            "GRANT" | "DENY" | "REVOKE" | "DBCC" | "BACKUP" | "RESTORE" | "OPEN" | "CLOSE" | "FETCH" | "DEALLOCATE" | "GOTO" | "KILL" => {
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
                if (matches!(tokens.get(1).map(|t| &t.tok), Some(Tok::UserVar(_))) && tokens.get(2).is_some_and(|t| t.is_op("=")))
                    || (next == "DISTINCT" && matches!(tokens.get(2).map(|t| &t.tok), Some(Tok::UserVar(_))) && tokens.get(3).is_some_and(|t| t.is_op("="))) =>
            {
                self.assignment_select(text, tokens, params).await
            }
            _ => {
                if let Some(p) = params {
                    p.used.lock().clear();
                }
                let translated = translate::translate(text, tokens.to_vec(), &Scope { s: self, params });
                match &translated {
                    Ok(f) => log::debug!("T-SQL {:?} -> {:?}", crate::sqlcompat::render_text(tokens), f.statements),
                    Err(e) => log::debug!("T-SQL {:?} refused: {}", crate::sqlcompat::render_text(tokens), e.message),
                }
                match translated {
                    Ok(forward) => match self.forward(forward, params).await? {
                        Ok(()) => Ok(Flow::Next),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
        };
        Ok(r)
    }

    async fn declare(&mut self, tokens: &[Token], params: Option<&ParamCtx>) -> Result<Flow, TsqlError> {
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
                return Err(TsqlError::unsupported("table variables"));
            }
            let string = matches!(ty.as_str(), "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "SYSNAME" | "XML" | "UNIQUEIDENTIFIER");
            let numeric = matches!(ty.as_str(), "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "BIT" | "DECIMAL" | "NUMERIC" | "MONEY" | "FLOAT" | "REAL");
            self.vars.insert(name.clone(), Var { value: Value::Null, string, numeric });
            if let Some(eq) = part.iter().position(|t| t.is_op("=")) {
                let v = self.evaluate(&part[eq + 1..], params).await?;
                self.assign(&name, v)?;
            }
        }
        Ok(Flow::Next)
    }

    async fn assignment_select(&mut self, text: &str, tokens: &[Token], params: Option<&ParamCtx>) -> Result<Flow, TsqlError> {
        if let Some(p) = params {
            p.used.lock().clear();
        }
        let Some((sql, names)) = translate::assignment_select(text, tokens, &Scope { s: self, params })? else {
            return Err(TsqlError::syntax("an assignment SELECT was expected"));
        };
        let bound = bound_values(params);
        let (oids, rows) = self.fetch(&sql, &bound).await.map_err(|e| error_from_engine(&e))?;
        // The last row's values, as SQL Server leaves them.
        if let Some(row) = rows.last() {
            for (k, name) in names.iter().enumerate() {
                let v = value_of(oids.get(k).copied().unwrap_or(25), row.get(k).cloned().flatten());
                self.assign(name, v)?;
            }
        }
        self.rowcount = rows.len() as i64;
        self.statement_done(if self.nocount { 0 } else { tds::DONE_COUNT }, 0xc1, rows.len() as u64);
        Ok(Flow::Next)
    }

    async fn raiserror(&mut self, tokens: &[Token], params: Option<&ParamCtx>) -> Result<Flow, TsqlError> {
        let mut inner = &tokens[1..];
        if inner.first().is_some_and(|t| t.is_op("(")) {
            let close = crate::sqlcompat::matching_paren(inner, 0).unwrap_or(inner.len() - 1);
            inner = &inner[1..close];
        }
        let args = split_commas(inner);
        if args.len() < 3 {
            return Err(TsqlError::syntax("RAISERROR needs a message, a severity and a state"));
        }
        let msg = self.evaluate(&args[0], params).await?.text().unwrap_or_default();
        let severity = render_value(&args[1]).and_then(|v| v.parse::<u8>().ok()).unwrap_or(16);
        let state = render_value(&args[2]).and_then(|v| v.parse::<u8>().ok()).unwrap_or(1);
        let mut values = Vec::new();
        for a in args.iter().skip(3) {
            values.push(self.evaluate(a, params).await?.text().unwrap_or_else(|| "(null)".into()));
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
            Err(TsqlError { number: 50000, class: severity.min(18), state, message: text })
        }
    }

    async fn use_database(&mut self, shown: &str) -> Result<Flow, TsqlError> {
        let db = resolve_database(&self.shared, shown).to_string();
        let db = db.as_str();
        if self.bridge().in_block() {
            return Err(TsqlError::new(574, "USE inside a transaction is not supported: COMMIT or ROLLBACK first."));
        }
        if self.shared.catalog.get(db).is_none() {
            return Err(TsqlError::new(911, format!("Database '{db}' does not exist. Make sure that the name is entered correctly.")));
        }
        let old = self.bridge().database_name.clone();
        if old != db {
            self.prepared.clear();
            self.open(db).await?;
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
                let at = words.iter().position(|w| w == "LEVEL").ok_or_else(|| TsqlError::syntax("SET TRANSACTION ISOLATION LEVEL was expected"))?;
                let level = match (words.get(at + 1).map(String::as_str), words.get(at + 2).map(String::as_str)) {
                    (Some("READ"), Some("UNCOMMITTED")) => "READ UNCOMMITTED",
                    (Some("READ"), Some("COMMITTED")) => "READ COMMITTED",
                    (Some("REPEATABLE"), _) => "REPEATABLE READ",
                    (Some("SERIALIZABLE"), _) | (Some("SNAPSHOT"), _) => "SERIALIZABLE",
                    _ => return Err(TsqlError::syntax("an isolation level was expected")),
                };
                self.silent(&format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"))
                    .await
                    .map_err(|e| error_from_engine(&e))?;
            }
            "LOCK_TIMEOUT" => {
                let ms: i64 = tokens.get(2).and_then(|t| match &t.tok {
                    Tok::Num(n) => n.parse().ok(),
                    _ => None,
                }).or_else(|| if tokens.get(2).is_some_and(|t| t.is_op("-")) { Some(-1) } else { None })
                .unwrap_or(-1);
                let value = if ms < 0 { 0 } else { ms.max(1) };
                self.silent(&format!("SET lock_timeout = {value}")).await.map_err(|e| error_from_engine(&e))?;
            }
            "ROWCOUNT" => {
                let n = render_value(&tokens[2..]).unwrap_or_default();
                if n != "0" {
                    return Err(TsqlError::unsupported("SET ROWCOUNT (use TOP)"));
                }
            }
            "FMTONLY" | "NOEXEC" | "PARSEONLY" if on() => return Err(TsqlError::unsupported(format!("SET {first} ON"))),
            "SHOWPLAN_ALL" | "SHOWPLAN_TEXT" | "SHOWPLAN_XML" if on() => return Err(TsqlError::unsupported(format!("SET {first} ON"))),
            // Accepted, with no effect here: ANSI_NULLS, QUOTED_IDENTIFIER,
            // ANSI_WARNINGS, ANSI_PADDING, ARITHABORT, CONCAT_NULL_YIELDS_NULL,
            // TEXTSIZE, DATEFORMAT, DATEFIRST, LANGUAGE, DEADLOCK_PRIORITY,
            // STATISTICS, IDENTITY_INSERT, CONTEXT_INFO, ...
            _ => {}
        }
        Ok(Flow::Next)
    }

    /// EXEC: a string as a batch, or one of the system procedures.
    async fn exec_statement(&mut self, tokens: &[Token], params: Option<&ParamCtx>) -> Result<Flow, TsqlError> {
        let rest = &tokens[1..];
        if rest.first().is_some_and(|t| t.is_op("(")) {
            // EXEC('…') / EXEC(@sql)
            let close = crate::sqlcompat::matching_paren(rest, 0).ok_or_else(|| TsqlError::syntax("unbalanced EXEC"))?;
            let sql = self.evaluate(&rest[1..close], params).await?.text().unwrap_or_default();
            return self.nested_batch(&sql, None).await;
        }
        // EXEC @rc = proc …
        let mut rest = rest;
        if matches!(rest.first().map(|t| &t.tok), Some(Tok::UserVar(_))) && rest.get(1).is_some_and(|t| t.is_op("=")) {
            rest = &rest[2..];
        }
        let mut k = 0;
        let mut name = String::new();
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
        for a in &args {
            if a.is_empty() {
                continue;
            }
            if let (Some(Tok::UserVar(n)), Some(eq)) = (a.first().map(|t| &t.tok), a.get(1)) {
                if eq.is_op("=") {
                    let v = self.evaluate(&a[2..], params).await?;
                    named.push((Some(n.clone()), v));
                    continue;
                }
            }
            let v = self.evaluate(a, params).await?;
            values.push(v.clone());
            named.push((None, v));
        }
        match name.as_str() {
            "sp_executesql" => {
                let stmt = named.first().and_then(|(_, v)| v.text()).unwrap_or_default();
                let decl = named.get(1).and_then(|(_, v)| v.text()).unwrap_or_default();
                let decls = parse_declarations(&decl)?;
                let mut vals = vec![ParamValue::Null; decls.len()];
                let mut position = 0;
                for (n, v) in named.iter().skip(2) {
                    let k = match n {
                        Some(n) => decls.iter().position(|d| d.0 == *n).ok_or_else(|| TsqlError::new(8145, format!("@{n} is not a parameter for procedure sp_executesql.")))?,
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
                let ctx = ParamCtx { decls, values: vals, used: parking_lot::Mutex::new(Vec::new()) };
                self.nested_batch(&stmt, Some(&ctx)).await
            }
            "sp_rename" => {
                let old = values.first().and_then(|v| v.text()).unwrap_or_default();
                let new = values.get(1).and_then(|v| v.text()).unwrap_or_default();
                let kind = values.get(2).and_then(|v| v.text()).unwrap_or_default().to_ascii_uppercase();
                let parts: Vec<String> = old.split('.').map(|p| p.trim_matches(['[', ']']).to_lowercase()).filter(|p| p != "dbo").collect();
                let sql = if kind == "COLUMN" && parts.len() >= 2 {
                    format!("ALTER TABLE {} RENAME COLUMN {} TO {}", ident_sql(&parts[parts.len() - 2]), ident_sql(&parts[parts.len() - 1]), ident_sql(&new.to_lowercase()))
                } else if kind == "INDEX" {
                    return Err(TsqlError::unsupported("sp_rename of an index"));
                } else {
                    format!("ALTER TABLE {} RENAME TO {}", ident_sql(parts.last().map(String::as_str).unwrap_or("")), ident_sql(&new.to_lowercase()))
                };
                self.silent(&sql).await.map_err(|e| error_from_engine(&e))?;
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "sp_reset_connection" | "sp_set_session_context" | "sp_setapprole" => {
                self.statement_done(0, 0, 0);
                Ok(Flow::Next)
            }
            "" => Err(TsqlError::syntax("EXEC needs a procedure")),
            other => Err(TsqlError::new(2812, format!("Could not find stored procedure '{other}'."))),
        }
    }

    /// A batch inside this one (EXEC, sp_executesql): its errors are its
    /// own, and do not end the outer batch.
    async fn nested_batch(&mut self, sql: &str, params: Option<&ParamCtx>) -> Result<Flow, TsqlError> {
        let tokens = lex::lex(sql).map_err(|e| TsqlError::syntax(e.0))?;
        let stmts = translate::parse_batch(&tokens)?;
        let owned = sql.to_string();
        match self.run_list(&owned, &stmts, params).await {
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
                    tds::done(self.wire.reply(), tds::TOKEN_DONEPROC, tds::DONE_ERROR, 0, 0);
                    self.in_rpc = false;
                    return Ok(());
                }
            };
            let more = r.remaining() > 0 && matches!(r.buf[r.pos], 0x80 | 0xff);
            let status = self.call(&proc_name, params).await?;
            tds::return_status(self.wire.reply(), status);
            let mut done = if status != 0 { tds::DONE_ERROR } else { 0 };
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
    async fn call(&mut self, name: &str, params: Vec<tds::Param>) -> io::Result<i32> {
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
                        self.prepared.insert(handle, Prepared { sql: sql.clone(), decls: decls.clone() });
                        let r = if name == "sp_prepexec" {
                            let values = bind_values(&decls, &params[3.min(params.len())..]);
                            self.run_rpc_batch(&sql, decls, values).await?
                        } else {
                            Ok(())
                        };
                        tds::return_value_int(self.wire.reply(), 0, params.first().map(|p| p.name.as_str()).unwrap_or("@handle"), handle);
                        r
                    }
                    Err(e) => Err(e),
                }
            }
            "sp_execute" => match int(params.first()).and_then(|h| self.prepared.get(&h).cloned()) {
                Some(p) => {
                    let values = bind_values(&p.decls, &params[1.min(params.len())..]);
                    self.run_rpc_batch(&p.sql, p.decls, values).await?
                }
                None => Err(TsqlError::new(8179, "Could not find prepared statement with handle.")),
            },
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
            other => {
                // A procedure by name: sp_executesql spelled out, or EXEC of
                // anything else, refused there.
                let mut tokens = vec![Token::word("EXEC"), Token::raw(other.to_string())];
                for (k, p) in params.iter().enumerate() {
                    if k > 0 {
                        tokens.push(Token::op(","));
                    }
                    if !p.name.is_empty() {
                        tokens.push(Token::new(Tok::UserVar(p.name.trim_start_matches('@').to_lowercase())));
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
        let status = if self.error_number != 0 { 1 } else { 0 };
        if self.error_number == 0 {
            self.error_number = failed_before.min(0);
        }
        Ok(status)
    }

    async fn run_rpc_batch(&mut self, sql: &str, decls: Vec<(String, bool, u32)>, values: Vec<ParamValue>) -> io::Result<Result<(), TsqlError>> {
        let ctx = ParamCtx { decls, values, used: parking_lot::Mutex::new(Vec::new()) };
        let tokens = match lex::lex(sql) {
            Ok(t) => t,
            Err(e) => return Ok(Err(TsqlError::syntax(e.0))),
        };
        let stmts = match translate::parse_batch(&tokens) {
            Ok(s) => s,
            Err(e) => return Ok(Err(e)),
        };
        match self.run_list(sql, &stmts, Some(&ctx)).await? {
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
                    self.rollback(if name.is_empty() { None } else { Some(&name) }).await
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
                let res = self.silent(&format!("SAVEPOINT {}", ident_sql(&name))).await.map_err(|e| error_from_engine(&e));
                if res.is_ok() {
                    self.savepoints.push(name);
                }
                res.map(|_| ())
            }
            other => Err(TsqlError::unsupported(format!("transaction manager request {other}"))),
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

/// The values of a call's parameters, in the order of the declarations
/// (by name, or by position when unnamed).
fn bind_values(decls: &[(String, bool, u32)], params: &[tds::Param]) -> Vec<ParamValue> {
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
        .map(|&k| match p.values.get(k).cloned().unwrap_or(ParamValue::Null) {
            ParamValue::Null => (0, None, 0),
            ParamValue::Text(t) => (0, Some(t.into_bytes()), 0),
            ParamValue::Bytes(b) => (1, Some(b), 17),
        })
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
