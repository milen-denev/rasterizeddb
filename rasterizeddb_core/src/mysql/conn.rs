//! One MySQL connection: handshake, authentication, and the command loop.
//!
//! **Statement atomicity.** In MySQL an error inside a transaction fails the
//! statement, not the transaction: the client may go on and commit what came
//! before it. In the engine (as in PostgreSQL) an error poisons the block. So
//! inside a transaction each statement runs under a savepoint, rolled back to
//! if the statement fails — except a deadlock or serialization failure, which
//! in MySQL rolls the whole transaction back and does here too.
//!
//! **Implicit transactions.** With `autocommit = 0`, MySQL opens a
//! transaction at the first statement and keeps it open until COMMIT or
//! ROLLBACK; DDL commits whatever is open first, as it does there.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use rclite::Arc;

use crate::core::database::Database;
use crate::pgwire::proxied::{
    BridgeOptions, OpenRefused, PgBridge, PgField, PgNotice, ProxiedLogin, data_row_cells,
    parse_row_description, tag_count,
};

use super::admin::{self, Action, Assignment, Filter, Show, Target};
use super::lex::{self, Mode, Tok, Token};
use super::packet::{MyStream, Packets, Reader, put_lenenc_int};
use super::results::{self, MyColumn};
use super::translate::{self, Env, Forward, Kind, MyError, Value};
use super::{MyAuth, MyTls, Registered, SERVER_VERSION, Shared};

// Capability flags.
const CLIENT_LONG_PASSWORD: u32 = 1;
const CLIENT_FOUND_ROWS: u32 = 2;
const CLIENT_LONG_FLAG: u32 = 4;
const CLIENT_CONNECT_WITH_DB: u32 = 8;
const CLIENT_NO_SCHEMA: u32 = 16;
const CLIENT_IGNORE_SPACE: u32 = 256;
const CLIENT_PROTOCOL_41: u32 = 512;
const CLIENT_INTERACTIVE: u32 = 1024;
const CLIENT_SSL: u32 = 2048;
const CLIENT_IGNORE_SIGPIPE: u32 = 4096;
const CLIENT_TRANSACTIONS: u32 = 8192;
const CLIENT_RESERVED: u32 = 16384;
const CLIENT_SECURE_CONNECTION: u32 = 32768;
const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
const CLIENT_PS_MULTI_RESULTS: u32 = 1 << 18;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 1 << 22;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

const SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_NO_SCHEMA
    | CLIENT_IGNORE_SPACE
    | CLIENT_PROTOCOL_41
    | CLIENT_INTERACTIVE
    | CLIENT_IGNORE_SIGPIPE
    | CLIENT_TRANSACTIONS
    | CLIENT_RESERVED
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PS_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
    | CLIENT_DEPRECATE_EOF;

const PLUGIN: &str = "caching_sha2_password";

/// The savepoint each statement inside a transaction runs under.
const STATEMENT_SAVEPOINT: &str = "rdb_mysql_statement";

/// `SHA256(SHA256(password))` of a role's last full login, by database and
/// role, with the stored verifier it was checked against: a later fast login
/// is accepted only while the verifier is still the same, so a changed
/// password retires it.
fn fast_auth_cache() -> &'static parking_lot::Mutex<HashMap<(String, String), (String, [u8; 32])>> {
    static CACHE: std::sync::OnceLock<
        parking_lot::Mutex<HashMap<(String, String), (String, [u8; 32])>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Equality that takes as long whichever byte differs.
fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(&out);
    a
}

/// A prepared statement.
struct Prepared {
    /// The engine's name for it, or `None` when it is run as text at each
    /// execute (statements the engine cannot prepare: SET, SHOW, DDL).
    engine_name: Option<String>,
    kind: Kind,
    /// The original text, for statements run as text.
    sql: Vec<u8>,
    params: usize,
    param_oids: Vec<u32>,
    /// The parameter types the client last bound.
    param_types: Vec<(u8, bool)>,
    long_data: HashMap<u16, Vec<u8>>,
}

/// How a statement's rows reach the client.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rows {
    Text,
    Binary,
}

/// What a statement ended as.
enum Outcome {
    /// A result set was sent.
    Rows {
        count: u64,
    },
    /// A command tag.
    Done {
        tag: String,
        insert_id: Option<u64>,
        returned: u64,
    },
    Failed(PgNotice),
}

/// What the engine is sent for one statement.
enum Request<'a> {
    Simple(&'a str),
    Bound {
        statement: &'a str,
        params: Vec<(i16, Option<Vec<u8>>)>,
    },
}

pub(crate) struct Session {
    io: Packets,
    shared: StdArc<Shared>,
    caps: u32,
    conn_id: u32,
    user: String,
    login: ProxiedLogin,
    bridge: Option<PgBridge>,
    vars: BTreeMap<String, Value>,
    user_vars: HashMap<String, Value>,
    last_insert_id: u64,
    found_rows: u64,
    row_count: i64,
    warnings: Vec<(String, u16, String)>,
    /// The ON UPDATE CURRENT_TIMESTAMP columns of each table, read before a
    /// write is translated.
    on_update: HashMap<String, Vec<String>>,
    /// TEMPORARY tables: their name → the table that holds them (the engine
    /// has no temporary tables; see [`crate::sqlcompat::TEMP_PREFIX`]).
    temp_tables: HashMap<String, String>,
    temp_prefix: String,
    statements: HashMap<u32, Prepared>,
    next_statement: u32,
    next_isolation: Option<String>,
    next_read_only: Option<bool>,
    deferred_fks: Vec<String>,
    registered: StdArc<Registered>,
    peer_watch: Option<StdArc<crate::pgwire::PeerWatch>>,
}

pub(crate) async fn serve_connection(
    stream: MyStream,
    peer: String,
    peer_uid: Option<u32>,
    shared: StdArc<Shared>,
    over_limit: bool,
) -> io::Result<()> {
    let _ = peer_uid;
    #[cfg(unix)]
    let watch = {
        use std::os::fd::AsFd;
        crate::pgwire::proxied::peer_watch_of(stream.as_fd())
    };
    #[cfg(not(unix))]
    let watch = None;
    let mut io = Packets::new(stream);
    let conn_id = super::next_connection_id();
    let nonce = {
        let mut n = [0u8; 20];
        crate::auth::fill_random(&mut n);
        // The scramble is sent NUL-terminated: no NUL inside it.
        for b in n.iter_mut() {
            *b = (*b % 94) + 33;
        }
        n
    };

    let deadline = shared.config.limits.handshake_timeout;
    let login = tokio::time::timeout(
        deadline,
        handshake(&mut io, &shared, conn_id, &nonce, over_limit),
    )
    .await;
    let (caps, user, database, login) = match login {
        Ok(Ok(Some(done))) => done,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            let error = MyError::new(1043, "08S01", "Bad handshake: authentication timed out");
            results::write_err(&mut io, &error);
            let _ = io.flush().await;
            return Ok(());
        }
    };

    let registered = StdArc::new(Registered {
        user: user.clone(),
        host: peer.clone(),
        database: parking_lot::Mutex::new(database.clone()),
        cancel: parking_lot::Mutex::new(None),
        kill: tokio::sync::Notify::new(),
        since: std::time::Instant::now(),
        query: parking_lot::Mutex::new(None),
    });
    super::registry().lock().insert(conn_id, registered.clone());

    // Logged in: packets up to max_allowed_packet, and read ahead.
    io.max_packet = super::packet::MAX_ALLOWED_PACKET;
    io.exact = false;
    let tls = io.stream.is_secure();
    let port = shared.config.tcp.as_ref().map(|t| t.1).unwrap_or(0);
    let mut session = Session {
        io,
        shared: shared.clone(),
        caps,
        conn_id,
        user,
        login,
        bridge: None,
        vars: admin::initial_variables(tls, port),
        user_vars: HashMap::new(),
        last_insert_id: 0,
        found_rows: 0,
        row_count: -1,
        warnings: Vec::new(),
        on_update: HashMap::new(),
        temp_tables: HashMap::new(),
        temp_prefix: crate::sqlcompat::temp_table_prefix("my", crate::sqlcompat::next_connection_id()),
        statements: HashMap::new(),
        next_statement: 1,
        next_isolation: None,
        next_read_only: None,
        deferred_fks: Vec::new(),
        registered,
        peer_watch: watch,
    };
    let result = match session.open(&database).await {
        Ok(()) => {
            let status = session.status(false);
            results::write_ok(&mut session.io, 0, 0, status, 0, "", false);
            session.io.flush().await?;
            session.command_loop().await
        }
        Err(error) => {
            results::write_err(&mut session.io, &error);
            session.io.flush().await
        }
    };
    super::registry().lock().remove(&conn_id);
    session.drop_temp_tables().await;
    if let Some(bridge) = session.bridge.take() {
        bridge.terminate().await;
    }
    let _ = session.io.stream.shutdown().await;
    result
}

type Login = (u32, String, String, ProxiedLogin);

/// The handshake and authentication. `None` when the client was refused
/// (and told why).
async fn handshake(
    io: &mut Packets,
    shared: &StdArc<Shared>,
    conn_id: u32,
    nonce: &[u8; 20],
    over_limit: bool,
) -> io::Result<Option<Login>> {
    let tls_offered = shared.acceptor.is_some();
    let mut caps = SERVER_CAPABILITIES;
    if tls_offered {
        caps |= CLIENT_SSL;
    }
    // Protocol::HandshakeV10
    let mut p = Vec::with_capacity(128);
    p.push(10);
    p.extend_from_slice(SERVER_VERSION.as_bytes());
    p.push(0);
    p.extend_from_slice(&conn_id.to_le_bytes());
    p.extend_from_slice(&nonce[..8]);
    p.push(0);
    p.extend_from_slice(&(caps as u16).to_le_bytes());
    p.push(results::CHARSET_UTF8MB4_BIN as u8);
    p.extend_from_slice(&results::STATUS_AUTOCOMMIT.to_le_bytes());
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes());
    p.push(21);
    p.extend_from_slice(&[0u8; 10]);
    p.extend_from_slice(&nonce[8..]);
    p.push(0);
    p.extend_from_slice(PLUGIN.as_bytes());
    p.push(0);
    io.seq = 0;
    io.packet(&p);
    io.flush().await?;

    let mut response = io.read().await?;
    let mut client_caps = Reader::new(&response).u32().unwrap_or(0);
    // SSLRequest: the short form of the response, then TLS.
    if client_caps & CLIENT_SSL != 0 && response.len() == 32 {
        let Some(acceptor) = shared.acceptor.clone().filter(|_| tls_offered) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "client asked for TLS, which is not offered",
            ));
        };
        if io.has_buffered_input() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data after the SSL request",
            ));
        }
        let seq = io.seq;
        let plain = std::mem::replace(&mut io.stream, MyStream::Upgrading);
        let aborted =
            |e: io::Error| io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string());
        io.stream = match plain {
            MyStream::Tcp(tcp) => {
                MyStream::Tls(Box::new(acceptor.accept(tcp).await.map_err(aborted)?))
            }
            #[cfg(unix)]
            MyStream::Unix(unix) => {
                MyStream::TlsUnix(Box::new(acceptor.accept(unix).await.map_err(aborted)?))
            }
            _ => return Err(io::Error::other("TLS asked for twice")),
        };
        io.seq = seq;
        response = io.read().await?;
        client_caps = Reader::new(&response).u32().unwrap_or(0);
    }

    let refuse = |io: &mut Packets, error: MyError| {
        results::write_err(io, &error);
    };
    if over_limit {
        refuse(io, MyError::new(1040, "08004", "Too many connections"));
        io.flush().await?;
        return Ok(None);
    }
    if client_caps & CLIENT_PROTOCOL_41 == 0 {
        refuse(
            io,
            MyError::new(
                1043,
                "08S01",
                "Bad handshake: the client does not speak protocol 4.1",
            ),
        );
        io.flush().await?;
        return Ok(None);
    }
    if shared.config.tls == MyTls::Require && !io.stream.is_secure() {
        refuse(
            io,
            MyError::new(
                3159,
                "HY000",
                "Connections using insecure transport are prohibited while --require_secure_transport=ON.",
            ),
        );
        io.flush().await?;
        return Ok(None);
    }

    // Protocol::HandshakeResponse41
    let mut r = Reader::new(&response);
    let _caps = r.u32()?;
    let _max_packet = r.u32()?;
    let _charset = r.u8()?;
    r.bytes(23)?;
    let user = String::from_utf8_lossy(r.cstr()?).into_owned();
    let auth_response: Vec<u8> = if client_caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        r.lenenc_str()?.to_vec()
    } else if client_caps & CLIENT_SECURE_CONNECTION != 0 {
        let n = r.u8()? as usize;
        r.bytes(n)?.to_vec()
    } else {
        r.cstr()?.to_vec()
    };
    let mut database = String::new();
    if client_caps & CLIENT_CONNECT_WITH_DB != 0 && r.remaining() > 0 {
        database = String::from_utf8_lossy(r.cstr()?).into_owned();
    }
    let mut plugin = String::new();
    if client_caps & CLIENT_PLUGIN_AUTH != 0 && r.remaining() > 0 {
        plugin = String::from_utf8_lossy(r.cstr()?).into_owned();
    }
    let caps = client_caps & (SERVER_CAPABILITIES | CLIENT_SSL);
    if database.is_empty() {
        database = shared.catalog.default_name().to_string();
    }
    let Some(db) = shared.catalog.get(&database) else {
        refuse(
            io,
            MyError::new(1049, "42000", format!("Unknown database '{database}'")),
        );
        io.flush().await?;
        return Ok(None);
    };

    let method = if io.stream.is_unix() {
        shared
            .config
            .socket
            .as_ref()
            .and_then(|s| s.auth)
            .unwrap_or(shared.config.auth)
    } else {
        shared.config.auth
    };
    let login = match method {
        MyAuth::Trust => ProxiedLogin {
            user: user.clone(),
            superuser: false,
            authenticated: false,
        },
        MyAuth::Password => {
            match authenticate(io, &db, &user, &database, auth_response, &plugin, nonce).await? {
                Some(login) => login,
                None => {
                    refuse(
                        io,
                        MyError::new(
                            1045,
                            "28000",
                            format!("Access denied for user '{user}' (using password: YES)"),
                        ),
                    );
                    io.flush().await?;
                    return Ok(None);
                }
            }
        }
    };
    Ok(Some((caps, user, database, login)))
}

/// `caching_sha2_password`. `None` for a refused login.
async fn authenticate(
    io: &mut Packets,
    database: &Arc<Database>,
    user: &str,
    database_name: &str,
    mut response: Vec<u8>,
    plugin: &str,
    nonce: &[u8; 20],
) -> io::Result<Option<ProxiedLogin>> {
    use crate::auth::Verifier;
    if plugin != PLUGIN {
        // AuthSwitchRequest to ours.
        let mut p = vec![0xfe];
        p.extend_from_slice(PLUGIN.as_bytes());
        p.push(0);
        p.extend_from_slice(nonce);
        p.push(0);
        io.packet(&p);
        io.flush().await?;
        response = io.read().await?;
    }
    let role = crate::auth::lookup_role(database, user).await;
    let verifier = role.as_ref().and_then(|r| r.verifier.clone());
    let may_login = role.as_ref().map(|r| r.can_login).unwrap_or(false);
    let superuser = role.as_ref().map(|r| r.is_superuser).unwrap_or(false);
    let fingerprint = verifier.as_ref().map(|v| v.to_stored()).unwrap_or_default();
    let key = (database_name.to_string(), user.to_string());
    let login = ProxiedLogin {
        user: user.to_string(),
        superuser,
        authenticated: true,
    };

    // Fast path: a scramble against the cached hash.
    if response.len() == 32 {
        let cached = fast_auth_cache().lock().get(&key).cloned();
        if let Some((stored, h2)) = cached {
            if stored == fingerprint && may_login {
                let k = sha256(&[&h2, nonce]);
                let mut h1 = [0u8; 32];
                for i in 0..32 {
                    h1[i] = response[i] ^ k[i];
                }
                if constant_time_eq(&sha256(&[&h1]), &h2) {
                    io.packet(&[0x01, 0x03]);
                    return Ok(Some(login));
                }
            }
        }
    }
    // Empty password.
    if response.is_empty() {
        let ok = may_login
            && verifier
                .as_ref()
                .is_some_and(|v| crate::auth::verify_password(v, user, ""));
        return Ok(ok.then_some(login));
    }
    // Full authentication: the password itself, over TLS or the socket.
    io.packet(&[0x01, 0x04]);
    io.flush().await?;
    let answer = io.read().await?;
    if answer.as_slice() == [0x02] {
        // A request for the server's RSA key: not offered.
        let error = MyError::new(
            2061,
            "HY000",
            "Authentication requires a secure connection: connect with TLS (or over the unix socket) for the first login",
        );
        results::write_err(io, &error);
        io.flush().await?;
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "RSA key requested",
        ));
    }
    if !io.stream.is_secure() {
        return Ok(None);
    }
    let password = answer.strip_suffix(&[0]).unwrap_or(&answer);
    let password = String::from_utf8_lossy(password).into_owned();
    let verifier = verifier.unwrap_or(Verifier::Plain(String::new()));
    if !may_login || !crate::auth::verify_password(&verifier, user, &password) {
        return Ok(None);
    }
    let h1 = sha256(&[password.as_bytes()]);
    let h2 = sha256(&[&h1]);
    fast_auth_cache().lock().insert(key, (fingerprint, h2));
    Ok(Some(login))
}

impl Env for Session {
    fn system_variable(&self, name: &str) -> Option<Value> {
        match name {
            "warning_count" => Some(Value::Int(self.warnings.len() as i64)),
            "error_count" => Some(Value::Int(0)),
            "identity" | "last_insert_id" => Some(Value::Int(self.last_insert_id as i64)),
            "hostname" => Some(Value::Str("localhost".into())),
            "pseudo_thread_id" => Some(Value::Int(self.conn_id as i64)),
            "in_transaction" => Some(Value::Int(
                self.bridge.as_ref().is_some_and(|b| b.in_block()) as i64,
            )),
            _ => self.vars.get(name).cloned(),
        }
    }
    fn user_variable(&self, name: &str) -> Value {
        self.user_vars.get(name).cloned().unwrap_or(Value::Null)
    }
    fn database(&self) -> &Arc<Database> {
        &self
            .bridge
            .as_ref()
            .expect("a session after authentication has a bridge")
            .database
    }
    fn database_name(&self) -> &str {
        self.bridge
            .as_ref()
            .map(|b| b.database_name.as_str())
            .unwrap_or("")
    }
    fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
    fn found_rows(&self) -> u64 {
        self.found_rows
    }
    fn row_count(&self) -> i64 {
        self.row_count
    }
    fn connection_id(&self) -> u32 {
        self.conn_id
    }
    fn user(&self) -> &str {
        &self.user
    }
    fn on_update_columns(&self, table: &str) -> Vec<String> {
        self.on_update.get(table).cloned().unwrap_or_default()
    }
    fn mode(&self) -> Mode {
        Mode::from_sql_mode(
            self.vars
                .get("sql_mode")
                .and_then(|v| v.text())
                .as_deref()
                .unwrap_or(""),
        )
    }
}

impl Session {
    fn deprecate_eof(&self) -> bool {
        self.caps & CLIENT_DEPRECATE_EOF != 0
    }

    fn autocommit(&self) -> bool {
        self.vars
            .get("autocommit")
            .map(|v| *v != Value::Int(0))
            .unwrap_or(true)
    }

    fn status(&self, more: bool) -> u16 {
        let mut s = 0u16;
        if self.bridge.as_ref().is_some_and(|b| b.in_block()) {
            s |= results::STATUS_IN_TRANS;
        }
        if self.autocommit() {
            s |= results::STATUS_AUTOCOMMIT;
        }
        if more {
            s |= results::STATUS_MORE_RESULTS;
        }
        if self.mode().no_backslash_escapes {
            s |= results::STATUS_NO_BACKSLASH_ESCAPES;
        }
        s
    }

    fn bridge(&mut self) -> &mut PgBridge {
        self.bridge
            .as_mut()
            .expect("a session after authentication has a bridge")
    }

    /// Opens the engine session on `database` (at login and at `USE`).
    async fn open(&mut self, database: &str) -> Result<(), MyError> {
        let mut parameters = HashMap::new();
        parameters.insert("database".to_string(), database.to_string());
        let options = BridgeOptions {
            login: self.login.clone(),
            limits: self.shared.config.limits,
            watch: self.peer_watch.clone(),
            parameters,
        };
        match PgBridge::open(self.shared.catalog.clone(), database, options).await {
            Ok(bridge) => {
                if let Some(old) = self.bridge.replace(bridge) {
                    old.terminate().await;
                }
                let b = self.bridge.as_ref().unwrap();
                *self.registered.cancel.lock() = Some((b.pid, b.key));
                *self.registered.database.lock() = database.to_string();
                // Session settings carried over to the new session.
                let mut replay = Vec::new();
                if let Some(Value::Str(level)) = self.vars.get("transaction_isolation") {
                    if let Some(words) = admin::isolation_words(level) {
                        if words != "READ COMMITTED" {
                            replay.push(format!(
                                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {words}"
                            ));
                        }
                    }
                }
                for sql in replay {
                    if let Err(n) = self.silent(&sql).await {
                        return Err(results::error_from_engine(&n));
                    }
                }
                Ok(())
            }
            Err(OpenRefused::NoDatabase) => Err(MyError::new(
                1049,
                "42000",
                format!("Unknown database '{database}'"),
            )),
            Err(OpenRefused::Loading) => Err(MyError::new(
                1105,
                "HY000",
                format!(
                    "database '{database}' is being imported and is not ready for connections yet"
                ),
            )),
            Err(OpenRefused::Io(e)) => Err(MyError::new(1105, "HY000", e.to_string())),
        }
    }

    async fn command_loop(&mut self) -> io::Result<()> {
        let mut stopping = self.shared.stopping.subscribe();
        loop {
            let registered = self.registered.clone();
            let packet = tokio::select! {
                p = self.io.read() => match p {
                    Ok(p) => p,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => return Err(e),
                },
                _ = registered.kill.notified() => return Ok(()),
                _ = stopping.wait_for(|s| *s) => return Ok(()),
            };
            let Some(&command) = packet.first() else {
                continue;
            };
            let body = &packet[1..];
            self.shared.questions.fetch_add(1, Ordering::Relaxed);
            match command {
                0x01 => return Ok(()),
                0x02 => {
                    let db = String::from_utf8_lossy(body).into_owned();
                    self.use_database(&db).await;
                }
                0x03 => {
                    *self.registered.query.lock() =
                        Some(String::from_utf8_lossy(&body[..body.len().min(256)]).into_owned());
                    self.com_query(body).await?;
                    *self.registered.query.lock() = None;
                }
                0x04 => {
                    // COM_FIELD_LIST (deprecated): no fields.
                    self.end_of_rows(false);
                }
                0x09 => {
                    let text = format!(
                        "Uptime: {}  Threads: {}  Questions: {}  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000",
                        self.shared.started.elapsed().as_secs(),
                        self.shared.active.load(Ordering::Relaxed),
                        self.shared.questions.load(Ordering::Relaxed)
                    );
                    self.io.packet(text.as_bytes());
                }
                0x0e | 0x0d => self.ok(0, 0, ""),
                0x0c => {
                    let id = Reader::new(body).u32().unwrap_or(0);
                    match self.kill(id, false) {
                        Ok(()) => self.ok(0, 0, ""),
                        Err(e) => self.err(&e),
                    }
                }
                0x11 => self.err(&MyError::unsupported(
                    "COM_CHANGE_USER (open a new connection)",
                )),
                0x16 => self.com_prepare(body).await?,
                0x17 => self.com_execute(body).await?,
                0x18 => {
                    // COM_STMT_SEND_LONG_DATA: no answer.
                    let mut r = Reader::new(body);
                    if let (Ok(id), Ok(param)) = (r.u32(), r.u16()) {
                        let data = r.rest().to_vec();
                        if let Some(p) = self.statements.get_mut(&id) {
                            p.long_data
                                .entry(param)
                                .or_default()
                                .extend_from_slice(&data);
                        }
                    }
                    continue;
                }
                0x19 => {
                    // COM_STMT_CLOSE: no answer.
                    let id = Reader::new(body).u32().unwrap_or(0);
                    if let Some(p) = self.statements.remove(&id) {
                        if let Some(name) = p.engine_name {
                            let b = self.bridge();
                            b.close(b'S', &name);
                            b.sync();
                            let _ = self.drain_to_ready().await;
                        }
                    }
                    continue;
                }
                0x1a => {
                    let id = Reader::new(body).u32().unwrap_or(0);
                    match self.statements.get_mut(&id) {
                        Some(p) => {
                            p.long_data.clear();
                            self.ok(0, 0, "");
                        }
                        None => self.err(&MyError::new(1243, "HY000", format!("Unknown prepared statement handler ({id}) given to mysqld_stmt_reset"))),
                    }
                }
                0x1b => {
                    // COM_SET_OPTION: multi-statements on (0) or off (1).
                    match Reader::new(body).u16().unwrap_or(0) {
                        0 => self.caps |= CLIENT_MULTI_STATEMENTS,
                        _ => self.caps &= !CLIENT_MULTI_STATEMENTS,
                    }
                    self.end_of_rows(false);
                }
                0x1c => self.err(&MyError::unsupported("cursors (COM_STMT_FETCH)")),
                0x1f => {
                    self.reset_connection().await;
                    self.ok(0, 0, "");
                }
                other => self.err(&MyError::new(
                    1047,
                    "08S01",
                    format!("Unknown command {other:#04x}"),
                )),
            }
            self.io.flush().await?;
        }
    }

    fn ok(&mut self, affected: u64, insert_id: u64, info: &str) {
        let status = self.status(false);
        let warnings = self.warnings.len() as u16;
        results::write_ok(
            &mut self.io,
            affected,
            insert_id,
            status,
            warnings,
            info,
            false,
        );
    }

    fn err(&mut self, error: &MyError) {
        results::write_err(&mut self.io, error);
    }

    /// The packet that ends a result set (or a column list).
    fn end_of_rows(&mut self, more: bool) {
        let status = self.status(more);
        let warnings = self.warnings.len() as u16;
        if self.deprecate_eof() {
            results::write_ok(&mut self.io, 0, 0, status, warnings, "", true);
        } else {
            results::write_eof(&mut self.io, status, warnings);
        }
    }

    async fn reset_connection(&mut self) {
        if self.bridge.as_ref().is_some_and(|b| b.in_block()) {
            let _ = self.silent("ROLLBACK").await;
        }
        self.drop_temp_tables().await;
        let names: Vec<String> = self
            .statements
            .drain()
            .filter_map(|(_, p)| p.engine_name)
            .collect();
        for name in names {
            let b = self.bridge();
            b.close(b'S', &name);
        }
        self.bridge().sync();
        let _ = self.drain_to_ready().await;
        let tls = self.io.stream.is_secure();
        let port = self.shared.config.tcp.as_ref().map(|t| t.1).unwrap_or(0);
        self.vars = admin::initial_variables(tls, port);
        self.user_vars.clear();
        self.warnings.clear();
        self.deferred_fks.clear();
        self.next_isolation = None;
        self.next_read_only = None;
        let _ = self.silent("RESET ALL").await;
    }

    async fn use_database(&mut self, db: &str) {
        if self.bridge.as_ref().is_some_and(|b| b.in_block()) {
            // MySQL keeps the transaction across USE; a session here belongs
            // to one database, so the switch would end it.
            self.err(&MyError::new(
                1105,
                "HY000",
                "USE inside a transaction is not supported: COMMIT or ROLLBACK first",
            ));
            return;
        }
        if self.shared.catalog.get(db).is_none() {
            self.err(&MyError::new(
                1049,
                "42000",
                format!("Unknown database '{db}'"),
            ));
            return;
        }
        if self.bridge.as_ref().is_some_and(|b| b.database_name == db) {
            self.ok(0, 0, "");
            return;
        }
        // Prepared statements belong to the old session.
        self.statements.clear();
        match self.open(db).await {
            Ok(()) => self.ok(0, 0, ""),
            Err(e) => self.err(&e),
        }
    }

    fn kill(&mut self, id: u32, query_only: bool) -> Result<(), MyError> {
        let target = super::registry().lock().get(&id).cloned();
        let Some(target) = target else {
            return Err(MyError::new(
                1094,
                "HY000",
                format!("Unknown thread id: {id}"),
            ));
        };
        if target.user != self.user && self.login.authenticated && !self.login.superuser {
            return Err(MyError::new(
                1095,
                "HY000",
                format!("You are not owner of thread {id}"),
            ));
        }
        if let Some((pid, key)) = *target.cancel.lock() {
            crate::core::cancel::cancel_request(pid, key);
        }
        if !query_only {
            target.kill.notify_one();
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Running statements on the engine
    // -----------------------------------------------------------------------

    /// Reads to ReadyForQuery, discarding.
    async fn drain_to_ready(&mut self) -> io::Result<()> {
        loop {
            if self.bridge().next().await?.tag == b'Z' {
                return Ok(());
            }
        }
    }

    /// Runs a statement whose answer is not the client's; its error, if any.
    async fn silent(&mut self, sql: &str) -> Result<String, PgNotice> {
        let b = self.bridge();
        b.query(sql);
        let mut error = None;
        let mut tag = String::new();
        loop {
            let frame = match b.next().await {
                Ok(f) => f,
                Err(e) => {
                    return Err(PgNotice {
                        code: "08006".into(),
                        message: e.to_string(),
                        ..Default::default()
                    });
                }
            };
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

    /// Before a write is translated: the tables' ON UPDATE CURRENT_TIMESTAMP
    /// columns, when any are recorded.
    async fn load_on_update(&mut self, tokens: &[Token]) {
        self.on_update.clear();
        let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
        if !matches!(head.as_str(), "UPDATE" | "INSERT" | "REPLACE") {
            return;
        }
        if !self.bridge().database.has_table(translate::ON_UPDATE_TABLE) {
            return;
        }
        let guard = self.bridge().in_block();
        if guard {
            let _ = self.silent("SAVEPOINT rdb_mysql_meta").await;
        }
        let read = self
            .fetch(&format!("SELECT tbl, col FROM {}", translate::ON_UPDATE_TABLE))
            .await;
        if guard {
            if read.is_err() {
                let _ = self.silent("ROLLBACK TO SAVEPOINT rdb_mysql_meta").await;
            }
            let _ = self.silent("RELEASE SAVEPOINT rdb_mysql_meta").await;
        }
        if let Ok((_, rows)) = read {
            for row in rows {
                if let (Some(Some(t)), Some(Some(c))) = (row.first(), row.get(1)) {
                    self.on_update.entry(t.clone()).or_default().push(c.clone());
                }
            }
        }
    }

    /// Runs a query whose rows are for this session (SHOW, SET's values),
    /// as text.
    async fn fetch(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<PgField>, Vec<Vec<Option<String>>>), PgNotice> {
        let b = self.bridge();
        b.query(sql);
        let mut error = None;
        let mut fields = Vec::new();
        let mut rows = Vec::new();
        loop {
            let frame = b.next().await.map_err(|e| PgNotice {
                code: "08006".into(),
                message: e.to_string(),
                ..Default::default()
            })?;
            match frame.tag {
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'T' => fields = parse_row_description(frame.body).unwrap_or_default(),
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
            None => Ok((fields, rows)),
        }
    }

    /// Sends one request and reads its answer, streaming any rows to the
    /// client (or capturing them, for an insert's ids).
    async fn exchange(
        &mut self,
        request: Request<'_>,
        rows_mode: Rows,
        capture: bool,
        more: bool,
    ) -> io::Result<Outcome> {
        let deprecate_eof = self.deprecate_eof();
        let schema = self.database_name().to_string();
        {
            let b = self.bridge.as_mut().unwrap();
            match &request {
                Request::Simple(sql) => b.query(sql),
                Request::Bound { statement, params } => {
                    let refs: Vec<(i16, Option<&[u8]>)> =
                        params.iter().map(|(f, v)| (*f, v.as_deref())).collect();
                    b.bind("", statement, &refs, &[0]);
                    b.describe(b'P', "");
                    b.execute("", 0);
                    b.sync();
                }
            }
        }
        let mut columns: Option<Vec<MyColumn>> = None;
        let mut count = 0u64;
        let mut first_id: Option<u64> = None;
        let mut error: Option<PgNotice> = None;
        let mut tag: Option<String> = None;
        let mut encode_error: Option<String> = None;
        loop {
            let b = self.bridge.as_mut().unwrap();
            let frame = b.next().await?;
            match frame.tag {
                b'T' => {
                    let fields = parse_row_description(frame.body)?;
                    let cols: Vec<MyColumn> = fields.iter().map(results::column_for).collect();
                    if !capture {
                        let at = self.io.begin();
                        put_lenenc_int(self.io.buf(), cols.len() as u64);
                        self.io.end(at);
                        for c in &cols {
                            results::write_column(&mut self.io, &schema, c);
                        }
                        if !deprecate_eof {
                            let status = self.status(more);
                            results::write_eof(&mut self.io, status, 0);
                        }
                    }
                    columns = Some(cols);
                }
                b'n' => {
                    // NoData: a portal without rows.
                }
                b'D' => {
                    let cells = data_row_cells(frame.body)?;
                    count += 1;
                    if capture {
                        if first_id.is_none() {
                            first_id = cells
                                .first()
                                .copied()
                                .flatten()
                                .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok());
                        }
                        continue;
                    }
                    let cols = columns.as_deref().unwrap_or(&[]);
                    match rows_mode {
                        Rows::Text => results::write_text_row(&mut self.io, cols, &cells),
                        Rows::Binary => {
                            if encode_error.is_none() {
                                if let Err(e) =
                                    results::write_binary_row(&mut self.io, cols, &cells)
                                {
                                    encode_error = Some(e);
                                }
                            }
                        }
                    }
                    if self.io.pending() > 64 * 1024 {
                        self.io.flush().await?;
                    }
                }
                b'C' => {
                    tag = Some(
                        String::from_utf8_lossy(
                            frame.body.strip_suffix(&[0]).unwrap_or(frame.body),
                        )
                        .into_owned(),
                    );
                }
                b'I' => tag = Some(String::new()),
                b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                b'N' => {
                    let n = PgNotice::parse(frame.body);
                    self.warnings.push(("Warning".into(), 1105, n.message));
                }
                b'Z' => break,
                _ => {}
            }
        }
        if let Some(e) = error {
            return Ok(Outcome::Failed(e));
        }
        if let Some(e) = encode_error {
            return Ok(Outcome::Failed(PgNotice {
                code: "XX000".into(),
                message: e,
                ..Default::default()
            }));
        }
        if columns.is_some() && !capture {
            return Ok(Outcome::Rows { count });
        }
        Ok(Outcome::Done {
            tag: tag.unwrap_or_default(),
            insert_id: first_id,
            returned: count,
        })
    }

    /// The statement that opens a transaction, with what `SET TRANSACTION`
    /// asked of the next one.
    fn begin_sql(&mut self, read_only: Option<bool>, isolation: Option<String>) -> String {
        let mut sql = String::from("BEGIN");
        if let Some(level) = isolation.or_else(|| self.next_isolation.take()) {
            sql.push_str(" ISOLATION LEVEL ");
            sql.push_str(&level);
        }
        match read_only.or_else(|| self.next_read_only.take()) {
            Some(true) => sql.push_str(" READ ONLY"),
            Some(false) => sql.push_str(" READ WRITE"),
            None => {}
        }
        sql
    }

    /// Runs a translated statement and answers the client. Whether it
    /// succeeded.
    async fn forward(
        &mut self,
        forward: Forward,
        more: bool,
        rows_mode: Rows,
        bound: Option<(&str, Vec<(i16, Option<Vec<u8>>)>)>,
    ) -> io::Result<bool> {
        let ddl = forward.kind == Kind::Ddl;
        // A failed block (which only an internal failure leaves) is ended
        // before anything else runs in it.
        if self.bridge().status == b'E' {
            log::warn!(
                "MySQL session {}: a failed transaction was found open; rolling it back",
                self.conn_id
            );
            let _ = self.silent("ROLLBACK").await;
        }
        if ddl && self.bridge().in_block() {
            if let Err(e) = self.silent("COMMIT").await {
                self.err(&results::error_from_engine(&e));
                return Ok(false);
            }
        }
        if !ddl && !self.autocommit() && !self.bridge().in_block() {
            let begin = self.begin_sql(None, None);
            if let Err(e) = self.silent(&begin).await {
                self.err(&results::error_from_engine(&e));
                return Ok(false);
            }
        }
        let guarded = !ddl && self.bridge().in_block();
        if guarded {
            if let Err(e) = self
                .silent(&format!("SAVEPOINT {STATEMENT_SAVEPOINT}"))
                .await
            {
                self.err(&results::error_from_engine(&e));
                return Ok(false);
            }
        }

        let capture = matches!(forward.kind, Kind::Insert { auto: Some(_) });
        let mut last: Option<Outcome> = None;
        let mut affected_total = 0u64;
        match bound {
            Some((statement, params)) => {
                let outcome = self
                    .exchange(
                        Request::Bound { statement, params },
                        rows_mode,
                        capture,
                        more,
                    )
                    .await?;
                last = Some(outcome);
            }
            None => {
                for sql in forward.statements.iter().filter(|s| !s.is_empty()) {
                    let outcome = self
                        .exchange(Request::Simple(sql), rows_mode, capture, more)
                        .await?;
                    let failed = matches!(outcome, Outcome::Failed(_));
                    if let Outcome::Done { tag, .. } = &outcome {
                        affected_total += tag_count(tag).unwrap_or(0);
                    }
                    last = Some(outcome);
                    if failed {
                        break;
                    }
                }
            }
        }
        let outcome = last.unwrap_or(Outcome::Done {
            tag: String::new(),
            insert_id: None,
            returned: 0,
        });

        match outcome {
            Outcome::Failed(notice) => {
                if guarded {
                    if notice.code == "40001" || notice.code == "40P01" {
                        let _ = self.silent("ROLLBACK").await;
                    } else {
                        let _ = self
                            .silent(&format!("ROLLBACK TO SAVEPOINT {STATEMENT_SAVEPOINT}"))
                            .await;
                        let _ = self
                            .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                            .await;
                    }
                }
                self.err(&results::error_from_engine(&notice));
                Ok(false)
            }
            Outcome::Rows { count } => {
                if guarded {
                    let _ = self
                        .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                        .await;
                }
                self.found_rows = count;
                if let Some(q) = &forward.found_rows {
                    if let Ok((_, rows)) = self.fetch(q).await {
                        self.found_rows = rows
                            .first()
                            .and_then(|r| r.first().cloned().flatten())
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(count);
                    }
                }
                self.row_count = -1;
                self.end_of_rows(more);
                Ok(true)
            }
            Outcome::Done {
                tag,
                insert_id,
                returned,
            } => {
                if guarded {
                    let _ = self
                        .silent(&format!("RELEASE SAVEPOINT {STATEMENT_SAVEPOINT}"))
                        .await;
                }
                if !forward.deferred.is_empty() {
                    self.deferred_fks.extend(forward.deferred.iter().cloned());
                }
                let affected = if capture {
                    returned
                } else if ddl {
                    0
                } else {
                    tag_count(&tag).unwrap_or(affected_total)
                };
                let mut reported_id = 0;
                if let Some(id) = insert_id_of(&forward.kind, insert_id) {
                    reported_id = id;
                    self.last_insert_id = id;
                }
                self.row_count = affected as i64;
                let info = match &forward.kind {
                    Kind::Write if tag.starts_with("UPDATE") => format!(
                        "Rows matched: {affected}  Changed: {affected}  Warnings: {}",
                        self.warnings.len()
                    ),
                    Kind::Insert { .. } if affected > 1 => format!(
                        "Records: {affected}  Duplicates: 0  Warnings: {}",
                        self.warnings.len()
                    ),
                    _ => String::new(),
                };
                let status = self.status(more);
                let warnings = self.warnings.len() as u16;
                results::write_ok(
                    &mut self.io,
                    affected,
                    reported_id,
                    status,
                    warnings,
                    &info,
                    false,
                );
                Ok(true)
            }
        }
    }

    // -----------------------------------------------------------------------
    // COM_QUERY
    // -----------------------------------------------------------------------

    async fn com_query(&mut self, sql: &[u8]) -> io::Result<()> {
        let tokens = match lex::lex(sql, self.mode()) {
            Ok(t) => t,
            Err(e) => {
                self.err(&MyError::syntax(format!(
                    "You have an error in your SQL syntax: {}",
                    e.0
                )));
                return Ok(());
            }
        };
        let statements = lex::split_statements(tokens);
        if statements.is_empty() {
            // Comments alone are an empty statement, answered OK (the mysql
            // client sends a dump's comments on their own); only a query
            // with nothing in it is an error.
            if sql.iter().all(|b| b.is_ascii_whitespace() || *b == b';') {
                self.err(&MyError::new(1065, "42000", "Query was empty"));
            } else {
                self.ok(0, 0, "");
            }
            return Ok(());
        }
        if statements.len() > 1 && self.caps & CLIENT_MULTI_STATEMENTS == 0 {
            self.err(&MyError::syntax(
                "You have an error in your SQL syntax: several statements were sent, and the client did not enable multi-statements",
            ));
            return Ok(());
        }
        let n = statements.len();
        for (k, tokens) in statements.into_iter().enumerate() {
            let more = k + 1 < n;
            if !self.statement(sql, tokens, more, Rows::Text).await? {
                break;
            }
            if self.io.pending() > 64 * 1024 {
                self.io.flush().await?;
            }
        }
        Ok(())
    }

    /// One statement of a query; whether it succeeded.
    async fn statement(
        &mut self,
        sql: &[u8],
        tokens: Vec<Token>,
        more: bool,
        rows_mode: Rows,
    ) -> io::Result<bool> {
        let temp = match self.bind_temp_tables(tokens) {
            Ok(t) => t,
            Err(e) => {
                self.err(&e);
                return Ok(false);
            }
        };
        if temp.noop {
            self.ok_more(more);
            return Ok(true);
        }
        let tokens = temp.tokens;
        let (action, filter) = admin::classify(&tokens);
        let keeps_warnings = matches!(action, Action::Show(Show::Warnings | Show::CountWarnings))
            || tokens
                .iter()
                .any(|t| matches!(&t.tok, Tok::SysVar(v) if v == "warning_count"));
        if !keeps_warnings {
            self.warnings.clear();
        }
        match action {
            Action::Data => {
                self.load_on_update(&tokens).await;
                let translated = translate::translate(sql, tokens, self, false);
                match translated {
                    Ok(mut forward) => {
                        // CREATE / DROP TEMPORARY TABLE do not commit what
                        // is open, in MySQL.
                        if temp.created.is_some() || !temp.dropped.is_empty() {
                            forward.kind = Kind::Write;
                        }
                        let ok = self.forward(forward, more, rows_mode, None).await?;
                        if ok {
                            if let Some((name, table)) = temp.created {
                                self.temp_tables.insert(name, table);
                            }
                            for name in temp.dropped {
                                self.temp_tables.remove(&name);
                            }
                        }
                        Ok(ok)
                    }
                    Err(e) => {
                        self.err(&e);
                        Ok(false)
                    }
                }
            }
            Action::Ok => {
                self.ok_more(more);
                Ok(true)
            }
            Action::Error(e) => {
                self.err(&e);
                Ok(false)
            }
            Action::Use(db) => {
                self.use_database(&db).await;
                Ok(true)
            }
            Action::Begin {
                read_only,
                isolation,
            } => {
                if self.bridge().in_block() {
                    // START TRANSACTION commits an open one, in MySQL.
                    if let Err(e) = self.silent("COMMIT").await {
                        self.err(&results::error_from_engine(&e));
                        return Ok(false);
                    }
                }
                let sql = self.begin_sql(read_only, isolation);
                self.control(&sql, more).await
            }
            Action::Commit { chain } => {
                if !self.bridge().in_block() {
                    self.ok_more(more);
                    return Ok(true);
                }
                match self.silent("COMMIT").await {
                    Ok(tag) if tag == "ROLLBACK" => {
                        self.err(&MyError::new(
                            1180,
                            "HY000",
                            "Got error during COMMIT: the transaction was rolled back",
                        ));
                        Ok(false)
                    }
                    Ok(_) => {
                        if chain {
                            let sql = self.begin_sql(None, None);
                            return self.control(&sql, more).await;
                        }
                        self.ok_more(more);
                        Ok(true)
                    }
                    Err(e) => {
                        self.err(&results::error_from_engine(&e));
                        Ok(false)
                    }
                }
            }
            Action::Rollback { chain } => {
                if !self.bridge().in_block() {
                    self.ok_more(more);
                    return Ok(true);
                }
                if let Err(e) = self.silent("ROLLBACK").await {
                    self.err(&results::error_from_engine(&e));
                    return Ok(false);
                }
                if chain {
                    let sql = self.begin_sql(None, None);
                    return self.control(&sql, more).await;
                }
                self.ok_more(more);
                Ok(true)
            }
            Action::Savepoint(sql) => self.control(&sql, more).await,
            Action::Set(assignments) => self.set(assignments, more).await,
            Action::SetTransaction {
                session,
                isolation,
                read_only,
            } => {
                if session {
                    let mut parts = Vec::new();
                    if let Some(level) = &isolation {
                        parts.push(format!("ISOLATION LEVEL {level}"));
                        let dashed = level.replace(' ', "-");
                        self.vars
                            .insert("transaction_isolation".into(), Value::Str(dashed.clone()));
                        self.vars.insert("tx_isolation".into(), Value::Str(dashed));
                    }
                    if let Some(ro) = read_only {
                        parts.push(if ro {
                            "READ ONLY".into()
                        } else {
                            "READ WRITE".into()
                        });
                        self.vars
                            .insert("transaction_read_only".into(), Value::Int(ro as i64));
                        self.vars
                            .insert("tx_read_only".into(), Value::Int(ro as i64));
                    }
                    if !parts.is_empty() {
                        let sql = format!(
                            "SET SESSION CHARACTERISTICS AS TRANSACTION {}",
                            parts.join(" ")
                        );
                        if let Err(e) = self.silent(&sql).await {
                            self.err(&results::error_from_engine(&e));
                            return Ok(false);
                        }
                    }
                } else {
                    if self.bridge().in_block() {
                        self.err(&MyError::new(1568, "25001", "Transaction characteristics can't be changed while a transaction is in progress"));
                        return Ok(false);
                    }
                    if isolation.is_some() {
                        self.next_isolation = isolation;
                    }
                    if read_only.is_some() {
                        self.next_read_only = read_only;
                    }
                }
                self.ok_more(more);
                Ok(true)
            }
            Action::Show(show) => self.show(show, filter, more, rows_mode).await,
            Action::Kill { id, query_only } => match self.kill(id, query_only) {
                Ok(()) => {
                    self.ok_more(more);
                    Ok(true)
                }
                Err(e) => {
                    self.err(&e);
                    Ok(false)
                }
            },
        }
    }

    fn ok_more(&mut self, more: bool) {
        let status = self.status(more);
        let warnings = self.warnings.len() as u16;
        results::write_ok(&mut self.io, 0, 0, status, warnings, "", false);
    }

    /// Transaction control, sent as is.
    async fn control(&mut self, sql: &str, more: bool) -> io::Result<bool> {
        match self.silent(sql).await {
            Ok(_) => {
                self.ok_more(more);
                Ok(true)
            }
            Err(e) => {
                self.err(&results::error_from_engine(&e));
                Ok(false)
            }
        }
    }

    // -----------------------------------------------------------------------
    // SET
    // -----------------------------------------------------------------------

    /// A SET's value: simple forms here, anything else by the engine.
    async fn evaluate(&mut self, tokens: &[Token]) -> Result<Option<Value>, MyError> {
        // `None` is DEFAULT.
        match tokens {
            [t] if t.is_word("DEFAULT") => return Ok(None),
            [t] if t.is_word("NULL") => return Ok(Some(Value::Null)),
            [t] if t.is_word("ON") || t.is_word("TRUE") => return Ok(Some(Value::Int(1))),
            [t] if t.is_word("OFF") || t.is_word("FALSE") => return Ok(Some(Value::Int(0))),
            [t] => match &t.tok {
                Tok::Num(n) => {
                    return Ok(Some(
                        n.parse::<i64>()
                            .map(Value::Int)
                            .unwrap_or_else(|_| Value::Str(n.clone())),
                    ));
                }
                Tok::Str(s) => {
                    return Ok(Some(Value::Str(String::from_utf8_lossy(s).into_owned())));
                }
                Tok::Word(w) | Tok::Quoted(w) => return Ok(Some(Value::Str(w.clone()))),
                Tok::SysVar(name) => {
                    return self.system_variable(name).map(Some).ok_or_else(|| {
                        MyError::new(1193, "HY000", format!("Unknown system variable '{name}'"))
                    });
                }
                Tok::UserVar(name) => return Ok(Some(self.user_variable(name))),
                _ => {}
            },
            [m, t] if m.is_op("-") => {
                if let Tok::Num(n) = &t.tok {
                    if let Ok(v) = n.parse::<i64>() {
                        return Ok(Some(Value::Int(-v)));
                    }
                }
            }
            _ => {}
        }
        // `SELECT <expr>` through the translator and the engine.
        let mut select = vec![Token::word("SELECT")];
        select.extend(tokens.iter().cloned());
        select.push(Token::word("AS"));
        select.push(Token::raw("v"));
        let forward = translate::translate(b"", select, self, false)?;
        let sql = forward.statements.first().cloned().unwrap_or_default();
        match self.fetch(&sql).await {
            Ok((fields, rows)) => {
                let value = rows.first().and_then(|r| r.first().cloned()).flatten();
                let oid = fields.first().map(|f| f.type_oid).unwrap_or(25);
                Ok(Some(match value {
                    None => Value::Null,
                    Some(v) if matches!(oid, 20 | 21 | 23) => {
                        v.parse().map(Value::Int).unwrap_or(Value::Str(v))
                    }
                    Some(v) if oid == 16 => Value::Int((v == "t") as i64),
                    Some(v) => Value::Str(v),
                }))
            }
            Err(e) => Err(results::error_from_engine(&e)),
        }
    }

    async fn set(&mut self, assignments: Vec<Assignment>, more: bool) -> io::Result<bool> {
        for a in assignments {
            if let Err(e) = self.assign(a).await {
                self.err(&e);
                return Ok(false);
            }
        }
        self.ok_more(more);
        Ok(true)
    }

    async fn assign(&mut self, a: Assignment) -> Result<(), MyError> {
        let tls = self.io.stream.is_secure();
        let port = self.shared.config.tcp.as_ref().map(|t| t.1).unwrap_or(0);
        match a.target {
            Target::Names(cs) => {
                check_charset(&cs)?;
                for v in [
                    "character_set_client",
                    "character_set_connection",
                    "character_set_results",
                ] {
                    self.vars.insert(v.into(), Value::Str(cs.clone()));
                }
                Ok(())
            }
            Target::User(name) => {
                let value = self.evaluate(&a.value).await?.unwrap_or(Value::Null);
                self.user_vars.insert(name, value);
                Ok(())
            }
            Target::System(name) => {
                let initial = admin::initial_variables(tls, port);
                let value = match self.evaluate(&a.value).await? {
                    Some(v) => v,
                    None => initial.get(&name).cloned().unwrap_or(Value::Null),
                };
                let engine_error = |e: PgNotice| results::error_from_engine(&e);
                match name.as_str() {
                    "autocommit" => {
                        let on =
                            admin::as_switch(&value).ok_or_else(|| wrong_value(&name, &value))?;
                        if on == 1 && !self.autocommit() && self.bridge().in_block() {
                            self.silent("COMMIT").await.map_err(engine_error)?;
                        }
                        self.vars.insert(name, Value::Int(on));
                    }
                    "foreign_key_checks" => {
                        let on =
                            admin::as_switch(&value).ok_or_else(|| wrong_value(&name, &value))?;
                        self.vars.insert(name, Value::Int(on));
                        if on == 1 {
                            let pending = std::mem::take(&mut self.deferred_fks);
                            for sql in pending {
                                self.silent(&sql).await.map_err(engine_error)?;
                            }
                        }
                    }
                    "unique_checks"
                    | "sql_notes"
                    | "sql_warnings"
                    | "sql_safe_updates"
                    | "sql_auto_is_null"
                    | "sql_quote_show_create" => {
                        let on =
                            admin::as_switch(&value).ok_or_else(|| wrong_value(&name, &value))?;
                        self.vars.insert(name, Value::Int(on));
                    }
                    "character_set_client"
                    | "character_set_connection"
                    | "character_set_results"
                    | "collation_connection" => {
                        if let Value::Str(cs) = &value {
                            check_charset(cs.split('_').next().unwrap_or(cs))?;
                        }
                        self.vars.insert(name, value);
                    }
                    "time_zone" => {
                        let zone = value.text().unwrap_or_else(|| "SYSTEM".into());
                        let sql = if zone.eq_ignore_ascii_case("SYSTEM") {
                            "SET TIME ZONE 'UTC'".to_string()
                        } else if let Some(offset) = utc_offset(&zone) {
                            format!("SET TIME ZONE INTERVAL '{offset}' HOUR TO MINUTE")
                        } else {
                            format!("SET TIME ZONE '{}'", zone.replace('\'', "''"))
                        };
                        self.silent(&sql).await.map_err(|e| {
                            MyError::new(
                                1298,
                                "HY000",
                                format!("Unknown or incorrect time zone: '{zone}' ({})", e.message),
                            )
                        })?;
                        self.vars.insert(name, Value::Str(zone));
                    }
                    "transaction_isolation" | "tx_isolation" => {
                        let text = value.text().unwrap_or_default();
                        let words = admin::isolation_words(&text)
                            .ok_or_else(|| wrong_value(&name, &value))?;
                        self.silent(&format!(
                            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {words}"
                        ))
                        .await
                        .map_err(engine_error)?;
                        let dashed = words.replace(' ', "-");
                        self.vars
                            .insert("transaction_isolation".into(), Value::Str(dashed.clone()));
                        self.vars.insert("tx_isolation".into(), Value::Str(dashed));
                    }
                    "transaction_read_only" | "tx_read_only" => {
                        let on =
                            admin::as_switch(&value).ok_or_else(|| wrong_value(&name, &value))?;
                        let words = if on == 1 { "READ ONLY" } else { "READ WRITE" };
                        self.silent(&format!(
                            "SET SESSION CHARACTERISTICS AS TRANSACTION {words}"
                        ))
                        .await
                        .map_err(engine_error)?;
                        self.vars
                            .insert("transaction_read_only".into(), Value::Int(on));
                        self.vars.insert("tx_read_only".into(), Value::Int(on));
                    }
                    "max_execution_time" => {
                        let ms = value
                            .text()
                            .and_then(|t| t.parse::<u64>().ok())
                            .ok_or_else(|| wrong_value(&name, &value))?;
                        self.silent(&format!("SET statement_timeout = {ms}"))
                            .await
                            .map_err(engine_error)?;
                        self.vars.insert(name, Value::Int(ms as i64));
                    }
                    "innodb_lock_wait_timeout" => {
                        let s = value
                            .text()
                            .and_then(|t| t.parse::<u64>().ok())
                            .ok_or_else(|| wrong_value(&name, &value))?;
                        self.silent(&format!("SET lock_timeout = {}", s * 1000))
                            .await
                            .map_err(engine_error)?;
                        self.vars.insert(name, Value::Int(s as i64));
                    }
                    "sql_select_limit" => {
                        let text = value.text().unwrap_or_default();
                        if text != "18446744073709551615" && !text.is_empty() {
                            return Err(MyError::unsupported(
                                "sql_select_limit (add LIMIT to the query)",
                            ));
                        }
                        self.vars.insert(name, value);
                    }
                    _ => {
                        // Stored, read back by SELECT @@name; no other effect.
                        self.vars.insert(name, value);
                    }
                }
                Ok(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // SHOW
    // -----------------------------------------------------------------------

    /// Sends locally built rows, filtered by a SHOW's LIKE or WHERE.
    async fn send_rows(
        &mut self,
        columns: Vec<MyColumn>,
        mut rows: Vec<Vec<Option<String>>>,
        filter: Filter,
        more: bool,
        rows_mode: Rows,
    ) -> io::Result<bool> {
        if let Some(pattern) = &filter.like {
            rows.retain(|r| {
                r.first()
                    .cloned()
                    .flatten()
                    .is_some_and(|v| admin::like_matches(pattern, &v))
            });
        }
        if let Some(condition) = filter.condition {
            match self.filter_rows(&columns, rows, condition).await {
                Ok(kept) => rows = kept,
                Err(e) => {
                    self.err(&e);
                    return Ok(false);
                }
            }
        }
        let schema = self.database_name().to_string();
        let at = self.io.begin();
        put_lenenc_int(self.io.buf(), columns.len() as u64);
        self.io.end(at);
        for c in &columns {
            results::write_column(&mut self.io, &schema, c);
        }
        if !self.deprecate_eof() {
            let status = self.status(more);
            results::write_eof(&mut self.io, status, 0);
        }
        for row in &rows {
            let cells: Vec<Option<&[u8]>> = row
                .iter()
                .map(|c| c.as_deref().map(str::as_bytes))
                .collect();
            match rows_mode {
                Rows::Text => results::write_text_row(&mut self.io, &columns, &cells),
                Rows::Binary => {
                    let _ = results::write_binary_row(&mut self.io, &columns, &cells);
                }
            }
        }
        self.found_rows = rows.len() as u64;
        self.end_of_rows(more);
        Ok(true)
    }

    /// `SHOW … WHERE cond`: the rows run through the engine as a VALUES list.
    async fn filter_rows(
        &mut self,
        columns: &[MyColumn],
        rows: Vec<Vec<Option<String>>>,
        condition: Vec<Token>,
    ) -> Result<Vec<Vec<Option<String>>>, MyError> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let mut where_tokens = Vec::new();
        for t in condition {
            let replaced = match t.ident() {
                Some(id) => names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(id))
                    .map(|k| Token::raw(format!("c{k}"))),
                None => None,
            };
            where_tokens.push(replaced.unwrap_or(t));
        }
        let condition = {
            let mut cx = translate::Cx::new(&*self, false);
            let rewritten = translate::rewrite_expr(where_tokens, &mut cx)?;
            translate::render(&rewritten)?
        };
        let values: Vec<String> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut cells = vec![i.to_string()];
                cells.extend(r.iter().map(|c| match c {
                    Some(v) => translate::string_sql(v.as_bytes()),
                    None => "NULL::text".into(),
                }));
                format!("({})", cells.join(", "))
            })
            .collect();
        let column_list: Vec<String> = (0..names.len()).map(|k| format!("c{k}")).collect();
        let sql = format!(
            "SELECT k FROM (VALUES {}) AS s(k, {}) WHERE {condition}",
            values.join(", "),
            column_list.join(", ")
        );
        let (_, kept) = self
            .fetch(&sql)
            .await
            .map_err(|e| results::error_from_engine(&e))?;
        let keep: std::collections::HashSet<usize> = kept
            .iter()
            .filter_map(|r| r.first().cloned().flatten())
            .filter_map(|v| v.parse().ok())
            .collect();
        Ok(rows
            .into_iter()
            .enumerate()
            .filter(|(i, _)| keep.contains(i))
            .map(|(_, r)| r)
            .collect())
    }

    async fn show(
        &mut self,
        show: Show,
        filter: Filter,
        more: bool,
        rows_mode: Rows,
    ) -> io::Result<bool> {
        let t = MyColumn::text;
        let db = self.database_name().to_string();
        match show {
            Show::Databases => {
                let rows = self
                    .shared
                    .catalog
                    .names()
                    .into_iter()
                    .map(|n| vec![Some(n)])
                    .collect();
                self.send_rows(vec![t("Database")], rows, filter, more, rows_mode)
                    .await
            }
            Show::Tables { full, database } => {
                let target = database.unwrap_or_else(|| db.clone());
                let listing = if target == db {
                    self.fetch("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'rdb_roles' AND substring(table_name, 1, 2) <> '__' ORDER BY table_name").await
                } else {
                    match self.other_database_fetch(&target, "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'rdb_roles' AND substring(table_name, 1, 2) <> '__' ORDER BY table_name").await {
                        Ok(r) => r,
                        Err(e) => {
                            self.err(&e);
                            return Ok(false);
                        }
                    }
                };
                let rows = match listing {
                    Ok((_, rows)) => rows,
                    Err(e) => {
                        self.err(&results::error_from_engine(&e));
                        return Ok(false);
                    }
                };
                let mut columns = vec![t(&format!("Tables_in_{target}"))];
                if full {
                    columns.push(t("Table_type"));
                }
                let rows = rows
                    .into_iter()
                    .map(|r| {
                        let mut out = vec![r.first().cloned().flatten()];
                        if full {
                            out.push(r.get(1).cloned().flatten());
                        }
                        out
                    })
                    .collect();
                self.send_rows(columns, rows, filter, more, rows_mode).await
            }
            Show::Columns { full, table } => {
                let described = match self.describe_table(&table).await {
                    Ok(d) => d,
                    Err(e) => {
                        self.err(&e);
                        return Ok(false);
                    }
                };
                let mut columns = vec![t("Field"), t("Type")];
                if full {
                    columns.push(t("Collation"));
                }
                columns.extend([t("Null"), t("Key"), t("Default"), t("Extra")]);
                if full {
                    columns.extend([t("Privileges"), t("Comment")]);
                }
                let rows = described
                    .into_iter()
                    .map(|c| {
                        let mut row = vec![Some(c.name), Some(c.mysql_type.clone())];
                        if full {
                            row.push(c.text.then(|| "utf8mb4_bin".to_string()));
                        }
                        row.extend([
                            Some(if c.nullable { "YES" } else { "NO" }.to_string()),
                            Some(c.key),
                            c.default,
                            Some(c.extra),
                        ]);
                        if full {
                            row.extend([
                                Some("select,insert,update,references".to_string()),
                                Some(String::new()),
                            ]);
                        }
                        row
                    })
                    .collect();
                self.send_rows(columns, rows, filter, more, rows_mode).await
            }
            Show::Index { table } => {
                let rows = match self.index_rows(&table) {
                    Ok(r) => r,
                    Err(e) => {
                        self.err(&e);
                        return Ok(false);
                    }
                };
                let columns = vec![
                    t("Table"),
                    MyColumn::int("Non_unique"),
                    t("Key_name"),
                    MyColumn::int("Seq_in_index"),
                    t("Column_name"),
                    t("Collation"),
                    MyColumn::int("Cardinality"),
                    MyColumn::int("Sub_part"),
                    t("Packed"),
                    t("Null"),
                    t("Index_type"),
                    t("Comment"),
                    t("Index_comment"),
                    t("Visible"),
                    t("Expression"),
                ];
                self.send_rows(columns, rows, filter, more, rows_mode).await
            }
            Show::CreateTable { table } => {
                let text = match self.create_table_text(&table).await {
                    Ok(text) => text,
                    Err(e) => {
                        self.err(&e);
                        return Ok(false);
                    }
                };
                self.send_rows(
                    vec![t("Table"), t("Create Table")],
                    vec![vec![Some(table), Some(text)]],
                    Filter::default(),
                    more,
                    rows_mode,
                )
                .await
            }
            Show::CreateDatabase { name } => {
                if self.shared.catalog.get(&name).is_none() {
                    self.err(&MyError::new(
                        1049,
                        "42000",
                        format!("Unknown database '{name}'"),
                    ));
                    return Ok(false);
                }
                let text = format!(
                    "CREATE DATABASE `{name}` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin */"
                );
                self.send_rows(
                    vec![t("Database"), t("Create Database")],
                    vec![vec![Some(name), Some(text)]],
                    Filter::default(),
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Variables => {
                let mut all: BTreeMap<String, Option<String>> = self
                    .vars
                    .iter()
                    .map(|(k, v)| (k.clone(), v.text()))
                    .collect();
                all.insert(
                    "warning_count".into(),
                    Some(self.warnings.len().to_string()),
                );
                let rows = all.into_iter().map(|(k, v)| vec![Some(k), v]).collect();
                self.send_rows(
                    vec![t("Variable_name"), t("Value")],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Status => {
                let rows = vec![
                    vec![
                        Some("Uptime".into()),
                        Some(self.shared.started.elapsed().as_secs().to_string()),
                    ],
                    vec![
                        Some("Threads_connected".into()),
                        Some(self.shared.active.load(Ordering::Relaxed).to_string()),
                    ],
                    vec![
                        Some("Questions".into()),
                        Some(self.shared.questions.load(Ordering::Relaxed).to_string()),
                    ],
                    vec![
                        Some("Ssl_cipher".into()),
                        Some(
                            if matches!(self.io.stream, MyStream::Tls(_))
                                || self.io.stream.is_tls_unix()
                            {
                                "TLS".into()
                            } else {
                                String::new()
                            },
                        ),
                    ],
                ];
                self.send_rows(
                    vec![t("Variable_name"), t("Value")],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Warnings => {
                let rows = self
                    .warnings
                    .iter()
                    .map(|(level, code, message)| {
                        vec![
                            Some(level.clone()),
                            Some(code.to_string()),
                            Some(message.clone()),
                        ]
                    })
                    .collect();
                self.send_rows(
                    vec![t("Level"), MyColumn::int("Code"), t("Message")],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::CountWarnings => {
                let n = self.warnings.len().to_string();
                self.send_rows(
                    vec![MyColumn::int("@@session.warning_count")],
                    vec![vec![Some(n)]],
                    Filter::default(),
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Engines => {
                let rows = vec![vec![
                    Some("InnoDB".into()),
                    Some("DEFAULT".into()),
                    Some("RasterizedDB storage, transactional".into()),
                    Some("YES".into()),
                    Some("NO".into()),
                    Some("YES".into()),
                ]];
                self.send_rows(
                    vec![
                        t("Engine"),
                        t("Support"),
                        t("Comment"),
                        t("Transactions"),
                        t("XA"),
                        t("Savepoints"),
                    ],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Charset => {
                let rows = vec![
                    vec![
                        Some("binary".into()),
                        Some("Binary pseudo charset".into()),
                        Some("binary".into()),
                        Some("1".into()),
                    ],
                    vec![
                        Some("utf8mb4".into()),
                        Some("UTF-8 Unicode".into()),
                        Some("utf8mb4_bin".into()),
                        Some("4".into()),
                    ],
                ];
                self.send_rows(
                    vec![
                        t("Charset"),
                        t("Description"),
                        t("Default collation"),
                        MyColumn::int("Maxlen"),
                    ],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Collation => {
                let rows = vec![
                    vec![
                        Some("binary".into()),
                        Some("binary".into()),
                        Some("63".into()),
                        Some("Yes".into()),
                        Some("Yes".into()),
                        Some("1".into()),
                        Some("NO PAD".into()),
                    ],
                    vec![
                        Some("utf8mb4_bin".into()),
                        Some("utf8mb4".into()),
                        Some("46".into()),
                        Some("Yes".into()),
                        Some("Yes".into()),
                        Some("1".into()),
                        Some("PAD SPACE".into()),
                    ],
                ];
                self.send_rows(
                    vec![
                        t("Collation"),
                        t("Charset"),
                        MyColumn::int("Id"),
                        t("Default"),
                        t("Compiled"),
                        MyColumn::int("Sortlen"),
                        t("Pad_attribute"),
                    ],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Processlist => {
                let mut rows: Vec<Vec<Option<String>>> = super::registry()
                    .lock()
                    .iter()
                    .map(|(id, r)| {
                        let query = r.query.lock().clone();
                        vec![
                            Some(id.to_string()),
                            Some(r.user.clone()),
                            Some(r.host.clone()),
                            Some(r.database.lock().clone()),
                            Some(if query.is_some() { "Query" } else { "Sleep" }.into()),
                            Some(r.since.elapsed().as_secs().to_string()),
                            Some(String::new()),
                            query,
                        ]
                    })
                    .collect();
                rows.sort_by_key(|r| r[0].as_deref().and_then(|v| v.parse::<u32>().ok()));
                self.send_rows(
                    vec![
                        MyColumn::int("Id"),
                        t("User"),
                        t("Host"),
                        t("db"),
                        t("Command"),
                        MyColumn::int("Time"),
                        t("State"),
                        t("Info"),
                    ],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Grants => {
                let grant = if !self.login.authenticated || self.login.superuser {
                    format!(
                        "GRANT ALL PRIVILEGES ON *.* TO `{}`@`%` WITH GRANT OPTION",
                        self.user
                    )
                } else {
                    format!("GRANT USAGE ON *.* TO `{}`@`%`", self.user)
                };
                let name = format!("Grants for {}@%", self.user);
                self.send_rows(
                    vec![t(&name)],
                    vec![vec![Some(grant)]],
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::TableStatus => {
                let rows = match self.fetch("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'rdb_roles' AND substring(table_name, 1, 2) <> '__' ORDER BY table_name").await {
                    Ok((_, rows)) => rows,
                    Err(e) => {
                        self.err(&results::error_from_engine(&e));
                        return Ok(false);
                    }
                };
                let database = self.database().clone();
                let rows = rows
                    .into_iter()
                    .map(|r| {
                        let name = r.first().cloned().flatten().unwrap_or_default();
                        let view = r.get(1).cloned().flatten().is_some_and(|k| k == "VIEW");
                        let auto = translate::auto_increment_column(&database, &name)
                            .map(|_| "1".to_string());
                        vec![
                            Some(name),
                            (!view).then(|| "InnoDB".into()),
                            (!view).then(|| "10".into()),
                            (!view).then(|| "Dynamic".into()),
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            auto,
                            None,
                            None,
                            None,
                            (!view).then(|| "utf8mb4_bin".into()),
                            None,
                            Some(String::new()),
                            Some(if view { "VIEW".into() } else { String::new() }),
                        ]
                    })
                    .collect();
                let i = MyColumn::int;
                self.send_rows(
                    vec![
                        t("Name"),
                        t("Engine"),
                        i("Version"),
                        t("Row_format"),
                        i("Rows"),
                        i("Avg_row_length"),
                        i("Data_length"),
                        i("Max_data_length"),
                        i("Index_length"),
                        i("Data_free"),
                        i("Auto_increment"),
                        t("Create_time"),
                        t("Update_time"),
                        t("Check_time"),
                        t("Collation"),
                        i("Checksum"),
                        t("Create_options"),
                        t("Comment"),
                    ],
                    rows,
                    filter,
                    more,
                    rows_mode,
                )
                .await
            }
            Show::Empty(names) => {
                let columns = names.into_iter().map(t).collect();
                self.send_rows(columns, Vec::new(), Filter::default(), more, rows_mode)
                    .await
            }
        }
    }

    async fn other_database_fetch(
        &mut self,
        database: &str,
        sql: &str,
    ) -> Result<Result<(Vec<PgField>, Vec<Vec<Option<String>>>), PgNotice>, MyError> {
        let options = BridgeOptions {
            login: self.login.clone(),
            limits: self.shared.config.limits,
            watch: None,
            parameters: HashMap::new(),
        };
        let mut bridge = match PgBridge::open(self.shared.catalog.clone(), database, options).await
        {
            Ok(b) => b,
            Err(OpenRefused::NoDatabase) => {
                return Err(MyError::new(
                    1049,
                    "42000",
                    format!("Unknown database '{database}'"),
                ));
            }
            Err(_) => {
                return Err(MyError::new(
                    1105,
                    "HY000",
                    format!("database '{database}' cannot be read now"),
                ));
            }
        };
        std::mem::swap(self.bridge.as_mut().unwrap(), &mut bridge);
        let result = self.fetch(sql).await;
        std::mem::swap(self.bridge.as_mut().unwrap(), &mut bridge);
        bridge.terminate().await;
        Ok(result)
    }

    /// A table's columns as SHOW COLUMNS describes them.
    async fn describe_table(&mut self, table: &str) -> Result<Vec<Described>, MyError> {
        let name = table.to_lowercase();
        let database = self.database().clone();
        let Some(view) = database.schema_view(&name) else {
            return Err(MyError::new(
                1146,
                "42S02",
                format!("Table '{}.{}' doesn't exist", self.database_name(), table),
            ));
        };
        let schema = view.schema().clone();
        let sql = format!(
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable \
             FROM information_schema.columns WHERE table_name = {} ORDER BY ordinal_position",
            translate::string_sql(name.as_bytes())
        );
        let (_, rows) = self
            .fetch(&sql)
            .await
            .map_err(|e| results::error_from_engine(&e))?;
        let primary: Vec<String> = schema.primary_key.iter().cloned().collect();
        let unique: Vec<String> = schema
            .unique_keys
            .iter()
            .filter(|k| !k.is_deleted && k.columns.len() == 1)
            .map(|k| k.columns[0].clone())
            .chain(
                schema
                    .fields
                    .iter()
                    .filter(|f| f.is_unique && !f.is_deleted)
                    .map(|f| f.name.clone()),
            )
            .collect();
        let indexed: Vec<String> = schema
            .secondary_indexes
            .iter()
            .filter(|i| !i.is_deleted)
            .filter_map(|i| i.columns.first())
            .filter_map(|w| {
                schema
                    .fields
                    .iter()
                    .find(|f| f.write_order as u32 == *w && !f.is_deleted)
            })
            .map(|f| f.name.clone())
            .chain(
                schema
                    .unique_keys
                    .iter()
                    .filter(|k| !k.is_deleted && k.columns.len() > 1)
                    .map(|k| k.columns[0].clone()),
            )
            .collect();
        let mut out = Vec::new();
        for r in rows {
            let get = |k: usize| r.get(k).cloned().flatten();
            let column = get(0).unwrap_or_default();
            let data_type = get(1).unwrap_or_default();
            let (mysql_type, text) = mysql_type_name(&data_type, get(2), get(3), get(4));
            let default_expr = schema
                .defaults
                .iter()
                .filter(|d| !d.is_deleted && d.column == column)
                .map(|d| d.expression.clone())
                .next_back();
            let auto = default_expr
                .as_deref()
                .is_some_and(|d| d.trim_start().to_ascii_lowercase().starts_with("nextval("));
            let (default, extra) = match default_expr {
                _ if auto => (None, "auto_increment".to_string()),
                Some(d) => {
                    let lower = d.to_ascii_lowercase();
                    if lower.starts_with("now()") || lower.starts_with("current_timestamp") {
                        (
                            Some("CURRENT_TIMESTAMP".to_string()),
                            "DEFAULT_GENERATED".to_string(),
                        )
                    } else {
                        let stripped =
                            crate::core::rql::column_default::strip_redundant_cast(&d).to_string();
                        let unquoted = stripped
                            .strip_prefix('\'')
                            .and_then(|s| s.strip_suffix('\''))
                            .map(|s| s.replace("''", "'"))
                            .unwrap_or(stripped);
                        (Some(unquoted), String::new())
                    }
                }
                None => (None, String::new()),
            };
            let key = if primary.contains(&column) {
                "PRI"
            } else if unique.contains(&column) {
                "UNI"
            } else if indexed.contains(&column) {
                "MUL"
            } else {
                ""
            };
            out.push(Described {
                nullable: get(5).as_deref() == Some("YES") && !primary.contains(&column),
                name: column,
                mysql_type,
                text,
                key: key.to_string(),
                default,
                extra,
                auto,
            });
        }
        Ok(out)
    }

    fn index_rows(&mut self, table: &str) -> Result<Vec<Vec<Option<String>>>, MyError> {
        let name = table.to_lowercase();
        let database = self.database().clone();
        let Some(view) = database.schema_view(&name) else {
            return Err(MyError::new(
                1146,
                "42S02",
                format!("Table '{}.{}' doesn't exist", self.database_name(), table),
            ));
        };
        let schema = view.schema();
        let mut rows = Vec::new();
        let mut push = |key: &str, non_unique: bool, columns: &[String]| {
            for (seq, column) in columns.iter().enumerate() {
                rows.push(vec![
                    Some(name.clone()),
                    Some((non_unique as u8).to_string()),
                    Some(key.to_string()),
                    Some((seq + 1).to_string()),
                    Some(column.clone()),
                    Some("A".into()),
                    None,
                    None,
                    None,
                    Some(String::new()),
                    Some("BTREE".into()),
                    Some(String::new()),
                    Some(String::new()),
                    Some("YES".into()),
                    None,
                ]);
            }
        };
        if let Some(pk) = &schema.primary_key {
            push("PRIMARY", false, std::slice::from_ref(pk));
        }
        for key in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
            // The primary key's own constraint is the PRIMARY row already.
            if schema
                .primary_key
                .as_ref()
                .is_some_and(|pk| key.columns.len() == 1 && &key.columns[0] == pk)
            {
                continue;
            }
            push(
                translate::plain_index_name(&name, &key.name),
                false,
                &key.columns,
            );
        }
        for index in schema.secondary_indexes.iter().filter(|i| !i.is_deleted) {
            let columns: Vec<String> = index
                .columns
                .iter()
                .filter_map(|w| {
                    schema
                        .fields
                        .iter()
                        .find(|f| f.write_order as u32 == *w && !f.is_deleted)
                })
                .map(|f| f.name.clone())
                .collect();
            push(
                translate::plain_index_name(&name, &index.name),
                !index.unique,
                &columns,
            );
        }
        Ok(rows)
    }

    async fn create_table_text(&mut self, table: &str) -> Result<String, MyError> {
        let described = self.describe_table(table).await?;
        let name = table.to_lowercase();
        let mut lines: Vec<String> = described
            .iter()
            .map(|c| {
                let mut line = format!("  `{}` {}", c.name, c.mysql_type);
                if !c.nullable {
                    line.push_str(" NOT NULL");
                }
                if c.auto {
                    line.push_str(" AUTO_INCREMENT");
                } else if let Some(d) = &c.default {
                    if d == "CURRENT_TIMESTAMP" || d.parse::<f64>().is_ok() {
                        line.push_str(&format!(" DEFAULT {d}"));
                    } else {
                        line.push_str(&format!(" DEFAULT '{}'", d.replace('\'', "''")));
                    }
                } else if c.nullable {
                    line.push_str(" DEFAULT NULL");
                }
                line
            })
            .collect();
        let rows = self.index_rows(&name)?;
        let mut keys: Vec<(String, bool, Vec<String>)> = Vec::new();
        for r in rows {
            let key = r[2].clone().unwrap_or_default();
            let non_unique = r[1].as_deref() == Some("1");
            let column = r[4].clone().unwrap_or_default();
            match keys.iter_mut().find(|k| k.0 == key) {
                Some(k) => k.2.push(column),
                None => keys.push((key, non_unique, vec![column])),
            }
        }
        for (key, non_unique, columns) in keys {
            let cols = columns
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(",");
            lines.push(if key == "PRIMARY" {
                format!("  PRIMARY KEY ({cols})")
            } else if non_unique {
                format!("  KEY `{key}` ({cols})")
            } else {
                format!("  UNIQUE KEY `{key}` ({cols})")
            });
        }
        if let Some(view) = self.database().schema_view(&name) {
            use crate::core::rql::foreign_key::ReferentialAction as A;
            let action = |a: &A| match a {
                A::NoAction => None,
                A::Restrict => Some("RESTRICT"),
                A::Cascade => Some("CASCADE"),
                A::SetNull => Some("SET NULL"),
                A::SetDefault => Some("SET DEFAULT"),
            };
            for fk in view.schema().foreign_keys.iter().filter(|f| !f.is_deleted) {
                let mut line = format!(
                    "  CONSTRAINT `{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`)",
                    fk.name, fk.column, fk.parent_table, fk.parent_column
                );
                if let Some(a) = action(&fk.on_delete) {
                    line.push_str(&format!(" ON DELETE {a}"));
                }
                if let Some(a) = action(&fk.on_update) {
                    line.push_str(&format!(" ON UPDATE {a}"));
                }
                lines.push(line);
            }
        }
        Ok(format!(
            "CREATE TABLE `{name}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
            lines.join(",\n")
        ))
    }

    // -----------------------------------------------------------------------
    // Prepared statements
    // -----------------------------------------------------------------------

    async fn com_prepare(&mut self, sql: &[u8]) -> io::Result<()> {
        let tokens = match lex::lex(sql, self.mode()) {
            Ok(t) => t,
            Err(e) => {
                self.err(&MyError::syntax(format!(
                    "You have an error in your SQL syntax: {}",
                    e.0
                )));
                return Ok(());
            }
        };
        let mut statements = lex::split_statements(tokens);
        if statements.len() != 1 {
            self.err(&MyError::syntax(if statements.is_empty() {
                "Query was empty"
            } else {
                "a prepared statement holds one statement"
            }));
            return Ok(());
        }
        let tokens = self.rename_temp_references(statements.pop().unwrap());
        let params = tokens
            .iter()
            .filter(|t| matches!(t.tok, Tok::Param(_)))
            .count();
        let id = self.next_statement;
        self.next_statement = self.next_statement.wrapping_add(1).max(1);
        let (action, _) = admin::classify(&tokens);
        let mut prepared = Prepared {
            engine_name: None,
            kind: Kind::Other,
            sql: sql.to_vec(),
            params,
            param_oids: Vec::new(),
            param_types: Vec::new(),
            long_data: HashMap::new(),
        };
        let mut columns: Vec<MyColumn> = Vec::new();
        if matches!(action, Action::Data) {
            self.load_on_update(&tokens).await;
            let forward = match translate::translate(sql, tokens, self, true) {
                Ok(f) => f,
                Err(e) => {
                    self.err(&e);
                    return Ok(());
                }
            };
            prepared.kind = forward.kind.clone();
            if forward.statements.len() == 1
                && forward.found_rows.is_none()
                && forward.kind != Kind::Ddl
            {
                let name = format!("my{}_{}", self.conn_id, id);
                let b = self.bridge();
                b.parse(&name, &forward.statements[0], &[]);
                b.describe(b'S', &name);
                b.sync();
                let mut error = None;
                let mut oids = Vec::new();
                let mut fields = Vec::new();
                loop {
                    let frame = self.bridge().next().await?;
                    match frame.tag {
                        b'E' if error.is_none() => error = Some(PgNotice::parse(frame.body)),
                        b't' => {
                            let body = frame.body;
                            let n = i16::from_be_bytes([body[0], body[1]]).max(0) as usize;
                            oids = (0..n)
                                .map(|k| {
                                    u32::from_be_bytes(
                                        body[2 + k * 4..6 + k * 4].try_into().unwrap(),
                                    )
                                })
                                .collect();
                        }
                        b'T' => fields = parse_row_description(frame.body)?,
                        b'Z' => break,
                        _ => {}
                    }
                }
                match error {
                    None => {
                        prepared.engine_name = Some(name);
                        prepared.param_oids = oids;
                        if forward.kind == Kind::Query {
                            columns = fields.iter().map(results::column_for).collect();
                        }
                    }
                    // A statement the engine cannot prepare is run as text
                    // at each execute; a statement that is simply wrong is
                    // refused now, as MySQL refuses it.
                    Some(e) if matches!(e.code.as_str(), "0A000" | "XX000" | "42P18" | "42804") => {
                    }
                    Some(e) => {
                        self.err(&results::error_from_engine(&e));
                        return Ok(());
                    }
                }
            }
        }
        // COM_STMT_PREPARE_OK
        let mut p = vec![0u8];
        p.extend_from_slice(&id.to_le_bytes());
        p.extend_from_slice(&(columns.len() as u16).to_le_bytes());
        p.extend_from_slice(&(params as u16).to_le_bytes());
        p.push(0);
        p.extend_from_slice(&0u16.to_le_bytes());
        self.io.packet(&p);
        let schema = self.database_name().to_string();
        if params > 0 {
            for _ in 0..params {
                let mut c = MyColumn::text("?");
                c.charset = results::CHARSET_BINARY;
                results::write_column(&mut self.io, &schema, &c);
            }
            if !self.deprecate_eof() {
                let status = self.status(false);
                results::write_eof(&mut self.io, status, 0);
            }
        }
        if !columns.is_empty() {
            for c in &columns {
                results::write_column(&mut self.io, &schema, c);
            }
            if !self.deprecate_eof() {
                let status = self.status(false);
                results::write_eof(&mut self.io, status, 0);
            }
        }
        self.statements.insert(id, prepared);
        Ok(())
    }

    async fn com_execute(&mut self, body: &[u8]) -> io::Result<()> {
        let mut r = Reader::new(body);
        let id = r.u32().unwrap_or(0);
        let Some(mut prepared) = self.statements.remove(&id) else {
            self.err(&MyError::new(
                1243,
                "HY000",
                format!("Unknown prepared statement handler ({id}) given to mysqld_stmt_execute"),
            ));
            return Ok(());
        };
        let values = match decode_params(&mut r, &mut prepared) {
            Ok(v) => v,
            Err(e) => {
                self.statements.insert(id, prepared);
                self.err(&e);
                return Ok(());
            }
        };
        prepared.long_data.clear();
        self.warnings.clear();
        match &prepared.engine_name {
            Some(name) => {
                let name = name.clone();
                let params: Vec<(i16, Option<Vec<u8>>)> = values
                    .into_iter()
                    .enumerate()
                    .map(|(k, v)| {
                        let oid = prepared.param_oids.get(k).copied().unwrap_or(0);
                        match v {
                            None => (0, None),
                            Some(ParamValue::Bytes(b)) if oid == 17 => (1, Some(b)),
                            Some(ParamValue::Bytes(b)) => (0, Some(b)),
                            Some(ParamValue::Text(t)) => (0, Some(t.into_bytes())),
                        }
                    })
                    .collect();
                let forward = Forward {
                    statements: Vec::new(),
                    kind: prepared.kind.clone(),
                    found_rows: None,
                    params: prepared.params,
                    deferred: Vec::new(),
                };
                self.forward(forward, false, Rows::Binary, Some((&name, params)))
                    .await?;
            }
            None => {
                // Run as text with the values in place of the `?`s.
                let tokens = match lex::lex(&prepared.sql, self.mode()) {
                    Ok(t) => t,
                    Err(e) => {
                        self.statements.insert(id, prepared);
                        self.err(&MyError::syntax(e.0));
                        return Ok(());
                    }
                };
                let tokens: Vec<Token> = tokens
                    .into_iter()
                    .map(|t| match t.tok {
                        Tok::Param(k) => match values.get(k) {
                            Some(Some(ParamValue::Text(text))) => {
                                Token::new(Tok::Str(text.clone().into_bytes()))
                            }
                            Some(Some(ParamValue::Bytes(b))) => Token::new(Tok::Str(b.clone())),
                            _ => Token::word("NULL"),
                        },
                        _ => t,
                    })
                    .collect();
                let sql = prepared.sql.clone();
                self.statement(&sql, tokens, false, Rows::Binary).await?;
            }
        }
        self.statements.insert(id, prepared);
        Ok(())
    }
}

/// A statement with its TEMPORARY tables named as the tables holding them.
struct TempBound {
    tokens: Vec<Token>,
    /// CREATE TEMPORARY TABLE: (its name, the table made).
    created: Option<(String, String)>,
    /// DROP TABLE of temporary tables: their names.
    dropped: Vec<String>,
    /// DROP TEMPORARY TABLE IF EXISTS of none: answered OK, nothing runs.
    noop: bool,
}

/// TEMPORARY tables, which the engine does not have: a table per
/// connection, under a name of its own, dropped when the connection ends
/// or is reset. A temporary table hides a table of its name, as in MySQL.
impl Session {
    fn temp_name(&self, name: &str) -> String {
        format!("{}{}", self.temp_prefix, name.to_lowercase())
    }

    fn bind_temp_tables(&self, tokens: Vec<Token>) -> Result<TempBound, MyError> {
        use crate::sqlcompat::kw;
        let quoted = |t: &Token, name: String| Token {
            tok: Tok::Quoted(name),
            space: t.space,
            span: t.span,
        };
        // CREATE TEMPORARY TABLE [IF NOT EXISTS] name …
        if kw(&tokens, 0, "CREATE") && kw(&tokens, 1, "TEMPORARY") && kw(&tokens, 2, "TABLE") {
            let mut at = 3;
            if kw(&tokens, at, "IF") && kw(&tokens, at + 1, "NOT") && kw(&tokens, at + 2, "EXISTS") {
                at += 3;
            }
            let mut name_at = at;
            if tokens.get(at + 1).is_some_and(|t| t.is_op(".")) {
                name_at = at + 2;
            }
            let name = tokens
                .get(name_at)
                .and_then(|t| t.ident())
                .ok_or_else(|| MyError::syntax("CREATE TEMPORARY TABLE needs a name"))?
                .to_lowercase();
            let table = self.temp_name(&name);
            let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
            for (k, t) in tokens.iter().enumerate() {
                if k == 1 {
                    continue;
                }
                if k == at && name_at != at {
                    continue;
                }
                if k == at + 1 && name_at != at {
                    continue;
                }
                out.push(if k == name_at { quoted(t, table.clone()) } else { t.clone() });
            }
            return Ok(TempBound {
                tokens: out,
                created: Some((name, table)),
                dropped: Vec::new(),
                noop: false,
            });
        }
        // DROP [TEMPORARY] TABLE [IF EXISTS] a, b
        let mut dropped = Vec::new();
        if kw(&tokens, 0, "DROP") && (kw(&tokens, 1, "TABLE") || (kw(&tokens, 1, "TEMPORARY") && kw(&tokens, 2, "TABLE"))) {
            let temporary = kw(&tokens, 1, "TEMPORARY");
            let mut at = if temporary { 3 } else { 2 };
            let if_exists = kw(&tokens, at, "IF") && kw(&tokens, at + 1, "EXISTS");
            if if_exists {
                at += 2;
            }
            for part in crate::sqlcompat::split_commas(&tokens[at..]) {
                let name = part.last().and_then(|t| t.ident()).unwrap_or("").to_lowercase();
                if self.temp_tables.contains_key(&name) {
                    dropped.push(name);
                } else if temporary && !if_exists {
                    return Err(MyError::new(1051, "42S02", format!("Unknown table '{name}'")));
                }
            }
            if temporary {
                // Only the temporary tables named.
                if dropped.is_empty() {
                    return Ok(TempBound { tokens, created: None, dropped, noop: true });
                }
                let mut out = vec![tokens[0].clone(), tokens[2].clone()];
                for (n, name) in dropped.iter().enumerate() {
                    if n > 0 {
                        out.push(Token::op(","));
                    }
                    out.push(Token::new(Tok::Quoted(self.temp_tables[name].clone())));
                }
                return Ok(TempBound { tokens: out, created: None, dropped, noop: false });
            }
        }
        Ok(TempBound {
            tokens: self.rename_temp_references(tokens),
            created: None,
            dropped,
            noop: false,
        })
    }

    /// Each temporary table's name, where a table's name stands, as the
    /// table holding it.
    fn rename_temp_references(&self, tokens: Vec<Token>) -> Vec<Token> {
        if self.temp_tables.is_empty() {
            return tokens;
        }
        let mut in_tables = false;
        let mut out = Vec::with_capacity(tokens.len());
        for k in 0..tokens.len() {
            let t = &tokens[k];
            let word = match &t.tok {
                Tok::Word(w) => Some(w.to_ascii_uppercase()),
                _ => None,
            };
            let table = t.ident().and_then(|n| self.temp_tables.get(&n.to_lowercase()));
            let prev_dot = k > 0 && tokens[k - 1].is_op(".");
            let next_dot = tokens.get(k + 1).is_some_and(|n| n.is_op("."));
            let next_call = tokens.get(k + 1).is_some_and(|n| n.is_op("("));
            match (table, word.as_deref()) {
                (Some(table), _) if !prev_dot && !next_call && (in_tables || next_dot) => {
                    out.push(Token {
                        tok: Tok::Quoted(table.clone()),
                        space: t.space,
                        span: t.span,
                    });
                    continue;
                }
                (_, Some("FROM" | "JOIN" | "INTO" | "UPDATE" | "TABLE" | "TABLES" | "DESCRIBE" | "DESC" | "EXPLAIN")) => in_tables = true,
                (_, Some("WHERE" | "ON" | "SET" | "GROUP" | "ORDER" | "LIMIT" | "USING" | "VALUES" | "VALUE" | "SELECT" | "HAVING" | "UNION" | "LIKE" | "WITH")) => in_tables = false,
                _ => {}
            }
            if t.is_op("(") {
                in_tables = false;
            }
            out.push(t.clone());
        }
        out
    }

    async fn drop_temp_tables(&mut self) {
        if self.temp_tables.is_empty() || self.bridge.is_none() {
            return;
        }
        if self.bridge().in_block() {
            let _ = self.silent("ROLLBACK").await;
        }
        for (_, table) in std::mem::take(&mut self.temp_tables) {
            let _ = self.silent(&format!("DROP TABLE IF EXISTS {}", crate::sqlcompat::ident_sql(&table))).await;
        }
    }
}

/// A parameter's value, for the engine.
enum ParamValue {
    Text(String),
    Bytes(Vec<u8>),
}

fn decode_params(
    r: &mut Reader<'_>,
    prepared: &mut Prepared,
) -> Result<Vec<Option<ParamValue>>, MyError> {
    let bad = |_| MyError::new(1210, "HY000", "Incorrect arguments to mysqld_stmt_execute");
    let n = prepared.params;
    let _flags = r.u8().map_err(bad)?;
    let _iterations = r.u32().map_err(bad)?;
    if n == 0 {
        return Ok(Vec::new());
    }
    let bitmap = r.bytes(n.div_ceil(8)).map_err(bad)?.to_vec();
    let bound = r.u8().map_err(bad)?;
    if bound == 1 {
        let mut types = Vec::with_capacity(n);
        for _ in 0..n {
            let t = r.u8().map_err(bad)?;
            let flags = r.u8().map_err(bad)?;
            types.push((t, flags & 0x80 != 0));
        }
        prepared.param_types = types;
    }
    if prepared.param_types.len() != n {
        return Err(MyError::new(
            1210,
            "HY000",
            "Incorrect arguments to mysqld_stmt_execute: no parameter types were bound",
        ));
    }
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        if bitmap[k / 8] & (1 << (k % 8)) != 0 {
            out.push(None);
            continue;
        }
        if let Some(data) = prepared.long_data.get(&(k as u16)) {
            out.push(Some(ParamValue::Bytes(data.clone())));
            continue;
        }
        let (t, unsigned) = prepared.param_types[k];
        use results::*;
        let v = match t {
            T_TINY => {
                let b = r.u8().map_err(bad)?;
                ParamValue::Text(if unsigned {
                    b.to_string()
                } else {
                    (b as i8).to_string()
                })
            }
            T_SHORT | T_YEAR => {
                let v = r.u16().map_err(bad)?;
                ParamValue::Text(if unsigned {
                    v.to_string()
                } else {
                    (v as i16).to_string()
                })
            }
            T_LONG | T_INT24 => {
                let v = r.u32().map_err(bad)?;
                ParamValue::Text(if unsigned {
                    v.to_string()
                } else {
                    (v as i32).to_string()
                })
            }
            T_LONGLONG => {
                let v = r.u64().map_err(bad)?;
                ParamValue::Text(if unsigned {
                    v.to_string()
                } else {
                    (v as i64).to_string()
                })
            }
            T_FLOAT => ParamValue::Text(
                f32::from_le_bytes(r.bytes(4).map_err(bad)?.try_into().unwrap()).to_string(),
            ),
            T_DOUBLE => ParamValue::Text(
                f64::from_le_bytes(r.bytes(8).map_err(bad)?.try_into().unwrap()).to_string(),
            ),
            T_DATE | T_DATETIME | T_TIMESTAMP => {
                let len = r.u8().map_err(bad)?;
                let b = r.bytes(len as usize).map_err(bad)?;
                let get = |i: usize| b.get(i).copied().unwrap_or(0);
                let year = if b.len() >= 2 {
                    u16::from_le_bytes([b[0], b[1]])
                } else {
                    0
                };
                let micros = if b.len() >= 11 {
                    u32::from_le_bytes([b[7], b[8], b[9], b[10]])
                } else {
                    0
                };
                let date = format!("{year:04}-{:02}-{:02}", get(2), get(3));
                ParamValue::Text(if t == T_DATE {
                    date
                } else if micros > 0 {
                    format!(
                        "{date} {:02}:{:02}:{:02}.{micros:06}",
                        get(4),
                        get(5),
                        get(6)
                    )
                } else {
                    format!("{date} {:02}:{:02}:{:02}", get(4), get(5), get(6))
                })
            }
            T_TIME => {
                let len = r.u8().map_err(bad)?;
                let b = r.bytes(len as usize).map_err(bad)?;
                if b.is_empty() {
                    ParamValue::Text("00:00:00".into())
                } else {
                    let negative = b[0] == 1;
                    let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]);
                    let micros = if b.len() >= 12 {
                        u32::from_le_bytes([b[8], b[9], b[10], b[11]])
                    } else {
                        0
                    };
                    let hours = days * 24 + b[5] as u32;
                    let mut text = format!(
                        "{}{hours:02}:{:02}:{:02}",
                        if negative { "-" } else { "" },
                        b[6],
                        b[7]
                    );
                    if micros > 0 {
                        text.push_str(&format!(".{micros:06}"));
                    }
                    ParamValue::Text(text)
                }
            }
            T_NULL => {
                out.push(None);
                continue;
            }
            _ => {
                let bytes = r.lenenc_str().map_err(bad)?.to_vec();
                match String::from_utf8(bytes) {
                    Ok(text)
                        if !matches!(t, T_BLOB | T_TINY_BLOB | T_MEDIUM_BLOB | T_LONG_BLOB) =>
                    {
                        ParamValue::Text(text)
                    }
                    Ok(text) => ParamValue::Bytes(text.into_bytes()),
                    Err(e) => ParamValue::Bytes(e.into_bytes()),
                }
            }
        };
        out.push(Some(v));
    }
    Ok(out)
}

/// A column as SHOW COLUMNS describes it.
struct Described {
    name: String,
    mysql_type: String,
    /// A text column (has a collation).
    text: bool,
    nullable: bool,
    key: String,
    default: Option<String>,
    extra: String,
    auto: bool,
}

/// The MySQL spelling of an engine type.
fn mysql_type_name(
    data_type: &str,
    length: Option<String>,
    precision: Option<String>,
    scale: Option<String>,
) -> (String, bool) {
    match data_type {
        "smallint" => ("smallint".into(), false),
        "integer" => ("int".into(), false),
        "bigint" => ("bigint".into(), false),
        "boolean" => ("tinyint(1)".into(), false),
        "real" => ("float".into(), false),
        "double precision" => ("double".into(), false),
        "numeric" => (
            match (precision, scale) {
                (Some(p), Some(s)) => format!("decimal({p},{s})"),
                _ => "decimal(65,30)".into(),
            },
            false,
        ),
        "character varying" | "character" => (
            match length {
                Some(n) => format!("varchar({n})"),
                None => "text".into(),
            },
            true,
        ),
        "text" => ("text".into(), true),
        "date" => ("date".into(), false),
        "timestamp without time zone" => ("datetime(6)".into(), false),
        "timestamp with time zone" => ("timestamp(6)".into(), false),
        "time without time zone" | "time with time zone" => ("time(6)".into(), false),
        "bytea" => ("longblob".into(), false),
        "json" | "jsonb" => ("json".into(), false),
        "uuid" => ("char(36)".into(), true),
        other => (other.to_string(), true),
    }
}

fn check_charset(cs: &str) -> Result<(), MyError> {
    match cs.trim_matches('\'').to_ascii_lowercase().as_str() {
        "" | "utf8mb4" | "utf8" | "utf8mb3" | "ascii" | "binary" | "default" => Ok(()),
        other => Err(MyError::new(
            1115,
            "42000",
            format!("Unknown character set: '{other}' (this server speaks utf8mb4 only)"),
        )),
    }
}

fn wrong_value(name: &str, value: &Value) -> MyError {
    MyError::new(
        1231,
        "42000",
        format!(
            "Variable '{name}' can't be set to the value of '{}'",
            value.text().unwrap_or_else(|| "NULL".into())
        ),
    )
}

/// `+05:30` as `+05:30` for an INTERVAL time zone, or `None` for a named zone.
fn utc_offset(zone: &str) -> Option<String> {
    let z = zone.trim();
    let sign = z.chars().next().filter(|c| *c == '+' || *c == '-')?;
    let (h, m) = z[1..].split_once(':')?;
    let h: u32 = h.parse().ok()?;
    let m: u32 = m.parse().ok()?;
    (h <= 14 && m < 60).then(|| format!("{sign}{h:02}:{m:02}"))
}

/// The insert id an INSERT's OK packet reports.
fn insert_id_of(kind: &Kind, returned: Option<u64>) -> Option<u64> {
    match kind {
        Kind::Insert { auto: Some(_) } => returned,
        _ => None,
    }
}
