//! A shard's side of rshard, the gateway↔shard protocol
//! (`rasterizeddb_router::rshard`, sharding-plan.md B1a.7, §4.9 G5).
//!
//! One rastcp/2 listener (`--shard-listen`), mTLS-only: a connection whose
//! certificate is not a trusted gateway's never reaches a handler. Each mux
//! connection carries many virtual sessions; each virtual session is a full
//! pgwire session on this shard, opened through the in-process bridge the
//! MySQL and TDS front ends use (`pgwire::proxied::PgBridge`), so every
//! rewrite (views, triggers, CTE temps, DO blocks, sequences, the privilege
//! gates) runs here unchanged. The bridge relays the client's pgwire bytes
//! and copies the backend's messages back whole; nothing is decoded or
//! re-encoded. (G5's frame-writing session I/O, which saves that copy,
//! replaces the bridge once `pgwire.rs` grows the session-I/O trait.)
//!
//! Sessions live in the connection's state: when the mux connection goes,
//! the state drops, every bridge closes, and each session rolls back what
//! it had open (R6's lease).
//!
//! **Epochs.** Every Pg and ExecPart request carries the gateway's map
//! epoch. A gateway behind this shard is refused `RDB03` (moved: refresh
//! the map), one ahead of it `57P03` (this shard has not seen the new map
//! yet), both before effect.
//!
//! **Per-user-shard (`pinned`) sessions** are admitted only when the
//! placement hook says this shard hosts the role (`RDB03` otherwise, so the
//! gateway refreshes the user's route).
//!
//! **Coordinated aggregates (B1c).** A keyless aggregate the gateway sends
//! to this shard as coordinator gathers every other shard's partial fold
//! over shard-to-shard rshard, then runs here with them merged in.
//!
//! **Not in version 1, refused before effect:** native requests; `TopN` fan-out parts; a
//! batch ending in Flush rather than Sync.

use std::collections::HashMap;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU64, Ordering};

use rasterizeddb_router::rshard::{
    self, AuthKeys, FENCED, HELLO_FIRST, MOVED, Merge, PROTOCOL, PartReply, Refusal, Request, Response, SESSION_GONE,
};
use rclite::Arc;
use tokio::sync::Mutex;

use crate::core::database::catalog::DatabaseCatalog;

/// Bytes of backend messages one reply carries before the rest waits for
/// `Continue`.
pub const CHUNK: usize = 256 << 10;

/// The schema epoch templates are classified under. Raised by cluster DDL
/// (B1a.10); a gateway drops templates of older epochs.
pub static SCHEMA_EPOCH: AtomicU64 = AtomicU64::new(1);

/// The listener's configuration (`--shard-listen` and friends).
#[derive(Clone, Debug)]
pub struct RshardConfig {
    pub addr: String,
    pub port: u16,
    /// This shard's certificate chain and key, PEM files.
    pub cert: String,
    pub key: String,
    /// Who may connect: a CA every gateway certificate is signed by, or the
    /// exact gateway certificates' SHA-256 fingerprints.
    pub gateway_ca: Option<String>,
    pub gateway_pins: Vec<String>,
    /// The CA the other shards' certificates are signed by, for a
    /// coordinator's connections to its peers (B1c); `gateway_ca` when
    /// unset.
    pub peer_ca: Option<String>,
}

/// What a coordinator needs to reach its peers: its own certificate as the
/// client identity, and the CA it trusts them by.
struct PeerConfig {
    cert: String,
    key: String,
    ca: Option<String>,
}

static PEERS: std::sync::OnceLock<PeerConfig> = std::sync::OnceLock::new();
static PEER_CLIENTS: std::sync::Mutex<Option<HashMap<u32, StdArc<rastcp::mux::MuxClient>>>> = std::sync::Mutex::new(None);
static NEXT_PEER_SESSION: AtomicU64 = AtomicU64::new(1 << 62);

/// One mux connection's state.
pub struct Conn {
    catalog: Arc<DatabaseCatalog>,
    hello: std::sync::Mutex<Hello>,
    sessions: std::sync::Mutex<HashMap<u64, StdArc<Mutex<VSession>>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Hello {
    None,
    /// `cluster_id = 0`: only MapFetch is served.
    Bootstrap,
    Done,
}

/// A virtual session.
struct VSession {
    bridge: crate::pgwire::proxied::PgBridge,
    database: String,
    role: String,
    /// Backend bytes read past a chunk's end, sent with the next Continue.
    pending: Vec<u8>,
    /// The reply ended (`ReadyForQuery` or CopyInResponse); Continue has
    /// nothing to read.
    finished: bool,
}

/// Starts the listener; runs until the process ends.
pub async fn serve(catalog: Arc<DatabaseCatalog>, config: RshardConfig) -> std::io::Result<()> {
    use rastcp::tls::{ClientAuth, load_certificates, load_private_key, parse_fingerprint};
    let io = |e: rastcp::error::RastcpError| std::io::Error::other(e.to_string());
    let client_auth = match (&config.gateway_ca, config.gateway_pins.is_empty()) {
        (Some(ca), _) => ClientAuth::RequireCa(load_certificates(ca).map_err(io)?),
        (None, false) => ClientAuth::RequirePinned(
            config.gateway_pins.iter().map(|p| parse_fingerprint(p)).collect::<Result<_, _>>().map_err(io)?,
        ),
        (None, true) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--shard-listen needs --gateway-ca or --gateway-pin: rshard admits gateways by certificate only",
            ));
        }
    };
    let _ = PEERS.set(PeerConfig {
        cert: config.cert.clone(),
        key: config.key.clone(),
        ca: config.peer_ca.clone().or_else(|| config.gateway_ca.clone()),
    });
    let server = rastcp::server::TcpServerBuilder::new(&config.addr, config.port)
        .client_auth(client_auth)
        .with_certificates(load_certificates(&config.cert).map_err(io)?, load_private_key(&config.key).map_err(io)?)
        .handler_timeout(None)
        .max_message_size(rshard::MAX_FIELD + 64)
        .build()
        .await
        .map_err(io)?;
    log::info!("rshard listening on {}:{}", config.addr, config.port);
    server
        .run_with_session(
            catalog,
            |catalog: &Arc<DatabaseCatalog>, _principal, _info| Conn {
                catalog: catalog.clone(),
                hello: std::sync::Mutex::new(Hello::None),
                sessions: std::sync::Mutex::new(HashMap::new()),
            },
            |_catalog, _principal, conn: StdArc<Conn>, data: Vec<u8>| async move { handle(&conn, &data).await.encode() },
        )
        .await
        .map_err(io)
}

fn served() -> Option<StdArc<crate::core::shard::map_state::Served>> {
    crate::core::shard::map_state::current()
}

fn epoch() -> u64 {
    served().map_or(0, |s| s.map.epoch())
}

fn refuse(sqlstate: [u8; 5], before_effect: bool, message: impl Into<String>) -> Response {
    Response::Refused(Refusal { sqlstate, before_effect, map_epoch: epoch(), message: message.into() })
}

async fn handle(conn: &Conn, data: &[u8]) -> Response {
    let request = match Request::decode(data) {
        Ok(r) => r,
        Err(e) => return refuse(PROTOCOL, true, format!("rshard: {e}")),
    };
    let Some(served) = served() else {
        return refuse(PROTOCOL, true, "this node is not a shard (--role shard)");
    };
    let hello = *conn.hello.lock().unwrap();
    match (&request, hello) {
        (Request::Hello { .. }, _) => {}
        (Request::MapFetch, Hello::Bootstrap | Hello::Done) => {}
        (_, Hello::Done) => {}
        (_, Hello::Bootstrap) => return refuse(PROTOCOL, true, "a bootstrap connection serves only MapFetch"),
        (_, Hello::None) => return refuse(HELLO_FIRST, true, "Hello first"),
    }
    match request {
        Request::Hello { cluster_id, gateway, .. } => {
            let mine = served.map.cluster_id();
            let state = if cluster_id == 0 {
                Hello::Bootstrap
            } else if cluster_id == mine {
                Hello::Done
            } else {
                return refuse(
                    PROTOCOL,
                    true,
                    format!("gateway {gateway} is for cluster {cluster_id:032x}, this shard is in {mine:032x}"),
                );
            };
            *conn.hello.lock().unwrap() = state;
            Response::HelloOk {
                cluster_id: mine,
                shard_id: served.shard_id,
                map_epoch: served.map.epoch(),
                map_digest: served.map.digest(),
                schema_epoch: SCHEMA_EPOCH.load(Ordering::Acquire),
            }
        }
        Request::MapFetch => Response::MapBytes { signature: served.signature.clone(), map: served.map.to_binary() },
        Request::AuthFetch { database, role } => auth_fetch(conn, &database, &role).await,
        Request::Classify { database, source, text, .. } => {
            let Some(guard) = conn.catalog.connect(&database) else {
                return refuse(*b"3D000", true, format!("database \"{database}\" does not exist"));
            };
            let template = super::classify::classify(guard.database(), source, &text);
            Response::Template { schema_epoch: SCHEMA_EPOCH.load(Ordering::Acquire), template }
        }
        Request::OpenSession { session, database, role, params, pinned, .. } => {
            if pinned && !crate::core::policy_hooks::placement().hosts_pinned_role(&database, &role) {
                return refuse(
                    MOVED,
                    true,
                    format!("role \"{role}\" of \"{database}\" is not placed on this shard: refresh the user's route"),
                );
            }
            open_session(conn, session, &database, &role, params).await
        }
        Request::Pg { session, map_epoch, bytes } => {
            if let Some(r) = epoch_refusal(map_epoch, served.map.epoch()) {
                return r;
            }
            pg(conn, session, &bytes).await
        }
        Request::Continue { session } => continue_reply(conn, session).await,
        Request::CloseSession { session } => {
            let gone = conn.sessions.lock().unwrap().remove(&session);
            if let Some(s) = gone
                && let Ok(s) = StdArc::try_unwrap(s)
            {
                s.into_inner().bridge.terminate().await;
            }
            Response::Closed { session }
        }
        Request::ExecPart { session, map_epoch, merge, bytes } => {
            if let Some(r) = epoch_refusal(map_epoch, served.map.epoch()) {
                return r;
            }
            exec_part(conn, session, merge, &bytes).await
        }
        Request::Cancel { backend_key } => {
            crate::core::cancel::cancel_request((backend_key >> 32) as i32, backend_key as u32 as i32);
            Response::Done
        }
        Request::Native { .. } => refuse(*b"0A000", true, "native requests through the gateway are not in rshard version 1"),
    }
}

fn epoch_refusal(theirs: u64, mine: u64) -> Option<Response> {
    if theirs < mine {
        Some(refuse(MOVED, true, format!("the gateway routes at map epoch {theirs}, this shard is at {mine}: refresh the map")))
    } else if theirs > mine {
        Some(refuse(FENCED, true, format!("the gateway routes at map epoch {theirs}, this shard is still at {mine}")))
    } else {
        None
    }
}

/// What SCRAM needs, from `role` in `database`; never a password. An
/// unknown role or a non-SCRAM verifier answers `found = false`.
async fn auth_fetch(conn: &Conn, database: &str, role: &str) -> Response {
    let not_found = || {
        Response::AuthKeys(AuthKeys {
            role_epoch: 0,
            found: false,
            can_login: false,
            iterations: 0,
            salt: Vec::new(),
            stored_key: [0; 32],
            server_key: [0; 32],
        })
    };
    let Some(guard) = conn.catalog.connect(database) else {
        return not_found();
    };
    let Some(r) = crate::auth::lookup_role(guard.database(), role).await else {
        return not_found();
    };
    let Some(crate::auth::Verifier::Scram { iterations, salt, stored_key, server_key }) = r.verifier.clone() else {
        return not_found();
    };
    let (Ok(stored), Ok(server)) = (<[u8; 32]>::try_from(stored_key.as_slice()), <[u8; 32]>::try_from(server_key.as_slice()))
    else {
        return not_found();
    };
    // Rises with any change to the credentials or attributes.
    let mut feed = Vec::new();
    feed.extend_from_slice(&stored);
    feed.extend_from_slice(&server);
    feed.extend_from_slice(&salt);
    feed.extend_from_slice(&iterations.to_le_bytes());
    feed.push(r.can_login as u8);
    feed.push(r.is_superuser as u8);
    let role_epoch = rasterizeddb_router::xxh64::xxh64(&feed, 0x5244_4252_4F4C_4501);
    Response::AuthKeys(AuthKeys {
        role_epoch,
        found: true,
        can_login: r.can_login,
        iterations,
        salt,
        stored_key: stored,
        server_key: server,
    })
}

async fn open_session(conn: &Conn, session: u64, database: &str, role: &str, params: Vec<(String, String)>) -> Response {
    use crate::pgwire::proxied::{BridgeOptions, OpenRefused, PgBridge, ProxiedLogin};
    let Some(guard) = conn.catalog.connect(database) else {
        return refuse(*b"3D000", true, format!("database \"{database}\" does not exist"));
    };
    // The gateway has verified the client's proof (G9); the role must still
    // exist here and may log in, and its attributes are this shard's.
    let Some(r) = crate::auth::lookup_role(guard.database(), role).await else {
        return refuse(*b"28000", true, format!("role \"{role}\" does not exist"));
    };
    if !r.can_login {
        return refuse(*b"28000", true, format!("role \"{role}\" is not permitted to log in"));
    }
    let ctx = crate::core::policy_hooks::SessionContext {
        role: &r.name,
        database,
        protocol: "rshard",
        superuser: r.is_superuser,
    };
    if let Err(e) = crate::core::policy_hooks::tenancy().admit(&ctx) {
        return refuse(*b"28000", true, e);
    }
    #[cfg(feature = "sharding_raft")]
    if let Err(e) = super::raft_apply::serving() {
        return refuse(FENCED, true, e);
    }
    drop(guard);
    let options = BridgeOptions {
        login: ProxiedLogin {
            user: r.name.clone(),
            superuser: r.is_superuser,
            authenticated: true,
            // Checked against this database's own role above: the session is
            // in the database it was checked in.
            database: database.to_string(),
            verifier: r.verifier.as_ref().map(|v| v.to_stored()),
            bootstrap_user: None,
        },
        limits: crate::pgwire::ConnectionLimits::default(),
        watch: None,
        parameters: params.into_iter().collect(),
        lenient_booleans: false,
    };
    let bridge = match PgBridge::open(conn.catalog.clone(), database, options).await {
        Ok(b) => b,
        Err(OpenRefused::NoDatabase) => return refuse(*b"3D000", true, format!("database \"{database}\" does not exist")),
        Err(OpenRefused::Loading) => return refuse(*b"57P03", true, format!("database \"{database}\" is being imported")),
        Err(OpenRefused::Io(e)) => return refuse(*b"08006", true, e.to_string()),
        Err(OpenRefused::Denied(e)) => return refuse(*b"28000", true, e),
    };
    let backend_key = ((bridge.pid as u32 as u64) << 32) | bridge.key as u32 as u64;
    let previous = conn.sessions.lock().unwrap().insert(
        session,
        StdArc::new(Mutex::new(VSession {
            bridge,
            database: database.to_string(),
            role: r.name.clone(),
            pending: Vec::new(),
            finished: true,
        })),
    );
    if previous.is_some() {
        return refuse(PROTOCOL, false, format!("session {session} was already open on this connection; it was replaced"));
    }
    Response::SessionOpened { session, backend_key, map_epoch: epoch(), bytes: Vec::new() }
}

fn session_of(conn: &Conn, session: u64) -> Option<StdArc<Mutex<VSession>>> {
    conn.sessions.lock().unwrap().get(&session).cloned()
}

/// The tag of the last whole frontend message in `bytes`, if they are whole.
fn last_tag(bytes: &[u8]) -> Option<u8> {
    let mut i = 0;
    let mut last = None;
    while i < bytes.len() {
        if bytes.len() - i < 5 {
            return None;
        }
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap());
        if len < 4 {
            return None;
        }
        last = Some(bytes[i]);
        i += 1 + len as usize;
    }
    (i == bytes.len()).then_some(last).flatten()
}

async fn pg(conn: &Conn, session: u64, bytes: &[u8]) -> Response {
    let Some(s) = session_of(conn, session) else {
        return refuse(SESSION_GONE, true, format!("session {session} is gone"));
    };
    let mut s = s.lock().await;
    match last_tag(bytes) {
        None => return refuse(PROTOCOL, true, "a Pg request must carry whole frontend messages"),
        Some(b'H') => return refuse(*b"0A000", true, "a batch ending in Flush is not in rshard version 1; end it with Sync"),
        Some(_) => {}
    }
    // A keyless aggregate the gateway sent here to coordinate (B1c).
    if let Some(sql) = simple_query_text(bytes)
        && may_aggregate(&sql)
        && let Some(guard) = conn.catalog.connect(&s.database)
    {
        let template = super::classify::classify(guard.database(), rshard::Source::Shape, &sql);
        drop(guard);
        if template == (rshard::RouteTemplate::FanOut { merge: Merge::Coordinator, writes: false }) {
            return coordinate(session, &mut s, bytes).await;
        }
    }
    s.bridge.raw(bytes);
    s.finished = false;
    s.pending.clear();
    read_chunk(session, &mut s).await
}

/// A cheap first look: only text that could be an aggregate is classified,
/// so ordinary statements pay nothing for the coordinator.
fn may_aggregate(sql: &str) -> bool {
    let up = sql.to_ascii_uppercase();
    ["GROUP", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "_AGG(", "BOOL_", "EVERY(", "STDDEV", "VARIANCE"]
        .iter()
        .any(|k| up.contains(k))
}

/// The SQL of a batch that is exactly one simple `Query` message.
fn simple_query_text(bytes: &[u8]) -> Option<String> {
    if bytes.first() != Some(&b'Q') || bytes.len() < 6 {
        return None;
    }
    let len = i32::from_be_bytes(bytes[1..5].try_into().ok()?) as usize;
    if 1 + len != bytes.len() {
        return None;
    }
    let body = &bytes[5..];
    let text = body.strip_suffix(&[0]).unwrap_or(body);
    String::from_utf8(text.to_vec()).ok()
}

/// A backend ErrorResponse then ReadyForQuery, as whole messages.
fn error_reply(code: &str, message: &str, status: u8) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, v) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', code), (b'M', message)] {
        body.push(k);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = vec![b'E'];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend(body);
    out.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
    out
}

/// A mux client to peer shard `id` at its map address, with this shard's
/// certificate, said Hello to once.
async fn peer_client(id: u32) -> Result<StdArc<rastcp::mux::MuxClient>, String> {
    if let Some(c) = PEER_CLIENTS.lock().unwrap().get_or_insert_with(HashMap::new).get(&id) {
        return Ok(c.clone());
    }
    let served = served().ok_or("this node is not a shard")?;
    let info = served.map.shard(id).ok_or_else(|| format!("shard {id} is not in the map"))?;
    let peers = PEERS.get().ok_or("the rshard listener is not configured")?;
    let ca = peers.ca.as_deref().ok_or("coordinating needs --peer-ca (or --gateway-ca) to trust the other shards")?;
    let e = |x: rastcp::error::RastcpError| x.to_string();
    let client = rastcp::client::TcpClientBuilder::new(&info.addr)
        .no_timeout()
        .trust(rastcp::tls::Trust::RootCa(rastcp::tls::load_certificates(ca).map_err(e)?))
        .identity(rastcp::tls::ClientIdentity::load(&peers.cert, &peers.key).map_err(e)?)
        .build_multiplexed()
        .await
        .map_err(e)?;
    let client = StdArc::new(client);
    let hello = Request::Hello {
        cluster_id: served.map.cluster_id(),
        gateway: format!("shard {}", served.shard_id),
        map_epoch: served.map.epoch(),
        map_digest: served.map.digest(),
    };
    match Response::decode(&client.send(&hello.encode()).await.map_err(e)?)? {
        Response::HelloOk { .. } => {}
        Response::Refused(r) => return Err(format!("shard {id} refused: {}", r.message)),
        other => return Err(format!("shard {id} answered Hello with {other:?}")),
    }
    PEER_CLIENTS.lock().unwrap().get_or_insert_with(HashMap::new).insert(id, client.clone());
    Ok(client)
}

/// One peer's partial for `bytes` (a simple Query), run as `role` in
/// `database` on its own virtual session there.
async fn peer_partial(id: u32, database: &str, role: &str, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let client = peer_client(id).await?;
    let session = NEXT_PEER_SESSION.fetch_add(1, Ordering::Relaxed);
    let e = |x: rastcp::error::RastcpError| x.to_string();
    let open = Request::OpenSession {
        session,
        database: database.to_string(),
        role: role.to_string(),
        params: Vec::new(),
        gateway: "coordinator".to_string(),
        pinned: false,
    };
    match Response::decode(&client.send(&open.encode()).await.map_err(e)?)? {
        Response::SessionOpened { .. } => {}
        Response::Refused(r) => return Err(format!("shard {id}: {}", r.message)),
        other => return Err(format!("shard {id} answered OpenSession with {other:?}")),
    }
    let req = Request::ExecPart { session, map_epoch: epoch(), merge: Merge::Coordinator, bytes: bytes.to_vec() };
    let answer = Response::decode(&client.send(&req.encode()).await.map_err(e)?);
    let _ = client.send(&Request::CloseSession { session }.encode()).await;
    match answer? {
        Response::PartReply(p) => match p.rows.into_iter().next() {
            Some((_, partial)) => Ok(partial),
            None => Err(format!("shard {id} returned no partial")),
        },
        Response::Refused(r) => Err(format!("shard {id}: {}", r.message)),
        other => Err(format!("shard {id} answered ExecPart with {other:?}")),
    }
}

/// Runs a keyless aggregate as the coordinator (B1c): every other shard's
/// partial gathered, then the statement run here with them merged into this
/// shard's own fold before it finishes. A fold that did not take them (the
/// statement was answered some other way) fails the statement `XX000`
/// rather than answer from this shard's rows alone.
async fn coordinate(session: u64, s: &mut VSession, bytes: &[u8]) -> Response {
    use crate::core::cancel::Fold;
    let Some(served) = served() else {
        return refuse(PROTOCOL, true, "this node is not a shard");
    };
    let mut partials = Vec::new();
    for info in served.map.shards().iter().filter(|i| i.id != served.shard_id) {
        match peer_partial(info.id, &s.database, &s.role, bytes).await {
            Ok(p) => partials.push(p),
            Err(e) => {
                let msg = format!("the coordinator could not gather shard {}'s part: {e}", info.id);
                return Response::PgReply { session, map_epoch: epoch(), more: false, status: s.bridge.status, bytes: error_reply("XX000", &msg, s.bridge.status) };
            }
        }
    }
    let Some(cancel) = crate::core::cancel::session_by_pid(s.bridge.pid) else {
        return refuse(*b"XX000", true, "the coordinating session is not registered");
    };
    *cancel.fold.lock().unwrap() = Fold::Coordinator { partials, consumed: false };
    s.bridge.raw(bytes);
    let mut out = Vec::new();
    loop {
        let frame = match s.bridge.next().await {
            Ok(f) => f,
            Err(e) => return refuse(*b"08006", false, format!("the session ended: {e}")),
        };
        out.push(frame.tag);
        out.extend_from_slice(&((frame.body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(frame.body);
        if frame.tag == b'Z' {
            break;
        }
        if out.len() > 256 << 20 {
            *cancel.fold.lock().unwrap() = Fold::Normal;
            return refuse(*b"54000", false, "a coordinated result passed 256 MiB");
        }
    }
    let consumed = matches!(&*cancel.fold.lock().unwrap(), Fold::Coordinator { consumed: true, .. });
    *cancel.fold.lock().unwrap() = Fold::Normal;
    s.finished = true;
    let status = s.bridge.status;
    if !consumed && !reply_has_error(&out) {
        let msg = "the statement was not answered by a fold the coordinator could merge the other shards into;                    it is refused rather than answered from one shard";
        return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply("XX000", msg, status) };
    }
    Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: out }
}

/// Whether a reply's messages include an ErrorResponse.
fn reply_has_error(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i + 5 <= bytes.len() {
        if bytes[i] == b'E' {
            return true;
        }
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap()) as usize;
        i += 1 + len;
    }
    false
}

async fn continue_reply(conn: &Conn, session: u64) -> Response {
    let Some(s) = session_of(conn, session) else {
        return refuse(SESSION_GONE, true, format!("session {session} is gone"));
    };
    let mut s = s.lock().await;
    read_chunk(session, &mut s).await
}

/// Reads backend messages into one reply of at most [`CHUNK`] bytes (plus
/// one message), ending at ReadyForQuery or CopyInResponse.
async fn read_chunk(session: u64, s: &mut VSession) -> Response {
    let mut out = std::mem::take(&mut s.pending);
    while !s.finished && out.len() < CHUNK {
        let frame = match s.bridge.next().await {
            Ok(f) => f,
            Err(e) => return refuse(*b"08006", false, format!("the session ended: {e}")),
        };
        let tag = frame.tag;
        out.push(tag);
        out.extend_from_slice(&((frame.body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(frame.body);
        if tag == b'Z' || tag == b'G' {
            s.finished = true;
        }
    }
    let status = if s.finished && out.len() >= 6 && out[out.len() - 6] == b'Z' { out[out.len() - 1] } else { 0 };
    Response::PgReply { session, map_epoch: epoch(), more: !s.finished, status, bytes: out }
}

/// One shard's part of a fan-out: the statement run as a Pg batch, its
/// DataRows split out for the gateway to concatenate or sum.
async fn exec_part(conn: &Conn, session: u64, merge: Merge, bytes: &[u8]) -> Response {
    if matches!(merge, Merge::TopN { .. }) {
        return refuse(*b"0A000", true, "a top-N fan-out is not in rshard version 1");
    }
    let Some(s) = session_of(conn, session) else {
        return refuse(SESSION_GONE, true, format!("session {session} is gone"));
    };
    let mut s = s.lock().await;
    if last_tag(bytes).is_none() {
        return refuse(PROTOCOL, true, "an ExecPart request must carry whole frontend messages");
    }
    // A coordinator's request for this shard's partial fold.
    let cancel = if merge == Merge::Coordinator {
        let Some(c) = crate::core::cancel::session_by_pid(s.bridge.pid) else {
            return refuse(*b"XX000", true, "the session is not registered");
        };
        *c.fold.lock().unwrap() = crate::core::cancel::Fold::Partial(None);
        Some(c)
    } else {
        None
    };
    s.bridge.raw(bytes);
    let (mut head, mut rows, mut tail) = (Vec::new(), Vec::new(), Vec::new());
    let mut status = 0;
    loop {
        let frame = match s.bridge.next().await {
            Ok(f) => f,
            Err(e) => return refuse(*b"08006", false, format!("the session ended: {e}")),
        };
        let mut msg = Vec::with_capacity(frame.body.len() + 5);
        msg.push(frame.tag);
        msg.extend_from_slice(&((frame.body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(frame.body);
        let tag = frame.tag;
        if tag == b'Z' {
            status = frame.body.first().copied().unwrap_or(0);
        }
        match tag {
            b'D' => rows.push((Vec::new(), msg)),
            _ if rows.is_empty() && tail.is_empty() && !matches!(tag, b'C' | b'Z' | b'E' | b'I') => head.extend(msg),
            _ => tail.extend(msg),
        }
        if tag == b'Z' {
            break;
        }
    }
    if let Some(c) = cancel {
        let taken = std::mem::take(&mut *c.fold.lock().unwrap());
        if reply_has_error(&tail) {
            return Response::PartReply(PartReply { session, map_epoch: epoch(), more: false, status, head, rows: Vec::new(), tail });
        }
        let crate::core::cancel::Fold::Partial(Some(partial)) = taken else {
            return refuse(*b"XX000", false, "the statement was not answered by a fold, so it has no partial to merge");
        };
        return Response::PartReply(PartReply { session, map_epoch: epoch(), more: false, status, head, rows: vec![(Vec::new(), partial)], tail });
    }
    Response::PartReply(PartReply { session, map_epoch: epoch(), more: false, status, head, rows, tail })
}
