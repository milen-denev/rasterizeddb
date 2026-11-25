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
    /// zstd level for the links to the other shards; `None`: uncompressed.
    pub peer_compression: Option<i32>,
}

/// `off`, `zstd` or `zstd:LEVEL` (1 to 22, or negative for the fast
/// levels): the zstd level, `None` for off.
pub fn parse_compression(v: &str) -> Result<Option<i32>, String> {
    match v.trim().to_ascii_lowercase().as_str() {
        "" | "off" | "none" => Ok(None),
        "zstd" => Ok(Some(3)),
        other => match other.strip_prefix("zstd:").map(|l| l.parse::<i32>()) {
            Some(Ok(l)) if (-7..=22).contains(&l) && l != 0 => Ok(Some(l)),
            _ => Err(format!("{v:?} is not off, zstd or zstd:LEVEL (-7 to 22, not 0)")),
        },
    }
}

/// Frames at least this long are compressed on a link that asked for it.
pub const COMPRESS_MIN_BYTES: u32 = 1024;

/// A received frame, decompressed when it came compressed. A declared length
/// past the largest frame, or one the bytes do not hold, is refused.
pub fn unwrap_frame(frame: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, String> {
    let Some((codec, len, body)) = rshard::split_compressed(frame) else {
        return Ok(std::borrow::Cow::Borrowed(frame));
    };
    if codec != rshard::CODEC_ZSTD {
        return Err(format!("rshard: compression codec {codec} is unknown"));
    }
    if len > rshard::MAX_FIELD + 64 {
        return Err(format!("rshard: a compressed frame of {len} bytes is past the limit"));
    }
    let plain = zstd::bulk::decompress(body, len).map_err(|e| format!("rshard: a compressed frame does not decompress: {e}"))?;
    if plain.len() != len {
        return Err("rshard: a compressed frame's length is not what it says".to_string());
    }
    Ok(std::borrow::Cow::Owned(plain))
}

/// `frame` compressed at `level` when it is at least `min` bytes and that
/// makes it smaller; else as it is.
pub fn wrap_frame(frame: Vec<u8>, level: i32, min: u32) -> Vec<u8> {
    if frame.len() < min as usize {
        return frame;
    }
    match zstd::bulk::compress(&frame, level) {
        Ok(z) if z.len() + 7 < frame.len() => rshard::compressed_frame(rshard::CODEC_ZSTD, frame.len(), &z),
        _ => frame,
    }
}

/// What a coordinator needs to reach its peers: its own certificate as the
/// client identity, and the CA it trusts them by.
struct PeerConfig {
    cert: String,
    key: String,
    ca: Option<String>,
    compression: Option<i32>,
}

static PEERS: std::sync::OnceLock<PeerConfig> = std::sync::OnceLock::new();
static PEER_CLIENTS: std::sync::Mutex<Option<HashMap<u32, StdArc<PeerLink>>>> = std::sync::Mutex::new(None);

/// A coordinator's link to one peer shard: frames compressed both ways
/// once the peer agreed to it (`--shard-peer-compression`).
struct PeerLink {
    client: rastcp::mux::MuxClient,
    compress: Option<i32>,
}

impl PeerLink {
    async fn send(&self, req: &[u8]) -> Result<Vec<u8>, rastcp::error::RastcpError> {
        let reply = match self.compress {
            Some(level) => self.client.send(&wrap_frame(req.to_vec(), level, COMPRESS_MIN_BYTES)).await?,
            None => self.client.send(req).await?,
        };
        match unwrap_frame(&reply) {
            Ok(std::borrow::Cow::Borrowed(_)) => Ok(reply),
            Ok(std::borrow::Cow::Owned(plain)) => Ok(plain),
            // Undecodable: let the caller's decode say so.
            Err(_) => Ok(reply),
        }
    }
}
static NEXT_PEER_SESSION: AtomicU64 = AtomicU64::new(1 << 62);

/// One mux connection's state.
pub struct Conn {
    catalog: Arc<DatabaseCatalog>,
    hello: std::sync::Mutex<Hello>,
    sessions: std::sync::Mutex<HashMap<u64, StdArc<Mutex<VSession>>>>,
    /// The zstd level and threshold this connection's replies are
    /// compressed with, once it asked (`Request::Compression`).
    compress: std::sync::Mutex<Option<(i32, u32)>>,
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
    /// ReadyForQuery messages still owed to the batch being read: one per
    /// Query or Sync it carried (a deferred `BEGIN` before the statement is
    /// two), so the reply is not cut at the first.
    ready_left: usize,
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
        compression: config.peer_compression,
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
                compress: std::sync::Mutex::new(None),
            },
            |_catalog, _principal, conn: StdArc<Conn>, data: Vec<u8>| async move {
                let reply = match unwrap_frame(&data) {
                    Ok(plain) => handle(&conn, &plain).await.encode(),
                    Err(e) => refuse(PROTOCOL, true, e).encode(),
                };
                match *conn.compress.lock().unwrap() {
                    Some((level, min)) => wrap_frame(reply, level, min),
                    None => reply,
                }
            },
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
        Request::Coordinate { session, map_epoch, partials, bytes } => {
            if let Some(r) = epoch_refusal(map_epoch, served.map.epoch()) {
                return r;
            }
            coordinate_given(conn, session, partials, &bytes).await
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
        Request::Decide { gid, commit } => {
            // A consistent fan-out is starting its snapshots (B6.1): a commit
            // decision waits for it, never longer than its hold.
            if commit {
                commit_hold::wait().await;
            }
            match decide(&gid, commit) {
                Ok(()) => Response::Done,
                Err(e) => refuse(*b"58030", false, e),
            }
        }
        Request::HoldCommits { ms } => {
            commit_hold::hold(conn as *const Conn as usize, ms);
            Response::Done
        }
        Request::Compression { codec, level, min_bytes } => {
            if codec != rshard::CODEC_ZSTD {
                return refuse(*b"0A000", true, format!("compression codec {codec} is not spoken here"));
            }
            let level = if level == 0 { 3 } else { level.clamp(-7, 22) };
            *conn.compress.lock().unwrap() = Some((level, min_bytes.max(64)));
            Response::Done
        }
        Request::ReleaseCommits => {
            commit_hold::release(conn as *const Conn as usize);
            Response::Done
        }
        Request::Decision { gid } => match decision(&gid) {
            Ok(commit) => Response::Decided { commit },
            Err(e) => refuse(*b"58030", true, e),
        },
        // B4.2: the gateway's deadlock detector.
        Request::Waits => Response::Waits { edges: super::deadlock::waits(&conn.catalog) },
        Request::AbortTxn { gid } => {
            super::deadlock::abort(&gid);
            Response::Done
        }
        // B4.3: the gateway's LISTEN hub.
        Request::Notifications { database, since } => match conn.catalog.get(&database) {
            Some(db) => {
                let (next, lost, notes) = crate::pgwire::notify::outbox_since(db.id, since);
                Response::Notifications { next, lost, notes: notes.into_iter().map(|n| (n.pid, n.channel, n.payload)).collect() }
            }
            None => refuse(*b"3D000", true, format!("database \"{database}\" does not exist")),
        },
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

/// The anchor's two-phase-commit decision log (sharding-plan.md B5):
/// `commit gid` / `abort gid` lines, appended and synced. The first decision
/// recorded for a gid is the decision; a later contrary one is refused.
const DECISIONS: &str = "rdb_2pc_decisions";

pub(crate) fn decision(gid: &str) -> Result<Option<bool>, String> {
    let dir = super::ownership::data_dir().ok_or("this shard has no data directory")?;
    let path = dir.join(DECISIONS);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    for line in text.lines() {
        if let Some((verb, g)) = line.split_once(' ')
            && g == gid
        {
            return Ok(Some(verb == "commit"));
        }
    }
    Ok(None)
}

/// The owner of holds made by SQL (`SHARD HOLD COMMITS`): one, shared by
/// every session, as a backup's cut is one at a time.
const ADMIN_HOLD: usize = usize::MAX;

/// `SHARD HOLD COMMITS <ms>`: commit decisions here wait until released or
/// `ms` pass (capped at 5 s, so a tool that dies cannot hold them for good).
pub(crate) fn hold_commits_admin(ms: u32) {
    commit_hold::hold(ADMIN_HOLD, ms);
}

pub(crate) fn release_commits_admin() {
    commit_hold::release(ADMIN_HOLD);
}

/// Waits out every hold in force.
pub(crate) async fn wait_commit_hold() {
    commit_hold::wait().await;
}

pub(crate) fn decide(gid: &str, commit: bool) -> Result<(), String> {
    use std::io::Write;
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _held = LOCK.lock().unwrap();
    if gid.is_empty() || gid.contains('\n') {
        return Err("a malformed global transaction id".to_string());
    }
    match decision(gid)? {
        Some(c) if c == commit => return Ok(()),
        Some(c) => return Err(format!("{gid} is already decided {}", if c { "commit" } else { "abort" })),
        None => {}
    }
    let dir = super::ownership::data_dir().ok_or("this shard has no data directory")?;
    let path = dir.join(DECISIONS);
    let io = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(io)?;
    f.write_all(format!("{} {gid}\n", if commit { "commit" } else { "abort" }).as_bytes()).map_err(io)?;
    f.sync_all().map_err(io)?;
    crate::core::sync_path::sync_dir(dir).map_err(io)?;
    Ok(())
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
            upstream_password: None,
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
            ready_left: 0,
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
/// Whether `bytes` are CopyData messages and nothing else.
fn only_copy_data(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i + 5 <= bytes.len() {
        if bytes[i] != b'd' {
            return false;
        }
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap()).max(4) as usize;
        i += 1 + len;
    }
    i == bytes.len() && !bytes.is_empty()
}

/// How many ReadyForQuery messages the whole frontend messages `bytes` are
/// answered with: one per Query and one per Sync.
fn ready_owed(bytes: &[u8]) -> usize {
    let mut i = 0;
    let mut n = 0;
    while i + 5 <= bytes.len() {
        if matches!(bytes[i], b'Q' | b'S') {
            n += 1;
        }
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap()).max(4) as usize;
        i += 1 + len;
    }
    n
}

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
    // A keyless aggregate, top-N or DISTINCT the gateway sent here to
    // coordinate (B1c): a simple Query, or one extended-protocol statement
    // (Parse, Bind, Describe, Execute, Sync), whose fold works the same.
    let (sql, extended) = match simple_query_text(bytes) {
        Some(sql) => (Some(sql), false),
        None => (extended_statement_text(bytes), true),
    };
    if let Some(sql) = sql
        && may_coordinate(&sql)
        && let Some(guard) = conn.catalog.connect(&s.database)
    {
        let source = if extended { rshard::Source::Parse } else { rshard::Source::Shape };
        let template = super::classify::classify(guard.database(), source, &sql);
        drop(guard);
        if template == (rshard::RouteTemplate::FanOut { merge: Merge::Coordinator, writes: false }) {
            // A fold, a top-N or a DISTINCT: the classifier sends all three.
            if extended
                && !may_aggregate_fold(&sql)
                && let Some(st) = super::topn_ext::statement(bytes)
            {
                if is_plain_distinct(&sql) {
                    return coordinate_distinct_ext(session, &mut s, bytes).await;
                }
                let status = s.bridge.status;
                return match super::topn_ext::plan(&st) {
                    Some(Ok(plan)) => coordinate_topn_ext(session, &mut s, &st, plan, bytes).await,
                    Some(Err(e)) => {
                        let (code, msg) = split_code(&e);
                        Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply(code, msg, status) }
                    }
                    None => {
                        let msg = "this ORDER BY or DISTINCT is not one a merge across shards handles: give it a route key";
                        Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply("0A000", msg, status) }
                    }
                };
            }
            if !may_aggregate_fold(&sql) && is_plain_distinct(&sql) {
                return coordinate_distinct(session, &mut s, &sql).await;
            }
            // A top-N runs as a prepared one does: its keys fetched as extra
            // columns, so an ORDER BY column the list does not show sorts too.
            if !may_aggregate_fold(&sql) {
                let st = super::topn_ext::Statement::simple(&sql);
                if let Some(planned) = super::topn_ext::plan(&st) {
                    return match planned {
                        Ok(plan) => coordinate_topn_ext(session, &mut s, &st, plan, bytes).await,
                        Err(e) => {
                            let status = s.bridge.status;
                            let (code, msg) = split_code(&e);
                            Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply(code, msg, status) }
                        }
                    };
                }
            }
            return coordinate(session, &mut s, bytes).await;
        }
    }
    // CopyData alone has no reply (an error in it is answered at CopyDone,
    // as PostgreSQL's is): sent, and answered empty at once.
    if only_copy_data(bytes) {
        s.bridge.raw(bytes);
        return Response::PgReply { session, map_epoch: epoch(), more: false, status: 0, bytes: Vec::new() };
    }
    s.bridge.raw(bytes);
    s.finished = false;
    s.ready_left = ready_owed(bytes).max(1);
    s.pending.clear();
    read_chunk(session, &mut s).await
}

/// The most a coordinator gathers for one statement before it refuses it
/// `54000`, rather than hold every shard's rows without bound.
const COORDINATED_MAX_BYTES: usize = 256 << 20;

/// A cheap first look: only text that could be an aggregate, a top-N or a
/// DISTINCT is classified, so ordinary statements pay nothing for the
/// coordinator.
fn may_coordinate(sql: &str) -> bool {
    if may_aggregate(sql) {
        return true;
    }
    let up = sql.to_ascii_uppercase();
    up.contains("DISTINCT") || up.contains("ORDER")
}

/// The SQL of a batch that is one extended-protocol statement: `Parse`,
/// `Bind`, an optional `Describe`, `Execute`, `Sync`, in that order.
fn extended_statement_text(bytes: &[u8]) -> Option<String> {
    let mut tags = Vec::new();
    let mut text = None;
    let mut i = 0;
    while i + 5 <= bytes.len() {
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().ok()?) as usize;
        let body = bytes.get(i + 5..i + 1 + len)?;
        tags.push(bytes[i]);
        if bytes[i] == b'P' {
            let name_end = body.iter().position(|&b| b == 0)?;
            let rest = &body[name_end + 1..];
            let q_end = rest.iter().position(|&b| b == 0)?;
            text = Some(String::from_utf8(rest[..q_end].to_vec()).ok()?);
        }
        i += 1 + len;
    }
    matches!(tags.as_slice(), [b'P', b'B', b'E', b'S'] | [b'P', b'B', b'D', b'E', b'S']).then_some(text).flatten()
}

/// A cheap first look: only text that could be an aggregate is classified,
/// so ordinary statements pay nothing for the coordinator.
fn may_aggregate(sql: &str) -> bool {
    let up = sql.to_ascii_uppercase();
    ["GROUP", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "_AGG(", "BOOL_", "EVERY(", "STDDEV", "VARIANCE"]
        .iter()
        .any(|k| up.contains(k))
}

/// Marks session `s` as running a fan-out part for as long as the returned
/// guard lives.
struct PartGuard(Option<StdArc<crate::core::cancel::SessionCancel>>);

impl PartGuard {
    fn on(s: &VSession) -> PartGuard {
        let c = crate::core::cancel::session_by_pid(s.bridge.pid);
        if let Some(c) = &c {
            c.fan_out_part.store(true, Ordering::Release);
        }
        PartGuard(c)
    }
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        if let Some(c) = &self.0 {
            c.fan_out_part.store(false, Ordering::Release);
        }
    }
}

/// Whether a coordinated statement is a fold (aggregates, GROUP BY) rather
/// than a top-N.
fn may_aggregate_fold(sql: &str) -> bool {
    may_aggregate(sql)
}

/// A simple `Query` message for `sql`.
fn query_message(sql: &str) -> Vec<u8> {
    let mut m = vec![b'Q'];
    m.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
    m.extend_from_slice(sql.as_bytes());
    m.push(0);
    m
}

/// One backend message as whole bytes.
fn message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut m = vec![tag];
    m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    m.extend_from_slice(body);
    m
}

/// What a top-N part returned: its RowDescription body, its rows (each its
/// sort key and its DataRow message), and its error message, if any.
struct TopPart {
    fields: Option<Vec<u8>>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    error: Option<Vec<u8>>,
}

/// `"XXXXX: message"` as its SQLSTATE and message (`XX000` when it has none).
fn split_code(e: &str) -> (&str, &str) {
    match e.split_once(": ") {
        Some((c, m)) if c.len() == 5 && c.bytes().all(|b| b.is_ascii_alphanumeric()) => (c, m),
        _ => ("XX000", e),
    }
}

/// Runs an extended statement's top-N `plan` on session `s` (module docs of
/// `topn_ext`): its rows keyed by the appended columns, which are cut from
/// every row and from the description it returns.
async fn ext_top_part(s: &mut VSession, st: &super::topn_ext::Statement, plan: &super::topn_ext::Plan) -> Result<TopPart, String> {
    use super::topn_ext as x;
    let _part = PartGuard::on(s);
    // The client's formats need the column count written out.
    let formats = if x::needs_count(&st.results) {
        let mut d = Vec::new();
        x::parse_msg(&mut d, &plan.sql, &plan.types);
        x::describe_msg(&mut d, b'S');
        d.extend_from_slice(&[b'S', 0, 0, 0, 4]);
        s.bridge.raw(&d);
        let mut total = None;
        let mut error = None;
        loop {
            let frame = s.bridge.next().await.map_err(|e| format!("the session ended: {e}"))?;
            match frame.tag {
                b'T' => total = super::topn::fields_of(frame.body).map(|f| f.len()),
                b'E' => error = Some(message(b'E', frame.body)),
                b'Z' => break,
                _ => {}
            }
        }
        if let Some(e) = error {
            return Ok(TopPart { fields: None, rows: Vec::new(), error: Some(e) });
        }
        x::result_formats(&st.results, total.ok_or("the rewritten statement returns no rows")?, plan.extras)
    } else {
        Vec::new()
    };
    let mut b = Vec::new();
    x::parse_msg(&mut b, &plan.sql, &plan.types);
    x::bind_msg(&mut b, &plan.params, &formats);
    x::describe_msg(&mut b, b'P');
    x::execute_sync(&mut b);
    s.bridge.raw(&b);
    let mut part = TopPart { fields: None, rows: Vec::new(), error: None };
    let mut index: Option<(Vec<usize>, Vec<(String, u32)>, usize)> = None;
    loop {
        let frame = s.bridge.next().await.map_err(|e| format!("the session ended: {e}"))?;
        match frame.tag {
            b'T' => {
                let fields = super::topn::fields_of(frame.body).ok_or("a malformed RowDescription")?;
                let idx = super::topn::resolve(&plan.keys, &fields)?;
                let own = fields.len() - plan.extras;
                part.fields = Some(x::strip_description(frame.body, own).ok_or("a malformed RowDescription")?);
                index = Some((idx, fields, own));
            }
            b'D' => {
                let (idx, fields, own) = index.as_ref().ok_or("rows before their description")?;
                let cells = super::topn::cells_of(frame.body).ok_or("a malformed DataRow")?;
                let key = super::topn::sort_key(&plan.keys, idx, fields, &cells)?;
                let row = x::strip_row(frame.body, *own).ok_or("a malformed DataRow")?;
                part.rows.push((key, message(b'D', &row)));
            }
            b'E' => part.error = Some(message(b'E', frame.body)),
            b'Z' => break,
            _ => {}
        }
    }
    Ok(part)
}

/// The acknowledgements an extended statement's reply opens with: Parse
/// and Bind complete, and its description when the batch asked for one.
fn ext_head(st: &super::topn_ext::Statement, fields: &Option<Vec<u8>>) -> Vec<u8> {
    let mut head = if st.simple { Vec::new() } else { vec![b'1', 0, 0, 0, 4, b'2', 0, 0, 0, 4] };
    if st.describe {
        match fields {
            Some(f) => head.extend(message(b'T', f)),
            None => head.extend_from_slice(&[b'n', 0, 0, 0, 4]),
        }
    }
    head
}

/// A prepared top-N over every shard, coordinated here: this shard's part
/// and every peer's (each from `bytes`, the client's statement) merged by
/// key, `offset` skipped, `limit` kept, answered as the client's batch asks.
async fn coordinate_topn_ext(session: u64, s: &mut VSession, st: &super::topn_ext::Statement, plan: super::topn_ext::Plan, bytes: &[u8]) -> Response {
    let (database, role) = (s.database.clone(), s.role.clone());
    let merge = Merge::TopN { limit: plan.limit.saturating_add(plan.offset), offset: 0 };
    let (mine, peers) = tokio::join!(ext_top_part(s, st, &plan), peer_parts(&database, &role, bytes, merge));
    let status = s.bridge.status;
    let fail = |code: &str, msg: &str| Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply(code, msg, status) };
    let mine = match mine {
        Ok(p) => p,
        Err(e) => return fail("XX000", &e),
    };
    if let Some(err) = mine.error {
        let mut bytes = err;
        bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
        return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes };
    }
    let mut rows = mine.rows;
    let mut size = rows.iter().map(|(k, r)| k.len() + r.len()).sum::<usize>();
    for (id, part) in peers {
        match part {
            Ok(p) => {
                if let Some((_, m)) = first_error(&p.tail) {
                    let mut bytes = message(b'E', &m);
                    bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
                    return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes };
                }
                size += p.rows.iter().map(|(k, r)| k.len() + r.len()).sum::<usize>();
                if size > COORDINATED_MAX_BYTES {
                    return fail("54000", "a coordinated result passed 256 MiB");
                }
                rows.extend(p.rows);
            }
            Err(e) => return fail("XX000", &format!("the coordinator could not gather shard {id}'s part: {e}")),
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let kept: Vec<&Vec<u8>> = rows.iter().skip(plan.offset as usize).take(plan.limit as usize).map(|(_, r)| r).collect();
    let mut bytes = ext_head(st, &mine.fields);
    for r in &kept {
        bytes.extend_from_slice(r);
    }
    let mut tag = format!("SELECT {}", kept.len()).into_bytes();
    tag.push(0);
    bytes.extend(message(b'C', &tag));
    bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
    Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes }
}

/// A prepared `SELECT DISTINCT` over every shard: this shard's rows and
/// every peer's, each row kept once by its bytes (in the client's formats,
/// the same on every shard).
async fn coordinate_distinct_ext(session: u64, s: &mut VSession, bytes: &[u8]) -> Response {
    let (database, role) = (s.database.clone(), s.role.clone());
    let local = async {
        let _part = PartGuard::on(s);
        s.bridge.raw(bytes);
        let (mut head, mut rows, mut error) = (Vec::new(), Vec::new(), None);
        loop {
            let frame = match s.bridge.next().await {
                Ok(f) => f,
                Err(e) => return Err(format!("the session ended: {e}")),
            };
            match frame.tag {
                b'D' => rows.push(message(b'D', frame.body)),
                b'E' => error = Some(message(b'E', frame.body)),
                b'Z' => break,
                b'C' => {}
                t if rows.is_empty() => head.extend(message(t, frame.body)),
                _ => {}
            }
        }
        Ok((head, rows, error))
    };
    let (mine, peers) = tokio::join!(local, peer_parts(&database, &role, bytes, Merge::Concat));
    let status = s.bridge.status;
    let fail = |code: &str, msg: &str| Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: error_reply(code, msg, status) };
    let (head, mine, error) = match mine {
        Ok(v) => v,
        Err(e) => return fail("XX000", &e),
    };
    if let Some(err) = error {
        let mut bytes = err;
        bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
        return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes };
    }
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<Vec<u8>> = Vec::new();
    let mut size = 0usize;
    for r in mine {
        if seen.insert(r.clone()) {
            size += r.len();
            rows.push(r);
        }
    }
    for (id, part) in peers {
        match part {
            Ok(p) => {
                if let Some((_, m)) = first_error(&p.tail) {
                    let mut bytes = message(b'E', &m);
                    bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
                    return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes };
                }
                for (_, r) in p.rows {
                    if seen.insert(r.clone()) {
                        size += r.len();
                        rows.push(r);
                    }
                }
                if size > COORDINATED_MAX_BYTES {
                    return fail("54000", "a coordinated result passed 256 MiB");
                }
            }
            Err(e) => return fail("XX000", &format!("the coordinator could not gather shard {id}'s part: {e}")),
        }
    }
    let mut out = head;
    for r in &rows {
        out.extend_from_slice(r);
    }
    let mut tag = format!("SELECT {}", rows.len()).into_bytes();
    tag.push(0);
    out.extend(message(b'C', &tag));
    out.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
    Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes: out }
}

/// Runs `topn.part_sql` on this shard's session `s` and keys its rows.
async fn top_part(s: &mut VSession, topn: &super::topn::TopN) -> Result<TopPart, String> {
    let _part = PartGuard::on(s);
    s.bridge.raw(&query_message(&topn.part_sql));
    let mut part = TopPart { fields: None, rows: Vec::new(), error: None };
    let mut index: Option<(Vec<usize>, Vec<(String, u32)>)> = None;
    loop {
        let frame = s.bridge.next().await.map_err(|e| format!("the session ended: {e}"))?;
        match frame.tag {
            b'T' => {
                let fields = super::topn::fields_of(frame.body).ok_or("a malformed RowDescription")?;
                let idx = super::topn::resolve(&topn.keys, &fields)?;
                part.fields = Some(frame.body.to_vec());
                index = Some((idx, fields));
            }
            b'D' => {
                let (idx, fields) = index.as_ref().ok_or("rows before their description")?;
                let cells = super::topn::cells_of(frame.body).ok_or("a malformed DataRow")?;
                let key = super::topn::sort_key(&topn.keys, idx, fields, &cells)?;
                part.rows.push((key, message(b'D', frame.body)));
            }
            b'E' => part.error = Some(message(b'E', frame.body)),
            b'Z' => break,
            _ => {}
        }
    }
    Ok(part)
}


/// `SELECT DISTINCT …` (not `DISTINCT ON`).
fn is_plain_distinct(sql: &str) -> bool {
    let up = sql.trim_start().to_ascii_uppercase();
    let rest = up.strip_prefix("SELECT").map(str::trim_start).unwrap_or("");
    rest.starts_with("DISTINCT ") && !rest.starts_with("DISTINCT ON")
}

/// Coordinates a keyless `SELECT DISTINCT`: each shard's distinct rows,
/// deduplicated here by their bytes as returned (equal values print
/// alike within one column's type; NULLs are equal, as DISTINCT says).
async fn coordinate_distinct(session: u64, s: &mut VSession, sql: &str) -> Response {
    let Some(served) = served() else {
        return refuse(PROTOCOL, true, "this node is not a shard");
    };
    let status_of = |s: &VSession| s.bridge.status;
    // Keys are not needed: a top-N with no keys and no limit is the plain run.
    let plain = super::topn::TopN { keys: Vec::new(), limit: u64::MAX, offset: 0, part_sql: sql.to_string() };
    let request = query_message(sql);
    let (database, role) = (s.database.clone(), s.role.clone());
    let (mine, peers) = tokio::join!(top_part(s, &plain), peer_parts(&database, &role, &request, Merge::Concat));
    let mine = match mine {
        Ok(p) => p,
        Err(e) => {
            let st = status_of(s);
            return Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes: error_reply("XX000", &e, st) };
        }
    };
    if let Some(err) = mine.error {
        let st = status_of(s);
        let mut bytes = err;
        bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, st]);
        return Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes };
    }
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for (_, r) in mine.rows {
        if seen.insert(r.clone()) {
            rows.push(r);
        }
    }
    let _ = &served;
    let mut size = rows.iter().map(Vec::len).sum::<usize>();
    for (id, part) in peers {
        match part {
            Ok(p) => {
                if let Some((_, m)) = first_error(&p.tail) {
                    let st = status_of(s);
                    let mut bytes = message(b'E', &m);
                    bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, st]);
                    return Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes };
                }
                for (_, r) in p.rows {
                    if seen.insert(r.clone()) {
                        size += r.len();
                        rows.push(r);
                    }
                }
                if size > COORDINATED_MAX_BYTES {
                    let st = status_of(s);
                    return Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes: error_reply("54000", "a coordinated result passed 256 MiB", st) };
                }
            }
            Err(e) => {
                let st = status_of(s);
                let msg = format!("the coordinator could not gather shard {id}'s part: {e}");
                return Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes: error_reply("XX000", &msg, st) };
            }
        }
    }
    let mut bytes = Vec::new();
    if let Some(f) = &mine.fields {
        bytes.extend(message(b'T', f));
    }
    for r in &rows {
        bytes.extend_from_slice(r);
    }
    let mut tag = format!("SELECT {}", rows.len()).into_bytes();
    tag.push(0);
    bytes.extend(message(b'C', &tag));
    let st = status_of(s);
    bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, st]);
    Response::PgReply { session, map_epoch: epoch(), more: false, status: st, bytes }
}

/// The first ErrorResponse among `bytes`' messages: its tag and body.
pub(crate) fn first_error(bytes: &[u8]) -> Option<(u8, Vec<u8>)> {
    let mut i = 0;
    while i + 5 <= bytes.len() {
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().ok()?) as usize;
        if bytes[i] == b'E' {
            return Some((b'E', bytes.get(i + 5..i + 1 + len)?.to_vec()));
        }
        i += 1 + len;
    }
    None
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
async fn peer_client(id: u32) -> Result<StdArc<PeerLink>, String> {
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
    // Compression, when this shard asks for it and the peer agrees.
    let mut compress = None;
    if let Some(level) = peers.compression {
        let ask = Request::Compression { codec: rshard::CODEC_ZSTD, level, min_bytes: COMPRESS_MIN_BYTES };
        match client.send(&ask.encode()).await.map_err(e).and_then(|b| Response::decode(&b)) {
            Ok(Response::Done) => compress = Some(level),
            other => log::warn!("shard {id} does not compress its link: {other:?}"),
        }
    }
    let client = StdArc::new(PeerLink { client, compress });
    PEER_CLIENTS.lock().unwrap().get_or_insert_with(HashMap::new).insert(id, client.clone());
    Ok(client)
}

/// One peer's partial from its part's reply.
///
/// `Err(Ok(body))` is the peer's own ErrorResponse body, which answers for
/// the statement as it is (its SQLSTATE kept); `Err(Err(text))` is a failure
/// to get the part at all.
fn partial_of(id: u32, part: Result<PartReply, String>) -> Result<Vec<u8>, Result<Vec<u8>, String>> {
    let p = part.map_err(Err)?;
    if let Some((_, m)) = first_error(&p.tail) {
        return Err(Ok(m));
    }
    p.rows.into_iter().next().map(|(_, partial)| partial).ok_or_else(|| Err(format!("shard {id} returned no partial")))
}

/// Every other shard's part of `bytes` under `merge`, asked for at once
/// (one round trip of latency, not one per shard), in map order.
async fn peer_parts(database: &str, role: &str, bytes: &[u8], merge: Merge) -> Vec<(u32, Result<PartReply, String>)> {
    let Some(served) = served() else {
        return Vec::new();
    };
    let ids: Vec<u32> = served.map.shards().iter().map(|i| i.id).filter(|&id| id != served.shard_id).collect();
    let asks = ids.iter().map(|&id| peer_part(id, database, role, bytes, merge.clone()));
    ids.iter().copied().zip(futures::future::join_all(asks).await).collect()
}

/// One peer's part of a coordinated statement, under `merge`.
///
/// The link is cached; a peer that restarted since it was made has lost
/// its Hello (or the connection), and opening the session says so before
/// anything ran there, so the link is made afresh and the open tried once
/// more.
async fn peer_part(id: u32, database: &str, role: &str, bytes: &[u8], merge: Merge) -> Result<PartReply, String> {
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
    let mut client = peer_client(id).await?;
    for attempt in 0..2 {
        let stale: Result<(), String> = match client.send(&open.encode()).await {
            Err(x) => Err(e(x)),
            Ok(reply) => match Response::decode(&reply)? {
                Response::SessionOpened { .. } => break,
                Response::Refused(r) if r.sqlstate == HELLO_FIRST => Err(format!("shard {id}: {}", r.message)),
                Response::Refused(r) => return Err(format!("shard {id}: {}", r.message)),
                other => return Err(format!("shard {id} answered OpenSession with {other:?}")),
            },
        };
        if let Err(why) = stale {
            if let Some(m) = PEER_CLIENTS.lock().unwrap().as_mut() {
                m.remove(&id);
            }
            if attempt == 1 {
                return Err(why);
            }
            client = peer_client(id).await?;
        }
    }
    let req = Request::ExecPart { session, map_epoch: epoch(), merge, bytes: bytes.to_vec() };
    let answer = Response::decode(&client.send(&req.encode()).await.map_err(e)?);
    let _ = client.send(&Request::CloseSession { session }.encode()).await;
    match answer? {
        Response::PartReply(p) => Ok(p),
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
    if served().is_none() {
        return refuse(PROTOCOL, true, "this node is not a shard");
    }
    let mut partials = Vec::new();
    for (id, part) in peer_parts(&s.database, &s.role, bytes, Merge::Coordinator).await {
        match partial_of(id, part) {
            Ok(p) => partials.push(p),
            Err(Ok(body)) => {
                let status = s.bridge.status;
                let mut bytes = message(b'E', &body);
                bytes.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
                return Response::PgReply { session, map_epoch: epoch(), more: false, status, bytes };
            }
            Err(Err(e)) => {
                let msg = format!("the coordinator could not gather shard {id}'s part: {e}");
                return Response::PgReply { session, map_epoch: epoch(), more: false, status: s.bridge.status, bytes: error_reply("XX000", &msg, s.bridge.status) };
            }
        }
    }
    merge_partials(session, s, partials, bytes).await
}

/// `Request::Coordinate`: the aggregate run on the gateway's session
/// `session` with the other shards' partials it gathered itself, inside the
/// transaction their parts ran in.
async fn coordinate_given(conn: &Conn, session: u64, partials: Vec<Vec<u8>>, bytes: &[u8]) -> Response {
    if served().is_none() {
        return refuse(PROTOCOL, true, "this node is not a shard");
    }
    if simple_query_text(bytes).is_none() && extended_statement_text(bytes).is_none() {
        return refuse(PROTOCOL, true, "a Coordinate request carries one statement: a simple Query, or Parse, Bind, Describe, Execute, Sync");
    }
    let Some(s) = session_of(conn, session) else {
        return refuse(SESSION_GONE, true, format!("session {session} is gone"));
    };
    let mut s = s.lock().await;
    s.finished = true;
    merge_partials(session, &mut s, partials, bytes).await
}

/// Runs `bytes` on `s` as the coordinator, `partials` merged into its fold.
async fn merge_partials(session: u64, s: &mut VSession, partials: Vec<Vec<u8>>, bytes: &[u8]) -> Response {
    use crate::core::cancel::Fold;
    let Some(cancel) = crate::core::cancel::session_by_pid(s.bridge.pid) else {
        return refuse(*b"XX000", true, "the coordinating session is not registered");
    };
    *cancel.fold.lock().unwrap() = Fold::Coordinator { partials, consumed: false };
    let _part = PartGuard::on(s);
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
        let msg = "the statement was not answered by a fold the coordinator could merge the other shards into; it is refused rather than answered from one shard";
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
        if tag == b'G' {
            s.finished = true;
        } else if tag == b'Z' {
            s.ready_left = s.ready_left.saturating_sub(1);
            s.finished = s.ready_left == 0;
        }
    }
    let status = if s.finished && out.len() >= 6 && out[out.len() - 6] == b'Z' { out[out.len() - 1] } else { 0 };
    Response::PgReply { session, map_epoch: epoch(), more: !s.finished, status, bytes: out }
}

/// One shard's part of a fan-out: the statement run as a Pg batch, its
/// DataRows split out for the gateway to concatenate or sum.
async fn exec_part(conn: &Conn, session: u64, merge: Merge, bytes: &[u8]) -> Response {
    if let Merge::TopN { .. } = merge {
        // A top-N's part: the statement rewritten so its keys come back in
        // text whatever formats the client asked for (module docs of
        // `topn_ext`); a simple Query the same way, in text.
        let st = match simple_query_text(bytes) {
            Some(sql) => super::topn_ext::Statement::simple(&sql),
            None => match super::topn_ext::statement(bytes) {
                Some(st) => st,
                None => return refuse(PROTOCOL, true, "a top-N part is one statement: a simple Query, or Parse, Bind, Describe, Execute, Sync"),
            },
        };
        let plan = match super::topn_ext::plan(&st) {
            Some(Ok(p)) => p,
            Some(Err(e)) => return refuse(*b"0A000", true, e),
            None => return refuse(*b"0A000", true, "not a top-N statement"),
        };
        let Some(s) = session_of(conn, session) else {
            return refuse(SESSION_GONE, true, format!("session {session} is gone"));
        };
        let mut s = s.lock().await;
        return match ext_top_part(&mut s, &st, &plan).await {
            Ok(p) => {
                let head = ext_head(&st, &p.fields);
                let status = s.bridge.status;
                let mut tail = p.error.unwrap_or_default();
                tail.extend_from_slice(&[b'Z', 0, 0, 0, 5, status]);
                Response::PartReply(PartReply { session, map_epoch: epoch(), more: false, status, head, rows: p.rows, tail })
            }
            Err(e) => refuse(*b"XX000", false, e),
        };
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
    let _part = PartGuard::on(&s);
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

/// The anchor shard's commit hold (B6.1): while a consistent fan-out starts
/// every part's snapshot, multi-shard commit decisions wait, so each such
/// transaction is wholly inside the snapshot or wholly outside it. A hold
/// belongs to the rshard connection that took it and ends at its release or
/// its expiry (at most 5 s), so a gateway that vanished stalls no one.
mod commit_hold {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    static HOLDS: Mutex<Option<HashMap<usize, Instant>>> = Mutex::new(None);

    pub(super) fn hold(owner: usize, ms: u32) {
        let until = Instant::now() + Duration::from_millis(u64::from(ms.min(5_000)));
        HOLDS.lock().unwrap_or_else(|e| e.into_inner()).get_or_insert_with(HashMap::new).insert(owner, until);
    }

    pub(super) fn release(owner: usize) {
        if let Some(h) = HOLDS.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            h.remove(&owner);
        }
    }

    /// The latest expiry of the holds still in force, dropping the expired.
    fn until() -> Option<Instant> {
        let mut g = HOLDS.lock().unwrap_or_else(|e| e.into_inner());
        let h = g.as_mut()?;
        let now = Instant::now();
        h.retain(|_, until| *until > now);
        h.values().max().copied()
    }

    pub(super) async fn wait() {
        while let Some(until) = until() {
            let left = until.saturating_duration_since(Instant::now());
            tokio::time::sleep(left.min(Duration::from_millis(2))).await;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn b61_a_decision_waits_for_a_hold_and_no_longer() {
            hold(1, 5_000);
            let start = Instant::now();
            let waiter = tokio::spawn(wait());
            tokio::time::sleep(Duration::from_millis(30)).await;
            release(1);
            waiter.await.unwrap();
            assert!(start.elapsed() >= Duration::from_millis(25) && start.elapsed() < Duration::from_secs(2));
            hold(2, 20);
            let start = Instant::now();
            wait().await;
            assert!(start.elapsed() < Duration::from_secs(1), "an expired hold ends the wait");
        }
    }
}
