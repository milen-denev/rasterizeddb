//! The configuration API (feature `config_api`): HTTP with a bearer token,
//! on `api_listen`, its configuration kept in a local embedded RasterizedDB
//! in `config_dir` (low-memory mode, always).
//!
//! The port is meant to be reachable only from the operator's own network
//! (firewall it from everyone else): there is no TLS of its own. Every
//! request carries `Authorization: Bearer <token>`, the token read from
//! `api_token_file` at start (only its SHA-256 is kept) and compared in
//! constant time. Every request that changes something is written to the
//! store's audit log.
//!
//! | method | path | does |
//! |---|---|---|
//! | GET | `/v1/health` | `{"ok": true}` |
//! | GET | `/v1/gateway` | `SHOW GATEWAY`'s rows as an object |
//! | GET | `/v1/map` | the shard map (epoch, shards, text) |
//! | GET | `/v1/settings` | the settings stored here |
//! | PUT | `/v1/settings/{key}` | `{"value": "…"}`: checked as a config line, stored; applied at the next start |
//! | DELETE | `/v1/settings/{key}` | forgets a stored setting |
//! | GET | `/v1/audit?limit=N` | the latest changes |
//!
//! A hook crate adds its own paths ([`ApiExtension`]); the enterprise gateway
//! adds user routes and customers.
//!
//! Stored settings are applied over the config file at start: what the API
//! set is what the gateway runs with.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::state::Shared;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A reply: HTTP status and JSON body.
pub type Reply = (u16, Value);

/// What an extension's handler gets.
pub struct ApiContext {
    pub shared: Arc<Shared>,
    pub store: Arc<rasterizeddb_client::Client>,
}

/// Paths a hook crate serves (module docs). Called for every request the
/// gateway's own paths do not answer, after the token is checked.
pub trait ApiExtension: Send + Sync {
    /// Statements run on the store at start (its own tables).
    fn schema(&self) -> Vec<String> {
        Vec::new()
    }
    /// Run once at start, after the schema: load what it keeps.
    fn load<'a>(&'a self, _store: &'a rasterizeddb_client::Client) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(&'a self, ctx: &'a ApiContext, method: &'a str, path: &'a str, query: &'a str, body: &'a Value) -> Option<BoxFuture<'a, Reply>>;
    /// The store, once open, with the runtime the API runs on: for an
    /// extension that keeps more in it (the embedded engine is one per
    /// process, so it must not open one of its own).
    fn store_ready(&self, _store: Arc<rasterizeddb_client::Client>, _rt: tokio::runtime::Handle) {}
}

/// A string as an SQL literal.
pub fn sql_text(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sha256(b: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(b).into()
}

/// The store: opened (or made) in `dir`, low-memory, its tables in place.
pub async fn open_store(dir: &std::path::Path) -> Result<(rasterizeddb_embedded::Embedded, rasterizeddb_client::Client), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let db = rasterizeddb_embedded::Builder::open(dir)
        .low_memory()
        .start()
        .await
        .map_err(|e| format!("the configuration store in {}: {e}", dir.display()))?;
    let client = db.connect().await.map_err(|e| format!("the configuration store: {e}"))?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS gateway_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at BIGINT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS api_audit (at BIGINT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, status BIGINT NOT NULL, detail TEXT NOT NULL)",
    ] {
        client.run(sql).await.map_err(|e| format!("the configuration store: {e}"))?;
    }
    Ok((db, client))
}

/// The settings stored, `(name, value)`, in name order.
pub async fn stored_settings(store: &rasterizeddb_client::Client) -> Result<Vec<(String, String)>, String> {
    let rows = store.run("SELECT name, value FROM gateway_settings ORDER BY name").await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows.iter() {
        out.push((r.get::<String>(0).map_err(|e| e.to_string())?, r.get::<String>(1).map_err(|e| e.to_string())?));
    }
    Ok(out)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Serves the API until the process ends.
pub async fn serve(
    shared: Arc<Shared>,
    addr: std::net::SocketAddr,
    token_hash: [u8; 32],
    store: Arc<rasterizeddb_client::Client>,
    ext: Option<Arc<dyn ApiExtension>>,
) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("configuration API {addr}: {e}");
            return;
        }
    };
    log::info!("configuration API on http://{addr}");
    let ctx = Arc::new(ApiContext { shared, store });
    loop {
        let Ok((mut s, peer)) = listener.accept().await else { continue };
        let ctx = ctx.clone();
        let ext = ext.clone();
        tokio::spawn(async move {
            let reply = match read_request(&mut s).await {
                Ok(req) => {
                    let r = answer(&ctx, ext.as_deref(), &token_hash, &req).await;
                    if req.method != "GET" && r.0 != 401 {
                        let detail = if r.0 < 300 { String::new() } else { r.1.to_string() };
                        let _ = ctx
                            .store
                            .run(&format!(
                                "INSERT INTO api_audit (at, method, path, status, detail) VALUES ({}, {}, {}, {}, {})",
                                now_secs(),
                                sql_text(&req.method),
                                sql_text(&req.path),
                                r.0,
                                sql_text(&format!("{peer}: {detail}"))
                            ))
                            .await;
                    }
                    r
                }
                Err((status, msg)) => (status, json!({ "error": msg })),
            };
            let body = reply.1.to_string();
            let reason = match reply.0 {
                200 => "OK",
                201 => "Created",
                204 => "No Content",
                400 => "Bad Request",
                401 => "Unauthorized",
                404 => "Not Found",
                405 => "Method Not Allowed",
                409 => "Conflict",
                413 => "Payload Too Large",
                _ => "Error",
            };
            let head = format!(
                "HTTP/1.1 {} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                reply.0,
                body.len()
            );
            let _ = s.write_all(head.as_bytes()).await;
            let _ = s.write_all(body.as_bytes()).await;
            let _ = s.shutdown().await;
        });
    }
}

pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub token: Option<String>,
    pub body: Vec<u8>,
}

/// One HTTP/1.1 request: head within 16 KiB, body within 1 MiB, in 10 s.
async fn read_request(s: &mut tokio::net::TcpStream) -> Result<Request, (u16, String)> {
    let read = async {
        let mut buf = Vec::with_capacity(4096);
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break p;
            }
            if buf.len() > 16 << 10 {
                return Err((413, "the request head is too large".to_string()));
            }
            let n = s.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
            if n == 0 {
                return Err((400, "the connection closed".to_string()));
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.lines();
        let first = lines.next().unwrap_or("");
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or("").to_ascii_uppercase();
        let target = parts.next().unwrap_or("/").to_string();
        let (path, query) = target.split_once('?').map(|(p, q)| (p.to_string(), q.to_string())).unwrap_or((target, String::new()));
        let mut length = 0usize;
        let mut token = None;
        for l in lines {
            let Some((k, v)) = l.split_once(':') else { continue };
            match k.trim().to_ascii_lowercase().as_str() {
                "content-length" => length = v.trim().parse().map_err(|_| (400, "a bad content-length".to_string()))?,
                "authorization" => token = v.trim().strip_prefix("Bearer ").map(|t| t.trim().to_string()),
                _ => {}
            }
        }
        if length > 1 << 20 {
            return Err((413, "the body is past 1 MiB".to_string()));
        }
        let mut body = buf[head_end + 4..].to_vec();
        while body.len() < length {
            let n = s.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(length);
        Ok(Request { method, path, query, token, body })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), read).await.map_err(|_| (400, "the request took too long".to_string()))?
}

async fn answer(ctx: &ApiContext, ext: Option<&dyn ApiExtension>, token_hash: &[u8; 32], req: &Request) -> Reply {
    use subtle_eq::eq;
    let given = req.token.as_deref().map(|t| sha256(t.as_bytes()));
    if !given.is_some_and(|g| eq(&g, token_hash)) {
        return (401, json!({ "error": "a bearer token is required (Authorization: Bearer <token>)" }));
    }
    let body: Value = if req.body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(e) => return (400, json!({ "error": format!("the body is not JSON: {e}") })),
        }
    };
    let path = req.path.trim_end_matches('/');
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match (req.method.as_str(), segs.as_slice()) {
        ("GET", ["v1", "health"]) => (200, json!({ "ok": true })),
        ("GET", ["v1", "gateway"]) => {
            let rows = crate::admin::show_gateway(&ctx.shared);
            (200, Value::Object(rows.into_iter().map(|(k, v)| (k, Value::String(v))).collect()))
        }
        ("GET", ["v1", "map"]) => {
            let map = ctx.shared.map.get();
            let shards: Vec<Value> = map
                .shards()
                .iter()
                .map(|s| json!({ "id": s.id, "address": s.addr, "standbys": s.standbys, "ranges": map.ranges_of(s.id).len() }))
                .collect();
            (200, json!({ "epoch": map.epoch(), "cluster_id": format!("{:032x}", map.cluster_id()), "shards": shards, "text": map.to_text() }))
        }
        ("GET", ["v1", "settings"]) => match stored_settings(&ctx.store).await {
            Ok(s) => (200, json!({ "settings": s.into_iter().map(|(k, v)| json!({ "name": k, "value": v })).collect::<Vec<_>>(), "applied": "at the next start" })),
            Err(e) => (500, json!({ "error": e })),
        },
        ("PUT", ["v1", "settings", name]) => {
            let Some(value) = body.get("value").and_then(|v| v.as_str().map(str::to_string).or_else(|| (!v.is_null()).then(|| v.to_string()))) else {
                return (400, json!({ "error": "the body is {\"value\": \"…\"}" }));
            };
            // Checked as the config file would read it.
            let mut probe = ctx.shared.cfg.clone();
            if let Err(e) = probe.set(name, &value) {
                return (400, json!({ "error": format!("{name} = {value}: {e}") }));
            }
            let sql = format!(
                "INSERT INTO gateway_settings (name, value, updated_at) VALUES ({}, {}, {}) ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                sql_text(name),
                sql_text(&value),
                now_secs()
            );
            match ctx.store.run(&sql).await {
                Ok(_) => (200, json!({ "name": name, "value": value, "applied": "at the next start" })),
                Err(e) => (500, json!({ "error": e.to_string() })),
            }
        }
        ("DELETE", ["v1", "settings", name]) => match ctx.store.run(&format!("DELETE FROM gateway_settings WHERE name = {}", sql_text(name))).await {
            Ok(r) if r.rows_affected().unwrap_or(0) > 0 => (200, json!({ "name": name, "removed": true })),
            Ok(_) => (404, json!({ "error": format!("no stored setting {name}") })),
            Err(e) => (500, json!({ "error": e.to_string() })),
        },
        ("GET", ["v1", "audit"]) => {
            let limit: u64 = req.query.split('&').find_map(|kv| kv.strip_prefix("limit=")).and_then(|v| v.parse().ok()).unwrap_or(100).min(10_000);
            match ctx.store.run(&format!("SELECT at, method, path, status, detail FROM api_audit ORDER BY at DESC LIMIT {limit}")).await {
                Ok(rows) => {
                    let list: Vec<Value> = rows
                        .iter()
                        .map(|r| {
                            json!({
                                "at": r.get::<i64>(0).unwrap_or(0),
                                "method": r.get::<String>(1).unwrap_or_default(),
                                "path": r.get::<String>(2).unwrap_or_default(),
                                "status": r.get::<i64>(3).unwrap_or(0),
                                "detail": r.get::<String>(4).unwrap_or_default(),
                            })
                        })
                        .collect();
                    (200, json!({ "audit": list }))
                }
                Err(e) => (500, json!({ "error": e.to_string() })),
            }
        }
        (method, _) => match ext.and_then(|x| x.handle(ctx, method, path, &req.query, &body)) {
            Some(f) => f.await,
            None => (404, json!({ "error": format!("no {method} {path}") })),
        },
    }
}

/// Constant-time equality of two digests.
mod subtle_eq {
    pub fn eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
        let mut d = 0u8;
        for i in 0..32 {
            d |= a[i] ^ b[i];
        }
        d == 0
    }
}

/// The SHA-256 of the token in `path` (its first line, trimmed). An empty
/// token is refused.
pub fn read_token(path: &std::path::Path) -> Result<[u8; 32], String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("api_token_file {}: {e}", path.display()))?;
    let token = text.lines().next().unwrap_or("").trim();
    if token.len() < 16 {
        return Err(format!("api_token_file {}: the token must be at least 16 characters", path.display()));
    }
    Ok(sha256(token.as_bytes()))
}

/// Runs `statements` in order on shard `shard`, in `database` as `role`, on
/// a session of its own over the gateway's mTLS link: for an extension that
/// provisions (a customer's database and role). The first error stops it.
pub async fn shard_statements(shared: &Shared, shard: u32, database: &str, role: &str, statements: &[String]) -> Result<(), String> {
    use rasterizeddb_router::rshard::{Request, Response};
    let map = shared.map.get();
    let info = map.shard(shard).ok_or_else(|| format!("shard {shard} is not in the map"))?;
    let client = crate::core::build_client(shared, &info.addr)?;
    client.connect().await.map_err(|e| format!("shard {shard}: {e}"))?;
    let call = |req: Vec<u8>| {
        let client = &client;
        async move { client.send(&req).await.map_err(|e| format!("shard {shard}: {e}")).and_then(|b| Response::decode(&b)) }
    };
    let hello = Request::Hello {
        cluster_id: map.cluster_id(),
        gateway: format!("{} api", shared.cfg.gateway_name),
        map_epoch: map.epoch(),
        map_digest: map.digest(),
    };
    match call(hello.encode()).await? {
        Response::HelloOk { .. } => {}
        Response::Refused(r) => return Err(format!("shard {shard}: {}", r.message)),
        other => return Err(format!("shard {shard} answered Hello with kind {:#04x}", other.kind())),
    }
    let session = u64::from_le_bytes(sha256(format!("{database}/{role}/{:?}", std::time::Instant::now()).as_bytes())[..8].try_into().unwrap());
    let open = Request::OpenSession {
        session,
        database: database.to_string(),
        role: role.to_string(),
        params: Vec::new(),
        gateway: shared.cfg.gateway_name.clone(),
        pinned: false,
    };
    match call(open.encode()).await? {
        Response::SessionOpened { .. } => {}
        Response::Refused(r) => return Err(format!("shard {shard}: {}", r.message)),
        other => return Err(format!("shard {shard} answered OpenSession with kind {:#04x}", other.kind())),
    }
    let mut result = Ok(());
    'statements: for sql in statements {
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        let mut req = Vec::new();
        Request::encode_pg(&mut req, session, map.epoch(), &q);
        let mut reply = call(req).await;
        loop {
            match reply {
                Ok(Response::PgReply { more, bytes, .. }) => {
                    if let Some((_, m)) = crate::pg::messages(&bytes).find(|(t, _)| *t == b'E') {
                        result = Err(format!(
                            "shard {shard}: {}: {}",
                            crate::pg::error_sqlstate(m).unwrap_or("XX000"),
                            crate::pg::error_message(m).unwrap_or_default()
                        ));
                        break 'statements;
                    }
                    if !more {
                        break;
                    }
                    reply = call(Request::Continue { session }.encode()).await;
                }
                Ok(Response::Refused(r)) => {
                    result = Err(format!("shard {shard}: {}", r.message));
                    break 'statements;
                }
                Ok(other) => {
                    result = Err(format!("shard {shard} answered with kind {:#04x}", other.kind()));
                    break 'statements;
                }
                Err(e) => {
                    result = Err(e);
                    break 'statements;
                }
            }
        }
    }
    let _ = call(Request::CloseSession { session }.encode()).await;
    result
}
