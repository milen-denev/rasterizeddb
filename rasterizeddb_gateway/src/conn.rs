//! A client connection: startup, TLS, CancelRequest, authentication, then
//! the session (§4.9 G9, R8).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use rasterizeddb_router::rshard::{AuthKeys, Request, Response};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::config::{AuthMode, UserRoutingMode};
use crate::core::Core;
use crate::hooks::Pin as UserPin;
use crate::pg::{self, MsgReader};
use crate::scram;
use crate::session::{Login, Session};

/// Catches a panic in one session so it ends that session alone (R8).
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = &mut self.0;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.as_mut().poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

/// Serves one accepted connection to its end.
pub async fn handle(core: Rc<Core>, stream: TcpStream, shutdown: tokio::sync::watch::Receiver<bool>) {
    let _ = stream.set_nodelay(true);
    let shared = core.shared.clone();
    if shared.sessions.fetch_add(1, Ordering::AcqRel) >= shared.cfg.max_sessions {
        shared.sessions.fetch_sub(1, Ordering::AcqRel);
        let mut s = stream;
        let mut out = Vec::new();
        pg::error(&mut out, "FATAL", "53300", "sorry, too many clients already");
        let _ = pg::write_all(&mut s, &out).await;
        return;
    }
    core.counters.borrow_mut().sessions_opened += 1;
    if let Err(e) = startup(core.clone(), stream, shutdown).await
        && e.kind() != io::ErrorKind::UnexpectedEof
    {
        log::debug!("connection ended: {e}");
    }
    core.counters.borrow_mut().sessions_closed += 1;
    shared.sessions.fetch_sub(1, Ordering::AcqRel);
}

async fn startup(core: Rc<Core>, mut stream: TcpStream, shutdown: tokio::sync::watch::Receiver<bool>) -> io::Result<()> {
    let mut reader = MsgReader::new(8 << 10, core.shared.cfg.max_message_bytes);
    loop {
        let (code, rest) = reader.startup(&mut stream).await?;
        match code {
            pg::SSL_REQUEST => {
                #[cfg(feature = "tls")]
                if let Some(tls) = &core.shared.tls {
                    pg::write_all(&mut stream, b"S").await?;
                    // Bytes sent before the handshake would be read as if
                    // they had come over TLS (CVE-2021-23214's shape).
                    if !reader.is_empty() {
                        return Err(pg::protocol("data received before the TLS handshake"));
                    }
                    let end_point = tls.end_point;
                    let tls_stream = tls.acceptor.accept(stream).await?;
                    let mut s = tls_stream;
                    return after_tls(core, &mut s, Some(end_point), shutdown).await;
                }
                pg::write_all(&mut stream, b"N").await?;
            }
            pg::GSSENC_REQUEST => pg::write_all(&mut stream, b"N").await?,
            pg::CANCEL_REQUEST => {
                if rest.len() >= 8 {
                    let pid = i32::from_be_bytes(rest[0..4].try_into().unwrap());
                    let secret = i32::from_be_bytes(rest[4..8].try_into().unwrap());
                    cancel(&core, pid, secret).await;
                }
                return Ok(());
            }
            pg::PROTOCOL_3 => return login(core, &mut stream, reader, rest, None, shutdown).await,
            other => {
                let mut out = Vec::new();
                pg::error(&mut out, "FATAL", "08P01", &format!("unsupported frontend protocol {}.{}", other >> 16, other & 0xFFFF));
                pg::write_all(&mut stream, &out).await?;
                return Ok(());
            }
        }
    }
}

#[cfg(feature = "tls")]
async fn after_tls<S: AsyncRead + AsyncWrite + Unpin>(
    core: Rc<Core>,
    s: &mut S,
    cbind: Option<[u8; 32]>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let mut reader = MsgReader::new(8 << 10, core.shared.cfg.max_message_bytes);
    loop {
        let (code, rest) = reader.startup(s).await?;
        match code {
            pg::PROTOCOL_3 => return login(core, s, reader, rest, cbind, shutdown).await,
            pg::GSSENC_REQUEST | pg::SSL_REQUEST => pg::write_all(s, b"N").await?,
            _ => return Err(pg::protocol("unexpected startup packet after TLS")),
        }
    }
}

/// A CancelRequest: every shard running a statement for that session is
/// asked to cancel it (B1a.7).
async fn cancel(core: &Core, pid: i32, secret: i32) {
    for (shard, key) in core.shared.cancels.targets((pid, secret)) {
        if let Ok(link) = core.link(shard) {
            let _ = link.call(core, &Request::Cancel { backend_key: key }.encode()).await;
        }
    }
}

async fn login<S: AsyncRead + AsyncWrite + Unpin>(
    core: Rc<Core>,
    s: &mut S,
    mut reader: MsgReader,
    rest: Vec<u8>,
    cbind: Option<[u8; 32]>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let shared = core.shared.clone();
    let mut out = Vec::new();
    if shared.draining.load(Ordering::Relaxed) {
        pg::error(&mut out, "FATAL", "57P03", "the gateway is shutting down");
        return pg::write_all(s, &out).await;
    }
    let all = pg::startup_params(&rest);
    let get = |k: &str| all.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    let Some(role) = get("user") else {
        pg::error(&mut out, "FATAL", "28000", "no PostgreSQL user name specified in startup packet");
        return pg::write_all(s, &out).await;
    };
    let database = get("database").unwrap_or_else(|| role.clone());
    if get("replication").is_some_and(|v| v != "false" && v != "0") {
        pg::error(&mut out, "FATAL", "0A000", "replication connections are not supported through the gateway");
        return pg::write_all(s, &out).await;
    }

    if shared.cfg.auth == AuthMode::Scram {
        match authenticate(&core, s, &mut reader, &database, &role, cbind.as_ref()).await? {
            Ok(()) => {}
            Err(msg) => {
                core.counters.borrow_mut().auth_failures += 1;
                pg::error(&mut out, "FATAL", if msg.starts_with("08006") { "08006" } else { "28P01" }, &msg);
                return pg::write_all(s, &out).await;
            }
        }
    }
    pg::put(&mut out, b'R', &0i32.to_be_bytes());

    // G11: the login's shard, once.
    let pinned = match shared.cfg.user_routing {
        UserRoutingMode::Off => None,
        mode => match shared.hooks.user_routing.pin(&database, &role) {
            UserPin::Shard(id) => Some(id),
            UserPin::Refuse(m) => {
                pg::error(&mut out, "FATAL", "28000", &m);
                return pg::write_all(s, &out).await;
            }
            UserPin::None if mode == UserRoutingMode::Require => {
                pg::error(&mut out, "FATAL", "28000", &format!("role \"{role}\" is not mapped to a shard on this gateway"));
                return pg::write_all(s, &out).await;
            }
            UserPin::None => None,
        },
    };

    for (k, v) in [
        ("server_version", shared.cfg.server_version.as_str()),
        ("server_encoding", "UTF8"),
        ("client_encoding", get("client_encoding").as_deref().unwrap_or("UTF8")),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", get("TimeZone").as_deref().unwrap_or("UTC")),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("is_superuser", "off"),
        ("session_authorization", role.as_str()),
        ("application_name", get("application_name").as_deref().unwrap_or("")),
    ] {
        pg::parameter_status(&mut out, k, v);
    }
    let key_bytes = scram::random_bytes::<8>();
    let pid = i32::from_be_bytes(key_bytes[0..4].try_into().unwrap()) & 0x7FFF_FFFF;
    let secret = i32::from_be_bytes(key_bytes[4..8].try_into().unwrap());
    let mut kd = pid.to_be_bytes().to_vec();
    kd.extend_from_slice(&secret.to_be_bytes());
    pg::put(&mut out, b'K', &kd);
    pg::ready(&mut out, b'I');
    pg::write_all(s, &out).await?;

    let cancel = shared.cancels.register((pid, secret));
    let params: Vec<(String, String)> =
        all.into_iter().filter(|(k, _)| k != "user" && k != "database").collect();
    let mut session = Session::new(core.clone(), Login { database, role, params, pinned, cancel });
    let result = CatchUnwind(Box::pin(session.serve(s, &mut reader, shutdown))).await;
    shared.cancels.remove((pid, secret));
    match result {
        Ok(r) => r,
        Err(()) => {
            core.counters.borrow_mut().panics += 1;
            log::error!("a session panicked; it alone was ended");
            let mut out = Vec::new();
            pg::error(&mut out, "FATAL", "XX000", "internal error in the gateway; the session was ended");
            let _ = pg::write_all(s, &out).await;
            Ok(())
        }
    }
}

/// SCRAM-SHA-256 against the role's keys. `Ok(Err(msg))` is a failed
/// login, already counted by the caller; `Err` is the connection failing.
async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    core: &Core,
    s: &mut S,
    reader: &mut MsgReader,
    database: &str,
    role: &str,
    cbind: Option<&[u8; 32]>,
) -> io::Result<Result<(), String>> {
    let shared = &core.shared;
    let keys = match shared.auth.get(database, role) {
        Some(k) => k,
        None => match fetch_keys(core, database, role).await {
            Ok(k) => {
                shared.auth.put(database, role, k.clone());
                k
            }
            Err(e) => return Ok(Err(format!("08006: the home shard cannot be reached to authenticate: {e}"))),
        },
    };
    let verifier = if keys.found && keys.can_login {
        scram::Verifier {
            iterations: keys.iterations,
            salt: keys.salt.clone(),
            stored_key: keys.stored_key,
            server_key: keys.server_key,
            real: true,
        }
    } else {
        scram::Verifier::fake(role, &shared.secret)
    };

    // AuthenticationSASL: the mechanisms, -PLUS first over TLS.
    let mut body = 10i32.to_be_bytes().to_vec();
    if cbind.is_some() {
        body.extend_from_slice(scram::MECHANISM_PLUS.as_bytes());
        body.push(0);
    }
    body.extend_from_slice(scram::MECHANISM.as_bytes());
    body.extend_from_slice(&[0, 0]);
    let mut out = Vec::new();
    pg::put(&mut out, b'R', &body);
    pg::write_all(s, &out).await?;

    let (t, m) = reader.next(s).await?;
    if t != b'p' {
        return Ok(Err("expected a SASLInitialResponse".into()));
    }
    let b = pg::body(m);
    let Some((mech, rest)) = pg::cstr(b) else { return Ok(Err("malformed SASLInitialResponse".into())) };
    let mech = mech.to_string();
    let len = rest.get(0..4).map(|l| i32::from_be_bytes(l.try_into().unwrap())).unwrap_or(-1);
    if len < 0 || rest.len() < 4 + len as usize {
        return Ok(Err("malformed SASLInitialResponse".into()));
    }
    let Ok(client_first) = std::str::from_utf8(&rest[4..4 + len as usize]).map(str::to_string) else {
        return Ok(Err("SCRAM message is not UTF-8".into()));
    };
    let (ex, server_first) = match scram::server_first(&mech, &client_first, &verifier, cbind, &scram::nonce()) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let mut body = 11i32.to_be_bytes().to_vec();
    body.extend_from_slice(server_first.as_bytes());
    out.clear();
    pg::put(&mut out, b'R', &body);
    pg::write_all(s, &out).await?;

    let (t, m) = reader.next(s).await?;
    if t != b'p' {
        return Ok(Err("expected a SASLResponse".into()));
    }
    let Ok(client_final) = std::str::from_utf8(pg::body(m)).map(str::to_string) else {
        return Ok(Err("SCRAM message is not UTF-8".into()));
    };
    match scram::server_final(&ex, &client_final, &verifier, cbind) {
        Ok(server_final) => {
            let mut body = 12i32.to_be_bytes().to_vec();
            body.extend_from_slice(server_final.as_bytes());
            out.clear();
            pg::put(&mut out, b'R', &body);
            pg::write_all(s, &out).await?;
            Ok(Ok(()))
        }
        Err(_) => {
            // A cached key may be stale: the next attempt fetches afresh.
            shared.auth.forget(database, role);
            Ok(Err(format!("password authentication failed for user \"{role}\"")))
        }
    }
}

async fn fetch_keys(core: &Core, database: &str, role: &str) -> Result<AuthKeys, String> {
    let map = core.map();
    let home = core.shared.home(&map);
    let link = core.link(home)?;
    let req = Request::AuthFetch { database: database.to_string(), role: role.to_string() }.encode();
    match link.call(core, &req).await {
        Ok(Response::AuthKeys(k)) => Ok(k),
        Ok(Response::Refused(r)) => Err(format!("{} {}", r.sqlstate_str(), r.message)),
        Ok(other) => Err(format!("unexpected reply kind {:#04x} to AuthFetch", other.kind())),
        Err(e) => Err(format!("{e:?}")),
    }
}
