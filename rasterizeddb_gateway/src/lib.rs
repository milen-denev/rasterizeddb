//! RasterizedDB's sharding gateway (docs/gateway.md; sharding-plan.md §4.9).
//!
//! A lean binary between PostgreSQL clients and the shards. It reads a
//! statement's framing, finds its route key without parsing SQL (an explicit
//! key, a Bind parameter, or a literal slot that a shard's classification
//! named once per statement shape), hashes it with `rasterizeddb_router`,
//! and relays the client's bytes to a virtual session on the owning shard.
//! Replies come back as bytes too: no row is decoded and none re-encoded.
//!
//! - **No engine.** No `rasterizeddb_core`: no parser, no catalogue, none of
//!   the engine's statics. Shards classify, execute and enforce ownership,
//!   so the gateway is never trusted for correctness (R1).
//! - **Thread per core.** Each worker is a single-threaded runtime with its
//!   own `SO_REUSEPORT` listener, mux connections, template cache, counters
//!   and buffers. A client stays on one core for life.
//! - **Hooks.** [`run_with`] takes [`hooks::Hooks`], through which a
//!   separately built crate installs per-user shards (G11).

#![forbid(unsafe_code)]

pub mod admin;
pub mod config;
pub mod conn;
pub mod core;
pub mod counters;
pub mod fanout;
pub mod hooks;
mod kv;
#[cfg(feature = "native")]
pub mod native;
pub mod pg;
pub mod relay;
pub mod route;
pub mod scram;
pub mod session;
pub mod state;

use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rasterizeddb_router::rshard::{Request, Response};
use rasterizeddb_router::shard_map::{BINARY_MAGIC, ShardMap};

use crate::config::{Config, Relay, UserRoutingMode};
use crate::counters::Board;
use crate::hooks::Hooks;
use crate::state::{AuthCache, CancelRegistry, MapHolder, Shared, SharedTemplates};

/// Runs the gateway with no hooks installed.
pub fn run(cfg: Config) -> Result<(), String> {
    run_with(cfg, Hooks::default())
}

/// Runs the gateway until SIGTERM or SIGINT, then drains (R10).
pub fn run_with(cfg: Config, hooks: Hooks) -> Result<(), String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if let Relay::Tcp(addr) = &cfg.relay {
        return relay::run(&cfg, addr.clone());
    }
    if cfg.user_routing != UserRoutingMode::Off && !hooks.has_user_routing {
        return Err("user_routing needs a build with per-user shards (rasterizeddb_gateway_enterprise); this build has none".into());
    }

    let (map, epoch_file) = load_map(&cfg)?;
    log::info!(
        "cluster {:032x}, map epoch {}, {} shards, digest {:016x}",
        map.cluster_id(),
        map.epoch(),
        map.shards().len(),
        map.digest()
    );
    let workers = cfg.worker_count();
    #[cfg(feature = "tls")]
    let tls = client_tls(&cfg)?;
    let shared = Arc::new(Shared {
        map: MapHolder::new(map, epoch_file)?,
        templates: SharedTemplates::new(cfg.template_cache_shared),
        auth: AuthCache::new(Duration::from_secs(cfg.auth_cache_secs)),
        cancels: CancelRegistry::default(),
        board: Board::new(workers),
        hooks,
        schema_epoch: AtomicU64::new(0),
        draining: AtomicBool::new(false),
        sessions: AtomicUsize::new(0),
        secret: scram::random_bytes::<32>(),
        #[cfg(feature = "tls")]
        tls,
        cfg,
    });

    // Every listener is bound before any worker starts, so a bind error is
    // the start's error, not a worker's log line.
    let mut listeners: Vec<Vec<std::net::TcpListener>> = Vec::with_capacity(workers);
    for _ in 0..workers {
        let mut mine = Vec::new();
        for addr in &shared.cfg.listen {
            mine.push(bind_reuse_port(*addr).map_err(|e| format!("listen {addr}: {e}"))?);
        }
        listeners.push(mine);
    }

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut threads = Vec::new();
    for (id, mine) in listeners.into_iter().enumerate() {
        let shared = shared.clone();
        let stop = stop_rx.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("gw-core-{id}"))
                .spawn(move || worker(id, shared, mine, stop))
                .map_err(|e| e.to_string())?,
        );
    }

    // The admin endpoint, the native front end and the signal watcher share
    // one small runtime of their own, off the cores' paths.
    let side = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    if let Some(addr) = shared.cfg.admin {
        side.spawn(admin::serve(shared.clone(), addr));
    }
    if let Some(addr) = shared.cfg.kv_listen {
        side.spawn(kv::serve(shared.clone(), addr, shared.cfg.kv_shards.clone()));
    }
    #[cfg(feature = "native")]
    if let Some(addr) = shared.cfg.native_listen {
        side.spawn(native::serve(shared.clone(), addr, shared.cfg.native_role.clone(), shared.cfg.native_database.clone()));
    }
    side.block_on(async {
        wait_for_signal().await;
        log::info!("draining: no new connections; idle sessions close now, the rest when their transaction ends");
        shared.draining.store(true, Ordering::Release);
        let _ = stop_tx.send(true);
        let deadline = Instant::now() + shared.cfg.drain;
        while shared.sessions.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let left = shared.sessions.load(Ordering::Acquire);
    if left > 0 {
        log::warn!("drain deadline passed with {left} sessions open; they are closed");
    }
    // Workers stop accepting at the signal; their sessions end with the
    // process past the deadline.
    drop(threads);
    Ok(())
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut term), Ok(mut int)) = (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// One core: its runtime, its listeners and its sessions.
fn worker(id: usize, shared: Arc<Shared>, listeners: Vec<std::net::TcpListener>, stop: tokio::sync::watch::Receiver<bool>) {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            log::error!("core {id}: {e}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let core = Rc::new(core::Core::new(id, shared));
        // Counters reach the board about once a second (G10).
        {
            let core = core.clone();
            tokio::task::spawn_local(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    core.publish();
                }
            });
        }
        let mut accepts = Vec::new();
        for l in listeners {
            let listener = match tokio::net::TcpListener::from_std(l) {
                Ok(l) => l,
                Err(e) => {
                    log::error!("core {id}: {e}");
                    continue;
                }
            };
            let core = core.clone();
            let mut stop = stop.clone();
            accepts.push(tokio::task::spawn_local(async move {
                loop {
                    tokio::select! {
                        r = listener.accept() => match r {
                            Ok((stream, _)) => {
                                let core = core.clone();
                                let stop = stop.clone();
                                tokio::task::spawn_local(conn::handle(core, stream, stop));
                            }
                            Err(e) => {
                                // Out of descriptors and the like: back off
                                // rather than spin.
                                log::warn!("accept: {e}");
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        },
                        _ = stop.changed() => break,
                    }
                }
                // The listener closes here: a new process on the same port
                // takes the new connections (R10).
            }));
        }
        for a in accepts {
            let _ = a.await;
        }
        // Keep serving the sessions already open until the process ends.
        std::future::pending::<()>().await;
    });
}

/// A non-blocking listener with `SO_REUSEPORT`, so every core binds the
/// same port and a new process can bind beside an old one (G2, R10).
fn bind_reuse_port(addr: std::net::SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.set_tcp_nodelay(true)?;
    socket.bind(&addr.into())?;
    socket.listen(4096)?;
    Ok(socket.into())
}

/// The map from its file, or fetched from a shard (`map_from`), and the
/// file the highest epoch seen is kept in.
fn load_map(cfg: &Config) -> Result<(ShardMap, Option<String>), String> {
    if let Some(path) = &cfg.map_file {
        let bytes = std::fs::read(path).map_err(|e| format!("{path}: {e}"))?;
        let map = if bytes.starts_with(&BINARY_MAGIC) {
            ShardMap::from_binary(&bytes)?
        } else {
            ShardMap::from_text(std::str::from_utf8(&bytes).map_err(|_| format!("{path} is not UTF-8"))?)?
        };
        return Ok((map, Some(format!("{path}.epoch"))));
    }
    let addr = cfg.map_from.as_deref().ok_or("no map configured")?;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())?;
    let shared_cfg = cfg.clone();
    let map = rt.block_on(async move {
        // A throwaway Shared only to build the client with the configured
        // trust and identity.
        let client = {
            let probe = probe_shared(shared_cfg);
            core::build_client(&probe, addr)?
        };
        // `cluster_id` 0: a gateway with no map yet. The shard answers
        // HelloOk and MapFetch only.
        let hello = Request::Hello { cluster_id: 0, gateway: "bootstrap".into(), map_epoch: 0, map_digest: 0 }.encode();
        client.send(&hello).await.map_err(|e| format!("map_from {addr}: {e}"))?;
        let bytes = client.send(&Request::MapFetch.encode()).await.map_err(|e| format!("map_from {addr}: {e}"))?;
        client.close().await;
        match Response::decode(&bytes)? {
            Response::MapBytes { map, .. } => ShardMap::from_binary(&map),
            Response::Refused(r) => Err(format!("map_from {addr}: {} {}", r.sqlstate_str(), r.message)),
            other => Err(format!("map_from {addr}: unexpected reply kind {:#04x}", other.kind())),
        }
    })?;
    Ok((map, Some(format!("{}.epoch", cfg.gateway_name))))
}

fn probe_shared(cfg: Config) -> Shared {
    let placeholder = ShardMap::initial(1, vec![rasterizeddb_router::ShardInfo { id: 0, addr: String::new(), standbys: vec![] }])
        .expect("a one-shard map is valid");
    Shared {
        map: MapHolder::new(placeholder, None).expect("no epoch file"),
        templates: SharedTemplates::new(16),
        auth: AuthCache::new(Duration::from_secs(1)),
        cancels: CancelRegistry::default(),
        board: Board::new(1),
        hooks: Hooks::default(),
        schema_epoch: AtomicU64::new(0),
        draining: AtomicBool::new(false),
        sessions: AtomicUsize::new(0),
        secret: [0; 32],
        #[cfg(feature = "tls")]
        tls: None,
        cfg,
    }
}

#[cfg(feature = "tls")]
fn client_tls(cfg: &Config) -> Result<Option<state::ClientTls>, String> {
    let (Some(cert), Some(key)) = (&cfg.tls_cert, &cfg.tls_key) else { return Ok(None) };
    let certs = rastcp::tls::load_certificates(cert).map_err(|e| e.to_string())?;
    let key = rastcp::tls::load_private_key(key).map_err(|e| e.to_string())?;
    let leaf = certs.first().ok_or_else(|| format!("{cert} holds no certificate"))?;
    let end_point = scram::sha256(leaf.as_ref());
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("{cert}: {e}"))?;
    Ok(Some(state::ClientTls {
        acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
        end_point,
    }))
}
