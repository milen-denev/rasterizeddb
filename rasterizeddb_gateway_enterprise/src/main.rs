//! `rasterizeddb_gateway_enterprise`: the gateway with the enterprise
//! features. Takes the gateway's own flags, plus:
//!
//! - `--user-routes FILE` (and `--user-routes-require`): per-user shards
//!   (G11). The file is reread within five seconds of a change; each role's
//!   next login sees it.
//! - `--client-stats off` turns the per-client statistics off (on by
//!   default when built with `client_stats`); `--client-stats-roles r1,r2`
//!   names the roles that may read them; `--client-stats-max-clients N`
//!   (4096) bounds how many are tracked apart.
//! - `--client-stats-dir DIR` keeps their history in an embedded
//!   RasterizedDB there (`client_stats_embedded`), an interval every
//!   `--client-stats-flush-secs` (60), for `--client-stats-retention-hours`
//!   (168).

use std::sync::Arc;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut routes_path = None;
    let mut require = false;
    let mut stats_on = cfg!(feature = "client_stats");
    let mut stats_roles: Vec<String> = Vec::new();
    let mut max_clients: usize = 4096;
    let mut stats_dir: Option<String> = None;
    let mut shard_role = "rasterized".to_string();
    let mut flush_secs: u64 = 60;
    let mut retention_hours: u64 = 168;
    let mut rest = Vec::new();
    let mut args = std::env::args().skip(1);
    let fail = |msg: String| -> ! {
        eprintln!("{msg}");
        std::process::exit(2);
    };
    let num = |name: &str, v: Option<String>| -> u64 {
        v.as_deref().and_then(|v| v.parse().ok()).unwrap_or_else(|| fail(format!("{name} takes a number")))
    };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--user-routes" => routes_path = args.next(),
            "--user-routes-require" => require = true,
            "--client-stats" => {
                stats_on = match args.next().as_deref() {
                    Some("on") => true,
                    Some("off") => false,
                    _ => fail("--client-stats takes on or off".into()),
                }
            }
            "--client-stats-roles" => {
                stats_roles = args.next().unwrap_or_default().split(',').map(|r| r.trim().to_string()).filter(|r| !r.is_empty()).collect()
            }
            "--client-stats-max-clients" => max_clients = num("--client-stats-max-clients", args.next()).max(1) as usize,
            "--client-stats-dir" => stats_dir = args.next(),
            "--client-stats-flush-secs" => flush_secs = num("--client-stats-flush-secs", args.next()).max(1),
            "--client-stats-retention-hours" => retention_hours = num("--client-stats-retention-hours", args.next()).max(1),
            "--api-shard-role" => shard_role = args.next().unwrap_or_else(|| fail("--api-shard-role takes a role".into())),
            _ => rest.push(a),
        }
    }
    let cfg = match rasterizeddb_gateway::config::Config::from_args(rest.into_iter()) {
        Ok(c) => c,
        Err(e) => fail(e.to_string()),
    };
    let mut hooks = rasterizeddb_gateway::hooks::Hooks::default();
    // Per-user routes: from a file, or kept by the configuration API in the
    // local store (config_dir).
    let routes = if let Some(path) = routes_path {
        let routes = match rasterizeddb_gateway_enterprise::Routes::open(std::path::Path::new(&path), require) {
            Ok(r) => Arc::new(r),
            Err(e) => fail(e),
        };
        watch_routes(routes.clone(), std::path::PathBuf::from(&path));
        Some(routes)
    } else if cfg.config_dir.is_some() {
        Some(Arc::new(rasterizeddb_gateway_enterprise::Routes::held(require)))
    } else {
        if require {
            fail("--user-routes-require needs --user-routes FILE or a config_dir".into());
        }
        None
    };
    if let Some(r) = &routes {
        hooks.user_routing = r.clone();
        hooks.has_user_routing = true;
    }
    #[cfg(feature = "client_stats")]
    let mut stats_handle = None;
    if stats_on {
        // One embedded engine per process: with a configuration store the
        // history is kept there (EnterpriseApi::store_ready), not in a
        // second engine on --client-stats-dir.
        let own_dir = if cfg.config_dir.is_some() {
            if let Some(d) = &stats_dir {
                log::warn!("--client-stats-dir {d} is not used: the history is kept in the configuration store (config_dir)");
            }
            None
        } else {
            stats_dir.clone()
        };
        let o = client_stats(stats_roles, max_clients, own_dir, flush_secs, retention_hours);
        #[cfg(feature = "client_stats")]
        {
            stats_handle = Some(o.clone());
        }
        hooks.observer = Some(o);
    } else if stats_dir.is_some() {
        fail("--client-stats-dir needs the client statistics on".into());
    }
    if let Some(routes) = routes.clone().filter(|_| cfg.config_dir.is_some()) {
        hooks.api = Some(Arc::new(rasterizeddb_gateway_enterprise::api::EnterpriseApi {
            routes,
            shard_role,
            #[cfg(feature = "client_stats")]
            stats: stats_handle,
            #[cfg(feature = "client_stats_embedded")]
            history: stats_on.then(|| (std::time::Duration::from_secs(flush_secs), std::time::Duration::from_secs(retention_hours * 3600))),
        }));
    }
    if let Err(e) = rasterizeddb_gateway::run_with(cfg, hooks) {
        eprintln!("rasterizeddb_gateway_enterprise: {e}");
        std::process::exit(1);
    }
}

fn watch_routes(routes: Arc<rasterizeddb_gateway_enterprise::Routes>, path: std::path::PathBuf) {
    std::thread::spawn(move || {
        let stamp = |p: &std::path::Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        let mut seen = stamp(&path);
        loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            let now = stamp(&path);
            if now != seen {
                seen = now;
                match routes.reload() {
                    Ok(()) => log::info!("user routes reloaded from {}", path.display()),
                    Err(e) => log::error!("user routes not reloaded, the old ones stay: {e}"),
                }
            }
        }
    });
}

#[cfg(feature = "client_stats")]
fn client_stats(
    roles: Vec<String>,
    max_clients: usize,
    dir: Option<String>,
    flush_secs: u64,
    retention_hours: u64,
) -> Arc<rasterizeddb_gateway_enterprise::stats::ClientStats> {
    use rasterizeddb_gateway_enterprise::stats::{ClientStats, Settings};
    if roles.is_empty() {
        log::warn!("client statistics are kept, but no role may read them: name some with --client-stats-roles");
    }
    let settings = Settings { max_clients, roles };
    let Some(dir) = dir else {
        return Arc::new(ClientStats::new(settings));
    };
    #[cfg(feature = "client_stats_embedded")]
    {
        // The history thread reads the live aggregate through a weak handle
        // (it reads only after its first interval), and the aggregate holds
        // the thread's channel.
        let stats = Arc::new_cyclic(|weak: &std::sync::Weak<ClientStats>| {
            let base = ClientStats::new(settings.clone());
            match rasterizeddb_gateway_enterprise::stats::History::start(
                std::path::PathBuf::from(&dir),
                std::time::Duration::from_secs(flush_secs),
                std::time::Duration::from_secs(retention_hours * 3600),
                weak.clone(),
            ) {
                Ok(h) => {
                    log::info!("client statistics history kept in {dir}, an interval every {flush_secs} s");
                    base.with_history(h)
                }
                Err(e) => {
                    log::error!("client statistics history not kept ({e}); the live statistics still are");
                    base
                }
            }
        });
        stats
    }
    #[cfg(not(feature = "client_stats_embedded"))]
    {
        let _ = (dir, flush_secs, retention_hours);
        eprintln!("--client-stats-dir needs a build with client_stats_embedded");
        std::process::exit(2);
    }
}

#[cfg(not(feature = "client_stats"))]
fn client_stats(_: Vec<String>, _: usize, _: Option<String>, _: u64, _: u64) -> Arc<dyn rasterizeddb_gateway::hooks::Observer> {
    eprintln!("this gateway was built without client_stats");
    std::process::exit(2);
}
