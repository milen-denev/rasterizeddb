//! `rasterizeddb_gateway_enterprise`: the gateway with per-user shards.
//! Takes the gateway's own flags plus `--user-routes FILE` (and
//! `--user-routes-require` to refuse unmapped logins). The file is reread
//! within five seconds of a change; each role's next login sees it.

use std::sync::Arc;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut routes_path = None;
    let mut require = false;
    let mut rest = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--user-routes" => routes_path = args.next(),
            "--user-routes-require" => require = true,
            _ => rest.push(a),
        }
    }
    let cfg = match rasterizeddb_gateway::config::Config::from_args(rest.into_iter()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let Some(path) = routes_path else {
        eprintln!("--user-routes FILE is required");
        std::process::exit(2);
    };
    let routes = match rasterizeddb_gateway_enterprise::Routes::open(std::path::Path::new(&path), require) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    {
        let routes = routes.clone();
        let path = std::path::PathBuf::from(&path);
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
    let hooks = rasterizeddb_gateway::hooks::Hooks { user_routing: routes, has_user_routing: true };
    if let Err(e) = rasterizeddb_gateway::run_with(cfg, hooks) {
        eprintln!("rasterizeddb_gateway_enterprise: {e}");
        std::process::exit(1);
    }
}
