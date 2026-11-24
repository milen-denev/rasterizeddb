//! `rasterizeddb_gateway`: see docs/gateway.md.

#![forbid(unsafe_code)]

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cfg = match rasterizeddb_gateway::config::Config::from_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = rasterizeddb_gateway::run(cfg) {
        eprintln!("rasterizeddb_gateway: {e}");
        std::process::exit(1);
    }
}
