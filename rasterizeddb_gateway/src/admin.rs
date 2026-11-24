//! `SHOW GATEWAY` and the metrics endpoint (§4.9 G10).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::state::Shared;

/// The rows `SHOW GATEWAY` answers: configuration, map, then counters.
pub fn show_gateway(shared: &Shared) -> Vec<(String, String)> {
    let map = shared.map.get();
    let c = shared.board.sum();
    let mut rows: Vec<(String, String)> = vec![
        ("gateway".into(), shared.cfg.gateway_name.clone()),
        ("version".into(), env!("CARGO_PKG_VERSION").into()),
        ("cluster_id".into(), format!("{:032x}", map.cluster_id())),
        ("map_epoch".into(), map.epoch().to_string()),
        ("map_digest".into(), format!("{:016x}", map.digest())),
        ("shards".into(), map.shards().len().to_string()),
        ("home_shard".into(), shared.home(&map).to_string()),
        ("schema_epoch".into(), shared.schema_epoch.load(Ordering::Relaxed).to_string()),
        ("workers".into(), shared.cfg.worker_count().to_string()),
        ("sessions".into(), shared.sessions.load(Ordering::Relaxed).to_string()),
        ("draining".into(), shared.draining.load(Ordering::Relaxed).to_string()),
        (
            "unrouted".into(),
            match shared.cfg.unrouted {
                crate::config::Unrouted::Refuse => "refuse".into(),
                crate::config::Unrouted::FanOut => "fan_out".into(),
            },
        ),
        ("fanout_built".into(), cfg!(feature = "fanout").to_string()),
        ("user_routing_built".into(), shared.hooks.has_user_routing.to_string()),
    ];
    for (k, v) in c.rows() {
        rows.push((k.to_string(), v));
    }
    rows
}

/// A minimal HTTP endpoint: `GET /metrics` in the Prometheus text format,
/// anything else the `SHOW GATEWAY` rows as plain text.
pub async fn serve(shared: Arc<Shared>, addr: std::net::SocketAddr) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("admin endpoint {addr}: {e}");
            return;
        }
    };
    loop {
        let Ok((mut s, _)) = listener.accept().await else { continue };
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let n = match tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let metrics = req.starts_with("GET /metrics");
            let mut body = String::new();
            for (k, v) in show_gateway(&shared) {
                if metrics {
                    if v.parse::<f64>().is_ok() {
                        body.push_str(&format!("rdb_gateway_{k} {v}\n"));
                    }
                } else {
                    body.push_str(&format!("{k}\t{v}\n"));
                }
            }
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes()).await;
        });
    }
}
