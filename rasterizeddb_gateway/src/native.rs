//! The native (rastcp) front end (B1a.8): routed by the request's `route=`
//! option, so routing reads no SQL (`rshard::parse_native_route`).
//!
//! Version 1's limits, stated: it runs with `auth = trust` only (native
//! SCRAM is not in the gateway yet), as the configured `native_role` in
//! `native_database`; every request is its own autocommit statement
//! (`rshard::Request::Native`); and a request with no `route=` goes to the
//! home shard, where a statement on a sharded table without its key is
//! refused by the shard's own ownership checks. Native transactions keep
//! going direct to a shard (Track A).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rastcp::mux::MuxClient;
use rastcp::server::TcpServerBuilder;
use rasterizeddb_router::key::shard_block;
use rasterizeddb_router::rshard::{self, Request, Response};
use rasterizeddb_router::{KeyFamily, KeyValue};

use crate::state::Shared;

/// The native error frame: `3 ‖ message` (`core::database::error_response`).
fn error(message: &str) -> Vec<u8> {
    let mut v = vec![3u8];
    v.extend_from_slice(message.as_bytes());
    v
}

/// The shard of a request's `route=` option, if it has one.
fn route_of(req: &[u8], shared: &Shared) -> Result<Option<u32>, String> {
    let Some(rest) = req.strip_prefix(b"\x01SIMPLE:") else { return Ok(None) };
    let Some(end) = rest.iter().position(|&b| b == b';') else { return Ok(None) };
    let opts = std::str::from_utf8(&rest[..end]).map_err(|_| "native options are not UTF-8")?;
    for opt in opts.split(',') {
        let Some(v) = opt.strip_prefix("route=") else { continue };
        let map = shared.map.get();
        let block = if let Some(text) = rshard::parse_native_text_route(v) {
            let t = String::from_utf8(text).map_err(|_| "a text route key is not UTF-8")?;
            shard_block(KeyValue::Text(&t), KeyFamily::Text)?
        } else {
            let k = rshard::parse_native_route(v).ok_or_else(|| format!("malformed route option {v:?}"))?;
            shard_block(k, k.family())?
        };
        return Ok(Some(map.shard_of_block(block)));
    }
    Ok(None)
}

pub async fn serve(shared: Arc<Shared>, addr: std::net::SocketAddr, role: String, database: String) {
    let clients: Arc<Mutex<HashMap<u32, MuxClient>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut b = TcpServerBuilder::new(&addr.ip().to_string(), addr.port()).handler_timeout(None).reuse_port(true);
    if let (Some(c), Some(k)) = (&shared.cfg.tls_cert, &shared.cfg.tls_key) {
        match (rastcp::tls::load_certificates(c), rastcp::tls::load_private_key(k)) {
            (Ok(certs), Ok(key)) => b = b.with_certificates(certs, key),
            _ => {
                log::error!("native front end: the TLS certificate or key could not be loaded");
                return;
            }
        }
    }
    let server = match b.build().await {
        Ok(s) => s,
        Err(e) => {
            log::error!("native front end {addr}: {e}");
            return;
        }
    };
    let handler = move |req: Vec<u8>| {
        let shared = shared.clone();
        let clients = clients.clone();
        let role = role.clone();
        let database = database.clone();
        async move {
            let map = shared.map.get();
            let shard = match route_of(&req, &shared) {
                Ok(Some(s)) => s,
                Ok(None) => shared.home(&map),
                Err(e) => return error(&format!("0A000: {e}")),
            };
            let client = {
                let mut g = clients.lock().unwrap_or_else(|e| e.into_inner());
                match g.get(&shard) {
                    Some(c) => c.clone(),
                    None => {
                        let Some(info) = map.shard(shard) else { return error("shard not in the map") };
                        match crate::core::build_client(&shared, &info.addr) {
                            Ok(c) => {
                                g.insert(shard, c.clone());
                                c
                            }
                            Err(e) => return error(&format!("08006: {e}")),
                        }
                    }
                }
            };
            // Hello on every request would cost a round trip: the shard
            // accepts Native on a connection whose Hello it has seen, so a
            // fresh connection says Hello first.
            if !client.is_connected() {
                let hello = Request::Hello {
                    cluster_id: map.cluster_id(),
                    gateway: shared.cfg.gateway_name.clone(),
                    map_epoch: map.epoch(),
                    map_digest: map.digest(),
                }
                .encode();
                if let Err(e) = client.send(&hello).await {
                    return error(&format!("08006: shard {shard}: {e}"));
                }
            }
            let req = Request::Native { database, role, map_epoch: map.epoch(), bytes: req }.encode();
            match client.send(&req).await.map_err(|e| e.to_string()).and_then(|b| Response::decode(&b)) {
                Ok(Response::NativeReply { bytes, .. }) => bytes,
                Ok(Response::Refused(r)) => error(&format!("{}: {}", r.sqlstate_str(), r.message)),
                Ok(_) => error("XX000: unexpected reply from the shard"),
                Err(e) => error(&format!("08007: shard {shard}: {e}")),
            }
        }
    };
    if let Err(e) = server.run(handler).await {
        log::error!("native front end {addr}: {e}");
    }
}
