//! The transport between a shard's Raft replicas (sharding-plan.md B3):
//! rastcp/2, mTLS-only, one request per message. The reply is empty;
//! delivery is best effort, which Raft's retries make enough.
//!
//! `--raft-listen ADDR:PORT` with `--raft-peer ID=ADDR` per other replica,
//! this replica's certificate (`--shard-cert`/`--shard-key`) as both its
//! server and its client identity, and `--peer-ca` as the authority every
//! replica's certificate is signed by.

use std::collections::BTreeMap;
use std::sync::Arc;

use rasterizeddb_raft::codec::{decode_message, encode_message};
use rasterizeddb_raft::{Message, NodeId};

use super::raft_group::{Group, Transport};

/// Where each peer listens.
#[derive(Clone, Debug)]
pub struct RaftNet {
    pub listen_addr: String,
    pub listen_port: u16,
    pub peers: BTreeMap<NodeId, String>,
    pub cert: String,
    pub key: String,
    pub ca: String,
}

/// Sends through one lazily dialled mux client per peer; a send that fails
/// drops the client, so the next one dials again.
pub struct RastcpTransport {
    net: RaftNet,
    clients: std::sync::Mutex<BTreeMap<NodeId, Arc<rastcp::mux::MuxClient>>>,
}

impl RastcpTransport {
    pub fn new(net: RaftNet) -> Arc<RastcpTransport> {
        Arc::new(RastcpTransport { net, clients: std::sync::Mutex::new(BTreeMap::new()) })
    }

    async fn client(self: &Arc<Self>, to: NodeId) -> Result<Arc<rastcp::mux::MuxClient>, String> {
        if let Some(c) = self.clients.lock().unwrap().get(&to) {
            return Ok(c.clone());
        }
        let addr = self.net.peers.get(&to).ok_or_else(|| format!("raft peer {to} has no address"))?;
        let e = |x: rastcp::error::RastcpError| x.to_string();
        let client = rastcp::client::TcpClientBuilder::new(addr)
            .no_timeout()
            .trust(rastcp::tls::Trust::RootCa(rastcp::tls::load_certificates(&self.net.ca).map_err(e)?))
            .identity(rastcp::tls::ClientIdentity::load(&self.net.cert, &self.net.key).map_err(e)?)
            .build_multiplexed()
            .await
            .map_err(e)?;
        let client = Arc::new(client);
        self.clients.lock().unwrap().insert(to, client.clone());
        Ok(client)
    }
}

/// The transport handed to the group: sends never block the group's loop.
pub struct Sender(pub Arc<RastcpTransport>);

impl Transport for Sender {
    fn send(&self, to: NodeId, message: Message) {
        let t = self.0.clone();
        tokio::spawn(async move {
            let bytes = encode_message(&message);
            match t.client(to).await {
                Ok(c) => {
                    if c.send(&bytes).await.is_err() {
                        t.clients.lock().unwrap().remove(&to);
                    }
                }
                Err(e) => log::debug!("raft: no client for peer {to}: {e}"),
            }
        });
    }
}

/// Serves peers' messages into `group` until the process ends.
pub async fn serve(net: RaftNet, group: Group) -> std::io::Result<()> {
    use rastcp::tls::{ClientAuth, load_certificates, load_private_key};
    let io = |e: rastcp::error::RastcpError| std::io::Error::other(e.to_string());
    let server = rastcp::server::TcpServerBuilder::new(&net.listen_addr, net.listen_port)
        .client_auth(ClientAuth::RequireCa(load_certificates(&net.ca).map_err(io)?))
        .with_certificates(load_certificates(&net.cert).map_err(io)?, load_private_key(&net.key).map_err(io)?)
        .handler_timeout(None)
        .max_message_size((1 << 30) + 64)
        .build()
        .await
        .map_err(io)?;
    log::info!("raft listening on {}:{}", net.listen_addr, net.listen_port);
    server
        .run_with_context(group, |group: Group, data: Vec<u8>| async move {
            match decode_message(&data) {
                Ok(m) => group.deliver(m).await,
                Err(e) => log::warn!("raft: a peer sent an unreadable message: {e}"),
            }
            Vec::new()
        })
        .await
        .map_err(io)
}
