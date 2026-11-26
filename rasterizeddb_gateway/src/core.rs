//! One core's state (§4.9 G2): its own mux connection to every shard, its
//! own route-template cache, counters and buffers. Only its own thread
//! touches any of it, so none of it is atomic or locked.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rastcp::client::TcpClientBuilder;
use rastcp::mux::MuxClient;
use rastcp::tls::{ClientIdentity, Trust};
use rasterizeddb_router::rshard::{self, Request, Response};
use rasterizeddb_router::shard_map::ShardMap;

use crate::config::ShardTrust;
use crate::counters::Counters;
use crate::state::{Shared, TemplateEntry};

/// Why a call to a shard failed.
#[derive(Debug)]
pub enum CallError {
    /// Nothing was sent: the shard is down or could not be reached. Safe to
    /// report as `08006` and to retry later.
    Down(String),
    /// The request may have reached the shard: its outcome is unknown
    /// (`08007`, never retried, R2).
    Lost(String),
    /// The shard answered something this gateway cannot read.
    Bad(String),
}

#[derive(Clone, Copy, Debug)]
enum Circuit {
    Up,
    /// Requests fail fast until `until`; then one probes (R4).
    Down { until: Instant, backoff: Duration },
}

/// One core's connection to one shard.
pub struct Link {
    pub shard: u32,
    pub addr: String,
    client: MuxClient,
    circuit: Cell<Circuit>,
    /// Hello has been exchanged on the current connection.
    hello: Cell<bool>,
    /// The zstd level this connection's frames are compressed with, once
    /// the shard agreed (`shard_compression`).
    compress: Cell<Option<i32>>,
    /// The shard's id as it said in Hello, checked against the map.
    pub schema_epoch: Cell<u64>,
    /// Raised at each Hello: a new connection, on which the shard holds
    /// none of the sessions opened on an earlier one.
    pub connection: Cell<u64>,
}

impl Link {
    /// Sends `req` and decodes the reply. Hello goes first on a new
    /// connection. A reply carrying a newer map epoch is reported to the
    /// caller through [`Core::observe_epoch`].
    pub async fn call(&self, core: &Core, req: &[u8]) -> Result<Response, CallError> {
        if let Circuit::Down { until, .. } = self.circuit.get()
            && Instant::now() < until
        {
            return Err(CallError::Down(format!("shard {} at {} is down", self.shard, self.addr)));
        }
        if !self.client.is_connected() {
            self.hello.set(false);
            if let Err(e) = self.client.connect().await {
                self.mark_down();
                return Err(CallError::Down(format!("shard {} at {}: {e}", self.shard, self.addr)));
            }
        }
        if !self.hello.get() {
            self.hello(core).await?;
        }
        let started = Instant::now();
        let packed = self.pack(core, req);
        let bytes = match self.client.send(packed.as_deref().unwrap_or(req)).await {
            Ok(b) => b,
            Err(e) => {
                self.hello.set(false);
                if !self.client.is_connected() {
                    self.mark_down();
                }
                return Err(CallError::Lost(format!("shard {} at {}: {e}", self.shard, self.addr)));
            }
        };
        core.counters.borrow_mut().hop(started.elapsed().as_micros() as u64);
        self.circuit.set(Circuit::Up);
        let plain_reply = self.unpack_counted(core, &bytes).map_err(CallError::Bad)?;
        crate::meter::record(req.len(), packed.as_ref().map_or(req.len(), Vec::len), plain_reply.len(), bytes.len());
        let mut resp = Response::decode(&plain_reply).map_err(CallError::Bad)?;
        // The mux reconnected under this request, and the new connection
        // has had no Hello: the shard refused it before effect. Hello and
        // send once more.
        if let Response::Refused(r) = &resp
            && r.before_effect
            && r.sqlstate == rshard::HELLO_FIRST
        {
            self.hello.set(false);
            self.hello(core).await?;
            let packed = self.pack(core, req);
            let bytes = self.client.send(packed.as_deref().unwrap_or(req)).await.map_err(|e| {
                self.hello.set(false);
                CallError::Lost(format!("shard {} at {}: {e}", self.shard, self.addr))
            })?;
            resp = Response::decode(&self.unpack_counted(core, &bytes).map_err(CallError::Bad)?).map_err(CallError::Bad)?;
        }
        if let Some(e) = resp.map_epoch() {
            core.observe_epoch(self.shard, e);
        }
        Ok(resp)
    }

    /// `req` compressed, when this link compresses and that makes it
    /// smaller; `None` to send it as it is.
    fn pack(&self, core: &Core, req: &[u8]) -> Option<Vec<u8>> {
        #[cfg(feature = "shard_compression")]
        if let Some(level) = self.compress.get()
            && req.len() >= core.shared.cfg.shard_compression_min_bytes as usize
            && let Ok(z) = zstd::bulk::compress(req, level)
            && z.len() + 7 < req.len()
        {
            let mut c = core.counters.borrow_mut();
            c.compressed_out += 1;
            c.compression_saved_bytes += (req.len() - z.len() - 7) as u64;
            return Some(rshard::compressed_frame(rshard::CODEC_ZSTD, req.len(), &z));
        }
        let _ = (core, req);
        None
    }

    /// [`unpack`], counted.
    fn unpack_counted<'b>(&self, core: &Core, bytes: &'b [u8]) -> Result<std::borrow::Cow<'b, [u8]>, String> {
        let plain = unpack(bytes)?;
        if let std::borrow::Cow::Owned(p) = &plain {
            let mut c = core.counters.borrow_mut();
            c.compressed_in += 1;
            c.compression_saved_bytes += p.len().saturating_sub(bytes.len()) as u64;
        }
        Ok(plain)
    }

    /// After Hello: asks the shard to compress this connection when the
    /// gateway is set to. A shard that does not know the request (or
    /// refuses it) leaves the link uncompressed.
    async fn negotiate(&self, core: &Core) {
        self.compress.set(None);
        let Some(level) = core.shared.cfg.shard_compression else { return };
        let ask = Request::Compression { codec: rshard::CODEC_ZSTD, level, min_bytes: core.shared.cfg.shard_compression_min_bytes }.encode();
        match self.client.send(&ask).await.map(|b| Response::decode(&b)) {
            Ok(Ok(Response::Done)) => self.compress.set(Some(level)),
            other => log::warn!("shard {} at {} does not compress its link: {other:?}", self.shard, self.addr),
        }
    }

    async fn hello(&self, core: &Core) -> Result<(), CallError> {
        self.connection.set(self.connection.get() + 1);
        let map = core.map();
        let req = Request::Hello {
            cluster_id: map.cluster_id(),
            gateway: core.shared.cfg.gateway_name.clone(),
            map_epoch: map.epoch(),
            map_digest: map.digest(),
        }
        .encode();
        let bytes = self.client.send(&req).await.map_err(|e| {
            self.mark_down();
            CallError::Down(format!("shard {} at {}: hello: {e}", self.shard, self.addr))
        })?;
        match Response::decode(&bytes).map_err(CallError::Bad)? {
            Response::HelloOk { cluster_id, shard_id, map_epoch, schema_epoch, .. } => {
                if cluster_id != map.cluster_id() {
                    self.client.close().await;
                    self.mark_down();
                    return Err(CallError::Down(format!(
                        "{} answers for cluster {cluster_id:032x}, not {:032x}",
                        self.addr,
                        map.cluster_id()
                    )));
                }
                if shard_id != self.shard {
                    self.client.close().await;
                    self.mark_down();
                    return Err(CallError::Down(format!(
                        "{} is shard {shard_id}, but the map says shard {} is there",
                        self.addr, self.shard
                    )));
                }
                self.schema_epoch.set(schema_epoch);
                core.shared.note_schema_epoch(schema_epoch);
                core.observe_epoch(self.shard, map_epoch);
                self.hello.set(true);
                self.negotiate(core).await;
                Ok(())
            }
            Response::Refused(r) => Err(CallError::Down(format!("shard {} refused hello: {} {}", self.shard, r.sqlstate_str(), r.message))),
            other => Err(CallError::Bad(format!("shard {} answered hello with kind {:#04x}", self.shard, other.kind()))),
        }
    }

    fn mark_down(&self) {
        let backoff = match self.circuit.get() {
            Circuit::Up => Duration::from_millis(100),
            Circuit::Down { backoff, .. } => (backoff * 2).min(Duration::from_secs(5)),
        };
        self.circuit.set(Circuit::Down { until: Instant::now() + backoff, backoff });
    }

    pub fn is_up(&self) -> bool {
        matches!(self.circuit.get(), Circuit::Up)
    }
}

/// Whose shard session an idle one may become: the shard, and everything a
/// session is opened with.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub shard: u32,
    pub database: String,
    pub role: String,
    pub pinned: bool,
    pub params: Vec<(String, String)>,
}

/// A shard session no client holds, reset and kept for the next one
/// (`session_pool`).
pub struct IdleSession {
    pub id: u64,
    /// The link's connection it was opened on: gone with that connection.
    pub connection: u64,
    pub backend_key: u64,
    since: Instant,
}

/// A core's route-template cache: `database → shapes / parse texts`.
#[derive(Default)]
pub struct CoreTemplates {
    by_db: HashMap<String, (HashMap<String, TemplateEntry>, HashMap<String, TemplateEntry>)>,
    count: usize,
}

impl CoreTemplates {
    pub fn get(&self, database: &str, parse: bool, text: &str, min_epoch: u64) -> Option<&TemplateEntry> {
        let (s, p) = self.by_db.get(database)?;
        let e = if parse { p.get(text) } else { s.get(text) }?;
        (e.schema_epoch >= min_epoch).then_some(e)
    }

    pub fn put(&mut self, cap: usize, database: &str, parse: bool, text: &str, e: TemplateEntry) {
        if self.count >= cap {
            self.by_db.clear();
            self.count = 0;
        }
        if !self.by_db.contains_key(database) {
            self.by_db.insert(database.to_string(), Default::default());
        }
        let (s, p) = self.by_db.get_mut(database).unwrap();
        let m = if parse { p } else { s };
        if m.insert(text.to_string(), e).is_none() {
            self.count += 1;
        }
    }

    pub fn remove(&mut self, database: &str, parse: bool, text: &str) {
        if let Some((s, p)) = self.by_db.get_mut(database) {
            let m = if parse { p } else { s };
            if m.remove(text).is_some() {
                self.count -= 1;
            }
        }
    }
}

pub struct Core {
    pub id: usize,
    pub shared: Arc<Shared>,
    links: RefCell<HashMap<u32, Rc<Link>>>,
    map: RefCell<Arc<ShardMap>>,
    pub templates: RefCell<CoreTemplates>,
    pub counters: RefCell<Counters>,
    pool: RefCell<Vec<Vec<u8>>>,
    next_session: Cell<u64>,
    /// A shard reported a map epoch above ours: fetch before routing more.
    stale_from: Cell<Option<(u32, u64)>>,
    /// Idle shard sessions (`session_pool`), and how many.
    idle: RefCell<HashMap<SessionKey, Vec<IdleSession>>>,
    idle_count: Cell<usize>,
    /// Raised each time a shard refutes a route template on this core: a
    /// template kept with a prepared statement is valid only under the
    /// count it was taken at.
    pub refuted: Cell<u64>,
}

impl Core {
    pub fn new(id: usize, shared: Arc<Shared>) -> Core {
        let map = shared.map.get();
        Core {
            id,
            shared,
            links: RefCell::new(HashMap::new()),
            map: RefCell::new(map),
            templates: RefCell::new(CoreTemplates::default()),
            counters: RefCell::new(Counters::default()),
            pool: RefCell::new(Vec::new()),
            next_session: Cell::new(1),
            stale_from: Cell::new(None),
            refuted: Cell::new(0),
            idle: RefCell::new(HashMap::new()),
            idle_count: Cell::new(0),
        }
    }

    /// The current map: this core's `Arc`, refreshed when the shared epoch
    /// moved (one relaxed load otherwise).
    pub fn map(&self) -> Arc<ShardMap> {
        let epoch = self.shared.map.epoch();
        if self.map.borrow().epoch() != epoch {
            *self.map.borrow_mut() = self.shared.map.get();
            // A new map can move a shard's address: links are rebuilt.
            let map = self.map.borrow().clone();
            self.links.borrow_mut().retain(|id, l| map.shard(*id).is_some_and(|s| s.addr == l.addr));
        }
        self.map.borrow().clone()
    }

    /// A unique virtual-session id on this core's connections.
    pub fn session_id(&self) -> u64 {
        let n = self.next_session.get();
        self.next_session.set(n + 1);
        (self.id as u64) << 48 | n
    }

    pub fn observe_epoch(&self, shard: u32, epoch: u64) {
        if epoch > self.shared.map.epoch() {
            self.stale_from.set(Some((shard, epoch)));
        }
    }

    /// R7: before routing, a map newer than ours that a shard mentioned is
    /// fetched from that shard and installed. A failure leaves the old map,
    /// and the shards' ownership checks keep it safe (R1).
    pub async fn refresh_if_stale(&self) {
        let Some((shard, epoch)) = self.stale_from.take() else { return };
        if epoch <= self.shared.map.epoch() {
            return;
        }
        if let Err(e) = self.fetch_map(shard).await {
            log::warn!("map epoch {epoch} reported by shard {shard} could not be fetched: {e}");
        }
    }

    pub async fn fetch_map(&self, shard: u32) -> Result<bool, String> {
        let link = self.link(shard)?;
        match link.call(self, &Request::MapFetch.encode()).await {
            Ok(Response::MapBytes { map, .. }) => {
                let m = ShardMap::from_binary(&map)?;
                let changed = self.shared.map.install(m)?;
                if changed {
                    self.counters.borrow_mut().map_refreshes += 1;
                }
                Ok(changed)
            }
            Ok(Response::Refused(r)) => Err(format!("{} {}", r.sqlstate_str(), r.message)),
            Ok(other) => Err(format!("unexpected reply kind {:#04x} to MapFetch", other.kind())),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// This core's link to `shard`, made on first use.
    pub fn link(&self, shard: u32) -> Result<Rc<Link>, String> {
        if let Some(l) = self.links.borrow().get(&shard) {
            return Ok(l.clone());
        }
        let map = self.map();
        let info = map.shard(shard).ok_or_else(|| format!("shard {shard} is not in the map"))?;
        if info.addr.is_empty() {
            return Err(format!("the map gives shard {shard} no address"));
        }
        // A core's links are used only by its own sessions, all tasks of
        // its `LocalSet`: the connection's task runs there with them.
        let client = build_client_with(&self.shared, &info.addr, true)?;
        let link = Rc::new(Link {
            shard,
            addr: info.addr.clone(),
            client,
            circuit: Cell::new(Circuit::Up),
            hello: Cell::new(false),
            compress: Cell::new(None),
            schema_epoch: Cell::new(0),
            connection: Cell::new(0),
        });
        self.links.borrow_mut().insert(shard, link.clone());
        Ok(link)
    }

    /// A pooled buffer (G8).
    pub fn buffer(&self) -> Vec<u8> {
        self.pool.borrow_mut().pop().unwrap_or_else(|| Vec::with_capacity(8 << 10))
    }

    /// Returns a buffer; one that grew past 256 KiB is dropped, not kept.
    pub fn give_back(&self, mut b: Vec<u8>) {
        if b.capacity() <= 256 << 10 {
            b.clear();
            let mut p = self.pool.borrow_mut();
            if p.len() < 256 {
                p.push(b);
            }
        }
    }

    /// An idle session for `key`, on the link's current `connection`. One
    /// opened on an earlier connection went with it, and is dropped here.
    pub fn take_idle(&self, key: &SessionKey, connection: u64) -> Option<IdleSession> {
        let mut idle = self.idle.borrow_mut();
        let list = idle.get_mut(key)?;
        let mut found = None;
        while let Some(s) = list.pop() {
            self.idle_count.set(self.idle_count.get() - 1);
            if s.connection == connection {
                found = Some(s);
                break;
            }
        }
        if list.is_empty() {
            idle.remove(key);
        }
        found
    }

    /// Keeps a reset session for the next client; `false` when the pool is
    /// full (the caller closes it).
    pub fn keep_idle(&self, key: SessionKey, id: u64, connection: u64, backend_key: u64) -> bool {
        if self.idle_count.get() >= self.shared.cfg.session_pool {
            return false;
        }
        self.idle.borrow_mut().entry(key).or_default().push(IdleSession { id, connection, backend_key, since: Instant::now() });
        self.idle_count.set(self.idle_count.get() + 1);
        true
    }

    /// The idle sessions kept past `session_pool_idle`, taken out: the
    /// caller closes them on their shards.
    pub fn expired_idle(&self) -> Vec<(u32, u64)> {
        if self.idle_count.get() == 0 {
            return Vec::new();
        }
        let ttl = self.shared.cfg.session_pool_idle;
        let mut out = Vec::new();
        self.idle.borrow_mut().retain(|key, list| {
            list.retain(|s| {
                let keep = s.since.elapsed() < ttl;
                if !keep {
                    out.push((key.shard, s.id));
                }
                keep
            });
            !list.is_empty()
        });
        self.idle_count.set(self.idle_count.get() - out.len());
        out
    }

    pub fn publish(&self) {
        self.shared.board.publish(self.id, &self.counters.borrow());
    }
}

/// A mux client to a shard, with the gateway's mTLS identity and the
/// configured trust.
pub fn build_client(shared: &Shared, addr: &str) -> Result<MuxClient, String> {
    build_client_with(shared, addr, false)
}

/// [`build_client`]; `local`: its connection task runs on the caller's
/// `LocalSet` (`TcpClientBuilder::local_driver`).
fn build_client_with(shared: &Shared, addr: &str, local: bool) -> Result<MuxClient, String> {
    let cfg = &shared.cfg;
    let mut b = TcpClientBuilder::new(addr).max_in_flight(cfg.max_in_flight).local_driver(local);
    b = match cfg.shard_timeout {
        Some(t) => b.timeout(t),
        // A statement may run as long as the client lets it: the
        // connection-level liveness is rastcp's own.
        None => b.no_timeout(),
    };
    b = match &cfg.shard_trust {
        ShardTrust::AcceptAny => b.trust(Trust::AcceptAny),
        ShardTrust::RootCa(f) => b.trust(Trust::RootCa(rastcp::tls::load_certificates(f).map_err(|e| e.to_string())?)),
        ShardTrust::Pinned(fps) => b.trust(Trust::Pinned(
            fps.iter().map(|f| rastcp::tls::parse_fingerprint(f).map_err(|e| e.to_string())).collect::<Result<_, _>>()?,
        )),
    };
    if let (Some(c), Some(k)) = (&cfg.shard_cert, &cfg.shard_key) {
        b = b.identity(ClientIdentity::load(c, k).map_err(|e| e.to_string())?);
    }
    // `build_multiplexed` does not dial and awaits nothing: one poll ends it.
    poll_once(b.build_multiplexed())
        .ok_or("internal: building a shard client did not complete at once")?
        .map_err(|e| e.to_string())
}

/// Polls a future once: `None` if it would wait.
fn poll_once<F: std::future::Future>(f: F) -> Option<F::Output> {
    use std::task::{Context, Poll, Waker};
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// A shard's reply, decompressed when it came compressed.
fn unpack(frame: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, String> {
    let Some((codec, len, body)) = rshard::split_compressed(frame) else {
        return Ok(std::borrow::Cow::Borrowed(frame));
    };
    #[cfg(feature = "shard_compression")]
    {
        if codec != rshard::CODEC_ZSTD {
            return Err(format!("a reply compressed with codec {codec}, which this gateway does not speak"));
        }
        if len > rshard::MAX_FIELD + 64 {
            return Err(format!("a compressed reply of {len} bytes is past the limit"));
        }
        let plain = zstd::bulk::decompress(body, len).map_err(|e| format!("a compressed reply does not decompress: {e}"))?;
        if plain.len() != len {
            return Err("a compressed reply's length is not what it says".to_string());
        }
        Ok(std::borrow::Cow::Owned(plain))
    }
    #[cfg(not(feature = "shard_compression"))]
    {
        let _ = (codec, len, body);
        Err("a compressed reply, and this gateway was built without shard_compression".to_string())
    }
}
