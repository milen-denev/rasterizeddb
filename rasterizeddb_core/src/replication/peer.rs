//! One outbound link to one peer.
//!
//! A peer is a handle plus a task. The handle is what the commit path touches:
//! enqueueing a record on it is a bounded channel send and nothing else, so a
//! slow or missing peer can never make a local commit wait. The task owns the
//! connection and does everything expensive — connecting, batching,
//! compressing, waiting for the acknowledgement, reconnecting.
//!
//! ## Why the queue is allowed to overflow
//!
//! The queue is bounded, and a full one drops the record rather than blocking.
//! That sounds lossy and is not, because the queue is not the record's only
//! home: it is already in the replication log. A drop raises a catch-up flag,
//! and the task backfills from the log the moment it comes round again. The
//! alternative — blocking the commit until a dead peer's queue drains — trades
//! a bounded amount of catch-up work for an unbounded amount of client latency.
//!
//! ## Batching
//!
//! Under load the sender coalesces whatever has accumulated into one frame, up
//! to a record and byte limit, waiting at most `batch_linger` for more. This is
//! where most of the throughput comes from: one TLS record, one round trip and
//! one compression pass amortised over a whole batch instead of per commit. At
//! low load the linger is sub-millisecond and the batch is one record, so
//! latency is not traded away for a throughput that is not being asked for.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use cacheguard::CacheGuard;
use std::time::Duration;

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rastcp::client::{TcpClient, TcpClientBuilder};

use super::config::{Codec, Compression, NodeId, PeerConfig};
use super::manager::{Outbound, Replication};
use super::protocol::{
    self, ApplyStatus, FrameCodec, FrameKind, Hello, HelloAck, Pull, PushAck, Reader, TxnRecord,
};

/// What the status view shows for one peer.
#[derive(Clone, Debug)]
pub struct PeerSnapshot {
    pub name: String,
    pub address: String,
    pub connected: bool,
    pub paused: bool,
    pub remote_node: NodeId,
    /// Highest locally-originated sequence this peer has confirmed.
    pub acked_seq: u64,
    /// How far behind the local stream that leaves it.
    pub lag: u64,
    pub queued: usize,
    pub records_sent: u64,
    pub batches_sent: u64,
    pub dropped: u64,
    pub codec: Codec,
    /// Compressed/uncompressed on the last batch, if it was compressed.
    pub last_ratio: Option<f64>,
    pub last_error: Option<String>,
}

/// The handle: what everything other than the peer's own task sees.
pub struct PeerHandle {
    pub name: String,
    address: Mutex<String>,
    sender: kanal::AsyncSender<Outbound>,
    paused: AtomicBool,
    connected: AtomicBool,
    stopping: AtomicBool,
    remote_node: AtomicU64,
    /// Padded. This is the one field on the handle written by the ack path and
    /// read by everything else — a genuine cross-thread ping-pong, unlike the
    /// counters below it, which the peer's own task owns outright and bumps
    /// three at a time (so they are better off sharing a line, not being split
    /// across three).
    acked_seq: CacheGuard<AtomicU64>,
    records_sent: AtomicU64,
    batches_sent: AtomicU64,
    dropped: AtomicU64,
    /// Raised when a record could not be queued, or a send failed. The task
    /// backfills from the log when it sees it.
    ///
    /// Padded for the same reason in the other direction: capturing writer
    /// threads raise it, the peer task polls it every loop iteration.
    needs_catchup: CacheGuard<AtomicBool>,
    codec: AtomicU64,
    /// Last observed compression ratio, as `f64` bits. `0` means "not yet".
    last_ratio: AtomicU64,
    last_error: Mutex<Option<String>>,
    compression: Mutex<Option<Compression>>,
}

impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerHandle")
            .field("name", &self.name)
            .field("address", &*self.address.lock())
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl PeerHandle {
    #[inline]
    pub fn address(&self) -> String {
        self.address.lock().clone()
    }

    #[inline]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        if !paused {
            // Anything committed while this peer was paused was never queued
            // for it — `Replication::publish` skips a paused peer — so the log
            // is the only place those records exist for this link. Resuming has
            // to go and look.
            self.needs_catchup.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    /// Ends the peer's task. The handle stays usable but inert.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        // Closing wakes the task out of `recv` rather than leaving it parked
        // until the next record that will never come.
        let _ = self.sender.close();
    }

    pub fn acked_seq(&self) -> u64 {
        self.acked_seq.load(Ordering::Relaxed)
    }

    /// The node id the peer gave in its handshake, or 0 before it has.
    #[inline]
    pub fn remote_node(&self) -> NodeId {
        self.remote_node.load(Ordering::Relaxed)
    }

    /// Queues a record. Never blocks; a full queue drops and flags a catch-up.
    pub fn enqueue(&self, outbound: Outbound) {
        if self.is_stopping() {
            return;
        }
        match self.sender.try_send(outbound) {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                // `Ok(false)` is a full queue, `Err` a closed one. Either way
                // the record is in the log and catch-up will find it.
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.needs_catchup.store(true, Ordering::Relaxed);
            }
        }
    }

    fn set_error(&self, message: Option<String>) {
        *self.last_error.lock() = message;
    }

    fn set_ratio(&self, ratio: Option<f64>) {
        self.last_ratio
            .store(ratio.map(f64::to_bits).unwrap_or(0), Ordering::Relaxed);
    }

    fn ratio(&self) -> Option<f64> {
        match self.last_ratio.load(Ordering::Relaxed) {
            0 => None,
            bits => Some(f64::from_bits(bits)),
        }
    }

    /// Overrides this link's compression, or clears the override.
    pub fn set_compression(&self, compression: Option<Compression>) {
        *self.compression.lock() = compression;
    }

    pub fn compression_override(&self) -> Option<Compression> {
        *self.compression.lock()
    }

    pub fn set_address(&self, address: String) {
        *self.address.lock() = address;
        // The task reconnects to the new address once the current attempt ends.
        self.needs_catchup.store(true, Ordering::Relaxed);
    }

    pub fn snapshot(&self, local_seq: u64) -> PeerSnapshot {
        let acked = self.acked_seq();
        PeerSnapshot {
            name: self.name.clone(),
            address: self.address(),
            connected: self.is_connected(),
            paused: self.is_paused(),
            remote_node: self.remote_node.load(Ordering::Relaxed),
            acked_seq: acked,
            lag: local_seq.saturating_sub(acked),
            queued: self.sender.len(),
            records_sent: self.records_sent.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            codec: Codec::from_wire(self.codec.load(Ordering::Relaxed) as u8)
                .unwrap_or(Codec::None),
            last_ratio: self.ratio(),
            last_error: self.last_error.lock().clone(),
        }
    }
}

/// Builds a peer and starts its task.
pub fn spawn(replication: Arc<Replication>, config: &PeerConfig) -> Arc<PeerHandle> {
    let capacity = replication.config().queue_capacity;
    let (sender, receiver) = kanal::bounded_async::<Outbound>(capacity);

    let handle = Arc::new(PeerHandle {
        name: config.name.clone(),
        address: Mutex::new(config.address.clone()),
        sender,
        paused: AtomicBool::new(config.paused),
        connected: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        remote_node: AtomicU64::new(0),
        acked_seq: AtomicU64::new(0).into(),
        records_sent: AtomicU64::new(0),
        batches_sent: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        // A fresh link has no idea what the peer holds, so its first act is to
        // ask — which is what catch-up does.
        needs_catchup: AtomicBool::new(true).into(),
        codec: AtomicU64::new(Codec::None.as_wire() as u64),
        last_ratio: AtomicU64::new(0),
        last_error: Mutex::new(None),
        compression: Mutex::new(config.compression),
    });

    let task_handle = handle.clone();
    tokio::spawn(async move {
        run(replication, task_handle, receiver).await;
    });

    handle
}

/// The peer's task: connect, catch up, then stream until told to stop.
async fn run(
    replication: Arc<Replication>,
    peer: Arc<PeerHandle>,
    receiver: kanal::AsyncReceiver<Outbound>,
) {
    let mut backoff = replication.config().reconnect_base;
    let mut codec = FrameCodec::new(replication.config().compression.level);

    while !peer.is_stopping() {
        if peer.is_paused() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        match connect(&replication, &peer).await {
            Ok(mut link) => {
                peer.connected.store(true, Ordering::Relaxed);
                peer.set_error(None);
                let acked_before = peer.acked_seq();
                info!(
                    "Replication link to {} ({}) established, node {}, codec {}",
                    peer.name, link.address, link.remote_node, link.codec
                );

                if let Err(e) = stream(&replication, &peer, &mut link, &receiver, &mut codec).await {
                    warn!("Replication link to {} ended: {}", peer.name, e);
                    peer.set_error(Some(e));
                }

                // Connecting is not progress. A peer that refuses every
                // transaction — a stalled origin under `conflict abort` — still
                // accepts the connection, so resetting the backoff here spun the
                // link at `reconnect_base` indefinitely: reconnect, re-send the
                // same refused transaction, drop, repeat, several times a second
                // for as long as the conflict stood. Reset only when the link
                // actually moved the peer forward, so a stall decays to
                // `reconnect_max` while a healthy flapping link still recovers
                // immediately.
                if peer.acked_seq() > acked_before {
                    backoff = replication.config().reconnect_base;
                }

                peer.connected.store(false, Ordering::Relaxed);
                // Whatever was in flight when the link dropped is unconfirmed;
                // the next connection re-derives what to send from the log.
                peer.needs_catchup.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                peer.connected.store(false, Ordering::Relaxed);
                debug!("Could not reach peer {}: {}", peer.name, e);
                peer.set_error(Some(e));
            }
        }

        if peer.is_stopping() {
            break;
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(replication.config().reconnect_max);
    }

    info!("Replication link to {} stopped", peer.name);
}

/// A connected, handshaken link.
struct Link {
    client: TcpClient,
    address: String,
    remote_node: NodeId,
    codec: Codec,
}

/// Dials the peer and completes the handshake.
async fn connect(replication: &Arc<Replication>, peer: &Arc<PeerHandle>) -> Result<Link, String> {
    let config = replication.config();
    let address = peer.address();

    let mut builder = TcpClientBuilder::new(&address)
        .timeout(config.request_timeout)
        // Who this node will believe is on the other end. Under
        // `AuthMode::Insecure` this is "anything", which is why that mode is
        // only allowed on loopback.
        .trust(config.tls.trust(config.auth)?);

    // The certificate this node presents when the peer asks for one. A peer
    // configured to require client authentication rejects the handshake
    // without it.
    if let Some((certs, key)) = config.tls.identity()? {
        builder = builder.identity(rastcp::tls::ClientIdentity { certs, key });
    }

    let mut client = builder
        .build()
        .await
        .map_err(|e| format!("could not build a client for {}: {}", address, e))?;

    tokio::time::timeout(config.connect_timeout, client.connect())
        .await
        .map_err(|_| format!("connecting to {} timed out", address))?
        .map_err(|e| format!("could not connect to {}: {}", address, e))?;

    let hello = Hello {
        node_id: config.node_id,
        cluster_id: config.cluster_id(),
        codecs: Codec::local_mask(),
        clock: replication.clock().now(),
        token: config.token.clone(),
        advertised_addr: format!("{}:{}", config.listen_addr, config.listen_port),
    };
    let mut payload = Vec::new();
    hello.encode(&mut payload);

    let response = client
        .send(protocol::plain_frame(FrameKind::Hello, 0, &payload))
        .await
        .map_err(|e| format!("handshake with {} failed: {}", address, e))?;

    let mut scratch = Vec::new();
    let mut codec = FrameCodec::default();
    let (header, body) = codec
        .decode_frame(&response, &mut scratch)
        .map_err(|e| format!("{} answered with an unreadable frame: {}", address, e))?;

    match header.kind {
        FrameKind::HelloAck => {}
        FrameKind::Error => return Err(protocol::decode_error(body)),
        other => return Err(format!("{} answered a HELLO with {}", address, other)),
    }

    let ack = HelloAck::decode(&mut Reader::new(body))
        .map_err(|e| format!("{} sent an unreadable HELLO-ACK: {}", address, e))?;

    if ack.cluster_id != config.cluster_id() {
        return Err(format!(
            "{} belongs to a different cluster; check --replication-cluster on both nodes",
            address
        ));
    }
    if ack.node_id == config.node_id {
        return Err(format!(
            "{} reports node id {}, which is this node's own — every node needs a unique id",
            address, ack.node_id
        ));
    }

    replication.clock().observe(ack.clock);
    peer.remote_node.store(ack.node_id, Ordering::Relaxed);
    peer.codec.store(ack.codec.as_wire() as u64, Ordering::Relaxed);

    // What the peer says it has of ours is where the next send starts.
    let ours = ack
        .applied
        .iter()
        .find(|entry| entry.origin == config.node_id)
        .map(|entry| entry.seq)
        .unwrap_or(0);
    peer.acked_seq.store(ours, Ordering::Relaxed);

    Ok(Link { client, address, remote_node: ack.node_id, codec: ack.codec })
}

/// Streams records until the link fails or the peer is stopped.
async fn stream(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    link: &mut Link,
    receiver: &kanal::AsyncReceiver<Outbound>,
    codec: &mut FrameCodec,
) -> Result<(), String> {
    // One buffer for the life of the link.
    let mut frame = Vec::new();

    loop {
        if peer.is_stopping() || peer.is_paused() {
            return Ok(());
        }

        if peer.needs_catchup.swap(false, Ordering::Relaxed) {
            if let Err(e) = catch_up(replication, peer, link, codec, &mut frame).await {
                // Put the flag back: the backlog is still there.
                peer.needs_catchup.store(true, Ordering::Relaxed);
                return Err(e);
            }
        }

        let Some(batch) = next_batch(replication, receiver).await else {
            // The channel closed, which only happens on `stop`.
            return Ok(());
        };
        if batch.is_empty() {
            // Idle. Round again, so the pause and catch-up flags are re-read.
            continue;
        }

        let records: Vec<Arc<TxnRecord>> = batch.iter().map(|o| o.record.clone()).collect();
        let mut refs: Vec<&TxnRecord> = records.iter().map(|r| r.as_ref()).collect();
        // Sequence numbers are assigned at commit and queued just after, so two
        // transactions committing concurrently can reach the queue in the other
        // order. The receiver refuses a record that would leave a hole and the
        // sender then backfills it, which converges but costs a round trip per
        // inversion — under concurrent load that was most of a batch. Sorting
        // here is a few comparisons on a handful of records and removes the
        // inversions that happen within one batch, which is nearly all of them.
        refs.sort_unstable_by_key(|record| (record.origin, record.seq));

        match push(replication, peer, link, codec, &refs, &mut frame).await {
            Ok(settled) => {
                // Only what the peer reported as applied or already-held. A
                // record it refused has *not* reached it, and signalling for
                // one would let a synchronous commit claim a durability it
                // does not have — the waiter times out instead, which is the
                // honest answer.
                let mut refused = 0;
                for outbound in &batch {
                    if settled.contains(&outbound.record) {
                        if let Some(ack) = outbound.ack.as_ref() {
                            ack.signal();
                        }
                    } else {
                        refused += 1;
                    }
                }

                if refused == batch.len() {
                    // Nothing got through. Most likely the peer's apply has
                    // stalled on a conflict, and hammering it changes nothing —
                    // back off so a stalled stream is quiet rather than a spin.
                    tokio::time::sleep(replication.config().reconnect_base).await;
                }
            }
            Err(e) => {
                // The batch is unconfirmed and stays that way; the commits
                // waiting on it fall back on their timeout, and the records
                // themselves are recovered from the log on reconnect.
                peer.needs_catchup.store(true, Ordering::Relaxed);
                return Err(e);
            }
        }
    }
}

/// How long an idle sender waits before returning to its loop.
///
/// Without it a sender with nothing queued parks in `recv` indefinitely and
/// never re-reads the flags around it — so a peer paused and then resumed would
/// sit there until the next commit happened to wake it, and a catch-up raised
/// while it was parked would not run. Coming up for air a few times a second
/// costs one timer per idle link.
const IDLE_POLL: Duration = Duration::from_millis(200);

/// Collects the next batch, waiting out the linger for company.
///
/// An empty batch means "nothing right now" and is the caller's cue to go round
/// its loop again. `None` means the channel has closed for good.
async fn next_batch(
    replication: &Arc<Replication>,
    receiver: &kanal::AsyncReceiver<Outbound>,
) -> Option<Vec<Outbound>> {
    let first = match tokio::time::timeout(IDLE_POLL, receiver.recv()).await {
        Ok(Ok(first)) => first,
        Ok(Err(_)) => return None,
        Err(_) => return Some(Vec::new()),
    };

    let config = replication.config();
    let mut bytes = first.record.encoded_size_hint();
    let mut batch = Vec::with_capacity(config.batch_max_records.min(64));
    batch.push(first);

    if config.batch_linger.is_zero() {
        // No linger: take whatever is already queued and go.
        while batch.len() < config.batch_max_records && bytes < config.batch_max_bytes {
            match receiver.try_recv() {
                Ok(Some(item)) => {
                    bytes += item.record.encoded_size_hint();
                    batch.push(item);
                }
                _ => break,
            }
        }
        return Some(batch);
    }

    let deadline = tokio::time::Instant::now() + config.batch_linger;
    while batch.len() < config.batch_max_records && bytes < config.batch_max_bytes {
        match tokio::time::timeout_at(deadline, receiver.recv()).await {
            Ok(Ok(item)) => {
                bytes += item.record.encoded_size_hint();
                batch.push(item);
            }
            // Either the linger expired or the channel closed. Both mean send
            // what we have; a closed channel is noticed on the next round.
            _ => break,
        }
    }

    Some(batch)
}

/// Sends one batch and applies what the acknowledgement says.
async fn push(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    link: &mut Link,
    codec: &mut FrameCodec,
    records: &[&TxnRecord],
    // Reused across batches. `encode_frame` clears it, so what carries over is
    // the capacity — which is what stops every batch from growing a buffer up
    // to its size from nothing.
    frame: &mut Vec<u8>,
) -> Result<Settled, String> {
    if records.is_empty() {
        return Ok(Settled::default());
    }

    let config = replication.config();
    let compression = peer
        .compression_override()
        .unwrap_or(config.compression);
    codec.set_level(compression.level);

    // The peer only advertised what it can decode; sending it something else
    // would be a decode failure at the far end rather than here.
    let compression = if link.codec == Codec::None {
        Compression { mode: super::config::CompressionMode::Off, codec: Codec::None, ..compression }
    } else {
        Compression { codec: link.codec, ..compression }
    };

    let mut payload =
        Vec::with_capacity(8 + records.iter().map(|r| r.encoded_size_hint()).sum::<usize>());
    protocol::encode_push(config.node_id, records.iter().copied(), records.len(), &mut payload);

    let used = codec
        .encode_frame(FrameKind::Push, 0, &payload, compression, peer.ratio(), frame)
        .map_err(|e| format!("could not encode a batch for {}: {}", peer.name, e))?;

    let raw = payload.len() as u64;
    let wire = frame.len() as u64;
    peer.set_ratio(if used == Codec::None {
        None
    } else {
        Some(wire as f64 / raw.max(1) as f64)
    });

    let response = link
        .client
        .send_borrowed(frame)
        .await
        .map_err(|e| format!("sending to {} failed: {}", peer.name, e))?;

    let stats = replication.stats();
    stats.bytes_raw.fetch_add(raw, Ordering::Relaxed);
    stats.bytes_wire.fetch_add(wire, Ordering::Relaxed);
    stats.batches_sent.fetch_add(1, Ordering::Relaxed);
    if used != Codec::None {
        stats.compressed_batches.fetch_add(1, Ordering::Relaxed);
    }
    peer.batches_sent.fetch_add(1, Ordering::Relaxed);
    peer.records_sent
        .fetch_add(records.len() as u64, Ordering::Relaxed);

    let mut scratch = Vec::new();
    let (header, body) = codec
        .decode_frame(&response, &mut scratch)
        .map_err(|e| format!("{} answered with an unreadable frame: {}", peer.name, e))?;

    match header.kind {
        FrameKind::PushAck => {}
        FrameKind::Error => return Err(protocol::decode_error(body)),
        other => return Err(format!("{} answered a PUSH with {}", peer.name, other)),
    }

    let ack = PushAck::decode(&mut Reader::new(body))
        .map_err(|e| format!("{} sent an unreadable PUSH-ACK: {}", peer.name, e))?;

    replication.clock().observe(ack.clock);
    Ok(record_outcomes(replication, peer, &ack))
}

/// Which records of a batch the peer actually took.
///
/// A record the peer refused is *not* in here, which is what stops a
/// synchronous commit from reporting success for a transaction the other node
/// never applied.
#[derive(Default, Debug)]
pub struct Settled {
    taken: std::collections::HashSet<(NodeId, u64)>,
}

impl Settled {
    #[inline]
    fn contains(&self, record: &TxnRecord) -> bool {
        self.taken.contains(&(record.origin, record.seq))
    }
}

/// Folds an acknowledgement into the peer's position and the node's counters.
fn record_outcomes(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    ack: &PushAck,
) -> Settled {
    let stats = replication.stats();
    let node_id = replication.node_id();
    let mut settled = Settled::default();

    for outcome in &ack.outcomes {
        if outcome.status.is_settled() {
            settled.taken.insert((outcome.origin, outcome.seq));
        }
        match outcome.status {
            ApplyStatus::Applied => {
                stats.applied.fetch_add(1, Ordering::Relaxed);
            }
            ApplyStatus::Duplicate => {
                stats.duplicates.fetch_add(1, Ordering::Relaxed);
            }
            ApplyStatus::Conflict => {
                stats.conflicts.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "{} refused transaction {}:{} as a conflict: {}",
                    peer.name, outcome.origin, outcome.seq, outcome.message
                );
            }
            ApplyStatus::Failed => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                error!(
                    "{} could not apply transaction {}:{}: {}",
                    peer.name, outcome.origin, outcome.seq, outcome.message
                );
            }
            ApplyStatus::Gap => {
                stats.gaps.fetch_add(1, Ordering::Relaxed);
                // The peer is missing something earlier; backfill it.
                peer.needs_catchup.store(true, Ordering::Relaxed);
            }
        }
    }

    // The peer's own account of how far it has got is more trustworthy than
    // counting acknowledgements, and it also covers what a third node sent.
    if let Some(entry) = ack.applied.iter().find(|entry| entry.origin == node_id) {
        peer.acked_seq.fetch_max(entry.seq, Ordering::Relaxed);
    }

    settled
}

/// Sends whatever the peer is missing of this node's stream, from the log.
///
/// Runs on connect and whenever something is known to have been dropped. The
/// starting point comes from the peer itself, so this cannot skip a record it
/// never received.
async fn catch_up(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    link: &mut Link,
    codec: &mut FrameCodec,
    frame: &mut Vec<u8>,
) -> Result<(), String> {
    let Some(log) = replication.log() else {
        return Ok(());
    };

    let node_id = replication.node_id();
    let mut from = peer.acked_seq();
    let target = replication.local_seq();
    if from >= target {
        return Ok(());
    }

    if !log.can_serve(node_id, from) {
        warn!(
            "Peer {} needs transactions from sequence {}, which retention has already \
             dropped. It will keep only what arrives from now on; run a full \
             resynchronisation to close the gap.",
            peer.name,
            from + 1
        );
        // Resume from what is still held rather than repeatedly failing on
        // history that no longer exists.
        from = log.earliest(node_id).saturating_sub(1);
    }

    let config = replication.config();
    info!(
        "Catching peer {} up from sequence {} to {}",
        peer.name, from, target
    );

    loop {
        if peer.is_stopping() || peer.is_paused() {
            return Ok(());
        }

        let (records, more) = log
            .read_after(
                node_id,
                from,
                config.batch_max_records,
                config.batch_max_bytes,
            )
            .map_err(|e| format!("could not read the replication log: {}", e))?;

        if records.is_empty() {
            return Ok(());
        }

        let highest = records.last().map(|r| r.seq).unwrap_or(from);
        let refs: Vec<&TxnRecord> = records.iter().collect();
        push(replication, peer, link, codec, &refs, frame).await?;

        from = highest;
        if !more {
            return Ok(());
        }
    }
}

/// Asks a peer for what *this* node is missing of `origin`'s stream.
///
/// The inverse of [`catch_up`], and what `REPLICATION RESYNC` uses: normally a
/// node is told what it missed by the origin pushing it, but after a reset — or
/// when the origin itself is unreachable and a third node has the history —
/// pulling is the only way to close the gap.
pub async fn pull_from(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    origin: NodeId,
    after_seq: u64,
) -> Result<(Vec<TxnRecord>, bool), String> {
    let config = replication.config();
    let mut link = connect(replication, peer).await?;
    let mut codec = FrameCodec::new(config.compression.level);

    let request = Pull {
        node_id: config.node_id,
        origin,
        after_seq,
        max_records: config.batch_max_records as u32,
        max_bytes: config.batch_max_bytes as u32,
    };
    let mut payload = Vec::new();
    request.encode(&mut payload);

    let response = link
        .client
        .send(protocol::plain_frame(FrameKind::Pull, 0, &payload))
        .await
        .map_err(|e| format!("pulling from {} failed: {}", peer.name, e))?;

    let mut scratch = Vec::new();
    let (header, body) = codec
        .decode_frame(&response, &mut scratch)
        .map_err(|e| format!("{} answered with an unreadable frame: {}", peer.name, e))?;

    match header.kind {
        FrameKind::PullAck => {
            let records = protocol::decode_records(&mut Reader::new(body))
                .map_err(|e| format!("{} sent unreadable records: {}", peer.name, e))?;
            // The peer sets this when it had more than one answer's worth. A
            // caller that ignores it resynchronises the first batch and silently
            // stops.
            Ok((records, header.flags & protocol::FLAG_MORE != 0))
        }
        FrameKind::Error => Err(protocol::decode_error(body)),
        other => Err(format!("{} answered a PULL with {}", peer.name, other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::config::ReplicationConfig;
    use crate::replication::protocol::ApplyOutcome;

    fn replication() -> Arc<Replication> {
        let replication = Arc::new(Replication::new());
        replication
            .set_config(ReplicationConfig {
                node_id: 1,
                // Small, so the overflow path is reachable in a test.
                queue_capacity: 4,
                ..Default::default()
            })
            .unwrap();
        replication.open_storage(None).unwrap();
        replication.set_active(true);
        replication
    }

    fn record(seq: u64) -> Arc<TxnRecord> {
        Arc::new(TxnRecord {
            origin: 1,
            seq,
            commit_hlc: seq * 100,
            flags: 0,
            statements: vec!["INSERT INTO t (id) VALUES (1)".to_string()],
            tables: vec!["t".to_string()],
        })
    }

    /// A peer that cannot be reached must not be able to block a commit, and
    /// must not silently lose the records either.
    #[tokio::test]
    async fn a_full_queue_drops_and_asks_for_a_catch_up_instead_of_blocking() {
        let replication = replication();
        // An address nothing is listening on, so the task never drains.
        let peer = spawn(
            replication.clone(),
            &PeerConfig::new("unreachable", "127.0.0.1:1"),
        );

        for seq in 1..=100 {
            peer.enqueue(Outbound { record: record(seq), ack: None });
        }

        assert!(
            peer.dropped.load(Ordering::Relaxed) > 0,
            "a bounded queue must overflow rather than grow"
        );
        assert!(
            peer.needs_catchup.load(Ordering::Relaxed),
            "a drop must leave a note to backfill"
        );

        peer.stop();
    }

    #[tokio::test]
    async fn a_stopped_peer_accepts_nothing_more() {
        let replication = replication();
        let peer = spawn(replication.clone(), &PeerConfig::new("gone", "127.0.0.1:1"));
        peer.stop();

        let before = peer.dropped.load(Ordering::Relaxed);
        peer.enqueue(Outbound { record: record(1), ack: None });
        assert_eq!(peer.dropped.load(Ordering::Relaxed), before, "nothing should be counted");
    }

    #[tokio::test]
    async fn a_snapshot_reports_the_lag_against_the_local_stream() {
        let replication = replication();
        let peer = spawn(replication.clone(), &PeerConfig::new("behind", "127.0.0.1:1"));
        peer.acked_seq.store(7, Ordering::Relaxed);

        let snapshot = peer.snapshot(10);
        assert_eq!(snapshot.acked_seq, 7);
        assert_eq!(snapshot.lag, 3);
        assert!(!snapshot.connected);

        peer.stop();
    }

    #[tokio::test]
    async fn a_paused_peer_reports_itself_as_such() {
        let replication = replication();
        let peer = spawn(replication.clone(), &PeerConfig::new("paused", "127.0.0.1:1"));
        peer.set_paused(true);

        assert!(peer.snapshot(0).paused);
        peer.stop();
    }

    #[tokio::test]
    async fn a_batch_takes_everything_already_queued() {
        let replication = Arc::new(Replication::new());
        replication
            .set_config(ReplicationConfig {
                node_id: 1,
                batch_linger: Duration::ZERO,
                batch_max_records: 16,
                ..Default::default()
            })
            .unwrap();

        let (sender, receiver) = kanal::bounded_async::<Outbound>(64);
        for seq in 1..=5 {
            sender
                .send(Outbound { record: record(seq), ack: None })
                .await
                .unwrap();
        }

        let batch = next_batch(&replication, &receiver).await.unwrap();
        assert_eq!(batch.len(), 5, "all five were waiting, so all five should go together");
    }

    #[tokio::test]
    async fn a_batch_stops_at_its_record_limit() {
        let replication = Arc::new(Replication::new());
        replication
            .set_config(ReplicationConfig {
                node_id: 1,
                batch_linger: Duration::ZERO,
                batch_max_records: 3,
                ..Default::default()
            })
            .unwrap();

        let (sender, receiver) = kanal::bounded_async::<Outbound>(64);
        for seq in 1..=10 {
            sender
                .send(Outbound { record: record(seq), ack: None })
                .await
                .unwrap();
        }

        let batch = next_batch(&replication, &receiver).await.unwrap();
        assert_eq!(batch.len(), 3);
    }

    /// The durability claim a synchronous commit makes: a record the peer
    /// refused must not count towards it.
    #[tokio::test]
    async fn only_records_the_peer_took_are_settled() {
        let replication = replication();
        let peer = spawn(replication.clone(), &PeerConfig::new("p", "127.0.0.1:1"));

        let ack = PushAck {
            node_id: 2,
            clock: 1,
            outcomes: vec![
                ApplyOutcome { origin: 1, seq: 1, status: ApplyStatus::Applied, message: String::new() },
                ApplyOutcome { origin: 1, seq: 2, status: ApplyStatus::Duplicate, message: String::new() },
                ApplyOutcome {
                    origin: 1,
                    seq: 3,
                    status: ApplyStatus::Conflict,
                    message: "duplicate key".to_string(),
                },
                ApplyOutcome {
                    origin: 1,
                    seq: 4,
                    status: ApplyStatus::Failed,
                    message: "no such table".to_string(),
                },
                ApplyOutcome { origin: 1, seq: 5, status: ApplyStatus::Gap, message: String::new() },
            ],
            applied: Vec::new(),
        };

        let settled = record_outcomes(&replication, &peer, &ack);

        assert!(settled.contains(&record(1)), "applied counts");
        assert!(settled.contains(&record(2)), "the peer already having it counts");
        assert!(!settled.contains(&record(3)), "a refused record has not reached the peer");
        assert!(!settled.contains(&record(4)), "nor has a failed one");
        assert!(!settled.contains(&record(5)), "nor one that arrived over a hole");

        peer.stop();
    }

    #[tokio::test]
    async fn a_closed_channel_ends_the_stream_rather_than_hanging() {
        let replication = replication();
        let (sender, receiver) = kanal::bounded_async::<Outbound>(4);
        drop(sender);

        assert!(next_batch(&replication, &receiver).await.is_none());
    }
}
