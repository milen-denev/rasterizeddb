//! One outbound link to one peer.
//!
//! A peer is a handle plus a task. The task owns the connection and does
//! everything: connecting, reading the log, batching, compressing, waiting
//! for the acknowledgement, reconnecting. The commit path does not touch a
//! peer at all. A commit writes its record to the replication log and, once
//! it is durable, moves the node's release mark; each peer's task reads the
//! log from where its peer has got to, up to that mark.
//!
//! ## Why the sender reads the log
//!
//! Records used to reach a peer through a bounded queue filled by the commit
//! path, with the log as the fallback when the queue overflowed. That made
//! two paths with different orders and different failure modes: the queue
//! delivered in arrival order, which is not sequence order; an overflow
//! dropped records and flagged a catch-up from the file; a relayed record
//! that was dropped was never caught up at all. One path cannot disagree with
//! itself. The log is in sequence order, holds everything, and the newest few
//! thousand records are served from memory, so reading it costs what the
//! queue did.
//!
//! ## The peer says where it is
//!
//! What to send next is never this node's guess. Every handshake and every
//! acknowledgement carries the peer's own account of how far it has applied
//! each origin, and the next read starts there. A refused record is therefore
//! sent again, never skipped; a peer that was rewound is followed back.
//!
//! ## Batching
//!
//! Whatever is sendable goes in one frame, up to a record and byte limit,
//! after waiting at most `batch_linger` for more. One TLS record, one round
//! trip and one compression pass are amortised over a batch, and at low load
//! the batch is one record.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use cacheguard::CacheGuard;
use std::time::Duration;

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rastcp::client::{TcpClient, TcpClientBuilder};

use super::config::{Codec, Compression, NodeId, PeerConfig};
use super::manager::Replication;
use super::protocol::{
    self, ApplyStatus, FrameCodec, FrameKind, Hello, HelloAck, Pull, PushAck, Reader, TxnRecord,
    VectorEntry,
};

/// What a link is doing about its peer's backlog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerState {
    /// Sending, or nothing to send.
    Streaming,
    /// The peer refuses the next transaction (a conflict, a statement that
    /// fails there). Nothing after it can be applied; it is offered again,
    /// less and less often, until someone resolves it.
    Stalled,
    /// The peer needs history this node no longer holds. Nothing is sent:
    /// sending what is left would skip the missing part without a trace.
    NeedsReseed,
}

impl PeerState {
    fn from_wire(byte: u8) -> PeerState {
        match byte {
            1 => PeerState::Stalled,
            2 => PeerState::NeedsReseed,
            _ => PeerState::Streaming,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PeerState::Streaming => "streaming",
            PeerState::Stalled => "stalled",
            PeerState::NeedsReseed => "needs reseed",
        }
    }
}

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
    /// Records committed here that are not yet free to send to anyone: their
    /// commit is not durable yet.
    pub queued: usize,
    pub records_sent: u64,
    pub batches_sent: u64,
    /// Always zero: nothing is dropped. Kept so the status view's columns do
    /// not move.
    pub dropped: u64,
    pub codec: Codec,
    /// Compressed/uncompressed on the last batch, if it was compressed.
    pub last_ratio: Option<f64>,
    pub last_error: Option<String>,
    pub state: PeerState,
}

/// The handle: what everything other than the peer's own task sees.
pub struct PeerHandle {
    pub name: String,
    address: Mutex<String>,
    paused: AtomicBool,
    connected: AtomicBool,
    stopping: AtomicBool,
    remote_node: AtomicU64,
    /// Padded. This is the one field on the handle written by the ack path and
    /// read by everything else — a genuine cross-thread ping-pong, unlike the
    /// counters below it, which the peer's own task owns outright.
    acked_seq: CacheGuard<AtomicU64>,
    records_sent: AtomicU64,
    batches_sent: AtomicU64,
    state: AtomicU8,
    codec: AtomicU64,
    /// Last observed compression ratio, as `f64` bits. `0` means "not yet".
    last_ratio: AtomicU64,
    last_error: Mutex<Option<String>>,
    compression: Mutex<Option<Compression>>,
    /// Wakes the task out of an idle wait: stopped, resumed, readdressed.
    kick: tokio::sync::Notify,
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
        // Resuming needs no backfill of its own: the task reads the log from
        // where the peer is, and what committed during the pause is there.
        self.kick.notify_waiters();
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
        self.kick.notify_waiters();
    }

    pub fn acked_seq(&self) -> u64 {
        self.acked_seq.load(Ordering::Relaxed)
    }

    /// The node id the peer gave in its handshake, or 0 before it has.
    #[inline]
    pub fn remote_node(&self) -> NodeId {
        self.remote_node.load(Ordering::Relaxed)
    }

    pub fn state(&self) -> PeerState {
        PeerState::from_wire(self.state.load(Ordering::Relaxed))
    }

    fn set_state(&self, state: PeerState) -> bool {
        self.state.swap(state as u8, Ordering::Relaxed) != state as u8
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
        self.kick.notify_waiters();
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
            queued: 0,
            records_sent: self.records_sent.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            dropped: 0,
            codec: Codec::from_wire(self.codec.load(Ordering::Relaxed) as u8)
                .unwrap_or(Codec::None),
            last_ratio: self.ratio(),
            last_error: self.last_error.lock().clone(),
            state: self.state(),
        }
    }
}

/// Builds a peer's handle without a task behind it.
fn handle(config: &PeerConfig) -> Arc<PeerHandle> {
    Arc::new(PeerHandle {
        name: config.name.clone(),
        address: Mutex::new(config.address.clone()),
        paused: AtomicBool::new(config.paused),
        connected: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        remote_node: AtomicU64::new(0),
        acked_seq: AtomicU64::new(0).into(),
        records_sent: AtomicU64::new(0),
        batches_sent: AtomicU64::new(0),
        state: AtomicU8::new(PeerState::Streaming as u8),
        codec: AtomicU64::new(Codec::None.as_wire() as u64),
        last_ratio: AtomicU64::new(0),
        last_error: Mutex::new(None),
        compression: Mutex::new(config.compression),
        kick: tokio::sync::Notify::new(),
    })
}

/// Builds a peer and starts its task.
pub fn spawn(replication: Arc<Replication>, config: &PeerConfig) -> Arc<PeerHandle> {
    let handle = handle(config);
    let task_handle = handle.clone();
    tokio::spawn(async move {
        run(replication, task_handle).await;
    });
    handle
}

/// The peer's task: connect, then stream until told to stop.
async fn run(replication: Arc<Replication>, peer: Arc<PeerHandle>) {
    let mut backoff = replication.config().reconnect_base;
    let mut codec = FrameCodec::new(replication.config().compression.level);

    while !peer.is_stopping() {
        if peer.is_paused() {
            let _ = tokio::time::timeout(Duration::from_millis(200), peer.kick.notified()).await;
            continue;
        }

        match connect(&replication, &peer).await {
            Ok(mut link) => {
                peer.connected.store(true, Ordering::Relaxed);
                if peer.state() == PeerState::Streaming {
                    peer.set_error(None);
                }
                let acked_before = peer.acked_seq();
                info!(
                    "Replication link to {} ({}) established, node {}, codec {}",
                    peer.name, link.address, link.remote_node, link.codec
                );

                if let Err(e) = stream(&replication, &peer, &mut link, &mut codec).await {
                    warn!("Replication link to {} ended: {}", peer.name, e);
                    peer.set_error(Some(e));
                }

                // Connecting is not progress. Reset the backoff only when the
                // link actually moved the peer forward, so a link that fails
                // the same way each time decays to `reconnect_max` while a
                // healthy flapping one still recovers immediately.
                if peer.acked_seq() > acked_before {
                    backoff = replication.config().reconnect_base;
                }

                peer.connected.store(false, Ordering::Relaxed);
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

        let _ = tokio::time::timeout(backoff, peer.kick.notified()).await;
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
    /// How far the peer has applied each origin, by its own last account.
    applied: HashMap<NodeId, u64>,
}

impl Link {
    fn take_vector(&mut self, vector: &[VectorEntry]) {
        self.applied = vector.iter().map(|entry| (entry.origin, entry.seq)).collect();
    }

    fn position(&self, origin: NodeId) -> u64 {
        self.applied.get(&origin).copied().unwrap_or(0)
    }
}

/// Dials the peer and completes the handshake.
async fn connect(replication: &Arc<Replication>, peer: &Arc<PeerHandle>) -> Result<Link, String> {
    let config = replication.config();
    let address = peer.address();

    let mut builder = TcpClientBuilder::new(&address)
        .timeout(config.request_timeout)
        .max_message_size(protocol::MAX_PAYLOAD + protocol::HEADER_LEN)
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
    peer.codec
        .store(ack.codec.as_wire() as u64, Ordering::Relaxed);

    let mut link = Link {
        client,
        address,
        remote_node: ack.node_id,
        codec: ack.codec,
        applied: HashMap::new(),
    };
    link.take_vector(&ack.applied);

    // What the peer says it has of ours is where the next send starts.
    let ours = link.position(config.node_id);
    if let Some(was) = replication.adopt_peer_position(ours) {
        error!(
            "{} has applied this node's stream up to {}, but this node's replication log ends at {}: \
             the log lost its tail. Local sequence moved to {} so no number the peer holds is reused",
            peer.name, ours, was, ours
        );
    }
    peer.acked_seq.store(ours, Ordering::Relaxed);
    replication.note_ack();

    Ok(link)
}

/// How long an idle sender waits before looking again without being woken.
///
/// A release wakes it at once. This is for what does not: a commit that did
/// not wait for its sync becomes sendable when the log writer's next round
/// makes it durable, and nothing announces that.
const IDLE_POLL: Duration = Duration::from_millis(200);

/// The same, while commits are waiting on exactly that.
const GATED_POLL: Duration = Duration::from_millis(10);

/// Streams records until the link fails or the peer is stopped.
async fn stream(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    link: &mut Link,
    codec: &mut FrameCodec,
) -> Result<(), String> {
    // One buffer for the life of the link.
    let mut frame = Vec::new();
    let node_id = replication.node_id();
    // How long to leave a peer alone after it refused what it was offered.
    let mut refusal_pause = Duration::ZERO;

    loop {
        if peer.is_stopping() || peer.is_paused() {
            return Ok(());
        }

        // Taken before the look, so a release during the look still wakes
        // the wait below.
        let woken = replication.woken();
        tokio::pin!(woken);
        woken.as_mut().enable();

        let config = replication.config();
        let mut outcome = send_from(
            replication,
            peer,
            link,
            codec,
            &mut frame,
            node_id,
            replication.released_through(),
        )
        .await?;

        if config.relay {
            // Everything this node has logged of other origins, for a peer
            // that may not hear them first-hand. Not the peer's own stream,
            // which it has by definition.
            if let Some(log) = replication.log() {
                for origin in log.origins() {
                    if origin == node_id || origin == link.remote_node {
                        continue;
                    }
                    let sent = send_from(
                        replication,
                        peer,
                        link,
                        codec,
                        &mut frame,
                        origin,
                        log.highest(origin),
                    )
                    .await?;
                    outcome = outcome.max(sent);
                }
            }
        }

        match outcome {
            Sent::Advanced => {
                refusal_pause = Duration::ZERO;
                continue;
            }
            Sent::Refused => {
                // Offered again, less and less often: a stalled stream is
                // quiet rather than a spin, and picks up by itself once
                // whatever blocked it is resolved.
                refusal_pause = if refusal_pause.is_zero() {
                    config.reconnect_base
                } else {
                    (refusal_pause * 2).min(config.reconnect_max)
                };
                let _ = tokio::time::timeout(refusal_pause, peer.kick.notified()).await;
            }
            Sent::Nothing => {
                let poll = if replication.unreleased() > 0 {
                    GATED_POLL
                } else {
                    IDLE_POLL
                };
                tokio::select! {
                    _ = &mut woken => {}
                    _ = peer.kick.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }
    }
}

/// What one look at one origin's stream came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Sent {
    /// The peer has everything sendable.
    Nothing,
    /// A batch went and the peer took none of it.
    Refused,
    /// The peer moved forward.
    Advanced,
}

/// Sends the peer the next batch of `origin`'s stream, from where the peer
/// says it is up to `upto`.
async fn send_from(
    replication: &Arc<Replication>,
    peer: &Arc<PeerHandle>,
    link: &mut Link,
    codec: &mut FrameCodec,
    frame: &mut Vec<u8>,
    origin: NodeId,
    upto: u64,
) -> Result<Sent, String> {
    let Some(log) = replication.log() else {
        return Ok(Sent::Nothing);
    };
    let from = link.position(origin);
    if from >= upto {
        return Ok(Sent::Nothing);
    }
    let own = origin == replication.node_id();

    if !log.can_serve(origin, from) {
        let message = format!(
            "{} has origin {}'s stream up to {} and this node's log now starts at {}: the \
             transactions between are no longer held here. Nothing more is sent, because sending \
             the rest would skip them. Reseed {} from a backup, or raise \
             --replication-retention-mb before a peer falls this far behind again",
            peer.name,
            origin,
            from,
            log.earliest(origin),
            peer.name
        );
        if peer.set_state(PeerState::NeedsReseed) {
            error!("{}", message);
        }
        // Ends the link: the next handshake finds out whether the peer has
        // been reseeded since.
        return Err(message);
    }

    let config = replication.config();
    // At low load the first record is all there is; a short wait lets the
    // commits right behind it share its frame.
    if own && !config.batch_linger.is_zero() && upto - from < config.batch_max_records as u64 {
        tokio::time::sleep(config.batch_linger).await;
    }
    let upto = if own { replication.released_through() } else { upto };

    let (records, _more) = match log.read_range(
        origin,
        from,
        upto,
        config.batch_max_records,
        config.batch_max_bytes,
    ) {
        Ok(read) => read,
        // A compaction swapped the file under the read. Look again.
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => return Ok(Sent::Advanced),
        Err(e) => return Err(format!("could not read the replication log: {}", e)),
    };
    if records.is_empty() {
        return Ok(Sent::Nothing);
    }

    // The stream has no holes by construction. One here means the log lost
    // records (a damaged file cut short at open): the peer cannot be given
    // what follows, and must not be told it is in step.
    let mut expected = from + 1;
    for record in &records {
        if record.seq != expected {
            let message = format!(
                "origin {}'s transactions {}..{} are missing from this node's replication log; \
                 {} cannot be brought past {} from here and must be reseeded",
                origin,
                expected,
                record.seq - 1,
                peer.name,
                from
            );
            if peer.set_state(PeerState::NeedsReseed) {
                error!("{}", message);
            }
            return Err(message);
        }
        expected += 1;
    }

    let refs: Vec<&TxnRecord> = records.iter().collect();
    let ack = push(replication, peer, link, codec, &refs, frame).await?;
    link.take_vector(&ack.applied);
    if own {
        // Stored, not raised: a peer that was rewound (`RESYNC … FROM`) is
        // followed back, or the status view would claim a position the peer
        // no longer has.
        peer.acked_seq.store(link.position(origin), Ordering::Relaxed);
        replication.note_ack();
    }

    if link.position(origin) > from {
        if peer.set_state(PeerState::Streaming) {
            info!("Peer {} is applying origin {}'s stream again", peer.name, origin);
        }
        peer.set_error(None);
        return Ok(Sent::Advanced);
    }

    // Nothing moved. The first record's outcome says why.
    let first = ack
        .outcomes
        .iter()
        .find(|outcome| outcome.origin == origin && !outcome.status.is_settled());
    match first {
        Some(outcome) if matches!(outcome.status, ApplyStatus::Conflict | ApplyStatus::Failed) => {
            let message = format!(
                "{} cannot apply transaction {}:{}: {}",
                peer.name, outcome.origin, outcome.seq, outcome.message
            );
            if peer.set_state(PeerState::Stalled) {
                error!(
                    "{}. Its stream from origin {} is stalled until that is resolved",
                    message, origin
                );
            }
            peer.set_error(Some(message));
        }
        _ => {}
    }
    Ok(Sent::Refused)
}

/// Sends one batch and returns what the peer said about it.
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
) -> Result<PushAck, String> {
    let config = replication.config();
    let compression = peer.compression_override().unwrap_or(config.compression);
    codec.set_level(compression.level);

    // The peer only advertised what it can decode; sending it something else
    // would be a decode failure at the far end rather than here.
    let compression = if link.codec == Codec::None {
        Compression {
            mode: super::config::CompressionMode::Off,
            codec: Codec::None,
            ..compression
        }
    } else {
        Compression {
            codec: link.codec,
            ..compression
        }
    };

    let mut payload =
        Vec::with_capacity(8 + records.iter().map(|r| r.encoded_size_hint()).sum::<usize>());
    protocol::encode_push(
        config.node_id,
        records.iter().copied(),
        records.len(),
        &mut payload,
    );

    let used = codec
        .encode_frame(
            FrameKind::Push,
            0,
            &payload,
            compression,
            peer.ratio(),
            frame,
        )
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
    count_outcomes(replication, peer, &ack);
    Ok(ack)
}

/// Folds an acknowledgement into the node's counters.
fn count_outcomes(replication: &Arc<Replication>, peer: &Arc<PeerHandle>, ack: &PushAck) {
    let stats = replication.stats();
    for outcome in &ack.outcomes {
        match outcome.status {
            ApplyStatus::Applied => {
                stats.applied.fetch_add(1, Ordering::Relaxed);
            }
            ApplyStatus::Duplicate => {
                stats.duplicates.fetch_add(1, Ordering::Relaxed);
            }
            ApplyStatus::Conflict => {
                stats.conflicts.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "{} refused transaction {}:{} as a conflict: {}",
                    peer.name, outcome.origin, outcome.seq, outcome.message
                );
            }
            ApplyStatus::Failed => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "{} could not apply transaction {}:{}: {}",
                    peer.name, outcome.origin, outcome.seq, outcome.message
                );
            }
            ApplyStatus::Gap => {
                stats.gaps.fetch_add(1, Ordering::Relaxed);
            }
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
                ..Default::default()
            })
            .unwrap();
        replication.open_storage(None).unwrap();
        replication.set_active(true);
        replication
    }

    #[tokio::test]
    async fn a_snapshot_reports_the_lag_against_the_local_stream() {
        let replication = replication();
        let peer = spawn(
            replication.clone(),
            &PeerConfig::new("behind", "127.0.0.1:1"),
        );
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
        let peer = spawn(
            replication.clone(),
            &PeerConfig::new("paused", "127.0.0.1:1"),
        );
        peer.set_paused(true);

        assert!(peer.snapshot(0).paused);
        peer.stop();
    }

    /// Every kind of outcome is counted, and none of them moves a position:
    /// only the peer's own account of where it is does that.
    #[tokio::test]
    async fn outcomes_are_counted_by_kind() {
        let replication = replication();
        let peer = handle(&PeerConfig::new("p", "127.0.0.1:1"));
        let outcome = |seq, status| ApplyOutcome {
            origin: 1,
            seq,
            status,
            message: String::new(),
        };
        let ack = PushAck {
            node_id: 2,
            clock: 1,
            outcomes: vec![
                outcome(1, ApplyStatus::Applied),
                outcome(2, ApplyStatus::Duplicate),
                outcome(3, ApplyStatus::Conflict),
                outcome(4, ApplyStatus::Failed),
                outcome(5, ApplyStatus::Gap),
            ],
            applied: Vec::new(),
        };

        count_outcomes(&replication, &peer, &ack);

        let stats = replication.stats();
        assert_eq!(stats.applied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.duplicates.load(Ordering::Relaxed), 1);
        assert_eq!(stats.conflicts.load(Ordering::Relaxed), 1);
        assert_eq!(stats.failures.load(Ordering::Relaxed), 1);
        assert_eq!(stats.gaps.load(Ordering::Relaxed), 1);
        assert_eq!(peer.acked_seq(), 0);
    }
}
