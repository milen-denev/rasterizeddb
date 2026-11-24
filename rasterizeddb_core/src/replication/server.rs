//! The inbound half: a listener that takes frames from peers and applies what
//! they carry.
//!
//! It runs on its own `rastcp` listener, on its own port, rather than sharing
//! the SQL one. Two reasons. The SQL port speaks a text protocol whose replies
//! are status bytes and rows, and multiplexing a binary framing onto it would
//! mean every SQL request paying for a discriminator it never needs. And a
//! deployment usually wants the two exposed differently — the SQL port to
//! applications, the replication port to the other nodes and nothing else —
//! which is a firewall rule, and a firewall rule needs a port.
//!
//! ## Applying is just executing
//!
//! A record carries statements, so applying one is running them against the
//! local executor inside a transaction of its own. The one thing that differs
//! from a client's transaction is that it is marked as an apply, so the
//! capture hook ignores it — otherwise a two-node mesh would bounce every
//! transaction between its members forever.

use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;

use log::{debug, error, info, warn};
use rclite::Arc;

use crate::core::database::Database;
use crate::core::rql::executor::execute_in_txn;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::config::{Codec, ConflictPolicy, NodeId};

/// No peer handed this record over — it was pulled, not pushed.
const NO_SENDER: NodeId = 0;
use super::manager::Replication;
use super::protocol::{
    self, ApplyOutcome, ApplyStatus, FrameCodec, FrameKind, Hello, HelloAck, Pull, PushAck, Reader,
    TxnRecord,
};

thread_local! {
    /// One codec per worker thread rather than one per frame.
    ///
    /// Zstd's decompression context owns a window buffer, and building one for
    /// every batch would put an allocation and a reset on the receive path for
    /// no reason — the handler is stateless, but the thread running it is not.
    static CODEC: std::cell::RefCell<FrameCodec> =
        std::cell::RefCell::new(FrameCodec::default());
}

/// Binds the replication listener.
///
/// Deliberately synchronous and separate from [`serve`]: binding is the step
/// that fails — a port already held, a certificate that will not load — and the
/// operator who asked for the listener has to be told. Doing it inside the
/// serving task would report success and then log a failure nobody is watching
/// for.
pub fn bind(router: &StdArc<Router>) -> std::io::Result<rastcp::server::TcpServer> {
    bind_on(router, None)
}

/// The databases the node's one replication listener serves: the default
/// database, and through the catalogue every other one.
///
/// Every database replicates over links of its own, one per database and
/// peer, and a link's greeting names its database (`Hello::database`, none
/// for the default). The listener routes a connection by that name at its
/// greeting and serves every frame after it to that database's replication.
pub struct Router {
    default: Arc<Database>,
    catalog: Option<Arc<crate::core::database::catalog::DatabaseCatalog>>,
    /// Applies the default stream's `CREATE DATABASE` / `DROP DATABASE`
    /// records. Owned here, for as long as the listener runs; each database's
    /// replication holds it weakly ([`Replication::link_catalog`]).
    _link: Option<StdArc<dyn super::manager::CatalogLink>>,
}

impl Router {
    /// A router over `default` and, when given, the catalogue it belongs to.
    /// Without one, only the default database is served, as before.
    pub fn new(
        default: Arc<Database>,
        catalog: Option<Arc<crate::core::database::catalog::DatabaseCatalog>>,
        link: Option<StdArc<dyn super::manager::CatalogLink>>,
    ) -> Router {
        Router { default, catalog, _link: link }
    }

    /// The default database's replication: the listener's configuration.
    pub fn replication(&self) -> StdArc<Replication> {
        self.default.replication.clone()
    }

    /// The database a greeting named: the default one for none.
    fn database(&self, name: Option<&str>) -> Result<Arc<Database>, String> {
        let Some(name) = name else {
            return Ok(self.default.clone());
        };
        let found = self.catalog.as_ref().and_then(|catalog| catalog.get(name));
        match found {
            Some(database) if database.replication.is_active() && database.replication.routed_database().as_deref() == Some(name) => {
                Ok(database)
            }
            Some(_) => Err(format!("database \"{name}\" does not replicate on this node")),
            None => Err(format!("database \"{name}\" does not exist on this node")),
        }
    }

    /// Every database this listener serves, the default one first.
    fn databases(&self) -> Vec<Arc<Database>> {
        let mut out = vec![self.default.clone()];
        if let Some(catalog) = &self.catalog {
            for name in catalog.names() {
                if let Some(database) = catalog.get(&name) {
                    if !Arc::ptr_eq(&database, &self.default) {
                        out.push(database);
                    }
                }
            }
        }
        out
    }
}

/// [`bind`], serving `listener` when one is given instead of binding
/// `listen_addr:listen_port`. The listener comes from
/// [`Replication::serve_on`], which says why a caller would hand one over.
pub fn bind_on(
    router: &StdArc<Router>,
    listener: Option<std::net::TcpListener>,
) -> std::io::Result<rastcp::server::TcpServer> {
    let replication = router.replication();
    let config = replication.config();

    if let Err(e) = config.validate() {
        error!("Replication not started: {}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
    }

    let mut builder =
        rastcp::server::TcpServerBuilder::new(&config.listen_addr, config.listen_port)
            .request_timeout(config.request_timeout)
            // One transaction is one record and a record is not split: the
            // transport must take the largest frame the protocol allows, or a
            // single large transaction stops its link for good.
            .max_message_size(protocol::MAX_PAYLOAD + protocol::HEADER_LEN)
            // What callers must prove before they reach the handler. Under
            // anything but `Insecure` an unauthenticated caller never gets to
            // send a frame at all.
            .client_auth(
                config
                    .tls
                    .client_auth(config.auth)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            );
    if let Some(listener) = listener {
        builder = builder.with_listener(listener);
    }

    // Who is calling is settled once, when the connection opens, and then
    // known for every frame on it. See [`Greeter`].
    builder = builder.authenticator(StdArc::new(Greeter {
        router: router.clone(),
    }));

    // Present this node's own certificate rather than a freshly generated one,
    // so the fingerprint the peers pinned stays the same across restarts.
    match config
        .tls
        .identity()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
    {
        Some((certs, key)) => builder = builder.with_certificates(certs, key),
        None => {
            if config.auth.authenticates() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "authenticated replication needs this node's certificate and key",
                ));
            }
        }
    }

    let server = builder.build_sync().map_err(|e| {
        std::io::Error::other(format!(
            "could not bind the replication listener on {}:{}: {}",
            config.listen_addr, config.listen_port, e
        ))
    })?;

    info!(
        "Replication listening on {}:{} as node {} of cluster '{}', authentication {}",
        config.listen_addr, config.listen_port, config.node_id, config.cluster, config.auth
    );
    // Printed so setting a cluster up does not need a detour through
    // `openssl`: this is the value the other nodes pin.
    // A replication listener is always TCP, and so always has one.
    if let Some(fingerprint) = server.certificate_fingerprint() {
        info!(
            "Replication certificate fingerprint: {}",
            rastcp::tls::format_fingerprint(&fingerprint)
        );
    }
    if !config.auth.authenticates() {
        warn!(
            "Replication authentication is off. The link is encrypted but not authenticated, \
             so anyone able to intercept it can impersonate a peer. This is only safe on \
             loopback or an already-trusted network."
        );
    }

    Ok(server)
}

/// Serves the bound listener until shutdown, and keeps the log flushed.
///
/// Returns once the listener has drained.
pub async fn serve(router: StdArc<Router>, server: rastcp::server::TcpServer) -> std::io::Result<()> {
    let shutdown = server.shutdown_handle();
    crate::shutdown::on_termination(move || shutdown.shutdown());

    // Neither file is flushed per commit — see `super::log` — so something has
    // to close the window between a write and its durability. Every
    // database's, each its own files.
    let flusher = router.clone();
    let flush_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // An fsync is blocking I/O; run on a runtime worker it parks that
            // worker and every task queued behind it for the whole sync.
            for database in flusher.databases() {
                let replication = database.replication.clone();
                let _ = tokio::task::spawn_blocking(move || replication.flush()).await;
            }
        }
    });

    let outcome = server
        .run_with_context_and_principal(router.clone(), handle_frame)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()));

    flush_task.abort();

    // Everything the log and the state buffered has to reach disk before the
    // process leaves, or a peer's catch-up point goes back in time.
    for database in router.databases() {
        database.replication.flush();
    }
    info!("Replication listener stopped");
    outcome
}

/// Greets a connection before anything else is served on it.
///
/// A sender's first frame on every connection it opens is a `HELLO`: which
/// node it is, which cluster it belongs to, and the cluster's token. That used
/// to be answered like any other request, by a handler that knew nothing of
/// the connection a frame arrived on — so the token was checked when a caller
/// chose to present it and at no other time. A caller that never greeted, or
/// greeted with the wrong token and was refused, could go on to `PULL` this
/// node's stream or `PUSH` into it on the same connection.
///
/// The transport runs this once per connection, before the first request, and
/// serves nothing on a connection it refuses. It reads the first frame, which
/// has to be a `HELLO`, checks it exactly as before, and answers it exactly as
/// before — so a sender, which sends its `HELLO` and waits for the answer,
/// cannot tell the difference, and a node with this talks to one without it in
/// either direction. What it adds is the caller's node id, handed to
/// [`handle_frame`] with every frame that follows.
struct Greeter {
    router: StdArc<Router>,
}

impl std::fmt::Debug for Greeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Greeter").finish_non_exhaustive()
    }
}

impl rastcp::session::ServerAuthenticator for Greeter {
    fn authenticate<'a>(
        &'a self,
        channel: &'a mut rastcp::session::AuthChannel<'_>,
    ) -> rastcp::session::AuthFuture<'a, rastcp::session::Principal> {
        Box::pin(async move {
            let frame = channel.recv().await?;

            let greeting = {
                let mut scratch = Vec::new();
                FrameCodec::default()
                    .decode_frame(&frame, &mut scratch)
                    .map(|(header, payload)| (header.kind, payload.to_vec()))
                    .map_err(|e| e.to_string())
            };

            let (reply, caller) = match greeting {
                Ok((FrameKind::Hello, payload)) => {
                    // The database the link is for, routed here: its
                    // replication answers the greeting with its own
                    // positions. One that is not here, or does not
                    // replicate, is refused, and the sender tries again
                    // later (a `CREATE DATABASE` record still on its way).
                    let named = Hello::decode(&mut Reader::new(&payload)).ok().and_then(|hello| hello.database);
                    match self.router.database(named.as_deref()) {
                        Ok(database) => {
                            let reply = handle_hello(&database.replication, &payload);
                            let caller = acknowledged_caller(&payload, &reply).map(|node| (node, named));
                            (reply, caller)
                        }
                        Err(e) => (protocol::error_frame(&e), None),
                    }
                }
                Ok((other, _)) => (
                    protocol::error_frame(&format!(
                        "a connection opens with HELLO; this one opened with {}",
                        other
                    )),
                    None,
                ),
                Err(e) => {
                    warn!("Rejecting a replication greeting: {}", e);
                    (protocol::error_frame(&e), None)
                }
            };

            // Answered either way: a caller that is refused is told why, as
            // it was when the greeting was an ordinary request.
            channel.send(&reply).await?;

            match caller {
                Some((node, database)) => {
                    Ok(rastcp::session::Principal::new(session_name(node, database.as_deref()), false))
                }
                None => Err(rastcp::error::RastcpError::AuthenticationError(format!(
                    "the replication greeting from {} was refused",
                    channel.peer()
                ))),
            }
        })
    }
}

/// The node a `HELLO` named, if `reply` — this node's answer to it — is an
/// acknowledgement and not a refusal.
fn acknowledged_caller(hello: &[u8], reply: &[u8]) -> Option<NodeId> {
    let acknowledged =
        protocol::parse_header(reply).is_ok_and(|header| header.kind == FrameKind::HelloAck);
    if !acknowledged {
        return None;
    }

    Hello::decode(&mut Reader::new(hello)).ok().map(|hello| hello.node_id)
}

/// The name a greeted connection's caller goes by in the transport:
/// `node:3` on the default database's link, `node:3@sales` on another's.
fn session_name(node: NodeId, database: Option<&str>) -> String {
    match database {
        Some(database) => format!("node:{node}@{database}"),
        None => format!("node:{node}"),
    }
}

/// The node a connection was greeted as, and the database it was greeted
/// for (`None`: the default one); `None` for a connection that was served
/// without a greeting — which this listener does not do, and which is
/// refused all the same rather than trusted on that account.
fn greeted_node(principal: &rastcp::session::Principal) -> Option<(NodeId, Option<String>)> {
    let rest = principal.user.strip_prefix("node:")?;
    match rest.split_once('@') {
        Some((node, database)) => Some((node.parse().ok()?, Some(database.to_string()))),
        None => Some((rest.parse().ok()?, None)),
    }
}

/// Answers one frame from the node `principal` was greeted as.
async fn handle_frame(
    router: StdArc<Router>,
    principal: StdArc<rastcp::session::Principal>,
    request: Vec<u8>,
) -> Vec<u8> {
    let Some((caller, named)) = greeted_node(&principal) else {
        return protocol::error_frame("this connection was not opened with HELLO");
    };
    // The database the connection was greeted for, looked up again for each
    // frame: one dropped since answers no more of them.
    let database = match router.database(named.as_deref()) {
        Ok(database) => database,
        Err(e) => return protocol::error_frame(&e),
    };
    let replication = database.replication.clone();

    // Decode with the thread's codec, then let go of it: the borrow cannot be
    // held across the `await`s below, and the payload has to outlive it, so it
    // is copied out. For an uncompressed frame that is the only copy made.
    let decoded = CODEC.with(|cell| {
        let mut codec = cell.borrow_mut();
        let mut scratch = Vec::new();
        match codec.decode_frame(&request, &mut scratch) {
            Ok((header, body)) => Ok((header, body.to_vec())),
            Err(e) => Err(e.to_string()),
        }
    });

    let (header, payload) = match decoded {
        Ok(decoded) => decoded,
        Err(e) => {
            warn!("Rejecting a replication frame: {}", e);
            return protocol::error_frame(&e);
        }
    };

    match header.kind {
        // Greeting again on a connection already greeted changes nothing
        // about who is on it, so it may only be the same node saying so.
        FrameKind::Hello => {
            match Hello::decode(&mut Reader::new(&payload)) {
                Ok(hello) if hello.node_id != caller => protocol::error_frame(&format!(
                    "this connection was opened by node {}, and cannot be taken over by node {}",
                    caller, hello.node_id
                )),
                _ => handle_hello(&replication, &payload),
            }
        }
        FrameKind::Push => handle_push(&database, &replication, caller, &payload).await,
        FrameKind::Pull => handle_pull(&replication, caller, &payload),
        FrameKind::Digest => super::check::answer(&database, &payload).await,
        FrameKind::Ping => {
            let mut out = Vec::with_capacity(8);
            out.extend_from_slice(&replication.clock().now().to_le_bytes());
            protocol::plain_frame(FrameKind::Pong, 0, &out)
        }
        other => protocol::error_frame(&format!("{} is not a request", other)),
    }
}

/// Validates a caller and tells it where this node has got to.
fn handle_hello(replication: &StdArc<Replication>, payload: &[u8]) -> Vec<u8> {
    let hello = match Hello::decode(&mut Reader::new(payload)) {
        Ok(hello) => hello,
        Err(e) => return protocol::error_frame(&format!("unreadable HELLO: {}", e)),
    };

    let config = replication.config();

    if hello.cluster_id != config.cluster_id() {
        warn!(
            "Refusing node {} at {}: it belongs to a different cluster",
            hello.node_id, hello.advertised_addr
        );
        return protocol::error_frame(
            "cluster mismatch: the two nodes disagree about which cluster they are in",
        );
    }

    // Compared in constant time: the token is a shared secret, and a
    // byte-at-a-time comparison leaks how much of a guess was right.
    if !config.token.is_empty() {
        use subtle::ConstantTimeEq;
        let presented = hello.token.as_bytes();
        let expected = config.token.as_bytes();
        let matches = presented.len() == expected.len() && bool::from(presented.ct_eq(expected));
        if !matches {
            warn!(
                "Refusing node {}: the replication token does not match",
                hello.node_id
            );
            return protocol::error_frame("authentication failed: replication token mismatch");
        }
    }

    if hello.node_id == config.node_id {
        return protocol::error_frame(
            "node id collision: the caller reports the same id as this node",
        );
    }
    if hello.node_id == 0 {
        return protocol::error_frame("the caller has no node id");
    }

    replication.clock().observe(hello.clock);

    let codec = protocol::negotiate(Codec::local_mask(), hello.codecs, config.compression.codec);

    debug!(
        "Node {} at {} connected; answering with codec {}",
        hello.node_id, hello.advertised_addr, codec
    );

    let ack = HelloAck {
        node_id: config.node_id,
        cluster_id: config.cluster_id(),
        codec,
        clock: replication.clock().now(),
        applied: replication.applied().vector(),
    };
    let mut out = Vec::new();
    ack.encode(&mut out);
    protocol::plain_frame(FrameKind::HelloAck, 0, &out)
}

/// Applies a batch and reports on each record.
async fn handle_push(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    caller: NodeId,
    payload: &[u8],
) -> Vec<u8> {
    let (sender, records) = match protocol::decode_push(&mut Reader::new(payload)) {
        Ok(decoded) => decoded,
        Err(e) => return protocol::error_frame(&format!("unreadable PUSH: {}", e)),
    };

    // A batch says who sends it, and that decides what is applied and what is
    // taken as already held. It has to be the node the connection was opened
    // by, not whichever node the batch would like to be taken for.
    if sender != caller {
        warn!(
            "Refusing a PUSH sent as node {} on a connection opened by node {}",
            sender, caller
        );
        return protocol::error_frame(&format!(
            "this connection was opened by node {}; a PUSH on it cannot be sent as node {}",
            caller, sender
        ));
    }

    // In a task of its own. The transport drops this future when its request
    // timeout passes or the connection goes, and an apply dropped between two
    // statements leaves a transaction open for good, holding its row locks.
    // The task runs the batch to its end whatever becomes of the request; a
    // sender that gave up and sent it again waits its turn and is told the
    // records are already held.
    let began = std::time::Instant::now();
    let task = {
        let database = database.clone();
        let replication = replication.clone();
        tokio::spawn(async move { apply_batch(&database, &replication, records, sender).await })
    };
    let outcomes = match task.await {
        Ok(outcomes) => outcomes,
        Err(e) => return protocol::error_frame(&format!("the apply did not finish: {}", e)),
    };
    let stats = replication.stats();
    stats.batches_applied.fetch_add(1, Ordering::Relaxed);
    stats
        .apply_us
        .fetch_add(began.elapsed().as_micros() as u64, Ordering::Relaxed);

    let ack = PushAck {
        node_id: replication.node_id(),
        clock: replication.clock().now(),
        outcomes,
        applied: replication.applied().vector(),
    };
    let mut out = Vec::new();
    ack.encode(&mut out);
    protocol::plain_frame(FrameKind::PushAck, 0, &out)
}

/// Serves history from the log.
fn handle_pull(replication: &StdArc<Replication>, caller: NodeId, payload: &[u8]) -> Vec<u8> {
    let request = match Pull::decode(&mut Reader::new(payload)) {
        Ok(request) => request,
        Err(e) => return protocol::error_frame(&format!("unreadable PULL: {}", e)),
    };

    if request.node_id != caller {
        return protocol::error_frame(&format!(
            "this connection was opened by node {}; a PULL on it cannot be made as node {}",
            caller, request.node_id
        ));
    }

    let Some(log) = replication.log() else {
        return protocol::error_frame("this node keeps no replication log");
    };

    if !log.can_serve(request.origin, request.after_seq) {
        return protocol::error_frame(&format!(
            "sequence {} for origin {} is older than this node's retention; \
             a full resynchronisation is needed",
            request.after_seq + 1,
            request.origin
        ));
    }

    // Of this node's own stream, only what is settled and durable here: a
    // puller must not get ahead of what a pushed peer would be sent.
    let upto = if request.origin == replication.node_id() {
        replication.released_through()
    } else {
        u64::MAX
    };
    let (records, more) = match log.read_range(
        request.origin,
        request.after_seq,
        upto,
        request.max_records as usize,
        request.max_bytes as usize,
    ) {
        Ok(result) => result,
        Err(e) => return protocol::error_frame(&format!("could not read the log: {}", e)),
    };

    let mut out = Vec::new();
    protocol::encode_records(records.iter(), records.len(), &mut out);
    protocol::plain_frame(
        FrameKind::PullAck,
        if more { protocol::FLAG_MORE } else { 0 },
        &out,
    )
}

/// Refuses a record without applying it, with a reason.
fn refuse(record: &TxnRecord, status: ApplyStatus, message: impl Into<String>) -> ApplyOutcome {
    ApplyOutcome {
        origin: record.origin,
        seq: record.seq,
        status,
        message: message.into(),
    }
}

/// A record applied and committed in memory, waiting for the sync that
/// makes it durable.
struct Waiting {
    /// `None` for a key/value record, which has no transaction behind it.
    owner: Option<crate::core::mvcc::owner::OwnerRelease>,
    record: TxnRecord,
    /// Where its outcome sits in the batch's answer.
    answer: usize,
}

/// The records of one origin applied since the last sync.
///
/// Each record is its own transaction, committed in order. What they share is
/// the `fdatasync`: one a batch, not one a record. The origin commits many
/// writers under one sync, so a peer paying one each could never keep up with
/// it, and falling behind is what every other failure here starts from.
/// Nothing is acknowledged before the sync, and if the sync fails nothing of
/// the batch is acknowledged at all: the origin's stream is halted and the
/// position reported goes back to where it stood (see `finish`).
struct Group {
    origin: NodeId,
    /// The position before the first record waiting.
    before: u64,
    waiting: Vec<Waiting>,
}

impl Group {
    fn new(origin: NodeId) -> Group {
        Group {
            origin,
            before: 0,
            waiting: Vec::new(),
        }
    }

    /// The one sync, and what follows from how it went.
    fn finish(
        &mut self,
        database: &Arc<Database>,
        replication: &StdArc<Replication>,
        from_node: NodeId,
        outcomes: &mut [ApplyOutcome],
    ) {
        if self.waiting.is_empty() {
            return;
        }
        let waiting = std::mem::take(&mut self.waiting);
        // The two halves of `Database::sync_own_location`, told apart,
        // because only the first decides whether the batch is durable.
        let synced = database.sync_own_log();
        if synced.is_ok() {
            // The log holds the rows, their outcomes and the intents, so the
            // batch is durable and is acknowledged as such. What can still
            // fail is the copy of those outcomes into the status file, which
            // the log already covers: the ranges stay staged and the next
            // sync, or a restart's replay, writes them. That is a disk worth
            // an operator's attention (the callee logs it as an error) and
            // not a reason to stop a stream whose every byte is safe; the
            // log writer's own round treats it the same way.
            let _ = database.apply_staged_outcomes();
        }

        if let Err(e) = synced {
            // Not undone. These transactions committed in memory and may or
            // may not be on disk, and a reader may already have seen them;
            // aborting them here, as this once did, took rows back from under
            // that reader on the strength of a guess. A failed log sync is
            // the same event wherever it happens: the write-ahead log refuses
            // further commits until the node is restarted, and the restart's
            // replay decides what survived. So this origin's stream is
            // halted, nothing of the batch is acknowledged, and the position
            // this node reports goes back to where it stood, so the sender
            // is told nothing it cannot rely on. The intents are left for
            // the restart: they ride the same log as the rows.
            replication.halt_apply(
                self.origin,
                format!("a batch of its transactions could not be made durable ({})", e),
            );
            replication.applied().retreat(self.origin, self.before);
            for held in waiting {
                outcomes[held.answer] = refuse(
                    &held.record,
                    ApplyStatus::Failed,
                    format!(
                        "the applied rows could not be made durable ({}); this node must be restarted",
                        e
                    ),
                );
                if let Some(owner) = held.owner {
                    owner.release();
                }
            }
            return;
        }

        // Durable: the slot takes the position the intents brought it to.
        if let Err(e) = replication.applied().settle(self.origin) {
            warn!("Could not settle origin {}'s applied position: {}", self.origin, e);
        }
        let relay = replication.config().relay;
        for held in waiting {
            // Durable: the apply's table locks go now (finding L4).
            if let Some(owner) = held.owner {
                owner.release();
            }
            replication.stamp_tables(&held.record.tables, held.record.commit_hlc, held.record.origin);
            debug!(
                "Applied transaction {}:{} ({} statements)",
                held.record.origin,
                held.record.seq,
                held.record.statements.len()
            );
            if relay {
                relay_onwards(replication, held.record, from_node);
            }
        }
    }
}

/// Logs a record that arrived here for the nodes this one can see and its
/// sender cannot. The senders read it from the log like any other.
fn relay_onwards(replication: &StdArc<Replication>, mut record: TxnRecord, _from_node: NodeId) {
    // Marked, so a reader can tell a transaction that passed through here
    // from one that started here.
    record.flags |= protocol::FLAG_RELAYED;
    if let Some(log) = replication.log() {
        if let Err(e) = log.append(&record) {
            warn!(
                "Could not log relayed transaction {}:{}: {}; the nodes behind this one will \
                 not get it from here",
                record.origin, record.seq, e
            );
            return;
        }
    }
    replication.relay_logged();
}

/// Applies a batch of records, in order, and reports on each.
///
/// Records of one origin are applied under that origin's lock, so the same
/// record arriving on two connections is applied by one and found already
/// held by the other.
pub(crate) async fn apply_batch(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    records: Vec<TxnRecord>,
    // Which peer handed these over. `NO_SENDER` when they did not come off a
    // link — a `RESYNC` pull, say.
    from_node: NodeId,
) -> Vec<ApplyOutcome> {
    let mut outcomes: Vec<ApplyOutcome> = Vec::with_capacity(records.len());
    let mut records = records.into_iter().peekable();

    while let Some(first) = records.next() {
        let origin = first.origin;
        let lock = replication.apply_lock(origin);
        let _turn = lock.lock().await;

        let mut group = Group::new(origin);
        let mut next = Some(first);
        while let Some(record) = next.take() {
            apply_one(database, replication, record, from_node, &mut group, &mut outcomes).await;
            if records.peek().is_some_and(|following| following.origin == origin) {
                next = records.next();
            }
        }
        group.finish(database, replication, from_node, &mut outcomes);
    }

    outcomes
}

/// Applies one record on its own. For callers with one record in hand.
#[cfg(test)]
pub(super) async fn apply_record(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    record: TxnRecord,
    from_node: NodeId,
) -> ApplyOutcome {
    apply_batch(database, replication, vec![record], from_node)
        .await
        .pop()
        .expect("one record in, one outcome out")
}

/// Applies one record into `group`, or explains why it was not applied. Its
/// outcome is pushed onto `outcomes`.
async fn apply_one(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    record: TxnRecord,
    from_node: NodeId,
    group: &mut Group,
    outcomes: &mut Vec<ApplyOutcome>,
) {
    let applied = replication.applied();
    let node_id = replication.node_id();

    // Our own transaction, arriving back from a relay. Nothing to do.
    if record.origin == node_id {
        outcomes.push(refuse(&record, ApplyStatus::Duplicate, ""));
        return;
    }

    if let Some(why) = replication.apply_halted(record.origin) {
        outcomes.push(refuse(
            &record,
            ApplyStatus::Failed,
            format!("not applying until this node is restarted: {}", why),
        ));
        return;
    }

    // Read under the origin's lock: nobody else moves this position.
    if applied.already_applied(record.origin, record.seq) {
        outcomes.push(refuse(&record, ApplyStatus::Duplicate, ""));
        return;
    }

    let high_water = applied.high_water(record.origin);
    if record.seq > high_water + 1 {
        // A record cannot be applied over a hole: the statements before it may
        // be what make it valid. The sender starts again from this node's
        // position on seeing this.
        //
        // From nothing as well. A node that held nothing of an origin used to
        // take up its stream wherever the first record to arrive happened to
        // be, on the grounds that the earlier history might be past retention.
        // What that did in practice was skip a refused transaction: the first
        // record of a stream was refused as a conflict, the second arrived
        // with nothing yet applied, and it was adopted as the start. A node
        // that really is missing an origin's early history is one to reseed,
        // and the sender says so rather than send it the rest.
        outcomes.push(refuse(
            &record,
            ApplyStatus::Gap,
            format!("expected sequence {}, got {}", high_water + 1, record.seq),
        ));
        return;
    }

    replication.clock().observe(record.commit_hlc);

    // Key/value cache effects. Applied in arrival order whatever the conflict
    // policy: the store is a cache, and a command that fails here is counted
    // and skipped rather than stopping the stream.
    if record.is_kv() && !record.is_void() {
        if group.waiting.is_empty() {
            // Nothing waiting on a sync: the position is written as for a
            // record passed over, and then the effects run. Written first, a
            // failure to write it applies nothing and the record comes again.
            if let Err(e) = applied.record(record.origin, record.seq) {
                outcomes.push(refuse(
                    &record,
                    ApplyStatus::Failed,
                    format!("the applied position could not be written: {}", e),
                ));
                return;
            }
            apply_kv(database, replication, &record);
            outcomes.push(ApplyOutcome {
                origin: record.origin,
                seq: record.seq,
                status: ApplyStatus::Applied,
                message: String::new(),
            });
            if replication.config().relay {
                relay_onwards(replication, record, from_node);
            }
            return;
        }
        // Behind row writes still waiting on their sync: it joins them, as a
        // step that holds if theirs do, so the stream's position never runs
        // ahead of rows that might not be there.
        if let Err(e) = applied.begin(record.origin, record.seq, 0) {
            outcomes.push(refuse(
                &record,
                ApplyStatus::Failed,
                format!("the applied position could not be written: {}", e),
            ));
            return;
        }
        apply_kv(database, replication, &record);
        applied.advance(record.origin, record.seq);
        outcomes.push(ApplyOutcome {
            origin: record.origin,
            seq: record.seq,
            status: ApplyStatus::Applied,
            message: String::new(),
        });
        group.waiting.push(Waiting {
            owner: None,
            record,
            answer: outcomes.len() - 1,
        });
        return;
    }

    // A sequence the origin handed to a transaction that did not commit.
    // There is nothing to run: the stream moves past it.
    if record.is_void() || record.statements.is_empty() {
        group.finish(database, replication, from_node, outcomes);
        let outcome = match applied.record(record.origin, record.seq) {
            Ok(_) => ApplyOutcome {
                origin: record.origin,
                seq: record.seq,
                status: ApplyStatus::Applied,
                message: String::new(),
            },
            Err(e) => refuse(
                &record,
                ApplyStatus::Failed,
                format!("the applied position could not be written: {}", e),
            ),
        };
        if outcome.status == ApplyStatus::Applied && replication.config().relay {
            relay_onwards(replication, record, from_node);
        }
        outcomes.push(outcome);
        return;
    }

    let policy = replication.conflict_policy();
    if policy == ConflictPolicy::LastWriterWins && replication.is_superseded(&record) {
        // Every table it writes has a strictly later local write, so applying
        // it would move those rows backwards in time.
        group.finish(database, replication, from_node, outcomes);
        if let Err(e) = applied.record(record.origin, record.seq) {
            warn!("Could not record the applied position: {}", e);
        }
        outcomes.push(refuse(
            &record,
            ApplyStatus::Conflict,
            "superseded by a later local write",
        ));
        return;
    }

    // A statement that is not a plain row write (a schema change) takes locks
    // that the records waiting in the group may hold their side of, and is
    // not undone by an abort. It runs with nothing waiting, and is synced
    // before anything follows it.
    let plain = record
        .statements
        .iter()
        .all(|statement| super::manager::plain_row_write(statement));
    if !plain {
        group.finish(database, replication, from_node, outcomes);
    }

    // A schema change makes itself durable as it goes, over several syncs,
    // and is not undone by an abort. So there is no place to write its
    // position that a power cut cannot separate from it: written after, as
    // for a row write, the table was there and the position was not in 36
    // crash images of 160, the origin offered the `CREATE` again, it was
    // refused as already existing, and the stream stalled until someone
    // stepped in; written before, the position was there and the table was
    // not in 46, which nothing would ever have reported.
    //
    // What can be made durable ahead of it is the fact that it was started.
    // A record that is nothing but schema changes is marked as in flight
    // before it runs. If the node stops before the record's position is
    // settled, the next start finds the mark, and when the origin offers the
    // record again, a statement of it that fails because its work is already
    // there is taken as done.
    let schema_only = record
        .statements
        .iter()
        .all(|statement| !super::manager::plain_row_write(statement));
    let stood = applied.high_water(record.origin);
    let resumed = schema_only && applied.schema_in_flight(record.origin, record.seq);
    if schema_only {
        if let Err(e) = applied.mark_schema(record.origin, record.seq) {
            outcomes.push(refuse(
                &record,
                ApplyStatus::Failed,
                format!("the applied position could not be written: {}", e),
            ));
            return;
        }
    }
    // The mark is taken back whenever the record is not applied.
    let take_back = || {
        if schema_only {
            if let Err(e) = applied.rollback(record.origin, stood) {
                warn!("Could not take the applied position back: {}", e);
            }
        }
    };

    // A statement that failed on a lock (40P01, 55P03, 57014, 57P01: a
    // deadlock's victim, a lock or statement timeout, a cancel) applied
    // nothing wrong; it lost a race with local work. The record is retried
    // whole, in a fresh transaction, after the failed one is aborted and its
    // owner released, so a deadlock it was part of is broken (finding L2,
    // part 2). Never skipped under Skip or LastWriterWins, which would lose
    // the statement on this node; never advanced past when the retries run
    // out, so the sender delivers it again.
    let mut tries = 0usize;
    let (tx, owner) = loop {
        let tx = database.transactions.begin();
        replication.enter_apply(tx, record.origin);
        // The apply's lock owner: a named internal owner, its transaction's
        // root, never `FROZEN_TX` (docs/persistent-indexes.md §7 Owner;
        // finding L2). Released after the last durability step, or on any
        // way out.
        let owner = crate::core::mvcc::owner::OwnerRelease::for_root(&database.row_locks, tx);

        let mut failure: Option<String> = None;
        let mut transient = false;
        for statement in &record.statements {
            // A catalogue change, on the default stream: made in this node's
            // catalogue, not run as a statement (`CatalogOp`). A schema-only
            // record, so it is marked in flight before it runs, and applying
            // it again after a restart finds it done.
            if replication.routed_database().is_none() {
                if let Some(op) = super::manager::CatalogOp::parse(statement) {
                    // A node replicating its one database alone has no
                    // catalogue to make it in: the change is passed over, as
                    // a statement it cannot make, rather than holding up the
                    // whole stream behind it.
                    let applied = match replication.catalog() {
                        Some(link) => link.apply(op).await,
                        None => {
                            warn!(
                                "Not applying {} from node {}: this node replicates its default database only",
                                statement, record.origin
                            );
                            Ok(())
                        }
                    };
                    match applied {
                        Ok(()) => continue,
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    }
                }
            }
            let Some(purpose) = recognize_query_purpose(statement) else {
                failure = Some(format!("could not parse `{}`", truncate(statement)));
                break;
            };

            // The peer's statement temporaries arrive as its `CREATE TABLE
            // __cte_…`, which only the peer's own CTE could have issued.
            let response = crate::core::rql::cte::allowing_temporaries(crate::core::mvcc::owner::under(
                &database.transactions,
                tx,
                crate::core::mvcc::owner::OwnerKind::Replication,
                execute_in_txn(purpose, database.clone(), tx),
            ))
            .await;
            if response.first() == Some(&3) {
                let message = String::from_utf8_lossy(&response[1..]).to_string();
                if lost_a_lock(&message) {
                    failure = Some(format!("{} (`{}`)", message, truncate(statement)));
                    transient = true;
                    break;
                }
                if resumed && already_done(&message) {
                    info!(
                        "Transaction {}:{} was being applied when this node last stopped; `{}` is \
                         already in place ({}), and is taken as done",
                        record.origin,
                        record.seq,
                        truncate(statement),
                        message
                    );
                    continue;
                }
                match policy {
                    ConflictPolicy::Skip | ConflictPolicy::LastWriterWins => {
                        warn!(
                            "Skipping a statement of transaction {}:{}: {} (`{}`)",
                            record.origin,
                            record.seq,
                            message,
                            truncate(statement)
                        );
                    }
                    ConflictPolicy::Abort => {
                        failure = Some(format!("{} (`{}`)", message, truncate(statement)));
                        break;
                    }
                }
            }
        }

        replication.leave_apply(tx);

        let Some(message) = failure else {
            break (tx, owner);
        };
        // What is waiting goes to disk first: the flush below is this
        // transaction's, and must not be what carries the others' outcomes.
        group.finish(database, replication, from_node, outcomes);
        // `Database::abort_transaction`, not the manager's `abort`: the latter
        // ended the transaction but left its row locks in the lock table and
        // its capture buffers behind (finding L2).
        database.abort_transaction(tx);
        if let Err(e) = database.transactions.flush() {
            warn!("Could not make the aborted outcome durable: {}", e);
        }
        owner.release();
        if transient {
            if tries < APPLY_LOCK_RETRIES {
                tries += 1;
                replication.stats().apply_retries.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "Transaction {}:{} lost a lock ({}); retrying it (try {})",
                    record.origin, record.seq, message, tries
                );
                tokio::time::sleep(apply_lock_backoff(tries)).await;
                continue;
            }
            // Not applied and not advanced past: the sender delivers it again.
            warn!(
                "Transaction {}:{} kept losing locks to local work ({}); it will be \
                 delivered again",
                record.origin, record.seq, message
            );
            take_back();
            outcomes.push(refuse(&record, ApplyStatus::Failed, message));
            return;
        }
        // Deliberately *not* advancing the applied position. Under `abort` the
        // stream stops here rather than silently skipping a transaction, which
        // is the difference between a visible stall and quiet divergence.
        error!(
            "Transaction {}:{} could not be applied and the stream from that origin \
             has stalled: {}. Resolve it, then `REPLICATION RESYNC PEER` or change \
             the conflict policy to skip.",
            record.origin, record.seq, message
        );
        take_back();
        outcomes.push(refuse(&record, ApplyStatus::Conflict, message));
        return;
    };

    // The position goes into the log ahead of the commit it depends on, as
    // "this sequence, if this transaction committed": one sync then covers
    // the rows, the position and the outcome, and after a crash the position
    // counts exactly when the rows do (`AppliedState`).
    let wrote = database.transactions.wrote_family(tx);
    let before = applied.high_water(record.origin);
    // The records waiting in the group are synced first: taking this one's
    // step back starts a new run of intents, and theirs must be in the slot
    // by then.
    let give_up = |why: String, group: &mut Group, outcomes: &mut Vec<ApplyOutcome>| {
        database.abort_transaction(tx);
        group.finish(database, replication, from_node, outcomes);
        let before = applied.high_water(record.origin).min(before);
        if let Err(e) = applied.rollback(record.origin, before) {
            warn!("Could not take the applied position back: {}", e);
        }
        if let Err(e) = database.transactions.flush() {
            warn!("Could not make the aborted outcome durable: {}", e);
        }
        // Not recorded as applied, so the record comes again and is applied
        // then, instead of being skipped for good.
        refuse(&record, ApplyStatus::Failed, why)
    };

    // A transaction that wrote no row has no durable outcome to ask about
    // later; its step is written as one that holds if the steps before it do.
    if let Err(e) = applied.begin(record.origin, record.seq, if wrote { tx } else { 0 }) {
        let outcome = give_up(format!("the applied position could not be written: {}", e), group, outcomes);
        owner.release();
        outcomes.push(outcome);
        return;
    }
    // Through the same path as a client's commit: rows applied to a table on
    // another storage location are in that location's log, and they have to
    // be durable before the outcome exists for any root flush to carry to
    // disk.
    if let Err(e) = database.commit_written_transaction(tx) {
        let outcome = give_up(format!("the applied rows could not be made durable: {}", e), group, outcomes);
        owner.release();
        outcomes.push(outcome);
        return;
    }
    if let Err(e) = database.transactions.flush_deferred() {
        let outcome = give_up(format!("the transaction outcome could not be recorded: {}", e), group, outcomes);
        owner.release();
        outcomes.push(outcome);
        return;
    }
    applied.advance(record.origin, record.seq);

    if group.waiting.is_empty() {
        group.before = before;
    }
    outcomes.push(ApplyOutcome {
        origin: record.origin,
        seq: record.seq,
        status: ApplyStatus::Applied,
        message: String::new(),
    });
    group.waiting.push(Waiting {
        owner: Some(owner),
        record,
        answer: outcomes.len() - 1,
    });
    if !plain {
        group.finish(database, replication, from_node, outcomes);
    }
}

/// Runs a key/value record's effects against this database's store.
///
/// Never fails the record: a command that does not apply is counted in
/// `kv_apply_errors` and skipped. Two nodes writing one key at once can leave
/// it different on each, as in any multi-primary cache.
fn apply_kv(database: &Arc<Database>, replication: &StdArc<Replication>, record: &TxnRecord) {
    use base64::Engine as _;
    let stats = replication.stats();
    let mut errors = 0u64;
    let mut first_error: Option<String> = None;
    for chunk in &record.statements {
        let bytes = match base64::engine::general_purpose::STANDARD.decode(chunk) {
            Ok(bytes) => bytes,
            Err(e) => {
                errors += 1;
                first_error.get_or_insert_with(|| format!("a chunk is not base64: {}", e));
                continue;
            }
        };
        #[cfg(feature = "kv_cache")]
        {
            let report = crate::core::kv_cache::repl::apply_chunk(&database.kv_cache, &bytes);
            stats.kv_applied.fetch_add(report.applied, Ordering::Relaxed);
            errors += report.errors;
            if first_error.is_none() {
                first_error = report.first_error;
            }
        }
        #[cfg(not(feature = "kv_cache"))]
        {
            let _ = (database, bytes);
            errors += 1;
            first_error.get_or_insert_with(|| "this build has no key/value cache".to_string());
        }
    }
    if errors > 0 {
        stats.kv_apply_errors.fetch_add(errors, Ordering::Relaxed);
        warn_kv_apply(record, errors, first_error.as_deref().unwrap_or(""));
    }
    debug!(
        "Applied key/value record {}:{} ({} chunk(s), {} error(s))",
        record.origin,
        record.seq,
        record.statements.len(),
        errors
    );
}

/// Says that key/value effects failed to apply, at most once every ten
/// seconds: under concurrent writes to the same keys on two nodes this can be
/// every record, and the counter in `SHOW REPLICATION STATS` has the total.
fn warn_kv_apply(record: &TxnRecord, errors: u64, first: &str) {
    static LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST.load(Ordering::Relaxed);
    if now < last + 10 || LAST.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        return;
    }
    warn!(
        "{} key/value command(s) of record {}:{} did not apply here and were skipped (first: {}); concurrent writes to one key on two nodes can leave it different on each",
        errors, record.origin, record.seq, first
    );
}

/// How many times a record that lost a lock is retried before it is handed
/// back undelivered (finding L2).
const APPLY_LOCK_RETRIES: usize = 8;

/// The pause before retry `n` of a record that lost a lock: doubling from
/// 10 ms, capped at 500 ms (about 1.6 s over every retry).
fn apply_lock_backoff(n: usize) -> std::time::Duration {
    std::time::Duration::from_millis((10u64 << n.min(6)).min(500))
}

/// Whether a statement's failure means it lost a lock to local work rather
/// than that it cannot apply: 40P01 (a deadlock's victim), 55P03 (lock
/// timeout, NOWAIT), 57014 (a statement timeout or cancel), 57P01 (shutdown).
/// Classified by the wire's own mapping, so the two never disagree.
fn lost_a_lock(message: &str) -> bool {
    matches!(
        crate::pgwire::sqlstate_for(message),
        "40P01" | "55P03" | "57014" | "57P01"
    )
}

/// Whether a schema change failed because what it makes is already there, or
/// what it removes is already gone: the answer a statement gets when it ran
/// before. Classified by the wire's own mapping, as [`lost_a_lock`] is.
fn already_done(message: &str) -> bool {
    matches!(
        crate::pgwire::sqlstate_for(message),
        // duplicate table, column, object, schema; undefined table, column,
        // object.
        "42P07" | "42701" | "42710" | "42P06" | "42P01" | "42703" | "42704"
    )
}

/// Keeps a statement short enough for a log line.
fn truncate(statement: &str) -> String {
    const LIMIT: usize = 120;
    if statement.len() <= LIMIT {
        return statement.to_string();
    }
    // On a character boundary, so a multi-byte literal cannot be split.
    let mut end = LIMIT;
    while end > 0 && !statement.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &statement[..end])
}

/// Applies records pulled from a peer, for `REPLICATION RESYNC`.
pub async fn apply_pulled(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    records: Vec<TxnRecord>,
) -> (usize, usize) {
    let mut applied = 0;
    let mut refused = 0;
    for outcome in apply_batch(database, replication, records, NO_SENDER).await {
        if outcome.status == ApplyStatus::Applied {
            applied += 1;
        } else if !outcome.status.is_settled() {
            refused += 1;
        }
    }
    replication
        .stats()
        .applied
        .fetch_add(applied as u64, Ordering::Relaxed);
    (applied, refused)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::config::ReplicationConfig;

    async fn database() -> Arc<Database> {
        let database = crate::replication::test_support::in_memory_database().await;
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.replication.open_storage(None).unwrap();
        database.replication.set_active(true);
        database
    }

    fn record(origin: NodeId, seq: u64, statements: Vec<&str>) -> TxnRecord {
        TxnRecord {
            origin,
            seq,
            commit_hlc: seq * 1000,
            flags: 0,
            statements: statements.into_iter().map(str::to_string).collect(),
            tables: vec!["repl_t".to_string()],
        }
    }

    async fn create_table(database: &Arc<Database>) {
        let purpose =
            recognize_query_purpose("CREATE TABLE repl_t (id INTEGER, name VARCHAR)").unwrap();
        let response = crate::core::rql::executor::execute(purpose, database.clone()).await;
        assert_ne!(response.first(), Some(&3), "the fixture table must exist");
    }

    async fn row_count(database: &Arc<Database>) -> usize {
        let purpose = recognize_query_purpose("SELECT id FROM repl_t WHERE 1 = 1").unwrap();
        let response = crate::core::rql::executor::execute(purpose, database.clone()).await;
        assert_eq!(response.first(), Some(&2), "a SELECT should return rows");
        crate::core::row::row::vec_into_rows(&response[1..])
            .map(|rows| rows.len())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn a_pushed_transaction_lands_in_the_table() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        assert_eq!(row_count(&database).await, 1);
        assert_eq!(replication.applied().high_water(2), 1);
    }

    /// A peer's write fed by a CTE carries its statement temporaries: their
    /// `CREATE TABLE __cte_…` was captured with the transaction. The prefix is
    /// refused to every user create (`cte::reserved_name_refusal`), so the
    /// apply loop opens it: refused here, the record failed and the write
    /// never landed on this node.
    ///
    /// Mutation: the apply loop without `cte::allowing_temporaries`.
    #[tokio::test]
    async fn a_peers_statement_temporaries_apply() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        let temp = format!("{}k_0_ab12", crate::core::rql::cte::TEMP_TABLE_PREFIX);
        let create = format!("CREATE TABLE {temp} (id INTEGER)");
        let fill = format!("INSERT INTO {temp} (id) VALUES (1)");
        let drop = format!("DROP TABLE {temp}");
        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec![&create, &fill, "INSERT INTO repl_t (id, name) VALUES (1, 'ada')", &drop],
            ),
            NO_SENDER,
        )
        .await;
        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        assert_eq!(row_count(&database).await, 1);
        assert!(!database.has_table(&temp), "the temporary outlived the applied transaction");
    }

    /// Replication apply runs under a named owner, its transaction's root,
    /// never unowned (§7 Owner; finding L2). A failed apply ends through
    /// `Database::abort_transaction`, so its row and referenced-key locks
    /// go, and its owner is released.
    #[tokio::test]
    async fn an_apply_runs_under_its_named_owner_and_releases_it_on_every_end() {
        use crate::core::mvcc::owner::{probe, LockOwner, OwnerKind};
        let database = database().await;
        create_table(&database).await;
        for sql in [
            "CREATE TABLE repl_parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE repl_child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES repl_parent(id))",
            "INSERT INTO repl_parent (id) VALUES (1)",
        ] {
            let response = crate::core::rql::executor::execute(recognize_query_purpose(sql).unwrap(), database.clone()).await;
            assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
        }
        let replication = database.replication.clone();

        let check = |what: &str, records: &[probe::Record]| -> crate::core::mvcc::TxId {
            assert!(!records.is_empty(), "{what}: the apply recorded nothing");
            let tx = records[0].tx;
            for r in records {
                let o = r.owner.unwrap_or_else(|| panic!("{what}: {:?} ran under no owner", r.site));
                assert_eq!(o.kind, OwnerKind::Replication, "{what}: {:?}", r.site);
                assert_eq!(o.owner, LockOwner::block(tx).unwrap(), "{what}: not the apply's own transaction");
                assert!(r.owner_live);
            }
            tx
        };

        let (outcome, records) = probe::capture(apply_record(
            &database,
            &replication,
            record(2, 1, vec!["INSERT INTO repl_child (id, pid) VALUES (10, 1)"]),
            NO_SENDER,
        ))
        .await;
        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        let tx = check("applied", &records);
        assert!(!database.row_locks.txn_holds_rows(tx), "an applied transaction kept its key locks");
        assert!(!database.row_locks.owner_holds_tables(tx));

        // Control: the first statement below takes a row lock its transaction
        // holds to its end, or the check after the failed apply proves nothing.
        let control = database.transactions.begin();
        let response = crate::core::rql::executor::execute_in_txn(
            recognize_query_purpose("SELECT id FROM repl_parent WHERE id = 1 FOR UPDATE").unwrap(),
            database.clone(),
            control,
        )
        .await;
        assert_ne!(response.first(), Some(&3), "{}", String::from_utf8_lossy(&response));
        assert!(database.row_locks.txn_holds_rows(control), "the control statement took no lock");
        database.abort_transaction(control);

        // Under `abort` (the default), a statement that fails ends the apply.
        // The statement before it took a row lock.
        assert_eq!(replication.conflict_policy(), ConflictPolicy::Abort);
        let (outcome, records) = probe::capture(apply_record(
            &database,
            &replication,
            record(
                2,
                2,
                vec![
                    "SELECT id FROM repl_parent WHERE id = 1 FOR UPDATE",
                    "INSERT INTO no_such_table (id) VALUES (1)",
                ],
            ),
            NO_SENDER,
        ))
        .await;
        assert_ne!(outcome.status, ApplyStatus::Applied);
        let tx = check("failed", &records);
        assert_eq!(database.transactions.outcome(tx), crate::core::mvcc::TxOutcome::Aborted);
        assert!(
            !database.row_locks.txn_holds_rows(tx),
            "a failed apply left its row locks in the lock table"
        );
        assert!(!database.row_locks.owner_holds_tables(tx));
    }

    #[tokio::test]
    async fn every_statement_of_a_transaction_is_applied() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec![
                    "INSERT INTO repl_t (id, name) VALUES (1, 'ada')",
                    "INSERT INTO repl_t (id, name) VALUES (2, 'grace')",
                    "INSERT INTO repl_t (id, name) VALUES (3, 'edsger')",
                ],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        assert_eq!(row_count(&database).await, 3);
    }

    /// Replication apply passes the write gate (docs/persistent-indexes.md
    /// §2, slice B11): from a forced clean point, the census sees a durable
    /// `DIRTY` and a ticket on the table's gate before every mutation and
    /// write the apply makes, and the apply's owner gives its ticket back once
    /// the record's outcome is final.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replication_apply_passes_the_write_gate() {
        use crate::core::index_store::census::{Census, Scope, check};
        crate::replication::test_support::init_globals();
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "repl_gate").await);
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.replication.open_storage(None).unwrap();
        database.replication.set_active(true);
        create_table(&database).await;
        let replication = database.replication.clone();
        let table = database.table_for_tests("repl_t").unwrap();
        table.wait_filters_ready().await.unwrap();
        table.gate().force_clean_for_tests();

        let census = Census::start();
        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec![
                    "INSERT INTO repl_t (id, name) VALUES (1, 'ada')",
                    "INSERT INTO repl_t (id, name) VALUES (2, 'bob')",
                    "UPDATE repl_t SET name = 'carol' WHERE id = 2",
                ],
            ),
            NO_SENDER,
        )
        .await;
        let events = census.finish();
        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        let summary = check(
            &events,
            &Scope {
                gate: table.gate().census_id(),
                began_clean: true,
                structures: &table.census_structures(),
                files: &table.census_files(),
            },
        )
        .unwrap_or_else(|e| panic!("census: {e}"));
        assert_eq!(summary.dirty_durable, 1);
        assert!(summary.writes > 0 && summary.ticket_mutations > 0, "{summary:?}");
        assert_eq!(table.gate().word().load().count(), 0, "the apply's owner kept its ticket");
        assert_eq!(row_count(&database).await, 2);
        drop(table);
        drop(database);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }

    /// The property the whole design rests on: applying a record must not make
    /// this node re-originate it.
    #[tokio::test]
    async fn applying_a_record_does_not_capture_it_for_republication() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        // The fixture's own `CREATE TABLE` is a local write and is captured, so
        // the property under test is that the *apply* adds nothing to that.
        let before = replication.local_seq();

        apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(
            replication.local_seq(),
            before,
            "an applied record must not become a local one, or a mesh would loop"
        );
        assert_eq!(
            replication.open_captures(),
            0,
            "no buffer should be left behind"
        );
    }

    #[tokio::test]
    async fn the_same_record_twice_is_applied_once() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let first = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;
        let second = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(first.status, ApplyStatus::Applied);
        assert_eq!(second.status, ApplyStatus::Duplicate);
        assert_eq!(
            row_count(&database).await,
            1,
            "a resend must not double-insert"
        );
    }

    /// A key/value record carrying `commands`, as `kv_flush` would make one.
    #[cfg(feature = "kv_cache")]
    fn kv_record(origin: NodeId, seq: u64, commands: &[&[&[u8]]]) -> TxnRecord {
        use base64::Engine as _;
        let mut chunk = vec![protocol::KV_FORMAT_V1];
        for command in commands {
            let effect: Vec<Vec<u8>> = command.iter().map(|a| a.to_vec()).collect();
            crate::core::kv_cache::repl::encode_entry(&mut chunk, 0, &[effect]);
        }
        TxnRecord {
            origin,
            seq,
            commit_hlc: seq * 1000,
            flags: protocol::FLAG_KV,
            statements: vec![base64::engine::general_purpose::STANDARD.encode(&chunk)],
            tables: Vec::new(),
        }
    }

    #[cfg(feature = "kv_cache")]
    fn kv_get(database: &Arc<Database>, key: &[u8]) -> crate::core::kv_cache::reply::Item {
        use crate::core::kv_cache::{commands, reply::Collect, store::Direct};
        let mut out = Collect::default();
        let args: [&[u8]; 2] = [b"GET", key];
        commands::run(commands::lookup(b"GET").unwrap(), &mut Direct::new(database.kv_cache.db(0)), &args, &mut out);
        out.take()
    }

    /// At-least-once delivery: a key/value record delivered again is taken
    /// as held, through the same positions table data uses, so an `INCR` in
    /// it does not count twice.
    #[cfg(feature = "kv_cache")]
    #[tokio::test]
    async fn a_kv_record_delivered_twice_is_applied_once() {
        use crate::core::kv_cache::reply::Item;
        let _settings = crate::core::kv_cache::tests::default_settings();
        let database = database().await;
        let replication = database.replication.clone();
        let incr = kv_record(2, 1, &[&[b"INCRBY", b"n", b"5"]]);

        let first = apply_record(&database, &replication, incr.clone(), NO_SENDER).await;
        let second = apply_record(&database, &replication, incr, NO_SENDER).await;
        assert_eq!(first.status, ApplyStatus::Applied);
        assert_eq!(second.status, ApplyStatus::Duplicate);
        assert_eq!(kv_get(&database, b"n"), Item::Bulk(b"5".to_vec()));

        // And one that skips a sequence is a gap, as for table data.
        let skipped = apply_record(&database, &replication, kv_record(2, 3, &[&[b"SET", b"x", b"1"]]), NO_SENDER).await;
        assert_eq!(skipped.status, ApplyStatus::Gap);
        assert_eq!(kv_get(&database, b"x"), Item::Null);
        assert_eq!(replication.local_seq(), 0, "an applied record is not captured again");
    }

    /// A key/value command that fails on apply (here a `WRONGTYPE`, as
    /// concurrent writes on two nodes can cause) is counted and skipped; the
    /// record still applies, under the strictest conflict policy, and the
    /// stream goes on.
    #[cfg(feature = "kv_cache")]
    #[tokio::test]
    async fn a_kv_command_that_fails_on_apply_is_skipped_not_fatal() {
        use crate::core::kv_cache::reply::Item;
        let _settings = crate::core::kv_cache::tests::default_settings();
        let database = database().await;
        let replication = database.replication.clone();
        assert_eq!(replication.conflict_policy(), ConflictPolicy::Abort);

        let record = kv_record(2, 1, &[&[b"SET", b"s", b"text"], &[b"LPUSH", b"s", b"x"], &[b"SET", b"t", b"1"]]);
        let outcome = apply_record(&database, &replication, record, NO_SENDER).await;
        assert_eq!(outcome.status, ApplyStatus::Applied);
        assert_eq!(kv_get(&database, b"t"), Item::Bulk(b"1".to_vec()));
        assert_eq!(replication.stats().kv_apply_errors.load(Ordering::Relaxed), 1);
        assert_eq!(replication.stats().kv_applied.load(Ordering::Relaxed), 2);

        let next = apply_record(&database, &replication, kv_record(2, 2, &[&[b"SET", b"u", b"2"]]), NO_SENDER).await;
        assert_eq!(next.status, ApplyStatus::Applied);
        assert_eq!(replication.applied().high_water(2), 2);
    }

    /// Key/value records interleaved with row writes in one batch: each
    /// applies, in order, and the position ends past all of them.
    #[cfg(feature = "kv_cache")]
    #[tokio::test]
    async fn kv_records_between_row_writes_apply_in_order() {
        use crate::core::kv_cache::reply::Item;
        let _settings = crate::core::kv_cache::tests::default_settings();
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let batch = vec![
            record(2, 1, vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"]),
            kv_record(2, 2, &[&[b"SET", b"k", b"1"]]),
            record(2, 3, vec!["INSERT INTO repl_t (id, name) VALUES (2, 'bob')"]),
            kv_record(2, 4, &[&[b"APPEND", b"k", b"2"]]),
        ];
        let outcomes = apply_batch(&database, &replication, batch, NO_SENDER).await;
        assert!(outcomes.iter().all(|o| o.status == ApplyStatus::Applied), "{outcomes:?}");
        assert_eq!(row_count(&database).await, 2);
        assert_eq!(kv_get(&database, b"k"), Item::Bulk(b"12".to_vec()));
        assert_eq!(replication.applied().high_water(2), 4);
    }

    #[tokio::test]
    async fn a_record_that_skips_a_sequence_is_refused_as_a_gap() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                5,
                vec!["INSERT INTO repl_t (id, name) VALUES (5, 'five')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Gap);
        assert_eq!(
            row_count(&database).await,
            1,
            "nothing may be applied over a hole"
        );
    }

    /// A node that holds nothing of an origin takes its stream from the
    /// first transaction or not at all. Taking it up wherever a record
    /// happened to arrive skipped whatever came before, and what came before
    /// was, in practice, a transaction this node had just refused.
    #[tokio::test]
    async fn a_stream_is_not_taken_up_part_way() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        // The first is refused: the table it writes is not there.
        let refused = apply_record(
            &database,
            &replication,
            record(2, 1, vec!["INSERT INTO no_such_table (id) VALUES (1)"]),
            NO_SENDER,
        )
        .await;
        assert_eq!(refused.status, ApplyStatus::Conflict);

        // The second arrives with nothing applied yet.
        let outcome = apply_record(
            &database,
            &replication,
            record(2, 2, vec!["INSERT INTO repl_t (id, name) VALUES (9, 'nine')"]),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Gap, "the refused transaction was passed over");
        assert_eq!(replication.applied().high_water(2), 0);
        assert_eq!(row_count(&database).await, 0);
    }

    /// A sync that fails under an applied batch: the batch is not
    /// acknowledged, the position this node reports does not move, nothing
    /// is taken back from under a reader, and the origin's stream is not
    /// applied again until the node is restarted.
    #[tokio::test]
    async fn a_failed_sync_halts_the_origin_and_acknowledges_nothing() {
        crate::replication::test_support::init_globals();
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "repl_halt").await);
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.open_replication_storage().unwrap();
        database.replication.set_active(true);
        create_table(&database).await;
        let replication = database.replication.clone();

        let first = apply_record(
            &database,
            &replication,
            record(2, 1, vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"]),
            NO_SENDER,
        )
        .await;
        assert_eq!(first.status, ApplyStatus::Applied, "{}", first.message);

        crate::core::wal::test_hooks::FAIL_LOG_SYNC.with(|f| f.set(Some(5)));
        let outcomes = apply_batch(
            &database,
            &replication,
            vec![
                record(2, 2, vec!["INSERT INTO repl_t (id, name) VALUES (2, 'grace')"]),
                record(2, 3, vec!["INSERT INTO repl_t (id, name) VALUES (3, 'edsger')"]),
            ],
            NO_SENDER,
        )
        .await;
        crate::core::wal::test_hooks::FAIL_LOG_SYNC.with(|f| f.set(None));

        assert!(outcomes.iter().all(|o| o.status == ApplyStatus::Failed), "{outcomes:?}");
        assert!(outcomes[0].message.contains("must be restarted"), "{}", outcomes[0].message);
        assert_eq!(replication.applied().high_water(2), 1, "nothing of the batch is claimed");
        assert!(replication.apply_halted(2).is_some());
        // Still there for whoever read them: not undone on a guess.
        assert_eq!(row_count(&database).await, 3);

        // Offered again, as a sender would: refused, and not applied twice.
        let again = apply_record(
            &database,
            &replication,
            record(2, 2, vec!["INSERT INTO repl_t (id, name) VALUES (2, 'grace')"]),
            NO_SENDER,
        )
        .await;
        assert_eq!(again.status, ApplyStatus::Failed);
        assert!(again.message.contains("restarted"), "{}", again.message);
        assert_eq!(row_count(&database).await, 3);

        // The restart decides. Here the log's page cache kept everything, so
        // all three survive, and the position comes back agreeing with them.
        drop(replication);
        drop(database);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let database = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "repl_halt").await);
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.open_replication_storage().unwrap();
        let position = database.replication.applied().high_water(2);
        assert_eq!(row_count(&database).await as u64, position, "rows and position agree after the restart");
        assert!(database.replication.apply_halted(2).is_none());
        drop(database);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }

    /// The log synced and only the copy of the outcomes into the status
    /// file failed: the batch is durable, so it is acknowledged, the
    /// position moves and the stream goes on. Nothing here is a reason to
    /// halt, because the log already covers what the copy would have put
    /// in the file.
    #[tokio::test]
    async fn a_failed_status_copy_after_a_durable_log_does_not_halt() {
        crate::replication::test_support::init_globals();
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "repl_copy").await);
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.open_replication_storage().unwrap();
        database.replication.set_active(true);
        create_table(&database).await;
        let replication = database.replication.clone();

        crate::core::database::test_hooks::FAIL_STAGED_COPY.with(|f| f.set(true));
        let outcomes = apply_batch(
            &database,
            &replication,
            vec![
                record(2, 1, vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"]),
                record(2, 2, vec!["INSERT INTO repl_t (id, name) VALUES (2, 'grace')"]),
            ],
            NO_SENDER,
        )
        .await;
        assert!(
            !crate::core::database::test_hooks::FAIL_STAGED_COPY.with(|f| f.get()),
            "the injected failure was never reached"
        );

        assert!(outcomes.iter().all(|o| o.status == ApplyStatus::Applied), "{outcomes:?}");
        assert_eq!(replication.applied().high_water(2), 2);
        assert!(replication.apply_halted(2).is_none());

        // The stream goes on.
        let next = apply_record(
            &database,
            &replication,
            record(2, 3, vec!["INSERT INTO repl_t (id, name) VALUES (3, 'edsger')"]),
            NO_SENDER,
        )
        .await;
        assert_eq!(next.status, ApplyStatus::Applied, "{}", next.message);

        // And a restart, which reads the outcomes back from the log, finds
        // every row and a position that agrees with them.
        drop(replication);
        drop(database);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let database = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "repl_copy").await);
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                ..Default::default()
            })
            .unwrap();
        database.open_replication_storage().unwrap();
        assert_eq!(database.replication.applied().high_water(2), 3);
        assert_eq!(row_count(&database).await, 3);
        drop(database);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }

    #[tokio::test]
    async fn our_own_transaction_coming_back_is_ignored() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                1,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Duplicate);
        assert_eq!(row_count(&database).await, 0);
    }

    #[tokio::test]
    async fn under_abort_a_failing_statement_leaves_nothing_behind() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        replication
            .update_config(|config| {
                config.conflict = ConflictPolicy::Abort;
                Ok(())
            })
            .unwrap();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec![
                    "INSERT INTO repl_t (id, name) VALUES (1, 'ada')",
                    "INSERT INTO no_such_table (id) VALUES (1)",
                ],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Conflict);
        assert_eq!(
            row_count(&database).await,
            0,
            "an aborted apply must be all-or-nothing"
        );
        assert_eq!(
            replication.applied().high_water(2),
            0,
            "the stream must stall rather than skip the transaction"
        );
    }

    /// A local block holding `LOCK TABLE repl_t IN ACCESS EXCLUSIVE MODE`,
    /// and the id that ends it.
    async fn lock_holder(database: &Arc<Database>) -> crate::core::mvcc::TxId {
        let holder = database.transactions.begin();
        let response = execute_in_txn(
            recognize_query_purpose("LOCK TABLE repl_t IN ACCESS EXCLUSIVE MODE").unwrap(),
            database.clone(),
            holder,
        )
        .await;
        assert_ne!(response.first(), Some(&3), "{}", String::from_utf8_lossy(&response));
        holder
    }

    fn end_holder(database: &Arc<Database>, holder: crate::core::mvcc::TxId) {
        database.commit_transaction(holder);
        database.release_owner(crate::core::mvcc::owner::LockOwner::block(holder).unwrap());
    }

    fn skip_policy(replication: &StdArc<Replication>) {
        replication
            .update_config(|config| {
                config.conflict = ConflictPolicy::Skip;
                Ok(())
            })
            .unwrap();
    }

    /// Finding L2, part 2: an apply whose statement loses a lock to local
    /// work (here 55P03: `lock_timeout` behind a local `LOCK TABLE`) is
    /// aborted, released and retried whole, never skipped, even under
    /// `Skip`; once the local block ends, the retry applies it. The row is
    /// there, the position advanced, and the retries were counted.
    ///
    /// Mutation: `lost_a_lock` answers false: under `Skip` the statement is
    /// skipped, the record reported applied, and the row is missing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_apply_that_loses_a_lock_is_retried_and_lands() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        skip_policy(&replication);
        let holder = lock_holder(&database).await;
        let retries_before = replication.stats().apply_retries.load(Ordering::Relaxed);
        // Ends the holder once the apply has retried, never by the clock.
        let ender = {
            let (database, replication) = (database.clone(), replication.clone());
            tokio::spawn(async move {
                tokio::time::timeout(std::time::Duration::from_secs(20), async {
                    while replication.stats().apply_retries.load(Ordering::Relaxed) <= retries_before {
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    }
                })
                .await
                .expect("the apply never retried");
                end_holder(&database, holder);
            })
        };
        let outcome = crate::core::cancel::with_lock_timeout(
            100,
            apply_record(
                &database,
                &replication,
                record(2, 1, vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"]),
                NO_SENDER,
            ),
        )
        .await;
        ender.await.unwrap();
        assert_eq!(outcome.status, ApplyStatus::Applied, "{}", outcome.message);
        assert_eq!(row_count(&database).await, 1, "the statement was skipped, not retried");
        assert_eq!(replication.applied().high_water(2), 1);
        assert!(replication.stats().apply_retries.load(Ordering::Relaxed) > retries_before);
    }

    /// When the local work outlasts every retry, the record is handed back
    /// undelivered (`Failed`, so the sender sends it again): not applied,
    /// not skipped, the position not advanced, nothing held.
    ///
    /// Mutation: `lost_a_lock` answers false: the record is reported applied
    /// under `Skip` and the position moves past a statement this node lacks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_apply_that_keeps_losing_its_lock_is_not_advanced_past() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        skip_policy(&replication);
        let holder = lock_holder(&database).await;
        let outcome = crate::core::cancel::with_lock_timeout(
            20,
            apply_record(
                &database,
                &replication,
                record(2, 1, vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"]),
                NO_SENDER,
            ),
        )
        .await;
        assert_eq!(outcome.status, ApplyStatus::Failed, "{}", outcome.message);
        assert!(!outcome.status.is_settled());
        assert_eq!(replication.applied().high_water(2), 0, "advanced past a statement it lacks");
        end_holder(&database, holder);
        assert_eq!(row_count(&database).await, 0);
    }

    /// The four lock failures are retried; a conflict or a constraint is the
    /// policy's to decide.
    #[test]
    fn lock_failures_are_told_apart_from_conflicts() {
        for message in [
            "deadlock detected while waiting for AccessExclusiveLock on relation \"t\"",
            "canceling statement due to lock timeout (waiting for transaction 7)",
            "could not obtain lock on relation \"t\"",
            "canceling statement due to statement timeout",
            "canceling statement due to user request",
        ] {
            assert!(lost_a_lock(message), "{message}");
        }
        for message in [
            "could not serialize access due to concurrent update of row 1",
            "duplicate value for UNIQUE column \"id\" (row 1 already holds it)",
            "relation does not exist",
        ] {
            assert!(!lost_a_lock(message), "{message}");
        }
    }

    #[tokio::test]
    async fn under_skip_the_rest_of_the_transaction_still_applies() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        replication
            .update_config(|config| {
                config.conflict = ConflictPolicy::Skip;
                Ok(())
            })
            .unwrap();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec![
                    "INSERT INTO no_such_table (id) VALUES (1)",
                    "INSERT INTO repl_t (id, name) VALUES (1, 'ada')",
                ],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Applied);
        assert_eq!(
            row_count(&database).await,
            1,
            "the good statement should survive"
        );
        assert_eq!(
            replication.applied().high_water(2),
            1,
            "the stream moves on"
        );
    }

    #[tokio::test]
    async fn last_writer_wins_drops_a_record_a_later_local_write_supersedes() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();
        replication
            .update_config(|config| {
                config.conflict = ConflictPolicy::LastWriterWins;
                Ok(())
            })
            .unwrap();

        // A local write to the same table, later than the record about to
        // arrive.
        replication.stamp_tables(&["repl_t".to_string()], 10_000_000, 1);

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                1,
                vec!["INSERT INTO repl_t (id, name) VALUES (1, 'ada')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Conflict);
        assert_eq!(row_count(&database).await, 0);
        assert_eq!(
            replication.applied().high_water(2),
            1,
            "a superseded record still advances the stream"
        );
    }

    #[tokio::test]
    async fn a_hello_from_another_cluster_is_refused() {
        let database = database().await;
        let replication = database.replication.clone();

        let hello = Hello {
            node_id: 2,
            cluster_id: 0xdead_beef,
            codecs: Codec::local_mask(),
            clock: 1,
            token: String::new(),
            advertised_addr: "10.0.0.9:61171".to_string(),
            database: None,
        };
        let mut payload = Vec::new();
        hello.encode(&mut payload);

        let response = handle_hello(&replication, &payload);
        let header = protocol::parse_header(&response).unwrap();
        assert_eq!(header.kind, FrameKind::Error);
    }

    #[tokio::test]
    async fn a_hello_reusing_this_nodes_id_is_refused() {
        let database = database().await;
        let replication = database.replication.clone();
        let config = replication.config();

        let hello = Hello {
            node_id: config.node_id,
            cluster_id: config.cluster_id(),
            codecs: Codec::local_mask(),
            clock: 1,
            token: String::new(),
            advertised_addr: "10.0.0.9:61171".to_string(),
            database: None,
        };
        let mut payload = Vec::new();
        hello.encode(&mut payload);

        let response = handle_hello(&replication, &payload);
        assert_eq!(
            protocol::parse_header(&response).unwrap().kind,
            FrameKind::Error
        );
    }

    #[tokio::test]
    async fn a_hello_with_the_wrong_token_is_refused() {
        let database = database().await;
        let replication = database.replication.clone();
        replication
            .update_config(|config| {
                config.token = "correct-horse".to_string();
                Ok(())
            })
            .unwrap();
        let config = replication.config();

        let mut payload = Vec::new();
        Hello {
            node_id: 2,
            cluster_id: config.cluster_id(),
            codecs: Codec::local_mask(),
            clock: 1,
            token: "battery-staple".to_string(),
            advertised_addr: "10.0.0.9:61171".to_string(),
            database: None,
        }
        .encode(&mut payload);

        assert_eq!(
            protocol::parse_header(&handle_hello(&replication, &payload))
                .unwrap()
                .kind,
            FrameKind::Error
        );

        // ...and the right one is accepted.
        let mut payload = Vec::new();
        Hello {
            node_id: 2,
            cluster_id: config.cluster_id(),
            codecs: Codec::local_mask(),
            clock: 1,
            token: "correct-horse".to_string(),
            advertised_addr: "10.0.0.9:61171".to_string(),
            database: None,
        }
        .encode(&mut payload);

        assert_eq!(
            protocol::parse_header(&handle_hello(&replication, &payload))
                .unwrap()
                .kind,
            FrameKind::HelloAck
        );
    }

    #[tokio::test]
    async fn a_hello_ack_reports_what_has_been_applied() {
        let database = database().await;
        let replication = database.replication.clone();
        replication.applied().record(7, 42).unwrap();
        let config = replication.config();

        let mut payload = Vec::new();
        Hello {
            node_id: 2,
            cluster_id: config.cluster_id(),
            codecs: Codec::local_mask(),
            clock: 1,
            token: String::new(),
            advertised_addr: "10.0.0.9:61171".to_string(),
            database: None,
        }
        .encode(&mut payload);

        let response = handle_hello(&replication, &payload);
        let header = protocol::parse_header(&response).unwrap();
        assert_eq!(header.kind, FrameKind::HelloAck);

        let ack = HelloAck::decode(&mut Reader::new(&response[protocol::HEADER_LEN..])).unwrap();
        assert!(ack.applied.iter().any(|e| e.origin == 7 && e.seq == 42));
    }

    #[tokio::test]
    async fn a_garbled_frame_is_answered_with_an_error_not_a_panic() {
        let database = database().await;
        let greeted = StdArc::new(rastcp::session::Principal::new(session_name(2, None), false));
        let router = StdArc::new(Router::new(database, None, None));
        let response = handle_frame(router, greeted, vec![0xde, 0xad, 0xbe, 0xef]).await;
        assert_eq!(
            protocol::parse_header(&response).unwrap().kind,
            FrameKind::Error
        );
    }

    #[tokio::test]
    async fn a_frame_on_a_connection_that_was_not_greeted_is_refused() {
        // The listener greets every connection, so this is not reachable
        // through it. It is refused all the same: a handler that served
        // whoever it was handed would be one configuration change away from
        // serving everyone.
        let database = database().await;
        let nobody = StdArc::new(rastcp::session::Principal::anonymous());

        let ping = protocol::plain_frame(FrameKind::Ping, 0, &[]);
        let response = handle_frame(StdArc::new(Router::new(database, None, None)), nobody, ping).await;

        assert_eq!(
            protocol::parse_header(&response).unwrap().kind,
            FrameKind::Error
        );
    }

    #[test]
    fn a_session_name_gives_back_the_node_it_was_made_from() {
        for node in [1u64, 2, 61171, u64::MAX] {
            let principal = rastcp::session::Principal::new(session_name(node, None), false);
            assert_eq!(greeted_node(&principal), Some((node, None)));
            let principal = rastcp::session::Principal::new(session_name(node, Some("sales")), false);
            assert_eq!(greeted_node(&principal), Some((node, Some("sales".to_string()))));
        }
        assert_eq!(greeted_node(&rastcp::session::Principal::new("node:two@sales", false)), None);
        // Anything else is nobody, including the transport's own anonymous
        // caller and a name that only looks like one of these.
        assert_eq!(greeted_node(&rastcp::session::Principal::anonymous()), None);
        assert_eq!(greeted_node(&rastcp::session::Principal::new("node:", false)), None);
        assert_eq!(greeted_node(&rastcp::session::Principal::new("node:two", false)), None);
        assert_eq!(greeted_node(&rastcp::session::Principal::new("alice", true)), None);
    }

    #[test]
    fn truncation_does_not_split_a_multibyte_character() {
        let statement = format!("INSERT INTO t VALUES ('{}')", "é".repeat(200));
        let short = truncate(&statement);
        assert!(short.len() <= 124, "{}", short.len());
        // The assertion that matters: it is still a valid `String`, which it
        // could not be if the slice had landed mid-character.
        assert!(short.ends_with('…'));
    }
}
