//! The node's replication state, and the two hooks the engine calls into.
//!
//! Everything a running node needs is here: what it is configured to do, which
//! peers it has, how far each of them has got, and the per-transaction buffer
//! that turns executed statements into a record worth shipping.
//!
//! ## The two hooks
//!
//! The engine touches replication in exactly two places, both on the write
//! path:
//!
//! - [`Replication::note_statement`] — a write statement succeeded. Appends it
//!   to the buffer for its transaction.
//! - [`Replication::commit_record`] / [`Replication::publish`] — the
//!   transaction committed. Turns the buffer into a record, stamps it, logs it,
//!   and hands it to the peers.
//!
//! Both are behind one relaxed atomic load, so a node with replication off pays
//! a predictable-branch's worth of nothing.
//!
//! ## Why the buffer is keyed by transaction id
//!
//! A transaction's statements have to travel together or a replica could apply
//! half of one. The executor already threads a [`TxId`] through every statement
//! it runs, including the ones a `BEGIN` block accumulates over several round
//! trips, so keying on it groups exactly what committed together — without
//! replication having to know anything about sessions or connections.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};

use arc_swap::{ArcSwap, ArcSwapOption};
use cacheguard::CacheGuard;
use dashmap::DashMap;
use log::{debug, info, warn};

use crate::core::mvcc::transaction::TxId;

use super::config::{ConflictPolicy, NodeId, ReplicationConfig, SyncMode};
use super::hlc::{Hlc, HybridClock};
use super::log::{AppliedState, ReplicationLog};
use super::peer::PeerHandle;
use super::protocol::TxnRecord;

/// What one in-flight transaction has written so far.
///
/// Each statement carries a database-wide order number. A transaction with
/// savepoints is not one id but several — writes inside a savepoint carry the
/// sub-transaction's id — so a commit gathers several of these and has to put
/// the statements back into the order they ran. The id alone cannot say: two
/// sub-transactions interleave, and ids are handed out at `SAVEPOINT` rather
/// than per statement.
#[derive(Debug, Default)]
struct Capture {
    statements: Vec<(u64, String)>,
    tables: Vec<String>,
}

impl Capture {
    fn note(&mut self, order: u64, sql: &str, table: Option<&str>) {
        self.statements.push((order, sql.to_string()));
        if let Some(table) = table {
            // Linear scan rather than a set: a transaction touches a handful of
            // tables, and a `Vec` of three strings beats a hash of one.
            if !self.tables.iter().any(|t| t == table) {
                self.tables.push(table.to_string());
            }
        }
    }
}

/// Counters behind `SHOW REPLICATION STATUS`.
#[derive(Debug, Default)]
pub struct Stats {
    /// The three hot counters are padded apart; the rest are not, on purpose.
    /// These are incremented from three *different* sets of threads — `captured`
    /// by whichever thread runs the write statement, `published` by the capture
    /// drain, `applied` by the inbound apply task — so on one line all three
    /// were serialising against each other. The counters below them either fire
    /// on error paths or are bumped several at a time by the peer task, which
    /// wants them sharing a line rather than split across several.
    pub captured: CacheGuard<AtomicU64>,
    pub published: CacheGuard<AtomicU64>,
    pub applied: CacheGuard<AtomicU64>,
    pub duplicates: AtomicU64,
    pub conflicts: AtomicU64,
    pub failures: AtomicU64,
    pub gaps: AtomicU64,
    /// Payload bytes before compression, summed over every batch sent.
    pub bytes_raw: AtomicU64,
    /// Bytes actually handed to the transport.
    pub bytes_wire: AtomicU64,
    pub batches_sent: AtomicU64,
    /// Batches that were actually compressed, as opposed to sent as they were
    /// because they fell under the size floor or did not compress.
    pub compressed_batches: AtomicU64,
    /// Commits that waited for a peer and gave up on the timeout.
    pub ack_timeouts: AtomicU64,
    /// Applies retried after a lock failure (40P01, 55P03, 57014, 57P01):
    /// aborted, released and run again rather than skipped (finding L2).
    pub apply_retries: AtomicU64,
}

impl Stats {
    /// How the bytes on the wire compare with the payload they carried, as a
    /// percentage saved. Negative when the wire carried more, which it does
    /// when nothing was compressed — every frame still costs its header.
    ///
    /// `None` before anything has been sent.
    pub fn compression_saving(&self) -> Option<f64> {
        let raw = self.bytes_raw.load(Ordering::Relaxed);
        let wire = self.bytes_wire.load(Ordering::Relaxed);
        if raw == 0 {
            return None;
        }
        Some((1.0 - wire as f64 / raw as f64) * 100.0)
    }
}

/// When a table was last written locally, for the last-writer-wins comparison.
#[derive(Clone, Copy, Debug)]
pub struct TableStamp {
    pub hlc: Hlc,
    pub node: NodeId,
}

/// Lets a commit wait until enough peers have taken its record.
///
/// One per synchronous commit, shared with every peer task the record was
/// handed to. A peer signals it once; the waiter wakes when `needed` have.
#[derive(Debug)]
pub struct AckWaiter {
    needed: usize,
    got: AtomicU64,
    notify: tokio::sync::Notify,
}

impl AckWaiter {
    pub fn new(needed: usize) -> Arc<AckWaiter> {
        Arc::new(AckWaiter {
            needed,
            got: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        })
    }

    /// One peer has the record. Wakes the waiter once enough do.
    pub fn signal(&self) {
        let got = self.got.fetch_add(1, Ordering::AcqRel) + 1;
        if got as usize >= self.needed {
            self.notify.notify_waiters();
        }
    }

    #[inline]
    pub fn satisfied(&self) -> bool {
        self.got.load(Ordering::Acquire) as usize >= self.needed
    }

    /// Waits for enough acknowledgements, or for `timeout`. Returns whether it
    /// was satisfied rather than timed out.
    pub async fn wait(&self, timeout: std::time::Duration) -> bool {
        if self.satisfied() {
            return true;
        }
        // Registering the waiter *before* the last check closes the race where
        // the final `signal` lands between a check and an await, which would
        // otherwise park until the timeout for a record that was already
        // acknowledged.
        let notified = self.notify.notified();
        tokio::pin!(notified);
        if self.satisfied() {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok() || self.satisfied()
    }
}

/// One record on its way to one peer.
#[derive(Clone, Debug)]
pub struct Outbound {
    pub record: Arc<TxnRecord>,
    /// Present only for a synchronous commit.
    pub ack: Option<Arc<AckWaiter>>,
}

/// Markers whose presence makes a statement mean something different on each
/// node. Uppercase; the scan is case-insensitive.
const NONDETERMINISTIC: &[&str] = &[
    "RANDOM(",
    "NOW(",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "NEXTVAL(",
    // The other spellings of the clock (`core::statement_clock`).
    "STATEMENT_TIMESTAMP(",
    "CLOCK_TIMESTAMP(",
    "TRANSACTION_TIMESTAMP(",
    "LOCALTIMESTAMP",
];

/// Which of [`NONDETERMINISTIC`] appear in `sql`, as a bitmask.
///
/// One pass over the statement, carrying a match position per marker, rather
/// than a `windows()` scan per marker. That difference is the point: this runs
/// on every replicated write, and the previous shape cost roughly
/// `len * markers` byte comparisons — tens of thousands of them for a large
/// `INSERT`, to answer a question that is almost always "none of them".
///
/// The restart rule is the naive one rather than a proper failure function.
/// That is exact for these markers, none of which begins with a sequence that
/// recurs inside itself, and the consequence of being wrong would be a missed
/// warning rather than a wrong answer.
fn scan_nondeterministic(sql: &str) -> u16 {
    let mut progress = [0usize; NONDETERMINISTIC.len()];
    let mut found = 0u16;

    for byte in sql.as_bytes() {
        let upper = byte.to_ascii_uppercase();
        for (index, marker) in NONDETERMINISTIC.iter().enumerate() {
            let marker = marker.as_bytes();
            if upper == marker[progress[index]] {
                progress[index] += 1;
                if progress[index] == marker.len() {
                    found |= 1 << index;
                    progress[index] = 0;
                }
            } else {
                // This byte cannot continue the match, but it may start one.
                progress[index] = usize::from(upper == marker[0]);
            }
        }
    }

    found
}

/// A node's replication state.
///
/// Held by the [`Database`](crate::core::database::Database) and shared with
/// every peer task and the inbound listener.
pub struct Replication {
    config: ArcSwap<ReplicationConfig>,
    /// The gate. Read on every write statement, written only by configuration
    /// changes, so `Relaxed` is right: a statement that races a `REPLICATION
    /// DISABLE` may be captured or not, and either is a correct answer.
    ///
    /// Being read by everyone and written by nobody is exactly the profile that
    /// suffers most from false sharing, and it used to sit one word away from
    /// `seq`. Every captured statement's `fetch_add` evicted the gate from every
    /// other core, so a database with replication *disabled* — where the load is
    /// the only replication work a write does at all — still paid a coherence
    /// miss per statement. On its own line the load is an L1 hit forever.
    active: CacheGuard<AtomicBool>,
    clock: HybridClock,
    /// The local origin sequence. Handed out in order, gap-free, so a peer can
    /// ask for "everything after N".
    seq: AtomicU64,
    log: ArcSwapOption<ReplicationLog>,
    applied: ArcSwap<AppliedState>,
    capture: DashMap<TxId, Capture, ahash::RandomState>,
    /// Transactions currently applying a remote record, and which origin they
    /// belong to. Their statements must not be captured again, or a mesh would
    /// echo every transaction forever.
    applying: DashMap<TxId, NodeId, ahash::RandomState>,
    peers: DashMap<String, Arc<PeerHandle>, ahash::RandomState>,
    stats: Stats,
    table_stamps: DashMap<String, TableStamp, ahash::RandomState>,
    /// One bit per entry in [`NONDETERMINISTIC`], so each warning is logged
    /// once per process rather than once per statement.
    ///
    /// Was padded, and should not be. `note_statement` reads `active`, then
    /// `fetch_add`s `capture_order`, then tests this bit — three padded fields
    /// touched by one lock-free operation is three cache-line fills where one
    /// would do, which is the same arithmetic that made padding the `Wal` cost
    /// +31%. Only `active` keeps its own line, because on a database with
    /// replication *off* — the common case — it is the only one of the four
    /// that is touched at all.
    warned: AtomicU16,
    /// Hands out the execution order stamped on each captured statement.
    capture_order: AtomicU64,
    /// Whether the inbound listener has been started. A port binds once per
    /// process, so a `DISABLE` followed by an `ENABLE` restarts the peers
    /// without trying to bind it a second time.
    listener: AtomicBool,
    /// A socket to serve instead of binding `listen_port`, taken by the next
    /// start. See [`Replication::serve_on`].
    bound_listener: parking_lot::Mutex<Option<std::net::TcpListener>>,
}

impl std::fmt::Debug for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replication")
            .field("active", &self.is_active())
            .field("node_id", &self.node_id())
            .field("peers", &self.peers.len())
            .finish()
    }
}

impl Default for Replication {
    fn default() -> Self {
        Replication::new()
    }
}

impl Replication {
    /// A disabled node. Configuration arrives later, from a flag or a
    /// statement.
    pub fn new() -> Replication {
        Replication {
            config: ArcSwap::from_pointee(ReplicationConfig::default()),
            active: AtomicBool::new(false).into(),
            clock: HybridClock::new(),
            seq: AtomicU64::new(0),
            log: ArcSwapOption::empty(),
            applied: ArcSwap::from_pointee(AppliedState::in_memory()),
            capture: DashMap::with_hasher(ahash::RandomState::new()),
            applying: DashMap::with_hasher(ahash::RandomState::new()),
            peers: DashMap::with_hasher(ahash::RandomState::new()),
            stats: Stats::default(),
            table_stamps: DashMap::with_hasher(ahash::RandomState::new()),
            warned: AtomicU16::new(0),
            capture_order: AtomicU64::new(0),
            listener: AtomicBool::new(false),
            bound_listener: parking_lot::Mutex::new(None),
        }
    }

    /// Claims the right to start the inbound listener, once.
    ///
    /// Returns true to exactly one caller. Everything after it gets false and
    /// leaves the running listener alone.
    pub fn claim_listener(&self) -> bool {
        !self.listener.swap(true, Ordering::AcqRel)
    }

    /// Gives the claim back, so a later attempt can bind.
    ///
    /// Called when the bind failed. Without it a port that was busy at the
    /// wrong moment would keep this node listener-less for the life of the
    /// process, with `REPLICATION ENABLE` reporting success each time.
    pub fn release_listener(&self) {
        self.listener.store(false, Ordering::Release);
    }

    /// Has the next start serve `listener` rather than bind `listen_port`.
    ///
    /// For tests, which must know every node's port before any node starts —
    /// each node is the others' peer — and so bind them all up front. Handing
    /// over the port *number* instead means letting the socket go until the
    /// node binds it again, and a test running in parallel can be given the
    /// same freed port in between: one of them then fails to bind, or a peer
    /// connects to the wrong test's node. Keeping the socket bound throughout
    /// closes that window.
    ///
    /// The listener's port must be the configured `listen_port`, which is
    /// what this node advertises to its peers. It reaches the listener however
    /// the start is asked for — `start_replication` or `REPLICATION ENABLE`.
    pub fn serve_on(&self, listener: std::net::TcpListener) {
        *self.bound_listener.lock() = Some(listener);
    }

    /// The socket [`Replication::serve_on`] left, if any, for the start that
    /// is about to bind.
    pub fn take_bound_listener(&self) -> Option<std::net::TcpListener> {
        self.bound_listener.lock().take()
    }

    /// Whether the listener is running.
    pub fn listener_started(&self) -> bool {
        self.listener.load(Ordering::Acquire)
    }

    // -- configuration ------------------------------------------------------

    pub fn config(&self) -> Arc<ReplicationConfig> {
        self.config.load_full()
    }

    /// Replaces the configuration wholesale. Validated first, so a rejected one
    /// leaves the running node exactly as it was.
    pub fn set_config(&self, config: ReplicationConfig) -> Result<(), String> {
        config.validate()?;
        self.config.store(Arc::new(config));
        Ok(())
    }

    /// Edits the configuration in place and publishes the result.
    ///
    /// Read-modify-write under a racing writer would lose one of the two edits.
    /// Configuration changes arrive from an operator typing a statement, so the
    /// contention is nil and a compare-and-swap loop is the simplest thing that
    /// cannot drop one.
    pub fn update_config<F>(&self, edit: F) -> Result<Arc<ReplicationConfig>, String>
    where
        F: Fn(&mut ReplicationConfig) -> Result<(), String>,
    {
        loop {
            let current = self.config.load_full();
            let mut next = (*current).clone();
            edit(&mut next)?;
            next.validate()?;

            let next = Arc::new(next);
            let previous = self.config.compare_and_swap(&current, next.clone());
            if Arc::ptr_eq(&previous, &current) {
                return Ok(next);
            }
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::Relaxed);
        info!(
            "Replication {}",
            if active { "enabled" } else { "disabled" }
        );
    }

    #[inline]
    pub fn node_id(&self) -> NodeId {
        self.config.load().node_id
    }

    pub fn clock(&self) -> &HybridClock {
        &self.clock
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The highest local sequence handed out.
    pub fn local_seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    // -- storage ------------------------------------------------------------

    /// Opens the log and the applied-position state.
    ///
    /// `dir` of `None` is an in-memory database, which keeps both in memory:
    /// there is nothing to be durable for when the data itself is not.
    pub fn open_storage(&self, dir: Option<&std::path::Path>) -> std::io::Result<()> {
        let retention = self.config.load().log_retention_bytes;

        let (log, applied) = match dir {
            Some(dir) => (
                ReplicationLog::open(dir, retention)?,
                AppliedState::open(dir)?,
            ),
            None => (
                ReplicationLog::in_memory(retention),
                AppliedState::in_memory(),
            ),
        };

        // Resume above what this node already published, so a sequence is never
        // reused with a different transaction behind it, and never hand out a
        // timestamp below one a peer has already seen.
        let node_id = self.node_id();
        self.seq.store(log.highest(node_id), Ordering::Release);
        self.clock.observe(log.last_hlc());

        self.log.store(Some(Arc::new(log)));
        self.applied.store(Arc::new(applied));

        info!(
            "Replication storage ready: local sequence resumes at {}",
            self.local_seq()
        );
        Ok(())
    }

    pub fn log(&self) -> Option<Arc<ReplicationLog>> {
        self.log.load_full()
    }

    pub fn applied(&self) -> Arc<AppliedState> {
        self.applied.load_full()
    }

    /// Flushes both files. Called periodically and at shutdown.
    pub fn flush(&self) {
        if let Some(log) = self.log.load_full() {
            if let Err(e) = log.flush() {
                warn!("Could not flush the replication log: {}", e);
            }
        }
        if let Err(e) = self.applied.load().flush() {
            warn!("Could not flush the replication state: {}", e);
        }
    }

    // -- peers --------------------------------------------------------------

    pub fn insert_peer(&self, peer: Arc<PeerHandle>) {
        self.peers.insert(peer.name.clone(), peer);
    }

    pub fn remove_peer(&self, name: &str) -> Option<Arc<PeerHandle>> {
        self.peers.remove(name).map(|(_, peer)| peer)
    }

    pub fn peer(&self, name: &str) -> Option<Arc<PeerHandle>> {
        self.peers.get(name).map(|entry| entry.value().clone())
    }

    pub fn peers(&self) -> Vec<Arc<PeerHandle>> {
        self.peers
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    // -- the apply side -----------------------------------------------------

    /// Marks `tx` as applying a record from `origin`, so its statements are not
    /// captured and re-published.
    ///
    /// Without this a two-node mesh would ship each transaction back and forth
    /// forever, each hop re-originating it under a fresh sequence that the
    /// deduplication could not recognise.
    pub fn enter_apply(&self, tx: TxId, origin: NodeId) {
        self.applying.insert(tx, origin);
    }

    pub fn leave_apply(&self, tx: TxId) {
        self.applying.remove(&tx);
    }

    #[inline]
    pub fn is_applying(&self, tx: TxId) -> bool {
        !self.applying.is_empty() && self.applying.contains_key(&tx)
    }

    /// Records that `node` wrote `tables` at `hlc`, for last-writer-wins.
    pub fn stamp_tables(&self, tables: &[String], hlc: Hlc, node: NodeId) {
        for table in tables {
            self.table_stamps
                .entry(table.clone())
                .and_modify(|stamp| {
                    if hlc > stamp.hlc {
                        *stamp = TableStamp { hlc, node };
                    }
                })
                .or_insert(TableStamp { hlc, node });
        }
    }

    pub fn table_stamp(&self, table: &str) -> Option<TableStamp> {
        self.table_stamps.get(table).map(|entry| *entry.value())
    }

    /// Whether an incoming record is superseded by what this node already has,
    /// under [`ConflictPolicy::LastWriterWins`].
    ///
    /// True only when *every* table the record writes has a local write that is
    /// strictly later. A record that is newer for any table it touches still
    /// carries information, so it applies.
    ///
    /// The tie — two nodes committing at the same hybrid timestamp — is broken
    /// by node id, so both ends of a link reach the same verdict rather than
    /// each keeping its own write.
    pub fn is_superseded(&self, record: &TxnRecord) -> bool {
        if record.tables.is_empty() {
            return false;
        }
        record
            .tables
            .iter()
            .all(|table| match self.table_stamp(table) {
                None => false,
                Some(stamp) => {
                    stamp.hlc > record.commit_hlc
                        || (stamp.hlc == record.commit_hlc && stamp.node > record.origin)
                }
            })
    }

    // -- the capture side ---------------------------------------------------

    /// A write statement succeeded inside `tx`.
    ///
    /// Costs one relaxed load when replication is off, which is the point: this
    /// sits on the write path of a database that is usually not replicating.
    pub fn note_statement(&self, tx: TxId, sql: &str, table: Option<&str>) {
        if !self.is_active() {
            return;
        }
        // A statement being applied *from* a peer is that peer's transaction,
        // not a new one of ours.
        if self.is_applying(tx) {
            return;
        }

        self.warn_if_nondeterministic(sql);

        let order = self.capture_order.fetch_add(1, Ordering::Relaxed);
        self.capture.entry(tx).or_default().note(order, sql, table);
    }

    /// Says so, once, when a statement will not mean the same thing elsewhere.
    ///
    /// Not an error: `NOW()` in a `WHERE` clause is usually harmless, and
    /// refusing the statement would be a worse answer than replicating it and
    /// saying why it might drift.
    fn warn_if_nondeterministic(&self, sql: &str) {
        let found = scan_nondeterministic(sql);
        if found == 0 {
            return;
        }

        for (bit, marker) in NONDETERMINISTIC.iter().enumerate() {
            let mask = 1u16 << bit;
            if found & mask == 0 {
                continue;
            }
            if self.warned.fetch_or(mask, Ordering::Relaxed) & mask != 0 {
                continue;
            }
            warn!(
                "Replicating a statement containing `{}`. Statement-based replication \
                 re-evaluates it on each node, so the rows may differ between them. \
                 Compute the value in the client and send a literal to avoid it.",
                marker
            );
        }
    }

    /// Drops what `tx` had buffered. A rolled-back transaction ships nothing.
    pub fn on_abort(&self, tx: TxId) {
        if self.capture.is_empty() {
            return;
        }
        self.capture.remove(&tx);
    }

    /// The same, over every id a transaction turned out to consist of.
    pub fn on_abort_family(&self, members: &[TxId]) {
        if self.capture.is_empty() {
            return;
        }
        for id in members {
            self.capture.remove(id);
        }
    }

    /// Turns `tx`'s buffer into a record, stamps it, and logs it.
    ///
    /// Returns `None` when there is nothing to ship: a read-only transaction,
    /// one applying a peer's record, or a node with replication off.
    ///
    /// The sequence and the timestamp are assigned here, at commit, rather than
    /// at `BEGIN`: they have to order transactions by when they *committed*, or
    /// a long-running transaction would appear to have happened before writes
    /// it actually observed.
    pub fn commit_record(&self, tx: TxId) -> Option<Arc<TxnRecord>> {
        self.commit_record_family(&[(tx, true)])
    }

    /// The same, for a transaction that is several ids.
    ///
    /// `members` is every id in the family paired with whether its writes
    /// survived — a savepoint that was rolled back contributes nothing, but is
    /// still drained, or its buffer would sit in the map for the life of the
    /// process.
    ///
    /// Statements are re-sorted into the order they ran. Concatenating them by
    /// id would reorder a block that opened a savepoint, wrote, released it and
    /// wrote again, and applying an `UPDATE` before the `INSERT` it depends on
    /// fails on the replica while succeeding here.
    pub fn commit_record_family(&self, members: &[(TxId, bool)]) -> Option<Arc<TxnRecord>> {
        if self.capture.is_empty() {
            return None;
        }

        let mut statements: Vec<(u64, String)> = Vec::new();
        let mut tables: Vec<String> = Vec::new();

        for (id, survived) in members {
            let Some((_, capture)) = self.capture.remove(id) else {
                continue;
            };
            if !survived {
                continue;
            }
            statements.extend(capture.statements);
            for table in capture.tables {
                if !tables.iter().any(|t| *t == table) {
                    tables.push(table);
                }
            }
        }

        if statements.is_empty() {
            return None;
        }
        if !self.is_active() {
            return None;
        }

        statements.sort_unstable_by_key(|(order, _)| *order);
        let capture = Capture { statements, tables };

        let origin = self.node_id();
        if origin == 0 {
            return None;
        }

        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        let commit_hlc = self.clock.now();

        let record = Arc::new(TxnRecord {
            origin,
            seq,
            commit_hlc,
            flags: 0,
            statements: capture.statements.into_iter().map(|(_, sql)| sql).collect(),
            tables: capture.tables,
        });

        self.stamp_tables(&record.tables, commit_hlc, origin);

        if let Some(log) = self.log.load_full() {
            if let Err(e) = log.append(&record) {
                // The transaction is committed and durable regardless; what is
                // lost is a peer's ability to catch up incrementally, which
                // costs it a resynchronisation rather than the write.
                warn!(
                    "Could not record transaction {}:{} in the replication log: {}",
                    origin, seq, e
                );
            }
        }

        self.stats.captured.fetch_add(1, Ordering::Relaxed);
        debug!(
            "Captured transaction {}:{} ({} statements)",
            origin,
            seq,
            record.statements.len()
        );

        Some(record)
    }

    /// Hands a record to the peers and, in a synchronous mode, waits for them.
    ///
    /// The wait is bounded: the local transaction has already committed, so a
    /// peer that has gone away must not be able to hold a client's write open
    /// indefinitely. A commit that times out is reported in the statistics and
    /// the record stays queued, so the peer still gets it when it returns.
    pub async fn publish(&self, record: Arc<TxnRecord>) {
        let config = self.config.load_full();
        let peers = self.peers();
        if peers.is_empty() {
            return;
        }

        let eligible: Vec<Arc<PeerHandle>> = peers
            .into_iter()
            .filter(|peer| !peer.is_paused() && peer.name != "")
            .collect();
        if eligible.is_empty() {
            return;
        }

        let required = config.sync_mode.required_acks(eligible.len());
        let waiter = (required > 0).then(|| AckWaiter::new(required));

        for peer in &eligible {
            peer.enqueue(Outbound {
                record: record.clone(),
                ack: waiter.clone(),
            });
        }
        self.stats.published.fetch_add(1, Ordering::Relaxed);

        if let Some(waiter) = waiter {
            if !waiter.wait(config.ack_timeout).await {
                self.stats.ack_timeouts.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Transaction {}:{} committed locally but only {} of {} peer(s) \
                     acknowledged within {:?}; it stays queued for the rest",
                    record.origin,
                    record.seq,
                    waiter.got.load(Ordering::Acquire),
                    required,
                    config.ack_timeout
                );
            }
        }
    }

    /// Forwards a record that arrived from `from_peer` to the other peers.
    ///
    /// Only under `relay`. In the intended full mesh every node hears every
    /// commit first-hand, and relaying there would send each transaction once
    /// per peer for no gain.
    pub fn relay(&self, record: Arc<TxnRecord>, from_node: NodeId) {
        if !self.config.load().relay {
            return;
        }
        for peer in self.peers() {
            // By node id rather than by name: the record arrived over a
            // connection, and what identifies the far end of one is the id it
            // gave in its handshake, not whatever this node happens to call it.
            if peer.is_paused() || peer.remote_node() == record.origin {
                continue;
            }
            if from_node != 0 && peer.remote_node() == from_node {
                continue;
            }
            peer.enqueue(Outbound {
                record: record.clone(),
                ack: None,
            });
        }
    }

    /// The conflict policy in force.
    #[inline]
    pub fn conflict_policy(&self) -> ConflictPolicy {
        self.config.load().conflict
    }

    #[inline]
    pub fn sync_mode(&self) -> SyncMode {
        self.config.load().sync_mode
    }

    /// Transactions whose buffers are still open, for the status view. A
    /// non-zero value at rest would mean buffers are leaking.
    pub fn open_captures(&self) -> usize {
        self.capture.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::config::PeerConfig;

    fn configured() -> Replication {
        let replication = Replication::new();
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

    #[test]
    fn a_disabled_node_captures_nothing() {
        let replication = Replication::new();
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));

        assert_eq!(replication.open_captures(), 0);
        assert!(replication.commit_record(7).is_none());
    }

    #[test]
    fn statements_in_one_transaction_travel_as_one_record() {
        let replication = configured();
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        replication.note_statement(7, "INSERT INTO t (id) VALUES (2)", Some("t"));
        replication.note_statement(7, "UPDATE u SET x = 1", Some("u"));

        let record = replication
            .commit_record(7)
            .expect("three writes committed");
        assert_eq!(record.statements.len(), 3);
        assert_eq!(record.tables, vec!["t".to_string(), "u".to_string()]);
        assert_eq!(record.origin, 1);
        assert_eq!(record.seq, 1);
    }

    #[test]
    fn a_rolled_back_transaction_ships_nothing() {
        let replication = configured();
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        replication.on_abort(7);

        assert!(replication.commit_record(7).is_none());
        assert_eq!(replication.open_captures(), 0);
    }

    #[test]
    fn a_read_only_transaction_ships_nothing() {
        let replication = configured();
        assert!(replication.commit_record(7).is_none());
    }

    /// The loop-prevention invariant: what a peer sent us is not ours to
    /// re-originate.
    #[test]
    fn a_statement_applied_from_a_peer_is_not_captured_again() {
        let replication = configured();
        replication.enter_apply(7, 2);
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        replication.leave_apply(7);

        assert!(
            replication.commit_record(7).is_none(),
            "an echo would loop forever"
        );
    }

    #[test]
    fn sequences_are_handed_out_in_order_and_without_gaps() {
        let replication = configured();
        for tx in 1..=5 {
            replication.note_statement(tx, "INSERT INTO t (id) VALUES (1)", Some("t"));
            let record = replication.commit_record(tx).unwrap();
            assert_eq!(record.seq, tx);
        }
        assert_eq!(replication.local_seq(), 5);
    }

    #[test]
    fn commit_timestamps_increase_with_the_sequence() {
        let replication = configured();
        let mut previous = 0;
        for tx in 1..=20 {
            replication.note_statement(tx, "INSERT INTO t (id) VALUES (1)", Some("t"));
            let record = replication.commit_record(tx).unwrap();
            assert!(record.commit_hlc > previous);
            previous = record.commit_hlc;
        }
    }

    #[test]
    fn transactions_do_not_bleed_into_each_other() {
        let replication = configured();
        replication.note_statement(1, "INSERT INTO a (id) VALUES (1)", Some("a"));
        replication.note_statement(2, "INSERT INTO b (id) VALUES (2)", Some("b"));

        let first = replication.commit_record(1).unwrap();
        let second = replication.commit_record(2).unwrap();

        assert_eq!(
            first.statements,
            vec!["INSERT INTO a (id) VALUES (1)".to_string()]
        );
        assert_eq!(
            second.statements,
            vec!["INSERT INTO b (id) VALUES (2)".to_string()]
        );
    }

    #[test]
    fn a_table_is_listed_once_however_often_it_is_written() {
        let replication = configured();
        for _ in 0..10 {
            replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        }
        let record = replication.commit_record(7).unwrap();
        assert_eq!(record.tables, vec!["t".to_string()]);
    }

    #[test]
    fn a_node_without_an_id_is_rejected_rather_than_replicating_anonymously() {
        let replication = Replication::new();
        assert!(
            replication
                .set_config(ReplicationConfig::default())
                .is_err()
        );
    }

    #[test]
    fn a_rejected_configuration_leaves_the_running_one_alone() {
        let replication = configured();
        let before = replication.config().node_id;

        let result = replication.update_config(|config| {
            config.node_id = 0;
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(replication.config().node_id, before);
    }

    #[test]
    fn a_configuration_edit_is_visible_afterwards() {
        let replication = configured();
        replication
            .update_config(|config| {
                config
                    .peers
                    .push(PeerConfig::new("berlin", "10.0.0.2:61171"));
                Ok(())
            })
            .unwrap();

        assert_eq!(replication.config().peers.len(), 1);
    }

    /// The marker's index in `NONDETERMINISTIC`, for readable assertions.
    fn bit_of(marker: &str) -> u16 {
        1 << NONDETERMINISTIC
            .iter()
            .position(|m| *m == marker)
            .expect("a known marker")
    }

    #[test]
    fn the_scan_finds_what_it_should_and_nothing_else() {
        assert_eq!(scan_nondeterministic("select now() from t"), bit_of("NOW("));
        assert_eq!(scan_nondeterministic("SELECT Random( )"), bit_of("RANDOM("));
        assert_eq!(scan_nondeterministic("select snow from t"), 0);
        assert_eq!(scan_nondeterministic("no"), 0);
        assert_eq!(scan_nondeterministic("INSERT INTO t VALUES (1, 'ada')"), 0);
    }

    #[test]
    fn the_scan_reports_every_marker_it_meets() {
        let found = scan_nondeterministic("INSERT INTO t VALUES (now(), random(), CURRENT_DATE)");
        assert_ne!(found & bit_of("NOW("), 0);
        assert_ne!(found & bit_of("RANDOM("), 0);
        assert_ne!(found & bit_of("CURRENT_DATE"), 0);
    }

    /// Every spelling of the clock is a marker, `now()`'s and the others.
    #[test]
    fn every_clock_function_is_nondeterministic() {
        for (sql, marker) in [
            ("SELECT statement_timestamp()", "STATEMENT_TIMESTAMP("),
            ("INSERT INTO t VALUES (clock_timestamp())", "CLOCK_TIMESTAMP("),
            ("UPDATE t SET at = Transaction_Timestamp()", "TRANSACTION_TIMESTAMP("),
            ("INSERT INTO t VALUES (LOCALTIMESTAMP)", "LOCALTIMESTAMP"),
        ] {
            assert_ne!(scan_nondeterministic(sql) & bit_of(marker), 0, "{sql}");
        }
    }

    /// A restart in the middle of a partial match must not lose the real one.
    #[test]
    fn a_false_start_does_not_hide_the_match_after_it() {
        assert_ne!(scan_nondeterministic("nnow(") & bit_of("NOW("), 0);
        assert_ne!(
            scan_nondeterministic("cucurrent_date") & bit_of("CURRENT_DATE"),
            0
        );
        assert_ne!(scan_nondeterministic("ranrandom(") & bit_of("RANDOM("), 0);
    }

    #[test]
    fn a_marker_split_across_a_long_statement_is_still_found() {
        let padding = "x".repeat(10_000);
        let sql = format!("INSERT INTO t VALUES ('{}', now())", padding);
        assert_ne!(scan_nondeterministic(&sql) & bit_of("NOW("), 0);
    }

    #[test]
    fn last_writer_wins_keeps_the_later_write() {
        let replication = configured();
        replication.stamp_tables(&["t".to_string()], 5_000, 1);

        let older = TxnRecord {
            origin: 2,
            seq: 1,
            commit_hlc: 4_000,
            flags: 0,
            statements: vec!["UPDATE t SET x = 1".to_string()],
            tables: vec!["t".to_string()],
        };
        let newer = TxnRecord {
            commit_hlc: 6_000,
            ..older.clone()
        };

        assert!(
            replication.is_superseded(&older),
            "the local write is later"
        );
        assert!(
            !replication.is_superseded(&newer),
            "the remote write is later"
        );
    }

    #[test]
    fn a_record_touching_any_untouched_table_still_applies() {
        let replication = configured();
        replication.stamp_tables(&["t".to_string()], 5_000, 1);

        let record = TxnRecord {
            origin: 2,
            seq: 1,
            commit_hlc: 4_000,
            flags: 0,
            statements: vec!["UPDATE t SET x = 1".to_string()],
            // `u` has never been written here, so this record is not redundant.
            tables: vec!["t".to_string(), "u".to_string()],
        };

        assert!(!replication.is_superseded(&record));
    }

    #[test]
    fn a_timestamp_tie_is_broken_the_same_way_on_both_nodes() {
        let replication = configured();
        replication.stamp_tables(&["t".to_string()], 5_000, 9);

        let from_lower_node = TxnRecord {
            origin: 3,
            seq: 1,
            commit_hlc: 5_000,
            flags: 0,
            statements: vec!["UPDATE t SET x = 1".to_string()],
            tables: vec!["t".to_string()],
        };
        // Local node 9 beats remote node 3 at the same instant, and node 3
        // performing the mirror comparison reaches the same conclusion.
        assert!(replication.is_superseded(&from_lower_node));
    }

    #[tokio::test]
    async fn an_ack_waiter_wakes_once_enough_peers_have_answered() {
        let waiter = AckWaiter::new(2);
        let signaller = waiter.clone();
        tokio::spawn(async move {
            signaller.signal();
            signaller.signal();
        });

        assert!(waiter.wait(std::time::Duration::from_secs(5)).await);
    }

    #[tokio::test]
    async fn an_ack_waiter_gives_up_rather_than_holding_a_commit_open() {
        let waiter = AckWaiter::new(2);
        waiter.signal();

        assert!(!waiter.wait(std::time::Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn an_already_satisfied_waiter_does_not_wait() {
        let waiter = AckWaiter::new(1);
        waiter.signal();

        assert!(waiter.wait(std::time::Duration::from_millis(1)).await);
    }
}
