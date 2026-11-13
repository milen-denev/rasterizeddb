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

/// Where one of this node's records stands between being written and being
/// sent.
#[derive(Debug, Clone, Copy)]
enum Pending {
    /// Written to the log ahead of its transaction's commit. Whether it is
    /// real is not known yet.
    Unresolved,
    /// Committed, or voided. Sendable once the write-ahead log is durable
    /// through `gate`: the record, and the outcome it depends on, were both
    /// logged before that point.
    Resolved { gate: u64 },
}

/// This node's records that are not yet free to send, and how far the stream
/// is.
///
/// A record is written, with its sequence number, *before* its transaction
/// commits, and released to the peers only once that commit is durable.
/// Senders read the log up to `released` and never past it, so a peer cannot
/// receive a transaction this node might still disown, and what it receives
/// is in sequence order with no holes.
#[derive(Debug, Default)]
struct Outbox {
    pending: std::collections::BTreeMap<u64, Pending>,
    /// Every sequence up to this one is settled and durable.
    released: u64,
}

/// A record written ahead of its transaction's commit.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub record: Arc<TxnRecord>,
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

/// Whether `sql` is a plain `INSERT`, `UPDATE` or `DELETE`: a statement whose
/// whole effect is rows.
pub(super) fn plain_row_write(sql: &str) -> bool {
    let word = sql.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    word.eq_ignore_ascii_case("INSERT")
        || word.eq_ignore_ascii_case("UPDATE")
        || word.eq_ignore_ascii_case("DELETE")
}

tokio::task_local! {
    /// Set around work that is this node's own business: see
    /// [`Replication::local_only`].
    static LOCAL_ONLY: ();
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
    /// ask for "everything after N". Moved only under `outbox`, together with
    /// the append it numbers: the log holds this node's records in sequence
    /// order.
    seq: AtomicU64,
    outbox: parking_lot::Mutex<Outbox>,
    /// Records written and not yet settled, by the transaction they belong
    /// to: the sequence, and whether the log must be synced before the
    /// statement is acknowledged (a record with no row write behind it has no
    /// commit sync to ride on).
    prepared: DashMap<TxId, (u64, bool), ahash::RandomState>,
    /// How many entries `prepared` holds, so the abort path can ask without
    /// taking every shard's lock.
    prepared_count: std::sync::atomic::AtomicUsize,
    /// The database's own write-ahead log, which the replication files ride
    /// on. `None` for an in-memory database or one that keeps no log.
    wal: ArcSwapOption<crate::core::wal::Wal>,
    /// Where storage was opened, so a second start does not open it again
    /// over records that are still being settled.
    storage: parking_lot::Mutex<Option<Option<std::path::PathBuf>>>,
    /// Wakes the senders: something was released, or a relayed record logged.
    wake: tokio::sync::Notify,
    /// Wakes commits waiting for a peer: some peer's position moved.
    acks: tokio::sync::Notify,
    /// One apply at a time for each origin. Two connections carrying the same
    /// record (a link that reconnected while its old request was still being
    /// served, a resync during a push) would otherwise both find it not yet
    /// applied, and both apply it.
    apply_locks: DashMap<NodeId, Arc<tokio::sync::Mutex<()>>, ahash::RandomState>,
    /// Origins whose stream this node has stopped applying until it is
    /// restarted, and why: see [`Replication::halt_apply`].
    halted: DashMap<NodeId, String, ahash::RandomState>,
    /// Set when a statement that ran could not be written to the replication
    /// log, so it will never reach a peer. Shown by `SHOW REPLICATION` until
    /// the node restarts: the peers are behind by something only a reseed or
    /// a manual statement restores.
    capture_failure: parking_lot::Mutex<Option<String>>,
    log: ArcSwapOption<ReplicationLog>,
    applied: ArcSwap<AppliedState>,
    capture: DashMap<TxId, Capture, ahash::RandomState>,
    /// Whether `capture` has ever held anything; see [`Self::nothing_captured`].
    ever_captured: AtomicBool,
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
            outbox: parking_lot::Mutex::new(Outbox::default()),
            prepared: DashMap::with_hasher(ahash::RandomState::new()),
            prepared_count: std::sync::atomic::AtomicUsize::new(0),
            wal: ArcSwapOption::empty(),
            storage: parking_lot::Mutex::new(None),
            wake: tokio::sync::Notify::new(),
            acks: tokio::sync::Notify::new(),
            apply_locks: DashMap::with_hasher(ahash::RandomState::new()),
            halted: DashMap::with_hasher(ahash::RandomState::new()),
            capture_failure: parking_lot::Mutex::new(None),
            log: ArcSwapOption::empty(),
            applied: ArcSwap::from_pointee(AppliedState::in_memory()),
            capture: DashMap::with_hasher(ahash::RandomState::new()),
            ever_captured: AtomicBool::new(false),
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

    /// Moves this node's sequence past what a peer holds of its stream. Only
    /// on an operator's say-so (`REPLICATION ADOPT PEER`): see the handshake
    /// in `peer::connect` for why it is not automatic.
    ///
    ///
    /// A peer holding more than this node remembers means the log lost its
    /// tail (a crash before the buffered tail reached the disk, or a log cut
    /// short at open). The sequence then resumed below numbers the peer has
    /// already applied, and every new transaction under one of them was taken
    /// for something already sent: never shipped, or dropped as a duplicate,
    /// with both nodes reporting themselves in step. The sequence moves past
    /// what the peer holds instead. Returns the sequence it was at when it had
    /// to move.
    pub fn adopt_peer_position(&self, peer_has: u64) -> Option<u64> {
        let _outbox = self.outbox.lock();
        let before = self.seq.fetch_max(peer_has, Ordering::AcqRel);
        (before < peer_has).then_some(before)
    }

    // -- storage ------------------------------------------------------------

    /// Opens the log and the applied-position state with nothing behind them:
    /// no write-ahead log, and nobody to ask how a transaction ended. For an
    /// in-memory database, and for tests of the pieces.
    pub fn open_storage(&self, dir: Option<&std::path::Path>) -> std::io::Result<()> {
        self.open_storage_logged(dir, None, &|_| false)
    }

    /// Opens the log and the applied-position state, and settles what a crash
    /// left in them.
    ///
    /// `dir` of `None` is an in-memory database, which keeps both in memory:
    /// there is nothing to be durable for when the data itself is not.
    ///
    /// `wal` is the database's own write-ahead log, already replayed, and
    /// `committed` answers for one of its transaction ids. Both files are
    /// written ahead of the commit they describe (see [`ReplicationLog`] and
    /// [`AppliedState`]), so each may end in an entry whose transaction never
    /// committed. They are decided here, once, at the first open after the
    /// crash: an aborted transaction's id is eventually forgotten and reads as
    /// committed from then on.
    ///
    /// Opening twice is opening once: a `REPLICATION ENABLE` after the server
    /// start finds the storage as the start left it.
    pub fn open_storage_logged(
        &self,
        dir: Option<&std::path::Path>,
        wal: Option<Arc<crate::core::wal::Wal>>,
        committed: &dyn Fn(TxId) -> bool,
    ) -> std::io::Result<()> {
        let mut storage = self.storage.lock();
        let wanted = dir.map(std::path::Path::to_path_buf);
        if storage.as_ref() == Some(&wanted) {
            drop(storage);
            self.resume_sequence();
            return Ok(());
        }
        let retention = self.config.load().log_retention_bytes;

        let (log, applied) = match dir {
            Some(dir) => (
                ReplicationLog::open_logged(dir, retention, wal.clone())?,
                AppliedState::open_logged(dir, wal.clone(), committed)?,
            ),
            None => (
                ReplicationLog::in_memory(retention),
                AppliedState::in_memory(),
            ),
        };

        self.clock.observe(log.last_hlc());
        let mut voided = 0usize;
        for (origin, seq, tx) in log.take_undecided() {
            if committed(tx) {
                log.confirm(seq);
            } else {
                voided += 1;
                warn!(
                    "Transaction {}:{} was written to the replication log and did not commit                      before this node stopped; it is voided",
                    origin, seq
                );
                // Fails only if the log cannot be written at all; the record
                // is void in memory either way, and the next open tries again.
                if let Err(e) = log.void(origin, seq, self.clock.now()) {
                    warn!("Could not write the void for {}:{}: {}", origin, seq, e);
                }
            }
        }
        if voided > 0 {
            if let Some(wal) = wal.as_ref() {
                if let Err(e) = wal.sync_pending() {
                    warn!("Could not make the voided records durable: {}", e);
                }
            } else if let Err(e) = log.sync() {
                warn!("Could not make the voided records durable: {}", e);
            }
        }

        self.wal.store(wal);
        self.log.store(Some(Arc::new(log)));
        self.applied.store(Arc::new(applied));
        *storage = Some(wanted);
        drop(storage);

        self.resume_sequence();
        info!(
            "Replication storage ready: local sequence resumes at {}",
            self.local_seq()
        );
        Ok(())
    }

    /// Resumes above what this node already published, so a sequence is never
    /// reused with a different transaction behind it. Asked again at every
    /// start, because the node id may only have been configured since the
    /// storage was opened.
    fn resume_sequence(&self) {
        let node_id = self.node_id();
        let Some(log) = self.log.load_full() else {
            return;
        };
        // The storage may have been opened before this configuration was
        // applied: what it keeps is the configuration's to say.
        log.set_retention(self.config.load().log_retention_bytes);
        let mut outbox = self.outbox.lock();
        let held = if node_id == 0 { 0 } else { log.highest(node_id) };
        self.seq.fetch_max(held, Ordering::AcqRel);
        if outbox.pending.is_empty() {
            outbox.released = outbox.released.max(self.seq.load(Ordering::Acquire));
        }
    }

    /// Whether the storage has been opened.
    pub fn storage_open(&self) -> bool {
        self.storage.lock().is_some()
    }

    /// A statement that ran and will never reach a peer, if there is one.
    pub fn capture_failure(&self) -> Option<String> {
        self.capture_failure.lock().clone()
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

    /// The lock that makes applies from `origin` take turns.
    pub fn apply_lock(&self, origin: NodeId) -> Arc<tokio::sync::Mutex<()>> {
        self.apply_locks
            .entry(origin)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Stops applying `origin`'s stream for the life of the process.
    ///
    /// For a failed sync under an applied batch. The transactions of that
    /// batch committed in memory and may or may not be on disk; nothing this
    /// process does afterwards can find out, and neither undoing them (a
    /// reader may have seen them) nor applying them again is right. A failed
    /// commit sync is treated the same way everywhere here: the write-ahead
    /// log refuses further commits until a restart, and the restart's replay
    /// decides what survived. The positions ride that log, so they come back
    /// agreeing with the rows.
    pub fn halt_apply(&self, origin: NodeId, why: String) {
        log::error!(
            "Applying origin {}'s stream is halted until this node is restarted: {}",
            origin,
            why
        );
        self.halted.insert(origin, why);
    }

    /// Why `origin`'s stream is not being applied, if it is halted.
    pub fn apply_halted(&self, origin: NodeId) -> Option<String> {
        if self.halted.is_empty() {
            return None;
        }
        self.halted.get(&origin).map(|why| why.value().clone())
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

    /// Runs `work` with nothing it writes captured for the peers.
    ///
    /// For what every node does for itself: a node creates its own role
    /// catalogue and its first role when it first starts, and so does every
    /// other. Shipped, each node's `CREATE TABLE` met the table the other had
    /// just made for itself, and both streams stopped on their first record.
    pub async fn local_only<F: std::future::Future>(work: F) -> F::Output {
        LOCAL_ONLY.scope((), work).await
    }

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
        if LOCAL_ONLY.try_with(|_| ()).is_ok() {
            return;
        }

        self.warn_if_nondeterministic(sql);

        let order = self.capture_order.fetch_add(1, Ordering::Relaxed);
        // Before the entry exists, so nobody can find the map occupied and
        // the flag still clear.
        self.ever_captured.store(true, Ordering::Release);
        self.capture.entry(tx).or_default().note(order, sql, table);
    }

    /// Whether no transaction has a buffer open.
    ///
    /// Asked at the end of every transaction on the server, a read's
    /// included. `DashMap::is_empty` takes each shard's lock in turn, sixty-
    /// four of them on this machine, to answer it; a database that has never
    /// captured a statement, which is every one that is not replicating,
    /// answers from one load instead.
    #[inline]
    fn nothing_captured(&self) -> bool {
        !self.ever_captured.load(Ordering::Acquire) || self.capture.is_empty()
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
        self.on_abort_family(&[tx]);
    }

    /// The same, over every id a transaction turned out to consist of.
    ///
    /// A transaction can be aborted after its record was written: the commit
    /// happened and its sync then failed. That record is voided here, so its
    /// place in the stream is kept and nothing of it is sent.
    pub fn on_abort_family(&self, members: &[TxId]) {
        if self.prepared_count.load(Ordering::Acquire) != 0 {
            for id in members {
                self.void_prepared(*id);
            }
        }
        if self.nothing_captured() {
            return;
        }
        for id in members {
            self.capture.remove(id);
        }
    }

    /// Turns `tx`'s buffer into a record and hands it straight to the peers:
    /// [`Replication::prepare_family`] and [`Replication::publish_prepared`]
    /// in one step, for a caller with no commit in between.
    pub fn commit_record(&self, tx: TxId) -> Option<Arc<TxnRecord>> {
        self.commit_record_family(&[(tx, true)])
    }

    /// [`Replication::commit_record`] for a transaction that is several ids.
    pub fn commit_record_family(&self, members: &[(TxId, bool)]) -> Option<Arc<TxnRecord>> {
        let root = members.first()?.0;
        let prepared = match self.prepare_family(members, root, false) {
            Ok(prepared) => prepared?,
            Err(e) => {
                warn!("Could not record a transaction in the replication log: {}", e);
                return None;
            }
        };
        self.release(root);
        Some(prepared.record)
    }

    /// Writes the record of a transaction that is about to commit.
    ///
    /// Called *before* the commit, while the transaction still holds its row
    /// locks. Three things follow from that order.
    ///
    /// - **No committed transaction is without its record.** The record is in
    ///   the replication log, and described to the write-ahead log, before
    ///   the outcome is; the commit's one sync covers both, record first. A
    ///   crash at any point leaves the record and no commit (voided at the
    ///   next open) or both. Assigned after the commit, as it was, a crash in
    ///   between left a committed transaction no peer would ever hear of.
    /// - **Sequence order is commit order where it matters.** A transaction
    ///   that waited for this one's row can only take its own number after
    ///   this one released the lock, which is after this number was taken.
    ///   Numbered after the lock release, the waiter could be numbered first,
    ///   and a peer applied the `UPDATE` before the `INSERT` it depended on.
    /// - **A failure here fails the commit.** Nothing has been committed yet,
    ///   so a record that cannot be written is a transaction that does not
    ///   happen, rather than one the peers silently never get.
    ///
    /// `members` is every id in the family paired with whether its writes
    /// survived: a savepoint that was rolled back contributes nothing, but is
    /// still drained, or its buffer would sit in the map for the life of the
    /// process. Statements are re-sorted into the order they ran.
    ///
    /// `wrote` says whether `root` wrote rows, and so will have a durable
    /// outcome. If it did, the record is real only if that outcome is
    /// "committed". If it did not (a statement that changed no row, a schema
    /// change), nothing durable will ever say it committed, so the record
    /// stands on its own.
    ///
    /// Returns `None` when there is nothing to ship: a read-only transaction,
    /// one applying a peer's record, or a node with replication off.
    pub fn prepare_family(
        &self,
        members: &[(TxId, bool)],
        root: TxId,
        wrote: bool,
    ) -> std::io::Result<Option<Prepared>> {
        if self.nothing_captured() {
            return Ok(None);
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

        if statements.is_empty() || !self.is_active() {
            return Ok(None);
        }
        let origin = self.node_id();
        if origin == 0 {
            return Ok(None);
        }

        statements.sort_unstable_by_key(|(order, _)| *order);
        let statements: Vec<String> = statements.into_iter().map(|(_, sql)| sql).collect();
        // A record with no commit sync to ride on is synced by its own
        // statement, unless losing it loses nothing: a write that changed no
        // row here has nothing on this node a peer could be missing.
        let sync_now = !wrote && !statements.iter().all(|sql| plain_row_write(sql));

        // One transaction is one record, and a record travels whole. Refused
        // here, before the commit, it is a transaction that does not happen;
        // let through, it would be one no peer could ever be sent.
        let size: usize = statements.iter().map(|sql| sql.len() + 4).sum::<usize>() + 64;
        if size > super::protocol::MAX_PAYLOAD / 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "the transaction's statements come to {} bytes, more than the {} a replicated \
                     transaction may hold; commit it in smaller transactions",
                    size,
                    super::protocol::MAX_PAYLOAD / 2
                ),
            ));
        }

        let log = self.log.load_full();
        let record = {
            let mut outbox = self.outbox.lock();
            let seq = self.seq.load(Ordering::Acquire) + 1;
            let record = Arc::new(TxnRecord {
                origin,
                seq,
                commit_hlc: self.clock.now(),
                flags: 0,
                statements,
                tables,
            });
            if let Some(log) = log.as_ref() {
                log.append_for(&record, if wrote { root } else { 0 })?;
            }
            self.seq.store(seq, Ordering::Release);
            outbox.pending.insert(seq, Pending::Unresolved);
            record
        };
        if let Some(log) = log.as_ref() {
            if !log.is_logged() {
                // No write-ahead log to carry the record: it is made durable
                // here, ahead of the commit, at the cost of a sync.
                if let Err(e) = log.sync() {
                    self.settle(record.seq, 0);
                    let _ = log.void(origin, record.seq, record.commit_hlc);
                    return Err(e);
                }
            }
        }
        self.prepared.insert(root, (record.seq, sync_now));
        self.prepared_count.fetch_add(1, Ordering::AcqRel);

        self.stamp_tables(&record.tables, record.commit_hlc, origin);
        self.stats.captured.fetch_add(1, Ordering::Relaxed);
        debug!(
            "Captured transaction {}:{} ({} statements)",
            origin,
            record.seq,
            record.statements.len()
        );

        Ok(Some(Prepared { record }))
    }

    /// Notes that a statement ran and could not be recorded.
    pub fn note_capture_failure(&self, what: String) {
        log::error!("{}", what);
        *self.capture_failure.lock() = Some(what);
    }

    /// The write-ahead log position everything logged so far is behind.
    fn gate_now(&self) -> u64 {
        self.wal
            .load()
            .as_ref()
            .map(|wal| wal.issued_through())
            .unwrap_or(0)
    }

    /// Marks `seq` settled, sendable once the log is durable through `gate`.
    fn settle(&self, seq: u64, gate: u64) {
        self.outbox
            .lock()
            .pending
            .insert(seq, Pending::Resolved { gate });
        self.wake.notify_waiters();
    }

    /// The transaction `root` committed: its record is real. Returns its
    /// sequence, or `None` when it had no record.
    ///
    /// The record goes to the peers once the write-ahead log is durable up to
    /// here, which covers the record and the outcome: at once after a commit
    /// that synced, and at the log writer's next round after one that did not
    /// (`synchronous_commit = off`). So nothing reaches a peer that a power
    /// cut could still take back from this node.
    pub fn release(&self, root: TxId) -> Option<u64> {
        if self.prepared_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        let (_, (seq, sync_now)) = self.prepared.remove(&root)?;
        self.prepared_count.fetch_sub(1, Ordering::AcqRel);
        let gate = self.gate_now();
        if sync_now {
            if let Some(wal) = self.wal.load_full() {
                if let Err(e) = wal.commit_through(gate) {
                    self.note_capture_failure(format!(
                        "Transaction {}:{} ran, but its replication record could not be made                          durable ({}); a crash now would leave the peers without it",
                        self.node_id(),
                        seq,
                        e
                    ));
                }
            }
        }
        if let Some(log) = self.log.load_full() {
            log.confirm(seq);
        }
        self.settle(seq, gate);
        self.stats.published.fetch_add(1, Ordering::Relaxed);
        Some(seq)
    }

    /// The transaction `root` did not commit after all: its record, if it
    /// had one, is voided.
    pub fn void_prepared(&self, root: TxId) {
        let Some((_, (seq, _))) = self.prepared.remove(&root) else {
            return;
        };
        self.prepared_count.fetch_sub(1, Ordering::AcqRel);
        if let Some(log) = self.log.load_full() {
            if let Err(e) = log.void(self.node_id(), seq, self.clock.now()) {
                warn!(
                    "Could not write the void for transaction {}:{} ({}); it is void in memory                      and the next start writes it",
                    self.node_id(),
                    seq,
                    e
                );
            }
        }
        self.settle(seq, self.gate_now());
    }

    /// Whether `root` has a record written and not yet settled.
    pub fn is_prepared(&self, root: TxId) -> bool {
        self.prepared_count.load(Ordering::Acquire) != 0 && self.prepared.contains_key(&root)
    }

    /// How far this node's stream may be sent: every sequence up to the
    /// answer is settled, and durable here.
    pub fn released_through(&self) -> u64 {
        let durable = self.wal.load().as_ref().map(|wal| wal.durable_through());
        let mut outbox = self.outbox.lock();
        loop {
            let Some((&seq, state)) = outbox.pending.iter().next() else {
                // Nothing in flight: everything numbered is out.
                let seq = self.seq.load(Ordering::Acquire);
                outbox.released = outbox.released.max(seq);
                break;
            };
            match *state {
                Pending::Resolved { gate } if durable.is_none_or(|durable| durable >= gate) => {
                    outbox.pending.remove(&seq);
                    outbox.released = outbox.released.max(seq);
                }
                _ => break,
            }
        }
        outbox.released
    }

    /// Records written and not yet released to the peers.
    pub fn unreleased(&self) -> usize {
        self.outbox.lock().pending.len()
    }

    /// Resolves once something may have become sendable. A sender takes this
    /// *before* it reads [`Replication::released_through`], so a release in
    /// between is not missed.
    pub fn woken(&self) -> tokio::sync::futures::Notified<'_> {
        self.wake.notified()
    }

    /// A peer's position moved, or a link came up or went down: commits
    /// waiting on a peer look again, and the log is told how far the slowest
    /// connected peer has got (`ReplicationLog::keep_for_peers`).
    pub fn note_ack(&self) {
        self.acks.notify_waiters();
        if let Some(log) = self.log.load_full() {
            let slowest = self
                .peers
                .iter()
                .filter(|peer| peer.is_connected() && !peer.is_paused())
                .filter(|peer| peer.state() == super::peer::PeerState::Streaming)
                .map(|peer| peer.acked_seq())
                .min()
                .unwrap_or(u64::MAX);
            log.keep_for_peers(self.node_id(), slowest);
        }
    }

    /// Hands `root`'s record to the peers and, in a synchronous mode, waits
    /// for them.
    ///
    /// The wait is bounded: the local transaction has already committed, so a
    /// peer that has gone away must not be able to hold a client's write open
    /// indefinitely. A commit that times out is reported in the statistics;
    /// the record is in the log, so the peer still gets it when it returns.
    pub async fn publish_prepared(&self, root: TxId) {
        let Some(seq) = self.release(root) else {
            return;
        };
        self.wait_for_peers(seq).await;
    }

    /// Waits until as many peers as the mode asks for have applied `seq`.
    async fn wait_for_peers(&self, seq: u64) {
        let config = self.config.load_full();
        let eligible = || -> Vec<Arc<PeerHandle>> {
            self.peers()
                .into_iter()
                .filter(|peer| !peer.is_paused())
                .collect()
        };
        let required = config.sync_mode.required_acks(eligible().len());
        if required == 0 {
            return;
        }
        let deadline = tokio::time::Instant::now() + config.ack_timeout;
        loop {
            // Registered before the count, so an acknowledgement landing
            // between the count and the wait still wakes this.
            let moved = self.acks.notified();
            tokio::pin!(moved);
            moved.as_mut().enable();
            let got = eligible()
                .iter()
                .filter(|peer| peer.acked_seq() >= seq)
                .count();
            if got >= required {
                return;
            }
            if tokio::time::timeout_at(deadline, moved).await.is_err() {
                self.stats.ack_timeouts.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Transaction {}:{} committed locally but only {} of {} peer(s)                      acknowledged within {:?}; they get it when they catch up",
                    self.node_id(),
                    seq,
                    got,
                    required,
                    config.ack_timeout
                );
                return;
            }
        }
    }

    /// A record that arrived from a peer was logged for the others: the
    /// senders look again. Only under `relay`.
    ///
    /// In the intended full mesh every node hears every commit first-hand,
    /// and relaying there would send each transaction once per peer for no
    /// gain.
    pub fn relay_logged(&self) {
        self.wake.notify_waiters();
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
    fn a_peer_ahead_of_the_sequence_moves_it_past_what_the_peer_holds() {
        let replication = configured();
        for tx in 1..=3 {
            replication.note_statement(tx, "INSERT INTO t (id) VALUES (1)", Some("t"));
            replication.commit_record(tx).unwrap();
        }
        // The peer applied up to 9 before this node lost its log's tail.
        assert_eq!(replication.adopt_peer_position(9), Some(3));
        replication.note_statement(4, "INSERT INTO t (id) VALUES (1)", Some("t"));
        assert_eq!(replication.commit_record(4).unwrap().seq, 10, "never a number the peer already holds");
        // A peer behind, or level, changes nothing.
        assert_eq!(replication.adopt_peer_position(4), None);
        assert_eq!(replication.adopt_peer_position(10), None);
        assert_eq!(replication.local_seq(), 10);
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


    /// A record is written before its transaction commits, and is not for
    /// sending until that commit is known.
    #[test]
    fn a_prepared_record_is_not_released_until_it_is_settled() {
        let replication = configured();
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        let prepared = replication
            .prepare_family(&[(7, true)], 7, true)
            .unwrap()
            .expect("a write was captured");
        assert_eq!(prepared.record.seq, 1);
        assert_eq!(replication.local_seq(), 1);
        assert_eq!(replication.released_through(), 0, "not committed yet");

        assert_eq!(replication.release(7), Some(1));
        assert_eq!(replication.released_through(), 1);
    }

    /// Sequence order is the order the records were written in, and a later
    /// one settled first does not overtake an earlier one still open.
    #[test]
    fn nothing_is_released_past_a_record_still_open() {
        let replication = configured();
        for tx in [7, 8, 9] {
            replication.note_statement(tx, "INSERT INTO t (id) VALUES (1)", Some("t"));
            replication.prepare_family(&[(tx, true)], tx, true).unwrap().unwrap();
        }
        replication.release(8);
        replication.release(9);
        assert_eq!(replication.released_through(), 0, "the first is still open");
        replication.release(7);
        assert_eq!(replication.released_through(), 3);
    }

    /// An aborted transaction's sequence is kept, as a record that carries
    /// nothing: the stream has no hole for a peer to wait on.
    #[test]
    fn a_record_whose_transaction_aborted_is_voided_in_place() {
        let replication = configured();
        replication.note_statement(7, "INSERT INTO t (id) VALUES (1)", Some("t"));
        replication.prepare_family(&[(7, true)], 7, true).unwrap().unwrap();
        replication.note_statement(8, "INSERT INTO t (id) VALUES (2)", Some("t"));
        replication.prepare_family(&[(8, true)], 8, true).unwrap().unwrap();

        replication.on_abort_family(&[7]);
        replication.release(8);
        assert_eq!(replication.released_through(), 2);

        let log = replication.log().unwrap();
        let (records, _) = log.read_after(1, 0, 10, 1 << 20).unwrap();
        assert_eq!(records.len(), 2, "both sequences are still there");
        assert!(records[0].is_void() && records[0].statements.is_empty());
        assert_eq!(records[1].statements.len(), 1);
    }

    #[test]
    fn only_a_plain_row_write_is_one() {
        assert!(plain_row_write("INSERT INTO t (id) VALUES (1)"));
        assert!(plain_row_write("  update t set a = 1"));
        assert!(plain_row_write("DELETE FROM t"));
        assert!(!plain_row_write("CREATE TABLE t (id INTEGER)"));
        assert!(!plain_row_write("INSERTED"));
        assert!(!plain_row_write(""));
    }
}
