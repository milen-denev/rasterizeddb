//! Multi-primary replication between RasterizedDB instances.
//!
//! Every node in a cluster accepts reads and writes. A transaction committed on
//! one is shipped to the others and applied there, so the cluster converges
//! without any node being designated the writer and without a failover step
//! when one goes away.
//!
//! ```text
//!            ┌──────────┐   commits, batched + compressed   ┌──────────┐
//!            │  node 1  │ ───────────────────────────────▶  │  node 2  │
//!            │ (primary)│ ◀───────────────────────────────  │ (primary)│
//!            └──────────┘                                   └──────────┘
//!                  ▲   ╲                                     ╱   ▲
//!                  │    ╲            ┌──────────┐           ╱    │
//!               clients   ────────▶  │  node 3  │  ◀───────      clients
//!                                    │ (primary)│
//!                                    └──────────┘
//! ```
//!
//! # What travels
//!
//! A committed transaction, as the statements that made it up, stamped with the
//! node that took it, that node's sequence number, and a hybrid logical clock
//! reading. See [`protocol::TxnRecord`] for why statements rather than pages.
//!
//! # The pieces
//!
//! | Module | What it owns |
//! |---|---|
//! | [`config`] | every knob, and the text it is spelled with |
//! | [`hlc`] | the clock that orders commits across nodes |
//! | [`protocol`] | frames, records, and the compression codec |
//! | [`log`] | history for peers that fall behind, and how far this node has applied |
//! | [`manager`] | node state, the capture hooks, the commit path |
//! | [`peer`] | one outbound link: batching, compression, reconnect, catch-up |
//! | [`server`] | the inbound listener and the apply path |
//! | [`sql`] | `REPLICATION ...` and `SHOW REPLICATION ...` |
//!
//! # What it guarantees, and what it does not
//!
//! **Delivery is at-least-once and ordering is per origin.** Each node's own
//! stream is applied in the order that node committed it. Two nodes' streams
//! are ordered against each other only by their hybrid timestamps, which is
//! enough to order causally related commits and not enough to serialise
//! concurrent ones — that is what multi-primary means.
//!
//! **Convergence is eventual, and conflicts are a real case.** Two nodes
//! writing the same key at the same time is what the topology permits, so one
//! of the two arrivals will not apply cleanly. What happens then is
//! [`config::ConflictPolicy`], and the default is to stop that origin's stream
//! and say so rather than to diverge quietly.
//!
//! **Statements must be deterministic.** `NOW()`, `RANDOM()` and friends are
//! re-evaluated on each node and produce different rows there.
//! [`manager::Replication::note_statement`] warns when it sees one. Values the
//! engine generates internally are not affected: a JSON document's UUID, for
//! instance, is generated once and replicated as a literal.
//!
//! **This is not a consensus protocol.** There is no leader election, no
//! quorum read, and no split-brain protection. Two partitioned halves both
//! keep accepting writes, and the conflict policy decides what happens when
//! they meet again. If you need a single serialisable history, this is the
//! wrong shape.
//!
//! # Getting a cluster up
//!
//! Every node needs a unique `--node-id` and the addresses of the others:
//!
//! ```text
//! # node 1
//! rasterizeddb_core --location /data --replication --node-id 1 \
//!     --peer 'two@10.0.0.2:61171' --peer 'three@10.0.0.3:61171'
//!
//! # node 2
//! rasterizeddb_core --location /data --replication --node-id 2 \
//!     --peer 'one@10.0.0.1:61171' --peer 'three@10.0.0.3:61171'
//! ```
//!
//! or, at runtime, `REPLICATION ADD PEER 'two' ADDRESS '10.0.0.2:61171'`
//! followed by `REPLICATION ENABLE`. See [`sql`] for the full surface.
//!
//! A node joining a cluster that already holds data needs a copy of that data
//! first — replication ships what happens from now on, not what happened
//! before. Seed it from a backup, then start it.

pub mod config;
pub mod hlc;
pub mod log;
pub mod manager;
pub mod peer;
pub mod protocol;
pub mod server;
pub mod sql;

pub use config::{
    AuthMode, Codec, Compression, CompressionMode, ConflictPolicy, NodeId, PeerConfig,
    ReplicationConfig, SyncMode, TlsMaterial,
};
pub use hlc::{Hlc, HybridClock};
pub use manager::{Replication, Stats};
pub use peer::PeerSnapshot;
pub use protocol::TxnRecord;

#[cfg(test)]
pub(crate) mod test_support {
    //! Fixtures shared by the replication tests.

    use rclite::Arc;

    use crate::core::database::Database;

    /// Sets the globals `main` sets at start-up.
    ///
    /// They are `OnceLock`s sized from the command line, and the query
    /// processor reads them unconditionally — so a `Database` built in a test
    /// without them panics on the first scan, several layers below whatever the
    /// test was actually about.
    pub fn init_globals() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 4);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024);
    }

    /// An in-memory database, with those globals in place.
    pub async fn in_memory_database() -> Arc<Database> {
        init_globals();
        Arc::new(Database::new_with_backend("", true).await)
    }

    /// A loopback listener on a port of the kernel's choosing, and that port.
    ///
    /// Hand the listener itself to the node — `Replication::serve_on` — rather
    /// than the number to bind again. A port that is let go is free for anyone:
    /// the kernel happily hands the same ephemeral port to two callers moments
    /// apart, so tests that each released two or three ports in parallel
    /// collided, failing with "could not bind the replication listener …
    /// Address already in use", or worse, with a peer reaching another test's
    /// node. An earlier fix, a process-wide counter with a probe bind, only
    /// narrowed the window; another process could still take the port between
    /// the probe and the real bind. Holding the socket is the only way to have
    /// no window at all.
    pub fn listener() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let port = listener.local_addr().expect("a bound address").port();
        (listener, port)
    }
}

#[cfg(test)]
mod tests;
