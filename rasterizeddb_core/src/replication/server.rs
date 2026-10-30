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
pub fn bind(replication: &StdArc<Replication>) -> std::io::Result<rastcp::server::TcpServer> {
    bind_on(replication, None)
}

/// [`bind`], serving `listener` when one is given instead of binding
/// `listen_addr:listen_port`. The listener comes from
/// [`Replication::serve_on`], which says why a caller would hand one over.
pub fn bind_on(
    replication: &StdArc<Replication>,
    listener: Option<std::net::TcpListener>,
) -> std::io::Result<rastcp::server::TcpServer> {
    let config = replication.config();

    if let Err(e) = config.validate() {
        error!("Replication not started: {}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
    }

    let mut builder =
        rastcp::server::TcpServerBuilder::new(&config.listen_addr, config.listen_port)
            .request_timeout(config.request_timeout)
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
    info!(
        "Replication certificate fingerprint: {}",
        rastcp::tls::format_fingerprint(&server.certificate_fingerprint())
    );
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
pub async fn serve(
    database: Arc<Database>,
    server: rastcp::server::TcpServer,
) -> std::io::Result<()> {
    let replication = database.replication.clone();

    let shutdown = server.shutdown_handle();
    crate::shutdown::on_termination(move || shutdown.shutdown());

    // Neither file is flushed per commit — see `super::log` — so something has
    // to close the window between a write and its durability.
    let flusher = replication.clone();
    let flush_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // An fsync is blocking I/O; run on a runtime worker it parks that
            // worker and every task queued behind it for the whole sync.
            let flusher = flusher.clone();
            let _ = tokio::task::spawn_blocking(move || flusher.flush()).await;
        }
    });

    let outcome = server
        .run_with_context(database, handle_frame)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()));

    flush_task.abort();

    // Everything the log and the state buffered has to reach disk before the
    // process leaves, or a peer's catch-up point goes back in time.
    replication.flush();
    info!("Replication listener stopped");
    outcome
}

/// Answers one frame.
async fn handle_frame(database: Arc<Database>, request: Vec<u8>) -> Vec<u8> {
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
        FrameKind::Hello => handle_hello(&replication, &payload),
        FrameKind::Push => handle_push(&database, &replication, &payload).await,
        FrameKind::Pull => handle_pull(&replication, &payload),
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
    payload: &[u8],
) -> Vec<u8> {
    let (sender, records) = match protocol::decode_push(&mut Reader::new(payload)) {
        Ok(decoded) => decoded,
        Err(e) => return protocol::error_frame(&format!("unreadable PUSH: {}", e)),
    };

    let mut outcomes = Vec::with_capacity(records.len());
    for record in records {
        let outcome = apply_record(database, replication, record, sender).await;
        outcomes.push(outcome);
    }

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
fn handle_pull(replication: &StdArc<Replication>, payload: &[u8]) -> Vec<u8> {
    let request = match Pull::decode(&mut Reader::new(payload)) {
        Ok(request) => request,
        Err(e) => return protocol::error_frame(&format!("unreadable PULL: {}", e)),
    };

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

    let (records, more) = match log.read_after(
        request.origin,
        request.after_seq,
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

/// Applies one record, or explains why it was not applied.
async fn apply_record(
    database: &Arc<Database>,
    replication: &StdArc<Replication>,
    record: TxnRecord,
    // Which peer handed this over, so relaying does not send it straight back.
    // `NO_SENDER` when it did not come off a link — a `RESYNC` pull, say.
    from_node: NodeId,
) -> ApplyOutcome {
    let applied = replication.applied();
    let node_id = replication.node_id();

    // Our own transaction, arriving back from a relay. Nothing to do.
    if record.origin == node_id {
        return refuse(&record, ApplyStatus::Duplicate, "");
    }

    if applied.already_applied(record.origin, record.seq) {
        return refuse(&record, ApplyStatus::Duplicate, "");
    }

    let high_water = applied.high_water(record.origin);
    if high_water > 0 && record.seq > high_water + 1 {
        // A record cannot be applied over a hole: the statements before it may
        // be what make it valid. The sender backfills on seeing this.
        return refuse(
            &record,
            ApplyStatus::Gap,
            format!("expected sequence {}, got {}", high_water + 1, record.seq),
        );
    }
    if high_water == 0 && record.seq > 1 {
        // Nothing has ever been applied from this origin and its stream does
        // not start at the beginning: its early history is beyond retention.
        // Adopting the stream here is the only way forward, and saying so is
        // the only honest thing to do about what came before.
        warn!(
            "Adopting origin {}'s stream at sequence {}: this node holds nothing earlier, \
             so transactions before it are not present. Seed this node from a backup of \
             that origin if the earlier history matters.",
            record.origin, record.seq
        );
    }

    replication.clock().observe(record.commit_hlc);

    let policy = replication.conflict_policy();
    if policy == ConflictPolicy::LastWriterWins && replication.is_superseded(&record) {
        // Every table it writes has a strictly later local write, so applying
        // it would move those rows backwards in time.
        if let Err(e) = applied.record(record.origin, record.seq) {
            warn!("Could not record the applied position: {}", e);
        }
        return refuse(
            &record,
            ApplyStatus::Conflict,
            "superseded by a later local write",
        );
    }

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
        // finding L2). Released after the last durability step below, or on
        // any way out.
        let owner = crate::core::mvcc::owner::OwnerRelease::for_root(&database.row_locks, tx);

        let mut failure: Option<String> = None;
        let mut transient = false;
        for statement in &record.statements {
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
            return refuse(&record, ApplyStatus::Failed, message);
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
        return refuse(&record, ApplyStatus::Conflict, message);
    };

    // Through the same path as a client's commit: rows applied to a table on
    // another storage location are in that location's log, and they have to
    // be durable before the outcome exists for any root flush to carry to
    // disk. The bare commit left them to the log writer, and a power cut in
    // between kept this transaction's root half and lost the rest.
    if let Err(e) = database.commit_written_transaction(tx) {
        database.abort_transaction(tx);
        if let Err(e) = database.transactions.flush() {
            warn!("Could not make the aborted outcome durable: {}", e);
        }
        owner.release();
        // Not recorded as applied, so the record comes again and is applied
        // then, instead of being skipped for good.
        return refuse(
            &record,
            ApplyStatus::Failed,
            format!("the applied rows could not be made durable: {}", e),
        );
    }
    if let Err(e) = database.transactions.flush() {
        // Back to aborted, which is what a restart would conclude anyway, and
        // not recorded as applied: recording it would skip a transaction this
        // node may not have, which is permanent divergence.
        database.abort_transaction(tx);
        if let Err(e) = database.transactions.flush() {
            warn!("Could not make the aborted outcome durable: {}", e);
        }
        owner.release();
        return refuse(
            &record,
            ApplyStatus::Failed,
            format!("the transaction outcome could not be made durable: {}", e),
        );
    }
    // Durable: the apply's table locks go now (finding L4).
    owner.release();

    // Recorded after the commit is durable: a crash in the window between them
    // replays this transaction rather than losing it. See `super::log`.
    if let Err(e) = applied.record(record.origin, record.seq) {
        warn!("Could not record the applied position: {}", e);
    }
    replication.stamp_tables(&record.tables, record.commit_hlc, record.origin);

    debug!(
        "Applied transaction {}:{} ({} statements)",
        record.origin,
        record.seq,
        record.statements.len()
    );

    // A record that arrived here may still have to reach nodes this one can
    // see and its sender cannot.
    let origin = record.origin;
    let seq = record.seq;

    if replication.config().relay {
        // Marked before it is either logged or forwarded, so what a third node
        // receives and what this node's log holds are the same record — and a
        // reader can tell a transaction that passed through here from one that
        // started here.
        let mut relayed = record;
        relayed.flags |= protocol::FLAG_RELAYED;
        let relayed = StdArc::new(relayed);

        if let Some(log) = replication.log() {
            if let Err(e) = log.append(&relayed) {
                warn!("Could not log a relayed transaction: {}", e);
            }
        }
        replication.relay(relayed, from_node);
    }

    ApplyOutcome {
        origin,
        seq,
        status: ApplyStatus::Applied,
        message: String::new(),
    }
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
    for record in records {
        let outcome = apply_record(database, replication, record, NO_SENDER).await;
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

    /// A node joining a cluster whose early history is gone adopts the stream
    /// where it finds it, rather than stalling forever on a gap it cannot close.
    #[tokio::test]
    async fn a_fresh_origin_is_adopted_wherever_its_stream_starts() {
        let database = database().await;
        create_table(&database).await;
        let replication = database.replication.clone();

        let outcome = apply_record(
            &database,
            &replication,
            record(
                2,
                900,
                vec!["INSERT INTO repl_t (id, name) VALUES (9, 'nine')"],
            ),
            NO_SENDER,
        )
        .await;

        assert_eq!(outcome.status, ApplyStatus::Applied);
        assert_eq!(replication.applied().high_water(2), 900);
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
        let response = handle_frame(database, vec![0xde, 0xad, 0xbe, 0xef]).await;
        assert_eq!(
            protocol::parse_header(&response).unwrap().kind,
            FrameKind::Error
        );
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
