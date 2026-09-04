//! `REPLICATION ...` and `SHOW REPLICATION ...` — running the cluster from SQL.
//!
//! ```text
//! REPLICATION ENABLE
//! REPLICATION DISABLE
//!
//! REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2:61171'
//! REPLICATION ADD PEER 'tokyo'  ADDRESS '10.0.1.7:61171' WITH (COMPRESSION = 'zstd:3')
//! REPLICATION DROP PEER 'berlin'
//! REPLICATION PAUSE PEER 'berlin'
//! REPLICATION RESUME PEER 'berlin'
//! REPLICATION RESYNC PEER 'berlin' [FROM <sequence>]
//!
//! REPLICATION SET compression = 'auto:zstd:1'
//! REPLICATION SET mode = 'quorum:1'
//! REPLICATION SET conflict = 'last-writer-wins'
//! REPLICATION SET batch_linger_us = 500
//!
//! SHOW REPLICATION            -- one line of summary plus a line per peer
//! SHOW REPLICATION STATUS     -- counters
//! SHOW REPLICATION PEERS
//! SHOW REPLICATION SETTINGS
//! SHOW REPLICATION KEYS       -- what SET accepts
//! ```
//!
//! Everything here is also a CLI flag, and the two write the same state: a flag
//! is the value the node starts with and a statement changes it afterwards.
//! Changes are not persisted — a peer added at runtime is gone after a restart
//! unless the flag is added too — which is deliberate, so that what a node
//! comes up as is always what its command line says.

use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;

use rclite::Arc;

use crate::core::database::Database;

use super::config::{
    Codec, Compression, ConflictPolicy, NodeId, PeerConfig, ReplicationConfig, SyncMode,
};
use super::hlc;
use super::manager::Replication;
use super::peer;

/// One tunable, for `SHOW REPLICATION KEYS`.
pub struct Key {
    pub name: &'static str,
    pub help: &'static str,
}

pub const KEYS: &[Key] = &[
    Key {
        name: "node_id",
        help: "this node's id, unique in the cluster",
    },
    Key {
        name: "cluster",
        help: "cluster name; peers must agree on it",
    },
    Key {
        name: "token",
        help: "shared secret presented in the handshake",
    },
    Key {
        name: "compression",
        help: "none | zstd[:level] | auto[:zstd[:level]]",
    },
    Key {
        name: "mode",
        help: "async | sync | quorum:N | all",
    },
    Key {
        name: "conflict",
        help: "abort | skip | last-writer-wins",
    },
    Key {
        name: "ack_timeout_ms",
        help: "how long a synchronous commit waits for peers",
    },
    Key {
        name: "batch_max_records",
        help: "records in one outbound batch",
    },
    Key {
        name: "batch_max_bytes",
        help: "uncompressed bytes in one outbound batch",
    },
    Key {
        name: "batch_linger_us",
        help: "how long a sender waits for more records",
    },
    Key {
        name: "queue_capacity",
        help: "records a peer queue holds before dropping to the log",
    },
    Key {
        name: "relay",
        help: "forward transactions received from one peer to the others",
    },
    Key {
        name: "log_retention_mb",
        help: "history kept for peers that fall behind",
    },
    Key {
        name: "connect_timeout_ms",
        help: "dial timeout for a peer",
    },
    Key {
        name: "request_timeout_ms",
        help: "per-request timeout on a peer link",
    },
    Key {
        name: "reconnect_base_ms",
        help: "first reconnect delay; doubles up to the maximum",
    },
    Key {
        name: "reconnect_max_ms",
        help: "longest reconnect delay",
    },
];

/// Whether this statement belongs to the replication surface.
///
/// Checked before the DDL and DML prefixes, so `REPLICATION` and
/// `SHOW REPLICATION` never reach a parser that would read them as something
/// else.
pub fn is_replication_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let upper = upper.trim_end_matches(';').trim_end();

    upper == "REPLICATION"
        || upper.starts_with("REPLICATION ")
        || upper == "SHOW REPLICATION"
        || upper.starts_with("SHOW REPLICATION ")
}

/// Strips one layer of `'` or `"` quoting.
fn unquote(value: &str) -> &str {
    let value = value.trim();
    for quote in ['\'', '"'] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

/// Splits a statement into words, keeping a quoted run together.
///
/// Names and addresses are quoted and may contain spaces; splitting on
/// whitespace alone would cut `'my peer'` in half.
fn words(statement: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in statement.chars() {
        match quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    quote = None;
                    out.push(std::mem::take(&mut current));
                }
            }
            None if ch == '\'' || ch == '"' => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                quote = Some(ch);
                current.push(ch);
            }
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Reads the `WITH ( key = value, ... )` tail, if there is one.
fn with_options(statement: &str) -> Result<Vec<(String, String)>, String> {
    let upper = statement.to_ascii_uppercase();
    let Some(at) = upper.find(" WITH ") else {
        return Ok(Vec::new());
    };
    let tail = statement[at + " WITH ".len()..].trim();
    let inner = tail
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| "WITH takes a parenthesised list: WITH (key = value)".to_string())?;

    let mut options = Vec::new();
    for entry in inner.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| format!("`{}` is not `key = value`", entry))?;
        options.push((key.trim().to_ascii_lowercase(), unquote(value).to_string()));
    }
    Ok(options)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" | "enabled" => Ok(true),
        "off" | "false" | "no" | "0" | "disabled" => Ok(false),
        other => Err(format!("`{}` is not a boolean", other)),
    }
}

fn parse_number<T: std::str::FromStr>(value: &str, key: &str) -> Result<T, String> {
    value
        .trim()
        .parse()
        .map_err(|_| format!("`{}` is not a valid value for {}", value.trim(), key))
}

/// Runs one statement. Returns the lines to show, or the reason it failed.
pub async fn execute(sql: &str, database: &Arc<Database>) -> Result<Vec<String>, String> {
    let statement = sql.trim().trim_end_matches(';').trim();
    let upper = statement.to_ascii_uppercase();
    let replication = database.replication.clone();

    if upper == "SHOW REPLICATION" || upper.starts_with("SHOW REPLICATION ") {
        let what = upper["SHOW REPLICATION".len()..].trim();
        return match what {
            "" | "STATUS" => Ok(show_status(&replication)),
            "PEERS" => Ok(show_peers(&replication)),
            "SETTINGS" | "CONFIG" => Ok(show_settings(&replication)),
            "KEYS" => Ok(KEYS
                .iter()
                .map(|key| format!("{:<20} {}", key.name, key.help))
                .collect()),
            other => Err(format!(
                "unknown SHOW REPLICATION form `{}`: try STATUS, PEERS, SETTINGS, KEYS, \
                 STATS or PEER STATUS",
                other
            )),
        };
    }

    let rest = upper
        .strip_prefix("REPLICATION")
        .ok_or_else(|| format!("not a replication statement: `{}`", statement))?
        .trim();

    if rest == "ENABLE" || rest == "START" {
        return enable(database).await;
    }
    if rest == "DISABLE" || rest == "STOP" {
        replication.set_active(false);
        for peer in replication.peers() {
            peer.stop();
            replication.remove_peer(&peer.name);
        }
        return Ok(vec!["replication disabled".to_string()]);
    }
    if rest == "FLUSH" {
        replication.flush();
        return Ok(vec!["replication log flushed".to_string()]);
    }

    let tokens = words(statement);
    let keyword = tokens
        .get(1)
        .map(|t| t.to_ascii_uppercase())
        .unwrap_or_default();

    match keyword.as_str() {
        "ADD" => add_peer(database, statement, &tokens).await,
        "DROP" | "REMOVE" => drop_peer(&replication, &tokens),
        "PAUSE" => set_peer_paused(&replication, &tokens, true),
        "RESUME" => set_peer_paused(&replication, &tokens, false),
        "RESYNC" => resync(database, &tokens).await,
        "SET" => set(database, statement, &tokens).await,
        _ => Err(format!(
            "unknown replication statement `{}`: try ENABLE, DISABLE, ADD PEER, DROP PEER, \
             PAUSE PEER, RESUME PEER, RESYNC PEER or SET",
            statement
        )),
    }
}

/// Brings the node up: opens storage, starts the peers, starts the listener.
async fn enable(database: &Arc<Database>) -> Result<Vec<String>, String> {
    let replication = database.replication.clone();
    let config = replication.config();
    config.validate()?;

    if replication.is_active() {
        return Ok(vec!["replication is already enabled".to_string()]);
    }

    Database::start_replication(database.clone()).map_err(|e| e.to_string())?;

    let mut lines = vec![format!(
        "replication enabled as node {} of cluster '{}', listening on {}:{}",
        config.node_id, config.cluster, config.listen_addr, config.listen_port
    )];
    for peer_config in &config.peers {
        lines.push(format!(
            "peer {} started at {}",
            peer_config.name, peer_config.address
        ));
    }

    Ok(lines)
}

async fn add_peer(
    database: &Arc<Database>,
    statement: &str,
    tokens: &[String],
) -> Result<Vec<String>, String> {
    // REPLICATION ADD PEER '<name>' ADDRESS '<host:port>' [WITH (...)]
    if tokens.get(2).map(|t| t.to_ascii_uppercase()).as_deref() != Some("PEER") {
        return Err("expected REPLICATION ADD PEER '<name>' ADDRESS '<host:port>'".to_string());
    }
    let name = unquote(
        tokens
            .get(3)
            .ok_or_else(|| "REPLICATION ADD PEER needs a name".to_string())?,
    )
    .to_string();
    if name.is_empty() {
        return Err("a peer needs a name".to_string());
    }

    let address = match tokens.get(4).map(|t| t.to_ascii_uppercase()).as_deref() {
        Some("ADDRESS") | Some("AT") => unquote(
            tokens
                .get(5)
                .ok_or_else(|| "ADDRESS needs a host:port".to_string())?,
        )
        .to_string(),
        _ => return Err("expected ADDRESS '<host:port>' after the peer name".to_string()),
    };

    let mut peer_config = PeerConfig::new(name.clone(), address.clone());
    // Validates the address shape the same way the CLI flag does.
    PeerConfig::parse_flag(&format!("{}@{}", name, address))?;

    for (key, value) in with_options(statement)? {
        match key.as_str() {
            "compression" => {
                peer_config.compression = Some(
                    Compression::parse(&value)
                        .ok_or_else(|| format!("`{}` is not a compression setting", value))?,
                );
            }
            "paused" => peer_config.paused = parse_bool(&value)?,
            other => return Err(format!("unknown peer option `{}`", other)),
        }
    }

    let replication = database.replication.clone();
    if replication.peer(&name).is_some() {
        return Err(format!("peer '{}' already exists", name));
    }

    let added = peer_config.clone();
    replication.update_config(move |config| {
        if config.peers.iter().any(|p| p.name == added.name) {
            return Err(format!("peer '{}' already exists", added.name));
        }
        config.peers.push(added.clone());
        Ok(())
    })?;

    // Only started once the node itself is up; otherwise the peer is recorded
    // and starts with everything else on `REPLICATION ENABLE`.
    if replication.is_active() {
        replication.insert_peer(peer::spawn(replication.clone(), &peer_config));
        Ok(vec![format!(
            "peer {} added at {} and started",
            name, address
        )])
    } else {
        Ok(vec![format!(
            "peer {} added at {}; it starts with REPLICATION ENABLE",
            name, address
        )])
    }
}

fn peer_name(tokens: &[String]) -> Result<String, String> {
    // `<verb> PEER '<name>'`, with PEER optional.
    let at = match tokens.get(2).map(|t| t.to_ascii_uppercase()).as_deref() {
        Some("PEER") => 3,
        _ => 2,
    };
    let name = unquote(
        tokens
            .get(at)
            .ok_or_else(|| "expected a peer name".to_string())?,
    );
    if name.is_empty() {
        return Err("expected a peer name".to_string());
    }
    Ok(name.to_string())
}

fn drop_peer(replication: &StdArc<Replication>, tokens: &[String]) -> Result<Vec<String>, String> {
    let name = peer_name(tokens)?;

    let removed = replication.remove_peer(&name);
    if let Some(peer) = removed.as_ref() {
        peer.stop();
    }

    let dropped = name.clone();
    replication.update_config(move |config| {
        config.peers.retain(|p| p.name != dropped);
        Ok(())
    })?;

    if removed.is_none() {
        return Ok(vec![format!(
            "peer {} was not running; removed from the configuration",
            name
        )]);
    }
    Ok(vec![format!("peer {} dropped", name)])
}

fn set_peer_paused(
    replication: &StdArc<Replication>,
    tokens: &[String],
    paused: bool,
) -> Result<Vec<String>, String> {
    let name = peer_name(tokens)?;

    let peer = replication
        .peer(&name)
        .ok_or_else(|| format!("no peer named '{}'", name))?;
    peer.set_paused(paused);

    let target = name.clone();
    replication.update_config(move |config| {
        if let Some(entry) = config.peers.iter_mut().find(|p| p.name == target) {
            entry.paused = paused;
        }
        Ok(())
    })?;

    Ok(vec![format!(
        "peer {} {}",
        name,
        if paused { "paused" } else { "resumed" }
    )])
}

/// Takes an origin's stream again from a given point.
///
/// Two things happen: the applied position is rewound, so records that were
/// already taken are no longer recognised as duplicates, and the peer is asked
/// for that history directly. The second part is what makes it useful after an
/// apply has stalled — the peer will not re-push what it thinks we have.
async fn resync(database: &Arc<Database>, tokens: &[String]) -> Result<Vec<String>, String> {
    let replication = database.replication.clone();
    let name = peer_name(tokens)?;

    let peer = replication
        .peer(&name)
        .ok_or_else(|| format!("no peer named '{}'", name))?;

    let from = match tokens.iter().position(|t| t.eq_ignore_ascii_case("FROM")) {
        Some(at) => parse_number::<u64>(
            tokens
                .get(at + 1)
                .ok_or_else(|| "FROM needs a sequence number".to_string())?,
            "FROM",
        )?,
        None => 0,
    };

    let snapshot = peer.snapshot(replication.local_seq());
    let origin: NodeId = snapshot.remote_node;
    if origin == 0 {
        return Err(format!(
            "peer '{}' has not completed a handshake yet, so its node id is unknown",
            name
        ));
    }

    let applied = replication.applied();
    let before = applied.high_water(origin);
    if from < before {
        applied
            .reset(origin)
            .map_err(|e| format!("could not rewind the applied position: {}", e))?;
        if from > 0 {
            applied
                .record(origin, from)
                .map_err(|e| format!("could not set the applied position: {}", e))?;
        }
    }

    // The peer answers a pull with one batch and flags whether it has more, so
    // a single round trip resynchronises only the first batch. Loop until it
    // says it is done, or until nothing moves.
    //
    // A resync of a long backlog is bounded by the peer's own retention rather
    // than by anything here, but the round counter is a backstop: a peer that
    // kept flagging `more` while returning nothing would otherwise spin.
    const MAX_ROUNDS: usize = 100_000;

    let mut cursor = from;
    let mut pulled = 0usize;
    let mut applied_count = 0usize;
    let mut refused = 0usize;
    let mut rounds = 0usize;

    loop {
        let (records, more) = peer::pull_from(&replication, &peer, origin, cursor).await?;
        if records.is_empty() {
            break;
        }

        cursor = records.last().map(|r| r.seq).unwrap_or(cursor);
        pulled += records.len();

        let (ok, bad) = super::server::apply_pulled(database, &replication, records).await;
        applied_count += ok;
        refused += bad;

        rounds += 1;
        if !more || rounds >= MAX_ROUNDS {
            if rounds >= MAX_ROUNDS {
                return Err(format!(
                    "resynchronising origin {} did not finish after {} rounds;                      stopped at sequence {}",
                    origin, rounds, cursor
                ));
            }
            break;
        }
    }

    Ok(vec![format!(
        "resynchronised origin {} from sequence {} to {} (was at {}):          {} pulled, {} applied, {} refused",
        origin, from, cursor, before, pulled, applied_count, refused
    )])
}

async fn set(
    database: &Arc<Database>,
    statement: &str,
    tokens: &[String],
) -> Result<Vec<String>, String> {
    let replication = database.replication.clone();

    let assignment = statement
        .trim()
        .get("REPLICATION".len()..)
        .unwrap_or("")
        .trim_start();
    let assignment = assignment
        .get("SET".len()..)
        .ok_or_else(|| "REPLICATION SET needs `key = value`".to_string())?
        .trim();

    let (key, value) = assignment
        .split_once('=')
        .ok_or_else(|| "REPLICATION SET needs `key = value`".to_string())?;
    let key = key.trim().to_ascii_lowercase();
    let value = unquote(value).to_string();
    let _ = tokens;

    // `node_id` moves the node's identity, which changes what its own records
    // are stamped with. Doing that with peers connected would make its stream
    // ambiguous, so it is only allowed while stopped.
    if key == "node_id" && replication.is_active() {
        return Err(
            "node_id cannot change while replication is running; REPLICATION DISABLE first"
                .to_string(),
        );
    }

    let edited = {
        let value = value.clone();
        let key = key.clone();
        replication.update_config(move |config| apply_setting(config, &key, &value))?
    };

    // A few settings are held by the running peers as well as the config.
    if key == "compression" {
        for peer in replication.peers() {
            if peer.compression_override().is_none() {
                peer.set_compression(None);
            }
        }
    }

    Ok(vec![format!(
        "{} = {}",
        key,
        describe_setting(&edited, &key)
    )])
}

/// Writes one setting into a configuration.
fn apply_setting(config: &mut ReplicationConfig, key: &str, value: &str) -> Result<(), String> {
    use std::time::Duration;

    match key {
        "node_id" => config.node_id = parse_number(value, key)?,
        "cluster" => config.cluster = value.to_string(),
        "token" => config.token = value.to_string(),
        "compression" => {
            config.compression = Compression::parse(value)
                .ok_or_else(|| format!("`{}` is not a compression setting", value))?
        }
        "mode" | "sync_mode" => {
            config.sync_mode =
                SyncMode::parse(value).ok_or_else(|| format!("`{}` is not a sync mode", value))?
        }
        "conflict" | "conflict_policy" => {
            config.conflict = ConflictPolicy::parse(value)
                .ok_or_else(|| format!("`{}` is not a conflict policy", value))?
        }
        "ack_timeout_ms" => config.ack_timeout = Duration::from_millis(parse_number(value, key)?),
        "batch_max_records" => config.batch_max_records = parse_number(value, key)?,
        "batch_max_bytes" => config.batch_max_bytes = parse_number(value, key)?,
        "batch_linger_us" => config.batch_linger = Duration::from_micros(parse_number(value, key)?),
        "queue_capacity" => config.queue_capacity = parse_number(value, key)?,
        "relay" => config.relay = parse_bool(value)?,
        "log_retention_mb" => {
            config.log_retention_bytes = parse_number::<u64>(value, key)? * 1024 * 1024
        }
        "connect_timeout_ms" => {
            config.connect_timeout = Duration::from_millis(parse_number(value, key)?)
        }
        "request_timeout_ms" => {
            config.request_timeout = Duration::from_millis(parse_number(value, key)?)
        }
        "reconnect_base_ms" => {
            config.reconnect_base = Duration::from_millis(parse_number(value, key)?)
        }
        "reconnect_max_ms" => {
            config.reconnect_max = Duration::from_millis(parse_number(value, key)?)
        }
        other => {
            return Err(format!(
                "unknown replication setting `{}`; SHOW REPLICATION KEYS lists them",
                other
            ));
        }
    }
    Ok(())
}

/// Renders one setting's current value.
fn describe_setting(config: &ReplicationConfig, key: &str) -> String {
    match key {
        "node_id" => config.node_id.to_string(),
        "cluster" => config.cluster.clone(),
        // Never echoed: it is a shared secret, and a status view is exactly
        // where one gets copied into a bug report.
        "token" => {
            if config.token.is_empty() {
                "(unset)".to_string()
            } else {
                "(set)".to_string()
            }
        }
        "compression" => config.compression.to_string(),
        "mode" | "sync_mode" => config.sync_mode.to_string(),
        "conflict" | "conflict_policy" => config.conflict.to_string(),
        "ack_timeout_ms" => config.ack_timeout.as_millis().to_string(),
        "batch_max_records" => config.batch_max_records.to_string(),
        "batch_max_bytes" => config.batch_max_bytes.to_string(),
        "batch_linger_us" => config.batch_linger.as_micros().to_string(),
        "queue_capacity" => config.queue_capacity.to_string(),
        "relay" => config.relay.to_string(),
        "log_retention_mb" => (config.log_retention_bytes / (1024 * 1024)).to_string(),
        "connect_timeout_ms" => config.connect_timeout.as_millis().to_string(),
        "request_timeout_ms" => config.request_timeout.as_millis().to_string(),
        "reconnect_base_ms" => config.reconnect_base.as_millis().to_string(),
        "reconnect_max_ms" => config.reconnect_max.as_millis().to_string(),
        _ => String::new(),
    }
}

fn show_settings(replication: &StdArc<Replication>) -> Vec<String> {
    let config = replication.config();
    KEYS.iter()
        .map(|key| format!("{:<20} {}", key.name, describe_setting(&config, key.name)))
        .collect()
}

fn show_peers(replication: &StdArc<Replication>) -> Vec<String> {
    let local_seq = replication.local_seq();
    let peers = replication.peers();

    if peers.is_empty() {
        return vec!["no peers configured".to_string()];
    }

    let mut lines = vec![format!(
        "{:<16} {:<24} {:<6} {:<10} {:<8} {:<8} {:<8} {:<8}",
        "peer", "address", "node", "state", "acked", "lag", "queued", "codec"
    )];

    for peer in peers {
        let snapshot = peer.snapshot(local_seq);
        let state = if snapshot.paused {
            "paused"
        } else if snapshot.connected {
            "up"
        } else {
            "down"
        };
        lines.push(format!(
            "{:<16} {:<24} {:<6} {:<10} {:<8} {:<8} {:<8} {:<8}",
            snapshot.name,
            snapshot.address,
            snapshot.remote_node,
            state,
            snapshot.acked_seq,
            snapshot.lag,
            snapshot.queued,
            snapshot.codec.to_string(),
        ));
        if let Some(error) = snapshot.last_error.as_ref() {
            lines.push(format!("{:<16} last error: {}", "", error));
        }
    }

    lines
}

fn show_status(replication: &StdArc<Replication>) -> Vec<String> {
    let config = replication.config();
    let stats = replication.stats();

    let mut lines = vec![
        format!(
            "node {} of cluster '{}' — {}",
            config.node_id,
            config.cluster,
            if replication.is_active() {
                "enabled"
            } else {
                "disabled"
            }
        ),
        format!(
            "listening on {}:{}, mode {}, conflict {}, compression {}",
            config.listen_addr,
            config.listen_port,
            config.sync_mode,
            config.conflict,
            config.compression
        ),
        format!(
            "local sequence {}, clock {}, {} peer(s), {} open capture(s)",
            replication.local_seq(),
            hlc::format(replication.clock().read()),
            replication.peer_count(),
            replication.open_captures()
        ),
        format!(
            "captured {}, published {}, applied {}, duplicates {}",
            stats.captured.load(Ordering::Relaxed),
            stats.published.load(Ordering::Relaxed),
            stats.applied.load(Ordering::Relaxed),
            stats.duplicates.load(Ordering::Relaxed)
        ),
        format!(
            "conflicts {}, failures {}, gaps {}, ack timeouts {}",
            stats.conflicts.load(Ordering::Relaxed),
            stats.failures.load(Ordering::Relaxed),
            stats.gaps.load(Ordering::Relaxed),
            stats.ack_timeouts.load(Ordering::Relaxed)
        ),
    ];

    let raw = stats.bytes_raw.load(Ordering::Relaxed);
    let wire = stats.bytes_wire.load(Ordering::Relaxed);
    let batches = stats.batches_sent.load(Ordering::Relaxed);
    let compressed = stats.compressed_batches.load(Ordering::Relaxed);
    lines.push(match stats.compression_saving() {
        // Phrased by what happened rather than as a signed saving: with small
        // batches nothing clears the compression floor, and every frame still
        // costs its header — reporting that as "-8% saved" reads as a fault
        // when it is the size floor working as intended.
        Some(saving) if compressed > 0 => format!(
            "{} batch(es), {} compressed: {} bytes of payload went out as {} ({:.1}% smaller)",
            batches, compressed, raw, wire, saving
        ),
        Some(_) => format!(
            "{} batch(es), none compressed: {} bytes of payload went out as {} \
             (payloads below the {} byte floor, or a codec neither end had)",
            batches, raw, wire, config.compression.min_bytes
        ),
        None => "nothing sent yet".to_string(),
    });

    if let Some(log) = replication.log() {
        lines.push(format!(
            "replication log holds {} bytes, retention {} MiB",
            log.bytes(),
            config.log_retention_bytes / (1024 * 1024)
        ));
    }

    let mut vector = replication.applied().vector();
    vector.sort_unstable_by_key(|entry| entry.origin);
    if !vector.is_empty() {
        lines.push(format!(
            "applied through: {}",
            vector
                .iter()
                .map(|entry| format!("node {} → {}", entry.origin, entry.seq))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    if !Codec::Zstd.is_available() {
        lines.push(
            "note: this build has no compression codec (feature `replication_compression` off)"
                .to_string(),
        );
    }

    lines.extend(show_peers(replication));
    lines
}

/// Packages lines as a one-column result set.
///
/// A `SHOW` has no table behind it and so no schema to project. Returning rows
/// rather than a status byte is what lets `psql` and the pooled client display
/// the output at all, and one `STRING` column is the least that achieves it.
pub fn text_result(lines: &[String]) -> Vec<u8> {
    let mut result = vec![2u8];
    for line in lines {
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(line.len());
        block.into_slice_mut().copy_from_slice(line.as_bytes());
        let row = crate::core::row::row::Row {
            position: 0,
            columns: smallvec::smallvec![crate::core::row::row::Column {
                schema_id: 0,
                data: block,
                column_type: crate::core::db_type::DbType::STRING,
            }],
            length: 0,
        };
        row.extend_serialized(&mut result);
    }
    result
}

// ---------------------------------------------------------------------------
// The structured forms
// ---------------------------------------------------------------------------
//
// `SHOW REPLICATION`, `... STATUS`, `... PEERS` and `... SETTINGS` return lines
// of text, which is right for `psql` and for a runbook: they are read, not
// parsed. A console wants columns — it aligns them, sorts them, and colours a
// peer that is disconnected — and turning "peer berlin: connected, lag 3" back
// into fields with a regex is the sort of thing that works until someone
// improves the wording.
//
// So the structured forms are added beside the text ones rather than replacing
// them. Nothing that reads the text output changes, and the console never has
// to parse prose.

/// Whether this statement is one of the structured forms.
pub fn is_structured(sql: &str) -> bool {
    structured_kind(sql).is_some()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Structured {
    /// Node state and counters, as `(metric, value, detail)`.
    Stats,
    /// A row per peer.
    PeerStatus,
}

fn structured_kind(sql: &str) -> Option<Structured> {
    let upper = sql
        .trim()
        .trim_end_matches(';')
        .trim_end()
        .to_ascii_uppercase();
    let normalised = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    match normalised.as_str() {
        "SHOW REPLICATION STATS" => Some(Structured::Stats),
        "SHOW REPLICATION PEER STATUS" => Some(Structured::PeerStatus),
        _ => None,
    }
}

/// The columns a structured form returns, or `None` when the statement is one
/// of the text ones.
pub fn structured_fields(sql: &str) -> Option<Vec<(String, crate::core::db_type::DbType)>> {
    use crate::core::db_type::DbType;
    let text = |name: &str| (name.to_string(), DbType::STRING);
    let number = |name: &str| (name.to_string(), DbType::U64);
    let flag = |name: &str| (name.to_string(), DbType::BOOL);

    Some(match structured_kind(sql)? {
        Structured::Stats => vec![text("metric"), text("value"), text("detail")],
        Structured::PeerStatus => vec![
            text("peer"),
            text("address"),
            number("remote_node"),
            flag("connected"),
            flag("paused"),
            number("acked_seq"),
            number("lag"),
            number("queued"),
            number("records_sent"),
            number("batches_sent"),
            number("dropped"),
            text("codec"),
            text("last_ratio"),
            text("last_error"),
        ],
    })
}

/// Runs a structured form, returning the engine's row encoding.
pub fn execute_structured(sql: &str, database: &Arc<Database>) -> Result<Vec<u8>, String> {
    let kind = structured_kind(sql).ok_or_else(|| format!("not a structured form: `{}`", sql))?;
    let rows = match kind {
        Structured::Stats => stats_rows(database),
        Structured::PeerStatus => peer_rows(database),
    };

    let mut out = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut out);
    }
    Ok(out)
}

fn stats_rows(database: &Arc<Database>) -> Vec<crate::core::row::row::Row> {
    let replication = &database.replication;
    let config = replication.config();
    let stats = replication.stats();
    let peers = replication.peers();

    let mut out = Vec::new();
    let mut push = |metric: &str, value: String, detail: &str| {
        out.push(structured_row(&[
            SCell::Text(metric.to_string()),
            SCell::Text(value),
            SCell::Text(detail.to_string()),
        ]));
    };

    push(
        "active",
        replication.is_active().to_string(),
        "whether committed transactions are being shipped",
    );
    push(
        "node_id",
        replication.node_id().to_string(),
        "half of every global transaction id",
    );
    push(
        "cluster",
        config.cluster.clone(),
        "peers must agree on this",
    );
    push(
        "listen",
        format!("{}:{}", config.listen_addr, config.listen_port),
        "where this node accepts peer connections",
    );
    push(
        "listener_started",
        replication.listener_started().to_string(),
        "",
    );
    push(
        "auth",
        config.auth.to_string(),
        "how peers prove they belong to the cluster",
    );
    push(
        "token_set",
        (!config.token.is_empty()).to_string(),
        "a shared secret is configured",
    );
    push(
        "mode",
        config.sync_mode.to_string(),
        "how long a commit waits for its peers",
    );
    push(
        "conflict",
        config.conflict.to_string(),
        "what happens to a transaction that will not apply",
    );
    push(
        "compression",
        config.compression.to_string(),
        "the same spelling REPLICATION SET compression accepts",
    );
    push(
        "relay",
        config.relay.to_string(),
        "forward what arrives from one peer to the others",
    );

    push("peers", peers.len().to_string(), "");
    push(
        "peers_connected",
        peers
            .iter()
            .filter(|peer| peer.is_connected())
            .count()
            .to_string(),
        "",
    );
    push(
        "peers_paused",
        peers
            .iter()
            .filter(|peer| peer.is_paused())
            .count()
            .to_string(),
        "",
    );

    let local_seq = replication.local_seq();
    push(
        "local_sequence",
        local_seq.to_string(),
        "the newest locally-originated transaction",
    );

    // The figure an operator actually watches: the furthest any peer is behind.
    let worst_lag = peers
        .iter()
        .map(|peer| local_seq.saturating_sub(peer.acked_seq()))
        .max()
        .unwrap_or(0);
    push(
        "max_peer_lag",
        worst_lag.to_string(),
        "transactions the furthest-behind peer has not confirmed",
    );

    let load = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed).to_string();
    push(
        "captured",
        load(&stats.captured),
        "transactions taken from the local commit path",
    );
    push(
        "published",
        load(&stats.published),
        "handed to the peer queues",
    );
    push(
        "applied",
        load(&stats.applied),
        "received from peers and applied here",
    );
    push(
        "duplicates",
        load(&stats.duplicates),
        "already seen, so ignored",
    );
    push(
        "conflicts",
        load(&stats.conflicts),
        "would not apply; see the conflict policy",
    );
    push("failures", load(&stats.failures), "");
    push(
        "gaps",
        load(&stats.gaps),
        "sequences missing from a peer's stream",
    );
    push("batches_sent", load(&stats.batches_sent), "");
    push(
        "compressed_batches",
        load(&stats.compressed_batches),
        "batches that were worth compressing",
    );
    push(
        "bytes_raw",
        load(&stats.bytes_raw),
        "payload before compression",
    );
    push(
        "bytes_wire",
        load(&stats.bytes_wire),
        "bytes handed to the transport",
    );
    push(
        "compression_saving_percent",
        match stats.compression_saving() {
            Some(saving) => format!("{:.1}", saving),
            None => "n/a".to_string(),
        },
        "negative means the framing cost more than compression saved",
    );
    push(
        "ack_timeouts",
        load(&stats.ack_timeouts),
        "synchronous commits that gave up waiting for a peer",
    );

    push(
        "ack_timeout_ms",
        config.ack_timeout.as_millis().to_string(),
        "",
    );
    push(
        "batch_max_records",
        config.batch_max_records.to_string(),
        "",
    );
    push("batch_max_bytes", config.batch_max_bytes.to_string(), "");
    push(
        "batch_linger_us",
        config.batch_linger.as_micros().to_string(),
        "",
    );
    push(
        "queue_capacity",
        config.queue_capacity.to_string(),
        "records a peer queue holds",
    );
    push(
        "log_retention_bytes",
        config.log_retention_bytes.to_string(),
        "history kept for a peer that falls behind; past this it must resynchronise",
    );

    out
}

fn peer_rows(database: &Arc<Database>) -> Vec<crate::core::row::row::Row> {
    let replication = &database.replication;
    let local_seq = replication.local_seq();

    let mut peers: Vec<_> = replication
        .peers()
        .iter()
        .map(|peer| peer.snapshot(local_seq))
        .collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name));

    peers
        .into_iter()
        .map(|peer| {
            structured_row(&[
                SCell::Text(peer.name),
                SCell::Text(peer.address),
                SCell::Number(peer.remote_node),
                SCell::Flag(peer.connected),
                SCell::Flag(peer.paused),
                SCell::Number(peer.acked_seq),
                SCell::Number(peer.lag),
                SCell::Number(peer.queued as u64),
                SCell::Number(peer.records_sent),
                SCell::Number(peer.batches_sent),
                SCell::Number(peer.dropped),
                SCell::Text(peer.codec.to_string()),
                SCell::Text(match peer.last_ratio {
                    Some(ratio) => format!("{:.2}", ratio),
                    None => String::new(),
                }),
                SCell::Text(peer.last_error.unwrap_or_default()),
            ])
        })
        .collect()
}

enum SCell {
    Text(String),
    Number(u64),
    Flag(bool),
}

fn structured_row(cells: &[SCell]) -> crate::core::row::row::Row {
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row};

    let mut columns: smallvec::SmallVec<[Column; 32]> = smallvec::SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            SCell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            SCell::Number(n) => (n.to_le_bytes().to_vec(), DbType::U64),
            SCell::Flag(b) => (vec![u8::from(*b)], DbType::BOOL),
        };
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(Column {
            schema_id: index as u64,
            data: block,
            column_type,
        });
    }

    let length = columns.iter().map(|c| c.data.len()).sum::<usize>();
    Row {
        position: 0,
        columns,
        #[cfg(feature = "enable_long_row")]
        length: length as u64,
        #[cfg(not(feature = "enable_long_row"))]
        length: length as u32,
    }
}

/// The column a replication result set has, for the wire protocols that need
/// to describe a row before sending one.
pub fn result_fields() -> Vec<(String, crate::core::db_type::DbType)> {
    vec![(
        "replication".to_string(),
        crate::core::db_type::DbType::STRING,
    )]
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn database() -> Arc<Database> {
        let database = crate::replication::test_support::in_memory_database().await;
        database
            .replication
            .set_config(ReplicationConfig {
                node_id: 1,
                // Its own port: these tests run in parallel and several of them
                // reach `REPLICATION ENABLE`, which now actually binds.
                listen_port: crate::replication::test_support::free_port(),
                ..Default::default()
            })
            .unwrap();
        database.replication.open_storage(None).unwrap();
        database
    }

    #[test]
    fn replication_statements_are_recognised_and_others_are_not() {
        assert!(is_replication_statement("REPLICATION ENABLE"));
        assert!(is_replication_statement(
            "  replication add peer 'a' address 'h:1'"
        ));
        assert!(is_replication_statement("SHOW REPLICATION"));
        assert!(is_replication_statement("SHOW REPLICATION PEERS;"));

        assert!(!is_replication_statement("SELECT * FROM replication_log"));
        assert!(!is_replication_statement("SHOW FILTER"));
        assert!(!is_replication_statement("INSERT INTO t (id) VALUES (1)"));
    }

    #[test]
    fn a_quoted_name_survives_the_word_split() {
        let split = words("REPLICATION ADD PEER 'my peer' ADDRESS '10.0.0.1:1'");
        assert_eq!(split[3], "'my peer'");
        assert_eq!(unquote(&split[3]), "my peer");
        assert_eq!(unquote(&split[5]), "10.0.0.1:1");
    }

    #[test]
    fn with_options_are_read_as_pairs() {
        let options = with_options(
            "REPLICATION ADD PEER 'a' ADDRESS 'h:1' WITH (COMPRESSION = 'zstd:3', PAUSED = true)",
        )
        .unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(
            options[0],
            ("compression".to_string(), "zstd:3".to_string())
        );
        assert_eq!(options[1], ("paused".to_string(), "true".to_string()));
    }

    #[test]
    fn a_malformed_with_clause_is_rejected() {
        assert!(
            with_options("REPLICATION ADD PEER 'a' ADDRESS 'h:1' WITH COMPRESSION = 'zstd'")
                .is_err()
        );
        assert!(with_options("REPLICATION ADD PEER 'a' ADDRESS 'h:1' WITH (nonsense)").is_err());
    }

    #[tokio::test]
    async fn a_peer_can_be_added_and_dropped() {
        let database = database().await;

        let added = execute(
            "REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2:61171'",
            &database,
        )
        .await
        .unwrap();
        assert!(added[0].contains("berlin"));
        assert_eq!(database.replication.config().peers.len(), 1);

        execute("REPLICATION DROP PEER 'berlin'", &database)
            .await
            .unwrap();
        assert_eq!(database.replication.config().peers.len(), 0);
    }

    #[tokio::test]
    async fn the_same_peer_cannot_be_added_twice() {
        let database = database().await;
        execute(
            "REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2:61171'",
            &database,
        )
        .await
        .unwrap();

        assert!(
            execute(
                "REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.3:61171'",
                &database
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn a_peer_without_a_port_is_rejected() {
        let database = database().await;
        assert!(
            execute(
                "REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2'",
                &database
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn add_peer_needs_the_address_keyword() {
        let database = database().await;
        assert!(
            execute("REPLICATION ADD PEER 'berlin' '10.0.0.2:1'", &database)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn settings_round_trip_through_set_and_show() {
        let database = database().await;

        execute("REPLICATION SET conflict = 'last-writer-wins'", &database)
            .await
            .unwrap();
        assert_eq!(
            database.replication.config().conflict,
            ConflictPolicy::LastWriterWins
        );

        execute("REPLICATION SET mode = 'quorum:2'", &database)
            .await
            .unwrap();
        assert_eq!(database.replication.config().sync_mode, SyncMode::Quorum(2));

        execute("REPLICATION SET compression = 'none'", &database)
            .await
            .unwrap();
        assert_eq!(database.replication.config().compression.codec, Codec::None);

        execute("REPLICATION SET batch_linger_us = 250", &database)
            .await
            .unwrap();
        assert_eq!(
            database.replication.config().batch_linger,
            std::time::Duration::from_micros(250)
        );
    }

    #[tokio::test]
    async fn an_unknown_setting_is_rejected_rather_than_ignored() {
        let database = database().await;
        assert!(
            execute("REPLICATION SET nonsense = 1", &database)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_bad_value_leaves_the_setting_alone() {
        let database = database().await;
        let before = database.replication.config().conflict;

        assert!(
            execute("REPLICATION SET conflict = 'whatever'", &database)
                .await
                .is_err()
        );
        assert_eq!(database.replication.config().conflict, before);
    }

    #[tokio::test]
    async fn the_token_is_never_echoed_back() {
        let database = database().await;
        execute("REPLICATION SET token = 'correct-horse'", &database)
            .await
            .unwrap();

        let shown = execute("SHOW REPLICATION SETTINGS", &database)
            .await
            .unwrap();
        let joined = shown.join("\n");
        assert!(
            !joined.contains("correct-horse"),
            "a secret must not appear in a status view"
        );
        assert!(joined.contains("(set)"));
    }

    #[tokio::test]
    async fn the_node_id_cannot_move_underneath_a_running_cluster() {
        let database = database().await;
        database.replication.set_active(true);

        assert!(
            execute("REPLICATION SET node_id = 9", &database)
                .await
                .is_err()
        );

        database.replication.set_active(false);
        execute("REPLICATION SET node_id = 9", &database)
            .await
            .unwrap();
        assert_eq!(database.replication.config().node_id, 9);
    }

    #[tokio::test]
    async fn status_reports_the_node_and_its_counters() {
        let database = database().await;
        let lines = execute("SHOW REPLICATION STATUS", &database).await.unwrap();
        let joined = lines.join("\n");

        assert!(joined.contains("node 1"));
        assert!(joined.contains("disabled"));
        assert!(joined.contains("captured"));
    }

    #[tokio::test]
    async fn peers_are_listed_with_their_lag() {
        let database = database().await;
        execute(
            "REPLICATION ADD PEER 'berlin' ADDRESS '127.0.0.1:1'",
            &database,
        )
        .await
        .unwrap();
        execute("REPLICATION ENABLE", &database).await.unwrap();

        let lines = execute("SHOW REPLICATION PEERS", &database).await.unwrap();
        let joined = lines.join("\n");
        assert!(joined.contains("berlin"));
        assert!(joined.contains("lag"));

        execute("REPLICATION DISABLE", &database).await.unwrap();
    }

    #[tokio::test]
    async fn a_peer_can_be_paused_and_resumed() {
        let database = database().await;
        execute(
            "REPLICATION ADD PEER 'berlin' ADDRESS '127.0.0.1:1'",
            &database,
        )
        .await
        .unwrap();
        execute("REPLICATION ENABLE", &database).await.unwrap();

        execute("REPLICATION PAUSE PEER 'berlin'", &database)
            .await
            .unwrap();
        assert!(database.replication.peer("berlin").unwrap().is_paused());

        execute("REPLICATION RESUME PEER 'berlin'", &database)
            .await
            .unwrap();
        assert!(!database.replication.peer("berlin").unwrap().is_paused());

        execute("REPLICATION DISABLE", &database).await.unwrap();
    }

    #[tokio::test]
    async fn pausing_a_peer_that_does_not_exist_says_so() {
        let database = database().await;
        assert!(
            execute("REPLICATION PAUSE PEER 'nobody'", &database)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn enabling_without_a_node_id_is_refused() {
        let database = crate::replication::test_support::in_memory_database().await;
        assert!(execute("REPLICATION ENABLE", &database).await.is_err());
    }

    #[tokio::test]
    async fn an_unknown_statement_explains_itself() {
        let database = database().await;
        let error = execute("REPLICATION FROBNICATE", &database)
            .await
            .unwrap_err();
        assert!(
            error.contains("ENABLE"),
            "the message should list what is accepted: {}",
            error
        );
    }

    #[tokio::test]
    async fn keys_are_listed_for_discovery() {
        let database = database().await;
        let lines = execute("SHOW REPLICATION KEYS", &database).await.unwrap();
        assert!(lines.iter().any(|line| line.starts_with("compression")));
        assert!(lines.iter().any(|line| line.starts_with("conflict")));
    }

    #[test]
    fn a_text_result_is_a_readable_row_set() {
        let lines = vec!["first".to_string(), "second".to_string()];
        let encoded = text_result(&lines);

        assert_eq!(encoded[0], 2);
        let rows = crate::core::row::row::vec_into_rows(&encoded[1..]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns[0].data.into_slice(), b"first");
        assert_eq!(rows[1].columns[0].data.into_slice(), b"second");
    }
}
