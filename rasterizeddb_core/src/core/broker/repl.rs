//! Replication of the broker over the database's replication stream
//! (docs/broker.md §9).
//!
//! Broker effects travel as the key/value cache's do: chunks in `FLAG_KV`
//! records, whose first byte names their encoding. Ours is
//! [`BROKER_FORMAT_V1`]. A peer routes each chunk by that byte, and one that
//! does not know it counts an apply error and moves on. Captured, in the order
//! the broker made them:
//!
//! - **DDL**, by name (topic and subscription ids are each node's own):
//!   topics, queues, subscriptions, exchanges, bindings.
//! - **Appends**: each batch as stamped, captured under its partition's lock,
//!   so a partition's batches enter the stream in offset order. A peer
//!   appends the same bytes at the same offset; one it already has is a
//!   duplicate and is skipped, and one beyond its end is a gap, counted.
//! - **Subscription state**: the records a subscription logs (commits, acks,
//!   dead letters, floors), applied to the peer's subscription of that name.
//!
//! A partition is written on one node at a time (a primary and its
//! replicas): two nodes appending to one partition diverge, and the second
//! node's batches are counted as conflicts on the first. Memory topics are
//! each node's own and are not replicated, and neither is retention, which
//! each node applies by its own clock. Nothing a peer applies is captured
//! again, so effects never echo back; relaying is the stream's own business.

use std::cell::Cell;
use std::sync::Arc;

use rasterizeddb_mq::batch::BatchView;
use rasterizeddb_mq::wire::{Binding, ExchangeKind, QueueConfig, Reader, SubConfig, TopicConfig, Writer};

use super::{BrokerError, BrokerHandle};
use crate::replication::Replication;

/// The first byte of a chunk of broker effects. Chosen apart from the
/// key/value cache's (`KV_FORMAT_V1` = 1).
pub const BROKER_FORMAT_V1: u8 = crate::replication::protocol::BROKER_FORMAT_V1;

const E_TOPIC: u8 = 1;
const E_TOPIC_DROP: u8 = 2;
const E_TOPIC_ALTER: u8 = 3;
const E_SUB: u8 = 4;
const E_SUB_DROP: u8 = 5;
const E_EXCHANGE: u8 = 6;
const E_EXCHANGE_DROP: u8 = 7;
const E_BIND: u8 = 8;
const E_UNBIND: u8 = 9;
const E_APPEND: u8 = 10;
const E_SUB_RECORD: u8 = 11;

thread_local! {
    /// Set while a peer's chunk is applied: nothing applied is captured.
    static APPLYING: Cell<bool> = const { Cell::new(false) };
}

/// Whether this thread is applying a peer's effects.
#[inline]
pub fn applying() -> bool {
    APPLYING.with(Cell::get)
}

/// The replication a broker effect is captured for, when it is on and this
/// is not an apply.
#[inline]
pub fn capturing(r: Option<&Arc<Replication>>) -> Option<&Arc<Replication>> {
    r.filter(|r| r.is_active()).filter(|_| !applying())
}

/// One effect: kind, length, body.
fn entry(kind: u8, body: impl FnOnce(&mut Writer<'_>)) -> Vec<u8> {
    let mut out = vec![kind, 0, 0, 0, 0];
    body(&mut Writer(&mut out));
    let n = (out.len() - 5) as u32;
    out[1..5].copy_from_slice(&n.to_le_bytes());
    out
}

#[cold]
#[inline(never)]
fn send(r: &Arc<Replication>, e: Vec<u8>) {
    r.effect_append(BROKER_FORMAT_V1, &e);
}

#[cold]
#[inline(never)]
pub fn topic(r: &Arc<Replication>, name: &str, config: &TopicConfig, queue: Option<&QueueConfig>) {
    send(r, entry(E_TOPIC, |w| {
        w.str(name);
        config.encode(w);
        match queue {
            Some(q) => {
                w.u8(1);
                q.encode(w)
            }
            None => w.u8(0),
        }
    }));
}

#[cold]
#[inline(never)]
pub fn topic_drop(r: &Arc<Replication>, name: &str) {
    send(r, entry(E_TOPIC_DROP, |w| w.str(name)));
}

#[cold]
#[inline(never)]
pub fn topic_alter(r: &Arc<Replication>, name: &str, config: &TopicConfig) {
    send(r, entry(E_TOPIC_ALTER, |w| {
        w.str(name);
        config.encode(w)
    }));
}

#[cold]
#[inline(never)]
pub fn sub(r: &Arc<Replication>, topic: &str, name: &str, config: &SubConfig) {
    send(r, entry(E_SUB, |w| {
        w.str(topic);
        w.str(name);
        config.encode(w)
    }));
}

#[cold]
#[inline(never)]
pub fn sub_drop(r: &Arc<Replication>, topic: &str, name: &str) {
    send(r, entry(E_SUB_DROP, |w| {
        w.str(topic);
        w.str(name)
    }));
}

#[cold]
#[inline(never)]
pub fn exchange(r: &Arc<Replication>, name: &str, kind: ExchangeKind, alternate: Option<&str>) {
    send(r, entry(E_EXCHANGE, |w| {
        w.str(name);
        w.u8(kind as u8);
        w.str(alternate.unwrap_or(""))
    }));
}

#[cold]
#[inline(never)]
pub fn exchange_drop(r: &Arc<Replication>, name: &str) {
    send(r, entry(E_EXCHANGE_DROP, |w| w.str(name)));
}

#[cold]
#[inline(never)]
pub fn bind(r: &Arc<Replication>, b: &Binding, unbind: bool) {
    send(r, entry(if unbind { E_UNBIND } else { E_BIND }, |w| b.encode(w)));
}

/// A stamped batch, as appended. Called under the partition's lock.
#[cold]
#[inline(never)]
pub fn append(r: &Arc<Replication>, topic: &str, partition: u32, batch: &[u8]) {
    send(r, entry(E_APPEND, |w| {
        w.str(topic);
        w.u32(partition);
        w.raw(batch)
    }));
}

/// A record a subscription logged: its kind and body as the log has them.
#[cold]
#[inline(never)]
pub fn sub_record(r: &Arc<Replication>, topic: &str, sub: &str, kind: u8, body: &[u8]) {
    send(r, entry(E_SUB_RECORD, |w| {
        w.str(topic);
        w.str(sub);
        w.u8(kind);
        w.raw(body)
    }));
}

/// What applying one chunk did.
#[derive(Default, Debug)]
pub struct Report {
    pub applied: u64,
    pub errors: u64,
    pub first_error: Option<String>,
}

/// Applies one chunk a peer sent. Every effect is tried, and one that fails
/// is counted, so one bad effect does not stop the stream.
pub fn apply_chunk(handle: &BrokerHandle, chunk: &[u8]) -> Report {
    let mut report = Report::default();
    let fail = |report: &mut Report, e: String| {
        report.errors += 1;
        report.first_error.get_or_insert(e);
    };
    if chunk.first() != Some(&BROKER_FORMAT_V1) {
        fail(&mut report, "not a broker chunk".into());
        return report;
    }
    let broker = match handle.get() {
        Ok(b) => b,
        Err(e) => {
            fail(&mut report, format!("the broker did not open: {e}"));
            return report;
        }
    };
    struct Applying;
    impl Drop for Applying {
        fn drop(&mut self) {
            APPLYING.with(|a| a.set(false));
        }
    }
    APPLYING.with(|a| a.set(true));
    let _applying = Applying;
    let mut at = 1;
    while at < chunk.len() {
        if chunk.len() - at < 5 {
            fail(&mut report, "a truncated effect".into());
            break;
        }
        let kind = chunk[at];
        let n = u32::from_le_bytes(chunk[at + 1..at + 5].try_into().unwrap()) as usize;
        let Some(body) = chunk.get(at + 5..at + 5 + n) else {
            fail(&mut report, "an effect runs past its chunk".into());
            break;
        };
        at += 5 + n;
        match apply_one(&broker, kind, body) {
            Ok(()) => report.applied += 1,
            Err(e) => fail(&mut report, e),
        }
    }
    report
}

fn apply_one(broker: &super::Broker, kind: u8, body: &[u8]) -> Result<(), String> {
    let mut r = Reader::new(body);
    let d = |e: rasterizeddb_mq::wire::DecodeError| e.to_string();
    let b = |e: BrokerError| e.message;
    match kind {
        E_TOPIC => {
            let name = r.string().map_err(d)?;
            let config = TopicConfig::decode(&mut r).map_err(d)?;
            if r.u8().map_err(d)? == 1 {
                let q = QueueConfig::decode(&mut r).map_err(d)?;
                broker.declare_queue(&name, q, true).map_err(b)?;
            } else {
                broker.create_topic(&name, config, true).map_err(b)?;
            }
        }
        E_TOPIC_DROP => {
            broker.delete_topic(&r.string().map_err(d)?, true).map_err(b)?;
        }
        E_TOPIC_ALTER => {
            let name = r.string().map_err(d)?;
            let c = TopicConfig::decode(&mut r).map_err(d)?;
            broker
                .alter_topic(&name, |old| {
                    old.retention_ms = c.retention_ms;
                    old.retention_bytes = c.retention_bytes;
                    old.max_batch_bytes = c.max_batch_bytes;
                    old.segment_bytes = c.segment_bytes;
                })
                .map_err(b)?;
        }
        E_SUB => {
            let topic = broker.require_topic(&r.string().map_err(d)?).map_err(b)?;
            let name = r.string().map_err(d)?;
            let config = SubConfig::decode(&mut r).map_err(d)?;
            broker.subscription(&topic, &name, config).map_err(b)?;
        }
        E_SUB_DROP => {
            let topic = broker.require_topic(&r.string().map_err(d)?).map_err(b)?;
            broker.delete_subscription(&topic, &r.string().map_err(d)?, true).map_err(b)?;
        }
        E_EXCHANGE => {
            let name = r.string().map_err(d)?;
            let kind = ExchangeKind::from_u8(r.u8().map_err(d)?).ok_or("unknown exchange kind")?;
            let alt = r.string().map_err(d)?;
            broker.declare_exchange(&name, kind, (!alt.is_empty()).then_some(alt.as_str()), true).map_err(b)?;
        }
        E_EXCHANGE_DROP => {
            broker.delete_exchange(&r.string().map_err(d)?, true).map_err(b)?;
        }
        E_BIND | E_UNBIND => {
            let binding = Binding::decode(&mut r).map_err(d)?;
            if kind == E_BIND {
                broker.bind(binding).map_err(b)?;
            } else {
                broker.unbind(&binding).map_err(b)?;
            }
        }
        E_APPEND => {
            let topic = broker.require_topic(&r.string().map_err(d)?).map_err(b)?;
            let p = topic.partition(r.u32().map_err(d)?).map_err(b)?.clone();
            let batch = r.rest();
            rasterizeddb_mq::batch::check(batch, rasterizeddb_mq::batch::MAX_BATCH, true).map_err(|e| e.to_string())?;
            let v = BatchView::trusted(batch);
            match p.append_replicated(batch).map_err(b)? {
                super::partition::Replicated::Applied | super::partition::Replicated::Duplicate => {}
                super::partition::Replicated::Gap { next } => {
                    return Err(format!(
                        "{}/{}: a batch at offset {} after a gap (this node is at {next})",
                        topic.name,
                        p.index,
                        v.base_offset()
                    ));
                }
            }
        }
        E_SUB_RECORD => {
            let topic = broker.require_topic(&r.string().map_err(d)?).map_err(b)?;
            let name = r.string().map_err(d)?;
            let sub = topic.subscription(&name).ok_or_else(|| format!("no subscription \"{name}\" on \"{}\"", topic.name))?;
            let kind = r.u8().map_err(d)?;
            sub.apply_record(kind, r.rest()).map_err(b)?;
        }
        other => return Err(format!("unknown broker effect {other}")),
    }
    Ok(())
}
