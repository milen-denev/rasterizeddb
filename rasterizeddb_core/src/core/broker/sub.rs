//! Subscriptions: how a topic's messages are handed out (docs/broker.md §1, §3).
//!
//! - **Ordered.** Each partition has one committed offset, and the topic's
//!   partitions are shared out among the members (`p → members[p % n]`, by
//!   join order). A member is pushed batches from its position, and an ack (or
//!   a commit) moves the committed offset. When the membership changes, a
//!   partition's new owner starts at the committed offset, so delivery is at
//!   least once.
//! - **Shared.** Competing consumers. A record goes to one member with
//!   credit, is in flight until acked, and is redelivered on a nack, on an ack
//!   timeout, or when its member leaves. A record delivered `max_deliveries`
//!   times goes to the dead-letter topic. The state is a `floor` (every offset
//!   below it is acked) and a bitmap of acks above it.
//! - **Broadcast.** One member, its own cursor on every partition, nothing
//!   stored.
//!
//! Delivery is a push. One dispatcher task per subscription, started when the
//! first member joins and ended when the last leaves, wakes on any
//! partition's appends, on credit, acks and membership changes, and on its
//! timers. It reads stored batches and sends `DELIVER` frames to the
//! members' outboxes. A frame carries whole batches plus the ranges of
//! records in them that are the member's (`wire::Deliver`).
//!
//! State changes (commits, acks, dead letters) go to the subscription's own
//! log, `t<topic>/s<id>.<gen>.log`. The flusher writes it in every round and
//! syncs it on the async clock. A lost ack after a crash means one more
//! delivery, which an at-least-once consumer has to expect anyway.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rasterizeddb_mq::batch::{BatchBuilder, Batches};
use rasterizeddb_mq::wire::{self, Code, Durability, Range, Start, SubConfig, SubMode, Writer, op};
use tokio::sync::{Notify, mpsc};

use super::BrokerError;
use super::engine::{Broker, Topic};
use super::flog::{self, LogFile};
use super::partition::WriteRound;
use crate::core::framed_log::Format;

const S_COMMIT: u8 = 1;
const S_ACK: u8 = 2;
const S_FLOOR: u8 = 3;
const S_DEAD: u8 = 4;

const SUB_FORMAT: Format = Format {
    name: "broker subscription log",
    max_record_len: 1 << 20,
    layout: flog::LAYOUT,
    known_kind: |k| matches!(k, 1..=4 | flog::SEALED),
};

/// A subscription log is compacted once it passes this.
const SUB_COMPACT_AT: u64 = 4 << 20;
/// Bytes one `DELIVER` frame carries, beyond its first batch.
pub const DELIVER_BYTES: usize = 1 << 20;
/// A shared partition stops handing out new records this far past its floor:
/// one record no one acks must not grow the bitmap without bound.
const MAX_WINDOW: u64 = 1 << 24;

/// Bytes of frames a member's connection may have queued and not yet taken
/// before it is treated as out of credit: a client that grants credit and
/// stops reading must not grow the server's memory without bound.
pub const OUTBOX_BYTES: usize = 16 << 20;

/// Frames to one connection, with the bytes queued and not yet taken.
#[derive(Clone)]
pub struct Outbox {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
}

/// A connection's end of its [`Outbox`].
pub struct OutboxRx {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
}

/// A connection's outbox and its receiving end.
pub fn outbox() -> (Outbox, OutboxRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    let queued = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    (Outbox { tx, queued: queued.clone() }, OutboxRx { rx, queued })
}

impl Outbox {
    pub fn send(&self, frame: Vec<u8>) -> Result<(), ()> {
        self.queued.fetch_add(frame.len(), Ordering::Relaxed);
        self.tx.send(frame).map_err(|_| ())
    }
    /// Past [`OUTBOX_BYTES`]: deliveries wait until the connection catches up.
    pub fn is_full(&self) -> bool {
        self.queued.load(Ordering::Relaxed) > OUTBOX_BYTES
    }
}

impl OutboxRx {
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        let f = self.rx.recv().await?;
        self.queued.fetch_sub(f.len(), Ordering::Relaxed);
        Some(f)
    }
    pub fn try_recv(&mut self) -> Result<Vec<u8>, mpsc::error::TryRecvError> {
        let f = self.rx.try_recv()?;
        self.queued.fetch_sub(f.len(), Ordering::Relaxed);
        Ok(f)
    }
}

struct Inflight {
    member: u64,
    deadline: Instant,
}

/// One partition's state in a subscription.
#[derive(Default)]
struct PartState {
    /// Ordered: the committed offset. Shared: unused.
    committed: Option<u64>,
    /// Shared: every offset below this is acked or dead.
    floor: u64,
    /// Shared: the offset of bit 0 of `bits[0]`, a multiple of 64 at or below
    /// `floor`. Bits are absolute, so moving the floor never shifts them.
    base: u64,
    /// Shared: bit `i` of word `w` is offset `base + 64w + i`, acked.
    bits: VecDeque<u64>,
    /// Shared: the next offset never handed out.
    cursor: u64,
    inflight: HashMap<u64, Inflight>,
    /// Shared: offsets to hand out again, ahead of new ones.
    redeliver: VecDeque<u64>,
    /// Shared: nacked with a delay.
    delayed: BinaryHeap<std::cmp::Reverse<(Instant, u64)>>,
    /// Shared: deliveries made of offsets delivered more than once.
    deliveries: HashMap<u64, u32>,
    /// Whether the position came from the log (or `Start`) yet.
    ready: bool,
}

impl PartState {
    /// Starts the window at `floor`, empty.
    fn reset_floor(&mut self, floor: u64) {
        self.floor = floor;
        self.base = floor & !63;
        self.bits.clear();
    }

    #[inline]
    fn bit(&self, off: u64) -> bool {
        let i = off - self.base;
        self.bits.get((i / 64) as usize).is_some_and(|w| w & (1 << (i % 64)) != 0)
    }

    fn is_acked(&self, off: u64) -> bool {
        off < self.floor || self.bit(off)
    }

    /// Marks `off` acked; `false` when it already was.
    fn set_acked(&mut self, off: u64) -> bool {
        if off < self.floor {
            return false;
        }
        let i = off - self.base;
        let word = (i / 64) as usize;
        if self.bits.len() <= word {
            self.bits.resize(word + 1, 0);
        }
        let bit = 1u64 << (i % 64);
        if self.bits[word] & bit != 0 {
            return false;
        }
        self.bits[word] |= bit;
        if off == self.floor {
            self.advance_floor();
        }
        true
    }

    /// Moves the floor past every acked offset at it, dropping whole words
    /// the floor has passed. O(1) per offset it moves.
    fn advance_floor(&mut self) {
        loop {
            let i = self.floor - self.base;
            let Some(&w) = self.bits.get((i / 64) as usize) else { break };
            let rest = w >> (i % 64);
            if rest & 1 == 0 {
                break;
            }
            // Every consecutive set bit from the floor, at once.
            let n = (rest.trailing_ones() as u64).min(64 - i % 64);
            self.floor += n;
            while self.floor - self.base >= 64 {
                self.bits.pop_front();
                self.base += 64;
            }
        }
        if self.bits.is_empty() {
            self.base = self.floor & !63;
        }
        if self.cursor < self.floor {
            self.cursor = self.floor;
        }
    }

    /// Moves the floor up to `to` (retention removed what was below it),
    /// keeping the acks above it.
    fn raise_floor(&mut self, to: u64) {
        if to <= self.floor {
            return;
        }
        self.floor = to;
        while self.floor - self.base >= 64 {
            if self.bits.pop_front().is_none() {
                self.base = self.floor & !63;
                break;
            }
            self.base += 64;
        }
        self.advance_floor();
        self.inflight.retain(|o, _| *o >= to);
        self.redeliver.retain(|o| *o >= to);
        if self.cursor < self.floor {
            self.cursor = self.floor;
        }
    }

    /// Acked offsets above the floor.
    fn acked_above_floor(&self) -> u64 {
        let mut n: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        if let Some(&w) = self.bits.front() {
            let below = (self.floor - self.base).min(64);
            let mask = if below == 64 { u64::MAX } else { (1u64 << below) - 1 };
            n -= (w & mask).count_ones() as u64;
        }
        n
    }

    /// Acked offsets above the floor, as runs.
    fn acked_runs(&self) -> Vec<(u64, u32)> {
        let mut runs: Vec<(u64, u32)> = Vec::new();
        for (wi, &w) in self.bits.iter().enumerate() {
            if w == 0 {
                continue;
            }
            for b in 0..64 {
                let off = self.base + wi as u64 * 64 + b;
                if off < self.floor || w & (1 << b) == 0 {
                    continue;
                }
                match runs.last_mut() {
                    Some((s, n)) if *s + *n as u64 == off => *n += 1,
                    _ => runs.push((off, 1)),
                }
            }
        }
        runs
    }
}

/// A record [`Subscription::take`] handed out.
#[derive(Clone, Debug)]
pub struct Taken {
    pub partition: u32,
    pub offset: u64,
    pub time_us: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
}

struct Member {
    id: u64,
    handle: u32,
    out: Outbox,
    credits: u64,
    /// Ordered: its partitions. Broadcast: every partition.
    assigned: Vec<u32>,
    /// Ordered and broadcast: the next offset to push, per partition.
    positions: HashMap<u32, u64>,
    /// Asked for delivery counts on its `DELIVER` frames
    /// (`wire::SUB_DELIVERY_COUNTS`).
    counts: bool,
}

#[derive(Default)]
struct Members {
    list: Vec<Member>,
    generation: u64,
    /// Shared: where the round robin resumes.
    rr: usize,
}

struct SubLog {
    file: LogFile,
    /// Framed records not yet written.
    pending: Vec<u8>,
}

pub struct Subscription {
    pub id: u32,
    pub name: String,
    pub topic: Arc<Topic>,
    pub config: SubConfig,
    broker: Weak<Broker>,
    log: Option<Mutex<SubLog>>,
    dir: Option<PathBuf>,
    parts: Vec<Mutex<PartState>>,
    members: Mutex<Members>,
    wake: Notify,
    closed: AtomicBool,
    running: AtomicBool,
    dirty: AtomicBool,
    next_member: AtomicU64,
    me: Weak<Subscription>,
    pub delivered: AtomicU64,
    pub acked: AtomicU64,
    pub redelivered: AtomicU64,
    pub dead: AtomicU64,
}

fn stem(id: u32) -> String {
    format!("s{id}")
}

impl Subscription {
    fn build(
        broker: &Broker,
        id: u32,
        topic: Arc<Topic>,
        name: &str,
        config: SubConfig,
        log: Option<LogFile>,
        parts: Vec<PartState>,
    ) -> Arc<Subscription> {
        let dir = broker.sub_dir(topic.id);
        let me_broker = broker.weak();
        Arc::new_cyclic(|me| Subscription {
            id,
            name: name.to_string(),
            topic,
            config,
            broker: me_broker,
            log: log.map(|file| Mutex::new(SubLog { file, pending: Vec::new() })),
            dir,
            parts: parts.into_iter().map(Mutex::new).collect(),
            members: Mutex::new(Members::default()),
            wake: Notify::new(),
            closed: AtomicBool::new(false),
            running: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
            next_member: AtomicU64::new(1),
            me: me.clone(),
            delivered: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            redelivered: AtomicU64::new(0),
            dead: AtomicU64::new(0),
        })
    }

    fn persistent(topic: &Topic) -> bool {
        topic.dir.is_some() && topic.config().durability != Durability::Memory
    }

    /// A new stored subscription. Its starting positions are resolved now and
    /// written first, so a restart resumes from them.
    pub fn create(broker: &Broker, id: u32, topic: Arc<Topic>, name: &str, config: SubConfig) -> io::Result<Arc<Subscription>> {
        let n = topic.partition_count() as usize;
        let mut parts: Vec<PartState> = (0..n).map(|_| PartState::default()).collect();
        let mut records = Vec::new();
        for (p, st) in parts.iter_mut().enumerate() {
            let pos = resolve_start(&topic, p as u32, config.start);
            match config.mode {
                SubMode::Ordered => {
                    st.committed = Some(pos);
                    records.push((S_COMMIT, rec(|w| {
                        w.u32(p as u32);
                        w.u64(pos)
                    })));
                }
                _ => {
                    st.reset_floor(pos);
                    st.cursor = pos;
                    records.push((S_FLOOR, rec(|w| {
                        w.u32(p as u32);
                        w.u64(pos)
                    })));
                }
            }
            st.ready = true;
        }
        let log = match (Self::persistent(&topic), broker.sub_dir(topic.id)) {
            (true, Some(dir)) => {
                LogFile::remove(&dir, &stem(id));
                let mut f = LogFile::open(&dir, &stem(id), SUB_FORMAT, |_, _| Ok(()))?;
                f.compact(&records)?;
                Some(f)
            }
            _ => None,
        };
        Ok(Self::build(broker, id, topic, name, config, log, parts))
    }

    /// A stored subscription, from its log.
    pub fn open(broker: &Broker, id: u32, topic: Arc<Topic>, name: &str, config: SubConfig) -> io::Result<Arc<Subscription>> {
        let n = topic.partition_count() as usize;
        let mut parts: Vec<PartState> = (0..n).map(|_| PartState::default()).collect();
        let log = match (Self::persistent(&topic), broker.sub_dir(topic.id)) {
            (true, Some(dir)) => Some(LogFile::open(&dir, &stem(id), SUB_FORMAT, |kind, body| {
                let mut r = wire::Reader::new(body);
                let e = |e: wire::DecodeError| e.to_string();
                let p = r.u32().map_err(e)? as usize;
                let Some(st) = parts.get_mut(p) else { return Ok(()) };
                match kind {
                    S_COMMIT => {
                        st.committed = Some(r.u64().map_err(e)?);
                        st.ready = true;
                    }
                    S_FLOOR => {
                        let f = r.u64().map_err(e)?;
                        if st.ready {
                            st.raise_floor(f);
                        } else {
                            st.reset_floor(f);
                        }
                        st.ready = true;
                    }
                    S_ACK | S_DEAD => {
                        let off = r.u64().map_err(e)?;
                        let count = r.u32().map_err(e)?;
                        for o in off..off + count as u64 {
                            st.set_acked(o);
                        }
                    }
                    _ => {}
                }
                Ok(())
            })?),
            _ => None,
        };
        for (p, st) in parts.iter_mut().enumerate() {
            if !st.ready {
                // A memory topic's subscription, or a log that lost its first
                // records: start over by the configured start.
                let pos = resolve_start(&topic, p as u32, config.start);
                st.committed = Some(pos);
                st.reset_floor(pos);
                st.ready = true;
            }
            st.cursor = st.floor;
        }
        Ok(Self::build(broker, id, topic, name, config, log, parts))
    }

    /// A broadcast subscription: one member, nothing stored.
    pub fn ephemeral(broker: &Broker, id: u32, topic: Arc<Topic>, config: SubConfig) -> Result<Arc<Subscription>, BrokerError> {
        let n = topic.partition_count() as usize;
        let parts = (0..n).map(|_| PartState { ready: true, ..Default::default() }).collect::<Vec<_>>();
        let name = format!("broadcast-{id}");
        Ok(Self::build(broker, id, topic, &name, config, None, parts))
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn member_count(&self) -> usize {
        self.members.lock().list.len()
    }

    /// The committed offset of an ordered subscription's partition.
    pub fn committed(&self, partition: u32) -> Option<u64> {
        self.parts.get(partition as usize).and_then(|p| p.lock().committed)
    }

    /// Records not yet acked: `next - floor - acked above floor` (shared), or
    /// `next - committed` (ordered).
    pub fn backlog(&self) -> u64 {
        let mut n = 0;
        for (i, p) in self.parts.iter().enumerate() {
            let next = self.topic.partitions[i].next_offset();
            let st = p.lock();
            n += match self.config.mode {
                SubMode::Ordered => next.saturating_sub(st.committed.unwrap_or(next)),
                _ => next.saturating_sub(st.floor).saturating_sub(st.acked_above_floor()),
            };
        }
        n
    }

    pub fn inflight(&self) -> usize {
        self.parts.iter().map(|p| p.lock().inflight.len()).sum()
    }

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------

    /// Adds a member that wants up to `credits` records pushed before it
    /// grants more. Returns its id and (ordered) its partitions. The
    /// dispatcher is started on `rt` if it is not running.
    pub fn join(&self, handle: u32, credits: u32, out: Outbox, rt: &tokio::runtime::Handle) -> Result<(u64, Vec<u32>), BrokerError> {
        self.join_with(handle, credits, 0, out, rt)
    }

    /// [`Subscription::join`] with `SUBSCRIBE`'s options
    /// (`wire::SUB_DELIVERY_COUNTS`).
    pub fn join_with(
        &self,
        handle: u32,
        credits: u32,
        options: u32,
        out: Outbox,
        rt: &tokio::runtime::Handle,
    ) -> Result<(u64, Vec<u32>), BrokerError> {
        if self.is_closed() {
            return Err(BrokerError::new(Code::NoSubscription, format!("subscription \"{}\" is closed", self.name)));
        }
        let id = self.next_member.fetch_add(1, Ordering::Relaxed);
        let assigned = {
            let mut m = self.members.lock();
            if self.config.mode == SubMode::Broadcast && !m.list.is_empty() {
                return Err(BrokerError::invalid("a broadcast subscription has one member"));
            }
            let mut member = Member {
                id,
                handle,
                out,
                credits: credits as u64,
                assigned: Vec::new(),
                positions: HashMap::new(),
                counts: options & wire::SUB_DELIVERY_COUNTS != 0,
            };
            if self.config.mode == SubMode::Broadcast {
                member.assigned = (0..self.topic.partition_count()).collect();
                for p in &member.assigned {
                    member.positions.insert(*p, resolve_start(&self.topic, *p, self.config.start));
                }
            }
            m.list.push(member);
            if self.config.mode == SubMode::Ordered {
                self.rebalance(&mut *m);
            }
            m.list.last().map(|x| x.assigned.clone()).unwrap_or_default()
        };
        self.ensure_dispatcher(rt);
        self.wake.notify_one();
        Ok((id, assigned))
    }

    /// Removes a member. Its in-flight records go back to the front (shared);
    /// its partitions go to the others (ordered).
    pub fn leave(&self, member: u64) {
        let mut m = self.members.lock();
        let Some(i) = m.list.iter().position(|x| x.id == member) else { return };
        m.list.remove(i);
        match self.config.mode {
            SubMode::Shared => {
                for p in &self.parts {
                    let mut st = p.lock();
                    let mine: Vec<u64> = st.inflight.iter().filter(|(_, f)| f.member == member).map(|(o, _)| *o).collect();
                    let mut mine = mine;
                    mine.sort_unstable();
                    for o in mine.into_iter().rev() {
                        st.inflight.remove(&o);
                        st.redeliver.push_front(o);
                    }
                }
            }
            SubMode::Ordered => self.rebalance(&mut *m),
            SubMode::Broadcast => {}
        }
        drop(m);
        self.wake.notify_one();
    }

    /// Removes a member and leaves its in-flight records in flight: they are
    /// still acked, nacked or rejected by whoever holds them, and go back at
    /// their ack timeout otherwise (AMQP's `basic.cancel`, after which the
    /// channel still settles what it was given).
    pub fn leave_keeping_inflight(&self, member: u64) {
        let mut m = self.members.lock();
        let Some(i) = m.list.iter().position(|x| x.id == member) else { return };
        m.list.remove(i);
        if self.config.mode == SubMode::Ordered {
            self.rebalance(&mut *m);
        }
        drop(m);
        self.wake.notify_one();
    }

    /// Shares the partitions out again: `p → members[p % n]`. A partition that
    /// moves starts at its committed offset with its new owner.
    fn rebalance(&self, m: &mut Members) {
        m.generation += 1;
        let n = m.list.len();
        let parts = self.topic.partition_count();
        for (i, member) in m.list.iter_mut().enumerate() {
            let want: Vec<u32> = (0..parts).filter(|p| *p as usize % n == i).collect();
            if want == member.assigned {
                continue;
            }
            member.positions.retain(|p, _| want.contains(p));
            for p in &want {
                if !member.positions.contains_key(p) {
                    let pos = self.parts[*p as usize]
                        .lock()
                        .committed
                        .unwrap_or_else(|| resolve_start(&self.topic, *p, self.config.start));
                    member.positions.insert(*p, pos);
                }
            }
            member.assigned = want;
            let mut frame = Vec::new();
            wire::frame(&mut frame, op::ASSIGN, wire::F_PUSH, member.handle, |w| {
                w.u64(m.generation);
                w.u32(member.assigned.len() as u32);
                for p in &member.assigned {
                    w.u32(*p);
                }
            });
            let _ = member.out.send(frame);
        }
    }

    pub fn credit(&self, member: u64, n: u32) {
        let mut m = self.members.lock();
        if let Some(x) = m.list.iter_mut().find(|x| x.id == member) {
            x.credits = x.credits.saturating_add(n as u64);
        }
        drop(m);
        self.wake.notify_one();
    }

    /// Acks records. Shared: each in-flight offset becomes acked. Ordered:
    /// each range's end becomes the partition's committed offset when it is
    /// past it. Broadcast: nothing.
    pub fn ack(&self, ranges: &[Range]) -> Result<u64, BrokerError> {
        let mut acked = 0u64;
        match self.config.mode {
            SubMode::Broadcast => {}
            SubMode::Ordered => {
                // An ack moves the committed offset forward only; `commit`
                // can move it anywhere.
                for r in ranges {
                    let end = r.offset + r.count as u64;
                    if self.committed(r.partition).is_none_or(|c| end > c) {
                        self.commit(r.partition, end)?;
                    }
                    acked += r.count as u64;
                }
            }
            SubMode::Shared => {
                let mut records = Vec::new();
                for r in ranges {
                    let Some(p) = self.parts.get(r.partition as usize) else {
                        return Err(BrokerError::new(Code::NoPartition, format!("no partition {}", r.partition)));
                    };
                    let mut st = p.lock();
                    let mut run: Option<(u64, u32)> = None;
                    for off in r.offset..r.offset + r.count as u64 {
                        // Only what was handed out: an offset at or past the
                        // cursor has not been delivered.
                        if off >= st.cursor {
                            break;
                        }
                        st.inflight.remove(&off);
                        st.deliveries.remove(&off);
                        if st.set_acked(off) {
                            acked += 1;
                            run = match run {
                                Some((s, n)) if s + n as u64 == off => Some((s, n + 1)),
                                Some(done) => {
                                    records.push((r.partition, done));
                                    Some((off, 1))
                                }
                                None => Some((off, 1)),
                            };
                        }
                    }
                    if let Some(done) = run {
                        records.push((r.partition, done));
                    }
                }
                for (p, (off, n)) in records {
                    self.log_record(S_ACK, |w| {
                        w.u32(p);
                        w.u64(off);
                        w.u32(n)
                    });
                }
            }
        }
        self.acked.fetch_add(acked, Ordering::Relaxed);
        if acked > 0 {
            self.wake.notify_one();
        }
        Ok(acked)
    }

    /// Gives records up without delivering them again: dead-lettered when the
    /// subscription has a dead-letter topic, then acked (AMQP's reject and
    /// nack without requeue).
    pub fn reject(&self, ranges: &[Range]) {
        for r in ranges {
            let offs: Vec<u64> = (r.offset..r.offset + r.count as u64).collect();
            self.bury(r.partition, &offs, true);
        }
        self.wake.notify_one();
    }

    /// Drops every record not in flight (AMQP's `queue.purge`): the floor
    /// moves to the end of each partition. Returns how many went.
    pub fn purge(&self) -> u64 {
        let mut n = 0;
        for (i, p) in self.parts.iter().enumerate() {
            let next = self.topic.partitions[i].next_offset();
            let mut st = p.lock();
            let inflight: Vec<u64> = st.inflight.keys().copied().collect();
            let before = next.saturating_sub(st.floor).saturating_sub(st.acked_above_floor());
            let low = inflight.iter().copied().min().unwrap_or(next);
            // Everything below the lowest in-flight record goes at once, the
            // rest one by one around the ones in flight.
            st.raise_floor(low);
            for o in low..next {
                if !st.inflight.contains_key(&o) {
                    st.set_acked(o);
                }
            }
            st.cursor = st.cursor.max(next);
            st.redeliver.clear();
            st.delayed.clear();
            let after = next.saturating_sub(st.floor).saturating_sub(st.acked_above_floor());
            n += before.saturating_sub(after);
            let floor = st.floor;
            let runs = st.acked_runs();
            drop(st);
            self.log_record(S_FLOOR, |w| {
                w.u32(i as u32);
                w.u64(floor)
            });
            for (off, c) in runs {
                self.log_record(S_ACK, |w| {
                    w.u32(i as u32);
                    w.u64(off);
                    w.u32(c)
                });
            }
        }
        n
    }

    /// Pushes in-flight records' ack deadlines `extend_ms` from now (0: the
    /// subscription's ack timeout): a lock renewal. Returns how many were in
    /// flight.
    pub fn renew(&self, ranges: &[Range], extend_ms: u32) -> u64 {
        let ms = if extend_ms == 0 { self.config.ack_timeout_ms } else { extend_ms };
        let deadline = Instant::now() + Duration::from_millis(ms.max(1) as u64);
        let mut n = 0;
        for r in ranges {
            let Some(p) = self.parts.get(r.partition as usize) else { continue };
            let mut st = p.lock();
            for off in r.offset..r.offset + r.count as u64 {
                if let Some(f) = st.inflight.get_mut(&off) {
                    f.deadline = deadline;
                    n += 1;
                }
            }
        }
        n
    }

    /// Hands records back. Shared only: they are delivered again after
    /// `delay_ms`.
    pub fn nack(&self, ranges: &[Range], delay_ms: u32) -> Result<(), BrokerError> {
        if self.config.mode != SubMode::Shared {
            return Err(BrokerError::invalid("only a shared subscription takes a nack; reposition an ordered one with commit"));
        }
        let due = Instant::now() + Duration::from_millis(delay_ms as u64);
        for r in ranges {
            let Some(p) = self.parts.get(r.partition as usize) else { continue };
            let mut st = p.lock();
            for off in r.offset..r.offset + r.count as u64 {
                if st.inflight.remove(&off).is_some() {
                    if delay_ms == 0 {
                        st.redeliver.push_back(off);
                    } else {
                        st.delayed.push(std::cmp::Reverse((due, off)));
                    }
                }
            }
        }
        self.wake.notify_one();
        Ok(())
    }

    /// Sets an ordered partition's committed offset.
    pub fn commit(&self, partition: u32, offset: u64) -> Result<(), BrokerError> {
        let Some(p) = self.parts.get(partition as usize) else {
            return Err(BrokerError::new(Code::NoPartition, format!("no partition {partition}")));
        };
        if self.config.mode == SubMode::Broadcast {
            return Ok(());
        }
        {
            let mut st = p.lock();
            if st.committed == Some(offset) {
                return Ok(());
            }
            st.committed = Some(offset);
        }
        self.log_record(S_COMMIT, |w| {
            w.u32(partition);
            w.u64(offset)
        });
        Ok(())
    }

    /// Applies a record a peer's subscription of this name logged
    /// (`repl`): a commit, acks or dead letters, a floor. Logged here too.
    pub fn apply_record(&self, kind: u8, body: &[u8]) -> Result<(), BrokerError> {
        let mut r = wire::Reader::new(body);
        let bad = |e: wire::DecodeError| BrokerError::new(Code::Protocol, e.to_string());
        let p = r.u32().map_err(bad)? as usize;
        let Some(part) = self.parts.get(p) else {
            return Err(BrokerError::new(Code::NoPartition, format!("no partition {p}")));
        };
        {
            let mut st = part.lock();
            match kind {
                S_COMMIT => st.committed = Some(r.u64().map_err(bad)?),
                S_FLOOR => {
                    let f = r.u64().map_err(bad)?;
                    st.raise_floor(f);
                }
                S_ACK | S_DEAD => {
                    let off = r.u64().map_err(bad)?;
                    let n = r.u32().map_err(bad)?;
                    for o in off..off + n as u64 {
                        st.inflight.remove(&o);
                        st.set_acked(o);
                    }
                }
                other => return Err(BrokerError::new(Code::Protocol, format!("unknown subscription record {other}"))),
            }
        }
        let body = body.to_vec();
        self.log_record(kind, |w| w.raw(&body));
        Ok(())
    }

    /// Ends the subscription for its members, telling each why.
    pub fn close(&self, why: &str) {
        self.closed.store(true, Ordering::Release);
        let mut m = self.members.lock();
        for member in m.list.drain(..) {
            let mut f = Vec::new();
            wire::frame(&mut f, op::CLOSED, wire::F_PUSH, member.handle, |w| {
                wire::ErrorBody { code: Code::NoSubscription, message: format!("subscription \"{}\" ended: {why}", self.name) }
                    .encode(w)
            });
            let _ = member.out.send(f);
        }
        drop(m);
        self.wake.notify_one();
    }

    pub fn remove_files(&self) {
        if let Some(dir) = &self.dir {
            LogFile::remove(dir, &stem(self.id));
        }
    }

    /// Takes up to `max` records from a shared subscription. With `ack`, they
    /// are acked at once (`BROKER POP`, at most once); without, they are in
    /// flight for `member` until acked, nacked or timed out (AMQP's
    /// `basic.get`). Redeliveries go first, then new records, partition by
    /// partition.
    pub fn take(&self, max: usize, ack: bool, member: u64) -> Result<Vec<Taken>, BrokerError> {
        if self.config.mode != SubMode::Shared {
            return Err(BrokerError::invalid("POP takes from a queue or a shared subscription"));
        }
        let mut out = Vec::new();
        for (pi, p) in self.parts.iter().enumerate() {
            if out.len() >= max {
                break;
            }
            let part = &self.topic.partitions[pi];
            let mut st = p.lock();
            let mut want: Vec<u64> = Vec::new();
            while out.len() + want.len() < max {
                let Some(o) = st.redeliver.pop_front() else { break };
                if !st.is_acked(o) && !st.inflight.contains_key(&o) {
                    want.push(o);
                }
            }
            let mut cursor = st.cursor.max(part.log_start());
            while out.len() + want.len() < max && cursor < part.next_offset() {
                if !st.is_acked(cursor) && !st.inflight.contains_key(&cursor) {
                    want.push(cursor);
                }
                cursor += 1;
            }
            st.cursor = st.cursor.max(cursor);
            want.sort_unstable();
            let mut i = 0;
            while i < want.len() {
                let Ok(r) = part.read(want[i], DELIVER_BYTES) else { break };
                let mut matched = false;
                for b in Batches::trusted(&r.bytes).flatten() {
                    for rec in b.records() {
                        if i < want.len() && rec.offset == want[i] {
                            out.push(Taken {
                                partition: pi as u32,
                                offset: rec.offset,
                                time_us: rec.time_us,
                                key: rec.key.map(<[u8]>::to_vec),
                                value: rec.value.to_vec(),
                                headers: rec.headers().map(|(n, v)| (n.to_vec(), v.to_vec())).collect(),
                            });
                            matched = true;
                            i += 1;
                        }
                    }
                }
                if !matched {
                    i += 1;
                }
            }
            let mut acked = Vec::new();
            if !ack {
                let deadline = Instant::now() + Duration::from_millis(self.config.ack_timeout_ms.max(1) as u64);
                for t in out.iter().filter(|t| t.partition == pi as u32) {
                    st.inflight.insert(t.offset, Inflight { member, deadline });
                    *st.deliveries.entry(t.offset).or_insert(0) += 1;
                }
                drop(st);
                continue;
            }
            for t in out.iter().filter(|t| t.partition == pi as u32) {
                if st.set_acked(t.offset) {
                    acked.push(t.offset);
                }
            }
            drop(st);
            for (off, n) in runs(pi as u32, &acked).into_iter().map(|r| (r.offset, r.count)) {
                self.log_record(S_ACK, |w| {
                    w.u32(pi as u32);
                    w.u64(off);
                    w.u32(n)
                });
            }
            self.acked.fetch_add(acked.len() as u64, Ordering::Relaxed);
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // The log
    // -----------------------------------------------------------------------

    fn log_record(&self, kind: u8, body: impl FnOnce(&mut Writer<'_>)) {
        let Some(log) = &self.log else { return };
        let bytes = rec(body);
        if let Some(b) = self.broker.upgrade() {
            if let Some(r) = super::repl::capturing(b.replication.get()) {
                super::repl::sub_record(r, &self.topic.name, &self.name, kind, &bytes);
            }
        }
        {
            let mut l = log.lock();
            match l.file.frame(kind, &bytes) {
                Ok(f) => l.pending.extend_from_slice(&f),
                Err(e) => log::warn!("broker: subscription \"{}\": {e}", self.name),
            }
        }
        if !self.dirty.swap(true, Ordering::AcqRel) {
            if let Some(f) = self.broker.upgrade().and_then(|b| b.flusher.clone()) {
                let me: Weak<dyn super::flush::Flushable> = self.me.clone();
                f.mark_dirty_dyn(me);
            }
        }
    }

    /// Writes and syncs the log now (shutdown).
    pub fn flush_now(&self) {
        if let Ok(Some(r)) = super::flush::Flushable::write_pending(self) {
            for f in r.files {
                let _ = super::sync_file(&f);
            }
        }
    }

    fn snapshot(&self) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        for (i, p) in self.parts.iter().enumerate() {
            let st = p.lock();
            match self.config.mode {
                SubMode::Ordered => {
                    if let Some(c) = st.committed {
                        out.push((S_COMMIT, rec(|w| {
                            w.u32(i as u32);
                            w.u64(c)
                        })));
                    }
                }
                _ => {
                    out.push((S_FLOOR, rec(|w| {
                        w.u32(i as u32);
                        w.u64(st.floor)
                    })));
                    for (off, n) in st.acked_runs() {
                        out.push((S_ACK, rec(|w| {
                            w.u32(i as u32);
                            w.u64(off);
                            w.u32(n)
                        })));
                    }
                }
            }
        }
        out
    }

    // -----------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------

    /// Ten times a second: ack timeouts, delayed nacks, retention under the
    /// floor, queue TTL and length limits.
    pub fn tick(&self) {
        if self.config.mode != SubMode::Shared || self.is_closed() {
            return;
        }
        let now = Instant::now();
        let mut wake = false;
        for (i, p) in self.parts.iter().enumerate() {
            let mut st = p.lock();
            let start = self.topic.partitions[i].log_start();
            if st.floor < start {
                st.raise_floor(start);
                self.log_record(S_FLOOR, |w| {
                    w.u32(i as u32);
                    w.u64(start)
                });
            }
            let expired: Vec<u64> = st.inflight.iter().filter(|(_, f)| f.deadline <= now).map(|(o, _)| *o).collect();
            for o in expired {
                st.inflight.remove(&o);
                st.redeliver.push_back(o);
                wake = true;
            }
            while let Some(std::cmp::Reverse((due, o))) = st.delayed.peek().copied() {
                if due > now {
                    break;
                }
                st.delayed.pop();
                st.redeliver.push_back(o);
                wake = true;
            }
            drop(st);
            if let Some(q) = &self.topic.queue {
                if q.max_length >= 0 {
                    self.enforce_max_length(i, q.max_length as u64);
                }
            }
        }
        if wake {
            self.wake.notify_one();
        }
    }

    /// Drops the oldest records past a queue's length limit (RabbitMQ's
    /// `drop-head`): they are acked as dead without being delivered.
    fn enforce_max_length(&self, partition: usize, max: u64) {
        let next = self.topic.partitions[partition].next_offset();
        let mut st = self.parts[partition].lock();
        let depth = next.saturating_sub(st.floor).saturating_sub(st.acked_above_floor());
        if depth <= max {
            return;
        }
        let mut excess = depth - max;
        let mut off = st.floor;
        let mut dropped = Vec::new();
        while excess > 0 && off < next {
            if !st.is_acked(off) && !st.inflight.contains_key(&off) {
                st.set_acked(off);
                dropped.push(off);
                excess -= 1;
            }
            off += 1;
        }
        if st.cursor < off {
            st.cursor = st.cursor.max(st.floor);
        }
        drop(st);
        self.dead.fetch_add(dropped.len() as u64, Ordering::Relaxed);
        for o in dropped {
            self.log_record(S_DEAD, |w| {
                w.u32(partition as u32);
                w.u64(o);
                w.u32(1)
            });
        }
    }

    // -----------------------------------------------------------------------
    // Dispatch
    // -----------------------------------------------------------------------

    fn ensure_dispatcher(&self, rt: &tokio::runtime::Handle) {
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(me) = self.me.upgrade() else {
            self.running.store(false, Ordering::Release);
            return;
        };
        rt.spawn(dispatch_loop(me));
    }

    /// Pushes everything it can now. `true` when it pushed anything.
    fn dispatch_once(&self) -> bool {
        match self.config.mode {
            SubMode::Ordered | SubMode::Broadcast => self.dispatch_positions(),
            SubMode::Shared => {
                let mut any = false;
                for p in 0..self.parts.len() {
                    any |= self.dispatch_shared(p as u32);
                }
                any
            }
        }
    }

    /// Ordered and broadcast: each member, each of its partitions, from its
    /// position.
    fn dispatch_positions(&self) -> bool {
        let mut m = self.members.lock();
        let mut any = false;
        for member in m.list.iter_mut() {
            for &p in member.assigned.clone().iter() {
                if member.credits == 0 || member.out.is_full() {
                    break;
                }
                let part = &self.topic.partitions[p as usize];
                let Some(pos) = member.positions.get(&p).copied() else { continue };
                let pos = pos.max(part.log_start());
                if pos >= part.next_offset() {
                    continue;
                }
                let mut frame = Vec::with_capacity(DELIVER_BYTES / 4);
                let start = wire::begin_frame(&mut frame, op::DELIVER, wire::F_PUSH, member.handle);
                frame.extend_from_slice(&p.to_le_bytes());
                frame.extend_from_slice(&1u32.to_le_bytes());
                let range_at = frame.len();
                frame.extend_from_slice(&[0u8; 16]);
                let batches_at = frame.len();
                if let Err(e) = part.read_into(pos, DELIVER_BYTES, &mut frame) {
                    log::warn!("broker: subscription \"{}\" partition {p}: {e}", self.name);
                    continue;
                }
                // Records from `pos` to the frame's end, cut at the credit.
                let mut end = pos;
                for b in Batches::trusted(&frame[batches_at..]).flatten() {
                    end = end.max(b.next_offset());
                }
                let n = (end - pos).min(member.credits);
                if n == 0 {
                    continue;
                }
                let range = Range { partition: p, offset: pos, count: n as u32 };
                frame[range_at..range_at + 4].copy_from_slice(&range.partition.to_le_bytes());
                frame[range_at + 4..range_at + 12].copy_from_slice(&range.offset.to_le_bytes());
                frame[range_at + 12..range_at + 16].copy_from_slice(&range.count.to_le_bytes());
                wire::end_frame(&mut frame, start);
                if member.out.send(frame).is_err() {
                    member.credits = 0;
                    break;
                }
                member.credits -= n;
                member.positions.insert(p, pos + n);
                self.delivered.fetch_add(n, Ordering::Relaxed);
                any = true;
            }
        }
        any
    }

    /// Shared: redeliveries first, then new records from the cursor, to the
    /// members with credit in turn.
    fn dispatch_shared(&self, p: u32) -> bool {
        let part = &self.topic.partitions[p as usize];
        let mut m = self.members.lock();
        if m.list.iter().all(|x| x.credits == 0 || x.out.is_full()) {
            return false;
        }
        let mut st = self.parts[p as usize].lock();
        let ack_timeout = Duration::from_millis(self.config.ack_timeout_ms.max(1) as u64);
        let ttl_us = self.topic.queue.as_ref().map_or(-1, |q| q.message_ttl_ms).saturating_mul(1000);
        let now_us = super::now_us();
        let mut any = false;
        let mut dead: Vec<u64> = Vec::new();
        let mut expired: Vec<u64> = Vec::new();

        // Redeliveries, batch by batch.
        while let Some(&first) = st.redeliver.front() {
            if m.list.iter().all(|x| x.credits == 0 || x.out.is_full()) {
                break;
            }
            if st.is_acked(first) || first < part.log_start() {
                st.redeliver.pop_front();
                continue;
            }
            let mut bytes = Vec::new();
            if part.read_into(first, 1, &mut bytes).is_err() || bytes.is_empty() {
                st.redeliver.pop_front();
                continue;
            }
            let Some(Ok(b)) = Batches::trusted(&bytes).next() else {
                st.redeliver.pop_front();
                continue;
            };
            let (lo, hi) = (b.base_offset(), b.next_offset());
            // Every pending redelivery inside this batch.
            let mut offs: Vec<u64> = Vec::new();
            st.redeliver.retain(|&o| {
                if o >= lo && o < hi {
                    offs.push(o);
                    false
                } else {
                    true
                }
            });
            offs.sort_unstable();
            offs.dedup();
            let mut give: Vec<u64> = Vec::new();
            for o in offs {
                if st.is_acked(o) {
                    continue;
                }
                let n = st.deliveries.entry(o).or_insert(1);
                if self.config.max_deliveries > 0 && *n >= self.config.max_deliveries {
                    dead.push(o);
                    continue;
                }
                give.push(o);
            }
            let mut left: VecDeque<u64> = give.into();
            while !left.is_empty() {
                let Some(mi) = next_member(&mut *m) else {
                    // No credit left: back to the front, in order.
                    for o in left.into_iter().rev() {
                        st.redeliver.push_front(o);
                    }
                    break;
                };
                let member = &mut m.list[mi];
                let take = (member.credits as usize).min(left.len());
                let mine: Vec<u64> = left.drain(..take).collect();
                let ranges = runs(p, &mine);
                // This delivery's number: one past those already made (a
                // scheduled record's entry is 0: its first).
                let counts: Vec<wire::DeliveryCount> = if member.counts {
                    mine.iter()
                        .map(|o| wire::DeliveryCount { partition: p, offset: *o, deliveries: st.deliveries.get(o).copied().unwrap_or(1) + 1 })
                        .filter(|c| c.deliveries > 1)
                        .collect()
                } else {
                    Vec::new()
                };
                if !send_deliver(member, p, &ranges, &counts, &bytes) {
                    for o in mine.into_iter().rev() {
                        st.redeliver.push_front(o);
                    }
                    member.credits = 0;
                    continue;
                }
                member.credits -= mine.len() as u64;
                let deadline = Instant::now() + ack_timeout;
                for o in &mine {
                    st.inflight.insert(*o, Inflight { member: member.id, deadline });
                    *st.deliveries.entry(*o).or_insert(1) += 1;
                }
                self.redelivered.fetch_add(mine.len() as u64, Ordering::Relaxed);
                self.delivered.fetch_add(mine.len() as u64, Ordering::Relaxed);
                any = true;
            }
        }

        // New records.
        loop {
            if m.list.iter().all(|x| x.credits == 0 || x.out.is_full()) {
                break;
            }
            let start = part.log_start();
            if st.cursor < start {
                st.raise_floor(start);
            }
            let next = part.next_offset();
            if st.cursor >= next || st.cursor - st.floor >= MAX_WINDOW {
                break;
            }
            let from = st.cursor;
            let mut bytes = Vec::new();
            if let Err(e) = part.read_into(from, DELIVER_BYTES, &mut bytes) {
                log::warn!("broker: subscription \"{}\" partition {p}: {e}", self.name);
                break;
            }
            if bytes.is_empty() {
                break;
            }
            // The deliverable records in these bytes, in order.
            let mut offs: Vec<u64> = Vec::new();
            let mut end = from;
            for b in Batches::trusted(&bytes).flatten() {
                end = end.max(b.next_offset());
                if ttl_us >= 0 && b.max_time() + ttl_us < now_us {
                    for o in b.base_offset().max(from)..b.next_offset() {
                        if !st.is_acked(o) {
                            expired.push(o);
                        }
                    }
                    continue;
                }
                let raw = b.codec() == rasterizeddb_mq::batch::CODEC_NONE;
                let headers = b.flags() & rasterizeddb_mq::batch::F_HEADERS != 0;
                if raw && (ttl_us >= 0 || headers) {
                    for r in b.records() {
                        if r.offset < from || st.is_acked(r.offset) || st.inflight.contains_key(&r.offset) {
                            continue;
                        }
                        if ttl_us >= 0 && r.time_us + ttl_us < now_us {
                            expired.push(r.offset);
                            continue;
                        }
                        // Scheduled delivery: held back until its time, in
                        // memory; after a restart it is read and held again.
                        if let Some(at) = r.header(wire::DELIVER_AT_HEADER).and_then(|v| <[u8; 8]>::try_from(v).ok()) {
                            let at = i64::from_le_bytes(at);
                            if at > now_us {
                                let due = Instant::now() + Duration::from_micros((at - now_us) as u64);
                                st.delayed.push(std::cmp::Reverse((due, r.offset)));
                                // Not delivered yet: its first delivery is
                                // not a redelivery against max_deliveries.
                                st.deliveries.insert(r.offset, 0);
                                continue;
                            }
                        }
                        offs.push(r.offset);
                    }
                } else {
                    for o in b.base_offset().max(from)..b.next_offset() {
                        if !st.is_acked(o) {
                            offs.push(o);
                        }
                    }
                }
            }
            let mut handed = 0usize;
            let total = offs.len();
            while handed < total {
                let Some(mi) = next_member(&mut *m) else { break };
                let member = &mut m.list[mi];
                let take = (member.credits as usize).min(total - handed);
                let mine = &offs[handed..handed + take];
                let ranges = runs(p, mine);
                if !send_deliver(member, p, &ranges, &[], &bytes) {
                    member.credits = 0;
                    continue;
                }
                member.credits -= take as u64;
                let deadline = Instant::now() + ack_timeout;
                for o in mine {
                    st.inflight.insert(*o, Inflight { member: member.id, deadline });
                }
                self.delivered.fetch_add(take as u64, Ordering::Relaxed);
                handed += take;
                any = true;
            }
            // The cursor moves past what was handed out (and what was
            // skipped as acked or expired); the rest waits for credit.
            st.cursor = if handed == total { end } else { offs[handed] };
            if handed < total {
                break;
            }
        }
        drop(st);
        drop(m);
        if !expired.is_empty() || !dead.is_empty() {
            self.bury(p, &expired, false);
            self.bury(p, &dead, true);
        }
        any
    }

    /// Records that will not be delivered again: copied to the dead-letter
    /// topic when there is one, then acked as dead.
    fn bury(&self, p: u32, offsets: &[u64], max_deliveries: bool) {
        if offsets.is_empty() {
            return;
        }
        let part = &self.topic.partitions[p as usize];
        if !self.config.dead_letter.is_empty() {
            if let Some(broker) = self.broker.upgrade() {
                let reason: &[u8] = if max_deliveries { b"delivery_limit" } else { b"expired" };
                let mut builder = BatchBuilder::new();
                for &o in offsets {
                    let Ok(r) = part.read(o, 1) else { continue };
                    for b in Batches::trusted(&r.bytes).flatten() {
                        for rec in b.records().filter(|x| x.offset == o) {
                            let mut headers: Vec<(&[u8], &[u8])> = rec.headers().collect();
                            headers.push((b"x-death-reason", reason));
                            headers.push((b"x-death-topic", self.topic.name.as_bytes()));
                            headers.push((b"x-death-subscription", self.name.as_bytes()));
                            let _ = builder.push(rec.key, rec.value, &headers);
                        }
                    }
                }
                if !builder.is_empty() {
                    let mut bytes = builder.finish();
                    // `@x:<exchange>:<routing key>` dead-letters through an
                    // exchange (AMQP's x-dead-letter-exchange); anything
                    // else names a topic.
                    let r = match self.config.dead_letter.strip_prefix("@x:") {
                        Some(rest) => {
                            let (x, key) = rest.split_once(':').unwrap_or((rest, ""));
                            broker.publish(x, key, &mut bytes, false).map(|_| ())
                        }
                        None => match broker.topic(&self.config.dead_letter) {
                            Some(dlq) => broker.produce(&dlq, wire::BY_KEY, &mut bytes, false).map(|_| ()),
                            None => Ok(()),
                        },
                    };
                    if let Err(e) = r {
                        log::warn!("broker: dead-lettering to \"{}\" failed: {e}", self.config.dead_letter);
                    }
                }
            }
        }
        let mut st = self.parts[p as usize].lock();
        let mut done = Vec::new();
        for &o in offsets {
            st.inflight.remove(&o);
            st.deliveries.remove(&o);
            if st.set_acked(o) {
                done.push(o);
            }
        }
        drop(st);
        self.dead.fetch_add(done.len() as u64, Ordering::Relaxed);
        for o in done {
            self.log_record(S_DEAD, |w| {
                w.u32(p);
                w.u64(o);
                w.u32(1)
            });
        }
    }
}

/// The next member with credit, round robin.
fn next_member(m: &mut Members) -> Option<usize> {
    let n = m.list.len();
    for k in 0..n {
        let i = (m.rr + k) % n;
        if m.list[i].credits > 0 && !m.list[i].out.is_full() {
            m.rr = (i + 1) % n;
            return Some(i);
        }
    }
    None
}

/// Sorted offsets as ranges.
fn runs(p: u32, offs: &[u64]) -> Vec<Range> {
    let mut out: Vec<Range> = Vec::new();
    for &o in offs {
        match out.last_mut() {
            Some(r) if r.offset + r.count as u64 == o => r.count += 1,
            _ => out.push(Range { partition: p, offset: o, count: 1 }),
        }
    }
    out
}

fn send_deliver(member: &Member, p: u32, ranges: &[Range], counts: &[wire::DeliveryCount], batches: &[u8]) -> bool {
    let mut frame = Vec::with_capacity(wire::FRAME_HEADER + 12 + ranges.len() * 16 + counts.len() * 16 + batches.len());
    let flags = if counts.is_empty() { wire::F_PUSH } else { wire::F_PUSH | wire::F_COUNTS };
    let start = wire::begin_frame(&mut frame, op::DELIVER, flags, member.handle);
    {
        let mut w = Writer(&mut frame);
        w.u32(p);
        w.u32(ranges.len() as u32);
        for r in ranges {
            w.u32(r.partition);
            w.u64(r.offset);
            w.u32(r.count);
        }
        if !counts.is_empty() {
            wire::encode_counts(&mut w, counts);
        }
        w.raw(batches);
    }
    wire::end_frame(&mut frame, start);
    member.out.send(frame).is_ok()
}

fn rec(f: impl FnOnce(&mut Writer<'_>)) -> Vec<u8> {
    let mut v = Vec::new();
    f(&mut Writer(&mut v));
    v
}

/// Where a subscription with no stored position starts on a partition.
pub fn resolve_start(topic: &Topic, p: u32, start: Start) -> u64 {
    let part = &topic.partitions[p as usize];
    match start {
        Start::Earliest => part.log_start(),
        Start::Latest => part.next_offset(),
        Start::Offset(o) => o.clamp(part.log_start(), part.next_offset()),
        Start::Time(t) => part.offset_for_time(t).unwrap_or_else(|_| part.next_offset()),
    }
}

async fn dispatch_loop(sub: Arc<Subscription>) {
    let mut rxs: Vec<tokio::sync::watch::Receiver<u64>> =
        sub.topic.partitions.iter().map(|p| p.subscribe_appends()).collect();
    loop {
        if sub.is_closed() || sub.members.lock().list.is_empty() {
            sub.running.store(false, Ordering::Release);
            // A member that joined between the check and the store finds
            // `running` false and starts another dispatcher; one that joined
            // before it is seen here.
            if sub.is_closed() || sub.members.lock().list.is_empty() {
                return;
            }
            if sub.running.swap(true, Ordering::AcqRel) {
                return;
            }
        }
        if sub.dispatch_once() {
            tokio::task::yield_now().await;
            continue;
        }
        let changed = futures::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.changed())));
        tokio::select! {
            _ = sub.wake.notified() => {}
            (r, _, _) = changed => {
                if r.is_err() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

impl super::flush::Flushable for Subscription {
    fn write_pending(&self) -> io::Result<Option<WriteRound>> {
        self.dirty.store(false, Ordering::Release);
        let Some(log) = &self.log else { return Ok(None) };
        let mut l = log.lock();
        if l.pending.is_empty() {
            return Ok(None);
        }
        let pending = std::mem::take(&mut l.pending);
        l.file.append_raw(&pending)?;
        if l.file.len() > SUB_COMPACT_AT {
            drop(l);
            let snap = self.snapshot();
            let mut l = log.lock();
            // Records logged since the snapshot are in `pending` and are
            // written after it.
            l.file.compact(&snap)?;
            return Ok(Some(WriteRound { files: vec![l.file.shared_file()], upto: 0, dir: None }));
        }
        Ok(Some(WriteRound { files: vec![l.file.shared_file()], upto: 0, dir: None }))
    }
    fn mark_durable(&self, _upto: u64) {}
    fn durability(&self) -> Durability {
        Durability::Async
    }
    fn poison(&self, e: &io::Error) {
        log::error!("broker: subscription \"{}\" log write failed: {e}", self.name);
    }
}
