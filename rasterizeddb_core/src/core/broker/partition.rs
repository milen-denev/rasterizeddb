//! One partition of a topic: an append-only log of batches (docs/broker.md
//! §2.3–2.4).
//!
//! # The append path
//!
//! [`Partition::append`] takes a batch whose records were already checked
//! outside any lock. Under the partition's mutex it does four things: runs the
//! idempotence check, stamps the offset and time (a 52-byte CRC), copies the
//! batch into the newest [`Chunk`], and notes it in the active segment's
//! sparse index. Nothing on that path makes a syscall. The flusher thread
//! writes chunks to their segment files later, with one `pwrite` per chunk
//! run, and one `fdatasync` per file per round for `sync` topics. That is the
//! group commit: every append since the last round is covered by the same
//! sync.
//!
//! # Watermarks
//!
//! - `next`: the offset the next append gets.
//! - `durable`: offsets below it are as durable as the topic promises
//!   (synced for `sync`, written for `async`, appended for `memory`).
//! - `start`: the first offset retention has kept.
//!
//! Waiters subscribe to the `appended` and `durable` watch channels. They
//! work across runtimes, so a fetch parked on one I/O thread is woken by an
//! append made on another.
//!
//! # Reads
//!
//! A read at an offset still held in a chunk copies straight from the chunk,
//! after the mutex is dropped (see [`super::chunk`]). Anything older is read
//! with `pread` from its segment file. Chunks are dropped oldest first, once
//! written, when they pass the partition's memory budget. An offset that is
//! not in a chunk is therefore always in a file.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;
use rasterizeddb_mq::batch::{self, BatchView, HEADER};
use rasterizeddb_mq::wire::{Code, Durability};
use tokio::sync::watch;

use super::chunk::Chunk;
use super::segment::{self, Segment};
use super::{BrokerError, now_us};

/// Bytes per chunk unless a batch is bigger.
pub const CHUNK_BYTES: usize = 1 << 20;
/// Chunks kept in memory, written or not, before the oldest written one goes.
pub const TAIL_BYTES: usize = 8 << 20;
/// Batches an idempotent producer's history keeps, as Kafka's five.
const PRODUCER_HISTORY: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub base_offset: u64,
    pub next_offset: u64,
    pub duplicate: bool,
}

/// What a read returns: whole stored batches, and the watermarks it saw.
#[derive(Debug, Default)]
pub struct ReadOut {
    pub bytes: Vec<u8>,
    pub next_offset: u64,
    pub log_start: u64,
}

struct ProducerState {
    epoch: u16,
    /// `(base_sequence, count, base_offset)` of its last batches.
    history: VecDeque<(u32, u32, u64)>,
}

struct State {
    /// Oldest first; the last is the active segment. Empty for `memory`.
    segments: VecDeque<Segment>,
    /// The newest bytes, oldest first; the last is open for appends.
    chunks: VecDeque<Arc<Chunk>>,
    /// Bytes of chunk capacity held.
    chunk_bytes: usize,
    producers: HashMap<u64, ProducerState>,
    segment_bytes: u64,
    /// The memory budget: `TAIL_BYTES` for files, `memory_bytes` for `memory`.
    budget: usize,
}

/// One partition.
pub struct Partition {
    pub topic: u32,
    pub index: u32,
    pub durability: Durability,
    dir: Option<PathBuf>,
    state: Mutex<State>,
    next: AtomicU64,
    start: AtomicU64,
    durable: AtomicU64,
    appended_tx: watch::Sender<u64>,
    durable_tx: watch::Sender<u64>,
    /// On the flusher's list, or about to be.
    pub(super) dirty: AtomicBool,
    /// An I/O error ended this partition's writes until a restart.
    poisoned: AtomicBool,
    /// Bytes of segments, for size retention.
    pub(super) bytes: AtomicU64,
    pub(super) appends: AtomicU64,
    pub(super) records: AtomicU64,
    /// The flusher this partition hands its writes to.
    pub(super) flusher: Option<Arc<super::flush::Flusher>>,
    /// Bytes in chunks not yet handed to the flusher's write: what it has to do.
    pending: AtomicUsize,
    /// Itself, for the flusher's dirty list.
    me: std::sync::Weak<Partition>,
}

impl Partition {
    /// A new, empty partition. Its directory is created by the caller.
    pub fn create(
        topic: u32,
        index: u32,
        dir: Option<PathBuf>,
        config: &rasterizeddb_mq::wire::TopicConfig,
        flusher: Option<Arc<super::flush::Flusher>>,
    ) -> Arc<Partition> {
        Self::with_state(topic, index, dir, config, flusher, VecDeque::new(), HashMap::new(), 0, 0)
    }

    #[allow(clippy::too_many_arguments)]
    fn with_state(
        topic: u32,
        index: u32,
        dir: Option<PathBuf>,
        config: &rasterizeddb_mq::wire::TopicConfig,
        flusher: Option<Arc<super::flush::Flusher>>,
        segments: VecDeque<Segment>,
        producers: HashMap<u64, ProducerState>,
        start: u64,
        next: u64,
    ) -> Arc<Partition> {
        let memory = config.durability == Durability::Memory || dir.is_none();
        let durability = if dir.is_none() { Durability::Memory } else { config.durability };
        let bytes = segments.iter().map(|s| s.len).sum();
        Arc::new_cyclic(|me| Partition {
            topic,
            index,
            durability,
            dir: if memory { None } else { dir },
            state: Mutex::new(State {
                segments,
                chunks: VecDeque::new(),
                chunk_bytes: 0,
                producers,
                segment_bytes: config.segment_bytes.clamp(1 << 20, 4 << 30),
                budget: if memory { config.memory_bytes.max(CHUNK_BYTES as u64) as usize } else { TAIL_BYTES },
            }),
            next: AtomicU64::new(next),
            start: AtomicU64::new(start),
            durable: AtomicU64::new(next),
            appended_tx: watch::Sender::new(next),
            durable_tx: watch::Sender::new(next),
            dirty: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            bytes: AtomicU64::new(bytes),
            appends: AtomicU64::new(0),
            records: AtomicU64::new(0),
            flusher: if memory { None } else { flusher },
            pending: AtomicUsize::new(0),
            me: me.clone(),
        })
    }

    pub(super) fn me(&self) -> Option<std::sync::Weak<dyn super::flush::Flushable>> {
        let me: std::sync::Weak<dyn super::flush::Flushable> = self.me.clone();
        Some(me)
    }

    /// Opens a partition from its directory: every segment, the newest
    /// scanned and cut at a torn tail.
    pub fn open(
        topic: u32,
        index: u32,
        dir: PathBuf,
        config: &rasterizeddb_mq::wire::TopicConfig,
        flusher: Option<Arc<super::flush::Flusher>>,
    ) -> io::Result<Arc<Partition>> {
        if config.durability == Durability::Memory {
            // A memory topic keeps nothing across a restart; clear what a
            // topic that was altered to memory left behind.
            for base in segment::list(&dir)? {
                segment::remove(&dir, base);
            }
            return Ok(Self::create(topic, index, Some(dir), config, flusher));
        }
        let bases = segment::list(&dir)?;
        let mut segments = VecDeque::with_capacity(bases.len());
        let mut producers = HashMap::new();
        for (i, &base) in bases.iter().enumerate() {
            let newest = i + 1 == bases.len();
            let opened = segment::open(&dir, base, newest)?;
            if newest {
                for b in opened.scanned {
                    if b.producer_id != 0 {
                        let st = producers.entry(b.producer_id).or_insert(ProducerState {
                            epoch: b.epoch,
                            history: VecDeque::new(),
                        });
                        if b.epoch > st.epoch {
                            st.epoch = b.epoch;
                            st.history.clear();
                        }
                        st.history.push_back((b.base_sequence, b.count, b.base_offset));
                        if st.history.len() > PRODUCER_HISTORY {
                            st.history.pop_front();
                        }
                    }
                }
            }
            let mut seg = opened.segment;
            if !newest {
                // Closed segments are opened again when read.
                seg.file = None;
            }
            segments.push_back(seg);
        }
        // A segment emptied by a cut, other than the newest, holds nothing.
        let start = segments.front().map_or(0, |s| s.base);
        let next = segments.back().map_or(0, |s| s.next);
        Ok(Self::with_state(topic, index, Some(dir), config, flusher, segments, producers, start, next))
    }

    #[inline]
    pub fn next_offset(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }
    #[inline]
    pub fn log_start(&self) -> u64 {
        self.start.load(Ordering::Acquire)
    }
    #[inline]
    pub fn durable_offset(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }
    pub fn subscribe_appends(&self) -> watch::Receiver<u64> {
        self.appended_tx.subscribe()
    }
    pub fn subscribe_durable(&self) -> watch::Receiver<u64> {
        self.durable_tx.subscribe()
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Appends one checked batch (its records' CRC and layout were verified by
    /// the caller, or it was built in this process). `batch` is stamped in
    /// place.
    pub fn append(&self, batch: &mut [u8]) -> Result<Appended, BrokerError> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(BrokerError::new(Code::Storage, "partition stopped after a write error; see the server log"));
        }
        let len = batch.len();
        let now = now_us();
        let mut st = self.state.lock();
        let base = self.next.load(Ordering::Relaxed);
        let view = BatchView::trusted(batch);
        let producer = view.producer_id();
        if producer != 0 {
            if let Some(dup) = check_producer(&mut st.producers, producer, view.epoch(), view.base_sequence(), view.count())? {
                return Ok(Appended { base_offset: dup, next_offset: base, duplicate: true });
            }
        }
        let stamped = batch::stamp_trusted(batch, base, now);
        let count = stamped.count;
        let next = base + count as u64;
        let base_time = BatchView::trusted(batch).base_time();
        let memory = self.dir.is_none();

        if !memory {
            let roll = match st.segments.back() {
                None => true,
                Some(s) => s.len > 0 && s.len + len as u64 > st.segment_bytes,
            };
            if roll {
                let dir = self.dir.as_deref().expect("file partition");
                st.segments.push_back(Segment::new(dir, base));
            }
        }
        let (seg_base, seg_pos) = st.segments.back().map_or((0, 0), |s| (s.base, s.len));
        let need_chunk = match st.chunks.back() {
            None => true,
            Some(c) => c.room() < len || (!memory && (c.segment_base != seg_base)),
        };
        if need_chunk {
            let chunk = Arc::new(Chunk::new(CHUNK_BYTES.max(len), base, seg_base, seg_pos));
            st.chunk_bytes += chunk.capacity();
            st.chunks.push_back(chunk);
        }
        let chunk = st.chunks.back().expect("a chunk");
        // SAFETY: the partition mutex is held, and `room() >= len` above.
        unsafe { chunk.append(batch) };
        if let Some(seg) = st.segments.back_mut() {
            seg.note_append(base, count, len as u64, base_time, stamped.max_time);
        }
        if producer != 0 {
            let p = st.producers.entry(producer).or_insert(ProducerState { epoch: stamped.epoch, history: VecDeque::new() });
            p.history.push_back((stamped.base_sequence, count, base));
            if p.history.len() > PRODUCER_HISTORY {
                p.history.pop_front();
            }
        }
        self.next.store(next, Ordering::Release);
        self.trim_chunks(&mut st);
        drop(st);

        self.appends.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(count as u64, Ordering::Relaxed);
        if memory {
            self.durable.store(next, Ordering::Release);
            self.durable_tx.send_replace(next);
        } else {
            self.bytes.fetch_add(len as u64, Ordering::Relaxed);
            self.pending.fetch_add(len, Ordering::Relaxed);
            if self.durability == Durability::Async {
                // Durable for an async topic means written; the flusher still
                // writes it, and syncs on its own clock.
            }
            if !self.dirty.swap(true, Ordering::AcqRel) {
                if let Some(f) = &self.flusher {
                    f.mark_dirty(self);
                }
            }
        }
        self.appended_tx.send_replace(next);
        Ok(Appended { base_offset: base, next_offset: next, duplicate: false })
    }

    /// Drops the oldest chunks past the budget: once written, for a file
    /// partition; for a memory one, whatever is oldest, which moves `start`.
    fn trim_chunks(&self, st: &mut State) {
        while st.chunk_bytes > st.budget && st.chunks.len() > 1 {
            let front = st.chunks.front().expect("non-empty");
            if self.dir.is_some() && !front_written(st) {
                break;
            }
            let cap = front.capacity();
            st.chunks.pop_front();
            st.chunk_bytes -= cap;
            if self.dir.is_none() {
                let start = st.chunks.front().map_or(self.next.load(Ordering::Relaxed), |c| c.first_offset);
                self.start.store(start, Ordering::Release);
            }
        }
    }

    /// Up to `max_bytes` of whole batches from `offset` on. Always at least
    /// one batch when there is one, however large. An offset before the log's
    /// start is [`Code::OffsetOutOfRange`]; one at or past `next` is empty.
    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<ReadOut, BrokerError> {
        let mut out = ReadOut::default();
        let (next, start) = self.read_into(offset, max_bytes, &mut out.bytes)?;
        out.next_offset = next;
        out.log_start = start;
        Ok(out)
    }

    /// [`Partition::read`], appending the batches to `buf` (a frame being
    /// built), and returning `(next_offset, log_start)` as it saw them.
    pub fn read_into(&self, offset: u64, max_bytes: usize, buf: &mut Vec<u8>) -> Result<(u64, u64), BrokerError> {
        let next = self.next_offset();
        let start = self.log_start();
        let base = buf.len();
        if offset >= next {
            return Ok((next, start));
        }
        if offset < start {
            return Err(BrokerError::new(
                Code::OffsetOutOfRange,
                format!("offset {offset} is before the log's start {start}"),
            ));
        }
        let max_bytes = max_bytes.max(HEADER);
        enum Source {
            Chunks(Vec<Arc<Chunk>>),
            File { file: Arc<File>, pos: u64, end: u64 },
        }
        let source = {
            let mut st = self.state.lock();
            match st.chunks.iter().rposition(|c| c.first_offset <= offset) {
                Some(i) => Source::Chunks(st.chunks.iter().skip(i).cloned().collect()),
                None => {
                    let i = match st.segments.partition_point(|s| s.base <= offset) {
                        0 => 0,
                        i => i - 1,
                    };
                    let Some(seg) = st.segments.get_mut(i) else {
                        return Ok((next, start));
                    };
                    let pos = seg.position_for(offset);
                    let end = seg.written;
                    let file = seg.file().map_err(BrokerError::io)?;
                    Source::File { file, pos, end }
                }
            }
        };
        match source {
            Source::Chunks(chunks) => {
                for chunk in chunks {
                    let bytes = chunk.bytes();
                    let mut at = 0;
                    while at + HEADER <= bytes.len() {
                        let v = BatchView::trusted(&bytes[at..]);
                        let len = v.len();
                        if v.next_offset() > offset {
                            if buf.len() > base && buf.len() - base + len > max_bytes {
                                return Ok((next, start));
                            }
                            buf.extend_from_slice(&bytes[at..at + len]);
                        }
                        at += len;
                    }
                }
            }
            Source::File { file, pos, end } => {
                let mut want = max_bytes;
                loop {
                    let raw = segment::read_at(&file, pos, end, want).map_err(BrokerError::io)?;
                    let mut at = 0;
                    let mut first_short = None;
                    while at + HEADER <= raw.len() {
                        let len = batch::claimed_len(&raw[at..]).unwrap_or(0);
                        if len < HEADER {
                            return Err(BrokerError::new(Code::Storage, "a segment holds a damaged batch"));
                        }
                        if at + len > raw.len() {
                            first_short = Some(len);
                            break;
                        }
                        let v = BatchView::trusted(&raw[at..]);
                        if v.next_offset() > offset {
                            if buf.len() > base && buf.len() - base + len > max_bytes {
                                break;
                            }
                            buf.extend_from_slice(&raw[at..at + len]);
                        }
                        at += len;
                    }
                    match first_short {
                        // The first wanted batch is larger than what was read.
                        Some(len) if buf.len() == base && (pos + raw.len() as u64) < end => {
                            want = (at + len).max(want * 2);
                        }
                        _ => break,
                    }
                }
            }
        }
        Ok((next, start))
    }

    /// The first offset whose batch is at or after `time_us`, or `next` when
    /// none is.
    pub fn offset_for_time(&self, time_us: i64) -> Result<u64, BrokerError> {
        let start = self.log_start();
        // Find a segment whose max time reaches it, then read forward.
        let from = {
            let st = self.state.lock();
            if st.segments.is_empty() {
                start
            } else {
                let i = st.segments.iter().position(|s| s.max_time >= time_us);
                match i {
                    None => return Ok(self.next_offset()),
                    Some(i) => {
                        let s = &st.segments[i];
                        let pos = s.position_for_time(time_us);
                        s.index.iter().rev().find(|e| e.pos as u64 <= pos).map_or(s.base, |e| s.base + e.rel as u64)
                    }
                }
            }
        };
        let mut at = from.max(start);
        loop {
            let r = self.read(at, 1 << 20)?;
            if r.bytes.is_empty() {
                return Ok(r.next_offset);
            }
            for b in batch::Batches::trusted(&r.bytes) {
                let Ok(v) = b else { break };
                if v.max_time() >= time_us {
                    for rec in v.records() {
                        if rec.time_us >= time_us && rec.offset >= at {
                            return Ok(rec.offset);
                        }
                    }
                    if v.codec() != batch::CODEC_NONE {
                        return Ok(v.base_offset().max(at));
                    }
                }
                at = v.next_offset();
            }
        }
    }

    // -----------------------------------------------------------------------
    // The flusher's side
    // -----------------------------------------------------------------------

    /// Writes every chunk byte not yet in its file (one `pwrite` per chunk
    /// run) and the new index entries. Returns the files a sync must cover
    /// and the offset the write reached. Run by the flusher only.
    pub(super) fn write_pending(&self) -> io::Result<Option<WriteRound>> {
        self.dirty.store(false, Ordering::Release);
        if self.dir.is_none() || self.poisoned.load(Ordering::Relaxed) {
            return Ok(None);
        }
        struct Job {
            chunk: Arc<Chunk>,
            from: usize,
            to: usize,
            file: Arc<File>,
            pos: u64,
            new_file: bool,
        }
        let (jobs, upto) = {
            let mut st = self.state.lock();
            let upto = self.next.load(Ordering::Relaxed);
            let mut jobs = Vec::new();
            let chunks: Vec<Arc<Chunk>> = st.chunks.iter().cloned().collect();
            for chunk in chunks {
                let to = chunk.len();
                let from = chunk.flushed();
                if from >= to {
                    continue;
                }
                let seg = st
                    .segments
                    .iter_mut()
                    .find(|s| s.base == chunk.segment_base)
                    .expect("a chunk's segment is held while the chunk is");
                let new_file = seg.file.is_none() && seg.written == 0;
                let file = seg.file()?;
                jobs.push(Job { chunk: chunk.clone(), from, to, file, pos: chunk.file_pos + from as u64, new_file });
            }
            (jobs, upto)
        };
        if jobs.is_empty() {
            return Ok(None);
        }
        let mut files: Vec<Arc<File>> = Vec::new();
        let mut new_files = false;
        for job in &jobs {
            use crate::core::pos_io::FileExt;
            let bytes = &job.chunk.bytes()[job.from..job.to];
            if let Err(e) = job.file.write_all_at(bytes, job.pos) {
                self.poison(&e);
                return Err(e);
            }
            new_files |= job.new_file;
            if !files.iter().any(|f| Arc::ptr_eq(f, &job.file)) {
                files.push(job.file.clone());
            }
        }
        {
            let mut st = self.state.lock();
            let mut written_bytes = 0;
            for job in &jobs {
                job.chunk.set_flushed(job.to);
                written_bytes += job.to - job.from;
                if let Some(seg) = st.segments.iter_mut().find(|s| s.base == job.chunk.segment_base) {
                    seg.written = seg.written.max(job.pos + (job.to - job.from) as u64);
                }
            }
            for seg in st.segments.iter_mut() {
                if let Err(e) = seg.write_index() {
                    log::warn!("broker: could not write {}: {e}", seg.idx_path().display());
                }
            }
            // Closed segments give their descriptors back once written.
            let n = st.segments.len();
            for seg in st.segments.iter_mut().take(n.saturating_sub(1)) {
                if seg.written >= seg.len {
                    seg.file = None;
                }
            }
            self.pending.fetch_sub(written_bytes.min(self.pending.load(Ordering::Relaxed)), Ordering::Relaxed);
            self.trim_chunks(&mut st);
        }
        if self.durability == Durability::Async {
            self.mark_durable(upto);
        }
        Ok(Some(WriteRound { files, upto, dir: new_files.then(|| self.dir.clone()).flatten() }))
    }

    /// Notes that everything below `upto` is durable, and wakes its waiters.
    pub(super) fn mark_durable(&self, upto: u64) {
        let prev = self.durable.fetch_max(upto, Ordering::AcqRel);
        if upto > prev {
            self.durable_tx.send_replace(upto);
        }
    }

    pub(super) fn poison(&self, e: &io::Error) {
        if !self.poisoned.swap(true, Ordering::AcqRel) {
            log::error!(
                "broker: topic {} partition {}: write failed ({e}); the partition takes no more appends until a restart",
                self.topic,
                self.index
            );
        }
    }

    /// Removes whole segments that retention no longer keeps, never the
    /// active one. Returns the bytes removed.
    pub(super) fn apply_retention(&self, retention_ms: i64, retention_bytes: i64) -> u64 {
        let Some(dir) = self.dir.clone() else {
            return 0;
        };
        let now = now_us();
        let mut removed = Vec::new();
        {
            let mut st = self.state.lock();
            let mut total: u64 = st.segments.iter().map(|s| s.len).sum();
            while st.segments.len() > 1 {
                let s = st.segments.front().expect("non-empty");
                let old = retention_ms >= 0 && s.max_time != i64::MIN && now - s.max_time > retention_ms.saturating_mul(1000);
                let big = retention_bytes >= 0 && total > retention_bytes as u64;
                // Only a segment wholly written goes: its chunks, if any are
                // still held, are flushed, and go with it below.
                if !(old || big) || s.written < s.len {
                    break;
                }
                let s = st.segments.pop_front().expect("non-empty");
                total -= s.len;
                removed.push((s.base, s.len));
            }
            if !removed.is_empty() {
                let start = st.segments.front().map_or(self.next.load(Ordering::Relaxed), |s| s.base);
                self.start.store(start, Ordering::Release);
                let gone: Vec<u64> = removed.iter().map(|(b, _)| *b).collect();
                let mut freed = 0;
                st.chunks.retain(|c| {
                    let keep = !gone.contains(&c.segment_base);
                    if !keep {
                        freed += c.capacity();
                    }
                    keep
                });
                st.chunk_bytes -= freed;
            }
        }
        let mut bytes = 0;
        for (base, len) in removed {
            segment::remove(&dir, base);
            bytes += len;
        }
        self.bytes.fetch_sub(bytes.min(self.bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
        bytes
    }

    /// Bytes appended and not yet written.
    pub fn pending_bytes(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    pub fn segment_count(&self) -> usize {
        self.state.lock().segments.len()
    }
}

fn front_written(st: &State) -> bool {
    st.chunks.front().is_some_and(|c| c.flushed() >= c.len())
}

/// The idempotence check: `Some(offset)` for a batch already appended there.
fn check_producer(
    producers: &mut HashMap<u64, ProducerState>,
    producer: u64,
    epoch: u16,
    seq: u32,
    count: u32,
) -> Result<Option<u64>, BrokerError> {
    let Some(st) = producers.get_mut(&producer) else {
        return Ok(None);
    };
    if epoch < st.epoch {
        return Err(BrokerError::new(
            Code::Fenced,
            format!("producer {producer} epoch {epoch} is older than {}", st.epoch),
        ));
    }
    if epoch > st.epoch {
        st.epoch = epoch;
        st.history.clear();
        return Ok(None);
    }
    if let Some(&(_, _, offset)) = st.history.iter().find(|(s, c, _)| *s == seq && *c == count) {
        return Ok(Some(offset));
    }
    if let Some(&(s, c, _)) = st.history.back() {
        let expected = s.wrapping_add(c);
        if seq != expected {
            return Err(BrokerError::new(
                Code::OutOfOrder,
                format!("producer {producer}: sequence {seq}, expected {expected}"),
            ));
        }
    }
    Ok(None)
}

/// What one partition's write covered.
pub(super) struct WriteRound {
    pub files: Vec<Arc<File>>,
    pub upto: u64,
    /// A directory whose new entries need a sync.
    pub dir: Option<PathBuf>,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("index", &self.index)
            .field("start", &self.log_start())
            .field("next", &self.next_offset())
            .field("durable", &self.durable_offset())
            .finish()
    }
}
