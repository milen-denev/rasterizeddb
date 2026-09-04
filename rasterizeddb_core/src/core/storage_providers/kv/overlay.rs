//! The byte ranges that differ from what the base chunks hold.
//!
//! Every write lands here first and stays until compaction folds it into a base
//! chunk, so this is simultaneously the read path's source of truth for recent
//! writes and the unit compaction works in. Entries are kept non-overlapping and
//! ordered, and an insert that touches an existing entry merges with it — which
//! is what turns a stream of row appends into one contiguous run rather than a
//! map entry per row.

use std::collections::BTreeMap;

#[derive(Default)]
pub struct Overlay {
    /// start offset -> bytes. Non-overlapping, and no two entries touch.
    spans: BTreeMap<u64, Vec<u8>>,
    bytes: u64,
}

impl Overlay {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    #[inline]
    pub fn span_count(&self) -> usize {
        self.spans.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn clear(&mut self) {
        self.spans.clear();
        self.bytes = 0;
    }

    /// Every span that overlaps or abuts `[start, end)`.
    fn touching(&self, start: u64, end: u64) -> Vec<u64> {
        let mut keys = Vec::new();
        // The one span that may begin before `start` and reach into the range.
        if let Some((&k, v)) = self.spans.range(..start).next_back()
            && k + v.len() as u64 >= start
        {
            keys.push(k);
        }
        // `k == end` abuts and is worth merging; `k > end` cannot touch.
        for (&k, _) in self.spans.range(start..) {
            if k > end {
                break;
            }
            keys.push(k);
        }
        keys
    }

    /// Writes `data` at `start`, replacing whatever was there.
    pub fn insert(&mut self, start: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let end = start + data.len() as u64;
        let keys = self.touching(start, end);

        if keys.is_empty() {
            self.bytes += data.len() as u64;
            self.spans.insert(start, data.to_vec());
            return;
        }

        let mut merged_start = start;
        let mut merged_end = end;
        for &k in &keys {
            let len = self.spans[&k].len() as u64;
            merged_start = merged_start.min(k);
            merged_end = merged_end.max(k + len);
        }

        let mut buf = vec![0u8; (merged_end - merged_start) as usize];
        for &k in &keys {
            let span = self.spans.remove(&k).expect("key came from this map");
            self.bytes -= span.len() as u64;
            let at = (k - merged_start) as usize;
            buf[at..at + span.len()].copy_from_slice(&span);
        }
        let at = (start - merged_start) as usize;
        buf[at..at + data.len()].copy_from_slice(data);

        self.bytes += buf.len() as u64;
        self.spans.insert(merged_start, buf);
    }

    /// Applies every span intersecting `[position, position + buf.len())` on top
    /// of `buf`, which the caller has already filled from the base chunks.
    pub fn apply(&self, position: u64, buf: &mut [u8]) {
        if buf.is_empty() {
            return;
        }
        let end = position + buf.len() as u64;
        // Start from the span that may begin before `position`.
        let first = self
            .spans
            .range(..=position)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(position);

        for (&k, span) in self.spans.range(first..) {
            if k >= end {
                break;
            }
            let span_end = k + span.len() as u64;
            if span_end <= position {
                continue;
            }
            let copy_start = k.max(position);
            let copy_end = span_end.min(end);
            let dst = (copy_start - position) as usize;
            let src = (copy_start - k) as usize;
            let n = (copy_end - copy_start) as usize;
            buf[dst..dst + n].copy_from_slice(&span[src..src + n]);
        }
    }

    /// Whether any span intersects `[start, end)`.
    pub fn intersects(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return false;
        }
        if let Some((&k, v)) = self.spans.range(..=start).next_back()
            && k + v.len() as u64 > start
        {
            return true;
        }
        self.spans
            .range(start..)
            .next()
            .is_some_and(|(&k, _)| k < end)
    }

    /// Whether one span covers all of `[start, end)`, so no base read is needed.
    pub fn covers(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return true;
        }
        self.spans
            .range(..=start)
            .next_back()
            .is_some_and(|(&k, v)| k + v.len() as u64 >= end)
    }

    /// Drops coverage of `[start, end)`, splitting any span that straddles it.
    ///
    /// What compaction calls once a chunk's bytes are safely in the store: the
    /// overlay must stop describing that region, but a span reaching past the
    /// chunk boundary still describes the next chunk and has to survive.
    pub fn remove_range(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let keys = self.touching(start, end);
        for k in keys {
            let Some(span) = self.spans.remove(&k) else {
                continue;
            };
            self.bytes -= span.len() as u64;
            let span_end = k + span.len() as u64;
            if k < start {
                let head_len = (start - k) as usize;
                let head = span[..head_len].to_vec();
                self.bytes += head.len() as u64;
                self.spans.insert(k, head);
            }
            if span_end > end {
                let tail_from = (end - k) as usize;
                let tail = span[tail_from..].to_vec();
                self.bytes += tail.len() as u64;
                self.spans.insert(end, tail);
            }
        }
    }

    /// Removes everything below `limit` and returns it, splitting a span that
    /// straddles the line.
    ///
    /// What a compaction freezes with. The line is the lowest offset any open
    /// transaction has written, so what comes back is committed data and what
    /// stays behind may not be — folding an uncommitted byte into a base chunk
    /// would publish a transaction that never finished.
    pub fn split_below(&mut self, limit: u64) -> Overlay {
        let mut out = Overlay::new();
        if limit == 0 {
            return out;
        }
        let keys: Vec<u64> = self.spans.range(..limit).map(|(&k, _)| k).collect();
        for k in keys {
            let Some(span) = self.spans.remove(&k) else {
                continue;
            };
            self.bytes -= span.len() as u64;
            let end = k + span.len() as u64;
            if end <= limit {
                out.bytes += span.len() as u64;
                out.spans.insert(k, span);
            } else {
                let head_len = (limit - k) as usize;
                out.bytes += head_len as u64;
                out.spans.insert(k, span[..head_len].to_vec());
                let tail = span[head_len..].to_vec();
                self.bytes += tail.len() as u64;
                self.spans.insert(limit, tail);
            }
        }
        out
    }

    /// Discards everything at or past `len`.
    pub fn truncate(&mut self, len: u64) {
        let mut over: Vec<u64> = self.spans.range(len..).map(|(&k, _)| k).collect();
        // Plus the one span that may begin before `len` and reach past it.
        if let Some((&k, v)) = self.spans.range(..len).next_back()
            && k + v.len() as u64 > len
        {
            over.push(k);
        }
        for k in over {
            let Some(span) = self.spans.remove(&k) else {
                continue;
            };
            self.bytes -= span.len() as u64;
            if k < len {
                let keep = (len - k) as usize;
                let head = span[..keep].to_vec();
                self.bytes += head.len() as u64;
                self.spans.insert(k, head);
            }
        }
    }

    /// Total overlay bytes falling inside `[start, end)`.
    ///
    /// Compaction ranks chunks by this: the chunk carrying the most overlay is
    /// the one whose rewrite frees the most memory and retires the most deltas.
    pub fn bytes_in(&self, start: u64, end: u64) -> u64 {
        let mut total = 0u64;
        let first = self
            .spans
            .range(..=start)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(start);
        for (&k, span) in self.spans.range(first..) {
            if k >= end {
                break;
            }
            let span_end = k + span.len() as u64;
            if span_end <= start {
                continue;
            }
            total += span_end.min(end) - k.max(start);
        }
        total
    }

    /// The distinct offsets spans begin at, for planning which chunks to fold.
    pub fn span_bounds(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.spans.iter().map(|(&k, v)| (k, k + v.len() as u64))
    }

    /// Every span, as `(offset, bytes)`.
    pub fn spans_iter(&self) -> impl Iterator<Item = (u64, &[u8])> + '_ {
        self.spans.iter().map(|(&k, v)| (k, v.as_slice()))
    }
}
