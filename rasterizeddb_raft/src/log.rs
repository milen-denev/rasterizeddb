//! The in-memory view of the log: a snapshot's position, then every entry
//! after it. The caller persists what [`crate::Ready`] hands it; on restart
//! it rebuilds this from its storage ([`RaftLog::restore`]).

use crate::types::{Entry, Index, Term};

#[derive(Clone, Debug, Default)]
pub struct RaftLog {
    /// The last index and term the snapshot covers (0, 0 for none).
    snapshot_index: Index,
    snapshot_term: Term,
    /// `entries[i]` has index `snapshot_index + 1 + i`.
    entries: Vec<Entry>,
    /// Entries through here are durable (the caller said so).
    persisted: Index,
    pub(crate) committed: Index,
    pub(crate) applied: Index,
}

impl RaftLog {
    /// A log rebuilt from storage: the snapshot's position, the entries after
    /// it (durable, contiguous), and the hard state's commit.
    pub fn restore(snapshot_index: Index, snapshot_term: Term, entries: Vec<Entry>, commit: Index) -> Result<RaftLog, String> {
        for (i, e) in entries.iter().enumerate() {
            if e.index != snapshot_index + 1 + i as u64 {
                return Err(format!("log entry {} is out of place (expected {})", e.index, snapshot_index + 1 + i as u64));
            }
            if i > 0 && e.term < entries[i - 1].term {
                return Err(format!("log entry {}'s term {} is below its predecessor's", e.index, e.term));
            }
        }
        let last = snapshot_index + entries.len() as u64;
        if commit > last {
            return Err(format!("the commit index {commit} is past the log's last index {last}"));
        }
        let committed = commit.max(snapshot_index);
        Ok(RaftLog { snapshot_index, snapshot_term, entries, persisted: last, committed, applied: snapshot_index })
    }

    pub fn first_index(&self) -> Index {
        self.snapshot_index + 1
    }

    pub fn last_index(&self) -> Index {
        self.snapshot_index + self.entries.len() as u64
    }

    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// The term of entry `index`, or `None` when it is compacted away or not
    /// there.
    pub fn term(&self, index: Index) -> Option<Term> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index || index > self.last_index() {
            return None;
        }
        Some(self.entries[(index - self.snapshot_index - 1) as usize].term)
    }

    pub fn entry(&self, index: Index) -> Option<&Entry> {
        if index <= self.snapshot_index || index > self.last_index() {
            return None;
        }
        Some(&self.entries[(index - self.snapshot_index - 1) as usize])
    }

    /// Entries `[lo, hi]`, at most `max` of them.
    pub fn slice(&self, lo: Index, hi: Index, max: usize) -> Vec<Entry> {
        // Past the end too: `hi` may reach beyond the log (a peer's next
        // index ahead of it), which made the end fall before the start.
        if lo > hi || lo <= self.snapshot_index || lo > self.last_index() {
            return Vec::new();
        }
        let start = (lo - self.snapshot_index - 1) as usize;
        // Saturating: callers ask for `usize::MAX` entries, and `start +
        // max` wrapped to `start - 1`, an end before the start.
        let end = ((hi.min(self.last_index()) - self.snapshot_index) as usize).min(start.saturating_add(max));
        self.entries[start..end].to_vec()
    }

    /// Whether a log ending at `(index, term)` is at least as up to date as
    /// this one (Raft §5.4.1).
    pub fn is_up_to_date(&self, index: Index, term: Term) -> bool {
        term > self.last_term() || (term == self.last_term() && index >= self.last_index())
    }

    /// Appends `entries` after `prev_index`, if `prev_term` matches there.
    /// A conflicting suffix is cut first (never below `committed`, which
    /// would break the state-machine safety the leader guarantees). Returns
    /// the index the log now matches through, or `None` on a mismatch, and
    /// the first index that was replaced, when any was.
    pub(crate) fn maybe_append(
        &mut self,
        prev_index: Index,
        prev_term: Term,
        entries: &[Entry],
    ) -> Result<Option<(Index, Option<Index>)>, String> {
        // Entries at or below the snapshot are committed, so every leader
        // holds them as they are: skip them, and check the rest against the
        // snapshot's own position.
        if prev_index < self.snapshot_index {
            let rest: Vec<Entry> = entries.iter().filter(|e| e.index > self.snapshot_index).cloned().collect();
            if rest.is_empty() {
                return Ok(Some((prev_index + entries.len() as u64, None)));
            }
            return self.maybe_append(self.snapshot_index, self.snapshot_term, &rest);
        }
        if self.term(prev_index) != Some(prev_term) {
            return Ok(None);
        }
        let mut replaced = None;
        for e in entries {
            match self.term(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    if e.index <= self.committed {
                        return Err(format!(
                            "a leader asked to replace committed entry {} (committed through {})",
                            e.index, self.committed
                        ));
                    }
                    self.truncate_from(e.index);
                    replaced.get_or_insert(e.index);
                    self.entries.push(e.clone());
                }
                None => {
                    if e.index != self.last_index() + 1 {
                        return Err(format!("entry {} would leave a gap after {}", e.index, self.last_index()));
                    }
                    self.entries.push(e.clone());
                }
            }
        }
        Ok(Some((prev_index + entries.len() as u64, replaced)))
    }

    pub(crate) fn append(&mut self, e: Entry) {
        debug_assert_eq!(e.index, self.last_index() + 1);
        self.entries.push(e);
    }

    fn truncate_from(&mut self, index: Index) {
        let keep = (index - self.snapshot_index - 1) as usize;
        self.entries.truncate(keep);
        self.persisted = self.persisted.min(index - 1);
    }

    pub(crate) fn mark_persisted(&mut self, through: Index, term: Term) {
        // Only if the entry there is still the one persisted (a later
        // truncation may have replaced it).
        if self.term(through) == Some(term) && through > self.persisted {
            self.persisted = through;
        }
    }

    pub fn persisted(&self) -> Index {
        self.persisted
    }

    /// Entries not yet durable.
    pub(crate) fn unpersisted(&self) -> Vec<Entry> {
        self.slice(self.persisted + 1, self.last_index(), usize::MAX)
    }

    pub(crate) fn commit_to(&mut self, index: Index) -> bool {
        let index = index.min(self.last_index());
        if index > self.committed {
            self.committed = index;
            return true;
        }
        false
    }

    /// Committed entries not yet applied, durable ones only.
    pub(crate) fn to_apply(&self, max: usize) -> Vec<Entry> {
        let hi = self.committed.min(self.persisted);
        if self.applied >= hi {
            return Vec::new();
        }
        self.slice(self.applied + 1, hi, max)
    }

    pub(crate) fn applied_to(&mut self, index: Index) -> Result<(), String> {
        if index > self.committed {
            return Err(format!("applied {index} past the commit index {}", self.committed));
        }
        if index > self.applied {
            self.applied = index;
        }
        Ok(())
    }

    /// Drops the entries through `index` (the state machine has a snapshot
    /// there).
    pub(crate) fn compact(&mut self, index: Index) -> Result<(), String> {
        if index <= self.snapshot_index {
            return Ok(());
        }
        if index > self.applied {
            return Err(format!("cannot compact past the applied index {}", self.applied));
        }
        let term = self.term(index).ok_or("compacting an unknown entry")?;
        let drop = (index - self.snapshot_index) as usize;
        self.entries.drain(..drop);
        self.snapshot_index = index;
        self.snapshot_term = term;
        Ok(())
    }

    /// Moves the log to a received snapshot's position. When the log holds
    /// the snapshot's last entry with its term, the entries after it are
    /// kept, as Raft §7 says: the leader may already count them replicated
    /// here, and dropping them left it sending after an end this log no
    /// longer had, for ever. Otherwise the log is replaced whole. Returns
    /// whether the suffix was kept, which says how the caller makes it
    /// durable (`Ready::snapshot_keeps_log`).
    pub(crate) fn restore_snapshot(&mut self, index: Index, term: Term) -> bool {
        let kept = index > self.snapshot_index && self.term(index) == Some(term);
        if kept {
            let drop = (index - self.snapshot_index) as usize;
            self.entries.drain(..drop);
            self.persisted = self.persisted.max(index);
        } else {
            self.entries.clear();
            // Nothing past the snapshot is in the log, durable or not.
            self.persisted = index;
        }
        self.snapshot_index = index;
        self.snapshot_term = term;
        self.committed = self.committed.max(index);
        self.applied = self.applied.max(index);
        kept
    }

    pub fn committed(&self) -> Index {
        self.committed
    }

    pub fn applied(&self) -> Index {
        self.applied
    }
}
