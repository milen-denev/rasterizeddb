//! A partitioned table's `SERIAL` high-water marks (docs/partitioning.md
//! §10, §3.5): reserved durably in `__pt_<uid>_seq.db`, in steps, as row ids
//! are, so the first `SERIAL` insert after a restart resumes from the file
//! and opens or scans no partition. A plain table still seeds its counter
//! from the column's maximum, once per process (`rql::sequence`).
//!
//! The counters themselves stay `rql::sequence`'s, in memory, keyed by
//! database and sequence name; this keeps, per sequence the table's defaults
//! resume, the value reserved on disk. A write path of any partition covers
//! the counters before its first stamp ([`SerialSource::cover`], through the
//! write watch): a counter past its reservation raises it to the counter
//! plus [`STEP`], through the root location's log, before the row that took
//! the value is written. The reservation's record is therefore logged before
//! that row's outcome, in the same log, and a committed row's value is never
//! above a durable reservation; a restart resumes past it. Values taken by a
//! statement that did not commit, and the rest of a step, are skipped: a gap,
//! as in PostgreSQL, after any restart.
//!
//! A sequence with no entry (a table created before P5a, whose file is
//! created at its next open; a sequence a `RENAME` gave a new name) is
//! seeded once from the maximum over every materialized partition, as
//! before, and reserved from there. An entry whose name the defaults no
//! longer read is forgotten at every open and schema change
//! ([`SerialSource::retain_read`]), so a name a later `RENAME` gives back
//! never resumes from a reservation its counter has since passed under
//! another name.
//!
//! A clean shutdown records the exact last value ([`SerialSource::
//! record_exact`]), so only a crash leaves a gap (up to one step).
//!
//! Only a sequence this table's own defaults resume is reserved: one a
//! second table shares through a hand-written `nextval` default is that
//! table's to seed, by its own rules (a plain one from its maximum). The
//! reservation still covers the counter whoever seeds it: entries are
//! tracked from the open, so [`SerialSource::cover`] raises one the counter
//! has passed even when the other table seeded the counter first, and a
//! clean shutdown lowers only an entry this table has floored the counter
//! with in this process (`Entry::seeded_here`): until then a counter the
//! other table seeded from its own rows can be below this table's values.
//! This table's first insert raises such a counter to the reservation
//! ([`SerialSource::resume`]); it used to leave it, and drew values it
//! already held.
//!
//! An explicit value written into a `SERIAL` column does not move the
//! counter, as in PostgreSQL: a later default can repeat it.
//!
//! # File
//!
//! `"RSQ1" ‖ len:u32 ‖ crc32:u32 ‖ payload[len]`, the CRC over the payload,
//! `payload = n:u32 ‖ n × (name_len:u16 ‖ name ‖ reserved:i64)`, all
//! little-endian, rewritten in place from byte 0 as one logged write; a
//! shorter rewrite leaves stale bytes past `len`, which nothing reads. An empty file has no entries; one that does not
//! parse is treated as empty, with a warning: every sequence then reseeds
//! from the maximum, which never repeats a stored value.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

/// Values reserved per raise: as many as row ids (`ID_RESERVATION_CHUNK`).
pub const STEP: i64 = 1024;

const MAGIC: &[u8; 4] = b"RSQ1";

/// One sequence's reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    /// The value reserved on disk.
    reserved: i64,
    /// Whether this process seeded the sequence's counter from this table
    /// (from `reserved`, [`SerialSource::resume`], or from the partitions'
    /// maximum, [`SerialSource::track`]): only then is the counter known to
    /// be at or above every value the table holds, so only then may a clean
    /// shutdown lower `reserved` to it ([`SerialSource::record_exact`]). A
    /// counter another table seeded (one sharing the sequence through a
    /// hand-written `nextval` default, seeded from its own rows) can sit below
    /// this table's values.
    seeded_here: bool,
}

/// The durable `SERIAL` reservations of one partitioned table.
pub struct SerialSource {
    db_id: u64,
    io: Option<StdArc<DbStorageProvider>>,
    /// By sequence name.
    entries: parking_lot::Mutex<BTreeMap<String, Entry>>,
    /// Any entry at all: the fast path's one load. Set at open for a file
    /// that has entries, whoever seeds the counters: a counter another table
    /// seeded first still hands this table values, and one past its
    /// reservation must raise it before the row is written ([`Self::cover`]).
    tracked: AtomicBool,
    raising: tokio::sync::Mutex<()>,
    /// Sequences seeded from the partitions' maximum, ever, by this
    /// instance: what a test asserts stays 0 after a restart.
    max_scans: AtomicU64,
}

fn encode(entries: &BTreeMap<String, i64>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (name, reserved) in entries {
        let bytes = name.as_bytes();
        let len = bytes.len().min(u16::MAX as usize);
        payload.extend_from_slice(&(len as u16).to_le_bytes());
        payload.extend_from_slice(&bytes[..len]);
        payload.extend_from_slice(&reserved.to_le_bytes());
    }
    let mut out = Vec::with_capacity(12 + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn decode(buf: &[u8]) -> Result<BTreeMap<String, i64>, String> {
    let mut entries = BTreeMap::new();
    if buf.is_empty() {
        return Ok(entries);
    }
    if buf.len() < 12 || &buf[..4] != MAGIC {
        return Err("no RSQ1 header".into());
    }
    let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let payload = buf.get(12..12 + len).ok_or("shorter than its length")?;
    if crc32fast::hash(payload) != crc {
        return Err("CRC mismatch".into());
    }
    let mut at = 0usize;
    let mut take = |n: usize| -> Result<&[u8], String> {
        let s = payload.get(at..at + n).ok_or_else(|| "truncated entry".to_string())?;
        at += n;
        Ok(s)
    };
    let n = u32::from_le_bytes(take(4)?.try_into().unwrap());
    for _ in 0..n {
        let len = u16::from_le_bytes(take(2)?.try_into().unwrap()) as usize;
        let name = String::from_utf8(take(len)?.to_vec()).map_err(|_| "a name that is not UTF-8".to_string())?;
        let reserved = i64::from_le_bytes(take(8)?.try_into().unwrap());
        entries.insert(name, reserved);
    }
    Ok(entries)
}

impl SerialSource {
    /// The reservations `io` holds.
    pub(crate) async fn open(db_id: u64, io: StdArc<DbStorageProvider>) -> io::Result<Self> {
        let len = io.get_len().await;
        let mut buf = vec![0u8; len as usize];
        if len > 0 {
            let mut at = 0u64;
            io.read_data_into_buffer(&mut at, &mut buf).await?;
        }
        let entries: BTreeMap<String, Entry> = decode(&buf)
            .unwrap_or_else(|why| {
            ::log::warn!(
                "the SERIAL reservations in {} cannot be read ({why}); every sequence of the table reseeds from \
                 its column's maximum",
                io.get_name()
            );
            BTreeMap::new()
            })
            .into_iter()
            .map(|(name, reserved)| (name, Entry { reserved, seeded_here: false }))
            .collect();
        let tracked = !entries.is_empty();
        Ok(SerialSource {
            db_id,
            io: Some(io),
            entries: parking_lot::Mutex::new(entries),
            tracked: AtomicBool::new(tracked),
            raising: tokio::sync::Mutex::new(()),
            max_scans: AtomicU64::new(0),
        })
    }

    /// The reservation of `name`, if the file has one.
    pub fn reserved(&self, name: &str) -> Option<i64> {
        self.entries.lock().get(name).map(|e| e.reserved)
    }

    pub fn max_scans(&self) -> u64 {
        self.max_scans.load(Ordering::Acquire)
    }

    /// Seeds `name`'s counter from its reservation, if the file has one:
    /// the next value is one past it. `false` when it has none, and the
    /// caller seeds from the maximum and then [`Self::track`]s it.
    ///
    /// A counter another table sharing the sequence seeded first, from its
    /// own rows, is raised to the reservation rather than left below it:
    /// left, this table drew values it already held. Either way the counter
    /// is at or above every value this table holds afterwards, so the entry
    /// is seeded here.
    pub(crate) fn resume(&self, name: &str) -> bool {
        let mut entries = self.entries.lock();
        let Some(entry) = entries.get_mut(name) else {
            return false;
        };
        crate::core::rql::sequence::seed(self.db_id, name, entry.reserved);
        entry.seeded_here = true;
        self.tracked.store(true, Ordering::Release);
        true
    }

    /// Tracks `name`, whose counter the caller seeded from the maximum `last`
    /// (`seeded`: whether the counter is now known to be at or above `last`,
    /// which `rql::sequence::seed` guarantees whoever seeded it first):
    /// reserved at `last`, so the first value taken raises it. Before any
    /// value is taken.
    pub(crate) fn track(&self, name: &str, last: i64, seeded: bool) {
        self.max_scans.fetch_add(1, Ordering::AcqRel);
        if self.io.is_none() {
            return;
        }
        let mut entries = self.entries.lock();
        let entry = entries.entry(name.to_string()).or_insert(Entry { reserved: last, seeded_here: false });
        entry.seeded_here |= seeded;
        self.tracked.store(true, Ordering::Release);
    }

    /// The counters past their reservations, with the value each must reach.
    fn behind(&self) -> Vec<(String, i64)> {
        self.entries
            .lock()
            .iter()
            .filter_map(|(name, e)| {
                let current = crate::core::rql::sequence::current(self.db_id, name)?;
                (current > e.reserved).then(|| (name.clone(), current))
            })
            .collect()
    }

    /// Raises every reservation a counter has passed to the counter plus
    /// [`STEP`], logged before this returns: before the caller writes the row
    /// that took the value.
    pub(crate) async fn cover(&self) -> io::Result<()> {
        if !self.tracked.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.behind().is_empty() {
            return Ok(());
        }
        Box::pin(self.raise()).await
    }

    async fn raise(&self) -> io::Result<()> {
        let _one = self.raising.lock().await;
        let behind = self.behind();
        if behind.is_empty() {
            return Ok(());
        }
        let Some(io) = &self.io else {
            return Ok(());
        };
        let raised: Vec<(String, i64)> = behind.into_iter().map(|(n, c)| (n, c.saturating_add(STEP))).collect();
        let next = {
            let mut entries = self.entries.lock().clone();
            for (name, value) in &raised {
                if let Some(e) = entries.get_mut(name) {
                    e.reserved = e.reserved.max(*value);
                }
            }
            entries
        };
        Self::write(io, &next, "raised").await?;
        // Merged, not replaced: a sequence `track`ed while the write was in
        // flight keeps its entry (the next raise writes it).
        let mut entries = self.entries.lock();
        for (name, value) in raised {
            if let Some(e) = entries.get_mut(&name) {
                e.reserved = e.reserved.max(value);
            }
        }
        Ok(())
    }

    /// Rewrites the file as `entries`, as one logged write from byte 0.
    async fn write(io: &StdArc<DbStorageProvider>, entries: &BTreeMap<String, Entry>, what: &str) -> io::Result<()> {
        let buf = encode(&entries.iter().map(|(n, e)| (n.clone(), e.reserved)).collect());
        let txn = io.begin_txn();
        let token = io.write_data_txn(txn, 0, &buf).await;
        io.commit_durable(token).await.map_err(|e| {
            io::Error::new(e.kind(), format!("the SERIAL reservation in {} could not be {what}: {e}", io.get_name()))
        })
    }

    /// Forgets every entry the table's defaults no longer read
    /// (`read`: the sequences of its `nextval` defaults now), durably, before
    /// any of them can be resumed from. A `RENAME` moves a `SERIAL`'s
    /// default to another sequence name, whose counter runs on under it; an
    /// entry left for the old name would be stale if a later `RENAME` gave
    /// the name back, and a restart would resume below values handed out
    /// meanwhile. Gone, the name reseeds from the partitions' maximum once.
    /// Run at every open and every schema change's reopen.
    pub(crate) async fn retain_read(&self, read: &std::collections::HashSet<String>) -> io::Result<()> {
        let _one = self.raising.lock().await;
        let Some(io) = &self.io else {
            return Ok(());
        };
        let next: BTreeMap<String, Entry> =
            self.entries.lock().iter().filter(|(n, _)| read.contains(*n)).map(|(n, v)| (n.clone(), *v)).collect();
        if next.len() == self.entries.lock().len() {
            return Ok(());
        }
        Self::write(io, &next, "pruned").await?;
        *self.entries.lock() = next;
        Ok(())
    }

    /// The clean-shutdown write (§10): every entry whose counter this
    /// table seeded in this process (`Entry::seeded_here`) lowered to the
    /// exact last value handed out, then
    /// synced, so a clean restart resumes with no gap, as PostgreSQL's
    /// sequences do. Sound at any moment, not only after the sessions end
    /// (a shutdown can start sealing with statements still running): a
    /// value taken after the counters are read is above the lowered entry,
    /// and the write path raises it again before the row is written.
    pub(crate) async fn record_exact(&self) -> io::Result<()> {
        // Under `raising` for the whole of it: a raise that began first has
        // written its file before this reads, and one a later value needs
        // waits until this has written, so its higher value lands last.
        let _one = self.raising.lock().await;
        let Some(io) = &self.io else {
            return Ok(());
        };
        // Read every counter and lower the entries in one critical section
        // of the lock `behind` reads under: a value taken after the read is
        // above the lowered entry, so its write raises again (and waits for
        // `raising`, above). Lowered after the file write instead, a value
        // taken while the write was in flight would still see the old, high
        // entry, raise nothing, and commit; a restart from the file would
        // then hand it out again.
        let next = {
            let mut entries = self.entries.lock();
            let mut changed = false;
            for (name, entry) in entries.iter_mut() {
                // Seeded elsewhere, the counter may be below values this table
                // holds: lowered to it, a restart would hand them out again.
                if !entry.seeded_here {
                    continue;
                }
                if let Some(last) = crate::core::rql::sequence::current(self.db_id, name)
                    && last < entry.reserved
                {
                    entry.reserved = last;
                    changed = true;
                }
            }
            if !changed {
                return Ok(());
            }
            entries.clone()
        };
        #[cfg(test)]
        test_hooks::park_before_write(io.get_location()).await;
        Self::write(io, &next, "recorded").await?;
        if let Some(location) = io.get_location() {
            crate::core::wal::with_wal_for(&location, |w| w.map_or(Ok(()), |w| w.sync_pending()))?;
        }
        Ok(())
    }
}

/// Test-only: holds `record_exact` between lowering its entries and writing
/// them, for tables whose files are under an armed directory.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::sync::Notify;

    type Park = (PathBuf, Arc<Notify>, Arc<Notify>);
    static PARKS: parking_lot::Mutex<Vec<Park>> = parking_lot::Mutex::new(Vec::new());

    /// The first `Notify` is signalled when a `record_exact` under `dir`
    /// reaches the park, which goes on once the second is.
    pub(crate) fn arm(dir: &Path) -> (Arc<Notify>, Arc<Notify>) {
        let (reached, resume) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        PARKS.lock().push((dir.to_path_buf(), reached.clone(), resume.clone()));
        (reached, resume)
    }

    pub(super) async fn park_before_write(location: Option<String>) {
        let Some(location) = location else {
            return;
        };
        let found = {
            let mut parks = PARKS.lock();
            let at = parks.iter().position(|(d, _, _)| Path::new(&location).starts_with(d));
            at.map(|i| parks.remove(i))
        };
        if let Some((_, reached, resume)) = found {
            reached.notify_one();
            resume.notified().await;
        }
    }
}

/// The sequences `schema`'s live `nextval` defaults read, by the names
/// `rql::sequence` keys their counters on.
pub(crate) fn sequences_read(schema: &crate::core::row::schema::TableSchema) -> std::collections::HashSet<String> {
    schema
        .defaults
        .iter()
        .filter(|d| !d.is_deleted)
        .filter_map(|d| crate::core::rql::sequence::parse_nextval(&d.expression))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_round_trips_and_refuses_damage() {
        let mut e = BTreeMap::new();
        assert_eq!(decode(&encode(&e)).unwrap(), e);
        e.insert("t_a_seq".to_string(), 1024);
        e.insert("t_b_seq".to_string(), i64::MAX - 3);
        let buf = encode(&e);
        assert_eq!(decode(&buf).unwrap(), e);
        assert_eq!(decode(&[]).unwrap(), BTreeMap::new());
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0x40;
            if let Ok(got) = decode(&bad) {
                assert_ne!(got, e, "byte {i} flipped and read back the same");
            }
        }
        assert!(decode(&buf[..buf.len() - 1]).is_err());
        // A longer, older file under a shorter rewrite: `len` bounds the read.
        let mut grown = encode(&e);
        grown.extend_from_slice(&[0xAB; 20]);
        assert_eq!(decode(&grown).unwrap(), e);
    }
}
