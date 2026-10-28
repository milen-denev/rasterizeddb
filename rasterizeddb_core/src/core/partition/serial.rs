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
//! before, and reserved from there.
//!
//! An explicit value written into a `SERIAL` column does not move the
//! counter, as in PostgreSQL: a later default can repeat it.
//!
//! # File
//!
//! `"RSQ1" ‖ len:u32 ‖ crc32:u32 ‖ payload[len]`, the CRC over the payload,
//! `payload = n:u32 ‖ n × (name_len:u16 ‖ name ‖ reserved:i64)`, all
//! little-endian, rewritten in place from byte 0 as one logged write. It only
//! grows (entries are never removed), so a shorter rewrite never leaves a
//! stale tail inside `len`. An empty file has no entries; one that does not
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

/// The durable `SERIAL` reservations of one partitioned table.
pub struct SerialSource {
    db_id: u64,
    io: Option<StdArc<DbStorageProvider>>,
    /// By sequence name: the value reserved on disk.
    entries: parking_lot::Mutex<BTreeMap<String, i64>>,
    /// Any entry at all: the fast path's one load.
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
        let entries = decode(&buf).unwrap_or_else(|why| {
            ::log::warn!(
                "the SERIAL reservations in {} cannot be read ({why}); every sequence of the table reseeds from \
                 its column's maximum",
                io.get_name()
            );
            BTreeMap::new()
        });
        Ok(SerialSource {
            db_id,
            io: Some(io),
            entries: parking_lot::Mutex::new(entries),
            tracked: AtomicBool::new(false),
            raising: tokio::sync::Mutex::new(()),
            max_scans: AtomicU64::new(0),
        })
    }

    /// The reservation of `name`, if the file has one.
    pub fn reserved(&self, name: &str) -> Option<i64> {
        self.entries.lock().get(name).copied()
    }

    pub fn max_scans(&self) -> u64 {
        self.max_scans.load(Ordering::Acquire)
    }

    /// Seeds `name`'s counter from its reservation, if the file has one:
    /// the next value is one past it. `false` when it has none, and the
    /// caller seeds from the maximum and then [`Self::track`]s it.
    pub(crate) fn resume(&self, name: &str) -> bool {
        let Some(reserved) = self.reserved(name) else {
            return false;
        };
        crate::core::rql::sequence::seed(self.db_id, name, reserved);
        self.tracked.store(true, Ordering::Release);
        true
    }

    /// Tracks `name`, seeded from the maximum `last`: reserved at `last`, so
    /// the first value taken raises it. Before any value is taken.
    pub(crate) fn track(&self, name: &str, last: i64) {
        self.max_scans.fetch_add(1, Ordering::AcqRel);
        if self.io.is_none() {
            return;
        }
        self.entries.lock().entry(name.to_string()).or_insert(last);
        self.tracked.store(true, Ordering::Release);
    }

    /// The counters past their reservations, with the value each must reach.
    fn behind(&self) -> Vec<(String, i64)> {
        self.entries
            .lock()
            .iter()
            .filter_map(|(name, &reserved)| {
                let current = crate::core::rql::sequence::current(self.db_id, name)?;
                (current > reserved).then(|| (name.clone(), current))
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
        let next = {
            let mut entries = self.entries.lock().clone();
            for (name, current) in behind {
                entries.insert(name, current.saturating_add(STEP));
            }
            entries
        };
        let buf = encode(&next);
        let txn = io.begin_txn();
        let token = io.write_data_txn(txn, 0, &buf).await;
        io.commit_durable(token).await.map_err(|e| {
            io::Error::new(e.kind(), format!("the SERIAL reservation in {} could not be raised: {e}", io.get_name()))
        })?;
        *self.entries.lock() = next;
        Ok(())
    }
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
