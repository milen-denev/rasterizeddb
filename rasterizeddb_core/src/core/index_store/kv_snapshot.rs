//! A key/value table's snapshot (docs/kv-partitions.md §2): the same
//! FILTER and INDEX sections a file set's `.ixs` holds, stored in the
//! table's own family as `<rows file>.ixs`, and validated by the store
//! instead of a marker log.
//!
//! A directory cannot say whether a file changed after a snapshot, so a file
//! set keeps `.ixm`. A key/value file can: every batch stored for it takes
//! the file's next sequence (monotonic across restarts), and a compaction or
//! a new generation moves the sequence or the generation too. So the
//! snapshot records each of the table's two files' state, [`KvState`]
//! `(generation, next sequence, length)`, read with nothing of the file
//! unstored or undecided, and an open that recovers exactly those states
//! recovered exactly the bytes the sections were captured from.
//!
//! # The stored form
//!
//! ```text
//! magic "RZKVIXS1" | pointers' KvState | rows' KvState | crc32c of the above | snapshot
//! ```
//!
//! where `snapshot` is [`super::snapshot::encode`]'s bytes, whose header
//! carries the [`Identity`] (incarnation, slot counts and lengths, the
//! version tuple, the index shape and the filter configuration) and whose
//! sections and footer carry their own checksums. The slot epoch in that
//! identity is always 0 here: the store's sequences already move with every
//! renumbering, and a detached gate's epoch is per process.

use std::io;

use super::codec::crc32c;
use super::identity::Identity;
use super::open_proof::{Proven, Reject};
use super::snapshot::{self, SectionEntry, SectionKind, SnapshotSource};
use crate::core::storage_providers::traits::KvState;

pub const KV_MAGIC: [u8; 8] = *b"RZKVIXS1";
/// The magic, two states of three `u64`s, and the crc32c.
pub const KV_HEADER_LEN: usize = 8 + 6 * 8 + 4;

/// The table's two files' states, as a snapshot records them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvIdentity {
    pub pointers: KvState,
    pub rows: KvState,
}

/// The stored form of `snapshot` (module documentation).
pub fn encode(kv: &KvIdentity, snapshot: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(KV_HEADER_LEN + snapshot.len());
    out.extend_from_slice(&KV_MAGIC);
    for s in [kv.pointers, kv.rows] {
        out.extend_from_slice(&s.generation.to_le_bytes());
        out.extend_from_slice(&s.next_seq.to_le_bytes());
        out.extend_from_slice(&s.len.to_le_bytes());
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(snapshot);
    out
}

/// The recorded states, checked, and where the snapshot starts. `None`
/// when the magic or the checksum is wrong.
pub fn header(bytes: &[u8]) -> Option<KvIdentity> {
    if bytes.len() < KV_HEADER_LEN || bytes[..8] != KV_MAGIC {
        return None;
    }
    let body = &bytes[..KV_HEADER_LEN - 4];
    let crc = u32::from_le_bytes(bytes[KV_HEADER_LEN - 4..KV_HEADER_LEN].try_into().ok()?);
    if crc32c(body) != crc {
        return None;
    }
    let word = |i: usize| u64::from_le_bytes(body[8 + i * 8..16 + i * 8].try_into().expect("eight bytes"));
    let state = |at: usize| KvState {
        generation: word(at),
        next_seq: word(at + 1),
        len: word(at + 2),
    };
    Some(KvIdentity {
        pointers: state(0),
        rows: state(3),
    })
}

/// The open's proof for a key/value table: the sidecar's `stored` bytes
/// (`None`: it does not stand) against `now`, the files' states as
/// recovered, and `expected`, the identity the files and this binary have
/// now. Every check as [`super::open_proof::prove`] makes them, the marker
/// log's replaced by the states'. On success the snapshot's bytes are the
/// load's source, held in memory.
pub fn prove(stored: Option<Vec<u8>>, now: &KvIdentity, expected: &Identity, has_filter: bool) -> Result<Proven, Reject> {
    let Some(mut bytes) = stored else {
        return Err(Reject::NoSnapshot);
    };
    let Some(recorded) = header(&bytes) else {
        return Err(Reject::SnapshotCorrupt("the key/value snapshot header does not check".into()));
    };
    if recorded != *now {
        return Err(Reject::StoreState);
    }
    let snap = bytes.split_off(KV_HEADER_LEN);
    let len = snap.len() as u64;
    let streamed = snapshot::verify_stream(&mut io::Cursor::new(&snap), len, |e| {
        e.kind == SectionKind::Filter && e.ordinal == 0
    })
    .map_err(|e| match e {
        snapshot::StreamError::Format(f) => Reject::SnapshotCorrupt(f.to_string()),
        snapshot::StreamError::Io(e) => Reject::SnapshotUnreadable(e.to_string()),
    })?;
    if let Some(field) = streamed.header.identity.first_mismatch(expected) {
        return Err(Reject::Identity(field));
    }
    let filter = streamed.kept.into_iter().next().map(|(_, b)| b);
    let index: Vec<SectionEntry> =
        streamed.header.sections.iter().filter(|e| e.kind == SectionKind::Index).copied().collect();
    let has_gin = streamed.header.sections.iter().any(|e| e.kind == SectionKind::Gin);
    let proven = |filter| Proven {
        filter,
        index,
        has_gin,
        source: SnapshotSource::Bytes(std::sync::Arc::new(snap)),
    };
    match (filter, has_filter) {
        (Some(b), true) => Ok(proven(Some(b))),
        (None, false) => Ok(proven(None)),
        (None, true) => Err(Reject::NoFilterSection),
        (Some(_), false) => Err(Reject::UnexpectedFilterSection),
    }
}

#[cfg(test)]
mod tests {
    use super::super::identity::sample;
    use super::*;

    fn kv() -> KvIdentity {
        KvIdentity {
            pointers: KvState { generation: 1, next_seq: 9, len: 370 },
            rows: KvState { generation: 0, next_seq: 12, len: 4096 },
        }
    }

    fn stored(filter: bool) -> Vec<u8> {
        let mut sections: Vec<(SectionKind, u16, &[u8])> = vec![(SectionKind::Index, 0, b"index zero")];
        if filter {
            sections.insert(0, (SectionKind::Filter, 0, b"the filter"));
        }
        encode(&kv(), &snapshot::encode(sample(), &sections))
    }

    #[test]
    fn a_snapshot_proves_against_the_states_it_recorded() {
        let proven = prove(Some(stored(true)), &kv(), &sample(), true).expect("proven");
        assert_eq!(proven.filter.as_deref(), Some(&b"the filter"[..]));
        assert_eq!(proven.index.len(), 1);
        assert_eq!(proven.source.read_section(&proven.index[0]).unwrap(), b"index zero");
    }

    #[test]
    fn any_other_state_is_refused() {
        let mut moved = kv();
        moved.rows.next_seq += 1;
        assert_eq!(prove(Some(stored(true)), &moved, &sample(), true).unwrap_err(), Reject::StoreState);
        let mut regenerated = kv();
        regenerated.pointers.generation += 1;
        assert_eq!(prove(Some(stored(true)), &regenerated, &sample(), true).unwrap_err(), Reject::StoreState);
        let mut longer = kv();
        longer.pointers.len += 37;
        assert_eq!(prove(Some(stored(true)), &longer, &sample(), true).unwrap_err(), Reject::StoreState);
    }

    #[test]
    fn a_damaged_or_missing_sidecar_is_refused() {
        assert_eq!(prove(None, &kv(), &sample(), true).unwrap_err(), Reject::NoSnapshot);
        let mut header_hit = stored(true);
        header_hit[20] ^= 1;
        assert!(matches!(prove(Some(header_hit), &kv(), &sample(), true), Err(Reject::SnapshotCorrupt(_))));
        let mut body_hit = stored(true);
        let last = body_hit.len() - 12;
        body_hit[last] ^= 1;
        assert!(matches!(prove(Some(body_hit), &kv(), &sample(), true), Err(Reject::SnapshotCorrupt(_))));
        let mut cut = stored(true);
        cut.truncate(cut.len() - 3);
        assert!(prove(Some(cut), &kv(), &sample(), true).is_err());
    }

    #[test]
    fn the_identity_and_the_filter_must_match() {
        let mut other = sample();
        other.incarnation ^= 1;
        assert!(matches!(prove(Some(stored(true)), &kv(), &other, true), Err(Reject::Identity(_))));
        assert_eq!(prove(Some(stored(false)), &kv(), &sample(), true).unwrap_err(), Reject::NoFilterSection);
        assert_eq!(prove(Some(stored(true)), &kv(), &sample(), false).unwrap_err(), Reject::UnexpectedFilterSection);
    }
}
