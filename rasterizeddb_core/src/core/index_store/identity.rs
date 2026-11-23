//! A snapshot's identity (docs/persistent-indexes.md §4): everything an open
//! compares before it trusts a snapshot, in one fixed-size record that `.ixs`
//! carries in its header and `.ixm` carries in each `CLEAN`.
//!
//! # Layout, version 1 (92 bytes, little-endian)
//!
//! | Bytes | Field | Catches |
//! |---|---|---|
//! | 0..2 | `IDENTITY_FORMAT_VER` (u16) | a later layout of this record |
//! | 2..18 | `incarnation` (u128, `_schema.db`, hook C7) | DROP + CREATE, an ALTER rewrite |
//! | 18..26 | `slot_epoch` (u64, the `rdb.ixe` record) | a renumbering the marker log never saw |
//! | 26..34 | `n_slots` (u64) | appended or truncated pointers |
//! | 34..42 | `pointers_len` (u64) | the same, in bytes |
//! | 42..50 | `rows_len` (u64) | lost unsynced row tails |
//! | 50..54 | `snapshot_format_ver` (u32, `.ixs`'s) | container drift |
//! | 54..58 | `encoding_version` (u32, `key::ENCODING_VERSION`) | key bytes |
//! | 58..62 | `text_render_version` (u32, `jsonb::TEXT_RENDER_VERSION`) | path keys |
//! | 62..66 | `item_derive_version` (u32, `gin::ITEM_DERIVE_VERSION`) | gin items |
//! | 66..70 | `gin_hasher_id` (u32, `gin::GIN_HASHER_ID`) | the gin hash |
//! | 70..74 | `filter_derive_version` (u32, `persist::FILTER_DERIVE_VERSION`) | filter derivations |
//! | 74..76 | `filter_format_ver` (u16, `persist::FORMAT_VER`) | the FILTER section's layout |
//! | 76..84 | `shape_hash` (u64, `TableIndexes::shape_hash`, hook C6) | changed declarations |
//! | 84..92 | `filter_config_hash` (u64) | filtered columns or widths |
//!
//! The record has no checksum of its own: both containers checksum the bytes
//! around it. Its version field is still checked, so a later layout is
//! refused rather than misread.

use super::codec::{FormatError, Reader};

/// This record's layout version.
pub const IDENTITY_FORMAT_VER: u16 = 1;
/// Its encoded size.
pub const IDENTITY_LEN: usize = 92;

const WHAT: &str = "snapshot identity";

/// The versions of everything a snapshot's bytes are derived by (§8). A
/// snapshot is trusted only by a binary whose tuple is equal, field for field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionTuple {
    pub snapshot_format_ver: u32,
    pub encoding_version: u32,
    pub text_render_version: u32,
    pub item_derive_version: u32,
    pub gin_hasher_id: u32,
    pub filter_derive_version: u32,
    pub filter_format_ver: u16,
}

impl VersionTuple {
    /// This binary's tuple.
    ///
    /// `persist::FORMAT_VER` is `pub(super)` inside `core::filter`, which this
    /// slice does not touch (owner's review), so the caller passes it; B13
    /// widens it when it wires the FILTER section in.
    pub fn current(filter_format_ver: u16) -> VersionTuple {
        VersionTuple {
            snapshot_format_ver: super::snapshot::SNAPSHOT_FORMAT_VER,
            encoding_version: crate::core::index::key::ENCODING_VERSION,
            text_render_version: crate::core::jsonb::TEXT_RENDER_VERSION,
            item_derive_version: crate::core::index::gin::ITEM_DERIVE_VERSION,
            gin_hasher_id: crate::core::index::gin::GIN_HASHER_ID,
            filter_derive_version: crate::core::filter::persist::FILTER_DERIVE_VERSION,
            filter_format_ver,
        }
    }
}

/// A snapshot's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Identity {
    pub incarnation: u128,
    pub slot_epoch: u64,
    pub n_slots: u64,
    pub pointers_len: u64,
    pub rows_len: u64,
    pub versions: VersionTuple,
    pub shape_hash: u64,
    pub filter_config_hash: u64,
}

/// A field of [`Identity`], in the order the open checks them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityField {
    Incarnation,
    SlotEpoch,
    NSlots,
    PointersLen,
    RowsLen,
    SnapshotFormatVer,
    EncodingVersion,
    TextRenderVersion,
    ItemDeriveVersion,
    GinHasherId,
    FilterDeriveVersion,
    FilterFormatVer,
    ShapeHash,
    FilterConfigHash,
}

impl Identity {
    pub fn encode(&self) -> [u8; IDENTITY_LEN] {
        let mut out = Vec::with_capacity(IDENTITY_LEN);
        self.encode_into(&mut out);
        out.try_into().expect("an identity encodes to IDENTITY_LEN bytes")
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        let v = &self.versions;
        out.extend_from_slice(&IDENTITY_FORMAT_VER.to_le_bytes());
        out.extend_from_slice(&self.incarnation.to_le_bytes());
        out.extend_from_slice(&self.slot_epoch.to_le_bytes());
        out.extend_from_slice(&self.n_slots.to_le_bytes());
        out.extend_from_slice(&self.pointers_len.to_le_bytes());
        out.extend_from_slice(&self.rows_len.to_le_bytes());
        out.extend_from_slice(&v.snapshot_format_ver.to_le_bytes());
        out.extend_from_slice(&v.encoding_version.to_le_bytes());
        out.extend_from_slice(&v.text_render_version.to_le_bytes());
        out.extend_from_slice(&v.item_derive_version.to_le_bytes());
        out.extend_from_slice(&v.gin_hasher_id.to_le_bytes());
        out.extend_from_slice(&v.filter_derive_version.to_le_bytes());
        out.extend_from_slice(&v.filter_format_ver.to_le_bytes());
        out.extend_from_slice(&self.shape_hash.to_le_bytes());
        out.extend_from_slice(&self.filter_config_hash.to_le_bytes());
    }

    /// Decodes the first [`IDENTITY_LEN`] bytes of `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Identity, FormatError> {
        Identity::read(&mut Reader::new(bytes, WHAT))
    }

    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Identity, FormatError> {
        let ver = r.u16()?;
        if ver != IDENTITY_FORMAT_VER {
            return Err(FormatError::UnsupportedVersion {
                what: WHAT,
                found: ver as u32,
            });
        }
        Ok(Identity {
            incarnation: r.u128()?,
            slot_epoch: r.u64()?,
            n_slots: r.u64()?,
            pointers_len: r.u64()?,
            rows_len: r.u64()?,
            versions: VersionTuple {
                snapshot_format_ver: r.u32()?,
                encoding_version: r.u32()?,
                text_render_version: r.u32()?,
                item_derive_version: r.u32()?,
                gin_hasher_id: r.u32()?,
                filter_derive_version: r.u32()?,
                filter_format_ver: r.u16()?,
            },
            shape_hash: r.u64()?,
            filter_config_hash: r.u64()?,
        })
    }

    /// The first field, in check order, where `self` differs from
    /// `expected`; `None` when they are equal. What an open logs when it
    /// rejects a snapshot.
    pub fn first_mismatch(&self, expected: &Identity) -> Option<IdentityField> {
        use IdentityField::*;
        let (a, b) = (&self.versions, &expected.versions);
        [
            (self.incarnation == expected.incarnation, Incarnation),
            (self.slot_epoch == expected.slot_epoch, SlotEpoch),
            (self.n_slots == expected.n_slots, NSlots),
            (self.pointers_len == expected.pointers_len, PointersLen),
            (self.rows_len == expected.rows_len, RowsLen),
            (a.snapshot_format_ver == b.snapshot_format_ver, SnapshotFormatVer),
            (a.encoding_version == b.encoding_version, EncodingVersion),
            (a.text_render_version == b.text_render_version, TextRenderVersion),
            (a.item_derive_version == b.item_derive_version, ItemDeriveVersion),
            (a.gin_hasher_id == b.gin_hasher_id, GinHasherId),
            (a.filter_derive_version == b.filter_derive_version, FilterDeriveVersion),
            (a.filter_format_ver == b.filter_format_ver, FilterFormatVer),
            (self.shape_hash == expected.shape_hash, ShapeHash),
            (self.filter_config_hash == expected.filter_config_hash, FilterConfigHash),
        ]
        .into_iter()
        .find(|(equal, _)| !equal)
        .map(|(_, field)| field)
    }
}

/// The identity the golden vectors of all three containers are built on.
/// Literal versions, not [`VersionTuple::current`]: a vector must not move
/// when a constant does.
#[cfg(test)]
pub(crate) fn sample() -> Identity {
    Identity {
        incarnation: 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210,
        slot_epoch: 7,
        n_slots: 1_530_000,
        pointers_len: 56_610_000,
        rows_len: 395_000_000,
        versions: VersionTuple {
            snapshot_format_ver: 1,
            encoding_version: 1,
            text_render_version: 1,
            item_derive_version: 2,
            gin_hasher_id: 2,
            filter_derive_version: 1,
            filter_format_ver: 5,
        },
        shape_hash: 0xdead_beef_cafe_f00d,
        filter_config_hash: 0x0bad_c0de_1234_5678,
    }
}

#[cfg(test)]
mod tests {
    use super::super::codec::{digest_hex, from_hex, to_hex};
    use super::*;
    use crate::core::index::vectors::{HistoryRow, check_history};

    /// [`sample`] encoded under `IDENTITY_FORMAT_VER` 1, frozen.
    const SAMPLE_V1: &str = "0100\
        1032547698badcfeefcdab8967452301\
        0700000000000000\
        9058170000000000\
        d0cc5f0300000000\
        c0388b1700000000\
        01000000\
        01000000\
        01000000\
        02000000\
        02000000\
        01000000\
        0500\
        0df0fecaefbeadde\
        78563412dec0ad0b";

    fn v1_digest() -> u64 {
        digest_hex(&[SAMPLE_V1])
    }

    /// `IDENTITY_FORMAT_VER`'s frozen vectors, oldest first.
    const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0x3419_bee3_455a_bc00)];

    #[test]
    fn the_sample_encodes_to_its_frozen_bytes_and_back() {
        let bytes = sample().encode();
        assert_eq!(to_hex(&bytes), to_hex(&from_hex(SAMPLE_V1)), "the identity layout moved");
        assert_eq!(bytes.len(), IDENTITY_LEN);
        assert_eq!(Identity::decode(&from_hex(SAMPLE_V1)), Ok(sample()));
    }

    #[test]
    fn identity_vectors_are_pinned_to_identity_format_ver() {
        check_history("identity::IDENTITY_FORMAT_VER", HISTORY, IDENTITY_FORMAT_VER as u32);
    }

    #[test]
    fn the_current_tuple_reads_the_binarys_constants() {
        let v = VersionTuple::current(5);
        assert_eq!(v.encoding_version, crate::core::index::key::ENCODING_VERSION);
        assert_eq!(v.gin_hasher_id, crate::core::index::gin::GIN_HASHER_ID);
        assert_eq!(v.snapshot_format_ver, super::super::snapshot::SNAPSHOT_FORMAT_VER);
        assert_eq!(v.filter_format_ver, 5);
    }

    #[test]
    fn every_field_is_compared() {
        let base = sample();
        assert_eq!(base.first_mismatch(&base), None);
        // Flip each field alone and see it named.
        let bytes = base.encode();
        let fields: [(usize, IdentityField); 14] = [
            (2, IdentityField::Incarnation),
            (18, IdentityField::SlotEpoch),
            (26, IdentityField::NSlots),
            (34, IdentityField::PointersLen),
            (42, IdentityField::RowsLen),
            (50, IdentityField::SnapshotFormatVer),
            (54, IdentityField::EncodingVersion),
            (58, IdentityField::TextRenderVersion),
            (62, IdentityField::ItemDeriveVersion),
            (66, IdentityField::GinHasherId),
            (70, IdentityField::FilterDeriveVersion),
            (74, IdentityField::FilterFormatVer),
            (76, IdentityField::ShapeHash),
            (84, IdentityField::FilterConfigHash),
        ];
        for (at, field) in fields {
            let mut b = bytes;
            b[at] ^= 0x01;
            let other = Identity::decode(&b).unwrap();
            assert_eq!(other.first_mismatch(&base), Some(field), "byte {at}");
            assert_eq!(base.first_mismatch(&other), Some(field), "byte {at}");
        }
    }

    #[test]
    fn a_truncated_or_later_identity_is_an_error_never_a_panic() {
        let bytes = sample().encode();
        for n in 0..IDENTITY_LEN {
            assert!(Identity::decode(&bytes[..n]).is_err(), "{n} bytes");
        }
        let mut later = bytes;
        later[0] = 2;
        assert_eq!(
            Identity::decode(&later),
            Err(FormatError::UnsupportedVersion { what: WHAT, found: 2 })
        );
        let mut rng = fastrand::Rng::with_seed(0x1de7);
        for _ in 0..5000 {
            let len = rng.usize(0..=IDENTITY_LEN + 8);
            let mut b: Vec<u8> = (0..len).map(|_| rng.u8(..)).collect();
            if rng.bool() && b.len() >= 2 {
                b[..2].copy_from_slice(&IDENTITY_FORMAT_VER.to_le_bytes());
            }
            let _ = Identity::decode(&b);
        }
    }
}
