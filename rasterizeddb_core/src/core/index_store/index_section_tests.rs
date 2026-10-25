//! The INDEX section codec's tests: frozen bytes and their history row,
//! round trips of random streams (restarts inside a key's slots, empty
//! keys, long shared prefixes, every presence shape), refusal of every
//! truncation and of each malformed field, and no panic on any bytes.

use super::super::codec::{digest_hex, from_hex, to_hex};
use super::*;
use crate::core::index::vectors::{HistoryRow, check_history};

/// What a stream holds, owned.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Stream {
    entries: Vec<(Vec<u8>, Vec<u64>)>,
    unkeyed: Vec<u64>,
    present: Vec<u64>,
}

impl Stream {
    fn encode(&self) -> Vec<u8> {
        let mut w = SectionWriter::new();
        for (k, s) in &self.entries {
            w.push(Sorted::Entry(k, s)).unwrap();
        }
        w.push(Sorted::Unkeyed(&self.unkeyed)).unwrap();
        w.push(Sorted::Present(trimmed(&self.present))).unwrap();
        w.finish().unwrap()
    }
}

fn trimmed(words: &[u64]) -> &[u64] {
    &words[..words.iter().rposition(|w| *w != 0).map_or(0, |w| w + 1)]
}

fn decoded(bytes: &[u8], n_slots: u64) -> Result<Stream, SectionError> {
    let mut s = Stream::default();
    decode_into(bytes, n_slots, |part| {
        match part {
            Sorted::Entry(k, slots) => s.entries.push((k.to_vec(), slots.to_vec())),
            Sorted::Unkeyed(slots) => s.unkeyed = slots.to_vec(),
            Sorted::Present(words) => s.present = words.to_vec(),
        }
        Ok(())
    })?;
    Ok(s)
}

/// A small stream touching every rule: an empty key, a key whose slots
/// cross a restart, shared prefixes, a long key, unkeyed slots, and
/// presence runs starting with a one, spanning a whole word and ending
/// mid-word.
fn golden() -> Stream {
    let mut entries = vec![
        (Vec::new(), vec![3]),
        (b"a".to_vec(), vec![0, 1]),
        (b"ab".to_vec(), vec![2]),
        (b"abc".to_vec(), (10..25).collect()),
        (b"abd".to_vec(), vec![7]),
        (vec![0xff; 30], vec![200, 1000]),
    ];
    entries.push((vec![0xff; 31], vec![5]));
    Stream {
        entries,
        unkeyed: vec![4, 6, 300],
        present: vec![0b1011, u64::MAX, 1 << 5],
    }
}

/// [`golden`] encoded under `INDEX_SECTION_VER` 1, frozen.
const SECTION_V1: &str = "01000000170000000000000000000300016100010000010162020201630a0300\
    0003000003000003000003000003000003000003000003000003000003000000\
    036162631603000003000002016407001effffffffffffffffffffffffffffff\
    ffffffffffffffffffffffffffffffc8011e009f061e01ff05030401a5020800\
    0201013c400501";

fn v1_digest() -> u64 {
    digest_hex(&[SECTION_V1])
}

/// `INDEX_SECTION_VER`'s frozen vectors, oldest first.
const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0x9a31_76c2_d6b6_ad91)];

#[test]
fn a_section_encodes_to_its_frozen_bytes() {
    let got = golden().encode();
    assert_eq!(to_hex(&got), to_hex(&from_hex(SECTION_V1)), "the INDEX section layout moved");
    assert_eq!(decoded(&got, 1024).unwrap(), Stream { present: vec![0b1011, u64::MAX, 1 << 5], ..golden() });
}

#[test]
fn index_section_vectors_are_pinned_to_index_section_ver() {
    check_history("index_section::INDEX_SECTION_VER", HISTORY, INDEX_SECTION_VER as u32);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A random well-formed stream over `n_slots` slots.
fn random_stream(r: &mut Rng, n_slots: u64) -> Stream {
    let mut keys: std::collections::BTreeMap<Vec<u8>, std::collections::BTreeSet<u64>> = Default::default();
    let n_keys = r.below(400);
    for _ in 0..n_keys {
        let len = match r.below(4) {
            0 => 0,
            1 => r.below(4),
            2 => 8 + r.below(2),
            _ => r.below(40),
        } as usize;
        let alphabet = [0u8, 1, b'a', b'b', 0x7f, 0xff];
        let key: Vec<u8> = (0..len).map(|_| alphabet[r.below(alphabet.len() as u64) as usize]).collect();
        let slots = keys.entry(key).or_default();
        let many = if r.below(5) == 0 { 40 } else { 2 };
        for _ in 0..1 + r.below(many) {
            slots.insert(r.below(n_slots));
        }
    }
    let unkeyed: std::collections::BTreeSet<u64> = (0..r.below(30)).map(|_| r.below(n_slots)).collect();
    let mut present = vec![0u64; n_slots.div_ceil(64) as usize];
    for (w, word) in present.iter_mut().enumerate() {
        *word = match r.below(5) {
            0 => 0,
            1 => u64::MAX,
            _ => r.next(),
        };
        let hi = n_slots.saturating_sub(w as u64 * 64);
        if hi < 64 {
            *word &= (1u64 << hi) - 1;
        }
    }
    Stream {
        entries: keys.into_iter().map(|(k, s)| (k, s.into_iter().collect())).collect(),
        unkeyed: unkeyed.into_iter().collect(),
        present: trimmed(&present).to_vec(),
    }
}

#[test]
fn random_streams_round_trip() {
    let mut r = Rng(0xb17_5ec7);
    for round in 0..400 {
        let n_slots = 1 + r.below(if round % 10 == 0 { 100_000 } else { 3000 });
        let s = random_stream(&mut r, n_slots);
        let bytes = s.encode();
        assert_eq!(decoded(&bytes, n_slots).unwrap(), s, "round {round}");
        // And through the checked load an index takes.
        assert_eq!(decode(&bytes, n_slots).unwrap().entries(), s.entries.len(), "round {round}");
    }
}

#[test]
fn every_truncation_is_refused_and_no_bytes_panic() {
    let bytes = golden().encode();
    for n in 0..bytes.len() {
        assert!(decoded(&bytes[..n], 1024).is_err(), "{n} of {} bytes were accepted", bytes.len());
    }
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(decoded(&longer, 1024).is_err(), "a trailing byte was accepted");
    let mut r = Rng(0xf022);
    for _ in 0..30_000 {
        let mut b = bytes.clone();
        for _ in 0..1 + r.below(4) {
            let i = r.below(b.len() as u64) as usize;
            b[i] = r.next() as u8;
        }
        let _ = decoded(&b, 1024);
        let _ = decode(&b, 1024);
        let junk: Vec<u8> = (0..r.below(64)).map(|_| r.next() as u8).collect();
        let _ = decoded(&junk, u64::MAX);
    }
}

#[test]
fn each_malformed_field_is_refused() {
    let good = golden().encode();
    // Another version, and unknown flags.
    let mut b = good.clone();
    b[0] = 2;
    assert!(matches!(decoded(&b, 1024), Err(SectionError::Format(FormatError::UnsupportedVersion { found: 2, .. }))));
    let mut b = good.clone();
    b[2] = 1;
    assert!(decoded(&b, 1024).is_err(), "flags");
    // An entry count past the bytes, and one short of them.
    let mut b = good.clone();
    b[4..12].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decoded(&b, 1024).is_err(), "entry count");
    let mut b = good.clone();
    let n = u64::from_le_bytes(b[4..12].try_into().unwrap());
    b[4..12].copy_from_slice(&(n - 1).to_le_bytes());
    assert!(decoded(&b, 1024).is_err(), "an entry left over");
    // A slot or presence bit past the table: refused by the stream's checks.
    assert!(matches!(decode(&good, 1000), Err(SectionError::Reject(SortedReject::PastEnd { slot: 1000, .. }))));
    assert!(matches!(decoded(&good, 100), Err(SectionError::Reject(SortedReject::PastEnd { .. }))), "presence past n_slots");
    // Overlong and oversized varints.
    let mut r = Reader::new(&[0x80, 0x00], WHAT);
    assert!(varint(&mut r).is_err());
    let mut r = Reader::new(&[0xff; 11], WHAT);
    assert!(varint(&mut r).is_err());
    let mut r = Reader::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01], WHAT);
    assert_eq!(varint(&mut r).unwrap(), u64::MAX);
    // A restart that shares a prefix: the first entry (empty key) is
    // `00 00 03`; make it claim one shared byte.
    let mut b = good.clone();
    b[HEAD_LEN] = 1;
    assert!(decoded(&b, 1024).is_err(), "a restart sharing a prefix");
}

/// The writer refuses a stream out of order rather than write it.
#[test]
fn the_writer_refuses_parts_out_of_order() {
    let mut w = SectionWriter::new();
    assert_eq!(w.push(Sorted::Present(&[1])), Err(SortedReject::PartOrder));
    let mut w = SectionWriter::new();
    w.push(Sorted::Unkeyed(&[])).unwrap();
    assert_eq!(w.push(Sorted::Entry(b"a", &[1])), Err(SortedReject::PartOrder));
    let w = SectionWriter::new();
    assert_eq!(w.finish(), Err(SortedReject::Truncated));
    let mut w = SectionWriter::new();
    assert!(w.push(Sorted::Entry(b"a", &[2, 1])).is_err());
}
