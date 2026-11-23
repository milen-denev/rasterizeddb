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
const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0x9a31_76c2_d6b6_ad91), (2, v2_digest, 0x08f8_e8fe_3eea_a6cc)];

/// [`golden`] encoded under `INDEX_SECTION_VER` 2, frozen: version 1's
/// bytes with the version raised and the chunk table after them.
const SECTION_V2: &str = "02000000170000000000000000000300016100010000010162020201630a0300\
    0003000003000003000003000003000003000003000003000003000003000000\
    036162631603000003000002016407001effffffffffffffffffffffffffffff\
    ffffffffffffffffffffffffffffffc8011e009f061e01ff05030401a5020800\
    0201013c40050101000c00077906000000";

fn v2_digest() -> u64 {
    digest_hex(&[SECTION_V2])
}

#[test]
fn a_section_encodes_to_its_frozen_bytes() {
    let got = golden().encode();
    assert_eq!(to_hex(&got), to_hex(&from_hex(SECTION_V2)), "the INDEX section layout moved");
    assert_eq!(decoded(&got, 1024).unwrap(), golden());
    // Version 1 is still read, as one chunk: a snapshot written before 2
    // loads rather than rebuilds.
    let v1 = from_hex(SECTION_V1);
    assert_eq!(decoded(&v1, 1024).unwrap(), golden());
    assert_eq!(decode(&v1, 1024).unwrap().pairs(), golden().entries);
    assert_eq!(&got[2..v1.len()], &v1[2..], "version 2's body is version 1's");
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
    let v1 = from_hex(SECTION_V1);
    for n in 0..v1.len() {
        assert!(decoded(&v1[..n], 1024).is_err(), "{n} of version 1's {} bytes were accepted", v1.len());
    }
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
    b[0] = 3;
    assert!(matches!(decoded(&b, 1024), Err(SectionError::Format(FormatError::UnsupportedVersion { found: 3, .. }))));
    let mut b = good.clone();
    b[0] = 0;
    assert!(matches!(decoded(&b, 1024), Err(SectionError::Format(FormatError::UnsupportedVersion { found: 0, .. }))));
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

/// `keys` keys of 8 bytes; every `every`th has `width` slots, the rest one:
/// past [`CHUNK_ENTRIES`] entries, so the section has several chunks.
fn big_stream(keys: u64, every: u64, width: u64) -> Stream {
    let mut slot = 0u64;
    let entries = (0..keys)
        .map(|k| {
            let n = if k % every == 0 { width } else { 1 };
            let slots: Vec<u64> = (slot..slot + n).collect();
            slot += n;
            ((k * 3).to_be_bytes().to_vec(), slots)
        })
        .collect();
    Stream { entries, unkeyed: vec![slot + 1], present: vec![] }
}

/// A version 2 section's chunk table, read back: each chunk's first entry,
/// offset and key ordinal; the key count; the tail's offset.
fn table_of(bytes: &[u8]) -> (Vec<(u64, u64, u64)>, u64, u64) {
    let last4 = bytes.len() - 4;
    let len = u32::from_le_bytes(bytes[last4..].try_into().unwrap()) as usize;
    let mut r = Reader::within(bytes, last4 - len, last4, WHAT);
    let n = varint(&mut r).unwrap();
    let mut prev = (0, 0, 0);
    let chunks = (0..n)
        .map(|_| {
            prev = (prev.0 + varint(&mut r).unwrap(), prev.1 + varint(&mut r).unwrap(), prev.2 + varint(&mut r).unwrap());
            prev
        })
        .collect();
    (chunks, varint(&mut r).unwrap(), varint(&mut r).unwrap())
}

/// `bytes` with its chunk table replaced by `chunks`, `n_keys` and `tail`
/// (chunks given as absolute values, possibly out of order: the
/// differences are written as they come, wrapping).
fn with_table(bytes: &[u8], chunks: &[(u64, u64, u64)], n_keys: u64, tail: u64) -> Vec<u8> {
    let last4 = bytes.len() - 4;
    let len = u32::from_le_bytes(bytes[last4..].try_into().unwrap()) as usize;
    let mut out = bytes[..last4 - len].to_vec();
    let start = out.len();
    put_varint(&mut out, chunks.len() as u64);
    let mut prev = (0u64, 0u64, 0u64);
    for &(e, o, k) in chunks {
        put_varint(&mut out, e.wrapping_sub(prev.0));
        put_varint(&mut out, o.wrapping_sub(prev.1));
        put_varint(&mut out, k.wrapping_sub(prev.2));
        prev = (e, o, k);
    }
    put_varint(&mut out, n_keys);
    put_varint(&mut out, tail);
    let len = (out.len() - start) as u32;
    out.extend_from_slice(&len.to_le_bytes());
    out
}

fn n_slots_of(s: &Stream) -> u64 {
    s.unkeyed.last().map_or(1, |u| u + 1)
}

/// A section of several chunks decodes, on threads and serially, to the
/// stream it was written from; so do streams whose wide keys leave few
/// places a chunk can start, and one key wider than a chunk.
#[test]
fn a_section_of_many_chunks_decodes_as_its_stream() {
    for (keys, every, width, at_least) in [
        (300_000, 7, 3, 4),
        (40_000, 1, 33, 4),
        // No key but the first starts at a restart: one chunk.
        (3, 2, CHUNK_ENTRIES * 2 + 5, 1),
        (3, 2, CHUNK_ENTRIES * 2 + 15, 2),
        (CHUNK_ENTRIES - 1, 1, 1, 1),
    ] {
        let s = big_stream(keys, every, width);
        let bytes = s.encode();
        let (chunks, n_keys, _) = table_of(&bytes);
        assert!(chunks.len() >= at_least, "{keys}/{every}/{width}: {} chunks", chunks.len());
        assert_eq!(n_keys, keys);
        for w in chunks.windows(2) {
            assert!(w[1].0 - w[0].0 >= CHUNK_ENTRIES && w[1].0 % RESTART_EVERY == 0);
        }
        let n_slots = n_slots_of(&s);
        assert_eq!(decoded(&bytes, n_slots).unwrap(), s, "{keys}/{every}/{width} serially");
        let load = decode(&bytes, n_slots).unwrap();
        assert_eq!(load.pairs(), s.entries, "{keys}/{every}/{width} on threads");
        assert_eq!(load.into_parts_for_test().unwrap(), (s.unkeyed.clone(), s.present.clone()));
    }
}

/// Every rule of the chunk table, broken one at a time, is refused, by the
/// threaded decode and the serial one alike; a table with fewer chunks is
/// another valid layout of the same entries.
#[test]
fn each_malformed_chunk_table_is_refused() {
    let s = big_stream(300_000, 7, 3);
    let n_slots = n_slots_of(&s);
    let good = s.encode();
    let (chunks, n_keys, tail) = table_of(&good);
    assert!(chunks.len() >= 4);
    assert_eq!(decode(&with_table(&good, &chunks, n_keys, tail), n_slots).unwrap().pairs(), s.entries);
    let refused = |b: &[u8], what: &str| {
        assert!(decode(b, n_slots).is_err(), "{what}: decoded on threads");
        assert!(decoded(b, n_slots).is_err(), "{what}: decoded serially");
    };
    let set = |i: usize, f: &dyn Fn(&mut (u64, u64, u64))| {
        let mut c = chunks.clone();
        f(&mut c[i]);
        c
    };
    refused(&with_table(&good, &set(0, &|c| c.1 += 1), n_keys, tail), "the first chunk past the head");
    refused(&with_table(&good, &set(0, &|c| c.2 = 1), n_keys, tail), "the first chunk's key not 0");
    refused(&with_table(&good, &set(2, &|c| c.0 += RESTART_EVERY), n_keys, tail), "a chunk's entry moved");
    refused(&with_table(&good, &set(2, &|c| c.0 += 1), n_keys, tail), "a chunk at no restart");
    refused(&with_table(&good, &set(2, &|c| c.1 += 1), n_keys, tail), "a chunk's offset moved");
    refused(&with_table(&good, &set(2, &|c| c.1 -= 1), n_keys, tail), "a chunk's offset moved back");
    refused(&with_table(&good, &set(2, &|c| c.2 += 1), n_keys, tail), "a chunk's key ordinal moved");
    refused(&with_table(&good, &set(2, &|c| c.2 -= 1), n_keys, tail), "a chunk's key ordinal moved back");
    refused(&with_table(&good, &set(2, &|c| *c = chunks[1]), n_keys, tail), "a chunk repeated");
    let mut swapped = chunks.clone();
    swapped.swap(1, 2);
    refused(&with_table(&good, &swapped, n_keys, tail), "chunks out of order");
    refused(&with_table(&good, &chunks, n_keys + 1, tail), "one key too many");
    refused(&with_table(&good, &chunks, n_keys - 1, tail), "one key too few");
    refused(&with_table(&good, &chunks, n_keys, tail + 1), "the tail moved");
    refused(&with_table(&good, &chunks, n_keys, tail - 1), "the tail moved back");
    refused(&with_table(&good, &[], n_keys, tail), "no chunk for the entries");
    let last = *chunks.last().unwrap();
    refused(&with_table(&good, &[chunks.clone(), vec![(last.0 + 16, tail, last.2 + 1)]].concat(), n_keys, tail), "a chunk at the tail");
    // A chunk starting mid-key: an entry that is a restart inside a wide
    // key (key 7k's three entries). The entries are whole there, so only
    // the join sees the key split in two.
    let (mut e, mut key) = (0u64, 0u64);
    let (mid_entry, mid_key) = loop {
        let width = if key % 7 == 0 { 3 } else { 1 };
        if (e + 1..e + width).any(|x| x % RESTART_EVERY == 0) && e > chunks[1].0 + 64 && e < chunks[2].0 - 64 {
            break ((e + 1..e + width).find(|x| x % RESTART_EVERY == 0).unwrap(), key);
        }
        e += width;
        key += 1;
    };
    let mut offset_of = None;
    decode_stretch_offsets(&good, |entry, at| {
        if entry == mid_entry {
            offset_of = Some(at);
        }
    });
    // Counted as the key's or the next's, the chunks' key counts disagree.
    for key in [mid_key, mid_key + 1] {
        let mut split = chunks.clone();
        split.insert(2, (mid_entry, offset_of.unwrap() as u64, key));
        refused(&with_table(&good, &split, n_keys, tail), "a key split across chunks");
    }
    // Fewer chunks: the same entries, decoded.
    let fewer: Vec<_> = chunks.iter().copied().step_by(2).collect();
    assert_eq!(decode(&with_table(&good, &fewer, n_keys, tail), n_slots).unwrap().pairs(), s.entries);
    assert_eq!(decoded(&with_table(&good, &chunks[..1], n_keys, tail), n_slots).unwrap(), s);
}

/// Each entry's ordinal and byte offset in a section (test helper: walks
/// the entries the way the decoder does).
fn decode_stretch_offsets(bytes: &[u8], mut at: impl FnMut(u64, usize)) {
    let n = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let mut r = Reader::within(bytes, HEAD_LEN, bytes.len(), WHAT);
    for i in 0..n {
        at(i, r.pos() as usize);
        let _shared = varint(&mut r).unwrap();
        let unshared = varint(&mut r).unwrap();
        r.take(unshared as usize).unwrap();
        varint(&mut r).unwrap();
    }
}

/// Rotted bytes anywhere in a many-chunk section never panic either path.
#[test]
fn no_bytes_panic_a_many_chunk_decode() {
    let s = big_stream(150_000, 5, 3);
    let n_slots = n_slots_of(&s);
    let good = s.encode();
    let mut r = Rng(0x5ec7_10a2);
    for round in 0..300 {
        let mut b = good.clone();
        let near_end = round % 2 == 0;
        for _ in 0..1 + r.below(3) {
            let i = if near_end { b.len() - 1 - r.below(40) as usize } else { r.below(b.len() as u64) as usize };
            b[i] = r.next() as u8;
        }
        let _ = decode(&b, n_slots);
        let _ = decoded(&b, n_slots);
    }
}

/// [`SortedLoad::assemble`] takes what pushing every entry in order would:
/// it refuses a stretch whose first key does not follow the one before's
/// last, a stretch left short or overfull, and an entry its stretch's checks
/// refuse; the first failure in stretch order is the one returned; and what
/// was written is dropped, not leaked or dropped twice, when a decode fails
/// or panics (the keys here are heap ones, so a double drop would abort).
#[test]
fn an_assembly_checks_what_pushing_in_order_checks() {
    let key = |i: usize| format!("{i:040}").into_bytes();
    let run = |keys: Vec<Vec<usize>>, counts: Vec<usize>, workers: usize| {
        SortedLoad::<KeyBuf>::assemble(100, &counts, workers, |i, stretch| -> Result<(), SectionError> {
            for &k in &keys[i] {
                stretch.push(&key(k), &[k as u64])?;
            }
            Ok(())
        })
    };
    for workers in [1, 4] {
        let load = run(vec![vec![0, 1, 2], vec![3], vec![4, 5]], vec![3, 1, 2], workers).unwrap();
        assert_eq!(load.pairs().iter().map(|p| p.0.clone()).collect::<Vec<_>>(), (0..6).map(key).collect::<Vec<_>>());
        let refused = |keys: Vec<Vec<usize>>, counts: Vec<usize>| run(keys, counts, workers).err().expect("refused");
        assert_eq!(refused(vec![vec![0, 1, 2], vec![2], vec![4, 5]], vec![3, 1, 2]), SectionError::Reject(SortedReject::KeyOrder { at: 3 }));
        assert_eq!(refused(vec![vec![0, 1, 2], vec![1], vec![4, 5]], vec![3, 1, 2]), SectionError::Reject(SortedReject::KeyOrder { at: 3 }));
        assert_eq!(refused(vec![vec![0, 2, 1], vec![3]], vec![3, 1]), SectionError::Reject(SortedReject::KeyOrder { at: 2 }));
        assert_eq!(refused(vec![vec![0, 1], vec![3]], vec![3, 1]), SectionError::Reject(SortedReject::Truncated), "short");
        assert_eq!(refused(vec![vec![0, 1, 2, 3], vec![4]], vec![3, 1]), SectionError::Reject(SortedReject::KeyOrder { at: 3 }), "overfull");
        assert!(matches!(refused(vec![vec![0], vec![150]], vec![1, 1]), SectionError::Reject(SortedReject::PastEnd { .. })));
        // Two failures: the earlier stretch's is reported.
        assert_eq!(refused(vec![vec![0, 1], vec![9, 8], vec![7, 6]], vec![3, 2, 2]), SectionError::Reject(SortedReject::Truncated));
        let empty = SortedLoad::<KeyBuf>::assemble(10, &[], workers, |_, _| -> Result<(), SectionError> { unreachable!() }).unwrap();
        assert_eq!(empty.entries(), 0);
        // A decode that panics midway: the panic comes out, and the entries
        // written are dropped once.
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = SortedLoad::<KeyBuf>::assemble(100, &[2, 2], workers, |i, stretch| -> Result<(), SectionError> {
                stretch.push(&key(i * 2), &[1])?;
                if i == 1 {
                    panic!("a decode panicked");
                }
                stretch.push(&key(i * 2 + 1), &[2])?;
                Ok(())
            });
        }));
        assert!(panicked.is_err());
    }
}
