//! The generated buffer the XXH64 tests pin reference vectors over: xxhsum's
//! sanity buffer (`XSUM_fillTestBuffer`), byte for byte. A copy of the one in
//! `rasterizeddb_core::core::hash::test_buffers`, which rapidhash's tests use.

const PRIME32: u64 = 2_654_435_761;

/// xxhsum's step (`PRIME64` in `cli/xsum_sanity_check.c`).
const XSUM_STEP: u64 = 11_400_714_785_074_694_797;

pub(crate) fn xsum_buffer(len: usize) -> Vec<u8> {
    let mut byte_gen = PRIME32;
    (0..len)
        .map(|_| {
            let b = (byte_gen >> 56) as u8;
            byte_gen = byte_gen.wrapping_mul(XSUM_STEP);
            b
        })
        .collect()
}
