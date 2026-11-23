//! The generated buffers the hash tests pin reference vectors over.
//!
//! Each byte is the top byte of a 64-bit generator that starts at PRIME32
//! and is multiplied by a fixed odd step after every byte, so the bytes never
//! run out (unlike squaring a `u32`, which reaches 1 within 30 steps and
//! leaves only zeros after the 28th byte).

/// The generator's start: XXH32's PRIME32_1.
const PRIME32: u64 = 2_654_435_761;

/// xxhsum's step (`PRIME64` in `cli/xsum_sanity_check.c`). Not XXH64's
/// PRIME64_1, which ends `...CA87`: this one ends `...CA8D`.
pub(super) const XSUM_STEP: u64 = 11_400_714_785_074_694_797;

/// XXH64's PRIME64_1. rapidhash's vectors were generated over this step, so
/// they stay on it.
pub(super) const PRIME64_1_STEP: u64 = 0x9E37_79B1_85EB_CA87;

/// `len` bytes from the generator, multiplying by `step`.
pub(super) fn generated(len: usize, step: u64) -> Vec<u8> {
    let mut byte_gen = PRIME32;
    (0..len)
        .map(|_| {
            let b = (byte_gen >> 56) as u8;
            byte_gen = byte_gen.wrapping_mul(step);
            b
        })
        .collect()
}

/// xxhsum's sanity buffer (`XSUM_fillTestBuffer`), byte for byte.
pub(super) fn xsum_buffer(len: usize) -> Vec<u8> {
    generated(len, XSUM_STEP)
}
