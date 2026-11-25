//! The generated buffers rapidhash's tests pin reference vectors over
//! (XXH64's, with xxhsum's buffer, moved to `rasterizeddb_router`).
//!
//! Each byte is the top byte of a 64-bit generator that starts at PRIME32
//! and is multiplied by a fixed odd step after every byte, so the bytes never
//! run out (unlike squaring a `u32`, which reaches 1 within 30 steps and
//! leaves only zeros after the 28th byte).

/// The generator's start: XXH32's PRIME32_1.
const PRIME32: u64 = 2_654_435_761;

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
