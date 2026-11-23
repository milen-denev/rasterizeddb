//! CRC-32C (Castagnoli, the iSCSI polynomial; check value `0xE306_9283` for
//! `"123456789"`): the index store's file checksums (`.ixm`, `.ixs`,
//! `rdb.ixe`; `index_store::codec`), persisted, so the function must never
//! move.
//!
//! The SSE 4.2 `crc32` instruction computes exactly this function, one
//! 8-byte step per instruction, about five times slice-by-16 here; every
//! x86-64 server CPU of the last decade has it, the production i5-10400
//! included. Elsewhere, and as the reference the instruction is tested
//! against, the `crc` crate's slice-by-16 table. B18: the open's proof
//! checks every byte of a snapshot and the load each section again, on the
//! path to a table's indexes being ready.

/// Slice-by-16. A `static`, not a `const`: the table is 16 KiB and a
/// `const` would be copied into every use.
static TABLE: crc::Crc<u32, crc::Table<16>> = crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISCSI);

/// CRC-32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut d = Digest::new();
    d.update(bytes);
    d.finalize()
}

/// A CRC-32C over bytes fed in pieces: what [`crc32c`] gives for their
/// concatenation.
pub enum Digest {
    /// The instruction's running state, before the final inversion.
    #[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
    Hw(u32),
    Table(crc::Digest<'static, u32, crc::Table<16>>),
}

impl Default for Digest {
    fn default() -> Self {
        Digest::new()
    }
}

impl Digest {
    /// The instruction where the CPU has it, else the table.
    pub fn new() -> Digest {
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("sse4.2") {
            return Digest::Hw(!0);
        }
        Digest::table()
    }

    /// The table, whatever the CPU: the reference.
    pub fn table() -> Digest {
        Digest::Table(TABLE.digest())
    }

    pub fn update(&mut self, bytes: &[u8]) {
        match self {
            // Built only after `is_x86_feature_detected!("sse4.2")` said so.
            #[cfg(target_arch = "x86_64")]
            Digest::Hw(state) => *state = unsafe { sse42(*state, bytes) },
            #[cfg(not(target_arch = "x86_64"))]
            Digest::Hw(_) => unreachable!("built only on x86-64"),
            Digest::Table(d) => d.update(bytes),
        }
    }

    pub fn finalize(self) -> u32 {
        match self {
            Digest::Hw(state) => !state,
            Digest::Table(d) => d.finalize(),
        }
    }
}

/// CRC-32C's reflected state over `bytes`, by the `crc32` instruction.
///
/// # Safety
/// The CPU must have SSE 4.2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn sse42(state: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
    let mut words = bytes.chunks_exact(8);
    let mut wide = state as u64;
    for w in &mut words {
        wide = _mm_crc32_u64(wide, u64::from_le_bytes(w.try_into().expect("eight bytes")));
    }
    let mut state = wide as u32;
    for b in words.remainder() {
        state = _mm_crc32_u8(state, *b);
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_is_castagnoli() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }

    /// The instruction and the table are one function: every length to 300
    /// and longer ones, whole and in random pieces.
    #[test]
    fn the_instruction_is_the_table() {
        let mut rng = fastrand::Rng::with_seed(0xc4c3);
        let data: Vec<u8> = (0..70_000).map(|_| rng.u8(..)).collect();
        for len in (0..300usize).chain([4095, 4096, 4097, 65_537, 70_000]) {
            let bytes = &data[..len];
            let want = TABLE.checksum(bytes);
            assert_eq!(crc32c(bytes), want, "length {len}");
            let mut d = Digest::new();
            let mut t = Digest::table();
            let mut at = 0;
            while at < len {
                let n = rng.usize(0..=(len - at).min(37));
                d.update(&bytes[at..at + n]);
                t.update(&bytes[at..at + n]);
                at += n;
            }
            assert_eq!(d.finalize(), want, "length {len} in pieces");
            assert_eq!(t.finalize(), want, "length {len} in pieces, the table");
        }
        #[cfg(target_arch = "x86_64")]
        assert!(
            !std::arch::is_x86_feature_detected!("sse4.2") || matches!(Digest::new(), Digest::Hw(_)),
            "the instruction is there and not used"
        );
    }
}
