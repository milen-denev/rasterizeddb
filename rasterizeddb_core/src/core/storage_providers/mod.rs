pub mod backend;
pub mod file_sync;
pub mod helpers;
#[cfg(feature = "kv_storage")]
pub mod kv;
pub mod memory;
pub mod mock_file_sync;
pub mod traits;

/// CRC-32/ISO-HDLC (the zlib/PNG polynomial), the checksum every durable
/// format here carries: WAL records, the replication log, backup archive
/// entries, KV layout batches and object metadata.
///
/// Computed by `crc32fast`, which uses PCLMULQDQ where the CPU has it and a
/// slice-by-16 table where it does not. This was `crc::Crc::<u32>` with the
/// default byte-at-a-time table, measured at ~0.5 GB/s, and every byte of every
/// backed-up file went through it. The polynomial, reflection, initial value
/// and final XOR are the same, so every stored checksum is unchanged — pinned
/// by `crc_tests::matches_the_previous_implementation` against the old code.
///
/// The API is the subset callers used of `crc::Crc`: `checksum` and a
/// streaming `digest`.
pub struct Crc32;

impl Crc32 {
    #[inline]
    pub fn checksum(&self, bytes: &[u8]) -> u32 {
        crc32fast::hash(bytes)
    }

    #[inline]
    pub fn digest(&self) -> Crc32Digest {
        Crc32Digest(crc32fast::Hasher::new())
    }
}

/// A streaming [`Crc32`]: `update` any number of times, then `finalize`.
pub struct Crc32Digest(crc32fast::Hasher);

impl Crc32Digest {
    #[inline]
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    #[inline]
    pub fn finalize(self) -> u32 {
        self.0.finalize()
    }
}

pub const CRC: Crc32 = Crc32;

#[cfg(test)]
mod crc_tests {
    use super::CRC;
    use crc::{CRC_32_ISO_HDLC, Crc};

    /// The implementation this replaced, kept as the reference.
    const OLD: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

    #[test]
    fn the_check_value_is_the_catalogue_one() {
        assert_eq!(CRC.checksum(b"123456789"), 0xCBF4_3926);
        assert_eq!(OLD.checksum(b"123456789"), 0xCBF4_3926);
        assert_eq!(CRC.checksum(b""), 0);
        assert_eq!(CRC.checksum(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }

    #[test]
    fn matches_the_previous_implementation() {
        // Every length 0..=4096 with random contents, plus all-zero and
        // all-ones buffers, and every way of splitting a buffer across
        // `update` calls at a few lengths — the WAL and archive stream their
        // input in pieces, so the digest path matters as much as `checksum`.
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for len in 0..=4096usize {
            let data: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            assert_eq!(CRC.checksum(&data), OLD.checksum(&data), "random, len {}", len);
            let zeros = vec![0u8; len];
            assert_eq!(CRC.checksum(&zeros), OLD.checksum(&zeros), "zeros, len {}", len);
            let ones = vec![0xFFu8; len];
            assert_eq!(CRC.checksum(&ones), OLD.checksum(&ones), "ones, len {}", len);

            let cut = if len == 0 { 0 } else { (next() as usize) % (len + 1) };
            let mut new_digest = CRC.digest();
            new_digest.update(&data[..cut]);
            new_digest.update(&data[cut..]);
            let mut old_digest = OLD.digest();
            old_digest.update(&data[..cut]);
            old_digest.update(&data[cut..]);
            assert_eq!(new_digest.finalize(), old_digest.finalize(), "split {} of {}", cut, len);
        }
        for len in [1usize, 7, 64, 333] {
            let data: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            for cut in 0..=len {
                let mut d = CRC.digest();
                d.update(&data[..cut]);
                d.update(&[]);
                d.update(&data[cut..]);
                assert_eq!(d.finalize(), OLD.checksum(&data), "split {} of {}", cut, len);
            }
        }
        // Large inputs cross the hardware path's folding block boundaries.
        for len in [65_536usize, 1_048_577] {
            let data: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            assert_eq!(CRC.checksum(&data), OLD.checksum(&data), "len {}", len);
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored crc_throughput --nocapture`
    #[test]
    #[ignore]
    fn crc_throughput() {
        use crc::Table;
        const SLICE16: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISO_HDLC);
        let sizes = [64usize, 4096, 1 << 20];
        for size in sizes {
            let data: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            let total: usize = 512 << 20;
            let rounds = (total / size).max(1);
            for round in 0..3 {
                let mut sink = 0u32;
                let t = std::time::Instant::now();
                for _ in 0..rounds {
                    sink ^= OLD.checksum(std::hint::black_box(&data));
                }
                let old = t.elapsed();
                let t = std::time::Instant::now();
                for _ in 0..rounds {
                    sink ^= SLICE16.checksum(std::hint::black_box(&data));
                }
                let s16 = t.elapsed();
                let t = std::time::Instant::now();
                for _ in 0..rounds {
                    sink ^= CRC.checksum(std::hint::black_box(&data));
                }
                let new = t.elapsed();
                let gbps = |d: std::time::Duration| (rounds * size) as f64 / d.as_secs_f64() / 1e9;
                println!(
                    "size {:>8} round {}: table1 {:6.2} GB/s  table16 {:6.2} GB/s  crc32fast {:6.2} GB/s  ({})",
                    size,
                    round,
                    gbps(old),
                    gbps(s16),
                    gbps(new),
                    sink
                );
            }
        }
    }
}
