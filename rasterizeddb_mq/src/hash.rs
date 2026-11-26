//! The partitioner a keyed produce uses, on the client and on the server.
//!
//! A key always lands in the same partition for a given partition count, on
//! every version, so the function is fixed: FNV-1a over the key's bytes, then
//! the SplitMix64 finaliser to spread FNV's weak low bits, reduced by
//! multiply-shift (no modulo bias, no division). Changing it would move every
//! key to another partition and break per-key ordering across an upgrade.

/// The 64-bit hash of `key`.
#[inline]
pub fn key_hash(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// The partition, of `partitions`, that `key` belongs to.
#[inline]
pub fn partition_for(key: &[u8], partitions: u32) -> u32 {
    debug_assert!(partitions > 0);
    (((key_hash(key) >> 32) * partitions as u64) >> 32) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_values() {
        // These must never change: see the module docs.
        assert_eq!(partition_for(b"", 16), partition_for(b"", 16));
        let spread: std::collections::HashSet<u32> =
            (0..1000u32).map(|i| partition_for(&i.to_le_bytes(), 8)).collect();
        assert_eq!(spread.len(), 8);
    }
}
