//! XXH64, in tree.
//!
//! Partition placement is persisted, so the hash under it must never move: a
//! crate update, a target feature or a pointer width that changed one output
//! would strand rows in a partition no query routes to. This is the reference
//! algorithm (xxHash spec, "XXH64"), little-endian reads on every target, and
//! the tests pin it to the C library's own outputs.

const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline(always)]
fn read_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

#[inline(always)]
fn read_u32(b: &[u8]) -> u64 {
    u32::from_le_bytes(b[..4].try_into().unwrap()) as u64
}

#[inline(always)]
fn round(acc: u64, lane: u64) -> u64 {
    acc.wrapping_add(lane.wrapping_mul(P2)).rotate_left(31).wrapping_mul(P1)
}

#[inline(always)]
fn merge_round(acc: u64, v: u64) -> u64 {
    (acc ^ round(0, v)).wrapping_mul(P1).wrapping_add(P4)
}

/// XXH64 of `input` under `seed`.
pub fn xxh64(input: &[u8], seed: u64) -> u64 {
    let len = input.len();
    let mut rest = input;

    let mut h = if len >= 32 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);
        while rest.len() >= 32 {
            v1 = round(v1, read_u64(rest));
            v2 = round(v2, read_u64(&rest[8..]));
            v3 = round(v3, read_u64(&rest[16..]));
            v4 = round(v4, read_u64(&rest[24..]));
            rest = &rest[32..];
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        merge_round(h, v4)
    } else {
        seed.wrapping_add(P5)
    };

    h = h.wrapping_add(len as u64);

    while rest.len() >= 8 {
        h ^= round(0, read_u64(rest));
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        rest = &rest[8..];
    }
    if rest.len() >= 4 {
        h ^= read_u32(rest).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        rest = &rest[4..];
    }
    for &b in rest {
        h ^= (b as u64).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
    }

    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^ (h >> 32)
}

#[cfg(test)]
mod tests {
    use super::xxh64;

    /// xxhsum's sanity buffer: 101 bytes from squaring PRIME32 (`xxhsum.c`,
    /// `BMK_sanityCheck`).
    fn sanity_buffer() -> Vec<u8> {
        let mut byte_gen: u32 = 2_654_435_761;
        (0..101)
            .map(|_| {
                let b = (byte_gen >> 24) as u8;
                byte_gen = byte_gen.wrapping_mul(byte_gen);
                b
            })
            .collect()
    }

    const PRIME32: u64 = 2_654_435_761;

    /// Every value below is the output of libxxhash 0.8.3 (the C reference,
    /// called through ctypes); the 0, 1, 14 and 101-byte rows are also
    /// xxhsum's published sanity table. Lengths cover the tail-only path, each
    /// tail step, and both sides of the 32-byte stripe.
    #[test]
    fn matches_the_reference_on_the_sanity_buffer() {
        let buf = sanity_buffer();
        let cases: &[(usize, u64, u64)] = &[
            (0, 0, 0xEF46_DB37_51D8_E999),
            (0, PRIME32, 0xAC75_FDA2_929B_17EF),
            (1, 0, 0x4FCE_394C_C889_52D8),
            (1, PRIME32, 0x7398_40CB_819F_A723),
            (4, 0, 0x9256_E58A_A397_AEF1),
            (4, PRIME32, 0x09D5_FFDF_B928_AB4B),
            (14, 0, 0xCFFA_8DB8_81BC_3A3D),
            (14, PRIME32, 0x5B96_1158_5EFC_C9CB),
            (31, 0, 0xAD09_D9A6_941D_D847),
            (31, PRIME32, 0x9C90_D9D9_C2E3_D340),
            (32, 0, 0xAF57_53D3_9159_EDEE),
            (32, PRIME32, 0xDCAB_9233_B8CA_7B0F),
            (33, 0, 0x6711_CBDD_8543_BAA8),
            (33, PRIME32, 0x66E9_CECF_2F1D_E71C),
            (63, 0, 0xFF44_10E1_7CE1_1EFA),
            (63, PRIME32, 0x7F71_5F51_D0F2_6050),
            (64, 0, 0x18F5_388F_1D2B_A08C),
            (64, PRIME32, 0x479E_7103_CF9A_A020),
            (101, 0, 0x0EAB_5433_84F8_78AD),
            (101, PRIME32, 0xCAA6_5939_306F_1E21),
        ];
        for &(n, seed, want) in cases {
            assert_eq!(xxh64(&buf[..n], seed), want, "len {n} seed {seed:#x}");
        }
    }

    #[test]
    fn matches_the_reference_on_text() {
        // The second column is the level-0 routing seed, so the seed's high
        // bits are exercised too.
        let seed = 0x5244_4250_5254_0000;
        let cases: &[(&[u8], u64, u64)] = &[
            (b"", 0xEF46_DB37_51D8_E999, 0xC9A7_5FE0_691A_A447),
            (b"a", 0xD24E_C4F1_A98C_6E5B, 0x517C_848B_BED8_755F),
            (b"abc", 0x44BC_2CF5_AD77_0999, 0x516F_8EE8_F21B_C891),
            (
                b"Nobody inspects the spammish repetition",
                0xFBCE_A83C_8A37_8BF1,
                0xBD64_36EF_6CC9_E9FF,
            ),
            (
                b"The quick brown fox jumps over the lazy dog",
                0x0B24_2D36_1FDA_71BC,
                0xAD3E_5BB8_C0C8_F3E1,
            ),
        ];
        for &(input, at_zero, at_seed) in cases {
            assert_eq!(xxh64(input, 0), at_zero, "{:?}", std::str::from_utf8(input));
            assert_eq!(xxh64(input, seed), at_seed, "{:?}", std::str::from_utf8(input));
        }
    }
}
