//! XXH64, in tree.
//!
//! Anything persisted that a hash decides must never move: a crate update, a
//! target feature or a pointer width that changed one output would strand
//! rows in a partition no query routes to, or make a persisted inverted index
//! answer a query with items its runs were never written under. This is the
//! reference algorithm (xxHash spec, "XXH64"), little-endian reads on every
//! target, and the tests pin it to the C library's own outputs.
//!
//! [`xxh64`] hashes one buffer. [`Xxh64`] is the same function fed in pieces,
//! for a caller that builds its input from many small fields and must not
//! allocate per hash; its digest equals [`xxh64`] of the concatenated pieces,
//! whatever the split.

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

#[inline(always)]
fn initial_lanes(seed: u64) -> [u64; 4] {
    [
        seed.wrapping_add(P1).wrapping_add(P2),
        seed.wrapping_add(P2),
        seed,
        seed.wrapping_sub(P1),
    ]
}

#[inline(always)]
fn stripe(v: &mut [u64; 4], s: &[u8]) {
    v[0] = round(v[0], read_u64(s));
    v[1] = round(v[1], read_u64(&s[8..]));
    v[2] = round(v[2], read_u64(&s[16..]));
    v[3] = round(v[3], read_u64(&s[24..]));
}

#[inline(always)]
fn converge(v: &[u64; 4]) -> u64 {
    let mut h = v[0]
        .rotate_left(1)
        .wrapping_add(v[1].rotate_left(7))
        .wrapping_add(v[2].rotate_left(12))
        .wrapping_add(v[3].rotate_left(18));
    h = merge_round(h, v[0]);
    h = merge_round(h, v[1]);
    h = merge_round(h, v[2]);
    merge_round(h, v[3])
}

/// The tail (fewer than 32 bytes) and the avalanche, shared by both forms.
#[inline(always)]
fn finish(mut h: u64, total_len: u64, mut rest: &[u8]) -> u64 {
    h = h.wrapping_add(total_len);
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

/// XXH64 of `input` under `seed`.
pub fn xxh64(input: &[u8], seed: u64) -> u64 {
    let len = input.len();
    let mut rest = input;

    let h = if len >= 32 {
        let mut v = initial_lanes(seed);
        while rest.len() >= 32 {
            stripe(&mut v, rest);
            rest = &rest[32..];
        }
        converge(&v)
    } else {
        seed.wrapping_add(P5)
    };

    finish(h, len as u64, rest)
}

/// XXH64 fed in pieces: [`Xxh64::digest`] equals [`xxh64`] of everything
/// passed to [`Xxh64::update`], concatenated. Holds at most one 32-byte
/// stripe; never allocates.
#[derive(Clone)]
pub struct Xxh64 {
    seed: u64,
    lanes: [u64; 4],
    buf: [u8; 32],
    buffered: usize,
    total: u64,
}

impl Xxh64 {
    pub fn new(seed: u64) -> Xxh64 {
        Xxh64 {
            seed,
            lanes: initial_lanes(seed),
            buf: [0; 32],
            buffered: 0,
            total: 0,
        }
    }

    pub fn update(&mut self, mut input: &[u8]) {
        self.total = self.total.wrapping_add(input.len() as u64);
        if self.buffered > 0 {
            let take = (32 - self.buffered).min(input.len());
            self.buf[self.buffered..self.buffered + take].copy_from_slice(&input[..take]);
            self.buffered += take;
            input = &input[take..];
            if self.buffered < 32 {
                return;
            }
            let buf = self.buf;
            stripe(&mut self.lanes, &buf);
            self.buffered = 0;
        }
        while input.len() >= 32 {
            stripe(&mut self.lanes, input);
            input = &input[32..];
        }
        self.buf[..input.len()].copy_from_slice(input);
        self.buffered = input.len();
    }

    pub fn digest(&self) -> u64 {
        let h = if self.total >= 32 {
            converge(&self.lanes)
        } else {
            self.seed.wrapping_add(P5)
        };
        finish(h, self.total, &self.buf[..self.buffered])
    }
}

#[cfg(test)]
mod tests {
    use super::{Xxh64, xxh64};
    use crate::core::hash::test_buffers::xsum_buffer;

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
    const SANITY: &[(usize, u64, u64)] = &[
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

    #[test]
    fn matches_the_reference_on_the_sanity_buffer() {
        let buf = sanity_buffer();
        for &(n, seed, want) in SANITY {
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

    /// Partition routing's seeds: level `i` hashes under `LEVEL_SEED + i`
    /// (`partition::key::level_hash`, `LEVEL_SEED` = 0x5244_4250_5254_0000),
    /// for levels 0 to 2 (`MAX_ARITY` is 3).
    const ROUTING_SEEDS: [u64; 3] = [0x5244_4250_5254_0000, 0x5244_4250_5254_0001, 0x5244_4250_5254_0002];

    /// xxhsum's own published XXH64 table (`XSUM_XXH64_testdata` in
    /// `cli/xsum_sanity_check.c` at tag v0.8.4), copied as published. They
    /// show [`xsum_buffer`], which [`XSUM`] is pinned over, is xxhsum's.
    const XSUM_PUBLISHED: &[(usize, u64, u64)] = &[
        (0, 0, 0xEF46_DB37_51D8_E999),
        (0, PRIME32, 0xAC75_FDA2_929B_17EF),
        (1, 0, 0xE934_A84A_DB05_2768),
        (1, PRIME32, 0x5014_6076_43A9_B4C3),
        (4, 0, 0x9136_A0DC_A574_57EE),
        (14, 0, 0x8282_DCC4_994E_35C8),
        (14, PRIME32, 0xC3BD_6BF6_3DEB_6DF0),
        (222, 0, 0xB641_AE8C_B691_C174),
        (222, PRIME32, 0x20CB_8AB7_AE10_C14A),
    ];

    /// Row `n` is the first `n` bytes of [`xsum_buffer`] under `[0,
    /// ROUTING_SEEDS[0], [1], [2]]`. [`SANITY`]'s buffer is zero from byte 28
    /// on, so its rows past 32 bytes barely move the lanes; these do. Every
    /// length from 0 to 240 crosses each tail step (8, 4, 1), one stripe
    /// with each tail length after it, and seven stripes.
    ///
    /// Generated by the C reference, xxHash (<https://github.com/Cyan4973/xxHash>)
    /// at release tag `v0.8.4` (commit
    /// `c87183a77d67f7d37e3d2d1b7eaac5e7c695e4f0`), `XXH64()` from its
    /// `xxhash.h`, nothing from a Rust crate. To regenerate:
    ///
    /// ```text
    /// git clone --depth 1 --branch v0.8.4 https://github.com/Cyan4973/xxHash
    /// cc -O2 -std=c99 -I xxHash -o gen gen.c && ./gen
    /// ```
    ///
    /// with `gen.c` as follows (its last line is [`XSUM_1024`]):
    ///
    /// ```c
    /// // XXH64 reference vectors for rasterizeddb_core/src/core/hash/xxh64.rs.
    /// // cc -O2 -std=c99 -I <xxHash at tag v0.8.4> -o gen gen.c && ./gen
    /// #define XXH_INLINE_ALL
    /// #include "xxhash.h"
    /// #include <stdio.h>
    /// #include <stdint.h>
    ///
    /// #define N 1024
    /// static void hex(uint64_t v) {
    ///     printf("0x%04X_%04X_%04X_%04X", (unsigned)(v >> 48), (unsigned)(v >> 32) & 0xFFFF,
    ///            (unsigned)(v >> 16) & 0xFFFF, (unsigned)v & 0xFFFF);
    /// }
    /// static void row(const uint8_t *buf, int n) {
    ///     /* seed 0, then partition routing's LEVEL_SEED + level for each level */
    ///     const uint64_t seeds[4] = {0, 0x5244425052540000ull, 0x5244425052540001ull, 0x5244425052540002ull};
    ///     printf("[");
    ///     for (int s = 0; s < 4; s++) {
    ///         hex(XXH64(buf, (size_t)n, seeds[s]));
    ///         printf(s < 3 ? ", " : "], // %d\n", n);
    ///     }
    /// }
    /// int main(void) {
    ///     static uint8_t buf[N];
    ///     uint64_t gen = 2654435761u;               /* xxhsum's XSUM_fillTestBuffer */
    ///     for (int i = 0; i < N; i++) {
    ///         buf[i] = (uint8_t)(gen >> 56);
    ///         gen *= 11400714785074694797ull;
    ///     }
    ///     for (int n = 0; n <= 240; n++) row(buf, n);
    ///     row(buf, N);
    ///     return 0;
    /// }
    /// ```
    #[rustfmt::skip]
    const XSUM: &[[u64; 4]] = &[
        [0xEF46_DB37_51D8_E999, 0xC9A7_5FE0_691A_A447, 0xB504_61E0_A864_2F2F, 0x22C4_6E8F_0064_4478], // 0
        [0xE934_A84A_DB05_2768, 0x4064_D88D_78FA_3E96, 0x4201_39BE_B83A_AB71, 0xED22_AD44_65BE_97B2], // 1
        [0x5D48_CD60_A77E_23FF, 0x1801_B858_57AD_046B, 0xB66B_E929_82B0_122B, 0xD980_659E_CFE2_93BA], // 2
        [0xFF7E_1959_CB50_794A, 0x4B85_0B6A_1DC6_C751, 0x8BBD_7E16_45B4_552F, 0x14FA_6A19_843C_3ED1], // 3
        [0x9136_A0DC_A574_57EE, 0x8206_BC5E_1B42_164A, 0x7972_B9EC_3471_48F1, 0x6790_2DBC_534C_BEE8], // 4
        [0x9B04_6FB1_397F_09A5, 0xE640_EE59_E12B_91C4, 0x366D_2FFE_519E_CBDB, 0x8796_D6BB_AB74_E27A], // 5
        [0xC725_65B7_1542_68A8, 0xD1E4_F9B5_C69A_7C76, 0x80A4_D31F_19DD_E7AD, 0x85BB_7EDB_BEE0_422E], // 6
        [0x6C83_909A_9F01_ED25, 0x8A85_9848_1F2A_E282, 0x1B34_DCA3_93E6_14A2, 0x72C4_E9F4_8678_DD99], // 7
        [0xCDBC_F538_E71D_1348, 0x341C_DA3E_1812_4F89, 0xF87C_41EC_B874_A455, 0x5EDE_8B83_E4BB_F7B0], // 8
        [0x554B_1AE9_91ED_A6B6, 0xD54C_02EF_3FDE_6F89, 0xE0CE_8151_ED97_977D, 0xAFF8_9DCE_2DED_A7D0], // 9
        [0x5D00_E735_1392_EA84, 0xC839_C347_F80F_7F74, 0xEE18_C8A0_B2B0_EC45, 0xDB40_2884_B740_6ECF], // 10
        [0x6345_D574_6F35_DA70, 0xE10C_5CCD_B61F_47AB, 0xC017_1BD9_0D12_2F46, 0xB9C9_F1D9_CF31_EB35], // 11
        [0x0723_BF50_086E_AD9A, 0x358F_4F8E_4B58_7B12, 0xD33E_C5D7_9CF8_E99D, 0x2991_92D8_0DED_7F31], // 12
        [0xC2E5_013E_3C40_BCF7, 0x2327_A8CD_7C75_C190, 0x8FD7_4FD2_D345_5C80, 0xC7D6_EDBC_E80A_3D3F], // 13
        [0x8282_DCC4_994E_35C8, 0x207B_3E80_D276_8C3B, 0x228F_961E_0CCA_3665, 0x2292_3BE4_1158_E93E], // 14
        [0x1807_1931_6D62_2D84, 0x1070_D6B5_1073_3AC2, 0x0D59_BEC6_4957_A7D4, 0x35D9_9F1D_B18B_1486], // 15
        [0x98C9_0B57_FDFC_B55C, 0x78E9_667B_8B3C_9627, 0x4AFD_B72D_9D65_EEF4, 0x0A93_D2C6_D0E1_C0A5], // 16
        [0x0D39_A2D0_51A3_0C2C, 0x70D4_BADF_831F_93CC, 0xFCFF_B154_32D6_652F, 0x200F_F29D_D0DD_9CAA], // 17
        [0x33E8_4A43_33B2_B2EB, 0x2F60_07EB_D499_3409, 0x029E_0EA7_F6AB_A7F5, 0xCABC_399E_96A5_AEB6], // 18
        [0xE91C_6EF3_1FC0_8F82, 0x122F_82B1_51E7_62C0, 0x847E_034E_D880_D120, 0x2310_7946_2CB9_F1ED], // 19
        [0x5F8C_6835_5769_439E, 0x6606_E513_0E6E_B032, 0xCA9C_4988_E8C5_70F2, 0x6CA6_79B3_648A_5E01], // 20
        [0x42B0_B8EE_353A_C461, 0xF837_D922_9AF5_CBB7, 0xB756_4AB6_8EA0_EE3C, 0x2DF7_46FB_2EA9_106C], // 21
        [0x65C9_35C6_9780_98B1, 0x955E_7D13_97BC_F0E8, 0x6080_9D7A_98F0_71DB, 0xCD66_EECA_33B8_74CA], // 22
        [0xD246_0ECC_840B_74DD, 0xCD47_3F82_B925_93E5, 0xD78C_C96A_6F45_4575, 0xD7D5_4047_1AA0_1E1F], // 23
        [0xF75A_6DEA_42DC_5BF4, 0xAFC0_2DBF_D220_3110, 0xDDAA_DE68_B2D9_BBBE, 0x5D55_BAA4_FC41_2EA1], // 24
        [0x52FA_A43C_3F20_B994, 0x7F53_3B58_64CB_E9CD, 0x2842_2FCF_D4C4_731A, 0x66CA_767D_7D76_C25D], // 25
        [0x8DB7_831E_C345_F9A3, 0x920E_2200_C47D_C1B0, 0x0D2A_64E6_910A_8250, 0x1E5E_DCFE_4C0A_4F3C], // 26
        [0x8894_5AA0_8051_FC2D, 0xCAD9_3D26_841D_D320, 0xDF00_7F98_FB30_E802, 0xF5F4_4975_7B31_6EBB], // 27
        [0x64CD_9E8C_96A9_E2DD, 0x7D2B_F966_BDD4_4DE8, 0x3E6A_0F8B_77C1_32D5, 0x0B67_B7AC_1C7D_6AB5], // 28
        [0x8C8F_345B_634A_C2B9, 0x0B3B_99D7_9966_5365, 0xC496_25F9_3137_574D, 0xF52A_0E83_0CB6_A9D5], // 29
        [0xE267_7241_D4C4_6CAF, 0x89D9_D275_BDA9_0F09, 0x57D9_0B90_BE43_13F6, 0xD12E_1FB5_7879_22B7], // 30
        [0x299B_39A2_90E6_D783, 0x1084_C942_39EA_5CB8, 0xFDF0_2673_6C0F_B678, 0x403E_BA53_AA99_2830], // 31
        [0x18B2_1649_2BB4_4B70, 0xE81C_A239_6935_9829, 0xB126_2B9A_94C1_CBFC, 0x12C9_838E_5756_57F1], // 32
        [0x55C8_DC3E_578F_5B59, 0xEB4A_9C6D_0AFF_716E, 0x27F3_1221_4AEB_6D3A, 0x74D7_CC0D_45F2_B997], // 33
        [0xD0CE_7CBA_E371_BEB2, 0xE4BB_3F0E_C54D_C6A2, 0x969D_4AD6_BA11_EEEE, 0x1136_6F24_92D6_02A0], // 34
        [0xCB0A_A8E2_A7A2_9707, 0x8744_9034_DAD1_31B4, 0x3557_92A7_8322_0F4F, 0x625D_49BA_871E_297F], // 35
        [0x0AE6_7584_084D_C6A4, 0xB26C_885A_EDAC_1F1D, 0xFAFF_9E27_3CC3_18FC, 0xA400_8341_7D61_25C2], // 36
        [0x292F_5332_C92B_73CE, 0x98C0_F7C4_91C7_4D28, 0x57DF_82E7_C7C7_9CA5, 0x946D_6C6D_8B8D_A4D2], // 37
        [0x5F39_8E2A_8478_BED3, 0xB6C1_2F12_F205_E0B4, 0xA339_C182_2FEF_EEBD, 0x25E3_7DC3_B59F_C38E], // 38
        [0xB888_CAF0_7592_B1B3, 0x5C55_1314_0158_4F6E, 0x3708_9F84_ED66_E83C, 0x57DC_96E6_DAF2_B1BF], // 39
        [0x23B3_810D_7D8B_1731, 0x1E1F_70A3_CFE3_CAAE, 0xD2B3_836A_6122_EA6B, 0x06E0_2B6C_14EC_0169], // 40
        [0x5342_C966_C7F3_D5F4, 0xB5BB_EC82_DA46_AB20, 0x5880_8B25_9E4A_6BD3, 0x5F2B_D888_3D16_44C9], // 41
        [0x7745_4B74_F1DB_BF63, 0x92CD_BD93_A786_6A14, 0x4ACC_2D39_C59F_4080, 0xCD25_7BF1_AE1B_03BF], // 42
        [0x9904_C935_0F59_7190, 0xB285_B32A_CCC7_E5F0, 0xD445_2883_9ACD_EA06, 0xAECD_8EF4_5115_406D], // 43
        [0x24EC_2B3D_0E44_2641, 0xD8BC_BE6E_9B98_010C, 0xE66E_1B32_71F4_A0A6, 0xD88E_B621_097F_BB21], // 44
        [0x4EC3_07EB_E0A6_50A4, 0xC51B_0BE5_FB22_65B2, 0x65D2_91AC_E2AF_3691, 0xE6F3_4BE7_FF5D_6721], // 45
        [0x7DD1_C551_4B22_40EF, 0x5AA9_7C74_352B_8A00, 0x44EF_58B9_95A2_BA35, 0x38D0_088C_BB7A_98C2], // 46
        [0x84A4_2E16_7CBD_1FEF, 0xD0C4_C7EA_10DD_5EBC, 0xCEF6_300F_3D32_FCF9, 0xEED1_17F1_DFD7_775D], // 47
        [0xFD0F_EEAC_7A93_9933, 0x51C5_A958_26C6_C832, 0x770E_EC67_B02A_CB7D, 0xC1A6_6B36_C437_F963], // 48
        [0xA91B_FF78_A718_5B66, 0x8447_6543_B1B7_3C28, 0xD697_C3BD_9072_2636, 0xE8F9_CBA4_29C6_A113], // 49
        [0xDFFE_9F29_3DDD_101D, 0x6F60_6880_2531_F285, 0xCD66_DD39_23F5_D117, 0xC0C4_01A7_6A44_8C15], // 50
        [0x2332_585C_18D7_483A, 0x44A5_F505_16DD_2C20, 0x6EBF_2853_6B96_94DF, 0xDC94_D04C_9956_EFC8], // 51
        [0x5A00_5651_1F9D_D653, 0xB86F_4513_B0BE_42E5, 0x83B1_6554_12A8_1C9E, 0xE605_5FEA_B722_9469], // 52
        [0x4426_A2F5_C8D5_9469, 0xEFDC_6752_0152_C67F, 0x32BF_2B78_653E_9680, 0x6A59_2086_067F_4C09], // 53
        [0x39EA_F889_3520_636A, 0xE9F8_896E_35E0_6CD9, 0x197D_5CA6_854B_64EA, 0xBE01_258F_AEBF_6612], // 54
        [0xAFEA_98CC_4E92_F7E1, 0xF963_9F7D_9C5C_0F1E, 0x5CD4_7855_B253_83B0, 0xF12F_901B_714E_3AAA], // 55
        [0xB7A6_414B_8D3E_2597, 0x4909_41E0_6495_111A, 0xB5E4_F1FD_895C_D11B, 0x728B_E606_EC38_2A34], // 56
        [0xF33D_6456_9CD4_B1C0, 0x7258_1924_9848_1FEC, 0x7FCB_F45B_4963_A6EE, 0xB457_990A_9620_242D], // 57
        [0x93FA_072F_0E8F_E7CA, 0x653A_E98A_3DB8_C4BD, 0xD4E7_A7B3_E099_2C61, 0x6CE0_D9C4_36AF_A5FD], // 58
        [0x4F28_9057_6949_D6ED, 0x9553_147C_01BB_C160, 0x8091_28A8_C3D6_4C8E, 0xC38E_7DB2_E9D8_153D], // 59
        [0x3378_C44F_459C_290B, 0xCE83_A4DD_9C46_C997, 0xFF16_AEFC_1AC3_06AF, 0x3477_60F8_3732_FDDB], // 60
        [0x40D4_86FD_DDAD_5CB7, 0xB35A_1D8C_27A3_5205, 0x2F90_3E4D_4FB8_5AB6, 0xC94D_52AD_1165_E0BE], // 61
        [0x2166_4C60_C551_CC87, 0x7744_BC38_3EEE_82E7, 0x4EAF_3466_5E1E_91EC, 0xD672_519C_F9BF_9852], // 62
        [0xA9EF_BE0F_A0F3_F4E7, 0xEE2A_9691_4ED9_DAF1, 0x4423_A852_9D5A_02FC, 0xA4ED_CDBC_5181_4A6C], // 63
        [0xEF55_8F8A_CAC2_B5CD, 0x4CA2_E1E5_978B_5ABA, 0xDD51_67B3_4569_A794, 0x5D88_1EE9_FB5D_5B93], // 64
        [0xDE0F_20DC_2631_AF7A, 0x2A54_3349_2151_1DC3, 0x8251_042F_F43D_A8A8, 0x4385_626E_49D8_7DEE], // 65
        [0xCF1E_52ED_E1C5_05C4, 0x4B65_8776_7028_764C, 0x4BDC_E6CB_A5E7_CE19, 0x956D_9FD7_DA02_4EE4], // 66
        [0x0965_DF72_19D2_E741, 0x325B_2D02_13FC_E655, 0xEB25_7F2F_3603_A7BA, 0x2437_EA57_151F_3398], // 67
        [0x1B83_78E9_23B2_47A7, 0xFA28_17F0_BD85_AB13, 0xA87E_D0A3_5567_31E9, 0xE89B_B234_1738_FBDB], // 68
        [0x43F2_B606_AD9B_A362, 0x82CB_AFFC_E809_F060, 0x5468_EC0D_07CF_2001, 0x30A8_6D9B_0ACC_F7DB], // 69
        [0xD9C3_4132_22F1_DEA4, 0xF4AD_C150_CEF2_5D1D, 0x1839_AFDC_9057_9684, 0x38F9_EEF6_0180_F73A], // 70
        [0x3D6E_CAB2_BCFB_E3FF, 0x9EFA_152E_F5A7_5CD3, 0x7DD6_5A76_93AF_1475, 0xA273_4155_D7AC_9E0F], // 71
        [0xEA85_73B6_0D5A_8800, 0xA841_14DC_34AA_9F28, 0xA9C0_E3BA_5D92_C348, 0x189A_827F_4BF7_655C], // 72
        [0xE372_599E_31F8_CFFD, 0xC3F2_20DD_D8B8_2638, 0xB4D7_A4F5_0EA5_0F26, 0x0594_172E_37E6_D40C], // 73
        [0x02B8_6794_A00B_DBE8, 0xE351_A82F_BAD7_6B2C, 0x4AD0_2A82_5FD4_E897, 0xBD11_8ABD_2D56_10AA], // 74
        [0xCA1A_21FB_2497_9810, 0x3F0A_9C42_E93B_9D77, 0x0419_4811_5714_69C5, 0x8ABE_C8D6_DA1A_F571], // 75
        [0x60DF_F17B_EC07_766D, 0xE535_33FF_F007_4EDA, 0x9921_0BC2_6459_B90C, 0x40D6_5624_C2B6_2874], // 76
        [0xF4AF_7482_8DE6_862D, 0x6421_5325_7FDE_4CDA, 0x4397_5C10_BDD9_DD55, 0x8A7A_C139_221E_E0FF], // 77
        [0x828E_4C8C_5257_CC15, 0xF8C1_E989_2D4A_9C54, 0x07D1_E7BC_4293_7E29, 0x4087_D27E_468B_184B], // 78
        [0x91F7_F9D6_0082_99A0, 0xF0C5_5CC4_0D88_E8B2, 0x08CB_2E36_DD11_F1B0, 0x9C08_B71B_B86C_D907], // 79
        [0x99BD_5D25_EB21_1099, 0xD7A2_F929_4A1F_26EA, 0x8BCF_62B8_E8BE_65D5, 0xD68A_ACD5_4ECE_CA59], // 80
        [0xCB32_9A8F_102F_05BC, 0xEAAE_DEC5_6183_F45D, 0xC430_2613_7FFA_C66B, 0xF590_61A1_DF29_DC39], // 81
        [0x993A_0B6E_6D5D_F0CB, 0x7884_18EA_81AF_C0BB, 0x59CF_8BF5_48E0_18DD, 0xBCDF_A01C_AAE6_2B27], // 82
        [0x818F_4CEC_407D_8CF9, 0xD068_A8BB_CB6A_A4AE, 0x7AC5_CCBE_F3F7_95DA, 0xB7A3_5AE5_D4D2_069A], // 83
        [0xAF57_F8CD_340F_B1B6, 0x0F11_4411_FFE1_E4DF, 0x4EC7_796A_3D4F_CA91, 0x3A29_141B_37CE_679F], // 84
        [0x5078_9F0F_60EB_AD3A, 0x9378_6F7C_96F8_0FA8, 0x08DC_1752_B7AA_FE42, 0xEF6B_D67F_42BA_F4EC], // 85
        [0xF321_4750_C1A6_455E, 0x16F4_68E1_60D0_E9D1, 0x4288_DF11_5349_BD72, 0xFDDC_EDFF_11E3_E9B3], // 86
        [0x8DBE_8913_BE34_6D20, 0xF9D2_53F9_7E27_2B56, 0x9AE4_17F6_6EAD_575A, 0xF0E0_7626_2E5C_7C43], // 87
        [0xA130_814F_81F8_7E43, 0x397D_2E38_E449_7EED, 0x2780_B218_4E57_906D, 0x8EF7_2723_6172_39B6], // 88
        [0x0023_276D_258E_DA58, 0xFDD3_2FF2_1174_A0CF, 0x673D_8E62_06CE_8DCC, 0x99ED_B29F_220A_3B6A], // 89
        [0x0A05_71F7_69B4_0C93, 0x8C36_9355_AD73_EABB, 0x64C2_33A7_CB8D_4F4A, 0x6C30_6F13_5710_A5BE], // 90
        [0x14A2_F505_1E88_93C8, 0x3C02_FF0C_297A_2DB0, 0xE042_0AAE_577E_CF54, 0x4CDC_420B_32B9_2C30], // 91
        [0x4FBB_E74E_0623_67B0, 0xE3E6_426D_F1A0_09A8, 0xE228_EA59_F20E_2FC1, 0x6F05_A9FB_1384_7FD5], // 92
        [0x4B37_5CEF_5035_86FC, 0x46B9_156E_68F8_14F5, 0x6607_BBEC_EC86_1679, 0xDBD9_FA48_694A_CA17], // 93
        [0x6AD4_B96E_4286_EB70, 0x4B58_6293_36FA_B7A5, 0xDBE3_E948_E8DE_A96A, 0x8F24_7D38_84B2_63B5], // 94
        [0xFF9F_46BD_CC64_4624, 0xB031_7034_CFF5_2D1E, 0x2829_F23C_7755_A5FD, 0x0FD8_24FD_E0F3_0EA1], // 95
        [0x1050_64E7_43ED_D1D9, 0x30EA_2B26_DA8B_07B1, 0x3C1D_B782_4D66_3C48, 0x4E88_4DFB_7F80_BE0E], // 96
        [0x097B_16E4_E9B0_A2E3, 0xF31C_BC68_B0A2_BAE8, 0x0007_B152_7FB7_173E, 0x609A_FFF9_625D_4F47], // 97
        [0x0666_2519_E839_3FF2, 0x319D_DB3B_75C6_A2C1, 0xE6D9_378E_F6BE_1F88, 0x5BE8_96CF_3035_0B5A], // 98
        [0xB51C_B5BA_2957_AB0B, 0x7258_C2B8_7D92_0B92, 0xAC67_36E3_16E7_1910, 0xDA31_74E9_65A6_C2F7], // 99
        [0x4BFE_019C_D91D_9EA4, 0x6A91_8EB4_9763_0F39, 0xFBD6_C604_0FCA_7DCC, 0x0A76_589A_FE2A_4AD9], // 100
        [0xA51E_A210_1B2D_114C, 0xA372_4EC1_B033_52ED, 0x3EDD_A55E_C23A_3BC9, 0x9D3F_2016_7954_B5A1], // 101
        [0x2625_9500_AD47_5ACF, 0xE157_D2A8_B94D_9A27, 0x53A5_C098_E007_C343, 0x06E0_A828_9A42_D507], // 102
        [0x74A9_7F54_7BB8_C845, 0x4F47_CA83_D24B_D8E8, 0x4DEE_D947_F6D0_B03C, 0x0B9F_F1A5_2081_E9C7], // 103
        [0x23ED_252F_132D_FA0F, 0x2633_C26B_46B3_DA43, 0x78B7_BE01_B012_51F4, 0x3B85_F4DD_AECF_0C4A], // 104
        [0x1E03_CFB5_2F4E_43F2, 0x5F97_F4DD_6983_D42F, 0x22B0_658D_22BB_72E9, 0x4089_5985_6C44_A3E5], // 105
        [0xB3C7_FBBF_1E2E_9E9F, 0x4E54_18D5_8563_998B, 0x38AA_033E_EBCD_7CFF, 0x256A_77F1_2D8D_6971], // 106
        [0xC85A_F484_4757_3841, 0xC009_0116_9B50_2467, 0x8C11_7582_28FB_FBE6, 0x21D1_D86E_F272_3034], // 107
        [0x3950_22FA_4E7C_679D, 0x6B82_2625_8BB5_FF40, 0x3153_EB1C_6186_457C, 0xBB26_64AD_5A22_C8CE], // 108
        [0x931C_E3A9_6224_8CB1, 0xF787_9C2F_6D04_A4D8, 0x731D_2F47_9BA8_598E, 0xE5F4_E99C_3FBA_C63E], // 109
        [0x46A7_A712_6CE7_D4E6, 0x5868_71F7_2706_C68D, 0xC4BD_F6D6_5EC7_435E, 0x89C1_5ED2_4DCC_5181], // 110
        [0xB7AF_FC46_1C9E_CE18, 0xFD3B_7929_4A8A_93D9, 0xDBAC_66E7_2949_4C96, 0x8ADF_1B79_9AFB_6181], // 111
        [0xE575_2EA6_E2B3_4417, 0x12E6_E1F6_B18A_CF3A, 0x8725_CE1C_E832_F9BE, 0x4FAE_0FBB_C450_0002], // 112
        [0xFDA1_7FD8_4B50_AD25, 0xC0A6_17E4_7A37_C6DB, 0x8323_1480_76E3_0F6B, 0x284C_A215_0368_6465], // 113
        [0x8751_DAC0_3143_81B6, 0x5456_FD1D_9712_29CD, 0x4500_D06D_980A_B4F7, 0xBEF5_0B50_71BD_7FEF], // 114
        [0xF7CB_3E3D_FCE9_3157, 0x645F_F40D_FCDD_FFBE, 0xF2E2_7662_A834_089A, 0x595E_9209_8E30_E07B], // 115
        [0x0A29_AECE_A5D8_240F, 0xB1D9_425C_EF43_CBA9, 0x8734_F70B_7E15_FF6A, 0x0E6F_9E1C_29E0_4025], // 116
        [0x6731_FD15_B033_D86F, 0xB61B_006F_51E0_0239, 0x9E49_5E43_EB6A_00AB, 0x86E4_AB16_3043_9B6E], // 117
        [0x00BB_7740_D233_CCA9, 0x589C_5FA9_2006_757E, 0x03A0_C5E3_5612_F9F3, 0xE8E4_4365_72E3_9071], // 118
        [0xC957_7B1D_5E99_EADD, 0x4DD7_2213_5776_BD19, 0x6BE6_3AE6_B4E7_F1B2, 0x7C5D_F238_0D5E_7657], // 119
        [0x35E5_1D65_9366_02F0, 0x3D95_20E4_BF18_C401, 0xAF7B_EE8C_9438_93D5, 0x11CB_D19C_6F5D_5334], // 120
        [0xD117_61D6_683C_7490, 0x89CA_CFB8_C480_348B, 0xD177_243A_4C54_F4CB, 0x36F5_D08F_4D9C_77BE], // 121
        [0xEC6A_FA46_8D61_B584, 0xE62A_4344_F3AC_5BC7, 0x7F4A_ED52_CC43_157C, 0x0B82_9131_4FCC_4500], // 122
        [0x147F_4024_6777_27EC, 0x5784_F935_6779_A416, 0x7490_2F73_A7BC_81A2, 0x5751_235D_9466_05FC], // 123
        [0x47FD_33A9_1C14_F035, 0xF0AA_9EDB_CED8_CD9F, 0x11CC_B98F_D725_9525, 0x4213_F2C6_A81A_DDBC], // 124
        [0x7B38_5D8B_837F_AA3E, 0x125F_B837_3AEE_21AE, 0xB30A_7A03_0511_D5DB, 0xB518_297E_B97E_29B7], // 125
        [0xA0E5_EC3E_D606_8228, 0x8B7C_3BD0_A9C7_ABD7, 0xB0F0_EA39_9697_AF8C, 0x4D86_8B87_ECA1_6811], // 126
        [0x3C7A_2111_9AA6_62B0, 0x0981_8062_4628_5D14, 0xF441_2167_0D33_68EB, 0x13B3_913B_B0D1_F2C7], // 127
        [0x90CA_0214_57D9_6DC5, 0x8ED5_434D_86CA_2D83, 0x53AB_85A3_06BC_FDFB, 0x2D24_C757_B1BF_5530], // 128
        [0x41C2_8013_2D69_7ABA, 0x0F71_0743_0502_CE5A, 0x4DD7_0FDA_CCAE_BBD5, 0xD0EE_9279_3D51_2211], // 129
        [0xFCEB_1990_4FA5_852E, 0x1836_36E9_87BC_637E, 0xF3B3_608E_00C9_7F71, 0x69D7_078C_A039_E6A3], // 130
        [0xA855_AB9C_545E_3AA0, 0x0FE8_C6F4_0795_606A, 0x952D_375B_60E4_2F01, 0xA481_D0D8_529B_B584], // 131
        [0x92FF_5FE9_DF03_B47E, 0xD4E9_2B60_F2B6_90C4, 0x871E_9767_C06D_A5D7, 0xFA84_806E_E31B_4F94], // 132
        [0xBEBA_BB79_841E_EFE3, 0x17BF_E801_DBE0_7BC8, 0xE724_818E_9CD4_88C0, 0x6F2C_412E_4270_6C09], // 133
        [0x8348_4B92_6165_1B21, 0x5AE9_0414_EA63_B5D0, 0xE590_9B36_E5D1_A2AC, 0xBB20_C819_EF42_5B63], // 134
        [0x1FA1_3853_B960_95B6, 0x12CF_D098_6E3A_5AAD, 0xADCD_F4A2_46DA_D862, 0x4FB8_DC36_967E_EF54], // 135
        [0xDED1_5F1F_9BA9_2FEF, 0xD10C_FBBE_C6AD_535C, 0xB8D0_F114_C819_34BB, 0xFC22_140E_E771_3C6F], // 136
        [0x413F_6647_912D_D2EA, 0xAE65_90B3_5F27_148F, 0x8E88_D017_259D_1DC5, 0xEB31_654C_1D20_5AF5], // 137
        [0xBF93_F159_16B8_9700, 0xBF53_6DC5_92DE_1ECF, 0xD5E1_6B31_1B3E_A9AF, 0x1734_3CD0_B4E8_6F79], // 138
        [0x9B4F_D36F_0552_1437, 0x1E84_7BAB_91EE_01B3, 0x704D_D346_5FC7_E63F, 0x8CA2_0647_2DB9_4CE5], // 139
        [0xAF1F_8B36_AFE6_DF4F, 0x7B4B_9D99_9BA3_76D8, 0x61B8_3F5D_83B2_4A9B, 0x8BCE_9849_B024_AE52], // 140
        [0x6948_F778_B107_26CB, 0x8242_1338_7096_7B88, 0xC6C1_79D2_462C_ABAA, 0x2AB0_B121_A66D_A697], // 141
        [0x57DE_58AC_79A0_84E8, 0xF09A_8AE3_2E9B_2E8B, 0xC803_9225_8539_CD62, 0x7A1A_2F9C_07C0_D7D1], // 142
        [0xD2CA_0115_A4F2_72AA, 0xADDF_B4D5_D022_B5A4, 0x7B32_B223_B8DD_1815, 0xE3B5_5659_C1D8_C1EC], // 143
        [0x4799_9F1F_28EC_88A6, 0x4BF8_A0E3_88F8_4DE9, 0x344B_DD05_9B8E_FF13, 0xF7CF_495E_C5E7_A3E5], // 144
        [0x18F5_1980_1BB6_2003, 0xCE53_783E_E1E0_9B9D, 0xD9C0_28A6_A1AC_23B2, 0x9270_B87B_2CA1_391E], // 145
        [0x15C7_61F4_15F9_7578, 0xB08F_B604_F527_73C0, 0x77AC_4378_22EF_4735, 0x1B23_0D42_A33A_65A6], // 146
        [0x4BA2_A072_E1FF_588F, 0xBE55_6469_2373_A77D, 0x0A5C_A8FC_64B3_4DAA, 0xF018_6982_D56F_8439], // 147
        [0xF2E4_CB94_F60F_07D2, 0x338C_5CBE_EFA8_DF6C, 0xD47D_C686_8EF8_0A6F, 0x0733_938B_5A96_00EC], // 148
        [0xAFBD_86DE_E66E_FE56, 0x6A78_06E4_688A_0898, 0xC87D_B01C_3B16_F9EF, 0x7A20_B74E_A121_F58A], // 149
        [0xC1B4_E21F_DB2F_ECE6, 0xB355_13B2_B79F_507B, 0x1124_ED59_6832_8CA3, 0x4374_B239_141F_DAF9], // 150
        [0x4DDE_A4A0_F2AA_9CF8, 0x852A_52C2_1ADD_4193, 0x1269_E33C_0C81_4A96, 0x21C6_ED2B_8ABB_C446], // 151
        [0x62A1_5F4D_530A_7EA5, 0x456D_4904_E06D_AC58, 0x4A21_7D9C_BA59_2706, 0x7F96_4670_D58C_6864], // 152
        [0x9B84_F225_2527_5E62, 0x3402_A815_EF78_124A, 0x71B0_1DD4_3E9B_D07B, 0x4499_B846_EBA1_A6F5], // 153
        [0x2EEB_0441_C945_1B1F, 0xADBF_E259_1B2F_7612, 0x0FCF_E909_D233_98C5, 0x700C_57D8_61F7_D267], // 154
        [0x1DE7_F807_36C5_FD1F, 0x2DB3_23AD_05E9_B83A, 0x43F8_D363_B85F_45DA, 0x1FC4_AFCD_4F85_E296], // 155
        [0x7374_5EC8_FDAB_F21C, 0x722E_AB13_C101_6EF6, 0xBADA_2413_C221_F162, 0x8CB7_1869_A275_43E5], // 156
        [0x8A28_90AE_42E4_DBC0, 0x080D_5A5C_DD3A_5332, 0x70A3_5CC0_5FAC_4BD9, 0xC6CB_F850_FE6C_E739], // 157
        [0x6B4F_62CF_F8A4_3B80, 0x2FFF_2138_948C_2A14, 0x11DF_33C7_9A4F_8CC8, 0x3508_40F9_7195_4CB2], // 158
        [0x5E02_EFAF_7ACD_F19C, 0x8569_FD31_6586_A431, 0x7B45_E376_0F84_977C, 0x6940_7B14_B8F7_7AE6], // 159
        [0x5B11_EFE8_DE35_0A3B, 0x435B_78F8_2FDB_E1D8, 0xD8C7_F7D1_EBF7_127B, 0xBA20_8A27_26A5_5A60], // 160
        [0xAD81_3CA7_B0D5_4781, 0xBAA9_04B5_50FE_D1DD, 0xC164_67ED_9018_FA4D, 0xB40F_4324_DB7A_6DBD], // 161
        [0xD2EC_094A_8B3C_A48F, 0xC924_4414_0EAD_FAF0, 0x061A_BD65_95D9_AB1A, 0x2969_313C_19EE_5871], // 162
        [0x4BF3_D48D_5ACB_F6AC, 0xEA10_E7ED_42FB_B53D, 0xA22A_80E1_B510_E18D, 0x06C2_AFC2_F028_E83F], // 163
        [0xE41F_05B8_76F9_96E6, 0x6E7D_954D_0180_95F7, 0x4D9C_EEC3_972F_AF53, 0xCB59_14AA_041F_7DA9], // 164
        [0x46C8_5006_98B3_3143, 0xF1D4_AAA2_E134_EFB7, 0x9889_D4EC_D59E_662D, 0x5DFD_E814_4E26_6068], // 165
        [0x1FF8_37B8_9DFF_74C6, 0x89F0_77EC_CBF4_9F53, 0x1D6A_502A_C0AA_3D47, 0x5E30_F607_0F93_FB74], // 166
        [0x8A9F_E53D_0989_B9DB, 0x4D02_BADB_D293_AD16, 0xD8DE_26F7_3BAB_DD4D, 0x52BE_2D03_939D_0002], // 167
        [0x1AED_20C4_77BA_590A, 0xACDB_0018_579A_CC4E, 0xDFA9_C7D0_0AE0_7C2E, 0x4C92_BB54_B9F8_63F1], // 168
        [0x5678_A561_F39E_4B7F, 0x2F66_3392_B5C2_9EAE, 0x9947_FD0F_6349_68AD, 0x6442_C0E5_30BA_166C], // 169
        [0x07BC_9808_A99E_91CC, 0x0542_0DAE_43D5_82E0, 0x1948_BABD_33B8_31CF, 0x3120_923E_9831_6870], // 170
        [0x1A1E_7EFC_FC5A_377F, 0xCEB6_DA3E_8A68_07E4, 0x19AF_2B2E_FF46_0FA1, 0x2E53_5FAC_F77D_ACC5], // 171
        [0x6254_49E5_05BB_4352, 0xDCF1_02E0_2990_7C0C, 0x65C2_838F_11F9_11C5, 0xB4B1_AC05_C46A_59AF], // 172
        [0x16D6_028D_F84F_9C3F, 0x1C1B_FAB2_5A1A_ED91, 0x6606_D39F_C8D6_047A, 0xAE03_0BB1_B17C_6DA9], // 173
        [0x1090_2519_949F_206E, 0x1689_DE75_B524_17B6, 0x3808_706D_1390_2DC4, 0xDFE1_5D14_F22E_5222], // 174
        [0x8B8F_04B2_0E8F_9322, 0xD533_B58E_3A0F_09EC, 0x056D_2CC8_E0C5_03AA, 0x3D9E_041C_1EA0_CD26], // 175
        [0xDF9F_79B4_EA27_2F55, 0xDD26_DDCF_3156_A9E3, 0x6415_2358_928F_6569, 0xA4B6_FA90_ACC9_7B2B], // 176
        [0x05E8_B1B6_1DBD_0B71, 0x0B4E_467F_2684_9427, 0x7831_C1E0_281F_8306, 0xE84C_4310_52AF_18A5], // 177
        [0xA046_B4F8_050F_F19F, 0x5260_F0E0_F44A_A178, 0x10B3_0706_FE22_7DB4, 0xCC99_4C02_DB78_AD4E], // 178
        [0xC3C9_B996_7B78_F65E, 0xEDE4_02A0_7ECA_2933, 0x1E26_F3F3_0133_52CB, 0x2AD4_C76E_21B8_1006], // 179
        [0x0755_7C6E_0143_8898, 0xC6B9_F6D2_4E5B_6FEC, 0x8569_1B87_4B49_B6E6, 0xC8DB_3F64_98AF_8717], // 180
        [0x8EB6_B9EA_8895_92E4, 0xD9F5_9540_B47F_F8F3, 0x66EF_BAF3_0C28_4690, 0xDFD8_30A1_042A_F267], // 181
        [0xB563_B680_EAF9_3D1C, 0x2C9E_4A13_DF03_53B9, 0x65D3_56C0_BCBD_E1AC, 0xD3F1_55FC_8C84_ADCE], // 182
        [0xDC0A_5985_46EE_F8D5, 0xDFF4_521C_1F76_A885, 0x6ECD_006C_C632_8E83, 0xFD71_F3C8_E971_7035], // 183
        [0x3DEA_E67F_2AF3_D038, 0xFE69_6D21_BE39_E4C4, 0x50CA_6B6C_3D1B_924A, 0x7C6B_3523_4F6A_ADC4], // 184
        [0xDFF0_E838_8895_A23E, 0x2467_CB20_35BC_D03E, 0x616D_17ED_F2BF_29AF, 0xD73F_8825_770F_327C], // 185
        [0x984E_F85C_5A53_28F2, 0x96EA_7D22_BE03_8252, 0xA151_A4EB_19B6_1F34, 0x7D34_2409_6FCD_B3F4], // 186
        [0xFDAD_135D_4A19_0C32, 0x179C_FC3F_908A_87D8, 0x7561_1073_425E_7914, 0xE1FF_6C9B_57D3_5CCF], // 187
        [0x8914_47AC_CCE5_9C8A, 0x7C12_0CEE_E9B1_60C8, 0x46C1_83EB_5974_5BF5, 0xA0B5_E683_A4DC_3753], // 188
        [0x3C44_BB5E_8E0B_8C55, 0xE615_789F_D698_66A9, 0x0040_0BF2_9619_006A, 0x7E63_5BB5_BBE7_51D0], // 189
        [0xA647_9C9D_DC0E_5E00, 0x2964_8B93_3AE2_83AA, 0x01DB_2E6B_68EE_E727, 0x7542_6078_E7CC_86FC], // 190
        [0xF832_9DA3_AD42_49E8, 0xF61D_F8CE_C06A_DECE, 0x9882_6B3A_49ED_9D8E, 0x79A0_61CD_AB11_0607], // 191
        [0xB76A_434A_C1E4_768B, 0xC286_8E83_FC5F_8892, 0xE9E6_7332_AE83_17DC, 0x0E9D_07D4_23DF_2113], // 192
        [0x2CDF_5248_8C4A_2F81, 0xB803_F31B_E4E7_FA07, 0x32BD_1F54_9020_1D12, 0x7072_1104_0BB9_CD5F], // 193
        [0x46A3_82CC_7B5D_8281, 0x0963_1214_9532_E3B0, 0x3601_48DD_FFF8_BA73, 0x23D0_FAE5_C79D_AA15], // 194
        [0x52B7_3ECD_B3EF_30E4, 0xDF33_431F_26CB_7979, 0x3EFD_A1A2_2D21_5CA8, 0x8E70_A7B9_365F_426C], // 195
        [0xEB9A_CBEF_C925_8D5C, 0x2D5C_4081_72D0_3744, 0x4C7C_7ADD_3DF6_FC06, 0x89A5_E33B_D8B9_B605], // 196
        [0x3356_A9D8_1CFB_5929, 0x8B11_FF80_3E50_436E, 0x2300_3FC1_4A43_9CE2, 0x71F0_F830_AF4B_39AC], // 197
        [0xE781_C3C6_402B_EA01, 0xFC4A_C46A_7904_24D2, 0xC567_B178_E1F6_421A, 0x9E27_8E16_A525_1892], // 198
        [0xE37F_EF44_9650_9A87, 0x1064_02A6_050B_713B, 0x5505_B74C_C308_D4FB, 0x5BA5_8527_2363_D0C2], // 199
        [0x4D86_3378_A205_2D65, 0x21F5_6593_58B9_8CE1, 0x5798_3E6F_42C2_726D, 0x2DF9_6628_779C_8722], // 200
        [0xF24E_8A1E_FDFF_290C, 0xF01B_E17B_5891_AC25, 0xB72E_F734_DC84_EEE5, 0xA719_9891_06C6_69AC], // 201
        [0x5C98_EE8D_FA4E_FCBF, 0xB4B6_377B_1D16_F306, 0x6930_85A6_5836_03D0, 0x3089_54D9_4BD7_7AD0], // 202
        [0x8360_6337_7C48_824A, 0x2C5F_34BC_7FDC_7CF2, 0x20EE_3F75_1774_2A2B, 0xF011_B3D9_BBA8_49F4], // 203
        [0xAAB4_0723_0B8C_829C, 0xBE67_E48A_DFE3_CB0D, 0x478C_10AB_525F_1CA2, 0xC7DC_DBD6_92A5_71CC], // 204
        [0xAFA6_E4AA_2AE0_3314, 0x81FD_0FDB_253D_BC85, 0xD450_FA0D_E741_B4CD, 0x0D86_069C_2999_F6F8], // 205
        [0xB258_FED1_14D4_FBE6, 0xF5BF_1CBA_FF2C_F042, 0x3E10_2743_2D66_C9BC, 0x3934_44D6_88C8_ED0E], // 206
        [0xC705_4B98_2C73_EB13, 0x5CCD_EAAD_E4BC_642D, 0x37D8_733B_C1DE_C0E0, 0x3C60_DCCE_C269_9675], // 207
        [0x9EB4_B106_7F96_C311, 0xF06F_65A5_D4C7_0F05, 0x0393_B97B_E051_FB88, 0xE93D_103C_FE2A_733A], // 208
        [0x7344_195C_5427_9951, 0xC8DB_8316_01ED_E4B2, 0xA51F_E04C_0273_91DA, 0x0BCD_27B8_CF6E_D4C6], // 209
        [0x8481_2698_B4B8_BF44, 0x1161_B12A_C333_7310, 0xC284_F0DF_03C3_9A67, 0x9100_B4ED_A8A7_5529], // 210
        [0x821B_3108_CD99_A5A8, 0x92F8_F278_C336_0D41, 0x9E04_DB99_6B04_95BD, 0x704E_72F9_1308_934F], // 211
        [0x8E8E_6815_1D5E_419D, 0x785F_5BA1_E58D_05DD, 0xC2C2_9D36_70A1_D176, 0x843B_0D1D_08F9_1C7B], // 212
        [0xFD0B_5E2A_5921_C14A, 0x3D00_21A7_9151_3773, 0x2968_4E3E_3E0E_B42E, 0xE549_2209_E07E_3891], // 213
        [0x87FA_20EB_6126_D0CC, 0xA786_033C_7899_F830, 0x1722_C125_5263_A3A1, 0xB77E_497C_1F63_000D], // 214
        [0xA38B_DC1F_221B_4DDC, 0x8BF3_F7DD_78F9_60F7, 0x6AFD_5567_F500_E20A, 0x6D01_B1A1_A524_283A], // 215
        [0xD997_AAAE_88AE_9C5F, 0x8E76_1F3C_52E9_29FC, 0x764E_620E_8180_BE8F, 0xAB2A_38C3_E6DB_7892], // 216
        [0x8E03_1800_E9DE_75D5, 0x1A54_ED24_8D28_90CF, 0x2986_288E_BD02_3166, 0xC96B_569C_59A1_D5A8], // 217
        [0x2EE2_208A_E332_E79D, 0x65A4_9DA9_FFE4_E7D2, 0x643A_360E_EA2A_2A68, 0x6DFD_9DF5_368B_84CE], // 218
        [0x21B8_7460_1A4D_AC92, 0xBCD4_7673_6927_713B, 0x3CA9_FF8A_E7D0_8AFB, 0x4037_7593_97B0_7AF7], // 219
        [0xB44A_85B4_23E0_4207, 0xFB83_15E3_4C68_634A, 0x4BA2_815E_AFFA_3B75, 0x2998_D4AB_4FA5_A77F], // 220
        [0x7C8C_B92E_CFE9_82DC, 0x6E16_9FC4_C51D_E07E, 0xEBB9_CB8E_968D_807A, 0x58B5_91D6_B4EF_02CD], // 221
        [0xB641_AE8C_B691_C174, 0x94F7_9B86_315C_4705, 0xC156_320C_B462_EA96, 0x7129_EB92_9863_8F65], // 222
        [0x3204_FA23_8CB7_A119, 0xA4F1_FEA8_976F_416B, 0xB2BE_6F50_475E_ACC1, 0xE197_EDD6_27C9_34AE], // 223
        [0x560A_22F6_8D44_BAA4, 0xE3A2_4AB1_BEE3_C8FF, 0x58B8_A451_4766_0A24, 0x4FEF_D4AC_AED7_01E9], // 224
        [0x2603_4ED8_D4CA_E4DC, 0xE80A_96FF_B929_A639, 0x0460_1D3A_19CF_E358, 0x8951_BBAB_8725_E7D2], // 225
        [0x6C44_B98E_3A56_766F, 0x0BA9_EA1F_6D56_52E6, 0x398D_82D7_4E9F_4422, 0xFBFD_B8E0_B881_4C4C], // 226
        [0x84D9_943F_84B3_F48C, 0xC2C2_9B06_CE00_360B, 0xEBBE_4068_9C0B_3E27, 0x0814_7BC4_DF61_DD97], // 227
        [0x07FB_F61C_952C_73A0, 0x2663_3EF0_9C08_1AD4, 0x26B8_5EA6_DDD9_E6C4, 0x5A90_371F_F888_F355], // 228
        [0x7650_089C_595D_1589, 0xD7F8_6ACB_A582_01AE, 0xF2D5_3098_C6E8_FF0B, 0x2A50_5957_1F46_4CD4], // 229
        [0xA6E2_C789_AE52_3788, 0xF3C7_C4A4_E8C7_0AF6, 0xC615_7C14_B2B0_D725, 0x3D26_A6D7_5B14_4DC2], // 230
        [0x1678_FC0F_21A2_D189, 0x42A0_5215_AC7D_A0F7, 0xE4DE_653D_4F93_5868, 0x4476_3CF2_327F_B9C3], // 231
        [0x0883_7FC1_8CF9_DC21, 0x16D6_ACE7_D755_5F80, 0xA49A_4BC7_2681_089D, 0x9D2F_C8EA_ACB9_ECD1], // 232
        [0x2B8C_BB3C_1BE4_B134, 0x92AA_E9EF_47A2_FF19, 0x2A9F_4DA3_38C6_82D5, 0x1F7E_B8AB_746F_0F5C], // 233
        [0x4850_2CC3_75AE_DE41, 0x70A1_DB08_8C2D_E44B, 0xDA14_AFC7_6E30_9321, 0x4C2F_46E3_517C_0FC9], // 234
        [0xC913_6721_158E_47F6, 0x49E1_96E2_7EAE_CC4C, 0xC5D4_52E1_A696_99BE, 0x1B6C_3BB8_FD3D_CAE8], // 235
        [0x6252_A731_7AF9_5FD4, 0xD363_D80B_7DEA_09C9, 0x1D56_82F7_0791_A4F3, 0x1ACB_C349_BAC6_C907], // 236
        [0x6FD0_AAE0_666C_5A27, 0x0D39_7C62_EF9C_B859, 0x4971_888C_2AAC_2B83, 0x3345_DAF4_7392_4B9C], // 237
        [0x0C2C_E719_985C_D28C, 0xFD72_47D5_270E_73CC, 0x85EA_4BAD_11C3_843A, 0x6E86_BD02_31AD_F03A], // 238
        [0xB937_1346_4372_DA03, 0x7B57_28A4_C382_95F1, 0xC287_83EF_6AF1_A735, 0xEFC4_5224_83A8_A078], // 239
        [0xB818_38D4_83BA_EE53, 0x41D0_EA66_B386_C5AE, 0x8BF4_C385_8735_4A51, 0x4BD5_B477_4C46_2A49], // 240
    ];

    /// 1024 bytes of [`xsum_buffer`] (32 stripes), from the same `gen.c`.
    #[rustfmt::skip]
    const XSUM_1024: [u64; 4] = [0x4775_BF7C_ACE4_D177, 0x7B80_0A0B_F417_DB61, 0xD20E_7D18_6820_3055, 0x6E14_119A_F5AF_0922];

    /// `[0, ROUTING_SEEDS[0], [1], [2]]`: the seeds of each [`XSUM`] row.
    fn xsum_seeds() -> [u64; 4] {
        [0, ROUTING_SEEDS[0], ROUTING_SEEDS[1], ROUTING_SEEDS[2]]
    }

    #[test]
    fn xxhsums_own_table_holds_on_its_buffer() {
        let buf = xsum_buffer(222);
        for &(n, seed, want) in XSUM_PUBLISHED {
            assert_eq!(xxh64(&buf[..n], seed), want, "len {n} seed {seed:#x}");
        }
    }

    #[test]
    fn matches_the_reference_on_xxhsums_buffer() {
        let buf = xsum_buffer(1024);
        let row = |n: usize| xsum_seeds().map(|seed| xxh64(&buf[..n], seed));
        for (n, want) in XSUM.iter().enumerate() {
            assert_eq!(row(n), *want, "len {n}");
        }
        assert_eq!(row(1024), XSUM_1024, "len 1024");
    }

    /// The streaming form against [`XSUM`], in pieces either side of a
    /// stripe.
    #[test]
    fn fed_in_pieces_it_matches_xxhsums_buffer() {
        let buf = xsum_buffer(1024);
        let rows = XSUM.iter().enumerate().chain([(1024, &XSUM_1024)]);
        for (n, want) in rows {
            for (seed, &want) in xsum_seeds().into_iter().zip(want) {
                for piece in 1..=33 {
                    let mut h = Xxh64::new(seed);
                    for chunk in buf[..n].chunks(piece) {
                        h.update(chunk);
                    }
                    assert_eq!(h.digest(), want, "len {n} seed {seed:#x} in pieces of {piece}");
                }
            }
        }
    }

    /// The streaming form against the same reference rows, fed in pieces of
    /// every size from one byte to past a stripe, so each buffering path
    /// (fill, flush, whole stripes, leftover) meets each tail length.
    #[test]
    fn fed_in_pieces_it_matches_the_reference() {
        let buf = sanity_buffer();
        for &(n, seed, want) in SANITY {
            for piece in 1..=40 {
                let mut h = Xxh64::new(seed);
                for chunk in buf[..n].chunks(piece) {
                    h.update(chunk);
                }
                assert_eq!(h.digest(), want, "len {n} seed {seed:#x} in pieces of {piece}");
            }
            // An empty update anywhere changes nothing.
            let mut h = Xxh64::new(seed);
            h.update(&[]);
            h.update(&buf[..n]);
            h.update(&[]);
            assert_eq!(h.digest(), want);
        }
    }

    /// Irregular splits, as a field-by-field feed makes them.
    #[test]
    fn any_split_digests_like_the_whole() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..2_000 {
            let len = (next() % 200) as usize;
            let input: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            let seed = next();
            let mut h = Xxh64::new(seed);
            let mut at = 0;
            while at < len {
                let take = ((next() % 11) as usize).min(len - at);
                h.update(&input[at..at + take]);
                at += take;
            }
            assert_eq!(h.digest(), xxh64(&input, seed), "len {len}");
        }
    }
}
