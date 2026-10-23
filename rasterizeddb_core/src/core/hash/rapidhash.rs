//! rapidhash, in tree, frozen at one upstream version.
//!
//! GIN items hash through here (`ItemFeed::rapid`, called only by
//! `jsonb::item_hasher`). An item is hashed once per document per build and
//! once per query, and is about forty bytes. On that, XXH64's serial chain of
//! multiplies costs 14.2 ns where this costs 8.4 (6.0 and 3.9 when items do
//! not depend on each other), and it made GIN builds 22-25% slower than they
//! were on ahash. A GIN index is a superset index under a recheck, so its
//! hash's quality only moves the false positive rate; the tests here and in
//! `jsonb` pin that a large realistic set of items, and a few adversarial
//! families, never collide. Everything else that is persisted or routed stays
//! on [`super::xxh64`].
//!
//! Frozen like XXH64, and for the same reason: an item a query builds must
//! equal the item a build wrote, in every process and every build. The source
//! is upstream `rapidhash.h` (<https://github.com/Nicoshev/rapidhash>) at
//! release tag `rapidhash_v3`, commit
//! `bc4b4baa48a15ff52ff4725e1ccdcda62815221c` ("rapidhash V3"), and this is
//! its `rapidhash_withSeed`: the full 64-bit function with the default
//! secrets, not `rapidhashMicro` or `rapidhashNano`. That header has two
//! compile-time switches, both at their defaults here:
//!
//! - `RAPIDHASH_FAST`, not `RAPIDHASH_PROTECTED`: the multiply overwrites its
//!   operands with the product's halves rather than xoring it into them.
//!   `PROTECTED` is a different function (other outputs).
//! - `RAPIDHASH_COMPACT`, not `RAPIDHASH_UNROLLED`: the 112-byte block loop
//!   runs one block per iteration. `UNROLLED` takes two per iteration and
//!   gives the same outputs (checked against the C build of both).
//!
//! Reads are little-endian on every target, as upstream's are on a
//! little-endian one, and the 128-bit product is `u128` arithmetic, so there
//! is no target-dependent path. The header's later commits up to `1ae7842`
//! reorganise the loop and give the same outputs on the vectors below; the
//! version is still `rapidhash_v3`, and a different one is a new
//! `GIN_HASHER_ID`.
//!
//! One-shot only: [`rapidhash`] needs the whole input, so `ItemFeed` gathers
//! an item's bytes before calling it.

/// Upstream's `rapid_secret`.
const SECRET: [u64; 8] = [
    0x2d35_8dcc_aa6c_78a5,
    0x8bb8_4b93_962e_acc9,
    0x4b33_a62e_d433_d4a3,
    0x4d5a_2da5_1de1_aa47,
    0xa076_1d64_78bd_642f,
    0xe703_7ed1_a0b4_28db,
    0x90ed_1765_281c_388c,
    0xaaaa_aaaa_aaaa_aaaa,
];

/// The block the main loop consumes: seven 16-byte lanes.
const BLOCK: usize = 112;

/// The secret each 16-byte step of the tail (after the blocks, at most
/// [`BLOCK`] bytes) mixes under, in order: upstream's nested `if (i > 16)`
/// through `if (i > 96)`.
const TAIL_SECRET: [u64; 6] = [SECRET[2], SECRET[2], SECRET[1], SECRET[1], SECRET[2], SECRET[1]];

#[inline(always)]
fn read_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

#[inline(always)]
fn read_u32(b: &[u8], at: usize) -> u64 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as u64
}

/// `rapid_mum` under `RAPIDHASH_FAST`: the 128-bit product, as (low, high).
#[inline(always)]
fn mum(a: u64, b: u64) -> (u64, u64) {
    let r = (a as u128) * (b as u128);
    (r as u64, (r >> 64) as u64)
}

/// `rapid_mix`: the product's halves folded together.
#[inline(always)]
fn mix(a: u64, b: u64) -> u64 {
    let (lo, hi) = mum(a, b);
    lo ^ hi
}

/// rapidhash V3 of `input` under `seed` (upstream's `rapidhash_withSeed`).
pub fn rapidhash(input: &[u8], seed: u64) -> u64 {
    let len = input.len();
    let mut seed = seed ^ mix(seed ^ SECRET[2], SECRET[1]);
    let (mut a, mut b);
    // Upstream's `i`: the bytes not yet consumed by a block, which also
    // enters the last mix. Equal to `len` up to 16 bytes.
    let mut i = len;
    if len <= 16 {
        if len >= 4 {
            seed ^= len as u64;
            if len >= 8 {
                a = read_u64(input, 0);
                b = read_u64(input, len - 8);
            } else {
                a = read_u32(input, 0);
                b = read_u32(input, len - 4);
            }
        } else if len > 0 {
            a = ((input[0] as u64) << 45) | input[len - 1] as u64;
            b = input[len >> 1] as u64;
        } else {
            a = 0;
            b = 0;
        }
    } else {
        // `p`: where the unconsumed bytes start; `p + i` is always `len`.
        let mut p = 0;
        if i > BLOCK {
            // `seed` and upstream's `see1` through `see6`, one per lane.
            let mut lanes = [seed; 7];
            while i > BLOCK {
                for (k, lane) in lanes.iter_mut().enumerate() {
                    *lane = mix(read_u64(input, p + 16 * k) ^ SECRET[k], read_u64(input, p + 16 * k + 8) ^ *lane);
                }
                p += BLOCK;
                i -= BLOCK;
            }
            seed = lanes.iter().fold(0, |acc, lane| acc ^ lane);
        }
        for (k, secret) in TAIL_SECRET.iter().enumerate() {
            if i <= 16 * (k + 1) {
                break;
            }
            seed = mix(read_u64(input, p + 16 * k) ^ secret, read_u64(input, p + 16 * k + 8) ^ seed);
        }
        // The last 16 bytes of the input, which may reach back into bytes a
        // block or a tail step already read.
        a = read_u64(input, len - 16) ^ i as u64;
        b = read_u64(input, len - 8);
    }
    a ^= SECRET[1];
    b ^= seed;
    let (a, b) = mum(a, b);
    mix(a ^ SECRET[7], b ^ SECRET[1] ^ i as u64)
}

#[cfg(test)]
mod tests {
    use super::rapidhash;
    use std::collections::HashSet;

    const PRIME32: u64 = 2_654_435_761;
    /// `jsonb::ITEM_SEED`, the seed every GIN item is hashed under, so the
    /// seed's high bits are exercised too.
    const GIN_SEED: u64 = 0x6a73_6f6e_6769_6e00;

    /// xxh64.rs's sanity buffer (xxhsum's original generator, squaring
    /// PRIME32), to [`SANITY`]'s length. Its bytes are all zero from the 28th
    /// on, since an odd number squared 30 times is 1 mod 2^32, so a block
    /// past there reads only zeros: [`primes_buffer`] covers those.
    fn squares_buffer() -> Vec<u8> {
        let mut byte_gen: u32 = PRIME32 as u32;
        (0..SANITY.len())
            .map(|_| {
                let b = (byte_gen >> 24) as u8;
                byte_gen = byte_gen.wrapping_mul(byte_gen);
                b
            })
            .collect()
    }

    /// xxhsum 0.8's sanity buffer (`XSUM_fillTestBuffer`: multiplying by
    /// PRIME64), whose bytes do not run out.
    fn primes_buffer() -> Vec<u8> {
        let mut byte_gen: u64 = PRIME32;
        (0..SANITY.len())
            .map(|_| {
                let b = (byte_gen >> 56) as u8;
                byte_gen = byte_gen.wrapping_mul(0x9E37_79B1_85EB_CA87);
                b
            })
            .collect()
    }

    /// `(text, under seed 0, under GIN_SEED)`, from the C reference (below).
    const TEXT: &[(&[u8], u64, u64)] = &[
        (b"", 0x0338_DC4B_E2CE_CDAE, 0xD9B4_914D_49C2_325C),
        (b"a", 0x599F_47DF_33A2_E1EB, 0xDAF0_2DD0_0DEF_B140),
        (b"abc", 0xCB47_5BEA_FA9C_0DA2, 0xFEE1_F6DE_9FEE_3086),
        (b"Nobody inspects the spammish repetition", 0x48B8_CE3A_8C6E_F968, 0x4DA0_72AE_4713_1F0F),
        (b"The quick brown fox jumps over the lazy dog", 0x9172_2DC8_D52A_3F7B, 0xFF70_C50B_CA61_D81A),
    ];

    /// Row `n` is the first `n` bytes of `[squares_buffer under 0, under
    /// PRIME32, primes_buffer under 0, under GIN_SEED]`. Every length from 0
    /// to two blocks and a full tail step past them, so each short path (0,
    /// 1-3, 4-7, 8-16), each tail step (17, 33, 49, 65, 81, 97), the first
    /// block (113), a block with each tail length after it, and a second
    /// block (225) are each crossed.
    ///
    /// Generated by the C reference, upstream `rapidhash.h` at tag
    /// `rapidhash_v3` (commit `bc4b4baa48a15ff52ff4725e1ccdcda62815221c`),
    /// nothing from a Rust crate. To regenerate:
    ///
    /// ```text
    /// git clone https://github.com/Nicoshev/rapidhash && git -C rapidhash checkout rapidhash_v3
    /// cc -O2 -std=c99 -I rapidhash -o gen gen.c && ./gen
    /// ```
    ///
    /// with `gen.c` as follows (the first five lines it prints are `TEXT`;
    /// `-DRAPIDHASH_UNROLLED` prints the same, `-DRAPIDHASH_PROTECTED` does
    /// not):
    ///
    /// ```c
    /// // rapidhash V3 reference vectors for rasterizeddb_core/src/core/hash/rapidhash.rs.
    /// // cc -O2 -std=c99 -I <rapidhash at tag rapidhash_v3> -o gen gen.c && ./gen
    /// #include <stdio.h>
    /// #include <stdint.h>
    /// #include "rapidhash.h"
    ///
    /// #define N 241
    /// static void hex(uint64_t v) {
    ///     printf("0x%04X_%04X_%04X_%04X", (unsigned)(v >> 48), (unsigned)(v >> 32) & 0xFFFF,
    ///            (unsigned)(v >> 16) & 0xFFFF, (unsigned)v & 0xFFFF);
    /// }
    /// int main(void) {
    ///     const uint64_t seeds[3] = {0, 2654435761u, 0x6a736f6e67696e00ull};
    ///     uint8_t squares[N], primes[N];
    ///     uint32_t g32 = 2654435761u;           /* xxh64.rs's buffer: xxhsum's old generator */
    ///     uint64_t g64 = 2654435761u;           /* xxhsum 0.8's generator */
    ///     for (int i = 0; i < N; i++) {
    ///         squares[i] = (uint8_t)(g32 >> 24); g32 *= g32;
    ///         primes[i] = (uint8_t)(g64 >> 56);  g64 *= 11400714785074694791ull;
    ///     }
    ///     const char *text[5] = {"", "a", "abc", "Nobody inspects the spammish repetition",
    ///                            "The quick brown fox jumps over the lazy dog"};
    ///     for (int t = 0; t < 5; t++) {
    ///         printf("(b\"%s\", ", text[t]);
    ///         hex(rapidhash_withSeed(text[t], strlen(text[t]), seeds[0])); printf(", ");
    ///         hex(rapidhash_withSeed(text[t], strlen(text[t]), seeds[2])); printf("),\n");
    ///     }
    ///     for (int n = 0; n < N; n++) {
    ///         printf("[");
    ///         hex(rapidhash_withSeed(squares, n, seeds[0])); printf(", ");
    ///         hex(rapidhash_withSeed(squares, n, seeds[1])); printf(", ");
    ///         hex(rapidhash_withSeed(primes, n, seeds[0])); printf(", ");
    ///         hex(rapidhash_withSeed(primes, n, seeds[2])); printf("], // %d\n", n);
    ///     }
    ///     return 0;
    /// }
    /// ```
    #[rustfmt::skip]
    const SANITY: &[[u64; 4]] = &[
        [0x0338_DC4B_E2CE_CDAE, 0x373D_1F7F_A324_9962, 0x0338_DC4B_E2CE_CDAE, 0xD9B4_914D_49C2_325C], // 0
        [0x892B_C885_59AD_B770, 0x70C2_AFAB_E1BC_66D6, 0x4F23_C791_B16E_BA02, 0xCC13_C288_B947_32C5], // 1
        [0xE1D5_019F_A716_5DBA, 0xBD7B_5528_9CBF_07A3, 0xE1C6_9F72_8398_08CA, 0x4582_3747_ED03_FF31], // 2
        [0x572A_A00B_3651_3433, 0x4FCB_791C_215F_E1E6, 0x0CBB_F252_1477_235C, 0x423E_8D68_D257_B62B], // 3
        [0x8323_04F0_1780_D1C3, 0x1480_A4AE_55B9_7FC0, 0x7497_8306_D9C1_494C, 0xDA06_886F_24E6_EFAA], // 4
        [0x51F8_81A4_77B7_CA03, 0xE9F9_D23B_E4A9_E4AE, 0x692E_C5CA_B57A_4A1C, 0xDFCA_0075_08B4_5DD7], // 5
        [0xB32B_326A_1B88_7DA3, 0xAE4E_B947_6392_6247, 0xFAEC_769A_DCB1_FA99, 0x1F35_98B2_6248_C264], // 6
        [0xF90C_139F_DD28_1896, 0x614F_936A_FC33_E2C9, 0x42F0_5ECF_638D_44E1, 0xD377_92C0_7B24_4EEA], // 7
        [0xCA40_5825_EB49_3603, 0x1E2F_CDE2_B03C_DE35, 0x3931_FB68_809D_0ED2, 0x96C9_929D_8367_76AD], // 8
        [0x5D0D_996F_5D20_53C2, 0xD162_5445_9408_9981, 0x6AFC_7B5A_3F41_7321, 0x103D_32A6_CA59_D7DB], // 9
        [0xD96D_C4DF_7301_699C, 0x1339_C769_F841_7CE9, 0x2D10_8DA8_FCE1_C6C7, 0xED79_302F_073C_D127], // 10
        [0xB9A8_C3EF_B9C6_541E, 0xA674_63D7_9874_BA0B, 0x1AEB_24A1_E7B5_4B59, 0xA90D_28E8_EE03_D3A2], // 11
        [0x459F_1D56_1820_4EE0, 0x3259_DC62_39B0_CB71, 0x2EF6_0D7D_7EE4_D9F3, 0x8FA3_6AD0_7A36_89DA], // 12
        [0xFC7A_7B35_0DEF_5482, 0xA592_B342_D580_3732, 0xBAD3_88D4_788C_D61B, 0xF871_E739_F6CD_9EC0], // 13
        [0xE36B_32A5_F0A8_15B0, 0x8AA7_8622_C702_8C92, 0xAB69_8BDF_E1C2_AB93, 0x8133_00A4_DBEF_97A5], // 14
        [0xC778_0037_E8D9_5D19, 0x5759_3963_8DEE_90BD, 0xA93B_151A_913B_29CF, 0x847F_562B_EAB4_74B8], // 15
        [0x5A63_A4DB_0262_6208, 0x2057_AFDF_FC88_CC10, 0xDCDB_4F27_5224_0B9E, 0xD0CB_FCE6_5FAA_4987], // 16
        [0x35EE_D7C4_406A_3961, 0x6502_9431_9B73_53D9, 0x6EEF_9F76_5B9C_4E2C, 0x858E_9E6A_1454_A975], // 17
        [0x70D9_FCD3_256B_8F89, 0xC1CA_93D1_FF65_1286, 0x829E_66B9_D788_FE60, 0x217B_E794_7882_BEB1], // 18
        [0x158C_915B_10CE_1AFC, 0xE3EC_DEE5_62E7_EA45, 0x7950_AEEC_19DB_7EE1, 0x0993_5437_6717_E1C2], // 19
        [0x32C6_BCEE_DCFE_5D0D, 0x3F56_AEF4_F368_4BF1, 0xB219_3B48_ADD6_1580, 0x7CF5_2812_61AA_077F], // 20
        [0x9990_3514_BDAD_6CD4, 0xFB31_5A4E_24CD_04C8, 0xB2FB_62F6_501F_A5DE, 0xCF4B_DDB3_3675_043F], // 21
        [0x9981_F052_3DA0_35EA, 0x45FB_C97B_143A_2998, 0xE886_77E5_D93C_0F03, 0xA733_6B45_315A_FB96], // 22
        [0x7CBF_0FE7_9090_439F, 0xD72A_F87E_0076_A5A5, 0xB517_44A7_0B2A_B96C, 0x8FC1_0BB7_AE02_593A], // 23
        [0x8EB6_B693_6E48_A3CC, 0x8961_9FD4_0B2B_CB1C, 0xFE28_F669_95C3_12E3, 0xCDA5_53BE_16D5_BB0A], // 24
        [0x3260_4396_080E_7A8F, 0xC5A6_DE27_278D_A5A6, 0x2765_B60A_5472_CFD9, 0x25E2_A849_2BC6_0B78], // 25
        [0x8889_8499_FA52_9DCB, 0xA834_8F0A_F69A_2476, 0x784B_FE64_E930_83F0, 0xA137_E830_028F_2F3D], // 26
        [0xF0FA_F307_1571_216B, 0xD301_4D58_4B4A_42AE, 0xA5A1_5ACA_F059_9987, 0xBE95_7A6F_60A6_56C1], // 27
        [0x926D_90A7_991F_B367, 0xC1F9_688D_3E66_9A25, 0xDB5E_AC7F_6B3C_6CAD, 0xBC1F_32F0_EFF0_CCD3], // 28
        [0x3257_ED1D_3329_7B32, 0x643A_3CB8_4A67_58C7, 0x8422_F7D7_C28D_D8B0, 0x4CFB_0E81_EB19_1CCA], // 29
        [0x92F7_F616_EE66_B5E4, 0x0871_913A_7709_618B, 0x683F_5582_8DAA_3E9F, 0x7077_26D5_FDA4_DEC1], // 30
        [0xC5EC_ADE2_3342_17E9, 0xF0E2_6AF9_8BD0_8FC6, 0x5AB4_DCE9_3BC0_A4D0, 0x7871_F252_0565_B5BB], // 31
        [0xEA20_8A51_640D_9A38, 0x96A5_5F5D_24EE_5B0A, 0x6D47_F233_CF72_8DCD, 0xCF55_3BAF_8058_2C03], // 32
        [0xEE01_24A9_45A5_C278, 0x8CE7_0A30_2EB0_1E14, 0xFDCF_A62D_6146_A3EF, 0xD141_D3E1_AC63_BD54], // 33
        [0x10C2_8F19_0EBE_ED99, 0x2F92_3416_F81C_BC7C, 0xCF06_03B3_6232_8B32, 0xB68B_CB98_BCC2_FDEF], // 34
        [0xD79A_12C8_84BF_E90C, 0xAADF_0E26_FEE0_46E3, 0x31B9_1CB7_EE92_1E22, 0xC72F_82EB_3CD6_2E34], // 35
        [0x80CF_5782_45F3_C652, 0xE09F_09A1_ED6B_9C51, 0x4B7D_D261_7D16_FF3F, 0xA26D_E4A5_E9A8_B382], // 36
        [0x3F2F_B6BA_31E4_9751, 0xD45F_FA71_1B6C_CD22, 0xD121_B4EF_A4F1_86DD, 0xCA2D_78B2_8C70_3DBE], // 37
        [0x16B2_1B5A_CDE9_58A2, 0x6D6A_5F1A_71C1_BF88, 0x0F4B_7D5A_7C16_6C68, 0x83BE_5502_0347_2240], // 38
        [0xECE0_DAE4_DCFE_83CC, 0x2169_6AC2_C681_BD2F, 0xDC2E_B565_AE2A_8436, 0x9F99_B6C1_2C73_6BD1], // 39
        [0x5885_DA9D_9AF4_CECC, 0xCA08_5FCF_6C9D_5D57, 0x2276_358C_870A_790C, 0x0285_7DE3_1B8C_BA2B], // 40
        [0x26F3_CF3C_139C_CE34, 0xBCCD_6B36_3344_79FD, 0x8D04_BC31_4245_4D74, 0xE921_260D_7276_FF2F], // 41
        [0x89BF_EC30_F531_E910, 0x49E8_D353_9798_8E33, 0x20AA_9BA5_151D_BEDC, 0x581A_132F_D83B_0E91], // 42
        [0x96E6_3B77_44E2_A2EE, 0xA441_12DF_E919_3F7D, 0x6EAF_2D8F_14CD_99A3, 0xC8B7_B1EC_022F_858D], // 43
        [0x4066_2BDC_99C5_13CA, 0x6D6A_D3C1_65B6_2289, 0xAA12_899E_8A3C_39B9, 0xA672_C179_D2F5_DC6F], // 44
        [0x9403_05B5_A1C2_F6D7, 0xFDCD_A783_E5D7_8172, 0x374F_72E0_18B9_6FCE, 0x6D14_536B_83AE_FD09], // 45
        [0xA4EB_196B_CB75_71E8, 0xCD22_BEC8_5F8B_59AA, 0x31DA_3C0F_F8BD_AA85, 0xFD84_DA13_DB2A_D0CD], // 46
        [0xBF88_4BB5_0E2C_4DF0, 0x03EC_3E07_F37C_44D3, 0xE732_9026_1DE5_850D, 0xC306_E16C_4B5D_39E8], // 47
        [0xECEC_FD3F_8A92_D3F2, 0x81C2_46FF_4418_2D48, 0xE79A_AE90_4F23_67F9, 0x17D7_F4BF_BCED_3B98], // 48
        [0xDC2A_9275_88DB_33C1, 0x36BA_5DC4_44BB_B077, 0x980D_6153_B00A_EE76, 0xA581_72C9_A6A6_4BF6], // 49
        [0xE403_8536_6C3F_431E, 0xCF70_390E_4ADC_00A8, 0x9C1F_58DE_ADB6_F1FB, 0xF901_9657_7E4C_0EE5], // 50
        [0xD879_B21F_3589_D222, 0xE27D_BD60_A13B_B101, 0x7B61_ACE7_5EF6_882B, 0x1608_64F3_5297_8520], // 51
        [0x584B_341A_E47F_86E1, 0x91F4_FCDE_700C_FCE0, 0xDF35_C7D7_814B_6C93, 0x9A6C_1CD5_E867_DD2B], // 52
        [0x4BBC_4538_0D3A_D5FA, 0x55A1_8201_E0A5_B741, 0xE54A_87C4_37D7_46E4, 0x327A_702C_846B_FA4C], // 53
        [0x233C_B239_AF9A_C579, 0x69AB_955F_682B_E950, 0x49AC_0E25_C54F_E18A, 0x50D2_6EA8_DF07_692C], // 54
        [0x25BF_08CB_D408_C528, 0x40FD_6ACE_9151_3EBF, 0xE882_59DB_91E3_CBE2, 0x0791_53C1_C286_2C2F], // 55
        [0x6D4D_DC01_D4B9_EF53, 0x3BC4_92B5_F42D_63BE, 0x7200_857A_AF08_BAEB, 0x3FE5_617D_C711_AC8D], // 56
        [0x6562_B0FF_956C_B73A, 0xFAC7_2CA6_5438_6BC1, 0x2DCA_C04A_1FC6_F422, 0x3B85_F15C_77A8_AE17], // 57
        [0xC397_0EEE_8F20_0A3C, 0x4249_412D_9D86_05B8, 0xD755_6AA0_5CC8_101A, 0x87FD_E406_FCF5_FF14], // 58
        [0x641A_2DF5_77C0_D5AE, 0x25BD_C209_ED12_CBC6, 0x38D6_22B6_29FA_A284, 0x51A6_CA6F_6382_08E7], // 59
        [0x6148_18A9_2784_8AFA, 0x2444_8944_B22E_FAAA, 0xCBBA_F724_5E62_FC24, 0xAC1D_2F42_8398_DDE6], // 60
        [0xE81D_E890_8983_A6E6, 0x266F_9F10_BF31_C6BB, 0x3ED1_8A14_4219_7A7D, 0x1D4A_D6E0_6389_984C], // 61
        [0x1DFF_1858_A92D_9576, 0x07F2_CD57_6E0A_B2AF, 0x8A92_A6DC_DB5A_2689, 0xB295_DD4C_D438_A52A], // 62
        [0x3C98_F746_795E_FBA5, 0x860D_5B0D_5B87_BFF0, 0xCA24_8FC0_90BB_B93B, 0xC279_103F_B8BF_D2C5], // 63
        [0x0EEA_540F_C806_A0C5, 0xF1CC_ABE6_996A_2663, 0x549E_F7F9_E1A7_26F1, 0xD81A_2E11_85E7_4AB1], // 64
        [0x809E_830D_D251_0428, 0x3EC5_239C_EE16_AD62, 0x819D_3264_F03B_89E4, 0xAA00_9D7D_88CD_F19F], // 65
        [0x3A55_0CA6_8380_2F3A, 0xF624_09BB_B4D2_D9C6, 0xF39C_CBAE_C8F1_ED68, 0x69C0_58A5_260B_5B24], // 66
        [0x0E53_2470_0889_1073, 0x1F55_2D17_E1EE_B0C4, 0xDB4F_F763_1070_679F, 0xB1D0_DF0A_4B0A_5079], // 67
        [0xBD64_9EAA_6438_39BE, 0x7A86_DF15_AA2F_9BE1, 0x2C16_1B66_4D03_3797, 0xA794_0EC1_B920_A6D9], // 68
        [0xA09B_04FC_F419_3762, 0xAF58_5814_168A_BAC1, 0x35FB_AB96_8F6D_8374, 0x8CBD_26CF_5ECC_FDE3], // 69
        [0xF542_2016_51AF_9DD4, 0xABBA_F45A_39BF_9820, 0xDB00_8D43_7F9A_AA2B, 0xB74C_ED42_81DE_7974], // 70
        [0xA3B9_775B_36D7_1DD7, 0x3C88_1320_3C13_E37B, 0x6292_5D3B_8905_7F4A, 0x4CD1_AD8A_525A_B974], // 71
        [0xE949_E4BC_FEFF_74DD, 0x92BF_9044_501E_5B8A, 0x343B_F69E_C79C_1255, 0xE977_E7F0_DA90_EBCB], // 72
        [0x992C_AA87_411D_E52F, 0xF49E_6B6F_DF9D_21EF, 0xDF0D_D555_F5FB_2B11, 0x73E1_79A5_3FEB_2EF6], // 73
        [0x6499_250C_352C_02E0, 0x5D73_D01C_03CE_BDA7, 0x3F8B_CB77_648A_A5DD, 0x25AF_CFE2_6394_35DB], // 74
        [0x7DDA_E75A_FCA1_C5E7, 0x0AA9_9547_8E94_601C, 0x507E_9F99_5A01_9363, 0x8D15_8D39_6852_22CA], // 75
        [0xBCF5_0CFC_9225_AD73, 0x81A0_8343_CE42_77E4, 0xF276_6596_099E_CF44, 0xE678_5D84_C752_187D], // 76
        [0x6CCF_E77C_73EF_E586, 0xE871_B2D1_1640_C73D, 0x97B0_AF54_2418_E312, 0xE4EE_E444_61C6_E7C1], // 77
        [0xF5AC_9537_205F_902E, 0x5C90_8863_14F1_6887, 0xD696_0529_3DD0_7A72, 0x6EF0_D3D3_7468_4145], // 78
        [0x4F63_2143_FF3C_C15F, 0xD576_3460_29F1_D32D, 0x8E8D_CFBE_6854_F2B8, 0xD941_D10A_07D4_35B7], // 79
        [0xFFDB_00FE_2F5A_06E9, 0x5A01_13D0_74D0_FA8E, 0x8191_6E82_E5F9_9081, 0x5667_BD8A_EDF0_01E7], // 80
        [0xDAF7_1919_EB43_09E9, 0xAC4B_10D7_D558_1719, 0x3F0B_061C_40E5_4087, 0xE024_2515_44F5_46FA], // 81
        [0xF932_ED98_E1B9_D2C0, 0x6419_19BD_07A1_FE3A, 0xB6E2_FC34_A506_7CC3, 0x79D0_3167_0594_42B0], // 82
        [0x4EBF_EBB5_2729_3B86, 0x389F_A823_F12F_594F, 0xF30B_1813_E64A_9212, 0x8BAB_C078_B00F_051D], // 83
        [0x227A_8640_62AC_5C6A, 0x1FFB_619D_8F48_18F9, 0xB233_950D_62A3_90FF, 0xF0A5_EE38_59C9_BE25], // 84
        [0x28F9_9AE3_A547_BEB1, 0x19C6_1A08_D56D_6412, 0xC30C_8909_0A84_6672, 0x8E7F_32E1_D88E_2772], // 85
        [0x7734_7442_9375_C190, 0xFB15_57F3_AAB7_77F0, 0xAB90_85B5_1154_02A7, 0x4BBC_B6AB_3CA9_0BFC], // 86
        [0xD256_8B7F_CD63_838B, 0x9B8A_7F33_5B78_24B9, 0xD6DF_26DC_52AE_FB2B, 0x9B71_818D_5F50_2898], // 87
        [0xD481_93AA_1A0E_B036, 0xD2BC_A170_920C_CDC7, 0x660B_5E16_DA4D_66C9, 0x8B39_3C7B_2E64_5474], // 88
        [0xE4CB_007C_58F0_935E, 0x6DE0_DD7D_A059_D642, 0x50E8_B3AD_9CCA_B00D, 0x80AC_A0A8_4DD6_9BD1], // 89
        [0x18C7_76C6_171C_E64D, 0xD888_79BA_DBA0_8558, 0x3C6E_652B_D1D1_1DBF, 0x6BC6_515B_29B1_E5A9], // 90
        [0x1CD1_BE40_33B3_1996, 0x625D_A8B9_B8AE_0918, 0xD4B8_F939_0B9A_B593, 0x4629_3139_8399_73AD], // 91
        [0xD8C1_B793_78CA_0CD3, 0xB4A9_7A06_F52D_46E9, 0x2E46_C22F_D246_36BF, 0x7F8D_3315_EF35_C69F], // 92
        [0x01CE_A7B6_B3F7_351A, 0xB0A7_633E_E086_F9A9, 0xD27D_0D48_896A_5406, 0x4E5D_8F98_8453_4CAA], // 93
        [0xFE96_BDBF_57C1_CCCE, 0xD230_AE4C_2EEB_9D70, 0x230A_00E1_45EE_AC0D, 0xFD1C_7BA1_2C53_547D], // 94
        [0x181D_B30D_BBFE_1ADC, 0xDF98_085E_358E_2C0F, 0xEEA6_4563_30F6_2A06, 0xF0EB_7F4B_5AB6_5BB0], // 95
        [0xD510_A3EF_054D_0BDA, 0xC808_BE61_0EAF_7CF1, 0x37BC_0DB5_0EF1_F8DC, 0x2E69_D7B6_1913_5CDD], // 96
        [0xBC8A_59D4_5329_BD80, 0xACCC_8AF0_6B7B_3709, 0x30F2_30D6_3C6F_BE9F, 0x9FD6_61E5_DE10_0A0D], // 97
        [0x9D94_EF9C_1E42_7CF1, 0x8B5A_4E86_3345_CDEB, 0xCF9D_501E_65CD_A30E, 0x3B4B_1355_C398_9E64], // 98
        [0x3711_8880_F81A_6DBC, 0x6F7E_73C1_9A54_DDD0, 0xBB82_C717_7E3A_37E9, 0x519C_662C_7610_C30F], // 99
        [0x2B85_40BA_FB90_175F, 0x6AC2_0B36_9C49_D45C, 0x477A_7F8D_3586_FB0F, 0x4996_89A2_D9EA_D94D], // 100
        [0xBD35_9D13_1CE6_C6AD, 0x9CDD_EE07_9E68_DAC0, 0x12D8_B39E_A4E0_50B8, 0x9696_84E8_9329_47AB], // 101
        [0xA1BA_9E34_0682_913D, 0xDEB4_E450_0DF2_3CB0, 0x4B67_D82E_C4AB_DD3C, 0x379C_E725_7CC3_9930], // 102
        [0xDAB9_E242_6C1C_AEBD, 0x6AA4_59DB_CC06_13D0, 0xD969_C9B7_7A5B_7A5A, 0x7421_1BD7_ADEE_3195], // 103
        [0x087B_0288_E410_2C7B, 0x87DD_F130_75A6_742F, 0x3C15_59BF_1161_B3E3, 0x1026_CCDC_0171_FF17], // 104
        [0x4708_AE7D_102A_4BE5, 0x87E3_8326_DFCB_70F0, 0xAC8F_7F69_CD5D_91F4, 0x5A66_9628_0CB1_5124], // 105
        [0x9691_5A2C_1BA8_9549, 0x1DC8_BA3A_089F_B0A5, 0x8CC5_DE01_8B20_EF49, 0x662C_7950_2B52_14AC], // 106
        [0xC108_4280_8249_9C95, 0x18B8_61E3_5978_8EB5, 0xEC6F_64AF_49D7_C65E, 0x1D5D_A983_9F2F_75AA], // 107
        [0x1A67_A243_FB2C_5296, 0x48F4_7AB5_E9E1_0A6F, 0x882C_7E36_6FCC_0E90, 0x317F_C4E8_90A8_8379], // 108
        [0x5987_29F4_B028_E3B7, 0x7794_D149_DAFB_17E5, 0xD011_7AA2_5830_66CA, 0xDC80_5D88_E025_362F], // 109
        [0x9A67_2B36_1619_82BE, 0x2CE0_C3B5_165C_85D5, 0xF68F_6BD3_4377_6452, 0x6E18_AAA3_9E8F_D9EA], // 110
        [0x7C5D_DEBA_E795_0D4E, 0x483D_6170_EAA1_B26A, 0x44D8_D62F_7B7F_1EB1, 0xCDCE_232C_5A77_C8F7], // 111
        [0x376A_1F0E_AA1B_85BD, 0xD6F3_FDD4_1E88_FEE9, 0x7AF8_58BC_7869_E812, 0x586A_0313_1EB7_E2F8], // 112
        [0x98F7_63E5_713D_3095, 0x8A53_ABAD_FECB_7266, 0xDA7C_96D4_7CDB_208A, 0x1075_0923_EB26_717F], // 113
        [0x317A_7AF3_DECF_EFE3, 0x7D45_AA4E_0CD8_FE27, 0xD182_60A5_E6F7_2C5C, 0xF054_26FE_57E0_34E5], // 114
        [0xA143_828A_6492_6092, 0x0B96_F041_A4C6_CD31, 0x2FE6_2A5D_BFA4_12DA, 0xA39A_E2E9_E798_1EC3], // 115
        [0x81F2_BC48_7197_924A, 0x3D82_89BE_DBC3_B6F6, 0x0038_5249_1CCF_39D1, 0x1AF0_BABD_2D27_A9EF], // 116
        [0x188F_7221_0E80_15D0, 0xA7EC_44DD_F702_C81B, 0x5F8A_7736_7F12_CA4C, 0xB977_A284_3893_DF47], // 117
        [0xB6AC_F0BB_D42E_CAB0, 0x39D0_5407_8A4C_75BB, 0x9016_8009_4BA5_2D8E, 0xA68C_6A74_76F2_9C93], // 118
        [0xC0A9_1362_3FB8_3AB9, 0x6473_9BDE_9974_C426, 0xDFC1_C46E_3EE0_A8C6, 0x15C4_592E_0810_4B6A], // 119
        [0xBEE2_3865_C655_4D32, 0x4E43_0512_544D_7605, 0x6DF0_C5DE_D845_7883, 0xCF67_B46D_5F24_51C7], // 120
        [0xD14C_2A16_0FAD_09B7, 0xC9A3_6791_AF0C_15D9, 0x6AAD_4ABA_7011_26AE, 0x3327_5D16_A842_2412], // 121
        [0x0303_A887_9B31_37DC, 0x4440_99DC_7FD5_28E2, 0x718F_3D7B_1770_0AAC, 0x96E2_7B64_D2E0_191F], // 122
        [0x23FD_8E2E_9C85_1E17, 0x7989_2FA1_B89C_B96F, 0x3971_FAEB_8B10_CA75, 0x3248_41DF_5218_9CF2], // 123
        [0x0A46_F41F_E878_335E, 0xFAE3_074C_B85E_8A6E, 0x579E_D146_5FAF_5E4C, 0x936F_B727_935C_ACA6], // 124
        [0xF142_3FFE_B768_599F, 0xC83F_9F3D_0684_6C36, 0x272E_CD25_2B38_5969, 0xF0F9_8DE9_A339_15EC], // 125
        [0x2381_A767_8693_19E8, 0xF92D_3A4A_7EAC_9657, 0xE1BD_D71E_F40B_376A, 0x2DE7_B59B_5CCD_B211], // 126
        [0xF6C2_12C7_1BC3_E398, 0xC4FD_A942_D804_6C14, 0x6956_53DC_1F3D_05D8, 0x20EA_B9ED_C7A6_8503], // 127
        [0x92C3_F7C0_F221_CE84, 0x46BC_0424_6DD2_6E7E, 0x92EB_D8D1_648B_F972, 0x91C9_FAED_3F5A_F542], // 128
        [0x97F9_A2C4_FBC1_0710, 0x680F_CCF7_1901_59A9, 0x260E_491D_1491_1CDF, 0xDF1F_D186_DFD0_9E04], // 129
        [0x76B2_4FFA_C5E8_9870, 0x6AAD_38BA_F1E7_A054, 0xE1C0_D8A0_D629_9E3C, 0x925A_CD23_C632_92C6], // 130
        [0x220B_1FBE_128C_2666, 0xBF1A_CB67_4B2E_E4E1, 0xEA97_7EBA_143E_3709, 0x8DD7_0202_F536_F74A], // 131
        [0xB46E_9886_D975_83D6, 0xE0D2_7510_25D5_1459, 0x8365_498F_3CEB_F7DD, 0xE50D_42EB_ABFE_424B], // 132
        [0x0A6E_D051_B750_69D9, 0xB7DF_8AEA_9812_D11A, 0xF7A4_D2BD_89E3_F2B6, 0xBF67_C4E3_0EAA_57E3], // 133
        [0x435D_8D1C_A56B_9744, 0x7679_79CF_61D7_31E3, 0x64AB_E173_6D68_B8EA, 0x69C9_F5D6_9560_6CDC], // 134
        [0xE80F_186D_4A34_8293, 0x2D2D_FDFA_2EBE_36FB, 0x388B_1003_B6D7_CFAC, 0x7DD5_06C5_0BA1_EF33], // 135
        [0x6D82_2A80_B66D_31E3, 0xC6A8_010B_3312_E486, 0x7527_8657_AE52_BA58, 0xE7F9_6F60_C61A_CAAA], // 136
        [0x20D8_6753_C00A_B5A3, 0xDAAE_3B2A_50E8_39BE, 0x447A_06A4_7E40_5D93, 0x5E32_4811_27E7_DA68], // 137
        [0x854B_9F8D_D631_C657, 0x433B_347E_6D62_B816, 0xA9EA_2C19_6E27_EA28, 0xC993_B2D2_01CC_60AB], // 138
        [0xC334_E677_B7C4_9A7A, 0xAC4E_669F_F887_AB50, 0x0FBE_68A3_C7A0_1508, 0x8E8D_398F_2089_2D0E], // 139
        [0x9F21_08D6_651F_6B78, 0x02AA_41A6_0BCB_C6E3, 0xF971_9E86_2016_0CDC, 0x4A8A_3698_2535_0181], // 140
        [0xAB17_3C72_7895_E326, 0x5C87_A2D2_0087_A1A7, 0x52DD_DCD4_C42C_CAE8, 0x7949_48F9_A9D1_409D], // 141
        [0x97F0_C6BA_B1A0_3BCC, 0x6785_0299_CA96_A038, 0x6CCF_082E_885E_8208, 0x20D4_8506_A371_C562], // 142
        [0xFACA_B350_060B_052E, 0xDAAF_1743_3875_829B, 0x67BD_7ABC_F572_7FA4, 0xAEAE_A3B1_879D_FEA3], // 143
        [0x6B9E_1C53_CD74_ECE8, 0xDA34_A663_CE16_5463, 0x6470_1B5E_B896_3B95, 0xD555_238F_E5B4_50A6], // 144
        [0x600C_664B_CB15_7A83, 0x05D8_855F_411F_FFC1, 0x7BF2_D208_ED17_1F47, 0x29C1_62EB_848A_F054], // 145
        [0x895C_4CC6_C7BD_4A95, 0x376E_AC12_B196_0C80, 0x08FB_D1F5_A950_3BE3, 0xF3EA_C359_80CE_48C4], // 146
        [0x12AB_A9D2_7A24_CF86, 0xA033_BA14_B5A2_AD66, 0x9261_542A_A97E_78FF, 0x72F2_B2AD_904A_A1D7], // 147
        [0x2CDF_460A_11A6_96F5, 0xE277_AE67_82A8_7E61, 0xB9CE_8B8F_715B_2243, 0x0868_E750_5E09_7D11], // 148
        [0x9A09_FE94_E9D5_DD45, 0xE615_FF34_86F8_F688, 0xAEAC_A168_AAEF_B65D, 0xA6F8_3C85_AD84_624A], // 149
        [0x3C16_E3EB_1643_98B0, 0xE81D_95BA_E733_51A6, 0x8B8B_E7C7_3765_63A9, 0x4984_D6A3_0803_8F10], // 150
        [0xFD8B_00BA_FA17_7D7D, 0xF284_4B4B_4C08_8115, 0xA003_0252_7A71_F9A8, 0x903F_63F4_9F58_CFB8], // 151
        [0xF49A_32B9_129B_13DC, 0x84B2_534D_C0F7_719E, 0xB381_D59B_4898_7C2C, 0x8296_EE37_702A_DA18], // 152
        [0x2309_3951_81FA_1190, 0x9733_2C2A_421B_FEDD, 0x3457_0958_8EC8_5B67, 0xA02E_0432_2FDE_9244], // 153
        [0xD15B_5A2E_0AF8_D4F0, 0x7684_921B_E89A_B13E, 0x30C3_2861_245F_C5AF, 0xEBFA_1E39_67BB_9BA4], // 154
        [0xB4B1_5A83_10CB_711E, 0x5000_5985_CE26_C53B, 0x76B2_1A60_A145_83FE, 0x8F7D_C3DF_2467_4BC4], // 155
        [0x590C_4508_FB00_3F91, 0xECC7_5971_537C_91F0, 0xE092_4892_61CD_CFB4, 0xF72E_D721_8420_863D], // 156
        [0x0812_461A_5E6C_C263, 0xC38A_81A4_5BD0_7F28, 0x4E16_738E_8088_5F50, 0xB80C_59B5_9DB8_BBA7], // 157
        [0x02D3_C32E_5D42_312B, 0xF712_A37F_5071_03D9, 0x90DF_7221_5C23_F167, 0xA399_4BF8_8B22_EAEB], // 158
        [0x4C0F_62B8_E6F9_3D2D, 0x17EB_B60C_2635_7749, 0x3EE0_9156_C6CD_6980, 0xFE7A_FD92_9C53_D024], // 159
        [0x9193_C730_48EE_E239, 0xF015_3FAA_519B_6E80, 0x2004_8B75_CF45_276D, 0xF41F_5B37_3108_4B95], // 160
        [0xBBA2_7529_139D_6A0E, 0xA7AE_4C7C_B274_62C8, 0x8C7B_B5DD_DBDA_BA05, 0x5911_9468_1695_8AAD], // 161
        [0xF6D5_6D23_9D85_7692, 0xDDE7_3069_2405_2C66, 0xC3D0_608C_34E0_429D, 0x7368_F496_CFB3_AA2B], // 162
        [0x5E06_B1EF_449D_F096, 0x56C8_C360_E5D4_FC53, 0xFC6F_1DDC_816A_EE1A, 0x1F4F_6D77_7A21_1636], // 163
        [0x339C_928D_BDBE_227D, 0xFF0E_9FA4_41C4_5769, 0xF6DD_D228_5569_0FC3, 0x4C14_7FF5_04CB_D678], // 164
        [0x20D2_976E_4B21_EDFE, 0xC984_25C2_55D3_7E47, 0xE02E_E539_19DC_01F4, 0x0CA9_4F72_BF1D_A847], // 165
        [0xF28E_977D_A5F3_124E, 0x9862_5D18_2093_70A6, 0xB59D_EE8B_B2D3_C12D, 0xAB74_D340_AE38_C5AC], // 166
        [0xB109_592A_D294_04A1, 0x0F1E_063D_35ED_2323, 0x9A77_9E0D_2FFE_6286, 0x39BA_86E4_9A8A_E073], // 167
        [0x1B5C_139A_E910_165C, 0x3C89_C287_79FD_12AF, 0x6B0A_0F04_9A72_59CE, 0x438F_483A_E16D_2ADC], // 168
        [0xFD96_0901_A4BC_2C63, 0xA6CB_C1E1_4B01_9600, 0x699F_9DF4_E64E_2EA8, 0x07F3_85A3_9569_4A63], // 169
        [0x29CF_891B_A080_2F12, 0x099F_6EA4_BF88_4796, 0x9FD1_D47B_12D5_1D72, 0x7D3A_C6CE_ECC2_D772], // 170
        [0x2962_67E1_1192_6D03, 0x21FD_43AD_08A7_8F6A, 0x2355_2944_DA58_9953, 0x0D06_371E_033D_91C1], // 171
        [0xFE46_BA37_85D6_9762, 0x3254_70F6_A7BA_459B, 0x68BC_BFA2_58C5_633E, 0xAD04_6FF2_2393_A019], // 172
        [0x8C56_9718_5D3D_5B59, 0x1F7B_7BB8_9ED1_5A5F, 0xC527_5EC6_398A_F43E, 0x9AF8_33D7_3AD7_B584], // 173
        [0x2642_A399_6CAC_3A7F, 0x8C40_D6FF_9ED4_33CA, 0xDA62_1C2B_9E59_28B6, 0x8D6F_AEA5_DB15_8C71], // 174
        [0x6C4A_5ED4_7833_CE52, 0xFB35_37D8_CDAA_3C2E, 0x362E_9D50_279A_48A4, 0x3B3F_9FB3_124A_CED3], // 175
        [0x38E2_B389_67FB_1FFF, 0x2172_0038_545A_F4BF, 0x8BDF_A584_A31E_D923, 0xA320_E6E8_6BA1_8F77], // 176
        [0x6859_496B_C9A5_F9CE, 0xCF1F_B2AF_B8EF_4D07, 0xD014_07BE_355E_0192, 0xA490_2D0E_1455_8CE0], // 177
        [0x2ADB_6CC7_B4EE_8C07, 0x4DA8_1B99_FAE0_9C2A, 0xC8E5_EC1D_BA60_B4B6, 0xFE88_F913_CD20_73BB], // 178
        [0xA63C_31B9_F2ED_E895, 0x8191_17BD_5E4F_7ACF, 0x3A11_650D_CEB6_F26A, 0xF90D_9CA1_ADE6_538B], // 179
        [0x8F84_88F4_A0CA_4967, 0x9856_AB58_3F47_4469, 0x806D_DA8C_EAC3_3E5F, 0xCE75_1694_0F88_5251], // 180
        [0xADC5_9907_3EBB_41A2, 0x6EC8_AFDE_76DA_B19F, 0xF7E7_CDFE_98F1_2EAC, 0xA01A_B884_8E5B_0800], // 181
        [0x7C48_197E_36D6_95C3, 0x63C2_2363_AFF8_5DD9, 0x826D_6159_732F_2E98, 0xD54B_71C1_10F4_428C], // 182
        [0xAB94_3C50_6DD9_1BDB, 0xE61C_89FA_EA4A_5709, 0x37E3_A6FE_A665_F5BA, 0xFBF9_A6E8_99A6_E31F], // 183
        [0x2DCC_DC29_DC6B_51F4, 0xD2DF_DDDF_AFE6_A820, 0x0745_255D_2C3C_8BE1, 0x93B0_CA65_80BC_1A27], // 184
        [0x15E8_40DF_C1E8_168E, 0x82D7_562E_ACFD_C9A1, 0x9F13_35FF_2669_B66D, 0xAAE3_4305_2715_4D82], // 185
        [0xFD17_D3E5_4951_E733, 0x4224_9A89_5774_AC26, 0xBCAC_4EEE_9B2B_D8C1, 0x4AB8_2420_8B09_398C], // 186
        [0x2A14_D534_B0D9_A99F, 0x8EFF_0E88_1FC4_B844, 0x9AEE_35DB_AA15_22C4, 0x26BF_50BC_09C5_B86F], // 187
        [0x57F6_4E0E_3F4A_85EC, 0xB8EF_8173_3DBA_5178, 0x8946_1174_04AE_15E3, 0x4EDB_9DAA_1715_AB7A], // 188
        [0xAED6_D389_D054_05B3, 0x0692_EFCA_A107_89EB, 0x6233_8132_0D2E_A3D6, 0x9306_5ABF_D959_BB23], // 189
        [0x4518_FE27_9462_B473, 0xE40A_24BC_236B_F820, 0xE30E_7BA6_240F_6D85, 0x8802_23DD_637A_549D], // 190
        [0x4215_5C1D_EC3A_B5B2, 0xE251_1977_6D12_6915, 0xAD87_3B5E_3BCB_06BB, 0x334E_EE77_A61F_C9EB], // 191
        [0x38C2_6A20_5CCF_DB39, 0x53C7_88DF_A9BA_256E, 0xF014_0440_0C02_FF91, 0x2F34_A64D_CE29_8731], // 192
        [0x000F_734B_F0CA_3F85, 0x2EC0_D291_7CB7_1B65, 0x152C_97E2_76B9_C31D, 0x11B0_EC32_4746_0F3D], // 193
        [0x36B4_1B56_3BF1_F601, 0xDB00_1E28_073D_8178, 0xAFDE_65D1_DAD4_7064, 0xB9CD_FF62_4773_E2FA], // 194
        [0xA9F7_837C_4382_7142, 0x82D9_2A9E_604F_0352, 0x12F2_4825_BDAB_34BF, 0x4291_8750_EFA8_D6F0], // 195
        [0x8BF0_82B4_A536_3EA5, 0x7B2D_E04D_0859_E26A, 0xEAF3_7A19_6783_7108, 0xCCAC_4BDC_6C0B_17C8], // 196
        [0x1C09_F299_250E_4E6B, 0x94BE_0D79_8927_3C6A, 0x856A_2870_467E_6BE9, 0x7282_DEE6_4A3C_E83B], // 197
        [0x01D0_8250_962C_D5A9, 0xC2F0_ED7B_747E_7479, 0xAFD0_7F01_F321_7EFE, 0x768B_1C67_3836_23C1], // 198
        [0x822C_FDCF_0C2F_50DA, 0x9533_2554_7332_CD74, 0xE386_EA69_029F_4856, 0x5977_7356_CFAB_998C], // 199
        [0x95FA_4DFF_EAA4_7D59, 0xE79B_4D05_216A_0E7F, 0x5DEA_507D_179B_F6BE, 0xCC2C_75E1_3C54_C1E2], // 200
        [0x7165_890F_E3DF_EB75, 0x39C4_06B4_D850_A93E, 0xFA8D_4FAB_E3B0_6442, 0x823C_B8EC_842F_BC5B], // 201
        [0x74AD_8578_8A43_F20E, 0x7699_54EA_E28C_9A9D, 0xD8DD_AFFA_0A20_B195, 0xC205_1A05_9063_6BC8], // 202
        [0x1729_41E2_9BFF_7FB9, 0x4D5D_86C4_05DE_1958, 0x8771_3D8E_58B0_80D5, 0xF63F_C7F5_6CFD_D3EA], // 203
        [0x6EE4_2C23_F564_20A4, 0xC267_AC32_827F_A43B, 0xB1B7_3234_7AEB_C79E, 0xDA9B_88B7_8E5D_2C35], // 204
        [0x27FF_CAC9_E2F6_F4EF, 0x6A8C_EFD1_4956_2EA4, 0x510E_3DB6_B5E6_EC70, 0x39E6_558F_97EE_ACD7], // 205
        [0xA2B9_90E2_87FA_8A5D, 0xDB34_D12B_2140_CCB4, 0x44C8_6505_FB04_77AE, 0xFDCD_BE85_F49F_4B50], // 206
        [0x8354_747C_1E18_BE0E, 0x1583_FCE0_0E33_9707, 0xA3FB_F0FA_AF87_52CE, 0x74C9_72B7_6AD2_902A], // 207
        [0xDDC8_FB60_BD8E_B41F, 0xA92B_B0B2_867F_6BF6, 0x0BFB_75C5_0C5D_415A, 0xC550_0DD1_19F4_A731], // 208
        [0x0B7E_33FB_264E_3A54, 0x7476_B424_719B_46B5, 0x0093_6E21_51C6_597D, 0x9961_4062_1CD9_B52B], // 209
        [0x8A30_8F39_44D6_AEE9, 0x6D18_6304_1EE3_EC9A, 0x4BAC_3F36_A8F0_76B7, 0x0C3F_4395_AA4A_7F7C], // 210
        [0xF2ED_C8C3_E0F3_F06A, 0xD434_F068_9A15_12B2, 0x370A_6B7D_028D_6142, 0x936E_B9EF_829C_BA0D], // 211
        [0x25CF_59EB_274D_0671, 0xDFB5_3977_5E06_4780, 0x8276_334E_7193_6CDB, 0x1427_29C1_9AB8_191F], // 212
        [0xB92D_033D_E1E2_06B3, 0x46CB_93AD_D36D_8806, 0x9CBB_9729_366E_E6FF, 0xB6D8_8174_3755_EDC2], // 213
        [0x8280_17A0_CD3D_D80C, 0x73CD_A639_7B53_813F, 0x7825_DAC8_6F44_639D, 0x932C_9E8B_6431_BF9D], // 214
        [0x64CB_3035_C2B3_8324, 0xF8EF_AA3C_35F7_45C2, 0x7B8F_68AB_A1A8_CB31, 0x5BD7_B2D0_B0F3_2009], // 215
        [0xB7E1_3616_83BA_0504, 0x1436_CB71_CA5D_DE9D, 0x2BA0_3651_A50F_95F9, 0x7AAA_52D3_0A85_709C], // 216
        [0x4F1E_7E90_B6D8_BEBF, 0x0831_B4D6_0C5C_E3C2, 0xD30F_1E9B_AAF3_DF6A, 0xDEF3_D7FF_AD6F_3020], // 217
        [0x9903_0D9A_3583_CB7B, 0x2D84_BBE8_6144_58C4, 0xEC65_FFFE_13E5_A486, 0x3CB7_3945_9368_BF20], // 218
        [0xE01F_33AC_1E3A_F020, 0x4C79_745F_F34E_2DA5, 0x43C2_717C_B2C1_4D3C, 0x2B6D_849D_9867_0076], // 219
        [0x73F6_BF09_7DE8_2456, 0x7A6A_5225_2474_7F1F, 0x61E3_111D_B6B6_5A50, 0x8F0D_CFC0_DA95_7EFF], // 220
        [0x79A9_8D66_7039_B74D, 0xD4C5_65FA_03B9_D7BA, 0xC7F3_5131_A02C_C5B8, 0x919A_FD59_FCCA_8985], // 221
        [0xDAE1_CADB_F9C7_C4FF, 0xB5DD_8D7E_DA39_455B, 0x1E59_ED27_A3FF_9B89, 0xBB2A_2FF3_037F_5EB5], // 222
        [0xE869_6EEF_C6FB_8B85, 0xBA25_10AD_7CAA_4271, 0x1DFB_B02A_7A26_05FA, 0x9F44_3630_314B_6D9C], // 223
        [0xD406_2CAA_0D99_BBEA, 0x7463_E212_6C09_C301, 0x5EAF_E21C_933C_1585, 0x1BD4_EEDD_4A9C_CCC9], // 224
        [0xE121_B8B8_0E32_82FE, 0x2192_390A_F08E_A20C, 0x8965_931C_57B0_9CFA, 0xC38E_41AB_56B0_BA5B], // 225
        [0xF236_8405_9147_9CE3, 0xBBB5_10F9_78E1_A7FE, 0x35D9_6D3C_24FB_B84D, 0xAA3E_60B3_DD16_56E4], // 226
        [0x9B1E_305B_76C2_A664, 0x29DF_8C69_9671_F356, 0x02C7_B311_A425_6010, 0xDE71_82E9_8AF3_9A6D], // 227
        [0xDAC4_AA7C_ACF8_3D44, 0xD5ED_F510_BF4C_BC0C, 0xB0E1_3E01_B365_7EBA, 0x4D4C_382E_AC24_E2C4], // 228
        [0x9F48_004B_F6BC_608B, 0x0743_DA02_BD3D_9D58, 0x55E9_06DF_7246_A7E8, 0xCA02_D91C_7FC5_FA51], // 229
        [0x5E53_4A4B_4149_818E, 0x9454_11BB_226E_E2BC, 0x0298_537B_63E9_6CE2, 0x9DC3_9C17_861F_21F6], // 230
        [0x5D8A_966E_A770_5C42, 0xABE7_6814_3056_C39F, 0x3AA1_B4D3_F4F3_7286, 0x1B28_2C67_408D_9E00], // 231
        [0x3FD3_7467_CF05_104B, 0xD87E_F267_08BA_E09D, 0xA2EF_6E29_A747_49F4, 0xFFE0_A88D_EE76_01B5], // 232
        [0x9629_55E0_8AD3_BBA2, 0xBD9F_31E0_E710_D5F6, 0x67B1_CB09_8C96_23AF, 0xCF7C_BCB9_276B_E8BC], // 233
        [0x75C1_2707_BEA5_3975, 0x849C_58C9_6C39_098C, 0x35E5_FC14_A9FB_515A, 0x30FA_7E7D_9720_2D76], // 234
        [0xD7C1_DD22_2C76_6DDA, 0xC52F_C8C2_516D_54C9, 0xAFB0_E2FE_C7C4_1025, 0x7AF5_4E8F_46B4_ED8B], // 235
        [0x509C_F798_AE46_C05A, 0x4BB9_7DDF_662E_071C, 0xE609_75B8_BBA5_2E26, 0x7282_A039_FCA1_D6CA], // 236
        [0x4B68_ECDC_EEC1_3459, 0xC150_8192_B901_F4CD, 0x51CA_B8C8_BE23_9501, 0x4B67_3FD7_EA67_A571], // 237
        [0x3170_E641_7657_BDD3, 0x722E_B0F2_E27D_8199, 0x986D_50FB_D9CA_5E0E, 0xB498_B23B_055D_58BF], // 238
        [0x6B74_4B46_44E4_447C, 0xCE06_9DF3_9EAA_9F5F, 0x9183_A012_8376_28AD, 0x9BD5_474B_DF6A_18A1], // 239
        [0xD6C7_5C59_E87C_7626, 0xA5A8_7694_2B05_F78E, 0x7B4E_B53E_F127_8E0E, 0x99B7_B978_0A77_A8AB], // 240
    ];

    #[test]
    fn matches_the_reference_on_text() {
        for &(input, at_zero, at_seed) in TEXT {
            assert_eq!(rapidhash(input, 0), at_zero, "{:?}", std::str::from_utf8(input));
            assert_eq!(rapidhash(input, GIN_SEED), at_seed, "{:?}", std::str::from_utf8(input));
        }
    }

    #[test]
    fn matches_the_reference_on_the_sanity_buffers() {
        let (squares, primes) = (squares_buffer(), primes_buffer());
        for (n, want) in SANITY.iter().enumerate() {
            let got = [
                rapidhash(&squares[..n], 0),
                rapidhash(&squares[..n], PRIME32),
                rapidhash(&primes[..n], 0),
                rapidhash(&primes[..n], GIN_SEED),
            ];
            assert_eq!(got, *want, "len {n}");
        }
    }

    /// Distinct inputs that differ least, under GIN's seed and another: all
    /// zero bytes at every length to 256, a 64-byte input (zeros, then the
    /// primes buffer) with each single bit flipped, and every one of those
    /// under both seeds. A hash that ignored a length, a position or a bit,
    /// or let one seed's outputs land on the other's, collides here.
    #[test]
    fn near_inputs_and_seeds_do_not_collide() {
        let mut inputs: Vec<Vec<u8>> = (0..=256).map(|n| vec![0u8; n]).collect();
        for base in [vec![0u8; 64], primes_buffer()[..64].to_vec()] {
            inputs.push(base.clone());
            for bit in 0..64 * 8 {
                let mut flipped = base.clone();
                flipped[bit / 8] ^= 1 << (bit % 8);
                inputs.push(flipped);
            }
        }
        // The 64-zero-byte input appears twice; every other input is distinct.
        let distinct: HashSet<&Vec<u8>> = inputs.iter().collect();
        assert_eq!(distinct.len(), inputs.len() - 1);
        let mut hashes = HashSet::new();
        for seed in [0, GIN_SEED] {
            for input in &distinct {
                assert!(hashes.insert(rapidhash(input, seed)), "{} bytes under {seed:#x} collided", input.len());
            }
        }
        assert_eq!(hashes.len(), 2 * distinct.len());
    }
}
