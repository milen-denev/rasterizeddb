//! Golden vectors for [`FILTER_DERIVE_VERSION`]: every derivation a persisted
//! filter's contents depend on, pinned to exact outputs.
//!
//! Per-value vectors cannot see an ordering change (one value's bound is
//! itself), so zone bounds are pinned as sequences observed into a zone, and
//! what `zone_may_match` keeps for each `SargOp` against probes. Planes are
//! persisted as raw words and read by hand-tuned kernels, so an interleaving
//! changed in `write_slot` and the kernels together would pass every value
//! vector: plane vectors pin the exact words `write_slot` produces, and probe
//! vectors run fixed plane words through every kernel this CPU can run.
//!
//! **The bump rule.** [`GOLDEN_V1`] is the version's frozen block and is never
//! edited; lines that are only *added* go in [`GOLDEN_ADDED`], with no bump.
//! A frozen or added line that no longer holds means a filter persisted by the
//! previous binary would be read differently. Nothing on disk records
//! [`FILTER_DERIVE_VERSION`] yet, so the bump is three constants: bump it,
//! bump `persist::FORMAT_VER`, and raise `persist::OLDEST_READABLE_VER` to the
//! new `FORMAT_VER`, which is what rejects the old sidecars. Then freeze the
//! render (`RDB_PRINT_FILTER_VECTORS=1`) as `GOLDEN_V<n>` in place of the old
//! block, empty `GOLDEN_ADDED`, and append a row to [`DERIVE_VERSIONS`].
//! Rows are never edited, and any change to `FORMAT_VER` or
//! `OLDEST_READABLE_VER` appends one too, so the rows are the history the
//! test checks a bump against.
//!
//! **Coverage.** [`class`] is one `match` over every `DbType` with no `_` arm,
//! so a new variant does not compile until it is classified, and
//! [`every_type_is_classified_and_covered`] then requires the vectors its class
//! needs, for every type the codec decodes, not only those listed in [`ALL`]. The filter classifies by the declared `field.db_type`, never by
//! `physical()`, which is why several excluded types that are physically
//! `STRING` stay excluded here.

use super::bloom::{BLOCK_BITS, LANES, SALT, SUPERZONE_ROWS, SUPERZONE_ZONES, SuperBloom};
use super::hash::{
    CODE_EMPTY, CODE_FIRST, CODE_NULL, HashSeed, canonical_hash, fingerprint, is_fingerprintable,
    is_indexable,
};
use super::kernels::{ZONE_ROWS, ZONE_WORDS, probe_zone_scalar, runnable_kernels, write_slot};
use super::persist::{FILTER_DERIVE_VERSION, FORMAT_VER, LoadReject, OLDEST_READABLE_VER};
use super::sarg::{MaskExpr, extract_with_scales};
use super::zonemap::{STRING_BOUND_PREFIX, SargOp, ZoneMap, successor};
use super::{FilterConfig, TableFilters};
use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// Fingerprint planes and zone bounds.
    Fingerprinted,
    /// Zone bounds only.
    BoundsOnly,
    /// Not indexed: `ZoneMap::new` is `None` and no column is persisted.
    Excluded,
}

/// The filter's class for each declared type. No `_` arm: a new `DbType`
/// variant must be classified here, and added to [`ALL`], before this
/// compiles and the coverage test passes.
fn class(t: &DbType) -> Class {
    use Class::*;
    match t {
        DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128
        | DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64
        | DbType::U128
        | DbType::F32
        | DbType::F64
        | DbType::STRING
        | DbType::DATETIME
        | DbType::CHAR
        | DbType::DECIMAL
        | DbType::UUID
        | DbType::INET => Fingerprinted,
        DbType::BOOL => BoundsOnly,
        DbType::BYTEA
        | DbType::ARRAY
        | DbType::JSON
        | DbType::BPCHAR(_)
        // Reporting-only, as `JSON` is: a `timestamp` column is stored and
        // indexed as the `DATETIME` it is.
        | DbType::TIMESTAMP
        | DbType::JSONB
        | DbType::INTERVAL
        | DbType::MACADDR
        | DbType::MACADDR8
        | DbType::BIT
        | DbType::VARBIT
        | DbType::DATE
        | DbType::TIME
        | DbType::TIMETZ
        | DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE
        | DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE
        | DbType::UNKNOWN
        | DbType::NULL
        | DbType::START
        | DbType::END => Excluded,
    }
}

const ALL: [DbType; 50] = [
    DbType::I8,
    DbType::I16,
    DbType::I32,
    DbType::I64,
    DbType::I128,
    DbType::U8,
    DbType::U16,
    DbType::U32,
    DbType::U64,
    DbType::U128,
    DbType::F32,
    DbType::F64,
    DbType::BOOL,
    DbType::CHAR,
    DbType::STRING,
    DbType::DATETIME,
    DbType::TIMESTAMP,
    DbType::UUID,
    DbType::INET,
    DbType::BYTEA,
    DbType::ARRAY,
    DbType::DECIMAL,
    DbType::JSON,
    // Reporting-only, as JSON is; the byte decodes to a `bpchar` with no length.
    DbType::BPCHAR(0),
    DbType::JSONB,
    DbType::INTERVAL,
    DbType::MACADDR,
    DbType::MACADDR8,
    DbType::BIT,
    DbType::VARBIT,
    DbType::DATE,
    DbType::TIME,
    DbType::TIMETZ,
    DbType::POINT,
    DbType::LINE,
    DbType::LSEG,
    DbType::BOX,
    DbType::PATH,
    DbType::POLYGON,
    DbType::CIRCLE,
    DbType::INT4RANGE,
    DbType::INT8RANGE,
    DbType::NUMRANGE,
    DbType::TSRANGE,
    DbType::TSTZRANGE,
    DbType::DATERANGE,
    DbType::UNKNOWN,
    DbType::NULL,
    DbType::START,
    DbType::END,
];

/// No `_` arm, for the same reason as [`class`].
fn op_name(op: SargOp) -> &'static str {
    match op {
        SargOp::Eq => "eq",
        SargOp::Gt => "gt",
        SargOp::Ge => "ge",
        SargOp::Lt => "lt",
        SargOp::Le => "le",
    }
}

const OPS: [SargOp; 5] = [SargOp::Eq, SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le];

/// Each op's entry in [`OPS`]. No `_` arm, and the index is a constant, so a
/// new `SargOp` variant does not compile until it has an arm here, and that
/// arm's index does not compile until `OPS` has grown to hold it.
fn op_in_ops(op: SargOp) -> SargOp {
    match op {
        SargOp::Eq => OPS[0],
        SargOp::Gt => OPS[1],
        SargOp::Ge => OPS[2],
        SargOp::Lt => OPS[3],
        SargOp::Le => OPS[4],
    }
}

fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "-".into();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn f32s(v: &[f32]) -> Vec<Vec<u8>> {
    v.iter().map(|x| x.to_le_bytes().to_vec()).collect()
}

fn f64s(v: &[f64]) -> Vec<Vec<u8>> {
    v.iter().map(|x| x.to_le_bytes().to_vec()).collect()
}

fn strs(v: &[&[u8]]) -> Vec<Vec<u8>> {
    v.iter().map(|x| x.to_vec()).collect()
}

macro_rules! le {
    ($($x:expr),* $(,)?) => { vec![$($x.to_le_bytes().to_vec()),*] };
}

fn inet(family: u8, addr: &[u8], prefix: u8) -> Vec<u8> {
    let mut v = vec![family];
    let mut a = [0u8; 16];
    a[16 - addr.len()..].copy_from_slice(addr);
    v.extend_from_slice(&a);
    v.push(prefix);
    v
}

/// One zone's observed values per entry (zone `i` gets entry `i`), and the
/// probes every zone is asked about. Empty for an excluded type.
fn zones_and_probes(t: &DbType) -> (Vec<Vec<Vec<u8>>>, Vec<Vec<u8>>) {
    let nan32 = f32::from_bits(0x7FC0_0001);
    let neg_nan32 = f32::from_bits(0xFFC0_0000);
    let nan64 = f64::from_bits(0x7FF8_0000_0000_0001);
    let neg_nan64 = f64::from_bits(0xFFF8_0000_0000_0000);
    let long_z: &[u8] = b"zzzzzzzzzzzzzzzzzzzz";
    let mut carry = vec![b'a'];
    carry.extend_from_slice(&[0xFF; 17]);
    match class(t) {
        Class::Excluded => (vec![], vec![]),
        _ => match t {
            DbType::I8 => (
                vec![le![5i8, -3i8, 100i8], le![-128i8]],
                le![-4i8, -3i8, 5i8, 100i8, 101i8, -128i8, 127i8],
            ),
            DbType::I16 => (vec![le![258i16, -2i16], le![i16::MAX]], le![-3i16, -2i16, 0i16, 258i16, 259i16]),
            DbType::I32 => (
                vec![le![1i32, -1i32, i32::MIN], le![7i32, 7i32]],
                le![i32::MIN, 0i32, 1i32, 2i32, 7i32],
            ),
            DbType::I64 => (
                vec![le![0i64, -1i64, i64::MAX], le![42i64]],
                le![-2i64, -1i64, 0i64, 42i64, i64::MAX],
            ),
            DbType::I128 => (vec![le![1i128, -1i128]], le![-2i128, -1i128, 0i128, 1i128, 2i128]),
            DbType::U8 => (vec![le![0u8, 255u8], le![7u8]], le![0u8, 7u8, 8u8, 255u8]),
            DbType::U16 => (vec![le![0x0102u16, 0x8000u16]], le![0u16, 0x0102u16, 0x8000u16, 0xFFFFu16]),
            DbType::U32 => (
                vec![le![0x0102_0304u32, 0x8000_0000u32]],
                le![0u32, 0x0102_0304u32, 0x8000_0000u32, u32::MAX],
            ),
            DbType::U64 => (vec![le![1u64, u64::MAX], le![9u64]], le![0u64, 1u64, 9u64, 10u64, u64::MAX]),
            DbType::U128 => (vec![le![1u128, 1u128 << 127]], le![0u128, 1u128, 2u128, u128::MAX]),
            DbType::F32 => (
                vec![
                    f32s(&[1.5, f32::NAN, -0.0, 3.5]),
                    f32s(&[f32::NEG_INFINITY, -1.5]),
                    f32s(&[nan32]),
                    f32s(&[0.0, -0.0]),
                    f32s(&[neg_nan32, 2.0]),
                ],
                f32s(&[0.0, -0.0, 1.5, 3.5, 4.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -2.0]),
            ),
            DbType::F64 => (
                vec![
                    f64s(&[1.5, f64::NAN, -0.0, 3.5]),
                    f64s(&[f64::NEG_INFINITY, -1.5]),
                    f64s(&[nan64]),
                    f64s(&[0.0, -0.0]),
                    f64s(&[neg_nan64, 2.0]),
                ],
                f64s(&[0.0, -0.0, 1.5, 3.5, 4.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -2.0]),
            ),
            DbType::BOOL => (vec![vec![vec![0], vec![0]], vec![vec![0], vec![1]]], vec![vec![0], vec![1]]),
            DbType::CHAR => (
                vec![le!['A' as u32, 'é' as u32], le!['z' as u32]],
                le!['A' as u32, 'B' as u32, 'z' as u32, 'é' as u32, 0x10FFFFu32],
            ),
            DbType::STRING => (
                vec![
                    strs(&[b"mmm", b"b", long_z]),
                    vec![vec![0xFF; 20]],
                    vec![carry.clone()],
                    strs(&[b""]),
                    strs(&[b"exactly16bytes!!", b"exactly16bytes!!x"]),
                    strs(&[b"ab\0", b"ab"]),
                ],
                {
                    let mut p = strs(&[
                        b"",
                        b"a",
                        b"ab",
                        b"ab\0",
                        b"b",
                        b"b\0",
                        b"mmm",
                        b"zzzzzzzzzzzzzzzz",
                        b"zzzzzzzzzzzzzzzz{",
                        long_z,
                        b"exactly16bytes!!",
                        b"exactly16bytes!\"",
                    ]);
                    p.push(vec![0xFF; 17]);
                    p.push(carry.clone());
                    p
                },
            ),
            DbType::DATETIME => (
                vec![le![0i64, -1i64, 1_700_000_000_000_000i64]],
                le![-2i64, -1i64, 0i64, 1_700_000_000_000_000i64, i64::MAX],
            ),
            // Units of the one fixed DECIMAL scale, so values that read as
            // different scales in SQL (1.00, 0.0001, 1e18) are plain i128s here.
            DbType::DECIMAL => (
                vec![
                    le![100i128, -250i128, 10i128.pow(20)],
                    le![1i128, -1i128],
                ],
                le![-251i128, -250i128, 0i128, 1i128, 100i128, 10i128.pow(20), i128::MAX],
            ),
            DbType::UUID => (
                vec![vec![(0u8..16).collect(), vec![0xFF; 16]]],
                vec![vec![0; 16], (0u8..16).collect(), vec![0x80; 16], vec![0xFF; 16]],
            ),
            DbType::INET => (
                vec![
                    vec![inet(4, &[192, 168, 1, 7], 32), inet(4, &[10, 0, 0, 0], 8)],
                    vec![inet(6, &[0x20, 0x01, 0x0d, 0xb8], 32)],
                ],
                vec![
                    inet(4, &[10, 0, 0, 0], 8),
                    inet(4, &[10, 0, 0, 0], 16),
                    inet(4, &[192, 168, 1, 7], 32),
                    inet(4, &[192, 168, 1, 8], 32),
                    inet(6, &[0x20, 0x01, 0x0d, 0xb8], 32),
                ],
            ),
            other => panic!("{other:?} is indexed but has no zone vectors"),
        },
    }
}

/// Whether `t` stores its bounds at a fixed width, where a probe of another
/// width must keep the zone.
fn fixed_width(t: &DbType) -> Option<usize> {
    super::zonemap::ordered_width(t)
}

/// Plane words that stand for a persisted zone, built without `write_slot` so
/// the probe vectors pin the kernels' reading order on their own: frozen
/// noise, then each probe code placed at its own slot by the layout as
/// documented (bit `j` of slot `local` is bit `local % 64` of word
/// `j * ZONE_WORDS + local / 64`), so that wide codes match somewhere too.
fn frozen_planes(f: u8) -> Vec<u64> {
    let mut planes: Vec<u64> = (0..f as usize * ZONE_WORDS)
        .map(|i| {
            let x = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            x ^ (x >> 29) ^ (x << 17)
        })
        .collect();
    for (i, g) in probe_codes(f).into_iter().enumerate() {
        let local = 37 * i + 5;
        for j in 0..f as usize {
            let word = &mut planes[j * ZONE_WORDS + local / 64];
            if (g >> j) & 1 == 1 {
                *word |= 1 << (local % 64);
            } else {
                *word &= !(1 << (local % 64));
            }
        }
    }
    planes
}

/// The codes probed at width `f`: each of `PROBE_CODES` cut to `f` bits, once.
fn probe_codes(f: u8) -> Vec<u32> {
    let mut codes: Vec<u32> = Vec::new();
    for g in PROBE_CODES {
        let g = g & ((1u32 << f) - 1);
        if !codes.contains(&g) {
            codes.push(g);
        }
    }
    codes
}

const PROBE_CODES: [u32; 7] = [0, 1, 2, 5, 0xA5, 0x1234, 0xFFFF];
const PROBE_WIDTHS: [u8; 4] = [4, 5, 8, 16];

/// Every derived output, one per line: the lines of [`GOLDEN_V1`] in order,
/// with those of [`GOLDEN_ADDED`] among them.
fn render() -> String {
    let seed = HashSeed::default();
    let mut out = String::new();
    let mut line = |s: String| {
        out.push_str(&s);
        out.push('\n');
    };

    for t in &ALL {
        let c = class(t);
        if c == Class::Excluded {
            line(format!("excluded {t:?} zonemap={}", ZoneMap::new(t.clone()).is_some()));
            continue;
        }
        let (zones, probes) = zones_and_probes(t);

        if c == Class::Fingerprinted {
            let mut seen: Vec<Vec<u8>> = Vec::new();
            for v in zones.iter().flatten().chain(probes.iter()) {
                if seen.contains(v) {
                    continue;
                }
                seen.push(v.clone());
                match canonical_hash(t, v, seed) {
                    Some(h) => line(format!(
                        "hash {t:?} {} -> {h:016x} f4={:x} f8={:x} f16={:x}",
                        hex(v),
                        fingerprint(h, 4),
                        fingerprint(h, 8),
                        fingerprint(h, 16)
                    )),
                    None => line(format!("hash {t:?} {} -> none", hex(v))),
                }
            }
            if let Some(w) = fixed_width(t) {
                let wrong = vec![0u8; w + 1];
                line(format!(
                    "hash {t:?} {} -> {:?}",
                    hex(&wrong),
                    canonical_hash(t, &wrong, seed)
                ));
            }
            let first = &zones[0][0];
            let h = canonical_hash(t, first, seed).expect("a sample of the column's own width");
            let mut b = SuperBloom::new();
            b.add(3, h);
            line(format!("bloom {t:?} {} sz3 -> {}", hex(first), bloom_words(&b)));
        }

        let mut zm = ZoneMap::new(t.clone()).expect("an indexed type carries bounds");
        for (z, values) in zones.iter().enumerate() {
            for v in values {
                zm.observe(z as u64, v);
            }
        }
        let stride = zm.data().len() / zones.len();
        for (z, values) in zones.iter().enumerate() {
            let vs: Vec<String> = values.iter().map(|v| hex(v)).collect();
            line(format!(
                "seq {t:?} z{z} [{}] -> {}",
                vs.join(","),
                hex(&zm.data()[z * stride..(z + 1) * stride])
            ));
        }
        line(format!("valid {t:?} -> {:x?}", zm.valid_bits()));

        let mut all_probes = probes.clone();
        if let Some(w) = fixed_width(t) {
            all_probes.push(vec![0u8; w + 1]);
        }
        for z in 0..zones.len() {
            for p in &all_probes {
                let outcomes: Vec<String> = OPS
                    .iter()
                    .map(|op| format!("{}={}", op_name(*op), zm.zone_may_match(z as u64, *op, p) as u8))
                    .collect();
                line(format!("zone {t:?} z{z} {} -> {}", hex(p), outcomes.join(" ")));
            }
        }
    }

    // `successor`: the upper end of `LIKE 'p%'`, and `round_up`'s carry.
    for p in [&b"abc"[..], b"ab\xFF", b"a\xFF\xFF", b"\xFF\xFF", b"", b"\x00"] {
        line(format!(
            "successor {} -> {}",
            hex(p),
            successor(p).map_or("none".into(), |s| hex(&s))
        ));
    }

    // `NUMERIC(p, 2)`: a literal rescaled to the column's scale by sarg (the
    // query side), against a zone observed in that scale's units (the stored
    // side), and the same literals at the engine default scale. The two sides
    // have to move together: the default-scale probe for `amt > 50` is 50 ×
    // 10^9 and would prune every zone of a scale-2 column.
    {
        let dec: smallvec::SmallVec<[SchemaField; 20]> =
            smallvec::smallvec![SchemaField::new("amt".into(), DbType::DECIMAL, 16, 0, 0, false)];
        let filters = TableFilters::new(&dec, FilterConfig::default());
        let units: [i128; 3] = [125, 5000, -1];
        let mut zm = ZoneMap::new(DbType::DECIMAL).expect("DECIMAL carries bounds");
        for u in units {
            zm.observe(0, &u.to_le_bytes());
        }
        line(format!("numeric s2 z0 {units:?} -> {}", hex(zm.data())));
        for text in [
            "amt = 1.25",
            "amt > 50",
            "amt >= 50.00",
            "amt < -0.01",
            "amt <= 1.255",
            "amt = 0.001",
            "amt > 49.99",
        ] {
            let toks = crate::core::tokenizer::query_tokenizer::tokenize(text, &dec);
            for (label, scales) in [("s2", &[(0u32, 2u32)][..]), ("default", &[][..])] {
                let out = match extract_with_scales(&toks, &filters, scales).map(|p| p.root) {
                    Some(MaskExpr::Pred(p)) => format!(
                        "{} {} zone={}",
                        op_name(p.op),
                        hex(&p.value),
                        zm.zone_may_match(0, p.op, &p.value) as u8
                    ),
                    Some(other) => format!("{other:?}"),
                    None => "none".into(),
                };
                line(format!("numeric {label} `{text}` -> {out}"));
            }
        }
    }

    // The span summary, by raw hash.
    let hashes = [
        0u64,
        1,
        u64::MAX,
        0x0123_4567_89AB_CDEF,
        0xDEAD_BEEF_1234_5678,
        canonical_hash(&DbType::I64, &42i64.to_le_bytes(), seed).unwrap(),
    ];
    for h in hashes {
        for sz in [0u64, 3] {
            let mut b = SuperBloom::new();
            b.add(sz, h);
            line(format!("bloom {h:016x} sz{sz} -> {}", bloom_words(&b)));
        }
    }
    let mut b = SuperBloom::new();
    for (i, h) in hashes.iter().enumerate() {
        b.add((i % 2) as u64, *h);
    }
    for sz in [0u64, 1, 2] {
        let bits: String = hashes
            .iter()
            .chain([2u64, 0x5555_5555_5555_5555].iter())
            .map(|h| if b.may_contain(sz, *h) { '1' } else { '0' })
            .collect();
        line(format!("may_contain sz{sz} -> {bits}"));
    }

    // The plane layout `write_slot` produces.
    for f in PROBE_WIDTHS {
        let mask = (1u32 << f) - 1;
        let writes = [(0usize, CODE_FIRST), (1, 3), (63, mask), (64, 0xAAAA & mask), (200, 7), (511, mask - 1)];
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        for (local, code) in writes {
            write_slot(&mut planes, f, local, code);
        }
        line(format!("plane f{f} {writes:?} -> {}", nonzero_words64(&planes)));
    }

    // The candidates a persisted zone yields, read by the scalar kernel; every
    // other kernel is held to the same answers by the test.
    for f in PROBE_WIDTHS {
        let planes = frozen_planes(f);
        for g in probe_codes(f) {
            let mut bm = [0u64; ZONE_WORDS];
            probe_zone_scalar(&planes, f, g, &mut bm);
            line(format!("probe f{f} g{g:x} -> {}", nonzero_words64(&bm)));
        }
    }

    out
}

fn bloom_words(b: &SuperBloom) -> String {
    let w: Vec<String> = b
        .words()
        .iter()
        .enumerate()
        .filter(|(_, w)| **w != 0)
        .map(|(i, w)| format!("{i}:{w:08x}"))
        .collect();
    w.join(",")
}

fn nonzero_words64(words: &[u64]) -> String {
    let w: Vec<String> = words
        .iter()
        .enumerate()
        .filter(|(_, w)| **w != 0)
        .map(|(i, w)| format!("{i}:{w:016x}"))
        .collect();
    if w.is_empty() { "-".into() } else { w.join(",") }
}

/// FNV-1a, 64-bit: a digest of a golden block that needs nothing outside this
/// file.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// `(FILTER_DERIVE_VERSION, FORMAT_VER, OLDEST_READABLE_VER, fnv1a(frozen
/// block))`, one row per change to any of the three versions. Append-only and
/// never edited; the last row is the current one.
///
/// v4 sidecars are read under derive version 1: v4 is the NaN-order change,
/// and v5 changed only the layout. v6 (persistent indexes B14) records
/// `cleared_below` in the superblock: a layout change, so derive version 1
/// and v4 still readable.
const DERIVE_VERSIONS: &[(u32, u16, u16, u64)] = &[(1, 5, 4, 0x76d32637f5a8cc9a), (1, 6, 4, 0x76d32637f5a8cc9a)];

/// The current derive version's frozen block. See the module docs.
const FROZEN: (u32, &str) = (1, GOLDEN_V1);

/// What a derive bump must have done, given the rows before and after it.
fn bump_violation(before: (u32, u16, u16, u64), after: (u32, u16, u16, u64)) -> Option<String> {
    let (d0, f0, o0, g0) = before;
    let (d1, f1, o1, g1) = after;
    if d1 < d0 || f1 < f0 || o1 < o0 {
        return Some(format!("{before:x?} -> {after:x?}: a version went down"));
    }
    if d1 == d0 {
        if g1 != g0 {
            return Some(format!(
                "{before:x?} -> {after:x?}: the frozen block changed without a FILTER_DERIVE_VERSION bump"
            ));
        }
        return None;
    }
    // Every format at or below `f0` may hold a sidecar the old derivation
    // wrote. Only a format the reader refuses keeps them from being trusted.
    if f1 <= f0 || o1 != f1 {
        return Some(format!(
            "{before:x?} -> {after:x?}: a FILTER_DERIVE_VERSION bump must bump FORMAT_VER and raise \
             OLDEST_READABLE_VER to it, or every existing sidecar is still read"
        ));
    }
    None
}

#[test]
fn derived_outputs_match_the_golden_vectors() {
    let rendered = render();
    if std::env::var_os("RDB_PRINT_FILTER_VECTORS").is_some() {
        println!("----- RENDER -----\n{rendered}----- END (fnv1a {:#018x}) -----", fnv1a(rendered.as_bytes()));
    }
    let frozen: Vec<&str> = FROZEN.1.lines().collect();
    let added: Vec<&str> = GOLDEN_ADDED.lines().collect();
    let got: Vec<&str> = rendered.lines().collect();
    let gone: Vec<&&str> = frozen.iter().chain(added.iter()).filter(|l| !got.contains(l)).collect();
    let new: Vec<&&str> = got.iter().filter(|l| !frozen.contains(l) && !added.contains(l)).collect();
    assert!(
        gone.is_empty(),
        "{} golden filter vector(s) no longer hold. A persisted filter would be read \
         differently: bump FILTER_DERIVE_VERSION and FORMAT_VER, raise OLDEST_READABLE_VER \
         to FORMAT_VER, and freeze a new block (see the module docs). Regenerating the \
         current block or appending to GOLDEN_ADDED does not make this safe.\n\
         gone or changed:\n{}\nnow:\n{}",
        gone.len(),
        gone.iter().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n"),
        new.iter().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n"),
    );
    assert!(
        new.is_empty(),
        "new filter vectors in neither the frozen block nor GOLDEN_ADDED (add them to \
         GOLDEN_ADDED; no bump needed when nothing changed):\n{}",
        new.iter().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n"),
    );
    assert!(added.iter().all(|l| !frozen.contains(l)), "GOLDEN_ADDED repeats a frozen line");
    let in_order: Vec<&str> = got.iter().copied().filter(|l| frozen.contains(l)).collect();
    assert_eq!(in_order, frozen, "the frozen vectors are rendered in a different order");

    assert_eq!(FROZEN.0, FILTER_DERIVE_VERSION, "no frozen block for this FILTER_DERIVE_VERSION");
    assert_eq!(
        *DERIVE_VERSIONS.last().unwrap(),
        (FILTER_DERIVE_VERSION, FORMAT_VER, OLDEST_READABLE_VER, fnv1a(FROZEN.1.as_bytes())),
        "DERIVE_VERSIONS has no row for the current versions and frozen block: \
         append one (rows are never edited)"
    );
    for w in DERIVE_VERSIONS.windows(2) {
        if let Some(why) = bump_violation(w[0], w[1]) {
            panic!("{why}");
        }
    }
}

/// [`bump_violation`] itself: the history it accepts and refuses.
#[test]
fn a_derive_bump_must_reject_the_old_sidecars() {
    let v1 = (1, 5, 4, 0xAA);
    // A layout-only change, and a derive bump that rejects every v1 sidecar.
    assert_eq!(bump_violation(v1, (1, 6, 4, 0xAA)), None);
    assert_eq!(bump_violation(v1, (2, 6, 6, 0xBB)), None);
    // A bump the loader cannot see, one that leaves format 5 readable, and
    // one that leaves format 4 readable.
    assert!(bump_violation(v1, (2, 5, 4, 0xBB)).is_some());
    assert!(bump_violation(v1, (2, 6, 5, 0xBB)).is_some());
    assert!(bump_violation(v1, (2, 6, 4, 0xBB)).is_some());
    // The frozen block edited in place, and a version going down.
    assert!(bump_violation(v1, (1, 5, 4, 0xBB)).is_some());
    assert!(bump_violation(v1, (1, 4, 4, 0xAA)).is_some());
}

/// The probe vectors hold for every kernel this CPU runs, not only the
/// scalar one `render` uses.
#[test]
fn every_kernel_reads_persisted_planes_alike() {
    let kernels = runnable_kernels();
    assert!(kernels.iter().any(|(name, _)| *name == "scalar"));
    for f in PROBE_WIDTHS {
        let planes = frozen_planes(f);
        for g in probe_codes(f) {
            let mut want = [0u64; ZONE_WORDS];
            probe_zone_scalar(&planes, f, g, &mut want);
            for (name, kernel) in &kernels {
                let mut got = [0u64; ZONE_WORDS];
                kernel(&planes, f, g, &mut got);
                assert_eq!(got, want, "kernel {name} reads persisted planes differently at f={f} g={g:x}");
            }
        }
    }
}

#[test]
fn constants_are_pinned() {
    assert_eq!((CODE_EMPTY, CODE_NULL, CODE_FIRST), (0, 1, 2));
    let s = HashSeed::default();
    assert_eq!((s.k0, s.k1), (0x243F_6A88_85A3_08D3, 0x1319_8A2E_0370_7344));
    assert_eq!((ZONE_ROWS, ZONE_WORDS), (512, 8));
    assert_eq!((SUPERZONE_ZONES, SUPERZONE_ROWS, BLOCK_BITS, LANES), (32, 16_384, 4096, 8));
    assert_eq!(
        SALT,
        [
            0x47b6_137b,
            0x4497_4d91,
            0x8824_ad5b,
            0xa2b7_289d,
            0x7054_95c7,
            0x2df1_424b,
            0x9efc_4947,
            0x5c6b_fb31
        ]
    );
    assert_eq!(STRING_BOUND_PREFIX, 16);
}

#[test]
fn every_type_is_classified_and_covered() {
    let mut bytes: Vec<u8> = ALL.iter().map(DbType::to_byte).collect();
    bytes.sort_unstable();
    bytes.dedup();
    assert_eq!(bytes.len(), ALL.len(), "ALL lists a type twice");
    // `ALL` is a hand-kept list; the codec is what a schema is read with. Every
    // type it decodes has to be in `ALL`, or the checks below skip it.
    let decoded: Vec<u8> = (0..=255u8)
        .filter(|b| std::panic::catch_unwind(|| DbType::from_byte(*b)).is_ok())
        .collect();
    for b in &decoded {
        let t = DbType::from_byte(*b);
        assert_eq!(t.to_byte(), *b, "byte {b} decodes to {t:?}, which encodes differently");
        assert!(bytes.contains(b), "{t:?} (byte {b}) is not in ALL, so no vectors cover it");
    }
    assert_eq!(decoded, bytes, "ALL lists a type the codec does not decode");
    for (i, op) in OPS.iter().enumerate() {
        assert_eq!(op_in_ops(*op), *op, "OPS[{i}] is not where op_in_ops puts {op:?}");
    }
    let count = |c| ALL.iter().filter(|t| class(t) == c).count();
    assert_eq!(
        (count(Class::Fingerprinted), count(Class::BoundsOnly), count(Class::Excluded)),
        (18, 1, 31)
    );

    let rendered = render();
    let lines_for = |kind: &str, t: &DbType| -> Vec<String> {
        let prefix = format!("{kind} {t:?} ");
        rendered
            .lines()
            .filter(|l| l.starts_with(&prefix))
            .map(str::to_string)
            .collect()
    };

    for t in &ALL {
        let c = class(t);
        assert_eq!(is_indexable(t), c != Class::Excluded, "{t:?}: is_indexable disagrees");
        assert_eq!(is_fingerprintable(t), c == Class::Fingerprinted, "{t:?}: is_fingerprintable disagrees");
        assert_eq!(ZoneMap::new(t.clone()).is_some(), c != Class::Excluded, "{t:?}: bounds disagree");

        match c {
            Class::Excluded => {
                assert_eq!(lines_for("excluded", t), vec![format!("excluded {t:?} zonemap=false")]);
            }
            Class::Fingerprinted | Class::BoundsOnly => {
                if c == Class::Fingerprinted {
                    let hashes = lines_for("hash", t);
                    assert!(
                        hashes.iter().filter(|l| l.contains(" f4=") && l.contains(" f8=") && l.contains(" f16=")).count() >= 2,
                        "{t:?}: needs canonical_hash and fingerprint vectors"
                    );
                    assert_eq!(lines_for("bloom", t).len(), 1, "{t:?}: needs a bloom vector");
                } else {
                    assert!(lines_for("hash", t).is_empty(), "{t:?} is bounds-only and must not hash");
                }
                assert!(!lines_for("seq", t).is_empty(), "{t:?}: needs observe sequence vectors");
                let zone = lines_for("zone", t);
                for op in OPS {
                    assert!(
                        zone.iter().all(|l| l.contains(&format!(" {}=", op_name(op)))),
                        "{t:?}: every zone vector needs an outcome for {op:?}"
                    );
                }
                if let Some(w) = fixed_width(t) {
                    let wrong = format!(" {} -> eq=1 gt=1 ge=1 lt=1 le=1", hex(&vec![0u8; w + 1]));
                    assert!(
                        zone.iter().any(|l| l.ends_with(&wrong)),
                        "{t:?}: a width-mismatched probe must keep the zone"
                    );
                }
            }
        }
    }

    // STRING's truncation cases: a value longer than the prefix, the
    // all-0xFF prefix (no upper bound), and the carries.
    let seq = lines_for("seq", &DbType::STRING).join("\n");
    assert!(seq.contains(&hex(&[0xFF; 20])), "STRING: needs an all-0xFF value");
    assert!(seq.contains(&hex(b"zzzzzzzzzzzzzzzzzzzz")), "STRING: needs a value past the prefix");
    let mut carry = vec![b'a'];
    carry.extend_from_slice(&[0xFF; 17]);
    assert!(seq.contains(&hex(&carry)), "STRING: needs a round_up carry");
    assert!(rendered.contains("successor 61ffff -> 62"), "needs a successor carry");
}

/// A sidecar written for one classification is refused by a binary whose
/// classification differs, rather than read against the wrong column set.
#[test]
fn a_load_with_a_different_column_set_is_refused() {
    let fields = |types: &[DbType]| -> smallvec::SmallVec<[SchemaField; 20]> {
        types
            .iter()
            .enumerate()
            .map(|(i, t)| SchemaField::new(format!("c{i}"), t.clone(), 8, 0, i as u64, false))
            .collect()
    };
    let written = fields(&[DbType::I64, DbType::JSONB, DbType::BOOL]);
    let f = TableFilters::new(&written, FilterConfig::default());
    let bytes = f.checkpoint_bytes(0);
    let load = |types: &[DbType]| {
        TableFilters::from_checkpoint(&bytes, &fields(types), FilterConfig::default(), 0).err()
    };
    assert_eq!(load(&[DbType::I64, DbType::JSONB, DbType::BOOL]), None);
    // An excluded type the reader indexes, and an indexed type it excludes.
    assert_eq!(load(&[DbType::I64, DbType::I64, DbType::BOOL]), Some(LoadReject::SchemaMismatch));
    assert_eq!(load(&[DbType::I64, DbType::JSONB, DbType::JSONB]), Some(LoadReject::SchemaMismatch));
}

/// Frozen by `render` for `FILTER_DERIVE_VERSION` 1. Never edited; see the
/// module docs.
const GOLDEN_V1: &str = r#"hash I8 05 -> d96b8de0490c3ddb f4=2 f8=e0 f16=8de0
hash I8 fd -> 53105b5062dbcf85 f4=2 f8=50 f16=5b50
hash I8 64 -> aca6d39898a73106 f4=8 f8=98 f16=d398
hash I8 80 -> 50727a7919e18b7a f4=9 f8=79 f16=7a79
hash I8 fc -> 65f5406de9188159 f4=d f8=6d f16=406d
hash I8 65 -> da9ae663936d26f6 f4=3 f8=63 f16=e663
hash I8 7f -> fd544502b37c4559 f4=2 f8=2 f16=4502
hash I8 0000 -> None
bloom I8 05 sz3 -> 384:20000000,385:00040000,386:00000004,387:00000080,388:00000800,389:00000100,390:00020000,391:00010000
seq I8 z0 [05,fd,64] -> fd64
seq I8 z1 [80] -> 8080
valid I8 -> [3]
zone I8 z0 fc -> eq=0 gt=1 ge=1 lt=0 le=0
zone I8 z0 fd -> eq=1 gt=1 ge=1 lt=0 le=1
zone I8 z0 05 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I8 z0 64 -> eq=1 gt=0 ge=1 lt=1 le=1
zone I8 z0 65 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z0 80 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I8 z0 7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z0 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I8 z1 fc -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 fd -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 05 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 64 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 65 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 80 -> eq=1 gt=0 ge=1 lt=0 le=1
zone I8 z1 7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone I8 z1 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash I16 0201 -> 552a85d37d74ae39 f4=3 f8=d3 f16=85d3
hash I16 feff -> 093ba1b69c503d75 f4=6 f8=b6 f16=a1b6
hash I16 ff7f -> e940f780ca24b18f f4=2 f8=80 f16=f780
hash I16 fdff -> e67ca9ec32da758f f4=c f8=ec f16=a9ec
hash I16 0000 -> 2757a0cf58e37309 f4=f f8=cf f16=a0cf
hash I16 0301 -> d650c478c622cf3a f4=8 f8=78 f16=c478
hash I16 000000 -> None
bloom I16 0201 sz3 -> 416:00004000,417:00000010,418:00400000,419:00010000,420:04000000,421:80000000,422:00400000,423:00020000
seq I16 z0 [0201,feff] -> feff0201
seq I16 z1 [ff7f] -> ff7fff7f
valid I16 -> [3]
zone I16 z0 fdff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z0 feff -> eq=1 gt=1 ge=1 lt=0 le=1
zone I16 z0 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I16 z0 0201 -> eq=1 gt=0 ge=1 lt=1 le=1
zone I16 z0 0301 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I16 z0 000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I16 z1 fdff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z1 feff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z1 0000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z1 0201 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z1 0301 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I16 z1 000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash I32 01000000 -> a22608e89f14f807 f4=8 f8=e8 f16=8e8
hash I32 ffffffff -> b2709ca0982a3722 f4=2 f8=a0 f16=9ca0
hash I32 00000080 -> d0b5d12ccd1fe8ab f4=c f8=2c f16=d12c
hash I32 07000000 -> b5837140ba92b4ad f4=2 f8=40 f16=7140
hash I32 00000000 -> d1f879ab97f82dd2 f4=b f8=ab f16=79ab
hash I32 02000000 -> 989a2980ccea78c9 f4=2 f8=80 f16=2980
hash I32 0000000000 -> None
bloom I32 01000000 sz3 -> 408:10000000,409:00002000,410:00000200,411:04000000,412:04000000,413:00008000,414:00200000,415:00000001
seq I32 z0 [01000000,ffffffff,00000080] -> 0000008001000000
seq I32 z1 [07000000,07000000] -> 0700000007000000
valid I32 -> [3]
zone I32 z0 00000080 -> eq=1 gt=1 ge=1 lt=0 le=1
zone I32 z0 00000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I32 z0 01000000 -> eq=1 gt=0 ge=1 lt=1 le=1
zone I32 z0 02000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I32 z0 07000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I32 z0 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I32 z1 00000080 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I32 z1 00000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I32 z1 01000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I32 z1 02000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I32 z1 07000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone I32 z1 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash I64 0000000000000000 -> a08f0660ad68f506 f4=2 f8=60 f16=660
hash I64 ffffffffffffffff -> d5093a3a92a4f6b8 f4=a f8=3a f16=3a3a
hash I64 ffffffffffffff7f -> 2158157a36b57070 f4=a f8=7a f16=157a
hash I64 2a00000000000000 -> b1e1b637df8fff82 f4=7 f8=37 f16=b637
hash I64 feffffffffffffff -> 1da3beec614826cd f4=c f8=ec f16=beec
hash I64 000000000000000000 -> None
bloom I64 0000000000000000 sz3 -> 464:00010000,465:08000000,466:00000200,467:00200000,468:00000400,469:00100000,470:00000010,471:00004000
seq I64 z0 [0000000000000000,ffffffffffffffff,ffffffffffffff7f] -> ffffffffffffffffffffffffffffff7f
seq I64 z1 [2a00000000000000] -> 2a000000000000002a00000000000000
valid I64 -> [3]
zone I64 z0 feffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I64 z0 ffffffffffffffff -> eq=1 gt=1 ge=1 lt=0 le=1
zone I64 z0 0000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I64 z0 2a00000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I64 z0 ffffffffffffff7f -> eq=1 gt=0 ge=1 lt=1 le=1
zone I64 z0 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I64 z1 feffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I64 z1 ffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I64 z1 0000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone I64 z1 2a00000000000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone I64 z1 ffffffffffffff7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone I64 z1 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash I128 01000000000000000000000000000000 -> b91b90d5bba4a1ae f4=5 f8=d5 f16=90d5
hash I128 ffffffffffffffffffffffffffffffff -> a0a4fcffb974ebd9 f4=f f8=ff f16=fcff
hash I128 feffffffffffffffffffffffffffffff -> bec892f6968476fb f4=6 f8=f6 f16=92f6
hash I128 00000000000000000000000000000000 -> 1a05c1afe0665bab f4=f f8=af f16=c1af
hash I128 02000000000000000000000000000000 -> 69ac6d1f03052b0b f4=f f8=1f f16=6d1f
hash I128 0000000000000000000000000000000000 -> None
bloom I128 01000000000000000000000000000000 sz3 -> 496:00100000,497:00040000,498:00001000,499:00000001,500:40000000,501:04000000,502:01000000,503:00000100
seq I128 z0 [01000000000000000000000000000000,ffffffffffffffffffffffffffffffff] -> ffffffffffffffffffffffffffffffff01000000000000000000000000000000
valid I128 -> [1]
zone I128 z0 feffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone I128 z0 ffffffffffffffffffffffffffffffff -> eq=1 gt=1 ge=1 lt=0 le=1
zone I128 z0 00000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone I128 z0 01000000000000000000000000000000 -> eq=1 gt=0 ge=1 lt=1 le=1
zone I128 z0 02000000000000000000000000000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone I128 z0 0000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash U8 00 -> 46a0a0e7fe15c623 f4=7 f8=e7 f16=a0e7
hash U8 ff -> e2b329dff50f40de f4=f f8=df f16=29df
hash U8 07 -> 5148bd9610223462 f4=6 f8=96 f16=bd96
hash U8 08 -> 3e8c5e85f8ca2a54 f4=5 f8=85 f16=5e85
hash U8 0000 -> None
bloom U8 00 sz3 -> 464:00000004,465:00000010,466:40000000,467:02000000,468:00000002,469:04000000,470:00000400,471:00008000
seq U8 z0 [00,ff] -> 00ff
seq U8 z1 [07] -> 0707
valid U8 -> [3]
zone U8 z0 00 -> eq=1 gt=1 ge=1 lt=0 le=1
zone U8 z0 07 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U8 z0 08 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U8 z0 ff -> eq=1 gt=0 ge=1 lt=1 le=1
zone U8 z0 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U8 z1 00 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U8 z1 07 -> eq=1 gt=0 ge=1 lt=0 le=1
zone U8 z1 08 -> eq=0 gt=0 ge=0 lt=1 le=1
zone U8 z1 ff -> eq=0 gt=0 ge=0 lt=1 le=1
zone U8 z1 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash U16 0201 -> 552a85d37d74ae39 f4=3 f8=d3 f16=85d3
hash U16 0080 -> 54c0e6993ffbabc6 f4=9 f8=99 f16=e699
hash U16 0000 -> 2757a0cf58e37309 f4=f f8=cf f16=a0cf
hash U16 ffff -> 1b111cf882c47946 f4=8 f8=f8 f16=1cf8
hash U16 000000 -> None
bloom U16 0201 sz3 -> 416:00004000,417:00000010,418:00400000,419:00010000,420:04000000,421:80000000,422:00400000,423:00020000
seq U16 z0 [0201,0080] -> 02010080
valid U16 -> [1]
zone U16 z0 0000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U16 z0 0201 -> eq=1 gt=1 ge=1 lt=0 le=1
zone U16 z0 0080 -> eq=1 gt=0 ge=1 lt=1 le=1
zone U16 z0 ffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone U16 z0 000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash U32 04030201 -> f06d149dcfbebcdf f4=d f8=9d f16=149d
hash U32 00000080 -> d0b5d12ccd1fe8ab f4=c f8=2c f16=d12c
hash U32 00000000 -> d1f879ab97f82dd2 f4=b f8=ab f16=79ab
hash U32 ffffffff -> b2709ca0982a3722 f4=2 f8=a0 f16=9ca0
hash U32 0000000000 -> None
bloom U32 04030201 sz3 -> 400:00000400,401:00000200,402:08000000,403:00001000,404:00040000,405:08000000,406:10000000,407:01000000
seq U32 z0 [04030201,00000080] -> 0403020100000080
valid U32 -> [1]
zone U32 z0 00000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U32 z0 04030201 -> eq=1 gt=1 ge=1 lt=0 le=1
zone U32 z0 00000080 -> eq=1 gt=0 ge=1 lt=1 le=1
zone U32 z0 ffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone U32 z0 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash U64 0100000000000000 -> 97ee6f47833e4b2f f4=7 f8=47 f16=6f47
hash U64 ffffffffffffffff -> d5093a3a92a4f6b8 f4=a f8=3a f16=3a3a
hash U64 0900000000000000 -> c7ef3400d35d7ad2 f4=2 f8=2 f16=3400
hash U64 0000000000000000 -> a08f0660ad68f506 f4=2 f8=60 f16=660
hash U64 0a00000000000000 -> 3bd4936e77173d02 f4=e f8=6e f16=936e
hash U64 000000000000000000 -> None
bloom U64 0100000000000000 sz3 -> 448:00004000,449:20000000,450:00000040,451:01000000,452:00000040,453:04000000,454:00800000,455:00000002
seq U64 z0 [0100000000000000,ffffffffffffffff] -> 0100000000000000ffffffffffffffff
seq U64 z1 [0900000000000000] -> 09000000000000000900000000000000
valid U64 -> [3]
zone U64 z0 0000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U64 z0 0100000000000000 -> eq=1 gt=1 ge=1 lt=0 le=1
zone U64 z0 0900000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U64 z0 0a00000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U64 z0 ffffffffffffffff -> eq=1 gt=0 ge=1 lt=1 le=1
zone U64 z0 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U64 z1 0000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U64 z1 0100000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U64 z1 0900000000000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone U64 z1 0a00000000000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone U64 z1 ffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone U64 z1 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash U128 01000000000000000000000000000000 -> b91b90d5bba4a1ae f4=5 f8=d5 f16=90d5
hash U128 00000000000000000000000000000080 -> 4a687e72ccd310d4 f4=2 f8=72 f16=7e72
hash U128 00000000000000000000000000000000 -> 1a05c1afe0665bab f4=f f8=af f16=c1af
hash U128 02000000000000000000000000000000 -> 69ac6d1f03052b0b f4=f f8=1f f16=6d1f
hash U128 ffffffffffffffffffffffffffffffff -> a0a4fcffb974ebd9 f4=f f8=ff f16=fcff
hash U128 0000000000000000000000000000000000 -> None
bloom U128 01000000000000000000000000000000 sz3 -> 496:00100000,497:00040000,498:00001000,499:00000001,500:40000000,501:04000000,502:01000000,503:00000100
seq U128 z0 [01000000000000000000000000000000,00000000000000000000000000000080] -> 0100000000000000000000000000000000000000000000000000000000000080
valid U128 -> [1]
zone U128 z0 00000000000000000000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone U128 z0 01000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=0 le=1
zone U128 z0 02000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone U128 z0 ffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone U128 z0 0000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash F32 0000c03f -> 88d10a907874982e f4=2 f8=90 f16=a90
hash F32 0000c07f -> 26698bc6681bb99b f4=6 f8=c6 f16=8bc6
hash F32 00000080 -> 7acdbb98b1344213 f4=8 f8=98 f16=bb98
hash F32 00006040 -> feaa6884c9869c3c f4=4 f8=84 f16=6884
hash F32 000080ff -> 7a17f79d326bb161 f4=d f8=9d f16=f79d
hash F32 0000c0bf -> 824b73903d7e3a2f f4=2 f8=90 f16=7390
hash F32 0100c07f -> 26698bc6681bb99b f4=6 f8=c6 f16=8bc6
hash F32 00000000 -> 7acdbb98b1344213 f4=8 f8=98 f16=bb98
hash F32 0000c0ff -> 26698bc6681bb99b f4=6 f8=c6 f16=8bc6
hash F32 00000040 -> c4eb9e41b15d9379 f4=3 f8=41 f16=9e41
hash F32 00008040 -> 0a43305a66684e27 f4=a f8=5a f16=305a
hash F32 0000807f -> 9212ac0d5ca2ec88 f4=d f8=d f16=ac0d
hash F32 000000c0 -> 0d7860c0c7b176ba f4=2 f8=c0 f16=60c0
hash F32 0000000000 -> None
bloom F32 0000c03f sz3 -> 384:00010000,385:00000010,386:00000100,387:00200000,388:00008000,389:00000400,390:02000000,391:01000000
seq F32 z0 [0000c03f,0000c07f,00000080,00006040] -> 000000800000c07f
seq F32 z1 [000080ff,0000c0bf] -> 000080ff0000c0bf
seq F32 z2 [0100c07f] -> 0100c07f0100c07f
seq F32 z3 [00000000,00000080] -> 0000000000000000
seq F32 z4 [0000c0ff,00000040] -> 000000400000c0ff
valid F32 -> [1f]
zone F32 z0 00000000 -> eq=1 gt=1 ge=1 lt=0 le=1
zone F32 z0 00000080 -> eq=1 gt=1 ge=1 lt=0 le=1
zone F32 z0 0000c03f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z0 00006040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z0 00008040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z0 0000c07f -> eq=1 gt=0 ge=1 lt=1 le=1
zone F32 z0 0000807f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z0 000080ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z0 000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z0 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z1 00000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 00000080 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 0000c03f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 00006040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 00008040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 0000c07f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 0000807f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z1 000080ff -> eq=1 gt=1 ge=1 lt=0 le=1
zone F32 z1 000000c0 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z1 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z2 00000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 00000080 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 0000c03f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 00006040 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 00008040 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 0000c07f -> eq=1 gt=0 ge=1 lt=0 le=1
zone F32 z2 0000807f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 000080ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z2 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z3 00000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone F32 z3 00000080 -> eq=1 gt=0 ge=1 lt=0 le=1
zone F32 z3 0000c03f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z3 00006040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z3 00008040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z3 0000c07f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z3 0000807f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F32 z3 000080ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z3 000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z3 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z4 00000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z4 00000080 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z4 0000c03f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z4 00006040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z4 00008040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z4 0000c07f -> eq=1 gt=0 ge=1 lt=1 le=1
zone F32 z4 0000807f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F32 z4 000080ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z4 000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F32 z4 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash F64 000000000000f83f -> b1729445fd16d0e5 f4=5 f8=45 f16=9445
hash F64 000000000000f87f -> 7e920b5d0207b2f6 f4=d f8=5d f16=b5d
hash F64 0000000000000080 -> 7acdbb98b1344213 f4=8 f8=98 f16=bb98
hash F64 0000000000000c40 -> 6f16f2c6d5193d29 f4=6 f8=c6 f16=f2c6
hash F64 000000000000f0ff -> 5b8a7bcb8d873d1c f4=b f8=cb f16=7bcb
hash F64 000000000000f8bf -> 0b30a47a86af057c f4=a f8=7a f16=a47a
hash F64 010000000000f87f -> 7e920b5d0207b2f6 f4=d f8=5d f16=b5d
hash F64 0000000000000000 -> 7acdbb98b1344213 f4=8 f8=98 f16=bb98
hash F64 000000000000f8ff -> 7e920b5d0207b2f6 f4=d f8=5d f16=b5d
hash F64 0000000000000040 -> 05ebd7142f7f3aba f4=4 f8=14 f16=d714
hash F64 0000000000001040 -> f7de82b3f90dad13 f4=3 f8=b3 f16=82b3
hash F64 000000000000f07f -> 5c8f5a98ebca3d33 f4=8 f8=98 f16=5a98
hash F64 00000000000000c0 -> 8276011fffd25255 f4=f f8=1f f16=11f
hash F64 000000000000000000 -> None
bloom F64 000000000000f83f sz3 -> 408:02000000,409:00020000,410:01000000,411:00004000,412:00000400,413:00002000,414:00010000,415:00040000
seq F64 z0 [000000000000f83f,000000000000f87f,0000000000000080,0000000000000c40] -> 0000000000000080000000000000f87f
seq F64 z1 [000000000000f0ff,000000000000f8bf] -> 000000000000f0ff000000000000f8bf
seq F64 z2 [010000000000f87f] -> 010000000000f87f010000000000f87f
seq F64 z3 [0000000000000000,0000000000000080] -> 00000000000000000000000000000000
seq F64 z4 [000000000000f8ff,0000000000000040] -> 0000000000000040000000000000f8ff
valid F64 -> [1f]
zone F64 z0 0000000000000000 -> eq=1 gt=1 ge=1 lt=0 le=1
zone F64 z0 0000000000000080 -> eq=1 gt=1 ge=1 lt=0 le=1
zone F64 z0 000000000000f83f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z0 0000000000000c40 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z0 0000000000001040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z0 000000000000f87f -> eq=1 gt=0 ge=1 lt=1 le=1
zone F64 z0 000000000000f07f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z0 000000000000f0ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z0 00000000000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z0 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z1 0000000000000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 0000000000000080 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 000000000000f83f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 0000000000000c40 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 0000000000001040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 000000000000f87f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 000000000000f07f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z1 000000000000f0ff -> eq=1 gt=1 ge=1 lt=0 le=1
zone F64 z1 00000000000000c0 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z1 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z2 0000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 0000000000000080 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 000000000000f83f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 0000000000000c40 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 0000000000001040 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 000000000000f87f -> eq=1 gt=0 ge=1 lt=0 le=1
zone F64 z2 000000000000f07f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 000000000000f0ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 00000000000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z2 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z3 0000000000000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone F64 z3 0000000000000080 -> eq=1 gt=0 ge=1 lt=0 le=1
zone F64 z3 000000000000f83f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z3 0000000000000c40 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z3 0000000000001040 -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z3 000000000000f87f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z3 000000000000f07f -> eq=0 gt=0 ge=0 lt=1 le=1
zone F64 z3 000000000000f0ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z3 00000000000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z3 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z4 0000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z4 0000000000000080 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z4 000000000000f83f -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z4 0000000000000c40 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z4 0000000000001040 -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z4 000000000000f87f -> eq=1 gt=0 ge=1 lt=1 le=1
zone F64 z4 000000000000f07f -> eq=1 gt=1 ge=1 lt=1 le=1
zone F64 z4 000000000000f0ff -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z4 00000000000000c0 -> eq=0 gt=1 ge=1 lt=0 le=0
zone F64 z4 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
seq BOOL z0 [00,00] -> 0000
seq BOOL z1 [00,01] -> 0001
valid BOOL -> [3]
zone BOOL z0 00 -> eq=1 gt=0 ge=1 lt=0 le=1
zone BOOL z0 01 -> eq=0 gt=0 ge=0 lt=1 le=1
zone BOOL z0 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone BOOL z1 00 -> eq=1 gt=1 ge=1 lt=0 le=1
zone BOOL z1 01 -> eq=1 gt=0 ge=1 lt=1 le=1
zone BOOL z1 0000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash CHAR 41000000 -> 18e9c659ef3f457c f4=9 f8=59 f16=c659
hash CHAR e9000000 -> 91c6408969b64529 f4=9 f8=89 f16=4089
hash CHAR 7a000000 -> 4d9bb57a8c118ef1 f4=a f8=7a f16=b57a
hash CHAR 42000000 -> bf7b1b802f037a10 f4=2 f8=80 f16=1b80
hash CHAR ffff1000 -> 02845cf876122dad f4=8 f8=f8 f16=5cf8
hash CHAR 0000000000 -> None
bloom CHAR 41000000 sz3 -> 440:01000000,441:80000000,442:00400000,443:20000000,444:40000000,445:00000800,446:10000000,447:00000004
seq CHAR z0 [41000000,e9000000] -> 41000000e9000000
seq CHAR z1 [7a000000] -> 7a0000007a000000
valid CHAR -> [3]
zone CHAR z0 41000000 -> eq=1 gt=1 ge=1 lt=0 le=1
zone CHAR z0 42000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone CHAR z0 7a000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone CHAR z0 e9000000 -> eq=1 gt=0 ge=1 lt=1 le=1
zone CHAR z0 ffff1000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone CHAR z0 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone CHAR z1 41000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone CHAR z1 42000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone CHAR z1 7a000000 -> eq=1 gt=0 ge=1 lt=0 le=1
zone CHAR z1 e9000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone CHAR z1 ffff1000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone CHAR z1 0000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash STRING 6d6d6d -> 2fd3f7910ff232b2 f4=3 f8=91 f16=f791
hash STRING 62 -> 60f8eebb89b2ab17 f4=b f8=bb f16=eebb
hash STRING 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> 549341dbe5ea0794 f4=b f8=db f16=41db
hash STRING ffffffffffffffffffffffffffffffffffffffff -> 9b25eeb685ba70e0 f4=6 f8=b6 f16=eeb6
hash STRING 61ffffffffffffffffffffffffffffffffff -> 40ae94d6a2999059 f4=6 f8=d6 f16=94d6
hash STRING - -> 7acdbb98b1344213 f4=8 f8=98 f16=bb98
hash STRING 65786163746c79313662797465732121 -> 4ac4fa1cfe032803 f4=c f8=1c f16=fa1c
hash STRING 65786163746c7931366279746573212178 -> 12a73e84eded0227 f4=4 f8=84 f16=3e84
hash STRING 616200 -> ae74cccf4bf33eb2 f4=f f8=cf f16=cccf
hash STRING 6162 -> a7eca2898d7670dd f4=9 f8=89 f16=a289
hash STRING 61 -> c61d2c9917c2c041 f4=9 f8=99 f16=2c99
hash STRING 6200 -> f2c2ed6e45652cb6 f4=e f8=6e f16=ed6e
hash STRING 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> 74a4a23420632f41 f4=4 f8=34 f16=a234
hash STRING 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> 5d236ff72ecdcff1 f4=7 f8=f7 f16=6ff7
hash STRING 65786163746c79313662797465732122 -> 33b4b6c3565f3422 f4=3 f8=c3 f16=b6c3
hash STRING ffffffffffffffffffffffffffffffffff -> b4dc47d44fe4198a f4=4 f8=d4 f16=47d4
bloom STRING 6d6d6d sz3 -> 432:40000000,433:00000001,434:00010000,435:20000000,436:00000200,437:00000020,438:00010000,439:00100000
seq STRING z0 [6d6d6d,62,7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a] -> 0162000000000000000000000000000000107a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b
seq STRING z1 [ffffffffffffffffffffffffffffffffffffffff] -> 10ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
seq STRING z2 [61ffffffffffffffffffffffffffffffffff] -> 1061ffffffffffffffffffffffffffffff1062000000000000000000000000000000
seq STRING z3 [-] -> 00000000000000000000000000000000000000000000000000000000000000000000
seq STRING z4 [65786163746c79313662797465732121,65786163746c7931366279746573212178] -> 1065786163746c793136627974657321211065786163746c79313662797465732122
seq STRING z5 [616200,6162] -> 02616200000000000000000000000000000361620000000000000000000000000000
valid STRING -> [3f]
zone STRING z0 - -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z0 61 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z0 6162 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z0 616200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z0 62 -> eq=1 gt=1 ge=1 lt=0 le=1
zone STRING z0 6200 -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 6d6d6d -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 65786163746c79313662797465732121 -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 65786163746c79313662797465732122 -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z0 ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z0 61ffffffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 - -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 61 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 6162 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 616200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 62 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 6200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 6d6d6d -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 65786163746c79313662797465732121 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 65786163746c79313662797465732122 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z1 ffffffffffffffffffffffffffffffffff -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z1 61ffffffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z2 - -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z2 61 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z2 6162 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z2 616200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z2 62 -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z2 6200 -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z2 6d6d6d -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 65786163746c79313662797465732121 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 65786163746c79313662797465732122 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z2 61ffffffffffffffffffffffffffffffffff -> eq=1 gt=1 ge=1 lt=1 le=1
zone STRING z3 - -> eq=1 gt=0 ge=1 lt=0 le=1
zone STRING z3 61 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 6162 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 616200 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 62 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 6200 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 6d6d6d -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 65786163746c79313662797465732121 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 65786163746c79313662797465732122 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z3 61ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 - -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 61 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 6162 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 616200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 62 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 6200 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z4 6d6d6d -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 65786163746c79313662797465732121 -> eq=1 gt=1 ge=1 lt=0 le=1
zone STRING z4 65786163746c79313662797465732122 -> eq=1 gt=0 ge=1 lt=1 le=1
zone STRING z4 ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z4 61ffffffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z5 - -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z5 61 -> eq=0 gt=1 ge=1 lt=0 le=0
zone STRING z5 6162 -> eq=1 gt=1 ge=1 lt=0 le=1
zone STRING z5 616200 -> eq=1 gt=0 ge=1 lt=1 le=1
zone STRING z5 62 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 6200 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 6d6d6d -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7b -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 65786163746c79313662797465732121 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 65786163746c79313662797465732122 -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
zone STRING z5 61ffffffffffffffffffffffffffffffffff -> eq=0 gt=0 ge=0 lt=1 le=1
hash DATETIME 0000000000000000 -> a08f0660ad68f506 f4=2 f8=60 f16=660
hash DATETIME ffffffffffffffff -> d5093a3a92a4f6b8 f4=a f8=3a f16=3a3a
hash DATETIME 00401e18240a0600 -> 5b845ac8ec3c0c68 f4=8 f8=c8 f16=5ac8
hash DATETIME feffffffffffffff -> 1da3beec614826cd f4=c f8=ec f16=beec
hash DATETIME ffffffffffffff7f -> 2158157a36b57070 f4=a f8=7a f16=157a
hash DATETIME 000000000000000000 -> None
bloom DATETIME 0000000000000000 sz3 -> 464:00010000,465:08000000,466:00000200,467:00200000,468:00000400,469:00100000,470:00000010,471:00004000
seq DATETIME z0 [0000000000000000,ffffffffffffffff,00401e18240a0600] -> ffffffffffffffff00401e18240a0600
valid DATETIME -> [1]
zone DATETIME z0 feffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone DATETIME z0 ffffffffffffffff -> eq=1 gt=1 ge=1 lt=0 le=1
zone DATETIME z0 0000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DATETIME z0 00401e18240a0600 -> eq=1 gt=0 ge=1 lt=1 le=1
zone DATETIME z0 ffffffffffffff7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone DATETIME z0 000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash UUID 000102030405060708090a0b0c0d0e0f -> 5985e9a821ff5811 f4=8 f8=a8 f16=e9a8
hash UUID ffffffffffffffffffffffffffffffff -> a0a4fcffb974ebd9 f4=f f8=ff f16=fcff
hash UUID 00000000000000000000000000000000 -> 1a05c1afe0665bab f4=f f8=af f16=c1af
hash UUID 80808080808080808080808080808080 -> 11794e0701ce6f53 f4=7 f8=7 f16=4e07
hash UUID 0000000000000000000000000000000000 -> None
bloom UUID 000102030405060708090a0b0c0d0e0f sz3 -> 448:00400000,449:00100000,450:20000000,451:40000000,452:00010000,453:00000100,454:80000000,455:04000000
seq UUID z0 [000102030405060708090a0b0c0d0e0f,ffffffffffffffffffffffffffffffff] -> 000102030405060708090a0b0c0d0e0fffffffffffffffffffffffffffffffff
valid UUID -> [1]
zone UUID z0 00000000000000000000000000000000 -> eq=0 gt=1 ge=1 lt=0 le=0
zone UUID z0 000102030405060708090a0b0c0d0e0f -> eq=1 gt=1 ge=1 lt=0 le=1
zone UUID z0 80808080808080808080808080808080 -> eq=1 gt=1 ge=1 lt=1 le=1
zone UUID z0 ffffffffffffffffffffffffffffffff -> eq=1 gt=0 ge=1 lt=1 le=1
zone UUID z0 0000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
hash INET 04000000000000000000000000c0a8010720 -> b048347f7323b1c7 f4=f f8=7f f16=347f
hash INET 040000000000000000000000000a00000008 -> a8c25ce7bced955f f4=7 f8=e7 f16=5ce7
hash INET 0600000000000000000000000020010db820 -> 2eff1b9ae609038c f4=a f8=9a f16=1b9a
hash INET 040000000000000000000000000a00000010 -> 28b6c183191c24a2 f4=3 f8=83 f16=c183
hash INET 04000000000000000000000000c0a8010820 -> e798ee7b69e44818 f4=b f8=7b f16=ee7b
hash INET 00000000000000000000000000000000000000 -> None
bloom INET 04000000000000000000000000c0a8010720 sz3 -> 392:00000040,393:00001000,394:04000000,395:00002000,396:00000040,397:00000100,398:00000100,399:04000000
seq INET z0 [04000000000000000000000000c0a8010720,040000000000000000000000000a00000008] -> 040000000000000000000000000a0000000804000000000000000000000000c0a8010720
seq INET z1 [0600000000000000000000000020010db820] -> 0600000000000000000000000020010db8200600000000000000000000000020010db820
valid INET -> [3]
zone INET z0 040000000000000000000000000a00000008 -> eq=1 gt=1 ge=1 lt=0 le=1
zone INET z0 040000000000000000000000000a00000010 -> eq=1 gt=1 ge=1 lt=1 le=1
zone INET z0 04000000000000000000000000c0a8010720 -> eq=1 gt=0 ge=1 lt=1 le=1
zone INET z0 04000000000000000000000000c0a8010820 -> eq=0 gt=0 ge=0 lt=1 le=1
zone INET z0 0600000000000000000000000020010db820 -> eq=0 gt=0 ge=0 lt=1 le=1
zone INET z0 00000000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone INET z1 040000000000000000000000000a00000008 -> eq=0 gt=1 ge=1 lt=0 le=0
zone INET z1 040000000000000000000000000a00000010 -> eq=0 gt=1 ge=1 lt=0 le=0
zone INET z1 04000000000000000000000000c0a8010720 -> eq=0 gt=1 ge=1 lt=0 le=0
zone INET z1 04000000000000000000000000c0a8010820 -> eq=0 gt=1 ge=1 lt=0 le=0
zone INET z1 0600000000000000000000000020010db820 -> eq=1 gt=0 ge=1 lt=0 le=1
zone INET z1 00000000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
excluded BYTEA zonemap=false
excluded ARRAY zonemap=false
hash DECIMAL 64000000000000000000000000000000 -> 102c59ecc6a100a5 f4=c f8=ec f16=59ec
hash DECIMAL 06ffffffffffffffffffffffffffffff -> 5f937750abc0e7c7 f4=2 f8=50 f16=7750
hash DECIMAL 000010632d5ec76b0500000000000000 -> b61dc0507bcb13b2 f4=2 f8=50 f16=c050
hash DECIMAL 01000000000000000000000000000000 -> b91b90d5bba4a1ae f4=5 f8=d5 f16=90d5
hash DECIMAL ffffffffffffffffffffffffffffffff -> a0a4fcffb974ebd9 f4=f f8=ff f16=fcff
hash DECIMAL 05ffffffffffffffffffffffffffffff -> 8e30ba5991784113 f4=9 f8=59 f16=ba59
hash DECIMAL 00000000000000000000000000000000 -> 1a05c1afe0665bab f4=f f8=af f16=c1af
hash DECIMAL ffffffffffffffffffffffffffffff7f -> 0b3eff312749c451 f4=3 f8=31 f16=ff31
hash DECIMAL 0000000000000000000000000000000000 -> None
bloom DECIMAL 64000000000000000000000000000000 sz3 -> 432:00000010,433:00000400,434:00400000,435:20000000,436:00001000,437:00000400,438:00800000,439:00080000
seq DECIMAL z0 [64000000000000000000000000000000,06ffffffffffffffffffffffffffffff,000010632d5ec76b0500000000000000] -> 06ffffffffffffffffffffffffffffff000010632d5ec76b0500000000000000
seq DECIMAL z1 [01000000000000000000000000000000,ffffffffffffffffffffffffffffffff] -> ffffffffffffffffffffffffffffffff01000000000000000000000000000000
valid DECIMAL -> [3]
zone DECIMAL z0 05ffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone DECIMAL z0 06ffffffffffffffffffffffffffffff -> eq=1 gt=1 ge=1 lt=0 le=1
zone DECIMAL z0 00000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DECIMAL z0 01000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DECIMAL z0 64000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DECIMAL z0 000010632d5ec76b0500000000000000 -> eq=1 gt=0 ge=1 lt=1 le=1
zone DECIMAL z0 ffffffffffffffffffffffffffffff7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone DECIMAL z0 0000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DECIMAL z1 05ffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone DECIMAL z1 06ffffffffffffffffffffffffffffff -> eq=0 gt=1 ge=1 lt=0 le=0
zone DECIMAL z1 00000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
zone DECIMAL z1 01000000000000000000000000000000 -> eq=1 gt=0 ge=1 lt=1 le=1
zone DECIMAL z1 64000000000000000000000000000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone DECIMAL z1 000010632d5ec76b0500000000000000 -> eq=0 gt=0 ge=0 lt=1 le=1
zone DECIMAL z1 ffffffffffffffffffffffffffffff7f -> eq=0 gt=0 ge=0 lt=1 le=1
zone DECIMAL z1 0000000000000000000000000000000000 -> eq=1 gt=1 ge=1 lt=1 le=1
excluded JSON zonemap=false
excluded JSONB zonemap=false
excluded INTERVAL zonemap=false
excluded MACADDR zonemap=false
excluded MACADDR8 zonemap=false
excluded BIT zonemap=false
excluded VARBIT zonemap=false
excluded DATE zonemap=false
excluded TIME zonemap=false
excluded TIMETZ zonemap=false
excluded POINT zonemap=false
excluded LINE zonemap=false
excluded LSEG zonemap=false
excluded BOX zonemap=false
excluded PATH zonemap=false
excluded POLYGON zonemap=false
excluded CIRCLE zonemap=false
excluded INT4RANGE zonemap=false
excluded INT8RANGE zonemap=false
excluded NUMRANGE zonemap=false
excluded TSRANGE zonemap=false
excluded TSTZRANGE zonemap=false
excluded DATERANGE zonemap=false
excluded UNKNOWN zonemap=false
excluded NULL zonemap=false
excluded START zonemap=false
excluded END zonemap=false
successor 616263 -> 616264
successor 6162ff -> 616300
successor 61ffff -> 620000
successor ffff -> none
successor - -> none
successor 00 -> 01
bloom 0000000000000000 sz0 -> 72:00000010,73:00010000,74:02000000,75:20000000,76:00000008,77:00000010,78:00000008,79:00100000
bloom 0000000000000000 sz3 -> 456:00000010,457:00010000,458:02000000,459:20000000,460:00000008,461:00000010,462:00000008,463:00100000
bloom 0000000000000001 sz0 -> 56:20000000,57:00008000,58:00000400,59:00001000,60:00200000,61:00000100,62:00100000,63:00001000
bloom 0000000000000001 sz3 -> 440:20000000,441:00008000,442:00000400,443:00001000,444:00200000,445:00000100,446:00100000,447:00001000
bloom ffffffffffffffff sz0 -> 96:00000400,97:01000000,98:00200000,99:00020000,100:00020000,101:00040000,102:00000001,103:00000400
bloom ffffffffffffffff sz3 -> 480:00000400,481:01000000,482:00200000,483:00020000,484:00020000,485:00040000,486:00000001,487:00000400
bloom 0123456789abcdef sz0 -> 16:00001000,17:00000040,18:00010000,19:20000000,20:00800000,21:00080000,22:00000800,23:00001000
bloom 0123456789abcdef sz3 -> 400:00001000,401:00000040,402:00010000,403:20000000,404:00800000,405:00080000,406:00000800,407:00001000
bloom deadbeef12345678 sz0 -> 56:08000000,57:00002000,58:00000020,59:00400000,60:00000040,61:00000004,62:80000000,63:00000400
bloom deadbeef12345678 sz3 -> 440:08000000,441:00002000,442:00000020,443:00400000,444:00000040,445:00000004,446:80000000,447:00000400
bloom b1e1b637df8fff82 sz0 -> 8:00000400,9:00000008,10:02000000,11:20000000,12:00040000,13:00040000,14:00000400,15:00400000
bloom b1e1b637df8fff82 sz3 -> 392:00000400,393:00000008,394:02000000,395:20000000,396:00040000,397:00040000,398:00000400,399:00400000
may_contain sz0 -> 10101000
may_contain sz1 -> 01010100
may_contain sz2 -> 11111111
plane f4 [(0, 2), (1, 3), (63, 15), (64, 10), (200, 7), (511, 14)] -> 0:8000000000000002,3:0000000000000100,8:8000000000000003,9:0000000000000001,11:0000000000000100,15:8000000000000000,16:8000000000000000,19:0000000000000100,23:8000000000000000,24:8000000000000000,25:0000000000000001,31:8000000000000000
plane f5 [(0, 2), (1, 3), (63, 31), (64, 10), (200, 7), (511, 30)] -> 0:8000000000000002,3:0000000000000100,8:8000000000000003,9:0000000000000001,11:0000000000000100,15:8000000000000000,16:8000000000000000,19:0000000000000100,23:8000000000000000,24:8000000000000000,25:0000000000000001,31:8000000000000000,32:8000000000000000,39:8000000000000000
plane f8 [(0, 2), (1, 3), (63, 255), (64, 170), (200, 7), (511, 254)] -> 0:8000000000000002,3:0000000000000100,8:8000000000000003,9:0000000000000001,11:0000000000000100,15:8000000000000000,16:8000000000000000,19:0000000000000100,23:8000000000000000,24:8000000000000000,25:0000000000000001,31:8000000000000000,32:8000000000000000,39:8000000000000000,40:8000000000000000,41:0000000000000001,47:8000000000000000,48:8000000000000000,55:8000000000000000,56:8000000000000000,57:0000000000000001,63:8000000000000000
plane f16 [(0, 2), (1, 3), (63, 65535), (64, 43690), (200, 7), (511, 65534)] -> 0:8000000000000002,3:0000000000000100,8:8000000000000003,9:0000000000000001,11:0000000000000100,15:8000000000000000,16:8000000000000000,19:0000000000000100,23:8000000000000000,24:8000000000000000,25:0000000000000001,31:8000000000000000,32:8000000000000000,39:8000000000000000,40:8000000000000000,41:0000000000000001,47:8000000000000000,48:8000000000000000,55:8000000000000000,56:8000000000000000,57:0000000000000001,63:8000000000000000,64:8000000000000000,71:8000000000000000,72:8000000000000000,73:0000000000000001,79:8000000000000000,80:8000000000000000,87:8000000000000000,88:8000000000000000,89:0000000000000001,95:8000000000000000,96:8000000000000000,103:8000000000000000,104:8000000000000000,105:0000000000000001,111:8000000000000000,112:8000000000000000,119:8000000000000000,120:8000000000000000,121:0000000000000001,127:8000000000000000
probe f4 g0 -> 0:0002004000000821,1:0000000000000042,2:0000000000000003,3:0001000204800884,4:0110200000000105,5:0002000200004006,6:0000040000500007,7:0003000000000000
probe f4 g1 -> 0:4000040000800000,1:5200000081010200,2:0000100000000c40,3:0000042001000000,4:8000000000000080,5:0000280000202020,6:0000000008010000,7:0000080000000000
probe f4 g2 -> 0:00a0000400244000,1:0400100000008000,2:1000000000040000,3:0040020000000000,4:0000410000020000,5:8000400010020000,6:0400000040000240,7:1000020000000000
probe f4 g5 -> 0:0804000000009108,1:0090080000200008,2:0004000440001008,3:1004009440008208,4:0220000000080000,5:0004012001000008,6:a900000000000000,7:0204000000008000
probe f4 g4 -> 0:0200400000000000,1:0120400002000000,2:0400204003004000,3:0000210000000000,4:0080000040008000,5:0000001000800040,6:0000108000001000,7:0100000000004000
probe f4 gf -> 0:0000000000000086,1:0003004200004005,2:4090000000000004,3:0002000000000003,4:0002000000000802,5:0101040000000001,6:0000000000084000,7:00002002001008c7
probe f5 g0 -> 0:0002000000000021,1:0000000000000002,2:0000000000000003,3:0001000000000884,4:0000200000000005,5:0002000000000006,6:0000040000500007,7:0003000000000000
probe f5 g1 -> 0:4000040000000000,1:0000000081000000,2:0000100000000840,3:0000040000000000,4:0000000000000080,5:0000200000200020,6:0000000000010000,7:0000080000000000
probe f5 g2 -> 0:0000000400200000,1:0000100000008000,2:1000000000000000,4:0000010000020000,5:8000000000000000,6:0000000000000240
probe f5 g5 -> 0:0800000000008000,1:0090080000200000,2:0000000040000000,3:1000001000000000,4:0220000000080000,5:0000002001000000,6:a100000000000000,7:0200000000008000
probe f5 g14 -> 0:0000400000000000,1:0000400000000000,2:0000200002004000,4:0080000040000000,5:0000001000800040,6:0000008000001000,7:0100000000004000
probe f5 g1f -> 0:0000000000000086,1:0003004200000005,2:4000000000000004,3:0002000000000003,4:0002000000000002,5:0101000000000001,7:0000000000000087
probe f8 g0 -> 0:0002000000000020,1:0000000000000002,2:0000000000000002,3:0000000000000004,4:0000000000000004,5:0000000000000006,6:0000000000000006,7:0002000000000000
probe f8 g1 -> 0:0000040000000000,2:0000000000000800
probe f8 g2 -> 1:0000000000008000
probe f8 g5 -> 1:0010000000000000,6:0100000000000000
probe f8 ga5 -> 0:0800000000000000,2:0000000042000000,3:0000001000000000,5:0000000001000000
probe f8 g34 -> 2:4000000000000000
probe f8 gff -> 0:0000000000000006,1:0000000000000004,2:0000000000000004,3:0002000800000002,4:0002000000000002,7:0000000000000006
probe f16 g0 -> 0:0000000000000020,3:0000000000000004,4:0000000000000004,5:0000000000000004,6:0000000000000004
probe f16 g1 -> 0:0000040000000000
probe f16 g2 -> 1:0000000000008000
probe f16 g5 -> 1:0010000000000000
probe f16 ga5 -> 2:0000000002000000
probe f16 g1234 -> 2:4000000000000000
probe f16 gffff -> 0:0000000000000004,1:0000000000000004,2:0000000000000004,3:0000000800000000,7:0000000000000004
"#;

/// Lines `render` added since [`GOLDEN_V1`] was frozen. Only appended to; a
/// change to one is a derivation change like any other.
const GOLDEN_ADDED: &str = r#"excluded BPCHAR(0) zonemap=false
numeric s2 z0 [125, 5000, -1] -> ffffffffffffffffffffffffffffffff88130000000000000000000000000000
numeric s2 `amt = 1.25` -> eq 7d000000000000000000000000000000 zone=1
numeric default `amt = 1.25` -> eq 807c814a000000000000000000000000 zone=0
numeric s2 `amt > 50` -> gt 88130000000000000000000000000000 zone=0
numeric default `amt > 50` -> gt 00743ba40b0000000000000000000000 zone=0
numeric s2 `amt >= 50.00` -> ge 88130000000000000000000000000000 zone=1
numeric default `amt >= 50.00` -> ge 00743ba40b0000000000000000000000 zone=0
numeric s2 `amt < -0.01` -> lt ffffffffffffffffffffffffffffffff zone=0
numeric default `amt < -0.01` -> lt 806967ffffffffffffffffffffffffff zone=0
numeric s2 `amt <= 1.255` -> none
numeric default `amt <= 1.255` -> le c0c7cd4a000000000000000000000000 zone=1
numeric s2 `amt = 0.001` -> none
numeric default `amt = 0.001` -> eq 40420f00000000000000000000000000 zone=0
numeric s2 `amt > 49.99` -> gt 87130000000000000000000000000000 zone=1
numeric default `amt > 49.99` -> gt 80dda2a30b0000000000000000000000 zone=0
excluded TIMESTAMP zonemap=false
"#;
