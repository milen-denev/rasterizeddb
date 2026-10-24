//! Binary-format DataRows for results whose every column is a fixed-width
//! number, built without the per-cell dispatch.
//!
//! # Why a whole row, not a cell
//!
//! A binary `int8` or `float8` cell is the stored little-endian value with its
//! bytes reversed, and one cell's reversal is already a single instruction
//! (`MOVBE`, part of the x86-64-v3 baseline). Vectorising that alone gains
//! nothing: a row's cells are not next to each other in the engine's
//! serialized form (each carries a size, a column id and a type tag), so
//! gathering them into a vector costs what the swap saved. What a vector unit
//! *can* do is the whole row at once. When a result's columns are all `int2`,
//! `int4`, `int8`, `float4` or `float8`, sent in binary, and the row has no
//! NULL, both its serialized bytes and its DataRow have the same layout as
//! every other such row: the DataRow is a constant template (tag, length,
//! column count, each cell's length prefix) with each value's bytes reversed
//! into place. That is a byte permutation, planned once per result.
//!
//! # The tiers
//!
//! * **AVX-512 VBMI** (`vpermi2b`), when the serialized row fits 128 bytes
//!   and the DataRow 64. Up to four 8-byte columns. Two masked loads, one
//!   masked compare that checks every size and type tag of the row against the
//!   plan at once, one two-source byte permute that reverses every value and
//!   places it, one blend with the template, one masked store.
//! * **Per cell**: copy the template, then one `MOVBE` load-swap per value.
//!   On AVX2-only CPUs (the production server among them), and for rows too
//!   wide for one permute.
//! * **The generic encoder** for any row that does not match the plan, such
//!   as a NULL or a column of another width. Checked per row, so one NULL row
//!   in a million costs one slow row, not a slow result.
//!
//! All three produce exactly the bytes [`super::encode_result_cell`] does.
//! `fixed_rows_tests` holds each tier to that, row by row.
//!
//! # Measured
//!
//! `fixed_row_encode_cost`, 1M binary rows through the serialized sender on an
//! i9-11900K, nanoseconds a row, medians of seven interleaved rounds:
//!
//! | columns | generic | per cell (AVX2 level) | VBMI permute |
//! |---|---|---|---|
//! | `int4, float8` | 37.1 | 20.2 | 15.5 |
//! | 4 × `int8` | 64.7 | 29.4 | 22.6 |
//! | 7 mixed (too wide to permute) | 111.0 | 43.3 | 41.0 |
//!
//! Most of the gain is dropping the per-cell dispatch, which is why the
//! per-cell tier already takes half of it; the permute takes another quarter
//! of what is left. What remains is per row: walking the serialized rows and
//! writing each frame to the stream.

use std::sync::atomic::{AtomicBool, Ordering};

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::simds::cpu::{self, SimdLevel};

static ENABLED: AtomicBool = AtomicBool::new(true);

/// For A/B measurement and tests: off sends every row through the generic
/// encoder.
pub(crate) fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

#[cfg(feature = "enable_long_row")]
const ROW_LEN_HEADER: usize = 8;
#[cfg(not(feature = "enable_long_row"))]
const ROW_LEN_HEADER: usize = 4;

/// Per serialized cell: an 8-byte size, an 8-byte column id, the data, and a
/// 1-byte type tag (`Row::extend_serialized`).
const CELL_OVERHEAD: usize = 8 + 8 + 1;

#[derive(Clone, Debug)]
struct Cell {
    /// Where the cell's 8-byte size is in the serialized row.
    size_at: usize,
    /// Where its value starts there.
    src: usize,
    /// Where its value goes in the DataRow.
    dst: usize,
    width: usize,
    /// The type tag that must follow the value.
    tag: u8,
}

#[derive(Clone, Debug)]
pub(super) struct FixedRowShape {
    /// The serialized row's whole length, header included.
    src_len: usize,
    /// The DataRow with every value zeroed.
    template: Vec<u8>,
    cells: SmallVec<[Cell; 16]>,
    #[cfg(target_arch = "x86_64")]
    permute: Option<Permute>,
}

/// The single-permute plan, for rows that fit it.
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Debug)]
struct Permute {
    /// For each DataRow byte, the serialized byte it comes from (0..128).
    idx: [u8; 64],
    /// The DataRow bytes that are value bytes; the rest come from `template`.
    data: u64,
    template: [u8; 64],
    /// The serialized bytes that must equal `expected` (sizes and tags).
    check_lo: u64,
    check_hi: u64,
    expected: [u8; 128],
}

/// The binary width of a declared type that is sent as its stored bytes
/// reversed, and nothing else.
fn swapped_width(ty: &DbType) -> Option<usize> {
    match ty {
        DbType::I16 => Some(2),
        DbType::I32 | DbType::F32 => Some(4),
        DbType::I64 | DbType::F64 => Some(8),
        _ => None,
    }
}

fn format_code(result_formats: &[i16], i: usize) -> i16 {
    // As `encode_result_cell` resolves it.
    if result_formats.is_empty() {
        0
    } else if result_formats.len() == 1 {
        result_formats[0]
    } else {
        *result_formats.get(i).unwrap_or(&0)
    }
}

impl FixedRowShape {
    /// The plan for a result, read from its first row, or `None` when the
    /// result is not of the shape this handles. `col_meta` and `result_formats`
    /// are what the generic encoder would use, so the two cannot disagree on
    /// which type or format a column is.
    pub(super) fn plan(
        first_row: &[u8],
        col_meta: &[(String, DbType)],
        result_formats: &[i16],
    ) -> Option<FixedRowShape> {
        if !ENABLED.load(Ordering::Relaxed) {
            return None;
        }
        Self::shape_of(first_row, col_meta, result_formats)
    }

    /// [`Self::plan`] whatever the switch says. Tests hold the tiers to the
    /// generic bytes through this, so a sibling test turning the switch off
    /// cannot make the plan they need disappear mid-test.
    fn shape_of(
        first_row: &[u8],
        col_meta: &[(String, DbType)],
        result_formats: &[i16],
    ) -> Option<FixedRowShape> {
        let mut cells: SmallVec<[Cell; 16]> = SmallVec::new();
        // 'D', the message length, the column count.
        let mut dst = 1 + 4 + 2;
        let mut p = ROW_LEN_HEADER;
        for (i, cell) in crate::core::row::row::serialized_cells(first_row).ok()?.enumerate() {
            let cell = cell.ok()?;
            if format_code(result_formats, i) != 1 {
                return None;
            }
            // The declared type decides the format, as in the generic
            // encoder; this only takes the columns stored as declared.
            let declared = col_meta.get(i).map(|(_, t)| t).unwrap_or(&cell.column_type);
            if *declared != cell.column_type {
                return None;
            }
            let width = swapped_width(declared)?;
            if cell.data.len() != width {
                return None;
            }
            cells.push(Cell {
                size_at: p,
                src: p + 16,
                dst: dst + 4,
                width,
                tag: cell.column_type.to_byte(),
            });
            p += CELL_OVERHEAD + width;
            dst += 4 + width;
        }
        if cells.is_empty() || p != first_row.len() {
            return None;
        }
        let count: i16 = cells.len().try_into().ok()?;
        let mut template = vec![0u8; dst];
        template[0] = b'D';
        template[1..5].copy_from_slice(&((dst - 1) as i32).to_be_bytes());
        template[5..7].copy_from_slice(&count.to_be_bytes());
        for c in &cells {
            template[c.dst - 4..c.dst].copy_from_slice(&(c.width as i32).to_be_bytes());
        }

        #[cfg(target_arch = "x86_64")]
        let permute = (p <= 128 && dst <= 64).then(|| {
            let mut idx = [0u8; 64];
            let mut data = 0u64;
            let mut expected = [0u8; 128];
            let (mut check_lo, mut check_hi) = (0u64, 0u64);
            let mut check = |at: usize, byte: u8| {
                expected[at] = byte;
                if at < 64 {
                    check_lo |= 1 << at;
                } else {
                    check_hi |= 1 << (at - 64);
                }
            };
            for c in &cells {
                for (k, b) in (c.width as u64).to_le_bytes().into_iter().enumerate() {
                    check(c.size_at + k, b);
                }
                check(c.src + c.width, c.tag);
                for j in 0..c.width {
                    // Big-endian: the DataRow's first value byte is the
                    // stored value's last.
                    idx[c.dst + j] = (c.src + c.width - 1 - j) as u8;
                    data |= 1 << (c.dst + j);
                }
            }
            let mut t = [0u8; 64];
            t[..dst].copy_from_slice(&template);
            Permute {
                idx,
                data,
                template: t,
                check_lo,
                check_hi,
                expected,
            }
        });

        Some(FixedRowShape {
            src_len: p,
            template,
            cells,
            #[cfg(target_arch = "x86_64")]
            permute,
        })
    }

    /// Append `row`'s DataRow to `frame` and return `true`, or leave `frame`
    /// untouched and return `false` when the row is not of this shape.
    #[inline]
    pub(super) fn encode(&self, row: &[u8], frame: &mut Vec<u8>) -> bool {
        #[cfg(target_arch = "x86_64")]
        if self.permute.is_some() && cpu::level() == SimdLevel::Avx512 && cpu::features().avx512vbmi {
            // SAFETY: the level and the VBMI bit were both checked.
            return unsafe { self.encode_vbmi(row, frame) };
        }
        self.encode_cells(row, frame)
    }

    /// One `MOVBE` per value.
    pub(super) fn encode_cells(&self, row: &[u8], frame: &mut Vec<u8>) -> bool {
        if row.len() != self.src_len {
            return false;
        }
        for c in &self.cells {
            let size = u64::from_le_bytes(row[c.size_at..c.size_at + 8].try_into().unwrap());
            if size != c.width as u64 || row[c.src + c.width] != c.tag {
                return false;
            }
        }
        let start = frame.len();
        frame.extend_from_slice(&self.template);
        let out = &mut frame[start..];
        for c in &self.cells {
            let (s, d) = (&row[c.src..c.src + c.width], &mut out[c.dst..c.dst + c.width]);
            match c.width {
                2 => d.copy_from_slice(&u16::from_le_bytes(s.try_into().unwrap()).to_be_bytes()),
                4 => d.copy_from_slice(&u32::from_le_bytes(s.try_into().unwrap()).to_be_bytes()),
                _ => d.copy_from_slice(&u64::from_le_bytes(s.try_into().unwrap()).to_be_bytes()),
            }
        }
        true
    }

    /// The whole row in one permute. Callers check for AVX-512 (F, BW) and
    /// VBMI first.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,avx512vbmi")]
    pub(super) unsafe fn encode_vbmi(&self, row: &[u8], frame: &mut Vec<u8>) -> bool {
        use std::arch::x86_64::*;
        let Some(pm) = self.permute.as_ref() else {
            return self.encode_cells(row, frame);
        };
        if row.len() != self.src_len {
            return false;
        }
        let n = self.src_len;
        let dst_len = self.template.len();
        unsafe {
            // Only the row's own bytes are read: the masked-off lanes of a
            // masked load are not accessed, so nothing past the slice is.
            let lo_mask: u64 = if n >= 64 { u64::MAX } else { (1u64 << n) - 1 };
            let hi_mask: u64 = if n > 64 { if n >= 128 { u64::MAX } else { (1u64 << (n - 64)) - 1 } } else { 0 };
            let p = row.as_ptr() as *const i8;
            let lo = _mm512_maskz_loadu_epi8(lo_mask, p);
            let hi = if hi_mask != 0 {
                _mm512_maskz_loadu_epi8(hi_mask, p.add(64))
            } else {
                _mm512_setzero_si512()
            };
            // Every size and tag of the row against the plan, in two compares.
            let exp_lo = _mm512_loadu_si512(pm.expected.as_ptr() as *const __m512i);
            let exp_hi = _mm512_loadu_si512(pm.expected.as_ptr().add(64) as *const __m512i);
            let bad = _mm512_mask_cmpneq_epi8_mask(pm.check_lo, lo, exp_lo)
                | _mm512_mask_cmpneq_epi8_mask(pm.check_hi, hi, exp_hi);
            if bad != 0 {
                return false;
            }
            let idx = _mm512_loadu_si512(pm.idx.as_ptr() as *const __m512i);
            let moved = _mm512_permutex2var_epi8(lo, idx, hi);
            let tmpl = _mm512_loadu_si512(pm.template.as_ptr() as *const __m512i);
            let out = _mm512_mask_blend_epi8(pm.data, tmpl, moved);
            frame.reserve(dst_len);
            let at = frame.len();
            let dst_mask: u64 = if dst_len >= 64 { u64::MAX } else { (1u64 << dst_len) - 1 };
            _mm512_mask_storeu_epi8(frame.as_mut_ptr().add(at) as *mut i8, dst_mask, out);
            frame.set_len(at + dst_len);
        }
        true
    }
}

#[cfg(test)]
mod fixed_rows_tests {
    use super::*;
    use crate::core::row::row::{Column, Row, RowColumns, rows_into_vec, schema_id_of};
    use crate::memory_pool::MEMORY_POOL;
    use crate::simds::cpu::SimdRequest;

    /// The tests that flip the switch or the SIMD level run one at a time.
    static SWITCH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn cell(schema_id: u64, ty: DbType, bytes: &[u8]) -> Column {
        let mut data = MEMORY_POOL.acquire(bytes.len());
        if !bytes.is_empty() {
            data.into_slice_mut().copy_from_slice(bytes);
        }
        Column {
            schema_id: schema_id_of(schema_id),
            data,
            column_type: ty,
        }
    }

    fn value(ty: &DbType, i: i64) -> Vec<u8> {
        let specials_f64 = [f64::NAN, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::MIN_POSITIVE, 1.5];
        let specials_f32 = [f32::NAN, -0.0, f32::INFINITY, f32::MIN, f32::MAX, 0.1];
        match ty {
            DbType::I16 => ((i * 7919) as i16).to_le_bytes().to_vec(),
            DbType::I32 => ((i * 2_654_435_761) as i32).to_le_bytes().to_vec(),
            DbType::I64 => (i.wrapping_mul(0x9E37_79B9_7F4A_7C15u64 as i64)).to_le_bytes().to_vec(),
            DbType::F32 => specials_f32[i as usize % 6].to_le_bytes().to_vec(),
            DbType::F64 => specials_f64[i as usize % 6].to_le_bytes().to_vec(),
            DbType::STRING => format!("s{i}").into_bytes(),
            DbType::BOOL => vec![(i % 2) as u8],
            _ => unreachable!(),
        }
    }

    /// Rows of `types`; every `null_every`-th row has a NULL in its last
    /// column, so the per-row check has to send it the slow way.
    fn rows(types: &[DbType], n: i64, null_every: i64) -> Vec<Row> {
        (0..n)
            .map(|i| {
                let mut columns = RowColumns::new();
                for (k, ty) in types.iter().enumerate() {
                    let last = k + 1 == types.len();
                    columns.push(if last && null_every > 0 && i % null_every == null_every - 1 {
                        cell(k as u64, DbType::NULL, &[])
                    } else {
                        cell(k as u64, ty.clone(), &value(ty, i + k as i64))
                    });
                }
                Row {
                    position: 0,
                    columns,
                    length: 0,
                }
            })
            .collect()
    }

    async fn send(bytes: &[u8], fields: &[(String, DbType)], formats: &[i16]) -> Vec<u8> {
        let mut out = Vec::new();
        crate::pgwire::send_serialized_query_result(&mut out, Some(fields), None, bytes, formats, "SELECT x", false)
            .await
            .unwrap();
        out
    }

    const SHAPES: &[&[DbType]] = &[
        &[DbType::I64],
        &[DbType::I16],
        &[DbType::I32, DbType::F32],
        &[DbType::I64, DbType::F64, DbType::I32, DbType::I16],
        &[DbType::I64, DbType::I64, DbType::I64, DbType::I64],
        // Too wide for one permute: the per-cell tier.
        &[DbType::I64, DbType::F64, DbType::I64, DbType::F64, DbType::I32, DbType::I16, DbType::F32],
        // Not fixed: planned as None, every row generic.
        &[DbType::I64, DbType::STRING],
        &[DbType::BOOL, DbType::I32],
    ];

    /// The whole sender, plan on and off, at the process's own SIMD level.
    ///
    /// Not at every level: `cpu::init` is process-wide, and the filter
    /// index's `probe_fn` caches whichever level it first sees, so forcing
    /// `scalar` here for a moment could leave every other test in the binary
    /// on the scalar probe. Each tier is held to the generic bytes directly by
    /// `every_tier_writes_the_generic_bytes` instead.
    #[tokio::test]
    async fn fixed_rows_go_out_byte_identical() {
        let _g = SWITCH.lock().await;
        let mut compared = 0;
        for types in SHAPES {
            let fields: Vec<(String, DbType)> =
                types.iter().enumerate().map(|(k, t)| (format!("c{k}"), t.clone())).collect();
            let mixed: Vec<i16> = (0..types.len()).map(|k| (k % 2) as i16).collect();
            for null_every in [0, 7] {
                let bytes = rows_into_vec(rows(types, 300, null_every));
                for formats in [&[1i16][..], &[], &[0], &mixed] {
                    set_enabled(false);
                    let generic = send(&bytes, &fields, formats).await;
                    set_enabled(true);
                    let fast = send(&bytes, &fields, formats).await;
                    assert!(
                        fast == generic,
                        "{types:?} nulls every {null_every} formats {formats:?} at {}",
                        cpu::level()
                    );
                    compared += 1;
                }
            }
        }
        set_enabled(true);
        assert!(compared > 0);
    }

    /// Each tier directly, so a tier the level routing would not pick on this
    /// machine is still checked, and a plan that declined everything is caught.
    #[test]
    fn every_tier_writes_the_generic_bytes() {
        let mut planned = 0;
        for types in &SHAPES[..6] {
            let fields: Vec<(String, DbType)> =
                types.iter().enumerate().map(|(k, t)| (format!("c{k}"), t.clone())).collect();
            let rs = rows(types, 64, 5);
            let bytes = rows_into_vec(rs);
            let spans: Vec<&[u8]> = crate::core::row::row::serialized_rows(&bytes).map(|s| s.unwrap()).collect();
            let shape = FixedRowShape::shape_of(spans[0], &fields, &[1]).expect("all fixed, all binary: planned");
            planned += 1;
            for span in &spans {
                // The generic encoder's DataRow for this row.
                let mut want = Vec::new();
                let start = super::super::wire_out::begin_message(&mut want, b'D');
                let count_at = want.len();
                want.extend_from_slice(&[0u8; 2]);
                let mut n = 0i16;
                let mut has_null = false;
                for (i, c) in crate::core::row::row::serialized_cells(span).unwrap().enumerate() {
                    let c = c.unwrap();
                    has_null |= c.column_type == DbType::NULL;
                    super::super::encode_result_cell(&mut want, i, &c.column_type, c.data, &fields, None, &[1]);
                    n += 1;
                }
                want[count_at..count_at + 2].copy_from_slice(&n.to_be_bytes());
                super::super::wire_out::finish_message(&mut want, start).unwrap();

                let mut got = vec![0xAAu8; 3];
                let ok = shape.encode_cells(span, &mut got);
                assert_eq!(ok, !has_null, "{types:?}: a NULL row must be refused, any other taken");
                if ok {
                    assert_eq!(&got[3..], &want[..], "{types:?} per cell");
                } else {
                    assert_eq!(got, vec![0xAA; 3], "a refused row must leave the frame alone");
                }
                #[cfg(target_arch = "x86_64")]
                if cpu::features().supports(SimdLevel::Avx512) && cpu::features().avx512vbmi {
                    let mut got = vec![0xAAu8; 3];
                    let ok = unsafe { shape.encode_vbmi(span, &mut got) };
                    assert_eq!(ok, !has_null, "{types:?} vbmi");
                    if ok {
                        assert_eq!(&got[3..], &want[..], "{types:?} vbmi");
                    } else {
                        assert_eq!(got, vec![0xAA; 3]);
                    }
                }
            }
        }
        assert_eq!(planned, 6);
        // A text column, or a text format, is not this shape.
        let f = |t: &[DbType]| -> Vec<(String, DbType)> { t.iter().map(|t| ("c".to_string(), t.clone())).collect() };
        let bytes = rows_into_vec(rows(&[DbType::I64, DbType::STRING], 1, 0));
        let span = crate::core::row::row::serialized_rows(&bytes).next().unwrap().unwrap();
        assert!(FixedRowShape::shape_of(span, &f(&[DbType::I64, DbType::STRING]), &[1]).is_none());
        let bytes = rows_into_vec(rows(&[DbType::I64], 1, 0));
        let span = crate::core::row::row::serialized_rows(&bytes).next().unwrap().unwrap();
        assert!(FixedRowShape::shape_of(span, &f(&[DbType::I64]), &[0]).is_none());
        // Declared differently from how it is stored: the generic encoder
        // formats by the declared type, so this must not plan.
        assert!(FixedRowShape::shape_of(span, &f(&[DbType::DATETIME]), &[1]).is_none());
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// fixed_row_encode_cost --nocapture --test-threads 1`.
    #[tokio::test]
    #[ignore]
    async fn fixed_row_encode_cost() {
        use std::time::Instant;
        let _g = SWITCH.lock().await;
        for types in [
            &[DbType::I64, DbType::I64, DbType::I64, DbType::I64][..],
            &[DbType::I32, DbType::F64][..],
            &[DbType::I64, DbType::F64, DbType::I64, DbType::F64, DbType::I32, DbType::I16, DbType::F32][..],
        ] {
            let fields: Vec<(String, DbType)> =
                types.iter().enumerate().map(|(k, t)| (format!("c{k}"), t.clone())).collect();
            let n = 1_000_000;
            let bytes = rows_into_vec(rows(types, n, 0));
            let mut configs: Vec<(String, bool, Option<SimdLevel>)> = vec![("generic".into(), false, None)];
            for level in cpu::runnable_levels().into_iter().filter(|l| *l != SimdLevel::Scalar) {
                configs.push((format!("fixed@{level}"), true, Some(level)));
            }
            let mut samples: Vec<Vec<f64>> = vec![Vec::new(); configs.len()];
            for _round in 0..7 {
                for (c, (_, on, level)) in configs.iter().enumerate() {
                    set_enabled(*on);
                    if let Some(l) = level {
                        cpu::init(SimdRequest::Fixed(*l)).unwrap();
                    }
                    let t = Instant::now();
                    std::hint::black_box(send(&bytes, &fields, &[1]).await);
                    samples[c].push(t.elapsed().as_secs_f64() * 1e9 / n as f64);
                }
            }
            let mut line = format!("{} cols:", types.len());
            for (c, (name, _, _)) in configs.iter().enumerate() {
                samples[c].sort_by(|a, b| a.partial_cmp(b).unwrap());
                line += &format!(" {name} {:.1} ns/row", samples[c][samples[c].len() / 2]);
            }
            println!("{line}");
        }
        cpu::init(SimdRequest::Auto).unwrap();
        set_enabled(true);
    }
}
