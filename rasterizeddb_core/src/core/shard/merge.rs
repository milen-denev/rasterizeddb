//! The partial-aggregate codec (sharding-plan.md B1c): a shard's folded but
//! unfinished groups, sent to the coordinator shard, which merges them with
//! the engine's own `Folder` and finishes once.
//!
//! **State travels; configuration does not.** Each accumulator is encoded as
//! its running state only (counts, totals, the winning bytes, the collected
//! values). Its configuration (the argument's type and scale, the
//! aggregate's `ORDER BY`, its delimiter and memory limit) is taken from the
//! coordinator's own plan of the statement: [`decode_acc`] fills a clone of
//! the coordinator's template accumulator. A shard therefore can never make
//! the coordinator finish a group differently from how it would have
//! finished it alone, and a shard whose plan disagrees (another scale, other
//! aggregates) is refused with `XX000` instead of merged into a number that
//! looks right ([[the-number-that-looks-right]]).
//!
//! Every variant is destructured with no `..`, so a field added to an
//! accumulator fails to compile here until it is carried or deliberately
//! taken from the template.
//!
//! # Layout
//!
//! All integers little-endian. A partial is
//! `version:u8 ‖ stride:u32 ‖ groups:u32 ‖ groups × group`, a group is
//! `key:bytes ‖ shown:opt(bytes) ‖ stride × acc`, and `bytes` is
//! `len:u32 ‖ len bytes`. Each `acc` opens with its variant tag.

use crate::core::rql::aggregate::Acc;

/// The layout version.
pub const PARTIAL_V1: u8 = 1;

const T_COUNT: u8 = 1;
const T_EXACT_SUM: u8 = 2;
const T_FLOAT_SUM: u8 = 3;
const T_INTERVAL_SUM: u8 = 4;
const T_MOMENTS: u8 = 5;
const T_FLOAT_MOMENTS: u8 = 6;
const T_EXTREME: u8 = 7;
const T_DISTINCT: u8 = 8;
const T_COLLECT: u8 = 9;
const T_BOOLS: u8 = 10;

/// The refusal for a partial that does not fit the coordinator's plan.
fn mismatch(what: &str) -> String {
    format!("XX000: a shard's partial aggregate does not match this statement's plan: {what}")
}

/// Appends `acc`'s running state to `out` (module docs).
pub(crate) fn encode_acc(acc: &Acc, out: &mut Vec<u8>) {
    match acc {
        Acc::Count(n) => {
            out.push(T_COUNT);
            put_i64(out, *n);
        }
        Acc::ExactSum { total, n, scaled, scale, overflow, dscale, track_dscale, own_scale, base } => {
            out.push(T_EXACT_SUM);
            put_i128(out, *total);
            put_i64(out, *n);
            out.push(*scaled as u8);
            put_u32(out, *scale);
            out.push(*overflow as u8);
            put_u32(out, *dscale);
            out.push(*track_dscale as u8);
            out.push(*own_scale as u8);
            put_u32(out, *base);
        }
        Acc::FloatSum { total, n } => {
            out.push(T_FLOAT_SUM);
            put_u64(out, total.to_bits());
            put_i64(out, *n);
        }
        Acc::IntervalSum { months, days, micros, n } => {
            out.push(T_INTERVAL_SUM);
            put_i64(out, *months);
            put_i64(out, *days);
            put_i128(out, *micros);
            put_i64(out, *n);
        }
        Acc::Moments { n, sum, sum_sq, scale, declared, overflow } => {
            out.push(T_MOMENTS);
            put_i64(out, *n);
            put_i128(out, *sum);
            put_i128(out, *sum_sq);
            put_u32(out, *scale);
            match declared {
                Some(d) => {
                    out.push(1);
                    put_u32(out, *d);
                }
                None => out.push(0),
            }
            out.push(*overflow as u8);
        }
        Acc::FloatMoments { n, sx, sxx } => {
            out.push(T_FLOAT_MOMENTS);
            put_u64(out, n.to_bits());
            put_u64(out, sx.to_bits());
            put_u64(out, sxx.to_bits());
        }
        Acc::Extreme { best, ty, want_max } => {
            out.push(T_EXTREME);
            // `ty` and `want_max` are configuration, checked against the
            // template on decode.
            out.push(ty.to_byte());
            out.push(*want_max as u8);
            put_opt_bytes(out, best.as_deref());
        }
        Acc::Distinct { seen, template, ty, null } => {
            out.push(T_DISTINCT);
            out.push(ty.to_byte());
            out.push(*null as u8);
            // The values in a stable order, so equal states encode equally.
            let mut values: Vec<&Vec<u8>> = seen.iter().collect();
            values.sort_unstable();
            put_u32(out, values.len() as u32);
            for v in values {
                put_bytes(out, v);
            }
            // The fed function's own accumulator is rebuilt over the union at
            // the end (`Acc::resolve`); its template is the coordinator's.
            let _ = template;
        }
        Acc::Collect { values, order, by_value_descending, elem, delimiter, held, limit } => {
            out.push(T_COLLECT);
            // Configuration taken from the template on decode.
            let _ = (order, by_value_descending, elem, delimiter, limit);
            put_u64(out, *held as u64);
            put_u32(out, values.len() as u32);
            for (keys, value) in values {
                put_u32(out, keys.len() as u32);
                for k in keys {
                    match k {
                        Some((bytes, ty)) => {
                            out.push(1);
                            out.push(ty.to_byte());
                            put_bytes(out, bytes);
                        }
                        None => out.push(0),
                    }
                }
                put_opt_bytes(out, value.as_deref());
            }
        }
        Acc::Bools { seen, all, any } => {
            out.push(T_BOOLS);
            out.push(*seen as u8);
            out.push(*all as u8);
            out.push(*any as u8);
        }
    }
}

/// A clone of the coordinator's `template` holding the state [`encode_acc`]
/// wrote (module docs). Refused when the state is of another kind, or its
/// configuration-bearing fields (an exact sum's scale, an extreme's type)
/// disagree with the template's.
pub(crate) fn decode_acc(template: &Acc, r: &mut Reader<'_>) -> Result<Acc, String> {
    let tag = r.u8()?;
    let mut acc = template.clone();
    match &mut acc {
        Acc::Count(n) => {
            expect(tag, T_COUNT)?;
            *n = r.i64()?;
        }
        Acc::ExactSum { total, n, scaled, scale, overflow, dscale, track_dscale, own_scale, base } => {
            expect(tag, T_EXACT_SUM)?;
            *total = r.i128()?;
            *n = r.i64()?;
            let their_scaled = r.bool()?;
            let their_scale = r.u32()?;
            *overflow = r.bool()?;
            *dscale = r.u32()?;
            let their_track = r.bool()?;
            let their_own = r.bool()?;
            let their_base = r.u32()?;
            // An own-scale sum lifts its scale to the most places it met, so
            // a shard's may be above the template's; the merge lifts the
            // smaller. Everything else must be the plan's.
            if their_scaled != *scaled || their_track != *track_dscale || their_base != *base {
                return Err(mismatch("an exact sum's scaling differs"));
            }
            if their_own {
                *own_scale = true;
                *scale = their_scale;
            } else if *own_scale || their_scale != *scale {
                return Err(mismatch(&format!(
                    "an exact sum is at scale {their_scale}, the plan's is {}",
                    *scale
                )));
            }
        }
        Acc::FloatSum { total, n } => {
            expect(tag, T_FLOAT_SUM)?;
            *total = f64::from_bits(r.u64()?);
            *n = r.i64()?;
        }
        Acc::IntervalSum { months, days, micros, n } => {
            expect(tag, T_INTERVAL_SUM)?;
            *months = r.i64()?;
            *days = r.i64()?;
            *micros = r.i128()?;
            *n = r.i64()?;
        }
        Acc::Moments { n, sum, sum_sq, scale, declared, overflow } => {
            expect(tag, T_MOMENTS)?;
            *n = r.i64()?;
            *sum = r.i128()?;
            *sum_sq = r.i128()?;
            *scale = r.u32()?;
            let theirs = if r.bool()? { Some(r.u32()?) } else { None };
            if theirs != *declared {
                return Err(mismatch("a variance's declared scale differs"));
            }
            *overflow = r.bool()?;
        }
        Acc::FloatMoments { n, sx, sxx } => {
            expect(tag, T_FLOAT_MOMENTS)?;
            *n = f64::from_bits(r.u64()?);
            *sx = f64::from_bits(r.u64()?);
            *sxx = f64::from_bits(r.u64()?);
        }
        Acc::Extreme { best, ty, want_max } => {
            expect(tag, T_EXTREME)?;
            let their_ty = r.u8()?;
            let their_max = r.bool()?;
            if their_ty != ty.to_byte() || their_max != *want_max {
                return Err(mismatch("a MIN/MAX is over another type or direction"));
            }
            *best = r.opt_bytes()?;
        }
        Acc::Distinct { seen, template: _, ty, null } => {
            expect(tag, T_DISTINCT)?;
            if r.u8()? != ty.to_byte() {
                return Err(mismatch("a DISTINCT aggregate is over another type"));
            }
            *null = r.bool()?;
            let n = r.u32()? as usize;
            seen.clear();
            seen.reserve(n.min(1 << 16));
            for _ in 0..n {
                seen.insert(r.bytes()?);
            }
        }
        Acc::Collect { values, order, by_value_descending: _, elem: _, delimiter: _, held, limit } => {
            expect(tag, T_COLLECT)?;
            *held = r.u64()? as usize;
            let n = r.u32()? as usize;
            values.clear();
            values.reserve(n.min(1 << 16));
            for _ in 0..n {
                let k = r.u32()? as usize;
                if k != order.len() {
                    return Err(mismatch("an ordered aggregate carries another number of keys"));
                }
                let mut keys = Vec::with_capacity(k);
                for _ in 0..k {
                    keys.push(if r.bool()? {
                        let ty = crate::core::db_type::DbType::from_byte(r.u8()?);
                        Some((r.bytes()?, ty))
                    } else {
                        None
                    });
                }
                values.push((keys, r.opt_bytes()?));
            }
            // A shard past the limit has let its values go; so does the
            // merge, with the same error at finish.
            if *held != usize::MAX && *held > limit.0 {
                *held = usize::MAX;
                values.clear();
            }
        }
        Acc::Bools { seen, all, any } => {
            expect(tag, T_BOOLS)?;
            *seen = r.bool()?;
            *all = r.bool()?;
            *any = r.bool()?;
        }
    }
    Ok(acc)
}

fn expect(tag: u8, want: u8) -> Result<(), String> {
    if tag == want {
        Ok(())
    } else {
        Err(mismatch(&format!("an accumulator of kind {tag} where the plan has kind {want}")))
    }
}

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i128(out: &mut Vec<u8>, v: i128) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

pub(crate) fn put_opt_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        Some(b) => {
            out.push(1);
            put_bytes(out, b);
        }
        None => out.push(0),
    }
}

/// A bounds-checked reader over a partial.
pub(crate) struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Reader { b, at: 0 }
    }

    pub(crate) fn done(&self) -> bool {
        self.at == self.b.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len());
        let Some(end) = end else {
            return Err(mismatch("the partial is truncated"));
        };
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(mismatch(&format!("a flag byte {b}"))),
        }
    }

    pub(crate) fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i128(&mut self) -> Result<i128, String> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    pub(crate) fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }

    pub(crate) fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.bool()? { Ok(Some(self.bytes()?)) } else { Ok(None) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r2r3_partial_count_round_trips_and_a_cut_frame_is_refused() {
        for n in [0i64, 1, -1, i64::MAX, 123_456_789] {
            let mut out = Vec::new();
            encode_acc(&Acc::Count(n), &mut out);
            let mut r = Reader::new(&out);
            let back = decode_acc(&Acc::Count(0), &mut r).unwrap();
            assert!(matches!(back, Acc::Count(v) if v == n));
            assert!(r.done(), "every byte read");
            for cut in 0..out.len() {
                assert!(decode_acc(&Acc::Count(0), &mut Reader::new(&out[..cut])).is_err(), "cut at {cut}");
            }
        }
    }
}
