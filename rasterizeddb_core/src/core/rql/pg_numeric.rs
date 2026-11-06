//! PostgreSQL's `numeric` arithmetic, display scale and all.
//!
//! A PostgreSQL `numeric` value carries its own *display scale* (`dscale`),
//! and every operator picks its result's from its operands (numeric.c):
//!
//! * `+`, `-` and `%`: the larger of the two (`add_var`, `sub_var`, `mod_var`);
//! * `*`: their sum, exactly (`numeric_mul` asks `mul_var` for
//!   `var1->dscale + var2->dscale`);
//! * `/`: [`select_div_scale`], which looks at the operands' *values* — enough
//!   places for sixteen significant digits, never fewer than either operand's
//!   display scale — and the quotient rounded half away from zero there;
//! * a written constant: the places it was written with (`numeric_in`), so
//!   `1.50` is two places and `1.5e3` none; an integer: none.
//!
//! The engine evaluates `NUMERIC` expressions at a fixed nine places
//! (`value_codec::DECIMAL_SCALE`), which cut `1.0 / 3` to 0.333333333 where
//! PostgreSQL gives twenty places, and `0.00001 * 0.00001` to 0. This module is
//! the value-and-scale arithmetic the evaluator (`expr::pg_numeric`) uses where
//! a result is output or stored, and where a comparison must be exact.
//!
//! A value is `m × 10^-s` in an `i128`: every scale up to 38. Anything past
//! that is `None`, for the caller to take the engine's path as before.

use std::cmp::Ordering;

/// The most places a [`PgNum`] carries: an `i128` holds 38 digits, and the
/// result cell that carries its own scale (`value_codec::decimal_cell_with_scale`)
/// is read back only up to this.
pub(crate) const MAX_SCALE: u32 = 38;

/// `NUMERIC_MIN_SIG_DIGITS`: a quotient keeps at least this many significant
/// digits.
const MIN_SIG_DIGITS: i32 = 16;

/// `DEC_DIGITS`: PostgreSQL's `numeric` digit is base 10000, four decimal
/// digits, and [`select_div_scale`] reasons in those digits.
const DEC_DIGITS: i32 = 4;

/// A `numeric` value with its display scale: `m × 10^-s`, `s` the places
/// PostgreSQL shows it with (trailing zeros included: `m` is 250 and `s` 2 for
/// `2.50`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PgNum {
    pub m: i128,
    pub s: u32,
}

/// The error PostgreSQL raises for a zero divisor (SQLSTATE 22012, which the
/// wire maps from this text).
pub(crate) const DIVISION_BY_ZERO: &str = "division by zero";

fn pow10(n: u32) -> Option<i128> {
    10i128.checked_pow(n)
}

impl PgNum {
    pub(crate) fn new(m: i128, s: u32) -> Option<Self> {
        (s <= MAX_SCALE).then_some(PgNum { m, s })
    }

    /// An integer, at scale 0.
    pub(crate) fn int(v: i128) -> Self {
        PgNum { m: v, s: 0 }
    }

    /// A value held at `scale` with no display scale of its own (an
    /// unconstrained `NUMERIC` column, which this engine stores at nine
    /// places), shown with the places its value needs: the zeros the storage
    /// added are not the value's.
    pub(crate) fn trimmed(mut m: i128, mut s: u32) -> Self {
        while s > 0 && m % 10 == 0 {
            m /= 10;
            s -= 1;
        }
        PgNum { m, s }
    }

    /// The unscaled integer at `s` places, which must be at least `self.s`.
    fn at(self, s: u32) -> Option<i128> {
        debug_assert!(s >= self.s);
        self.m.checked_mul(pow10(s - self.s)?)
    }

    /// Rounded (half away from zero, as `round_var`) or extended to `s` places.
    pub(crate) fn round_to(self, s: u32) -> Option<Self> {
        if s > MAX_SCALE {
            return None;
        }
        if s >= self.s {
            return Some(PgNum { m: self.at(s)?, s });
        }
        let divisor = pow10(self.s - s)?;
        let (q, r) = (self.m / divisor, self.m % divisor);
        let m = if r.unsigned_abs() >= divisor.unsigned_abs() - r.unsigned_abs() {
            q.checked_add(self.m.signum())?
        } else {
            q
        };
        Some(PgNum { m, s })
    }

    /// `round(x, places)`: half away from zero, as `round_var`. A negative
    /// `places` rounds to tens, hundreds, … at scale 0 (`round(1250, -2)` is
    /// 1300), in one step: rounding to 0 places first would round twice
    /// (1249.5 to 1250, then to 1300, where PostgreSQL gives 1200).
    pub(crate) fn round_places(self, places: i32) -> Option<Self> {
        if places >= 0 {
            return self.round_to(places as u32);
        }
        let k = places.unsigned_abs();
        // A unit wider than any i128 value: every value rounds to zero.
        let Some(divisor) = self.s.checked_add(k).and_then(pow10) else {
            return Some(PgNum { m: 0, s: 0 });
        };
        let (q, r) = (self.m / divisor, self.m % divisor);
        let q = if r.unsigned_abs() >= divisor.unsigned_abs() - r.unsigned_abs() {
            q.checked_add(self.m.signum())?
        } else {
            q
        };
        if q == 0 {
            return Some(PgNum { m: 0, s: 0 });
        }
        Some(PgNum { m: q.checked_mul(pow10(k)?)?, s: 0 })
    }

    pub(crate) fn add(self, other: Self) -> Option<Self> {
        let s = self.s.max(other.s);
        Some(PgNum { m: self.at(s)?.checked_add(other.at(s)?)?, s })
    }

    pub(crate) fn sub(self, other: Self) -> Option<Self> {
        let s = self.s.max(other.s);
        Some(PgNum { m: self.at(s)?.checked_sub(other.at(s)?)?, s })
    }

    /// Exact, at the sum of the two scales. Past 38 places, or past an
    /// i128, the zeros at the operands' ends are dropped first: the value is
    /// still exact, at as many of its places as 38 hold (`None` when even
    /// its own digits do not fit).
    pub(crate) fn mul(self, other: Self) -> Option<Self> {
        if let Some(v) = self.m.checked_mul(other.m).and_then(|m| PgNum::new(m, self.s + other.s)) {
            return Some(v);
        }
        let (a, b) = (PgNum::trimmed(self.m, self.s), PgNum::trimmed(other.m, other.s));
        let v = PgNum::new(a.m.checked_mul(b.m)?, a.s + b.s)?;
        // Shown with as many of its display places as an i128 holds.
        let most = (self.s + other.s).min(MAX_SCALE);
        Some((v.s..=most).rev().find_map(|s| v.round_to(s)).unwrap_or(v))
    }

    pub(crate) fn neg(self) -> Option<Self> {
        Some(PgNum { m: self.m.checked_neg()?, s: self.s })
    }

    pub(crate) fn abs(self) -> Option<Self> {
        Some(PgNum { m: self.m.checked_abs()?, s: self.s })
    }

    /// `numeric_div`: rounded at [`select_div_scale`]'s places. `Err` for a
    /// zero divisor, `Ok(None)` past what an `i128` holds.
    pub(crate) fn div(self, other: Self) -> Result<Option<Self>, &'static str> {
        if other.m == 0 {
            return Err(DIVISION_BY_ZERO);
        }
        let rscale = select_div_scale(self, other);
        if rscale > MAX_SCALE {
            // Past 38 places: answered only when the quotient ends within
            // them (`1e-9 / 1e11` is exactly 1e-20), at 38; anything else is
            // `None`, for the caller to refuse rather than cut.
            if self.s > MAX_SCALE {
                return Ok(None);
            }
            let k = MAX_SCALE + other.s - self.s;
            return Ok(match div_round_exact(self.m, k, other.m) {
                Some((m, true)) => Some(PgNum { m, s: MAX_SCALE }),
                _ => None,
            });
        }
        // q = (a.m / 10^a.s) / (b.m / 10^b.s) at 10^-rscale
        //   = a.m · 10^(rscale + b.s − a.s) / b.m, and rscale ≥ a.s.
        let k = rscale + other.s - self.s;
        Ok(div_round(self.m, k, other.m).map(|m| PgNum { m, s: rscale }))
    }

    /// `sqrt_var`: the square root at `rscale` places, rounded half away
    /// from zero (PostgreSQL's `round_var` of its truncated root). `None` for
    /// a negative value or past what an `i128` holds.
    ///
    /// Exact: the root is floored at one place more, through 256 bits, and
    /// then rounded. Flooring at more places would change nothing: the digit
    /// past `rscale` decides, and its floor is the digit.
    pub(crate) fn sqrt_at(self, rscale: u32) -> Option<Self> {
        if self.m < 0 || rscale > MAX_SCALE {
            return None;
        }
        if self.m == 0 {
            return Some(PgNum { m: 0, s: rscale });
        }
        // floor(sqrt(m · 10^(2(rscale + 1) − s))) is the root at rscale + 1
        // places; a negative exponent floors the target first, which leaves
        // the floored root unchanged.
        let k = 2 * (rscale as i64 + 1) - self.s as i64;
        let (mut hi, mut lo) = (0u128, self.m as u128);
        if k >= 0 {
            let mut left = k as u32;
            while left > 0 {
                let step = left.min(38);
                (hi, lo) = mul_u256_u128(hi, lo, 10u128.pow(step))?;
                left -= step;
            }
        } else {
            lo /= pow10((-k) as u32)? as u128;
        }
        let (mut low, mut high) = (0u128, u128::MAX);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if widening_mul(mid, mid) <= (hi, lo) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let rounded = low / 10 + u128::from(low % 10 >= 5);
        Some(PgNum { m: i128::try_from(rounded).ok()?, s: rscale })
    }

    /// `numeric_mod`: the remainder of truncating division, at the larger
    /// scale, with the dividend's sign.
    pub(crate) fn rem(self, other: Self) -> Result<Option<Self>, &'static str> {
        if other.m == 0 {
            return Err(DIVISION_BY_ZERO);
        }
        let s = self.s.max(other.s);
        let (Some(x), Some(y)) = (self.at(s), other.at(s)) else {
            return Ok(None);
        };
        Ok(x.checked_rem(y).map(|m| PgNum { m, s }))
    }

    /// Numeric order; the display scale takes no part (`1.50 = 1.5`).
    pub(crate) fn cmp_value(self, other: Self) -> Ordering {
        let s = self.s.max(other.s);
        match (self.at(s), other.at(s)) {
            (Some(x), Some(y)) => x.cmp(&y),
            // The one that overflowed when lifted is the larger magnitude.
            (None, _) => {
                if self.m < 0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, None) => {
                if other.m < 0 {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        }
    }

    /// PostgreSQL's text for the value: every place of its display scale.
    pub(crate) fn to_text(self) -> String {
        crate::core::value_codec::format_decimal_scaled(self.m, self.s)
    }

    /// The value as a result cell carrying its own display scale.
    pub(crate) fn to_cell(self) -> Vec<u8> {
        crate::core::value_codec::decimal_cell_with_scale(self.m, self.s)
    }

    /// The value at `scale` places (the engine's nine, or a column's own),
    /// rounded half away from zero.
    pub(crate) fn units_at(self, scale: u32) -> Option<i128> {
        self.round_to(scale).map(|v| v.m)
    }

    /// Whether the value is exactly representable at `scale` places.
    pub(crate) fn exact_at(self, scale: u32) -> bool {
        scale >= self.s || pow10(self.s - scale).is_some_and(|d| self.m % d == 0)
    }
}

/// The weight and first digit of `m × 10^-s` in PostgreSQL's base-10000
/// representation: the value lies in `[NBASE^weight, NBASE^(weight+1))` and
/// its leading base-10000 digit is `first`. Zero is `(0, 0)`, as the loop in
/// `select_div_scale` leaves it.
fn weight_and_first_digit(m: i128, s: u32) -> (i32, u128) {
    if m == 0 {
        return (0, 0);
    }
    let a = m.unsigned_abs();
    let digits = a.checked_ilog10().map_or(1, |l| l + 1) as i32;
    // The decimal exponent of the leading digit.
    let p = digits - 1 - s as i32;
    let weight = p.div_euclid(DEC_DIGITS);
    let e = s as i32 + DEC_DIGITS * weight;
    // `e` is below zero only when the value has fewer than four digits in
    // all, so the lift cannot overflow.
    let first = if e >= 0 { a / 10u128.pow(e as u32) } else { a * 10u128.pow((-e) as u32) };
    (weight, first)
}

/// `select_div_scale` (numeric.c): the result scale of `a / b`.
pub(crate) fn select_div_scale(a: PgNum, b: PgNum) -> u32 {
    let (weight1, first1) = weight_and_first_digit(a.m, a.s);
    let (weight2, first2) = weight_and_first_digit(b.m, b.s);
    // Estimate the weight of the quotient. If the two first digits are equal
    // it cannot be sure, and assumes the dividend is the smaller.
    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }
    let rscale = (MIN_SIG_DIGITS - qweight * DEC_DIGITS)
        .max(a.s as i32)
        .max(b.s as i32)
        .max(0) // NUMERIC_MIN_DISPLAY_SCALE
        .min(1000); // NUMERIC_MAX_DISPLAY_SCALE
    rscale as u32
}

/// `round(a · 10^k / d)`, half away from zero, through 256 bits; `None` when
/// the quotient does not fit an `i128`. `d` is not zero.
fn div_round(a: i128, k: u32, d: i128) -> Option<i128> {
    div_round_exact(a, k, d).map(|(q, _)| q)
}

/// [`div_round`], and whether the division left no remainder.
fn div_round_exact(a: i128, k: u32, d: i128) -> Option<(i128, bool)> {
    let negative = (a < 0) != (d < 0);
    let divisor = d.unsigned_abs();
    // The numerator a · 10^k as (hi, lo), lifted in steps an `u128` holds.
    let (mut hi, mut lo) = (0u128, a.unsigned_abs());
    let mut left = k;
    while left > 0 {
        let step = left.min(38);
        (hi, lo) = mul_u256_u128(hi, lo, 10u128.pow(step))?;
        left -= step;
    }
    let (q, r) = div_rem_u256_u128(hi, lo, divisor)?;
    let q = if r >= divisor - r { q.checked_add(1)? } else { q };
    let q = i128::try_from(q).ok()?;
    Some((if negative { -q } else { q }, r == 0))
}

/// `(hi:lo) · f`, `None` past 256 bits.
fn mul_u256_u128(hi: u128, lo: u128, f: u128) -> Option<(u128, u128)> {
    let (lo_hi, lo_lo) = widening_mul(lo, f);
    let (hi_hi, hi_lo) = widening_mul(hi, f);
    if hi_hi != 0 {
        return None;
    }
    Some((lo_hi.checked_add(hi_lo)?, lo_lo))
}

/// The 256-bit product of two `u128`s, as (high, low).
fn widening_mul(a: u128, b: u128) -> (u128, u128) {
    const MASK: u128 = u64::MAX as u128;
    let (a_hi, a_lo) = (a >> 64, a & MASK);
    let (b_hi, b_lo) = (b >> 64, b & MASK);
    let ll = a_lo * b_lo;
    let lh = a_lo * b_hi;
    let hl = a_hi * b_lo;
    let hh = a_hi * b_hi;
    let mid = (ll >> 64) + (lh & MASK) + (hl & MASK);
    let lo = (ll & MASK) | (mid << 64);
    let hi = hh + (lh >> 64) + (hl >> 64) + (mid >> 64);
    (hi, lo)
}

/// `(hi:lo) / d` and its remainder, for `d` below 2^127 (any `i128`
/// magnitude); `None` when the quotient needs more than 128 bits.
fn div_rem_u256_u128(hi: u128, lo: u128, d: u128) -> Option<(u128, u128)> {
    debug_assert!(d != 0 && d <= 1u128 << 127);
    if hi == 0 {
        return Some((lo / d, lo % d));
    }
    if hi >= d {
        return None;
    }
    let mut rem = hi;
    let mut q: u128 = 0;
    for bit in (0..128).rev() {
        rem = (rem << 1) | ((lo >> bit) & 1);
        q <<= 1;
        if rem >= d {
            rem -= d;
            q |= 1;
        }
    }
    Some((q, rem))
}

/// A written number's value and display scale, as `numeric_in` reads it:
/// `1.50` is two places, `1.5e3` none, `1e-10` ten. `None` for text that is
/// not a plain decimal or exponent number, or past an `i128`.
pub(crate) fn parse_text(text: &str) -> Option<PgNum> {
    let (m, e) = crate::core::rql::expr::decimal_digits(text.trim())?;
    if e >= 0 {
        Some(PgNum { m: m.checked_mul(pow10(e as u32)?)?, s: 0 })
    } else {
        PgNum::new(m, e.unsigned_abs())
    }
}

/// `float8_numeric`: the float printed with fifteen significant digits
/// (`%.15g`, `DBL_DIG`) and read back as a written number, so `0.1::float8`
/// becomes `0.1`, not the binary fraction's 55 digits. `None` for NaN or an
/// infinity, or past an `i128`.
pub(crate) fn from_f64(v: f64) -> Option<PgNum> {
    if !v.is_finite() {
        return None;
    }
    // `{:.14e}` is fifteen significant digits; `%g` then drops the zeros at
    // the end of the mantissa, which `parse_text`'s scale does not count.
    let shown = format!("{:.14e}", v);
    let (mantissa, exponent) = shown.split_once('e')?;
    let mantissa = if mantissa.contains('.') { mantissa.trim_end_matches('0').trim_end_matches('.') } else { mantissa };
    parse_text(&format!("{mantissa}e{exponent}"))
}

/// `float4_numeric`: the same with six significant digits (`FLT_DIG`).
pub(crate) fn from_f32(v: f32) -> Option<PgNum> {
    if !v.is_finite() {
        return None;
    }
    let shown = format!("{:.5e}", v);
    let (mantissa, exponent) = shown.split_once('e')?;
    let mantissa = if mantissa.contains('.') { mantissa.trim_end_matches('0').trim_end_matches('.') } else { mantissa };
    parse_text(&format!("{mantissa}e{exponent}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(text: &str) -> PgNum {
        parse_text(text).unwrap_or_else(|| panic!("{text}"))
    }

    /// `(a, b, PostgreSQL 17's a / b)`.
    const QUOTIENTS: &[(&str, &str, &str)] = &[
        ("1.0", "3", "0.33333333333333333333"),
        ("1", "3.0", "0.33333333333333333333"),
        ("10.0", "4", "2.5000000000000000"),
        ("2.0", "3.0", "0.66666666666666666667"),
        ("100.00", "7", "14.2857142857142857"),
        ("7", "2.0", "3.5000000000000000"),
        ("1", "7.0", "0.14285714285714285714"),
        ("123456789.0", "7", "17636684.142857142857"),
        ("0.000001", "3", "0.000000333333333333333333"),
        ("2.50", "3", "0.83333333333333333333"),
        ("10.00", "3", "3.3333333333333333"),
        ("2.50", "1.2345", "2.0251113811259619"),
        ("3.14159", "3", "1.04719666666666666667"),
        ("0.123456789123", "3", "0.04115226304100000000"),
        ("1.000000000001", "3", "0.33333333333366666667"),
        ("0", "3", "0.00000000000000000000"),
        ("0.000", "7.5", "0.00000000000000000000"),
        ("-1.0", "3", "-0.33333333333333333333"),
        ("1.0", "-3", "-0.33333333333333333333"),
        ("-2.0", "-3.0", "0.66666666666666666667"),
        ("10000", "1", "10000.0000000000000000"),
        ("9999", "1", "9999.0000000000000000"),
        ("1", "10000", "0.000100000000000000000000"),
        ("5", "0.001", "5000.0000000000000000"),
        ("2", "3", "0.66666666666666666667"),
    ];

    #[test]
    fn division_takes_postgresqls_result_scale() {
        for (a, b, pg) in QUOTIENTS {
            let q = n(a).div(n(b)).unwrap().unwrap();
            assert_eq!(q.to_text(), *pg, "{a} / {b}");
        }
    }

    /// `(a, b, PostgreSQL 17's a * b)`.
    const PRODUCTS: &[(&str, &str, &str)] = &[
        ("1.5", "2.25", "3.375"),
        ("0.00001", "0.00001", "0.0000000001"),
        ("1.23456", "1.23456", "1.5241383936"),
        ("0.1234567", "0.1234567", "0.01524155677489"),
        ("2.50", "2", "5.00"),
        ("2.50", "1.2345", "3.086250"),
        ("-1.5", "2.0", "-3.00"),
    ];

    #[test]
    fn a_product_is_exact_at_the_sum_of_the_scales() {
        for (a, b, pg) in PRODUCTS {
            assert_eq!(n(a).mul(n(b)).unwrap().to_text(), *pg, "{a} * {b}");
        }
    }

    #[test]
    fn sums_differences_and_remainders_take_the_larger_scale() {
        assert_eq!(n("1.50").add(n("2.5")).unwrap().to_text(), "4.00");
        assert_eq!(n("2.50").sub(n("0.001")).unwrap().to_text(), "2.499");
        // PostgreSQL 17: 2.50 % 3 = 2.50, -7.5 % 2 = -1.5, 10.00 % 3 = 1.00.
        assert_eq!(n("2.50").rem(n("3")).unwrap().unwrap().to_text(), "2.50");
        assert_eq!(n("-7.5").rem(n("2")).unwrap().unwrap().to_text(), "-1.5");
        assert_eq!(n("10.00").rem(n("3")).unwrap().unwrap().to_text(), "1.00");
        assert_eq!(n("1").div(n("0")), Err(DIVISION_BY_ZERO));
        assert_eq!(n("1").rem(n("0.0")), Err(DIVISION_BY_ZERO));
    }

    #[test]
    fn written_numbers_keep_the_places_they_were_written_with() {
        for (text, pg) in [("1.50", "1.50"), ("1.5e3", "1500"), ("1e-10", "0.0000000001"), ("100e-2", "1.00"), (".5", "0.5")] {
            assert_eq!(n(text).to_text(), pg, "{text}");
        }
        assert_eq!(PgNum::trimmed(1_500_000_000, 9).to_text(), "1.5");
        assert_eq!(PgNum::trimmed(0, 9).to_text(), "0");
    }

    #[test]
    fn past_38_places_only_an_exact_value_is_answered() {
        // PostgreSQL 17: 0.000000001 / 100000000000 = 0.00000000000000000001000000000000000000
        // (38 places shown here, the value exact); 1 / 3e30 has no end.
        assert_eq!(n("0.000000001").div(n("100000000000")).unwrap().unwrap().to_text(), "0.00000000000000000001000000000000000000");
        assert_eq!(n("1").div(n("3000000000000000000000000000000")).unwrap(), None);
        // A product past 38 places whose own digits fit: exact.
        assert_eq!(
            n("1.50000000000000000000").mul(n("2.50000000000000000000")).unwrap().to_text(),
            "3.7500000000000000000000000000000000000"
        );
        assert_eq!(n("0.00000000000000000001").mul(n("0.0000000000000000000003")).map(|v| v.to_text()), None);
    }

    #[test]
    fn rounding_is_half_away_from_zero() {
        assert_eq!(n("2.345").round_to(2).unwrap().to_text(), "2.35");
        assert_eq!(n("-2.345").round_to(2).unwrap().to_text(), "-2.35");
        assert_eq!(n("2.344").round_to(2).unwrap().to_text(), "2.34");
        assert_eq!(n("2.5").round_to(3).unwrap().to_text(), "2.500");
    }

    #[test]
    fn a_float_becomes_its_fifteen_digit_text() {
        // PostgreSQL 17: 0.1::float8::numeric = 0.1, (1/3::float8)::numeric =
        // 0.333333333333333, 1e20::float8::numeric = 100000000000000000000.
        assert_eq!(from_f64(0.1).unwrap().to_text(), "0.1");
        assert_eq!(from_f64(1.0 / 3.0).unwrap().to_text(), "0.333333333333333");
        assert_eq!(from_f64(1e20).unwrap().to_text(), "100000000000000000000");
        assert_eq!(from_f64(2.5).unwrap().to_text(), "2.5");
        assert_eq!(from_f32(0.1).unwrap().to_text(), "0.1");
    }
}
