//! PostgreSQL's `numeric` `exp`, `ln`, `log` and `power`, result scale and all.
//!
//! Each function picks its result's display scale the way numeric.c does —
//! enough places for sixteen significant digits (`NUMERIC_MIN_SIG_DIGITS`),
//! never fewer than an argument's display scale — from an *estimate* of the
//! result's decimal weight that numeric.c makes in double precision or from the
//! argument's leading base-10000 digits. The estimate is reproduced exactly,
//! truncations included, because it decides how many places are shown: `exp(1.0)`
//! is `2.7182818284590452`, sixteen places, and `exp(100.0)` one.
//!
//! The value is then computed in fixed point at that scale plus guard digits
//! (`GUARD`) and rounded half away from zero, as `round_var` does. The guard
//! digits make the result the correctly rounded one except where the true
//! value lies within `10^-GUARD` units of a rounding boundary.
//!
//! A [`PgNum`] is an `i128` with at most 38 places. A result PostgreSQL would
//! show with more places (`exp(-100.0)` has 59), or with more than 38 digits
//! in all (`exp(100.0)`), is refused as not supported rather than rounded to
//! fit: the places are the value's display, and a different display is a
//! different answer.

use num_bigint::BigInt;
use num_traits::{One, Signed, ToPrimitive, Zero};

use super::pg_numeric::{MAX_SCALE, PgNum};

/// `NUMERIC_MIN_SIG_DIGITS`.
const MIN_SIG_DIGITS: i64 = 16;
/// `NUMERIC_MAX_DISPLAY_SCALE`.
const MAX_DISPLAY_SCALE: i64 = 1000;
/// `NUMERIC_MAX_RESULT_SCALE`.
const MAX_RESULT_SCALE: f64 = 2000.0;
/// `NUMERIC_WEIGHT_MAX`: the largest base-10000 weight a `numeric` has.
const WEIGHT_MAX: f64 = 32767.0;
/// Decimal digits computed past the result's scale.
const GUARD: i64 = 16;
/// An exponent this large or smaller is raised exactly, as a power of the
/// argument's integer; past it, through `exp(n · ln(x))` at a working
/// precision wide enough for the result.
const EXACT_POWER_DIGITS: u64 = 4000;

pub(crate) const LOG_OF_ZERO: &str = "cannot take logarithm of zero";
pub(crate) const LOG_OF_NEGATIVE: &str = "cannot take logarithm of a negative number";
pub(crate) const ZERO_TO_NEGATIVE: &str = "zero raised to a negative power is undefined";
pub(crate) const NEGATIVE_TO_FRACTION: &str =
    "a negative number raised to a non-integer power yields a complex result";
pub(crate) const OVERFLOW: &str = "value overflows numeric format";
const DIVISION_BY_ZERO: &str = "division by zero";

/// A result PostgreSQL shows but a [`PgNum`] cannot hold.
fn too_wide(func: &str) -> String {
    format!(
        "{func}(numeric) results with more than {MAX_SCALE} digits or places are not supported yet: \
         cast the argument to double precision"
    )
}

fn pow10(n: i64) -> BigInt {
    BigInt::from(10u32).pow(u32::try_from(n).expect("a working scale is positive and small"))
}

/// `a / b` rounded half away from zero; `b` must not be zero.
fn div_round(a: &BigInt, b: &BigInt) -> BigInt {
    let (a, b) = if b.is_negative() { (-a, -b) } else { (a.clone(), b.clone()) };
    let (q, r) = (&a / &b, &a % &b);
    if r.abs() * 2u32 >= b {
        q + a.signum()
    } else {
        q
    }
}

/// `x` as an unscaled integer at `w` places; exact when `w >= x.s`, which every
/// caller arranges.
fn fixed(x: PgNum, w: i64) -> BigInt {
    let m = BigInt::from(x.m);
    let s = i64::from(x.s);
    if w >= s { m * pow10(w - s) } else { div_round(&m, &pow10(s - w)) }
}

/// `x` as numeric.c's `numericvar_to_double_no_overflow` reads it: its text,
/// parsed.
fn to_f64(x: PgNum) -> f64 {
    x.to_text().parse().unwrap_or(0.0)
}

/// A value at `w` places, rounded to `rscale` and handed back as a [`PgNum`],
/// or the refusal for one that does not fit.
fn finish(v: &BigInt, w: i64, rscale: i64, func: &str) -> Result<PgNum, String> {
    let m = div_round(v, &pow10(w - rscale));
    let m = m.to_i128().filter(|m| m.unsigned_abs() < 10u128.pow(38)).ok_or_else(|| too_wide(func))?;
    PgNum::new(m, rscale as u32).ok_or_else(|| too_wide(func))
}

/// A result scale clamped as every numeric.c function clamps it, and refused
/// past what a [`PgNum`] carries.
fn result_scale(rscale: i64, func: &str) -> Result<i64, String> {
    let rscale = rscale.clamp(0, MAX_DISPLAY_SCALE);
    if rscale > i64::from(MAX_SCALE) {
        return Err(too_wide(func));
    }
    Ok(rscale)
}

/// The base-10000 digits a `NumericVar` holds for `x`: its weight (the power
/// of 10000 of the first digit) and its digits, leading and trailing zero
/// digits stripped (`strip_var`). Zero has no digits and weight 0.
struct Digits {
    weight: i64,
    digits: Vec<u32>,
}

fn digits(x: PgNum) -> Digits {
    let s = x.s as usize;
    let mut text = x.m.unsigned_abs().to_string();
    if text.len() <= s {
        text = "0".repeat(s + 1 - text.len()) + &text;
    }
    let (int, frac) = text.split_at(text.len() - s);
    let int = "0".repeat((4 - int.len() % 4) % 4) + int;
    let frac = frac.to_string() + &"0".repeat((4 - frac.len() % 4) % 4);
    let mut weight = (int.len() / 4) as i64 - 1;
    let mut groups: Vec<u32> = (int + &frac)
        .as_bytes()
        .chunks(4)
        .map(|g| g.iter().fold(0u32, |n, d| n * 10 + u32::from(d - b'0')))
        .collect();
    let leading = groups.iter().take_while(|g| **g == 0).count();
    groups.drain(..leading);
    weight -= leading as i64;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }
    Digits { weight, digits: groups }
}

/// The decimal exponent of `x`'s leading digit: `floor(log10(|x|))`, exactly.
/// `x` must not be zero.
fn leading_exponent(x: PgNum) -> i64 {
    x.m.unsigned_abs().to_string().len() as i64 - 1 - i64::from(x.s)
}

/// `estimate_ln_dweight`: numeric.c's estimate of `ln(x)`'s decimal weight.
fn estimate_ln_dweight(x: PgNum) -> i64 {
    if x.m <= 0 {
        return 0;
    }
    let (m, s) = (BigInt::from(x.m), i64::from(x.s));
    let scaled = |n: u32| BigInt::from(n) * pow10(s);
    // 0.9 <= x <= 1.1: ln(x) is about x - 1, whose weight it takes.
    if &m * 10u32 >= scaled(9) && &m * 10u32 <= scaled(11) {
        let one = PgNum::new(10i128.pow(x.s), x.s).expect("x.s is a PgNum's scale");
        let d = x.sub(one).expect("within 0.1 of one");
        return if d.m == 0 { 0 } else { leading_exponent(d) };
    }
    let v = digits(x);
    let mut leading = f64::from(v.digits[0]);
    let mut dweight = v.weight * 4;
    if v.digits.len() > 1 {
        leading = leading * 10000.0 + f64::from(v.digits[1]);
        dweight -= 4;
    }
    let ln = leading.ln() + dweight as f64 * 2.302585092994046;
    ln.abs().log10() as i64
}

/// `ln(2)` at `w` places: `2·atanh(1/3)`.
fn ln2(w: i64) -> BigInt {
    let f = pow10(w);
    let mut term = &f / 3u32;
    let mut sum = term.clone();
    let mut k = 1u32;
    loop {
        term /= 9u32;
        if term.is_zero() {
            break;
        }
        sum += &term / (2 * k + 1);
        k += 1;
    }
    sum * 2u32
}

/// `ln(x)` at `w` places, for `x > 0` at `w` places: `x = 2^j · y` with `y`
/// within a third of one, and `ln(y) = 2·atanh((y-1)/(y+1))`.
fn ln_fixed(x: &BigInt, w: i64) -> BigInt {
    let f = pow10(w);
    let shifted = |j: i64| if j >= 0 { x >> (j as usize) } else { x << ((-j) as usize) };
    let mut j = x.bits() as i64 - f.bits() as i64;
    let mut y = shifted(j);
    let (low, high) = (&f * 2u32 / 3u32, &f * 4u32 / 3u32);
    while y > high {
        j += 1;
        y = shifted(j);
    }
    while y < low {
        j -= 1;
        y = shifted(j);
    }
    let z = div_round(&((&y - &f) * &f), &(&y + &f));
    let z2 = div_round(&(&z * &z), &f);
    let mut term = z.clone();
    let mut sum = z;
    let mut k = 1u32;
    loop {
        term = div_round(&(&term * &z2), &f);
        if term.is_zero() {
            break;
        }
        sum += &term / (2 * k + 1);
        k += 1;
    }
    sum * 2u32 + ln2(w) * j
}

/// `e^x` at `w` places, for `x` at `w` places: `x = n·ln(2) + r`, `e^r` by its
/// series over `r / 2^10` squared back ten times, shifted by `n`.
fn exp_fixed(x: &BigInt, w: i64) -> BigInt {
    const HALVINGS: usize = 10;
    let f = pow10(w);
    let l2 = ln2(w);
    let n = div_round(x, &l2);
    let r = div_round(&(x - &n * &l2), &(BigInt::one() << HALVINGS));
    let mut sum = f.clone();
    let mut term = f.clone();
    let mut i = 1u32;
    loop {
        term = div_round(&(&term * &r), &(&f * i));
        if term.is_zero() {
            break;
        }
        sum += &term;
        i += 1;
    }
    for _ in 0..HALVINGS {
        sum = div_round(&(&sum * &sum), &f);
    }
    let n = n.to_i64().expect("|x| is bounded by the callers' overflow tests");
    if n >= 0 {
        sum << (n as usize)
    } else {
        div_round(&sum, &(BigInt::one() << ((-n) as usize)))
    }
}

/// `exp(numeric)`.
pub(crate) fn exp(x: PgNum) -> Result<PgNum, String> {
    let val = to_f64(x);
    if val.abs() > MAX_RESULT_SCALE * 3.0 {
        // numeric.c answers a large negative argument with zero at a
        // thousand places.
        return Err(if val > 0.0 { OVERFLOW.to_string() } else { too_wide("exp") });
    }
    let weight = (val * 0.434294481903252).clamp(-MAX_RESULT_SCALE, MAX_RESULT_SCALE);
    let rscale = result_scale((MIN_SIG_DIGITS - weight as i64).max(i64::from(x.s)), "exp")?;
    if weight > 38.0 {
        return Err(too_wide("exp"));
    }
    let w = rscale + GUARD + weight.max(0.0) as i64 + 4;
    finish(&exp_fixed(&fixed(x, w), w), w, rscale, "exp")
}

fn check_log_argument(x: PgNum) -> Result<(), String> {
    match x.m.signum() {
        0 => Err(LOG_OF_ZERO.to_string()),
        -1 => Err(LOG_OF_NEGATIVE.to_string()),
        _ => Ok(()),
    }
}

/// `ln(numeric)`.
pub(crate) fn ln(x: PgNum) -> Result<PgNum, String> {
    check_log_argument(x)?;
    let rscale = result_scale((MIN_SIG_DIGITS - estimate_ln_dweight(x)).max(i64::from(x.s)), "ln")?;
    let w = rscale + GUARD + 4;
    finish(&ln_fixed(&fixed(x, w), w), w, rscale, "ln")
}

/// `log(base, x)`: `log_var`. `log(x)` and `log10(x)` are `log(10, x)`.
pub(crate) fn log(base: PgNum, x: PgNum) -> Result<PgNum, String> {
    check_log_argument(base)?;
    check_log_argument(x)?;
    let (base_weight, x_weight) = (estimate_ln_dweight(base), estimate_ln_dweight(x));
    let rscale = result_scale(
        (MIN_SIG_DIGITS - (x_weight - base_weight)).max(i64::from(base.s)).max(i64::from(x.s)),
        "log",
    )?;
    if base.m == 10i128.pow(base.s) {
        return Err(DIVISION_BY_ZERO.to_string());
    }
    // A base near one has a tiny logarithm, which magnifies the error in the
    // dividend's; and a large quotient needs places for its integer digits.
    let w = rscale + GUARD + 4 + (-base_weight).max(0) + (x_weight - base_weight).max(0);
    let (lb, lx) = (ln_fixed(&fixed(base, w), w), ln_fixed(&fixed(x, w), w));
    finish(&div_round(&(lx * pow10(w)), &lb), w, rscale, "log")
}

/// Whether `x` is a whole number, as `power_var` asks it of the exponent.
fn is_integral(x: PgNum) -> bool {
    let v = digits(x);
    v.digits.is_empty() || v.digits.len() as i64 <= v.weight + 1
}

/// `power(numeric, numeric)` and `numeric ^ numeric`.
pub(crate) fn power(base: PgNum, exponent: PgNum) -> Result<PgNum, String> {
    if base.m == 0 && exponent.m < 0 {
        return Err(ZERO_TO_NEGATIVE.to_string());
    }
    if is_integral(exponent) {
        let whole = exponent.trunc_places(0).map(|t| t.m);
        if let Some(n) = whole.and_then(|n| i32::try_from(n).ok()) {
            return power_int(base, n, i64::from(exponent.s));
        }
    }
    if base.m == 0 {
        // `power_var`: zero to a non-integer power is zero, shown at sixteen
        // places.
        return Ok(PgNum::new(0, MIN_SIG_DIGITS as u32).expect("sixteen places"));
    }
    let negative = if base.m < 0 {
        if !is_integral(exponent) {
            return Err(NEGATIVE_TO_FRACTION.to_string());
        }
        // An integer too large for an int: odd if its units digit is.
        let v = digits(exponent);
        v.digits.len() as i64 == v.weight + 1 && v.digits.last().is_some_and(|d| d & 1 == 1)
    } else {
        false
    };
    let base = PgNum { m: base.m.abs(), s: base.s };

    // The estimate of the result's weight that decides its scale, made as
    // `power_var` makes it: ln(base) to about eight significant digits, the
    // product with the exponent rounded at the same scale, then read as a
    // double.
    let ln_weight = estimate_ln_dweight(base);
    let low = (8 - ln_weight).max(0);
    let lw = low + GUARD;
    let ln_low = div_round(&ln_fixed(&fixed(base, lw), lw), &pow10(GUARD));
    let product = div_round(
        &(ln_low * BigInt::from(exponent.m)),
        &pow10(i64::from(exponent.s)),
    );
    let val = product.to_f64().unwrap_or(f64::INFINITY) / 10f64.powi(low as i32);
    if val.abs() > MAX_RESULT_SCALE * 3.01 {
        return Err(if val > 0.0 { OVERFLOW.to_string() } else { too_wide("power") });
    }
    let weight = val * 0.434294481903252;
    let rscale = result_scale(
        (MIN_SIG_DIGITS - weight as i64).max(i64::from(base.s)).max(i64::from(exponent.s)),
        "power",
    )?;
    if weight > 38.0 {
        return Err(too_wide("power"));
    }
    let exponent_digits = exponent.m.unsigned_abs().to_string().len() as i64 - i64::from(exponent.s);
    let w = rscale + GUARD + 4 + weight.max(0.0) as i64 + exponent_digits.max(0) + (-ln_weight).max(0);
    let ln_base = ln_fixed(&fixed(base, w), w);
    let scaled = div_round(&(ln_base * fixed(exponent, w)), &pow10(w));
    let v = exp_fixed(&scaled, w);
    let v = if negative { -v } else { v };
    finish(&v, w, rscale, "power")
}

/// `power_var_int`: a base raised to an exponent that fits an `int`, at the
/// scale numeric.c picks from a double-precision estimate of the result's
/// weight.
fn power_int(base: PgNum, n: i32, exponent_scale: i64) -> Result<PgNum, String> {
    let v = digits(base);
    let f = if v.digits.is_empty() {
        0.0
    } else {
        let mut f = f64::from(v.digits[0]);
        let mut p = v.weight * 4;
        for d in v.digits.iter().skip(1).take(3) {
            f = f * 10000.0 + f64::from(*d);
            p -= 4;
        }
        f64::from(n) * (f.abs().log10() + p as f64)
    };
    if f > (WEIGHT_MAX + 1.0) * 4.0 * 1.01 {
        return Err(OVERFLOW.to_string());
    }
    if f + 1.0 < -(MAX_DISPLAY_SCALE as f64) * 1.01 {
        return Err(too_wide("power"));
    }
    let rscale = result_scale(
        (MIN_SIG_DIGITS - f as i64).max(i64::from(base.s)).max(exponent_scale),
        "power",
    )?;
    if f > 38.0 {
        return Err(too_wide("power"));
    }
    let s = i64::from(base.s);
    let m = BigInt::from(base.m);
    let rs = pow10(rscale);
    let value = match n {
        // 0 ^ 0 is 1, as SQL:2003 has it.
        0 => rs,
        1 => div_round(&(m * rs), &pow10(s)),
        _ if base.m == 0 => {
            if n < 0 {
                return Err(DIVISION_BY_ZERO.to_string());
            }
            BigInt::zero()
        }
        _ if (u64::from(n.unsigned_abs())) * (base.m.unsigned_abs().to_string().len() as u64) <= EXACT_POWER_DIGITS => {
            let k = n.unsigned_abs();
            let num = m.pow(k);
            let den = pow10(s * i64::from(k));
            if n > 0 { div_round(&(num * rs), &den) } else { div_round(&(den * rs), &num) }
        }
        _ => {
            // Too many digits to raise exactly: through ln and exp, wide
            // enough for the result's integer digits and the exponent's.
            let magnitude = PgNum { m: base.m.abs(), s: base.s };
            let w = rscale + GUARD + 4 + f.max(0.0) as i64 + 10 + (-estimate_ln_dweight(magnitude)).max(0);
            let ln_base = ln_fixed(&fixed(magnitude, w), w);
            let v = exp_fixed(&(ln_base * n), w);
            let v = if base.m < 0 && n % 2 != 0 { -v } else { v };
            return finish(&v, w, rscale, "power");
        }
    };
    finish(&value, rscale, rscale, "power")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rql::pg_numeric::parse_text;

    fn n(text: &str) -> PgNum {
        parse_text(text).unwrap()
    }

    fn show(r: Result<PgNum, String>) -> String {
        match r {
            Ok(v) => v.to_text(),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    /// Every figure here is PostgreSQL 17's own answer, digits and places.
    #[test]
    fn exp_and_ln_agree_with_postgres() {
        for (arg, want) in [
            ("1.0", "2.7182818284590452"),
            ("1.50", "4.4816890703380648"),
            ("0", "1.0000000000000000"),
            ("88.0", "165163625499400185552832979626485876707.0"),
            ("1.123456789012345678901234567890", "3.075467090314884708969442161416"),
        ] {
            let got = show(exp(n(arg)));
            if want.len() > 40 {
                // 39 digits: past what a PgNum holds, so refused.
                assert!(got.contains("not supported"), "exp({arg}) = {got}");
            } else {
                assert_eq!(got, want, "exp({arg})");
            }
        }
        for (arg, want) in [
            ("2.0", "0.6931471805599453"),
            ("1", "0.0000000000000000"),
            ("1e-30", "-69.077552789821370520539743640531"),
            ("1.0000001", "0.00000009999999500000033"),
        ] {
            assert_eq!(show(ln(n(arg))), want, "ln({arg})");
        }
        assert_eq!(show(ln(n("0"))), format!("ERROR: {LOG_OF_ZERO}"));
        assert_eq!(show(ln(n("-1.5"))), format!("ERROR: {LOG_OF_NEGATIVE}"));
        assert!(show(exp(n("-100.0"))).contains("not supported"), "59 places");
        assert!(show(exp(n("100.0"))).contains("not supported"), "45 digits");
        assert_eq!(show(exp(n("7000"))), format!("ERROR: {OVERFLOW}"));
    }

    #[test]
    fn log_agrees_with_postgres() {
        for (base, arg, want) in [
            ("10", "100.0", "2.0000000000000000"),
            ("2.0", "8.0", "3.0000000000000000"),
            ("2", "8", "3.0000000000000000"),
            ("10", "0.001", "-3.0000000000000000"),
        ] {
            assert_eq!(show(log(n(base), n(arg))), want, "log({base}, {arg})");
        }
        assert_eq!(show(log(n("1"), n("10.0"))), "ERROR: division by zero");
        assert_eq!(show(log(n("0"), n("10.0"))), format!("ERROR: {LOG_OF_ZERO}"));
        assert_eq!(show(log(n("10"), n("-1"))), format!("ERROR: {LOG_OF_NEGATIVE}"));
    }

    #[test]
    fn power_agrees_with_postgres() {
        for (base, e, want) in [
            ("2.0", "10", "1024.0000000000000"),
            ("2", "0.5", "1.4142135623730950"),
            ("10.0", "-3", "0.0010000000000000000"),
            ("0.0", "0", "1.0000000000000000"),
            ("1.5", "2.5", "2.7556759606310754"),
            ("-2.0", "3", "-8.0000000000000000"),
            ("-2.0", "3.0", "-8.0000000000000000"),
            ("2.00", "100", "1267650600228229401496703205376.00"),
            ("1.0001", "10000", "2.7181459268252249"),
        ] {
            assert_eq!(show(power(n(base), n(e))), want, "power({base}, {e})");
        }
        assert_eq!(show(power(n("0.0"), n("-1"))), format!("ERROR: {ZERO_TO_NEGATIVE}"));
        assert_eq!(show(power(n("-8"), n("0.3333"))), format!("ERROR: {NEGATIVE_TO_FRACTION}"));
    }

    /// The base-10000 digits estimate_ln_dweight and power_var_int read.
    #[test]
    fn digits_are_numeric_cs() {
        let v = digits(n("12345.6789"));
        assert_eq!((v.weight, v.digits), (1, vec![1, 2345, 6789]));
        let v = digits(n("0.00012"));
        assert_eq!((v.weight, v.digits), (-1, vec![1, 2000]));
        let v = digits(n("10000"));
        assert_eq!((v.weight, v.digits), (1, vec![1]));
        assert!(digits(n("0.000")).digits.is_empty());
    }
}
