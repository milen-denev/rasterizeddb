//! The range functions and operators over two whole ranges: the bounds
//! (`lower`, `upper`, `isempty`, `lower_inc`, …), the constructors
//! (`int4range(1, 5, '[]')`), the set operations (`*`, `+`, `-`,
//! `range_merge`) and the positional tests (`<<`, `>>`, `&<`, `&>`,
//! `-|-`), each as PostgreSQL's `rangetypes.c` computes it.
//!
//! A range is held in PostgreSQL's binary form (`pg_types::parse_range`):
//! a flag byte, then each finite bound as the element's wire bytes. Bounds
//! are compared by `pg_types::range_element_key`, with PostgreSQL's rule
//! for equal values (`range_cmp_bounds`): an inclusive lower bound sorts
//! before an exclusive one, an exclusive upper before an inclusive one.

use std::cmp::Ordering;

use crate::core::db_type::DbType;
use crate::core::pg_types::{decode_range, range_element, range_element_key, range_flags};

/// The message PostgreSQL gives a range whose lower bound is above its upper.
pub const CROSSED_BOUNDS: &str = "range lower bound must be less than or equal to range upper bound";

#[derive(Clone, Debug)]
struct Bound {
    /// The element's wire bytes and their key; `None` is infinite.
    value: Option<(Vec<u8>, i128)>,
    inclusive: bool,
    lower: bool,
}

#[derive(Clone, Debug)]
struct Range {
    empty: bool,
    lo: Bound,
    hi: Bound,
}

fn read(db_type: &DbType, bytes: &[u8]) -> Option<Range> {
    let element = range_element(db_type)?;
    let r = decode_range(db_type, bytes)?;
    let bound = |b: &Option<Vec<u8>>, inclusive: bool, lower: bool| -> Option<Bound> {
        Some(Bound {
            value: match b {
                Some(v) => Some((v.clone(), range_element_key(&element, v)?)),
                None => None,
            },
            inclusive,
            lower,
        })
    };
    Some(Range {
        empty: r.empty,
        lo: bound(&r.lower, r.lower_inclusive, true)?,
        hi: bound(&r.upper, r.upper_inclusive, false)?,
    })
}

/// PostgreSQL's `range_cmp_bounds`.
fn cmp(a: &Bound, b: &Bound) -> Ordering {
    let side = |lower: bool| if lower { Ordering::Less } else { Ordering::Greater };
    match (&a.value, &b.value) {
        (None, None) if a.lower == b.lower => Ordering::Equal,
        (None, None) | (None, Some(_)) => side(a.lower),
        (Some(_), None) => side(b.lower).reverse(),
        (Some((_, x)), Some((_, y))) => match x.cmp(y) {
            Ordering::Equal => match (a.inclusive, b.inclusive) {
                (false, false) if a.lower == b.lower => Ordering::Equal,
                (false, _) => side(a.lower).reverse(),
                (true, false) => side(b.lower),
                (true, true) => Ordering::Equal,
            },
            other => other,
        },
    }
}

/// `upper` of one range directly followed by `lower` of another, nothing
/// between: equal values, one bound inclusive and the other not. (A
/// discrete range is stored `[)`, so its bounds meet only so.)
fn bounds_adjacent(upper: &Bound, lower: &Bound) -> bool {
    match (&upper.value, &lower.value) {
        (Some((_, x)), Some((_, y))) => x == y && upper.inclusive != lower.inclusive,
        _ => false,
    }
}

fn overlaps(a: &Range, b: &Range) -> bool {
    !a.empty
        && !b.empty
        && ((cmp(&a.lo, &b.lo) != Ordering::Less && cmp(&a.lo, &b.hi) != Ordering::Greater)
            || (cmp(&b.lo, &a.lo) != Ordering::Less && cmp(&b.lo, &a.hi) != Ordering::Greater))
}

fn adjacent(a: &Range, b: &Range) -> bool {
    !a.empty && !b.empty && (bounds_adjacent(&a.hi, &b.lo) || bounds_adjacent(&b.hi, &a.lo))
}

fn empty_range() -> Vec<u8> {
    vec![range_flags::EMPTY]
}

/// A range from two bounds, canonical as an input of it would be.
fn make(db_type: &DbType, lo: &Bound, hi: &Bound) -> Option<Vec<u8>> {
    let element = range_element(db_type)?;
    let mut lo_value = lo.value.as_ref().map(|(v, _)| v.clone());
    let mut hi_value = hi.value.as_ref().map(|(v, _)| v.clone());
    let mut lo_inc = lo.inclusive && lo_value.is_some();
    let mut hi_inc = hi.inclusive && hi_value.is_some();
    if crate::core::pg_types::range_is_discrete(db_type) {
        if !lo_inc && let Some(v) = &lo_value {
            lo_value = Some(crate::core::pg_types::step_element(&element, v, 1)?);
            lo_inc = true;
        }
        if hi_inc && let Some(v) = &hi_value {
            hi_value = Some(crate::core::pg_types::step_element(&element, v, 1)?);
            hi_inc = false;
        }
    }
    if let (Some(l), Some(h)) = (&lo_value, &hi_value) {
        let (l, h) = (range_element_key(&element, l)?, range_element_key(&element, h)?);
        if l > h || (l == h && !(lo_inc && hi_inc)) {
            return Some(empty_range());
        }
    }
    let mut flags = 0u8;
    if lo_inc {
        flags |= range_flags::LB_INC;
    }
    if hi_inc {
        flags |= range_flags::UB_INC;
    }
    if lo_value.is_none() {
        flags |= range_flags::LB_INF;
    }
    if hi_value.is_none() {
        flags |= range_flags::UB_INF;
    }
    let mut out = vec![flags];
    for v in [&lo_value, &hi_value].into_iter().flatten() {
        out.extend_from_slice(&(v.len() as i32).to_be_bytes());
        out.extend_from_slice(v);
    }
    Some(out)
}

fn flipped(b: &Bound) -> Bound {
    Bound { value: b.value.clone(), inclusive: !b.inclusive, lower: !b.lower }
}

/// A set operation over two ranges of one type: `*`, `+`, `-` or
/// `range_merge`. `Err` with PostgreSQL's message (22000) for a union or
/// difference that would not be one range.
pub fn set_operation(op: &str, db_type: &DbType, a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    let bad = || format!("malformed range value of type {}", crate::core::value_codec::pg_input_type_name(db_type));
    let (x, y) = (read(db_type, a).ok_or_else(bad)?, read(db_type, b).ok_or_else(bad)?);
    let min = |p: &Bound, q: &Bound| if cmp(p, q) == Ordering::Greater { q.clone() } else { p.clone() };
    let max = |p: &Bound, q: &Bound| if cmp(p, q) == Ordering::Less { q.clone() } else { p.clone() };
    let made = |lo: &Bound, hi: &Bound| make(db_type, lo, hi).ok_or_else(bad);
    match op {
        "*" => {
            if !overlaps(&x, &y) {
                return Ok(empty_range());
            }
            made(&max(&x.lo, &y.lo), &min(&x.hi, &y.hi))
        }
        "+" | "range_merge" => {
            if x.empty {
                return Ok(b.to_vec());
            }
            if y.empty {
                return Ok(a.to_vec());
            }
            if op == "+" && !overlaps(&x, &y) && !adjacent(&x, &y) {
                return Err("result of range union would not be contiguous".to_string());
            }
            made(&min(&x.lo, &y.lo), &max(&x.hi, &y.hi))
        }
        "-" => {
            if x.empty || y.empty {
                return Ok(a.to_vec());
            }
            let l1l2 = cmp(&x.lo, &y.lo);
            let l1u2 = cmp(&x.lo, &y.hi);
            let u1l2 = cmp(&x.hi, &y.lo);
            let u1u2 = cmp(&x.hi, &y.hi);
            if l1l2 == Ordering::Less && u1u2 == Ordering::Greater {
                return Err("result of range difference would not be contiguous".to_string());
            }
            if l1u2 == Ordering::Greater || u1l2 == Ordering::Less {
                return Ok(a.to_vec());
            }
            if l1l2 != Ordering::Less && u1u2 != Ordering::Greater {
                return Ok(empty_range());
            }
            if l1l2 != Ordering::Greater && u1l2 != Ordering::Less && u1u2 != Ordering::Greater {
                return made(&x.lo, &flipped(&y.lo));
            }
            if l1l2 != Ordering::Less && u1u2 != Ordering::Less && l1u2 != Ordering::Greater {
                return made(&flipped(&y.hi), &x.hi);
            }
            Err(bad())
        }
        _ => Err(bad()),
    }
}

/// A positional test over two ranges of one type: `<<`, `>>`, `&<`, `&>`,
/// `-|-`. An empty range is in no position (`false`).
pub fn position(op: &str, db_type: &DbType, a: &[u8], b: &[u8]) -> Option<bool> {
    let (x, y) = (read(db_type, a)?, read(db_type, b)?);
    if x.empty || y.empty {
        return Some(false);
    }
    Some(match op {
        "<<" => cmp(&x.hi, &y.lo) == Ordering::Less,
        ">>" => cmp(&x.lo, &y.hi) == Ordering::Greater,
        "&<" => cmp(&x.hi, &y.hi) != Ordering::Greater,
        "&>" => cmp(&x.lo, &y.lo) != Ordering::Less,
        "-|-" => adjacent(&x, &y),
        _ => return None,
    })
}

/// What a bound accessor answers: `isempty`, `lower_inc`, `upper_inc`,
/// `lower_inf`, `upper_inf`.
pub fn bound_test(func: &str, db_type: &DbType, bytes: &[u8]) -> Option<bool> {
    let r = read(db_type, bytes)?;
    Some(match func {
        "isempty" => r.empty,
        "lower_inc" => !r.empty && r.lo.inclusive,
        "upper_inc" => !r.empty && r.hi.inclusive,
        "lower_inf" => !r.empty && r.lo.value.is_none(),
        "upper_inf" => !r.empty && r.hi.value.is_none(),
        _ => return None,
    })
}

/// `lower(r)` / `upper(r)`: the bound's text as the element type prints
/// it, `None` for an empty range or an infinite bound (SQL NULL).
pub fn bound_text(db_type: &DbType, bytes: &[u8], upper: bool) -> Option<String> {
    let element = range_element(db_type)?;
    let r = read(db_type, bytes)?;
    if r.empty {
        return None;
    }
    let (value, _) = if upper { r.hi.value? } else { r.lo.value? };
    let text = crate::core::pg_types::format_range_element(&element, &value)?;
    Some(text.trim_matches('"').to_string())
}

/// `int4range(lo, hi [, bounds])` and its kin: the range of the two bounds'
/// texts (`None` an infinite bound), `bounds` `[]`, `[)`, `(]` or `()`.
pub fn construct(db_type: &DbType, lo: Option<&str>, hi: Option<&str>, bounds: &str) -> Result<Vec<u8>, String> {
    let element = range_element(db_type).ok_or("not a range type")?;
    let (lo_inc, hi_inc) = match bounds {
        "[]" => (true, true),
        "[)" => (true, false),
        "(]" => (false, true),
        "()" => (false, false),
        _ => return Err("invalid range bound flags".to_string()),
    };
    let name = crate::core::value_codec::pg_input_type_name(&element);
    let bound = |text: Option<&str>, lower: bool, inclusive: bool| -> Result<Bound, String> {
        let value = match text {
            None => None,
            Some(t) => {
                let bytes = crate::core::pg_types::encode_range_element(&element, t).ok_or_else(|| {
                    format!("invalid input syntax for type {}: {}", name, crate::core::value_codec::double_quoted(t))
                })?;
                let key = range_element_key(&element, &bytes).ok_or("malformed range bound")?;
                Some((bytes, key))
            }
        };
        Ok(Bound { value, inclusive, lower })
    };
    let lo = bound(lo, true, lo_inc)?;
    let hi = bound(hi, false, hi_inc)?;
    if let (Some((_, l)), Some((_, h))) = (&lo.value, &hi.value)
        && l > h
    {
        return Err(CROSSED_BOUNDS.to_string());
    }
    make(db_type, &lo, &hi).ok_or_else(|| "malformed range value".to_string())
}

/// Whether a range literal's bounds cross (`[5,1)`), which PostgreSQL
/// refuses where the engine's reader would store an empty range.
pub fn literal_bounds_cross(db_type: &DbType, text: &str) -> bool {
    let Some(element) = range_element(db_type) else { return false };
    let text = text.trim();
    if text.len() < 2 || !matches!(text.as_bytes()[0], b'[' | b'(') {
        return false;
    }
    let body = &text[1..text.len() - 1];
    let parts = crate::core::pg_types::split_range_body(body);
    let [lo, hi] = parts.as_slice() else { return false };
    let key = |t: &str| {
        let t = t.trim();
        (!t.is_empty())
            .then(|| crate::core::pg_types::encode_range_element(&element, t))
            .flatten()
            .and_then(|b| range_element_key(&element, &b))
    };
    matches!((key(lo), key(hi)), (Some(l), Some(h)) if l > h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::pg_types::{format_range, parse_range};

    fn r(t: &DbType, text: &str) -> Vec<u8> {
        parse_range(t, text).unwrap_or_else(|| panic!("{text}"))
    }

    fn set(op: &str, t: &DbType, a: &str, b: &str) -> Result<String, String> {
        set_operation(op, t, &r(t, a), &r(t, b)).map(|bytes| format_range(t, &bytes).expect("formats"))
    }

    #[test]
    fn set_operations_are_postgresql_s() {
        let i = DbType::INT4RANGE;
        assert_eq!(set("*", &i, "[1,5)", "[4,9)").as_deref(), Ok("[4,5)"));
        assert_eq!(set("*", &i, "[1,5)", "[6,9)").as_deref(), Ok("empty"));
        assert_eq!(set("+", &i, "[1,5)", "[4,9)").as_deref(), Ok("[1,9)"));
        assert_eq!(set("+", &i, "[1,5)", "[5,9)").as_deref(), Ok("[1,9)"));
        assert_eq!(set("+", &i, "empty", "[5,9)").as_deref(), Ok("[5,9)"));
        assert_eq!(set("+", &i, "[1,5)", "[7,9)"), Err("result of range union would not be contiguous".to_string()));
        assert_eq!(set("range_merge", &i, "[1,2)", "[5,6)").as_deref(), Ok("[1,6)"));
        assert_eq!(set("-", &i, "[1,5)", "[4,9)").as_deref(), Ok("[1,4)"));
        assert_eq!(set("-", &i, "[4,9)", "[1,5)").as_deref(), Ok("[5,9)"));
        assert_eq!(set("-", &i, "[4,5)", "[1,9)").as_deref(), Ok("empty"));
        assert_eq!(set("-", &i, "[1,9)", "[4,5)"), Err("result of range difference would not be contiguous".to_string()));
        assert_eq!(set("-", &i, "(,5)", "[3,)").as_deref(), Ok("(,3)"));
        let n = DbType::NUMRANGE;
        assert_eq!(set("*", &n, "[1.5,2.5)", "[2,3]").as_deref(), Ok("[2,2.5)"));
        // A continuous range keeps the bound a difference leaves open.
        assert_eq!(set("-", &n, "[1,3]", "[2,3]").as_deref(), Ok("[1,2)"));
        assert_eq!(set("-", &n, "[1,3]", "(2,4]").as_deref(), Ok("[1,2]"));
    }

    #[test]
    fn positions_and_bounds() {
        let i = DbType::INT4RANGE;
        let n = DbType::NUMRANGE;
        let pos = |op: &str, t: &DbType, a: &str, b: &str| position(op, t, &r(t, a), &r(t, b)).expect("answers");
        assert!(pos("<<", &i, "[1,5)", "[5,9)") && !pos("<<", &i, "[1,6)", "[5,9)"));
        assert!(pos(">>", &i, "[5,9)", "[1,5)"));
        assert!(pos("&<", &i, "[1,5)", "[4,5)") && !pos("&<", &i, "[1,6)", "[4,5)"));
        assert!(pos("&>", &i, "[4,9)", "[1,5)") && !pos("&>", &i, "[0,9)", "[1,5)"));
        assert!(pos("-|-", &i, "[1,5)", "[5,9)") && pos("-|-", &i, "[5,9)", "[1,5)") && !pos("-|-", &i, "[1,5)", "[6,9)"));
        assert!(pos("-|-", &n, "[1,2.5)", "[2.5,3)") && !pos("-|-", &n, "[1,2.5]", "[2.5,3)"));
        assert!(!pos("<<", &i, "empty", "[1,2)"));
        let test = |f: &str, text: &str| bound_test(f, &i, &r(&i, text)).expect("answers");
        assert!(test("isempty", "[1,1)") && test("lower_inc", "[1,5)") && !test("upper_inc", "[1,5]"));
        assert!(test("lower_inf", "(,5)") && test("upper_inf", "[1,)") && !test("lower_inf", "empty"));
        assert_eq!(bound_text(&i, &r(&i, "[1,5]"), true).as_deref(), Some("6"));
        assert_eq!(bound_text(&i, &r(&i, "(,5)"), false), None);
        assert_eq!(bound_text(&i, &r(&i, "empty"), false), None);
    }

    #[test]
    fn constructors_and_crossed_bounds() {
        let i = DbType::INT4RANGE;
        let made = |lo, hi, f| construct(&i, lo, hi, f).map(|b| format_range(&i, &b).expect("formats"));
        assert_eq!(made(Some("1"), Some("5"), "[]").as_deref(), Ok("[1,6)"));
        assert_eq!(made(Some("1"), Some("5"), "()").as_deref(), Ok("[2,5)"));
        assert_eq!(made(None, Some("5"), "[)").as_deref(), Ok("(,5)"));
        assert_eq!(made(Some("5"), Some("5"), "[)").as_deref(), Ok("empty"));
        assert_eq!(made(Some("5"), Some("1"), "[)"), Err(CROSSED_BOUNDS.to_string()));
        assert_eq!(made(Some("1"), Some("5"), "xx"), Err("invalid range bound flags".to_string()));
        assert!(literal_bounds_cross(&i, "[5,1)") && !literal_bounds_cross(&i, "[5,5)") && !literal_bounds_cross(&i, "(,1)"));
        assert_eq!(parse_range(&i, "[5,1)"), None);
        assert_eq!(parse_range(&i, "(5,5]"), Some(vec![range_flags::EMPTY]));
    }
}
