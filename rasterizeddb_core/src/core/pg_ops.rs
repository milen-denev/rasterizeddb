//! The PostgreSQL operators that ask a question about *structure* rather than
//! about order: containment, overlap, network subsumption and text-search
//! match.
//!
//! `=` and `<` mean one thing for every type. `@>` does not — it asks whether a
//! document holds another document, whether a range covers a value, whether a
//! polygon encloses a shape, whether a network subsumes an address. So these
//! live together here, dispatched on the operand's declared type, rather than
//! being folded into the ordered comparison in
//! [`crate::core::row::logical`] where every arm would have to grow a case that
//! did not apply to it.
//!
//! # What the geometric answers are, exactly
//!
//! Overlap and containment between shapes are answered on **bounding boxes**,
//! not on exact geometry. PostgreSQL's own `&&` is a bounding-box test for
//! every geometric type, so that half matches. Its `<@` is exact for some type
//! pairs, and here it is not: a polygon lying inside another's bounding box but
//! outside the polygon itself answers `true` where PostgreSQL answers `false`.
//! That is stated rather than hidden — a caller that needs exact containment
//! does not have it — and it is the same approximation an index-only scan makes
//! before its recheck.

use crate::core::db_type::DbType;
use crate::core::pg_types;

/// The structural operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgOp {
    /// `@>` — the left operand contains the right.
    Contains,
    /// `<@` — the left operand is contained by the right.
    ContainedBy,
    /// `&&` — the operands overlap.
    Overlaps,
    /// `<<` — strictly contained by, for networks.
    NetContainedBy,
    /// `<<=` — contained by or equal, for networks.
    NetContainedByOrEq,
    /// `>>` — strictly contains, for networks.
    NetContains,
    /// `>>=` — contains or equal, for networks.
    NetContainsOrEq,
    /// `@@` — a text-search vector matches a query.
    TextMatch,
}

impl PgOp {
    /// The operator a symbol names, or `None` when it is not one of these.
    pub fn from_symbol(symbol: &str) -> Option<PgOp> {
        Some(match symbol {
            "@>" => PgOp::Contains,
            "<@" => PgOp::ContainedBy,
            "&&" => PgOp::Overlaps,
            "<<" => PgOp::NetContainedBy,
            "<<=" => PgOp::NetContainedByOrEq,
            ">>" => PgOp::NetContains,
            ">>=" => PgOp::NetContainsOrEq,
            "@@" => PgOp::TextMatch,
            _ => return None,
        })
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            PgOp::Contains => "@>",
            PgOp::ContainedBy => "<@",
            PgOp::Overlaps => "&&",
            PgOp::NetContainedBy => "<<",
            PgOp::NetContainedByOrEq => "<<=",
            PgOp::NetContains => ">>",
            PgOp::NetContainsOrEq => ">>=",
            PgOp::TextMatch => "@@",
        }
    }

    /// Whether the operator, applied to this type, has an answer here.
    ///
    /// Consulted before planning so an unsupported pairing is refused with the
    /// operator and type named, rather than silently answering `false` and
    /// dropping every row — the failure mode this engine is most prone to.
    pub fn applies_to(&self, db_type: &DbType) -> bool {
        match self {
            PgOp::NetContainedBy
            | PgOp::NetContainedByOrEq
            | PgOp::NetContains
            | PgOp::NetContainsOrEq => matches!(db_type, DbType::INET),
            PgOp::TextMatch => matches!(db_type, DbType::STRING),
            PgOp::Contains | PgOp::ContainedBy | PgOp::Overlaps => matches!(
                db_type,
                DbType::JSONB
                    | DbType::JSON
                    | DbType::ARRAY
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
            ),
        }
    }
}

/// Applies a structural operator to two values of `left_type`.
///
/// `right_type` is the declared type of the right operand, which differs from
/// the left for the one shape PostgreSQL also treats specially: a range
/// compared against a single element, as in `sample_frequencies @> 100`.
///
/// `None` means the operator has no answer for these operands — a malformed
/// query rather than a false one, and the caller reports it as such.
pub fn evaluate(
    op: PgOp,
    left_type: &DbType,
    left: &[u8],
    right_type: &DbType,
    right: &[u8],
) -> Option<bool> {
    match op {
        PgOp::NetContainedBy => inet_relation(left, right, false, false),
        PgOp::NetContainedByOrEq => inet_relation(left, right, false, true),
        PgOp::NetContains => inet_relation(right, left, false, false),
        PgOp::NetContainsOrEq => inet_relation(right, left, false, true),
        PgOp::TextMatch => Some(text_match(left, right)),
        PgOp::Contains | PgOp::ContainedBy | PgOp::Overlaps => {
            // `a <@ b` is `b @> a`, so containment is written once.
            let (op, lt, l, rt, r) = if op == PgOp::ContainedBy {
                (PgOp::Contains, right_type, right, left_type, left)
            } else {
                (op, left_type, left, right_type, right)
            };
            structural(op, lt, l, rt, r)
        }
    }
}

fn structural(
    op: PgOp,
    left_type: &DbType,
    left: &[u8],
    right_type: &DbType,
    right: &[u8],
) -> Option<bool> {
    match left_type {
        DbType::JSONB | DbType::JSON => {
            let outer = crate::core::jsonb::decode(left).ok()?;
            let inner = crate::core::jsonb::decode(right).ok()?;
            Some(match op {
                PgOp::Contains => json_contains(&outer, &inner),
                // Two documents overlap when either holds the other's top-level
                // shape. PostgreSQL has no `&&` for jsonb; this is the nearest
                // sensible reading and exists so a mixed expression does not
                // fail outright.
                PgOp::Overlaps => json_contains(&outer, &inner) || json_contains(&inner, &outer),
                _ => return None,
            })
        }
        DbType::ARRAY => {
            let outer = crate::core::value_codec::format_array(left)?;
            let inner = crate::core::value_codec::format_array(right)?;
            let outer = array_elements(&outer);
            let inner = array_elements(&inner);
            Some(match op {
                PgOp::Contains => inner.iter().all(|e| outer.contains(e)),
                PgOp::Overlaps => inner.iter().any(|e| outer.contains(e)),
                _ => return None,
            })
        }
        t if pg_types::range_element(t).is_some() => {
            let element = pg_types::range_element(t)?;
            // `range @> element` — the right operand is a bare value, not a
            // range. Told apart by its declared type, which is how PostgreSQL
            // tells the two operators apart too.
            if right_type != left_type {
                if op != PgOp::Contains {
                    return None;
                }
                let key = element_key_of(&element, right_type, right)?;
                return pg_types::range_contains_key(t, left, key);
            }
            let (lo_a, hi_a, lo_inc_a, hi_inc_a) = match pg_types::range_bounds_key(t, left) {
                Some(bounds) => bounds,
                // An empty range contains nothing and overlaps nothing, but is
                // contained by everything.
                None => return Some(op == PgOp::Contains && pg_types::range_bounds_key(t, right).is_none()),
            };
            let (lo_b, hi_b, lo_inc_b, hi_inc_b) = match pg_types::range_bounds_key(t, right) {
                Some(bounds) => bounds,
                None => return Some(op == PgOp::Contains),
            };
            Some(match op {
                PgOp::Contains => {
                    let lower_ok = lo_a < lo_b || (lo_a == lo_b && (lo_inc_a || !lo_inc_b));
                    let upper_ok = hi_a > hi_b || (hi_a == hi_b && (hi_inc_a || !hi_inc_b));
                    lower_ok && upper_ok
                }
                PgOp::Overlaps => {
                    let after = lo_a > hi_b || (lo_a == hi_b && !(lo_inc_a && hi_inc_b));
                    let before = hi_a < lo_b || (hi_a == lo_b && !(hi_inc_a && lo_inc_b));
                    !after && !before
                }
                _ => return None,
            })
        }
        t if is_geometry(t) => {
            let a = pg_types::bounding_box(t, left)?;
            let b = pg_types::bounding_box(right_type, right)?;
            Some(match op {
                PgOp::Contains => a.0 <= b.0 && a.1 <= b.1 && a.2 >= b.2 && a.3 >= b.3,
                PgOp::Overlaps => a.0 <= b.2 && b.0 <= a.2 && a.1 <= b.3 && b.1 <= a.3,
                _ => return None,
            })
        }
        _ => None,
    }
}

fn is_geometry(db_type: &DbType) -> bool {
    matches!(
        db_type,
        DbType::POINT
            | DbType::LINE
            | DbType::LSEG
            | DbType::BOX
            | DbType::PATH
            | DbType::POLYGON
            | DbType::CIRCLE
    )
}

/// A bare value's key on the scale [`pg_types::range_element_key`] uses.
///
/// The operand is an ordinary **engine** value, in the engine's own layout —
/// little-endian for the integers, microseconds from the Unix epoch for a
/// timestamp — whereas a range's stored *bounds* are in PostgreSQL's wire
/// layout, big-endian and counted from 2000-01-01. Decoding the operand with
/// the bound's reader is what made `sample_frequencies @> 100` answer `false`
/// for a range that plainly holds 100: `50` little-endian read big-endian is
/// 838860800.
///
/// So the operand is decoded here as what it is, and only then lifted onto the
/// element's scale.
fn element_key_of(element: &DbType, actual: &DbType, bytes: &[u8]) -> Option<i128> {
    // Text is re-parsed at the element's own type, which yields a bound-shaped
    // value and so goes through the bound reader.
    if matches!(actual, DbType::STRING) {
        let text = std::str::from_utf8(bytes).ok()?;
        let encoded = crate::core::value_codec::encode_literal(element, text)?;
        return pg_types::range_element_key(element, &encoded);
    }

    let raw: i128 = match actual {
        DbType::I8 => *bytes.first()? as i8 as i128,
        DbType::I16 => i16::from_le_bytes(bytes.get(0..2)?.try_into().ok()?) as i128,
        DbType::I32 => i32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as i128,
        DbType::I64 => i64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?) as i128,
        DbType::U8 => *bytes.first()? as i128,
        DbType::U16 => u16::from_le_bytes(bytes.get(0..2)?.try_into().ok()?) as i128,
        DbType::U32 => u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as i128,
        DbType::U64 => u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?) as i128,
        DbType::DECIMAL => i128::from_le_bytes(bytes.get(0..16)?.try_into().ok()?),
        DbType::DATETIME => i64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?) as i128,
        // `DATE` is one of the types stored in PostgreSQL's own layout, so its
        // engine value already *is* days from 2000-01-01, big-endian.
        DbType::DATE => i32::from_be_bytes(bytes.get(0..4)?.try_into().ok()?) as i128,
        _ => return None,
    };

    Some(match element {
        // A `NUMRANGE` bound is unscaled decimal units, so a plain integer has
        // to be lifted onto that scale before the two are compared.
        DbType::DECIMAL if !matches!(actual, DbType::DECIMAL) => {
            raw.checked_mul(10i128.pow(crate::core::value_codec::DECIMAL_SCALE))?
        }
        DbType::DECIMAL => raw,
        // A `TSRANGE` bound counts from 2000-01-01; the operand counts from
        // 1970. Only a timestamp is comparable against one — an integer here
        // would be a category error rather than an instant.
        DbType::DATETIME => {
            if !matches!(actual, DbType::DATETIME) {
                return None;
            }
            raw - crate::core::value_codec::PG_EPOCH_OFFSET_US as i128
        }
        DbType::DATE => {
            if !matches!(actual, DbType::DATE) {
                return None;
            }
            raw
        }
        _ => raw,
    })
}

/// PostgreSQL's `jsonb` containment.
///
/// An object contains another when it holds every one of its keys with a
/// contained value; an array contains another when every element of the right
/// is contained by some element of the left; a scalar contains only an equal
/// scalar. The one asymmetry PostgreSQL defines is kept: a top-level array
/// contains a bare scalar that is one of its elements.
pub fn json_contains(outer: &serde_json::Value, inner: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (outer, inner) {
        (Value::Object(a), Value::Object(b)) => b
            .iter()
            .all(|(key, value)| a.get(key).is_some_and(|found| json_contains(found, value))),
        (Value::Array(a), Value::Array(b)) => {
            b.iter().all(|want| a.iter().any(|have| json_contains(have, want)))
        }
        (Value::Array(a), scalar) if !scalar.is_object() => {
            a.iter().any(|have| json_contains(have, scalar))
        }
        (a, b) => a == b,
    }
}

/// `inet` subsumption: whether `inner` lies inside `outer`'s network.
///
/// `or_equal` allows the two to name the same network; `strict` is its
/// complement and is spelled by the caller passing `false`.
fn inet_relation(inner: &[u8], outer: &[u8], _strict: bool, or_equal: bool) -> Option<bool> {
    // `[family][address (16, big-endian)][prefix bits]`, as `value_codec` lays
    // it out.
    if inner.len() != crate::core::value_codec::INET_LEN
        || outer.len() != crate::core::value_codec::INET_LEN
    {
        return None;
    }
    if inner[0] != outer[0] {
        return Some(false);
    }
    let inner_bits = inner[17];
    let outer_bits = outer[17];
    // A narrower network cannot contain a wider one.
    if inner_bits < outer_bits {
        return Some(false);
    }
    if inner_bits == outer_bits && !or_equal {
        return Some(false);
    }

    // The addresses must agree on every bit the outer network fixes.
    let full = (outer_bits / 8) as usize;
    let remainder = outer_bits % 8;
    let inner_address = &inner[1..17];
    let outer_address = &outer[1..17];
    if inner_address[..full] != outer_address[..full] {
        return Some(false);
    }
    if remainder != 0 {
        let mask = 0xffu8 << (8 - remainder);
        if inner_address[full] & mask != outer_address[full] & mask {
            return Some(false);
        }
    }
    Some(true)
}

/// `tsvector @@ tsquery`.
///
/// Both sides arrive as the text [`to_tsvector`] and [`to_tsquery`] produced,
/// which is a normalised lexeme list and a boolean expression over lexemes —
/// see [`crate::core::text_search`].
fn text_match(vector: &[u8], query: &[u8]) -> bool {
    let Ok(vector) = std::str::from_utf8(vector) else {
        return false;
    };
    let Ok(query) = std::str::from_utf8(query) else {
        return false;
    };
    crate::core::text_search::matches(vector, query)
}

/// The elements of an array's text form, `{a,b,"c d"}`.
fn array_elements(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .unwrap_or(trimmed);
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => in_quotes = !in_quotes,
            '\\' if in_quotes => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            ',' if !in_quotes => out.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    if !current.is_empty() || !out.is_empty() {
        out.push(current);
    }
    out.into_iter().map(|e| e.trim().to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inet(text: &str) -> Vec<u8> {
        crate::core::value_codec::parse_inet(text).unwrap().to_vec()
    }

    #[test]
    fn an_address_is_contained_by_its_network() {
        let host = inet("192.168.1.10");
        let net = inet("192.168.0.0/16");
        assert_eq!(
            evaluate(PgOp::NetContainedByOrEq, &DbType::INET, &host, &DbType::INET, &net),
            Some(true)
        );
        assert_eq!(
            evaluate(PgOp::NetContainsOrEq, &DbType::INET, &net, &DbType::INET, &host),
            Some(true)
        );
    }

    #[test]
    fn an_address_outside_the_network_is_not() {
        let host = inet("10.0.0.5");
        let net = inet("192.168.0.0/16");
        assert_eq!(
            evaluate(PgOp::NetContainedByOrEq, &DbType::INET, &host, &DbType::INET, &net),
            Some(false)
        );
    }

    #[test]
    fn a_network_does_not_strictly_contain_itself() {
        let net = inet("192.168.0.0/16");
        assert_eq!(
            evaluate(PgOp::NetContainedBy, &DbType::INET, &net, &DbType::INET, &net),
            Some(false)
        );
        assert_eq!(
            evaluate(PgOp::NetContainedByOrEq, &DbType::INET, &net, &DbType::INET, &net),
            Some(true)
        );
    }

    #[test]
    fn a_prefix_that_is_not_a_whole_byte_still_masks_correctly() {
        // /12 fixes four bits of the second octet: 172.16 and 172.31 are in,
        // 172.32 is out.
        let net = inet("172.16.0.0/12");
        for (address, expected) in
            [("172.16.0.1", true), ("172.31.255.254", true), ("172.32.0.1", false)]
        {
            assert_eq!(
                evaluate(PgOp::NetContainedByOrEq, &DbType::INET, &inet(address), &DbType::INET, &net),
                Some(expected),
                "{}",
                address
            );
        }
    }

    #[test]
    fn a_document_contains_the_pairs_it_holds() {
        let outer: serde_json::Value =
            serde_json::from_str(r#"{"a": 1, "b": {"c": 2}, "d": [1, 2, 3]}"#).unwrap();
        assert!(json_contains(&outer, &serde_json::json!({"a": 1})));
        assert!(json_contains(&outer, &serde_json::json!({"b": {"c": 2}})));
        assert!(json_contains(&outer, &serde_json::json!({"d": [2]})));
        assert!(!json_contains(&outer, &serde_json::json!({"a": 2})));
        assert!(!json_contains(&outer, &serde_json::json!({"z": 1})));
    }

    #[test]
    fn a_false_value_is_contained_and_a_missing_key_is_not() {
        // The distinction the risk-flag filter rests on: `false` is a value,
        // not an absence, so a document without the key must not match.
        let flagged: serde_json::Value = serde_json::from_str(r#"{"is_risk_flagged": false}"#).unwrap();
        let other: serde_json::Value = serde_json::from_str(r#"{"other": 1}"#).unwrap();
        let want = serde_json::json!({"is_risk_flagged": false});
        assert!(json_contains(&flagged, &want));
        assert!(!json_contains(&other, &want));
    }

    #[test]
    fn a_range_contains_the_values_between_its_bounds() {
        let range = pg_types::parse_range(&DbType::INT4RANGE, "[50,150)").unwrap();
        for (value, expected) in [(50i32, true), (100, true), (149, true), (150, false), (49, false)] {
            assert_eq!(
                evaluate(
                    PgOp::Contains,
                    &DbType::INT4RANGE,
                    &range,
                    &DbType::I32,
                    &value.to_le_bytes()
                ),
                Some(expected),
                "{}",
                value
            );
        }
    }

    #[test]
    fn two_ranges_overlap_when_they_share_a_value() {
        let a = pg_types::parse_range(&DbType::INT4RANGE, "[50,150)").unwrap();
        let b = pg_types::parse_range(&DbType::INT4RANGE, "[100,200)").unwrap();
        let c = pg_types::parse_range(&DbType::INT4RANGE, "[200,300)").unwrap();
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::INT4RANGE, &a, &DbType::INT4RANGE, &b),
            Some(true)
        );
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::INT4RANGE, &a, &DbType::INT4RANGE, &c),
            Some(false)
        );
    }

    #[test]
    fn adjacent_ranges_do_not_overlap() {
        // `[50,150)` ends where `[150,200)` begins, and neither includes 150.
        let a = pg_types::parse_range(&DbType::INT4RANGE, "[50,150)").unwrap();
        let b = pg_types::parse_range(&DbType::INT4RANGE, "[150,200)").unwrap();
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::INT4RANGE, &a, &DbType::INT4RANGE, &b),
            Some(false)
        );
    }

    #[test]
    fn shapes_overlap_by_their_bounding_boxes() {
        let square = pg_types::parse_polygon("((0,0),(0,10),(10,10),(10,0),(0,0))").unwrap();
        let near = pg_types::parse_polygon("((5,5),(5,15),(15,15),(15,5),(5,5))").unwrap();
        let far = pg_types::parse_polygon("((20,20),(20,30),(30,30),(30,20),(20,20))").unwrap();
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::POLYGON, &square, &DbType::POLYGON, &near),
            Some(true)
        );
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::POLYGON, &square, &DbType::POLYGON, &far),
            Some(false)
        );
    }

    #[test]
    fn a_circle_is_contained_by_a_larger_one_around_it() {
        let small = pg_types::parse_circle("<(5,5),3>").unwrap();
        let large = pg_types::parse_circle("<(5,5),20>").unwrap();
        assert_eq!(
            evaluate(PgOp::ContainedBy, &DbType::CIRCLE, &small, &DbType::CIRCLE, &large),
            Some(true)
        );
        assert_eq!(
            evaluate(PgOp::ContainedBy, &DbType::CIRCLE, &large, &DbType::CIRCLE, &small),
            Some(false)
        );
    }

    #[test]
    fn an_array_contains_the_elements_it_holds() {
        let outer = crate::core::value_codec::parse_array_literal("{express,gift}").unwrap();
        let inner = crate::core::value_codec::parse_array_literal("{gift}").unwrap();
        let absent = crate::core::value_codec::parse_array_literal("{standard}").unwrap();
        assert_eq!(
            evaluate(PgOp::Contains, &DbType::ARRAY, &outer, &DbType::ARRAY, &inner),
            Some(true)
        );
        assert_eq!(
            evaluate(PgOp::Contains, &DbType::ARRAY, &outer, &DbType::ARRAY, &absent),
            Some(false)
        );
        assert_eq!(
            evaluate(PgOp::Overlaps, &DbType::ARRAY, &outer, &DbType::ARRAY, &absent),
            Some(false)
        );
    }

    #[test]
    fn an_operator_that_does_not_apply_says_so_rather_than_answering_false() {
        // The distinction that matters: `None` is reported to the client as an
        // error, where `Some(false)` would drop every row and look like an
        // empty table.
        assert!(!PgOp::TextMatch.applies_to(&DbType::INET));
        assert!(!PgOp::NetContains.applies_to(&DbType::POLYGON));
        assert!(PgOp::Contains.applies_to(&DbType::JSONB));
    }
}
