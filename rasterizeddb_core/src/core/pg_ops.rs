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
    /// `?` — a JSONB document has this key at the top level, or holds this
    /// string as a top-level array element (or is that string).
    KeyExists,
    /// `?|` — any of an array of keys exists, in the sense of `?`.
    KeyExistsAny,
    /// `?&` — every one of an array of keys exists.
    KeyExistsAll,
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
            "?" => PgOp::KeyExists,
            "?|" => PgOp::KeyExistsAny,
            "?&" => PgOp::KeyExistsAll,
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
            PgOp::KeyExists => "?",
            PgOp::KeyExistsAny => "?|",
            PgOp::KeyExistsAll => "?&",
        }
    }

    /// Whether the right operand is text read as itself — a key, or a
    /// `'{a,b}'` list of keys — rather than a value of the left's type.
    ///
    /// The planner re-encodes a text literal at the left operand's type for the
    /// other operators (`doc @> '{"a":1}'` is a document). A key must not be:
    /// `doc ? '1'` would become the JSON number 1, and `doc ?| '{}'` an empty
    /// object.
    pub fn takes_text_operand(&self) -> bool {
        matches!(
            self,
            PgOp::KeyExists | PgOp::KeyExistsAny | PgOp::KeyExistsAll
        )
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
            PgOp::KeyExists | PgOp::KeyExistsAny | PgOp::KeyExistsAll => {
                matches!(db_type, DbType::JSONB | DbType::JSON)
            }
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
        PgOp::KeyExists | PgOp::KeyExistsAny | PgOp::KeyExistsAll => {
            let _ = (left_type, right_type);
            key_exists(op, left, right)
        }
        PgOp::Contains | PgOp::ContainedBy | PgOp::Overlaps
            if matches!(left_type, DbType::JSONB | DbType::JSON)
                && (left.is_empty() || right.is_empty()) =>
        {
            // An empty document is SQL NULL: a NULL column stores nothing, and
            // a `->` path that reaches nothing extracts nothing. No encoded
            // document is empty — even JSON `null` is a tag byte — so this
            // cannot swallow a malformed one. PostgreSQL's answer is NULL, which
            // a WHERE clause treats as not matching; the evaluator here is
            // two-valued, so that is `false`. Answering `None` instead failed
            // the whole statement over one NULL row, even under the
            // `__null_doc = 0 AND …` guard, because every comparison is
            // evaluated before the guard's AND is applied.
            //
            // `false` is only sound because no rewrite negates a structural
            // operator: `NOT (doc @> x)` is refused before planning rather
            // than complemented, so a NULL never turns into a match.
            Some(false)
        }
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

/// `?`, `?|` and `?&` over an encoded document.
///
/// An empty left operand is a NULL document — the companion says so, and the
/// guard in front of this comparison already rejects the row — so it answers
/// `false` rather than failing the statement on a row that was never going to
/// match. A key list that is not a `'{…}'` array is a malformed query.
fn key_exists(op: PgOp, doc: &[u8], right: &[u8]) -> Option<bool> {
    if doc.is_empty() {
        return Some(false);
    }
    let text = std::str::from_utf8(right).ok()?;
    if op == PgOp::KeyExists {
        return crate::core::jsonb::key_exists(doc, text);
    }
    let keys = parse_key_list(text)?;
    key_exists_in_list(op, doc, &keys)
}

/// The key list of `?|` / `?&`, split once.
///
/// A literal list is split when the predicate is planned and kept beside the
/// comparison (see `ColumnTransformer::key_list`), so a scan does not re-parse
/// `'{a,b}'` and allocate a `String` per key for every row. Only a list read
/// from a column per row comes through [`key_exists`]'s text form.
pub type KeyList = Vec<Option<String>>;

/// Splits a `'{a,b}'` key list, or `None` when it is not one.
pub fn parse_key_list(text: &str) -> Option<KeyList> {
    crate::core::tokenizer::predicate_rewrite::text_array_elements(text)
}

/// `?|` / `?&` against an already-split key list. `op` must be one of the two.
pub fn key_exists_in_list(op: PgOp, doc: &[u8], keys: &[Option<String>]) -> Option<bool> {
    debug_assert!(matches!(op, PgOp::KeyExistsAny | PgOp::KeyExistsAll));
    // A NULL document; see `key_exists`.
    if doc.is_empty() {
        return Some(false);
    }
    for key in keys {
        // A NULL element of the key list is skipped, by both operators — as
        // PostgreSQL's `jsonb_exists_any` / `jsonb_exists_all` skip it.
        let Some(key) = key else { continue };
        let found = crate::core::jsonb::key_exists(doc, key)?;
        match (op, found) {
            (PgOp::KeyExistsAny, true) => return Some(true),
            (PgOp::KeyExistsAll, false) => return Some(false),
            _ => {}
        }
    }
    // `?|` of no keys is false; `?&` of no keys is true.
    Some(op == PgOp::KeyExistsAll)
}

fn structural(
    op: PgOp,
    left_type: &DbType,
    left: &[u8],
    right_type: &DbType,
    right: &[u8],
) -> Option<bool> {
    match left_type {
        // Answered on the stored bytes. Both documents used to be decoded into
        // `serde_json::Value` trees for every row — the constant side included
        // — which was most of what a containment filter cost.
        DbType::JSONB | DbType::JSON => Some(match op {
            PgOp::Contains => crate::core::jsonb::contains(left, right)?,
            // Two documents overlap when either holds the other's top-level
            // shape. PostgreSQL has no `&&` for jsonb; this is the nearest
            // sensible reading and exists so a mixed expression does not
            // fail outright.
            PgOp::Overlaps => {
                crate::core::jsonb::contains(left, right)?
                    || crate::core::jsonb::contains(right, left)?
            }
            _ => return None,
        }),
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
                None => {
                    return Some(
                        op == PgOp::Contains && pg_types::range_bounds_key(t, right).is_none(),
                    );
                }
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

/// PostgreSQL's `jsonb` containment over decoded documents.
///
/// The operator itself runs on the encoded bytes — [`crate::core::jsonb::contains`]
/// — and this is the reference it is checked against: the same rules, stated
/// over `serde_json::Value` where they are easiest to read.
///
/// The rules are PostgreSQL's `JsonbDeepContains`. An object root contains
/// only an object root. An object contains another when it holds every one of
/// its keys with a contained value; an array contains another when every
/// scalar element of the right equals some scalar element of the left, and
/// every container element of the right is contained by some container element
/// of the left. A bare scalar on the right is contained by a **top-level**
/// array holding it, and nowhere else: `'[1,[2]]' @> '[2]'` and
/// `'{"a":[1,2]}' @> '{"a":1}'` are false. Scalars compare by kind and value,
/// and numbers numerically — `1.0` equals `1`.
pub fn json_contains(outer: &serde_json::Value, inner: &serde_json::Value) -> bool {
    use serde_json::Value;
    if outer.is_object() != inner.is_object() {
        return false;
    }
    let container = |v: &Value| v.is_object() || v.is_array();
    match (outer, inner) {
        (Value::Array(a), scalar) if !container(scalar) => {
            a.iter().any(|have| !container(have) && json_scalar_eq(have, scalar))
        }
        (scalar, Value::Array(_)) if !container(scalar) => false,
        _ => json_nested_contains(outer, inner),
    }
}

fn json_nested_contains(outer: &serde_json::Value, inner: &serde_json::Value) -> bool {
    use serde_json::Value;
    let container = |v: &Value| v.is_object() || v.is_array();
    match (outer, inner) {
        (Value::Object(a), Value::Object(b)) => b.iter().all(|(key, value)| {
            a.get(key)
                .is_some_and(|found| json_nested_contains(found, value))
        }),
        (Value::Array(a), Value::Array(b)) => b.iter().all(|want| {
            if container(want) {
                a.iter()
                    .any(|have| container(have) && json_nested_contains(have, want))
            } else {
                a.iter().any(|have| !container(have) && json_scalar_eq(have, want))
            }
        }),
        (a, b) if !container(a) && !container(b) => json_scalar_eq(a, b),
        _ => false,
    }
}

fn json_scalar_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => crate::core::jsonb::json_numbers_equal(x, y),
        _ => a == b,
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
            evaluate(
                PgOp::NetContainedByOrEq,
                &DbType::INET,
                &host,
                &DbType::INET,
                &net
            ),
            Some(true)
        );
        assert_eq!(
            evaluate(
                PgOp::NetContainsOrEq,
                &DbType::INET,
                &net,
                &DbType::INET,
                &host
            ),
            Some(true)
        );
    }

    #[test]
    fn an_address_outside_the_network_is_not() {
        let host = inet("10.0.0.5");
        let net = inet("192.168.0.0/16");
        assert_eq!(
            evaluate(
                PgOp::NetContainedByOrEq,
                &DbType::INET,
                &host,
                &DbType::INET,
                &net
            ),
            Some(false)
        );
    }

    #[test]
    fn a_network_does_not_strictly_contain_itself() {
        let net = inet("192.168.0.0/16");
        assert_eq!(
            evaluate(
                PgOp::NetContainedBy,
                &DbType::INET,
                &net,
                &DbType::INET,
                &net
            ),
            Some(false)
        );
        assert_eq!(
            evaluate(
                PgOp::NetContainedByOrEq,
                &DbType::INET,
                &net,
                &DbType::INET,
                &net
            ),
            Some(true)
        );
    }

    #[test]
    fn a_prefix_that_is_not_a_whole_byte_still_masks_correctly() {
        // /12 fixes four bits of the second octet: 172.16 and 172.31 are in,
        // 172.32 is out.
        let net = inet("172.16.0.0/12");
        for (address, expected) in [
            ("172.16.0.1", true),
            ("172.31.255.254", true),
            ("172.32.0.1", false),
        ] {
            assert_eq!(
                evaluate(
                    PgOp::NetContainedByOrEq,
                    &DbType::INET,
                    &inet(address),
                    &DbType::INET,
                    &net
                ),
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
        let flagged: serde_json::Value =
            serde_json::from_str(r#"{"is_risk_flagged": false}"#).unwrap();
        let other: serde_json::Value = serde_json::from_str(r#"{"other": 1}"#).unwrap();
        let want = serde_json::json!({"is_risk_flagged": false});
        assert!(json_contains(&flagged, &want));
        assert!(!json_contains(&other, &want));
    }

    fn doc(text: &str) -> Vec<u8> {
        crate::core::jsonb::encode(&serde_json::from_str(text).expect("test document"))
    }

    fn jsonb_contains(outer: &str, inner: &str) -> Option<bool> {
        evaluate(
            PgOp::Contains,
            &DbType::JSONB,
            &doc(outer),
            &DbType::JSONB,
            &doc(inner),
        )
    }

    /// PostgreSQL's answers, case by case — each `false` here except the
    /// object/array mismatches was `true` before, and the first was `false`.
    const POSTGRES_CONTAINMENT: &[(&str, &str, bool)] = &[
        // Numbers compare numerically.
        (r#"{"a":1.0}"#, r#"{"a":1}"#, true),
        (r#"{"a":1}"#, r#"{"a":1.0}"#, true),
        (r#"[100]"#, r#"[1e2]"#, true),
        (r#"{"a":{"b":1,"c":2}}"#, r#"{"a":{"c":2.00}}"#, true),
        (r#"1"#, r#"1.00"#, true),
        (r#"[0]"#, r#"[-0.0]"#, true),
        (r#"[1.5]"#, r#"[1.50]"#, true),
        (r#"[1.5]"#, r#"[15e-1]"#, true),
        (r#"[1.5]"#, r#"[1.05]"#, false),
        (r#"[10]"#, r#"[1]"#, false),
        (r#"[18446744073709551615]"#, r#"[18446744073709551615]"#, true),
        (r#"[18446744073709551615]"#, r#"[18446744073709551614]"#, false),
        // A bare scalar is contained in a top-level array only.
        (r#"[1,2]"#, r#"[2]"#, true),
        (r#"[1,2]"#, r#"2"#, true),
        (r#"["foo","bar"]"#, r#""foo""#, true),
        (r#"[1,[2]]"#, r#"[2]"#, false),
        (r#"[1,[2]]"#, r#"2"#, false),
        (r#"[1,[2]]"#, r#"[[2]]"#, true),
        (r#"{"a":[1,2]}"#, r#"{"a":1}"#, false),
        (r#"{"a":[1,2]}"#, r#"{"a":[1]}"#, true),
        (r#"[1,2,[1,3]]"#, r#"[1,3]"#, false),
        (r#"[1,2,[1,3]]"#, r#"[[1,3]]"#, true),
        (r#"[1,2,3]"#, r#"[3,1]"#, true),
        (r#"[1,2,3]"#, r#"[1,2,2]"#, true),
        (r#"{"foo":{"bar":"baz"}}"#, r#"{"bar":"baz"}"#, false),
        (r#"{"foo":{"bar":"baz"}}"#, r#"{"foo":{}}"#, true),
        // A raw scalar contains only an equal scalar.
        (r#""foo""#, r#""foo""#, true),
        (r#""foo""#, r#"["foo"]"#, false),
        (r#"null"#, r#"null"#, true),
        (r#"[null]"#, r#"null"#, true),
        // Kinds must agree.
        (r#"true"#, r#"1"#, false),
        (r#""1""#, r#"1"#, false),
        (r#"[1]"#, r#"["1"]"#, false),
        (r#"{"a":null}"#, r#"{"a":false}"#, false),
        (r#"{"is_risk_flagged":false}"#, r#"{"is_risk_flagged":false}"#, true),
        // Object and array roots never contain one another.
        (r#"{}"#, r#"[]"#, false),
        (r#"[]"#, r#"{}"#, false),
        (r#"[{"a":1}]"#, r#"{"a":1}"#, false),
        (r#"[{"a":1}]"#, r#"[{"a":1}]"#, true),
        (r#"[{"a":1,"b":2}]"#, r#"[{"b":2}]"#, true),
        (r#"[{"a":1}]"#, r#"[{"a":1,"b":2}]"#, false),
        (r#"{"a":1}"#, r#"1"#, false),
        // Empties.
        (r#"{"a":1}"#, r#"{}"#, true),
        (r#"[1]"#, r#"[]"#, true),
        (r#"[]"#, r#"[]"#, true),
        (r#"{}"#, r#"{"a":1}"#, false),
        (r#"[]"#, r#"[1]"#, false),
    ];

    #[test]
    fn jsonb_containment_answers_as_postgres_does() {
        let mut wrong = Vec::new();
        for (outer, inner, want) in POSTGRES_CONTAINMENT {
            let got = jsonb_contains(outer, inner);
            if got != Some(*want) {
                wrong.push(format!("{} @> {}: got {:?}, want {}", outer, inner, got, want));
            }
            // The decoded reference states the same rules.
            let reference = json_contains(
                &serde_json::from_str(outer).unwrap(),
                &serde_json::from_str(inner).unwrap(),
            );
            if reference != *want {
                wrong.push(format!("reference {} @> {}: got {}, want {}", outer, inner, reference, want));
            }
            // `<@` is the mirror.
            let mirrored = evaluate(
                PgOp::ContainedBy,
                &DbType::JSONB,
                &doc(inner),
                &DbType::JSONB,
                &doc(outer),
            );
            if mirrored != Some(*want) {
                wrong.push(format!("{} <@ {}: got {:?}, want {}", inner, outer, mirrored, want));
            }
        }
        assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
    }

    #[test]
    fn a_malformed_document_has_no_answer() {
        assert_eq!(evaluate(PgOp::Contains, &DbType::JSONB, &[0x09], &DbType::JSONB, &doc("1")), None);
        // A composite whose table claims more than the bytes hold.
        let mut cut = doc(r#"{"a":[1,2,3]}"#);
        cut.truncate(cut.len() - 3);
        assert_eq!(
            evaluate(PgOp::Contains, &DbType::JSONB, &cut, &DbType::JSONB, &doc(r#"{"a":[3]}"#)),
            None
        );
    }

    /// Empty bytes are SQL NULL — a NULL column, or a `->` path that reached
    /// nothing — and NULL never matches. Every one of these was `None` for
    /// containment, which the scan reports as an error, so one NULL row failed
    /// the whole statement where PostgreSQL returns the other rows.
    #[test]
    fn a_null_document_matches_nothing_rather_than_failing() {
        let some = doc(r#"{"a":1}"#);
        for op in [PgOp::Contains, PgOp::ContainedBy, PgOp::Overlaps] {
            for (l, r) in [(&[][..], &some[..]), (&some[..], &[][..]), (&[][..], &[][..])] {
                assert_eq!(
                    evaluate(op, &DbType::JSONB, l, &DbType::JSONB, r),
                    Some(false),
                    "{:?} with an empty operand",
                    op
                );
            }
        }
        for (op, keys) in [
            (PgOp::KeyExists, "a"),
            (PgOp::KeyExistsAny, "{a,b}"),
            (PgOp::KeyExistsAll, "{a,b}"),
            // `?&` of no keys is true for a document, but still not for NULL.
            (PgOp::KeyExistsAll, "{}"),
        ] {
            assert_eq!(
                evaluate(op, &DbType::JSONB, &[], &DbType::STRING, keys.as_bytes()),
                Some(false),
                "{:?} {}",
                op,
                keys
            );
        }
    }

    /// The pre-split list answers exactly as the text form does.
    #[test]
    fn a_split_key_list_answers_as_the_text_form() {
        let docs = [
            r#"{"a":1,"b":2}"#,
            r#"{"a":1}"#,
            r#"{"c":null}"#,
            r#"["a","x"]"#,
            r#"[1,"b"]"#,
            r#""a""#,
            r#"{}"#,
            r#"[]"#,
            r#"7"#,
        ];
        let lists = ["{a,b}", "{a}", "{}", "{NULL,a}", r#"{"a",b}"#, "{x,y,z}", r#"{"a b",c}"#];
        for d in docs {
            let d = doc(d);
            for list in lists {
                let keys = parse_key_list(list).expect(list);
                for op in [PgOp::KeyExistsAny, PgOp::KeyExistsAll] {
                    assert_eq!(
                        key_exists_in_list(op, &d, &keys),
                        evaluate(op, &DbType::JSONB, &d, &DbType::STRING, list.as_bytes()),
                        "{:?} {}",
                        op,
                        list
                    );
                }
            }
        }
        // And against PostgreSQL's answers for the common shapes.
        let d = doc(r#"{"a":1,"b":2}"#);
        let any = |l: &str| key_exists_in_list(PgOp::KeyExistsAny, &d, &parse_key_list(l).unwrap());
        let all = |l: &str| key_exists_in_list(PgOp::KeyExistsAll, &d, &parse_key_list(l).unwrap());
        assert_eq!(any("{q,b}"), Some(true));
        assert_eq!(any("{q,r}"), Some(false));
        assert_eq!(any("{}"), Some(false));
        assert_eq!(all("{a,b}"), Some(true));
        assert_eq!(all("{a,q}"), Some(false));
        assert_eq!(all("{}"), Some(true));
        assert_eq!(all("{a,NULL}"), Some(true));
        assert!(parse_key_list("a,b").is_none());
    }

    /// Generated documents, small enough that containment is often true:
    /// few keys, few distinct scalars, numbers spelled several ways.
    fn gen_value(rng: &mut fastrand::Rng, depth: u32) -> serde_json::Value {
        use serde_json::{Value, json};
        let leaf = depth == 0 || rng.u8(0..10) < 5;
        if leaf {
            return match rng.u8(0..12) {
                0 => Value::Null,
                1 => json!(true),
                2 => json!(false),
                3 => json!(rng.i64(-2..3)),
                4 => serde_json::from_str(["1.0", "2.0", "1.5", "-0.0", "1e2", "100", "0.1"][rng.usize(0..7)]).unwrap(),
                5 => json!(u64::MAX - rng.u64(0..2)),
                6 | 7 => json!(["a", "b", "1", ""][rng.usize(0..4)]),
                _ => json!(rng.i64(0..3)),
            };
        }
        if rng.bool() {
            let n = rng.usize(0..4);
            Value::Array((0..n).map(|_| gen_value(rng, depth - 1)).collect())
        } else {
            let n = rng.usize(0..4);
            let mut m = serde_json::Map::new();
            for _ in 0..n {
                m.insert(["a", "b", "c", "aa", ""][rng.usize(0..5)].to_string(), gen_value(rng, depth - 1));
            }
            Value::Object(m)
        }
    }

    /// Shrinks a document toward one it contains, so the generated pairs
    /// are not almost all `false`.
    fn sub_value(rng: &mut fastrand::Rng, v: &serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match v {
            Value::Array(items) => {
                let mut out = Vec::new();
                for i in items {
                    if rng.bool() {
                        out.push(sub_value(rng, i));
                    }
                }
                Value::Array(out)
            }
            Value::Object(m) => {
                let mut out = serde_json::Map::new();
                for (k, i) in m {
                    if rng.u8(0..4) > 0 {
                        out.insert(k.clone(), sub_value(rng, i));
                    }
                }
                Value::Object(out)
            }
            other => other.clone(),
        }
    }

    /// The blob walk against the decoded reference, over generated pairs.
    #[test]
    fn blob_containment_matches_the_decoded_reference() {
        let mut rng = fastrand::Rng::with_seed(0x6a73_6f6e);
        let (mut trues, mut total) = (0usize, 0usize);
        for _ in 0..60_000 {
            let outer = gen_value(&mut rng, 3);
            let inner = match rng.u8(0..4) {
                0 => gen_value(&mut rng, 3),
                1 => outer.clone(),
                _ => sub_value(&mut rng, &outer),
            };
            // A scalar from somewhere inside, to exercise the top-level rule.
            let inner = if rng.u8(0..8) == 0 {
                match &outer {
                    serde_json::Value::Array(a) if !a.is_empty() => a[rng.usize(0..a.len())].clone(),
                    _ => inner,
                }
            } else {
                inner
            };
            let want = json_contains(&outer, &inner);
            let (o, i) = (crate::core::jsonb::encode(&outer), crate::core::jsonb::encode(&inner));
            let got = crate::core::jsonb::contains(&o, &i);
            assert_eq!(got, Some(want), "{} @> {}", outer, inner);
            total += 1;
            trues += want as usize;
        }
        // Both answers must be well represented, or the test proves little.
        assert!(trues > total / 5 && trues < total * 4 / 5, "{} of {} true", trues, total);
    }

    /// Per-row cost of `@>`: the blob walk against decoding both sides.
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored jsonb_contains_bench --nocapture`
    #[test]
    #[ignore]
    fn jsonb_contains_bench() {
        let docs: Vec<Vec<u8>> = (0..2000)
            .map(|i| {
                doc(&format!(
                    r#"{{"id":{i},"status":"{}","is_risk_flagged":{},"tags":["a","b","t{}"],"meta":{{"score":{}.5,"owner":"u{}","nested":{{"k":[1,2,3]}}}},"amount":1{}.25}}"#,
                    ["open", "closed", "pending"][i % 3],
                    i % 2 == 0,
                    i % 7,
                    i % 100,
                    i % 13,
                    i % 50,
                ))
            })
            .collect();
        let needles = [
            doc(r#"{"is_risk_flagged":false}"#),
            doc(r#"{"status":"open","meta":{"owner":"u3"}}"#),
            doc(r#"{"tags":["t3"]}"#),
            doc(r#"{"meta":{"nested":{"k":[2]}}}"#),
        ];
        let old = |a: &[u8], b: &[u8]| {
            let x = crate::core::jsonb::decode(a).unwrap();
            let y = crate::core::jsonb::decode(b).unwrap();
            json_contains(&x, &y)
        };
        let rounds = 50;
        for round in 0..6 {
            let t = std::time::Instant::now();
            let mut hits = 0usize;
            for _ in 0..rounds {
                for n in &needles {
                    for d in &docs {
                        hits += old(std::hint::black_box(d), n) as usize;
                    }
                }
            }
            let old_ns = t.elapsed().as_nanos() as f64 / (rounds * needles.len() * docs.len()) as f64;
            let t = std::time::Instant::now();
            let mut hits2 = 0usize;
            for _ in 0..rounds {
                for n in &needles {
                    for d in &docs {
                        hits2 += crate::core::jsonb::contains(std::hint::black_box(d), n).unwrap() as usize;
                    }
                }
            }
            let new_ns = t.elapsed().as_nanos() as f64 / (rounds * needles.len() * docs.len()) as f64;
            assert_eq!(hits, hits2);
            println!("round {round}: decode {old_ns:.1} ns/row, blob {new_ns:.1} ns/row ({:.1}x)", old_ns / new_ns);
        }
    }

    #[test]
    fn a_range_contains_the_values_between_its_bounds() {
        let range = pg_types::parse_range(&DbType::INT4RANGE, "[50,150)").unwrap();
        for (value, expected) in [
            (50i32, true),
            (100, true),
            (149, true),
            (150, false),
            (49, false),
        ] {
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
            evaluate(
                PgOp::Overlaps,
                &DbType::INT4RANGE,
                &a,
                &DbType::INT4RANGE,
                &b
            ),
            Some(true)
        );
        assert_eq!(
            evaluate(
                PgOp::Overlaps,
                &DbType::INT4RANGE,
                &a,
                &DbType::INT4RANGE,
                &c
            ),
            Some(false)
        );
    }

    #[test]
    fn adjacent_ranges_do_not_overlap() {
        // `[50,150)` ends where `[150,200)` begins, and neither includes 150.
        let a = pg_types::parse_range(&DbType::INT4RANGE, "[50,150)").unwrap();
        let b = pg_types::parse_range(&DbType::INT4RANGE, "[150,200)").unwrap();
        assert_eq!(
            evaluate(
                PgOp::Overlaps,
                &DbType::INT4RANGE,
                &a,
                &DbType::INT4RANGE,
                &b
            ),
            Some(false)
        );
    }

    #[test]
    fn shapes_overlap_by_their_bounding_boxes() {
        let square = pg_types::parse_polygon("((0,0),(0,10),(10,10),(10,0),(0,0))").unwrap();
        let near = pg_types::parse_polygon("((5,5),(5,15),(15,15),(15,5),(5,5))").unwrap();
        let far = pg_types::parse_polygon("((20,20),(20,30),(30,30),(30,20),(20,20))").unwrap();
        assert_eq!(
            evaluate(
                PgOp::Overlaps,
                &DbType::POLYGON,
                &square,
                &DbType::POLYGON,
                &near
            ),
            Some(true)
        );
        assert_eq!(
            evaluate(
                PgOp::Overlaps,
                &DbType::POLYGON,
                &square,
                &DbType::POLYGON,
                &far
            ),
            Some(false)
        );
    }

    #[test]
    fn a_circle_is_contained_by_a_larger_one_around_it() {
        let small = pg_types::parse_circle("<(5,5),3>").unwrap();
        let large = pg_types::parse_circle("<(5,5),20>").unwrap();
        assert_eq!(
            evaluate(
                PgOp::ContainedBy,
                &DbType::CIRCLE,
                &small,
                &DbType::CIRCLE,
                &large
            ),
            Some(true)
        );
        assert_eq!(
            evaluate(
                PgOp::ContainedBy,
                &DbType::CIRCLE,
                &large,
                &DbType::CIRCLE,
                &small
            ),
            Some(false)
        );
    }

    #[test]
    fn an_array_contains_the_elements_it_holds() {
        let outer = crate::core::value_codec::parse_array_literal("{express,gift}").unwrap();
        let inner = crate::core::value_codec::parse_array_literal("{gift}").unwrap();
        let absent = crate::core::value_codec::parse_array_literal("{standard}").unwrap();
        assert_eq!(
            evaluate(
                PgOp::Contains,
                &DbType::ARRAY,
                &outer,
                &DbType::ARRAY,
                &inner
            ),
            Some(true)
        );
        assert_eq!(
            evaluate(
                PgOp::Contains,
                &DbType::ARRAY,
                &outer,
                &DbType::ARRAY,
                &absent
            ),
            Some(false)
        );
        assert_eq!(
            evaluate(
                PgOp::Overlaps,
                &DbType::ARRAY,
                &outer,
                &DbType::ARRAY,
                &absent
            ),
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
