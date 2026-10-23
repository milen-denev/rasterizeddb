//! Golden vectors: the exact bytes a persisted index is made of.
//!
//! The property tests beside each encoder prove it agrees with the evaluator.
//! These prove it has not *moved*: a snapshot written by one build is read by
//! the next, and an encoding that still agrees with the evaluator but writes
//! other bytes would leave every persisted key unfindable.
//!
//! Each versioned constant ([`key::ENCODING_VERSION`],
//! [`crate::core::jsonb::TEXT_RENDER_VERSION`], and in `jsonb`'s tests
//! `gin::ITEM_DERIVE_VERSION`) has a *frozen* vector set per version and a
//! history of `(version, digest of that set as written, pinned digest)`
//! rows, checked by [`check_history`]. The rules:
//!
//! - More coverage, no format change: add rows to the `*_ADDED` tables.
//!   They are checked against the encoders like the frozen set, but are not
//!   pinned, so no version moves.
//! - An output moved: the vector test fails. That is a format change. Keep
//!   the current frozen tables unedited under a `_V<n>` name (their history
//!   row still digests them), write the next set from the new outputs with
//!   the `*_ADDED` rows folded in, raise the constant, and append the new
//!   set's row. Persisted indexes written under the old version rebuild.
//! - Editing a frozen table in place, correctly or not, fails its history
//!   row: it is what that version's snapshots were written under.

use super::*;
use crate::core::hash::ItemFeed;

fn hex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex {s:?}");
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

/// `(type, value, last, encode_ordered)`: escapes, sign flips, float order
/// (-0 = +0, every NaN one key above +inf), NUMERIC as its scaled `i128`
/// (`100` is `1.00` at scale 2), and the big-endian types passed through.
pub(crate) const ORDERED: &[(DbType, &str, bool, &str)] = &[
    (DbType::I8, "00", true, "80"),
    (DbType::I8, "ff", true, "7f"),
    (DbType::I8, "7f", true, "ff"),
    (DbType::I8, "80", true, "00"),
    (DbType::I16, "0201", true, "8102"),
    (DbType::I16, "feff", true, "7ffe"),
    (DbType::I32, "01000000", true, "80000001"),
    (DbType::I32, "ffffffff", true, "7fffffff"),
    (DbType::I32, "00000080", true, "00000000"),
    (DbType::I64, "0000000000000000", true, "8000000000000000"),
    (DbType::I64, "ffffffffffffffff", true, "7fffffffffffffff"),
    (DbType::I64, "ffffffffffffff7f", true, "ffffffffffffffff"),
    (DbType::I128, "01000000000000000000000000000000", true, "80000000000000000000000000000001"),
    (DbType::I128, "ffffffffffffffffffffffffffffffff", true, "7fffffffffffffffffffffffffffffff"),
    (DbType::U8, "00", true, "00"),
    (DbType::U8, "ff", true, "ff"),
    (DbType::U16, "0201", true, "0102"),
    (DbType::U32, "04030201", true, "01020304"),
    (DbType::U64, "0100000000000000", true, "0000000000000001"),
    (DbType::U128, "01000000000000000000000000000000", true, "00000000000000000000000000000001"),
    (DbType::BOOL, "00", true, "00"),
    (DbType::BOOL, "01", true, "01"),
    (DbType::F32, "0000c03f", true, "bfc00000"),
    (DbType::F32, "0000c0bf", true, "403fffff"),
    (DbType::F32, "00000000", true, "80000000"),
    (DbType::F32, "00000080", true, "80000000"),
    (DbType::F32, "0000c07f", true, "ffc00000"),
    (DbType::F32, "0000c0ff", true, "ffc00000"),
    (DbType::F32, "0000807f", true, "ff800000"),
    (DbType::F32, "000080ff", true, "007fffff"),
    (DbType::F64, "000000000000f83f", true, "bff8000000000000"),
    (DbType::F64, "000000000000f8bf", true, "4007ffffffffffff"),
    (DbType::F64, "0000000000000000", true, "8000000000000000"),
    (DbType::F64, "0000000000000080", true, "8000000000000000"),
    (DbType::F64, "000000000000f87f", true, "fff8000000000000"),
    (DbType::F64, "000000000000f8ff", true, "fff8000000000000"),
    (DbType::F64, "000000000000f07f", true, "fff0000000000000"),
    (DbType::F64, "000000000000f0ff", true, "000fffffffffffff"),
    (DbType::DECIMAL, "64000000000000000000000000000000", true, "80000000000000000000000000000064"),
    (DbType::DECIMAL, "9cffffffffffffffffffffffffffffff", true, "7fffffffffffffffffffffffffffff9c"),
    (DbType::DECIMAL, "01000000000000000000000000000000", true, "80000000000000000000000000000001"),
    (DbType::DATETIME, "0000000000000000", true, "8000000000000000"),
    (DbType::DATETIME, "ffffffffffffffff", true, "7fffffffffffffff"),
    (DbType::DATE, "00000001", true, "80000001"),
    (DbType::DATE, "ffffffff", true, "7fffffff"),
    (DbType::TIME, "0000000000000001", true, "8000000000000001"),
    (DbType::UUID, "000102030405060708090a0b0c0d0e0f", true, "000102030405060708090a0b0c0d0e0f"),
    (DbType::INET, "04000000000000000000000000c0a8010720", true, "04000000000000000000000000c0a8010720"),
    (DbType::MACADDR, "08002b010203", true, "08002b010203"),
    (DbType::MACADDR8, "08002b0102030405", true, "08002b0102030405"),
    (DbType::STRING, "616263", true, "616263"),
    (DbType::STRING, "610062", true, "610062"),
    (DbType::STRING, "610062", false, "6100ff620000"),
    (DbType::STRING, "", false, "0000"),
    (DbType::STRING, "ff00", false, "ff00ff0000"),
];

/// `(type, value, encode_canonical)`: length-prefixed bytes, floats
/// canonicalised. `JSONB` is in [`JSONB_CANONICAL`].
pub(crate) const CANONICAL: &[(DbType, &str, &str)] = &[
    (DbType::I8, "00", "0100000000"),
    (DbType::I8, "ff", "01000000ff"),
    (DbType::I8, "7f", "010000007f"),
    (DbType::I8, "80", "0100000080"),
    (DbType::I16, "0201", "020000000201"),
    (DbType::I16, "feff", "02000000feff"),
    (DbType::I32, "01000000", "0400000001000000"),
    (DbType::I32, "ffffffff", "04000000ffffffff"),
    (DbType::I32, "00000080", "0400000000000080"),
    (DbType::I64, "0000000000000000", "080000000000000000000000"),
    (DbType::I64, "ffffffffffffffff", "08000000ffffffffffffffff"),
    (DbType::I64, "ffffffffffffff7f", "08000000ffffffffffffff7f"),
    (DbType::I128, "01000000000000000000000000000000", "1000000001000000000000000000000000000000"),
    (DbType::I128, "ffffffffffffffffffffffffffffffff", "10000000ffffffffffffffffffffffffffffffff"),
    (DbType::U8, "00", "0100000000"),
    (DbType::U8, "ff", "01000000ff"),
    (DbType::U16, "0201", "020000000201"),
    (DbType::U32, "04030201", "0400000004030201"),
    (DbType::U64, "0100000000000000", "080000000100000000000000"),
    (DbType::U128, "01000000000000000000000000000000", "1000000001000000000000000000000000000000"),
    (DbType::BOOL, "00", "0100000000"),
    (DbType::BOOL, "01", "0100000001"),
    (DbType::F32, "0000c03f", "040000000000c03f"),
    (DbType::F32, "0000c0bf", "040000000000c0bf"),
    (DbType::F32, "00000000", "0400000000000000"),
    (DbType::F32, "00000080", "0400000000000000"),
    (DbType::F32, "0000c07f", "040000000000c07f"),
    (DbType::F32, "0000c0ff", "040000000000c07f"),
    (DbType::F32, "0000807f", "040000000000807f"),
    (DbType::F32, "000080ff", "04000000000080ff"),
    (DbType::F64, "000000000000f83f", "08000000000000000000f83f"),
    (DbType::F64, "000000000000f8bf", "08000000000000000000f8bf"),
    (DbType::F64, "0000000000000000", "080000000000000000000000"),
    (DbType::F64, "0000000000000080", "080000000000000000000000"),
    (DbType::F64, "000000000000f87f", "08000000000000000000f87f"),
    (DbType::F64, "000000000000f8ff", "08000000000000000000f87f"),
    (DbType::F64, "000000000000f07f", "08000000000000000000f07f"),
    (DbType::F64, "000000000000f0ff", "08000000000000000000f0ff"),
    (DbType::DECIMAL, "64000000000000000000000000000000", "1000000064000000000000000000000000000000"),
    (DbType::DECIMAL, "9cffffffffffffffffffffffffffffff", "100000009cffffffffffffffffffffffffffffff"),
    (DbType::DECIMAL, "01000000000000000000000000000000", "1000000001000000000000000000000000000000"),
    (DbType::DATETIME, "0000000000000000", "080000000000000000000000"),
    (DbType::DATETIME, "ffffffffffffffff", "08000000ffffffffffffffff"),
    (DbType::DATE, "00000001", "0400000000000001"),
    (DbType::DATE, "ffffffff", "04000000ffffffff"),
    (DbType::TIME, "0000000000000001", "080000000000000000000001"),
    (DbType::UUID, "000102030405060708090a0b0c0d0e0f", "10000000000102030405060708090a0b0c0d0e0f"),
    (DbType::INET, "04000000000000000000000000c0a8010720", "1200000004000000000000000000000000c0a8010720"),
    (DbType::MACADDR, "08002b010203", "0600000008002b010203"),
    (DbType::MACADDR8, "08002b0102030405", "0800000008002b0102030405"),
    (DbType::STRING, "616263", "03000000616263"),
    (DbType::STRING, "610062", "03000000610062"),
    (DbType::CHAR, "41000000", "0400000041000000"),
    (DbType::CHAR, "e9000000", "04000000e9000000"),
];

/// `(document, encode_canonical(JSONB))`: equal values give equal keys
/// (`1` and `1.0`; `0` and `-0.0`), whatever their stored bytes.
pub(crate) const JSONB_CANONICAL: &[(&str, &str)] = &[
    (r#"1"#, "1300000004020100000000000000010000000000000031"),
    (r#"1.0"#, "1300000004020100000000000000010000000000000031"),
    (r#"-0.0"#, "020000000400"),
    (r#"0"#, "020000000400"),
    (r#""s""#, "06000000050100000073"),
    (r#"null"#, "0100000000"),
    (r#"true"#, "0100000002"),
    (r#"{"b":1,"a":[true,null,"x",2.50]}"#, "4300000007020000000100000061060400000002000501000000780402010000000000000002000000000000003235010000006204020100000000000000010000000000000031"),
    (r#"123456789012345678901234567890"#, "2f00000004021e000000000000001d000000000000003132333435363738393031323334353637383930313233343536373839"),
];

/// A value that is not a document is keyed by its own bytes.
const JSONB_UNREADABLE: (&str, &str) = ("ee0102", "03000000ee0102");

/// What has no encoding: a wrong width, a type with no proven order or
/// canonical form.
const REFUSED_ORDERED: &[(DbType, &str)] = &[
    (DbType::I32, "010203"),
    (DbType::F64, "00000000"),
    (DbType::CHAR, "41000000"),
    (DbType::JSONB, "00"),
    (DbType::INTERVAL, "00000000000000000000000000000000"),
];
const REFUSED_CANONICAL: &[(DbType, &str)] = &[
    (DbType::F64, "00000000"),
    (DbType::F32, "0000000000000000"),
    (DbType::INTERVAL, "00000000000000000000000000000000"),
    (DbType::TIMETZ, "000000000000000000000000"),
    (DbType::BYTEA, "00"),
];

/// The document every [`PATHS`] row reads.
pub(crate) const PATH_DOC: &str = r#"{"a":{"b":"x\"y","n":1.50,"s1":1.5,"s3":0.125,"m":1E+3,"z":0,"neg":-0.0,"big":123456789012345678901234567890,"e":1e-7,"t":true,"f":false,"nul":null,"o":{"k":[1,"two"]}},"arr":[10,"s",null,[1,2]],"1":"one","-1":"minus one","01":"zero one"}"#;

/// `(path expression, key)`: a path index's key is the text `->>` reads,
/// encoded as a last `STRING`; a missing path and a JSON null are the empty
/// key. Numbers at several scales, a huge one, escapes, nested containers,
/// and every step kind: `Key`, positive and negative `Index`, `Either` on an
/// object (`'1'`, `'-1'` and `'01'` are three keys) and on an array (leading
/// space, sign, leading zero; trailing text and out of range read nothing).
pub(crate) const PATHS: &[(&str, &str)] = &[
    ("doc->'a'->>'b'", "x\"y"),
    ("doc->'a'->>'n'", "1.50"),
    ("doc->'a'->>'s1'", "1.5"),
    ("doc->'a'->>'s3'", "0.125"),
    ("doc->'a'->>'m'", "1000"),
    ("doc->'a'->>'z'", "0"),
    ("doc->'a'->>'neg'", "0.0"),
    ("doc->'a'->>'big'", "123456789012345678901234567890"),
    ("doc->'a'->>'e'", "0.0000001"),
    ("doc->'a'->>'t'", "true"),
    ("doc->'a'->>'f'", "false"),
    ("doc->'a'->>'nul'", ""),
    ("doc->'a'->>'missing'", ""),
    ("doc->'a'->>'o'", "{\"k\": [1, \"two\"]}"),
    ("doc->'a'->'o'->'k'->>1", "two"),
    ("doc->'arr'->>0", "10"),
    ("doc->'arr'->>-1", "[1, 2]"),
    ("doc->'arr'->>9", ""),
    ("doc->'arr'->>-9", ""),
    ("doc->>'arr'", "[10, \"s\", null, [1, 2]]"),
    ("doc#>>'{1}'", "one"),
    ("doc#>>'{-1}'", "minus one"),
    ("doc#>>'{01}'", "zero one"),
    ("doc#>>'{arr,1}'", "s"),
    ("doc#>>'{arr,-1}'", "[1, 2]"),
    ("doc#>>'{arr,\" 1\"}'", "s"),
    ("doc#>>'{arr,+1}'", "s"),
    ("doc#>>'{arr,01}'", "s"),
    ("doc#>>'{arr,1x}'", ""),
    ("doc#>>'{arr,99999999999}'", ""),
    ("doc#>>'{a,b}'", "x\"y"),
    ("doc#>>'{a,t,x}'", ""),
];

/// A key over several columns, as [`LiveIndex::key_of_values`] composes it
/// and a persisted index stores it (owner's invariant 9c): `(method,
/// per-column (type, value) in key order, key)`. A B-tree concatenates
/// order-preserving encodings with only the final column marked `last`; a
/// hash concatenates canonical ones.
pub(crate) type Composite = (Method, &'static [(DbType, &'static str)], &'static str);

/// Frozen under `ENCODING_VERSION` 1, with `ORDERED`, `CANONICAL`,
/// `JSONB_CANONICAL` and `JSONB_UNREADABLE`.
pub(crate) const COMPOSITE: &[Composite] = &[
    // An embedded 0x00 in a string that is not last is escaped and
    // terminated.
    (Method::BTree, &[(DbType::STRING, "610062"), (DbType::I64, "0100000000000000")], "6100ff6200008000000000000001"),
    // A string that is last is not.
    (Method::BTree, &[(DbType::I32, "01000000"), (DbType::STRING, "ff00")], "80000001ff00"),
    (Method::BTree, &[(DbType::STRING, ""), (DbType::STRING, "61")], "000061"),
    (
        Method::Hash,
        &[(DbType::I64, "0100000000000000"), (DbType::UUID, "000102030405060708090a0b0c0d0e0f")],
        "08000000010000000000000010000000000102030405060708090a0b0c0d0e0f",
    ),
    (Method::Hash, &[(DbType::STRING, "6162"), (DbType::STRING, "")], "02000000616200000000"),
];

/// Key coverage added since the frozen set: checked, not pinned (see the
/// module doc). Folded into the next frozen set when the version is raised.
pub(crate) const ORDERED_ADDED: &[(DbType, &str, bool, &str)] = &[];
pub(crate) const CANONICAL_ADDED: &[(DbType, &str, &str)] = &[];
pub(crate) const JSONB_CANONICAL_ADDED: &[(&str, &str)] = &[];
pub(crate) const COMPOSITE_ADDED: &[Composite] = &[];
/// Path coverage added since the frozen set: `(document, path expression,
/// key)`.
pub(crate) const PATHS_ADDED: &[(&str, &str, &str)] = &[];

/// A frozen vector set's history row: the version it was frozen under, a
/// function digesting that set *as written* (inputs and expected outputs,
/// never recomputed by the encoders), and the pinned digest.
pub(crate) type HistoryRow = (u32, fn() -> u64, u64);

/// `ENCODING_VERSION`'s frozen sets, oldest first.
const KEY_HISTORY: &[HistoryRow] = &[(1, key_v1_digest, 0xe393_3074_b3ec_53c1)];
/// `TEXT_RENDER_VERSION`'s frozen sets, oldest first.
const TEXT_RENDER_HISTORY: &[HistoryRow] = &[(1, text_render_v1_digest, 0x92f3_c15c_181a_1735)];

pub(crate) fn path_key(expr: &str, doc: &[u8]) -> Option<Vec<u8>> {
    let path = crate::core::jsonb::parse_path_expression(expr)?;
    let decl = IndexDecl {
        name: "p".into(),
        method: Method::BTree,
        unique: false,
        columns: vec![1000],
        is_deleted: false,
    };
    let index = LiveIndex::over_path(decl, 1000, 0, path.steps);
    index.key_of(|wo| (wo == 0).then_some(doc)).map(|k| k.to_vec())
}

fn ordered(t: &DbType, v: &[u8], last: bool) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    key::encode_ordered(t, v, last, &mut out).map(|_| out)
}

fn canonical(t: &DbType, v: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    key::encode_canonical(t, v, &mut out).map(|_| out)
}

fn jsonb_doc(text: &str) -> Vec<u8> {
    crate::core::jsonb::encode_text(text).unwrap()
}

#[test]
fn ordered_keys_are_their_vectors() {
    for (t, v, last, want) in ORDERED.iter().chain(ORDERED_ADDED) {
        let got = ordered(t, &hex(v), *last).unwrap_or_else(|| panic!("{t:?} {v} refused"));
        assert_eq!(got, hex(want), "encode_ordered({t:?}, {v}, last={last}) moved");
    }
    for (t, v) in REFUSED_ORDERED {
        assert!(ordered(t, &hex(v), true).is_none(), "{t:?} {v} is now encoded");
    }
}

#[test]
fn canonical_keys_are_their_vectors() {
    for (t, v, want) in CANONICAL.iter().chain(CANONICAL_ADDED) {
        let got = canonical(t, &hex(v)).unwrap_or_else(|| panic!("{t:?} {v} refused"));
        assert_eq!(got, hex(want), "encode_canonical({t:?}, {v}) moved");
    }
    for (doc, want) in JSONB_CANONICAL.iter().chain(JSONB_CANONICAL_ADDED) {
        assert_eq!(canonical(&DbType::JSONB, &jsonb_doc(doc)).unwrap(), hex(want), "JSONB {doc} moved");
    }
    let (v, want) = JSONB_UNREADABLE;
    assert_eq!(canonical(&DbType::JSONB, &hex(v)).unwrap(), hex(want));
    for (t, v) in REFUSED_CANONICAL {
        assert!(canonical(t, &hex(v)).is_none(), "{t:?} {v} is now encoded");
    }
}

fn composite(method: Method, columns: &[(DbType, &str)]) -> Option<Vec<u8>> {
    let decl = IndexDecl {
        name: "c".into(),
        method,
        unique: false,
        columns: (0..columns.len() as u32).collect(),
        is_deleted: false,
    };
    let typed = columns.iter().enumerate().map(|(i, (t, _))| (i as u32, t.clone())).collect();
    let values: Vec<Vec<u8>> = columns.iter().map(|(_, v)| hex(v)).collect();
    let index = LiveIndex::new(decl, typed, false);
    index.key_of_values(|wo| values.get(wo as usize).map(|v| v.as_slice())).map(|k| k.to_vec())
}

#[test]
fn composite_keys_are_their_vectors() {
    for (method, columns, want) in COMPOSITE.iter().chain(COMPOSITE_ADDED) {
        let got = composite(*method, columns).unwrap_or_else(|| panic!("{method:?} {columns:?} refused"));
        assert_eq!(got, hex(want), "{method:?} key over {columns:?} moved");
    }
}

#[test]
fn path_keys_are_their_vectors() {
    let doc = jsonb_doc(PATH_DOC);
    let rows = PATHS.iter().map(|(expr, want)| (PATH_DOC, &doc, *expr, *want));
    let added: Vec<_> = PATHS_ADDED.iter().map(|(d, expr, want)| (*d, jsonb_doc(d), *expr, *want)).collect();
    for (text, doc, expr, want) in rows.chain(added.iter().map(|(t, d, e, w)| (*t, d, *e, *w))) {
        let got = path_key(expr, doc).unwrap_or_else(|| panic!("{expr} on {text} built no key"));
        assert_eq!(String::from_utf8(got).unwrap(), want, "{expr} on {text} moved");
    }
}

/// A digest of the key vectors frozen under `ENCODING_VERSION` 1, as
/// written: the tables' inputs and expected outputs, not what this build
/// computes from them.
fn key_v1_digest() -> u64 {
    let mut h = ItemFeed::new(0);
    let mut put = |tag: u8, t: &DbType, input: &[u8], out: &[u8]| {
        h.u8(tag);
        h.u8(t.to_byte());
        h.u64_le(input.len() as u64);
        h.bytes(input);
        h.u64_le(out.len() as u64);
        h.bytes(out);
    };
    for (t, v, last, want) in ORDERED {
        put(1 + *last as u8, t, &hex(v), &hex(want));
    }
    for (t, v, want) in CANONICAL {
        put(3, t, &hex(v), &hex(want));
    }
    for (doc, want) in JSONB_CANONICAL {
        put(4, &DbType::JSONB, doc.as_bytes(), &hex(want));
    }
    put(5, &DbType::JSONB, &hex(JSONB_UNREADABLE.0), &hex(JSONB_UNREADABLE.1));
    for (method, columns, want) in COMPOSITE {
        h.u8(6);
        h.u8(match method {
            Method::BTree => 1,
            Method::Hash => 2,
            Method::GinOps => 3,
            Method::GinPathOps => 4,
        });
        h.u64_le(columns.len() as u64);
        for (t, v) in columns.iter() {
            let v = hex(v);
            h.u8(t.to_byte());
            h.u64_le(v.len() as u64);
            h.bytes(&v);
        }
        let want = hex(want);
        h.u64_le(want.len() as u64);
        h.bytes(&want);
    }
    h.finish()
}

/// A digest of the path vectors frozen under `TEXT_RENDER_VERSION` 1, as
/// written: the document, each expression and its expected key.
fn text_render_v1_digest() -> u64 {
    let mut h = ItemFeed::new(0);
    h.u64_le(PATH_DOC.len() as u64);
    h.bytes(PATH_DOC.as_bytes());
    for (expr, want) in PATHS {
        h.u64_le(expr.len() as u64);
        h.bytes(expr.as_bytes());
        h.u64_le(want.len() as u64);
        h.bytes(want.as_bytes());
    }
    h.finish()
}

/// Checks a versioned constant against its frozen sets' history (see the
/// module doc for the rules a failure here asks you to follow).
pub(crate) fn check_history(name: &str, history: &[HistoryRow], version: u32) {
    assert!(history.windows(2).all(|w| w[0].0 < w[1].0), "{name}: versions must rise");
    let &(last, ..) = history.last().expect("a history");
    assert_eq!(
        last, version,
        "{name} is {version} but the newest frozen vector set is for {last}: freeze a set for it and append its row"
    );
    for &(v, digest, pinned) in history {
        let got = digest();
        assert_eq!(
            got, pinned,
            "{name} {v}: the vectors frozen under it were edited (digest {got:#018x}). They are what \
             that version's snapshots were written under, so they never change: more coverage goes \
             in the *_ADDED tables with no version change; an output that moved is a format change, \
             so raise {name}, freeze a new set and append its row"
        );
    }
}

#[test]
fn key_vectors_are_pinned_to_encoding_version() {
    check_history("key::ENCODING_VERSION", KEY_HISTORY, key::ENCODING_VERSION);
}

#[test]
fn path_vectors_are_pinned_to_text_render_version() {
    check_history("jsonb::TEXT_RENDER_VERSION", TEXT_RENDER_HISTORY, crate::core::jsonb::TEXT_RENDER_VERSION);
}
