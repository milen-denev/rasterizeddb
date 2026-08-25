//! A JSON document as bytes.
//!
//! # Why a blob rather than columns
//!
//! A JSON column used to be *shredded*: each key became a column of a table of
//! its own, each nested object a further table. The intent was that a document
//! stop being opaque — its fields becoming ordinary typed columns that the
//! query engine could see into.
//!
//! It does not survive contact with real documents, for a reason that is
//! structural rather than incidental: shredding makes **the schema a function
//! of the data**. A document keyed by data — statistics keyed by column name, a
//! row keyed by user-defined field — has unboundedly many keys, so the table
//! grows a column per distinct key. Two engine properties then multiply that
//! into a quadratic: a new key needs `ALTER TABLE ADD COLUMN`, and adding a
//! column to a populated table rewrites every row of it, because a row is a
//! fixed-width positional record. A single import reached 217 shadow tables
//! against 154 real ones, 4,484 of 7,741 files, one shadow schema of 23 KB of
//! column definitions, and 43 GB of writes discarded by repeated rewriting.
//!
//! Storing the document as bytes makes a write one row write, always. Nothing
//! about the schema depends on what a document happens to contain, so there is
//! no rewrite, no per-key table, and no upper bound on how strange a document
//! may be. It also removes by construction a class of bug the mapping kept
//! producing: keys differing only in case colliding on one column, keys that
//! are not valid column names being renamed, nesting limits, arrays of mixed
//! type, and `{}` coming back for a value that was something else.
//!
//! Fields stay reachable — better than before, in fact. Shredding exposed them
//! only through the internal shadow table, under a hashed name no client could
//! be expected to know; `->` and `->>` here are what PostgreSQL clients
//! actually emit, and [`super::json_index`] indexes a path on request.
//!
//! # The format
//!
//! One tag byte, then a payload. Composites carry a fixed-stride table of
//! 32-bit offsets so a lookup can binary-search the keys and jump straight to a
//! value without decoding anything it does not need — which is the whole point
//! of a binary form over keeping the text.
//!
//! ```text
//! Null   0x00
//! False  0x01
//! True   0x02
//! Int    0x03  i64 little-endian
//! Num    0x04  u32 len, ASCII text
//! Str    0x05  u32 len, UTF-8 bytes
//! Arr    0x06  u32 count, count x (u32 offset, u32 len), body
//! Obj    0x07  u32 count, count x (u32 key_off, u32 key_len, u32 val_off, u32 val_len), body
//! ```
//!
//! Offsets are relative to the start of the body, so a nested value's bytes are
//! a plain subslice and navigating into one copies nothing.
//!
//! **Numbers that are not integers keep their text.** A `f64` would round-trip
//! most values and silently change the rest, and "silently changed the value
//! and reported success" is the failure this engine keeps having to hunt. The
//! text a number arrived as is exactly what it leaves as.
//!
//! **Object keys are sorted, and a duplicate key keeps the last value.** Sorted
//! because a binary search needs it; last-wins because that is what
//! PostgreSQL's `jsonb` does and what a client that sends a duplicate expects.
//! Key order is therefore canonical rather than as-written — also PostgreSQL's
//! behaviour, and what makes two equal documents equal as bytes.

use serde_json::{Map, Value};

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_NUM: u8 = 0x04;
const TAG_STR: u8 = 0x05;
const TAG_ARR: u8 = 0x06;
const TAG_OBJ: u8 = 0x07;

/// Bytes per entry in an array's offset table: offset, length.
const ARR_ENTRY: usize = 8;
/// Bytes per entry in an object's offset table: key offset and length, then
/// the value's.
const OBJ_ENTRY: usize = 16;

/// A step of a path into a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// A key of an object.
    Key(String),
    /// A position in an array. Negative counts from the end, as PostgreSQL's
    /// `->` does.
    Index(i64),
}

// ---------------------------------------------------------------------------
// encoding
// ---------------------------------------------------------------------------

/// Encodes a document.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_value(&mut out, value);
    out
}

fn write_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(false) => out.push(TAG_FALSE),
        Value::Bool(true) => out.push(TAG_TRUE),
        Value::Number(n) => write_number(out, n),
        Value::String(s) => {
            out.push(TAG_STR);
            write_len_prefixed(out, s.as_bytes());
        }
        Value::Array(items) => write_array(out, items),
        Value::Object(fields) => write_object(out, fields),
    }
}

fn write_number(out: &mut Vec<u8>, n: &serde_json::Number) {
    // An integer that fits is stored as one, because that is the common case
    // and it compares and extracts without parsing. Everything else — a
    // fraction, an exponent, an integer too large for `i64` — keeps the text it
    // arrived as, which is the only representation guaranteed to give back the
    // value that was written.
    if let Some(i) = n.as_i64() {
        out.push(TAG_INT);
        out.extend_from_slice(&i.to_le_bytes());
    } else {
        out.push(TAG_NUM);
        write_len_prefixed(out, n.to_string().as_bytes());
    }
}

fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn write_array(out: &mut Vec<u8>, items: &[Value]) {
    out.push(TAG_ARR);
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());

    // The table is written blank, then filled in: an entry's offset is not
    // known until the values before it have been laid out.
    let table_at = out.len();
    out.resize(table_at + items.len() * ARR_ENTRY, 0);
    let body_at = out.len();

    for (index, item) in items.iter().enumerate() {
        let offset = out.len() - body_at;
        write_value(out, item);
        let len = out.len() - body_at - offset;
        let entry = table_at + index * ARR_ENTRY;
        out[entry..entry + 4].copy_from_slice(&(offset as u32).to_le_bytes());
        out[entry + 4..entry + 8].copy_from_slice(&(len as u32).to_le_bytes());
    }
}

fn write_object(out: &mut Vec<u8>, fields: &Map<String, Value>) {
    // Sorted for the binary search, and deduplicated last-wins. `Map` is
    // insertion-ordered unless serde_json is built with `preserve_order`, and
    // either way cannot be relied on to be sorted.
    let mut entries: Vec<(&str, &Value)> =
        fields.iter().map(|(k, v)| (k.as_str(), v)).collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries.dedup_by(|later, earlier| {
        // `dedup_by` keeps `earlier` and drops `later`, so the last of a run
        // has to be moved into the one that survives.
        if later.0 == earlier.0 {
            earlier.1 = later.1;
            true
        } else {
            false
        }
    });

    out.push(TAG_OBJ);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    let table_at = out.len();
    out.resize(table_at + entries.len() * OBJ_ENTRY, 0);
    let body_at = out.len();

    for (index, (key, value)) in entries.iter().enumerate() {
        let key_off = out.len() - body_at;
        out.extend_from_slice(key.as_bytes());
        let val_off = out.len() - body_at;
        write_value(out, value);
        let val_len = out.len() - body_at - val_off;

        let entry = table_at + index * OBJ_ENTRY;
        out[entry..entry + 4].copy_from_slice(&(key_off as u32).to_le_bytes());
        out[entry + 4..entry + 8].copy_from_slice(&(key.len() as u32).to_le_bytes());
        out[entry + 8..entry + 12].copy_from_slice(&(val_off as u32).to_le_bytes());
        out[entry + 12..entry + 16].copy_from_slice(&(val_len as u32).to_le_bytes());
    }
}

// ---------------------------------------------------------------------------
// decoding
// ---------------------------------------------------------------------------

/// Rebuilds the document `bytes` encodes.
pub fn decode(bytes: &[u8]) -> Result<Value, String> {
    let (value, _) = read_value(bytes)?;
    Ok(value)
}

fn read_value(bytes: &[u8]) -> Result<(Value, usize), String> {
    let tag = *bytes.first().ok_or("a document ended where a value was expected")?;
    match tag {
        TAG_NULL => Ok((Value::Null, 1)),
        TAG_FALSE => Ok((Value::Bool(false), 1)),
        TAG_TRUE => Ok((Value::Bool(true), 1)),
        TAG_INT => {
            let raw = bytes.get(1..9).ok_or("an integer was truncated")?;
            let mut buf = [0u8; 8];
            buf.copy_from_slice(raw);
            Ok((Value::Number(i64::from_le_bytes(buf).into()), 9))
        }
        TAG_NUM => {
            let (text, used) = read_len_prefixed(bytes)?;
            let text = std::str::from_utf8(text).map_err(|_| "a number was not text")?;
            // Through serde_json's own parser, so the `Number` produced is the
            // one the document would have parsed to — and prints back the same.
            let value: Value = serde_json::from_str(text)
                .map_err(|e| format!("a stored number is not valid JSON: {}", e))?;
            Ok((value, used))
        }
        TAG_STR => {
            let (raw, used) = read_len_prefixed(bytes)?;
            let text = std::str::from_utf8(raw).map_err(|_| "a string was not UTF-8")?;
            Ok((Value::String(text.to_string()), used))
        }
        TAG_ARR => {
            let (count, table_at, body_at) = composite_header(bytes, ARR_ENTRY)?;
            let mut items = Vec::with_capacity(count);
            for index in 0..count {
                let entry = table_at + index * ARR_ENTRY;
                let slice = slice_at(bytes, body_at, entry, entry + 4)?;
                items.push(decode(slice)?);
            }
            Ok((Value::Array(items), total_len(bytes, body_at, table_at, count, ARR_ENTRY)?))
        }
        TAG_OBJ => {
            let (count, table_at, body_at) = composite_header(bytes, OBJ_ENTRY)?;
            let mut fields = Map::new();
            for index in 0..count {
                let entry = table_at + index * OBJ_ENTRY;
                let key = slice_at(bytes, body_at, entry, entry + 4)?;
                let key = std::str::from_utf8(key).map_err(|_| "a key was not UTF-8")?;
                let value = slice_at(bytes, body_at, entry + 8, entry + 12)?;
                fields.insert(key.to_string(), decode(value)?);
            }
            Ok((Value::Object(fields), total_len(bytes, body_at, table_at, count, OBJ_ENTRY)?))
        }
        other => Err(format!("{:#04x} is not a JSON value tag", other)),
    }
}

fn read_len_prefixed(bytes: &[u8]) -> Result<(&[u8], usize), String> {
    let len = read_u32(bytes, 1)? as usize;
    let raw = bytes.get(5..5 + len).ok_or("a length-prefixed value was truncated")?;
    Ok((raw, 5 + len))
}

fn composite_header(bytes: &[u8], stride: usize) -> Result<(usize, usize, usize), String> {
    let count = read_u32(bytes, 1)? as usize;
    let table_at: usize = 5;
    let body_at = table_at
        .checked_add(count.checked_mul(stride).ok_or("a composite claims an impossible size")?)
        .ok_or("a composite claims an impossible size")?;
    if bytes.len() < body_at {
        return Err("a composite's offset table is truncated".to_string());
    }
    Ok((count, table_at, body_at))
}

/// The full encoded length of a composite, which is its body's end.
fn total_len(
    bytes: &[u8],
    body_at: usize,
    table_at: usize,
    count: usize,
    stride: usize,
) -> Result<usize, String> {
    let mut end = body_at;
    for index in 0..count {
        let entry = table_at + index * stride;
        // The last field of an entry is always the value's length, and the one
        // before it the value's offset.
        let offset = read_u32(bytes, entry + stride - 8)? as usize;
        let len = read_u32(bytes, entry + stride - 4)? as usize;
        end = end.max(body_at + offset + len);
    }
    Ok(end)
}

fn slice_at<'a>(
    bytes: &'a [u8],
    body_at: usize,
    off_at: usize,
    len_at: usize,
) -> Result<&'a [u8], String> {
    let offset = read_u32(bytes, off_at)? as usize;
    let len = read_u32(bytes, len_at)? as usize;
    let start = body_at.checked_add(offset).ok_or("an offset is out of range")?;
    let end = start.checked_add(len).ok_or("a length is out of range")?;
    bytes.get(start..end).ok_or_else(|| "a value is truncated".to_string())
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, String> {
    let raw = bytes.get(at..at + 4).ok_or("a document ended inside a header")?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

// ---------------------------------------------------------------------------
// navigation
// ---------------------------------------------------------------------------

/// The bytes of the value at `path`, without decoding anything else.
///
/// This is what makes the blob worth more than the text it came from: reaching
/// a key is a binary search and a jump, not a parse of the whole document.
pub fn get_path<'a>(bytes: &'a [u8], path: &[Step]) -> Option<&'a [u8]> {
    let mut current = bytes;
    for step in path {
        current = match step {
            Step::Key(key) => get_key(current, key)?,
            Step::Index(index) => get_index(current, *index)?,
        };
    }
    Some(current)
}

/// The bytes of `key`'s value in an object.
pub fn get_key<'a>(bytes: &'a [u8], key: &str) -> Option<&'a [u8]> {
    if bytes.first() != Some(&TAG_OBJ) {
        return None;
    }
    let (count, table_at, body_at) = composite_header(bytes, OBJ_ENTRY).ok()?;

    // Keys are sorted at encode time, so this is a binary search.
    let (mut low, mut high) = (0usize, count);
    while low < high {
        let mid = (low + high) / 2;
        let entry = table_at + mid * OBJ_ENTRY;
        let candidate = slice_at(bytes, body_at, entry, entry + 4).ok()?;
        match candidate.cmp(key.as_bytes()) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                return slice_at(bytes, body_at, entry + 8, entry + 12).ok();
            }
        }
    }
    None
}

/// The bytes of the element at `index` of an array, counting from the end when
/// negative.
pub fn get_index(bytes: &[u8], index: i64) -> Option<&[u8]> {
    if bytes.first() != Some(&TAG_ARR) {
        return None;
    }
    let (count, table_at, body_at) = composite_header(bytes, ARR_ENTRY).ok()?;
    let resolved = if index < 0 {
        let from_end = index.checked_neg()? as usize;
        count.checked_sub(from_end)?
    } else {
        index as usize
    };
    if resolved >= count {
        return None;
    }
    let entry = table_at + resolved * ARR_ENTRY;
    slice_at(bytes, body_at, entry, entry + 4).ok()
}

/// The `->>` reading of a value: a string yields its own text, anything else
/// the JSON it would print as, and a JSON null yields nothing.
///
/// The asymmetry is PostgreSQL's and it is deliberate — `->>` exists to get a
/// value *as text*, so quoting a string would be handing back a JSON document
/// where a value was asked for.
pub fn as_text(bytes: &[u8]) -> Option<String> {
    match bytes.first()? {
        &TAG_NULL => None,
        &TAG_STR => {
            let (raw, _) = read_len_prefixed(bytes).ok()?;
            Some(std::str::from_utf8(raw).ok()?.to_string())
        }
        _ => Some(decode(bytes).ok()?.to_string()),
    }
}

/// Whether these bytes could be a document this module wrote.
///
/// A cheap shape check, not a validation: it is here so a column holding
/// something else is refused rather than decoded into nonsense.
pub fn looks_encoded(bytes: &[u8]) -> bool {
    matches!(
        bytes.first(),
        Some(&TAG_NULL | &TAG_FALSE | &TAG_TRUE | &TAG_INT | &TAG_NUM | &TAG_STR | &TAG_ARR | &TAG_OBJ)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(text: &str) {
        let value: Value = serde_json::from_str(text).expect("the test document parses");
        let encoded = encode(&value);
        let decoded = decode(&encoded).expect("decodes");
        assert_eq!(decoded, value, "round trip changed {}", text);
    }

    #[test]
    fn every_kind_of_value_round_trips() {
        for text in [
            "null",
            "true",
            "false",
            "0",
            "-1",
            "9223372036854775807",
            "-9223372036854775808",
            "1.5",
            "11203.75",
            "0.1",
            "1e10",
            "-2.5e-8",
            "\"\"",
            "\"hello\"",
            "\"a \\\"quoted\\\" string\"",
            "\"unicode: \\u00e9\\u4e2d\"",
            "[]",
            "{}",
            "[1,2,3]",
            "[\"a\",\"b\"]",
            "[true,false,null]",
            "[[1,2],[3]]",
            "{\"a\":1}",
            "{\"a\":{\"b\":{\"c\":1}}}",
            "[{\"a\":1},{\"b\":2}]",
        ] {
            round_trip(text);
        }
    }

    /// The document that stopped a real import, with the numeric array that an
    /// earlier mapping turned into strings.
    #[test]
    fn the_document_that_broke_the_import_round_trips() {
        let text = r#"{"title":"Pie chart","labels_json":["Deodorant","Body Wash","Shampoo"],"values_json":[11203.75,10696.66,10189.94,9944.26]}"#;
        round_trip(text);

        let value: Value = serde_json::from_str(text).unwrap();
        let encoded = encode(&value);
        // Numbers stay numbers, which is the failure the shredding mapping had:
        // `[1,2]` came back as `["1","2"]`.
        let values = get_key(&encoded, "values_json").expect("values_json");
        let first = get_index(values, 0).expect("first element");
        assert_eq!(as_text(first).as_deref(), Some("11203.75"));
        assert_eq!(decode(first).unwrap(), json!(11203.75));
    }

    /// Keys are case-sensitive in JSON. The shredding mapping folded them onto
    /// one column, so a document holding both lost one.
    #[test]
    fn keys_differing_only_in_case_are_separate() {
        let value = json!({"date": "lower", "Date": "upper"});
        let encoded = encode(&value);
        assert_eq!(as_text(get_key(&encoded, "date").unwrap()).as_deref(), Some("lower"));
        assert_eq!(as_text(get_key(&encoded, "Date").unwrap()).as_deref(), Some("upper"));
        assert_eq!(decode(&encoded).unwrap(), value);
    }

    /// A key that is not a valid column name needed renaming under shredding;
    /// here it is just a key.
    #[test]
    fn a_key_may_be_any_text() {
        let value = json!({
            "QA Comments": 1,
            "values": 2,
            "select from where": 3,
            "": 4,
            "ключ": 5,
            "a.b.c": 6,
        });
        let encoded = encode(&value);
        assert_eq!(decode(&encoded).unwrap(), value);
        assert_eq!(as_text(get_key(&encoded, "QA Comments").unwrap()).as_deref(), Some("1"));
        assert_eq!(as_text(get_key(&encoded, "").unwrap()).as_deref(), Some("4"));
        assert_eq!(as_text(get_key(&encoded, "ключ").unwrap()).as_deref(), Some("5"));
    }

    #[test]
    fn a_duplicate_key_keeps_the_last_value() {
        // `serde_json` already resolves duplicates when parsing, so the map is
        // built directly to get one through.
        let mut fields = Map::new();
        fields.insert("a".to_string(), json!(1));
        let mut with_dup = fields.clone();
        with_dup.insert("a".to_string(), json!(2));
        let encoded = encode(&Value::Object(with_dup));
        assert_eq!(as_text(get_key(&encoded, "a").unwrap()).as_deref(), Some("2"));
    }

    #[test]
    fn a_path_reaches_into_nesting() {
        let value = json!({"a": {"b": [10, {"c": "deep"}]}});
        let encoded = encode(&value);
        let path = vec![
            Step::Key("a".to_string()),
            Step::Key("b".to_string()),
            Step::Index(1),
            Step::Key("c".to_string()),
        ];
        let found = get_path(&encoded, &path).expect("the path resolves");
        assert_eq!(as_text(found).as_deref(), Some("deep"));

        // A path that does not exist is absent, not an error.
        assert!(get_path(&encoded, &[Step::Key("nope".to_string())]).is_none());
        assert!(get_path(&encoded, &[Step::Key("a".to_string()), Step::Index(0)]).is_none());
    }

    #[test]
    fn a_negative_index_counts_from_the_end() {
        let encoded = encode(&json!([1, 2, 3]));
        assert_eq!(as_text(get_index(&encoded, -1).unwrap()).as_deref(), Some("3"));
        assert_eq!(as_text(get_index(&encoded, -3).unwrap()).as_deref(), Some("1"));
        assert!(get_index(&encoded, -4).is_none());
        assert!(get_index(&encoded, 3).is_none());
    }

    /// `->>` gives a string its own text and everything else its JSON form.
    #[test]
    fn as_text_follows_the_double_arrow_rule() {
        let encoded = encode(&json!({
            "s": "plain",
            "n": 42,
            "f": 1.25,
            "b": true,
            "z": null,
            "a": [1, 2],
            "o": {"k": 1},
        }));
        assert_eq!(as_text(get_key(&encoded, "s").unwrap()).as_deref(), Some("plain"));
        assert_eq!(as_text(get_key(&encoded, "n").unwrap()).as_deref(), Some("42"));
        assert_eq!(as_text(get_key(&encoded, "f").unwrap()).as_deref(), Some("1.25"));
        assert_eq!(as_text(get_key(&encoded, "b").unwrap()).as_deref(), Some("true"));
        assert_eq!(as_text(get_key(&encoded, "z").unwrap()), None);
        assert_eq!(as_text(get_key(&encoded, "a").unwrap()).as_deref(), Some("[1,2]"));
        assert_eq!(as_text(get_key(&encoded, "o").unwrap()).as_deref(), Some("{\"k\":1}"));
    }

    /// A large integer and a high-precision decimal both have to come back as
    /// they went in — this is why a non-integer keeps its text.
    #[test]
    fn precision_is_not_quietly_lost() {
        for text in [
            "123456789012345678901234567890",
            "0.12345678901234567890123",
            "1.000000000000000000001",
            "-0.0",
        ] {
            let value: Value = serde_json::from_str(text).unwrap();
            let decoded = decode(&encode(&value)).unwrap();
            assert_eq!(
                decoded.to_string(),
                value.to_string(),
                "{} did not survive",
                text
            );
        }
    }

    #[test]
    fn a_big_document_navigates_without_decoding() {
        let mut fields = Map::new();
        for i in 0..2_000 {
            fields.insert(format!("key_{:05}", i), json!({"n": i}));
        }
        let value = Value::Object(fields);
        let encoded = encode(&value);
        // The point of the offset table: this reaches one value out of two
        // thousand without building any of the others.
        let found = get_path(
            &encoded,
            &[Step::Key("key_01234".to_string()), Step::Key("n".to_string())],
        )
        .expect("the key resolves");
        assert_eq!(as_text(found).as_deref(), Some("1234"));
        assert_eq!(decode(&encoded).unwrap(), value);
    }

    #[test]
    fn nonsense_bytes_are_refused_rather_than_decoded() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0xff]).is_err());
        // A truncated string.
        assert!(decode(&[TAG_STR, 10, 0, 0, 0, b'a']).is_err());
        // A count that would run past the end.
        assert!(decode(&[TAG_ARR, 0xff, 0xff, 0, 0]).is_err());
        assert!(!looks_encoded(&[0xff]));
        assert!(looks_encoded(&encode(&json!({"a": 1}))));
    }
}

// ---------------------------------------------------------------------------
// path expressions
// ---------------------------------------------------------------------------

/// A reference to a value inside a document column, as written in SQL.
///
/// One shape serves both readers: the evaluator asking what `doc->>'status'`
/// is for a row, and `CREATE INDEX` naming the thing it indexes. They have to
/// agree exactly — an index built over a path the evaluator resolves
/// differently would prune rows that do match, which is the one failure mode
/// this subsystem must never have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathExpr {
    /// The document column the path starts from.
    pub column: String,
    /// The steps to walk, in order. Empty means the document itself.
    pub steps: Vec<Step>,
    /// `->>`/`#>>` rather than `->`/`#>`: the value is wanted as text.
    pub as_text: bool,
}

impl PathExpr {
    /// A stable name for this path, used as the key an index is stored and
    /// looked up under.
    ///
    /// Canonical rather than as-written, so `doc -> 'a' ->> 'b'` and
    /// `doc->'a'->>'b'` are one index. `as_text` is deliberately **not** part
    /// of it: `->` and `->>` differ only in how the value is rendered, not in
    /// which value it is, so one index serves both.
    pub fn index_key(&self) -> String {
        let mut key = self.column.to_ascii_lowercase();
        for step in &self.steps {
            match step {
                Step::Key(k) => {
                    key.push_str("->'");
                    key.push_str(&k.replace('\'', "''"));
                    key.push('\'');
                }
                Step::Index(i) => {
                    key.push_str("->");
                    key.push_str(&i.to_string());
                }
            }
        }
        key
    }

    /// Resolves this path against an encoded document.
    pub fn resolve<'a>(&self, document: &'a [u8]) -> Option<&'a [u8]> {
        get_path(document, &self.steps)
    }
}

/// Parses `col->'a'->>'b'`, `col#>>'{a,b}'` and the like.
///
/// Returns `None` for anything that is not a path expression, which is the
/// common case — this runs against arbitrary SQL fragments looking for one.
pub fn parse_path_expression(expr: &str) -> Option<PathExpr> {
    let text = expr.trim();
    // A whole expression may be parenthesised, as it must be in `CREATE INDEX
    // ON t ((doc->>'k'))`.
    let text = strip_outer_parens(text);

    let (column, mut rest) = take_identifier(text)?;
    if rest.trim_start().is_empty() {
        return None; // a bare column is not a path
    }

    let mut steps = Vec::new();
    let mut as_text = false;
    loop {
        let trimmed = rest.trim_start();
        if trimmed.is_empty() {
            break;
        }
        // Longest operator first: `->>` before `->`, `#>>` before `#>`.
        let (operator, after) = if let Some(after) = trimmed.strip_prefix("#>>") {
            ("#>>", after)
        } else if let Some(after) = trimmed.strip_prefix("#>") {
            ("#>", after)
        } else if let Some(after) = trimmed.strip_prefix("->>") {
            ("->>", after)
        } else if let Some(after) = trimmed.strip_prefix("->") {
            ("->", after)
        } else {
            return None; // trailing text that is not a step
        };

        // Only the final operator decides the rendering, and a `->>` may only
        // be final: `a ->> 'b' -> 'c'` would be text indexed as a document.
        if as_text {
            return None;
        }
        as_text = operator.ends_with(">>") && operator.len() == 3;

        if operator.starts_with('#') {
            let (literal, after) = take_quoted(after.trim_start())?;
            for part in parse_brace_array(&literal)? {
                steps.push(step_from_text(&part));
            }
            rest = after;
        } else {
            let after = after.trim_start();
            if after.starts_with('\'') {
                let (literal, after) = take_quoted(after)?;
                steps.push(Step::Key(literal));
                rest = after;
            } else {
                let (number, after) = take_integer(after)?;
                steps.push(Step::Index(number));
                rest = after;
            }
        }
    }

    if steps.is_empty() {
        return None;
    }
    Some(PathExpr { column, steps, as_text })
}

fn strip_outer_parens(text: &str) -> &str {
    let mut current = text.trim();
    loop {
        if !current.starts_with('(') || !current.ends_with(')') {
            return current;
        }
        // Only strip when the opening paren is the one the closing paren
        // matches — `(a)->(b)` must not lose its middle.
        let mut depth = 0usize;
        let mut matched_at_end = false;
        for (index, byte) in current.bytes().enumerate() {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        matched_at_end = index == current.len() - 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !matched_at_end {
            return current;
        }
        current = current[1..current.len() - 1].trim();
    }
}

fn take_identifier(text: &str) -> Option<(String, &str)> {
    let text = text.trim_start();
    if let Some(rest) = text.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some((rest[..end].to_string(), &rest[end + 1..]));
    }
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    if end == 0 {
        return None;
    }
    Some((text[..end].to_string(), &text[end..]))
}

fn take_quoted(text: &str) -> Option<(String, &str)> {
    let rest = text.strip_prefix('\'')?;
    let mut value = String::new();
    let mut chars = rest.char_indices();
    while let Some((index, c)) = chars.next() {
        if c != '\'' {
            value.push(c);
            continue;
        }
        // `''` is an escaped quote, anything else ends the literal.
        match chars.clone().next() {
            Some((_, '\'')) => {
                value.push('\'');
                chars.next();
            }
            _ => return Some((value, &rest[index + 1..])),
        }
    }
    None
}

fn take_integer(text: &str) -> Option<(i64, &str)> {
    let negative = text.starts_with('-');
    let digits_at = if negative { 1 } else { 0 };
    let end = text[digits_at..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| i + digits_at)
        .unwrap_or(text.len());
    if end == digits_at {
        return None;
    }
    Some((text[..end].parse().ok()?, &text[end..]))
}

/// `{a,b,2}` as its parts — PostgreSQL's text form of a path array.
fn parse_brace_array(text: &str) -> Option<Vec<String>> {
    let inner = text.trim().strip_prefix('{')?.strip_suffix('}')?;
    if inner.trim().is_empty() {
        return None;
    }
    Some(inner.split(',').map(|p| p.trim().trim_matches('"').to_string()).collect())
}

/// A `#>` path element: digits are an array index, anything else a key.
///
/// This is PostgreSQL's rule and it is genuinely ambiguous — a key that is all
/// digits is unreachable through `#>`. `->'0'` addresses it unambiguously.
fn step_from_text(part: &str) -> Step {
    match part.parse::<i64>() {
        Ok(index) => Step::Index(index),
        Err(_) => Step::Key(part.to_string()),
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    fn key(k: &str) -> Step {
        Step::Key(k.to_string())
    }

    #[test]
    fn the_ordinary_forms_parse() {
        let p = parse_path_expression("doc->>'status'").expect("parses");
        assert_eq!(p.column, "doc");
        assert_eq!(p.steps, vec![key("status")]);
        assert!(p.as_text);

        let p = parse_path_expression("doc->'a'").expect("parses");
        assert_eq!(p.steps, vec![key("a")]);
        assert!(!p.as_text);

        let p = parse_path_expression("doc->'a'->>'b'").expect("parses");
        assert_eq!(p.steps, vec![key("a"), key("b")]);
        assert!(p.as_text);

        let p = parse_path_expression("doc->0->>'name'").expect("parses");
        assert_eq!(p.steps, vec![Step::Index(0), key("name")]);
    }

    #[test]
    fn whitespace_quoting_and_parens_do_not_change_the_meaning() {
        let canonical = parse_path_expression("doc->'a'->>'b'").unwrap().index_key();
        for variant in [
            "doc -> 'a' ->> 'b'",
            "  doc->'a'  ->>  'b'  ",
            "(doc->'a'->>'b')",
            "((doc->'a'->>'b'))",
            "\"doc\"->'a'->>'b'",
            "DOC->'a'->>'b'",
        ] {
            let parsed = parse_path_expression(variant)
                .unwrap_or_else(|| panic!("{} should parse", variant));
            assert_eq!(parsed.index_key(), canonical, "{} keyed differently", variant);
        }
    }

    /// `->` and `->>` address the same value, so they must share one index.
    #[test]
    fn the_rendering_operator_does_not_change_the_index_key() {
        let a = parse_path_expression("doc->'a'").unwrap();
        let b = parse_path_expression("doc->>'a'").unwrap();
        assert_ne!(a.as_text, b.as_text);
        assert_eq!(a.index_key(), b.index_key());
    }

    #[test]
    fn the_hash_forms_parse() {
        let p = parse_path_expression("doc#>>'{a,b}'").expect("parses");
        assert_eq!(p.steps, vec![key("a"), key("b")]);
        assert!(p.as_text);

        let p = parse_path_expression("doc#>'{a,2,c}'").expect("parses");
        assert_eq!(p.steps, vec![key("a"), Step::Index(2), key("c")]);
        assert!(!p.as_text);
    }

    #[test]
    fn a_quoted_key_may_contain_anything() {
        let p = parse_path_expression("doc->>'a key with spaces'").unwrap();
        assert_eq!(p.steps, vec![key("a key with spaces")]);

        let p = parse_path_expression("doc->>'it''s'").unwrap();
        assert_eq!(p.steps, vec![key("it's")]);

        let p = parse_path_expression("doc->>'->'").unwrap();
        assert_eq!(p.steps, vec![key("->")]);
    }

    #[test]
    fn what_is_not_a_path_is_refused() {
        for text in [
            "doc",                 // no step
            "",                    // nothing
            "doc->",               // no key
            "doc->>",              // no key
            "'literal'",           // not an identifier
            "doc->'a' extra",      // trailing text
            "doc->>'a'->>'b'",     // text cannot be indexed further
            "doc->>'a'->'b'",      // same
            "count(*)",            // an ordinary expression
            "a + b",
            "doc#>'{}'",           // an empty path array
            "doc#>'notbraces'",
        ] {
            assert!(
                parse_path_expression(text).is_none(),
                "{:?} should not parse as a path",
                text
            );
        }
    }

    #[test]
    fn a_parsed_path_resolves_against_a_document() {
        let document = encode(&serde_json::json!({
            "a": {"b": [{"c": "found"}]},
            "status": "live",
        }));
        let p = parse_path_expression("doc->>'status'").unwrap();
        assert_eq!(as_text(p.resolve(&document).unwrap()).as_deref(), Some("live"));

        let p = parse_path_expression("doc->'a'->'b'->0->>'c'").unwrap();
        assert_eq!(as_text(p.resolve(&document).unwrap()).as_deref(), Some("found"));

        let p = parse_path_expression("doc#>>'{a,b,0,c}'").unwrap();
        assert_eq!(as_text(p.resolve(&document).unwrap()).as_deref(), Some("found"));

        let p = parse_path_expression("doc->>'missing'").unwrap();
        assert!(p.resolve(&document).is_none());
    }
}
