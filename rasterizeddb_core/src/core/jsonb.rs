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
//! **Numbers that are not integers keep their digits.** A `f64` would
//! round-trip most values and silently change the rest, and "silently changed
//! the value and reported success" is the failure this engine keeps having to
//! hunt. PostgreSQL stores a JSON number as `numeric`, so the text kept is
//! what `numeric` would print for it: every digit and the scale as written
//! (`1.10` stays `1.10`), an exponent written out (`1e2` is `100`, `1.5e-1`
//! is `0.15`), and no negative zero. Only the lexeme is read, never a float
//! serde_json made of it; see [`write_numeric_text`]. A number whose written
//! out text would be long is stored in a short exponent form with the same
//! value and scale (`1e131071`, not 131072 digits), and written out only
//! when it is printed, as PostgreSQL does; see `Numeric::write_stored`.
//!
//! Documents written before this format kept a float's `Display` as a
//! number's text (`100.0` for `1e2`, `1.5` for `1.50`). They print, contain
//! and hash by the value they hold, but they are not the same bytes as the
//! same literal encoded now, so anything that compares documents as bytes
//! (`=`, grouping, hash keys) keeps the two apart until it compares values.
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
    // A `Number` that is not an integer already went through a `f64`, so its
    // digits are whatever that float prints as; this path cannot do better
    // than store those. `encode_text` reads the digits from the text instead.
    if let Some(i) = n.as_i64() {
        out.push(TAG_INT);
        out.extend_from_slice(&i.to_le_bytes());
    } else {
        let text = n.to_string();
        // A finite `f64` is always inside `numeric`'s range, so the fallback
        // is never taken; it keeps the old form rather than lose the value.
        if write_number_lexeme(out, text.as_bytes()).is_err() {
            out.push(TAG_NUM);
            write_len_prefixed(out, text.as_bytes());
        }
    }
}

/// PostgreSQL's refusal of a number `numeric` cannot hold.
pub const NUMERIC_OVERFLOW: &str = "value overflows numeric format";
/// The most digits `numeric` keeps before the decimal point: a weight of
/// 32767 base-10000 digits.
const NUMERIC_MAX_INT_DIGITS: i64 = 131_072;
/// The most digits `numeric` keeps after it, its `dscale` field's limit.
const NUMERIC_MAX_SCALE: i64 = 16_383;
/// The largest exponent `numeric_in` reads before refusing, `INT_MAX / 2`.
const NUMERIC_MAX_EXPONENT: i64 = 1_073_741_823;

/// Stores the number spelled by `lexeme`: as an integer when its `numeric`
/// value is an integer with no scale that fits `i64`, otherwise as text in
/// [`Numeric::write_stored`]'s form.
///
/// `1e2` and `100` therefore encode to the same bytes, as they print the same;
/// `1.0` keeps its scale and so stays text.
fn write_number_lexeme(out: &mut Vec<u8>, lexeme: &[u8]) -> Result<(), &'static str> {
    // The common case, a short integer, needs no rewriting: it cannot carry
    // an exponent, a scale or more digits than an `i64` holds.
    let digits = lexeme.strip_prefix(b"-").unwrap_or(lexeme);
    if !digits.is_empty() && digits.len() <= 18 && digits.iter().all(u8::is_ascii_digit) {
        let mut v: i64 = 0;
        for d in digits {
            v = v * 10 + (d - b'0') as i64;
        }
        out.push(TAG_INT);
        let v = if digits.len() < lexeme.len() { -v } else { v };
        out.extend_from_slice(&v.to_le_bytes());
        return Ok(());
    }
    let number = Numeric::parse(lexeme)?;
    if let Some(v) = number.as_i64() {
        out.push(TAG_INT);
        out.extend_from_slice(&v.to_le_bytes());
        return Ok(());
    }
    out.push(TAG_NUM);
    let len_at = out.len();
    out.extend_from_slice(&[0u8; 4]);
    number.write_stored(out);
    let len = (out.len() - len_at - 4) as u32;
    out[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// Appends what PostgreSQL's `numeric` prints for the JSON number `lexeme`.
///
/// `numeric_in` keeps every digit and a display scale of the fraction digits
/// written less the exponent, never below zero; `numeric_out` prints the value
/// in plain positional notation at that scale and has no negative zero. So
/// `1.50` is `1.50`, `1.50e1` is `15.0`, `1e-2` is `0.01`, `1e2` is `100` and
/// `-0.0` is `0.0`. A value outside `numeric`'s range is refused with
/// PostgreSQL's own message, which also bounds how long the text can get.
///
/// It accepts every form a number has been stored in: the exponent form this
/// build stores a long number in, positional text, and what earlier builds
/// stored, which is a float's `Display` (`1e+20`, `-0.0`). So a stored number
/// prints the same however it was kept.
fn write_numeric_text(lexeme: &[u8], out: &mut Vec<u8>) -> Result<(), &'static str> {
    Numeric::parse(lexeme)?.write_positional(out);
    Ok(())
}

/// The longest positional text a number is always stored as. Past it, the
/// exponent form is stored when it is shorter; see [`Numeric::write_stored`].
const POSITIONAL_UP_TO: i64 = 40;

/// A JSON number as `numeric_in` reads it, before anything is written out:
/// the digits as written, where the exponent puts the decimal point among
/// them, and the display scale.
struct Numeric<'a> {
    /// Never set for zero, which has no sign in `numeric`.
    negative: bool,
    /// The digits before and after the decimal point as written. Together
    /// they are the run that the positions below count in.
    int: &'a [u8],
    frac: &'a [u8],
    /// The first nonzero digit of the run.
    lead: i64,
    /// One past the last nonzero digit of the run; `lead` when it is zero.
    end: i64,
    /// Where the decimal point falls in the run once the exponent has moved
    /// it, which may be outside the run.
    point: i64,
    /// How many digits print after the point.
    scale: i64,
}

impl<'a> Numeric<'a> {
    fn parse(lexeme: &'a [u8]) -> Result<Numeric<'a>, &'static str> {
        const MALFORMED: &str = "a stored number is not a JSON number";
        let (negative, rest) = match lexeme.split_first() {
            Some((b'-', rest)) => (true, rest),
            _ => (false, lexeme),
        };
        let mantissa_end = rest
            .iter()
            .position(|b| *b == b'e' || *b == b'E')
            .unwrap_or(rest.len());
        let (int, frac) = match rest[..mantissa_end].iter().position(|b| *b == b'.') {
            Some(dot) => (&rest[..dot], &rest[dot + 1..mantissa_end]),
            None => (&rest[..mantissa_end], &rest[..0]),
        };
        if int.is_empty() || !int.iter().chain(frac).all(u8::is_ascii_digit) {
            return Err(MALFORMED);
        }
        if frac.is_empty() && mantissa_end > int.len() {
            // A point with nothing after it.
            return Err(MALFORMED);
        }
        let mut exponent: i64 = 0;
        if mantissa_end < rest.len() {
            let mut e = &rest[mantissa_end + 1..];
            let negative_exp = match e.first() {
                Some(b'-') => {
                    e = &e[1..];
                    true
                }
                Some(b'+') => {
                    e = &e[1..];
                    false
                }
                _ => false,
            };
            if e.is_empty() || !e.iter().all(u8::is_ascii_digit) {
                return Err(MALFORMED);
            }
            for d in e {
                exponent = exponent * 10 + (d - b'0') as i64;
                // Checked per digit, as `numeric_in` does, so a long exponent
                // is refused before it can overflow.
                if exponent > NUMERIC_MAX_EXPONENT {
                    return Err(NUMERIC_OVERFLOW);
                }
            }
            if negative_exp {
                exponent = -exponent;
            }
        }

        let mut number = Numeric {
            negative: false,
            int,
            frac,
            lead: 0,
            end: 0,
            point: int.len() as i64 + exponent,
            scale: (frac.len() as i64 - exponent).max(0),
        };
        if number.scale > NUMERIC_MAX_SCALE {
            return Err(NUMERIC_OVERFLOW);
        }
        let total = (int.len() + frac.len()) as i64;
        number.lead = (0..total).take_while(|&i| number.digit(i) == b'0').count() as i64;
        let trailing = (number.lead..total)
            .rev()
            .take_while(|&i| number.digit(i) == b'0')
            .count() as i64;
        number.end = total - trailing;
        if !number.is_zero() && number.point - number.lead > NUMERIC_MAX_INT_DIGITS {
            return Err(NUMERIC_OVERFLOW);
        }
        number.negative = negative && !number.is_zero();
        Ok(number)
    }

    fn is_zero(&self) -> bool {
        self.lead == self.end
    }

    /// The digit at position `i` of the run, and zero outside it.
    fn digit(&self, i: i64) -> u8 {
        let int = self.int.len() as i64;
        if i < 0 || i >= int + self.frac.len() as i64 {
            b'0'
        } else if i < int {
            self.int[i as usize]
        } else {
            self.frac[(i - int) as usize]
        }
    }

    /// The value, when it is an integer with no scale that fits `i64`.
    fn as_i64(&self) -> Option<i64> {
        if self.scale > 0 {
            return None;
        }
        if self.is_zero() {
            return Some(0);
        }
        if self.point - self.lead > 19 {
            return None;
        }
        let mut v: i128 = 0;
        for i in self.lead..self.point {
            v = v * 10 + (self.digit(i) - b'0') as i128;
        }
        i64::try_from(if self.negative { -v } else { v }).ok()
    }

    /// How long [`Numeric::write_positional`]'s text is.
    fn positional_len(&self) -> i64 {
        let int = if !self.is_zero() && self.point > self.lead {
            self.point - self.lead
        } else {
            1
        };
        let frac = if self.scale > 0 { 1 + self.scale } else { 0 };
        self.negative as i64 + int + frac
    }

    /// `numeric_out`'s text: plain positional notation at the display scale.
    fn write_positional(&self, out: &mut Vec<u8>) {
        out.reserve(self.positional_len() as usize);
        if self.negative {
            out.push(b'-');
        }
        if !self.is_zero() && self.point > self.lead {
            out.extend((self.lead..self.point).map(|i| self.digit(i)));
        } else {
            out.push(b'0');
        }
        if self.scale > 0 {
            out.push(b'.');
            out.extend((self.point..self.point + self.scale).map(|i| self.digit(i)));
        }
    }

    /// One past the last digit the exponent form writes. With a scale that is
    /// the last digit printed, so `1.000e-45` keeps its zeros; without one it
    /// is the last nonzero digit, and the exponent stands for the rest.
    fn compact_end(&self) -> i64 {
        if self.scale > 0 {
            self.point + self.scale
        } else {
            self.end
        }
    }

    /// The exponent [`Numeric::write_compact`] writes after its first digit.
    fn compact_exponent(&self) -> i64 {
        self.point - self.lead - 1
    }

    /// How long [`Numeric::write_compact`]'s text is.
    fn compact_len(&self) -> i64 {
        let decimal_len =
            |v: i64| (v < 0) as i64 + v.unsigned_abs().checked_ilog10().unwrap_or(0) as i64 + 1;
        if self.is_zero() {
            return if self.scale > 0 { 3 + decimal_len(self.scale) } else { 1 };
        }
        let digits = self.compact_end() - self.lead;
        self.negative as i64 + digits + (digits > 1) as i64 + 1 + decimal_len(self.compact_exponent())
    }

    /// One digit, the point and the rest of the digits, then the exponent:
    /// `1e131071`, `1.000e-45`, or `0e-16383` for a zero with a scale.
    /// `numeric_in` reads it back as the same value at the same scale, so it
    /// prints as the positional text does.
    fn write_compact(&self, out: &mut Vec<u8>) {
        if self.is_zero() {
            out.push(b'0');
            if self.scale > 0 {
                out.extend_from_slice(b"e-");
                crate::core::value_codec::write_i64(out, self.scale);
            }
            return;
        }
        if self.negative {
            out.push(b'-');
        }
        out.push(self.digit(self.lead));
        let end = self.compact_end();
        if end - self.lead > 1 {
            out.push(b'.');
            out.extend((self.lead + 1..end).map(|i| self.digit(i)));
        }
        out.push(b'e');
        crate::core::value_codec::write_i64(out, self.compact_exponent());
    }

    /// The text a `TAG_NUM` holds. A number keeps its positional text, which
    /// is what it prints as, unless that is long and the exponent form is
    /// shorter. `numeric` allows 131072 digits before the point and 16383
    /// after it, so expanding at encode time stored `1e131071`, eight bytes
    /// of input, as 131072, and a short literal could build a document of
    /// gigabytes. PostgreSQL expands only when printing, and so does
    /// [`render_text_into`].
    ///
    /// The choice depends only on the value and its scale, never on how the
    /// number was spelled, so spellings that print alike are stored alike.
    fn write_stored(&self, out: &mut Vec<u8>) {
        if self.positional_len() <= self.compact_len().max(POSITIONAL_UP_TO) {
            self.write_positional(out);
        } else {
            self.write_compact(out);
        }
    }
}

/// The span of the next JSON number in `text` at or after `from`, skipping
/// strings, as serde_json's lexer would read it.
///
/// [`encode_text`] calls this once per number serde_json hands it, so the two
/// must agree on where numbers are. They do because serde_json only reaches
/// a number where a value starts with `-` or a digit, outside a string, and
/// reads the same greedy grammar: an optional `-`, `0` or a run of digits, a
/// `.` with digits, an exponent with digits. Text serde_json would refuse is
/// refused before a misread lexeme could be used.
fn next_number_lexeme(text: &[u8], mut at: usize) -> Option<(usize, usize)> {
    let digits_from = |mut i: usize| {
        while i < text.len() && text[i].is_ascii_digit() {
            i += 1;
        }
        i
    };
    while at < text.len() {
        match text[at] {
            b'"' => {
                at += 1;
                loop {
                    at += memchr::memchr2(b'"', b'\\', text.get(at..)?)?;
                    if text[at] == b'"' {
                        at += 1;
                        break;
                    }
                    // An escape: the byte after the backslash cannot end the
                    // string.
                    at += 2;
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = at;
                let mut i = at + (text[at] == b'-') as usize;
                if text.get(i) == Some(&b'0') {
                    i += 1;
                } else {
                    i = digits_from(i);
                }
                if text.get(i) == Some(&b'.') && text.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    i = digits_from(i + 1);
                }
                if matches!(text.get(i), Some(b'e' | b'E')) {
                    let mut j = i + 1;
                    if matches!(text.get(j), Some(b'+' | b'-')) {
                        j += 1;
                    }
                    if text.get(j).is_some_and(u8::is_ascii_digit) {
                        i = digits_from(j);
                    }
                }
                return Some((start, i));
            }
            _ => at += 1,
        }
    }
    None
}

/// `text` with every number replaced by a `0` and spaces of the same length,
/// for serde_json to lex when a number would overflow its `f64`.
///
/// The lengths are kept so that every error serde_json still finds is
/// reported at the same line and column.
fn mask_numbers(text: &str) -> String {
    let mut masked = text.as_bytes().to_vec();
    let mut at = 0;
    while let Some((start, end)) = next_number_lexeme(text.as_bytes(), at) {
        // A lone `-` is not a number, and serde_json's complaint about it
        // must stay its own.
        if end > start + (masked[start] == b'-') as usize {
            masked[start] = b'0';
            masked[start + 1..end].fill(b' ');
        }
        at = end;
    }
    // Only ASCII bytes outside strings were replaced, by ASCII.
    String::from_utf8(masked).expect("masking keeps UTF-8")
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
    let mut entries: Vec<(&str, &Value)> = fields.iter().map(|(k, v)| (k.as_str(), v)).collect();
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

/// Encodes a document straight from its JSON text.
///
/// The same bytes as `encode(&serde_json::from_str(text)?)`, without the
/// `Value` tree in between: no `String` per key and value, no map node per
/// entry, no `Vec` per array. serde_json still does the lexing — unescaping,
/// number parsing, the depth limit and every error message are its own — and
/// hands each value to [`ValueSeed`], which writes the encoding as it goes.
/// A composite's offset table cannot be written until its last element is
/// known, so its children are laid out first and the header is put in front
/// of them when it closes; an object whose keys did not arrive sorted and
/// unique is re-laid in key order then, last value winning, as
/// [`write_object`] does.
///
/// One deliberate divergence: with serde_json's `raw_value` feature on (it is,
/// in this workspace), a `Value` reads an object whose first key is
/// `"$serde_json::private::RawValue"` as a *raw value* and stores the document
/// its string holds instead of the object. That is a serde_json internal
/// leaking into user data; here the object is stored as written.
///
/// Numbers are the other: serde_json without `arbitrary_precision` hands a
/// fraction, an exponent or a wide integer over as a `f64`, already rounded
/// (`1.000000000000000000001` is `1.0`), and refuses one past the `f64` range
/// that `numeric` holds (`1e400`). So the value serde_json reports is ignored
/// and the number's lexeme is read from `text` instead, by
/// [`next_number_lexeme`] stepping along in step with the parse. When
/// serde_json refuses a number as out of range, the parse is run again over
/// the text with its numbers masked, which serde_json cannot overflow on.
/// That feature is not turned on because it would change `Number` for every
/// other user of serde_json in the build.
pub fn encode_text(text: &str) -> Result<Vec<u8>, String> {
    match encode_text_from(text, text, MAX_DOCUMENT_BYTES) {
        Err(e) if e.starts_with("number out of range") => {
            encode_text_from(&mask_numbers(text), text, MAX_DOCUMENT_BYTES)
        }
        result => result,
    }
}

/// The largest encoded document. Every offset in a document is a `u32`, so
/// one past 4 GiB would be written with truncated offsets and read back as
/// other values. The bound is half that because an object whose keys arrive
/// out of order is re-laid in scratch after its children while those are
/// still in place.
const MAX_DOCUMENT_BYTES: usize = i32::MAX as usize;

/// The refusal of a document past [`MAX_DOCUMENT_BYTES`].
const DOCUMENT_TOO_LARGE: &str = "total size of jsonb document exceeds the maximum of 2147483647 bytes";

/// [`encode_text`]'s parse of `lexed`, taking each number's lexeme from
/// `text`, which differs from `lexed` at most in its numbers, and refusing
/// to grow the encoding past `limit` bytes.
fn encode_text_from(lexed: &str, text: &str, limit: usize) -> Result<Vec<u8>, String> {
    let mut builder = Builder {
        out: Vec::with_capacity(text.len() + 16),
        slots: Vec::new(),
        order: Vec::new(),
        text: text.as_bytes(),
        cursor: 0,
        limit,
    };
    let mut de = serde_json::Deserializer::from_str(lexed);
    serde::de::DeserializeSeed::deserialize(ValueSeed(&mut builder), &mut de)
        .map_err(|e| e.to_string())?;
    de.end().map_err(|e| e.to_string())?;
    Ok(builder.out)
}

/// The output of [`encode_text`] and the scratch its composites share.
struct Builder<'t> {
    out: Vec<u8>,
    /// The document's text, and how far into it numbers have been read.
    text: &'t [u8],
    cursor: usize,
    /// For each open composite, its children: `(key_off, key_len, val_off,
    /// val_len)` as absolute positions in `out` (keys are zero-length for an
    /// array). A stack — a composite pushes its own above its parent's and
    /// truncates back when it closes.
    slots: Vec<[u32; 4]>,
    /// Scratch for sorting an object's entries when it closes.
    order: Vec<u32>,
    /// How large `out` may grow, scratch included; see
    /// [`MAX_DOCUMENT_BYTES`].
    limit: usize,
}

impl Builder<'_> {
    /// Refuses the document when `out` and `more` bytes after it would pass
    /// the limit. A composite checks it when it closes, before it writes its
    /// offset table, so no offset past the limit is ever written. Only a
    /// composite can grow the document much past its text: a scalar is
    /// stored about as long as it was written.
    fn room<E: serde::de::Error>(&self, more: usize) -> Result<(), E> {
        if self.out.len().saturating_add(more) > self.limit {
            return Err(E::custom(DOCUMENT_TOO_LARGE));
        }
        Ok(())
    }

    fn string(&mut self, text: &str) {
        self.out.push(TAG_STR);
        write_len_prefixed(&mut self.out, text.as_bytes());
    }

    /// The number serde_json has just read, from its lexeme in the text.
    fn number<E: serde::de::Error>(&mut self) -> Result<(), E> {
        let Some((start, end)) = next_number_lexeme(self.text, self.cursor) else {
            // Cannot happen while the two lexers agree; refused rather than
            // stored as some other number.
            return Err(E::custom("a number could not be located in the document text"));
        };
        self.cursor = end;
        write_number_lexeme(&mut self.out, &self.text[start..end]).map_err(E::custom)
    }

    /// Puts a composite's header and offset table in front of its children,
    /// which occupy `out[start..]`, described by `slots[base..]`.
    fn close<E: serde::de::Error>(&mut self, tag: u8, start: usize, base: usize) -> Result<(), E> {
        let count = self.slots.len() - base;
        let stride = if tag == TAG_ARR { ARR_ENTRY } else { OBJ_ENTRY };
        let header = 5 + count * stride;

        // An object's entries must be sorted and unique. Usually they already
        // are, and the children can stay where they are, as for an array.
        let in_order = tag == TAG_ARR
            || self.slots[base..].windows(2).all(|w| {
                let a = &self.out[w[0][0] as usize..(w[0][0] + w[0][1]) as usize];
                let b = &self.out[w[1][0] as usize..(w[1][0] + w[1][1]) as usize];
                a < b
            });

        if in_order {
            self.room(header)?;
            let end = self.out.len();
            self.out.resize(end + header, 0);
            self.out.copy_within(start..end, start + header);
            let body_at = start + header;
            self.out[start] = tag;
            self.out[start + 1..start + 5].copy_from_slice(&(count as u32).to_le_bytes());
            for (index, slot) in self.slots[base..].iter().enumerate() {
                let entry = start + 5 + index * stride;
                // Positions moved right by `header`; relative to the body they
                // are what they were relative to `start`.
                let rel = |abs: u32| (abs as usize - start) as u32;
                if tag == TAG_ARR {
                    self.out[entry..entry + 4].copy_from_slice(&rel(slot[2]).to_le_bytes());
                    self.out[entry + 4..entry + 8].copy_from_slice(&slot[3].to_le_bytes());
                } else {
                    self.out[entry..entry + 4].copy_from_slice(&rel(slot[0]).to_le_bytes());
                    self.out[entry + 4..entry + 8].copy_from_slice(&slot[1].to_le_bytes());
                    self.out[entry + 8..entry + 12].copy_from_slice(&rel(slot[2]).to_le_bytes());
                    self.out[entry + 12..entry + 16].copy_from_slice(&slot[3].to_le_bytes());
                }
            }
            debug_assert_eq!(body_at + (end - start), self.out.len());
            self.slots.truncate(base);
            return Ok(());
        }

        // Sorted by key, ties by arrival, then each run of equal keys reduced
        // to its last arrival — `write_object`'s stable sort and last-wins
        // dedup, without its allocation.
        self.order.clear();
        self.order.extend(0..count as u32);
        {
            let (out, slots) = (&self.out, &self.slots[base..]);
            let key = |i: u32| {
                let s = &slots[i as usize];
                &out[s[0] as usize..(s[0] + s[1]) as usize]
            };
            self.order
                .sort_unstable_by(|a, b| key(*a).cmp(key(*b)).then(a.cmp(b)));
            let mut kept = 0;
            for at in 0..self.order.len() {
                let last_of_run =
                    at + 1 == self.order.len() || key(self.order[at]) != key(self.order[at + 1]);
                if last_of_run {
                    self.order[kept] = self.order[at];
                    kept += 1;
                }
            }
            self.order.truncate(kept);
        }

        // Laid out after the scratch, then moved down over it.
        let kept = self.order.len();
        let table = 5 + kept * OBJ_ENTRY;
        self.room(table + (self.out.len() - start))?;
        let final_at = self.out.len();
        self.out.resize(final_at + table, 0);
        self.out[final_at] = TAG_OBJ;
        self.out[final_at + 1..final_at + 5].copy_from_slice(&(kept as u32).to_le_bytes());
        let body_at = final_at + table;
        for index in 0..kept {
            let slot = self.slots[base + self.order[index] as usize];
            let key_off = self.out.len() - body_at;
            self.out
                .extend_from_within(slot[0] as usize..(slot[0] + slot[1]) as usize);
            let val_off = self.out.len() - body_at;
            self.out
                .extend_from_within(slot[2] as usize..(slot[2] + slot[3]) as usize);
            let entry = final_at + 5 + index * OBJ_ENTRY;
            self.out[entry..entry + 4].copy_from_slice(&(key_off as u32).to_le_bytes());
            self.out[entry + 4..entry + 8].copy_from_slice(&slot[1].to_le_bytes());
            self.out[entry + 8..entry + 12].copy_from_slice(&(val_off as u32).to_le_bytes());
            self.out[entry + 12..entry + 16].copy_from_slice(&slot[3].to_le_bytes());
        }
        let end = self.out.len();
        self.out.copy_within(final_at..end, start);
        self.out.truncate(start + (end - final_at));
        self.slots.truncate(base);
        Ok(())
    }
}

/// Writes one JSON value into a [`Builder`]; see [`encode_text`].
struct ValueSeed<'b, 't>(&'b mut Builder<'t>);

impl<'de> serde::de::DeserializeSeed<'de> for ValueSeed<'_, '_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for ValueSeed<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<(), E> {
        self.0.out.push(if v { TAG_TRUE } else { TAG_FALSE });
        Ok(())
    }

    // Every number arm ignores the value serde_json made and reads the
    // lexeme; see `encode_text`. Only the first three are reached from JSON
    // text, the wide ones are here so no number could bypass the lexeme.

    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        self.0.number()
    }

    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        self.0.number()
    }

    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        self.0.number()
    }

    fn visit_i128<E: serde::de::Error>(self, _: i128) -> Result<(), E> {
        self.0.number()
    }

    fn visit_u128<E: serde::de::Error>(self, _: u128) -> Result<(), E> {
        self.0.number()
    }

    fn visit_str<E>(self, v: &str) -> Result<(), E> {
        self.0.string(v);
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        self.0.out.push(TAG_NULL);
        Ok(())
    }

    fn visit_none<E>(self) -> Result<(), E> {
        self.0.out.push(TAG_NULL);
        Ok(())
    }

    fn visit_some<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let b = self.0;
        let (start, base) = (b.out.len(), b.slots.len());
        loop {
            let at = b.out.len();
            if seq.next_element_seed(ValueSeed(&mut *b))?.is_none() {
                break;
            }
            let len = (b.out.len() - at) as u32;
            b.slots.push([0, 0, at as u32, len]);
        }
        b.close(TAG_ARR, start, base)
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let b = self.0;
        let (start, base) = (b.out.len(), b.slots.len());
        loop {
            let key_at = b.out.len();
            if map.next_key_seed(KeySeed(&mut *b))?.is_none() {
                break;
            }
            let val_at = b.out.len();
            map.next_value_seed(ValueSeed(&mut *b))?;
            b.slots.push([
                key_at as u32,
                (val_at - key_at) as u32,
                val_at as u32,
                (b.out.len() - val_at) as u32,
            ]);
        }
        b.close(TAG_OBJ, start, base)
    }
}

/// Writes an object key's bytes, bare, into a [`Builder`].
struct KeySeed<'b, 't>(&'b mut Builder<'t>);

impl<'de> serde::de::DeserializeSeed<'de> for KeySeed<'_, '_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> serde::de::Visitor<'de> for KeySeed<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a string key")
    }

    fn visit_str<E>(self, v: &str) -> Result<(), E> {
        self.0.out.extend_from_slice(v.as_bytes());
        Ok(())
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
    let tag = *bytes
        .first()
        .ok_or("a document ended where a value was expected")?;
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
            // A `Value` holds a non-integer only as a `f64`, so this is where
            // a decoded document loses digits, and a number past the `f64`
            // range does not decode at all. Nothing that answers a client
            // goes through here: text comes from `render_text_into`.
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
            Ok((
                Value::Array(items),
                total_len(bytes, body_at, table_at, count, ARR_ENTRY)?,
            ))
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
            Ok((
                Value::Object(fields),
                total_len(bytes, body_at, table_at, count, OBJ_ENTRY)?,
            ))
        }
        other => Err(format!("{:#04x} is not a JSON value tag", other)),
    }
}

fn read_len_prefixed(bytes: &[u8]) -> Result<(&[u8], usize), String> {
    let len = read_u32(bytes, 1)? as usize;
    let raw = bytes
        .get(5..5 + len)
        .ok_or("a length-prefixed value was truncated")?;
    Ok((raw, 5 + len))
}

fn composite_header(bytes: &[u8], stride: usize) -> Result<(usize, usize, usize), String> {
    let count = read_u32(bytes, 1)? as usize;
    let table_at: usize = 5;
    let body_at = table_at
        .checked_add(
            count
                .checked_mul(stride)
                .ok_or("a composite claims an impossible size")?,
        )
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
    let start = body_at
        .checked_add(offset)
        .ok_or("an offset is out of range")?;
    let end = start.checked_add(len).ok_or("a length is out of range")?;
    bytes
        .get(start..end)
        .ok_or_else(|| "a value is truncated".to_string())
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, String> {
    let raw = bytes
        .get(at..at + 4)
        .ok_or("a document ended inside a header")?;
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
    // A string is copied out of the document once; anything else is rendered
    // into the buffer that becomes the `String`, so neither form pays for a
    // second copy.
    if bytes.first() == Some(&TAG_STR) {
        let mut unused = Vec::new();
        let text = as_text_into(bytes, &mut unused)?;
        return Some(std::str::from_utf8(text).ok()?.to_string());
    }
    let mut out = Vec::with_capacity(bytes.len());
    as_text_into(bytes, &mut out)?;
    String::from_utf8(out).ok()
}

/// [`as_text`] without the `String`: a string value's UTF-8 is borrowed
/// straight from the document, and anything else is rendered into `scratch`
/// (cleared first) and borrowed from there.
///
/// This is what a `->>` predicate runs once per row, where the owned form cost
/// an allocation and a copy before the bytes were copied again into the
/// comparison's block. The answers are `as_text`'s, which is defined on top of
/// this so the two cannot drift.
pub fn as_text_into<'a>(bytes: &'a [u8], scratch: &'a mut Vec<u8>) -> Option<&'a [u8]> {
    match bytes.first()? {
        &TAG_NULL => None,
        &TAG_STR => {
            let (raw, _) = read_len_prefixed(bytes).ok()?;
            std::str::from_utf8(raw).ok()?;
            Some(raw)
        }
        _ => {
            scratch.clear();
            render_text_into(bytes, scratch).ok()?;
            Some(&scratch[..])
        }
    }
}

// ---------------------------------------------------------------------------
// containment
// ---------------------------------------------------------------------------

/// PostgreSQL's `jsonb @>`, answered on the encoded bytes of both documents.
///
/// Nothing is decoded and nothing allocated: a key is found by binary search
/// over the sorted offset table, and a nested value is a subslice. The rules
/// are PostgreSQL's `JsonbDeepContains`, which differ from a naive "every part
/// of the right is somewhere in the left" in three ways that each answered
/// wrongly before:
///
/// - **A bare scalar on the right is contained in a *top-level* array only.**
///   `'[1,2]' @> '2'` is true; `'{"a":[1,2]}' @> '{"a":1}'` and
///   `'[1,[2]]' @> '[2]'` are false — inside a structure a scalar matches only
///   an equal scalar, and an array element that is itself an array is a
///   container, never searched for scalars.
/// - **Numbers compare as numbers.** `'{"a":1.0}' @> '{"a":1}'` is true; JSON
///   value equality said false, because `1.0` and `1` are different `Number`s.
/// - **Types must agree at every level.** An object contains only an object,
///   an array only an array; `1` is not `"1"`.
///
/// `None` means a document is malformed.
pub fn contains(outer: &[u8], inner: &[u8]) -> Option<bool> {
    let (o, i) = (*outer.first()?, *inner.first()?);
    // PostgreSQL's first test: an object root contains only an object root.
    if (o == TAG_OBJ) != (i == TAG_OBJ) {
        return Some(false);
    }
    match (is_container(o), is_container(i)) {
        (true, true) => nested_contains(outer, inner),
        // A top-level scalar is PostgreSQL's raw-scalar pseudo-array of one
        // element: `[x] @> y` searches the scalars of the left, and a raw
        // scalar on the left contains no real array.
        (true, false) => array_has_scalar(&Composite::of(outer, ARR_ENTRY)?, outer, inner),
        (false, true) => Some(false),
        (false, false) => scalar_eq(outer, inner),
    }
}

/// PostgreSQL's `jsonb ? text`: whether `key` is a top-level key of an
/// object, a top-level **string** element of an array, or — for a document
/// that is a bare string — that string.
///
/// The array case is what `doc->'k' <> ''` could not express: `'["a","b"]' ?
/// 'a'` is true in PostgreSQL, while the path form looks up a key and finds
/// none. Only string elements count — `'[1]' ? '1'` is false — and only at the
/// top level. `None` when the document is malformed.
pub fn key_exists(doc: &[u8], key: &str) -> Option<bool> {
    match *doc.first()? {
        TAG_OBJ => Some(get_key(doc, key).is_some()),
        TAG_ARR => {
            let a = Composite::of(doc, ARR_ENTRY)?;
            for at in 0..a.count {
                let element = a.element(doc, at)?;
                if element.first() == Some(&TAG_STR)
                    && read_len_prefixed(element).ok()?.0 == key.as_bytes()
                {
                    return Some(true);
                }
            }
            Some(false)
        }
        TAG_STR => Some(read_len_prefixed(doc).ok()?.0 == key.as_bytes()),
        tag if tag <= TAG_OBJ => Some(false),
        _ => None,
    }
}

#[inline]
fn is_container(tag: u8) -> bool {
    tag == TAG_ARR || tag == TAG_OBJ
}

/// A composite's header, read once so a search does not re-read it per probe.
struct Composite {
    count: usize,
    table_at: usize,
    body_at: usize,
}

impl Composite {
    #[inline]
    fn of(bytes: &[u8], stride: usize) -> Option<Composite> {
        let (count, table_at, body_at) = composite_header(bytes, stride).ok()?;
        Some(Composite {
            count,
            table_at,
            body_at,
        })
    }

    /// The `index`th array element.
    #[inline]
    fn element<'a>(&self, bytes: &'a [u8], index: usize) -> Option<&'a [u8]> {
        let entry = self.table_at + index * ARR_ENTRY;
        slice_at(bytes, self.body_at, entry, entry + 4).ok()
    }

    /// The `index`th object entry's key and value.
    #[inline]
    fn entry<'a>(&self, bytes: &'a [u8], index: usize) -> Option<(&'a [u8], &'a [u8])> {
        let entry = self.table_at + index * OBJ_ENTRY;
        Some((
            slice_at(bytes, self.body_at, entry, entry + 4).ok()?,
            slice_at(bytes, self.body_at, entry + 8, entry + 12).ok()?,
        ))
    }
}

/// Containment between two values below the top level, or two top-level
/// containers.
fn nested_contains(outer: &[u8], inner: &[u8]) -> Option<bool> {
    let (o, i) = (*outer.first()?, *inner.first()?);
    match (o, i) {
        (TAG_OBJ, TAG_OBJ) => object_contains(outer, inner),
        (TAG_ARR, TAG_ARR) => array_contains(outer, inner),
        _ if !is_container(o) && !is_container(i) => scalar_eq(outer, inner),
        // A container against a scalar, or an object against an array.
        _ => Some(false),
    }
}

fn object_contains(outer: &[u8], inner: &[u8]) -> Option<bool> {
    let a = Composite::of(outer, OBJ_ENTRY)?;
    let b = Composite::of(inner, OBJ_ENTRY)?;
    // Keys are unique, so a right side with more keys cannot be contained.
    if b.count > a.count {
        return Some(false);
    }
    // Both sides are sorted, so each key's search starts where the last one
    // ended.
    let mut low = 0usize;
    for index in 0..b.count {
        let (key, want) = b.entry(inner, index)?;
        let mut high = a.count;
        let mut found = None;
        while low < high {
            let mid = (low + high) / 2;
            let (candidate, value) = a.entry(outer, mid)?;
            match candidate.cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    found = Some(value);
                    low = mid + 1;
                    break;
                }
            }
        }
        let Some(have) = found else {
            return Some(false);
        };
        if !nested_contains(have, want)? {
            return Some(false);
        }
    }
    Some(true)
}

fn array_contains(outer: &[u8], inner: &[u8]) -> Option<bool> {
    let a = Composite::of(outer, ARR_ENTRY)?;
    let b = Composite::of(inner, ARR_ENTRY)?;
    for index in 0..b.count {
        let want = b.element(inner, index)?;
        let found = if is_container(*want.first()?) {
            // A container is matched against the left's containers only.
            let mut any = false;
            for at in 0..a.count {
                let have = a.element(outer, at)?;
                if is_container(*have.first()?) && nested_contains(have, want)? {
                    any = true;
                    break;
                }
            }
            any
        } else {
            array_has_scalar(&a, outer, want)?
        };
        if !found {
            return Some(false);
        }
    }
    Some(true)
}

/// Whether an array holds an element equal to `scalar`. Only the scalar
/// elements are candidates: `[[2]]` does not hold `2`.
fn array_has_scalar(a: &Composite, outer: &[u8], scalar: &[u8]) -> Option<bool> {
    for at in 0..a.count {
        let have = a.element(outer, at)?;
        if !is_container(*have.first()?) && scalar_eq(have, scalar)? {
            return Some(true);
        }
    }
    Some(false)
}

/// Equality of two encoded scalars, as PostgreSQL's `equalsJsonbScalarValue`:
/// the same kind, then the same value — numerically, for numbers.
fn scalar_eq(a: &[u8], b: &[u8]) -> Option<bool> {
    let (ta, tb) = (*a.first()?, *b.first()?);
    // A tag this module never writes is a malformed document, not a value
    // that happens to differ.
    if ta > TAG_OBJ || tb > TAG_OBJ {
        return None;
    }
    // A scalar is exactly as long as its tag says. Without this the four bytes
    // of an `i32` 1 began with the tag of `false` and compared equal to it, and
    // the eight bytes of an `f64` 2.5 equalled `null`. Every slice this is
    // given — a column's value, a path's target, an element — is exact, so
    // only bytes that are not a document fail it.
    if !scalar_well_formed(a) || !scalar_well_formed(b) {
        return None;
    }
    let number = |t: u8| t == TAG_INT || t == TAG_NUM;
    Some(match (ta, tb) {
        (TAG_INT, TAG_INT) => a.get(1..9)? == b.get(1..9)?,
        _ if number(ta) && number(tb) => {
            let mut buf_a = [0u8; 20];
            let mut buf_b = [0u8; 20];
            numbers_equal(number_text(a, &mut buf_a)?, number_text(b, &mut buf_b)?)
        }
        (TAG_STR, TAG_STR) => read_len_prefixed(a).ok()?.0 == read_len_prefixed(b).ok()?.0,
        _ => ta == tb && matches!(ta, TAG_NULL | TAG_TRUE | TAG_FALSE),
    })
}

/// Whether `bytes` is one whole encoded scalar and nothing more. A container
/// passes; its own header is checked where it is read.
fn scalar_well_formed(bytes: &[u8]) -> bool {
    match bytes.first() {
        Some(&(TAG_NULL | TAG_FALSE | TAG_TRUE)) => bytes.len() == 1,
        Some(&TAG_INT) => bytes.len() == 9,
        Some(&(TAG_NUM | TAG_STR)) => {
            matches!(read_len_prefixed(bytes), Ok((_, used)) if used == bytes.len())
        }
        Some(_) => true,
        None => false,
    }
}

/// The decimal text of an encoded number. An integer is formatted into `buf`.
fn number_text<'a>(bytes: &'a [u8], buf: &'a mut [u8; 20]) -> Option<&'a [u8]> {
    match *bytes.first()? {
        TAG_INT => {
            let v = i64::from_le_bytes(bytes.get(1..9)?.try_into().ok()?);
            let mut n = v.unsigned_abs();
            let mut at = buf.len();
            loop {
                at -= 1;
                buf[at] = b'0' + (n % 10) as u8;
                n /= 10;
                if n == 0 {
                    break;
                }
            }
            if v < 0 {
                at -= 1;
                buf[at] = b'-';
            }
            Some(&buf[at..])
        }
        TAG_NUM => Some(read_len_prefixed(bytes).ok()?.0),
        _ => None,
    }
}

/// A JSON number's value, without its spelling: the sign, the significant
/// digits with no leading or trailing zeros, and where the decimal point falls
/// relative to the first of them. `1`, `1.0`, `1.00` and `1e0` agree; `0` and
/// `-0.0` are both zero.
struct NumberParts<'a> {
    negative: bool,
    int: &'a [u8],
    frac: &'a [u8],
    /// Leading zeros skipped across `int` then `frac`.
    lead: usize,
    /// Significant digits after `lead`, trailing zeros excluded.
    len: usize,
    /// Position of the decimal point after the first significant digit's
    /// predecessor: the value is `0.DIGITS × 10^point`.
    point: i64,
}

impl<'a> NumberParts<'a> {
    fn parse(text: &'a [u8]) -> Option<NumberParts<'a>> {
        let (negative, rest) = match text.first()? {
            b'-' => (true, &text[1..]),
            _ => (false, text),
        };
        let mantissa_end = rest
            .iter()
            .position(|b| *b == b'e' || *b == b'E')
            .unwrap_or(rest.len());
        let exponent: i64 = if mantissa_end < rest.len() {
            let e = std::str::from_utf8(&rest[mantissa_end + 1..]).ok()?;
            let e = e.strip_prefix('+').unwrap_or(e);
            // Far beyond anything a JSON number can mean; saturated so a
            // pathological exponent cannot wrap.
            e.parse::<i64>().ok().map(|v| v.clamp(-1 << 40, 1 << 40))?
        } else {
            0
        };
        let mantissa = &rest[..mantissa_end];
        let (int, frac) = match mantissa.iter().position(|b| *b == b'.') {
            Some(dot) => (&mantissa[..dot], &mantissa[dot + 1..]),
            None => (mantissa, &mantissa[..0]),
        };
        if !int.iter().chain(frac).all(u8::is_ascii_digit) || int.len() + frac.len() == 0 {
            return None;
        }
        let total = int.len() + frac.len();
        let digit = |i: usize| if i < int.len() { int[i] } else { frac[i - int.len()] };
        let lead = (0..total).take_while(|&i| digit(i) == b'0').count();
        let trail = (lead..total).rev().take_while(|&i| digit(i) == b'0').count();
        Some(NumberParts {
            negative,
            int,
            frac,
            lead,
            len: total - lead - trail,
            point: int.len() as i64 - lead as i64 + exponent,
        })
    }

    fn digits(&self) -> impl Iterator<Item = u8> + '_ {
        self.int
            .iter()
            .chain(self.frac.iter())
            .skip(self.lead)
            .take(self.len)
            .copied()
    }
}

/// Whether two JSON number spellings denote the same value.
fn numbers_equal(a: &[u8], b: &[u8]) -> bool {
    if a == b {
        return true;
    }
    let (Some(x), Some(y)) = (NumberParts::parse(a), NumberParts::parse(b)) else {
        return false;
    };
    match (x.len == 0, y.len == 0) {
        (true, true) => true,
        (false, false) => {
            x.negative == y.negative && x.point == y.point && x.len == y.len && x.digits().eq(y.digits())
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// equality
// ---------------------------------------------------------------------------

/// PostgreSQL's `jsonb = jsonb`, answered on the encoded bytes of both
/// documents.
///
/// Equal bytes are not the test. The encoder keeps a number's spelling, so
/// `1` and `1.0` are different bytes and the same value, and PostgreSQL calls
/// them equal at every depth: `'[1.0]' = '[1]'` and `'{"a":1e0}' = '{"a":1}'`
/// are both true. Everything else follows `scalar_eq`, which containment
/// already uses: the same kind, then the same value. Arrays compare in order;
/// object keys are sorted when a document is encoded, so two objects with the
/// same keys list them at the same positions. A bare scalar is not equal to
/// the one-element array holding it.
///
/// `None` means a document is malformed, or is not an encoded document at
/// all.
pub fn values_equal(a: &[u8], b: &[u8]) -> Option<bool> {
    let (ta, tb) = (*a.first()?, *b.first()?);
    if ta > TAG_OBJ || tb > TAG_OBJ {
        return None;
    }
    match (ta, tb) {
        (TAG_ARR, TAG_ARR) => {
            let (x, y) = (Composite::of(a, ARR_ENTRY)?, Composite::of(b, ARR_ENTRY)?);
            if x.count != y.count {
                return Some(false);
            }
            for at in 0..x.count {
                if !values_equal(x.element(a, at)?, y.element(b, at)?)? {
                    return Some(false);
                }
            }
            Some(true)
        }
        (TAG_OBJ, TAG_OBJ) => {
            let (x, y) = (Composite::of(a, OBJ_ENTRY)?, Composite::of(b, OBJ_ENTRY)?);
            if x.count != y.count {
                return Some(false);
            }
            for at in 0..x.count {
                let ((key_a, value_a), (key_b, value_b)) = (x.entry(a, at)?, y.entry(b, at)?);
                if key_a != key_b || !values_equal(value_a, value_b)? {
                    return Some(false);
                }
            }
            Some(true)
        }
        _ if is_container(ta) || is_container(tb) => Some(false),
        _ => scalar_eq(a, b),
    }
}

/// Bytes that are equal exactly when [`values_equal`] calls two documents
/// equal, for a hash index to key a document by.
///
/// A hash index keyed on the stored bytes would file `1.0` and `1` apart, and
/// a lookup for `doc = '1'` would then miss the row the scan returns, which is
/// a row silently missing from an indexed answer. So a number is written as
/// its value, the way `numbers_equal` reads it: zero as one byte whatever its
/// sign, anything else as its sign, decimal point and significant digits. A
/// number whose text cannot be read is kept as that text, which only the same
/// text equals, as in `numbers_equal`.
///
/// `None` when the document is malformed.
pub fn canonical_key(bytes: &[u8], out: &mut Vec<u8>) -> Option<()> {
    let tag = *bytes.first()?;
    match tag {
        TAG_NULL | TAG_FALSE | TAG_TRUE => out.push(tag),
        TAG_INT | TAG_NUM => {
            let mut buf = [0u8; 20];
            let text = number_text(bytes, &mut buf)?;
            out.push(TAG_NUM);
            match NumberParts::parse(text) {
                Some(parts) if parts.len == 0 => out.push(0),
                Some(parts) => {
                    out.push(if parts.negative { 1 } else { 2 });
                    out.extend_from_slice(&parts.point.to_le_bytes());
                    out.extend_from_slice(&(parts.len as u64).to_le_bytes());
                    out.extend(parts.digits());
                }
                None => {
                    out.push(3);
                    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
                    out.extend_from_slice(text);
                }
            }
        }
        TAG_STR => {
            let (raw, _) = read_len_prefixed(bytes).ok()?;
            out.push(TAG_STR);
            out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            out.extend_from_slice(raw);
        }
        TAG_ARR => {
            let a = Composite::of(bytes, ARR_ENTRY)?;
            out.push(TAG_ARR);
            out.extend_from_slice(&(a.count as u32).to_le_bytes());
            for at in 0..a.count {
                canonical_key(a.element(bytes, at)?, out)?;
            }
        }
        TAG_OBJ => {
            let o = Composite::of(bytes, OBJ_ENTRY)?;
            out.push(TAG_OBJ);
            out.extend_from_slice(&(o.count as u32).to_le_bytes());
            for at in 0..o.count {
                let (key, value) = o.entry(bytes, at)?;
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                canonical_key(value, out)?;
            }
        }
        _ => return None,
    }
    Some(())
}

/// `doc->'a'->>'b'`: a path written out, the last step with `->>` when
/// `as_text`.
pub fn path_text(column: &str, steps: &[Step], as_text: bool) -> String {
    let mut out = column.to_string();
    for (i, step) in steps.iter().enumerate() {
        out.push_str(if as_text && i + 1 == steps.len() { "->>" } else { "->" });
        match step {
            Step::Key(k) => {
                out.push('\'');
                out.push_str(&k.replace('\'', "''"));
                out.push('\'');
            }
            Step::Index(n) => out.push_str(&n.to_string()),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// inverted-index items
// ---------------------------------------------------------------------------

/// What an inverted index over documents keeps per document, PostgreSQL's two
/// GIN operator classes for `jsonb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GinOpclass {
    /// `jsonb_ops`: every object key and every string array element as a
    /// *key* item, and every scalar as a *value* item, with no path. Answers
    /// `@>`, `?`, `?|` and `?&`.
    Ops,
    /// `jsonb_path_ops`: one item per scalar, hashing the object keys on its
    /// path with it. Fewer, more selective items; answers `@>` only.
    PathOps,
}

const ITEM_KEY: u8 = 1;
const ITEM_VALUE: u8 = 2;
const ITEM_PATH: u8 = 3;

fn item_hasher() -> impl std::hash::Hasher {
    use std::hash::BuildHasher;
    // Fixed seeds: an item must hash the same in every process, since a query
    // built here is matched against items built when the rows were indexed.
    ahash::RandomState::with_seeds(0x6a73_6f6e, 0x6769_6e00, 0x7061_7468, 0x6f70_7300).build_hasher()
}

/// Feeds a scalar into `h` in a form two scalars [`scalar_eq`] calls equal
/// always share, so equal values always produce equal items.
///
/// Numbers are hashed by value, not spelling: `1`, `1.0` and `1e0` agree, and
/// every zero is the same zero, exactly as `numbers_equal` decides. A number
/// whose text does not parse is equal only to the same text, so its text is
/// hashed as it stands, under a different class.
fn hash_scalar(bytes: &[u8], h: &mut impl std::hash::Hasher) -> Option<()> {
    match *bytes.first()? {
        TAG_NULL => h.write_u8(0),
        TAG_FALSE => h.write_u8(1),
        TAG_TRUE => h.write_u8(2),
        TAG_INT | TAG_NUM => {
            let mut buf = [0u8; 20];
            let text = number_text(bytes, &mut buf)?;
            match NumberParts::parse(text) {
                Some(parts) if parts.len == 0 => h.write_u8(3),
                Some(parts) => {
                    h.write_u8(4);
                    h.write_u8(parts.negative as u8);
                    h.write_i64(parts.point);
                    for d in parts.digits() {
                        h.write_u8(d);
                    }
                    h.write_usize(parts.len);
                }
                None => {
                    h.write_u8(5);
                    h.write(text);
                    h.write_usize(text.len());
                }
            }
        }
        TAG_STR => {
            let (raw, _) = read_len_prefixed(bytes).ok()?;
            h.write_u8(6);
            h.write(raw);
            h.write_usize(raw.len());
        }
        _ => return None,
    }
    Some(())
}

/// The key item for `key`: an object key, a string array element, or the
/// operand of `?`.
pub fn gin_key_item(key: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut h = item_hasher();
    h.write_u8(ITEM_KEY);
    h.write(key);
    h.write_usize(key.len());
    h.finish()
}

/// Every item `doc` produces under `opclass`, appended to `out` (unsorted,
/// possibly repeated). `None` when the document is malformed; an empty input
/// (a SQL `NULL`) has no items.
///
/// The property the index rests on: when `contains(doc, query)` is true, every
/// item of `query` is an item of `doc`. Each rule of [`contains`] keeps it:
/// - a key of the query is a key at the matching place in the document;
/// - a scalar of the query equals a scalar at the matching place, and equal
///   scalars hash alike ([`hash_scalar`]);
/// - array nesting never adds a path step, so an element found anywhere in a
///   matching array, and a top-level scalar found among a top-level array's
///   elements, sit on the same path as in the query;
/// - a string array element is a key item too, so the query's string elements
///   and `?` operands match it under `jsonb_ops`.
pub fn gin_items(doc: &[u8], opclass: GinOpclass, out: &mut Vec<u64>) -> Option<()> {
    if doc.is_empty() {
        return Some(());
    }
    let mut path: Vec<&[u8]> = Vec::new();
    gin_walk(doc, opclass, &mut path, true, out)
}

fn gin_walk<'a>(
    bytes: &'a [u8],
    opclass: GinOpclass,
    path: &mut Vec<&'a [u8]>,
    top: bool,
    out: &mut Vec<u64>,
) -> Option<()> {
    use std::hash::Hasher;
    match *bytes.first()? {
        TAG_OBJ => {
            let o = Composite::of(bytes, OBJ_ENTRY)?;
            for at in 0..o.count {
                let (key, value) = o.entry(bytes, at)?;
                if opclass == GinOpclass::Ops {
                    out.push(gin_key_item(key));
                }
                path.push(key);
                let walked = gin_walk(value, opclass, path, false, out);
                path.pop();
                walked?;
            }
        }
        TAG_ARR => {
            let a = Composite::of(bytes, ARR_ENTRY)?;
            for at in 0..a.count {
                let element = a.element(bytes, at)?;
                if opclass == GinOpclass::Ops && element.first() == Some(&TAG_STR) {
                    out.push(gin_key_item(read_len_prefixed(element).ok()?.0));
                }
                gin_walk(element, opclass, path, false, out)?;
            }
        }
        tag if tag < TAG_ARR => {
            let mut h = item_hasher();
            match opclass {
                GinOpclass::Ops => {
                    h.write_u8(ITEM_VALUE);
                    // A document that is one string answers `?` for it.
                    if top && tag == TAG_STR {
                        out.push(gin_key_item(read_len_prefixed(bytes).ok()?.0));
                    }
                }
                GinOpclass::PathOps => {
                    h.write_u8(ITEM_PATH);
                    h.write_usize(path.len());
                    for step in path.iter() {
                        h.write(step);
                        h.write_usize(step.len());
                    }
                }
            }
            hash_scalar(bytes, &mut h)?;
            out.push(h.finish());
        }
        _ => return None,
    }
    Some(())
}

/// Whether two JSON numbers are equal as numbers — `1.0` and `1` are. Used
/// by [`crate::core::pg_ops::json_contains`], the decoded-tree reference that
/// [`contains`] is checked against.
pub fn json_numbers_equal(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    numbers_equal(a.to_string().as_bytes(), b.to_string().as_bytes())
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

/// Appends the text PostgreSQL's `jsonb` output gives the document `bytes`
/// encodes to `out`.
///
/// That is `jsonb_out`'s format, which clients see and compare against: a
/// space after every `:` and `,` (`{"a": 1, "b": [1, 2]}`), object keys
/// shorter first and then bytewise (`{"y": 1, "it's": 2}`), and numbers as
/// `numeric` prints them ([`write_numeric_text`]). The order is only for
/// printing: keys are stored bytewise because the lookups binary-search them.
/// The same text answers `->>` of a container, `::text` and `SELECT doc`,
/// so a predicate on it sees what the client is shown.
///
/// An object whose keys are not strictly ascending, which this module never
/// writes, is put through `decode` and `encode` first, which re-sort and
/// deduplicate it. Anything malformed goes that way too, so that the error
/// is `decode`'s.
pub fn render_text_into(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let start = out.len();
    let mut order = Vec::new();
    if render_value(bytes, out, &mut order).is_ok() {
        return Ok(());
    }
    out.truncate(start);
    let canonical = encode(&decode(bytes)?);
    order.clear();
    render_value(&canonical, out, &mut order)
        .map_err(|()| "a document could not be rendered as text".to_string())
}

/// `Err(())` means "render this through `decode` instead", not that the
/// document is bad. `order` is scratch for the objects' printing order, a
/// stack each object pushes its entries onto and truncates back off.
fn render_value(bytes: &[u8], out: &mut Vec<u8>, order: &mut Vec<u32>) -> Result<(), ()> {
    match *bytes.first().ok_or(())? {
        TAG_NULL => out.extend_from_slice(b"null"),
        TAG_FALSE => out.extend_from_slice(b"false"),
        TAG_TRUE => out.extend_from_slice(b"true"),
        TAG_INT => {
            let raw = bytes.get(1..9).ok_or(())?;
            let value = i64::from_le_bytes(raw.try_into().map_err(|_| ())?);
            crate::core::value_codec::write_i64(out, value);
        }
        TAG_NUM => {
            // Already `numeric`'s text for anything this build wrote; an
            // older build stored a float's `Display` (`1e+20`, `-0.0`), which
            // this rewrites to what PostgreSQL prints for the same value.
            let (text, _) = read_len_prefixed(bytes).map_err(|_| ())?;
            let start = out.len();
            if write_numeric_text(text, out).is_err() {
                out.truncate(start);
                return Err(());
            }
        }
        TAG_STR => {
            let (raw, _) = read_len_prefixed(bytes).map_err(|_| ())?;
            write_json_string(out, std::str::from_utf8(raw).map_err(|_| ())?);
        }
        TAG_ARR => {
            let (count, table_at, body_at) = composite_header(bytes, ARR_ENTRY).map_err(|_| ())?;
            out.push(b'[');
            for index in 0..count {
                if index > 0 {
                    out.extend_from_slice(b", ");
                }
                let entry = table_at + index * ARR_ENTRY;
                let slice = slice_at(bytes, body_at, entry, entry + 4).map_err(|_| ())?;
                render_value(slice, out, order)?;
            }
            out.push(b']');
        }
        TAG_OBJ => {
            let (count, table_at, body_at) = composite_header(bytes, OBJ_ENTRY).map_err(|_| ())?;
            let key_at = |index: usize| {
                let entry = table_at + index * OBJ_ENTRY;
                slice_at(bytes, body_at, entry, entry + 4).map_err(|_| ())
            };
            let base = order.len();
            let mut previous: Option<&[u8]> = None;
            for index in 0..count {
                let key = key_at(index)?;
                // A map would reorder or merge these; let `decode` do it.
                if previous.is_some_and(|p| p >= key) {
                    order.truncate(base);
                    return Err(());
                }
                previous = Some(key);
                order.push(index as u32);
            }
            // PostgreSQL's order is by length, then bytewise. Stored order is
            // bytewise, so among keys of one length it already is theirs, and
            // sorting by (length, stored position) gives the whole of it.
            let key_len = |index: u32| {
                let entry = table_at + index as usize * OBJ_ENTRY + 4;
                read_u32(bytes, entry).unwrap_or(0)
            };
            order[base..].sort_unstable_by_key(|&index| (key_len(index), index));
            out.push(b'{');
            for at in 0..count {
                let index = order[base + at] as usize;
                if at > 0 {
                    out.extend_from_slice(b", ");
                }
                let key = key_at(index)?;
                write_json_string(out, std::str::from_utf8(key).map_err(|_| ())?);
                out.extend_from_slice(b": ");
                let entry = table_at + index * OBJ_ENTRY;
                let value = slice_at(bytes, body_at, entry + 8, entry + 12).map_err(|_| ())?;
                render_value(value, out, order)?;
            }
            order.truncate(base);
            out.push(b'}');
        }
        _ => return Err(()),
    }
    Ok(())
}

/// Whether a byte must be escaped inside a JSON string, as serde_json decides
/// it: the control characters, the quote and the backslash. Nothing else —
/// not `/`, not DEL, not anything non-ASCII.
#[inline]
fn needs_escape(b: u8) -> bool {
    b < 0x20 || b == b'"' || b == b'\\'
}

/// Where the next byte needing an escape is, eight bytes at a time.
#[inline]
fn next_escape(bytes: &[u8], mut i: usize) -> usize {
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    while i + 8 <= bytes.len() {
        let w = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        // A byte below 0x20, or equal to '"' or '\\'. The classic zero-byte
        // test, applied to `w - 0x20` (bytes below 0x20 borrow) and to `w`
        // xor each special byte (equal bytes become zero). It can report a
        // false hit next to a real one, never miss one, so a hit is
        // re-checked a byte at a time below.
        let quote = w ^ (LO * b'"' as u64);
        let slash = w ^ (LO * b'\\' as u64);
        let hit = ((w.wrapping_sub(LO * 0x20) & !w)
            | (quote.wrapping_sub(LO) & !quote)
            | (slash.wrapping_sub(LO) & !slash))
            & HI;
        if hit != 0 {
            break;
        }
        i += 8;
    }
    while i < bytes.len() && !needs_escape(bytes[i]) {
        i += 1;
    }
    i
}

/// Appends `text` as a JSON string, escaped exactly as serde_json escapes it.
fn write_json_string(out: &mut Vec<u8>, text: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = text.as_bytes();
    out.reserve(bytes.len() + 2);
    out.push(b'"');
    let mut run = 0;
    loop {
        let at = next_escape(bytes, run);
        out.extend_from_slice(&bytes[run..at]);
        if at == bytes.len() {
            break;
        }
        let b = bytes[at];
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0C => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(b >> 4) as usize],
                HEX[(b & 0xF) as usize],
            ]),
        }
        run = at + 1;
    }
    out.push(b'"');
}

/// Whether these bytes could be a document this module wrote.
///
/// A cheap shape check, not a validation: it is here so a column holding
/// something else is refused rather than decoded into nonsense.
pub fn looks_encoded(bytes: &[u8]) -> bool {
    matches!(
        bytes.first(),
        Some(
            &TAG_NULL
                | &TAG_FALSE
                | &TAG_TRUE
                | &TAG_INT
                | &TAG_NUM
                | &TAG_STR
                | &TAG_ARR
                | &TAG_OBJ
        )
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
        assert_eq!(
            as_text(get_key(&encoded, "date").unwrap()).as_deref(),
            Some("lower")
        );
        assert_eq!(
            as_text(get_key(&encoded, "Date").unwrap()).as_deref(),
            Some("upper")
        );
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
        assert_eq!(
            as_text(get_key(&encoded, "QA Comments").unwrap()).as_deref(),
            Some("1")
        );
        assert_eq!(
            as_text(get_key(&encoded, "").unwrap()).as_deref(),
            Some("4")
        );
        assert_eq!(
            as_text(get_key(&encoded, "ключ").unwrap()).as_deref(),
            Some("5")
        );
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
        assert_eq!(
            as_text(get_key(&encoded, "a").unwrap()).as_deref(),
            Some("2")
        );
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
        assert_eq!(
            as_text(get_index(&encoded, -1).unwrap()).as_deref(),
            Some("3")
        );
        assert_eq!(
            as_text(get_index(&encoded, -3).unwrap()).as_deref(),
            Some("1")
        );
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
        assert_eq!(
            as_text(get_key(&encoded, "s").unwrap()).as_deref(),
            Some("plain")
        );
        assert_eq!(
            as_text(get_key(&encoded, "n").unwrap()).as_deref(),
            Some("42")
        );
        assert_eq!(
            as_text(get_key(&encoded, "f").unwrap()).as_deref(),
            Some("1.25")
        );
        assert_eq!(
            as_text(get_key(&encoded, "b").unwrap()).as_deref(),
            Some("true")
        );
        assert_eq!(as_text(get_key(&encoded, "z").unwrap()), None);
        // PostgreSQL's `jsonb` text: a space after `,` and `:`.
        assert_eq!(
            as_text(get_key(&encoded, "a").unwrap()).as_deref(),
            Some("[1, 2]")
        );
        assert_eq!(
            as_text(get_key(&encoded, "o").unwrap()).as_deref(),
            Some("{\"k\": 1}")
        );
    }

    /// A large integer and a high-precision decimal both have to come back as
    /// they went in — this is why a non-integer keeps its digits. Each answer
    /// is what PostgreSQL 17 printed for `'<text>'::jsonb::text`.
    #[test]
    fn precision_is_not_quietly_lost() {
        for (text, pg) in [
            ("123456789012345678901234567890", "123456789012345678901234567890"),
            ("0.12345678901234567890123", "0.12345678901234567890123"),
            ("1.000000000000000000001", "1.000000000000000000001"),
            ("-0.0", "0.0"),
            ("[1.000000000000000000001]", "[1.000000000000000000001]"),
        ] {
            let mut out = Vec::new();
            render_text_into(&encode_text(text).unwrap(), &mut out).unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), pg, "{} did not survive", text);
        }
    }

    fn pg_text(text: &str) -> Result<String, String> {
        let mut out = Vec::new();
        render_text_into(&encode_text(text)?, &mut out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// Every spelling here was put through PostgreSQL 17 as `'<x>'::jsonb::text`,
    /// `'{"a":<x>}'::jsonb->>'a'` and `'[<x>]'::jsonb::text`; the three agreed,
    /// and the right-hand column is what they printed. `numeric` keeps the
    /// scale as written, writes an exponent out, and has no negative zero.
    #[test]
    fn numbers_print_as_postgres_numeric() {
        let table = PG_NUMBERS;
        let long = [
            ("1e308", format!("1{}", "0".repeat(308))),
            ("1e309", format!("1{}", "0".repeat(309))),
            ("1e400", format!("1{}", "0".repeat(400))),
            ("-1e400", format!("-1{}", "0".repeat(400))),
            ("1e-320", format!("0.{}1", "0".repeat(319))),
            ("5e-324", format!("0.{}5", "0".repeat(323))),
            ("1.5E-400", format!("0.{}15", "0".repeat(399))),
            ("1e-400", format!("0.{}1", "0".repeat(399))),
        ];
        let all = table
            .iter()
            .map(|(t, p)| (*t, p.to_string()))
            .chain(long.iter().map(|(t, p)| (*t, p.clone())));
        for (text, pg) in all {
            assert_eq!(pg_text(text).as_deref(), Ok(pg.as_str()), "{text}");
            assert_eq!(pg_text(&format!("[{text}]")), Ok(format!("[{pg}]")), "[{text}]");
            let doc = encode_text(&format!("{{\"a\":{text}}}")).unwrap();
            assert_eq!(as_text(get_key(&doc, "a").unwrap()), Some(pg.clone()), "->> of {text}");
        }
    }

    /// The digits of every number in a document are its own, wherever it
    /// sits: `encode_text` reads them from the text in step with serde_json,
    /// so strings holding quotes, backslashes and digits, keys, nesting and
    /// re-sorted objects must not put it out of step. The expected text is
    /// built from PostgreSQL's answers in [`PG_NUMBERS`], and in its format.
    #[test]
    fn numbers_keep_their_digits_anywhere_in_a_document() {
        const STRINGS: &[(&str, &str)] = &[
            (r#""1e5""#, r#""1e5""#),
            (r#""a\"1.5\"b""#, r#""a\"1.5\"b""#),
            (r#""\\""#, r#""\\""#),
            (r#""\\\"-0""#, r#""\\\"-0""#),
            (r#""1""#, r#""1""#),
            (r#""x,2]""#, r#""x,2]""#),
            ("true", "true"),
            ("null", "null"),
        ];
        // Distinct keys, listed in PostgreSQL's order: shorter first, then
        // bytewise. Some hold digits and quotes to tempt a misread.
        const KEYS: &[&str] = &["", "1", "a", "-1", "e5", "é", r#"a\"9"#, "zz9.5"];
        fn generate(rng: &mut fastrand::Rng, depth: u32, text: &mut String, pg: &mut String) {
            let ws = |rng: &mut fastrand::Rng, text: &mut String| {
                if rng.u8(0..4) == 0 {
                    text.push_str([" ", "\n", "\t", "\r\n "][rng.usize(0..4)]);
                }
            };
            ws(rng, text);
            match if depth == 0 { rng.u8(0..2) } else { rng.u8(0..4) } {
                0 => {
                    let (t, p) = PG_NUMBERS[rng.usize(0..PG_NUMBERS.len())];
                    text.push_str(t);
                    pg.push_str(p);
                }
                1 => {
                    let (t, p) = STRINGS[rng.usize(0..STRINGS.len())];
                    text.push_str(t);
                    pg.push_str(p);
                }
                2 => {
                    text.push('[');
                    pg.push('[');
                    for i in 0..rng.usize(0..4) {
                        if i > 0 {
                            text.push(',');
                            pg.push_str(", ");
                        }
                        generate(rng, depth - 1, text, pg);
                    }
                    ws(rng, text);
                    text.push(']');
                    pg.push(']');
                }
                _ => {
                    let mut picked: Vec<usize> = (0..KEYS.len()).filter(|_| rng.bool()).collect();
                    let mut values = Vec::new();
                    for _ in &picked {
                        let (mut t, mut p) = (String::new(), String::new());
                        generate(rng, depth - 1, &mut t, &mut p);
                        values.push((t, p));
                    }
                    // Written in the expected order, sent shuffled.
                    pg.push('{');
                    for (i, (k, (_, p))) in picked.iter().zip(&values).enumerate() {
                        if i > 0 {
                            pg.push_str(", ");
                        }
                        pg.push_str(&format!("\"{}\": {}", KEYS[*k], p));
                    }
                    pg.push('}');
                    let mut order: Vec<usize> = (0..picked.len()).collect();
                    rng.shuffle(&mut order);
                    text.push('{');
                    for (i, at) in order.iter().enumerate() {
                        if i > 0 {
                            text.push(',');
                        }
                        ws(rng, text);
                        text.push_str(&format!("\"{}\":", KEYS[picked[*at]]));
                        text.push_str(&values[*at].0);
                    }
                    ws(rng, text);
                    text.push('}');
                    picked.clear();
                }
            }
            ws(rng, text);
        }
        let mut rng = fastrand::Rng::with_seed(0x6e75_6d73);
        for _ in 0..20_000 {
            let (mut text, mut pg) = (String::new(), String::new());
            generate(&mut rng, 4, &mut text, &mut pg);
            assert_eq!(pg_text(&text).as_deref(), Ok(pg.as_str()), "{text:?}");
        }
    }

    /// PostgreSQL 17's text for each spelling; see
    /// `numbers_print_as_postgres_numeric`.
    const PG_NUMBERS: &[(&str, &str)] = &[
        ("0", "0"),
        ("-0", "0"),
        ("0.0", "0.0"),
        ("-0.0", "0.0"),
        ("0.00", "0.00"),
        ("0e10", "0"),
        ("0E+5", "0"),
        ("-0e-3", "0.000"),
        ("1", "1"),
        ("-1", "-1"),
        ("1.0", "1.0"),
        ("1.10", "1.10"),
        ("1.50", "1.50"),
        ("100.00", "100.00"),
        ("1e0", "1"),
        ("1E-0", "1"),
        ("1e2", "100"),
        ("1E+2", "100"),
        ("1e+2", "100"),
        ("1.5e1", "15"),
        ("1.50e1", "15.0"),
        ("15e-1", "1.5"),
        ("0.15e1", "1.5"),
        ("1e-2", "0.01"),
        ("1.0e-2", "0.010"),
        ("4.2E1", "42"),
        ("10e-1", "1.0"),
        ("12.345e1", "123.45"),
        ("-1.5E-1", "-0.15"),
        ("0.1", "0.1"),
        ("0.10", "0.10"),
        ("1e-1", "0.1"),
        ("5e-1", "0.5"),
        ("2.50", "2.50"),
        ("25E-1", "2.5"),
        ("1e20", "100000000000000000000"),
        ("1e+20", "100000000000000000000"),
        ("100000000000000000000", "100000000000000000000"),
        ("99999999999999999999.5", "99999999999999999999.5"),
        ("1.000000000000000000001", "1.000000000000000000001"),
        ("3.14159265358979323846264338327950288", "3.14159265358979323846264338327950288"),
        ("123456789012345678901234567890", "123456789012345678901234567890"),
        ("-123456789012345678901234567890", "-123456789012345678901234567890"),
        ("9007199254740993", "9007199254740993"),
        ("9007199254740993.0", "9007199254740993.0"),
        ("9.007199254740993e15", "9007199254740993"),
        ("9223372036854775807", "9223372036854775807"),
        ("9223372036854775808", "9223372036854775808"),
        ("-9223372036854775808", "-9223372036854775808"),
        ("-9223372036854775809", "-9223372036854775809"),
        ("18446744073709551615", "18446744073709551615"),
        ("18446744073709551616", "18446744073709551616"),
        ("0.30000000000000004", "0.30000000000000004"),
        ("1.23456789012345678901", "1.23456789012345678901"),
        ("0.000001", "0.000001"),
        ("1e-7", "0.0000001"),
        ("123e-10", "0.0000000123"),
        ("0.0000000000000000000000000000000000000000000000000000000000000001e64", "1"),
        ("7e0", "7"),
        ("-7.000", "-7.000"),
    ];

    /// `numeric`'s limits, as PostgreSQL 17 enforces them on `jsonb` input:
    /// 131072 digits before the point, a scale of 16383, and an exponent of
    /// at most 1073741823 even on a zero. Each refused spelling was refused
    /// there with "value overflows numeric format", and each accepted one
    /// accepted (`'1e131071'::jsonb::text` is 131072 characters long).
    #[test]
    fn numbers_beyond_numeric_are_refused_as_postgres_refuses_them() {
        for text in ["1e131072", "10e131071", "1e-16384", "1.5e-16383", "0e-20000", "0e1073741824", "1e2000000000", "{\"a\":1e2000000000}", "1e-1000000", "1e99999999999999999999", "1e-99999999999999999999"] {
            let err = encode_text(text).unwrap_err();
            assert!(err.starts_with(NUMERIC_OVERFLOW), "{text}: {err}");
        }
        assert_eq!(pg_text("1e131071").unwrap().len(), 131_072);
        assert_eq!(pg_text("0.1e131072").unwrap().len(), 131_072);
        assert_eq!(pg_text("1e-16383").unwrap().len(), 16_385);
        assert_eq!(pg_text("0e1000000").unwrap(), "0");
        assert_eq!(pg_text("0e1073741823").unwrap(), "0");
    }

    /// A number is stored about as long as it was written, not as long as it
    /// prints. `1e131071` prints 131072 characters, and PostgreSQL 17 stores
    /// it in 22 bytes (`pg_column_size('1e131071'::jsonb)`), and
    /// `'0e-16383'::jsonb` in 20. Expanding at encode time made a literal of
    /// 40 such numbers, 360 bytes of text, a 5 MB document, and 40,000 of
    /// them a 5 GB one.
    #[test]
    fn a_number_is_stored_compactly_and_prints_in_full() {
        for (text, printed) in [
            ("1e131071", 131_072),
            ("-1e131071", 131_073),
            ("0e-16383", 16_385),
            ("1e-16383", 16_385),
            ("-9.5e-16382", 16_386),
            ("1.000e-16380", 16_385),
            ("12.50e40", 42),
        ] {
            let stored = encode_text(text).unwrap();
            assert!(stored.len() <= 5 + text.len() + 2, "{text} stored in {} bytes", stored.len());
            assert_eq!(pg_text(text).unwrap().len(), printed, "{text}");
        }
        let many = format!("[{}1e131071]", "1e131071,".repeat(999));
        let stored = encode_text(&many).unwrap();
        assert!(stored.len() < 32 * 1000, "1000 numbers stored in {} bytes", stored.len());
        // PostgreSQL 17: `length(('[' || repeat('1e131071,', 39) ||
        // '1e131071]')::jsonb::text)` is 5242960.
        let forty = format!("[{}1e131071]", "1e131071,".repeat(39));
        assert_eq!(pg_text(&forty).unwrap().len(), 5_242_960);
    }

    /// Random numbers, many of them past the length where the exponent form
    /// is stored: each must print exactly as its own lexeme does, be stored
    /// no longer than it was written (or than short positional text), and be
    /// stored as the same bytes as another spelling of the same value and
    /// scale, `0.<all the digits>` with the exponent moved to match.
    #[test]
    fn the_stored_form_keeps_value_and_scale() {
        let mut rng = fastrand::Rng::with_seed(0x636f_6d70);
        for _ in 0..50_000 {
            let negative = rng.bool();
            let int: String = match rng.u8(0..3) {
                0 => "0".into(),
                _ => {
                    let mut s = rng.u8(1..10).to_string();
                    for _ in 0..rng.usize(0..6) {
                        s.push(['0', '0', '5', '9'][rng.usize(0..4)]);
                    }
                    s
                }
            };
            let frac: String = (0..rng.usize(0..6)).map(|_| ['0', '0', '3', '7'][rng.usize(0..4)]).collect();
            let exponent: i64 = match rng.u8(0..4) {
                0 => 0,
                1 => rng.i64(-10..10),
                _ => rng.i64(-300..300),
            };
            let sign = if negative { "-" } else { "" };
            let dot = if frac.is_empty() { "" } else { "." };
            let lexeme = format!("{sign}{int}{dot}{frac}e{exponent}");
            let respelled = format!("{sign}0.{int}{frac}e{}", exponent + int.len() as i64);

            let mut want = Vec::new();
            write_numeric_text(lexeme.as_bytes(), &mut want).unwrap();
            let stored = encode_text(&lexeme).unwrap();
            assert_eq!(pg_text(&lexeme).unwrap().as_bytes(), &want[..], "{lexeme}");
            assert!(
                stored.len() <= 5 + lexeme.len().max(POSITIONAL_UP_TO as usize),
                "{lexeme} stored in {} bytes",
                stored.len()
            );
            assert_eq!(encode_text(&respelled).unwrap(), stored, "{lexeme} and {respelled}");
            assert_eq!(scalar_eq(&stored, &encode_text(&respelled).unwrap()), Some(true));
        }
    }

    /// A document whose encoding would pass the size limit is refused, not
    /// written with offsets truncated to `u32`. The real limit is 2 GiB;
    /// the same checks are run here against a small one, at each place the
    /// encoding grows: an array, an object in order, and an object re-laid
    /// in scratch because its keys arrived out of order.
    #[test]
    fn a_document_past_the_size_limit_is_refused() {
        for text in [
            "[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]",
            r#"{"a":0,"b":0,"c":0,"d":0,"e":0,"f":0,"g":0,"h":0}"#,
            r#"{"h":0,"g":0,"f":0,"e":0,"d":0,"c":0,"b":0,"a":0}"#,
            r#"[[[["a long string that alone is past the limit of this test"]]]]"#,
        ] {
            let size = encode_text(text).unwrap().len();
            // Out of order, the object needs its scratch copy as well.
            let needed = if text.starts_with(r#"{"h""#) { 2 * size } else { size };
            assert!(encode_text_from(text, text, needed).is_ok(), "{text} within {needed}");
            let err = encode_text_from(text, text, size - 1).unwrap_err();
            assert!(err.starts_with(DOCUMENT_TOO_LARGE), "{text}: {err}");
        }
    }

    /// A value and scale have one stored form, however they were spelled, so
    /// that spellings PostgreSQL prints alike stay alike as bytes. Each
    /// printed form is PostgreSQL 17's, from
    /// `SELECT '[1e50, 1e-50, 1.000e-45, 12.50e40, -3.10e-30, 0e-45]'::jsonb`.
    #[test]
    fn spellings_that_print_alike_are_stored_alike() {
        let zeros = |n: usize| "0".repeat(n);
        for (spellings, pg) in [
            (vec!["1e50".to_string(), format!("1{}", zeros(50)), "10e49".into(), "0.1e51".into()], format!("1{}", zeros(50))),
            (vec!["1e-50".to_string(), format!("0.{}1", zeros(49)), "0.1e-49".into(), "0.01e-48".into()], format!("0.{}1", zeros(49))),
            (vec!["1.000e-45".to_string(), format!("0.{}1000", zeros(44)), "1000e-48".into()], format!("0.{}1000", zeros(44))),
            (vec!["12.50e40".to_string(), "1.250e41".into(), format!("125{}", zeros(39))], format!("125{}", zeros(39))),
            (vec!["-3.10e-30".to_string(), "-310e-32".into(), format!("-0.{}310", zeros(29))], format!("-0.{}310", zeros(29))),
            (vec!["0e-45".to_string(), "-0e-45".into(), "0.000e-42".into(), format!("0.{}", zeros(45))], format!("0.{}", zeros(45))),
            (vec!["1.5e1".to_string(), "15".into(), "0.15e2".into()], "15".to_string()),
            (vec!["1.50e1".to_string(), "15.0".into(), "150e-1".into()], "15.0".to_string()),
            (vec!["1e20".to_string(), format!("1{}", zeros(20))], format!("1{}", zeros(20))),
        ] {
            let first = encode_text(&spellings[0]).unwrap();
            for s in &spellings {
                assert_eq!(pg_text(s).unwrap(), pg, "{s}");
                assert_eq!(encode_text(s).unwrap(), first, "{s} and {} are stored apart", spellings[0]);
            }
        }
    }

    /// Documents an earlier build wrote kept a float's `Display` as the
    /// number's text. They print as PostgreSQL prints the same value, and
    /// compare and hash as the value they hold.
    #[test]
    fn numbers_stored_by_older_builds_read_correctly() {
        fn legacy(text: &str) -> Vec<u8> {
            let mut out = vec![TAG_NUM];
            write_len_prefixed(&mut out, text.as_bytes());
            out
        }
        for (old, pg, same_as) in [
            ("1e+20", "100000000000000000000", "1e20"),
            ("1e20", "100000000000000000000", "100000000000000000000"),
            ("-0.0", "0.0", "0"),
            ("1.0", "1.0", "1"),
            ("1.2345678901234568e+29", "123456789012345680000000000000", "123456789012345680000000000000"),
            ("1e-7", "0.0000001", "0.0000001"),
            ("9007199254740994.0", "9007199254740994.0", "9007199254740994"),
        ] {
            let stored = legacy(old);
            let mut out = Vec::new();
            render_text_into(&stored, &mut out).unwrap();
            assert_eq!(std::str::from_utf8(&out).unwrap(), pg, "{old}");
            let fresh = encode_text(same_as).unwrap();
            assert_eq!(scalar_eq(&stored, &fresh), Some(true), "{old} = {same_as}");
            assert_eq!(contains(&stored, &fresh), Some(true), "{old} @> {same_as}");
            let mut a = Vec::new();
            let mut b = Vec::new();
            gin_items(&stored, GinOpclass::PathOps, &mut a).unwrap();
            gin_items(&fresh, GinOpclass::PathOps, &mut b).unwrap();
            assert_eq!(a, b, "{old} and {same_as} hash apart");
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
            &[
                Step::Key("key_01234".to_string()),
                Step::Key("n".to_string()),
            ],
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
    Some(PathExpr {
        column,
        steps,
        as_text,
    })
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
    Some(
        inner
            .split(',')
            .map(|p| p.trim().trim_matches('"').to_string())
            .collect(),
    )
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
            assert_eq!(
                parsed.index_key(),
                canonical,
                "{} keyed differently",
                variant
            );
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
            "doc",             // no step
            "",                // nothing
            "doc->",           // no key
            "doc->>",          // no key
            "'literal'",       // not an identifier
            "doc->'a' extra",  // trailing text
            "doc->>'a'->>'b'", // text cannot be indexed further
            "doc->>'a'->'b'",  // same
            "count(*)",        // an ordinary expression
            "a + b",
            "doc#>'{}'", // an empty path array
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
        assert_eq!(
            as_text(p.resolve(&document).unwrap()).as_deref(),
            Some("live")
        );

        let p = parse_path_expression("doc->'a'->'b'->0->>'c'").unwrap();
        assert_eq!(
            as_text(p.resolve(&document).unwrap()).as_deref(),
            Some("found")
        );

        let p = parse_path_expression("doc#>>'{a,b,0,c}'").unwrap();
        assert_eq!(
            as_text(p.resolve(&document).unwrap()).as_deref(),
            Some("found")
        );

        let p = parse_path_expression("doc->>'missing'").unwrap();
        assert!(p.resolve(&document).is_none());
    }
}

/// `render_text_into` against what it replaced, `decode(..)?.to_string()`,
/// over generated documents and hand-made blobs, byte for byte.
#[cfg(test)]
mod render_tests {
    use super::*;
    use serde_json::json;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn random_string(rng: &mut Rng) -> String {
        const PIECES: &[&str] = &[
            "a", "Z", " ", "\"", "\\", "/", "\n", "\r", "\t", "\u{8}", "\u{c}", "\u{0}", "\u{1f}",
            "\u{7f}", "é", "日本", "\u{1F600}", "\u{2028}", "plain ascii run of text ",
            "0123456789abcdef",
        ];
        let mut s = String::new();
        for _ in 0..rng.below(40) {
            s.push_str(PIECES[rng.below(PIECES.len() as u64) as usize]);
        }
        s
    }

    /// Numbers whose `Display` is already PostgreSQL's text for them, so the
    /// reference below can print them as they are. How every other spelling
    /// prints is pinned by `numbers_print_as_postgres_numeric`.
    fn random_number(rng: &mut Rng) -> Value {
        match rng.below(6) {
            0 => json!(rng.next() as i64),
            1 => json!(rng.next()),
            2 => json!((rng.next() % 2_000_000) as f64 / 4.0 - 250_000.25),
            3 => json!(i64::MIN),
            4 => json!(0.5),
            _ => json!(11203.75),
        }
    }

    /// PostgreSQL's `jsonb` text for a document, written independently of
    /// the renderer: `, ` and `: ` separators, keys shorter first and then
    /// bytewise, strings escaped as serde_json escapes them (which is what
    /// `escape_json` does too).
    fn reference(value: &Value) -> String {
        match value {
            Value::Array(items) => {
                let parts: Vec<String> = items.iter().map(reference).collect();
                format!("[{}]", parts.join(", "))
            }
            Value::Object(fields) => {
                let mut keys: Vec<&String> = fields.keys().collect();
                keys.sort_by(|a, b| a.len().cmp(&b.len()).then(a.as_bytes().cmp(b.as_bytes())));
                let parts: Vec<String> = keys
                    .iter()
                    .map(|k| format!("{}: {}", Value::String(k.to_string()), reference(&fields[*k])))
                    .collect();
                format!("{{{}}}", parts.join(", "))
            }
            other => other.to_string(),
        }
    }

    fn random_value(rng: &mut Rng, depth: u32) -> Value {
        let pick = if depth >= 4 { rng.below(4) } else { rng.below(7) };
        match pick {
            0 => Value::Null,
            1 => Value::Bool(rng.below(2) == 0),
            2 => random_number(rng),
            3 => Value::String(random_string(rng)),
            4 | 5 => Value::Array((0..rng.below(6)).map(|_| random_value(rng, depth + 1)).collect()),
            _ => {
                let mut map = Map::new();
                for _ in 0..rng.below(6) {
                    map.insert(random_string(rng), random_value(rng, depth + 1));
                }
                Value::Object(map)
            }
        }
    }

    fn old(bytes: &[u8]) -> Result<String, String> {
        decode(bytes).map(|v| reference(&v))
    }

    fn new(bytes: &[u8]) -> Result<String, String> {
        let mut out = b"prefix".to_vec();
        render_text_into(bytes, &mut out)?;
        Ok(String::from_utf8(out[6..].to_vec()).expect("rendered JSON is UTF-8"))
    }

    #[test]
    fn generated_documents_render_identically() {
        let mut rng = Rng(0x0DDB_A11C_AFE5_EED5);
        let fixed = [
            json!(null),
            json!({}),
            json!([]),
            json!({"": ""}),
            json!([[], {}, [[]], {"a": {}}]),
            json!("\u{0}\u{1}\u{2}\u{3}\u{4}\u{5}\u{6}\u{7}\u{8}\u{9}\u{a}\u{b}\u{c}\u{d}\u{e}\u{f}\u{10}\u{1f}\"\\"),
            json!({"b": 1, "a": 2, "aa": 3, "B": 4, "é": 5}),
            json!([1.5, 0.1, 9007199254740993u64, 18446744073709551615u64]),
        ];
        let mut docs: Vec<Value> = fixed.to_vec();
        for _ in 0..5000 {
            docs.push(random_value(&mut rng, 0));
        }
        for doc in docs {
            let bytes = encode(&doc);
            assert_eq!(new(&bytes), Ok(reference(&doc)), "{doc}");
            // The text path stores the same document.
            assert_eq!(encode_text(&doc.to_string()), Ok(bytes), "{doc}");
        }
    }

    /// PostgreSQL 17's own output for documents that exercise the format:
    /// `SELECT '<input>'::jsonb::text`.
    #[test]
    fn documents_print_as_postgres_prints_them() {
        for (input, pg) in [
            (
                r#"{"b":1,"aa":2,"a":3,"ab":{"y":[1,2,{}],"x":[]},"":null,"é":"x","z":"a\"b\nc"}"#,
                r#"{"": null, "a": 3, "b": 1, "z": "a\"b\nc", "aa": 2, "ab": {"x": [], "y": [1, 2, {}]}, "é": "x"}"#,
            ),
            (r#"{"a":{"y":1,"it's":2}}"#, r#"{"a": {"y": 1, "it's": 2}}"#),
            (r#"{"y":1.50,"bb":1e2}"#, r#"{"y": 1.50, "bb": 100}"#),
            (r#"[1, [2,3], {"k":1.50,"jj":[]}]"#, r#"[1, [2, 3], {"k": 1.50, "jj": []}]"#),
            ("{}", "{}"),
            ("[]", "[]"),
            ("[{}]", "[{}]"),
            (r#"{"a":{}}"#, r#"{"a": {}}"#),
            ("\"x\\u0001y\u{7f}/\"", "\"x\\u0001y\u{7f}/\""),
        ] {
            let mut out = Vec::new();
            render_text_into(&encode_text(input).unwrap(), &mut out).unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), pg, "{input}");
        }
    }

    /// Old-vs-new timing. Run with
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored jsonb_render_bench --nocapture`.
    #[test]
    #[ignore]
    fn jsonb_render_bench() {
        use std::hint::black_box;
        use std::time::Instant;
        let docs: Vec<(&str, Vec<u8>)> = vec![
            ("small object", encode(&json!({"id": 12345, "name": "Ada Lovelace", "active": true, "score": 98.5, "tags": ["a", "b"]}))),
            ("nested 2 KB", encode(&json!({
                "user": {"id": 1, "email": "someone@example.com", "roles": ["admin", "dev", "ops"], "address": {"street": "1 Long Road", "city": "Somewhere", "zip": "12345"}},
                "items": (0..20).map(|i| json!({"sku": format!("SKU-{i:05}"), "qty": i, "price": 9.99 + i as f64, "note": "line one\nline \"two\""})).collect::<Vec<_>>(),
                "meta": {"created": "2026-10-08T12:00:00Z", "version": 3}
            }))),
            ("long text value", encode(&json!({"body": "lorem ipsum dolor sit amet ".repeat(80)}))),
        ];
        for (name, bytes) in &docs {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            let reps = 2000;
            for _ in 0..15 {
                let t = Instant::now();
                for _ in 0..reps { black_box(decode(black_box(bytes)).unwrap().to_string()); }
                a.push(t.elapsed().as_nanos() as f64 / reps as f64);
                let t = Instant::now();
                let mut out = Vec::with_capacity(bytes.len());
                for _ in 0..reps { out.clear(); render_text_into(black_box(bytes), &mut out).unwrap(); black_box(&out); }
                b.push(t.elapsed().as_nanos() as f64 / reps as f64);
            }
            a.sort_by(f64::total_cmp);
            b.sort_by(f64::total_cmp);
            println!("jsonb {name:<16} old {:>9.1} ns  new {:>9.1} ns  ({:.2}x)", a[7], b[7], a[7] / b[7]);
        }
    }

    /// Every run length around the eight-byte scan, with the special byte at
    /// every position.
    #[test]
    fn escapes_are_found_at_every_offset() {
        for len in 0..40usize {
            for at in 0..len {
                for special in [b'"', b'\\', 0u8, 0x1f, b'\n', 0x7f, b'/'] {
                    let mut raw = vec![b'x'; len];
                    raw[at] = special;
                    let text = String::from_utf8(raw).unwrap();
                    let bytes = encode(&Value::String(text.clone()));
                    assert_eq!(new(&bytes), old(&bytes), "{text:?}");
                }
            }
        }
    }

    /// Blobs this module never writes still render, or fail with `decode`'s
    /// error: out-of-order and duplicate keys, a number stored as text in a
    /// non-canonical form, truncation, bad tags and bad UTF-8.
    #[test]
    fn unusual_blobs_render_identically() {
        fn obj(entries: &[(&[u8], Vec<u8>)]) -> Vec<u8> {
            let mut out = vec![TAG_OBJ];
            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            let table_at = out.len();
            out.resize(table_at + entries.len() * OBJ_ENTRY, 0);
            let body_at = out.len();
            for (i, (k, v)) in entries.iter().enumerate() {
                let key_off = out.len() - body_at;
                out.extend_from_slice(k);
                let val_off = out.len() - body_at;
                out.extend_from_slice(v);
                let e = table_at + i * OBJ_ENTRY;
                out[e..e + 4].copy_from_slice(&(key_off as u32).to_le_bytes());
                out[e + 4..e + 8].copy_from_slice(&(k.len() as u32).to_le_bytes());
                out[e + 8..e + 12].copy_from_slice(&(val_off as u32).to_le_bytes());
                out[e + 12..e + 16].copy_from_slice(&(v.len() as u32).to_le_bytes());
            }
            out
        }
        fn num(text: &str) -> Vec<u8> {
            let mut out = vec![TAG_NUM];
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
            out
        }
        let one = encode(&json!(1));
        let two = encode(&json!(2));
        let blobs: Vec<Vec<u8>> = vec![
            obj(&[(b"b", one.clone()), (b"a", two.clone())]),
            obj(&[(b"a", one.clone()), (b"a", two.clone())]),
            obj(&[(b"\xff", one.clone())]),
            num("1.0"),
            num("not a number"),
            num("\"a string\""),
            vec![],
            vec![0x09],
            vec![TAG_INT, 1, 2],
            vec![TAG_STR, 3, 0, 0, 0, b'a'],
            vec![TAG_STR, 1, 0, 0, 0, 0xff],
            vec![TAG_ARR, 2, 0, 0, 0],
            vec![TAG_ARR, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, TAG_NULL],
            // Trailing bytes after a complete value are ignored by both.
            [encode(&json!([1, "x"])), vec![0xAB, 0xCD]].concat(),
        ];
        for blob in blobs {
            assert_eq!(new(&blob), old(&blob), "{blob:?}");
        }
        // A number stored in a form this build would not write prints as
        // PostgreSQL prints its value; `decode` would go through a `f64`.
        for (blob, want) in [
            (obj(&[(b"a", one.clone()), (b"b", num("1.50"))]), r#"{"a": 1, "b": 1.50}"#),
            (num("1e2"), "100"),
            (num("-0"), "0"),
            (num("12345678901234567890123"), "12345678901234567890123"),
        ] {
            assert_eq!(new(&blob).as_deref(), Ok(want), "{blob:?}");
        }
    }
}

/// [`encode_text`] against the path it replaces — `serde_json::from_str` into
/// a `Value`, then [`encode`] — which is kept as the reference.
#[cfg(test)]
mod encode_text_tests {
    use super::*;

    fn reference(text: &str) -> Result<Vec<u8>, String> {
        serde_json::from_str::<Value>(text)
            .map(|v| encode(&v))
            .map_err(|e| e.to_string())
    }

    /// Whether two encodings hold the same document, numbers allowed to
    /// differ by the reference's rounding: the reference reads a number
    /// through a `f64` (not even a correctly rounded one), while
    /// `encode_text` keeps its digits, so only the `f64`s can be compared.
    /// The digits themselves are pinned against PostgreSQL by
    /// `numbers_print_as_postgres_numeric`. Everything else must be equal.
    fn same_document(ours: &[u8], reference: &[u8]) -> bool {
        let (Some(&a), Some(&b)) = (ours.first(), reference.first()) else {
            return false;
        };
        let number = |t: u8| t == TAG_INT || t == TAG_NUM;
        if number(a) && number(b) {
            let (mut buf_a, mut buf_b) = ([0u8; 20], [0u8; 20]);
            let x: f64 = std::str::from_utf8(number_text(ours, &mut buf_a).unwrap()).unwrap().parse().unwrap();
            let y: f64 = std::str::from_utf8(number_text(reference, &mut buf_b).unwrap()).unwrap().parse().unwrap();
            // A few units in the last place: serde_json's fast path is off
            // by more than one on some long fractions.
            return x == y || (x - y).abs() <= x.abs().max(y.abs()) * 1e-15;
        }
        if a != b {
            return false;
        }
        match a {
            TAG_ARR => {
                let (x, y) = (Composite::of(ours, ARR_ENTRY).unwrap(), Composite::of(reference, ARR_ENTRY).unwrap());
                x.count == y.count
                    && (0..x.count).all(|i| same_document(x.element(ours, i).unwrap(), y.element(reference, i).unwrap()))
            }
            TAG_OBJ => {
                let (x, y) = (Composite::of(ours, OBJ_ENTRY).unwrap(), Composite::of(reference, OBJ_ENTRY).unwrap());
                x.count == y.count
                    && (0..x.count).all(|i| {
                        let ((ka, va), (kb, vb)) = (x.entry(ours, i).unwrap(), y.entry(reference, i).unwrap());
                        ka == kb && same_document(va, vb)
                    })
            }
            _ => ours == reference,
        }
    }

    fn check(text: &str) {
        match (encode_text(text), reference(text)) {
            (Ok(ours), Ok(theirs)) => assert!(same_document(&ours, &theirs), "{text:?}"),
            (ours, theirs) => assert_eq!(ours, theirs, "{text:?}"),
        }
    }

    #[test]
    fn hand_picked_documents_encode_identically() {
        for text in [
            "null", "true", "false", "0", "-0", "-0.0", "1", "-1", "1.0", "1.50", "1e2", "1E+2",
            "-2.5e-8", "0.1", "9223372036854775807", "9223372036854775808",
            "-9223372036854775808", "-9223372036854775809", "18446744073709551615",
            "18446744073709551616", "123456789012345678901234567890", "1e308", "5e-324",
            "\"\"", "\"plain\"", r#""esc \" \\ \/ \b \f \n \r \t \u00e9 \ud83d\ude00""#,
            "\"\u{1F600} raw\"", "[]", "{}", "[[]]", "[{}]", "{\"a\":{}}", " [ 1 , 2 ] ",
            "[1,\"a\",null,true,[2,[3,[4]]],{\"k\":[]}]",
            r#"{"b":1,"a":2}"#, r#"{"a":1,"a":2}"#, r#"{"a":1,"b":2,"a":3}"#,
            r#"{"b":{"y":1,"x":2},"a":[{"d":1,"c":2}]}"#, r#"{"":1,"\u0000":2,"é":3,"e":4}"#,
            r#"{"a\"b":1,"a":2,"a\\":3}"#, r#"{"z":1,"y":2,"x":3,"y":4,"z":5}"#,
            "{\"$serde_json::private::RawValueX\":1}",
        ] {
            check(text);
        }
    }

    #[test]
    fn malformed_text_is_refused_with_the_same_error() {
        let deep_ok = format!("{}{}", "[".repeat(120), "]".repeat(120));
        let too_deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        for text in [
            "", " ", "{", "}", "[1,]", "[1 2]", "{\"a\"}", "{\"a\":}", "{a:1}", "{\"a\":1,}",
            "nul", "tru", "01", "1.", ".5", "1e", "+1", "--1", "\"unterminated", "\"\\x\"",
            "\"\\ud800\"", "[1] x", "1 2", "{\"a\":1}}", "\u{0}", "\"tab\tin string\"", "NaN",
            "Infinity",
            // After a number serde_json could not hold, so the masked second
            // parse has to report these exactly where the first would have.
            "[1e400,]", "[1e400 2]", "{\"a\":1e400,}", "[1e400,-]", "[1e400,01]", "[1e400,1.]",
            "[1e400,\"\\x\"]", "[1e400] x",
        ] {
            let (new, old) = (encode_text(text), reference(&text.replace("1e400", "1e300")));
            assert!(old.is_err(), "reference accepted {text:?}");
            assert_eq!(new, old, "{text:?}");
        }
        check(&deep_ok);
        assert_eq!(encode_text(&too_deep), reference(&too_deep));
        assert!(encode_text(&too_deep).is_err());
        // serde_json refuses these as out of range; `numeric` holds them, and
        // PostgreSQL stores them.
        for text in ["1e400", "-1e400", "[1e999]", "{\"a\":[1e309,\"x\"],\"b\":-1E+400}"] {
            assert!(reference(text).is_err(), "{text:?}");
            assert!(encode_text(text).is_ok(), "{text:?}");
        }
    }

    /// The one place the two differ, on purpose: serde_json's `raw_value`
    /// marker key, which the `Value` path reads as an instruction.
    #[test]
    fn the_raw_value_marker_key_is_just_a_key() {
        let text = r#"{"$serde_json::private::RawValue":"[1,2]"}"#;
        let stored = encode_text(text).unwrap();
        assert_eq!(
            decode(&stored).unwrap(),
            serde_json::json!({"$serde_json::private::RawValue": "[1,2]"})
        );
    }

    fn gen_text(rng: &mut fastrand::Rng, depth: u32, out: &mut String) {
        let ws = |rng: &mut fastrand::Rng, out: &mut String| {
            if rng.u8(0..6) == 0 {
                out.push_str([" ", "\n", "\t ", "\r\n"][rng.usize(0..4)]);
            }
        };
        ws(rng, out);
        let leaf = depth == 0 || rng.u8(0..10) < 4;
        if leaf {
            match rng.u8(0..14) {
                0 => out.push_str("null"),
                1 => out.push_str("true"),
                2 => out.push_str("false"),
                3 => out.push_str(&rng.i64(..).to_string()),
                4 => out.push_str(&rng.u64(..).to_string()),
                5 => out.push_str(&rng.i32(-1000..1000).to_string()),
                6 => {
                    let f = (rng.f64() - 0.5) * 10f64.powi(rng.i32(-30..30));
                    out.push_str(&format!("{}", f));
                }
                7 => out.push_str(
                    ["1.0", "1.50", "1e2", "-0", "-0.0", "2E-3", "123456789012345678901234", "0.1e+1", "100e-2"]
                        [rng.usize(0..9)],
                ),
                8 => out.push_str(&format!("{:e}", rng.f64() * 1e10)),
                _ => {
                    out.push('"');
                    for _ in 0..rng.usize(0..8) {
                        out.push_str(
                            ["a", "Z", " ", "é", "日", "\\\"", "\\\\", "\\n", "\\u0041", "\\ud83d\\ude00", "\\u0000", "/", "\\/"]
                                [rng.usize(0..13)],
                        );
                    }
                    out.push('"');
                }
            }
        } else if rng.bool() {
            out.push('[');
            let n = rng.usize(0..5);
            for i in 0..n {
                if i > 0 {
                    out.push(',');
                }
                gen_text(rng, depth - 1, out);
            }
            ws(rng, out);
            out.push(']');
        } else {
            out.push('{');
            let n = rng.usize(0..6);
            for i in 0..n {
                if i > 0 {
                    out.push(',');
                }
                ws(rng, out);
                // Few distinct keys, so duplicates and disorder are common.
                out.push_str(["\"a\"", "\"b\"", "\"c\"", "\"aa\"", "\"\"", "\"\\u0061\"", "\"B\"", "\"é\""][rng.usize(0..8)]);
                ws(rng, out);
                out.push(':');
                gen_text(rng, depth - 1, out);
            }
            ws(rng, out);
            out.push('}');
        }
        ws(rng, out);
    }

    #[test]
    fn generated_documents_encode_identically() {
        let mut rng = fastrand::Rng::with_seed(0x5eed_1500);
        let mut text = String::new();
        let mut broken = 0usize;
        for i in 0..40_000 {
            text.clear();
            gen_text(&mut rng, 4, &mut text);
            // Every tenth document is damaged, so errors are compared too.
            if i % 10 == 0 && !text.is_empty() {
                let at = rng.usize(0..text.len());
                if text.is_char_boundary(at) {
                    text.insert(at, [',', ']', '}', '"', ':', 'x'][rng.usize(0..6)]);
                }
            }
            broken += reference(&text).is_err() as usize;
            check(&text);
        }
        assert!(broken > 1000, "only {broken} malformed documents were exercised");
    }

    /// Insert-path cost: text to blob through a `Value` against straight.
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored encode_text_bench --nocapture`
    #[test]
    #[ignore]
    fn encode_text_bench() {
        let docs: Vec<String> = (0..2000)
            .map(|i| {
                format!(
                    r#"{{"id":{i},"status":"{}","is_risk_flagged":{},"tags":["alpha","beta","t{}"],"meta":{{"score":{}.5,"owner":"user-{}","nested":{{"k":[1,2,3],"label":"a \"quoted\" label"}}}},"amount":1{}.25,"created_at":"2026-10-08T12:34:56Z","notes":null}}"#,
                    ["open", "closed", "pending"][i % 3],
                    i % 2 == 0,
                    i % 7,
                    i % 100,
                    i % 13,
                    i % 50,
                )
            })
            .collect();
        let bytes: usize = docs.iter().map(|d| d.len()).sum();
        for round in 0..6 {
            let t = std::time::Instant::now();
            let mut sum = 0usize;
            for d in &docs {
                sum += reference(std::hint::black_box(d)).unwrap().len();
            }
            let old_ns = t.elapsed().as_nanos() as f64 / docs.len() as f64;
            let t = std::time::Instant::now();
            let mut sum2 = 0usize;
            for d in &docs {
                sum2 += encode_text(std::hint::black_box(d)).unwrap().len();
            }
            let new_ns = t.elapsed().as_nanos() as f64 / docs.len() as f64;
            assert_eq!(sum, sum2);
            println!(
                "round {round}: via Value {old_ns:.0} ns/doc, direct {new_ns:.0} ns/doc ({:.2}x), {} bytes/doc",
                old_ns / new_ns,
                bytes / docs.len()
            );
        }
    }
}

#[cfg(test)]
mod gin_item_tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// A random scalar, spelled several ways so equal values meet unequal
    /// bytes: `1`, `1.0`, `1e0`, `-0.0` and `0` all occur.
    fn scalar(r: &mut Rng) -> String {
        match r.below(16) {
            0 => "null".into(),
            1 => "true".into(),
            2 => "false".into(),
            3 => "1".into(),
            4 => "1.0".into(),
            5 => "1e0".into(),
            6 => "0".into(),
            7 => "-0.0".into(),
            8 => "2.50".into(),
            9 => "\"a\"".into(),
            10 => "\"b\"".into(),
            11 | 12 => NUMBERS[r.below(NUMBERS.len() as u64) as usize].into(),
            _ => "\"\"".into(),
        }
    }

    /// Spellings that meet as equal values with different digits, scale or
    /// exponent, past the `f64` range and past its precision, so that
    /// containment between them is exercised and not just random misses.
    const NUMBERS: &[&str] = &[
        "1", "1.0", "1.00", "1e0", "10e-1", "0.1e1", "-0", "0e5", "-0.000", "100", "1e2", "100.00",
        "1E+2", "1e400", "1E+400", "10e399", "-1e400", "1e-400", "0.1e-399", "1e-401",
        "1.000000000000000000001", "1.0000000000000000000010", "1.000000000000000000002",
        "9007199254740993", "9007199254740993.0", "9.007199254740993e15", "9007199254740992",
        "123456789012345678901234567890", "1.2345678901234567890123456789e29", "0.1", "0.10000000000000001",
    ];

    fn value(r: &mut Rng, depth: u32) -> String {
        if depth == 0 || r.below(3) == 0 {
            return scalar(r);
        }
        let n = r.below(4) as usize;
        if r.below(2) == 0 {
            let items: Vec<String> = (0..n).map(|_| value(r, depth - 1)).collect();
            format!("[{}]", items.join(","))
        } else {
            let keys = ["a", "b", "c", "d"];
            let items: Vec<String> = (0..n)
                .map(|_| format!("\"{}\":{}", keys[r.below(4) as usize], value(r, depth - 1)))
                .collect();
            format!("{{{}}}", items.join(","))
        }
    }

    /// A query likely to be contained in `doc`: a random piece of it, with
    /// numbers respelled, so the property is exercised on true containments
    /// rather than on the random pairs that almost never contain.
    fn carve(v: &serde_json::Value, r: &mut Rng) -> serde_json::Value {
        use serde_json::Value;
        match v {
            Value::Object(m) => {
                let mut out = serde_json::Map::new();
                for (k, x) in m {
                    if r.below(2) == 0 {
                        out.insert(k.clone(), carve(x, r));
                    }
                }
                Value::Object(out)
            }
            Value::Array(a) => {
                let mut out = Vec::new();
                for x in a {
                    if r.below(2) == 0 {
                        out.push(carve(x, r));
                    }
                }
                Value::Array(out)
            }
            Value::Number(n) if n.as_f64() == Some(1.0) && r.below(2) == 0 => serde_json::json!(1.0),
            other => other.clone(),
        }
    }

    fn items(doc: &[u8], opclass: GinOpclass) -> std::collections::HashSet<u64> {
        let mut out = Vec::new();
        gin_items(doc, opclass, &mut out).expect("well-formed");
        out.into_iter().collect()
    }

    #[test]
    fn a_contained_query_has_only_items_the_document_has() {
        let mut r = Rng(0x5eed_9e37);
        let mut contained = 0;
        for _ in 0..20_000 {
            let doc_text = value(&mut r, 4);
            let doc = encode_text(&doc_text).unwrap();
            // serde_json cannot read a number past the `f64` range, so such a
            // document is carved from with its numbers masked; its query then
            // holds 0s there, which is still a containment to test.
            let doc_value: serde_json::Value = serde_json::from_str(&doc_text)
                .or_else(|_| serde_json::from_str(&mask_numbers(&doc_text)))
                .unwrap();
            let query_text = match r.below(3) {
                0 => value(&mut r, 3),
                1 => scalar(&mut r),
                _ => carve(&doc_value, &mut r).to_string(),
            };
            let query = encode_text(&query_text).unwrap();
            if contains(&doc, &query) != Some(true) {
                continue;
            }
            contained += 1;
            for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
                let have = items(&doc, opclass);
                let want = items(&query, opclass);
                assert!(
                    want.is_subset(&have),
                    "{doc_text} @> {query_text} under {opclass:?}, but the query has an item the document lacks"
                );
            }
        }
        assert!(contained > 2_000, "too few containments to test ({contained})");
    }

    /// The index rests on this: two scalars hash alike exactly when
    /// `scalar_eq` calls them equal. Hashing apart what compares equal drops
    /// a matching row; the converse only costs a recheck, but is asserted too
    /// since here it would mean the hash ignores part of the value. Every
    /// pair is tried, encoded as this build writes it and as an older build
    /// stored a float's text, so stored documents of either age meet queries
    /// of the other.
    #[test]
    fn numbers_hash_alike_exactly_when_they_compare_equal() {
        fn legacy(text: &str) -> Vec<u8> {
            let mut out = vec![TAG_NUM];
            write_len_prefixed(&mut out, text.as_bytes());
            out
        }
        let hash = |bytes: &[u8]| {
            use std::hash::Hasher;
            let mut h = item_hasher();
            hash_scalar(bytes, &mut h).unwrap();
            h.finish()
        };
        let mut encoded: Vec<(String, Vec<u8>)> = Vec::new();
        for text in NUMBERS.iter().chain(["1e+20", "1e20", "-0.0", "0.0", "1.2345678901234568e+29", "5e-324", "1e-7"].iter()) {
            encoded.push((text.to_string(), encode_text(text).unwrap()));
            encoded.push((format!("legacy {text}"), legacy(text)));
        }
        let mut equal_pairs = 0;
        for (ta, a) in &encoded {
            for (tb, b) in &encoded {
                let eq = scalar_eq(a, b).unwrap();
                equal_pairs += eq as usize;
                assert_eq!(hash(a) == hash(b), eq, "{ta} vs {tb}: scalar_eq says {eq}");
            }
        }
        assert!(equal_pairs > 4 * encoded.len(), "too few equal pairs ({equal_pairs})");
    }

    #[test]
    fn equal_numbers_make_equal_items_and_kinds_do_not_mix() {
        let one = |t: &str| items(&encode_text(t).unwrap(), GinOpclass::PathOps);
        assert_eq!(one(r#"{"a":1}"#), one(r#"{"a":1.0}"#));
        assert_eq!(one(r#"{"a":1}"#), one(r#"{"a":1e0}"#));
        assert_eq!(one(r#"{"a":0}"#), one(r#"{"a":-0.0}"#));
        assert_ne!(one(r#"{"a":1}"#), one(r#"{"a":"1"}"#));
        assert_ne!(one(r#"{"a":1}"#), one(r#"{"b":1}"#));
        // Array nesting is not a path step.
        assert_eq!(one(r#"{"a":[1]}"#), one(r#"{"a":1}"#));
        // `?` finds top-level keys and string elements, and a bare string.
        let ops = |t: &str| items(&encode_text(t).unwrap(), GinOpclass::Ops);
        assert!(ops(r#"{"k":1}"#).contains(&gin_key_item(b"k")));
        assert!(ops(r#"["k",2]"#).contains(&gin_key_item(b"k")));
        assert!(ops(r#""k""#).contains(&gin_key_item(b"k")));
        assert!(!ops(r#"[1]"#).contains(&gin_key_item(b"1")));
        // NULL (no bytes) has no items.
        let mut out = Vec::new();
        gin_items(&[], GinOpclass::Ops, &mut out).unwrap();
        assert!(out.is_empty());
    }

    /// `v` written out again with each number spelled another way it can be
    /// spelled, so an equal document arrives as different bytes.
    fn respell(v: &serde_json::Value, r: &mut Rng) -> String {
        use serde_json::Value;
        match v {
            Value::Object(m) => {
                let items: Vec<String> = m
                    .iter()
                    .map(|(k, x)| format!("{}:{}", Value::String(k.clone()), respell(x, r)))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
            Value::Array(a) => {
                let items: Vec<String> = a.iter().map(|x| respell(x, r)).collect();
                format!("[{}]", items.join(","))
            }
            Value::Number(n) => {
                let spellings: &[&str] = match n.as_f64() {
                    Some(f) if f == 1.0 => &["1", "1.0", "1e0", "10e-1"],
                    Some(f) if f == 0.0 => &["0", "-0.0", "0e3", "0.00"],
                    Some(f) if f == 2.5 => &["2.50", "2.5", "25e-1"],
                    _ => return n.to_string(),
                };
                spellings[r.below(spellings.len() as u64) as usize].to_string()
            }
            other => other.to_string(),
        }
    }

    /// PostgreSQL's jsonb equality, on parsed values: numbers by value,
    /// arrays in order, objects by key.
    fn reference_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
        use serde_json::Value;
        match (a, b) {
            (Value::Number(x), Value::Number(y)) => json_numbers_equal(x, y),
            (Value::Array(x), Value::Array(y)) => {
                x.len() == y.len() && x.iter().zip(y).all(|(p, q)| reference_equal(p, q))
            }
            (Value::Object(x), Value::Object(y)) => {
                x.len() == y.len()
                    && x.iter().all(|(k, p)| y.get(k).is_some_and(|q| reference_equal(p, q)))
            }
            _ => a == b,
        }
    }

    /// `values_equal` is jsonb value equality, `canonical_key` is equal
    /// exactly when it is, and a hash index key built from it agrees with the
    /// evaluator's `=` — the agreement `index::key` requires of every type it
    /// keys, or a lookup misses a row the scan returns.
    #[test]
    fn document_equality_is_value_equality_and_keys_agree_with_it() {
        use crate::core::db_type::DbType;
        use crate::core::processor::transformer::ComparerOperation;
        use crate::core::row::logical::perform_comparison_operation;
        let mut r = Rng(0x0e9a_1172);
        let mut equal = 0;
        for _ in 0..20_000 {
            let a_text = value(&mut r, 3);
            let a_value: serde_json::Value = serde_json::from_str(&a_text).unwrap();
            let b_text = match r.below(3) {
                0 => value(&mut r, 3),
                _ => respell(&a_value, &mut r),
            };
            let b_value: serde_json::Value = serde_json::from_str(&b_text).unwrap();
            let (a, b) = (encode_text(&a_text).unwrap(), encode_text(&b_text).unwrap());
            let want = reference_equal(&a_value, &b_value);
            equal += usize::from(want);
            assert_eq!(values_equal(&a, &b), Some(want), "{a_text} = {b_text}");

            let (mut ka, mut kb) = (Vec::new(), Vec::new());
            canonical_key(&a, &mut ka).unwrap();
            canonical_key(&b, &mut kb).unwrap();
            assert_eq!(ka == kb, want, "keys of {a_text} and {b_text}");

            let (mut ha, mut hb) = (Vec::new(), Vec::new());
            crate::core::index::key::encode_canonical(&DbType::JSONB, &a, &mut ha).unwrap();
            crate::core::index::key::encode_canonical(&DbType::JSONB, &b, &mut hb).unwrap();
            let evaluator =
                perform_comparison_operation(&a, &b, &DbType::JSONB, &ComparerOperation::Equals).unwrap();
            assert_eq!(evaluator, want, "{a_text} = {b_text}");
            assert_eq!(ha == hb, evaluator, "hash keys of {a_text} and {b_text}");
            let unequal =
                perform_comparison_operation(&a, &b, &DbType::JSONB, &ComparerOperation::NotEquals).unwrap();
            assert_eq!(unequal, !want, "{a_text} <> {b_text}");
        }
        assert!(equal > 5_000, "too few equal pairs to test ({equal})");
    }

    /// PostgreSQL 17's answers for the cases a byte compare got wrong, and the
    /// ones that must stay unequal.
    #[test]
    fn document_equality_matches_postgresql() {
        let eq = |a: &str, b: &str| values_equal(&encode_text(a).unwrap(), &encode_text(b).unwrap());
        // `SELECT '1'::jsonb = '[1]'::jsonb, '[]'::jsonb = '{}'::jsonb,
        //  '{"a":[1,2]}'::jsonb = '{"a":[1.0,2e0]}'::jsonb,
        //  '[1,2]'::jsonb = '[2,1]'::jsonb, '-0'::jsonb = '0'::jsonb`
        // gives f, f, t, f, t.
        assert_eq!(eq("1", "[1]"), Some(false));
        assert_eq!(eq("[]", "{}"), Some(false));
        assert_eq!(eq(r#"{"a":[1,2]}"#, r#"{"a":[1.0,2e0]}"#), Some(true));
        assert_eq!(eq("[1,2]", "[2,1]"), Some(false));
        assert_eq!(eq("-0", "0"), Some(true));
        assert_eq!(eq("1", "1.0"), Some(true));
        assert_eq!(eq(r#"{"n":1.0,"a":1}"#, r#"{"a":1,"n":1}"#), Some(true));
        assert_eq!(eq(r#"{"a":1}"#, r#"{"a":1,"b":null}"#), Some(false));
        assert_eq!(eq("null", "null"), Some(true));
        assert_eq!(eq(r#""1""#, "1"), Some(false));
        // Bytes that are not a document are not compared as one.
        assert_eq!(values_equal(br#"{"a":1}"#, &encode_text(r#"{"a":1}"#).unwrap()), None);
        // Nor are the bytes of a number or a boolean that happen to begin with
        // a scalar's tag: `i32` 1 starts like `false`, `f64` 2.5 like `null`,
        // `i32` 2 like `true`.
        let doc = |t: &str| encode_text(t).unwrap();
        assert_eq!(values_equal(&1i32.to_le_bytes(), &doc("false")), None);
        assert_eq!(values_equal(&doc("false"), &1i32.to_le_bytes()), None);
        assert_eq!(values_equal(&2.5f64.to_le_bytes(), &doc("null")), None);
        assert_eq!(values_equal(&2i32.to_le_bytes(), &doc("true")), None);
        assert_eq!(values_equal(&[3, 1, 0, 0, 0, 0, 0, 0, 0, 9], &doc("1")), None);
        let mut long_text = doc(r#""x""#);
        long_text.push(0);
        assert_eq!(values_equal(&long_text, &doc(r#""x""#)), None);
        // And the evaluator falls back to the bytes for them, which differ.
        use crate::core::db_type::DbType;
        use crate::core::processor::transformer::ComparerOperation;
        use crate::core::row::logical::perform_comparison_operation;
        let equals = |a: &[u8], b: &[u8]| {
            perform_comparison_operation(a, b, &DbType::JSONB, &ComparerOperation::Equals).unwrap()
        };
        assert!(!equals(&doc("false"), &1i32.to_le_bytes()));
        assert!(!equals(&doc("null"), &2.5f64.to_le_bytes()));
        assert!(equals(&doc("false"), &doc("false")));
    }
}
