//! `json`: a document kept as the text it was written as (`DbType::JSONTEXT`).
//!
//! PostgreSQL's `json` stores its input verbatim — whitespace, key order and
//! duplicate keys included — and its operators read the text each time:
//! `->` hands back the member's own text, `{"a": [1,  2]}` -> 'a' is `[1,  2]`
//! with its two spaces, and of duplicate keys the last is the one found. This
//! module is that reading: a scanner over the text that finds a member's span
//! without building the document, so what `->` returns is a slice of what was
//! stored.
//!
//! The text is one document already (`value_codec::check_json_text` refused
//! anything else on the way in), so the scanner trusts its shape and reports
//! a malformed span as an error rather than guessing.

use crate::core::jsonb::Step;

/// A byte span of the stored text.
type Span = std::ops::Range<usize>;

fn malformed(at: usize) -> String {
    format!("invalid input syntax for type json: malformed document at byte {at}")
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// The end of the string whose opening quote is at `i`.
fn string_end(b: &[u8], mut i: usize) -> Result<usize, String> {
    let start = i;
    i += 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return Ok(i + 1),
            _ => i += 1,
        }
    }
    Err(malformed(start))
}

/// The end of the value starting at `i` (whitespace already skipped).
fn value_end(b: &[u8], i: usize) -> Result<usize, String> {
    match b.get(i) {
        Some(b'"') => string_end(b, i),
        Some(b'{') | Some(b'[') => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = string_end(b, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Ok(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            Err(malformed(i))
        }
        Some(_) => {
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') {
                j += 1;
            }
            if j == i { Err(malformed(i)) } else { Ok(j) }
        }
        None => Err(malformed(i)),
    }
}

/// The span of the whole document, without its surrounding whitespace.
fn document(text: &str) -> Result<Span, String> {
    let b = text.as_bytes();
    let start = skip_ws(b, 0);
    let end = value_end(b, start)?;
    Ok(start..end)
}

/// The unescaped text of the string spanning `span` (quotes included).
fn unescape(text: &str, span: Span) -> Result<String, String> {
    serde_json::from_str::<String>(&text[span.clone()]).map_err(|_| malformed(span.start))
}

/// Of object `span`, the value of the last member keyed `key`.
fn member(text: &str, span: Span, key: &str) -> Result<Option<Span>, String> {
    let b = text.as_bytes();
    let mut i = skip_ws(b, span.start + 1);
    let mut found = None;
    if b.get(i) == Some(&b'}') {
        return Ok(None);
    }
    loop {
        if b.get(i) != Some(&b'"') {
            return Err(malformed(i));
        }
        let key_end = string_end(b, i)?;
        let name = unescape(text, i..key_end)?;
        i = skip_ws(b, key_end);
        if b.get(i) != Some(&b':') {
            return Err(malformed(i));
        }
        i = skip_ws(b, i + 1);
        let end = value_end(b, i)?;
        if name == key {
            found = Some(i..end);
        }
        i = skip_ws(b, end);
        match b.get(i) {
            Some(b',') => i = skip_ws(b, i + 1),
            Some(b'}') => return Ok(found),
            _ => return Err(malformed(i)),
        }
    }
}

/// Of array `span`, its elements' spans.
fn elements(text: &str, span: Span) -> Result<Vec<Span>, String> {
    let b = text.as_bytes();
    let mut i = skip_ws(b, span.start + 1);
    let mut out = Vec::new();
    if b.get(i) == Some(&b']') {
        return Ok(out);
    }
    loop {
        let end = value_end(b, i)?;
        out.push(i..end);
        i = skip_ws(b, end);
        match b.get(i) {
            Some(b',') => i = skip_ws(b, i + 1),
            Some(b']') => return Ok(out),
            _ => return Err(malformed(i)),
        }
    }
}

fn element(text: &str, span: Span, index: i64) -> Result<Option<Span>, String> {
    let all = elements(text, span)?;
    let at = if index < 0 { all.len() as i64 + index } else { index };
    Ok(usize::try_from(at).ok().and_then(|at| all.get(at).cloned()))
}

/// The span `steps` reach in the stored `text`, `None` when one is missing
/// (or meets a container of the other kind), as `->` and `#>` answer.
pub fn get_path(text: &str, steps: &[Step]) -> Result<Option<Span>, String> {
    let mut at = document(text)?;
    for step in steps {
        let kind = text.as_bytes()[at.start];
        let next = match (step, kind) {
            (Step::Key(k), b'{') => member(text, at, k)?,
            (Step::Index(n), b'[') => element(text, at, *n)?,
            (Step::Either(k), b'{') => member(text, at, k)?,
            (Step::Either(k), b'[') => match k.parse::<i64>() {
                Ok(n) => element(text, at, n)?,
                Err(_) => None,
            },
            _ => None,
        };
        let Some(next) = next else { return Ok(None) };
        at = next;
    }
    Ok(Some(at))
}

/// What `->>` / `#>>` gives for the value at `span`: a string unescaped, a
/// JSON `null` as SQL NULL, anything else its text as stored.
pub fn as_text(text: &str, span: Span) -> Result<Option<String>, String> {
    let raw = &text[span.clone()];
    match text.as_bytes()[span.start] {
        b'"' => unescape(text, span).map(Some),
        _ if raw == "null" => Ok(None),
        _ => Ok(Some(raw.to_string())),
    }
}

/// `json_typeof`: what the document's outermost value is.
pub fn type_of(text: &str) -> Result<&'static str, String> {
    let span = document(text)?;
    let raw = &text[span.clone()];
    Ok(match text.as_bytes()[span.start] {
        b'{' => "object",
        b'[' => "array",
        b'"' => "string",
        b't' | b'f' => "boolean",
        b'n' if raw == "null" => "null",
        _ => "number",
    })
}

/// `json_array_length`: the outermost array's element count.
pub fn array_length(text: &str) -> Result<i32, String> {
    let span = document(text)?;
    if text.as_bytes()[span.start] != b'[' {
        return Err(if text.as_bytes()[span.start] == b'{' {
            "cannot get array length of a non-array".to_string()
        } else {
            "cannot get array length of a scalar".to_string()
        });
    }
    Ok(elements(text, span)?.len() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(text: &str, steps: &[Step]) -> Option<String> {
        get_path(text, steps).unwrap().map(|s| text[s].to_string())
    }

    /// Every answer here is PostgreSQL 17's.
    #[test]
    fn members_are_their_own_text_and_the_last_duplicate_wins() {
        let doc = r#" {"b": 1,  "a": [1,  2], "b": 3} "#;
        assert_eq!(path(doc, &[Step::Key("a".into())]).as_deref(), Some("[1,  2]"));
        assert_eq!(path(doc, &[Step::Key("b".into())]).as_deref(), Some("3"));
        assert_eq!(path(doc, &[Step::Key("z".into())]), None);
        assert_eq!(path(doc, &[Step::Key("a".into()), Step::Index(-1)]).as_deref(), Some("2"));
        assert_eq!(path(doc, &[Step::Index(0)]), None);
        let doc = r#"[1, {"z" : true}]"#;
        assert_eq!(path(doc, &[Step::Index(1)]).as_deref(), Some(r#"{"z" : true}"#));
        assert_eq!(path(doc, &[Step::Either("1".into()), Step::Key("z".into())]).as_deref(), Some("true"));
        assert_eq!(path(doc, &[Step::Index(5)]), None);
    }

    #[test]
    fn text_reads_strings_unescaped_and_null_as_null() {
        let doc = r#"{"a":"é", "n": null, "x": [1,  2]}"#;
        let at = |k: &str| get_path(doc, &[Step::Key(k.into())]).unwrap().unwrap();
        assert_eq!(as_text(doc, at("a")).unwrap().as_deref(), Some("é"));
        assert_eq!(as_text(doc, at("n")).unwrap(), None);
        assert_eq!(as_text(doc, at("x")).unwrap().as_deref(), Some("[1,  2]"));
        assert_eq!(type_of("  [1]").unwrap(), "array");
        assert_eq!(type_of("1e2").unwrap(), "number");
        assert_eq!(array_length("[1,2, 3]").unwrap(), 3);
        assert!(array_length("{}").is_err());
    }
}
