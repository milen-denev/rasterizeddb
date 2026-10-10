//! Redis's glob, for `KEYS`, `SCAN … MATCH` and `PSUBSCRIBE`: `*`, `?`,
//! `[abc]`, `[a-z]`, `[^a]`, and `\` to escape.
//!
//! Iterative with one backtrack point, so it is linear in practice and never
//! recurses: a client-supplied pattern cannot exhaust the stack.

pub fn matches(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    // Where the last `*` was, and how much of the text it had consumed, so a
    // mismatch further on can retry with the star eating one more byte.
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some((p, t));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next)) = class_match(&pattern[p..], text[t]) {
                        if matched {
                            p += next;
                            t += 1;
                            continue;
                        }
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                c => {
                    if c == text[t] {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
            }
        }
        match star {
            Some((sp, st)) => {
                p = sp + 1;
                t = st + 1;
                star = Some((sp, st + 1));
            }
            None => return false,
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Matches `c` against the class at `class[0] == b'['`. Returns whether it
/// matched and how many pattern bytes the class spans, or `None` for an
/// unterminated class, which then matches nothing.
fn class_match(class: &[u8], c: u8) -> Option<(bool, usize)> {
    let mut i = 1;
    let negate = class.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    loop {
        let ch = *class.get(i)?;
        if ch == b']' && !first {
            break;
        }
        first = false;
        let lo = if ch == b'\\' {
            i += 1;
            *class.get(i)?
        } else {
            ch
        };
        if class.get(i + 1) == Some(&b'-') && class.get(i + 2).is_some_and(|&h| h != b']') {
            let hi = class[i + 2];
            if lo <= c && c <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if lo == c {
                matched = true;
            }
            i += 1;
        }
    }
    Some((matched != negate, i + 1))
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn redis_glob_cases() {
        assert!(matches(b"*", b""));
        assert!(matches(b"*", b"anything"));
        assert!(matches(b"h?llo", b"hello"));
        assert!(!matches(b"h?llo", b"hllo"));
        assert!(matches(b"h*llo", b"heeeello"));
        assert!(matches(b"h[ae]llo", b"hallo"));
        assert!(!matches(b"h[ae]llo", b"hillo"));
        assert!(matches(b"h[^e]llo", b"hallo"));
        assert!(!matches(b"h[^e]llo", b"hello"));
        assert!(matches(b"h[a-b]llo", b"hbllo"));
        assert!(matches(b"user:*:name", b"user:42:name"));
        assert!(!matches(b"user:*:name", b"user:42:mail"));
        assert!(matches(b"a\\*b", b"a*b"));
        assert!(!matches(b"a\\*b", b"axb"));
        assert!(!matches(b"[abc", b"a"));
        // Pathological for a recursive matcher; linear here.
        let text = vec![b'a'; 10_000];
        assert!(!matches(b"*a*a*a*a*a*a*a*b", &text));
    }
}
