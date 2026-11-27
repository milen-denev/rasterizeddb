//! The clauses after a `CREATE LEDGER TABLE`'s column list, or a
//! `CREATE VAULT`'s name (docs/ledger-and-vault-plan.md §3.1, §4.2):
//!
//! ```text
//! NO DELETE UNTIL <n> <unit> AFTER INSERT     -- ledger: required
//! NO DROP UNTIL <n> <unit> IDLE
//! HASHING USING 'sha256'
//! ANCHOR TO '<sink>' [, '<sink>' …]
//! RETAIN DELETED <n> <unit>                   -- vault: purge protection
//! KEY PROVIDER '<uri>'                        -- vault: required
//! VERSIONED                                   -- ledger: rows may be updated; every version kept
//! ```
//!
//! Units count long rather than short, so a retention never ends early: a
//! `MONTH` is 31 days, a `YEAR` 366.

use rasterizeddb_core::core::row::schema::SpecialTable;

use super::policy::{HashAlg, LedgerPolicy};

const SECOND: i64 = 1_000_000;
const DAY: i64 = 86_400 * SECOND;

/// A token: a word, a number, a quoted string, or a comma.
#[derive(Debug, PartialEq)]
enum Tok {
    Word(String),
    Quoted(String),
    Comma,
}

fn tokens(s: &str) -> Result<Vec<Tok>, String> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == ',' {
            chars.next();
            out.push(Tok::Comma);
        } else if c == '\'' {
            chars.next();
            let mut v = String::new();
            loop {
                match chars.next() {
                    None => return Err("a quoted value is not closed".to_string()),
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                        v.push('\'');
                    }
                    Some('\'') => break,
                    Some(ch) => v.push(ch),
                }
            }
            out.push(Tok::Quoted(v));
        } else {
            let mut w = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == ',' || ch == '\'' {
                    break;
                }
                w.push(ch);
                chars.next();
            }
            out.push(Tok::Word(w.to_ascii_uppercase()));
        }
    }
    Ok(out)
}

fn unit(word: &str) -> Option<i64> {
    Some(match word.trim_end_matches('S') {
        "SECOND" => SECOND,
        "MINUTE" => 60 * SECOND,
        "HOUR" => 3_600 * SECOND,
        "DAY" => DAY,
        "WEEK" => 7 * DAY,
        "MONTH" => 31 * DAY,
        "YEAR" => 366 * DAY,
        _ => return None,
    })
}

struct Cursor {
    toks: Vec<Tok>,
    at: usize,
}

impl Cursor {
    fn word(&mut self, w: &str) -> bool {
        if matches!(self.toks.get(self.at), Some(Tok::Word(x)) if x == w) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, w: &str) -> Result<(), String> {
        if self.word(w) { Ok(()) } else { Err(format!("expected {w} at {}", self.here())) }
    }

    fn here(&self) -> String {
        match self.toks.get(self.at) {
            None => "the end".to_string(),
            Some(Tok::Word(w)) => format!("{w:?}"),
            Some(Tok::Quoted(q)) => format!("'{q}'"),
            Some(Tok::Comma) => "','".to_string(),
        }
    }

    fn duration(&mut self) -> Result<i64, String> {
        let n: i64 = match self.toks.get(self.at) {
            Some(Tok::Word(w)) => w.parse().map_err(|_| format!("expected a number at {}", self.here()))?,
            _ => return Err(format!("expected a number at {}", self.here())),
        };
        self.at += 1;
        let per = match self.toks.get(self.at) {
            Some(Tok::Word(w)) => unit(w).ok_or_else(|| format!("{w} is not a unit of time"))?,
            _ => return Err(format!("expected a unit of time at {}", self.here())),
        };
        self.at += 1;
        if n < 0 {
            return Err("a duration cannot be negative".to_string());
        }
        n.checked_mul(per).ok_or_else(|| "that duration is too long".to_string())
    }

    fn quoted(&mut self) -> Result<String, String> {
        match self.toks.get(self.at) {
            Some(Tok::Quoted(q)) => {
                self.at += 1;
                if q.is_empty() || q.chars().any(|c| c.is_whitespace() || c.is_control()) {
                    return Err(format!("'{q}': a sink or provider has no spaces or control characters"));
                }
                Ok(q.clone())
            }
            _ => Err(format!("expected a quoted value at {}", self.here())),
        }
    }
}

/// The policy a new table of `kind` gets from its clauses.
pub fn parse(kind: SpecialTable, clauses: &str) -> Result<LedgerPolicy, String> {
    let mut c = Cursor { toks: tokens(clauses)?, at: 0 };
    let mut p = LedgerPolicy::new(kind, 0, 0);
    let mut retention = None;
    let vault = kind == SpecialTable::Vault;
    while c.at < c.toks.len() {
        if c.word("NO") {
            if c.word("DELETE") {
                c.expect("UNTIL")?;
                let d = c.duration()?;
                c.expect("AFTER")?;
                c.expect("INSERT")?;
                retention = Some(d);
            } else if c.word("DROP") {
                c.expect("UNTIL")?;
                p.drop_idle_us = c.duration()?;
                c.expect("IDLE")?;
            } else {
                return Err(format!("expected DELETE or DROP after NO, at {}", c.here()));
            }
        } else if !vault && c.word("VERSIONED") {
            p.versioned = true;
        } else if c.word("HASHING") {
            c.expect("USING")?;
            p.hash = match c.quoted()?.to_ascii_lowercase().as_str() {
                "sha256" | "sha2_256" => HashAlg::Sha256,
                other => return Err(format!("'{other}' is not a hash a ledger chains with (sha256)")),
            };
        } else if c.word("ANCHOR") {
            c.expect("TO")?;
            loop {
                let sink = c.quoted()?;
                if !(sink.starts_with("dir:") || sink.starts_with("s3://")) {
                    return Err(format!("'{sink}': an anchor is dir:<path> or s3://<bucket>/<prefix>"));
                }
                if !p.anchors.contains(&sink) {
                    p.anchors.push(sink);
                }
                if !matches!(c.toks.get(c.at), Some(Tok::Comma)) {
                    break;
                }
                c.at += 1;
            }
        } else if vault && c.word("RETAIN") {
            c.expect("DELETED")?;
            retention = Some(c.duration()?);
        } else if vault && c.word("KEY") {
            c.expect("PROVIDER")?;
            let k = c.quoted()?;
            if !(k.starts_with("file:") || k.starts_with("pkcs11:") || k.starts_with("mlkem:") || k.starts_with("tpm:")) {
                return Err(format!(
                    "'{k}': a key provider is file:<path>, pkcs11:<attributes> or mlkem:<path> (post-quantum)"
                ));
            }
            p.key_provider = Some(k);
        } else {
            return Err(format!("unexpected {} in the {} clauses", c.here(), if vault { "vault" } else { "ledger" }));
        }
    }
    match (vault, retention) {
        (false, None) => return Err("a ledger table needs NO DELETE UNTIL <n> <unit> AFTER INSERT".to_string()),
        (true, None) => p.retention_us = 90 * DAY,
        (_, Some(r)) => p.retention_us = r,
    }
    if vault && p.key_provider.is_none() {
        return Err("a vault needs KEY PROVIDER 'file:<path>', 'pkcs11:<attributes>' or 'mlkem:<path>'".to_string());
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_clauses() {
        let p = parse(
            SpecialTable::Ledger,
            "NO DELETE UNTIL 7 YEARS AFTER INSERT no drop until 90 days idle HASHING USING 'sha256' \
             ANCHOR TO 'dir:/mnt/d2/l', 's3://b/p'",
        )
        .unwrap();
        assert_eq!(p.retention_us, 7 * 366 * DAY);
        assert_eq!(p.drop_idle_us, 90 * DAY);
        assert_eq!(p.anchors, vec!["dir:/mnt/d2/l".to_string(), "s3://b/p".to_string()]);
    }

    #[test]
    fn refusals() {
        assert!(parse(SpecialTable::Ledger, "NO DROP UNTIL 1 DAY IDLE").is_err());
        assert!(parse(SpecialTable::Ledger, "NO DELETE UNTIL 1 FORTNIGHT AFTER INSERT").is_err());
        assert!(parse(SpecialTable::Ledger, "NO DELETE UNTIL 1 DAY AFTER INSERT ANCHOR TO 'ftp://x'").is_err());
        assert!(parse(SpecialTable::Ledger, "NO DELETE UNTIL 1 DAY AFTER INSERT KEY PROVIDER 'file:/k'").is_err());
        assert!(parse(SpecialTable::Vault, "RETAIN DELETED 30 DAYS").is_err());
    }

    #[test]
    fn vault_clauses() {
        let p = parse(SpecialTable::Vault, "KEY PROVIDER 'pkcs11:module=/usr/lib/softhsm.so;token=rdb;key=kek'").unwrap();
        assert_eq!(p.retention_us, 90 * DAY);
        assert!(p.key_provider.unwrap().starts_with("pkcs11:"));
    }
}
