//! Encrypted columns (docs/ledger-and-vault-plan.md §4.7): the licensed
//! side of `rasterizeddb_core::core::column_crypto`.
//!
//! ```sql
//! CREATE TABLE patients (id BIGINT, ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii' NOT NULL);
//! ```
//!
//! - **Storage.** Each value is AES-256-GCM ciphertext under the column's own
//!   data key, kept as text: `rdbenc1:<column id>:<base64(nonce|ct|tag)>`,
//!   bound (AAD) to the column id. The data key is wrapped by the vault key
//!   (`aes256-gcm`, or `hsm-aes256-gcm` held in the vault's HSM) and kept in
//!   `encrypted_columns` beside the chain (`ledger::store::chain_dir`).
//! - **Only whole values.** A statement may name an encrypted column only
//!   as a whole select-list or `RETURNING` item, in an `INSERT` column list
//!   with a literal (or `NULL`) for it, or as `SET column = <literal>`.
//!   Anything else that names it (a filter, sort, group, join, index,
//!   function, cast, `DISTINCT`, a set operation, a subquery, `ON CONFLICT`,
//!   `COPY`, a rename) is refused with 0A000 before it runs: an answer over
//!   ciphertext would be wrong without an error.
//! - **Who reads plaintext:** a role holding `SELECT` on the vault, decided
//!   by core per statement. Writing needs it too.
//!
//! The analysis is a token walk, not a parse: anything it cannot place is
//! refused, never let through (an allowlist).

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use parking_lot::Mutex;
use rasterizeddb_core::core::column_crypto::{ColumnCrypto, Plan, MARK};
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::row::schema::SpecialTable;
use zeroize::Zeroizing;

use crate::crypto;

// ---- the registry ---------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Col {
    id: u128,
    table: String,
    column: String,
    vault: String,
    key: String,
    /// The data key, sealed under the vault key.
    wrapped: Vec<u8>,
    /// `VARCHAR(n)`: the plaintext's longest, in characters.
    max_len: Option<u32>,
}

enum Pending {
    Declare(String, Vec<Col>),
    Forget(Vec<String>),
}

#[derive(Default)]
struct Store {
    loaded: bool,
    cols: Vec<Col>,
}

static STORES: OnceLock<Mutex<HashMap<String, Store>>> = OnceLock::new();
static PENDING: OnceLock<Mutex<HashMap<u64, (String, Pending)>>> = OnceLock::new();
static DEKS: OnceLock<Mutex<HashMap<u128, crypto::Dek>>> = OnceLock::new();
static TOKENS: AtomicU64 = AtomicU64::new(1);

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

fn sidecar(store: &str) -> Option<std::path::PathBuf> {
    (!store.starts_with("memory:")).then(|| crate::ledger::store::chain_dir(store).join("encrypted_columns"))
}

fn read_sidecar(store: &str) -> Result<Vec<Col>, String> {
    let Some(path) = sidecar(store) else { return Ok(Vec::new()) };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let bad = || format!("XX001: {} is damaged", path.display());
    let mut cols = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let f: Vec<&str> = line.split(' ').collect();
        if f.len() != 7 {
            return Err(bad());
        }
        let text_of = |s: &str| unhex(s).and_then(|b| String::from_utf8(b).ok()).ok_or_else(bad);
        let id = u128::from_str_radix(f[0], 16).map_err(|_| bad())?;
        cols.push(Col {
            id,
            table: text_of(f[1])?,
            column: text_of(f[2])?,
            vault: text_of(f[3])?,
            key: text_of(f[4])?,
            wrapped: unhex(f[5]).ok_or_else(bad)?,
            max_len: f[6].parse::<u32>().ok().filter(|n| *n > 0),
        });
    }
    Ok(cols)
}

fn write_sidecar(store: &str, cols: &[Col]) -> Result<(), String> {
    let Some(path) = sidecar(store) else { return Ok(()) };
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
    for c in cols {
        writeln!(
            f,
            "{:032x} {} {} {} {} {} {}",
            c.id,
            hex(c.table.as_bytes()),
            hex(c.column.as_bytes()),
            hex(c.vault.as_bytes()),
            hex(c.key.as_bytes()),
            hex(&c.wrapped),
            c.max_len.unwrap_or(0)
        )
        .map_err(|e| e.to_string())?;
    }
    f.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Some(d) = path.parent().and_then(|d| std::fs::File::open(d).ok()) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// The store's encrypted columns, read from its sidecar the first time. An
/// unreadable sidecar is an error for every statement of the store: which
/// columns are encrypted is then unknown, and none may run as if plain.
fn cols(store: &str) -> Result<Vec<Col>, String> {
    let all = STORES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = all.lock();
    let s = map.entry(store.to_string()).or_default();
    if !s.loaded {
        s.cols = read_sidecar(store)?;
        s.loaded = true;
    }
    Ok(s.cols.clone())
}

fn set_cols(store: &str, cols: Vec<Col>) -> Result<(), String> {
    write_sidecar(store, &cols)?;
    let all = STORES.get_or_init(|| Mutex::new(HashMap::new()));
    all.lock().insert(store.to_string(), Store { loaded: true, cols });
    Ok(())
}

// ---- tokens ---------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum K {
    Word,
    QWord,
    Str,
    Num,
    Param,
    Punct(char),
    Op,
}

#[derive(Clone, Debug)]
struct Tok {
    k: K,
    /// A word as written, a quoted name or string unquoted.
    text: String,
    start: usize,
    end: usize,
    /// Parentheses around it.
    depth: i32,
}

impl Tok {
    fn kw(&self, w: &str) -> bool {
        self.k == K::Word && self.text.eq_ignore_ascii_case(w)
    }
    fn is(&self, c: char) -> bool {
        self.k == K::Punct(c)
    }
    fn name(&self) -> Option<String> {
        matches!(self.k, K::Word | K::QWord).then(|| self.text.to_ascii_lowercase())
    }
}

fn lex(sql: &str) -> Result<Vec<Tok>, String> {
    let b: Vec<(usize, char)> = sql.char_indices().collect();
    let at = |i: usize| b.get(i).map(|x| x.1);
    let pos = |i: usize| b.get(i).map_or(sql.len(), |x| x.0);
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i].1;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '-' && at(i + 1) == Some('-') {
            while i < b.len() && b[i].1 != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && at(i + 1) == Some('*') {
            i += 2;
            while i < b.len() && !(b[i].1 == '*' && at(i + 1) == Some('/')) {
                i += 1;
            }
            i += 2;
            continue;
        }
        let start = i;
        let tok = |k: K, text: String, s: usize, e: usize, d: i32| Tok { k, text, start: pos(s), end: pos(e), depth: d };
        match c {
            '\'' => {
                let mut v = String::new();
                i += 1;
                loop {
                    match at(i) {
                        None => return Err("42601: a string is not closed".into()),
                        Some('\'') if at(i + 1) == Some('\'') => {
                            v.push('\'');
                            i += 2;
                        }
                        Some('\'') => {
                            i += 1;
                            break;
                        }
                        Some(ch) => {
                            v.push(ch);
                            i += 1;
                        }
                    }
                }
                out.push(tok(K::Str, v, start, i, depth));
            }
            '"' => {
                let mut v = String::new();
                i += 1;
                loop {
                    match at(i) {
                        None => return Err("42601: a quoted name is not closed".into()),
                        Some('"') if at(i + 1) == Some('"') => {
                            v.push('"');
                            i += 2;
                        }
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some(ch) => {
                            v.push(ch);
                            i += 1;
                        }
                    }
                }
                out.push(tok(K::QWord, v, start, i, depth));
            }
            '$' if at(i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                i += 1;
                while at(i).is_some_and(|d| d.is_ascii_digit()) {
                    i += 1;
                }
                out.push(tok(K::Param, sql[pos(start)..pos(i)].to_string(), start, i, depth));
            }
            '$' => {
                // $tag$ … $tag$: a string.
                let mut j = i + 1;
                while at(j).is_some_and(|d| d.is_alphanumeric() || d == '_') {
                    j += 1;
                }
                if at(j) != Some('$') {
                    out.push(tok(K::Op, "$".into(), start, i + 1, depth));
                    i += 1;
                    continue;
                }
                let tag = &sql[pos(i)..pos(j + 1)];
                let body_start = pos(j + 1);
                let Some(rel) = sql[body_start..].find(tag) else { return Err("42601: a $-quoted string is not closed".into()) };
                let body_end = body_start + rel;
                let end_byte = body_end + tag.len();
                while i < b.len() && b[i].0 < end_byte {
                    i += 1;
                }
                out.push(Tok { k: K::Str, text: sql[body_start..body_end].to_string(), start: pos(start), end: end_byte, depth });
            }
            '0'..='9' => {
                while at(i).is_some_and(|d| d.is_ascii_alphanumeric() || d == '.') {
                    i += 1;
                }
                out.push(tok(K::Num, sql[pos(start)..pos(i)].to_string(), start, i, depth));
            }
            '.' if at(i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                i += 1;
                while at(i).is_some_and(|d| d.is_ascii_alphanumeric() || d == '.') {
                    i += 1;
                }
                out.push(tok(K::Num, sql[pos(start)..pos(i)].to_string(), start, i, depth));
            }
            c if c.is_alphabetic() || c == '_' => {
                while at(i).is_some_and(|d| d.is_alphanumeric() || d == '_' || d == '$') {
                    i += 1;
                }
                out.push(tok(K::Word, sql[pos(start)..pos(i)].to_string(), start, i, depth));
            }
            '(' => {
                out.push(tok(K::Punct('('), "(".into(), start, i + 1, depth));
                depth += 1;
                i += 1;
            }
            ')' => {
                depth -= 1;
                out.push(tok(K::Punct(')'), ")".into(), start, i + 1, depth));
                i += 1;
            }
            ',' | '.' | ';' | '[' | ']' => {
                out.push(tok(K::Punct(c), c.to_string(), start, i + 1, depth));
                i += 1;
            }
            _ => {
                const OPS: &str = "+-*/<>=~!@#%^&|`?:";
                while at(i).is_some_and(|d| OPS.contains(d)) {
                    i += 1;
                }
                if i == start {
                    i += 1;
                }
                let text = sql[pos(start)..pos(i)].to_string();
                let k = match text.as_str() {
                    "*" => K::Punct('*'),
                    "=" => K::Punct('='),
                    _ => K::Op,
                };
                out.push(tok(k, text, start, i, depth));
            }
        }
    }
    Ok(out)
}

// ---- the analysis ---------------------------------------------------------

/// Raw: the client's text, values may still be `$n`. Plan: the text the
/// executor runs; literals are encrypted.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Raw,
    Plan,
}

#[derive(Default, Debug)]
struct Analysis {
    /// (byte range, replacement).
    edits: Vec<(usize, usize, String)>,
    /// (column, plaintext literal token index): values to encrypt.
    encrypt: Vec<(Col, usize)>,
    vaults: BTreeSet<String>,
    declare: Option<(String, Vec<Col>)>,
    forget: Vec<String>,
}

fn refuse(col: &str, why: &str) -> String {
    format!(
        "0A000: column \"{col}\" is encrypted: {why}. An encrypted column can only be read whole (in a select list or \
         RETURNING) or written whole (a literal in INSERT … VALUES or UPDATE … SET); it cannot be filtered, sorted, \
         grouped, joined on, compared, indexed or computed with"
    )
}

/// Keywords that end a select list or a `RETURNING` list.
const LIST_END: &[&str] = &[
    "FROM", "INTO", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW", "UNION", "INTERSECT", "EXCEPT",
    "ON", "RETURNING",
];

fn is_list_end(t: &Tok) -> bool {
    t.k == K::Word && LIST_END.iter().any(|k| t.text.eq_ignore_ascii_case(k)) || t.is(';')
}

/// Whether the reference at `i` (a name, or a `*`) is a whole item of the
/// depth-0 list that starts after `head` (its keyword) and ends at `end`:
/// `Ok(Some(alias))` when it carries one.
fn whole_item(t: &[Tok], i: usize, head: usize, end: usize) -> Result<Option<String>, ()> {
    if !(head < i && i < end) || t[i].depth != 0 {
        return Err(());
    }
    // `q.col` / `q.*`: the item starts at the qualifier.
    let start = if i >= 2 && t[i - 1].is('.') && matches!(t[i - 2].k, K::Word | K::QWord) { i - 2 } else { i };
    let before = &t[start - 1];
    if !(start - 1 == head || before.is(',') || before.kw("ALL")) {
        return Err(());
    }
    let mut j = i + 1;
    let mut alias = None;
    if j < end && t[j].kw("AS") {
        alias = t.get(j + 1).and_then(Tok::name);
        j += 2;
    } else if j < end && matches!(t[j].k, K::Word | K::QWord) && !is_list_end(&t[j]) {
        alias = t[j].name();
        j += 1;
    }
    if j == end || t.get(j).is_some_and(|x| x.is(',') && x.depth == 0) {
        Ok(alias)
    } else {
        Err(())
    }
}

/// The first depth-0 token at or after `from` that ends a list.
fn list_end(t: &[Tok], from: usize) -> usize {
    (from..t.len()).find(|&j| t[j].depth == 0 && is_list_end(&t[j])).unwrap_or(t.len())
}

fn analyze(all: &[Col], sql: &str, mode: Mode, database: Option<&Database>) -> Result<Option<Analysis>, String> {
    let t = lex(sql)?;
    let Some(first) = t.first() else { return Ok(None) };
    let mut an = Analysis::default();

    // A declaration: CREATE [..] TABLE with ENCRYPTED WITH VAULT.
    if first.kw("CREATE") && t.iter().any(|x| x.kw("ENCRYPTED")) {
        return declare(&t, sql, mode, database).map(Some);
    }
    if t.iter().any(|x| x.kw("ENCRYPTED")) && t.iter().any(|x| x.kw("VAULT")) && (first.kw("ALTER")) {
        return Err("0A000: a column is declared ENCRYPTED when its table is created (CREATE TABLE … ENCRYPTED WITH VAULT v KEY 'k'); ALTER TABLE cannot add one".into());
    }

    // The tables with encrypted columns this statement names.
    let named: Vec<&Col> = all
        .iter()
        .filter(|c| t.iter().any(|x| match x.k {
            K::Word => x.text.eq_ignore_ascii_case(&c.table),
            K::QWord => x.text == c.table,
            _ => false,
        }))
        .collect();
    if named.is_empty() {
        return Ok(None);
    }
    let tables: BTreeSet<String> = named.iter().map(|c| c.table.to_ascii_lowercase()).collect();
    let col_of = |name: &str| named.iter().find(|c| c.column.eq_ignore_ascii_case(name)).copied();
    let head = first.text.to_ascii_uppercase();

    // References: a name that is one of their encrypted columns (not an
    // alias being defined), and a `*` that selects every column.
    let refs: Vec<usize> = (0..t.len())
        .filter(|&i| {
            let is_col = t[i].name().is_some_and(|n| col_of(&n).is_some()) && !(i > 0 && t[i - 1].kw("AS"));
            let star = t[i].is('*')
                && i > 0
                && (t[i - 1].kw("SELECT") || t[i - 1].is(',') || t[i - 1].kw("ALL") || t[i - 1].kw("DISTINCT") || t[i - 1].is('.') || t[i - 1].kw("RETURNING"));
            is_col || star
        })
        .collect();
    let label = |i: usize| -> String {
        match t[i].name().and_then(|n| col_of(&n)) {
            Some(c) => format!("{}.{}", c.table, c.column),
            None => format!("{}.{}", named[0].table, named[0].column),
        }
    };

    match head.as_str() {
        "COPY" => return Err(refuse(&format!("{}.{}", named[0].table, named[0].column), "COPY moves values unencrypted")),
        "DROP" | "TRUNCATE" => {
            if head == "DROP" && t.get(1).is_some_and(|x| x.kw("TABLE")) {
                an.forget = tables.iter().cloned().collect();
            }
            return Ok(Some(an));
        }
        "ALTER" if t.iter().any(|x| x.kw("RENAME")) => {
            return Err(format!(
                "0A000: table \"{}\" has encrypted columns, which are bound to its name and theirs: it and they cannot be renamed",
                named[0].table
            ));
        }
        _ => {}
    }

    // Where whole references are allowed, by statement.
    let mut allowed: BTreeSet<usize> = BTreeSet::new();
    let mut aliases: Vec<(String, usize)> = Vec::new();
    let whole = |i: usize, head_at: usize, end: usize, allowed: &mut BTreeSet<usize>, aliases: &mut Vec<(String, usize)>| {
        if let Ok(alias) = whole_item(&t, i, head_at, end) {
            allowed.insert(i);
            if let Some(a) = alias {
                aliases.push((a, i));
            }
        }
    };
    match head.as_str() {
        "SELECT" | "WITH" => {
            let main = (0..t.len()).find(|&j| t[j].depth == 0 && t[j].kw("SELECT"));
            let set_op = t.iter().any(|x| x.depth == 0 && (x.kw("UNION") || x.kw("INTERSECT") || x.kw("EXCEPT")));
            let into = t.iter().any(|x| x.depth == 0 && x.kw("INTO"));
            if let Some(m) = main {
                let distinct = t.get(m + 1).is_some_and(|x| x.kw("DISTINCT"));
                if !set_op && !into && !distinct {
                    let end = list_end(&t, m + 1);
                    for &i in &refs {
                        whole(i, m, end, &mut allowed, &mut aliases);
                    }
                }
                // ORDER BY 2 / GROUP BY 1 over a list holding one.
                if !allowed.is_empty() {
                    for j in 0..t.len() {
                        if t[j].depth == 0 && t[j].kw("BY") && t.get(j + 1).is_some_and(|x| x.k == K::Num) {
                            return Err(refuse(&label(*allowed.iter().next().unwrap()), "a position in ORDER BY or GROUP BY may name it"));
                        }
                    }
                }
            }
        }
        "INSERT" => {
            // INSERT INTO name [AS a] [(cols)] VALUES … | SELECT … | DEFAULT VALUES
            let target = t.iter().position(|x| x.kw("INTO")).map(|p| p + 1);
            let target_name = target.and_then(|p| t.get(p)).and_then(Tok::name);
            let target_enc: Vec<&Col> = named.iter().copied().filter(|c| Some(c.table.to_ascii_lowercase()) == target_name).collect();
            let mut p = target.unwrap_or(0) + 1;
            if t.get(p).is_some_and(|x| x.kw("AS")) {
                p += 2;
            }
            let mut columns: Option<Vec<String>> = None;
            if t.get(p).is_some_and(|x| x.is('(') && x.depth == 0) {
                let close = (p + 1..t.len()).find(|&j| t[j].is(')') && t[j].depth == 0).unwrap_or(t.len());
                let names: Vec<String> = (p + 1..close).filter_map(|j| t[j].name()).collect();
                for j in p + 1..close {
                    if refs.contains(&j) {
                        allowed.insert(j);
                    }
                }
                columns = Some(names);
                p = close + 1;
            }
            if !target_enc.is_empty() {
                let positions: Vec<(usize, &Col)> = match &columns {
                    Some(names) => names
                        .iter()
                        .enumerate()
                        .filter_map(|(k, n)| target_enc.iter().find(|c| c.column.eq_ignore_ascii_case(n)).map(|c| (k, *c)))
                        .collect(),
                    None if t.get(p).is_some_and(|x| x.kw("DEFAULT")) => Vec::new(),
                    None => {
                        return Err(format!(
                            "0A000: name the columns when inserting into \"{}\": it has encrypted columns, whose values are encrypted by position",
                            target_enc[0].table
                        ))
                    }
                };
                if !positions.is_empty() {
                    if !t.get(p).is_some_and(|x| x.kw("VALUES")) {
                        return Err(refuse(&format!("{}.{}", positions[0].1.table, positions[0].1.column), "only INSERT … VALUES with literals can write it"));
                    }
                    // Each tuple: `( v, v, … )` at depth 0, comma separated.
                    let mut j = p + 1;
                    while j < t.len() && t[j].is('(') && t[j].depth == 0 {
                        let close = (j + 1..t.len()).find(|&x| t[x].is(')') && t[x].depth == 0).ok_or("42601: a VALUES tuple is not closed")?;
                        let mut items: Vec<(usize, usize)> = Vec::new();
                        let mut s = j + 1;
                        for x in j + 1..=close {
                            if x == close || (t[x].is(',') && t[x].depth == 1) {
                                items.push((s, x));
                                s = x + 1;
                            }
                        }
                        for (k, c) in &positions {
                            let Some(&(a, b)) = items.get(*k) else { return Err("42601: a VALUES tuple has fewer values than columns".into()) };
                            value(&t, a, b, c, mode, &mut an)?;
                        }
                        j = close + 1;
                        if t.get(j).is_some_and(|x| x.is(',') && x.depth == 0) {
                            j += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            if let Some(r) = t.iter().position(|x| x.depth == 0 && x.kw("RETURNING")) {
                let end = list_end(&t, r + 1);
                for &i in &refs {
                    whole(i, r, end, &mut allowed, &mut aliases);
                }
            }
        }
        "UPDATE" => {
            if let Some(set) = t.iter().position(|x| x.depth == 0 && x.kw("SET")) {
                let end = (set + 1..t.len())
                    .find(|&j| t[j].depth == 0 && (t[j].kw("FROM") || t[j].kw("WHERE") || t[j].kw("RETURNING") || t[j].is(';')))
                    .unwrap_or(t.len());
                // Assignments: name = value, at depth 0.
                let mut s = set + 1;
                for x in set + 1..=end {
                    if x == end || (t[x].is(',') && t[x].depth == 0) {
                        if s + 1 < x && refs.contains(&s) && t[s + 1].is('=') {
                            if let Some(c) = t[s].name().and_then(|n| col_of(&n)) {
                                allowed.insert(s);
                                value(&t, s + 2, x, c, mode, &mut an)?;
                            }
                        }
                        s = x + 1;
                    }
                }
            }
            if let Some(r) = t.iter().position(|x| x.depth == 0 && x.kw("RETURNING")) {
                let end = list_end(&t, r + 1);
                for &i in &refs {
                    whole(i, r, end, &mut allowed, &mut aliases);
                }
            }
        }
        "DELETE" => {
            if let Some(r) = t.iter().position(|x| x.depth == 0 && x.kw("RETURNING")) {
                let end = list_end(&t, r + 1);
                for &i in &refs {
                    whole(i, r, end, &mut allowed, &mut aliases);
                }
            }
        }
        _ => {}
    }
    for &i in &refs {
        if !allowed.contains(&i) {
            let why = if t[i].is('*') { "a * here would carry its ciphertext into a computation" } else { "it is named here" };
            return Err(refuse(&label(i), why));
        }
    }
    // An alias of one, used again (ORDER BY its alias, …).
    for (alias, def) in &aliases {
        let alias_at = |j: usize| t[j].name().as_deref() == Some(alias.as_str()) && j != *def + 1 && j != *def + 2;
        if let Some(_j) = (0..t.len()).find(|&j| alias_at(j) && !(j > 0 && t[j - 1].kw("AS"))) {
            return Err(refuse(&label(*def), &format!("its alias \"{alias}\" is used again")));
        }
    }
    if mode == Mode::Plan {
        for (c, i) in std::mem::take(&mut an.encrypt) {
            an.vaults.insert(c.vault.clone());
            let plain = t[i].text.clone();
            if let Some(n) = c.max_len {
                if plain.chars().count() > n as usize {
                    return Err(format!("22001: value too long for type character varying({n})"));
                }
            }
            let db = database.ok_or("XX000: no database to encrypt with")?;
            let cell = seal_cell(db, &c, plain.as_bytes())?;
            an.edits.push((t[i].start, t[i].end, format!("'{cell}'")));
        }
    }
    Ok(Some(an))
}

/// The value tokens `a..b` written for encrypted column `c`: one literal or
/// `NULL` (or, in the client's text, a `$n` the server binds).
fn value(t: &[Tok], a: usize, b: usize, c: &Col, mode: Mode, an: &mut Analysis) -> Result<(), String> {
    let label = format!("{}.{}", c.table, c.column);
    if b != a + 1 {
        return Err(refuse(&label, "its value must be a single literal, not an expression"));
    }
    let v = &t[a];
    // E'…' and the like: a prefix word glued to the string.
    if v.k == K::Str && a > 0 && t[a - 1].end == v.start && t[a - 1].k == K::Word {
        return Err(refuse(&label, "its value must be a plain '…' literal"));
    }
    match (&v.k, mode) {
        (K::Str | K::Num, _) => {
            an.encrypt.push((c.clone(), a));
            Ok(())
        }
        (K::Word, _) if v.kw("NULL") => Ok(()),
        (K::Param, Mode::Raw) => Ok(()),
        _ => Err(refuse(&label, "its value must be a literal or NULL")),
    }
}

/// `CREATE [TEMP…] [LEDGER] TABLE name ( … col TYPE ENCRYPTED WITH VAULT v
/// KEY 'k' … ) …`: the columns it declares, and the text without the
/// clause. A constraint on such a column, or a type that is not text, is
/// refused.
fn declare(t: &[Tok], sql: &str, mode: Mode, database: Option<&Database>) -> Result<Analysis, String> {
    let table_at = t.iter().position(|x| x.kw("TABLE")).ok_or("0A000: ENCRYPTED WITH VAULT declares a column of a new table: CREATE TABLE …")?;
    if t[..table_at].iter().any(|x| x.kw("TEMP") || x.kw("TEMPORARY") || x.kw("UNLOGGED")) {
        return Err("0A000: a temporary or unlogged table cannot have encrypted columns".into());
    }
    let mut p = table_at + 1;
    if t.get(p).is_some_and(|x| x.kw("IF")) {
        return Err("0A000: CREATE TABLE IF NOT EXISTS cannot declare encrypted columns: create the table once, without IF NOT EXISTS".into());
    }
    let table = t.get(p).and_then(|x| matches!(x.k, K::Word | K::QWord).then(|| x.text.clone())).ok_or("42601: the table needs a name")?;
    if t.get(p + 1).is_some_and(|x| x.is('.')) {
        return Err("0A000: a table with encrypted columns is named without a schema".into());
    }
    p += 1;
    if !t.get(p).is_some_and(|x| x.is('(') && x.depth == 0) {
        return Err("0A000: CREATE TABLE … AS cannot declare encrypted columns".into());
    }
    let close = (p + 1..t.len()).find(|&j| t[j].is(')') && t[j].depth == 0).ok_or("42601: the column list is not closed")?;
    // Definitions: split at depth-1 commas.
    let mut defs: Vec<(usize, usize)> = Vec::new();
    let mut s = p + 1;
    for x in p + 1..=close {
        if x == close || (t[x].is(',') && t[x].depth == 1) {
            defs.push((s, x));
            s = x + 1;
        }
    }
    let mut out = Analysis::default();
    let mut declared: Vec<Col> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for &(a, b) in &defs {
        let Some(enc) = (a..b).find(|&j| t[j].kw("ENCRYPTED")) else { continue };
        let column = t[a].name().map(|_| t[a].text.clone()).ok_or("42601: a column needs a name")?;
        let label = format!("{table}.{column}");
        // ENCRYPTED WITH VAULT v KEY 'k'
        let ok = t.get(enc + 1).is_some_and(|x| x.kw("WITH"))
            && t.get(enc + 2).is_some_and(|x| x.kw("VAULT"))
            && t.get(enc + 3).is_some_and(|x| matches!(x.k, K::Word | K::QWord))
            && t.get(enc + 4).is_some_and(|x| x.kw("KEY"))
            && t.get(enc + 5).is_some_and(|x| x.k == K::Str);
        if !ok {
            return Err(format!("42601: column {column}: ENCRYPTED WITH VAULT <vault> KEY '<key>'"));
        }
        let (vault, key) = (t[enc + 3].text.clone(), t[enc + 5].text.clone());
        // The type: TEXT, VARCHAR[(n)] or CHARACTER VARYING[(n)].
        let ty: Vec<&Tok> = t[a + 1..enc].iter().collect();
        let words: Vec<String> = ty.iter().filter(|x| x.k == K::Word).map(|x| x.text.to_ascii_uppercase()).collect();
        let max_len = ty.iter().find(|x| x.k == K::Num).and_then(|x| x.text.parse::<u32>().ok());
        let type_ok = match words.as_slice() {
            [w] if w == "TEXT" => true,
            [w] if w == "VARCHAR" => true,
            [a, b] if a == "CHARACTER" && b == "VARYING" => true,
            _ => false,
        };
        let rest: Vec<String> = t[enc + 6..b].iter().filter(|x| x.k == K::Word).map(|x| x.text.to_ascii_uppercase()).collect();
        let rest_ok = rest.is_empty() || rest == ["NOT", "NULL"] || rest == ["NULL"];
        let before_ok = words.iter().all(|w| matches!(w.as_str(), "TEXT" | "VARCHAR" | "CHARACTER" | "VARYING"));
        if !type_ok || !before_ok {
            return Err(format!(
                "0A000: column {label}: an encrypted column is TEXT or VARCHAR(n), declared as `{column} TEXT ENCRYPTED WITH VAULT v KEY 'k'`: its ciphertext is text, and a constraint before ENCRYPTED is not taken"
            ));
        }
        if !rest_ok {
            return Err(format!(
                "0A000: column {label}: an encrypted column takes NOT NULL only, not {}: a key, a unique or check constraint, a default or a reference would compare or compute on its ciphertext",
                rest.join(" ")
            ));
        }
        let not_null = rest.first().is_some_and(|w| w == "NOT");
        out.edits.push((t[a].start, t[b - 1].end, format!("{} TEXT{}", &sql[t[a].start..t[a].end], if not_null { " NOT NULL" } else { "" })));
        names.push(column.to_ascii_lowercase());
        declared.push(Col { id: 0, table: table.clone(), column, vault, key, wrapped: Vec::new(), max_len });
    }
    // A table constraint naming one.
    for &(a, b) in &defs {
        let first = &t[a];
        if ["PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT", "EXCLUDE"].iter().any(|k| first.kw(k)) {
            if let Some(j) = (a..b).find(|&j| t[j].name().is_some_and(|n| names.contains(&n))) {
                return Err(refuse(&format!("{table}.{}", t[j].text), "a table constraint names it"));
            }
        }
    }
    // Nothing else in the statement may name one (a clause after the list).
    for j in close + 1..t.len() {
        if t[j].name().is_some_and(|n| names.contains(&n)) {
            return Err(refuse(&format!("{table}.{}", t[j].text), "a clause after the column list names it"));
        }
    }
    if mode == Mode::Plan {
        let db = database.ok_or("XX000: no database")?;
        for c in &mut declared {
            let mut id = [0u8; 16];
            crypto::fill_random(&mut id)?;
            c.id = u128::from_le_bytes(id);
            let dek = crypto::new_dek()?;
            c.wrapped = wrap_dek(db, c, &dek[..])?;
            DEKS.get_or_init(|| Mutex::new(HashMap::new())).lock().insert(c.id, dek);
            out.vaults.insert(c.vault.clone());
        }
    }
    out.declare = Some((table, declared));
    Ok(out)
}

// ---- keys and cells -------------------------------------------------------

fn dek_aad(id: u128) -> Vec<u8> {
    let mut a = b"rdb-coldek-v1".to_vec();
    a.extend_from_slice(&id.to_le_bytes());
    a
}

fn cell_aad(id: u128) -> Vec<u8> {
    let mut a = b"rdb-col-v1".to_vec();
    a.extend_from_slice(&id.to_le_bytes());
    a
}

fn vault_schema(db: &Database, vault: &str) -> Result<rasterizeddb_core::core::row::schema::TableSchema, String> {
    let r = db.relation(vault).ok_or_else(|| format!("42704: no vault \"{vault}\""))?;
    let schema = r.schema().clone();
    if schema.special != SpecialTable::Vault {
        return Err(format!("42809: \"{vault}\" is not a vault"));
    }
    Ok(schema)
}

fn wrap_dek(db: &Database, c: &Col, dek: &[u8]) -> Result<Vec<u8>, String> {
    let g = crate::ledger::guard().ok_or("XX000: the ledger guard is not installed")?;
    crate::vault::key_seal(g, &db.ledger_store(), &vault_schema(db, &c.vault)?, &c.key, dek, &dek_aad(c.id))
}

fn dek(db: &Database, c: &Col) -> Result<crypto::Dek, String> {
    let cache = DEKS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(k) = cache.lock().get(&c.id) {
        return Ok(k.clone());
    }
    let g = crate::ledger::guard().ok_or("XX000: the ledger guard is not installed")?;
    let plain = crate::vault::key_open(g, &db.ledger_store(), &vault_schema(db, &c.vault)?, &c.key, &c.wrapped, &dek_aad(c.id))
        .map_err(|e| format!("could not unwrap the key of encrypted column {}.{}: {e}", c.table, c.column))?;
    if plain.len() != 32 {
        return Err("XX001: an encrypted column's key is not 32 bytes".into());
    }
    let mut k = Zeroizing::new([0u8; 32]);
    k.copy_from_slice(&plain);
    cache.lock().insert(c.id, k.clone());
    Ok(k)
}

fn seal_cell(db: &Database, c: &Col, plain: &[u8]) -> Result<String, String> {
    let k = dek(db, c)?;
    let sealed = crypto::seal(&k, plain, &cell_aad(c.id))?;
    Ok(format!(
        "{}{:032x}:{}",
        std::str::from_utf8(MARK).unwrap(),
        c.id,
        base64::engine::general_purpose::STANDARD.encode(sealed)
    ))
}

/// `rdbenc1:<id>:<base64>` → (id, sealed bytes).
fn parse_cell(cell: &[u8]) -> Option<(u128, Vec<u8>)> {
    let rest = cell.strip_prefix(MARK)?;
    let text = std::str::from_utf8(rest).ok()?;
    let (id, b64) = text.split_once(':')?;
    if id.len() != 32 {
        return None;
    }
    let id = u128::from_str_radix(id, 16).ok()?;
    let sealed = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    Some((id, sealed))
}

// ---- the hooks ------------------------------------------------------------

struct Cipher;

fn apply(sql: &str, mut edits: Vec<(usize, usize, String)>) -> String {
    edits.sort_by_key(|e| std::cmp::Reverse(e.0));
    let mut out = sql.to_string();
    for (a, b, text) in edits {
        out.replace_range(a..b, &text);
    }
    out
}

impl ColumnCrypto for Cipher {
    fn active(&self, store: &str) -> bool {
        cols(store).map_or(true, |c| !c.is_empty())
    }

    fn touches(&self, store: &str, sql: &str) -> bool {
        let low = sql.to_ascii_lowercase();
        cols(store).map_or(true, |c| c.iter().any(|c| low.contains(&c.table.to_ascii_lowercase())))
    }

    fn guard(&self, store: &str, sql: &str) -> Result<(), String> {
        analyze(&cols(store)?, sql, Mode::Raw, None).map(drop)
    }

    fn plan(&self, database: &Database, sql: &str) -> Result<Option<Plan>, String> {
        let store = database.ledger_store();
        let Some(an) = analyze(&cols(&store)?, sql, Mode::Plan, Some(database))? else { return Ok(None) };
        let declares = an.declare.is_some();
        let pending = match (an.declare, an.forget.is_empty()) {
            (Some((table, cols)), _) => Some(Pending::Declare(table, cols)),
            (None, false) => Some(Pending::Forget(an.forget)),
            _ => None,
        };
        let token = pending.map(|p| {
            let t = TOKENS.fetch_add(1, Ordering::Relaxed);
            PENDING.get_or_init(|| Mutex::new(HashMap::new())).lock().insert(t, (store.clone(), p));
            t
        });
        Ok(Some(Plan {
            sql: (!an.edits.is_empty()).then(|| apply(sql, an.edits)),
            vaults: an.vaults.into_iter().collect(),
            declares,
            token,
        }))
    }

    fn finish(&self, store: &str, token: u64, ok: bool) {
        let Some((s, p)) = PENDING.get_or_init(|| Mutex::new(HashMap::new())).lock().remove(&token) else { return };
        if !ok || s != store {
            return;
        }
        let mut all = match cols(store) {
            Ok(c) => c,
            Err(e) => {
                log::error!("encrypted columns: {e}");
                return;
            }
        };
        match p {
            Pending::Declare(table, new) => {
                all.retain(|c| !c.table.eq_ignore_ascii_case(&table));
                all.extend(new);
            }
            Pending::Forget(tables) => all.retain(|c| !tables.contains(&c.table.to_ascii_lowercase())),
        }
        if let Err(e) = set_cols(store, all) {
            log::error!("encrypted columns: could not record a change: {e}");
        }
    }

    fn vault_of(&self, store: &str, cell: &[u8]) -> Option<String> {
        let (id, _) = parse_cell(cell)?;
        cols(store).ok()?.into_iter().find(|c| c.id == id).map(|c| c.vault)
    }

    fn decrypt(&self, database: &Database, cell: &[u8]) -> Result<Vec<u8>, String> {
        let (id, sealed) = parse_cell(cell).ok_or("XX001: an encrypted value is damaged")?;
        let c = cols(&database.ledger_store())?
            .into_iter()
            .find(|c| c.id == id)
            .ok_or("XX001: an encrypted value names no known column")?;
        let k = dek(database, &c)?;
        crypto::open(&k, &sealed, &cell_aad(id))
            .map(|p| p.to_vec())
            .map_err(|_| format!("XX001: an encrypted value of {}.{} does not verify: it was altered, or moved from another column", c.table, c.column))
    }

    fn show_lines(&self, store: &str) -> Vec<String> {
        show_lines(store)
    }
}

/// `SHOW LEDGER`'s lines about encrypted columns.
pub fn show_lines(store: &str) -> Vec<String> {
    cols(store)
        .unwrap_or_default()
        .iter()
        .map(|c| {
            format!(
                "encrypted column {}.{}: vault {} key '{}'{}",
                c.table,
                c.column,
                c.vault,
                c.key,
                c.max_len.map_or(String::new(), |n| format!(", varchar({n})"))
            )
        })
        .collect()
}

/// Forgets every unwrapped column key and every registry read, as a new
/// process would have neither: for tests that reopen a database.
#[doc(hidden)]
pub fn forget_cached_keys() {
    if let Some(d) = DEKS.get() {
        d.lock().clear();
    }
    if let Some(s) = STORES.get() {
        s.lock().clear();
    }
}

/// Installs the column cipher. With the ledger guard (`ledger::install`)
/// first: the cipher reads vault policies through it.
pub fn install() -> Result<(), String> {
    rasterizeddb_core::core::column_crypto::install(Box::new(Cipher))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(table: &str, column: &str) -> Col {
        Col { id: 7, table: table.into(), column: column.into(), vault: "hr".into(), key: "pii".into(), wrapped: vec![], max_len: None }
    }

    fn check(sql: &str) -> Result<(), String> {
        analyze(&[col("patients", "ssn")], sql, Mode::Raw, None).map(drop)
    }

    #[test]
    fn whole_reads_and_literal_writes_pass() {
        for ok in [
            "SELECT ssn FROM patients",
            "SELECT id, ssn FROM patients WHERE id = 3 ORDER BY id LIMIT 5",
            "SELECT p.ssn AS s, id FROM patients p",
            "SELECT * FROM patients WHERE id > 1",
            "SELECT count(*) FROM patients",
            "SELECT id FROM patients WHERE name = 'ssn'",
            "INSERT INTO patients (id, ssn) VALUES (1, '123-45-6789'), (2, NULL)",
            "INSERT INTO patients (id, ssn) VALUES ($1, $2)",
            "INSERT INTO patients (id, ssn) VALUES (1, 'x') RETURNING ssn",
            "INSERT INTO patients DEFAULT VALUES",
            "UPDATE patients SET ssn = 'new', name = upper(name) WHERE id = 1",
            "UPDATE patients SET ssn = NULL WHERE id = 1 RETURNING id, ssn",
            "DELETE FROM patients WHERE id = 1 RETURNING ssn",
            "DROP TABLE patients",
            "TRUNCATE patients",
            "SELECT id FROM other WHERE x = 1",
            "GRANT SELECT ON patients TO app",
        ] {
            check(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
    }

    #[test]
    fn anything_that_would_compute_on_ciphertext_is_refused() {
        for bad in [
            "SELECT id FROM patients WHERE ssn = '123'",
            "SELECT id FROM patients WHERE ssn LIKE '1%'",
            "SELECT id FROM patients WHERE ssn IS NOT NULL AND ssn <> ''",
            "SELECT id FROM patients ORDER BY ssn",
            "SELECT ssn, count(*) FROM patients GROUP BY ssn",
            "SELECT DISTINCT ssn FROM patients",
            "SELECT DISTINCT * FROM patients",
            "SELECT upper(ssn) FROM patients",
            "SELECT ssn || 'x' FROM patients",
            "SELECT ssn::int FROM patients",
            "SELECT count(DISTINCT ssn) FROM patients",
            "SELECT max(ssn) FROM patients",
            "SELECT ssn FROM patients ORDER BY 1",
            "SELECT ssn AS s FROM patients ORDER BY s",
            "SELECT a.id FROM patients a JOIN people b ON a.ssn = b.ssn",
            "SELECT id FROM people WHERE ssn IN (SELECT ssn FROM patients)",
            "SELECT (SELECT ssn FROM patients LIMIT 1)",
            "SELECT ssn FROM patients UNION SELECT ssn FROM patients",
            "SELECT * FROM patients UNION SELECT * FROM patients",
            "SELECT row_number() OVER (PARTITION BY ssn) FROM patients",
            "SELECT id FROM patients HAVING max(ssn) > '1'",
            "WITH c AS (SELECT ssn FROM patients) SELECT * FROM c",
            "SELECT * INTO copy FROM patients",
            "CREATE TABLE copy AS SELECT * FROM patients",
            "CREATE INDEX ix ON patients (ssn)",
            "INSERT INTO patients (id, ssn) VALUES (1, upper('x'))",
            "INSERT INTO patients (id, ssn) VALUES (1, 'x' || 'y')",
            "INSERT INTO patients (id, ssn) VALUES (1, E'x')",
            "INSERT INTO patients (id, ssn) SELECT id, ssn FROM people",
            "INSERT INTO patients VALUES (1, 'x')",
            "INSERT INTO patients (id, ssn) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET ssn = EXCLUDED.ssn",
            "UPDATE patients SET ssn = upper(ssn)",
            "UPDATE patients SET name = 'a' WHERE ssn = '1'",
            "UPDATE patients SET ssn = other.x FROM other",
            "DELETE FROM patients WHERE ssn = '1'",
            "COPY patients FROM STDIN",
            "ALTER TABLE patients RENAME TO p2",
            "ALTER TABLE patients DROP COLUMN ssn",
            "EXPLAIN SELECT id FROM patients WHERE ssn = 'x'",
            "SELECT to_json(p.*) FROM patients p",
        ] {
            let e = check(bad).expect_err(bad);
            assert!(e.starts_with("0A000: "), "{bad}: {e}");
        }
    }

    #[test]
    fn a_declaration_is_rewritten_to_text_and_its_constraints_refused() {
        let an = analyze(&[], "CREATE TABLE p (id BIGINT PRIMARY KEY, ssn VARCHAR(11) ENCRYPTED WITH VAULT hr KEY 'pii' NOT NULL, n TEXT)", Mode::Raw, None)
            .unwrap()
            .unwrap();
        let (table, cols) = an.declare.unwrap();
        assert_eq!((table.as_str(), cols.len(), cols[0].max_len), ("p", 1, Some(11)));
        assert_eq!(
            apply("CREATE TABLE p (id BIGINT PRIMARY KEY, ssn VARCHAR(11) ENCRYPTED WITH VAULT hr KEY 'pii' NOT NULL, n TEXT)", an.edits),
            "CREATE TABLE p (id BIGINT PRIMARY KEY, ssn TEXT NOT NULL, n TEXT)"
        );
        for bad in [
            "CREATE TABLE p (ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii' PRIMARY KEY)",
            "CREATE TABLE p (ssn TEXT UNIQUE ENCRYPTED WITH VAULT hr KEY 'pii')",
            "CREATE TABLE p (ssn INT ENCRYPTED WITH VAULT hr KEY 'pii')",
            "CREATE TABLE p (ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii' DEFAULT 'x')",
            "CREATE TABLE p (ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii', UNIQUE (ssn))",
            "CREATE TEMP TABLE p (ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii')",
            "CREATE TABLE IF NOT EXISTS p (ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii')",
            "CREATE TABLE p (ssn TEXT ENCRYPTED BY hr)",
            "ALTER TABLE p ADD COLUMN ssn TEXT ENCRYPTED WITH VAULT hr KEY 'pii'",
        ] {
            assert!(analyze(&[], bad, Mode::Raw, None).is_err(), "{bad}");
        }
    }

    #[test]
    fn cells_parse_and_strings_are_not_cells() {
        assert!(parse_cell(b"rdbenc1:00000000000000000000000000000007:AAAA").is_some());
        assert!(parse_cell(b"rdbenc1:7:AAAA").is_none());
        assert!(parse_cell(b"plain").is_none());
    }
}
