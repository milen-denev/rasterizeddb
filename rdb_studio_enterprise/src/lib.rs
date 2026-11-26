//! RasterizedDB Studio's LEDGER page (licensed: see LICENSE). It covers the
//! ledger and vault features (docs/ledger-and-vault-plan.md):
//!
//! - **Chain:** entries, head, anchors and location as tiles, the server's
//!   warnings as pills, and VERIFY's findings as a table.
//! - **Ledger tables:** a grid (rows, mode, retention, holds), each table's
//!   actions (verify, history, receipt, retention, legal holds, anchors,
//!   its data), and a form that creates one.
//! - **Vaults:** a grid of secrets with a name filter, a revealed value in a
//!   marked box until HIDE (and never past a tab or vault switch), versions,
//!   the forms that set a secret, use a key, rotate the key and create a
//!   vault, and the audit trail.
//!
//! Studio's GPL side holds the tab, runs statements and supplies the widgets
//! ([`Kit`], drawn with its own theme, grid and confirmation dialog). This
//! crate builds every statement, reads the answers into a [`LedgerPage`] and
//! lays the page out ([`view`]). The server decides who may do what; a
//! refusal is shown as the answer.

use std::collections::BTreeMap;

use iced::widget::{column, row, Space};
use iced::{Element, Fill};

/// Every statement the page runs to load.
pub const SHOW_SQL: &str = "SHOW LEDGER";
pub const VERIFY_SQL: &str = "VERIFY LEDGER";

/// A vault's `LIST`.
pub fn list_sql(vault: &str) -> String {
    format!("VAULT {vault} LIST")
}

/// A vault's `AUDIT`.
pub fn audit_sql(vault: &str) -> String {
    format!("VAULT {vault} AUDIT")
}

/// How many rows a ledger table holds, for its grid.
pub fn count_sql(table: &str) -> String {
    format!("SELECT count(*) FROM {table}")
}

/// `s` as a SQL string literal.
pub fn q(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

// ---------------------------------------------------------------------------
// What the server said, read into structure
// ---------------------------------------------------------------------------

/// The chain line of `SHOW LEDGER`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Chain {
    pub entries: u64,
    pub head: String,
    pub location: String,
}

/// A ledger table or vault, from its `SHOW LEDGER` line.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Special {
    pub name: String,
    pub vault: bool,
    pub retention_s: i64,
    pub drop_idle_s: i64,
    pub hash: String,
    pub versioned: bool,
    pub anchors: Vec<String>,
    pub holds: Vec<String>,
    pub key_provider: Option<String>,
}

/// `SHOW LEDGER`, read.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Shown {
    pub chain: Option<Chain>,
    /// The anchor sinks, and the mode and signing note in parentheses.
    pub anchors: Vec<String>,
    pub anchor_mode: String,
    pub unanchored: String,
    /// `WARNING: …` lines, and `last anchor error`.
    pub warnings: Vec<String>,
    pub specials: Vec<Special>,
    /// Anything not read above, shown as it came.
    pub other: Vec<String>,
}

fn parse_special(name: &str, rest: &str) -> Option<Special> {
    let (vault, rest) = if let Some(r) = rest.strip_prefix("vault id ") {
        (true, r)
    } else {
        (false, rest.strip_prefix("ledger id ")?)
    };
    let mut s = Special { name: name.to_string(), vault, ..Special::default() };
    for part in rest.split(", ").skip(1) {
        if let Some(v) = part.strip_prefix("retention ") {
            s.retention_s = v.trim_end_matches(" s").parse().unwrap_or(0);
        } else if let Some(v) = part.strip_prefix("drop after ") {
            s.drop_idle_s = v.trim_end_matches(" s idle").parse().unwrap_or(0);
        } else if let Some(v) = part.strip_prefix("hash ") {
            s.hash = v.to_string();
        } else if part == "versioned" {
            s.versioned = true;
        } else if let Some(v) = part.strip_prefix("anchors ") {
            s.anchors = v.split(' ').map(str::to_string).collect();
        } else if let Some(v) = part.strip_prefix("holds ") {
            s.holds = v.split(' ').map(str::to_string).collect();
        } else if let Some(v) = part.strip_prefix("key provider ") {
            s.key_provider = Some(v.to_string());
        }
    }
    Some(s)
}

impl Shown {
    pub fn parse(lines: &[String]) -> Shown {
        let mut out = Shown::default();
        for l in lines {
            if let Some(rest) = l.strip_prefix("chain: ") {
                let mut c = Chain::default();
                for part in rest.split(", ") {
                    if let Some(n) = part.strip_suffix(" entries") {
                        c.entries = n.parse().unwrap_or(0);
                    } else if let Some(h) = part.strip_prefix("head ") {
                        c.head = h.to_string();
                    } else if let Some(at) = part.strip_prefix("at ") {
                        c.location = at.to_string();
                    }
                }
                out.chain = Some(c);
            } else if let Some(w) = l.strip_prefix("WARNING: ") {
                out.warnings.push(w.to_string());
            } else if let Some(e) = l.strip_prefix("last anchor error: ") {
                out.warnings.push(format!("last anchor error: {e}"));
            } else if let Some(rest) = l.strip_prefix("anchors: ") {
                if rest != "none" {
                    let (sinks, tail) = rest.split_once(" (").unwrap_or((rest, ""));
                    out.anchors = sinks.split(' ').map(str::to_string).collect();
                    if let Some((mode, un)) = tail.split_once("), unanchored: ") {
                        out.anchor_mode = mode.to_string();
                        out.unanchored = un.to_string();
                    }
                }
            } else if let Some(s) = l.split_once(": ").and_then(|(n, r)| parse_special(n, r)) {
                out.specials.push(s);
            } else {
                out.other.push(l.clone());
            }
        }
        out
    }
}

/// One line of a vault's `LIST`: never a value.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Secret {
    pub name: String,
    pub version: i64,
    pub state: String,
    pub content_type: String,
    pub expires_us: i64,
    pub updated_us: i64,
}

impl Secret {
    pub fn parse(line: &str) -> Option<Secret> {
        let mut w = line.split_whitespace();
        let mut s = Secret {
            name: w.next()?.to_string(),
            version: w.next()?.trim_start_matches('v').parse().ok()?,
            state: w.next()?.to_string(),
            ..Secret::default()
        };
        for kv in w {
            match kv.split_once('=') {
                Some(("type", v)) => s.content_type = v.to_string(),
                Some(("expires_us", v)) => s.expires_us = v.parse().unwrap_or(0),
                Some(("updated_us", v)) => s.updated_us = v.parse().unwrap_or(0),
                _ => {}
            }
        }
        Some(s)
    }

    /// A key used in place (`key:<name>`), not a secret: never revealed.
    pub fn is_key(&self) -> bool {
        self.name.starts_with("key:")
    }
}

/// A status word at the head of a line (`OK`, `WARNING`, `TAMPERED`,
/// `UNKNOWN`, `chained`, `UNCHAINED`), and the rest.
pub fn status_of(line: &str) -> (&str, &str) {
    let line = line.trim_start();
    match line.split_once(char::is_whitespace) {
        Some((w, rest)) if w.chars().all(|c| c.is_ascii_alphabetic()) => (w, rest.trim_start()),
        _ => ("", line),
    }
}

/// The tone a status word is shown in.
pub fn status_tone(status: &str) -> Tone {
    match status {
        "OK" | "chained" | "intact" => Tone::Good,
        "WARNING" => Tone::Warn,
        "TAMPERED" | "UNKNOWN" | "UNCHAINED" | "FAILED" | "REFUSED" => Tone::Bad,
        _ => Tone::Dim,
    }
}

/// Microseconds since the Unix epoch as `YYYY-MM-DD HH:MM:SS` UTC.
pub fn when(us: i64) -> String {
    if us <= 0 {
        return "—".to_string();
    }
    let secs = us.div_euclid(1_000_000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", rem / 3_600, rem % 3_600 / 60, rem % 60)
}

/// A span in seconds, in the largest unit that divides it.
pub fn span(secs: i64) -> String {
    if secs <= 0 {
        return "—".to_string();
    }
    for (unit, n) in [("year", 366 * 86_400), ("day", 86_400), ("hour", 3_600), ("minute", 60)] {
        if secs % n == 0 {
            let k = secs / n;
            return format!("{k} {unit}{}", if k == 1 { "" } else { "s" });
        }
    }
    format!("{secs} s")
}

/// The middle of a long value cut out: `edbb21ed…87502c`.
pub fn elide_middle(s: &str, keep: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= keep * 2 + 1 {
        return s.to_string();
    }
    format!("{}…{}", chars[..keep].iter().collect::<String>(), chars[chars.len() - keep..].iter().collect::<String>())
}

// ---------------------------------------------------------------------------
// The page's state
// ---------------------------------------------------------------------------

/// One vault on the page.
#[derive(Clone, Debug, Default)]
pub struct VaultView {
    pub name: String,
    /// `LIST`'s lines: name, version, state, type, expiry. Never a value.
    pub secrets: Vec<String>,
    pub audit: Option<Vec<String>>,
}

/// What the page shows.
#[derive(Clone, Debug, Default)]
pub struct LedgerPage {
    pub show: Vec<String>,
    pub verify: Option<Vec<String>>,
    pub vaults: Vec<VaultView>,
    /// Each ledger table's row count, where `count(*)` answered.
    pub counts: BTreeMap<String, i64>,
    /// Why the page could not load: no ledger support on the server, or no
    /// permission.
    pub error: Option<String>,
    /// The page's text inputs, by key.
    pub inputs: BTreeMap<String, String>,
    /// The last action's heading (its verb, never the statement) and answer.
    pub last: Option<(String, Result<Vec<String>, String>)>,
    /// An answer that belongs beside what asked for it: a secret's versions,
    /// a row's history or receipt, a key's output. (heading, lines).
    pub detail: Option<(String, Vec<String>)>,
    /// A value revealed on request: (vault, name, value). Cleared by HIDE, a
    /// load, a vault switch and leaving the tab.
    pub revealed: Option<(String, String, String)>,
}

/// The verb of a statement, for the answer's heading: never the statement,
/// which can carry a secret.
fn label(sql: &str) -> String {
    let w: Vec<&str> = sql.split_whitespace().take(3).collect();
    w.join(" ")
}

/// Input keys the page itself reads.
pub const VIEW: &str = "__view";
pub const VAULT: &str = "__vault";
pub const TABLE: &str = "__table";
pub const HIDE: &str = "__hide";
pub const CLEAR: &str = "__clear";
pub const ADD_COLUMN: &str = "__add_column";
pub const DROP_COLUMN: &str = "__drop_column";

impl LedgerPage {
    /// The vaults `SHOW LEDGER` names (its lines `<name>: vault id …`).
    pub fn vault_names(show: &[String]) -> Vec<String> {
        Self::named(show, "vault ")
    }

    /// The ledger tables `SHOW LEDGER` names.
    pub fn ledger_names(show: &[String]) -> Vec<String> {
        Self::named(show, "ledger ")
    }

    fn named(show: &[String], kind: &str) -> Vec<String> {
        show.iter()
            .filter_map(|l| {
                let (name, rest) = l.split_once(": ")?;
                rest.starts_with(kind).then(|| name.to_string())
            })
            .collect()
    }

    /// Whether the last `VERIFY` found everything intact.
    pub fn verified_intact(&self) -> Option<bool> {
        self.verify.as_ref().map(|v| !v.iter().any(|l| l.starts_with("TAMPERED") || l.starts_with("UNKNOWN")))
    }

    /// Keeps what a reload must not lose: the inputs, the last answer and
    /// the detail panel. A revealed value is dropped.
    pub fn carry_over(&mut self, old: LedgerPage) {
        self.inputs = old.inputs;
        self.last = old.last;
        self.detail = old.detail;
        if self.verify.is_none() {
            self.verify = old.verify;
        }
        for v in &mut self.vaults {
            if let Some(o) = old.vaults.iter().find(|o| o.name == v.name) {
                v.audit = o.audit.clone();
            }
        }
        self.revealed = None;
    }

    /// Records an action's answer. A value that is a secret (`GET SECRET`,
    /// `DECRYPT`) goes to the revealed box, never to the answer lines; an
    /// answer about one thing (versions, history, a receipt, a key's output)
    /// goes to the detail panel.
    pub fn ran(&mut self, sql: &str, result: Result<Vec<String>, String>) {
        let w: Vec<&str> = sql.split_whitespace().collect();
        let up = |i: usize| w.get(i).map(|s| s.to_ascii_uppercase()).unwrap_or_default();
        let unquote = |i: usize| w.get(i).map(|s| s.trim_matches('\'').to_string()).unwrap_or_default();
        // A value written by a form is not kept in the inputs once sent.
        self.inputs.retain(|k, _| !k.ends_with(":value") && !k.ends_with(":plain"));
        if up(0) == "VAULT" {
            if let Ok(lines) = &result {
                let reveal = match (up(2).as_str(), up(3).as_str()) {
                    ("GET", "SECRET") => Some(unquote(4)),
                    ("DECRYPT", "KEY") => Some(format!("decrypted with {}", unquote(4))),
                    _ => None,
                };
                if let Some(name) = reveal {
                    self.revealed = Some((w[1].to_string(), name, lines.join("\n")));
                    self.last = None;
                    return;
                }
                let about = match (up(2).as_str(), up(3).as_str()) {
                    ("VERSIONS", "SECRET") => Some(format!("VERSIONS OF {}", unquote(4))),
                    ("ENCRYPT" | "SIGN" | "VERIFY", "KEY") => Some(format!("{} WITH {}", up(2), unquote(4))),
                    ("PUBLIC", "KEY") => Some(format!("PUBLIC KEY {}", unquote(4))),
                    _ => None,
                };
                if let Some(heading) = about {
                    self.detail = Some((heading, lines.clone()));
                    self.last = None;
                    return;
                }
            }
        }
        if up(0) == "SHOW" && up(1) == "LEDGER" && matches!(up(2).as_str(), "HISTORY" | "RECEIPT") {
            if let Ok(lines) = &result {
                self.detail = Some((format!("{} {} {}", up(2), w.get(3).unwrap_or(&""), w.get(4).unwrap_or(&"")), lines.clone()));
                self.last = None;
                return;
            }
        }
        if up(0) == "VERIFY" && up(1) == "LEDGER" {
            if let Ok(lines) = &result {
                self.verify = Some(lines.clone());
                self.last = None;
                return;
            }
        }
        self.last = Some((label(sql), result));
    }

    /// Hides a revealed value.
    pub fn hide(&mut self) {
        self.revealed = None;
    }

    /// An input changed. The page's own keys switch the view, select a vault
    /// or table (hiding a revealed value) and edit the column list.
    pub fn input(&mut self, key: String, value: String) {
        match key.as_str() {
            HIDE => self.hide(),
            CLEAR => {
                self.last = None;
                self.detail = None;
            }
            VAULT | VIEW => {
                self.hide();
                self.detail = None;
                self.inputs.insert(key, value);
            }
            ADD_COLUMN => {
                let n = self.columns();
                self.inputs.insert("new:ledger:ncols".into(), (n + 1).to_string());
            }
            DROP_COLUMN => {
                let (n, at) = (self.columns(), value.parse::<usize>().unwrap_or(usize::MAX));
                if n > 1 && at < n {
                    for i in at..n - 1 {
                        for f in COLUMN_FIELDS {
                            match self.inputs.get(&format!("new:ledger:col:{}:{f}", i + 1)).cloned() {
                                Some(next) => self.inputs.insert(format!("new:ledger:col:{i}:{f}"), next),
                                None => self.inputs.remove(&format!("new:ledger:col:{i}:{f}")),
                            };
                        }
                    }
                    for f in COLUMN_FIELDS {
                        self.inputs.remove(&format!("new:ledger:col:{}:{f}", n - 1));
                    }
                    self.inputs.insert("new:ledger:ncols".into(), (n - 1).to_string());
                }
            }
            // Another type: its size boxes start empty (their placeholders
            // show the usual values).
            // Another type: its size boxes start at the type's usual size
            // (`TYPE\u{1}len` or `TYPE\u{1}precision\u{1}scale`), which the
            // statement then carries, and can be edited or cleared.
            k if k.starts_with("new:ledger:col:") && k.ends_with(":type") => {
                let row = k.trim_end_matches(":type").to_string();
                for f in ["len", "prec", "scale"] {
                    self.inputs.remove(&format!("{row}:{f}"));
                }
                let mut parts = value.split('\u{1}');
                let ty = parts.next().unwrap_or("").to_string();
                match (parts.next(), parts.next()) {
                    (Some(l), None) => {
                        self.inputs.insert(format!("{row}:len"), l.to_string());
                    }
                    (Some(p), Some(sc)) => {
                        self.inputs.insert(format!("{row}:prec"), p.to_string());
                        self.inputs.insert(format!("{row}:scale"), sc.to_string());
                    }
                    _ => {}
                }
                self.inputs.insert(key, ty);
            }
            _ => {
                self.inputs.insert(key, value);
            }
        }
    }

    /// How many rows the new table's column list has.
    pub fn columns(&self) -> usize {
        self.get("new:ledger:ncols").parse().unwrap_or(1).max(1)
    }

    /// What a fresh page's forms start with: one column, `id BIGINT NOT
    /// NULL`, and seven years.
    pub fn prefill(&mut self) {
        for (k, v) in [
            ("new:ledger:col:0:name", "id"),
            ("new:ledger:col:0:type", "BIGINT"),
            ("new:ledger:col:0:notnull", "true"),
            ("new:ledger:keep", "7"),
        ] {
            self.inputs.entry(k.to_string()).or_insert_with(|| v.to_string());
        }
    }

    /// Column `i` of the new table as DDL, `Ok(None)` for an empty row.
    fn column_sql(&self, i: usize) -> Result<Option<String>, String> {
        let f = |n: &str| self.get(&format!("new:ledger:col:{i}:{n}")).trim().to_string();
        let (name, ty) = (f("name"), f("type"));
        if name.is_empty() && ty.is_empty() {
            return Ok(None);
        }
        if !ident_ok(&name) {
            return Err(format!("Column {}: a name is letters, digits and underscores, not starting with a digit.", i + 1));
        }
        if ty.is_empty() {
            return Err(format!("Column {name}: choose its type."));
        }
        if !ty.chars().all(|c| c.is_ascii_alphanumeric() || c == ' ' || c == '_') {
            return Err(format!("Column {name}: choose its type from the list."));
        }
        let num = |n: &str, what: &str| -> Result<Option<u32>, String> {
            let v = f(n);
            if v.is_empty() {
                return Ok(None);
            }
            v.parse::<u32>().ok().filter(|x| *x > 0 || n == "scale").map(Some).ok_or(format!("Column {name}: {what} is a whole number."))
        };
        let size = match (num("len", "its length")?, num("prec", "its precision")?, num("scale", "its scale")?) {
            (Some(l), _, _) => format!("({l})"),
            (None, Some(p), Some(sc)) if sc > p => return Err(format!("Column {name}: the scale ({sc}) cannot exceed the precision ({p}).")),
            (None, Some(p), Some(sc)) => format!("({p}, {sc})"),
            (None, Some(p), None) => format!("({p})"),
            (None, None, Some(_)) => return Err(format!("Column {name}: a scale needs a precision.")),
            (None, None, None) => String::new(),
        };
        let not_null = if self.on(&format!("new:ledger:col:{i}:notnull")) { " NOT NULL" } else { "" };
        Ok(Some(format!("{name} {ty}{size}{not_null}")))
    }

    pub fn get(&self, key: &str) -> &str {
        self.inputs.get(key).map(String::as_str).unwrap_or("")
    }

    fn on(&self, key: &str) -> bool {
        self.get(key) == "true"
    }

    /// The vault the page shows: the chosen one, or the first.
    pub fn current_vault(&self) -> Option<&VaultView> {
        let chosen = self.get(VAULT);
        self.vaults.iter().find(|v| v.name == chosen).or(self.vaults.first())
    }

    /// The `CREATE LEDGER TABLE` the form describes, or what it still lacks.
    pub fn create_ledger_sql(&self) -> Result<String, String> {
        let name = self.get("new:ledger:name").trim();
        if !ident_ok(name) {
            return Err("Name the table: letters, digits and underscores.".into());
        }
        let mut cols = Vec::new();
        for i in 0..self.columns() {
            if let Some(c) = self.column_sql(i)? {
                cols.push(c);
            }
        }
        if cols.is_empty() {
            return Err("Give it at least one column.".into());
        }
        let n = self.get("new:ledger:keep").trim();
        if n.parse::<u64>().map_or(true, |n| n == 0) {
            return Err("How long must each row be kept before it may be deleted? A whole number of the unit.".into());
        }
        let unit = unit_or(self.get("new:ledger:keep_unit"), "YEARS");
        let mut sql = format!("CREATE LEDGER TABLE {name} ({}) NO DELETE UNTIL {n} {unit} AFTER INSERT", cols.join(", "));
        if self.on("new:ledger:versioned") {
            sql.push_str(" VERSIONED");
        }
        let idle = self.get("new:ledger:idle").trim();
        if !idle.is_empty() {
            if idle.parse::<u64>().is_err() {
                return Err("NO DROP UNTIL takes a whole number.".into());
            }
            sql.push_str(&format!(" NO DROP UNTIL {idle} {} IDLE", unit_or(self.get("new:ledger:idle_unit"), "DAYS")));
        }
        let anchor = self.get("new:ledger:anchor").trim();
        if !anchor.is_empty() {
            sql.push_str(&format!(" ANCHOR TO {}", q(anchor)));
        }
        Ok(sql)
    }

    /// The `CREATE VAULT` the form describes, or what it still lacks.
    pub fn create_vault_sql(&self) -> Result<String, String> {
        let name = self.get("new:vault:name").trim();
        if !ident_ok(name) {
            return Err("Name the vault: letters, digits and underscores.".into());
        }
        let provider = self.provider_spec("new:vault:provider")?.ok_or("Choose where its key lives.")?;
        let mut sql = format!("CREATE VAULT {name} KEY PROVIDER {}", q(&provider));
        let n = self.get("new:vault:retain").trim();
        if !n.is_empty() {
            if n.parse::<u64>().is_err() {
                return Err("RETAIN DELETED takes a whole number.".into());
            }
            sql.push_str(&format!(" RETAIN DELETED {n} {}", unit_or(self.get("new:vault:retain_unit"), "DAYS")));
        }
        Ok(sql)
    }

    /// The key provider spec the provider form under `prefix` describes:
    /// `Ok(None)` when no kind is chosen, `Err` when a field it needs is
    /// empty or holds a `;` (the spec's separator).
    pub fn provider_spec(&self, prefix: &str) -> Result<Option<String>, String> {
        let f = |n: &str| self.get(&format!("{prefix}:{n}")).trim().to_string();
        let kind = f("kind");
        let path = f("path");
        let fields = [f("path"), f("module"), f("token"), f("keylabel"), f("pin"), f("tcti")];
        if fields.iter().any(|v| v.contains(';')) {
            return Err("A field holds a ';', which separates the provider's attributes.".into());
        }
        let need_path = |what: &str| if path.is_empty() { Err(format!("Give the {what}.")) } else { Ok(()) };
        let create = self.on(&format!("{prefix}:create"));
        Ok(Some(match kind.as_str() {
            "" | "same" => return Ok(None),
            "file" => {
                need_path("key file's path")?;
                format!("file:{path}")
            }
            "tpm" => {
                need_path("sealed key file's path")?;
                let mut s = format!("tpm:{path}");
                let tcti = f("tcti");
                if !tcti.is_empty() {
                    s.push_str(&format!(";tcti={tcti}"));
                }
                if create {
                    s.push_str(";create");
                }
                s
            }
            "mlkem" => {
                need_path("key pair file's path")?;
                format!("mlkem:{path}{}", if create { ";create" } else { "" })
            }
            "pkcs11" => {
                let (module, token, key) = (f("module"), f("token"), f("keylabel"));
                if module.is_empty() || token.is_empty() || key.is_empty() {
                    return Err("Give the PKCS#11 module, the token's label and the key's label.".into());
                }
                let mut s = format!("pkcs11:module={module};token={token};key={key}");
                let pin = f("pin");
                if !pin.is_empty() {
                    let attr = if self.get(&format!("{prefix}:pin_kind")) == "file" { "pin-source" } else { "pin-env" };
                    s.push_str(&format!(";{attr}={pin}"));
                }
                match self.get(&format!("{prefix}:mechanism")) {
                    m @ ("kwp" | "cbc") => s.push_str(&format!(";mechanism={m}")),
                    _ => {}
                }
                s
            }
            other => return Err(format!("Unknown key provider {other:?}.")),
        }))
    }

    /// The `VAULT … SET SECRET` the form describes. The value is in it, so
    /// the preview shows it masked ([`Self::set_secret_preview`]).
    pub fn set_secret_sql(&self, vault: &str) -> Result<String, String> {
        let k = |f: &str| format!("vault:{vault}:{f}");
        let name = self.get(&k("name")).trim();
        let value = self.get(&k("value"));
        if name.is_empty() {
            return Err("Name the secret (e.g. db/password).".into());
        }
        if value.is_empty() {
            return Err("Type its value.".into());
        }
        let mut sql = format!("VAULT {vault} SET SECRET {} = {}", q(name), q(value));
        let ct = self.get(&k("type")).trim();
        if !ct.is_empty() {
            sql.push_str(&format!(" CONTENT TYPE {}", q(ct)));
        }
        let ex = self.get(&k("expires")).trim();
        if let Some(p) = date_problem(ex) {
            return Err(format!("Expires: {p}"));
        }
        if !ex.is_empty() {
            sql.push_str(&format!(" EXPIRES {}", q(ex)));
        }
        Ok(sql)
    }

    pub fn set_secret_preview(&self, vault: &str) -> Result<String, String> {
        self.set_secret_sql(vault).map(|_| {
            let k = |f: &str| format!("vault:{vault}:{f}");
            let mut sql = format!("VAULT {vault} SET SECRET {} = '••••••'", q(self.get(&k("name")).trim()));
            let ct = self.get(&k("type")).trim();
            if !ct.is_empty() {
                sql.push_str(&format!(" CONTENT TYPE {}", q(ct)));
            }
            let ex = self.get(&k("expires")).trim();
            if !ex.is_empty() {
                sql.push_str(&format!(" EXPIRES {}", q(ex)));
            }
            sql
        })
    }
}

fn ident_ok(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !s.starts_with(|c: char| c.is_ascii_digit())
}

/// A column row's inputs: `new:ledger:col:<i>:<field>`.
const COLUMN_FIELDS: [&str; 6] = ["name", "type", "len", "prec", "scale", "notnull"];

const UNITS: [&str; 7] = ["SECONDS", "MINUTES", "HOURS", "DAYS", "WEEKS", "MONTHS", "YEARS"];

fn unit_or<'s>(chosen: &'s str, default: &'static str) -> &'s str {
    if UNITS.contains(&chosen) { chosen } else { default }
}

// ---------------------------------------------------------------------------
// The widgets Studio lends the page
// ---------------------------------------------------------------------------

/// How a piece of text or a pill is coloured, in Studio's palette.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tone {
    Normal,
    Dim,
    Faint,
    Good,
    Warn,
    Bad,
    Accent,
}

/// One column of a [`Kit::grid`].
pub struct Col {
    pub key: &'static str,
    pub header: &'static str,
    pub width: f32,
    pub right: bool,
    pub sortable: bool,
}

impl Col {
    pub const fn new(key: &'static str, header: &'static str, width: f32) -> Col {
        Col { key, header, width, right: false, sortable: true }
    }
    pub const fn right(mut self) -> Col {
        self.right = true;
        self
    }
    pub const fn unsortable(mut self) -> Col {
        self.sortable = false;
        self
    }
}

/// A cell of a [`Kit::grid`].
pub enum Cell<'a, M> {
    Text(String, Tone),
    Widget(Element<'a, M>),
}

/// What a column sorts by, for one row.
pub enum SortVal {
    Text(String),
    Num(f64),
}

/// One row of a [`Kit::grid`]: its cells, and a sort value per column.
pub struct GridRow<'a, M> {
    pub cells: Vec<Cell<'a, M>>,
    pub keys: Vec<SortVal>,
}

/// Studio's widgets, in its theme: what the page is drawn with.
pub trait Kit<'a, M: Clone + 'a> {
    /// A titled panel, as every Studio page is framed.
    fn frame(&self, title: String, trailing: Option<Element<'a, M>>, body: Element<'a, M>) -> Element<'a, M>;
    /// A section: a panel holding a section label and its body.
    fn section(&self, label: String, trailing: Option<Element<'a, M>>, body: Element<'a, M>) -> Element<'a, M>;
    /// A form's raised strip (the create forms).
    fn form(&self, label: String, body: Element<'a, M>) -> Element<'a, M>;
    /// A box that says what it holds is sensitive (a revealed value).
    fn alert(&self, body: Element<'a, M>) -> Element<'a, M>;
    fn text(&self, text: String, tone: Tone) -> Element<'a, M>;
    fn mono(&self, text: String, tone: Tone) -> Element<'a, M>;
    fn small_button(&self, label: &str, on: Option<M>) -> Element<'a, M>;
    /// A compact button for a grid row.
    fn row_button(&self, label: &str, on: Option<M>, destructive: bool) -> Element<'a, M>;
    fn primary_button(&self, label: &str, on: Option<M>) -> Element<'a, M>;
    fn choice(&self, label: &str, selected: bool, on: M) -> Element<'a, M>;
    /// A drop-down of `options` (shown labels); `on` gets the chosen one.
    fn pick(&self, label: String, options: Vec<String>, selected: Option<String>, width: f32, on: Box<dyn Fn(String) -> M + 'a>) -> Element<'a, M>;
    fn field(&self, label: String, placeholder: &str, value: &'a str, secret: bool, width: f32, on: Box<dyn Fn(String) -> M + 'a>) -> Element<'a, M>;
    /// The column types the server accepts, for the column editor.
    fn column_types(&self) -> Vec<ColumnType>;
    fn toggle(&self, label: String, value: bool, on: Box<dyn Fn(bool) -> M + 'a>) -> Element<'a, M>;
    fn tile(&self, label: String, value: String, note: Option<String>) -> Element<'a, M>;
    fn pill(&self, label: String, tone: Tone) -> Element<'a, M>;
    /// A sortable grid with resizable columns, at most `max_height` tall.
    fn grid(&self, scope: &str, cols: Vec<Col>, rows: Vec<GridRow<'a, M>>, max_height: f32) -> Element<'a, M>;
}

/// A column type the server accepts: its name as written in DDL, what it
/// takes after the name, and a line about it.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnType {
    pub name: String,
    pub params: TypeParams,
    pub help: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeParams {
    None,
    /// `VARCHAR(n)`, with the usual `n`.
    Length(u32),
    /// `NUMERIC(p, s)`, with the usual `p` and `s`.
    PrecisionScale(u32, u32),
}

/// The messages the page sends back to Studio.
#[derive(Clone)]
pub struct Actions<M> {
    pub reload: M,
    pub verify: M,
    /// Load one vault's audit trail.
    pub audit: fn(String) -> M,
    /// Run a statement this page built; its answer comes back to
    /// [`LedgerPage::ran`] and the page reloads.
    pub run: fn(String) -> M,
    /// An input changed: (key, value), to [`LedgerPage::input`].
    pub input: fn(String, String) -> M,
    /// A destructive statement, through Studio's confirmation dialog:
    /// (title, what it does, the statement).
    pub confirm: fn(String, String, String) -> M,
    /// Open a table on the DATA tab.
    pub open_table: fn(String) -> M,
}

// ---------------------------------------------------------------------------
// The page
// ---------------------------------------------------------------------------

/// The page.
pub fn view<'a, M: Clone + 'a, K: Kit<'a, M>>(page: Option<&'a LedgerPage>, a: Actions<M>, k: &K) -> Element<'a, M> {
    let bar: Element<'a, M> = row![
        k.small_button("VERIFY ALL", Some(a.verify.clone())),
        k.small_button("RELOAD", Some(a.reload.clone())),
    ]
    .spacing(6)
    .into();
    let Some(page) = page else {
        return k.frame("LEDGER".into(), Some(bar), k.text("Loading…".into(), Tone::Faint));
    };
    if let Some(e) = &page.error {
        return k.frame("LEDGER".into(), Some(bar), unsupported(k, e));
    }
    let shown = Shown::parse(&page.show);
    let tables: Vec<&Special> = shown.specials.iter().filter(|s| !s.vault).collect();
    let view = match page.get(VIEW) {
        "vaults" => "vaults",
        "tables" => "tables",
        _ if tables.is_empty() && !page.vaults.is_empty() => "vaults",
        _ => "tables",
    };
    let input = a.input;
    let switch = row![
        k.choice(&format!("LEDGER TABLES · {}", tables.len()), view == "tables", input(VIEW.into(), "tables".into())),
        k.choice(&format!("VAULTS · {}", page.vaults.len()), view == "vaults", input(VIEW.into(), "vaults".into())),
    ]
    .spacing(4);

    let mut body = column![chain(k, &shown, page)].spacing(10);
    if let Some(r) = result(k, &a, page) {
        body = body.push(r);
    }
    body = body.push(switch);
    if view == "tables" {
        body = body.push(tables_grid(k, &a, page, &tables));
        if let Some(t) = page.inputs.get(TABLE).and_then(|n| tables.iter().find(|t| &t.name == n)) {
            body = body.push(table_detail(k, &a, page, t));
        }
        body = body.push(create_ledger_form(k, &a, page));
        body = body.push(rollback_form(k, &a, page));
    } else {
        body = body.push(vaults(k, &a, page, &shown));
    }
    k.frame(
        "LEDGER".into(),
        Some(bar),
        iced::widget::scrollable(body.padding(iced::Padding { right: 12.0, ..iced::Padding::ZERO })).height(Fill).into(),
    )
}

fn unsupported<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, e: &str) -> Element<'a, M> {
    column![
        k.text("This server has no ledger tables or vaults".into(), Tone::Normal),
        k.text(
            format!(
                "SHOW LEDGER was refused: {e}. Ledger tables and vaults need a server built with the enterprise \
                 features (rasterizeddb_core_enterprise), and a role allowed to read them."
            ),
            Tone::Dim
        ),
    ]
    .spacing(8)
    .max_width(720)
    .into()
}

/// The chain as tiles, with the warnings as pills, and VERIFY's findings.
fn chain<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, shown: &Shown, page: &'a LedgerPage) -> Element<'a, M> {
    let c = shown.chain.clone().unwrap_or_default();
    let anchors = if shown.anchors.is_empty() { "none".to_string() } else { shown.anchors.len().to_string() };
    let tiles = row![
        k.tile("CHAIN ENTRIES".into(), c.entries.to_string(), None),
        k.tile("HEAD".into(), elide_middle(&c.head, 6), Some("sha256 of the newest entry".into())),
        k.tile(
            "ANCHORS".into(),
            anchors,
            Some(if shown.anchors.is_empty() {
                "nothing outside this server holds the head".into()
            } else {
                format!("{} · unanchored: {}", shown.anchor_mode, shown.unanchored)
            })
        ),
        k.tile("LOCATION".into(), elide_middle(&c.location, 10), Some(c.location.clone())),
    ]
    .spacing(8);
    let mut pills = row![].spacing(6);
    let mut any = false;
    for w in &shown.warnings {
        pills = pills.push(k.pill(w.to_uppercase(), Tone::Warn));
        any = true;
    }
    if shown.anchors.is_empty() && shown.chain.is_some() {
        pills = pills.push(k.pill("NO ANCHORS: A ROLLBACK OF THE WHOLE SERVER WOULD GO UNSEEN".into(), Tone::Warn));
        any = true;
    }
    if !shown.anchors.is_empty() {
        for s in &shown.anchors {
            pills = pills.push(k.pill(format!("ANCHOR {}", elide_middle(s, 24)), Tone::Dim));
        }
        any = true;
    }
    let mut col = column![tiles].spacing(8);
    if any {
        col = col.push(pills.wrap());
    }
    for o in &shown.other {
        col = col.push(k.mono(o.clone(), Tone::Dim));
    }
    if let Some(v) = &page.verify {
        let (label, tone) = if page.verified_intact() == Some(true) { ("INTACT", Tone::Good) } else { ("PROBLEMS FOUND", Tone::Bad) };
        let mut list = column![row![k.text("VERIFY".into(), Tone::Dim), k.pill(label.into(), tone)].spacing(8)].spacing(3);
        for l in v {
            let (status, rest) = status_of(l);
            list = list.push(
                row![
                    iced::widget::container(k.pill(if status.is_empty() { "—".into() } else { status.to_string() }, status_tone(status))).width(64),
                    k.mono(rest.to_string(), Tone::Normal),
                ]
                .spacing(8)
                .align_y(iced::alignment::Vertical::Center),
            );
        }
        col = col.push(list);
    }
    k.section("CHAIN".into(), None, col.into())
}

/// The last action's answer, and the detail panel.
fn result<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage) -> Option<Element<'a, M>> {
    if page.last.is_none() && page.detail.is_none() {
        return None;
    }
    let mut col = column![].spacing(4);
    if let Some((what, result)) = &page.last {
        let (pill, lines): (Element<'a, M>, Vec<String>) = match result {
            Ok(l) => (k.pill("DONE".into(), Tone::Good), if l.is_empty() { Vec::new() } else { l.clone() }),
            Err(e) => (k.pill("REFUSED".into(), Tone::Bad), vec![e.clone()]),
        };
        col = col.push(row![pill, k.mono(what.clone(), Tone::Dim)].spacing(8).align_y(iced::alignment::Vertical::Center));
        for l in lines.iter().take(12) {
            col = col.push(k.mono(l.clone(), Tone::Normal));
        }
        if lines.len() > 12 {
            col = col.push(k.text(format!("… {} more lines", lines.len() - 12), Tone::Faint));
        }
    }
    if let Some((heading, lines)) = &page.detail {
        col = col.push(k.text(heading.clone(), Tone::Dim));
        for l in lines {
            let (status, rest) = status_of(l);
            let line: Element<'a, M> = if status_tone(status) == Tone::Dim {
                k.mono(versions_line(l), Tone::Normal)
            } else {
                row![k.pill(status.to_string(), status_tone(status)), k.mono(rest.to_string(), Tone::Normal)].spacing(8).into()
            };
            col = col.push(line);
        }
    }
    Some(k.section("RESULT".into(), Some(k.small_button("CLEAR", Some((a.input)(CLEAR.into(), String::new())))), col.into()))
}

/// `v2 set by alice at 1795…` with the time readable.
fn versions_line(l: &str) -> String {
    match l.rsplit_once(" at ") {
        Some((head, us)) if us.chars().all(|c| c.is_ascii_digit()) && us.len() > 12 => format!("{head} at {}", when(us.parse().unwrap_or(0))),
        _ => l.to_string(),
    }
}

fn tables_grid<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, tables: &[&Special]) -> Element<'a, M> {
    if tables.is_empty() {
        return k.section(
            "LEDGER TABLES".into(),
            None,
            k.text("No ledger tables. Make one below: rows can be added, never changed, and not deleted before their retention ends.".into(), Tone::Faint),
        );
    }
    let selected = page.get(TABLE);
    let cols = vec![
        Col::new("name", "TABLE", 150.0),
        Col::new("actions", "", 230.0).unsortable(),
        Col::new("rows", "ROWS", 80.0).right(),
        Col::new("mode", "MODE", 110.0),
        Col::new("retention", "NO DELETE FOR", 120.0),
        Col::new("holds", "LEGAL HOLDS", 160.0),
        Col::new("anchors", "OWN ANCHORS", 160.0),
    ];
    let rows = tables
        .iter()
        .map(|t| {
            let count = page.counts.get(&t.name).copied();
            let actions = row![
                k.row_button("VERIFY", Some((a.run)(format!("VERIFY LEDGER {}", t.name))), false),
                k.row_button(if selected == t.name { "CLOSE" } else { "MANAGE" }, Some((a.input)(TABLE.into(), if selected == t.name { String::new() } else { t.name.clone() })), false),
                k.row_button("DATA", Some((a.open_table)(t.name.clone())), false),
            ]
            .spacing(4);
            GridRow {
                cells: vec![
                    Cell::Text(t.name.clone(), if selected == t.name { Tone::Accent } else { Tone::Normal }),
                    Cell::Widget(actions.into()),
                    Cell::Text(count.map_or("—".into(), |c| c.to_string()), Tone::Dim),
                    Cell::Text(if t.versioned { "versioned" } else { "append-only" }.into(), Tone::Dim),
                    Cell::Text(span(t.retention_s), Tone::Dim),
                    Cell::Text(if t.holds.is_empty() { "—".into() } else { t.holds.join(", ") }, if t.holds.is_empty() { Tone::Faint } else { Tone::Warn }),
                    Cell::Text(if t.anchors.is_empty() { "—".into() } else { t.anchors.join(" ") }, Tone::Faint),
                ],
                keys: vec![
                    SortVal::Text(t.name.clone()),
                    SortVal::Text(String::new()),
                    SortVal::Num(count.unwrap_or(-1) as f64),
                    SortVal::Text(if t.versioned { "versioned" } else { "append-only" }.into()),
                    SortVal::Num(t.retention_s as f64),
                    SortVal::Text(t.holds.join(" ")),
                    SortVal::Text(t.anchors.join(" ")),
                ],
            }
        })
        .collect();
    k.section("LEDGER TABLES".into(), None, k.grid("ledger-tables", cols, rows, 320.0))
}

fn field<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, key: String, label: &str, placeholder: &str, secret: bool, width: f32) -> Element<'a, M> {
    let input = a.input;
    let value = page.inputs.get(&key).map(String::as_str).unwrap_or("");
    k.field(label.to_string(), placeholder, value, secret, width, Box::new(move |v| input(key.clone(), v)))
}

/// [`field`] with what is wrong with its value, if anything, under it.
fn checked<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, field: Element<'a, M>, problem: Option<String>) -> Element<'a, M> {
    match problem {
        Some(p) => column![field, k.text(p, Tone::Bad)].spacing(2).into(),
        None => field,
    }
}

/// `Err` with why, when `s` is neither empty nor a date (`YYYY-MM-DD`,
/// optionally ` HH:MM[:SS]`, optionally a zone or `Z`).
pub fn date_problem(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let bad = || Some("Write a date as YYYY-MM-DD, or YYYY-MM-DD HH:MM.".to_string());
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let parts: Vec<&str> = date.split('-').collect();
    let [y, m, d] = parts.as_slice() else { return bad() };
    if y.len() != 4 {
        return bad();
    }
    let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i32>(), m.parse::<u32>(), d.parse::<u32>()) else { return bad() };
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return Some(format!("There is no month {m}.")),
    };
    if d == 0 || d > days {
        return Some(format!("{date} has no day {d}."));
    }
    if let Some(t) = time {
        let t = t.trim_end_matches('Z');
        let t = t.split(['+']).next().unwrap_or(t);
        let hms: Vec<&str> = t.split(':').collect();
        let ok = matches!(hms.len(), 2 | 3)
            && hms[0].parse::<u32>().is_ok_and(|h| h < 24)
            && hms[1].parse::<u32>().is_ok_and(|m| m < 60)
            && hms.get(2).is_none_or(|s| s.split('.').next().unwrap_or("").parse::<u32>().is_ok_and(|s| s < 60));
        if !ok {
            return Some("Write a time as HH:MM or HH:MM:SS (24-hour).".to_string());
        }
    }
    None
}

/// A unit picker: one choice per unit, the chosen one lit.
fn units<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, key: &str, default: &'static str, only: &[&'static str]) -> Element<'a, M> {
    let chosen = unit_or(page.get(key), default);
    let mut r = row![].spacing(2);
    for u in only {
        r = r.push(k.choice(u, chosen == *u, (a.input)(key.to_string(), u.to_string())));
    }
    column![k.text("Unit".into(), Tone::Dim), r].spacing(4).into()
}

fn preview<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, sql: &Result<String, String>) -> Element<'a, M> {
    match sql {
        Ok(s) => k.mono(s.clone(), Tone::Dim),
        Err(why) => k.text(why.clone(), Tone::Faint),
    }
}

/// Bottom-aligned beside fields that have a label above them.
fn lowered<'a, M: 'a>(e: Element<'a, M>) -> Element<'a, M> {
    column![Space::new().height(15), e].into()
}

fn table_detail<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, t: &Special) -> Element<'a, M> {
    let name = t.name.clone();
    let key = |f: &str| format!("ledger:{name}:{f}");
    let (n, hold, anchor, stamp) = (
        page.get(&key("retention")).trim().to_string(),
        page.get(&key("hold")).trim().to_string(),
        page.get(&key("anchor")).trim().to_string(),
        page.get(&key("stamp")).trim().to_string(),
    );
    let unit = unit_or(page.get(&key("retention_unit")), "YEARS").to_string();
    let retention_sql = (n.parse::<u64>().is_ok_and(|n| n > 0)).then(|| format!("ALTER LEDGER TABLE {name} SET RETENTION {n} {unit}"));
    let stamp_ok = stamp.parse::<i64>().is_ok();
    let place = (!hold.is_empty()).then(|| format!("ALTER LEDGER TABLE {name} SET LEGAL HOLD {}", q(&hold)));
    let mut holds = row![k.text("Legal holds".into(), Tone::Dim)].spacing(6).align_y(iced::alignment::Vertical::Center);
    if t.holds.is_empty() {
        holds = holds.push(k.text("none".into(), Tone::Faint));
    }
    for h in &t.holds {
        let sql = format!("ALTER LEDGER TABLE {name} RELEASE LEGAL HOLD {}", q(h));
        holds = holds.push(k.pill(h.to_uppercase(), Tone::Warn));
        holds = holds.push(k.row_button(
            "RELEASE",
            Some((a.confirm)(
                format!("Release legal hold {h}?"),
                format!("Rows of {name} past their retention can then be deleted again."),
                sql,
            )),
            true,
        ));
    }
    let body = column![
        holds,
        row![
            field(k, a, page, key("hold"), "New legal hold", "e.g. case-2026-117", false, 220.0),
            lowered(k.small_button(
                "PLACE HOLD",
                place.map(|sql| (a.confirm)(
                    format!("Place legal hold {hold} on {name}?"),
                    "No row of the table can be deleted while the hold stands, retention or not.".into(),
                    sql
                ))
            )),
            Space::new().width(24),
            checked(
                k,
                field(k, a, page, key("retention"), "Raise retention to", "e.g. 10", false, 90.0),
                (!n.is_empty() && !n.parse::<u64>().is_ok_and(|n| n > 0)).then(|| "A whole number above 0.".to_string())
            ),
            units(k, a, page, &key("retention_unit"), "YEARS", &["DAYS", "MONTHS", "YEARS"]),
            lowered(k.small_button(
                "SET RETENTION",
                retention_sql.map(|sql| (a.confirm)(
                    format!("Raise the retention of {name}?"),
                    format!("Retention is {} now. It can be raised, never lowered.", span(t.retention_s)),
                    sql
                ))
            )),
        ]
        .spacing(8)
        .align_y(iced::alignment::Vertical::Top),
        row![
            checked(
                k,
                field(k, a, page, key("stamp"), "Row stamp (__ledger_ts)", "e.g. 1795729719594470", false, 200.0),
                (!stamp.is_empty() && !stamp_ok).then(|| "A whole number: SELECT __ledger_ts, * FROM the table.".to_string())
            ),
            lowered(k.small_button("HISTORY", stamp_ok.then(|| (a.run)(format!("SHOW LEDGER HISTORY {name} {stamp}"))))),
            lowered(k.small_button("RECEIPT", stamp_ok.then(|| (a.run)(format!("SHOW LEDGER RECEIPT {name} {stamp}"))))),
            Space::new().width(24),
            field(k, a, page, key("anchor"), "Add an anchor", "dir:/mnt/anchors or s3://bucket/prefix", false, 300.0),
            lowered(k.small_button("ADD ANCHOR", (!anchor.is_empty()).then(|| (a.run)(format!("ALTER LEDGER TABLE {name} ADD ANCHOR {}", q(&anchor)))))),
        ]
        .spacing(8)
        .align_y(iced::alignment::Vertical::Top),
        k.text(
            format!(
                "{} · hash {} · {}. Find a row's __ledger_ts with SELECT __ledger_ts, * FROM {name}.",
                if t.versioned { "VERSIONED: rows can be updated, every version is kept and chained" } else { "Append-only" },
                t.hash,
                if t.drop_idle_s > 0 { format!("cannot be dropped until idle for {}", span(t.drop_idle_s)) } else { "no drop protection".to_string() }
            ),
            Tone::Faint
        ),
    ]
    .spacing(8);
    k.section(format!("MANAGE {}", t.name.to_uppercase()), None, body.into())
}

fn create_ledger_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage) -> Element<'a, M> {
    let sql = page.create_ledger_sql();
    let input = a.input;
    let types = k.column_types();
    let names: Vec<String> = types.iter().map(|t| t.name.clone()).collect();
    // Labels once, over the rows, at the rows' widths.
    let header = row![
        iced::widget::container(k.text("Column name".into(), Tone::Dim)).width(180),
        iced::widget::container(k.text("Type".into(), Tone::Dim)).width(200),
        iced::widget::container(k.text("Size".into(), Tone::Dim)).width(150),
        k.text("Not null".into(), Tone::Dim),
    ]
    .spacing(8);
    let mut cols = column![header].spacing(4);
    let n = page.columns();
    for i in 0..n {
        let key = |f: &str| format!("new:ledger:col:{i}:{f}");
        let ty = page.get(&key("type")).to_string();
        let chosen = types.iter().find(|t| t.name == ty);
        let name_key = key("name");
        let name = k.field(String::new(), "e.g. amount", page.inputs.get(&name_key).map(String::as_str).unwrap_or(""), false, 180.0, Box::new(move |v| input(name_key.clone(), v)));
        let type_key = key("type");
        let sized: Vec<(String, TypeParams)> = types.iter().map(|t| (t.name.clone(), t.params)).collect();
        let pick = k.pick(
            String::new(),
            names.clone(),
            (!ty.is_empty()).then(|| ty.clone()),
            200.0,
            Box::new(move |v| {
                let with_size = match sized.iter().find(|(n, _)| *n == v).map(|(_, p)| *p) {
                    Some(TypeParams::Length(l)) => format!("{v}\u{1}{l}"),
                    Some(TypeParams::PrecisionScale(p, s)) => format!("{v}\u{1}{p}\u{1}{s}"),
                    _ => v,
                };
                input(type_key.clone(), with_size)
            }),
        );
        let small = |f: &str, hint: String| {
            let fk = key(f);
            k.field(String::new(), &hint, page.inputs.get(&fk).map(String::as_str).unwrap_or(""), false, 70.0, Box::new(move |v| input(fk.clone(), v)))
        };
        let size: Element<'a, M> = match chosen.map(|t| t.params) {
            Some(TypeParams::Length(_)) => row![small("len", "none".to_string())].into(),
            Some(TypeParams::PrecisionScale(..)) => row![small("prec", "prec.".to_string()), small("scale", "scale".to_string())].spacing(4).into(),
            _ => k.text("—".into(), Tone::Faint),
        };
        let nn_key = key("notnull");
        let not_null = k.toggle(String::new(), page.get(&nn_key) == "true", Box::new(move |v| input(nn_key.clone(), v.to_string())));
        let help = chosen.map(|t| t.help.clone()).unwrap_or_default();
        cols = cols.push(
            row![
                name,
                pick,
                iced::widget::container(size).width(150),
                iced::widget::container(not_null).width(60),
                k.row_button("×", (n > 1).then(|| input(DROP_COLUMN.into(), i.to_string())), true),
                k.text(help, Tone::Faint),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Center),
        );
    }
    cols = cols.push(k.row_button("+ ADD COLUMN", Some(input(ADD_COLUMN.into(), String::new())), false));
    let versioned = page.get("new:ledger:versioned") == "true";
    let body = column![
        row![
            field(k, a, page, "new:ledger:name".into(), "Table name", "e.g. payments", false, 200.0),
            field(k, a, page, "new:ledger:keep".into(), "Rows kept at least", "e.g. 7", false, 120.0),
            units(k, a, page, "new:ledger:keep_unit", "YEARS", &["HOURS", "DAYS", "MONTHS", "YEARS"]),
            lowered(k.toggle("Versioned: rows may be updated, every version kept".into(), versioned, Box::new(move |on| input("new:ledger:versioned".into(), on.to_string())))),
        ]
        .spacing(10)
        .align_y(iced::alignment::Vertical::Bottom),
        cols,
        row![
            field(k, a, page, "new:ledger:idle".into(), "Not droppable until idle for", "optional, e.g. 30", false, 160.0),
            units(k, a, page, "new:ledger:idle_unit", "DAYS", &["DAYS", "MONTHS", "YEARS"]),
            field(k, a, page, "new:ledger:anchor".into(), "Anchor to", "optional: dir:/path or s3://bucket/prefix", false, 300.0),
            lowered(k.primary_button("CREATE LEDGER TABLE", sql.as_ref().ok().map(|s| (a.run)(s.clone())))),
        ]
        .spacing(10)
        .align_y(iced::alignment::Vertical::Bottom),
        preview(k, &sql),
        k.text(
            "A row can be added, never changed (unless versioned), and not deleted until it is as old as the period. \
             An idle period keeps the whole table from being dropped while rows keep arriving."
                .into(),
            Tone::Faint
        ),
    ]
    .spacing(8);
    k.form("NEW LEDGER TABLE".into(), body.into())
}

fn rollback_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage) -> Element<'a, M> {
    let reason = page.get("ack:reason").trim().to_string();
    let sql = (!reason.is_empty()).then(|| format!("ACKNOWLEDGE LEDGER ROLLBACK {}", q(&reason)));
    k.form(
        "AFTER A RESTORE".into(),
        column![
            row![
                field(k, a, page, "ack:reason".into(), "Why the ledger is behind its anchors", "e.g. restored the 2026-11-20 backup after disk loss", false, 360.0),
                lowered(k.small_button(
                    "ACKNOWLEDGE ROLLBACK",
                    sql.map(|s| (a.confirm)(
                        "Acknowledge the rollback?".into(),
                        "Ledger writes resume from the restored chain. The acknowledgement is chained, with this reason.".into(),
                        s
                    ))
                )),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            k.text("Only after restoring a backup older than the anchors: until then ledger writes are refused.".into(), Tone::Faint),
        ]
        .spacing(6)
        .into(),
    )
}

fn vaults<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, shown: &Shown) -> Element<'a, M> {
    let mut col = column![].spacing(10);
    let current = page.current_vault();
    if page.vaults.len() > 1 {
        let mut pick = row![k.text("Vault".into(), Tone::Dim)].spacing(4).align_y(iced::alignment::Vertical::Center);
        for v in &page.vaults {
            pick = pick.push(k.choice(&v.name, current.is_some_and(|c| c.name == v.name), (a.input)(VAULT.into(), v.name.clone())));
        }
        col = col.push(pick);
    }
    match current {
        None => col = col.push(k.section("VAULTS".into(), None, k.text("No vaults. Make one below.".into(), Tone::Faint))),
        Some(v) => {
            let special = shown.specials.iter().find(|s| s.vault && s.name == v.name);
            col = col.push(vault(k, a, page, v, special));
        }
    }
    col = col.push(create_vault_form(k, a, page));
    col.into()
}

fn vault<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, v: &'a VaultView, special: Option<&Special>) -> Element<'a, M> {
    let name = v.name.clone();
    let secrets: Vec<Secret> = v.secrets.iter().filter_map(|l| Secret::parse(l)).collect();
    let unread: Vec<&String> = v.secrets.iter().filter(|l| Secret::parse(l).is_none()).collect();
    let count = |state: &str| secrets.iter().filter(|s| !s.is_key() && s.state == state).count();
    let keys = secrets.iter().filter(|s| s.is_key()).count();
    let tiles = row![
        k.tile("SECRETS".into(), (secrets.len() - keys).to_string(), Some(format!("{} active · {} disabled · {} deleted", count("active"), count("disabled"), count("deleted")))),
        k.tile("KEYS".into(), keys.to_string(), Some("used in place, never revealed".into())),
        k.tile("KEY PROVIDER".into(), special.and_then(|s| s.key_provider.clone()).map_or("—".into(), |p| elide_middle(&p, 10)), special.and_then(|s| s.key_provider.clone())),
        k.tile("DELETED KEPT FOR".into(), special.map_or("—".into(), |s| span(s.retention_s)), Some("then PURGE may erase them".into())),
    ]
    .spacing(8);

    let mut col = column![tiles].spacing(10);
    if let Some((rv, rn, value)) = &page.revealed {
        if *rv == name {
            col = col.push(k.alert(
                column![
                    row![
                        k.pill("REVEALED".into(), Tone::Warn),
                        k.mono(rn.clone(), Tone::Normal),
                        Space::new().width(Fill),
                        k.small_button("HIDE", Some((a.input)(HIDE.into(), String::new()))),
                    ]
                    .spacing(8)
                    .align_y(iced::alignment::Vertical::Center),
                    k.mono(value.clone(), Tone::Normal),
                    k.text("This read is in the vault's audit trail. Hidden on HIDE, on reload, and when you leave the tab or vault.".into(), Tone::Faint),
                ]
                .spacing(6)
                .into(),
            ));
        }
    }

    // The secrets.
    let filter_key = format!("vault:{name}:filter");
    let filter = page.get(&filter_key).trim().to_ascii_lowercase();
    let shown: Vec<&Secret> = secrets.iter().filter(|s| filter.is_empty() || s.name.to_ascii_lowercase().contains(&filter)).collect();
    let cols = vec![
        Col::new("name", "NAME", 240.0),
        Col::new("actions", "", 250.0).unsortable(),
        Col::new("version", "VERSION", 70.0).right(),
        Col::new("state", "STATE", 90.0),
        Col::new("type", "TYPE", 120.0),
        Col::new("updated", "UPDATED (UTC)", 160.0),
        Col::new("expires", "EXPIRES (UTC)", 160.0),
    ];
    let rows: Vec<GridRow<'a, M>> = shown
        .iter()
        .map(|s| {
            let sn = q(&s.name);
            let run = |sql: String| Some((a.run)(sql));
            let mut actions = row![].spacing(4);
            if s.is_key() {
                let key = q(s.name.trim_start_matches("key:"));
                actions = actions.push(k.row_button("PUBLIC KEY", run(format!("VAULT {name} PUBLIC KEY {key}")), false));
            } else {
                let live = s.state == "active";
                actions = actions.push(k.row_button("REVEAL", live.then(|| (a.run)(format!("VAULT {name} GET SECRET {sn}"))), false));
                actions = actions.push(k.row_button("VERSIONS", run(format!("VAULT {name} VERSIONS SECRET {sn}")), false));
                match s.state.as_str() {
                    "active" => actions = actions.push(k.row_button("DISABLE", run(format!("VAULT {name} DISABLE SECRET {sn}")), false)),
                    "disabled" => actions = actions.push(k.row_button("ENABLE", run(format!("VAULT {name} ENABLE SECRET {sn}")), false)),
                    _ => {}
                }
                if s.state == "deleted" {
                    actions = actions.push(k.row_button("RECOVER", run(format!("VAULT {name} RECOVER SECRET {sn}")), false));
                    actions = actions.push(k.row_button(
                        "PURGE",
                        Some((a.confirm)(
                            format!("Purge {}?", s.name),
                            "Every version is erased and its data key destroyed: it cannot be recovered. Refused while the vault's retention for deleted secrets runs."
                                .into(),
                            format!("VAULT {name} PURGE SECRET {sn}"),
                        )),
                        true,
                    ));
                } else {
                    actions = actions.push(k.row_button(
                        "DELETE",
                        Some((a.confirm)(
                            format!("Delete {}?", s.name),
                            format!(
                                "A soft delete: RECOVER brings it back until it is purged{}.",
                                special.map_or(String::new(), |sp| format!(", which is refused for {}", span(sp.retention_s)))
                            ),
                            format!("VAULT {name} DELETE SECRET {sn}"),
                        )),
                        true,
                    ));
                }
            }
            let state_tone = match s.state.as_str() {
                "active" => Tone::Good,
                "disabled" => Tone::Warn,
                "deleted" => Tone::Bad,
                _ => Tone::Dim,
            };
            GridRow {
                cells: vec![
                    Cell::Text(s.name.clone(), if s.is_key() { Tone::Dim } else { Tone::Normal }),
                    Cell::Widget(actions.into()),
                    Cell::Text(format!("v{}", s.version), Tone::Dim),
                    Cell::Widget(k.pill(s.state.to_uppercase(), state_tone)),
                    Cell::Text(if s.content_type.is_empty() { "—".into() } else { s.content_type.clone() }, Tone::Dim),
                    Cell::Text(when(s.updated_us), Tone::Dim),
                    Cell::Text(when(s.expires_us), if s.expires_us > 0 { Tone::Normal } else { Tone::Faint }),
                ],
                keys: vec![
                    SortVal::Text(s.name.clone()),
                    SortVal::Text(String::new()),
                    SortVal::Num(s.version as f64),
                    SortVal::Text(s.state.clone()),
                    SortVal::Text(s.content_type.clone()),
                    SortVal::Num(s.updated_us as f64),
                    SortVal::Num(s.expires_us as f64),
                ],
            }
        })
        .collect();
    let header = row![
        field(k, a, page, filter_key.clone(), "Filter by name", "e.g. db/ or api-key", false, 260.0),
        lowered(k.text(format!("{} of {}", shown.len(), secrets.len()), Tone::Faint)),
    ]
    .spacing(8)
    .align_y(iced::alignment::Vertical::Bottom);
    let mut list = column![header].spacing(6);
    if secrets.is_empty() {
        list = list.push(k.text("No secrets yet. Set one below.".into(), Tone::Faint));
    } else {
        list = list.push(k.grid(&format!("ledger-secrets-{name}"), cols, rows, 460.0));
    }
    for u in unread {
        list = list.push(k.mono(u.clone(), Tone::Bad));
    }
    col = col.push(k.section(format!("SECRETS IN {}", name.to_uppercase()), None, list.into()));

    col = col.push(set_secret_form(k, a, page, &name));
    col = col.push(keys_form(k, a, page, v));
    col = col.push(audit(k, a, v));
    k.section(format!("VAULT {}", name.to_uppercase()), None, col.into())
}

fn set_secret_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, vault: &str) -> Element<'a, M> {
    let key = |f: &str| format!("vault:{vault}:{f}");
    let sql = page.set_secret_sql(vault);
    k.form(
        "SET A SECRET".into(),
        column![
            row![
                field(k, a, page, key("name"), "Secret name", "e.g. db/password", false, 220.0),
                field(k, a, page, key("value"), "Value", "the secret (hidden as you type)", true, 260.0),
                field(k, a, page, key("type"), "Content type", "optional, e.g. text/plain", false, 160.0),
                checked(k, field(k, a, page, key("expires"), "Expires", "optional, YYYY-MM-DD", false, 170.0), date_problem(page.get(&key("expires")))),
                lowered(k.primary_button("SET SECRET", sql.as_ref().ok().map(|s| (a.run)(s.clone())))),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Top),
            preview(k, &page.set_secret_preview(vault)),
            k.text("An existing name gets a new version; the old ones stay readable by VERSION n until purged.".into(), Tone::Faint),
        ]
        .spacing(6)
        .into(),
    )
}

fn keys_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, vault: &'a VaultView) -> Element<'a, M> {
    let v = vault.name.clone();
    let key = |f: &str| format!("vault:{v}:{f}");
    let input = a.input;
    // A new key.
    let new_name = page.get(&key("newkey")).trim().to_string();
    let kind = match page.get(&key("keytype")) {
        "" => "aes256-gcm".to_string(),
        t => t.to_string(),
    };
    let mut kinds = row![].spacing(2);
    for (t, label) in [("aes256-gcm", "AES-256-GCM"), ("mldsa65", "ML-DSA-65 signing"), ("hsm-aes256-gcm", "AES in the HSM"), ("hsm-ecdsa-p256", "ECDSA P-256 in the HSM")] {
        kinds = kinds.push(k.choice(label, kind == t, input(key("keytype"), t.to_string())));
    }
    let name_problem = (!new_name.is_empty() && !new_name.chars().all(|c| c.is_ascii_alphanumeric() || "_-./".contains(c)))
        .then(|| "Letters, digits and _ - . / only.".to_string());
    let create = (!new_name.is_empty() && name_problem.is_none())
        .then(|| (a.run)(format!("VAULT {v} CREATE KEY {} TYPE {}", q(&new_name), q(&kind))));

    // Using one: the vault's keys, and what each kind does.
    let keys: Vec<Secret> = vault.secrets.iter().filter_map(|l| Secret::parse(l)).filter(|s| s.is_key() && s.state == "active").collect();
    let names: Vec<String> = keys.iter().map(|s| s.name.trim_start_matches("key:").to_string()).collect();
    let chosen = page.get(&key("key")).to_string();
    let chosen_kind = keys.iter().find(|s| s.name.trim_start_matches("key:") == chosen).map(|s| s.content_type.clone()).unwrap_or_default();
    let (kp, ks) = (page.get(&key("plain")).to_string(), page.get(&key("sig")).trim().to_string());
    let encrypts = chosen_kind.ends_with("aes256-gcm");
    let signs = matches!(chosen_kind.as_str(), "mldsa65" | "hsm-ecdsa-p256");
    let with = |verb: &str| (!kp.is_empty()).then(|| (a.run)(format!("VAULT {v} {verb} KEY {} VALUE {}", q(&chosen), q(&kp))));
    let mut ops = row![].spacing(4);
    if encrypts {
        ops = ops.push(k.small_button("ENCRYPT", with("ENCRYPT"))).push(k.small_button("DECRYPT", with("DECRYPT")));
    }
    if signs {
        ops = ops
            .push(k.small_button("SIGN", with("SIGN")))
            .push(k.small_button(
                "VERIFY",
                (!kp.is_empty() && !ks.is_empty()).then(|| (a.run)(format!("VAULT {v} VERIFY KEY {} VALUE {} SIGNATURE {}", q(&chosen), q(&kp), q(&ks)))),
            ))
            .push(k.small_button("PUBLIC KEY", Some((a.run)(format!("VAULT {v} PUBLIC KEY {}", q(&chosen))))));
    }
    let key_pick = k.pick("Key".into(), names.clone(), (!chosen.is_empty()).then(|| chosen.clone()), 200.0, Box::new({
        let kk = key("key");
        move |c| input(kk.clone(), c)
    }));
    let mut use_row = row![key_pick].spacing(8).align_y(iced::alignment::Vertical::Bottom);
    if encrypts || signs {
        use_row = use_row.push(field(k, a, page, key("plain"), "Input", if encrypts { "text to encrypt, or ciphertext to decrypt" } else { "the message" }, true, 280.0));
        if signs {
            use_row = use_row.push(field(k, a, page, key("sig"), "Signature", "hex, for VERIFY", false, 200.0));
        }
        use_row = use_row.push(lowered(ops.into()));
    }
    let use_why = if names.is_empty() {
        "This vault has no key yet: create one above."
    } else if chosen.is_empty() {
        "Choose a key: what it can do follows from its type."
    } else if kp.is_empty() {
        "Type the input. Output appears in RESULT (a decrypted value in the revealed box)."
    } else {
        ""
    };

    let rotate_sql = match page.provider_spec(&key("rotate")) {
        Ok(None) => Ok(format!("VAULT {v} ROTATE KEY")),
        Ok(Some(spec)) => Ok(format!("VAULT {v} ROTATE KEY TO {}", q(&spec))),
        Err(e) => Err(e),
    };
    k.form(
        "KEYS USED IN PLACE".into(),
        column![
            row![
                checked(k, field(k, a, page, key("newkey"), "New key name", "e.g. invoices", false, 200.0), name_problem),
                column![k.text("Type".into(), Tone::Dim), kinds].spacing(4),
                lowered(k.small_button("CREATE KEY", create)),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Top),
            use_row,
            k.text(use_why.into(), Tone::Faint),
            provider_form(k, a, page, &key("rotate"), "Rotate the vault key to", Some("A new key, same provider")),
            row![
                k.small_button(
                    "ROTATE KEY",
                    rotate_sql.as_ref().ok().map(|sql| (a.confirm)(
                        format!("Rotate the key of {v}?"),
                        "Every data key is re-wrapped under the new key-encryption key. Values are not re-encrypted.".into(),
                        sql.clone()
                    ))
                ),
                preview(k, &rotate_sql),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Center),
        ]
        .spacing(8)
        .into(),
    )
}

fn audit<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, v: &'a VaultView) -> Element<'a, M> {
    let load = k.small_button(if v.audit.is_some() { "REFRESH" } else { "LOAD" }, Some((a.audit)(v.name.clone())));
    let Some(lines) = &v.audit else {
        return k.section("AUDIT TRAIL".into(), Some(load), k.text("Every read and change of this vault, chained before it was answered. LOAD to read it.".into(), Tone::Faint));
    };
    let cols = vec![Col::new("status", "CHAIN", 90.0), Col::new("time", "TIME (UTC)", 170.0), Col::new("event", "EVENT", 520.0)];
    let rows: Vec<GridRow<'a, M>> = lines
        .iter()
        .rev()
        .map(|l| {
            let (status, rest) = status_of(l);
            let (us, event) = rest.split_once(' ').unwrap_or(("", rest));
            let us: i64 = us.parse().unwrap_or(0);
            GridRow {
                cells: vec![
                    Cell::Widget(k.pill(if status.is_empty() { "—".into() } else { status.to_uppercase() }, status_tone(status))),
                    Cell::Text(when(us), Tone::Dim),
                    Cell::Text(event.to_string(), Tone::Normal),
                ],
                keys: vec![SortVal::Text(status.to_string()), SortVal::Num(us as f64), SortVal::Text(event.to_string())],
            }
        })
        .collect();
    k.section(format!("AUDIT TRAIL · {} EVENTS, NEWEST FIRST", lines.len()), Some(load), k.grid(&format!("ledger-audit-{}", v.name), cols, rows, 360.0))
}

/// The key provider kinds a vault's key can live in: (value, shown label).
const PROVIDERS: [(&str, &str); 4] = [
    ("pkcs11", "PKCS#11 HSM (key never leaves it)"),
    ("tpm", "TPM 2.0 (sealed to this machine)"),
    ("mlkem", "ML-KEM-768 + X25519 (post-quantum)"),
    ("file", "Key file (development only)"),
];

/// A key provider chosen from a drop-down, with only the fields its kind
/// needs and its hint beneath them. `same`, when given, is a first choice
/// that keeps the current provider.
fn provider_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage, prefix: &str, label: &str, same: Option<&str>) -> Element<'a, M> {
    let key = |f: &str| format!("{prefix}:{f}");
    let mut options: Vec<(&str, &str)> = Vec::new();
    if let Some(s) = same {
        options.push(("same", s));
    }
    options.extend(PROVIDERS);
    let kind = page.get(&key("kind"));
    let shown = options.iter().find(|(v, _)| *v == kind).map(|(_, l)| l.to_string());
    let input = a.input;
    let kind_key = key("kind");
    let labels: Vec<(String, String)> = options.iter().map(|(v, l)| (v.to_string(), l.to_string())).collect();
    let pick = k.pick(
        label.to_string(),
        labels.iter().map(|(_, l)| l.clone()).collect(),
        shown,
        300.0,
        Box::new(move |chosen| {
            let v = labels.iter().find(|(_, l)| *l == chosen).map(|(v, _)| v.clone()).unwrap_or_default();
            input(kind_key.clone(), v)
        }),
    );
    let mut fields = row![pick].spacing(8).align_y(iced::alignment::Vertical::Bottom);
    let create_toggle = |what: &str| {
        let ck = key("create");
        let on = page.get(&ck) == "true";
        lowered(k.toggle(format!("Create {what} if missing"), on, Box::new(move |v| input(ck.clone(), v.to_string()))))
    };
    let hint = match kind {
        "file" => {
            fields = fields.push(field(k, a, page, key("path"), "Key file", "/etc/rasterizeddb/vault.key", false, 320.0));
            "The key sits on the database host beside the data: for tests and development only. The server must run with RDB_VAULT_ALLOW_FILE_KEY=1, and the file must be on a filesystem that keeps Unix permissions."
        }
        "pkcs11" => {
            let pin_kind = page.get(&key("pin_kind"));
            let mut kinds = row![].spacing(2);
            for (v, l) in [("env", "env var"), ("file", "file")] {
                let selected = pin_kind == v || (pin_kind.is_empty() && v == "env");
                kinds = kinds.push(k.choice(l, selected, input(key("pin_kind"), v.to_string())));
            }
            let mech = page.get(&key("mechanism"));
            let mut mechs = row![].spacing(2);
            for (v, l) in [("", "AES-GCM"), ("kwp", "KWP"), ("cbc", "CBC")] {
                mechs = mechs.push(k.choice(l, mech == v, input(key("mechanism"), v.to_string())));
            }
            fields = fields
                .push(field(k, a, page, key("module"), "PKCS#11 module", "/usr/lib/softhsm/libsofthsm2.so", false, 260.0))
                .push(field(k, a, page, key("token"), "Token label", "e.g. rdb", false, 140.0))
                .push(field(k, a, page, key("keylabel"), "Key label", "e.g. vault-kek", false, 140.0));
            let more = row![
                field(k, a, page, key("pin"), if pin_kind == "file" { "PIN file" } else { "PIN environment variable" }, if pin_kind == "file" { "/etc/rasterizeddb/hsm.pin" } else { "e.g. HSM_PIN" }, false, 180.0),
                column![k.text("PIN from".into(), Tone::Dim), kinds].spacing(4),
                column![k.text("Wrapping".into(), Tone::Dim), mechs].spacing(4),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom);
            return column![
                fields,
                more,
                k.text(
                    "The key-encryption key is made and kept inside the HSM (SoftHSM, a network HSM, a cloud HSM, or a TPM through tpm2-pkcs11); data keys are wrapped and unwrapped there. AES-GCM unless the token lacks it."
                        .into(),
                    Tone::Faint
                ),
            ]
            .spacing(6)
            .into();
        }
        "tpm" => {
            fields = fields
                .push(field(k, a, page, key("path"), "Sealed key file", "/etc/rasterizeddb/vault.tpmkey", false, 300.0))
                .push(field(k, a, page, key("tcti"), "TPM connection (TCTI)", "device:/dev/tpmrm0", false, 220.0))
                .push(create_toggle("and seal one"));
            "Sealed by this machine's TPM: the file is useless on any other machine or after the storage hierarchy is cleared. The server's user needs the tss group for /dev/tpmrm0."
        }
        "mlkem" => {
            fields = fields.push(field(k, a, page, key("path"), "Key pair file", "/etc/rasterizeddb/vault.mlkem", false, 320.0)).push(create_toggle("the key pair"));
            "Data keys are wrapped with a hybrid ML-KEM-768 + X25519 key pair, sound against a quantum computer. Not offered in FIPS mode."
        }
        "same" => "A new key-encryption key from the same provider; every data key is re-wrapped under it.",
        _ => "Where the vault's key-encryption key lives. An HSM keeps it out of the server's memory altogether.",
    };
    column![fields, k.text(hint.into(), Tone::Faint)].spacing(6).into()
}

fn create_vault_form<'a, M: Clone + 'a, K: Kit<'a, M>>(k: &K, a: &Actions<M>, page: &'a LedgerPage) -> Element<'a, M> {
    let sql = page.create_vault_sql();
    k.form(
        "NEW VAULT".into(),
        column![
            row![
                field(k, a, page, "new:vault:name".into(), "Vault name", "e.g. app_secrets", false, 180.0),
                field(k, a, page, "new:vault:retain".into(), "Keep deleted secrets", "e.g. 90", false, 90.0),
                units(k, a, page, "new:vault:retain_unit", "DAYS", &["DAYS", "MONTHS", "YEARS"]),
                lowered(k.primary_button("CREATE VAULT", sql.as_ref().ok().map(|s| (a.run)(s.clone())))),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            provider_form(k, a, page, "new:vault:provider", "Key provider", None),
            preview(k, &sql),
        ]
        .spacing(6)
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn show_ledger_is_read_into_structure() {
        let shown = Shown::parse(&s(&[
            "chain: 321 entries, head edbb21ed30, at /x/rdb_ledger/chain.log",
            "WARNING: the chain is on the same device as the data",
            "anchors: dir:/a (async), unanchored: none",
            "pay: ledger id b62b, retention 86400 s, drop after 0 s idle, hash sha256, holds case-1",
            "acct: ledger id 560e, retention 86400 s, drop after 0 s idle, hash sha256, versioned",
            "sec: vault id 0e6e, retention 604800 s, drop after 0 s idle, hash sha256, key provider file:/k",
        ]));
        assert_eq!(shown.chain.as_ref().unwrap().entries, 321);
        assert_eq!(shown.chain.as_ref().unwrap().location, "/x/rdb_ledger/chain.log");
        assert_eq!(shown.warnings, ["the chain is on the same device as the data"]);
        assert_eq!(shown.anchors, ["dir:/a"]);
        assert_eq!((shown.anchor_mode.as_str(), shown.unanchored.as_str()), ("async", "none"));
        assert_eq!(shown.specials.len(), 3);
        assert_eq!(shown.specials[0].holds, ["case-1"]);
        assert!(shown.specials[1].versioned && !shown.specials[0].versioned);
        assert_eq!(shown.specials[2].key_provider.as_deref(), Some("file:/k"));
        assert!(shown.other.is_empty());
        assert_eq!(LedgerPage::vault_names(&s(&["sec: vault id 0e6e, retention 1 s"])), ["sec"]);
    }

    #[test]
    fn list_lines_are_read() {
        let x = Secret::parse("app/url v3 active type=text/plain expires_us=1814313600000000 updated_us=1795729719719161").unwrap();
        assert_eq!((x.version, x.state.as_str(), x.content_type.as_str()), (3, "active", "text/plain"));
        assert_eq!(when(x.expires_us), "2027-06-30 00:00:00");
        assert!(Secret::parse("key:k v1 active type=aes256-gcm").unwrap().is_key());
    }

    #[test]
    fn a_reveal_and_a_decrypt_go_to_the_box_and_set_values_are_forgotten() {
        let mut p = LedgerPage::default();
        p.inputs.insert("vault:v:value".into(), "hunter2".into());
        p.ran("VAULT v SET SECRET 'db' = 'hunter2'", Ok(vec!["db version 1".into()]));
        assert!(!p.inputs.contains_key("vault:v:value"));
        assert_eq!(p.last.as_ref().unwrap().0, "VAULT v SET", "the heading never carries the statement");
        p.ran("VAULT v GET SECRET 'db'", Ok(vec!["hunter2".into()]));
        assert_eq!(p.revealed.as_ref().unwrap().2, "hunter2");
        assert!(!format!("{:?}", p.last).contains("hunter2"));
        p.ran("VAULT v DECRYPT KEY 'k' VALUE 'ct'", Ok(vec!["plain".into()]));
        assert_eq!(p.revealed.as_ref().unwrap().2, "plain");
        // A vault switch, a reload and HIDE each hide it.
        p.input(VAULT.into(), "w".into());
        assert!(p.revealed.is_none());
        p.ran("VAULT v GET SECRET 'db'", Ok(vec!["hunter2".into()]));
        let mut reloaded = LedgerPage::default();
        reloaded.carry_over(p);
        assert!(reloaded.revealed.is_none());
    }

    #[test]
    fn versions_and_history_go_to_the_detail_panel() {
        let mut p = LedgerPage::default();
        p.ran("VAULT v VERSIONS SECRET 'db'", Ok(vec!["v1 set by a at 1795729719719161".into()]));
        assert_eq!(p.detail.as_ref().unwrap().0, "VERSIONS OF db");
        assert_eq!(versions_line(&p.detail.as_ref().unwrap().1[0]), "v1 set by a at 2026-11-26 21:48:39");
        p.ran("SHOW LEDGER HISTORY acct 17", Ok(vec!["v1 chained   id=1".into()]));
        assert!(p.detail.as_ref().unwrap().0.starts_with("HISTORY acct 17"));
        p.ran("VERIFY LEDGER pay", Ok(vec!["OK        pay: 3 row(s) match the chain".into()]));
        assert_eq!(p.verified_intact(), Some(true));
    }

    #[test]
    fn the_create_forms_build_their_statements() {
        let mut p = LedgerPage::default();
        assert!(p.create_ledger_sql().is_err());
        p.prefill();
        p.input(ADD_COLUMN.into(), String::new());
        for (k, v) in [
            ("new:ledger:name", "payments"),
            ("new:ledger:col:1:name", "amount"),
            ("new:ledger:col:1:type", "NUMERIC"),
            ("new:ledger:col:1:prec", "12"),
            ("new:ledger:col:1:scale", "2"),
            ("new:ledger:versioned", "true"),
        ] {
            p.input(k.into(), v.into());
        }
        assert_eq!(
            p.create_ledger_sql().unwrap(),
            "CREATE LEDGER TABLE payments (id BIGINT NOT NULL, amount NUMERIC(12, 2)) NO DELETE UNTIL 7 YEARS AFTER INSERT VERSIONED"
        );
        // A row with a name and no type, a scale past the precision, and a
        // changed type's stale size each say what is wrong.
        p.input(ADD_COLUMN.into(), String::new());
        p.input("new:ledger:col:2:name".into(), "memo".into());
        assert_eq!(p.create_ledger_sql().unwrap_err(), "Column memo: choose its type.");
        p.input("new:ledger:col:2:type".into(), "VARCHAR".into());
        p.input("new:ledger:col:2:len".into(), "80".into());
        p.input("new:ledger:col:1:scale".into(), "14".into());
        assert!(p.create_ledger_sql().unwrap_err().contains("cannot exceed the precision"));
        p.input("new:ledger:col:1:scale".into(), "2".into());
        p.input(DROP_COLUMN.into(), "0".into());
        assert_eq!(p.columns(), 2);
        assert!(p.create_ledger_sql().unwrap().contains("(amount NUMERIC(12, 2), memo VARCHAR(80))"), "{:?}", p.create_ledger_sql());
        p.input("new:ledger:col:1:type".into(), "TEXT".into());
        assert!(p.create_ledger_sql().unwrap().contains("memo TEXT)"), "a new type drops the old size");
        // Picking a sized type fills in its usual size, which the statement carries.
        p.input("new:ledger:col:1:type".into(), "VARCHAR\u{1}255".into());
        assert!(p.create_ledger_sql().unwrap().contains("memo VARCHAR(255))"), "{:?}", p.create_ledger_sql());
        p.input("new:ledger:col:0:type".into(), "NUMERIC\u{1}12\u{1}2".into());
        assert!(p.create_ledger_sql().unwrap().contains("(amount NUMERIC(12, 2), memo VARCHAR(255))"), "{:?}", p.create_ledger_sql());
        assert_eq!(p.get("new:ledger:col:0:type"), "NUMERIC");
        p.input("new:ledger:col:0:type".into(), "INT); DROP TABLE x; --".into());
        assert!(p.create_ledger_sql().is_err());
        assert_eq!(date_problem("2027-06-31").unwrap(), "2027-06-31 has no day 31.");
        assert_eq!(date_problem(""), None);
        assert_eq!(date_problem("2027-06-30 18:00"), None);
        p.input("vault:app:expires".into(), "next week".into());

        for (k, v) in [
            ("new:vault:name", "app"),
            ("new:vault:provider:kind", "tpm"),
            ("new:vault:provider:path", "/k"),
            ("new:vault:provider:create", "true"),
            ("new:vault:retain", "90"),
        ] {
            p.input(k.into(), v.into());
        }
        assert_eq!(p.create_vault_sql().unwrap(), "CREATE VAULT app KEY PROVIDER 'tpm:/k;create' RETAIN DELETED 90 DAYS");
        // Each kind builds its spec from its own fields.
        let mut h = LedgerPage::default();
        assert_eq!(h.provider_spec("x"), Ok(None));
        for (k, v) in [("x:kind", "pkcs11"), ("x:module", "/usr/lib/softhsm/libsofthsm2.so"), ("x:token", "rdb")] {
            h.input(k.into(), v.into());
        }
        assert!(h.provider_spec("x").is_err(), "the key label is missing");
        h.input("x:keylabel".into(), "kek".into());
        h.input("x:pin".into(), "HSM_PIN".into());
        assert_eq!(
            h.provider_spec("x").unwrap().unwrap(),
            "pkcs11:module=/usr/lib/softhsm/libsofthsm2.so;token=rdb;key=kek;pin-env=HSM_PIN"
        );
        h.input("x:pin_kind".into(), "file".into());
        h.input("x:mechanism".into(), "kwp".into());
        assert!(h.provider_spec("x").unwrap().unwrap().ends_with(";pin-source=HSM_PIN;mechanism=kwp"));
        h.input("x:token".into(), "a;b".into());
        assert!(h.provider_spec("x").is_err(), "a ';' would split the spec");
        let mut m = LedgerPage::default();
        m.input("y:kind".into(), "mlkem".into());
        assert!(m.provider_spec("y").is_err());
        m.input("y:path".into(), "/keys/pq".into());
        assert_eq!(m.provider_spec("y").unwrap().unwrap(), "mlkem:/keys/pq");
        m.input("y:kind".into(), "same".into());
        assert_eq!(m.provider_spec("y"), Ok(None));

        p.input("vault:app:name".into(), "db/pw".into());
        p.input("vault:app:value".into(), "s3cr'et".into());
        assert!(p.set_secret_sql("app").unwrap_err().starts_with("Expires: "), "a bad date is refused, saying why");
        p.input("vault:app:expires".into(), String::new());
        assert_eq!(p.set_secret_sql("app").unwrap(), "VAULT app SET SECRET 'db/pw' = 's3cr''et'");
        assert!(!p.set_secret_preview("app").unwrap().contains("s3cr"), "the preview never shows the value");
    }

    #[test]
    fn small_formats() {
        assert_eq!(q("it's"), "'it''s'");
        assert_eq!(span(86_400), "1 day");
        assert_eq!(span(604_800), "7 days");
        assert_eq!(span(366 * 86_400 * 7), "7 years");
        assert_eq!(elide_middle("abcdefghijklmnop", 3), "abc…nop");
        assert_eq!(status_of("OK        chain: 3"), ("OK", "chain: 3"));
        assert_eq!(status_of("v1 chained"), ("", "v1 chained"));
    }
}
