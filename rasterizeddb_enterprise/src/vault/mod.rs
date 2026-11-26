//! Secret vaults: a replacement for Azure Key Vault, AWS Secrets Manager and
//! HashiCorp Vault's KV engine (docs/ledger-and-vault-plan.md §4).
//!
//! A vault is a ledger table of fixed shape (`ledger_ddl::VAULT_COLUMNS`)
//! that only these statements write:
//!
//! ```sql
//! VAULT v SET SECRET 'name' = '<value>' [CONTENT TYPE '<t>'] [EXPIRES '<time>'] [NOT BEFORE '<time>'] [TAGS '<text>']
//! VAULT v GET SECRET 'name' [VERSION <n>]
//! VAULT v LIST
//! VAULT v VERSIONS SECRET 'name'
//! VAULT v DISABLE | ENABLE | DELETE | RECOVER | PURGE SECRET 'name'
//! VAULT v ROTATE KEY [TO '<provider>']
//! VAULT v AUDIT [SECRET 'name']
//! ```
//!
//! - **Keys.** Each secret version is sealed (AES-256-GCM) under a data key
//!   of its own. The data key is kept only wrapped, by the vault's key
//!   provider (an HSM or TPM through PKCS#11, a post-quantum hybrid key, or a
//!   file for tests), in the vault's keyring beside its chain. The sealed
//!   value is bound to the vault, the name and the version, so it cannot be
//!   moved to another secret.
//! - **Versions.** A `SET` is a new row. Nothing is updated, so every
//!   version stays readable until it is purged.
//! - **Soft delete, purge protection.** `DELETE` writes a state row.
//!   `RECOVER` undoes it. `PURGE` is refused until the vault's
//!   `RETAIN DELETED` time has passed since the delete. It then
//!   **crypto-shreds** the secret: its data keys leave the keyring and its
//!   sealed values can never be opened again. The rows and their digests
//!   stay, so the ledger still verifies.
//! - **Audit.** Every `GET`, served or refused, and every change is written
//!   to the vault's audit log and chained, *before* a value is returned. If
//!   the audit cannot be written, nothing is returned.
//! - **Exposure.** The value is never in the WAL, a replication stream or a
//!   backup in plaintext: only ciphertext is inserted. A statement carrying
//!   it is cut at `SECRET` wherever statement text is kept
//!   (`core::redact`). Data keys and plaintext live in locked memory
//!   ([`crate::crypto::secure`]), and core dumps are off once a vault is used.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use parking_lot::Mutex;
use rasterizeddb_core::core::ledger_hooks::{LedgerReply, LedgerRequest};
use rasterizeddb_core::core::row::schema::TableSchema;

use crate::crypto::{self, KeyProvider, secure};
use crate::ledger::store::{self, Item, OP_EVENT};
use crate::ledger::{LedgerGuard, clock};

/// One row of a vault, decoded.
#[derive(Clone, Debug, Default)]
struct VRow {
    name: String,
    version: i64,
    value: String,
    state: String,
    content_type: String,
    not_before: i64,
    expires_at: i64,
    created_by: String,
    created_at: i64,
    ts: i64,
}

fn text(b: &[u8]) -> String {
    let end = b.iter().rposition(|x| *x != 0).map_or(0, |i| i + 1);
    String::from_utf8_lossy(&b[..end]).into_owned()
}

fn int(b: &[u8]) -> i64 {
    let mut w = [0u8; 8];
    let n = b.len().min(8);
    w[..n].copy_from_slice(&b[..n]);
    i64::from_le_bytes(w)
}

fn decode(table: &TableSchema, rows: &[(i64, Vec<(u32, Vec<u8>)>)]) -> Vec<VRow> {
    let order: HashMap<&str, u32> = table
        .fields
        .iter()
        .filter(|f| !f.is_deleted)
        .map(|f| (f.name.as_str(), f.write_order as u32))
        .collect();
    let mut out: Vec<VRow> = rows
        .iter()
        .map(|(ts, cells)| {
            let get = |col: &str| order.get(col).and_then(|o| cells.iter().find(|(c, _)| c == o)).map(|(_, b)| b.as_slice()).unwrap_or(&[]);
            VRow {
                name: text(get("name")),
                version: int(get("version")),
                value: text(get("value")),
                state: text(get("state")),
                content_type: text(get("content_type")),
                not_before: int(get("not_before")),
                expires_at: int(get("expires_at")),
                created_by: text(get("created_by")),
                created_at: int(get("created_at")),
                ts: *ts,
            }
        })
        .collect();
    out.sort_by_key(|r| r.ts);
    out
}

/// Words, with quoted strings kept whole (leading `'`, closing dropped).
fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = sql.trim().trim_end_matches(';').chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '\'' {
            chars.next();
            let mut v = String::from("'");
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        v.push('\'');
                        continue;
                    }
                    break;
                }
                v.push(ch);
            }
            out.push(v);
        } else if c == '=' {
            chars.next();
            out.push("=".to_string());
        } else {
            let mut w = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == '\'' || ch == '=' {
                    break;
                }
                w.push(ch);
                chars.next();
            }
            out.push(w);
        }
    }
    out
}

fn quoted(w: Option<&String>, what: &str) -> Result<String, String> {
    w.and_then(|w| w.strip_prefix('\'')).map(str::to_string).ok_or_else(|| format!("42601: expected {what} in quotes"))
}

fn valid_name(n: &str) -> Result<(), String> {
    if n.is_empty() || n.len() > 256 || !n.chars().all(|c| c.is_ascii_alphanumeric() || "._-/:".contains(c)) {
        return Err(format!("42602: a secret name is 1 to 256 of A-Z a-z 0-9 . _ - / : (got {n:?})"));
    }
    Ok(())
}

/// `s` as a JSON string literal.
fn json_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn sql_text(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS[Z]` (UTC) as microseconds.
fn parse_time(s: &str) -> Result<i64, String> {
    let bad = || format!("22007: {s:?} is not a time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ, UTC)");
    let s = s.trim().trim_end_matches('Z');
    let (date, time) = s.split_once(['T', ' ']).unwrap_or((s, "00:00:00"));
    let d: Vec<i64> = date.split('-').map(|x| x.parse().ok()).collect::<Option<_>>().ok_or_else(bad)?;
    let t: Vec<i64> = time.split(':').map(|x| x.parse().ok()).collect::<Option<_>>().ok_or_else(bad)?;
    let ([y, m, dd], [h, mi, se]) = (d.as_slice(), t.as_slice()) else { return Err(bad()) };
    if !(1..=12).contains(m) || !(1..=31).contains(dd) || *h > 23 || *mi > 59 || *se > 60 {
        return Err(bad());
    }
    Ok((clock::days_from_civil(*y, *m, *dd) * 86_400 + h * 3600 + mi * 60 + se) * 1_000_000)
}

// ---- keyring --------------------------------------------------------------

/// A vault's wrapped data keys: `(name, version) → wrapped key`. Beside the
/// chain as `vault-<id>.keys`, rewritten whole (temporary file, sync,
/// rename) when a purge removes keys or a rotation rewraps them.
struct Keyring {
    path: Option<std::path::PathBuf>,
    keys: BTreeMap<(String, i64), Vec<u8>>,
}

static KEYRINGS: OnceLock<Mutex<HashMap<(String, u128), Arc<Mutex<Keyring>>>>> = OnceLock::new();

fn keyring(store_key: &str, vault: u128) -> Result<Arc<Mutex<Keyring>>, String> {
    let all = KEYRINGS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = all.lock();
    if let Some(k) = map.get(&(store_key.to_string(), vault)) {
        return Ok(k.clone());
    }
    let path = (!store_key.starts_with("memory:")).then(|| store::chain_dir(store_key).join(format!("vault-{vault:032x}.keys")));
    // The store's directory, which nothing may have made yet: a vault's first
    // SET can come before any ledger row opened its chain.
    if let Some(dir) = path.as_ref().and_then(|p| p.parent()) {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let mut keys = BTreeMap::new();
    if let Some(p) = &path {
        match std::fs::read_to_string(p) {
            Ok(t) => {
                for line in t.lines() {
                    let mut f = line.split_whitespace();
                    let (Some(name), Some(v), Some(w)) = (f.next(), f.next(), f.next()) else { continue };
                    let name = unhex(name).and_then(|b| String::from_utf8(b).ok()).ok_or("XX001: the vault keyring is damaged")?;
                    let v: i64 = v.parse().map_err(|_| "XX001: the vault keyring is damaged")?;
                    keys.insert((name, v), unhex(w).ok_or("XX001: the vault keyring is damaged")?);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("{}: {e}", p.display())),
        }
    }
    let k = Arc::new(Mutex::new(Keyring { path, keys }));
    map.insert((store_key.to_string(), vault), k.clone());
    Ok(k)
}

fn hexs(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

impl Keyring {
    fn line(name: &str, v: i64, w: &[u8]) -> String {
        format!("{} {v} {}\n", hexs(name.as_bytes()), hexs(w))
    }

    /// Adds one key, appended and synced.
    fn add(&mut self, name: &str, v: i64, wrapped: Vec<u8>) -> Result<(), String> {
        if let Some(p) = &self.path {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(p).map_err(|e| format!("{}: {e}", p.display()))?;
            f.write_all(Self::line(name, v, &wrapped).as_bytes())
                .and_then(|()| f.sync_data())
                .map_err(|e| format!("{}: {e}", p.display()))?;
        }
        self.keys.insert((name.to_string(), v), wrapped);
        Ok(())
    }

    /// Writes the whole keyring again (after a purge or a rotation).
    fn rewrite(&self) -> Result<(), String> {
        let Some(p) = &self.path else { return Ok(()) };
        use std::io::Write;
        let tmp = p.with_extension("keys.tmp");
        let mut f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        for ((n, v), w) in &self.keys {
            f.write_all(Self::line(n, *v, w).as_bytes()).map_err(|e| e.to_string())?;
        }
        f.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, p).map_err(|e| e.to_string())?;
        if let Some(d) = p.parent().and_then(|d| std::fs::File::open(d).ok()) {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

// ---- providers ------------------------------------------------------------

static PROVIDERS: OnceLock<Mutex<HashMap<String, Arc<dyn KeyProvider>>>> = OnceLock::new();

fn provider(uri: &str) -> Result<Arc<dyn KeyProvider>, String> {
    if uri.starts_with("file:") && std::env::var("RDB_VAULT_ALLOW_FILE_KEY").as_deref() != Ok("1") {
        return Err(
            "55000: this vault's key is a file on the database host; set RDB_VAULT_ALLOW_FILE_KEY=1 to allow it \
             (tests and development only), or move it to pkcs11: or mlkem:"
                .to_string(),
        );
    }
    let all = PROVIDERS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(p) = all.lock().get(uri) {
        return Ok(p.clone());
    }
    let p: Arc<dyn KeyProvider> = Arc::from(crypto::provider(uri)?);
    all.lock().insert(uri.to_string(), p.clone());
    Ok(p)
}

fn aad(vault: u128, name: &str, version: i64) -> Vec<u8> {
    let mut a = b"rdb-vault-v1".to_vec();
    a.extend_from_slice(&vault.to_le_bytes());
    a.extend_from_slice(&(name.len() as u32).to_le_bytes());
    a.extend_from_slice(name.as_bytes());
    a.extend_from_slice(&version.to_le_bytes());
    a
}

// ---- audit ----------------------------------------------------------------

static AUDIT_MEMORY: OnceLock<Mutex<HashMap<String, Vec<String>>>> = OnceLock::new();

fn audit_path(store_key: &str) -> Option<std::path::PathBuf> {
    (!store_key.starts_with("memory:")).then(|| store::chain_dir(store_key).join("vault-audit.log"))
}

/// Writes one audit line and chains it. An error means it was not recorded,
/// and the caller returns nothing.
fn audit(store_key: &str, vault: u128, line: &str) -> Result<(), String> {
    let stamped = format!("{} {line}", clock::now_micros());
    match audit_path(store_key) {
        Some(p) => {
            use std::io::Write;
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)
                    .map_err(|e| format!("55000: the vault audit could not be written ({e}); nothing is returned"))?;
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&p)
                .map_err(|e| format!("55000: the vault audit could not be written ({e}); nothing is returned"))?;
            writeln!(f, "{stamped}")
                .and_then(|()| f.sync_data())
                .map_err(|e| format!("55000: the vault audit could not be written ({e}); nothing is returned"))?;
        }
        None => AUDIT_MEMORY.get_or_init(Default::default).lock().entry(store_key.to_string()).or_default().push(stamped.clone()),
    }
    let item = Item { op: OP_EVENT, ledger: vault, row_ts: clock::now_micros(), width: 0, leaf: store::event_leaf(&stamped) };
    let chain = store::chain(store_key)?;
    let hash = chain.lock().append(0, &[item]).map_err(|e| format!("55000: the vault audit could not be chained ({e})"))?;
    crate::ledger::anchor::note_append(store_key, &chain, hash);
    Ok(())
}

fn audit_lines(store_key: &str) -> Result<Vec<String>, String> {
    match audit_path(store_key) {
        Some(p) => match std::fs::read_to_string(&p) {
            Ok(t) => Ok(t.lines().map(str::to_string).collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.to_string()),
        },
        None => Ok(AUDIT_MEMORY.get_or_init(Default::default).lock().get(store_key).cloned().unwrap_or_default()),
    }
}

// ---- statements -----------------------------------------------------------

/// The newest state of each secret: (newest value row, state now).
fn current(rows: &[VRow]) -> BTreeMap<String, (Option<VRow>, String)> {
    let mut out: BTreeMap<String, (Option<VRow>, String)> = BTreeMap::new();
    for r in rows {
        let e = out.entry(r.name.clone()).or_insert((None, String::new()));
        if r.version > 0 {
            e.0 = Some(r.clone());
        }
        e.1 = r.state.clone();
    }
    out
}

pub fn run(g: &LedgerGuard, store_key: &str, req: &LedgerRequest<'_>) -> Result<LedgerReply, String> {
    secure::harden_process();
    let Some((table, raw)) = req.tables.first() else {
        return Err("42P01: VAULT names no vault".to_string());
    };
    let policy = g.policy(table)?;
    let vault = policy.id;
    let rows = decode(table, raw);
    let w = words(req.sql);
    let up: Vec<String> = w.iter().map(|x| x.to_ascii_uppercase()).collect();
    let at = |i: usize| up.get(i).map(String::as_str).unwrap_or("");
    let role = if req.role.is_empty() { "?" } else { req.role };
    let reply = |lines: Vec<String>, inserts: Vec<String>| LedgerReply { lines, records: Vec::new(), inserts };
    let insert = |name: &str, version: i64, value: &str, state: &str, ct: &str, nb: i64, exp: i64, tags: &str| {
        format!(
            "INSERT INTO {} (name, version, value, state, content_type, not_before, expires_at, tags, created_by, created_at) \
             VALUES ({}, {version}, {}, {}, {}, {nb}, {exp}, {}, {}, {})",
            table.name,
            sql_text(name),
            sql_text(value),
            sql_text(state),
            sql_text(ct),
            sql_text(tags),
            sql_text(role),
            clock::now_micros()
        )
    };
    let secrets = current(&rows);
    match at(2) {
        "SET" => {
            if at(3) != "SECRET" || w.get(5).map(String::as_str) != Some("=") {
                return Err("42601: VAULT v SET SECRET 'name' = '<value>' [CONTENT TYPE …] [EXPIRES …] [NOT BEFORE …] [TAGS …]".to_string());
            }
            let name = quoted(w.get(4), "the secret's name")?;
            valid_name(&name)?;
            if name.starts_with("key:") {
                return Err("42602: names starting key: are the vault's keys (VAULT v CREATE KEY)".to_string());
            }
            // The value, only ever in locked memory from here.
            let value = secure::SecretBuf::from_slice(quoted(w.get(6), "the value")?.as_bytes())?;
            let (mut ct, mut exp, mut nb, mut tags) = (String::new(), 0i64, 0i64, String::new());
            let mut i = 7;
            while i < up.len() {
                match (at(i), at(i + 1)) {
                    ("CONTENT", "TYPE") => {
                        ct = quoted(w.get(i + 2), "a content type")?;
                        i += 3;
                    }
                    ("EXPIRES", _) => {
                        exp = parse_time(&quoted(w.get(i + 1), "a time")?)?;
                        i += 2;
                    }
                    ("NOT", "BEFORE") => {
                        nb = parse_time(&quoted(w.get(i + 2), "a time")?)?;
                        i += 3;
                    }
                    ("TAGS", _) => {
                        tags = quoted(w.get(i + 1), "tags")?;
                        i += 2;
                    }
                    _ => return Err(format!("42601: unexpected {} in VAULT SET", w[i])),
                }
            }
            if secrets.get(&name).is_some_and(|(_, s)| s == "deleted") {
                return Err(format!("55000: secret '{name}' is deleted; RECOVER it or PURGE it first"));
            }
            let version = rows.iter().filter(|r| r.name == name).map(|r| r.version).max().unwrap_or(0) + 1;
            let provider = provider(policy.key_provider.as_deref().ok_or("XX001: the vault has no key provider")?)?;
            let dek = crypto::new_dek()?;
            let wrapped = provider.wrap(&dek)?;
            let sealed = crypto::seal(&dek, value.as_slice(), &aad(vault, &name, version))?;
            drop(dek);
            keyring(store_key, vault)?.lock().add(&name, version, wrapped)?;
            audit(store_key, vault, &format!("SET {} '{name}' v{version} by {role}", table.name))?;
            let b64 = base64::engine::general_purpose::STANDARD.encode(sealed);
            Ok(reply(vec![format!("{name} version {version}")], vec![insert(&name, version, &b64, "active", &ct, nb, exp, &tags)]))
        }
        // Every active, current secret at once (an application loading its
        // configuration): one row each, `{"name":…,"version":…,"value":…}`,
        // each read audited as a GET of its own. Keys (`key:…`) are not
        // secrets and are left out.
        "GET" if at(3) == "ALL" => {
            let prefix = match at(4) {
                "PREFIX" => Some(quoted(w.get(5), "a name prefix")?),
                "" => None,
                other => return Err(format!("42601: unexpected {other} in VAULT GET ALL")),
            };
            let now = clock::now_micros();
            let provider = provider(policy.key_provider.as_deref().ok_or("XX001: the vault has no key provider")?)?;
            let ring = keyring(store_key, vault)?;
            let mut out = Vec::new();
            for (name, (newest, state)) in &secrets {
                if name.starts_with("key:") || state != "active" || prefix.as_ref().is_some_and(|p| !name.starts_with(p.as_str())) {
                    continue;
                }
                let Some(row) = newest else { continue };
                if (row.expires_at != 0 && now >= row.expires_at) || (row.not_before != 0 && now < row.not_before) {
                    continue;
                }
                let Some(wrapped) = ring.lock().keys.get(&(name.clone(), row.version)).cloned() else { continue };
                let dek = provider.unwrap(&wrapped)?;
                let sealed = base64::engine::general_purpose::STANDARD
                    .decode(row.value.as_bytes())
                    .map_err(|_| "XX001: a sealed value is not base64".to_string())?;
                let plain = crypto::open(&dek, &sealed, &aad(vault, name, row.version))?;
                let text = String::from_utf8(plain.to_vec()).map_err(|_| format!("22021: secret '{name}' is not UTF-8 text"))?;
                audit(store_key, vault, &format!("GET {} '{name}' v{} by {role} (GET ALL)", table.name, row.version))?;
                out.push(format!("{{\"name\":{},\"version\":{},\"value\":{}}}", json_str(name), row.version, json_str(&text)));
            }
            Ok(reply(out, Vec::new()))
        }
        "GET" => {
            if at(3) != "SECRET" {
                return Err("42601: VAULT v GET SECRET 'name' [VERSION n]".to_string());
            }
            let name = quoted(w.get(4), "the secret's name")?;
            let wanted: Option<i64> = match at(5) {
                "VERSION" => Some(at(6).parse().map_err(|_| "42601: VERSION takes a number")?),
                "" => None,
                other => return Err(format!("42601: unexpected {other} in VAULT GET")),
            };
            let refuse = |why: &str| -> Result<LedgerReply, String> {
                audit(store_key, vault, &format!("GET {} '{name}' by {role} REFUSED: {why}", table.name))?;
                Err(format!("55000: secret '{name}': {why}"))
            };
            let Some((newest, state)) = secrets.get(&name) else {
                return refuse("no such secret");
            };
            if state != "active" {
                return refuse(&format!("it is {state}"));
            }
            let row = match wanted {
                None => newest.clone(),
                Some(v) => rows.iter().rev().find(|r| r.name == name && r.version == v).cloned(),
            };
            let Some(row) = row else { return refuse("no such version") };
            let now = clock::now_micros();
            if row.expires_at != 0 && now >= row.expires_at {
                return refuse("it has expired");
            }
            if row.not_before != 0 && now < row.not_before {
                return refuse("it is not valid yet");
            }
            let Some(wrapped) = keyring(store_key, vault)?.lock().keys.get(&(name.clone(), row.version)).cloned() else {
                return refuse("its key was purged; it can no longer be read");
            };
            let provider = provider(policy.key_provider.as_deref().ok_or("XX001: the vault has no key provider")?)?;
            let dek = provider.unwrap(&wrapped)?;
            let sealed = base64::engine::general_purpose::STANDARD
                .decode(row.value.as_bytes())
                .map_err(|_| "XX001: a sealed value is not base64".to_string())?;
            let plain = crypto::open(&dek, &sealed, &aad(vault, &name, row.version))?;
            let plain = secure::SecretBuf::from_slice(&plain)?;
            // Recorded before anything is returned: no audit, no value.
            audit(store_key, vault, &format!("GET {} '{name}' v{} by {role}", table.name, row.version))?;
            let text = String::from_utf8(plain.as_slice().to_vec()).map_err(|_| "22021: the secret is not UTF-8 text".to_string())?;
            Ok(reply(vec![text], Vec::new()))
        }
        "LIST" => {
            let mut lines = Vec::new();
            for (name, (newest, state)) in &secrets {
                let v = newest.as_ref();
                lines.push(format!(
                    "{name} v{} {state}{}{}",
                    v.map_or(0, |r| r.version),
                    v.filter(|r| !r.content_type.is_empty()).map_or_else(String::new, |r| format!(" type={}", r.content_type)),
                    v.filter(|r| r.expires_at != 0).map_or_else(String::new, |r| format!(" expires_us={}", r.expires_at)),
                ));
            }
            Ok(reply(lines, Vec::new()))
        }
        "VERSIONS" => {
            let name = quoted(w.get(4), "the secret's name")?;
            let ring = keyring(store_key, vault)?;
            let ring = ring.lock();
            let lines = rows
                .iter()
                .filter(|r| r.name == name)
                .map(|r| {
                    if r.version > 0 {
                        format!(
                            "v{} set by {} at {}{}",
                            r.version,
                            r.created_by,
                            r.created_at,
                            if ring.keys.contains_key(&(name.clone(), r.version)) { "" } else { " (purged)" }
                        )
                    } else {
                        format!("{} by {} at {}", r.state, r.created_by, r.created_at)
                    }
                })
                .collect();
            Ok(reply(lines, Vec::new()))
        }
        verb @ ("DISABLE" | "ENABLE" | "DELETE" | "RECOVER") => {
            let name = quoted(w.get(4), "the secret's name")?;
            let Some((_, state)) = secrets.get(&name) else {
                return Err(format!("42704: no secret '{name}'"));
            };
            let (from_ok, to) = match verb {
                "DISABLE" => (state == "active", "disabled"),
                "ENABLE" => (state == "disabled", "active"),
                "DELETE" => (state != "deleted", "deleted"),
                _ => (state == "deleted", "active"),
            };
            if !from_ok {
                return Err(format!("55000: secret '{name}' is {state}; {verb} does not apply"));
            }
            audit(store_key, vault, &format!("{verb} {} '{name}' by {role}", table.name))?;
            Ok(reply(vec![format!("{name} {to}")], vec![insert(&name, 0, "", to, "", 0, 0, "")]))
        }
        "PURGE" => {
            let name = quoted(w.get(4), "the secret's name")?;
            let Some((_, state)) = secrets.get(&name) else {
                return Err(format!("42704: no secret '{name}'"));
            };
            if state != "deleted" {
                return Err(format!("55000: secret '{name}' is {state}; only a deleted secret is purged"));
            }
            let deleted_at = rows.iter().rev().find(|r| r.name == name && r.version == 0).map_or(i64::MAX, |r| r.ts);
            if !clock::retention_passed(deleted_at, policy.retention_us) {
                return Err(format!(
                    "55000: secret '{name}' is purge-protected for {} s after its deletion",
                    policy.retention_us / 1_000_000
                ));
            }
            let ring = keyring(store_key, vault)?;
            let mut ring = ring.lock();
            let before = ring.keys.len();
            ring.keys.retain(|(n, _), _| n != &name);
            let gone = before - ring.keys.len();
            ring.rewrite()?;
            audit(store_key, vault, &format!("PURGE {} '{name}' by {role}: {gone} key(s) destroyed", table.name))?;
            Ok(reply(vec![format!("{name} purged: {gone} version(s) can never be decrypted again")], Vec::new()))
        }
        "ROTATE" => {
            if at(3) != "KEY" {
                return Err("42601: VAULT v ROTATE KEY [TO '<provider>']".to_string());
            }
            let old_uri = policy.key_provider.clone().ok_or("XX001: the vault has no key provider")?;
            let new_uri = match at(4) {
                "TO" => quoted(w.get(5), "a key provider")?,
                _ => old_uri.clone(),
            };
            let old = provider(&old_uri)?;
            let new = provider(&new_uri)?;
            let ring = keyring(store_key, vault)?;
            let mut ring = ring.lock();
            let mut rewrapped = BTreeMap::new();
            for (k, wrapped) in &ring.keys {
                let dek = old.unwrap(wrapped)?;
                rewrapped.insert(k.clone(), new.wrap(&dek)?);
            }
            let n = rewrapped.len();
            ring.keys = rewrapped;
            ring.rewrite()?;
            audit(store_key, vault, &format!("ROTATE KEY {} by {role}: {n} key(s) rewrapped under {}", table.name, new.describe()))?;
            let mut out = reply(vec![format!("{n} data key(s) rewrapped under {}", new.describe())], Vec::new());
            if new_uri != old_uri {
                out.records.push((
                    table.name.clone(),
                    crate::ledger::policy::record(&format!("v1\nkey_provider {new_uri}\n")),
                ));
            }
            Ok(out)
        }
        "AUDIT" => {
            let only: Option<String> = if at(3) == "SECRET" { Some(quoted(w.get(4), "the secret's name")?) } else { None };
            let chain = store::chain(store_key)?;
            let read = chain.lock().reread()?;
            let chained: std::collections::HashSet<store::Hash> =
                read.entries.iter().flat_map(|e| e.items.iter()).filter(|i| i.op == OP_EVENT && i.ledger == vault).map(|i| i.leaf).collect();
            let mut lines = Vec::new();
            for l in audit_lines(store_key)? {
                if !l.contains(&format!(" {} ", table.name)) {
                    continue;
                }
                if let Some(n) = &only {
                    if !l.contains(&format!("'{n}'")) {
                        continue;
                    }
                }
                let ok = chained.contains(&store::event_leaf(&l));
                lines.push(format!("{} {l}", if ok { "chained  " } else { "UNCHAINED" }));
            }
            Ok(reply(lines, Vec::new()))
        }
        // Keys used in place (Key Vault's "keys"): created here, used by
        // the server, never returned. Stored as a secret named `key:<name>`
        // whose content type is the key's type and whose material is a data
        // key in the keyring, wrapped like every other.
        "CREATE" if at(3) == "KEY" => {
            let name = quoted(w.get(4), "the key's name")?;
            valid_name(&name)?;
            if name.contains(':') {
                return Err("42602: a key's name has no ':'".to_string());
            }
            let kind = match (at(5), w.get(6).and_then(|t| t.strip_prefix('\''))) {
                ("TYPE", Some(t)) => t.to_ascii_lowercase(),
                _ => "aes256-gcm".to_string(),
            };
            if !matches!(kind.as_str(), "aes256-gcm" | "mldsa65") {
                return Err(format!("22023: key type {kind:?}: aes256-gcm or mldsa65 (feature `pqc`)"));
            }
            if cfg!(feature = "fips") && kind == "mldsa65" {
                return Err("0A000: mldsa65 keys are not offered in FIPS mode: this ML-DSA is not the validated module's".to_string());
            }
            #[cfg(not(feature = "pqc"))]
            if kind == "mldsa65" {
                return Err("0A000: mldsa65 keys need the `pqc` feature".to_string());
            }
            let full = format!("key:{name}");
            if secrets.contains_key(&full) {
                return Err(format!("42710: key '{name}' exists"));
            }
            let provider = provider(policy.key_provider.as_deref().ok_or("XX001: the vault has no key provider")?)?;
            let material = crypto::new_dek()?;
            keyring(store_key, vault)?.lock().add(&full, 1, provider.wrap(&material)?)?;
            audit(store_key, vault, &format!("CREATE KEY {} '{name}' {kind} by {role}", table.name))?;
            Ok(reply(vec![format!("key {name} ({kind})")], vec![insert(&full, 1, "", "active", &kind, 0, 0, "")]))
        }
        op @ ("ENCRYPT" | "DECRYPT" | "SIGN" | "VERIFY" | "PUBLIC") if at(3) == "KEY" => {
            let name = quoted(w.get(4), "the key's name")?;
            let full = format!("key:{name}");
            let Some((Some(row), state)) = secrets.get(&full) else {
                return Err(format!("42704: no key '{name}'"));
            };
            if state != "active" {
                return Err(format!("55000: key '{name}' is {state}"));
            }
            let kind = row.content_type.clone();
            let wrapped = keyring(store_key, vault)?
                .lock()
                .keys
                .get(&(full.clone(), 1))
                .cloned()
                .ok_or_else(|| format!("55000: key '{name}' was purged"))?;
            let provider = provider(policy.key_provider.as_deref().ok_or("XX001: the vault has no key provider")?)?;
            let material = provider.unwrap(&wrapped)?;
            let value = || quoted(w.get(6), "a value").map(|v| (v, at(5) == "VALUE"));
            let aad_k = aad(vault, &full, 1);
            let out = match (op, kind.as_str()) {
                ("ENCRYPT", "aes256-gcm") => {
                    let (v, _) = value()?;
                    base64::engine::general_purpose::STANDARD.encode(crypto::seal(&material, v.as_bytes(), &aad_k)?)
                }
                ("DECRYPT", "aes256-gcm") => {
                    let (v, _) = value()?;
                    let sealed = base64::engine::general_purpose::STANDARD
                        .decode(v.trim().as_bytes())
                        .map_err(|_| "22023: DECRYPT takes base64 from ENCRYPT".to_string())?;
                    let plain = crypto::open(&material, &sealed, &aad_k)?;
                    String::from_utf8(plain.to_vec()).map_err(|_| "22021: the plaintext is not UTF-8 text".to_string())?
                }
                #[cfg(feature = "pqc")]
                ("SIGN", "mldsa65") => {
                    let (v, _) = value()?;
                    crate::crypto::pqc_sign::sign(&material, v.as_bytes())
                }
                #[cfg(feature = "pqc")]
                ("VERIFY", "mldsa65") => {
                    let (v, _) = value()?;
                    let sig = match (at(7), w.get(8).and_then(|x| x.strip_prefix('\''))) {
                        ("SIGNATURE", Some(sig)) => sig.to_string(),
                        _ => return Err("42601: VAULT v VERIFY KEY 'k' VALUE '…' SIGNATURE '<hex>'".to_string()),
                    };
                    crate::crypto::pqc_sign::verify(&material, v.as_bytes(), &sig).to_string()
                }
                #[cfg(feature = "pqc")]
                ("PUBLIC", "mldsa65") => crate::crypto::pqc_sign::public(&material),
                (op, kind) => return Err(format!("0A000: {op} does not apply to a {kind} key")),
            };
            // Decrypting hands data back; everything else is recorded too.
            audit(store_key, vault, &format!("{op} KEY {} '{name}' by {role}", table.name))?;
            Ok(reply(vec![out], Vec::new()))
        }
        _ => Err("42601: VAULT v SET | GET | LIST | VERSIONS | DISABLE | ENABLE | DELETE | RECOVER | PURGE | ROTATE KEY | AUDIT | CREATE KEY | ENCRYPT | DECRYPT | SIGN | VERIFY | PUBLIC KEY".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_words() {
        assert_eq!(
            words("VAULT v SET SECRET 'db' = 'p''w=1' EXPIRES '2027-01-01'"),
            vec!["VAULT", "v", "SET", "SECRET", "'db", "=", "'p'w=1", "EXPIRES", "'2027-01-01"]
        );
    }

    #[test]
    fn json_strings_escape() {
        assert_eq!(json_str("a\"b\\c\nd\u{1}"), "\"a\\\"b\\\\c\\nd\\u0001\"");
    }

    #[test]
    fn times_and_names() {
        assert_eq!(parse_time("1970-01-02").unwrap(), 86_400_000_000);
        assert_eq!(parse_time("1970-01-01T00:00:01Z").unwrap(), 1_000_000);
        assert!(parse_time("tomorrow").is_err());
        assert!(valid_name("db/password-1").is_ok());
        assert!(valid_name("x y").is_err());
        assert!(valid_name("").is_err());
    }

    #[test]
    fn a_sealed_value_is_bound_to_its_name_and_version() {
        let dek = crypto::new_dek().unwrap();
        let s = crypto::seal(&dek, b"v", &aad(1, "a", 1)).unwrap();
        assert!(crypto::open(&dek, &s, &aad(1, "a", 1)).is_ok());
        assert!(crypto::open(&dek, &s, &aad(1, "b", 1)).is_err());
        assert!(crypto::open(&dek, &s, &aad(1, "a", 2)).is_err());
        assert!(crypto::open(&dek, &s, &aad(2, "a", 1)).is_err());
    }
}
