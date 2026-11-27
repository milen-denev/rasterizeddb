//! Anchors: copies of a chain's head kept where the database host cannot
//! rewrite them (docs/ledger-and-vault-plan.md §3.4, phase L3).
//!
//! The chain catches any edit of one entry, but not someone who rewrites the
//! data, the chain and every hash together. An anchor is an outside copy of
//! "entry N had hash H at time T". Once that copy exists, the chain can no
//! longer be recomputed or cut back unnoticed.
//!
//! Sinks:
//!
//! - **`dir:<path>`**: a file per anchor under `<path>/<store>/`, written
//!   once and synced, never overwritten. It belongs on another disk, or a
//!   mount this host can only append to.
//! - **`s3://bucket/prefix`** (feature `ledger_s3`): an object per anchor
//!   under S3 Object Lock in COMPLIANCE mode, which not even the bucket's
//!   owner can delete before the retention date. The response's `Date`
//!   becomes the trusted clock's witness.
//!
//! Sinks come from `RDB_LEDGER_ANCHORS` (comma-separated), the enterprise
//! configuration's `ledger_anchor` lines, and each ledger table's
//! `ANCHOR TO`.
//!
//! **Async by default.** A thread anchors each store's head about once a
//! second. A commit does not wait, but the unanchored window is bounded by
//! `RDB_LEDGER_MAX_UNANCHORED_SECS` (default 60). Past it, ledger writes are
//! refused (`refuse_writes`, the default) or go on with an error in the log
//! every time (`RDB_LEDGER_ON_ANCHOR_FAILURE=continue`). With
//! `RDB_LEDGER_ANCHOR_MODE=sync`, every commit's head is anchored before the
//! commit returns.
//!
//! An anchor's text, one line per field:
//!
//! ```text
//! rdb-anchor-v1
//! store <store>
//! seq <n>
//! head <hex>
//! time <micros>
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::store::{self, Chain, Hash, Item, OP_EVENT};

/// One store's anchoring state.
#[derive(Default)]
struct State {
    sinks: Vec<String>,
    /// The newest appended head not yet anchored everywhere: seq, hash, when.
    pending: Option<(u64, Hash, Instant)>,
    /// The newest anchored seq per sink.
    anchored: HashMap<String, u64>,
    last_error: Option<String>,
}

static STATES: OnceLock<Mutex<HashMap<String, State>>> = OnceLock::new();
static GLOBAL_SINKS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static WORKER: OnceLock<()> = OnceLock::new();

fn states() -> &'static Mutex<HashMap<String, State>> {
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn global_sinks() -> &'static Mutex<Vec<String>> {
    GLOBAL_SINKS.get_or_init(|| {
        let from_env: Vec<String> = std::env::var("RDB_LEDGER_ANCHORS")
            .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
            .unwrap_or_default();
        Mutex::new(from_env)
    })
}

/// Adds a sink every store anchors to (the enterprise configuration's
/// `ledger_anchor` lines).
pub fn add_global_sink(sink: String) {
    let mut g = global_sinks().lock();
    if !g.contains(&sink) {
        g.push(sink);
    }
}

/// Adds sinks a ledger table's policy names to its store's set.
pub fn add_sinks(store_key: &str, sinks: &[String]) {
    if sinks.is_empty() {
        return;
    }
    let mut s = states().lock();
    let st = s.entry(store_key.to_string()).or_default();
    for sink in sinks {
        if !st.sinks.contains(sink) {
            st.sinks.push(sink.clone());
        }
    }
}

fn sinks_of(store_key: &str) -> Vec<String> {
    let mut all = global_sinks().lock().clone();
    if let Some(st) = states().lock().get(store_key) {
        for s in &st.sinks {
            if !all.contains(s) {
                all.push(s.clone());
            }
        }
    }
    all
}

fn sync_mode() -> bool {
    std::env::var("RDB_LEDGER_ANCHOR_MODE").is_ok_and(|v| v.eq_ignore_ascii_case("sync"))
}

fn max_unanchored() -> Duration {
    Duration::from_secs(
        std::env::var("RDB_LEDGER_MAX_UNANCHORED_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60),
    )
}

fn refuse_on_failure() -> bool {
    !std::env::var("RDB_LEDGER_ON_ANCHOR_FAILURE").is_ok_and(|v| v.eq_ignore_ascii_case("continue"))
}

/// Refuses ledger writes to `store_key` once its head has gone unanchored
/// longer than the bound and the policy is to stop.
pub fn admit_writes(store_key: &str) -> Result<(), String> {
    if sinks_of(store_key).is_empty() {
        return Ok(());
    }
    let s = states().lock();
    let Some(st) = s.get(store_key) else { return Ok(()) };
    let Some((seq, _, since)) = st.pending else { return Ok(()) };
    if since.elapsed() <= max_unanchored() {
        return Ok(());
    }
    let why = format!(
        "the ledger head (entry {seq}) has not been anchored for {} s{}",
        since.elapsed().as_secs(),
        st.last_error.as_ref().map_or_else(String::new, |e| format!(": {e}"))
    );
    if refuse_on_failure() {
        Err(format!("55000: {why}; ledger writes stop until an anchor succeeds"))
    } else {
        log::error!("{why}; writing on (RDB_LEDGER_ON_ANCHOR_FAILURE=continue)");
        Ok(())
    }
}

/// Notes a new head of `store_key`'s chain, anchoring it now in sync mode.
pub fn note_append(store_key: &str, chain: &Arc<Mutex<Chain>>, hash: Hash) {
    let seq = chain.lock().seq;
    {
        let mut s = states().lock();
        let st = s.entry(store_key.to_string()).or_default();
        let since = st.pending.map_or_else(Instant::now, |(_, _, t)| t);
        st.pending = Some((seq, hash, since));
    }
    if sync_mode() {
        anchor_now(store_key);
    } else {
        start_worker();
    }
}

fn start_worker() {
    WORKER.get_or_init(|| {
        let _ = std::thread::Builder::new().name("rdb-ledger-anchor".into()).spawn(|| loop {
            std::thread::sleep(Duration::from_secs(1));
            let keys: Vec<String> = states().lock().keys().cloned().collect();
            for k in keys {
                anchor_now(&k);
            }
        });
    });
}

fn anchor_text(store_key: &str, seq: u64, head: &Hash, time: i64) -> String {
    let text = format!("rdb-anchor-v1\nstore {store_key}\nseq {seq}\nhead {}\ntime {time}\n", store::hex(head));
    // Signed (ML-DSA-65) when a signing key is set: the signature covers
    // every line above it.
    #[cfg(feature = "pqc")]
    if let Some(sig) = super::sign::sign(&text) {
        return text + &sig;
    }
    text
}

/// Whether an anchor's signature, if it has one, holds: `Some(false)` for a
/// bad one, `None` for none or no key to check it with.
fn signature_holds(text: &str) -> Option<bool> {
    let at = text.find("sig-mldsa65 ")?;
    #[cfg(feature = "pqc")]
    {
        let sig = text[at + "sig-mldsa65 ".len()..].trim();
        super::sign::verify(&text[..at], sig)
    }
    #[cfg(not(feature = "pqc"))]
    {
        let _ = at;
        None
    }
}

/// Anchors `store_key`'s pending head to every sink that does not have it.
fn anchor_now(store_key: &str) {
    let Some((seq, head, _)) = states().lock().get(store_key).and_then(|s| s.pending) else { return };
    let sinks = sinks_of(store_key);
    if sinks.is_empty() {
        return;
    }
    let time = super::clock::now_micros();
    let text = anchor_text(store_key, seq, &head, time);
    let mut failed: Option<String> = None;
    for sink in &sinks {
        let done = states().lock().get(store_key).and_then(|s| s.anchored.get(sink).copied()).unwrap_or(0);
        if done >= seq {
            continue;
        }
        match write(sink, store_key, seq, &head, &text) {
            Ok(()) => {
                if let Some(st) = states().lock().get_mut(store_key) {
                    st.anchored.insert(sink.clone(), seq);
                }
            }
            Err(e) => failed = Some(format!("{sink}: {e}")),
        }
    }
    let mut s = states().lock();
    if let Some(st) = s.get_mut(store_key) {
        st.last_error = failed.clone();
        let everywhere = sinks.iter().all(|k| st.anchored.get(k).copied().unwrap_or(0) >= seq);
        if everywhere && st.pending.is_some_and(|(p, _, _)| p == seq) {
            st.pending = None;
        }
    }
    if let Some(e) = failed {
        log::warn!("ledger anchor of {store_key} entry {seq} failed: {e}");
    }
}

fn store_dir_name(store_key: &str) -> String {
    store_key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn write(sink: &str, store_key: &str, seq: u64, head: &Hash, text: &str) -> Result<(), String> {
    let name = format!("anchor-{seq:020}-{}", &store::hex(head)[..16]);
    if let Some(path) = sink.strip_prefix("dir:") {
        use std::io::Write;
        let dir = PathBuf::from(path).join(store_dir_name(store_key));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let file = dir.join(&name);
        // Never over an existing anchor.
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).open(&file).map_err(|e| e.to_string())?;
        f.write_all(text.as_bytes()).and_then(|()| f.sync_all()).map_err(|e| e.to_string())?;
        if let Ok(d) = std::fs::File::open(&dir) {
            let _ = d.sync_all();
        }
        return Ok(());
    }
    if sink.starts_with("s3://") {
        return s3::put(sink, &format!("{}/{name}", store_dir_name(store_key)), text.as_bytes());
    }
    Err(format!("unknown anchor sink {sink}"))
}

/// Every anchor `sink` holds for `store_key`: (seq, head hex, time).
fn read_sink(sink: &str, store_key: &str) -> Result<Vec<(u64, String, i64)>, String> {
    let texts: Vec<String> = if let Some(path) = sink.strip_prefix("dir:") {
        let dir = PathBuf::from(path).join(store_dir_name(store_key));
        match std::fs::read_dir(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.to_string()),
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("anchor-"))
                .filter_map(|e| std::fs::read_to_string(e.path()).ok())
                .collect(),
        }
    } else if sink.starts_with("s3://") {
        s3::read_all(sink, &store_dir_name(store_key))?
    } else {
        return Err(format!("unknown anchor sink {sink}"));
    };
    // An anchor whose signature does not verify is not an anchor this
    // database wrote: it is reported as such rather than read.
    let mut out = Vec::new();
    for t in &texts {
        if signature_holds(t) == Some(false) {
            return Err("an anchor's ML-DSA signature does not verify: it was not written by this database's key".to_string());
        }
        out.extend(parse(t));
    }
    Ok(out)
}

fn parse(text: &str) -> Option<(u64, String, i64)> {
    let mut lines = text.lines();
    if lines.next()? != "rdb-anchor-v1" {
        return None;
    }
    let (mut seq, mut head, mut time) = (None, None, None);
    for l in lines {
        match l.split_once(' ') {
            Some(("seq", v)) => seq = v.parse().ok(),
            Some(("head", v)) => head = Some(v.to_string()),
            Some(("time", v)) => time = v.parse().ok(),
            _ => {}
        }
    }
    Some((seq?, head?, time?))
}

/// What `VERIFY LEDGER` says about each sink: the newest anchor it holds,
/// and whether the chain still has that entry with that hash.
pub fn verify_lines(store_key: &str, read: &store::ChainRead) -> Vec<String> {
    let mut lines = Vec::new();
    let sinks = sinks_of(store_key);
    if sinks.is_empty() {
        lines.push(
            "WARNING   no anchors: the chain proves entries were not edited one by one, not that the whole \
             chain was not rewritten (set ANCHOR TO, RDB_LEDGER_ANCHORS or ledger_anchor)"
                .to_string(),
        );
        return lines;
    }
    let acknowledged = acknowledged_heads(read);
    for sink in sinks {
        match read_sink(&sink, store_key) {
            Err(e) => lines.push(format!("UNKNOWN   anchor {sink}: could not be read: {e}")),
            Ok(anchors) => {
                let Some(newest) = anchors.iter().max_by_key(|a| (a.2, a.0)) else {
                    lines.push(format!("WARNING   anchor {sink}: holds no anchor yet"));
                    continue;
                };
                // Every anchor must name an entry the chain still has, unless a
                // rollback past it was acknowledged.
                let mut bad = Vec::new();
                for (seq, head, _) in &anchors {
                    let ok = read.entries.get((*seq as usize).wrapping_sub(1)).is_some_and(|e| store::hex(&e.hash) == *head);
                    if !ok && !acknowledged.contains(head) {
                        bad.push(*seq);
                    }
                }
                if bad.is_empty() {
                    lines.push(format!("OK        anchor {sink}: newest entry {} matches the chain", newest.0));
                } else {
                    bad.sort_unstable();
                    lines.push(format!(
                        "TAMPERED  anchor {sink}: entries {:?} were anchored with other hashes or are gone from the \
                         chain: it was rewritten or rolled back (ACKNOWLEDGE LEDGER ROLLBACK records a restore)",
                        &bad[..bad.len().min(10)]
                    ));
                }
            }
        }
    }
    lines
}

/// The anchored heads an acknowledged rollback names.
fn acknowledged_heads(read: &store::ChainRead) -> HashSet<String> {
    let mut out = HashSet::new();
    let acks = ACKS.get_or_init(|| Mutex::new(HashMap::new())).lock();
    for e in &read.entries {
        for i in e.items.iter().filter(|i| i.op == OP_EVENT) {
            if let Some(heads) = acks.get(&i.leaf) {
                out.extend(heads.iter().cloned());
            }
        }
    }
    out
}

/// Acknowledged rollbacks' texts by their event leaf, so `VERIFY` can tell
/// which anchors they cover. Kept beside the chain as `acks.log`.
static ACKS: OnceLock<Mutex<HashMap<Hash, Vec<String>>>> = OnceLock::new();

fn acks_path(store_key: &str) -> Option<PathBuf> {
    if store_key.starts_with("memory:") { None } else { Some(store::chain_dir(store_key).join("acks.log")) }
}

/// Loads `store_key`'s acknowledged rollbacks (each line `<leaf hex> <head hex>…`).
pub fn load_acks(store_key: &str) {
    let Some(path) = acks_path(store_key) else { return };
    let Ok(text) = std::fs::read_to_string(path) else { return };
    let mut acks = ACKS.get_or_init(|| Mutex::new(HashMap::new())).lock();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(leaf) = parts.next().and_then(unhex) else { continue };
        acks.insert(leaf, parts.map(str::to_string).collect());
    }
}

fn unhex(s: &str) -> Option<Hash> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// `ACKNOWLEDGE LEDGER ROLLBACK`: records, in the chain itself, that the
/// anchors past its head are a restore's and not tampering, with who said
/// so and why. Verification then accepts those anchors; the event stays in
/// the chain, and is itself anchored, for good.
pub fn acknowledge_rollback(store_key: &str, reason: &str, role: &str) -> Result<Vec<String>, String> {
    let chain = store::chain(store_key)?;
    let read = chain.lock().reread()?;
    let mut orphaned = Vec::new();
    for sink in sinks_of(store_key) {
        for (seq, head, _) in read_sink(&sink, store_key)? {
            let ok = read.entries.get((seq as usize).wrapping_sub(1)).is_some_and(|e| store::hex(&e.hash) == head);
            if !ok && !orphaned.contains(&head) {
                orphaned.push(head);
            }
        }
    }
    if orphaned.is_empty() {
        return Ok(vec!["nothing to acknowledge: every anchor matches the chain".to_string()]);
    }
    let text = format!(
        "rollback acknowledged by {}: {} anchored head(s) past the restored chain; reason: {reason}",
        if role.is_empty() { "?" } else { role },
        orphaned.len()
    );
    let leaf = store::event_leaf(&text);
    if let Some(path) = acks_path(store_key) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|e| e.to_string())?;
        writeln!(f, "{} {}", store::hex(&leaf), orphaned.join(" ")).and_then(|()| f.sync_all()).map_err(|e| e.to_string())?;
    }
    ACKS.get_or_init(|| Mutex::new(HashMap::new())).lock().insert(leaf, orphaned.clone());
    let item = Item { op: OP_EVENT, ledger: 0, row_ts: super::clock::now_micros(), width: 0, leaf };
    let hash = chain.lock().append(0, &[item])?;
    note_append(store_key, &chain, hash);
    log::warn!("ledger {store_key}: {text}");
    Ok(vec![text])
}

/// The newest seq every sink of `store_key` has anchored, as this process
/// wrote them (`None` before the first, or with no sinks).
pub fn anchored_seq(store_key: &str) -> Option<u64> {
    let sinks = sinks_of(store_key);
    if sinks.is_empty() {
        return None;
    }
    let s = states().lock();
    let st = s.get(store_key)?;
    sinks.iter().map(|k| st.anchored.get(k).copied().unwrap_or(0)).min()
}

/// What `SHOW LEDGER` says about anchoring.
pub fn show_lines(store_key: &str) -> Vec<String> {
    let sinks = sinks_of(store_key);
    if sinks.is_empty() {
        return vec!["anchors: none".to_string()];
    }
    let s = states().lock();
    let st = s.get(store_key);
    #[cfg(feature = "pqc")]
    let signed = super::sign::public_hex().map_or_else(String::new, |k| format!(", signed ML-DSA-65 (verify key {k})"));
    #[cfg(not(feature = "pqc"))]
    let signed = String::new();
    let mut lines = vec![format!(
        "anchors: {} ({}{signed}), unanchored: {}",
        sinks.join(" "),
        if sync_mode() { "sync" } else { "async" },
        st.and_then(|st| st.pending).map_or_else(
            || "none".to_string(),
            |(seq, _, since)| format!("entry {seq} for {} s", since.elapsed().as_secs())
        )
    )];
    if let Some(e) = st.and_then(|st| st.last_error.as_ref()) {
        lines.push(format!("last anchor error: {e}"));
    }
    lines
}

#[cfg(feature = "ledger_s3")]
mod s3 {
    use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint, S3Client, S3Config};

    /// How long an anchor object is locked: `RDB_LEDGER_ANCHOR_LOCK_DAYS`,
    /// ten years by default.
    fn lock_days() -> i64 {
        std::env::var("RDB_LEDGER_ANCHOR_LOCK_DAYS").ok().and_then(|v| v.parse().ok()).unwrap_or(3660)
    }

    fn client(sink: &str) -> Result<(S3Client, String), String> {
        let rest = sink.strip_prefix("s3://").ok_or("not an s3:// sink")?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let region = std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION")).unwrap_or_else(|_| "us-east-1".into());
        let endpoint_url = std::env::var("RDB_LEDGER_S3_ENDPOINT")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .unwrap_or_else(|_| format!("https://s3.{region}.amazonaws.com"));
        let path_style = !endpoint_url.contains("amazonaws.com");
        let endpoint = Endpoint::parse(&endpoint_url)?;
        let credentials = Credentials::from_env().ok_or("no AWS credentials in the environment for the anchor sink")?;
        let config = S3Config { endpoint, region, bucket: bucket.to_string(), credentials, path_style, max_attempts: 3 };
        let client = S3Client::new(config).map_err(|e| e.to_string())?;
        Ok((client, prefix.trim_matches('/').to_string()))
    }

    fn runtime() -> Result<tokio::runtime::Runtime, String> {
        tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())
    }

    pub fn put(sink: &str, key: &str, body: &[u8]) -> Result<(), String> {
        let (client, prefix) = client(sink)?;
        let full = if prefix.is_empty() { key.to_string() } else { format!("{prefix}/{key}") };
        let until = super::super::clock::rfc3339_days_from_now(lock_days());
        let date = runtime()?.block_on(client.put_object_locked(&full, body, &until)).map_err(|e| e.to_string())?;
        if let Some(micros) = date.as_deref().and_then(super::super::clock::parse_http_date) {
            super::super::clock::witness(micros);
        }
        Ok(())
    }

    pub fn read_all(sink: &str, dir: &str) -> Result<Vec<String>, String> {
        let (client, prefix) = client(sink)?;
        let full = if prefix.is_empty() { format!("{dir}/") } else { format!("{prefix}/{dir}/") };
        let rt = runtime()?;
        let keys = rt.block_on(client.list_prefix(&full)).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for (key, _) in keys {
            let bytes = rt.block_on(client.get_object(&key)).map_err(|e| e.to_string())?;
            out.push(String::from_utf8_lossy(&bytes).into_owned());
        }
        Ok(out)
    }
}

#[cfg(not(feature = "ledger_s3"))]
mod s3 {
    pub fn put(sink: &str, _key: &str, _body: &[u8]) -> Result<(), String> {
        Err(format!("{sink}: this build has no S3 anchors (feature `ledger_s3`)"))
    }

    pub fn read_all(sink: &str, _dir: &str) -> Result<Vec<String>, String> {
        Err(format!("{sink}: this build has no S3 anchors (feature `ledger_s3`)"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dir_anchor_round_trips_and_a_rewritten_chain_is_caught() {
        let dir = std::env::temp_dir().join(format!("rdb-anchor-test-{}", std::process::id()));
        let sink = format!("dir:{}", dir.display());
        let key = "memory:anchor-test";
        add_sinks(key, &[sink.clone()]);
        let chain = store::chain(key).unwrap();
        let item = Item { op: super::super::store::OP_INSERT, ledger: 1, row_ts: 1, width: 1, leaf: [7; 32] };
        let h = chain.lock().append(1, &[item]).unwrap();
        {
            let mut s = states().lock();
            s.get_mut(key).unwrap().pending = Some((1, h, Instant::now()));
        }
        anchor_now(key);
        let read = chain.lock().reread().unwrap();
        assert!(verify_lines(key, &read).iter().any(|l| l.starts_with("OK        anchor")));
        // A different chain under the same store: the anchor no longer matches.
        let other = store::read(&[]);
        assert!(verify_lines(key, &other).iter().any(|l| l.starts_with("TAMPERED")));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
