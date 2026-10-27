//! Startup settings saved for the next start: `rasterizeddb.conf` beside the
//! databases (`--location`).
//!
//! `SET CONFIG` on a key that is read once at start-up cannot change the
//! running server, so it saves the value here instead, and the next start
//! applies it. The file is `section.key = value` lines; `#` starts a comment.
//! It is the server's, not a database's: one file however many databases
//! the location holds.
//!
//! **Precedence**, as PostgreSQL's: a flag given on the command line wins over
//! a saved value, and a saved value over the default. Which of the three a
//! running value came from is `SHOW CONFIG`'s `source` column, so a value saved
//! under a flag that overrides it is visible as such rather than silently
//! never taking effect.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use parking_lot::Mutex;

/// The file's name, inside `--location`.
pub const FILE_NAME: &str = "rasterizeddb.conf";

/// Where a running value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Default,
    CommandLine,
    Saved,
}

impl Source {
    pub fn name(self) -> &'static str {
        match self {
            Source::Default => "default",
            Source::CommandLine => "command line",
            Source::Saved => "saved",
        }
    }
}

struct State {
    path: Option<PathBuf>,
    /// What the file holds now.
    saved: BTreeMap<String, String>,
    /// What each startup key's running value is, as text, and where it came
    /// from. Recorded once at start-up.
    running: BTreeMap<String, (String, Source)>,
}

fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(State {
            path: None,
            saved: BTreeMap::new(),
            running: BTreeMap::new(),
        })
    })
}

/// Parses the file's text: `section.key = value` per line, `#` comments,
/// blank lines ignored. A value may be quoted in `'` or `"`.
pub fn parse(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("{FILE_NAME} line {}: expected `section.key = value`", n + 1))?;
        let key = key.trim().to_ascii_lowercase();
        if !key.contains('.') {
            return Err(format!("{FILE_NAME} line {}: `{key}` is not `section.key`", n + 1));
        }
        let value = value.trim().trim_matches(|c| c == '\'' || c == '"').to_string();
        out.insert(key, value);
    }
    Ok(out)
}

fn render(saved: &BTreeMap<String, String>) -> String {
    let mut out = String::from(
        "# Startup settings saved by SET CONFIG, applied at the next start.\n\
         # A flag given on the command line wins over a value here.\n",
    );
    for (k, v) in saved {
        out.push_str(&format!("{k} = {v}\n"));
    }
    out
}

/// Reads `<location>/rasterizeddb.conf` and remembers where it is. A missing
/// file is an empty one. Call once, before the startup settings are applied.
pub fn load(location: &Path) -> Result<BTreeMap<String, String>, String> {
    let path = location.join(FILE_NAME);
    let saved = match std::fs::read_to_string(&path) {
        Ok(text) => parse(&text)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
        Err(e) => return Err(format!("reading {}: {e}", path.display())),
    };
    let mut s = state().lock();
    s.path = Some(path);
    s.saved = saved.clone();
    Ok(saved)
}

/// Records a startup key's running value and its source.
pub fn record(key: &str, value: impl Into<String>, source: Source) {
    state().lock().running.insert(key.to_ascii_lowercase(), (value.into(), source));
}

/// Where `key`'s running value came from; `Default` for a key not recorded.
pub fn source(key: &str) -> Source {
    state().lock().running.get(&key.to_ascii_lowercase()).map_or(Source::Default, |(_, s)| *s)
}

/// The value saved for the next start, when it differs from the running one:
/// what a restart would change. `None` when nothing is pending.
pub fn pending(key: &str) -> Option<String> {
    let s = state().lock();
    let key = key.to_ascii_lowercase();
    let saved = s.saved.get(&key)?;
    match s.running.get(&key) {
        Some((running, _)) if running == saved => None,
        _ => Some(saved.clone()),
    }
}

/// Whether a flag overrides `key`, so a saved value will not take effect.
pub fn shadowed_by_flag(key: &str) -> bool {
    source(key) == Source::CommandLine
}

/// Saves `key = value` for the next start, writing the file whole: to a
/// temporary beside it, synced, then renamed over it. `Err` when the server
/// was started with no location to keep it in.
pub fn save(key: &str, value: &str) -> Result<(), String> {
    let mut s = state().lock();
    let path = s
        .path
        .clone()
        .ok_or_else(|| "this server has no data directory to save settings in".to_string())?;
    let mut next = s.saved.clone();
    next.insert(key.to_ascii_lowercase(), value.to_string());
    let tmp = path.with_extension("conf.tmp");
    let write = || -> io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(render(&next).as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        if let Some(dir) = path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    };
    write().map_err(|e| format!("saving {}: {e}", path.display()))?;
    s.saved = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_reads_back_what_it_wrote() {
        let mut m = BTreeMap::new();
        m.insert("index.residency".to_string(), "disk".to_string());
        m.insert("server.malloc_arenas".to_string(), "2".to_string());
        assert_eq!(parse(&render(&m)).unwrap(), m);
        let text = "# c\n\n server.batch_size = '5000' # five thousand\nINDEX.Residency=\"hybrid\"\n";
        let p = parse(text).unwrap();
        assert_eq!(p.get("server.batch_size").map(String::as_str), Some("5000"));
        assert_eq!(p.get("index.residency").map(String::as_str), Some("hybrid"));
        assert!(parse("no equals sign").is_err());
        assert!(parse("nodot = 1").is_err());
    }
}
