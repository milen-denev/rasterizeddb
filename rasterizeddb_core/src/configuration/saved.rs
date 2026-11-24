//! Settings saved by `SET CONFIG`: `rasterizeddb.conf` beside the databases
//! (`--location`), applied at every start.
//!
//! `SET CONFIG` on a key that is read once at start-up cannot change the
//! running server, so it saves the value here instead, and the next start
//! applies it. A hot key (one a running server changes) takes effect at once
//! and is saved here too, so that a restart does not silently undo it. The
//! keys that act on one database (`replication.*`, `vacuum.enabled`) are not
//! saved. The file is `section.key = value` lines; `#` starts a comment. It
//! is the server's, not a database's: one file however many databases the
//! location holds.
//!
//! A file that does not parse never keeps the server from starting: it
//! starts with none of it applied, and `SET CONFIG` refuses to save until the
//! file is fixed, since writing the file whole would drop what it holds.
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
    /// Why the file [`load`] found could not be read, when it could not:
    /// [`save`] then refuses rather than replace it with one line.
    broken: Option<String>,
}

fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(State {
            path: None,
            saved: BTreeMap::new(),
            running: BTreeMap::new(),
            broken: None,
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
        "# Settings saved by SET CONFIG, applied at every start.\n\
         # A flag given on the command line wins over a value here.\n",
    );
    for (k, v) in saved {
        out.push_str(&format!("{k} = {v}\n"));
    }
    out
}

/// Reads `<location>/rasterizeddb.conf` and remembers where it is. A missing
/// file is an empty one. Call once, before the startup settings are applied.
///
/// A file that cannot be read or parsed is an `Err` for the caller to report,
/// and the start goes on without it; the path is still remembered, so that
/// [`save`] can say what is wrong with the file instead of claiming there is
/// no data directory.
pub fn load(location: &Path) -> Result<BTreeMap<String, String>, String> {
    let path = location.join(FILE_NAME);
    let read = match std::fs::read_to_string(&path) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(format!("reading {}: {e}", path.display())),
    };
    let mut s = state().lock();
    s.path = Some(path);
    match read {
        Ok(saved) => {
            s.saved = saved.clone();
            s.broken = None;
            Ok(saved)
        }
        Err(e) => {
            s.saved.clear();
            s.broken = Some(e.clone());
            Err(e)
        }
    }
}

/// Forgets the file [`load`] read: what a start with no data directory (an
/// in-memory engine) calls in a process that had one before, so that its
/// `SET CONFIG` on a startup key is refused rather than written into the
/// earlier directory's file.
pub fn unload() {
    let mut s = state().lock();
    s.path = None;
    s.saved.clear();
    s.broken = None;
}

/// Records a startup key's running value and its source.
pub fn record(key: &str, value: impl Into<String>, source: Source) {
    state().lock().running.insert(key.to_ascii_lowercase(), (value.into(), source));
}

/// `key`'s running value as recorded at start-up; `None` for a key not
/// recorded.
pub fn running(key: &str) -> Option<String> {
    state().lock().running.get(&key.to_ascii_lowercase()).map(|(v, _)| v.clone())
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
/// was started with no location to keep it in, when the file it found there
/// does not parse (writing it whole would drop the rest of it), and for a
/// value the file cannot hold on one line.
pub fn save(key: &str, value: &str) -> Result<(), String> {
    let mut s = state().lock();
    let path = s
        .path
        .clone()
        .ok_or_else(|| "this server has no data directory to save settings in".to_string())?;
    if let Some(why) = &s.broken {
        return Err(format!(
            "{why}; fix or remove {} first, as saving would replace everything it holds",
            path.display()
        ));
    }
    if value.contains(['\n', '\r', '#']) {
        return Err(format!("`{}` cannot be saved: a line break or `#` would not read back", value.escape_debug()));
    }
    let mut next = s.saved.clone();
    next.insert(key.to_ascii_lowercase(), value.to_string());
    let tmp = path.with_extension("conf.tmp");
    let write = || -> io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(render(&next).as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        if let Some(dir) = path.parent() {
            crate::core::sync_path::sync_dir(dir)?;
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

    /// A file that does not parse is kept as it is: `save` refuses, naming
    /// the line, rather than write a file holding only the new key. The
    /// state is the process's, so this is the only test in the lib that
    /// loads a file.
    #[test]
    fn a_file_that_does_not_parse_is_never_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let text = "server.batch_size = 5000\nthis line has no equals sign\n";
        std::fs::write(&path, text).unwrap();
        let e = load(dir.path()).unwrap_err();
        assert!(e.contains("line 2"), "{e}");
        let e = save("partition.idle_close_secs", "600").unwrap_err();
        assert!(e.contains("line 2") && e.contains("fix or remove"), "{e}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);

        // Fixed, it saves, keeping what it held.
        std::fs::write(&path, "server.batch_size = 5000\n").unwrap();
        load(dir.path()).unwrap();
        save("partition.idle_close_secs", "600").unwrap();
        assert!(save("partition.idle_close_secs", "6\nserver.batch_size = 1").is_err());
        let back = parse(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(back.get("server.batch_size").map(String::as_str), Some("5000"));
        assert_eq!(back.get("partition.idle_close_secs").map(String::as_str), Some("600"));
        unload();
    }
}
