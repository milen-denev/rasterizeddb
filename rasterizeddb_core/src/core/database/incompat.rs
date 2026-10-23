//! `incompat_features`: the on-disk formats a binary must understand to open a
//! database directory (persistent indexes, V14).
//!
//! A root file of flags, one per line under a header. Each flag names a format
//! that some file in the directory may use and that a binary without support
//! for it would read wrongly or fail to keep correct: a build without
//! persistent indexes would let an index snapshot go stale under its writes and
//! a later build would trust it. So every binary refuses, loudly and before it
//! touches anything, a directory carrying a flag it does not know, and which
//! flags it knows is decided by its Cargo features.
//!
//! A flag is set durably **before** the first file that needs it is created,
//! never after: in the other order a crash leaves the file without the fence.
//! Binaries older than this file cannot be fenced; they ignore it.
//!
//! The name ends in no table suffix — every table file ends in `.db` or
//! `.db.tmp` — so no table can own it.

use std::io;
use std::path::Path;

/// The file, in the database's root directory.
pub const FILE_NAME: &str = "incompat_features";

/// Its first line. A file that does not start with it is refused, as an
/// unknown flag is: it is either damaged or from a later format of this file.
const HEADER: &str = "rasterizeddb incompat_features 1";

/// Persistent index snapshots (`.ixs`, `.ixm`, `rdb.ixe`) may exist in this
/// directory. Set before the first of them is created.
pub const INDEX_STORE_V1: &str = "index_store_v1";

/// Every flag any build has defined, with what it means, for the refusal
/// message. Not the set this build supports: that is [`KNOWN`].
const DESCRIBED: &[(&str, &str)] = &[(
    INDEX_STORE_V1,
    "persistent index snapshots (Cargo feature `persistent_indexes`)",
)];

/// The flags this build understands.
#[cfg(feature = "persistent_indexes")]
const KNOWN: &[&str] = &[INDEX_STORE_V1];
/// The flags this build understands.
#[cfg(not(feature = "persistent_indexes"))]
const KNOWN: &[&str] = &[];

/// Serialises [`require`] within the process: two first snapshots in one
/// directory would otherwise race on the temporary file.
#[cfg(feature = "persistent_indexes")]
static WRITE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The flags `text` holds, or why it cannot be read as this file.
fn parse(text: &str) -> Result<Vec<String>, String> {
    let mut lines = text.lines();
    if lines.next().map(str::trim_end) != Some(HEADER) {
        return Err(format!(
            "its {FILE_NAME} file does not start with \"{HEADER}\", so it is damaged or written \
             by a later format this binary cannot read"
        ));
    }
    Ok(lines
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Why a directory carrying `flags` cannot be opened by a build that knows
/// `known`, or `Ok` when every flag is known.
fn refusal(flags: &[String], known: &[&str]) -> Result<(), String> {
    let unknown: Vec<String> = flags
        .iter()
        .filter(|f| !known.contains(&f.as_str()))
        .map(|f| match DESCRIBED.iter().find(|(name, _)| name == f) {
            Some((_, what)) => format!("{f} ({what})"),
            None => f.clone(),
        })
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(format!(
        "it uses on-disk features this binary does not support: {}. Opening it would read or \
         write files this binary cannot keep correct; use a build that supports them",
        unknown.join(", ")
    ))
}

/// The flags `dir` carries: none when it has no such file.
pub fn flags(dir: &Path) -> Result<Vec<String>, String> {
    match std::fs::read(dir.join(FILE_NAME)) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => parse(&text),
            Err(_) => Err(format!("its {FILE_NAME} file is not text, so it is damaged")),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        // Unreadable proves nothing about what the directory uses.
        Err(e) => Err(format!("its {FILE_NAME} file could not be read: {e}")),
    }
}

/// Refuses a directory that carries a flag this build does not know.
///
/// Called before anything in the directory is opened, replayed or created.
pub fn check(dir: &Path) -> Result<(), String> {
    refusal(&flags(dir)?, KNOWN)
}

/// Sets `flag` in `dir`, durably, before the caller creates the first file
/// that needs it. Nothing is written when it is already set.
///
/// Written whole to a temporary file, synced, renamed over the old one and the
/// directory synced, so a crash leaves the old set or the new, never a torn
/// one.
#[cfg(feature = "persistent_indexes")]
pub fn require(dir: &Path, flag: &str) -> io::Result<()> {
    if !KNOWN.contains(&flag) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} is not an on-disk feature this build supports"),
        ));
    }
    let _write = WRITE.lock();
    let mut set = flags(dir).map_err(io::Error::other)?;
    if set.iter().any(|f| f == flag) {
        return Ok(());
    }
    set.push(flag.to_string());
    let mut text = String::from(HEADER);
    text.push('\n');
    for f in &set {
        text.push_str(f);
        text.push('\n');
    }
    let path = dir.join(FILE_NAME);
    let temp = dir.join(format!("{FILE_NAME}.tmp"));
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(text.as_bytes())?;
        crate::core::sync_audit::note("incompat_features", &temp);
        file.sync_all()?;
    }
    std::fs::rename(&temp, &path)?;
    crate::core::wal::sync_parent_dir(&path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, text: &str) {
        std::fs::write(dir.join(FILE_NAME), text).unwrap();
    }

    #[test]
    fn a_directory_with_no_file_or_no_flags_opens() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(check(dir.path()), Ok(()));
        write(dir.path(), &format!("{HEADER}\n\n"));
        assert_eq!(check(dir.path()), Ok(()));
    }

    /// V14: a flag the build does not know is refused, by name.
    #[test]
    fn an_unknown_flag_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &format!("{HEADER}\nmade_up_v9\n"));
        let refused = check(dir.path()).unwrap_err();
        assert!(refused.contains("made_up_v9"), "{refused}");
    }

    /// V14: a build without persistent indexes refuses a directory that has
    /// used them, and says which feature it lacks. Every build runs the
    /// refusal against an empty known set, which is that build's set.
    #[test]
    fn a_build_without_persistent_indexes_refuses_an_index_store_directory() {
        let flags = vec![INDEX_STORE_V1.to_string()];
        let refused = refusal(&flags, &[]).unwrap_err();
        assert!(refused.contains("index_store_v1"), "{refused}");
        assert!(refused.contains("persistent_indexes"), "{refused}");
        assert_eq!(refusal(&flags, &[INDEX_STORE_V1]), Ok(()));

        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &format!("{HEADER}\n{INDEX_STORE_V1}\n"));
        assert_eq!(
            check(dir.path()).is_ok(),
            cfg!(feature = "persistent_indexes"),
            "this build's answer does not follow its feature"
        );
    }

    /// A file that cannot be read as this format proves nothing about what
    /// the directory uses, so it is refused rather than taken as empty.
    #[test]
    fn a_damaged_or_unreadable_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "index_store_v1\n");
        assert!(check(dir.path()).is_err(), "a file without its header was read");
        std::fs::write(dir.path().join(FILE_NAME), [0xFFu8, 0xFE, 0x00]).unwrap();
        assert!(check(dir.path()).is_err(), "a binary file was read");
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(FILE_NAME)).unwrap();
        assert!(check(dir.path()).is_err(), "an unreadable file was taken as no flags");
    }

    #[cfg(feature = "persistent_indexes")]
    #[test]
    fn require_sets_a_flag_once_and_keeps_the_others() {
        let dir = tempfile::tempdir().unwrap();
        require(dir.path(), INDEX_STORE_V1).unwrap();
        assert_eq!(flags(dir.path()), Ok(vec![INDEX_STORE_V1.to_string()]));
        // Every write renames a new file into place, so a new inode is a write
        // (timestamps are too coarse to tell two writes a moment apart).
        use std::os::unix::fs::MetadataExt;
        let written = std::fs::metadata(dir.path().join(FILE_NAME)).unwrap().ino();
        require(dir.path(), INDEX_STORE_V1).unwrap();
        assert_eq!(
            std::fs::metadata(dir.path().join(FILE_NAME)).unwrap().ino(),
            written,
            "a flag already set was written again"
        );
        assert!(!dir.path().join(format!("{FILE_NAME}.tmp")).exists());
        assert!(require(dir.path(), "made_up_v9").is_err(), "an unsupported flag was set");

        // A flag another build set is kept when this one adds its own.
        write(dir.path(), &format!("{HEADER}\nother_v1\n"));
        require(dir.path(), INDEX_STORE_V1).unwrap();
        assert_eq!(
            flags(dir.path()),
            Ok(vec!["other_v1".to_string(), INDEX_STORE_V1.to_string()])
        );
    }

    /// The open refuses before it replays, opens or creates anything: the
    /// directory is exactly as it was, log included.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_open_refuses_an_unknown_flag_before_touching_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &format!("{HEADER}\nmade_up_v9\n"));
        let log = dir.path().join(crate::core::wal::DEFAULT_WAL_NAME);
        std::fs::write(&log, b"not replayed").unwrap();
        let listing = |dir: &Path| {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        };
        let before = listing(dir.path());

        let location = dir.path().to_string_lossy().into_owned();
        let refused = match crate::core::database::Database::try_new_named(&location, false, "incompat").await {
            Ok(_) => panic!("a directory with an unknown on-disk feature was opened"),
            Err(e) => e,
        };
        assert!(refused.contains("made_up_v9"), "{refused}");
        assert_eq!(listing(dir.path()), before, "the refused open created files");
        assert_eq!(std::fs::read(&log).unwrap(), b"not replayed");
        assert!(crate::core::wal::wal_for(&location).is_none(), "a log was installed");
    }
}
