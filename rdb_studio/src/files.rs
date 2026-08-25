//! Opening and saving files.
//!
//! # Why a native dialog rather than a path box
//!
//! The backup page asks for a restore directory as a text field, and that is
//! right there — the path is part of a runbook, often on a *different* machine,
//! and it gets pasted rather than browsed to. Exporting a result set is the
//! opposite: it goes somewhere on this machine, next to whatever else the
//! person is working on, and typing an absolute path to it is a chore nobody
//! else asks of them.
//!
//! The dialog goes through the XDG desktop portal, so it is the desktop's own
//! file chooser and works under a sandbox.
//!
//! # Cancelling is not failing
//!
//! Every one of these returns `Ok(None)` when the user closes the dialog. A
//! console that reported "export failed" because somebody changed their mind
//! would be crying wolf, and the one time it means something nobody would look.

use std::path::PathBuf;

/// What a saved file is called and what it is.
pub struct SaveSpec {
    /// Suggested file name, extension included.
    pub name: String,
    /// Human name for the kind, e.g. `CSV`.
    pub kind: &'static str,
    /// Extensions the dialog filters on.
    pub extensions: &'static [&'static str],
}

impl SaveSpec {
    pub fn new(name: impl Into<String>, kind: &'static str, extensions: &'static [&'static str]) -> Self {
        Self { name: name.into(), kind, extensions }
    }
}

/// Asks where to put something, then writes it.
///
/// Returns the path written, or `None` when the dialog was dismissed.
pub async fn save(spec: SaveSpec, contents: String) -> Result<Option<PathBuf>, String> {
    let handle = rfd::AsyncFileDialog::new()
        .set_title(format!("Save {}", spec.kind))
        .set_file_name(&spec.name)
        .add_filter(spec.kind, spec.extensions)
        .save_file()
        .await;

    let Some(handle) = handle else { return Ok(None) };
    let path = handle.path().to_path_buf();

    // The portal returns whatever the user typed, extension or not. Adding the
    // expected one when it is missing means a file that opens in the right
    // thing when they double-click it.
    let path = with_extension(path, spec.extensions.first().copied().unwrap_or("txt"));

    tokio::fs::write(&path, contents)
        .await
        .map_err(|e| format!("could not write {}: {}", path.display(), e))?;
    Ok(Some(path))
}

/// Asks for a file and reads it.
pub async fn open(kind: &'static str, extensions: &'static [&'static str]) -> Result<Option<(PathBuf, String)>, String> {
    let handle = rfd::AsyncFileDialog::new()
        .set_title(format!("Open {}", kind))
        .add_filter(kind, extensions)
        .pick_file()
        .await;

    let Some(handle) = handle else { return Ok(None) };
    let path = handle.path().to_path_buf();

    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| format!("could not read {}: {}", path.display(), e))?;

    // Lossy rather than a refusal: a .sql file with one stray byte in a comment
    // is still a file somebody wants to look at, and refusing to open it helps
    // nobody.
    Ok(Some((path.clone(), String::from_utf8_lossy(&bytes).into_owned())))
}

/// Adds `extension` when the path has none.
///
/// Only when it has *none*: someone who typed `dump.txt` for a CSV meant
/// `dump.txt`, and turning it into `dump.txt.csv` would be overruling them.
fn with_extension(path: PathBuf, extension: &str) -> PathBuf {
    if path.extension().is_some() {
        return path;
    }
    path.with_extension(extension)
}

/// A file name that is safe on every platform and says what it holds.
///
/// A table can be called things a filesystem cannot: `orders/2026`, `a:b`, or
/// on Windows `con`. Anything outside a conservative set becomes `_`.
pub fn suggested_name(base: &str, extension: &str) -> String {
    let cleaned: String = base
        .trim()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let cleaned = cleaned.trim_matches('_');
    let stem = if cleaned.is_empty() { "export" } else { cleaned };
    format!("{}.{}", stem, extension)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_extension_is_added_only_when_missing() {
        assert_eq!(with_extension(PathBuf::from("/tmp/a"), "csv"), PathBuf::from("/tmp/a.csv"));
        // Somebody who typed an extension meant it.
        assert_eq!(
            with_extension(PathBuf::from("/tmp/a.txt"), "csv"),
            PathBuf::from("/tmp/a.txt")
        );
    }

    /// A table can be called things a filesystem cannot.
    #[test]
    fn a_suggested_name_is_safe_to_write() {
        assert_eq!(suggested_name("users", "csv"), "users.csv");
        assert_eq!(suggested_name("orders/2026", "json"), "orders_2026.json");
        assert_eq!(suggested_name("a:b*c", "csv"), "a_b_c.csv");
        assert_eq!(suggested_name("", "sql"), "export.sql");
        assert_eq!(suggested_name("   ", "sql"), "export.sql");
        assert_eq!(suggested_name("__", "sql"), "export.sql");
        assert!(!suggested_name("../../etc/passwd", "csv").contains('/'));
    }
}
