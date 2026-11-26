//! A small framed log file with compaction by generation, on
//! [`crate::core::framed_log`]: the broker's metadata log and each
//! subscription's state log.
//!
//! `<stem>.<generation>.log` holds framed records. A compaction writes the
//! whole state to generation + 1, ends it with [`SEALED`], syncs it and its
//! directory, and only then removes the old generation. At open, the live
//! generation is the highest that replays sealed (generation 0 needs no seal),
//! following `framed_log::live_generation`. A torn tail is cut before the
//! next append.

use std::fs::{File, OpenOptions};
use std::sync::Arc;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::core::framed_log::{self, Format, GenerationReplay};

/// The record that closes a compacted generation.
pub const SEALED: u8 = 0xFF;
/// The layout byte every payload opens with.
pub const LAYOUT: u8 = 1;

pub struct LogFile {
    dir: PathBuf,
    stem: String,
    format: Format,
    generation: u64,
    file: Arc<File>,
    len: u64,
}

fn name(stem: &str, generation: u64) -> String {
    format!("{stem}.{generation}.log")
}

fn generations(dir: &Path, stem: &str) -> io::Result<Vec<u64>> {
    let mut out = Vec::new();
    let prefix = format!("{stem}.");
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for e in rd {
                let e = e?;
                let n = e.file_name();
                let Some(n) = n.to_str() else { continue };
                if let Some(g) = n.strip_prefix(&prefix).and_then(|r| r.strip_suffix(".log")) {
                    if let Ok(g) = g.parse::<u64>() {
                        out.push(g);
                    }
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    out.sort_unstable_by(|a, b| b.cmp(a));
    Ok(out)
}

impl LogFile {
    /// Opens (or creates) the log and hands every record of its live
    /// generation to `apply`, in order, without the layout byte.
    pub fn open(
        dir: &Path,
        stem: &str,
        format: Format,
        mut apply: impl FnMut(u8, &[u8]) -> Result<(), String>,
    ) -> io::Result<LogFile> {
        std::fs::create_dir_all(dir)?;
        let gens = generations(dir, stem)?;
        let bad = |e: String| io::Error::new(io::ErrorKind::InvalidData, e);
        let (live, unreadable) = framed_log::live_generation(&gens, |g| {
            let bytes = std::fs::read(dir.join(name(stem, g))).map_err(|e| e.to_string())?;
            let mut sealed = false;
            match framed_log::scan(&format, &bytes, |_, kind, _| {
                sealed = kind == SEALED;
                Ok(())
            }) {
                Err(e) => Ok(GenerationReplay::Unreadable(e)),
                Ok(end) if end.torn.is_none() && sealed => Ok(GenerationReplay::Sealed),
                Ok(_) => Ok(GenerationReplay::Unsealed),
            }
        })
        .map_err(bad)?;
        for (g, e) in &unreadable {
            log::warn!("broker: {} generation {g} does not replay ({e}); keeping it, using {live}", stem);
        }
        let path = dir.join(name(stem, live));
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let end = framed_log::scan(&format, &bytes, |_, kind, payload| {
            if kind == SEALED {
                return Ok(());
            }
            let body = payload.get(1..).ok_or("empty payload")?;
            apply(kind, body)
        })
        .map_err(bad)?;
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        if let Some(t) = &end.torn {
            log::warn!(
                "broker: {} ends in a torn record at byte {} ({}); cutting {} bytes",
                path.display(),
                t.offset,
                t.reason,
                t.trailing
            );
            file.set_len(end.valid_len as u64)?;
            file.sync_data()?;
        }
        if bytes.is_empty() {
            crate::core::sync_path::sync_dir(dir)?;
        }
        // Lower generations, and higher ones that are only unfinished
        // compactions, are dead.
        for g in gens {
            if g != live && !unreadable.iter().any(|(u, _)| *u == g) {
                let _ = std::fs::remove_file(dir.join(name(stem, g)));
            }
        }
        Ok(LogFile { dir: dir.to_path_buf(), stem: stem.to_string(), format, generation: live, file: Arc::new(file), len: end.valid_len as u64 })
    }

    /// One framed record, as [`LogFile::append_raw`] takes it.
    pub fn frame(&self, kind: u8, body: &[u8]) -> io::Result<Vec<u8>> {
        let mut buf = framed_log::begin(kind, LAYOUT);
        buf.extend_from_slice(body);
        framed_log::finish(&self.format, buf).map_err(|n| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{} record of {n} bytes is too large", self.stem))
        })
    }

    /// Appends framed records (not synced).
    pub fn append_raw(&mut self, framed: &[u8]) -> io::Result<()> {
        use crate::core::pos_io::FileExt;
        self.file.write_all_at(framed, self.len)?;
        self.len += framed.len() as u64;
        Ok(())
    }

    /// Appends one record and syncs it.
    pub fn append_sync(&mut self, kind: u8, body: &[u8]) -> io::Result<()> {
        let framed = self.frame(kind, body)?;
        self.append_raw(&framed)?;
        self.file.sync_data()
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    /// The file, for a sync on another thread.
    pub fn shared_file(&self) -> Arc<File> {
        self.file.clone()
    }

    /// Replaces the log with `records` (the whole state) in a new generation.
    pub fn compact(&mut self, records: &[(u8, Vec<u8>)]) -> io::Result<()> {
        let next = self.generation + 1;
        let path = self.dir.join(name(&self.stem, next));
        let mut buf = Vec::new();
        for (kind, body) in records {
            buf.extend_from_slice(&self.frame(*kind, body)?);
        }
        buf.extend_from_slice(&self.frame(SEALED, &[])?);
        let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path)?;
        f.write_all(&buf)?;
        f.sync_data()?;
        crate::core::sync_path::sync_dir(&self.dir)?;
        let old = self.dir.join(name(&self.stem, self.generation));
        self.file = Arc::new(f);
        self.len = buf.len() as u64;
        self.generation = next;
        let _ = std::fs::remove_file(old);
        Ok(())
    }

    /// Removes every generation of the log.
    pub fn remove(dir: &Path, stem: &str) {
        if let Ok(gens) = generations(dir, stem) {
            for g in gens {
                let _ = std::fs::remove_file(dir.join(name(stem, g)));
            }
        }
    }
}
