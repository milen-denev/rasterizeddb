//! The container a snapshot travels in.
//!
//! # Why not tar
//!
//! A snapshot is a flat set of files in one directory — no permissions, no
//! symlinks, no ownership, no nesting. Tar spends 512 bytes of header and a
//! checksum on metadata none of which is restored, and reading it back means
//! tolerating the half-dozen historical dialects. This format carries exactly
//! the two facts a restore uses, name and length, and its reader rejects
//! anything it does not recognise instead of guessing.
//!
//! # Layout
//!
//! ```text
//! "RDBA" u32 | version u8 | pad u8 x3 | entry_count u32
//! per entry: name_len u16 | name utf-8 | data_len u64 | data | crc32 u32
//! ```
//!
//! Written into a zstd stream, so the numbers above are what the *compressor*
//! sees. Entries are streamed one after another with no index and no seeking,
//! which is what lets the archive be produced and uploaded at the same time
//! without ever holding a whole database in memory.
//!
//! The per-entry CRC is the same polynomial the storage layer uses elsewhere,
//! and it is checked on unpack: zstd's own frame checksum covers the stream,
//! but a snapshot is the last copy of the data and is worth doubting twice.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use crate::core::storage_providers::CRC;

const MAGIC: u32 = 0x5244_4241; // "RDBA"
const VERSION: u8 = 1;

/// Refuses an absurd name length before allocating for it.
const MAX_NAME_LEN: usize = 4096;

/// Streams entries into `sink`.
pub struct ArchiveWriter<W: Write> {
    sink: W,
}

impl<W: Write> ArchiveWriter<W> {
    /// Starts an archive that will hold exactly `entry_count` files.
    ///
    /// The count is written up front so a reader can tell a truncated archive
    /// from a complete one without a trailer it would have to seek to.
    pub fn new(mut sink: W, entry_count: u32) -> io::Result<Self> {
        let mut header = [0u8; 12];
        header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        header[4] = VERSION;
        header[8..12].copy_from_slice(&entry_count.to_le_bytes());
        sink.write_all(&header)?;
        Ok(Self { sink })
    }

    /// Copies one file in under the name `name`.
    ///
    /// The length is taken once and then honoured exactly: a file being
    /// appended to while the snapshot runs is captured at the length it had
    /// when its turn came, and the log replay that follows a restore supplies
    /// everything written after that. Reading to EOF instead would record a
    /// length that no longer matched the bytes already declared.
    pub fn add_file(&mut self, name: &str, path: &Path) -> io::Result<u64> {
        self.add_open_file(name, File::open(path)?)
    }

    /// [`ArchiveWriter::add_file`] for a file the caller opened, so it can
    /// tell a file that is gone from a failure to copy one that is there.
    pub fn add_open_file(&mut self, name: &str, file: File) -> io::Result<u64> {
        let len = file.metadata()?.len();
        self.add_reader(name, len, &mut BufReader::with_capacity(256 * 1024, file))
    }

    pub fn add_reader(&mut self, name: &str, len: u64, source: &mut impl Read) -> io::Result<u64> {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > MAX_NAME_LEN {
            return Err(io::Error::other(format!(
                "archive entry name too long: {name}"
            )));
        }

        // Header first, so the reader knows how much to expect.
        self.sink
            .write_all(&(name_bytes.len() as u16).to_le_bytes())?;
        self.sink.write_all(name_bytes)?;
        self.sink.write_all(&len.to_le_bytes())?;

        // The checksum trails its entry rather than heading it. That ordering
        // is what lets the body be streamed: a leading CRC would have to be
        // known before the first byte went out, which means either reading the
        // file twice or buffering all of it, and a compressor is not seekable
        // so a patched-in placeholder is not available either. The reader
        // recomputes as it goes and compares at the end, so it never has to
        // trust a prefix.
        let mut remaining = len;
        let mut chunk = vec![0u8; 1024 * 1024];
        let mut digest = CRC.digest();

        while remaining > 0 {
            let want = chunk.len().min(remaining as usize);
            source.read_exact(&mut chunk[..want])?;
            digest.update(&chunk[..want]);
            self.sink.write_all(&chunk[..want])?;
            remaining -= want as u64;
        }

        self.sink.write_all(&digest.finalize().to_le_bytes())?;

        Ok(len)
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.sink.flush()?;
        Ok(self.sink)
    }
}

/// One entry's identity, as reported while unpacking.
#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub len: u64,
}

/// Unpacks an archive into `target_dir`.
///
/// Returns what it wrote. Entry names are validated rather than trusted: an
/// archive is a file fetched from a network service, and a name containing a
/// separator or `..` would otherwise write outside the directory the operator
/// named.
pub fn unpack(source: &mut impl Read, target_dir: &Path) -> io::Result<Vec<Entry>> {
    let mut header = [0u8; 12];
    source.read_exact(&mut header)?;

    let magic = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
    if magic != MAGIC {
        return Err(io::Error::other(
            "not a RasterizedDB snapshot archive (bad magic)",
        ));
    }
    if header[4] != VERSION {
        return Err(io::Error::other(format!(
            "snapshot archive version {} is newer than this build understands",
            header[4]
        )));
    }
    let entry_count = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));

    std::fs::create_dir_all(target_dir)?;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for index in 0..entry_count {
        let mut name_len = [0u8; 2];
        source.read_exact(&mut name_len)?;
        let name_len = u16::from_le_bytes(name_len) as usize;
        if name_len == 0 || name_len > MAX_NAME_LEN {
            return Err(io::Error::other(
                "snapshot archive has a malformed entry name",
            ));
        }

        let mut name_bytes = vec![0u8; name_len];
        source.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|_| io::Error::other("snapshot archive entry name is not UTF-8"))?;

        if !is_safe_name(&name) {
            return Err(io::Error::other(format!(
                "snapshot archive entry '{name}' names something outside the restore \
                 directory"
            )));
        }

        let mut len = [0u8; 8];
        source.read_exact(&mut len)?;
        let len = u64::from_le_bytes(len);

        let path = target_dir.join(&name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out = io::BufWriter::with_capacity(256 * 1024, File::create(&path)?);
        let mut digest = CRC.digest();
        let mut remaining = len;
        let mut chunk = vec![0u8; 1024 * 1024];

        while remaining > 0 {
            let want = chunk.len().min(remaining as usize);
            source.read_exact(&mut chunk[..want])?;
            digest.update(&chunk[..want]);
            out.write_all(&chunk[..want])?;
            remaining -= want as u64;
        }
        out.flush()?;
        out.into_inner()
            .map_err(|e| io::Error::other(e.to_string()))?
            .sync_data()?;

        let mut expected = [0u8; 4];
        source.read_exact(&mut expected)?;
        let expected = u32::from_le_bytes(expected);
        let actual = digest.finalize();
        if actual != expected {
            return Err(io::Error::other(format!(
                "snapshot entry '{name}' failed its checksum \
                 (expected {expected:08x}, got {actual:08x}); the backup is corrupt"
            )));
        }

        log::debug!(
            "restored {} ({} bytes, entry {}/{})",
            name,
            len,
            index + 1,
            entry_count
        );
        entries.push(Entry { name, len });
    }

    // To the end of the stream. The per-entry checksum covers an entry's
    // bytes, not its name or length, and a decompressor verifies its frame
    // checksum — the one check over everything — only when it reaches the
    // frame's end. Stopping at the last entry skipped it, so a damaged byte
    // in a name restored that file under another name and the restore
    // reported success.
    let mut rest = [0u8; 4096];
    let mut trailing = 0u64;
    loop {
        let read = source.read(&mut rest)?;
        if read == 0 {
            break;
        }
        trailing += read as u64;
    }
    if trailing > 0 {
        return Err(io::Error::other(format!(
            "snapshot archive has {trailing} bytes after its last entry; the backup is corrupt"
        )));
    }

    Ok(entries)
}

/// A name that names a file in one directory and nothing else.
/// Whether an entry name may be joined onto the restore directory.
///
/// A name is a relative path with `/` separators and may descend into
/// subdirectories: every database created with `CREATE DATABASE` is one, and
/// its files are named relative to the location. It may not climb out, and it
/// may not be absolute — an entry that did either would write wherever it liked
/// on the machine doing the restore.
///
/// It used to require a plain file name with no separator at all. That was
/// safe, and it silently made a nested entry unrestorable, so the two sides had
/// to be relaxed together.
pub(crate) fn is_safe_name(name: &str) -> bool {
    if name.is_empty()
        || name.starts_with('/')
        || name.ends_with('/')
        || name.contains('\\')
        || name.contains('\0')
        // A Windows drive-relative name like `C:foo` would escape too.
        || name.contains(':')
        || name.contains("//")
    {
        return false;
    }
    name.split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_entries() {
        let dir = temp_testdir::TempDir::default();
        let source_dir = dir.join("src");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(source_dir.join("a.db"), b"hello world").unwrap();
        std::fs::write(source_dir.join("b.db"), vec![7u8; 3_000_000]).unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArchiveWriter::new(&mut buffer, 2).unwrap();
        writer.add_file("a.db", &source_dir.join("a.db")).unwrap();
        writer.add_file("b.db", &source_dir.join("b.db")).unwrap();
        writer.finish().unwrap();

        let target = dir.join("out");
        let entries = unpack(&mut buffer.as_slice(), &target).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(std::fs::read(target.join("a.db")).unwrap(), b"hello world");
        assert_eq!(std::fs::read(target.join("b.db")).unwrap().len(), 3_000_000);
    }

    #[test]
    fn rejects_a_corrupted_entry() {
        let dir = temp_testdir::TempDir::default();
        std::fs::write(dir.join("a.db"), b"payload").unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArchiveWriter::new(&mut buffer, 1).unwrap();
        writer.add_file("a.db", &dir.join("a.db")).unwrap();
        writer.finish().unwrap();

        // Flip a byte inside the payload, leaving the framing intact.
        let payload_start = 12 + 2 + "a.db".len() + 8;
        buffer[payload_start] ^= 0xff;

        let err = unpack(&mut buffer.as_slice(), &dir.join("out")).unwrap_err();
        assert!(err.to_string().contains("failed its checksum"), "{err}");
    }

    /// No damaged byte of a compressed snapshot unpacks into something else:
    /// it fails, or — a bit of the frame header no content depends on — gives
    /// back exactly the original. A flip inside a name used to restore that
    /// file under another name: the entry checksum does not cover names, and
    /// the decompressor's frame checksum went unread because unpacking stopped
    /// at the last entry.
    #[test]
    fn no_damaged_byte_of_a_compressed_archive_unpacks_differently() {
        let dir = temp_testdir::TempDir::default();
        std::fs::write(dir.join("t.db"), b"rows").unwrap();
        std::fs::write(dir.join("t_pointers.db"), vec![3u8; 64]).unwrap();
        let mut plain = Vec::new();
        let mut writer = ArchiveWriter::new(&mut plain, 2).unwrap();
        writer.add_file("t.db", &dir.join("t.db")).unwrap();
        writer.add_file("t_pointers.db", &dir.join("t_pointers.db")).unwrap();
        writer.finish().unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();

        let out = dir.join("out");
        let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice()).unwrap();
        assert_eq!(unpack(&mut decoder, &out).unwrap().len(), 2);
        for at in 0..compressed.len() {
            let mut damaged = compressed.clone();
            damaged[at] ^= 0x01;
            let _ = std::fs::remove_dir_all(&out);
            let unpacked = zstd::stream::read::Decoder::new(damaged.as_slice())
                .and_then(|mut decoder| unpack(&mut decoder, &out));
            if let Ok(entries) = unpacked {
                let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
                assert_eq!(names, ["t.db", "t_pointers.db"], "byte {at} of {} damaged", compressed.len());
                assert_eq!(std::fs::read(out.join("t.db")).unwrap(), b"rows", "byte {at}");
                assert_eq!(std::fs::read(out.join("t_pointers.db")).unwrap(), vec![3u8; 64], "byte {at}");
            }
        }
    }

    #[test]
    fn rejects_a_traversing_name() {
        let mut buffer = Vec::new();
        let mut writer = ArchiveWriter::new(&mut buffer, 1).unwrap();
        writer
            .add_reader("../escape.db", 4, &mut b"data".as_slice())
            .unwrap();
        writer.finish().unwrap();

        let dir = temp_testdir::TempDir::default();
        let err = unpack(&mut buffer.as_slice(), &dir.join("out")).unwrap_err();
        // The name check now allows a *downward* path, since a database made
        // with `CREATE DATABASE` is a subdirectory of the location. Climbing
        // out is still refused, which is what this has always been about.
        assert!(err.to_string().contains("outside the restore"), "{err}");
    }
}

#[cfg(test)]
mod nested_entry_tests {
    use super::*;

    fn archive_of(name: &str) -> Vec<u8> {
        let dir = temp_testdir::TempDir::default();
        let source = dir.join("payload.db");
        std::fs::write(&source, b"payload").unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArchiveWriter::new(&mut buffer, 1).unwrap();
        writer.add_file(name, &source).unwrap();
        writer.finish().unwrap();
        buffer
    }

    /// An archive names its files relative to the location, and a database made
    /// with `CREATE DATABASE` is a subdirectory of it — so a name may contain
    /// `/`. It may only ever go downwards: an entry that climbed out would
    /// write wherever it liked on the machine doing the restore.
    #[test]
    fn an_entry_that_climbs_out_is_refused() {
        for name in ["../escape.db", "/etc/passwd", "a/../../escape.db", "./x.db"] {
            let buffer = archive_of(name);
            let target = temp_testdir::TempDir::default();
            let err = unpack(&mut buffer.as_slice(), &target).unwrap_err();
            assert!(
                err.to_string().contains("outside the restore"),
                "{} was accepted: {}",
                name,
                err
            );
        }
    }

    #[test]
    fn a_nested_entry_round_trips() {
        let buffer = archive_of("vynflowdebug/rows.db");
        let target = temp_testdir::TempDir::default();
        let entries = unpack(&mut buffer.as_slice(), &target).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            std::fs::read(target.join("vynflowdebug/rows.db")).unwrap(),
            b"payload"
        );
    }
}
