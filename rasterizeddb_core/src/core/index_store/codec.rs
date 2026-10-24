//! What the index store's file codecs share (docs/persistent-indexes.md §1):
//! crc32c, a bounds-checked little-endian reader that returns an error where
//! slicing would panic, and the one error type every decoder returns.
//!
//! Every multi-byte integer in `.ixm`, `.ixs` and `rdb.ixe` is little-endian.
//! The checksums are CRC-32C (Castagnoli, the iSCSI polynomial; check value
//! `0xE306_9283` for `"123456789"`), as the design names them, not the
//! CRC-32/ISO-HDLC that the WAL and the floors file carry
//! (`storage_providers::CRC`). The two are different functions, so a record
//! of one format can never pass the other's check by accident.

use std::fmt;

/// CRC-32C, slice-by-16. A `static`, not a `const`: the table is 16 KiB and a
/// `const` would be copied into every use.
static CRC32C: crc::Crc<u32, crc::Table<16>> = crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISCSI);

/// CRC-32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}

/// Why bytes are not a valid file of one of the index store's formats.
///
/// Every decoder returns one of these for every input it cannot accept;
/// none panics, whatever the bytes (the fuzz tests beside each format pin
/// that).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    /// The bytes end before `need` more could be read at `at`.
    Truncated { what: &'static str, at: u64, need: u64 },
    /// The file does not start with the format's magic.
    BadMagic { what: &'static str },
    /// A version this build does not read.
    UnsupportedVersion { what: &'static str, found: u32 },
    /// A checksum over the bytes at `at` does not match.
    BadChecksum { what: &'static str, at: u64 },
    /// Well-framed bytes that break a rule of the format.
    Malformed { what: &'static str, at: u64, why: &'static str },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::Truncated { what, at, need } => {
                write!(f, "{what}: truncated at byte {at} ({need} more bytes needed)")
            }
            FormatError::BadMagic { what } => write!(f, "{what}: bad magic"),
            FormatError::UnsupportedVersion { what, found } => {
                write!(f, "{what}: unsupported format version {found}")
            }
            FormatError::BadChecksum { what, at } => write!(f, "{what}: checksum mismatch at byte {at}"),
            FormatError::Malformed { what, at, why } => write!(f, "{what}: malformed at byte {at}: {why}"),
        }
    }
}

impl std::error::Error for FormatError {}

/// Reads little-endian fields from a byte slice, never past its end.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], what: &'static str) -> Reader<'a> {
        Reader { bytes, pos: 0, what }
    }

    pub(crate) fn pos(&self) -> u64 {
        self.pos as u64
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        if self.remaining() < n {
            return Err(FormatError::Truncated {
                what: self.what,
                at: self.pos as u64,
                need: (n - self.remaining()) as u64,
            });
        }
        let out = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, FormatError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn u128(&mut self) -> Result<u128, FormatError> {
        Ok(u128::from_le_bytes(self.array()?))
    }
}

/// Hex of `bytes`, lower case: how the golden vectors are written.
#[cfg(test)]
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `s` as bytes; panics on bad hex (test input only).
#[cfg(test)]
pub(crate) fn from_hex(s: &str) -> Vec<u8> {
    let s: String = s.split_whitespace().collect();
    assert!(s.len() % 2 == 0, "odd hex");
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

/// A history row's digest of a frozen golden vector, as written.
#[cfg(test)]
pub(crate) fn digest_hex(parts: &[&str]) -> u64 {
    let mut h = crate::core::hash::ItemFeed::new(0);
    for p in parts {
        let b = from_hex(p);
        h.u64_le(b.len() as u64);
        h.bytes(&b);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_is_castagnoli() {
        // The catalogue's check value, and one more pinned vector.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_ne!(crc32c(b"123456789"), crate::core::storage_providers::CRC.checksum(b"123456789"));
    }

    #[test]
    fn the_reader_errs_instead_of_reading_past_the_end() {
        let mut r = Reader::new(&[1, 2, 3], "test");
        assert_eq!(r.u16().unwrap(), 0x0201);
        assert_eq!(
            r.u32(),
            Err(FormatError::Truncated {
                what: "test",
                at: 2,
                need: 3
            })
        );
        assert_eq!(r.u8().unwrap(), 3);
        assert!(r.u8().is_err());
        assert_eq!(r.remaining(), 0);
    }
}
