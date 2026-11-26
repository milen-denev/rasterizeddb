//! Memory for keys and secrets (docs/ledger-and-vault-plan.md §4.3).
//!
//! A decrypted secret or a data key must not end up in swap, in a core
//! dump, or readable through `/proc/<pid>/mem` by a process that is allowed
//! to attach. [`SecretBuf`] holds such bytes:
//!
//! - in pages from `memfd_secret(2)` when the kernel offers it (Linux 5.14+,
//!   `secretmem.enable`): removed from the kernel's direct map, so not even
//!   the kernel reads them by accident;
//! - otherwise in an anonymous mapping that is `mlock`ed (no swap) and
//!   marked `MADV_DONTDUMP` (not in a core dump) and `MADV_WIPEONFORK` (a
//!   child sees zeros).
//!
//! Either way the bytes are zeroed before the pages go back.
//!
//! [`harden_process`] turns off core dumps and ptrace attachment for the
//! process (`PR_SET_DUMPABLE = 0`) once a vault is in use.

use std::sync::Once;

/// Bytes kept out of swap, dumps and the direct map; zeroed on drop.
pub struct SecretBuf {
    ptr: *mut u8,
    len: usize,
    cap: usize,
    secretmem: bool,
}

// The buffer owns its mapping outright.
unsafe impl Send for SecretBuf {}
unsafe impl Sync for SecretBuf {}

fn page() -> usize {
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p > 0 { p as usize } else { 4096 }
}

impl SecretBuf {
    /// A copy of `bytes` in protected memory.
    pub fn from_slice(bytes: &[u8]) -> Result<SecretBuf, String> {
        let cap = bytes.len().max(1).div_ceil(page()) * page();
        let (ptr, secretmem) = map(cap)?;
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len()) };
        Ok(SecretBuf { ptr, len: bytes.len(), cap, secretmem })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Whether the pages came from `memfd_secret`.
    pub fn is_secretmem(&self) -> bool {
        self.secretmem
    }
}

#[cfg(target_os = "linux")]
fn map(cap: usize) -> Result<(*mut u8, bool), String> {
    // memfd_secret: syscall 447 on x86-64 and aarch64.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    unsafe {
        let fd = libc::syscall(447, 0) as i32;
        if fd >= 0 {
            let ok = libc::ftruncate(fd, cap as libc::off_t) == 0;
            let p = if ok {
                libc::mmap(std::ptr::null_mut(), cap, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0)
            } else {
                libc::MAP_FAILED
            };
            libc::close(fd);
            if p != libc::MAP_FAILED {
                return Ok((p.cast(), true));
            }
        }
    }
    anonymous(cap)
}

#[cfg(not(target_os = "linux"))]
fn map(cap: usize) -> Result<(*mut u8, bool), String> {
    anonymous(cap)
}

#[cfg(unix)]
fn anonymous(cap: usize) -> Result<(*mut u8, bool), String> {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            cap,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            return Err(format!("no memory for a secret: {}", std::io::Error::last_os_error()));
        }
        // Best effort each: an mlock refused by RLIMIT_MEMLOCK still leaves
        // the other protections, and the bytes are zeroed either way.
        libc::mlock(p, cap);
        #[cfg(target_os = "linux")]
        {
            libc::madvise(p, cap, libc::MADV_DONTDUMP);
            libc::madvise(p, cap, libc::MADV_WIPEONFORK);
        }
        Ok((p.cast(), false))
    }
}

impl Drop for SecretBuf {
    fn drop(&mut self) {
        unsafe {
            // Volatile, so the zeroing is not optimised away.
            for i in 0..self.cap {
                std::ptr::write_volatile(self.ptr.add(i), 0);
            }
            #[cfg(unix)]
            {
                if !self.secretmem {
                    libc::munlock(self.ptr.cast(), self.cap);
                }
                libc::munmap(self.ptr.cast(), self.cap);
            }
        }
    }
}

static HARDEN: Once = Once::new();

/// Turns off core dumps and ptrace attachment by non-root processes, once.
pub fn harden_process() {
    HARDEN.call_once(|| {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_buffer_holds_its_bytes() {
        let b = SecretBuf::from_slice(b"top secret").unwrap();
        assert_eq!(b.as_slice(), b"top secret");
        let empty = SecretBuf::from_slice(b"").unwrap();
        assert!(empty.as_slice().is_empty());
    }
}
