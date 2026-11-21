//! Counts durable syncs by call site, for tests that need to know which code
//! path flushed which file.
//!
//! A checkpoint used to fsync every registered table file, and on a database
//! of 2,042 tables that was the whole cost of `VACUUM FULL`; which pass came
//! from where could only be guessed at from totals. Each sync site that can
//! touch many files calls [`note`] with a tag naming it. Off unless a test
//! [`watch`]es a directory, in which case only syncs of paths under that
//! directory are recorded, so tests running in parallel do not see each
//! other's. The cost when off is one relaxed atomic load.
//!
//! Test builds also record what a crash image needs: each sync's length,
//! issuing thread and order ([`SyncEvent`]), whether it has returned
//! ([`completion`]), and directory operations a hooked path announces
//! ([`dir_op`]). `core::power_loss` builds crash images from them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

static WATCHERS: AtomicUsize = AtomicUsize::new(0);
static WATCHED: Mutex<Vec<(PathBuf, Vec<(&'static str, PathBuf)>)>> = Mutex::new(Vec::new());

/// One noted sync with what a crash image needs to know about it: how much
/// of the file is durable once it returns, and the thread that issued it.
///
/// `len` is the file's length when the sync was issued, except for a log,
/// which says where its records ended ([`note_extent`]): a log is longer
/// than its records by the zeros it writes ahead of them, and records
/// written after the sync land inside that length.
#[cfg(any(test, feature = "index_store_failpoints"))]
#[derive(Debug, Clone)]
pub struct SyncEvent {
    pub tag: &'static str,
    pub path: PathBuf,
    pub len: u64,
    pub thread: std::thread::ThreadId,
    /// The issuing thread's kernel id, for asking `/proc` where it is.
    pub tid: i32,
    /// Which of its thread's calls to [`note`] recorded this, counted while
    /// anything is watched. A later call on the same thread means this
    /// sync returned: `note` is called just before the sync it names.
    pub entry: u64,
    /// Process-wide order of recording.
    pub seq: u64,
}

/// Per watched directory, in order. Test-only: the `metadata` call is a
/// system call per sync, which production does not pay.
#[cfg(any(test, feature = "index_store_failpoints"))]
static EVENTS: Mutex<Vec<(PathBuf, Vec<SyncEvent>)>> = Mutex::new(Vec::new());

/// Held by a call site across the sync a [`note`] announced: dropping it says
/// the sync has returned. Without it nothing off Linux could tell (there,
/// `/proc` shows where the thread is), and the power-loss harness took
/// every sync of another thread as still in flight, durable never. Nothing
/// in a build without the harness.
#[must_use = "hold it until the sync it notes has returned"]
pub struct SyncNote {
    #[cfg(any(test, feature = "index_store_failpoints"))]
    done: Option<(std::sync::Arc<calls::Calls>, u64)>,
}

impl SyncNote {
    const NONE: SyncNote = SyncNote {
        #[cfg(any(test, feature = "index_store_failpoints"))]
        done: None,
    };
}

impl Drop for SyncNote {
    #[inline]
    fn drop(&mut self) {
        #[cfg(any(test, feature = "index_store_failpoints"))]
        if let Some((calls, entry)) = self.done.take() {
            calls.returned(entry);
        }
    }
}

/// Records a sync of `path` from the site named `tag`. Hold what it returns
/// until the sync has returned.
#[inline]
pub fn note(tag: &'static str, path: &Path) -> SyncNote {
    note_as(tag, path, None)
}

/// [`note`] for a file whose durable part is not its whole length: `extent`
/// bytes of it are what the sync makes durable. The log's syncs, which cover
/// its records and not the padding past them.
#[inline]
pub fn note_extent(tag: &'static str, path: &Path, extent: u64) -> SyncNote {
    note_as(tag, path, Some(extent))
}

#[inline]
#[cfg_attr(not(any(test, feature = "index_store_failpoints")), allow(unused_variables))]
fn note_as(tag: &'static str, path: &Path, extent: Option<u64>) -> SyncNote {
    if WATCHERS.load(Ordering::Relaxed) == 0 {
        return SyncNote::NONE;
    }
    // Entered before any lock below, so a thread parked on one of them is
    // seen as still inside this call: its sync has not been issued.
    #[cfg(any(test, feature = "index_store_failpoints"))]
    let inside = calls::enter();
    let mut watched = WATCHED.lock();
    for (dir, seen) in watched.iter_mut() {
        if path.starts_with(dir) {
            seen.push((tag, path.to_path_buf()));
        }
    }
    drop(watched);
    #[cfg(any(test, feature = "index_store_failpoints"))]
    let mut recorded = None;
    #[cfg(any(test, feature = "index_store_failpoints"))]
    {
        let mut events = EVENTS.lock();
        if events.iter().any(|(dir, _)| path.starts_with(dir)) {
            // Taken before the caller's sync, so it is a lower bound on what
            // that sync makes durable: a crash image cut here never claims
            // more than the disk could hold.
            let len = extent.unwrap_or_else(|| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0));
            let thread = std::thread::current().id();
            let event = SyncEvent {
                tag,
                path: path.to_path_buf(),
                len,
                thread,
                tid: inside.tid,
                entry: inside.entry,
                seq: calls::SEQ.fetch_add(1, Ordering::Relaxed),
            };
            for (dir, seen) in events.iter_mut() {
                if path.starts_with(dir) {
                    seen.push(event.clone());
                }
            }
            recorded = Some(event);
        }
    }
    // Outside every lock here: a recorder reads the file, and a failpoint
    // armed on this sync may cut power, which reads the whole directory.
    #[cfg(any(test, feature = "index_store_failpoints"))]
    if let Some(event) = recorded {
        crate::core::power_loss::on_sync(&event);
    }
    #[cfg(any(test, feature = "index_store_failpoints"))]
    {
        let done = inside.done();
        drop(inside);
        SyncNote { done: Some(done) }
    }
    #[cfg(not(any(test, feature = "index_store_failpoints")))]
    SyncNote::NONE
}

/// A directory operation, announced by a hooked path just **before** it
/// runs. Until a sync of the directory is issued after it and returns, a
/// power cut may undo it, and crash images keep or revert it per seed.
/// Operations nobody announces are kept, as an image read from the
/// directory shows them.
#[cfg(any(test, feature = "index_store_failpoints"))]
#[derive(Debug, Clone, Copy)]
pub enum DirOp<'a> {
    Create(&'a Path),
    Rename { from: &'a Path, to: &'a Path },
    Unlink(&'a Path),
}

/// Announces `op` to whatever records the directory it touches. A no-op
/// unless something is watching.
#[cfg(any(test, feature = "index_store_failpoints"))]
pub fn dir_op(op: DirOp<'_>) {
    if WATCHERS.load(Ordering::Relaxed) == 0 {
        return;
    }
    crate::core::power_loss::on_dir_op(op, calls::SEQ.fetch_add(1, Ordering::Relaxed));
}

/// Whether the sync an event recorded has returned, as far as can be told
/// without the call site saying so.
#[cfg(any(test, feature = "index_store_failpoints"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// Its thread has moved on: a later [`note`], or a blocking system call
    /// that is not a sync of that file, or the thread is gone.
    Returned,
    /// Still inside [`note`] (the sync not yet issued) or inside a sync of
    /// that file.
    InFlight,
    /// Running in user space, between `note` and the sync or after it.
    /// Ask again shortly.
    Unknown,
}

/// Where `event`'s sync stands now. See [`Completion`].
#[cfg(any(test, feature = "index_store_failpoints"))]
pub fn completion(event: &SyncEvent) -> Completion {
    calls::completion(event)
}

/// This thread's id: the kernel's on Linux (what `/proc/self/task/<id>` is
/// named by), a process-wide counter elsewhere.
#[cfg(any(test, feature = "index_store_failpoints"))]
pub fn thread_id() -> i32 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid takes no arguments and cannot fail.
        (unsafe { libc::syscall(libc::SYS_gettid) }) as i32
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::sync::atomic::AtomicI32;
        static NEXT: AtomicI32 = AtomicI32::new(1);
        thread_local! {
            static ID: i32 = NEXT.fetch_add(1, Ordering::Relaxed);
        }
        ID.with(|id| *id)
    }
}

/// Per thread: how many calls to [`note`] it has entered, and whether it is
/// inside one, readable from another thread.
#[cfg(any(test, feature = "index_store_failpoints"))]
mod calls {
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use parking_lot::Mutex;

    use super::{Completion, SyncEvent};

    pub(super) static SEQ: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    pub(crate) struct Calls {
        entered: AtomicU64,
        inside: AtomicBool,
        /// The highest entry whose sync its site said had returned.
        returned: AtomicU64,
    }

    impl Calls {
        pub(super) fn returned(&self, entry: u64) {
            self.returned.fetch_max(entry, Ordering::AcqRel);
        }
    }

    /// Every thread that ever called `note` while something was watched, by
    /// kernel id. A reused id replaces the dead thread's entry.
    static THREADS: Mutex<Vec<(i32, StdArc<Calls>)>> = Mutex::new(Vec::new());

    thread_local! {
        static MINE: (i32, StdArc<Calls>) = {
            let tid = super::thread_id();
            let calls = StdArc::new(Calls::default());
            let mut threads = THREADS.lock();
            threads.retain(|(t, _)| *t != tid);
            threads.push((tid, calls.clone()));
            (tid, calls)
        };
    }

    pub(super) struct Inside {
        pub tid: i32,
        pub entry: u64,
        calls: StdArc<Calls>,
    }

    impl Inside {
        /// What the site's [`super::SyncNote`] reports to when dropped.
        pub(super) fn done(&self) -> (StdArc<Calls>, u64) {
            (self.calls.clone(), self.entry)
        }
    }

    impl Drop for Inside {
        fn drop(&mut self) {
            self.calls.inside.store(false, Ordering::Release);
        }
    }

    pub(super) fn enter() -> Inside {
        MINE.with(|(tid, calls)| {
            // Inside first: from here until the drop the thread counts as
            // not having issued its sync.
            calls.inside.store(true, Ordering::Release);
            let entry = calls.entered.fetch_add(1, Ordering::AcqRel) + 1;
            Inside {
                tid: *tid,
                entry,
                calls: calls.clone(),
            }
        })
    }

    /// Without `/proc` a thread past `note` whose site has not said its sync
    /// returned ([`super::SyncNote`]) counts as in flight: what a cut also
    /// decides on Linux once its probe rounds run out.
    #[cfg(not(target_os = "linux"))]
    fn probe(_event: &SyncEvent) -> Completion {
        Completion::InFlight
    }

    /// Kernel numbers of the calls that flush a file.
    #[cfg(target_os = "linux")]
    fn is_sync_call(nr: i64) -> bool {
        [
            libc::SYS_fsync,
            libc::SYS_fdatasync,
            libc::SYS_sync_file_range,
            libc::SYS_syncfs,
            libc::SYS_msync,
            libc::SYS_sync,
        ]
        .contains(&nr)
    }

    /// The files `event`'s sync may have open: the file named, its
    /// directory (a directory sync names a file in it), and the temporary
    /// file that sites syncing before a rename name by its final path.
    #[cfg(target_os = "linux")]
    fn synced_paths(event: &SyncEvent) -> Vec<std::path::PathBuf> {
        let mut out = vec![event.path.clone()];
        if let Some(parent) = event.path.parent() {
            out.push(parent.to_path_buf());
        }
        if let Some(temp) = crate::core::power_loss::temp_synced_for(event.tag, &event.path) {
            out.push(temp);
        }
        out
    }

    pub(super) fn completion(event: &SyncEvent) -> Completion {
        let calls = THREADS
            .lock()
            .iter()
            .find(|(t, _)| *t == event.tid)
            .map(|(_, c)| c.clone());
        let Some(calls) = calls else {
            return Completion::Returned;
        };
        // Read before `inside`: entering sets `inside` first, so a thread
        // seen outside with this count has left the call that recorded it.
        let entered = calls.entered.load(Ordering::Acquire);
        if entered > event.entry || calls.returned.load(Ordering::Acquire) >= event.entry {
            return Completion::Returned;
        }
        if calls.inside.load(Ordering::Acquire) {
            return Completion::InFlight;
        }
        probe(event)
    }

    /// Where the thread stands, read from `/proc/self/task/<tid>/syscall`.
    #[cfg(target_os = "linux")]
    fn probe(event: &SyncEvent) -> Completion {
        let task = format!("/proc/self/task/{}/syscall", event.tid);
        let Ok(text) = std::fs::read_to_string(&task) else {
            // The thread is gone, and a thread does not exit inside a sync.
            return Completion::Returned;
        };
        let mut words = text.split_whitespace();
        let Some(Ok(nr)) = words.next().map(str::parse::<i64>) else {
            // "running": on a CPU, anywhere.
            return Completion::Unknown;
        };
        if nr < 0 {
            // Preempted in user space, before or after the sync.
            return Completion::Unknown;
        }
        if !is_sync_call(nr) {
            return Completion::Returned;
        }
        if nr == libc::SYS_sync || nr == libc::SYS_syncfs {
            return Completion::InFlight;
        }
        let fd = words
            .next()
            .and_then(|w| i64::from_str_radix(w.trim_start_matches("0x"), 16).ok());
        let target = fd.and_then(|fd| std::fs::read_link(format!("/proc/self/fd/{fd}")).ok());
        match target {
            Some(t) if !synced_paths(event).contains(&t) => Completion::Returned,
            _ => Completion::InFlight,
        }
    }
}

/// [`note`] for a sync of an open file whose path the caller does not hold,
/// such as a replay target opened by file id. The path is read back from the
/// descriptor, and only while something is watching.
#[inline]
pub fn note_file(tag: &'static str, file: &std::fs::File) -> SyncNote {
    if WATCHERS.load(Ordering::Relaxed) == 0 {
        return SyncNote::NONE;
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        if let Ok(path) = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd())) {
            return note(tag, &path);
        }
    }
    #[cfg(windows)]
    if let Some(path) = windows_path_of(file) {
        return note(tag, &path);
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    let _ = (tag, file);
    SyncNote::NONE
}

/// The path `file` was opened at, from its handle, spelled as the watched
/// directory it lies under spells it. Windows gives the long, `\\?\`-prefixed
/// form (`runneradmin`), and a watch may have been made on the short one
/// (`RUNNER~1`), so a match goes through each watched directory's canonical
/// form.
#[cfg(windows)]
fn windows_path_of(file: &std::fs::File) -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::io::AsRawHandle;
    unsafe extern "system" {
        fn GetFinalPathNameByHandleW(file: *mut std::ffi::c_void, path: *mut u16, len: u32, flags: u32) -> u32;
    }
    let mut buf = vec![0u16; 1024];
    loop {
        // SAFETY: an open handle and a buffer of the length passed.
        let n = unsafe { GetFinalPathNameByHandleW(file.as_raw_handle().cast(), buf.as_mut_ptr(), buf.len() as u32, 0) } as usize;
        if n == 0 {
            return None;
        }
        if n < buf.len() {
            buf.truncate(n);
            break;
        }
        buf.resize(n + 1, 0);
    }
    let strip = |p: PathBuf| -> PathBuf {
        let text = p.to_string_lossy().into_owned();
        PathBuf::from(text.strip_prefix(r"\\?\").map(str::to_string).unwrap_or(text))
    };
    let path = strip(PathBuf::from(std::ffi::OsString::from_wide(&buf)));
    for (dir, _) in WATCHED.lock().iter() {
        if let Ok(canonical) = std::fs::canonicalize(dir).map(strip)
            && let Ok(rest) = path.strip_prefix(&canonical)
        {
            return Some(dir.join(rest));
        }
    }
    Some(path)
}

/// Starts recording syncs of paths under `dir`; the guard stops it and hands
/// back what was seen.
pub fn watch(dir: &Path) -> Watch {
    WATCHED.lock().push((dir.to_path_buf(), Vec::new()));
    #[cfg(any(test, feature = "index_store_failpoints"))]
    EVENTS.lock().push((dir.to_path_buf(), Vec::new()));
    WATCHERS.fetch_add(1, Ordering::AcqRel);
    Watch {
        dir: dir.to_path_buf(),
    }
}

pub struct Watch {
    dir: PathBuf,
}

impl Watch {
    /// Everything recorded so far, and starts afresh.
    pub fn take(&self) -> Vec<(&'static str, PathBuf)> {
        let mut watched = WATCHED.lock();
        watched
            .iter_mut()
            .find(|(d, _)| *d == self.dir)
            .map(|(_, seen)| std::mem::take(seen))
            .unwrap_or_default()
    }

    /// Every sync recorded so far with its length and thread, in order, and
    /// starts afresh. Independent of [`Watch::take`].
    #[cfg(any(test, feature = "index_store_failpoints"))]
    pub fn take_events(&self) -> Vec<SyncEvent> {
        let mut events = EVENTS.lock();
        events
            .iter_mut()
            .find(|(d, _)| *d == self.dir)
            .map(|(_, seen)| std::mem::take(seen))
            .unwrap_or_default()
    }

    /// Syncs recorded so far, counted per tag, and starts afresh.
    pub fn counts(&self) -> std::collections::BTreeMap<&'static str, usize> {
        let mut out = std::collections::BTreeMap::new();
        for (tag, _) in self.take() {
            *out.entry(tag).or_insert(0) += 1;
        }
        out
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut watched = WATCHED.lock();
        if let Some(i) = watched.iter().position(|(d, _)| *d == self.dir) {
            watched.remove(i);
        }
        drop(watched);
        #[cfg(any(test, feature = "index_store_failpoints"))]
        {
            let mut events = EVENTS.lock();
            if let Some(i) = events.iter().position(|(d, _)| *d == self.dir) {
                events.remove(i);
            }
        }
        WATCHERS.fetch_sub(1, Ordering::AcqRel);
    }
}
