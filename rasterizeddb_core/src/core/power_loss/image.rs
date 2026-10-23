//! Crash images: what a directory could hold after a power cut at a chosen
//! instant. See the module notes in [`super`] for the model.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc as StdArc, Weak};

use parking_lot::Mutex;

use crate::core::sync_audit::{self, Completion, DirOp, SyncEvent};

pub(crate) const PAGE: usize = 4096;

/// How long a cut waits for another thread's sync to show where it stands
/// before counting it in flight.
const PROBE_ROUNDS: usize = 200;

pub(crate) fn current_tid() -> i32 {
    (unsafe { libc::syscall(libc::SYS_gettid) }) as i32
}

/// xorshift64*, so a failing seed reproduces without a dependency.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    /// Uniform in `[0, 1)`.
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// A sync recorded and not yet known to have returned.
struct Pending {
    event: SyncEvent,
    /// The inode the sync flushes and its bytes when issued; `None` for a
    /// directory sync.
    file: Option<(u64, Vec<u8>)>,
}

/// A hooked directory operation: each name it touches, with the inode that
/// name held before it (`None`: absent).
#[derive(Clone)]
struct Op {
    seq: u64,
    dir: PathBuf,
    durable: bool,
    affected: Vec<(PathBuf, Option<u64>)>,
}

struct State {
    dir: PathBuf,
    /// Every inode at the start, durable whole.
    baseline: HashMap<u64, Vec<u8>>,
    /// Per inode, its bytes at the latest completed sync, by issue order.
    durable: HashMap<u64, (u64, Vec<u8>)>,
    pending: Vec<Pending>,
    ops: Vec<Op>,
    /// Inodes an operation removed or replaced, as they were just before.
    victims: HashMap<u64, Vec<u8>>,
    /// Inodes some recorded sync touched: never taken whole.
    synced_inodes: HashSet<u64>,
}

/// Recorders by directory. The directory sits beside the state so finding
/// one never waits on a cut holding its lock.
static RECORDERS: Mutex<Vec<(PathBuf, Weak<Mutex<State>>)>> = Mutex::new(Vec::new());

fn recorders_for(path: &Path) -> Vec<StdArc<Mutex<State>>> {
    let mut list = RECORDERS.lock();
    list.retain(|(_, w)| w.strong_count() > 0);
    list.iter()
        .filter(|(dir, _)| path.starts_with(dir))
        .filter_map(|(_, w)| w.upgrade())
        .collect()
}

fn tag_flushes_bytes(tag: &str) -> bool {
    super::SYNC_TAGS.iter().any(|(t, bytes)| *t == tag && *bytes)
}

/// Every regular file under `dir`, recursively, by path relative to it.
pub(crate) fn walk(dir: &Path) -> Vec<PathBuf> {
    fn go(base: &Path, at: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(at) else { return };
        for e in entries.flatten() {
            let Ok(ty) = e.file_type() else { continue };
            let p = e.path();
            if ty.is_dir() {
                go(base, &p, out);
            } else if ty.is_file()
                && let Ok(rel) = p.strip_prefix(base)
            {
                out.push(rel.to_path_buf());
            }
        }
    }
    let mut out = Vec::new();
    go(dir, dir, &mut out);
    out.sort();
    out
}

fn ino_of(path: &Path) -> Option<u64> {
    std::fs::symlink_metadata(path).ok().map(|m| m.ino())
}

impl State {
    fn promote(&mut self, p: Pending) {
        if let Some((ino, bytes)) = p.file {
            let newer = self.durable.get(&ino).is_none_or(|(seq, _)| *seq < p.event.seq);
            if newer {
                self.durable.insert(ino, (p.event.seq, bytes));
            }
        } else if let Some(dir) = super::dir_synced_by(p.event.tag, &p.event.path) {
            for op in self.ops.iter_mut() {
                if op.seq < p.event.seq && op.dir == dir {
                    op.durable = true;
                }
            }
        }
    }

    fn record(&mut self, e: &SyncEvent) {
        // Every earlier sync of this thread has returned.
        let (done, keep): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|p| p.event.tid == e.tid && p.event.entry < e.entry);
        self.pending = keep;
        for p in done {
            self.promote(p);
        }
        let file = if tag_flushes_bytes(e.tag) {
            let target = super::temp_synced_for(e.tag, &e.path)
                .filter(|t| t.exists())
                .unwrap_or_else(|| e.path.clone());
            match (ino_of(&target), std::fs::read(&target)) {
                (Some(ino), Ok(bytes)) => {
                    self.synced_inodes.insert(ino);
                    Some((ino, bytes))
                }
                // Gone already: the sync will fail or flush nothing here.
                _ => return,
            }
        } else if super::dir_synced_by(e.tag, &e.path).is_some() {
            None
        } else {
            return;
        };
        self.pending.push(Pending { event: e.clone(), file });
    }

    fn record_op(&mut self, op: DirOp<'_>, seq: u64) {
        let rel = |p: &Path| p.strip_prefix(&self.dir).map(Path::to_path_buf).ok();
        let mut affected = Vec::new();
        let mut touched: Vec<&Path> = Vec::new();
        match op {
            DirOp::Create(p) | DirOp::Unlink(p) => touched.push(p),
            DirOp::Rename { from, to } => {
                touched.push(from);
                touched.push(to);
            }
        }
        let Some(dir) = touched[0].parent().map(Path::to_path_buf) else {
            return;
        };
        for p in touched {
            let Some(r) = rel(p) else { return };
            let prior = ino_of(p);
            if let Some(ino) = prior
                && let Ok(bytes) = std::fs::read(p)
            {
                self.victims.insert(ino, bytes);
            }
            affected.push((r, prior));
        }
        self.ops.push(Op {
            seq,
            dir,
            durable: false,
            affected,
        });
    }
}

pub(crate) fn on_sync(event: &SyncEvent) {
    for r in recorders_for(&event.path) {
        r.lock().record(event);
    }
}

pub(crate) fn on_dir_op(op: DirOp<'_>, seq: u64) {
    let probe = match op {
        DirOp::Create(p) | DirOp::Unlink(p) => p,
        DirOp::Rename { to, .. } => to,
    };
    for r in recorders_for(probe) {
        r.lock().record_op(op, seq);
    }
}

/// Watches one directory and freezes crash points from it.
pub(crate) struct Recorder {
    state: StdArc<Mutex<State>>,
    _watch: sync_audit::Watch,
}

/// Where an inode's image starts from.
#[derive(Clone)]
enum Base {
    /// Its bytes at the last completed sync, or at the baseline, or nothing
    /// for an inode no sync has completed on.
    Synced(Vec<u8>),
    /// Created by an unhooked path and never synced where `sync_audit` can
    /// see: taken as it is.
    Whole,
}

/// One frozen instant, imaged any number of times.
pub(crate) struct CrashPoint {
    pub label: String,
    dir: PathBuf,
    /// Every file at the cut: inode and bytes.
    files: BTreeMap<PathBuf, (u64, Vec<u8>)>,
    victims: HashMap<u64, Vec<u8>>,
    bases: HashMap<u64, Base>,
    /// Operations not yet durable, oldest first.
    ops: Vec<Op>,
    /// What the cut saw in flight, for messages.
    pub in_flight: Vec<String>,
}

impl Recorder {
    /// Syncs every file under `dir` and the directory itself, takes that as
    /// durable, and records from here on.
    pub(crate) fn start(dir: &Path) -> StdArc<Recorder> {
        let mut baseline = HashMap::new();
        for rel in walk(dir) {
            let path = dir.join(&rel);
            let f = std::fs::File::options().read(true).write(true).open(&path).unwrap();
            f.sync_all().unwrap();
            let ino = f.metadata().unwrap().ino();
            baseline.insert(ino, std::fs::read(&path).unwrap());
        }
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        let state = StdArc::new(Mutex::new(State {
            dir: dir.to_path_buf(),
            baseline,
            durable: HashMap::new(),
            pending: Vec::new(),
            ops: Vec::new(),
            victims: HashMap::new(),
            synced_inodes: HashSet::new(),
        }));
        RECORDERS.lock().push((dir.to_path_buf(), StdArc::downgrade(&state)));
        let watch = sync_audit::watch(dir);
        StdArc::new(Recorder { state, _watch: watch })
    }

    /// Cuts with no sync of the calling thread in flight: between statements.
    pub(crate) fn cut_quiescent(&self, label: impl Into<String>) -> Result<CrashPoint, String> {
        self.cut(label.into(), false)
    }

    /// Cuts from inside a `sync:<tag>` failpoint: the calling thread's
    /// latest sync is the one about to be issued, so it is in flight.
    pub(crate) fn cut_in_sync(&self, label: impl Into<String>) -> Result<CrashPoint, String> {
        self.cut(label.into(), true)
    }

    /// An action for [`super::failpoint::arm`] that cuts here and pushes the
    /// point (or why it was discarded) to `sink`.
    pub(crate) fn cut_action(
        self: &StdArc<Self>,
        label: String,
        sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>>,
    ) -> super::failpoint::Action {
        let me = self.clone();
        StdArc::new(move |hit: &super::failpoint::Hit<'_>| {
            let label = format!("{label}, at {} hit {} on {}", hit.name, hit.nth, hit.path.display());
            sink.lock().push(me.cut_in_sync(label));
            super::failpoint::Outcome::Continue
        })
    }

    fn cut(&self, label: String, inside_sync: bool) -> Result<CrashPoint, String> {
        let me = current_tid();
        let mut st = self.state.lock();
        // Settle every sync recorded so far: returned ones become durable.
        let pending = std::mem::take(&mut st.pending);
        let mut undecided = Vec::new();
        let mut in_flight = Vec::new();
        for p in pending {
            if p.event.tid == me {
                // Recording promoted this thread's earlier syncs; what is
                // left is the one this failpoint is about to issue, or, at
                // a quiescent cut, syncs that have all returned.
                if inside_sync {
                    in_flight.push(p);
                } else {
                    st.promote(p);
                }
            } else {
                undecided.push(p);
            }
        }
        for round in 0..PROBE_ROUNDS {
            let mut still = Vec::new();
            for p in undecided {
                match sync_audit::completion(&p.event) {
                    Completion::Returned => st.promote(p),
                    Completion::InFlight => in_flight.push(p),
                    Completion::Unknown => still.push(p),
                }
            }
            undecided = still;
            if undecided.is_empty() {
                break;
            }
            if round + 1 < PROBE_ROUNDS {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        in_flight.append(&mut undecided);

        let dir = st.dir.clone();
        let mut files = BTreeMap::new();
        for rel in walk(&dir) {
            let path = dir.join(&rel);
            if let (Some(ino), Ok(bytes)) = (ino_of(&path), std::fs::read(&path)) {
                files.insert(rel, (ino, bytes));
            }
        }
        // A sync counted in flight that returned while the files were read
        // lets its thread write what assumed it durable (a commit's data
        // after its log), and the image could keep that write while losing
        // the log. Such a cut is not an instant; discard it.
        let other_in_flight: Vec<&Pending> = in_flight.iter().filter(|p| p.event.tid != me).collect();
        for p in &other_in_flight {
            if sync_audit::completion(&p.event) != Completion::InFlight {
                let why = format!(
                    "{label}: disturbed, {} sync of {} by thread {} returned during the cut",
                    p.event.tag,
                    p.event.path.display(),
                    p.event.tid
                );
                st.pending.extend(in_flight);
                return Err(why);
            }
        }
        let names: Vec<String> = in_flight
            .iter()
            .map(|p| format!("{} {} (thread {})", p.event.tag, p.event.path.display(), p.event.tid))
            .collect();
        st.pending.extend(in_flight);

        let mut victims = HashMap::new();
        let ops: Vec<Op> = st.ops.iter().filter(|o| !o.durable).cloned().collect();
        for op in &ops {
            for (_, prior) in &op.affected {
                if let Some(ino) = prior
                    && let Some(b) = st.victims.get(ino)
                {
                    victims.insert(*ino, b.clone());
                }
            }
        }
        // Inodes a hooked operation put in place follow the page model even
        // if no sync touched them: their creation was announced.
        let mut announced: HashSet<u64> = HashSet::new();
        for op in &st.ops {
            for (name, _) in &op.affected {
                if let Some((ino, _)) = files.get(name) {
                    announced.insert(*ino);
                }
            }
        }
        let mut bases = HashMap::new();
        let inodes: Vec<u64> = files.values().map(|(i, _)| *i).chain(victims.keys().copied()).collect();
        for ino in inodes {
            let base = if let Some((_, b)) = st.durable.get(&ino) {
                Base::Synced(b.clone())
            } else if let Some(b) = st.baseline.get(&ino) {
                Base::Synced(b.clone())
            } else if st.synced_inodes.contains(&ino) || announced.contains(&ino) {
                Base::Synced(Vec::new())
            } else {
                Base::Whole
            };
            bases.insert(ino, base);
        }
        Ok(CrashPoint {
            label,
            dir,
            files,
            victims,
            bases,
            ops,
            in_flight: names,
        })
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let mut list = RECORDERS.lock();
        list.retain(|(_, w)| w.upgrade().is_some_and(|s| !StdArc::ptr_eq(&s, &self.state)));
    }
}

/// How one seed treats what is not durable.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SeedKind {
    /// Keep probability for an unsynced page and an undurable operation.
    pub p: f64,
    /// Logs cut at their synced length whatever `p` keeps elsewhere.
    pub logs_at_synced: bool,
}

pub(crate) fn seed_kind(seed: u64, rng: &mut Rng) -> SeedKind {
    match seed % 5 {
        0 => SeedKind { p: 0.0, logs_at_synced: false },
        1 => SeedKind { p: 1.0, logs_at_synced: false },
        2 => SeedKind { p: 1.0, logs_at_synced: true },
        3 => SeedKind { p: 0.5, logs_at_synced: false },
        _ => SeedKind { p: rng.unit(), logs_at_synced: false },
    }
}

fn keep(p: f64, rng: &mut Rng) -> bool {
    if p >= 1.0 {
        true
    } else if p <= 0.0 {
        false
    } else {
        rng.unit() < p
    }
}

/// `base` with each 4 KiB page that differs from `current` kept (current
/// bytes) or dropped (base bytes, zeros past its end), at a length between
/// the two.
pub(crate) fn mix_pages(base: &[u8], current: &[u8], p: f64, at_base_len: bool, rng: &mut Rng) -> Vec<u8> {
    let len = if at_base_len || p <= 0.0 {
        base.len()
    } else if p >= 1.0 {
        current.len()
    } else {
        let (lo, hi) = (base.len().min(current.len()), base.len().max(current.len()));
        lo + rng.below((hi - lo) as u64 + 1) as usize
    };
    let mut out = vec![0u8; len];
    let mut at = 0;
    while at < len {
        let end = (at + PAGE).min(len);
        let differs = (at..end).any(|j| current.get(j) != base.get(j));
        let kept = differs && keep(p, rng);
        for (j, b) in out[at..end].iter_mut().enumerate() {
            let j = at + j;
            let synced = base.get(j).copied().unwrap_or(0);
            *b = if kept { current.get(j).copied().unwrap_or(synced) } else { synced };
        }
        at = end;
    }
    out
}

fn is_log(rel: &Path) -> bool {
    rel.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with(crate::core::wal::DEFAULT_WAL_NAME))
}

impl CrashPoint {
    /// Bytes held, for the scenario bound.
    pub(crate) fn held_bytes(&self) -> u64 {
        let f: usize = self.files.values().map(|(_, b)| b.len()).sum();
        let v: usize = self.victims.values().map(Vec::len).sum();
        let b: usize = self
            .bases
            .values()
            .map(|b| match b {
                Base::Synced(v) => v.len(),
                Base::Whole => 0,
            })
            .sum();
        (f + v + b) as u64
    }

    /// Each file at the cut with its inode, current length and where its
    /// image starts from, and the undurable operations: for failure reports.
    pub(crate) fn describe(&self) -> String {
        let mut out = String::new();
        for (name, (ino, bytes)) in &self.files {
            let base = match self.bases.get(ino) {
                Some(Base::Synced(b)) => format!("synced {} bytes", b.len()),
                Some(Base::Whole) => "whole".to_string(),
                None => "?".to_string(),
            };
            out.push_str(&format!("\n  {} ino {ino}: {} bytes now, {base}", name.display(), bytes.len()));
        }
        for op in &self.ops {
            out.push_str(&format!("\n  undurable op {:?}", op.affected));
        }
        out
    }

    /// Names (relative) and bytes of the image for `seed`.
    pub(crate) fn image(&self, seed: u64) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for b in self.label.bytes() {
            h = (h ^ b as u64).wrapping_mul(0x100_0000_01b3);
        }
        let mut rng = Rng::new(seed ^ h);
        let kind = seed_kind(seed, &mut rng);
        let mut names: BTreeMap<PathBuf, u64> =
            self.files.iter().map(|(n, (ino, _))| (n.clone(), *ino)).collect();
        let n = self.ops.len();
        let kept_ops = if kind.p <= 0.0 {
            0
        } else if kind.p >= 1.0 {
            n
        } else {
            rng.below(n as u64 + 1) as usize
        };
        for op in self.ops[kept_ops..].iter().rev() {
            for (name, prior) in op.affected.iter().rev() {
                match prior {
                    Some(ino) => names.insert(name.clone(), *ino),
                    None => names.remove(name),
                };
            }
        }
        let current: HashMap<u64, &Vec<u8>> = self
            .files
            .values()
            .map(|(i, b)| (*i, b))
            .chain(self.victims.iter().map(|(i, b)| (*i, b)))
            .collect();
        let mut out = BTreeMap::new();
        for (name, ino) in names {
            let Some(now) = current.get(&ino) else { continue };
            let bytes = match self.bases.get(&ino) {
                Some(Base::Synced(base)) => {
                    mix_pages(base, now, kind.p, kind.logs_at_synced && is_log(&name), &mut rng)
                }
                Some(Base::Whole) | None => (*now).clone(),
            };
            out.insert(name, bytes);
        }
        out
    }

    /// Writes the image for `seed` into `out`, a fresh directory, with the
    /// storage pool file's absolute paths pointed at it.
    pub(crate) fn write_image(&self, seed: u64, out: &Path) {
        std::fs::create_dir_all(out).unwrap();
        for (name, bytes) in self.image(seed) {
            let path = out.join(&name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&path, bytes).unwrap();
        }
        #[cfg(feature = "storage_pools")]
        let pool = out.join(crate::core::storage::POOL_FILE);
        #[cfg(feature = "storage_pools")]
        if let Ok(text) = std::fs::read_to_string(&pool) {
            let from = self.dir.to_string_lossy().into_owned();
            let to = out.to_string_lossy().into_owned();
            std::fs::write(&pool, text.replace(&from, &to)).unwrap();
        }
    }
}
