//! The engine's settings as a [`crate::Builder`] carries them to `start`.
//!
//! The engine keeps its settings in process-wide state, the way the server
//! binary's flags leave them. `apply` does for an embedding process what
//! `main` does for the server: it resolves each startup key from the Builder,
//! then `rasterizeddb.conf`, then the default, sets it before any engine
//! thread exists, and records where the running value came from for
//! `SHOW CONFIG`. The hot keys a session saved in `rasterizeddb.conf` are
//! applied here too, under the Builder's; the Builder's own hot keys are left
//! for the engine thread, which applies them through the same code `SET
//! CONFIG` runs.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use rasterizeddb_core::cache::query_cache;
use rasterizeddb_core::configuration::saved::{self, Source};
use rasterizeddb_core::core::index::paged;
use rasterizeddb_core::core::introspect;
use rasterizeddb_core::core::kv_cache::{self, EvictionPolicy};
use rasterizeddb_core::core::partition::budget as partition_budget;
use rasterizeddb_core::simds::cpu::{self, SimdLevel, SimdRequest};

/// Every key the engine reads once, before it starts (`SHOW CONFIG KEYS`
/// with `hot` false), and its default as `SHOW CONFIG` spells it.
/// `wal.enabled` is not here: it is fixed on.
pub(crate) const STARTUP_KEYS: &[(&str, &str)] = &[
    ("server.batch_size", "10000"),
    ("server.concurrent_threads", "16"),
    ("server.malloc_arenas", "0"),
    ("wal.checkpoint_bytes", "67108864"),
    ("wal.prealloc_kb", "1024"),
    // Before `cache.enabled`, which builds the cache at this size.
    ("cache.size", "100000"),
    ("cache.enabled", "false"),
    ("index.residency", "memory"),
    ("index.cache_mb", "0"),
    ("index.delta_mb", "0"),
    ("partition.max_open", "4096"),
];

/// What a Builder was told. `config` holds every `section.key` setting in
/// the order given, so the last word on a key wins whichever method said it.
pub(crate) struct Settings {
    pub config: Vec<(String, String)>,
    pub simd: Option<SimdLevel>,
    pub scan_prefilter: Option<bool>,
    pub tune_allocator: bool,
    pub idle_trim: bool,
    pub join_memory_mb: Option<usize>,
    pub kv_max_memory: Option<usize>,
    pub kv_eviction: Option<EvictionPolicy>,
    pub kv_persist: Option<bool>,
    pub kv_save_interval: Option<Duration>,
    pub json_shadow_tables: Option<bool>,
    pub settle_status: Option<bool>,
    pub raise_descriptor_limit: bool,
    pub wal_truncate_damaged: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            config: Vec::new(),
            simd: None,
            scan_prefilter: None,
            tune_allocator: false,
            idle_trim: true,
            join_memory_mb: None,
            kv_max_memory: None,
            kv_eviction: None,
            kv_persist: None,
            kv_save_interval: None,
            json_shadow_tables: None,
            settle_status: None,
            raise_descriptor_limit: false,
            wal_truncate_damaged: false,
        }
    }
}

/// What is left for the engine thread once [`Settings::apply`] has run.
pub(crate) struct Deferred {
    /// Hot keys, set before a database opens so that the opens see them.
    pub before_open: Vec<(String, String)>,
    /// Hot keys that act on the open database.
    pub after_open: Vec<(String, String)>,
    pub idle_trim: bool,
}

/// Who asked for a startup value.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    Builder,
    Saved,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// Sets a read-once value. `false` when the process already runs with
/// another one.
fn set_once<T: PartialEq>(lock: &OnceLock<T>, wanted: T) -> bool {
    match lock.set(wanted) {
        Ok(()) => true,
        Err(wanted) => lock.get() == Some(&wanted),
    }
}

/// A startup value that cannot take effect because the process already runs
/// with another: an error when the Builder asked for it (silently running
/// with something else is worse than not starting), a warning when the
/// settings file did, as a bad file never keeps the server from starting.
fn refused(key: &str, wanted: &str, origin: Origin) -> io::Result<bool> {
    let why = format!(
        "{key} = {wanted} cannot take effect: it is process-wide and read once, and an engine \
         started earlier in this process already runs with another value"
    );
    match origin {
        Origin::Builder => Err(invalid(why)),
        Origin::Saved => {
            log::warn!("{}: {why}", saved::FILE_NAME);
            Ok(false)
        }
    }
}

impl Settings {
    /// Checks everything, then sets the process-wide state the engine reads
    /// at start. `directory` is the database's, `None` in memory. Call before
    /// the engine's threads exist: the allocator's arena limit binds only
    /// arenas not yet made.
    pub fn apply(&self, directory: Option<&Path>) -> io::Result<Deferred> {
        // The last word on each key, split into the keys read once and the
        // hot ones, which keep the order they were given in.
        let mut startup: BTreeMap<String, String> = BTreeMap::new();
        let mut hot: Vec<(String, String)> = Vec::new();
        for (key, value) in &self.config {
            let key = key.trim().to_ascii_lowercase();
            match introspect::config_key_is_hot(&key) {
                None => {
                    return Err(invalid(format!(
                        "unknown config key `{key}`: they are `section.key`, and SHOW CONFIG KEYS lists them"
                    )));
                }
                Some(true) => {
                    hot.retain(|(k, _)| *k != key);
                    hot.push((key, value.clone()));
                }
                Some(false) => {
                    let (section, name) = key.split_once('.').expect("a known key is section.key");
                    if !STARTUP_KEYS.iter().any(|(k, _)| *k == key) {
                        return Err(invalid(format!("{key} cannot be set")));
                    }
                    if key == "cache.size" && value.trim() == "0" {
                        return Err(invalid(
                            "query_cache_entries (cache.size) must be above 0; turn the cache off with query_cache(false)",
                        ));
                    }
                    let canonical = introspect::startup_value(section, name, value)
                        .map_err(|e| invalid(format!("{key} = {value}: {e}")))?;
                    startup.insert(key, canonical);
                }
            }
        }
        if let Some(interval) = self.kv_save_interval
            && interval.as_secs() == 0
        {
            return Err(invalid("kv_save_interval must be at least one second"));
        }
        if self.join_memory_mb == Some(0) {
            return Err(invalid("join_memory_mb must be above 0: there is no unlimited setting"));
        }
        if let Some(level) = self.simd {
            cpu::init(SimdRequest::Fixed(level)).map_err(|e| invalid(format!("Builder::simd: {e}")))?;
        }

        let stored = match directory {
            Some(dir) => saved::load(dir).unwrap_or_else(|e| {
                log::warn!("{e}; starting without the saved settings");
                BTreeMap::new()
            }),
            None => {
                // No file of its own, and not an earlier engine's either.
                saved::unload();
                BTreeMap::new()
            }
        };

        // Before anything else allocates on a new thread.
        for (key, default) in STARTUP_KEYS {
            let wanted = match startup.get(*key) {
                Some(v) => Some((v.clone(), Origin::Builder)),
                None => stored.get(*key).and_then(|v| {
                    let (section, name) = key.split_once('.').expect("a startup key is section.key");
                    match introspect::startup_value(section, name, v) {
                        Ok(canonical) => Some((canonical, Origin::Saved)),
                        Err(e) => {
                            log::warn!("{}: {e}; ignored", saved::FILE_NAME);
                            None
                        }
                    }
                }),
            };
            let source = match &wanted {
                None => Source::Default,
                Some((value, origin)) => {
                    if self.set_startup(key, value, *origin)? {
                        match origin {
                            Origin::Builder => Source::CommandLine,
                            Origin::Saved => Source::Saved,
                        }
                    } else {
                        Source::Default
                    }
                }
            };
            saved::record(key, running(key).unwrap_or_else(|| default.to_string()), source);
        }
        // The hot keys a session saved, under the Builder's: a key the
        // Builder gives keeps the Builder's value, which the engine thread
        // sets.
        let given = |key: &str| hot.iter().any(|(k, _)| k == key);
        for warning in introspect::apply_saved_hot_keys(&stored, &given) {
            log::warn!("{warning}");
        }

        if let Some(on) = self.scan_prefilter {
            rasterizeddb_core::core::processor::prefilter::set_enabled(on);
        }
        if self.tune_allocator {
            rasterizeddb_core::tune_allocator();
        }
        if let Some(mb) = self.join_memory_mb
            && !rasterizeddb_core::core::rql::join::set_join_memory_mb(mb)
        {
            refused("join_memory_mb", &mb.to_string(), Origin::Builder)?;
        }
        if let Some(bytes) = self.kv_max_memory {
            kv_cache::set_max_memory(bytes);
        }
        if let Some(policy) = self.kv_eviction {
            kv_cache::set_eviction_policy(policy);
        }
        if let Some(on) = self.kv_persist {
            kv_cache::set_persist(on);
        }
        if let Some(interval) = self.kv_save_interval {
            kv_cache::set_save_interval_secs(interval.as_secs()).map_err(invalid)?;
        }
        if let Some(on) = self.json_shadow_tables
            && !set_once(&rasterizeddb_core::JSON_SHADOW_TABLES, on)
        {
            refused("json_shadow_tables", &on.to_string(), Origin::Builder)?;
        }
        if let Some(on) = self.settle_status {
            rasterizeddb_core::core::mvcc::status::set_settle_on_open(on);
        }
        // Every start, off too: it is this start's decision, and an earlier
        // start in the process must not leave it on.
        rasterizeddb_core::core::wal::set_truncate_damaged(self.wal_truncate_damaged);
        if self.raise_descriptor_limit {
            let (before, now, budget) = rasterizeddb_core::core::partition::budget::raise_descriptor_limit();
            log::info!("descriptors: soft limit {now} (was {before}); partition open budget {budget}");
        }

        // `replication.*` and `vacuum.enabled` act on the open database;
        // every other hot key is process-wide state the opens read.
        let (after_open, before_open) = hot
            .into_iter()
            .partition(|(key, _)| key.starts_with("replication.") || key.starts_with("vacuum."));
        Ok(Deferred {
            before_open,
            after_open,
            idle_trim: self.idle_trim,
        })
    }

    /// Sets one startup key to its checked value. `Ok(false)` when it could
    /// not take effect and that is not an error (see [`refused`]).
    fn set_startup(&self, key: &str, value: &str, origin: Origin) -> io::Result<bool> {
        let number = || value.parse::<u64>().expect("startup_value checked the number");
        let took = match key {
            "server.batch_size" => set_once(&rasterizeddb_core::BATCH_SIZE, number() as usize),
            "server.concurrent_threads" => set_once(&rasterizeddb_core::MAX_PERMITS_THREADS, number() as usize),
            "server.malloc_arenas" => {
                let n = number() as usize;
                if n == rasterizeddb_core::malloc_arenas() {
                    true
                } else if n == 0 {
                    // A limit once set is not lifted: arenas are not unmade.
                    false
                } else if rasterizeddb_core::set_malloc_arenas(n) {
                    true
                } else {
                    // No such knob off glibc. A tuning hint, so the engine
                    // starts without it, as the server does.
                    log::warn!("server.malloc_arenas = {n}: this platform's allocator has no arena limit; ignored");
                    return Ok(false);
                }
            }
            "wal.checkpoint_bytes" => set_once(&rasterizeddb_core::WAL_CHECKPOINT_BYTES, number()),
            "wal.prealloc_kb" => set_once(&rasterizeddb_core::WAL_PREALLOC_BYTES, number().saturating_mul(1024)),
            "cache.size" => {
                let n = number() as usize;
                match query_cache::QUERY_CACHE.get() {
                    // Built by an earlier engine in this process.
                    Some(_) => query_cache::entries() == n,
                    None => {
                        query_cache::set_entries(n);
                        true
                    }
                }
            }
            "cache.enabled" => {
                let on = value == "true";
                if on {
                    // Built before the flag is set, so a query never sees a
                    // half-configured cache; at `cache.size`, set before it.
                    query_cache::init(query_cache::entries());
                }
                set_once(&query_cache::ENABLE_CACHE, on)
            }
            "index.residency" => {
                paged::set_residency(paged::Residency::parse(value).expect("startup_value checked the residency"));
                true
            }
            "index.cache_mb" => {
                paged::set_cache_mb(number());
                true
            }
            "index.delta_mb" => {
                paged::set_delta_mb(number());
                true
            }
            "partition.max_open" => {
                partition_budget::settings::set_max_open(number() as usize);
                true
            }
            other => unreachable!("{other} is in STARTUP_KEYS and has no setter"),
        };
        if took { Ok(true) } else { refused(key, value, origin) }
    }
}

/// A startup key's running value, as `SHOW CONFIG` spells it. Reading the
/// two log settings fixes them, which a database's first log record would
/// do anyway.
fn running(key: &str) -> Option<String> {
    Some(match key {
        "server.batch_size" => rasterizeddb_core::batch_size().to_string(),
        "server.concurrent_threads" => rasterizeddb_core::max_permits_threads().to_string(),
        "server.malloc_arenas" => rasterizeddb_core::malloc_arenas().to_string(),
        "wal.checkpoint_bytes" => rasterizeddb_core::wal_checkpoint_bytes().to_string(),
        "wal.prealloc_kb" => (rasterizeddb_core::wal_prealloc_bytes() / 1024).to_string(),
        "cache.size" => query_cache::entries().to_string(),
        "cache.enabled" => query_cache::enabled().to_string(),
        "index.residency" => paged::residency().name().to_string(),
        "index.cache_mb" => paged::cache_mb_setting().to_string(),
        "index.delta_mb" => paged::delta_mb_setting().to_string(),
        "partition.max_open" => partition_budget::settings::max_open().to_string(),
        _ => return None,
    })
}
