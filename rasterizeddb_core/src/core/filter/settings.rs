//! Runtime-tunable filter settings, resolved per column.
//!
//! Three layers, most specific wins:
//!
//! ```text
//! column override   SET FILTER max_f = 20 FOR COLUMN users.email
//!      ↓ falls back to
//! table override    SET FILTER max_f = 12 FOR TABLE users
//!      ↓ falls back to
//! global default    SET FILTER max_f = 16      /  --filter-max-f 16
//! ```
//!
//! Every layer is partial: an override names only the knobs it cares about and
//! inherits the rest. That is what makes per-column tuning practical — a table
//! usually wants one column widened, not a whole config restated.
//!
//! Settings are read on the paths that build filters, so a change takes effect
//! for tables opened afterwards and for columns re-sized at their next zone
//! seal. Changing a width does not rewrite existing generations; old zones keep
//! the width they were sealed with, which is exactly how growth already works.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;

use super::column::FilterConfig;

/// Largest share of a table a mask may leave as candidates and still be worth
/// using on a scan that reads everything.
///
/// A mask costs a per-record test and saves a row fetch on each record it
/// excludes, so the break-even is `1 - test/fetch`. Both costs are measured, by
/// `cost_ratio.rs` in `rasterizeddb_filter_tests`: a masked-out slot is
/// **0.17–0.41 ns** — the whole-word skip clears 64 slots per test, and a range
/// predicate excludes contiguously — against **12–52 ns** for a record actually
/// fetched. That puts break-even between **0.970** (narrow rows, cached, on a
/// contended machine) and **0.996** (2 KB rows, 400 MB of row file).
///
/// 0.90 sits under the whole measured range with margin. It was 0.5, which
/// discarded masks that paid for themselves several times over; the figure was
/// picked before either cost had a number under it.
///
/// The margin only has to cover error in one direction. Nothing about a larger
/// or colder table makes the *mask* dearer — it is one bit per row and stays
/// resident — while it makes every fetch dearer, so break-even can only move
/// away from this default, never toward it.
///
/// A mask that lets a scan **start late** is kept regardless of this: see
/// `build_candidates`, where a long leading run of excluded slots is what
/// decides the cost of a `LIMIT` query rather than the overall ratio.
pub const DEFAULT_MIN_PRUNING_RATIO: f64 = 0.90;

/// A partial override. `None` means "inherit from the layer below".
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Overrides {
    /// Exclude this column (or every column of this table) from the index.
    pub indexed: Option<bool>,
    /// Maintain per-zone min/max bounds. Turning this off keeps fingerprints
    /// and drops range pruning, which is the right trade for a column whose
    /// values have no locality — random UUIDs, hashes — where the bounds of
    /// every zone span the space and prune nothing.
    pub zone_bounds: Option<bool>,
    /// Keep the per-superzone span summary (§3's L0/L1 tier).
    ///
    /// Off by default, and deliberately: it reduces how many plane *bytes* a
    /// probe streams and does nothing for how many rows it falsely fetches. Turn
    /// it on for a column that takes heavy point-lookup traffic and holds few
    /// distinct values per 16 384-row span — see [`super::bloom`] for the
    /// arithmetic that says when it pays.
    pub bloom: Option<bool>,
    pub min_f: Option<u8>,
    pub default_f: Option<u8>,
    pub max_f: Option<u8>,
    pub min_selectivity: Option<f64>,
    pub min_table_rows: Option<u64>,
}

impl Overrides {
    pub fn is_empty(&self) -> bool {
        *self == Overrides::default()
    }

    /// Layer `self` on top of `base`, with `self` winning where it is set.
    fn merge_onto(&self, base: &Overrides) -> Overrides {
        Overrides {
            indexed: self.indexed.or(base.indexed),
            zone_bounds: self.zone_bounds.or(base.zone_bounds),
            bloom: self.bloom.or(base.bloom),
            min_f: self.min_f.or(base.min_f),
            default_f: self.default_f.or(base.default_f),
            max_f: self.max_f.or(base.max_f),
            min_selectivity: self.min_selectivity.or(base.min_selectivity),
            min_table_rows: self.min_table_rows.or(base.min_table_rows),
        }
    }

    /// Apply to a concrete config, clamping so a bad pair cannot invert the
    /// bounds — `min_f > max_f` would make `choose_f` clamp to nonsense.
    fn apply(&self, mut cfg: FilterConfig) -> FilterConfig {
        if let Some(v) = self.min_f {
            cfg.min_f = v.clamp(super::kernels::MIN_F, super::kernels::MAX_F);
        }
        if let Some(v) = self.max_f {
            cfg.max_f = v.clamp(super::kernels::MIN_F, super::kernels::MAX_F);
        }
        if cfg.min_f > cfg.max_f {
            cfg.min_f = cfg.max_f;
        }
        if let Some(v) = self.default_f {
            cfg.default_f = v.clamp(cfg.min_f, cfg.max_f);
        } else {
            cfg.default_f = cfg.default_f.clamp(cfg.min_f, cfg.max_f);
        }
        if let Some(v) = self.min_selectivity {
            // 0 would demand infinite distinct values; >1 would index everything.
            cfg.min_selectivity = v.clamp(f64::MIN_POSITIVE, 1.0);
        }
        if let Some(v) = self.min_table_rows {
            cfg.min_table_rows = v;
        }
        cfg
    }
}

/// Everything resolved for one column.
#[derive(Clone, Copy, Debug)]
pub struct Resolved {
    pub cfg: FilterConfig,
    pub indexed: bool,
    pub zone_bounds: bool,
    pub bloom: bool,
}

#[derive(Default)]
struct TableEntry {
    table: Overrides,
    columns: HashMap<String, Overrides>,
}

pub struct FilterSettings {
    enabled: AtomicBool,
    backfill_on_open: AtomicBool,
    /// `f64` bits, so it can live in an atomic.
    min_pruning_ratio: AtomicU64,
    checkpoint_interval_secs: AtomicU64,
    global: RwLock<Overrides>,
    tables: RwLock<HashMap<String, TableEntry>>,
}

impl Default for FilterSettings {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            backfill_on_open: AtomicBool::new(true),
            min_pruning_ratio: AtomicU64::new(DEFAULT_MIN_PRUNING_RATIO.to_bits()),
            checkpoint_interval_secs: AtomicU64::new(10),
            global: RwLock::new(Overrides::default()),
            tables: RwLock::new(HashMap::new()),
        }
    }
}

static SETTINGS: OnceLock<FilterSettings> = OnceLock::new();

pub fn settings() -> &'static FilterSettings {
    SETTINGS.get_or_init(FilterSettings::default)
}

/// Case-insensitive, so `Users` and `users` are one table.
fn key(s: &str) -> String {
    s.trim().trim_matches('"').to_ascii_lowercase()
}

impl FilterSettings {
    // -- global toggles -----------------------------------------------------

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    pub fn set_enabled(&self, v: bool) {
        self.enabled.store(v, Ordering::Relaxed);
    }

    pub fn backfill_on_open(&self) -> bool {
        self.backfill_on_open.load(Ordering::Relaxed)
    }
    pub fn set_backfill_on_open(&self, v: bool) {
        self.backfill_on_open.store(v, Ordering::Relaxed);
    }

    pub fn min_pruning_ratio(&self) -> f64 {
        f64::from_bits(self.min_pruning_ratio.load(Ordering::Relaxed))
    }
    pub fn set_min_pruning_ratio(&self, v: f64) {
        self.min_pruning_ratio
            .store(v.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    pub fn checkpoint_interval_secs(&self) -> u64 {
        self.checkpoint_interval_secs.load(Ordering::Relaxed).max(1)
    }
    pub fn set_checkpoint_interval_secs(&self, v: u64) {
        self.checkpoint_interval_secs
            .store(v.max(1), Ordering::Relaxed);
    }

    // -- overrides ----------------------------------------------------------

    pub fn global_overrides(&self) -> Overrides {
        *self.global.read()
    }

    pub fn update_global(&self, f: impl FnOnce(&mut Overrides)) {
        f(&mut self.global.write());
    }

    pub fn update_table(&self, table: &str, f: impl FnOnce(&mut Overrides)) {
        let mut g = self.tables.write();
        f(&mut g.entry(key(table)).or_default().table);
    }

    pub fn update_column(&self, table: &str, column: &str, f: impl FnOnce(&mut Overrides)) {
        let mut g = self.tables.write();
        let e = g.entry(key(table)).or_default();
        f(e.columns.entry(key(column)).or_default());
    }

    pub fn table_overrides(&self, table: &str) -> Overrides {
        self.tables
            .read()
            .get(&key(table))
            .map(|e| e.table)
            .unwrap_or_default()
    }

    pub fn column_overrides(&self, table: &str, column: &str) -> Overrides {
        self.tables
            .read()
            .get(&key(table))
            .and_then(|e| e.columns.get(&key(column)).copied())
            .unwrap_or_default()
    }

    /// Column names with an override on `table`, sorted for stable output.
    pub fn overridden_columns(&self, table: &str) -> Vec<String> {
        let g = self.tables.read();
        let mut v: Vec<String> = g
            .get(&key(table))
            .map(|e| e.columns.keys().cloned().collect())
            .unwrap_or_default();
        v.sort();
        v
    }

    pub fn overridden_tables(&self) -> Vec<String> {
        let mut v: Vec<String> = self.tables.read().keys().cloned().collect();
        v.sort();
        v
    }

    pub fn reset_global(&self) {
        *self.global.write() = Overrides::default();
        self.set_enabled(true);
        self.set_backfill_on_open(true);
        self.set_min_pruning_ratio(DEFAULT_MIN_PRUNING_RATIO);
        self.set_checkpoint_interval_secs(10);
    }

    pub fn reset_table(&self, table: &str) {
        self.tables.write().remove(&key(table));
    }

    pub fn reset_column(&self, table: &str, column: &str) {
        if let Some(e) = self.tables.write().get_mut(&key(table)) {
            e.columns.remove(&key(column));
        }
    }

    pub fn reset_all(&self) {
        self.tables.write().clear();
        self.reset_global();
    }

    // -- resolution ---------------------------------------------------------

    /// The effective settings for one column, after layering.
    pub fn resolve(&self, table: &str, column: &str) -> Resolved {
        let merged = self
            .column_overrides(table, column)
            .merge_onto(&self.table_overrides(table))
            .merge_onto(&self.global_overrides());

        Resolved {
            cfg: merged.apply(FilterConfig::default()),
            indexed: merged.indexed.unwrap_or(true),
            zone_bounds: merged.zone_bounds.unwrap_or(true),
            bloom: merged.bloom.unwrap_or(false),
        }
    }

    /// Whether the filter is active for a table at all.
    pub fn table_enabled(&self, table: &str) -> bool {
        if !self.enabled() {
            return false;
        }
        self.table_overrides(table).indexed.unwrap_or(true)
    }

    /// A human-readable dump of the layer that applies at `scope`.
    pub fn describe(&self, table: Option<&str>, column: Option<&str>) -> Vec<String> {
        let mut out = Vec::new();
        match (table, column) {
            (None, _) => {
                out.push(format!("enabled = {}", self.enabled()));
                out.push(format!("backfill_on_open = {}", self.backfill_on_open()));
                out.push(format!("min_pruning_ratio = {}", self.min_pruning_ratio()));
                out.push(format!(
                    "checkpoint_interval_secs = {}",
                    self.checkpoint_interval_secs()
                ));
                let r = self.resolve("", "");
                out.push(format!("min_f = {}", r.cfg.min_f));
                out.push(format!("default_f = {}", r.cfg.default_f));
                out.push(format!("max_f = {}", r.cfg.max_f));
                out.push(format!("min_selectivity = {}", r.cfg.min_selectivity));
                out.push(format!("min_table_rows = {}", r.cfg.min_table_rows));
                out.push(format!("zone_bounds = {}", r.zone_bounds));
                out.push(format!("bloom = {}", r.bloom));
                let tables = self.overridden_tables();
                if !tables.is_empty() {
                    out.push(format!("tables with overrides = {}", tables.join(", ")));
                }
            }
            (Some(t), None) => {
                let r = self.resolve(t, "");
                out.push(format!("table = {}", t));
                out.push(format!("indexed = {}", r.indexed));
                out.push(format!("min_f = {}", r.cfg.min_f));
                out.push(format!("default_f = {}", r.cfg.default_f));
                out.push(format!("max_f = {}", r.cfg.max_f));
                out.push(format!("min_selectivity = {}", r.cfg.min_selectivity));
                out.push(format!("min_table_rows = {}", r.cfg.min_table_rows));
                out.push(format!("zone_bounds = {}", r.zone_bounds));
                out.push(format!("bloom = {}", r.bloom));
                let cols = self.overridden_columns(t);
                if !cols.is_empty() {
                    out.push(format!("columns with overrides = {}", cols.join(", ")));
                }
            }
            (Some(t), Some(c)) => {
                let r = self.resolve(t, c);
                out.push(format!("column = {}.{}", t, c));
                out.push(format!("indexed = {}", r.indexed));
                out.push(format!("zone_bounds = {}", r.zone_bounds));
                out.push(format!("bloom = {}", r.bloom));
                out.push(format!("min_f = {}", r.cfg.min_f));
                out.push(format!("default_f = {}", r.cfg.default_f));
                out.push(format!("max_f = {}", r.cfg.max_f));
                out.push(format!("min_selectivity = {}", r.cfg.min_selectivity));
                out.push(format!("min_table_rows = {}", r.cfg.min_table_rows));
            }
        }
        out
    }
}
