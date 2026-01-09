use std::sync::OnceLock;
use std::sync::Arc;

use dashmap::DashMap;
use smallvec::SmallVec;

use crate::cache::atomic_cache::AtomicGenericCache;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;

use super::rules::RowRange;

const DEFAULT_DIRTY_CACHE_CAPACITY: usize = 65_536;

/// Tracks row-pointer-record offsets affected by INSERT/UPDATE/DELETE.
///
/// The stored identity is the pointer-record byte offset (`pointer_record_pos`) in the
/// pointers file. Since pointer records are fixed-size, a virtual row-id can be derived as:
/// `row_id = pointer_record_pos / ROW_POINTER_RECORD_LEN`.
///
/// This tracker is intentionally conservative: it records *pointers*, not whether a row
/// ultimately matches a query. The query executor still applies the full predicate.
pub struct DirtyRowTracker {
    per_table: DashMap<String, Arc<AtomicGenericCache<u64, u64>>, ahash::RandomState>,
    capacity: usize,
}

impl DirtyRowTracker {
    pub fn new(capacity: usize) -> Self {
        Self {
            per_table: DashMap::with_hasher(ahash::RandomState::new()),
            capacity: capacity.max(64),
        }
    }

    #[inline]
    pub fn row_id_from_pointer_record_pos(pointer_record_pos: u64) -> u64 {
        pointer_record_pos / (ROW_POINTER_RECORD_LEN as u64)
    }

    fn cache_for_table(&self, table_name: &str) -> Arc<AtomicGenericCache<u64, u64>> {
        if let Some(v) = self.per_table.get(table_name) {
            return Arc::clone(v.value());
        }

        let cache = AtomicGenericCache::new(self.capacity, self.capacity);
        self.per_table
            .insert(table_name.to_string(), Arc::clone(&cache));
        cache
    }

    /// Marks the pointer record position as dirty for the given table.
    pub fn mark_pointer_record_pos(&self, table_name: &str, pointer_record_pos: u64) {
        let rec_len = ROW_POINTER_RECORD_LEN as u64;
        if rec_len == 0 || (pointer_record_pos % rec_len) != 0 {
            return;
        }

        let cache = self.cache_for_table(table_name);
        // Store the pointer position as both key and value; we only need to enumerate values.
        let _ = cache.force_insert(pointer_record_pos, pointer_record_pos);
    }

    /// Returns merged row ranges that cover all dirty pointer-record positions.
    pub fn dirty_ranges(&self, table_name: &str) -> SmallVec<[RowRange; 64]> {
        let Some(cache) = self.per_table.get(table_name) else {
            return SmallVec::new();
        };

        let mut positions: Vec<u64> = cache
            .value()
            .entries()
            .into_iter()
            .map(|(_khash, pos)| pos)
            .collect();

        if positions.is_empty() {
            return SmallVec::new();
        }

        let rec_len = ROW_POINTER_RECORD_LEN as u64;
        positions.sort_unstable();
        positions.dedup();

        let mut out: SmallVec<[RowRange; 64]> = SmallVec::new();

        let mut start = positions[0];
        let mut prev = positions[0];
        let mut count: u64 = 1;

        for &pos in positions.iter().skip(1) {
            if pos == prev.saturating_add(rec_len) {
                count = count.saturating_add(1);
                prev = pos;
                continue;
            }

            out.push(RowRange {
                start_pointer_pos: start,
                row_count: count,
            });
            start = pos;
            prev = pos;
            count = 1;
        }

        out.push(RowRange {
            start_pointer_pos: start,
            row_count: count,
        });

        out
    }

    pub fn clear_table(&self, table_name: &str) {
        if let Some(cache) = self.per_table.get(table_name) {
            _ = cache.value().clear();
        }
    }
}

static DIRTY_ROW_TRACKER: OnceLock<DirtyRowTracker> = OnceLock::new();

#[inline]
pub fn dirty_row_tracker() -> &'static DirtyRowTracker {
    DIRTY_ROW_TRACKER.get_or_init(|| DirtyRowTracker::new(DEFAULT_DIRTY_CACHE_CAPACITY))
}
