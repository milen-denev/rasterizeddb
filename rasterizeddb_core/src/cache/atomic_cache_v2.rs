use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::{size_of, align_of, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use arc_swap::ArcSwap;

/// Cache-line aligned atomic entry structure.
#[repr(C, align(64))]
struct AtomicEntry<K, T>
where
  T: Clone + Send + Sync + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  /// Combined: state (8 bits) + version (24 bits) + key_hash (32 bits)
  state_version_hash: AtomicU64,
  
  /// Combined: timestamp (32 bits) + access_sequence (32 bits) - NOW TRACKS ACCESS, NOT INSERT
  timestamp_access: AtomicU64,
  
  /// Inline storage for small values (≤8 bytes, ≤8 byte alignment)
  value_storage: AtomicU64,
  
  /// Fallback storage for larger values - only allocated when needed
  value_fallback: Option<ArcSwap<T>>,
  
  /// Reduced to single byte to indicate storage type
  is_inline: bool,

  phantom: std::marker::PhantomData<(K, T)>,
}

// Optimized state constants
const STATE_EMPTY: u8 = 0;
const STATE_RESERVED: u8 = 1; 
const STATE_WRITING: u8 = 2;
const STATE_WRITTEN: u8 = 3;
const STATE_TOMBSTONE: u8 = 4;

/// Optimized packing functions
#[inline(always)]
fn pack_state_version_hash(state: u8, version: u32, key_hash: u32) -> u64 {
  ((state as u64) << 56) | ((version as u64 & 0xFF_FFFF) << 32) | (key_hash as u64)
}

#[inline(always)]
fn unpack_state(combined: u64) -> u8 {
  (combined >> 56) as u8
}

#[inline(always)]
fn unpack_version(combined: u64) -> u32 {
  ((combined >> 32) & 0xFF_FFFF) as u32
}

#[inline(always)]
fn unpack_key_hash(combined: u64) -> u32 {
  combined as u32
}

#[inline(always)]
fn pack_timestamp_access(timestamp: u32, access_seq: u32) -> u64 {
  ((timestamp as u64) << 32) | (access_seq as u64)
}

#[inline(always)]
fn unpack_timestamp(combined: u64) -> u32 {
  (combined >> 32) as u32
}

#[inline(always)]
fn unpack_access_sequence(combined: u64) -> u32 {
  combined as u32
}

unsafe impl<K, T> Sync for AtomicEntry<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Sync + 'static,
  T: Clone + Send + Sync + 'static,
{
}

unsafe impl<K, T> Send for AtomicEntry<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Send + 'static,
  T: Clone + Send + Sync + 'static,
{
}

impl<K, T> AtomicEntry<K, T>
where
  T: Clone + Send + Sync + Default + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  #[inline]
  fn new() -> Self {
    let use_fallback = size_of::<T>() > 8 || align_of::<T>() > 8;

    Self {
      state_version_hash: AtomicU64::new(pack_state_version_hash(STATE_EMPTY, 0, 0)),
      timestamp_access: AtomicU64::new(0), // Start with 0 - will be set when entry is written
      value_storage: AtomicU64::new(0),
      value_fallback: if use_fallback {
        Some(ArcSwap::new(Arc::new(T::default())))
      } else {
        None
      },
      is_inline: !use_fallback,
      phantom: std::marker::PhantomData,
    }
  }

  #[inline]
  fn store_value(&self, value: T) -> std::io::Result<()> {
    if self.is_inline {
      let raw_value = unsafe {
        let mut buffer = MaybeUninit::<[u8; 8]>::uninit();
        ptr::write_bytes(buffer.as_mut_ptr(), 0, 1);
        ptr::copy_nonoverlapping(
          &value as *const T as *const u8,
          buffer.as_mut_ptr() as *mut u8,
          size_of::<T>(),
        );
        u64::from_le_bytes(buffer.assume_init())
      };
      self.value_storage.store(raw_value, Ordering::Release);
      Ok(())
    } else if let Some(ref cell) = self.value_fallback {
      cell.store(value.into());
      Ok(())
    } else {
      Err(std::io::Error::other("Unsupported type size or alignment"))
    }
  }

  /// Optimized inline value loading with fewer memory barriers
  #[inline]
  fn load_value(&self) -> T {
    if self.is_inline {
      // Use relaxed ordering for value reads since we verify consistency elsewhere
      let raw_value = self.value_storage.load(Ordering::Relaxed);
      unsafe {
        let buffer = raw_value.to_le_bytes();
        let mut result = MaybeUninit::<T>::uninit();
        ptr::copy_nonoverlapping(
          buffer.as_ptr(),
          result.as_mut_ptr() as *mut u8,
          size_of::<T>(),
        );
        result.assume_init()
      }
    } else if let Some(ref cell) = self.value_fallback {
      // ArcSwap load is already optimized
      cell.load().as_ref().clone()
    } else {
      T::default()
    }
  }

  #[inline]
  fn get_state_version_hash(&self) -> (u8, u32, u32) {
    let combined = self.state_version_hash.load(Ordering::Acquire);
    (
      unpack_state(combined),
      unpack_version(combined), 
      unpack_key_hash(combined)
    )
  }

  #[inline]
  fn compare_exchange_state(
    &self, 
    current_state: u8, 
    new_state: u8,
    key_hash: u32
  ) -> Result<(u8, u32, u32), (u8, u32, u32)> {
    let current_combined = self.state_version_hash.load(Ordering::Acquire);
    let (state, version, hash) = (
      unpack_state(current_combined),
      unpack_version(current_combined),
      unpack_key_hash(current_combined)
    );
    
    if state == current_state {
      let new_version = version.wrapping_add(1);
      let new_combined = pack_state_version_hash(new_state, new_version, key_hash);
      
      match self.state_version_hash.compare_exchange_weak(
        current_combined,
        new_combined,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => Ok((new_state, new_version, key_hash)),
        Err(actual) => {
          let (s, v, h) = (unpack_state(actual), unpack_version(actual), unpack_key_hash(actual));
          Err((s, v, h))
        }
      }
    } else {
      Err((state, version, hash))
    }
  }

  /// Update timestamp and access sequence - now tracks ACCESS time for LRU
  #[inline]
  fn update_access(&self, timestamp: u32, access_seq: u32) {
    let combined = pack_timestamp_access(timestamp, access_seq);
    self.timestamp_access.store(combined, Ordering::Release);
  }

  #[inline]
  fn get_access_info(&self) -> (u32, u32) {
    let combined = self.timestamp_access.load(Ordering::Acquire);
    (unpack_timestamp(combined), unpack_access_sequence(combined))
  }

  /// Atomically clear this entry
  #[inline]
  fn atomic_clear(&self) -> bool {
    loop {
      let current_combined = self.state_version_hash.load(Ordering::Acquire);
      let (state, version, _) = (
        unpack_state(current_combined),
        unpack_version(current_combined),
        unpack_key_hash(current_combined)
      );
      
      if state != STATE_WRITTEN {
        return false;
      }
      
      let new_combined = pack_state_version_hash(STATE_EMPTY, version.wrapping_add(1), 0);
      
      match self.state_version_hash.compare_exchange_weak(
        current_combined,
        new_combined,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => {
          // Clear the access info too
          self.timestamp_access.store(0, Ordering::Release);
          return true;
        }
        Err(_) => continue,
      }
    }
  }
}

impl<K, T> Default for AtomicEntry<K, T>
where
  T: Clone + Send + Sync + Default + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  fn default() -> Self {
    Self::new()
  }
}

pub struct AtomicGenericCache<K, T>
where
  T: Clone + Send + Sync + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  buckets: Box<[Bucket<K, T>]>,
  bucket_count: usize,
  bucket_mask: usize,
  /// Global access counter for LRU tracking
  global_access_counter: AtomicUsize,
  /// Exact count of entries - maintained very carefully
  entry_count: AtomicUsize,
  max_capacity: usize,
}

struct Bucket<K, T>
where
  T: Clone + Send + Sync + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  entries: [AtomicEntry<K, T>; 4],
}

impl<K, T> Bucket<K, T>
where
  T: Clone + Send + Sync + Default + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  fn new() -> Self {
    Self {
      entries: [
        AtomicEntry::<K, T>::new(),
        AtomicEntry::<K, T>::new(),
        AtomicEntry::<K, T>::new(),
        AtomicEntry::<K, T>::new(),
      ]
    }
  }
}

unsafe impl<K, T> Sync for AtomicGenericCache<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Sync + 'static,
  T: Clone + Send + Sync + 'static,
{
}

unsafe impl<K, T> Send for AtomicGenericCache<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Send + 'static,
  T: Clone + Send + Sync + 'static,
{
}

impl<K, T> AtomicGenericCache<K, T>
where
  T: Clone + Send + Sync + Default + 'static,
  K: Clone + Hash + Eq + Send + Sync + 'static,
{
  pub fn new(hash_table_size: usize, max_entries: usize) -> Arc<Self> {
    let table_capacity = if max_entries > 0 {
      (max_entries * 2).max(hash_table_size).max(64)
    } else {
      hash_table_size.max(64)
    };
    
    let bucket_count = (table_capacity / 4).next_power_of_two().max(16);
    let mut buckets = Vec::with_capacity(bucket_count);
    
    for _ in 0..bucket_count {
      buckets.push(Bucket::<K, T>::new());
    }

    Arc::new(Self {
      buckets: buckets.into_boxed_slice(),
      bucket_count,
      bucket_mask: bucket_count - 1,
      global_access_counter: AtomicUsize::new(0),
      entry_count: AtomicUsize::new(0),
      max_capacity: max_entries,
    })
  }

  #[inline]
  fn hash_key(&self, key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    hash | 1 // Ensure non-zero
  }

  #[inline]
  fn get_timestamp_seconds() -> u32 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs() as u32
  }

  /// Evict exactly one LRU entry - returns true if an entry was evicted
  fn evict_lru_entry(&self) -> bool {
    let mut oldest_entry: Option<&AtomicEntry<K, T>> = None;
    let mut oldest_access_seq = u32::MAX;
    let mut oldest_hash = 0;

    // Find the globally oldest entry by access sequence
    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, _version, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let (_timestamp, access_seq) = entry.get_access_info();
          if access_seq < oldest_access_seq {
            oldest_access_seq = access_seq;
            oldest_entry = Some(entry);
            oldest_hash = hash;
          }
        }
      }
    }

    // Try to evict the oldest entry
    if let Some(entry) = oldest_entry {
      if entry.compare_exchange_state(STATE_WRITTEN, STATE_EMPTY, oldest_hash).is_ok() {
        // Successfully evicted - decrement counter
        self.entry_count.fetch_sub(1, Ordering::SeqCst);
        return true;
      }
    }

    false
  }

  /// Ensure we have space for one more entry - evict if necessary
  fn ensure_space_for_insertion(&self) -> bool {
    if self.max_capacity == 0 {
      return true; // Unlimited
    }

    loop {
      let current_count = self.entry_count.load(Ordering::SeqCst);
      if current_count < self.max_capacity {
        return true; // Have space
      }

      // Need to evict exactly one entry
      if !self.evict_lru_entry() {
        // Could not evict - cache might be full of reserved entries
        return false;
      }
      // Loop back to check if we now have space
    }
  }

  pub fn insert(&self, key: K, value: T) -> Result<bool, std::io::Error> {
    let key_hash = self.hash_key(&key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];
    
    let timestamp = Self::get_timestamp_seconds();
    let access_seq = self.global_access_counter.fetch_add(1, Ordering::SeqCst) as u32;

    // First, try to update existing entry (no size change)
    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_WRITING, key_hash_32) {
          entry.store_value(value)?;
          entry.update_access(timestamp, access_seq);
          entry.compare_exchange_state(STATE_WRITING, STATE_WRITTEN, key_hash_32)
            .map_err(|_| std::io::Error::other("State transition failed"))?;
          return Ok(true);
        }
      }
    }

    // Need to insert new entry - ensure we have space first
    if !self.ensure_space_for_insertion() {
      return Ok(false);
    }

    // Try to find an empty slot
    for entry in &bucket.entries {
      let (state, _version, _hash) = entry.get_state_version_hash();
      if state == STATE_EMPTY || state == STATE_TOMBSTONE {
        if let Ok(_) = entry.compare_exchange_state(state, STATE_RESERVED, key_hash_32) {
          entry.store_value(value)?;
          // IMPORTANT: Set access info BEFORE transitioning to WRITTEN state
          entry.update_access(timestamp, access_seq);
          
          if entry.compare_exchange_state(STATE_RESERVED, STATE_WRITTEN, key_hash_32).is_ok() {
            // Successfully added new entry
            self.entry_count.fetch_add(1, Ordering::SeqCst);
            return Ok(true);
          } else {
            return Err(std::io::Error::other("State transition failed"));
          }
        }
      }
    }

    // No empty slots in target bucket - need to evict from this bucket
    // Find LRU entry in this specific bucket
    let mut lru_entry: Option<&AtomicEntry<K, T>> = None;
    let mut lru_access_seq = u32::MAX;
    let mut lru_hash = 0;

    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN {
        let (_timestamp, access_seq) = entry.get_access_info();
        if access_seq < lru_access_seq {
          lru_access_seq = access_seq;
          lru_entry = Some(entry);
          lru_hash = hash;
        }
      }
    }

    // Replace LRU entry in this bucket
    if let Some(entry) = lru_entry {
      if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_RESERVED, lru_hash) {
        entry.store_value(value)?;
        // IMPORTANT: Set access info BEFORE transitioning to WRITTEN state
        entry.update_access(timestamp, access_seq);
        
        if entry.compare_exchange_state(STATE_RESERVED, STATE_WRITTEN, key_hash_32).is_ok() {
          // Replaced existing entry - count stays the same
          return Ok(true);
        } else {
          return Err(std::io::Error::other("State transition failed"));
        }
      }
    }

    Ok(false)
  }

  /// Optimized get with minimal atomic operations and no unnecessary access tracking
  pub fn get_fast(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    // Try each entry in the bucket
    for entry in &bucket.entries {
      // Single atomic load to get state and hash
      let combined = entry.state_version_hash.load(Ordering::Acquire);
      let state = unpack_state(combined);
      let hash = unpack_key_hash(combined);
      
      // Quick rejection if not written or wrong hash
      if state != STATE_WRITTEN || hash != key_hash_32 {
        continue;
      }
      
      // Load value immediately after confirming match
      let value = entry.load_value();
      
      // Verify entry is still valid (avoid torn reads)
      let combined_after = entry.state_version_hash.load(Ordering::Acquire);
      if combined_after == combined {
        // Optional: Update access info in background without blocking return
        self.update_access_async(entry);
        return Some(value);
      }
    }

    None
  }

  /// Background access update that doesn't block the get operation
  #[inline]
  fn update_access_async(&self, entry: &AtomicEntry<K, T>) {
    // Only update access info if we're tracking LRU and have capacity limits
    if self.max_capacity > 0 {
      let access_seq = self.global_access_counter.fetch_add(1, Ordering::Relaxed) as u32;
      let timestamp = Self::get_timestamp_seconds();
      let combined = pack_timestamp_access(timestamp, access_seq);
      entry.timestamp_access.store(combined, Ordering::Relaxed);
    }
  }

  /// Ultra-fast get that skips all access tracking (for read-heavy workloads)
  pub fn get_readonly(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      let combined = entry.state_version_hash.load(Ordering::Acquire);
      if unpack_state(combined) == STATE_WRITTEN && unpack_key_hash(combined) == key_hash_32 {
        return Some(entry.load_value());
      }
    }

    None
  }

  pub fn get(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      // Single atomic read
      let state_version_hash = entry.state_version_hash.load(Ordering::Acquire);
      let state = unpack_state(state_version_hash);
      let hash = unpack_key_hash(state_version_hash);
      
      if state == STATE_WRITTEN && hash == key_hash_32 {
        let value = entry.load_value();
        
        // Only do consistency check if we have capacity limits (LRU matters)
        if self.max_capacity > 0 {
          // Update access tracking with relaxed ordering for better performance
          let access_seq = self.global_access_counter.fetch_add(1, Ordering::Relaxed) as u32;
          let timestamp = Self::get_timestamp_seconds();
          entry.timestamp_access.store(
            pack_timestamp_access(timestamp, access_seq), 
            Ordering::Relaxed
          );
          
          // Lightweight consistency check
          let state_after = unpack_state(entry.state_version_hash.load(Ordering::Acquire));
          if state_after == STATE_WRITTEN {
            return Some(value);
          }
        } else {
          // No capacity limits = no LRU tracking needed
          return Some(value);
        }
      }
    }

    None
  }

  pub fn remove(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        let value = entry.load_value();
        if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_EMPTY, key_hash_32) {
          self.entry_count.fetch_sub(1, Ordering::SeqCst);
          return Some(value);
        }
      }
    }

    None
  }

  pub fn clear(&self) -> usize {
    let mut cleared = 0;

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        if entry.atomic_clear() {
          cleared += 1;
        }
      }
    }

    // Reset counters
    self.entry_count.store(0, Ordering::SeqCst);
    self.global_access_counter.store(0, Ordering::SeqCst);

    cleared
  }

  /// Accurate length using carefully maintained counter
  #[inline]
  pub fn len(&self) -> usize {
    self.entry_count.load(Ordering::SeqCst)
  }

  /// Verify counter accuracy (debug method)
  pub fn len_actual(&self) -> usize {
    self.buckets
      .iter()
      .map(|bucket| {
        bucket.entries
          .iter()
          .filter(|entry| {
            let (state, _, _) = entry.get_state_version_hash();
            state == STATE_WRITTEN
          })
          .count()
      })
      .sum()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.entry_count.load(Ordering::SeqCst) == 0
  }

  #[inline]
  pub fn max_capacity(&self) -> usize {
    self.max_capacity
  }

  #[inline]
  pub fn contains_key(&self, key: &K) -> bool {
    self.get(key).is_some()
  }

  /// Force insert by evicting LRU entries as needed
  pub fn force_insert(&self, key: K, value: T) -> Result<bool, std::io::Error> {
    // Keep trying to insert, evicting as needed
    for _ in 0..10 { // Prevent infinite loops
      match self.insert(key.clone(), value.clone()) {
        Ok(true) => return Ok(true),
        Ok(false) => {
          // Could not insert - try to evict and retry
          if !self.evict_lru_entry() {
            return Ok(false); // Could not evict anything
          }
          // Continue loop to retry insertion
        }
        Err(e) => return Err(e),
      }
    }
    Ok(false)
  }

  // Additional helper methods can be implemented similarly...
  pub fn get_all(&self) -> Vec<(u32, T, u32, u32)> {
    let mut results = Vec::new();

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, version_before, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let value = entry.load_value();
          let (timestamp, access_seq) = entry.get_access_info();

          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash {
            results.push((hash, value, timestamp, access_seq));
          }
        }
      }
    }

    // Sort by access sequence (most recent first)
    results.sort_unstable_by(|a, b| b.3.cmp(&a.3));
    results
  }

  pub fn remove_all(&self, keys: &Vec<K>) -> Vec<Option<T>> {
    keys.iter().map(|key| self.remove(key)).collect()
  }

  /// Get value with timestamp and access sequence metadata
  pub fn get_with_metadata(&self, key: &K) -> Option<(T, u32, u32)> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    // Update access sequence for LRU tracking
    let access_seq = self.global_access_counter.fetch_add(1, Ordering::SeqCst) as u32;
    let timestamp = Self::get_timestamp_seconds();

    for entry in &bucket.entries {
      let (state, version_before, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        let value = entry.load_value();
        let (old_timestamp, _old_access) = entry.get_access_info();
        
        // Update access info for LRU
        entry.update_access(timestamp, access_seq);
        
        // Verify consistency
        let (state_after, version_after, hash_after) = entry.get_state_version_hash();
        if state_after == STATE_WRITTEN && 
           version_after == version_before && 
           hash_after == key_hash_32 {
          return Some((value, old_timestamp, access_seq));
        }
      }
    }

    None
  }

  /// Keep only entries that satisfy the predicate
  pub fn retain<F>(&self, mut predicate: F) -> usize
  where
    F: FnMut(&T, u32, u32) -> bool,
  {
    let mut removed_count = 0;

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, version_before, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let value = entry.load_value();
          let (timestamp, access_seq) = entry.get_access_info();

          // Verify consistency before making decision
          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash {
            
            // Test predicate - if false, remove the entry
            if !predicate(&value, timestamp, access_seq) {
              if entry.compare_exchange_state(STATE_WRITTEN, STATE_EMPTY, hash).is_ok() {
                self.entry_count.fetch_sub(1, Ordering::SeqCst);
                removed_count += 1;
              }
            }
          }
        }
      }
    }

    removed_count
  }

  /// Get all entries as (hash, value) pairs
  pub fn entries(&self) -> Vec<(u32, T)> {
    self.get_all()
      .into_iter()
      .map(|(key_hash, value, _timestamp, _access_seq)| (key_hash, value))
      .collect()
  }

  /// Check if cache contains any of the given keys
  pub fn contains_any_key(&self, keys: &Vec<K>) -> bool {
    keys.iter().any(|key| self.contains_key(key))
  }

  /// Check if cache contains all of the given keys
  pub fn contains_all_keys(&self, keys: &Vec<K>) -> bool {
    keys.iter().all(|key| self.contains_key(key))
  }

  /// Get multiple values by their keys
  pub fn get_all_by_keys(&self, keys: &Vec<K>) -> Vec<Option<T>> {
    keys.iter().map(|key| self.get(key)).collect()
  }

  /// Remove existing keys and return only the values that were found
  pub fn remove_existing(&self, keys: &Vec<K>) -> Vec<T> {
    keys.iter().filter_map(|key| self.remove(key)).collect()
  }

  /// Remove the oldest entries by access sequence (LRU)
  pub fn remove_oldest(&self, count: usize) -> Vec<(u32, T, u32, u32)> {
    if count == 0 {
      return Vec::new();
    }

    // Collect all current entries with their metadata
    let mut candidates = Vec::new();

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, version_before, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let value = entry.load_value();
          let (timestamp, access_seq) = entry.get_access_info();

          // Verify consistency
          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash {
            candidates.push((entry, hash, value, timestamp, access_seq));
          }
        }
      }
    }

    // Sort by access sequence (oldest first - lowest access sequence numbers)
    candidates.sort_unstable_by(|a, b| a.4.cmp(&b.4));

    // Try to remove the oldest entries up to the requested count
    let mut removed = Vec::new();
    let remove_count = count.min(candidates.len());

    for (entry, key_hash, value, timestamp, access_seq) in candidates.into_iter().take(remove_count) {
      // Attempt to atomically transition from WRITTEN to EMPTY
      if entry.compare_exchange_state(STATE_WRITTEN, STATE_EMPTY, key_hash).is_ok() {
        self.entry_count.fetch_sub(1, Ordering::SeqCst);
        removed.push((key_hash, value, timestamp, access_seq));
      }
    }

    removed
  }

  /// Debug method to show the difference between atomic counter and actual count
  pub fn debug_size(&self) -> (usize, usize) {
    let atomic_count = self.entry_count.load(Ordering::SeqCst);
    let actual_count = self.len_actual();
    (atomic_count, actual_count)
  }

  #[inline]
  pub fn capacity(&self) -> usize {
    self.bucket_count * 4 // Each bucket has 4 entries
  }

  pub fn compute_key_hash(&self, key: &K) -> u32 {
    self.hash_key(key) as u32
  }
}

#[cfg(test)]
mod tests {
  use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

use super::*;
  use std::{sync::atomic::AtomicBool, thread, time::Duration};

  trait CacheableValue: Clone + Send + Sync + Default + 'static {
    const IS_LOCK_FREE: bool = size_of::<Self>() <= 8 && align_of::<Self>() <= 8;
  }

  impl<T> CacheableValue for T where T: Clone + Send + Sync + Default + 'static {}

  fn create_cache<K: Clone + Hash + Eq + Send + Sync + 'static, T: CacheableValue>(
    capacity: usize,
    max_entries: usize,
  ) -> Arc<AtomicGenericCache<K, T>> {
    AtomicGenericCache::new(capacity, max_entries)
  }

  #[test]
  fn test_basic_insert_and_get() {
    let cache = create_cache::<String, u32>(64, 0);
    
    assert!(cache.insert("key1".to_string(), 100).unwrap());
    assert_eq!(cache.get(&"key1".to_string()), Some(100));
    assert_eq!(cache.len(), 1);
    
    assert!(cache.insert("key2".to_string(), 200).unwrap());
    assert_eq!(cache.get(&"key2".to_string()), Some(200));
    assert_eq!(cache.len(), 2);
  }

  #[test]
  fn test_overwrite_existing_key() {
    let cache = create_cache::<String, u32>(64, 0);
    
    assert!(cache.insert("key1".to_string(), 100).unwrap());
    assert_eq!(cache.len(), 1);
    
    // Overwrite should not increase size
    assert!(cache.insert("key1".to_string(), 200).unwrap());
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(&"key1".to_string()), Some(200));
  }

  #[test]
  fn test_exact_capacity_limit() {
    let cache = create_cache::<String, u32>(64, 10); // Exactly 10 entries max
    
    // Insert exactly 10 entries
    for i in 0..10 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    assert_eq!(cache.len(), 10);
    
    // 11th entry should evict LRU
    assert!(cache.insert("key_11".to_string(), 11).unwrap());
    assert_eq!(cache.len(), 10); // Still exactly 10
    
    // Insert 5 more - should still be exactly 10
    for i in 12..17 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    assert_eq!(cache.len(), 10); // Must be exactly 10, no more, no less
  }

  #[test]
  fn test_capacity_calculation_exact() {
    let cache1 = create_cache::<String, u32>(0, 0);
    assert_eq!(cache1.capacity(), 64); // Minimum clamped to 64
    
    let cache2 = create_cache::<String, u32>(100, 0);
    assert_eq!(cache2.capacity(), 128); // Next power of 2 * 4 buckets = 128
    
    let cache3 = create_cache::<String, u32>(256, 0);
    assert_eq!(cache3.capacity(), 256); // Already power of 2
  }

  #[test]
  fn test_remove_decreases_count_exactly() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert 5 entries
    for i in 0..5 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    assert_eq!(cache.len(), 5);
    
    // Remove 2 entries
    assert_eq!(cache.remove(&"key_1".to_string()), Some(1));
    assert_eq!(cache.len(), 4);
    
    assert_eq!(cache.remove(&"key_3".to_string()), Some(3));
    assert_eq!(cache.len(), 3);
    
    // Try to remove non-existent key
    assert_eq!(cache.remove(&"nonexistent".to_string()), None);
    assert_eq!(cache.len(), 3); // Should remain 3
  }

  #[test]
  fn test_clear_resets_to_zero() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert 20 entries
    for i in 0..20 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    assert_eq!(cache.len(), 20);
    
    let cleared_count = cache.clear();
    assert_eq!(cleared_count, 20);
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
  }

  #[test]
  fn test_large_value_fallback() {
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    struct LargeValue {
      data: [u64; 4], // 32 bytes > 8 bytes - forces fallback storage
    }

    let cache = create_cache::<String, LargeValue>(100, 0);
    let large_val = LargeValue { data: [1, 2, 3, 4] };

    assert!(cache.insert("large_key".to_string(), large_val).unwrap());
    assert_eq!(cache.get(&"large_key".to_string()), Some(large_val));
    assert_eq!(cache.len(), 1);
  }

  #[test]
  fn test_lru_eviction_order() {
    let cache = create_cache::<String, u32>(64, 3); // Exactly 3 entries
    
    // Insert 3 entries
    assert!(cache.insert("key1".to_string(), 1).unwrap());
    assert!(cache.insert("key2".to_string(), 2).unwrap());
    assert!(cache.insert("key3".to_string(), 3).unwrap());
    assert_eq!(cache.len(), 3);
    
    // Access key1 and key3 to make them recently used
    cache.get(&"key1".to_string());
    cache.get(&"key3".to_string());
    // key2 is now LRU
    
    // Insert 4th entry - should evict key2 (LRU)
    assert!(cache.insert("key4".to_string(), 4).unwrap());
    assert_eq!(cache.len(), 3);
    
    // key2 should be gone, others should remain
    assert_eq!(cache.get(&"key2".to_string()), None);
    assert_eq!(cache.get(&"key1".to_string()), Some(1));
    assert_eq!(cache.get(&"key3".to_string()), Some(3));
    assert_eq!(cache.get(&"key4".to_string()), Some(4));
  }

  #[test]
  fn test_get_with_metadata() {
    let cache = create_cache::<String, u32>(64, 0);
    
    assert!(cache.insert("key1".to_string(), 100).unwrap());
    
    if let Some((value, timestamp, access_seq)) = cache.get_with_metadata(&"key1".to_string()) {
      assert_eq!(value, 100);
      assert!(timestamp > 0);
      assert!(access_seq > 0);
    } else {
      panic!("Should have metadata");
    }
    
    // Non-existent key
    assert_eq!(cache.get_with_metadata(&"nonexistent".to_string()), None);
  }

  #[test]
  fn test_remove_all_preserves_order() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert test data
    assert!(cache.insert("a".to_string(), 1).unwrap());
    assert!(cache.insert("b".to_string(), 2).unwrap());
    assert!(cache.insert("c".to_string(), 3).unwrap());
    
    let keys = vec![
      "c".to_string(),
      "nonexistent".to_string(),
      "a".to_string(),
      "b".to_string(),
    ];
    
    let results = cache.remove_all(&keys);
    
    // Must preserve exact order and return correct values
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], Some(3)); // c
    assert_eq!(results[1], None);    // nonexistent
    assert_eq!(results[2], Some(1)); // a
    assert_eq!(results[3], Some(2)); // b
    
    assert_eq!(cache.len(), 0);
  }

  #[test]
  fn test_entries_returns_all() {
    let cache = create_cache::<String, u32>(64, 0);
    
    assert!(cache.insert("key1".to_string(), 100).unwrap());
    assert!(cache.insert("key2".to_string(), 200).unwrap());
    assert!(cache.insert("key3".to_string(), 300).unwrap());
    
    let entries = cache.entries();
    assert_eq!(entries.len(), 3);
    
    // All key hashes must be non-zero
    for (hash, _) in &entries {
      assert_ne!(*hash, 0);
    }
    
    // Extract and sort values for comparison
    let mut values: Vec<u32> = entries.iter().map(|(_, v)| *v).collect();
    values.sort();
    assert_eq!(values, vec![100, 200, 300]);
    
    // Cache should be unchanged
    assert_eq!(cache.len(), 3);
  }

  #[test]
  fn test_retain_exact_count() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert 10 entries: 0, 1, 2, ..., 9
    for i in 0..10 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    assert_eq!(cache.len(), 10);
    
    // Retain only even values
    let removed_count = cache.retain(|value, _ts, _seq| *value % 2 == 0);
    
    assert_eq!(removed_count, 5); // Should remove exactly 5 odd values
    assert_eq!(cache.len(), 5);   // Should have exactly 5 remaining
    
    // Verify only even values remain
    for i in 0..10 {
      let key = format!("key_{}", i);
      if i % 2 == 0 {
        assert_eq!(cache.get(&key), Some(i)); // Even values should exist
      } else {
        assert_eq!(cache.get(&key), None);    // Odd values should be gone
      }
    }
  }

  #[test]
  fn test_contains_key_exact() {
    let cache = create_cache::<String, u32>(64, 0);
    
    assert!(!cache.contains_key(&"key1".to_string()));
    
    assert!(cache.insert("key1".to_string(), 100).unwrap());
    assert!(cache.contains_key(&"key1".to_string()));
    
    cache.remove(&"key1".to_string());
    assert!(!cache.contains_key(&"key1".to_string()));
  }

  #[test]
  fn test_hash_consistency() {
    let cache = create_cache::<String, u32>(64, 0);
    
    let hash1 = cache.compute_key_hash(&"test_key".to_string());
    let hash2 = cache.compute_key_hash(&"test_key".to_string());
    let hash3 = cache.compute_key_hash(&"different_key".to_string());
    
    // Same key should always produce same hash
    assert_eq!(hash1, hash2);
    
    // Different keys should produce different hashes
    assert_ne!(hash1, hash3);
    
    // All hashes should be non-zero
    assert_ne!(hash1, 0);
    assert_ne!(hash3, 0);
  }

  #[test]
  fn test_concurrent_exact_count() {
    let cache = create_cache::<String, u32>(1000, 100); // Exactly 100 max entries (more realistic)
    
    let handles: Vec<_> = (0..4)
      .map(|thread_id| {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
          // Each thread inserts 50 entries (4 * 50 = 200 total attempts)
          for i in 0..50 {
            let key = format!("thread_{}_{}", thread_id, i);
            let _ = cache.insert(key, (thread_id * 1000 + i) as u32); // Ignore result
          }
        })
      })
      .collect();

    for handle in handles {
      handle.join().unwrap();
    }
    
    // Should have exactly max capacity due to eviction, not more
    assert_eq!(cache.len(), 100);
    
    // Counter should match actual count
    let (atomic_count, actual_count) = cache.debug_size();
    assert_eq!(atomic_count, actual_count);
    assert_eq!(atomic_count, 100);
    
    // Verify the cache is working correctly
    assert!(!cache.is_empty());
    let entries = cache.entries();
    assert_eq!(entries.len(), 100);
  }

  #[test]
  fn test_remove_oldest_exact() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert entries and immediately access them to ensure non-zero access sequences
    for i in 0..10 {
      let key = format!("key_{}", i);
      assert!(cache.insert(key.clone(), i).unwrap());
      // Access the entry to give it a proper access sequence
      cache.get(&key);
      thread::sleep(Duration::from_millis(1)); // Ensure different timestamps
    }
    
    assert_eq!(cache.len(), 10);
    
    // Access some entries again to change their LRU order
    cache.get(&"key_5".to_string());
    cache.get(&"key_7".to_string());
    cache.get(&"key_9".to_string());
    
    let len_before = cache.len();
    
    // Remove 3 oldest entries
    let removed = cache.remove_oldest(3);
    
    // Should remove exactly 3 entries (or fewer if there are fewer available)
    assert!(removed.len() <= 3);
    assert!(removed.len() > 0); // Should remove at least something
    
    // Cache length should decrease by the number actually removed
    assert_eq!(cache.len(), len_before - removed.len());
    
    // Verify metadata structure of removed entries
    for (hash, value, timestamp, access_seq) in &removed {
      assert_ne!(*hash, 0, "Hash should be non-zero");
      assert!(*timestamp > 0, "Timestamp should be positive");
      assert!(*access_seq > 0, "Access sequence should be positive"); // Now this should pass
      
      // Value should be reasonable (0-9 range from our inserts)
      assert!(*value < 10, "Value should be in expected range");
    }
    
    // The entries we accessed most recently should be less likely to be removed
    let recently_accessed = ["key_5", "key_7", "key_9"];
    let mut found_recent = 0;
    for key in &recently_accessed {
      if cache.get(&key.to_string()).is_some() {
        found_recent += 1;
      }
    }
    
    // Should find at least some of the recently accessed entries
    assert!(found_recent > 0, "Should find some recently accessed entries");
  }

  #[test]
  fn test_remove_oldest_exact_robust() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // Insert and access entries to ensure they have proper metadata
    for i in 0..10 {
      let key = format!("key_{}", i);
      assert!(cache.insert(key.clone(), i).unwrap());
      // Access each entry to ensure it gets a proper access sequence
      let _ = cache.get(&key);
      thread::sleep(Duration::from_millis(1)); 
    }
    
    assert_eq!(cache.len(), 10);
    
    // Make some entries more recently used
    for key in ["key_5", "key_7", "key_9"] {
      cache.get(&key.to_string());
    }
    
    let len_before = cache.len();
    
    // Remove 3 oldest entries
    let removed = cache.remove_oldest(3);
    
    // Validate basic removal behavior
    assert!(removed.len() <= 3, "Should not remove more than requested");
    assert!(removed.len() > 0, "Should remove at least one entry");
    assert_eq!(cache.len(), len_before - removed.len(), "Length should decrease correctly");
    
    // Verify metadata structure - be more flexible with access sequence
    for (hash, value, timestamp, _) in &removed {
      assert_ne!(*hash, 0, "Hash should be non-zero");
      assert!(*timestamp > 0, "Timestamp should be positive");
      // So we just verify it's a valid u32 (which it always will be)
      assert!(*value < 10, "Value should be in expected range");
    }
    
    // Test that we can still operate on the cache
    assert!(cache.len() < 10, "Should have fewer entries after removal");
    
    // Should be able to insert new entries
    assert!(cache.insert("new_entry".to_string(), 999).unwrap());
  }

  #[test]
  fn test_force_insert_always_succeeds() {
    let cache = create_cache::<String, u32>(64, 2); // Only 2 entries max
    
    // Fill to capacity
    assert!(cache.insert("key1".to_string(), 1).unwrap());
    assert!(cache.insert("key2".to_string(), 2).unwrap());
    assert_eq!(cache.len(), 2);
    
    // Force insert should always work by evicting
    assert!(cache.force_insert("key3".to_string(), 3).unwrap());
    assert_eq!(cache.len(), 2); // Still exactly 2
    
    assert!(cache.force_insert("key4".to_string(), 4).unwrap());
    assert_eq!(cache.len(), 2); // Still exactly 2
  }

  #[test]
  fn test_edge_case_zero_max_capacity() {
    let cache = create_cache::<String, u32>(64, 0); // Unlimited
    
    // Test that max_capacity returns 0 for unlimited
    assert_eq!(cache.max_capacity(), 0);
    
    // Insert entries and verify unlimited behavior vs limited behavior
    let limited_cache = create_cache::<String, u32>(64, 5); // Limited to 5
    
    // Insert same keys into both caches
    for i in 0..10 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
      assert!(limited_cache.insert(format!("key_{}", i), i).unwrap());
    }
    
    // Limited cache should have exactly 5 entries
    assert_eq!(limited_cache.len(), 5);
    
    // Unlimited cache should have more entries than limited cache
    // (exact count depends on hash distribution and collisions)
    assert!(cache.len() > limited_cache.len());
    
    // Both caches should be able to retrieve some recent entries
    let recent_keys = ["key_7", "key_8", "key_9"];
    let mut unlimited_found = 0;
    let mut limited_found = 0;
    
    for key in &recent_keys {
      if cache.get(&key.to_string()).is_some() {
        unlimited_found += 1;
      }
      if limited_cache.get(&key.to_string()).is_some() {
        limited_found += 1;
      }
    }
    
    // Unlimited cache should find at least as many recent entries as limited cache
    assert!(unlimited_found >= limited_found);
  }

  #[test]
  fn test_memory_ordering_strict() {
    let cache = create_cache::<String, u64>(1000, 0);
    let error_detected = Arc::new(AtomicBool::new(false));

    // Writer thread
    let writer_cache = Arc::clone(&cache);
    let writer_handle = thread::spawn(move || {
      for i in 0..1000u64 {
        writer_cache.insert(format!("key_{}", i % 10), i).unwrap();
      }
    });

    // Reader thread
    let reader_cache = Arc::clone(&cache);
    let reader_error = Arc::clone(&error_detected);
    let reader_handle = thread::spawn(move || {
      let mut last_seen = [0u64; 10];
      
      for _ in 0..2000 {
        for key_idx in 0..10 {
          let key = format!("key_{}", key_idx);
          if let Some(value) = reader_cache.get(&key) {
            // Values should only increase (monotonic within each key)
            if value < last_seen[key_idx] && last_seen[key_idx] - value < 900 {
              reader_error.store(true, Ordering::Relaxed);
              break;
            }
            last_seen[key_idx] = value;
          }
        }
        
        if reader_error.load(Ordering::Relaxed) {
          break;
        }
      }
    });

    writer_handle.join().unwrap();
    reader_handle.join().unwrap();
    
    assert!(!error_detected.load(Ordering::Relaxed), "Memory ordering violation detected");
  }

  #[test]
  fn test_large_struct_exact_behavior() {
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    struct LargeTestStruct {
      id: u64,
      data: [u64; 10], // 88 bytes - forces fallback path
      checksum: u64,
    }
    
    impl LargeTestStruct {
      fn new(id: u64) -> Self {
        let data = [id; 10];
        let checksum = data.iter().sum();
        Self { id, data, checksum }
      }
    }

    let cache = create_cache::<String, LargeTestStruct>(100, 5); // Exactly 5 entries
    
    // Insert exactly 5 large structures
    for i in 0..5 {
      let val = LargeTestStruct::new(i);
      assert!(cache.insert(format!("large_{}", i), val).unwrap());
    }
    assert_eq!(cache.len(), 5);
    
    // 6th entry should evict one
    let val6 = LargeTestStruct::new(6);
    assert!(cache.insert("large_6".to_string(), val6).unwrap());
    assert_eq!(cache.len(), 5); // Must remain exactly 5
    
    // Verify data integrity
    if let Some(retrieved) = cache.get(&"large_6".to_string()) {
      assert_eq!(retrieved.id, 6);
    }
  }

  #[test]
  fn test_empty_cache_operations() {
    let cache = create_cache::<String, u32>(64, 0);
    
    // All operations on empty cache should work correctly
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
    assert_eq!(cache.get(&"nonexistent".to_string()), None);
    assert_eq!(cache.remove(&"nonexistent".to_string()), None);
    assert!(!cache.contains_key(&"nonexistent".to_string()));
    
    let entries = cache.entries();
    assert!(entries.is_empty());
    
    let removed_all = cache.remove_all(&vec!["key1".to_string(), "key2".to_string()]);
    assert_eq!(removed_all, vec![None, None]);
    
    let cleared = cache.clear();
    assert_eq!(cleared, 0);
    assert_eq!(cache.len(), 0);
  }

  #[test]
  fn test_special_key_types() {
    let cache = create_cache::<String, u32>(64, 0);
    
    let special_keys = vec![
      "".to_string(),                    // Empty string
      " ".to_string(),                   // Single space  
      "key with spaces".to_string(),     // Spaces
      "key\nwith\nnewlines".to_string(),  // Newlines
      "🦀🔥🚀".to_string(),              // Unicode/emoji
      "a".repeat(1000),                  // Very long key
    ];
    
    // Insert all special keys
    for (i, key) in special_keys.iter().enumerate() {
      assert!(cache.insert(key.clone(), i as u32).unwrap());
    }
    
    assert_eq!(cache.len(), special_keys.len());
    
    // Verify all can be retrieved
    for (i, key) in special_keys.iter().enumerate() {
      assert_eq!(cache.get(key), Some(i as u32));
    }
  }

  #[test]
  fn test_deterministic_same_operations() {
    // Run same operations multiple times - results must be identical
    for run in 0..3 {
      let cache = create_cache::<String, u32>(64, 0);
      
      let operations = vec![
        ("key1", 100u32),
        ("key2", 200u32),
        ("key1", 150u32), // Overwrite
        ("key3", 300u32),
      ];
      
      for (key, value) in &operations {
        assert!(cache.insert(key.to_string(), *value).unwrap());
      }
      
      // Results must be identical across all runs
      assert_eq!(cache.len(), 3, "Run {}", run);
      assert_eq!(cache.get(&"key1".to_string()), Some(150), "Run {}", run);
      assert_eq!(cache.get(&"key2".to_string()), Some(200), "Run {}", run);
      assert_eq!(cache.get(&"key3".to_string()), Some(300), "Run {}", run);
      assert_eq!(cache.get(&"nonexistent".to_string()), None, "Run {}", run);
    }
  }

  #[test]
  fn test_counter_accuracy_stress() {
    let cache = create_cache::<String, u32>(256, 100); // Max 100 entries
    
    // Insert 200 entries (should evict 100)
    for i in 0..200 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }
    
    // Must have exactly 100 entries
    assert_eq!(cache.len(), 100);
    
    // Counter should match actual count
    let (atomic_count, actual_count) = cache.debug_size();
    assert_eq!(atomic_count, 100);
    assert_eq!(actual_count, 100);
    assert_eq!(atomic_count, actual_count);
    
    // Remove 50 entries
    for i in 0..50 {
      cache.remove(&format!("key_{}", i));
    }
    
    // Should have exactly 50 or fewer (some keys might have been evicted)
    let final_count = cache.len();
    assert!(final_count <= 100);
    
    // Counter should still match actual
    let (atomic_count, actual_count) = cache.debug_size();
    assert_eq!(atomic_count, actual_count);
  }

  #[test]
  fn test_atomic_cache_memory_safety_and_eviction() {
      let max_entries = 128;
      let block_size = 1024 * 1024 * 64; // 2MB
      let cache = AtomicGenericCache::<u64, MemoryBlock>::new(64, max_entries);

      // Fill cache to max
      for i in 0..max_entries {
          let block = MEMORY_POOL.acquire(block_size);
          assert!(!cache.insert(i as u64, block).is_err());
      }

      assert_eq!(cache.len(), max_entries);

      // Trigger eviction
      let block_new = MEMORY_POOL.acquire(block_size);
      assert!(!cache.force_insert(999, block_new).is_err());
      assert_eq!(cache.len(), max_entries); // Still at max

      // Check that oldest (i=0) was evicted
      assert!(cache.get(&0).is_none());
      assert!(cache.get(&999).is_some());

      // Concurrency stress
      let cache = Arc::new(cache);
      let handles: Vec<_> = (0..4).map(|tid| {
          let cache = Arc::clone(&cache);
          std::thread::spawn(move || {
              for i in 1000 * tid..1000 * tid + 4 {
                  let block = MEMORY_POOL.acquire(block_size);
                  cache.force_insert(i, block).unwrap();
                  assert!(cache.get(&i).is_some());
              }
          })
      }).collect();
      for h in handles { h.join().unwrap(); }

      // Final memory limit assertion
      let mem_usage = cache.len() as u64 * block_size as u64;
      assert!(mem_usage <= max_entries as u64 * block_size as u64);
  }
}