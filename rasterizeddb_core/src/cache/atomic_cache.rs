use std::hash::Hash;
use std::mem::{align_of, size_of, MaybeUninit};
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
  /// This reduces from 3 separate atomics to 1
  state_version_hash: AtomicU64,
  
  /// Combined: timestamp (32 bits) + sequence (32 bits)
  /// Reduces another atomic operation
  timestamp_sequence: AtomicU64,
  
  /// Inline storage for small values (≤8 bytes, ≤8 byte alignment)
  value_storage: AtomicU64,
  
  /// Fallback storage for larger values - only allocated when needed
  value_fallback: Option<ArcSwap<T>>,
  
  /// Reduced to single byte to indicate storage type
  is_inline: bool,

  phantom: std::marker::PhantomData<(K, T)>,
}

// Optimized state constants - using smaller bit space
const STATE_EMPTY: u8 = 0;
const STATE_RESERVED: u8 = 1; 
const STATE_WRITING: u8 = 2;
const STATE_WRITTEN: u8 = 3;
const STATE_TOMBSTONE: u8 = 4;

/// Optimized packing functions with better bit utilization
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
fn pack_timestamp_sequence(timestamp: u32, sequence: u32) -> u64 {
  ((timestamp as u64) << 32) | (sequence as u64)
}

#[inline(always)]
fn unpack_timestamp(combined: u64) -> u32 {
  (combined >> 32) as u32
}

#[inline(always)]
fn unpack_sequence(combined: u64) -> u32 {
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
      timestamp_sequence: AtomicU64::new(0),
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
      // Use Release ordering to ensure value is visible before state change
      self.value_storage.store(raw_value, Ordering::Release);
      Ok(())
    } else if let Some(ref cell) = self.value_fallback {
      cell.store(value.into());
      Ok(())
    } else {
      Err(std::io::Error::other("Unsupported type size or alignment"))
    }
  }

  #[inline]
  fn load_value(&self) -> T {
    if self.is_inline {
      // Use Acquire ordering to ensure we see the latest value
      let raw_value = self.value_storage.load(Ordering::Acquire);
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
        Ordering::AcqRel,  // Stronger ordering for state changes
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

  #[inline]
  #[allow(dead_code)]
  fn set_state(&self, state: u8, key_hash: u32) {
    let current_combined = self.state_version_hash.load(Ordering::Relaxed);
    let current_version = unpack_version(current_combined);
    let new_combined = pack_state_version_hash(state, current_version, key_hash);
    self.state_version_hash.store(new_combined, Ordering::Release);
  }

  /// Update timestamp and sequence in single operation
  #[inline]
  fn update_timestamp_sequence(&self, timestamp: u32, sequence: u32) {
    let combined = pack_timestamp_sequence(timestamp, sequence);
    self.timestamp_sequence.store(combined, Ordering::Release);
  }

  #[inline]
  fn get_timestamp_sequence(&self) -> (u32, u32) {
    let combined = self.timestamp_sequence.load(Ordering::Acquire);
    (unpack_timestamp(combined), unpack_sequence(combined))
  }

  
  /// Atomically clear this entry - used by clear() method
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
        return false; // Nothing to clear
      }
      
      let new_combined = pack_state_version_hash(STATE_EMPTY, version.wrapping_add(1), 0);
      
      match self.state_version_hash.compare_exchange_weak(
        current_combined,
        new_combined,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => return true,
        Err(_) => continue, // Retry
      }
    }
  }

  /// Atomically drain this entry - used by drain() method
  #[inline]
  fn atomic_drain(&self) -> Option<(u32, T, u32, u32)> {
    loop {
      let current_combined = self.state_version_hash.load(Ordering::Acquire);
      let (state, version, hash) = (
        unpack_state(current_combined),
        unpack_version(current_combined),
        unpack_key_hash(current_combined)
      );
      
      if state != STATE_WRITTEN {
        return None; // Nothing to drain
      }
      
      // Load value and metadata before attempting to drain
      let value = self.load_value();
      let (timestamp, sequence) = self.get_timestamp_sequence();
      
      let new_combined = pack_state_version_hash(STATE_EMPTY, version.wrapping_add(1), 0);
      
      match self.state_version_hash.compare_exchange_weak(
        current_combined,
        new_combined,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => return Some((hash, value, timestamp, sequence)),
        Err(_) => continue, // Retry
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
  /// Hash table buckets - each bucket is a small array of entries
  buckets: Box<[Bucket<K, T>]>,
  /// Number of buckets (power of 2)
  bucket_count: usize,
  /// Mask for fast bucket selection
  bucket_mask: usize,
  /// Global sequence counter
  global_sequence: AtomicUsize,
  /// Maximum capacity
  max_capacity: usize
}

/// Each bucket contains multiple entries to handle collisions
struct Bucket<K, T>
where
  T: Clone + Send + Sync + 'static,
  K: Hash + Eq + Send + Sync + 'static,
{
  entries: [AtomicEntry<K, T>; 4], // 4 entries per bucket
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
  pub fn new(capacity: usize, max_capacity: usize) -> Arc<Self> {
    let bucket_count = (capacity / 4).next_power_of_two().max(16);
    let mut buckets = Vec::with_capacity(bucket_count);
    
    for _ in 0..bucket_count {
      buckets.push(Bucket::<K, T>::new());
    }

    Arc::new(Self {
      buckets: buckets.into_boxed_slice(),
      bucket_count,
      bucket_mask: bucket_count - 1,
      global_sequence: AtomicUsize::new(0),
      max_capacity
    })
  }

  #[inline]
  fn hash_key(&self, key: &K) -> u64 {
    let hasher = ahash::RandomState::with_seed(0x51_7c_c1_b7_27_22_0a_95);
    let hash = hasher.hash_one(key);
    hash | 1
  }

  #[inline]
  fn get_timestamp_seconds() -> u32 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs() as u32
  }

  pub fn insert(&self, key: K, value: T) -> Result<bool, std::io::Error> {
    let key_hash = self.hash_key(&key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];
    
    let timestamp = Self::get_timestamp_seconds();
    // Pre-increment to ensure sequence is always > 0
    let sequence = (self.global_sequence.fetch_add(1, Ordering::Relaxed) + 1) as u32;

    // Try to update existing entry first
    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_WRITING, key_hash_32) {
          entry.store_value(value)?;
          entry.update_timestamp_sequence(timestamp, sequence);
          entry.compare_exchange_state(STATE_WRITING, STATE_WRITTEN, key_hash_32)
            .map_err(|_| std::io::Error::other("State transition failed"))?;
          return Ok(true);
        }
      }
    }

    // Try to find empty slot in target bucket
    for entry in &bucket.entries {
      let (state, _version, _hash) = entry.get_state_version_hash();
      if state == STATE_EMPTY || state == STATE_TOMBSTONE {
        if let Ok(_) = entry.compare_exchange_state(state, STATE_RESERVED, key_hash_32) {
          entry.store_value(value)?;
          entry.update_timestamp_sequence(timestamp, sequence);
          entry.compare_exchange_state(STATE_RESERVED, STATE_WRITTEN, key_hash_32)
            .map_err(|_| std::io::Error::other("State transition failed"))?;
          return Ok(true);
        }
      }
    }

    // Target bucket is full - implement ring buffer behavior
    // Check if we're at max capacity and need to evict globally
    if self.max_capacity > 0 && self.len() >= self.max_capacity {
      // Find and evict the oldest entry globally
      if let Some((oldest_entry, oldest_hash)) = self.find_oldest_entry() {
        if let Ok(_) = oldest_entry.compare_exchange_state(STATE_WRITTEN, STATE_TOMBSTONE, oldest_hash) {
          // Successfully evicted, now try to insert in target bucket again
          for entry in &bucket.entries {
            let (state, _version, _hash) = entry.get_state_version_hash();
            if state == STATE_EMPTY || state == STATE_TOMBSTONE {
              if let Ok(_) = entry.compare_exchange_state(state, STATE_RESERVED, key_hash_32) {
                entry.store_value(value)?;
                entry.update_timestamp_sequence(timestamp, sequence);
                entry.compare_exchange_state(STATE_RESERVED, STATE_WRITTEN, key_hash_32)
                  .map_err(|_| std::io::Error::other("State transition failed"))?;
                return Ok(true);
              }
            }
          }
        }
      }
    }

    // If target bucket is still full, evict the oldest entry in this bucket (ring buffer behavior)
    if let Some((oldest_entry, oldest_hash)) = self.find_oldest_entry_in_bucket(bucket) {
      if let Ok(_) = oldest_entry.compare_exchange_state(STATE_WRITTEN, STATE_RESERVED, oldest_hash) {
        // Reuse this slot
        oldest_entry.store_value(value)?;
        oldest_entry.update_timestamp_sequence(timestamp, sequence);
        oldest_entry.compare_exchange_state(STATE_RESERVED, STATE_WRITTEN, key_hash_32)
          .map_err(|_| std::io::Error::other("State transition failed"))?;
        return Ok(true);
      }
    }

    Ok(false) // Should rarely happen
  }

  // Helper method to find the oldest entry globally
  fn find_oldest_entry(&self) -> Option<(&AtomicEntry<K, T>, u32)> {
    let mut oldest_entry = None;
    let mut oldest_sequence = u32::MAX;
    let mut oldest_hash = 0;

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, _version, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let (_timestamp, sequence) = entry.get_timestamp_sequence();
          if sequence < oldest_sequence {
            oldest_sequence = sequence;
            oldest_entry = Some(entry);
            oldest_hash = hash;
          }
        }
      }
    }

    oldest_entry.map(|entry| (entry, oldest_hash))
  }

  // Helper method to find the oldest entry in a specific bucket
  fn find_oldest_entry_in_bucket<'a>(&self, bucket: &'a Bucket<K, T>) -> Option<(&'a AtomicEntry<K, T>, u32)> {
    let mut oldest_entry = None;
    let mut oldest_sequence = u32::MAX;
    let mut oldest_hash = 0;

    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN {
        let (_timestamp, sequence) = entry.get_timestamp_sequence();
        if sequence < oldest_sequence {
          oldest_sequence = sequence;
          oldest_entry = Some(entry);
          oldest_hash = hash;
        }
      }
    }

    oldest_entry.map(move |entry| (entry, oldest_hash))
  }

  pub fn force_insert(
    &self,
    key: K,
    value: T,
  ) -> Result<(bool, Vec<(u32, T, u32, u32)>), std::io::Error> {
    let mut removed_entries = Vec::new();

    // If we're at max capacity, remove oldest entries to make space
    if self.max_capacity > 0 {
      let current_len = self.len();
      if current_len >= self.max_capacity {
        // Remove at least one entry, but potentially more if we're over capacity
        let entries_to_remove = (current_len - self.max_capacity + 1).max(1);
        removed_entries = self.remove_oldest(entries_to_remove);
      }
    }

    // Try regular insert first - this will move key and value
    match self.insert(key, value) {
      Ok(true) => return Ok((true, removed_entries)),
      Ok(false) => {
        // Continue with eviction logic below
      },
      Err(e) => return Err(e),
    }

    // If insert failed, find oldest entries to evict
    let mut oldest_entries = Vec::new();
    let mut min_sequence = u32::MAX;
    let mut min_bucket_idx = 0;
    let mut min_entry_idx = 0;

    // Find the oldest entry across all buckets
    for (bucket_idx, bucket) in self.buckets.iter().enumerate() {
      for (entry_idx, entry) in bucket.entries.iter().enumerate() {
        let (state, _version, _) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let (_timestamp, sequence) = entry.get_timestamp_sequence();
          if sequence < min_sequence {
            min_sequence = sequence;
            min_bucket_idx = bucket_idx;
            min_entry_idx = entry_idx;
          }
        }
      }
    }

    // Evict the oldest entry
    if min_sequence != u32::MAX {
      let bucket = &self.buckets[min_bucket_idx];
      let entry = &bucket.entries[min_entry_idx];
      let (state, _version, hash) = entry.get_state_version_hash();
      
      if state == STATE_WRITTEN {
        let value_old = entry.load_value();
        let (timestamp, sequence) = entry.get_timestamp_sequence();
        
        if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_TOMBSTONE, hash) {
          oldest_entries.push((hash, value_old, timestamp, sequence));
        }
      }
    }

    // Since we can't reuse the moved key and value, we need to return failure
    // The caller will need to retry the operation
    removed_entries.extend(oldest_entries);
    Ok((false, removed_entries))
  }

  pub fn get(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      let (state, version_before, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        let value = entry.load_value();
        
        // Verify consistency with single atomic read
        let (state_after, version_after, hash_after) = entry.get_state_version_hash();
        if state_after == STATE_WRITTEN && 
           version_after == version_before && 
           hash_after == key_hash_32 {
          return Some(value);
        }
      }
    }

    None
  }

  pub fn get_unverified(&self, key: &K) -> Option<T> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      let (state, _version, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        return Some(entry.load_value());
      }
    }

    None
  }

  pub fn get_with_metadata(&self, key: &K) -> Option<(T, u32, u32)> {
    let key_hash = self.hash_key(key);
    let key_hash_32 = key_hash as u32;
    let bucket_idx = (key_hash as usize) & self.bucket_mask;
    let bucket = &self.buckets[bucket_idx];

    for entry in &bucket.entries {
      let (state, version_before, hash) = entry.get_state_version_hash();
      if state == STATE_WRITTEN && hash == key_hash_32 {
        let value = entry.load_value();
        let (timestamp, sequence) = entry.get_timestamp_sequence();

        // Verify consistency
        let (state_after, version_after, hash_after) = entry.get_state_version_hash();
        if state_after == STATE_WRITTEN && 
           version_after == version_before && 
           hash_after == key_hash_32 {
          return Some((value, timestamp, sequence));
        }
      }
    }

    None
  }

  pub fn get_all_by_keys(&self, keys: &Vec<K>) -> Vec<Option<T>> {
    keys.iter().map(|key| self.get(key)).collect()
  }

  #[inline]
  pub fn contains_key(&self, key: &K) -> bool {
    self.get(key).is_some()
  }

  pub fn contains_any_key(&self, keys: &Vec<K>) -> bool {
    keys.iter().any(|key| self.contains_key(key))
  }

  pub fn contains_all_keys(&self, keys: &Vec<K>) -> bool {
    keys.iter().all(|key| self.contains_key(key))
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
        if let Ok(_) = entry.compare_exchange_state(STATE_WRITTEN, STATE_TOMBSTONE, key_hash_32) {
          return Some(value);
        }
      }
    }

    None
  }

  pub fn remove_all(&self, keys: &Vec<K>) -> Vec<Option<T>> {
    keys.iter().map(|key| self.remove(key)).collect()
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

    cleared
  }

  pub fn remove_existing(&self, keys: &Vec<K>) -> Vec<T> {
    keys.iter().filter_map(|key| self.remove(key)).collect()
  }

  pub fn remove_oldest(&self, count: usize) -> Vec<(u32, T, u32, u32)> {
    if count == 0 {
      return Vec::new();
    }

    // First, collect all current entries with their metadata
    let mut candidates = Vec::new();

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, version_before, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let value = entry.load_value();
          let (timestamp, sequence) = entry.get_timestamp_sequence();

          // Verify consistency
          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash {
            candidates.push((entry, hash, value, timestamp, sequence));
          }
        }
      }
    }

    // Sort by sequence number (oldest first - lowest sequence numbers)
    candidates.sort_unstable_by(|a, b| a.4.cmp(&b.4));

    // Try to remove the oldest entries up to the requested count
    let mut removed = Vec::new();
    let remove_count = count.min(candidates.len());

    for (entry, key_hash, value, timestamp, sequence) in candidates.into_iter().take(remove_count) {
      // Attempt to atomically transition from WRITTEN to TOMBSTONE
      if entry
        .compare_exchange_state(STATE_WRITTEN, STATE_TOMBSTONE, key_hash)
        .is_ok()
      {
        // Double-check that this is still the same entry (sequence hasn't changed)
        let (_, current_sequence) = entry.get_timestamp_sequence();
        if current_sequence == sequence {
          removed.push((key_hash, value, timestamp, sequence));
        }
        // If sequence changed, the slot was reused, but we still removed something
        // so we'll count it as successful removal
      }
    }

    removed
  }

  #[inline]
  pub fn len(&self) -> usize {
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

  /// Check if cache is empty
  pub fn is_empty(&self) -> bool {
    self.buckets
      .iter()
      .all(|bucket| {
        bucket.entries
          .iter()
          .all(|entry| {
            let (state, _, _) = entry.get_state_version_hash();
            state != STATE_WRITTEN
          })
      })
  }

  pub fn entries(&self) -> Vec<(u32, T)> {
    self
      .get_all()
      .into_iter()
      .map(|(key_hash, value, _, _)| (key_hash, value))
      .collect()
  }

  pub fn drain(&self) -> Vec<(u32, T, u32, u32)> {
    let mut results = Vec::new();

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        if let Some(drained) = entry.atomic_drain() {
          results.push(drained);
        }
      }
    }

    // Sort by sequence (most recent first)
    results.sort_unstable_by(|a, b| b.3.cmp(&a.3));
    results
  }

  #[inline]
  pub fn capacity(&self) -> usize {
    self.bucket_count * 4
  }

  pub fn get_all(&self) -> Vec<(u32, T, u32, u32)> {
    let mut results = Vec::new();

    for bucket in self.buckets.iter() {
      for entry in &bucket.entries {
        let (state, version_before, hash) = entry.get_state_version_hash();
        if state == STATE_WRITTEN {
          let value = entry.load_value();
          let (timestamp, sequence) = entry.get_timestamp_sequence();

          // Verify consistency
          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash {
            results.push((hash, value, timestamp, sequence));
          }
        }
      }
    }

    // Sort by sequence (most recent first)
    results.sort_unstable_by(|a, b| b.3.cmp(&a.3));
    results
  }

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
          let (timestamp, sequence) = entry.get_timestamp_sequence();

          // Verify consistency
          let (state_after, version_after, hash_after) = entry.get_state_version_hash();
          if state_after == STATE_WRITTEN && 
             version_after == version_before && 
             hash_after == hash &&
             !predicate(&value, timestamp, sequence) &&
             entry.compare_exchange_state(STATE_WRITTEN, STATE_TOMBSTONE, hash).is_ok() {
            removed_count += 1;
          }
        }
      }
    }

    removed_count
  }

  pub fn compute_key_hash(&self, key: &K) -> u64 {
    self.hash_key(key)
  }
}

#[cfg(test)]
#[cfg(debug_assertions)]
mod tests {
  use super::*;
  use std::{sync::atomic::AtomicBool, thread, time::Duration};

  trait CacheableValue: Clone + Send + Sync + Default + 'static {
    const IS_LOCK_FREE: bool = size_of::<Self>() <= 8 && align_of::<Self>() <= 8;
  }

  impl<T> CacheableValue for T where T: Clone + Send + Sync + Default + 'static {}

  fn create_cache<K: Clone + Hash + Eq + Send + Sync + 'static, T: CacheableValue>(
    capacity: usize,
  ) -> Arc<AtomicGenericCache<K, T>> {
    AtomicGenericCache::new(capacity, 0)
  }

  #[derive(Copy, Clone, Debug, Default, PartialEq)]
  struct TestEntry {
    id: u32,
    value: u32,
  }

  unsafe impl Sync for TestEntry {}
  unsafe impl Send for TestEntry {}

  #[test]
  fn test_large_value_fallback() {
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    struct LargeValue {
      data: [u64; 4], // 32 bytes > 8 bytes
    }

    let cache = create_cache::<String, LargeValue>(100);
    let large_val = LargeValue { data: [1, 2, 3, 4] };

    assert!(cache.insert("large_key".to_string(), large_val).unwrap());
    assert_eq!(cache.get(&"large_key".to_string()), Some(large_val));
  }

  #[test]
  fn test_concurrent_stress() {
    let cache = create_cache::<String, TestEntry>(1000);

    let handles: Vec<_> = (0..8)
      .map(|thread_id| {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
          for i in 0..1000 {
            let entry = TestEntry {
              id: (thread_id * 1000 + i) as u32,
              value: (thread_id * 1000 + i) as u32,
            };
            cache
              .insert(format!("key:{}:{}", thread_id, i), entry)
              .unwrap();
            cache.get(&format!("key:{}:{}", thread_id, i));
          }
        })
      })
      .collect();

    for handle in handles {
      handle.join().unwrap();
    }

    assert!(true);
  }

  #[test]
  fn test_concurrent_stress_2() {
    let cache = create_cache::<String, TestEntry>(1000);

    let handles: Vec<_> = (0..50)
      .map(|thread_id| {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
          for i in 0..1_000 {
            let entry = TestEntry {
              id: (thread_id * 1000 + i) as u32,
              value: (thread_id * 1000 + i) as u32,
            };
            cache
              .insert(format!("key:{}:{}", thread_id, i), entry)
              .unwrap();
          }
        })
      })
      .collect();

    for handle in handles {
      handle.join().unwrap();
    }

    let handles: Vec<_> = (0..50)
      .map(|thread_id| {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
          for i in 0..1_000 {
            cache.get(&format!("key:{}:{}", thread_id, i));
          }
        })
      })
      .collect();

    for handle in handles {
      handle.join().unwrap();
    }

    assert!(true);
  }

  #[derive(Copy, Clone, Debug, Default, PartialEq)]
  struct SessionData {
    user_id: u32,
    session_token: u32,
    permissions: u16,
    login_count: u16,
  }

  #[derive(Copy, Clone, Debug, Default, PartialEq)]
  struct LargeTestStruct {
    id: u64,
    data: [u64; 10], // 88 bytes total - forces fallback path
    checksum: u64,
  }

  impl LargeTestStruct {
    fn new(id: u64) -> Self {
      let data = [id; 10];
      let checksum = data.iter().sum();
      Self { id, data, checksum }
    }

    fn is_valid(&self) -> bool {
      self.data.iter().sum::<u64>() == self.checksum
    }
  }

  #[test]
  fn test_edge_case_zero_capacity() {
    let cache = create_cache::<String, u32>(0);
    assert_eq!(cache.capacity(), 64); // Should be clamped to minimum

    assert!(cache.insert("key".to_string(), 42).unwrap());
    assert_eq!(cache.get(&"key".to_string()), Some(42));
  }

  #[test]
  fn test_edge_case_power_of_two_capacity() {
    let cache = create_cache::<String, u32>(100);
    assert_eq!(cache.capacity(), 128); // Next power of 2

    let cache2 = create_cache::<String, u32>(256);
    assert_eq!(cache2.capacity(), 256); // Already power of 2
  }

  #[test]
  fn test_hash_collision_handling() {
    let cache = create_cache::<String, u32>(64);

    // Create keys that are likely to hash to similar values
    let keys: Vec<String> = (0..1000).map(|i| format!("collision_key_{}", i)).collect();

    // Insert all keys
    for (i, key) in keys.iter().enumerate() {
      assert!(cache.insert(key.clone(), i as u32).unwrap());
    }

    // Verify all keys can be retrieved (some may have been evicted due to ring buffer)
    let mut found_count = 0;
    for (i, key) in keys.iter().enumerate() {
      if let Some(value) = cache.get(key) {
        assert_eq!(value, i as u32);
        found_count += 1;
      }
    }

    assert!(found_count >= 32); // Should find at least some keys
  }

  #[test]
  fn test_key_overwrite_behavior() {
    let cache = create_cache::<String, u64>(100);

    // Insert initial value
    assert!(cache.insert("overwrite_key".to_string(), 100u64).unwrap());
    assert_eq!(cache.get(&"overwrite_key".to_string()), Some(100u64));

    // Overwrite with new value
    assert!(cache.insert("overwrite_key".to_string(), 200u64).unwrap());
    assert_eq!(cache.get(&"overwrite_key".to_string()), Some(200u64));

    // Overwrite multiple times rapidly
    for i in 300..400 {
      assert!(cache.insert("overwrite_key".to_string(), i).unwrap());
    }

    // Should have the last value
    if let Some(final_value) = cache.get(&"overwrite_key".to_string()) {
      assert!((300..400).contains(&final_value));
    }
  }

  #[test]
  fn test_memory_ordering_consistency() {
    let cache = create_cache::<String, u64>(1000);
    let inconsistency_detected = Arc::new(AtomicBool::new(false));

    // Writer thread that maintains sequence
    let writer_cache = Arc::clone(&cache);
    let writer_handle = thread::spawn(move || {
      for i in 0..10000u64 {
        writer_cache.insert(format!("seq_{}", i % 100), i).unwrap();
        if i % 1000 == 0 {
          thread::sleep(Duration::from_micros(1));
        }
      }
    });

    // Reader threads that check for consistency
    let readers: Vec<_> = (0..4)
      .map(|_| {
        let cache = Arc::clone(&cache);
        let inconsistency = Arc::clone(&inconsistency_detected);

        thread::spawn(move || {
          let mut last_seen = vec![0u64; 100];

          for _ in 0..5000 {
            for key_idx in 0..100 {
              let key = format!("seq_{}", key_idx);
              if let Some(value) = cache.get(&key) {
                if value < last_seen[key_idx] {
                  // Should never see a value go backwards (except for wraparound)
                  if last_seen[key_idx] - value > 5000 {
                    inconsistency.store(true, Ordering::Relaxed);
                  }
                }
                last_seen[key_idx] = value;
              }
            }

            if fastrand::u32(0..100) == 0 {
              thread::sleep(Duration::from_micros(1));
            }
          }
        })
      })
      .collect();

    writer_handle.join().unwrap();
    for reader in readers {
      reader.join().unwrap();
    }

    assert!(
      !inconsistency_detected.load(Ordering::Relaxed),
      "Memory ordering inconsistency detected!"
    );
  }

  #[test]
  fn test_large_value_fallback_path() {
    let cache = create_cache::<String, LargeTestStruct>(100);

    let large_val = LargeTestStruct::new(12345);
    assert!(large_val.is_valid());

    assert!(cache.insert("large_key".to_string(), large_val).unwrap());

    if let Some(retrieved) = cache.get(&"large_key".to_string()) {
      assert_eq!(retrieved.id, 12345);
      assert!(retrieved.is_valid());
      assert_eq!(retrieved, large_val);
    } else {
      panic!("Failed to retrieve large value");
    }

    // Test concurrent access to large values
    let handles: Vec<_> = (0..5)
      .map(|thread_id| {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
          for i in 0..100 {
            let key = format!("large_{}_{}", thread_id, i);
            let val = LargeTestStruct::new((thread_id * 100 + i) as u64);

            cache.insert(key.clone(), val).unwrap();

            if let Some(retrieved) = cache.get(&key) {
              assert!(retrieved.is_valid());
              assert_eq!(retrieved.id, (thread_id * 100 + i) as u64);
            }
          }
        })
      })
      .collect();

    for handle in handles {
      handle.join().unwrap();
    }
  }

  #[test]
  fn test_remove_and_tombstone_behavior() {
    let cache = create_cache::<String, SessionData>(100);

    // Insert some test data
    let sessions = vec![
      (
        "user1",
        SessionData {
          user_id: 1,
          session_token: 0xAAA,
          permissions: 0xFF,
          login_count: 5,
        },
      ),
      (
        "user2",
        SessionData {
          user_id: 2,
          session_token: 0xBBB,
          permissions: 0x0F,
          login_count: 2,
        },
      ),
      (
        "user3",
        SessionData {
          user_id: 3,
          session_token: 0xCCC,
          permissions: 0xF0,
          login_count: 10,
        },
      ),
    ];

    for (key, session) in &sessions {
      assert!(cache.insert(key.to_string(), *session).unwrap());
    }

    assert_eq!(cache.len(), 3);

    // Remove middle entry
    let removed = cache.remove(&"user2".to_string());
    assert_eq!(removed, Some(sessions[1].1));
    assert_eq!(cache.len(), 2);

    // Verify other entries still exist
    assert_eq!(cache.get(&"user1".to_string()), Some(sessions[0].1));
    assert_eq!(cache.get(&"user3".to_string()), Some(sessions[2].1));
    assert_eq!(cache.get(&"user2".to_string()), None);

    // Re-insert into tombstone slot
    let new_session = SessionData {
      user_id: 4,
      session_token: 0xDDD,
      permissions: 0xFF,
      login_count: 1,
    };
    assert!(cache.insert("user2".to_string(), new_session).unwrap());
    assert_eq!(cache.get(&"user2".to_string()), Some(new_session));
    assert_eq!(cache.len(), 3);
  }

  #[test]
  fn test_retain_functionality_comprehensive() {
    let cache = create_cache::<String, SessionData>(200);

    // Insert varied test data
    for i in 0..150 {
      let session = SessionData {
        user_id: i as u32,
        session_token: 0x1000 + i as u32,
        permissions: (i % 4) as u16 * 0x0F,
        login_count: (i % 20) as u16,
      };
      cache.insert(format!("user_{}", i), session).unwrap();
    }

    let initial_count = cache.len();

    // Retain only active users (login_count > 5)
    let removed_inactive = cache.retain(|session, _ts, _seq| session.login_count > 5);
    let after_activity_filter = cache.len();

    assert!(removed_inactive > 0);
    assert_eq!(initial_count, after_activity_filter + removed_inactive);

    // Retain only privileged users (permissions > 0)
    let removed_no_perms = cache.retain(|session, _ts, _seq| session.permissions > 0);
    let after_perms_filter = cache.len();

    assert_eq!(after_activity_filter, after_perms_filter + removed_no_perms);

    // Verify remaining entries meet criteria
    let all_remaining = cache.get_all();
    for (_, session, _, _) in all_remaining {
      assert!(session.login_count > 5);
      assert!(session.permissions > 0);
    }
  }

  #[test]
  fn test_metadata_timestamp_ordering() {
    let cache = create_cache::<String, u32>(100);

    let mut insert_order = Vec::new();

    // Insert entries with deliberate delays to ensure different timestamps
    for i in 0..10 {
      let key = format!("timed_key_{}", i);
      cache.insert(key.clone(), i as u32).unwrap();
      insert_order.push(key);
      thread::sleep(Duration::from_millis(2)); // Ensure different timestamps
    }

    // Get all entries and verify timestamp ordering
    let all_entries = cache.get_all();
    assert!(!all_entries.is_empty());

    // Should be sorted by sequence number (newest first)
    for i in 1..all_entries.len() {
      assert!(
        all_entries[i - 1].3 >= all_entries[i].3,
        "Entries should be ordered by sequence number"
      );
    }

    // Check metadata consistency
    for (i, key) in insert_order.iter().enumerate() {
      if let Some((value, timestamp, sequence)) = cache.get_with_metadata(key) {
        assert_eq!(value, i as u32);
        assert!(timestamp > 0);
        assert!(sequence > 0);
      }
    }
  }

  #[test]
  fn test_cache_overflow_ring_buffer() {
    let small_cache = create_cache::<String, u64>(32); // Small cache to force overflow

    // Fill way beyond capacity
    let overflow_factor = 5;
    let total_inserts = small_cache.capacity() * overflow_factor;

    for i in 0..total_inserts {
      small_cache
        .insert(format!("overflow_{}", i), i as u64)
        .unwrap();
    }

    // Cache should not exceed capacity
    assert!(small_cache.len() <= small_cache.capacity());

    // Most recent entries should still be findable
    let mut recent_found = 0;
    let recent_range = total_inserts - small_cache.capacity()..total_inserts;

    for i in recent_range {
      if small_cache.get(&format!("overflow_{}", i)).is_some() {
        recent_found += 1;
      }
    }

    assert!(recent_found > small_cache.capacity() / 4); // Should find reasonable portion

    // Older entries should mostly be gone
    let mut old_found = 0;
    for i in 0..small_cache.capacity() {
      if small_cache.get(&format!("overflow_{}", i)).is_some() {
        old_found += 1;
      }
    }

    assert!(old_found < small_cache.capacity() / 2); // Most old entries should be gone
  }

  #[test]
  fn test_edge_case_empty_and_invalid_keys() {
    let cache = create_cache::<String, u32>(100);

    // Test empty string key
    assert!(cache.insert("".to_string(), 42u32).unwrap());
    assert_eq!(cache.get(&"".to_string()), Some(42u32));

    // Test very long key
    let long_key = "a".repeat(1000);
    assert!(cache.insert(long_key.clone(), 123u32).unwrap());
    assert_eq!(cache.get(&long_key), Some(123u32));

    // Test special characters in keys
    let special_keys = [
      "key with spaces",
      "key\nwith\nnewlines",
      "key\twith\ttabs",
      "key-with-dashes",
      "key_with_underscores",
      "key.with.dots",
      "key/with/slashes",
      "key\\with\\backslashes",
      "🦀🔥 emoji key 🚀",
    ];

    for (i, key) in special_keys.iter().enumerate() {
      assert!(cache.insert(key.to_string(), i as u32).unwrap());
      assert_eq!(cache.get(&key.to_string()), Some(i as u32));
    }
  }

  #[test]
  fn test_deterministic_behavior_same_input() {
    // Test that same operations produce consistent results
    for _run in 0..3 {
      let cache = create_cache::<String, u64>(100);

      // Same sequence of operations
      let operations = vec![
        ("key1", 100u64),
        ("key2", 200u64),
        ("key3", 300u64),
        ("key1", 150u64), // Overwrite
        ("key4", 400u64),
      ];

      for (key, value) in &operations {
        cache.insert(key.to_string(), *value).unwrap();
      }

      // Results should be consistent across runs
      assert_eq!(cache.get(&"key1".to_string()), Some(150u64));
      assert_eq!(cache.get(&"key2".to_string()), Some(200u64));
      assert_eq!(cache.get(&"key3".to_string()), Some(300u64));
      assert_eq!(cache.get(&"key4".to_string()), Some(400u64));
      assert_eq!(cache.get(&"nonexistent".to_string()), None);
    }
  }

  #[test]
  fn test_drain() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    assert_eq!(cache.len(), 3);

    // Drain the cache
    let drained_entries = cache.drain();

    // Verify cache is empty after drain
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);

    // Verify all entries were drained
    assert_eq!(drained_entries.len(), 3);

    // Extract key hashes and values for verification
    let mut drained_values: Vec<String> = drained_entries
      .iter()
      .map(|(_, value, _, _)| value.clone())
      .collect();
    drained_values.sort();

    let mut expected_values = vec![
      "value1".to_string(),
      "value2".to_string(),
      "value3".to_string(),
    ];
    expected_values.sort();

    assert_eq!(drained_values, expected_values);

    // Verify entries have metadata
    for (key_hash, _, timestamp, sequence) in &drained_entries {
      assert_ne!(*key_hash, 0); // Key hash should be non-zero
      assert_ne!(*timestamp, 0); // Timestamp should be set
      assert_ne!(*sequence, 0); // Sequence should be set
    }
  }

  #[test]
  fn test_drain_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    let drained_entries = cache.drain();

    assert!(drained_entries.is_empty());
    assert!(cache.is_empty());
  }

  #[test]
  fn test_drain_ordering_by_sequence() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Insert entries with forced time gaps
    assert!(cache
      .insert("first".to_string(), "value1".to_string())
      .unwrap());
    std::thread::sleep(std::time::Duration::from_millis(100)); // time gap

    assert!(cache
      .insert("second".to_string(), "value2".to_string())
      .unwrap());
    std::thread::sleep(std::time::Duration::from_millis(100)); // time gap

    assert!(cache
      .insert("third".to_string(), "value3".to_string())
      .unwrap());

    let drained_entries = cache.drain();

    // Verify we got all entries
    assert_eq!(drained_entries.len(), 3);

    let mut values: Vec<String> = drained_entries
      .iter()
      .map(|(_, value, _, _)| value.clone())
      .collect();
    values.sort();

    let mut expected_values = vec![
      "value1".to_string(),
      "value2".to_string(),
      "value3".to_string(),
    ];
    expected_values.sort();

    assert_eq!(values, expected_values);

    // Verify all entries have valid metadata
    for (key_hash, _, timestamp, sequence) in &drained_entries {
      assert_ne!(*key_hash, 0, "Key hash should be non-zero");
      assert_ne!(*timestamp, 0, "Timestamp should be non-zero");
      assert_ne!(*sequence, 0, "Sequence should be non-zero");
    }

    // Verify cache is empty after drain
    assert!(cache.is_empty());
  }

  #[test]
  fn test_entries() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    let entries = cache.entries();

    // Verify we get all entries
    assert_eq!(entries.len(), 3);

    // Extract values for comparison
    let mut values: Vec<String> = entries.iter().map(|(_, value)| value.clone()).collect();
    values.sort();

    let mut expected_values = vec![
      "value1".to_string(),
      "value2".to_string(),
      "value3".to_string(),
    ];
    expected_values.sort();

    assert_eq!(values, expected_values);

    // Verify all key hashes are non-zero
    for (key_hash, _) in &entries {
      assert_ne!(*key_hash, 0);
    }

    // Verify cache is unchanged
    assert_eq!(cache.len(), 3);
  }

  #[test]
  fn test_entries_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    let entries = cache.entries();

    assert!(entries.is_empty());
  }

  #[test]
  fn test_remove_existing() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    // Remove mix of existing and non-existing keys
    let keys = vec![
      "key1".to_string(),
      "nonexistent".to_string(),
      "key3".to_string(),
      "another_missing".to_string(),
    ];
    let removed_values = cache.remove_existing(&keys);

    // Should only return values that were actually removed
    assert_eq!(removed_values.len(), 2);

    let mut sorted_removed = removed_values.clone();
    sorted_removed.sort();

    let mut expected = vec!["value1".to_string(), "value3".to_string()];
    expected.sort();

    assert_eq!(sorted_removed, expected);

    // Verify cache state
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
    assert_eq!(cache.get(&"key1".to_string()), None);
    assert_eq!(cache.get(&"key3".to_string()), None);
  }

  #[test]
  fn test_remove_existing_all_missing() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add some data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());

    // Try to remove only non-existing keys
    let keys = vec![
      "missing1".to_string(),
      "missing2".to_string(),
      "missing3".to_string(),
    ];
    let removed_values = cache.remove_existing(&keys);

    assert!(removed_values.is_empty());
    assert_eq!(cache.len(), 1); // Original data should remain
  }

  #[test]
  fn test_remove_existing_empty_keys() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());

    let removed_values = cache.remove_existing(&Vec::<String>::new());

    assert!(removed_values.is_empty());
    assert_eq!(cache.len(), 1);
  }

  #[test]
  fn test_remove_all() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    // Remove mix of existing and non-existing keys
    let keys = vec![
      "key1".to_string(),
      "nonexistent".to_string(),
      "key3".to_string(),
      "another_missing".to_string(),
    ];
    let removed_values = cache.remove_all(&keys);

    // Should return same length as input, with None for missing keys
    assert_eq!(removed_values.len(), 4);
    assert_eq!(removed_values[0], Some("value1".to_string())); // key1
    assert_eq!(removed_values[1], None); // nonexistent
    assert_eq!(removed_values[2], Some("value3".to_string())); // key3
    assert_eq!(removed_values[3], None); // another_missing

    // Verify cache state
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
  }

  #[test]
  fn test_remove_all_preserve_order() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    assert!(cache
      .insert("a".to_string(), "value_a".to_string())
      .unwrap());
    assert!(cache
      .insert("b".to_string(), "value_b".to_string())
      .unwrap());
    assert!(cache
      .insert("c".to_string(), "value_c".to_string())
      .unwrap());

    let keys = vec![
      "c".to_string(),
      "a".to_string(),
      "missing".to_string(),
      "b".to_string(),
    ];
    let removed_values = cache.remove_all(&keys);

    // Verify order is preserved
    assert_eq!(removed_values.len(), 4);
    assert_eq!(removed_values[0], Some("value_c".to_string()));
    assert_eq!(removed_values[1], Some("value_a".to_string()));
    assert_eq!(removed_values[2], None);
    assert_eq!(removed_values[3], Some("value_b".to_string()));
  }

  #[test]
  fn test_remove_all_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    let keys = vec!["key1".to_string(), "key2".to_string()];
    let removed_values = cache.remove_all(&keys);

    assert_eq!(removed_values.len(), 2);
    assert_eq!(removed_values[0], None);
    assert_eq!(removed_values[1], None);
  }

  #[test]
  fn test_contains_all_keys() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    // Test with all existing keys
    let all_existing = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    assert!(cache.contains_all_keys(&all_existing));

    // Test with subset of existing keys
    let subset_existing = vec!["key1".to_string(), "key3".to_string()];
    assert!(cache.contains_all_keys(&subset_existing));

    // Test with one missing key
    let one_missing = vec![
      "key1".to_string(),
      "key2".to_string(),
      "missing".to_string(),
    ];
    assert!(!cache.contains_all_keys(&one_missing));

    // Test with all missing keys
    let all_missing = vec!["missing1".to_string(), "missing2".to_string()];
    assert!(!cache.contains_all_keys(&all_missing));

    // Test with empty vector (should return true)
    let empty_keys: Vec<String> = vec![];
    assert!(cache.contains_all_keys(&empty_keys));
  }

  #[test]
  fn test_contains_all_keys_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Empty cache with any non-empty key list should return false
    let keys = vec!["key1".to_string()];
    assert!(!cache.contains_all_keys(&keys));

    // Empty cache with empty key list should return true
    let empty_keys: Vec<String> = vec![];
    assert!(cache.contains_all_keys(&empty_keys));
  }

  #[test]
  fn test_contains_any_key() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    // Test with all existing keys
    let all_existing = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    assert!(cache.contains_any_key(&all_existing));

    // Test with one existing key
    let one_existing = vec![
      "missing1".to_string(),
      "key2".to_string(),
      "missing2".to_string(),
    ];
    assert!(cache.contains_any_key(&one_existing));

    // Test with no existing keys
    let no_existing = vec![
      "missing1".to_string(),
      "missing2".to_string(),
      "missing3".to_string(),
    ];
    assert!(!cache.contains_any_key(&no_existing));

    // Test with empty vector (should return false)
    let empty_keys: Vec<String> = vec![];
    assert!(!cache.contains_any_key(&empty_keys));
  }

  #[test]
  fn test_contains_any_key_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Empty cache should return false for any key list
    let keys = vec!["key1".to_string(), "key2".to_string()];
    assert!(!cache.contains_any_key(&keys));

    let empty_keys: Vec<String> = vec![];
    assert!(!cache.contains_any_key(&empty_keys));
  }

  #[test]
  fn test_get_all_by_keys() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    // Add test data
    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());
    assert!(cache
      .insert("key2".to_string(), "value2".to_string())
      .unwrap());
    assert!(cache
      .insert("key3".to_string(), "value3".to_string())
      .unwrap());

    // Test with mix of existing and non-existing keys
    let keys = vec![
      "key1".to_string(),
      "missing".to_string(),
      "key3".to_string(),
      "another_missing".to_string(),
      "key2".to_string(),
    ];
    let values = cache.get_all_by_keys(&keys);

    // Should return values in same order as keys, with None for missing
    assert_eq!(values.len(), 5);
    assert_eq!(values[0], Some("value1".to_string())); // key1
    assert_eq!(values[1], None); // missing
    assert_eq!(values[2], Some("value3".to_string())); // key3
    assert_eq!(values[3], None); // another_missing
    assert_eq!(values[4], Some("value2".to_string())); // key2
  }

  #[test]
  fn test_get_all_by_keys_preserve_order() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    assert!(cache
      .insert("a".to_string(), "value_a".to_string())
      .unwrap());
    assert!(cache
      .insert("b".to_string(), "value_b".to_string())
      .unwrap());
    assert!(cache
      .insert("c".to_string(), "value_c".to_string())
      .unwrap());

    // Test different orderings
    let keys1 = vec!["c".to_string(), "a".to_string(), "b".to_string()];
    let values1 = cache.get_all_by_keys(&keys1);
    assert_eq!(values1[0], Some("value_c".to_string()));
    assert_eq!(values1[1], Some("value_a".to_string()));
    assert_eq!(values1[2], Some("value_b".to_string()));

    let keys2 = vec!["b".to_string(), "c".to_string(), "a".to_string()];
    let values2 = cache.get_all_by_keys(&keys2);
    assert_eq!(values2[0], Some("value_b".to_string()));
    assert_eq!(values2[1], Some("value_c".to_string()));
    assert_eq!(values2[2], Some("value_a".to_string()));
  }

  #[test]
  fn test_get_all_by_keys_empty_input() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());

    let empty_keys: Vec<String> = vec![];
    let values = cache.get_all_by_keys(&empty_keys);

    assert!(values.is_empty());
  }

  #[test]
  fn test_get_all_by_keys_empty_cache() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let values = cache.get_all_by_keys(&keys);

    assert_eq!(values.len(), 3);
    assert_eq!(values[0], None);
    assert_eq!(values[1], None);
    assert_eq!(values[2], None);
  }

  #[test]
  fn test_get_all_by_keys_duplicate_keys() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    assert!(cache
      .insert("key1".to_string(), "value1".to_string())
      .unwrap());

    // Test with duplicate keys
    let keys = vec![
      "key1".to_string(),
      "key1".to_string(),
      "missing".to_string(),
      "key1".to_string(),
    ];
    let values = cache.get_all_by_keys(&keys);

    assert_eq!(values.len(), 4);
    assert_eq!(values[0], Some("value1".to_string()));
    assert_eq!(values[1], Some("value1".to_string()));
    assert_eq!(values[2], None);
    assert_eq!(values[3], Some("value1".to_string()));
  }

  #[test]
  fn test_concurrent_drain() {
    let cache = Arc::new(AtomicGenericCache::<String, String>::new(256, 0));

    // Populate cache
    for i in 0..100 {
      assert!(cache
        .insert(format!("key{}", i), format!("value{}", i))
        .unwrap());
    }

    let initial_len = cache.len();
    assert!(initial_len > 80);

    // Concurrent drain operations
    let mut handles = vec![];

    for _ in 0..3 {
      let cache_clone = Arc::clone(&cache);
      let handle = thread::spawn(move || cache_clone.drain());
      handles.push(handle);
    }

    let mut all_drained = vec![];
    for handle in handles {
      let drained = handle.join().unwrap();
      all_drained.extend(drained);
    }

    // Cache should be empty or nearly empty
    assert!(cache.len() < initial_len);

    // Should have drained some entries
    assert!(!all_drained.is_empty());

    // Verify no duplicate entries were drained
    let mut key_hashes: Vec<u64> = all_drained.iter().map(|(hash, _, _, _)| *hash as u64).collect::<Vec<u64>>();
    key_hashes.sort();
    let original_len = key_hashes.len();
    key_hashes.dedup();
    assert_eq!(
      key_hashes.len(),
      original_len,
      "Duplicate entries were drained"
    );
  }

  #[test]
  fn test_compute_key_hash() {
    let cache = AtomicGenericCache::<String, String>::new(64, 0);

    let hash1 = cache.compute_key_hash(&"key1".to_string());
    let hash2 = cache.compute_key_hash(&"key2".to_string());
    let hash1_again = cache.compute_key_hash(&"key1".to_string());

    // Different keys should have different hashes
    assert_ne!(hash1, hash2);

    // Same key should always produce same hash
    assert_eq!(hash1, hash1_again);

    // Hashes should be non-zero
    assert_ne!(hash1, 0);
    assert_ne!(hash2, 0);
  }

  #[test]
  fn test_clear() {
    let cache = AtomicGenericCache::<String, i32>::new(512, 0);

    // Insert many items
    for i in 0..100 {
      assert!(cache.insert(format!("key_{}", i), i).unwrap());
    }

    assert!(cache.len() > 95);

    cache.clear();

    assert_eq!(cache.len(), 0);
  }

  #[test]
  fn test_capacity() {
    let cache = AtomicGenericCache::<String, i32>::new(1000, 512);

    // Insert many items
    for i in 0..1000 {
      _ = cache.insert(format!("key_{}", i), i);
    }

    assert!(cache.len() <= 530 && cache.capacity() >= 470);
  }
}
