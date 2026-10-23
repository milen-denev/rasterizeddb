//! A filter checkpoint whose write fails must leave the sidecar as untrusted
//! as it found it, and must not leave the in-memory bookkeeping claiming the
//! record landed.
//!
//! The checkpoint used to write through `StorageIO::write_data`, which cannot
//! report a failure, and then clear `disk_dirty` and every column's
//! checkpointed marks whatever had happened. A failed base image then left the
//! filter believing the sidecar was clean and current, so nothing rewrote it;
//! a failed delta left `disk_len` past bytes that never landed, so every later
//! delta went where no replay reaches it.
//!
//! The provider here fails a write in one of two ways. The plain one models a
//! logged provider: the file is left as it was, and the failure is reported by
//! the transaction's `commit_durable` (and swallowed by `write_data`, as
//! `LocalStorageProvider::write_data` does). The in-doubt one applies the
//! bytes and still fails the commit, which is what the KV provider reports
//! for a put that may yet arrive, what a mirror reports when its backups were
//! written and the primary's commit failed, and what a log-less file provider
//! reports when its flush fails after the overwrite.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;
use smallvec::SmallVec;

use super::persist::LoadReject;
use super::{FilterConfig, TableFilters, sarg};
use crate::core::db_type::DbType;
use crate::core::index_store::gate::{Proof, test_mint};
use crate::core::row::schema::SchemaField;
use crate::core::storage_providers::traits::{IOResult, StorageIO};

#[derive(Clone, Default)]
struct SidecarIo {
    bytes: Arc<Mutex<Vec<u8>>>,
    /// Writes still to fail. Each failure leaves the file untouched.
    fail_next: Arc<AtomicU32>,
    /// Writes still to fail in doubt: each lands, and its commit fails anyway.
    land_and_fail_next: Arc<AtomicU32>,
    /// Transactions whose write failed, for `commit_durable` to report.
    failed: Arc<Mutex<HashSet<u64>>>,
    next_txn: Arc<AtomicU64>,
}

impl SidecarIo {
    fn fail_next_write(&self) {
        self.fail_next.fetch_add(1, Ordering::AcqRel);
    }

    fn fail_next_write_after_landing(&self) {
        self.land_and_fail_next.fetch_add(1, Ordering::AcqRel);
    }

    fn take_failure(&self) -> bool {
        self.fail_next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
    }

    fn take_landed_failure(&self) -> bool {
        self.land_and_fail_next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
    }

    fn apply(&self, position: u64, buffer: &[u8]) {
        let mut b = self.bytes.lock();
        let end = position as usize + buffer.len();
        if b.len() < end {
            b.resize(end, 0);
        }
        b[position as usize..end].copy_from_slice(buffer);
    }

    fn image(&self) -> Vec<u8> {
        self.bytes.lock().clone()
    }
}

impl StorageIO for SidecarIo {
    async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
        true
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        if !self.take_failure() {
            self.apply(position, buffer);
        }
    }

    async fn write_data_seek(&self, _seek: std::io::SeekFrom, _buffer: &[u8]) {
        unreachable!("the filter writes at explicit offsets")
    }

    async fn read_data_into_buffer(&self, _position: &mut u64, _buffer: &mut [u8]) -> IOResult<()> {
        unreachable!("the filter does not read its sidecar through the provider here")
    }

    async fn read_vectored(&self, _reads: &mut [(u64, &mut [u8])]) -> IOResult<()> {
        unreachable!("the filter does not read its sidecar through the provider here")
    }

    async fn append_data(&self, _buffer: &[u8], _immediate: bool) {
        unreachable!("the filter writes at explicit offsets")
    }

    async fn append_data_returning_offset(&self, _buffer: &[u8], _immediate: bool) -> u64 {
        unreachable!("the filter writes at explicit offsets")
    }

    fn begin_txn(&self) -> u64 {
        self.next_txn.fetch_add(1, Ordering::AcqRel) + 1
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        if self.take_landed_failure() {
            self.apply(position, buffer);
            self.failed.lock().insert(txn);
        } else if self.take_failure() {
            self.failed.lock().insert(txn);
        } else {
            self.apply(position, buffer);
        }
        txn
    }

    async fn commit_durable(&self, token: u64) -> IOResult<()> {
        if self.failed.lock().remove(&token) {
            return Err(std::io::Error::other("injected sidecar write failure"));
        }
        Ok(())
    }

    async fn get_len(&self) -> u64 {
        self.bytes.lock().len() as u64
    }

    fn exists(_location: &str, _table_name: &str) -> bool {
        false
    }

    async fn create_temp(&self) -> Self {
        self.clone()
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {}

    fn get_location(&self) -> Option<String> {
        None
    }

    async fn create_new(&self, _name: String) -> Self {
        self.clone()
    }

    fn drop_io(&self) {}

    fn get_hash(&self) -> u32 {
        0
    }

    async fn start_service(&self) {}

    fn get_name(&self) -> String {
        "SidecarIo".to_string()
    }
}

fn schema() -> SmallVec<[SchemaField; 20]> {
    smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("age".into(), DbType::I32, 4, 0, 1, false),
    ]
}

fn value(slot: u64) -> u64 {
    slot.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1
}

fn index(f: &TableFilters, slots: std::ops::Range<u64>) {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    for slot in slots {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, value(slot).to_le_bytes().to_vec()),
            (1u32, ((slot % 97) as i32).to_le_bytes().to_vec()),
        ];
        f.backfill_row(proof, slot, &cols);
    }
}

fn load(io: &SidecarIo, n: u64) -> Result<TableFilters, LoadReject> {
    TableFilters::from_checkpoint(&io.image(), &schema(), FilterConfig::default(), n)
}

/// Whether `f` keeps `slot` as a candidate for `id = value(slot)`.
fn finds(f: &TableFilters, slot: u64, n: u64) -> bool {
    let toks = crate::core::tokenizer::query_tokenizer::tokenize(
        &format!("id = {}", value(slot)),
        &schema(),
    );
    let program = sarg::extract(&toks, f).expect("an equality on an indexed column extracts");
    f.evaluate(&program, n).is_none_or(|bm| bm.get(slot))
}

/// A delete marks the sidecar dirty, so the next checkpoint plans a base image
/// to clear the byte. When that write fails the checkpoint reports it, and the
/// next checkpoint rewrites the sidecar with every delete since in it.
#[tokio::test]
async fn a_failed_base_image_is_reported_and_rewritten() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let n = 2_000u64;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n);
    let io = SidecarIo::default();
    assert!(f.try_checkpoint(&io, n).await.is_ok());
    assert!(load(&io, n).is_ok(), "the first checkpoint did not load");

    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 7);
    assert!(f.disk_dirty.load(Ordering::Acquire));

    io.fail_next_write();
    assert!(
        f.try_checkpoint(&io, n).await.is_err(),
        "a checkpoint whose write failed reported success"
    );
    // A precondition about the fake rather than a check of the fix: this
    // failure leaves the file as it was, so its byte is still set.
    assert_eq!(load(&io, n).err(), Some(LoadReject::Dirty));

    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 8);
    assert_eq!(
        load(&io, n).err(),
        Some(LoadReject::Dirty),
        "the next open trusted a sidecar after a failed checkpoint and a delete"
    );

    // The retry is the next checkpoint, and it has to write a base image: the
    // sidecar on disk is still the dirty one. It vouches only for slots below
    // the delete of 8 (`persistable_valid_upto`), which still covers 7.
    assert!(f.try_checkpoint(&io, n).await.is_ok());
    assert!(!f.disk_dirty.load(Ordering::Acquire), "a successful checkpoint left disk_dirty set");
    let back = load(&io, n).expect("the checkpoint after a failed one was never written");
    assert_eq!(back.resume_slot(), 8);
    assert!(!finds(&back, 7, n), "the delete of slot 7 is missing from the rewritten sidecar");
    assert!(finds(&back, 3, n));
}

/// A base image whose commit fails after its bytes landed leaves a clean
/// sidecar on disk that the filter cannot know about. The next delete has to
/// set the byte again before it changes a row, or a crash after it reopens to
/// that clean sidecar and prunes the row the delete changed.
#[tokio::test]
async fn a_base_image_that_landed_but_failed_is_marked_dirty_by_the_next_delete() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let n = 2_000u64;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n);
    let io = SidecarIo::default();
    assert!(f.try_checkpoint(&io, n).await.is_ok());

    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 7);

    io.fail_next_write_after_landing();
    assert!(f.try_checkpoint(&io, n).await.is_err());
    // Precondition about the fake: the failed base image is on disk, clean.
    assert!(load(&io, n).is_ok());

    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 8);
    assert_eq!(
        load(&io, n).err(),
        Some(LoadReject::Dirty),
        "a delete after an in-doubt checkpoint left the sidecar on disk marked clean"
    );

    assert!(f.try_checkpoint(&io, n).await.is_ok());
    let back = load(&io, n).expect("the checkpoint after an in-doubt one did not load");
    assert_eq!(back.resume_slot(), 8, "the rewrite vouches for slots past the delete after it");
    assert!(!finds(&back, 7, n));
    assert!(finds(&back, 3, n));
}

/// The same for the first checkpoint, where `has_checkpoint` is what
/// `mark_disk_dirty` consults.
#[tokio::test]
async fn a_first_checkpoint_that_landed_but_failed_is_marked_dirty_by_the_next_delete() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let n = 2_000u64;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n);
    let io = SidecarIo::default();

    io.fail_next_write_after_landing();
    assert!(f.try_checkpoint(&io, n).await.is_err());
    assert!(load(&io, n).is_ok(), "precondition: the failed first checkpoint is on disk");

    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 7);
    assert_eq!(
        load(&io, n).err(),
        Some(LoadReject::Dirty),
        "a delete after an in-doubt first checkpoint left that checkpoint trusted"
    );
}

/// A first checkpoint that failed with nothing written leaves no sidecar to
/// trust, and the dirty byte the next delete writes does not make one.
#[tokio::test]
async fn a_first_checkpoint_that_failed_leaves_nothing_to_trust() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let n = 2_000u64;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n);
    let io = SidecarIo::default();

    io.fail_next_write();
    assert!(f.try_checkpoint(&io, n).await.is_err());
    f.mark_disk_dirty(&io).await;
    f.on_delete(proof, 7);
    assert!(load(&io, n).is_err(), "a sidecar nobody finished writing was trusted");

    assert!(f.try_checkpoint(&io, n).await.is_ok());
    let back = load(&io, n).expect("the checkpoint after a failed first one did not load");
    assert_eq!(back.resume_slot(), 7);
    assert!(finds(&back, 3, n));
}

/// A delta that fails leaves the file as it was. The next record must not be
/// written past the bytes that never landed, where replay stops before it.
#[tokio::test]
async fn a_failed_delta_does_not_strand_the_records_after_it() {
    let n0 = 20_000u64;
    let n1 = n0 + 100;
    let n2 = n1 + 100;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n0);
    let io = SidecarIo::default();
    assert!(f.try_checkpoint(&io, n0).await.is_ok());
    assert_eq!(load(&io, n0).expect("the base did not load").resume_slot(), n0);

    index(&f, n0..n1);
    io.fail_next_write();
    assert!(f.try_checkpoint(&io, n1).await.is_err(), "a failed delta reported success");
    // The file is still the base: sound, and short of the failed rows.
    assert_eq!(load(&io, n1).expect("the base stopped loading").resume_slot(), n0);

    index(&f, n1..n2);
    assert!(f.try_checkpoint(&io, n2).await.is_ok());
    let back = load(&io, n2).expect("the checkpoint after a failed delta did not load");
    assert_eq!(
        back.resume_slot(),
        n2,
        "the record after a failed delta was written where no replay reaches it"
    );
    for slot in [0, n0, n1 - 1, n1, n2 - 1] {
        assert!(finds(&back, slot, n2), "slot {slot} lost across the failed checkpoint");
    }
}

/// The same failed delta, landed in doubt: the next record is a base image
/// either way, so whatever the delta left behind is overwritten, and the
/// chain goes on from that base.
#[tokio::test]
async fn a_delta_that_landed_but_failed_is_overwritten_by_the_next_record() {
    let n0 = 20_000u64;
    let n1 = n0 + 100;
    let n2 = n1 + 100;
    let n3 = n2 + 100;
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n0);
    let io = SidecarIo::default();
    assert!(f.try_checkpoint(&io, n0).await.is_ok());

    index(&f, n0..n1);
    io.fail_next_write_after_landing();
    assert!(f.try_checkpoint(&io, n1).await.is_err());

    index(&f, n1..n2);
    assert!(f.try_checkpoint(&io, n2).await.is_ok());
    assert_eq!(f.deltas_since_base(), 0, "the record after a failed one was not a base image");
    index(&f, n2..n3);
    assert!(f.try_checkpoint(&io, n3).await.is_ok());
    let back = load(&io, n3).expect("the chain after an in-doubt delta did not load");
    assert_eq!(back.resume_slot(), n3);
    for slot in [0, n0, n1 - 1, n1, n2 - 1, n2, n3 - 1] {
        assert!(finds(&back, slot, n3), "slot {slot} lost across the in-doubt checkpoint");
    }
}

/// A KV-backed table's sidecar is written through the KV provider, and the
/// checkpoint's write is a family transaction there. A base image, a delete
/// and its rewrite, then a delta, all have to come back from the store after a
/// restart.
#[cfg(feature = "kv_storage")]
#[tokio::test]
async fn a_kv_sidecar_checkpoints_and_reopens() {
    use crate::core::storage_providers::kv::{KvConfig, KvFamily, KvStorageProvider, KvStore, MemoryKvStore};
    let ticket = test_mint();
    let proof = Proof::from(&ticket);

    async fn open(store: &Arc<dyn KvStore>) -> KvStorageProvider {
        let mut config = KvConfig::new("t");
        config.chunk_size = 4096;
        config.page_size = 1024;
        let family = KvFamily::open(store.clone(), config).await.unwrap();
        KvStorageProvider::open(family, "t_flt.db").await.unwrap()
    }
    async fn reload(store: &Arc<dyn KvStore>, n: u64) -> Result<TableFilters, LoadReject> {
        let io = open(store).await;
        let mut bytes = vec![0u8; io.get_len().await as usize];
        let mut at = 0u64;
        io.read_data_into_buffer(&mut at, &mut bytes).await.unwrap();
        TableFilters::from_checkpoint(&bytes, &schema(), FilterConfig::default(), n)
    }

    let n0 = 20_000u64;
    let n1 = n0 + 100;
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let f = TableFilters::new(&schema(), FilterConfig::default());
    index(&f, 0..n0);
    {
        let io = open(&store).await;
        assert!(f.try_checkpoint(&io, n0).await.is_ok());
        f.mark_disk_dirty(&io).await;
        f.on_delete(proof, 7);
    }
    assert_eq!(reload(&store, n0).await.err(), Some(LoadReject::Dirty));
    {
        let io = open(&store).await;
        assert!(f.try_checkpoint(&io, n0).await.is_ok());
        index(&f, n0..n1);
        assert!(f.try_checkpoint(&io, n1).await.is_ok());
        assert!(f.deltas_since_base() > 0, "the second checkpoint was meant to be a delta");
    }
    let back = reload(&store, n1).await.expect("the KV sidecar did not reopen");
    assert_eq!(back.resume_slot(), n1);
    assert!(!finds(&back, 7, n1));
    for slot in [0, 8, n0 - 1, n0, n1 - 1] {
        assert!(finds(&back, slot, n1), "slot {slot} lost from the KV sidecar");
    }
}
