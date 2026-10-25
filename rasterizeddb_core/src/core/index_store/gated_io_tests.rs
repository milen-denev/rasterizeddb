//! `GatedIo` (slice B12): the verdicts on a provider's own writes, a write
//! parked inside the provider holding a seal off, the release slow path and
//! its failures through `failed_appends`, every family of a real table
//! registering on its word, DDL taking its ticket, and the allowlist.

use std::collections::BTreeSet;
use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc as StdArc;
use std::time::Duration;

use rclite::Arc;

use super::park;
use super::{Policy, UNGATED, Ungated};
use crate::core::database::Database;
use crate::core::index_store::census::{Census, Event};
use crate::core::index_store::file_gate::{self, FileGate};
use crate::core::index_store::gate;
use crate::core::index_store::gate_word::{GateState, PublishEnd, SealEnd, SealKind, SealStart};
use crate::core::index_store::marker::{self, LogState};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::file_sync::{LocalStorageProvider, failed_appends};
use crate::core::storage_providers::traits::StorageIO;

const INC: u128 = 0xB12;

type DbTable = Table<DbStorageProvider>;

/// What a spawned future's panic said, or `None` when it returned.
async fn panic_of<F: Future<Output = ()> + Send + 'static>(f: F) -> Option<String> {
    match tokio::spawn(f).await {
        Ok(()) => None,
        Err(e) if e.is_panic() => {
            let p = e.into_panic();
            Some(
                p.downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default(),
            )
        }
        Err(e) => panic!("the task did not finish: {e}"),
    }
}

fn loc(dir: &Path) -> String {
    dir.to_str().unwrap().to_string()
}

/// A file provider for `name` in `dir`, attached to `gate` under `policy`.
async fn provider(dir: &Path, name: &str, gate: &StdArc<FileGate>, policy: Policy) -> LocalStorageProvider {
    let p = LocalStorageProvider::new(&loc(dir), Some(name)).await;
    p.attach_write_gate(gate, policy);
    p
}

fn word_of(dir: &Path, stem: &str) -> StdArc<FileGate> {
    FileGate::open(Some(dir.to_path_buf()), stem, INC)
}

fn log_state(gate: &FileGate) -> LogState {
    marker::scan(&std::fs::read(gate.ixm_path().unwrap()).unwrap()).unwrap().state()
}

fn file_len(dir: &Path, name: &str) -> u64 {
    std::fs::metadata(dir.join(name)).map(|m| m.len()).unwrap_or(0)
}

/// One write through `site` of `p`.
async fn write_at(p: LocalStorageProvider, site: super::Site) {
    use super::Site;
    let bytes = [7u8; 16];
    match site {
        Site::WriteInPlace => {
            let txn = p.begin_txn();
            let _ = p.write_data_txn(txn, 0, &bytes).await;
        }
        Site::Append => {
            let txn = p.begin_txn();
            let _ = p.append_data_txn(txn, &bytes).await;
        }
        Site::Stage => p.append_data(&bytes, false).await,
        Site::WriteSeek => p.write_data_seek(SeekFrom::Start(0), &bytes).await,
        Site::Truncate => {
            let _ = p.truncate(0).await;
        }
        Site::Land => {
            let _ = p.commit_durable(0).await;
        }
        Site::SwapTemp => {
            let mut other = p.clone();
            p.swap_temp(&mut other).await;
        }
    }
}

const SITES: [super::Site; 7] = [
    super::Site::WriteInPlace,
    super::Site::Append,
    super::Site::Stage,
    super::Site::WriteSeek,
    super::Site::Truncate,
    super::Site::Land,
    super::Site::SwapTemp,
];

/// The backstop: every write entry point of a file provider, with no
/// ticket, on a `Clean` word, panics naming the site and the file, and
/// nothing reaches the file. `Unknown` likewise.
///
/// Mutation: `admit` skips the verdict on a word that backed out (`if
/// a.panics()` made `if false`, test verdicts kept): every site issues.
#[tokio::test(flavor = "multi_thread")]
async fn a_ticketless_write_on_a_clean_word_panics() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "bk");
    for site in SITES {
        let p = provider(dir.path(), "bk.db", &g, Policy::Backstop).await;
        // The setup writes while the word is Dirty.
        g.word().force_state_for_tests(GateState::Dirty);
        if site == super::Site::Land {
            // Staged bytes for the commit to land, while the word is Dirty.
            p.append_data(&[1u8; 8], false).await;
        }
        if site == super::Site::Truncate {
            p.append_data(&[1u8; 8], true).await;
        }
        let before = file_len(dir.path(), "bk.db");
        g.force_clean_for_tests();
        let msg = panic_of(write_at(p.clone(), site))
            .await
            .unwrap_or_else(|| panic!("{site:?}: a ticketless write on a Clean word was issued"));
        assert!(
            msg.contains(&format!("a {site:?} write")) && msg.contains("bk.db") && msg.contains("no ticket held"),
            "{site:?}: {msg}"
        );
        assert_eq!(file_len(dir.path(), "bk.db"), before, "{site:?}: bytes reached the file");
        assert_eq!(g.state(), GateState::Clean, "{site:?}: the verdict changed the word");
        assert_eq!(g.word().load().count(), 0, "{site:?}: the registration was not given back");
    }
    g.force_unknown_for_tests();
    let p = provider(dir.path(), "bk.db", &g, Policy::Backstop).await;
    let msg = panic_of(write_at(p, super::Site::Append)).await.expect("Unknown: issued");
    assert!(msg.contains("Unknown"), "{msg}");
}

/// The same write under a ticket: the ticket makes `DIRTY` durable first,
/// and the write registers on the word, in that order in the census.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_under_a_ticket_registers_after_the_durable_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "tk");
    let p = provider(dir.path(), "tk.db", &g, Policy::Backstop).await;
    g.force_clean_for_tests();
    let census = Census::start();
    let ticket = gate::ticket(&g, None).await.unwrap();
    p.append_data(&[3u8; 32], true).await;
    drop(ticket);
    let events = census.finish();
    assert_eq!(file_len(dir.path(), "tk.db"), 32);
    assert_eq!(log_state(&g), LogState::Dirty);
    let file = dir.path().join("tk.db");
    let id = g.census_id();
    let dirty = events
        .iter()
        .position(|e| matches!(e, Event::DirtyDurable { gate } if *gate == id))
        .expect("DIRTY made durable");
    let write = events
        .iter()
        .position(|e| matches!(e, Event::Write { file: f, gate: Some(g), .. } if *f == file && *g == id))
        .expect("the write registered on the word");
    assert!(dirty < write, "the write was registered before DIRTY was durable");
}

/// A registration is not a ticket: the census (B11's V3 check) still sees a
/// write with no ticket held as one, though the write registered on the
/// word. Otherwise every write would vouch for itself and the census would
/// pass vacuously.
///
/// Mutation: `register_write` records a census hold for its count.
#[tokio::test(flavor = "multi_thread")]
async fn the_census_does_not_count_a_registration_as_a_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "cn");
    let p = provider(dir.path(), "cn.db", &g, Policy::Backstop).await;
    let file = dir.path().join("cn.db");
    let census = Census::start();
    // Dirty and unsealed: layer 2 issues it without a verdict.
    p.append_data(&[4u8; 8], true).await;
    let events = census.finish();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Write { file: f, gate: Some(_), .. } if *f == file)),
        "the write did not register"
    );
    let files = [file.clone()];
    let verdict = crate::core::index_store::census::check(
        &events,
        &crate::core::index_store::census::Scope {
            gate: g.census_id(),
            began_clean: false,
            structures: &[],
            files: &files,
            exempt: &[],
        },
    );
    assert!(
        verdict.as_ref().is_err_and(|e| e.contains("no ticket held")),
        "a registered write passed the census as if a ticket were held: {verdict:?}"
    );
}

/// `ticketless_write_publishing`: a write while a seal publishes its
/// `CLEAN`. No ticket can be held then, so tests panic; release waits for
/// the window to end and then, the `CLEAN` published, takes the slow path:
/// `DIRTY` durable before the write is issued.
///
/// Mutation: release issues on a `PUBLISHING` word (`admit`'s back-out arm
/// returns `Registration::Ungated` instead of the slow path): the bytes land
/// while the seal publishes, and the log ends `CLEAN` over them.
#[tokio::test(flavor = "multi_thread")]
async fn ticketless_write_publishing() {
    let dir = tempfile::tempdir().unwrap();
    // Tests: a panic.
    let g = word_of(dir.path(), "pa");
    let p = provider(dir.path(), "pa.db", &g, Policy::Backstop).await;
    assert_eq!(g.word().seal(SealKind::Optional), SealStart::Sealed { outstanding: 0 });
    assert_eq!(g.word().try_publish(), PublishEnd::Publishing);
    let msg = panic_of(write_at(p, super::Site::Append)).await.expect("issued while publishing");
    assert!(msg.contains("publishing"), "{msg}");
    g.word().unseal(SealEnd::Abandoned);

    // Release: waits, then the slow path.
    let g = word_of(dir.path(), "pb");
    g.set_release_verdicts_for_tests(true);
    let p = provider(dir.path(), "pb.db", &g, Policy::Backstop).await;
    assert_eq!(g.word().seal(SealKind::Optional), SealStart::Sealed { outstanding: 0 });
    assert_eq!(g.word().try_publish(), PublishEnd::Publishing);
    let writer = tokio::spawn(write_at(p, super::Site::Append));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(file_len(dir.path(), "pb.db"), 0, "a write was issued during PUBLISHING");
    assert!(!writer.is_finished());
    // The sealer's CLEAN, durable, then the unseal.
    let mut log = marker::new_log(INC);
    log.extend_from_slice(&marker::encode_record(
        1,
        &marker::MarkerRecord::Clean(file_gate::dummy_clean(INC)),
    ));
    std::fs::write(g.ixm_path().unwrap(), &log).unwrap();
    assert_eq!(g.word().unseal(SealEnd::Published), GateState::Clean);
    tokio::time::timeout(Duration::from_secs(10), writer)
        .await
        .expect("the write never went ahead")
        .unwrap();
    assert_eq!(file_len(dir.path(), "pb.db"), 16);
    assert_eq!(log_state(&g), LogState::Dirty, "the write went ahead with the log ending CLEAN");
    assert_eq!(g.state(), GateState::Dirty);
}

/// `ticketless_write_parked_in_provider`: a write registered and parked
/// inside the provider, not yet issued, keeps a seal from publishing (its
/// count is on the word) and a mandatory seal's drain waiting, until it
/// returns (RV15).
///
/// Mutation: "check with a load": `admit` reads the word instead of
/// registering (`register_write` replaced by a `load` that returns
/// `Registration::Ungated` on a Dirty word); the seal finds no count and
/// publishes while the write is parked.
#[tokio::test(flavor = "multi_thread")]
async fn ticketless_write_parked_in_provider() {
    let dir = tempfile::tempdir().unwrap();
    for kind in [SealKind::Optional, SealKind::Mandatory] {
        let stem = format!("pk{}", kind as u8);
        let name = format!("{stem}.db");
        let g = word_of(dir.path(), &stem);
        let p = provider(dir.path(), &name, &g, Policy::Backstop).await;
        let parked = park::arm(&name);
        let writer = tokio::spawn(write_at(p, super::Site::Append));
        tokio::time::timeout(Duration::from_secs(10), parked.reached.notified())
            .await
            .expect("the write never reached the park");
        assert_eq!(g.word().seal(kind), SealStart::Sealed { outstanding: 1 }, "{kind:?}");
        assert_eq!(
            g.word().try_publish(),
            PublishEnd::Busy { count: 1 },
            "{kind:?}: a seal published over a write parked in the provider"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), g.word().drained()).await.is_err(),
            "{kind:?}: the drain finished with a write in flight"
        );
        parked.release.notify_one();
        writer.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), g.word().drained())
            .await
            .expect("the drain never finished");
        assert_eq!(g.word().try_publish(), PublishEnd::Publishing, "{kind:?}");
        g.word().unseal(SealEnd::Abandoned);
    }
}

/// `held_writer_during_cancelled_seal`: a writer whose ticket preempted an
/// optional seal writes while the abandoning sealer still has `SEALED` set:
/// issued, no panic (P8), and the seal stays unable to publish.
///
/// Mutation: "panic on SEALED with CANCEL": `admit` treats any sealed word
/// as ticketless; the held writer panics.
#[tokio::test(flavor = "multi_thread")]
async fn held_writer_during_cancelled_seal() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "hw");
    let p = provider(dir.path(), "hw.db", &g, Policy::Backstop).await;
    assert_eq!(g.word().seal(SealKind::Optional), SealStart::Sealed { outstanding: 0 });
    let ticket = gate::ticket(&g, None).await.unwrap();
    let w = g.word().load();
    assert!(w.sealed() && w.cancelled() && !w.publishing(), "{w:?}");
    let q = p.clone();
    assert_eq!(panic_of(write_at(q, super::Site::Append)).await, None, "the held writer panicked");
    assert_eq!(file_len(dir.path(), "hw.db"), 16);
    assert_eq!(g.word().try_publish(), PublishEnd::Cancelled);
    drop(ticket);
    g.word().unseal(SealEnd::Abandoned);
}

/// A ticketless write during an unpublished seal, with no count on the
/// word: tests panic; release cancels the seal by compare-and-swap, mandatory
/// ones included, so it can never publish over the write (P7).
///
/// Mutation: "a sealed write only marks the seal broken": `IoOp::Cancel`'s
/// compare-and-swap leaves `CANCEL` clear; the seal publishes.
#[tokio::test(flavor = "multi_thread")]
async fn a_ticketless_write_during_an_unpublished_seal_cancels_it() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "us");
    let p = provider(dir.path(), "us.db", &g, Policy::Backstop).await;
    assert_eq!(g.word().seal(SealKind::Optional), SealStart::Sealed { outstanding: 0 });
    let msg = panic_of(write_at(p, super::Site::Append)).await.expect("a ticketless write issued");
    assert!(msg.contains("unpublished seal"), "{msg}");
    g.word().unseal(SealEnd::Abandoned);

    for kind in [SealKind::Optional, SealKind::Mandatory] {
        let stem = format!("ur{}", kind as u8);
        let g = word_of(dir.path(), &stem);
        g.set_release_verdicts_for_tests(true);
        let p = provider(dir.path(), &format!("{stem}.db"), &g, Policy::Backstop).await;
        assert_eq!(g.word().seal(kind), SealStart::Sealed { outstanding: 0 });
        write_at(p, super::Site::Append).await;
        assert_eq!(file_len(dir.path(), &format!("{stem}.db")), 16);
        assert_eq!(g.word().try_publish(), PublishEnd::Cancelled, "{kind:?}: the seal can still publish");
        g.word().unseal(SealEnd::Abandoned);
    }
}

/// Release on a `Clean` word: the slow path inline, `DIRTY` durable before
/// the write is issued.
///
/// Mutation: release issues on a word that backed out (`admit`'s slow arm
/// returns `Registration::Ungated`): the write lands with the log ending
/// `CLEAN`.
#[tokio::test(flavor = "multi_thread")]
async fn release_takes_the_slow_path_before_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let g = word_of(dir.path(), "rs");
    g.set_release_verdicts_for_tests(true);
    let p = provider(dir.path(), "rs.db", &g, Policy::Backstop).await;
    g.force_clean_for_tests();
    let census = Census::start();
    write_at(p.clone(), super::Site::WriteSeek).await;
    let events = census.finish();
    assert_eq!(file_len(dir.path(), "rs.db"), 16);
    assert_eq!(log_state(&g), LogState::Dirty);
    let id = g.census_id();
    let file = dir.path().join("rs.db");
    let dirty = events
        .iter()
        .position(|e| matches!(e, Event::DirtyDurable { gate } if *gate == id))
        .expect("no DIRTY");
    let write = events
        .iter()
        .position(|e| matches!(e, Event::Write { file: f, .. } if *f == file))
        .expect("no write");
    assert!(dirty < write);
}

/// A release slow path that fails (no durable `DIRTY`, no unlinked `.ixs`)
/// skips the write and fails the statement: a write in a transaction fails
/// that transaction's commit through **another** file of the directory (an
/// `INSERT` writes its row and commits through the pointers), and a write
/// with no transaction fails the next commit in the directory, once.
///
/// Mutations: `failed_appends` keys a transaction's failure by its file
/// again (the pointers' commit succeeds over a skipped row write); the gate
/// failure is recorded under the file's `STAGED` key (no commit reports it).
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_slow_path_skips_the_write_and_fails_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let location = loc(dir.path());
    let wal = std::sync::Arc::new(crate::core::wal::Wal::open(dir.path().join("rastdb.wal")).unwrap());
    crate::core::wal::install_for(&location, wal);
    let g = word_of(dir.path(), "fs");
    g.set_release_verdicts_for_tests(true);
    let rows = provider(dir.path(), "fs.db", &g, Policy::Backstop).await;
    let pointers = provider(dir.path(), "fs_pointers.db", &g, Policy::Backstop).await;

    // In a transaction.
    g.force_clean_for_tests();
    g.fail_next_slow_path_for_tests();
    let txn = rows.begin_txn();
    assert_ne!(txn, 0, "the directory has a log");
    let (_, _) = rows.append_data_txn(txn, &[5u8; 64]).await;
    assert_eq!(file_len(dir.path(), "fs.db"), 0, "a skipped write reached the file");
    let (_, token) = pointers.append_data_txn(txn, &[6u8; 16]).await;
    let committed = pointers.commit_durable(token).await;
    assert!(committed.is_err(), "a transaction whose row write was skipped committed");
    assert!(
        committed.unwrap_err().to_string().contains("injected slow-path failure"),
        "the commit reported another failure"
    );

    // With no transaction.
    g.force_clean_for_tests();
    g.fail_next_slow_path_for_tests();
    rows.write_data_seek(SeekFrom::Start(0), &[9u8; 8]).await;
    assert_eq!(file_len(dir.path(), "fs.db"), 0, "a skipped write reached the file");
    let unrelated = pointers.begin_txn();
    assert!(
        pointers.commit_durable(unrelated).await.is_err(),
        "the next commit in the directory did not report the skipped write"
    );
    let next = pointers.begin_txn();
    assert!(pointers.commit_durable(next).await.is_ok(), "the failure was reported twice");
    assert_eq!(failed_appends::pending_in(&location), 0);
    crate::core::wal::remove_for(&location);
}

/// A row append that fails part-way fails its transaction's commit through
/// the pointers. Keyed by file, the failure sat where no commit looked: the
/// pointer committed, naming bytes that had been cut back off, and the next
/// append landed on them (the pre-B12 behaviour; shown failing there by the
/// keying mutation below). An aborted transaction forgets its failures.
///
/// Mutation: `txn_key` keys by file whatever the transaction; the commit
/// succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_row_append_fails_the_commit_through_the_pointers() {
    let dir = tempfile::tempdir().unwrap();
    let location = loc(dir.path());
    let wal = std::sync::Arc::new(crate::core::wal::Wal::open(dir.path().join("rastdb.wal")).unwrap());
    crate::core::wal::install_for(&location, wal);
    let rows = LocalStorageProvider::new(&location, Some("ra.db")).await;
    let pointers = LocalStorageProvider::new(&location, Some("ra_pointers.db")).await;

    let txn = rows.begin_txn();
    failed_appends::SHORT_APPEND.with(|h| h.set(Some(10)));
    let (_, _) = rows.append_data_txn(txn, &[1u8; 64]).await;
    failed_appends::SHORT_APPEND.with(|h| h.set(None));
    let (_, token) = pointers.append_data_txn(txn, &[2u8; 16]).await;
    assert!(
        pointers.commit_durable(token).await.is_err(),
        "a pointer committed over a row append that failed"
    );

    // Aborted instead of committed: nothing is left recorded.
    let txn = rows.begin_txn();
    failed_appends::SHORT_APPEND.with(|h| h.set(Some(10)));
    let (_, _) = rows.append_data_txn(txn, &[1u8; 64]).await;
    failed_appends::SHORT_APPEND.with(|h| h.set(None));
    assert_eq!(failed_appends::pending_in(&location), 1);
    pointers.abort_txn(txn).await;
    assert_eq!(failed_appends::pending_in(&location), 0, "an aborted transaction left its failure");
    crate::core::wal::remove_for(&location);
}

// ---------------------------------------------------------------------------
// A real table
// ---------------------------------------------------------------------------

async fn open(dir: &Path, name: &str) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(
        Database::try_new_named(&dir.to_string_lossy(), false, name)
            .await
            .expect("open the database"),
    )
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        Err(String::from_utf8_lossy(&out).into_owned())
    } else {
        Ok(out)
    }
}

async fn exec(db: &Arc<Database>, sql: &str) {
    if let Err(e) = try_exec(db, sql).await {
        panic!("{sql}: {e}");
    }
}

fn table(db: &Database, name: &str) -> Arc<DbTable> {
    db.table_for_tests(name).expect("the table")
}

async fn seeded(db: &Arc<Database>, name: &str, rows: i64) -> Arc<DbTable> {
    exec(db, &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)")).await;
    if rows > 0 {
        let values: Vec<String> = (0..rows).map(|i| format!("({i}, {}, 's{i}')", i * 10)).collect();
        exec(db, &format!("INSERT INTO {name} (id, v, s) VALUES {}", values.join(", "))).await;
    }
    let t = table(db, name);
    t.wait_filters_ready().await.unwrap();
    t
}

/// Which family a test writes.
#[derive(Debug, Clone, Copy)]
enum Family {
    Data,
    Pointers,
    Schema,
    Sequence,
    Sidecar,
}

/// A ticketless write to one family of a table whose word is `Clean`
/// panics naming that family's file: its provider registers on the table's
/// word.
async fn family_registers(family: Family) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "families").await;
    let t = seeded(&db, "fam", 5).await;
    t.gate().force_clean_for_tests();
    let (file, write): (&str, std::pin::Pin<Box<dyn Future<Output = ()> + Send>>) = {
        let d = t.data().unwrap();
        match family {
            Family::Data => {
                let io = (*d.io_rows).clone();
                ("fam.db", Box::pin(async move { write_ticketless(io).await }))
            }
            Family::Pointers => {
                let io = (*d.io_pointers).clone();
                ("fam_pointers.db", Box::pin(async move { write_ticketless(io).await }))
            }
            Family::Schema => {
                let io = (*t.io_schema).clone();
                ("fam_schema.db", Box::pin(async move { write_ticketless(io).await }))
            }
            Family::Sequence => {
                let io = (*d.ids.io).clone();
                ("fam_seq.db", Box::pin(async move { write_ticketless(io).await }))
            }
            Family::Sidecar => {
                // Through the real sidecar writer: a provider opened from
                // `_schema.db` for `_schema_CHK.db`.
                let io_schema = t.io_schema.clone();
                (
                    "fam_schema_CHK.db",
                    Box::pin(async move {
                        let entry = crate::core::rql::check::CheckConstraint {
                            name: "fam_ck".into(),
                            expression: "v >= 0".into(),
                            is_deleted: false,
                        };
                        let _ = crate::core::rql::check::append(io_schema, &entry).await;
                    }),
                )
            }
        }
    };
    let msg = panic_of(write).await.unwrap_or_else(|| panic!("{family:?}: a ticketless write issued"));
    assert!(
        msg.contains(file) && msg.contains("was issued with no ticket held"),
        "{family:?}: {msg}"
    );
    assert_eq!(t.gate().state(), GateState::Clean);
    drop(t);
    close(dir.path(), db);
}

/// An empty append in a transaction of its own, aborted after.
async fn write_ticketless(io: DbStorageProvider) {
    let txn = io.begin_txn();
    let _ = io.append_data_txn(txn, &[]).await;
    io.abort_txn(txn).await;
}

/// Mutation: the rows provider is not attached in `assemble`.
#[tokio::test(flavor = "multi_thread")]
async fn the_data_family_registers() {
    family_registers(Family::Data).await;
}

/// Mutation: the pointers provider is not attached in `assemble`.
#[tokio::test(flavor = "multi_thread")]
async fn the_pointer_family_registers() {
    family_registers(Family::Pointers).await;
}

/// Mutation: `_schema.db`'s provider is not attached in `assemble`.
#[tokio::test(flavor = "multi_thread")]
async fn the_schema_family_registers() {
    family_registers(Family::Schema).await;
}

/// Mutation: `_seq.db`'s provider is not attached in `assemble`.
#[tokio::test(flavor = "multi_thread")]
async fn the_sequence_family_registers() {
    family_registers(Family::Sequence).await;
}

/// Mutation: `create_new` does not pass the word on (`inherit_from` gone).
#[tokio::test(flavor = "multi_thread")]
async fn the_sidecar_family_registers() {
    family_registers(Family::Sidecar).await;
}

/// `_flt.db` registers too, under its exemption: its checkpoint loop writes
/// under no ticket, so a write on a `Clean` word takes the release slow path
/// (`DIRTY` durable first) instead of panicking.
///
/// Mutation: the filter provider is not attached in `assemble` (its write
/// then reaches the live, `Clean` file set around the word, and panics).
#[tokio::test(flavor = "multi_thread")]
async fn the_filter_family_registers() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "families").await;
    let t = seeded(&db, "flt", 5).await;
    t.gate().force_clean_for_tests();
    let census = Census::start();
    let tt = t.clone();
    let panicked = panic_of(async move {
        tt.checkpoint_filters().await.unwrap();
    })
    .await;
    let events = census.finish();
    assert_eq!(panicked, None, "the filter checkpoint panicked");
    let file = dir.path().join("flt_flt.db");
    let id = t.gate().census_id();
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::Write { file: f, gate: Some(g), ungated: Some(Ungated::FilterCheckpoint), .. }
                if *f == file && *g == id
        )),
        "the checkpoint's write did not register on the table's word"
    );
    assert_eq!(t.gate().state(), GateState::Dirty);
    assert_eq!(log_state(t.gate()), LogState::Dirty);
    drop(t);
    close(dir.path(), db);
}

/// A DDL that writes a table's schema family takes its ticket first: on a
/// `Clean` table every arm below runs without the backstop firing, and
/// leaves the word `Dirty`, its `DIRTY` durable.
///
/// Mutation: neither ACCESS EXCLUSIVE nor the reload takes a ticket
/// (`ticket_exclusive` returns at once, `reload_with_instance`'s
/// `gate::ticket` removed): the first arm's sidecar write panics. Either one
/// alone covers these arms.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_on_a_clean_table_takes_its_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "ddl_clean").await;
    seeded(&db, "dd", 5).await;
    exec(&db, "CREATE TABLE de (id BIGINT PRIMARY KEY, v BIGINT)").await;
    for (name, sql) in [
        ("dd", "CREATE INDEX dd_v ON dd (v)"),
        ("dd", "DROP INDEX dd_v"),
        ("dd", "ALTER TABLE dd ADD CONSTRAINT dd_ck CHECK (v >= 0)"),
        ("dd", "ALTER TABLE dd ALTER COLUMN v SET DEFAULT 7"),
        ("dd", "ALTER TABLE dd RENAME COLUMN s TO s2"),
        ("dd", "ALTER TABLE dd ADD CONSTRAINT dd_uq UNIQUE (v)"),
        ("dd", "ALTER TABLE dd ALTER COLUMN v SET NOT NULL"),
        ("dd", "ALTER TABLE dd ADD COLUMN w BIGINT DEFAULT 1"),
        ("dd", "ALTER TABLE dd DROP COLUMN w"),
        ("de", "ALTER TABLE de ADD COLUMN w BIGINT"),
        ("de", "ALTER TABLE de DROP COLUMN w"),
    ] {
        let t = table(&db, name);
        t.gate().force_clean_for_tests();
        let gate = t.gate().clone();
        drop(t);
        let dbc = db.clone();
        let sql_owned = sql.to_string();
        let out = panic_of(async move {
            exec(&dbc, &sql_owned).await;
        })
        .await;
        assert_eq!(out, None, "{sql}: the backstop fired");
        let after = table(&db, name);
        assert_eq!(after.gate().state(), GateState::Dirty, "{sql}: the word stayed Clean");
        if StdArc::ptr_eq(after.gate(), &gate) {
            assert_eq!(log_state(&gate), LogState::Dirty, "{sql}: DIRTY is not durable");
        }
    }
    close(dir.path(), db);
}

/// A reload outside every statement (no owner, so no family and no ACCESS
/// EXCLUSIVE ticket: start-up, tools, tests below the statement layer)
/// takes its own ticket for its work: the sidecar write on a `Clean` table
/// does not fire the backstop.
///
/// Mutation: `reload_with_instance` takes no ticket.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_outside_every_statement_takes_its_own_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "reload_bare").await;
    seeded(&db, "rb", 3).await;
    let t = table(&db, "rb");
    t.gate().force_clean_for_tests();
    let gate = t.gate().clone();
    drop(t);
    let dbc = db.clone();
    let out = panic_of(async move {
        dbc.reload_with("rb", |old| async move {
            let entry = crate::core::rql::check::CheckConstraint {
                name: "rb_ck".into(),
                expression: "v >= 0".into(),
                is_deleted: false,
            };
            crate::core::rql::check::append(old.io_schema.clone(), &entry)
                .await
                .map_err(|e| e.to_string())
        })
        .await
        .unwrap();
    })
    .await;
    assert_eq!(out, None, "the reload's sidecar write fired the backstop");
    assert_eq!(gate.state(), GateState::Dirty);
    assert_eq!(log_state(&gate), LogState::Dirty);
    close(dir.path(), db);
}

/// A DDL on a parent that rewrites its foreign-key children's sidecars
/// (held under ACCESS EXCLUSIVE with the parent, finding L11) tickets each
/// child it writes: on a `Clean` child the backstop does not fire, and the
/// child's word is `Dirty` after, its `DIRTY` durable.
///
/// Mutation: ACCESS EXCLUSIVE takes no ticket (`ticket_exclusive` returns at
/// once): the child's FK sidecar write panics (the reload tickets only the
/// parent).
#[tokio::test(flavor = "multi_thread")]
async fn ddl_writing_a_child_s_sidecars_tickets_the_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "ddl_children").await;
    exec(&db, "CREATE TABLE fp (id BIGINT PRIMARY KEY, v BIGINT)").await;
    exec(&db, "CREATE TABLE fc (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES fp(id))").await;
    exec(&db, "INSERT INTO fp (id, v) VALUES (1, 1)").await;
    exec(&db, "INSERT INTO fc (id, pid) VALUES (1, 1)").await;
    for (parent, sql) in [
        ("fp", "ALTER TABLE fp RENAME COLUMN id TO id2"),
        ("fp2", "ALTER TABLE fp RENAME TO fp2"),
    ] {
        let child = table(&db, "fc");
        child.gate().force_clean_for_tests();
        let gate = child.gate().clone();
        drop(child);
        let dbc = db.clone();
        let sql_owned = sql.to_string();
        let out = panic_of(async move {
            exec(&dbc, &sql_owned).await;
        })
        .await;
        assert_eq!(out, None, "{sql}: the backstop fired");
        let _ = parent;
        let after = table(&db, "fc");
        assert_eq!(after.gate().state(), GateState::Dirty, "{sql}: the child's word stayed Clean");
        if StdArc::ptr_eq(after.gate(), &gate) {
            assert_eq!(log_state(&gate), LogState::Dirty, "{sql}: the child's DIRTY is not durable");
        }
    }
    close(dir.path(), db);
}

/// The allowlist, exactly: over a workload of every family (DML, DDL, a
/// rewrite, VACUUM and VACUUM FULL, the filter checkpoint, a partitioned
/// table, a provider opening over staged bytes), every write that reaches a
/// live file set without registering on its word runs inside a listed
/// [`UNGATED`] entry, and every listed entry is reached.
///
/// Mutations: the rewrite runs outside its `ungated` scope (an unlisted
/// write); an entry dropped from `UNGATED` (reached but not listed).
#[tokio::test(flavor = "multi_thread")]
async fn ungated_writes_are_exactly_the_listed_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "allowlist").await;
    let census = Census::start();
    exec(&db, "CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 3 CHECK (v >= 0), s TEXT UNIQUE)").await;
    exec(&db, "INSERT INTO a (id, v, s) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')").await;
    exec(&db, "UPDATE a SET v = 11 WHERE id = 1").await;
    exec(&db, "UPDATE a SET s = 'yy' WHERE v > 15").await;
    exec(&db, "DELETE FROM a WHERE id = 3").await;
    exec(&db, "CREATE INDEX a_v ON a (v)").await;
    exec(&db, "ALTER TABLE a ADD CONSTRAINT a_ck CHECK (id > 0)").await;
    exec(&db, "ALTER TABLE a RENAME COLUMN s TO s2").await;
    exec(&db, "ALTER TABLE a ADD COLUMN w BIGINT DEFAULT 5").await;
    exec(&db, "VACUUM a").await;
    exec(&db, "VACUUM FULL a").await;
    exec(&db, "INSERT INTO a (id, v, s2) VALUES (4, 40, 'q')").await;
    let t = table(&db, "a");
    t.checkpoint_filters().await.unwrap();
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT, v BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 2",
    )
    .await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)").await;
    exec(&db, "UPDATE pt SET v = 9 WHERE tenant_id = 2").await;
    // A provider of the live file set opening over staged bytes: a sidecar
    // the table does not use, with bytes left in its `.tmp`.
    std::fs::write(dir.path().join("a_schema_PIX.db.tmp"), [0u8; 8]).unwrap();
    let _opened = t.io_schema.create_new("a_schema_PIX.db".to_string()).await;
    let events = census.finish();

    let mut reached = BTreeSet::new();
    let mut unlisted = Vec::new();
    for e in &events {
        let Event::Write { file, gate, live, ungated } = e else {
            continue;
        };
        if !file.starts_with(dir.path()) {
            continue;
        }
        if let Some(why) = ungated {
            reached.insert(format!("{why:?}"));
        }
        if gate.is_none() && live.is_some() && ungated.is_none() {
            unlisted.push(file.clone());
        }
    }
    assert!(
        unlisted.is_empty(),
        "writes reached a live file set around its word outside every listed exemption: {unlisted:?}"
    );
    let listed: BTreeSet<String> = UNGATED.iter().map(|(why, _)| format!("{why:?}")).collect();
    assert_eq!(reached, listed, "the exemptions reached are not exactly the listed ones");
    drop(t);
    close(dir.path(), db);
}

/// Every listed exemption states its reason.
#[test]
fn every_exemption_has_a_reason() {
    let mut seen = BTreeSet::new();
    for (why, reason) in UNGATED {
        assert!(reason.len() > 80, "{why:?}: no reason given");
        assert!(seen.insert(format!("{why:?}")), "{why:?} listed twice");
    }
}
