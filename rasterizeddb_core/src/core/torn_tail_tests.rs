//! A fixed-record file torn by a power cut: cut back before replay, never
//! appended after, and never cut below its durable floor.

use super::*;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Result<Arc<Database>, String> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Database::try_new_named(&path_str(dir), false, "torn").await.map(Arc::new)
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

/// Every row of `select`, as column bytes, sorted.
async fn rows(db: &Arc<Database>, select: &str) -> Result<Vec<Vec<Vec<u8>>>, String> {
    let out = exec(db, select).await?;
    if out.first() != Some(&2) {
        return Err(format!("not a row set: {}", String::from_utf8_lossy(&out)));
    }
    let mut rows: Vec<Vec<Vec<u8>>> = crate::core::row::row::vec_into_rows(&out[1..])
        .map_err(|e| e.to_string())?
        .iter()
        .map(|r| r.columns.iter().map(|c| c.data.into_slice().to_vec()).collect())
        .collect();
    rows.sort();
    Ok(rows)
}

/// The `id`s of table `t`, sorted.
async fn ids(db: &Arc<Database>) -> Vec<i64> {
    let mut out: Vec<i64> = rows(db, "SELECT id FROM t")
        .await
        .unwrap()
        .iter()
        .map(|r| i64::from_le_bytes(r[0].as_slice().try_into().unwrap()))
        .collect();
    out.sort_unstable();
    out
}

fn checkpoint(dir: &Path) {
    let wal = crate::core::wal::wal_for(&path_str(dir)).expect("the database has a log");
    assert!(wal.checkpoint().unwrap(), "the checkpoint declined");
}

/// Copies the database in `from` to `to` as a fresh directory, with the
/// storage pool file's absolute paths pointed at it, as a crash image is.
fn copy_db(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
    let pool = to.join("db_storage.cfg");
    if let Ok(text) = std::fs::read_to_string(&pool) {
        std::fs::write(&pool, text.replace(&path_str(from), &path_str(to))).unwrap();
    }
}

fn len(p: &Path) -> u64 {
    std::fs::metadata(p).unwrap().len()
}

// ---------------------------------------------------------------------------
// The helper itself
// ---------------------------------------------------------------------------

/// The strides the module documentation lists, so a record-size change shows
/// up here and there together.
#[test]
#[cfg(not(feature = "enable_long_row"))]
fn the_strides_are_the_documented_ones() {
    let got: Vec<(&str, u64)> = FIXED_RECORD_SUFFIXES.to_vec();
    assert_eq!(
        got,
        vec![
            ("_pointers.db", 37),
            ("_schema_FIELDS.db", 153),
            ("_schema_FK.db", 519),
            ("_schema_DEF.db", 387),
            ("_schema_JSON.db", 259),
            ("_schema_PIX.db", 259),
            ("_schema_IDX.db", 197),
            ("_schema_UK.db", 1163),
            ("_schema_TYP.db", 134),
            ("_schema_GEN.db", 643),
            ("_schema_CHK.db", 643),
        ]
    );
}

#[test]
fn a_torn_tail_of_every_length_is_cut_to_the_last_whole_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f");
    for (header, stride) in [(0u64, 37u64), (7, 37), (0, 5), (11, 1163)] {
        let whole = header + 3 * stride;
        // Exactly whole: nothing to do.
        std::fs::write(&path, vec![0xA5; whole as usize]).unwrap();
        assert_eq!(truncate_to_whole_records(&path, header, stride, whole).unwrap(), Truncated::Whole);
        for torn in 1..stride {
            std::fs::write(&path, vec![0xA5; (whole + torn) as usize]).unwrap();
            let done = truncate_to_whole_records(&path, header, stride, whole).unwrap();
            assert_eq!(done, Truncated::Cut { from: whole + torn, to: whole }, "header {header}, stride {stride}, torn {torn}");
            assert_eq!(done.bytes(), torn);
            assert_eq!(len(&path), whole);
        }
    }
    // Shorter than its header, or missing: left alone.
    std::fs::write(&path, [1, 2, 3]).unwrap();
    assert_eq!(truncate_to_whole_records(&path, 7, 37, 0).unwrap(), Truncated::Whole);
    assert_eq!(len(&path), 3);
    assert_eq!(
        truncate_to_whole_records(&dir.path().join("missing"), 0, 37, 0).unwrap(),
        Truncated::Whole
    );
}

#[test]
fn a_cut_below_the_durable_floor_is_refused_and_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t_pointers.db");
    std::fs::write(&path, vec![0x11; 8 * 37 + 3]).unwrap();
    let err = truncate_to_whole_records(&path, 0, 37, 10 * 37).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let msg = err.to_string();
    for needle in ["t_pointers.db", "299 bytes", "37 bytes", "370 bytes", "ends at 296"] {
        assert!(msg.contains(needle), "the refusal does not say {needle:?}: {msg}");
    }
    assert_eq!(len(&path), 299, "a refused cut changed the file");
    // At the floor exactly, the cut goes ahead.
    assert_eq!(
        truncate_to_whole_records(&path, 0, 37, 8 * 37).unwrap(),
        Truncated::Cut { from: 299, to: 296 }
    );
}

#[test]
fn only_a_tables_own_sidecars_are_fixed_record_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    for name in ["a_pointers.db", "a_schema_UK.db", "x_pointers.db", "x_pointers_pointers.db", "a.db"] {
        std::fs::write(d.join(name), b"").unwrap();
    }
    // No schema file yet: nothing is anyone's sidecar.
    assert_eq!(classify(d, "a_pointers.db"), None);
    std::fs::write(d.join("a_schema.db"), b"").unwrap();
    // Table `x_pointers`: its row data file is not a pointer file.
    std::fs::write(d.join("x_pointers_schema.db"), b"").unwrap();
    assert_eq!(classify(d, "a_pointers.db").map(|k| k.stride), Some(ROW_POINTER_RECORD_LEN as u64));
    assert_eq!(classify(d, "a_schema_UK.db").map(|k| k.stride), Some(crate::core::rql::unique_key::UNIQUE_KEY_SIZE));
    assert_eq!(classify(d, "x_pointers.db"), None, "table x_pointers' rows taken for a pointer file");
    assert!(classify(d, "x_pointers_pointers.db").is_some());
    assert_eq!(classify(d, "a.db"), None);
    assert_eq!(classify(d, "a_schema.db"), None);
    assert_eq!(classify(d, "_pointers.db"), None);
}

#[test]
fn floors_round_trip_and_a_damaged_file_is_no_floors() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("rasterized.wal");
    let at = Floors::path_for(&wal);
    assert_eq!(at.file_name().unwrap(), "rasterized.wal.floors");
    std::fs::write(dir.path().join("t_schema.db"), b"").unwrap();
    let file = dir.path().join("t_pointers.db");
    std::fs::write(&file, vec![0; 37 * 4]).unwrap();
    let meta = std::fs::metadata(&file).unwrap();
    let mut floors = Floors::default();
    assert!(floors.set(FileIdentity::of(&meta), "t pointers with spaces.db", 37 * 4));
    assert!(!floors.set(FileIdentity::of(&meta), "t pointers with spaces.db", 37 * 4));
    floors.store(&at).unwrap();
    let back = Floors::load(&at);
    assert_eq!(back, floors);
    assert_eq!(back.floor_of(&meta), 37 * 4);
    // Lowered, never raised by `lower`.
    let mut lowered = back.clone();
    assert!(!lowered.lower(FileIdentity::of(&meta), 37 * 9));
    assert!(lowered.lower(FileIdentity::of(&meta), 37));
    assert_eq!(lowered.floor_of(&meta), 37);
    // Reconcile clamps to what the file holds and forgets what is gone.
    std::fs::write(&file, vec![0; 37 * 2 + 5]).unwrap();
    let mut clamped = back.clone();
    assert!(clamped.reconcile(dir.path()));
    assert_eq!(clamped.floor_of(&std::fs::metadata(&file).unwrap()), 37 * 2);
    std::fs::remove_file(&file).unwrap();
    assert!(clamped.reconcile(dir.path()));
    assert!(clamped.is_empty());
    // A torn or edited floors file is ignored rather than trusted.
    let text = std::fs::read_to_string(&at).unwrap();
    std::fs::write(&at, text.replace(&format!(" {} ", 37 * 4), &format!(" {} ", 37 * 40))).unwrap();
    assert!(Floors::load(&at).is_empty());
    std::fs::write(&at, &text[..text.len() / 2]).unwrap();
    assert!(Floors::load(&at).is_empty());
}

// ---------------------------------------------------------------------------
// Through the engine, one file kind at a time
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Kind {
    Pointers,
    Fields,
    Fk,
    Def,
    Json,
    Pix,
    Idx,
    Uk,
    Typ,
    Gen,
    Chk,
}

impl Kind {
    fn suffix(self) -> &'static str {
        match self {
            Kind::Pointers => "_pointers.db",
            Kind::Fields => "_schema_FIELDS.db",
            Kind::Fk => "_schema_FK.db",
            Kind::Def => "_schema_DEF.db",
            Kind::Json => "_schema_JSON.db",
            Kind::Pix => "_schema_PIX.db",
            Kind::Idx => "_schema_IDX.db",
            Kind::Uk => "_schema_UK.db",
            Kind::Typ => "_schema_TYP.db",
            Kind::Gen => "_schema_GEN.db",
            Kind::Chk => "_schema_CHK.db",
        }
    }

    fn stride(self) -> u64 {
        FIXED_RECORD_SUFFIXES
            .iter()
            .find(|(s, _)| *s == self.suffix())
            .map(|(_, stride)| *stride)
            .unwrap()
    }

    /// The write after the restart: appends to this kind's file.
    async fn write(self, db: &Arc<Database>) {
        let sql: &[&str] = match self {
            Kind::Pointers => &["INSERT INTO t (id, a, b, doc) VALUES (4, 4, 4, '{\"k\": 4}')"],
            Kind::Fk => &["ALTER TABLE t ADD CONSTRAINT fk2 FOREIGN KEY (b) REFERENCES p (id)"],
            Kind::Def => &["ALTER TABLE t ALTER COLUMN b SET DEFAULT 2"],
            Kind::Pix => &["CREATE INDEX pi2 ON t ((doc->>'m'))"],
            Kind::Idx => &["CREATE INDEX i2 ON t (b)"],
            Kind::Uk => &["ALTER TABLE t ADD CONSTRAINT u2 UNIQUE (b)"],
            // `ALTER TABLE … ADD COLUMN` rebuilds the table's files, so the
            // torn file would be replaced rather than appended to; and it
            // appends no type modifier, generation expression or check
            // (`ADD CHECK` writes nothing at all). These are written
            // through the table's own schema provider, the path
            // `CREATE TABLE` takes.
            Kind::Fields | Kind::Json | Kind::Typ | Kind::Gen | Kind::Chk => &[],
        };
        for s in sql {
            exec(db, s).await.unwrap();
        }
        let schema_io = || db.table_for_tests("t").unwrap().io_schema.clone();
        match self {
            Kind::Fields => {
                let mut schema = db.table_for_tests("t").unwrap().schema.clone();
                schema
                    .add_field(schema_io(), "c2".into(), crate::core::db_type::DbType::I32, false)
                    .await;
            }
            Kind::Json => crate::core::rql::json_map::append(
                schema_io(),
                &crate::core::rql::json_map::JsonColumn {
                    column: "b".into(),
                    shadow_table: "t_b_json".into(),
                    is_deleted: false,
                },
            )
            .await
            .unwrap(),
            Kind::Typ => crate::core::rql::type_modifier::append(
                schema_io(),
                &crate::core::rql::type_modifier::TypeModifier {
                    column: "m1".into(),
                    precision: 12,
                    scale: 2,
                    is_deleted: false,
                },
            )
            .await
            .unwrap(),
            Kind::Gen => crate::core::rql::generated::append(
                schema_io(),
                &crate::core::rql::generated::GeneratedColumn {
                    column: "b".into(),
                    expression: "a * 2".into(),
                    is_deleted: false,
                },
            )
            .await
            .unwrap(),
            Kind::Chk => crate::core::rql::check::append(
                schema_io(),
                &crate::core::rql::check::CheckConstraint {
                    name: "ck2".into(),
                    expression: "b > 0".into(),
                    is_deleted: false,
                },
            )
            .await
            .unwrap(),
            _ => {}
        }
    }
}

/// A database whose table `t` has a record in every fixed-record file, and
/// the parent table `p` its foreign keys point at; checkpointed, so the log
/// is empty and every file is as durable as a checkpoint leaves it.
async fn build_base(dir: &Path) {
    let db = open(dir).await.unwrap();
    let mut sql = vec!["CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, n INTEGER)".to_string()];
    for i in 1..=4 {
        sql.push(format!("INSERT INTO p (id, n) VALUES ({i}, {i})"));
    }
    sql.extend(
        [
            "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER, b INTEGER, doc JSONB, m1 NUMERIC(10,2), \
             g1 INTEGER GENERATED ALWAYS AS (a + 1) STORED, CHECK (a > 0))",
            "ALTER TABLE t ADD CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p (id)",
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT 1",
            "ALTER TABLE t ADD COLUMN j1 JSON",
            "CREATE INDEX pi1 ON t ((doc->>'k'))",
            "CREATE INDEX i1 ON t (a)",
            "ALTER TABLE t ADD CONSTRAINT u1 UNIQUE (id)",
        ]
        .map(String::from),
    );
    for i in 1..=3 {
        sql.push(format!("INSERT INTO t (id, a, b, doc) VALUES ({i}, {i}, {i}, '{{\"k\": {i}}}')"));
    }
    for s in &sql {
        exec(&db, s).await.unwrap();
    }
    checkpoint(dir);
    close(dir, db);
}

/// What a restart shows of `kind`'s write and everything before it: the
/// file's bytes, the table's schema as loaded, and its rows.
#[derive(Debug, PartialEq)]
struct Seen {
    file: Vec<u8>,
    schema: String,
    rows: Vec<Vec<Vec<u8>>>,
    parent: Vec<Vec<Vec<u8>>>,
}

async fn restart_and_look(dir: &Path, kind: Kind) -> Seen {
    let db = open(dir).await.unwrap();
    // Less its incarnation, which an `ALTER … ADD COLUMN` mints afresh.
    let mut schema = db.table_for_tests("t").unwrap().schema.clone();
    schema.incarnation = 0;
    let schema = format!("{schema:?}");
    let seen = Seen {
        file: std::fs::read(dir.join(format!("t{}", kind.suffix()))).unwrap(),
        schema,
        rows: rows(&db, "SELECT id, a, b FROM t").await.unwrap(),
        parent: rows(&db, "SELECT id, n FROM p").await.unwrap(),
    };
    close(dir, db);
    seen
}

/// For each torn length 1..stride: the file ends that far into the record
/// the write appends, the database opens (and cuts it), takes the write,
/// restarts, and shows exactly what an untorn copy shows.
async fn every_torn_length(kind: Kind) {
    let base = tempfile::tempdir().unwrap();
    let src = base.path().join("base");
    build_base(&src).await;
    let name = format!("t{}", kind.suffix());
    let whole = len(&src.join(&name));
    let stride = kind.stride();
    assert!(whole > 0 && whole % stride == 0, "{name}: {whole} bytes is not whole {stride}-byte records");

    // The untorn copy: what the write appends, and what a restart shows.
    let control = base.path().join("control");
    copy_db(&src, &control);
    let db = open(&control).await.unwrap();
    kind.write(&db).await;
    close(&control, db);
    let want = restart_and_look(&control, kind).await;
    assert!(
        want.file.len() as u64 >= whole + stride,
        "{name}: the write appended nothing ({} bytes, {whole} before)",
        want.file.len()
    );
    let record = want.file[whole as usize..(whole + stride) as usize].to_vec();
    std::fs::remove_dir_all(&control).unwrap();

    let work = base.path().join("work");
    for torn in 1..stride {
        copy_db(&src, &work);
        let file = work.join(&name);
        {
            let mut f = File::options().append(true).open(&file).unwrap();
            f.write_all(&record[..torn as usize]).unwrap();
        }
        let db = open(&work).await.unwrap_or_else(|e| panic!("{name}, {torn} bytes torn: {e}"));
        assert_eq!(len(&file), whole, "{name}, {torn} bytes torn: the open did not cut it back");
        kind.write(&db).await;
        close(&work, db);
        let got = restart_and_look(&work, kind).await;
        if got != want {
            let at = got.file.iter().zip(&want.file).position(|(a, b)| a != b);
            panic!(
                "{name}, {torn} bytes torn: after the write and a restart the database differs \
                 from an untorn copy (file {} bytes against {}, first difference at {at:?}; \
                 schema equal: {}; rows {:?} against {:?})",
                got.file.len(),
                want.file.len(),
                got.schema == want.schema,
                got.rows,
                want.rows
            );
        }
        std::fs::remove_dir_all(&work).unwrap();
    }
}

#[tokio::test]
async fn torn_row_pointers_are_cut_before_the_next_insert() {
    every_torn_length(Kind::Pointers).await;
}

#[tokio::test]
async fn torn_schema_fields_are_cut_before_the_next_column() {
    every_torn_length(Kind::Fields).await;
}

#[tokio::test]
async fn torn_foreign_keys_are_cut_before_the_next_one() {
    every_torn_length(Kind::Fk).await;
}

#[tokio::test]
async fn torn_column_defaults_are_cut_before_the_next_one() {
    every_torn_length(Kind::Def).await;
}

#[tokio::test]
async fn torn_json_registrations_are_cut_before_the_next_one() {
    every_torn_length(Kind::Json).await;
}

#[tokio::test]
async fn torn_path_indexes_are_cut_before_the_next_one() {
    every_torn_length(Kind::Pix).await;
}

#[tokio::test]
async fn torn_index_declarations_are_cut_before_the_next_one() {
    every_torn_length(Kind::Idx).await;
}

#[tokio::test]
async fn torn_unique_keys_are_cut_before_the_next_one() {
    every_torn_length(Kind::Uk).await;
}

#[tokio::test]
async fn torn_type_modifiers_are_cut_before_the_next_one() {
    every_torn_length(Kind::Typ).await;
}

#[tokio::test]
async fn torn_generated_columns_are_cut_before_the_next_one() {
    every_torn_length(Kind::Gen).await;
}

#[tokio::test]
async fn torn_check_constraints_are_cut_before_the_next_one() {
    every_torn_length(Kind::Chk).await;
}

// ---------------------------------------------------------------------------
// The floor, and the order against replay
// ---------------------------------------------------------------------------

/// A pointer file that lost bytes a checkpoint had synced ends inside a record
/// below its durable floor. Cutting it would drop a committed pointer the log
/// no longer holds; the open is refused, naming the file, its length, the
/// record size and the floor, and the file is left as it was.
#[tokio::test]
async fn a_torn_tail_below_the_durable_floor_refuses_to_open() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER)").await.unwrap();
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&dir);
    let floors = crate::core::wal::wal_for(&path_str(&dir)).unwrap().floors();
    close(&dir, db);
    let file = dir.join("t_pointers.db");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    assert_eq!(len(&file), 10 * stride);
    assert_eq!(floors.floor_of(&std::fs::metadata(&file).unwrap()), 10 * stride, "the checkpoint recorded no floor");

    // In place, so it is still the file the floor was recorded for.
    let original = std::fs::read(&file).unwrap();
    let lost = 8 * stride + 3;
    File::options().write(true).open(&file).unwrap().set_len(lost).unwrap();
    let err = match open(&dir).await {
        Ok(db) => {
            let n = ids(&db).await.len();
            close(&dir, db);
            panic!("a pointer file cut below its durable floor opened, with {n} rows");
        }
        Err(e) => e,
    };
    crate::core::wal::remove_for(&path_str(&dir));
    for needle in [
        "t_pointers.db".to_string(),
        format!("{lost} bytes"),
        format!("{stride} bytes"),
        format!("{} bytes", 10 * stride),
    ] {
        assert!(err.contains(&needle), "the refusal does not name {needle:?}: {err}");
    }
    assert_eq!(len(&file), lost, "a refused open changed the file");

    // Above the floor the same file opens: a whole-record tail is cut, and
    // nothing durable is lost.
    let mut f = File::options().write(true).open(&file).unwrap();
    f.write_all(&original).unwrap();
    f.write_all(&[0x5A; 3]).unwrap();
    drop(f);
    let db = open(&dir).await.unwrap();
    assert_eq!(ids(&db).await, (1..=10).collect::<Vec<i64>>());
    close(&dir, db);
    assert_eq!(len(&file), 10 * stride);
}

/// The cut comes before replay. A statement's pointer is torn mid-append
/// and its log record never committed, while a concurrent insert appended
/// the next pointer and committed it, in the log only. Replay writes that
/// one past the cut, so the torn slot must come back as the hole a
/// never-committed append leaves, with none of the fragment's bytes. The
/// fragment here is a whole pointer but its `xmax`, stamped frozen, so one
/// that survived reads as a row every snapshot sees.
#[tokio::test]
async fn the_cut_comes_before_replay_so_no_fragment_survives_under_a_replayed_record() {
    let base = tempfile::tempdir().unwrap();
    let built = base.path().join("built");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    let db = open(&built).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER)").await.unwrap();
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&built);
    close(&built, db);
    let bytes = std::fs::read(built.join("t_pointers.db")).unwrap();
    assert_eq!(bytes.len() as u64, 5 * stride);
    let row5 = bytes[(4 * stride) as usize..].to_vec();

    // The power cut, in a copy (whose files no floor names): slot 4 holds
    // the head of a pointer through its `xmin`, and row 5's pointer, at slot
    // 5, is in the log alone.
    let dir = base.path().join("db");
    copy_db(&built, &dir);
    let file = dir.join("t_pointers.db");
    let fragment_len = stride as usize - 8; // all but `xmax`
    let mut torn = bytes[..(4 * stride) as usize].to_vec();
    torn.extend_from_slice(&bytes[..fragment_len]); // row 1's pointer
    let xmin_at = torn.len() - 8;
    torn[xmin_at..].copy_from_slice(&FROZEN_TX.to_le_bytes());
    std::fs::write(&file, &torn).unwrap();
    {
        let wal = crate::core::wal::Wal::open(dir.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap();
        let txn = wal.begin();
        wal.log(txn, crate::core::wal::file_id_for_name("t_pointers.db"), 5 * stride, &row5)
            .unwrap();
        wal.commit_txn(txn).unwrap();
    }

    let db = open(&dir).await.unwrap();
    let after = std::fs::read(&file).unwrap();
    assert_eq!(after.len() as u64, 6 * stride, "replay did not restore the committed pointer");
    assert!(
        after[(4 * stride) as usize..(5 * stride) as usize].iter().all(|b| *b == 0),
        "the torn slot kept fragment bytes under the replayed record: {:?}",
        &after[(4 * stride) as usize..(5 * stride) as usize]
    );
    assert_eq!(ids(&db).await, vec![1, 2, 3, 4, 5], "the rows after replay");
    exec(&db, "INSERT INTO t (id, a) VALUES (6, 6)").await.unwrap();
    close(&dir, db);
    let db = open(&dir).await.unwrap();
    assert_eq!(ids(&db).await, vec![1, 2, 3, 4, 5, 6], "the rows after a write and a restart");
    close(&dir, db);
}

/// `VACUUM`'s compaction shortens a pointer file in place. Its floor must
/// come down with it, or a torn append after the shrink would be cut below
/// a floor the file no longer has, and the next open refused.
#[tokio::test]
async fn a_shrink_in_place_lowers_the_floor_first() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER)").await.unwrap();
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&dir);
    let file = dir.join("t_pointers.db");
    let floor = |db: &Arc<Database>| {
        let _ = db;
        crate::core::wal::wal_for(&path_str(&dir)).unwrap().floors().floor_of(&std::fs::metadata(&file).unwrap())
    };
    assert_eq!(floor(&db), 10 * stride);
    let io = db.table_for_tests("t").unwrap().io_pointers().unwrap().clone();
    crate::core::storage_providers::traits::StorageIO::truncate(&*io, 4 * stride).await.unwrap();
    assert_eq!(floor(&db), 4 * stride, "the shrink left the floor above the file");
    // And on disk, where the next open reads it.
    let stored = Floors::load(&Floors::path_for(&dir.join("rasterized.wal")));
    assert_eq!(stored.floor_of(&std::fs::metadata(&file).unwrap()), 4 * stride);
    close(&dir, db);
    // A torn tail after the shrink opens.
    let mut f = File::options().append(true).open(&file).unwrap();
    f.write_all(&[0x5A; 9]).unwrap();
    drop(f);
    let db = open(&dir).await.unwrap();
    close(&dir, db);
    assert_eq!(len(&file), 4 * stride);
}

// ---------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------

/// The pointer files of `dir`'s partitions, by name, with their lengths.
fn partition_pointer_files(dir: &Path) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            (name.starts_with(crate::core::partition::files::PREFIX) && name.ends_with("_pointers.db"))
                .then(|| (name, e.metadata().unwrap().len()))
        })
        .collect();
    out.sort();
    out
}

/// A partition's pointers are `__pt_<u>_<pid>_pointers.db`, beside the
/// table's one shared schema `__pt_<u>_schema.db` rather than a schema of
/// their own; its sidecars are `__pt_<u>_schema_<SIDECAR>.db`. Each is the
/// fixed-record file it is, and nothing else under the prefix is one.
#[test]
fn a_partitions_pointers_and_its_tables_sidecars_are_fixed_record_files() {
    use crate::core::partition::files;
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let uid = 0x0123_4567_89ab_cdef;
    let pointers = format!("{}_pointers.db", files::data_stem(uid, 7));
    let rows = format!("{}.db", files::data_stem(uid, 7));
    let filter = format!("{}_flt.db", files::data_stem(uid, 7));
    let fields = format!("{}_FIELDS.db", files::schema_stem(uid));
    let others = [rows.clone(), filter.clone(), files::map_file(uid, 0), files::ids_file(uid), files::seq_file(uid)];
    for name in others.iter().chain([&pointers, &fields]) {
        std::fs::write(d.join(name), b"").unwrap();
    }
    // No schema yet: nothing is anyone's.
    assert_eq!(classify(d, &pointers), None);
    assert_eq!(classify(d, &fields), None);
    std::fs::write(d.join(format!("{}.db", files::schema_stem(uid))), b"").unwrap();
    assert_eq!(classify(d, &pointers).map(|k| k.stride), Some(ROW_POINTER_RECORD_LEN as u64));
    assert_eq!(classify(d, &fields).map(|k| k.stride), Some(crate::core::row::schema::SCHEMA_FIELD_SIZE));
    for name in &others {
        assert_eq!(classify(d, name), None, "{name} taken for a fixed-record file");
    }
    // Another table's schema does not make a partition's pointers fixed-record.
    let stranger = format!("{}_pointers.db", files::data_stem(uid + 1, 7));
    std::fs::write(d.join(&stranger), b"").unwrap();
    assert_eq!(classify(d, &stranger), None);
    // Nor a name only shaped like one: a pid that is not eight hex digits.
    let misshapen = format!("{}{uid:016x}_7_pointers.db", files::PREFIX);
    std::fs::write(d.join(&misshapen), b"").unwrap();
    assert_eq!(classify(d, &misshapen), None);
}

/// A partition's pointer file torn by a power cut is cut back at the open
/// like a plain table's, so the insert after it lands on a record boundary:
/// for each torn length, the database opens, takes an insert routed to that
/// partition, restarts, and shows exactly what an untorn copy shows.
#[tokio::test]
async fn torn_partition_pointers_are_cut_before_the_next_insert() {
    let base = tempfile::tempdir().unwrap();
    let src = base.path().join("base");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    let db = open(&src).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER) PARTITION BY HASH (id) PARTITIONS 4")
        .await
        .unwrap();
    for i in 1..=12 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&src);
    close(&src, db);
    let before = partition_pointer_files(&src);
    assert!(before.len() > 1, "twelve rows reached one partition: {before:?}");

    // The untorn copy: which partition id 100 goes to, and the record it
    // appends there.
    let control = base.path().join("control");
    copy_db(&src, &control);
    let db = open(&control).await.unwrap();
    exec(&db, "INSERT INTO t (id, a) VALUES (100, 100)").await.unwrap();
    close(&control, db);
    let after = partition_pointer_files(&control);
    let grown: Vec<_> = after.iter().filter(|f| !before.contains(f)).collect();
    assert_eq!(grown.len(), 1, "one partition takes the row: {before:?} -> {after:?}");
    let name = grown[0].0.clone();
    let whole = before.iter().find(|(n, _)| *n == name).map_or(0, |(_, l)| *l);
    assert!(whole > 0 && whole % stride == 0, "{name}: {whole} bytes is not whole records");
    let db = open(&control).await.unwrap();
    let want_ids = ids(&db).await;
    close(&control, db);
    let want_file = std::fs::read(control.join(&name)).unwrap();
    assert_eq!(want_file.len() as u64, whole + stride);
    let record = want_file[whole as usize..].to_vec();
    assert_eq!(want_ids, (1..=12).chain([100]).collect::<Vec<i64>>());

    let work = base.path().join("work");
    for torn in 1..stride {
        copy_db(&src, &work);
        let file = work.join(&name);
        {
            let mut f = File::options().append(true).open(&file).unwrap();
            f.write_all(&record[..torn as usize]).unwrap();
        }
        let db = open(&work).await.unwrap_or_else(|e| panic!("{name}, {torn} bytes torn: {e}"));
        assert_eq!(len(&file), whole, "{name}, {torn} bytes torn: the open did not cut it back");
        exec(&db, "INSERT INTO t (id, a) VALUES (100, 100)").await.unwrap();
        close(&work, db);
        let db = open(&work).await.unwrap();
        let got_ids = ids(&db).await;
        close(&work, db);
        assert_eq!(got_ids, want_ids, "{name}, {torn} bytes torn: the rows after a write and a restart");
        assert_eq!(
            std::fs::read(&file).unwrap(),
            want_file,
            "{name}, {torn} bytes torn: the pointer file differs from an untorn copy's"
        );
        std::fs::remove_dir_all(&work).unwrap();
    }
}

/// A checkpoint records a partition's pointer file's floor like any other
/// fixed-record file's, keyed by its identity, so a partition that lost
/// synced bytes refuses to open rather than being cut below them.
#[tokio::test]
async fn a_partitions_pointers_get_a_durable_floor() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER) PARTITION BY HASH (id) PARTITIONS 2")
        .await
        .unwrap();
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&dir);
    let files = partition_pointer_files(&dir);
    let (name, whole) = files.iter().max_by_key(|(_, l)| *l).cloned().unwrap();
    assert!(whole >= 2 * stride, "{files:?}");
    let file = dir.join(&name);
    let floor = crate::core::wal::wal_for(&path_str(&dir)).unwrap().floors().floor_of(&std::fs::metadata(&file).unwrap());
    assert_eq!(floor, whole, "{name}: the checkpoint recorded no floor");
    close(&dir, db);
    // Lose a synced record and tear the one before it: below the floor.
    let f = File::options().write(true).open(&file).unwrap();
    f.set_len(whole - stride - 3).unwrap();
    drop(f);
    let e = match open(&dir).await {
        Ok(db) => {
            close(&dir, db);
            panic!("{name}: a partition's pointers cut below their durable floor opened");
        }
        Err(e) => e,
    };
    crate::core::wal::remove_for(&path_str(&dir));
    assert!(e.contains(&name) && e.contains("durable floor"), "{e}");
    assert_eq!(len(&file), whole - stride - 3, "the refused open changed the file");
}

/// A partition's pointer file shrunk in place lowers its floor through the
/// database's log, as a plain table's does: its provider is in the root, so
/// `lower_floor_for` reaches the log whose checkpoint recorded the floor. A
/// torn append after the shrink then opens.
#[tokio::test]
async fn a_partitions_shrink_in_place_lowers_its_floor() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let stride = ROW_POINTER_RECORD_LEN as u64;
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL, a INTEGER) PARTITION BY HASH (id) PARTITIONS 2")
        .await
        .unwrap();
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t (id, a) VALUES ({i}, {i})")).await.unwrap();
    }
    checkpoint(&dir);
    let pt = db.partitioned_table("t").unwrap();
    let epoch = pt.epoch().unwrap();
    let (pid, part) = pt
        .materialized(&epoch, &crate::core::partition::Route::All).await
        .unwrap()
        .into_iter()
        .max_by_key(|(_, p)| p.data().unwrap().len.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap();
    let file = dir.join(format!("{}_pointers.db", crate::core::partition::files::data_stem(pt.uid(), pid)));
    let whole = len(&file);
    assert!(whole >= 2 * stride, "{}: {whole} bytes", file.display());
    let floor = || {
        crate::core::wal::wal_for(&path_str(&dir)).unwrap().floors().floor_of(&std::fs::metadata(&file).unwrap())
    };
    assert_eq!(floor(), whole, "the checkpoint recorded no floor for the partition");
    let io = part.io_pointers().unwrap().clone();
    crate::core::storage_providers::traits::StorageIO::truncate(&*io, stride).await.unwrap();
    assert_eq!(floor(), stride, "the shrink left the partition's floor above its file");
    drop((io, part, epoch, pt));
    close(&dir, db);
    let mut f = File::options().append(true).open(&file).unwrap();
    f.write_all(&[0x5A; 9]).unwrap();
    drop(f);
    let db = open(&dir).await.unwrap();
    close(&dir, db);
    assert_eq!(len(&file), stride);
}

/// A schema sidecar kind, for the write-ahead tests below: how its `i`th
/// record is appended through the sidecar's own `append`, and what its
/// loader names it.
#[derive(Debug, Clone, Copy)]
enum Sidecar {
    Fk,
    Chk,
    Uk,
    Def,
    Idx,
    Typ,
    Gen,
    Json,
    Pix,
}

impl Sidecar {
    const ALL: [Sidecar; 9] =
        [Sidecar::Fk, Sidecar::Chk, Sidecar::Uk, Sidecar::Def, Sidecar::Idx, Sidecar::Typ, Sidecar::Gen, Sidecar::Json, Sidecar::Pix];

    fn suffix(self) -> &'static str {
        match self {
            Sidecar::Fk => "FK.db",
            Sidecar::Chk => "CHK.db",
            Sidecar::Uk => "UK.db",
            Sidecar::Def => "DEF.db",
            Sidecar::Idx => "IDX.db",
            Sidecar::Typ => "TYP.db",
            Sidecar::Gen => "GEN.db",
            Sidecar::Json => "JSON.db",
            Sidecar::Pix => "PIX.db",
        }
    }

    fn stride(self) -> u64 {
        match self {
            Sidecar::Fk => crate::core::rql::foreign_key::FOREIGN_KEY_SIZE,
            Sidecar::Chk => crate::core::rql::check::CHECK_SIZE,
            Sidecar::Uk => crate::core::rql::unique_key::UNIQUE_KEY_SIZE,
            Sidecar::Def => crate::core::rql::column_default::COLUMN_DEFAULT_SIZE,
            Sidecar::Idx => crate::core::index::decl::DECL_SIZE,
            Sidecar::Typ => crate::core::rql::type_modifier::TYPE_MODIFIER_SIZE,
            Sidecar::Gen => crate::core::rql::generated::GENERATED_SIZE,
            Sidecar::Json => crate::core::rql::json_map::JSON_COLUMN_SIZE,
            Sidecar::Pix => crate::core::rql::path_index::PATH_INDEX_SIZE,
        }
    }

    /// The name record `i` is loaded under.
    fn name(self, i: usize) -> String {
        match self {
            Sidecar::Def | Sidecar::Typ | Sidecar::Gen | Sidecar::Json => format!("c{i}"),
            _ => format!("{}{i}", self.suffix().trim_end_matches(".db").to_ascii_lowercase()),
        }
    }

    async fn append<S: crate::core::storage_providers::traits::StorageIO>(self, io: &Arc<S>, i: usize) -> io::Result<()> {
        use crate::core::rql::*;
        let io = io.clone();
        let name = self.name(i);
        match self {
            Sidecar::Fk => {
                let fk = foreign_key::ForeignKey {
                    name,
                    column: "v".into(),
                    parent_table: format!("parent_{i}"),
                    parent_column: "id".into(),
                    on_delete: foreign_key::ReferentialAction::Cascade,
                    on_update: foreign_key::ReferentialAction::SetNull,
                    is_deleted: false,
                };
                foreign_key::append(io, &fk).await
            }
            Sidecar::Chk => {
                check::append(io, &check::CheckConstraint { name, expression: format!("v > {i}"), is_deleted: false }).await
            }
            Sidecar::Uk => {
                let key = unique_key::UniqueKey {
                    name,
                    columns: vec!["v".into(), "id".into()],
                    is_deleted: false,
                    predicate: None,
                    compiled: Default::default(),
                };
                unique_key::append(io, &key).await
            }
            Sidecar::Def => {
                column_default::append(io, &column_default::ColumnDefault { column: name, expression: format!("{i}"), is_deleted: false })
                    .await
            }
            Sidecar::Idx => {
                let decl = crate::core::index::decl::IndexDecl {
                    name,
                    method: crate::core::index::decl::Method::BTree,
                    unique: false,
                    columns: vec![1],
                    is_deleted: false,
                };
                crate::core::index::decl::append(io, &decl).await
            }
            Sidecar::Typ => {
                type_modifier::append(io, &type_modifier::TypeModifier { column: name, precision: 12, scale: 3, is_deleted: false })
                    .await
            }
            Sidecar::Gen => {
                generated::append(io, &generated::GeneratedColumn { column: name, expression: format!("v + {i}"), is_deleted: false })
                    .await
            }
            Sidecar::Json => {
                json_map::append(io, &json_map::JsonColumn { column: name, shadow_table: format!("shadow_{i}"), is_deleted: false })
                    .await
            }
            Sidecar::Pix => {
                path_index::append(io, &path_index::PathIndex { name, path: format!("a{i}"), is_deleted: false }).await
            }
        }
    }

    /// Record `i`, staged into `writes` through the sidecar's own `stage`.
    fn stage<S: crate::core::storage_providers::traits::StorageIO>(
        self,
        io: &S,
        writes: &mut SidecarWrites,
        i: usize,
    ) -> io::Result<()> {
        use crate::core::rql::*;
        let name = self.name(i);
        match self {
            Sidecar::Fk => foreign_key::stage(
                io,
                writes,
                &foreign_key::ForeignKey {
                    name,
                    column: "v".into(),
                    parent_table: format!("parent_{i}"),
                    parent_column: "id".into(),
                    on_delete: foreign_key::ReferentialAction::Cascade,
                    on_update: foreign_key::ReferentialAction::SetNull,
                    is_deleted: false,
                },
            ),
            Sidecar::Chk => {
                check::stage(io, writes, &check::CheckConstraint { name, expression: format!("v > {i}"), is_deleted: false })
            }
            Sidecar::Uk => unique_key::stage(
                io,
                writes,
                &unique_key::UniqueKey {
                    name,
                    columns: vec!["v".into(), "id".into()],
                    is_deleted: false,
                    predicate: None,
                    compiled: Default::default(),
                },
            ),
            Sidecar::Def => column_default::stage(
                io,
                writes,
                &column_default::ColumnDefault { column: name, expression: format!("{i}"), is_deleted: false },
            ),
            Sidecar::Idx => crate::core::index::decl::stage(
                io,
                writes,
                &crate::core::index::decl::IndexDecl {
                    name,
                    method: crate::core::index::decl::Method::BTree,
                    unique: false,
                    columns: vec![1],
                    is_deleted: false,
                },
            ),
            Sidecar::Typ => type_modifier::stage(
                io,
                writes,
                &type_modifier::TypeModifier { column: name, precision: 12, scale: 3, is_deleted: false },
            ),
            Sidecar::Gen => generated::stage(
                io,
                writes,
                &generated::GeneratedColumn { column: name, expression: format!("v + {i}"), is_deleted: false },
            ),
            Sidecar::Json => json_map::stage(
                io,
                writes,
                &json_map::JsonColumn { column: name, shadow_table: format!("shadow_{i}"), is_deleted: false },
            ),
            Sidecar::Pix => {
                path_index::stage(io, writes, &path_index::PathIndex { name, path: format!("a{i}"), is_deleted: false })
            }
        }
    }

    /// What the loader reads, each record as its name and the field that
    /// carries the rest of it (a torn record reads with a name and a cut or
    /// missing remainder).
    async fn load<S: crate::core::storage_providers::traits::StorageIO>(self, io: &Arc<S>) -> Vec<(String, String)> {
        use crate::core::rql::*;
        let io = io.clone();
        match self {
            Sidecar::Fk => foreign_key::load(io)
                .await
                .into_iter()
                .map(|f| (f.name, format!("{} {} {} {:?} {:?}", f.column, f.parent_table, f.parent_column, f.on_delete, f.on_update)))
                .collect(),
            Sidecar::Chk => check::load(io).await.into_iter().map(|c| (c.name, c.expression)).collect(),
            Sidecar::Uk => unique_key::load(io).await.into_iter().map(|k| (k.name, k.columns.join(","))).collect(),
            Sidecar::Def => column_default::load(io).await.into_iter().map(|d| (d.column, d.expression)).collect(),
            Sidecar::Idx => crate::core::index::decl::load(io)
                .await
                .into_iter()
                .map(|d| (d.name, format!("{:?} {:?}", d.method, d.columns)))
                .collect(),
            Sidecar::Typ => {
                type_modifier::load(io).await.into_iter().map(|t| (t.column, format!("{} {}", t.precision, t.scale))).collect()
            }
            Sidecar::Gen => generated::load(io).await.into_iter().map(|g| (g.column, g.expression)).collect(),
            Sidecar::Json => json_map::load(io).await.into_iter().map(|j| (j.column, j.shadow_table)).collect(),
            Sidecar::Pix => path_index::load(io).await.into_iter().map(|p| (p.name, p.path)).collect(),
        }
    }

    /// What the loader reads after records `0..n`.
    async fn expected(self, n: usize) -> Vec<(String, String)> {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(crate::core::storage_providers::file_sync::LocalStorageProvider::new(path_str(dir.path()).as_str(), Some("e_schema.db")).await);
        for i in 0..n {
            self.append(&io, i).await.unwrap();
        }
        self.load(&io).await
    }
}

/// A record of zeros in a schema sidecar (what a build before
/// `SidecarWrites` could leave: a logged append whose length a power cut
/// kept and whose page it dropped) is skipped by every loader, and the
/// records around it still read.
///
/// Mutation: any one loader's `is_unwritten` check removed; that loader
/// reads a declaration with no name (or no column), and the count is off.
#[tokio::test]
async fn a_schema_record_of_zeros_reads_as_no_declaration() {
    for kind in Sidecar::ALL {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(crate::core::storage_providers::file_sync::LocalStorageProvider::new(path_str(dir.path()).as_str(), Some("z_schema.db")).await);
        let sidecar = || {
            crate::core::storage_providers::traits::StorageIO::create_new(&*io, format!("z_schema_{}", kind.suffix()))
        };
        kind.append(&io, 0).await.unwrap();
        crate::core::storage_providers::traits::StorageIO::append_data(&sidecar().await, &vec![0u8; kind.stride() as usize], true).await;
        kind.append(&io, 1).await.unwrap();
        assert_eq!(
            crate::core::storage_providers::traits::StorageIO::get_len(&sidecar().await).await,
            3 * kind.stride(),
            "{kind:?}: the zero record is not in the file"
        );
        let got = kind.load(&io).await;
        assert_eq!(got, kind.expected(2).await, "{kind:?}: a record of zeros read as a declaration");
    }
}

/// Each schema sidecar's records, appended through its `append` with the
/// recorder running, cut after each of the last ones (past the file's
/// first page boundary, so one record straddles two pages): every image
/// under every seed kind reads exactly the records appended so far, whole.
/// None lost once its `append` returned, none torn, none of zeros.
///
/// Mutation: `SidecarWrites::commit` back to a logged append (`append_data(..,
/// true)`). The bytes reach the file before the log's sync, which the
/// statement used to take later: an image that drops the log loses records
/// `append` reported, and one that keeps the length and drops a page reads
/// a record of zeros (skipped) or a page's half of one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_schema_record_reads_whole_after_a_cut_once_appended() {
    use crate::core::power_loss::image::Recorder;
    let seeds: u64 = std::env::var("RDB_POWER_LOSS_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(5).clamp(1, 64);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    {
        let db = open(&dir).await.unwrap();
        exec(&db, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await.unwrap();
        db.sync_durable().unwrap();
        close(&dir, db);
    }
    let db = open(&dir).await.unwrap();
    let io = db.table_held("t").unwrap().io_schema.clone();
    let rec = Recorder::start(&dir);
    let mut points = Vec::new();
    let mut disturbed = 0;
    for kind in Sidecar::ALL {
        // Two records past the first page boundary.
        let records = (4096 / kind.stride() + 2) as usize;
        for i in 0..records {
            kind.append(&io, i).await.unwrap_or_else(|e| panic!("{kind:?} #{i}: {e}"));
            if i + 3 >= records {
                match rec.cut_quiescent(format!("{kind:?} after record {i}")) {
                    Ok(point) => points.push((kind, i + 1, point)),
                    Err(why) => {
                        eprintln!("{why}");
                        disturbed += 1;
                    }
                }
            }
        }
    }
    drop(io);
    close(&dir, db);
    drop(rec);
    assert!(disturbed * 4 <= points.len(), "too many cuts were disturbed: {disturbed} of {}", points.len() + disturbed);
    eprintln!("{} cuts, {disturbed} disturbed, {seeds} seeds each", points.len());

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for (kind, n, point) in &points {
        let want = kind.expected(*n).await;
        for seed in 0..seeds {
            point.write_image(seed, &img);
            match open(&img).await {
                Err(e) => problems.push(format!("{}: seed {seed}: does not open: {e}", point.label)),
                Ok(db) => {
                    let io = db.table_held("t").unwrap().io_schema.clone();
                    let got = kind.load(&io).await;
                    if got != want {
                        problems.push(format!("{} seed {seed}: read {got:?}\n  appended {want:?}", point.label));
                    }
                    drop(io);
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
}

/// Not a test: the statements whose syncs `strace -f -e
/// trace=fsync,fdatasync,openat` counts, each preceded by an `openat` of
/// `/__rdb_sync_mark_<label>` (which fails, and so marks the trace). Run
/// alone, single-threaded; see `.claude/reviews/p4-fk-crash/count_syncs.sh`.
/// tmpfs hides what a sync costs, never how many there are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn sidecar_sync_count_probe() {
    let mark = |label: &str| {
        let _ = std::fs::File::open(format!("/__rdb_sync_mark_{label}"));
    };
    let settle = || tokio::time::sleep(std::time::Duration::from_millis(400));
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)").await.unwrap();
    exec(&db, "INSERT INTO parent (id) VALUES (1), (2), (3)").await.unwrap();
    exec(&db, "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, p BIGINT, v BIGINT)").await.unwrap();
    exec(&db, "INSERT INTO t2 (id, p, v) VALUES (1, 1, 10), (2, 2, 20)").await.unwrap();
    db.sync_durable().unwrap();
    settle().await;
    let statements = [
        (
            "create_table",
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b TEXT, \
             c BIGINT CHECK (c > 0), d BIGINT CHECK (d < 100), e BIGINT DEFAULT 1, f TEXT DEFAULT 'x', \
             g BIGINT DEFAULT 2, n1 NUMERIC(10, 2), n2 NUMERIC(12, 4), p BIGINT REFERENCES parent (id), \
             CONSTRAINT t_a UNIQUE (a), CONSTRAINT t_b UNIQUE (b))",
        ),
        ("add_fk", "ALTER TABLE t2 ADD CONSTRAINT t2_fk FOREIGN KEY (p) REFERENCES parent (id)"),
        ("create_index", "CREATE INDEX t2_v ON t2 (v)"),
        ("set_default", "ALTER TABLE t2 ALTER COLUMN v SET DEFAULT 7"),
        ("add_check", "ALTER TABLE t2 ADD CONSTRAINT t2_c CHECK (v > 0)"),
    ];
    for (label, sql) in statements {
        mark(&format!("{label}_begin"));
        exec(&db, sql).await.unwrap_or_else(|e| panic!("{label}: {e}"));
        mark(&format!("{label}_end"));
        settle().await;
    }
    let t = db.table_held("t").unwrap();
    let s = &t.schema;
    eprintln!(
        "t declares uk {:?} chk {} def {} typ {} fk {}",
        s.unique_keys.iter().map(|k| &k.name).collect::<Vec<_>>(),
        s.checks.len(),
        s.defaults.len(),
        s.type_modifiers.len(),
        s.foreign_keys.len()
    );
    assert!(s.unique_keys.len() >= 2 && s.checks.len() == 2 && s.defaults.len() == 3);
    assert!(s.type_modifiers.len() == 2 && s.foreign_keys.len() == 1);
    close(&dir, db);
}

/// One statement's records in several sidecars, staged into one
/// `SidecarWrites` and committed once, cut inside each log sync the commit
/// takes and after it returns: every image under every seed kind holds all
/// of them, whole, or none (and an image cut after the commit, all). Across
/// the seeds both happen, or the cut tells nothing apart.
///
/// Mutations: each file committed as its own transaction; the first file
/// committed before the others are written. Either takes a second log sync,
/// and a cut inside it keeps the first file's records without the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_statements_sidecar_records_read_all_or_none_after_a_cut() {
    use crate::core::power_loss::failpoint;
    use crate::core::power_loss::image::Recorder;
    use parking_lot::Mutex;
    const KINDS: [Sidecar; 6] = [Sidecar::Fk, Sidecar::Chk, Sidecar::Uk, Sidecar::Def, Sidecar::Typ, Sidecar::Gen];
    const EACH: usize = 3;
    let seeds: u64 = std::env::var("RDB_POWER_LOSS_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(5).clamp(1, 64);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    {
        let db = open(&dir).await.unwrap();
        exec(&db, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await.unwrap();
        db.sync_durable().unwrap();
        close(&dir, db);
    }
    let db = open(&dir).await.unwrap();
    let io = db.table_held("t").unwrap().io_schema.clone();
    let rec = Recorder::start(&dir);
    let sink = std::sync::Arc::new(Mutex::new(Vec::new()));
    let guards: Vec<failpoint::Guard> = (1..=4)
        .map(|nth| failpoint::arm("sync:wal.log", &dir, nth, rec.cut_action(format!("log sync #{nth}"), sink.clone())))
        .collect();
    let mut writes = SidecarWrites::default();
    for kind in KINDS {
        for i in 0..EACH {
            kind.stage(&*io, &mut writes, i).unwrap();
        }
    }
    writes.commit(&*io).await.unwrap();
    let fired: Vec<u64> = guards.iter().map(|g| g.fired()).collect();
    drop(guards);
    let after = rec.cut_quiescent("after the commit");
    drop(io);
    close(&dir, db);
    drop(rec);
    assert!(fired[0] == 1, "the commit took no log sync ({fired:?})");
    let mut points: Vec<_> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    let after = after.expect("the cut after the commit was disturbed");
    let after_label = after.label.clone();
    points.push(after);

    let mut want = Vec::new();
    for kind in KINDS {
        want.push(kind.expected(EACH).await);
    }
    let img = base.path().join("image");
    let (mut all, mut none, mut problems) = (0, 0, Vec::new());
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img);
            match open(&img).await {
                Err(e) => problems.push(format!("{} seed {seed}: does not open: {e}", point.label)),
                Ok(db) => {
                    let io = db.table_held("t").unwrap().io_schema.clone();
                    let mut got = Vec::new();
                    for kind in KINDS {
                        got.push(kind.load(&io).await);
                    }
                    if got == want {
                        all += 1;
                    } else if got.iter().all(Vec::is_empty) && point.label != after_label {
                        none += 1;
                    } else {
                        problems.push(format!("{} seed {seed}: read {got:?}", point.label));
                    }
                    drop(io);
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    eprintln!("{} cuts (log syncs fired {fired:?}): {all} all, {none} none", points.len());
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    assert!(all > 0 && none > 0, "every image fell on one side ({all} all, {none} none): the cuts tell nothing apart");
}

/// A field record of zeros in `<t>_schema_FIELDS.db` (an append a power cut
/// kept the length of and dropped the page of) is no column: the schema
/// loads the columns around it, and none named "".
///
/// Mutation: `SchemaFieldIterator::next_schema_field`'s `is_unwritten` check
/// removed.
#[tokio::test]
async fn a_field_record_of_zeros_reads_as_no_column() {
    use crate::core::db_type::DbType;
    use crate::core::row::schema::{SCHEMA_FIELD_SIZE, TableSchema};
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(crate::core::storage_providers::file_sync::LocalStorageProvider::new(path_str(dir.path()).as_str(), Some("f_schema.db")).await);
    let mut schema = TableSchema::new("f".into(), false);
    schema.save(io.clone()).await.unwrap();
    schema.add_field(io.clone(), "a".into(), DbType::I64, false).await;
    let fields = crate::core::storage_providers::traits::StorageIO::create_new(&*io, "f_schema_FIELDS.db".to_string()).await;
    crate::core::storage_providers::traits::StorageIO::append_data(&fields, &vec![0u8; SCHEMA_FIELD_SIZE as usize], true).await;
    schema.add_field(io.clone(), "b".into(), DbType::STRING, false).await;
    let on_disk = std::fs::metadata(dir.path().join("f_schema_FIELDS.db")).unwrap().len();
    assert_eq!(on_disk, 3 * SCHEMA_FIELD_SIZE, "the zero record is not between the two fields");
    let loaded = TableSchema::load(io.clone()).await.expect("a schema with a record of zeros must load");
    let names: Vec<&str> = loaded.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["a", "b"], "a record of zeros read as a column");
}

/// A batch one of whose writes failed is refused at its commit, and the
/// transaction is released with it: the other files' held writes no longer
/// count as an open transaction, so the next checkpoint is not declined for
/// it, and the next batch commits.
///
/// Mutation: `commit_durable_with` no longer aborting a transaction it
/// refuses before committing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_batch_releases_its_transaction() {
    use crate::core::storage_providers::file_sync::failed_appends::FAIL_INPLACE_IN;
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await.unwrap();
    db.sync_durable().unwrap();
    let io = db.table_held("t").unwrap().io_schema.clone();
    let location = crate::core::storage_providers::traits::StorageIO::get_location(&*io).unwrap();
    let wal = crate::core::wal::wal_for(&location).expect("the table's directory has a log");
    assert_eq!(wal.pending_transactions(), 0);

    // The first file's write is refused; the second's is held.
    FAIL_INPLACE_IN.lock().push(location.clone());
    let mut writes = SidecarWrites::default();
    Sidecar::Fk.stage(&*io, &mut writes, 0).unwrap();
    Sidecar::Chk.stage(&*io, &mut writes, 0).unwrap();
    let refused = writes.commit(&*io).await;
    FAIL_INPLACE_IN.lock().retain(|l| *l != location);
    assert!(refused.is_err(), "a batch with a refused write committed");
    assert_eq!(wal.pending_transactions(), 0, "the refused batch's transaction is still held");
    assert_eq!(Sidecar::Chk.load(&io).await, Vec::new(), "the refused batch's other file was written");

    let mut writes = SidecarWrites::default();
    Sidecar::Fk.stage(&*io, &mut writes, 1).unwrap();
    writes.commit(&*io).await.expect("the next batch commits");
    assert_eq!(Sidecar::Fk.load(&io).await, Sidecar::Fk.expected(2).await[1..].to_vec());
    drop(io);
    close(&dir, db);
}
