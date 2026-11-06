//! Reshape (docs/partitioning.md §6.2, phase 5): one primitive behind
//! `SPLIT PARTITION`, `MERGE PARTITIONS`, `ISOLATE KEY`, `SPLIT KEY` and
//! `STORAGE MOVE PARTITION`. Sources (contiguous live partitions) are
//! replaced by targets (new pids tiling their span); **every target is a new
//! pid and every source is retired**, so no live partition ever holds a
//! version outside its range.
//!
//! # As built (phase 5, P7): under ACCESS EXCLUSIVE
//!
//! The design copies with writers running and fences them only for a delta
//! (§6.2 steps 2–4). This version runs the whole reshape under ACCESS
//! EXCLUSIVE on the logical name, which every statement takes before it
//! reaches a partition, and which a transaction that touched the table holds
//! until it ends (persistent-indexes §7). So while a reshape runs:
//!
//! - no statement reads or writes the table, and none holds a map epoch
//!   (§6.2 step 8's epoch-gated reclaim has nothing to wait for);
//! - no transaction has an uncommitted version in it (a writer's block holds
//!   the lock until it commits or aborts), so the copy never races a stamp;
//! - a `REPEATABLE READ` snapshot taken earlier, by a transaction that has
//!   not touched the table yet, may still read it afterwards: the copy is
//!   **raw**, each version's `xmin`, `xmax` and row id kept (as `VACUUM FULL`
//!   moves rows), so every snapshot sees in the targets exactly what it saw
//!   in the sources. No MVCC mover (§6.2: delete-and-insert loses rows under
//!   `REPEATABLE READ`).
//!
//! The cost is the table's availability for the copy's length; the online
//! copy with a fence is left for when that is measured to matter.
//!
//! # Steps (`PartitionedTable::reshape`)
//!
//! 1. Open the sources, then retire the instance: every open partition's
//!    maintenance stopped and joined, nothing opens through it again.
//! 2. `RESHAPE_BEGIN`, durable: target pids are spent from here on.
//! 3. Create each target's files (one directory fsync) and open it
//!    `External`, its maintenance retired at once.
//! 4. Copy every version of every source a snapshot may still see (not
//!    reclaimable: vacuum may have given a reclaimable one's bytes to another
//!    row), each **routed by its
//!    own recomputed point** to the target whose range holds it (a point
//!    outside every target aborts), in storage transactions committed
//!    durably.
//! 5. Verify: per target, the pointer records and row bytes read back match
//!    what was written (count and CRC32), and the records copied equal the
//!    records read.
//! 6. `RESHAPE_COMMIT`, durable, each target pinned at or below the lowest
//!    pin of the sources it overlaps (they hold those sources' raw-copied
//!    aborted stamps, §7.2): **the commit point**.
//!
//! The caller (`Database::reshape_partitioned`) then reopens the table over
//! the new map (`PartitionedTable::reopened` builds slots from it), deletes
//! the sources' files after voiding their log records, and appends
//! `RETIRED_GONE`. A failure before step 6 appends `RESHAPE_ABORT` and the
//! caller deletes the targets' files. A crash: before the commit, the open
//! aborts the reshape and the startup reaper deletes the targets' files
//! (no `MATERIALIZE` names them); after it, the open records the sources
//! gone and the reaper deletes their files (a retired partition is never
//! placed): `PinBoard::finish_reshapes`, `files::reapable`.

use std::io;
use std::path::{Path, PathBuf};

use rclite::Arc;

use super::map::PointRange;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointer};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

type PartTable = Table<DbStorageProvider>;

/// Pointer records read per chunk.
const CHUNK_RECORDS: usize = 4096;

/// One version of a source, raw.
struct RawVersion {
    pointer: RowPointer,
    bytes: Vec<u8>,
    point: u64,
}

/// One target as the copy writes it.
pub(super) struct CopyTarget {
    pub range: PointRange,
    pub table: Arc<PartTable>,
    /// Records and the CRC32 of their row bytes, in append order.
    pub records: u64,
    crc: crc32fast::Hasher,
    pub row_bytes: u64,
}

impl CopyTarget {
    pub(super) fn new(range: PointRange, table: Arc<PartTable>) -> Self {
        CopyTarget { range, table, records: 0, crc: crc32fast::Hasher::new(), row_bytes: 0 }
    }
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Up to [`CHUNK_RECORDS`] versions of `source` from pointer slot `from`:
/// every record that is not reclaimable under `horizon`, with its row bytes
/// and its point. Returns them and the slot after the last one read; an
/// empty read means the end.
///
/// A reclaimable version (a tombstone, a dead one no snapshot can see, an
/// aborted insert) is skipped, as `VACUUM FULL` skips it, and must be: vacuum
/// frees such a version's extent without tombstoning its pointer, so its
/// bytes may already be another row's (scans skip it for the same reason,
/// `VersionHorizon::for_visibility`). Copying one read a stranger's bytes as
/// its row: a point outside every target, or a misread header. A record
/// that is not reclaimable and names bytes past the rows file refuses the
/// reshape: silently dropping it, as a compaction does, would be a lost row
/// here.
async fn read_chunk(
    source: &PartTable,
    from: u64,
    horizon: &crate::core::vacuum::VersionHorizon,
) -> io::Result<(Vec<RawVersion>, u64)> {
    let d = source.data()?;
    let rec = ROW_POINTER_RECORD_LEN as u64;
    let records = d.io_pointers.get_len().await / rec;
    if from >= records {
        return Ok((Vec::new(), from));
    }
    let take = (records - from).min(CHUNK_RECORDS as u64);
    let mut buf = vec![0u8; (take * rec) as usize];
    let mut at = from * rec;
    d.io_pointers.read_data_into_buffer(&mut at, &mut buf).await?;
    let rows_len = d.io_rows.get_len().await;
    let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&source.schema.fields);
    let mut row = Row::default();
    let mut out = Vec::with_capacity(take as usize);
    for (i, record) in buf.chunks_exact(ROW_POINTER_RECORD_LEN).enumerate() {
        let pointer = RowPointer::from_slice_unknown(record);
        if horizon.is_reclaimable(pointer.deleted, pointer.xmin, pointer.xmax) {
            continue;
        }
        let end = pointer.position.checked_add(pointer.length as u64);
        if end.is_none_or(|end| end > rows_len) {
            return Err(other(format!(
                "partition {} slot {} names row bytes [{}, +{}) past its {rows_len}-byte rows file; \
                 nothing was reshaped",
                source.schema.name,
                from + i as u64,
                pointer.position,
                pointer.length
            )));
        }
        pointer.fetch_row_reuse_async(&*d.io_rows, &fetch, &mut row).await.map_err(other)?;
        let point = source.point_of_row(&row)?;
        let mut bytes = vec![0u8; pointer.length as usize];
        let mut pos = pointer.position;
        d.io_rows.read_data_into_buffer(&mut pos, &mut bytes).await?;
        out.push(RawVersion { pointer, bytes, point });
    }
    Ok((out, from + take))
}

/// Appends `versions` to `target` raw (each its row bytes, then its pointer
/// record naming where they landed, `xmin`, `xmax` and id kept), in storage
/// transactions committed durably.
async fn append(target: &mut CopyTarget, versions: Vec<RawVersion>) -> io::Result<()> {
    if versions.is_empty() {
        return Ok(());
    }
    let d = target.table.data()?;
    let mut txn = d.io_rows.begin_txn();
    for mut v in versions {
        let (pos, _) = d.io_rows.append_data_txn(txn, &v.bytes).await;
        v.pointer.position = pos;
        if let Err(e) = v.pointer.save_in_txn(&*d.io_pointers, txn).await {
            d.io_pointers.abort_txn(txn).await;
            return Err(other(e));
        }
        target.records += 1;
        target.row_bytes += v.bytes.len() as u64;
        target.crc.update(&v.bytes);
        if crate::core::wal::should_break_transaction(&*d.io_rows) {
            d.io_pointers.commit_durable(txn).await?;
            txn = d.io_rows.begin_txn();
        }
    }
    d.io_pointers.commit_durable(txn).await
}

/// Copies every version of `sources` into `targets` (sorted by range,
/// tiling the sources' span), each by its own point. Returns the records
/// copied, after verifying each target reads back what was written.
pub(super) async fn copy(sources: &[(u32, Arc<PartTable>)], targets: &mut [CopyTarget]) -> io::Result<u64> {
    let mut copied = 0u64;
    for (pid, source) in sources {
        // Sampled once, before the first read: the oldest snapshot can only
        // move on while the copy runs, so a version reclaimable now stays so.
        let horizon = source.version_horizon();
        let mut from = 0u64;
        loop {
            let (versions, next) = read_chunk(source, from, &horizon).await?;
            if next == from {
                break;
            }
            from = next;
            let mut by_target: Vec<Vec<RawVersion>> = (0..targets.len()).map(|_| Vec::new()).collect();
            for v in versions {
                let Some(t) = targets.iter().position(|t| t.range.contains(v.point)) else {
                    return Err(other(format!(
                        "partition {pid} holds a row with point {:#018x} outside every target of the reshape; \
                         nothing was reshaped",
                        v.point
                    )));
                };
                by_target[t].push(v);
            }
            for (t, versions) in by_target.into_iter().enumerate() {
                copied += versions.len() as u64;
                append(&mut targets[t], versions).await?;
            }
        }
    }
    for t in targets.iter() {
        verify(t).await?;
    }
    Ok(copied)
}

/// Reads `t` back: its pointer records number what was appended, none a
/// tombstone, and its rows file holds exactly the bytes appended (§6.2 step
/// 5: the copy started from empty files, so they are the whole file).
async fn verify(t: &CopyTarget) -> io::Result<()> {
    let d = t.table.data()?;
    let rec = ROW_POINTER_RECORD_LEN as u64;
    let pointers = d.io_pointers.get_len().await;
    let rows = d.io_rows.get_len().await;
    if pointers != t.records * rec || rows != t.row_bytes {
        return Err(other(format!(
            "a reshape target {} read back {} pointer bytes and {rows} row bytes, but {} records of {} bytes were \
             written; nothing was reshaped",
            t.table.schema.name,
            pointers,
            t.records,
            t.row_bytes
        )));
    }
    let mut crc = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut at = 0u64;
    while at < rows {
        let want = ((rows - at) as usize).min(buf.len());
        let mut pos = at;
        d.io_rows.read_data_into_buffer(&mut pos, &mut buf[..want]).await?;
        crc.update(&buf[..want]);
        at += want as u64;
    }
    if crc.finalize() != t.crc.clone().finalize() {
        return Err(other(format!(
            "a reshape target {} read back row bytes that differ from those written; nothing was reshaped",
            t.table.schema.name
        )));
    }
    Ok(())
}

/// Lands what a target staged, as an exclusive close does, so its files
/// hold every byte before the commit; one the gate refuses stays in `.tmp`,
/// which the next open lands.
pub(super) async fn land(table: &PartTable) {
    if let Ok(d) = table.data() {
        for io in [&d.io_rows, &d.io_pointers] {
            for _ in 0..3 {
                if !io.land_background().await {
                    break;
                }
            }
        }
    }
}

/// What a statement asks of a reshape, before it is planned against a map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReshapeOp {
    /// `ALTER TABLE t SPLIT PARTITION FOR (k = v, …)`: the partition holding
    /// that key, cut in two at its midpoint.
    SplitPartition { at: String },
    /// `ALTER TABLE t MERGE PARTITIONS FOR (…), FOR (…)`: two adjacent
    /// partitions made one.
    MergePartitions { first: String, second: String },
    /// `ALTER TABLE t ISOLATE KEY (k = v) [TO 'dir']`: the key's block made a
    /// partition of its own (a three-way cut at its edges).
    IsolateKey { key: String, to: Option<String> },
    /// `ALTER TABLE t SPLIT KEY (k = v) INTO n`: the block of a leading
    /// prefix cut into `n` equal pieces.
    SplitKey { key: String, into: u32 },
    /// `STORAGE MOVE PARTITION t FOR (…) TO 'dir'`.
    MovePartition { at: String, to: String },
}

impl ReshapeOp {
    /// The statement's verb, for messages.
    pub fn verb(&self) -> &'static str {
        match self {
            ReshapeOp::SplitPartition { .. } => "SPLIT PARTITION",
            ReshapeOp::MergePartitions { .. } => "MERGE PARTITIONS",
            ReshapeOp::IsolateKey { .. } => "ISOLATE KEY",
            ReshapeOp::SplitKey { .. } => "SPLIT KEY",
            ReshapeOp::MovePartition { .. } => "MOVE PARTITION",
        }
    }
}

/// A planned reshape: its sources and its targets' starts and directories
/// (`None`: the source's own location, or the dealer's for a source never
/// materialized).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshapePlan {
    pub sources: Vec<u32>,
    pub targets: Vec<(u64, Option<PathBuf>)>,
}

/// What a reshape left for the caller: the committed sources' files to
/// delete, or, on failure, the targets' files.
#[derive(Debug, Default)]
pub struct ReshapeFiles {
    /// `(pid, directory)` of each partition whose files go.
    pub doomed: Vec<(u32, PathBuf)>,
}

/// The cut points of `[lo, hi)` at `cuts` (each strictly inside, sorted),
/// as target starts.
pub(super) fn starts(lo: u64, hi: u128, cuts: &[u128]) -> Vec<u64> {
    let mut out = vec![lo];
    for &c in cuts {
        if c > lo as u128 && c < hi {
            out.push(c as u64);
        }
    }
    out.dedup();
    out
}

/// Whether `dir` is `root`.
pub(super) fn is_root(dir: &Path, root: &Path) -> bool {
    super::files::dir_key(&dir.to_string_lossy()) == super::files::dir_key(&root.to_string_lossy())
}

// ------------------------------------------------------------------ parsing

/// One token of a reshape statement: a word, a quoted identifier, a string
/// literal, or a parenthesised group with its inside as written.
#[derive(Debug, PartialEq, Eq)]
enum Tok<'a> {
    Word(&'a str),
    Quoted(String),
    Str(String),
    Group(&'a str),
    Comma,
}

/// Splits `s` into [`Tok`]s, tracking nesting and quotes (memory
/// `scanners-must-track-nesting`): a `)` inside a string literal or a quoted
/// identifier never ends a group.
fn tokens(s: &str) -> Result<Vec<Tok<'_>>, String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b',' {
            out.push(Tok::Comma);
            i += 1;
        } else if c == b'\'' || c == b'"' {
            let (text, end) = quoted(s, i)?;
            out.push(if c == b'\'' { Tok::Str(text) } else { Tok::Quoted(text) });
            i = end;
        } else if c == b'(' {
            let start = i + 1;
            let mut depth = 0usize;
            loop {
                let Some(&ch) = b.get(i) else {
                    return Err("unbalanced parentheses".to_string());
                };
                match ch {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    b'\'' | b'"' => {
                        i = quoted(s, i)?.1;
                        continue;
                    }
                    _ => {}
                }
                i += 1;
            }
            out.push(Tok::Group(&s[start..i]));
            i += 1;
        } else if c == b')' {
            return Err("unbalanced parentheses".to_string());
        } else {
            let start = i;
            while i < b.len() && !b[i].is_ascii_whitespace() && !matches!(b[i], b',' | b'(' | b')' | b'\'' | b'"' | b';') {
                i += 1;
            }
            if i == start {
                // A stray `;` at the end.
                i += 1;
                continue;
            }
            out.push(Tok::Word(&s[start..i]));
        }
    }
    Ok(out)
}

/// The quoted text starting at `s[i]` (its quote doubled inside) and the
/// index after the closing quote.
fn quoted(s: &str, i: usize) -> Result<(String, usize), String> {
    let b = s.as_bytes();
    let q = b[i];
    let mut j = i + 1;
    let mut text = String::new();
    loop {
        match b.get(j) {
            None => return Err("unterminated quoted text".to_string()),
            Some(&c) if c == q => {
                if b.get(j + 1) == Some(&q) {
                    text.push(q as char);
                    j += 2;
                    continue;
                }
                return Ok((text, j + 1));
            }
            Some(_) => {
                let ch = s[j..].chars().next().unwrap_or('\u{0}');
                text.push(ch);
                j += ch.len_utf8();
            }
        }
    }
}

fn kw(t: Option<&Tok<'_>>, word: &str) -> bool {
    matches!(t, Some(Tok::Word(w)) if w.eq_ignore_ascii_case(word))
}

fn name_of(t: Option<&Tok<'_>>) -> Option<String> {
    match t? {
        Tok::Word(w) => Some(w.to_string()),
        Tok::Quoted(q) => Some(q.clone()),
        _ => None,
    }
}

/// `FOR (…)` at `toks[i..]`: the group's inside and the index after it.
fn for_group(toks: &[Tok<'_>], i: usize, verb: &str) -> Result<(String, usize), String> {
    if !kw(toks.get(i), "FOR") {
        return Err(format!("{verb} expects FOR (<key column> = <value>, …)"));
    }
    match toks.get(i + 1) {
        Some(Tok::Group(g)) if !g.trim().is_empty() => Ok((g.trim().to_string(), i + 2)),
        _ => Err(format!("{verb} expects FOR (<key column> = <value>, …)")),
    }
}

/// `TO '<dir>'` at `toks[i..]`, if present: the directory and the index
/// after it.
fn to_dir(toks: &[Tok<'_>], i: usize, verb: &str) -> Result<(Option<String>, usize), String> {
    if !kw(toks.get(i), "TO") {
        return Ok((None, i));
    }
    match toks.get(i + 1) {
        Some(Tok::Str(d)) if !d.is_empty() => Ok((Some(d.clone()), i + 2)),
        _ => Err(format!("{verb} … TO expects a quoted storage location")),
    }
}

fn done(toks: &[Tok<'_>], i: usize, verb: &str) -> Result<(), String> {
    if i == toks.len() {
        Ok(())
    } else {
        Err(format!("unexpected text after {verb}: {:?}", toks[i]))
    }
}

/// A reshaping `ALTER TABLE`: `None` when `sql` is another statement, else
/// the table and the operation, or why the statement does not parse.
pub fn parse_alter(sql: &str) -> Option<Result<(String, ReshapeOp), String>> {
    let toks = match tokens(sql) {
        Ok(t) => t,
        Err(_) => return None,
    };
    if !kw(toks.first(), "ALTER") || !kw(toks.get(1), "TABLE") {
        return None;
    }
    let mut i = 2;
    if kw(toks.get(i), "IF") && kw(toks.get(i + 1), "EXISTS") {
        i += 2;
    }
    if kw(toks.get(i), "ONLY") {
        i += 1;
    }
    let table = name_of(toks.get(i))?;
    i += 1;
    let (a, b) = (toks.get(i), toks.get(i + 1));
    let parsed = if kw(a, "SPLIT") && kw(b, "PARTITION") {
        for_group(&toks, i + 2, "SPLIT PARTITION").and_then(|(at, j)| {
            done(&toks, j, "SPLIT PARTITION")?;
            Ok(ReshapeOp::SplitPartition { at })
        })
    } else if kw(a, "MERGE") && kw(b, "PARTITIONS") {
        for_group(&toks, i + 2, "MERGE PARTITIONS").and_then(|(first, j)| {
            if toks.get(j) != Some(&Tok::Comma) {
                return Err("MERGE PARTITIONS expects two FOR (…) clauses, separated by a comma".to_string());
            }
            let (second, k) = for_group(&toks, j + 1, "MERGE PARTITIONS")?;
            done(&toks, k, "MERGE PARTITIONS")?;
            Ok(ReshapeOp::MergePartitions { first, second })
        })
    } else if kw(a, "ISOLATE") && kw(b, "KEY") {
        match toks.get(i + 2) {
            Some(Tok::Group(g)) if !g.trim().is_empty() => to_dir(&toks, i + 3, "ISOLATE KEY").and_then(|(to, j)| {
                done(&toks, j, "ISOLATE KEY")?;
                Ok(ReshapeOp::IsolateKey { key: g.trim().to_string(), to })
            }),
            _ => Err("ISOLATE KEY expects (<key column> = <value>, …)".to_string()),
        }
    } else if kw(a, "SPLIT") && kw(b, "KEY") {
        match (toks.get(i + 2), toks.get(i + 3), toks.get(i + 4)) {
            (Some(Tok::Group(g)), into, Some(Tok::Word(n))) if kw(into, "INTO") && !g.trim().is_empty() => {
                match n.parse::<u32>() {
                    Ok(into) => done(&toks, i + 5, "SPLIT KEY").map(|()| ReshapeOp::SplitKey { key: g.trim().to_string(), into }),
                    Err(_) => Err(format!("SPLIT KEY … INTO expects a piece count, not {n}")),
                }
            }
            _ => Err("SPLIT KEY expects (<key column> = <value>, …) INTO <n>".to_string()),
        }
    } else if (kw(a, "PARTITION") && kw(b, "BY")) || (kw(a, "ATTACH") || kw(a, "DETACH")) && kw(b, "PARTITION") {
        // Conversion and PostgreSQL's attach/detach: refused by the ALTER
        // arm's own wording, below.
        return None;
    } else {
        return None;
    };
    Some(parsed.map(|op| (table, op)))
}

/// `STORAGE MOVE PARTITION t FOR (…) TO '<dir>'`: `None` when `sql` is
/// another statement.
pub fn parse_move(sql: &str) -> Option<Result<(String, ReshapeOp), String>> {
    let toks = tokens(sql).ok()?;
    if !kw(toks.first(), "STORAGE") || !kw(toks.get(1), "MOVE") || !kw(toks.get(2), "PARTITION") {
        return None;
    }
    let Some(table) = name_of(toks.get(3)) else {
        return Some(Err("STORAGE MOVE PARTITION expects a table name".to_string()));
    };
    let parsed = for_group(&toks, 4, "MOVE PARTITION").and_then(|(at, j)| {
        let (to, k) = to_dir(&toks, j, "MOVE PARTITION")?;
        let to = to.ok_or_else(|| "STORAGE MOVE PARTITION … TO expects a quoted storage location".to_string())?;
        done(&toks, k, "MOVE PARTITION")?;
        Ok(ReshapeOp::MovePartition { at, to })
    });
    Some(parsed.map(|op| (table, op)))
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    fn alter(sql: &str) -> (String, ReshapeOp) {
        parse_alter(sql).expect("a reshape").expect("parses")
    }

    #[test]
    fn every_spelling_parses() {
        assert_eq!(
            alter("ALTER TABLE t SPLIT PARTITION FOR (tenant_id = 42)"),
            ("t".into(), ReshapeOp::SplitPartition { at: "tenant_id = 42".into() })
        );
        assert_eq!(
            alter("alter table \"My T\" merge partitions for (k = 1), for (k = 2);"),
            ("My T".into(), ReshapeOp::MergePartitions { first: "k = 1".into(), second: "k = 2".into() })
        );
        assert_eq!(
            alter("ALTER TABLE t ISOLATE KEY (tenant_id = 42) TO '/mnt/nvme2'"),
            ("t".into(), ReshapeOp::IsolateKey { key: "tenant_id = 42".into(), to: Some("/mnt/nvme2".into()) })
        );
        assert_eq!(
            alter("ALTER TABLE t ISOLATE KEY (name = 'a)b')"),
            ("t".into(), ReshapeOp::IsolateKey { key: "name = 'a)b'".into(), to: None })
        );
        assert_eq!(
            alter("ALTER TABLE IF EXISTS t SPLIT KEY (tenant_id = 42) INTO 8"),
            ("t".into(), ReshapeOp::SplitKey { key: "tenant_id = 42".into(), into: 8 })
        );
        assert_eq!(
            parse_move("STORAGE MOVE PARTITION t FOR (k = 'x') TO '/d'").unwrap().unwrap(),
            ("t".into(), ReshapeOp::MovePartition { at: "k = 'x'".into(), to: "/d".into() })
        );
    }

    #[test]
    fn other_statements_are_not_reshapes() {
        for sql in [
            "ALTER TABLE t ADD COLUMN split INT",
            "ALTER TABLE t RENAME COLUMN partition TO p",
            "ALTER TABLE t PARTITION BY HASH (k)",
            "ALTER TABLE split ADD COLUMN key INT",
            "STORAGE MOVE TABLE t TO '/d'",
        ] {
            assert!(parse_alter(sql).is_none() && parse_move(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn malformed_reshapes_are_refused() {
        for sql in [
            "ALTER TABLE t SPLIT PARTITION",
            "ALTER TABLE t SPLIT PARTITION FOR ()",
            "ALTER TABLE t MERGE PARTITIONS FOR (k = 1)",
            "ALTER TABLE t MERGE PARTITIONS FOR (k = 1) FOR (k = 2)",
            "ALTER TABLE t ISOLATE KEY (k = 1) TO",
            "ALTER TABLE t SPLIT KEY (k = 1) INTO many",
            "ALTER TABLE t SPLIT KEY (k = 1)",
            "ALTER TABLE t SPLIT PARTITION FOR (k = 1) extra",
        ] {
            assert!(matches!(parse_alter(sql), Some(Err(_))), "{sql}");
        }
        assert!(matches!(parse_move("STORAGE MOVE PARTITION t FOR (k = 1)"), Some(Err(_))));
    }
}
