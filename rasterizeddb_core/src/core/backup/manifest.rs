//! The backup catalogue: one small object that describes every snapshot and
//! every archived log segment.
//!
//! # Why a manifest and not a listing
//!
//! `ListObjectsV2` returns 1000 keys per request and tells you nothing about
//! what is inside them, so planning a restore from a listing means one request
//! per thousand objects and then a `GET` per segment just to learn its LSN
//! range. A database that has been archiving for a year would spend hundreds of
//! requests deciding what to download.
//!
//! Keeping the catalogue in one object makes planning exactly one `GET`. The
//! cost is that the manifest is written on every archive flush — but that is a
//! single small `PUT` alongside an upload that was happening anyway, and it is
//! what lets `RESTORE ... AT TIME` answer without touching a segment it does
//! not end up needing.
//!
//! # Concurrency
//!
//! One server owns a prefix and rewrites the manifest under its own lock. Two
//! servers pointed at the same prefix would clobber each other's entries, which
//! is why [`super::BackupService`] refuses to start when the prefix already
//! carries a different instance's id.

use serde::{Deserialize, Serialize};

/// Bumped when a field's meaning changes. A reader that does not recognise the
/// version refuses the manifest rather than restoring from half of it.
///
/// 2: a snapshot may keep some of its files as separate objects, possibly
/// written by an *earlier* snapshot ([`SnapshotEntry::objects`]). A version-1
/// reader would restore only the snapshot's own archive and silently leave
/// those files out — so a catalogue that uses them is written as version 2,
/// which such a reader refuses, and one that does not stays at 1 and remains
/// readable by older builds. See [`Manifest::required_version`].
pub const MANIFEST_VERSION: u32 = 2;

/// Key of the catalogue within the configured prefix.
pub const MANIFEST_KEY: &str = "MANIFEST.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    /// Identifies the database instance that owns this prefix.
    pub instance_id: String,
    /// The directory the snapshots were taken from.
    ///
    /// Recorded because a restore has to tell an in-place restore from a clone,
    /// and because the storage pool file inside a snapshot names absolute paths
    /// that are only true at the source — see
    /// [`crate::core::storage::repoint_pool_config`]. Empty for a catalogue
    /// written before it was recorded.
    #[serde(default)]
    pub source_location: String,
    /// Unix seconds of the last write.
    pub updated_at: u64,
    /// Unix seconds the last rollover finished, or 0 for a catalogue that has
    /// never had one.
    ///
    /// A rollover leaves an ordinary snapshot behind, so nothing in the lists
    /// below tells one apart after the fact — and the schedule has to survive a
    /// restart, or a server restarted more often than the rollover interval
    /// never reaches one. Catalogues written before rollovers were scheduled
    /// have no such field; they read as 0 and the schedule falls back to the
    /// newest snapshot, which is the closest thing to a last rollover they
    /// carry.
    #[serde(default)]
    pub last_rollover_at: u64,
    /// The lowest sequence a newly archived bundle may take.
    ///
    /// Retention deletes entries, so deriving the next sequence from
    /// `max(sequence) + 1` over the list alone rewinds once the bundles a
    /// snapshot no longer needs are gone. A rewound counter would hand a *new*
    /// bundle a sequence below the floor a surviving snapshot records, and a
    /// restore would then look straight past it. Kept as a high-water mark
    /// instead: it only ever goes up.
    #[serde(default)]
    pub segment_sequence_high: u64,
    #[serde(default)]
    pub snapshots: Vec<SnapshotEntry>,
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    pub fn new(instance_id: String) -> Self {
        Self {
            version: MANIFEST_VERSION,
            instance_id,
            source_location: String::new(),
            updated_at: super::timeutil::now_unix(),
            last_rollover_at: 0,
            segment_sequence_high: 0,
            snapshots: Vec::new(),
            segments: Vec::new(),
        }
    }

    pub fn decode(bytes: &[u8]) -> std::io::Result<Self> {
        Self::decode_understanding(bytes, MANIFEST_VERSION)
    }

    /// `decode` as a build that understands versions up to `understood` would
    /// run it. Separate so a test can play the part of an older reader.
    pub fn decode_understanding(bytes: &[u8], understood: u32) -> std::io::Result<Self> {
        let manifest: Manifest = serde_json::from_slice(bytes)
            .map_err(|e| std::io::Error::other(format!("backup manifest is unreadable: {e}")))?;
        if manifest.version > understood {
            return Err(std::io::Error::other(format!(
                "backup manifest is version {} but this build understands {}",
                manifest.version, understood
            )));
        }
        Ok(manifest)
    }

    /// The lowest version that describes this catalogue faithfully: 2 once any
    /// snapshot keeps files outside its archive, 1 otherwise — so a server that
    /// never takes an incremental snapshot keeps writing catalogues an older
    /// build can still restore from.
    pub fn required_version(&self) -> u32 {
        if self.snapshots.iter().any(|s| !s.objects.is_empty()) {
            2
        } else {
            1
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        // The version is derived, never carried over: a catalogue that gained
        // its first incremental snapshot must stop being readable by a build
        // that would restore it without those files.
        let mut stamped = self.clone();
        stamped.version = self.required_version();
        // Pretty-printed: it is read by operators at least as often as by the
        // engine, and the size difference on an object this small is noise.
        serde_json::to_vec_pretty(&stamped).expect("a manifest is always serialisable")
    }

    /// Every content object any snapshot can reuse, by content hash.
    ///
    /// A snapshot reuses an object only if its bytes hash the same as the file
    /// it is copying *and* have the same length, so an object found here is
    /// byte-for-byte what a fresh upload would have stored.
    pub fn reusable_objects(&self) -> std::collections::HashMap<(String, u64), String> {
        let mut out = std::collections::HashMap::new();
        for snapshot in &self.snapshots {
            for object in &snapshot.objects {
                out.entry((object.sha256.clone(), object.len))
                    .or_insert_with(|| object.key.clone());
            }
        }
        out
    }

    /// Snapshots newest first.
    ///
    /// Ordered by `sequence`, which is monotonic; the timestamp and commit
    /// sequence only break ties among entries written before `sequence`
    /// existed. Sorting on `finished_at` alone is wrong — see
    /// [`SnapshotEntry::sequence`].
    pub fn snapshots_newest_first(&self) -> Vec<&SnapshotEntry> {
        let mut ordered: Vec<&SnapshotEntry> = self.snapshots.iter().collect();
        ordered.sort_by_key(|s| std::cmp::Reverse((s.sequence, s.finished_at, s.end_lsn)));
        ordered
    }

    /// The sequence a new snapshot should take.
    pub fn next_snapshot_sequence(&self) -> u64 {
        self.snapshots
            .iter()
            .map(|s| s.sequence + 1)
            .max()
            .unwrap_or(0)
    }

    /// The sequence a newly archived bundle should take.
    ///
    /// Past the high-water mark as well as past everything still listed, so
    /// pruning cannot make the counter repeat itself.
    pub fn next_segment_sequence(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| s.sequence + 1)
            .max()
            .unwrap_or(0)
            .max(self.segment_sequence_high)
    }

    /// Records an archived bundle and moves the high-water mark past it.
    ///
    /// The only way a segment should enter the catalogue — pushing onto
    /// `segments` directly leaves the counter free to rewind on the next prune.
    pub fn record_segment(&mut self, segment: SegmentEntry) {
        self.segment_sequence_high = self.segment_sequence_high.max(segment.sequence + 1);
        self.segments.push(segment);
    }

    /// The newest snapshot that is a legal starting point for `target`.
    ///
    /// A snapshot can only serve a recovery target at or after the point it
    /// finished: its files are a fuzzy copy made consistent by replaying the
    /// log through `end_lsn`, so stopping short of that leaves the copy
    /// half-updated. Recovering to an *earlier* point needs an earlier
    /// snapshot, which is the whole reason more than one is retained.
    pub fn snapshot_for(&self, target: &RecoveryTarget) -> Option<&SnapshotEntry> {
        let usable = |s: &&SnapshotEntry| match target {
            RecoveryTarget::Latest => true,
            RecoveryTarget::Lsn(lsn) => s.end_lsn <= *lsn,
            RecoveryTarget::Time(unix) => s.finished_at <= *unix,
            RecoveryTarget::Snapshot(id) => &s.id == id,
        };
        self.snapshots_newest_first()
            .into_iter()
            .find(|s| usable(s))
    }

    /// One log's bundles, in the order they were cut.
    ///
    /// By sequence alone, which is the order the bundles were archived in and
    /// therefore the order their records were written in. Commit sequences
    /// cannot order these: each database counts its own.
    pub fn segments_for(&self, database: &str) -> Vec<&SegmentEntry> {
        let mut ordered: Vec<&SegmentEntry> = self
            .segments
            .iter()
            .filter(|s| s.database == database)
            .collect();
        ordered.sort_by_key(|s| s.sequence);
        ordered
    }

    /// Every log the catalogue holds bundles for, root first.
    pub fn databases_with_segments(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for segment in &self.segments {
            if !names.contains(&segment.database) {
                names.push(segment.database.clone());
            }
        }
        names.sort();
        names
    }

    /// Segments in log order.
    pub fn segments_ordered(&self) -> Vec<&SegmentEntry> {
        let mut ordered: Vec<&SegmentEntry> = self.segments.iter().collect();
        // `first_lsn` is assigned by a single monotonic counter, so it totally
        // orders segments. A segment holding no commit at all reports zero for
        // both bounds and is ordered by the sequence it was cut at instead.
        ordered.sort_by_key(|s| (s.first_lsn, s.sequence));
        ordered
    }
}

/// One snapshot: a whole database directory in a single compressed object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub id: String,
    /// Order this snapshot was taken in.
    ///
    /// Wall-clock time cannot order snapshots on its own: `finished_at` has
    /// one-second granularity, so two snapshots taken inside the same second
    /// compare equal, and a stable sort then leaves the *oldest* first — which
    /// would have retention delete the newest and `LATEST` resolve to an older
    /// one. A counter that only ever goes up removes the question, and it also
    /// survives a clock stepped backwards by NTP.
    #[serde(default)]
    pub sequence: u64,
    pub key: String,
    /// Unix seconds when the copy started and when it finished. The window
    /// between them is the span whose log records the restore must replay.
    pub started_at: u64,
    pub finished_at: u64,
    /// Commit sequence at the moment the copy started.
    ///
    /// Of the database at the location itself. Every log's window, that one
    /// included, is in [`Self::windows`]; these two fields stay because they
    /// are what catalogues written before sub-database logs were archived
    /// carry, and what a reader of an older build understands.
    pub start_lsn: u64,
    /// Commit sequence at the moment the copy finished. Replaying the log
    /// through here is what makes the fuzzy copy consistent.
    pub end_lsn: u64,
    /// Sequence of the first archived bundle that can hold anything this
    /// snapshot needs: every bundle in its copy window has `sequence >= this`.
    ///
    /// Recorded because the commit sequence cannot answer the question. See
    /// [`segment_needed_by`]. `None` for catalogues written before this field
    /// existed.
    #[serde(default)]
    pub first_segment_sequence: Option<u64>,
    /// The copy window of every log under the location, the root's included.
    ///
    /// Each database created with `CREATE DATABASE` keeps its own log and its
    /// own commit sequence, so one pair of numbers cannot describe them. Empty
    /// for a catalogue written before they were archived, in which case only
    /// the root's window is known.
    #[serde(default)]
    pub windows: Vec<LogWindow>,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub file_count: u32,
    /// zstd level the object was written at, recorded for diagnosis only —
    /// decompression does not need to be told.
    pub compression_level: i32,
    /// Files the snapshot holds, for `SHOW BACKUPS` and for telling an
    /// operator what a restore is about to produce.
    #[serde(default)]
    pub files: Vec<String>,
    /// Files kept as objects of their own rather than inside `key`'s archive.
    ///
    /// An incremental snapshot stores a large file this way, and a later
    /// snapshot whose copy of the file hashes the same references the same
    /// object instead of uploading it again — so `key` here may name an
    /// object an *earlier* snapshot wrote. Retention therefore deletes an
    /// object only once no surviving snapshot references it
    /// ([`Manifest::plan_retention`]). Empty for a full snapshot, and for
    /// every snapshot written before this existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<FileObject>,
}

/// One file of a snapshot stored as its own compressed object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileObject {
    /// Relative path in the database directory, as in [`SnapshotEntry::files`].
    pub name: String,
    /// Uncompressed length.
    pub len: u64,
    /// Hex SHA-256 of the uncompressed bytes. Checked on restore.
    pub sha256: String,
    /// The object holding the zstd-compressed bytes.
    pub key: String,
}

/// What one log had to be replayed through for a snapshot to be consistent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogWindow {
    /// `""` for the database at the location itself, otherwise the
    /// subdirectory it lives in.
    pub database: String,
    /// Commit sequence when the copy started and when it finished. Replay
    /// through `end_lsn` is what makes this database's files consistent.
    pub start_lsn: u64,
    pub end_lsn: u64,
}

/// A bundle of archived log segments in a single compressed object.
///
/// Segments are bundled rather than uploaded one at a time because a busy
/// database cuts one per checkpoint, and a `PUT` each would make the request
/// count scale with write volume instead of with time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// Order the bundle was cut in, which breaks ties between empty segments.
    ///
    /// Allocated from one counter across every log in the installation, so a
    /// snapshot's [`SnapshotEntry::first_segment_sequence`] bounds all of them
    /// at once.
    pub sequence: u64,
    /// Which log this bundle was cut from: `""` for the database at the
    /// location itself, otherwise the subdirectory of the database created
    /// with `CREATE DATABASE`.
    ///
    /// Bundles are never concatenated across this boundary. Each database has
    /// its own log, its own commit sequence, and — because a file id is a hash
    /// of the file's *name* — its own id space: two databases both holding
    /// `users.db` name it identically, so replaying one's records against the
    /// other's files would write the wrong bytes into the wrong table.
    #[serde(default)]
    pub database: String,
    pub key: String,
    pub archived_at: u64,
    /// Lowest and highest commit sequence any record in the bundle belongs to.
    /// Zero for a bundle that contains no commit marker at all.
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    /// How many cut segments the bundle holds, concatenated in log order.
    pub segment_count: u32,
}

impl SnapshotEntry {
    /// This snapshot's copy window for one log.
    ///
    /// The root's window falls back to `start_lsn`/`end_lsn`, which is where a
    /// catalogue written before `windows` existed keeps it.
    pub fn window_for(&self, database: &str) -> Option<LogWindow> {
        if let Some(window) = self.windows.iter().find(|w| w.database == database) {
            return Some(window.clone());
        }
        if database.is_empty() {
            return Some(LogWindow {
                database: String::new(),
                start_lsn: self.start_lsn,
                end_lsn: self.end_lsn,
            });
        }
        None
    }
}

/// What a retention pass decided.
pub struct RetentionPlan {
    /// Snapshots to keep, newest first.
    pub snapshots: Vec<SnapshotEntry>,
    /// Segment bundles to keep, in log order.
    pub segments: Vec<SegmentEntry>,
    /// Object keys to delete.
    pub doomed: Vec<String>,
}

impl Manifest {
    /// Decides what retention would remove, without removing it.
    ///
    /// Separated from the deletion so the rule can be tested without a network:
    /// this is the one function in the feature whose bug is silent. Deleting a
    /// segment that a surviving snapshot still needs does not fail anything at
    /// the time — it produces a catalogue that looks healthy and a restore that
    /// cannot complete, discovered only when someone needs it.
    ///
    /// The rule: keep the `keep` newest snapshots, then keep every segment any
    /// one of them might replay — see [`segment_needed_by`] for which those are
    /// and why the bundle sequence rather than the commit sequence decides it.
    ///
    /// `keep` of 0 means unlimited and removes nothing.
    pub fn plan_retention(&self, keep: usize) -> RetentionPlan {
        let mut ordered: Vec<SnapshotEntry> =
            self.snapshots_newest_first().into_iter().cloned().collect();

        if keep == 0 || ordered.len() <= keep {
            return RetentionPlan {
                snapshots: ordered,
                segments: self.segments_ordered().into_iter().cloned().collect(),
                doomed: Vec::new(),
            };
        }

        let retired: Vec<SnapshotEntry> = ordered.split_off(keep);
        let survivors = ordered;

        // An object goes only when no survivor references it: a newer
        // incremental snapshot may be built on objects an older one wrote.
        let still_referenced: std::collections::HashSet<&str> = survivors
            .iter()
            .flat_map(|s| s.objects.iter().map(|o| o.key.as_str()))
            .collect();
        let mut doomed: Vec<String> = Vec::new();
        for snapshot in &retired {
            for object in &snapshot.objects {
                if !still_referenced.contains(object.key.as_str()) && !doomed.contains(&object.key) {
                    doomed.push(object.key.clone());
                }
            }
        }
        doomed.extend(retired.into_iter().map(|s| s.key));

        let (segments, dropped): (Vec<SegmentEntry>, Vec<SegmentEntry>) = self
            .segments_ordered()
            .into_iter()
            .cloned()
            // Asked of every survivor rather than only the oldest: under the
            // sequence rule the oldest's floor is the lowest anyway, and under
            // the fallback a catalogue whose commit sequences restarted has no
            // one snapshot that reaches furthest back. Keeping what any of them
            // might need is the direction that cannot strand a restore.
            .partition(|s| {
                survivors
                    .iter()
                    .any(|snapshot| segment_needed_by(snapshot, s))
            });

        doomed.extend(dropped.into_iter().map(|s| s.key));

        RetentionPlan {
            snapshots: survivors,
            segments,
            doomed,
        }
    }
}

/// Whether `segment` can hold anything the copy window of `snapshot` needs.
///
/// The one rule both retention and restore ask, so they cannot disagree about
/// which bundles belong to a snapshot — a disagreement here deletes a bundle a
/// restore still downloads, or replays one it should not.
///
/// By bundle sequence, which the catalogue assigns and only ever increases.
/// Deliberately *not* by commit sequence: that counter lives in the log, and a
/// checkpoint discards the log, so a cleanly restarted server begins again at
/// zero. Two things follow, and both were observed in a live catalogue. A
/// snapshot taken before the first commit of a run records `start_lsn` 0, and
/// every bundle ever archived then compares as still needed — which is why a
/// rollover deleted the older snapshots and kept every segment behind them.
/// And a bundle written before the restart compares as *newer* than the
/// snapshot, so a restore replays records from an earlier life of the database
/// over the copy.
///
/// A snapshot written before the sequence was recorded has `None`, and falls
/// back to the commit sequence. That is the rule that over-keeps rather than
/// the one that deletes: those catalogues stay restorable, and the first
/// snapshot taken after an upgrade carries a sequence that lets retention
/// clear what they left behind.
pub fn segment_needed_by(snapshot: &SnapshotEntry, segment: &SegmentEntry) -> bool {
    match snapshot.first_segment_sequence {
        Some(floor) => segment.sequence >= floor,
        // A bundle holding no commit at all reports zero and is kept: it costs
        // almost nothing, and dropping it could strand records whose commit
        // marker landed in a neighbouring bundle.
        None => segment.last_lsn == 0 || segment.last_lsn >= snapshot.start_lsn,
    }
}

/// Where a restore should stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryTarget {
    /// Everything that has been archived.
    Latest,
    /// Through this commit sequence, inclusive. Exact: the cut is taken on the
    /// commit marker, so a transaction is wholly in or wholly out.
    Lsn(u64),
    /// Through the last archive flush at or before this instant.
    ///
    /// Coarser than an LSN by construction. Log records carry no timestamp —
    /// stamping each one would cost a clock read on the write path — so time is
    /// only known at the granularity of a bundle, and the target resolves to
    /// the end of the last bundle archived at or before it.
    Time(u64),
    /// This snapshot exactly, with no log replay beyond making it consistent.
    Snapshot(String),
}

impl RecoveryTarget {
    /// The commit sequence a restore should replay the root database through.
    ///
    /// `None` means "no ceiling" — replay everything available.
    pub fn resolve_lsn(&self, manifest: &Manifest, snapshot: &SnapshotEntry) -> Option<u64> {
        self.resolve_lsn_for(manifest, snapshot, "")
    }

    /// The commit sequence a restore should replay one database through.
    ///
    /// Each database counts its own commits, so a target cannot name one number
    /// that means the same thing to all of them. Only the shapes that *are*
    /// shared translate: a snapshot names each database's own window end, and
    /// an instant is an instant everywhere. `AT LSN` names a commit of the
    /// database at the location itself, and reaches the others through the
    /// moment that commit was archived — the same granularity `AT TIME` has,
    /// and the same one PostgreSQL's `recovery_target_time` has.
    pub fn resolve_lsn_for(
        &self,
        manifest: &Manifest,
        snapshot: &SnapshotEntry,
        database: &str,
    ) -> Option<u64> {
        // Below its window end a database's copy is not yet consistent, so a
        // target that lands earlier still has to replay that far.
        let window_end = snapshot
            .window_for(database)
            .map(|w| w.end_lsn)
            .unwrap_or(0);

        match self {
            RecoveryTarget::Latest => None,
            RecoveryTarget::Snapshot(_) => Some(window_end),
            RecoveryTarget::Time(unix) => {
                let ceiling = manifest
                    .segments_for(database)
                    .into_iter()
                    .filter(|s| s.archived_at <= *unix)
                    .map(|s| s.last_lsn)
                    .max()
                    .unwrap_or(0);
                Some(ceiling.max(window_end))
            }
            RecoveryTarget::Lsn(lsn) if database.is_empty() => Some(*lsn),
            RecoveryTarget::Lsn(lsn) => {
                // The instant the root's ceiling reached S3. A target past
                // everything the root archived has no instant to stand on and
                // means "as far as this database goes", which is `None`.
                match manifest
                    .segments_for("")
                    .into_iter()
                    .filter(|s| s.last_lsn != 0 && s.last_lsn >= *lsn)
                    .map(|s| s.archived_at)
                    .min()
                {
                    Some(at) => {
                        RecoveryTarget::Time(at).resolve_lsn_for(manifest, snapshot, database)
                    }
                    None => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(id: &str, finished_at: u64, end_lsn: u64) -> SnapshotEntry {
        // Sequence derived from the LSN so the fixtures stay in the order the
        // tests read them as being in.
        SnapshotEntry {
            id: id.to_string(),
            sequence: end_lsn / 100,
            key: format!("snapshots/{id}.rdba.zst"),
            started_at: finished_at - 10,
            finished_at,
            start_lsn: end_lsn.saturating_sub(50),
            end_lsn,
            first_segment_sequence: None,
            windows: Vec::new(),
            compressed_bytes: 1,
            uncompressed_bytes: 2,
            file_count: 1,
            compression_level: 10,
            files: Vec::new(),
            objects: Vec::new(),
        }
    }

    fn object(name: &str, key: &str) -> FileObject {
        FileObject {
            name: name.to_string(),
            len: 10,
            sha256: format!("hash-of-{key}"),
            key: key.to_string(),
        }
    }

    /// Retiring a snapshot must not delete an object a survivor still
    /// references — the newer incremental snapshot was built on it.
    #[test]
    fn retention_keeps_objects_a_survivor_references() {
        let mut m = manifest();
        let mut old = sequenced("old", 1_000, 0);
        old.objects = vec![object("a.db", "snapshots/old/0.zst"), object("b.db", "snapshots/old/1.zst")];
        let mut new = sequenced("new", 2_000, 1);
        // `a.db` unchanged: reused. `b.db` changed: uploaded afresh.
        new.objects = vec![object("a.db", "snapshots/old/0.zst"), object("b.db", "snapshots/new/1.zst")];
        m.snapshots = vec![old, new];

        let plan = m.plan_retention(1);
        assert_eq!(plan.snapshots.len(), 1);
        assert!(plan.doomed.contains(&"snapshots/old/1.zst".to_string()));
        assert!(plan.doomed.contains(&"snapshots/old.rdba.zst".to_string()));
        assert!(
            !plan.doomed.contains(&"snapshots/old/0.zst".to_string()),
            "deleted an object the surviving snapshot restores from: {:?}",
            plan.doomed
        );
        assert!(!plan.doomed.contains(&"snapshots/new/1.zst".to_string()));
    }

    /// A catalogue with incremental snapshots must be refused by a reader that
    /// does not know about them; one without stays readable by it.
    #[test]
    fn an_old_reader_refuses_only_catalogues_it_would_misread() {
        let mut m = manifest();
        let plain = m.encode();
        assert!(Manifest::decode_understanding(&plain, 1).is_ok());

        m.snapshots[0].objects = vec![object("a.db", "snapshots/s1/0.zst")];
        let bytes = m.encode();
        let err = Manifest::decode_understanding(&bytes, 1).unwrap_err();
        assert!(err.to_string().contains("version 2"), "{err}");
        let back = Manifest::decode(&bytes).unwrap();
        assert_eq!(back.snapshots[0].objects, m.snapshots[0].objects);
    }

    fn manifest() -> Manifest {
        let mut m = Manifest::new("test".to_string());
        m.snapshots = vec![
            snapshot("s1", 1_000, 100),
            snapshot("s2", 2_000, 200),
            snapshot("s3", 3_000, 300),
        ];
        m.segments = vec![
            SegmentEntry {
                sequence: 0,
                database: String::new(),
                key: "wal/0.zst".to_string(),
                archived_at: 1_500,
                first_lsn: 101,
                last_lsn: 150,
                compressed_bytes: 1,
                uncompressed_bytes: 2,
                segment_count: 1,
            },
            SegmentEntry {
                sequence: 1,
                database: String::new(),
                key: "wal/1.zst".to_string(),
                archived_at: 2_500,
                first_lsn: 151,
                last_lsn: 250,
                compressed_bytes: 1,
                uncompressed_bytes: 2,
                segment_count: 1,
            },
        ];
        m
    }

    #[test]
    fn latest_target_picks_the_newest_snapshot() {
        let m = manifest();
        let picked = m.snapshot_for(&RecoveryTarget::Latest).unwrap();
        assert_eq!(picked.id, "s3");
        assert_eq!(RecoveryTarget::Latest.resolve_lsn(&m, picked), None);
    }

    #[test]
    fn lsn_target_picks_a_snapshot_that_finished_before_it() {
        let m = manifest();
        let picked = m.snapshot_for(&RecoveryTarget::Lsn(250)).unwrap();
        assert_eq!(picked.id, "s2");
        assert_eq!(RecoveryTarget::Lsn(250).resolve_lsn(&m, picked), Some(250));
    }

    #[test]
    fn a_target_before_every_snapshot_has_no_starting_point() {
        let m = manifest();
        assert!(m.snapshot_for(&RecoveryTarget::Lsn(50)).is_none());
    }

    #[test]
    fn time_target_resolves_to_a_segment_boundary() {
        let m = manifest();
        let picked = m.snapshot_for(&RecoveryTarget::Time(2_600)).unwrap();
        assert_eq!(picked.id, "s2");
        // Both bundles were archived by 2600, so replay runs to the later one.
        assert_eq!(
            RecoveryTarget::Time(2_600).resolve_lsn(&m, picked),
            Some(250)
        );
    }

    #[test]
    fn time_target_never_stops_before_its_snapshot_is_consistent() {
        let m = manifest();
        // Nothing was archived by 1100, but s1 still needs replay to its own
        // end_lsn for its fuzzy copy to be usable.
        let picked = m.snapshot_for(&RecoveryTarget::Time(1_100)).unwrap();
        assert_eq!(picked.id, "s1");
        assert_eq!(
            RecoveryTarget::Time(1_100).resolve_lsn(&m, picked),
            Some(100)
        );
    }

    #[test]
    fn retention_keeps_the_newest_snapshots() {
        let m = manifest();
        let plan = m.plan_retention(2);
        assert_eq!(
            plan.snapshots
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s3", "s2"]
        );
        assert!(plan.doomed.contains(&"snapshots/s1.rdba.zst".to_string()));
    }

    #[test]
    fn snapshots_taken_in_the_same_second_still_order_newest_first() {
        // The regression this guards: `finished_at` is whole seconds, so a
        // burst of snapshots compares equal on it. Ordering by the timestamp
        // alone left them oldest-first, which made retention delete the newest
        // snapshots and `LATEST` resolve to an older one.
        let mut m = Manifest::new("test".to_string());
        for i in 0..4u64 {
            let mut s = snapshot(&format!("s{i}"), 1_000, 100 + i);
            s.sequence = i;
            m.snapshots.push(s);
        }

        assert_eq!(
            m.snapshots_newest_first()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s3", "s2", "s1", "s0"]
        );

        let plan = m.plan_retention(2);
        assert_eq!(
            plan.snapshots
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s3", "s2"],
            "retention must keep the newest, not the oldest"
        );

        assert_eq!(m.snapshot_for(&RecoveryTarget::Latest).unwrap().id, "s3");
    }

    #[test]
    fn a_new_snapshot_takes_the_next_sequence() {
        let m = manifest();
        let highest = m.snapshots.iter().map(|s| s.sequence).max().unwrap();
        assert_eq!(m.next_snapshot_sequence(), highest + 1);
        assert_eq!(Manifest::new("t".to_string()).next_snapshot_sequence(), 0);
    }

    #[test]
    fn retention_of_zero_removes_nothing() {
        let m = manifest();
        let plan = m.plan_retention(0);
        assert!(plan.doomed.is_empty());
        assert_eq!(plan.snapshots.len(), 3);
        assert_eq!(plan.segments.len(), 2);
    }

    #[test]
    fn retention_above_the_count_removes_nothing() {
        let m = manifest();
        assert!(m.plan_retention(10).doomed.is_empty());
    }

    #[test]
    fn retention_drops_only_segments_no_survivor_can_reach() {
        // Keeping just s3, whose window starts at 250: the bundle covering
        // 151..250 reaches it and stays, the one covering 101..150 does not and
        // goes. Getting this backwards is the failure that leaves a
        // healthy-looking catalogue and a restore that cannot complete.
        let m = manifest();
        let plan = m.plan_retention(1);

        assert!(
            plan.segments.iter().any(|s| s.key == "wal/1.zst"),
            "the bundle covering the surviving snapshot's window must be kept"
        );
        assert!(
            plan.doomed.contains(&"wal/0.zst".to_string()),
            "a bundle no survivor can reach should be removed"
        );
    }

    #[test]
    fn retention_keeps_a_bundle_that_ends_exactly_on_the_floor() {
        // s2 survives with start_lsn 150 and wal/0 ends at exactly 150, so it
        // holds a commit inside s2's window. An exclusive comparison here would
        // silently drop the first commit every restore needs.
        let m = manifest();
        let plan = m.plan_retention(2);
        assert!(
            plan.segments.iter().any(|s| s.key == "wal/0.zst"),
            "the boundary bundle is inside the surviving window and must be kept"
        );
        assert!(plan.doomed.iter().all(|k| !k.starts_with("wal/")));
    }

    #[test]
    fn retention_never_strands_a_surviving_snapshot() {
        // The property that matters, stated directly: whatever retention keeps,
        // every surviving snapshot must still find the segments its own copy
        // window needs.
        let m = manifest();
        for keep in 1..=3 {
            let plan = m.plan_retention(keep);
            for snapshot in &plan.snapshots {
                let covered = plan
                    .segments
                    .iter()
                    .any(|seg| seg.last_lsn == 0 || seg.last_lsn >= snapshot.start_lsn);
                assert!(
                    covered || snapshot.end_lsn == snapshot.start_lsn,
                    "keep={keep}: snapshot {} lost the segments it needs",
                    snapshot.id
                );
            }
        }
    }

    #[test]
    fn retention_keeps_a_bundle_that_holds_no_commit() {
        let mut m = manifest();
        m.segments.push(SegmentEntry {
            sequence: 2,
            database: String::new(),
            key: "wal/empty.zst".to_string(),
            archived_at: 400,
            first_lsn: 0,
            last_lsn: 0,
            compressed_bytes: 1,
            uncompressed_bytes: 2,
            segment_count: 1,
        });

        let plan = m.plan_retention(1);
        assert!(!plan.doomed.contains(&"wal/empty.zst".to_string()));
    }

    #[test]
    fn retention_leaves_the_catalogue_self_consistent() {
        // Nothing kept may reference an object that was deleted, and nothing
        // deleted may still be listed.
        let m = manifest();
        let plan = m.plan_retention(1);

        for snapshot in &plan.snapshots {
            assert!(
                !plan.doomed.contains(&snapshot.key),
                "{} was kept and deleted",
                snapshot.id
            );
        }
        for segment in &plan.segments {
            assert!(
                !plan.doomed.contains(&segment.key),
                "{} was kept and deleted",
                segment.key
            );
        }
    }

    #[test]
    fn round_trips_through_json() {
        let m = manifest();
        let decoded = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(decoded.snapshots.len(), 3);
        assert_eq!(decoded.segments.len(), 2);
    }

    #[test]
    fn refuses_a_future_version() {
        let mut m = manifest();
        m.version = MANIFEST_VERSION + 1;
        // Serialised directly: `encode` derives the version from the content.
        let err = Manifest::decode(&serde_json::to_vec(&m).unwrap()).unwrap_err();
        assert!(err.to_string().contains("this build understands"), "{err}");
    }

    /// A snapshot whose window is bounded by bundle sequence rather than by
    /// commit sequence.
    fn sequenced(id: &str, finished_at: u64, first_segment: u64) -> SnapshotEntry {
        let mut s = snapshot(id, finished_at, 0);
        s.first_segment_sequence = Some(first_segment);
        s
    }

    #[test]
    fn rollover_removes_the_bundles_behind_the_snapshot_it_keeps() {
        // The live failure this comes from: a rollover kept its new snapshot,
        // deleted the older ones, and left every archived bundle in the bucket.
        // Both snapshots reported `start_lsn` 0 — the log had been checkpointed
        // and the server restarted, so the commit sequence began again — and
        // `last_lsn >= 0` is true of every bundle ever written.
        let mut m = manifest();
        m.snapshots = vec![sequenced("old", 1_000, 0), sequenced("fresh", 3_000, 2)];

        let plan = m.plan_retention(1);

        assert_eq!(plan.snapshots.len(), 1);
        assert_eq!(plan.snapshots[0].id, "fresh");
        assert!(
            plan.segments.is_empty(),
            "every bundle predates the surviving snapshot and should be gone"
        );
        for key in ["wal/0.zst", "wal/1.zst", "snapshots/old.rdba.zst"] {
            assert!(
                plan.doomed.contains(&key.to_string()),
                "{key} should have been removed"
            );
        }
    }

    #[test]
    fn a_sequence_floor_keeps_the_window_it_names() {
        let mut m = manifest();
        // The surviving snapshot's window opens at bundle 1, so bundle 0 goes
        // and bundle 1 stays — regardless of what the commit sequences say,
        // which here would keep both.
        m.snapshots = vec![sequenced("old", 1_000, 0), sequenced("fresh", 3_000, 1)];

        let plan = m.plan_retention(1);

        assert_eq!(
            plan.segments
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["wal/1.zst"]
        );
        assert!(plan.doomed.contains(&"wal/0.zst".to_string()));
    }

    #[test]
    fn a_catalogue_without_sequences_still_retains_by_commit() {
        // Snapshots written before the floor existed keep the old rule, so an
        // upgrade cannot delete a bundle their restore would download.
        let m = manifest();
        let plan = m.plan_retention(1);
        assert!(plan.segments.iter().any(|s| s.key == "wal/1.zst"));
        assert!(plan.doomed.contains(&"wal/0.zst".to_string()));
    }

    #[test]
    fn pruning_every_bundle_does_not_rewind_the_sequence() {
        // The trap in bounding a window by sequence: if the counter came from
        // `max(sequence) + 1` over a list retention had just emptied, the next
        // bundle would take 0 again, land below the surviving snapshot's floor,
        // and be invisible to the restore that needs it.
        let mut m = manifest();
        m.snapshots = vec![sequenced("old", 1_000, 0), sequenced("fresh", 3_000, 2)];
        m.segment_sequence_high = 2;

        let plan = m.plan_retention(1);
        assert!(plan.segments.is_empty());

        m.snapshots = plan.snapshots;
        m.segments = plan.segments;
        assert_eq!(
            m.next_segment_sequence(),
            2,
            "the counter must not repeat a sequence"
        );
    }

    #[test]
    fn recording_a_segment_moves_the_high_water_mark() {
        let mut m = Manifest::new("test".to_string());
        m.record_segment(SegmentEntry {
            sequence: 7,
            database: String::new(),
            key: "wal/7.zst".to_string(),
            archived_at: 1,
            first_lsn: 1,
            last_lsn: 2,
            compressed_bytes: 1,
            uncompressed_bytes: 2,
            segment_count: 1,
        });
        assert_eq!(m.segment_sequence_high, 8);
        assert_eq!(m.next_segment_sequence(), 8);
    }

    #[test]
    fn a_sequence_floor_survives_a_round_trip_through_json() {
        let mut m = manifest();
        m.snapshots = vec![sequenced("fresh", 3_000, 4)];
        m.segment_sequence_high = 9;

        let back = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(back.snapshots[0].first_segment_sequence, Some(4));
        assert_eq!(back.segment_sequence_high, 9);
    }

    #[test]
    fn a_catalogue_written_before_the_floor_existed_still_decodes() {
        // Exactly what is sitting in a live bucket right now: no
        // `first_segment_sequence`, no `segment_sequence_high`.
        let json = br#"{
            "version": 1,
            "instance_id": "rdb-old",
            "updated_at": 10,
            "snapshots": [{
                "id": "s1", "sequence": 0, "key": "snapshots/s1.rdba.zst",
                "started_at": 1, "finished_at": 2,
                "start_lsn": 0, "end_lsn": 0,
                "compressed_bytes": 1, "uncompressed_bytes": 2,
                "file_count": 1, "compression_level": 10
            }],
            "segments": []
        }"#;

        let m = Manifest::decode(json).unwrap();
        assert_eq!(m.snapshots[0].first_segment_sequence, None);
        assert_eq!(m.segment_sequence_high, 0);
        assert_eq!(m.last_rollover_at, 0);
    }

    /// A catalogue holding two logs: the root's and one created database's.
    fn two_databases() -> Manifest {
        let mut m = Manifest::new("test".to_string());
        let mut snap = snapshot("s1", 3_000, 300);
        snap.first_segment_sequence = Some(0);
        snap.windows = vec![
            LogWindow {
                database: String::new(),
                start_lsn: 250,
                end_lsn: 300,
            },
            LogWindow {
                database: "shop".to_string(),
                start_lsn: 4,
                end_lsn: 6,
            },
        ];
        m.snapshots = vec![snap];
        m.segments = vec![
            SegmentEntry {
                sequence: 0,
                database: String::new(),
                key: "wal/0.zst".to_string(),
                archived_at: 3_010,
                first_lsn: 250,
                last_lsn: 320,
                compressed_bytes: 1,
                uncompressed_bytes: 2,
                segment_count: 1,
            },
            SegmentEntry {
                sequence: 1,
                database: "shop".to_string(),
                key: "wal/shop/1.zst".to_string(),
                archived_at: 3_010,
                first_lsn: 4,
                last_lsn: 9,
                compressed_bytes: 1,
                uncompressed_bytes: 2,
                segment_count: 1,
            },
            SegmentEntry {
                sequence: 2,
                database: "shop".to_string(),
                key: "wal/shop/2.zst".to_string(),
                archived_at: 4_000,
                first_lsn: 10,
                last_lsn: 12,
                compressed_bytes: 1,
                uncompressed_bytes: 2,
                segment_count: 1,
            },
        ];
        m
    }

    #[test]
    fn each_database_sees_only_its_own_bundles() {
        let m = two_databases();
        assert_eq!(
            m.segments_for("")
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["wal/0.zst"]
        );
        assert_eq!(
            m.segments_for("shop")
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["wal/shop/1.zst", "wal/shop/2.zst"],
            "one database's bundles must come back in the order they were cut"
        );
        assert_eq!(
            m.databases_with_segments(),
            vec!["".to_string(), "shop".to_string()]
        );
    }

    #[test]
    fn a_window_is_read_per_database() {
        let m = two_databases();
        let snap = &m.snapshots[0];
        assert_eq!(snap.window_for("shop").unwrap().end_lsn, 6);
        assert_eq!(snap.window_for("").unwrap().start_lsn, 250);
        assert_eq!(snap.window_for("absent"), None);
    }

    #[test]
    fn a_catalogue_without_windows_still_describes_the_root() {
        // What every catalogue written before sub-database logs were archived
        // looks like: no `windows`, and the root's window in the old fields.
        let m = manifest();
        let snap = &m.snapshots[0];
        assert!(snap.windows.is_empty());
        let window = snap
            .window_for("")
            .expect("the root's window is always known");
        assert_eq!(
            (window.start_lsn, window.end_lsn),
            (snap.start_lsn, snap.end_lsn)
        );
    }

    #[test]
    fn a_time_target_stops_each_database_at_its_own_commit() {
        // 3_500 is after the first flush of both logs and before the shop's
        // second, so each stops at a number that means something only to it.
        let m = two_databases();
        let snap = &m.snapshots[0];
        let at = RecoveryTarget::Time(3_500);
        assert_eq!(at.resolve_lsn_for(&m, snap, ""), Some(320));
        assert_eq!(at.resolve_lsn_for(&m, snap, "shop"), Some(9));
    }

    #[test]
    fn a_time_target_never_stops_before_a_database_is_consistent() {
        let m = two_databases();
        let snap = &m.snapshots[0];
        // Nothing was archived for either log by 3_000, but both still need
        // replay to their own window end for their copies to be usable.
        let at = RecoveryTarget::Time(3_000);
        assert_eq!(at.resolve_lsn_for(&m, snap, ""), Some(300));
        assert_eq!(at.resolve_lsn_for(&m, snap, "shop"), Some(6));
    }

    #[test]
    fn an_lsn_target_reaches_other_databases_through_the_moment_it_was_archived() {
        // `AT LSN 310` names a commit of the root database. The shop counts its
        // own commits, so what it means there is "as far as the shop had got by
        // the time that commit reached S3" — bundle 1 (archived 3_010), not
        // bundle 2 (archived 4_000).
        let m = two_databases();
        let snap = &m.snapshots[0];
        let at = RecoveryTarget::Lsn(310);
        assert_eq!(at.resolve_lsn_for(&m, snap, ""), Some(310));
        assert_eq!(at.resolve_lsn_for(&m, snap, "shop"), Some(9));
    }

    #[test]
    fn an_lsn_target_past_everything_archived_replays_the_others_whole() {
        let m = two_databases();
        let snap = &m.snapshots[0];
        assert_eq!(
            RecoveryTarget::Lsn(9_999).resolve_lsn_for(&m, snap, "shop"),
            None
        );
    }

    #[test]
    fn a_snapshot_target_stops_each_database_at_its_own_window() {
        let m = two_databases();
        let snap = &m.snapshots[0];
        let at = RecoveryTarget::Snapshot("s1".to_string());
        assert_eq!(at.resolve_lsn_for(&m, snap, ""), Some(300));
        assert_eq!(at.resolve_lsn_for(&m, snap, "shop"), Some(6));
    }

    #[test]
    fn retention_bounds_every_database_with_one_sequence() {
        // The sequence counter is installation-wide, so one floor prunes every
        // log at once — and a bundle of a database the snapshot predates is not
        // kept alive by the root's numbers.
        let mut m = two_databases();
        let mut newer = snapshot("s2", 5_000, 400);
        newer.sequence = 9;
        newer.first_segment_sequence = Some(2);
        m.snapshots.push(newer);

        let plan = m.plan_retention(1);
        assert_eq!(
            plan.segments
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["wal/shop/2.zst"]
        );
        assert!(plan.doomed.contains(&"wal/0.zst".to_string()));
        assert!(plan.doomed.contains(&"wal/shop/1.zst".to_string()));
    }
}
