//! Command-line surface for S3 backup and restore.
//!
//! Kept out of `main.rs` so that the flags, their defaults, and the rules for
//! resolving them against the AWS environment variables live in one place, and
//! so a build without the `s3_backup` feature carries none of it.

use std::path::{Path, PathBuf};

use clap::Args;

use rasterizeddb_core::core::backup::BackupConfig;
use rasterizeddb_core::core::backup::manifest::RecoveryTarget;
use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint};

/// zstd's ceiling. Above 19 it enters "ultra" territory, where memory and time
/// climb steeply for a ratio that on database files barely moves.
const MAX_LEVEL: i32 = 22;

#[derive(Args, Debug, Clone)]
pub struct BackupArgs {
    /// S3 bucket to back up into. Setting it is what enables backups.
    #[arg(long = "s3-bucket", value_name = "BUCKET")]
    pub bucket: Option<String>,

    /// Key prefix within the bucket. One database per prefix — the server
    /// refuses to start if the prefix already holds another database's
    /// catalogue.
    #[arg(long = "s3-prefix", value_name = "PREFIX", default_value = "")]
    pub prefix: String,

    /// Region for request signing (default: $AWS_REGION, else us-east-1).
    #[arg(long = "s3-region", value_name = "REGION")]
    pub region: Option<String>,

    /// Endpoint URL. Point this at MinIO, R2, B2 or Wasabi to use them instead
    /// of AWS (default: $AWS_ENDPOINT_URL, else the regional AWS endpoint).
    #[arg(long = "s3-endpoint", value_name = "URL")]
    pub endpoint: Option<String>,

    /// Access key (default: $AWS_ACCESS_KEY_ID).
    #[arg(long = "s3-access-key", value_name = "KEY")]
    pub access_key: Option<String>,

    /// Secret key (default: $AWS_SECRET_ACCESS_KEY).
    ///
    /// Prefer the environment variable: an argument is visible to anything that
    /// can read the process list.
    #[arg(long = "s3-secret-key", value_name = "SECRET")]
    pub secret_key: Option<String>,

    /// Session token for temporary credentials (default: $AWS_SESSION_TOKEN).
    #[arg(long = "s3-session-token", value_name = "TOKEN")]
    pub session_token: Option<String>,

    /// Address buckets as `host/bucket/key` rather than `bucket.host/key`.
    /// Required by MinIO and by buckets whose names are not DNS-safe.
    #[arg(long = "s3-path-style", default_value_t = false)]
    pub path_style: bool,

    /// zstd level for snapshots, 1..=22 (default: 12).
    ///
    /// 12 with long-range matching is close to what level 19 achieves on
    /// database files, for a small fraction of the CPU. Raise it if object
    /// storage costs more than the machine's time.
    #[arg(long = "s3-snapshot-level", value_name = "1..22", default_value_t = 12)]
    pub snapshot_level: i32,

    /// zstd level for archived log segments, 1..=22 (default: 6).
    ///
    /// Lower than the snapshot's on purpose: segments are compressed far more
    /// often, on the machine that is also serving queries, and they are small.
    #[arg(long = "s3-segment-level", value_name = "1..22", default_value_t = 6)]
    pub segment_level: i32,

    /// Compression threads, or 0 to use half the machine's cores (max 8).
    #[arg(long = "s3-compression-threads", value_name = "N", default_value_t = 0)]
    pub compression_threads: u32,

    /// Turn off long-distance matching. It is on by default because database
    /// files repeat their record framing across megabytes, which is where most
    /// of a snapshot's compression ratio comes from.
    #[arg(long = "s3-no-long-range", default_value_t = false)]
    pub no_long_range: bool,

    /// Multipart part size in MiB (default: 32, minimum 5).
    ///
    /// Larger means fewer requests and more memory in flight. Raise it for a
    /// database beyond ~320 GB, which is where 32 MiB parts reach S3's
    /// 10 000-part limit.
    #[arg(long = "s3-part-size-mb", value_name = "MB", default_value_t = 32)]
    pub part_size_mb: usize,

    /// Parts uploaded at once (default: 3). Peak memory is roughly this times
    /// the part size.
    #[arg(long = "s3-upload-concurrency", value_name = "N", default_value_t = 3)]
    pub upload_concurrency: usize,

    /// Attempts per request, including the first (default: 4).
    #[arg(long = "s3-max-attempts", value_name = "N", default_value_t = 4)]
    pub max_attempts: u32,

    /// Seconds between automatic snapshots, or 0 for manual only
    /// (default: 86400, daily).
    ///
    /// Measured from the last snapshot in the catalogue, not from start-up, so
    /// restarting does not push the next one a full interval away. A prefix
    /// with no snapshot at all gets one immediately whatever this is set to —
    /// the log starts archiving as soon as the server does, and segments with
    /// no snapshot under them restore nothing.
    #[arg(
        long = "s3-snapshot-interval-secs",
        value_name = "N",
        default_value_t = 86_400
    )]
    pub snapshot_interval_secs: u64,

    /// Seconds between log archive flushes, or 0 for manual only
    /// (default: 300).
    ///
    /// This is the recovery point objective: a crash can lose at most this much
    /// of the most recent write history. Lowering it costs two more requests
    /// per flush and nothing else.
    #[arg(long = "s3-wal-flush-secs", value_name = "N", default_value_t = 300)]
    pub wal_flush_secs: u64,

    /// Staged log bytes that trigger a flush ahead of the interval, in MiB
    /// (default: 64). Bounds how much a write-heavy burst can accumulate on
    /// local disk between flushes.
    #[arg(long = "s3-wal-bundle-mb", value_name = "MB", default_value_t = 64)]
    pub wal_bundle_mb: u64,

    /// Snapshots to keep, oldest deleted first, or 0 to keep everything
    /// (default: 7). Log segments only an expired snapshot could have used are
    /// deleted with it.
    #[arg(long = "s3-retain-snapshots", value_name = "N", default_value_t = 7)]
    pub retain_snapshots: usize,

    /// Hours between backup rollovers, or 0 to never roll over (default: 0).
    ///
    /// A rollover takes a fresh full snapshot and, once it has landed,
    /// deletes every earlier generation — so the bucket returns to holding one
    /// self-contained backup instead of an ever-lengthening chain of segments
    /// a restore would have to replay. The delete only happens after the new
    /// snapshot succeeds; a failed rollover keeps the previous one.
    ///
    /// This is retention by time, and it operates independently of
    /// `--s3-retain-snapshots`, which is retention by count between rollovers.
    ///
    /// Measured from the last rollover recorded in the catalogue rather than
    /// from start-up, so restarting does not postpone it. A catalogue that has
    /// never rolled over measures from its newest snapshot instead.
    #[arg(long = "s3-rollover-hours", value_name = "HOURS", default_value_t = 0)]
    pub rollover_hours: u64,

    /// Store files of at least this many MiB as objects of their own, so a
    /// later snapshot reuses any whose bytes have not changed instead of
    /// uploading them again (default: 0, off — every snapshot is one
    /// self-contained archive).
    ///
    /// A catalogue holding such snapshots is written as manifest version 2,
    /// which builds without this feature refuse rather than restore from
    /// partially. Rollovers always take a full snapshot.
    #[arg(long = "s3-incremental-min-mb", value_name = "MIB", default_value_t = 0)]
    pub incremental_min_mb: u64,

    /// Staged log bytes after which archiving is suspended, in MiB
    /// (default: 4096). 0 disables the ceiling.
    ///
    /// This is a safety valve, not a tuning knob. If uploads start failing,
    /// every checkpoint keeps adding to the staging directory and nothing
    /// drains it; left alone that fills the disk and stops the database. Past
    /// this point checkpoints go back to discarding the log — point-in-time
    /// recovery breaks, loudly, rather than the database going down.
    #[arg(long = "s3-staging-max-mb", value_name = "MB", default_value_t = 4096)]
    pub staging_max_mb: u64,

    /// Take a snapshot on every start, even when the catalogue already holds
    /// one and the schedule is not yet due.
    ///
    /// Not needed to get a new prefix its first snapshot; that happens on its
    /// own. This is for wanting a fresh copy at every boot on top of the
    /// schedule.
    #[arg(long = "s3-backup-now", default_value_t = false)]
    pub backup_now: bool,

    // ---- restore ----------------------------------------------------------
    /// Restore from S3 before opening any table, replacing the database at
    /// --location.
    ///
    /// This is the in-place path, and it runs before the engine touches
    /// storage, which is why it is a flag rather than a statement: a `RESTORE`
    /// executed by a running server would swap the files underneath its own
    /// open tables.
    #[arg(long = "restore-from-s3", default_value_t = false)]
    pub restore: bool,

    /// Restore into this directory and exit without starting a server. Use it
    /// to verify a backup or to clone a database.
    #[arg(long = "restore-to", value_name = "DIR")]
    pub restore_to: Option<PathBuf>,

    /// Stop recovery at this commit sequence. Exact: a transaction is either
    /// wholly applied or wholly absent.
    #[arg(long = "restore-at-lsn", value_name = "N")]
    pub restore_at_lsn: Option<u64>,

    /// Stop recovery at this instant, e.g. '2026-08-20T10:00:00Z'. Resolves to
    /// the last archive flush at or before it.
    #[arg(long = "restore-at-time", value_name = "TIMESTAMP")]
    pub restore_at_time: Option<String>,

    /// Restore this snapshot exactly, replaying only enough log to make it
    /// consistent.
    #[arg(long = "restore-snapshot", value_name = "ID")]
    pub restore_snapshot: Option<String>,

    /// Allow an in-place restore over a --location that already holds data.
    ///
    /// The existing directory is renamed aside, never deleted: it may be the
    /// last local copy, and a restore is usually run because something has
    /// already gone wrong.
    #[arg(long = "restore-replace", default_value_t = false)]
    pub restore_replace: bool,
}

impl BackupArgs {
    pub fn enabled(&self) -> bool {
        self.bucket.is_some()
    }

    /// Whether a restore should run before the server opens anything.
    pub fn wants_restore(&self) -> bool {
        self.restore || self.restore_to.is_some()
    }

    /// Builds the backup configuration, resolving anything unset from the
    /// environment.
    pub fn to_config(&self) -> Result<BackupConfig, String> {
        let bucket = self
            .bucket
            .clone()
            .ok_or("--s3-bucket is required to use backup or restore")?;

        let region = self
            .region
            .clone()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".to_string());

        let endpoint_url = self
            .endpoint
            .clone()
            .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
            .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint = Endpoint::parse(&endpoint_url)
            .map_err(|e| format!("--s3-endpoint {endpoint_url}: {e}"))?;

        // An explicit flag wins over the environment, and a partially specified
        // pair is an error rather than a silent fallback to whatever half of it
        // the environment happens to hold.
        let credentials = match (&self.access_key, &self.secret_key) {
            (Some(access_key), Some(secret_key)) => Credentials {
                access_key: access_key.clone(),
                secret_key: secret_key.clone(),
                session_token: self
                    .session_token
                    .clone()
                    .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok()),
            },
            (None, None) => Credentials::from_env().ok_or(
                "no S3 credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, \
                 or pass --s3-access-key and --s3-secret-key",
            )?,
            _ => {
                return Err(
                    "--s3-access-key and --s3-secret-key must be given together".to_string()
                );
            }
        };

        check_level("--s3-snapshot-level", self.snapshot_level)?;
        check_level("--s3-segment-level", self.segment_level)?;

        if self.upload_concurrency == 0 {
            return Err("--s3-upload-concurrency must be at least 1".to_string());
        }
        if self.max_attempts == 0 {
            return Err("--s3-max-attempts must be at least 1".to_string());
        }
        if self.part_size_mb < 5 {
            return Err("--s3-part-size-mb must be at least 5, which is S3's minimum".to_string());
        }

        Ok(BackupConfig {
            bucket,
            prefix: self.prefix.clone(),
            region,
            endpoint,
            credentials,
            path_style: self.path_style,
            snapshot_level: self.snapshot_level,
            segment_level: self.segment_level,
            compression_threads: self.compression_threads,
            long_range: !self.no_long_range,
            part_size: self.part_size_mb * 1024 * 1024,
            upload_concurrency: self.upload_concurrency,
            max_attempts: self.max_attempts,
            snapshot_interval_secs: self.snapshot_interval_secs,
            wal_flush_secs: self.wal_flush_secs,
            wal_bundle_bytes: self.wal_bundle_mb * 1024 * 1024,
            retain_snapshots: self.retain_snapshots,
            rollover_interval_secs: self.rollover_hours.saturating_mul(3600),
            staging_max_bytes: self.staging_max_mb * 1024 * 1024,
            incremental_min_bytes: self.incremental_min_mb.saturating_mul(1024 * 1024),
        })
    }

    /// Where recovery should stop.
    pub fn recovery_target(&self) -> Result<RecoveryTarget, String> {
        let named = [
            self.restore_at_lsn.is_some(),
            self.restore_at_time.is_some(),
            self.restore_snapshot.is_some(),
        ]
        .iter()
        .filter(|given| **given)
        .count();

        if named > 1 {
            return Err(
                "give at most one of --restore-at-lsn, --restore-at-time and --restore-snapshot"
                    .to_string(),
            );
        }

        if let Some(lsn) = self.restore_at_lsn {
            return Ok(RecoveryTarget::Lsn(lsn));
        }
        if let Some(id) = &self.restore_snapshot {
            return Ok(RecoveryTarget::Snapshot(id.clone()));
        }
        if let Some(text) = &self.restore_at_time {
            let unix = rasterizeddb_core::core::backup::timeutil::parse_rfc3339(text).ok_or_else(
                || {
                    format!(
                        "--restore-at-time {text:?} is not a timestamp; \
                         expected something like '2026-08-20T10:00:00Z'"
                    )
                },
            )?;
            return Ok(RecoveryTarget::Time(unix));
        }
        Ok(RecoveryTarget::Latest)
    }
}

fn check_level(flag: &str, level: i32) -> Result<(), String> {
    if !(1..=MAX_LEVEL).contains(&level) {
        return Err(format!("{flag} must be between 1 and {MAX_LEVEL}"));
    }
    Ok(())
}

/// Runs whatever restore the flags asked for.
///
/// Returns `true` when the process should exit rather than start a server,
/// which is what `--restore-to` means.
pub async fn run_restore(args: &BackupArgs, db_location: &str) -> Result<bool, String> {
    let config = args.to_config()?;
    let target = args.recovery_target()?;

    // `--restore-to` clones somewhere new and stops. `--restore-from-s3`
    // rebuilds the live directory and then serves it.
    let (destination, standalone) = match &args.restore_to {
        Some(dir) => (dir.clone(), true),
        None => (PathBuf::from(db_location), false),
    };

    if !standalone {
        if db_location.is_empty() {
            return Err(
                "--restore-from-s3 needs --location; there is nowhere to restore an in-memory \
                 database to"
                    .to_string(),
            );
        }
        move_aside(&destination, args.restore_replace)?;
    }

    println!(
        "Restoring from s3://{}/{} into {}",
        config.bucket,
        config.normalized_prefix(),
        destination.display()
    );

    let report = rasterizeddb_core::core::backup::restore_from(&config, &destination, target)
        .await
        .map_err(|e| format!("restore failed: {e}"))?;

    println!(
        "Restored snapshot {} ({}): {} files, {} bytes, {} log bundles replayed, \
         {} records applied, {} discarded as uncommitted, recovered to LSN {}",
        report.snapshot_id,
        rasterizeddb_core::core::backup::timeutil::format_rfc3339(report.snapshot_taken_at),
        report.files_restored,
        report.bytes_restored,
        report.segments_replayed,
        report.records_applied,
        report.records_discarded,
        report
            .recovered_to_lsn
            .map(|l| l.to_string())
            .unwrap_or_else(|| "end of archive".to_string()),
    );

    if report.records_unresolved > 0 {
        eprintln!(
            "warning: {} log records named files the snapshot did not contain and were skipped",
            report.records_unresolved
        );
    }

    Ok(standalone)
}

/// Clears the way for an in-place restore, without destroying anything.
fn move_aside(destination: &Path, allowed: bool) -> Result<(), String> {
    let occupied = match std::fs::read_dir(destination) {
        Ok(mut entries) => entries.next().is_some(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(format!("cannot read {}: {e}", destination.display())),
    };
    if !occupied {
        return Ok(());
    }

    if !allowed {
        return Err(format!(
            "{} already holds a database. Pass --restore-replace to move it aside first, \
             or use --restore-to <DIR> to restore somewhere else.",
            destination.display()
        ));
    }

    let stamp = rasterizeddb_core::core::backup::timeutil::format_basic(
        rasterizeddb_core::core::backup::timeutil::now_unix(),
    );
    let aside = destination.with_file_name(format!(
        "{}.pre-restore-{stamp}",
        destination
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("database")
    ));

    std::fs::rename(destination, &aside).map_err(|e| {
        format!(
            "could not move {} aside to {}: {e}",
            destination.display(),
            aside.display()
        )
    })?;
    println!(
        "Moved the existing database to {} — delete it once the restore is verified",
        aside.display()
    );
    Ok(())
}
