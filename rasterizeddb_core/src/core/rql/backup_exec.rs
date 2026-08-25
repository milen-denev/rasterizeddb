//! Running the `BACKUP` / `RESTORE` / `SHOW BACKUPS` statements.
//!
//! Kept out of `executor.rs` so that enabling the feature adds a module rather
//! than threading conditional compilation through the main statement dispatch.

use std::sync::Arc;

use log::{error, info};
use smallvec::SmallVec;

use crate::core::backup::manifest::RecoveryTarget;
use crate::core::backup::{self, BackupService};
use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};
use crate::memory_pool::MEMORY_POOL;

use super::lexer_backup::BackupStatement;

/// Column metadata a statement's result set carries, or `None` when it reports
/// a count rather than rows.
pub fn result_fields(statement: &BackupStatement) -> Option<Vec<(String, DbType)>> {
    match statement {
        BackupStatement::Show => Some(vec![
            ("kind".to_string(), DbType::STRING),
            ("id".to_string(), DbType::STRING),
            ("created_at".to_string(), DbType::STRING),
            ("start_lsn".to_string(), DbType::U64),
            ("end_lsn".to_string(), DbType::U64),
            ("uncompressed_bytes".to_string(), DbType::U64),
            ("compressed_bytes".to_string(), DbType::U64),
            ("detail".to_string(), DbType::STRING),
        ]),
        BackupStatement::Restore { .. } => Some(vec![
            ("snapshot_id".to_string(), DbType::STRING),
            ("target_dir".to_string(), DbType::STRING),
            ("recovered_to_lsn".to_string(), DbType::STRING),
            ("files_restored".to_string(), DbType::U64),
            ("bytes_restored".to_string(), DbType::U64),
            ("segments_replayed".to_string(), DbType::U64),
            ("records_applied".to_string(), DbType::U64),
            ("records_discarded".to_string(), DbType::U64),
        ]),
        _ => None,
    }
}

pub async fn execute(statement: BackupStatement) -> Vec<u8> {
    match statement {
        BackupStatement::Snapshot => match require_service() {
            Err(response) => response,
            Ok(service) => match service.snapshot_now().await {
                Ok(report) => {
                    info!(
                        "BACKUP DATABASE produced {} ({} bytes compressed)",
                        report.id, report.compressed_bytes
                    );
                    count(report.compressed_bytes)
                }
                Err(e) => {
                    error!("BACKUP DATABASE failed: {e}");
                    failure(&format!("BACKUP DATABASE failed: {e}"))
                }
            },
        },

        BackupStatement::ArchiveWal => match require_service() {
            Err(response) => response,
            Ok(service) => match service.archive_wal_now().await {
                Ok(report) => count(report.compressed_bytes),
                Err(e) => {
                    error!("BACKUP WAL failed: {e}");
                    failure(&format!("BACKUP WAL failed: {e}"))
                }
            },
        },

        BackupStatement::Prune => match require_service() {
            Err(response) => response,
            Ok(service) => match service.prune().await {
                Ok(removed) => count(removed as u64),
                Err(e) => {
                    error!("BACKUP PRUNE failed: {e}");
                    failure(&format!("BACKUP PRUNE failed: {e}"))
                }
            },
        },

        BackupStatement::Rollover => match require_service() {
            Err(response) => response,
            Ok(service) => match service.rollover_now().await {
                Ok((report, removed)) => {
                    info!(
                        "BACKUP ROLLOVER produced {} and removed {} object(s) from earlier \
                         generations",
                        report.id, removed
                    );
                    count(removed as u64)
                }
                Err(e) => {
                    error!("BACKUP ROLLOVER failed: {e}");
                    failure(&format!("BACKUP ROLLOVER failed: {e}"))
                }
            },
        },

        BackupStatement::Show => match require_service() {
            Err(response) => response,
            Ok(service) => rows(show_rows(&service).await),
        },

        BackupStatement::Restore { target_dir, target } => match require_service() {
            Err(response) => response,
            Ok(service) => restore(&service, &target_dir, target).await,
        },
    }
}

async fn restore(
    service: &Arc<BackupService>,
    target_dir: &str,
    target: RecoveryTarget,
) -> Vec<u8> {
    let path = std::path::Path::new(target_dir);

    // Restoring into the directory the server is serving from would swap files
    // underneath every open table. The check is on the resolved path so a
    // relative or symlinked spelling of the same directory is caught too.
    if same_directory(path, std::path::Path::new(service.location())) {
        return failure(
            "RESTORE cannot write into the directory this server is running from. \
             Restore into a new directory, or stop the server and start it with \
             --restore-from-s3 to replace the live database in place.",
        );
    }

    match backup::restore_from(service.config(), path, target).await {
        Ok(report) => {
            info!(
                "RESTORE rebuilt {} from snapshot {}",
                report.target_dir.display(),
                report.snapshot_id
            );
            rows(vec![row(&[
                Cell::Text(report.snapshot_id.clone()),
                Cell::Text(report.target_dir.display().to_string()),
                Cell::Text(
                    report
                        .recovered_to_lsn
                        .map(|l| l.to_string())
                        .unwrap_or_else(|| "end of archive".to_string()),
                ),
                Cell::Number(report.files_restored as u64),
                Cell::Number(report.bytes_restored),
                Cell::Number(report.segments_replayed as u64),
                Cell::Number(report.records_applied),
                Cell::Number(report.records_discarded),
            ])])
        }
        Err(e) => {
            error!("RESTORE failed: {e}");
            failure(&format!("RESTORE failed: {e}"))
        }
    }
}

async fn show_rows(service: &Arc<BackupService>) -> Vec<Row> {
    let manifest = service.manifest_snapshot().await;
    let mut out = Vec::with_capacity(manifest.snapshots.len() + manifest.segments.len());

    for snapshot in manifest.snapshots_newest_first() {
        out.push(row(&[
            Cell::Text("snapshot".to_string()),
            Cell::Text(snapshot.id.clone()),
            Cell::Text(backup::timeutil::format_rfc3339(snapshot.finished_at)),
            Cell::Number(snapshot.start_lsn),
            Cell::Number(snapshot.end_lsn),
            Cell::Number(snapshot.uncompressed_bytes),
            Cell::Number(snapshot.compressed_bytes),
            Cell::Text(format!(
                "{} files, zstd level {}, s3://{}/{}",
                snapshot.file_count,
                snapshot.compression_level,
                service.config().bucket,
                snapshot.key
            )),
        ]));
    }

    for segment in manifest.segments_ordered().into_iter().rev() {
        out.push(row(&[
            Cell::Text("wal".to_string()),
            Cell::Text(format!("{:016x}", segment.sequence)),
            Cell::Text(backup::timeutil::format_rfc3339(segment.archived_at)),
            Cell::Number(segment.first_lsn),
            Cell::Number(segment.last_lsn),
            Cell::Number(segment.uncompressed_bytes),
            Cell::Number(segment.compressed_bytes),
            Cell::Text(format!(
                "{} segments, s3://{}/{}",
                segment.segment_count,
                service.config().bucket,
                segment.key
            )),
        ]));
    }

    out
}

/// Two paths naming the same directory, as far as the filesystem is concerned.
///
/// Falls back to a textual comparison for a path that does not exist yet, which
/// is the normal case for a restore destination.
fn same_directory(a: &std::path::Path, b: &std::path::Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

fn require_service() -> Result<Arc<BackupService>, Vec<u8>> {
    backup::service().ok_or_else(|| {
        failure(
            "no backup destination is configured. Start the server with --s3-bucket \
             (and --s3-region / --s3-endpoint as needed) to enable backups.",
        )
    })
}

// ---- result encoding -------------------------------------------------------

enum Cell {
    Text(String),
    Number(u64),
}

fn row(cells: &[Cell]) -> Row {
    let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            Cell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            Cell::Number(n) => (n.to_le_bytes().to_vec(), DbType::U64),
        };
        let mut block = MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(Column {
            schema_id: index as u64,
            data: block,
            column_type,
        });
    }

    let length = columns.iter().map(|c| c.data.len()).sum::<usize>();
    Row {
        position: 0,
        columns,
        #[cfg(feature = "enable_long_row")]
        length: length as u64,
        #[cfg(not(feature = "enable_long_row"))]
        length: length as u32,
    }
}

fn rows(rows: Vec<Row>) -> Vec<u8> {
    let mut out = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut out);
    }
    out
}

fn count(n: u64) -> Vec<u8> {
    let mut out = vec![0u8];
    out.extend_from_slice(&n.to_le_bytes());
    out
}

fn failure(message: &str) -> Vec<u8> {
    let mut out = vec![3u8];
    out.extend_from_slice(message.as_bytes());
    out
}
