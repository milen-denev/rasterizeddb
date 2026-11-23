//! Backups: what has been stored, how to add to it, and how to get it back.
//!
//! # The three questions
//!
//! 1. **Is there a backup destination at all?** Three states that look the same
//!    from a distance and want completely different things done about them: the
//!    binary was built without the feature, the feature is there but no bucket
//!    was configured, or it is configured. The status query answers all three
//!    rather than returning an empty catalogue for each.
//!
//! 2. **Is it healthy?** A backup destination that stopped working does not
//!    announce itself: the catalogue simply stops growing, and nobody looks at
//!    a catalogue that has always looked fine. `archiving_suspended` is
//!    promoted to a banner for exactly that reason.
//!
//! 3. **What would a restore actually give me?** Every recovery target except
//!    `Latest` deliberately leaves commits out, and the console says which and
//!    how many — before, as a warning, and after, as `records_discarded`.
//!
//! # What this page will not do
//!
//! Restore over the live database. The engine refuses it from SQL because
//! swapping files under open tables leaves every reader looking at storage that
//! no longer describes what it thinks it does. Rather than saying "not
//! supported", the page prints the start-up command that does it, filled in
//! from the destination and the recovery target already chosen — because the
//! moment someone needs that is the worst moment to be reading documentation.

use iced::widget::{Space, column, container, pick_list, row, rule, scrollable, text};
use iced::{Center, Element, Fill, Length};

use crate::app::{BackupAction, Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, primary_button, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let flavor = studio.flavor();
    let manageable = dialect::backups_are_manageable(flavor);

    let Some(snapshot) = &studio.backup else {
        return framed(
            "BACKUPS",
            Some(small_button("RELOAD", Some(Message::ReloadBackups))),
            container(text("Loading…").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };

    let state = snapshot.state();
    let body = match &state {
        BackupState::NotCompiled => unavailable(studio, snapshot, false, flavor),
        BackupState::NotConfigured => unavailable(studio, snapshot, true, flavor),
        BackupState::Unreadable(error) => unreadable(studio, snapshot, error.clone()),
        BackupState::Configured | BackupState::ConfiguredWithoutStatus => {
            let entries = snapshot.catalogue.as_deref().unwrap_or(&[]);
            let mut page = column![].spacing(10);

            if state == BackupState::ConfiguredWithoutStatus {
                page = page.push(no_status_note(snapshot));
            } else {
                page = page.push(health(&snapshot.status));
                page = page.push(summary(&snapshot.status, entries));
            }

            page = page.push(actions(studio, snapshot, manageable));
            page = page.push(
                row![
                    container(catalogue(studio, entries)).width(Fill),
                    container(restore_panel(studio, snapshot, manageable))
                        .width(Length::Fixed(430.0)),
                ]
                .spacing(10)
                .height(Fill),
            );
            page.into()
        }
    };

    framed(
        "BACKUPS",
        Some(small_button("RELOAD", Some(Message::ReloadBackups))),
        body,
    )
}

/// A server that has backups but cannot describe them.
///
/// This is what a deployment running a build older than `SHOW BACKUP STATUS`
/// looks like. Everything on the page still works — the catalogue is real and
/// the actions are real — so it says what is missing and carries on, rather
/// than claiming the destination does not exist.
fn no_status_note(snapshot: &BackupSnapshot) -> Element<'_, Message> {
    let stored: u64 = snapshot
        .catalogue
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|entry| entry.compressed_bytes)
        .sum();

    column![
        container(
            column![
                text("THIS SERVER DOES NOT REPORT BACKUP STATUS")
                    .size(11)
                    .font(theme::MONO)
                    .color(theme::WARN),
                text(
                    "It answered SHOW BACKUP STATUS as if it were a runtime parameter, which is \
                     what a build from before that statement existed does. The catalogue below is \
                     real and the actions work; what is missing is the health reporting — whether \
                     archiving has suspended itself, how much is staged, how far a restore can \
                     reach. Upgrade the server to get it."
                )
                .size(11)
                .color(theme::TEXT_DIM),
            ]
            .spacing(4),
        )
        .padding(9)
        .width(Fill)
        .style(theme::tile),
        row![
            super::tile(
                "SNAPSHOTS",
                count(
                    snapshot
                        .catalogue
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .filter(|entry| entry.is_snapshot())
                        .count() as u64
                ),
                None,
            ),
            super::tile("STORED", bytes(stored), Some("compressed, in the bucket".into())),
            super::tile(
                "CATALOGUE ENTRIES",
                count(snapshot.catalogue.as_deref().unwrap_or(&[]).len() as u64),
                None,
            ),
        ]
        .spacing(10),
    ]
    .spacing(10)
    .into()
}

/// Neither question could be answered, which is not a state of the backup
/// system and must not be reported as one.
fn unreadable<'a>(
    studio: &'a Studio,
    snapshot: &'a BackupSnapshot,
    error: String,
) -> Element<'a, Message> {
    let mut panel = column![
        text("Backups could not be read").size(15).color(theme::TEXT),
        container(Space::new().height(2)).width(72).style(theme::accent_underline),
        container(text(error).size(12).font(theme::MONO))
            .padding(9)
            .width(Fill)
            .style(theme::error_box),
        text(
            "This is the server refusing or failing the query, not a statement about whether \
             backups exist. Check the role's permissions and the connection before concluding \
             anything about the destination."
        )
        .size(11)
        .color(theme::TEXT_DIM),
    ]
    .spacing(10)
    .max_width(760);

    if !snapshot.status.is_empty() {
        panel = panel.push(rule::horizontal(1).style(theme::hairline));
        panel = panel.push(section_label("WHAT THE SERVER DID SAY"));
        panel = panel.push(
            container(super::metric_grid(studio, "backup:status", &snapshot.status))
                .height(Length::Fixed(26.0 + snapshot.status.len() as f32 * 19.0)),
        );
    }

    container(scrollable(panel).height(Fill).style(theme::scroller))
        .padding(10)
        .into()
}

// ---------------------------------------------------------------------------
// Empty states
// ---------------------------------------------------------------------------

fn unavailable<'a>(
    studio: &'a Studio,
    snapshot: &'a BackupSnapshot,
    compiled: bool,
    flavor: Flavor,
) -> Element<'a, Message> {
    let (heading, body): (&str, String) = match (flavor, compiled) {
        (Flavor::Postgres, _) => (
            "WAL archiving is off on this server",
            "PostgreSQL takes backups outside the server — pg_basebackup, pgBackRest, WAL-G — so \
             there is nothing here to drive. Turn on archive_mode and set an archive_command, and \
             this page will report whether archiving is keeping up."
                .to_string(),
        ),
        (Flavor::Rasterized, false) => (
            "This server was built without backup support",
            "Nothing you can do at runtime will change that. Rebuild with \
             `--features s3_backup` and start it with a bucket."
                .to_string(),
        ),
        (Flavor::Rasterized, true) => (
            "No backup destination is configured",
            "The server can back up; it has not been told where to. Start it with a bucket:\n\n\
             rasterizeddb_core --location <PATH> \\\n  \
             --s3-bucket my-backups --s3-region eu-central-1 --s3-prefix prod/orders\n\n\
             Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY. \
             --s3-endpoint points it at MinIO, R2, B2 or Wasabi instead of AWS."
                .to_string(),
        ),
    };

    let mut panel = column![
        text(heading).size(15).color(theme::TEXT),
        container(Space::new().height(2)).width(72).style(theme::accent_underline),
        text(body).size(12).font(theme::MONO).color(theme::TEXT_DIM),
    ]
    .spacing(10)
    .max_width(720);

    // Whatever the server *did* say is still worth showing: for PostgreSQL that
    // is the archiver's counters, which say whether anything is being archived
    // even when this console cannot drive it.
    if snapshot.status.len() > 2 {
        panel = panel.push(rule::horizontal(1).style(theme::hairline));
        panel = panel.push(section_label("WHAT THE SERVER REPORTS"));
        panel = panel.push(
            container(super::metric_grid(studio, "backup:status", &snapshot.status))
                .height(Length::Fixed(26.0 + snapshot.status.len() as f32 * 19.0)),
        );
    }

    container(scrollable(panel).height(Fill).style(theme::scroller))
        .padding(10)
        .into()
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// The warnings that matter, loudest first.
///
/// Each of these is a way of losing data that leaves the catalogue looking
/// healthy, which is why they are banners rather than rows in a metric list
/// nobody reads to the bottom of.
fn health(status: &[Stat]) -> Element<'_, Message> {
    let mut warnings = column![].spacing(6);
    let mut any = false;

    if flag(status, "archiving_suspended").unwrap_or(false) {
        any = true;
        warnings = warnings.push(alarm(
            "ARCHIVING IS SUSPENDED",
            "Log segments are no longer being uploaded, and the log is being discarded so a full \
             staging disk cannot stop the database. Point-in-time recovery has a hole in it from \
             now until uploads work again — a restore will stop at the last bundle that made it. \
             Fix the destination, then take a fresh snapshot.",
        ));
    }

    if value(status, "newest_snapshot").as_deref() == Some("none") {
        any = true;
        warnings = warnings.push(alarm(
            "THERE IS NOTHING TO RESTORE FROM",
            "The catalogue holds no snapshot. Archived log alone cannot rebuild a database — it \
             replays onto a base copy. Take one with BACKUP NOW.",
        ));
    }

    if number(status, "wal_flush_secs") == Some(0) {
        any = true;
        warnings = warnings.push(caution(
            "AUTOMATIC LOG ARCHIVING IS OFF",
            "wal_flush_secs is 0, so the log is uploaded only when ARCHIVE WAL is pressed. \
             Recovery reaches the last time somebody did that, not the last commit.",
        ));
    }

    if number(status, "snapshot_interval_secs") == Some(0) {
        any = true;
        warnings = warnings.push(caution(
            "AUTOMATIC SNAPSHOTS ARE OFF",
            "snapshot_interval_secs is 0. Log bundles keep accumulating against an increasingly \
             old base copy, which makes every restore slower and every pruning decision riskier.",
        ));
    }

    let staged = number(status, "staged_bytes").unwrap_or(0);
    let ceiling = number(status, "staging_max_bytes").unwrap_or(0);
    if ceiling > 0 && staged * 10 > ceiling * 8 {
        any = true;
        warnings = warnings.push(caution(
            "THE STAGING DIRECTORY IS FILLING UP",
            format!(
                "{} of {} staged and not yet uploaded. At the ceiling, archiving suspends itself \
                 and point-in-time coverage stops.",
                bytes(staged),
                bytes(ceiling)
            ),
        ));
    }

    if !any {
        return Space::new().height(0).into();
    }
    warnings.into()
}

fn alarm(heading: &'static str, body: &'static str) -> Element<'static, Message> {
    container(
        column![
            text(heading).size(12).font(theme::MONO).color(theme::BAD),
            text(body).size(11),
        ]
        .spacing(3),
    )
    .padding(9)
    .width(Fill)
    .style(theme::error_box)
    .into()
}

fn caution(heading: &'static str, body: impl Into<String>) -> Element<'static, Message> {
    container(
        column![
            text(heading).size(11).font(theme::MONO).color(theme::WARN),
            text(body.into()).size(11).color(theme::TEXT_DIM),
        ]
        .spacing(3),
    )
    .padding(9)
    .width(Fill)
    .style(theme::tile)
    .into()
}

// ---------------------------------------------------------------------------
// Summary and actions
// ---------------------------------------------------------------------------

fn summary<'a>(status: &'a [Stat], entries: &'a [BackupEntry]) -> Element<'a, Message> {
    let tile = |label: &'static str, v: String, note: Option<String>| super::tile(label, v, note);

    let stored: u64 = entries.iter().map(|b| b.compressed_bytes).sum();
    let snapshots = entries.iter().filter(|b| b.is_snapshot()).count();

    column![
        row![
            tile(
                "DESTINATION",
                format!(
                    "{}/{}",
                    value(status, "bucket").unwrap_or_default(),
                    value(status, "prefix").unwrap_or_default().trim_end_matches('/')
                ),
                value(status, "endpoint"),
            ),
            tile("SNAPSHOTS", count(snapshots as u64), value(status, "retain_snapshots").map(|r| format!("keeping {}", r))),
            tile(
                "LOG BUNDLES",
                count(number(status, "wal_bundles").unwrap_or(0)),
                Some(format!(
                    "flushed every {}s",
                    number(status, "wal_flush_secs").unwrap_or(0)
                )),
            ),
            tile("STORED", bytes(stored), Some("compressed, in the bucket".into())),
            tile(
                "RECOVERABLE TO",
                format!("LSN {}", count(number(status, "recoverable_to_lsn").unwrap_or(0))),
                Some("the furthest a restore can reach".into()),
            ),
        ]
        .spacing(10),
        row![
            labelled("NEWEST SNAPSHOT", value(status, "newest_snapshot").unwrap_or_default()),
            labelled("SOURCE", value(status, "source_directory").unwrap_or_default()),
            labelled("STAGED", bytes(number(status, "staged_bytes").unwrap_or(0))),
        ]
        .spacing(24),
    ]
    .spacing(10)
    .into()
}

fn labelled(label: &'static str, v: String) -> Element<'static, Message> {
    column![
        text(label).size(10).font(theme::MONO).color(theme::TEXT_FAINT),
        text(if v.is_empty() { "—".to_string() } else { v })
            .size(12)
            .font(theme::MONO),
    ]
    .spacing(2)
    .into()
}

/// How many snapshots the catalogue currently holds.
fn snapshot_count(snapshot: &BackupSnapshot) -> usize {
    snapshot
        .catalogue
        .as_ref()
        .ok()
        .map(|entries| entries.iter().filter(|e| e.is_snapshot()).count())
        .unwrap_or(0)
}

fn actions<'a>(
    studio: &'a Studio,
    snapshot: &'a BackupSnapshot,
    manageable: bool,
) -> Element<'a, Message> {
    let busy = studio.backup_busy.is_some();
    let enabled = manageable && !busy;

    let retention = number(&snapshot.status, "retain_snapshots").unwrap_or(0);

    container(
        row![
            primary_button(
                "BACKUP NOW",
                enabled.then_some(Message::Backup(BackupAction::Snapshot)),
            ),
            small_button(
                "ARCHIVE WAL",
                enabled.then_some(Message::Backup(BackupAction::ArchiveWal)),
            ),
            iced::widget::button(text("PRUNE").size(11).font(theme::MONO))
                .padding([5, 10])
                .style(theme::destructive)
                .on_press_maybe(enabled.then_some(Message::Backup(BackupAction::Prune))),
            // Destructive too, but in a different way from PRUNE: it takes a
            // fresh snapshot first and only then removes what came before, so
            // the bucket ends up holding one self-contained backup rather than
            // a lengthening chain the restore has to replay.
            iced::widget::button(text("FORCE ROLLOVER").size(11).font(theme::MONO))
                .padding([5, 10])
                .style(theme::destructive)
                .on_press_maybe(enabled.then_some(Message::Backup(BackupAction::Rollover))),
            Space::new().width(Fill),
            text(match &studio.backup_busy {
                Some(what) => format!("running {}…", what),
                None => if retention == 0 {
                    "PRUNE deletes nothing: retention is 0, so every snapshot is kept. \
                     FORCE ROLLOVER takes a fresh snapshot and removes everything before it."
                        .to_string()
                } else if snapshot_count(snapshot) <= retention as usize {
                    // The state this screen is usually in, and the one that
                    // makes PRUNE look broken: it only ever removes snapshots
                    // *past* the retention count, so with fewer than that it
                    // reports success and deletes nothing — while the log
                    // bundles keep accumulating. FORCE ROLLOVER is what
                    // actually reclaims them.
                    format!(
                        "PRUNE deletes nothing yet: {} of {} retained snapshots. \
                         FORCE ROLLOVER takes a fresh snapshot and removes everything before it.",
                        snapshot_count(snapshot),
                        retention
                    )
                } else {
                    format!(
                        "BACKUP NOW copies the whole database. PRUNE permanently deletes \
                         everything past the newest {} snapshots.",
                        retention
                    )
                },
            })
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(6)
        .align_y(Center),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

// ---------------------------------------------------------------------------
// Catalogue
// ---------------------------------------------------------------------------

fn catalogue<'a>(studio: &'a Studio, entries: &'a [BackupEntry]) -> Element<'a, Message> {
    if entries.is_empty() {
        return framed(
            "CATALOGUE",
            None,
            container(text("Nothing stored yet.").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    }

    let layout = super::GridLayout::new(studio, "backup");
    let columns = vec![
        super::GridColumn::new("kind", "KIND", 80.0),
        super::GridColumn::new("id", "ID", 260.0),
        super::GridColumn::new("created", "CREATED", 176.0),
        super::GridColumn::new("lsn", "LSN RANGE", 120.0).right(),
        super::GridColumn::new("stored", "STORED", 90.0).right(),
        super::GridColumn::new("original", "ORIGINAL", 90.0).right(),
        super::GridColumn::new("ratio", "RATIO", 70.0).right(),
        super::GridColumn::new("action", "", 104.0).unsortable(),
        super::GridColumn::new("detail", "WHERE", 420.0),
    ];
    let widths = super::column_widths(&layout, &columns);

    let count = entries.len();
    let mut entries: Vec<&BackupEntry> = entries.iter().collect();
    super::sort_rows(&layout, &mut entries, |entry, column| match column {
        "id" => super::SortKey::text(&entry.id),
        // By the end of the range, which is what orders two backups in time
        // when they were taken in the same second.
        "lsn" => super::SortKey::number(entry.end_lsn as f64),
        "stored" => super::SortKey::number(entry.compressed_bytes as f64),
        "original" => super::SortKey::number(entry.uncompressed_bytes as f64),
        "ratio" => super::SortKey::number(entry.ratio()),
        "detail" => super::SortKey::text(&entry.detail),
        "kind" => super::SortKey::text(&entry.kind),
        _ => super::SortKey::text(&entry.created_at),
    });

    let mut body = column![].spacing(0);
    for (index, entry) in entries.into_iter().enumerate() {
        let selected = entry.is_snapshot() && studio.restore_snapshot == entry.id;

        let action: Element<'_, Message> = if entry.is_snapshot() {
            container(small_button(
                if selected { "SELECTED" } else { "RESTORE…" },
                Some(Message::UseSnapshot(entry.id.clone())),
            ))
            .padding([2, 4])
            .into()
        } else {
            Space::new().into()
        };

        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(
                    entry.kind.to_uppercase(),
                    false,
                    if entry.is_snapshot() { theme::RUST } else { theme::TEXT_DIM },
                ),
                super::grid_cell(entry.id.clone(), false, theme::TEXT),
                super::grid_cell(entry.created_at.clone(), false, theme::TEXT_DIM),
                super::grid_cell(
                    format!("{}–{}", entry.start_lsn, entry.end_lsn),
                    true,
                    theme::TEXT_DIM,
                ),
                super::grid_cell(bytes(entry.compressed_bytes), true, theme::TEXT),
                super::grid_cell(bytes(entry.uncompressed_bytes), true, theme::TEXT_FAINT),
                super::grid_cell(format!("{:.1}×", entry.ratio()), true, theme::TEXT_FAINT),
                action,
                super::grid_cell(entry.detail.clone(), false, theme::TEXT_FAINT),
            ],
        );

        let styled = container(line).width(Length::Fixed(super::grid_total(&widths)));
        body = body.push(if selected {
            container(styled).style(theme::tile).into()
        } else {
            super::grid_stripe(index, styled.into())
        });
    }

    framed(
        format!("CATALOGUE · {}", count),
        None,
        super::grid_frame(
            &layout,
            &widths,
            super::grid_header(&layout, &columns, &widths),
            body.into(),
        ),
    )
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

fn restore_panel<'a>(
    studio: &'a Studio,
    snapshot: &'a BackupSnapshot,
    manageable: bool,
) -> Element<'a, Message> {
    let target = studio.restore_target();
    let flush = number(&snapshot.status, "wal_flush_secs").unwrap_or(0);

    let mut form = column![
        super::field("Destination directory", &studio.restore_dir, Message::RestoreDir),
        text("A new, empty directory. This is where the rebuilt database is written.")
            .size(10)
            .color(theme::TEXT_FAINT),
        column![
            text("Recover to").size(11).font(theme::MONO).color(theme::TEXT_DIM),
            pick_list(
                RestoreKind::ALL,
                Some(studio.restore_kind),
                Message::RestoreKindChanged,
            )
            .padding([6, 8])
            .text_size(13)
            .width(Fill),
        ]
        .spacing(4),
    ]
    .spacing(8);

    form = match studio.restore_kind {
        RestoreKind::Latest => form.push(
            text("Everything archived is replayed. Nothing is left out.")
                .size(11)
                .color(theme::OK),
        ),
        RestoreKind::Snapshot => form.push(column![
            super::field("Snapshot id", &studio.restore_snapshot, Message::RestoreSnapshot),
            text("Pick one from the catalogue, or paste an id. The copy is that snapshot exactly, \
                  with no log replayed past it.")
                .size(10)
                .color(theme::TEXT_FAINT),
        ]
        .spacing(4)),
        RestoreKind::Lsn => form.push(column![
            super::field("Commit sequence (LSN)", &studio.restore_lsn, Message::RestoreLsn),
            text("Exact: the cut lands on a commit marker, so a transaction is wholly in or \
                  wholly out.")
                .size(10)
                .color(theme::TEXT_FAINT),
        ]
        .spacing(4)),
        RestoreKind::Time => form.push(column![
            super::field("Instant", &studio.restore_time, Message::RestoreTime),
            text(format!(
                "RFC 3339, as in 2026-08-20T10:00:00Z. Coarser than an LSN: the cut is the end of \
                 the last bundle archived at or before that instant, so up to one flush interval \
                 — currently {}s — of commits can be missing.",
                flush
            ))
            .size(10)
            .color(theme::WARN),
        ]
        .spacing(4)),
    };

    // The warning that is always true of a restore, and the one that is only
    // true of a target that names a point in the past.
    form = form.push(
        container(
            column![
                text("BEFORE YOU RUN THIS").size(11).font(theme::MONO).color(theme::WARN),
                text(
                    "The destination is written into. Anything already in that directory can be \
                     overwritten, and there is no undo."
                )
                .size(11),
                text(
                    "The running database is not touched. This makes a copy — for verifying a \
                     backup, cloning, or recovering onto a fresh host."
                )
                .size(11)
                .color(theme::TEXT_DIM),
            ]
            .spacing(4),
        )
        .padding(8)
        .width(Fill)
        .style(theme::tile),
    );

    if target.discards_later_commits() {
        form = form.push(
            container(
                column![
                    text("THIS DISCARDS COMMITS").size(11).font(theme::MONO).color(theme::BAD),
                    text(format!(
                        "You asked for {}. Every commit after that point is deliberately left out \
                         of the copy. The report will say how many.",
                        target.label()
                    ))
                    .size(11),
                ]
                .spacing(3),
            )
            .padding(8)
            .width(Fill)
            .style(theme::error_box),
        );
    }

    form = form.push(
        row![
            primary_button(
                if studio.restore_running { "RESTORING…" } else { "RESTORE" },
                (manageable && !studio.restore_running).then_some(Message::RunRestore),
            ),
            Space::new().width(Fill),
        ]
        .spacing(6),
    );

    form = form.push(rule::horizontal(1).style(theme::hairline));
    form = form.push(in_place(&snapshot.status, &target));

    if let Some(report) = &studio.restore_report {
        form = form.push(rule::horizontal(1).style(theme::hairline));
        form = form.push(report_panel(report));
    }

    framed(
        "RESTORE",
        None,
        scrollable(form).height(Fill).style(theme::scroller).into(),
    )
}

/// The command that replaces the live database, which no statement can do.
fn in_place<'a>(status: &'a [Stat], target: &RestoreTarget) -> Element<'a, Message> {
    let command = dialect::in_place_restore_command(status, target);
    column![
        text("REPLACING THE LIVE DATABASE")
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
        text(
            "No statement can do it: swapping files under open tables leaves every reader looking \
             at storage that no longer describes what it thinks it does. Stop the server and start \
             it like this — it restores before opening anything, and --restore-replace renames the \
             existing directory aside rather than deleting it."
        )
        .size(10)
        .color(theme::TEXT_FAINT),
        container(text(command).size(10).font(theme::MONO).color(theme::TEXT))
            .padding(8)
            .width(Fill)
            .style(theme::tile),
    ]
    .spacing(5)
    .into()
}

fn report_panel(report: &RestoreReport) -> Element<'_, Message> {
    let lost = report.records_discarded > 0;

    let mut lines = column![
        text("LAST RESTORE").size(10).font(theme::MONO).color(theme::TEXT_DIM),
        pair("into", report.target_dir.clone()),
        pair("from snapshot", report.snapshot_id.clone()),
        pair("recovered to", report.recovered_to_lsn.clone()),
        pair("files", count(report.files_restored)),
        pair("bytes", bytes(report.bytes_restored)),
        pair("log bundles replayed", count(report.segments_replayed)),
        pair("records applied", count(report.records_applied)),
    ]
    .spacing(2);

    if lost {
        lines = lines.push(
            container(
                text(format!(
                    "{} records were discarded — they were past the recovery target, so the \
                     restored copy does not contain them.",
                    count(report.records_discarded)
                ))
                .size(11),
            )
            .padding(7)
            .width(Fill)
            .style(theme::error_box),
        );
    } else {
        lines = lines.push(
            container(text("Nothing was discarded.").size(11))
                .padding(7)
                .width(Fill)
                .style(theme::ok_box),
        );
    }

    lines.into()
}

fn pair(label: &'static str, v: String) -> Element<'static, Message> {
    row![
        text(label)
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_FAINT)
            .width(Length::Fixed(150.0)),
        text(v).size(10).font(theme::MONO).width(Fill),
    ]
    .spacing(6)
    .into()
}

// ---------------------------------------------------------------------------
// Reading the status result
// ---------------------------------------------------------------------------

fn value(status: &[Stat], metric: &str) -> Option<String> {
    status
        .iter()
        .find(|s| s.metric == metric)
        .map(|s| s.value.clone())
}

fn flag(status: &[Stat], metric: &str) -> Option<bool> {
    value(status, metric).map(|v| matches!(v.trim(), "true" | "t" | "on" | "1" | "yes"))
}

fn number(status: &[Stat], metric: &str) -> Option<u64> {
    value(status, metric)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(metric: &str, v: &str) -> Stat {
        Stat { metric: metric.into(), value: v.into(), detail: String::new() }
    }

    /// The three states the page has to tell apart. An absent `configured`
    /// must not read as configured, or a server with no destination shows an
    /// empty catalogue and buttons that fail.
    #[test]
    fn the_status_flags_are_read_conservatively() {
        assert_eq!(flag(&[], "configured"), None);
        assert_eq!(flag(&[stat("configured", "false")], "configured"), Some(false));
        assert_eq!(flag(&[stat("configured", "true")], "configured"), Some(true));
        assert_eq!(flag(&[stat("configured", "t")], "configured"), Some(true));
    }

    #[test]
    fn a_missing_number_is_absent_rather_than_zero() {
        assert_eq!(number(&[], "wal_flush_secs"), None);
        assert_eq!(number(&[stat("wal_flush_secs", "300")], "wal_flush_secs"), Some(300));
        assert_eq!(number(&[stat("wal_flush_secs", "n/a")], "wal_flush_secs"), None);
    }
}
