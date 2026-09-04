//! Activity: what the server has been running, what it cost, and what it read.
//!
//! # Two lists, two questions
//!
//! *What has this server been doing* is a list in time order — the one to open
//! when a page is slow right now. *Which statement is the expensive one* is a
//! list in cost order, folded so that a hundred executions of one shape are one
//! row. They are different questions and the page shows both rather than
//! picking, because the answer to the first is usually a statement you then
//! want the second's totals for.
//!
//! # `SCANNED` is the column this page exists for — for scan-bound work
//!
//! `ROWS` is what the client received; `SCANNED` is what the engine had to read
//! to find it, after the filter index excluded what it could. The ratio is the
//! diagnosis:
//!
//! - a few rows out of millions scanned is a predicate nothing indexes;
//! - millions out of millions is a statement that asked for the table;
//! - rows returned with nothing scanned came from the query cache;
//! - **slow, with nothing scanned, is a statement whose cost was not the scan.**
//!
//! That last one is why the column is amber on the ratio and never on `MS`
//! alone. A CTE or a derived table is built as a real relation before the outer
//! statement reads anything, so no scan happens and `SCANNED` reports the
//! statement honestly and uselessly: a high `MS` with a low `SCANNED` means
//! "the cost was elsewhere", not "this cannot be the slow one".
//!
//! `BUILT` and `BUILT MS` are that elsewhere, and they are what turns "the cost
//! was not the scan" into a number. They sit beside `SCANNED` rather than being
//! folded into one cost column because they are different kinds of figure:
//! `SCANNED` is read against `ROWS` as a ratio, while a count of relations
//! built means something on its own and has no denominator. `BUILT MS` goes
//! amber once it is more than half the statement's time — the point at which
//! the statement is not slow for any reason this page's other columns can show.
//!
//! A server that does not report them, which is every PostgreSQL server, shows
//! a dash rather than a zero: PostgreSQL does not build these relations at all,
//! which is not the same as building them instantly.
//!
//! [`crate::model::scan_is_heavy`] decides which of those is worth colouring,
//! and deliberately says nothing about small tables: the engine scans anything
//! under `filter.min_table_rows` without consulting its index at all, so
//! flagging those would flag every statement on a small database.
//!
//! # What PostgreSQL answers instead
//!
//! PostgreSQL keeps no history of finished statements, so the top pane there is
//! `pg_stat_activity` — what its backends are executing at this instant — under
//! a heading that says so, with the two figures it cannot produce shown as
//! dashes rather than zeroes. The bottom pane is `pg_stat_statements`, which is
//! the real equivalent, when the extension is loaded.

use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let flavor = studio.flavor();

    let Some(snapshot) = &studio.activity else {
        return framed(
            "ACTIVITY",
            Some(small_button("RELOAD", Some(Message::ReloadActivity))),
            container(text("Loading…").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };

    // Both halves silent means the server records nothing at all, and the page
    // has nothing to say that the explanation does not say better.
    if !snapshot.recent_supported && !snapshot.shapes_supported {
        return framed("ACTIVITY", Some(reload_button()), unsupported(flavor));
    }

    let body = column![
        summary(snapshot, flavor),
        recent(studio, snapshot, flavor),
        shapes(studio, snapshot, flavor),
    ]
    .spacing(10);

    framed(
        "ACTIVITY",
        Some(reload_button()),
        scrollable(body).height(Fill).style(theme::scroller).into(),
    )
}

fn reload_button() -> Element<'static, Message> {
    row![
        small_button("RELOAD", Some(Message::ReloadActivity)),
        iced::widget::button(text("FORGET ALL").size(11).font(theme::MONO))
            .padding([5, 10])
            .style(theme::destructive)
            .on_press(Message::ResetStatementStats),
    ]
    .spacing(6)
    .into()
}

/// The same trap the backup, replication and storage pages carry: a server that
/// would not answer must not render as one that answered "nothing".
fn unsupported(flavor: Flavor) -> Element<'static, Message> {
    container(
        column![
            text("This server does not report what it runs").size(15).color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            // Both halves are silent here, and on PostgreSQL they fail for two
            // unrelated reasons — so both are given rather than the one that
            // happens to be listed first.
            text(dialect::recent_statements_missing(flavor)).size(12).color(theme::TEXT_DIM),
            text(dialect::statement_shapes_missing(flavor)).size(12).color(theme::TEXT_DIM),
        ]
        .spacing(10)
        .max_width(720),
    )
    .padding(14)
    .into()
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

fn summary(snapshot: &ActivitySnapshot, flavor: Flavor) -> Element<'static, Message> {
    let failures = snapshot.failures();

    let slowest = snapshot
        .slowest()
        .map(|record| {
            (millis(record.duration_ms), Some(super::elide(&record.statement, 44).to_string()))
        })
        .unwrap_or_else(|| ("—".to_string(), None));

    // Rows read against rows returned, over everything the ring holds. On a
    // healthy database this is a small multiple; a large one is a table being
    // read to answer a question about a few of its rows.
    let (read, read_note) = match snapshot.scan_totals() {
        Some((scanned, returned)) => (
            count(scanned),
            Some(format!("{} returned · {}", count(returned), ratio(scanned, returned))),
        ),
        None => (
            "—".to_string(),
            Some("PostgreSQL does not report this for a running statement".to_string()),
        ),
    };

    row![
        super::tile(
            match flavor {
                Flavor::Rasterized => "STATEMENTS",
                Flavor::Postgres => "RUNNING",
            },
            count(snapshot.recent.len() as u64),
            Some(
                match flavor {
                    Flavor::Rasterized => "kept by the server, newest first",
                    Flavor::Postgres => "backends executing something",
                }
                .to_string()
            ),
        ),
        super::tile("SHAPES", count(snapshot.shapes.len() as u64), Some("distinct statements".to_string())),
        super::tile("SLOWEST", slowest.0, slowest.1),
        super::tile("ROWS SCANNED", read, read_note),
        super::tile(
            "FAILED",
            count(failures as u64),
            Some(
                if failures == 0 {
                    "nothing was refused".to_string()
                } else {
                    "statements the server refused".to_string()
                }
            ),
        ),
    ]
    .spacing(10)
    .into()
}

// ---------------------------------------------------------------------------
// Recent
// ---------------------------------------------------------------------------

fn recent<'a>(
    studio: &'a Studio,
    snapshot: &'a ActivitySnapshot,
    flavor: Flavor,
) -> Element<'a, Message> {
    let mut head = column![section_label(dialect::recent_statements_label(flavor))].spacing(2);
    if let Some(caveat) = dialect::recent_statements_caveat(flavor) {
        head = head.push(text(caveat).size(10).color(theme::TEXT_FAINT).width(Length::Fixed(760.0)));
    }

    if !snapshot.recent_supported {
        return container(
            column![
                head,
                text(dialect::recent_statements_missing(flavor)).size(12).color(theme::TEXT_DIM),
            ]
            .spacing(6),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    if snapshot.recent.is_empty() {
        return container(
            column![
                head,
                text(match flavor {
                    Flavor::Rasterized =>
                        "Nothing recorded. Either the server has just started, somebody has just \
                         run RESET QUERY STATS, or recording is off — SHOW CONFIG reports \
                         query_stats.enabled.",
                    Flavor::Postgres => "No other backend is executing a statement.",
                })
                .size(12)
                .color(theme::TEXT_FAINT),
            ]
            .spacing(6),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    let layout = super::GridLayout::new(studio, "activity:recent");
    let columns = vec![
        super::GridColumn::new("started", "STARTED", 176.0),
        super::GridColumn::new("ms", "MS", 88.0).right(),
        super::GridColumn::new("rows", "ROWS", 84.0).right(),
        super::GridColumn::new("scanned", "SCANNED", 104.0).right(),
        super::GridColumn::new("built", "BUILT", 72.0).right(),
        super::GridColumn::new("built_ms", "BUILT MS", 96.0).right(),
        super::GridColumn::new("status", "STATUS", 74.0),
        super::GridColumn::new("database", "DATABASE", 110.0),
        super::GridColumn::new("user", "USER", 110.0),
        super::GridColumn::new("statement", "STATEMENT", 520.0),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    // Newest first as it arrives, which is the order this list is *for*; a
    // header puts it in some other order for as long as somebody is asking a
    // different question — the slow ones, or everything one user ran.
    let mut records: Vec<&StatementRecord> = snapshot.recent.iter().collect();
    super::sort_rows(&layout, &mut records, |record, column| match column {
        "ms" => super::SortKey::number(record.duration_ms),
        "rows" => optional(record.rows),
        "scanned" => optional(record.rows_scanned),
        "built" => optional(record.materialised),
        "built_ms" => match record.materialised_ms {
            Some(ms) => super::SortKey::number(ms),
            None => super::SortKey::Empty,
        },
        "status" => super::SortKey::text(&record.status),
        "database" => super::SortKey::text(&record.database),
        "user" => super::SortKey::text(&record.username),
        "statement" => super::SortKey::text(&record.statement),
        _ => super::SortKey::text(&record.started_at),
    });

    // The list is capped at `app::RECENT_STATEMENTS`, which is below the
    // threshold the virtualised grids exist for, so these are built in full.
    let mut body = column![].spacing(0);
    for (index, record) in records.into_iter().enumerate() {
        let failed = record.failed();

        let mut line = column![super::grid_row(
            &widths,
            vec![
                super::grid_cell(record.started_at.clone(), false, theme::TEXT_DIM),
                super::grid_cell(
                    millis(record.duration_ms),
                    true,
                    if record.duration_ms >= SLOW_MS { theme::WARN } else { theme::TEXT_DIM },
                ),
                super::grid_cell(maybe_count(record.rows), true, theme::TEXT_DIM),
                super::grid_cell(
                    maybe_count(record.rows_scanned),
                    true,
                    if record.heavy() { theme::WARN } else { theme::TEXT_DIM },
                ),
                super::grid_cell(maybe_count(record.materialised), true, theme::TEXT_DIM),
                // Amber once building the relations is most of the statement:
                // that is the reading this column exists to make possible, and
                // it is the one `SCANNED` cannot show.
                super::grid_cell(
                    maybe_millis(record.materialised_ms),
                    true,
                    if dominated(record.materialised_ms, record.duration_ms) {
                        theme::WARN
                    } else {
                        theme::TEXT_DIM
                    },
                ),
                container(super::pill(
                    status_label(&record.status, failed),
                    if failed { theme::BAD } else { theme::OK },
                ))
                .padding([3, 6])
                .into(),
                super::grid_cell(record.database.clone(), false, theme::TEXT_FAINT),
                super::grid_cell(record.username.clone(), false, theme::TEXT_FAINT),
                super::grid_cell(
                    super::elide(&record.statement, 200).to_string(),
                    false,
                    theme::TEXT,
                ),
            ],
        )]
        .spacing(0);

        // The reason a statement was refused is the whole content of the row,
        // and far too long to live in a column.
        if !record.error.is_empty() {
            line = line.push(
                container(
                    text(super::elide(&record.error, 220).to_string())
                        .size(10)
                        .font(theme::MONO)
                        .color(theme::BAD),
                )
                .padding([0, 12]),
            );
        }

        body = body.push(super::grid_stripe(
            index,
            container(line).width(Length::Fixed(total)).into(),
        ));
    }

    container(
        column![
            head,
            super::grid_frame(
                &layout,
                &widths,
                super::grid_header(&layout, &columns, &widths),
                body.into(),
            ),
        ]
        .spacing(2),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

fn shapes<'a>(
    studio: &'a Studio,
    snapshot: &'a ActivitySnapshot,
    flavor: Flavor,
) -> Element<'a, Message> {
    // The label says what the order actually is. "COSTLIEST FIRST" over a list
    // somebody has re-sorted by calls is a heading that contradicts the grid
    // under it, which is worse than no heading at all.
    let head = section_label(if studio.sort.contains_key("activity:shapes") {
        "BY SHAPE"
    } else {
        "BY SHAPE · COSTLIEST FIRST"
    });

    if !snapshot.shapes_supported {
        return container(
            column![
                head,
                text(dialect::statement_shapes_missing(flavor)).size(12).color(theme::TEXT_DIM),
            ]
            .spacing(6),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    if snapshot.shapes.is_empty() {
        return container(
            column![head, text("Nothing recorded yet.").size(12).color(theme::TEXT_FAINT)]
                .spacing(6),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    let layout = super::GridLayout::new(studio, "activity:shapes");
    let columns = vec![
        super::GridColumn::new("calls", "CALLS", 84.0).right(),
        super::GridColumn::new("total", "TOTAL MS", 104.0).right(),
        super::GridColumn::new("mean", "MEAN MS", 96.0).right(),
        super::GridColumn::new("max", "MAX MS", 96.0).right(),
        super::GridColumn::new("rows", "ROWS", 92.0).right(),
        super::GridColumn::new("scanned", "SCANNED", 104.0).right(),
        super::GridColumn::new("built", "BUILT", 72.0).right(),
        super::GridColumn::new("built_ms", "BUILT MS", 96.0).right(),
        super::GridColumn::new("errors", "ERRORS", 84.0).right(),
        super::GridColumn::new("statement", "STATEMENT", 560.0),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    let mut shapes: Vec<&StatementShape> = snapshot.shapes.iter().collect();
    super::sort_rows(&layout, &mut shapes, |shape, column| match column {
        "calls" => super::SortKey::number(shape.calls as f64),
        "mean" => super::SortKey::number(shape.mean_ms),
        "max" => super::SortKey::number(shape.max_ms),
        "rows" => optional(shape.rows),
        "scanned" => optional(shape.rows_scanned),
        "built" => optional(shape.materialised),
        "built_ms" => match shape.materialised_ms {
            Some(ms) => super::SortKey::number(ms),
            None => super::SortKey::Empty,
        },
        "errors" => optional(shape.errors),
        "statement" => super::SortKey::text(&shape.statement),
        _ => super::SortKey::number(shape.total_ms),
    });

    let mut body = column![].spacing(0);
    for (index, shape) in shapes.into_iter().enumerate() {
        let failing = shape.errors.unwrap_or(0) > 0;

        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(count(shape.calls), true, theme::TEXT),
                super::grid_cell(millis(shape.total_ms), true, theme::TEXT),
                super::grid_cell(
                    millis(shape.mean_ms),
                    true,
                    if shape.mean_ms >= SLOW_MS { theme::WARN } else { theme::TEXT_DIM },
                ),
                super::grid_cell(millis(shape.max_ms), true, theme::TEXT_DIM),
                super::grid_cell(maybe_count(shape.rows), true, theme::TEXT_DIM),
                super::grid_cell(
                    maybe_count(shape.rows_scanned),
                    true,
                    if shape.heavy() { theme::WARN } else { theme::TEXT_DIM },
                ),
                super::grid_cell(maybe_count(shape.materialised), true, theme::TEXT_DIM),
                super::grid_cell(
                    maybe_millis(shape.materialised_ms),
                    true,
                    if dominated(shape.materialised_ms, shape.total_ms) {
                        theme::WARN
                    } else {
                        theme::TEXT_DIM
                    },
                ),
                super::grid_cell(
                    maybe_count(shape.errors),
                    true,
                    if failing { theme::BAD } else { theme::TEXT_FAINT },
                ),
                super::grid_cell(
                    super::elide(&shape.statement, 220).to_string(),
                    false,
                    theme::TEXT,
                ),
            ],
        );

        body = body.push(super::grid_stripe(
            index,
            container(line).width(Length::Fixed(total)).into(),
        ));
    }

    container(
        column![
            head,
            super::grid_frame(
                &layout,
                &widths,
                super::grid_header(&layout, &columns, &widths),
                body.into(),
            ),
        ]
        .spacing(2),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

/// A duration a server may not report at all, rendered as a dash rather than as
/// zero — PostgreSQL does not build these relations and so reports nothing,
/// which is not the same as building them instantly.
fn maybe_millis(value: Option<f64>) -> String {
    match value {
        Some(ms) => millis(ms),
        None => "—".to_string(),
    }
}

/// Whether building the relations was most of what the statement did.
///
/// The threshold is half, and it is the whole point of the column: a statement
/// that spent more time materialising than doing everything else put together
/// is not slow for a reason `SCANNED` can show. Anything below that is
/// ordinary and is not coloured, because a warning colour that is always on is
/// one nobody reads.
fn dominated(materialised_ms: Option<f64>, total_ms: f64) -> bool {
    matches!(materialised_ms, Some(ms) if total_ms > 0.0 && ms > total_ms / 2.0)
}

/// A counter a server may not report at all, which is not the same as zero:
/// it sorts as missing rather than as the smallest number.
fn optional(value: Option<u64>) -> super::SortKey {
    match value {
        Some(value) => super::SortKey::number(value as f64),
        None => super::SortKey::Empty,
    }
}

/// A duration long enough to be worth pointing at, in milliseconds.
///
/// Not a threshold anybody tuned: it is the point past which a statement is
/// visible in a page load rather than lost in it.
const SLOW_MS: f64 = 100.0;

/// Milliseconds, at a precision that suits the magnitude.
///
/// A fixed number of decimals cannot serve both ends of this column: three
/// would print a two-minute statement as `126431.000`, and none would print
/// most statements as `0`.
pub fn millis(value: f64) -> String {
    if !value.is_finite() {
        return "—".to_string();
    }
    if value >= 10_000.0 {
        return format!("{:.1} s", value / 1_000.0);
    }
    if value >= 100.0 {
        return format!("{:.0}", value);
    }
    if value >= 1.0 {
        return format!("{:.2}", value);
    }
    format!("{:.3}", value)
}

/// A figure the server could not produce reads as a dash, never as a zero.
fn maybe_count(value: Option<u64>) -> String {
    match value {
        Some(value) => count(value),
        None => "—".to_string(),
    }
}

/// Rows read per row returned, as the summary tile says it.
fn ratio(scanned: u64, returned: u64) -> String {
    if returned == 0 {
        return if scanned == 0 { "nothing read".to_string() } else { "nothing returned".to_string() };
    }
    let ratio = scanned as f64 / returned as f64;
    if ratio < 10.0 {
        format!("{:.1}× read per row", ratio)
    } else {
        format!("{}× read per row", ratio.round() as u64)
    }
}

/// What the status pill says.
///
/// RasterizedDB answers `ok` or `error`; PostgreSQL answers a backend state
/// like `active` or `idle in transaction`. Both are shown as they came, upper
/// cased, rather than being mapped onto a shared vocabulary that would have to
/// invent a meaning for one of them.
fn status_label(status: &str, failed: bool) -> String {
    if status.is_empty() {
        return if failed { "ERROR".to_string() } else { "OK".to_string() };
    }
    status.to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every statement here is sub-millisecond, and a column of zeroes would
    /// say the server is infinitely fast rather than that the format is wrong.
    #[test]
    fn a_fast_statement_is_not_rendered_as_zero() {
        assert_eq!(millis(0.084), "0.084");
        assert_eq!(millis(0.0), "0.000");
        assert_eq!(millis(1.485), "1.49");
        assert_eq!(millis(150.764), "151");
        assert_eq!(millis(12_400.0), "12.4 s");
    }

    #[test]
    fn a_figure_the_server_cannot_produce_is_not_a_zero() {
        assert_eq!(maybe_count(None), "—");
        assert_eq!(maybe_count(Some(0)), "0");
    }

    /// The reading the page is for: a full scan that returned almost nothing.
    #[test]
    fn the_scan_ratio_flags_the_case_it_is_for() {
        // Three rows out of ten million: a predicate nothing indexes.
        assert!(scan_is_heavy(Some(3), Some(10_000_000)));
        // A full scan that found nothing is the same missing index, and must
        // not be hidden by dividing by zero.
        assert!(scan_is_heavy(Some(0), Some(10_000_000)));
        // Reading the table to return the table is not a fault.
        assert!(!scan_is_heavy(Some(10_000_000), Some(10_000_000)));
        // A small table is scanned deliberately; flagging it would flag
        // everything on a small database.
        assert!(!scan_is_heavy(Some(1), Some(500)));
        // PostgreSQL answers neither figure, and silence is not a fault.
        assert!(!scan_is_heavy(None, None));
        assert!(!scan_is_heavy(Some(1), None));
    }

    #[test]
    fn the_summary_ratio_says_which_way_round_it_is() {
        assert_eq!(ratio(0, 0), "nothing read");
        assert_eq!(ratio(1_000, 0), "nothing returned");
        assert_eq!(ratio(300, 100), "3.0× read per row");
        assert_eq!(ratio(1_000_000, 10), "100000× read per row");
    }

    /// A failed statement whose server sent no status word still reads as one.
    #[test]
    fn a_status_is_never_invented_but_is_never_blank() {
        assert_eq!(status_label("ok", false), "OK");
        assert_eq!(status_label("", true), "ERROR");
        assert_eq!(status_label("", false), "OK");
        assert_eq!(status_label("idle in transaction", false), "IDLE IN TRANSACTION");
    }
}
