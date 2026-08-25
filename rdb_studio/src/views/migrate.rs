//! Importing a PostgreSQL database into this one.
//!
//! # What this is, and is not
//!
//! One direction, one shot. It reads a live PostgreSQL server and builds a new
//! RasterizedDB database from what it finds. It does not write back, it does
//! not stay in sync, and it refuses a target that already holds tables.
//!
//! It is deliberately not a "migrations" screen in the Flyway sense — there is
//! no versioned folder of `.sql` files and no schema-version table, because the
//! engine has no such concept and inventing one in a console would mean a
//! migration state that only this console knows about. Versioned migrations
//! belong in the repository that owns the schema; use the query editor, which
//! can open a `.sql` file and run it.
//!
//! # Why the preview is the default
//!
//! An import creates a database and streams every row of another one into it.
//! `DRY RUN` works the whole thing out — which tables, which columns, what each
//! type becomes, what it cannot carry — and writes nothing. Making somebody opt
//! *out* of finding that out first is the wrong way round, so the box starts
//! ticked.

use iced::widget::{Space, column, container, pick_list, row, rule, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{ImportField, Message, Studio};
use crate::dialect::{self, SslMode};
use crate::theme;

use super::{framed, primary_button, section_label, small_button};

/// Height of the source and target panels.
///
/// Fixed rather than `Shrink`, because the page scrolls: see the note in
/// [`view`] about `Fill` inside a `scrollable`. Tall enough for the target
/// panel, which is the longer of the two.
const FORM_HEIGHT: f32 = 430.0;

/// Height of the report panel.
const REPORT_HEIGHT: f32 = 400.0;

pub fn view(studio: &Studio) -> Element<'_, Message> {
    if !dialect::import_is_supported(studio.flavor()) {
        return framed("MIGRATE", None, unsupported());
    }

    let request = &studio.import;

    // Every panel here is given an explicit height. `framed` fills the space
    // it is handed, and inside a vertical `scrollable` that space is unbounded
    // — so a `Fill` height resolves to nothing and the panel collapses to its
    // border. That is what happened to the whole form on this screen: the
    // explainer and the report placeholder rendered, and the source and target
    // panels between them were invisible.
    let body = column![
        explainer(),
        row![
            container(source(studio)).width(Fill).height(Length::Fixed(FORM_HEIGHT)),
            container(target(studio))
                .width(Length::Fixed(430.0))
                .height(Length::Fixed(FORM_HEIGHT)),
        ]
        .spacing(10),
        container(report(studio)).height(Length::Fixed(REPORT_HEIGHT)),
    ]
    .spacing(10);

    framed(
        "MIGRATE",
        Some(
            text(if request.dry_run { "preview" } else { "WILL WRITE" })
                .size(11)
                .font(theme::MONO)
                .color(if request.dry_run { theme::TEXT_DIM } else { theme::BAD })
                .into(),
        ),
        scrollable(body).height(Fill).style(theme::scroller).into(),
    )
}

fn unsupported() -> Element<'static, Message> {
    container(
        column![
            text("This screen imports into RasterizedDB").size(15).color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            text(
                "You are connected to PostgreSQL, which is the side an import reads *from*. \
                 Connect to a RasterizedDB server and point this at your PostgreSQL one."
            )
            .size(12)
            .color(theme::TEXT_DIM),
        ]
        .spacing(10)
        .max_width(700),
    )
    .padding(14)
    .into()
}

fn explainer() -> Element<'static, Message> {
    container(
        column![
            text("ONE DIRECTION, ONE SHOT")
                .size(11)
                .font(theme::MONO)
                .color(theme::TEXT_DIM),
            text(
                "Reads a live PostgreSQL database and builds a new one here from what it finds: \
                 tables, columns, types, nullability, defaults, primary keys, unique constraints \
                 and foreign keys, then every row. It does not write back to the source, it does \
                 not stay in sync afterwards, and it refuses a target that already holds tables."
            )
            .size(11)
            .color(theme::TEXT_DIM),
            text(
                "Anything a type here cannot carry faithfully stops the import and names the \
                 column, rather than rounding it. The preview tells you about those before \
                 anything is created."
            )
            .size(11)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(4),
    )
    .padding(9)
    .width(Fill)
    .style(theme::tile)
    .into()
}

fn source(studio: &Studio) -> Element<'_, Message> {
    let request = &studio.import;
    let field = |label: &'static str, value: &str, which: ImportField| {
        super::field(label, value, move |v| Message::ImportField(which, v))
    };

    let body = column![
        row![
            field("Host", &request.host, ImportField::Host),
            container(field("Port", &request.port, ImportField::Port))
                .width(Length::Fixed(110.0)),
        ]
        .spacing(8),
        row![
            field("Database", &request.database, ImportField::Database),
            field("Schema", &request.schema, ImportField::Schema),
        ]
        .spacing(8),
        row![
            field("User", &request.user, ImportField::User),
            secret("Password", &request.password),
        ]
        .spacing(8),
        column![
            text("TLS").size(11).font(theme::MONO).color(theme::TEXT_DIM),
            pick_list(SslMode::ALL, Some(request.sslmode), |mode| {
                Message::ImportSslMode(mode)
            })
            .padding([6, 8])
            .text_size(13)
            .width(Length::Fixed(200.0)),
            text(request.sslmode.caveat())
                .size(10)
                .color(match request.sslmode {
                    SslMode::Disable | SslMode::Require => theme::WARN,
                    _ => theme::TEXT_FAINT,
                }),
        ]
        .spacing(4),
    ]
    .spacing(10);

    framed("SOURCE · POSTGRESQL", None, body.into())
}

fn target(studio: &Studio) -> Element<'_, Message> {
    let request = &studio.import;
    let running = studio.import_running;

    let body = column![
        super::field("New database name", &request.target, |v| {
            Message::ImportField(ImportField::Target, v)
        }),
        text("Created by the import. An existing database that already holds tables is refused.")
            .size(10)
            .color(theme::TEXT_FAINT),
        super::field("Exclude tables", &request.exclude, |v| {
            Message::ImportField(ImportField::Exclude, v)
        }),
        text("Comma separated. Empty imports every table in the schema.")
            .size(10)
            .color(theme::TEXT_FAINT),
        rule::horizontal(1).style(theme::hairline),
        super::toggle("Preview only — work it out and write nothing", request.dry_run, |v| {
            Message::ImportDryRun(v)
        }),
        super::toggle(
            "Without constraints — faster, and you fix them up afterwards",
            request.skip_constraints,
            |v| Message::ImportSkipConstraints(v),
        ),
        warning(request.dry_run),
        row![
            primary_button(
                if running {
                    "WORKING…"
                } else if request.dry_run {
                    "PREVIEW"
                } else {
                    "IMPORT"
                },
                (!running).then_some(Message::RunImport),
            ),
            Space::new().width(Fill),
        ],
    ]
    .spacing(8);

    framed("TARGET · HERE", None, body.into())
}

fn warning(dry_run: bool) -> Element<'static, Message> {
    if dry_run {
        return container(
            text(
                "Nothing will be created. The report below is what an import would do.",
            )
            .size(11),
        )
        .padding(8)
        .width(Fill)
        .style(theme::ok_box)
        .into();
    }

    container(
        column![
            text("THIS WRITES").size(11).font(theme::MONO).color(theme::BAD),
            text(
                "A database is created here and every row of the source is streamed into it. On \
                 a large source that is a long time and a lot of disk. It cannot be rolled back — \
                 to undo it, drop the database it created.",
            )
            .size(11),
        ]
        .spacing(3),
    )
    .padding(8)
    .width(Fill)
    .style(theme::error_box)
    .into()
}

fn report(studio: &Studio) -> Element<'_, Message> {
    if studio.import_report.is_empty() {
        return container(
            text("Run a preview to see what would be imported.")
                .size(12)
                .color(theme::TEXT_FAINT),
        )
        .padding(10)
        .into();
    }

    let mut lines = column![].spacing(1);
    for (index, line) in studio.import_report.iter().enumerate() {
        lines = lines.push(
            text(line.clone())
                .size(11)
                .font(theme::MONO)
                .color(report_colour(&studio.import_report, index))
                .width(Fill),
        );
    }

    framed(
        "REPORT",
        Some(small_button("COPY", Some(Message::CopyImportReport))),
        column![
            section_label(if studio.import.dry_run { "PREVIEW" } else { "IMPORT" }),
            scrollable(lines).height(Fill).style(theme::scroller),
        ]
        .spacing(2)
        .into(),
    )
}

/// What colour a line of the report is.
///
/// Structural rather than keyword-matched. The server's report ends with a
/// section — "Things that did not come across unchanged" — and *everything*
/// under it is a caveat, whether or not the sentence happens to contain a word
/// like "cannot". That section is the reason to read the report at all, so it
/// is found by locating the heading and colouring what follows.
fn report_colour(report: &[String], index: usize) -> iced::Color {
    let caveats_begin = report
        .iter()
        .position(|line| line.to_ascii_lowercase().contains("did not come across unchanged"));

    match caveats_begin {
        Some(heading) if index == heading => theme::WARN,
        Some(heading) if index > heading => theme::TEXT_DIM,
        _ => {
            // Above the caveats, the only line worth colouring is a failure.
            let lowered = report[index].to_ascii_lowercase();
            if lowered.contains("cannot")
                || lowered.contains("refus")
                || lowered.contains("failed")
                || lowered.starts_with("error")
            {
                theme::BAD
            } else {
                theme::TEXT
            }
        }
    }
}

fn secret<'a>(label: &'a str, value: &'a str) -> Element<'a, Message> {
    column![
        text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        iced::widget::text_input("", value)
            .on_input(|v| Message::ImportField(ImportField::Password, v))
            .secure(true)
            .padding([6, 8])
            .size(13)
            .font(theme::MONO)
            .style(theme::input),
    ]
    .spacing(4)
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Vec<String> {
        vec![
            "Imported 4 tables from … into \"imported\"".to_string(),
            "  users     3 rows   0.0s".to_string(),
            "".to_string(),
            "9 Things that did not come across unchanged:".to_string(),
            "  - 4 sequences in the source were not imported".to_string(),
            "  - NUMERIC keeps the value but not the declared precision".to_string(),
        ]
    }

    /// The caveats are the reason to read the report, and none of them
    /// contains a word a keyword match would find.
    #[test]
    fn the_caveats_are_found_by_the_heading_not_by_keywords() {
        let report = report();
        assert_eq!(report_colour(&report, 3), theme::WARN, "the heading stands out");
        assert_eq!(report_colour(&report, 4), theme::TEXT_DIM);
        assert_eq!(report_colour(&report, 5), theme::TEXT_DIM);
        assert_eq!(report_colour(&report, 0), theme::TEXT, "the summary is ordinary");
    }

    /// A report with no caveat section still colours a failure.
    #[test]
    fn a_failure_above_the_caveats_is_still_red() {
        let report = vec!["cannot carry column users.weird".to_string()];
        assert_eq!(report_colour(&report, 0), theme::BAD);
    }
}
