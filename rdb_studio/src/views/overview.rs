//! The database overview: the six figures that matter, then every metric the
//! server reported, grouped.
//!
//! The tiles are chosen rather than taken from the top of the list. A metric
//! list is a list; a console's front page should answer "is this database
//! healthy and how big is it" without reading one.

use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Element, Fill};

use crate::app::{Message, Studio};
use crate::model::*;
use crate::theme;

use super::{format_metric, framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let stats = &studio.db_stats;

    let find = |metric: &str| stats.iter().find(|s| s.metric == metric);
    let figure = |metric: &str| {
        find(metric)
            .map(|s| format_metric(metric, &s.value))
            .unwrap_or_else(|| "—".to_string())
    };

    let headline = row![
        super::tile("TABLES", figure("tables"), None),
        super::tile("ROW SLOTS", figure("row_slots"), Some("upper bound on live rows".into())),
        super::tile("TOTAL SIZE", figure("total_bytes"), None),
        super::tile("DATA", figure("data_bytes"), None),
        super::tile("INDEX", figure("index_bytes"), None),
        super::tile(
            "RECLAIMABLE",
            figure("reclaimable_bytes"),
            Some("a VACUUM would return this".into()),
        ),
    ]
    .spacing(10);

    let (durability_text, durability_colour) = durability(studio);
    let identity = row![
        labelled("DATABASE", find("name").map(|s| s.value.clone()).unwrap_or_default()),
        labelled("LOCATION", find("location").map(|s| s.value.clone()).unwrap_or_default()),
        labelled(
            "VERSION",
            find("server_version").map(|s| s.value.clone()).unwrap_or_default(),
        ),
        labelled_in(theme::TEXT_FAINT, "DURABILITY", durability_text, durability_colour),
    ]
    .spacing(24);

    let grouped = group_stats(stats);
    let mut sections = column![].spacing(10);
    for (section, metrics) in &grouped {
        // The headline figures are already above; repeating them here doubles
        // the page for nothing.
        let metrics: Vec<Stat> = metrics
            .iter()
            .cloned()
            .filter(|s: &Stat| !HEADLINE.contains(&s.metric.as_str()))
            .collect();
        if metrics.is_empty() {
            continue;
        }

        // Sized to its own contents: a section of four metrics should not
        // leave the height of a full grid behind it.
        let height = 26.0 + metrics.len() as f32 * 19.0;
        sections = sections.push(
            column![
                section_label(section.to_ascii_uppercase()),
                container(super::metric_grid(
                    studio,
                    format!("overview:{}", section),
                    &metrics,
                ))
                .height(iced::Length::Fixed(height)),
            ]
            .spacing(2),
        );
    }

    let body = column![
        headline,
        identity,
        scrollable(sections).height(Fill).style(theme::scroller),
    ]
    .spacing(14);

    framed(
        "DATABASE STATISTICS",
        Some(small_button("REFRESH", Some(Message::RefreshAll))),
        if stats.is_empty() {
            column![
                text("No statistics loaded yet.").size(13).color(theme::TEXT_DIM),
                Space::new().height(Fill),
            ]
            .into()
        } else {
            body.into()
        },
    )
}

/// Metrics already shown as tiles, which the grouped list therefore skips.
const HEADLINE: &[&str] = &[
    "tables",
    "row_slots",
    "total_bytes",
    "data_bytes",
    "index_bytes",
    "reclaimable_bytes",
    "name",
    "location",
    "server_version",
];

fn labelled(label: &'static str, value: String) -> Element<'static, Message> {
    labelled_in(theme::TEXT_FAINT, label, value, theme::TEXT)
}

/// `labelled`, with the value in a colour of its own.
fn labelled_in(
    label_colour: iced::Color,
    label: &'static str,
    value: String,
    value_colour: iced::Color,
) -> Element<'static, Message> {
    column![
        text(label).size(10).font(theme::MONO).color(label_colour),
        text(if value.is_empty() { "—".to_string() } else { value })
            .size(13)
            .font(theme::MONO)
            .color(value_colour),
    ]
    .spacing(2)
    .into()
}

/// How the server answers `synchronous_commit`, and whether that is worth
/// noticing.
///
/// On the front page rather than only in the configuration list, because a
/// server committing asynchronously is trading durability for speed and that is
/// not a thing to discover by filtering twenty-odd settings. It says what is at
/// risk, not just that something is: "asynchronous" alone invites the reading
/// that committed data may be lost at any time, which is not what it means.
///
/// Looked up by bare key, which works for both servers: RasterizedDB reports it
/// as `wal.synchronous_commit` and PostgreSQL as a `pg_settings` row of the same
/// name. `local`, `remote_write` and `remote_apply` are PostgreSQL's
/// standby-related levels; all of them still wait for the local log, so they are
/// shown as themselves rather than flattened into "synchronous".
fn durability(studio: &Studio) -> (String, iced::Color) {
    durability_from(&studio.config)
}

fn durability_from(config: &[ConfigEntry]) -> (String, iced::Color) {
    let Some(entry) = config
        .iter()
        .find(|e| e.key.eq_ignore_ascii_case("synchronous_commit"))
    else {
        // The configuration has not been loaded yet, or this server does not
        // report the setting. Either way, saying nothing beats guessing.
        return (String::new(), theme::TEXT);
    };

    match entry.value.trim().to_ascii_lowercase().as_str() {
        "off" | "false" | "0" => {
            let window = config
                .iter()
                .find(|e| {
                    e.key.eq_ignore_ascii_case("writer_delay_ms")
                        || e.key.eq_ignore_ascii_case("wal_writer_delay")
                })
                .map(|e| format!(" · up to {} at risk", delay_label(&e.value)))
                .unwrap_or_default();
            (format!("asynchronous{}", window), theme::WARN)
        }
        "on" | "true" | "1" => ("synchronous".to_string(), theme::TEXT),
        other => (other.to_string(), theme::TEXT),
    }
}

/// The writer delay as a person would say it. RasterizedDB reports a bare
/// number of milliseconds; PostgreSQL reports one with its unit already on.
fn delay_label(value: &str) -> String {
    let trimmed = value.trim();
    match trimmed.parse::<u64>() {
        Ok(ms) if ms >= 1000 && ms % 1000 == 0 => format!("{}s", ms / 1000),
        Ok(ms) => format!("{}ms", ms),
        Err(_) => trimmed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(section: &str, key: &str, value: &str) -> ConfigEntry {
        ConfigEntry {
            section: section.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            hot: true,
            set_with: String::new(),
            help: String::new(),
        }
    }

    /// The RasterizedDB shape: `wal.synchronous_commit` plus a bare millisecond
    /// count for the writer delay.
    #[test]
    fn an_asynchronous_rasterizeddb_server_says_what_is_at_risk() {
        let config = vec![
            entry("wal", "synchronous_commit", "off"),
            entry("wal", "writer_delay_ms", "200"),
        ];
        let (label, colour) = durability_from(&config);
        assert_eq!(label, "asynchronous · up to 200ms at risk");
        assert_eq!(colour, theme::WARN, "an asynchronous server is not flagged");
    }

    /// The PostgreSQL shape: a `pg_settings` row of the same bare name, and a
    /// delay that already carries its unit.
    #[test]
    fn an_asynchronous_postgres_server_is_read_the_same_way() {
        let config = vec![
            entry("Write-Ahead Log / Settings", "synchronous_commit", "off"),
            entry("Write-Ahead Log / Settings", "wal_writer_delay", "200 ms"),
        ];
        let (label, colour) = durability_from(&config);
        assert_eq!(label, "asynchronous · up to 200 ms at risk");
        assert_eq!(colour, theme::WARN);
    }

    /// The default must not shout. A warning colour that is always on is one
    /// nobody reads.
    #[test]
    fn a_synchronous_server_is_stated_without_alarm() {
        let config = vec![entry("wal", "synchronous_commit", "on")];
        let (label, colour) = durability_from(&config);
        assert_eq!(label, "synchronous");
        assert_eq!(colour, theme::TEXT);
    }

    /// PostgreSQL's standby levels all still wait for the local log, so none of
    /// them is the risky mode and none should be flattened into "synchronous"
    /// either — they mean something more than it.
    #[test]
    fn the_standby_levels_are_shown_as_themselves() {
        for level in ["local", "remote_write", "remote_apply"] {
            let config = vec![entry("wal", "synchronous_commit", level)];
            let (label, colour) = durability_from(&config);
            assert_eq!(label, level);
            assert_eq!(colour, theme::TEXT, "{} was flagged as risky", level);
        }
    }

    /// Before the configuration has loaded there is nothing to say, and a guess
    /// would be a claim about durability.
    #[test]
    fn an_unloaded_configuration_claims_nothing() {
        assert_eq!(durability_from(&[]).0, "");
    }

    /// A server that reports `off` but no delay still has to say the important
    /// half.
    #[test]
    fn an_unknown_delay_still_reports_the_mode() {
        let config = vec![entry("wal", "synchronous_commit", "off")];
        assert_eq!(durability_from(&config).0, "asynchronous");
    }

    #[test]
    fn a_whole_second_reads_as_seconds() {
        assert_eq!(delay_label("1000"), "1s");
        assert_eq!(delay_label("200"), "200ms");
        assert_eq!(delay_label("1500"), "1500ms");
        assert_eq!(delay_label("200 ms"), "200 ms");
    }
}
