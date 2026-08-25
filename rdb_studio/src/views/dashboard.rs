//! The live dashboard: what the database is doing, as it does it.
//!
//! # Why this is not just the overview with numbers that change
//!
//! A number tells you the level. A line tells you the *shape*, and the shape is
//! what a person is actually looking for: reclaimable space climbing steadily
//! means the vacuum is not keeping up, and a commit rate that flatlines for
//! four seconds every minute is a checkpoint stalling writes. Neither is
//! visible in a figure that is only ever shown at its current value.
//!
//! # Where the samples come from
//!
//! One statement per tick — the same `SHOW DATABASE STATS` / `pg_stat_*` query
//! the overview already runs — folded into a ring buffer per series. There is
//! no separate metrics endpoint, no second connection, and no server-side
//! state, so pointing the dashboard at a database costs exactly one query every
//! couple of seconds.
//!
//! Counters are plotted as their slope rather than their value; see
//! [`crate::model::SeriesKind::Rate`] for why.

use iced::mouse;
use iced::widget::canvas::{self, Frame, Geometry, Path, Stroke, Text};
use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Center, Color, Element, Fill, Length, Point, Rectangle, Renderer, Size, Theme};

use crate::app::{Message, Studio};
use crate::model::{Metrics, SERIES, SeriesKind, SeriesSpec, bytes, count};
use crate::theme;

/// Charts per row. Fixed rather than responsive: the cards carry a line each,
/// and a line squeezed into a fifth of the window says nothing.
const PER_ROW: usize = 3;

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let live = studio.live;
    let controls = row![
        super::small_button(
            if live { "◼ PAUSE" } else { "▶ RESUME" },
            Some(Message::ToggleLive),
        ),
        interval_picker(studio),
        super::small_button("CLEAR", Some(Message::ClearMetrics)),
    ]
    .spacing(6)
    .align_y(Center);

    // Only the series this server actually reports. A RasterizedDB database has
    // no `xact_commit` and PostgreSQL has no filter-index size, and drawing an
    // empty card for the other one's counters would make half the page dead
    // space on both.
    let present: Vec<&SeriesSpec> = SERIES
        .iter()
        .filter(|spec| studio.metrics.points(spec.metric).is_some())
        .collect();

    let body: Element<'_, Message> = if present.is_empty() {
        waiting(studio)
    } else {
        let mut grid = column![].spacing(10);
        for chunk in present.chunks(PER_ROW) {
            let mut line = row![].spacing(10);
            for spec in chunk {
                line = line.push(card(spec, &studio.metrics));
            }
            // The last row keeps its cards the same width as the rows above
            // rather than stretching two cards across three columns.
            for _ in chunk.len()..PER_ROW {
                line = line.push(Space::new().width(Fill));
            }
            grid = grid.push(line);
        }
        grid = grid.push(largest_tables(studio));
        scrollable(grid).height(Fill).style(theme::scroller).into()
    };

    super::framed(
        format!(
            "LIVE · {} sample{}",
            studio.metrics.samples,
            if studio.metrics.samples == 1 { "" } else { "s" }
        ),
        Some(controls.into()),
        column![note(studio), body].spacing(8).into(),
    )
}

fn interval_picker(studio: &Studio) -> Element<'_, Message> {
    let mut picker = row![
        text("every").size(10).font(theme::MONO).color(theme::TEXT_FAINT),
    ]
    .spacing(4)
    .align_y(Center);

    for seconds in [1u64, 2, 5, 10, 30] {
        let active = studio.interval == seconds;
        picker = picker.push(
            iced::widget::button(
                text(format!("{}s", seconds)).size(10).font(theme::MONO),
            )
            .padding([4, 7])
            .style(theme::tree_item(active))
            .on_press(Message::SetInterval(seconds)),
        );
    }
    picker.into()
}

fn note(studio: &Studio) -> Element<'_, Message> {
    if let Some(error) = &studio.metrics_error {
        // A failed sample is reported here rather than in the status bar: the
        // dashboard polls, so one transient failure would otherwise leave a red
        // status line behind long after sampling recovered.
        return container(
            text(format!("last sample failed: {}", error))
                .size(11)
                .font(theme::MONO),
        )
        .padding(6)
        .width(Fill)
        .style(theme::error_box)
        .into();
    }

    container(
        text(if studio.live {
            "Sampling. One statement per tick — the same statistics query the overview runs."
        } else {
            "Paused. Nothing is being sent to the server."
        })
        .size(11)
        .color(theme::TEXT_FAINT),
    )
    .padding([2, 2])
    .into()
}

fn waiting(studio: &Studio) -> Element<'_, Message> {
    let message = if !studio.live {
        "Paused, and nothing sampled yet. Press RESUME."
    } else if studio.metrics.samples == 0 {
        "Waiting for the first sample…"
    } else {
        // Every plotted series is a rate, and a rate needs two readings.
        "Waiting for a second sample — a rate is the difference between two."
    };
    container(text(message).size(13).color(theme::TEXT_DIM))
        .center_x(Fill)
        .center_y(Fill)
        .into()
}

fn card<'a>(spec: &'a SeriesSpec, metrics: &'a Metrics) -> Element<'a, Message> {
    let points = metrics.points(spec.metric);
    let current = points.and_then(|p| p.back().copied()).unwrap_or(0.0);
    let (low, high) = extent(points.map(|p| p.iter().copied()));

    let colour = match spec.kind {
        SeriesKind::Rate => theme::RUST_BRIGHT,
        SeriesKind::Percent => theme::OK,
        _ => theme::RUST,
    };

    let chart = iced::widget::Canvas::new(Spark {
        points: points.map(|p| p.iter().copied().collect()).unwrap_or_default(),
        colour,
    })
    .width(Fill)
    .height(Length::Fixed(74.0));

    container(
        column![
            row![
                text(spec.label).size(10).font(theme::MONO).color(theme::TEXT_DIM),
                Space::new().width(Fill),
                text(spec.kind.format(current)).size(15).font(theme::MONO).color(colour),
            ]
            .align_y(Center),
            chart,
            row![
                text(format!("min {}", spec.kind.format(low)))
                    .size(9)
                    .font(theme::MONO)
                    .color(theme::TEXT_FAINT),
                Space::new().width(Fill),
                text(format!("max {}", spec.kind.format(high)))
                    .size(9)
                    .font(theme::MONO)
                    .color(theme::TEXT_FAINT),
            ],
            text(spec.help).size(9).color(theme::TEXT_FAINT),
        ]
        .spacing(5),
    )
    .padding(10)
    .width(Fill)
    .style(theme::tile)
    .into()
}

/// The lowest and highest value in a series, or `(0, 0)` when it is empty.
fn extent(points: Option<impl Iterator<Item = f64>>) -> (f64, f64) {
    let Some(points) = points else { return (0.0, 0.0) };
    points.fold((f64::MAX, f64::MIN), |(low, high), value| {
        (low.min(value), high.max(value))
    })
    .into_finite()
}

/// Turns the sentinel pair a fold over an empty iterator leaves behind into
/// something a label can print.
trait Finite {
    fn into_finite(self) -> (f64, f64);
}

impl Finite for (f64, f64) {
    fn into_finite(self) -> (f64, f64) {
        if self.0 > self.1 { (0.0, 0.0) } else { self }
    }
}

/// The tables taking the most space, as a bar each.
///
/// Not a time series — it is the same figures the tables list shows — but it is
/// the question a dashboard is opened to answer second, after "what is it
/// doing": *where is the space going*.
fn largest_tables(studio: &Studio) -> Element<'_, Message> {
    let mut largest: Vec<_> = studio.tables.iter().collect();
    largest.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    largest.truncate(8);

    let biggest = largest.first().map(|t| t.total_bytes).unwrap_or(0).max(1);

    let mut bars = column![].spacing(3);
    for table in &largest {
        // Proportion of the largest table rather than of the total: with one
        // dominant table every other bar would be invisible, and the comparison
        // that matters here is between tables.
        let share = table.total_bytes as f32 / biggest as f32;
        let dead = if table.total_bytes > 0 {
            table.reclaimable_bytes as f32 / table.total_bytes as f32
        } else {
            0.0
        };

        bars = bars.push(
            row![
                text(&table.name)
                    .size(11)
                    .font(theme::MONO)
                    .color(theme::TEXT_DIM)
                    .width(Length::Fixed(150.0)),
                container(
                    iced::widget::Canvas::new(Bar { share, dead })
                        .width(Fill)
                        .height(Length::Fixed(12.0)),
                )
                .width(Fill),
                text(bytes(table.total_bytes))
                    .size(11)
                    .font(theme::MONO)
                    .width(Length::Fixed(80.0))
                    .align_x(iced::alignment::Horizontal::Right),
                text(count(table.row_slots))
                    .size(10)
                    .font(theme::MONO)
                    .color(theme::TEXT_FAINT)
                    .width(Length::Fixed(80.0))
                    .align_x(iced::alignment::Horizontal::Right),
            ]
            .spacing(8)
            .align_y(Center),
        );
    }

    if largest.is_empty() {
        return Space::new().height(0).into();
    }

    container(
        column![
            row![
                text("LARGEST TABLES").size(10).font(theme::MONO).color(theme::TEXT_DIM),
                Space::new().width(Fill),
                text("the darker part of each bar is reclaimable")
                    .size(9)
                    .color(theme::TEXT_FAINT),
            ],
            bars,
        ]
        .spacing(6),
    )
    .padding(10)
    .width(Fill)
    .style(theme::tile)
    .into()
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

/// One series, as a line with the area under it filled.
struct Spark {
    points: Vec<f64>,
    colour: Color,
}

impl canvas::Program<Message> for Spark {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        renderer: &Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<Geometry> {
        let mut frame = Frame::new(renderer, bounds.size());
        let (width, height) = (bounds.width, bounds.height);
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), theme::BG);

        if self.points.len() < 2 || width <= 1.0 || height <= 1.0 {
            frame.fill_text(Text {
                content: "…".to_string(),
                position: Point::new(6.0, height / 2.0 - 6.0),
                color: theme::TEXT_FAINT,
                size: 11.0.into(),
                font: theme::MONO,
                ..Text::default()
            });
            return vec![frame.into_geometry()];
        }

        let low = self.points.iter().copied().fold(f64::MAX, f64::min);
        let high = self.points.iter().copied().fold(f64::MIN, f64::max);
        // A flat series has no range to scale to. Drawing it down the middle is
        // the honest picture; scaling it to the full height would turn rounding
        // noise into a mountain range.
        let span = (high - low).max(f64::EPSILON);
        let flat = high - low < f64::EPSILON;

        let pad = 4.0;
        let plot = height - pad * 2.0;
        let step = width / (self.points.len() - 1) as f32;

        let at = |index: usize, value: f64| -> Point {
            let y = if flat {
                height / 2.0
            } else {
                pad + plot - ((value - low) / span) as f32 * plot
            };
            Point::new(index as f32 * step, y)
        };

        // The filled area first, so the line sits on top of its own shading.
        let area = Path::new(|builder| {
            builder.move_to(Point::new(0.0, height));
            for (index, value) in self.points.iter().enumerate() {
                builder.line_to(at(index, *value));
            }
            builder.line_to(Point::new((self.points.len() - 1) as f32 * step, height));
            builder.close();
        });
        frame.fill(&area, Color { a: 0.16, ..self.colour });

        let line = Path::new(|builder| {
            builder.move_to(at(0, self.points[0]));
            for (index, value) in self.points.iter().enumerate().skip(1) {
                builder.line_to(at(index, *value));
            }
        });
        frame.stroke(&line, Stroke::default().with_color(self.colour).with_width(1.4));

        // The head of the series, so the eye finds "now" without counting.
        let head = at(self.points.len() - 1, *self.points.last().unwrap());
        frame.fill(&Path::circle(head, 2.4), self.colour);

        vec![frame.into_geometry()]
    }
}

/// One table's size, with the reclaimable part shaded darker.
struct Bar {
    share: f32,
    dead: f32,
}

impl canvas::Program<Message> for Bar {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        renderer: &Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<Geometry> {
        let mut frame = Frame::new(renderer, bounds.size());
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), theme::RAISED);

        let live = bounds.width * self.share.clamp(0.0, 1.0);
        frame.fill_rectangle(Point::ORIGIN, Size::new(live, bounds.height), theme::RUST_DIM);

        let dead = live * self.dead.clamp(0.0, 1.0);
        if dead > 0.5 {
            frame.fill_rectangle(
                Point::new(live - dead, 0.0),
                Size::new(dead, bounds.height),
                theme::WARN,
            );
        }
        vec![frame.into_geometry()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Stat;

    fn stat(metric: &str, value: &str) -> Stat {
        Stat {
            metric: metric.to_string(),
            value: value.to_string(),
            detail: String::new(),
        }
    }

    /// A counter's first reading is a baseline, not a data point. Plotting it
    /// would put one enormous value at the head of the series.
    #[test]
    fn a_rate_needs_two_readings() {
        let mut metrics = Metrics::default();
        metrics.sample(&[stat("query_commits", "1000")], 2.0);
        assert!(metrics.points("query_commits").is_none());

        metrics.sample(&[stat("query_commits", "1020")], 2.0);
        assert_eq!(metrics.points("query_commits").unwrap().back(), Some(&10.0));
    }

    /// A restarted server resets its counters. A negative rate is not a thing.
    #[test]
    fn a_counter_going_backwards_reads_as_zero() {
        let mut metrics = Metrics::default();
        metrics.sample(&[stat("query_commits", "1000")], 2.0);
        metrics.sample(&[stat("query_commits", "5")], 2.0);
        assert_eq!(metrics.points("query_commits").unwrap().back(), Some(&0.0));
    }

    /// A level is plotted as it arrives, from the very first sample.
    #[test]
    fn a_gauge_is_plotted_immediately() {
        let mut metrics = Metrics::default();
        metrics.sample(&[stat("total_bytes", "4096")], 2.0);
        assert_eq!(metrics.points("total_bytes").unwrap().back(), Some(&4096.0));
    }

    /// A console left open for a day must not grow without bound.
    #[test]
    fn the_history_is_bounded() {
        let mut metrics = Metrics::default();
        for value in 0..(crate::model::HISTORY + 50) {
            metrics.sample(&[stat("total_bytes", &value.to_string())], 1.0);
        }
        let points = metrics.points("total_bytes").unwrap();
        assert_eq!(points.len(), crate::model::HISTORY);
        assert_eq!(
            points.back(),
            Some(&((crate::model::HISTORY + 49) as f64)),
            "the newest sample must survive, not the oldest"
        );
    }

    /// A metric the server does not report leaves no series behind, which is
    /// what keeps the other dialect's counters off the page.
    #[test]
    fn an_absent_metric_draws_no_card() {
        let mut metrics = Metrics::default();
        metrics.sample(&[stat("total_bytes", "10")], 2.0);
        assert!(metrics.points("cache_hit_ratio").is_none());
    }

    #[test]
    fn an_empty_series_has_a_printable_extent() {
        assert_eq!(extent(None::<std::vec::IntoIter<f64>>), (0.0, 0.0));
        assert_eq!(extent(Some(vec![3.0, 1.0, 7.0].into_iter())), (1.0, 7.0));
    }
}
