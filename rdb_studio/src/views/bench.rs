//! The benchmark page: one statement, run as fast as a number of clients can
//! run it, and what that looked like.
//!
//! The page keeps its own state and handles its own messages, rather than
//! spreading a dozen fields through [`Studio`]: nothing else in the console
//! reads them, and the run itself lives in [`crate::bench`].
//!
//! # What it is for, and what it is not
//!
//! It answers "how fast is this statement against this server, from here" —
//! the question someone has right after writing a query, and the one a
//! changed index or a changed setting should move. It is not a load test: the
//! clients are this console, on this machine, so a server faster than the
//! machine running the console measures the console. Above a few thousand
//! statements a second it is worth running the same statement from a second
//! machine and seeing whether the numbers add.

use std::time::Duration;

use iced::mouse;
use iced::widget::canvas::{self, Frame, Geometry, Path, Stroke, Text};
use iced::widget::{
    Space, button, column, container, pick_list, row, rule, scrollable, text, text_editor,
};
use iced::{Center, Color, Element, Fill, Length, Point, Rectangle, Renderer, Size, Task, Theme};
use tokio::sync::watch;

use crate::app::{Message, Status, Studio};
use crate::bench::{self, Event, Phase, Plan, Progress, Protocol, Report, duration};
use crate::theme;

/// Runs kept for comparison. A comparison wants the last handful; a session's
/// worth is a list nobody reads.
const KEPT_RUNS: usize = 20;

/// Throughput points kept for the live line: an hour at four a second.
const LIVE_POINTS: usize = 4 * 3600;

pub struct Page {
    pub editor: text_editor::Content,
    pub connections: String,
    pub duration: String,
    pub warmup: String,
    pub protocol: Protocol,
    /// Set while a run is going; sending `true` stops it.
    stop: Option<watch::Sender<bool>>,
    /// How long the running plan measures and warms up for, for the progress
    /// bar.
    planned: (Duration, Duration),
    pub progress: Option<Progress>,
    /// Throughput samples while the run is measuring.
    pub live: Vec<f64>,
    pub error: Option<String>,
    /// Finished runs, newest first.
    pub runs: Vec<Report>,
    /// Which run is on screen.
    pub shown: usize,
}

impl Default for Page {
    fn default() -> Self {
        Self {
            editor: text_editor::Content::with_text("SELECT 1"),
            connections: "4".to_string(),
            duration: "10".to_string(),
            warmup: "2".to_string(),
            protocol: Protocol::Simple,
            stop: None,
            planned: (Duration::ZERO, Duration::ZERO),
            progress: None,
            live: Vec::new(),
            error: None,
            runs: Vec::new(),
            shown: 0,
        }
    }
}

impl Page {
    pub fn running(&self) -> bool {
        self.stop.is_some()
    }

    /// Stops a run in flight, if there is one. Its report still arrives.
    pub fn stop(&mut self) {
        if let Some(stop) = &self.stop {
            let _ = stop.send(true);
        }
    }

    fn report(&self) -> Option<&Report> {
        self.runs.get(self.shown)
    }
}

#[derive(Debug, Clone)]
pub enum BenchMessage {
    Editor(text_editor::Action),
    Connections(String),
    Duration(String),
    Warmup(String),
    Protocol(Protocol),
    /// Take the statement from the QUERY tab — its selection, or all of it.
    FromQueryEditor,
    Start,
    Stop,
    Event(Event),
    Show(usize),
    Copy,
    ClearRuns,
}

/// A count from a box, within bounds, or why not.
fn parse_number(value: &str, what: &str, low: u64, high: u64) -> Result<u64, String> {
    let n: u64 = value
        .trim()
        .parse()
        .map_err(|_| format!("{} must be a whole number", what))?;
    if n < low || n > high {
        return Err(format!("{} must be between {} and {}", what, low, high));
    }
    Ok(n)
}

impl Studio {
    pub fn update_bench(&mut self, message: BenchMessage) -> Task<Message> {
        let page = &mut self.bench;
        match message {
            BenchMessage::Editor(action) => page.editor.perform(action),
            BenchMessage::Connections(v) => page.connections = v,
            BenchMessage::Duration(v) => page.duration = v,
            BenchMessage::Warmup(v) => page.warmup = v,
            BenchMessage::Protocol(p) => page.protocol = p,
            BenchMessage::FromQueryEditor => {
                let sql = self
                    .editor
                    .selection()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| self.editor.text());
                page.editor = text_editor::Content::with_text(sql.trim());
            }
            BenchMessage::Start => {
                if page.running() {
                    return Task::none();
                }
                let plan = (|| {
                    let connections = parse_number(
                        &page.connections,
                        "connections",
                        1,
                        bench::MAX_CONNECTIONS as u64,
                    )?;
                    let duration =
                        parse_number(&page.duration, "duration", 1, bench::MAX_DURATION.as_secs())?;
                    let warmup =
                        parse_number(&page.warmup, "warm-up", 0, bench::MAX_WARMUP.as_secs())?;
                    let sql = page.editor.text().trim().to_string();
                    if sql.is_empty() {
                        return Err("there is no statement to run".to_string());
                    }
                    Ok(Plan {
                        sql,
                        connections: connections as usize,
                        duration: Duration::from_secs(duration),
                        warmup: Duration::from_secs(warmup),
                        protocol: page.protocol,
                        profile: self.form.clone(),
                    })
                })();
                let plan = match plan {
                    Ok(plan) => plan,
                    Err(e) => {
                        page.error = Some(e);
                        return Task::none();
                    }
                };
                let (stop, stopped) = watch::channel(false);
                page.stop = Some(stop);
                page.planned = (plan.duration, plan.warmup);
                page.progress = None;
                page.live.clear();
                page.error = None;
                self.status = Status::Busy(format!(
                    "benchmarking on {} connection{}",
                    plan.connections,
                    if plan.connections == 1 { "" } else { "s" }
                ));
                return Task::run(bench::run(plan, stopped), |event| {
                    Message::Bench(BenchMessage::Event(event))
                });
            }
            BenchMessage::Stop => page.stop(),
            BenchMessage::Event(Event::Progress(progress)) => {
                if progress.phase == Phase::Measuring && page.live.len() < LIVE_POINTS {
                    page.live.push(progress.current_qps);
                }
                page.progress = Some(progress);
            }
            BenchMessage::Event(Event::Finished(result)) => {
                page.stop = None;
                page.progress = None;
                match result {
                    Ok(report) => {
                        self.status = Status::Done(format!(
                            "benchmark: {:.0} queries/s, p99 {}",
                            report.qps(),
                            duration(report.latency.p99)
                        ));
                        page.runs.insert(0, *report);
                        page.runs.truncate(KEPT_RUNS);
                        page.shown = 0;
                    }
                    Err(e) => {
                        self.status = Status::Failed("benchmark failed".to_string());
                        page.error = Some(e);
                    }
                }
            }
            BenchMessage::Show(index) => page.shown = index.min(page.runs.len().saturating_sub(1)),
            BenchMessage::Copy => {
                if let Some(report) = page.report() {
                    return Task::batch([
                        iced::clipboard::write(report.to_text()),
                        Task::done(Message::Copied("benchmark report copied".into())),
                    ]);
                }
            }
            BenchMessage::ClearRuns => {
                page.runs.clear();
                page.shown = 0;
            }
        }
        Task::none()
    }
}

fn send(message: BenchMessage) -> Message {
    Message::Bench(message)
}

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let page = &studio.bench;

    let action: Element<'_, Message> = if page.running() {
        button(text("◼ STOP").size(12).font(theme::MONO))
            .padding([8, 16])
            .style(theme::destructive)
            .on_press(send(BenchMessage::Stop))
            .into()
    } else {
        super::primary_button("▶ RUN BENCHMARK", Some(send(BenchMessage::Start)))
    };

    let editor = text_editor(&page.editor)
        .placeholder("-- the statement to benchmark")
        .on_action(|a| send(BenchMessage::Editor(a)))
        .font(theme::MONO)
        .size(13)
        .padding(10)
        .height(Length::Fixed(120.0))
        .style(theme::editor);

    let settings = row![
        number_field("connections", &page.connections, |v| send(BenchMessage::Connections(v))),
        number_field("duration (s)", &page.duration, |v| send(BenchMessage::Duration(v))),
        number_field("warm-up (s)", &page.warmup, |v| send(BenchMessage::Warmup(v))),
        column![
            text("protocol").size(11).font(theme::MONO).color(theme::TEXT_DIM),
            pick_list(Protocol::ALL, Some(page.protocol), |p| send(BenchMessage::Protocol(p)))
                .padding([6, 8])
                .text_size(13)
                .width(Length::Fixed(130.0)),
        ]
        .spacing(4),
        Space::new().width(Fill),
        column![
            Space::new().height(15),
            row![
                super::small_button(
                    "FROM QUERY TAB",
                    (!page.running()).then_some(send(BenchMessage::FromQueryEditor)),
                ),
                action,
            ]
            .spacing(8)
            .align_y(Center),
        ],
    ]
    .spacing(10)
    .align_y(iced::alignment::Vertical::Bottom);

    let mut body = column![container(editor).width(Fill), settings].spacing(8);

    if bench::modifies_data(&page.editor.text()) {
        body = body.push(
            container(
                text(
                    "This statement looks like it changes data. A benchmark runs it thousands \
                     of times, on every connection, and once more before it starts.",
                )
                .size(11),
            )
            .padding(6)
            .width(Fill)
            .style(theme::error_box),
        );
    }
    body = body.push(
        text(
            "Each connection is a separate client opened from this profile. Times are end to end: \
             from sending the statement to the last row arriving, network included.",
        )
        .size(10)
        .color(theme::TEXT_FAINT),
    );

    if let Some(error) = &page.error {
        body = body.push(
            container(text(error.clone()).size(12).font(theme::MONO))
                .padding(8)
                .width(Fill)
                .style(theme::error_box),
        );
    }

    body = body.push(rule::horizontal(1).style(theme::hairline));

    if page.running() {
        body = body.push(progress(page));
    } else if let Some(report) = page.report() {
        body = body.push(results(report));
    } else {
        body = body.push(super::faint(
            "Write a statement, choose how many clients run it and for how long, and press RUN.",
        ));
    }

    row![
        container(super::framed(
            "BENCHMARK",
            None,
            scrollable(body.padding(iced::Padding { right: 12.0, ..Default::default() }))
                .height(Fill)
                .style(theme::scroller)
                .into(),
        ))
        .width(Fill),
        runs(page),
    ]
    .spacing(10)
    .into()
}

fn number_field<'a>(
    label: &'a str,
    value: &'a str,
    on_input: impl Fn(String) -> Message + 'a,
) -> Element<'a, Message> {
    column![
        text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        iced::widget::text_input("", value)
            .on_input(on_input)
            .padding([6, 8])
            .size(13)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(110.0)),
    ]
    .spacing(4)
    .into()
}

fn progress(page: &Page) -> Element<'_, Message> {
    let Some(p) = &page.progress else {
        return super::faint("starting…");
    };
    let (measure, warmup) = page.planned;
    let done = match p.phase {
        Phase::Connecting | Phase::WarmingUp => 0.0,
        Phase::Measuring => p.elapsed.as_secs_f32() / measure.as_secs_f32().max(0.001),
        Phase::Draining => 1.0,
    };
    let label = match p.phase {
        Phase::WarmingUp => format!("{} · {} of warm-up, not counted", p.phase.label(), duration(warmup)),
        Phase::Measuring => format!(
            "{} · {:.0} of {} s",
            p.phase.label(),
            p.elapsed.as_secs_f32().floor(),
            measure.as_secs()
        ),
        _ => p.phase.label().to_string(),
    };

    let bar = iced::widget::Canvas::new(Meter { share: done }).width(Fill).height(Length::Fixed(6.0));

    column![
        text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        bar,
        row![
            super::tile("QUERIES/S NOW", format!("{:.0}", p.current_qps), None),
            super::tile("COMPLETED", p.completed.to_string(), None),
            super::tile(
                "MEAN SO FAR",
                p.mean.map(duration).unwrap_or_else(|| "—".to_string()),
                None
            ),
            super::tile("ERRORS", p.errors.to_string(), None),
        ]
        .spacing(10),
        chart_card(
            "THROUGHPUT, LIVE",
            iced::widget::Canvas::new(Line { points: page.live.clone(), colour: theme::RUST_BRIGHT })
                .width(Fill)
                .height(Length::Fixed(90.0))
                .into(),
            None,
        ),
    ]
    .spacing(10)
    .into()
}

fn results(report: &Report) -> Element<'_, Message> {
    let l = &report.latency;
    let d = |v: Duration| duration(v);

    let headline = format!(
        "{} connection{} · {} · {} measured{}{}",
        report.connections,
        if report.connections == 1 { "" } else { "s" },
        report.protocol,
        duration(report.elapsed),
        if report.warmup > Duration::ZERO {
            format!(" after {} warm-up", duration(report.warmup))
        } else {
            String::new()
        },
        if report.stopped { " · STOPPED EARLY" } else { "" },
    );

    let mut out = column![
        row![
            text(headline).size(11).font(theme::MONO).color(theme::TEXT_DIM),
            Space::new().width(Fill),
            super::small_button("COPY REPORT", Some(send(BenchMessage::Copy))),
        ]
        .align_y(Center),
        row![
            super::tile(
                "QUERIES / SECOND",
                format!("{:.1}", report.qps()),
                Some(format!("{} completed", report.completed)),
            ),
            super::tile("MEAN", d(l.mean), Some(format!("± {} stddev", d(l.stddev)))),
            super::tile("MEDIAN", d(l.p50), None),
            super::tile("P90", d(l.p90), None),
            super::tile("P99", d(l.p99), None),
        ]
        .spacing(10),
        row![
            super::tile("MIN", d(l.min), None),
            super::tile("P95", d(l.p95), None),
            super::tile("P99.9", d(l.p999), None),
            super::tile("MAX", d(l.max), None),
            super::tile(
                "ERRORS",
                report.errors.to_string(),
                Some(format!("{:.2}% of attempts", report.error_rate() * 100.0)),
            ),
        ]
        .spacing(10),
        row![
            super::tile(
                "ROWS / QUERY",
                format!("{:.1}", report.rows_per_query()),
                Some(format!("{:.0} rows/s", report.rows_per_second())),
            ),
            super::tile(
                "CLIENT IMBALANCE",
                if report.per_connection.1 > 0 {
                    let (fewest, most) = report.per_connection;
                    format!("{:.1}%", (most - fewest) as f64 / most as f64 * 100.0)
                } else {
                    "—".to_string()
                },
                Some(format!(
                    "one client finished {} queries, another {}",
                    report.per_connection.0, report.per_connection.1
                )),
            ),
            super::tile(
                "CONNECT TIME",
                d(report.connect_mean),
                Some(format!("mean; slowest {}", d(report.connect_max))),
            ),
            match report.throughput_spread() {
                Some((low, high, cv)) => super::tile(
                    "THROUGHPUT SPREAD",
                    format!("{:.1}%", cv * 100.0),
                    Some(format!("per-second variation; {} to {} q/s", low, high)),
                ),
                None => super::tile("THROUGHPUT SPREAD", "—".to_string(), Some("under a second measured".to_string())),
            },
            super::tile(
                "TAIL RATIO",
                if l.p50 > Duration::ZERO {
                    format!("{:.1}×", l.p99.as_secs_f64() / l.p50.as_secs_f64())
                } else {
                    "—".to_string()
                },
                Some("p99 over median".to_string()),
            ),
        ]
        .spacing(10),
    ]
    .spacing(10);

    if let Some(error) = &report.first_error {
        out = out.push(
            container(text(format!("first error: {}", error)).size(11).font(theme::MONO))
                .padding(6)
                .width(Fill)
                .style(theme::error_box),
        );
    }

    let per_second: Vec<f64> = report.per_second.iter().map(|&v| v as f64).collect();
    out = out.push(
        row![
            chart_card(
                "QUERIES PER SECOND",
                iced::widget::Canvas::new(Columns {
                    values: per_second,
                    colour: theme::RUST_BRIGHT,
                    markers: Vec::new(),
                })
                .width(Fill)
                .height(Length::Fixed(120.0))
                .into(),
                Some("one bar per second of the measured window".to_string()),
            ),
            distribution(report),
        ]
        .spacing(10),
    );

    out.into()
}

/// The latency histogram, log-scaled, with the median and p99 marked.
fn distribution(report: &Report) -> Element<'_, Message> {
    let bins = &report.distribution;
    let values: Vec<f64> = bins.iter().map(|b| b.2 as f64).collect();
    let (low, high) = match (bins.first(), bins.last()) {
        (Some(first), Some(last)) => (first.0.max(1) as f64, last.1.max(1) as f64),
        _ => (1.0, 1.0),
    };
    let at = |v: Duration| -> f32 {
        let v = (v.as_nanos() as f64).max(low);
        if high <= low { 0.0 } else { ((v / low).ln() / (high / low).ln()) as f32 }
    };
    let markers = vec![
        (at(report.latency.p50), theme::OK),
        (at(report.latency.p99), theme::WARN),
    ];
    chart_card(
        "LATENCY DISTRIBUTION",
        iced::widget::Canvas::new(Columns { values, colour: theme::RUST, markers })
            .width(Fill)
            .height(Length::Fixed(120.0))
            .into(),
        Some(format!(
            "{} to {}, log scale · green median, yellow p99",
            duration(Duration::from_nanos(low as u64)),
            duration(Duration::from_nanos(high as u64)),
        )),
    )
}

fn chart_card<'a>(
    label: &'static str,
    chart: Element<'a, Message>,
    note: Option<String>,
) -> Element<'a, Message> {
    let mut inner = column![
        text(label).size(10).font(theme::MONO).color(theme::TEXT_DIM),
        chart,
    ]
    .spacing(5);
    if let Some(note) = note {
        inner = inner.push(text(note).size(9).color(theme::TEXT_FAINT));
    }
    container(inner).padding(10).width(Fill).style(theme::tile).into()
}

/// Finished runs, for comparing one setting against another.
fn runs(page: &Page) -> Element<'_, Message> {
    let mut list = column![].spacing(3);
    for (index, report) in page.runs.iter().enumerate() {
        // The highlighted row is drawn in the accent colour, which the accent
        // figure would vanish into.
        let selected = index == page.shown && !page.running();
        let figure = if selected { theme::TEXT } else { theme::RUST_BRIGHT };
        let faint = if selected { theme::TEXT } else { theme::TEXT_FAINT };
        let item = button(
            column![
                text(super::elide(report.sql.trim(), 120))
                    .size(11)
                    .font(theme::MONO)
                    .color(theme::TEXT),
                row![
                    text(format!("{:.0} q/s", report.qps()))
                        .size(10)
                        .font(theme::MONO)
                        .color(figure),
                    text(format!("p50 {}", duration(report.latency.p50)))
                        .size(9)
                        .font(theme::MONO)
                        .color(faint),
                    text(format!("p99 {}", duration(report.latency.p99)))
                        .size(9)
                        .font(theme::MONO)
                        .color(faint),
                ]
                .spacing(6)
                .align_y(Center),
                text(format!(
                    "{} conn · {} · {}{}",
                    report.connections,
                    report.protocol,
                    duration(report.elapsed),
                    if report.errors > 0 { format!(" · {} errors", report.errors) } else { String::new() },
                ))
                .size(9)
                .font(theme::MONO)
                .color(if report.errors > 0 && !selected { theme::BAD } else { faint }),
            ]
            .spacing(2),
        )
        .width(Fill)
        .padding([4, 6])
        .style(theme::tree_item(selected))
        .on_press(send(BenchMessage::Show(index)));
        list = list.push(column![item, rule::horizontal(1).style(theme::hairline)].spacing(3));
    }
    if page.runs.is_empty() {
        list = list.push(super::faint(
            "Finished runs collect here, newest first, so one setting can be compared with another.",
        ));
    }

    let heading = row![
        text(page.runs.len().to_string()).size(10).font(theme::MONO).color(theme::TEXT_DIM),
        Space::new().width(Fill),
        super::small_button("CLEAR", (!page.runs.is_empty()).then_some(send(BenchMessage::ClearRuns))),
    ]
    .spacing(6)
    .align_y(Center);

    container(super::framed(
        "RUNS",
        Some(heading.into()),
        scrollable(list).height(Fill).style(theme::scroller).into(),
    ))
    .width(Length::Fixed(300.0))
    .into()
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

/// A progress bar.
struct Meter {
    share: f32,
}

impl canvas::Program<Message> for Meter {
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
        let filled = bounds.width * self.share.clamp(0.0, 1.0);
        frame.fill_rectangle(Point::ORIGIN, Size::new(filled, bounds.height), theme::RUST);
        vec![frame.into_geometry()]
    }
}

/// Bars from zero, with optional vertical markers at fractions of the width.
struct Columns {
    values: Vec<f64>,
    colour: Color,
    markers: Vec<(f32, Color)>,
}

impl canvas::Program<Message> for Columns {
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

        let high = self.values.iter().copied().fold(0.0, f64::max);
        if self.values.is_empty() || high <= 0.0 {
            frame.fill_text(Text {
                content: "no data".to_string(),
                position: Point::new(6.0, height / 2.0 - 6.0),
                color: theme::TEXT_FAINT,
                size: 11.0.into(),
                font: theme::MONO,
                ..Text::default()
            });
            return vec![frame.into_geometry()];
        }

        // Bars start at zero, never at the minimum: a throughput that varies
        // by 2 % must not look like one that halves.
        let slot = width / self.values.len() as f32;
        let gap = if slot > 4.0 { 1.0 } else { 0.0 };
        for (index, value) in self.values.iter().enumerate() {
            // The top 14 px are the scale label's.
            let h = ((value / high) as f32 * (height - 16.0)).max(if *value > 0.0 { 1.0 } else { 0.0 });
            frame.fill_rectangle(
                Point::new(index as f32 * slot, height - h),
                Size::new((slot - gap).max(0.5), h),
                self.colour,
            );
        }

        for (at, colour) in &self.markers {
            let x = (at.clamp(0.0, 1.0) * width).clamp(1.0, width - 1.0);
            let path = Path::line(Point::new(x, 0.0), Point::new(x, height));
            frame.stroke(&path, Stroke::default().with_color(*colour).with_width(1.5));
        }

        frame.fill_text(Text {
            content: format!("{:.0}", high),
            position: Point::new(4.0, 2.0),
            color: theme::TEXT_FAINT,
            size: 9.0.into(),
            font: theme::MONO,
            ..Text::default()
        });

        vec![frame.into_geometry()]
    }
}

/// A line from zero, for the live throughput.
struct Line {
    points: Vec<f64>,
    colour: Color,
}

impl canvas::Program<Message> for Line {
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

        let high = self.points.iter().copied().fold(0.0, f64::max);
        if self.points.len() < 2 || high <= 0.0 {
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

        let step = width / (self.points.len() - 1) as f32;
        let at = |index: usize, value: f64| {
            // The top 14 px are the scale label's.
            Point::new(index as f32 * step, height - 2.0 - (value / high) as f32 * (height - 16.0))
        };
        let line = Path::new(|builder| {
            builder.move_to(at(0, self.points[0]));
            for (index, value) in self.points.iter().enumerate().skip(1) {
                builder.line_to(at(index, *value));
            }
        });
        frame.stroke(&line, Stroke::default().with_color(self.colour).with_width(1.4));
        frame.fill_text(Text {
            content: format!("{:.0}", high),
            position: Point::new(4.0, 2.0),
            color: theme::TEXT_FAINT,
            size: 9.0.into(),
            font: theme::MONO,
            ..Text::default()
        });
        vec![frame.into_geometry()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_are_bounded() {
        assert_eq!(parse_number(" 8 ", "connections", 1, 256), Ok(8));
        assert!(parse_number("0", "connections", 1, 256).is_err());
        assert!(parse_number("257", "connections", 1, 256).is_err());
        assert!(parse_number("four", "connections", 1, 256).is_err());
        assert_eq!(parse_number("0", "warm-up", 0, 600), Ok(0));
    }
}
