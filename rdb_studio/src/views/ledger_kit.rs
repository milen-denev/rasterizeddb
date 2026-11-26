//! The widgets the LEDGER page is drawn with (the enterprise build): Studio's
//! own panels, buttons, fields, pills and sortable grids, lent to the
//! licensed page through [`rdb_studio_enterprise::Kit`]. Nothing here knows
//! about ledgers; it maps the page's requests onto this module's helpers so
//! the page looks like the rest of Studio.

use iced::widget::{button, column, container, row, text, text_input, Space};
use iced::{Element, Fill, Length};

use rdb_studio_enterprise::{Cell, Col, GridRow, Kit, SortVal, Tone};

use crate::app::{Message, Studio};
use crate::theme;

pub struct StudioKit<'a> {
    pub studio: &'a Studio,
}

fn colour(tone: Tone) -> iced::Color {
    match tone {
        Tone::Normal => theme::TEXT,
        Tone::Dim => theme::TEXT_DIM,
        Tone::Faint => theme::TEXT_FAINT,
        Tone::Good => theme::OK,
        Tone::Warn => theme::WARN,
        Tone::Bad => theme::BAD,
        Tone::Accent => theme::RUST_BRIGHT,
    }
}

/// A grid row with buttons in it is this tall; one of text only, the grid's
/// usual row.
const BUTTON_ROW: f32 = 27.0;
const TEXT_ROW: f32 = 22.0;

impl<'a> Kit<'a, Message> for StudioKit<'a> {
    fn frame(&self, title: String, trailing: Option<Element<'a, Message>>, body: Element<'a, Message>) -> Element<'a, Message> {
        super::framed(title, trailing, body)
    }

    fn section(&self, label: String, trailing: Option<Element<'a, Message>>, body: Element<'a, Message>) -> Element<'a, Message> {
        let mut strip = row![super::section_label(label), Space::new().width(Fill)].align_y(iced::alignment::Vertical::Center);
        if let Some(t) = trailing {
            strip = strip.push(container(t).padding([2, 6]));
        }
        container(column![strip, container(body).padding([4, 8]).width(Fill)].spacing(2))
            .padding(iced::Padding { bottom: 8.0, ..iced::Padding::ZERO })
            .width(Fill)
            .style(theme::panel)
            .into()
    }

    fn form(&self, label: String, body: Element<'a, Message>) -> Element<'a, Message> {
        container(column![super::section_label(label), body].spacing(6))
            .padding(8)
            .width(Fill)
            .style(theme::header)
            .into()
    }

    fn alert(&self, body: Element<'a, Message>) -> Element<'a, Message> {
        container(body)
            .padding(10)
            .width(Fill)
            .style(|_: &iced::Theme| container::Style {
                background: Some(theme::RAISED.into()),
                border: iced::Border { color: theme::WARN, width: 1.0, radius: iced::border::radius(0) },
                ..container::Style::default()
            })
            .into()
    }

    fn text(&self, value: String, tone: Tone) -> Element<'a, Message> {
        text(value).size(11).color(colour(tone)).into()
    }

    fn mono(&self, value: String, tone: Tone) -> Element<'a, Message> {
        text(value).size(11).font(theme::MONO).color(colour(tone)).into()
    }

    fn small_button(&self, label: &str, on: Option<Message>) -> Element<'a, Message> {
        super::small_button(label.to_string(), on)
    }

    fn row_button(&self, label: &str, on: Option<Message>, destructive: bool) -> Element<'a, Message> {
        let mut b = button(text(label.to_string()).size(10).font(theme::MONO))
            .padding([2, 7])
            .style(if destructive { theme::destructive } else { theme::secondary });
        if let Some(m) = on {
            b = b.on_press(m);
        }
        b.into()
    }

    fn primary_button(&self, label: &str, on: Option<Message>) -> Element<'a, Message> {
        super::primary_button(label.to_string(), on)
    }

    fn choice(&self, label: &str, selected: bool, on: Message) -> Element<'a, Message> {
        button(text(label.to_string()).size(11).font(theme::MONO))
            .padding([5, 10])
            .style(if selected { theme::primary } else { theme::secondary })
            .on_press(on)
            .into()
    }

    fn field(
        &self,
        label: String,
        value: &'a str,
        secret: bool,
        width: f32,
        on: Box<dyn Fn(String) -> Message + 'a>,
    ) -> Element<'a, Message> {
        let input = text_input("", value)
            .on_input(on)
            .secure(secret)
            .padding([5, 8])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(width));
        if label.is_empty() {
            return input.into();
        }
        column![text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM), input].spacing(4).into()
    }

    fn toggle(&self, label: String, value: bool, on: Box<dyn Fn(bool) -> Message + 'a>) -> Element<'a, Message> {
        super::toggle(label, value, on)
    }

    fn tile(&self, label: String, value: String, note: Option<String>) -> Element<'a, Message> {
        super::tile(label, value, note)
    }

    fn pill(&self, label: String, tone: Tone) -> Element<'a, Message> {
        super::pill(label, colour(tone))
    }

    fn grid(&self, scope: &str, cols: Vec<Col>, mut rows: Vec<GridRow<'a, Message>>, max_height: f32) -> Element<'a, Message> {
        let layout = super::GridLayout::new(self.studio, scope.to_string());
        let columns: Vec<super::GridColumn> = cols
            .iter()
            .map(|c| {
                let mut g = super::GridColumn::new(c.key, c.header, c.width);
                if c.right {
                    g = g.right();
                }
                if !c.sortable {
                    g = g.unsortable();
                }
                g
            })
            .collect();
        let widths = super::column_widths(&layout, &columns);
        let total = super::grid_total(&widths);
        let keys: Vec<&'static str> = cols.iter().map(|c| c.key).collect();
        super::sort_rows(&layout, &mut rows, |r, column| {
            match keys.iter().position(|k| *k == column).and_then(|i| r.keys.get(i)) {
                Some(SortVal::Num(n)) => super::SortKey::number(*n),
                Some(SortVal::Text(t)) => super::SortKey::text(t),
                None => super::SortKey::text(""),
            }
        });
        let mut height = 44.0;
        let mut body = column![].spacing(0);
        for (index, r) in rows.into_iter().enumerate() {
            let widgets = r.cells.iter().any(|c| matches!(c, Cell::Widget(_)));
            let row_height = if widgets { BUTTON_ROW } else { TEXT_ROW };
            height += row_height;
            let cells: Vec<Element<'a, Message>> = r
                .cells
                .into_iter()
                .zip(cols.iter())
                .map(|(c, col)| match c {
                    Cell::Text(value, tone) => super::grid_cell(value, col.right, colour(tone)),
                    Cell::Widget(e) => container(e).padding([2, 6]).height(Length::Fixed(row_height)).align_y(iced::alignment::Vertical::Center).into(),
                })
                .collect();
            let line = super::grid_row(&widths, cells).height(Length::Fixed(row_height)).align_y(iced::alignment::Vertical::Center);
            body = body.push(super::grid_stripe(index, container(line).width(Length::Fixed(total)).into()));
        }
        container(super::grid_frame(&layout, &widths, super::grid_header(&layout, &columns, &widths), body.into()))
            .height(Length::Fixed(height.min(max_height)))
            .into()
    }
}
