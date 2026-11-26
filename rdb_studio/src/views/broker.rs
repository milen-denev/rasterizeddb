//! Broker: RasterizedDB's message broker — topics, queues, subscriptions,
//! exchanges and their bindings — administered and used from the console.
//!
//! Like the other pages it is the SQL surface with buttons: every form shows
//! the `BROKER …` statement it sends, the drops go through the confirmation
//! dialog, and the message panel shows what `READ` (which takes nothing),
//! `POP` (which takes and acknowledges) and `SHOW PARTITIONS` answered.

use iced::widget::{Space, button, column, container, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, section_label, small_button};

/// Which text box of the page changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerField {
    // The create form.
    Name,
    Partitions,
    Durability,
    Retention,
    RetentionBytes,
    MaxDeliveries,
    DeadLetter,
    AckTimeout,
    ExchangeType,
    Target,
    Mode,
    From,
    Key,
    // The publish form.
    PublishTarget,
    PublishRoutingKey,
    PublishValue,
    PublishKey,
    PublishHeaderName,
    PublishHeaderValue,
    // The read controls.
    ReadPartition,
    ReadFrom,
    ReadLimit,
}

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let reload = Some(small_button("RELOAD", Some(Message::ReloadBroker)));
    let Some(snapshot) = &studio.broker else {
        return framed(
            "BROKER",
            reload,
            container(text("Loading…").size(13).color(theme::TEXT_FAINT)).center_x(Fill).center_y(Fill).into(),
        );
    };
    if !snapshot.supported {
        return framed("BROKER", reload, unsupported(&snapshot.refusal));
    }
    let body = column![
        create_form(studio),
        publish_form(studio),
        browse_panel(studio),
        topics(studio, snapshot),
        queues(studio, snapshot),
        subscriptions(studio, snapshot),
        exchanges(studio, snapshot),
    ]
    .spacing(10);
    framed("BROKER", reload, scrollable(body).height(Fill).style(theme::scroller).into())
}

fn unsupported(refusal: &str) -> Element<'_, Message> {
    container(
        column![
            text("This server does not answer BROKER statements").size(15).color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            text(
                "BROKER SHOW TOPICS was refused. The message broker is a RasterizedDB feature \
                 (broker); a PostgreSQL server, or a RasterizedDB build without it, has none."
            )
            .size(12)
            .color(theme::TEXT_DIM),
            text(refusal.to_string()).size(11).font(theme::MONO).color(theme::TEXT_FAINT),
        ]
        .spacing(10)
        .max_width(720),
    )
    .padding(14)
    .into()
}

fn choice<'a>(label: &'static str, selected: bool, message: Message) -> Element<'a, Message> {
    button(text(label).size(11).font(theme::MONO))
        .padding([5, 10])
        .style(if selected { theme::primary } else { theme::secondary })
        .on_press(message)
        .into()
}

fn field<'a>(label: &'static str, value: &str, which: BrokerField) -> Element<'a, Message> {
    container(super::field(label, value, move |v| Message::BrokerField(which, v))).width(Length::Fixed(150.0)).into()
}

fn preview(statement: &Result<String, String>) -> Element<'static, Message> {
    match statement {
        Ok(sql) => text(sql.clone()).size(11).font(theme::MONO).color(theme::TEXT_DIM).into(),
        Err(why) => text(why.clone()).size(11).color(theme::TEXT_FAINT).into(),
    }
}

fn create_form(studio: &Studio) -> Element<'_, Message> {
    let form = &studio.broker_form;
    let statement = dialect::broker_create_sql(form);
    let mut kinds = row![].spacing(4);
    for kind in BrokerCreateKind::ALL {
        kinds = kinds.push(choice(kind.label(), form.kind == kind, Message::BrokerKind(kind)));
    }
    let fields: Element<'_, Message> = match form.kind {
        BrokerCreateKind::Topic => row![
            field("Name", &form.name, BrokerField::Name),
            field("Partitions", &form.partitions, BrokerField::Partitions),
            field("Durability (sync/async/memory)", &form.durability, BrokerField::Durability),
            field("Retention (7d)", &form.retention, BrokerField::Retention),
            field("Retention bytes (10GB)", &form.retention_bytes, BrokerField::RetentionBytes),
        ]
        .spacing(8)
        .into(),
        BrokerCreateKind::Queue => row![
            field("Name", &form.name, BrokerField::Name),
            field("Durability", &form.durability, BrokerField::Durability),
            field("Ack timeout (30s)", &form.ack_timeout, BrokerField::AckTimeout),
            field("Max deliveries", &form.max_deliveries, BrokerField::MaxDeliveries),
            field("Dead letter", &form.dead_letter, BrokerField::DeadLetter),
            field("Message TTL (1h)", &form.retention, BrokerField::Retention),
        ]
        .spacing(8)
        .into(),
        BrokerCreateKind::Exchange => row![
            field("Name", &form.name, BrokerField::Name),
            field("Type (direct/fanout/topic/headers)", &form.exchange_type, BrokerField::ExchangeType),
        ]
        .spacing(8)
        .into(),
        BrokerCreateKind::Subscription => row![
            field("Name", &form.name, BrokerField::Name),
            field("On topic", &form.target, BrokerField::Target),
            field("Mode (ordered/shared)", &form.mode, BrokerField::Mode),
            field("From (EARLIEST/LATEST/offset)", &form.from, BrokerField::From),
            field("Ack timeout", &form.ack_timeout, BrokerField::AckTimeout),
            field("Max deliveries", &form.max_deliveries, BrokerField::MaxDeliveries),
            field("Dead letter", &form.dead_letter, BrokerField::DeadLetter),
        ]
        .spacing(8)
        .into(),
        BrokerCreateKind::Binding => row![
            field("Exchange", &form.name, BrokerField::Name),
            field("To topic or queue", &form.target, BrokerField::Target),
            field("Key (order.*)", &form.key, BrokerField::Key),
        ]
        .spacing(8)
        .into(),
    };
    container(
        column![
            section_label("CREATE"),
            kinds,
            row![
                fields,
                column![
                    Space::new().height(15),
                    super::primary_button(
                        if form.kind == BrokerCreateKind::Binding { "BIND" } else { "CREATE" },
                        statement.is_ok().then_some(Message::BrokerCreate)
                    ),
                ],
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            preview(&statement),
            text(
                "Empty fields are left out, so the server's defaults apply. A queue is a topic \
                 with one shared subscription; a broadcast subscription belongs to a connection \
                 and is made over the broker port, not here."
            )
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

fn publish_form(studio: &Studio) -> Element<'_, Message> {
    let form = &studio.broker_publish;
    let statement = dialect::broker_publish_sql(form);
    let route: Element<'_, Message> = if form.via_exchange {
        field("Routing key", &form.routing_key, BrokerField::PublishRoutingKey)
    } else {
        Space::new().width(0).into()
    };
    container(
        column![
            section_label("PUBLISH"),
            row![
                choice("TO A TOPIC OR QUEUE", !form.via_exchange, Message::BrokerViaExchange(false)),
                choice("THROUGH AN EXCHANGE", form.via_exchange, Message::BrokerViaExchange(true)),
            ]
            .spacing(4),
            row![
                field(
                    if form.via_exchange { "Exchange" } else { "Topic or queue" },
                    &form.target,
                    BrokerField::PublishTarget
                ),
                route,
                container(super::field("Value", &form.value, |v| Message::BrokerField(BrokerField::PublishValue, v)))
                    .width(Length::Fixed(280.0)),
                field("Key", &form.key, BrokerField::PublishKey),
                field("Header name", &form.header_name, BrokerField::PublishHeaderName),
                field("Header value", &form.header_value, BrokerField::PublishHeaderValue),
                column![
                    Space::new().height(15),
                    super::primary_button(
                        if form.via_exchange { "SEND" } else { "PUBLISH" },
                        statement.is_ok().then_some(Message::BrokerPublish)
                    ),
                ],
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            preview(&statement),
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

fn browse_panel(studio: &Studio) -> Element<'_, Message> {
    let read = &studio.broker_read;
    let controls = row![
        field("Partition (empty: all)", &read.partition, BrokerField::ReadPartition),
        field("From", &read.from, BrokerField::ReadFrom),
        field("Limit", &read.limit, BrokerField::ReadLimit),
        container(
            text("READ uses partition, from and limit; POP uses limit, and takes the messages it shows.")
                .size(10)
                .color(theme::TEXT_FAINT)
        )
        .padding([18, 0]),
    ]
    .spacing(8);
    let Some(browse) = &studio.broker_browse else {
        return container(
            column![
                section_label("MESSAGES"),
                controls,
                text("READ a topic or POP a queue below to see its messages here.").size(12).color(theme::TEXT_FAINT),
            ]
            .spacing(6),
        )
        .padding(8)
        .width(Fill)
        .style(theme::panel)
        .into();
    };
    let content: Element<'_, Message> = match &browse.result {
        None => text("Reading…").size(12).color(theme::TEXT_FAINT).into(),
        Some(Err(e)) => text(e.clone()).size(12).color(theme::BAD).into(),
        Some(Ok(grid)) if grid.rows.is_empty() => text("Nothing.").size(12).color(theme::TEXT_FAINT).into(),
        Some(Ok(grid)) => result_grid(grid),
    };
    container(
        column![
            row![
                section_label(browse.title.clone()),
                Space::new().width(Fill),
                small_button("CLOSE", Some(Message::BrokerCloseBrowse)),
            ],
            controls,
            text(browse.statement.clone()).size(11).font(theme::MONO).color(theme::TEXT_DIM),
            content,
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::panel)
    .into()
}

/// A statement's rows as they came: the columns of READ, POP and SHOW
/// PARTITIONS differ, and each is short.
fn result_grid(grid: &Grid) -> Element<'_, Message> {
    let width = |c: &str| match c {
        "value" => 360.0,
        "headers" => 200.0,
        "key" => 140.0,
        "time_us" => 150.0,
        _ => 90.0,
    };
    let mut header = row![].spacing(4);
    for c in &grid.columns {
        header = header.push(
            text(c.to_uppercase()).size(10).font(theme::MONO).color(theme::TEXT_DIM).width(Length::Fixed(width(c))),
        );
    }
    let mut body = column![header].spacing(2);
    for r in &grid.rows {
        let mut line = row![].spacing(4);
        for (c, v) in grid.columns.iter().zip(r) {
            let shown = match v {
                Some(v) => super::elide(v, 200).into_owned(),
                None => "NULL".to_string(),
            };
            line = line.push(
                text(shown)
                    .size(11)
                    .font(theme::MONO)
                    .color(if v.is_some() { theme::TEXT } else { theme::TEXT_FAINT })
                    .wrapping(iced::widget::text::Wrapping::None)
                    .width(Length::Fixed(width(c))),
            );
        }
        body = body.push(container(line).clip(true));
    }
    scrollable(body).direction(scrollable::Direction::Horizontal(scrollable::Scrollbar::default())).into()
}

fn panel<'a>(title: &'static str, empty: &'static str, rows: Vec<Element<'a, Message>>, header: Element<'a, Message>) -> Element<'a, Message> {
    let content: Element<'a, Message> = if rows.is_empty() {
        text(empty).size(12).color(theme::TEXT_FAINT).into()
    } else {
        let mut list = column![header].spacing(2);
        for r in rows {
            list = list.push(r);
        }
        scrollable(list).direction(scrollable::Direction::Horizontal(scrollable::Scrollbar::default())).into()
    };
    container(column![section_label(title), content].spacing(6)).padding(8).width(Fill).style(theme::panel).into()
}

fn cells<'a>(values: Vec<(String, f32, bool)>, actions: Element<'a, Message>) -> Element<'a, Message> {
    let mut line = row![].spacing(4).align_y(iced::alignment::Vertical::Center);
    for (v, w, right) in values {
        line = line.push(
            text(v)
                .size(12)
                .font(theme::MONO)
                .color(theme::TEXT)
                .wrapping(iced::widget::text::Wrapping::None)
                .align_x(if right { iced::alignment::Horizontal::Right } else { iced::alignment::Horizontal::Left })
                .width(Length::Fixed(w)),
        );
    }
    line.push(Space::new().width(8)).push(actions).into()
}

fn heads<'a>(names: &[(&'static str, f32, bool)]) -> Element<'a, Message> {
    let mut line = row![].spacing(4);
    for (n, w, right) in names {
        line = line.push(
            text(*n)
                .size(10)
                .font(theme::MONO)
                .color(theme::TEXT_DIM)
                .align_x(if *right { iced::alignment::Horizontal::Right } else { iced::alignment::Horizontal::Left })
                .width(Length::Fixed(*w)),
        );
    }
    line.into()
}

fn drop_button<'a>(message: Message) -> Element<'a, Message> {
    button(text("DROP").size(11).font(theme::MONO)).padding([5, 10]).style(theme::destructive).on_press(message).into()
}

fn topics<'a>(_studio: &'a Studio, s: &'a BrokerSnapshot) -> Element<'a, Message> {
    const H: [(&str, f32, bool); 7] = [
        ("TOPIC", 160.0, false),
        ("PARTITIONS", 80.0, true),
        ("DURABILITY", 90.0, false),
        ("MESSAGES", 90.0, true),
        ("SIZE", 80.0, true),
        ("RETENTION", 80.0, true),
        ("", 0.0, false),
    ];
    let rows = s
        .topics
        .iter()
        .filter(|t| !t.queue)
        .map(|t| {
            cells(
                vec![
                    (t.name.clone(), H[0].1, false),
                    (t.partitions.to_string(), H[1].1, true),
                    (t.durability.clone(), H[2].1, false),
                    (count(t.messages), H[3].1, true),
                    (bytes(t.bytes), H[4].1, true),
                    (dialect::broker_retention(t.retention_ms), H[5].1, true),
                ],
                row![
                    small_button("READ", Some(Message::BrokerRead(t.name.clone()))),
                    small_button("PARTITIONS", Some(Message::BrokerPartitions(t.name.clone()))),
                    drop_button(Message::BrokerDrop("TOPIC", t.name.clone())),
                ]
                .spacing(4)
                .into(),
            )
        })
        .collect();
    panel("TOPICS", "No topics. Make one above, or BROKER CREATE TOPIC name.", rows, heads(&H[..6]))
}

fn queues<'a>(_studio: &'a Studio, s: &'a BrokerSnapshot) -> Element<'a, Message> {
    const H: [(&str, f32, bool); 5] = [
        ("QUEUE", 160.0, false),
        ("MESSAGES", 90.0, true),
        ("READY", 80.0, true),
        ("UNACKED", 80.0, true),
        ("CONSUMERS", 80.0, true),
    ];
    let rows = s
        .queues
        .iter()
        .map(|q| {
            cells(
                vec![
                    (q.name.clone(), H[0].1, false),
                    (count(q.messages), H[1].1, true),
                    (count(q.ready), H[2].1, true),
                    (count(q.unacked), H[3].1, true),
                    (count(q.consumers), H[4].1, true),
                ],
                row![
                    small_button("READ", Some(Message::BrokerRead(q.name.clone()))),
                    small_button("POP", Some(Message::BrokerPop(q.name.clone()))),
                    drop_button(Message::BrokerDrop("QUEUE", q.name.clone())),
                ]
                .spacing(4)
                .into(),
            )
        })
        .collect();
    panel("QUEUES", "No queues.", rows, heads(&H))
}

fn subscriptions<'a>(_studio: &'a Studio, s: &'a BrokerSnapshot) -> Element<'a, Message> {
    const H: [(&str, f32, bool); 9] = [
        ("TOPIC", 140.0, false),
        ("SUBSCRIPTION", 140.0, false),
        ("MODE", 80.0, false),
        ("MEMBERS", 70.0, true),
        ("BACKLOG", 80.0, true),
        ("IN FLIGHT", 80.0, true),
        ("DELIVERED", 90.0, true),
        ("ACKED", 90.0, true),
        ("DEAD", 70.0, true),
    ];
    // A queue's own subscription is the queue: listed there, not here.
    let queue_names: Vec<&str> = s.queues.iter().map(|q| q.name.as_str()).collect();
    let rows = s
        .subscriptions
        .iter()
        .filter(|x| !(x.name == x.topic && queue_names.contains(&x.topic.as_str())))
        .map(|x| {
            cells(
                vec![
                    (x.topic.clone(), H[0].1, false),
                    (x.name.clone(), H[1].1, false),
                    (x.mode.clone(), H[2].1, false),
                    (count(x.members), H[3].1, true),
                    (count(x.backlog), H[4].1, true),
                    (count(x.inflight), H[5].1, true),
                    (count(x.delivered), H[6].1, true),
                    (count(x.acked), H[7].1, true),
                    (count(x.dead), H[8].1, true),
                ],
                drop_button(Message::BrokerDropSubscription(x.topic.clone(), x.name.clone())),
            )
        })
        .collect();
    panel("SUBSCRIPTIONS", "No subscriptions.", rows, heads(&H))
}

fn exchanges<'a>(_studio: &'a Studio, s: &'a BrokerSnapshot) -> Element<'a, Message> {
    const H: [(&str, f32, bool); 4] =
        [("EXCHANGE", 160.0, false), ("TYPE", 80.0, false), ("DURABLE", 70.0, false), ("BINDINGS", 80.0, true)];
    let rows: Vec<Element<'a, Message>> = s
        .exchanges
        .iter()
        .map(|x| {
            cells(
                vec![
                    (x.name.clone(), H[0].1, false),
                    (x.kind.clone(), H[1].1, false),
                    ((if x.durable { "yes" } else { "no" }).to_string(), H[2].1, false),
                    (count(x.bindings), H[3].1, true),
                ],
                drop_button(Message::BrokerDrop("EXCHANGE", x.name.clone())),
            )
        })
        .collect();
    const B: [(&str, f32, bool); 3] = [("EXCHANGE", 160.0, false), ("TO", 160.0, false), ("KEY", 200.0, false)];
    let bindings: Vec<Element<'a, Message>> = s
        .bindings
        .iter()
        .map(|b| {
            cells(
                vec![(b.exchange.clone(), B[0].1, false), (b.topic.clone(), B[1].1, false), (b.key.clone(), B[2].1, false)],
                small_button("UNBIND", Some(Message::BrokerUnbind(b.clone()))),
            )
        })
        .collect();
    column![
        panel("EXCHANGES", "No exchanges.", rows, heads(&H)),
        panel("BINDINGS", "No bindings.", bindings, heads(&B)),
    ]
    .spacing(10)
    .into()
}
