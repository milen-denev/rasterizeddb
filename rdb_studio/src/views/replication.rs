//! Replication: is it on, who is it talking to, and how far behind are they.
//!
//! # The two topologies
//!
//! The page renders both servers with the same columns because the questions
//! are the same — who is it, is it connected, how far behind — but the
//! direction is not, and the page says which it is looking at rather than
//! leaving it implied:
//!
//! - **RasterizedDB is multi-master.** Every node accepts writes and ships them
//!   to the others. A peer is somewhere this node *sends to*, and lag is
//!   measured in transactions it has not confirmed.
//! - **PostgreSQL streams primary to standby.** A row is a standby *receiving*
//!   from here, and lag is measured in bytes of WAL.
//!
//! Showing "lag: 4096" without saying which unit that is in would be worse than
//! showing nothing.
//!
//! # What counts as a fault
//!
//! A paused peer is not a broken one, and colouring it red teaches people to
//! ignore red. [`PeerHealth`] separates the four states so only the two that
//! need attention get it.

use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let flavor = studio.flavor();
    let manageable = dialect::replication_is_manageable(flavor);

    let Some(snapshot) = &studio.replication else {
        return framed(
            "REPLICATION",
            Some(small_button("RELOAD", Some(Message::ReloadReplication))),
            container(text("Loading…").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };

    if !snapshot.supported {
        return framed(
            "REPLICATION",
            Some(small_button("RELOAD", Some(Message::ReloadReplication))),
            unsupported(),
        );
    }

    let body = column![
        health(snapshot, flavor),
        summary(snapshot, flavor),
        controls(studio, snapshot, manageable),
        peers(studio, snapshot, flavor, manageable),
        counters(studio, snapshot),
    ]
    .spacing(10);

    framed(
        "REPLICATION",
        Some(small_button("RELOAD", Some(Message::ReloadReplication))),
        scrollable(body).height(Fill).style(theme::scroller).into(),
    )
}

/// Same trap as the backup page: a build from before the statement existed
/// answers it as a runtime parameter rather than refusing it, and silence has
/// to be distinguished from a "no".
fn unsupported() -> Element<'static, Message> {
    container(
        column![
            text("This server does not report replication status")
                .size(15)
                .color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            text(
                "It answered SHOW REPLICATION STATS as if it were a runtime parameter, which is \
                 what a build from before that statement existed does. The text forms still work \
                 — run SHOW REPLICATION or SHOW REPLICATION PEERS from the query editor — but \
                 this page needs the structured ones. Upgrade the server."
            )
            .size(12)
            .color(theme::TEXT_DIM),
        ]
        .spacing(10)
        .max_width(720),
    )
    .padding(14)
    .into()
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

fn health(snapshot: &ReplicationSnapshot, flavor: Flavor) -> Element<'static, Message> {
    let mut warnings = column![].spacing(6);
    let mut any = false;

    if snapshot.active() == Some(false) {
        any = true;
        warnings = warnings.push(banner(
            theme::WARN,
            "REPLICATION IS NOT ACTIVE",
            match flavor {
                Flavor::Rasterized =>
                    "Committed transactions are not being shipped anywhere. Peers keep whatever \
                     they already had; everything written here since is only here.",
                Flavor::Postgres =>
                    "No standby is streaming from this server and it is not one itself. Nothing \
                     written here is being copied anywhere by streaming replication.",
            }
            .to_string(),
        ));
    }

    let down: Vec<String> = snapshot
        .peers
        .iter()
        .filter(|peer| peer.health() == PeerHealth::Down)
        .map(|peer| peer.name.clone())
        .collect();
    if !down.is_empty() {
        any = true;
        warnings = warnings.push(banner(
            theme::BAD,
            "PEERS ARE DOWN",
            format!(
                "{} is not connected. Anything committed here is not reaching {}, and the history \
                 kept for a peer is finite — past the retention it has to resynchronise from \
                 scratch rather than catch up.",
                down.join(", "),
                if down.len() == 1 { "it" } else { "them" }
            ),
        ));
    }

    let dropped: u64 = snapshot.peers.iter().map(|peer| peer.dropped).sum();
    if dropped > 0 {
        any = true;
        let names: Vec<&str> = snapshot
            .peers
            .iter()
            .filter(|peer| peer.dropped > 0)
            .map(|peer| peer.name.as_str())
            .collect();
        warnings = warnings.push(banner(
            theme::WARN,
            "A PEER QUEUE HAS OVERFLOWED",
            format!(
                "{} records could not be queued for {}. They are not lost — the sender backfills \
                 from the log — but a queue that fills is a peer that is not keeping up.",
                count(dropped),
                names.join(", ")
            ),
        ));
    }

    // A slot nobody is using still pins WAL, which is how a primary runs out of
    // disk because of a standby that was decommissioned last month.
    let inactive = snapshot.number("inactive_slots").unwrap_or(0);
    if inactive > 0 {
        any = true;
        warnings = warnings.push(banner(
            theme::BAD,
            "THERE ARE INACTIVE REPLICATION SLOTS",
            format!(
                "{} slot(s) have no client. A slot holds WAL until whoever owns it consumes it, so \
                 an abandoned one fills the disk and stops the server. Drop it if the standby is \
                 gone.",
                inactive
            ),
        ));
    }

    if !any {
        return Space::new().height(0).into();
    }
    warnings.into()
}

fn banner(colour: iced::Color, heading: &'static str, body: String) -> Element<'static, Message> {
    let style = if colour == theme::BAD { theme::error_box } else { theme::tile };
    container(
        column![
            text(heading).size(11).font(theme::MONO).color(colour),
            text(body).size(11).color(theme::TEXT_DIM),
        ]
        .spacing(3),
    )
    .padding(9)
    .width(Fill)
    .style(style)
    .into()
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

fn summary(snapshot: &ReplicationSnapshot, flavor: Flavor) -> Element<'static, Message> {
    let text_of = |metric: &str| snapshot.metric(metric).unwrap_or("—").to_string();

    let worst = snapshot.number("max_peer_lag").unwrap_or(0);
    let (lag_value, lag_unit) = match flavor {
        Flavor::Rasterized => (count(worst), "transactions behind"),
        Flavor::Postgres => (bytes(worst), "bytes of WAL behind"),
    };

    column![
        row![
            super::tile(
                "STATE",
                match snapshot.active() {
                    Some(true) => "ACTIVE".to_string(),
                    Some(false) => "OFF".to_string(),
                    None => "—".to_string(),
                },
                Some(
                    match flavor {
                        Flavor::Rasterized => "multi-master: every node accepts writes",
                        Flavor::Postgres => "streaming: primary to standby",
                    }
                    .to_string()
                ),
            ),
            super::tile(
                "PEERS",
                format!("{} / {}", text_of("peers_connected"), text_of("peers")),
                Some("connected of configured".to_string()),
            ),
            super::tile("MAX LAG", lag_value, Some(lag_unit.to_string())),
            super::tile("MODE", text_of("mode"), Some("how long a commit waits".to_string())),
            super::tile("LOCAL SEQUENCE", text_of("local_sequence"), None),
        ]
        .spacing(10),
        row![
            labelled("NODE", text_of("node_id")),
            labelled("CLUSTER", text_of("cluster")),
            labelled("LISTEN", text_of("listen")),
            labelled("CONFLICT", text_of("conflict")),
            labelled("COMPRESSION", text_of("compression")),
        ]
        .spacing(24),
    ]
    .spacing(10)
    .into()
}

fn labelled(label: &'static str, v: String) -> Element<'static, Message> {
    column![
        text(label).size(10).font(theme::MONO).color(theme::TEXT_FAINT),
        text(v).size(12).font(theme::MONO),
    ]
    .spacing(2)
    .into()
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

fn controls<'a>(
    studio: &'a Studio,
    snapshot: &'a ReplicationSnapshot,
    manageable: bool,
) -> Element<'a, Message> {
    if !manageable {
        return container(
            text(
                "PostgreSQL replication is configured in postgresql.conf and by whoever connects \
                 to it, not from SQL. This page reports; it changes nothing.",
            )
            .size(11)
            .color(theme::TEXT_DIM),
        )
        .padding(8)
        .width(Fill)
        .style(theme::header)
        .into();
    }

    let active = snapshot.active().unwrap_or(false);

    container(
        row![
            if active {
                iced::widget::button(text("DISABLE REPLICATION").size(11).font(theme::MONO))
                    .padding([5, 10])
                    .style(theme::destructive)
                    .on_press(Message::SetReplicationActive(false))
            } else {
                iced::widget::button(text("ENABLE REPLICATION").size(11).font(theme::MONO))
                    .padding([5, 10])
                    .style(theme::primary)
                    .on_press(Message::SetReplicationActive(true))
            },
            super::field("Peer name", &studio.peer_name, Message::PeerName),
            super::field("Address", &studio.peer_address, Message::PeerAddress),
            column![
                Space::new().height(15),
                small_button("ADD PEER", Some(Message::AddPeer)),
            ],
            Space::new().width(Fill),
            text(
                "A change here is not written to disk: it is gone after a restart unless the \
                 matching --peer or --replication flag is added too.",
            )
            .size(10)
            .color(theme::TEXT_FAINT)
            .width(Length::Fixed(300.0)),
        ]
        .spacing(8)
        .align_y(iced::alignment::Vertical::Bottom),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

// ---------------------------------------------------------------------------
// Peers
// ---------------------------------------------------------------------------

fn peers<'a>(
    studio: &'a Studio,
    snapshot: &'a ReplicationSnapshot,
    flavor: Flavor,
    manageable: bool,
) -> Element<'a, Message> {
    if snapshot.peers.is_empty() {
        return container(
            text(match flavor {
                Flavor::Rasterized =>
                    "No peers. Add one above, or start the server with --peer name@host:port.",
                Flavor::Postgres => "No standby is streaming from this server.",
            })
            .size(12)
            .color(theme::TEXT_FAINT),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    let layout = super::GridLayout::new(studio, "replication");
    let columns = vec![
        super::GridColumn::new("peer", "PEER", 140.0),
        super::GridColumn::new("address", "ADDRESS", 180.0),
        super::GridColumn::new("state", "STATE", 90.0),
        super::GridColumn::new(
            "lag",
            // The same question in different units, which the header says
            // rather than leaving it to be guessed from the magnitude.
            match flavor {
                Flavor::Rasterized => "LAG (TXN)",
                Flavor::Postgres => "LAG (WAL)",
            },
            104.0,
        )
        .right(),
        super::GridColumn::new("acked", "ACKED", 118.0).right(),
        super::GridColumn::new("queued", "QUEUED", 84.0).right(),
        super::GridColumn::new("sent", "SENT", 104.0).right(),
        super::GridColumn::new("dropped", "DROPPED", 92.0).right(),
        super::GridColumn::new("codec", "CODEC", 84.0),
        super::GridColumn::new("actions", "", 264.0),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    let mut body = column![].spacing(0);
    for (index, peer) in snapshot.peers.iter().enumerate() {
        let health = peer.health();
        let colour = match health {
            PeerHealth::Healthy => theme::OK,
            PeerHealth::Behind => theme::WARN,
            PeerHealth::Paused => theme::TEXT_DIM,
            PeerHealth::Down => theme::BAD,
        };

        let actions: Element<'a, Message> = if manageable {
            container(
                row![
                    small_button(
                        if peer.paused { "RESUME" } else { "PAUSE" },
                        Some(Message::PausePeer(peer.name.clone(), !peer.paused)),
                    ),
                    small_button("RESYNC", Some(Message::ResyncPeer(peer.name.clone()))),
                    iced::widget::button(text("DROP").size(11).font(theme::MONO))
                        .padding([5, 10])
                        .style(theme::destructive)
                        .on_press(Message::DropPeer(peer.name.clone())),
                ]
                .spacing(4),
            )
            .padding([2, 6])
            .into()
        } else {
            Space::new().into()
        };

        let lag_text = match flavor {
            Flavor::Rasterized => count(peer.lag),
            Flavor::Postgres => bytes(peer.lag),
        };

        let mut line = column![super::grid_row(
            &widths,
            vec![
                super::grid_cell(peer.name.clone(), false, theme::TEXT),
                super::grid_cell(peer.address.clone(), false, theme::TEXT_DIM),
                container(super::pill(health.label(), colour)).padding([3, 6]).into(),
                super::grid_cell(
                    lag_text,
                    true,
                    if peer.lag > 0 { theme::WARN } else { theme::TEXT_DIM },
                ),
                super::grid_cell(count(peer.acked_seq), true, theme::TEXT_DIM),
                super::grid_cell(count(peer.queued), true, theme::TEXT_DIM),
                super::grid_cell(count(peer.records_sent), true, theme::TEXT_DIM),
                super::grid_cell(
                    count(peer.dropped),
                    true,
                    if peer.dropped > 0 { theme::BAD } else { theme::TEXT_FAINT },
                ),
                super::grid_cell(peer.codec.clone(), false, theme::TEXT_FAINT),
                actions,
            ],
        )]
        .spacing(0);

        // When a peer is down the reason is the whole content of the row, and
        // it is far too long to live in a column.
        if !peer.last_error.is_empty() {
            line = line.push(
                container(
                    text(super::elide(&peer.last_error, 220))
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
            section_label(match flavor {
                Flavor::Rasterized => "PEERS · THIS NODE SENDS TO THEM",
                Flavor::Postgres => "STANDBYS · THEY STREAM FROM THIS SERVER",
            }),
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
// Counters
// ---------------------------------------------------------------------------

/// Everything the tiles did not promote, as it came back.
fn counters(studio: &Studio, snapshot: &ReplicationSnapshot) -> Element<'static, Message> {
    let rest: Vec<Stat> = snapshot
        .stats
        .iter()
        .filter(|stat| !PROMOTED.contains(&stat.metric.as_str()))
        .cloned()
        .collect();
    let height = 26.0 + rest.len() as f32 * 19.0;

    column![
        section_label("COUNTERS AND SETTINGS"),
        container(super::metric_grid(studio, "replication:counters", &rest))
            .height(Length::Fixed(height)),
    ]
    .spacing(2)
    .into()
}

/// Metrics the tiles and the identity row already show.
const PROMOTED: &[&str] = &[
    "active",
    "peers",
    "peers_connected",
    "max_peer_lag",
    "mode",
    "local_sequence",
    "node_id",
    "cluster",
    "listen",
    "conflict",
    "compression",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(connected: bool, paused: bool, lag: u64, dropped: u64) -> PeerStatus {
        PeerStatus {
            name: "berlin".into(),
            address: "10.0.0.2:61171".into(),
            remote_node: 2,
            connected,
            paused,
            acked_seq: 10,
            lag,
            queued: 0,
            records_sent: 100,
            batches_sent: 4,
            dropped,
            codec: "zstd".into(),
            last_ratio: String::new(),
            last_error: String::new(),
        }
    }

    /// A paused peer is a decision, not a fault. Colouring it like an outage
    /// teaches people to ignore the colour.
    #[test]
    fn a_paused_peer_is_not_a_broken_one() {
        assert_eq!(peer(false, true, 99, 0).health(), PeerHealth::Paused);
        assert_eq!(peer(true, true, 0, 0).health(), PeerHealth::Paused);
    }

    #[test]
    fn health_separates_down_from_behind() {
        assert_eq!(peer(true, false, 0, 0).health(), PeerHealth::Healthy);
        assert_eq!(peer(true, false, 5, 0).health(), PeerHealth::Behind);
        assert_eq!(peer(true, false, 0, 3).health(), PeerHealth::Behind);
        assert_eq!(peer(false, false, 0, 0).health(), PeerHealth::Down);
    }

    /// Silence is not "off": a server that would not answer must not render as
    /// one that answered no.
    #[test]
    fn an_unanswered_state_is_not_off() {
        let snapshot = ReplicationSnapshot { stats: Vec::new(), supported: true, peers: Vec::new() };
        assert_eq!(snapshot.active(), None);
    }
}
