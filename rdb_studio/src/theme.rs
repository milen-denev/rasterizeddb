//! The Rust palette, and the widget styles built on it.
//!
//! # Why a hand-built palette rather than one of iced's
//!
//! `Theme::custom` generates an extended palette from six colours by shifting
//! lightness, which produces a coherent theme but not a *specific* one. This
//! console wants Rust's own orange to read as the accent everywhere and
//! everything else to stay out of its way, so the six seeds are chosen against
//! that and the handful of surfaces the generator does not cover — the sidebar,
//! the grid lines, the header strip — are named here explicitly.
//!
//! The look is deliberately raw: square corners, one-pixel borders that are
//! actually visible, and a monospace face wherever a value is a value rather
//! than prose. A database console is a dense instrument, and rounded cards with
//! generous padding fit perhaps a third as many rows on a screen.

use iced::widget::{button, container, rule, scrollable, text_input};
use iced::{Background, Border, Color, Font, Theme, border, color};

/// Rust's brand orange. Everything accented is this or derived from it.
pub const RUST: Color = color!(0xCE422B);
/// The brighter orange of the logo's gear, for hover and focus.
pub const RUST_BRIGHT: Color = color!(0xF74C00);
/// A muted rust, for a selected row that must not shout.
pub const RUST_DIM: Color = color!(0x7A2A1D);

pub const BG: Color = color!(0x141110);
pub const PANEL: Color = color!(0x1B1816);
pub const RAISED: Color = color!(0x221E1B);
pub const LINE: Color = color!(0x322C28);
pub const LINE_BRIGHT: Color = color!(0x4A423C);

pub const TEXT: Color = color!(0xE8E2DC);
pub const TEXT_DIM: Color = color!(0x9A8F86);
pub const TEXT_FAINT: Color = color!(0x6B625B);

pub const OK: Color = color!(0x7FA65C);
pub const WARN: Color = color!(0xD9A441);
pub const BAD: Color = color!(0xCF4B3F);

/// Values, identifiers and SQL. Anything a person compares character by
/// character is monospaced; prose is not.
pub const MONO: Font = Font::MONOSPACE;

/// The theme every widget resolves its defaults from.
pub fn theme() -> Theme {
    Theme::custom(
        "Rust".to_string(),
        iced::theme::Palette {
            background: BG,
            text: TEXT,
            primary: RUST,
            success: OK,
            warning: WARN,
            danger: BAD,
        },
    )
}

// --- containers -------------------------------------------------------------

/// The window background, behind everything.
pub fn root(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(BG)),
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// A panel: one shade up from the background, with a hairline around it.
pub fn panel(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(PANEL)),
        border: Border { color: LINE, width: 1.0, radius: border::radius(0) },
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// A panel with no border, for nesting inside one that has it.
pub fn panel_flat(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(PANEL)),
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// The strip along the top of a panel that names it.
pub fn header(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(RAISED)),
        border: Border { color: LINE, width: 1.0, radius: border::radius(0) },
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// The title bar, which carries the accent so the window is identifiable at a
/// glance in a row of terminals.
pub fn titlebar(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(color!(0x191412))),
        border: Border {
            color: RUST_DIM,
            width: 0.0,
            radius: border::radius(0),
        },
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// A stat tile: the number, its label, and a rule underneath in the accent.
pub fn tile(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(RAISED)),
        border: Border { color: LINE, width: 1.0, radius: border::radius(0) },
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

/// Every other row of a grid, so a long line stays readable across.
pub fn zebra(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(color!(0x191614))),
        ..container::Style::default()
    }
}

/// An error report: red border, dark red fill, so it is unmissable but does not
/// glow.
pub fn error_box(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(color!(0x2A1513))),
        border: Border { color: BAD, width: 1.0, radius: border::radius(0) },
        text_color: Some(color!(0xF0B4AE)),
        ..container::Style::default()
    }
}

/// A statement that succeeded.
pub fn ok_box(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(color!(0x18200F))),
        border: Border { color: OK, width: 1.0, radius: border::radius(0) },
        text_color: Some(color!(0xC6DCA9)),
        ..container::Style::default()
    }
}

/// A modal's scrim, dimming what is behind it.
pub fn scrim(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(Color { a: 0.72, ..BG })),
        ..container::Style::default()
    }
}

/// A modal dialog.
pub fn dialog(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(PANEL)),
        border: Border { color: RUST, width: 1.0, radius: border::radius(0) },
        text_color: Some(TEXT),
        ..container::Style::default()
    }
}

// --- buttons ----------------------------------------------------------------

/// The one action a screen is for.
pub fn primary(_: &Theme, status: button::Status) -> button::Style {
    let bg = match status {
        button::Status::Hovered | button::Status::Pressed => RUST_BRIGHT,
        button::Status::Disabled => LINE,
        button::Status::Active => RUST,
    };
    button::Style {
        background: Some(Background::Color(bg)),
        text_color: if matches!(status, button::Status::Disabled) { TEXT_FAINT } else { color!(0xFFF6F2) },
        border: Border { color: bg, width: 1.0, radius: border::radius(0) },
        ..button::Style::default()
    }
}

/// Everything else: an outline that fills on hover.
pub fn secondary(_: &Theme, status: button::Status) -> button::Style {
    let (bg, border_color, text) = match status {
        button::Status::Hovered | button::Status::Pressed => (RAISED, RUST, TEXT),
        button::Status::Disabled => (PANEL, LINE, TEXT_FAINT),
        button::Status::Active => (PANEL, LINE_BRIGHT, TEXT_DIM),
    };
    button::Style {
        background: Some(Background::Color(bg)),
        text_color: text,
        border: Border { color: border_color, width: 1.0, radius: border::radius(0) },
        ..button::Style::default()
    }
}

/// A destructive action, which has to look like one before it is pressed.
pub fn destructive(_: &Theme, status: button::Status) -> button::Style {
    let (bg, text) = match status {
        button::Status::Hovered | button::Status::Pressed => (BAD, color!(0xFFF0EE)),
        button::Status::Disabled => (PANEL, TEXT_FAINT),
        button::Status::Active => (PANEL, BAD),
    };
    button::Style {
        background: Some(Background::Color(bg)),
        text_color: text,
        border: Border { color: BAD, width: 1.0, radius: border::radius(0) },
        ..button::Style::default()
    }
}

/// A tab across the top of the workspace. The active one carries the accent
/// underline rather than a fill, so the row reads as one strip.
pub fn tab(active: bool) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |_, status| {
        let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
        button::Style {
            background: Some(Background::Color(if active {
                PANEL
            } else if hovered {
                RAISED
            } else {
                BG
            })),
            text_color: if active { TEXT } else { TEXT_DIM },
            border: Border {
                color: if active { RUST } else { Color::TRANSPARENT },
                width: 0.0,
                radius: border::radius(0),
            },
            ..button::Style::default()
        }
    }
}

/// A row in the sidebar tree.
pub fn tree_item(active: bool) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |_, status| {
        let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
        button::Style {
            background: Some(Background::Color(if active {
                RUST_DIM
            } else if hovered {
                RAISED
            } else {
                Color::TRANSPARENT
            })),
            text_color: if active { TEXT } else { TEXT_DIM },
            border: Border { color: Color::TRANSPARENT, width: 0.0, radius: border::radius(0) },
            ..button::Style::default()
        }
    }
}

/// A grid cell that can be clicked into for editing. It has to look flat until
/// hovered, or a grid of them is unreadable.
pub fn cell(selected_cell: bool) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |_, status| {
        let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
        button::Style {
            background: Some(Background::Color(if selected_cell {
                RUST_DIM
            } else if hovered {
                RAISED
            } else {
                Color::TRANSPARENT
            })),
            text_color: TEXT,
            border: Border {
                color: if selected_cell { RUST } else { Color::TRANSPARENT },
                width: 1.0,
                radius: border::radius(0),
            },
            ..button::Style::default()
        }
    }
}

/// A column header that sorts the grid when clicked.
///
/// Flat by default, because a header row of buttons that look like buttons
/// reads as a toolbar rather than as a heading. What makes it discoverable is
/// the hover: the header lights up under the cursor the way every other
/// clickable thing here does.
pub fn sort_header(active: bool) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |_, status| {
        let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
        button::Style {
            background: Some(Background::Color(if hovered { LINE } else { Color::TRANSPARENT })),
            text_color: if active { RUST_BRIGHT } else { RUST },
            border: Border { color: Color::TRANSPARENT, width: 0.0, radius: border::radius(0) },
            ..button::Style::default()
        }
    }
}

// --- inputs -----------------------------------------------------------------

pub fn input(_: &Theme, status: text_input::Status) -> text_input::Style {
    let border_color = match status {
        text_input::Status::Focused { .. } => RUST,
        text_input::Status::Hovered => LINE_BRIGHT,
        _ => LINE,
    };
    text_input::Style {
        background: Background::Color(color!(0x100E0D)),
        border: Border { color: border_color, width: 1.0, radius: border::radius(0) },
        icon: TEXT_DIM,
        placeholder: TEXT_FAINT,
        value: TEXT,
        selection: RUST_DIM,
    }
}

pub fn hairline(_: &Theme) -> rule::Style {
    rule::Style {
        color: LINE,
        radius: border::radius(0),
        fill_mode: rule::FillMode::Full,
        snap: true,
    }
}

/// Built from iced's own scrollbar rather than from scratch: it carries an
/// auto-scroll overlay whose look this console has no opinion about, and
/// restating it here would mean tracking a struct that is not ours.
pub fn scroller(theme: &Theme, status: scrollable::Status) -> scrollable::Style {
    let hovered = matches!(
        status,
        scrollable::Status::Hovered { .. } | scrollable::Status::Dragged { .. }
    );
    let color = if hovered { RUST } else { LINE_BRIGHT };
    let mut style = scrollable::default(theme, status);
    for rail in [&mut style.vertical_rail, &mut style.horizontal_rail] {
        rail.background = Some(Background::Color(BG));
        rail.border = Border { color: Color::TRANSPARENT, width: 0.0, radius: border::radius(0) };
        rail.scroller.background = Background::Color(color);
        rail.scroller.border =
            Border { color: Color::TRANSPARENT, width: 0.0, radius: border::radius(0) };
    }
    style
}

/// The two-pixel accent bar under an active tab or a panel heading.
pub fn accent_underline(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(RUST)),
        ..container::Style::default()
    }
}

/// Its inactive counterpart, which keeps the row of tabs the same height
/// whichever one is active.
pub fn no_underline(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::TRANSPARENT)),
        ..container::Style::default()
    }
}

/// The drag handle between two grid columns. Visible enough to be found and
/// quiet enough not to look like a column of its own.
pub fn grip(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(LINE)),
        ..container::Style::default()
    }
}
