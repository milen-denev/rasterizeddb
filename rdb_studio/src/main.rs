//! rdb_studio — a database console for RasterizedDB, and for PostgreSQL.
//!
//! Run it with no arguments; the connection screen is the first thing it shows.
//! A saved profile can be opened straight away by naming it:
//!
//! ```text
//! rdb_studio            # pick a connection
//! rdb_studio local      # open the profile called `local`
//! ```

// A console is a windowed application. On Windows a console subsystem binary
// opens a terminal behind it, which is noise the user did not ask for; the
// attribute is inert everywhere else.
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

use rdb_studio::{app, theme};

fn main() -> iced::Result {
    // rustls picks a provider from its features, but only when exactly one is
    // enabled. Installing it explicitly means a later dependency turning on a
    // second backend produces a clear failure here rather than a panic inside
    // the first TLS handshake.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let opening = std::env::args().nth(1);

    iced::application(move || app::Studio::new(opening.clone()), app::Studio::update, app::Studio::view)
        .title(app::Studio::title)
        .subscription(app::Studio::subscription)
        // The parameter is annotated so the closure is inferred as
        // higher-ranked over the state's lifetime; without it rustc picks one
        // concrete lifetime and the builder's `for<'a>` bound does not hold.
        .theme(|_state: &app::Studio| theme::theme())
        .window(iced::window::Settings {
            size: iced::Size::new(1440.0, 900.0),
            min_size: Some(iced::Size::new(960.0, 600.0)),
            ..Default::default()
        })
        .antialiasing(true)
        .run()
}
