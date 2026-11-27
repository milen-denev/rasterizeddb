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

use rdb_studio::{app, icon, theme};

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
        .window(window_settings())
        .antialiasing(true)
        .run()
}

/// The main window: its size, and the RasterizedDB mark as its icon. On Linux
/// the application id also names it, because a Wayland desktop takes the
/// taskbar icon from the `.desktop` file of that name, not from the window.
fn window_settings() -> iced::window::Settings {
    #[allow(unused_mut)]
    let mut settings = iced::window::Settings {
        size: iced::Size::new(1440.0, 900.0),
        min_size: Some(iced::Size::new(960.0, 600.0)),
        icon: icon::window_icon(),
        ..Default::default()
    };
    #[cfg(target_os = "linux")]
    {
        settings.platform_specific.application_id = icon::APP_ID.to_string();
    }
    settings
}
