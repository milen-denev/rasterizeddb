//! Saved connections, on disk.
//!
//! One JSON file under the user's config directory. A password is written only
//! when the profile says to save it, and the file is created with owner-only
//! permissions where the platform has them — a console that drops credentials
//! into a world-readable file is worse than one that asks every time.

use std::path::PathBuf;

use crate::model::Connection;

/// Where the profiles live, or `None` when the platform has no config
/// directory — in which case the console runs and simply does not persist.
pub fn path() -> Option<PathBuf> {
    Some(dirs::config_dir()?.join("rdb_studio").join("connections.json"))
}

pub fn load() -> Vec<Connection> {
    let Some(path) = path() else { return Vec::new() };
    let Ok(text) = std::fs::read_to_string(&path) else { return Vec::new() };
    serde_json::from_str(&text).unwrap_or_default()
}

/// Writes the profiles, blanking any password the profile did not ask to keep.
///
/// The blanking happens here rather than at the call site so that no path can
/// persist a password by forgetting to.
pub fn save(profiles: &[Connection]) -> Result<(), String> {
    let Some(path) = path() else {
        return Err("no configuration directory on this platform".to_string());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    let sanitised: Vec<Connection> = profiles
        .iter()
        .map(|profile| {
            if profile.save_password {
                profile.clone()
            } else {
                Connection { password: String::new(), ..profile.clone() }
            }
        })
        .collect();

    let text = serde_json::to_string_pretty(&sanitised).map_err(|e| e.to_string())?;
    std::fs::write(&path, text).map_err(|e| e.to_string())?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(())
}
