//! Saved connections, on disk, and their passwords in the OS keyring.
//!
//! One JSON file under the user's config directory. A password is kept only
//! when the profile says to save it, and then in the operating system's
//! credential store unless the profile asks for the file: the file holds a
//! password only for a profile set to keep it there (or saved before there
//! was a keyring), and it is created with owner-only permissions where the
//! platform has them — a console that drops credentials into a world-readable
//! file is worse than one that asks every time.
//!
//! The keyring calls block (an unlock prompt waits for the user), so the
//! console makes them through [`blocking`], never on the thread that draws.

use std::path::{Path, PathBuf};

use crate::model::{Connection, PasswordStore};

/// The service the keyring entries are filed under; each entry's account is
/// the profile's name.
pub const KEYRING_SERVICE: &str = "rdb_studio";

/// Where the profiles live, or `None` when the platform has no config
/// directory — in which case the console runs and simply does not persist.
pub fn path() -> Option<PathBuf> {
    Some(dirs::config_dir()?.join("rdb_studio").join("connections.json"))
}

pub fn load() -> Vec<Connection> {
    let Some(path) = path() else { return Vec::new() };
    load_from(&path)
}

pub fn load_from(path: &Path) -> Vec<Connection> {
    let Ok(text) = std::fs::read_to_string(path) else { return Vec::new() };
    serde_json::from_str(&text).unwrap_or_default()
}

/// Writes the profiles, blanking every password the file is not meant to hold.
///
/// The blanking happens here rather than at the call site so that no path can
/// persist a password by forgetting to.
pub fn save(profiles: &[Connection]) -> Result<(), String> {
    let Some(path) = path() else {
        return Err("no configuration directory on this platform".to_string());
    };
    save_to(&path, profiles)
}

pub fn save_to(path: &Path, profiles: &[Connection]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string_pretty(&on_disk(profiles)).map_err(|e| e.to_string())?;

    // Written beside the file and renamed over it, so a crash leaves the old
    // profiles rather than half of the new ones, and created owner-only so
    // there is no moment at which a password sits in a file others can read.
    let staging = path.with_extension("json.tmp");
    let _ = std::fs::remove_file(&staging);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let written = (|| {
        use std::io::Write;
        let mut file = options.open(&staging)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&staging, path)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&staging);
        return Err(format!("could not write {}: {e}", path.display()));
    }

    // A file written by an earlier version may have been created readable by
    // others; the rename replaced it, but say so explicitly for the new one.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(())
}

/// The profiles as the file holds them: a password only where the profile
/// keeps it in the file, and every profile's store written out, so that one
/// saved before the choice stops being read by the rule for those.
fn on_disk(profiles: &[Connection]) -> Vec<Connection> {
    profiles
        .iter()
        .map(|profile| {
            let store = profile.password_store();
            let keep = profile.save_password && store == PasswordStore::File;
            Connection {
                password: if keep { profile.password.clone() } else { String::new() },
                password_store: Some(store),
                ..profile.clone()
            }
        })
        .collect()
}

/// A credential store, by profile name. The console's is [`OsKeyring`]; the
/// tests use one in memory.
pub trait Vault: Send + Sync {
    /// The password saved for the profile, or `None` when there is none.
    fn get(&self, profile: &str) -> Result<Option<String>, String>;
    fn set(&self, profile: &str, password: &str) -> Result<(), String>;
    /// Removes the profile's password; one that is not there is not an error.
    fn delete(&self, profile: &str) -> Result<(), String>;
}

/// The operating system's credential store, through the `keyring` crate.
pub struct OsKeyring;

impl OsKeyring {
    fn entry(profile: &str) -> Result<keyring::Entry, String> {
        keyring::Entry::new(KEYRING_SERVICE, profile).map_err(|e| describe(&e))
    }
}

/// A keyring error as the status line says it. `NoDefaultStore` means the
/// platform's store could not be reached at all, which on Linux is a desktop
/// without a Secret Service: said as that, with the way around it.
fn describe(error: &keyring::Error) -> String {
    match error {
        keyring::Error::NoDefaultStore => match keyring::Entry::store_status() {
            Err(cause) if !matches!(cause, keyring::Error::NoDefaultStore) => format!(
                "the OS keyring is not available ({cause}); keep the password in the profile file instead"
            ),
            _ => "the OS keyring is not available; keep the password in the profile file instead"
                .to_string(),
        },
        other => format!("the OS keyring: {other}"),
    }
}

impl Vault for OsKeyring {
    fn get(&self, profile: &str) -> Result<Option<String>, String> {
        match Self::entry(profile)?.get_password() {
            Ok(password) => Ok(Some(password)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(describe(&e)),
        }
    }

    fn set(&self, profile: &str, password: &str) -> Result<(), String> {
        Self::entry(profile)?.set_password(password).map_err(|e| describe(&e))
    }

    fn delete(&self, profile: &str) -> Result<(), String> {
        match Self::entry(profile)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(describe(&e)),
        }
    }
}

/// Runs a keyring call off the async executor's threads: the call blocks
/// while the store asks the user to unlock it.
pub async fn blocking<T: Send + 'static>(
    call: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|e| format!("the OS keyring call did not finish: {e}"))?
}

/// What saving `profile` does to the keyring, given what was saved under its
/// name before. A typed password is stored; an empty one leaves whatever is
/// stored (the form shows a saved password only once it has been read, and a
/// failed read must not become an erase); a profile that stops keeping its
/// password there has it removed.
pub fn keep_password(
    profile: &Connection,
    previous: Option<&Connection>,
    vault: &dyn Vault,
) -> Result<(), String> {
    if profile.password_in_keyring() {
        if !profile.password.is_empty() {
            vault.set(&profile.name, &profile.password)?;
        }
    } else if previous.is_some_and(Connection::password_in_keyring) {
        vault.delete(&profile.name)?;
    }
    Ok(())
}

/// Whether [`keep_password`] has anything to do, so a save that does not
/// involve the keyring never waits on it (or on its unlock prompt).
pub fn keeping_touches_keyring(profile: &Connection, previous: Option<&Connection>) -> bool {
    if profile.password_in_keyring() {
        !profile.password.is_empty()
    } else {
        previous.is_some_and(Connection::password_in_keyring)
    }
}

/// Removes a deleted profile's password from the keyring, if it kept one there.
pub fn forget_password(profile: &Connection, vault: &dyn Vault) -> Result<(), String> {
    if profile.password_in_keyring() {
        vault.delete(&profile.name)?;
    }
    Ok(())
}

/// The profile with its saved password filled in from the keyring, when it
/// keeps one there and none was typed. Nothing saved leaves it empty, and
/// the login says what is missing; a keyring that cannot be read is an error
/// rather than an attempt without the password.
pub fn with_saved_password(mut profile: Connection, vault: &dyn Vault) -> Result<Connection, String> {
    if profile.password.is_empty() && profile.password_in_keyring() {
        match vault.get(&profile.name) {
            Ok(Some(password)) => profile.password = password,
            Ok(None) => {}
            Err(e) => {
                return Err(format!(
                    "could not read the saved password of `{}`: {e}",
                    profile.name
                ))
            }
        }
    }
    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Memory {
        entries: Mutex<HashMap<String, String>>,
        broken: bool,
    }

    impl Memory {
        fn broken() -> Self {
            Self { broken: true, ..Self::default() }
        }
        fn held(&self, profile: &str) -> Option<String> {
            self.entries.lock().unwrap().get(profile).cloned()
        }
    }

    impl Vault for Memory {
        fn get(&self, profile: &str) -> Result<Option<String>, String> {
            if self.broken {
                return Err("locked".to_string());
            }
            Ok(self.held(profile))
        }
        fn set(&self, profile: &str, password: &str) -> Result<(), String> {
            if self.broken {
                return Err("locked".to_string());
            }
            self.entries.lock().unwrap().insert(profile.to_string(), password.to_string());
            Ok(())
        }
        fn delete(&self, profile: &str) -> Result<(), String> {
            if self.broken {
                return Err("locked".to_string());
            }
            self.entries.lock().unwrap().remove(profile);
            Ok(())
        }
    }

    fn profile(name: &str, password: &str, save: bool, store: Option<PasswordStore>) -> Connection {
        Connection {
            name: name.to_string(),
            password: password.to_string(),
            save_password: save,
            password_store: store,
            ..Connection::default()
        }
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rdb_studio_profiles_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("connections.json")
    }

    #[test]
    fn a_new_profile_keeps_its_password_in_the_keyring() {
        assert_eq!(Connection::default().password_store(), PasswordStore::Keyring);
    }

    #[test]
    fn a_profile_saved_before_the_choice_keeps_a_password_in_the_file() {
        let legacy: Connection = serde_json::from_str(
            r#"{"name":"old","flavor":"Postgres","host":"h","port":5432,"database":"d",
                "user":"u","password":"hunter2","save_password":true,"tls":"Prefer"}"#,
        )
        .unwrap();
        assert_eq!(legacy.password_store, None);
        assert_eq!(legacy.password_store(), PasswordStore::File);
        assert!(!legacy.password_in_keyring());

        let empty: Connection = serde_json::from_str(
            r#"{"name":"old","flavor":"Postgres","host":"h","port":5432,"database":"d",
                "user":"u","tls":"Prefer"}"#,
        )
        .unwrap();
        assert_eq!(empty.password_store(), PasswordStore::Keyring);
    }

    #[test]
    fn the_file_holds_a_password_only_for_a_profile_keeping_it_there() {
        let path = scratch("file");
        let profiles = vec![
            profile("keyring", "k-secret", true, Some(PasswordStore::Keyring)),
            profile("file", "f-secret", true, Some(PasswordStore::File)),
            profile("unsaved", "u-secret", false, Some(PasswordStore::File)),
            profile("legacy", "l-secret", true, None),
        ];
        save_to(&path, &profiles).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("k-secret"), "{text}");
        assert!(!text.contains("u-secret"), "{text}");
        assert!(text.contains("f-secret"), "{text}");
        assert!(text.contains("l-secret"), "{text}");

        let back = load_from(&path);
        assert_eq!(back.len(), 4);
        assert_eq!(back[0].password, "");
        assert_eq!(back[1].password, "f-secret");
        // The legacy profile's store is now written out, so it no longer
        // depends on its password being in the file to read as File.
        assert_eq!(back[3].password_store, Some(PasswordStore::File));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert!(!path.with_extension("json.tmp").exists());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn saving_stores_and_removes_keyring_passwords() {
        let vault = Memory::default();

        let saved = profile("p", "secret", true, Some(PasswordStore::Keyring));
        keep_password(&saved, None, &vault).unwrap();
        assert_eq!(vault.held("p").as_deref(), Some("secret"));

        // Saved again with the password not (yet) read: what is stored stays.
        let unread = profile("p", "", true, Some(PasswordStore::Keyring));
        keep_password(&unread, Some(&saved), &vault).unwrap();
        assert_eq!(vault.held("p").as_deref(), Some("secret"));

        // Moved to the file: out of the keyring.
        let to_file = profile("p", "secret", true, Some(PasswordStore::File));
        keep_password(&to_file, Some(&saved), &vault).unwrap();
        assert_eq!(vault.held("p"), None);

        // Saving turned off: out of the keyring.
        keep_password(&saved, Some(&to_file), &vault).unwrap();
        assert_eq!(vault.held("p").as_deref(), Some("secret"));
        let off = profile("p", "secret", false, Some(PasswordStore::Keyring));
        keep_password(&off, Some(&saved), &vault).unwrap();
        assert_eq!(vault.held("p"), None);

        // A profile that never used the keyring does not touch it.
        let other = profile("q", "x", false, Some(PasswordStore::File));
        keep_password(&other, Some(&other), &Memory::broken()).unwrap();
    }

    /// The app skips the keyring when `keeping_touches_keyring` says so; it
    /// must say so exactly when `keep_password` would not call the store.
    #[test]
    fn touching_the_keyring_is_predicted_exactly() {
        let stores = [None, Some(PasswordStore::Keyring), Some(PasswordStore::File)];
        let mut cases = Vec::new();
        for save in [false, true] {
            for store in stores {
                for password in ["", "pw"] {
                    cases.push(profile("p", password, save, store));
                }
            }
        }
        for form in &cases {
            for previous in std::iter::once(None).chain(cases.iter().map(Some)) {
                let touches = keeping_touches_keyring(form, previous);
                let refused = keep_password(form, previous, &Memory::broken()).is_err();
                assert_eq!(touches, refused, "form {form:?} previous {previous:?}");
            }
        }
    }

    #[test]
    fn a_keyring_that_refuses_fails_the_save() {
        let saved = profile("p", "secret", true, Some(PasswordStore::Keyring));
        assert!(keep_password(&saved, None, &Memory::broken()).is_err());
    }

    #[test]
    fn deleting_a_profile_removes_its_keyring_password() {
        let vault = Memory::default();
        let saved = profile("p", "secret", true, Some(PasswordStore::Keyring));
        keep_password(&saved, None, &vault).unwrap();
        forget_password(&saved, &vault).unwrap();
        assert_eq!(vault.held("p"), None);
    }

    #[test]
    fn connecting_reads_the_keyring_password_unless_one_was_typed() {
        let vault = Memory::default();
        vault.set("p", "stored").unwrap();

        let unread = profile("p", "", true, Some(PasswordStore::Keyring));
        assert_eq!(with_saved_password(unread.clone(), &vault).unwrap().password, "stored");

        let typed = profile("p", "typed", true, Some(PasswordStore::Keyring));
        assert_eq!(with_saved_password(typed, &vault).unwrap().password, "typed");

        let none = profile("absent", "", true, Some(PasswordStore::Keyring));
        assert_eq!(with_saved_password(none, &vault).unwrap().password, "");

        let file = profile("p", "", true, Some(PasswordStore::File));
        assert_eq!(with_saved_password(file, &Memory::broken()).unwrap().password, "");

        let err = with_saved_password(unread, &Memory::broken()).unwrap_err();
        assert!(err.contains("`p`") && err.contains("locked"), "{err}");
    }

    /// What a desktop with no credential store reaching it is told: run with
    /// `--ignored` and `DBUS_SESSION_BUS_ADDRESS` pointing nowhere.
    #[test]
    #[ignore]
    fn an_unreachable_keyring_says_so() {
        let err = OsKeyring.set("rdb_studio test unreachable", "x").unwrap_err();
        eprintln!("refusal: {err}");
        assert!(err.contains("OS keyring"), "{err}");
    }

    /// Against the machine's real credential store, under a service of its
    /// own: run with `--ignored` on a desktop that has one.
    #[test]
    #[ignore]
    fn the_os_keyring_round_trips_a_password() {
        struct Test;
        impl Test {
            fn entry() -> keyring::Entry {
                keyring::Entry::new("rdb_studio-test", "round-trip").unwrap()
            }
        }
        let entry = Test::entry();
        let _ = entry.delete_credential();
        assert!(matches!(entry.get_password(), Err(keyring::Error::NoEntry)));
        entry.set_password("pä$$wörd").unwrap();
        assert_eq!(Test::entry().get_password().unwrap(), "pä$$wörd");
        entry.delete_credential().unwrap();
        assert!(matches!(Test::entry().get_password(), Err(keyring::Error::NoEntry)));

        // And through the console's own vault, under its real service.
        let name = format!("rdb_studio test {}", std::process::id());
        OsKeyring.set(&name, "second").unwrap();
        assert_eq!(OsKeyring.get(&name).unwrap().as_deref(), Some("second"));
        OsKeyring.delete(&name).unwrap();
        assert_eq!(OsKeyring.get(&name).unwrap(), None);
        OsKeyring.delete(&name).unwrap();
    }
}
