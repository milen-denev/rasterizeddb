//! `STORAGE ... STORE` — defining key/value stores from SQL.
//!
//! ```text
//! STORAGE ADD STORE archive S3 WITH (
//!     ENDPOINT = 'https://s3.us-east-1.amazonaws.com',
//!     REGION   = 'us-east-1',
//!     BUCKET   = 'my-bucket',
//!     PREFIX   = 'rdb/archive',
//!     ADDRESSING = 'virtual',
//!     CHUNK_SIZE = '4MiB')
//! STORAGE ADD STORE scratch DIRECTORY WITH (PATH = '/mnt/fast/kv', PREFIX = 'scratch')
//! STORAGE ADD STORE IF NOT EXISTS archive S3 WITH (...)   -- issuable on every start-up
//! STORAGE ALTER STORE archive SET CACHE_BYTES = '512MiB'
//! STORAGE DROP STORE scratch
//! SHOW STORAGE STORES
//! ```
//!
//! The counterpart of the location statements in [`super::sql`], and persisted
//! for the same reason: a store that exists only in this process is one every
//! table placed on it stops finding at the next restart. Where they differ is
//! what a mistake costs. A location is a directory, and a wrong one fails
//! loudly at the first write; a store is an address, and a wrong one is a
//! prefix that reads as an empty table. So the checks here are about *when* a
//! definition is allowed to change, not only whether it parses.

use rclite::Arc;

use crate::core::database::Database;

/// The credential options, and every spelling of each.
///
/// A store that carries its own signs with them instead of the process's
/// environment, which is what lets two stores in one database live in two
/// accounts. They never reach `db_kv.cfg` — see
/// [`super::kv_registry::KV_SECRETS_FILE`] for where they go and why that is a
/// second file.
const ACCESS_KEY_OPTIONS: &[&str] = &["access_key_id", "access_key"];
const SECRET_KEY_OPTIONS: &[&str] = &["secret_access_key", "secret_key", "secret"];
const SESSION_TOKEN_OPTIONS: &[&str] = &["session_token", "token"];

/// Whether `key` is any of them, for the paths that only need to know that much.
fn is_credential(key: &str) -> bool {
    ACCESS_KEY_OPTIONS.contains(&key)
        || SECRET_KEY_OPTIONS.contains(&key)
        || SESSION_TOKEN_OPTIONS.contains(&key)
}

/// What every statement here says when the feature is not compiled in.
#[cfg(not(feature = "kv_storage"))]
fn unavailable() -> String {
    "this server was built without the kv_storage feature, so it has no key/value stores"
        .to_string()
}

// ---------------------------------------------------------------------------
// STORAGE ADD STORE
// ---------------------------------------------------------------------------

/// `STORAGE ADD STORE <name> <kind> WITH (...)`
#[cfg(feature = "kv_storage")]
pub fn add_store(
    tokens: &[String],
    options: Vec<(String, String)>,
    database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    use super::kv_registry::{DEFAULT_PREFIX, StoreDefinition};

    // `IF NOT EXISTS` is what makes this issuable on every start-up. A
    // deployment that configures its own store has no good moment to decide
    // whether it is the first one, and wrapping the statement in a `SHOW` and a
    // branch is a race as well as a nuisance.
    let mut at = 3;
    if tokens.get(at..at + 3).is_some_and(|words| {
        ["IF", "NOT", "EXISTS"]
            .iter()
            .zip(words)
            .all(|(k, w)| w.eq_ignore_ascii_case(k))
    }) {
        at += 3;
    }
    let if_not_exists = at > 3;

    let name = tokens
        .get(at)
        .map(|word| unquote(word).to_string())
        .ok_or("expected STORAGE ADD STORE [IF NOT EXISTS] <name> <S3|DIRECTORY> WITH (...)")?;
    let kind = tokens
        .get(at + 1)
        .map(|word| unquote(word).to_ascii_lowercase())
        .ok_or_else(|| {
            format!(
                "expected a kind after the name: STORAGE ADD STORE {name} S3 WITH (...), \
                 or STORAGE ADD STORE {name} DIRECTORY WITH (PATH = '/mnt/fast/kv')"
            )
        })?;

    let (mut address, tuning, credentials) = split_options(options)?;
    let take = |address: &mut Vec<(String, String)>, key: &str| -> Option<String> {
        let at = address.iter().position(|(k, _)| k == key)?;
        Some(address.remove(at).1)
    };

    let definition = match kind.as_str() {
        "dir" | "directory" | "local" | "path" => {
            let path = take(&mut address, "path")
                .or_else(|| take(&mut address, "directory"))
                .ok_or("a directory store needs PATH, as WITH (PATH = '/mnt/fast/kv')")?;
            let prefix = take(&mut address, "prefix")
                .or_else(|| take(&mut address, "root"))
                .unwrap_or_else(|| DEFAULT_PREFIX.to_string());
            reject_leftovers(&address, "a directory store takes PATH and PREFIX")?;
            if credentials.is_some() {
                // Refused rather than stored and ignored. A credential accepted
                // for something that never signs a request is one the operator
                // believes is in use.
                return Err(
                    "a directory store signs nothing, so it has no use for credentials".to_string(),
                );
            }
            StoreDefinition::Directory { path, prefix }
        }
        #[cfg(feature = "kv_storage_s3")]
        "s3" | "object" => {
            let endpoint = take(&mut address, "endpoint").ok_or(
                "an S3 store needs ENDPOINT, as WITH (ENDPOINT = 'https://s3.us-east-1.amazonaws.com')",
            )?;
            let bucket = take(&mut address, "bucket").ok_or("an S3 store needs BUCKET")?;
            // Every S3 request is signed over a region, so one is always sent.
            // Defaulted rather than demanded because the stores that ignore it —
            // MinIO, Ceph, R2 — are exactly the ones an operator sets up first.
            let region = take(&mut address, "region").unwrap_or_else(|| "us-east-1".to_string());
            let prefix = take(&mut address, "prefix")
                .or_else(|| take(&mut address, "root"))
                .unwrap_or_else(|| DEFAULT_PREFIX.to_string());
            let path_style =
                match take(&mut address, "addressing").or_else(|| take(&mut address, "style")) {
                    Some(value) => super::kv_registry::parse_addressing(&value)?,
                    None => false,
                };
            reject_leftovers(
                &address,
                "an S3 store takes ENDPOINT, REGION, BUCKET, PREFIX and ADDRESSING",
            )?;
            StoreDefinition::S3 {
                endpoint,
                region,
                bucket,
                prefix,
                path_style,
            }
        }
        #[cfg(not(feature = "kv_storage_s3"))]
        "s3" | "object" => {
            return Err(
                "this server was built without the kv_storage_s3 feature, so it cannot open \
                 an S3 store. A DIRECTORY store uses the same object layout on a local disk"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "unknown store kind {other:?}: expected S3 or DIRECTORY"
            ));
        }
    };

    let registry = registry(database)?;
    let carries_credentials = credentials.is_some();

    if if_not_exists && registry.has(&name) {
        // Idempotent only when it is genuinely the same store. A definition
        // that disagrees is refused and says which fields do, because the two
        // ways of being lenient are both worse: leaving the old address in
        // place makes the statement a lie, and adopting the new one repoints a
        // store out from under whatever is already on it.
        let differ = registry.differences(&name, &definition);
        if !differ.is_empty() {
            return Err(format!(
                "key/value store '{name}' already exists and differs in {}. \
                 STORAGE ALTER STORE {name} SET <key> = <value> changes it — and is refused \
                 while tables are placed on it, because changing where a store points moves \
                 no bytes. SHOW STORAGE STORES reports what is configured",
                differ.join(", ")
            ));
        }
        let mut lines = vec![format!(
            "key/value store '{name}' already exists with this definition"
        )];
        // Credentials are still applied. Re-issuing the statement with a key
        // read from the environment is how a deployment rotates one, and
        // refusing that would make the idempotent path the one that cannot.
        if let Some(credentials) = credentials {
            registry.set_credentials(&name, Some(credentials))?;
            lines.push("its credentials were replaced with the ones given".to_string());
            if registry.is_open(&name) {
                lines.push(
                    "this server already has the store open, so it keeps signing with the \
                     credentials it opened with until it restarts"
                        .to_string(),
                );
            }
        }
        lines.push(registry.describe(&name).unwrap_or_default());
        return Ok(lines);
    }

    registry.add_store(&name, definition, tuning, credentials)?;

    let described = registry.describe(&name).unwrap_or_default();
    let mut lines = vec![format!("key/value store '{name}' added: {described}")];
    if carries_credentials {
        // Said because the operator has just handed the server a long-lived
        // secret, and where it went is not guessable.
        lines.push(format!(
            "its credentials are stored in {}, mode 0600, beside {} — which holds no \
             secrets and stays safe to read",
            super::kv_registry::KV_SECRETS_FILE,
            super::kv_registry::KV_FILE,
        ));
        lines.push(
            "a copy of the database directory takes that file with it; a backup of this \
             database is as sensitive as the key in it"
                .to_string(),
        );
    } else {
        lines.push(
            "it signs with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the server's \
             environment; give it ACCESS_KEY_ID and SECRET_ACCESS_KEY to use its own"
                .to_string(),
        );
    }
    lines.push(format!(
        "place a table on it with STORAGE PLACE TABLE <name> AT 'kv:{name}', before it is created"
    ));
    // Said every time, because the store has not been contacted: nothing here
    // opened a connection, so a wrong endpoint, a missing bucket or a rejected
    // credential is still silent at this point and shows up as the first table
    // failing to open.
    lines.push(format!(
        "nothing has been written to it yet; the first table placed there is what proves \
         the address and {}",
        if carries_credentials {
            "these credentials"
        } else {
            "the environment's credentials"
        }
    ));
    Ok(lines)
}

#[cfg(not(feature = "kv_storage"))]
pub fn add_store(
    _tokens: &[String],
    _options: Vec<(String, String)>,
    _database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    Err(unavailable())
}

// ---------------------------------------------------------------------------
// STORAGE ALTER STORE
// ---------------------------------------------------------------------------

/// `STORAGE ALTER STORE <name> SET <key> = <value>[, ...]`
#[cfg(feature = "kv_storage")]
pub fn alter_store(
    statement: &str,
    tokens: &[String],
    database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    let name = tokens
        .get(3)
        .map(|word| unquote(word).to_string())
        .ok_or("expected STORAGE ALTER STORE <name> SET <key> = <value>")?;

    let upper = statement.to_ascii_uppercase();
    let at = upper
        .rfind(" SET ")
        .ok_or("expected STORAGE ALTER STORE <name> SET <key> = <value>")?;
    let mut changes = Vec::new();
    let mut credential_parts: Vec<(String, String)> = Vec::new();
    for assignment in statement[at + " SET ".len()..].split(',') {
        let assignment = assignment.trim();
        if assignment.is_empty() {
            continue;
        }
        let (key, value) = assignment.split_once('=').ok_or_else(|| {
            // Quoting the assignment would put a secret in the error, and
            // this is the one parse error that can be looking at one.
            format!("expected <key> = <value> in STORAGE ALTER STORE {name} SET ...")
        })?;
        let key = key.trim().to_ascii_lowercase();
        if is_credential(&key) {
            credential_parts.push((key, unquote(value).to_string()));
        } else {
            changes.push((key, unquote(value).to_string()));
        }
    }

    let registry = registry(database)?;
    if !registry.has(&name) {
        return Err(unknown_store(&name, &registry.names()));
    }

    // Rotating a key is not a change of address: it moves no bytes, so unlike
    // ENDPOINT or BUCKET it is allowed while tables are placed on the store.
    // Applied before the rest so a statement that also fails on an address
    // change still leaves the new credentials in place — the half an operator
    // rotating a compromised key needs to have landed.
    let mut applied = Vec::new();
    if !credential_parts.is_empty() {
        let take = |names: &[&str]| -> Option<String> {
            credential_parts
                .iter()
                .find(|(key, _)| names.contains(&key.as_str()))
                .map(|(_, value)| value.clone())
        };
        let credentials = credentials_from(
            take(ACCESS_KEY_OPTIONS),
            take(SECRET_KEY_OPTIONS),
            take(SESSION_TOKEN_OPTIONS),
        )?;
        registry.set_credentials(&name, credentials)?;
        applied.push("credentials".to_string());
    }

    // An address change is refused while anything is relying on the store
    // pointing where it points, and that is the whole point of this statement's
    // shape. The two reasons are different and are named separately, because
    // the operator's next step is different: move the tables off, or restart.
    let occupants = tables_on(database, &name);
    let _ = &occupants;
    let address_block = if !occupants.is_empty() {
        Some(format!(
            "Move these off it first, or drop them: {}",
            occupants.join(", ")
        ))
    } else if registry.is_open(&name) {
        Some(
            "This server has it open, and every table on it shares that one family; \
             restart before repointing it"
                .to_string(),
        )
    } else {
        None
    };

    if !changes.is_empty() {
        applied.extend(registry.alter_store(&name, &changes, address_block.as_deref())?);
    }

    let mut lines = vec![format!(
        "key/value store '{name}': {} changed",
        applied.join(", ")
    )];
    lines.push(registry.describe(&name).unwrap_or_default());
    if registry.is_open(&name) {
        // The family carries the geometry and the page budget it was built
        // with, and it is shared by every open table on the store. Rebuilding
        // it underneath them would change a live file's chunk size, which is
        // not a setting — it is a different layout for the same bytes.
        lines.push(
            "this store is already open, so the running server keeps the settings it was \
             opened with until it restarts"
                .to_string(),
        );
    }
    Ok(lines)
}

#[cfg(not(feature = "kv_storage"))]
pub fn alter_store(
    _statement: &str,
    _tokens: &[String],
    _database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    Err(unavailable())
}

// ---------------------------------------------------------------------------
// STORAGE DROP STORE
// ---------------------------------------------------------------------------

/// `STORAGE DROP STORE <name>`
#[cfg(feature = "kv_storage")]
pub fn drop_store(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let name = tokens
        .get(3)
        .map(|word| unquote(word).to_string())
        .ok_or("expected STORAGE DROP STORE <name>")?;

    let registry = registry(database)?;
    if !registry.has(&name) {
        return Err(unknown_store(&name, &registry.names()));
    }

    // Refused rather than cascaded. A placement naming a store that is gone is
    // what stops the *database* opening — see `KvRegistry::validate` — so this
    // would be a statement that succeeds now and refuses to start later.
    let occupants = tables_on(database, &name);
    if !occupants.is_empty() {
        return Err(format!(
            "{} table(s) are placed on '{name}': {}. Move them off first \
             (STORAGE MOVE TABLE <name> TO '/some/directory'), or drop them; a placement \
             naming a store that is gone stops this database opening at all",
            occupants.len(),
            occupants.join(", ")
        ));
    }

    registry.drop_store(&name)?;
    Ok(vec![
        format!("key/value store '{name}' is no longer configured"),
        // Said because it is the difference between this and DROP TABLE: the
        // objects are still billed, and only the operator knows whether
        // something else is reading them.
        "its objects were not deleted; remove them from the store itself if nothing else \
         needs them"
            .to_string(),
    ])
}

#[cfg(not(feature = "kv_storage"))]
pub fn drop_store(_tokens: &[String], _database: &Arc<Database>) -> Result<Vec<String>, String> {
    Err(unavailable())
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

/// The tables placed on `store`, live or merely pinned.
#[cfg(feature = "kv_storage")]
pub fn tables_on(database: &Arc<Database>, store: &str) -> Vec<String> {
    let mut out: Vec<String> = database
        .storage
        .placements()
        .into_iter()
        .filter(|(_, spec)| {
            matches!(super::TableBackend::parse(spec), super::TableBackend::KeyValue(name) if name == store)
        })
        .map(|(table, _)| table)
        .collect();
    out.sort_unstable();
    out
}

#[cfg(not(feature = "kv_storage"))]
pub fn tables_on(_database: &Arc<Database>, _store: &str) -> Vec<String> {
    Vec::new()
}

/// One line per store, for `SHOW STORAGE`.
#[cfg(feature = "kv_storage")]
pub fn summary_lines(database: &Arc<Database>) -> Vec<String> {
    let Ok(registry) = registry(database) else {
        return Vec::new();
    };
    registry
        .list()
        .into_iter()
        .map(|store| {
            let tables = tables_on(database, &store.name).len();
            // Endpoint, bucket and prefix together, because any one of them
            // being wrong is the same mistake and the line exists to be read
            // against what the operator meant to type.
            let mut at = store.endpoint.clone();
            if !store.bucket.is_empty() {
                at = format!("{at}/{}", store.bucket);
            }
            if !store.prefix.is_empty() {
                at = format!("{at} ({})", store.prefix);
            }
            format!(
                "kv:{}: {}{}, {} table(s), {} credentials{}{}",
                store.name,
                store.kind,
                if at.is_empty() {
                    String::new()
                } else {
                    format!(" {at}")
                },
                tables,
                store.credentials,
                if store.settings.is_empty() {
                    String::new()
                } else {
                    format!(", {}", store.settings)
                },
                if store.supported {
                    ""
                } else {
                    ", not supported by this build"
                },
            )
        })
        .collect()
}

#[cfg(not(feature = "kv_storage"))]
pub fn summary_lines(_database: &Arc<Database>) -> Vec<String> {
    Vec::new()
}

/// `SHOW STORAGE KEYS` entries contributed by the store surface.
#[cfg(feature = "kv_storage")]
pub fn tunable_help() -> Vec<String> {
    super::kv_registry::TUNABLES
        .iter()
        .map(|t| format!("{:<20} {}", t.name, t.help))
        .collect()
}

#[cfg(not(feature = "kv_storage"))]
pub fn tunable_help() -> Vec<String> {
    Vec::new()
}

#[cfg(feature = "kv_storage")]
fn registry(database: &Arc<Database>) -> Result<&super::kv_registry::KvRegistry, String> {
    if database.storage.root().is_none() {
        return Err("this database is in memory and has no key/value stores".to_string());
    }
    Ok(&database.kv)
}

#[cfg(feature = "kv_storage")]
fn unknown_store(name: &str, known: &[String]) -> String {
    if known.is_empty() {
        format!(
            "no key/value store named '{name}': this database has none. \
             STORAGE ADD STORE {name} S3 WITH (...) defines one"
        )
    } else {
        format!(
            "no key/value store named '{name}'. This database has: {}",
            known.join(", ")
        )
    }
}

/// The three parts of a `WITH (...)`: the address fields, still as text; the
/// tunables, already parsed into the numbers the config takes; and the
/// credentials, if the statement gave any.
#[cfg(feature = "kv_storage")]
type SplitOptions = (
    Vec<(String, String)>,
    Vec<(String, u64)>,
    Option<super::kv_registry::StoreCredentials>,
);

/// Splits `WITH (...)` three ways, rejecting an unknown key on the way through.
#[cfg(feature = "kv_storage")]
fn split_options(options: Vec<(String, String)>) -> Result<SplitOptions, String> {
    let mut address = Vec::new();
    let mut tuning = Vec::new();
    let mut access_key = None;
    let mut secret_key = None;
    let mut session_token = None;

    for (key, value) in options {
        if ACCESS_KEY_OPTIONS.contains(&key.as_str()) {
            access_key = Some(value);
        } else if SECRET_KEY_OPTIONS.contains(&key.as_str()) {
            secret_key = Some(value);
        } else if SESSION_TOKEN_OPTIONS.contains(&key.as_str()) {
            session_token = Some(value);
        } else {
            match super::kv_registry::tunable(&key) {
                Some(field) => {
                    tuning.push((key, super::kv_registry::parse_setting(field, &value)?))
                }
                None => address.push((key, value)),
            }
        }
    }
    tuning.sort_by(|a, b| a.0.cmp(&b.0));
    check_tunable_geometry(&tuning)?;
    Ok((
        address,
        tuning,
        credentials_from(access_key, secret_key, session_token)?,
    ))
}

/// The default `page_size` a store takes when the statement does not name one,
/// from `KvConfig::new`. Duplicated rather than imported because this check has
/// to run for a store that has never been opened — there is no config yet to
/// ask. `a_chunk_size_that_is_not_whole_pages_is_refused` pins the two together.
#[cfg(feature = "kv_storage")]
const DEFAULT_PAGE_SIZE: u64 = 64 * 1024;

/// Refuses a chunk/page geometry the store could never open with.
///
/// `KvConfig::validate` already rejects it — but that runs when the *provider*
/// opens, which is the first time a table is placed there and long after the
/// statement that made the mistake. Everything in between looked like success:
/// `STORAGE ADD STORE` returned ok, `SHOW STORAGE STORES` listed the store,
/// tables could be placed on it and appeared to accept writes and reads. The
/// only symptoms were `opened = f` in the store listing and a single line in
/// the server's log.
///
/// That is the failure this module's header warns about — "a store is an
/// address, and a wrong one is a prefix that reads as an empty table" — arriving
/// through the geometry rather than the address. Checked here so the operator
/// is told at the point they are still looking at the setting.
///
/// Found the hard way: `CHUNK_SIZE = 4096000` is decimal four million, not
/// `4MiB`, and 4,096,000 is not a whole number of 65,536-byte pages. The
/// difference is invisible unless you are looking for it.
#[cfg(feature = "kv_storage")]
fn check_tunable_geometry(tuning: &[(String, u64)]) -> Result<(), String> {
    let get = |name: &str| tuning.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
    let chunk = get("chunk_size");
    let page = get("page_size");
    // Nothing to check when the statement sets neither: the defaults agree.
    if chunk.is_none() && page.is_none() {
        return Ok(());
    }
    let page = page.unwrap_or(DEFAULT_PAGE_SIZE);
    let chunk = chunk.unwrap_or(4 * 1024 * 1024);

    if page == 0 || chunk == 0 {
        return Err("CHUNK_SIZE and PAGE_SIZE must be greater than zero".to_string());
    }
    if chunk % page != 0 {
        // The usual cause is a decimal/binary mix-up - `4096000` typed for what
        // was meant as `4MiB`. The nearest multiples of the page size are *not*
        // the useful advice there: for 4,096,000 they are 4,063,232 and
        // 4,128,768, while the value actually wanted is 4,194,304, which is
        // neither. So the binary size the number was reaching for is offered
        // first, and the arithmetic neighbours only after it.
        const MIB: u64 = 1024 * 1024;
        let meant = ((chunk + MIB / 2) / MIB) * MIB;
        let hint = if meant != 0 && meant != chunk && meant % page == 0 {
            format!(
                "; if you meant {} MiB, write '{}MiB' or {}",
                meant / MIB,
                meant / MIB,
                meant
            )
        } else {
            String::new()
        };
        let down = (chunk / page) * page;
        let up = down + page;
        return Err(format!(
            "CHUNK_SIZE {chunk} is not a whole number of {page}-byte pages, so this store could never be opened{hint}. The nearest whole-page sizes are {down} and {up}"
        ));
    }
    Ok(())
}

/// Assembles a credential from the halves a statement gave, refusing a pair
/// that is not one.
///
/// A lone half is an error rather than a partial update. A secret with no key id
/// cannot sign anything, and accepting it would leave a store that looks
/// configured and fails at the first request — by which time the operator has
/// moved on and the failure reads as the endpoint being wrong.
#[cfg(feature = "kv_storage")]
fn credentials_from(
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
) -> Result<Option<super::kv_registry::StoreCredentials>, String> {
    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key)) => {
            if access_key.trim().is_empty() || secret_key.trim().is_empty() {
                return Err(
                    "ACCESS_KEY_ID and SECRET_ACCESS_KEY cannot be empty. Leave both out \
                     entirely for a store that signs with the server's environment"
                        .to_string(),
                );
            }
            for (what, value) in [
                ("ACCESS_KEY_ID", &access_key),
                ("SECRET_ACCESS_KEY", &secret_key),
            ] {
                // The value would be split across fields on the way back in, so
                // a store defined with one would come back as a different one.
                // Refused without quoting it.
                if value.contains('\t') || value.contains('\n') {
                    return Err(format!("{what} cannot contain a tab or a newline"));
                }
            }
            if session_token
                .as_deref()
                .is_some_and(|token| token.contains('\t') || token.contains('\n'))
            {
                return Err("SESSION_TOKEN cannot contain a tab or a newline".to_string());
            }
            Ok(Some(super::kv_registry::StoreCredentials {
                access_key,
                secret_key,
                session_token: session_token.filter(|token| !token.trim().is_empty()),
            }))
        }
        (None, None) if session_token.is_some() => Err(
            "SESSION_TOKEN is only meaningful beside ACCESS_KEY_ID and SECRET_ACCESS_KEY"
                .to_string(),
        ),
        (None, None) => Ok(None),
        (Some(_), None) => Err(
            "ACCESS_KEY_ID was given without SECRET_ACCESS_KEY: a key id alone cannot sign \
             a request"
                .to_string(),
        ),
        (None, Some(_)) => Err(
            "SECRET_ACCESS_KEY was given without ACCESS_KEY_ID: a secret alone cannot sign \
             a request"
                .to_string(),
        ),
    }
}

/// Rejects the options a kind did not consume, rather than ignoring them.
#[cfg(feature = "kv_storage")]
fn reject_leftovers(address: &[(String, String)], takes: &str) -> Result<(), String> {
    if address.is_empty() {
        return Ok(());
    }
    Err(format!(
        "unknown option(s) {}: {}, plus {}",
        address
            .iter()
            .map(|(k, _)| k.to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join(", "),
        takes,
        super::kv_registry::TUNABLES
            .iter()
            .map(|t| t.name.to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// Strips one layer of `'` or `"` quoting. The same rule [`super::sql`] uses.
fn unquote(value: &str) -> &str {
    let value = value.trim();
    for quote in ['\'', '"'] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}
