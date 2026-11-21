//! Authentication for the native (rastcp) protocol.
//!
//! The same accounts as the PostgreSQL listener: both read [`crate::auth`]'s
//! role catalogue, so `CREATE USER alice PASSWORD '...'` issued over either
//! protocol produces one account that works on both, and a password is stored
//! once, in PostgreSQL's own verifier format.
//!
//! What differs is how much the exchange can insist on, because the native
//! protocol has no third-party clients to accommodate:
//!
//! - **SCRAM-SHA-256 only.** `md5` and cleartext exist in the PostgreSQL
//!   listener because drivers that speak nothing else still exist. Nothing
//!   speaks this protocol but this crate, so the weak methods are simply not
//!   offered.
//! - **Channel binding is mandatory.** Every exchange is bound to the TLS 1.3
//!   exporter of the connection it runs on (RFC 9266). This is the part that
//!   matters given that the default client does not verify the server's
//!   certificate: without binding, anyone in the network path could relay the
//!   whole exchange onto their own connection to the real server and end up
//!   authenticated as the caller. With it, a relayed proof verifies nowhere.
//! - **The client checks the server too.** The server's final message proves it
//!   holds the stored verifier, which an impersonator does not.
//!
//! The password itself never crosses the wire in either direction, and the
//! server stores nothing that can be replayed as one.
//!
//! # On a unix socket
//!
//! A unix socket has no TLS and needs none: only root can watch it, and who
//! may connect is decided by its file's permissions. SCRAM runs there exactly
//! as over TCP, bound to [`rastcp::session::UNIX_CHANNEL_BINDING`] in place of
//! the exporter. A socket can also be set to [`AuthMethod::Peer`]: the kernel
//! reports which operating-system user connected, and a role of that name may
//! log in without a password — what PostgreSQL calls peer authentication.

use std::fmt;
use std::time::Duration;

use rastcp::error::RastcpError;
use rastcp::session::{AuthChannel, AuthFuture, Principal, ServerAuthenticator};
use rclite::Arc;

use crate::auth::{AuthMethod, ChannelBinding, ScramServer, Verifier};
use crate::core::database::Database;

/// Version of this exchange, so a future revision can be told apart from a
/// client that is simply confused.
const PROTOCOL_VERSION: u8 = 1;

/// The hello that also names a database: `[C_HELLO][2][u16 len][user]
/// [u16 len][database]`, lengths little-endian. A client sends it only when
/// it names a database, so a server older than it refuses the login by its
/// version ("unsupported authentication protocol version 2") rather than
/// letting the caller into the default database unasked.
const PROTOCOL_VERSION_DATABASE: u8 = 2;

// Client -> server message tags.
const C_HELLO: u8 = 0x01;
const C_SASL_INITIAL: u8 = 0x02;
const C_SASL_FINAL: u8 = 0x03;

// Server -> client message tags.
const S_OK: u8 = 0x00;
const S_SASL: u8 = 0x01;
const S_CONTINUE: u8 = 0x02;
const S_ERROR: u8 = 0xFF;

/// How long a refused caller is held before being told.
///
/// Not a defence on its own — an attacker can open many connections — but it
/// turns "guess a few thousand passwords a second down one connection" into
/// something that has to be worth the attacker's while, and it costs a
/// legitimate user who mistyped a password nothing they will notice.
const REFUSAL_DELAY: Duration = Duration::from_millis(250);

/// The one thing a refused caller is ever told.
///
/// A wrong password, an unknown user and a role that may not log in are
/// deliberately indistinguishable: anything else turns the listener into a way
/// to enumerate accounts.
fn refusal(user: &str) -> String {
    format!("authentication failed for user \"{}\"", user)
}

/// How the native listener authenticates.
#[derive(Debug, Clone)]
pub struct NativeAuthConfig {
    /// Only [`AuthMethod::Trust`] and [`AuthMethod::ScramSha256`] are accepted
    /// here; see the module documentation for why the other two are not.
    pub method: AuthMethod,
    /// Role seeded when the catalogue is empty, so there is someone to log in
    /// as on a fresh database.
    pub bootstrap_user: String,
    /// Password for that role. `None` seeds a role that cannot authenticate,
    /// rather than a passwordless superuser.
    pub bootstrap_password: Option<String>,
}

impl Default for NativeAuthConfig {
    fn default() -> Self {
        Self {
            method: AuthMethod::ScramSha256,
            bootstrap_user: "rasterized".to_string(),
            bootstrap_password: None,
        }
    }
}

impl NativeAuthConfig {
    /// Rejects the methods this protocol deliberately does not offer, for the
    /// TCP listener.
    pub fn validate(&self) -> Result<(), String> {
        match self.method {
            AuthMethod::Peer => Err(
                "peer authentication is offered only on a unix socket, where the kernel says \
                 who is connecting; over TCP the caller would only be saying so. Set it for the \
                 socket with --rdb-socket-auth peer."
                    .to_string(),
            ),
            method => Self::validate_method(method),
        }
    }

    /// Rejects the methods the native protocol does not offer on a unix
    /// socket, which is all of the TCP listener's and [`AuthMethod::Peer`].
    pub fn validate_socket_method(method: AuthMethod) -> Result<(), String> {
        match method {
            AuthMethod::Peer => Ok(()),
            method => Self::validate_method(method),
        }
    }

    fn validate_method(method: AuthMethod) -> Result<(), String> {
        match method {
            AuthMethod::Trust | AuthMethod::ScramSha256 | AuthMethod::Peer => Ok(()),
            AuthMethod::Md5 | AuthMethod::Password => Err(format!(
                "the native protocol does not offer {}: it exists in the PostgreSQL listener \
                 only for drivers that speak nothing else. Use scram-sha-256.",
                method.name()
            )),
        }
    }

    /// Whether a caller has to prove anything.
    pub fn authenticates(&self) -> bool {
        self.method != AuthMethod::Trust
    }
}

/// Creates the role catalogue and its bootstrap role if there is none yet.
pub async fn seed_roles(database: &Arc<Database>, config: &NativeAuthConfig) -> Result<(), String> {
    if config.method == AuthMethod::Trust {
        return Ok(());
    }
    crate::auth::ensure_role_catalogue(
        database,
        &config.bootstrap_user,
        config.bootstrap_password.as_deref(),
        config.method,
    )
    .await
}

// ----------------------------------------------------------------- server

/// The operating-system user at the other end of `peer`, if it is a unix
/// socket and the user has a name.
fn peer_user(peer: &rastcp::net::Peer) -> Option<String> {
    #[cfg(unix)]
    if let rastcp::net::Peer::Unix(peer) = peer {
        return os_user_name(peer.uid);
    }
    let _ = peer;
    None
}

/// The login name of `uid`, from the password database.
#[cfg(unix)]
pub fn os_user_name(uid: u32) -> Option<String> {
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::passwd = std::ptr::null_mut();
    let mut buffer = vec![0u8; 4096];

    loop {
        // SAFETY: every pointer is to memory owned here and live for the call;
        // `getpwuid_r` writes the entry's strings into `buffer`, which outlives
        // the read of `pw_name` below.
        let status = unsafe {
            libc::getpwuid_r(
                uid,
                &mut entry,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };

        if status == libc::ERANGE && buffer.len() < 1 << 20 {
            buffer.resize(buffer.len() * 2, 0);
            continue;
        }
        if status != 0 || found.is_null() || entry.pw_name.is_null() {
            return None;
        }

        // SAFETY: `pw_name` points into `buffer`, NUL-terminated by getpwuid_r.
        let name = unsafe { std::ffi::CStr::from_ptr(entry.pw_name) };
        return name.to_str().ok().map(str::to_string);
    }
}

/// Checks callers against the role catalogue.
pub struct RoleAuthenticator {
    database: Arc<Database>,
    method: AuthMethod,
    /// The server's databases, which a hello naming one is resolved against.
    /// `None` for a listener serving `database` alone: a hello may then name
    /// only that database.
    catalog: Option<Arc<crate::core::database::catalog::DatabaseCatalog>>,
}

impl fmt::Debug for RoleAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoleAuthenticator")
            .field("method", &self.method.name())
            .finish()
    }
}

impl RoleAuthenticator {
    pub fn new(database: Arc<Database>, method: AuthMethod) -> Self {
        Self {
            database,
            method,
            catalog: None,
        }
    }

    /// Resolves a hello that names a database against `catalog`, and checks
    /// the caller against that database's roles, as the PostgreSQL listener
    /// does: roles are per database
    /// ([`crate::core::database::catalog`]).
    pub fn with_catalog(
        mut self,
        catalog: Option<Arc<crate::core::database::catalog::DatabaseCatalog>>,
    ) -> Self {
        self.catalog = catalog;
        self
    }

    /// The database a hello named, or why it cannot be logged in to. Said
    /// to a caller that has not authenticated yet, as pgwire says it: a
    /// database's roles decide the login, so it has to be resolved first,
    /// and database names are not secret.
    fn resolve(&self, name: &str) -> Result<Arc<Database>, String> {
        crate::core::database::resolve_native_database(
            self.catalog.as_deref(),
            &self.database,
            name,
        )
        .map_err(|(_, message)| message)
    }

    /// Tells the caller why, then fails.
    ///
    /// The message reaches the client so a mistyped password says so instead of
    /// looking like a network fault; it carries nothing that distinguishes one
    /// kind of failure from another.
    async fn refuse(channel: &mut AuthChannel<'_>, user: &str) -> RastcpError {
        tokio::time::sleep(REFUSAL_DELAY).await;
        let mut message = vec![S_ERROR];
        message.extend_from_slice(refusal(user).as_bytes());
        let _ = channel.send(&message).await;
        RastcpError::AuthenticationError(refusal(user))
    }

    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<Principal, RastcpError> {
        let hello = channel.recv().await?;
        let (user, named) = parse_hello(&hello)?;

        if user.is_empty() {
            return Err(Self::refuse(channel, &user).await);
        }

        // The database whose roles decide this login: the one the hello
        // named, or this listener's own.
        let database = match named.as_deref() {
            None => self.database.clone(),
            Some(name) => match self.resolve(name) {
                Ok(database) => database,
                Err(e) => {
                    let mut message = vec![S_ERROR];
                    message.extend_from_slice(e.as_bytes());
                    let _ = channel.send(&message).await;
                    return Err(RastcpError::AuthenticationError(e));
                }
            },
        };
        let principal = |user: String, superuser: bool| match &named {
            Some(name) => Principal::new(user, superuser).in_database(name.clone()),
            None => Principal::new(user, superuser),
        };

        // Looked up before anything is decided, so that the same messages and
        // roughly the same work happen whether or not the role exists.
        let role = crate::auth::lookup_role(&database, &user).await;
        let may_login = role.as_ref().map(|r| r.can_login).unwrap_or(false);
        let superuser = role.as_ref().map(|r| r.is_superuser).unwrap_or(false);

        if self.method == AuthMethod::Trust {
            channel.send(&[S_OK]).await?;
            return Ok(principal(user, superuser));
        }

        if self.method == AuthMethod::Peer {
            // Who the kernel says connected, not who the caller says it is.
            // A caller on TCP has no such answer, and is refused: this
            // authenticator is only ever put on a unix socket, and if it is
            // put anywhere else it must not let anyone in.
            let connected_as = peer_user(&channel.peer());
            if connected_as.as_deref() != Some(user.as_str()) || !may_login {
                return Err(Self::refuse(channel, &user).await);
            }
            channel.send(&[S_OK]).await?;
            return Ok(principal(user, superuser));
        }

        let verifier = match role.as_ref().and_then(|r| r.verifier.clone()) {
            // A password stored as written is usable: derive the SCRAM keys
            // from it now rather than refusing an account an operator set up by
            // hand.
            Some(Verifier::Plain(password)) => crate::auth::build_scram_verifier(&password),
            Some(verifier) => verifier,
            // No role, or one with no password. The exchange still runs to
            // completion against a decoy so that it fails at the proof, the
            // same place a wrong password does.
            None => Verifier::Plain(String::new()),
        };

        channel.send(&[S_SASL]).await?;

        let binding = ChannelBinding::TlsExporter(channel.channel_binding().to_vec());

        let client_first = read_text(channel, C_SASL_INITIAL).await?;
        let (server, server_first) =
            match ScramServer::start_bound(verifier, &client_first, &binding) {
                Ok(started) => started,
                Err(e) => {
                    // A malformed or downgraded client-first is a protocol
                    // fault, not a wrong password: saying so helps the operator
                    // and tells an attacker nothing about the account.
                    let mut message = vec![S_ERROR];
                    message.extend_from_slice(e.as_bytes());
                    let _ = channel.send(&message).await;
                    return Err(RastcpError::AuthenticationError(e));
                }
            };

        let mut message = vec![S_CONTINUE];
        message.extend_from_slice(server_first.as_bytes());
        channel.send(&message).await?;

        let client_final = read_text(channel, C_SASL_FINAL).await?;
        let Ok(server_final) = server.finish(&client_final) else {
            return Err(Self::refuse(channel, &user).await);
        };

        // Checked after the proof, so that a role that exists but may not log
        // in fails at the same point as a wrong password.
        if !may_login {
            return Err(Self::refuse(channel, &user).await);
        }

        let mut message = vec![S_OK];
        message.extend_from_slice(server_final.as_bytes());
        channel.send(&message).await?;

        Ok(principal(user, superuser))
    }
}

impl ServerAuthenticator for RoleAuthenticator {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, Principal> {
        Box::pin(self.run(channel))
    }
}

/// Reads the user name, and the database when the hello names one, out of
/// the client's opening message.
fn parse_hello(message: &[u8]) -> Result<(String, Option<String>), RastcpError> {
    let malformed = || RastcpError::AuthenticationError("malformed hello".to_string());

    if message.len() < 4 || message[0] != C_HELLO {
        return Err(malformed());
    }
    let names_database = match message[1] {
        PROTOCOL_VERSION => false,
        PROTOCOL_VERSION_DATABASE => true,
        version => {
            return Err(RastcpError::AuthenticationError(format!(
                "unsupported authentication protocol version {}",
                version
            )));
        }
    };

    // One `[u16 len][UTF-8]` field starting at `at`, and where the next begins.
    let field = |at: usize, what: &str| -> Result<(String, usize), RastcpError> {
        let header = message.get(at..at + 2).ok_or_else(malformed)?;
        let len = u16::from_le_bytes([header[0], header[1]]) as usize;
        let end = (at + 2).checked_add(len).ok_or_else(malformed)?;
        let bytes = message.get(at + 2..end).ok_or_else(malformed)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| RastcpError::AuthenticationError(format!("{what} is not UTF-8")))?;
        Ok((text.to_string(), end))
    };

    let (user, end) = field(2, "user name")?;
    let (database, end) = if names_database {
        let (database, end) = field(end, "database name")?;
        if database.is_empty() {
            return Err(RastcpError::AuthenticationError(
                "the hello names an empty database".to_string(),
            ));
        }
        (Some(database), end)
    } else {
        (None, end)
    };
    if end != message.len() {
        return Err(malformed());
    }
    Ok((user, database))
}

/// Reads one tagged text message from the peer.
async fn read_text(channel: &mut AuthChannel<'_>, expected: u8) -> Result<String, RastcpError> {
    let message = channel.recv().await?;
    match message.split_first() {
        Some((tag, rest)) if *tag == expected => Ok(String::from_utf8_lossy(rest).to_string()),
        _ => Err(RastcpError::AuthenticationError(
            "unexpected authentication message".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hello_round_trips() {
        let mut hello = vec![C_HELLO, PROTOCOL_VERSION];
        hello.extend_from_slice(&(5u16).to_le_bytes());
        hello.extend_from_slice(b"alice");
        assert_eq!(parse_hello(&hello).unwrap(), ("alice".to_string(), None));
    }

    fn hello_v2(user: &[u8], database: &[u8]) -> Vec<u8> {
        let mut hello = vec![C_HELLO, PROTOCOL_VERSION_DATABASE];
        hello.extend_from_slice(&(user.len() as u16).to_le_bytes());
        hello.extend_from_slice(user);
        hello.extend_from_slice(&(database.len() as u16).to_le_bytes());
        hello.extend_from_slice(database);
        hello
    }

    #[test]
    fn a_hello_naming_a_database_round_trips() {
        assert_eq!(
            parse_hello(&hello_v2(b"alice", b"kopa")).unwrap(),
            ("alice".to_string(), Some("kopa".to_string()))
        );
        // Multibyte names are lengths in bytes, not characters.
        assert_eq!(
            parse_hello(&hello_v2("ålice".as_bytes(), "κόπα".as_bytes())).unwrap(),
            ("ålice".to_string(), Some("κόπα".to_string()))
        );
    }

    #[test]
    fn a_hello_naming_a_database_is_checked_to_its_last_byte() {
        // An empty name, a short field, trailing bytes and a cut length are
        // each refused, never read as the default database.
        assert!(parse_hello(&hello_v2(b"alice", b"")).is_err());
        let full = hello_v2(b"alice", b"kopa");
        for cut in 1..full.len() {
            assert!(parse_hello(&full[..cut]).is_err(), "cut at {cut}");
        }
        let mut long = full.clone();
        long.push(0);
        assert!(parse_hello(&long).is_err());
        let mut bad = hello_v2(b"alice", b"kopa");
        let at = bad.len() - 1;
        bad[at] = 0xFF;
        assert!(parse_hello(&bad).unwrap_err().to_string().contains("UTF-8"));
    }

    #[test]
    fn a_hello_whose_length_does_not_match_is_refused() {
        let mut hello = vec![C_HELLO, PROTOCOL_VERSION];
        hello.extend_from_slice(&(99u16).to_le_bytes());
        hello.extend_from_slice(b"alice");
        assert!(
            parse_hello(&hello).is_err(),
            "a claimed length must be checked"
        );

        assert!(parse_hello(&[]).is_err());
        assert!(parse_hello(&[C_HELLO]).is_err());
    }

    #[test]
    fn a_hello_from_a_future_version_is_named_as_such() {
        let mut hello = vec![C_HELLO, PROTOCOL_VERSION_DATABASE + 1];
        hello.extend_from_slice(&(0u16).to_le_bytes());
        let err = parse_hello(&hello).unwrap_err().to_string();
        assert!(err.contains("version"), "{}", err);
    }

    #[test]
    fn the_weak_methods_are_not_offered_here() {
        for method in [AuthMethod::Md5, AuthMethod::Password] {
            let config = NativeAuthConfig {
                method,
                ..Default::default()
            };
            assert!(config.validate().is_err(), "{:?} must be refused", method);
        }
        for method in [AuthMethod::Trust, AuthMethod::ScramSha256] {
            let config = NativeAuthConfig {
                method,
                ..Default::default()
            };
            assert!(config.validate().is_ok());
        }
    }

    /// Every refusal has to read the same, whatever went wrong.
    #[test]
    fn refusals_do_not_distinguish_their_cause() {
        assert_eq!(refusal("alice"), "authentication failed for user \"alice\"");
    }
}
