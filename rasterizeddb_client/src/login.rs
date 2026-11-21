//! How a connection logs in.

use std::fmt;
use std::sync::Arc;

use rastcp::error::RastcpError;
use rastcp::session::{AuthChannel, AuthFuture, ClientAuthenticator, ConnectionSetup};

/// Who a connection logs in as.
///
/// `Debug` never prints a password.
#[derive(Clone, Default)]
pub enum Login {
    /// No exchange at all: for a server that asks for none, such as a unix
    /// socket under `--rdb-socket-auth trust`. Against a server that does ask,
    /// the connection is refused.
    #[default]
    Trust,
    /// As the operating-system user this process runs as, with no password,
    /// on a unix socket under `--rdb-socket-auth peer`. A role of that name
    /// must exist and be allowed to log in.
    Peer,
    /// As `user`, by peer authentication. The server accepts it only when
    /// `user` is the name of the operating-system user connecting.
    PeerAs(String),
    /// As `user` with `password`, by SCRAM-SHA-256 bound to the connection
    /// (`tls-exporter` over TCP, rastcp's own binding on a unix socket). The
    /// password never crosses the wire, and the server must prove it holds
    /// the user's verifier: a server that lets the connection in without the
    /// exchange (one run under `trust`) is refused at once, because it never
    /// proved it is the server meant; the error says to use [`Login::Trust`]
    /// for such a server.
    Password {
        /// The role to log in as.
        user: String,
        /// Its password.
        password: String,
    },
}

impl Login {
    /// [`Login::Password`] for `user`.
    pub fn password(user: impl Into<String>, password: impl Into<String>) -> Self {
        Login::Password {
            user: user.into(),
            password: password.into(),
        }
    }
}

impl fmt::Debug for Login {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Login::Trust => f.write_str("Trust"),
            Login::Peer => f.write_str("Peer"),
            Login::PeerAs(user) => f.debug_tuple("PeerAs").field(user).finish(),
            Login::Password { user, .. } => f
                .debug_struct("Password")
                .field("user", user)
                .finish_non_exhaustive(),
        }
    }
}

impl Login {
    /// The exchange this login runs, naming `database` in its hello when one
    /// is given. `None` for [`Login::Trust`], which runs none: its database
    /// goes in [`database_setup`] instead.
    pub(crate) fn authenticator(
        &self,
        database: Option<&str>,
    ) -> Result<Option<Arc<dyn ClientAuthenticator>>, RastcpError> {
        let database = database.map(str::to_string);
        let user = match self {
            Login::Trust => return Ok(None),
            Login::Password { user, password } => {
                return Ok(Some(Arc::new(PasswordLogin {
                    user: user.clone(),
                    password: password.clone(),
                    database,
                }) as Arc<dyn ClientAuthenticator>));
            }
            Login::Peer => current_os_user().ok_or_else(|| {
                RastcpError::AuthenticationError(NO_OS_USER.to_string())
            })?,
            Login::PeerAs(user) => user.clone(),
        };
        Ok(Some(
            Arc::new(PeerLogin { user, database }) as Arc<dyn ClientAuthenticator>
        ))
    }

    /// What selects `database` on a connection this login opens, when the
    /// login runs no exchange to name it in: a [`Login::Trust`] connection
    /// sends it as its first request. `None` for the other logins and for
    /// no database.
    pub(crate) fn database_setup(
        &self,
        database: Option<&str>,
    ) -> Option<Arc<dyn ConnectionSetup>> {
        match (self, database) {
            (Login::Trust, Some(name)) => Some(Arc::new(DatabaseSetup {
                name: name.to_string(),
            }) as Arc<dyn ConnectionSetup>),
            _ => None,
        }
    }
}

/// What every error saying a server may predate database selection holds
/// ([`crate::Error::may_predate_database_selection`]).
pub(crate) const PREDATES_DATABASE_SELECTION: &str = "predate database selection";

/// The server's `pgwire::native_session::DATABASE_PREFIX`.
const DATABASE_PREFIX: &[u8] = b"\x01DATABASE;";

/// Selects a database on a connection that logs in with no exchange: the
/// connection's first request, answered in the simple-query form.
#[derive(Debug)]
struct DatabaseSetup {
    name: String,
}

impl ConnectionSetup for DatabaseSetup {
    fn request(&self) -> Vec<u8> {
        let mut request = DATABASE_PREFIX.to_vec();
        request.extend_from_slice(self.name.as_bytes());
        request
    }

    fn accept(&self, answer: &[u8]) -> Result<(), RastcpError> {
        let predates = |detail: &str| {
            RastcpError::AuthenticationError(format!(
                "this server cannot select database \"{}\" over the native protocol (it seems \
                 to {PREDATES_DATABASE_SELECTION}), so the connection was not used: {detail}",
                self.name
            ))
        };
        match crate::simple::decode(answer.to_vec()) {
            Ok(_) => Ok(()),
            // Two statements to a server older than the simple-query form.
            Err(crate::Error::Unsupported(detail)) => Err(predates(&detail)),
            Err(e) if e.sqlstate() == Some("0A000") => Err(predates(&e.to_string())),
            Err(e) => Err(RastcpError::AuthenticationError(match e {
                crate::Error::Server(message) => message,
                other => other.to_string(),
            })),
        }
    }
}

// The exchange's opening message and the server's reply tags
// (`rasterizeddb_core::native_auth`).
const PROTOCOL_VERSION: u8 = 1;
/// The hello that names a database too; sent only when one is named, so a
/// server older than it refuses the login instead of using its default.
const PROTOCOL_VERSION_DATABASE: u8 = 2;
const C_HELLO: u8 = 0x01;
const C_SASL_INITIAL: u8 = 0x02;
const C_SASL_FINAL: u8 = 0x03;
const S_OK: u8 = 0x00;
const S_SASL: u8 = 0x01;
const S_CONTINUE: u8 = 0x02;
const S_ERROR: u8 = 0xFF;

/// A server that runs no login exchange (a unix socket under
/// `--rdb-socket-auth trust`) takes the hello for a request: a multiplexed
/// connection is closed at once, a plain one answers it as a request. Either
/// way the login failed for a reason the caller can act on, said as such
/// rather than as a closed connection or the bytes of a query's answer.
fn no_exchange_here(what_happened: &str) -> RastcpError {
    RastcpError::AuthenticationError(format!(
        "the server did not take part in the login ({what_happened}): it most likely asks for \
         none, as a unix socket under --rdb-socket-auth trust does; connect with Login::Trust \
         if that server is meant"
    ))
}

/// The server's answer to the hello. `database` is the one it named.
async fn hello_reply(
    channel: &mut AuthChannel<'_>,
    database: Option<&str>,
) -> Result<(u8, String), RastcpError> {
    match server_reply(channel).await {
        // A server that predates the hello naming a database closes the
        // connection at it without a word, as one running no exchange can.
        Err(RastcpError::ConnectionClosed) if database.is_some() => {
            Err(RastcpError::AuthenticationError(format!(
                "the server closed the connection at the login, which named database \"{}\": \
                 it may {PREDATES_DATABASE_SELECTION} over the native protocol, or ask for no \
                 login at all (a unix socket under --rdb-socket-auth trust; connect with \
                 Login::Trust then)",
                database.unwrap_or_default()
            )))
        }
        Err(RastcpError::ConnectionClosed) => Err(no_exchange_here("it closed the connection")),
        Ok((tag, _)) if !matches!(tag, S_OK | S_SASL | S_ERROR) => {
            Err(no_exchange_here(&format!("it answered with a message of kind {tag}")))
        }
        other => other,
    }
}

/// The opening message: who the caller wants to be, and in which database
/// when it names one.
fn hello(user: &str, database: Option<&str>) -> Result<Vec<u8>, RastcpError> {
    let field = |text: &str, what: &str| {
        u16::try_from(text.len())
            .map(|len| len.to_le_bytes())
            .map_err(|_| RastcpError::AuthenticationError(format!("the {what} is too long")))
    };
    let mut hello = Vec::with_capacity(6 + user.len() + database.map_or(0, str::len));
    hello.push(C_HELLO);
    hello.push(if database.is_some() { PROTOCOL_VERSION_DATABASE } else { PROTOCOL_VERSION });
    hello.extend_from_slice(&field(user, "user name")?);
    hello.extend_from_slice(user.as_bytes());
    if let Some(database) = database {
        hello.extend_from_slice(&field(database, "database name")?);
        hello.extend_from_slice(database.as_bytes());
    }
    Ok(hello)
}

/// One tagged message from the server: its tag, and the rest as text.
async fn server_reply(channel: &mut AuthChannel<'_>) -> Result<(u8, String), RastcpError> {
    let message = channel.recv().await?;
    match message.split_first() {
        Some((tag, rest)) => Ok((*tag, String::from_utf8_lossy(rest).into_owned())),
        None => Err(RastcpError::AuthenticationError(
            "the server sent an empty authentication message".to_string(),
        )),
    }
}

struct PasswordLogin {
    user: String,
    password: String,
    database: Option<String>,
}

impl fmt::Debug for PasswordLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PasswordLogin")
            .field("user", &self.user)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl PasswordLogin {
    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        let database = self.database.as_deref();
        channel.send(&hello(&self.user, database)?).await?;
        match hello_reply(channel, database).await? {
            (S_SASL, _) => {}
            (S_OK, _) => {
                return Err(RastcpError::AuthenticationError(
                    "the server let the connection in without a password exchange, so it never \
                     proved who it is; log in with Login::Trust if that server is meant"
                        .to_string(),
                ));
            }
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        }

        let (mut scram, client_first) =
            crate::scram::ScramClient::start(&self.user, &self.password, channel.channel_binding())
                .map_err(|e| {
                    RastcpError::AuthenticationError(format!("no random bytes for a nonce: {e}"))
                })?;
        let mut message = vec![C_SASL_INITIAL];
        message.extend_from_slice(client_first.as_bytes());
        channel.send(&message).await?;

        let server_first = match server_reply(channel).await? {
            (S_CONTINUE, payload) => payload,
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        };
        let client_final = scram
            .respond(&server_first)
            .map_err(RastcpError::AuthenticationError)?;
        let mut message = vec![C_SASL_FINAL];
        message.extend_from_slice(client_final.as_bytes());
        channel.send(&message).await?;

        let server_final = match server_reply(channel).await? {
            (S_OK, payload) => payload,
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        };
        // Not optional: without it this side has proved itself to whoever
        // answered.
        scram
            .verify(&server_final)
            .map_err(RastcpError::AuthenticationError)
    }
}

impl ClientAuthenticator for PasswordLogin {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

struct PeerLogin {
    user: String,
    database: Option<String>,
}

impl fmt::Debug for PeerLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerLogin")
            .field("user", &self.user)
            .field("database", &self.database)
            .finish()
    }
}

impl PeerLogin {
    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        let database = self.database.as_deref();
        channel.send(&hello(&self.user, database)?).await?;
        match hello_reply(channel, database).await? {
            (S_OK, _) => Ok(()),
            (S_SASL, _) => Err(RastcpError::AuthenticationError(
                "the server asks for a password: peer login needs a unix socket under \
                 --rdb-socket-auth peer"
                    .to_string(),
            )),
            (_, message) => Err(RastcpError::AuthenticationError(message)),
        }
    }
}

impl ClientAuthenticator for PeerLogin {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

#[cfg(unix)]
const NO_OS_USER: &str = "this process's user has no name in the password database";
#[cfg(not(unix))]
const NO_OS_USER: &str =
    "peer login needs a unix socket, which this platform does not have; log in with a password";

/// The name of the operating-system user this process runs as. Only a unix
/// socket carries the peer's credentials, so elsewhere there is none to give.
#[cfg(not(unix))]
pub(crate) fn current_os_user() -> Option<String> {
    None
}

/// The name of the operating-system user this process runs as.
#[cfg(unix)]
pub(crate) fn current_os_user() -> Option<String> {
    // SAFETY: getuid cannot fail and has no preconditions.
    let uid = unsafe { libc::getuid() };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hello_naming_no_database_is_the_one_every_server_reads() {
        // Byte for byte what clients sent before database selection, so a
        // client that names none still logs in to an older server.
        assert_eq!(hello("alice", None).unwrap(), b"\x01\x01\x05\x00alice");
    }

    #[test]
    fn a_hello_naming_a_database_says_so_in_its_version() {
        assert_eq!(
            hello("alice", Some("kopa")).unwrap(),
            b"\x01\x02\x05\x00alice\x04\x00kopa"
        );
        // Lengths are bytes.
        let multibyte = hello("a", Some("κ")).unwrap();
        assert_eq!(&multibyte[5..7], &[2, 0]);
        assert!(hello("a", Some(&"x".repeat(70_000))).is_err());
    }

    fn simple(messages: &[(u8, &[u8])]) -> Vec<u8> {
        let mut out = vec![4u8];
        for (tag, body) in messages {
            out.push(*tag);
            out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            out.extend_from_slice(body);
        }
        out
    }

    fn error_body(code: &str, message: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (field, value) in [(b'S', "ERROR"), (b'C', code), (b'M', message)] {
            body.push(field);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        body
    }

    #[test]
    fn the_setup_answer_decides_the_connection() {
        let setup = DatabaseSetup { name: "kopa".to_string() };
        assert_eq!(setup.request(), b"\x01DATABASE;kopa");

        let ok = simple(&[(b'C', b"SET\0"), (b'Z', b"I")]);
        assert!(setup.accept(&ok).is_ok());

        // Refused by the server that knows the request: its reason, with its
        // SQLSTATE, and not "too old".
        let missing = simple(&[
            (b'E', &error_body("3D000", "database \"kopa\" does not exist")),
            (b'Z', b"I"),
        ]);
        let e = crate::Error::Transport(setup.accept(&missing).unwrap_err());
        assert!(e.to_string().contains("3D000"), "{e}");
        assert!(!e.may_predate_database_selection(), "{e}");

        // A server with marked requests but not this one, and one older still
        // that refuses the text as two statements: both "too old", neither
        // ever the default database.
        let unknown = simple(&[
            (b'E', &error_body("0A000", "this server does not know the marked request")),
            (b'Z', b"I"),
        ]);
        let e = crate::Error::Transport(setup.accept(&unknown).unwrap_err());
        assert!(e.may_predate_database_selection(), "{e}");
        let mut two_statements = vec![3u8];
        two_statements.extend_from_slice(b"the native protocol runs one statement per request");
        let e = crate::Error::Transport(setup.accept(&two_statements).unwrap_err());
        assert!(e.may_predate_database_selection(), "{e}");
    }
}
