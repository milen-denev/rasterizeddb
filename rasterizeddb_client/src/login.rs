//! How a connection logs in.

use std::fmt;
use std::sync::Arc;

use rastcp::error::RastcpError;
use rastcp::session::{AuthChannel, AuthFuture, ClientAuthenticator};

/// Who a connection logs in as.
#[derive(Debug, Clone, Default)]
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
}

impl Login {
    pub(crate) fn authenticator(
        &self,
    ) -> Result<Option<Arc<dyn ClientAuthenticator>>, RastcpError> {
        let user = match self {
            Login::Trust => return Ok(None),
            Login::Peer => current_os_user().ok_or_else(|| {
                RastcpError::AuthenticationError(
                    "this process's user has no name in the password database".to_string(),
                )
            })?,
            Login::PeerAs(user) => user.clone(),
        };
        Ok(Some(
            Arc::new(PeerLogin { user }) as Arc<dyn ClientAuthenticator>
        ))
    }
}

// The exchange's opening message and the server's reply tags
// (`rasterizeddb_core::native_auth`).
const PROTOCOL_VERSION: u8 = 1;
const C_HELLO: u8 = 0x01;
const S_OK: u8 = 0x00;
const S_SASL: u8 = 0x01;

struct PeerLogin {
    user: String,
}

impl fmt::Debug for PeerLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerLogin")
            .field("user", &self.user)
            .finish()
    }
}

impl PeerLogin {
    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        let len = u16::try_from(self.user.len()).map_err(|_| {
            RastcpError::AuthenticationError("the user name is too long".to_string())
        })?;
        let mut hello = Vec::with_capacity(4 + self.user.len());
        hello.push(C_HELLO);
        hello.push(PROTOCOL_VERSION);
        hello.extend_from_slice(&len.to_le_bytes());
        hello.extend_from_slice(self.user.as_bytes());
        channel.send(&hello).await?;

        let reply = channel.recv().await?;
        match reply.split_first() {
            Some((&S_OK, _)) => Ok(()),
            Some((&S_SASL, _)) => Err(RastcpError::AuthenticationError(
                "the server asks for a password: peer login needs a unix socket under \
                 --rdb-socket-auth peer"
                    .to_string(),
            )),
            Some((_, message)) => Err(RastcpError::AuthenticationError(
                String::from_utf8_lossy(message).into_owned(),
            )),
            None => Err(RastcpError::AuthenticationError(
                "the server sent an empty authentication message".to_string(),
            )),
        }
    }
}

impl ClientAuthenticator for PeerLogin {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

/// The name of the operating-system user this process runs as.
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
