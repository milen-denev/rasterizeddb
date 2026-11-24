//! A pgwire session for a client that another protocol front end (MySQL,
//! SQL Server) has already authenticated, and the in-process client
//! ([`PgBridge`]) that front end drives it with.
//!
//! The front end speaks its own protocol to its client and the PostgreSQL
//! protocol to this session, over an in-memory pipe. Everything a session
//! is — transaction blocks and their rollback when the connection goes,
//! savepoints, isolation levels, prepared statements and their describe,
//! cancellation, idle timeouts, the privilege gates, routing by database —
//! is the one pgwire already runs, rather than a second copy of it per
//! protocol. What a front end owns is its wire format, its dialect, and its
//! authentication handshake; the [`Principal`] it proved is handed in here.
//!
//! The pipe costs a copy of each message each way, which is small next to
//! running the statement, and buys a session whose behaviour cannot drift
//! from PostgreSQL's.

use super::*;

/// Who a front end authenticated: the session's principal.
#[derive(Clone, Debug)]
pub(crate) struct ProxiedLogin {
    pub(crate) user: String,
    pub(crate) superuser: bool,
    /// Whether the name was proven with a credential. False under `trust`,
    /// as for a pgwire `trust` login: see [`Principal`].
    pub(crate) authenticated: bool,
    /// The database the login was checked against: a session in any other
    /// is checked again there ([`login_for`]), since a role of the same name
    /// in another database is another account.
    pub(crate) database: String,
    /// The role's verifier as stored when it logged in (`Verifier::to_stored`),
    /// for an authenticated login: the credential it proved. Another
    /// database admits the login only where its role holds this same one.
    pub(crate) verifier: Option<String>,
    /// The bootstrap role a `trust` login is checked with
    /// (`auth::trust_login`) in another database.
    pub(crate) bootstrap_user: Option<String>,
}

/// `login`, as it stands in database `name` (`database`): the same login
/// when it is the one it logged in to; otherwise checked again against that
/// database's roles, with that role's attributes (its `superuser`, not the
/// first database's). An authenticated login needs the role there to hold
/// the very credential it proved, which a role copied by `CREATE DATABASE`
/// does; a `trust` login is what `auth::trust_login` makes it there. Refused
/// otherwise: MySQL's `USE` and SQL Server's `USE` used to carry a login
/// into any database, as whatever role held its name there.
pub(crate) async fn login_for(database: &Arc<Database>, name: &str, login: &ProxiedLogin) -> Result<ProxiedLogin, String> {
    if login.database == name {
        return Ok(login.clone());
    }
    let superuser = if login.authenticated {
        let role = crate::auth::lookup_role(database, &login.user).await;
        let same_credential = role.as_ref().is_some_and(|r| {
            r.can_login && login.verifier.is_some() && r.verifier.as_ref().map(|v| v.to_stored()) == login.verifier
        });
        if !same_credential {
            return Err(format!(
                "role \"{}\" cannot use database \"{name}\" without logging in to it: its account there is not \
                 the one this connection proved; connect to \"{name}\" instead",
                login.user
            ));
        }
        role.is_some_and(|r| r.is_superuser)
    } else {
        crate::auth::trust_login(database, &login.user, login.bootstrap_user.as_deref()).await?
    };
    Ok(ProxiedLogin { superuser, database: name.to_string(), ..login.clone() })
}

/// A watch on a front end's client socket, for a statement to notice the
/// client has gone (see [`PeerWatch`]). Best-effort: `None` when the
/// descriptor cannot be duplicated.
#[cfg(unix)]
pub(crate) fn peer_watch_of(fd: std::os::fd::BorrowedFd<'_>) -> Option<StdArc<PeerWatch>> {
    fd.try_clone_to_owned()
        .ok()
        .map(|copy| StdArc::new(PeerWatch::new(copy)))
}

/// Runs the session on the server end of the pipe until the client end is
/// closed or the session ends itself (an idle timeout, shutdown).
async fn serve_proxied<S>(
    catalog: Arc<DatabaseCatalog>,
    guard: crate::core::database::catalog::SessionGuard,
    login: ProxiedLogin,
    limits: ConnectionLimits,
    stream: S,
    parameters: HashMap<String, String>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Held for the life of the session: `DROP DATABASE` counts what is
    // still attached before it removes anything.
    let database = guard.database().clone();
    let mut stream = stream;
    let watch = PEER_WATCH.try_with(|w| w.clone()).ok().flatten();
    let mut conn = wire_out::WireStream::new(&mut stream, watch);

    let mut key = [0u8; 4];
    crate::auth::fill_random(&mut key);
    let registration = crate::core::cancel::register_session(key);
    // `authenticate` sends AuthenticationOk for a pgwire client; the front
    // end reads the same sequence, so the bridge's startup has one shape.
    send_auth_ok(&mut conn).await?;
    send_startup_responses(&mut conn, registration.pid, registration.key).await?;
    let startup = (
        startup_statement_timeout(&parameters),
        startup_lock_timeout(&parameters),
        startup_shape_change(&parameters),
    );
    let principal = Principal {
        name: login.user,
        superuser: login.superuser,
        authenticated: login.authenticated,
    };
    // The one kind of session that takes a front end's catalogue statements
    // (`SYSTEM_STATEMENT_TAG`).
    let result = PROXIED_SESSION
        .scope(
            (),
            SESSION_CANCEL.scope(
            registration.session.clone(),
            run_message_loop(
                catalog,
                &database,
                AuthConfig::default(),
                principal,
                limits,
                &mut conn,
                startup,
                registration.pid,
            ),
            ),
        )
        .await;
    let _ = conn.flush().await;
    drop(guard);
    result
}

/// What a front end asked for when it opened a session.
pub(crate) struct BridgeOptions {
    pub(crate) login: ProxiedLogin,
    pub(crate) limits: ConnectionLimits,
    pub(crate) watch: Option<StdArc<PeerWatch>>,
    /// StartupMessage parameters (`statement_timeout`, …).
    pub(crate) parameters: HashMap<String, String>,
    /// The front end's own leniency over booleans: MySQL's `SUM(active)`.
    /// See `aggregate::MYSQL_BOOLEAN_AGGREGATES`.
    pub(crate) lenient_booleans: bool,
}

/// Why a session could not be opened.
#[derive(Debug)]
pub(crate) enum OpenRefused {
    /// No database by that name.
    NoDatabase,
    /// The database is being imported.
    Loading,
    /// The login is not admitted to the database ([`login_for`]).
    Denied(String),
    Io(io::Error),
}

/// One backend message, borrowed from the bridge's read buffer.
pub(crate) struct Frame<'a> {
    pub(crate) tag: u8,
    pub(crate) body: &'a [u8],
}

/// An error or notice's fields.
#[derive(Clone, Debug, Default)]
pub(crate) struct PgNotice {
    pub(crate) severity: String,
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
}

impl PgNotice {
    pub(crate) fn parse(body: &[u8]) -> Self {
        let mut out = PgNotice::default();
        let mut p = 0usize;
        while p < body.len() && body[p] != 0 {
            let field = body[p];
            p += 1;
            let end = body[p..]
                .iter()
                .position(|b| *b == 0)
                .map(|e| p + e)
                .unwrap_or(body.len());
            let value = String::from_utf8_lossy(&body[p..end]).into_owned();
            p = end + 1;
            match field {
                b'S' | b'V' if out.severity.is_empty() => out.severity = value,
                b'C' => out.code = value,
                b'M' => out.message = value,
                b'D' => out.detail = Some(value),
                b'H' => out.hint = Some(value),
                _ => {}
            }
        }
        out
    }
}

/// One column of a RowDescription.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct PgField {
    pub(crate) name: String,
    pub(crate) table_oid: u32,
    pub(crate) column: i16,
    pub(crate) type_oid: u32,
    pub(crate) type_size: i16,
    pub(crate) type_modifier: i32,
}

/// Parses a RowDescription body.
pub(crate) fn parse_row_description(body: &[u8]) -> io::Result<Vec<PgField>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed RowDescription");
    let count = i16::from_be_bytes(body.get(0..2).ok_or_else(bad)?.try_into().unwrap());
    let mut p = 2usize;
    let mut fields = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let end = body
            .get(p..)
            .ok_or_else(bad)?
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(bad)?
            + p;
        let name = String::from_utf8_lossy(&body[p..end]).into_owned();
        p = end + 1;
        let fixed = body.get(p..p + 18).ok_or_else(bad)?;
        fields.push(PgField {
            name,
            table_oid: u32::from_be_bytes(fixed[0..4].try_into().unwrap()),
            column: i16::from_be_bytes(fixed[4..6].try_into().unwrap()),
            type_oid: u32::from_be_bytes(fixed[6..10].try_into().unwrap()),
            type_size: i16::from_be_bytes(fixed[10..12].try_into().unwrap()),
            type_modifier: i32::from_be_bytes(fixed[12..16].try_into().unwrap()),
        });
        p += 18;
    }
    Ok(fields)
}

/// The cells of a DataRow body, `None` for NULL.
pub(crate) fn data_row_cells(body: &[u8]) -> io::Result<smallvec::SmallVec<[Option<&[u8]>; 16]>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed DataRow");
    let count = i16::from_be_bytes(body.get(0..2).ok_or_else(bad)?.try_into().unwrap());
    let mut p = 2usize;
    let mut cells = smallvec::SmallVec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let len = i32::from_be_bytes(body.get(p..p + 4).ok_or_else(bad)?.try_into().unwrap());
        p += 4;
        if len < 0 {
            cells.push(None);
        } else {
            let end = p + len as usize;
            cells.push(Some(body.get(p..end).ok_or_else(bad)?));
            p = end;
        }
    }
    Ok(cells)
}

/// The rows count at the end of a command tag (`INSERT 0 3`, `UPDATE 2`).
pub(crate) fn tag_count(tag: &str) -> Option<u64> {
    let verb = tag.split(' ').next()?;
    match verb {
        "INSERT" | "UPDATE" | "DELETE" | "SELECT" | "MERGE" | "COPY" | "FETCH" | "MOVE" => {
            tag.rsplit(' ').next()?.parse().ok()
        }
        _ => None,
    }
}

/// The client end of an in-process session.
pub(crate) struct PgBridge {
    io: tokio::io::DuplexStream,
    out: Vec<u8>,
    rbuf: Vec<u8>,
    rpos: usize,
    /// The session's cancel target: a front end's own cancel (MySQL's
    /// `KILL QUERY`, a TDS attention) is a `CancelRequest` for it.
    pub(crate) pid: i32,
    pub(crate) key: i32,
    /// The status byte of the last ReadyForQuery: `I` idle, `T` in a block,
    /// `E` in a failed block.
    pub(crate) status: u8,
    /// The database the session is attached to.
    pub(crate) database: Arc<Database>,
    pub(crate) database_name: String,
    /// The login as this database admits it ([`login_for`]): what the front
    /// end's own checks (KILL, SHOW GRANTS) read after a `USE`.
    pub(crate) login: ProxiedLogin,
}

const BRIDGE_PIPE: usize = 256 * 1024;

impl PgBridge {
    /// Opens a session on `database_name` for an authenticated client, and
    /// reads its startup through the first ReadyForQuery.
    pub(crate) async fn open(
        catalog: Arc<DatabaseCatalog>,
        database_name: &str,
        options: BridgeOptions,
    ) -> Result<Self, OpenRefused> {
        if catalog.is_loading(database_name) {
            return Err(OpenRefused::Loading);
        }
        let Some(guard) = catalog.connect(database_name) else {
            return Err(OpenRefused::NoDatabase);
        };
        let database = guard.database().clone();
        let BridgeOptions {
            login,
            limits,
            watch,
            parameters,
            lenient_booleans,
        } = options;
        // A database other than the one the login was checked against
        // checks it again, before any session exists there.
        let login = login_for(&database, database_name, &login).await.map_err(OpenRefused::Denied)?;
        let effective = login.clone();
        let (client, server) = tokio::io::duplex(BRIDGE_PIPE);
        let session = async move {
            if let Err(e) = serve_proxied(catalog, guard, login, limits, server, parameters).await {
                log::debug!("proxied session ended: {e}");
            }
        };
        if lenient_booleans {
            tokio::spawn(PEER_WATCH.scope(
                watch,
                crate::core::rql::aggregate::MYSQL_BOOLEAN_AGGREGATES.scope((), session),
            ));
        } else {
            tokio::spawn(PEER_WATCH.scope(watch, session));
        }
        let mut bridge = PgBridge {
            io: client,
            out: Vec::with_capacity(1024),
            rbuf: Vec::with_capacity(8 * 1024),
            rpos: 0,
            pid: 0,
            key: 0,
            status: b'I',
            database,
            database_name: database_name.to_string(),
            login: effective,
        };
        loop {
            let frame = bridge.next().await.map_err(OpenRefused::Io)?;
            match frame.tag {
                b'K' if frame.body.len() >= 8 => {
                    let pid = i32::from_be_bytes(frame.body[0..4].try_into().unwrap());
                    let key = i32::from_be_bytes(frame.body[4..8].try_into().unwrap());
                    bridge.pid = pid;
                    bridge.key = key;
                }
                b'Z' => break,
                b'E' => {
                    let notice = PgNotice::parse(frame.body);
                    return Err(OpenRefused::Io(io::Error::other(notice.message)));
                }
                _ => {}
            }
        }
        Ok(bridge)
    }

    fn message(&mut self, tag: u8, body: impl FnOnce(&mut Vec<u8>)) {
        self.out.push(tag);
        let at = self.out.len();
        self.out.extend_from_slice(&[0, 0, 0, 0]);
        body(&mut self.out);
        let len = (self.out.len() - at) as i32;
        self.out[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }

    fn cstr(out: &mut Vec<u8>, text: &str) {
        out.extend_from_slice(text.as_bytes());
        out.push(0);
    }

    /// Queues a simple Query.
    pub(crate) fn query(&mut self, sql: &str) {
        self.message(b'Q', |out| Self::cstr(out, sql));
    }

    /// Queues a simple Query that is the front end's own statement on its
    /// catalogue (`__rdb_routines`, `__rdb_mysql_on_update`), announced by
    /// [`SYSTEM_STATEMENT_TAG`] so the session's catalogue guard lets it
    /// through. Never for text a client sent: the guard is what keeps a
    /// client off every catalogue.
    pub(crate) fn system_query(&mut self, sql: &str) {
        self.message(SYSTEM_STATEMENT_TAG, |_| {});
        self.query(sql);
    }

    /// Queues a Parse.
    pub(crate) fn parse(&mut self, name: &str, sql: &str, param_oids: &[u32]) {
        self.message(b'P', |out| {
            Self::cstr(out, name);
            Self::cstr(out, sql);
            out.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
            for oid in param_oids {
                out.extend_from_slice(&oid.to_be_bytes());
            }
        });
    }

    /// Queues a Bind. `params` are `(format, value)`; a format of 1 is binary.
    pub(crate) fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[(i16, Option<&[u8]>)],
        result_formats: &[i16],
    ) {
        self.message(b'B', |out| {
            Self::cstr(out, portal);
            Self::cstr(out, statement);
            out.extend_from_slice(&(params.len() as i16).to_be_bytes());
            for (format, _) in params {
                out.extend_from_slice(&format.to_be_bytes());
            }
            out.extend_from_slice(&(params.len() as i16).to_be_bytes());
            for (_, value) in params {
                match value {
                    None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                    Some(bytes) => {
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(bytes);
                    }
                }
            }
            out.extend_from_slice(&(result_formats.len() as i16).to_be_bytes());
            for format in result_formats {
                out.extend_from_slice(&format.to_be_bytes());
            }
        });
    }

    /// Queues a Describe of a prepared statement (`S`) or a portal (`P`).
    pub(crate) fn describe(&mut self, kind: u8, name: &str) {
        self.message(b'D', |out| {
            out.push(kind);
            Self::cstr(out, name);
        });
    }

    /// Queues an Execute of a portal, `max_rows` 0 for all.
    pub(crate) fn execute(&mut self, portal: &str, max_rows: i32) {
        self.message(b'E', |out| {
            Self::cstr(out, portal);
            out.extend_from_slice(&max_rows.to_be_bytes());
        });
    }

    /// Queues a Close of a prepared statement (`S`) or a portal (`P`).
    pub(crate) fn close(&mut self, kind: u8, name: &str) {
        self.message(b'C', |out| {
            out.push(kind);
            Self::cstr(out, name);
        });
    }

    /// Queues a Sync.
    pub(crate) fn sync(&mut self) {
        self.message(b'S', |_| {});
    }

    /// Queues frontend messages exactly as a client sent them: a gateway's
    /// virtual session relays its client's bytes (`core::shard::rshard`).
    /// The caller has checked they are whole messages.
    #[cfg(feature = "sharding")]
    pub(crate) fn raw(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// Sends what is queued.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            self.io.write_all(&self.out).await?;
            self.out.clear();
            if self.out.capacity() > 64 * 1024 {
                self.out = Vec::with_capacity(1024);
            }
        }
        Ok(())
    }

    /// Reads the next backend message, flushing what is queued first. A
    /// ReadyForQuery updates [`PgBridge::status`] as it passes.
    pub(crate) async fn next(&mut self) -> io::Result<Frame<'_>> {
        self.flush().await?;
        // Drop what earlier frames used.
        if self.rpos > 0 && self.rpos == self.rbuf.len() {
            self.rbuf.clear();
            self.rpos = 0;
            if self.rbuf.capacity() > 1024 * 1024 {
                self.rbuf = Vec::with_capacity(8 * 1024);
            }
        } else if self.rpos > 64 * 1024 {
            self.rbuf.drain(..self.rpos);
            self.rpos = 0;
        }
        loop {
            let avail = self.rbuf.len() - self.rpos;
            if avail >= 5 {
                let len =
                    i32::from_be_bytes(self.rbuf[self.rpos + 1..self.rpos + 5].try_into().unwrap());
                if len < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bad backend frame",
                    ));
                }
                let total = 1 + len as usize;
                if avail >= total {
                    let start = self.rpos;
                    self.rpos += total;
                    let tag = self.rbuf[start];
                    if tag == b'Z' && total >= 6 {
                        self.status = self.rbuf[start + 5];
                    }
                    return Ok(Frame {
                        tag,
                        body: &self.rbuf[start + 5..start + total],
                    });
                }
                self.rbuf.reserve(total - avail);
            }
            let old = self.rbuf.len();
            let want = (self.rbuf.capacity() - old).max(8 * 1024);
            self.rbuf.resize(old + want, 0);
            let n = self.io.read(&mut self.rbuf[old..]).await;
            match n {
                Ok(0) => {
                    self.rbuf.truncate(old);
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the session ended",
                    ));
                }
                Ok(n) => self.rbuf.truncate(old + n),
                Err(e) => {
                    self.rbuf.truncate(old);
                    return Err(e);
                }
            }
        }
    }

    /// Whether a transaction block is open (or failed and not yet closed).
    pub(crate) fn in_block(&self) -> bool {
        self.status != b'I'
    }

    /// Ends the session: Terminate, then the pipe is closed.
    pub(crate) async fn terminate(mut self) {
        self.message(b'X', |_| {});
        let _ = self.flush().await;
        let _ = self.io.shutdown().await;
    }
}

/// A TLS acceptor for a front end's listener: the certificate and key given,
/// or a self-signed pair made now, as pgwire's.
pub(crate) fn tls_acceptor(
    cert_path: Option<String>,
    key_path: Option<String>,
) -> io::Result<TlsAcceptor> {
    build_tls_acceptor(&TlsConfig {
        mode: TlsMode::Require,
        cert_path,
        key_path,
    })
}

/// [`tls_acceptor`], offering `alpn` (TDS 8.0 clients ask for `tds/8.0`).
/// The protocols are offered, not required: a client that names none is
/// served all the same.
pub(crate) fn tls_acceptor_with_alpn(
    cert_path: Option<String>,
    key_path: Option<String>,
    alpn: Vec<Vec<u8>>,
) -> io::Result<TlsAcceptor> {
    ensure_crypto_provider();
    let (certs, key) = match (&cert_path, &key_path) {
        (Some(cert_path), Some(key_path)) => load_tls_from_pem(cert_path, key_path)?,
        (None, None) => rastcp::cert::generate_self_signed_cert()
            .map_err(|e| io::Error::other(e.to_string()))?,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS needs both a certificate and a key, or neither",
            ));
        }
    };
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    config.alpn_protocols = alpn;
    // No TLS 1.3 tickets: TDS 7.x wraps the handshake in its own packets and
    // stops wrapping when the handshake ends, so a ticket written after that
    // point would reach a client expecting the other framing.
    config.send_tls13_tickets = 0;
    Ok(TlsAcceptor::from(StdArc::new(config)))
}
