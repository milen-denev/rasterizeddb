//! A minimal PostgreSQL *client*: enough of the version 3 frontend protocol to
//! log in, ask questions of the system catalogues, and stream a table out.
//!
//! Hand-rolled for the same reason the S3 client is (`core::backup::s3`):
//! `tokio-postgres` would bring a dependency tree an order of magnitude larger
//! than the code it replaces, and everything hard here already exists in the
//! tree. The SCRAM exchange is [`crate::auth::ScramClient`] — the very code the
//! pgwire *listener* is the other side of — the transport is the
//! `rustls`/`tokio-rustls` that listener needs anyway, and MD5 comes from the
//! `md-5` this crate already carries for old clients.
//!
//! What is *not* here is as deliberate as what is: no extended query protocol,
//! no prepared statements, no pipelining, no notification handling, no
//! `COPY FROM`. A migration asks a handful of catalogue questions and then
//! streams tables, and every one of those is a simple query.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::auth::{ChannelBinding, ScramClient};

/// Protocol version 3.0, as the two 16-bit halves of one `i32`.
const PROTOCOL_VERSION: i32 = 196_608;
/// The magic number that asks a server to start TLS. Not a version: it is
/// deliberately outside the range any version can take.
const SSL_REQUEST_CODE: i32 = 80_877_103;

/// Read buffer. PostgreSQL emits one `CopyData` message per row in text format,
/// so an unbuffered reader would cost two syscalls a row; at 64 KiB a wide row
/// still fits and a narrow one is amortised a few hundred to one.
const READ_BUFFER: usize = 64 * 1024;

/// A ceiling on one protocol message, so a corrupt or hostile length prefix
/// cannot be turned into an allocation. Comfortably above any catalogue row or
/// `CopyData` chunk PostgreSQL emits.
const MAX_MESSAGE: usize = 256 * 1024 * 1024;

/// How a connection is protected, using libpq's names and libpq's meanings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Never negotiate TLS.
    Disable,
    /// Ask for TLS; carry on in cleartext if the server declines.
    #[default]
    Prefer,
    /// Demand TLS, but do not check who the server is.
    ///
    /// This is libpq's meaning of `require` and it is worth being explicit
    /// about: it stops a passive eavesdropper and nothing else. Anyone able to
    /// intercept the connection can present their own certificate and read the
    /// password and every row. Use `verify-full` across an untrusted network.
    Require,
    /// Demand TLS, and check the certificate chains to a public root and names
    /// the host that was dialled.
    VerifyFull,
}

impl SslMode {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "disable" => Some(SslMode::Disable),
            "allow" | "prefer" => Some(SslMode::Prefer),
            "require" => Some(SslMode::Require),
            // `verify-ca` is accepted as `verify-full`: this client has no way
            // to check a chain without also checking the name, and quietly
            // doing less than was asked for is the wrong direction to round in.
            "verify-ca" | "verify-full" => Some(SslMode::VerifyFull),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

/// Where to connect and as whom.
#[derive(Debug, Clone)]
pub struct ConnectParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub sslmode: SslMode,
    pub connect_timeout: Duration,
}

impl Default for ConnectParams {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: None,
            database: String::new(),
            sslmode: SslMode::Prefer,
            connect_timeout: Duration::from_secs(30),
        }
    }
}

impl ConnectParams {
    /// Parses a libpq connection URI: `postgresql://user:pass@host:port/db?k=v`.
    ///
    /// `postgres://` is accepted as the same thing, which is the spelling every
    /// framework's configuration actually uses.
    pub fn parse_uri(uri: &str) -> Result<Self, String> {
        let rest = uri
            .strip_prefix("postgresql://")
            .or_else(|| uri.strip_prefix("postgres://"))
            .ok_or_else(|| {
                format!(
                    "{:?} is not a PostgreSQL connection URI; expected something like \
                     postgresql://user:password@host:5432/dbname",
                    uri
                )
            })?;

        let mut params = ConnectParams::default();

        // Query string first, so the authority parsing below never sees a `/`
        // or `@` that belongs to an option value.
        let (rest, query) = match rest.split_once('?') {
            Some((head, tail)) => (head, Some(tail)),
            None => (rest, None),
        };

        // The *last* `@` separates credentials from the host: a password may
        // legitimately contain one, and percent-encoding it is optional in
        // practice even though the RFC asks for it.
        let (credentials, authority) = match rest.rfind('@') {
            Some(at) => (Some(&rest[..at]), &rest[at + 1..]),
            None => (None, rest),
        };

        let (authority, database) = match authority.split_once('/') {
            Some((host, db)) => (host, db),
            None => (authority, ""),
        };

        if let Some(credentials) = credentials {
            let (user, password) = match credentials.split_once(':') {
                Some((u, p)) => (u, Some(p)),
                None => (credentials, None),
            };
            if !user.is_empty() {
                params.user = percent_decode(user)?;
            }
            params.password = match password {
                Some(p) => Some(percent_decode(p)?),
                None => None,
            };
        }

        if !authority.is_empty() {
            // An IPv6 literal is bracketed, and its colons are not the port
            // separator.
            let (host, port) = if let Some(close) = authority.strip_prefix('[') {
                let (inside, after) = close
                    .split_once(']')
                    .ok_or("the IPv6 host in the connection URI has no closing ']'")?;
                (inside.to_string(), after.strip_prefix(':'))
            } else {
                match authority.split_once(':') {
                    Some((h, p)) => (h.to_string(), Some(p)),
                    None => (authority.to_string(), None),
                }
            };
            if !host.is_empty() {
                params.host = percent_decode(&host)?;
            }
            if let Some(port) = port.filter(|p| !p.is_empty()) {
                params.port = port
                    .parse()
                    .map_err(|_| format!("{:?} is not a port number", port))?;
            }
        }

        if !database.is_empty() {
            params.database = percent_decode(database)?;
        }

        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("{:?} in the connection URI is not key=value", pair))?;
            let value = percent_decode(value)?;
            match key.to_ascii_lowercase().as_str() {
                "sslmode" => {
                    params.sslmode = SslMode::parse(&value).ok_or_else(|| {
                        format!(
                            "sslmode={:?} is not one of disable, prefer, require or verify-full",
                            value
                        )
                    })?;
                }
                "user" => params.user = value,
                "password" => params.password = Some(value),
                "dbname" | "database" => params.database = value,
                "host" => params.host = value,
                "port" => {
                    params.port = value
                        .parse()
                        .map_err(|_| format!("port={:?} is not a port number", value))?;
                }
                "connect_timeout" => {
                    let seconds: u64 = value
                        .parse()
                        .map_err(|_| format!("connect_timeout={:?} is not a number", value))?;
                    params.connect_timeout = Duration::from_secs(seconds.max(1));
                }
                // Options this client has no use for are ignored rather than
                // refused: a DSN copied from an application's configuration
                // carries pool sizes and application names, and failing on them
                // would make the copy-paste that should just work not work.
                _ => {}
            }
        }

        if params.database.is_empty() {
            return Err(
                "the connection URI names no database; expected postgresql://…/dbname".to_string(),
            );
        }

        Ok(params)
    }

    /// How this connection would be written down, with the password redacted.
    ///
    /// Used in reports and log lines, which are exactly the places a password
    /// must not reach.
    pub fn redacted(&self) -> String {
        let credentials = match self.password {
            Some(_) => format!("{}:***@", self.user),
            None => format!("{}@", self.user),
        };
        format!(
            "postgresql://{}{}:{}/{}?sslmode={}",
            credentials,
            self.host,
            self.port,
            self.database,
            self.sslmode.name()
        )
    }
}

fn percent_decode(text: &str) -> Result<String, String> {
    if !text.contains('%') {
        return Ok(text.to_string());
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .ok_or_else(|| format!("{:?} ends in an incomplete % escape", text))?;
            let value = u8::from_str_radix(
                std::str::from_utf8(hex)
                    .map_err(|_| "a % escape is not hexadecimal".to_string())?,
                16,
            )
            .map_err(|_| {
                format!(
                    "%{} is not a hexadecimal escape",
                    String::from_utf8_lossy(hex)
                )
            })?;
            out.push(value);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| "a % escape decoded to something that is not UTF-8".to_string())
}

/// A plaintext or TLS connection, buffered, behind one interface.
enum Stream {
    Plain(BufReader<TcpStream>),
    Tls(Box<BufReader<tokio_rustls::client::TlsStream<TcpStream>>>),
}

impl Stream {
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(bytes).await,
            Stream::Tls(s) => s.write_all(bytes).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush().await,
            Stream::Tls(s) => s.flush().await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.read_exact(buf).await.map(|_| ()),
            Stream::Tls(s) => s.read_exact(buf).await.map(|_| ()),
        }
    }
}

/// One backend message: its type byte and its payload, header excluded.
struct Message {
    tag: u8,
    body: Vec<u8>,
}

/// A field of a result set, as the server describes it.
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    /// The type the server says this column has. The migrator maps from this,
    /// not from a type *name*, so a domain or an alias cannot be mistaken for
    /// something it is not.
    pub type_oid: u32,
    /// `atttypmod`: the declared precision, length or scale, packed as
    /// PostgreSQL packs it. `-1` when the type takes no modifier.
    pub type_modifier: i32,
}

/// The rows of a simple query, as text.
#[derive(Debug, Default)]
pub struct QueryResult {
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl QueryResult {
    /// The value in `row` under the column called `name`.
    ///
    /// Panics on an unknown column: every call site names a column of a query
    /// written a few lines above it, so a miss is a typo in this crate rather
    /// than anything the server or the operator did.
    #[track_caller]
    pub fn get<'a>(&'a self, row: &'a [Option<String>], name: &str) -> Option<&'a str> {
        let index = self
            .fields
            .iter()
            .position(|f| f.name == name)
            .unwrap_or_else(|| panic!("the catalogue query returned no column called {:?}", name));
        row[index].as_deref()
    }
}

/// Whether a `COPY` stream is still running, and what is left of the last chunk.
struct CopyState {
    /// Bytes received but not yet split into rows.
    pending: Vec<u8>,
    /// How much of `pending` has already been handed out.
    consumed: usize,
    /// The server has sent `CopyDone`; `pending` is all that remains.
    ended: bool,
}

pub struct PgConnection {
    stream: Stream,
    /// Server settings, as reported by `ParameterStatus`.
    parameters: Vec<(String, String)>,
    copy: Option<CopyState>,
    /// How the connection is described in errors and reports. Never carries the
    /// password.
    description: String,
}

impl PgConnection {
    /// Opens a connection and authenticates.
    pub async fn connect(params: &ConnectParams) -> Result<Self, String> {
        let description = params.redacted();
        let address = format!("{}:{}", params.host, params.port);

        let tcp = tokio::time::timeout(params.connect_timeout, TcpStream::connect(&address))
            .await
            .map_err(|_| {
                format!(
                    "connecting to {} timed out after {}s",
                    address,
                    params.connect_timeout.as_secs()
                )
            })?
            .map_err(|e| format!("could not connect to {}: {}", address, e))?;

        // Nagle would hold back the small messages the handshake is made of,
        // waiting for an acknowledgement that is waiting for them.
        let _ = tcp.set_nodelay(true);

        let stream = Self::maybe_upgrade(tcp, params).await?;

        let mut connection = PgConnection {
            stream,
            parameters: Vec::new(),
            copy: None,
            description,
        };

        connection.startup(params).await?;
        Ok(connection)
    }

    /// Negotiates TLS if the mode asks for it.
    async fn maybe_upgrade(tcp: TcpStream, params: &ConnectParams) -> Result<Stream, String> {
        if params.sslmode == SslMode::Disable {
            return Ok(Stream::Plain(BufReader::with_capacity(READ_BUFFER, tcp)));
        }

        let mut tcp = tcp;
        let mut request = Vec::with_capacity(8);
        request.extend_from_slice(&8i32.to_be_bytes());
        request.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        tcp.write_all(&request)
            .await
            .map_err(|e| format!("could not ask for TLS: {}", e))?;

        let mut answer = [0u8; 1];
        tcp.read_exact(&mut answer)
            .await
            .map_err(|e| format!("the server closed the connection when asked for TLS: {}", e))?;

        match answer[0] {
            b'S' => {}
            b'N' => {
                return match params.sslmode {
                    SslMode::Prefer => {
                        log::warn!(
                            "{}:{} does not offer TLS; continuing in cleartext because sslmode is \
                             'prefer'",
                            params.host,
                            params.port
                        );
                        Ok(Stream::Plain(BufReader::with_capacity(READ_BUFFER, tcp)))
                    }
                    _ => Err(format!(
                        "{}:{} refused TLS, and sslmode={} demands it",
                        params.host,
                        params.port,
                        params.sslmode.name()
                    )),
                };
            }
            // 'E' is an ErrorResponse from a server too old to know the
            // request. Reported as itself rather than as a mystery byte.
            b'E' => {
                return Err(format!(
                    "{}:{} rejected the TLS request outright; try sslmode=disable",
                    params.host, params.port
                ));
            }
            other => {
                return Err(format!(
                    "{}:{} answered the TLS request with {:?}, which is not part of the protocol \
                     — is that really a PostgreSQL server?",
                    params.host, params.port, other as char
                ));
            }
        }

        // aws-lc-rs is what `rastcp` installs; installing it twice is not an
        // error worth reporting, only the first call wins.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let config = match params.sslmode {
            SslMode::VerifyFull => {
                let roots = rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                };
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth()
            }
            // libpq's `require` encrypts without authenticating. Reproducing
            // that faithfully means accepting any certificate — which is why
            // `SslMode::Require`'s documentation says so in as many words.
            _ => rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServer::new()))
                .with_no_client_auth(),
        };

        let server_name =
            rustls::pki_types::ServerName::try_from(params.host.clone()).map_err(|_| {
                format!(
                    "{:?} is not a name TLS can be verified against",
                    params.host
                )
            })?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tls = connector.connect(server_name, tcp).await.map_err(|e| {
            format!(
                "the TLS handshake with {}:{} failed: {}",
                params.host, params.port, e
            )
        })?;

        Ok(Stream::Tls(Box::new(BufReader::with_capacity(
            READ_BUFFER,
            tls,
        ))))
    }

    /// Sends the startup message and works through whatever authentication the
    /// server asks for.
    async fn startup(&mut self, params: &ConnectParams) -> Result<(), String> {
        let mut body = Vec::with_capacity(128);
        body.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        let settings: [(&str, &str); 7] = [
            ("user", params.user.as_str()),
            ("database", params.database.as_str()),
            ("application_name", "rasterizeddb-import"),
            // Every one of the four below exists to make the *text* the server
            // renders values into predictable, because that text is what the
            // migrator converts. Left at the server's defaults, a database
            // configured with `DateStyle = 'SQL, DMY'` would hand over
            // `21/08/2026` and the import would read it as the 8th of an
            // impossible month — or, worse, as the 8th of a real one.
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, YMD"),
            // Timestamps come out already in UTC, so no zone arithmetic is
            // needed on this side and none can be got wrong.
            ("TimeZone", "UTC"),
            // Shortest-round-trip float output. PostgreSQL 12 and later do this
            // by default; older servers truncate to 6 significant digits and
            // silently lose the low bits of every double.
            ("extra_float_digits", "3"),
        ];
        for (key, value) in settings {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);

        let mut message = Vec::with_capacity(body.len() + 4);
        message.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        message.extend_from_slice(&body);
        self.write(&message).await?;

        loop {
            let message = self.read_message().await?;
            match message.tag {
                b'R' => {
                    if self.authenticate(&message.body, params).await? {
                        continue;
                    }
                }
                b'S' => self.note_parameter(&message.body),
                // BackendKeyData. Only cancellation uses it, which this client
                // does not do.
                b'K' => {}
                b'Z' => return Ok(()),
                b'E' => {
                    return Err(format!(
                        "{} refused the connection: {}",
                        self.description,
                        error_text(&message.body)
                    ));
                }
                b'N' => log_notice(&message.body),
                other => {
                    return Err(format!(
                        "unexpected message {:?} during start-up",
                        other as char
                    ));
                }
            }
        }
    }

    /// Answers one authentication request. Returns whether the exchange
    /// continues.
    async fn authenticate(&mut self, body: &[u8], params: &ConnectParams) -> Result<bool, String> {
        let kind = read_i32(body, 0)?;
        match kind {
            // AuthenticationOk
            0 => Ok(false),
            // Cleartext password.
            3 => {
                let password = self.require_password(params)?;
                let mut message = Vec::with_capacity(password.len() + 1);
                message.extend_from_slice(password.as_bytes());
                message.push(0);
                self.send(b'p', &message).await?;
                Ok(true)
            }
            // MD5, salted.
            5 => {
                let password = self.require_password(params)?;
                let salt = body
                    .get(4..8)
                    .ok_or("the MD5 authentication request carried no salt")?;
                let response = md5_response(&params.user, &password, salt);
                let mut message = Vec::with_capacity(response.len() + 1);
                message.extend_from_slice(response.as_bytes());
                message.push(0);
                self.send(b'p', &message).await?;
                Ok(true)
            }
            // SASL: the server lists mechanisms, we pick one.
            10 => {
                let password = self.require_password(params)?;
                let mechanisms = read_cstring_list(&body[4..]);
                if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    return Err(format!(
                        "the server offers only {:?}, and this client speaks SCRAM-SHA-256",
                        mechanisms
                    ));
                }
                // Deliberately not SCRAM-SHA-256-PLUS. Channel binding would
                // tie the exchange to the TLS connection, but only
                // `verify-full` makes that worth anything, and a server
                // offering PLUS over a connection whose certificate was not
                // checked is exactly the case where the binding value itself
                // came from the attacker.
                let (mut scram, client_first) =
                    ScramClient::start(&params.user, &password, ChannelBinding::None);

                let mut message = Vec::with_capacity(client_first.len() + 32);
                message.extend_from_slice(b"SCRAM-SHA-256");
                message.push(0);
                message.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
                message.extend_from_slice(client_first.as_bytes());
                self.send(b'p', &message).await?;

                // SASLContinue
                let server_first = self.expect_sasl(11).await?;
                let client_final = scram.respond(&server_first)?;
                self.send(b'p', client_final.as_bytes()).await?;

                // SASLFinal
                let server_final = self.expect_sasl(12).await?;
                scram.verify(&server_final)?;
                Ok(true)
            }
            2 | 7 | 9 => Err(
                "the server asked for GSSAPI or SSPI authentication, which this client does not \
                 speak; a password method (scram-sha-256, md5 or password) is needed for an import"
                    .to_string(),
            ),
            other => Err(format!(
                "the server asked for authentication method {}, which this client does not know",
                other
            )),
        }
    }

    fn require_password(&self, params: &ConnectParams) -> Result<String, String> {
        params.password.clone().ok_or_else(|| {
            format!(
                "{} asked for a password and none was given",
                self.description
            )
        })
    }

    /// Reads the next message, insisting it is the SASL step expected.
    async fn expect_sasl(&mut self, step: i32) -> Result<String, String> {
        loop {
            let message = self.read_message().await?;
            match message.tag {
                b'R' => {
                    let kind = read_i32(&message.body, 0)?;
                    if kind != step {
                        return Err(format!(
                            "expected SASL step {} and the server sent {}",
                            step, kind
                        ));
                    }
                    return Ok(String::from_utf8_lossy(&message.body[4..]).into_owned());
                }
                b'E' => {
                    return Err(format!(
                        "authentication failed: {}",
                        error_text(&message.body)
                    ));
                }
                b'N' => log_notice(&message.body),
                other => {
                    return Err(format!(
                        "unexpected message {:?} during authentication",
                        other as char
                    ));
                }
            }
        }
    }

    fn note_parameter(&mut self, body: &[u8]) {
        let mut parts = body.split(|b| *b == 0);
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            self.parameters.push((
                String::from_utf8_lossy(key).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            ));
        }
    }

    /// A server setting as reported at start-up, such as `server_version`.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The connection as it can safely be printed.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Runs one statement and collects its rows.
    ///
    /// Every value comes back as text. That is what the migrator wants: the
    /// catalogue answers are text to begin with, and a table's data is
    /// converted from PostgreSQL's own rendering rather than from a binary
    /// format this client would have to decode a second implementation of.
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult, String> {
        self.assert_not_copying()?;

        let mut message = Vec::with_capacity(sql.len() + 1);
        message.extend_from_slice(sql.as_bytes());
        message.push(0);
        self.send(b'Q', &message).await?;

        let mut result = QueryResult::default();
        let mut failure: Option<String> = None;

        loop {
            let message = self.read_message().await?;
            match message.tag {
                b'T' => result.fields = parse_row_description(&message.body)?,
                b'D' => result.rows.push(parse_data_row(&message.body)?),
                // CommandComplete / EmptyQueryResponse / NoticeResponse:
                // nothing to collect, but the exchange is not over until
                // ReadyForQuery.
                b'C' | b'I' => {}
                b'N' => log_notice(&message.body),
                b'S' => self.note_parameter(&message.body),
                b'E' => {
                    // Recorded rather than returned: the server still owes a
                    // ReadyForQuery, and leaving it unread would desynchronise
                    // the next statement on this connection.
                    failure = Some(error_text(&message.body));
                }
                b'Z' => break,
                other => {
                    return Err(format!(
                        "unexpected message {:?} while running a query",
                        other as char
                    ));
                }
            }
        }

        match failure {
            Some(message) => Err(format!("{}\nwhile running: {}", message, one_line(sql))),
            None => Ok(result),
        }
    }

    /// Begins a `COPY … TO STDOUT` and returns the columns it will produce.
    ///
    /// Rows are then read one at a time with [`PgConnection::copy_row`] until
    /// it returns `None`, which is also when the connection becomes usable for
    /// anything else.
    pub async fn copy_out(&mut self, sql: &str) -> Result<(), String> {
        self.assert_not_copying()?;

        let mut message = Vec::with_capacity(sql.len() + 1);
        message.extend_from_slice(sql.as_bytes());
        message.push(0);
        self.send(b'Q', &message).await?;

        loop {
            let message = self.read_message().await?;
            match message.tag {
                // CopyOutResponse: the copy has begun.
                b'H' => {
                    self.copy = Some(CopyState {
                        pending: Vec::new(),
                        consumed: 0,
                        ended: false,
                    });
                    return Ok(());
                }
                b'N' => log_notice(&message.body),
                b'S' => self.note_parameter(&message.body),
                b'E' => {
                    let text = error_text(&message.body);
                    // Drain to ReadyForQuery so the connection stays usable and
                    // the next table can still be attempted.
                    self.drain_to_ready().await?;
                    return Err(format!("{}\nwhile running: {}", text, one_line(sql)));
                }
                other => {
                    return Err(format!(
                        "expected a COPY to start and the server sent {:?}",
                        other as char
                    ));
                }
            }
        }
    }

    /// The next row of the running copy, as one line of `COPY … TO STDOUT`
    /// text, newline excluded. `None` once the stream has ended.
    pub async fn copy_row(&mut self) -> Result<Option<Vec<u8>>, String> {
        loop {
            {
                let Some(state) = self.copy.as_mut() else {
                    return Err("no COPY is running on this connection".to_string());
                };

                // A row is everything up to the next newline. Text-format COPY
                // escapes any newline *inside* a value as `\n`, two characters,
                // so a raw one always ends a row — the message boundaries do
                // not, which is why this buffers instead of returning a chunk.
                if let Some(at) = memchr_newline(&state.pending[state.consumed..]) {
                    let start = state.consumed;
                    let end = start + at;
                    let row = state.pending[start..end].to_vec();
                    state.consumed = end + 1;
                    if state.consumed == state.pending.len() {
                        state.pending.clear();
                        state.consumed = 0;
                    }
                    return Ok(Some(row));
                }

                if state.ended {
                    // A final line without its newline would be a truncated
                    // row, not a row: reporting it as data is how half a value
                    // gets stored as the whole of one.
                    let leftover = state.pending.len() - state.consumed;
                    if leftover > 0 {
                        return Err(format!(
                            "the COPY stream ended mid-row, with {} bytes that no newline \
                             terminates",
                            leftover
                        ));
                    }
                    self.copy = None;
                    self.drain_to_ready().await?;
                    return Ok(None);
                }

                // Reclaim what has been handed out before asking for more, so a
                // long copy does not grow a buffer of everything it has read.
                if state.consumed > 0 {
                    state.pending.drain(..state.consumed);
                    state.consumed = 0;
                }
            }

            let message = self.read_message().await?;
            let Some(state) = self.copy.as_mut() else {
                return Err("the COPY ended while a row was being read".to_string());
            };
            match message.tag {
                b'd' => state.pending.extend_from_slice(&message.body),
                b'c' => state.ended = true,
                b'N' => log_notice(&message.body),
                b'S' => {
                    let body = message.body;
                    self.note_parameter(&body);
                }
                b'E' => {
                    let text = error_text(&message.body);
                    self.copy = None;
                    self.drain_to_ready().await?;
                    return Err(format!("the COPY failed part-way: {}", text));
                }
                other => {
                    return Err(format!(
                        "unexpected message {:?} in a COPY stream",
                        other as char
                    ));
                }
            }
        }
    }

    /// Reads everything up to and including the next `ReadyForQuery`.
    async fn drain_to_ready(&mut self) -> Result<(), String> {
        loop {
            let message = self.read_message().await?;
            match message.tag {
                b'Z' => return Ok(()),
                b'N' => log_notice(&message.body),
                b'S' => self.note_parameter(&message.body),
                _ => {}
            }
        }
    }

    fn assert_not_copying(&self) -> Result<(), String> {
        if self.copy.is_some() {
            return Err(
                "a COPY is still running on this connection; read it to the end first".to_string(),
            );
        }
        Ok(())
    }

    /// Asks the server to close the connection, ignoring whether it hears.
    pub async fn close(mut self) {
        let _ = self.send(b'X', &[]).await;
    }

    async fn send(&mut self, tag: u8, body: &[u8]) -> Result<(), String> {
        let mut message = Vec::with_capacity(body.len() + 5);
        message.push(tag);
        message.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        message.extend_from_slice(body);
        self.write(&message).await
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|e| format!("could not write to {}: {}", self.description, e))?;
        self.stream
            .flush()
            .await
            .map_err(|e| format!("could not write to {}: {}", self.description, e))
    }

    async fn read_message(&mut self) -> Result<Message, String> {
        let mut header = [0u8; 5];
        self.stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("{} closed the connection: {}", self.description, e))?;
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        if length < 4 {
            return Err(format!(
                "the server sent a message of length {}, which is not a length",
                length
            ));
        }
        let body_len = length as usize - 4;
        if body_len > MAX_MESSAGE {
            return Err(format!(
                "the server sent a {}-byte message, past the {}-byte ceiling this client accepts",
                body_len, MAX_MESSAGE
            ));
        }
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            self.stream.read_exact(&mut body).await.map_err(|e| {
                format!(
                    "{} closed the connection mid-message: {}",
                    self.description, e
                )
            })?;
        }
        Ok(Message {
            tag: header[0],
            body,
        })
    }
}

/// PostgreSQL's MD5 challenge response: `md5(md5(password || user) || salt)`.
fn md5_response(user: &str, password: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};

    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner = hex_lower(&inner.finalize());

    let mut outer = Md5::new();
    outer.update(inner.as_bytes());
    outer.update(salt);
    format!("md5{}", hex_lower(&outer.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap());
        out.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap());
    }
    out
}

fn read_i32(body: &[u8], at: usize) -> Result<i32, String> {
    body.get(at..at + 4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(|| "a message ended where a 32-bit field was expected".to_string())
}

fn read_cstring_list(body: &[u8]) -> Vec<String> {
    body.split(|b| *b == 0)
        .filter(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).into_owned())
        .collect()
}

fn parse_row_description(body: &[u8]) -> Result<Vec<Field>, String> {
    let count = body
        .get(0..2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
        .ok_or("a RowDescription with no field count")?;
    let mut fields = Vec::with_capacity(count);
    let mut at = 2usize;
    for _ in 0..count {
        let end = body[at..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("a RowDescription field name with no terminator")?;
        let name = String::from_utf8_lossy(&body[at..at + end]).into_owned();
        at += end + 1;
        // table oid (4), column number (2), type oid (4), type size (2),
        // type modifier (4), format code (2)
        let type_oid = body
            .get(at + 6..at + 10)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or("a truncated RowDescription field")?;
        let type_modifier = read_i32(body, at + 12)?;
        at += 18;
        fields.push(Field {
            name,
            type_oid,
            type_modifier,
        });
    }
    Ok(fields)
}

fn parse_data_row(body: &[u8]) -> Result<Vec<Option<String>>, String> {
    let count = body
        .get(0..2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
        .ok_or("a DataRow with no column count")?;
    let mut values = Vec::with_capacity(count);
    let mut at = 2usize;
    for _ in 0..count {
        let length = read_i32(body, at)?;
        at += 4;
        if length < 0 {
            values.push(None);
            continue;
        }
        let length = length as usize;
        let bytes = body
            .get(at..at + length)
            .ok_or("a DataRow value ran past the end of its message")?;
        values.push(Some(String::from_utf8_lossy(bytes).into_owned()));
        at += length;
    }
    Ok(values)
}

/// Renders an ErrorResponse or NoticeResponse into something worth reading.
fn error_text(body: &[u8]) -> String {
    let mut message = String::new();
    let mut detail = String::new();
    let mut hint = String::new();
    let mut code = String::new();

    for field in body.split(|b| *b == 0) {
        let Some((&kind, rest)) = field.split_first() else {
            continue;
        };
        let value = String::from_utf8_lossy(rest).into_owned();
        match kind {
            b'M' => message = value,
            b'D' => detail = value,
            b'H' => hint = value,
            b'C' => code = value,
            _ => {}
        }
    }

    let mut out = message;
    if !code.is_empty() {
        out = format!("{} [{}]", out, code);
    }
    if !detail.is_empty() {
        out = format!("{}: {}", out, detail);
    }
    if !hint.is_empty() {
        out = format!("{} ({})", out, hint);
    }
    out
}

fn log_notice(body: &[u8]) {
    log::debug!("PostgreSQL notice: {}", error_text(body));
}

fn memchr_newline(haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|b| *b == b'\n')
}

/// A statement squashed onto one line, for an error message.
fn one_line(sql: &str) -> String {
    let flat: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.len() > 200 {
        let mut cut = 200;
        while cut > 0 && !flat.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}…", &flat[..cut])
    } else {
        flat
    }
}

/// The certificate verifier `sslmode=require` uses: none at all.
///
/// It exists so that `require` means what libpq means by it. Every other mode
/// either does not use TLS or uses the real verifier.
#[derive(Debug)]
struct AcceptAnyServer {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl AcceptAnyServer {
    fn new() -> Self {
        Self {
            provider: Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
