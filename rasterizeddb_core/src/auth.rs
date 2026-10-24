//! PostgreSQL authentication.
//!
//! Implements the methods a real client negotiates: SCRAM-SHA-256 (RFC 5802 and
//! RFC 7677), which every modern driver prefers, plus `md5` and cleartext for
//! older ones. Stored secrets use PostgreSQL's own verifier formats, so a
//! password hash can be moved between the two systems unchanged.
//!
//! # Why the password never reaches the server
//!
//! Under SCRAM the client proves it knows the password without sending it, and
//! the server stores only values derived from it. A stolen verifier cannot be
//! replayed as a password, which is the property `md5` and cleartext lack —
//! both are offered only because clients that speak nothing else still exist.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Iteration count for new verifiers. PostgreSQL's own default.
pub const SCRAM_DEFAULT_ITERATIONS: u32 = 4096;

const SCRAM_SALT_LEN: usize = 16;
const SCRAM_NONCE_LEN: usize = 18;

/// How a connection is expected to authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// No verification. Every connection is accepted.
    Trust,
    /// The password crosses the wire as-is. Only safe under TLS.
    Password,
    /// Salted MD5. Legacy, and offered only for clients that speak nothing else.
    Md5,
    /// SCRAM-SHA-256. The default, and what modern drivers negotiate.
    ScramSha256,
}

impl AuthMethod {
    pub fn parse(text: &str) -> Option<Self> {
        Some(
            match text.trim().to_ascii_lowercase().replace('-', "").as_str() {
                "trust" => AuthMethod::Trust,
                "password" | "cleartext" => AuthMethod::Password,
                "md5" => AuthMethod::Md5,
                "scram" | "scramsha256" => AuthMethod::ScramSha256,
                _ => return None,
            },
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            AuthMethod::Trust => "trust",
            AuthMethod::Password => "password",
            AuthMethod::Md5 => "md5",
            AuthMethod::ScramSha256 => "scram-sha-256",
        }
    }
}

/// A stored secret, in one of PostgreSQL's verifier formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verifier {
    /// `SCRAM-SHA-256$<iterations>:<salt>$<stored key>:<server key>`
    Scram {
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    /// `md5<hex>`, where the hex is `md5(password || username)`.
    Md5 { hex: String },
    /// A password stored as written. Accepted on input so an operator can set
    /// one directly, and upgraded to SCRAM whenever a verifier is built.
    Plain(String),
}

impl Verifier {
    /// Parses a stored secret, recognising PostgreSQL's formats.
    pub fn parse(stored: &str) -> Verifier {
        if let Some(rest) = stored.strip_prefix("SCRAM-SHA-256$") {
            if let Some(v) = parse_scram_verifier(rest) {
                return v;
            }
        }
        if let Some(hex) = stored.strip_prefix("md5") {
            if hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Verifier::Md5 {
                    hex: hex.to_ascii_lowercase(),
                };
            }
        }
        Verifier::Plain(stored.to_string())
    }

    /// Renders back to the stored form.
    pub fn to_stored(&self) -> String {
        match self {
            Verifier::Scram {
                iterations,
                salt,
                stored_key,
                server_key,
            } => format!(
                "SCRAM-SHA-256${}:{}${}:{}",
                iterations,
                BASE64.encode(salt),
                BASE64.encode(stored_key),
                BASE64.encode(server_key)
            ),
            Verifier::Md5 { hex } => format!("md5{}", hex),
            Verifier::Plain(p) => p.clone(),
        }
    }
}

fn parse_scram_verifier(rest: &str) -> Option<Verifier> {
    let (params, keys) = rest.split_once('$')?;
    let (iterations, salt) = params.split_once(':')?;
    let (stored_key, server_key) = keys.split_once(':')?;

    Some(Verifier::Scram {
        iterations: iterations.parse().ok()?,
        salt: BASE64.decode(salt).ok()?,
        stored_key: BASE64.decode(stored_key).ok()?,
        server_key: BASE64.decode(server_key).ok()?,
    })
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Derives the SCRAM keys from a password and salt.
pub fn scram_keys(password: &str, salt: &[u8], iterations: u32) -> (Vec<u8>, Vec<u8>) {
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);

    let client_key = hmac(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac(&salted, b"Server Key");
    (stored_key, server_key)
}

/// Builds a SCRAM verifier for a new password, with a fresh random salt.
pub fn build_scram_verifier(password: &str) -> Verifier {
    let mut salt = vec![0u8; SCRAM_SALT_LEN];
    fill_random(&mut salt);
    let (stored_key, server_key) = scram_keys(password, &salt, SCRAM_DEFAULT_ITERATIONS);
    Verifier::Scram {
        iterations: SCRAM_DEFAULT_ITERATIONS,
        salt,
        stored_key,
        server_key,
    }
}

/// The `md5(password || username)` hex PostgreSQL stores.
pub fn md5_verifier(user: &str, password: &str) -> Verifier {
    use md5::Md5;
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(user.as_bytes());
    Verifier::Md5 {
        hex: hex_lower(&hasher.finalize()),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap());
        out.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap());
    }
    out
}

/// Random bytes for salts and nonces.
///
/// From a cryptographic generator — `rand`'s thread RNG, ChaCha reseeded from
/// the OS. `fastrand` is a wyrand PRNG whose state is recoverable from its
/// output, so a client that saw a few nonces could predict the next salt.
pub(crate) fn fill_random(out: &mut [u8]) {
    rand::fill(out);
}

/// A nonce built from the printable ASCII SCRAM allows, excluding `,`.
fn make_nonce() -> String {
    use rand::RngExt;
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..SCRAM_NONCE_LEN)
        .map(|_| ALPHABET[rng.random_range(..ALPHABET.len())] as char)
        .collect()
}

/// Verifies the response to an `md5` challenge.
///
/// The client sends `md5(md5(password || username) || salt)`, so the server can
/// check it holding only the inner hash.
pub fn verify_md5(verifier: &Verifier, user: &str, salt: &[u8; 4], response: &str) -> bool {
    use md5::Md5;

    let inner = match verifier {
        Verifier::Md5 { hex } => hex.clone(),
        Verifier::Plain(password) => match md5_verifier(user, password) {
            Verifier::Md5 { hex } => hex,
            _ => unreachable!("md5_verifier returns Md5"),
        },
        // A SCRAM verifier stores no MD5-compatible secret; PostgreSQL cannot
        // answer an md5 challenge from one either.
        Verifier::Scram { .. } => return false,
    };

    let mut hasher = Md5::new();
    hasher.update(inner.as_bytes());
    hasher.update(salt);
    let expected = format!("md5{}", hex_lower(&hasher.finalize()));

    expected.as_bytes().ct_eq(response.as_bytes()).into()
}

/// Verifies a cleartext password.
pub fn verify_password(verifier: &Verifier, user: &str, password: &str) -> bool {
    match verifier {
        Verifier::Plain(stored) => stored.as_bytes().ct_eq(password.as_bytes()).into(),
        Verifier::Md5 { .. } => {
            let candidate = md5_verifier(user, password);
            candidate == *verifier
        }
        Verifier::Scram {
            iterations,
            salt,
            stored_key,
            ..
        } => {
            let (candidate, _) = scram_keys(password, salt, *iterations);
            candidate.ct_eq(stored_key).into()
        }
    }
}

/// What an exchange is tied to, so that it cannot be relayed elsewhere.
///
/// A SCRAM exchange over TLS proves the caller knows the password. On its own
/// it does not prove the caller is speaking on *this* connection: an attacker
/// who can intercept — which, against a server whose certificate is not
/// verified, means anyone in the network path — can open its own connection to
/// the real server and pass the messages through in both directions. The proof
/// verifies, the attacker is logged in, and the client is talking to the
/// attacker.
///
/// Channel binding closes that. Both ends mix a value derived from their own
/// TLS connection into what is signed. A relay sits on two different
/// connections with two different values, so whichever one it forwards is wrong
/// at the other end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelBinding {
    /// Not bound. What PostgreSQL's plain `SCRAM-SHA-256` does, and all a
    /// client that speaks only that can do.
    None,
    /// RFC 9266 `tls-exporter`, exported from a TLS 1.3 connection.
    TlsExporter(Vec<u8>),
}

impl ChannelBinding {
    /// The GS2 channel-binding name, as it appears in `p=<name>`.
    pub fn name(&self) -> Option<&'static str> {
        match self {
            ChannelBinding::None => None,
            ChannelBinding::TlsExporter(_) => Some("tls-exporter"),
        }
    }

    fn data(&self) -> &[u8] {
        match self {
            ChannelBinding::None => &[],
            ChannelBinding::TlsExporter(data) => data,
        }
    }
}

/// The `c=` value that goes with a GS2 header: base64 of the header, followed
/// by the binding data when the header asked for binding.
fn cbind_input(gs2_header: &str, binding: &ChannelBinding) -> String {
    let mut input = gs2_header.as_bytes().to_vec();
    if gs2_header.starts_with("p=") {
        input.extend_from_slice(binding.data());
    }
    BASE64.encode(input)
}

/// Server side of a SCRAM-SHA-256 exchange.
#[derive(Debug)]
pub struct ScramServer {
    verifier: Verifier,
    client_first_bare: String,
    server_first: String,
    nonce: String,
    /// What the client's `c=` must be. Checked in [`ScramServer::finish`], so a
    /// GS2 header rewritten in flight — the downgrade that would strip channel
    /// binding — does not verify.
    expected_cbind: String,
}

impl ScramServer {
    /// Consumes the client's first message and produces the server's.
    ///
    /// `client_first` is the whole message including its GS2 header. The
    /// exchange is unbound: a client asking for channel binding is refused
    /// rather than silently served without it.
    pub fn start(verifier: Verifier, client_first: &str) -> Result<(Self, String), String> {
        Self::start_bound(verifier, client_first, &ChannelBinding::None)
    }

    /// As [`ScramServer::start`], bound to this connection.
    ///
    /// With a binding other than [`ChannelBinding::None`], binding is
    /// *mandatory*: a client that does not ask for it is refused. Anything else
    /// would let an attacker strip the protection by rewriting one header.
    pub fn start_bound(
        verifier: Verifier,
        client_first: &str,
        binding: &ChannelBinding,
    ) -> Result<(Self, String), String> {
        // GS2 header: `n,,` (client does not support binding), `y,,` (client
        // supports it but believes the server does not), or `p=<name>,,`
        // (bound).
        let comma = client_first
            .find(",,")
            .ok_or_else(|| "malformed SCRAM client-first message".to_string())?;
        let gs2_header = &client_first[..comma + 2];
        let bare = &client_first[comma + 2..];

        match (gs2_header.strip_prefix("p="), binding) {
            (Some(_), ChannelBinding::None) => {
                // Accepting `p=` and not binding would defeat the protection
                // the client asked for.
                return Err("channel binding is not supported".to_string());
            }
            (Some(name), _) => {
                let name = name.trim_end_matches(',');
                if Some(name) != binding.name() {
                    return Err(format!("unsupported channel binding: {}", name));
                }
            }
            (None, ChannelBinding::None) => {}
            (None, _) => {
                // The server supports binding, so a client that declined it is
                // either old or being downgraded. This protocol is spoken only
                // by clients that can bind, so it is the latter.
                return Err("this server requires channel binding".to_string());
            }
        }

        let client_nonce =
            field(bare, 'r').ok_or_else(|| "SCRAM client-first has no nonce".to_string())?;
        if client_nonce.is_empty() {
            return Err("SCRAM client-first has an empty nonce".to_string());
        }

        let (iterations, salt) = match &verifier {
            Verifier::Scram {
                iterations, salt, ..
            } => (*iterations, salt.clone()),
            // Only a SCRAM verifier can answer a SCRAM exchange. A fabricated
            // salt would let an attacker distinguish a missing user from a
            // wrong password, so one is derived deterministically instead.
            other => {
                let salt = decoy_salt(other);
                (SCRAM_DEFAULT_ITERATIONS, salt)
            }
        };

        let nonce = format!("{}{}", client_nonce, make_nonce());
        let server_first = format!("r={},s={},i={}", nonce, BASE64.encode(&salt), iterations);

        Ok((
            ScramServer {
                verifier,
                client_first_bare: bare.to_string(),
                server_first: server_first.clone(),
                nonce,
                expected_cbind: cbind_input(gs2_header, binding),
            },
            server_first,
        ))
    }

    /// Verifies the client's final message and produces the server's.
    pub fn finish(&self, client_final: &str) -> Result<String, String> {
        let nonce = field(client_final, 'r')
            .ok_or_else(|| "SCRAM client-final has no nonce".to_string())?;
        if nonce != self.nonce {
            return Err("SCRAM nonce does not match".to_string());
        }

        // `c=` repeats the GS2 header inside the signed message. Checking it
        // here is what makes a rewritten header — the way channel binding would
        // be stripped in flight — fail rather than pass unnoticed.
        let cbind = field(client_final, 'c')
            .ok_or_else(|| "SCRAM client-final has no channel-binding field".to_string())?;
        if !bool::from(cbind.as_bytes().ct_eq(self.expected_cbind.as_bytes())) {
            return Err("SCRAM channel binding does not match".to_string());
        }

        let proof_b64 = field(client_final, 'p')
            .ok_or_else(|| "SCRAM client-final has no proof".to_string())?;
        let proof = BASE64
            .decode(proof_b64)
            .map_err(|_| "SCRAM proof is not valid base64".to_string())?;

        let without_proof = client_final
            .rfind(",p=")
            .map(|at| &client_final[..at])
            .ok_or_else(|| "SCRAM client-final has no proof".to_string())?;

        let Verifier::Scram {
            stored_key,
            server_key,
            ..
        } = &self.verifier
        else {
            // The exchange ran with a decoy salt: the client's proof cannot
            // match, and it learns nothing beyond "authentication failed".
            return Err("authentication failed".to_string());
        };

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        let client_signature = hmac(stored_key, auth_message.as_bytes());
        if proof.len() != client_signature.len() {
            return Err("authentication failed".to_string());
        }
        let client_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(p, s)| p ^ s)
            .collect();

        let candidate = sha256(&client_key);
        let matches: bool = candidate.ct_eq(stored_key).into();
        if !matches {
            return Err("authentication failed".to_string());
        }

        let server_signature = hmac(server_key, auth_message.as_bytes());
        Ok(format!("v={}", BASE64.encode(server_signature)))
    }
}

/// Client side of a SCRAM-SHA-256 exchange.
///
/// The counterpart to [`ScramServer`], and the reason a client can prove it
/// knows the password without ever sending it. It also *checks the server*:
/// [`ScramClient::verify`] fails unless the server proves it holds the stored
/// verifier, which a server that merely intercepted the exchange cannot do.
/// Skipping that check would leave a client happy to be talking to anyone.
#[derive(Debug)]
pub struct ScramClient {
    password: String,
    binding: ChannelBinding,
    gs2_header: String,
    client_first_bare: String,
    client_nonce: String,
    /// Set by [`ScramClient::respond`], checked by [`ScramClient::verify`].
    server_signature: Option<Vec<u8>>,
}

/// Iteration counts below this are refused.
///
/// The salt and iteration count come from the server, so a hostile or
/// downgraded one could ask for `i=1` and make a captured exchange cheap to
/// attack offline. PostgreSQL's own default is the floor.
const SCRAM_MIN_ITERATIONS: u32 = SCRAM_DEFAULT_ITERATIONS;

impl ScramClient {
    /// Begins an exchange, returning the client-first message to send.
    pub fn start(user: &str, password: &str, binding: ChannelBinding) -> (Self, String) {
        let gs2_header = match binding.name() {
            Some(name) => format!("p={},,", name),
            None => "n,,".to_string(),
        };
        let client_nonce = make_nonce();
        let client_first_bare = format!("n={},r={}", saslname(user), client_nonce);
        let client_first = format!("{}{}", gs2_header, client_first_bare);

        (
            ScramClient {
                password: password.to_string(),
                binding,
                gs2_header,
                client_first_bare,
                client_nonce,
                server_signature: None,
            },
            client_first,
        )
    }

    /// Answers the server's first message with the client's final one.
    pub fn respond(&mut self, server_first: &str) -> Result<String, String> {
        let nonce = field(server_first, 'r')
            .ok_or_else(|| "SCRAM server-first has no nonce".to_string())?;
        // The server's nonce must extend the client's. Without this check a
        // replayed server-first from an earlier exchange would be accepted.
        if !nonce.starts_with(&self.client_nonce) || nonce.len() == self.client_nonce.len() {
            return Err("SCRAM server-first does not extend the client nonce".to_string());
        }

        let salt = BASE64
            .decode(
                field(server_first, 's')
                    .ok_or_else(|| "SCRAM server-first has no salt".to_string())?,
            )
            .map_err(|_| "SCRAM salt is not valid base64".to_string())?;
        let iterations: u32 = field(server_first, 'i')
            .ok_or_else(|| "SCRAM server-first has no iteration count".to_string())?
            .parse()
            .map_err(|_| "SCRAM iteration count is not a number".to_string())?;
        if iterations < SCRAM_MIN_ITERATIONS {
            return Err(format!(
                "SCRAM iteration count {} is below the {} this client accepts",
                iterations, SCRAM_MIN_ITERATIONS
            ));
        }

        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(self.password.as_bytes(), &salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac(&salted, b"Server Key");

        let without_proof = format!(
            "c={},r={}",
            cbind_input(&self.gs2_header, &self.binding),
            nonce
        );
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, without_proof
        );

        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        self.server_signature = Some(hmac(&server_key, auth_message.as_bytes()));

        Ok(format!("{},p={}", without_proof, BASE64.encode(proof)))
    }

    /// Checks the server's final message, which proves the server knew the
    /// verifier rather than merely having relayed the exchange.
    pub fn verify(&self, server_final: &str) -> Result<(), String> {
        let expected = self
            .server_signature
            .as_ref()
            .ok_or_else(|| "the exchange has not reached the server-final message".to_string())?;

        let signature = BASE64
            .decode(
                field(server_final, 'v')
                    .ok_or_else(|| "SCRAM server-final has no signature".to_string())?,
            )
            .map_err(|_| "SCRAM server signature is not valid base64".to_string())?;

        if bool::from(signature.ct_eq(expected)) {
            Ok(())
        } else {
            Err("the server did not prove it knows this password".to_string())
        }
    }
}

/// Escapes a user name for the `n=` field, where `,` and `=` are structural.
fn saslname(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// A salt for a user with no SCRAM verifier.
///
/// Derived from the stored secret rather than randomly, so repeated attempts
/// against the same unknown user return the same salt — a random one would tell
/// an attacker the user does not exist.
fn decoy_salt(verifier: &Verifier) -> Vec<u8> {
    let seed = match verifier {
        Verifier::Plain(p) => p.clone(),
        Verifier::Md5 { hex } => hex.clone(),
        Verifier::Scram { .. } => String::new(),
    };
    sha256(seed.as_bytes())[..SCRAM_SALT_LEN].to_vec()
}

/// Reads a `key=value` field out of a comma-separated SCRAM message.
fn field(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| {
        let mut chars = part.chars();
        if chars.next() == Some(key) && chars.next() == Some('=') {
            Some(&part[2..])
        } else {
            None
        }
    })
}

// ------------------------------------------------------- role catalogue

/// Name of the table roles are stored in.
///
/// An ordinary table, so roles are queried, written and persisted by the same
/// machinery as anything else, and survive a restart without a second format.
pub const ROLE_TABLE: &str = "rdb_roles";

/// DDL for the role catalogue.
pub const ROLE_TABLE_DDL: &str = "CREATE TABLE rdb_roles (\
     name TEXT NOT NULL, \
     password TEXT, \
     can_login BOOL NOT NULL, \
     is_superuser BOOL NOT NULL)";

/// A role as stored.
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub verifier: Option<Verifier>,
    pub can_login: bool,
    pub is_superuser: bool,
}

impl Role {
    /// The `INSERT` that stores this role.
    pub fn insert_statement(&self) -> String {
        format!(
            "INSERT INTO {} (name, password, can_login, is_superuser) VALUES ({}, {}, {}, {})",
            ROLE_TABLE,
            quote(&self.name),
            match &self.verifier {
                Some(v) => quote(&v.to_stored()),
                None => "NULL".to_string(),
            },
            u8::from(self.can_login),
            u8::from(self.is_superuser),
        )
    }
}

/// Single-quotes a value, doubling any quote inside it.
pub fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for c in value.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Creates the role catalogue if it does not exist, and seeds it with the
/// bootstrap superuser.
///
/// Without a first role no one could connect to create one, so the credentials
/// come from configuration. Nothing is seeded when the catalogue already has
/// rows, so a restart does not resurrect a deleted account or reset a password.
pub async fn ensure_role_catalogue(
    database: &rclite::Arc<crate::core::database::Database>,
    bootstrap_user: &str,
    bootstrap_password: Option<&str>,
    method: AuthMethod,
) -> Result<(), String> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    async fn run(
        database: &rclite::Arc<crate::core::database::Database>,
        sql: &str,
    ) -> Result<Vec<u8>, String> {
        let purpose =
            recognize_query_purpose(sql).ok_or_else(|| format!("could not parse: {}", sql))?;
        let response = executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database.clone()).await;
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    }

    if !database.tables.contains_key(ROLE_TABLE) {
        run(database, ROLE_TABLE_DDL).await?;
    }

    let existing = run(database, &format!("SELECT name FROM {}", ROLE_TABLE)).await?;
    let has_roles = existing.first() == Some(&2) && existing.len() > 1;
    if has_roles {
        return Ok(());
    }

    let role = Role {
        name: bootstrap_user.to_string(),
        // A bootstrap user with no password can only be reached under `trust`,
        // which is the point: it is not silently a passwordless superuser on a
        // password-protected server.
        //
        // The verifier has to be one the configured method can actually answer.
        // A SCRAM verifier stores nothing an `md5` challenge can be checked
        // against — PostgreSQL refuses that combination too — so the method
        // chooses the format, the way `password_encryption` does there.
        verifier: bootstrap_password.map(|password| match method {
            AuthMethod::Md5 => md5_verifier(bootstrap_user, password),
            _ => build_scram_verifier(password),
        }),
        can_login: true,
        is_superuser: true,
    };
    run(database, &role.insert_statement()).await?;

    log::info!(
        "Created role catalogue with bootstrap role \"{}\"",
        bootstrap_user
    );
    Ok(())
}

/// Looks a role up by name, or `None` when there is none.
pub async fn lookup_role(
    database: &rclite::Arc<crate::core::database::Database>,
    name: &str,
) -> Option<Role> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    if !database.tables.contains_key(ROLE_TABLE) {
        return None;
    }

    let sql = format!(
        "SELECT name, password, can_login, is_superuser FROM {} WHERE name = {} LIMIT 1",
        ROLE_TABLE,
        quote(name)
    );
    let purpose = recognize_query_purpose(&sql)?;
    let response = executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database.clone()).await;
    if response.first() != Some(&2) {
        return None;
    }

    let rows = crate::core::row::row::vec_into_rows(&response[1..]).ok()?;
    let row = rows.first()?;

    let text = |index: usize| -> Option<String> {
        let column = row.columns.get(index)?;
        if column.column_type == crate::core::db_type::DbType::NULL {
            return None;
        }
        std::str::from_utf8(column.data.into_slice())
            .ok()
            .map(str::to_string)
    };
    let flag = |index: usize| -> bool {
        row.columns
            .get(index)
            .map(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
            .unwrap_or(false)
    };

    Some(Role {
        name: text(0).unwrap_or_else(|| name.to_string()),
        verifier: text(1).map(|stored| Verifier::parse(&stored)),
        can_login: flag(2),
        is_superuser: flag(3),
    })
}

/// Copies every role from one database into another.
///
/// Roles are per database — a database resolves its own `rdb_roles` and
/// authenticates a connection against that, which is why a database name has
/// to be resolved before its connection can be authenticated. What was missing
/// is the other half: nothing gave a *newly created* database any roles, so
/// `CREATE DATABASE` and `IMPORT … INTO` both produced a database that no
/// account could log in to. The name resolved, the password was correct, and
/// the answer was "password authentication failed" — a database you could
/// create and then never open.
///
/// A new database therefore starts with the roles of the one it was created
/// from, which is what an operator expects: the account that could create it
/// can open it. Divergence afterwards is still possible and still per database.
///
/// Verifiers are copied as stored, so no password is re-derived and none has to
/// be known here.
pub async fn copy_role_catalogue(
    from: &rclite::Arc<crate::core::database::Database>,
    to: &rclite::Arc<crate::core::database::Database>,
) -> Result<(), String> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    if !from.tables.contains_key(ROLE_TABLE) {
        // Trust authentication keeps no catalogue, so there is nothing to carry
        // and nothing to refuse.
        return Ok(());
    }
    if to.tables.contains_key(ROLE_TABLE) {
        return Ok(());
    }

    let run = |database: rclite::Arc<crate::core::database::Database>, sql: String| async move {
        let purpose =
            recognize_query_purpose(&sql).ok_or_else(|| format!("could not parse: {}", sql))?;
        let response = executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database).await;
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok::<Vec<u8>, String>(response)
    };

    let response = run(
        from.clone(),
        format!(
            "SELECT name, password, can_login, is_superuser FROM {}",
            ROLE_TABLE
        ),
    )
    .await?;
    if response.first() != Some(&2) {
        return Ok(());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..])
        .map_err(|e| format!("could not read the role catalogue: {}", e))?;

    run(to.clone(), ROLE_TABLE_DDL.to_string()).await?;

    for row in &rows {
        let text = |index: usize| -> Option<String> {
            let column = row.columns.get(index)?;
            if column.column_type == crate::core::db_type::DbType::NULL {
                return None;
            }
            std::str::from_utf8(column.data.into_slice())
                .ok()
                .map(str::to_string)
        };
        let flag = |index: usize| -> bool {
            row.columns
                .get(index)
                .map(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
                .unwrap_or(false)
        };
        let Some(name) = text(0) else { continue };
        let password = match text(1) {
            Some(stored) => quote(&stored),
            None => "NULL".to_string(),
        };
        run(
            to.clone(),
            format!(
                "INSERT INTO {} (name, password, can_login, is_superuser) VALUES ({}, {}, {}, {})",
                ROLE_TABLE,
                quote(&name),
                password,
                u8::from(flag(2)),
                u8::from(flag(3)),
            ),
        )
        .await?;
    }

    Ok(())
}

/// Whether any role could actually log in: one that may, and has a password.
///
/// What decides whether a listener that was given no bootstrap password should
/// still demand one. A database where someone has created an account with a
/// password is a database that expects to be locked; a fresh one is not, and
/// refusing every connection on it would only teach operators to pass
/// `--rdb-auth trust`.
pub async fn has_login_role(database: &rclite::Arc<crate::core::database::Database>) -> bool {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    if !database.tables.contains_key(ROLE_TABLE) {
        return false;
    }

    let sql = format!(
        "SELECT name, password, can_login, is_superuser FROM {}",
        ROLE_TABLE
    );
    let Some(purpose) = recognize_query_purpose(&sql) else {
        return false;
    };
    let response = executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database.clone()).await;
    if response.first() != Some(&2) {
        return false;
    }

    let Ok(rows) = crate::core::row::row::vec_into_rows(&response[1..]) else {
        return false;
    };

    rows.iter().any(|row| {
        let has_password = row
            .columns
            .get(1)
            .map(|c| c.column_type != crate::core::db_type::DbType::NULL)
            .unwrap_or(false);
        let can_login = row
            .columns
            .get(2)
            .map(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
            .unwrap_or(false);
        has_password && can_login
    })
}

/// A parsed `CREATE`/`ALTER`/`DROP` `ROLE`/`USER` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum RoleStatement {
    Create {
        name: String,
        password: Option<String>,
        can_login: bool,
        is_superuser: bool,
    },
    AlterPassword {
        name: String,
        password: Option<String>,
    },
    Drop {
        name: String,
        if_exists: bool,
    },
}

/// Recognises role management SQL, or `None` when the statement is something
/// else.
pub fn parse_role_statement(sql: &str) -> Option<Result<RoleStatement, String>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();

    let is_role_word = |word: &str| word == "ROLE" || word == "USER";
    let mut words = upper.split_whitespace();
    let verb = words.next()?;
    let noun = words.next()?;
    if !is_role_word(noun) {
        return None;
    }

    // Names and passwords are read from the original text, since case matters.
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    match verb {
        "CREATE" => {
            let Some(name) = tokens.get(2).map(|n| n.trim_matches('"').to_string()) else {
                return Some(Err("CREATE ROLE names no role".to_string()));
            };
            let password = match extract_password(trimmed) {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };
            // PostgreSQL's `CREATE USER` implies LOGIN; `CREATE ROLE` does not.
            let can_login = noun == "USER" || upper.contains(" LOGIN");
            Some(Ok(RoleStatement::Create {
                name,
                password,
                can_login: can_login && !upper.contains(" NOLOGIN"),
                is_superuser: upper.contains(" SUPERUSER") && !upper.contains(" NOSUPERUSER"),
            }))
        }
        "ALTER" => {
            let Some(name) = tokens.get(2).map(|n| n.trim_matches('"').to_string()) else {
                return Some(Err("ALTER ROLE names no role".to_string()));
            };
            if !upper.contains("PASSWORD") {
                return Some(Err(
                    "only ALTER ROLE ... PASSWORD is supported yet".to_string()
                ));
            }
            match extract_password(trimmed) {
                Ok(password) => Some(Ok(RoleStatement::AlterPassword { name, password })),
                Err(e) => Some(Err(e)),
            }
        }
        "DROP" => {
            let if_exists = upper.contains("IF EXISTS");
            let index = if if_exists { 4 } else { 2 };
            let Some(name) = tokens.get(index).map(|n| n.trim_matches('"').to_string()) else {
                return Some(Err("DROP ROLE names no role".to_string()));
            };
            Some(Ok(RoleStatement::Drop { name, if_exists }))
        }
        _ => None,
    }
}

/// Reads the quoted value after `PASSWORD`, or `None` for `PASSWORD NULL`.
fn extract_password(sql: &str) -> Result<Option<String>, String> {
    let upper = sql.to_ascii_uppercase();
    let Some(at) = upper.find("PASSWORD") else {
        return Ok(None);
    };
    let rest = sql[at + "PASSWORD".len()..].trim();

    if rest.to_ascii_uppercase().starts_with("NULL") {
        return Ok(None);
    }
    let Some(stripped) = rest.strip_prefix('\'') else {
        return Err("PASSWORD must be given as a quoted string".to_string());
    };

    // A doubled quote inside the literal is an escaped quote.
    let bytes = stripped.as_bytes();
    let mut out = String::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return Ok(Some(out));
        }
        out.push(bytes[i] as char);
        i += 1;
    }

    Err("PASSWORD literal is not terminated".to_string())
}

/// Executes a role statement against the catalogue.
pub async fn execute_role_statement(
    database: &rclite::Arc<crate::core::database::Database>,
    statement: RoleStatement,
    method: AuthMethod,
) -> Result<(), String> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    async fn run(
        database: &rclite::Arc<crate::core::database::Database>,
        sql: &str,
    ) -> Result<(), String> {
        let purpose =
            recognize_query_purpose(sql).ok_or_else(|| format!("could not parse: {}", sql))?;
        let response = executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database.clone()).await;
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(())
    }

    if !database.tables.contains_key(ROLE_TABLE) {
        run(database, ROLE_TABLE_DDL).await?;
    }

    // The verifier format follows the configured method, as
    // `password_encryption` does in PostgreSQL.
    let verifier_for = |name: &str, password: &str| match method {
        AuthMethod::Md5 => md5_verifier(name, password),
        _ => build_scram_verifier(password),
    };

    match statement {
        RoleStatement::Create {
            name,
            password,
            can_login,
            is_superuser,
        } => {
            if lookup_role(database, &name).await.is_some() {
                return Err(format!("role \"{}\" already exists", name));
            }
            let role = Role {
                verifier: password.as_deref().map(|p| verifier_for(&name, p)),
                name,
                can_login,
                is_superuser,
            };
            run(database, &role.insert_statement()).await
        }
        RoleStatement::AlterPassword { name, password } => {
            if lookup_role(database, &name).await.is_none() {
                return Err(format!("role \"{}\" does not exist", name));
            }
            let stored = match password.as_deref() {
                Some(p) => quote(&verifier_for(&name, p).to_stored()),
                None => "NULL".to_string(),
            };
            run(
                database,
                &format!(
                    "UPDATE {} SET password = {} WHERE name = {}",
                    ROLE_TABLE,
                    stored,
                    quote(&name)
                ),
            )
            .await
        }
        RoleStatement::Drop { name, if_exists } => {
            if lookup_role(database, &name).await.is_none() {
                return if if_exists {
                    Ok(())
                } else {
                    Err(format!("role \"{}\" does not exist", name))
                };
            }
            run(
                database,
                &format!("DELETE FROM {} WHERE name = {}", ROLE_TABLE, quote(&name)),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_user_implies_login_and_reads_its_password() {
        let parsed = parse_role_statement("CREATE USER alice PASSWORD 's3cret'")
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed,
            RoleStatement::Create {
                name: "alice".to_string(),
                password: Some("s3cret".to_string()),
                // `CREATE USER` implies LOGIN; `CREATE ROLE` does not.
                can_login: true,
                is_superuser: false,
            }
        );
    }

    #[test]
    fn create_role_does_not_imply_login() {
        let parsed = parse_role_statement("CREATE ROLE reader").unwrap().unwrap();
        assert_eq!(
            parsed,
            RoleStatement::Create {
                name: "reader".to_string(),
                password: None,
                can_login: false,
                is_superuser: false,
            }
        );

        let parsed = parse_role_statement("CREATE ROLE admin LOGIN SUPERUSER PASSWORD 'x'")
            .unwrap()
            .unwrap();
        assert!(matches!(
            parsed,
            RoleStatement::Create {
                can_login: true,
                is_superuser: true,
                ..
            }
        ));
    }

    #[test]
    fn a_quote_inside_a_password_is_read_not_terminated_on() {
        let parsed = parse_role_statement("CREATE USER bob PASSWORD 'o''brien'")
            .unwrap()
            .unwrap();
        match parsed {
            RoleStatement::Create { password, .. } => {
                assert_eq!(password.as_deref(), Some("o'brien"))
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn alter_and_drop_are_recognised() {
        assert_eq!(
            parse_role_statement("ALTER USER alice PASSWORD 'new'")
                .unwrap()
                .unwrap(),
            RoleStatement::AlterPassword {
                name: "alice".to_string(),
                password: Some("new".to_string())
            }
        );
        assert_eq!(
            parse_role_statement("ALTER USER alice PASSWORD NULL")
                .unwrap()
                .unwrap(),
            RoleStatement::AlterPassword {
                name: "alice".to_string(),
                password: None
            }
        );
        assert_eq!(
            parse_role_statement("DROP USER IF EXISTS alice")
                .unwrap()
                .unwrap(),
            RoleStatement::Drop {
                name: "alice".to_string(),
                if_exists: true
            }
        );
    }

    #[test]
    fn statements_that_are_not_about_roles_are_left_alone() {
        assert!(parse_role_statement("SELECT 1").is_none());
        assert!(parse_role_statement("CREATE TABLE users (id INT)").is_none());
        assert!(parse_role_statement("DROP TABLE users").is_none());
    }

    #[test]
    fn an_unquoted_password_is_refused_rather_than_guessed_at() {
        assert!(
            parse_role_statement("CREATE USER bob PASSWORD hunter2")
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn a_role_renders_an_insert_that_quotes_its_values() {
        let role = Role {
            name: "o'brien".to_string(),
            verifier: Some(Verifier::Plain("pw".to_string())),
            can_login: true,
            is_superuser: false,
        };
        let sql = role.insert_statement();
        assert!(
            sql.contains("'o''brien'"),
            "a quote in a name must not escape: {}",
            sql
        );
        assert!(sql.contains("'pw'"));
    }

    #[test]
    fn a_role_without_a_password_stores_null() {
        let role = Role {
            name: "nologin".to_string(),
            verifier: None,
            can_login: false,
            is_superuser: false,
        };
        assert!(role.insert_statement().contains("NULL"));
    }

    /// The worked example from RFC 7677 §3, which pins the whole derivation:
    /// PBKDF2, the key derivation, the auth message, and the signatures.
    #[test]
    fn matches_the_rfc_7677_test_vector() {
        let password = "pencil";
        let salt = BASE64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let iterations = 4096;

        let (stored_key, server_key) = scram_keys(password, &salt, iterations);
        let verifier = Verifier::Scram {
            iterations,
            salt: salt.clone(),
            stored_key,
            server_key,
        };

        let client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let server_first = format!("r={},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", nonce);
        let client_final = format!(
            "c=biws,r={},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            nonce
        );

        // The server's own nonce is random, so the exchange is reconstructed
        // with the RFC's to check the cryptography rather than the randomness.
        let server = ScramServer {
            verifier,
            client_first_bare: client_first[3..].to_string(),
            server_first,
            nonce: nonce.to_string(),
            // base64("n,,"), which is what the RFC's client-final carries.
            expected_cbind: "biws".to_string(),
        };

        assert_eq!(
            server.finish(&client_final).unwrap(),
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn a_verifier_round_trips_through_its_stored_form() {
        let verifier = build_scram_verifier("hunter2");
        let stored = verifier.to_stored();
        assert!(stored.starts_with("SCRAM-SHA-256$4096:"));
        assert_eq!(Verifier::parse(&stored), verifier);
    }

    #[test]
    fn postgres_verifier_formats_are_recognised() {
        assert!(matches!(
            Verifier::parse("md5d41d8cd98f00b204e9800998ecf8427e"),
            Verifier::Md5 { .. }
        ));
        // Not 32 hex digits, so it is a password that happens to start "md5".
        assert!(matches!(Verifier::parse("md5short"), Verifier::Plain(_)));
        assert!(matches!(Verifier::parse("plaintext"), Verifier::Plain(_)));
    }

    #[test]
    fn a_full_exchange_accepts_the_right_password() {
        let verifier = build_scram_verifier("correct horse");
        let client_first = "n,,n=,r=clientnonce123456";
        let (server, server_first) = ScramServer::start(verifier.clone(), client_first).unwrap();

        let nonce = field(&server_first, 'r').unwrap().to_string();
        let salt = BASE64.decode(field(&server_first, 's').unwrap()).unwrap();
        let iterations: u32 = field(&server_first, 'i').unwrap().parse().unwrap();

        let client_final = client_final_for(
            "correct horse",
            &salt,
            iterations,
            "n,,n=,r=clientnonce123456",
            &server_first,
            &nonce,
        );
        assert!(server.finish(&client_final).is_ok());
    }

    #[test]
    fn a_full_exchange_rejects_the_wrong_password() {
        let verifier = build_scram_verifier("correct horse");
        let client_first = "n,,n=,r=clientnonce123456";
        let (server, server_first) = ScramServer::start(verifier, client_first).unwrap();

        let nonce = field(&server_first, 'r').unwrap().to_string();
        let salt = BASE64.decode(field(&server_first, 's').unwrap()).unwrap();
        let iterations: u32 = field(&server_first, 'i').unwrap().parse().unwrap();

        let client_final = client_final_for(
            "wrong horse",
            &salt,
            iterations,
            "n,,n=,r=clientnonce123456",
            &server_first,
            &nonce,
        );
        assert!(server.finish(&client_final).is_err());
    }

    /// A replayed nonce is what a man-in-the-middle would send.
    #[test]
    fn a_mismatched_nonce_is_rejected() {
        let verifier = build_scram_verifier("pw");
        let (server, _) = ScramServer::start(verifier, "n,,n=,r=abc123").unwrap();
        assert!(server.finish("c=biws,r=somethingelse,p=AAAA").is_err());
    }

    /// Asking for channel binding and not getting it must fail, not silently
    /// proceed without the protection the client asked for.
    #[test]
    fn channel_binding_is_refused_rather_than_ignored() {
        let verifier = build_scram_verifier("pw");
        let err = ScramServer::start(verifier, "p=tls-server-end-point,,n=,r=abc").unwrap_err();
        assert!(err.contains("channel binding"), "{}", err);
    }

    /// An unknown user must not be distinguishable from a wrong password: the
    /// exchange runs to completion and fails at the proof.
    #[test]
    fn an_unknown_user_gets_a_stable_decoy_salt() {
        let missing = Verifier::Plain(String::new());
        let (_, first_a) = ScramServer::start(missing.clone(), "n,,n=,r=abc123").unwrap();
        let (_, first_b) = ScramServer::start(missing, "n,,n=,r=abc123").unwrap();
        assert_eq!(
            field(&first_a, 's'),
            field(&first_b, 's'),
            "a random salt would reveal that the user does not exist"
        );
    }

    #[test]
    fn md5_challenge_response_verifies() {
        let verifier = md5_verifier("alice", "s3cret");
        let salt = [0x01u8, 0x02, 0x03, 0x04];

        let response = {
            use md5::Md5;
            let Verifier::Md5 { hex } = &verifier else {
                unreachable!()
            };
            let mut hasher = Md5::new();
            hasher.update(hex.as_bytes());
            hasher.update(salt);
            format!("md5{}", hex_lower(&hasher.finalize()))
        };

        assert!(verify_md5(&verifier, "alice", &salt, &response));
        assert!(!verify_md5(
            &verifier,
            "alice",
            &salt,
            "md500000000000000000000000000000000"
        ));
    }

    /// A SCRAM verifier stores nothing an md5 challenge can be answered from.
    #[test]
    fn md5_cannot_be_answered_from_a_scram_verifier() {
        let verifier = build_scram_verifier("pw");
        assert!(!verify_md5(&verifier, "alice", &[0; 4], "md5whatever"));
    }

    #[test]
    fn cleartext_verifies_against_every_verifier_shape() {
        assert!(verify_password(&build_scram_verifier("pw"), "alice", "pw"));
        assert!(!verify_password(
            &build_scram_verifier("pw"),
            "alice",
            "nope"
        ));
        assert!(verify_password(&md5_verifier("alice", "pw"), "alice", "pw"));
        assert!(!verify_password(
            &md5_verifier("alice", "pw"),
            "alice",
            "nope"
        ));
        assert!(verify_password(
            &Verifier::Plain("pw".into()),
            "alice",
            "pw"
        ));
        assert!(!verify_password(
            &Verifier::Plain("pw".into()),
            "alice",
            "nope"
        ));
    }

    #[test]
    fn methods_parse_from_their_configuration_names() {
        assert_eq!(
            AuthMethod::parse("scram-sha-256"),
            Some(AuthMethod::ScramSha256)
        );
        assert_eq!(AuthMethod::parse("SCRAM"), Some(AuthMethod::ScramSha256));
        assert_eq!(AuthMethod::parse("md5"), Some(AuthMethod::Md5));
        assert_eq!(AuthMethod::parse("trust"), Some(AuthMethod::Trust));
        assert_eq!(AuthMethod::parse("nonsense"), None);
    }

    /// The two halves have to agree, or nothing authenticates. Driving the
    /// real client against the real server is the only test that checks that.
    #[test]
    fn the_client_and_server_halves_complete_an_exchange() {
        let verifier = build_scram_verifier("hunter2");
        let (mut client, client_first) =
            ScramClient::start("alice", "hunter2", ChannelBinding::None);
        let (server, server_first) = ScramServer::start(verifier, &client_first).unwrap();
        let client_final = client.respond(&server_first).unwrap();
        let server_final = server.finish(&client_final).unwrap();
        client.verify(&server_final).unwrap();
    }

    #[test]
    fn the_client_refuses_a_wrong_password_at_the_server() {
        let verifier = build_scram_verifier("hunter2");
        let (mut client, client_first) =
            ScramClient::start("alice", "not hunter2", ChannelBinding::None);
        let (server, server_first) = ScramServer::start(verifier, &client_first).unwrap();
        let client_final = client.respond(&server_first).unwrap();
        assert!(server.finish(&client_final).is_err());
    }

    /// The check that makes the client's half worth having: a server that does
    /// not know the verifier cannot produce the final message, so an
    /// impersonator is caught even after the client has sent its proof.
    #[test]
    fn the_client_refuses_a_server_that_cannot_prove_it_knows_the_verifier() {
        let (mut client, client_first) =
            ScramClient::start("alice", "hunter2", ChannelBinding::None);
        let (server, server_first) =
            ScramServer::start(build_scram_verifier("hunter2"), &client_first).unwrap();
        let _ = client.respond(&server_first).unwrap();
        let _ = server;

        assert!(
            client
                .verify("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                .is_err(),
            "a fabricated server signature must not be accepted"
        );
    }

    #[test]
    fn a_bound_exchange_completes_when_both_ends_see_the_same_connection() {
        let binding = ChannelBinding::TlsExporter(vec![7u8; 32]);
        let verifier = build_scram_verifier("hunter2");

        let (mut client, client_first) = ScramClient::start("alice", "hunter2", binding.clone());
        let (server, server_first) =
            ScramServer::start_bound(verifier, &client_first, &binding).unwrap();
        let client_final = client.respond(&server_first).unwrap();
        let server_final = server.finish(&client_final).unwrap();
        client.verify(&server_final).unwrap();
    }

    /// The attack channel binding exists to stop: the messages verify on the
    /// connection they were made for and nowhere else.
    #[test]
    fn a_bound_exchange_relayed_onto_another_connection_fails() {
        let clients_view = ChannelBinding::TlsExporter(vec![1u8; 32]);
        let servers_view = ChannelBinding::TlsExporter(vec![2u8; 32]);
        let verifier = build_scram_verifier("hunter2");

        let (mut client, client_first) = ScramClient::start("alice", "hunter2", clients_view);
        let (server, server_first) =
            ScramServer::start_bound(verifier, &client_first, &servers_view).unwrap();
        let client_final = client.respond(&server_first).unwrap();

        assert!(
            server.finish(&client_final).is_err(),
            "a proof made against one connection must not verify on another"
        );
    }

    /// Stripping the binding is the other half of the same attack.
    #[test]
    fn a_server_that_binds_refuses_a_client_that_does_not() {
        let binding = ChannelBinding::TlsExporter(vec![3u8; 32]);
        let (_, unbound_first) = ScramClient::start("alice", "pw", ChannelBinding::None);

        let err = ScramServer::start_bound(build_scram_verifier("pw"), &unbound_first, &binding)
            .unwrap_err();
        assert!(err.contains("requires channel binding"), "{}", err);
    }

    /// A rewritten GS2 header has to fail at the proof, not pass unnoticed.
    #[test]
    fn a_rewritten_channel_binding_field_is_rejected() {
        let verifier = build_scram_verifier("pw");
        let (mut client, client_first) = ScramClient::start("alice", "pw", ChannelBinding::None);
        let (server, server_first) = ScramServer::start(verifier, &client_first).unwrap();
        let client_final = client.respond(&server_first).unwrap();

        let tampered = client_final.replacen("c=biws", "c=eSws", 1);
        assert!(server.finish(&tampered).is_err());
    }

    /// A server that asks for a cheap iteration count is asking for a captured
    /// exchange to be cheap to attack offline.
    #[test]
    fn the_client_refuses_a_weak_iteration_count() {
        let (mut client, _) = ScramClient::start("alice", "pw", ChannelBinding::None);
        let server_first = format!(
            "r={}extra,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=1",
            client.client_nonce
        );
        let err = client.respond(&server_first).unwrap_err();
        assert!(err.contains("iteration count"), "{}", err);
    }

    /// A server-first that does not extend the client's nonce is a replay of
    /// some other exchange.
    #[test]
    fn the_client_refuses_a_nonce_that_is_not_its_own() {
        let (mut client, _) = ScramClient::start("alice", "pw", ChannelBinding::None);
        let server_first = "r=somebodyelsesnonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert!(client.respond(server_first).is_err());
    }

    #[test]
    fn a_user_name_with_structural_characters_is_escaped() {
        let (_, client_first) = ScramClient::start("a,b=c", "pw", ChannelBinding::None);
        assert!(client_first.contains("n=a=2Cb=3Dc"), "{}", client_first);
    }

    /// Reproduces what a client computes, so the exchange can be driven from
    /// both sides in a test.
    fn client_final_for(
        password: &str,
        salt: &[u8],
        iterations: u32,
        client_first: &str,
        server_first: &str,
        nonce: &str,
    ) -> String {
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);

        let without_proof = format!("c=biws,r={}", nonce);
        let auth_message = format!("{},{},{}", &client_first[3..], server_first, without_proof);
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        format!("{},p={}", without_proof, BASE64.encode(proof))
    }
}
