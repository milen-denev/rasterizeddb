//! The client side of SCRAM-SHA-256 (RFC 5802, RFC 7677), bound to the
//! connection it runs on (RFC 9266 `tls-exporter`, or rastcp's unix-socket
//! binding).
//!
//! A copy of the server's `auth::ScramClient` (`rasterizeddb_core/src/auth.rs`),
//! kept here so this crate does not compile the engine. `tests/sdk_live.rs`
//! logs in with it against a real server, so the two cannot drift apart
//! unnoticed.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Iteration counts below this are refused.
///
/// The salt and iteration count come from the server, so a hostile or
/// downgraded one could ask for `i=1` and make a captured exchange cheap to
/// attack offline. PostgreSQL's own default is the floor.
const MIN_ITERATIONS: u32 = 4096;
/// The most a server may ask for. The server's own count is 4096; a
/// hostile one (or anyone in the middle of a connection that does not pin
/// the server's certificate) could otherwise ask for 4 billion and hold this
/// client in PBKDF2 for hours. A million is about a second.
const MAX_ITERATIONS: u32 = 1_000_000;

const NONCE_LEN: usize = 18;

/// One exchange, from the client-first message to the server's proof.
pub(crate) struct ScramClient {
    password: String,
    /// The binding data (empty when unbound).
    binding: Vec<u8>,
    gs2_header: String,
    client_first_bare: String,
    client_nonce: String,
    /// Set by [`ScramClient::respond`], checked by [`ScramClient::verify`].
    server_signature: Option<Vec<u8>>,
}

impl ScramClient {
    /// Begins an exchange bound to `binding` (the rastcp channel's binding
    /// data), returning the client-first message to send.
    pub(crate) fn start(
        user: &str,
        password: &str,
        binding: &[u8],
    ) -> std::io::Result<(Self, String)> {
        // rastcp always hands over binding data, TLS or unix socket, and the
        // server insists on it; an empty one would be refused there.
        let gs2_header = "p=tls-exporter,,".to_string();
        let client_nonce = make_nonce()?;
        let client_first_bare = format!("n={},r={}", saslname(user), client_nonce);
        let client_first = format!("{gs2_header}{client_first_bare}");
        Ok((
            ScramClient {
                password: password.to_string(),
                binding: binding.to_vec(),
                gs2_header,
                client_first_bare,
                client_nonce,
                server_signature: None,
            },
            client_first,
        ))
    }

    /// Answers the server's first message with the client's final one.
    pub(crate) fn respond(&mut self, server_first: &str) -> Result<String, String> {
        let nonce = field(server_first, 'r')
            .ok_or_else(|| "SCRAM server-first has no nonce".to_string())?;
        // The server's nonce must extend the client's: otherwise a replayed
        // server-first from an earlier exchange would be accepted.
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
        if iterations < MIN_ITERATIONS {
            return Err(format!(
                "SCRAM iteration count {iterations} is below the {MIN_ITERATIONS} this client accepts"
            ));
        }
        if iterations > MAX_ITERATIONS {
            return Err(format!(
                "SCRAM iteration count {iterations} is above the {MAX_ITERATIONS} this client accepts"
            ));
        }

        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(self.password.as_bytes(), &salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(&client_key).to_vec();
        let server_key = hmac(&salted, b"Server Key");

        let mut cbind = self.gs2_header.as_bytes().to_vec();
        cbind.extend_from_slice(&self.binding);
        let without_proof = format!("c={},r={}", BASE64.encode(cbind), nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, without_proof
        );

        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&client_signature)
            .map(|(k, s)| k ^ s)
            .collect();
        self.server_signature = Some(hmac(&server_key, auth_message.as_bytes()));

        Ok(format!("{without_proof},p={}", BASE64.encode(proof)))
    }

    /// Checks the server's final message, which proves the server holds the
    /// stored verifier rather than having relayed the exchange.
    pub(crate) fn verify(&self, server_final: &str) -> Result<(), String> {
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
        // Constant time: every byte is compared whatever the first difference.
        let same = signature.len() == expected.len()
            && signature
                .iter()
                .zip(expected)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        if same {
            Ok(())
        } else {
            Err("the server did not prove it knows this password".to_string())
        }
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Escapes a user name for the `n=` field, where `,` and `=` are structural.
fn saslname(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// A nonce of printable characters from the operating system's generator.
fn make_nonce() -> std::io::Result<String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut nonce = String::with_capacity(NONCE_LEN);
    let mut bytes = [0u8; 64];
    while nonce.len() < NONCE_LEN {
        crate::types::random_bytes(&mut bytes)?;
        // Rejection sampling: 248 is the largest multiple of 62 under 256, so
        // every character is equally likely.
        for &b in &bytes {
            if nonce.len() == NONCE_LEN {
                break;
            }
            if (b as usize) < 248 {
                nonce.push(ALPHABET[b as usize % ALPHABET.len()] as char);
            }
        }
    }
    Ok(nonce)
}

/// Reads a `key=value` field out of a comma-separated SCRAM message.
fn field(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| {
        let mut chars = part.chars();
        (chars.next() == Some(key) && chars.next() == Some('=')).then(|| &part[2..])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_name_is_escaped() {
        assert_eq!(saslname("a,b=c"), "a=2Cb=3Dc");
    }

    #[test]
    fn the_nonce_is_fresh_and_printable() {
        let a = make_nonce().unwrap();
        let b = make_nonce().unwrap();
        assert_eq!(a.len(), NONCE_LEN);
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(a, b);
    }

    #[test]
    fn a_server_nonce_that_does_not_extend_ours_is_refused() {
        let (mut client, _) = ScramClient::start("alice", "pw", b"bind").unwrap();
        let err = client
            .respond("r=someoneelse,s=c2FsdA==,i=4096")
            .unwrap_err();
        assert!(err.contains("extend"), "{err}");
    }

    #[test]
    fn a_low_iteration_count_is_refused() {
        let (mut client, _) = ScramClient::start("alice", "pw", b"bind").unwrap();
        let nonce = format!("{}xyz", client.client_nonce);
        let err = client
            .respond(&format!("r={nonce},s=c2FsdA==,i=1"))
            .unwrap_err();
        assert!(err.contains("iteration"), "{err}");
    }

    /// A count a server could use to hold the client in PBKDF2 for hours is
    /// refused before any hashing.
    #[test]
    fn a_huge_iteration_count_is_refused() {
        let (mut client, _) = ScramClient::start("alice", "pw", b"bind").unwrap();
        let nonce = format!("{}xyz", client.client_nonce);
        let started = std::time::Instant::now();
        let err = client
            .respond(&format!("r={nonce},s=c2FsdA==,i=4294967295"))
            .unwrap_err();
        assert!(err.contains("above"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn a_wrong_server_signature_is_refused() {
        let (mut client, _) = ScramClient::start("alice", "pw", b"bind").unwrap();
        let nonce = format!("{}xyz", client.client_nonce);
        client
            .respond(&format!("r={nonce},s=c2FsdA==,i=4096"))
            .unwrap();
        assert!(client.verify("v=AAAA").is_err());
        assert!(client.verify("e=other-error").is_err());
    }

    /// The exchange against the server's own arithmetic: a verifier built the
    /// way the server stores one accepts this client's proof, and the
    /// server-final it would send verifies here.
    #[test]
    fn a_proof_verifies_against_a_stored_verifier() {
        let salt = b"0123456789abcdef";
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(b"hunter2", salt, 4096, &mut salted);
        let stored_key = Sha256::digest(hmac(&salted, b"Client Key")).to_vec();
        let server_key = hmac(&salted, b"Server Key");

        let (mut client, client_first) = ScramClient::start("alice", "hunter2", b"bind").unwrap();
        let client_first_bare = client_first.strip_prefix("p=tls-exporter,,").unwrap();
        let server_first = format!(
            "r={}srv,s={},i=4096",
            client.client_nonce,
            BASE64.encode(salt)
        );
        let client_final = client.respond(&server_first).unwrap();

        let (without_proof, proof) = client_final.rsplit_once(",p=").unwrap();
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let signature = hmac(&stored_key, auth_message.as_bytes());
        let client_key: Vec<u8> = BASE64
            .decode(proof)
            .unwrap()
            .iter()
            .zip(&signature)
            .map(|(p, s)| p ^ s)
            .collect();
        assert_eq!(Sha256::digest(&client_key).to_vec(), stored_key);

        let server_final = format!(
            "v={}",
            BASE64.encode(hmac(&server_key, auth_message.as_bytes()))
        );
        client.verify(&server_final).unwrap();
    }
}
