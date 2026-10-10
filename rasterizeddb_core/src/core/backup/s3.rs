//! A small S3 client: SigV4 signing over a pooled HTTP/1.1 connection.
//!
//! # Why not an SDK
//!
//! The two things this module has to be good at are *not* spending CPU and not
//! spending requests, and both are easier to guarantee from here than through a
//! general-purpose client. Uploads go out with `UNSIGNED-PAYLOAD`, which is what
//! saves a full SHA-256 pass over every byte of a multi-gigabyte snapshot;
//! connections and derived signing keys are pooled, so a burst of part uploads
//! pays one TLS handshake and one key derivation between them; and every call
//! site here is one the backup code chose deliberately, so there is no retry
//! layer quietly turning one logical operation into five requests.
//!
//! It also costs nothing to carry. The transport is `rustls`/`tokio-rustls` and
//! the signing is `sha2`/`hmac`/`base64` — all of which the pgwire listener
//! already required — so enabling `s3_backup` adds a compressor and a root
//! certificate bundle, not a dependency tree.
//!
//! # Compatibility
//!
//! Plain SigV4 against the REST API, so anything speaking S3 works: AWS,
//! MinIO, Ceph RGW, Cloudflare R2, Backblaze B2, Wasabi. Path-style addressing
//! is available for the ones that do not do virtual-host buckets.

use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, KeyInit, Mac};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::timeutil;

type HmacSha256 = Hmac<Sha256>;

/// Value that tells S3 to accept a body it has not hashed.
///
/// Legal over HTTPS, where the transport already authenticates the bytes. It is
/// the single biggest CPU saving in the upload path: without it every byte of
/// every snapshot would be hashed once for the signature on top of being
/// compressed.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// SHA-256 of the empty string, for requests that carry no body.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How long an idle pooled connection is trusted before being reopened.
///
/// S3 closes idle connections at around 20 seconds; reaching for one it has
/// already dropped costs a failed write and a retry, which is more expensive
/// than the handshake it was meant to save.
const IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// Longest one attempt of a request may take before it counts as failed (and
/// is retried, like any other failure), plus [`REQUEST_TIME_PER_BODY_BYTE`]
/// for the body it sends.
///
/// There was no limit: a connection the network blackholed — a dead NAT
/// entry, a load balancer that stopped answering — held the caller forever,
/// and for the object-store backend the caller is a commit.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// One second per 256 KiB sent, so a large part upload on a slow link is not
/// cut off by the flat limit.
const REQUEST_BODY_BYTES_PER_SEC: u64 = 256 * 1024;

/// How long a streamed download may go without a single byte arriving.
///
/// An idle limit rather than a whole-transfer one: a restore of a large
/// database legitimately streams for hours, and only a stall should end it.
/// A stalled attempt fails like a dropped connection, so
/// `get_object_streaming` resumes it with a ranged request.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Ceiling on a pooled response body held in memory. Bodies this module reads
/// whole are manifests and XML replies; a snapshot is streamed instead.
const MAX_INLINE_BODY: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    /// Set for temporary credentials (STS, IRSA, instance roles).
    pub session_token: Option<String>,
}

impl Credentials {
    /// Reads the standard AWS environment variables.
    pub fn from_env() -> Option<Self> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        Some(Self {
            access_key,
            secret_key,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        })
    }
}

/// Where the client connects and how it addresses buckets.
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

impl Endpoint {
    /// Parses `https://host[:port]`, defaulting the scheme to HTTPS.
    pub fn parse(url: &str) -> Result<Self, String> {
        let (tls, rest) = match url.split_once("://") {
            Some(("https", rest)) => (true, rest),
            Some(("http", rest)) => (false, rest),
            Some((scheme, _)) => return Err(format!("unsupported scheme '{scheme}'")),
            None => (true, url),
        };
        let rest = rest.trim_end_matches('/');
        let rest = rest.split('/').next().unwrap_or(rest);
        if rest.is_empty() {
            return Err("endpoint has no host".to_string());
        }

        // Only the last colon separates a port, so an IPv6 literal in brackets
        // is left alone.
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') && p.chars().all(|c| c.is_ascii_digit()) => (
                h.to_string(),
                p.parse::<u16>()
                    .map_err(|_| format!("bad port in '{url}'"))?,
            ),
            _ => (rest.to_string(), if tls { 443 } else { 80 }),
        };

        Ok(Self { host, port, tls })
    }

    fn authority(&self) -> String {
        let default = if self.tls { 443 } else { 80 };
        if self.port == default {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: Endpoint,
    pub region: String,
    pub bucket: String,
    pub credentials: Credentials,
    /// `https://host/bucket/key` instead of `https://bucket.host/key`. Required
    /// by MinIO and by any bucket whose name is not DNS-safe.
    pub path_style: bool,
    /// Attempts per request, including the first.
    pub max_attempts: u32,
}

/// One pooled, still-open connection.
struct Pooled {
    stream: Stream,
    since: Instant,
}

/// A plaintext or TLS connection, behind one interface.
enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
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

    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf).await,
            Stream::Tls(s) => s.read(buf).await,
        }
    }

    /// Reads at most `max` bytes onto the end of `buf`, into its spare
    /// capacity: nothing is zeroed first and nothing is copied afterwards.
    /// Returns how many arrived; 0 is end of stream.
    async fn read_into(&mut self, buf: &mut Vec<u8>, max: usize) -> io::Result<usize> {
        use tokio::io::{AsyncRead, ReadBuf};
        buf.reserve(max);
        let n = {
            let spare = &mut buf.spare_capacity_mut()[..max];
            let mut target = ReadBuf::uninit(spare);
            std::future::poll_fn(|cx| match self {
                Stream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, &mut target),
                Stream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, &mut target),
            })
            .await?;
            target.filled().len()
        };
        // SAFETY: `poll_read` reports in `filled` exactly the prefix of the
        // spare capacity it initialised, and `n <= max <= spare capacity`.
        unsafe { buf.set_len(buf.len() + n) };
        Ok(n)
    }
}

/// The most a streamed body is read in one go.
const READ_CHUNK: usize = 128 * 1024;

pub struct S3Client {
    config: S3Config,
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Idle connections, newest last.
    pool: Mutex<Vec<Pooled>>,
    /// The derived signing key and the day it belongs to. Deriving it is four
    /// HMACs; a day's worth of part uploads should pay for them once.
    signing_key: Mutex<Option<(String, [u8; 32])>>,
    /// See [`REQUEST_TIMEOUT`]. A field so a test can shorten it.
    request_timeout: Duration,
    /// See [`STREAM_IDLE_TIMEOUT`]. A field so a test can shorten it.
    stream_idle_timeout: Duration,
}

/// A streaming download that stopped early, and how far it got.
struct StreamFailure {
    /// Bytes already handed to the sink, which a resume must not repeat.
    consumed: u64,
    error: io::Error,
}

/// What a request came back with.
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

impl S3Client {
    pub fn new(config: S3Config) -> io::Result<Self> {
        let tls = if config.endpoint.tls {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            Some(Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            tls,
            pool: Mutex::new(Vec::new()),
            signing_key: Mutex::new(None),
            request_timeout: REQUEST_TIMEOUT,
            stream_idle_timeout: STREAM_IDLE_TIMEOUT,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Overrides the per-attempt request deadline ([`REQUEST_TIMEOUT`] by
    /// default). For tests and tooling that talk to a deliberately silent
    /// endpoint; production keeps the default.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Host header for this request, which virtual-host addressing folds the
    /// bucket into.
    fn host_header(&self) -> String {
        if self.config.path_style {
            self.config.endpoint.authority()
        } else {
            format!(
                "{}.{}",
                self.config.bucket,
                self.config.endpoint.authority()
            )
        }
    }

    /// Path component, which path-style addressing folds the bucket into.
    fn request_path(&self, key: &str) -> String {
        let encoded = encode_key(key);
        if self.config.path_style {
            format!("/{}/{}", self.config.bucket, encoded)
        } else {
            format!("/{encoded}")
        }
    }

    // ---- object operations -------------------------------------------------

    /// Uploads a whole object in one request.
    pub async fn put_object(&self, key: &str, body: &[u8]) -> io::Result<String> {
        let response = self.send("PUT", key, &[], &[], body, None).await?;
        expect_ok(&response, "PUT", key)?;
        Ok(response
            .header("etag")
            .unwrap_or_default()
            .trim_matches('"')
            .to_string())
    }

    /// Fetches a whole object.
    pub async fn get_object(&self, key: &str) -> io::Result<Vec<u8>> {
        let response = self.send("GET", key, &[], &[], &[], None).await?;
        expect_ok(&response, "GET", key)?;
        Ok(response.body)
    }

    /// Fetches a whole object, or `None` if there is no such key.
    ///
    /// A missing object is an ordinary answer for a store used as a filesystem:
    /// a sparse file's unwritten chunks are simply absent.
    pub async fn get_object_opt(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let response = self.send("GET", key, &[], &[], &[], None).await?;
        if response.status == 404 {
            return Ok(None);
        }
        expect_ok(&response, "GET", key)?;
        Ok(Some(response.body))
    }

    /// Fetches `len` bytes from `offset`, or `None` if there is no such key.
    ///
    /// Returns fewer bytes than asked for when the object ends first, and an
    /// empty vector when `offset` is already past the end — the same answers a
    /// `pread` gives, which is what the caller is emulating.
    pub async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        len: usize,
    ) -> io::Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let last = offset + len as u64 - 1;
        let range = format!("bytes={offset}-{last}");
        let response = self
            .send("GET", key, &[], &[("range", range.as_str())], &[], None)
            .await?;
        match response.status {
            404 => Ok(None),
            // The range starts past the end of the object.
            416 => Ok(Some(Vec::new())),
            206 => Ok(Some(response.body)),
            200 => {
                // The store ignored the range and sent the whole object. Slicing
                // client-side is right rather than an error: the answer is still
                // correct, only more expensive, and some S3-compatible stores do
                // this for small objects.
                let start = (offset as usize).min(response.body.len());
                let end = start.saturating_add(len).min(response.body.len());
                Ok(Some(response.body[start..end].to_vec()))
            }
            _ => Err(s3_error("GET range", key, response.status, &response.body)),
        }
    }

    /// Every key under `prefix`, with its size, following continuations.
    pub async fn list_prefix(&self, prefix: &str) -> io::Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query: Vec<(&str, &str)> = vec![("list-type", "2"), ("prefix", prefix)];
            if let Some(t) = token.as_deref() {
                query.push(("continuation-token", t));
            }
            let response = self.send("GET", "", &query, &[], &[], None).await?;
            expect_ok(&response, "GET ?list-type=2", prefix)?;

            let text = String::from_utf8_lossy(&response.body);
            for chunk in text.split("<Contents>").skip(1) {
                let Some(entry) = chunk.split("</Contents>").next() else {
                    continue;
                };
                let Some(key) = xml_tag(entry, "Key") else {
                    continue;
                };
                let size = xml_tag(entry, "Size")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                out.push((xml_unescape(key), size));
            }

            let truncated = xml_tag(&text, "IsTruncated").is_some_and(|v| v.trim() == "true");
            // MinIO has been seen to echo a prefix back and return nothing on
            // other listing APIs, so the continuation is trusted only when the
            // listing says it is truncated *and* names a token.
            match (truncated, xml_tag(&text, "NextContinuationToken")) {
                (true, Some(next)) => token = Some(xml_unescape(next)),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Fetches an object, handing each chunk to `sink` as it arrives.
    ///
    /// Nothing is buffered whole, so a snapshot larger than memory restores
    /// fine. `sink` runs on the runtime thread, so it should hand work off
    /// rather than decompress inline.
    pub async fn get_object_streaming(
        &self,
        key: &str,
        mut sink: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<u64> {
        // A dropped connection part-way through is resumed with a ranged
        // request rather than restarting, because the caller has already
        // consumed what arrived and cannot un-consume it. Without this a single
        // blip fails a whole restore, and the larger the database the more
        // likely that becomes — which is exactly backwards from what a restore
        // needs.
        let mut delivered = 0u64;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match self.stream_once(key, delivered, &mut sink).await {
                Ok(read) => return Ok(delivered + read),
                Err(StreamFailure { consumed, error }) => {
                    delivered += consumed;

                    // A failure the server chose to report is not transient.
                    if error.kind() == io::ErrorKind::NotFound
                        || error.kind() == io::ErrorKind::PermissionDenied
                        || attempt >= self.config.max_attempts
                    {
                        return Err(error);
                    }

                    log::warn!(
                        "S3 GET {} interrupted after {} bytes (attempt {}/{}): {}; resuming",
                        key,
                        delivered,
                        attempt,
                        self.config.max_attempts,
                        error
                    );
                    self.backoff(attempt).await;
                }
            }
        }
    }

    /// One attempt at streaming `key` from byte `offset`.
    ///
    /// Reports how much it managed to hand to `sink` before failing, so the
    /// caller knows where to resume from.
    async fn stream_once(
        &self,
        key: &str,
        offset: u64,
        sink: &mut impl FnMut(&[u8]) -> io::Result<()>,
    ) -> Result<u64, StreamFailure> {
        // Held for the whole call so the signed header borrows something that
        // outlives the request.
        let range = format!("bytes={offset}-");
        let ranged = [("range", range.as_str())];
        let extra: &[(&str, &str)] = if offset == 0 { &[] } else { &ranged };

        let mut fail = |e: io::Error| StreamFailure {
            consumed: 0,
            error: e,
        };

        let idle = self.stream_idle_timeout;
        let stalled = || {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no progress on GET {key} for {idle:?}"),
            )
        };
        let mut stream = tokio::time::timeout(idle, self.connect())
            .await
            .map_err(|_| stalled())
            .and_then(|r| r)
            .map_err(&mut fail)?;
        let request = self
            .sign_request("GET", key, &[], extra, 0, None)
            .map_err(&mut fail)?;
        tokio::time::timeout(idle, async {
            stream.write_all(&request).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| stalled())
        .and_then(|r| r)
        .map_err(&mut fail)?;

        let mut reader = ResponseReader::new();
        reader.idle = Some(idle);
        let head = reader.read_head(&mut stream).await.map_err(&mut fail)?;

        if head.status / 100 != 2 {
            let body = reader
                .read_body(&mut stream, &head)
                .await
                .unwrap_or_default();
            return Err(StreamFailure {
                consumed: 0,
                error: s3_error("GET", key, head.status, &body),
            });
        }

        // A resumed request that comes back 200 rather than 206 is sending the
        // whole object again. Feeding that to the sink would duplicate
        // everything already delivered, so it is refused: a corrupt restore is
        // worse than a failed one.
        if offset > 0 && head.status != 206 {
            return Err(StreamFailure {
                consumed: 0,
                error: io::Error::other(format!(
                    "S3 ignored the range request for {key} (returned {}),                      so the download cannot be resumed safely",
                    head.status
                )),
            });
        }

        // Counted inside the sink wrapper rather than from the return value,
        // because a failure part-way through still has to report what it fed.
        let mut consumed = 0u64;
        let outcome = {
            let mut counting = |chunk: &[u8]| {
                sink(chunk)?;
                consumed += chunk.len() as u64;
                Ok(())
            };
            reader.stream_body(&mut stream, &head, &mut counting).await
        };

        match outcome {
            Ok(_) => {
                if head.keep_alive {
                    self.release(stream);
                }
                Ok(consumed)
            }
            Err(error) => Err(StreamFailure { consumed, error }),
        }
    }

    /// Removes up to 1000 keys in a single request.
    ///
    /// Retention pruning would otherwise be one DELETE per object, which on a
    /// long-lived database is by far the most requests the backup ever makes.
    pub async fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        for chunk in keys.chunks(1000) {
            let mut xml = String::from("<Delete><Quiet>true</Quiet>");
            for key in chunk {
                xml.push_str("<Object><Key>");
                xml_escape_into(key, &mut xml);
                xml.push_str("</Key></Object>");
            }
            xml.push_str("</Delete>");

            let body = xml.into_bytes();
            let digest = base64_md5(&body);
            let response = self
                .send(
                    "POST",
                    "",
                    &[("delete", "")],
                    &[("content-md5", digest.as_str())],
                    &body,
                    None,
                )
                .await?;
            expect_ok(&response, "POST ?delete", "")?;
            // A multi-object delete answers 200 whatever happened to each key;
            // the failures are `<Error>` entries in the body (in quiet mode
            // they are all it lists). Reading only the status reported keys
            // that were never deleted as deleted — for the object-store
            // backend, a dropped table's objects left under a name that a new
            // table of the same name then reads back after a restart.
            let text = String::from_utf8_lossy(&response.body);
            if let Some(start) = text.find("<Error>") {
                let entry = &text[start..];
                let failed = text.matches("<Error>").count();
                return Err(io::Error::other(format!(
                    "S3 did not delete {failed} of {} objects; first {}: {}",
                    chunk.len(),
                    xml_tag(entry, "Key").map(xml_unescape).unwrap_or_default(),
                    xml_tag(entry, "Message")
                        .or_else(|| xml_tag(entry, "Code"))
                        .unwrap_or("unknown error")
                )));
            }
        }
        Ok(())
    }

    // ---- multipart ---------------------------------------------------------

    pub async fn create_multipart_upload(&self, key: &str) -> io::Result<String> {
        let response = self
            .send("POST", key, &[("uploads", "")], &[], &[], None)
            .await?;
        expect_ok(&response, "POST ?uploads", key)?;
        let text = String::from_utf8_lossy(&response.body);
        xml_tag(&text, "UploadId")
            .map(str::to_string)
            .ok_or_else(|| io::Error::other(format!("S3 returned no UploadId for {key}")))
    }

    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> io::Result<String> {
        let part = part_number.to_string();
        let response = self
            .send(
                "PUT",
                key,
                &[("partNumber", part.as_str()), ("uploadId", upload_id)],
                &[],
                body,
                None,
            )
            .await?;
        expect_ok(&response, "PUT part", key)?;
        response
            .header("etag")
            .map(|e| e.trim_matches('"').to_string())
            .ok_or_else(|| io::Error::other(format!("S3 returned no ETag for part {part_number}")))
    }

    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> io::Result<()> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            xml.push_str("<Part><PartNumber>");
            xml.push_str(&number.to_string());
            xml.push_str("</PartNumber><ETag>&quot;");
            xml_escape_into(etag, &mut xml);
            xml.push_str("&quot;</ETag></Part>");
        }
        xml.push_str("</CompleteMultipartUpload>");

        let response = self
            .send(
                "POST",
                key,
                &[("uploadId", upload_id)],
                &[],
                xml.as_bytes(),
                None,
            )
            .await?;
        expect_ok(&response, "POST complete", key)?;

        // S3 can return 200 with an error document: the connection is held open
        // while the parts are assembled, so the status line is written before
        // the outcome is known. Treating that as success is how a multipart
        // upload silently ends up missing.
        let text = String::from_utf8_lossy(&response.body);
        if text.contains("<Error>") {
            return Err(io::Error::other(format!(
                "S3 failed to assemble {key}: {}",
                xml_tag(&text, "Message").unwrap_or("unknown error")
            )));
        }
        Ok(())
    }

    /// Every multipart upload still in progress whose key starts with `prefix`.
    ///
    /// Returns `(key, upload_id)` pairs.
    ///
    /// The filtering happens here rather than at the server. `prefix` is a
    /// documented parameter of `ListMultipartUploads`, but MinIO echoes it back
    /// in the response and then ignores it, returning nothing — so a caller
    /// trusting the server-side filter would conclude there were no orphans and
    /// leave every one of them in place. Listing unfiltered and matching
    /// locally costs the same one request and behaves the same everywhere.
    ///
    /// Only the first page is read: this clears orphans at start-up, and a
    /// bucket with more than a page of them has a problem paging does not fix.
    pub async fn list_multipart_uploads(&self, prefix: &str) -> io::Result<Vec<(String, String)>> {
        let response = self
            .send("GET", "", &[("uploads", "")], &[], &[], None)
            .await?;
        expect_ok(&response, "GET ?uploads", prefix)?;

        let text = String::from_utf8_lossy(&response.body);
        let mut found = Vec::new();
        for chunk in text.split("<Upload>").skip(1) {
            let Some(entry) = chunk.split("</Upload>").next() else {
                continue;
            };
            if let (Some(key), Some(id)) = (xml_tag(entry, "Key"), xml_tag(entry, "UploadId")) {
                let key = xml_unescape(key);
                if key.starts_with(prefix) {
                    found.push((key, id.to_string()));
                }
            }
        }
        Ok(found)
    }

    pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> io::Result<()> {
        let response = self
            .send("DELETE", key, &[("uploadId", upload_id)], &[], &[], None)
            .await?;
        // 404 means it is already gone, which is the state we wanted.
        if response.status == 404 {
            return Ok(());
        }
        expect_ok(&response, "DELETE upload", key)?;
        Ok(())
    }

    // ---- transport ---------------------------------------------------------

    /// Sends a request, retrying what is safe to retry.
    ///
    /// Every request this module makes is idempotent — PUT of a whole object,
    /// GET, a numbered part, an abort — so a retry cannot duplicate an effect.
    async fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
        body: &[u8],
        content_type: Option<&str>,
    ) -> io::Result<Response> {
        let mut attempt = 0;
        let deadline = self.request_timeout
            + Duration::from_secs(body.len() as u64 / REQUEST_BODY_BYTES_PER_SEC);
        loop {
            attempt += 1;
            let outcome = match tokio::time::timeout(
                deadline,
                self.send_once(method, key, query, extra_headers, body, content_type),
            )
            .await
            {
                Ok(outcome) => outcome,
                // Dropping the attempt drops its connection with it, so a
                // stalled stream never goes back into the pool.
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("no complete response within {deadline:?}"),
                )),
            };
            match outcome {
                Ok(response) => {
                    let retryable = response.status == 429
                        || response.status == 503
                        || (500..=599).contains(&response.status);
                    if retryable && attempt < self.config.max_attempts {
                        self.backoff(attempt).await;
                        continue;
                    }
                    return Ok(response);
                }
                Err(e) if attempt < self.config.max_attempts => {
                    log::warn!(
                        "S3 {} {} failed (attempt {}/{}): {}",
                        method,
                        key,
                        attempt,
                        self.config.max_attempts,
                        e
                    );
                    self.backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Exponential backoff with full jitter, so a fleet that all failed at once
    /// does not all retry at once.
    async fn backoff(&self, attempt: u32) {
        let ceiling = 100u64 << attempt.min(6);
        tokio::time::sleep(Duration::from_millis(fastrand::u64(50..=ceiling.max(51)))).await;
    }

    async fn send_once(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
        body: &[u8],
        content_type: Option<&str>,
    ) -> io::Result<Response> {
        let request =
            self.sign_request(method, key, query, extra_headers, body.len(), content_type)?;

        let mut stream = self.connect().await?;
        stream.write_all(&request).await?;
        if !body.is_empty() {
            stream.write_all(body).await?;
        }
        stream.flush().await?;

        let mut reader = ResponseReader::new();
        let head = reader.read_head(&mut stream).await?;
        let body = reader.read_body(&mut stream, &head).await?;

        if head.keep_alive {
            self.release(stream);
        }

        Ok(Response {
            status: head.status,
            headers: head.headers,
            body,
        })
    }

    /// Takes a live connection from the pool, or opens one.
    async fn connect(&self) -> io::Result<Stream> {
        loop {
            let candidate = self.pool.lock().pop();
            match candidate {
                Some(pooled) if pooled.since.elapsed() < IDLE_TIMEOUT => {
                    return Ok(pooled.stream);
                }
                // Too old to trust; drop it and look at the next one.
                Some(_) => continue,
                None => break,
            }
        }

        let tcp = TcpStream::connect((
            self.config.endpoint.host.as_str(),
            self.config.endpoint.port,
        ))
        .await?;
        // Request framing is written in one shot, so Nagle would only add a
        // round trip of delay before the body follows.
        tcp.set_nodelay(true)?;

        match &self.tls {
            None => Ok(Stream::Plain(tcp)),
            Some(config) => {
                let server_name =
                    rustls::pki_types::ServerName::try_from(self.config.endpoint.host.clone())
                        .map_err(|_| {
                            io::Error::other(format!(
                                "invalid TLS host '{}'",
                                self.config.endpoint.host
                            ))
                        })?;
                let connector = tokio_rustls::TlsConnector::from(config.clone());
                Ok(Stream::Tls(Box::new(
                    connector.connect(server_name, tcp).await?,
                )))
            }
        }
    }

    /// Returns a connection for reuse, bounding how many are held.
    fn release(&self, stream: Stream) {
        let mut pool = self.pool.lock();
        if pool.len() < 16 {
            pool.push(Pooled {
                stream,
                since: Instant::now(),
            });
        }
    }

    // ---- SigV4 -------------------------------------------------------------

    /// The day's signing key, derived at most once per day.
    fn signing_key(&self, date: &str) -> [u8; 32] {
        let mut cached = self.signing_key.lock();
        if let Some((day, key)) = cached.as_ref() {
            if day == date {
                return *key;
            }
        }

        let mut secret = String::with_capacity(4 + self.config.credentials.secret_key.len());
        secret.push_str("AWS4");
        secret.push_str(&self.config.credentials.secret_key);

        let k_date = hmac(secret.as_bytes(), date.as_bytes());
        let k_region = hmac(&k_date, self.config.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let key = hmac(&k_service, b"aws4_request");

        *cached = Some((date.to_string(), key));
        key
    }

    /// Builds the full request head, signed.
    fn sign_request(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
        body_len: usize,
        content_type: Option<&str>,
    ) -> io::Result<Vec<u8>> {
        let now = timeutil::now_unix();
        let amz_date = timeutil::format_basic(now);
        let date = amz_date[..8].to_string();

        // The body is never hashed for the signature. `UNSIGNED-PAYLOAD` is a
        // documented SigV4 mode that every S3 implementation accepts, and
        // skipping the hash is the single biggest CPU saving in the upload
        // path: a multi-gigabyte snapshot would otherwise be read twice, once
        // to compress and once to sign. Integrity does not rest on it —
        // TLS covers the wire, and the archive carries a CRC per entry plus a
        // zstd frame checksum, both of which are verified on restore.
        //
        // An empty body is the exception, only because its digest is a
        // constant and costs nothing to state exactly.
        let payload_hash = if body_len == 0 {
            EMPTY_SHA256
        } else {
            UNSIGNED_PAYLOAD
        };

        let host = self.host_header();
        let path = self.request_path(key);

        // Canonical headers must be sorted by lowercased name.
        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &self.config.credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        for (name, value) in extra_headers {
            headers.push((name.to_ascii_lowercase(), (*value).to_string()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        let signed_headers = signed_names.join(";");

        let mut canonical_query: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
            .collect();
        canonical_query.sort();
        let canonical_query = canonical_query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let mut canonical = String::with_capacity(256);
        canonical.push_str(method);
        canonical.push('\n');
        canonical.push_str(&path);
        canonical.push('\n');
        canonical.push_str(&canonical_query);
        canonical.push('\n');
        for (name, value) in &headers {
            canonical.push_str(name);
            canonical.push(':');
            canonical.push_str(value.trim());
            canonical.push('\n');
        }
        canonical.push('\n');
        canonical.push_str(&signed_headers);
        canonical.push('\n');
        canonical.push_str(payload_hash);

        let scope = format!("{}/{}/s3/aws4_request", date, self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            hex(&Sha256::digest(canonical.as_bytes()))
        );

        let signature = hex(&hmac(&self.signing_key(&date), string_to_sign.as_bytes()));

        let target = if canonical_query.is_empty() {
            path
        } else {
            format!("{path}?{canonical_query}")
        };

        let mut request = String::with_capacity(512);
        request.push_str(method);
        request.push(' ');
        request.push_str(&target);
        request.push_str(" HTTP/1.1\r\n");
        for (name, value) in &headers {
            request.push_str(name);
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
        if let Some(ct) = content_type {
            request.push_str("content-type: ");
            request.push_str(ct);
            request.push_str("\r\n");
        }
        request.push_str("content-length: ");
        request.push_str(&body_len.to_string());
        request.push_str("\r\n");
        request.push_str("authorization: AWS4-HMAC-SHA256 Credential=");
        request.push_str(&self.config.credentials.access_key);
        request.push('/');
        request.push_str(&scope);
        request.push_str(", SignedHeaders=");
        request.push_str(&signed_headers);
        request.push_str(", Signature=");
        request.push_str(&signature);
        request.push_str("\r\n\r\n");

        Ok(request.into_bytes())
    }
}

// ---- response parsing ------------------------------------------------------

struct Head {
    status: u16,
    headers: Vec<(String, String)>,
    content_length: Option<u64>,
    chunked: bool,
    keep_alive: bool,
}

/// Reads one HTTP/1.1 response, keeping whatever of the body arrived alongside
/// the head so it is not lost.
struct ResponseReader {
    buf: Vec<u8>,
    /// Bytes of `buf` already handed back.
    cursor: usize,
    /// Longest a single read may wait for its first byte. See
    /// [`STREAM_IDLE_TIMEOUT`]; `None` leaves the request's own deadline as
    /// the only limit.
    idle: Option<Duration>,
}

/// `stream.read_into`, failing with `TimedOut` if nothing arrives within
/// `idle`.
async fn read_idle(
    idle: Option<Duration>,
    stream: &mut Stream,
    buf: &mut Vec<u8>,
    max: usize,
) -> io::Result<usize> {
    match idle {
        None => stream.read_into(buf, max).await,
        Some(limit) => match tokio::time::timeout(limit, stream.read_into(buf, max)).await {
            Ok(read) => read,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no bytes from S3 for {limit:?}"),
            )),
        },
    }
}

impl ResponseReader {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(8 * 1024),
            cursor: 0,
            idle: None,
        }
    }

    async fn read_head(&mut self, stream: &mut Stream) -> io::Result<Head> {
        let mut chunk = [0u8; 8 * 1024];
        let header_end = loop {
            if let Some(pos) = find_subsequence(&self.buf, b"\r\n\r\n") {
                break pos + 4;
            }
            if self.buf.len() > 256 * 1024 {
                return Err(io::Error::other(
                    "S3 response headers are implausibly large",
                ));
            }
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "S3 closed the connection before sending a response",
                ));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        };

        let text = String::from_utf8_lossy(&self.buf[..header_end]).to_string();
        let mut lines = text.split("\r\n");
        let status_line = lines.next().unwrap_or_default();
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::other(format!("unparsable status line '{status_line}'")))?;

        let mut headers = Vec::new();
        let mut content_length = None;
        let mut chunked = false;
        let mut keep_alive = true;

        for line in lines {
            if line.is_empty() {
                continue;
            }
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            match name.as_str() {
                "content-length" => content_length = value.parse().ok(),
                "transfer-encoding" if value.eq_ignore_ascii_case("chunked") => chunked = true,
                "connection" if value.eq_ignore_ascii_case("close") => keep_alive = false,
                _ => {}
            }
            headers.push((name, value));
        }

        self.cursor = header_end;
        Ok(Head {
            status,
            headers,
            content_length,
            chunked,
            keep_alive,
        })
    }

    async fn read_body(&mut self, stream: &mut Stream, head: &Head) -> io::Result<Vec<u8>> {
        // A body whose length is known is read straight into one buffer of
        // that size: no regrowth, no zeroed scratch, no copy out of one.
        if let (false, Some(len)) = (head.chunked, head.content_length) {
            let len = usize::try_from(len).unwrap_or(usize::MAX);
            if len > MAX_INLINE_BODY {
                return Err(io::Error::other("S3 response body is too large to buffer"));
            }
            let mut out = Vec::with_capacity(len);
            let buffered = (self.buf.len() - self.cursor).min(len);
            out.extend_from_slice(&self.buf[self.cursor..self.cursor + buffered]);
            self.cursor += buffered;
            while out.len() < len {
                let want = (len - out.len()).min(READ_CHUNK);
                if read_idle(self.idle, stream, &mut out, want).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "S3 response body ended early",
                    ));
                }
            }
            return Ok(out);
        }

        let mut out = Vec::new();
        self.stream_body(stream, head, &mut |chunk: &[u8]| {
            if out.len() + chunk.len() > MAX_INLINE_BODY {
                return Err(io::Error::other("S3 response body is too large to buffer"));
            }
            out.extend_from_slice(chunk);
            Ok(())
        })
        .await?;
        Ok(out)
    }

    /// Feeds the body to `sink` as it arrives, honouring both framings.
    async fn stream_body(
        &mut self,
        stream: &mut Stream,
        head: &Head,
        sink: &mut impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<u64> {
        if head.chunked {
            return self.stream_chunked(stream, sink).await;
        }

        let mut remaining = match head.content_length {
            Some(n) => n,
            // No framing at all: the body runs to end of connection.
            None if !head.keep_alive => u64::MAX,
            None => 0,
        };
        let mut total = 0u64;

        // Whatever arrived in the same packets as the headers.
        let buffered = self.buf.len() - self.cursor;
        if buffered > 0 {
            let take = buffered.min(remaining as usize);
            sink(&self.buf[self.cursor..self.cursor + take])?;
            self.cursor += take;
            total += take as u64;
            // `u64::MAX` means read-to-close and must stay the sentinel:
            // subtracting from it turned EOF into "ended early" whenever body
            // bytes had arrived in the same packet as the headers.
            if remaining != u64::MAX {
                remaining -= take as u64;
            }
        }

        // Allocated once and never zeroed: each read lands in its spare
        // capacity.
        let mut chunk: Vec<u8> = Vec::new();
        while remaining > 0 {
            let want = READ_CHUNK.min(usize::try_from(remaining).unwrap_or(usize::MAX));
            chunk.clear();
            let n = read_idle(self.idle, stream, &mut chunk, want).await?;
            if n == 0 {
                if remaining == u64::MAX {
                    break; // read-to-close finished normally
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "S3 response body ended early",
                ));
            }
            sink(&chunk)?;
            total += n as u64;
            if remaining != u64::MAX {
                remaining -= n as u64;
            }
        }
        Ok(total)
    }

    async fn stream_chunked(
        &mut self,
        stream: &mut Stream,
        sink: &mut impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<u64> {
        let mut total = 0u64;
        loop {
            let line = self.read_line(stream).await?;
            // A chunk extension follows a semicolon and is not part of the size.
            let size_text = line.split(';').next().unwrap_or("").trim();
            let size = u64::from_str_radix(size_text, 16)
                .map_err(|_| io::Error::other(format!("bad chunk size '{size_text}'")))?;

            if size == 0 {
                // Trailers, then the terminating blank line.
                loop {
                    let trailer = self.read_line(stream).await?;
                    if trailer.is_empty() {
                        break;
                    }
                }
                return Ok(total);
            }

            let mut remaining = size;
            while remaining > 0 {
                self.fill(stream, 1).await?;
                let available = (self.buf.len() - self.cursor).min(remaining as usize);
                sink(&self.buf[self.cursor..self.cursor + available])?;
                self.cursor += available;
                remaining -= available as u64;
                total += available as u64;
            }
            // The CRLF that closes the chunk.
            self.read_line(stream).await?;
            self.compact();
        }
    }

    async fn read_line(&mut self, stream: &mut Stream) -> io::Result<String> {
        loop {
            if let Some(pos) = find_subsequence(&self.buf[self.cursor..], b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.cursor..self.cursor + pos]).to_string();
                self.cursor += pos + 2;
                return Ok(line);
            }
            self.fill(stream, 1).await?;
        }
    }

    /// Reads at least `want` more bytes into the buffer.
    async fn fill(&mut self, stream: &mut Stream, want: usize) -> io::Result<()> {
        if self.buf.len() - self.cursor >= want {
            return Ok(());
        }
        // Straight into the buffer's spare capacity: this runs for every line
        // of a chunked body, and a fresh zeroed 128 KiB scratch per call was
        // most of its cost.
        while self.buf.len() - self.cursor < want {
            let n = read_idle(self.idle, stream, &mut self.buf, READ_CHUNK).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "S3 response body ended early",
                ));
            }
        }
        Ok(())
    }

    /// Drops consumed bytes so a long chunked body does not grow the buffer
    /// without bound.
    fn compact(&mut self) {
        if self.cursor > 0 {
            self.buf.drain(..self.cursor);
            self.cursor = 0;
        }
    }
}

// ---- helpers ---------------------------------------------------------------

fn expect_ok(response: &Response, what: &str, key: &str) -> io::Result<()> {
    if response.status / 100 == 2 {
        return Ok(());
    }
    Err(s3_error(what, key, response.status, &response.body))
}

fn s3_error(what: &str, key: &str, status: u16, body: &[u8]) -> io::Error {
    let text = String::from_utf8_lossy(body);
    let detail = xml_tag(&text, "Message")
        .or_else(|| xml_tag(&text, "Code"))
        .unwrap_or_else(|| text.trim());
    let kind = match status {
        404 => io::ErrorKind::NotFound,
        403 | 401 => io::ErrorKind::PermissionDenied,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(
        kind,
        format!(
            "S3 {what} {key} returned {status}: {}",
            truncate(detail, 400)
        ),
    )
}

fn truncate(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}

fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

/// Percent-encodes per RFC 3986, which is stricter than most URL encoders: a
/// signature computed over a differently-encoded path simply does not verify.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn encode_key(key: &str) -> String {
    uri_encode(key.trim_start_matches('/'), false)
}

fn xml_tag<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)? + open.len();
    let end = text[start..].find(&close)? + start;
    Some(&text[start..end])
}

/// Reverses [`xml_escape_into`] for the few entities S3 emits in a key.
fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        // Last, so an escaped ampersand cannot be re-expanded into another
        // entity by the replacements above.
        .replace("&amp;", "&")
}

fn xml_escape_into(text: &str, out: &mut String) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
}

/// `Content-MD5`, which the batch-delete API requires.
///
/// MD5 is used here as a transport checksum on a small XML document, exactly as
/// the S3 API specifies; nothing about it is relied on for security.
fn base64_md5(body: &[u8]) -> String {
    use base64::Engine;
    use md5::Digest as _;
    let digest = md5::Md5::digest(body);
    base64::engine::general_purpose::STANDARD.encode(digest)
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// So `Write`-shaped callers can be adapted where needed.
pub struct SinkWriter<F: FnMut(&[u8]) -> io::Result<()>>(pub F);

impl<F: FnMut(&[u8]) -> io::Result<()>> Write for SinkWriter<F> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (self.0)(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    /// Serves `response` (split into `pieces` writes) on a local socket and
    /// reads it back through `ResponseReader`.
    async fn exchange(response: Vec<u8>, pieces: usize) -> (Head, io::Result<Vec<u8>>, Vec<u8>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let step = response.len().div_ceil(pieces.max(1)).max(1);
            for part in response.chunks(step) {
                socket.write_all(part).await.unwrap();
                socket.flush().await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        let mut stream = Stream::Plain(TcpStream::connect(addr).await.unwrap());
        let mut reader = ResponseReader::new();
        let head = reader.read_head(&mut stream).await.unwrap();
        let body = reader.read_body(&mut stream, &head).await;
        // Whatever followed the body stays for the next reader, unconsumed.
        let mut rest = reader.buf[reader.cursor..].to_vec();
        server.await.unwrap();
        let _ = stream.read_into(&mut rest, 1024).await;
        (head, body, rest)
    }

    fn payload(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 31 % 251) as u8).collect()
    }

    #[tokio::test]
    async fn content_length_bodies_read_exactly() {
        for (len, pieces) in [(0usize, 1usize), (5, 1), (300_000, 7), (128 * 1024, 3), (1, 2)] {
            let body = payload(len);
            let mut response =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {len}\r\n\r\n").into_bytes();
            response.extend_from_slice(&body);
            // Bytes past the body must not be swallowed into it.
            response.extend_from_slice(b"NEXT");
            let (head, got, rest) = exchange(response, pieces).await;
            assert_eq!(head.status, 200);
            assert_eq!(got.unwrap(), body, "len {len}");
            assert!(rest.starts_with(b"NEXT") || rest.is_empty() || b"NEXT".starts_with(&rest), "len {len}: {rest:?}");
        }
    }

    #[tokio::test]
    async fn a_short_body_is_an_error_not_a_short_answer() {
        let mut response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n".to_vec();
        response.extend_from_slice(&payload(60));
        let (_, got, _) = exchange(response, 2).await;
        assert_eq!(got.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn chunked_and_unframed_bodies_still_read() {
        let body = payload(200_000);
        let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for part in body.chunks(70_001) {
            response.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
            response.extend_from_slice(part);
            response.extend_from_slice(b"\r\n");
        }
        response.extend_from_slice(b"0\r\n\r\n");
        let (_, got, _) = exchange(response, 9).await;
        assert_eq!(got.unwrap(), body);

        let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
        response.extend_from_slice(&body);
        let (_, got, _) = exchange(response, 5).await;
        assert_eq!(got.unwrap(), body);
    }

    #[tokio::test]
    async fn streamed_bodies_reach_the_sink_whole() {
        let body = payload(400_000);
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        response.extend_from_slice(&body);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for part in response.chunks(50_000) {
                socket.write_all(part).await.unwrap();
            }
        });
        let mut stream = Stream::Plain(TcpStream::connect(addr).await.unwrap());
        let mut reader = ResponseReader::new();
        let head = reader.read_head(&mut stream).await.unwrap();
        let mut got = Vec::new();
        let n = reader
            .stream_body(&mut stream, &head, &mut |c: &[u8]| {
                got.extend_from_slice(c);
                Ok(())
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(n as usize, body.len());
        assert_eq!(got, body);
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Answers every request on a local socket with `reply`, or never answers
    /// when it is `None`. Counts the requests it read.
    async fn fake(reply: Option<&'static str>, seen: Arc<AtomicUsize>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    // The head, then whatever body it announced.
                    loop {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf).to_lowercase();
                        if let Some(end) = text.find("\r\n\r\n") {
                            let len = text
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            if buf.len() >= end + 4 + len {
                                break;
                            }
                        }
                    }
                    seen.fetch_add(1, Ordering::SeqCst);
                    match reply {
                        None => tokio::time::sleep(Duration::from_secs(3600)).await,
                        Some(body) => {
                            let out = format!(
                                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = socket.write_all(out.as_bytes()).await;
                        }
                    }
                });
            }
        });
        port
    }

    fn client(port: u16, attempts: u32) -> S3Client {
        S3Client::new(S3Config {
            endpoint: Endpoint::parse(&format!("http://127.0.0.1:{port}")).unwrap(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            credentials: Credentials {
                access_key: "k".into(),
                secret_key: "s".into(),
                session_token: None,
            },
            path_style: true,
            max_attempts: attempts,
        })
        .unwrap()
    }

    /// A server that accepts the request and never answers fails the request
    /// — each attempt at its deadline — instead of holding the caller forever.
    #[tokio::test]
    async fn a_request_nobody_answers_times_out_and_is_retried() {
        let seen = Arc::new(AtomicUsize::new(0));
        let port = fake(None, seen.clone()).await;
        let mut c = client(port, 2);
        c.request_timeout = Duration::from_millis(300);
        let outcome =
            tokio::time::timeout(Duration::from_secs(20), c.get_object_opt("t/META")).await;
        let error = outcome
            .expect("the request hung past every deadline")
            .expect_err("a request nobody answered succeeded");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(seen.load(Ordering::SeqCst), 2, "each attempt reached the server");
    }

    /// Serves `body` to GETs. The first connection sends half of it and then
    /// goes silent (when `stall_first`); every connection otherwise sends its
    /// bytes in 8 pieces `gap` apart. A `Range: bytes=N-` request gets a 206
    /// with the rest.
    async fn streaming(body: Vec<u8>, stall_first: bool, gap: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let body = Arc::new(body);
        tokio::spawn(async move {
            let mut first = true;
            while let Ok((mut socket, _)) = listener.accept().await {
                let stall = std::mem::take(&mut first) && stall_first;
                let body = body.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        head.extend_from_slice(&chunk[..n]);
                    }
                    let text = String::from_utf8_lossy(&head).to_lowercase();
                    let from = text
                        .lines()
                        .find_map(|l| l.strip_prefix("range: bytes="))
                        .and_then(|r| r.trim().trim_end_matches('-').parse::<usize>().ok());
                    let (status, rest) = match from {
                        Some(at) => ("206 Partial Content", &body[at..]),
                        None => ("200 OK", &body[..]),
                    };
                    let out = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        rest.len()
                    );
                    if socket.write_all(out.as_bytes()).await.is_err() {
                        return;
                    }
                    if stall {
                        let _ = socket.write_all(&rest[..rest.len() / 2]).await;
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        return;
                    }
                    for piece in rest.chunks(rest.len().div_ceil(8).max(1)) {
                        if socket.write_all(piece).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(gap).await;
                    }
                });
            }
        });
        port
    }

    /// A download that stops sending part-way fails at the idle limit and is
    /// resumed from where it stopped, delivering every byte once.
    #[tokio::test]
    async fn a_stalled_download_is_resumed_after_the_idle_limit() {
        let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let port = streaming(body.clone(), true, Duration::ZERO).await;
        let mut c = client(port, 3);
        c.stream_idle_timeout = Duration::from_millis(300);
        let mut got = Vec::new();
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            c.get_object_streaming("t/big", |chunk| {
                got.extend_from_slice(chunk);
                Ok(())
            }),
        )
        .await
        .expect("a stalled download hung");
        assert_eq!(outcome.unwrap() as usize, body.len());
        assert_eq!(got, body);
    }

    /// The limit is on silence, not on the whole transfer: a download that
    /// keeps trickling for longer than the limit in total still completes.
    #[tokio::test]
    async fn a_slow_steady_download_is_not_cut_off() {
        let body: Vec<u8> = (0..80_000u32).map(|i| (i % 13) as u8).collect();
        // Eight pieces 150 ms apart: about 1.2 s in all, never 300 ms silent.
        let port = streaming(body.clone(), false, Duration::from_millis(150)).await;
        let mut c = client(port, 1);
        c.stream_idle_timeout = Duration::from_millis(300);
        let mut got = Vec::new();
        let n = c
            .get_object_streaming("t/slow", |chunk| {
                got.extend_from_slice(chunk);
                Ok(())
            })
            .await
            .expect("a steady download was cut off");
        assert_eq!(n as usize, body.len());
        assert_eq!(got, body);
    }

    /// A 200 `DeleteResult` naming keys it could not delete is a failure.
    #[tokio::test]
    async fn a_delete_result_listing_errors_is_an_error() {
        let seen = Arc::new(AtomicUsize::new(0));
        let port = fake(
            Some(
                "<DeleteResult><Error><Key>t/x</Key><Code>AccessDenied</Code>\
                 <Message>denied</Message></Error></DeleteResult>",
            ),
            seen,
        )
        .await;
        let error = client(port, 1)
            .delete_objects(&["t/x".to_string(), "t/y".to_string()])
            .await
            .expect_err("undeleted keys were reported as deleted");
        let text = error.to_string();
        assert!(text.contains("t/x") && text.contains("denied"), "{text}");

        let port = fake(Some("<DeleteResult></DeleteResult>"), Arc::new(AtomicUsize::new(0))).await;
        client(port, 1)
            .delete_objects(&["t/x".to_string()])
            .await
            .expect("a quiet DeleteResult with no errors is success");
    }
}
