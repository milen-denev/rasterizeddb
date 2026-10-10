//! A minimal in-process S3, for tests.
//!
//! Speaks exactly what [`super::s3::S3Client`] sends, path-style over plain
//! HTTP/1.1 with keep-alive: whole-object PUT/GET (ranges included),
//! `ListObjectsV2` with continuation, batch `DELETE`, and the multipart
//! calls. Signatures are not checked. It exists so the backup's end-to-end
//! behaviour — snapshot, archive, retention, restore — can be exercised in
//! every test run rather than only where a MinIO is at hand.
//!
//! It runs on a thread and runtime of its own, so it outlives the
//! `#[tokio::test]` runtimes of the tests that share it.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// What the server holds: `bucket/key` -> bytes, and open multipart uploads.
#[derive(Default)]
pub struct Store {
    pub objects: BTreeMap<String, Vec<u8>>,
    /// upload id -> (`bucket/key`, part number -> bytes)
    uploads: BTreeMap<String, (String, BTreeMap<u32, Vec<u8>>)>,
    next_upload: u64,
    /// Requests served, for tests that count round trips.
    pub requests: u64,
}

pub struct FakeS3 {
    pub port: u16,
    pub store: Arc<Mutex<Store>>,
}

impl FakeS3 {
    /// `http://127.0.0.1:<port>`, for `Endpoint::parse`.
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Every key under `bucket/prefix`, without the bucket.
    pub fn keys(&self, bucket: &str, prefix: &str) -> Vec<String> {
        let full = format!("{bucket}/{prefix}");
        self.store
            .lock()
            .objects
            .keys()
            .filter(|k| k.starts_with(&full))
            .map(|k| k[bucket.len() + 1..].to_string())
            .collect()
    }
}

/// One server per process, started on first use.
pub fn shared() -> &'static FakeS3 {
    static SERVER: OnceLock<FakeS3> = OnceLock::new();
    SERVER.get_or_init(start)
}

/// Starts a fresh server on its own thread.
pub fn start() -> FakeS3 {
    let store = Arc::new(Mutex::new(Store::default()));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake S3");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().expect("addr").port();
    let served = store.clone();
    std::thread::Builder::new()
        .name("fake-s3".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("fake S3 runtime");
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).expect("listener");
                loop {
                    let Ok((socket, _)) = listener.accept().await else {
                        continue;
                    };
                    let store = served.clone();
                    tokio::spawn(async move {
                        let _ = connection(socket, store).await;
                    });
                }
            });
        })
        .expect("spawn fake S3");
    FakeS3 { port, store }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&text[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn tags<'a>(text: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    text.split(&open)
        .skip(1)
        .filter_map(|chunk| chunk.split(&close).next())
        .collect()
}

struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }
    fn not_found(key: &str) -> Self {
        Self::new(
            404,
            format!(
                "<Error><Code>NoSuchKey</Code><Message>no such key {}</Message></Error>",
                xml_escape(key)
            ),
        )
    }
}

async fn connection(mut socket: tokio::net::TcpStream, store: Arc<Mutex<Store>>) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = vec![0u8; 64 * 1024];
    loop {
        let head_end = loop {
            if let Some(p) = find(&buf, b"\r\n\r\n") {
                break p + 4;
            }
            let n = socket.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default().to_string();
        let mut headers: Vec<(String, String)> = Vec::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
        }
        let header = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let length: usize = header("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + length {
            let n = socket.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let body = buf[head_end..head_end + length].to_vec();
        buf.drain(..head_end + length);

        let mut parts = request_line.split(' ');
        let method = parts.next().unwrap_or_default().to_string();
        let target = parts.next().unwrap_or_default().to_string();
        let reply = handle(&store, &method, &target, header("range"), body);

        let mut out = format!("HTTP/1.1 {} X\r\ncontent-length: {}\r\n", reply.status, reply.body.len());
        for (k, v) in &reply.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str("\r\n");
        socket.write_all(out.as_bytes()).await?;
        socket.write_all(&reply.body).await?;
        socket.flush().await?;
    }
}

fn handle(
    store: &Mutex<Store>,
    method: &str,
    target: &str,
    range: Option<String>,
    body: Vec<u8>,
) -> Reply {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query: BTreeMap<String, String> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect();
    let path = percent_decode(path.trim_start_matches('/'));
    let (bucket, key) = path.split_once('/').unwrap_or((path.as_str(), ""));
    let full = format!("{bucket}/{key}");

    let mut store = store.lock();
    store.requests += 1;

    match (method, key.is_empty()) {
        ("GET", true) if query.contains_key("uploads") => {
            let mut xml = String::from("<ListMultipartUploadsResult>");
            for (id, (object, _)) in &store.uploads {
                if let Some(k) = object.strip_prefix(&format!("{bucket}/")) {
                    xml.push_str(&format!(
                        "<Upload><Key>{}</Key><UploadId>{}</UploadId></Upload>",
                        xml_escape(k),
                        id
                    ));
                }
            }
            xml.push_str("</ListMultipartUploadsResult>");
            Reply::new(200, xml)
        }
        ("GET", true) => {
            // ListObjectsV2, 1000 a page, the token being the last key served.
            let prefix = query.get("prefix").cloned().unwrap_or_default();
            let after = query.get("continuation-token").cloned();
            let from = format!("{bucket}/{prefix}");
            let mut xml = String::from("<ListBucketResult>");
            let mut served = 0;
            let mut last = None;
            let mut truncated = false;
            for (k, v) in store.objects.range(from.clone()..) {
                if !k.starts_with(&from) {
                    break;
                }
                let name = &k[bucket.len() + 1..];
                if after.as_deref().is_some_and(|a| name <= a) {
                    continue;
                }
                if served == 1000 {
                    truncated = true;
                    break;
                }
                xml.push_str(&format!(
                    "<Contents><Key>{}</Key><Size>{}</Size></Contents>",
                    xml_escape(name),
                    v.len()
                ));
                served += 1;
                last = Some(name.to_string());
            }
            xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
            if truncated {
                if let Some(last) = last {
                    xml.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        xml_escape(&last)
                    ));
                }
            }
            xml.push_str("</ListBucketResult>");
            Reply::new(200, xml)
        }
        ("POST", true) if query.contains_key("delete") => {
            let text = String::from_utf8_lossy(&body).to_string();
            for key in tags(&text, "Key") {
                store.objects.remove(&format!("{bucket}/{}", xml_unescape(key)));
            }
            Reply::new(200, "<DeleteResult></DeleteResult>")
        }
        ("POST", false) if query.contains_key("uploads") => {
            store.next_upload += 1;
            let id = format!("upload{}", store.next_upload);
            store.uploads.insert(id.clone(), (full, BTreeMap::new()));
            Reply::new(
                200,
                format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
            )
        }
        ("PUT", false) if query.contains_key("uploadId") => {
            let id = &query["uploadId"];
            let number: u32 = query
                .get("partNumber")
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            let Some((_, parts)) = store.uploads.get_mut(id) else {
                return Reply::new(404, "<Error><Code>NoSuchUpload</Code></Error>");
            };
            parts.insert(number, body);
            let mut reply = Reply::new(200, Vec::new());
            reply.headers.push(("etag".to_string(), format!("\"part{number}\"")));
            reply
        }
        ("POST", false) if query.contains_key("uploadId") => {
            let id = query["uploadId"].clone();
            let Some((object, parts)) = store.uploads.remove(&id) else {
                return Reply::new(404, "<Error><Code>NoSuchUpload</Code></Error>");
            };
            let text = String::from_utf8_lossy(&body).to_string();
            let mut assembled = Vec::new();
            for number in tags(&text, "PartNumber") {
                let Some(part) = number.parse().ok().and_then(|n: u32| parts.get(&n)) else {
                    return Reply::new(
                        200,
                        "<Error><Code>InvalidPart</Code><Message>missing part</Message></Error>",
                    );
                };
                assembled.extend_from_slice(part);
            }
            store.objects.insert(object, assembled);
            Reply::new(200, "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>")
        }
        ("DELETE", false) if query.contains_key("uploadId") => {
            match store.uploads.remove(&query["uploadId"]) {
                Some(_) => Reply::new(204, Vec::new()),
                None => Reply::new(404, Vec::new()),
            }
        }
        ("PUT", false) => {
            store.objects.insert(full, body);
            let mut reply = Reply::new(200, Vec::new());
            reply.headers.push(("etag".to_string(), "\"fake\"".to_string()));
            reply
        }
        ("GET", false) => {
            let Some(object) = store.objects.get(&full) else {
                return Reply::not_found(key);
            };
            match range.as_deref().and_then(|r| r.strip_prefix("bytes=")) {
                None => Reply::new(200, object.clone()),
                Some(spec) => {
                    let (a, b) = spec.split_once('-').unwrap_or((spec, ""));
                    let start: usize = a.parse().unwrap_or(0);
                    if start >= object.len() {
                        return Reply::new(416, Vec::new());
                    }
                    let end = b
                        .parse::<usize>()
                        .map(|e| (e + 1).min(object.len()))
                        .unwrap_or(object.len());
                    Reply::new(206, object[start..end].to_vec())
                }
            }
        }
        ("DELETE", false) => {
            store.objects.remove(&full);
            Reply::new(204, Vec::new())
        }
        _ => Reply::new(400, format!("<Error><Code>Unsupported</Code><Message>{method} {target}</Message></Error>")),
    }
}
