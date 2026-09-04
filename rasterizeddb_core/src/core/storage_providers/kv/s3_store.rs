//! The object-store backing for [`super::KvStorageProvider`].
//!
//! Thin by design: the SigV4 client, the connection pool and the retry policy
//! already exist for `s3_backup`, and every decision that makes the driver
//! usable — how big an object is, when to read a page instead of a chunk, how
//! many requests to have in flight — belongs to the provider, not here.
//!
//! Works against anything speaking the S3 REST API: AWS, MinIO, Ceph RGW,
//! Cloudflare R2, Backblaze B2, Wasabi.

use std::io;
use std::sync::Arc;

use crate::core::backup::s3::{Credentials, Endpoint, S3Client, S3Config};

use super::store::{BoxFuture, KvStore, ObjectInfo};

/// A single PUT above this is split into parts.
///
/// Well under the 5 GiB the API allows: a large single PUT that fails has to be
/// retried whole, and a chunk size that large is a configuration mistake rather
/// than a case to optimise for.
const MULTIPART_THRESHOLD: usize = 64 * 1024 * 1024;
const PART_SIZE: usize = 16 * 1024 * 1024;

pub struct S3KvStore {
    client: Arc<S3Client>,
    /// Requests to keep in flight. An object store answers one request after a
    /// latency floor that has nothing to do with size, so this — not bandwidth —
    /// is what a scan's throughput is made of.
    concurrency: usize,
    describe: String,
}

impl S3KvStore {
    pub fn new(config: S3Config) -> io::Result<Self> {
        let describe = format!(
            "s3://{}@{}:{}",
            config.bucket, config.endpoint.host, config.endpoint.port
        );
        Ok(Self {
            client: Arc::new(S3Client::new(config)?),
            concurrency: 32,
            describe,
        })
    }

    /// Builds a store from the standard AWS environment variables plus an
    /// endpoint, region and bucket.
    pub fn from_parts(
        endpoint: &str,
        region: &str,
        bucket: &str,
        path_style: bool,
    ) -> io::Result<Self> {
        let credentials = Credentials::from_env().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set",
            )
        })?;
        let endpoint = Endpoint::parse(endpoint).map_err(io::Error::other)?;
        Self::new(S3Config {
            endpoint,
            region: region.to_string(),
            bucket: bucket.to_string(),
            credentials,
            path_style,
            max_attempts: 4,
        })
    }

    /// How many requests this store will run at once. Raise it for a wide scan
    /// over a high-latency endpoint; lower it if the store starts returning 503.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    pub fn client(&self) -> &Arc<S3Client> {
        &self.client
    }

    async fn put_multipart(&self, key: &str, body: Vec<u8>) -> io::Result<()> {
        let upload_id = self.client.create_multipart_upload(key).await?;
        let mut parts = Vec::new();
        for (index, part) in body.chunks(PART_SIZE).enumerate() {
            let number = index as u32 + 1;
            match self.client.upload_part(key, &upload_id, number, part).await {
                Ok(etag) => parts.push((number, etag)),
                Err(e) => {
                    // Leaving an upload open bills for its parts until a
                    // lifecycle rule reaps them, so it is aborted here even
                    // though the caller only sees the original failure.
                    let _ = self.client.abort_multipart_upload(key, &upload_id).await;
                    return Err(e);
                }
            }
        }
        self.client
            .complete_multipart_upload(key, &upload_id, &parts)
            .await
    }
}

impl KvStore for S3KvStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move { self.client.get_object_opt(key).await })
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move { self.client.get_object_range(key, offset, len).await })
    }

    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            if body.len() > MULTIPART_THRESHOLD {
                return self.put_multipart(key, body).await;
            }
            self.client.put_object(key, &body).await.map(|_etag| ())
        })
    }

    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, io::Result<()>> {
        // One request per 1000 keys rather than one per key — the client batches
        // this itself.
        Box::pin(async move { self.client.delete_objects(&keys).await })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<ObjectInfo>>> {
        Box::pin(async move {
            let listed = self.client.list_prefix(prefix).await?;
            Ok(listed
                .into_iter()
                .map(|(key, size)| ObjectInfo { key, size })
                .collect())
        })
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }
}
