// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `la://` object store: read-only access to a bucket through a fleet of
//! S3-compatible cache nodes.
//!
//! The `endpoint` storage option is the URL of a topology document that
//! lists the nodes. The store splits every ranged read into fixed-size
//! chunks and sends each chunk to the node that owns it under rendezvous
//! hashing, so each chunk is served (and cached) by one node and its bytes
//! cross the network once. The document is refetched periodically:
//!
//! ```json
//! {"nodes": [{"endpoint": "http://cache-0:9100", "healthy": true},
//!            {"endpoint": "http://cache-1:9100", "healthy": true}],
//!  "chunk_size": 4194304,
//!  "hash": "rendezvous-xxh3-64-v1"}
//! ```
//!
//! Nodes that are unhealthy or have no `endpoint` are skipped, and other
//! fields are ignored. `headers.*` storage options are sent with every
//! request, including the topology request.
//!
//! `rendezvous-xxh3-64-v1`: the chunk of `{bucket}/{path}` at
//! `offset / chunk_size` has the key `{bucket}/{path}-{chunk_index}`. Its
//! owner is the node whose endpoint has the highest
//! `xxh3_64("{endpoint}\n{key}")`. A chunk is only ever read from its owner:
//! a failed or timed-out read is returned as an error rather than sent to
//! another node, which would cache the chunk a second time.

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpConnector, HttpRequestBody, ReqwestConnector};
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore as OSObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RetryConfig,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::{Error, Result};

pub const HASH_RENDEZVOUS_XXH3_64_V1: &str = "rendezvous-xxh3-64-v1";

const ENDPOINT_OPTION: &str = "endpoint";
const REGION_OPTION: &str = "region";
const DEFAULT_REGION: &str = "us-east-1";
const ROUTING_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// A failed read is reported as is; retrying it would only add load to an owner
/// that is already falling behind.
const ENDPOINT_MAX_RETRIES: usize = 0;
const ENDPOINT_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
/// Chunks requested ahead of the one being returned.
const CHUNK_FETCH_CONCURRENCY: usize = 8;

#[derive(Default, Debug)]
pub struct LaStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for LaStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("la:// URL must contain a bucket name"))?
            .to_string();
        let topology_url = storage_options
            .get(ENDPOINT_OPTION)
            .cloned()
            .ok_or_else(|| Error::invalid_input("la:// requires the `endpoint` storage option"))?;
        let block_size = storage_options
            .block_size()?
            .or(params.block_size)
            .unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let download_retry_count = storage_options.download_retry_count();
        let inner = LaObjectStore::try_new(bucket, topology_url, storage_options).await?;
        Ok(ObjectStore {
            inner: Arc::new(inner),
            local_dir_operations: None,
            local_fsync: false,
            scheme: String::from(base_path.scheme()),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: true,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
            paginated_lister: None,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TopologyNode {
    endpoint: Option<String>,
    #[serde(default = "default_healthy")]
    healthy: bool,
}

fn default_healthy() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct TopologyDocument {
    nodes: Vec<TopologyNode>,
    chunk_size: u64,
    hash: String,
}

impl TopologyDocument {
    /// The endpoints of the healthy nodes, in a stable order.
    fn endpoints(self) -> Vec<String> {
        let mut endpoints: Vec<String> = self
            .nodes
            .into_iter()
            .filter(|node| node.healthy)
            .filter_map(|node| node.endpoint)
            .collect();
        endpoints.sort();
        endpoints.dedup();
        endpoints
    }
}

#[derive(Debug)]
struct Routing {
    urls: Vec<String>,
    stores: Vec<Arc<dyn OSObjectStore>>,
    chunk_size: u64,
    fetched_at: Instant,
}

impl Routing {
    /// The store of the endpoint that owns `key`.
    fn owner(&self, key: &str) -> Arc<dyn OSObjectStore> {
        self.stores[rendezvous_owner(&self.urls, key)].clone()
    }
}

/// Index of the endpoint with the highest rendezvous score for `key`.
fn rendezvous_owner(urls: &[String], key: &str) -> usize {
    urls.iter()
        .enumerate()
        .max_by_key(|(_, url)| xxh3_64(format!("{url}\n{key}").as_bytes()))
        .map(|(idx, _)| idx)
        .expect("routing always has at least one endpoint")
}

/// Splits `range` at `chunk_size` boundaries into `(chunk_index, sub_range)`.
fn split_into_chunks(range: Range<u64>, chunk_size: u64) -> Vec<(u64, Range<u64>)> {
    let mut chunks = Vec::new();
    let mut start = range.start;
    while start < range.end {
        let chunk_index = start / chunk_size;
        let end = ((chunk_index + 1) * chunk_size).min(range.end);
        chunks.push((chunk_index, start..end));
        start = end;
    }
    chunks
}

fn chunk_key(bucket: &str, location: &Path, chunk_index: u64) -> String {
    format!("{bucket}/{location}-{chunk_index}")
}

/// Client options from `storage_options`, with `allow_http` applied.
fn client_options(storage_options: &StorageOptions) -> Result<ClientOptions> {
    Ok(storage_options
        .client_options()?
        .with_allow_http(storage_options.allow_http()))
}

fn endpoint_retry() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig::default(),
        max_retries: ENDPOINT_MAX_RETRIES,
        retry_timeout: ENDPOINT_RETRY_TIMEOUT,
    }
}

fn build_endpoint_store(
    url: &str,
    bucket: &str,
    storage_options: &StorageOptions,
) -> Result<Arc<dyn OSObjectStore>> {
    let region = storage_options
        .get(REGION_OPTION)
        .map(String::as_str)
        .unwrap_or(DEFAULT_REGION);
    let mut builder = AmazonS3Builder::new()
        .with_client_options(client_options(storage_options)?)
        .with_endpoint(url)
        .with_bucket_name(bucket)
        .with_region(region)
        .with_retry(endpoint_retry());
    builder = match (
        storage_options.get("aws_access_key_id"),
        storage_options.get("aws_secret_access_key"),
    ) {
        (Some(key), Some(secret)) => builder
            .with_access_key_id(key)
            .with_secret_access_key(secret),
        _ => builder.with_skip_signature(true),
    };
    Ok(Arc::new(builder.build()?))
}

async fn fetch_topology(url: &str, storage_options: &StorageOptions) -> Result<TopologyDocument> {
    let client = ReqwestConnector::default().connect(&client_options(storage_options)?)?;
    let request = http::Request::get(url)
        .body(HttpRequestBody::empty())
        .map_err(|err| Error::invalid_input(format!("invalid la:// endpoint {url}: {err}")))?;
    let response = client
        .execute(request)
        .await
        .map_err(|err| Error::io(format!("failed to fetch la:// topology from {url}: {err}")))?;
    let status = response.status();
    let body = response
        .into_body()
        .bytes()
        .await
        .map_err(|err| Error::io(format!("failed to read la:// topology from {url}: {err}")))?;
    if !status.is_success() {
        return Err(Error::io(format!(
            "la:// topology request to {url} returned {status}: {}",
            String::from_utf8_lossy(&body)
        )));
    }
    serde_json::from_slice(&body).map_err(|err| {
        Error::invalid_input(format!("invalid la:// topology document from {url}: {err}"))
    })
}

async fn fetch_routing(
    topology_url: &str,
    bucket: &str,
    storage_options: &StorageOptions,
) -> Result<Routing> {
    let document = fetch_topology(topology_url, storage_options).await?;
    if document.hash != HASH_RENDEZVOUS_XXH3_64_V1 {
        return Err(Error::not_supported(format!(
            "la:// hash {:?} from {topology_url} is not supported; expected {HASH_RENDEZVOUS_XXH3_64_V1}",
            document.hash
        )));
    }
    let chunk_size = document.chunk_size;
    let urls = document.endpoints();
    if urls.is_empty() || chunk_size == 0 {
        return Err(Error::invalid_input(format!(
            "la:// topology from {topology_url} needs a healthy node with an endpoint and a non-zero chunk_size"
        )));
    }
    let stores = urls
        .iter()
        .map(|url| build_endpoint_store(url, bucket, storage_options))
        .collect::<Result<Vec<_>>>()?;
    Ok(Routing {
        urls,
        stores,
        chunk_size,
        fetched_at: Instant::now(),
    })
}

fn read_only(operation: &str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("la:// is read-only; {operation} is not supported").into(),
    }
}

/// The `la://` [`OSObjectStore`].
#[derive(Debug)]
pub struct LaObjectStore {
    bucket: String,
    topology_url: String,
    storage_options: StorageOptions,
    routing: ArcSwap<Routing>,
    refreshing: Mutex<()>,
}

impl Display for LaObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LaObjectStore({}, {})", self.bucket, self.topology_url)
    }
}

impl LaObjectStore {
    pub async fn try_new(
        bucket: String,
        topology_url: String,
        storage_options: StorageOptions,
    ) -> Result<Self> {
        let routing = fetch_routing(&topology_url, &bucket, &storage_options).await?;
        Ok(Self {
            bucket,
            topology_url,
            storage_options,
            routing: ArcSwap::from_pointee(routing),
            refreshing: Mutex::new(()),
        })
    }

    /// The current routing, refreshed when stale. A failed refresh keeps the
    /// previous routing.
    async fn routing(&self) -> Arc<Routing> {
        let current = self.routing.load_full();
        if current.fetched_at.elapsed() < ROUTING_REFRESH_INTERVAL {
            return current;
        }
        let Ok(_guard) = self.refreshing.try_lock() else {
            return current;
        };
        match fetch_routing(&self.topology_url, &self.bucket, &self.storage_options).await {
            Ok(routing) => {
                let routing = Arc::new(routing);
                self.routing.store(routing.clone());
                routing
            }
            Err(err) => {
                log::warn!(
                    "failed to refresh la:// topology from {}: {err}",
                    self.topology_url
                );
                current
            }
        }
    }

    fn list_key(&self, prefix: Option<&Path>) -> String {
        format!(
            "{}/{}",
            self.bucket,
            prefix.map(|p| p.as_ref()).unwrap_or("")
        )
    }

    async fn head_on_owner(
        &self,
        routing: &Routing,
        location: &Path,
    ) -> object_store::Result<ObjectMeta> {
        routing
            .owner(&chunk_key(&self.bucket, location, 0))
            .head(location)
            .await
    }

    async fn resolve_range(
        &self,
        routing: &Routing,
        location: &Path,
        range: GetRange,
    ) -> object_store::Result<Range<u64>> {
        Ok(match range {
            GetRange::Bounded(range) => range,
            GetRange::Offset(offset) => offset..self.head_on_owner(routing, location).await?.size,
            GetRange::Suffix(length) => {
                let size = self.head_on_owner(routing, location).await?.size;
                size.saturating_sub(length)..size
            }
        })
    }
}

async fn get_chunk(
    routing: &Routing,
    key: &str,
    location: &Path,
    options: &GetOptions,
    chunk: Range<u64>,
) -> object_store::Result<GetResult> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(chunk)),
        ..options.clone()
    };
    routing.owner(key).get_opts(location, options).await
}

#[async_trait::async_trait]
impl OSObjectStore for LaObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only("put"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only("multipart put"))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let routing = self.routing().await;
        let range = match options.range.clone() {
            Some(range) if !options.head => range,
            // Whole-object reads and heads go to the owner of the first chunk.
            _ => {
                return routing
                    .owner(&chunk_key(&self.bucket, location, 0))
                    .get_opts(location, options)
                    .await;
            }
        };
        let range = self.resolve_range(&routing, location, range).await?;
        let mut chunks = split_into_chunks(range.clone(), routing.chunk_size).into_iter();
        let Some((first_index, first_range)) = chunks.next() else {
            let meta = self.head_on_owner(&routing, location).await?;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range,
                attributes: Default::default(),
                extensions: Default::default(),
            });
        };
        // The first chunk is fetched eagerly to return the object's metadata.
        let first_key = chunk_key(&self.bucket, location, first_index);
        let first = get_chunk(&routing, &first_key, location, &options, first_range).await?;
        let rest: Vec<(u64, Range<u64>)> = chunks.collect();
        if rest.is_empty() {
            return Ok(GetResult { range, ..first });
        }
        let meta = first.meta.clone();
        let attributes = first.attributes.clone();
        let extensions = first.extensions.clone();
        let bucket = self.bucket.clone();
        let location = location.clone();
        let rest_bytes = futures::stream::iter(rest)
            .map(move |(chunk_index, chunk)| {
                let routing = routing.clone();
                let options = options.clone();
                let location = location.clone();
                let key = chunk_key(&bucket, &location, chunk_index);
                async move {
                    get_chunk(&routing, &key, &location, &options, chunk)
                        .await?
                        .bytes()
                        .await
                }
            })
            .buffered(CHUNK_FETCH_CONCURRENCY)
            .map_ok(|bytes: Bytes| futures::stream::once(async move { Ok(bytes) }))
            .try_flatten();
        Ok(GetResult {
            payload: GetResultPayload::Stream(first.into_stream().chain(rest_bytes).boxed()),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations.map(|_| Err(read_only("delete"))).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.routing
            .load()
            .owner(&self.list_key(prefix))
            .list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.routing
            .load()
            .owner(&self.list_key(prefix))
            .list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.routing()
            .await
            .owner(&self.list_key(prefix))
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only("copy"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("http://cache-{i}:9100")).collect()
    }

    #[test]
    fn rendezvous_is_stable_and_spreads_keys() {
        let urls = urls(3);
        let mut owners = [0usize; 3];
        for chunk in 0..300 {
            let key = format!("bucket/t.lance/data/a.lance-{chunk}");
            let owner = rendezvous_owner(&urls, &key);
            assert_eq!(owner, rendezvous_owner(&urls, &key));
            owners[owner] += 1;
        }
        assert!(owners.iter().all(|&n| n > 50), "{owners:?}");
    }

    #[test]
    fn removing_an_endpoint_only_moves_its_keys() {
        let urls = urls(4);
        let remaining = urls[..3].to_vec();
        for chunk in 0..200 {
            let key = format!("bucket/f-{chunk}");
            let before = &urls[rendezvous_owner(&urls, &key)];
            let after = &remaining[rendezvous_owner(&remaining, &key)];
            if before != &urls[3] {
                assert_eq!(before, after);
            }
        }
    }

    #[test]
    fn rendezvous_matches_the_spec() {
        let urls = vec!["http://a:1".to_string(), "http://b:1".to_string()];
        let key = "bucket/t.lance/data/a.lance-7";
        let score = |url: &str| xxh3_64(format!("{url}\n{key}").as_bytes());
        let owner = if score("http://a:1") > score("http://b:1") {
            0
        } else {
            1
        };
        assert_eq!(rendezvous_owner(&urls, key), owner);
        assert_eq!(
            chunk_key("bucket", &Path::from("t.lance/data/a.lance"), 7),
            key
        );
    }

    #[test]
    fn topology_lists_healthy_node_endpoints() {
        let document: TopologyDocument = serde_json::from_str(
            r#"{"nodes": [
                {"name": "b", "endpoint": "http://b:1", "healthy": true},
                {"name": "a", "endpoint": "http://a:1"},
                {"name": "c", "endpoint": "http://c:1", "healthy": false},
                {"name": "d", "endpoint": null, "healthy": true}
              ],
              "chunk_size": 4, "hash": "rendezvous-xxh3-64-v1", "routing": "x"}"#,
        )
        .unwrap();
        assert_eq!(document.endpoints(), vec!["http://a:1", "http://b:1"]);
    }

    #[test]
    fn splits_ranges_at_chunk_boundaries() {
        assert_eq!(split_into_chunks(0..0, 4), vec![]);
        assert_eq!(split_into_chunks(1..3, 4), vec![(0, 1..3)]);
        assert_eq!(
            split_into_chunks(3..10, 4),
            vec![(0, 3..4), (1, 4..8), (2, 8..10)]
        );
    }
}
