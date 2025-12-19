// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Storage options provider for dynamic credential fetching
//!
//! This module provides a trait for fetching storage options from various sources
//! (namespace servers, secret managers, etc.) with support for expiration tracking
//! and automatic refresh.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use mock_instant::thread_local::MockClock;
#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "aws")]
use object_store::aws::AmazonS3ConfigKey;
#[cfg(feature = "azure")]
use object_store::azure::AzureConfigKey;
#[cfg(feature = "gcp")]
use object_store::gcp::GoogleConfigKey;

use crate::{Error, Result};

/// Get the current time in milliseconds since UNIX epoch.
/// Uses MockClock in test mode for predictable time-based tests.
#[cfg(test)]
fn current_time_millis() -> u64 {
    MockClock::system_time().as_millis() as u64
}

/// Get the current time in milliseconds since UNIX epoch.
#[cfg(not(test))]
fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}
use async_trait::async_trait;
use lance_namespace::models::DescribeTableRequest;
use lance_namespace::LanceNamespace;
use snafu::location;
use tokio::sync::RwLock;

/// Key for the expiration timestamp in storage options HashMap
pub const EXPIRES_AT_MILLIS_KEY: &str = "expires_at_millis";

/// Trait for providing storage options with expiration tracking
///
/// Implementations can fetch storage options from various sources (namespace servers,
/// secret managers, etc.) and are usable from Python/Java.
///
/// # Current Use Cases
///
/// - **Temporary Credentials**: Fetch short-lived AWS temporary credentials that expire
///   after a set time period, with automatic refresh before expiration
///
/// # Future Possible Use Cases
///
/// - **Dynamic Storage Location Resolution**: Resolve logical names to actual storage
///   locations (bucket aliases, S3 Access Points, region-specific endpoints) that may
///   change based on region, tier, data migration, or failover scenarios
/// - **Runtime S3 Tags Assignment**: Inject cost allocation tags, security labels, or
///   compliance metadata into S3 requests based on the current execution context (user,
///   application, workspace, etc.)
/// - **Dynamic Endpoint Configuration**: Update storage endpoints for disaster recovery,
///   A/B testing, or gradual migration scenarios
/// - **Just-in-time Permission Elevation**: Request elevated permissions only when needed
///   for sensitive operations, then immediately revoke them
/// - **Secret Manager Integration**: Fetch encryption keys from AWS Secrets Manager,
///   Azure Key Vault, or Google Secret Manager with automatic rotation
/// - **OIDC/SAML Federation**: Integrate with identity providers to obtain storage
///   credentials based on user identity and group membership
///
/// # Equality and Hashing
///
/// Implementations must provide `provider_id()` which returns a unique identifier for
/// equality and hashing purposes. Two providers with the same ID are considered equal
/// and will share the same cached ObjectStore in the registry.
#[async_trait]
pub trait StorageOptionsProvider: Send + Sync + fmt::Debug {
    /// Fetch fresh storage options
    ///
    /// Returns None if no storage options are available, or Some(HashMap) with the options.
    /// If the [`EXPIRES_AT_MILLIS_KEY`] key is present in the HashMap, it should contain the
    /// epoch time in milliseconds when the options expire, and credentials will automatically
    /// refresh before expiration.
    /// If [`EXPIRES_AT_MILLIS_KEY`] is not provided, the options are considered to never expire.
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>>;

    /// Return a human-readable unique identifier for this provider instance
    ///
    /// This is used for equality comparison and hashing in the object store registry.
    /// Two providers with the same ID will be treated as equal and share the same cached
    /// ObjectStore.
    ///
    /// The ID should be human-readable for debugging and logging purposes.
    /// For example: `"namespace[dir(root=/data)],table[db$schema$table1]"`
    ///
    /// The ID should uniquely identify the provider's configuration.
    fn provider_id(&self) -> String;
}

/// StorageOptionsProvider implementation that fetches options from a LanceNamespace
pub struct LanceNamespaceStorageOptionsProvider {
    namespace: Arc<dyn LanceNamespace>,
    table_id: Vec<String>,
}

impl fmt::Debug for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl fmt::Display for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl LanceNamespaceStorageOptionsProvider {
    /// Create a new LanceNamespaceStorageOptionsProvider
    ///
    /// # Arguments
    /// * `namespace` - The namespace implementation to fetch storage options from
    /// * `table_id` - The table identifier
    pub fn new(namespace: Arc<dyn LanceNamespace>, table_id: Vec<String>) -> Self {
        Self {
            namespace,
            table_id,
        }
    }
}

#[async_trait]
impl StorageOptionsProvider for LanceNamespaceStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let request = DescribeTableRequest {
            id: Some(self.table_id.clone()),
            version: None,
            with_table_uri: None,
        };

        let response = self
            .namespace
            .describe_table(request)
            .await
            .map_err(|e| Error::IO {
                source: Box::new(std::io::Error::other(format!(
                    "Failed to fetch storage options: {}",
                    e
                ))),
                location: location!(),
            })?;

        Ok(response.storage_options)
    }

    fn provider_id(&self) -> String {
        format!(
            "LanceNamespaceStorageOptionsProvider {{ namespace: {}, table_id: {:?} }}",
            self.namespace.namespace_id(),
            self.table_id
        )
    }
}

/// Cached storage options from the provider with expiration tracking
#[derive(Debug, Clone)]
struct CachedProviderOptions {
    options: HashMap<String, String>,
    expires_at_millis: Option<u64>,
    /// Version number incremented on each refresh, used by credential providers
    /// to know when to re-extract credentials
    version: u64,
}

/// Unified accessor for storage options with optional caching provider
///
/// This struct encapsulates both static storage options and an optional dynamic
/// provider. When both are present, options are merged with provider options
/// taking precedence over initial options.
///
/// Thread-safe with double-check locking pattern to prevent concurrent refreshes.
pub struct StorageOptionsAccessor {
    /// Initial/static storage options (always available immediately)
    /// Wrapped in RwLock to allow applying environment variables
    initial_options: RwLock<Option<HashMap<String, String>>>,
    /// Optional dynamic provider for refreshable options
    provider: Option<Arc<dyn StorageOptionsProvider>>,
    /// Cache for provider options (only used when provider is present)
    provider_cache: Arc<RwLock<Option<CachedProviderOptions>>>,
    /// Duration before expiry to refresh options
    refresh_offset: Duration,
    /// Counter for generating unique version numbers
    next_version: Arc<std::sync::atomic::AtomicU64>,
}

impl fmt::Debug for StorageOptionsAccessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use try_read to avoid blocking in Debug
        let has_options = self
            .initial_options
            .try_read()
            .map(|guard| guard.is_some())
            .unwrap_or(false);
        f.debug_struct("StorageOptionsAccessor")
            .field("initial_options", &has_options)
            .field("provider", &self.provider)
            .field("refresh_offset", &self.refresh_offset)
            .finish()
    }
}

// Hash is based on initial_options and provider's provider_id
// Note: This uses blocking read, so should only be called from sync context
impl std::hash::Hash for StorageOptionsAccessor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash initial options in a deterministic order
        // Use blocking_read since Hash trait is sync
        if let Some(opts) = self.initial_options.blocking_read().as_ref() {
            let mut sorted_keys: Vec<_> = opts.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                key.hash(state);
                opts.get(key).hash(state);
            }
        }
        // Hash provider's provider_id if present
        if let Some(provider) = &self.provider {
            provider.provider_id().hash(state);
        }
    }
}

impl PartialEq for StorageOptionsAccessor {
    fn eq(&self, other: &Self) -> bool {
        // Use blocking_read since PartialEq trait is sync
        let self_opts = self.initial_options.blocking_read();
        let other_opts = other.initial_options.blocking_read();
        *self_opts == *other_opts
            && self.provider.as_ref().map(|p| p.provider_id())
                == other.provider.as_ref().map(|p| p.provider_id())
    }
}

impl Eq for StorageOptionsAccessor {}

impl StorageOptionsAccessor {
    /// Create a new accessor with only static storage options (no provider)
    ///
    /// # Arguments
    /// * `options` - Static storage options
    pub fn new_with_options(options: HashMap<String, String>) -> Self {
        Self {
            initial_options: RwLock::new(Some(options)),
            provider: None,
            provider_cache: Arc::new(RwLock::new(None)),
            refresh_offset: Duration::from_secs(60),
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Create a new accessor with only a dynamic provider (no static options)
    ///
    /// # Arguments
    /// * `provider` - The storage options provider
    /// * `refresh_offset` - Duration before expiry to refresh options (e.g., 60 seconds)
    pub fn new_with_provider(
        provider: Arc<dyn StorageOptionsProvider>,
        refresh_offset: Duration,
    ) -> Self {
        Self {
            initial_options: RwLock::new(None),
            provider: Some(provider),
            provider_cache: Arc::new(RwLock::new(None)),
            refresh_offset,
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Create a new accessor with both static options and a dynamic provider
    ///
    /// When getting options, provider options take precedence over initial options.
    ///
    /// # Arguments
    /// * `initial_options` - Static storage options
    /// * `provider` - The storage options provider
    /// * `refresh_offset` - Duration before expiry to refresh options (e.g., 60 seconds)
    pub fn new_with_options_and_provider(
        initial_options: HashMap<String, String>,
        provider: Arc<dyn StorageOptionsProvider>,
        refresh_offset: Duration,
    ) -> Self {
        Self {
            initial_options: RwLock::new(Some(initial_options)),
            provider: Some(provider),
            provider_cache: Arc::new(RwLock::new(None)),
            refresh_offset,
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Create a new accessor with both static options and a dynamic provider,
    /// with initial provider options pre-populated in the cache
    ///
    /// # Arguments
    /// * `initial_options` - Static storage options
    /// * `provider` - The storage options provider
    /// * `refresh_offset` - Duration before expiry to refresh options
    /// * `initial_provider_options` - Initial provider options to cache
    pub fn new_with_initial_provider_cache(
        initial_options: Option<HashMap<String, String>>,
        provider: Arc<dyn StorageOptionsProvider>,
        refresh_offset: Duration,
        initial_provider_options: HashMap<String, String>,
    ) -> Self {
        let expires_at_millis: Option<u64> = initial_provider_options
            .get(EXPIRES_AT_MILLIS_KEY)
            .and_then(|v| v.parse().ok());
        Self {
            initial_options: RwLock::new(initial_options),
            provider: Some(provider),
            provider_cache: Arc::new(RwLock::new(Some(CachedProviderOptions {
                options: initial_provider_options,
                expires_at_millis,
                version: 1,
            }))),
            refresh_offset,
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(2)),
        }
    }

    /// Get current storage options, merging initial options with provider options.
    /// Provider options take precedence over initial options.
    pub async fn get_options(&self) -> Result<Option<HashMap<String, String>>> {
        let (opts, _version) = self.get_options_with_version().await?;
        Ok(opts)
    }

    /// Get current storage options with version number.
    /// The version number increments on each provider refresh, allowing callers to detect changes.
    pub async fn get_options_with_version(&self) -> Result<(Option<HashMap<String, String>>, u64)> {
        let initial_opts = self.initial_options.read().await.clone();

        // If no provider, just return initial options with version 0
        if self.provider.is_none() {
            return Ok((initial_opts, 0));
        }

        // Get provider options (with caching)
        let (provider_opts, version) = self.get_provider_options_with_version().await?;

        // Merge: start with initial, extend with provider (provider takes precedence)
        let merged = match (initial_opts, provider_opts) {
            (None, None) => None,
            (Some(initial), None) => Some(initial),
            (None, Some(provider)) => Some(provider),
            (Some(mut initial), Some(provider)) => {
                initial.extend(provider);
                Some(initial)
            }
        };

        Ok((merged, version))
    }

    /// Get cached provider options, refreshing if expired.
    async fn get_provider_options_with_version(
        &self,
    ) -> Result<(Option<HashMap<String, String>>, u64)> {
        loop {
            match self.do_get_provider_options().await? {
                Some((opts, version)) => return Ok((opts, version)),
                None => {
                    // Write lock was busy, wait and retry
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Internal implementation that returns None if write lock is busy (signals retry)
    async fn do_get_provider_options(
        &self,
    ) -> Result<Option<(Option<HashMap<String, String>>, u64)>> {
        let provider = self.provider.as_ref().expect("provider must be set");

        // 1. Check cache with read lock (multiple readers allowed)
        {
            let cached = self.provider_cache.read().await;
            if !self.is_cache_expired(&cached) {
                return Ok(Some(match cached.as_ref() {
                    Some(c) => (Some(c.options.clone()), c.version),
                    None => (None, 0),
                }));
            }
        }

        // 2. Try to acquire write lock - return None if busy (another thread refreshing)
        let Ok(mut cache) = self.provider_cache.try_write() else {
            return Ok(None); // Signal caller to retry
        };

        // 3. Double-check: another thread may have refreshed while we waited
        if !self.is_cache_expired(&cache) {
            return Ok(Some(match cache.as_ref() {
                Some(c) => (Some(c.options.clone()), c.version),
                None => (None, 0),
            }));
        }

        // 4. Actually refresh
        log::debug!(
            "Refreshing storage options from provider: {}",
            provider.provider_id()
        );

        let fresh_options = provider.fetch_storage_options().await?;
        let expires_at_millis: Option<u64> = fresh_options
            .as_ref()
            .and_then(|opts| opts.get(EXPIRES_AT_MILLIS_KEY))
            .and_then(|v| v.parse().ok());

        // Get next version number
        let version = self
            .next_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if let Some(expires_at) = expires_at_millis {
            let now_ms = current_time_millis();
            let expires_in_secs = expires_at.saturating_sub(now_ms) / 1000;
            log::debug!(
                "Successfully refreshed storage options from provider: {}, expires in {} seconds (version {})",
                provider.provider_id(),
                expires_in_secs,
                version
            );
        } else {
            log::debug!(
                "Successfully refreshed storage options from provider: {} (no expiration, version {})",
                provider.provider_id(),
                version
            );
        }

        *cache = fresh_options.as_ref().map(|opts| CachedProviderOptions {
            options: opts.clone(),
            expires_at_millis,
            version,
        });

        Ok(Some((fresh_options, version)))
    }

    fn is_cache_expired(&self, cached: &Option<CachedProviderOptions>) -> bool {
        match cached {
            None => true,
            Some(c) => match c.expires_at_millis {
                None => false, // No expiration = never expires
                Some(expires_at) => {
                    let now_ms = current_time_millis();
                    now_ms + self.refresh_offset.as_millis() as u64 >= expires_at
                }
            },
        }
    }

    /// Check if this accessor has a provider
    pub fn has_provider(&self) -> bool {
        self.provider.is_some()
    }

    /// Find the configured Azure storage account name from initial options (sync)
    ///
    /// This checks the initial_options for Azure account name configuration.
    /// Falls back to environment variables if not found in options.
    #[cfg(feature = "azure")]
    pub fn find_azure_storage_account(&self) -> Option<String> {
        self.initial_options
            .blocking_read()
            .as_ref()
            .and_then(Self::find_configured_storage_account)
    }

    /// Get the provider, if present
    pub fn provider(&self) -> Option<Arc<dyn StorageOptionsProvider>> {
        self.provider.clone()
    }

    /// Create a new accessor with updated initial options, preserving the provider
    ///
    /// This is useful when you need to merge additional options into an existing
    /// accessor while keeping the same provider for dynamic refresh.
    ///
    /// # Arguments
    /// * `new_initial_options` - The new initial options (replaces existing)
    /// * `refresh_offset` - Duration before expiry to refresh options
    pub fn with_updated_initial_options(
        &self,
        new_initial_options: HashMap<String, String>,
        refresh_offset: Duration,
    ) -> Self {
        Self {
            initial_options: RwLock::new(Some(new_initial_options)),
            provider: self.provider.clone(),
            // Create new cache - we don't copy the old cache since credentials
            // should be fresh from the provider
            provider_cache: Arc::new(RwLock::new(None)),
            refresh_offset,
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Create a new accessor by merging additional options with this accessor's initial options
    /// and setting a new provider.
    ///
    /// The additional options take precedence over existing initial options.
    ///
    /// # Arguments
    /// * `additional_options` - Options to merge (these take precedence)
    /// * `provider` - The new storage options provider
    /// * `refresh_offset` - Duration before expiry to refresh options
    pub fn merge_with_provider(
        &self,
        additional_options: HashMap<String, String>,
        provider: Arc<dyn StorageOptionsProvider>,
        refresh_offset: Duration,
    ) -> Self {
        let mut merged = self
            .initial_options
            .blocking_read()
            .clone()
            .unwrap_or_default();
        merged.extend(additional_options);
        Self {
            initial_options: RwLock::new(Some(merged)),
            provider: Some(provider),
            provider_cache: Arc::new(RwLock::new(None)),
            refresh_offset,
            next_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Apply S3 environment variables to initial options
    ///
    /// This mutates the accessor's internal state to include S3-specific environment variables.
    /// Should be called once when the accessor is first used for S3 operations.
    #[cfg(feature = "aws")]
    pub async fn apply_s3_env(&self) {
        let mut opts = self.initial_options.write().await;
        let options = opts.get_or_insert_with(HashMap::new);
        Self::apply_env_overrides(options);
        Self::apply_env_s3(options);
    }

    /// Apply Azure environment variables to initial options
    ///
    /// This mutates the accessor's internal state to include Azure-specific environment variables.
    /// Should be called once when the accessor is first used for Azure operations.
    #[cfg(feature = "azure")]
    pub async fn apply_azure_env(&self) {
        let mut opts = self.initial_options.write().await;
        let options = opts.get_or_insert_with(HashMap::new);
        Self::apply_env_overrides(options);
        let env_options = Self::from_env_azure();
        Self::apply_env_azure(options, &env_options);
    }

    /// Apply GCS environment variables to initial options
    ///
    /// This mutates the accessor's internal state to include GCS-specific environment variables.
    /// Should be called once when the accessor is first used for GCS operations.
    #[cfg(feature = "gcp")]
    pub async fn apply_gcs_env(&self) {
        let mut opts = self.initial_options.write().await;
        let options = opts.get_or_insert_with(HashMap::new);
        Self::apply_env_overrides(options);
        Self::apply_env_gcs(options);
    }

    // =========================================================================
    // Async helper methods for extracting configuration from merged options
    // These use get_options() to get merged/refreshed options from the provider
    // =========================================================================

    /// Apply environment variable overrides to a HashMap of storage options
    ///
    /// This reads common environment variables and adds them to the options if present:
    /// - AZURE_STORAGE_ALLOW_HTTP / AZURE_STORAGE_USE_HTTP -> allow_http
    /// - AWS_ALLOW_HTTP -> allow_http
    /// - OBJECT_STORE_CLIENT_MAX_RETRIES -> client_max_retries
    /// - OBJECT_STORE_CLIENT_RETRY_TIMEOUT -> client_retry_timeout
    pub fn apply_env_overrides(options: &mut HashMap<String, String>) {
        if let Ok(value) = std::env::var("AZURE_STORAGE_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AZURE_STORAGE_USE_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AWS_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("OBJECT_STORE_CLIENT_MAX_RETRIES") {
            options.insert("client_max_retries".into(), value);
        }
        if let Ok(value) = std::env::var("OBJECT_STORE_CLIENT_RETRY_TIMEOUT") {
            options.insert("client_retry_timeout".into(), value);
        }
    }

    /// Get a value from merged options by key (async, refreshes if needed)
    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let opts = self.get_options().await?;
        Ok(opts.and_then(|o| o.get(key).cloned()))
    }

    /// Denotes if unsecure connections via http are allowed (async, uses merged options)
    pub async fn allow_http(&self) -> Result<bool> {
        let opts = self.get_options().await?;
        Ok(opts
            .map(|o| {
                o.iter().any(|(key, value)| {
                    key.to_ascii_lowercase().contains("allow_http") && str_is_truthy(value)
                })
            })
            .unwrap_or(false))
    }

    /// Number of times to retry a download that fails (async, uses merged options)
    pub async fn download_retry_count(&self) -> Result<usize> {
        let opts = self.get_options().await?;
        Ok(opts
            .and_then(|o| {
                o.iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("download_retry_count"))
                    .map(|(_, value)| value.parse::<usize>().unwrap_or(3))
            })
            .unwrap_or(3))
    }

    /// Max retry times to set in RetryConfig for object store client (async, uses merged options)
    pub async fn client_max_retries(&self) -> Result<usize> {
        let opts = self.get_options().await?;
        Ok(opts
            .and_then(|o| {
                o.iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("client_max_retries"))
                    .and_then(|(_, value)| value.parse::<usize>().ok())
            })
            .unwrap_or(10))
    }

    /// Seconds of timeout to set in RetryConfig for object store client (async, uses merged options)
    pub async fn client_retry_timeout(&self) -> Result<u64> {
        let opts = self.get_options().await?;
        Ok(opts
            .and_then(|o| {
                o.iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("client_retry_timeout"))
                    .and_then(|(_, value)| value.parse::<u64>().ok())
            })
            .unwrap_or(180))
    }

    /// Get the expiration time in milliseconds since epoch, if present (async, uses merged options)
    pub async fn expires_at_millis(&self) -> Result<Option<u64>> {
        let opts = self.get_options().await?;
        Ok(opts.and_then(|o| {
            o.get(EXPIRES_AT_MILLIS_KEY)
                .and_then(|s| s.parse::<u64>().ok())
        }))
    }

    /// Check if use_opendal flag is set (async, uses merged options)
    pub async fn use_opendal(&self) -> Result<bool> {
        let opts = self.get_options().await?;
        Ok(opts
            .and_then(|o| o.get("use_opendal").map(|v| v.as_str() == "true"))
            .unwrap_or(false))
    }

    /// Get DynamoDB endpoint for S3 commit lock (async, uses merged options, falls back to env var)
    #[cfg(feature = "aws")]
    pub async fn dynamodb_endpoint(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?;
        let from_opts = opts.and_then(|o| {
            o.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("dynamodb_endpoint"))
                .map(|(_, value)| value.clone())
        });
        // Fall back to environment variable if not in options
        Ok(from_opts.or_else(|| std::env::var("DYNAMODB_ENDPOINT").ok()))
    }

    /// Get Hugging Face token (async, uses merged options, checks env vars as fallback)
    pub async fn hf_token(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?;
        Ok(opts
            .and_then(|o| o.get("hf_token").cloned())
            .or_else(|| std::env::var("HF_TOKEN").ok())
            .or_else(|| std::env::var("HUGGINGFACE_TOKEN").ok()))
    }

    /// Get Hugging Face revision (async, uses merged options)
    pub async fn hf_revision(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?;
        Ok(opts.and_then(|o| o.get("hf_revision").cloned()))
    }

    /// Get Hugging Face root path (async, uses merged options)
    pub async fn hf_root(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?;
        Ok(opts.and_then(|o| o.get("hf_root").cloned()))
    }

    // =========================================================================
    // AWS S3 provider-specific methods
    // =========================================================================

    /// Add values from the environment to storage options for S3
    #[cfg(feature = "aws")]
    pub fn apply_env_s3(options: &mut HashMap<String, String>) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !options.contains_key(config_key.as_ref()) {
                        options.insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }
    }

    /// Convert storage options to S3-specific options map (static helper)
    #[cfg(feature = "aws")]
    pub fn as_s3_options(options: &HashMap<String, String>) -> HashMap<AmazonS3ConfigKey, String> {
        options
            .iter()
            .filter_map(|(key, value)| {
                let s3_key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((s3_key, value.clone()))
            })
            .collect()
    }

    /// Get merged storage options as S3-specific options map (async, refreshes if needed)
    #[cfg(feature = "aws")]
    pub async fn get_s3_options(&self) -> Result<HashMap<AmazonS3ConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_s3_options(&opts))
    }

    /// Get merged storage options for S3 (async, refreshes if needed)
    ///
    /// Note: Call `apply_s3_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "aws")]
    pub async fn get_s3_storage_options(&self) -> Result<HashMap<String, String>> {
        Ok(self.get_options().await?.unwrap_or_default())
    }

    /// Get merged storage options as S3-specific options map
    ///
    /// Note: Call `apply_s3_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "aws")]
    pub async fn get_s3_options_with_env(&self) -> Result<HashMap<AmazonS3ConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_s3_options(&opts))
    }

    /// Check if use_opendal flag is set for S3
    #[cfg(feature = "aws")]
    pub async fn s3_use_opendal(&self) -> Result<bool> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts
            .get("use_opendal")
            .map(|v| v == "true")
            .unwrap_or(false))
    }

    /// Check if S3 Express mode is enabled
    #[cfg(feature = "aws")]
    pub async fn s3_express(&self) -> Result<bool> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("s3_express").map(|v| v == "true").unwrap_or(false))
    }

    /// Check if this is Cloudflare R2 (requires constant size upload parts)
    #[cfg(feature = "aws")]
    pub async fn is_cloudflare_r2(&self) -> Result<bool> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts
            .get("aws_endpoint")
            .map(|endpoint| endpoint.contains("r2.cloudflarestorage.com"))
            .unwrap_or(false))
    }

    // =========================================================================
    // Azure provider-specific methods
    // =========================================================================

    /// Create storage options from environment variables for Azure
    #[cfg(feature = "azure")]
    pub fn from_env_azure() -> HashMap<String, String> {
        let mut opts = HashMap::<String, String>::new();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AzureConfigKey::from_str(&key.to_ascii_lowercase()) {
                    opts.insert(config_key.as_ref().to_string(), value.to_string());
                }
            }
        }
        opts
    }

    /// Add values from the environment to storage options for Azure
    #[cfg(feature = "azure")]
    pub fn apply_env_azure(
        options: &mut HashMap<String, String>,
        env_options: &HashMap<String, String>,
    ) {
        for (os_key, os_value) in env_options {
            if !options.contains_key(os_key) {
                options.insert(os_key.clone(), os_value.clone());
            }
        }
    }

    /// Convert storage options to Azure-specific options map (static helper)
    #[cfg(feature = "azure")]
    pub fn as_azure_options(options: &HashMap<String, String>) -> HashMap<AzureConfigKey, String> {
        options
            .iter()
            .filter_map(|(key, value)| {
                let az_key = AzureConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((az_key, value.clone()))
            })
            .collect()
    }

    /// Get merged storage options as Azure-specific options map (async, refreshes if needed)
    #[cfg(feature = "azure")]
    pub async fn get_azure_options(&self) -> Result<HashMap<AzureConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_azure_options(&opts))
    }

    /// Get merged storage options for Azure (async, refreshes if needed)
    ///
    /// Note: Call `apply_azure_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "azure")]
    pub async fn get_azure_storage_options(&self) -> Result<HashMap<String, String>> {
        Ok(self.get_options().await?.unwrap_or_default())
    }

    /// Get merged storage options as Azure-specific options map
    ///
    /// Note: Call `apply_azure_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "azure")]
    pub async fn get_azure_options_with_env(&self) -> Result<HashMap<AzureConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_azure_options(&opts))
    }

    /// Check if use_opendal flag is set for Azure
    #[cfg(feature = "azure")]
    pub async fn azure_use_opendal(&self) -> Result<bool> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts
            .get("use_opendal")
            .map(|v| v == "true")
            .unwrap_or(false))
    }

    /// Find the configured Azure storage account name from options (static helper)
    #[cfg(feature = "azure")]
    #[allow(clippy::manual_map)]
    pub fn find_configured_storage_account(options: &HashMap<String, String>) -> Option<String> {
        if let Some(account) = options.get("azure_storage_account_name") {
            Some(account.clone())
        } else if let Some(account) = options.get("account_name") {
            Some(account.clone())
        } else {
            None
        }
    }

    /// Get the configured Azure storage account name (async, uses merged options)
    #[cfg(feature = "azure")]
    pub async fn get_azure_storage_account(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::find_configured_storage_account(&opts))
    }

    // =========================================================================
    // GCS provider-specific methods
    // =========================================================================

    /// Add values from the environment to storage options for GCS
    #[cfg(feature = "gcp")]
    pub fn apply_env_gcs(options: &mut HashMap<String, String>) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                let lowercase_key = key.to_ascii_lowercase();
                let token_key = "google_storage_token";

                if let Ok(config_key) = GoogleConfigKey::from_str(&lowercase_key) {
                    if !options.contains_key(config_key.as_ref()) {
                        options.insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
                // Check for GOOGLE_STORAGE_TOKEN until GoogleConfigKey supports storage token
                else if lowercase_key == token_key && !options.contains_key(token_key) {
                    options.insert(token_key.to_string(), value.to_string());
                }
            }
        }
    }

    /// Convert storage options to GCS-specific options map (static helper)
    #[cfg(feature = "gcp")]
    pub fn as_gcs_options(options: &HashMap<String, String>) -> HashMap<GoogleConfigKey, String> {
        options
            .iter()
            .filter_map(|(key, value)| {
                let gcs_key = GoogleConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((gcs_key, value.clone()))
            })
            .collect()
    }

    /// Get merged storage options as GCS-specific options map (async, refreshes if needed)
    #[cfg(feature = "gcp")]
    pub async fn get_gcs_options(&self) -> Result<HashMap<GoogleConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_gcs_options(&opts))
    }

    /// Get merged storage options for GCS (async, refreshes if needed)
    ///
    /// Note: Call `apply_gcs_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "gcp")]
    pub async fn get_gcs_storage_options(&self) -> Result<HashMap<String, String>> {
        Ok(self.get_options().await?.unwrap_or_default())
    }

    /// Get merged storage options as GCS-specific options map
    ///
    /// Note: Call `apply_gcs_env()` once before using this method to ensure
    /// environment variables are applied to the accessor.
    #[cfg(feature = "gcp")]
    pub async fn get_gcs_options_with_env(&self) -> Result<HashMap<GoogleConfigKey, String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(Self::as_gcs_options(&opts))
    }

    /// Check if use_opendal flag is set for GCS
    #[cfg(feature = "gcp")]
    pub async fn gcs_use_opendal(&self) -> Result<bool> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts
            .get("use_opendal")
            .map(|v| v == "true")
            .unwrap_or(false))
    }

    /// Get Google storage token (async, uses merged options)
    #[cfg(feature = "gcp")]
    pub async fn google_storage_token(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("google_storage_token").cloned())
    }

    // =========================================================================
    // OSS provider-specific methods
    // =========================================================================

    /// Get OSS endpoint (async, uses merged options)
    pub async fn oss_endpoint(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("oss_endpoint").cloned())
    }

    /// Get OSS access key ID (async, uses merged options)
    pub async fn oss_access_key_id(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("oss_access_key_id").cloned())
    }

    /// Get OSS secret access key (async, uses merged options)
    pub async fn oss_secret_access_key(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("oss_secret_access_key").cloned())
    }

    /// Get OSS region (async, uses merged options)
    pub async fn oss_region(&self) -> Result<Option<String>> {
        let opts = self.get_options().await?.unwrap_or_default();
        Ok(opts.get("oss_region").cloned())
    }
}

/// Check if a string value is truthy (true, 1, yes, on)
fn str_is_truthy(val: &str) -> bool {
    let lower = val.to_ascii_lowercase();
    lower == "1" || lower == "true" || lower == "on" || lower == "yes"
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;

    #[derive(Debug)]
    struct MockStorageOptionsProvider {
        call_count: Arc<RwLock<usize>>,
        expires_in_millis: Option<u64>,
    }

    impl MockStorageOptionsProvider {
        fn new(expires_in_millis: Option<u64>) -> Self {
            Self {
                call_count: Arc::new(RwLock::new(0)),
                expires_in_millis,
            }
        }

        async fn get_call_count(&self) -> usize {
            *self.call_count.read().await
        }
    }

    #[async_trait]
    impl StorageOptionsProvider for MockStorageOptionsProvider {
        async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
            let count = {
                let mut c = self.call_count.write().await;
                *c += 1;
                *c
            };

            let mut options = HashMap::from([
                ("key".to_string(), format!("value_{}", count)),
                ("count".to_string(), count.to_string()),
            ]);

            if let Some(expires_in) = self.expires_in_millis {
                let now_ms = MockClock::system_time().as_millis() as u64;
                let expires_at = now_ms + expires_in;
                options.insert(EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string());
            }

            Ok(Some(options))
        }

        fn provider_id(&self) -> String {
            let ptr = Arc::as_ptr(&self.call_count) as usize;
            format!("MockStorageOptionsProvider {{ id: {} }}", ptr)
        }
    }

    #[tokio::test]
    async fn test_accessor_initial_fetch() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns options expiring in 10 minutes
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create accessor with only a provider (no initial options)
        let accessor = StorageOptionsAccessor::new_with_provider(
            mock_provider.clone(),
            Duration::from_secs(300),
        );

        // First call should fetch from provider
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("key"), Some(&"value_1".to_string()));
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Second call should use cached options (not expired yet)
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("key"), Some(&"value_1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1); // Still 1, didn't fetch again
    }

    #[tokio::test]
    async fn test_accessor_with_initial_provider_cache() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create a mock provider
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create initial provider options that expire in 10 minutes
        let expires_at = now_ms + 600_000;
        let initial_provider_options = HashMap::from([
            ("key".to_string(), "initial_value".to_string()),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
        ]);

        // Create accessor with initial provider cache
        let accessor = StorageOptionsAccessor::new_with_initial_provider_cache(
            None,
            mock_provider.clone(),
            Duration::from_secs(300),
            initial_provider_options,
        );

        // First call should use initial cached options
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("key"), Some(&"initial_value".to_string()));

        // Should not have called the provider yet
        assert_eq!(mock_provider.get_call_count().await, 0);
    }

    #[tokio::test]
    async fn test_accessor_refresh_on_expiration() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns options expiring in 10 minutes
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create accessor with 5 minute refresh offset
        let accessor = StorageOptionsAccessor::new_with_provider(
            mock_provider.clone(),
            Duration::from_secs(300),
        );

        // First call should fetch from provider
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Advance time to 6 minutes - should trigger refresh (within 5 min refresh offset)
        MockClock::set_system_time(Duration::from_secs(100_000 + 360));
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"2".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 2);

        // Advance time to 11 minutes total - should trigger another refresh
        MockClock::set_system_time(Duration::from_secs(100_000 + 660));
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"3".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 3);
    }

    #[tokio::test]
    async fn test_accessor_refresh_lead_time() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns options expiring in 4 minutes
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(240_000)));

        // Create accessor with 5 minute refresh offset
        // This means options should be refreshed when they have less than 5 minutes left
        let accessor = StorageOptionsAccessor::new_with_provider(
            mock_provider.clone(),
            Duration::from_secs(300),
        );

        // First call should fetch (no initial cache)
        // Options expire in 4 minutes, which is less than our 5 minute refresh offset,
        // so they should be considered "needs refresh" immediately
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Second call should trigger refresh because options expire in 4 minutes
        // but our refresh lead time is 5 minutes (now + 5min > expires_at)
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"2".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 2);
    }

    #[tokio::test]
    async fn test_accessor_no_expiration() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns options WITHOUT expiration
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(None));

        // Create accessor
        let accessor = StorageOptionsAccessor::new_with_provider(
            mock_provider.clone(),
            Duration::from_secs(300),
        );

        // First call should fetch
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Advance time significantly - should NOT trigger refresh (no expiration)
        MockClock::set_system_time(Duration::from_secs(100_000 + 3600)); // 1 hour later
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
        assert_eq!(mock_provider.get_call_count().await, 1); // Still 1, didn't refresh
    }

    #[tokio::test]
    async fn test_accessor_concurrent_access() {
        // Create a mock provider with far future expiration
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(9999999999999)));

        let accessor = Arc::new(StorageOptionsAccessor::new_with_provider(
            mock_provider.clone(),
            Duration::from_secs(300),
        ));

        // Spawn 10 concurrent tasks that all try to get options at the same time
        let mut handles = vec![];
        for i in 0..10 {
            let accessor = accessor.clone();
            let handle = tokio::spawn(async move {
                let opts = accessor.get_options().await.unwrap().unwrap();
                // Verify we got the correct options (should all be count=1 from first fetch)
                assert_eq!(opts.get("count"), Some(&"1".to_string()));
                i // Return task number for verification
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 10 tasks completed successfully
        assert_eq!(results.len(), 10);
        for i in 0..10 {
            assert!(results.contains(&i));
        }

        // The provider should have been called exactly once (first request triggers fetch,
        // subsequent requests use cache)
        let call_count = mock_provider.get_call_count().await;
        assert_eq!(
            call_count, 1,
            "Provider should be called exactly once despite concurrent access"
        );
    }

    #[tokio::test]
    async fn test_accessor_concurrent_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create initial options that expired in the past
        let expires_at = now_ms - 1_000_000;
        let initial_provider_options = HashMap::from([
            ("key".to_string(), "old_value".to_string()),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
        ]);

        // Mock will return options expiring in 1 hour
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(3_600_000)));

        let accessor = Arc::new(StorageOptionsAccessor::new_with_initial_provider_cache(
            None,
            mock_provider.clone(),
            Duration::from_secs(300),
            initial_provider_options,
        ));

        // Spawn 20 concurrent tasks that all try to get options at the same time
        // Since the initial options are expired, they'll all try to refresh
        let mut handles = vec![];
        for i in 0..20 {
            let accessor = accessor.clone();
            let handle = tokio::spawn(async move {
                let opts = accessor.get_options().await.unwrap().unwrap();
                // All should get the new options (count=1 from first fetch)
                assert_eq!(opts.get("count"), Some(&"1".to_string()));
                i
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 20 tasks completed successfully
        assert_eq!(results.len(), 20);

        // The provider should have been called at least once, but possibly more times
        // due to the try_write mechanism and race conditions
        let call_count = mock_provider.get_call_count().await;
        assert!(
            call_count >= 1,
            "Provider should be called at least once, was called {} times",
            call_count
        );

        // It shouldn't be called 20 times though - the lock should prevent most concurrent fetches
        assert!(
            call_count < 10,
            "Provider should not be called too many times due to lock contention, was called {} times",
            call_count
        );
    }

    #[tokio::test]
    async fn test_accessor_static_options_only() {
        // Create accessor with only static options (no provider)
        let static_options = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let accessor = StorageOptionsAccessor::new_with_options(static_options);

        // Should return static options immediately
        let opts = accessor.get_options().await.unwrap().unwrap();
        assert_eq!(opts.get("key1"), Some(&"value1".to_string()));
        assert_eq!(opts.get("key2"), Some(&"value2".to_string()));

        // Version should be 0 (no provider)
        let (opts, version) = accessor.get_options_with_version().await.unwrap();
        assert_eq!(version, 0);
        assert!(opts.is_some());
    }

    #[tokio::test]
    async fn test_accessor_merges_initial_and_provider_options() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create static options
        let initial_options = HashMap::from([
            ("static_key".to_string(), "static_value".to_string()),
            ("shared_key".to_string(), "initial_value".to_string()),
        ]);

        // Provider returns different options
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create accessor with both
        let accessor = StorageOptionsAccessor::new_with_options_and_provider(
            initial_options,
            mock_provider,
            Duration::from_secs(300),
        );

        // Should merge: initial + provider (provider takes precedence for shared keys)
        let opts = accessor.get_options().await.unwrap().unwrap();

        // Static key should be present
        assert_eq!(opts.get("static_key"), Some(&"static_value".to_string()));

        // Provider keys should be present
        assert_eq!(opts.get("key"), Some(&"value_1".to_string()));
        assert_eq!(opts.get("count"), Some(&"1".to_string()));
    }
}
