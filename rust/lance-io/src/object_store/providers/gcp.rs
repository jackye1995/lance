// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, sync::Arc, time::Duration};

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{services::Gcs, Operator};
use snafu::location;

use object_store::{
    gcp::{GcpCredential, GoogleCloudStorageBuilder},
    RetryConfig, StaticCredentialProvider,
};
use url::Url;

use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptionsAccessor,
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE,
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct GcsStoreProvider;

impl GcsStoreProvider {
    async fn build_opendal_gcs_store(
        &self,
        base_path: &Url,
        storage_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("GCS URL must contain bucket name", location!()))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Start with all storage options as the config map
        // OpenDAL will handle environment variables through its default credentials chain
        let mut config_map: HashMap<String, String> = storage_options.clone();

        // Set required OpenDAL configuration
        config_map.insert("bucket".to_string(), bucket);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), format!("/{}", prefix));
        }

        let operator = Operator::from_iter::<Gcs>(config_map)
            .map_err(|e| {
                Error::invalid_input(
                    format!("Failed to create GCS operator: {:?}", e),
                    location!(),
                )
            })?
            .finish();

        Ok(Arc::new(OpendalStore::new(operator)) as Arc<dyn OSObjectStore>)
    }

    async fn build_google_cloud_store(
        &self,
        base_path: &Url,
        accessor: &Arc<StorageOptionsAccessor>,
        storage_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let max_retries = accessor.client_max_retries().await?;
        let retry_timeout = accessor.client_retry_timeout().await?;
        let retry_config = RetryConfig {
            backoff: Default::default(),
            max_retries,
            retry_timeout: Duration::from_secs(retry_timeout),
        };

        let mut builder = GoogleCloudStorageBuilder::new()
            .with_url(base_path.as_ref())
            .with_retry(retry_config);
        for (key, value) in StorageOptionsAccessor::as_gcs_options(storage_options) {
            builder = builder.with_config(key, value);
        }
        let storage_token = accessor.google_storage_token().await?;
        if let Some(token) = storage_token {
            let credential = GcpCredential { bearer: token };
            let credential_provider = Arc::new(StaticCredentialProvider::new(credential)) as _;
            builder = builder.with_credentials(credential_provider);
        }

        Ok(Arc::new(builder.build()?) as Arc<dyn OSObjectStore>)
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for GcsStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let accessor = params.accessor();

        // Apply GCS environment variables to the accessor once
        accessor.apply_gcs_env().await;

        // Get configuration from merged options
        let storage_options = accessor.get_gcs_storage_options().await?;
        let download_retry_count = accessor.download_retry_count().await?;
        let use_opendal = accessor.gcs_use_opendal().await?;

        let inner = if use_opendal {
            self.build_opendal_gcs_store(&base_path, &storage_options)
                .await?
        } else {
            self.build_google_cloud_store(&base_path, &accessor, &storage_options)
                .await?
        };

        Ok(ObjectStore {
            inner,
            scheme: String::from("gs"),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: true,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::ObjectStoreParams;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_gcs_store_path() {
        let provider = GcsStoreProvider;

        let url = Url::parse("gs://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_use_opendal_flag() {
        let provider = GcsStoreProvider;
        let url = Url::parse("gs://test-bucket/path").unwrap();
        let params_with_flag = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::new_with_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    (
                        "service_account".to_string(),
                        "test@example.iam.gserviceaccount.com".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider
            .new_store(url.clone(), &params_with_flag)
            .await
            .unwrap();
        assert_eq!(store.scheme, "gs");
    }
}
