// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{services::Azblob, Operator};
use snafu::location;

use object_store::{azure::MicrosoftAzureBuilder, RetryConfig};
use url::Url;

use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptionsAccessor,
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE,
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct AzureBlobStoreProvider;

impl AzureBlobStoreProvider {
    async fn build_opendal_azure_store(
        &self,
        base_path: &Url,
        storage_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let container = base_path
            .host_str()
            .ok_or_else(|| {
                Error::invalid_input("Azure URL must contain container name", location!())
            })?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Start with all storage options as the config map
        // OpenDAL will handle environment variables through its default credentials chain
        let mut config_map: HashMap<String, String> = storage_options.clone();

        // Set required OpenDAL configuration
        config_map.insert("container".to_string(), container);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), format!("/{}", prefix));
        }

        let operator = Operator::from_iter::<Azblob>(config_map)
            .map_err(|e| {
                Error::invalid_input(
                    format!("Failed to create Azure Blob operator: {:?}", e),
                    location!(),
                )
            })?
            .finish();

        Ok(Arc::new(OpendalStore::new(operator)) as Arc<dyn OSObjectStore>)
    }

    async fn build_microsoft_azure_store(
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

        let mut builder = MicrosoftAzureBuilder::new()
            .with_url(base_path.as_ref())
            .with_retry(retry_config);
        for (key, value) in StorageOptionsAccessor::as_azure_options(storage_options) {
            builder = builder.with_config(key, value);
        }

        Ok(Arc::new(builder.build()?) as Arc<dyn OSObjectStore>)
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for AzureBlobStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let accessor = params.accessor();

        // Apply Azure environment variables to the accessor once
        accessor.apply_azure_env().await;

        // Get configuration from merged options
        let storage_options = accessor.get_azure_storage_options().await?;
        let download_retry_count = accessor.download_retry_count().await?;
        let use_opendal = accessor.azure_use_opendal().await?;

        let inner = if use_opendal {
            self.build_opendal_azure_store(&base_path, &storage_options)
                .await?
        } else {
            self.build_microsoft_azure_store(&base_path, &accessor, &storage_options)
                .await?
        };

        Ok(ObjectStore {
            inner,
            scheme: String::from("az"),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: true,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
        })
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        accessor: &StorageOptionsAccessor,
    ) -> Result<String> {
        let authority = url.authority();
        let (container, account) = match authority.find("@") {
            Some(at_index) => {
                // The URI looks like 'az://container@account.dfs.core.windows.net/path-part/file',
                // or possibly 'az://container@account/path-part/file'.
                let container = &authority[..at_index];
                let account = &authority[at_index + 1..];
                (
                    container,
                    account.split(".").next().unwrap_or_default().to_string(),
                )
            }
            None => {
                // The URI looks like 'az://container/path-part/file'.
                // We must look at the storage options to find the account.
                let account = accessor
                    .find_azure_storage_account()
                    .or_else(|| {
                        StorageOptionsAccessor::find_configured_storage_account(&ENV_OPTIONS)
                    })
                    .ok_or(Error::invalid_input(
                        "Unable to find object store prefix: no Azure account name in URI, and no storage account configured.",
                        location!(),
                    ))?;
                (authority, account)
            }
        };
        Ok(format!("{}${}@{}", url.scheme(), container, account))
    }
}

static ENV_OPTIONS: LazyLock<HashMap<String, String>> =
    LazyLock::new(StorageOptionsAccessor::from_env_azure);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::ObjectStoreParams;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_azure_store_path() {
        let provider = AzureBlobStoreProvider;

        let url = Url::parse("az://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_use_opendal_flag() {
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("az://test-container/path").unwrap();
        let params_with_flag = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::new_with_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    ("account_name".to_string(), "test_account".to_string()),
                    (
                        "endpoint".to_string(),
                        "https://test_account.blob.core.windows.net".to_string(),
                    ),
                    (
                        "account_key".to_string(),
                        "dGVzdF9hY2NvdW50X2tleQ==".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider
            .new_store(url.clone(), &params_with_flag)
            .await
            .unwrap();
        assert_eq!(store.scheme, "az");
    }

    #[test]
    fn test_find_configured_storage_account() {
        assert_eq!(
            Some("myaccount".to_string()),
            StorageOptionsAccessor::find_configured_storage_account(&HashMap::from_iter(
                [
                    ("access_key".to_string(), "myaccesskey".to_string()),
                    (
                        "azure_storage_account_name".to_string(),
                        "myaccount".to_string()
                    )
                ]
                .into_iter()
            ))
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_from_url_and_options() {
        let provider = AzureBlobStoreProvider;
        let accessor = StorageOptionsAccessor::new_with_options(HashMap::from_iter([(
            "account_name".to_string(),
            "bob".to_string(),
        )]));
        assert_eq!(
            "az$container@bob",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("az://container/path").unwrap(),
                    &accessor
                )
                .unwrap()
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_from_url_and_ignored_options() {
        let provider = AzureBlobStoreProvider;
        let accessor = StorageOptionsAccessor::new_with_options(HashMap::from_iter([(
            "account_name".to_string(),
            "bob".to_string(),
        )]));
        assert_eq!(
            "az$container@account",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("az://container@account.dfs.core.windows.net/path").unwrap(),
                    &accessor
                )
                .unwrap()
        );
    }

    #[test]
    fn test_fail_to_calculate_object_store_prefix_from_url() {
        let provider = AzureBlobStoreProvider;
        let accessor = StorageOptionsAccessor::new_with_options(HashMap::from_iter([(
            "access_key".to_string(),
            "myaccesskey".to_string(),
        )]));
        let expected = "Invalid user input: Unable to find object store prefix: no Azure account name in URI, and no storage account configured.";
        let result = provider
            .calculate_object_store_prefix(&Url::parse("az://container/path").unwrap(), &accessor)
            .expect_err("expected error")
            .to_string();
        assert_eq!(expected, &result[..expected.len()]);
    }
}
