// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for integrating remap index with datasets
//!

use crate::dataset::index::LanceIndexStoreExt;
use crate::dataset::transaction::{Operation, Transaction};
use crate::index::DatasetIndexInternalExt;
use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::remap::{RemapIndex, RemapIndexDetails, REMAP_INDEX_NAME};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::DatasetIndexExt;
use lance_table::format::Index as IndexMetadata;
use lance_table::format::{pb, Index};
use prost_types::Any;
use snafu::location;
use std::sync::Arc;

pub fn open_remap_index(dataset: &Dataset, index: &Index) -> Result<Arc<RemapIndex>> {
    let details_any = index.index_details.clone();
    if details_any.is_none()
        || !details_any
            .as_ref()
            .unwrap()
            .type_url
            .ends_with("RemapIndexDetails")
    {
        return Err(Error::InvalidInput {
            source: "Details is not for remap index".into(),
            location: location!(),
        });
    }

    let proto = details_any.unwrap().to_msg::<pb::RemapIndexDetails>()?;
    let details = RemapIndexDetails::try_from(proto)?;

    let uuid_str = index.uuid.to_string();
    let index_store = Arc::new(LanceIndexStore::from_dataset(dataset, &uuid_str));
    Ok(Arc::new(RemapIndex::try_from_serialized(
        &details,
        index_store,
    )?))
}

/// Trims version mappings from the remap index that are no longer needed.
/// A version mapping is considered no longer needed if all indices have a dataset_version
/// higher than the version mapping's dataset_version.
///
/// Also attempts to clean up any unreferenced row ID mapping files after trimming.
/// File cleanup failures are logged but do not cause the function to fail.
///
/// Returns Ok(true) if any version mappings were trimmed, Ok(false) if no changes were needed.
pub async fn trim_remap_index(dataset: &mut Dataset) -> Result<bool> {
    let remap_index = if let Some(r) = dataset.open_remap_index(&NoOpMetricsCollector).await? {
        r
    } else {
        return Ok(false);
    };

    let indices = dataset.load_indices().await?;
    let current_index_meta = indices
        .iter()
        .find(|idx| idx.name == REMAP_INDEX_NAME)
        .cloned();

    let mut details = remap_index.details.clone();
    let original_n_versions = details.version_mappings.len();
    let min_index_version = indices
        .iter()
        .filter(|i| i.name != REMAP_INDEX_NAME)
        .map(|i| i.dataset_version)
        .min()
        .unwrap_or(0);
    details
        .version_mappings
        .retain(|vm| vm.dataset_version <= min_index_version);
    if details.version_mappings.len() == original_n_versions {
        return Ok(false);
    }

    let new_id = uuid::Uuid::new_v4();
    let details_proto = pb::RemapIndexDetails::from(&details);
    let updated_index = IndexMetadata {
        uuid: new_id,
        name: REMAP_INDEX_NAME.to_string(),
        fields: vec![],
        dataset_version: dataset.manifest.version,
        fragment_bitmap: None,
        index_details: Some(Any::from_msg(&details_proto)?),
    };

    // Create and commit transaction to update the remap index
    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::CreateIndex {
            new_indices: vec![updated_index],
            removed_indices: vec![current_index_meta.unwrap()],
        },
        None,
        None,
    );

    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await?;
    remap_index
        .as_ref()
        .cleanup_unreferenced_row_id_maps()
        .await?;
    Ok(true)
}
