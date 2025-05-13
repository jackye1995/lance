// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::scalar::IndexStore;
use crate::{Index, IndexType};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use lance_table::format::pb;
use lance_table::format::Fragment;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use snafu::location;
use std::collections::HashSet;
use std::{any::Any, collections::HashMap, sync::Arc};
use uuid::Uuid;

pub const REMAP_INDEX_NAME: &str = "remap_index";

/// Internal representation of NewFragmentSignature
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct NewFragmentSignature {
    pub fragment_id: u64,
    pub num_rows: u64,
}

impl From<&NewFragmentSignature> for pb::remap_index_details::NewFragmentSignature {
    fn from(sig: &NewFragmentSignature) -> Self {
        Self {
            fragment_id: sig.fragment_id,
            num_rows: sig.num_rows,
        }
    }
}

impl TryFrom<pb::remap_index_details::NewFragmentSignature> for NewFragmentSignature {
    type Error = Error;

    fn try_from(sig: pb::remap_index_details::NewFragmentSignature) -> Result<Self> {
        Ok(Self {
            fragment_id: sig.fragment_id,
            num_rows: sig.num_rows,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct FragmentMapping {
    pub old_fragments: Vec<Fragment>,
    pub new_fragment_signatures: Vec<NewFragmentSignature>,
}

impl From<&FragmentMapping> for pb::remap_index_details::FragmentMapping {
    fn from(mapping: &FragmentMapping) -> Self {
        Self {
            old_fragments: mapping
                .old_fragments
                .iter()
                .map(pb::DataFragment::from)
                .collect(),
            new_fragment_signatures: mapping
                .new_fragment_signatures
                .iter()
                .map(|sig| sig.into())
                .collect(),
        }
    }
}

impl TryFrom<pb::remap_index_details::FragmentMapping> for FragmentMapping {
    type Error = Error;

    fn try_from(mapping: pb::remap_index_details::FragmentMapping) -> Result<Self> {
        Ok(Self {
            old_fragments: mapping
                .old_fragments
                .into_iter()
                .map(Fragment::try_from)
                .collect::<Result<_>>()?,
            new_fragment_signatures: mapping
                .new_fragment_signatures
                .into_iter()
                .map(|sig| sig.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct VersionFragmentMapping {
    pub dataset_version: u64,
    pub fragment_mappings: Vec<FragmentMapping>,
    pub row_id_map_path: String,
}

impl From<&VersionFragmentMapping> for pb::remap_index_details::VersionFragmentMapping {
    fn from(mapping: &VersionFragmentMapping) -> Self {
        Self {
            dataset_version: mapping.dataset_version,
            fragment_mappings: mapping.fragment_mappings.iter().map(|m| m.into()).collect(),
            row_id_map_path: mapping.row_id_map_path.clone(),
        }
    }
}

impl TryFrom<pb::remap_index_details::VersionFragmentMapping> for VersionFragmentMapping {
    type Error = Error;

    fn try_from(mapping: pb::remap_index_details::VersionFragmentMapping) -> Result<Self> {
        Ok(Self {
            dataset_version: mapping.dataset_version,
            fragment_mappings: mapping
                .fragment_mappings
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?,
            row_id_map_path: mapping.row_id_map_path,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct RemapIndexDetails {
    pub version_mappings: Vec<VersionFragmentMapping>,
}

impl RemapIndexDetails {
    pub fn remove_version_mapping(&self, version: u64) -> Result<Self> {
        let mut new_details = self.clone();

        if !new_details
            .version_mappings
            .iter()
            .any(|mapping| mapping.dataset_version == version)
        {
            return Err(Error::Internal {
                message: format!("Version mapping {} does not exist", version),
                location: location!(),
            });
        }

        new_details
            .version_mappings
            .retain(|mapping| mapping.dataset_version != version);
        Ok(new_details)
    }

    pub fn add_version_mapping(&self, mapping: VersionFragmentMapping) -> Result<Self> {
        let mut new_details = self.clone();

        if new_details
            .version_mappings
            .iter()
            .any(|m| m.dataset_version == mapping.dataset_version)
        {
            return Err(Error::Internal {
                message: format!("Version mapping {} already exists", mapping.dataset_version),
                location: location!(),
            });
        }

        new_details.version_mappings.push(mapping);
        Ok(new_details)
    }
}

impl From<&RemapIndexDetails> for pb::RemapIndexDetails {
    fn from(details: &RemapIndexDetails) -> Self {
        Self {
            version_mappings: details.version_mappings.iter().map(|m| m.into()).collect(),
        }
    }
}

impl TryFrom<pb::RemapIndexDetails> for RemapIndexDetails {
    type Error = Error;

    fn try_from(details: pb::RemapIndexDetails) -> Result<Self> {
        Ok(Self {
            version_mappings: details
                .version_mappings
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// A system index that stores row ID mappings from old to new row IDs for different dataset versions
#[derive(Debug, Clone)]
pub struct RemapIndex {
    // a mapping to check if an unindexed new fragment exists in the index and its row count match
    pub new_fragment_row_counts: HashMap<u64, u64>,

    // use this mapping to find the fragment details of an old fragment to fulfill a query
    pub old_fragments: HashMap<u64, Fragment>,

    // when performing remapping, use this to get the path of the row_id_map file
    pub row_id_map_paths: HashMap<u64, String>,

    // maps new fragment IDs to a list of old fragment IDs for efficient lookup
    pub fragment_map: HashMap<u64, Vec<u64>>,

    pub details: RemapIndexDetails,

    store: Arc<dyn IndexStore>,
}

impl DeepSizeOf for RemapIndex {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.new_fragment_row_counts.deep_size_of_children(context)
            + self.old_fragments.deep_size_of_children(context)
            + self.row_id_map_paths.deep_size_of_children(context)
            + self.fragment_map.deep_size_of_children(context)
            + self.store.deep_size_of_children(context)
    }
}

impl RemapIndex {
    pub fn new(
        new_fragment_row_counts: HashMap<u64, u64>,
        old_fragments: HashMap<u64, Fragment>,
        row_id_map_paths: HashMap<u64, String>,
        fragment_map: HashMap<u64, Vec<u64>>,
        details: RemapIndexDetails,
        store: Arc<dyn IndexStore>,
    ) -> Self {
        Self {
            new_fragment_row_counts,
            old_fragments,
            row_id_map_paths,
            fragment_map,
            details,
            store,
        }
    }

    pub fn try_from_serialized(
        details: &RemapIndexDetails,
        store: Arc<dyn IndexStore>,
    ) -> Result<Self> {
        let mut new_fragment_row_counts = HashMap::new();
        let mut old_fragments = HashMap::new();
        let mut row_id_map_paths = HashMap::new();
        let mut fragment_map = HashMap::new();

        details.version_mappings.iter().for_each(|version_mapping| {
            let version = version_mapping.dataset_version;
            row_id_map_paths.insert(version, version_mapping.row_id_map_path.clone());

            version_mapping
                .fragment_mappings
                .iter()
                .for_each(|fragment_mapping| {
                    let mut old_fragment_ids = Vec::new();

                    fragment_mapping
                        .old_fragments
                        .iter()
                        .for_each(|old_fragment| {
                            old_fragments.insert(
                                old_fragment.id,
                                Fragment::try_from(old_fragment.clone()).unwrap(),
                            );
                            old_fragment_ids.push(old_fragment.id);
                        });

                    fragment_mapping
                        .new_fragment_signatures
                        .iter()
                        .for_each(|new_sig| {
                            new_fragment_row_counts.insert(new_sig.fragment_id, new_sig.num_rows);
                            fragment_map.insert(new_sig.fragment_id, old_fragment_ids.clone());
                        });
                });
        });

        Ok(Self::new(
            new_fragment_row_counts,
            old_fragments,
            row_id_map_paths,
            fragment_map,
            details.clone(),
            store,
        ))
    }

    pub async fn load_combined_row_id_map(&self) -> Result<HashMap<u64, Option<u64>>> {
        let mut combined_row_id_map: HashMap<u64, Option<u64>> = HashMap::new();
        for row_id_map_path in self.row_id_map_paths.values() {
            let row_id_map = self.load_row_id_map(row_id_map_path).await?;
            combined_row_id_map.extend(row_id_map);
        }

        Ok(combined_row_id_map)
    }

    pub fn get_new_row_id_map_file_path(version: u64) -> String {
        format!("{}_row_id_map_{}.lance", version, Uuid::new_v4())
    }

    pub async fn load_row_id_map(&self, path: &str) -> Result<HashMap<u64, Option<u64>>> {
        let reader = self.store.open_index_file(path).await?;

        let batch = reader.read_range(0..reader.num_rows(), None).await?;
        let old_row_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let new_row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut mapping = HashMap::with_capacity(old_row_ids.len());
        for i in 0..old_row_ids.len() {
            let old_row_id = old_row_ids.value(i);
            let new_row_id = if new_row_ids.is_null(i) {
                None
            } else {
                Some(new_row_ids.value(i))
            };
            mapping.insert(old_row_id, new_row_id);
        }

        Ok(mapping)
    }

    /// Recalculate the fragment bitmap of an index after it is being remapped
    pub fn remap_index_fragment_bitmap(&self, old: &RoaringBitmap) -> Result<RoaringBitmap> {
        let mut new_bitmap = old.clone();

        for version_mapping in self.details.version_mappings.iter() {
            for frag_mapping in version_mapping.fragment_mappings.iter() {
                let any_in_index = frag_mapping
                    .old_fragments
                    .iter()
                    .any(|frag| old.contains(frag.id as u32));
                let all_in_index = frag_mapping
                    .old_fragments
                    .iter()
                    .all(|frag| old.contains(frag.id as u32));

                if any_in_index {
                    if all_in_index {
                        for frag_id in frag_mapping.old_fragments.iter().map(|frag| frag.id as u32)
                        {
                            new_bitmap.remove(frag_id);
                        }
                        new_bitmap.extend(
                            frag_mapping
                                .new_fragment_signatures
                                .iter()
                                .map(|frag| frag.fragment_id as u32),
                        );
                    } else {
                        return Err(Error::invalid_input("The compaction plan included a rewrite group that was a split of indexed and non-indexed data", location!()));
                    }
                }
            }
        }

        Ok(new_bitmap)
    }

    /// Given a bitmap of covered fragments (e.g., from index), check if in the given fragments,
    /// there are fragments that can be remapped.
    ///
    /// Returns a tuple.
    /// The first item is the set of fragment IDs in the given fragments that is remappable.
    /// The second item is a list of fragments that can be used as replacements to be added
    /// to the list of indexed fragments.
    pub fn check_remappable_fragments(
        &self,
        covered_frags: &RoaringBitmap,
        fragments: &Vec<Fragment>,
    ) -> Result<(HashSet<u64>, Vec<Fragment>)> {
        let id_to_frags = fragments
            .iter()
            .map(|frag| (frag.id, frag))
            .collect::<HashMap<u64, &Fragment>>();

        let mut remappable = HashSet::new();
        let mut remapped = Vec::with_capacity(fragments.len());

        for version_mapping in self.details.version_mappings.iter() {
            for frag_mapping in version_mapping.fragment_mappings.iter() {
                let new_frag_id_to_num_rows = frag_mapping
                    .new_fragment_signatures
                    .iter()
                    .map(|sig| (sig.fragment_id, sig.num_rows))
                    .collect::<HashMap<u64, u64>>();

                let any_unindexed_in_fragments =
                    new_frag_id_to_num_rows.iter().any(|(id, num_rows)| {
                        id_to_frags.contains_key(id)
                            && id_to_frags[id].id == *num_rows
                            && !covered_frags.contains(*id as u32)
                    });
                let all_unindexed_in_fragments =
                    new_frag_id_to_num_rows.iter().all(|(id, num_rows)| {
                        id_to_frags.contains_key(id)
                            && id_to_frags[id].id == *num_rows
                            && !covered_frags.contains(*id as u32)
                    });

                if any_unindexed_in_fragments {
                    if all_unindexed_in_fragments {
                        remapped.extend(frag_mapping.old_fragments.clone());
                        remappable.extend(new_frag_id_to_num_rows.keys());
                    } else {
                        // Unlike the remap_index_fragment_bitmap case, this is possible if
                        // the user only would like to scan a subset of fragments,
                        // and it happens to not fully match all new fragments.
                        // In that case, we cannot replace them with old indexed fragments
                        // because it might result in extra data being returned.
                    }
                }
            }
        }

        Ok((remappable, remapped))
    }

    /// Attempts to clean up any unreferenced row ID mapping files.
    /// File cleanup failures are logged but do not cause the function to fail.
    pub async fn cleanup_unreferenced_row_id_maps(&self) -> Result<()> {
        let referenced_paths: HashSet<String> = self
            .details
            .version_mappings
            .iter()
            .map(|vm| vm.row_id_map_path.clone())
            .collect();

        let paths = self.store.as_ref().list_index_files().await?;
        for path in paths {
            if path.contains("_row_id_map_") && !referenced_paths.contains(&path) {
                if let Err(e) = self.store.as_ref().delete_index_file(&path).await {
                    log::warn!(
                        "Failed to delete unreferenced row ID mapping file {}: {}",
                        path,
                        e
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct RemapStatistics {
    // TODO: add more info in stats
    num_versions: usize,
}

#[async_trait]
impl Index for RemapIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn as_vector_index(self: Arc<Self>) -> Result<Arc<dyn crate::vector::VectorIndex>> {
        Err(Error::NotSupported {
            source: "RemapIndex is not a vector index".into(),
            location: location!(),
        })
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let stats = RemapStatistics {
            num_versions: self.row_id_map_paths.len(),
        };
        serde_json::to_value(stats).map_err(|e| Error::Internal {
            message: format!("failed to serialize remap index statistics: {}", e),
            location: location!(),
        })
    }

    async fn prewarm(&self) -> Result<()> {
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::Remap
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

pub async fn save_row_id_map(
    index_store: Arc<dyn IndexStore>,
    version: &u64,
    row_id_map: &HashMap<u64, Option<u64>>,
) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("old_row_id", DataType::UInt64, false),
        Field::new("new_row_id", DataType::UInt64, true),
    ]));

    let mut old_row_ids = Vec::with_capacity(row_id_map.len());
    let mut new_row_ids = Vec::with_capacity(row_id_map.len());

    for (&old_row_id, &new_row_id) in row_id_map {
        old_row_ids.push(old_row_id);
        new_row_ids.push(new_row_id);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(old_row_ids)),
            Arc::new(UInt64Array::from(new_row_ids)),
        ],
    )?;

    let file_path = new_row_id_map_file_path(*version);
    let mut writer = index_store.new_index_file(&file_path, schema).await?;
    writer.write_record_batch(batch).await?;
    writer.finish().await?;
    Ok(file_path)
}

fn new_row_id_map_file_path(version: u64) -> String {
    format!("{}_row_id_map_{}.lance", version, Uuid::new_v4())
}
