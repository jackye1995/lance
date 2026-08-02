// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! CommitBuilder-shaped research surface for the Bε manifest.
//!
//! Mirrors the shape production wiring needs: layout selection from table
//! config, an operation intent, and one `execute` entry point that translates
//! the intent into `FragmentAction`s and commits through the transaction-aware
//! Bε path. This is NOT `lance::dataset::CommitBuilder` integration. It proves
//! the boundary in `lance-table` so the production builder can adopt it later.

use std::collections::HashMap;

use lance_core::{Error, Result};

use crate::betree::dataset::{BeTreeDataset, MANIFEST_LAYOUT_BETREE, MANIFEST_LAYOUT_KEY};
use crate::betree::tree::CommitStats;
use crate::format::{DataFile, DeletionFile, Fragment};

/// Research config value selecting today's flat manifest layout.
pub const MANIFEST_LAYOUT_FLAT: &str = "flat";

/// The manifest layout a table's config selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestLayout {
    /// Today's single-protobuf manifest. The default when the key is unset.
    Flat,
    /// The Bε-tree research layout, selected by `lance.manifest.layout=betree`.
    BeTree,
}

impl ManifestLayout {
    /// Select a layout from table config, treating an unset key as flat.
    ///
    /// Rejects unknown values instead of falling back, so a typo cannot
    /// silently commit through the wrong layout.
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        match config.get(MANIFEST_LAYOUT_KEY).map(String::as_str) {
            None | Some(MANIFEST_LAYOUT_FLAT) => Ok(Self::Flat),
            Some(MANIFEST_LAYOUT_BETREE) => Ok(Self::BeTree),
            Some(other) => Err(Error::invalid_input(format!(
                "unsupported {MANIFEST_LAYOUT_KEY} value: layout={other:?}, \
                 expected={MANIFEST_LAYOUT_FLAT:?} or {MANIFEST_LAYOUT_BETREE:?}"
            ))),
        }
    }
}

/// A commit intent, the research analog of `lance` transaction operations.
#[derive(Debug, Clone)]
pub enum Operation {
    Append {
        fragments: Vec<Fragment>,
    },
    AddColumns {
        files: Vec<(u64, DataFile)>,
    },
    ReplaceDataFiles {
        replacements: Vec<(u64, String, DataFile)>,
    },
    DropDataFiles {
        files: Vec<(u64, String)>,
    },
    SetDeletionFiles {
        deletion_files: Vec<(u64, DeletionFile)>,
    },
    Delete {
        fragment_ids: Vec<u64>,
    },
    Overwrite {
        fragments: Vec<Fragment>,
    },
}

/// Commits one [`Operation`] against a Bε dataset through the
/// transaction-aware path, mirroring `CommitBuilder::execute(transaction)`.
pub struct CommitBuilder<'a> {
    dataset: &'a mut BeTreeDataset,
}

impl<'a> CommitBuilder<'a> {
    pub fn new(dataset: &'a mut BeTreeDataset) -> Self {
        Self { dataset }
    }

    /// Translate the intent into fragment actions and commit them. Every
    /// commit publishes a transaction record beside the new root.
    pub async fn execute(self, operation: Operation) -> Result<CommitStats> {
        match operation {
            Operation::Append { fragments } => self.dataset.append(&fragments).await,
            Operation::AddColumns { files } => self.dataset.add_columns(&files).await,
            Operation::ReplaceDataFiles { replacements } => {
                self.dataset.replace_data_files(&replacements).await
            }
            Operation::DropDataFiles { files } => self.dataset.drop_data_files(&files).await,
            Operation::SetDeletionFiles { deletion_files } => {
                self.dataset.set_deletion_files(&deletion_files).await
            }
            Operation::Delete { fragment_ids } => self.dataset.delete(&fragment_ids).await,
            Operation::Overwrite { fragments } => self.dataset.overwrite(&fragments).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::betree::node::BeTreeConfig;
    use crate::betree::support::{make_backfill_data_file, make_fragment};
    use lance_core::cache::LanceCache;
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::object_store::ObjectStore;
    use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
    use std::sync::Arc;

    #[test]
    fn layout_selection_defaults_flat_and_rejects_unknown_values() {
        assert_eq!(
            ManifestLayout::from_config(&HashMap::new()).unwrap(),
            ManifestLayout::Flat
        );
        let flat = HashMap::from([(MANIFEST_LAYOUT_KEY.to_string(), "flat".to_string())]);
        assert_eq!(
            ManifestLayout::from_config(&flat).unwrap(),
            ManifestLayout::Flat
        );
        let betree = HashMap::from([(MANIFEST_LAYOUT_KEY.to_string(), "betree".to_string())]);
        assert_eq!(
            ManifestLayout::from_config(&betree).unwrap(),
            ManifestLayout::BeTree
        );
        let unknown = HashMap::from([(MANIFEST_LAYOUT_KEY.to_string(), "tiered".to_string())]);
        let error = ManifestLayout::from_config(&unknown).unwrap_err();
        assert!(error.to_string().contains("layout=\"tiered\""));
    }

    #[tokio::test]
    async fn executes_intents_through_the_transacted_path() {
        let tempdir = TempObjDir::default();
        let base = tempdir.clone().join("betree");
        let object_store = Arc::new(ObjectStore::local());
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        let cache = Arc::new(LanceCache::with_capacity(64 * 1024 * 1024));
        let original: Vec<Fragment> = (0..12).map(make_fragment).collect();
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            original.clone(),
            Vec::new(),
        )
        .await
        .unwrap();

        let stats = CommitBuilder::new(&mut dataset)
            .execute(Operation::Append {
                fragments: vec![make_fragment(12)],
            })
            .await
            .unwrap();
        assert!(stats.transaction_bytes > 0);
        CommitBuilder::new(&mut dataset)
            .execute(Operation::AddColumns {
                files: vec![(3, make_backfill_data_file(3, 0))],
            })
            .await
            .unwrap();
        CommitBuilder::new(&mut dataset)
            .execute(Operation::ReplaceDataFiles {
                replacements: vec![(
                    1,
                    original[1].files[0].path.clone(),
                    make_backfill_data_file(1, 7),
                )],
            })
            .await
            .unwrap();
        CommitBuilder::new(&mut dataset)
            .execute(Operation::Delete {
                fragment_ids: vec![4],
            })
            .await
            .unwrap();

        assert_eq!(dataset.count_fragments(), 12);
        for (version, operation) in [
            (2, "append"),
            (3, "add_columns"),
            (4, "replace_data_files"),
            (5, "delete"),
        ] {
            assert_eq!(
                dataset
                    .tree()
                    .read_transaction(version)
                    .await
                    .unwrap()
                    .operation,
                operation
            );
        }
    }
}
