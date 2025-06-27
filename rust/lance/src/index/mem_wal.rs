// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::dataset::transaction::{Operation, Transaction};
use crate::index::DatasetIndexInternalExt;
use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::mem_wal::{MemWal, MemWalId, MemWalIndex, MemWalIndexDetails, MEM_WAL_INDEX_NAME};
use lance_index::metrics::NoOpMetricsCollector;
use lance_table::format::{pb, Index};
use prost::Message;
use snafu::location;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

fn load_mem_wal_index_details(index: Index) -> Result<MemWalIndexDetails> {
    if let Some(details_any) = index.index_details.as_ref() {
        if !details_any.type_url.ends_with("MemWalIndexDetails") {
            return Err(Error::Index {
                message: format!(
                    "Index details is not for the MemWAL index, but {}",
                    details_any.type_url
                ),
                location: location!(),
            });
        }

        Ok(MemWalIndexDetails::try_from(
            details_any.to_msg::<pb::MemWalIndexDetails>()?,
        )?)
    } else {
        Err(Error::Index {
            message: "Index details not found for the MemWAL index".into(),
            location: location!(),
        })
    }
}

pub(crate) fn open_mem_wal_index(index: Index) -> Result<Arc<MemWalIndex>> {
    Ok(Arc::new(MemWalIndex::new(load_mem_wal_index_details(
        index,
    )?)))
}

/// Advance the generation of the MemWAL for the given region.
/// If the MemWAL does not exist, create one with generation 0.
/// If the MemWAL exists, seal the one with the latest generation,
/// and open one with the same name and the next generation.
/// If the MemWALIndex structure does not exist, create it along the way.
pub async fn advance_mem_wal_generation(
    dataset: &mut Dataset,
    region: &str,
    new_mem_table_location: &str,
    new_wal_location: &str,
) -> Result<()> {
    let transaction = if let Some(mem_wal_index) =
        dataset.open_mem_wal_index(&NoOpMetricsCollector).await?
    {
        let (added_mem_wal, updated_mem_wal, removed_mem_wal) = if let Some(generations) =
            mem_wal_index.mem_wal_map.get(region)
        {
            // MemWALs of the same region is ordered increasingly by its generation
            let mut values_iter = generations.values().rev();
            if let Some(latest_mem_wal) = values_iter.next() {
                if latest_mem_wal.wal_location == new_wal_location {
                    return Err(Error::invalid_input(
                        format!(
                            "Must use a different WAL location from current: {}",
                            latest_mem_wal.wal_location
                        ),
                        location!(),
                    ));
                }
                if latest_mem_wal.mem_table_location == new_mem_table_location {
                    return Err(Error::invalid_input(
                        format!(
                            "Must use a different MemTable location from current: {}",
                            latest_mem_wal.mem_table_location
                        ),
                        location!(),
                    ));
                }

                let (updated_mem_wal, removed_mem_wal) = if !latest_mem_wal.sealed {
                    let mut updated_mem_wal = latest_mem_wal.clone();
                    updated_mem_wal.sealed = true;
                    (Some(updated_mem_wal), Some(latest_mem_wal.clone()))
                } else {
                    (None, None)
                };

                let added_mem_wal = MemWal::new_empty(
                    MemWalId::new(region, latest_mem_wal.id.generation + 1),
                    new_mem_table_location,
                    new_wal_location,
                );

                Ok((added_mem_wal, updated_mem_wal, removed_mem_wal))
            } else {
                Err(Error::Internal {
                    message: format!("Encountered MemWAL index mapping that has a region with an empty list of generations: {}", region),
                    location: location!(),
                })
            }
        } else {
            Ok((
                MemWal::new_empty(
                    MemWalId::new(region, 0),
                    new_mem_table_location,
                    new_wal_location,
                ),
                None,
                None,
            ))
        }?;

        Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added: vec![added_mem_wal],
                updated: updated_mem_wal.into_iter().collect(),
                removed: removed_mem_wal.into_iter().collect(),
            },
            None,
            None,
        )
    } else {
        // this is the first time the MemWAL index is created
        Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added: vec![MemWal::new_empty(
                    MemWalId::new(region, 0),
                    new_mem_table_location,
                    new_wal_location,
                )],
                updated: vec![],
                removed: vec![],
            },
            None,
            None,
        )
    };

    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await
}

/// Add a new entry to the MemWAL
pub async fn append_mem_wal_entry(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
    entry_id: u64,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> Result<MemWal> {
        // Can only append to unsealed and unflushed MemWALs
        mem_wal.check_flushed(false)?;
        mem_wal.check_sealed(false)?;

        let mut updated_mem_wal = mem_wal.clone();
        let wal_entries = updated_mem_wal.wal_entries();
        updated_mem_wal.wal_entries =
            pb::U64Segment::from(wal_entries.with_new_high(entry_id)?).encode_to_vec();
        Ok(updated_mem_wal)
    };

    mutate_mem_wal(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as sealed.
/// Typically, it is recommended to call [`advance_mem_wal_generation`] instead.
/// But this will always keep the table in a state with an unsealed MemTable.
/// Calling this function will only seal the current latest MemWAL without opening the next one.
pub async fn mark_mem_wal_as_sealed(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> Result<MemWal> {
        // Can only seal unsealed or flushed MemWALs
        mem_wal.check_flushed(false)?;
        mem_wal.check_sealed(false)?;

        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.sealed = true;
        Ok(updated_mem_wal)
    };

    mutate_mem_wal(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as flushed
pub async fn mark_mem_wal_as_flushed(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> Result<MemWal> {
        // Can only flush sealed but unflushed MemWALs
        mem_wal.check_sealed(true)?;
        mem_wal.check_flushed(false)?;

        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.flushed = true;
        Ok(updated_mem_wal)
    };

    mutate_mem_wal(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as flushed, in the list of indices in the dataset.
/// This is intended to be used as a part of the Update transaction after resolving all conflicts.
pub(crate) fn update_mem_wal_index_from_indices_list(
    dataset_version: u64,
    indices: &mut Vec<Index>,
    added: &[MemWal],
    updated: &[MemWal],
    removed: &[MemWal],
) -> Result<()> {
    let new_meta = if let Some(pos) = indices
        .iter()
        .position(|idx| idx.name == MEM_WAL_INDEX_NAME)
    {
        let current_meta = indices.remove(pos);
        let mut details = load_mem_wal_index_details(current_meta)?;
        let removed_set = removed
            .iter()
            .map(|rm| rm.id.clone())
            .collect::<HashSet<_>>();
        details
            .mem_wal_list
            .retain(|m| !removed_set.contains(&m.id));
        details.mem_wal_list.extend(added.iter().cloned());
        details.mem_wal_list.extend(updated.iter().cloned());
        new_mem_wal_index_meta(dataset_version, details.mem_wal_list)?
    } else {
        // This should only happen with new index creation when opening the first MemWAL
        if !updated.is_empty() || !removed.is_empty() {
            return Err(Error::invalid_input(
                "Cannot update MemWAL state without a MemWAL index",
                location!(),
            ));
        }
        new_mem_wal_index_meta(dataset_version, added.to_vec())?
    };

    indices.push(new_meta);
    Ok(())
}

/// Start the replay process of a MemWAL.
/// This updates the MemTable location of the specific MemWAL.
/// Any other concurrent writer will see this change of location and abort any new writes.
pub async fn start_replay_mem_wal(
    dataset: &mut Dataset,
    region: &str,
    generation: u64,
    new_mem_table_location: &str,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> Result<MemWal> {
        // start replay should only commit if it's against an unflushed MemWAL.
        mem_wal.check_flushed(false)?;
        mem_wal.check_sealed(false)?;

        if new_mem_table_location == mem_wal.mem_table_location {
            return Err(Error::invalid_input(
                format!(
                    "Must use a different MemTable location from current: {}",
                    mem_wal.mem_table_location
                ),
                location!(),
            ));
        }

        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.mem_table_location = new_mem_table_location.to_owned();
        Ok(updated_mem_wal)
    };

    mutate_mem_wal(dataset, region, generation, mutate).await
}

/// Trim all the MemWALs that are already flushed.
pub async fn trim_mem_wal_index(dataset: &mut Dataset) -> Result<()> {
    if let Some(mem_wal_index) = dataset.open_mem_wal_index(&NoOpMetricsCollector).await? {
        let mut removed = Vec::new();
        for (_, generations) in mem_wal_index.mem_wal_map.iter() {
            for (_, mem_wal) in generations.iter() {
                if mem_wal.flushed {
                    removed.push(mem_wal.clone());
                }
            }
        }

        let transaction = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added: vec![],
                updated: vec![],
                removed,
            },
            None,
            None,
        );

        dataset
            .apply_commit(transaction, &Default::default(), &Default::default())
            .await
    } else {
        Err(Error::NotSupported {
            source: "MemWAL is not enabled".into(),
            location: location!(),
        })
    }
}

async fn mutate_mem_wal<F>(
    dataset: &mut Dataset,
    region: &str,
    generation: u64,
    mutate: F,
) -> Result<()>
where
    F: Fn(&MemWal) -> Result<MemWal>,
{
    if let Some(mem_wal_index) = dataset.open_mem_wal_index(&NoOpMetricsCollector).await? {
        if let Some(generations) = mem_wal_index.mem_wal_map.get(region) {
            if let Some(mem_wal) = generations.get(&generation) {
                let updated_mem_wal = mutate(mem_wal)?;

                let transaction = Transaction::new(
                    dataset.manifest.version,
                    Operation::UpdateMemWalState {
                        added: vec![],
                        updated: vec![updated_mem_wal],
                        removed: vec![mem_wal.clone()],
                    },
                    None,
                    None,
                );

                dataset
                    .apply_commit(transaction, &Default::default(), &Default::default())
                    .await
            } else {
                Err(Error::invalid_input(
                    format!(
                        "Cannot find MemWAL generation {} for region {}",
                        generation, region
                    ),
                    location!(),
                ))
            }
        } else {
            Err(Error::invalid_input(
                format!("Cannot find MemWAL for region {}", region),
                location!(),
            ))
        }
    } else {
        Err(Error::NotSupported {
            source: "MemWAL is not enabled".into(),
            location: location!(),
        })
    }
}

pub(crate) fn new_mem_wal_index_meta(
    dataset_version: u64,
    new_mem_wal_list: Vec<MemWal>,
) -> Result<Index> {
    Ok(Index {
        uuid: Uuid::new_v4(),
        name: MEM_WAL_INDEX_NAME.to_string(),
        fields: vec![],
        dataset_version,
        fragment_bitmap: None,
        index_details: Some(prost_types::Any::from_msg(&pb::MemWalIndexDetails::from(
            &MemWalIndexDetails {
                mem_wal_list: new_mem_wal_list,
            },
        ))?),
        index_version: 0,
        created_at: Some(chrono::Utc::now()),
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::types::{Float32Type, Int32Type};
    use lance_datafusion::datagen::DatafusionDatagenExt;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use lance_datagen::{BatchCount, Dimension, RowCount};
    use lance_index::DatasetIndexExt;
    use lance_index::mem_wal::{MemWalId, MEM_WAL_INDEX_NAME};

    use super::*;

    #[tokio::test]
    async fn test_advance_mem_wal_generation() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Initially, there should be no MemWAL index
        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME));

        // First call to advance_mem_wal_generation should create the MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Verify the MemWAL index was created
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should be created");

        // Load and verify the MemWAL index details
        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        assert_eq!(mem_wal_details.mem_wal_list.len(), 1);

        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert_eq!(mem_wal.id.region, "GLOBAL");
        assert_eq!(mem_wal.id.generation, 0);
        assert_eq!(mem_wal.mem_table_location, "mem_table_location_0");
        assert_eq!(mem_wal.wal_location, "wal_location_0");
        assert!(!mem_wal.sealed);
        assert!(!mem_wal.flushed);

        // Second call to advance_mem_wal_generation should seal generation 0 and create generation 1
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_1",
            "wal_location_1",
        )
        .await
        .unwrap();

        // Verify the MemWAL index now has two generations
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        assert_eq!(mem_wal_details.mem_wal_list.len(), 2);

        // Find generation 0 (should be sealed) and generation 1 (should be unsealed)
        let gen_0 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 0)
            .expect("Generation 0 should exist");
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");

        // Verify generation 0 is sealed
        assert_eq!(gen_0.id.region, "GLOBAL");
        assert_eq!(gen_0.id.generation, 0);
        assert_eq!(gen_0.mem_table_location, "mem_table_location_0");
        assert_eq!(gen_0.wal_location, "wal_location_0");
        assert!(gen_0.sealed);
        assert!(!gen_0.flushed);

        // Verify generation 1 is unsealed
        assert_eq!(gen_1.id.region, "GLOBAL");
        assert_eq!(gen_1.id.generation, 1);
        assert_eq!(gen_1.mem_table_location, "mem_table_location_1");
        assert_eq!(gen_1.wal_location, "wal_location_1");
        assert!(!gen_1.sealed);
        assert!(!gen_1.flushed);

        // Test that using the same MemTable location should fail
        let result = advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_1", // Same as current generation
            "wal_location_2",       // Different WAL location
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail when using same MemTable location as current generation"
        );

        // Test that using the same WAL location should fail
        let result = advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_2", // Different MemTable location
            "wal_location_1",       // Same as current generation
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail when using same WAL location as current generation"
        );
    }

    #[tokio::test]
    async fn test_append_new_entry_to_mem_wal() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Test failure case: MemWAL is not enabled
        let result = append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 123).await;
        assert!(result.is_err(), "Should fail when MemWAL is not enabled");

        // Create MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Test failure case: region doesn't exist
        let result = append_mem_wal_entry(&mut dataset, "NONEXISTENT", 0, 123).await;
        assert!(result.is_err(), "Should fail when region doesn't exist");

        // Test failure case: generation doesn't exist
        let result = append_mem_wal_entry(&mut dataset, "GLOBAL", 999, 123).await;
        assert!(result.is_err(), "Should fail when generation doesn't exist");

        // Test success case: append entry to generation 0
        append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 123)
            .await
            .unwrap();

        // Verify the entry was added
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];

        // Check that the WAL entries contain the entry_id
        let wal_entries = mem_wal.wal_entries();
        assert!(
            wal_entries.contains(123),
            "WAL entries should contain entry_id 123"
        );

        // Test appending multiple entries
        append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 456)
            .await
            .unwrap();
        append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 789)
            .await
            .unwrap();

        // Verify all entries were added
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];

        let wal_entries = mem_wal.wal_entries();
        assert!(
            wal_entries.contains(123),
            "WAL entries should contain entry_id 123"
        );
        assert!(
            wal_entries.contains(456),
            "WAL entries should contain entry_id 456"
        );
        assert!(
            wal_entries.contains(789),
            "WAL entries should contain entry_id 789"
        );

        // Test failure case: cannot append to sealed MemWAL
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();
        let result = append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 999).await;
        assert!(
            result.is_err(),
            "Should fail when trying to append to sealed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is sealed: true, but expected false"), 
                "Error message should indicate the MemWAL is sealed, got: {}", error);

        // Test failure case: cannot append to flushed MemWAL
        mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();
        let result = append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 999).await;
        assert!(
            result.is_err(),
            "Should fail when trying to append to flushed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is flushed: true, but expected false"), 
                "Error message should indicate the MemWAL is flushed, got: {}", error);
    }

    #[tokio::test]
    async fn test_seal_mem_wal() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Test failure case: MemWAL is not enabled
        let result = mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0).await;
        assert!(result.is_err(), "Should fail when MemWAL is not enabled");

        // Create MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Test failure case: region doesn't exist
        let result = mark_mem_wal_as_sealed(&mut dataset, "NONEXISTENT", 0).await;
        assert!(result.is_err(), "Should fail when region doesn't exist");

        // Test failure case: generation doesn't exist
        let result = mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 999).await;
        assert!(result.is_err(), "Should fail when generation doesn't exist");

        // Verify generation 0 is initially unsealed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(!mem_wal.sealed, "Generation 0 should initially be unsealed");

        // Test success case: seal generation 0
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        // Verify generation 0 is now sealed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(mem_wal.sealed, "Generation 0 should now be sealed");

        // Create a new generation and test sealing it
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_1",
            "wal_location_1",
        )
        .await
        .unwrap();

        // Verify generation 1 is unsealed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");

        assert!(!gen_1.sealed, "Generation 1 should be unsealed");

        // Seal generation 1
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 1)
            .await
            .unwrap();

        // Verify it's sealed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");

        assert!(gen_1.sealed, "Generation 1 should be sealed");

        // Test that sealing an already sealed MemWAL should fail
        let result = mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 1).await;
        assert!(
            result.is_err(),
            "Should fail when trying to seal an already sealed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 1 } is sealed: true, but expected false"), 
                "Error message should indicate the MemWAL is already sealed, got: {}", error);

        // Test that sealing an already flushed MemWAL should fail
        mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();
        let result = mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0).await;
        assert!(
            result.is_err(),
            "Should fail when trying to seal an already flushed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is flushed: true, but expected false"),
                "Error message should indicate the MemWAL is already flushed, got: {}", error);
    }

    #[tokio::test]
    async fn test_flush_mem_wal() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Test failure case: MemWAL is not enabled
        let result = mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0).await;
        assert!(result.is_err(), "Should fail when MemWAL is not enabled");

        // Create MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Test failure case: region doesn't exist
        let result = mark_mem_wal_as_flushed(&mut dataset, "NONEXISTENT", 0).await;
        assert!(result.is_err(), "Should fail when region doesn't exist");

        // Test failure case: generation doesn't exist
        let result = mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 999).await;
        assert!(result.is_err(), "Should fail when generation doesn't exist");

        // Verify generation 0 is initially unflushed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(
            !mem_wal.flushed,
            "Generation 0 should initially be unflushed"
        );

        // Test failure case: cannot flush unsealed MemWAL
        let result = mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0).await;
        assert!(
            result.is_err(),
            "Should fail when trying to flush unsealed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is sealed: false, but expected true"), 
                "Error message should indicate the MemWAL is not sealed, got: {}", error);

        // Seal generation 0 first
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        // Test success case: mark sealed generation 0 as flushed
        mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        // Verify generation 0 is now flushed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(mem_wal.flushed, "Generation 0 should now be flushed");

        // Test failure case: cannot flush already flushed MemWAL
        let result = mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0).await;
        assert!(
            result.is_err(),
            "Should fail when trying to flush already flushed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is flushed: true, but expected false"), 
                "Error message should indicate the MemWAL is already flushed, got: {}", error);
    }

    #[tokio::test]
    async fn test_start_replay_mem_wal() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Test failure case: MemWAL is not enabled
        let result =
            start_replay_mem_wal(&mut dataset, "GLOBAL", 0, "new_mem_table_location").await;
        assert!(result.is_err(), "Should fail when MemWAL is not enabled");

        // Create MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Test failure case: region doesn't exist
        let result =
            start_replay_mem_wal(&mut dataset, "NONEXISTENT", 0, "new_mem_table_location").await;
        assert!(result.is_err(), "Should fail when region doesn't exist");

        // Test failure case: generation doesn't exist
        let result =
            start_replay_mem_wal(&mut dataset, "GLOBAL", 999, "new_mem_table_location").await;
        assert!(result.is_err(), "Should fail when generation doesn't exist");

        // Test failure case: cannot replay with same MemTable location
        let result = start_replay_mem_wal(&mut dataset, "GLOBAL", 0, "mem_table_location_0").await;
        assert!(
            result.is_err(),
            "Should fail when using same MemTable location"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(
                "Must use a different MemTable location from current: mem_table_location_0"
            ),
            "Error message should indicate the MemTable location must be different, got: {}",
            error
        );

        // Test success case: start replay with different MemTable location
        start_replay_mem_wal(&mut dataset, "GLOBAL", 0, "new_mem_table_location")
            .await
            .unwrap();

        // Verify the MemTable location was updated
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert_eq!(
            mem_wal.mem_table_location, "new_mem_table_location",
            "MemTable location should be updated"
        );

        // Test failure case: cannot replay sealed MemWAL
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();
        let result =
            start_replay_mem_wal(&mut dataset, "GLOBAL", 0, "another_mem_table_location").await;
        assert!(
            result.is_err(),
            "Should fail when trying to replay sealed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is sealed: true, but expected false"), 
                "Error message should indicate the MemWAL is sealed, got: {}", error);

        // Test failure case: cannot replay flushed MemWAL
        // First create a new generation
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_1",
            "wal_location_1",
        )
        .await
        .unwrap();

        // Now generation 0 is sealed, so we can flush it
        mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        let result =
            start_replay_mem_wal(&mut dataset, "GLOBAL", 0, "another_mem_table_location").await;
        assert!(
            result.is_err(),
            "Should fail when trying to replay flushed MemWAL"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is flushed: true, but expected false"), 
                "Error message should indicate the MemWAL is flushed, got: {}", error);

        // Test success case: can replay generation 1 (which is unsealed and unflushed)
        start_replay_mem_wal(&mut dataset, "GLOBAL", 1, "new_mem_table_location_1")
            .await
            .unwrap();

        // Verify the MemTable location was updated for generation 1
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");

        assert_eq!(
            gen_1.mem_table_location, "new_mem_table_location_1",
            "Generation 1 MemTable location should be updated"
        );
    }

    #[tokio::test]
    async fn test_trim_mem_wal_index() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Test failure case: MemWAL is not enabled
        let result = trim_mem_wal_index(&mut dataset).await;
        assert!(result.is_err(), "Should fail when MemWAL is not enabled");

        // Create MemWAL index and multiple generations
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_1",
            "wal_location_1",
        )
        .await
        .unwrap();

        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_2",
            "wal_location_2",
        )
        .await
        .unwrap();

        // Verify we have 3 generations initially
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        assert_eq!(
            mem_wal_details.mem_wal_list.len(),
            3,
            "Should have 3 generations initially"
        );

        // flush generation 0
        mark_mem_wal_as_flushed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        // Verify the states before trimming
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_0 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 0)
            .expect("Generation 0 should exist");
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");
        let gen_2 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 2)
            .expect("Generation 2 should exist");

        assert!(gen_0.flushed, "Generation 0 should be flushed");
        assert!(
            gen_1.sealed && !gen_1.flushed,
            "Generation 1 should be sealed but not flushed"
        );
        assert!(
            !gen_2.sealed && !gen_2.flushed,
            "Generation 2 should be unsealed and unflushed"
        );

        // Trim the MemWAL index
        trim_mem_wal_index(&mut dataset).await.unwrap();

        // Verify that only flushed MemWALs (generation 0) were removed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        assert_eq!(
            mem_wal_details.mem_wal_list.len(),
            2,
            "Should have 2 generations after trimming"
        );

        // Verify generation 0 was removed
        let gen_0_exists = mem_wal_details
            .mem_wal_list
            .iter()
            .any(|m| m.id.generation == 0);
        assert!(!gen_0_exists, "Generation 0 should be removed");

        // Verify generation 1 and 2 still exist
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should still exist");
        let gen_2 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 2)
            .expect("Generation 2 should still exist");

        assert!(
            gen_1.sealed && !gen_1.flushed,
            "Generation 1 should still be sealed but not flushed"
        );
        assert!(
            !gen_2.sealed && !gen_2.flushed,
            "Generation 2 should still be unsealed and unflushed"
        );

        // Test trimming when no MemWALs are flushed (should do nothing)
        trim_mem_wal_index(&mut dataset).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        assert_eq!(
            mem_wal_details.mem_wal_list.len(),
            2,
            "Should still have 2 generations after second trim"
        );

        // Test with multiple regions
        advance_mem_wal_generation(
            &mut dataset,
            "REGION_A",
            "mem_table_location_a_0",
            "wal_location_a_0",
        )
        .await
        .unwrap();

        // Seal and flush the region A MemWAL
        mark_mem_wal_as_sealed(&mut dataset, "REGION_A", 0)
            .await
            .unwrap();
        mark_mem_wal_as_flushed(&mut dataset, "REGION_A", 0)
            .await
            .unwrap();

        // Trim again - should remove the flushed region A MemWAL
        trim_mem_wal_index(&mut dataset).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();

        // Should only have GLOBAL region MemWALs left
        let global_mem_wals = mem_wal_details
            .mem_wal_list
            .iter()
            .filter(|m| m.id.region == "GLOBAL")
            .count();
        assert_eq!(global_mem_wals, 2, "Should have 2 GLOBAL region MemWALs");

        let region_a_mem_wals = mem_wal_details
            .mem_wal_list
            .iter()
            .filter(|m| m.id.region == "REGION_A")
            .count();
        assert_eq!(
            region_a_mem_wals, 0,
            "Should have 0 REGION_A MemWALs after trimming"
        );
    }

    #[tokio::test]
    async fn test_flush_mem_wal_through_merge_insert() {
        // Create a dataset with some data
        let mut dataset = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Create MemWAL index and generation 0
        advance_mem_wal_generation(
            &mut dataset,
            "GLOBAL",
            "mem_table_location_0",
            "wal_location_0",
        )
        .await
        .unwrap();

        // Add some entries to the MemWAL
        append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 123)
            .await
            .unwrap();
        append_mem_wal_entry(&mut dataset, "GLOBAL", 0, 456)
            .await
            .unwrap();

        // Seal the MemWAL (required before flushing)
        mark_mem_wal_as_sealed(&mut dataset, "GLOBAL", 0)
            .await
            .unwrap();

        // Verify the MemWAL is sealed but not flushed
        let indices = dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(mem_wal.sealed, "MemWAL should be sealed");
        assert!(!mem_wal.flushed, "MemWAL should not be flushed yet");

        // Create new data for merge insert
        let new_data = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step_custom::<Int32Type>(1000, 1))
            .into_df_stream(RowCount::from(100), BatchCount::from(10));

        // Create merge insert job that will flush the MemWAL
        let merge_insert_job = crate::dataset::MergeInsertBuilder::try_new(
            Arc::new(dataset.clone()),
            vec!["i".to_string()],
        )
        .unwrap()
        .when_matched(crate::dataset::WhenMatched::UpdateAll)
        .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
        .mark_mem_wal_as_flushed(MemWalId::new("GLOBAL", 0))
        .await
        .unwrap()
        .try_build()
        .unwrap();

        // Execute the merge insert
        let (updated_dataset, _stats) = merge_insert_job.execute_reader(new_data).await.unwrap();

        // Verify that the MemWAL is now marked as flushed
        let indices = updated_dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let mem_wal = &mem_wal_details.mem_wal_list[0];
        assert!(mem_wal.sealed, "MemWAL should still be sealed");
        assert!(mem_wal.flushed, "MemWAL should now be flushed");

        // Test that trying to mark a non-existent MemWAL as flushed fails
        let mut merge_insert_job = crate::dataset::MergeInsertBuilder::try_new(
            updated_dataset.clone(),
            vec!["i".to_string()],
        )
        .unwrap();
        merge_insert_job
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll);

        let result = merge_insert_job
            .mark_mem_wal_as_flushed(MemWalId::new("GLOBAL", 999))
            .await;
        assert!(
            result.is_err(),
            "Should fail when trying to mark non-existent MemWAL as flushed"
        );

        // Test that trying to mark a MemWAL from non-existent region fails
        let result = merge_insert_job
            .mark_mem_wal_as_flushed(MemWalId::new("NONEXISTENT", 0))
            .await;
        assert!(
            result.is_err(),
            "Should fail when trying to mark MemWAL from non-existent region as flushed"
        );

        // Test that trying to mark an unsealed MemWAL as flushed fails
        // First, create a new generation that is unsealed
        let mut dataset_for_advance = updated_dataset.as_ref().clone();
        advance_mem_wal_generation(
            &mut dataset_for_advance,
            "GLOBAL",
            "mem_table_location_1",
            "wal_location_1",
        )
        .await
        .unwrap();

        // Update our reference to use the new dataset
        let updated_dataset = Arc::new(dataset_for_advance);

        // Verify that generation 1 exists and is unsealed
        let indices = updated_dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");
        assert!(!gen_1.sealed, "Generation 1 should be unsealed");

        let mut merge_insert_job_unsealed = crate::dataset::MergeInsertBuilder::try_new(
            updated_dataset.clone(),
            vec!["i".to_string()],
        )
        .unwrap();
        merge_insert_job_unsealed
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll);

        let result = merge_insert_job_unsealed
            .mark_mem_wal_as_flushed(MemWalId::new("GLOBAL", 1))
            .await;
        assert!(
            result.is_err(),
            "Should fail when trying to mark unsealed MemWAL as flushed"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 1 } is sealed: false, but expected true"), 
                "Error message should indicate the MemWAL is not sealed, got: {}", error);

        // Test that trying to mark an already flushed MemWAL as flushed fails
        let mut merge_insert_job_flushed = crate::dataset::MergeInsertBuilder::try_new(
            updated_dataset.clone(),
            vec!["i".to_string()],
        )
        .unwrap();
        merge_insert_job_flushed
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll);

        let result = merge_insert_job_flushed
            .mark_mem_wal_as_flushed(MemWalId::new("GLOBAL", 0))
            .await;
        assert!(
            result.is_err(),
            "Should fail when trying to mark already flushed MemWAL as flushed"
        );

        // Check the specific error message
        let error = result.unwrap_err();
        assert!(error.to_string().contains("MemWAL MemWalId { region: \"GLOBAL\", generation: 0 } is flushed: true, but expected false"), 
                "Error message should indicate the MemWAL is already flushed, got: {}", error);

        // Test that merge insert with mark_mem_wal_as_flushed works correctly when MemWAL is in proper state
        // Seal generation 1 and then test the merge insert
        let mut dataset_for_seal = updated_dataset.as_ref().clone();
        mark_mem_wal_as_sealed(&mut dataset_for_seal, "GLOBAL", 1)
            .await
            .unwrap();
        let updated_dataset = Arc::new(dataset_for_seal);

        // Verify generation 1 is now sealed but not flushed
        let indices = updated_dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should exist");
        assert!(gen_1.sealed, "Generation 1 should be sealed");
        assert!(!gen_1.flushed, "Generation 1 should not be flushed");

        // Create merge insert that flushes generation 1
        let new_data_valid = lance_datagen::gen()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step_custom::<Int32Type>(4000, 1))
            .into_df_stream(RowCount::from(75), BatchCount::from(5));

        let merge_insert_job_valid = crate::dataset::MergeInsertBuilder::try_new(
            updated_dataset.clone(),
            vec!["i".to_string()],
        )
        .unwrap()
        .when_matched(crate::dataset::WhenMatched::UpdateAll)
        .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
        .mark_mem_wal_as_flushed(MemWalId::new("GLOBAL", 1))
        .await
        .unwrap()
        .try_build()
        .unwrap();

        // Execute the merge insert - this should succeed
        let (final_dataset, _stats) = merge_insert_job_valid
            .execute_reader(new_data_valid)
            .await
            .unwrap();

        // Verify that generation 1 is now flushed
        let indices = final_dataset.load_indices().await.unwrap();
        let mem_wal_index_meta = indices
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .expect("MemWAL index should still exist");

        let mem_wal_details = load_mem_wal_index_details(mem_wal_index_meta.clone()).unwrap();
        let gen_1 = mem_wal_details
            .mem_wal_list
            .iter()
            .find(|m| m.id.generation == 1)
            .expect("Generation 1 should still exist");
        assert!(gen_1.sealed, "Generation 1 should still be sealed");
        assert!(gen_1.flushed, "Generation 1 should now be flushed");
    }
}
