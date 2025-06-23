// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::dataset::transaction::{Operation, Transaction};
use crate::index::DatasetIndexInternalExt;
use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::mem_wal::{MemWal, MemWalIndex, MemWalIndexDetails, MEM_WAL_INDEX_NAME};
use lance_index::metrics::NoOpMetricsCollector;
use lance_table::format::{pb, Index};
use prost::Message;
use snafu::location;
use std::collections::HashMap;
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

pub fn open_mem_wal_index(index: Index) -> Result<Arc<MemWalIndex>> {
    Ok(Arc::new(MemWalIndex::new(load_mem_wal_index_details(index)?)))
}

/// Advance the generation of the MemWAL for the given region.
/// If the MemWAL does not exist, create one with generation 0.
/// If the MemWAL exists, seal the one with the latest generation,
/// and open one with the same name and the next generation.
/// If the MemWALIndex structure does not exist, create it along the way.
pub async fn advance_mem_wal_generation(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    new_mem_table_location: &str,
    new_wal_location: &str,
) -> Result<()> {
    let transaction = if let Some(mem_wal_index) =
        dataset.open_mem_wal_index(&NoOpMetricsCollector).await?
    {
        let (added_mem_wal, updated_mem_wal, removed_mem_wal_list, mut new_mem_wal_list) =
            if let Some(generations) = mem_wal_index.mem_wal_map.get(mem_wal_region) {
                let mut new_mem_wal_list: Vec<MemWal> = Vec::new();
                let mut removed_mem_wal_list: Vec<MemWal> = Vec::new();
                // MemWALs of the same region is ordered increasingly by its generation
                let mut values_iter = generations.values().rev();
                if let Some(latest_mem_wal) = values_iter.next() {

                    let updated_mem_wal = if !latest_mem_wal.sealed {
                        let mut updated_mem_wal = latest_mem_wal.clone();
                        updated_mem_wal.sealed = true;
                        new_mem_wal_list.push(updated_mem_wal.clone());
                        removed_mem_wal_list.push(latest_mem_wal.clone());
                        Some(updated_mem_wal)
                    } else {
                        None
                    };

                    let added_mem_wal = MemWal::new_empty(mem_wal_region, latest_mem_wal.generation + 1, new_mem_table_location, new_wal_location);
                    new_mem_wal_list.push(added_mem_wal.clone());


                    // add the remaining MemWALs of the same region
                    for mem_wal in values_iter {
                        new_mem_wal_list.push(mem_wal.clone());
                    }

                    Ok((
                        added_mem_wal,
                        updated_mem_wal,
                        removed_mem_wal_list,
                        new_mem_wal_list,
                    ))
                } else {
                    Err(Error::Internal {
                    message: format!("Encountered MemWAL index mapping that has a region with no any generation: {}", mem_wal_region),
                    location: location!(),
                })
                }
            } else {
                let added_mem_wal =
                    MemWal::new_empty(mem_wal_region, 0, new_mem_table_location, new_wal_location);

                Ok((added_mem_wal.clone(), None, vec![], vec![added_mem_wal]))
            }?;

        mem_wal_index
            .mem_wal_map
            .iter()
            .filter(|(region, _)| *region != mem_wal_region)
            .for_each(|(_, generations)| new_mem_wal_list.extend(generations.values().cloned()));

        Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added_mem_wal: Some(added_mem_wal),
                updated_mem_wal,
                removed_mem_wal_list,
                new_mem_wal_list: Some(new_mem_wal_list),
            },
            None,
            None,
        )
    } else {
        // this is the first time the MemWAL index is created
        let added_mem_wal =
            MemWal::new_empty(mem_wal_region, 0, new_mem_table_location, new_wal_location);

        Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added_mem_wal: Some(added_mem_wal.clone()),
                updated_mem_wal: None,
                removed_mem_wal_list: Vec::new(),
                new_mem_wal_list: Some(vec![added_mem_wal]),
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
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        let wal_entries = updated_mem_wal.wal_entries();
        updated_mem_wal.wal_entries =
            pb::U64Segment::from(wal_entries.with_new_high(entry_id)).encode_to_vec();
        updated_mem_wal
    };

    mutate_mem_wal_generation(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as sealed.
/// Typically, it is recommended to call [`advance_mem_wal_generation`]
/// But this will always keep the table in a state with an unsealed MemTable.
/// Calling this function will only seal the current latest MemWAL,
/// without opening the next generation.
pub async fn mark_mem_wal_as_sealed(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.sealed = true;
        updated_mem_wal
    };

    mutate_mem_wal_generation(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as flushed
pub async fn mark_mem_wal_as_flushed(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.flushed = true;
        updated_mem_wal
    };

    mutate_mem_wal_generation(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as flushed.
/// This is intended to be used as a part of the Update transaction.
pub fn update_indices_mark_mem_wal_as_flushed(
    dataset_version: u64,
    indices: &mut Vec<Index>,
    mem_wal_region: &str,
    mem_wal_generation: u64,
) -> Result<()> {
    if let Some(pos) = indices.iter().position(|x| x.name == MEM_WAL_INDEX_NAME) {
        let mem_wal_index_meta = indices[pos].clone();
        let mut details = load_mem_wal_index_details(mem_wal_index_meta.clone())?;
        if let Some(mem_wal) = details.mem_wal_list.iter_mut()
            .find(|m| m.region == mem_wal_region && m.generation == mem_wal_generation) {
            mem_wal.flushed = true;
            indices[pos] = new_mem_wal_index_meta(dataset_version, details.mem_wal_list)?;
            Ok(())
        } else {
            Err(Error::invalid_input(
                format!(
                    "Cannot find MemWAL generation {} for region {}",
                    mem_wal_generation, mem_wal_region
                ),
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

/// Start the replay process of a MemWAL.
/// This updates the MemTable location of the specific MemWAL.
pub async fn start_replay_mem_wal(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
    new_mem_table_location: &str,
) -> Result<()> {
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.mem_table_location = new_mem_table_location.to_owned();
        updated_mem_wal
    };

    mutate_mem_wal_generation(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Remove a set of MemWALs given the regions and related generations.
/// This is used for clean up WALs after the MemTable is flushed.
pub async fn remove_mem_wal_generations(
    dataset: &mut Dataset,
    region_generations_to_remove: &HashMap<String, Vec<u64>>,
) -> Result<()> {
    if let Some(mem_wal_index) = dataset.open_mem_wal_index(&NoOpMetricsCollector).await? {
        let mut removed_mem_wal_list = Vec::new();
        let mut new_mem_wal_list = Vec::new();
        for (region, generations) in mem_wal_index.mem_wal_map.iter() {
            for (gen, mem_wal) in generations {
                if region_generations_to_remove.contains_key(region)
                    && region_generations_to_remove
                        .get(region.as_str())
                        .unwrap()
                        .contains(gen)
                {
                    removed_mem_wal_list.push(mem_wal.clone());
                } else {
                    new_mem_wal_list.push(mem_wal.clone());
                }
            }
        }

        let transaction = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                added_mem_wal: None,
                updated_mem_wal: None,
                removed_mem_wal_list,
                new_mem_wal_list: Some(new_mem_wal_list),
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

async fn mutate_mem_wal_generation<F>(
    dataset: &mut Dataset,
    mem_wal_region: &str,
    mem_wal_generation: u64,
    mutate: F,
) -> Result<()>
where
    F: Fn(&MemWal) -> MemWal,
{
    if let Some(mem_wal_index) = dataset.open_mem_wal_index(&NoOpMetricsCollector).await? {
        if let Some(generations) = mem_wal_index.mem_wal_map.get(mem_wal_region) {
            if let Some(mem_wal) = generations.get(&mem_wal_generation) {
                let updated_mem_wal = mutate(mem_wal);
                let mut new_mem_wal_list = vec![updated_mem_wal.clone()];
                for (region, generations) in mem_wal_index.mem_wal_map.iter() {
                    for (gen, mem_wal) in generations {
                        if !(mem_wal_region == region && mem_wal_generation == *gen) {
                            new_mem_wal_list.push(mem_wal.clone());
                        }
                    }
                }

                let transaction = Transaction::new(
                    dataset.manifest.version,
                    Operation::UpdateMemWalState {
                        added_mem_wal: None,
                        updated_mem_wal: Some(updated_mem_wal),
                        removed_mem_wal_list: vec![mem_wal.clone()],
                        new_mem_wal_list: Some(new_mem_wal_list),
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
                        mem_wal_generation, mem_wal_region
                    ),
                    location!(),
                ))
            }
        } else {
            Err(Error::invalid_input(
                format!("Cannot find MemWAL for region {}", mem_wal_region),
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
