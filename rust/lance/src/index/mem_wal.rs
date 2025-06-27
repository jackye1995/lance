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

pub fn open_mem_wal_index(index: Index) -> Result<Arc<MemWalIndex>> {
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
                    message: format!("Encountered MemWAL index mapping that has a region with no any generation: {}", region),
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
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        let wal_entries = updated_mem_wal.wal_entries();
        updated_mem_wal.wal_entries =
            pb::U64Segment::from(wal_entries.with_new_high(entry_id)).encode_to_vec();
        updated_mem_wal
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
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.sealed = true;
        updated_mem_wal
    };

    mutate_mem_wal(dataset, mem_wal_region, mem_wal_generation, mutate).await
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

    mutate_mem_wal(dataset, mem_wal_region, mem_wal_generation, mutate).await
}

/// Mark the specific MemWAL as flushed.
/// This is intended to be used as a part of the Update transaction.
pub fn update_indices_mark_mem_wal_as_flushed(
    dataset_version: u64,
    indices: &mut [Index],
    region: &str,
    generation: u64,
) -> Result<()> {
    if let Some(pos) = indices.iter().position(|x| x.name == MEM_WAL_INDEX_NAME) {
        let mem_wal_index_meta = indices[pos].clone();
        let mut details = load_mem_wal_index_details(mem_wal_index_meta)?;
        if let Some(mem_wal) = details
            .mem_wal_list
            .iter_mut()
            .find(|m| m.id.region == region && m.id.generation == generation)
        {
            mem_wal.flushed = true;
            indices[pos] = new_mem_wal_index_meta(dataset_version, details.mem_wal_list)?;
            Ok(())
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
        Err(Error::NotSupported {
            source: "MemWAL is not enabled".into(),
            location: location!(),
        })
    }
}

/// Mark the specific MemWAL as flushed, in the list of indices in the dataset.
/// This is intended to be used as a part of the Update transaction after resolving all conflicts.
pub fn update_mem_wal_index_from_indices_list(
    dataset_version: u64,
    indices: &mut Vec<Index>,
    added: &[MemWal],
    updated: &[MemWal],
    removed: &[MemWal],
) -> Result<()> {
    let new_meta = if let Some(pos) = indices
        .iter()
        .position(|idx| idx.name != MEM_WAL_INDEX_NAME)
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
    let mutate = |mem_wal: &MemWal| -> MemWal {
        let mut updated_mem_wal = mem_wal.clone();
        updated_mem_wal.mem_table_location = new_mem_table_location.to_owned();
        updated_mem_wal
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
    F: Fn(&MemWal) -> MemWal,
{
    if let Some(mem_wal_index) = dataset.open_mem_wal_index(&NoOpMetricsCollector).await? {
        if let Some(generations) = mem_wal_index.mem_wal_map.get(region) {
            if let Some(mem_wal) = generations.get(&generation) {
                let updated_mem_wal = mutate(mem_wal);

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
