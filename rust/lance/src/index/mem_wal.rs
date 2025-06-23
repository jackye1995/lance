use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::frag_reuse::{
    FragReuseIndexDetails, FRAG_REUSE_DETAILS_FILE_NAME, FRAG_REUSE_INDEX_NAME,
};
use lance_index::mem_wal::{MemWal, MemWalIndex, MemWalIndexDetails, MEM_WAL_INDEX_NAME};
use lance_table::format::pb::fragment_reuse_index_details::{Content, InlineContent};
use lance_table::format::pb::{ExternalFile, FragmentReuseIndexDetails};
use lance_table::format::{pb, Index};
use roaring::RoaringBitmap;
use snafu::location;
use std::sync::Arc;
use chrono::Utc;
use tokio::io::AsyncWriteExt;

pub fn load_mem_wal_index_details(index: &Index) -> Result<MemWalIndexDetails> {
    let details_any = index.index_details.clone();
    if details_any.is_none()
        || !details_any
            .as_ref()
            .unwrap()
            .type_url
            .ends_with("MemWalIndexDetails")
    {
        return Err(Error::Index {
            message: "Index details is not for the MemWAL index".into(),
            location: location!(),
        });
    }

    let proto = details_any.unwrap().to_msg::<pb::MemWalIndexDetails>()?;
    Ok(MemWalIndexDetails::try_from(proto)?)
}

/// Mark a write to the MemWAL.
/// This will raise the watermark of the specific MemWAL by 1
pub async fn mark_write_to_mem_wal(
    dataset: &mut Dataset,
    mem_wal: MemWal,
) -> Result<()> {

}


/// Initialize a new MemWAL.
/// Atomically compare-and-set the current MemTable as sealed
/// If there is an old unsealed MemWAL, seal it at the same time.
pub async fn mark_mem_wal_as_sealed(
    dataset: &mut Dataset,
    mem_wal_to_recover: Option<&MemWal>,
    next_wal_location: Option<&str>,
    next_mem_table_location: Option<&str>,
) -> Result<()> {

}

/// Mark a MemWAL as recovered after replaying the WAL.
/// Atomically compare-and-swap the current MemTable location in the current MemWAL
/// to the [`new_mem_table_location`]
pub fn mark_mem_wal_as_recovered(
    dataset: &Dataset,
    mem_wal_to_recover: &MemWal,
    new_mem_table_location: &str,
) {

}

/// Mark a MemWAL as flushed after replaying the WAL.
/// Atomically compare-and-set the current MemTable as flushed
pub fn mark_mem_wal_as_flushed(
    dataset: &Dataset,
    mem_wal_to_flush: &MemWal,
) {

}

pub(crate) fn build_mem_wal_index_metadata(
    dataset: &Dataset,
    index_meta: Option<&Index>,
    new_index_details: MemWalIndexDetails,
) -> Result<Index> {
    let index_id = uuid::Uuid::new_v4();
    let new_index_details_proto = pb::MemWalIndexDetails::from(&new_index_details);

    Ok(Index {
        uuid: index_id,
        name: MEM_WAL_INDEX_NAME.to_string(),
        fields: vec![],
        dataset_version: dataset.manifest.version,
        fragment_bitmap: None,
        index_details: Some(prost_types::Any::from_msg(&new_index_details_proto)?),
        index_version: index_meta.map_or(0, |index_meta| index_meta.index_version),
        created_at: Some(Utc::now()),
    })
}
