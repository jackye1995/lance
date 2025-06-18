use std::sync::Arc;
use roaring::RoaringBitmap;
use snafu::location;
use tokio::io::AsyncWriteExt;
use lance_core::{Result, Error};
use lance_index::frag_reuse::{FragReuseIndexDetails, FRAG_REUSE_DETAILS_FILE_NAME, FRAG_REUSE_INDEX_NAME};
use lance_index::mem_wal::{MemWalIndexDetails, MEM_WAL_INDEX_NAME};
use lance_table::format::{pb, Index};
use lance_table::format::pb::{ExternalFile, FragmentReuseIndexDetails};
use lance_table::format::pb::fragment_reuse_index_details::{Content, InlineContent};
use crate::Dataset;

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

pub fn drop_mem_wal(
    dataset: &Dataset,
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
    })
}