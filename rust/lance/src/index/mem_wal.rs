use std::sync::Arc;
use snafu::location;
use lance_core::{Result, Error};
use lance_index::mem_wal::MemWalIndexDetails;
use lance_table::format::{pb, Index};
use crate::Dataset;

pub fn load_mem_wal_index_details(index: &Index) -> Result<Arc<MemWalIndexDetails>> {
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
    Ok(Arc::new(MemWalIndexDetails::try_from(proto)?))
}

