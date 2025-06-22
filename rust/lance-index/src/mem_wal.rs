use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use itertools::Itertools;
use roaring::RoaringBitmap;
use lance_core::cache::DeepSizeOf;
use lance_core::Error;
use lance_table::format::pb;
use serde::{Deserialize, Serialize};
use snafu::location;
use crate::frag_reuse::{FragReuseIndex, FragReuseIndexDetails};
use crate::{Index, IndexType};

pub const MEM_WAL_INDEX_NAME: &str = "__lance_mem_wal";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct MemWal {
    pub mem_table_location: String,
    pub prev_mem_table_location: String,
    pub wal_location: String,
    pub sealed: bool,
    pub flushed: bool,
    pub created_at: u64,
}

impl From<&MemWal> for pb::mem_wal_index_details::MemWal {
    fn from(mem_wal: &MemWal) -> Self {
        Self {
            mem_table_location: mem_wal.mem_table_location.clone(),
            prev_mem_table_location: mem_wal.prev_mem_table_location.clone(),
            wal_location: mem_wal.wal_location.clone(),
            sealed: mem_wal.sealed,
            flushed: mem_wal.flushed,
            created_at: mem_wal.created_at,
        }
    }
}

impl TryFrom<pb::mem_wal_index_details::MemWal> for MemWal {
    type Error = Error;

    fn try_from(mem_wal: pb::mem_wal_index_details::MemWal) -> lance_core::Result<Self> {
        Ok(Self {
            mem_table_location: mem_wal.mem_table_location.clone(),
            prev_mem_table_location: mem_wal.prev_mem_table_location.clone(),
            wal_location: mem_wal.wal_location.clone(),
            sealed: mem_wal.sealed,
            flushed: mem_wal.flushed,
            created_at: mem_wal.created_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct MemWalIndexDetails {
    pub mem_wal_list: Vec<MemWal>,
}

impl From<&MemWalIndexDetails> for pb::MemWalIndexDetails {
    fn from(details: &MemWalIndexDetails) -> Self {
        Self {
            mem_wal_list: details.mem_wal_list.iter()
                .map(|m| m.into())
                // make sure the latest
                .sorted_by_key(|m: &pb::mem_wal_index_details::MemWal| 0 - m.created_at)
                .collect(),
        }
    }
}

impl TryFrom<pb::MemWalIndexDetails> for MemWalIndexDetails {
    type Error = Error;

    fn try_from(details: pb::MemWalIndexDetails) -> lance_core::Result<Self> {
        Ok(Self {
            mem_wal_list: details
                .mem_wal_list
                .into_iter()
                .map(MemWal::try_from)
                .collect::<lance_core::Result<_>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct MemWalIndex {
    pub details: MemWalIndexDetails,
}

impl MemWalIndex {

    pub fn new(details: MemWalIndexDetails) -> Self {
        Self { details }
    }
}

#[derive(Serialize)]
struct MemWalStatistics {
    num_mem_wal_list: usize,
}


#[async_trait]
impl Index for MemWalIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn as_vector_index(self: Arc<Self>) -> lance_core::Result<Arc<dyn crate::vector::VectorIndex>> {
        Err(Error::NotSupported {
            source: "FragReuseIndex is not a vector index".into(),
            location: location!(),
        })
    }

    fn statistics(&self) -> lance_core::Result<serde_json::Value> {
        let stats = MemWalStatistics {
            num_mem_wal_list: self.details.mem_wal_list.len(),
        };
        serde_json::to_value(stats).map_err(|e| Error::Internal {
            message: format!("failed to serialize MemWAL index statistics: {}", e),
            location: location!(),
        })
    }

    async fn prewarm(&self) -> lance_core::Result<()> {
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::MemWal
    }

    async fn calculate_included_frags(&self) -> lance_core::Result<RoaringBitmap> {
        unimplemented!()
    }
}