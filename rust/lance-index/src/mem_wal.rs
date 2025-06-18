use serde::{Deserialize, Serialize};
use lance_core::cache::DeepSizeOf;
use lance_core::Error;
use lance_table::format::pb;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct MemWal {
    pub memtable_location: String,
    pub wal_location: String,
    pub watermark: u64,
}

impl From<&MemWal> for pb::mem_wal_index_details::MemWal {
    fn from(mem_wal: &MemWal) -> Self {
        Self {
            memtable_location: mem_wal.memtable_location.clone(),
            wal_location: mem_wal.wal_location.clone(),
            watermark: mem_wal.watermark,
        }
    }
}

impl TryFrom<pb::mem_wal_index_details::MemWal> for MemWal {
    type Error = Error;

    fn try_from(mem_wal: pb::mem_wal_index_details::MemWal) -> lance_core::Result<Self> {
        Ok(Self {
            memtable_location: mem_wal.memtable_location.clone(),
            wal_location: mem_wal.wal_location.clone(),
            watermark: mem_wal.watermark,
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
            mem_wal_list: details.mem_wal_list.iter().map(|m| m.into()).collect()
        }
    }
}

impl TryFrom<pb::MemWalIndexDetails> for MemWalIndexDetails {
    type Error = Error;

    fn try_from(details: pb::MemWalIndexDetails) -> lance_core::Result<Self> {
        Ok(Self {
            mem_wal_list: details.mem_wal_list.into_iter()
                .map(MemWal::try_from)
                .collect::<lance_core::Result<_>>()?,
        })
    }
}