// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use object_store::path::Path;

pub struct WalStore {
    
}

impl WalStore {

    pub fn new() -> Self {
        Self {
        }
    }
    
    pub fn location(base: &Path, region: &str, generation: u64) -> Path {
        todo!()
    }

    pub fn wal_file_path_with_entry_id(region: &str, generation: u64, entry_id: u64) -> Path {
        let reversed = entry_id.reverse_bits();
        let reversed_bits = format!("{:064b}", reversed);
        Path::from(format!("_wal/{}/{}/{}.arrow", region, generation, reversed_bits))
    }
    
}