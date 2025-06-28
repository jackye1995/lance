// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use crate::Dataset;
use crate::index::mem_wal::advance_mem_wal_generation;

pub struct MemTableStore {
    
    dataset: Arc<Dataset>,
    region: String,
}

impl MemTableStore {

    pub async fn new(mut dataset: Arc<Dataset>, region: &str) -> Self {
        
        
        
        
        Self {
        }
    }
    
    pub fn location(job_id: &str, region: &str, generation: u64) -> String {
        format!("memory://{}/{}/{}", job_id, region, generation)
    }

}