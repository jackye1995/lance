// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::{Error, Result};
use serde::{Deserialize, Serialize};

/// Consumer start position when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartPosition {
    /// Start from the first WAL entry.
    Earliest,
    /// Start after the current last WAL entry.
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupOffset {
    pub partition_id: u32,
    pub producer_id: String,
    pub next_entry_position: u64,
}

pub fn validate_group_id(group_id: &str) -> Result<()> {
    if group_id.is_empty() {
        return Err(Error::invalid_input("consumer group_id cannot be empty"));
    }
    if group_id == "." || group_id == ".." {
        return Err(Error::invalid_input(format!(
            "consumer group_id '{}' cannot be a relative path segment",
            group_id
        )));
    }
    if group_id.contains('/') || group_id.contains('\\') {
        return Err(Error::invalid_input(format!(
            "consumer group_id '{}' cannot contain path separators",
            group_id
        )));
    }
    if group_id.contains('$') {
        return Err(Error::invalid_input(format!(
            "consumer group_id '{}' cannot contain '$'",
            group_id
        )));
    }
    Ok(())
}
