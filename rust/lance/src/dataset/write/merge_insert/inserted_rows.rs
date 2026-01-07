// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Key existence tracking for merge insert conflict detection.
//!
//! This module provides data structures for tracking keys of newly inserted rows
//! during merge insert operations. This is used for detecting conflicts when
//! concurrent transactions attempt to insert rows with the same key.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use arrow_array::cast::AsArray;
use arrow_array::{BinaryArray, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::DataType;
use deepsize::DeepSizeOf;
use lance_core::Result;
use lance_index::scalar::bloomfilter::sbbf::{bloom_filters_might_overlap, Sbbf, SbbfBuilder};
use lance_table::format::pb;
use snafu::location;

// ============================================================================
// Constants for Bloom Filter configuration
// ============================================================================

// IMPORTANT: All bloom filters for conflict detection MUST use the same size.
// Different sized filters cannot be correctly compared for intersection.
// These values are fixed to ensure all filters have identical dimensions.
//
// These values match the defaults in lance-index bloomfilter.rs:
// NumberOfItems: 8192 + Probability: 0.00057(1 in 1754) -> NumberOfBytes: 16384(16KiB)
pub const BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS: u64 = 8192;
pub const BLOOM_FILTER_DEFAULT_PROBABILITY: f64 = 0.00057;

/// Create a BloomFilter protobuf message with default configuration
fn create_bloom_filter_pb(bitmap: Vec<u8>, num_bits: u32) -> pb::transaction::BloomFilter {
    pb::transaction::BloomFilter {
        bitmap,
        num_bits,
        number_of_items: BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS,
        probability: BLOOM_FILTER_DEFAULT_PROBABILITY,
    }
}

// ============================================================================
// Key types for conflict detection
// ============================================================================

/// Key value that can be used in conflict detection for inserted rows
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    String(String),
    Int64(i64),
    UInt64(u64),
    Binary(Vec<u8>),
    Composite(Vec<KeyValue>),
}

impl KeyValue {
    /// Convert the key value to bytes for hashing
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::String(s) => s.as_bytes().to_vec(),
            Self::Int64(i) => i.to_le_bytes().to_vec(),
            Self::UInt64(u) => u.to_le_bytes().to_vec(),
            Self::Binary(b) => b.clone(),
            Self::Composite(values) => {
                let mut result = Vec::new();
                for value in values {
                    result.extend_from_slice(&value.to_bytes());
                    result.push(0); // separator
                }
                result
            }
        }
    }

    /// Get a hash of the key value
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.to_bytes().hash(&mut hasher);
        hasher.finish()
    }
}

// ============================================================================
// Bloom Filter Builder for tracking inserted rows
// ============================================================================

/// Builder for KeyExistenceFilter using a Split Block Bloom Filter (SBBF).
/// Used to track keys of inserted rows for conflict detection.
#[derive(Debug, Clone)]
pub struct KeyExistenceFilterBuilder {
    sbbf: Sbbf,
    /// Field IDs of columns that form the key (must match unenforced primary key)
    field_ids: Vec<i32>,
    /// Number of items inserted (for len())
    item_count: usize,
}

impl KeyExistenceFilterBuilder {
    /// Create a new KeyExistenceFilterBuilder using SBBF with default parameters.
    pub fn new(field_ids: Vec<i32>) -> Self {
        let sbbf = SbbfBuilder::new()
            .expected_items(BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS)
            .false_positive_probability(BLOOM_FILTER_DEFAULT_PROBABILITY)
            .build()
            .expect("Failed to build SBBF for KeyExistenceFilterBuilder");
        Self {
            sbbf,
            field_ids,
            item_count: 0,
        }
    }

    /// Add a key to the filter
    pub fn insert(&mut self, key: KeyValue) -> Result<()> {
        let bytes = key.to_bytes();
        self.sbbf.insert(&bytes[..]);
        self.item_count += 1;
        Ok(())
    }

    /// Check if a key might be present
    pub fn contains(&self, key: &KeyValue) -> bool {
        let bytes = key.to_bytes();
        self.sbbf.check(&bytes[..])
    }

    /// Check if this filter might intersect with another filter.
    /// This is probabilistic - may return true for non-overlapping sets (false positive).
    ///
    /// Returns an error if the filters have different sizes/configurations.
    pub fn might_intersect(&self, other: &Self) -> Result<bool> {
        self.sbbf
            .might_intersect(&other.sbbf)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string(), location!()))
    }

    /// Get the key field IDs
    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    /// Get the estimated size in bytes
    pub fn estimated_size_bytes(&self) -> usize {
        self.sbbf.size_bytes()
    }

    /// Convert to typed protobuf KeyExistenceFilter (Bloom variant)
    pub fn to_pb(&self) -> pb::transaction::KeyExistenceFilter {
        let bitmap = self.sbbf.to_bytes();
        let num_bits = (self.sbbf.size_bytes() as u32) * 8;
        pb::transaction::KeyExistenceFilter {
            field_ids: self.field_ids.clone(),
            data: Some(pb::transaction::key_existence_filter::Data::Bloom(
                create_bloom_filter_pb(bitmap, num_bits),
            )),
        }
    }

    /// Get the number of items
    pub fn len(&self) -> usize {
        self.item_count
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.item_count == 0
    }

    /// Check if this filter might produce false positives (Bloom filters are probabilistic)
    pub fn might_have_false_positives(&self) -> bool {
        true
    }

    /// Build a KeyExistenceFilter from this builder
    pub fn build(&self) -> KeyExistenceFilter {
        let bitmap = self.sbbf.to_bytes();
        let num_bits = (self.sbbf.size_bytes() as u32) * 8;
        KeyExistenceFilter {
            field_ids: self.field_ids.clone(),
            filter: FilterType::Bloom {
                bitmap,
                num_bits,
                number_of_items: BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS,
                probability: BLOOM_FILTER_DEFAULT_PROBABILITY,
            },
        }
    }
}

// ============================================================================
// KeyExistenceFilter metadata for conflict detection
// ============================================================================

/// Type of filter used for storing key existence data
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub enum FilterType {
    ExactSet(HashSet<u64>),
    Bloom {
        bitmap: Vec<u8>,
        num_bits: u32,
        /// Number of items the filter was sized for
        number_of_items: u64,
        /// False positive probability the filter was sized for
        probability: f64,
    },
}

/// Metadata about keys of newly inserted rows, used for conflict detection.
/// Only created when the merge insert's ON columns match the schema's unenforced primary key.
/// The presence of this filter indicates strict primary key conflict detection should be used.
/// Only tracks keys from INSERT operations, not UPDATE operations.
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct KeyExistenceFilter {
    pub field_ids: Vec<i32>,
    pub filter: FilterType,
}

impl KeyExistenceFilter {
    /// Create KeyExistenceFilter from a bloom filter builder
    pub fn from_bloom_filter(bloom: &KeyExistenceFilterBuilder) -> Self {
        bloom.build()
    }

    pub fn to_pb(&self) -> pb::transaction::KeyExistenceFilter {
        match &self.filter {
            FilterType::ExactSet(hashes) => pb::transaction::KeyExistenceFilter {
                field_ids: self.field_ids.clone(),
                data: Some(pb::transaction::key_existence_filter::Data::Exact(
                    pb::transaction::ExactKeySetFilter {
                        key_hashes: hashes.iter().copied().collect(),
                    },
                )),
            },
            FilterType::Bloom {
                bitmap,
                num_bits,
                number_of_items,
                probability,
            } => pb::transaction::KeyExistenceFilter {
                field_ids: self.field_ids.clone(),
                data: Some(pb::transaction::key_existence_filter::Data::Bloom(
                    pb::transaction::BloomFilter {
                        bitmap: bitmap.clone(),
                        num_bits: *num_bits,
                        number_of_items: *number_of_items,
                        probability: *probability,
                    },
                )),
            },
        }
    }

    pub fn from_pb(message: &pb::transaction::KeyExistenceFilter) -> Result<Self> {
        let field_ids = message.field_ids.clone();
        let filter = match message.data.as_ref() {
            Some(pb::transaction::key_existence_filter::Data::Exact(exact)) => {
                FilterType::ExactSet(exact.key_hashes.iter().copied().collect())
            }
            Some(pb::transaction::key_existence_filter::Data::Bloom(b)) => {
                // Use defaults for backwards compatibility with older filters
                // that don't have number_of_items/probability set (they will be 0)
                let number_of_items = if b.number_of_items == 0 {
                    BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS
                } else {
                    b.number_of_items
                };
                let probability = if b.probability == 0.0 {
                    BLOOM_FILTER_DEFAULT_PROBABILITY
                } else {
                    b.probability
                };
                FilterType::Bloom {
                    bitmap: b.bitmap.clone(),
                    num_bits: b.num_bits,
                    number_of_items,
                    probability,
                }
            }
            None => {
                // Treat missing data as empty exact set
                FilterType::ExactSet(HashSet::new())
            }
        };
        Ok(Self { field_ids, filter })
    }

    /// Determine if 2 filters intersect, and whether it might be a false positive.
    ///
    /// Returns `Err` if the bloom filter configs don't match (different expected_items or fpp),
    /// since bloom filters with different sizes cannot be reliably compared.
    ///
    /// Returns `Ok((has_intersection, might_be_false_positive))` on success.
    pub fn intersects(&self, other: &Self) -> Result<(bool, bool)> {
        match (&self.filter, &other.filter) {
            (FilterType::ExactSet(a), FilterType::ExactSet(b)) => {
                let has = a.iter().any(|h| b.contains(h));
                Ok((has, false))
            }
            (FilterType::ExactSet(_), FilterType::Bloom { .. })
            | (FilterType::Bloom { .. }, FilterType::ExactSet(_)) => {
                // ExactSet stores hashes from an unknown scheme, while Bloom uses XxHash64.
                // Since we can't reliably compare them, we conservatively assume
                // there might be an intersection to avoid missing conflicts.
                Ok((true, true))
            }
            (
                FilterType::Bloom {
                    bitmap: a_bits,
                    number_of_items: a_num_items,
                    probability: a_prob,
                    ..
                },
                FilterType::Bloom {
                    bitmap: b_bits,
                    number_of_items: b_num_items,
                    probability: b_prob,
                    ..
                },
            ) => {
                // Bloom filters with different configurations cannot be reliably compared
                // because they have different sizes and bit patterns
                if a_num_items != b_num_items || (a_prob - b_prob).abs() > f64::EPSILON {
                    return Err(lance_core::Error::invalid_input(
                        format!(
                            "Cannot compare bloom filters with different configurations: \
                             self(number_of_items={}, probability={}) vs other(number_of_items={}, probability={}). \
                             Both filters must use the same parameters for reliable intersection checking.",
                            a_num_items, a_prob, b_num_items, b_prob
                        ),
                        location!(),
                    ));
                }
                // Since configs are validated above, this should not fail
                let has = bloom_filters_might_overlap(a_bits, b_bits)
                    .map_err(|e| lance_core::Error::invalid_input(e.to_string(), location!()))?;
                Ok((has, has))
            }
        }
    }
}

// ============================================================================
// Utility functions for extracting key values from batches
// ============================================================================

/// Extract key value from a batch row for conflict detection bloom filter.
/// Returns None if any ON column is null or has an unsupported type.
pub fn extract_key_value_from_batch(
    batch: &RecordBatch,
    row_idx: usize,
    on_columns: &[String],
) -> Option<KeyValue> {
    let mut parts: Vec<KeyValue> = Vec::with_capacity(on_columns.len());

    for col_name in on_columns {
        let (col_idx, _) = batch.schema().column_with_name(col_name)?;
        let column = batch.column(col_idx);

        if column.is_null(row_idx) {
            return None; // Skip rows with null key values
        }

        let key_part = match column.data_type() {
            DataType::Utf8 => {
                let arr = column.as_any().downcast_ref::<StringArray>()?;
                KeyValue::String(arr.value(row_idx).to_string())
            }
            DataType::LargeUtf8 => {
                let arr = column.as_any().downcast_ref::<LargeStringArray>()?;
                KeyValue::String(arr.value(row_idx).to_string())
            }
            DataType::UInt64 => {
                let arr = column.as_primitive::<arrow_array::types::UInt64Type>();
                KeyValue::UInt64(arr.value(row_idx))
            }
            DataType::Int64 => {
                let arr = column.as_primitive::<arrow_array::types::Int64Type>();
                KeyValue::Int64(arr.value(row_idx))
            }
            DataType::UInt32 => {
                let arr = column.as_primitive::<arrow_array::types::UInt32Type>();
                KeyValue::UInt64(arr.value(row_idx) as u64)
            }
            DataType::Int32 => {
                let arr = column.as_primitive::<arrow_array::types::Int32Type>();
                KeyValue::Int64(arr.value(row_idx) as i64)
            }
            DataType::Binary => {
                let arr = column.as_any().downcast_ref::<BinaryArray>()?;
                KeyValue::Binary(arr.value(row_idx).to_vec())
            }
            DataType::LargeBinary => {
                let arr = column.as_any().downcast_ref::<LargeBinaryArray>()?;
                KeyValue::Binary(arr.value(row_idx).to_vec())
            }
            _ => return None, // Unsupported type
        };
        parts.push(key_part);
    }

    if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        Some(parts.into_iter().next().unwrap())
    } else {
        Some(KeyValue::Composite(parts))
    }
}
