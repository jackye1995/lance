// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Which versions of a dataset must survive, and which may be discarded.
//!
//! Shared by [`cleanup`](super::cleanup) and [`expire`](super::expire). The two differ
//! only in what they delete and in where they get a version's age from; the rules for
//! what is protected are the same and live here so they cannot drift apart.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::Dataset;
use crate::Result;

/// Versions that must survive regardless of age or policy.
///
/// Resolved by reading the dataset's refs, never by reading manifests. Listing failures
/// propagate rather than yielding an empty set: a tag that cannot be read must never
/// license a deletion.
#[derive(Clone, Debug, Default)]
pub struct ProtectedVersions {
    /// The newest version. Deleting it would destroy the dataset.
    pub latest: u64,
    /// Versions a tag on the current branch points at.
    pub tagged: HashSet<u64>,
    /// Versions a branch is rooted at. Removing one orphans that branch's history.
    pub branch_referenced: HashSet<u64>,
}

impl ProtectedVersions {
    /// Resolve the protected set for `dataset`.
    ///
    /// Only tags on the current branch are collected, matching cleanup: a tag on another
    /// branch protects manifests in that branch's own path, not in this one.
    pub async fn resolve(dataset: &Dataset) -> Result<Self> {
        let tags = dataset.tags().list().await?;
        let current_branch = &dataset.manifest.branch;
        let tagged = Self::tagged_versions(&tags, current_branch.as_ref());

        let branch_identifier = dataset.branch_identifier().await?;
        let branches = dataset.branches().list().await?;
        let branch_referenced = branch_identifier
            .collect_referenced_versions(&branches)
            .into_iter()
            .map(|(_name, version)| version)
            .collect();

        Ok(Self {
            latest: dataset.manifest.version,
            tagged,
            branch_referenced,
        })
    }

    /// Tags on `current_branch`, as cleanup selects them.
    fn tagged_versions(
        tags: &HashMap<String, crate::dataset::refs::TagContents>,
        current_branch: Option<&String>,
    ) -> HashSet<u64> {
        tags.values()
            .filter(|tag| match (tag.branch.as_ref(), current_branch) {
                (Some(branch_of_tag), Some(current_branch)) => branch_of_tag == current_branch,
                (None, None) => true,
                _ => false,
            })
            .map(|tag| tag.version)
            .collect()
    }

    /// True when `version` must survive whatever the policy says.
    pub fn contains(&self, version: u64) -> bool {
        version >= self.latest
            || self.tagged.contains(&version)
            || self.branch_referenced.contains(&version)
    }

    /// Protected versions that the policy would otherwise have expired, so a caller can
    /// report them rather than silently keeping them.
    pub fn tagged_but_expired<I>(&self, expired: I) -> HashSet<u64>
    where
        I: IntoIterator<Item = u64>,
    {
        expired
            .into_iter()
            .filter(|v| self.tagged.contains(v))
            .collect()
    }
}

/// Keep the newest version in each bucket of `width`, discarding the rest.
///
/// Buckets are absolute rather than relative to the newest candidate, so the survivors do
/// not shift as the table grows: a version's bucket depends only on its own timestamp.
/// Returns the versions to keep.
///
/// Ties on timestamp resolve to the higher version, so the survivor of a bucket is always
/// the latest state within it.
pub fn thin_to_one_per(candidates: &[(u64, DateTime<Utc>)], width: Duration) -> HashSet<u64> {
    let width_secs = width.as_secs().max(1) as i64;
    let mut newest_in_bucket: HashMap<i64, (u64, DateTime<Utc>)> = HashMap::new();
    for (version, timestamp) in candidates {
        let bucket = timestamp.timestamp().div_euclid(width_secs);
        newest_in_bucket
            .entry(bucket)
            .and_modify(|held| {
                if (*timestamp, *version) > (held.1, held.0) {
                    *held = (*version, *timestamp);
                }
            })
            .or_insert((*version, *timestamp));
    }
    newest_in_bucket
        .into_values()
        .map(|(version, _)| version)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn protected_versions_covers_latest_tags_and_branches() {
        let protected = ProtectedVersions {
            latest: 100,
            tagged: HashSet::from([7]),
            branch_referenced: HashSet::from([42]),
        };

        assert!(protected.contains(100), "the latest version is protected");
        assert!(
            protected.contains(101),
            "anything at or past latest is protected"
        );
        assert!(protected.contains(7), "a tagged version is protected");
        assert!(
            protected.contains(42),
            "a branch-referenced version is protected"
        );
        assert!(!protected.contains(41), "an ordinary old version is not");
    }

    #[test]
    fn thinning_keeps_exactly_one_per_bucket() {
        // Three versions in the first hour, two in the second.
        let hour = Duration::from_secs(3600);
        let candidates = vec![
            (1, ts(0)),
            (2, ts(600)),
            (3, ts(3599)),
            (4, ts(3600)),
            (5, ts(7000)),
        ];

        let kept = thin_to_one_per(&candidates, hour);
        assert_eq!(kept.len(), 2, "one survivor per hour bucket");
        assert!(
            kept.contains(&3),
            "the newest of hour 0 survives, not 1 or 2"
        );
        assert!(kept.contains(&5), "the newest of hour 1 survives, not 4");
    }

    #[test]
    fn thinning_survivor_is_the_newest_not_an_arbitrary_one() {
        // Deliberately out of order: a naive implementation that keeps the first seen
        // would keep version 1 here.
        let candidates = vec![(1, ts(10)), (9, ts(20)), (5, ts(15))];
        let kept = thin_to_one_per(&candidates, Duration::from_secs(3600));
        assert_eq!(kept, HashSet::from([9]));
    }

    #[test]
    fn thinning_breaks_timestamp_ties_toward_the_higher_version() {
        // Equal timestamps happen when commits land inside one clock tick; the later
        // version is the later state.
        let candidates = vec![(4, ts(10)), (6, ts(10)), (5, ts(10))];
        let kept = thin_to_one_per(&candidates, Duration::from_secs(3600));
        assert_eq!(kept, HashSet::from([6]));
    }

    #[test]
    fn thinning_a_zero_width_bucket_keeps_every_version() {
        // Guard against a divide-by-zero and against silently collapsing everything into
        // one bucket, which would delete all but one version.
        let candidates = vec![(1, ts(0)), (2, ts(1)), (3, ts(2))];
        let kept = thin_to_one_per(&candidates, Duration::from_secs(0));
        assert_eq!(kept.len(), 3, "a zero width must not collapse the history");
    }

    #[test]
    fn thinning_nothing_keeps_nothing() {
        let kept = thin_to_one_per(&[], Duration::from_secs(3600));
        assert!(kept.is_empty());
    }
}
