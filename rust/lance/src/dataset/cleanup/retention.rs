// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::time::Duration;

use chrono::TimeDelta;
use humantime::parse_duration;
use lance_core::{Error, Result};
use serde::{Deserialize, Deserializer};

pub(super) const MIN_THINNING_AGE: Duration = Duration::from_secs(2 * 86_400);

/// An age-based rule in a cleanup retention policy, ordered by increasing age.
///
/// Each version uses the oldest matching `KeepOne` rule. `RemoveAll` may only
/// appear last and expires sampled versions too. Current versions, tags and
/// branch dependencies retain their usual protection.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetentionRule {
    /// Keep the newest eligible commit per UTC epoch-aligned interval.
    KeepOne {
        /// Positive bucket width; each age band has independent buckets.
        #[serde(deserialize_with = "deserialize_retention_duration")]
        per_interval: Duration,
        /// Full-history window, at least two days, relative to the hinted commit
        /// (or this dataset's commit when no hint exists).
        #[serde(deserialize_with = "deserialize_retention_duration")]
        older_than: Duration,
    },
    /// Expire all matching versions older than a wall-clock age, including samples.
    RemoveAll {
        /// Non-negative expiration age. Zero expires every non-protected version.
        #[serde(deserialize_with = "deserialize_retention_duration")]
        older_than: Duration,
    },
}

fn deserialize_retention_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Duration, D::Error> {
    let value = String::deserialize(deserializer)?;
    parse_duration(&value).map_err(serde::de::Error::custom)
}

/// Parse a retention policy with duration strings such as `"1h"` and `"2days"`.
///
/// ```
/// # use lance::dataset::cleanup::parse_retention_policy;
/// let rules = parse_retention_policy(r#"[
///   {"type":"keep_one","per_interval":"1h","older_than":"2days"},
///   {"type":"remove_all","older_than":"30days"}
/// ]"#)?;
/// # Ok::<_, lance::Error>(())
/// ```
pub fn parse_retention_policy(value: &str) -> Result<Vec<RetentionRule>> {
    let rules: Vec<RetentionRule> = serde_json::from_str(value).map_err(|error| {
        Error::invalid_input(format!("Invalid cleanup retention policy: {error}"))
    })?;
    validate_retention_policy(&rules)?;
    Ok(rules)
}

pub(super) fn validate_retention_policy(rules: &[RetentionRule]) -> Result<()> {
    if rules.is_empty() {
        return Err(Error::invalid_input("retention policy must not be empty"));
    }
    let mut previous_age = None;
    for (index, rule) in rules.iter().enumerate() {
        let age = match *rule {
            RetentionRule::KeepOne {
                per_interval,
                older_than,
            } => {
                if per_interval.is_zero() {
                    return Err(Error::invalid_input(format!(
                        "keep_one.per_interval must be greater than zero at policy[{index}]"
                    )));
                }
                validate_thinning_age(older_than)?;
                older_than
            }
            RetentionRule::RemoveAll { older_than } => {
                if index + 1 != rules.len() {
                    return Err(Error::invalid_input(format!(
                        "remove_all must be the final retention rule, got policy[{index}]"
                    )));
                }
                TimeDelta::from_std(older_than).map_err(|_| {
                    Error::invalid_input(format!(
                        "remove_all.older_than is out of range: {older_than:?}"
                    ))
                })?;
                older_than
            }
        };
        if previous_age.is_some_and(|previous| age <= previous) {
            return Err(Error::invalid_input(format!(
                "retention policy ages must be strictly increasing; policy[{index}] has older_than={age:?}"
            )));
        }
        previous_age = Some(age);
    }
    Ok(())
}

fn validate_thinning_age(age: Duration) -> Result<()> {
    TimeDelta::from_std(age).map_err(|_| {
        Error::invalid_input(format!("keep_one.older_than is out of range: {age:?}"))
    })?;
    if age < MIN_THINNING_AGE {
        return Err(Error::invalid_input(format!(
            "keep_one.older_than must be at least 2 days, got {age:?}"
        )));
    }
    Ok(())
}
