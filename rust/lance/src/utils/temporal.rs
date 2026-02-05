// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! These utilities make it possible to mock out the "current time" when running
//! unit tests.  Anywhere in production code where we need to get the current time
//! we should use the below methods and types instead of the builtin methods and types

use chrono::{DateTime, TimeZone, Utc};
#[cfg(test)]
use mock_instant::thread_local::{SystemTime as NativeSystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use std::time::{SystemTime as NativeSystemTime, UNIX_EPOCH};

pub type SystemTime = NativeSystemTime;

/// Mirror function that mimics DateTime<Utc>::now() with the exception that it
/// uses the potentially mocked system time.
pub fn utc_now() -> DateTime<Utc> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch");
    let naive = DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .expect("DateTime::from_timestamp")
        .naive_utc();
    Utc.from_utc_datetime(&naive)
}

pub fn timestamp_to_nanos(timestamp: Option<SystemTime>) -> u128 {
    let timestamp = timestamp.unwrap_or_else(SystemTime::now);
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

const MILLIS_TO_NANOS: u128 = 1_000_000;

/// Returns a timestamp in nanoseconds that is guaranteed to be strictly greater
/// than `prev_timestamp_nanos` (by at least 1 millisecond).
///
/// Formula: `max(current_time_nanos, prev_timestamp_nanos + 1ms)`
///
/// If `prev_timestamp_nanos` is `None` (first version), returns the current time.
pub fn monotonic_timestamp_nanos(
    timestamp: Option<SystemTime>,
    prev_timestamp_nanos: Option<u128>,
) -> u128 {
    let current_nanos = timestamp_to_nanos(timestamp);
    match prev_timestamp_nanos {
        Some(prev) => std::cmp::max(current_nanos, prev + MILLIS_TO_NANOS),
        None => current_nanos,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;
    use std::time::Duration;

    #[test]
    fn test_monotonic_timestamp_no_previous() {
        MockClock::set_system_time(Duration::from_secs(100));
        let result = monotonic_timestamp_nanos(None, None);
        assert_eq!(result, Duration::from_secs(100).as_nanos());
    }

    #[test]
    fn test_monotonic_timestamp_current_time_ahead() {
        // current time is well ahead of prev + 1ms, so current time wins
        MockClock::set_system_time(Duration::from_secs(200));
        let prev = Duration::from_secs(100).as_nanos();
        let result = monotonic_timestamp_nanos(None, Some(prev));
        assert_eq!(result, Duration::from_secs(200).as_nanos());
    }

    #[test]
    fn test_monotonic_timestamp_prev_ahead() {
        // current time is behind prev, so prev + 1ms wins
        MockClock::set_system_time(Duration::from_secs(100));
        let prev = Duration::from_secs(200).as_nanos();
        let result = monotonic_timestamp_nanos(None, Some(prev));
        assert_eq!(result, prev + MILLIS_TO_NANOS);
    }

    #[test]
    fn test_monotonic_timestamp_same_time() {
        // current time == prev, so prev + 1ms wins
        MockClock::set_system_time(Duration::from_secs(100));
        let prev = Duration::from_secs(100).as_nanos();
        let result = monotonic_timestamp_nanos(None, Some(prev));
        assert_eq!(result, prev + MILLIS_TO_NANOS);
    }
}
