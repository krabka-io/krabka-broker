//! Shared fixtures for the fetch-session unit tests: a cache on a manually
//! advanced LRU clock, and builders for the `FetchRequest` and `FetchTopic`
//! values that the tests feed to it.

use std::sync::Arc;

use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use qubit_clock::ManualMonotonicClock;

use super::cache::FetchSessionCache;

/// Builds a cache whose LRU clock is a [`ManualMonotonicClock`] sitting at its
/// own origin. The returned `Arc` is both the clock the cache stamps from and
/// the handle that advances it, so a test can put successive allocations on
/// distinct points in the cache's recency order. The test advances logical
/// time with `clock.advance(..)` instead of a sleep between allocations.
pub(super) fn manual_cache(max_slots: usize) -> (FetchSessionCache, Arc<ManualMonotonicClock>) {
    let clock = ManualMonotonicClock::new_shared();
    let cache = FetchSessionCache::with_clock(max_slots, clock.clone());
    (cache, clock)
}

/// A one-nanosecond tick: the smallest advance that still gives the next
/// allocation a strictly greater last-use stamp than the previous one.
pub(super) const TICK: std::time::Duration = std::time::Duration::from_nanos(1);

pub(super) fn req(
    session_id: i32,
    session_epoch: i32,
    topics: Vec<FetchTopic>,
    forgotten: Vec<ForgottenTopic>,
) -> FetchRequest {
    FetchRequest {
        session_id,
        session_epoch,
        topics,
        forgotten_topics_data: forgotten,
        ..Default::default()
    }
}

pub(super) fn topic(name: &str, partitions: &[i32]) -> FetchTopic {
    FetchTopic {
        topic: name.to_string(),
        topic_id: WireUuid::ZERO,
        partitions: partitions
            .iter()
            .map(|&p| FetchPartition {
                partition: p,
                fetch_offset: 0,
                partition_max_bytes: 1024,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
