//! Shared fixtures for the fetch-session unit tests: a cache on a manually
//! advanced LRU clock, and builders for the `FetchRequest` and `FetchTopic`
//! values that the tests feed to it.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use qubit_clock::ManualMonotonicClock;

use super::{
    CachedPartitionState, FetchSessionId, FetchSessionKey, SessionDecision,
    cache::FetchSessionCache,
};

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

/// A `Fetch` version that names topics, which the fixtures below build.
pub(super) const NAME_FETCH_VERSION: i16 = super::FIRST_TOPIC_ID_FETCH_VERSION - 1;

/// A one-nanosecond tick: the smallest advance that still gives the next
/// allocation a strictly greater last-use stamp than the previous one.
pub(super) const TICK: std::time::Duration = std::time::Duration::from_nanos(1);

#[derive(Clone, Copy, Default)]
pub(super) struct RequestSessionId(pub FetchSessionId);

#[derive(Clone, Copy, Default)]
pub(super) struct RequestSessionEpoch(pub super::FetchSessionEpoch);

#[derive(Default)]
pub(super) struct SessionRequestSetup {
    pub session_id: RequestSessionId,
    pub session_epoch: RequestSessionEpoch,
    pub topics: Vec<FetchTopic>,
    pub forgotten: Vec<ForgottenTopic>,
}

impl SessionRequestSetup {
    /// The first incremental request for a session allocated by the cache.
    pub(super) fn incremental(session_id: RequestSessionId) -> Self {
        Self {
            session_id,
            session_epoch: RequestSessionEpoch(1),
            ..Default::default()
        }
    }
}

pub(super) fn req(setup: SessionRequestSetup) -> FetchRequest {
    let SessionRequestSetup {
        session_id,
        session_epoch,
        topics,
        forgotten,
    } = setup;
    FetchRequest {
        session_id: session_id.0,
        session_epoch: session_epoch.0,
        topics,
        forgotten_topics_data: forgotten,
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ForgottenTopicSetup {
    #[default("t".into())]
    pub topic: String,
    pub topic_id: WireUuid,
    #[default(vec![PartitionIndex(0)])]
    pub partitions: Vec<PartitionIndex>,
}

pub(super) fn forgotten_topic(setup: ForgottenTopicSetup) -> ForgottenTopic {
    let ForgottenTopicSetup {
        topic,
        topic_id,
        partitions,
    } = setup;
    ForgottenTopic {
        topic,
        topic_id,
        partitions: partitions.into_iter().map(|index| index.0).collect(),
        ..Default::default()
    }
}

pub(super) fn topic(name: &str, partitions: &[PartitionIndex]) -> FetchTopic {
    FetchTopic {
        topic: name.to_string(),
        topic_id: WireUuid::ZERO,
        partitions: partitions
            .iter()
            .map(|&p| FetchPartition {
                partition: p.0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn seed_resolved_partition(
    cache: &FetchSessionCache,
    topic_id: WireUuid,
) -> FetchSessionId {
    cache.try_allocate(
        false,
        false,
        "alice".into(),
        vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 5,
                max_bytes: 1024,
                ..Default::default()
            },
        )],
    )
}

pub(super) fn error_code(cache: &FetchSessionCache, request: &FetchRequest, version: i16) -> i16 {
    match cache.classify(request, version) {
        SessionDecision::Error { code } => code,
        other => panic!("expected Error, got {other:?}"),
    }
}
