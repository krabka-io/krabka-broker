//! Request classification: the four-way decision that the fetch handler
//! dispatches on, and the epoch check and cached-set update behind it.
//!
//! `classify` reads the `session_id` and `session_epoch` pair off the request,
//! rejects a stale epoch or an unknown id with the KIP-227 error code, and on
//! a valid incremental fetch applies the partition-set diff and returns the
//! full effective partition set under one lock.

use std::sync::atomic::Ordering;

use krabka_protocol::owned::fetch_request::FetchRequest;

use super::{
    cache::FetchSessionCache,
    diff::apply_incremental,
    epoch::{
        FINAL_EPOCH, FetchSessionEpoch, FetchSessionId, INITIAL_EPOCH, INVALID_SESSION_ID,
        next_epoch,
    },
    state::{CachedPartitionState, FetchSessionKey},
};
use crate::codes;

/// Outcome of `FetchSessionCache::classify`. The handler dispatches on this
/// value before it does any read.
#[derive(Debug)]
pub enum SessionDecision {
    /// `(session_id=0, epoch=-1)`: serve from `req.topics`, with no caching,
    /// and a response `session_id = 0`.
    Sessionless,
    /// `(session_id=0, epoch=0)`: serve from `req.topics`, then ask the cache
    /// to allocate a new session for the result. The cache can refuse the
    /// allocation when it is full of privileged sessions. The response
    /// `session_id` is then `0`, and the client falls back to sessionless
    /// fetches next time.
    NewSession,
    /// `(session_id!=0, epoch>=0)` that matches the cached epoch: serve from
    /// the cached subscription set. The cache merges `req.topics` in as
    /// updates and new entries, and removes `forgotten_topics_data`. The
    /// response holds only the partitions whose state has changed.
    Incremental {
        session_id: FetchSessionId,
        /// This value is already incremented. It goes nowhere on the wire,
        /// because the response has no epoch field. The cache uses it as the
        /// *next* expected epoch for the following request.
        new_epoch: FetchSessionEpoch,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    },
    /// `(session_id!=0, epoch=-1)`: serve from `req.topics` like a sessionless
    /// fetch, then drop the cached session.
    Close { session_id: FetchSessionId },
    /// Protocol violation. Emit an empty response with this top-level
    /// `error_code` and `session_id = 0`.
    Error { code: i16 },
}

impl FetchSessionCache {
    /// Inspects the request and decides which of the four branches the fetch
    /// handler should take. On `Incremental` it does the following atomically:
    /// - validates the epoch,
    /// - removes `forgotten_topics_data` from the cached partition set,
    /// - merges `req.topics` into the cached set (updates `fetch_offset` etc.
    ///   on existing entries; adds new entries verbatim),
    /// - bumps `next_epoch`,
    /// - and returns the full effective partition set for the handler to
    ///   read.
    ///
    /// The handler must call `finalize_incremental` after it assembles the
    /// response, so that the `last_*` comparison fields stay in step with what
    /// the broker sent.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn classify(&self, req: &FetchRequest) -> SessionDecision {
        let sid = req.session_id;
        let epoch = req.session_epoch;

        if sid == INVALID_SESSION_ID {
            return match epoch {
                FINAL_EPOCH => SessionDecision::Sessionless,
                INITIAL_EPOCH => SessionDecision::NewSession,
                _ => SessionDecision::Error {
                    code: codes::INVALID_FETCH_SESSION_EPOCH,
                },
            };
        }

        let mut guard = self.inner.lock().expect("poisoned");

        if epoch == FINAL_EPOCH {
            if !guard.sessions.contains_key(&sid) {
                return SessionDecision::Error {
                    code: codes::FETCH_SESSION_ID_NOT_FOUND,
                };
            }
            return SessionDecision::Close { session_id: sid };
        }

        let Some(session) = guard.sessions.get_mut(&sid) else {
            return SessionDecision::Error {
                code: codes::FETCH_SESSION_ID_NOT_FOUND,
            };
        };

        if epoch != session.next_epoch {
            return SessionDecision::Error {
                code: codes::INVALID_FETCH_SESSION_EPOCH,
            };
        }

        // The forget + merge below add and drop partitions; snapshot the
        // count now so we can fold the net delta into `num_partitions`
        // (which backs the lock-free `total_partitions_cached()` gauge).
        let partitions_before = session.partitions.len();

        // Drop forgotten partitions then merge request topics (KIP-227). The
        // forget/merge logic — and the half-identity matching that prevents a
        // partial-identity request from shadowing a fully-resolved cached key —
        // lives in `apply_incremental`, verified by `fetch_session_model`.
        apply_incremental(
            &mut session.partitions,
            &req.forgotten_topics_data,
            &req.topics,
        );

        let partitions_after = session.partitions.len();
        if partitions_after >= partitions_before {
            self.num_partitions
                .fetch_add(partitions_after - partitions_before, Ordering::Relaxed);
        } else {
            self.num_partitions
                .fetch_sub(partitions_before - partitions_after, Ordering::Relaxed);
        }

        let new_epoch = next_epoch(session.next_epoch);
        session.next_epoch = new_epoch;

        let partitions: Vec<(FetchSessionKey, CachedPartitionState)> = session
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let privileged = session.privileged;

        // Mark the session most-recently-used. This is the only place a live
        // session moves in the recency order, and it is O(1): the entry is
        // unlinked and relinked at the head of its class's list. It happens
        // after the `session` borrow above ends, because both halves of the
        // cache live behind the one `guard`.
        guard.order.touch(sid, privileged, self.clock.nanos());

        SessionDecision::Incremental {
            session_id: sid,
            new_epoch,
            partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid as WireUuid,
    };

    use super::*;
    use crate::fetch_session::test_support::{req, topic};

    #[test]
    fn sessionless_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, FINAL_EPOCH, vec![], vec![]);
        assert!(matches!(cache.classify(&r), SessionDecision::Sessionless));
    }

    #[test]
    fn new_session_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, INITIAL_EPOCH, vec![topic("t", &[0])], vec![]);
        assert!(matches!(cache.classify(&r), SessionDecision::NewSession));
    }

    #[test]
    fn unknown_session_id_returns_not_found() {
        let cache = FetchSessionCache::new(10);
        let r = req(12345, 1, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert!(code == codes::FETCH_SESSION_ID_NOT_FOUND);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn stale_epoch_returns_invalid_epoch() {
        let cache = FetchSessionCache::new(10);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        // Session's expected next_epoch is 1; send epoch=99.
        let r = req(id, 99, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn close_request_returns_close_then_handler_drops() {
        let cache = FetchSessionCache::new(10);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        let r = req(id, FINAL_EPOCH, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Close { session_id } => assert!(session_id == id),
            other => panic!("expected Close, got {other:?}"),
        }
        cache.close(id);
        assert!(cache.len() == 0);
        // Subsequent classify with the same id is now NOT_FOUND.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert!(code == codes::FETCH_SESSION_ID_NOT_FOUND);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn invalid_session_id_zero_with_stray_epoch_is_error() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, 5, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn incremental_merges_request_topics_and_bumps_epoch() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid::ZERO,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 100,
                max_bytes: 1024,
                ..Default::default()
            },
        )];
        let id = cache.try_allocate(false, "alice".into(), initial);

        // Incremental that updates partition 0's fetch_offset and adds partition 1.
        let r = req(id, 1, vec![topic("t", &[0, 1])], vec![]);
        match cache.classify(&r) {
            SessionDecision::Incremental {
                session_id,
                new_epoch,
                partitions,
            } => {
                check!(session_id == id);
                check!(new_epoch == 2);
                check!(partitions.len() == 2);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }

        // Re-sending with the old epoch fails — broker advanced to 2.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_id_only() {
        // Reproduces the broker-jvm-acceptance regression: a v ≥ 13 client
        // opens a session, the broker resolves and caches `(name, id, p)`;
        // then the client sends an incremental that only carries `topic_id`
        // (empty `topic`). The merge must update the cached entry — not
        // insert a duplicate with default `max_bytes`, which would silently
        // drop bytes from the subsequent read.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([7u8; 16]);
        let cached_key = FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        };
        let id = cache.try_allocate(
            false,
            "alice".into(),
            vec![(
                cached_key.clone(),
                CachedPartitionState {
                    fetch_offset: 5,
                    max_bytes: 1024,
                    ..Default::default()
                },
            )],
        );

        // v ≥ 13 incremental: topic_id set, topic_name empty, new fetch_offset.
        let r = req(
            id,
            1,
            vec![FetchTopic {
                topic: String::new(),
                topic_id: tid,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 42,
                    partition_max_bytes: 2048,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let SessionDecision::Incremental { partitions, .. } = cache.classify(&r) else {
            panic!("expected Incremental");
        };
        // No duplicate entry created; the cached (fully-resolved) key is
        // preserved and its desired state updated in place.
        let expected = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: tid,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 42,
                last_fetched_epoch: -1,
                current_leader_epoch: -1,
                max_bytes: 2048,
                log_start_offset: -1,
                last_high_watermark: 0,
                last_last_stable_offset: 0,
                last_log_start_offset: 0,
                last_preferred_read_replica: 0,
                last_aborted_txns_hash: 0,
                last_error_code: 0,
            },
        )];
        assert!(partitions == expected);
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_name_only() {
        // Mirror case for v ≤ 12 clients: cache has (name, id, p) after
        // server-side resolution; request carries name only, id ZERO.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([9u8; 16]);
        let cached_key = FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        };
        let id = cache.try_allocate(
            false,
            "alice".into(),
            vec![(
                cached_key.clone(),
                CachedPartitionState {
                    fetch_offset: 5,
                    max_bytes: 1024,
                    ..Default::default()
                },
            )],
        );

        let r = req(
            id,
            1,
            vec![FetchTopic {
                topic: "t".into(),
                topic_id: WireUuid::ZERO,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 99,
                    partition_max_bytes: 4096,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let SessionDecision::Incremental { partitions, .. } = cache.classify(&r) else {
            panic!("expected Incremental");
        };
        let expected = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: tid,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 99,
                last_fetched_epoch: -1,
                current_leader_epoch: -1,
                max_bytes: 4096,
                log_start_offset: -1,
                last_high_watermark: 0,
                last_last_stable_offset: 0,
                last_log_start_offset: 0,
                last_preferred_read_replica: 0,
                last_aborted_txns_hash: 0,
                last_error_code: 0,
            },
        )];
        assert!(partitions == expected);
    }

    #[test]
    fn forgotten_topics_drop_partitions_from_cache() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 1,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 2,
                },
                CachedPartitionState::default(),
            ),
        ];
        let id = cache.try_allocate(false, "alice".into(), initial);

        let forgotten = vec![ForgottenTopic {
            topic: "t".into(),
            topic_id: WireUuid::ZERO,
            partitions: vec![1],
            ..Default::default()
        }];
        let r = req(id, 1, vec![], forgotten);
        match cache.classify(&r) {
            SessionDecision::Incremental { partitions, .. } => {
                let mut parts: Vec<i32> = partitions.iter().map(|(k, _)| k.partition).collect();
                parts.sort_unstable();
                assert!(parts == vec![0, 2]);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }
}
