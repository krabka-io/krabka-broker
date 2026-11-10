//! Request classification: the three-way decision that the fetch handler
//! dispatches on, and the epoch check and cached-set update behind it.
//!
//! `classify` reads the `session_id` and `session_epoch` pair off the request.
//! It decides full vs. incremental from `session_epoch` alone (Kafka's
//! `FetchMetadata.isFull()`), rejects a stale epoch or an unknown id on the
//! incremental path with the KIP-227 error code, and on a valid incremental
//! fetch applies the partition-set diff and returns the full effective
//! partition set under one lock.

use {
    super::{
        cache::FetchSessionCache,
        diff::apply_incremental,
        epoch::{
            FINAL_EPOCH, FIRST_TOPIC_ID_FETCH_VERSION, FetchSessionEpoch, FetchSessionId,
            INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
        },
        state::{CachedPartitionState, FetchSessionKey},
    },
    crate::codes,
    krabka_protocol::owned::fetch_request::FetchRequest,
    qubit_clock::MonotonicClock as _,
    std::sync::{PoisonError, atomic::Ordering},
};

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
        /// The partitions the session forgot in the last
        /// [`super::RECENTLY_FORGOTTEN_TTL`] and has not named again, this
        /// request's included. The read loop wakes a parked consumer fetch
        /// when one of them moves; see
        /// [`super::ForgottenPartition`].
        forgotten: Vec<FetchSessionKey>,
    },
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
    ///
    /// The branch taken is decided from `epoch` alone, matching Kafka's
    /// `FetchMetadata.isFull()`. The session id carried alongside a full-fetch
    /// epoch (`INITIAL_EPOCH` or `FINAL_EPOCH`) is not looked up for a match;
    /// it names a session to close before the full read runs, if one exists.
    /// This is the ordinary reconnect path of the Java consumer
    /// (`nextCloseExistingAttemptNew`): it closes its old session by sending
    /// `INITIAL_EPOCH` with the old id, and expects a fresh session back, not
    /// an error.
    pub fn classify(&self, req: &FetchRequest, version: i16) -> SessionDecision {
        let sid = req.session_id;
        let epoch = req.session_epoch;

        if epoch == INITIAL_EPOCH || epoch == FINAL_EPOCH {
            if sid != INVALID_SESSION_ID {
                // Best-effort close: a session that does not exist (already
                // evicted, or named by a client that never had one) is not an
                // error here, exactly as Kafka's `newContext` ignores a
                // `cache.remove` that finds nothing.
                self.close(sid);
            }
            return if epoch == FINAL_EPOCH {
                SessionDecision::Sessionless
            } else {
                SessionDecision::NewSession
            };
        }

        // Not a full fetch: a genuine incremental epoch. Kafka looks the id
        // up unconditionally, `session_id == 0` included — id 0 is never
        // allocated, so an incremental naming it always misses.
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);

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

        // Kafka's `FetchManager.newContext` checks the topic-id use of the
        // session after its epoch, and before it touches the cached set.
        if session.uses_topic_ids != (version >= FIRST_TOPIC_ID_FETCH_VERSION) {
            return SessionDecision::Error {
                code: codes::FETCH_SESSION_TOPIC_ID_ERROR,
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

        if partitions_after == 0 {
            // Kafka's `IncrementalFetchContext.updateAndGenerateResponseData`:
            // an incremental that forgets its last partitions and adds none
            // leaves nothing worth caching. The session is dropped and the
            // response reports no session, so the client falls back to a
            // sessionless fetch next time rather than keep re-polling an
            // empty one.
            guard.sessions.remove(&sid);
            guard.order.remove(sid);
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            return SessionDecision::Incremental {
                session_id: INVALID_SESSION_ID,
                new_epoch: next_epoch(epoch),
                partitions: Vec::new(),
                forgotten: Vec::new(),
            };
        }

        let new_epoch = next_epoch(session.next_epoch);
        session.next_epoch = new_epoch;

        let partitions: Vec<(FetchSessionKey, CachedPartitionState)> = session
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let now = self.clock.now().elapsed_since_origin();
        let forgotten = session.note_forgotten(&req.forgotten_topics_data, now);

        // Kafka's `touch`: the session was used now, and holds the partitions
        // the merge above left it. This is the only place a live session moves
        // in the eviction index. It happens after the `session` borrow above
        // ends, because both halves of the cache live behind the one `guard`.
        guard.order.touch(sid, partitions_after, now);
        drop(guard);

        SessionDecision::Incremental {
            session_id: sid,
            new_epoch,
            partitions,
            forgotten,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::fetch_session::{
            RECENTLY_FORGOTTEN_CAP, RECENTLY_FORGOTTEN_TTL,
            test_support::{
                AllocatedCache, ManualCache, NAME_FETCH_VERSION, RequestSessionId,
                SessionRequestSetup, SessionSlotCount, TICK, TopicAddressing, allocate_session,
                allocated_cache, error_code, forgotten_topic, manual_cache, req,
                seed_resolved_partition, session_key, topic,
            },
        },
        assert2::{assert, check},
        krabka_bdd::{Given, given, then, when},
        krabka_protocol::{
            owned::fetch_request::{FetchPartition, FetchTopic},
            primitives::uuid::Uuid as WireUuid,
        },
    };

    /// How a scenario's fetch was classified: the `SessionDecision` variant,
    /// with an error decision named by its code.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum Classification {
        #[step_param("opening a new session")]
        NewSession,
        Sessionless,
        /// An incremental fetch the session accepted.
        Accepted,
        #[step_param("FETCH_SESSION_ID_NOT_FOUND")]
        FetchSessionIdNotFound,
        #[step_param("INVALID_FETCH_SESSION_EPOCH")]
        InvalidFetchSessionEpoch,
        #[step_param("FETCH_SESSION_TOPIC_ID_ERROR")]
        FetchSessionTopicIdError,
    }

    impl Classification {
        fn of(decision: &SessionDecision) -> Self {
            match decision {
                SessionDecision::NewSession => Self::NewSession,
                SessionDecision::Sessionless => Self::Sessionless,
                SessionDecision::Incremental { .. } => Self::Accepted,
                SessionDecision::Error { code } => match *code {
                    codes::FETCH_SESSION_ID_NOT_FOUND => Self::FetchSessionIdNotFound,
                    codes::INVALID_FETCH_SESSION_EPOCH => Self::InvalidFetchSessionEpoch,
                    codes::FETCH_SESSION_TOPIC_ID_ERROR => Self::FetchSessionTopicIdError,
                    other => panic!("unexpected fetch session error code {other}"),
                },
            }
        }
    }

    /// How the request that opened a session named its topics.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum SessionTopics {
        Names,
        #[step_param("IDs")]
        Ids,
    }

    impl From<SessionTopics> for TopicAddressing {
        fn from(topics: SessionTopics) -> Self {
            match topics {
                SessionTopics::Names => Self::Name,
                SessionTopics::Ids => Self::Id,
            }
        }
    }

    struct FetchClassification {
        cache: FetchSessionCache,
        session_id: Given<FetchSessionId>,
        decision: Given<SessionDecision>,
    }

    #[rstest::fixture]
    fn fetch_classification() -> FetchClassification {
        FetchClassification {
            cache: FetchSessionCache::new(10),
            session_id: Given::new(
                "session_id",
                "Given a fetch request names an existing cached session",
            ),
            decision: Given::new(
                "decision",
                "When the name-based fetch is classified with session epoch <epoch>",
            ),
        }
    }

    #[given("a fetch request names an existing cached session")]
    fn existing_classification_session(fetch_classification: &mut FetchClassification) {
        let id = allocate_session().cache(&fetch_classification.cache).call();
        fetch_classification.session_id.set(id);
    }

    #[given("a fetch request names uncached session id {id}")]
    fn uncached_classification_session(fetch_classification: &mut FetchClassification, id: i32) {
        fetch_classification.session_id.set(id);
    }

    #[given("a fetch session was opened with topic {topics}")]
    fn session_opened_with(fetch_classification: &mut FetchClassification, topics: SessionTopics) {
        let key = session_key()
            .topic_id(WireUuid([1; 16]))
            .partition(0)
            .call();
        let id = allocate_session()
            .cache(&fetch_classification.cache)
            .addressing(topics.into())
            .partitions(vec![(key, CachedPartitionState::default())])
            .call();
        fetch_classification.session_id.set(id);
    }

    fn classify_at(fetch_classification: &mut FetchClassification, epoch: i32, version: i16) {
        let request = req()
            .session_id(RequestSessionId(*fetch_classification.session_id.get()))
            .session_epoch(epoch)
            .call();
        let decision = fetch_classification.cache.classify(&request, version);
        fetch_classification.decision.set(decision);
    }

    #[when("the name-based fetch is classified with session epoch {epoch}")]
    fn classify_supplied_fetch(fetch_classification: &mut FetchClassification, epoch: i32) {
        classify_at(fetch_classification, epoch, NAME_FETCH_VERSION);
    }

    #[when("an incremental fetch at epoch {epoch} is classified at Fetch v{version}")]
    fn classify_incremental_at_version(
        fetch_classification: &mut FetchClassification,
        epoch: i32,
        version: i16,
    ) {
        classify_at(fetch_classification, epoch, version);
    }

    fn check_classification(fetch_classification: &FetchClassification, expected: Classification) {
        assert!(Classification::of(fetch_classification.decision.get()) == expected);
    }

    #[then("the fetch is classified as {classification}")]
    fn classified_fetch(
        fetch_classification: &FetchClassification,
        classification: Classification,
    ) {
        check_classification(fetch_classification, classification);
    }

    #[then("the fetch fails with {error}")]
    fn classified_failed_fetch(fetch_classification: &FetchClassification, error: Classification) {
        check_classification(fetch_classification, error);
    }

    #[then("the incremental fetch is {outcome}")]
    fn classified_incremental_fetch(
        fetch_classification: &FetchClassification,
        outcome: Classification,
    ) {
        check_classification(fetch_classification, outcome);
    }

    #[then("the session next expects epoch {epoch}")]
    fn session_next_epoch(fetch_classification: &FetchClassification, epoch: i32) {
        let id = fetch_classification.session_id.get();
        let next = fetch_classification
            .cache
            .inner
            .lock()
            .expect("poisoned")
            .sessions[id]
            .next_epoch;
        assert!(next == epoch);
    }

    #[then("the fetch session cache contains no sessions")]
    fn empty_classification_cache(fetch_classification: &FetchClassification) {
        assert!(fetch_classification.cache.len() == 0);
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/fetch_session/classify.feature",
        fixtures = [fetch_classification: FetchClassification]
    );

    #[test]
    fn close_request_closes_the_session_inline() {
        let AllocatedCache { cache, id } = allocated_cache().call();
        let r = req()
            .session_id(RequestSessionId(id))
            .session_epoch(FINAL_EPOCH)
            .call();
        assert!(matches!(
            cache.classify(&r, NAME_FETCH_VERSION),
            SessionDecision::Sessionless
        ));
        // classify() already dropped the session; nothing left to close.
        assert!(cache.len() == 0);
        // Subsequent classify with the same id is now NOT_FOUND.
        let r2 = req()
            .session_id(RequestSessionId(id))
            .session_epoch(1)
            .call();
        assert!(error_code(&cache, &r2, NAME_FETCH_VERSION) == codes::FETCH_SESSION_ID_NOT_FOUND);
    }

    #[test]
    fn incremental_merges_request_topics_and_bumps_epoch() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![(session_key().partition(0).call(), CachedPartitionState {
            fetch_offset: 100,
            max_bytes: 1024,
            ..Default::default()
        })];
        let id = allocate_session().cache(&cache).partitions(initial).call();

        // Incremental that updates partition 0's fetch_offset and adds partition 1.
        let r = req()
            .session_id(SessionRequestSetup::incremental(RequestSessionId(id)).session_id)
            .session_epoch(SessionRequestSetup::incremental(RequestSessionId(id)).session_epoch)
            .topics(vec![topic("t", &[
                krabka_ids::PartitionIndex(0),
                krabka_ids::PartitionIndex(1),
            ])])
            .forgotten(SessionRequestSetup::incremental(RequestSessionId(id)).forgotten)
            .call();
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental {
                session_id,
                new_epoch,
                partitions,
                forgotten,
            } => {
                check!(session_id == id);
                check!(new_epoch == 2);
                check!(partitions.len() == 2);
                check!(forgotten.is_empty());
            }
            other => panic!("expected Incremental, got {other:?}"),
        }

        // Re-sending with the old epoch fails — broker advanced to 2.
        let r2 = req()
            .session_id(RequestSessionId(id))
            .session_epoch(1)
            .call();
        assert!(error_code(&cache, &r2, NAME_FETCH_VERSION) == codes::INVALID_FETCH_SESSION_EPOCH);
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct FetchByteLimit(i32);

    #[bon::builder]
    fn incremental_topic(
        #[builder(default = "t" . into ())] name: String,
        #[builder(default)] topic_id: WireUuid,
        #[builder(into, default)] fetch_offset: krabka_ids::Offset,
        #[builder(into, default = FetchByteLimit (1024))] partition_max_bytes: FetchByteLimit,
    ) -> FetchTopic {
        FetchTopic {
            topic: name,
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: fetch_offset.0,
                partition_max_bytes: partition_max_bytes.0,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn incremental_partitions(
        decision: SessionDecision,
    ) -> Vec<(FetchSessionKey, CachedPartitionState)> {
        let SessionDecision::Incremental { partitions, .. } = decision else {
            panic!("expected Incremental");
        };
        partitions
    }

    fn expected_incremental_state(
        fetch_offset: krabka_ids::Offset,
        max_bytes: FetchByteLimit,
    ) -> CachedPartitionState {
        CachedPartitionState {
            fetch_offset: fetch_offset.0,
            last_fetched_epoch: -1,
            current_leader_epoch: -1,
            max_bytes: max_bytes.0,
            log_start_offset: -1,
            last_high_watermark: 0,
            last_last_stable_offset: 0,
            last_log_start_offset: 0,
            last_preferred_read_replica: 0,
            last_aborted_txns_hash: 0,
            last_error_code: 0,
        }
    }

    /// Incrementals retain the cache's resolved topic identity.
    fn resolved_incremental_partition(
        topic_id: WireUuid,
        state: CachedPartitionState,
    ) -> Vec<(FetchSessionKey, CachedPartitionState)> {
        vec![(session_key().topic_id(topic_id).partition(0).call(), state)]
    }

    fn check_resolved_incremental(
        cache: &FetchSessionCache,
        request: &FetchRequest,
        topic_id: WireUuid,
        expected: CachedPartitionState,
    ) {
        let partitions = incremental_partitions(cache.classify(request, NAME_FETCH_VERSION));
        assert!(partitions == resolved_incremental_partition(topic_id, expected));
    }

    /// The cache key is fully resolved, while an incremental may carry only
    /// the topic name (legacy clients) or only the topic id (v13+ clients).
    #[yare::parameterized(
        topic_id_only = { String::new(), WireUuid([9u8; 16]), 42, 2048 },
        topic_name_only = { "t".to_owned(), WireUuid::ZERO, 99, 4096 },
    )]
    fn incremental_merge_matches_cached_key_by_either_identity(
        name: String,
        topic_id: WireUuid,
        fetch_offset: i64,
        partition_max_bytes: i32,
    ) {
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([9u8; 16]);
        let id = seed_resolved_partition(&cache, tid);

        let r = req()
            .session_id(id)
            .session_epoch(1)
            .topics(vec![
                incremental_topic()
                    .name(name)
                    .topic_id(topic_id)
                    .fetch_offset(fetch_offset)
                    .partition_max_bytes(partition_max_bytes)
                    .call(),
            ])
            .call();

        // No duplicate entry is created; the cached, fully resolved key is
        // preserved and its requested state is updated in place.
        check_resolved_incremental(
            &cache,
            &r,
            tid,
            expected_incremental_state(
                krabka_ids::Offset(fetch_offset),
                FetchByteLimit(partition_max_bytes),
            ),
        );
    }

    #[derive(Clone, Copy)]
    enum ForgottenPartitionsCase {
        RemoveOne,
        /// Forgetting every partition drops the session ID to zero (#871).
        RemoveAll,
    }

    #[yare::parameterized(
        remove_one = { ForgottenPartitionsCase::RemoveOne },
        remove_all = { ForgottenPartitionsCase::RemoveAll },
    )]
    fn forgetting_partitions_updates_the_cached_session(case: ForgottenPartitionsCase) {
        let cache = FetchSessionCache::new(10);
        let (initial, forgotten, expected_partitions) = match case {
            ForgottenPartitionsCase::RemoveOne => (
                vec![
                    (
                        session_key().partition(0).call(),
                        CachedPartitionState::default(),
                    ),
                    (
                        session_key().partition(1).call(),
                        CachedPartitionState::default(),
                    ),
                    (
                        session_key().partition(2).call(),
                        CachedPartitionState::default(),
                    ),
                ],
                forgotten_topic()
                    .partitions(vec![krabka_ids::PartitionIndex(1)])
                    .call(),
                vec![0, 2],
            ),
            ForgottenPartitionsCase::RemoveAll => (
                vec![(session_key().call(), CachedPartitionState::default())],
                forgotten_topic().call(),
                vec![],
            ),
        };
        let id = allocate_session().cache(&cache).partitions(initial).call();
        let r = req()
            .session_id(id)
            .session_epoch(1)
            .forgotten(vec![forgotten])
            .call();
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental {
                session_id,
                partitions,
                ..
            } => {
                let mut parts: Vec<i32> = partitions.iter().map(|(k, _)| k.partition).collect();
                parts.sort_unstable();
                assert2::assert!(parts == expected_partitions);
                let expected_id = if matches!(case, ForgottenPartitionsCase::RemoveAll) {
                    0
                } else {
                    id
                };
                check!(session_id == expected_id);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
        if matches!(case, ForgottenPartitionsCase::RemoveAll) {
            assert2::assert!(cache.len() == 0);
            assert2::assert!(cache.total_partitions_cached() == 0);
        }
    }

    /// A partition an incremental fetch forgets stays with the session, for
    /// the read loop to wake a parked fetch on, until the client names it
    /// again or `RECENTLY_FORGOTTEN_TTL` passes.
    #[test]
    fn a_forgotten_partition_is_remembered_until_named_again_or_expired() {
        let ManualCache { cache, clock } = manual_cache(SessionSlotCount(10));
        let initial = vec![
            (
                session_key().partition(0).call(),
                CachedPartitionState::default(),
            ),
            (
                session_key().partition(1).call(),
                CachedPartitionState::default(),
            ),
        ];
        let id = allocate_session().cache(&cache).partitions(initial).call();
        let forgets_one = |epoch| {
            req()
                .session_id(id)
                .session_epoch(epoch)
                .forgotten(vec![
                    forgotten_topic()
                        .partitions(vec![krabka_ids::PartitionIndex(1)])
                        .call(),
                ])
                .call()
        };
        let names_nothing = |epoch| req().session_id(id).session_epoch(epoch).call();
        let forgotten_of = |r: &FetchRequest| match cache.classify(r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental { forgotten, .. } => forgotten,
            other => panic!("expected Incremental, got {other:?}"),
        };

        assert!(forgotten_of(&forgets_one(1)) == vec![session_key().partition(1).call()]);
        // A later fetch that says nothing about the partition still remembers it.
        assert!(forgotten_of(&names_nothing(2)).len() == 1);
        // Naming it again ends the memory.
        let names_it_again = req()
            .session_id(id)
            .session_epoch(3)
            .topics(vec![topic("t", &[krabka_ids::PartitionIndex(1)])])
            .call();
        assert!(forgotten_of(&names_it_again).is_empty());
        // Forgotten once more, then left alone past the TTL.
        assert!(forgotten_of(&forgets_one(4)).len() == 1);
        clock
            .advance(RECENTLY_FORGOTTEN_TTL + TICK)
            .expect("advance the manual clock");
        assert!(forgotten_of(&names_nothing(5)).is_empty());
    }

    /// The memory is bounded: a fetch that forgets more partitions than the
    /// cap keeps the newest ones.
    #[test]
    fn a_session_remembers_at_most_the_newest_forgotten_partitions() {
        let cache = FetchSessionCache::new(10);
        let cap = i32::try_from(RECENTLY_FORGOTTEN_CAP).unwrap();
        let initial: Vec<_> = (0..=cap + 1)
            .map(|p| {
                (
                    session_key().partition(p).call(),
                    CachedPartitionState::default(),
                )
            })
            .collect();
        let id = allocate_session().cache(&cache).partitions(initial).call();
        // Forget every partition but the last, so the session stays open.
        let r = req()
            .session_id(id)
            .session_epoch(1)
            .forgotten(vec![
                forgotten_topic()
                    .partitions((0..=cap).map(krabka_ids::PartitionIndex).collect())
                    .call(),
            ])
            .call();
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental { forgotten, .. } => {
                let kept: Vec<i32> = forgotten.iter().map(|k| k.partition).collect();
                assert!(kept == (1..=cap).collect::<Vec<_>>());
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }
}
