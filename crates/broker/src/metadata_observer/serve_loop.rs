//! The observer's serve loop: it picks a controller voter round robin, fetches
//! from it, records the applied offset and the leader hint, and parks on the
//! poll interval whenever it is caught up or the voter is unreachable.

use std::sync::{Arc, atomic::Ordering};

use krabka_raft::NodeId;
use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use super::{MetadataObserver, ObserverConfig, fetch::fetch_once};

/// Round-robin pick into a non-empty voter list: the index `idx` wrapped by
/// the list length.
///
/// This helper is separate from the serve loop so that a unit test covers the
/// wrap-around. A `/` written for `%` would stop the observer from rotating to
/// the next voter when the current one is unreachable, and strand it on a dead
/// voter.
fn voter_at(voters: &[(NodeId, String)], idx: usize) -> &(NodeId, String) {
    &voters[idx % voters.len()]
}

pub(super) async fn run_loop(
    config: ObserverConfig,
    observer: Arc<MetadataObserver>,
    shutdown: CancellationToken,
) {
    let mut fetch_offset: u64 = 0;
    let mut target_idx: usize = 0;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if config.voters.is_empty() {
            config
                .sleeper
                .sleep_for_async(config.poll_interval.to_std())
                .await;
            continue;
        }
        let (target, addr) = voter_at(&config.voters, target_idx).clone();
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, &addr, target, fetch_offset, &observer.image) => r,
        };
        if let Some(outcome) = result {
            let new_offset = outcome.next_fetch_offset;
            observer.metadata_offset.store(
                i64::try_from(new_offset).unwrap_or(i64::MAX) - 1,
                Ordering::Release,
            );
            // The quorum's high watermark counts committed *records*; the
            // offset convention here is one-past-the-last, so the last
            // committed offset is one below it, matching `metadata_offset`.
            // It is the quorum's and not the responder's: the voter this loop
            // picked may itself be a follower that is behind.
            observer.quorum_committed_offset.store(
                outcome.quorum_high_watermark.saturating_sub(1),
                Ordering::Release,
            );
            let _ = observer.leader.send_replace(Some(target));
            if new_offset == fetch_offset {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = config.sleeper.sleep_for_async(config.poll_interval.to_std()) => {}
                }
            } else {
                fetch_offset = new_offset;
            }
        } else {
            target_idx = target_idx.wrapping_add(1);
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = config.sleeper.sleep_for_async(config.poll_interval.to_std()) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };
    use krabka_raft::OutboundDialer;
    use krabka_units::millis;
    use qubit_clock::{MockWaiterKind, sleep::MockSleeper};
    use uuid::Uuid;

    use super::*;
    use crate::metadata_observer::test_support::TEST_MAX_FETCH_BYTES;

    /// A write side for an `ObserverSource` built only to read offsets
    /// through: readiness never submits anything.
    struct NoWrites;

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataWriter for NoWrites {
        async fn submit_change(
            &self,
            _records: Vec<krabka_metadata::MetadataRecord>,
        ) -> Result<krabka_raft::SubmitChangeResult, krabka_raft::RaftError> {
            unreachable!("readiness reads offsets, it never writes")
        }
    }

    #[derive(Clone)]
    struct CountingDialer {
        dial_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for CountingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: krabka_client_core::ConnectionOptions,
        ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
            self.dial_count.fetch_add(1, Ordering::SeqCst);
            krabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    fn api_versions_response_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// `high_watermark` is what the responder itself has committed and
    /// `quorum_high_watermark` is what the quorum has: they differ whenever
    /// the controller that answered is a follower still catching up.
    fn metadata_fetch_response_body(
        records: Bytes,
        high_watermark: i64,
        quorum_high_watermark: i64,
    ) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        krabka_raft::KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 1,
            log_start_offset: 0,
            high_watermark,
            quorum_high_watermark,
            records,
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    #[test]
    fn voter_at_wraps_round_robin_by_modulo() {
        let voters = vec![
            (krabka_raft::NodeId(1), "a:9093".to_string()),
            (krabka_raft::NodeId(2), "b:9093".to_string()),
            (krabka_raft::NodeId(3), "c:9093".to_string()),
        ];
        // In-range picks each distinct voter. `idx / len` (the `%`→`/` mutant)
        // would collapse 1 and 2 to index 0 ("a"), so distinguishing 0/1/2 here
        // proves the modulo, not integer division, indexes the list.
        // Wrap-around: index 3 must rotate back to the first voter (3 % 3 == 0);
        // `3 / 3 == 1` would return the second voter instead.
        let cases = [
            (0usize, krabka_raft::NodeId(1)),
            (1, krabka_raft::NodeId(2)),
            (2, krabka_raft::NodeId(3)),
            (3, krabka_raft::NodeId(1)),
            (4, krabka_raft::NodeId(2)),
        ];
        for (idx, expected_id) in cases {
            assert!(voter_at(&voters, idx).0 == expected_id, "idx {idx}");
        }
    }

    #[tokio::test]
    async fn run_loop_sleeps_after_empty_fetch() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_for_mock = fetches.clone();
        let mock =
            krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    fetches_for_mock.fetch_add(1, Ordering::SeqCst);
                    return Some(metadata_fetch_response_body(Bytes::new(), 0, 0));
                }
                None
            })
            .await;
        let dial_count = Arc::new(AtomicUsize::new(0));
        let sleeper = MockSleeper::new();
        let timeline = sleeper.timeline();
        let observer = MetadataObserver::start(ObserverConfig {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            dialer: Arc::new(CountingDialer {
                dial_count: dial_count.clone(),
            }),
            client_id: "sleep-test".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: millis(250),
            sleeper: Arc::new(sleeper),
        });

        // Await (not sleep) for the first fetch to land. The fetch is real
        // loopback network I/O through the mock broker, which is not time-gated
        // — drive the executor with `yield_now` until the counter moves.
        let mut saw_first_fetch = false;
        for _ in 0..100_000 {
            if fetches.load(Ordering::SeqCst) >= 1 {
                saw_first_fetch = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(saw_first_fetch, "observer should issue the first fetch");
        let after_first_fetch = fetches.load(Ordering::SeqCst);

        // The empty fetch left the observer caught up, so it must now be parked
        // on `sleep_for_async(poll_interval)`. Confirm the sleep waiter is
        // registered (blocking thread — never stalls the current-thread runtime
        // that drives the observer to its park). Parked on a mock timeline that
        // we never advance, the observer cannot re-fetch, so the counts are
        // deterministically frozen at their first-fetch values.
        let tl = timeline.clone();
        let parked = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "observer should park on the poll-interval sleep after an empty fetch",
        );

        assert!(fetches.load(Ordering::SeqCst) == after_first_fetch);
        assert!(dial_count.load(Ordering::SeqCst) == after_first_fetch);

        observer.cancel().await;
        mock.stop();
    }

    /// The observer records the *quorum's* committed offset, not the watermark
    /// of whichever controller answered.
    ///
    /// Every controller serves `MetadataFetch`, and the loop keeps whichever
    /// voter responds, so the responder can be a follower whose own watermark
    /// is clamped to a log end far below what the quorum has committed. An
    /// observer that read that value would draw level with the lagging
    /// follower and report zero readiness lag against a quorum thousands of
    /// records ahead.
    #[tokio::test]
    async fn the_quorums_committed_offset_is_read_past_a_lagging_responder() {
        let mock =
            krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    // A follower holding 5 of the quorum's 10 000 records.
                    return Some(metadata_fetch_response_body(Bytes::new(), 5, 10_000));
                }
                None
            })
            .await;
        let observer = MetadataObserver::start(ObserverConfig {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            dialer: Arc::new(krabka_raft::PlaintextDialer),
            client_id: "lag-test".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: millis(250),
            sleeper: Arc::new(MockSleeper::new()),
        });

        // The fetch is real loopback I/O, so drive the executor until the
        // first successful round trip has published its offsets.
        let mut recorded = -1;
        for _ in 0..100_000 {
            recorded = observer.quorum_committed_offset();
            if recorded != -1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // 10 000 committed records is offsets 0..=9_999.
        assert!(recorded == 9_999);
        assert!(observer.current_metadata_offset() == -1);

        // The readiness probe reads these two offsets through
        // `health::metadata_progress`, and this is the only fixture where they
        // differ -- so it is where the mapping is pinned. Swapped, or both
        // taken from one side, a node 10 000 records behind would report no
        // lag at all and `/readyz` would answer 200 for it.
        let source = Arc::new(crate::metadata_source::ObserverSource::new(
            observer.clone(),
            Arc::new(NoWrites),
        ));
        let progress = crate::health::metadata_progress(source);
        assert!(progress.node_metadata_offset() == -1);
        assert!(progress.quorum_committed_offset() == 9_999);

        observer.cancel().await;
        mock.stop();
    }
}
