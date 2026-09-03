//! The observer's serve loop: it picks a controller voter round robin, fetches
//! from it, records the applied offset and the leader hint, and parks on the
//! poll interval whenever it is caught up or the voter is unreachable.
//!
//! The loop starts from what this node last checkpointed rather than from
//! offset 0, and checkpoints again as it advances, so a restart does not replay
//! the whole metadata log — and does not ask a pruned controller for records it
//! no longer holds.

use std::sync::{Arc, atomic::Ordering};

use krabka_raft::NodeId;
use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use super::{MetadataObserver, ObserverConfig, fetch::fetch_once, store::ObserverStore};
use crate::time_util;

/// Names this loop in the timer-failure logs that [`time_util::arm`] and
/// [`time_util::fired`] emit.
const TASK: &str = "metadata observer";

/// Parks for the configured poll interval on the injected timer.
///
/// Reports whether the timer held: `false` means the observer's only pacing
/// mechanism is gone, and the loop must stop rather than spin at full speed
/// through a poll that no longer waits.
async fn park(config: &ObserverConfig) -> bool {
    let Some(tick) = time_util::arm(&*config.timer, config.poll_interval.to_std(), TASK) else {
        return false;
    };
    time_util::fired(tick.await, TASK)
}

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

/// Successive fetches that leave the image empty while the quorum has
/// committed records before the loop warns about it. One such fetch is
/// ordinary — a node that has just started has an empty image and no records
/// yet; a second one means no progress is being made at all.
const EMPTY_IMAGE_POLLS_BEFORE_WARNING: u32 = 2;

/// Watches for the stall where every fetch is answered and nothing is ever
/// applied: the quorum has committed records, and this node's image is still
/// empty a poll interval later.
///
/// Left silent, that state is only visible as a readiness lag the node can
/// never close. The warning is raised once per stall and re-armed when the
/// observer starts making progress again, so a node that is merely slow to see
/// its first record does not warn, and a node that is genuinely stuck does not
/// repeat itself on every poll.
#[derive(Default)]
struct EmptyImageWatch {
    polls: u32,
    warned: bool,
}

impl EmptyImageWatch {
    /// Record one answered fetch. Reports whether this is the moment to warn.
    fn observe(&mut self, image_is_empty: bool, quorum_high_watermark: i64) -> bool {
        if !image_is_empty || quorum_high_watermark <= 0 {
            *self = Self::default();
            return false;
        }
        self.polls = self.polls.saturating_add(1);
        let warn_now = self.polls >= EMPTY_IMAGE_POLLS_BEFORE_WARNING && !self.warned;
        self.warned |= warn_now;
        warn_now
    }
}

/// Restore the image this node last checkpointed and report the offset to
/// fetch from next. A node with no checkpoint starts at the log start.
fn resume(config: &ObserverConfig, store: &mut ObserverStore, observer: &MetadataObserver) -> u64 {
    let Some((image, fetch_offset)) = store.resume(config.cluster_id) else {
        return 0;
    };
    let _ = observer.image.send_replace(Arc::new(image));
    // The offset convention is one-past-the-last, so the last applied record
    // sits one below the offset the next fetch asks for.
    observer.metadata_offset.store(
        i64::try_from(fetch_offset).unwrap_or(i64::MAX) - 1,
        Ordering::Release,
    );
    fetch_offset
}

pub(super) async fn run_loop(
    config: ObserverConfig,
    observer: Arc<MetadataObserver>,
    shutdown: CancellationToken,
) {
    let mut store = ObserverStore::open(&config.data_dir, config.snapshot_interval_records);
    let mut fetch_offset: u64 = resume(&config, &mut store, &observer);
    let mut target_idx: usize = 0;
    let mut empty_image = EmptyImageWatch::default();
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if config.voters.is_empty() {
            if !park(&config).await {
                return;
            }
            continue;
        }
        let (target, addr) = voter_at(&config.voters, target_idx).clone();
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, &addr, target, fetch_offset, &observer.image, &mut store) => r,
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
            // Every poll is answered and nothing is ever applied: the stall
            // shows up as a readiness lag this node can never close, with
            // nothing saying why. The responder's log start is what separates
            // "still catching up" from "asking for records that were pruned
            // away".
            if empty_image.observe(
                observer.metadata_offset.load(Ordering::Acquire) < 0,
                outcome.quorum_high_watermark,
            ) {
                tracing::warn!(
                    fetch_offset,
                    log_start_offset = outcome.log_start_offset,
                    quorum_high_watermark = outcome.quorum_high_watermark,
                    voter = target.0,
                    "observer metadata image is still empty while the quorum has committed \
                     records; this node is not making progress"
                );
            }
            if new_offset == fetch_offset {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    held = park(&config) => if !held { return; },
                }
            } else {
                fetch_offset = new_offset;
                store.maybe_checkpoint(&observer.current_image(), fetch_offset);
            }
        } else {
            target_idx = target_idx.wrapping_add(1);
            tokio::select! {
                () = shutdown.cancelled() => return,
                held = park(&config) => if !held { return; },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use assert2::assert;
    use bytes::Bytes;
    use krabka_protocol::owned::api_versions_request;
    use krabka_raft::OutboundDialer;
    use krabka_units::millis;
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer};
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata_observer::test_support::{api_versions_response_v0, observer_config},
        test_support::{BrokenTimer, TimerFailure},
    };

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
            snapshot_id: None,
            records,
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    /// The stall warning fires once, after more than one answered fetch has
    /// left the image empty, and re-arms only after the observer has made
    /// progress. A node that has simply not seen its first record yet, or one
    /// whose quorum has committed nothing, must stay silent.
    #[test]
    fn the_empty_image_warning_fires_once_per_stall_and_re_arms_after_progress() {
        let mut watch = EmptyImageWatch::default();

        // A quorum with nothing committed says nothing about this node.
        assert!(!watch.observe(true, 0));
        // The first answered fetch on a fresh node is ordinary.
        assert!(!watch.observe(true, 10_000));
        // The second one is the stall, and it is reported exactly once.
        assert!(watch.observe(true, 10_000));
        assert!(!watch.observe(true, 10_000));

        // Progress clears it, and a later stall is reported again.
        assert!(!watch.observe(false, 10_000));
        assert!(!watch.observe(true, 10_000));
        assert!(watch.observe(true, 10_000));
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
        let clock = ManualMonotonicClock::new_shared();
        let dir = tempfile::tempdir().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            dialer: Arc::new(CountingDialer {
                dial_count: dial_count.clone(),
            }),
            client_id: "sleep-test".into(),
            poll_interval: millis(250),
            timer: clock.new_timer(),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
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
        // on a timer armed for `poll_interval`. That park is the loop's only
        // registration on this clock, so confirming one waiter confirms the
        // park (blocking thread — never stalls the current-thread runtime that
        // drives the observer to its park). Parked on a manual timeline that we
        // never advance, the observer cannot re-fetch, so the counts are
        // deterministically frozen at their first-fetch values.
        let waiters = clock.clone();
        let parked = tokio::task::spawn_blocking(move || {
            waiters.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "observer should park on the poll-interval timer after an empty fetch",
        );

        assert!(fetches.load(Ordering::SeqCst) == after_first_fetch);
        assert!(dial_count.load(Ordering::SeqCst) == after_first_fetch);

        observer.cancel().await;
        mock.stop();
    }

    /// Runs the serve loop over `config` until it returns on its own, and
    /// hands back the observer it wrote to.
    ///
    /// The shutdown token is never cancelled, so a loop that returns here
    /// returned because it gave up on its timer. A hang-guard keeps a loop
    /// that does not give up from wedging the whole suite.
    async fn run_until_it_stops(config: ObserverConfig) -> Arc<MetadataObserver> {
        let shutdown = CancellationToken::new();
        let observer = MetadataObserver::new(config.cluster_id, shutdown.clone());
        tokio::time::timeout(
            Duration::from_secs(10),
            run_loop(config, Arc::clone(&observer), shutdown),
        )
        .await
        .expect("the serve loop stops once its timer is gone");
        observer
    }

    /// An `ObserverConfig` over `voters`, driven by `timer` and storing its
    /// checkpoints under `data_dir`.
    fn config_on(
        voters: Vec<(NodeId, String)>,
        timer: Arc<dyn Timer>,
        data_dir: &std::path::Path,
    ) -> ObserverConfig {
        ObserverConfig {
            voters,
            client_id: "dead-timer-test".into(),
            poll_interval: millis(250),
            timer,
            ..observer_config(Uuid::nil(), data_dir.to_path_buf())
        }
    }

    /// The loopback port nothing listens on, so a dial fails at once rather
    /// than on a connect timeout.
    const UNREACHABLE: &str = "127.0.0.1:1";

    /// A restarted observer comes up on what it last checkpointed rather than on
    /// an empty image at offset 0.
    ///
    /// The loop publishes the restored image and its applied offset before it
    /// contacts anybody, so a node that was caught up before the restart serves
    /// metadata immediately instead of replaying the log — or, once the
    /// controller has pruned, instead of asking for records that are gone.
    #[tokio::test]
    async fn the_loop_resumes_from_the_checkpoint_this_node_last_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = krabka_metadata::MetadataImage::new(Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "survives-the-restart".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            },
        ));
        ObserverStore::open(dir.path(), 1).maybe_checkpoint(&image, 12);

        // No voters and a dead timer, so the loop resumes and then stops
        // without ever reaching a controller: what it holds came off disk.
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(
            observer
                .current_image()
                .topic("survives-the-restart")
                .is_some()
        );
        // The checkpoint covers records below offset 12, so the last applied
        // one is 11 — the offset this node reports in its `BrokerHeartbeat`.
        assert!(observer.current_metadata_offset() == 11);
    }

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_cannot_be_armed() {
        // No voters: the loop's only move is to park, and the park cannot be
        // armed, so it stops instead of spinning through a poll that no longer
        // waits.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(observer.watch_leader().borrow().is_none());
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_is_armed_but_never_completes() {
        // The other half of the park: the deadline registers and then fails,
        // which ends the loop the same way a refused registration does.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Completion);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_the_back_off_after_an_unreachable_voter_cannot_be_armed() {
        // The fetch fails, so the loop rotates to the next voter and backs off
        // — and the back-off is the park that cannot be armed. It leaves no
        // leader hint, because no voter answered.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), UNREACHABLE.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(observer.watch_leader().borrow().is_none());
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_the_caught_up_park_cannot_be_armed() {
        // A fetch that returns no records leaves the observer caught up, which
        // is the third place the loop parks. The record of that answered fetch
        // — the applied offset and the leader hint — survives the shutdown,
        // which is what separates this exit from the unreachable-voter one.
        let mock =
            krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    return Some(metadata_fetch_response_body(Bytes::new(), 0, 0));
                }
                None
            })
            .await;
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), mock.addr.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(*observer.watch_leader().borrow() == Some(NodeId(1)));
        assert!(timer.registrations() == 1);
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
        let dir = tempfile::tempdir().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            client_id: "lag-test".into(),
            poll_interval: millis(250),
            timer: ManualMonotonicClock::new_shared().new_timer(),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
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
