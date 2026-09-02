//! The observer's serve loop: it picks a controller voter round robin, fetches
//! from it, records the applied offset and the leader hint, and parks on the
//! poll interval whenever it is caught up or the voter is unreachable.

use std::sync::{Arc, atomic::Ordering};

use krabka_raft::NodeId;
use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use super::{MetadataObserver, ObserverConfig, fetch::fetch_once};
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
            if !park(&config).await {
                return;
            }
            continue;
        }
        let (target, addr) = voter_at(&config.voters, target_idx).clone();
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, &addr, target, fetch_offset, &observer.image) => r,
        };
        if let Some(new_offset) = result {
            observer.metadata_offset.store(
                i64::try_from(new_offset).unwrap_or(i64::MAX) - 1,
                Ordering::Release,
            );
            let _ = observer.leader.send_replace(Some(target));
            if new_offset == fetch_offset {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    held = park(&config) => if !held { return; },
                }
            } else {
                fetch_offset = new_offset;
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
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer};
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata_observer::test_support::TEST_MAX_FETCH_BYTES,
        test_support::{BrokenTimer, TimerFailure},
    };

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

    fn metadata_fetch_response_body(records: Bytes) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        krabka_raft::KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 1,
            log_start_offset: 0,
            high_watermark: 0,
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
                    return Some(metadata_fetch_response_body(Bytes::new()));
                }
                None
            })
            .await;
        let dial_count = Arc::new(AtomicUsize::new(0));
        let clock = ManualMonotonicClock::new_shared();
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
            timer: clock.new_timer(),
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

    /// An `ObserverConfig` over `voters`, driven by `timer`.
    fn config_on(voters: Vec<(NodeId, String)>, timer: Arc<dyn Timer>) -> ObserverConfig {
        ObserverConfig {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            voters,
            dialer: Arc::new(krabka_raft::PlaintextDialer),
            client_id: "dead-timer-test".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: millis(250),
            timer,
        }
    }

    /// The loopback port nothing listens on, so a dial fails at once rather
    /// than on a connect timeout.
    const UNREACHABLE: &str = "127.0.0.1:1";

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_cannot_be_armed() {
        // No voters: the loop's only move is to park, and the park cannot be
        // armed, so it stops instead of spinning through a poll that no longer
        // waits.
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(observer.watch_leader().borrow().is_none());
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_is_armed_but_never_completes() {
        // The other half of the park: the deadline registers and then fails,
        // which ends the loop the same way a refused registration does.
        let timer = BrokenTimer::dead(TimerFailure::Completion);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_the_back_off_after_an_unreachable_voter_cannot_be_armed() {
        // The fetch fails, so the loop rotates to the next voter and backs off
        // — and the back-off is the park that cannot be armed. It leaves no
        // leader hint, because no voter answered.
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), UNREACHABLE.to_string())],
            timer.injectable(),
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
                    return Some(metadata_fetch_response_body(Bytes::new()));
                }
                None
            })
            .await;
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), mock.addr.to_string())],
            timer.injectable(),
        ))
        .await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(*observer.watch_leader().borrow() == Some(NodeId(1)));
        assert!(timer.registrations() == 1);
        mock.stop();
    }
}
