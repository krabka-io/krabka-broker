//! KIP-124 `request_percentage` helper.
//!
//! The request quota throttles on the server-side time a request takes, as a
//! percentage of one request-handler thread. The per-connection dispatch loop
//! calls this helper for most APIs. The `Produce`, `Fetch` and `ShareFetch`
//! handlers call it inline, so that the broker can combine the request
//! throttle with the byte-rate data throttle, with `max`, into one
//! `throttle_time_ms` and one channel mute (KIP-219).

use {
    super::{QuotaConsumption, QuotaType, buckets::QuotaBuckets, record_client_quota},
    krabka_metadata::MetadataImage,
    krabka_units::Time,
    std::{
        future,
        future::Future,
        time::{Duration, Instant},
    },
};

/// Records `elapsed_micros` of request-handler time on the
/// `request_percentage` quota for `(principal, client_id)`.
///
/// It returns the throttle delay to apply before the broker sends the
/// response. The delay is a zero extent when no quota is configured, when the
/// rate is not positive, or when the measured rate is within the quota.
/// `maximum_delay`, one `quota.window.size.seconds` in Kafka, caps the
/// returned delay, as Kafka's `ClientRequestQuotaManager` takes
/// `QuotaUtils.boundedThrottleTime`, the only client quota it bounds. The
/// samples expire on their own, so a client that stops overrunning is not
/// throttled past the window.
///
/// `request_percentage` is a percentage of one thread-second, so `100.0` is a
/// bound of 1 000 000 µs per second. The quota therefore records
/// microseconds, the same unit as `elapsed_micros`. Kafka records the
/// percentage itself, and the throttle, a ratio of the measured rate to the
/// bound, is the same in either unit.
#[must_use]
pub fn consume_request_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    elapsed_micros: u64,
    maximum_delay: Time,
) -> super::QuotaDelay {
    let mut recorded = record_client_quota(QuotaConsumption {
        image,
        buckets,
        principal,
        client_id,
        quota_type: QuotaType::Request,
        amount: elapsed_micros,
    })
    .delay;
    recorded.delay = recorded.delay.min(maximum_delay);
    recorded
}

/// The bound of a `request_percentage` quota: microseconds of handler time
/// per second, so `100.0` is 1 000 000.
pub(super) fn request_percentage_bound(rate_pct: f64) -> f64 { rate_pct * 10_000.0 }

/// The output of a future, and the time spent inside its `poll` calls.
pub struct Timed<T> {
    pub(crate) output: T,
    pub(crate) active: Duration,
}

/// Awaits `future` and reports, beside its output, the time spent inside its
/// `poll` calls. The time the future is parked between polls is not counted.
///
/// This is the handler time KIP-124 charges to `request_percentage`: Kafka
/// meters the time a request holds a handler thread, so a handler parked on a
/// timer, a long poll, a channel or a raft commit is not charged for the wait.
pub async fn with_active_time<F: Future>(future: F) -> Timed<F::Output> {
    let mut future = std::pin::pin!(future);
    let mut active = Duration::ZERO;
    let output = future::poll_fn(|cx| {
        let polled_at = Instant::now();
        let poll = future.as_mut().poll(cx);
        active += polled_at.elapsed();
        poll
    })
    .await;
    Timed { output, active }
}

/// The handler time KIP-124 charges, for a handler whose every await goes
/// through [`Self::run`]: the time since [`Self::start`], less the time those
/// awaits spent parked between polls. A long poll's wait is therefore not
/// charged, as Kafka parks such a request off the handler thread.
pub struct ActiveClock {
    started: Instant,
    parked: Duration,
}

impl ActiveClock {
    pub(crate) fn start() -> Self {
        Self {
            started: Instant::now(),
            parked: Duration::ZERO,
        }
    }

    /// Awaits `future`, and counts the time it spent parked.
    pub(crate) async fn run<F: Future>(&mut self, future: F) -> F::Output {
        let awaited_at = Instant::now();
        let Timed { output, active } = with_active_time(future).await;
        self.parked += awaited_at.elapsed().saturating_sub(active);
        output
    }

    /// The handler time so far, parked time excluded.
    pub(crate) fn active(&self) -> Duration { self.started.elapsed().saturating_sub(self.parked) }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::quota::test_support::{
            QuotaEntityInput, image_with_quotas, image_with_rate, manual_buckets, quota_record,
        },
        assert2::{assert, check},
        krabka_units::{convert::TimeExt, millis, secs},
        std::{thread, time::Duration},
        tokio::time,
    };

    fn img_with_quota(
        entity: Vec<QuotaEntityInput<'_>>,
        rate: f64,
    ) -> krabka_metadata::MetadataImage {
        image_with_rate(entity, "request_percentage", rate)
    }

    fn check_request_delay(image: &MetadataImage, elapsed: u64, cap: Time, expected: Time) {
        let buckets = QuotaBuckets::new();
        assert!(
            consume_request_quota(image, &buckets, "alice", Some(""), elapsed, cap) == expected
        );
    }

    #[test]
    fn zero_elapsed_returns_zero_delay() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 100.0);
        check_request_delay(&img, 0, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn no_quota_returns_zero_delay() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        check_request_delay(&img, 5000, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn under_budget_returns_zero_delay() {
        // rate=100% ⇒ 1_000_000 µs/sec budget; 5_000 µs is well under one
        // second of capacity → no overage.
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 100.0);
        check_request_delay(&img, 5000, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=0.001% ⇒ 10 µs/sec budget; 1_000_000 µs of work is a colossal
        // overage → multi-day delay → capped at 1s.
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 0.001);
        check_request_delay(&img, 1_000_000, secs(1), secs(1));
    }

    #[test]
    fn overage_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 0.001);
        check_request_delay(&img, 1_000_000, millis(25), millis(25));
    }

    /// Kafka's quota window forgets a sample whose last record is
    /// `quota.window.num * quota.window.size.seconds` old, so a client that
    /// stops overrunning is not throttled for the size of the overrun: 1000
    /// seconds of handler time in one request keep the capped throttle for
    /// 11 s, and not after.
    #[test]
    fn an_overload_is_forgotten_once_its_sample_leaves_the_window() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 100.0);
        let (clock, buckets) = manual_buckets();
        let bound = millis(20);
        let consume_after = |millis, micros| {
            check!(let Ok(()) = clock.advance(Duration::from_millis(millis)));
            consume_request_quota(&img, &buckets, "alice", Some(""), micros, bound).delay
        };

        let during = consume_after(0, 1_000_000_000);
        let last_moment = consume_after(10_999, 1_000);
        let after = consume_after(1, 1_000);

        check!((during, last_moment, after) == (bound, bound, <Time as TimeExt>::ZERO));
    }

    /// The overage of one request stays charged for the next (#1212), as it
    /// does for a byte rate: Kafka records the handler time before it checks
    /// the request quota, so a client that keeps overrunning it is throttled
    /// for everything it has overrun, not for its last request alone.
    #[test]
    fn overage_stays_charged_for_the_next_request() {
        // rate=100% is a bound of 1_000_000 us/sec. Over the padded 10 s
        // window, 12_000_000 us are 1_200_000 us/sec, a
        // `(1.2 - 1) / 1 * 10 s` = 2 s throttle, and 3_000_000 more are
        // 1_500_000 us/sec, 5 s.
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 100.0);
        let (_, buckets) = manual_buckets();

        let first = consume_request_quota(&img, &buckets, "alice", Some(""), 12_000_000, secs(60));
        let second = consume_request_quota(&img, &buckets, "alice", Some(""), 3_000_000, secs(60));

        assert!((first.delay, second.delay) == (secs(2), secs(5)));
    }

    /// Kafka's `DefaultQuotaCallback` gives a request with a null client id the
    /// quota of the first level in `(user, <default>)`, `user`, ... that is
    /// configured, and only a user level throttles. With a `(user, <default>)`
    /// quota beside the user quota, a null client id is not throttled, and an
    /// empty one skips the pair level and pays the user quota (#1241).
    #[test]
    fn a_null_client_id_is_shadowed_by_a_user_default_client_quota() {
        let img = image_with_quotas(
            [vec![QuotaEntityInput::user(Some("alice"))], vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(None),
            ]]
            .map(|entity| {
                quota_record()
                    .entity(entity)
                    .key("request_percentage")
                    .value(0.001)
                    .call()
            })
            .to_vec(),
        );

        let [null_client, empty_client] = [None, Some("")].map(|client_id| {
            consume_request_quota(
                &img,
                &QuotaBuckets::new(),
                "alice",
                client_id,
                1_000_000,
                secs(1),
            )
        });

        assert!(null_client == <Time as TimeExt>::ZERO);
        assert!(empty_client == secs(1));
    }

    #[test]
    fn overage_returns_scaled_uncapped_delay() {
        // rate=100% is a bound of 1_000_000 us/sec. Over the padded 10 s
        // window, 10_500_000 us are 1_050_000 us/sec, Kafka's
        // `(1.05 - 1) / 1 * 10 s` = 500 ms, under the 1 s cap.
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 100.0);
        let (_, buckets) = manual_buckets();

        let delay = consume_request_quota(&img, &buckets, "alice", Some(""), 10_500_000, secs(1));

        assert!(delay == millis(500));
    }

    /// KIP-124 meters the time a request holds a handler thread, so a handler
    /// parked on a timer, a channel or a raft commit is not charged for the
    /// wait. Only the time spent inside `poll` counts.
    #[tokio::test]
    async fn active_time_counts_polling_and_not_parking() {
        use {assert2::check, std::time::Duration};

        let Timed {
            output: value,
            active: parked,
        } = with_active_time(async {
            time::sleep(Duration::from_millis(300)).await;
            7
        })
        .await;
        check!(value == 7);
        check!(parked < Duration::from_millis(100), "{parked:?}");

        let Timed {
            output: (),
            active: working,
        } = with_active_time(async {
            thread::sleep(Duration::from_millis(60));
        })
        .await;
        check!(working >= Duration::from_millis(60), "{working:?}");
    }

    /// An `ActiveClock` charges the work between and inside its awaits, and
    /// not the time an await spent parked.
    #[tokio::test]
    async fn active_clock_charges_work_and_not_a_parked_await() {
        use {assert2::check, std::time::Duration};

        let mut clock = ActiveClock::start();
        thread::sleep(Duration::from_millis(40));
        let value = clock
            .run(async {
                time::sleep(Duration::from_millis(300)).await;
                thread::sleep(Duration::from_millis(20));
                7
            })
            .await;
        let active = clock.active();

        check!(value == 7);
        check!(active >= Duration::from_millis(60), "{active:?}");
        check!(active < Duration::from_millis(200), "{active:?}");
    }
}
