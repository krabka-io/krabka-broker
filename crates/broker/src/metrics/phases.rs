//! The per-request accumulator that carries a request's phase durations from
//! the place that spends them to the place that observes them.
//!
//! One Produce request appends to many partitions and one Fetch reads many,
//! so neither phase is a single interval that a guard could wrap. The handler
//! creates one [`RequestPhases`], hands a borrow of it to every partition it
//! works on, and observes the totals once when the request is done. That keeps
//! `_count` on the phase families equal to the request count, which is what
//! makes the phases comparable with `request_duration_seconds`.
//!
//! The fields are atomics rather than cells because the accumulator is
//! borrowed across `.await` points inside futures that must stay `Send`.
//!
//! The partitions of one Produce request append at once, so their local
//! intervals can overlap. They open a [`LocalInterval`] rather than add an
//! elapsed time, and the local phase is charged with the union of the open
//! intervals: the time during which at least one of them was open.

use {
    super::BrokerMetrics,
    crate::handlers::ApiKeyCode,
    std::{
        sync::{
            Mutex, PoisonError,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
};

/// Nanosecond accumulators for the phases of one in-flight request.
///
/// The phases are disjoint: a request is in at most one of them at a time, and
/// no interval is charged twice. They do not cover the whole request — see
/// [`BrokerMetrics::request_throttle_duration_seconds`] for what the remainder
/// is.
#[derive(Debug, Default)]
pub(crate) struct RequestPhases {
    local_nanos: AtomicU64,
    remote_nanos: AtomicU64,
    open_local: Mutex<OpenIntervals>,
}

/// The local intervals open right now, and since when at least one has been.
#[derive(Debug, Default)]
struct OpenIntervals {
    count: usize,
    since: Option<Instant>,
}

/// One open local interval of a request. Dropping it closes the interval.
#[must_use = "the interval closes when the guard is dropped"]
pub(crate) struct LocalInterval<'a> {
    phases: &'a RequestPhases,
}

impl Drop for LocalInterval<'_> {
    fn drop(&mut self) { self.phases.close_local_at(Instant::now()); }
}

impl RequestPhases {
    /// Charge `elapsed` to the local phase: this broker's own log.
    pub(crate) fn add_local(&self, elapsed: Duration) { Self::add(&self.local_nanos, elapsed); }

    /// Charge `elapsed` to the remote phase: a wait on something other than
    /// this broker's own log — a follower, a consumer's long poll, or the
    /// object store behind a tiered or diskless partition.
    pub(crate) fn add_remote(&self, elapsed: Duration) { Self::add(&self.remote_nanos, elapsed); }

    /// Open a local interval, which ends when the guard is dropped. Intervals
    /// that overlap charge the local phase once, for their union.
    pub(crate) fn open_local(&self) -> LocalInterval<'_> {
        self.open_local_at(Instant::now());
        LocalInterval { phases: self }
    }

    fn open_local_at(&self, now: Instant) {
        let mut open = self
            .open_local
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if open.count == 0 {
            open.since = Some(now);
        }
        open.count += 1;
    }

    fn close_local_at(&self, now: Instant) {
        let mut open = self
            .open_local
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        open.count = open.count.saturating_sub(1);
        if open.count == 0
            && let Some(since) = open.since.take()
        {
            Self::add(&self.local_nanos, now.saturating_duration_since(since));
        }
    }

    /// Seconds charged to the local phase so far.
    pub(crate) fn local_seconds(&self) -> f64 { Self::seconds(&self.local_nanos) }

    /// Seconds charged to the remote phase so far.
    pub(crate) fn remote_seconds(&self) -> f64 { Self::seconds(&self.remote_nanos) }

    /// Saturating add, so a clock that jumped cannot wrap a total back to
    /// nearly zero and report a request as instantaneous.
    fn add(slot: &AtomicU64, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        slot.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(nanos))
        })
        .ok();
    }

    /// Round-trips through [`Duration`] rather than dividing the raw count,
    /// which keeps the conversion free of a lossy integer-to-float cast.
    fn seconds(slot: &AtomicU64) -> f64 { Self::duration(slot).as_secs_f64() }

    fn duration(slot: &AtomicU64) -> Duration { Duration::from_nanos(slot.load(Ordering::Relaxed)) }
}

impl BrokerMetrics {
    /// Observe the local and remote phases of one finished request, on the
    /// same `api_key` label the total latency uses.
    ///
    /// Both are observed on every request, including with a zero value: a
    /// Produce whose partitions were all rejected by an ACL did no local work,
    /// and recording that zero is what keeps `_count` equal to the request
    /// count so the phases can be compared with the total.
    pub(crate) fn observe_request_phases(&self, api_key: ApiKeyCode, phases: &RequestPhases) {
        self.observe_request_local_duration(api_key, phases.local_seconds());
        self.observe_request_remote_duration(api_key, phases.remote_seconds());
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn phases_accumulate_each_kind_independently() {
        let phases = RequestPhases::default();
        phases.add_local(Duration::from_millis(3));
        phases.add_local(Duration::from_millis(7));
        phases.add_remote(Duration::from_millis(250));

        assert!((phases.local_seconds() - 0.010).abs() < 1.0e-9);
        assert!((phases.remote_seconds() - 0.250).abs() < 1.0e-9);
    }

    /// Each interval is (open, close) in milliseconds after a common start.
    #[yare::parameterized(
        disjoint_intervals_add_up = { &[(0, 10), (20, 25)], 15 },
        overlapping_intervals_count_once = { &[(0, 10), (5, 30)], 30 },
        a_nested_interval_adds_nothing = { &[(0, 30), (5, 10)], 30 },
        overlap_beside_a_gap = { &[(0, 10), (0, 10), (50, 51)], 11 },
    )]
    fn local_intervals_charge_their_union(intervals: &[(u64, u64)], charged_ms: u64) {
        let phases = RequestPhases::default();
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let mut events: Vec<(u64, bool)> = intervals
            .iter()
            .flat_map(|&(open, close)| [(open, true), (close, false)])
            .collect();
        // At one instant, close before opening, as a guard dropped before the
        // next is taken would.
        events.sort_by_key(|&(ms, opens)| (ms, opens));
        for (ms, opens) in events {
            if opens {
                phases.open_local_at(at(ms));
            } else {
                phases.close_local_at(at(ms));
            }
        }

        assert2::assert!(
            RequestPhases::duration(&phases.local_nanos) == Duration::from_millis(charged_ms)
        );
    }

    #[test]
    fn a_dropped_interval_charges_the_local_phase() {
        let phases = RequestPhases::default();
        let interval = phases.open_local();
        std::thread::sleep(Duration::from_millis(2));
        drop(interval);

        assert!(phases.local_seconds() >= 0.002);
    }

    #[test]
    fn phases_start_at_zero_and_saturate_instead_of_wrapping() {
        let phases = RequestPhases::default();
        assert!(phases.local_seconds() < 1.0e-9);
        assert!(phases.remote_seconds() < 1.0e-9);

        // Two maximal durations must not wrap the total back toward zero: a
        // wrapped total would report a stalled request as instantaneous.
        phases.add_remote(Duration::MAX);
        phases.add_remote(Duration::MAX);
        assert!(phases.remote_seconds() > 1.0e9);
    }

    #[tokio::test]
    async fn observe_request_phases_records_one_sample_on_each_family() {
        let metrics = BrokerMetrics::new();
        let phases = RequestPhases::default();
        phases.add_local(Duration::from_millis(2));

        metrics.observe_request_phases(0, &phases);

        // `Histogram::sum`/`count` are behind prometheus-client's `test-util`
        // feature, so the rendered exposition is what a test reads — the same
        // text an operator scrapes.
        krabka_macros::metric_registry_fixture!(metrics, rendered, registry; unwrap());

        for expected in [
            "krabka_broker_request_local_duration_seconds_count{api_key=\"Produce\"} 1",
            "krabka_broker_request_local_duration_seconds_sum{api_key=\"Produce\"} 0.002",
            // The remote phase is observed too, with the zero this request
            // spent there: that is what keeps the two `_count`s equal.
            "krabka_broker_request_remote_duration_seconds_count{api_key=\"Produce\"} 1",
            "krabka_broker_request_remote_duration_seconds_sum{api_key=\"Produce\"} 0.0",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in:\n{rendered}"
            );
        }
    }
}
