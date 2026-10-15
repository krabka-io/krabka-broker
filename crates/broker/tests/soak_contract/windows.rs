//! Steady and disturbed time, and the latency and throughput split across it.
//!
//! The contract's latency budgets and throughput floors apply to the steady
//! windows only. A window is disturbed from a fault's injection until its
//! cleanup plus the longest deadline of the recoveries it is measured for:
//! that is how long the contract lets the cluster take to recover, so nothing
//! inside it is held to a steady budget. Everything else, the settle tail
//! included, is steady.
//!
//! A sample is classified when it is taken, against two sets of spans: the
//! spans the schedule plans, and the spans the faults actually ran, which a
//! slow cleanup can stretch past the plan. While a fault is injected and not
//! yet cleared, every sample is disturbed.

use {
    hdrhistogram::Histogram,
    krabka_bench_driver::{
        hist,
        soak::{
            contract::Contract,
            evidence::{LatencySummary, PathMeasurement},
            schedule::ScheduledFault,
        },
    },
    krabka_units::prelude::*,
    std::sync::Mutex,
};

/// A half-open interval of wall-clock Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

impl Span {
    fn contains(self, at: i64) -> bool { self.start <= at && at < self.end }
}

/// The disturbed span the schedule plans for `fault`, for a load that
/// started at `load_start_ms`.
pub(crate) fn planned_span(
    contract: &Contract,
    fault: &ScheduledFault,
    load_start_ms: i64,
) -> Span {
    Span {
        start: load_start_ms + fault.start.millis_i64(),
        end: load_start_ms
            + (fault.end() + fault.recovery_budget(&contract.deadlines)).millis_i64(),
    }
}

/// Milliseconds of `load` that no span of `spans` covers.
pub(crate) fn steady_millis(load: Span, spans: &[Span]) -> i64 {
    let mut clipped: Vec<Span> = spans
        .iter()
        .map(|span| Span {
            start: span.start.max(load.start),
            end: span.end.min(load.end),
        })
        .filter(|span| span.start < span.end)
        .collect();
    clipped.sort_by_key(|span| span.start);
    let mut covered = 0;
    let mut reached = load.start;
    for span in clipped {
        let start = span.start.max(reached);
        if span.end > start {
            covered += span.end - start;
            reached = span.end;
        }
    }
    (load.end - load.start).max(0) - covered
}

/// The spans that make time disturbed, shared by every task that classifies
/// a sample.
pub(crate) struct Windows {
    planned: Vec<Span>,
    actual: Mutex<Vec<Span>>,
    /// The injection instant of the fault now in progress.
    active_since: Mutex<Option<i64>>,
}

impl Windows {
    pub(crate) fn new(planned: Vec<Span>) -> Self {
        Self {
            planned,
            actual: Mutex::new(Vec::new()),
            active_since: Mutex::new(None),
        }
    }

    /// Whether a sample taken at `at` is disturbed.
    pub(crate) fn disturbed(&self, at: i64) -> bool {
        let active = self
            .active_since
            .lock()
            .map_or(true, |since| since.is_some_and(|since| at >= since));
        let actual = self
            .actual
            .lock()
            .map_or(true, |spans| spans.iter().any(|span| span.contains(at)));
        active || actual || self.planned.iter().any(|span| span.contains(at))
    }

    /// A fault was injected at `at`.
    pub(crate) fn injected(&self, at: i64) {
        if let Ok(mut since) = self.active_since.lock() {
            *since = Some(at);
        }
    }

    /// The fault in progress was cleared; `span` is how long it disturbs.
    pub(crate) fn cleared(&self, span: Span) {
        if let Ok(mut spans) = self.actual.lock() {
            spans.push(span);
        }
        if let Ok(mut since) = self.active_since.lock() {
            *since = None;
        }
    }

    /// Every span, planned and actual, for the steady-time total.
    pub(crate) fn spans(&self) -> Vec<Span> {
        let mut spans = self.planned.clone();
        if let Ok(actual) = self.actual.lock() {
            spans.extend(actual.iter().copied());
        }
        if let Ok(since) = self.active_since.lock()
            && let Some(since) = *since
        {
            spans.push(Span {
                start: since,
                end: i64::MAX,
            });
        }
        spans
    }
}

/// Latency samples split by window, and the acknowledgements that count
/// toward the steady rate.
pub(crate) struct Latencies {
    steady: Histogram<u64>,
    disturbed: Histogram<u64>,
    /// Samples taken in steady windows.
    pub(crate) steady_count: u64,
}

impl Latencies {
    pub(crate) fn new() -> Self {
        Self {
            steady: hist::new(),
            disturbed: hist::new(),
            steady_count: 0,
        }
    }

    pub(crate) fn record(&mut self, disturbed: bool, latency: Time) {
        if disturbed {
            hist::record(&mut self.disturbed, latency);
        } else {
            hist::record(&mut self.steady, latency);
            self.steady_count += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        // Both sides were built by `hist::new`, so their bounds agree and the
        // addition cannot fail.
        let _ = self.steady.add(&other.steady);
        let _ = self.disturbed.add(&other.disturbed);
        self.steady_count += other.steady_count;
    }

    fn steady(&self) -> LatencySummary { summary(&self.steady) }

    fn disturbed(&self) -> LatencySummary { summary(&self.disturbed) }
}

fn summary(histogram: &Histogram<u64>) -> LatencySummary {
    let percentiles = hist::percentiles(histogram);
    LatencySummary {
        p50: percentiles.p50,
        p99: percentiles.p99,
        p999: percentiles.p999,
        count: percentiles.count,
    }
}

/// A path's measurement from its producer's and consumers' samples.
///
/// The steady rate is the producer's steady acknowledgements over the steady
/// time of the load.
pub(crate) fn measurement(
    path: &str,
    produce: &Latencies,
    consume: &Latencies,
    steady: Time,
) -> PathMeasurement {
    let acks = f64::from(u32::try_from(produce.steady_count).unwrap_or(u32::MAX));
    let seconds = steady.secs_f64();
    PathMeasurement {
        path: path.to_owned(),
        produce_steady: produce.steady(),
        produce_disturbed: produce.disturbed(),
        consume_steady: consume.steady(),
        consume_disturbed: consume.disturbed(),
        steady_rate: Frequency::from_per_sec(if seconds > 0.0 { acks / seconds } else { 0.0 }),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Latencies, Span, Windows, measurement, planned_span, steady_millis},
        crate::support::manifest_dir,
        assert2::check,
        krabka_bench_driver::soak::{
            contract::{Contract, FaultKind, Target},
            schedule::ScheduledFault,
        },
        krabka_units::prelude::*,
    };

    const fn span(start: i64, end: i64) -> Span { Span { start, end } }

    #[yare::parameterized(
        nothing_disturbed = { &[], 100 },
        one_inside = { &[span(10, 30)], 80 },
        overlapping = { &[span(10, 30), span(20, 40)], 70 },
        nested = { &[span(10, 50), span(20, 30)], 60 },
        unsorted_disjoint = { &[span(60, 70), span(10, 20)], 80 },
        clipped_at_both_ends = { &[span(-50, 10), span(90, 500)], 80 },
        outside = { &[span(200, 300)], 100 },
        everything = { &[span(0, 100)], 0 },
    )]
    fn steady_time_is_the_load_minus_every_span(spans: &[Span], steady: i64) {
        check!(steady_millis(span(0, 100), spans) == steady);
    }

    #[test]
    fn a_planned_span_runs_to_the_longest_deadline_after_cleanup() {
        let path = manifest_dir().join("../../bench/soak/self-check.yaml");
        let mut contract = Contract::from_yaml(&std::fs::read_to_string(path).unwrap()).unwrap();
        contract.deadlines.leader_failover = secs(20);
        contract.deadlines.isr_rejoin = secs(30);
        let kind = FaultKind::Restart { graceful: false };
        let restart = ScheduledFault {
            id: "restart#0".into(),
            plan: "restart".into(),
            checks: kind.checks(Target::Leader),
            kind,
            target: Target::Leader,
            start: secs(140),
            duration: secs(5),
        };
        // Measured for leader_failover (20s) and isr_rejoin (30s): disturbed
        // from 140s until 30s after its cleanup at 145s.
        check!(planned_span(&contract, &restart, 1_000) == span(141_000, 176_000));
    }

    #[yare::parameterized(
        before = { 5, false },
        planned = { 15, true },
        between = { 25, false },
        actual = { 45, true },
        active = { 70, true },
    )]
    fn a_sample_is_disturbed_inside_any_span_or_a_live_fault(at: i64, disturbed: bool) {
        let windows = Windows::new(vec![span(10, 20)]);
        windows.injected(40);
        windows.cleared(span(40, 50));
        windows.injected(60);
        check!(windows.disturbed(at) == disturbed);
    }

    #[test]
    fn a_cleared_fault_stops_disturbing_after_its_span() {
        let windows = Windows::new(Vec::new());
        windows.injected(40);
        check!(windows.disturbed(1_000));
        windows.cleared(span(40, 50));
        check!(!windows.disturbed(1_000));
        check!(windows.spans() == [span(40, 50)]);
    }

    #[test]
    fn the_steady_rate_counts_only_steady_acknowledgements() {
        let mut produce = Latencies::new();
        for _ in 0..20 {
            produce.record(false, millis(4));
        }
        for _ in 0..7 {
            produce.record(true, secs(3));
        }
        let mut consume = Latencies::new();
        let mut other = Latencies::new();
        other.record(false, millis(9));
        consume.merge(&other);
        let measured = measurement("p", &produce, &consume, secs(10));
        check!(measured.produce_steady.count == 20);
        check!(measured.produce_disturbed.count == 7);
        check!(measured.produce_steady.p999 < millis(5));
        check!(measured.produce_disturbed.p50 > secs(2));
        check!(measured.consume_steady.count == 1);
        check!(measured.consume_disturbed.count == 0);
        check!((measured.steady_rate.per_sec_f64() - 2.0).abs() < 1.0e-9);
    }
}
