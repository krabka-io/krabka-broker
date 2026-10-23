//! Failover evidence checks and recovery analysis for benchmark runs.

use {
    crate::{
        aggregate::{CellKey, stat},
        ids::{MessageCount, TimeOffsetMs},
        numeric::to_f64,
        scenario::{Disturbance, RunOutput, Sample, Stack},
    },
    krabka_units::{fmt::Human as _, prelude::*},
    std::collections::BTreeMap,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RateRecovery {
    pub(crate) baseline: Frequency,
    pub(crate) min_after_kill: Frequency,
    pub(crate) recovery: Option<Time>,
}

/// Why one failover run carries no usable evidence, or `None` when it does.
///
/// A run that errored, that recorded no disturbance, that stamped a recovery
/// before the kill, or that moved no traffic is not a measurement. Admitting
/// one is what let the gate report PASS over a workload that died: a recovery
/// stamp below the kill stamp saturates to 0 ms in
/// [`crate::ids::TimeOffsetMs::since`], and a zero-rate baseline reads as a
/// stack that never slowed down. Each of these is a gate violation and not a
/// reason to drop the run quietly.
fn failover_evidence_defect(run: &RunOutput) -> Option<String> {
    if !run.errors.is_empty() {
        return Some(format!(
            "the run reported errors: {}",
            run.errors.join("; ")
        ));
    }
    let Some(disturbance) = run.disturbance.as_ref() else {
        return Some("the run recorded no disturbance".to_owned());
    };
    if disturbance.kill_at_ms == TimeOffsetMs(0) {
        return Some("the run recorded no broker kill".to_owned());
    }
    if disturbance.recovery_at_ms < disturbance.kill_at_ms {
        return Some(format!(
            "the recovery stamp {} is before the kill stamp {}",
            disturbance.recovery_at_ms, disturbance.kill_at_ms
        ));
    }
    if disturbance.recovery_at_ms == TimeOffsetMs(0) {
        return Some("the run never acknowledged a record after the kill".to_owned());
    }
    if run.throughput.msgs_produced == MessageCount(0) {
        return Some("the run produced no messages".to_owned());
    }
    if run.throughput.producer_rate <= Frequency::ZERO {
        return Some("the run measured a zero producer rate".to_owned());
    }
    None
}

/// Evaluate failover evidence and comparisons without I/O. An empty result means the gate passes.
#[must_use]
pub fn failover_gate_violations(runs: &[RunOutput]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut by_group: BTreeMap<CellKey, Vec<&RunOutput>> = BTreeMap::new();
    for r in runs.iter().filter(|r| r.scenario.failover.is_some()) {
        // Validate before grouping. A run with no usable evidence never
        // reaches the comparison, and it fails the gate on its own account.
        if let Some(defect) = failover_evidence_defect(r) {
            violations.push(format!(
                "{}: {} failover run is not a measurement: {defect}",
                CellKey::of(r).label(),
                r.stack.as_str(),
            ));
            continue;
        }
        by_group.entry(CellKey::of(r)).or_default().push(r);
    }

    if by_group.is_empty() {
        violations.push("missing failover results".into());
        return violations;
    }

    for (key, group) in by_group {
        let krabka: Vec<&RunOutput> = group
            .iter()
            .copied()
            .filter(|r| r.stack == Stack::Krabka)
            .collect();
        let kafka: Vec<&RunOutput> = group
            .iter()
            .copied()
            .filter(|r| r.stack == Stack::Kafka)
            .collect();
        let label = key.label();

        let Some(c_recovery) = mean_failover_recovery(&krabka) else {
            violations.push(format!(
                "{label}: missing Krabka failover disturbance result"
            ));
            continue;
        };
        let Some(k_recovery) = mean_failover_recovery(&kafka) else {
            violations.push(format!(
                "{label}: missing kafka failover disturbance result"
            ));
            continue;
        };
        if c_recovery > k_recovery {
            violations.push(format!(
                "{label}: Krabka recovery {} is slower than kafka {}",
                c_recovery.human(),
                k_recovery.human()
            ));
        }
        compare_failover_smoothness(&mut violations, &label, &krabka, &kafka);

        require_rate_samples(&mut violations, &label, "krabka", &krabka);
        require_rate_samples(&mut violations, &label, "kafka", &kafka);
        compare_rate_recovery(
            &mut violations,
            &label,
            &krabka,
            &kafka,
            "producer",
            producer_sample_rate,
        );
        compare_rate_recovery(
            &mut violations,
            &label,
            &krabka,
            &kafka,
            "consumer",
            consumer_sample_rate,
        );
    }

    violations
}

fn compare_failover_smoothness(
    violations: &mut Vec<String>,
    label: &str,
    krabka: &[&RunOutput],
    kafka: &[&RunOutput],
) {
    if let (Some(c_drops), Some(k_drops)) =
        (mean_failover_dropped(krabka), mean_failover_dropped(kafka))
        && c_drops > k_drops
    {
        violations.push(format!(
            "{label}: Krabka dropped {c_drops:.0} messages vs kafka {k_drops:.0}"
        ));
    }
    if let (Some(c_spike), Some(k_spike)) = (
        mean_failover_latency_spike(krabka),
        mean_failover_latency_spike(kafka),
    ) && c_spike > k_spike
    {
        violations.push(format!(
            "{label}: Krabka latency spike {} is higher than kafka {}",
            c_spike.human(),
            k_spike.human()
        ));
    }
}

fn compare_rate_recovery(
    violations: &mut Vec<String>,
    label: &str,
    krabka: &[&RunOutput],
    kafka: &[&RunOutput],
    metric: &str,
    select: SampleRate,
) {
    let (Some(c_rate), Some(k_rate)) = (
        mean_rate_recovery(krabka, select),
        mean_rate_recovery(kafka, select),
    ) else {
        return;
    };

    match (c_rate.recovery, k_rate.recovery) {
        (Some(c), Some(k)) if c > k => violations.push(format!(
            "{label}: Krabka {metric} rate recovery {} is slower than kafka {}",
            c.human(),
            k.human()
        )),
        (None, Some(k)) => violations.push(format!(
            "{label}: Krabka {metric} rate did not recover while kafka recovered in {}",
            k.human()
        )),
        (None, None) => violations.push(format!("{label}: Krabka {metric} rate did not recover")),
        _ => {}
    }
}

fn require_rate_samples(
    violations: &mut Vec<String>,
    label: &str,
    stack_label: &str,
    runs: &[&RunOutput],
) {
    if runs.is_empty() {
        return;
    }
    for (idx, run) in runs.iter().enumerate() {
        if rate_recovery_for_run(run, producer_sample_rate).is_none()
            || rate_recovery_for_run(run, consumer_sample_rate).is_none()
        {
            violations.push(format!(
                "{label}: {stack_label} failover run is missing message-rate samples (run {})",
                idx + 1
            ));
        }
    }
}

fn mean_failover_value(runs: &[&RunOutput], value: impl Fn(&Disturbance) -> f64) -> Option<f64> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(value)
        .collect();
    (!vals.is_empty()).then(|| stat(&vals).mean)
}

pub(crate) fn mean_failover_recovery(runs: &[&RunOutput]) -> Option<Time> {
    mean_failover_value(runs, |d| d.recovery_at_ms.since(d.kill_at_ms).secs_f64())
        .map(Time::from_secs_f64)
}

fn mean_failover_dropped(runs: &[&RunOutput]) -> Option<f64> {
    mean_failover_value(runs, |d| to_f64(d.dropped.0))
}

fn mean_failover_latency_spike(runs: &[&RunOutput]) -> Option<Time> {
    mean_failover_value(runs, |d| d.latency_spike_max.secs_f64()).map(Time::from_secs_f64)
}

pub(crate) fn mean_rate_recovery(runs: &[&RunOutput], select: SampleRate) -> Option<RateRecovery> {
    let vals: Vec<RateRecovery> = runs
        .iter()
        .filter_map(|r| rate_recovery_for_run(r, select))
        .collect();
    if vals.is_empty() {
        return None;
    }
    let baselines: Vec<f64> = vals.iter().map(|v| v.baseline.per_sec_f64()).collect();
    let mins: Vec<f64> = vals
        .iter()
        .map(|v| v.min_after_kill.per_sec_f64())
        .collect();
    let recoveries: Vec<f64> = vals
        .iter()
        .filter_map(|v| v.recovery.map(TimeExt::secs_f64))
        .collect();
    Some(RateRecovery {
        baseline: Frequency::from_per_sec(stat(&baselines).mean),
        min_after_kill: Frequency::from_per_sec(stat(&mins).mean),
        recovery: (!recoveries.is_empty()).then(|| Time::from_secs_f64(stat(&recoveries).mean)),
    })
}

fn rate_recovery_for_run(r: &RunOutput, select: SampleRate) -> Option<RateRecovery> {
    let failover = r.scenario.failover.as_ref()?;
    if r.samples.is_empty() {
        return None;
    }
    // Sample offsets are measured from the *measurement* window start, so the
    // kill offset drops the warmup the scenario spent before it.
    let kill_offset = failover.kill_after - r.scenario.warmup;
    let rate_at = |s: &Sample| select(s).per_sec_f64();
    let baseline: Vec<f64> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms.as_time() < kill_offset)
        .map(rate_at)
        .collect();
    if baseline.is_empty() {
        return None;
    }
    let baseline_rate = stat(&baseline).mean;
    let after: Vec<&Sample> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms.as_time() >= kill_offset)
        .collect();
    if after.is_empty() {
        return None;
    }
    let min_after_kill = after
        .iter()
        .map(|s| rate_at(s))
        .fold(f64::INFINITY, f64::min);
    let threshold = baseline_rate * 0.90;
    let recovery = after
        .iter()
        .find(|s| rate_at(s) >= threshold)
        .map(|s| s.t_offset_ms.as_time() - kill_offset);

    Some(RateRecovery {
        baseline: Frequency::from_per_sec(baseline_rate),
        min_after_kill: Frequency::from_per_sec(min_after_kill),
        recovery,
    })
}

/// How one of the two client message rates is read out of a time-series sample.
pub(crate) type SampleRate = fn(&Sample) -> Frequency;

pub(crate) fn producer_sample_rate(s: &Sample) -> Frequency { s.producer_rate }

pub(crate) fn consumer_sample_rate(s: &Sample) -> Frequency { s.consumer_rate }
