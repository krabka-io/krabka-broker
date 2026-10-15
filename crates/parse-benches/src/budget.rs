//! The budget evaluator: judge a [`PerformanceRecord`] against the
//! [`Contract`] it was measured under.
//!
//! The statistics are the criterion lane's. Each side's centre is its median
//! over the interleaved rounds, its spread is its relative median absolute
//! deviation, and the allowed change is the larger of the materiality floor
//! and a multiple of the larger spread -- [`calibrated_tolerance`], the same
//! function [`compare_summaries`](crate::compare_summaries) uses.

use {
    crate::{
        calibrated_tolerance,
        contract::{Budget, BudgetError, Contract, Direction, Metric, Mode, contract_digest},
        median,
        num::to_f64,
        record::{Measurement, PerformanceRecord, Side, Subject, Trial, TrialOutcome},
        relative_mad,
    },
    serde::{Deserialize, Serialize},
    std::collections::{BTreeMap, BTreeSet},
};

/// How one budgeted metric of one cell came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// The median did not get worse by more than the materiality floor.
    WithinBudget,
    /// The median got better by more than the allowed change.
    Improved,
    /// The median got worse by more than the materiality floor, but not by
    /// more than the spread allows: the change cannot be told from noise.
    NoisyNotMaterial,
    /// The median got worse by more than both the floor and the spread allow.
    MaterialRegression,
    /// A side's spread exceeded the variance budget, so no verdict is possible.
    ExcessVariance,
    /// The metric is recorded for this cell, not budgeted.
    NotBudgeted,
}

/// The numbers behind one metric's [`Status`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricVerdict {
    /// The reference median.
    pub reference_median: f64,
    /// The candidate median.
    pub candidate_median: f64,
    /// How much worse the candidate median is, as a ratio of the reference
    /// median. Negative means better, whichever way the metric points.
    pub worse_change: f64,
    /// The reference side's relative median absolute deviation.
    pub reference_spread: f64,
    /// The candidate side's relative median absolute deviation.
    pub candidate_spread: f64,
    /// The change the budget allows, as a ratio; zero when not budgeted.
    pub allowed_change: f64,
    /// The outcome.
    pub status: Status,
}

/// Why a comparison was rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Rejection {
    /// A trial failed, or its counts show a lost, duplicated or failed record.
    CorrectnessFailure {
        /// The cell.
        cell: String,
        /// The round.
        round: usize,
        /// The side.
        side: Side,
        /// What went wrong.
        reason: String,
    },
    /// The contract expects a trial that the record does not hold.
    MissingTrial {
        /// The cell.
        cell: String,
        /// The round.
        round: usize,
        /// The side.
        side: Side,
    },
    /// The same side ran first in two consecutive rounds of a cell.
    NotInterleaved {
        /// The cell.
        cell: String,
        /// The later of the two rounds.
        round: usize,
    },
    /// A budgeted metric regressed beyond its budget, in a gating mode.
    MaterialRegression {
        /// The cell.
        cell: String,
        /// The metric.
        metric: Metric,
        /// The candidate's relative worsening.
        worse_change: f64,
        /// The change the budget allowed.
        allowed_change: f64,
    },
    /// A budgeted metric's spread exceeded its variance budget.
    ExcessVariance {
        /// The cell.
        cell: String,
        /// The metric.
        metric: Metric,
        /// The larger side's relative spread.
        spread: f64,
        /// The variance budget, as a ratio.
        max_spread: f64,
    },
}

/// The machine-readable verdict over one record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetVerdict {
    /// The contract's name.
    pub contract_name: String,
    /// The contract's digest.
    pub contract_sha256: String,
    /// The comparison mode.
    pub mode: Mode,
    /// Whether this mode's material regressions reject.
    pub gate_performance: bool,
    /// The reference side.
    pub reference: Subject,
    /// The candidate side.
    pub candidate: Subject,
    /// Per cell, per metric. A cell with a rejected trial has no entry.
    pub cells: BTreeMap<String, BTreeMap<Metric, MetricVerdict>>,
    /// Everything that rejects the comparison.
    pub rejections: Vec<Rejection>,
    /// True only when there is no rejection.
    pub passed: bool,
}

/// The value of `metric` in one completed trial.
#[must_use]
pub fn metric_value(metric: Metric, measurement: &Measurement) -> f64 {
    match metric {
        Metric::RecordsPerSecond => measurement.records_per_second,
        Metric::LatencyMsP50 => measurement.latency_ms_p50,
        Metric::LatencyMsP95 => measurement.latency_ms_p95,
        Metric::LatencyMsP99 => measurement.latency_ms_p99,
        Metric::CpuMicrosPerRecord => {
            measurement.cpu_seconds * 1.0e6 / to_f64(measurement.consumed.max(1))
        }
        Metric::PeakRssMib => to_f64(measurement.peak_rss_bytes) / 1_048_576.0,
    }
}

/// Why a trial is not a correct run of `records` records, if it is not.
#[must_use]
pub fn correctness_failure(outcome: &TrialOutcome, records: u64) -> Option<String> {
    match outcome {
        TrialOutcome::Failed { reason } => Some(reason.clone()),
        TrialOutcome::Completed(measurement) => {
            let Measurement {
                sent,
                consumed,
                duplicates,
                errors,
                ..
            } = measurement;
            (*sent != records || *consumed != records || *duplicates != 0 || *errors != 0).then(
                || {
                    format!(
                        "expected {records} records; sent={sent} consumed={consumed} \
                         duplicates={duplicates} errors={errors}"
                    )
                },
            )
        }
    }
}

/// Classify one metric from both sides' values, which must be non-empty.
///
/// `budget` is `None` for a metric the cell records but does not budget.
#[must_use]
pub fn judge(
    metric: Metric,
    reference: &[f64],
    candidate: &[f64],
    budget: Option<&Budget>,
) -> MetricVerdict {
    let reference_median = median(reference.to_vec());
    let candidate_median = median(candidate.to_vec());
    let reference_spread = spread(reference, reference_median);
    let candidate_spread = spread(candidate, candidate_median);
    let worse_change = if reference_median == 0.0 {
        0.0
    } else {
        let change = (candidate_median - reference_median) / reference_median;
        match metric.direction() {
            Direction::LowerIsBetter => change,
            Direction::HigherIsBetter => -change,
        }
    };
    let (allowed_change, status) = match budget {
        None => (0.0, Status::NotBudgeted),
        Some(budget) => {
            let materiality = budget.materiality_percent / 100.0;
            let allowed = calibrated_tolerance(
                materiality,
                budget.spread_multiplier,
                reference_spread,
                candidate_spread,
            );
            let status =
                if reference_spread.max(candidate_spread) > budget.max_spread_percent / 100.0 {
                    Status::ExcessVariance
                } else if worse_change > allowed {
                    Status::MaterialRegression
                } else if worse_change > materiality {
                    Status::NoisyNotMaterial
                } else if -worse_change > allowed {
                    Status::Improved
                } else {
                    Status::WithinBudget
                };
            (allowed, status)
        }
    };
    MetricVerdict {
        reference_median,
        candidate_median,
        worse_change,
        reference_spread,
        candidate_spread,
        allowed_change,
        status,
    }
}

/// Relative MAD, with an all-zero side (a latency below the workload's
/// resolution, say) counting as no spread rather than a division by zero.
fn spread(values: &[f64], center: f64) -> f64 {
    if center == 0.0 {
        0.0
    } else {
        relative_mad(values, center).abs()
    }
}

/// Judge `record` against `contract`, whose bytes are `contract_bytes`.
///
/// Structural problems -- another contract, a trial for an unknown cell or
/// round, a trial recorded twice -- are errors: the record cannot be judged at
/// all. Everything else is a [`Rejection`] in the verdict. A correctness
/// failure, a missing trial, broken interleaving or excess variance rejects in
/// every mode; a material regression rejects where the mode's policy gates
/// performance.
///
/// # Errors
///
/// Returns [`BudgetError`] when the record does not belong to this contract or
/// is structurally invalid.
pub fn evaluate(
    contract: &Contract,
    contract_bytes: &[u8],
    record: &PerformanceRecord,
) -> Result<BudgetVerdict, BudgetError> {
    let digest = contract_digest(contract_bytes);
    if record.contract_name != contract.name || record.contract_sha256 != digest {
        return Err(BudgetError::ContractMismatch {
            recorded: format!("{} ({})", record.contract_name, record.contract_sha256),
            supplied: format!("{} ({digest})", contract.name),
        });
    }
    let mut trials: BTreeMap<(&str, usize, Side), &Trial> = BTreeMap::new();
    let mut orders = BTreeSet::new();
    for trial in &record.trials {
        if contract.cell(&trial.cell).is_none() {
            return Err(BudgetError::InvalidRecord(format!(
                "trial for unknown cell {}",
                trial.cell
            )));
        }
        if trial.round == 0 || trial.round > contract.rounds {
            return Err(BudgetError::InvalidRecord(format!(
                "trial for cell {} has round {} outside 1..={}",
                trial.cell, trial.round, contract.rounds
            )));
        }
        if !orders.insert(trial.order) {
            return Err(BudgetError::InvalidRecord(format!(
                "two trials share position {}",
                trial.order
            )));
        }
        if trials
            .insert((trial.cell.as_str(), trial.round, trial.side), trial)
            .is_some()
        {
            return Err(BudgetError::InvalidRecord(format!(
                "cell {} round {} {} appears twice",
                trial.cell,
                trial.round,
                trial.side.name()
            )));
        }
    }

    let gate_performance = contract.modes[&record.mode].gate_performance;
    let mut rejections = Vec::new();
    let mut cells = BTreeMap::new();
    for cell in &contract.cells {
        let mut sound = true;
        let mut values: BTreeMap<Side, Vec<&Measurement>> = BTreeMap::new();
        for round in 1..=contract.rounds {
            for side in Side::BOTH {
                let Some(trial) = trials.get(&(cell.id.as_str(), round, side)) else {
                    sound = false;
                    rejections.push(Rejection::MissingTrial {
                        cell: cell.id.clone(),
                        round,
                        side,
                    });
                    continue;
                };
                if let Some(reason) = correctness_failure(&trial.outcome, cell.records) {
                    sound = false;
                    rejections.push(Rejection::CorrectnessFailure {
                        cell: cell.id.clone(),
                        round,
                        side,
                        reason,
                    });
                } else if let TrialOutcome::Completed(measurement) = &trial.outcome {
                    values.entry(side).or_default().push(measurement);
                }
            }
        }
        let first_side = |round: usize| -> Option<Side> {
            let reference = trials.get(&(cell.id.as_str(), round, Side::Reference))?;
            let candidate = trials.get(&(cell.id.as_str(), round, Side::Candidate))?;
            Some(if reference.order < candidate.order {
                Side::Reference
            } else {
                Side::Candidate
            })
        };
        for round in 2..=contract.rounds {
            if let (Some(previous), Some(current)) = (first_side(round - 1), first_side(round))
                && previous == current
            {
                sound = false;
                rejections.push(Rejection::NotInterleaved {
                    cell: cell.id.clone(),
                    round,
                });
            }
        }
        if !sound {
            continue;
        }
        let mut metrics = BTreeMap::new();
        for metric in Metric::ALL {
            let side_values = |side: Side| -> Vec<f64> {
                values[&side]
                    .iter()
                    .map(|measurement| metric_value(metric, measurement))
                    .collect()
            };
            let budget = cell
                .budgeted
                .contains(&metric)
                .then(|| &contract.budgets[&metric]);
            let verdict = judge(
                metric,
                &side_values(Side::Reference),
                &side_values(Side::Candidate),
                budget,
            );
            match verdict.status {
                Status::ExcessVariance => rejections.push(Rejection::ExcessVariance {
                    cell: cell.id.clone(),
                    metric,
                    spread: verdict.reference_spread.max(verdict.candidate_spread),
                    max_spread: budget.map_or(0.0, |budget| budget.max_spread_percent / 100.0),
                }),
                Status::MaterialRegression if gate_performance => {
                    rejections.push(Rejection::MaterialRegression {
                        cell: cell.id.clone(),
                        metric,
                        worse_change: verdict.worse_change,
                        allowed_change: verdict.allowed_change,
                    });
                }
                _ => {}
            }
            metrics.insert(metric, verdict);
        }
        cells.insert(cell.id.clone(), metrics);
    }
    Ok(BudgetVerdict {
        contract_name: contract.name.clone(),
        contract_sha256: digest,
        mode: record.mode,
        gate_performance,
        reference: record.reference.clone(),
        candidate: record.candidate.clone(),
        cells,
        passed: rejections.is_empty(),
        rejections,
    })
}
