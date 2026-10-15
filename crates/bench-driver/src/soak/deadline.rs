//! The deadline checker: every fault ran, was cleaned up, and was recovered
//! from in time; every path stayed inside its budgets.
//!
//! A fault the schedule named but the evidence does not record fails, as does
//! a recovery the fault kind implies but nobody measured. The alternative --
//! judging only what was recorded -- passes a run whose fault injector silently
//! did nothing, which is the failure a soak is least able to notice by itself.

use {
    super::{
        contract::{Contract, RecoveryCheck, human},
        evidence::{Evidence, LatencySummary, RecoveryRecord},
        schedule::ScheduledFault,
    },
    krabka_units::{prelude::*, serde_units},
    serde::{Deserialize, Serialize},
    std::fmt,
};

/// How one measured recovery compared with its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadlineOutcome {
    Met,
    Missed,
    /// No measurement was recorded, or the harness gave up waiting.
    NotObserved,
}

/// One recovery against its deadline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadlineVerdict {
    pub fault: String,
    pub check: RecoveryCheck,
    #[serde(with = "serde_units::human::time")]
    pub deadline: Time,
    #[serde(with = "serde_units::human::option_time")]
    pub took: Option<Time>,
    pub outcome: DeadlineOutcome,
}

impl fmt::Display for DeadlineVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let took = self.took.map_or_else(|| "never".to_owned(), human);
        write!(
            f,
            "{} {}: took {took} against {} ({:?})",
            self.fault,
            self.check,
            human(self.deadline),
            self.outcome
        )
    }
}

/// A scheduled fault the evidence shows was not executed cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultProblem {
    pub fault: String,
    pub problem: String,
}

/// One latency percentile or throughput floor against its budget.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetVerdict {
    pub path: String,
    /// `produce p99`, `consume p50`, `throughput`, ...
    pub measure: String,
    pub budget: String,
    pub measured: String,
    pub within: bool,
}

/// Check every scheduled fault's execution and recoveries.
///
/// A self-check contract's `missed_deadline` replaces the deadline of its one
/// named check on the first occurrence of its fault.
#[must_use]
pub fn check_recoveries(
    contract: &Contract,
    schedule: &[ScheduledFault],
    evidence: &Evidence,
) -> (Vec<DeadlineVerdict>, Vec<FaultProblem>) {
    let mut verdicts = Vec::new();
    let mut problems = Vec::new();
    for fault in schedule {
        match evidence.faults.iter().find(|record| record.id == fault.id) {
            None => problems.push(FaultProblem {
                fault: fault.id.clone(),
                problem: "scheduled but never injected".to_owned(),
            }),
            Some(record) => {
                if let Some(error) = &record.cleanup_error {
                    problems.push(FaultProblem {
                        fault: fault.id.clone(),
                        problem: format!("cleanup failed: {error}"),
                    });
                } else if record.cleared_unix_ms.is_none() {
                    problems.push(FaultProblem {
                        fault: fault.id.clone(),
                        problem: "injected but never cleared".to_owned(),
                    });
                }
            }
        }
        for &check in &fault.checks {
            let deadline = deadline_for(contract, fault, check);
            let took = evidence
                .recoveries
                .iter()
                .find(|record| record.fault == fault.id && record.check == check)
                .and_then(RecoveryRecord::took);
            let outcome = match took {
                None => DeadlineOutcome::NotObserved,
                Some(took) if took > deadline => DeadlineOutcome::Missed,
                Some(_) => DeadlineOutcome::Met,
            };
            verdicts.push(DeadlineVerdict {
                fault: fault.id.clone(),
                check,
                deadline,
                took,
                outcome,
            });
        }
    }
    (verdicts, problems)
}

/// The deadline `check` is judged against after `fault`.
#[must_use]
pub fn deadline_for(contract: &Contract, fault: &ScheduledFault, check: RecoveryCheck) -> Time {
    match &contract.self_check {
        Some(self_check)
            if self_check.missed_deadline.fault == fault.plan
                && self_check.missed_deadline.check == check
                && fault.id == format!("{}#0", fault.plan) =>
        {
            self_check.missed_deadline.deadline
        }
        _ => check.deadline(&contract.deadlines),
    }
}

/// Check every path's steady-window latency and throughput against its budget.
#[must_use]
pub fn check_budgets(contract: &Contract, evidence: &Evidence) -> Vec<BudgetVerdict> {
    let mut verdicts = Vec::new();
    for path in &contract.paths {
        let Some(measured) = evidence
            .paths
            .iter()
            .find(|measured| measured.path == path.name)
        else {
            verdicts.push(BudgetVerdict {
                path: path.name.clone(),
                measure: "measurement".to_owned(),
                budget: "present".to_owned(),
                measured: "absent".to_owned(),
                within: false,
            });
            continue;
        };
        for (direction, budget, summary) in [
            ("produce", &path.produce_latency, &measured.produce_steady),
            ("consume", &path.consume_latency, &measured.consume_steady),
        ] {
            push_latency(&mut verdicts, &path.name, direction, summary, [
                ("p50", budget.p50, summary.p50),
                ("p99", budget.p99, summary.p99),
                ("p999", budget.p999, summary.p999),
            ]);
        }
        verdicts.push(BudgetVerdict {
            path: path.name.clone(),
            measure: "throughput".to_owned(),
            budget: format!(">= {}", rate(path.throughput_floor)),
            measured: rate(measured.steady_rate),
            within: measured.steady_rate >= path.throughput_floor,
        });
    }
    verdicts
}

fn push_latency(
    verdicts: &mut Vec<BudgetVerdict>,
    path: &str,
    direction: &str,
    summary: &LatencySummary,
    percentiles: [(&str, Time, Time); 3],
) {
    if summary.count == 0 {
        verdicts.push(BudgetVerdict {
            path: path.to_owned(),
            measure: format!("{direction} latency"),
            budget: "samples".to_owned(),
            measured: "none".to_owned(),
            within: false,
        });
        return;
    }
    for (name, budget, measured) in percentiles {
        verdicts.push(BudgetVerdict {
            path: path.to_owned(),
            measure: format!("{direction} {name}"),
            budget: format!("<= {}", human(budget)),
            measured: human(measured),
            within: measured <= budget,
        });
    }
}

fn rate(frequency: Frequency) -> String {
    use krabka_units::fmt::Human as _;
    frequency.human().to_string()
}

#[cfg(test)]
mod tests {
    use {
        super::{DeadlineOutcome, check_budgets, check_recoveries},
        crate::soak::{
            contract::{RecoveryCheck, fixture},
            evidence::{Evidence, FaultRecord, LatencySummary, PathMeasurement, RecoveryRecord},
            schedule::expand,
        },
        assert2::check,
        krabka_units::prelude::*,
    };

    fn record(id: &str) -> FaultRecord {
        FaultRecord {
            id: id.to_owned(),
            kind: "pause".to_owned(),
            target: "broker-1".to_owned(),
            role: "follower".to_owned(),
            injected_unix_ms: 0,
            cleared_unix_ms: Some(1_000),
            cleanup_error: None,
            detail: String::new(),
        }
    }

    fn recovery(fault: &str, check: RecoveryCheck, took_ms: Option<i64>) -> RecoveryRecord {
        RecoveryRecord {
            fault: fault.to_owned(),
            check,
            from_unix_ms: 10_000,
            recovered_unix_ms: took_ms.map(|took| 10_000 + took),
        }
    }

    /// Every fault of the fixture executed and recovered in two seconds.
    fn recovered() -> Evidence {
        Evidence {
            faults: vec![record("short-pause#0"), record("leader-restart#0")],
            recoveries: vec![
                recovery(
                    "short-pause#0",
                    RecoveryCheck::CatchUpAfterPause,
                    Some(2_000),
                ),
                recovery(
                    "leader-restart#0",
                    RecoveryCheck::LeaderFailover,
                    Some(2_000),
                ),
                recovery("leader-restart#0", RecoveryCheck::IsrRejoin, Some(2_000)),
            ],
            ..Evidence::default()
        }
    }

    #[test]
    fn the_self_check_override_makes_its_one_deadline_impossible() {
        let contract = fixture::self_check();
        let schedule = expand(&contract).unwrap();
        let (verdicts, problems) = check_recoveries(&contract, &schedule, &recovered());
        check!(problems.is_empty());
        let missed: Vec<String> = verdicts
            .iter()
            .filter(|verdict| verdict.outcome != DeadlineOutcome::Met)
            .map(|verdict| format!("{} {}", verdict.fault, verdict.check))
            .collect();
        check!(missed == ["leader-restart#0 isr_rejoin"]);
    }

    type Edit = fn(&mut Evidence);

    #[yare::parameterized(
        late = { |e: &mut Evidence| e.recoveries[0].recovered_unix_ms = Some(10_000 + 31_000), "short-pause#0 catch_up_after_pause Missed" },
        never = { |e: &mut Evidence| e.recoveries[1].recovered_unix_ms = None, "leader-restart#0 leader_failover NotObserved" },
        unmeasured = { |e: &mut Evidence| { e.recoveries.remove(0); }, "short-pause#0 catch_up_after_pause NotObserved" },
    )]
    fn a_late_or_unmeasured_recovery_fails(edit: Edit, expected: &str) {
        let mut contract = fixture::self_check();
        contract.self_check = None;
        let schedule = expand(&contract).unwrap();
        let mut evidence = recovered();
        edit(&mut evidence);
        let (verdicts, _) = check_recoveries(&contract, &schedule, &evidence);
        let failed: Vec<String> = verdicts
            .iter()
            .filter(|verdict| verdict.outcome != DeadlineOutcome::Met)
            .map(|verdict| format!("{} {} {:?}", verdict.fault, verdict.check, verdict.outcome))
            .collect();
        check!(failed == [expected]);
    }

    #[yare::parameterized(
        never_injected = { |e: &mut Evidence| { e.faults.remove(0); }, "scheduled but never injected" },
        never_cleared = { |e: &mut Evidence| e.faults[0].cleared_unix_ms = None, "injected but never cleared" },
        cleanup_failed = { |e: &mut Evidence| e.faults[0].cleanup_error = Some("SIGCONT refused".into()), "cleanup failed: SIGCONT refused" },
    )]
    fn a_fault_that_did_not_run_cleanly_is_a_problem(edit: Edit, expected: &str) {
        let contract = fixture::self_check();
        let schedule = expand(&contract).unwrap();
        let mut evidence = recovered();
        edit(&mut evidence);
        let (_, problems) = check_recoveries(&contract, &schedule, &evidence);
        check!(problems.len() == 1);
        check!(problems[0].fault == "short-pause#0");
        check!(problems[0].problem == expected);
    }

    fn summary(p50: Time, p99: Time, p999: Time) -> LatencySummary {
        LatencySummary {
            p50,
            p99,
            p999,
            count: 1_000,
        }
    }

    #[yare::parameterized(
        inside = { millis(400), per_sec(45), &[] },
        slow_p99 = { millis(600), per_sec(45), &["classic-idempotent produce p99"] },
        too_few = { millis(400), per_sec(30), &["classic-idempotent throughput"] },
    )]
    fn budgets_are_judged_on_the_steady_windows(p99: Time, steady: Frequency, outside: &[&str]) {
        let contract = fixture::self_check();
        let measured = |path: &str, p99, steady_rate| PathMeasurement {
            path: path.to_owned(),
            produce_steady: summary(millis(10), p99, secs(1)),
            produce_disturbed: summary(secs(9), secs(9), secs(9)),
            consume_steady: summary(millis(10), millis(100), secs(1)),
            consume_disturbed: LatencySummary::default(),
            steady_rate,
        };
        let evidence = Evidence {
            paths: vec![
                measured("classic-idempotent", p99, steady),
                measured("classic-transactional", millis(400), per_sec(45)),
            ],
            ..Evidence::default()
        };
        let failed: Vec<String> = check_budgets(&contract, &evidence)
            .iter()
            .filter(|verdict| !verdict.within)
            .map(|verdict| format!("{} {}", verdict.path, verdict.measure))
            .collect();
        check!(failed == outside);
    }

    #[test]
    fn a_path_with_no_measurement_fails_its_budget() {
        let contract = fixture::self_check();
        let verdicts = check_budgets(&contract, &Evidence::default());
        check!(verdicts.iter().all(|verdict| !verdict.within));
        check!(verdicts.len() == contract.paths.len());
    }
}
