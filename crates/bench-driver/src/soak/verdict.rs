//! The verdict: a contract and a run's evidence, judged.
//!
//! [`judge`] is what a soak is graded by. [`judge_self_check`] is what proves
//! the grading works: it passes only when the injected missing record and the
//! injected missed deadline are each reported exactly, and nothing else is.

use {
    super::{
        contract::{Contract, Profile},
        deadline::{
            BudgetVerdict, DeadlineOutcome, DeadlineVerdict, FaultProblem, check_budgets,
            check_recoveries,
        },
        evidence::Evidence,
        ledger::SeqSet,
        reconcile::{FindingKind, Reconciliation, reconcile},
        schedule::{ScheduledFault, expand},
    },
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// A run, judged against its contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub contract: String,
    pub profile: Profile,
    /// Reasons the contract itself, or the evidence's claim to belong to it,
    /// is unusable. Any entry fails the verdict.
    pub contract_errors: Vec<String>,
    pub schedule: Vec<ScheduledFault>,
    pub reconciliation: Reconciliation,
    pub deadlines: Vec<DeadlineVerdict>,
    pub fault_problems: Vec<FaultProblem>,
    pub budgets: Vec<BudgetVerdict>,
}

impl Verdict {
    /// One line per failure, in the order a reader should fix them.
    #[must_use]
    pub fn failures(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .contract_errors
            .iter()
            .map(|error| format!("contract: {error}"))
            .collect();
        lines.extend(
            self.reconciliation
                .findings
                .iter()
                .map(|finding| format!("reconcile: {finding}")),
        );
        lines.extend(
            self.fault_problems
                .iter()
                .map(|problem| format!("fault: {} {}", problem.fault, problem.problem)),
        );
        lines.extend(
            self.deadlines
                .iter()
                .filter(|verdict| verdict.outcome != DeadlineOutcome::Met)
                .map(|verdict| format!("deadline: {verdict}")),
        );
        lines.extend(
            self.budgets
                .iter()
                .filter(|verdict| !verdict.within)
                .map(|verdict| {
                    format!(
                        "budget: {} {} measured {}, budget {}",
                        verdict.path, verdict.measure, verdict.measured, verdict.budget
                    )
                }),
        );
        lines
    }

    /// Whether the run met its contract.
    #[must_use]
    pub fn passed(&self) -> bool { self.failures().is_empty() }
}

/// Judge `evidence` against `contract`.
#[must_use]
pub fn judge(contract: &Contract, evidence: &Evidence) -> Verdict {
    let mut contract_errors: Vec<String> = contract
        .validate()
        .into_iter()
        .map(|error| error.0)
        .collect();
    if evidence.contract != contract.name {
        contract_errors.push(format!(
            "the evidence was recorded under contract `{}`, not `{}`",
            evidence.contract, contract.name
        ));
    }
    if evidence.contract_digest != contract.digest() {
        contract_errors.push(format!(
            "the evidence was recorded under contract digest {}, but this contract's is {}: \
             it was edited after the run",
            evidence.contract_digest,
            contract.digest()
        ));
    }
    if evidence.seed != contract.seed {
        contract_errors.push(format!(
            "the evidence was recorded with seed {}, not the contract's {}",
            evidence.seed, contract.seed
        ));
    }
    if contract.profile == Profile::Soak && !evidence.injected.is_empty() {
        contract_errors.push("a soak's evidence records injected losses".to_owned());
    }
    let schedule = expand(contract).unwrap_or_default();
    let (deadlines, fault_problems) = check_recoveries(contract, &schedule, evidence);
    Verdict {
        contract: contract.name.clone(),
        profile: contract.profile,
        contract_errors,
        reconciliation: reconcile(contract, evidence),
        budgets: check_budgets(contract, evidence),
        schedule,
        deadlines,
        fault_problems,
    }
}

/// What a self-check proved, or why it did not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelfCheckReport {
    pub passed: bool,
    /// What was injected and found, one line each.
    pub detected: Vec<String>,
    /// What was injected and not found, or found and not injected.
    pub problems: Vec<String>,
    pub verdict: Verdict,
}

/// Judge a self-check run.
///
/// The verdict must fail, and fail for exactly two reasons: the records the
/// run deleted are reported lost (by every reader of their path, and nothing
/// else is), and the one deadline the contract made impossible is reported
/// missed (and no other recovery is). Anything else the verdict reports is a
/// problem with the harness or the cluster, and fails the self-check too.
#[must_use]
pub fn judge_self_check(contract: &Contract, evidence: &Evidence) -> SelfCheckReport {
    let verdict = judge(contract, evidence);
    let mut detected = Vec::new();
    let mut problems = Vec::new();
    let Some(self_check) = &contract.self_check else {
        problems.push("the contract has no self_check section".to_owned());
        return SelfCheckReport {
            passed: false,
            detected,
            problems,
            verdict,
        };
    };

    // Injected losses, per producer.
    let mut injected: BTreeMap<&str, SeqSet> = BTreeMap::new();
    for loss in &evidence.injected {
        if loss.path == self_check.missing_record_path {
            injected
                .entry(loss.producer.as_str())
                .or_default()
                .insert(loss.seq);
        } else {
            problems.push(format!(
                "a loss was injected on `{}`, not on `{}`",
                loss.path, self_check.missing_record_path
            ));
        }
    }
    if injected.is_empty() {
        problems.push("the run injected no missing record, so detection proves nothing".to_owned());
    }
    let readers = contract
        .path(&self_check.missing_record_path)
        .map_or(0, |path| path.consumers.len());
    for (producer, seqs) in &injected {
        let found: Vec<_> = verdict
            .reconciliation
            .findings
            .iter()
            .filter(|finding| {
                finding.kind == FindingKind::Lost
                    && finding.path == self_check.missing_record_path
                    && finding.subject == *producer
            })
            .collect();
        let exact = found.len() == readers && found.iter().all(|finding| finding.seqs == *seqs);
        if exact {
            detected.push(format!(
                "reconciler: {} injected missing record(s) of {producer} ({:?}) reported lost by all {readers} reader(s)",
                seqs.len(),
                seqs.sample(5)
            ));
        } else {
            problems.push(format!(
                "reconciler: injected {:?} of {producer} on {} reader(s) but found {:?}",
                seqs.sample(5),
                readers,
                found
                    .iter()
                    .map(|finding| finding.seqs.sample(5))
                    .collect::<Vec<_>>()
            ));
        }
    }
    for finding in &verdict.reconciliation.findings {
        let expected = finding.kind == FindingKind::Lost
            && finding.path == self_check.missing_record_path
            && injected.contains_key(finding.subject.as_str());
        if !expected {
            problems.push(format!("unexpected reconciliation finding: {finding}"));
        }
    }

    // The impossible deadline.
    let wanted_fault = format!("{}#0", self_check.missed_deadline.fault);
    for deadline in &verdict.deadlines {
        let is_target =
            deadline.fault == wanted_fault && deadline.check == self_check.missed_deadline.check;
        match (is_target, deadline.outcome) {
            (true, DeadlineOutcome::Missed) => {
                detected.push(format!("deadline checker: {deadline}"));
            }
            (true, _) => problems.push(format!(
                "deadline checker did not report a miss: {deadline}"
            )),
            (false, DeadlineOutcome::Met) => {}
            (false, _) => problems.push(format!("unexpected deadline failure: {deadline}")),
        }
    }
    if !verdict.deadlines.iter().any(|deadline| {
        deadline.fault == wanted_fault && deadline.check == self_check.missed_deadline.check
    }) {
        problems.push(format!(
            "no {} measurement for {wanted_fault} exists to miss",
            self_check.missed_deadline.check
        ));
    }

    // Everything else must be clean.
    problems.extend(
        verdict
            .contract_errors
            .iter()
            .map(|error| format!("contract: {error}")),
    );
    problems.extend(
        verdict
            .fault_problems
            .iter()
            .map(|problem| format!("fault: {} {}", problem.fault, problem.problem)),
    );
    problems.extend(
        verdict
            .budgets
            .iter()
            .filter(|budget| !budget.within)
            .map(|budget| {
                format!(
                    "budget: {} {} measured {}, budget {}",
                    budget.path, budget.measure, budget.measured, budget.budget
                )
            }),
    );

    SelfCheckReport {
        passed: problems.is_empty() && detected.len() == injected.len() + 1,
        detected,
        problems,
        verdict,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{judge, judge_self_check},
        crate::soak::{
            contract::{Profile, fixture},
            synthetic::{clean_run, inject_missing_record},
        },
        assert2::{assert, check},
        krabka_units::prelude::*,
    };

    #[test]
    fn a_clean_synthetic_run_of_a_soak_passes() {
        let mut contract = fixture::self_check();
        contract.self_check = None;
        contract.profile = Profile::SelfCheck;
        let verdict = judge(&contract, &clean_run(&contract));
        // Only the missing self_check section is wrong with this contract.
        check!(verdict.reconciliation.findings.is_empty());
        check!(verdict.fault_problems.is_empty());
        check!(verdict.budgets.iter().all(|budget| budget.within));
        check!(
            verdict.failures() == ["contract: a self-check contract needs a self_check section"]
        );
    }

    #[test]
    fn the_self_check_passes_when_both_injections_are_detected() {
        let contract = fixture::self_check();
        let mut evidence = clean_run(&contract);
        inject_missing_record(&contract, &mut evidence);
        let report = judge_self_check(&contract, &evidence);
        assert!(report.passed, "{:#?}", report.problems);
        check!(report.detected.len() == 2);
        check!(report.detected[0].starts_with("reconciler: 1 injected missing record(s)"));
        check!(report.detected[1].starts_with("deadline checker: leader-restart#0 isr_rejoin"));
    }

    #[test]
    fn the_self_check_fails_when_nothing_was_injected() {
        let contract = fixture::self_check();
        let report = judge_self_check(&contract, &clean_run(&contract));
        check!(!report.passed);
        check!(
            report
                .problems
                .iter()
                .any(|problem| problem.contains("injected no missing record"))
        );
    }

    #[test]
    fn the_self_check_fails_when_the_reconciler_misses_the_loss() {
        let contract = fixture::self_check();
        let mut evidence = clean_run(&contract);
        inject_missing_record(&contract, &mut evidence);
        // The record is claimed deleted but every reader still saw it: a
        // reconciler that reports nothing here would pass a lossy cluster.
        let seq = evidence.injected[0].seq;
        for reader in &mut evidence.readers {
            reader.seen.insert(seq);
        }
        let report = judge_self_check(&contract, &evidence);
        check!(!report.passed);
        check!(
            report
                .problems
                .iter()
                .any(|problem| problem.starts_with("reconciler: injected"))
        );
    }

    #[test]
    fn the_self_check_fails_on_any_other_failure() {
        let contract = fixture::self_check();
        let mut evidence = clean_run(&contract);
        inject_missing_record(&contract, &mut evidence);
        evidence.faults[0].cleared_unix_ms = None;
        let report = judge_self_check(&contract, &evidence);
        check!(!report.passed);
        check!(report.problems == ["fault: short-pause#0 injected but never cleared"]);
    }

    #[test]
    fn evidence_judged_against_an_edited_contract_is_refused() {
        let contract = fixture::self_check();
        let evidence = clean_run(&contract);
        let mut edited = fixture::self_check();
        edited.paths[0].produce_latency.p99 = secs(9);
        let verdict = judge(&edited, &evidence);
        check!(
            verdict
                .contract_errors
                .iter()
                .any(|error| error.contains("edited after the run"))
        );
    }

    #[test]
    fn evidence_from_another_contract_is_refused() {
        let contract = fixture::self_check();
        let mut evidence = clean_run(&contract);
        evidence.contract = "something-else".into();
        let verdict = judge(&contract, &evidence);
        check!(
            verdict
                .contract_errors
                .iter()
                .any(|error| error.contains("something-else"))
        );
    }
}
