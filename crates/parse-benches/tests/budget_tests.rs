//! The broker performance lane: the contract, the record, and the budget
//! evaluator that judges one against the other.

use {
    krabka_parse_benches::{
        Command,
        budget::{BudgetVerdict, Rejection, Status, evaluate, judge},
        contract::{Budget, BudgetError, Contract, Metric, Mode, contract_digest},
        record::{
            Host, Measurement, PerformanceRecord, ProcessSample, Side, Subject, Trial,
            TrialOutcome, assemble, parse_env, parse_host, parse_snapshot, resource_delta,
        },
        render::render_markdown,
        run_command,
    },
    std::{collections::BTreeMap, fmt::Write as _, fs, path::Path},
    tempfile::tempdir,
};

const RECORDS: u64 = 1_000_000;
const MIB: u64 = 1_048_576;

/// A one-cell, five-round contract: throughput and p99 are budgeted.
const CONTRACT: &str = r#"{
  "format_version": 1,
  "name": "test-contract",
  "rounds": 5,
  "cluster": {"brokers": 3, "replication_factor": 3, "min_insync_replicas": 2},
  "kafka_image": "apache/kafka:4.3.1@sha256:00",
  "cells": [{
    "id": "c",
    "record_bytes": 1024,
    "payload": "random",
    "compression": "lz4",
    "partitions": 12,
    "records": 1000000,
    "rate": null,
    "idle_seconds": 0,
    "timeout_seconds": 600,
    "budgeted": ["records_per_second", "latency_ms_p99"]
  }],
  "budgets": {
    "records_per_second": {"materiality_percent": 5.0, "spread_multiplier": 3.0, "max_spread_percent": 10.0},
    "latency_ms_p99": {"materiality_percent": 10.0, "spread_multiplier": 3.0, "max_spread_percent": 25.0}
  },
  "modes": {
    "before_after": {"gate_performance": true},
    "pinned_kafka": {"gate_performance": false}
  }
}"#;

/// The checked-in contract every real run is judged by.
const CHECKED_IN_CONTRACT: &str =
    include_str!("../../../packaging/performance/regression-contract.json");

fn contract() -> Contract { Contract::from_json(CONTRACT.as_bytes()).unwrap() }

#[bon::builder]
fn trial(
    #[builder(default = "c")] cell: &str,
    round: usize,
    side: Side,
    order: usize,
    #[builder(default = 100_000.0)] records_per_second: f64,
    #[builder(default = 5.0)] latency_ms_p99: f64,
    #[builder(default = RECORDS)] consumed: u64,
    #[builder(default = 0)] duplicates: u64,
    failure: Option<&str>,
) -> Trial {
    let outcome = match failure {
        Some(reason) => TrialOutcome::Failed {
            reason: reason.to_string(),
        },
        None => TrialOutcome::Completed(Measurement {
            sent: RECORDS,
            consumed,
            duplicates,
            errors: 0,
            seconds: 10.0,
            records_per_second,
            mib_per_second: records_per_second / 1024.0,
            latency_ms_p50: 1.0,
            latency_ms_p95: 2.0,
            latency_ms_p99,
            cpu_seconds: 20.0,
            peak_rss_bytes: 300 * MIB,
        }),
    };
    Trial {
        cell: cell.to_string(),
        round,
        side,
        order,
        outcome,
    }
}

fn subject(label: &str, revision: &str) -> Subject {
    Subject {
        label: label.to_string(),
        revision: revision.to_string(),
        image: format!("docker.io/krabka-io/krabka-broker:{revision}"),
        image_digest: format!("sha256:{revision}"),
    }
}

#[bon::builder]
fn record(
    trials: Vec<Trial>,
    #[builder(default = Mode::BeforeAfter)] mode: Mode,
) -> PerformanceRecord {
    PerformanceRecord {
        format_version: 1,
        contract_name: "test-contract".to_string(),
        contract_sha256: contract_digest(CONTRACT.as_bytes()),
        mode,
        reference: subject("krabka", "aaaa"),
        candidate: subject("krabka", "bbbb"),
        client_image: "apache/kafka:4.3.1".to_string(),
        client_image_digest: "sha256:cccc".to_string(),
        host: Host {
            cpu_model: "test cpu".to_string(),
            logical_cpus: 16,
            memory_bytes: 64 * 1024 * MIB,
            disk_available_bytes: 400 * 1024 * MIB,
            kernel: "6.0.0".to_string(),
            clock_ticks_per_second: 100,
        },
        started_at: "2026-10-15T00:00:00Z".to_string(),
        settings: BTreeMap::new(),
        trials,
    }
}

/// Five interleaved rounds of throughput: odd rounds run the reference first,
/// even rounds the candidate, as the harness does.
fn rounds(reference: [f64; 5], candidate: [f64; 5]) -> Vec<Trial> {
    let mut trials = Vec::new();
    for (index, (reference, candidate)) in reference.into_iter().zip(candidate).enumerate() {
        let round = index + 1;
        let (first, second) = if round % 2 == 1 {
            ((Side::Reference, reference), (Side::Candidate, candidate))
        } else {
            ((Side::Candidate, candidate), (Side::Reference, reference))
        };
        for (offset, (side, rate)) in [first, second].into_iter().enumerate() {
            trials.push(
                trial()
                    .round(round)
                    .side(side)
                    .order(index * 2 + offset + 1)
                    .records_per_second(rate)
                    .call(),
            );
        }
    }
    trials
}

const STEADY: [f64; 5] = [100_000.0, 101_000.0, 99_000.0, 100_500.0, 99_500.0];

fn statuses(verdict: &BudgetVerdict) -> BTreeMap<Metric, Status> {
    verdict.cells["c"]
        .iter()
        .map(|(metric, judged)| (*metric, judged.status))
        .collect()
}

fn verdict(record: &PerformanceRecord) -> BudgetVerdict {
    evaluate(&contract(), CONTRACT.as_bytes(), record).unwrap()
}

const THROUGHPUT: Budget = Budget {
    materiality_percent: 5.0,
    spread_multiplier: 3.0,
    max_spread_percent: 10.0,
};

#[yare::parameterized(
    unchanged = { Metric::RecordsPerSecond, &[100.0, 101.0, 99.0], &[100.0, 100.5, 99.5], Some(THROUGHPUT), Status::WithinBudget },
    small_slowdown = { Metric::RecordsPerSecond, &[100.0, 101.0, 99.0], &[97.0, 98.0, 96.0], Some(THROUGHPUT), Status::WithinBudget },
    material_slowdown = { Metric::RecordsPerSecond, &[100.0, 101.0, 99.0], &[80.0, 81.0, 79.0], Some(THROUGHPUT), Status::MaterialRegression },
    material_speedup = { Metric::RecordsPerSecond, &[100.0, 101.0, 99.0], &[120.0, 121.0, 119.0], Some(THROUGHPUT), Status::Improved },
    // 8% slower is past the 5% floor, but each side's 3% spread allows 9%.
    noisy_slowdown = { Metric::RecordsPerSecond, &[100.0, 103.0, 97.0], &[92.0, 95.0, 89.0], Some(THROUGHPUT), Status::NoisyNotMaterial },
    // A 12% spread exceeds the 10% variance budget whatever the medians do.
    excess_variance = { Metric::RecordsPerSecond, &[100.0, 112.0, 88.0], &[100.0, 100.0, 100.0], Some(THROUGHPUT), Status::ExcessVariance },
    // Latency points the other way: a higher candidate is the regression.
    latency_regression = { Metric::LatencyMsP99, &[5.0, 5.1, 4.9], &[6.0, 6.1, 5.9], Some(THROUGHPUT), Status::MaterialRegression },
    latency_improvement = { Metric::LatencyMsP99, &[5.0, 5.1, 4.9], &[4.0, 4.1, 3.9], Some(THROUGHPUT), Status::Improved },
    not_budgeted = { Metric::LatencyMsP50, &[1.0, 1.0, 1.0], &[9.0, 9.0, 9.0], None, Status::NotBudgeted },
)]
fn judge_classifies_one_metric(
    metric: Metric,
    reference: &[f64],
    candidate: &[f64],
    budget: Option<Budget>,
    expected: Status,
) {
    assert2::assert!(judge(metric, reference, candidate, budget.as_ref()).status == expected);
}

#[test]
fn judge_reports_the_numbers_behind_a_regression() {
    let judged = judge(
        Metric::RecordsPerSecond,
        &[100.0, 101.0, 99.0],
        &[80.0, 81.0, 79.0],
        Some(&THROUGHPUT),
    );
    let expected = krabka_parse_benches::budget::MetricVerdict {
        reference_median: 100.0,
        candidate_median: 80.0,
        worse_change: 0.2,
        reference_spread: 0.01,
        candidate_spread: 0.0125,
        allowed_change: 0.05,
        status: Status::MaterialRegression,
    };
    assert2::assert!(judged == expected);
}

/// The rejections expected for one scenario, and the throughput status when
/// the cell is evaluated at all.
struct Expected {
    passed: bool,
    rejections: Vec<Rejection>,
    throughput: Option<Status>,
}

fn regression() -> Vec<Trial> { rounds(STEADY, STEADY.map(|rate| rate * 0.8)) }

#[yare::parameterized(
    within_budget = {
        rounds(STEADY, STEADY.map(|rate| rate * 0.98)), Mode::BeforeAfter,
        Expected { passed: true, rejections: vec![], throughput: Some(Status::WithinBudget) }
    },
    material_regression = {
        regression(), Mode::BeforeAfter,
        Expected {
            passed: false,
            rejections: vec![Rejection::MaterialRegression {
                cell: "c".to_string(),
                metric: Metric::RecordsPerSecond,
                worse_change: 0.2,
                allowed_change: 0.05,
            }],
            throughput: Some(Status::MaterialRegression),
        }
    },
    // Against pinned Kafka, a performance difference is reported, not gated.
    kafka_difference_does_not_gate = {
        regression(), Mode::PinnedKafka,
        Expected { passed: true, rejections: vec![], throughput: Some(Status::MaterialRegression) }
    },
    noisy_but_not_material = {
        rounds(
            [100_000.0, 103_000.0, 97_000.0, 103_000.0, 97_000.0],
            [92_000.0, 95_000.0, 89_000.0, 95_000.0, 89_000.0],
        ),
        Mode::BeforeAfter,
        Expected { passed: true, rejections: vec![], throughput: Some(Status::NoisyNotMaterial) }
    },
    excess_variance_rejects_in_every_mode = {
        rounds([100_000.0, 115_000.0, 85_000.0, 115_000.0, 85_000.0], STEADY), Mode::PinnedKafka,
        Expected {
            passed: false,
            rejections: vec![Rejection::ExcessVariance {
                cell: "c".to_string(),
                metric: Metric::RecordsPerSecond,
                spread: 0.15,
                max_spread: 0.1,
            }],
            throughput: Some(Status::ExcessVariance),
        }
    },
)]
fn evaluate_judges_a_record(trials: Vec<Trial>, mode: Mode, expected: Expected) {
    let verdict = verdict(&record().trials(trials).mode(mode).call());
    assert2::assert!(verdict.passed == expected.passed);
    assert2::assert!(verdict.rejections == expected.rejections);
    assert2::assert!(
        verdict
            .cells
            .get("c")
            .map(|_| statuses(&verdict)[&Metric::RecordsPerSecond])
            == expected.throughput
    );
}

/// Replace one trial of a steady, within-budget run.
fn with_trial(round: usize, side: Side, replace: impl FnOnce(Trial) -> Trial) -> Vec<Trial> {
    let mut replace = Some(replace);
    rounds(STEADY, STEADY)
        .into_iter()
        .map(|trial| {
            if trial.round == round && trial.side == side {
                (replace.take().unwrap())(trial)
            } else {
                trial
            }
        })
        .collect()
}

#[yare::parameterized(
    duplicate_record = {
        with_trial(3, Side::Candidate, |t| trial().round(t.round).side(t.side).order(t.order).duplicates(1).call()),
        Rejection::CorrectnessFailure {
            cell: "c".to_string(),
            round: 3,
            side: Side::Candidate,
            reason: "expected 1000000 records; sent=1000000 consumed=1000000 duplicates=1 errors=0".to_string(),
        }
    },
    lost_record = {
        with_trial(2, Side::Candidate, |t| trial().round(t.round).side(t.side).order(t.order).consumed(RECORDS - 1).call()),
        Rejection::CorrectnessFailure {
            cell: "c".to_string(),
            round: 2,
            side: Side::Candidate,
            reason: "expected 1000000 records; sent=1000000 consumed=999999 duplicates=0 errors=0".to_string(),
        }
    },
    // A reference that loses data invalidates the comparison too.
    failed_reference = {
        with_trial(1, Side::Reference, |t| trial().round(t.round).side(t.side).order(t.order).failure("sent=10 consumed=9").call()),
        Rejection::CorrectnessFailure {
            cell: "c".to_string(),
            round: 1,
            side: Side::Reference,
            reason: "sent=10 consumed=9".to_string(),
        }
    },
)]
fn correctness_failures_always_reject(trials: Vec<Trial>, expected: Rejection) {
    for mode in [Mode::BeforeAfter, Mode::PinnedKafka] {
        let verdict = verdict(&record().trials(trials.clone()).mode(mode).call());
        assert2::assert!(!verdict.passed);
        assert2::assert!(verdict.rejections == vec![expected.clone()]);
        // No performance number is reported for a cell that lost data.
        assert2::assert!(verdict.cells.is_empty());
    }
}

#[test]
fn a_missing_trial_rejects() {
    let trials = rounds(STEADY, STEADY)
        .into_iter()
        .filter(|trial| !(trial.round == 5 && trial.side == Side::Candidate))
        .collect();
    let verdict = verdict(&record().trials(trials).call());
    assert2::assert!(
        verdict.rejections
            == vec![Rejection::MissingTrial {
                cell: "c".to_string(),
                round: 5,
                side: Side::Candidate,
            }]
    );
}

#[test]
fn the_same_side_first_in_consecutive_rounds_rejects() {
    // Every round runs the reference first: drift on the host would then
    // always land on the candidate.
    let trials = (1..=5)
        .flat_map(|round| {
            [Side::Reference, Side::Candidate]
                .into_iter()
                .enumerate()
                .map(move |(offset, side)| {
                    trial()
                        .round(round)
                        .side(side)
                        .order(round * 2 + offset)
                        .call()
                })
        })
        .collect();
    let verdict = verdict(&record().trials(trials).call());
    let expected: Vec<_> = (2..=5)
        .map(|round| Rejection::NotInterleaved {
            cell: "c".to_string(),
            round,
        })
        .collect();
    assert2::assert!(verdict.rejections == expected);
}

#[test]
fn a_budget_edited_after_the_run_cannot_judge_it() {
    let edited = CONTRACT.replace(
        "\"materiality_percent\": 5.0",
        "\"materiality_percent\": 50.0",
    );
    let contract = Contract::from_json(edited.as_bytes()).unwrap();
    let result = evaluate(
        &contract,
        edited.as_bytes(),
        &record().trials(regression()).call(),
    );
    assert2::assert!(matches!(result, Err(BudgetError::ContractMismatch { .. })));
}

#[yare::parameterized(
    unknown_cell = { trial().cell("other").round(1).side(Side::Reference).order(99).call() },
    round_out_of_range = { trial().round(6).side(Side::Reference).order(99).call() },
    duplicate_trial = { trial().round(1).side(Side::Reference).order(99).call() },
    shared_position = { trial().round(1).side(Side::Reference).order(1).call() },
)]
fn a_structurally_invalid_record_is_an_error(extra: Trial) {
    let mut trials = rounds(STEADY, STEADY);
    if extra.order == 1 {
        trials.retain(|trial| trial.order != 2);
        trials.push(Trial {
            side: Side::Candidate,
            ..extra
        });
    } else {
        trials.push(extra);
    }
    let result = evaluate(
        &contract(),
        CONTRACT.as_bytes(),
        &record().trials(trials).call(),
    );
    assert2::assert!(matches!(result, Err(BudgetError::InvalidRecord(_))));
}

#[yare::parameterized(
    two_rounds = { "\"rounds\": 5", "\"rounds\": 2" },
    unbudgeted_metric = { "\"budgeted\": [\"records_per_second\", \"latency_ms_p99\"]", "\"budgeted\": [\"peak_rss_mib\"]" },
    bad_cell_id = { "\"id\": \"c\"", "\"id\": \"C/../x\"" },
    tiny_records = { "\"record_bytes\": 1024", "\"record_bytes\": 8" },
    zero_rate = { "\"rate\": null", "\"rate\": 0" },
    negative_budget = { "\"max_spread_percent\": 10.0", "\"max_spread_percent\": -1.0" },
    min_isr_above_rf = { "\"min_insync_replicas\": 2", "\"min_insync_replicas\": 4" },
    missing_mode = { "\"pinned_kafka\": {\"gate_performance\": false}", "\"other\": {}" },
    future_format = { "\"format_version\": 1", "\"format_version\": 2" },
    rf_above_brokers = { "\"brokers\": 3", "\"brokers\": 2" },
    zero_partitions = { "\"partitions\": 12", "\"partitions\": 0" },
    nothing_budgeted = { "\"budgeted\": [\"records_per_second\", \"latency_ms_p99\"]", "\"budgeted\": []" },
    unknown_field = { "\"rounds\": 5", "\"rounds\": 5, \"extra\": true" },
)]
fn an_invalid_contract_is_refused(from: &str, to: &str) {
    let edited = CONTRACT.replace(from, to);
    assert2::assert!(edited != CONTRACT);
    assert2::assert!(Contract::from_json(edited.as_bytes()).is_err());
}

#[test]
fn the_checked_in_contract_is_valid() {
    let contract = Contract::from_json(CHECKED_IN_CONTRACT.as_bytes()).unwrap();
    assert2::assert!(contract.modes[&Mode::BeforeAfter].gate_performance);
    assert2::assert!(!contract.modes[&Mode::PinnedKafka].gate_performance);
}

fn sample(cpu_ticks: u64, rss_hwm_kib: u64) -> ProcessSample {
    started(cpu_ticks, rss_hwm_kib, 5_000)
}

fn started(cpu_ticks: u64, rss_hwm_kib: u64, process_start_ticks: u64) -> ProcessSample {
    ProcessSample {
        cpu_ticks,
        process_start_ticks,
        rss_hwm_kib,
    }
}

#[test]
fn a_snapshot_parses_into_per_pod_samples() {
    let text = "pod=krabka-0\ncpu_ticks=100\nprocess_start_ticks=5000\nrss_kib=900\nrss_hwm_kib=1024\nfd_count=40\n\
                pod=krabka-1\ncpu_ticks=250\nprocess_start_ticks=5000\nrss_hwm_kib=2048\n";
    let parsed = parse_snapshot("resources-before.txt", text).unwrap();
    let expected = BTreeMap::from([
        ("krabka-0".to_string(), sample(100, 1024)),
        ("krabka-1".to_string(), sample(250, 2048)),
    ]);
    assert2::assert!(parsed == expected);
}

#[yare::parameterized(
    key_before_pod = { "cpu_ticks=1\npod=a\ncpu_ticks=1\nprocess_start_ticks=1\nrss_hwm_kib=1\n" },
    pod_twice = { "pod=a\ncpu_ticks=1\nprocess_start_ticks=1\nrss_hwm_kib=1\npod=a\ncpu_ticks=1\nprocess_start_ticks=1\nrss_hwm_kib=1\n" },
    missing_hwm = { "pod=a\ncpu_ticks=1\nprocess_start_ticks=1\n" },
    missing_start = { "pod=a\ncpu_ticks=1\nrss_hwm_kib=1\n" },
    empty = { "" },
)]
fn a_malformed_snapshot_is_an_error(text: &str) {
    assert2::assert!(parse_snapshot("resources-after.txt", text).is_err());
}

#[test]
fn resource_delta_sums_cpu_and_peak_memory_over_brokers() {
    let before = BTreeMap::from([
        ("a".to_string(), sample(100, 1)),
        ("b".to_string(), sample(50, 1)),
    ]);
    let after = BTreeMap::from([
        ("a".to_string(), sample(300, 1024)),
        ("b".to_string(), sample(150, 2048)),
    ]);
    assert2::assert!(resource_delta(&before, &after, 100) == Ok((3.0, 3 * MIB)));
}

#[yare::parameterized(
    restarted = { BTreeMap::from([("a".to_string(), sample(10, 1))]) },
    // The container restarted under the same pod name, and the new process
    // has already used more CPU than the old one had.
    restarted_with_more_cpu = { BTreeMap::from([("a".to_string(), started(500, 1, 9_000))]) },
    pod_replaced = { BTreeMap::from([("z".to_string(), sample(500, 1))]) },
)]
fn resource_delta_refuses_a_replaced_broker(after: BTreeMap<String, ProcessSample>) {
    let before = BTreeMap::from([("a".to_string(), sample(100, 1))]);
    assert2::assert!(resource_delta(&before, &after, 100).is_err());
}

const WORKLOAD: &str = r#"{"sent":1000000,"consumed":1000000,"duplicates":0,"errors":0,"seconds":10.000000,"records_per_second":RATE,"mib_per_second":97.656,"latency_ms_p50":1.000,"latency_ms_p95":2.000,"latency_ms_p99":5.000}"#;

/// Write one trial directory the way `qualify.sh budget` does.
fn write_trial(dir: &Path, round: usize, side: Side, order: usize, rate: Option<f64>) {
    let trial = dir.join(format!("trials/round-{round}/{}/c", side.name()));
    fs::create_dir_all(&trial).unwrap();
    fs::write(trial.join("order"), format!("{order}\n")).unwrap();
    let snapshot = |ticks: u64| {
        let mut text = String::new();
        for pod in 0..3 {
            let _ = write!(
                text,
                "pod=krabka-{pod}\ncpu_ticks={ticks}\nprocess_start_ticks=5000\nrss_kib=90000\nrss_hwm_kib=102400\n"
            );
        }
        text
    };
    fs::write(trial.join("resources-before.txt"), snapshot(1_000)).unwrap();
    fs::write(trial.join("resources-after.txt"), snapshot(1_700)).unwrap();
    match rate {
        Some(rate) => {
            fs::write(trial.join("workload.json"), WORKLOAD.replace("RATE", &rate.to_string())).unwrap();
        }
        None => fs::write(
            trial.join("workload.log"),
            "Exception in thread \"main\" java.lang.IllegalStateException: sent=1000000 consumed=999998 duplicates=0 errors=0\n",
        )
        .unwrap(),
    }
}

/// A complete `qualify.sh budget` artifact directory: the recorded sample the
/// machinery is validated against without a benchmark host.
fn write_artifacts(dir: &Path, candidate: [Option<f64>; 5]) {
    fs::write(dir.join("contract.json"), CONTRACT).unwrap();
    let digest = contract_digest(CONTRACT.as_bytes());
    fs::write(
        dir.join("contract.sha256"),
        format!("{digest}  contract.json\n"),
    )
    .unwrap();
    fs::write(
        dir.join("provenance.env"),
        format!(
            "mode=before_after\ncontract_sha256={digest}\n\
             reference_label=krabka\nreference_revision=aaaa\n\
             reference_image=docker.io/krabka-io/krabka-broker:budget-reference\n\
             reference_image_digest=sha256:1111\n\
             candidate_label=krabka\ncandidate_revision=bbbb\n\
             candidate_image=docker.io/krabka-io/krabka-broker:budget-candidate\n\
             candidate_image_digest=sha256:2222\n\
             client_image=apache/kafka:4.3.1\nclient_image_digest=sha256:3333\n\
             started_at=2026-10-15T00:00:00Z\nlinger_ms=5\n"
        ),
    )
    .unwrap();
    fs::write(
        dir.join("host.env"),
        "cpu_model=Test CPU\nlogical_cpus=16\nmemory_bytes=68719476736\n\
         disk_available_bytes=400000000000\nkernel=6.0.0\nclock_ticks_per_second=100\n",
    )
    .unwrap();
    let mut order = 1;
    for (index, (reference, candidate)) in STEADY.into_iter().zip(candidate).enumerate() {
        let round = index + 1;
        let sides = if round % 2 == 1 {
            [
                (Side::Reference, Some(reference)),
                (Side::Candidate, candidate),
            ]
        } else {
            [
                (Side::Candidate, candidate),
                (Side::Reference, Some(reference)),
            ]
        };
        for (side, rate) in sides {
            write_trial(dir, round, side, order, rate);
            order += 1;
        }
    }
}

#[test]
fn assemble_reads_the_harness_layout() {
    let dir = tempdir().unwrap();
    write_artifacts(dir.path(), STEADY.map(Some));
    let record = assemble(dir.path()).unwrap();
    assert2::assert!(record.trials.len() == 10);
    assert2::assert!(
        record.settings == BTreeMap::from([("linger_ms".to_string(), "5".to_string())])
    );
    assert2::assert!(
        record.candidate
            == Subject {
                label: "krabka".to_string(),
                revision: "bbbb".to_string(),
                image: "docker.io/krabka-io/krabka-broker:budget-candidate".to_string(),
                image_digest: "sha256:2222".to_string(),
            }
    );
    let first = &record.trials[0];
    let expected = Trial {
        cell: "c".to_string(),
        round: 1,
        side: Side::Reference,
        order: 1,
        outcome: TrialOutcome::Completed(Measurement {
            sent: RECORDS,
            consumed: RECORDS,
            duplicates: 0,
            errors: 0,
            seconds: 10.0,
            records_per_second: 100_000.0,
            mib_per_second: 97.656,
            latency_ms_p50: 1.0,
            latency_ms_p95: 2.0,
            latency_ms_p99: 5.0,
            // Three brokers, 700 ticks each, at 100 ticks per second.
            cpu_seconds: 21.0,
            peak_rss_bytes: 300 * MIB,
        }),
    };
    assert2::assert!(*first == expected);
}

#[test]
fn assemble_refuses_a_contract_edited_after_the_run_started() {
    let dir = tempdir().unwrap();
    write_artifacts(dir.path(), STEADY.map(Some));
    fs::write(
        dir.path().join("contract.json"),
        CONTRACT.replace("\"rounds\": 5", "\"rounds\": 4"),
    )
    .unwrap();
    assert2::assert!(matches!(
        assemble(dir.path()),
        Err(BudgetError::InvalidArtifact { .. })
    ));
}

fn evaluate_artifacts(candidate: [Option<f64>; 5]) -> (bool, BudgetVerdict, String) {
    let dir = tempdir().unwrap();
    let artifacts = dir.path().join("artifacts");
    fs::create_dir_all(&artifacts).unwrap();
    write_artifacts(&artifacts, candidate);
    let contract = dir.path().join("regression-contract.json");
    fs::write(&contract, CONTRACT).unwrap();
    let record = dir.path().join("performance-record.json");
    let (assembled, _) = run_command(&Command::Assemble {
        artifact_dir: artifacts,
        output: record.clone(),
    })
    .unwrap();
    assert2::assert!(assembled);
    let output = dir.path().join("verdict.json");
    let markdown = dir.path().join("performance-budget.md");
    let (passed, _) = run_command(&Command::Evaluate {
        record,
        contract,
        output: output.clone(),
        markdown: Some(markdown.clone()),
    })
    .unwrap();
    let verdict = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
    (passed, verdict, fs::read_to_string(markdown).unwrap())
}

#[test]
fn the_recorded_sample_passes_and_renders_its_contract() {
    let (passed, verdict, markdown) = evaluate_artifacts(STEADY.map(|rate| Some(rate * 1.01)));
    assert2::assert!(passed);
    assert2::assert!(verdict.passed);
    assert2::assert!(markdown.starts_with("# Performance budget: `test-contract`\n"));
    assert2::assert!(markdown.contains("not a ranking of brokers and not a supported limit"));
    assert2::assert!(markdown.contains("**Verdict: pass.**"));
    assert2::assert!(markdown.contains("| candidate | krabka | `bbbb` | `docker.io/krabka-io/krabka-broker:budget-candidate` | `sha256:2222` |"));
    assert2::assert!(markdown.contains(
        "| `c` | records/s | 100000 | 101000 | -1.0% | 0.5% / 0.5% | +5.0% | within budget |"
    ));
    // All ten raw trials are published.
    assert2::assert!(markdown.matches("| `c` | ").count() == 6 + 10);
}

#[test]
fn a_lost_record_in_the_sample_rejects_and_names_it() {
    let mut candidate = STEADY.map(Some);
    candidate[3] = None;
    let (passed, verdict, markdown) = evaluate_artifacts(candidate);
    assert2::assert!(!passed);
    assert2::assert!(verdict.rejections == vec![Rejection::CorrectnessFailure {
        cell: "c".to_string(),
        round: 4,
        side: Side::Candidate,
        reason: "the workload reported no result: Exception in thread \"main\" \
                 java.lang.IllegalStateException: sent=1000000 consumed=999998 duplicates=0 errors=0"
            .to_string(),
    }]);
    assert2::assert!(markdown.contains("**Verdict: reject.**"));
    assert2::assert!(markdown.contains("| `c` | all | — | — | — | — | — | not evaluated |"));
}

#[test]
fn check_contract_prints_the_digest_the_harness_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("contract.json");
    fs::write(&path, CHECKED_IN_CONTRACT).unwrap();
    let (valid, message) = run_command(&Command::CheckContract { contract: path }).unwrap();
    assert2::assert!(valid);
    assert2::assert!(message.ends_with(&contract_digest(CHECKED_IN_CONTRACT.as_bytes())));
}

#[test]
fn a_duplicate_cell_is_refused() {
    let mut contract = contract();
    contract.cells.push(contract.cells[0].clone());
    assert2::assert!(matches!(
        contract.validate(),
        Err(BudgetError::InvalidContract(_))
    ));
}

const HOST: &str = "cpu_model=Test CPU\nlogical_cpus=16\nmemory_bytes=1\n\
                    disk_available_bytes=1\nkernel=6.0.0\nclock_ticks_per_second=100\n";

#[yare::parameterized(
    missing_key = { "logical_cpus=16\n", "" },
    not_a_number = { "logical_cpus=16", "logical_cpus=many" },
    zero_clock = { "clock_ticks_per_second=100", "clock_ticks_per_second=0" },
    no_equals = { "kernel=6.0.0", "kernel" },
    repeated_key = { "kernel=6.0.0", "kernel=6.0.0\nkernel=6.1.0" },
)]
fn a_malformed_host_file_is_an_error(from: &str, to: &str) {
    let edited = HOST.replace(from, to);
    assert2::assert!(edited != HOST);
    assert2::assert!(matches!(
        parse_host(&edited),
        Err(BudgetError::InvalidArtifact { .. })
    ));
}

#[test]
fn parse_env_skips_comments_and_blank_lines() {
    let parsed = parse_env(
        "provenance.env",
        "# written by qualify.sh\n\nmode = before_after\n",
    )
    .unwrap();
    assert2::assert!(parsed == BTreeMap::from([("mode".to_string(), "before_after".to_string())]));
}

#[test]
fn the_report_names_every_rejection() {
    let mut trials = rounds(STEADY, STEADY.map(|rate| rate - 20_000.0));
    trials.extend(
        rounds(STEADY, STEADY)
            .into_iter()
            .map(|trial| Trial {
                cell: "d".to_string(),
                order: trial.order + 100,
                ..trial
            })
            .filter(|trial| !(trial.round == 2 && trial.side == Side::Reference)),
    );
    trials.push(
        trial()
            .cell("e")
            .round(1)
            .side(Side::Candidate)
            .order(200)
            .failure("broker died")
            .call(),
    );
    let mut contract = contract();
    for id in ["d", "e"] {
        let mut cell = contract.cells[0].clone();
        cell.id = id.to_string();
        contract.cells.push(cell);
    }
    // The verdict judges the edited contract, so give the record its digest.
    let bytes = serde_json::to_vec(&contract).unwrap();
    let mut record = record().trials(trials).call();
    record.contract_sha256 = contract_digest(&bytes);
    let verdict = evaluate(&contract, &bytes, &record).unwrap();
    let markdown = render_markdown(&contract, &record, &verdict);
    for line in [
        "**Verdict: reject.**",
        "- `c` records/s is +20.0% worse; the budget allows +5.0%",
        "- missing trial: `d` round 2 (reference)",
        "- correctness failure in `e` round 1 (candidate): broker died",
        "- missing trial: `e` round 1 (reference)",
        "| `c` | records/s | 100000 | 80000 | +20.0% | 0.5% / 0.6% | +5.0% | **material regression** |",
        "| `c` | p50 ms | 1.000 | 1.000 | +0.0% | 0.0% / 0.0% | — | recorded |",
        "| `d` | all | — | — | — | — | — | not evaluated |",
        "| 200 | `e` | 1 | candidate | — | — | — | — | — | — | failed: broker died |",
    ] {
        assert2::assert!(markdown.contains(line), "missing {line:?}");
    }
}

#[yare::parameterized(
    regression_against_kafka = { Mode::PinnedKafka, rounds(STEADY, STEADY.map(|rate| rate - 20_000.0)), "material difference (not gating)" },
    excess_variance = { Mode::BeforeAfter, rounds([100_000.0, 115_000.0, 85_000.0, 115_000.0, 85_000.0], STEADY), "spread is +15.0%; the variance budget is +10.0%" },
    improvement = { Mode::BeforeAfter, rounds(STEADY, STEADY.map(|rate| rate + 20_000.0)), "| improved |" },
    noisy = {
        Mode::BeforeAfter,
        rounds([100_000.0, 103_000.0, 97_000.0, 103_000.0, 97_000.0], [92_000.0, 95_000.0, 89_000.0, 95_000.0, 89_000.0]),
        "| noisy, not material |"
    },
    against_kafka = { Mode::PinnedKafka, rounds(STEADY, STEADY), "the pinned Apache Kafka image" },
)]
fn the_report_says_what_each_status_means(mode: Mode, trials: Vec<Trial>, expected: &str) {
    let record = record().trials(trials).mode(mode).call();
    let markdown = render_markdown(&contract(), &record, &verdict(&record));
    assert2::assert!(
        markdown.contains(expected),
        "missing {expected:?} in {markdown}"
    );
}

#[test]
fn a_record_from_a_future_format_is_refused() {
    let mut json = serde_json::to_value(record().trials(vec![]).call()).unwrap();
    json["format_version"] = 2.into();
    let bytes = serde_json::to_vec(&json).unwrap();
    assert2::assert!(matches!(
        PerformanceRecord::from_json(&bytes),
        Err(BudgetError::FormatVersion { found: 2, .. })
    ));
}
