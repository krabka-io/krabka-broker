//! `krabka-soak-check` run the way an operator or CI runs it.

use {
    assert2::{assert, check},
    krabka_bench_driver::soak::{contract::Contract, synthetic::clean_run},
    std::{
        path::PathBuf,
        process::{Command, Output},
    },
};

/// A contract under `bench/soak/`, under whichever runner started this test.
fn contract(name: &str) -> PathBuf {
    if let Ok(srcdir) = std::env::var("TEST_SRCDIR") {
        let workspace =
            std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
        return [srcdir.as_str(), workspace.as_str(), "bench", "soak", name]
            .iter()
            .collect();
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set");
    [manifest.as_str(), "..", "..", "bench", "soak", name]
        .iter()
        .collect()
}

fn run(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_krabka-soak-check"))
        .args(args)
        .output()
        .expect("run krabka-soak-check")
}

fn text(bytes: &[u8]) -> String { String::from_utf8_lossy(bytes).into_owned() }

#[test]
fn the_synthetic_self_check_reports_both_injections() {
    let path = contract("self-check.yaml");
    let output = run(&[
        "self-check".as_ref(),
        "--contract".as_ref(),
        path.as_os_str(),
    ]);
    let stdout = text(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        text(&output.stderr)
    );
    check!(stdout.contains("detected: reconciler: 1 injected missing record(s)"));
    check!(stdout.contains("detected: deadline checker: leader-restart#0 isr_rejoin"));
    check!(stdout.contains("self-check: self-check PASS"));
}

#[test]
fn plan_prints_the_schedule_the_contract_expands_to() {
    let path = contract("rf3-72h.yaml");
    let output = run(&["plan".as_ref(), "--contract".as_ref(), path.as_os_str()]);
    assert!(output.status.success(), "{}", text(&output.stderr));
    let schedule: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("JSON schedule");
    let faults = schedule.as_array().expect("a list of faults");
    check!(faults.len() > 50);
    check!(faults[0]["id"] == "quiet-pause#0");
}

#[test]
fn judge_fails_evidence_missing_a_record_and_writes_the_verdict() {
    let path = contract("rf3-72h.yaml");
    let yaml = std::fs::read_to_string(&path).expect("read the contract");
    let parsed = Contract::from_yaml(&yaml).expect("the contract parses");
    let dir = tempfile::tempdir().expect("temp dir");

    let mut evidence = clean_run(&parsed);
    let clean = dir.path().join("clean.json");
    std::fs::write(&clean, serde_json::to_vec(&evidence).unwrap()).unwrap();
    let output = run(&[
        "judge".as_ref(),
        "--contract".as_ref(),
        path.as_os_str(),
        "--evidence".as_ref(),
        clean.as_os_str(),
    ]);
    assert!(output.status.success(), "{}", text(&output.stderr));
    check!(text(&output.stdout).contains("rf3-72h: PASS"));

    evidence.readers[0].seen.remove(7);
    let lossy = dir.path().join("lossy.json");
    let verdict = dir.path().join("verdict.json");
    std::fs::write(&lossy, serde_json::to_vec(&evidence).unwrap()).unwrap();
    let output = run(&[
        "judge".as_ref(),
        "--contract".as_ref(),
        path.as_os_str(),
        "--evidence".as_ref(),
        lossy.as_os_str(),
        "--out".as_ref(),
        verdict.as_os_str(),
    ]);
    check!(!output.status.success());
    check!(text(&output.stderr).contains("reconcile: lost on classic-idempotent (classic-idempotent-0) under read_uncommitted: 1 records, first [7]"));
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&verdict).expect("the verdict was written")).unwrap();
    check!(written["reconciliation"]["findings"][0]["kind"] == "lost");
}

#[test]
fn an_unusable_contract_is_refused_with_its_reasons() {
    let dir = tempfile::tempdir().expect("temp dir");
    let yaml = std::fs::read_to_string(contract("self-check.yaml"))
        .unwrap()
        .replace("replication_factor: 3", "replication_factor: 2");
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, yaml).unwrap();
    let output = run(&["plan".as_ref(), "--contract".as_ref(), bad.as_os_str()]);
    check!(!output.status.success());
    check!(
        text(&output.stderr)
            .contains("contract: replication_factor is 2; the sustained-load contract is RF=3")
    );
}
