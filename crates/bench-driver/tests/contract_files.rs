//! Every checked-in sustained-load contract is runnable and judgeable.
//!
//! `bench/soak/*.yaml` declares the limits a soak is graded against, and a
//! contract that cannot be met even by a perfect run would fail every real run
//! for a reason that has nothing to do with the broker. So each file gets two
//! trials: it loads and validates, and a synthetic run of it is judged the way
//! its profile says a real run will be. A soak's perfect run must pass. A
//! self-check's must pass once one record is removed, and must fail when
//! nothing is, so the self-check cannot pass vacuously.
//!
//! This is `libtest-mimic` with the corpus listed by hand, for the reason
//! `scenario_files` gives: Bazel stages runfiles as symbolic links, and
//! `Path::is_file` follows them.

use {
    assert2::{assert, check},
    krabka_bench_driver::soak::{
        contract::{Contract, Profile},
        synthetic::{clean_run, inject_missing_record},
        verdict::{judge, judge_self_check},
    },
    std::{
        path::{Path, PathBuf},
        process::ExitCode,
    },
};

/// Where the contracts are, under whichever runner started this test. See
/// `scenario_files` for why Bazel's variables are read first.
fn contract_root() -> PathBuf {
    if let Ok(srcdir) = std::env::var("TEST_SRCDIR") {
        let workspace =
            std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
        return [srcdir.as_str(), workspace.as_str(), "bench", "soak"]
            .iter()
            .collect();
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("TEST_SRCDIR (bazel) or CARGO_MANIFEST_DIR (cargo) must be set");
    [manifest.as_str(), "..", "..", "bench", "soak"]
        .iter()
        .collect()
}

/// Every `*.yaml` contract, sorted. An empty corpus is a wrong path, and fails.
fn contract_paths() -> Vec<PathBuf> {
    let root = contract_root();
    let entries = std::fs::read_dir(&root)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", root.display()));
    let mut paths: Vec<PathBuf> = entries
        .map(|entry| {
            entry
                .unwrap_or_else(|error| {
                    panic!("cannot read an entry in {}: {error}", root.display())
                })
                .path()
        })
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml") && path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no contract found in {}", root.display());
    paths
}

fn load(path: &Path) -> Result<Contract, String> {
    let yaml = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    Contract::from_yaml(&yaml).map_err(|error| format!("{} does not load: {error}", path.display()))
}

/// The contract loads, validates, and survives a round trip through its own
/// encoding.
fn validates(path: &Path) -> Result<(), String> {
    let contract = load(path)?;
    let errors = contract.validate();
    check!(errors.is_empty(), "{errors:?}");
    let reencoded = serde_saphyr::to_string(&contract).map_err(|error| error.to_string())?;
    let reparsed = Contract::from_yaml(&reencoded).map_err(|error| error.to_string())?;
    assert!(reparsed == contract);
    Ok(())
}

/// A synthetic run is judged the way the contract's profile says.
fn synthetic_run_is_judged(path: &Path) -> Result<(), String> {
    let contract = load(path)?;
    let clean = clean_run(&contract);
    match contract.profile {
        Profile::Soak => {
            let verdict = judge(&contract, &clean);
            check!(verdict.passed(), "{:#?}", verdict.failures());
            check!(
                !verdict.schedule.is_empty(),
                "a soak with no faults proves nothing about recovery"
            );
        }
        Profile::SelfCheck => {
            let mut injected = clean.clone();
            inject_missing_record(&contract, &mut injected);
            let report = judge_self_check(&contract, &injected);
            check!(report.passed, "{:#?}", report.problems);
            check!(!judge_self_check(&contract, &clean).passed);
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = libtest_mimic::Arguments::from_args();
    let mut trials = Vec::new();
    for path in contract_paths() {
        let file = path
            .file_name()
            .map_or_default(|name| name.to_string_lossy().into_owned());
        let validated = path.clone();
        trials.push(libtest_mimic::Trial::test(
            format!("contract_file::{file}::validates"),
            move || validates(&validated).map_err(libtest_mimic::Failed::from),
        ));
        trials.push(libtest_mimic::Trial::test(
            format!("contract_file::{file}::synthetic_run_is_judged"),
            move || synthetic_run_is_judged(&path).map_err(libtest_mimic::Failed::from),
        ));
    }
    libtest_mimic::run(&args, trials).exit_code()
}
