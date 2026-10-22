//! Two revisions that differ only in source, built and measured the way the
//! nightly criterion lane does it (#1608).
//!
//! Drives a real `cargo`, so Bazel tags it `manual`: a test sandbox has no
//! Cargo toolchain. It needs no network; the fixture has no dependencies.

use {
    assert2::assert,
    krabka_parse_benches::{
        BenchmarkMetric, BenchmarkSummary, ParseBenchesError, compare_summaries,
        executables::{
            BenchExecutable, ExecutableManifest, manifest_from_cargo_messages, verify_pass,
        },
    },
    std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        path::Path,
        process::{Command, Output},
    },
    tempfile::tempdir,
};

const MANIFEST: &str = r#"[package]
name = "isolation-review"
version = "0.1.0"
edition = "2024"

[[bench]]
name = "latency"
harness = false
"#;

const BENCH: &str = r#"fn main() { println!("measured={}", isolation_review::revision()); }"#;

/// Write one revision of the fixture, which returns `side` from its library.
fn revision(dir: &Path, side: &str) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::create_dir_all(dir.join("benches")).unwrap();
    fs::write(dir.join("Cargo.toml"), MANIFEST).unwrap();
    fs::write(dir.join("benches/latency.rs"), BENCH).unwrap();
    fs::write(
        dir.join("src/lib.rs"),
        format!("pub fn revision() -> &'static str {{ \"{side}\" }}\n"),
    )
    .unwrap();
}

fn cargo(source: &Path, target: &Path, extra: &[&str]) -> Output {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let output = Command::new(cargo)
        .current_dir(source)
        .env("CARGO_TARGET_DIR", target)
        .args(["bench", "--offline", "--bench", "latency"])
        .args(extra)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// The lane's prebuild: build without running, and record what was built.
fn prebuild(source: &Path, target: &Path, commit: &str) -> ExecutableManifest {
    let output = cargo(source, target, &["--no-run", "--message-format=json"]);
    manifest_from_cargo_messages(&String::from_utf8(output.stdout).unwrap(), commit).unwrap()
}

/// One measurement pass: what the bench printed, and the executables the
/// pass is verified to have run.
fn measure(
    source: &Path,
    target: &Path,
    manifest: &ExecutableManifest,
    commit: &str,
) -> (String, Result<Vec<BenchExecutable>, ParseBenchesError>) {
    let output = cargo(source, target, &[]);
    let stderr = String::from_utf8(output.stderr).unwrap();
    (
        String::from_utf8(output.stdout).unwrap(),
        verify_pass(manifest, &stderr, source, commit),
    )
}

fn summary(commit: &str, executables: Vec<BenchExecutable>) -> BenchmarkSummary {
    BenchmarkSummary {
        suite: "isolation-review".to_string(),
        commit: commit.to_string(),
        timestamp: "2026-10-22T00:00:00Z".to_string(),
        benchmarks: BTreeMap::from([("latency".to_string(), BenchmarkMetric {
            ns_per_iter: 100.0,
            variance_ns: 1.0,
        })]),
        executables,
    }
}

#[test]
fn separate_target_directories_measure_each_revision_on_every_pass() {
    let work = tempdir().unwrap();
    let (reference, candidate) = (work.path().join("reference"), work.path().join("candidate"));
    // The issue's order: the candidate tree exists first.
    revision(&candidate, "candidate");
    revision(&reference, "reference");
    let reference_target = work.path().join("target-reference");
    let candidate_target = work.path().join("target-candidate");
    let reference_manifest = prebuild(&reference, &reference_target, "reference");
    let candidate_manifest = prebuild(&candidate, &candidate_target, "candidate");

    let mut summaries = BTreeMap::<&str, Vec<BenchmarkSummary>>::new();
    for pass in 1..=3 {
        let sides = if pass % 2 == 0 {
            [
                (
                    "candidate",
                    &candidate,
                    &candidate_target,
                    &candidate_manifest,
                ),
                (
                    "reference",
                    &reference,
                    &reference_target,
                    &reference_manifest,
                ),
            ]
        } else {
            [
                (
                    "reference",
                    &reference,
                    &reference_target,
                    &reference_manifest,
                ),
                (
                    "candidate",
                    &candidate,
                    &candidate_target,
                    &candidate_manifest,
                ),
            ]
        };
        for (side, source, target, manifest) in sides {
            let (printed, ran) = measure(source, target, manifest, side);
            assert!(printed == format!("measured={side}\n"), "pass {pass}");
            let ran = ran.unwrap();
            assert!(ran == manifest.executables, "pass {pass}");
            summaries.entry(side).or_default().push(summary(side, ran));
        }
    }

    let verdict = compare_summaries(&summaries["reference"], &summaries["candidate"], 0.1).unwrap();
    assert!(verdict.reference_executables == reference_manifest.executables);
    assert!(verdict.candidate_executables == candidate_manifest.executables);
    assert!(verdict.reference_executables[0].sha256 != verdict.candidate_executables[0].sha256);
}

#[test]
fn a_shared_target_directory_never_yields_a_mislabelled_verdict() {
    let work = tempdir().unwrap();
    let (reference, candidate) = (work.path().join("reference"), work.path().join("candidate"));
    revision(&candidate, "candidate");
    revision(&reference, "reference");
    let shared = work.path().join("target");
    let reference_manifest = prebuild(&reference, &shared, "reference");
    let candidate_manifest = prebuild(&candidate, &shared, "candidate");

    let (_, reference_ran) = measure(&reference, &shared, &reference_manifest, "reference");
    let (printed, candidate_ran) = measure(&candidate, &shared, &candidate_manifest, "candidate");
    let (Ok(reference_ran), Ok(candidate_ran)) = (reference_ran, candidate_ran) else {
        // The reference pass rebuilt the shared executable under the
        // candidate's manifest, or the other way round: refused per pass.
        return;
    };
    let verdict = compare_summaries(
        &vec![summary("reference", reference_ran); 3],
        &vec![summary("candidate", candidate_ran); 3],
        0.1,
    );

    // Either the candidate pass really measured the candidate, or the
    // comparison is refused: a reference measurement never passes as the
    // candidate's.
    assert!(
        printed == "measured=candidate\n"
            || matches!(verdict, Err(ParseBenchesError::SharedExecutable(_))),
        "printed {printed:?}, verdict {verdict:?}"
    );
}
