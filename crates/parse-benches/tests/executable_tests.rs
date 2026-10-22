//! Executable identity: manifests, per-pass verification, and the verdict's
//! refusal of shared or drifting executables.

use {
    assert2::assert,
    krabka_parse_benches::{
        Args, BenchmarkMetric, BenchmarkSummary, Command, ParseBenchesError, compare_summaries,
        executables::{
            BenchExecutable, ExecutableManifest, file_digest, manifest_from_cargo_messages,
            ran_executables, verify_pass,
        },
        run_command, run_from_args,
    },
    std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
    },
    tempfile::{TempDir, tempdir},
};

/// A target directory holding one fake bench executable per name.
fn target_dir(names: &[&str]) -> TempDir {
    let dir = tempdir().unwrap();
    fs::create_dir_all(dir.path().join("release/deps")).unwrap();
    for name in names {
        fs::write(
            dir.path().join(format!("release/deps/{name}-0123")),
            format!("binary {name}"),
        )
        .unwrap();
    }
    dir
}

fn executable_path(target: &Path, name: &str) -> PathBuf {
    fs::canonicalize(target.join(format!("release/deps/{name}-0123"))).unwrap()
}

/// One `compiler-artifact` message, as Cargo writes it on one line.
fn artifact_message(kind: &str, name: &str, executable: Option<&Path>) -> String {
    serde_json::json!({
        "reason": "compiler-artifact",
        "package_id": "path+file:///work/crates/log#krabka-log@1.0.0",
        "target": { "kind": [kind], "crate_types": ["bin"], "name": name },
        "executable": executable,
        "fresh": false,
    })
    .to_string()
}

#[test]
fn a_manifest_records_every_bench_executable_and_nothing_else() {
    let target = target_dir(&["log", "produce", "tool"]);
    let log = executable_path(target.path(), "log");
    let produce = executable_path(target.path(), "produce");
    let messages = [
        artifact_message("lib", "krabka_log", None),
        artifact_message("bench", "produce", Some(&produce)),
        artifact_message("bin", "tool", Some(&executable_path(target.path(), "tool"))),
        artifact_message("bench", "log", Some(&log)),
        r#"{"reason":"build-finished","success":true}"#.to_string(),
    ]
    .join("\n");

    let manifest = manifest_from_cargo_messages(&messages, "abc123").unwrap();

    assert!(
        manifest
            == ExecutableManifest {
                commit: "abc123".to_string(),
                executables: vec![
                    BenchExecutable {
                        target: "log".to_string(),
                        sha256: file_digest(&log).unwrap(),
                        path: log,
                    },
                    BenchExecutable {
                        target: "produce".to_string(),
                        sha256: file_digest(&produce).unwrap(),
                        path: produce,
                    },
                ],
            }
    );
}

#[yare::parameterized(
    no_bench = { &[r#"{"reason":"build-finished","success":true}"#] },
    empty = { &[] },
)]
fn a_build_without_bench_executables_has_no_manifest(lines: &[&str]) {
    assert2::assert!(matches!(
        manifest_from_cargo_messages(&lines.join("\n"), "abc123"),
        Err(ParseBenchesError::NoBenchExecutables)
    ));
}

#[test]
fn running_lines_resolve_relative_and_absolute_paths() {
    let log = "\
   Compiling krabka-log v1.0.0 (/work/crates/log)
    Finished `bench` profile [optimized] target(s) in 0.56s
     Running benches/log.rs (target/release/deps/log-0123)
Benchmarking log/append: Warming up for 3.0000 s
     Running benches/produce.rs (/elsewhere/release/deps/produce-4567)
";

    assert!(
        ran_executables(log, Path::new("/work"))
            == vec![
                PathBuf::from("/work/target/release/deps/log-0123"),
                PathBuf::from("/elsewhere/release/deps/produce-4567"),
            ]
    );
}

/// A source directory whose `target/` holds `log`, its manifest, and the
/// Cargo log of a pass that ran it by a relative path.
struct Pass {
    source: TempDir,
    manifest: ExecutableManifest,
    cargo_log: String,
}

fn pass() -> Pass {
    let source = tempdir().unwrap();
    let target = source.path().join("target/release/deps");
    fs::create_dir_all(&target).unwrap();
    fs::write(target.join("log-0123"), "binary log").unwrap();
    let path = fs::canonicalize(target.join("log-0123")).unwrap();
    let manifest =
        manifest_from_cargo_messages(&artifact_message("bench", "log", Some(&path)), "abc123")
            .unwrap();
    Pass {
        source,
        manifest,
        cargo_log: "     Running benches/log.rs (target/release/deps/log-0123)\n".to_string(),
    }
}

#[test]
fn a_pass_that_ran_its_prebuilt_executable_is_verified() {
    let Pass {
        source,
        manifest,
        cargo_log,
    } = pass();

    let ran = verify_pass(&manifest, &cargo_log, source.path(), "abc123").unwrap();

    assert!(ran == manifest.executables);
}

#[test]
fn a_pass_labelled_with_another_commit_is_refused() {
    let Pass {
        source,
        manifest,
        cargo_log,
    } = pass();

    let error = verify_pass(&manifest, &cargo_log, source.path(), "def456").unwrap_err();

    assert!(matches!(
        error,
        ParseBenchesError::CommitMismatch { manifest, pass }
            if manifest == "abc123" && pass == "def456"
    ));
}

#[test]
fn a_pass_that_ran_nothing_is_refused() {
    let Pass {
        source, manifest, ..
    } = pass();

    let error = verify_pass(&manifest, "", source.path(), "abc123").unwrap_err();

    assert!(matches!(error, ParseBenchesError::NoExecutableRan));
}

#[test]
fn a_pass_that_ran_the_other_sides_executable_is_refused() {
    let Pass {
        source, manifest, ..
    } = pass();
    let other = target_dir(&["log"]);
    let other_log = format!(
        "     Running benches/log.rs ({})\n",
        executable_path(other.path(), "log").display()
    );

    let error = verify_pass(&manifest, &other_log, source.path(), "abc123").unwrap_err();

    assert!(matches!(
        error,
        ParseBenchesError::UnrecordedExecutable(path)
            if path == executable_path(other.path(), "log")
    ));
}

#[test]
fn a_pass_whose_executable_was_rebuilt_is_refused() {
    let Pass {
        source,
        manifest,
        cargo_log,
    } = pass();
    fs::write(
        source.path().join("target/release/deps/log-0123"),
        "rebuilt",
    )
    .unwrap();

    let error = verify_pass(&manifest, &cargo_log, source.path(), "abc123").unwrap_err();

    assert!(matches!(error, ParseBenchesError::ExecutableChanged(_)));
}

#[test]
fn the_executables_command_and_a_summary_carry_the_identity_through() {
    let Pass {
        source,
        manifest,
        cargo_log,
    } = pass();
    let work = tempdir().unwrap();
    let messages = work.path().join("cargo-messages.json");
    fs::write(
        &messages,
        artifact_message("bench", "log", Some(&manifest.executables[0].path)),
    )
    .unwrap();
    let manifest_path = work.path().join("executables.json");
    let (recorded, _) = run_command(&Command::Executables {
        cargo_messages: messages,
        commit: "abc123".to_string(),
        output: manifest_path.clone(),
    })
    .unwrap();
    assert!(recorded);
    let run_dir = work.path().join("run-1");
    fs::create_dir_all(&run_dir).unwrap();
    fs::write(
        run_dir.join("raw.txt"),
        "test log/append ... bench: 100 ns/iter (+/- 1)\n",
    )
    .unwrap();
    let cargo_log_path = work.path().join("cargo.log");
    fs::write(&cargo_log_path, cargo_log).unwrap();

    let summary = run_from_args(&Args {
        results_dir: run_dir,
        output: Some(work.path().join("summary.json")),
        suite: "krabka-broker".to_string(),
        commit: Some("abc123".to_string()),
        reference_dir: None,
        candidate_dir: None,
        executables: Some(manifest_path),
        cargo_log: Some(cargo_log_path),
        source_dir: Some(source.path().to_path_buf()),
        minimum_tolerance_percent: 3.0,
        command: None,
    })
    .unwrap();

    assert!(summary.executables == manifest.executables);
}

fn summaries(commit: &str, executables: &[Vec<BenchExecutable>]) -> Vec<BenchmarkSummary> {
    executables
        .iter()
        .map(|executables| BenchmarkSummary {
            suite: "test".to_string(),
            commit: commit.to_string(),
            timestamp: "2026-10-22T00:00:00Z".to_string(),
            benchmarks: BTreeMap::from([("hot/path".to_string(), BenchmarkMetric {
                ns_per_iter: 100.0,
                variance_ns: 1.0,
            })]),
            executables: executables.clone(),
        })
        .collect()
}

fn recorded(path: &str, sha256: &str) -> Vec<BenchExecutable> {
    vec![BenchExecutable {
        target: "hot".to_string(),
        path: PathBuf::from(path),
        sha256: sha256.to_string(),
    }]
}

#[test]
fn the_verdict_names_each_sides_executables() {
    let reference_executables = recorded("/reference/release/deps/hot-0123", "aaaa");
    let candidate_executables = recorded("/candidate/release/deps/hot-0123", "bbbb");
    let reference = summaries("reference", &vec![reference_executables.clone(); 3]);
    let candidate = summaries("candidate", &vec![candidate_executables.clone(); 3]);

    let verdict = compare_summaries(&reference, &candidate, 0.1).unwrap();

    assert!(verdict.passed);
    assert!(verdict.reference_executables == reference_executables);
    assert!(verdict.candidate_executables == candidate_executables);
}

#[test]
fn identical_bytes_in_separate_target_directories_are_accepted() {
    // A candidate that changes nothing the benches link builds the same bytes.
    let reference = summaries("reference", &vec![
        recorded(
            "/reference/release/deps/hot-0123",
            "same"
        );
        3
    ]);
    let candidate = summaries("candidate", &vec![
        recorded(
            "/candidate/release/deps/hot-0123",
            "same"
        );
        3
    ]);

    assert!(compare_summaries(&reference, &candidate, 0.1).is_ok());
}

#[test]
fn a_shared_executable_path_is_refused() {
    let shared = recorded("/shared/release/deps/hot-0123", "same");
    let reference = summaries("reference", &vec![shared.clone(); 3]);
    let candidate = summaries("candidate", &vec![shared; 3]);

    let error = compare_summaries(&reference, &candidate, 0.1).unwrap_err();

    assert!(matches!(
        error,
        ParseBenchesError::SharedExecutable(path)
            if path == Path::new("/shared/release/deps/hot-0123")
    ));
}

/// The error comparing `reference` with three well-formed candidate passes.
fn refusal(reference: &[Vec<BenchExecutable>]) -> ParseBenchesError {
    let candidate = summaries("candidate", &vec![
        recorded(
            "/candidate/release/deps/hot-0123",
            "bbbb"
        );
        3
    ]);
    compare_summaries(&summaries("reference", reference), &candidate, 0.1).unwrap_err()
}

#[test]
fn passes_that_ran_different_executables_are_refused() {
    let error = refusal(&[
        recorded("/reference/release/deps/hot-0123", "aaaa"),
        recorded("/reference/release/deps/hot-0123", "aaaa"),
        recorded("/reference/release/deps/hot-0123", "rebuilt"),
    ]);

    assert!(matches!(
        error,
        ParseBenchesError::ExecutableDrift("reference")
    ));
}

#[test]
fn summaries_without_executable_identity_are_refused() {
    let error = refusal(&vec![Vec::new(); 3]);

    assert!(matches!(
        error,
        ParseBenchesError::MissingExecutableIdentity("reference")
    ));
}
