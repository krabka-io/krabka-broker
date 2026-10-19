//! What `run_from_args`, the entry point the binary's `main` calls, does with
//! a whole archive: the exit code and the target it leaves behind, with and
//! without `--dry-run`.
//!
//! The tests here drive the argv-parsing entry point rather than `restore`,
//! or the binary itself, so they also cover the flag handling that sits in
//! front of the pipeline.

use {
    crate::{
        args::{RestoreOptions, restore_argv},
        fixture::build_fixture,
    },
    assert2::check,
    krabka_log::name,
    krabka_restore::{EXIT_OK, run_from_args},
    std::{path::Path, process::Command},
};

/// The entries of `dir`, or `None` when it does not exist.
fn entries(dir: &Path) -> Option<Vec<std::ffi::OsString>> {
    let read = std::fs::read_dir(dir).ok()?;
    let mut names: Vec<_> = read
        .map(|entry| entry.expect("directory entry").file_name())
        .collect();
    names.sort();
    Some(names)
}

/// 1. `--dry-run` leaves the target exactly as it found it, so the runbook's
///    plan-then-restore order works: the same command without `--dry-run`
///    then restores into the same `--log-dir` (#1512).
#[yare::parameterized(
    absent_target = { false },
    empty_target = { true },
)]
#[test_macro(tokio::test)]
async fn a_dry_run_writes_nothing_and_the_same_restore_then_succeeds(precreate: bool) {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    if precreate {
        std::fs::create_dir(&log_dir).expect("empty target");
    }
    let before = entries(&log_dir);
    let argv = |extra| {
        restore_argv(fixture.archive_root.path(), &log_dir, RestoreOptions {
            extra,
            ..Default::default()
        })
    };

    check!(run_from_args(argv(&["--dry-run"])).await == EXIT_OK);
    check!(entries(&log_dir) == before);

    check!(run_from_args(argv(&[])).await == EXIT_OK);
    check!(log_dir.join(krabka_format::META_PROPERTIES).exists());
    for partition in fixture.partitions() {
        let dir = name::partition_dir(&log_dir, partition.topic, partition.partition);
        check!(dir.exists(), "{}-{}", partition.topic, partition.partition);
    }
}

/// 2. With `--report json`, stdout holds the one report object and nothing
///    else, so `krabka-restore ... --report json | jq` parses. A real run puts
///    the formatter's progress lines on stderr. A dry run formats nothing the
///    operator keeps, so it drops them (#1512).
#[yare::parameterized(
    dry_run = { &["--report", "json", "--dry-run"], true },
    real_run = { &["--report", "json"], false },
)]
fn json_report_stdout_is_one_json_object(extra: &[&str], dry_run: bool) {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let argv = restore_argv(fixture.archive_root.path(), &log_dir, RestoreOptions {
        extra,
        ..Default::default()
    });
    // `CARGO_BIN_EXE_<bin>` is set because the binary is in this package.
    let output = Command::new(env!("CARGO_BIN_EXE_krabka-restore"))
        .args(&argv[1..])
        .output()
        .expect("run krabka-restore");
    check!(output.status.code() == Some(EXIT_OK));
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is one JSON value");
    check!(report["dry_run"] == serde_json::Value::Bool(dry_run));
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    check!(stderr.contains("Formatted 1 log directory with cluster-id") == !dry_run);
}
