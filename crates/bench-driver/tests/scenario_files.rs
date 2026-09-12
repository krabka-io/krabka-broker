//! Every checked-in benchmark scenario loads through the driver's own loader.
//!
//! `bench/scenarios/*.yaml` is an operator-written corpus that lives outside
//! this crate, so nothing linked it to the `Scenario` type. When the dimensioned
//! fields grew units, all twelve files stopped parsing and no test noticed.
//! This harness closes that gap. It runs each file through the same `serde_yaml`
//! path that `main.rs` uses, with one nextest process per scenario, so a broken
//! file names itself.

use std::path::Path;

use assert2::{assert, check};
use krabka_bench_driver::scenario::{LoadMode, Scenario};
use krabka_units::prelude::*;

/// Where the scenario corpus is, under whichever runner started this test.
///
/// Bazel is asked first, and that order is the whole point. Both runners export
/// `CARGO_MANIFEST_DIR`, so reading it first answers for Bazel too, and under
/// Bazel it holds the package-relative `crates/bench-driver` rather than an
/// absolute path. Walking `..` out of that lands somewhere that depends on the
/// working directory and on whether the runfiles entry is a symlink, and in the
/// sandbox it found an empty directory:
///
/// ```text
/// no test cases found for test 'scenario_file' -- scanned directory:
///   `crates/bench-driver/../../bench/scenarios`
/// ```
///
/// `TEST_SRCDIR` is set by Bazel alone and is absolute. Joined with
/// `TEST_WORKSPACE` it is the runfiles root, and Bazel stages a target's `data`
/// under it by package path, so `//bench:scenarios` is exactly
/// `bench/scenarios` there. No `..`, and no dependence on the working
/// directory.
///
/// Cargo sets no `TEST_SRCDIR`, and it runs a test with the crate directory as
/// the working directory, so the relative form is right there.
///
/// # Panics
///
/// Panics when neither Bazel's pair nor Cargo's variable is set, which means
/// the test was launched by something that stages data differently again.
fn scenario_root() -> String {
    if let Ok(srcdir) = std::env::var("TEST_SRCDIR") {
        let workspace =
            std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
        return format!("{srcdir}/{workspace}/bench/scenarios");
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("TEST_SRCDIR (bazel) or CARGO_MANIFEST_DIR (cargo) must be set");
    format!("{manifest}/../../bench/scenarios")
}

/// Loads one scenario file and checks it describes a runnable benchmark.
fn scenario_file(path: &Path) -> datatest_stable::Result<()> {
    let yaml = std::fs::read_to_string(path)?;
    let scenario: Scenario = serde_yaml::from_str(&yaml)
        .map_err(|error| format!("{} does not load: {error}", path.display()))?;

    check!(!scenario.name.is_empty());
    // A zero-length measurement window or a zero-byte record would run but
    // measure nothing, so these are load-bearing rather than decorative.
    check!(scenario.duration > Time::ZERO);
    check!(scenario.msg_size > ByteSize::ZERO);
    check!(scenario.batch_size > ByteSize::ZERO);
    check!(scenario.partitions > 0);
    check!(scenario.producers > 0);

    if let LoadMode::FixedRate { rate } = scenario.mode {
        check!(rate > Frequency::ZERO);
    }

    // A kill scheduled at or after the end of the run would never fire.
    if let Some(failover) = &scenario.failover {
        check!(failover.kill_after < scenario.duration + scenario.warmup);
    }

    // The quantities survive a round trip through the operator-facing encoding,
    // so a scenario echoed into a report reads back as the same benchmark.
    let reencoded = serde_yaml::to_string(&scenario)?;
    let reparsed: Scenario = serde_yaml::from_str(&reencoded)?;
    assert!(reparsed == scenario);

    Ok(())
}

datatest_stable::harness! {
    { test = scenario_file, root = scenario_root(), pattern = r".*\.yaml$" },
}
