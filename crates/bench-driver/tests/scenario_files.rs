//! Every checked-in benchmark scenario loads through the driver's own loader.
//!
//! `bench/scenarios/*.yaml` is an operator-written corpus that lives outside
//! this crate, so nothing linked it to the `Scenario` type. When the dimensioned
//! fields grew units, all twelve files stopped parsing and no test noticed.
//! This harness closes that gap. It runs each file through the same `serde_yaml`
//! path that `main.rs` uses, with one nextest process per scenario, so a broken
//! file names itself.

use std::path::Path;

/// Where the scenario corpus is, under whichever runner started this test.
///
/// Cargo runs a test with the crate directory as the working directory and
/// exports `CARGO_MANIFEST_DIR`, so the corpus is two levels up. Bazel does
/// neither: it runs the test from the runfiles root and stages a target's
/// `data` under `$TEST_SRCDIR/$TEST_WORKSPACE/<package>`, which for
/// `//bench:scenarios` is `bench/scenarios`. A single relative path cannot be
/// right for both, and the one that was right for Cargo failed the Bazel
/// coverage run with `NotFound`.
///
/// `crates/broker/tests/support::manifest_dir` resolves the same pair for the
/// container suites' fixtures.
///
/// # Panics
///
/// Panics when neither Cargo's variable nor Bazel's pair is set, which means
/// the test was launched by something that stages data differently again.
fn scenario_root() -> String {
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return format!("{dir}/../../bench/scenarios");
    }
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace =
        std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    format!("{srcdir}/{workspace}/bench/scenarios")
}

use assert2::{assert, check};
use krabka_bench_driver::scenario::{LoadMode, Scenario};
use krabka_units::prelude::*;

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
