//! Every checked-in benchmark scenario loads through the driver's own loader.
//!
//! `bench/scenarios/*.yaml` is an operator-written corpus that lives outside
//! this crate, so nothing linked it to the `Scenario` type. When the dimensioned
//! fields grew units, all twelve files stopped parsing and no test noticed.
//! This harness closes that gap. It runs each file through the same `serde_saphyr`
//! path that `main.rs` uses, with one nextest process per scenario, so a broken
//! file names itself.
//!
//! The harness is `libtest-mimic` directly rather than `datatest-stable`, which
//! wraps it. `datatest-stable` walks the corpus with `walkdir` and keeps only
//! entries whose `file_type()` is a file. It does not follow links, and Bazel
//! stages every runfile as a symbolic link, so under Bazel it saw no scenarios
//! at all:
//!
//! ```text
//! no test cases found for test 'scenario_file' -- scanned directory:
//!   `.../scenario_files_test.runfiles/_main/bench/scenarios`
//! ```
//!
//! `scenario_paths` lists the directory itself and asks `Path::is_file`, which
//! follows the link, so both runners see the same twelve files.

mod corpus;

use {
    assert2::{assert, check},
    krabka_bench_driver::scenario::{LoadMode, Scenario},
    krabka_units::prelude::*,
    std::{path::Path, process::ExitCode},
};

/// Loads one scenario file and checks it describes a runnable benchmark.
fn scenario_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string(path)?;
    let scenario: Scenario = serde_saphyr::from_str(&yaml)
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
    let reencoded = serde_saphyr::to_string(&scenario)?;
    let reparsed: Scenario = serde_saphyr::from_str(&reencoded)?;
    assert!(reparsed == scenario);

    Ok(())
}

fn main() -> ExitCode {
    let args = libtest_mimic::Arguments::from_args();
    let trials = corpus::yaml_files("scenarios")
        .into_iter()
        .map(|path| {
            // The same `scenario_file::<file>` names `datatest-stable` produced,
            // so nextest filters and CI history keep matching.
            let file = path
                .file_name()
                .map_or_default(|name| name.to_string_lossy().into_owned());
            libtest_mimic::Trial::test(format!("scenario_file::{file}"), move || {
                scenario_file(&path).map_err(libtest_mimic::Failed::from)
            })
        })
        .collect();
    libtest_mimic::run(&args, trials).exit_code()
}
