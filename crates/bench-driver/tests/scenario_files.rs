//! Executable BDD specifications for the operator-written benchmark corpus.
//! Both Cargo and Bazel load the actual YAML through the driver's parser.

mod corpus;

use {
    assert2::{assert, check},
    krabka_bench_driver::scenario::{LoadMode, Scenario},
    krabka_units::prelude::*,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::{error::Error, fs, path::PathBuf},
};

struct BenchmarkScenario {
    paths: Vec<PathBuf>,
    loaded: Vec<Scenario>,
}

#[fixture]
fn benchmark() -> BenchmarkScenario {
    BenchmarkScenario {
        paths: Vec::new(),
        loaded: Vec::new(),
    }
}

#[given("benchmark scenario {filename}")]
fn scenario_path(benchmark: &mut BenchmarkScenario, filename: String) -> Result<(), String> {
    benchmark.paths = vec![corpus::bench_dir("scenarios")?.join(filename)];
    Ok(())
}

#[given("all checked in benchmark scenarios")]
fn corpus_paths(benchmark: &mut BenchmarkScenario) -> Result<(), String> {
    // Directory discovery follows Bazel runfile symlinks and fails on an empty
    // corpus. New YAML files remain covered before gaining a named scenario.
    benchmark.paths = corpus::yaml_files("scenarios")?;
    Ok(())
}

#[when("the benchmark YAML is loaded")]
fn load_scenarios(benchmark: &mut BenchmarkScenario) -> Result<(), Box<dyn Error>> {
    assert!(!benchmark.paths.is_empty());
    for path in &benchmark.paths {
        let yaml = fs::read_to_string(path)?;
        let scenario = serde_saphyr::from_str(&yaml)
            .map_err(|error| format!("{} does not load: {error}", path.display()))?;
        benchmark.loaded.push(scenario);
    }
    Ok(())
}

#[then("every benchmark is runnable")]
fn runnable(benchmark: &BenchmarkScenario) {
    assert!(!benchmark.loaded.is_empty());
    for scenario in &benchmark.loaded {
        check!(!scenario.name.is_empty());
        check!(scenario.duration > Time::ZERO);
        check!(scenario.msg_size > ByteSize::ZERO);
        check!(scenario.batch_size > ByteSize::ZERO);
        check!(scenario.partitions > 0);
        check!(scenario.producers > 0);
        if let LoadMode::FixedRate { rate } = scenario.mode {
            check!(rate > Frequency::ZERO);
        }
        if let Some(failover) = &scenario.failover {
            check!(failover.kill_after < scenario.duration + scenario.warmup);
        }
    }
}

#[then("every benchmark round trips through YAML")]
fn round_trip(benchmark: &BenchmarkScenario) -> Result<(), Box<dyn Error>> {
    assert!(!benchmark.loaded.is_empty());
    for scenario in &benchmark.loaded {
        let yaml = serde_saphyr::to_string(scenario)?;
        let reparsed: Scenario = serde_saphyr::from_str(&yaml)?;
        assert!(reparsed == *scenario);
    }
    Ok(())
}
scenarios!("tests/features/scenario_files", fixtures = [benchmark: BenchmarkScenario]);
