use {
    assert2::check,
    krabka_bench_driver::{
        ids::{MessageCount, TimeOffsetMs, WallclockMs},
        report,
        scenario::{
            Acks, Compression, Disturbance, FailoverSpec, LatencyPercentiles, LoadMode, ModeTag,
            Resource, RunOutput, Sample, Scenario, Stack, Throughput, Topology,
        },
    },
    krabka_units::prelude::*,
    std::{path::Path, process::Command},
};

#[test]
fn failover_gate_help_names_required_evidence() {
    let output = Command::new(env!("CARGO_BIN_EXE_krabka-bench-report"))
        .arg("--help")
        .output()
        .expect("run krabka-bench-report --help");

    check!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help output is utf8");

    check!(stdout.contains("rate, drop, latency-spike, and topology evidence"));
}

#[test]
fn failover_gate_exits_nonzero_without_failover_evidence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = dir.path().join("SUMMARY.md");

    let output = Command::new(env!("CARGO_BIN_EXE_krabka-bench-report"))
        .arg("--input-dir")
        .arg(dir.path())
        .arg("--out")
        .arg(&out)
        .arg("--failover-gate")
        .output()
        .expect("run krabka-bench-report --failover-gate");

    check!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    check!(stderr.contains("failover gate: missing failover results"));
}

/// The `RunOutput` the driver writes must survive a round trip through the
/// report binary. The aggregator reads the same JSON encoding that the driver
/// writes, renders the dimensioned values in the operator form, and writes back
/// the unit-named CSV columns.
#[test]
fn report_reads_the_run_output_encoding_and_renders_operator_units() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_run(dir.path(), "krabka-cell-run01.json", Stack::Krabka);
    write_run(dir.path(), "kafka-cell-run01.json", Stack::Kafka);

    let summary = dir.path().join("SUMMARY.md");
    let csv = dir.path().join("summary.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_krabka-bench-report"))
        .arg("--input-dir")
        .arg(dir.path())
        .arg("--out")
        .arg(&summary)
        .arg("--csv")
        .arg(&csv)
        .arg("--strict")
        .output()
        .expect("run krabka-bench-report");

    check!(output.status.success());

    let markdown = std::fs::read_to_string(&summary).expect("summary written");
    // Latencies, sizes and rates all render with their unit attached.
    for needle in [
        "Duration=1m, warmup=10s",
        "| p99 producer ack (lower better) | 4.25ms |",
        "| cgroup working set (lower better) | 300MiB |",
        "| producer msgs/s (higher better) | 10000/s |",
        "| producer byte rate (higher better) | 40MiB/s |",
    ] {
        check!(markdown.contains(needle), "missing {needle:?}");
    }

    let summary_csv = std::fs::read_to_string(&csv).expect("csv written");
    let header = summary_csv.lines().next().expect("header row");
    check!(header.contains("msg_size_bytes"));
    check!(header.contains("producer_p99_ms"));
    // Sizes in bytes, latencies in fractional milliseconds.
    check!(summary_csv.contains(",1024,leader,"));
    check!(summary_csv.contains(",4.250,"));
}

#[test]
fn failover_gate_preserves_summary_on_pass_and_failure() {
    for (recovery_ms, passes) in [(16_000, true), (18_000, false)] {
        let dir = tempfile::tempdir().expect("temp dir");
        for (name, stack, recovery) in [
            ("krabka.json", Stack::Krabka, recovery_ms),
            ("kafka.json", Stack::Kafka, 17_000),
        ] {
            let mut run = run_fixture(stack);
            run.scenario.failover = Some(FailoverSpec {
                kill_after: secs(14),
                target: "partition0_leader".into(),
            });
            run.disturbance = Some(Disturbance {
                kill_at_ms: TimeOffsetMs(14_000),
                recovery_at_ms: TimeOffsetMs(recovery),
                ..Disturbance::default()
            });
            run.samples.push(Sample {
                t_offset_ms: TimeOffsetMs(6_000),
                producer_rate: per_sec(10_000),
                consumer_rate: per_sec(10_000),
                ..Sample::default()
            });
            write_run_output(dir.path(), name, &run);
        }
        let expected = report::render_markdown(dir.path(), true).expect("render summary");
        for gate in [false, true] {
            let summary = dir.path().join("SUMMARY.md");
            let mut command = Command::new(env!("CARGO_BIN_EXE_krabka-bench-report"));
            command
                .arg("--input-dir")
                .arg(dir.path())
                .arg("--out")
                .arg(&summary)
                .arg("--strict");
            if gate {
                command.arg("--failover-gate");
            }
            let output = command.output().expect("run report");
            check!(output.status.success() == (!gate || passes));
            check!(std::fs::read_to_string(&summary).expect("summary written") == expected);
            let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
            check!(stdout.contains("failover gate: PASS") == (gate && passes));
            if gate && !passes {
                let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
                check!(stderr.contains("Krabka recovery 4s is slower than kafka 3s"));
                check!(stderr.contains("failover gate failed with 1 violation(s)"));
            }
        }
    }
}

krabka_macros::benchmark_latency_fixture!(sample_latency);
krabka_macros::benchmark_scenario_fixture!(benchmark_scenario);

fn run_fixture(stack: Stack) -> RunOutput {
    RunOutput {
        scenario: benchmark_scenario("cell", kibibytes(1)),
        stack,
        topology: Topology {
            partitions: 6,
            replication_factor: 1,
            broker_count: 3,
        },
        wallclock_start_unix_ms: WallclockMs(1_700_000_000_000),
        wallclock_end_unix_ms: WallclockMs(1_700_000_060_000),
        throughput: Throughput {
            msgs_produced: MessageCount(600_000),
            msgs_consumed: MessageCount(600_000),
            bytes_in: mebibytes(2400),
            bytes_out: mebibytes(2400),
            producer_rate: per_sec(10_000),
            consumer_rate: per_sec(10_000),
        },
        producer_latency: sample_latency(),
        consumer_e2e_latency: LatencyPercentiles::default(),
        resource: Resource {
            broker_cpu: secs(120),
            mem_cgroup_working_set: mebibytes(300),
            msgs_per_cpu_second: per_sec(5_000),
            ..Resource::default()
        },
        disturbance: None,
        startup: Some(secs(3)),
        first_ack: millis(42),
        errors: vec![],
        notes: vec![],
        samples: vec![Sample {
            t_offset_ms: TimeOffsetMs(0),
            producer_rate: per_sec(10_000),
            consumer_rate: per_sec(10_000),
            producer_p50: micros(1500),
            producer_p99: micros(4250),
            consumer_e2e_p99: millis(7),
        }],
        broker_samples: vec![],
    }
}

fn write_run(dir: &Path, name: &str, stack: Stack) {
    write_run_output(dir, name, &run_fixture(stack));
}

fn write_run_output(dir: &Path, name: &str, run: &RunOutput) {
    let json = serde_json::to_string_pretty(run).expect("encode run output");
    std::fs::write(dir.join(name), json).expect("write run output");
}
