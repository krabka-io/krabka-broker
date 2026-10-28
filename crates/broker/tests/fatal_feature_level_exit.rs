//! `krabka-broker` exits with a failure status when its controller meets a
//! fatal fault: it replays a feature level that it does not support, or its
//! activation cannot write the bootstrap records.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws, and
//! Kafka's `QuorumController` gives a failed activation to its
//! `fatalFaultHandler`. In both cases the `ProcessTerminatingFaultHandler`
//! that the controller server installs halts the process with status 1. One
//! case formats a node at `4.4-IV2`, an unstable `metadata.version` in Kafka
//! 4.3, and boots the binary without `unstable.feature.versions.enable`. The
//! formatted bootstrap records finalize the unstable level as soon as the node
//! leads its own quorum. The other case formats bootstrap records that create
//! one topic name twice, which the active controller refuses to write.
//!
//! The binary's stop after a fault that lands once it is running is a unit test
//! beside `serve`, since no command line can commit an unsupported level to a
//! started broker.
#![cfg(test)]

use {
    assert2::{assert, check},
    krabka_metadata::{MetadataRecord, TopicRecord},
    std::{
        fs::{self, File},
        net::TcpListener,
        path::Path,
        process::{Command, Stdio},
        thread,
        time::{Duration, Instant},
    },
    tokio::runtime::Builder,
};

/// A loopback port that nothing holds at the moment, for the controller
/// listener. `krabka format` records it as the voter's endpoint, so it has to be
/// a known number, and it cannot stay on the binary's default port 9093, which
/// `cli_smoke` binds while this suite may run.
fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Formats a standalone log directory with the extra `flags`, and `extra`
/// seeded into the bootstrap records. The voter set records `controller_port`
/// as this node's endpoint, and the broker has to listen there.
///
/// Called in process rather than spawned, as `cli_smoke` does: the formatting
/// is setup, not the thing under test.
fn format_standalone(
    log_dir: &Path,
    controller_port: u16,
    flags: &[&str],
    extra: Vec<MetadataRecord>,
) {
    let mut argv: Vec<String> = [
        "krabka-format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        &format!("127.0.0.1:{controller_port}"),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    argv.extend(flags.iter().map(|flag| (*flag).to_owned()));
    let code = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
        .block_on(krabka_format::run_from_args_with_records(argv, extra));
    assert!(code == 0, "krabka-format exited {code}");
}

struct FatalControllerStartup {
    directory: tempfile::TempDir,
    controller_port: u16,
    status: Option<ExitStatus>,
    output: Option<Output>,
    stdout: String,
}
#[rstest::fixture]
fn fatal_startup() -> FatalControllerStartup {
    let directory = tempfile::tempdir().expect("process workspace");
    FatalControllerStartup {
        directory,
        controller_port: free_loopback_port(),
        status: None,
        output: None,
        stdout: String::new(),
    }
}
#[rstest_bdd_macros::given("standalone node 1 is formatted at unstable metadata release 4.4-IV2")]
fn unsupported_controller_bootstrap(fatal_startup: &mut FatalControllerStartup) {
    format_standalone(
        &fatal_startup.directory.path().join("data"),
        fatal_startup.controller_port,
        &[
            "--release-version",
            "4.4-IV2",
            "--unstable-feature-versions-enable",
        ],
        Vec::new(),
    );
}
#[rstest_bdd_macros::given("standalone node 1 is formatted with these bootstrap topic records")]
fn duplicate_controller_bootstrap(
    fatal_startup: &mut FatalControllerStartup,
    datatable: Vec<Vec<String>>,
) {
    let records = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            MetadataRecord::V1Topic(TopicRecord {
                name: cells[0].clone(),
                topic_id: uuid::Uuid::from_u128(cells[1].parse().expect("topic UUID")),
                partitions: cells[2].parse().expect("partitions"),
                replication_factor: cells[3].parse().expect("replication factor"),
            })
        })
        .collect();
    format_standalone(
        &fatal_startup.directory.path().join("data"),
        fatal_startup.controller_port,
        &[],
        records,
    );
}
#[rstest_bdd_macros::when(
    "the broker binary starts node 1 without unstable feature versions enabled"
)]
fn boot_controller_with_fatal_records(fatal_startup: &mut FatalControllerStartup) {
    let stdout_path = fatal_startup.directory.path().join("stdout.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka-broker"))
        .arg("--listen-addr=127.0.0.1:0")
        .arg(format!(
            "--controller-listen-addr=127.0.0.1:{}",
            fatal_startup.controller_port
        ))
        .arg(format!(
            "--log-dir={}",
            fatal_startup.directory.path().join("data").display()
        ))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .arg("--health-listen-addr=none")
        .stdout(Stdio::from(File::create(&stdout_path).expect("stdout log")))
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn broker");
    let deadline = Instant::now() + Duration::from_secs(60);
    fatal_startup.status = loop {
        if let Some(status) = child.try_wait().expect("poll broker") {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            break None;
        }
        // This test waits for an external process, which has no async broker handle.
        thread::sleep(Duration::from_millis(100));
    };
    fatal_startup.output = Some(child.wait_with_output().expect("broker process output"));
    fatal_startup.stdout = fs::read_to_string(stdout_path).expect("stdout log");
}
#[rstest_bdd_macros::then(
    "the broker exits within 60 seconds with status 1 and reports a fatal controller halt"
)]
fn fatal_controller_exit(fatal_startup: &FatalControllerStartup) {
    let output = fatal_startup.output.as_ref().expect("broker was started");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let status = fatal_startup
        .status
        .unwrap_or_else(|| panic!("broker kept running; stderr:\n{stderr}"));
    check!(status.code() == Some(1), "exit status {status}");
    check!(
        fatal_startup
            .stdout
            .contains("halting: the metadata controller met a fatal fault"),
        "stdout:\n{}",
        fatal_startup.stdout
    );
}
#[rstest_bdd_macros::then("stderr contains the Kafka fatal fault \"{fault}\"")]
fn fatal_controller_diagnostic(fatal_startup: &FatalControllerStartup, fault: String) {
    let stderr = String::from_utf8_lossy(
        &fatal_startup
            .output
            .as_ref()
            .expect("broker process output")
            .stderr,
    );
    check!(stderr.contains(&fault), "stderr:\n{stderr}");
}
rstest_bdd_macros::scenarios!("tests/features/controller/fatal_startup.feature", fixtures = [fatal_startup: FatalControllerStartup]);

use std::process::{ExitStatus, Output};
