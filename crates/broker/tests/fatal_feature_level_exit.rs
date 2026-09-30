//! `krabka-broker` exits with a failure status when its controller replays a
//! feature level that it does not support.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws, and the
//! `ProcessTerminatingFaultHandler` that the controller server installs halts
//! the process with status 1. This suite formats a node at `4.4-IV2`, an
//! unstable `metadata.version` in Kafka 4.3, and boots the binary without
//! `unstable.feature.versions.enable`. The formatted bootstrap records finalize
//! the unstable level as soon as the node leads its own quorum.
//!
//! The binary's stop after a fault that lands once it is running is a unit test
//! beside `serve`, since no command line can commit an unsupported level to a
//! started broker.

use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use assert2::{assert, check};

/// A loopback port that nothing holds at the moment, for the controller
/// listener. `krabka format` records it as the voter's endpoint, so it has to be
/// a known number, and it cannot stay on the binary's default port 9093, which
/// `cli_smoke` binds while this suite may run.
fn free_loopback_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Formats a standalone log directory at `4.4-IV2`, which needs the unstable
/// flag at format time. The voter set records `controller_port` as this node's
/// endpoint, and the broker has to listen there.
///
/// Called in process rather than spawned, as `cli_smoke` does: the formatting
/// is setup, not the thing under test.
fn format_at_unstable_metadata_version(log_dir: &std::path::Path, controller_port: u16) {
    let code = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
        .block_on(krabka_format::run_from_args([
            "krabka-format",
            "--log-dir",
            log_dir.to_str().unwrap(),
            "--standalone",
            "--node-id",
            "1",
            "--controller-listener",
            &format!("127.0.0.1:{controller_port}"),
            "--release-version",
            "4.4-IV2",
            "--unstable-feature-versions-enable",
        ]));
    assert!(code == 0, "krabka-format exited {code}");
}

#[test]
fn the_broker_exits_non_zero_with_kafkas_message_over_an_unsupported_level() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    let controller_port = free_loopback_port();
    format_at_unstable_metadata_version(&log_dir, controller_port);
    // The logs go to a file, not to a pipe that nothing reads while the test
    // polls for the exit.
    let stdout_path = tmp.path().join("stdout.log");

    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka-broker"))
        // The broker binds and registers its own client port, so there is no
        // probe for it to race another process over.
        .arg("--listen-addr=127.0.0.1:0")
        .arg(format!(
            "--controller-listen-addr=127.0.0.1:{controller_port}"
        ))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .arg("--health-listen-addr=none")
        .stdout(Stdio::from(
            std::fs::File::create(&stdout_path).expect("create the stdout log"),
        ))
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn krabka-broker");

    // The broker stops on its own, within seconds: the fault ends its start at
    // once. A hang is the failure, so a deadline turns it into one and kills the
    // process it leaves behind. It is well under the two minutes that a start
    // waits for its first unfence, so a start that ignored the fault fails it.
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll krabka-broker") {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            break None;
        }
        // intentional: waiting on a spawned krabka-broker subprocess to exit;
        // no in-process handle exists to await instead.
        std::thread::sleep(Duration::from_millis(100));
    };
    let output = child
        .wait_with_output()
        .expect("collect krabka-broker output");
    let stderr = String::from_utf8_lossy(&output.stderr);

    let status = status.unwrap_or_else(|| panic!("krabka-broker kept running; stderr:\n{stderr}"));
    // Kafka's `Exit.halt(1)`.
    check!(status.code() == Some(1), "exit status {status}");
    let stdout = std::fs::read_to_string(&stdout_path).expect("read the stdout log");
    check!(
        stdout.contains("halting: the metadata controller met a fatal fault"),
        "stdout:\n{stdout}"
    );
    check!(
        stderr.contains(
            "Encountered fatal fault: Tried to apply FeatureLevelRecord \
             FeatureLevelRecord(name='metadata.version', featureLevel=33), \
             but this controller only supports versions 7-30"
        ),
        "stderr:\n{stderr}"
    );
}
