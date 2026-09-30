//! `krabka-broker` exits with a failure status when its controller replays a
//! feature level that it does not support.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws, and the
//! `ProcessTerminatingFaultHandler` that the controller server installs halts
//! the process with status 1. This suite formats a node at `4.4-IV2`, an
//! unstable `metadata.version` in Kafka 4.3, and boots the binary without
//! `unstable.feature.versions.enable`. The formatted bootstrap records finalize
//! the unstable level as soon as the node leads its own quorum.

use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use assert2::{assert, check};

const CONTROLLER_LISTENER: &str = "127.0.0.1:9093";

/// Formats a standalone log directory at `4.4-IV2`, which needs the unstable
/// flag at format time.
///
/// Called in process rather than spawned, as `cli_smoke` does: the formatting
/// is setup, not the thing under test.
fn format_at_unstable_metadata_version(log_dir: &std::path::Path) {
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
            CONTROLLER_LISTENER,
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
    format_at_unstable_metadata_version(&log_dir);
    let client_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let mut child = Command::new(env!("CARGO_BIN_EXE_krabka-broker"))
        .arg(format!("--listen-addr=127.0.0.1:{client_port}"))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .arg("--health-listen-addr=none")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn krabka-broker");

    // The broker stops on its own. A hang is the failure, so a deadline turns it
    // into one and kills the process it leaves behind.
    let deadline = Instant::now() + Duration::from_secs(120);
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
    check!(!status.success(), "exit status {status}");
    check!(
        stderr.contains(
            "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', \
             featureLevel=33), but this controller only supports versions 7-30"
        ),
        "stderr:\n{stderr}"
    );
}
