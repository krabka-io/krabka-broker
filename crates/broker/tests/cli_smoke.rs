use std::process::Command;

use assert2::assert;

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_crabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
}

/// Formats a fresh standalone log directory.
///
/// KIP-853 needs every node formatted before `crabka-broker` boots: the step
/// seeds `meta.properties.json` and the singleton `VotersRecord`, and the broker
/// treats an unformatted dir as operator error and aborts startup.
///
/// Called in process rather than spawned. The formatting is setup for the boot
/// test below, not the thing under test -- `crabka-format`'s own `format_smoke`
/// suite runs the real binary -- and a subprocess would need a Cargo working
/// tree to build from, which a Bazel test sandbox does not have. This test is
/// synchronous, so it drives the async formatter on a current-thread runtime.
fn run_crabka_format(log_dir: &std::path::Path, node_id: u32, controller_listener: &str) {
    let code = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
        .block_on(crabka_format::run_from_args([
            "crabka-format",
            "--log-dir",
            log_dir.to_str().unwrap(),
            "--standalone",
            "--node-id",
            &node_id.to_string(),
            "--controller-listener",
            controller_listener,
        ]));
    assert!(code == 0, "crabka-format exited {code}");
}

#[test]
fn help_mentions_cluster_id_and_advertised_listener() {
    let out = Command::new(broker_bin()).arg("--help").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("--cluster-id"),
        "help missing --cluster-id:\n{help}"
    );
    assert!(
        help.contains("--advertised-listener"),
        "help missing --advertised-listener:\n{help}"
    );
}

#[test]
fn version_returns_zero() {
    let out = Command::new(broker_bin())
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
}

/// Boot `crabka-broker` with `--config-file` set to a minimal TOML, and
/// assert that the process binds the listener declared in the file. The port
/// comes from the file, not from a CLI flag.
#[test]
fn boots_with_config_file_listener() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");

    // KIP-853: the broker refuses to boot an unformatted log dir, so seed
    // it first. `crabka format` creates the directory itself (it must be
    // empty or non-existent), so don't pre-create it.
    run_crabka_format(&log_dir, 1, "127.0.0.1:9093");

    // Pick an ephemeral port by binding briefly, then release it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let cfg_path = tmp.path().join("broker.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(
        f,
        r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "127.0.0.1:{port}"
advertised = "127.0.0.1:{port}"
protocol = "Plaintext"
"#
    )
    .unwrap();

    let mut child = Command::new(broker_bin())
        .arg(format!("--config-file={}", cfg_path.display()))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn crabka-broker");

    // Poll for the port to accept connections within 10 seconds.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            connected = true;
            break;
        }
        // intentional: waiting on a spawned crabka-broker subprocess to bind its
        // TCP listener; no in-process BrokerHandle, image, or metric to await here.
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Tear down before assertions so a hang doesn't leave a stray process.
    let _ = child.kill();
    let _ = child.wait();

    assert!(connected, "broker never opened TCP listener on port {port}");
}

#[test]
fn errors_when_config_file_and_listen_addr_both_set() {
    let out = Command::new(broker_bin())
        .arg("--config-file=/tmp/nonexistent.toml")
        .arg("--listen-addr=127.0.0.1:9092")
        .output()
        .expect("spawn crabka-broker");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("config-file") && stderr.contains("listen-addr"),
        "expected clap mutual-exclusion error, got stderr:\n{stderr}"
    );
}
