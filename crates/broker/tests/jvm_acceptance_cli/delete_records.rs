//! The `kafka-delete-records` tool, which trims a log through `DeleteRecords`
//! and reports the new low watermark.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, docker_run_kafka_tool, nc_check_connectivity,
    start_host_broker,
};

/// `kafka-delete-records --offset-json-file <(...)`: produce 20
/// records, trim to offset 10, expect success + `low_watermark`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trims_log() {
    const TOPIC: &str = "krabka-delete-recs-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // Produce 20 records via console-producer stdin.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        for i in 0..20 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(child.stdin.take());
    let prod_out = child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // Build offset-json on the host so we can pass it into the container.
    // The cp-kafka container runs as a non-root user; on Linux,
    // `tempfile::NamedTempFile` creates the file 0600, so the bind-mount is
    // unreadable inside the container. Relax to 0644 so the container's uid
    // can read it. WSL/Docker-Desktop ignores this, but native Linux CI
    // enforces it strictly.
    let json = format!(
        r#"{{"partitions":[{{"topic":"{TOPIC}","partition":0,"offset":10}}],"version":1}}"#
    );
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), &json).expect("write json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod offsets.json");
    }
    let host_path = tmp.path().to_path_buf();
    let mount = format!("{}:/offsets.json:ro", host_path.display());

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-delete-records",
            "--bootstrap-server",
            broker0_advertised(),
            "--offset-json-file",
            "/offsets.json",
        ])
        .output()
        .expect("spawn delete-records");
    assert!(
        out.status.success(),
        "delete-records failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("low_watermark") || s.contains("10"),
        "delete-records output missing low_watermark: {s}"
    );
}
