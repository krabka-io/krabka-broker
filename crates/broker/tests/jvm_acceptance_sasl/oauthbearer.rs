//! The SASL/OAUTHBEARER produce-and-consume round-trip over a
//! `SASL_PLAINTEXT` listener.
//!
//! OAUTHBEARER keeps its own file because it is the only mechanism whose
//! principal comes from a bearer token rather than from a stored credential:
//! the JVM client mints an `alg:none` JWS and the broker derives the principal
//! from the RFC 7628 client initial response.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, docker_run_kafka_tool_with_mount, nc_check_connectivity,
    oauthbearer_jaas, start_oauthbearer_broker, write_client_props,
};

/// End-to-end `SASL_PLAINTEXT` + OAUTHBEARER drive of the JVM
/// `kafka-topics` / `kafka-console-producer` / `kafka-console-consumer`
/// tools. The JVM client uses the built-in unsecured login module to mint an
/// `alg:none` JWS for `sub=admin`. Krabka parses the RFC 7628 client initial
/// response, validates the token, and derives `User:admin`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_oauthbearer_produce_consume() {
    const TOPIC: &str = "krabka-sasl-oauthbearer-itest";
    const USER: &str = "admin";

    let (broker, _dir) = start_oauthbearer_broker().await;
    nc_check_connectivity();

    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=OAUTHBEARER\n\
         sasl.login.callback.handler.class=\
         org.apache.kafka.common.security.oauthbearer.internals.unsecured.\
         OAuthBearerUnsecuredLoginCallbackHandler\n\
         sasl.jaas.config={}\n",
        oauthbearer_jaas(USER),
    );
    let props_file = write_client_props(&props);
    let mount = props_file.mount_str();

    docker_run_kafka_tool_with_mount(
        &mount,
        &[
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
            "--command-config",
            "/client.properties",
        ],
    );

    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..10)
        .map(|i| format!("msg-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    let consumer_out = docker_run_kafka_tool_with_mount(
        &mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "20000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}
