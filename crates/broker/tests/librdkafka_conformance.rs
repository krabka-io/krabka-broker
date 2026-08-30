//! Stock librdkafka clients against krabka, spanning librdkafka 1.1.0 to 1.8.2.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

use assert2::assert;
use krabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::jvm_acceptance::{broker0_advertised, start_host_broker};

const CLIENTS: [(&str, &str, &str); 2] = [
    (
        "mirror.gcr.io/edenhill/kafkacat:1.5.0",
        "kafkacat",
        "librdkafka 1.1.0",
    ),
    (
        "mirror.gcr.io/edenhill/kcat:1.7.1",
        "kcat",
        "librdkafka 1.8.2",
    ),
];

fn run_client(image: &str, program: &str, args: &[&str], input: Option<&str>) -> Output {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            program,
            image,
        ])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn librdkafka client");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("client stdin")
            .write_all(input.as_bytes())
            .expect("write client stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait for client");
    assert!(
        out.status.success(),
        "{program} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn round_trip_group_join_and_api_versions_across_librdkafka_versions() {
    let (broker, _dir) = start_host_broker().await;
    let bootstrap = broker0_advertised();
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");

    for (index, (image, program, expected_library)) in CLIENTS.iter().enumerate() {
        let version = run_client(image, program, &["-V"], None);
        let version = format!(
            "{}{}",
            String::from_utf8_lossy(&version.stdout),
            String::from_utf8_lossy(&version.stderr)
        );
        assert!(
            version.contains(expected_library),
            "{program} is not linked to {expected_library}: {version}"
        );

        let topic = format!("librdkafka-conformance-{index}");
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: topic.clone(),
                    partitions: 1,
                    replicas: 1,
                    configs: Default::default(),
                }],
                krabka_units::secs(5),
            )
            .await
            .expect("create conformance topic");

        let metadata = run_client(
            image,
            program,
            &[
                "-L",
                "-b",
                bootstrap,
                "-t",
                &topic,
                "-X",
                "api.version.request=true",
                "-d",
                "protocol",
            ],
            None,
        );
        let trace = String::from_utf8_lossy(&metadata.stderr);
        assert!(
            trace.contains("ApiVersionRequest") && trace.contains("ApiVersionResponse"),
            "{program} did not negotiate ApiVersions: {trace}"
        );

        let payload = format!("hello-from-{program}\n");
        run_client(
            image,
            program,
            &["-P", "-b", bootstrap, "-t", &topic, "-p", "0"],
            Some(&payload),
        );

        let group = format!("librdkafka-group-{index}");
        let consumed = run_client(
            image,
            program,
            &[
                "-G",
                &group,
                "-b",
                bootstrap,
                "-X",
                "auto.offset.reset=earliest",
                "-X",
                "enable.auto.commit=false",
                "-c",
                "1",
                "-f",
                "%s\\n",
                "-d",
                "cgrp,protocol",
                &topic,
            ],
            None,
        );
        assert!(
            String::from_utf8_lossy(&consumed.stdout) == payload,
            "{program} round-trip mismatch: {}",
            String::from_utf8_lossy(&consumed.stdout)
        );
        assert!(
            String::from_utf8_lossy(&consumed.stderr).contains("JoinGroup"),
            "{program} did not join a consumer group: {}",
            String::from_utf8_lossy(&consumed.stderr)
        );
    }

    broker.shutdown().await;
}
