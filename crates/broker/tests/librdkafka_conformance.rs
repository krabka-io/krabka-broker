//! The stock librdkafka client against krabka.
//!
//! Only one entry: no published image carries a CLI linked against a current
//! librdkafka. `kcat` has had no release since 1.7.1 and its own build script
//! pins librdkafka 1.8.2, so Confluent's `cp-kcat:8.2.3` -- a 2026 build --
//! still reports `Version 1.7.1 (... librdkafka 1.8.2)`, exactly what
//! `edenhill/kcat:1.7.1` reports. A librdkafka 2.x run would therefore need an
//! image built here rather than a pinned published one; see the note on
//! [`CLIENTS`]. The table stays a table so that adding one is a row.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

use assert2::assert;
use krabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::jvm_acceptance::{broker0_advertised, start_host_broker};

/// One librdkafka client this suite runs the whole round trip against.
struct ClientCase {
    /// The image, pinned in `MODULE.bazel` and named in the
    /// `librdkafka_conformance` entry of the `docker` map in `BUILD.bazel`.
    image: &'static str,
    /// The binary inside it.
    program: &'static str,
    /// How the KIP matrix names this client's library. The generator reads
    /// this field out of the table, so it is the string the matrix shows.
    library: &'static str,
    /// The substring of the client's own `-V` banner that proves it: the
    /// banner is what says which librdkafka the binary is linked against.
    version_marker: &'static str,
}

/// Every librdkafka client this suite drives.
///
/// The 1.x client has neither KIP-848 (`group.protocol=consumer`) nor KIP-714
/// (metrics push), so this suite says nothing about either: it establishes the
/// version-negotiation and flexible-version rows of the KIP matrix and the
/// classic-protocol round trip, and nothing more.
const CLIENTS: [ClientCase; 1] = [ClientCase {
    image: "mirror.gcr.io/edenhill/kcat:1.7.1",
    program: "kcat",
    library: "librdkafka 1.8.2",
    version_marker: "librdkafka 1.8.2",
}];

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
async fn round_trip_group_join_and_api_versions_with_kcat() {
    let (broker, _dir) = start_host_broker().await;
    let bootstrap = broker0_advertised();
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");

    for (
        index,
        ClientCase {
            image,
            program,
            library,
            version_marker,
        },
    ) in CLIENTS.iter().enumerate()
    {
        let version = run_client(image, program, &["-V"], None);
        let version = format!(
            "{}{}",
            String::from_utf8_lossy(&version.stdout),
            String::from_utf8_lossy(&version.stderr)
        );
        assert!(
            version.contains(version_marker),
            "{program} is not linked to {library}: {version}"
        );

        let topic = format!("librdkafka-conformance-{index}");
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: topic.clone(),
                    partitions: 1,
                    replicas: 1,
                    configs: std::collections::BTreeMap::default(),
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
                "-X",
                "enable.auto.offset.store=false",
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
        let trace = String::from_utf8_lossy(&consumed.stderr);
        assert!(
            trace.contains("JoinGroup"),
            "{program} did not join a consumer group: {trace}"
        );
    }

    broker.shutdown().await;
}
