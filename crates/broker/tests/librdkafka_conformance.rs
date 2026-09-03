//! Stock librdkafka clients against krabka: the 1.x kcat the project has
//! always run, and a current 2.x build.
//!
//! The 2.x entry is what makes the librdkafka column of the KIP matrix mean
//! anything beyond `ApiVersions`: librdkafka 2.x is the client family behind
//! confluent-kafka-python, confluent-kafka-go and node-rdkafka, and it is
//! where KIP-848 (`group.protocol=consumer`) and KIP-714 (metrics push) are
//! implemented. [`ClientCase::extras`] is what separates the two runs: the
//! 1.x client has neither feature and is exercised on the classic protocol.

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
    /// `-X` settings added to the consume run, and the protocol markers the
    /// debug trace must then carry. Empty for a client too old for them.
    extras: &'static [(&'static str, &'static str)],
}

/// `kcat` has had no release since 1.7.1 (librdkafka 1.8.2, 2021), so the 2.x
/// build comes from Confluent's own `cp-kcat` image, which ships the same
/// program against a current library.
const CLIENTS: [ClientCase; 2] = [
    ClientCase {
        image: "mirror.gcr.io/edenhill/kcat:1.7.1",
        program: "kcat",
        library: "librdkafka 1.8.2",
        version_marker: "librdkafka 1.8.2",
        extras: &[],
    },
    ClientCase {
        image: "mirror.gcr.io/confluentinc/cp-kcat:8.2.3",
        program: "kcat",
        // Only the major version is pinned. The image is a Confluent Platform
        // release rather than a librdkafka one, so its exact library version
        // moves with the platform patch level; what this suite claims is that
        // the client is a 2.x one, which is what makes the KIP-848 and KIP-714
        // assertions below meaningful.
        library: "librdkafka 2.x",
        version_marker: "librdkafka 2.",
        extras: &[
            // KIP-848: the next-generation protocol replaces JoinGroup and
            // SyncGroup with ConsumerGroupHeartbeat.
            ("group.protocol=consumer", "ConsumerGroupHeartbeat"),
            // KIP-714: the client asks the broker for a metrics subscription
            // on connect.
            ("enable.metrics.push=true", "GetTelemetrySubscriptions"),
        ],
    },
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
            extras,
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
        let mut consume_args = vec![
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
        ];
        for (setting, _marker) in *extras {
            consume_args.extend_from_slice(&["-X", setting]);
        }
        consume_args.extend_from_slice(&["-c", "1", "-f", "%s\\n", "-d", "cgrp,protocol", &topic]);
        let consumed = run_client(image, program, &consume_args, None);
        assert!(
            String::from_utf8_lossy(&consumed.stdout) == payload,
            "{program} round-trip mismatch: {}",
            String::from_utf8_lossy(&consumed.stdout)
        );
        let trace = String::from_utf8_lossy(&consumed.stderr);
        // The 1.x client joins on the classic protocol; the 2.x one is asked
        // for `group.protocol=consumer` above, so its own marker replaces
        // `JoinGroup` rather than joining it.
        let joined = if extras.is_empty() {
            trace.contains("JoinGroup")
        } else {
            true
        };
        assert!(joined, "{program} did not join a consumer group: {trace}");
        for (setting, marker) in *extras {
            assert!(
                trace.contains(marker),
                "{program} with -X {setting} did not send {marker}: {trace}"
            );
        }
    }

    broker.shutdown().await;
}
