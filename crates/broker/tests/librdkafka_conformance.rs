//! The librdkafka clients against krabka.
//!
//! Two rows, because one client cannot hold both ends. `kcat` has had no
//! release since 1.7.1 and its own build script pins librdkafka 1.8.2, so
//! every published image of it -- Confluent's `cp-kcat:8.2.3`, a 2026 build,
//! included -- still reports `Version 1.7.1 (... librdkafka 1.8.2)`, exactly
//! what `edenhill/kcat:1.7.1` reports. The librdkafka 2.x row therefore runs
//! an image built here rather than pulled: `//bazel/images:kcat_librdkafka_2x`
//! assembles the same `kcat` 1.7.1 CLI against Wolfi's librdkafka 2.15.0, so
//! its banner still says `Version 1.7.1` and the librdkafka it names is what
//! the row is about. The table stays a table so that adding a client is a row.
#![cfg(test)]

mod jvm_acceptance;
mod support;

use {
    crate::{
        jvm_acceptance::{broker0_advertised, start_host_broker_with},
        support::admin::topic_spec,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_admin::{AdminClient, AdminClientLike as _},
    rstest_bdd::datatable::Rows,
    std::{
        io::Write,
        process::{Command, Output, Stdio},
    },
    tempfile::TempDir,
};

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
    /// Both images run `kcat` 1.7.1, so only this part of the banner tells
    /// them apart, and a silently downgraded image fails the suite here.
    version_marker: &'static str,
}

/// Every librdkafka client this suite drives.
///
/// The 1.x client predates KIP-848 (`group.protocol=consumer`), KIP-714
/// (metrics push) and topic-id Fetch, so it holds the row that a client from
/// before those KIPs still completes the classic round trip against krabka:
/// version negotiation, metadata, produce, and a classic group. The 2.x client
/// is the one the whole confluent-kafka-{python,go,dotnet} family sits on, and
/// [`next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x`] drives it
/// through the paths the older one cannot reach.
const CLIENTS: [ClientCase; 2] = [
    ClientCase {
        image: "mirror.gcr.io/edenhill/kcat:1.7.1",
        program: "kcat",
        library: "librdkafka 1.8.2",
        version_marker: "librdkafka 1.8.2",
    },
    ClientCase {
        image: "docker.io/krabka-io/kcat:librdkafka-2.15.0",
        program: "kcat",
        library: "librdkafka 2.15.0",
        version_marker: "librdkafka 2.15.0",
    },
];

/// The librdkafka 2.x row of [`CLIENTS`].
#[rustfmt::skip]
fn librdkafka_2x() -> &'static ClientCase {
    &CLIENTS[1]
}

fn check_client_version(image: &str, program: &str, library: &str, marker: &str) {
    let version = run_client(image, program, &["-V"], None);
    let version = format!(
        "{}{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr)
    );
    assert!(
        version.contains(marker),
        "{program} is not linked to {library}: {version}"
    );
}

fn check_consumed_payload(output: &Output, expected: &str, program: &str) {
    assert!(
        String::from_utf8_lossy(&output.stdout) == expected,
        "{program} round-trip mismatch: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

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

/// The highest version at which `trace` shows `<request>Request` being sent.
///
/// librdkafka's `protocol` debug writes one `Sent <name>Request (v<version>,
/// ...` line per request, so the negotiated version of a call is observable in
/// the trace and nowhere else in a client run.
fn max_sent_version(trace: &str, request: &str) -> Option<u32> {
    let needle = format!("Sent {request}Request (v");
    trace
        .split(needle.as_str())
        .skip(1)
        .filter_map(|rest| {
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .max()
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct LibraryClientInput {
    image: String,
    program: String,
    library: String,
    version_marker: String,
    topic: String,
    group: String,
    payload: String,
}

struct LibraryClientRun {
    input: LibraryClientInput,
    metadata: Option<Output>,
    produced: Option<Output>,
    consumed: Option<Output>,
}

#[derive(Default)]
struct LibraryConformance {
    host: Option<(BrokerHandle, TempDir)>,
    admin: Option<AdminClient>,
    clients: Vec<LibraryClientRun>,
}

#[rstest::fixture]
fn library_conformance() -> LibraryConformance {
    // Each pinned client speaks to one new broker and its own topic/group.
    LibraryConformance::default()
}

impl LibraryConformance {
    async fn shutdown(self) {
        let Self { host, admin, .. } = self;
        drop(admin);
        let (broker, directory) = host.expect("host broker");
        broker.shutdown().await;
        drop(directory);
    }

    fn produce(&mut self, settings: &[&str]) {
        for client in &mut self.clients {
            let mut args = vec![
                "-P",
                "-b",
                broker0_advertised(),
                "-t",
                &client.input.topic,
                "-p",
                "0",
            ];
            args.extend_from_slice(settings);
            client.produced = Some(run_client(
                &client.input.image,
                &client.input.program,
                &args,
                Some(&format!("{}\n", client.input.payload)),
            ));
        }
    }

    fn consume(&mut self, settings: &[&str]) {
        for client in &mut self.clients {
            let mut args = vec!["-G", &client.input.group, "-b", broker0_advertised()];
            args.extend_from_slice(settings);
            args.extend_from_slice(&[
                "-c",
                "1",
                "-f",
                "%s\\n",
                "-d",
                "cgrp,protocol",
                &client.input.topic,
            ]);
            client.consumed = Some(run_client(
                &client.input.image,
                &client.input.program,
                &args,
                None,
            ));
        }
    }
}

#[rstest_bdd_macros::given("a host broker has client metrics enabled {enabled}")]
async fn library_host_broker(library_conformance: &mut LibraryConformance, enabled: bool) {
    library_conformance.host =
        Some(start_host_broker_with(|config| config.client_metrics_enable = enabled).await);
    library_conformance.admin = Some(
        AdminClient::connect(&[library_conformance
            .host
            .as_ref()
            .expect("host broker")
            .0
            .listen_addr()
            .to_string()])
        .await
        .expect("admin client"),
    );
}

#[rstest_bdd_macros::given(
    "these pinned clients have separate RF1 single-partition topics and consumer groups"
)]
async fn library_client_topics(
    library_conformance: &mut LibraryConformance,
    #[datatable] rows: Rows<LibraryClientInput>,
) {
    for row in rows {
        let pinned = CLIENTS
            .iter()
            .find(|pinned| pinned.image == row.image)
            .expect("a repository-pinned client image");
        assert!(
            pinned.program == row.program
                && pinned.library == row.library
                && pinned.version_marker == row.version_marker,
            "the feature's client metadata agrees with the evidence catalog"
        );
        library_conformance
            .admin
            .as_mut()
            .expect("admin client")
            .create_topics(
                &[topic_spec(row.topic.clone(), 1, 1)],
                krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
            )
            .await
            .expect("create conformance topic");
        library_conformance.clients.push(LibraryClientRun {
            input: row,
            metadata: None,
            produced: None,
            consumed: None,
        });
    }
}

#[rstest_bdd_macros::then(
    "every client version banner contains its declared linked-library marker"
)]
fn pinned_library_versions(library_conformance: &LibraryConformance) {
    for client in &library_conformance.clients {
        check_client_version(
            &client.input.image,
            &client.input.program,
            &client.input.library,
            &client.input.version_marker,
        );
    }
}

#[rstest_bdd_macros::then("the sole client is the pinned librdkafka 2.15.0 client")]
fn modern_pinned_library_client(library_conformance: &LibraryConformance) {
    assert!(library_conformance.clients.len() == 1);
    let client = &library_conformance.clients[0].input;
    let pinned = librdkafka_2x();
    assert!(client.image == pinned.image && client.library == pinned.library);
}

#[rstest_bdd_macros::when(
    "each client requests topic metadata with api.version.request enabled and protocol tracing"
)]
fn library_metadata_requests(library_conformance: &mut LibraryConformance) {
    for client in &mut library_conformance.clients {
        client.metadata = Some(run_client(
            &client.input.image,
            &client.input.program,
            &[
                "-L",
                "-b",
                broker0_advertised(),
                "-t",
                &client.input.topic,
                "-X",
                "api.version.request=true",
                "-d",
                "protocol",
            ],
            None,
        ));
    }
}

#[rstest_bdd_macros::then("every metadata trace contains ApiVersionRequest and ApiVersionResponse")]
fn negotiated_library_versions(library_conformance: &LibraryConformance) {
    for client in &library_conformance.clients {
        let trace =
            String::from_utf8_lossy(&client.metadata.as_ref().expect("metadata requested").stderr);
        assert!(
            trace.contains("ApiVersionRequest") && trace.contains("ApiVersionResponse"),
            "{} did not negotiate ApiVersions: {trace}",
            client.input.program
        );
    }
}

#[rstest_bdd_macros::when("each client produces its declared payload to partition 0")]
fn classic_library_produce(library_conformance: &mut LibraryConformance) {
    library_conformance.produce(&[]);
}

#[rstest_bdd_macros::when(
    "each client consumes one record through a classic group from earliest with auto commit and auto offset store disabled"
)]
fn classic_library_consume(library_conformance: &mut LibraryConformance) {
    library_conformance.consume(&[
        "-X",
        "auto.offset.reset=earliest",
        "-X",
        "enable.auto.commit=false",
        "-X",
        "enable.auto.offset.store=false",
    ]);
}

#[rstest_bdd_macros::then(
    "each consumer prints exactly its declared payload followed by one newline and its trace contains JoinGroup"
)]
fn classic_library_round_trip(library_conformance: &LibraryConformance) {
    for client in &library_conformance.clients {
        let consumed = client.consumed.as_ref().expect("classic group consumed");
        check_consumed_payload(
            consumed,
            &format!("{}\n", client.input.payload),
            &client.input.program,
        );
        let trace = String::from_utf8_lossy(&consumed.stderr);
        assert!(
            trace.contains("JoinGroup"),
            "{} did not join a consumer group: {trace}",
            client.input.program
        );
    }
}

#[rstest_bdd_macros::when(
    "the modern client produces its payload with idempotence, metrics push and protocol tracing enabled"
)]
fn modern_library_produce(library_conformance: &mut LibraryConformance) {
    library_conformance.produce(&[
        "-X",
        "enable.idempotence=true",
        "-X",
        "enable.metrics.push=true",
        "-d",
        "protocol",
    ]);
}

#[rstest_bdd_macros::then(
    "the producer trace sends and receives InitProducerId and GetTelemetrySubscriptions"
)]
fn modern_library_handshakes(library_conformance: &LibraryConformance) {
    for client in &library_conformance.clients {
        let trace = String::from_utf8_lossy(
            &client
                .produced
                .as_ref()
                .expect("modern client produced")
                .stderr,
        );
        assert!(
            trace.contains("Sent InitProducerIdRequest")
                && trace.contains("Received InitProducerIdResponse"),
            "idempotent {} did not acquire a producer id: {trace}",
            client.input.program
        );
        assert!(
            trace.contains("Sent GetTelemetrySubscriptionsRequest")
                && trace.contains("Received GetTelemetrySubscriptionsResponse"),
            "{} opened no telemetry handshake against a client-metrics broker: {trace}",
            client.input.program
        );
    }
}

#[rstest_bdd_macros::when(
    "the modern client consumes one record using group.protocol consumer from earliest with auto commit disabled"
)]
fn modern_library_consume(library_conformance: &mut LibraryConformance) {
    library_conformance.consume(&[
        "-X",
        "group.protocol=consumer",
        "-X",
        "auto.offset.reset=earliest",
        "-X",
        "enable.auto.commit=false",
    ]);
}

#[rstest_bdd_macros::then(
    "the payload matches exactly, the trace sends and receives ConsumerGroupHeartbeat without JoinGroup or SyncGroup, and the highest Fetch version is at least 13"
)]
fn modern_library_protocol(library_conformance: &LibraryConformance) {
    for client in &library_conformance.clients {
        let consumed = client.consumed.as_ref().expect("modern group consumed");
        check_consumed_payload(
            consumed,
            &format!("{}\n", client.input.payload),
            &client.input.program,
        );
        let trace = String::from_utf8_lossy(&consumed.stderr);
        assert!(
            trace.contains("Sent ConsumerGroupHeartbeatRequest")
                && trace.contains("Received ConsumerGroupHeartbeatResponse"),
            "{} did not use the KIP-848 group protocol: {trace}",
            client.input.program
        );
        assert!(
            !trace.contains("Sent JoinGroupRequest") && !trace.contains("Sent SyncGroupRequest"),
            "{} fell back to the classic group protocol: {trace}",
            client.input.program
        );
        let fetch_version = max_sent_version(&trace, "Fetch");
        assert!(
            fetch_version.is_some_and(|version| version >= 13),
            "{} did not send a topic-id Fetch (v13 or above): {fetch_version:?} in {trace}",
            client.input.program
        );
    }
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/librdkafka_conformance.feature",
    name = "round trip group join and api versions with kcat"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_group_join_and_api_versions_with_kcat(library_conformance: LibraryConformance) {
    library_conformance.shutdown().await;
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/librdkafka_conformance.feature",
    name = "next gen group topic ids and telemetry with librdkafka 2x"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x(
    library_conformance: LibraryConformance,
) {
    library_conformance.shutdown().await;
}
