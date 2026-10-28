//! KIP-1022 "updating features" — JVM acceptance.
//!
//! Drives the real `mirror.gcr.io/apache/kafka:4.3.1` `kafka-features` and
//! `kafka-metadata-quorum` admin tools against an
//! in-process Krabka broker advertised at `host.docker.internal:9092`, proving
//! the `UpdateFeatures` / `ApiVersions` feature surface round-trips end to end:
//!
//! 1. `describe` lists Krabka's finalized features (`metadata.version`,
//!    `group.version`, `transaction.version`) at the self-bootstrap defaults.
//! 2. `downgrade --feature transaction.version=1` is accepted and a follow-up
//!    `describe` reflects the change.
//! 3. `upgrade --feature transaction.version=2` round-trips it back.
//! 4. `kraft.version` upgrades from 0 to 1, survives `describe`, and cannot be
//!    downgraded.
//! 5. `kafka-metadata-quorum describe` reads the exact live voter identity and
//!    `remove-controller` reaches the last-voter safety check.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`. Binds host port
//! 9092, so it must not run concurrently with `jvm_acceptance` (single test
//! here keeps it self-contained).
#![cfg(test)]

mod support;

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{
        fs,
        process::{Command, Output},
        sync::OnceLock,
        time::Duration,
    },
    tokio::time,
};

/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

fn controller_listen() -> &'static str { &support::jvm_listeners().controller }

/// The controller as the containers address it.
fn controller_bootstrap() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    V.get_or_init(|| controller_listen().replace("0.0.0.0", "host.docker.internal"))
}

/// The controller's port on its own, for callers that need the number rather
/// than the address -- a `VoterEndpoint` carries host and port separately.
fn controller_port() -> u16 {
    controller_listen()
        .rsplit(':')
        .next()
        .expect("controller addr has a port")
        .parse()
        .expect("controller port is numeric")
}

/// A second controller port, for the node that joins the quorum.
fn joiner_controller() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    V.get_or_init(|| format!("host.docker.internal:{}", support::free_port()))
}
/// Kafka 4.3.1 is the compatibility oracle for KIP-853 and KIP-1186.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";
const DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(1);
const DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAQ";
const JOINER_DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(2);
const JOINER_DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAg";

/// A running broker and the directory that backs it.
struct StartedBroker {
    handle: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Boot an in-process Krabka broker listening on `listen_addr()`, advertised as
/// `host.docker.internal:9092`. A standalone self-bootstrap finalizes the
/// latest-release feature defaults (metadata.version=25, group.version=1,
/// transaction.version=2).
async fn start_host_broker() -> StartedBroker {
    let (handle, dir) = support::start_jvm_single("krabka_broker=info,warn", |config| {
        config.directory_id = DIRECTORY_ID;
        config.controller_quorum_voters =
            vec![(krabka_broker::NodeId(1), controller_bootstrap().to_owned())];
    })
    .await;
    StartedBroker { handle, dir }
}
/// A running controller observer and the directory that backs it.
struct StartedObserver {
    handle: krabka_raft::ControllerHandle,
    dir: tempfile::TempDir,
}

/// Start a caught-up controller observer without auto-join. The official JVM
/// `add-controller` command promotes this exact live identity.
async fn start_host_observer() -> StartedObserver {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config =
        krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(2), dir.path().to_path_buf());
    config.directory_id = JOINER_DIRECTORY_ID;
    config.controller_listen_addr = joiner_controller()
        .replace("host.docker.internal", "0.0.0.0")
        .parse()
        .expect("allocated addr");
    config.bootstrap_mode = krabka_raft::BootstrapMode::Join;
    config.cluster_id = Some(uuid::Uuid::nil());
    config.initial_voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
        id: krabka_raft::NodeId(1),
        directory_id: DIRECTORY_ID,
        endpoints: vec![krabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: controller_port(),
        }],
        kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
    }]);
    let handle = krabka_raft::Controller::start(config)
        .await
        .expect("start observer");
    StartedObserver { handle, dir }
}

/// Run `kafka-features <args>` from the Kafka oracle container that can
/// reach the host broker via `--add-host=host.docker.internal:host-gateway`.
fn kafka_features(args: &[&str]) -> Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap_addr(),
    ];
    full.extend_from_slice(args);
    let out = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-features");
    eprintln!(
        "KRABKA[test] kafka-features {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Run `kafka-metadata-quorum <args>` against the controller listener.
fn kafka_metadata_quorum(args: &[&str]) -> Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-metadata-quorum.sh",
        "--bootstrap-controller",
        controller_bootstrap(),
    ];
    full.extend_from_slice(args);
    let output = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-metadata-quorum");
    eprintln!(
        "KRABKA[test] kafka-metadata-quorum {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

/// Run Kafka 4.3.1's `add-controller` with a real controller properties file
/// and Kafka-compatible `meta.properties` for node 2.
fn kafka_add_controller() -> Output {
    let mount_dir = tempfile::tempdir().expect("controller command config dir");
    let metadata_dir = mount_dir.path().join("metadata");
    fs::create_dir(&metadata_dir).expect("create metadata dir");
    let joiner = joiner_controller();
    let quorum = controller_bootstrap();
    let properties = format!(
        "process.roles=controller\n\
         node.id=2\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://{joiner}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.bootstrap.servers={quorum}\n\
         log.dirs=/tmp/kraft-controller-2\n"
    );
    let properties_path = mount_dir.path().join("controller.properties");
    fs::write(&properties_path, properties).expect("write controller properties");
    fs::write(
        metadata_dir.join("meta.properties"),
        format!(
            "version=1\ncluster.id=AAAAAAAAAAAAAAAAAAAAAA\nnode.id=2\ndirectory.id={JOINER_DIRECTORY_ID_BASE64}\n"
        ),
    )
    .expect("write meta.properties");
    let properties_mount = format!(
        "{}:/tmp/controller.properties:ro",
        properties_path.display()
    );
    let metadata_mount = format!("{}:/tmp/kraft-controller-2", metadata_dir.display());
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &properties_mount,
            "-v",
            &metadata_mount,
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-metadata-quorum.sh",
            "--bootstrap-controller",
            controller_bootstrap(),
            "--command-config",
            "/tmp/controller.properties",
            "add-controller",
        ])
        .output()
        .expect("spawn docker run kafka-metadata-quorum add-controller");
    eprintln!(
        "KRABKA[test] kafka-metadata-quorum add-controller status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

struct FeatureAdministration {
    broker: Option<StartedBroker>,
    observer: Option<StartedObserver>,
    mutation: Option<Output>,
    description: Option<Output>,
}
#[rstest::fixture]
fn features() -> FeatureAdministration {
    // Start this scenario with fresh inputs and observations.

    FeatureAdministration {
        broker: None,
        observer: None,
        mutation: None,
        description: None,
    }
}
#[rstest_bdd_macros::given("a standalone broker finalizes the latest-release feature defaults")]
async fn latest_features(features: &mut FeatureAdministration) {
    features.broker = Some(start_host_broker().await);
}
#[rstest_bdd_macros::when("the Kafka 4.3.1 feature tool describes finalized features")]
fn describe_features(features: &mut FeatureAdministration) {
    features.description = Some(kafka_features(&["describe"]));
}
#[rstest_bdd_macros::then("feature description succeeds and contains these finalized levels")]
fn described_levels(features: &FeatureAdministration, datatable: Vec<Vec<String>>) {
    let output = features.description.as_ref().expect("features described");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    for row in datatable.into_iter().skip(1) {
        let [feature, level]: [String; 2] = row.try_into().expect("feature level");
        assert!(text.contains(&feature), "{text}");
        if level != "present" {
            assert!(
                support::jvm_finalized_level(&text, &feature)
                    == Some(level.parse().expect("level")),
                "{text}"
            );
        }
    }
}
#[rstest_bdd_macros::when("the feature tool requests {verb} of {feature} to level {level}")]
fn feature_change(features: &mut FeatureAdministration, verb: String, feature: String, level: i64) {
    features.mutation = Some(kafka_features(&[
        &verb,
        "--feature",
        &format!("{feature}={level}"),
    ]));
    features.description = Some(kafka_features(&["describe"]));
}
#[rstest_bdd_macros::then(
    "the feature mutation succeeds and {feature} reads finalized level {level}"
)]
fn mutation_applied(features: &FeatureAdministration, feature: String, level: i64) {
    let output = features.mutation.as_ref().expect("feature mutation");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let description = features.description.as_ref().expect("description");
    let text = String::from_utf8_lossy(&description.stdout);
    assert!(
        support::jvm_finalized_level(&text, &feature) == Some(level),
        "{text}"
    );
}
#[rstest_bdd_macros::then(
    "the feature mutation succeeds and eligible.leader.replicas.version is disabled by an absent or zero level"
)]
fn elr_disabled(features: &FeatureAdministration) {
    assert!(
        features
            .mutation
            .as_ref()
            .expect("mutation")
            .status
            .success()
    );
    let text = String::from_utf8_lossy(&features.description.as_ref().expect("description").stdout);
    assert!(
        support::jvm_finalized_level(&text, "eligible.leader.replicas.version").unwrap_or(0) == 0
    );
}
#[rstest_bdd_macros::then("the irreversible kraft.version downgrade is refused")]
fn irreversible(features: &FeatureAdministration) {
    assert!(
        !features
            .mutation
            .as_ref()
            .expect("mutation")
            .status
            .success()
    );
}
#[rstest_bdd_macros::given(
    "controller observer 2 with directory AAAAAAAAAAAAAAAAAAAAAg has fetched kraft.version 1"
)]
async fn live_observer(features: &mut FeatureAdministration) {
    features.observer = Some(start_host_observer().await);
    time::timeout(
        Duration::from_secs(30),
        features
            .observer
            .as_ref()
            .expect("observer")
            .handle
            .watch_image()
            .wait_for(|image| image.kraft_version() == 1),
    )
    .await
    .expect("observer fetched kraft.version 1")
    .expect("observer image channel");
}
#[rstest_bdd_macros::when("the quorum tool adds controller observer 2 and describes quorum status")]
async fn promote_observer(features: &mut FeatureAdministration) {
    features.mutation = Some(kafka_add_controller());
    features
        .broker
        .as_ref()
        .expect("broker")
        .handle
        .wait_for_image(|image| image.voters().len() == 2)
        .await;
    time::timeout(
        Duration::from_secs(30),
        features
            .observer
            .as_ref()
            .expect("observer")
            .handle
            .watch_image()
            .wait_for(|image| image.voters().len() == 2),
    )
    .await
    .expect("observer committed second voter")
    .expect("observer image channel");
    features.description = Some(kafka_metadata_quorum(&["describe", "--status"]));
}
#[rstest_bdd_macros::then(
    "controller addition succeeds and quorum status names leader 1 and voters 1 and 2 with their configured endpoints"
)]
fn quorum_membership(features: &FeatureAdministration) {
    assert!(
        features
            .mutation
            .as_ref()
            .expect("addition")
            .status
            .success()
    );
    let output = features.description.as_ref().expect("quorum described");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.lines()
            .any(|line| line.starts_with("LeaderId:") && line.ends_with('1'))
    );
    assert!(
        text.lines().any(|line| line.starts_with("CurrentVoters:")
            && line.contains("\"id\": 1")
            && line.contains(&format!("CONTROLLER://{}", controller_bootstrap()))
            && line.contains("\"id\": 2")
            && line.contains(joiner_controller())),
        "{text}"
    );
}
#[rstest_bdd_macros::when(
    "the quorum tool removes controller 2 using directory AAAAAAAAAAAAAAAAAAAAAg"
)]
async fn remove_observer(features: &mut FeatureAdministration) {
    features.mutation = Some(kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "2",
        "--controller-directory-id",
        JOINER_DIRECTORY_ID_BASE64,
    ]));
    features
        .broker
        .as_ref()
        .expect("broker")
        .handle
        .wait_for_image(|image| image.voters().len() == 1)
        .await;
}
#[rstest_bdd_macros::then("controller removal succeeds leaving one committed voter")]
fn one_voter(features: &FeatureAdministration) {
    assert!(
        features
            .mutation
            .as_ref()
            .expect("removal")
            .status
            .success()
    );
    assert!(
        features
            .broker
            .as_ref()
            .expect("broker")
            .handle
            .controller_image_for_test()
            .voters()
            .len()
            == 1
    );
}
#[rstest_bdd_macros::when(
    "the quorum tool removes the last controller 1 using directory AAAAAAAAAAAAAAAAAAAAAQ"
)]
fn remove_last(features: &mut FeatureAdministration) {
    features.mutation = Some(kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "1",
        "--controller-directory-id",
        DIRECTORY_ID_BASE64,
    ]));
}
#[rstest_bdd_macros::then("last-voter removal fails with VoterNotFoundException")]
fn protected_quorum(features: &FeatureAdministration) {
    let output = features.mutation.as_ref().expect("last removal");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("VoterNotFoundException"));
}
async fn shutdown(features: &mut FeatureAdministration) {
    if let Some(StartedObserver { handle, dir }) = features.observer.take() {
        handle.shutdown().await;
        drop(dir);
    }
    let StartedBroker { handle, dir } = features.broker.take().expect("broker");
    handle.shutdown().await;
    drop(dir);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_features.feature",
    name = "kafka features describe and round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_features_describe_and_round_trip(features: FeatureAdministration) {
    shutdown(&mut features).await;
    drop(features);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_features.feature",
    name = "kafka features describes and round trips elr"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_features_describes_and_round_trips_elr(features: FeatureAdministration) {
    shutdown(&mut features).await;
    drop(features);
}
