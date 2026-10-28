//! KIP-919 `--bootstrap-controller`: the official Kafka admin tools driving
//! krabka's controller listener with no broker endpoint in play.
//!
//! Each tool bootstraps with `DescribeCluster(endpoint_type=CONTROLLERS)`,
//! reads the `ApiVersions` table the controller listener advertises, and then
//! sends its Admin RPCs there. An api key the listener does not route gets no
//! error frame: send a `Metadata` request to a live
//! `mirror.gcr.io/apache/kafka:4.3.1` controller listener and it half-closes,
//! and krabka's does the same. That is why a routing gap surfaces in these
//! tools as a network failure rather than as an unsupported operation, and why
//! these cases assert on the tool exiting zero with the expected output.
//!
//! ## Why not `kafka-topics`
//!
//! `kafka-topics.sh` has no `--bootstrap-controller` option in either pinned
//! image: `mirror.gcr.io/apache/kafka:4.0.0` and `:4.3.1` both exit with
//! `joptsimple.UnrecognizedOptionException` before opening a socket, which
//! [`kafka_topics_has_no_bootstrap_controller_option`] pins. `--describe`
//! would need `Metadata`, whose schema is tagged `broker` only, so no Kafka
//! controller has ever answered it. The five tools that do carry the flag in
//! both images are `kafka-acls`, `kafka-configs`, `kafka-features`,
//! `kafka-metadata-quorum` and `kafka-reassign-partitions`.
//!
//! The JVM `AdminClient` also keeps its own allowlist of calls it will route
//! to a controller, narrower than the listener's advertised set: against a
//! real Kafka 4.3.1 controller, `kafka-configs --bootstrap-controller
//! --entity-type users` fails locally with `UnsupportedEndpointTypeException`
//! even though that controller advertises `AlterUserScramCredentials`. The
//! api keys behind that client-side gate are covered on the wire by
//! `controller_admin_surface` instead; the cases here are the flows a stock
//! Kafka 4.x tool can actually complete.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`.
#![cfg(test)]

mod support;

use {
    crate::support::{controller_voters, init_jvm_tracing, jvm_broker_config},
    assert2::check,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer},
    std::{
        iter,
        process::Command,
        sync::{Arc, OnceLock},
    },
};

/// Kafka 4.3.1 is the compatibility oracle for KIP-919.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The principal an unauthenticated controller-listener connection carries.
/// The ACL case makes it a super-user so `kafka-acls` is exercising the ACL
/// store rather than the cluster-Alter gate in front of it.
const CONTROLLER_PRINCIPAL: &str = "ANONYMOUS";

/// The advertised, listen and controller listen addresses of this test
/// process, allocated once rather than fixed, so this suite can run beside the
/// other container suites.
struct Ports {
    advertised: String,
    listen: String,
    controller: String,
}

fn ports() -> &'static Ports {
    crate::once_value!(Ports, {
        let (client, controller) = (support::free_port(), support::free_port());
        Ports {
            advertised: format!("host.docker.internal:{client}"),
            listen: format!("0.0.0.0:{client}"),
            controller: format!("0.0.0.0:{controller}"),
        }
    })
}

fn advertised_addr() -> &'static str { &ports().advertised }

fn listen_addr() -> &'static str { &ports().listen }

fn controller_listen() -> &'static str { &ports().controller }

/// The controller listener as the CLI containers address it. The voter set
/// carries the same name, because `DescribeCluster` projects the voter
/// endpoints and the tool reconnects to whatever comes back.
fn controller_bootstrap() -> &'static str {
    static ADDR: OnceLock<String> = OnceLock::new();
    ADDR.get_or_init(|| controller_listen().replace("0.0.0.0", "host.docker.internal"))
}

/// A running broker and the directory that backs it.
struct StartedBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Boot an in-process broker whose controller listener the CLI containers
/// reach at [`controller_bootstrap`], letting the caller adjust the config.
async fn start_host_broker_with(adjust: impl FnOnce(&mut BrokerConfig)) -> StartedBroker {
    init_jvm_tracing("krabka_broker=info,warn");
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig {
        controller_quorum_voters: vec![(
            krabka_broker::NodeId(1),
            controller_bootstrap().to_owned(),
        )],
        ..jvm_broker_config()
            .log_dir(dir.path().to_path_buf())
            .listen(listen_addr().parse().expect("allocated addr"))
            .controller(controller_listen().parse().expect("allocated addr"))
            .advertised((advertised_addr()).to_owned())
            .voters(controller_voters(&[(
                1,
                controller_listen().parse().expect("allocated addr"),
            )]))
            .call()
    };
    adjust(&mut config);
    let broker = Broker::start(config).await.expect("start broker");
    StartedBroker { broker, dir }
}

/// Run one Kafka admin tool with `--bootstrap-controller <controller>` from
/// the oracle container, which reaches the host through
/// `--add-host=host.docker.internal:host-gateway`. Captures stdout, stderr
/// and status so each scenario can check its observed controller result.
fn kafka_tool(tool: &str, args: &[&str]) -> Output {
    let binary = format!("/opt/kafka/bin/{tool}.sh");
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        &binary,
        "--bootstrap-controller",
        controller_bootstrap(),
    ];
    full.extend_from_slice(args);
    let output = Command::new("docker")
        .args(&full)
        .output()
        .unwrap_or_else(|error| panic!("spawn docker run {tool}: {error}"));
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    eprintln!(
        "KRABKA[test] {tool} {args:?} status={}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
    output
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
};
const THROTTLE: &str = "leader.replication.throttled.rate=1048576";
const ACL_TOPIC: &str = "controller-acl-topic";
struct ControllerAdministration {
    broker: Option<StartedBroker>,
    commands: Vec<Output>,
    capability: Option<Output>,
    image: Option<&'static str>,
}
#[fixture]
fn admin() -> ControllerAdministration {
    // Start this scenario with fresh inputs and observations.

    ControllerAdministration {
        broker: None,
        commands: Vec::new(),
        capability: None,
        image: None,
    }
}
#[given("a broker exposes its live controller administration listener")]
async fn controller(admin: &mut ControllerAdministration) {
    admin.broker = Some(start_host_broker_with(|_| {}).await);
}
#[given("the live controller listener authorizes its ANONYMOUS principal as an ACL administrator")]
async fn authorized_controller(admin: &mut ControllerAdministration) {
    admin.broker = Some(
        start_host_broker_with(|config| {
            config.authorizer = Arc::new(SimpleAclAuthorizer::new(
                iter::once(CONTROLLER_PRINCIPAL.to_owned()).collect(),
            ));
        })
        .await,
    );
}
#[given("the pinned stock Kafka distribution provides the topic administration tool")]
fn pinned_tool(admin: &mut ControllerAdministration) { admin.image = Some(KAFKA_IMAGE); }
#[when(
    "the operator sets broker one's replication throttle and reads it back through the controller"
)]
fn throttle(admin: &mut ControllerAdministration) {
    admin.commands.push(kafka_tool("kafka-configs", &[
        "--entity-type",
        "brokers",
        "--entity-name",
        "1",
        "--alter",
        "--add-config",
        THROTTLE,
    ]));
    admin.commands.push(kafka_tool("kafka-configs", &[
        "--entity-type",
        "brokers",
        "--entity-name",
        "1",
        "--describe",
    ]));
}
fn successful(commands: &[Output]) {
    check!(!commands.is_empty());
    for output in commands {
        check!(
            output.status.success(),
            "controller tool failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[then("the successful controller config report contains the written replication throttle")]
fn throttle_read(admin: &ControllerAdministration) {
    successful(&admin.commands);
    let stdout = String::from_utf8_lossy(
        &admin
            .commands
            .last()
            .expect("config report captured")
            .stdout,
    );
    check!(
        stdout.contains(THROTTLE),
        "describe missing dynamic config: {stdout}"
    );
}
#[when("the controller ACL tool adds alice's Read binding and lists the topic")]
fn add_acl(admin: &mut ControllerAdministration) {
    admin.commands.push(kafka_tool("kafka-acls", &[
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--topic",
        ACL_TOPIC,
    ]));
    admin
        .commands
        .push(kafka_tool("kafka-acls", &["--list", "--topic", ACL_TOPIC]));
}
#[then("the successful controller ACL listing contains alice's Read grant")]
fn acl_present(admin: &ControllerAdministration) {
    successful(&admin.commands);
    let stdout =
        String::from_utf8_lossy(&admin.commands.last().expect("ACL report captured").stdout);
    check!(
        stdout.contains("User:alice") && stdout.contains("READ"),
        "list missing ACL: {stdout}"
    );
}
#[when("the controller ACL tool removes alice's binding and lists the topic again")]
fn remove_acl(admin: &mut ControllerAdministration) {
    admin.commands.push(kafka_tool("kafka-acls", &[
        "--remove",
        "--force",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--topic",
        ACL_TOPIC,
    ]));
    admin
        .commands
        .push(kafka_tool("kafka-acls", &["--list", "--topic", ACL_TOPIC]));
}
#[then("the successful controller ACL listing no longer contains alice")]
fn acl_absent(admin: &ControllerAdministration) {
    successful(&admin.commands);
    let stdout =
        String::from_utf8_lossy(&admin.commands.last().expect("ACL report captured").stdout);
    check!(
        !stdout.contains("User:alice"),
        "ACL survived removal: {stdout}"
    );
}
#[when("the operator lists reassignments through the controller listener")]
fn reassignments(admin: &mut ControllerAdministration) {
    admin
        .commands
        .push(kafka_tool("kafka-reassign-partitions", &["--list"]));
}
#[then("the successful controller report says no partition reassignments exist")]
fn no_reassignments(admin: &ControllerAdministration) {
    successful(&admin.commands);
    let stdout = String::from_utf8_lossy(
        &admin
            .commands
            .last()
            .expect("reassignment report captured")
            .stdout,
    );
    check!(
        stdout.contains("No partition reassignments found."),
        "unexpected reassignment report: {stdout}"
    );
}
#[when("the operator asks kafka topics to list through bootstrap controller")]
fn topic_capability(admin: &mut ControllerAdministration) {
    admin.capability = Some(
        Command::new("docker")
            .args([
                "run",
                "--rm",
                admin.image.expect("pinned topic tool selected"),
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-controller",
                "127.0.0.1:1",
                "--list",
            ])
            .output()
            .expect("spawn docker run kafka-topics"),
    );
}
#[then("the pinned topic tool reports that bootstrap controller is not a recognized option")]
fn unsupported(admin: &ControllerAdministration) {
    let output = admin
        .capability
        .as_ref()
        .expect("topic capability probe completed");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    check!(
        combined.contains("bootstrap-controller is not a recognized option"),
        "topic tool option changed: {combined}"
    );
}
#[when("the operator describes finalized features through the controller listener")]
fn features(admin: &mut ControllerAdministration) {
    admin
        .commands
        .push(kafka_tool("kafka-features", &["describe"]));
}
#[then("the successful controller report finalizes transaction version at {version}")]
fn finalized(admin: &ControllerAdministration, version: i64) {
    successful(&admin.commands);
    let stdout = String::from_utf8_lossy(
        &admin
            .commands
            .last()
            .expect("feature report captured")
            .stdout,
    );
    check!(
        support::jvm_finalized_level(&stdout, "transaction.version") == Some(version),
        "unexpected feature report: {stdout}"
    );
}
#[when("the operator downgrades transaction version to one through the controller listener")]
fn downgrade(admin: &mut ControllerAdministration) {
    admin.commands.push(kafka_tool("kafka-features", &[
        "downgrade",
        "--feature",
        "transaction.version=1",
        "--unsafe",
    ]));
}
async fn shutdown(admin: ControllerAdministration) {
    if let Some(StartedBroker { broker, dir }) = admin.broker {
        broker.shutdown().await;
        drop(dir);
    }
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_bootstrap_controller.feature",
    name = "Dynamic broker configuration round trips through the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_configs_bootstrap_controller_round_trips_a_dynamic_broker_config(
    admin: ControllerAdministration,
) {
    shutdown(admin).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_bootstrap_controller.feature",
    name = "ACL administration completes its lifecycle through the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_acls_bootstrap_controller_add_list_remove(admin: ControllerAdministration) {
    shutdown(admin).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_bootstrap_controller.feature",
    name = "Reassignment inspection reaches the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_reassign_partitions_bootstrap_controller_lists(admin: ControllerAdministration) {
    shutdown(admin).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_bootstrap_controller.feature",
    name = "The stock topic tool explicitly lacks controller bootstrap support"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_has_no_bootstrap_controller_option(admin: ControllerAdministration) {
    shutdown(admin).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_bootstrap_controller.feature",
    name = "Finalized features can be changed through the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_features_bootstrap_controller_describes_and_downgrades(
    admin: ControllerAdministration,
) {
    shutdown(admin).await;
}
