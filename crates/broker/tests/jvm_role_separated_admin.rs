//! Admin writes against a non-controller node in a role-separated topology.
//!
//! This is the deployment shape `KRaft` assumes and issue #82 names: one
//! controller-only node holding the quorum, plus broker-only nodes that
//! replicate metadata by fetching it and never join the voter set. A client
//! never talks to the controller-only node — it is not in the broker list —
//! so every admin write it sends lands on a node that is not the controller.
//!
//! Both tools here are driven from `mirror.gcr.io/apache/kafka:4.3.1`, the
//! same oracle image `jvm_features` uses, and both send their write to the
//! broker-only node the `Metadata` response names as controller:
//!
//! - `kafka-configs --alter` sends `IncrementalAlterConfigs` (api key 44).
//! - `kafka-features upgrade` and `downgrade` send `UpdateFeatures` (57).
//!
//! Both keys are in `ApiKeys.forwardable`, which is exactly the set a `KRaft`
//! broker wraps in a KIP-590 `Envelope`. A krabka broker-only node reaches its
//! own controller over the krabka-private `SubmitChange` RPC instead, and this
//! suite pins the client-visible outcome of that: the write succeeds and a
//! follow-up `--describe` reads it back. Without a forwarding path of any
//! kind, both tools would fail with `NOT_CONTROLLER`.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`. CI runs it as
//! `//crates/broker:jvm_role_separated_admin_docker_test`.
//!
//! Keeping a broker-only node alive here took two fixes. `324ab7d` moved
//! `BrokerHeartbeat` onto the controller listener, which is the only endpoint
//! a controller-only node publishes for itself, and `4a8e6cb` resolved that
//! leader through its KIP-853 voter endpoint rather than a
//! `BrokerRegistration` a controller-only node never writes. Before them no
//! heartbeat ever left a broker-only node, the controller fenced the whole
//! cluster within one liveness tick, and every tool below failed with
//! `Timed out waiting for a node assignment` before it could say anything
//! about forwarding.
#![cfg(test)]

mod support;

use {
    assert2::{assert, check},
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, config::NodeRole},
    std::{
        array,
        path::Path,
        process::Output,
        time::{Duration, Instant},
    },
    tokio::time,
};

// Kafka 4.3.1 is the compatibility oracle: its `kafka-features.sh` exposes
// the explicit `upgrade` / `downgrade` verbs, and its `AdminClient` routes
// admin writes to whichever node `Metadata` names as controller.

/// One controller-only node and two broker-only nodes, each with a client and
/// a controller port. Allocated once per process so two container suites can
/// run at the same time.
struct Ports {
    /// `0.0.0.0:<port>` bind addresses for the three client listeners.
    client_listen: [String; 3],
    /// `host.docker.internal:<port>` for the same three, as a container sees
    /// them. The broker advertises this name because the `AdminClient`
    /// reconnects after `Metadata` and that connect happens inside the
    /// container.
    client_advertised: [String; 3],
    /// `0.0.0.0:<port>` bind addresses for the three controller listeners.
    controller: [String; 3],
}

fn ports() -> &'static Ports {
    crate::once_value!(Ports, {
        let (_, client_listen, client_advertised) = support::jvm_client_ports::<3>();
        Ports {
            client_listen,
            client_advertised,
            controller: array::from_fn(|_| format!("0.0.0.0:{}", support::free_port())),
        }
    })
}

/// A booted role-separated cluster. Held so the log directories outlive the
/// nodes that write them.
struct RoleSeparated {
    controller: BrokerHandle,
    brokers: Vec<BrokerHandle>,
    _dirs: Vec<tempfile::TempDir>,
}

/// The bootstrap address a container uses. It is a broker-only node, so every
/// request the `AdminClient` sends starts at a node that is not the controller.
fn bootstrap() -> &'static str { &ports().client_advertised[1] }

/// A node config on the shared test timings, with the client listener bound on
/// all interfaces and advertised under the name the containers resolve.
///
/// The base is [`BrokerConfig::for_tests`], the same base every in-process
/// cluster in this suite uses: it carries the short liveness, observer-poll and
/// self-registration timings a multi-node test needs, which
/// `BrokerConfig::default` does not.
fn node_config(index: usize, dir: &tempfile::TempDir, mode: BootstrapMode) -> BrokerConfig {
    let ports = ports();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.broker_id = i32::try_from(index + 1).expect("broker id");
    config.listen_addr = ports.client_listen[index].parse().expect("client addr");
    config
        .advertised_listener
        .clone_from(&ports.client_advertised[index]);
    config.node_id = NodeId(u64::try_from(index + 1).expect("node id"));
    config.controller_listen_addr = ports.controller[index].parse().expect("controller addr");
    // Only node 1 is a voter, so a broker-only node dials it to forward its
    // own registration and every admin write it is handed.
    config.controller_quorum_voters = vec![(
        NodeId(1),
        ports.controller[0].replace("0.0.0.0", "127.0.0.1"),
    )];
    config.bootstrap_mode = mode;
    // `for_tests` sets a two-second broker-heartbeat session timeout, which
    // suits an all-in-process test. A container suite shares the machine with
    // a JVM starting up, so it uses the wider budget every other
    // `jvm_acceptance_*` suite uses; otherwise a broker is marked DEAD, drops
    // out of the broker list, and the tool reports
    // `Timed out waiting for a node assignment` rather than anything about
    // forwarding.
    config.heartbeat_interval = krabka_units::millis(3_000);
    config.heartbeat_timeout = krabka_units::millis(9_000);
    config
}

/// Boot node 1 as the sole voter with `roles = [Controller]`, then nodes 2 and
/// 3 with `roles = [Broker]`. The broker-only nodes observe the metadata log
/// by fetching it; they never join the quorum, so neither of them is ever the
/// active controller.
async fn start_role_separated() -> RoleSeparated {
    support::init_tracing();

    let mut dirs = Vec::with_capacity(3);
    let controller_dir = tempfile::tempdir().expect("tempdir");
    let mut controller_config = node_config(0, &controller_dir, BootstrapMode::Bootstrap);
    controller_config.roles = vec![NodeRole::Controller];
    let controller = Broker::start(controller_config)
        .await
        .expect("start controller-only node");
    dirs.push(controller_dir);
    controller.wait_until_controller_leader().await.unwrap();

    let mut brokers = Vec::with_capacity(2);
    for index in 1..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = node_config(index, &dir, BootstrapMode::Join);
        config.roles = vec![NodeRole::Broker];
        brokers.push(Broker::start(config).await.expect("start broker-only node"));
        dirs.push(dir);
    }

    // A broker-only node self-registers by forwarding the registration to the
    // controller. Wait until the controller's committed image holds both, and
    // then until each observer has replicated that image back — an observer
    // answers `Metadata` out of its own copy, which lags by one fetch.
    controller.wait_until_brokers_registered(2).await;
    for broker in &brokers {
        broker.wait_until_brokers_registered(2).await;
    }

    RoleSeparated {
        controller,
        brokers,
        _dirs: dirs,
    }
}

/// `AdminClient` properties every tool container is given, written once per
/// process into a host directory the containers bind-mount.
///
/// The JVM default `default.api.timeout.ms` is 60 seconds. A CI host running
/// several container suites at once needs longer than that just to start the
/// JVM and complete one metadata round trip, and the `AdminClient` reports
/// running out of it as `Timed out waiting for a node assignment` — a message
/// about its own deadline, indistinguishable from a genuine routing failure.
/// Widening the budget is what makes a real routing failure legible; it is not
/// a retry, and a request that is actually refused still fails at once.
fn command_config() -> &'static Path { support::jvm_admin_config() }

/// Run one bundled Kafka CLI tool against `bootstrap` in a throwaway
/// container, and return its captured output without asserting on the status,
/// so a caller can pin the failure text as well as the success.
///
/// The wait runs on the blocking pool, not on a runtime worker: a CLI
/// container takes tens of seconds, and three in-process brokers share this
/// runtime with a broker-heartbeat session timeout measured in seconds.
/// Blocking a worker for that long marks every broker DEAD and empties the
/// broker list, and the tool then fails for reasons that have nothing to do
/// with forwarding.
async fn kafka_tool(tool: &str, bootstrap: &str, args: &[&str]) -> Output {
    let mount = format!("{}:/krabka-config", command_config().display());
    let mut full = support::jvm_admin_args()
        .mount(&mount)
        .tool(tool)
        .bootstrap(bootstrap)
        .call();
    full.extend(args.iter().map(|arg| (*arg).to_owned()));
    let tool = tool.to_owned();
    let out = support::docker_run_blocking(full, "spawn docker run").await;
    eprintln!(
        "KRABKA[role-separated] {tool} {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

async fn kafka_configs(bootstrap: &str, args: &[&str]) -> Output {
    kafka_tool("/opt/kafka/bin/kafka-configs.sh", bootstrap, args).await
}

async fn kafka_features(bootstrap: &str, args: &[&str]) -> Output {
    kafka_tool("/opt/kafka/bin/kafka-features.sh", bootstrap, args).await
}

fn combined(output: &Output) -> String { support::combined_output(output) }

/// `FinalizedVersionLevel` for `feature` in `kafka-features describe` output.
fn finalized_level(describe_stdout: &str, feature: &str) -> Option<i64> {
    support::jvm_finalized_level(describe_stdout, feature)
}

struct Administration {
    cluster: Option<RoleSeparated>,
    topic: String,
    outputs: Vec<Output>,
    described: Option<Output>,
}
#[rstest::fixture]
fn administration() -> Administration {
    // Start this scenario with fresh inputs and observations.

    Administration {
        cluster: None,
        topic: String::new(),
        outputs: Vec::new(),
        described: None,
    }
}
#[rstest_bdd_macros::given(
    "one controller-only voter and two registered broker-only metadata observers"
)]
async fn roles(administration: &mut Administration) {
    administration.cluster = Some(Box::pin(start_role_separated()).await);
}
#[rstest_bdd_macros::when(
    "the JVM administrator creates single-replica topic {topic} through a broker-only node"
)]
async fn create(administration: &mut Administration, topic: String) {
    administration.outputs.push(
        kafka_tool("/opt/kafka/bin/kafka-topics.sh", bootstrap(), &[
            "--create",
            "--topic",
            &topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ])
        .await,
    );
    administration.topic = topic;
}
#[rstest_bdd_macros::then("every forwarded administration command succeeds")]
fn accepted(administration: &Administration) {
    assert!(!administration.outputs.is_empty());
    for output in &administration.outputs {
        assert!(output.status.success(), "{}", combined(output));
    }
}
#[rstest_bdd_macros::when(
    "the administrator alters topic configuration to {key}={value} through a broker-only node"
)]
async fn configure(administration: &mut Administration, key: String, value: String) {
    administration.outputs.push(
        kafka_configs(bootstrap(), &[
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            &administration.topic,
            "--add-config",
            &format!("{key}={value}"),
        ])
        .await,
    );
    administration.described = Some(
        kafka_configs(bootstrap(), &[
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            &administration.topic,
        ])
        .await,
    );
}
#[rstest_bdd_macros::then("the tool and both broker-only images report {key}={value}")]
async fn replicated_config(administration: &Administration, key: String, value: String) {
    let rendered = combined(
        administration
            .described
            .as_ref()
            .expect("configuration described"),
    );
    check!(rendered.contains(&format!("{key}={value}")), "{rendered}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if administration
            .cluster
            .as_ref()
            .expect("cluster")
            .brokers
            .iter()
            .all(|broker| {
                broker
                    .controller_image_for_test()
                    .topic_config(&administration.topic)
                    .is_some_and(|overrides| {
                        overrides.get(&key).map(String::as_str) == Some(value.as_str())
                    })
            })
        {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "forwarded configuration did not reach both broker-only images"
        );
        time::sleep(Duration::from_millis(200)).await;
    }
}
#[rstest_bdd_macros::when(
    "the administrator requests {verb} of feature {feature} to {level} through a broker-only node"
)]
async fn change_feature(
    administration: &mut Administration,
    verb: String,
    feature: String,
    level: i64,
) {
    administration.outputs.push(
        kafka_features(bootstrap(), &[
            &verb,
            "--feature",
            &format!("{feature}={level}"),
        ])
        .await,
    );
    administration.described = Some(kafka_features(bootstrap(), &["describe"]).await);
}
#[rstest_bdd_macros::then("the feature tool reports finalized {feature} level {level}")]
fn feature_level(administration: &Administration, feature: String, level: i64) {
    let rendered = String::from_utf8_lossy(
        &administration
            .described
            .as_ref()
            .expect("feature described")
            .stdout,
    );
    check!(
        finalized_level(&rendered, &feature) == Some(level),
        "{rendered}"
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_role_separated_admin.feature",
    name = "Broker-only nodes forward configuration and feature administration"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_write_tools_reach_the_controller_from_a_broker_only_node(
    administration: Administration,
) {
    let cluster = administration.cluster.take().expect("cluster");
    for broker in cluster.brokers {
        broker.shutdown().await;
    }
    cluster.controller.shutdown().await;
}
