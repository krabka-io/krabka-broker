//! `kafka-configs --entity-type broker-loggers` — JVM acceptance.
//!
//! Drives the real `mirror.gcr.io/apache/kafka:4.3.1` `kafka-configs.sh`
//! against an in-process krabka broker advertised at `host.docker.internal`,
//! covering the whole operator loop for KIP-412 dynamic log levels:
//!
//! 1. `--describe --all` lists every logger the node has, at
//!    `DYNAMIC_BROKER_LOGGER_CONFIG`.
//! 2. `--describe` *without* `--all` lists the same set. A real
//!    `apache/kafka:4.3.1` broker prints identical bodies under both flags --
//!    only the `Dynamic configs` / `All configs` header differs -- because
//!    `ConfigCommand.getResourceConfig` maps `BROKER_LOGGER` to no
//!    `ConfigSource` at all, so neither path filters entries by source.
//!
//!    What the non-`--all` path adds is an existence probe: before printing
//!    anything it asks `Admin.describeCluster()` whether some node's
//!    `idString` equals the entity name, and prints `The broker-logger '<id>'
//!    doesn't exist and doesn't have dynamic config.` -- exit status 0 -- when
//!    none does. `--all` skips that probe entirely, so the two flags disagree
//!    exactly when the broker list is wrong. 4.3.1's `describeCluster` sends
//!    `DescribeCluster` (api key 60), where every earlier tool sent
//!    `Metadata`, which is why this is the one case that pins krabka's
//!    `DescribeCluster` broker list against a JVM tool.
//! 3. `--alter --add-config` raises a level and `--describe` reads it back.
//! 4. `--alter --delete-config` puts it back and `--describe` confirms.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`.
#![cfg(test)]

mod support;

use {
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, NodeId},
    std::{path::Path, process::Output, sync::OnceLock},
};

// Kafka 4.3.1 is the compatibility oracle: it is the released tool whose
// non-`--all` describe path goes through `DescribeCluster`.

/// The node id the broker runs with, and therefore the only `broker-loggers`
/// resource name it accepts.
const NODE_ID: i32 = 1;

/// The logger this suite moves. It is a crate root named by
/// [`krabka_broker::config::DEFAULT_LOG_FILTER`], so it exists on a stock
/// broker and an alter against it is not a fixture the production default
/// could drift away from.
const LOGGER: &str = "krabka_broker";

/// Client and controller addresses for this test process.
///
/// Ports are allocated rather than fixed so this suite can run beside the
/// other container suites instead of contending for 9092.
fn listeners() -> &'static support::JvmListeners {
    static LISTENERS: OnceLock<support::JvmListeners> = OnceLock::new();
    LISTENERS.get_or_init(support::JvmListeners::allocate)
}

/// A running broker and the directory that backs it.
struct StartedBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Boot a single-node in-process broker that self-bootstraps its own quorum.
async fn start_broker() -> StartedBroker {
    support::init_tracing();
    let dir = tempfile::tempdir().expect("tempdir");
    let listeners = listeners();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.broker_id = NODE_ID;
    config.node_id = NodeId(1);
    config.listen_addr = listeners.listen.parse().expect("client addr");
    config.advertised_listener.clone_from(&listeners.advertised);
    config.controller_listen_addr = listeners.controller.parse().expect("controller addr");
    config.controller_quorum_voters = vec![(
        NodeId(1),
        listeners.controller.replace("0.0.0.0", "127.0.0.1"),
    )];
    // `for_tests` uses a two-second broker-heartbeat session, which suits an
    // all-in-process test. A JVM tool container starting up on a shared runner
    // is slower than that, and a broker that misses the window drops out of
    // the broker list -- which is exactly the signal case 2 below reads, so it
    // must not be able to fire for scheduling reasons.
    config.heartbeat_interval = krabka_units::millis(3_000);
    config.heartbeat_timeout = krabka_units::millis(9_000);
    let handle = Broker::start(config).await.expect("start broker");
    handle.wait_until_controller_leader().await.unwrap();
    handle.wait_until_brokers_registered(1).await;
    StartedBroker {
        broker: handle,
        dir,
    }
}

/// `AdminClient` properties every tool container is given.
///
/// The JVM default `default.api.timeout.ms` is 60 seconds, which a shared CI
/// runner can spend just starting the JVM and completing a metadata round
/// trip. The `AdminClient` reports running out of it as `Timed out waiting for
/// a node assignment`, a message about its own deadline that is
/// indistinguishable from a real routing failure. Widening the budget is what
/// keeps a real failure legible; a request that is actually refused still
/// fails at once.
fn command_config() -> &'static Path { support::jvm_admin_config() }

/// Run `kafka-configs.sh --entity-type broker-loggers --entity-name 1 <args>`
/// in a throwaway container, returning the output without asserting on the
/// status so a caller can pin a failure as well as a success.
///
/// The wait runs on the blocking pool: a CLI container takes tens of seconds,
/// and blocking a runtime worker for that long starves the broker's own
/// heartbeat.
async fn kafka_configs(args: &[&str]) -> Output {
    let mount = format!("{}:/krabka-config", command_config().display());
    let node = NODE_ID.to_string();
    let mut full = support::jvm_admin_args()
        .mount(&mount)
        .bootstrap(&listeners().advertised)
        .call();
    full.extend(
        ["--entity-type", "broker-loggers", "--entity-name", &node]
            .into_iter()
            .map(str::to_owned),
    );
    full.extend(args.iter().map(|arg| (*arg).to_owned()));
    let owned_args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    let out = support::docker_run_blocking(full, "spawn docker run kafka-configs").await;
    eprintln!(
        "KRABKA[broker-loggers] kafka-configs {owned_args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

fn combined(output: &Output) -> String { support::combined_output(output) }

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

struct LoggerAdministration {
    broker: Option<StartedBroker>,
    all: Option<Output>,
    described: Option<Output>,
    altered: Option<Output>,
    after_alter: Option<Output>,
    deleted: Option<Output>,
    after_delete: Option<Output>,
}
#[fixture]
fn logger() -> LoggerAdministration {
    // Start this scenario with fresh inputs and observations.

    LoggerAdministration {
        broker: None,
        all: None,
        described: None,
        altered: None,
        after_alter: None,
        deleted: None,
        after_delete: None,
    }
}
#[given("a registered broker starts with its default INFO logger level")]
async fn registered(logger: &mut LoggerAdministration) {
    logger.broker = Some(start_broker().await);
}
#[when("the operator describes broker loggers with and without the all flag")]
async fn startup_descriptions(logger: &mut LoggerAdministration) {
    logger.all = Some(kafka_configs(&["--describe", "--all"]).await);
    logger.described = Some(kafka_configs(&["--describe"]).await);
}
#[then("both successful reports expose the existing broker logger at INFO")]
fn startup_levels(logger: &LoggerAdministration) {
    for output in [&logger.all, &logger.described] {
        let output = output
            .as_ref()
            .expect("the operator obtained both descriptions");
        assert!(
            output.status.success(),
            "describe failed: {}",
            combined(output)
        );
        let rendered = combined(output);
        assert!(
            !rendered.contains("doesn't exist"),
            "broker-logger reported missing: {rendered}"
        );
        check!(
            rendered.contains(&format!("{LOGGER}=INFO")),
            "startup level missing: {rendered}"
        );
    }
}
#[when("the operator sets the broker logger to DEBUG and reads it back")]
async fn raise_level(logger: &mut LoggerAdministration) {
    logger.altered =
        Some(kafka_configs(&["--alter", "--add-config", &format!("{LOGGER}=DEBUG")]).await);
    logger.after_alter = Some(kafka_configs(&["--describe"]).await);
}
#[then("the alteration succeeds and the next report shows DEBUG")]
fn raised(logger: &LoggerAdministration) {
    let altered = logger.altered.as_ref().expect("alter command completed");
    assert!(
        altered.status.success(),
        "alter failed: {}",
        combined(altered)
    );
    let rendered = combined(logger.after_alter.as_ref().expect("altered level was read"));
    check!(
        rendered.contains(&format!("{LOGGER}=DEBUG")),
        "altered level missing: {rendered}"
    );
}
#[when("the operator deletes the logger override and reads all levels again")]
async fn remove_override(logger: &mut LoggerAdministration) {
    logger.deleted = Some(kafka_configs(&["--alter", "--delete-config", LOGGER]).await);
    logger.after_delete = Some(kafka_configs(&["--describe", "--all"]).await);
}
#[then("the deletion succeeds and the startup INFO level is restored")]
fn restored(logger: &LoggerAdministration) {
    let deleted = logger.deleted.as_ref().expect("delete command completed");
    assert!(
        deleted.status.success(),
        "delete failed: {}",
        combined(deleted)
    );
    let rendered = combined(
        logger
            .after_delete
            .as_ref()
            .expect("restored level was read"),
    );
    check!(
        rendered.contains(&format!("{LOGGER}=INFO")),
        "startup level not restored: {rendered}"
    );
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_broker_loggers.feature",
    name = "Operators inspect override and restore a broker logger level"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_configs_describes_and_alters_broker_loggers(logger: LoggerAdministration) {
    let StartedBroker { broker, dir } = logger.broker.expect("broker started");
    broker.shutdown().await;
    drop(dir);
}
