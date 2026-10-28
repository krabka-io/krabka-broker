//! KIP-113 log-directory reporting: `kafka-log-dirs --describe` against a
//! two-directory JBOD broker.
//!
//! This is the one suite here that boots a single host broker rather than the
//! three-broker SASL cluster, because JBOD spread is a per-broker property.

use {
    crate::jvm_acceptance::{
        broker0_advertised, docker_run_kafka_tool, nc_check_connectivity, start_host_broker_jbod,
    },
    assert2::check,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::fs,
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

struct DirectoryReport {
    port_ownership: Option<MutexGuard<'static, ()>>,
    broker: Option<BrokerHandle>,
    primary: Option<TempDir>,
    extra: Option<TempDir>,
    stdout: Option<String>,
}
#[fixture]
fn directories() -> DirectoryReport {
    // Start this scenario with fresh inputs and observations.

    DirectoryReport {
        port_ownership: None,
        broker: None,
        primary: None,
        extra: None,
        stdout: None,
    }
}

#[given("six partitions are materialized across a broker configured with two log directories")]
async fn jbod_topic(directories: &mut DirectoryReport) {
    directories.port_ownership = Some(crate::claim_broker_ports().await);
    let (broker, primary, extra) = start_host_broker_jbod().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "jbodtopic",
        "--partitions",
        "6",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // Wait for the local writer-actor of every partition to materialize on
    // disk before the JVM tool inspects the log dirs.
    for p in 0..6 {
        broker
            .wait_until_local_log_end_offset("jbodtopic", p, 0)
            .await;
    }

    directories.broker = Some(broker);
    directories.primary = Some(primary);
    directories.extra = Some(extra);
}

#[when("the operator describes broker one with kafka log dirs")]
fn inspect(directories: &mut DirectoryReport) {
    let out = docker_run_kafka_tool(&[
        "kafka-log-dirs",
        "--describe",
        "--bootstrap-server",
        broker0_advertised(),
        "--broker-list",
        "1",
    ]);
    directories.stdout = Some(String::from_utf8_lossy(&out.stdout).into_owned());
}

#[then("the report contains both canonical log directories and the topic partitions")]
fn visible_directories(directories: &DirectoryReport) {
    let stdout = directories
        .stdout
        .as_ref()
        .expect("the operator obtained a directory report");
    let primary = directories.primary.as_ref().expect("primary log directory");
    let extra = directories.extra.as_ref().expect("extra log directory");

    // The broker reports canonical absolute host paths; canonicalize the
    // expected dirs so the substring match is robust to /tmp symlinks.
    let primary_path =
        fs::canonicalize(primary.path()).unwrap_or_else(|_| primary.path().to_path_buf());
    let extra_path = fs::canonicalize(extra.path()).unwrap_or_else(|_| extra.path().to_path_buf());

    check!(
        stdout.contains(&primary_path.display().to_string()),
        "kafka-log-dirs output missing primary dir {}; got: {stdout}",
        primary_path.display()
    );
    check!(
        stdout.contains(&extra_path.display().to_string()),
        "kafka-log-dirs output missing extra dir {}; got: {stdout}",
        extra_path.display()
    );
    check!(
        stdout.contains("jbodtopic"),
        "kafka-log-dirs output missing topic partitions; got: {stdout}"
    );
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/log_dirs.feature",
    name = "Operators see both configured JBOD directories"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_kafka_log_dirs_describe_reports_jbod_spread(directories: DirectoryReport) {
    directories.shutdown().await;
    drop(directories);
}

impl DirectoryReport {
    async fn shutdown(&mut self) {
        let port_ownership = self.port_ownership.take();
        self.broker.take().expect("running broker").shutdown().await;
        drop((self.primary.take(), self.extra.take()));
        drop(port_ownership);
    }
}
