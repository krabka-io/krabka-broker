//! Tests 1 and 2: `IncrementalAlterConfigs` writes a throttle key and the
//! committed metadata image reports it back.
//!
//! Both run on the `SASL_PLAINTEXT` cluster, because a config alter is an
//! authorized operation and the suite wants a named super user driving it.
//! One test sets the broker-scoped rate, the other the topic-scoped replica
//! list.

use {
    crate::{
        CLIENT_ID,
        cluster::{
            create_topic_as_admin, leader_rate_matches,
            start_single_broker_sasl_plaintext_with_users, wait_partition_exists,
        },
        configs::drive_incremental_alter_configs,
        support::topics::configured_topic_setup,
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, throttle::TopicThrottle},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::net::SocketAddr,
    tempfile::TempDir,
};

#[derive(Default)]
struct ThrottleConfiguration {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    error: Option<i16>,
}

#[fixture]
fn throttle_configuration() -> ThrottleConfiguration {
    // Each configuration mutation starts with a fresh authorized broker.
    ThrottleConfiguration::default()
}

impl ThrottleConfiguration {
    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("authorized broker running") }

    async fn shutdown(&mut self) {
        self.broker
            .take()
            .expect("authorized broker running")
            .shutdown()
            .await;
        self.directory.take();
    }
}

#[given("an authenticated replication administrator")]
async fn replication_administrator(throttle_configuration: &mut ThrottleConfiguration) {
    let (broker, directory, address) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;
    throttle_configuration.broker = Some(broker);
    throttle_configuration.directory = Some(directory);
    throttle_configuration.address = Some(address);
}

#[given("an administered topic foo with partition 0")]
async fn throttle_topic(throttle_configuration: &mut ThrottleConfiguration) {
    create_topic_as_admin()
        .addr(throttle_configuration.address.expect("listener"))
        .client_id(CLIENT_ID)
        .topic(configured_topic_setup().name("foo").call())
        .call()
        .await;
    wait_partition_exists(throttle_configuration.broker(), "foo", 0).await;
}

#[when("the administrator sets the broker leader replication rate to 2048 bytes per second")]
async fn alter_broker_rate(throttle_configuration: &mut ThrottleConfiguration) {
    let node_id = throttle_configuration.broker().node_id();
    throttle_configuration.error = Some(
        drive_incremental_alter_configs(
            throttle_configuration.address.expect("listener"),
            "admin",
            "admin-secret",
            vec![(4, node_id.to_string(), vec![(
                "leader.replication.throttled.rate".into(),
                Some("2048".into()),
                0,
            )])],
        )
        .await,
    );
}

#[when("the administrator throttles foo partition 0 on brokers 1 and 2")]
async fn alter_topic_replicas(throttle_configuration: &mut ThrottleConfiguration) {
    throttle_configuration.error = Some(
        drive_incremental_alter_configs(
            throttle_configuration.address.expect("listener"),
            "admin",
            "admin-secret",
            vec![(2, "foo".into(), vec![(
                "leader.replication.throttled.replicas".into(),
                Some("0:1,0:2".into()),
                0,
            )])],
        )
        .await,
    );
}

#[then("the throttle alteration succeeds")]
fn throttle_altered(throttle_configuration: &ThrottleConfiguration) {
    assert!(
        throttle_configuration.error == Some(0),
        "alter should succeed: {:?}",
        throttle_configuration.error
    );
}

#[then("the committed broker image reports a leader replication rate of 2048 bytes per second")]
async fn broker_rate_visible(throttle_configuration: &ThrottleConfiguration) {
    let node_id = throttle_configuration.broker().node_id();
    throttle_configuration
        .broker()
        .wait_for_image(|image| leader_rate_matches(image, node_id, 2048))
        .await;
}

#[then("the committed topic throttle applies to brokers 1 and 2 and excludes broker 3")]
async fn topic_replicas_visible(throttle_configuration: &ThrottleConfiguration) {
    throttle_configuration
        .broker()
        .wait_for_image(|image| {
            let throttles_partition = |broker| {
                TopicThrottle::for_topic(image, "foo", krabka_broker::NodeId(broker))
                    .leader
                    .contains(0)
            };
            throttles_partition(1) && throttles_partition(2) && !throttles_partition(3)
        })
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/config_propagation.feature",
    name = "broker scoped alter persists in image"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_scoped_alter_persists_in_image(throttle_configuration: ThrottleConfiguration) {
    throttle_configuration.shutdown().await;
    drop(throttle_configuration);
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/config_propagation.feature",
    name = "topic throttle config propagates"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_throttle_config_propagates(throttle_configuration: ThrottleConfiguration) {
    throttle_configuration.shutdown().await;
    drop(throttle_configuration);
}
