//! Waiters on the brokers' own metadata image.
//!
//! A JVM tool returns as soon as the controller accepts its request, so a test
//! that asserts on the result first has to wait for the change to reach the
//! image it reads.

pub(crate) use crate::support::partitions::{
    wait_partition_isr_contains as wait_jvm_isr_contains,
    wait_partition_leader as wait_jvm_partition_leader,
};

/// Poll until `handle` reports any non-zero leader for `(topic, partition)`.
/// Returns the leader node id.
pub(crate) async fn wait_jvm_partition_any_leader(
    handle: &krabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
) -> u64 {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader != 0)
        })
        .await;
    handle
        .partition_leader_for_test(topic, partition)
        .expect("non-zero leader present after wait")
}

/// Poll until all three brokers have seen `n_brokers` registered brokers.
pub(crate) async fn wait_three_brokers_registered(
    h1: &krabka_broker::BrokerHandle,
    h2: &krabka_broker::BrokerHandle,
    h3: &krabka_broker::BrokerHandle,
    n_brokers: usize,
) {
    h1.wait_until_brokers_registered(n_brokers).await;
    h2.wait_until_brokers_registered(n_brokers).await;
    h3.wait_until_brokers_registered(n_brokers).await;
}

/// Wait for a local partition's reconciled log configuration under the original 10s/100ms bounds.
pub(crate) async fn wait_jvm_log_config(
    broker: &krabka_broker::BrokerHandle,
    topic: &str,
    ready: impl Fn(&krabka_log::LogConfig) -> bool,
    assert_within_deadline: impl Fn(bool),
) -> krabka_log::LogConfig {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(config) = broker.partition_log_config_for_test(topic, 0)
            && ready(&config)
        {
            return config;
        }
        assert_within_deadline(std::time::Instant::now() <= deadline);
        // Local reconciled LogConfig is not surfaced by an image awaiter or metric.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
