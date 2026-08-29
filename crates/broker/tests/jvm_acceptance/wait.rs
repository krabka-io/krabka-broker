//! Waiters on the brokers' own metadata image.
//!
//! A JVM tool returns as soon as the controller accepts its request, so a test
//! that asserts on the result first has to wait for the change to reach the
//! image it reads.

/// Poll until `handle` reports `leader` as the leader for `(topic, partition)`.
pub(crate) async fn wait_jvm_partition_leader(
    handle: &krabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader == leader)
        })
        .await;
}

/// Poll until the ISR for `(topic, partition)` contains `node`.
pub(crate) async fn wait_jvm_isr_contains(
    handle: &krabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    node: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&krabka_metadata::NodeId(node)))
        })
        .await;
}

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
