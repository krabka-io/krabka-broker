//! The delivery watermark is reconstructed from retained batch timestamps after restart.
use {
    crate::{
        dat_fixtures::{DeliveryScenario, delivery},
        dat_wire::wait_for_delivery_policy,
        support,
    },
    krabka_broker::NodeId,
};
#[rstest_bdd_macros::when("the broker shuts down and restarts from the same log directory")]
async fn restart_delivery_log(delivery: &mut DeliveryScenario) {
    drop(delivery.client.take());
    delivery
        .broker
        .take()
        .expect("delivery broker started")
        .shutdown()
        .await;
    let (broker, client) = support::start_with_dir(
        delivery
            .directory
            .as_ref()
            .expect("delivery directory retained")
            .path(),
    )
    .await;
    broker
        .wait_until_local_partition_leader(&delivery.topic, 0, NodeId(1))
        .await;
    wait_for_delivery_policy(
        &broker,
        &delivery.topic,
        delivery.mode.expect("delivery mode configured"),
    )
    .await;
    delivery.topic_id = Some(support::topic_id_for(&client, &delivery.topic).await);
    delivery.broker = Some(broker);
    delivery.client = Some(client);
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_restart.feature",
    name = "the delivery watermark survives a broker restart"
)]
#[tokio::test]
async fn the_delivery_watermark_survives_a_broker_restart(
    delivery: DeliveryScenario,
    mode: String,
    topic: String,
    latest: i64,
    values: String,
) {
    let _ = (mode, topic, latest, values);
    delivery.shutdown().await;
}
