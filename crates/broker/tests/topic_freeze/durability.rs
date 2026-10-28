//! A durable topic freeze must survive the controller that recorded it.

use {
    crate::{
        control_plane::wait_for_registry_len,
        support,
        wire::{
            CONTROL, ProduceOutcome, accepted, frozen_topic_with_record, produce_outcome, refused,
            wait_controlled_partitions,
        },
    },
    assert2::check,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::krabka::freeze::{DescribedTopicFreeze, PATTERN_TYPE_LITERAL},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct DurableFreeze {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Client>,
    entries: Vec<DescribedTopicFreeze>,
    frozen_write: Option<ProduceOutcome>,
    control_write: Option<ProduceOutcome>,
}

#[fixture]
fn durable_freeze() -> DurableFreeze {
    // Each scenario owns fresh services and observations.
    DurableFreeze {
        directory: None,
        ..Default::default()
    }
}

#[given("a controller has durably frozen orders after one acknowledged record")]
async fn recorded_freeze(durable_freeze: &mut DurableFreeze) {
    let directory = tempfile::tempdir().expect("tempdir");
    let (broker, client) = support::start_with_dir(directory.path()).await;
    frozen_topic_with_record(&broker, &client, "orders", "cutover").await;
    durable_freeze.directory = Some(directory);
    durable_freeze.broker = Some(broker);
    durable_freeze.client = Some(client);
}

#[when("the controller restarts from the same data directory")]
async fn restart_frozen_controller(durable_freeze: &mut DurableFreeze) {
    drop(durable_freeze.client.take());
    durable_freeze
        .broker
        .take()
        .expect("the controller is running")
        .shutdown()
        .await;
    let (broker, client) = support::start_with_dir(
        durable_freeze
            .directory
            .as_ref()
            .expect("a durable directory exists")
            .path(),
    )
    .await;
    wait_controlled_partitions(&broker, "orders").await;
    durable_freeze.entries = wait_for_registry_len(&client, 1).await;
    durable_freeze.broker = Some(broker);
    durable_freeze.client = Some(client);
}

#[then("the freeze registry still contains the literal orders scope")]
fn restored_registry(durable_freeze: &DurableFreeze) {
    check!(durable_freeze.entries[0].scope == "orders");
    check!(durable_freeze.entries[0].pattern_type == PATTERN_TYPE_LITERAL);
}

#[when("the client writes to orders and its control after the restart")]
async fn post_restart_writes(durable_freeze: &mut DurableFreeze) {
    let broker = durable_freeze
        .broker
        .as_ref()
        .expect("the controller restarted");
    let client = durable_freeze
        .client
        .as_ref()
        .expect("the client reconnected");
    let frozen = support::topic_id_for(client, "orders").await;
    let control = support::topic_id_for(client, CONTROL).await;
    durable_freeze.frozen_write = Some(produce_outcome(broker, client, "orders", frozen).await);
    durable_freeze.control_write = Some(produce_outcome(broker, client, CONTROL, control).await);
}

#[then("orders remains refused at log end one while its control accepts a record")]
fn durable_refusal(durable_freeze: &DurableFreeze) {
    check!(
        durable_freeze
            .frozen_write
            .as_ref()
            .expect("orders was written")
            == &refused("literal", "orders", "cutover", 1)
    );
    check!(
        durable_freeze
            .control_write
            .as_ref()
            .expect("control was written")
            == &accepted(1)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/durability.feature",
    name = "a freeze survives a controller restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_freeze_survives_a_controller_restart(durable_freeze: DurableFreeze) {
    durable_freeze
        .broker
        .expect("the controller restarted")
        .shutdown()
        .await;
}
