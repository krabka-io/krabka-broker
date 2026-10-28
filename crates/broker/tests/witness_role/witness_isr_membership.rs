//! The visible half of the role: the witness registers with its rack, takes a
//! replica and an ISR seat of an rf=3 partition, and is still never handed to a
//! consumer as a read replica.
//!
//! `Metadata.brokers[]` is what every admin tool resolves replica ids through,
//! so a witness missing from it would break `kafka-topics` and
//! `kafka-reassign-partitions` output. The KIP-392 check belongs with it: a
//! same-rack in-ISR replica is exactly the candidate the rack-aware selector
//! reaches for, which makes this the case where the redirect ban has to hold.

use {
    crate::{
        LEADER_ID, SITE_A, SITE_B, SITE_C, WITNESS_ID, WitnessTopic, witness_topic,
        witness_wire::{PartitionView, consumer_fetch, partition_view},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_protocol::owned::metadata_request::MetadataRequest,
    std::collections::BTreeMap,
};

#[rstest_bdd_macros::when("Metadata lists brokers and describes witness-topic partition 0")]
async fn witness_metadata(witness_topic: &mut WitnessTopic) {
    witness_topic.metadata = Some(
        witness_topic
            .leader()
            .send(MetadataRequest::default())
            .await
            .expect("Metadata for the broker list"),
    );
    witness_topic.partition = Some(partition_view(witness_topic.leader()).await);
}

#[rstest_bdd_macros::then(
    "Metadata lists racks for brokers 1, 2 and witness 3 and the partition is led by 1 with replicas and ISR 1,2,3 and no offline replica"
)]
fn witness_isr_metadata(witness_topic: &WitnessTopic) {
    let racks: BTreeMap<i32, Option<String>> = witness_topic
        .metadata
        .as_ref()
        .expect("metadata read")
        .brokers
        .iter()
        .map(|broker| (broker.node_id, broker.rack.clone()))
        .collect();
    check!(
        racks
            == maplit::btreemap! { 1 => Some(SITE_A.to_owned()), 2 => Some(SITE_B.to_owned()), WITNESS_ID => Some(SITE_C.to_owned()) },
        "every broker, the witness included, is in Metadata.brokers[] with its rack"
    );
    check!(
        witness_topic.partition.as_ref()
            == Some(&PartitionView {
                error_code: codes::NONE,
                partition_index: 0,
                leader_id: LEADER_ID,
                replica_nodes: vec![1, 2, WITNESS_ID],
                isr_nodes: maplit::btreeset! {1,2,WITNESS_ID},
                offline_replicas: vec![]
            }),
        "the witness is a replica and an ISR member; the leader is in the preferred site"
    );
}

#[rstest_bdd_macros::when("a consumer in site C fetches from leader 1")]
async fn witness_rack_fetch(witness_topic: &mut WitnessTopic) {
    witness_topic.fetched = Some(
        witness_topic
            .leader()
            .send(consumer_fetch(witness_topic.topic_id(), SITE_C))
            .await
            .expect("consumer Fetch to the leader with client.rack=site-c"),
    );
}

#[rstest_bdd_macros::then(
    "the consumer receives preferred_read_replica -1 rather than a witness redirect"
)]
fn no_witness_redirect(witness_topic: &WitnessTopic) {
    check!(
        witness_topic
            .fetched
            .as_ref()
            .expect("consumer fetched")
            .responses[0]
            .partitions[0]
            .preferred_read_replica
            == -1,
        "a consumer in the witness site must not be redirected to the witness"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/witness_role/witness_isr_membership.feature",
    name = "witness is a visible isr member that serves no reads"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_is_a_visible_isr_member_that_serves_no_reads(mut witness_topic: WitnessTopic) {
    witness_topic.shutdown().await;
    drop(witness_topic);
}
