//! The consumed approval and the transition it authorizes commit in one raft
//! append, read back out of the committed metadata log.
//!
//! Nothing in the type system holds that rule: each gated handler prepends the
//! consumed record to its own change. The case therefore fetches the log off
//! the controller listener and asserts on the batch, which is the only place
//! a second append would show.

use {
    crate::{
        cluster::boot,
        principals::ALICE,
        proposals::{ACTION_UNREGISTER_BROKER, approved_proposal},
        support::{client::metadata_fetch, records::metadata_batches as decode_metadata_batches},
        transitions::{BROKER_ID, unregister},
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, NodeId, codes},
    krabka_metadata::{
        BreakGlassProposalRecord, MetadataImage, MetadataRecord, UnregisterBrokerRecord,
    },
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
};

/// Read the committed metadata log back off the controller listener from
/// `from`, and answer the records of each batch, batch by batch.
///
/// `image` has to be the image that was current when the records were written:
/// a Kafka metadata record names a topic by id, and translating one back needs
/// an image that still knows the id.
async fn metadata_batches(
    broker: &BrokerHandle,
    from: i64,
    image: &MetadataImage,
) -> Vec<Vec<MetadataRecord>> {
    let response = metadata_fetch(broker.controller_addr(), "break-glass-test", from).await;

    let mut batches = Vec::new();
    for batch in decode_metadata_batches(&response.records) {
        if batch.attributes.is_control_batch() {
            continue;
        }
        batches.push(
            batch
                .records
                .iter()
                .filter_map(|record| record.value.as_ref())
                .filter_map(|value| krabka_metadata::from_kraft_value(value, image).ok())
                .collect(),
        );
    }
    batches
}

/// Whether `record` unregisters a broker.
fn is_unregister(record: &MetadataRecord) -> bool {
    matches!(record, MetadataRecord::V1UnregisterBroker(_))
}

/// Whether `record` is the spent form of proposal `id`.
fn is_consume_of(record: &MetadataRecord, id: WireUuid) -> bool {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => {
            proposal.proposal_id.as_bytes() == &id.0 && proposal.consumed_at_ms != 0
        }
        _ => false,
    }
}

use {
    crate::cluster::Cluster,
    krabka_client_core::Client,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
};

#[derive(Default)]
struct AtomicTransition {
    cluster: Option<Cluster>,
    alice: Option<Client>,
    id: Option<WireUuid>,
    image: Option<Arc<MetadataImage>>,
    from: i64,
    held: Option<BreakGlassProposalRecord>,
    transition: Option<MetadataRecord>,
    result: Option<i16>,
    batches: Vec<Vec<MetadataRecord>>,
}

#[fixture]
fn atomic() -> AtomicTransition {
    // Start this scenario with fresh inputs and observations.

    AtomicTransition {
        cluster: None,
        alice: None,
        id: None,
        image: None,
        from: 0,
        held: None,
        transition: None,
        result: None,
        batches: Vec::new(),
    }
}

#[given("alice has bob and carol's approvals to unregister broker one")]
async fn approved_unregistration(atomic: &mut AtomicTransition) {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let id = approved_proposal(&cluster, ACTION_UNREGISTER_BROKER, &BROKER_ID.to_string()).await;
    let image = cluster.broker.controller_image_for_test();
    atomic.from = i64::try_from(
        cluster
            .broker
            .controller_quorum_state_for_test()
            .last_applied_index,
    )
    .expect("log offset inside i64");
    let held = image
        .break_glass_proposal(uuid::Uuid::from_bytes(id.0))
        .expect("approved proposal in image")
        .clone();
    check!(held.consumed_at_ms == 0);
    let broker_epoch = image.broker_epoch(NodeId(1)).expect("broker1 registered");
    atomic.transition = Some(MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
        node_id: NodeId(1),
        broker_epoch,
    }));
    atomic.held = Some(held);
    atomic.image = Some(image);
    atomic.id = Some(id);
    atomic.alice = Some(alice);
    atomic.cluster = Some(cluster);
}

#[when("alice unregisters broker one and reads the newly committed controller batches")]
async fn unregister_and_read(atomic: &mut AtomicTransition) {
    atomic.result = Some(unregister(atomic.alice.as_ref().unwrap(), BROKER_ID).await);
    let broker = &atomic.cluster.as_ref().unwrap().broker;
    broker
        .wait_for_image(|image| image.broker(NodeId(1)).is_none())
        .await;
    atomic.batches = metadata_batches(broker, atomic.from, atomic.image.as_ref().unwrap()).await;
}

#[then(
    "unregistration succeeds and exactly one append contains consume first, only partition changes, and unregistration last"
)]
fn one_append(atomic: &AtomicTransition) {
    check!(atomic.result == Some(codes::NONE));
    let batches = &atomic.batches;
    let held = atomic.held.as_ref().unwrap().clone();
    let id = atomic.id.unwrap();
    let carrying: Vec<&Vec<MetadataRecord>> = batches
        .iter()
        .filter(|batch| batch.iter().any(is_unregister))
        .collect();
    assert!(
        carrying.len() == 1,
        "exactly one append carries the unregistration, found {}",
        carrying.len()
    );

    let consumed_at_ms = match carrying[0].first() {
        Some(MetadataRecord::V1BreakGlassProposal(proposal)) => proposal.consumed_at_ms,
        other => panic!("the consume leads the append; found {other:?}"),
    };
    check!(consumed_at_ms != 0);
    let consume = MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
        consumed_at_ms,
        ..held
    });
    let transition = atomic.transition.as_ref().unwrap().clone();
    // Kafka's `handleBrokerUnregistered` takes the broker out of every ISR in
    // the same record list, ahead of the `UnregisterBrokerRecord`, so the
    // partition changes sit between the consume and the transition.
    let (leaves, ends) = match carrying[0].as_slice() {
        [first, leaves @ .., last] => (leaves, (first, last)),
        other => panic!("the append holds the consume and the transition; found {other:?}"),
    };
    check!(
        ends == (&consume, &transition),
        "the append starts with the consume and ends with the transition"
    );
    check!(
        leaves.iter().all(|record| matches!(
            record,
            MetadataRecord::V1Partition(_)
                | MetadataRecord::V1PartitionUpdate(_)
                | MetadataRecord::V1PartitionElr(_)
        )),
        "only partition changes sit between them: {leaves:?}"
    );
    check!(
        batches
            .iter()
            .filter(|batch| batch.iter().any(|record| is_consume_of(record, id)))
            .count()
            == 1,
        "the approval is spent in one append and no other"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/atomicity.feature",
    name = "the consume and the transition land in one raft append"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_consume_and_the_transition_land_in_one_raft_append(atomic: AtomicTransition) {
    drop(atomic.alice);
    atomic.cluster.unwrap().broker.shutdown().await;
}
