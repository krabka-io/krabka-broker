//! Unit tests for the `ElectLeaders` handler, centred on the KFC-9 break-glass
//! gate.
//!
//! The fixtures build a one-partition topic whose only in-sync replica is dead,
//! so an unclean election always has an out-of-ISR replica to elect and the
//! tests differ only in the approvals the metadata image holds.

use std::collections::HashSet;

use assert2::{assert, check};
use krabka_metadata::{
    BreakGlassAction, BreakGlassProposalRecord, LeaderEpoch, MetadataImage, MetadataRecord, NodeId,
    PartitionRecord, TopicRecord,
};
use krabka_protocol::owned::{
    elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    elect_leaders_response::{self, ElectLeadersResponse, PartitionResult},
};
use uuid::Uuid;

use super::{
    WIRE_ELECTION_PREFERRED, WIRE_ELECTION_UNCLEAN, batch::ElectionBatch, env::ElectionEnv, handle,
    partition::elect_one,
};
use crate::{
    break_glass::gate::tests::approval,
    broker::{Broker, BrokerHandle},
    codes,
    config::BreakGlassConfig,
    handlers::RequestContext,
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::ElectionType,
    test_support::{peer, principal, start_broker_with},
    time_util::now_ms,
};

const TOPIC: &str = "orders";
const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
const VERSION: i16 = elect_leaders_response::MAX_VERSION;

crate::test_support::response_helpers!(
    ElectLeadersResponse,
    version = VERSION,
    client_id = "kafka-leader-election"
);

fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

/// A proposal that two people approved, and that has not expired against
/// the wall clock the gate reads.
fn approved_proposal(target: &str) -> BreakGlassProposalRecord {
    let now = now_ms();
    BreakGlassProposalRecord {
        proposal_id: PROPOSAL,
        action: BreakGlassAction::UncleanElectLeaders,
        target: target.to_owned(),
        proposer: "User:carol".to_owned(),
        reason: "incident 42".to_owned(),
        created_at_ms: now - 1_000,
        expires_at_ms: now + 600_000,
        approvals: vec![approval("User:alice"), approval("User:bob")],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

/// One two-replica partition of [`TOPIC`], led by broker 1, beside the
/// proposals the registry holds.
fn image_with(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_owned(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_owned(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1)],
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }));
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}

/// The leader change that an unclean election of `orders-0` makes: broker 1
/// is in the ISR and dead, broker 2 is alive and out of it.
fn elected() -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.to_owned(),
        partition: 0,
        leader: NodeId(2),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(2)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    }
}

/// Broker 2 alive, broker 1 dead, so an unclean election has an out-of-ISR
/// replica to elect.
async fn liveness() -> ControllerLivenessState {
    let liveness = ControllerLivenessState::new(krabka_units::secs(30));
    liveness.record_heartbeat(2).await;
    liveness
}

async fn broker_with(config: BreakGlassConfig) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(move |cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.break_glass = config;
    })
    .await
}

/// Drive one partition through the election, and answer its row beside the
/// records the request would append.
async fn elect(
    broker: &Broker,
    image: &MetadataImage,
    election: ElectionType,
) -> (PartitionResult, Vec<MetadataRecord>) {
    let liveness = liveness().await;
    let witnesses = HashSet::new();
    let principal = principal("admin");
    let peer = peer();
    let ctx = test_context(&principal, &peer);
    let env = ElectionEnv {
        broker,
        image,
        ctx: &ctx,
        liveness: &liveness,
        witnesses: &witnesses,
        election,
    };
    let mut batch = ElectionBatch::default();
    let row = elect_one(&env, &mut batch, TOPIC, 0).await;
    (row, batch.records)
}

#[tokio::test]
async fn an_unclean_election_with_no_proposal_is_refused_and_appends_nothing() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();

    let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Unclean).await;

    check!(row.error_code == codes::POLICY_VIOLATION);
    check!(
        row.error_message
            == Some("break-glass refused unclean_elect_leaders on orders-0: no approved proposal covers the request".to_owned())
    );
    assert!(records == vec![], "a refused election appends nothing");
    handle.shutdown().await;
}

#[tokio::test]
async fn an_approved_unclean_election_appends_the_consume_beside_the_leader_change() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    let proposal = approved_proposal("orders-0");
    let image = image_with(std::slice::from_ref(&proposal));

    let (row, records) = elect(&broker, &image, ElectionType::Unclean).await;

    check!(row.error_code == codes::NONE);
    // The consume and the transition it authorized are one raft append.
    assert!(records.len() == 2, "{records:?}");
    assert!(let MetadataRecord::V1BreakGlassProposal(consumed) = &records[0]);
    check!(consumed.proposal_id == PROPOSAL);
    check!(consumed.consumed_at_ms != 0, "the approval is spent");
    check!(
        *consumed
            == BreakGlassProposalRecord {
                consumed_at_ms: consumed.consumed_at_ms,
                ..proposal
            }
    );
    check!(records[1] == MetadataRecord::V1Partition(elected()));
    handle.shutdown().await;
}

#[tokio::test]
async fn a_topic_wide_proposal_is_spent_once_for_every_partition_it_covers() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    let image = image_with(&[approved_proposal(TOPIC)]);
    let liveness = liveness().await;
    let witnesses = HashSet::new();
    let principal = principal("admin");
    let peer = peer();
    let ctx = test_context(&principal, &peer);
    let env = ElectionEnv {
        broker: &broker,
        image: &image,
        ctx: &ctx,
        liveness: &liveness,
        witnesses: &witnesses,
        election: ElectionType::Unclean,
    };
    let mut batch = ElectionBatch::default();

    let first = elect_one(&env, &mut batch, TOPIC, 0).await;
    let second = elect_one(&env, &mut batch, TOPIC, 0).await;

    check!(first.error_code == codes::NONE);
    check!(second.error_code == codes::NONE);
    let consumes = batch
        .records
        .iter()
        .filter(|record| matches!(record, MetadataRecord::V1BreakGlassProposal(_)))
        .count();
    check!(
        consumes == 1,
        "one approval is spent once: {:?}",
        batch.records
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn a_preferred_election_is_never_gated() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();

    let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Preferred).await;

    // Broker 1 is already the preferred leader, so the election is not
    // needed. It is never `POLICY_VIOLATION`, which is the point.
    check!(row.error_code == codes::ELECTION_NOT_NEEDED);
    assert!(records == vec![]);
    handle.shutdown().await;
}

#[tokio::test]
async fn a_broker_with_no_approver_set_gates_nothing() {
    let (handle, _dir) = broker_with(BreakGlassConfig::default()).await;
    let broker = handle.broker_arc_for_test();

    let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Unclean).await;

    check!(row.error_code == codes::NONE);
    assert!(records == vec![MetadataRecord::V1Partition(elected())]);
    handle.shutdown().await;
}

#[tokio::test]
async fn the_wire_handler_refuses_an_unclean_election_that_no_proposal_covers() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let ctx = test_context(&principal, &peer);
    let request = |election_type| ElectLeadersRequest {
        election_type,
        topic_partitions: Some(vec![TopicPartitions {
            topic: TOPIC.to_owned(),
            partitions: vec![0],
            ..Default::default()
        }]),
        timeout_ms: 5_000,
        ..Default::default()
    };

    let unclean = handle_request(&broker, request(WIRE_ELECTION_UNCLEAN), &ctx).await;
    let preferred = handle_request(&broker, request(WIRE_ELECTION_PREFERRED), &ctx).await;

    // The gate is an authority gate, so it answers before the broker looks
    // the partition up. A preferred election never reaches it and reports
    // the missing partition instead.
    check!(unclean == codes::POLICY_VIOLATION);
    check!(preferred == codes::UNKNOWN_TOPIC_OR_PARTITION);
    handle.shutdown().await;
}

/// Run the wire handler and answer the one partition row's error code.
async fn handle_request(
    broker: &Broker,
    req: ElectLeadersRequest,
    ctx: &RequestContext<'_>,
) -> i16 {
    let bytes = handle(broker, req, ctx, VERSION).await.expect("handle");
    let response = decode_response(&bytes);
    response.replica_election_results[0].partition_result[0].error_code
}
