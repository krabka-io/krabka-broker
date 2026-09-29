//! Unit tests for the `UnregisterBroker` handler.
//!
//! Two kinds sit here. The wire tests drive `handle` against a live in-process
//! broker and compare the whole decoded response, because the shape of a
//! refusal is as much the contract as its error code. The gate tests call
//! `unregister_records` directly, where the break-glass decision is a pure
//! function of the metadata image, the approver set, and the clock.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_metadata::{
    BreakGlassProposalRecord, MetadataImage, MetadataRecord, UnregisterBrokerRecord,
};
use krabka_protocol::owned::unregister_broker_response::{self, UnregisterBrokerResponse};
use krabka_security::Principal;
use uuid::Uuid;

use super::*;
use crate::{
    authorizer::Authorizer, break_glass::gate::tests::approval, broker::BrokerHandle,
    config::BreakGlassConfig, test_support::DenyAll,
};

fn encode_request(req: &UnregisterBrokerRequest, version: i16) -> Bytes {
    crate::test_support::encode_request(req, version)
}

fn decode_response(bytes: &Bytes) -> UnregisterBrokerResponse {
    crate::test_support::decode_response(bytes, unregister_broker_response::MAX_VERSION)
}

fn principal() -> Principal {
    crate::test_support::principal("admin")
}

fn context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "unregister-client")
}

async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

#[test]
fn response_preserves_error_fields_and_throttle() {
    let resp = response(codes::UNKNOWN_SERVER_ERROR, Some("submit failed".into()));

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::UNKNOWN_SERVER_ERROR,
        error_message: Some("submit failed".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_with_message_and_throttle() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("unregister-broker denied".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// An id with no registration, a negative one included, answers
/// `BROKER_ID_NOT_REGISTERED` with the message of Kafka's
/// `ReplicationControlManager.unregisterBroker`.
#[tokio::test]
async fn handle_answers_broker_id_not_registered_for_unknown_ids() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);

    for broker_id in [-1, 0, 999] {
        let req = UnregisterBrokerRequest {
            broker_id,
            ..Default::default()
        };
        let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::BROKER_ID_NOT_REGISTERED,
            error_message: Some(format!("Broker ID {broker_id} is not currently registered")),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        check!(resp == expected, "broker_id {broker_id}");
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unregisters_registered_broker_with_success_shape() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
const DOOMED: NodeId = NodeId(7);
/// The epoch of the registration an unregistration of [`DOOMED`] removes.
const DOOMED_EPOCH: i64 = 42;

fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

/// A proposal that two people approved, and that has not expired.
fn approved_proposal(target: &str) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: PROPOSAL,
        action: BreakGlassAction::UnregisterBroker,
        target: target.to_owned(),
        proposer: "User:carol".to_owned(),
        reason: "broker 7 is never coming back".to_owned(),
        created_at_ms: 1_000,
        expires_at_ms: 600_000,
        approvals: vec![approval("User:alice"), approval("User:bob")],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

fn image_of(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}

const NOW_MS: i64 = 60_000;

#[test]
fn an_unregistration_with_no_proposal_appends_nothing() {
    let image = image_of(&[]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect_err("no proposal covers broker 7");

    check!(denial.action == BreakGlassAction::UnregisterBroker);
    check!(denial.target == "7");
    check!(
        denial.to_string()
            == "break-glass refused unregister_broker on 7: no approved proposal covers the request"
    );
}

#[test]
fn an_approved_unregistration_appends_the_consume_beside_the_unregister() {
    let proposal = approved_proposal("7");
    let image = image_of(std::slice::from_ref(&proposal));

    let records = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect("the proposal authorizes the unregistration");

    let expected = vec![
        MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
            consumed_at_ms: NOW_MS,
            ..proposal
        }),
        MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
            node_id: DOOMED,
            broker_epoch: DOOMED_EPOCH,
        }),
    ];
    assert!(records == expected);
}

#[test]
fn a_proposal_for_another_broker_does_not_cover_this_one() {
    let image = image_of(&[approved_proposal("8")]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect_err("a proposal for broker 8 authorizes nothing about broker 7");

    check!(denial.proposal_id() == None);
}

#[test]
fn a_broker_with_no_approver_set_gates_nothing() {
    let records = unregister_records(
        &image_of(&[]),
        &BreakGlassConfig::default(),
        DOOMED,
        DOOMED_EPOCH,
        NOW_MS,
    )
    .expect("an ungated broker unregisters with no proposal");

    assert!(
        records
            == vec![MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: DOOMED,
                broker_epoch: DOOMED_EPOCH,
            })]
    );
}

/// The `break_glass_refusals` count for this action.
fn refusals(metrics: &crate::metrics::BrokerMetrics) -> u64 {
    metrics
        .break_glass_refusals
        .get_or_create(&crate::metrics::BreakGlassActionLabel {
            action: crate::metrics::BreakGlassAction(BreakGlassAction::UnregisterBroker),
        })
        .get()
}

#[tokio::test]
async fn the_wire_handler_refuses_an_unregistration_that_no_proposal_covers() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.break_glass = gated_config();
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    check!(resp.error_code == codes::POLICY_VIOLATION);
    check!(
        resp.error_message
            == Some(
                "break-glass refused unregister_broker on 1: no approved proposal covers the request"
                    .to_owned()
            )
    );
    // The refusal refused: broker 1 is still registered.
    check!(
        broker
            .controller
            .current_image()
            .broker(NodeId(1))
            .is_some()
    );
    // The refusal reached the series an operator reads.
    check!(refusals(&broker.metrics) == 1);
    broker_handle.shutdown().await;
}

/// A new registration of broker `node_id`. It carries no epoch (-1), and the
/// controller stamps it with the offset that it commits at.
fn registration(node_id: u64, fenced: bool) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(krabka_metadata::BrokerRegistrationRecord {
        fenced,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(node_id),
        broker_epoch: -1,
        incarnation_id: Uuid::from_u128(u128::from(node_id)),
        host: format!("broker-{node_id}"),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs: vec![],
        features: std::collections::BTreeMap::new(),
    })
}

/// Partition `index` of topic `t`, replicated on brokers 1 and 2.
fn replicated_partition(
    index: i32,
    leader: u64,
    isr: &[u64],
    leader_epoch: i32,
    partition_epoch: i32,
) -> krabka_metadata::PartitionRecord {
    krabka_metadata::PartitionRecord {
        topic: "t".into(),
        partition: index,
        leader: NodeId(leader),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: isr.iter().copied().map(NodeId).collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch,
    }
}

/// Topic `t`, with `partitions` partitions and a replication factor of 2.
fn topic(partitions: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(0x7),
        partitions,
        replication_factor: 2,
    })
}

async fn wait_for_leader(broker: &crate::broker::Broker) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !broker
        .controller
        .watch_leader()
        .borrow()
        .is_some_and(|node| node == broker.config.node_id)
    {
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Kafka's `ReplicationControlManager.unregisterBroker` writes
/// `handleBrokerUnregistered`'s partition changes ahead of the
/// `UnregisterBrokerRecord`: the broker leaves every ISR, and a partition it led
/// moves to another replica. The change is committed by the time the handler
/// answers, so no partition names the broker as leader or ISR member while the
/// liveness ticker waits for a heartbeat timeout.
#[tokio::test]
async fn handle_removes_the_broker_from_every_isr_in_the_unregistering_append() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![
            registration(2, false),
            topic(3),
            MetadataRecord::V1Partition(replicated_partition(0, 1, &[1, 2], 5, 0)),
            MetadataRecord::V1Partition(replicated_partition(1, 2, &[2, 1], 5, 0)),
            MetadataRecord::V1Partition(replicated_partition(2, 2, &[2], 5, 0)),
        ])
        .await
        .expect("seed the partitions");
    // Broker 2 is heartbeating, so it may take over.
    broker.liveness.record_heartbeat(2).await;
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    check!(resp.error_code == codes::NONE, "{resp:?}");
    let image = broker.controller.current_image();
    check!(image.broker(NodeId(1)).is_none());
    // Broker 1 led partition 0, so broker 2 takes it at the next leader epoch.
    // It only followed partition 1, so the leader stays and the epoch does not
    // move. It was in neither of partition 2's lists, so that one is untouched.
    let expected = [
        replicated_partition(0, 2, &[2], 6, 1),
        replicated_partition(1, 2, &[2], 5, 1),
        replicated_partition(2, 2, &[2], 5, 0),
    ];
    for partition in expected {
        check!(
            image.partition("t", partition.partition) == Some(&partition),
            "partition {}",
            partition.partition
        );
    }
    broker_handle.shutdown().await;
}

/// A node that is not the active controller keeps no liveness registry, so it
/// reads the brokers that may lead from the image: Kafka's
/// `ClusterControlManager.isActive`, a registered broker that is not fenced.
#[tokio::test]
async fn a_node_that_is_not_the_controller_reads_the_active_brokers_from_the_image() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![
            topic(1),
            MetadataRecord::V1Partition(replicated_partition(0, 1, &[1, 2], 5, 0)),
        ])
        .await
        .expect("seed the partition");

    // Broker 2 never heartbeated this node, whose registry is empty.
    let elects_broker_2 = vec![MetadataRecord::V1Partition(replicated_partition(
        0,
        2,
        &[2],
        6,
        1,
    ))];
    for (fenced, expected) in [(false, elects_broker_2), (true, vec![])] {
        broker
            .controller
            .submit_change(vec![registration(2, fenced)])
            .await
            .expect("register broker 2");
        let image = broker.controller.current_image();

        let leaves = leave::leave_isrs_as(&broker, &image, NodeId(1), false).await;

        check!(leaves == expected, "broker 2 fenced: {fenced}");
    }
    broker_handle.shutdown().await;
}

/// The partition changes go between the consumed proposal and the unregister
/// record: the approval commits first, and the registration goes last.
#[test]
fn the_isr_departures_sit_between_the_consume_and_the_unregister_record() {
    let consumed = MetadataRecord::V1BreakGlassProposal(approved_proposal("7"));
    let unregister = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
        node_id: DOOMED,
        broker_epoch: DOOMED_EPOCH,
    });
    let leave = MetadataRecord::V1Partition(replicated_partition(0, 2, &[2], 6, 1));

    for (case, records, expected) in [
        (
            "gated",
            vec![consumed.clone(), unregister.clone()],
            vec![consumed, leave.clone(), unregister.clone()],
        ),
        (
            "ungated",
            vec![unregister.clone()],
            vec![leave.clone(), unregister],
        ),
    ] {
        check!(
            gate::with_leaves(records, vec![leave.clone()]) == expected,
            "{case}"
        );
    }
}
