//! Table-driven coverage of where the per-topic codes land relative to the
//! group routing, staged producer identity and KIP-447 fencing exits (#866),
//! driven through the dispatch registry against a real in-process broker.
//!
//! Kafka's `KafkaApis.handleTxnOffsetCommitRequest` stamps
//! `TOPIC_AUTHORIZATION_FAILED` and `UNKNOWN_TOPIC_OR_PARTITION` into its
//! response builder first, calls the group coordinator only when a row
//! survives (lines 2163-2165), and merges the coordinator's answer onto the
//! surviving rows only (line 2185).

use std::{collections::HashSet, sync::Arc};

use assert2::check;
use krabka_log::ProducerId;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    txn_offset_commit_request::{self, TxnOffsetCommitRequest, TxnOffsetCommitRequestTopic},
    txn_offset_commit_response::{
        TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
    },
};

use super::integration_tests::{log_holds_key, seed_topic_a, topic};
use crate::{
    authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
    coordinator::{bootstrap::OFFSETS_TOPIC, partitioner::partition_for_group},
    test_support::{
        decode_response, dispatch_context, encode_request, peer, principal, request_context,
        start_broker_with,
    },
    txn::{state::TxnEntry, version::TxnVersion},
};

/// The one topic the principal may not read.
const DENIED: &str = "denied";

/// Allows every request except `Read` on the topic [`DENIED`].
#[derive(Debug)]
struct DenyOneTopic;

impl Authorizer for DenyOneTopic {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if request.resource_type == ResourceType::Topic
            && request.operation == AclOperation::Read
            && request.resource_name == DENIED
        {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }
}

/// Who the request says it is on the group side.
#[derive(Clone, Copy)]
enum Member {
    /// No group metadata: a simple consumer, which is never fenced.
    Simple,
    /// A member id the group never registered, which classic fencing answers
    /// with `UNKNOWN_MEMBER_ID`.
    Unregistered,
}

struct Case {
    name: &'static str,
    topics: Vec<TxnOffsetCommitRequestTopic>,
    /// Whether this broker leads the group's `__consumer_offsets` partition.
    coordinator: bool,
    /// Whether the transactional id holds a staged producer identity.
    staged: bool,
    member: Member,
    expected: Vec<(&'static str, i32, i16)>,
    appended: Vec<(&'static str, i32)>,
}

/// One response topic per row, which is the shape every case's request has.
fn expected_response(rows: &[(&str, i32, i16)]) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics: rows
            .iter()
            .map(
                |&(name, partition_index, error_code)| TxnOffsetCommitResponseTopic {
                    name: name.to_string(),
                    partitions: vec![TxnOffsetCommitResponsePartition {
                        partition_index,
                        error_code,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

/// Moves the leader of `group_id`'s `__consumer_offsets` partition to another
/// node, so this broker answers the group's routing check with
/// `NOT_COORDINATOR`.
async fn lead_group_elsewhere(handle: &crate::broker::BrokerHandle, group_id: &str) {
    let broker = handle.broker_arc_for_test();
    let partition = partition_for_group(&broker.controller.current_image(), group_id);
    let elsewhere = krabka_metadata::NodeId(broker.config.node_id.0 + 1);
    broker
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: OFFSETS_TOPIC.to_string(),
                partition,
                leader: elsewhere,
                replicas: vec![elsewhere],
                isr: vec![elsewhere],
                leader_epoch: krabka_metadata::LeaderEpoch(1),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: vec![uuid::Uuid::nil()],
                partition_epoch: 1,
            },
        )])
        .await
        .unwrap_or_else(|error| panic!("move {group_id}'s offsets partition: {error}"));
    handle
        .wait_for_image(|image| {
            image
                .partition(OFFSETS_TOPIC, partition)
                .is_some_and(|record| record.leader == elsewhere)
        })
        .await;
}

/// Creates the one-partition `__transaction_state` topic and waits until the
/// transaction coordinator has loaded it, so [`stage_producer_identity`] can
/// append to it.
async fn bootstrap_transaction_state(handle: &crate::broker::BrokerHandle) {
    let broker = handle.broker_arc_for_test();
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    crate::txn::bootstrap::ensure_topic(
        &broker.controller,
        1,
        1,
        &crate::txn::bootstrap::topic_configs(
            broker.config.transaction_state_segment_bytes,
            broker.config.transaction_state_min_isr,
        ),
    )
    .await
    .expect("bootstrap __transaction_state");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while broker
            .txn_coordinator
            .load_status(krabka_ids::PartitionIndex(0))
            .await
            != Some(crate::txn::coordinator::leadership::LoadStatus::Loaded)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("__transaction_state-0 becomes local");
}

/// Gives `transactional_id` a durable entry with a staged producer identity,
/// the state an interrupted `InitProducerId` leaves behind.
async fn stage_producer_identity(
    broker: &crate::broker::Broker,
    transactional_id: &str,
    producer_id: i64,
) {
    let mut entry = TxnEntry::new_empty(
        transactional_id.to_string(),
        ProducerId(producer_id),
        0,
        60_000,
        crate::txn::util::now_millis(),
    );
    entry.next_producer_id = ProducerId(producer_id + 1);
    entry.next_producer_epoch = 0;
    broker
        .txn_coordinator
        .put(entry, TxnVersion::Flexible)
        .await
        .unwrap_or_else(|error| panic!("stage {transactional_id}: {error}"));
}

/// The issue's table, plus the unknown-topic and staged-identity rows. Every
/// case the broker coordinates runs before any case that moves a group's
/// offsets partition away, so a hash collision between two case groups can
/// only move a partition whose cases have already run.
#[tokio::test]
async fn per_topic_codes_survive_every_exit_and_gate_the_coordinator_call() {
    let (handle, _dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(DenyOneTopic);
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = handle.broker_arc_for_test();
    bootstrap_transaction_state(&handle).await;
    seed_topic_a(&broker).await;

    let cases = [
        Case {
            name: "denied_row_keeps_29_beside_a_committed_row",
            topics: vec![topic("a", &[0]), topic(DENIED, &[0])],
            coordinator: true,
            staged: false,
            member: Member::Simple,
            expected: vec![
                ("a", 0, codes::NONE),
                (DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED),
            ],
            appended: vec![("a", 0)],
        },
        Case {
            name: "all_rows_denied_skips_fencing",
            topics: vec![topic(DENIED, &[0])],
            coordinator: true,
            staged: false,
            member: Member::Unregistered,
            expected: vec![(DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED)],
            appended: vec![],
        },
        Case {
            name: "all_rows_unknown_skips_fencing",
            topics: vec![topic("missing", &[0])],
            coordinator: true,
            staged: false,
            member: Member::Unregistered,
            expected: vec![("missing", 0, codes::UNKNOWN_TOPIC_OR_PARTITION)],
            appended: vec![],
        },
        Case {
            name: "surviving_row_is_fenced",
            topics: vec![topic("a", &[0])],
            coordinator: true,
            staged: false,
            member: Member::Unregistered,
            expected: vec![("a", 0, codes::UNKNOWN_MEMBER_ID)],
            appended: vec![],
        },
        Case {
            name: "staged_identity_on_the_coordinator_is_invalid_txn_state",
            topics: vec![topic("a", &[0]), topic(DENIED, &[0])],
            coordinator: true,
            staged: true,
            member: Member::Simple,
            expected: vec![
                ("a", 0, codes::INVALID_TXN_STATE),
                (DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED),
            ],
            appended: vec![],
        },
        Case {
            name: "all_rows_denied_skips_the_staged_identity_gate",
            topics: vec![topic(DENIED, &[0])],
            coordinator: true,
            staged: true,
            member: Member::Simple,
            expected: vec![(DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED)],
            appended: vec![],
        },
        Case {
            name: "denied_row_keeps_29_beside_a_not_coordinator_row",
            topics: vec![topic("a", &[0]), topic(DENIED, &[0])],
            coordinator: false,
            staged: false,
            member: Member::Simple,
            expected: vec![
                ("a", 0, codes::NOT_COORDINATOR),
                (DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED),
            ],
            appended: vec![],
        },
        Case {
            name: "routing_runs_before_fencing",
            topics: vec![topic("a", &[0])],
            coordinator: false,
            staged: false,
            member: Member::Unregistered,
            expected: vec![("a", 0, codes::NOT_COORDINATOR)],
            appended: vec![],
        },
        Case {
            name: "routing_runs_before_the_staged_identity_gate",
            topics: vec![topic("a", &[0])],
            coordinator: false,
            staged: true,
            member: Member::Simple,
            expected: vec![("a", 0, codes::NOT_COORDINATOR)],
            appended: vec![],
        },
        Case {
            name: "all_rows_denied_skips_routing",
            topics: vec![topic(DENIED, &[0])],
            coordinator: false,
            staged: false,
            member: Member::Simple,
            expected: vec![(DENIED, 0, codes::TOPIC_AUTHORIZATION_FAILED)],
            appended: vec![],
        },
    ];

    let address = peer();
    let user = principal("user");
    let ctx = request_context(&user, &address, "txn-offset-commit-ordering");
    let version = 3; // flexible, carries the group metadata fencing reads
    for (case_index, case) in cases.into_iter().enumerate() {
        let group_id = format!("ordering-group-{case_index}");
        let transactional_id = format!("ordering-tid-{case_index}");
        // Two apart, so a staged entry's next producer id is never another's.
        let producer_id = 1_000 + 2 * i64::try_from(case_index).expect("case index fits");
        if !case.coordinator {
            lead_group_elsewhere(&handle, &group_id).await;
        }
        if case.staged {
            stage_producer_identity(&broker, &transactional_id, producer_id).await;
        }
        let (member_id, generation_id) = match case.member {
            Member::Simple => (String::new(), -1),
            Member::Unregistered => ("never-registered-member".to_string(), 0),
        };
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: 0,
            member_id,
            generation_id_or_member_epoch: generation_id,
            topics: case.topics.clone(),
            ..Default::default()
        };
        let bytes = dispatch_context(
            &broker,
            txn_offset_commit_request::API_KEY,
            version,
            &encode_request(&request, version),
            &ctx,
        )
        .await;
        let response: TxnOffsetCommitResponse = decode_response(&bytes, version);
        check!(
            response == expected_response(&case.expected),
            "{}",
            case.name
        );

        let appended: HashSet<(&str, i32)> = case.appended.iter().copied().collect();
        for req_topic in &case.topics {
            for part in &req_topic.partitions {
                let key = (req_topic.name.as_str(), part.partition_index);
                check!(
                    log_holds_key(&broker, &group_id, key.0, key.1) == appended.contains(&key),
                    "{}: log holds {:?}",
                    case.name,
                    key
                );
            }
        }
    }

    handle.shutdown().await;
}
