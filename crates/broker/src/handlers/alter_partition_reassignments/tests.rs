//! End-to-end tests for the `AlterPartitionReassignments` wire handler.
//!
//! They drive a live broker, so they cover the cluster authorization
//! preamble, the response shape for a row the metadata image does not know,
//! and the metadata a successful alter leaves behind.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_metadata::{
    BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, PatternType,
    TopicFreezeRecord, TopicRecord,
};
use krabka_protocol::UnknownTaggedFields;
use krabka_raft::NodeId;
use krabka_security::{AuthMethod, Principal};
use uuid::Uuid;

use super::*;
use crate::{
    codes::{POLICY_VIOLATION, UNKNOWN_TOPIC_OR_PARTITION},
    handlers::alter_partition_reassignments::test_support::{
        decode_response, request, test_context,
    },
    test_support::{DenyAll, start_broker_with_authorizer as start_broker},
};

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn seed_reassignable_partition(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(1),
                broker_epoch: 1,
                incarnation_id: uuid::Uuid::nil(),
                host: "localhost".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            }),
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 1,
                incarnation_id: uuid::Uuid::nil(),
                host: "localhost".into(),
                port: 9093,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition: 7,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: LeaderEpoch(3),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 11,
            }),
        ])
        .await
        .expect("seed reassignment metadata");
}

/// A partition mid-reassignment that is adding a replica which has not caught
/// up, so the reassignment task leaves it alone. Its ISR is already below
/// `min.insync.replicas`, so it carries KIP-966 state naming both replicas
/// that left the ISR -- including the one a cancel drops from the replica set.
async fn seed_cancellable_partition(broker: &Broker) {
    let mut records: Vec<MetadataRecord> = (1..=3u64)
        .map(|node| {
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(node),
                broker_epoch: 1,
                incarnation_id: uuid::Uuid::nil(),
                host: "localhost".into(),
                port: 9092 + u16::try_from(node).expect("node id fits u16"),
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            })
        })
        .collect();
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: 2,
    }));
    records.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: "orders".into(),
        partition: 7,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1)],
        leader_epoch: LeaderEpoch(3),
        adding_replicas: vec![NodeId(3)],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 11,
    }));
    records.push(MetadataRecord::V1TopicConfig(
        krabka_metadata::TopicConfigRecord {
            topic: "orders".into(),
            overrides: [
                (
                    crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                    "3".to_string(),
                ),
                (
                    crate::config_keys::ELIGIBLE_LEADER_REPLICAS.to_string(),
                    "7:2,3:".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        },
    ));
    broker
        .controller
        .submit_change(records)
        .await
        .expect("seed cancellable reassignment metadata");
}

/// KIP-966: a cancel reverts the replica set, so a replica the published ELR
/// calls eligible can stop being a replica at all. It was last known to hold
/// every committed record, but the partition can no longer elect it, so it
/// lands in the last-known set instead -- and the batch that reverts the
/// partition is the batch that says so.
#[tokio::test]
async fn a_cancel_publishes_the_eligible_leader_state_the_revert_implies() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    seed_cancellable_partition(&broker).await;
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().expect("peer address");
    let ctx = test_context(&principal, &peer);

    let bytes = handle(&broker, request(true, "orders", 7, None), &ctx, version)
        .await
        .expect("handle");
    let resp = decode_response(&bytes, version);
    assert!(resp.responses[0].partitions[0].error_code == 0, "{resp:?}");

    let image = broker.controller.current_image();
    let partition = image.partition("orders", 7).expect("partition committed");
    assert!(partition.replicas == vec![NodeId(1), NodeId(2)]);
    assert!(partition.isr == vec![NodeId(1)]);
    assert!(
        crate::elr::TopicElr::of_topic(&image, "orders").partition(7)
            == crate::elr::state::PartitionElr {
                eligible_leader_replicas: vec![2],
                last_known_elr: vec![3],
            }
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_preserves_unknown_partition_response_shape() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);

    let bytes = handle(
        &broker,
        request(false, "payments", 8, Some(vec![1, 2])),
        &ctx,
        version,
    )
    .await
    .expect("handle");
    let resp = decode_response(&bytes, version);

    let expected = AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        allow_replication_factor_change: false,
        error_code: 0,
        error_message: None,
        responses: vec![ReassignableTopicResponse {
            name: "payments".into(),
            partitions: vec![ReassignablePartitionResponse {
                partition_index: 8,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown partition".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_requested_partition() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);

    let bytes = handle(
        &broker,
        request(false, "payments", 8, Some(vec![1, 2])),
        &ctx,
        version,
    )
    .await
    .expect("handle");
    let resp = decode_response(&bytes, version);

    let expected = AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        allow_replication_factor_change: false,
        error_code: 0,
        error_message: None,
        responses: vec![ReassignableTopicResponse {
            name: "payments".into(),
            partitions: vec![ReassignablePartitionResponse {
                partition_index: 8,
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("alter-reassignment denied".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_submits_successful_reassignment_records() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    seed_reassignable_partition(&broker).await;
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);

    let bytes = handle(
        &broker,
        request(true, "orders", 7, Some(vec![1, 2])),
        &ctx,
        version,
    )
    .await
    .expect("handle");
    let resp = decode_response(&bytes, version);

    let expected = AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        allow_replication_factor_change: true,
        error_code: 0,
        error_message: None,
        responses: vec![ReassignableTopicResponse {
            name: "orders".into(),
            partitions: vec![ReassignablePartitionResponse {
                partition_index: 7,
                error_code: 0,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let image = broker.controller.current_image();
    let partition = image.partition("orders", 7).expect("partition committed");
    assert!(partition.adding_replicas == vec![NodeId(2)]);
    assert!(partition.partition_epoch == 12);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_refuses_a_frozen_reassignment_without_mutating_the_partition() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    seed_reassignable_partition(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            scope: "orders".into(),
            pattern_type: PatternType::Literal,
            frozen: true,
            reason: "DR cutover".into(),
            set_by: "User:alice".into(),
            set_at_ms: 10,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        })])
        .await
        .expect("seed topic freeze");
    let before = broker
        .controller
        .current_image()
        .partition("orders", 7)
        .expect("seeded partition")
        .clone();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);

    let bytes = handle(
        &broker,
        request(true, "orders", 7, Some(vec![1, 2])),
        &ctx,
        version,
    )
    .await
    .expect("handle");
    let response = decode_response(&bytes, version);
    let row = &response.responses[0].partitions[0];
    check!(row.error_code == POLICY_VIOLATION);
    check!(
        row.error_message.as_deref()
            == Some(
                "a write freeze on the literal scope \"orders\" refuses this reassignment: DR cutover"
            )
    );

    let after = broker
        .controller
        .current_image()
        .partition("orders", 7)
        .expect("partition remains")
        .clone();
    check!(after == before, "the refused row must append no metadata");
    broker_handle.shutdown().await;
}
