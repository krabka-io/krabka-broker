//! End-to-end tests of the `CreatePartitions` handler, driven over the wire
//! encoding against a running broker: the authorization gate, the per-topic
//! error rows, the `validate_only` dry run, a successful grow, and the
//! KIP-599 mutation quota.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse;
use krabka_security::Principal;

use super::*;
use crate::{
    handlers::create_partitions::test_support::{
        VERSION, assn, request, seed_controller_quota, seed_topic, topic_req,
    },
    test_support::{DenyAll, peer, principal},
};

crate::test_support::wire_helpers!(
    CreatePartitionsRequest,
    CreatePartitionsResponse,
    version = VERSION,
    client_id = "admin-client"
);

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &CreatePartitionsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> CreatePartitionsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

#[tokio::test]
async fn handle_denies_topic_alter_for_each_topic() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request(
        vec![topic_req("orders", 2, None), topic_req("payments", 2, None)],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![
            CreatePartitionsTopicResult {
                name: "orders".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            CreatePartitionsTopicResult {
                name: "payments".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_unknown_topic_and_rejects_same_partition_count() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "stable", 2, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(
        vec![topic_req("missing", 3, None), topic_req("stable", 2, None)],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![
            CreatePartitionsTopicResult {
                name: "missing".into(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown topic `missing`".into()),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            CreatePartitionsTopicResult {
                name: "stable".into(),
                error_code: codes::INVALID_PARTITIONS,
                error_message: Some(
                    "topic `stable` already has 2 partitions; cannot decrease to 2".into(),
                ),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("stable")
            .count()
            == 2
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn validate_only_reports_success_without_adding_partitions() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "dry-run", 1, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_req("dry-run", 3, None)], true);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "dry-run".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("dry-run")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_adds_new_partitions_and_preserves_response_identity() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "grow", 1, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(
        vec![topic_req("grow", 3, Some(vec![assn(&[1]), assn(&[1])]))],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "grow".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("grow")
            .count()
            == 3
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_an_unplaceable_new_diskless_partition() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "diskless-grow", 1, 1).await;
    broker_handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "diskless-grow".into(),
                overrides: maplit::btreemap! {
                    crate::config_keys::DISKLESS.to_string() => "true".to_string()
                },
            },
        )])
        .await
        .expect("mark topic diskless");
    let broker = broker_handle.broker_arc_for_test();
    let req = request(
        vec![topic_req("diskless-grow", 2, Some(vec![assn(&[1])]))],
        false,
    );

    let resp = drive(&broker, &req, &principal("admin"), &peer()).await;

    assert!(resp.results[0].error_code == codes::INVALID_CONFIG);
    let message = resp.results[0].error_message.as_deref().unwrap_or_default();
    for needle in ["partition 1", "leader 1", "broker.rack"] {
        check!(message.contains(needle), "{message}");
    }
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("diskless-grow")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn strict_create_partitions_rejects_after_quota_exhaustion() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "metered", 2, 1).await;
    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_req("metered", 5, None)], false);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "metered".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let rejected = drive(
        &broker,
        &request(vec![topic_req("metered", 6, None)], false),
        &p,
        &peer,
    )
    .await;
    let expected = CreatePartitionsResponse {
        throttle_time_ms: rejected.throttle_time_ms,
        results: vec![CreatePartitionsTopicResult {
            name: "metered".into(),
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(rejected == expected);
    check!(rejected.throttle_time_ms > 0 && rejected.throttle_time_ms <= 500);
    check!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("metered")
            .count()
            == 5
    );
    broker_handle.shutdown().await;
}

/// Commit topic `grow` with one partition on the remote brokers 3 and 4, so
/// its replication factor is 2.
async fn seed_remote_topic(handle: &crate::broker::BrokerHandle) {
    let replicas = vec![krabka_raft::NodeId(3), krabka_raft::NodeId(4)];
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: "grow".into(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 2,
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: "grow".into(),
                partition: 0,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ])
        .await
        .expect("seed topic");
}

/// A manual assignment for a new partition keeps its replica list, but its
/// ISR holds only the listed brokers that are active, and its leader is the
/// first of them (#745). Kafka's `ReplicationControlManager.createPartitions`
/// filters the ISR with `ClusterControlManager.isActive`, and answers
/// `INVALID_REPLICA_ASSIGNMENT` when no listed broker is active.
///
/// Brokers 2, 3 and 4 are remote registrations. A fenced heartbeat on the
/// controller makes a broker unavailable, as a real fenced broker is.
#[tokio::test]
async fn manual_assignment_leaves_unavailable_brokers_out_of_the_isr() {
    /// One row: the fenced brokers, the witness brokers, the replica list of
    /// each new partition, and the expected error code, error message and
    /// `(leader, isr)` per new partition.
    type Row = (
        &'static [u64],
        &'static [u64],
        &'static [&'static [i32]],
        i16,
        Option<&'static str>,
        Vec<(krabka_raft::NodeId, Vec<krabka_raft::NodeId>)>,
    );
    let n = krabka_raft::NodeId;
    let rows: [Row; 6] = [
        (
            &[],
            &[],
            &[&[2, 3]],
            codes::NONE,
            None,
            vec![(n(2), vec![n(2), n(3)])],
        ),
        (
            &[2],
            &[],
            &[&[2, 3]],
            codes::NONE,
            None,
            vec![(n(3), vec![n(3)])],
        ),
        (
            &[2],
            &[],
            &[&[4, 2], &[2, 3]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(4)]), (n(3), vec![n(3)])],
        ),
        (
            &[2, 3],
            &[],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 1 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All active brokers specified in the manual partition assignment for partition \
                 1 are witnesses, and a witness cannot lead.",
            ),
            vec![],
        ),
        (
            &[],
            &[3],
            &[&[3, 4]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(3), n(4)])],
        ),
    ];

    for (fenced, witnesses, lists, error_code, error_message, partitions) in rows {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        for node_id in [2, 3, 4] {
            crate::test_support::seed_remote_broker(&broker_handle, node_id).await;
        }
        seed_remote_topic(&broker_handle).await;
        for &node_id in witnesses {
            crate::test_support::make_witness(&broker_handle, node_id).await;
        }
        for &node_id in fenced {
            crate::test_support::fence_remote_broker(&broker_handle, node_id).await;
        }
        let broker = broker_handle.broker_arc_for_test();
        let count = 1 + i32::try_from(lists.len()).expect("count");
        let req = request(
            vec![topic_req(
                "grow",
                count,
                Some(lists.iter().map(|ids| assn(ids)).collect()),
            )],
            false,
        );

        let resp = drive(&broker, &req, &principal("admin"), &peer()).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsTopicResult {
                name: "grow".into(),
                error_code,
                error_message: error_message.map(str::to_owned),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        check!(resp == expected, "fenced {fenced:?}, assignment {lists:?}");

        let image = broker_handle.controller_image_for_test();
        let added = (1..)
            .map_while(|index| image.partition("grow", index).cloned())
            .collect::<Vec<_>>();
        let expected_records = partitions
            .into_iter()
            .zip(lists)
            .zip(1..)
            .map(
                |(((leader, isr), replicas), partition)| krabka_metadata::PartitionRecord {
                    topic: "grow".into(),
                    partition,
                    leader,
                    replicas: replicas
                        .iter()
                        .map(|&id| n(u64::try_from(id).expect("broker id")))
                        .collect(),
                    isr,
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                },
            )
            .collect::<Vec<_>>();
        check!(
            added == expected_records,
            "fenced {fenced:?}, assignment {lists:?}"
        );
        broker_handle.shutdown().await;
    }
}
