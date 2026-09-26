//! Handler tests for the KIP-951 `NodeEndpoints` of `ShareFetch` and
//! `ShareAcknowledge`.
//!
//! Kafka's `KafkaApis.processShareFetchResponse` and
//! `processShareAcknowledgeResponse` set `CurrentLeader` on every row with
//! `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH`, and add the leader's
//! node on the listener of the request to `NodeEndpoints`, once per node. A
//! leader that no live broker registration backs keeps its id in the hint and
//! adds no endpoint.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId, PartitionRecord, TopicRecord,
};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::{self, ShareAcknowledgeResponse},
        share_fetch_request::{FetchPartition, FetchTopic, ShareFetchRequest},
        share_fetch_response::{
            self, LeaderIdAndEpoch, NodeEndpoint, PartitionData, ShareFetchResponse,
            ShareFetchableTopicResponse,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use super::handle;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

const GROUP: &str = "endpoints-group";

/// The remote broker that leads both partitions of [`REMOTE`].
const REMOTE_NODE: i32 = 2;
/// A leader that no broker registration backs.
const UNREGISTERED_NODE: i32 = 7;

const REMOTE: WireUuid = WireUuid([0x22; 16]);
const ORPHAN: WireUuid = WireUuid([0x77; 16]);

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.enable = true;
    })
    .await
}

async fn create_local_topic(broker: &BrokerHandle) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-endpoints-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "local".to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present("local", 0).await;
    let image = broker.controller_image_for_test();
    WireUuid(
        image
            .topic("local")
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

fn partition(topic: &str, partition: i32, leader: i32, epoch: i32) -> MetadataRecord {
    let leader = NodeId(u64::try_from(leader).expect("a node id"));
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition,
        leader,
        replicas: vec![leader],
        isr: vec![leader],
        leader_epoch: LeaderEpoch(epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

/// Registers [`REMOTE_NODE`] with an endpoint on the test listener, and adds
/// a two-partition topic that it leads and a topic led by
/// [`UNREGISTERED_NODE`].
async fn seed_remote_leaders(broker: &BrokerHandle) {
    let listener = request_context(&principal("x"), &peer(), "x")
        .connection_listener_name
        .to_string();
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "legacy-2".into(),
                port: 1000,
                rack: Some("rack-2".into()),
                endpoints: vec![BrokerEndpoint {
                    name: listener,
                    host: "remote-2".into(),
                    port: 9192,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                }],
                log_dirs: vec![],
                features: std::collections::BTreeMap::new(),
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "remote".into(),
                topic_id: uuid::Uuid::from_bytes(REMOTE.0),
                partitions: 2,
                replication_factor: 1,
            }),
            partition("remote", 0, REMOTE_NODE, 4),
            partition("remote", 1, REMOTE_NODE, 4),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orphan".into(),
                topic_id: uuid::Uuid::from_bytes(ORPHAN.0),
                partitions: 1,
                replication_factor: 1,
            }),
            partition("orphan", 0, UNREGISTERED_NODE, 1),
        ])
        .await
        .expect("seed the remote leaders");
}

/// The rows of the request: `(topic, partition)`.
fn rows(local: WireUuid) -> Vec<(WireUuid, i32)> {
    vec![(local, 0), (REMOTE, 0), (REMOTE, 1), (ORPHAN, 0)]
}

fn fetch_row(partition_index: i32, error_code: i16, leader: (i32, i32)) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        current_leader: LeaderIdAndEpoch {
            leader_id: leader.0,
            leader_epoch: leader.1,
            ..Default::default()
        },
        records: Some(RecordsPayload::Legacy(Bytes::new())),
        ..Default::default()
    }
}

#[tokio::test]
async fn share_fetch_sends_the_endpoint_of_each_remote_leader_once() {
    let (broker, _dir) = start().await;
    let local = create_local_topic(&broker).await;
    crate::test_support::initialize_share_state(&broker, GROUP, uuid::Uuid::from_bytes(local.0), 0)
        .await;
    seed_remote_leaders(&broker).await;
    let request = ShareFetchRequest {
        group_id: Some(GROUP.into()),
        member_id: Some("member".into()),
        share_session_epoch: 0,
        max_bytes: 1 << 20,
        max_records: 10,
        batch_size: 10,
        topics: [local, REMOTE, ORPHAN]
            .into_iter()
            .map(|topic_id| FetchTopic {
                topic_id,
                partitions: rows(local)
                    .into_iter()
                    .filter(|(row_topic, _)| *row_topic == topic_id)
                    .map(|(_, partition_index)| FetchPartition {
                        partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let version = share_fetch_response::MAX_VERSION;
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let response = handle(
        &shared,
        version,
        7,
        &encode_request(&request, version),
        &ctx,
    )
    .await
    .expect("handle share fetch");
    let response: ShareFetchResponse = decode_response(&response, version);

    let expected = ShareFetchResponse {
        acquisition_lock_timeout_ms: 30_000,
        responses: vec![
            ShareFetchableTopicResponse {
                topic_id: local,
                partitions: vec![fetch_row(0, codes::NONE, (0, 0))],
                ..Default::default()
            },
            ShareFetchableTopicResponse {
                topic_id: REMOTE,
                partitions: vec![
                    fetch_row(0, codes::NOT_LEADER_OR_FOLLOWER, (REMOTE_NODE, 4)),
                    fetch_row(1, codes::NOT_LEADER_OR_FOLLOWER, (REMOTE_NODE, 4)),
                ],
                ..Default::default()
            },
            ShareFetchableTopicResponse {
                topic_id: ORPHAN,
                partitions: vec![fetch_row(
                    0,
                    codes::NOT_LEADER_OR_FOLLOWER,
                    (UNREGISTERED_NODE, 1),
                )],
                ..Default::default()
            },
        ],
        node_endpoints: vec![NodeEndpoint {
            node_id: REMOTE_NODE,
            host: "remote-2".into(),
            port: 9192,
            rack: Some("rack-2".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(response == expected);
    broker.shutdown().await;
}

#[tokio::test]
async fn share_acknowledge_sends_the_endpoint_of_each_remote_leader_once() {
    let (broker, _dir) = start().await;
    let local = create_local_topic(&broker).await;
    seed_remote_leaders(&broker).await;
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    shared
        .share_partition_leaders
        .update_fetch_session(
            GROUP,
            "member",
            ctx.connection_id,
            0,
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
            false,
            false,
        )
        .expect("open the share session");
    let request = ShareAcknowledgeRequest {
        group_id: Some(GROUP.into()),
        member_id: Some("member".into()),
        share_session_epoch: 1,
        topics: [REMOTE, ORPHAN]
            .into_iter()
            .map(|topic_id| AcknowledgeTopic {
                topic_id,
                partitions: rows(local)
                    .into_iter()
                    .filter(|(row_topic, _)| *row_topic == topic_id)
                    .map(|(_, partition_index)| AcknowledgePartition {
                        partition_index,
                        acknowledgement_batches: vec![AcknowledgeBatch {
                            first_offset: 0,
                            last_offset: 0,
                            acknowledge_types: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let version = share_acknowledge_response::MAX_VERSION;
    let response = crate::handlers::share_acknowledge::handle(
        &shared,
        version,
        7,
        &encode_request(&request, version),
        &ctx,
    )
    .await
    .expect("handle share acknowledge");
    let response: ShareAcknowledgeResponse = decode_response(&response, version);

    let row =
        |partition_index, leader_id, leader_epoch| share_acknowledge_response::PartitionData {
            partition_index,
            error_code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: share_acknowledge_response::LeaderIdAndEpoch {
                leader_id,
                leader_epoch,
                ..Default::default()
            },
            ..Default::default()
        };
    let expected = ShareAcknowledgeResponse {
        acquisition_lock_timeout_ms: 30_000,
        responses: vec![
            share_acknowledge_response::ShareAcknowledgeTopicResponse {
                topic_id: REMOTE,
                partitions: vec![row(0, REMOTE_NODE, 4), row(1, REMOTE_NODE, 4)],
                ..Default::default()
            },
            share_acknowledge_response::ShareAcknowledgeTopicResponse {
                topic_id: ORPHAN,
                partitions: vec![row(0, UNREGISTERED_NODE, 1)],
                ..Default::default()
            },
        ],
        node_endpoints: vec![share_acknowledge_response::NodeEndpoint {
            node_id: REMOTE_NODE,
            host: "remote-2".into(),
            port: 9192,
            rack: Some("rack-2".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(response == expected);
    broker.shutdown().await;
}
