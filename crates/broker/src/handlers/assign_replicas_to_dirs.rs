//! `AssignReplicasToDirs` (`api_key=73`, KIP-858).
//!
//! A broker reports, for each of its replicas, which log-directory UUID hosts
//! it. The controller records that in
//! `PartitionRecord.directories[broker_slot]`, so it can later map an
//! `offline_log_dirs` heartbeat back to exactly the affected partitions for
//! failover.
//!
//! Only the leader serves this RPC, and any other broker returns
//! `NOT_CONTROLLER`. This mirrors `alter_partition`.
//!
//! This file holds the ACL preamble, the broker-epoch fence, the leader
//! check, and the request-to-controller flow. The pure halves live beside it:
//! `validation` checks the reporting broker's registration and epoch,
//! `changes` maps a reported directory onto a metadata delta and a
//! per-partition error code, and `response` builds the fixed responses and
//! encodes what goes back on the wire.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext},
};

mod changes;
mod response;
#[cfg(test)]
mod test_support;
mod validation;

use self::{
    changes::{AssignmentPlan, plan_assignments},
    response::{encode_resp, not_controller_response},
    validation::check_broker_epoch,
};

pub(crate) async fn handle(
    broker: &Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = AssignReplicasToDirsRequest::decode(&mut cur, version)?;

    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let image = controller.current_image();

    // ── ACL preamble ────────────────────────────────────────────
    // Inter-broker control-plane RPC: `ClusterAction` on
    // `Cluster("kafka-cluster")`. On Deny → whole-response
    // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`, as Kafka's
    // `ControllerApis.handleAssignReplicasToDirs` requires before it forwards
    // to the controller.
    if crate::handlers::cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_resp(
            version,
            &AssignReplicasToDirsResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
        );
    }

    let is_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| is_controller_leader(Some(n.0), node_id.0));
    if !is_leader {
        return encode_resp(version, &not_controller_response());
    }

    // Kafka's `ClusterControlManager.checkBrokerEpoch`, which
    // `ReplicationControlManager.handleAssignReplicasToDirs` runs before it
    // looks at the reported rows. Fences out a stale or restarted broker: an
    // unregistered id (a negative wire value included) gets
    // `BROKER_ID_NOT_REGISTERED`, and a registered id reporting the wrong
    // epoch gets `STALE_BROKER_EPOCH`.
    let broker_slot_id = match check_broker_epoch(&image, req.broker_id, req.broker_epoch) {
        Ok(broker_slot_id) => broker_slot_id,
        Err(error_code) => {
            return encode_resp(
                version,
                &AssignReplicasToDirsResponse {
                    error_code,
                    ..Default::default()
                },
            );
        }
    };
    if image.finalized_metadata_version().is_some_and(|level| {
        level < krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL
    }) {
        return encode_resp(
            version,
            &AssignReplicasToDirsResponse {
                error_code: codes::UNSUPPORTED_VERSION,
                ..Default::default()
            },
        );
    }
    let AssignmentPlan { changes, response } = plan_assignments(&image, broker_slot_id, &req);

    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        return Err(BrokerError::Replication(format!("submit_change: {e}")));
    }

    encode_resp(version, &response)
}

fn is_controller_leader(leader: Option<u64>, node_id: u64) -> bool {
    leader == Some(node_id)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord, PartitionRecord, TopicRecord};
    use krabka_protocol::{
        owned::assign_replicas_to_dirs_response::{
            DirectoryData as RespDirData, PartitionData as RespPartData, TopicData as RespTopicData,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    };

    use super::{
        test_support::{
            VERSION, decode_response, handle_allowed, own_broker_epoch, request, start_broker,
            wait_for_leader,
        },
        *,
    };

    #[test]
    fn leader_predicate_matches_current_node_only() {
        for (leader, want) in [(Some(1), true), (Some(2), false), (None, false)] {
            assert!(is_controller_leader(leader, 1) == want, "leader {leader:?}");
        }
    }

    /// The topic reference and the partition that one request row reports.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Report {
        /// Partition 0 of the seeded topic, which broker 1 replicates.
        KnownPartition,
        /// Partition 7 of the seeded topic, which does not exist.
        UnknownPartition,
        /// A non-zero topic id that names no topic.
        UnknownTopicId,
        /// The zero topic id.
        ZeroTopicId,
    }

    /// Kafka's `ReplicationControlManager.handleAssignReplicasToDirs` answers
    /// `UNKNOWN_TOPIC_ID` for a topic id that names no topic, the zero id
    /// included, and `UNKNOWN_TOPIC_OR_PARTITION` for a partition that the
    /// topic does not have.
    #[tokio::test]
    async fn handle_answers_each_partition_with_kafkas_error_code() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let broker_epoch = own_broker_epoch(&broker);
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        seed_topic(&broker, topic_uuid).await;

        let cases = [
            (Report::KnownPartition, codes::NONE),
            (Report::UnknownPartition, codes::UNKNOWN_TOPIC_OR_PARTITION),
            (Report::UnknownTopicId, codes::UNKNOWN_TOPIC_ID),
            (Report::ZeroTopicId, codes::UNKNOWN_TOPIC_ID),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (report, error_code) in cases {
            let (topic_id, partition_index) = match report {
                Report::KnownPartition => (topic_uuid, 0),
                Report::UnknownPartition => (topic_uuid, 7),
                Report::UnknownTopicId => (uuid::Uuid::from_u128(0xCC), 0),
                Report::ZeroTopicId => (uuid::Uuid::nil(), 0),
            };
            let bytes = handle_allowed(
                &broker,
                VERSION,
                9,
                &request(broker_epoch, dir_uuid, topic_id, partition_index),
            )
            .await
            .expect("AssignReplicasToDirs handler");
            actual.push((report, decode_response(&bytes)));
            expected.push((
                report,
                AssignReplicasToDirsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NONE,
                    directories: vec![RespDirData {
                        id: ProtocolUuid(dir_uuid.into_bytes()),
                        topics: vec![RespTopicData {
                            topic_id: ProtocolUuid(topic_id.into_bytes()),
                            partitions: vec![RespPartData {
                                partition_index,
                                error_code,
                                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                            }],
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            ));
        }
        assert!(actual == expected);
        let image = broker.controller.current_image();
        let partition = image.partition("t", 0).expect("partition");
        assert!(partition.directories == vec![dir_uuid]);

        broker_handle.shutdown().await;
    }

    /// Seed topic "t" with id `topic_uuid` and partition 0 on broker 1.
    async fn seed_topic(broker: &Broker, topic_uuid: uuid::Uuid) {
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: vec![krabka_audit::NodeId(1)],
                    isr: vec![krabka_audit::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 0,
                }),
            ])
            .await
            .expect("seed partition");
    }

    #[tokio::test]
    async fn handle_leader_commits_known_directory_assignment() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: vec![krabka_audit::NodeId(1)],
                    isr: vec![krabka_audit::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 0,
                }),
            ])
            .await
            .expect("seed partition");
        let req = request(own_broker_epoch(&broker), dir_uuid, topic_uuid, 0);

        let bytes = handle_allowed(&broker, VERSION, 9, &req)
            .await
            .expect("AssignReplicasToDirs handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.directories[0].topics[0].partitions[0].error_code == codes::NONE);
        let image = broker.controller.current_image();
        let partition = image.partition("t", 0).expect("partition");
        assert!(partition.directories == vec![dir_uuid]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_directory_assignment_below_kip_858_metadata_version() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1,
            })])
            .await
            .expect("seed downgraded metadata version");
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: vec![krabka_audit::NodeId(1)],
                    isr: vec![krabka_audit::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 0,
                }),
            ])
            .await
            .expect("seed downgraded partition");

        let bytes = handle_allowed(
            &broker,
            VERSION,
            9,
            &request(
                own_broker_epoch(&broker),
                uuid::Uuid::from_u128(0xAA),
                topic_uuid,
                0,
            ),
        )
        .await
        .expect("AssignReplicasToDirs handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        // PartitionRecord v0 has no directories field at this metadata
        // version, so replay projects the seeded nil slot to an empty vector.
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                .is_empty()
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `ControllerApis.handleAssignReplicasToDirs` checks
    /// `ClusterAction` on the cluster before the controller applies any
    /// assignment (#636). A denial answers `CLUSTER_AUTHORIZATION_FAILED` and
    /// commits nothing.
    #[tokio::test]
    async fn handle_needs_cluster_action() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
            config.authorizer = std::sync::Arc::new(crate::test_support::GrantsInPrincipalName);
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let broker_epoch = own_broker_epoch(&broker);
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        seed_topic(&broker, topic_uuid).await;

        let cases = [
            ("none", codes::CLUSTER_AUTHORIZATION_FAILED),
            (
                "Cluster:Alter+Cluster:Describe",
                codes::CLUSTER_AUTHORIZATION_FAILED,
            ),
            ("Cluster:ClusterAction", codes::NONE),
        ];
        let address = crate::test_support::peer();
        for (grants, expected_error) in cases {
            let user = crate::test_support::principal(grants);
            let ctx = crate::test_support::request_context(&user, &address, "assign-test");
            let bytes = handle(
                &broker,
                VERSION,
                9,
                &request(broker_epoch, dir_uuid, topic_uuid, 0),
                &ctx,
            )
            .await
            .expect("AssignReplicasToDirs handler");
            let resp = decode_response(&bytes);
            assert!(resp.error_code == expected_error, "{grants}: {resp:?}");
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `ClusterControlManager.checkBrokerEpoch`, which
    /// `ReplicationControlManager.handleAssignReplicasToDirs` runs before it
    /// looks at the reported rows (#636). A stale epoch or an unregistered id
    /// is refused and commits nothing; only the current epoch commits the
    /// assignment.
    #[tokio::test]
    async fn handle_fences_stale_and_unregistered_brokers() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let broker_epoch = own_broker_epoch(&broker);
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        seed_topic(&broker, topic_uuid).await;

        // A stale epoch is refused, and the seeded directory slot is
        // untouched. `+ 1` rather than `- 1`: a registered epoch of 0 would
        // otherwise turn "stale" into `-1`, which means "not provided" and
        // matches any registration (see `validation::check_broker_epoch`).
        let stale = decode_response(
            &handle_allowed(
                &broker,
                VERSION,
                9,
                &request(broker_epoch + 1, dir_uuid, topic_uuid, 0),
            )
            .await
            .expect("AssignReplicasToDirs handler"),
        );
        assert!(stale.error_code == codes::STALE_BROKER_EPOCH, "{stale:?}");
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                == vec![uuid::Uuid::nil()]
        );

        // The current epoch succeeds and commits the assignment.
        let current = decode_response(
            &handle_allowed(
                &broker,
                VERSION,
                10,
                &request(broker_epoch, dir_uuid, topic_uuid, 0),
            )
            .await
            .expect("AssignReplicasToDirs handler"),
        );
        assert!(current.error_code == codes::NONE, "{current:?}");
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                == vec![dir_uuid]
        );

        // `-1` ("not provided") also succeeds: it is the epoch
        // `crate::assign_dirs::build_request` sends for this broker's own
        // self-reported directory assignments.
        let unspecified = decode_response(
            &handle_allowed(&broker, VERSION, 11, &request(-1, dir_uuid, topic_uuid, 0))
                .await
                .expect("AssignReplicasToDirs handler"),
        );
        assert!(unspecified.error_code == codes::NONE, "{unspecified:?}");

        broker_handle.shutdown().await;
    }

    /// An id that names no registration is refused with
    /// `BROKER_ID_NOT_REGISTERED`, the negative wire sentinel included.
    #[tokio::test]
    async fn handle_refuses_an_unregistered_broker_id() {
        use krabka_protocol::{
            Encode,
            owned::assign_replicas_to_dirs_request::{
                AssignReplicasToDirsRequest, DirectoryData as ReqDirData,
            },
        };

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;

        for broker_id in [99i32, -1] {
            let req = AssignReplicasToDirsRequest {
                broker_id,
                broker_epoch: -1,
                directories: Vec::<ReqDirData>::new(),
                ..Default::default()
            };
            let mut buf = bytes::BytesMut::with_capacity(req.encoded_len(VERSION));
            req.encode(&mut buf, VERSION).expect("encode request");

            let resp = decode_response(
                &handle_allowed(&broker, VERSION, 12, &buf.freeze())
                    .await
                    .expect("AssignReplicasToDirs handler"),
            );
            assert!(
                resp.error_code == codes::BROKER_ID_NOT_REGISTERED,
                "broker_id {broker_id}: {resp:?}"
            );
        }
        broker_handle.shutdown().await;
    }
}
