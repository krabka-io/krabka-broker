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
//! This file holds the leader check and the request-to-controller flow. The
//! two pure halves live beside it: `changes` maps a reported directory onto a
//! metadata delta, and `response` builds and encodes what goes back on the
//! wire.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::{
    Decode,
    owned::{
        assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
    },
};

use crate::{broker::Broker, codes, error::BrokerError};

mod changes;
mod response;
#[cfg(test)]
mod test_support;

use self::response::{encode_resp, not_controller_response};
pub(crate) use self::{changes::collect_assignment_changes, response::build_echo_response};

pub(crate) fn handle(
    broker: &Broker,
    version: crate::handlers::ApiVersion,
    _correlation_id: crate::handlers::CorrelationId,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AssignReplicasToDirsRequest::decode(&mut cur, version)?;

        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| is_controller_leader(Some(n.0), node_id.0));
        if !is_leader {
            return encode_resp(version, &not_controller_response());
        }

        let Ok(broker_slot_id) = u64::try_from(req.broker_id) else {
            return encode_resp(version, &AssignReplicasToDirsResponse::default());
        };
        let image = controller.current_image();
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
        let changes = collect_assignment_changes(&image, broker_slot_id, &req);

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        encode_resp(version, &build_echo_response(&req))
    })
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
        test_support::{VERSION, decode_response, request, start_broker, wait_for_leader},
        *,
    };

    #[test]
    fn leader_predicate_matches_current_node_only() {
        for (leader, want) in [(Some(1), true), (Some(2), false), (None, false)] {
            assert!(is_controller_leader(leader, 1) == want, "leader {leader:?}");
        }
    }

    #[tokio::test]
    async fn handle_leader_echoes_request_shape() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        let req = request(dir_uuid, topic_uuid, 7);

        let bytes = handle(&broker, VERSION, 9, &req)
            .await
            .expect("AssignReplicasToDirs handler");
        let resp = decode_response(&bytes);

        let expected = AssignReplicasToDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            directories: vec![RespDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![RespTopicData {
                    topic_id: ProtocolUuid(topic_uuid.into_bytes()),
                    partitions: vec![RespPartData {
                        partition_index: 7,
                        error_code: codes::NONE,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");

        broker_handle.shutdown().await;
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
        let req = request(dir_uuid, topic_uuid, 0);

        let bytes = handle(&broker, VERSION, 9, &req)
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

        let bytes = handle(
            &broker,
            VERSION,
            9,
            &request(uuid::Uuid::from_u128(0xAA), topic_uuid, 0),
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
}
