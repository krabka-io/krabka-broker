//! The KIP-853 controller admin surface: the API keys the listener owns, the
//! router that picks a handler for one of them, the `DescribeQuorum` read, and
//! the per-API refusal an unauthorized caller receives.

use bytes::{Bytes, BytesMut};

use super::voter_admin::{
    add_raft_voter_response, remove_raft_voter_response, update_raft_voter_response,
};
use crate::{error::RaftError, kraft::KraftController};

pub(super) const API_KEY_DESCRIBE_QUORUM: i16 = 55;
pub(super) const API_KEY_ADD_RAFT_VOTER: i16 = 80;
pub(super) const API_KEY_REMOVE_RAFT_VOTER: i16 = 81;
pub(super) const API_KEY_UPDATE_RAFT_VOTER: i16 = 82;

pub(super) async fn kip853_admin_response(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key {
        API_KEY_DESCRIBE_QUORUM => describe_quorum_response(version, body, engine).await,
        API_KEY_ADD_RAFT_VOTER => add_raft_voter_response(version, body, engine).await,
        API_KEY_REMOVE_RAFT_VOTER => remove_raft_voter_response(version, body, engine).await,
        API_KEY_UPDATE_RAFT_VOTER => update_raft_voter_response(version, body, engine).await,
        _ => unreachable!("filtered KIP-853 admin API key"),
    }
}

async fn describe_quorum_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            common::describe_quorum_response::replica_state::ReplicaState,
            describe_quorum_request::DescribeQuorumRequest,
            describe_quorum_response::{
                DescribeQuorumResponse, Listener, Node, PartitionData, TopicData,
            },
        },
    };

    let mut input = body;
    let request = DescribeQuorumRequest::decode(&mut input, version)?;
    let quorum = engine.quorum_state().await?;
    let topics = request
        .topics
        .into_iter()
        .map(|topic| TopicData {
            topic_name: topic.topic_name.clone(),
            partitions: topic
                .partitions
                .into_iter()
                .map(|partition| {
                    if topic.topic_name != "__cluster_metadata" || partition.partition_index != 0 {
                        return PartitionData {
                            partition_index: partition.partition_index,
                            error_code: 17,
                            error_message: Some(
                                "DescribeQuorum supports only __cluster_metadata".into(),
                            ),
                            leader_id: -1,
                            leader_epoch: -1,
                            high_watermark: -1,
                            ..Default::default()
                        };
                    }
                    let state = |id: crate::NodeId, directory_id: uuid::Uuid| ReplicaState {
                        replica_id: i32::try_from(id.0).unwrap_or(-1),
                        replica_directory_id: krabka_protocol::primitives::uuid::Uuid(
                            *directory_id.as_bytes(),
                        ),
                        log_end_offset: quorum
                            .per_replica_fetch_offset
                            .get(&id)
                            .copied()
                            .unwrap_or(-1),
                        ..Default::default()
                    };
                    PartitionData {
                        partition_index: 0,
                        error_code: 0,
                        error_message: None,
                        leader_id: quorum
                            .leader_id
                            .and_then(|id| i32::try_from(id.0).ok())
                            .unwrap_or(-1),
                        leader_epoch: i32::try_from(quorum.leader_epoch).unwrap_or(i32::MAX),
                        high_watermark: quorum.high_watermark,
                        current_voters: quorum
                            .voters
                            .iter()
                            .map(|voter| state(voter.id, voter.directory_id))
                            .collect(),
                        observers: quorum
                            .observers
                            .iter()
                            .map(|id| state(*id, uuid::Uuid::nil()))
                            .collect(),
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                    }
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    let nodes = quorum
        .voters
        .iter()
        .map(|voter| Node {
            node_id: i32::try_from(voter.id.0).unwrap_or(-1),
            listeners: voter
                .endpoints
                .iter()
                .map(|endpoint| Listener {
                    name: endpoint.name.clone(),
                    host: endpoint.host.clone(),
                    port: endpoint.port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    let mut output = BytesMut::new();
    DescribeQuorumResponse {
        topics,
        nodes,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::Decode;
    use uuid::Uuid;

    use super::*;
    use crate::server::test_support::{
        activate_dynamic_membership, single_voter_engine, wait_for_leader,
    };

    /// `DescribeQuorum` answers for `__cluster_metadata` partition 0 and
    /// refuses everything else with a partition-level error.
    ///
    /// The refusal has to name the partition asked for and blank the leader,
    /// epoch and high watermark, or a client reads a real quorum's numbers off
    /// a topic that has none. And the real answer has to carry the leader,
    /// epoch, watermark and voter list -- each read from the quorum
    /// separately, so each can be wrong on its own.
    #[tokio::test]
    async fn describe_quorum_answers_only_for_the_metadata_partition() {
        use krabka_protocol::{
            Encode as _,
            owned::{
                describe_quorum_request::{self, DescribeQuorumRequest, PartitionData, TopicData},
                describe_quorum_response::DescribeQuorumResponse,
            },
        };

        fn body(version: i16, topic: &str, partition: i32) -> Bytes {
            let request = DescribeQuorumRequest {
                topics: vec![TopicData {
                    topic_name: topic.to_owned(),
                    partitions: vec![PartitionData {
                        partition_index: partition,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            request.encode(&mut buf, version).expect("encode request");
            buf.freeze()
        }

        let version = describe_quorum_request::MAX_VERSION;
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let answered =
            describe_quorum_response(version, &body(version, "__cluster_metadata", 0), &engine)
                .await
                .expect("describe the metadata quorum");
        let mut cursor = &answered[..];
        let decoded = DescribeQuorumResponse::decode(&mut cursor, version).expect("decode");
        let partition = &decoded.topics[0].partitions[0];
        check!(
            partition.error_code == 0,
            "the metadata partition is answered"
        );
        check!(partition.partition_index == 0);
        check!(partition.leader_id >= 0, "a leader was elected");
        check!(partition.leader_epoch >= 0);
        check!(partition.high_watermark >= 0);
        check!(
            !partition.current_voters.is_empty(),
            "the answer carries the voter set"
        );

        // Anything else is refused, and the refusal keeps the partition it was
        // asked about while blanking the quorum numbers.
        for (what, topic, index) in [
            ("another topic", "orders", 0),
            (
                "the right topic, another partition",
                "__cluster_metadata",
                7,
            ),
        ] {
            let refused = describe_quorum_response(version, &body(version, topic, index), &engine)
                .await
                .expect("refuse politely");
            let mut cursor = &refused[..];
            let decoded = DescribeQuorumResponse::decode(&mut cursor, version).expect("decode");
            let partition = &decoded.topics[0].partitions[0];
            check!(partition.error_code == 17, "{what}: error code");
            check!(
                partition.partition_index == index,
                "{what}: keeps the index"
            );
            check!(
                (
                    partition.leader_id,
                    partition.leader_epoch,
                    partition.high_watermark
                ) == (-1, -1, -1),
                "{what}: the quorum numbers are blank"
            );
            check!(partition.error_message.is_some(), "{what}: says why");
        }
    }

    #[tokio::test]
    async fn kip853_controller_apis_describe_exact_identity_and_reject_last_removal() {
        use krabka_protocol::{
            Encode,
            owned::{
                add_raft_voter_request::AddRaftVoterRequest,
                add_raft_voter_response::AddRaftVoterResponse,
                describe_quorum_request::{
                    DescribeQuorumRequest, PartitionData as RequestPartition,
                    TopicData as RequestTopic,
                },
                describe_quorum_response::DescribeQuorumResponse,
                remove_raft_voter_request::RemoveRaftVoterRequest,
                remove_raft_voter_response::RemoveRaftVoterResponse,
                update_raft_voter_request::UpdateRaftVoterRequest,
                update_raft_voter_response::UpdateRaftVoterResponse,
            },
        };

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        activate_dynamic_membership(&engine).await;

        let describe = DescribeQuorumRequest {
            topics: vec![RequestTopic {
                topic_name: "__cluster_metadata".into(),
                partitions: vec![RequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut request_body = bytes::BytesMut::new();
        describe.encode(&mut request_body, 2).unwrap();
        let response_body =
            super::kip853_admin_response(API_KEY_DESCRIBE_QUORUM, 2, &request_body, &engine)
                .await
                .expect("DescribeQuorum");
        let mut response_bytes = response_body.as_ref();
        let response = DescribeQuorumResponse::decode(&mut response_bytes, 2).unwrap();
        let partition = &response.topics[0].partitions[0];
        check!(response.topics[0].topic_name == "__cluster_metadata");
        check!(partition.partition_index == 0);
        check!(partition.leader_id == 1);
        check!(partition.leader_epoch >= 1);
        check!(partition.high_watermark == engine.quorum_state().await.unwrap().high_watermark);
        check!(partition.current_voters[0].replica_id == 1);
        check!(
            partition.current_voters[0].replica_directory_id.0 == *Uuid::from_u128(1).as_bytes()
        );
        check!(partition.current_voters[0].log_end_offset >= 0);
        check!(partition.observers.is_empty());
        check!(response.nodes[0].node_id == 1);
        check!(response.nodes[0].listeners[0].name == "CONTROLLER");
        check!(response.nodes[0].listeners[0].host == "controller-1");
        check!(response.nodes[0].listeners[0].port == 9093);

        // Deliver Inbound::Fetch from observer so partition.observers is non-empty
        let req = crate::kraft::transport::wire::PeerRequest::Fetch {
            from: crate::NodeId(99),
            fetch_epoch: partition.leader_epoch.cast_unsigned(),
            fetch_offset: 0,
            replica_directory_id: Uuid::from_u128(99),
        };
        let req_bytes = req.try_encode().expect("encode fetch");
        let (tx, rx) = tokio::sync::oneshot::channel();
        engine
            .deliver(crate::kraft::transport::Inbound::Fetch {
                req: req_bytes,
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await;

        let describe_body = {
            let mut buf = bytes::BytesMut::new();
            describe.encode(&mut buf, 2).unwrap();
            buf
        };
        let response_body =
            super::kip853_admin_response(API_KEY_DESCRIBE_QUORUM, 2, &describe_body, &engine)
                .await
                .expect("DescribeQuorum with observer");
        let mut response_bytes = response_body.as_ref();
        let response = DescribeQuorumResponse::decode(&mut response_bytes, 2).unwrap();
        let partition = &response.topics[0].partitions[0];
        check!(partition.observers.len() == 1);
        check!(partition.observers[0].replica_id == 99);

        // Test API_KEY_ADD_RAFT_VOTER through kip853_admin_response
        let add = AddRaftVoterRequest {
            cluster_id: None,
            voter_id: -1,
            ..Default::default()
        };
        let mut add_body = bytes::BytesMut::new();
        add.encode(&mut add_body, 0).unwrap();
        let add_resp_body =
            super::kip853_admin_response(API_KEY_ADD_RAFT_VOTER, 0, &add_body, &engine)
                .await
                .expect("AddRaftVoter");
        let mut add_resp_bytes = add_resp_body.as_ref();
        let add_resp = AddRaftVoterResponse::decode(&mut add_resp_bytes, 0).unwrap();
        check!(add_resp.error_code == 42);

        // Test API_KEY_UPDATE_RAFT_VOTER through kip853_admin_response
        let update = UpdateRaftVoterRequest {
            cluster_id: Some("00000000-0000-0000-0000-0000000000ff".into()),
            ..Default::default()
        };
        let mut update_body = bytes::BytesMut::new();
        update.encode(&mut update_body, 0).unwrap();
        let update_resp_body =
            super::kip853_admin_response(API_KEY_UPDATE_RAFT_VOTER, 0, &update_body, &engine)
                .await
                .expect("UpdateRaftVoter");
        let mut update_resp_bytes = update_resp_body.as_ref();
        let update_resp = UpdateRaftVoterResponse::decode(&mut update_resp_bytes, 0).unwrap();
        check!(update_resp.error_code == 104);

        let remove = RemoveRaftVoterRequest {
            cluster_id: Some(engine.current_image().cluster_id().to_string()),
            voter_id: 1,
            voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                *Uuid::from_u128(1).as_bytes(),
            ),
            ..Default::default()
        };
        let mut request_body = bytes::BytesMut::new();
        remove.encode(&mut request_body, 0).unwrap();
        let response_body =
            super::kip853_admin_response(API_KEY_REMOVE_RAFT_VOTER, 0, &request_body, &engine)
                .await
                .expect("RemoveRaftVoter");
        let mut response_bytes = response_body.as_ref();
        let response = RemoveRaftVoterResponse::decode(&mut response_bytes, 0).unwrap();
        assert2::assert!(response.error_code == 42);
        assert2::assert!(
            response
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("last voter"))
        );

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn describe_quorum_response_fields_and_unestablished_leader() {
        use krabka_protocol::{
            Encode,
            owned::{
                describe_quorum_request::{
                    DescribeQuorumRequest, PartitionData as RequestPartition,
                    TopicData as RequestTopic,
                },
                describe_quorum_response::DescribeQuorumResponse,
            },
        };

        use crate::server::test_support::{
            controller_endpoint, test_engine_with_voters, topic_record, voter,
        };

        // 1. Controller with two voters before election:
        // leader_id is None -> mapped to -1.
        // Voter 2 has never fetched -> log_end_offset is -1.
        let (engine, _dir) = test_engine_with_voters(
            1,
            [
                voter(1, vec![controller_endpoint("controller-1", 9093)]),
                voter(2, vec![controller_endpoint("controller-2", 9093)]),
                voter(u64::MAX, vec![controller_endpoint("controller-max", 9093)]),
            ],
        );
        let describe = DescribeQuorumRequest {
            topics: vec![RequestTopic {
                topic_name: "__cluster_metadata".into(),
                partitions: vec![RequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut req_body = bytes::BytesMut::new();
        describe.encode(&mut req_body, 2).unwrap();
        let resp_body = super::describe_quorum_response(2, &req_body, &engine)
            .await
            .unwrap();
        let resp = DescribeQuorumResponse::decode(&mut resp_body.as_ref(), 2).unwrap();
        let part = &resp.topics[0].partitions[0];
        assert2::assert!(part.leader_id == -1);

        let v2 = part
            .current_voters
            .iter()
            .find(|v| v.replica_id == 2)
            .expect("voter 2");
        assert2::assert!(v2.log_end_offset == -1);
        let vmax = part
            .current_voters
            .iter()
            .find(|v| {
                v.replica_directory_id
                    == krabka_protocol::primitives::uuid::Uuid(
                        *uuid::Uuid::from_u128(u128::from(u64::MAX)).as_bytes(),
                    )
            })
            .expect("voter max");
        assert2::assert!(vmax.replica_id == -1);

        let n2 = resp.nodes.iter().find(|n| n.node_id == 2).expect("node 2");
        assert2::assert!(n2.node_id == 2);
        let nmax = resp
            .nodes
            .iter()
            .find(|n| n.listeners.iter().any(|l| l.host == "controller-max"))
            .expect("node max");
        assert2::assert!(nmax.node_id == -1);
        engine.shutdown().await;

        // 2. Controller with leader and committed record:
        // Voter 1 is leader with log_end_offset >= 1.
        let (engine2, _dir2) = single_voter_engine();
        wait_for_leader(&engine2).await;
        engine2
            .submit_change(vec![topic_record("t1")])
            .await
            .expect("submit");

        let mut req_body2 = bytes::BytesMut::new();
        describe.encode(&mut req_body2, 2).unwrap();
        let resp_body2 = super::describe_quorum_response(2, &req_body2, &engine2)
            .await
            .unwrap();
        let resp2 = DescribeQuorumResponse::decode(&mut resp_body2.as_ref(), 2).unwrap();
        let part2 = &resp2.topics[0].partitions[0];
        assert2::assert!(part2.leader_id == 1);
        assert2::assert!(part2.current_voters[0].replica_id == 1);
        assert2::assert!(part2.current_voters[0].log_end_offset >= 1);
        assert2::assert!(resp2.nodes[0].node_id == 1);
        engine2.shutdown().await;
    }
}
