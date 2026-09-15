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

pub(super) fn kip853_authorization_failure(api_key: i16, version: i16) -> Result<Bytes, RaftError> {
    use krabka_protocol::{Encode, owned};

    let mut output = BytesMut::new();
    let message = Some("Cluster authorization failed.".into());
    match api_key {
        API_KEY_ADD_RAFT_VOTER => owned::add_raft_voter_response::AddRaftVoterResponse {
            error_code: 31,
            error_message: message,
            ..Default::default()
        }
        .encode(&mut output, version)?,
        API_KEY_REMOVE_RAFT_VOTER => {
            owned::remove_raft_voter_response::RemoveRaftVoterResponse {
                error_code: 31,
                error_message: message,
                ..Default::default()
            }
            .encode(&mut output, version)?;
        }
        API_KEY_UPDATE_RAFT_VOTER => {
            owned::update_raft_voter_response::UpdateRaftVoterResponse {
                error_code: 31,
                ..Default::default()
            }
            .encode(&mut output, version)?;
        }
        _ => unreachable!("authorization helper called for non-mutating API"),
    }
    Ok(output.freeze())
}

async fn describe_quorum_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{Decode, Encode, owned::describe_quorum_request::DescribeQuorumRequest};

    let request = DescribeQuorumRequest::decode(&mut &body[..], version)?;
    let quorum = engine.quorum_state().await?;
    let mut output = BytesMut::new();
    super::describe_quorum::describe_quorum(&request, &quorum).encode(&mut output, version)?;
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

    /// Every KIP-853 admin API refuses an unauthorized caller in its own
    /// response type, carrying Kafka's `CLUSTER_AUTHORIZATION_FAILED`.
    ///
    /// Each arm builds a different response, so encoding one API's refusal
    /// into another's shape produces bytes the client cannot decode.
    #[test]
    fn each_kip853_admin_api_refuses_in_its_own_response_shape() {
        use krabka_protocol::owned::{
            add_raft_voter_response::{self, AddRaftVoterResponse},
            remove_raft_voter_response::{self, RemoveRaftVoterResponse},
            update_raft_voter_response::{self, UpdateRaftVoterResponse},
        };

        const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

        let bytes = kip853_authorization_failure(
            API_KEY_ADD_RAFT_VOTER,
            add_raft_voter_response::MAX_VERSION,
        )
        .expect("encode add refusal");
        let mut cursor = &bytes[..];
        let decoded =
            AddRaftVoterResponse::decode(&mut cursor, add_raft_voter_response::MAX_VERSION)
                .expect("decode add refusal");
        check!(decoded.error_code == CLUSTER_AUTHORIZATION_FAILED);
        check!(decoded.error_message.is_some(), "the refusal says why");

        let bytes = kip853_authorization_failure(
            API_KEY_REMOVE_RAFT_VOTER,
            remove_raft_voter_response::MAX_VERSION,
        )
        .expect("encode remove refusal");
        let mut cursor = &bytes[..];
        let decoded =
            RemoveRaftVoterResponse::decode(&mut cursor, remove_raft_voter_response::MAX_VERSION)
                .expect("decode remove refusal");
        check!(decoded.error_code == CLUSTER_AUTHORIZATION_FAILED);

        let bytes = kip853_authorization_failure(
            API_KEY_UPDATE_RAFT_VOTER,
            update_raft_voter_response::MAX_VERSION,
        )
        .expect("encode update refusal");
        let mut cursor = &bytes[..];
        let decoded =
            UpdateRaftVoterResponse::decode(&mut cursor, update_raft_voter_response::MAX_VERSION)
                .expect("decode update refusal");
        check!(decoded.error_code == CLUSTER_AUTHORIZATION_FAILED);
    }

    /// The listener answers `DescribeQuorum` from the live engine: the
    /// elected leader describes the metadata partition, and a request for
    /// another partition gets `UNKNOWN_TOPIC_OR_PARTITION`. The response rows
    /// are covered as whole structs in `server::describe_quorum`.
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

        // (label, topic, partition, error code, leader id)
        for (label, topic, index, error_code, leader_id) in [
            ("the metadata partition", "__cluster_metadata", 0, 0, 1),
            ("another topic", "orders", 0, 3, 0),
            ("another partition", "__cluster_metadata", 7, 3, 0),
        ] {
            let answered = describe_quorum_response(version, &body(version, topic, index), &engine)
                .await
                .expect("an answer");
            let decoded =
                DescribeQuorumResponse::decode(&mut &answered[..], version).expect("decode");
            let partition = &decoded.topics[0].partitions[0];
            check!(
                (
                    partition.partition_index,
                    partition.error_code,
                    partition.leader_id
                ) == (index, error_code, leader_id),
                "{label}"
            );
        }

        // A voter of a two-voter quorum with no leader yet leads nothing.
        let (follower, _follower_dir) = crate::server::test_support::test_engine_with_voters(
            1,
            [
                crate::server::test_support::voter(1, vec![]),
                crate::server::test_support::voter(2, vec![]),
            ],
        );
        let answered =
            describe_quorum_response(version, &body(version, "__cluster_metadata", 0), &follower)
                .await
                .expect("an answer");
        let decoded = DescribeQuorumResponse::decode(&mut &answered[..], version).expect("decode");
        check!(decoded.topics[0].partitions[0].error_code == 6);
    }

    #[tokio::test]
    async fn kip853_controller_apis_describe_exact_identity_and_reject_last_removal() {
        use krabka_protocol::{
            Encode,
            owned::{
                describe_quorum_request::{
                    DescribeQuorumRequest, PartitionData as RequestPartition,
                    TopicData as RequestTopic,
                },
                describe_quorum_response::DescribeQuorumResponse,
                remove_raft_voter_request::RemoveRaftVoterRequest,
                remove_raft_voter_response::RemoveRaftVoterResponse,
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
        check!(
            (
                partition.leader_id,
                partition.current_voters[0].replica_id,
                partition.current_voters[0].replica_directory_id.0,
                response.nodes[0].listeners[0].host.as_str(),
            ) == (1, 1, *Uuid::from_u128(1).as_bytes(), "controller-1",)
        );

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
}
