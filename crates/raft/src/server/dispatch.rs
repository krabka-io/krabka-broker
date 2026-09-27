//! Routing of an inbound RPC body to the local engine: the set of APIs the Raft
//! listener owns outright, the match that turns an API key into an [`Inbound`]
//! command, and the oneshot round-trip that waits for the encoded reply.

use bytes::Bytes;
use krabka_ids::ApiKey;
use tokio::sync::oneshot;

use super::metadata_rpc::{
    dispatch_delegation_token_mutation, dispatch_metadata_fetch, dispatch_submit_change,
};
use crate::{
    error::RaftError,
    kraft::{
        KraftController,
        transport::{Inbound, api_key},
    },
    wire::{API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE},
};

/// APIs owned by the Raft listener itself rather than its KIP-919 Admin
/// extension. These must reach `dispatch_with_router` even while the broker-side
/// Admin router is not bound yet: private `SubmitChange` is used during broker
/// self-registration.
pub(super) fn is_native_raft_api(api_key: i16) -> bool {
    matches!(
        api_key,
        api_key::FETCH
            | api_key::VOTE
            | api_key::BEGIN_QUORUM_EPOCH
            | api_key::END_QUORUM_EPOCH
            | api_key::FETCH_SNAPSHOT
            | API_KEY_SUBMIT_CHANGE
            | API_KEY_METADATA_FETCH
            | API_KEY_DELEGATION_TOKEN_MUTATION
    )
}

/// Route an inbound RPC body to the engine and produce the response body.
///
/// The KIP-595 engine RPCs (1/52/53/54) go through [`KraftController::deliver`],
/// which decodes the body, runs the core, and replies on a oneshot with the
/// encoded response body. The Krabka-private 1003/1004 keep their bespoke
/// request/response wire types.
#[cfg(test)]
#[tracing::instrument(level = "debug", skip_all, fields(node = engine.node_id().0, api_key = api_key_n.get()), err)]
pub(super) async fn dispatch(
    api_key_n: ApiKey,
    version: i16,
    body: Bytes,
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    dispatch_with_router(api_key_n, version, body, engine, None, None).await
}

/// Route one request, decoded and answered at `version`, the version its
/// header carried.
pub(super) async fn dispatch_with_router(
    api_key_n: ApiKey,
    version: i16,
    body: Bytes,
    engine: &KraftController,
    shard_router: Option<&dyn crate::RaftShardRouter>,
    principal: Option<&krabka_security::Principal>,
) -> Result<Bytes, RaftError> {
    if let Some(router) = shard_router
        && let Some(resp) = router
            .route(api_key_n.get(), body.clone(), principal)
            .await?
    {
        return Ok(resp);
    }
    match api_key_n {
        ApiKey(api_key::FETCH) => {
            deliver_inbound(engine, |reply| Inbound::Fetch {
                req: body,
                version,
                reply,
            })
            .await
        }
        ApiKey(api_key::VOTE) => {
            deliver_inbound(engine, |reply| Inbound::Vote {
                req: body,
                version,
                reply,
            })
            .await
        }
        ApiKey(api_key::BEGIN_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::BeginQuorumEpoch {
                req: body,
                version,
                reply,
            })
            .await
        }
        ApiKey(api_key::END_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::EndQuorumEpoch {
                req: body,
                version,
                reply,
            })
            .await
        }
        ApiKey(api_key::FETCH_SNAPSHOT) => {
            deliver_inbound(engine, |reply| Inbound::FetchSnapshot {
                req: body,
                version,
                reply,
            })
            .await
        }
        ApiKey(API_KEY_SUBMIT_CHANGE) => dispatch_submit_change(&body, engine).await,
        ApiKey(API_KEY_METADATA_FETCH) => dispatch_metadata_fetch(&body, engine).await,
        ApiKey(API_KEY_DELEGATION_TOKEN_MUTATION) => {
            dispatch_delegation_token_mutation(&body, engine).await
        }
        _ => Err(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("unknown controller api key"),
        )),
    }
}

/// Deliver an [`Inbound`] to the engine and await the encoded response body.
async fn deliver_inbound<F>(engine: &KraftController, make: F) -> Result<Bytes, RaftError>
where
    F: FnOnce(oneshot::Sender<Bytes>) -> Inbound,
{
    let (reply, rx) = oneshot::channel();
    engine.deliver(make(reply)).await?;
    rx.await.map_err(|_| RaftError::Shutdown)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::NodeId;
    use krabka_protocol::owned::{
        begin_quorum_epoch_request, end_quorum_epoch_request, fetch_request,
        fetch_snapshot_request, vote_request,
    };

    use super::*;
    use crate::server::{
        api_versions::table::CONTROLLER_LISTENER_APIS,
        test_support::{single_voter_engine, wait_for_leader},
    };

    #[test]
    fn private_startup_apis_bypass_the_admin_extension() {
        for api_key in [
            api_key::FETCH,
            api_key::VOTE,
            api_key::BEGIN_QUORUM_EPOCH,
            api_key::END_QUORUM_EPOCH,
            api_key::FETCH_SNAPSHOT,
            API_KEY_SUBMIT_CHANGE,
            API_KEY_METADATA_FETCH,
        ] {
            assert2::assert!(is_native_raft_api(api_key));
        }
        assert2::assert!(!is_native_raft_api(
            krabka_protocol::owned::create_topics_request::API_KEY
        ));
    }

    #[tokio::test]
    async fn dispatch_routes_kip595_peer_apis_to_engine() {
        use crate::kraft::transport::wire::{self, PeerRequest, PeerResponse};

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let vote = PeerRequest::Vote {
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            last_epoch: 0,
            last_offset: 0,
            pre_vote: false,
        }
        .encode();
        // A truncated body does not decode. Kafka closes such a connection,
        // so the engine sends no answer.
        let malformed_vote = vote.slice(..vote.len() - 1);
        let vote_resp = super::dispatch(ApiKey(api_key::VOTE), wire::VOTE_VERSION, vote, &engine)
            .await
            .expect("vote dispatch");
        assert2::assert!(PeerResponse::decode_vote(&vote_resp).is_some());
        assert2::assert!(
            super::dispatch(
                ApiKey(api_key::VOTE),
                wire::VOTE_VERSION,
                malformed_vote,
                &engine
            )
            .await
            .is_err()
        );

        let fetch = PeerRequest::Fetch {
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
            from: NodeId(2),
            current_leader_epoch: 1,
            fetch_epoch: 1,
            fetch_offset: 0,
            replica_directory_id: uuid::Uuid::nil(),
        }
        .encode();
        let fetch_resp =
            super::dispatch(ApiKey(api_key::FETCH), wire::FETCH_VERSION, fetch, &engine)
                .await
                .expect("fetch dispatch");
        assert2::assert!(PeerResponse::decode_fetch(&fetch_resp).is_some());

        let leader_epoch = engine
            .quorum_state()
            .await
            .expect("quorum state")
            .leader_epoch;
        let snapshot = PeerRequest::FetchSnapshot {
            cluster_id: None,
            from: NodeId(2),
            current_leader_epoch: i32::try_from(leader_epoch).expect("epoch fits i32"),
            snapshot_id: (10, 1),
            position: 0,
            max_bytes: 32,
        }
        .encode();
        let snapshot_resp = super::dispatch(
            ApiKey(api_key::FETCH_SNAPSHOT),
            wire::FETCH_SNAPSHOT_VERSION,
            snapshot,
            &engine,
        )
        .await
        .expect("snapshot dispatch");
        assert2::assert!(matches!(
            PeerResponse::decode_fetch_snapshot(&snapshot_resp),
            Some(PeerResponse::FetchSnapshot { error_code: 98, .. })
        ));

        let begin = PeerRequest::BeginQuorumEpoch {
            cluster_id: None,
            voter_id: NodeId(2),
            voter_directory_id: uuid::Uuid::nil(),
            leader_endpoints: Vec::new(),
            leader_id: NodeId(1),
            leader_epoch: 1,
        }
        .encode();
        let begin_resp = super::dispatch(
            ApiKey(api_key::BEGIN_QUORUM_EPOCH),
            wire::QUORUM_EPOCH_VERSION,
            begin,
            &engine,
        )
        .await
        .expect("begin dispatch");
        assert2::assert!(!begin_resp.is_empty());

        let end = PeerRequest::EndQuorumEpoch {
            cluster_id: None,
            leader_id: NodeId(1),
            leader_epoch: 1,
            preferred_candidates: Vec::new(),
        }
        .encode();
        let end_resp = super::dispatch(
            ApiKey(api_key::END_QUORUM_EPOCH),
            wire::QUORUM_EPOCH_VERSION,
            end,
            &engine,
        )
        .await
        .expect("end dispatch");
        assert2::assert!(!end_resp.is_empty());
    }

    /// Encodes `message` at `version`.
    fn encoded(message: &impl krabka_protocol::Encode, version: i16) -> Bytes {
        let mut body = bytes::BytesMut::new();
        message.encode(&mut body, version).expect("encode request");
        body.freeze()
    }

    /// Whether `body` decodes whole as a `T` at `version`.
    fn decodes_whole<T>(body: &[u8], version: i16) -> bool
    where
        T: for<'de> krabka_protocol::Decode<'de>,
    {
        let mut cur = body;
        T::decode(&mut cur, version).is_ok() && cur.is_empty()
    }

    /// Kafka advertises each KIP-595 peer api over its whole schema range and
    /// answers a request at the version it arrived at. So a request at every
    /// generated version gets an answer that decodes whole at that version,
    /// and a request at a version below the range is not answered: its body
    /// does not decode, and the connection closes.
    #[tokio::test]
    async fn each_peer_api_is_answered_at_the_request_version() {
        use krabka_protocol::owned::{
            begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
            begin_quorum_epoch_response::BeginQuorumEpochResponse,
            end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
            end_quorum_epoch_response::EndQuorumEpochResponse,
            fetch_request::{self as fetch_req, FetchRequest},
            fetch_response::FetchResponse,
            fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
            fetch_snapshot_response::FetchSnapshotResponse,
            vote_request::{self as vote_req, VoteRequest},
            vote_response::VoteResponse,
        };

        type Decodes = fn(&[u8], i16) -> bool;
        const METADATA: &str = "__cluster_metadata";

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let vote = VoteRequest {
            topics: vec![vote_req::TopicData {
                topic_name: METADATA.into(),
                partitions: vec![vote_req::PartitionData {
                    replica_epoch: 1,
                    replica_id: 2,
                    last_offset_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let begin = BeginQuorumEpochRequest {
            topics: vec![bqe_req::TopicData {
                topic_name: METADATA.into(),
                partitions: vec![bqe_req::PartitionData {
                    leader_id: 2,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let end = EndQuorumEpochRequest {
            topics: vec![eqe_req::TopicData {
                topic_name: METADATA.into(),
                partitions: vec![eqe_req::PartitionData {
                    leader_id: 2,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = FetchSnapshotRequest {
            topics: vec![fs_req::TopicSnapshot {
                name: METADATA.into(),
                partitions: vec![fs_req::PartitionSnapshot::default()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let fetch = FetchRequest {
            replica_id: 2,
            replica_state: fetch_req::ReplicaState {
                replica_id: 2,
                ..Default::default()
            },
            topics: vec![fetch_req::FetchTopic {
                topic: METADATA.into(),
                topic_id: crate::kraft::transport::wire::METADATA_TOPIC_ID,
                partitions: vec![fetch_req::FetchPartition::default()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let mut cases: Vec<(&str, i16, i16, Bytes, Decodes)> = Vec::new();
        for version in -1..=vote_request::MAX_VERSION {
            let body = encoded(&vote, version.max(0));
            cases.push((
                "Vote",
                api_key::VOTE,
                version,
                body,
                decodes_whole::<VoteResponse>,
            ));
        }
        for version in -1..=begin_quorum_epoch_request::MAX_VERSION {
            cases.push((
                "BeginQuorumEpoch",
                api_key::BEGIN_QUORUM_EPOCH,
                version,
                encoded(&begin, version.max(0)),
                decodes_whole::<BeginQuorumEpochResponse>,
            ));
        }
        for version in -1..=end_quorum_epoch_request::MAX_VERSION {
            cases.push((
                "EndQuorumEpoch",
                api_key::END_QUORUM_EPOCH,
                version,
                encoded(&end, version.max(0)),
                decodes_whole::<EndQuorumEpochResponse>,
            ));
        }
        for version in -1..=fetch_snapshot_request::MAX_VERSION {
            cases.push((
                "FetchSnapshot",
                api_key::FETCH_SNAPSHOT,
                version,
                encoded(&snapshot, version.max(0)),
                decodes_whole::<FetchSnapshotResponse>,
            ));
        }
        for version in fetch_request::MIN_VERSION - 1..=fetch_request::MAX_VERSION {
            cases.push((
                "Fetch",
                api_key::FETCH,
                version,
                encoded(&fetch, version.max(fetch_request::MIN_VERSION)),
                decodes_whole::<FetchResponse>,
            ));
        }

        for (name, key, version, body, decodes) in cases {
            let served = CONTROLLER_LISTENER_APIS
                .iter()
                .find(|api| api.api_key == key)
                .is_some_and(|api| (api.min_version..=api.max_version).contains(&version));
            let answer = super::dispatch(ApiKey(key), version, body, &engine).await;
            match answer {
                Ok(answer) => {
                    check!(
                        served && decodes(&answer, version),
                        "{name} v{version} is answered at its own version"
                    );
                }
                Err(_) => {
                    check!(!served, "{name} v{version} is not answered");
                }
            }
        }
    }
}
