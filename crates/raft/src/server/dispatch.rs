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
    body: Bytes,
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    dispatch_with_router(api_key_n, body, engine, None, None).await
}

pub(super) async fn dispatch_with_router(
    api_key_n: ApiKey,
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
            deliver_inbound(engine, |reply| Inbound::Fetch { req: body, reply }).await
        }
        ApiKey(api_key::VOTE) => {
            deliver_inbound(engine, |reply| Inbound::Vote { req: body, reply }).await
        }
        ApiKey(api_key::BEGIN_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::BeginQuorumEpoch {
                req: body,
                reply,
            })
            .await
        }
        ApiKey(api_key::END_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::EndQuorumEpoch { req: body, reply }).await
        }
        ApiKey(api_key::FETCH_SNAPSHOT) => {
            deliver_inbound(engine, |reply| Inbound::FetchSnapshot { req: body, reply }).await
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
    use krabka_metadata::NodeId;

    use super::*;
    use crate::server::test_support::{single_voter_engine, wait_for_leader};

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
        use crate::kraft::transport::wire::{PeerRequest, PeerResponse};

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
        let mut malformed_vote = vote.to_vec();
        malformed_vote.push(0);
        let vote_resp = super::dispatch(ApiKey(api_key::VOTE), vote, &engine)
            .await
            .expect("vote dispatch");
        assert2::assert!(PeerResponse::decode_vote(&vote_resp).is_some());
        let malformed_vote_resp =
            super::dispatch(ApiKey(api_key::VOTE), Bytes::from(malformed_vote), &engine)
                .await
                .expect("malformed vote dispatch returns a denial");
        assert2::assert!(matches!(
            PeerResponse::decode_vote(&malformed_vote_resp),
            Some(PeerResponse::Vote { granted: false, .. })
        ));

        let fetch = PeerRequest::Fetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 0,
            replica_directory_id: uuid::Uuid::nil(),
        }
        .encode();
        let fetch_resp = super::dispatch(ApiKey(api_key::FETCH), fetch, &engine)
            .await
            .expect("fetch dispatch");
        assert2::assert!(PeerResponse::decode_fetch(&fetch_resp).is_some());

        let snapshot = PeerRequest::FetchSnapshot {
            from: NodeId(2),
            snapshot_id: (10, 1),
            position: 0,
            max_bytes: 32,
        }
        .encode();
        let snapshot_resp = super::dispatch(ApiKey(api_key::FETCH_SNAPSHOT), snapshot, &engine)
            .await
            .expect("snapshot dispatch");
        assert2::assert!(matches!(
            PeerResponse::decode_fetch_snapshot(&snapshot_resp),
            Some(PeerResponse::FetchSnapshot { error_code: 98, .. })
        ));

        let begin = PeerRequest::BeginQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        }
        .encode();
        let begin_resp = super::dispatch(ApiKey(api_key::BEGIN_QUORUM_EPOCH), begin, &engine)
            .await
            .expect("begin dispatch");
        assert2::assert!(!begin_resp.is_empty());

        let end = PeerRequest::EndQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        }
        .encode();
        let end_resp = super::dispatch(ApiKey(api_key::END_QUORUM_EPOCH), end, &engine)
            .await
            .expect("end dispatch");
        assert2::assert!(!end_resp.is_empty());
    }
}
