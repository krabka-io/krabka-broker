//! `UpdateRaftVoter` (`api_key=82`, KIP-853). Admin RPC that rewrites an
//! existing voter's listeners and its supported `kraft.version` range.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! The outcome → error code mapping is shared with
//! [`super::add_raft_voter`]. `UpdateVoter` never returns
//! `VoterNotCaughtUp`. An unknown voter id comes back as
//! `ReconfigRejected → INVALID_REQUEST`.
//!
//! Request validation follows `KafkaRaftClient.handleUpdateVoterRequest` in
//! the pinned image: a cluster id that names another cluster is
//! `INCONSISTENT_CLUSTER_ID (104)`, a leader epoch on either side of the
//! quorum's is `FENCED_LEADER_EPOCH (74)` or `UNKNOWN_LEADER_EPOCH (75)`, and
//! everything else malformed is `INVALID_REQUEST (42)`. Kafka assigns no
//! separate "invalid voter update" code.
//!
//! A request that carries no cluster id at all passes the first check, because
//! `KafkaRaftClient.hasValidClusterId` returns true for a null cluster id. The
//! add and remove paths already read it that way.

use bytes::Bytes;
use krabka_metadata::{Voter, VoterEndpoint};
use krabka_protocol::{
    Decode,
    owned::{
        update_raft_voter_request::UpdateRaftVoterRequest,
        update_raft_voter_response::UpdateRaftVoterResponse,
    },
};
use krabka_raft::reconfig::UpdateVoter;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{add_raft_voter::outcome_to_code, cluster_alter_denied},
};

#[tracing::instrument(
    name = "handle_update_raft_voter",
    level = "info",
    skip_all,
    fields(api = "UpdateRaftVoter", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = UpdateRaftVoterRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return refuse(version, codes::CLUSTER_AUTHORIZATION_FAILED);
    }

    let cluster_id = image.cluster_id().to_string();
    let quorum = broker.controller.quorum_state();
    if req
        .cluster_id
        .as_deref()
        .is_some_and(|request_cluster| request_cluster != cluster_id)
    {
        return refuse(version, codes::INCONSISTENT_CLUSTER_ID);
    }

    let current_epoch = i64::try_from(quorum.current_term).unwrap_or(i64::MAX);
    let requested_epoch = i64::from(req.current_leader_epoch);
    if requested_epoch < current_epoch {
        return refuse(version, codes::FENCED_LEADER_EPOCH);
    }
    if requested_epoch > current_epoch {
        return refuse(version, codes::UNKNOWN_LEADER_EPOCH);
    }

    if req.voter_directory_id == krabka_protocol::primitives::uuid::Uuid::ZERO
        || req.listeners.is_empty()
        || req.listeners.iter().any(|listener| {
            listener.name.is_empty() || listener.host.is_empty() || listener.port == 0
        })
    {
        return refuse(version, codes::INVALID_REQUEST);
    }

    let Ok(min_version) = u16::try_from(req.k_raft_version_feature.min_supported_version) else {
        return refuse(version, codes::INVALID_REQUEST);
    };
    let Ok(max_version) = u16::try_from(req.k_raft_version_feature.max_supported_version) else {
        return refuse(version, codes::INVALID_REQUEST);
    };
    if min_version > max_version {
        return refuse(version, codes::INVALID_REQUEST);
    }

    let Ok(id) = u64::try_from(req.voter_id) else {
        return refuse(version, codes::INVALID_REQUEST);
    };

    let voter = Voter {
        id: krabka_raft::NodeId(id),
        directory_id: uuid::Uuid::from_bytes(req.voter_directory_id.0),
        endpoints: req
            .listeners
            .into_iter()
            .map(|l| VoterEndpoint {
                name: l.name,
                host: l.host,
                port: l.port,
            })
            .collect(),
        kraft_version: krabka_metadata::KRaftVersionRange {
            min: min_version,
            max: max_version,
        },
    };

    let (error_code, _msg) =
        outcome_to_code(broker.controller.update_voter(UpdateVoter { voter }).await);

    encode_resp(
        version,
        &UpdateRaftVoterResponse {
            error_code,
            ..Default::default()
        },
    )
}

fn encode_resp(version: i16, resp: &UpdateRaftVoterResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

/// Encodes a response that carries nothing but `error_code`.
fn refuse(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    encode_resp(
        version,
        &UpdateRaftVoterResponse {
            error_code,
            ..Default::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::{
        owned::update_raft_voter_request::{KRaftVersionFeature, Listener},
        primitives::uuid::Uuid as ProtoUuid,
    };
    use krabka_security::{AuthMethod, Principal};

    use crate::test_support::DenyAll;

    fn request(voter_id: i32) -> UpdateRaftVoterRequest {
        UpdateRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            current_leader_epoch: 1,
            voter_id,
            voter_directory_id: ProtoUuid([4; 16]),
            listeners: vec![Listener {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
                ..Default::default()
            }],
            k_raft_version_feature: KRaftVersionFeature {
                min_supported_version: 1,
                max_supported_version: 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Applies one malformation to an otherwise well-formed request.
    type Mutate = fn(&mut UpdateRaftVoterRequest);

    crate::test_support::wire_helpers!(
        UpdateRaftVoterRequest,
        UpdateRaftVoterResponse,
        client_id = "admin-client"
    );

    use super::*;
    use crate::test_support::start_broker_with_authorizer as start_broker;

    /// Decode and encode round-trip at the min and max versions.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use krabka_protocol::owned::update_raft_voter_response::{self, UpdateRaftVoterResponse};
        for version in [
            update_raft_voter_response::MIN_VERSION,
            update_raft_voter_response::MAX_VERSION,
        ] {
            let resp = UpdateRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                ..Default::default()
            };
            let bytes = encode_resp(version, &resp).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = UpdateRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(decoded.error_code == codes::INVALID_REQUEST);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = 0;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(2), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = 0;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let mut request = request(-7);
        request.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        request.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let req_bytes = encode_request(&request, version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::INVALID_REQUEST);
        broker_handle.shutdown().await;
    }

    /// Each rejected field carries the code that
    /// `KafkaRaftClient.handleUpdateVoterRequest` carries for it. None of
    /// them is voter-specific: KIP-853 adds no "invalid voter update" code.
    #[tokio::test]
    async fn handle_reports_the_kafka_code_for_each_rejected_field() {
        let version = 0;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let cluster_id = broker.controller.current_image().cluster_id().to_string();
        let epoch = i32::try_from(broker.controller.quorum_state().current_term)
            .expect("the test quorum's term fits an i32");
        let well_formed = || {
            let mut req = request(2);
            req.cluster_id = Some(cluster_id.clone());
            req.current_leader_epoch = epoch;
            req
        };

        let cases: [(&str, Mutate, i16); 6] = [
            (
                "another cluster's id",
                |req| req.cluster_id = Some("not-this-cluster".into()),
                codes::INCONSISTENT_CLUSTER_ID,
            ),
            (
                "an epoch the quorum has left behind",
                |req| req.current_leader_epoch -= 1,
                codes::FENCED_LEADER_EPOCH,
            ),
            (
                "an epoch ahead of the quorum's",
                |req| req.current_leader_epoch += 1,
                codes::UNKNOWN_LEADER_EPOCH,
            ),
            (
                "a zero voter directory id",
                |req| req.voter_directory_id = ProtoUuid([0; 16]),
                codes::INVALID_REQUEST,
            ),
            (
                "no listeners at all",
                |req| req.listeners.clear(),
                codes::INVALID_REQUEST,
            ),
            (
                "an inverted kraft.version range",
                |req| req.k_raft_version_feature.min_supported_version = 2,
                codes::INVALID_REQUEST,
            ),
        ];

        for (what, mutate, want) in cases {
            let mut req = well_formed();
            mutate(&mut req);
            let req_bytes = encode_request(&req, version);
            let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp, version);
            assert!(resp.error_code == want, "{what}");
        }
        broker_handle.shutdown().await;
    }

    /// `KafkaRaftClient.hasValidClusterId` answers true for a request that
    /// carries no cluster id, so an absent one is not an inconsistent one: the
    /// request runs the rest of the checks and reaches the voter set, which
    /// holds no voter 2.
    #[tokio::test]
    async fn handle_accepts_a_request_that_names_no_cluster() {
        let version = 0;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let mut named = request(2);
        named.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        named.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let mut anonymous = named.clone();
        anonymous.cluster_id = None;

        let mut codes_seen = Vec::new();
        for req in [named, anonymous] {
            let req_bytes = encode_request(&req, version);
            let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
                .await
                .expect("handle");
            codes_seen.push(decode_response(&resp, version).error_code);
        }

        assert!(codes_seen == vec![codes::VOTER_NOT_FOUND, codes::VOTER_NOT_FOUND]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_reconfig_error_from_controller() {
        let version = 0;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let mut request = request(2);
        request.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        request.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let req_bytes = encode_request(&request, version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::VOTER_NOT_FOUND);
        broker_handle.shutdown().await;
    }
}
