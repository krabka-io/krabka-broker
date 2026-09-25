//! `UpdateRaftVoter` (`api_key=82`, KIP-853). Admin RPC that rewrites an
//! existing voter's listeners and its supported `kraft.version` range.
//!
//! ## ACL
//!
//! `ClusterAction` on `Cluster("kafka-cluster")`, matching
//! `ControllerApis.handleUpdateRaftVoter`, which calls
//! `authorizeClusterOperation(request, CLUSTER_ACTION)`. This differs from
//! `AddRaftVoter` and `RemoveRaftVoter`, which both check `Alter`: a
//! controller sends `UpdateRaftVoter` for itself to update its own endpoints
//! (KIP-853), and its principal usually holds `ClusterAction` rather than
//! `Alter`. Deny → whole-response `error_code =
//! CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! After the ACL gate, the request runs the controller listener's own checks
//! in `krabka_raft::voter_requests`, which follow
//! `KafkaRaftClient.handleUpdateVoterRequest` and `UpdateVoterHandler`: a
//! cluster id that names another cluster is `INCONSISTENT_CLUSTER_ID (104)`, a
//! leader epoch on either side of the quorum's is `FENCED_LEADER_EPOCH (74)` or
//! `UNKNOWN_LEADER_EPOCH (75)`, a node that is not the leader answers
//! `NOT_LEADER_OR_FOLLOWER (6)`, and everything else malformed, including
//! listeners without the leader's controller listener name, is
//! `INVALID_REQUEST (42)`. Every answer after the ACL gate names the leader in
//! `CurrentLeader`.
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
use krabka_raft::{reconfig::UpdateVoter, voter_requests};

use crate::{broker::Broker, codes, error::BrokerError, handlers::cluster_action_denied};

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

    if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return refuse(version, codes::CLUSTER_AUTHORIZATION_FAILED);
    }

    // Broker-only observer forward to the active controller quorum (#392)
    if let Some(forwarded) = broker
        .controller
        .forward_raw(82, version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        return forwarded.map_err(BrokerError::from);
    }

    // The request checks, their order and their codes are the controller
    // listener's own (`krabka_raft::voter_requests`).
    let Some(quorum) = broker.controller.quorum_snapshot() else {
        return refuse(version, voter_requests::NOT_LEADER_OR_FOLLOWER);
    };
    let error_code = if let Some(code) =
        voter_requests::update_voter_refusal(&req, &image.cluster_id().to_string(), &quorum)
    {
        code
    } else {
        let feature = &req.k_raft_version_feature;
        let kraft_version = krabka_metadata::KRaftVersionRange {
            min: u16::try_from(feature.min_supported_version).unwrap_or_default(),
            max: u16::try_from(feature.max_supported_version).unwrap_or_default(),
        };
        let (voter_id, directory_id) = (req.voter_id, req.voter_directory_id);
        let voter = Voter {
            id: krabka_raft::NodeId(u64::try_from(voter_id).unwrap_or_default()),
            directory_id: uuid::Uuid::from_bytes(directory_id.0),
            endpoints: req
                .listeners
                .into_iter()
                .map(|l| VoterEndpoint {
                    name: l.name,
                    host: l.host,
                    port: l.port,
                })
                .collect(),
            kraft_version,
        };
        voter_requests::reconfiguration_refusal(
            broker.controller.update_voter(UpdateVoter { voter }).await,
            voter_id,
            directory_id,
        )
        .0
    };

    // Kafka's `RaftUtil.updateVoterResponse` names the leader in every answer.
    let quorum = broker.controller.quorum_snapshot().unwrap_or(quorum);
    encode_resp(
        version,
        &UpdateRaftVoterResponse {
            error_code,
            current_leader: voter_requests::update_voter_current_leader(&quorum),
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

        let cases: [(&str, Mutate, i16); 7] = [
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
            (
                "listeners without the leader's controller listener",
                |req| req.listeners[0].name = "PLAINTEXT".into(),
                codes::INVALID_REQUEST,
            ),
        ];
        let leader_id = i32::try_from(broker.config.node_id.0).expect("node id fits an i32");

        for (what, mutate, want) in cases {
            let mut req = well_formed();
            mutate(&mut req);
            let req_bytes = encode_request(&req, version);
            let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp, version);
            assert!(resp.error_code == want, "{what}");
            // Every refusal names the leader, as `RaftUtil.updateVoterResponse`
            // fills it.
            assert!(
                (
                    resp.current_leader.leader_id,
                    resp.current_leader.leader_epoch,
                ) == (leader_id, epoch),
                "{what}"
            );
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

    /// `UpdateRaftVoter` checks `ClusterAction`, matching Kafka's
    /// `ControllerApis.handleUpdateRaftVoter`. `AddRaftVoter` and
    /// `RemoveRaftVoter` keep checking `Alter` (#688): a principal with only
    /// the operation each api actually needs gets past the ACL gate, and a
    /// principal with only the other one does not.
    #[tokio::test]
    async fn each_raft_voter_api_checks_its_own_cluster_operation() {
        use assert2::check;
        use krabka_metadata::AclOperation;
        use krabka_protocol::owned::{
            add_raft_voter_request::{self, AddRaftVoterRequest},
            add_raft_voter_response::AddRaftVoterResponse,
            remove_raft_voter_request::RemoveRaftVoterRequest,
            remove_raft_voter_response::RemoveRaftVoterResponse,
        };

        /// Authorizer that allows exactly one cluster operation and denies
        /// every other one, including on other resources.
        #[derive(Debug)]
        struct GrantOnly(AclOperation);

        impl crate::authorizer::Authorizer for GrantOnly {
            fn authorize(
                &self,
                _source: &dyn krabka_authz::AclSource,
                req: &crate::authorizer::AuthorizationRequest<'_>,
            ) -> crate::authorizer::AuthorizationResult {
                if req.operation == self.0 {
                    crate::authorizer::AuthorizationResult::Allow
                } else {
                    crate::authorizer::AuthorizationResult::Deny
                }
            }
        }

        enum Api {
            Update,
            Add,
            Remove,
        }

        let version = 0;
        let cases: [(&str, Api, AclOperation, bool); 5] = [
            (
                "UpdateRaftVoter",
                Api::Update,
                AclOperation::ClusterAction,
                false,
            ),
            ("UpdateRaftVoter", Api::Update, AclOperation::Alter, true),
            ("AddRaftVoter", Api::Add, AclOperation::Alter, false),
            ("AddRaftVoter", Api::Add, AclOperation::ClusterAction, true),
            ("RemoveRaftVoter", Api::Remove, AclOperation::Alter, false),
        ];

        for (api_name, api, grant, want_cluster_authorization_failed) in cases {
            let (broker_handle, _dir) = start_broker(Arc::new(GrantOnly(grant))).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = Principal {
                name: "alice".into(),
                auth_method: AuthMethod::Anonymous,
                groups: Vec::new(),
            };
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);

            let error_code = match api {
                Api::Update => {
                    let req_bytes = encode_request(&request(2), version);
                    let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
                        .await
                        .expect("handle");
                    decode_response(&resp, version).error_code
                }
                Api::Add => {
                    let req = AddRaftVoterRequest {
                        cluster_id: Some("cluster".into()),
                        timeout_ms: 1_000,
                        voter_id: 2,
                        voter_directory_id: ProtoUuid([2; 16]),
                        listeners: vec![add_raft_voter_request::Listener {
                            name: "CONTROLLER".into(),
                            host: "127.0.0.1".into(),
                            port: 9093,
                            ..Default::default()
                        }],
                        ack_when_committed: true,
                        ..Default::default()
                    };
                    let req_bytes = crate::test_support::encode_request(&req, version);
                    let resp = crate::handlers::add_raft_voter::handle(
                        &broker, version, 123, &req_bytes, &ctx,
                    )
                    .await
                    .expect("handle");
                    crate::test_support::decode_response::<AddRaftVoterResponse>(&resp, version)
                        .error_code
                }
                Api::Remove => {
                    let req = RemoveRaftVoterRequest {
                        cluster_id: Some("cluster".into()),
                        voter_id: 2,
                        voter_directory_id: ProtoUuid([3; 16]),
                        ..Default::default()
                    };
                    let req_bytes = crate::test_support::encode_request(&req, version);
                    let resp = crate::handlers::remove_raft_voter::handle(
                        &broker, version, 123, &req_bytes, &ctx,
                    )
                    .await
                    .expect("handle");
                    crate::test_support::decode_response::<RemoveRaftVoterResponse>(&resp, version)
                        .error_code
                }
            };

            check!(
                (error_code == codes::CLUSTER_AUTHORIZATION_FAILED)
                    == want_cluster_authorization_failed,
                "{api_name} with {grant:?} only"
            );
            broker_handle.shutdown().await;
        }
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
