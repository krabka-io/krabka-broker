//! `AddRaftVoter` (`api_key=80`, KIP-853). Admin RPC that promotes a
//! caught-up observer into the controller-raft voter set.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Request checks and error codes
//!
//! After the ACL gate, the request runs the controller listener's own checks
//! in `krabka_raft::voter_requests`, in the order of Kafka's
//! `KafkaRaftClient.handleAddVoterRequest` and `AddVoterHandler`: a foreign
//! cluster id is `INCONSISTENT_CLUSTER_ID (104)`, a node that is not the leader
//! answers `NOT_LEADER_OR_FOLLOWER (6)`, an invalid voter key or listener set
//! is `INVALID_REQUEST (42)`, a candidate whose `kraft.version` range does not
//! cover the finalized version is `INVALID_REQUEST (42)`, and a candidate that
//! is not caught up is `REQUEST_TIMED_OUT (7)`.

use bytes::Bytes;
use krabka_metadata::{Voter, VoterEndpoint};
use krabka_protocol::{
    Decode,
    owned::{
        add_raft_voter_request::AddRaftVoterRequest, add_raft_voter_response::AddRaftVoterResponse,
        api_versions_request::ApiVersionsRequest,
    },
};
use krabka_raft::{reconfig::AddVoter, voter_requests};

use crate::{broker::Broker, codes, error::BrokerError, handlers::cluster_alter_denied};

#[tracing::instrument(
    name = "handle_add_raft_voter",
    level = "info",
    skip_all,
    fields(api = "AddRaftVoter", version, req_bytes = req_bytes.len()),
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
    let req = AddRaftVoterRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Cluster:Alter gate — KIP-853 reconfiguration is a cluster-wide
    // mutation, same gate as UnregisterBroker.
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_resp(
            version,
            &AddRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("add-raft-voter denied".into()),
                ..Default::default()
            },
        );
    }

    // Broker-only observer forward to the active controller quorum (#392)
    if let Some(forwarded) = broker
        .controller
        .forward_raw(80, version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        return forwarded.map_err(BrokerError::from);
    }

    // The request checks, their order and their codes are the controller
    // listener's own (`krabka_raft::voter_requests`).
    let Some(quorum) = broker.controller.quorum_snapshot() else {
        return encode_resp(
            version,
            &AddRaftVoterResponse {
                error_code: voter_requests::NOT_LEADER_OR_FOLLOWER,
                ..Default::default()
            },
        );
    };
    let refusal =
        match voter_requests::add_voter_refusal(&req, &image.cluster_id().to_string(), &quorum) {
            Some(refusal) => Some(refusal),
            None if image.kraft_version() >= 1 => {
                probe_candidate(broker, &req, image.kraft_version())
                    .await
                    .err()
            }
            None => None,
        };
    if let Some((error_code, error_message)) = refusal {
        return encode_resp(
            version,
            &AddRaftVoterResponse {
                error_code,
                error_message,
                ..Default::default()
            },
        );
    }

    let (voter_id, directory_id) = (req.voter_id, req.voter_directory_id);
    let id = u64::try_from(voter_id).unwrap_or_default();
    let voter = Voter {
        id: krabka_raft::NodeId(id),
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
        kraft_version: krabka_metadata::KRaftVersionRange::default(),
    };

    let (error_code, error_message) = voter_requests::reconfiguration_refusal(
        broker
            .controller
            .add_voter(AddVoter {
                voter,
                ack_when_committed: version == 0 || req.ack_when_committed,
            })
            .await,
        voter_id,
        directory_id,
    );

    if error_code == codes::NONE {
        crate::handlers::audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "AddRaftVoter",
            vec![crate::handlers::audit_resource("RaftVoter", id.to_string())],
        );
    }

    encode_resp(
        version,
        &AddRaftVoterResponse {
            error_code,
            error_message,
            ..Default::default()
        },
    )
}

/// Asks the candidate for its `ApiVersions` over the controller listener, as
/// `AddVoterHandler` does, and refuses it when it cannot answer or does not
/// support the finalized `kraft.version`.
async fn probe_candidate(
    broker: &Broker,
    req: &AddRaftVoterRequest,
    finalized_version: u16,
) -> Result<(), voter_requests::Refusal> {
    let unavailable = |error: String| {
        voter_requests::candidate_unavailable_refusal(req.voter_id, req.voter_directory_id, &error)
    };
    let endpoint = req
        .listeners
        .iter()
        .find(|listener| listener.name.eq_ignore_ascii_case("CONTROLLER"))
        .or_else(|| req.listeners.first())
        .expect("validated non-empty listeners");
    let server_name = broker
        .config
        .controller_server_name
        .as_deref()
        .unwrap_or(&endpoint.host);
    let connection = broker
        .inter_broker_client
        .connect_as_connection(
            &endpoint.host,
            endpoint.port,
            broker.config.controller_listener_protocol,
            server_name,
            krabka_client_core::ConnectionOptions {
                client_id: "krabka-voter-probe".into(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let request = ApiVersionsRequest {
        client_software_name: "krabka".into(),
        client_software_version: env!("CARGO_PKG_VERSION").into(),
        ..Default::default()
    };
    let response = connection
        .send(request)
        .await
        .map_err(|error| unavailable(error.to_string()));
    connection.close();
    let response = response?;
    let supported = response
        .supported_features
        .iter()
        .find(|feature| feature.name == "kraft.version")
        .is_some_and(|feature| {
            i16::try_from(finalized_version).is_ok_and(|version| {
                feature.min_version <= version && version <= feature.max_version
            })
        });
    if !supported {
        return Err(voter_requests::candidate_kraft_version_refusal(
            req.voter_id,
            req.voter_directory_id,
            finalized_version,
        ));
    }
    Ok(())
}

fn encode_resp(version: i16, resp: &AddRaftVoterResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::{
        owned::add_raft_voter_request::Listener, primitives::uuid::Uuid as ProtoUuid,
    };
    use krabka_security::{AuthMethod, Principal};

    use crate::test_support::DenyAll;

    fn request(voter_id: i32) -> AddRaftVoterRequest {
        AddRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            timeout_ms: 1_000,
            voter_id,
            voter_directory_id: ProtoUuid([2; 16]),
            listeners: vec![Listener {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
                ..Default::default()
            }],
            ack_when_committed: true,
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        AddRaftVoterRequest,
        AddRaftVoterResponse,
        client_id = "admin-client"
    );

    use super::*;
    use crate::test_support::start_broker_with_authorizer as start_broker;

    /// Decode→encode round-trip at min and max versions. Guards against
    /// the response failing to encode at either end of the version range
    /// the schema declares.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use krabka_protocol::owned::add_raft_voter_response::{self, AddRaftVoterResponse};
        for version in [
            add_raft_voter_response::MIN_VERSION,
            add_raft_voter_response::MAX_VERSION,
        ] {
            let resp = AddRaftVoterResponse {
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
                error_message: Some("not the raft leader".into()),
                ..Default::default()
            };
            let bytes = encode_resp(version, &resp).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = AddRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(decoded.error_code == codes::NOT_LEADER_OR_FOLLOWER);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = 1;
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
        assert!(resp.error_message.as_deref() == Some("add-raft-voter denied"));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = 1;
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
        let req_bytes = encode_request(&request, version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(
            resp == AddRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Add voter request didn't include a valid voter".into()),
                ..Default::default()
            }
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_reconfig_error_from_controller() {
        let version = 1;
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
        let req_bytes = encode_request(&request, version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(
            resp == AddRaftVoterResponse {
                error_code: codes::UNSUPPORTED_VERSION,
                error_message: Some(
                    "Cluster doesn't support changing voters because the kraft.version feature \
                     is 0"
                        .into()
                ),
                ..Default::default()
            }
        );
        broker_handle.shutdown().await;
    }
}
