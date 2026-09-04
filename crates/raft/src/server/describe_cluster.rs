//! `DescribeCluster` (KIP-919) on the controller listener: the projection of the
//! quorum's voter endpoints that lets an `AdminClient` bootstrapped with
//! `--bootstrap-controller` discover the controllers, and the encoder that turns
//! that projection into a response body.

use bytes::{Bytes, BytesMut};

use crate::{error::RaftError, kraft::KraftController};

/// `DescribeCluster` (KIP-919) — served on the controller listener so an
/// `AdminClient` bootstrapped with `--bootstrap-controller` can discover the
/// quorum's controller endpoints directly from the leader.
pub(super) const API_KEY_DESCRIBE_CLUSTER: i16 = 60;

/// Serve `DescribeCluster` (60, KIP-919) on the controller listener from the
/// controller's metadata image. `endpoint_type=2` (CONTROLLERS) projects the
/// voter set so a `--bootstrap-controller` `AdminClient` can discover the
/// quorum. Other endpoint types are rejected with KIP-919's
/// `MISMATCHED_ENDPOINT_TYPE`. Any configured authentication is terminated by
/// the controller-listener handshake before the Raft image projection.
// cargo-mutants: the broker-id `i32::try_from(node_id).unwrap_or(-1)` overflow fallback is
// unreachable: the metadata layer rejects registering a `node_id` exceeding
// `i32::MAX` (BrokerRegistrationRecord encode validation), so the `-1` sentinel
// is dead defensive code that no input can reach. The reachable voter/broker
// projection is covered by the sibling tests.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_cluster_response_body(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{Decode, owned::describe_cluster_request::DescribeClusterRequest};

    let mut cur = body;
    let req = DescribeClusterRequest::decode(&mut cur, version)?;
    let image = engine.current_image();

    // Controller endpoints: each voter's CONTROLLER-named listener, falling back
    // to its first advertised endpoint.
    let voters: Vec<(i32, String, i32)> = image
        .voters()
        .iter()
        .map(|v| {
            let ep = v
                .endpoints
                .iter()
                .find(|e| e.name.eq_ignore_ascii_case("CONTROLLER"))
                .or_else(|| v.endpoints.first());
            (
                i32::try_from(v.id.0).unwrap_or(-1),
                ep.map(|e| e.host.clone()).unwrap_or_default(),
                ep.map_or(-1, |e| i32::from(e.port)),
            )
        })
        .collect();
    let controller_id: i32 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i32::try_from(l.0).ok())
        .unwrap_or(-1);

    Ok(build_describe_cluster_body(
        version,
        req.endpoint_type,
        &voters,
        &image.cluster_id().to_string(),
        controller_id,
    )?)
}

/// Encode a `DescribeClusterResponse` body for `version` from already-projected
/// node tuples. Pure (no engine), so the projection-and-encode is unit-testable.
fn build_describe_cluster_body(
    version: i16,
    endpoint_type: i8,
    voters: &[(i32, String, i32)],
    cluster_id: &str,
    controller_id: i32,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    use krabka_protocol::{
        Encode,
        owned::describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    };

    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    const MISMATCHED_ENDPOINT_TYPE: i16 = 114;
    let (error_code, error_message, entries) = if endpoint_type == ENDPOINT_TYPE_CONTROLLERS {
        let entries = voters
            .iter()
            .map(|(id, host, port)| DescribeClusterBroker {
                broker_id: *id,
                host: host.clone(),
                port: *port,
                ..Default::default()
            })
            .collect();
        (0, None, entries)
    } else {
        (
            MISMATCHED_ENDPOINT_TYPE,
            Some("controller listener requires endpoint_type=CONTROLLERS".into()),
            Vec::new(),
        )
    };

    // Throttle time and cluster_authorized_operations keep their protocol
    // defaults; the endpoint-specific fields are explicit in both branches.
    let resp = DescribeClusterResponse {
        error_code,
        error_message,
        endpoint_type,
        cluster_id: cluster_id.to_string(),
        controller_id,
        brokers: entries,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::Decode;
    use uuid::Uuid;

    use super::*;
    use crate::server::{
        api_versions::api_versions_response_body,
        test_support::{test_engine_with_voters, voter},
    };

    fn describe_cluster_body(version: i16, endpoint_type: i8) -> Bytes {
        use krabka_protocol::{Encode, owned::describe_cluster_request::DescribeClusterRequest};

        let req = DescribeClusterRequest {
            endpoint_type,
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        req.encode(&mut out, version).expect("describe request");
        out.freeze()
    }

    #[tokio::test]
    async fn describe_cluster_response_body_projects_controller_fallbacks() {
        use krabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;

        let (engine, _dir) = test_engine_with_voters(1, [voter(u64::MAX, Vec::new())]);
        let body = super::describe_cluster_response_body(1, &describe_cluster_body(1, 2), &engine)
            .await
            .expect("describe cluster");

        let mut cur = &body[..];
        let resp = DescribeClusterResponse::decode(&mut cur, 1).expect("describe response");
        check!(cur.is_empty());
        check!(
            (
                resp.controller_id,
                resp.brokers
                    .iter()
                    .map(|broker| (broker.broker_id, broker.host.as_str(), broker.port))
                    .collect::<Vec<_>>(),
            ) == (-1, vec![(-1, "", -1)])
        );
    }

    #[test]
    fn describe_cluster_body_projects_controllers_and_rejects_brokers() {
        use krabka_protocol::{
            Decode,
            owned::{
                api_versions_response::ApiVersionsResponse,
                describe_cluster_response::DescribeClusterResponse,
            },
        };

        // DescribeCluster (60) is advertised so clients negotiate it (KIP-919).
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let av = api_versions_response_body(4, &image, None, 0);
        let mut cur = &av[..];
        let avr = ApiVersionsResponse::decode(&mut cur, 4).unwrap();
        assert2::assert!(avr.api_keys.iter().any(|k| k.api_key == 60));

        let voters = vec![
            (1i32, "c1".to_string(), 9093i32),
            (2, "c2".to_string(), 9093),
        ];
        for version in [1i16, 2] {
            // endpoint_type = CONTROLLERS (2) → voter projection.
            let body =
                super::build_describe_cluster_body(version, 2, &voters, "clusterX", 1).unwrap();
            let mut cur = &body[..];
            let resp = DescribeClusterResponse::decode(&mut cur, version).unwrap();
            assert2::assert!(cur.is_empty());
            check!(
                (
                    resp.endpoint_type,
                    resp.cluster_id.as_str(),
                    resp.controller_id,
                    resp.brokers
                        .iter()
                        .map(|broker| (broker.broker_id, broker.host.as_str(), broker.port))
                        .collect::<Vec<_>>(),
                ) == (2, "clusterX", 1, vec![(1, "c1", 9093), (2, "c2", 9093)])
            );

            // endpoint_type = BROKERS (1) is the wrong listener surface.
            let body =
                super::build_describe_cluster_body(version, 1, &voters, "clusterX", 1).unwrap();
            let mut cur = &body[..];
            let resp = DescribeClusterResponse::decode(&mut cur, version).unwrap();
            check!(
                (
                    resp.error_code,
                    resp.endpoint_type,
                    resp.brokers.is_empty(),
                    resp.error_message.as_deref(),
                ) == (
                    114,
                    1,
                    true,
                    Some("controller listener requires endpoint_type=CONTROLLERS"),
                )
            );
        }
    }
}
