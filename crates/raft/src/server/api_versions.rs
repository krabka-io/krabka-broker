//! The `ApiVersions` handshake that every controller-listener connection begins
//! with: the advertised API table, the supported and finalized feature ranges,
//! and the KIP-1242 routing-identity check that tells a client it dialled the
//! wrong node.

use bytes::{Bytes, BytesMut};

use self::table::CONTROLLER_LISTENER_APIS;
use crate::error::RaftError;

pub(super) mod table;

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `krabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
pub(super) const API_KEY_API_VERSIONS: i16 = 18;

/// Highest `ApiVersions` request version this listener speaks: the clamp
/// applied to the response body codec, and the same generated maximum the
/// `api_keys` table advertises for API 18 (current JVM controllers dial at v5;
/// Krabka's own client at v0).
const API_VERSIONS_MAX_VERSION: i16 = krabka_protocol::owned::api_versions_request::MAX_VERSION;
/// First `ApiVersions` version that carries KIP-1242 routing identity.
const API_VERSIONS_ROUTING_MIN_VERSION: i16 = 5;
const API_VERSIONS_INVALID_REQUEST: i16 = 42;
const API_VERSIONS_REBOOTSTRAP_REQUIRED: i16 = 129;
/// First `ApiVersions` response version where JVM clients accept a zero minimum
/// for `kraft.version`.
const KRAFT_ZERO_MIN_API_VERSION: i16 = 4;

/// Validate the KIP-1242 routing identity carried by `ApiVersions` v5.
pub(super) fn api_versions_routing_error(
    req_version: i16,
    body: &[u8],
    expected_cluster_id: &str,
    expected_node_id: u64,
) -> Result<i16, RaftError> {
    use krabka_protocol::{Decode, owned::api_versions_request::ApiVersionsRequest};

    if req_version < API_VERSIONS_ROUTING_MIN_VERSION {
        return Ok(0);
    }

    let mut cur = body;
    let request = ApiVersionsRequest::decode(&mut cur, req_version)?;
    let expected_node_id = i32::try_from(expected_node_id).map_err(|_| {
        RaftError::Protocol(krabka_protocol::ProtocolError::InvalidValue(
            "controller node id exceeds the Kafka wire range",
        ))
    })?;
    Ok(match (&request.cluster_id, request.node_id) {
        (None, -1) => 0,
        (Some(_), -1) | (None, _) => API_VERSIONS_INVALID_REQUEST,
        (Some(cluster_id), node_id)
            if cluster_id != expected_cluster_id || node_id != expected_node_id =>
        {
            API_VERSIONS_REBOOTSTRAP_REQUIRED
        }
        (Some(_), _) => 0,
    })
}

/// `ApiVersionsResponse` advertising the controller-listener APIs.
///
/// A real `mirror.gcr.io/apache/kafka:4.0.0` controller dials peers with `ApiVersions v4` over a
/// flexible (v2) request header, then consults the returned table to decide
/// which version of `Vote`/`Fetch`/etc. to send. An EMPTY `api_keys` list made
/// the JVM treat every raft RPC as `UNSUPPORTED_VERSION` and refuse to send
/// `Vote` on the wire. Advertising the KIP-595 APIs at the versions Krabka's
/// engine speaks lets compatible peers proceed to real `Vote`/`Fetch`. Those
/// versions come from [`table::CONTROLLER_LISTENER_APIS`], which derives them
/// from the generated message constants; the KIP-919 Admin surface the broker
/// attaches contributes the rest.
///
/// Body is the flexible (v3+) `ApiVersionsResponse` shape: `error_code(i16)`,
/// `api_keys` compact-array of `{api_key(i16), min(i16), max(i16), tagged(0)}`,
/// `throttle_time_ms(i32)`, response-level `tagged(0)`. Per the documented Kafka
/// asymmetry, the *response header* stays v0 (no leading tagged-fields byte) —
/// so this is written via [`super::framing::write_response_no_tagged_fields`].
pub(super) fn api_versions_response_body(
    req_version: i16,
    image: &krabka_metadata::MetadataImage,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
    error_code: i16,
) -> Bytes {
    use krabka_protocol::{
        Encode,
        owned::api_versions_response::{
            ApiVersion as ApiVersionEntry, ApiVersionsResponse, FinalizedFeatureKey,
            SupportedFeatureKey,
        },
    };
    if error_code != 0 {
        let response = ApiVersionsResponse {
            error_code,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        let _ = response.encode(&mut body, req_version.clamp(0, API_VERSIONS_MAX_VERSION));
        return body.freeze();
    }
    let entry = |version: &crate::ControllerApiVersion| ApiVersionEntry {
        api_key: version.api_key,
        min_version: version.min_version,
        max_version: version.max_version,
        ..Default::default()
    };
    let mut api_keys: Vec<ApiVersionEntry> = CONTROLLER_LISTENER_APIS.iter().map(entry).collect();
    if let Some(router) = admin_router {
        api_keys.extend(router.api_versions().iter().map(entry));
    }
    api_keys.sort_unstable_by_key(|version| version.api_key);

    // `Admin::describeFeatures` is carried by ApiVersions. Keep the
    // controller-listener view on the same metadata registry and live
    // finalized image as the broker listener, including kraft.version's
    // v4-only zero minimum compatibility rule.
    let supported_features = krabka_metadata::feature_registry()
        .iter()
        .map(|feature| {
            let (minimum, maximum) = feature.supported_range();
            SupportedFeatureKey {
                name: feature.name().into(),
                min_version: if feature.name()
                    == krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                    && req_version >= KRAFT_ZERO_MIN_API_VERSION
                {
                    minimum
                } else {
                    minimum.max(1)
                },
                max_version: maximum,
                ..Default::default()
            }
        })
        .collect();
    let mut finalized_features: Vec<_> = image
        .finalized_features()
        .iter()
        .map(|(name, level)| FinalizedFeatureKey {
            name: name.clone(),
            min_version_level: *level,
            max_version_level: *level,
            ..Default::default()
        })
        .collect();
    let kraft_version = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
    finalized_features.push(FinalizedFeatureKey {
        name: krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE.into(),
        min_version_level: kraft_version,
        max_version_level: kraft_version,
        ..Default::default()
    });

    let resp = ApiVersionsResponse {
        api_keys,
        supported_features,
        finalized_features_epoch: image.finalized_features_epoch(),
        finalized_features,
        ..Default::default()
    };
    // JVM dials at v4 (flexible); Krabka's own client at v0 (non-flexible). The
    // codec emits the correct body shape per version: req v<=2 → non-flexible
    // v0-shaped body, req v>=3 → flexible (compact) body. The v0 ApiVersions
    // response HEADER asymmetry lives in the framing (`write_response_no_tagged_fields`),
    // not here.
    let body_version = req_version.clamp(0, API_VERSIONS_MAX_VERSION);
    let mut buf = BytesMut::new();
    let _ = resp.encode(&mut buf, body_version);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use krabka_protocol::Decode;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn api_versions_body_advertises_kip595_set_both_shapes() {
        use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};
        let mut image = krabka_metadata::MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: 24,
        }));
        for req_v in [0i16, 4i16] {
            let body = super::api_versions_response_body(req_v, &image, None, 0);
            let v = req_v.clamp(0, 4);
            let mut cur = &body[..];
            let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
            assert2::assert!(cur.is_empty());
            assert2::assert!(resp.error_code == 0);
            let keys: std::collections::BTreeSet<i16> =
                resp.api_keys.iter().map(|k| k.api_key).collect();
            for want in [1i16, 18, 52, 53, 54, 59, 62, 63, 70] {
                assert2::assert!(keys.contains(&want));
            }
            // Vote is pinned to the one version the engine's codec speaks, so
            // the advertised range is that version on both ends.
            let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
            assert2::assert!(vote.min_version == 2 && vote.max_version == 2);
            if req_v >= 3 {
                let kraft = resp
                    .supported_features
                    .iter()
                    .find(|feature| feature.name == "kraft.version")
                    .expect("kraft.version support");
                assert2::assert!((kraft.min_version, kraft.max_version) == (0, 1));
                let metadata = resp
                    .supported_features
                    .iter()
                    .find(|feature| feature.name == "metadata.version")
                    .expect("metadata.version support");
                assert2::assert!((metadata.min_version, metadata.max_version) == (7, 25));
                let finalized_metadata = resp
                    .finalized_features
                    .iter()
                    .find(|feature| feature.name == "metadata.version")
                    .expect("metadata.version finalized");
                assert2::assert!(
                    (
                        finalized_metadata.min_version_level,
                        finalized_metadata.max_version_level
                    ) == (24, 24)
                );
                assert2::assert!(resp.finalized_features_epoch == image.finalized_features_epoch());
            }
        }
    }

    #[test]
    fn api_versions_v5_validates_controller_routing_identity() {
        use krabka_protocol::{
            Encode,
            owned::{
                api_versions_request::ApiVersionsRequest,
                api_versions_response::ApiVersionsResponse,
            },
        };

        let request = |cluster_id: Option<&str>, node_id| {
            let request = ApiVersionsRequest {
                client_software_name: "krabka-test".into(),
                client_software_version: "1.0.0".into(),
                cluster_id: cluster_id.map(str::to_string),
                node_id,
                ..Default::default()
            };
            let mut body = bytes::BytesMut::new();
            request.encode(&mut body, 5).expect("encode ApiVersions v5");
            body.freeze()
        };

        for (cluster_id, node_id, expected) in [
            (None, -1, 0),
            (Some("cluster"), -1, API_VERSIONS_INVALID_REQUEST),
            (None, 7, API_VERSIONS_INVALID_REQUEST),
            (Some("cluster"), 7, 0),
            (Some("wrong-cluster"), 7, API_VERSIONS_REBOOTSTRAP_REQUIRED),
            (Some("cluster"), 8, API_VERSIONS_REBOOTSTRAP_REQUIRED),
        ] {
            let error =
                super::api_versions_routing_error(5, &request(cluster_id, node_id), "cluster", 7)
                    .expect("decode ApiVersions v5");
            assert2::assert!(error == expected);
        }

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let body =
            super::api_versions_response_body(5, &image, None, API_VERSIONS_REBOOTSTRAP_REQUIRED);
        let response = ApiVersionsResponse::decode(&mut body.as_ref(), 5).unwrap();
        assert2::assert!(response.error_code == API_VERSIONS_REBOOTSTRAP_REQUIRED);
        assert2::assert!(response.api_keys.is_empty());
    }
}
