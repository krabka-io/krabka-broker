//! The `ApiVersions` handshake that every controller-listener connection begins
//! with: the advertised API table, the supported and finalized feature ranges,
//! and the request checks of Kafka's `ControllerApis.handleApiVersionsRequest`.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::{self, ApiVersionsRequest},
        api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
    },
};

pub use self::client_software::is_valid_client_info;
use self::table::CONTROLLER_LISTENER_APIS;
use crate::error::RaftError;

mod client_software;
pub(super) mod table;

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `krabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
pub(super) const API_KEY_API_VERSIONS: i16 = 18;

/// Lowest `ApiVersions` request version this listener speaks.
const API_VERSIONS_MIN_VERSION: i16 = api_versions_request::MIN_VERSION;
/// Highest `ApiVersions` request version this listener speaks: the clamp
/// applied to the response body codec, and the same generated maximum the
/// `api_keys` table advertises for API 18 (current JVM controllers dial at v5;
/// Krabka's own client at v0).
const API_VERSIONS_MAX_VERSION: i16 = api_versions_request::MAX_VERSION;
/// First `ApiVersions` version that carries the KIP-511 client software name
/// and version.
const API_VERSIONS_CLIENT_SOFTWARE_MIN_VERSION: i16 = 3;
/// First `ApiVersions` version that carries the KIP-1242 cluster id and node
/// id.
const API_VERSIONS_ROUTING_MIN_VERSION: i16 = 5;
const API_VERSIONS_UNSUPPORTED_VERSION: i16 = 35;
const API_VERSIONS_INVALID_REQUEST: i16 = 42;
/// First `ApiVersions` response version where JVM clients accept a zero minimum
/// for `kraft.version`.
const KRAFT_ZERO_MIN_API_VERSION: i16 = 4;

/// Answers one controller-listener `ApiVersions` request with the response
/// body. The body always goes out behind a v0 response header.
///
/// The checks follow Kafka's `ControllerApis.handleApiVersionsRequest` and
/// `SaslServerAuthenticator.handleApiVersionsRequest`, which answer the same
/// way:
///
/// 1. A version outside `MIN..=MAX` is not decoded. Kafka's `RequestContext`
///    parses it as an empty v0 request, and `ApiVersionsRequest.getErrorResponse`
///    answers `UNSUPPORTED_VERSION` in a v0 body with one `api_keys` entry, the
///    `ApiVersions` range, so the client retries at a version the listener
///    serves.
/// 2. `ApiVersionsRequest.isValid` fails, and the answer is `INVALID_REQUEST`
///    with an empty table. From v5 the cluster id and the node id must be
///    both set or both unset. From v3 the client software name and version
///    must match the KIP-511 pattern.
/// 3. Otherwise the answer is the full controller-listener table.
///
/// Kafka's controller does not run the KIP-1242 `REBOOTSTRAP_REQUIRED` check.
/// Only `KafkaApis` on the broker listener runs it.
///
/// # Errors
/// Returns the decode error for a supported version whose body does not
/// decode. Kafka's `RequestContext.parseRequest` also fails such a request,
/// and the connection closes.
pub(crate) fn api_versions_response(
    req_version: i16,
    body: &[u8],
    image: &krabka_metadata::MetadataImage,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
) -> Result<Bytes, RaftError> {
    if !(API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION).contains(&req_version) {
        return Ok(encode_body(
            &ApiVersionsResponse {
                error_code: API_VERSIONS_UNSUPPORTED_VERSION,
                api_keys: vec![ApiVersionEntry {
                    api_key: API_KEY_API_VERSIONS,
                    min_version: API_VERSIONS_MIN_VERSION,
                    max_version: API_VERSIONS_MAX_VERSION,
                    ..Default::default()
                }],
                ..Default::default()
            },
            0,
        ));
    }
    let request = ApiVersionsRequest::decode(&mut &body[..], req_version)?;
    if !is_valid_request(&request, req_version) {
        return Ok(encode_body(
            &ApiVersionsResponse {
                error_code: API_VERSIONS_INVALID_REQUEST,
                ..Default::default()
            },
            req_version,
        ));
    }
    Ok(api_versions_response_body(req_version, image, admin_router))
}

/// Kafka's `ApiVersionsRequest.isValid`.
fn is_valid_request(request: &ApiVersionsRequest, version: i16) -> bool {
    if version >= API_VERSIONS_ROUTING_MIN_VERSION
        && (request.cluster_id.is_none() != (request.node_id == -1))
    {
        return false;
    }
    version < API_VERSIONS_CLIENT_SOFTWARE_MIN_VERSION
        || (is_valid_client_info(&request.client_software_name)
            && is_valid_client_info(&request.client_software_version))
}

fn encode_body(response: &ApiVersionsResponse, version: i16) -> Bytes {
    let mut body = BytesMut::with_capacity(response.encoded_len(version));
    // The version is in the generated range, and the fields are defaults or
    // table entries, so the encoder has nothing to refuse.
    let _ = response.encode(&mut body, version);
    body.freeze()
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
) -> Bytes {
    use krabka_protocol::owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey};
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
    encode_body(&resp, req_version.clamp(0, API_VERSIONS_MAX_VERSION))
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
            let body = super::api_versions_response_body(req_v, &image, None);
            let v = req_v.clamp(0, 4);
            let mut cur = &body[..];
            let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
            assert2::assert!(cur.is_empty());
            assert2::assert!(resp.error_code == 0);
            let keys: std::collections::BTreeSet<i16> =
                resp.api_keys.iter().map(|k| k.api_key).collect();
            for want in [1i16, 18, 52, 53, 54, 59, 70] {
                assert2::assert!(keys.contains(&want));
            }
            // `BrokerRegistration` and `BrokerHeartbeat` reach the controller
            // listener through the Admin router, so the native table must not
            // advertise them on its own.
            // `an_admin_router_contributes_its_own_api_versions` covers the
            // other half: with a router bound, a key is advertised.
            assert2::assert!(!keys.contains(&62i16));
            assert2::assert!(!keys.contains(&63i16));
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

    /// The controller listener advertises what a bound Admin router serves, on
    /// top of the APIs it answers itself. `BrokerHeartbeat` is the case that
    /// matters: a broker reads this table to learn where to send it.
    #[test]
    fn an_admin_router_contributes_its_own_api_versions() {
        use krabka_protocol::owned::api_versions_response::ApiVersionsResponse;

        struct HeartbeatRouter;
        impl crate::ControllerAdminRouter for HeartbeatRouter {
            fn api_versions(&self) -> &[crate::ControllerApiVersion] {
                use krabka_protocol::owned::broker_heartbeat_request;
                &[crate::ControllerApiVersion {
                    api_key: broker_heartbeat_request::API_KEY,
                    min_version: broker_heartbeat_request::MIN_VERSION,
                    max_version: broker_heartbeat_request::MAX_VERSION,
                    flexible_min: broker_heartbeat_request::FLEXIBLE_MIN,
                }]
            }

            fn route(
                &self,
                _request: crate::ControllerAdminRequest,
            ) -> crate::ControllerAdminRouteFuture<'_> {
                Box::pin(async { Ok(None) })
            }
        }

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let body = super::api_versions_response_body(4, &image, Some(&HeartbeatRouter));
        let resp = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode body");

        let heartbeat = resp
            .api_keys
            .iter()
            .find(|key| key.api_key == 63)
            .expect("BrokerHeartbeat advertised through the Admin router");
        assert2::assert!(
            (heartbeat.min_version, heartbeat.max_version)
                == (
                    krabka_protocol::owned::broker_heartbeat_request::MIN_VERSION,
                    krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION,
                )
        );
    }

    /// One row per `ApiVersions` request shape: Kafka's unsupported-version
    /// answer, the `isValid` refusals, and the full table. A request whose
    /// version is not served carries a body that does not decode, because
    /// Kafka does not read it.
    #[test]
    fn api_versions_response_runs_kafka_request_checks() {
        use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

        struct Row {
            label: &'static str,
            version: i16,
            request: Option<ApiVersionsRequest>,
            expected: Option<(i16, ApiVersionsResponse)>,
        }
        let request = |name: &str, software_version: &str, cluster_id: Option<&str>, node_id| {
            Some(ApiVersionsRequest {
                client_software_name: name.into(),
                client_software_version: software_version.into(),
                cluster_id: cluster_id.map(str::to_string),
                node_id,
                ..Default::default()
            })
        };
        let unsupported = Some((
            0,
            ApiVersionsResponse {
                error_code: API_VERSIONS_UNSUPPORTED_VERSION,
                api_keys: vec![ApiVersionEntry {
                    api_key: 18,
                    min_version: 0,
                    max_version: 5,
                    ..Default::default()
                }],
                ..Default::default()
            },
        ));
        let invalid = |version| {
            Some((
                version,
                ApiVersionsResponse {
                    error_code: API_VERSIONS_INVALID_REQUEST,
                    ..Default::default()
                },
            ))
        };
        let rows = [
            Row {
                label: "v6",
                version: 6,
                request: None,
                expected: unsupported.clone(),
            },
            Row {
                label: "i16::MAX",
                version: i16::MAX,
                request: None,
                expected: unsupported.clone(),
            },
            Row {
                label: "negative",
                version: -1,
                request: None,
                expected: unsupported,
            },
            Row {
                label: "v3 empty name",
                version: 3,
                request: request("", "1.0", None, -1),
                expected: invalid(3),
            },
            Row {
                label: "v4 name with a space",
                version: 4,
                request: request("a b", "1.0", None, -1),
                expected: invalid(4),
            },
            Row {
                label: "v3 empty software version",
                version: 3,
                request: request("krabka", "", None, -1),
                expected: invalid(3),
            },
            Row {
                label: "v5 cluster id without node id",
                version: 5,
                request: request("krabka", "1.0", Some("cluster"), -1),
                expected: invalid(5),
            },
            Row {
                label: "v5 node id without cluster id",
                version: 5,
                request: request("krabka", "1.0", None, 7),
                expected: invalid(5),
            },
            Row {
                label: "v5 another cluster and node, no KIP-1242 check",
                version: 5,
                request: request("krabka", "1.0", Some("other"), 8),
                expected: None,
            },
            Row {
                label: "v5 valid",
                version: 5,
                request: request("krabka", "1.0", None, -1),
                expected: None,
            },
            Row {
                label: "v0",
                version: 0,
                request: Some(ApiVersionsRequest::default()),
                expected: None,
            },
        ];

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        for row in rows {
            let body = row.request.map_or_else(
                || bytes::Bytes::from_static(&[0xff]),
                |request| {
                    let mut body = BytesMut::new();
                    request.encode(&mut body, row.version).expect("encode");
                    body.freeze()
                },
            );
            let answer =
                super::api_versions_response(row.version, &body, &image, None).expect("answer");
            let (version, expected) = row.expected.unwrap_or_else(|| {
                let full = super::api_versions_response_body(row.version, &image, None);
                (
                    row.version,
                    ApiVersionsResponse::decode(&mut &full[..], row.version).expect("full"),
                )
            });
            let mut cur = &answer[..];
            let decoded = ApiVersionsResponse::decode(&mut cur, version).expect("decode");
            assert2::check!(cur.is_empty(), "{}", row.label);
            assert2::check!(decoded == expected, "{}", row.label);
            if expected.error_code == 0 {
                assert2::check!(!decoded.api_keys.is_empty(), "{}", row.label);
            }
        }
    }

    /// A served version whose body does not decode fails the request, as
    /// Kafka's `RequestContext.parseRequest` does.
    #[test]
    fn api_versions_response_refuses_a_malformed_body() {
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        assert2::assert!(super::api_versions_response(3, &[0xff], &image, None).is_err());
    }
}
