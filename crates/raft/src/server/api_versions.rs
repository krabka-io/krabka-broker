//! The `ApiVersions` handshake that every controller-listener connection begins
//! with: the advertised API table, the supported and finalized feature ranges,
//! and the request checks of Kafka's `ControllerApis.handleApiVersionsRequest`.

pub use self::{
    client_software::is_valid_client_info,
    features::{finalized_feature_keys, supported_feature_key, supported_feature_keys},
};
use {
    self::table::CONTROLLER_LISTENER_APIS,
    crate::{
        ControllerAdminRouter, ControllerApiVersion, UnstableApiVersions, UnstableFeatureVersions,
        error::RaftError, kafka_4_3_1_max,
    },
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::{self, ApiVersionsRequest},
            api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
        },
    },
};

mod client_software;
mod features;
pub(super) mod table;

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `krabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
pub(super) const API_KEY_API_VERSIONS: i16 = 18;

/// Lowest `ApiVersions` request version this listener speaks.
const API_VERSIONS_MIN_VERSION: i16 = api_versions_request::MIN_VERSION;
/// Highest `ApiVersions` request version this listener decodes: the generated
/// maximum. [`api_versions_max_version`] is what it serves.
const API_VERSIONS_MAX_VERSION: i16 = api_versions_request::MAX_VERSION;

/// Highest `ApiVersions` version a listener serves under `unstable`.
///
/// It is the same maximum its `api_keys` table advertises for API 18: Kafka 4.3.1's v4 by
/// default, and trunk's KIP-1242 v5 under `unstable.api.versions.enable`.
#[must_use]
pub fn api_versions_max_version(unstable: UnstableApiVersions) -> i16 {
    match unstable {
        UnstableApiVersions::Enabled => API_VERSIONS_MAX_VERSION,
        UnstableApiVersions::Disabled => {
            kafka_4_3_1_max(API_KEY_API_VERSIONS).unwrap_or(API_VERSIONS_MAX_VERSION)
        }
    }
}
/// First `ApiVersions` version that carries the KIP-511 client software name
/// and version.
const API_VERSIONS_CLIENT_SOFTWARE_MIN_VERSION: i16 = 3;
/// First `ApiVersions` version that carries the KIP-1242 cluster id and node
/// id.
const API_VERSIONS_ROUTING_MIN_VERSION: i16 = 5;
const API_VERSIONS_UNSUPPORTED_VERSION: i16 = 35;
const API_VERSIONS_INVALID_REQUEST: i16 = 42;

/// What one controller-listener `ApiVersions` answer is built from.
#[derive(Clone, Copy)]
pub struct ApiVersionsView<'a> {
    /// The applied metadata image: its finalized features.
    pub(crate) image: &'a krabka_metadata::MetadataImage,
    /// The finalized-features epoch: the last contained offset of the newest
    /// applied batch or snapshot that carried a `FeatureLevelRecord`, or `-1`
    /// before the first. Kafka's `FeaturesPublisher` takes
    /// `image.provenance().lastContainedOffset()` only from a delta with a
    /// features delta, so topic records do not move it.
    pub(crate) finalized_features_epoch: i64,
    /// The KIP-919 Admin surface the broker attaches, if any.
    pub(crate) admin_router: Option<&'a dyn ControllerAdminRouter>,
    /// Kafka's `unstable.api.versions.enable`. It caps both the API table and
    /// the supported features: the controller listener's
    /// `SimpleApiVersionManager` is built with it and takes no
    /// `unstable.feature.versions.enable`.
    pub(crate) unstable: UnstableApiVersions,
}

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
pub fn api_versions_response(
    req_version: i16,
    body: &[u8],
    view: ApiVersionsView<'_>,
) -> Result<Bytes, RaftError> {
    if !(API_VERSIONS_MIN_VERSION..=api_versions_max_version(view.unstable)).contains(&req_version)
    {
        return Ok(encode_body(&unsupported_version_response(view.unstable), 0));
    }
    let request = ApiVersionsRequest::decode(&mut &body[..], req_version)?;
    if !is_valid_api_versions_request(&request, req_version) {
        return Ok(encode_body(
            &ApiVersionsResponse {
                error_code: API_VERSIONS_INVALID_REQUEST,
                ..Default::default()
            },
            req_version,
        ));
    }
    Ok(api_versions_response_body(req_version, view))
}

/// Kafka's `ApiVersionsRequest.getErrorResponse` for `UNSUPPORTED_VERSION`
/// (KIP-511).
///
/// It is error 35 and exactly one `api_keys` entry, the range of
/// `ApiVersions` itself (`ApiVersionsResponse.toApiVersion(API_VERSIONS)`),
/// which the client reads to pick the version it retries with.
///
/// Every listener, broker or controller, answers with these bytes in a v0 body.
#[must_use]
pub fn unsupported_version_response(unstable: UnstableApiVersions) -> ApiVersionsResponse {
    ApiVersionsResponse {
        error_code: API_VERSIONS_UNSUPPORTED_VERSION,
        api_keys: vec![ApiVersionEntry {
            api_key: API_KEY_API_VERSIONS,
            min_version: API_VERSIONS_MIN_VERSION,
            max_version: api_versions_max_version(unstable),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Kafka's `ApiVersionsRequest.isValid`.
#[must_use]
pub fn is_valid_api_versions_request(request: &ApiVersionsRequest, version: i16) -> bool {
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

/// The controller listener's advertised table under `unstable`, sorted by API
/// key: its own APIs and the Admin router's, each capped at the version
/// `unstable` enables, as Kafka's `ApiKeys.toApiVersion` caps it. An api key
/// the enabled table does not have -- one Kafka 4.3.1 lacks, while unstable
/// api versions are disabled -- is left out.
pub(super) fn advertised_api_keys(
    admin_router: Option<&dyn ControllerAdminRouter>,
    unstable: UnstableApiVersions,
) -> Vec<ApiVersionEntry> {
    let entry = |version: &ControllerApiVersion| {
        version
            .enabled_max(unstable)
            .map(|max_version| ApiVersionEntry {
                api_key: version.api_key,
                min_version: version.min_version,
                max_version,
                ..Default::default()
            })
    };
    let mut api_keys: Vec<ApiVersionEntry> =
        CONTROLLER_LISTENER_APIS.iter().filter_map(entry).collect();
    if let Some(router) = admin_router {
        api_keys.extend(router.api_versions().iter().filter_map(entry));
    }
    api_keys.sort_unstable_by_key(|version| version.api_key);
    api_keys
}

/// Whether `version` of `api_key` is a version that `unstable` disables on
/// this listener: one above the enabled maximum, or any version of an api key
/// the enabled table does not have. Kafka's `Processor.parseRequestHeader`
/// closes the connection on either (`ApiKeys.isVersionEnabled`, and
/// `ApiKeys.forId` for a key the release does not know).
pub(super) fn is_disabled_version(
    api_key: i16,
    version: i16,
    admin_router: Option<&dyn ControllerAdminRouter>,
    unstable: UnstableApiVersions,
) -> bool {
    let router_apis = admin_router.map_or(&[][..], |router| router.api_versions());
    CONTROLLER_LISTENER_APIS
        .iter()
        .chain(router_apis)
        .find(|api| api.api_key == api_key)
        .is_some_and(|api| api.is_disabled_version(version, unstable))
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
/// The feature rows follow Kafka's `SimpleApiVersionManager`: the supported
/// set of `BrokerFeatures.defaultSupportedFeatures(enableUnstableLastVersion)`,
/// filtered for `alterFeatureLevel0` below v4, and the finalized levels above 0
/// with the image offset as their epoch. The controller listener builds that
/// manager with `unstable.api.versions.enable`, so that flag, and not
/// `unstable.feature.versions.enable`, caps the supported levels. The broker
/// listener caps them with the feature flag. [`features`] holds the rules the
/// two share.
///
/// Body is the flexible (v3+) `ApiVersionsResponse` shape: `error_code(i16)`,
/// `api_keys` compact-array of `{api_key(i16), min(i16), max(i16), tagged(0)}`,
/// `throttle_time_ms(i32)`, response-level `tagged(0)`. Per the documented Kafka
/// asymmetry, the *response header* stays v0 (no leading tagged-fields byte) —
/// so this is written via [`super::framing::write_response`].
pub(super) fn api_versions_response_body(req_version: i16, view: ApiVersionsView<'_>) -> Bytes {
    let resp = ApiVersionsResponse {
        api_keys: advertised_api_keys(view.admin_router, view.unstable),
        supported_features: supported_feature_keys(
            req_version,
            UnstableFeatureVersions::from(view.unstable == UnstableApiVersions::Enabled),
        ),
        finalized_features_epoch: view.finalized_features_epoch,
        finalized_features: finalized_feature_keys(view.image),
        ..Default::default()
    };
    // JVM dials at v4 (flexible); Krabka's own client at v0 (non-flexible). The
    // codec emits the correct body shape per version: req v<=2 → non-flexible
    // v0-shaped body, req v>=3 → flexible (compact) body. The v0 ApiVersions
    // response HEADER asymmetry lives in the framing (`write_response` with tagged fields disabled),
    // not here.
    encode_body(
        &resp,
        req_version.clamp(0, api_versions_max_version(view.unstable)),
    )
}

/// What a test Admin router that answers only `BrokerHeartbeat` advertises.
#[cfg(test)]
pub(super) const BROKER_HEARTBEAT_ONLY: &[ControllerApiVersion] = {
    use krabka_protocol::owned::broker_heartbeat_request;
    &[ControllerApiVersion {
        api_key: broker_heartbeat_request::API_KEY,
        min_version: broker_heartbeat_request::MIN_VERSION,
        max_version: broker_heartbeat_request::MAX_VERSION,
        released_max: Some(broker_heartbeat_request::MAX_VERSION),
        flexible_min: broker_heartbeat_request::FLEXIBLE_MIN,
    }]
};

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ControllerAdminRequest, ControllerAdminRouteFuture, ControllerAdminRouter,
            ControllerApiVersion, LATEST_PRODUCTION_METADATA_VERSION, test_support::feature_image,
        },
        krabka_metadata::{
            FeatureLevelRecord, MetadataRecord,
            metadata_version::{
                METADATA_VERSION_FEATURE, METADATA_VERSION_MAX, METADATA_VERSION_MIN,
            },
        },
        krabka_protocol::{
            Decode,
            owned::broker_heartbeat_request::{MAX_VERSION, MIN_VERSION},
        },
        std::collections::BTreeSet,
        uuid::Uuid,
    };

    /// The view a listener with no Admin router and the default unstable
    /// setting answers from, at finalized-features epoch
    /// `finalized_features_epoch`.
    fn view(
        image: &krabka_metadata::MetadataImage,
        finalized_features_epoch: i64,
    ) -> ApiVersionsView<'_> {
        ApiVersionsView {
            image,
            finalized_features_epoch,
            admin_router: None,
            unstable: UnstableApiVersions::Disabled,
        }
    }

    #[yare::parameterized(
        legacy_shape = { 0i16 },
        flexible_shape = { 4i16 },
    )]
    fn api_versions_body_advertises_kip595_set_both_shapes(req_v: i16) {
        use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};
        let mut image = krabka_metadata::MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: METADATA_VERSION_FEATURE.into(),
            level: 24,
        }));
        let body = super::api_versions_response_body(req_v, view(&image, 41));
        let v = req_v.clamp(0, 4);
        let mut cur = &body[..];
        let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
        assert2::assert!(cur.is_empty());
        assert2::assert!(resp.error_code == 0);
        let keys: BTreeSet<i16> = resp.api_keys.iter().map(|k| k.api_key).collect();
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
        // Vote is advertised over its whole schema range, as Kafka's
        // `ApiKeys` does.
        let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
        assert2::assert!(vote.min_version == 0 && vote.max_version == 2);
        if req_v >= 3 {
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
            assert2::assert!(resp.finalized_features_epoch == 41);
        }
    }

    /// The controller listener advertises what a bound Admin router serves, on
    /// top of the APIs it answers itself. `BrokerHeartbeat` is the case that
    /// matters: a broker reads this table to learn where to send it.
    #[test]
    fn an_admin_router_contributes_its_own_api_versions() {
        use krabka_protocol::owned::api_versions_response::ApiVersionsResponse;

        struct HeartbeatRouter;
        impl ControllerAdminRouter for HeartbeatRouter {
            fn api_versions(&self) -> &[ControllerApiVersion] { super::BROKER_HEARTBEAT_ONLY }

            fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
                Box::pin(async { Ok(None) })
            }
        }

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let body = super::api_versions_response_body(4, ApiVersionsView {
            admin_router: Some(&HeartbeatRouter),
            ..view(&image, -1)
        });
        let resp = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode body");

        let heartbeat = resp
            .api_keys
            .iter()
            .find(|key| key.api_key == 63)
            .expect("BrokerHeartbeat advertised through the Admin router");
        assert2::assert!(
            (heartbeat.min_version, heartbeat.max_version) == (MIN_VERSION, MAX_VERSION,)
        );
    }

    /// A router whose one API has an unstable last version, the shape of
    /// Kafka's `InitProducerId` v6 (`latestVersionUnstable`).
    struct UnstableRouter;

    impl ControllerAdminRouter for UnstableRouter {
        fn api_versions(&self) -> &[ControllerApiVersion] {
            &[ControllerApiVersion {
                api_key: 22,
                min_version: 0,
                max_version: 6,
                released_max: Some(5),
                flexible_min: 2,
            }]
        }

        fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
            Box::pin(async { Ok(None) })
        }
    }

    /// #646: an unstable last version is advertised, and accepted, only under
    /// `unstable.api.versions.enable`, as Kafka's `ApiKeys.toApiVersion` and
    /// `ApiKeys.isVersionEnabled` gate it.
    #[yare::parameterized(
        disabled = { UnstableApiVersions::Disabled, 5, true },
        enabled = { UnstableApiVersions::Enabled, 6, false },
    )]
    fn unstable_last_versions_follow_unstable_api_versions_enable(
        unstable: UnstableApiVersions,
        advertised_max: i16,
        v6_disabled: bool,
    ) {
        let api_keys = advertised_api_keys(Some(&UnstableRouter), unstable);
        let row = api_keys
            .iter()
            .find(|key| key.api_key == 22)
            .expect("router api advertised");
        assert2::check!(
            (row.min_version, row.max_version) == (0, advertised_max),
            "{unstable:?}"
        );
        assert2::check!(
            is_disabled_version(22, 6, Some(&UnstableRouter), unstable) == v6_disabled,
            "{unstable:?}"
        );
        assert2::check!(
            !is_disabled_version(22, 5, Some(&UnstableRouter), unstable),
            "{unstable:?}"
        );
        // An out-of-range version is the decoder's business, not this
        // gate's; an api the listener does not know is not disabled.
        assert2::check!(!is_disabled_version(22, 7, Some(&UnstableRouter), unstable));
        assert2::check!(!is_disabled_version(22, 6, None, unstable));
    }

    /// A router api Kafka 4.3.1 does not have -- `UnregisterController` (94),
    /// a trunk key -- is neither advertised nor accepted while unstable api
    /// versions are disabled, and is served whole when they are enabled.
    /// The controller listener's table is strictly ascending by api key, the
    /// router's rows merged in, as Kafka's `apisForListener` `EnumSet` orders
    /// it.
    #[test]
    fn the_advertised_table_is_strictly_ascending() {
        let api_keys = advertised_api_keys(Some(&UnstableRouter), UnstableApiVersions::Disabled);
        assert2::assert!(
            api_keys
                .windows(2)
                .all(|pair| pair[0].api_key < pair[1].api_key)
        );
    }

    /// One row per `ApiVersions` request shape: Kafka's unsupported-version
    /// answer, the `isValid` refusals, and the full table. A request whose
    /// version is not served carries a body that does not decode, because
    /// Kafka does not read it.
    /// A served version whose body does not decode fails the request, as
    /// Kafka's `RequestContext.parseRequest` does.
    #[test]
    fn api_versions_response_refuses_a_malformed_body() {
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        assert2::assert!(super::api_versions_response(3, &[0xff], view(&image, -1)).is_err());
    }

    /// #783 on the controller listener, table-driven over the issue's rows:
    /// the supported rows follow Kafka's minimums and `alterFeatureLevel0`,
    /// the finalized rows omit level 0 (`kraft.version` included), and the
    /// epoch is the metadata offset the view carries.
    /// #1344: before the first feature delta Kafka's controller answers from
    /// the initial state of `FeaturesPublisher`,
    /// `FinalizedFeatures.fromKRaftVersion(MINIMUM_VERSION)`, which finalizes
    /// `metadata.version` at its minimum with epoch -1. `ControllerServer`
    /// adds `kraft.version` through `setFinalizedLevel`, which drops level 0.
    /// Kafka's `ControllerServer` builds the listener's
    /// `SimpleApiVersionManager` with `unstable.api.versions.enable`, so that
    /// flag alone caps the supported `metadata.version` here. With only
    /// `unstable.feature.versions.enable` set the listener stays at the latest
    /// production level, and with only the API flag set it advertises the
    /// highest level krabka knows. The listener takes no feature flag at all:
    /// its view has none.
    #[yare::parameterized(
        disabled = { UnstableApiVersions::Disabled, LATEST_PRODUCTION_METADATA_VERSION },
        enabled = {
            UnstableApiVersions::Enabled,
            METADATA_VERSION_MAX,
        },
    )]
    fn the_controller_listener_caps_supported_features_with_the_api_flag(
        unstable: UnstableApiVersions,
        want_max: i16,
    ) {
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let view = ApiVersionsView {
            unstable,
            ..view(&image, 0)
        };
        let body = super::api_versions_response_body(4, view);
        let resp = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode");
        let metadata_version = resp
            .supported_features
            .iter()
            .find(|feature| feature.name == "metadata.version")
            .expect("metadata.version is supported");
        assert2::check!(
            metadata_version.max_version == want_max,
            "unstable.api.versions.enable {unstable:?}"
        );
    }
    use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;
    #[derive(serde::Deserialize)]
    struct VersionRequestInput {
        unstable: bool,
        version: i16,
        body: Option<(String, String, Option<String>, i32)>,
    }
    #[derive(serde::Deserialize)]
    struct VersionRequestExpected {
        version: i16,
        code: i16,
        apis: Vec<(i16, i16, i16)>,
        supported: Vec<(String, i16, i16)>,
        finalized: Vec<(String, i16, i16)>,
        epoch: i64,
    }
    impl VersionRequestExpected {
        fn response(self) -> ApiVersionsResponse {
            use krabka_protocol::owned::api_versions_response::{
                FinalizedFeatureKey, SupportedFeatureKey,
            };
            ApiVersionsResponse {
                error_code: self.code,
                api_keys: self
                    .apis
                    .into_iter()
                    .map(|(api_key, min_version, max_version)| ApiVersionEntry {
                        api_key,
                        min_version,
                        max_version,
                        ..Default::default()
                    })
                    .collect(),
                supported_features: self
                    .supported
                    .into_iter()
                    .map(|(name, min_version, max_version)| SupportedFeatureKey {
                        name,
                        min_version,
                        max_version,
                        ..Default::default()
                    })
                    .collect(),
                finalized_features: self
                    .finalized
                    .into_iter()
                    .map(
                        |(name, min_version_level, max_version_level)| FinalizedFeatureKey {
                            name,
                            min_version_level,
                            max_version_level,
                            ..Default::default()
                        },
                    )
                    .collect(),
                finalized_features_epoch: self.epoch,
                ..Default::default()
            }
        }
    }

    use {
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    struct TrunkOnlyRouter;
    impl ControllerAdminRouter for TrunkOnlyRouter {
        fn api_versions(&self) -> &[ControllerApiVersion] {
            &[ControllerApiVersion {
                api_key: 94,
                min_version: 0,
                max_version: 0,
                released_max: None,
                flexible_min: 0,
            }]
        }

        fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
            Box::pin(async { Ok(None) })
        }
    }
    #[derive(Default)]
    struct TrunkAdvertisement {
        enabled: bool,
        range: Slot<Option<(i16, i16)>>,
        disabled: Option<bool>,
    }
    #[rstest::fixture]
    fn trunk_advertisement() -> TrunkAdvertisement {
        // Start this scenario with fresh inputs and observations.
        TrunkAdvertisement::default()
    }
    #[given("the controller router serves UnregisterController and unstable APIs are {enabled}")]
    fn trunk_policy(trunk_advertisement: &mut TrunkAdvertisement, enabled: bool) {
        trunk_advertisement.enabled = enabled;
    }
    #[when("the controller advertises and checks UnregisterController version 0")]
    fn trunk_api(trunk_advertisement: &mut TrunkAdvertisement) {
        let policy = if trunk_advertisement.enabled {
            UnstableApiVersions::Enabled
        } else {
            UnstableApiVersions::Disabled
        };
        trunk_advertisement.range.set(
            advertised_api_keys(Some(&TrunkOnlyRouter), policy)
                .into_iter()
                .find(|key| key.api_key == 94)
                .map(|key| (key.min_version, key.max_version)),
        );
        trunk_advertisement.disabled =
            Some(is_disabled_version(94, 0, Some(&TrunkOnlyRouter), policy));
    }
    #[then("UnregisterController is advertised {advertised} and disabled {disabled}")]
    fn trunk_api_result(
        trunk_advertisement: &TrunkAdvertisement,
        advertised: bool,
        disabled: bool,
    ) {
        assert2::check!(trunk_advertisement.range.get() == Some(advertised.then_some((0, 0))));
        assert2::check!(trunk_advertisement.disabled == Some(disabled));
    }

    #[derive(Default)]
    struct VersionRequest {
        input: Option<VersionRequestInput>,
        answer: Option<bytes::Bytes>,
    }
    #[rstest::fixture]
    fn version_request() -> VersionRequest {
        // Each handshake owns its version, native client fields and returned bytes.
        VersionRequest::default()
    }
    #[given("an ApiVersions request with {request}")]
    fn api_request_input(version_request: &mut VersionRequest, request: String) {
        version_request.input = Some(
            serde_json::from_str(&request)
                .expect("ApiVersions version, policy and native body fields"),
        );
    }
    #[when("the controller handles the ApiVersions request under its release policy")]
    fn api_request_operation(version_request: &mut VersionRequest) {
        let input = version_request.input.as_ref().expect("request prepared");
        let body = input.body.as_ref().map_or_else(
            || bytes::Bytes::from_static(&[0xff]),
            |(name, software_version, cluster_id, node_id)| {
                let request = ApiVersionsRequest {
                    client_software_name: name.clone(),
                    client_software_version: software_version.clone(),
                    cluster_id: cluster_id.clone(),
                    node_id: *node_id,
                    ..Default::default()
                };
                let mut body = BytesMut::new();
                request
                    .encode(&mut body, input.version)
                    .expect("encode request");
                body.freeze()
            },
        );
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let view = ApiVersionsView {
            unstable: if input.unstable {
                UnstableApiVersions::Enabled
            } else {
                UnstableApiVersions::Disabled
            },
            ..view(&image, -1)
        };
        version_request.answer = Some(
            super::api_versions_response(input.version, &body, view).expect("controller answered"),
        );
    }
    #[then(
        "the ApiVersions answer consumes all bytes and matches its complete expected Kafka response {expected}"
    )]
    fn api_request_result(version_request: &VersionRequest, expected: String) {
        let expected: VersionRequestExpected =
            serde_json::from_str(&expected).expect("independent full ApiVersions response");
        let version = expected.version;
        let expected = expected.response();
        let mut cursor = &version_request.answer.as_ref().expect("request handled")[..];
        let decoded = ApiVersionsResponse::decode(&mut cursor, version).expect("decode answer");
        assert2::check!(cursor.is_empty());
        assert2::check!(decoded == expected);
        if expected.error_code == 0 {
            assert2::check!(!decoded.api_keys.is_empty());
        }
    }

    #[derive(Default)]
    struct FeatureAdvertisement {
        image: Option<krabka_metadata::MetadataImage>,
        epoch: i64,
        response: Option<ApiVersionsResponse>,
    }
    #[rstest::fixture]
    fn feature_advertisement() -> FeatureAdvertisement {
        // Start this scenario with fresh inputs and observations.
        FeatureAdvertisement::default()
    }
    fn advertised_levels(text: &str) -> Vec<(&str, i16)> {
        if text == "none" {
            return vec![];
        }
        text.split(',')
            .map(|entry| {
                let (name, level) = entry.split_once(':').expect("feature:level");
                (name, level.parse().expect("feature level"))
            })
            .collect()
    }
    #[given(
        "a controller at kraft version {kraft} with finalized levels {levels} and metadata epoch {epoch}"
    )]
    fn controller_feature_image(
        feature_advertisement: &mut FeatureAdvertisement,
        kraft: u16,
        levels: String,
        epoch: i64,
    ) {
        feature_advertisement.image = Some(feature_image(kraft, &advertised_levels(&levels)));
        feature_advertisement.epoch = epoch;
    }
    #[when("the controller answers feature fields at ApiVersions version {version}")]
    fn controller_feature_response(feature_advertisement: &mut FeatureAdvertisement, version: i16) {
        let image = feature_advertisement
            .image
            .as_ref()
            .expect("controller image prepared");
        let body =
            super::api_versions_response_body(version, view(image, feature_advertisement.epoch));
        feature_advertisement.response = Some(
            ApiVersionsResponse::decode(&mut &body[..], version).expect("decode feature response"),
        );
    }
    #[then(
        "supported features use modern minimums {modern} and finalized levels are {levels} at epoch {epoch}"
    )]
    fn controller_feature_fields(
        feature_advertisement: &FeatureAdvertisement,
        modern: bool,
        levels: String,
        epoch: i64,
    ) {
        krabka_macros::supported_feature_fixture!(supported);
        krabka_macros::supported_features_fixture!(modern_supported, supported);
        krabka_macros::finalized_feature_fixture!(finalized);
        let metadata = supported(
            "metadata.version",
            METADATA_VERSION_MIN,
            LATEST_PRODUCTION_METADATA_VERSION,
        );
        let expected = if modern {
            modern_supported(metadata, 1)
        } else {
            vec![metadata]
        };
        let levels = advertised_levels(&levels)
            .into_iter()
            .map(|(name, level)| finalized(name, level))
            .collect::<Vec<_>>();
        let actual = feature_advertisement
            .response
            .as_ref()
            .expect("feature response produced");
        assert2::check!(
            (
                &actual.supported_features,
                &actual.finalized_features,
                actual.finalized_features_epoch
            ) == (&expected, &levels, epoch)
        );
    }
    #[then(
        "the fresh controller finalizes the minimum metadata version and kraft version {kraft} at epoch minus one"
    )]
    fn fresh_feature_fields(feature_advertisement: &FeatureAdvertisement, kraft: u16) {
        krabka_macros::finalized_feature_fixture!(finalized);
        let mut expected = vec![];
        if kraft == 1 {
            expected.push(finalized("kraft.version", 1));
        }
        expected.push(finalized("metadata.version", METADATA_VERSION_MIN));
        let actual = feature_advertisement
            .response
            .as_ref()
            .expect("feature response produced");
        assert2::check!(
            (&actual.finalized_features, actual.finalized_features_epoch) == (&expected, -1)
        );
    }
    mod trunk_cases {
        use super::*;
        scenarios!("tests/features/migrated/src/server/api_versions.feature", tags="@trunk", fixtures=[trunk_advertisement: TrunkAdvertisement]);
    }
    mod request_cases {
        use super::*;
        scenarios!("tests/features/migrated/src/server/api_versions.feature", tags="@request", fixtures=[version_request: VersionRequest]);
    }
    mod features_cases {
        use super::*;
        scenarios!("tests/features/migrated/src/server/api_versions.feature", tags="@features", fixtures=[feature_advertisement: FeatureAdvertisement]);
    }
}
