//! Kafka broker/controller lifecycle RPCs served on the controller listener.

use std::collections::{BTreeMap, HashSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, ControllerRegistrationRecord, MetadataRecord, NodeId,
};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        broker_heartbeat_request::{self, BrokerHeartbeatRequest},
        broker_heartbeat_response::BrokerHeartbeatResponse,
        broker_registration_request::{self, BrokerRegistrationRequest},
        broker_registration_response::BrokerRegistrationResponse,
        controller_registration_request::{self, ControllerRegistrationRequest},
        controller_registration_response::ControllerRegistrationResponse,
    },
};
use krabka_security::ListenerProtocol;

use crate::{RaftError, kraft::KraftController};

const SUCCESS: i16 = 0;
const UNKNOWN_SERVER_ERROR: i16 = -1;
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const UNSUPPORTED_VERSION: i16 = 35;
const NOT_CONTROLLER: i16 = 41;
const STALE_BROKER_EPOCH: i16 = 77;
const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
const BROKER_ID_NOT_REGISTERED: i16 = 102;
const INCONSISTENT_CLUSTER_ID: i16 = 104;
const UNKNOWN_CONTROLLER_ID: i16 = 116;
const INVALID_REGISTRATION: i16 = 119;

pub(super) const SUPPORTED_APIS: [(i16, i16); 3] = [
    (
        broker_registration_request::API_KEY,
        broker_registration_request::MAX_VERSION,
    ),
    (
        broker_heartbeat_request::API_KEY,
        broker_heartbeat_request::MAX_VERSION,
    ),
    (
        controller_registration_request::API_KEY,
        controller_registration_request::MAX_VERSION,
    ),
];

pub(super) fn is_controller_api(api_key: i16) -> bool {
    SUPPORTED_APIS.iter().any(|&(key, _)| key == api_key)
}

pub(super) async fn dispatch(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    match api_key {
        broker_registration_request::API_KEY => {
            broker_registration(version, body, engine, authorized).await
        }
        broker_heartbeat_request::API_KEY => broker_heartbeat(version, body, engine, authorized),
        controller_registration_request::API_KEY => {
            controller_registration(version, body, engine, authorized).await
        }
        _ => Err(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("unknown controller lifecycle API"),
        )),
    }
}

async fn broker_registration(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = BrokerRegistrationRequest::decode(&mut body, version)?;
    if !authorized {
        return broker_registration_response(version, CLUSTER_AUTHORIZATION_FAILED, -1);
    }
    if !is_leader(engine) {
        return broker_registration_response(version, NOT_CONTROLLER, -1);
    }
    let image = engine.current_image();
    let node_id = match u64::try_from(request.broker_id) {
        Ok(id) => NodeId(id),
        Err(_) => return broker_registration_response(version, INVALID_REGISTRATION, -1),
    };
    if !cluster_id_matches(&request.cluster_id, image.cluster_id()) {
        return broker_registration_response(version, INCONSISTENT_CLUSTER_ID, -1);
    }
    if request.is_migrating_zk_broker {
        return broker_registration_response(version, BROKER_ID_NOT_REGISTERED, -1);
    }
    let endpoints = match decode_broker_listeners(&request.listeners) {
        Ok(endpoints) => endpoints,
        Err(code) => return broker_registration_response(version, code, -1),
    };
    if !features_support_finalized(&request, &image) {
        return broker_registration_response(version, UNSUPPORTED_VERSION, -1);
    }
    let incarnation_id = uuid::Uuid::from_bytes(request.incarnation_id.0);
    if let Some(existing) = image.broker(node_id) {
        let result = if existing.incarnation_id == incarnation_id {
            (SUCCESS, existing.broker_epoch)
        } else {
            (DUPLICATE_BROKER_REGISTRATION, -1)
        };
        return broker_registration_response(version, result.0, result.1);
    }

    let first = &endpoints[0];
    let features = request
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    let log_dirs = request
        .log_dirs
        .iter()
        .map(|directory| uuid::Uuid::from_bytes(directory.0))
        .collect();
    let record = BrokerRegistrationRecord {
        node_id,
        broker_epoch: 0,
        incarnation_id,
        host: first.host.clone(),
        port: first.port,
        rack: request.rack,
        endpoints,
        log_dirs,
        features,
    };
    if let Err(error) = engine
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(record)])
        .await
    {
        return broker_registration_response(version, raft_error_code(&error), -1);
    }
    let epoch = engine.current_image().broker_epoch(node_id).unwrap_or(-1);
    let error = if epoch < 0 {
        UNKNOWN_SERVER_ERROR
    } else {
        SUCCESS
    };
    broker_registration_response(version, error, epoch)
}

fn broker_heartbeat(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = BrokerHeartbeatRequest::decode(&mut body, version)?;
    let mut response = BrokerHeartbeatResponse::default();
    response.error_code = if !authorized {
        CLUSTER_AUTHORIZATION_FAILED
    } else if !is_leader(engine) {
        NOT_CONTROLLER
    } else {
        validate_heartbeat(&request, &engine.current_image(), &mut response)
    };
    encode(&response, version)
}

/// Takes the image rather than the engine it came from: the decision is a
/// function of the registration on record, and a test should be able to hand it
/// one without standing up a quorum to hold it.
fn validate_heartbeat(
    request: &BrokerHeartbeatRequest,
    image: &krabka_metadata::MetadataImage,
    response: &mut BrokerHeartbeatResponse,
) -> i16 {
    let Ok(node) = u64::try_from(request.broker_id).map(NodeId) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    let Some(registration) = image.broker(node) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    if registration.broker_epoch != request.broker_epoch {
        return STALE_BROKER_EPOCH;
    }
    response.is_caught_up = request.current_metadata_offset >= registration.broker_epoch;
    response.is_fenced = request.want_fence || !response.is_caught_up;
    response.should_shut_down = request.want_shut_down;
    SUCCESS
}

async fn controller_registration(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = ControllerRegistrationRequest::decode(&mut body, version)?;
    if !authorized {
        return controller_registration_response(
            version,
            CLUSTER_AUTHORIZATION_FAILED,
            Some("cluster action denied".into()),
        );
    }
    if !is_leader(engine) {
        return controller_registration_response(version, NOT_CONTROLLER, None);
    }
    let node_id = match u64::try_from(request.controller_id) {
        Ok(id) => NodeId(id),
        Err(_) => {
            return controller_registration_response(
                version,
                INVALID_REGISTRATION,
                Some("controller id must be non-negative".into()),
            );
        }
    };
    if !engine.quorum_snapshot().voters.contains(node_id) {
        return controller_registration_response(
            version,
            UNKNOWN_CONTROLLER_ID,
            Some(format!(
                "controller {} is not a quorum voter",
                request.controller_id
            )),
        );
    }
    let endpoints = match decode_controller_listeners(&request.listeners) {
        Ok(endpoints) => endpoints,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let features = match decode_controller_features(&request) {
        Ok(features) => features,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let record = ControllerRegistrationRecord {
        node_id,
        incarnation_id: uuid::Uuid::from_bytes(request.incarnation_id.0),
        zk_migration_ready: request.zk_migration_ready,
        endpoints,
        features,
    };
    if engine.current_image().controller(node_id) == Some(&record) {
        return controller_registration_response(version, SUCCESS, None);
    }
    let result = engine
        .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
        .await;
    match result {
        Ok(_) => controller_registration_response(version, SUCCESS, None),
        Err(error) => controller_registration_response(
            version,
            raft_error_code(&error),
            Some(error.to_string()),
        ),
    }
}

fn decode_controller_features(
    request: &ControllerRegistrationRequest,
) -> Result<BTreeMap<String, (i16, i16)>, String> {
    let features: BTreeMap<_, _> = request
        .features
        .iter()
        .map(|feature| {
            (
                feature.name.clone(),
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    if features
        .iter()
        .any(|(name, (min, max))| name.is_empty() || min > max)
    {
        return Err("invalid controller feature range".into());
    }
    Ok(features)
}

fn cluster_id_matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == cluster_id.to_string() || request == URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
}

fn decode_broker_listeners(
    listeners: &[broker_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, i16> {
    decode_listeners(listeners.iter().map(|listener| {
        (
            listener.name.as_str(),
            listener.host.as_str(),
            listener.port,
            listener.security_protocol,
        )
    }))
    .map_err(|_| INVALID_REGISTRATION)
}

fn decode_controller_listeners(
    listeners: &[controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    decode_listeners(listeners.iter().map(|listener| {
        (
            listener.name.as_str(),
            listener.host.as_str(),
            listener.port,
            listener.security_protocol,
        )
    }))
}

fn decode_listeners<'a>(
    listeners: impl Iterator<Item = (&'a str, &'a str, u16, i16)>,
) -> Result<Vec<BrokerEndpoint>, String> {
    let mut names = HashSet::new();
    let endpoints = listeners
        .map(|(name, host, port, protocol)| {
            if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name.to_owned()) {
                return Err("invalid or duplicate registration listener".into());
            }
            let protocol = protocol_from_wire(protocol)
                .ok_or_else(|| "unknown listener security protocol".to_owned())?;
            Ok(BrokerEndpoint {
                name: name.to_owned(),
                host: host.to_owned(),
                port,
                protocol,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if endpoints.is_empty() {
        return Err("registration has no listeners".into());
    }
    Ok(endpoints)
}

fn protocol_from_wire(protocol: i16) -> Option<ListenerProtocol> {
    match protocol {
        0 => Some(ListenerProtocol::Plaintext),
        1 => Some(ListenerProtocol::Ssl),
        2 => Some(ListenerProtocol::SaslPlaintext),
        3 => Some(ListenerProtocol::SaslSsl),
        _ => None,
    }
}

fn features_support_finalized(
    request: &BrokerRegistrationRequest,
    image: &krabka_metadata::MetadataImage,
) -> bool {
    image.finalized_features().iter().all(|(name, level)| {
        request.features.iter().any(|feature| {
            feature.name == *name
                && feature.min_supported_version <= *level
                && *level <= feature.max_supported_version
        })
    })
}

fn is_leader(engine: &KraftController) -> bool {
    engine.watch_leader().borrow().as_ref() == Some(&engine.node_id())
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => NOT_CONTROLLER,
        RaftError::Metadata(_) | RaftError::ChangeRejected(_) => INVALID_REGISTRATION,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn broker_registration_response(
    version: i16,
    error_code: i16,
    broker_epoch: i64,
) -> Result<Bytes, RaftError> {
    encode(
        &BrokerRegistrationResponse {
            error_code,
            broker_epoch,
            ..Default::default()
        },
        version,
    )
}

fn controller_registration_response(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, RaftError> {
    encode(
        &ControllerRegistrationResponse {
            error_code,
            error_message,
            ..Default::default()
        },
        version,
    )
}

fn encode(response: &impl Encode, version: i16) -> Result<Bytes, RaftError> {
    let mut bytes = BytesMut::new();
    response.encode(&mut bytes, version)?;
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn feature(name: &str, min: i16, max: i16) -> controller_registration_request::Feature {
        controller_registration_request::Feature {
            name: name.to_owned(),
            min_supported_version: min,
            max_supported_version: max,
            ..Default::default()
        }
    }

    fn request_with(
        features: Vec<controller_registration_request::Feature>,
    ) -> ControllerRegistrationRequest {
        ControllerRegistrationRequest {
            features,
            ..Default::default()
        }
    }

    /// The two registration responses carry the error code and the epoch or
    /// message the caller passed, and encode to bytes.
    ///
    /// These are one-line wrappers, which is exactly why nothing tested them:
    /// swapping a field or dropping the encode leaves a function that still
    /// returns `Ok`.
    #[test]
    fn registration_responses_carry_what_they_were_given() {
        use krabka_protocol::{
            Decode as _,
            owned::{broker_registration_response, controller_registration_response},
        };

        let bytes = broker_registration_response(
            broker_registration_response::MAX_VERSION,
            INVALID_REGISTRATION,
            42,
        )
        .expect("encode broker response");
        let mut cursor = &bytes[..];
        let decoded = BrokerRegistrationResponse::decode(
            &mut cursor,
            broker_registration_response::MAX_VERSION,
        )
        .expect("decode broker response");
        check!((decoded.error_code, decoded.broker_epoch) == (INVALID_REGISTRATION, 42));

        let bytes = controller_registration_response(
            controller_registration_response::MAX_VERSION,
            NOT_CONTROLLER,
            Some("not the controller".to_owned()),
        )
        .expect("encode controller response");
        let mut cursor = &bytes[..];
        let decoded = ControllerRegistrationResponse::decode(
            &mut cursor,
            controller_registration_response::MAX_VERSION,
        )
        .expect("decode controller response");
        check!(decoded.error_code == NOT_CONTROLLER);
        check!(decoded.error_message.as_deref() == Some("not the controller"));
    }

    /// The controller answers the lifecycle APIs it declares and nothing else.
    #[test]
    fn only_the_declared_apis_are_controller_apis() {
        for &(key, _) in &SUPPORTED_APIS {
            check!(is_controller_api(key), "declared api {key}");
        }
        // A key nothing declares: Produce is a broker API, never a controller one.
        check!(!is_controller_api(0), "Produce is not a controller api");
        check!(!is_controller_api(i16::MAX));
    }

    /// Both listener decoders run the same checks; each reports failure in the
    /// shape its caller needs -- an error code for the broker path, a message
    /// for the controller path.
    #[test]
    fn both_listener_decoders_reject_an_unusable_listener() {
        let broker_bad = vec![broker_registration_request::Listener {
            name: String::new(),
            host: "host".to_owned(),
            port: 9092,
            security_protocol: 0,
            ..Default::default()
        }];
        check!(decode_broker_listeners(&broker_bad) == Err(INVALID_REGISTRATION));

        let broker_ok = vec![broker_registration_request::Listener {
            name: "PLAINTEXT".to_owned(),
            host: "host".to_owned(),
            port: 9092,
            security_protocol: 0,
            ..Default::default()
        }];
        let decoded = decode_broker_listeners(&broker_ok).expect("a usable listener");
        check!(decoded.len() == 1 && decoded[0].port == 9092);

        let controller_bad = vec![controller_registration_request::Listener {
            name: "CONTROLLER".to_owned(),
            host: String::new(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        }];
        check!(decode_controller_listeners(&controller_bad).is_err());

        let controller_ok = vec![controller_registration_request::Listener {
            name: "CONTROLLER".to_owned(),
            host: "host".to_owned(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        }];
        let decoded = decode_controller_listeners(&controller_ok).expect("a usable listener");
        check!(decoded.len() == 1 && decoded[0].port == 9093);
    }

    /// A heartbeat is answered from the registration on record: an unknown
    /// broker and a stale epoch are refused, and a known one is told whether
    /// it is caught up, fenced, and asked to shut down.
    ///
    /// `is_fenced` is the one that matters most -- a broker that is behind
    /// must be fenced whether or not it asked to be, because it is the
    /// controller's job to keep a lagging replica out of the ISR.
    #[test]
    fn a_heartbeat_is_answered_from_the_registration_on_record() {
        use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord};

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(7),
                broker_epoch: 100,
                incarnation_id: uuid::Uuid::nil(),
                host: "broker-7".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));

        let answer = |broker_id: i32,
                      broker_epoch: i64,
                      offset: i64,
                      want_fence: bool,
                      want_shut_down: bool| {
            let request = BrokerHeartbeatRequest {
                broker_id,
                broker_epoch,
                current_metadata_offset: offset,
                want_fence,
                want_shut_down,
                ..Default::default()
            };
            let mut response = BrokerHeartbeatResponse::default();
            let code = validate_heartbeat(&request, &image, &mut response);
            (code, response)
        };

        // A broker nobody registered.
        let (code, _) = answer(9, 100, 100, false, false);
        check!(code == BROKER_ID_NOT_REGISTERED, "an unregistered broker");

        // A negative id is not a node id at all.
        let (code, _) = answer(-1, 100, 100, false, false);
        check!(code == BROKER_ID_NOT_REGISTERED, "a negative broker id");

        // The right broker at the wrong epoch.
        let (code, _) = answer(7, 99, 100, false, false);
        check!(code == STALE_BROKER_EPOCH, "a stale epoch");

        // Caught up, asking for nothing: not fenced, not shutting down.
        let (code, response) = answer(7, 100, 100, false, false);
        check!(code == SUCCESS);
        check!(
            (
                response.is_caught_up,
                response.is_fenced,
                response.should_shut_down
            ) == (true, false, false),
            "caught up and unfenced"
        );

        // Behind the registration: fenced even though it did not ask to be.
        let (_, response) = answer(7, 100, 99, false, false);
        check!(
            (response.is_caught_up, response.is_fenced) == (false, true),
            "a lagging broker is fenced regardless"
        );

        // Caught up but asking to be fenced, and to shut down.
        let (_, response) = answer(7, 100, 100, true, true);
        check!(
            (
                response.is_caught_up,
                response.is_fenced,
                response.should_shut_down
            ) == (true, true, true),
            "an explicit fence and shutdown are honoured"
        );
    }

    /// A broker may register only if it supports every feature the cluster has
    /// already finalized, at the finalized level.
    ///
    /// The level has to sit inside the broker's advertised range on both
    /// sides, and a feature the broker does not mention at all is not support.
    /// Admitting a broker that cannot speak a finalized feature puts a member
    /// in the cluster that will mis-handle records already being written.
    #[test]
    fn a_broker_registers_only_when_it_supports_every_finalized_feature() {
        use krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};

        /// (what the broker advertises, may it register?)
        type Case<'a> = (&'a str, &'a [(&'a str, i16, i16)], bool);

        fn image_finalizing(name: &str, level: i16) -> MetadataImage {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.to_owned(),
                level,
            }));
            image
        }

        fn request_supporting(features: &[(&str, i16, i16)]) -> BrokerRegistrationRequest {
            BrokerRegistrationRequest {
                features: features
                    .iter()
                    .map(|&(name, min, max)| broker_registration_request::Feature {
                        name: name.to_owned(),
                        min_supported_version: min,
                        max_supported_version: max,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        }

        let image = image_finalizing("metadata.version", 20);
        // (what the broker advertises, may it register?)
        let cases: &[Case<'_>] = &[
            (
                "a range around the finalized level",
                &[("metadata.version", 7, 25)],
                true,
            ),
            (
                "a range starting at it",
                &[("metadata.version", 20, 25)],
                true,
            ),
            ("a range ending at it", &[("metadata.version", 7, 20)], true),
            (
                "a range entirely below it",
                &[("metadata.version", 7, 19)],
                false,
            ),
            (
                "a range entirely above it",
                &[("metadata.version", 21, 25)],
                false,
            ),
            ("some other feature only", &[("group.version", 0, 1)], false),
            ("nothing at all", &[], false),
        ];
        for (what, features, may_register) in cases {
            let request = request_supporting(features);
            check!(
                features_support_finalized(&request, &image) == *may_register,
                "{what}"
            );
        }

        // With nothing finalized there is nothing to support, so any broker may
        // register -- including one advertising no features.
        let empty = MetadataImage::new(uuid::Uuid::nil());
        check!(features_support_finalized(&request_supporting(&[]), &empty));
    }

    /// A controller's advertised feature ranges are taken as given only when
    /// every one of them is named and non-inverted.
    ///
    /// A blank name collides with any other blank name in the map, and a range
    /// whose minimum exceeds its maximum supports nothing -- accepting either
    /// puts a value into the controller's view of the cluster that no version
    /// negotiation can satisfy.
    #[test]
    fn controller_feature_ranges_must_be_named_and_not_inverted() {
        // (what it is, features, accepted?)
        let cases: Vec<(&str, Vec<controller_registration_request::Feature>, bool)> = vec![
            ("no features at all", vec![], true),
            (
                "one ordinary range",
                vec![feature("kraft.version", 0, 1)],
                true,
            ),
            (
                "a range that is a single point",
                vec![feature("metadata.version", 7, 7)],
                true,
            ),
            (
                "several ordinary ranges",
                vec![
                    feature("kraft.version", 0, 1),
                    feature("group.version", 0, 1),
                ],
                true,
            ),
            ("a nameless feature", vec![feature("", 0, 1)], false),
            (
                "an inverted range",
                vec![feature("kraft.version", 2, 1)],
                false,
            ),
            (
                "one good range and one inverted",
                vec![
                    feature("kraft.version", 0, 1),
                    feature("group.version", 5, 4),
                ],
                false,
            ),
        ];
        for (what, features, accepted) in cases {
            let request = request_with(features);
            let got = decode_controller_features(&request);
            check!(got.is_ok() == accepted, "{what}: {got:?}");
        }
    }

    /// The decoded map carries each feature's range under its own name.
    #[test]
    fn decoded_controller_features_keep_their_ranges() {
        let request = request_with(vec![
            feature("kraft.version", 0, 1),
            feature("metadata.version", 7, 25),
        ]);
        let decoded = decode_controller_features(&request).expect("valid ranges");
        check!(decoded.len() == 2);
        check!(decoded.get("kraft.version") == Some(&(0, 1)));
        check!(decoded.get("metadata.version") == Some(&(7, 25)));
    }

    /// The wire's security-protocol numbering, and nothing outside it.
    #[test]
    fn listener_protocol_numbers_map_to_their_protocols() {
        let cases = [
            (0i16, Some(ListenerProtocol::Plaintext)),
            (1, Some(ListenerProtocol::Ssl)),
            (2, Some(ListenerProtocol::SaslPlaintext)),
            (3, Some(ListenerProtocol::SaslSsl)),
            (4, None),
            (-1, None),
            (i16::MAX, None),
        ];
        for (wire, want) in cases {
            check!(protocol_from_wire(wire) == want, "protocol {wire}");
        }
    }

    /// A listener set is rejected when any entry is unusable or repeats a
    /// name, and an empty set is rejected outright.
    #[test]
    fn registration_listeners_must_be_usable_and_uniquely_named() {
        type Row<'a> = (&'a str, Vec<(&'a str, &'a str, u16, i16)>, bool);
        let cases: Vec<Row<'_>> = vec![
            (
                "one usable listener",
                vec![("PLAINTEXT", "host", 9092, 0)],
                true,
            ),
            (
                "two, differently named",
                vec![("PLAINTEXT", "host", 9092, 0), ("SSL", "host", 9093, 1)],
                true,
            ),
            ("none at all", vec![], false),
            ("a nameless listener", vec![("", "host", 9092, 0)], false),
            (
                "a hostless listener",
                vec![("PLAINTEXT", "", 9092, 0)],
                false,
            ),
            ("port zero", vec![("PLAINTEXT", "host", 0, 0)], false),
            (
                "a repeated name",
                vec![
                    ("PLAINTEXT", "host", 9092, 0),
                    ("PLAINTEXT", "host", 9093, 0),
                ],
                false,
            ),
            (
                "an unknown security protocol",
                vec![("PLAINTEXT", "host", 9092, 9)],
                false,
            ),
        ];
        for (what, listeners, accepted) in cases {
            let got = decode_listeners(listeners.iter().map(|&(n, h, p, proto)| (n, h, p, proto)));
            check!(got.is_ok() == accepted, "{what}: {got:?}");
        }
    }

    /// Each raft failure maps to the error code a Kafka client acts on: a
    /// leadership problem tells it to look elsewhere, a rejected registration
    /// tells it not to retry unchanged.
    #[test]
    fn raft_errors_map_to_the_client_visible_code() {
        check!(raft_error_code(&RaftError::LeaderUnknown) == NOT_CONTROLLER);
        check!(raft_error_code(&RaftError::ChangeRejected("no".into())) == INVALID_REGISTRATION,);
        // Anything else is not something the client can act on specifically.
        check!(raft_error_code(&RaftError::Shutdown) == UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn recognizes_kafka_and_uuid_cluster_ids() {
        let cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::assert!(cluster_id_matches(&cluster_id.to_string(), cluster_id));
        assert2::assert!(cluster_id_matches(
            &URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()),
            cluster_id
        ));
    }

    #[test]
    fn lifecycle_api_table_matches_generated_schemas() {
        assert2::assert!(SUPPORTED_APIS == [(62, 4), (63, 2), (70, 0)]);
    }
}
