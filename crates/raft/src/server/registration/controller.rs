//! The `ControllerRegistration` API (KIP-919): records a quorum voter's
//! endpoints and supported feature ranges in the metadata log.
//!
//! Unlike a broker, a controller must already be a voter before it may
//! register, and every refusal carries a message rather than a bare code, so
//! the checks and the feature-range decoding live together here.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_metadata::{ControllerRegistrationRecord, MetadataRecord, NodeId};
use krabka_protocol::{
    Decode, owned::controller_registration_request::ControllerRegistrationRequest,
};

use super::{
    CLUSTER_AUTHORIZATION_FAILED, INVALID_REGISTRATION, NOT_CONTROLLER, SUCCESS,
    UNKNOWN_CONTROLLER_ID, is_leader, listeners::decode_controller_listeners, raft_error_code,
    response::controller_registration_response,
};
use crate::{RaftError, kraft::KraftController};

pub(super) async fn controller_registration(
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

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::controller_registration_request;

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
}
