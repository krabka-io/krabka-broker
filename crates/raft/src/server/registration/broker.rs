//! The `BrokerRegistration` API (KIP-631): admits a broker to the cluster and
//! records it in the metadata log.
//!
//! Registration is where a broker is checked against the cluster it claims to
//! be joining -- the cluster id it names, the listeners it advertises, and the
//! finalized features it says it can speak -- before a
//! `BrokerRegistrationRecord` is submitted for it.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, NodeId};
use krabka_protocol::{Decode, owned::broker_registration_request::BrokerRegistrationRequest};

use super::{
    BROKER_ID_NOT_REGISTERED, CLUSTER_AUTHORIZATION_FAILED, DUPLICATE_BROKER_REGISTRATION,
    INCONSISTENT_CLUSTER_ID, INVALID_REGISTRATION, NOT_CONTROLLER, SUCCESS, UNKNOWN_SERVER_ERROR,
    UNSUPPORTED_VERSION, is_leader, listeners::decode_broker_listeners, raft_error_code,
    response::broker_registration_response,
};
use crate::{RaftError, kraft::KraftController};

pub(super) async fn broker_registration(
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
    match incarnation_decision(&image, node_id, incarnation_id) {
        krabka_verified::BrokerRegistrationDecision::RejectCompatibility => {
            return broker_registration_response(version, INVALID_REGISTRATION, -1);
        }
        krabka_verified::BrokerRegistrationDecision::Idempotent(epoch) => {
            return broker_registration_response(version, SUCCESS, epoch);
        }
        krabka_verified::BrokerRegistrationDecision::DuplicateIncarnation => {
            return broker_registration_response(version, DUPLICATE_BROKER_REGISTRATION, -1);
        }
        krabka_verified::BrokerRegistrationDecision::Register => {}
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

fn incarnation_decision(
    image: &krabka_metadata::MetadataImage,
    node_id: NodeId,
    incarnation_id: uuid::Uuid,
) -> krabka_verified::BrokerRegistrationDecision {
    let existing = image.broker(node_id);
    krabka_verified::broker_registration_decision(
        true,
        true,
        true,
        true,
        true,
        existing.map(|registration| registration.broker_epoch),
        existing.is_some_and(|registration| registration.incarnation_id == incarnation_id),
    )
}

fn cluster_id_matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == cluster_id.to_string() || request == URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
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

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::broker_registration_request;

    use super::*;

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
    fn incarnation_adapter_is_idempotent_and_fences_replacements() {
        use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord};
        use krabka_verified::BrokerRegistrationDecision::{
            DuplicateIncarnation, Idempotent, Register,
        };

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        let incarnation = uuid::Uuid::from_u128(0xA7);
        check!(incarnation_decision(&image, NodeId(7), incarnation) == Register);
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(7),
                broker_epoch: 42,
                incarnation_id: incarnation,
                host: "broker-7".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));

        for _ in 0..2 {
            check!(incarnation_decision(&image, NodeId(7), incarnation) == Idempotent(42));
        }
        check!(
            incarnation_decision(&image, NodeId(7), uuid::Uuid::from_u128(0xB7))
                == DuplicateIncarnation
        );
    }
}
