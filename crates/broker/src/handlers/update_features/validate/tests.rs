//! Tests for the per-feature validation loop, kept in their own file because
//! they build whole metadata images and outweigh the module they cover.

use assert2::assert;

use super::*;
use crate::handlers::update_features::{
    test_support::{VERSION, metadata_update, validate_only},
    upgrade_type::{UPGRADE_TYPE_SAFE_DOWNGRADE, UPGRADE_TYPE_UNSAFE_DOWNGRADE},
};

#[test]
fn metadata_version_floor_via_registry() {
    // A fresh image floors metadata.version at its supported min; the
    // registry trait path returns that floor.
    let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    let feat = krabka_metadata::feature("metadata.version").unwrap();
    assert!(feat.min_required_floor(&image) == crate::features::METADATA_VERSION_MIN);
}

fn image_with_directory(metadata_version: i16) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    let supported_features = krabka_metadata::supported_feature_ranges();
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level: metadata_version,
    }));
    image.apply(&MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            node_id: krabka_metadata::NodeId(1),
            broker_epoch: 9,
            incarnation_id: uuid::Uuid::from_u128(1),
            host: "broker-1".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
            log_dirs: vec![uuid::Uuid::from_u128(0xD2)],
            features: supported_features.clone(),
        },
    ));
    image.apply(&MetadataRecord::V1Partition(
        krabka_metadata::PartitionRecord {
            topic: "orders".into(),
            partition: 0,
            leader: krabka_metadata::NodeId(1),
            replicas: vec![krabka_metadata::NodeId(1)],
            isr: vec![krabka_metadata::NodeId(1)],
            directories: vec![uuid::Uuid::from_u128(0xD1)],
            ..Default::default()
        },
    ));
    image
}

#[test]
fn unsafe_metadata_downgrade_cleans_lossy_fields_before_version_record() {
    let image = image_with_directory(crate::features::METADATA_VERSION_MAX);
    let target = krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1;

    let (safe_results, safe_records) = validate_updates(
        &validate_only(vec![metadata_update(target, UPGRADE_TYPE_SAFE_DOWNGRADE)]),
        &image,
        VERSION,
    );
    assert!(safe_results[0].error_code == codes::INVALID_UPDATE_VERSION);
    assert!(
        safe_results[0]
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("lossy"))
    );
    assert!(safe_records.is_empty());

    let (unsafe_results, unsafe_records) = validate_updates(
        &validate_only(vec![metadata_update(target, UPGRADE_TYPE_UNSAFE_DOWNGRADE)]),
        &image,
        VERSION,
    );
    assert!(unsafe_results[0].error_code == codes::NONE);
    let expected = vec![
        MetadataRecord::V1BrokerRegistration(krabka_metadata::BrokerRegistrationRecord {
            node_id: krabka_metadata::NodeId(1),
            broker_epoch: 9,
            incarnation_id: uuid::Uuid::from_u128(1),
            host: "broker-1".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
            log_dirs: vec![],
            features: krabka_metadata::supported_feature_ranges(),
        }),
        MetadataRecord::V1PartitionDirAssignment(krabka_metadata::PartitionDirAssignmentRecord {
            topic: "orders".into(),
            partition: 0,
            replica: krabka_metadata::NodeId(1),
            directory: uuid::Uuid::nil(),
        }),
        MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: target,
        }),
    ];
    assert!(unsafe_records == expected);

    let mut projected = image;
    for record in &unsafe_records {
        projected.apply(record);
    }
    assert!(
        projected
            .partition("orders", 0)
            .expect("partition")
            .directories
            == vec![uuid::Uuid::nil()]
    );
    assert!(
        projected
            .broker(krabka_metadata::NodeId(1))
            .expect("broker")
            .log_dirs
            .is_empty()
    );
    assert!(projected.finalized_metadata_version() == Some(target));
}

#[test]
fn safe_metadata_downgrade_preserves_representable_directory_fields() {
    let image = image_with_directory(crate::features::METADATA_VERSION_MAX);
    let target = krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL;

    let (results, records) = validate_updates(
        &validate_only(vec![metadata_update(target, UPGRADE_TYPE_SAFE_DOWNGRADE)]),
        &image,
        VERSION,
    );

    assert!(results[0].error_code == codes::NONE);
    assert!(
        records
            == vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: target,
            })]
    );
}

#[test]
fn metadata_downgrade_rejects_registered_nodes_without_capability() {
    let supported = maplit::btreemap! {krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into() => (
        crate::features::METADATA_VERSION_MIN,
        crate::features::METADATA_VERSION_MAX,
    )};
    let registrations = [
        (
            MetadataRecord::V1BrokerRegistration(krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_metadata::NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: String::new(),
                port: 0,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: supported.clone(),
            }),
            "Broker 2",
        ),
        (
            MetadataRecord::V1ControllerRegistration(
                krabka_metadata::ControllerRegistrationRecord {
                    node_id: krabka_metadata::NodeId(3),
                    incarnation_id: uuid::Uuid::nil(),
                    zk_migration_ready: false,
                    endpoints: vec![],
                    features: supported,
                },
            ),
            "Controller 3",
        ),
    ];

    for (registration, expected_node) in registrations {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: crate::features::METADATA_VERSION_MAX,
        }));
        image.apply(&registration);
        let (results, records) = validate_updates(
            &validate_only(vec![metadata_update(
                krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
                UPGRADE_TYPE_SAFE_DOWNGRADE,
            )]),
            &image,
            VERSION,
        );

        assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            results[0].error_message.as_deref().is_some_and(|message| {
                message.contains(expected_node)
                    && message.contains("does not support online metadata.version downgrade")
            }),
            "{results:?}"
        );
        assert!(records.is_empty());
    }
}

#[test]
fn metadata_update_checks_every_capable_registered_node_supports_target() {
    let mut supported = krabka_metadata::supported_feature_ranges();
    supported.insert(
        krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        (
            krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
            crate::features::METADATA_VERSION_MAX,
        ),
    );
    let registrations = [
        (
            MetadataRecord::V1BrokerRegistration(krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_metadata::NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: String::new(),
                port: 0,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: supported.clone(),
            }),
            "Broker 2",
        ),
        (
            MetadataRecord::V1ControllerRegistration(
                krabka_metadata::ControllerRegistrationRecord {
                    node_id: krabka_metadata::NodeId(3),
                    incarnation_id: uuid::Uuid::nil(),
                    zk_migration_ready: false,
                    endpoints: vec![],
                    features: supported,
                },
            ),
            "Controller 3",
        ),
    ];

    for (registration, expected_node) in registrations {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: crate::features::METADATA_VERSION_MAX,
        }));
        image.apply(&registration);
        let (results, records) = validate_updates(
            &validate_only(vec![metadata_update(
                krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1,
                UPGRADE_TYPE_SAFE_DOWNGRADE,
            )]),
            &image,
            VERSION,
        );

        assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            results[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains(expected_node)),
            "{results:?}"
        );
        assert!(records.is_empty());
    }
}

#[test]
fn metadata_downgrade_rejects_unregistered_quorum_controller() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level: crate::features::METADATA_VERSION_MAX,
    }));
    image.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
        voters: krabka_metadata::voters::VoterSet::from_voters([krabka_metadata::voters::Voter {
            id: krabka_metadata::NodeId(3),
            directory_id: uuid::Uuid::from_u128(3),
            endpoints: vec![],
            kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
        }]),
    }));

    let (results, records) = validate_updates(
        &validate_only(vec![metadata_update(
            krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
            UPGRADE_TYPE_SAFE_DOWNGRADE,
        )]),
        &image,
        VERSION,
    );

    assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
    assert!(
        results[0]
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("Controller 3 has not registered")),
        "{results:?}"
    );
    assert!(records.is_empty());
}

#[test]
fn downgrade_type_cannot_raise_a_finalized_feature() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level: krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
    }));
    let (results, records) = validate_updates(
        &validate_only(vec![metadata_update(
            krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL + 1,
            UPGRADE_TYPE_SAFE_DOWNGRADE,
        )]),
        &image,
        VERSION,
    );

    assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
    assert!(
        results[0]
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("newer"))
    );
    assert!(records.is_empty());
}
