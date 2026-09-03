//! Tests for the per-feature validation loop, kept in their own file because
//! they build whole metadata images and outweigh the module they cover.

use assert2::assert;

use super::*;
use crate::handlers::update_features::{
    test_support::{VERSION, elr_update, metadata_update, validate_only},
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
fn rejected_lossy_downgrade_is_retryable_and_write_free() {
    let image = image_with_directory(crate::features::METADATA_VERSION_MAX);
    let target = krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1;
    let request = validate_only(vec![metadata_update(target, UPGRADE_TYPE_SAFE_DOWNGRADE)]);

    for _ in 0..2 {
        let (results, records) = validate_updates(&request, &image, VERSION);
        assert!(results[0].error_code == codes::INVALID_UPDATE_VERSION);
        assert!(
            results[0]
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("lossy"))
        );
        assert!(records.is_empty());
        assert!(
            image
                .partition("orders", 0)
                .expect("unchanged partition")
                .directories
                == vec![uuid::Uuid::from_u128(0xD1)]
        );
    }
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

/// An image at `metadata_version`, with `eligible.leader.replicas.version`
/// finalized at `elr_level` when one is given and a published ELR on topic
/// `orders` when `published` is.
fn elr_image(
    metadata_version: i16,
    elr_level: Option<i16>,
    published: Option<&str>,
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level: metadata_version,
    }));
    if let Some(level) = elr_level {
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::ELR_VERSION.into(),
            level,
        }));
    }
    if let Some(published) = published {
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "orders".into(),
                overrides: [
                    (
                        crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                        "2".to_string(),
                    ),
                    (
                        crate::config_keys::ELIGIBLE_LEADER_REPLICAS.to_string(),
                        published.to_string(),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ));
    }
    image
}

/// KIP-966: finalizing the feature back to 0 clears the memberships it
/// published, the way Kafka's controller emits its own cleaning records. The
/// clearing records go in ahead of the feature record, so a replay that stops
/// between them has forgotten the memberships rather than kept them under a
/// feature that still reads as on.
#[test]
fn an_elr_downgrade_clears_the_published_state_before_the_feature_record() {
    let image = elr_image(
        crate::features::METADATA_VERSION_MAX,
        Some(1),
        Some("0:2,3:"),
    );
    let request = validate_only(vec![elr_update(0, UPGRADE_TYPE_SAFE_DOWNGRADE)]);
    let (results, records) = validate_updates(&request, &image, VERSION);

    assert!(results[0].error_code == codes::NONE, "{results:?}");
    assert!(
        records
            == vec![
                MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                    topic: "orders".into(),
                    overrides: [(
                        crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                        "2".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                }),
                MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: crate::features::ELR_VERSION.into(),
                    level: 0,
                }),
            ],
        "{records:?}"
    );
}

/// A cluster that never turned the feature on has nothing to clear, so the
/// downgrade is the feature record alone.
#[test]
fn an_elr_downgrade_without_published_state_emits_only_the_feature_record() {
    let image = elr_image(crate::features::METADATA_VERSION_MAX, Some(1), None);
    let request = validate_only(vec![elr_update(0, UPGRADE_TYPE_SAFE_DOWNGRADE)]);
    let (results, records) = validate_updates(&request, &image, VERSION);

    assert!(results[0].error_code == codes::NONE, "{results:?}");
    assert!(
        records
            == vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::ELR_VERSION.into(),
                level: 0,
            })],
        "{records:?}"
    );
}

/// KIP-1022: `ELRV_1` depends on `metadata.version` at 4.0-IV1, the level
/// whose `PartitionRecord` carries the ELR fields, so a cluster below it
/// cannot finalize the feature.
#[test]
fn elr_level_one_requires_the_elr_metadata_version() {
    for (case, metadata_version, want_code) in [
        (
            "below 4.0-IV1",
            krabka_metadata::metadata_version::ELR_MIN_LEVEL - 1,
            codes::INVALID_UPDATE_VERSION,
        ),
        (
            "at 4.0-IV1",
            krabka_metadata::metadata_version::ELR_MIN_LEVEL,
            codes::NONE,
        ),
        (
            "above 4.0-IV1",
            crate::features::METADATA_VERSION_MAX,
            codes::NONE,
        ),
    ] {
        let image = elr_image(metadata_version, None, None);
        let request = validate_only(vec![elr_update(1, 1)]);
        let (results, _records) = validate_updates(&request, &image, VERSION);
        assert!(results[0].error_code == want_code, "{case}: {results:?}");
    }
}
