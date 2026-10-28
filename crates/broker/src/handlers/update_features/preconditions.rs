//! Predicates over the live metadata image that gate a feature update.
//!
//! Each function answers one question about the cluster as the image records
//! it, in the words Kafka's controller answers it: whether every node supports
//! a level (`FeatureControlManager.reasonNotSupported`) and whether the
//! KIP-1022 dependencies of a level hold (`Feature.validateVersion`).

use {krabka_metadata::metadata_version, std::collections::BTreeMap};

/// `MetadataVersion.isControllerRegistrationSupported`: controllers
/// register from `3.7-IV0`.
const CONTROLLER_REGISTRATION_MIN_LEVEL: i16 = metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL;

/// The inclusive range of levels that a node supports for a feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SupportedRange {
    min: i16,
    max: i16,
}

/// Kafka's `QuorumFeatures.DISABLED`: what a node that did not register a
/// feature supports.
const DISABLED: SupportedRange = SupportedRange { min: 0, max: 0 };

/// `QuorumFeatures.reasonNotSupported`, with `VersionRange.toString`.
fn reason_range_not_supported(
    level: i16,
    what: &str,
    SupportedRange { min, max }: SupportedRange,
) -> Option<String> {
    if (min..=max).contains(&level) {
        None
    } else if max == 0 {
        Some(format!("{what} does not support this feature."))
    } else if min == max {
        Some(format!("{what} only supports versions {min}"))
    } else {
        Some(format!("{what} only supports versions {min}-{max}"))
    }
}

/// The controller answering an `UpdateFeatures`: its node id, which Kafka
/// names as `Local controller <id>`, and the
/// `unstable.feature.versions.enable` its own supported ranges follow
/// (`QuorumFeatures.defaultSupportedFeatureMap`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalController {
    pub node_id: krabka_metadata::NodeId,
    pub unstable_features: krabka_raft::UnstableFeatureVersions,
}

/// `FeatureControlManager.reasonNotSupported`: why some node cannot take
/// `feature` to `level`, or `None` when every node can.
///
/// The local controller answers first from this binary's feature registry,
/// then every registered broker. From `3.7-IV0`, when controllers register,
/// every other registered controller answers too, and a quorum voter that has
/// not registered blocks the update.
pub(super) fn reason_not_supported(
    image: &krabka_metadata::MetadataImage,
    local: LocalController,
    feature: &str,
    level: i16,
) -> Option<String> {
    let local_controller = local.node_id;
    let local = krabka_metadata::feature(feature).map_or(DISABLED, |feature| {
        let (min, max) = krabka_raft::supported_feature_range(feature, local.unstable_features);
        SupportedRange { min, max }
    });
    let range = |registered| match registered {
        Some((min, max)) => SupportedRange { min, max },
        None => DISABLED,
    };
    reason_range_not_supported(
        level,
        &format!("Local controller {local_controller}"),
        local,
    )
    .or_else(|| {
        image.brokers().find_map(|broker| {
            reason_range_not_supported(
                level,
                &format!("Broker {}", broker.node_id),
                range(broker.features.get(feature).copied()),
            )
        })
    })
    .or_else(|| {
        if image
            .finalized_metadata_version()
            .is_none_or(|mv| mv < CONTROLLER_REGISTRATION_MIN_LEVEL)
        {
            return None;
        }
        image
            .controllers()
            .filter(|controller| controller.node_id != local_controller)
            .find_map(|controller| {
                reason_range_not_supported(
                    level,
                    &format!("Controller {}", controller.node_id),
                    range(controller.features.get(feature).copied()),
                )
            })
            .or_else(|| {
                image
                    .voters()
                    .iter()
                    .map(|voter| voter.id)
                    .find(|&id| id != local_controller && image.controller(id).is_none())
                    .map(|id| {
                        format!(
                            "controller {id} has not registered, and may not support this \
                                 feature"
                        )
                    })
            })
    })
}

/// The dependency half of `Feature.validateVersion`: why `feature` cannot be
/// set to `level` given `proposed`, the levels the request would leave, or
/// `None` when every dependency holds. `deps` is the feature's
/// `dependencies(level)`.
pub(super) fn dependency_error(
    feature: &str,
    level: i16,
    deps: &[(&str, i16)],
    proposed: &BTreeMap<String, i16>,
) -> Option<String> {
    deps.iter().find_map(|&(dep, min_level)| {
        proposed
            .get(dep)
            .is_none_or(|&have| have < min_level)
            .then(|| {
                format!(
                    "{feature} could not be set to {level} because it depends on {dep} level \
                     {min_level}"
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::broker_registration,
        assert2::assert,
        krabka_metadata::{
            BrokerRegistrationRecord, ControllerRegistrationRecord, FeatureLevelRecord,
            MetadataImage, MetadataRecord, NodeId,
            voters::{KRaftVersionRange, Voter, VoterSet},
        },
        rstest_bdd::Slot,
    };

    const LOCAL: LocalController = LocalController {
        node_id: NodeId(1),
        unstable_features: krabka_raft::UnstableFeatureVersions::Disabled,
    };

    struct FeatureCompatibilityContract {
        range: SupportedRange,
        level: i16,
        image: Option<MetadataImage>,
        reason: Slot<Option<String>>,
    }

    #[rstest::fixture]
    fn feature_compatibility() -> FeatureCompatibilityContract {
        // Start this scenario with fresh inputs and observations.

        FeatureCompatibilityContract {
            range: DISABLED,
            level: 0,
            image: None,
            reason: Slot::default(),
        }
    }

    #[rstest_bdd_macros::given("Broker 2 supports feature levels {minimum} through {maximum}")]
    fn broker_feature_range(
        feature_compatibility: &mut FeatureCompatibilityContract,
        minimum: i16,
        maximum: i16,
    ) {
        feature_compatibility.range = SupportedRange {
            min: minimum,
            max: maximum,
        };
    }

    #[rstest_bdd_macros::given("the requested feature level is {level}")]
    fn requested_feature_level(
        feature_compatibility: &mut FeatureCompatibilityContract,
        level: i16,
    ) {
        feature_compatibility.level = level;
    }

    #[rstest_bdd_macros::given("the feature-update cluster uses metadata version {version}")]
    fn feature_cluster_metadata(
        feature_compatibility: &mut FeatureCompatibilityContract,
        version: i16,
    ) {
        feature_compatibility.image = Some(image(version));
    }

    fn advertised_group_support(availability: &str) -> BTreeMap<String, (i16, i16)> {
        let mut features = krabka_metadata::supported_feature_ranges();
        match availability {
            "present" => {}
            "absent" => {
                features.remove("group.version");
            }
            other => panic!("unknown feature availability {other}"),
        }
        features
    }

    #[rstest_bdd_macros::given("broker {node} advertises group.version as {availability}")]
    fn broker_group_feature(
        feature_compatibility: &mut FeatureCompatibilityContract,
        node: u64,
        availability: String,
    ) {
        feature_compatibility
            .image
            .as_mut()
            .expect("a cluster metadata version was given")
            .apply(&broker(node, advertised_group_support(&availability)));
    }

    #[rstest_bdd_macros::given("controller {node} advertises group.version as {availability}")]
    fn controller_group_feature(
        feature_compatibility: &mut FeatureCompatibilityContract,
        node: u64,
        availability: String,
    ) {
        feature_compatibility
            .image
            .as_mut()
            .expect("a cluster metadata version was given")
            .apply(&controller(node, advertised_group_support(&availability)));
    }

    #[rstest_bdd_macros::given("the feature-update quorum voters are {nodes}")]
    fn feature_quorum(feature_compatibility: &mut FeatureCompatibilityContract, nodes: String) {
        let ids: Vec<u64> = nodes
            .split(',')
            .map(|id| id.parse().expect("a voter node ID"))
            .collect();
        feature_compatibility
            .image
            .as_mut()
            .expect("a cluster metadata version was given")
            .apply(&voters(&ids));
    }

    #[rstest_bdd_macros::when("Broker 2 checks the requested feature level against its range")]
    fn check_broker_feature_range(feature_compatibility: &mut FeatureCompatibilityContract) {
        feature_compatibility.reason.set(reason_range_not_supported(
            feature_compatibility.level,
            "Broker 2",
            feature_compatibility.range,
        ));
    }

    #[rstest_bdd_macros::when("controller 1 checks cluster support for {feature} level {level}")]
    fn check_cluster_feature(
        feature_compatibility: &mut FeatureCompatibilityContract,
        feature: String,
        level: i16,
    ) {
        feature_compatibility.reason.set(reason_not_supported(
            feature_compatibility
                .image
                .as_ref()
                .expect("a cluster was given"),
            LOCAL,
            &feature,
            level,
        ));
    }

    #[rstest_bdd_macros::then("the feature compatibility diagnostic is {message}")]
    fn feature_compatibility_reason(
        feature_compatibility: &FeatureCompatibilityContract,
        message: String,
    ) {
        let expected: Option<String> =
            serde_json::from_str(&message).expect("nullable JSON diagnostic");
        assert!(feature_compatibility.reason.get() == Some(expected));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/update_features/preconditions.feature", fixtures = [feature_compatibility: FeatureCompatibilityContract]);

    /// KIP-1191: `share.version` 2 is Kafka trunk's `SV_2`, so a controller
    /// supports it only with `unstable.feature.versions.enable`, and answers
    /// as a 4.3.1 controller does without it.
    #[test]
    fn share_version_two_needs_unstable_feature_versions() {
        let local = |unstable| LocalController {
            node_id: NodeId(1),
            unstable_features: unstable,
        };
        let image = MetadataImage::new(uuid::Uuid::nil());
        let rows = [
            (1, krabka_raft::UnstableFeatureVersions::Disabled, None),
            (
                2,
                krabka_raft::UnstableFeatureVersions::Disabled,
                Some("Local controller 1 only supports versions 0-1"),
            ),
            (2, krabka_raft::UnstableFeatureVersions::Enabled, None),
            (
                3,
                krabka_raft::UnstableFeatureVersions::Enabled,
                Some("Local controller 1 only supports versions 0-2"),
            ),
        ];
        for (level, unstable, want) in rows {
            assert!(
                reason_not_supported(&image, local(unstable), "share.version", level).as_deref()
                    == want,
                "{level} {unstable:?}"
            );
        }
    }

    fn image(metadata_version: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: metadata_version,
        }));
        image
    }

    fn broker(node_id: u64, features: BTreeMap<String, (i16, i16)>) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            host: String::new(),
            port: 0,
            features,
            ..broker_registration(krabka_raft::NodeId(node_id))
        })
    }

    fn controller(node_id: u64, features: BTreeMap<String, (i16, i16)>) -> MetadataRecord {
        MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
            node_id: NodeId(node_id),
            incarnation_id: uuid::Uuid::nil(),
            zk_migration_ready: false,
            endpoints: vec![],
            features,
        })
    }

    fn voters(ids: &[u64]) -> MetadataRecord {
        MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
            voters: VoterSet::from_voters(ids.iter().map(|&id| Voter {
                id: NodeId(id),
                directory_id: uuid::Uuid::from_u128(u128::from(id)),
                endpoints: vec![],
                kraft_version: KRaftVersionRange::default(),
            })),
        })
    }

    #[yare::parameterized(
        absent = {
            None,
            Some(
                "eligible.leader.replicas.version could not be set to 1 because it depends on metadata.version level 23",
            ),
        },
        below = {
            Some(22),
            Some(
                "eligible.leader.replicas.version could not be set to 1 because it depends on metadata.version level 23",
            ),
        },
        minimum = { Some(23), None },
        above = { Some(30), None },
    )]
    fn dependencies_read_the_proposed_levels(mv: Option<i16>, expected: Option<&str>) {
        let deps: &[(&str, i16)] = &[("metadata.version", 23)];
        let proposed = |mv: Option<i16>| -> BTreeMap<String, i16> {
            mv.map(|level| ("metadata.version".to_owned(), level))
                .into_iter()
                .collect()
        };
        let expected = expected.map(str::to_owned);
        assert2::assert!(
            dependency_error("eligible.leader.replicas.version", 1, deps, &proposed(mv))
                == expected,
            "{mv:?}"
        );
        assert2::assert!(dependency_error("group.version", 1, &[], &proposed(None)).is_none());
    }
}
