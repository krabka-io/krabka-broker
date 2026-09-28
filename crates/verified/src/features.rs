//! Feature-finalization admission for one `UpdateFeatures` row.
//!
//! The decision follows Kafka 4.3.1's `FeatureControlManager.updateFeature`
//! check for check: the upgrade type, a negative level, every node's support
//! (`reasonNotSupported`), the update direction against the current level
//! (an unfinalized feature reads as level 0), and then the per-feature tail:
//! `updateMetadataVersion`'s `didMetadataChange` refusal for
//! `metadata.version`, the no-downgrade rule for `kraft.version`, and
//! `Feature.validateVersion`'s KIP-1022 dependencies for every other feature.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::*;

/// KIP-584 `FeatureUpdate.UpgradeType`, decoded from the request.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FeatureUpdateType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

/// The levels one feature row compares.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FeatureLevels {
    /// The row's `MaxVersionLevel`.
    pub requested: i16,
    /// The current level, `0` for a feature that is not finalized.
    pub current: i16,
}

/// Which branch of `updateFeature` decides the row after the shared checks,
/// with the one fact that branch reads.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FeatureKind {
    /// `metadata.version`. `metadata_changed` is Kafka's
    /// `MetadataVersion.checkIfMetadataChanged` between the current and the
    /// requested level.
    MetadataVersion { metadata_changed: bool },
    /// `kraft.version`, which the Raft layer finalizes.
    KRaftVersion,
    /// Every other feature. `dependencies_met` is `Feature.validateVersion`
    /// against the proposed levels: the finalized levels with every update
    /// of the same request applied.
    Other { dependencies_met: bool },
}

/// Everything one `UpdateFeatures` row is decided on.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FeatureUpdateFacts {
    /// `None` when the row carries an unknown upgrade-type code.
    pub update_type: Option<FeatureUpdateType>,
    pub levels: FeatureLevels,
    /// The local controller, every registered broker, and every registered
    /// controller support the requested level, and every quorum controller
    /// has registered.
    pub all_nodes_support: bool,
    pub kind: FeatureKind,
}

/// The first failed rule, or what an admitted row does.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FeatureUpdateDecision {
    UnknownUpdateType,
    NegativeLevel,
    UnsupportedByNode,
    DowngradeWithoutFlag,
    DowngradeToNewerLevel,
    /// A `metadata.version` downgrade across a level that changed metadata,
    /// asked for as `UNSAFE_DOWNGRADE`.
    UnsafeMetadataDowngrade,
    /// A `metadata.version` downgrade across a level that changed metadata,
    /// asked for as `SAFE_DOWNGRADE`.
    LossyMetadataDowngrade,
    /// A `kraft.version` downgrade.
    KRaftDowngrade,
    DependencyUnmet,
    /// Emit the feature-level record.
    EmitFeature,
    /// Hand the `kraft.version` upgrade to the Raft layer.
    UpgradeKRaft,
    /// A `kraft.version` row with a downgrade type at the current level:
    /// nothing to do.
    NoChange,
}

/// The reference decision: the first failed rule in `updateFeature`'s
/// order, or what the admitted row does.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn feature_update_model(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    pearlite! {
        match facts.update_type {
            None => FeatureUpdateDecision::UnknownUpdateType,
            Some(update_type) => {
                let requested = facts.levels.requested@;
                let current = facts.levels.current@;
                if requested < 0 {
                    FeatureUpdateDecision::NegativeLevel
                } else if !facts.all_nodes_support {
                    FeatureUpdateDecision::UnsupportedByNode
                } else if requested < current && update_type == FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DowngradeWithoutFlag
                } else if requested > current && update_type != FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DowngradeToNewerLevel
                } else {
                    match facts.kind {
                        FeatureKind::MetadataVersion { metadata_changed } =>
                            if requested < current && metadata_changed {
                                if update_type == FeatureUpdateType::UnsafeDowngrade {
                                    FeatureUpdateDecision::UnsafeMetadataDowngrade
                                } else {
                                    FeatureUpdateDecision::LossyMetadataDowngrade
                                }
                            } else {
                                FeatureUpdateDecision::EmitFeature
                            },
                        FeatureKind::KRaftVersion =>
                            if update_type == FeatureUpdateType::Upgrade {
                                FeatureUpdateDecision::UpgradeKRaft
                            } else if requested != current {
                                FeatureUpdateDecision::KRaftDowngrade
                            } else {
                                FeatureUpdateDecision::NoChange
                            },
                        FeatureKind::Other { dependencies_met } =>
                            if dependencies_met {
                                FeatureUpdateDecision::EmitFeature
                            } else {
                                FeatureUpdateDecision::DependencyUnmet
                            },
                    }
                }
            }
        }
    }
}

/// Decide one feature row from the host's facts.
///
/// A rejection names the first rule that fails in the order of Kafka's
/// `FeatureControlManager.updateFeature`. A `metadata.version` downgrade is
/// admitted only when no level between the two changed metadata, whatever the
/// downgrade type, and an admitted `metadata.version` row never writes
/// anything but its feature-level record.
#[cfg_attr(creusot, ensures(result == feature_update_model(facts)))]
#[cfg_attr(creusot, ensures(match facts.kind {
    FeatureKind::MetadataVersion { metadata_changed } =>
        (metadata_changed && result == FeatureUpdateDecision::EmitFeature)
            ==> facts.levels.requested@ >= facts.levels.current@,
    _ => true,
}))]
#[must_use]
pub fn feature_update_decision(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    let Some(update_type) = facts.update_type else {
        return FeatureUpdateDecision::UnknownUpdateType;
    };
    let FeatureLevels { requested, current } = facts.levels;
    if requested < 0 {
        return FeatureUpdateDecision::NegativeLevel;
    }
    if !facts.all_nodes_support {
        return FeatureUpdateDecision::UnsupportedByNode;
    }
    let upgrade = matches!(update_type, FeatureUpdateType::Upgrade);
    if requested < current && upgrade {
        return FeatureUpdateDecision::DowngradeWithoutFlag;
    }
    if requested > current && !upgrade {
        return FeatureUpdateDecision::DowngradeToNewerLevel;
    }
    match facts.kind {
        FeatureKind::MetadataVersion { metadata_changed } => {
            if requested < current && metadata_changed {
                if matches!(update_type, FeatureUpdateType::UnsafeDowngrade) {
                    FeatureUpdateDecision::UnsafeMetadataDowngrade
                } else {
                    FeatureUpdateDecision::LossyMetadataDowngrade
                }
            } else {
                FeatureUpdateDecision::EmitFeature
            }
        }
        FeatureKind::KRaftVersion => {
            if upgrade {
                FeatureUpdateDecision::UpgradeKRaft
            } else if requested != current {
                FeatureUpdateDecision::KRaftDowngrade
            } else {
                FeatureUpdateDecision::NoChange
            }
        }
        FeatureKind::Other { dependencies_met } => {
            if dependencies_met {
                FeatureUpdateDecision::EmitFeature
            } else {
                FeatureUpdateDecision::DependencyUnmet
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        FeatureKind as K, FeatureLevels, FeatureUpdateDecision as D, FeatureUpdateFacts,
        FeatureUpdateType as T, feature_update_decision,
    };

    const fn row(update_type: T, requested: i16, current: i16, kind: K) -> FeatureUpdateFacts {
        FeatureUpdateFacts {
            update_type: Some(update_type),
            levels: FeatureLevels { requested, current },
            all_nodes_support: true,
            kind,
        }
    }

    const MET: K = K::Other {
        dependencies_met: true,
    };
    const UNMET: K = K::Other {
        dependencies_met: false,
    };
    const LOSSY: K = K::MetadataVersion {
        metadata_changed: true,
    };
    const LOSSLESS: K = K::MetadataVersion {
        metadata_changed: false,
    };

    #[test]
    fn shared_checks_follow_update_feature_order() {
        let unsupported = |facts: FeatureUpdateFacts| FeatureUpdateFacts {
            all_nodes_support: false,
            ..facts
        };
        let cases = [
            ("upgrade", row(T::Upgrade, 1, 0, MET), D::EmitFeature),
            ("same level", row(T::Upgrade, 1, 1, MET), D::EmitFeature),
            (
                "safe downgrade",
                row(T::SafeDowngrade, 0, 1, MET),
                D::EmitFeature,
            ),
            (
                "unsafe downgrade",
                row(T::UnsafeDowngrade, 0, 1, MET),
                D::EmitFeature,
            ),
            // A level-0 downgrade of a feature that is not finalized is a
            // no-op that Kafka still writes.
            (
                "delete unfinalized",
                row(T::SafeDowngrade, 0, 0, MET),
                D::EmitFeature,
            ),
            (
                "unknown type before everything",
                FeatureUpdateFacts {
                    update_type: None,
                    ..unsupported(row(T::Upgrade, -1, 0, UNMET))
                },
                D::UnknownUpdateType,
            ),
            (
                "negative before support",
                unsupported(row(T::Upgrade, -1, 0, MET)),
                D::NegativeLevel,
            ),
            (
                "support before direction",
                unsupported(row(T::SafeDowngrade, 1, 0, MET)),
                D::UnsupportedByNode,
            ),
            (
                "downgrade as upgrade",
                row(T::Upgrade, 0, 1, MET),
                D::DowngradeWithoutFlag,
            ),
            (
                "downgrade type raises",
                row(T::SafeDowngrade, 1, 0, MET),
                D::DowngradeToNewerLevel,
            ),
            (
                "direction before dependencies",
                row(T::UnsafeDowngrade, 1, 0, UNMET),
                D::DowngradeToNewerLevel,
            ),
            (
                "dependency unmet",
                row(T::Upgrade, 1, 0, UNMET),
                D::DependencyUnmet,
            ),
        ];
        for (case, facts, expected) in cases {
            assert!(feature_update_decision(facts) == expected, "{case}");
        }
    }

    #[test]
    fn metadata_version_refuses_every_lossy_downgrade() {
        let cases = [
            (
                "lossless safe",
                row(T::SafeDowngrade, 24, 25, LOSSLESS),
                D::EmitFeature,
            ),
            (
                "lossless unsafe",
                row(T::UnsafeDowngrade, 24, 25, LOSSLESS),
                D::EmitFeature,
            ),
            (
                "lossy safe",
                row(T::SafeDowngrade, 22, 25, LOSSY),
                D::LossyMetadataDowngrade,
            ),
            (
                "lossy unsafe",
                row(T::UnsafeDowngrade, 17, 25, LOSSY),
                D::UnsafeMetadataDowngrade,
            ),
            // The walk only matters for a downgrade.
            (
                "upgrade across a change",
                row(T::Upgrade, 30, 25, LOSSY),
                D::EmitFeature,
            ),
            (
                "upgrade type downgrade",
                row(T::Upgrade, 24, 25, LOSSLESS),
                D::DowngradeWithoutFlag,
            ),
        ];
        for (case, facts, expected) in cases {
            assert!(feature_update_decision(facts) == expected, "{case}");
        }
    }

    #[test]
    fn kraft_version_upgrades_through_raft_and_never_downgrades() {
        let cases = [
            (
                "upgrade",
                row(T::Upgrade, 1, 0, K::KRaftVersion),
                D::UpgradeKRaft,
            ),
            (
                "same level upgrade",
                row(T::Upgrade, 1, 1, K::KRaftVersion),
                D::UpgradeKRaft,
            ),
            (
                "downgrade",
                row(T::SafeDowngrade, 0, 1, K::KRaftVersion),
                D::KRaftDowngrade,
            ),
            (
                "unsafe downgrade",
                row(T::UnsafeDowngrade, 0, 1, K::KRaftVersion),
                D::KRaftDowngrade,
            ),
            (
                "downgrade type at the current level",
                row(T::SafeDowngrade, 1, 1, K::KRaftVersion),
                D::NoChange,
            ),
            (
                "downgrade type raises",
                row(T::SafeDowngrade, 1, 0, K::KRaftVersion),
                D::DowngradeToNewerLevel,
            ),
        ];
        for (case, facts, expected) in cases {
            assert!(feature_update_decision(facts) == expected, "{case}");
        }
    }
}
