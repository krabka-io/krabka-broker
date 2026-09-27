//! Feature-finalization admission and downgrade record sequencing.
//!
//! The decision follows the order in which Kafka's
//! `FeatureControlManager.updateFeature` rejects an `UpdateFeatures` row:
//! the upgrade type, the requested level against the supported range, every
//! registered node's support (`reasonNotSupported`), and the update direction.
//! Krabka then adds its online `metadata.version` downgrade gates, the
//! representability floor, KIP-1022 dependencies, KIP-584 delete semantics,
//! and the explicit unsafe mode that a lossy downgrade needs.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::*;

/// KIP-584 `FeatureUpdate.UpgradeType`, decoded from either request shape.
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
    /// The row's `MaxVersionLevel`; `0` deletes the finalized feature.
    pub requested: i16,
    /// The finalized level, or `None` when the feature is not finalized.
    pub finalized: Option<i16>,
    /// The highest level this controller supports.
    pub max_supported: i16,
    /// The lowest level the target image can represent.
    pub floor: i16,
}

/// Facts that the host folds over the registered nodes and the target image.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FeatureClusterFacts {
    /// Every registered broker and controller supports the requested level.
    pub all_nodes_support: bool,
    /// Every KIP-1022 dependency of the requested level is finalized high
    /// enough in the target image.
    pub dependencies_met: bool,
}

/// Facts that only a `metadata.version` row carries.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct MetadataVersionFacts {
    /// The lowest level an online downgrade may target.
    pub online_downgrade_min_level: i16,
    /// Every quorum voter has registered as a controller.
    pub all_controllers_registered: bool,
    /// Every registered node supports online `metadata.version` downgrade.
    pub all_nodes_downgrade_capable: bool,
    /// The downgrade must emit records that discard metadata the target
    /// level cannot represent.
    pub cleanup_required: bool,
}

/// Everything one `UpdateFeatures` row is decided on.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FeatureUpdateFacts {
    /// `None` when the row carries an unknown upgrade-type code.
    pub update_type: Option<FeatureUpdateType>,
    pub levels: FeatureLevels,
    pub cluster: FeatureClusterFacts,
    /// `Some` exactly for the `metadata.version` feature.
    pub metadata_version: Option<MetadataVersionFacts>,
}

/// The first failed rule, or the record plan of an admitted row.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FeatureUpdateDecision {
    UnknownUpdateType,
    OutOfSupportedRange,
    UnsupportedByNode,
    DowngradeWithoutFlag,
    DowngradeToNewerLevel,
    OnlineDowngradeUnsupported,
    UnregisteredController,
    NodeCannotDowngrade,
    BelowFloor,
    DependencyUnmet,
    DeleteMissingFeature,
    DeleteWithoutFlag,
    LossyDowngradeNotUnsafe,
    /// Emit only the feature-level record.
    EmitFeature,
    /// Emit every cleanup record, then the feature-level record.
    EmitCleanupThenFeature,
}

/// Kafka compares against the finalized level, with an unfinalized feature
/// at level 0.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn finalized_or_zero(levels: FeatureLevels) -> Int {
    pearlite! {
        match levels.finalized {
            Some(level) => level@,
            None => 0,
        }
    }
}

/// The row lowers the finalized level.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn is_downgrade(levels: FeatureLevels) -> bool {
    pearlite! { levels.requested@ < finalized_or_zero(levels) }
}

/// Kafka's direction rule: a lower level needs a downgrade type, and a
/// downgrade type cannot raise the level.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn direction_valid(update_type: FeatureUpdateType, levels: FeatureLevels) -> bool {
    pearlite! {
        if is_downgrade(levels) {
            update_type != FeatureUpdateType::Upgrade
        } else if levels.requested@ > finalized_or_zero(levels) {
            update_type == FeatureUpdateType::Upgrade
        } else {
            true
        }
    }
}

/// The online `metadata.version` downgrade gates hold, or the row is not a
/// `metadata.version` downgrade.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn metadata_downgrade_gates_hold(facts: FeatureUpdateFacts) -> bool {
    pearlite! {
        match facts.metadata_version {
            Some(mv) => !is_downgrade(facts.levels)
                || (facts.levels.requested@ >= mv.online_downgrade_min_level@
                    && mv.all_controllers_registered
                    && mv.all_nodes_downgrade_capable),
            None => true,
        }
    }
}

/// KIP-584 delete: level 0 removes a finalized feature and needs a
/// downgrade type.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn delete_valid(update_type: FeatureUpdateType, levels: FeatureLevels) -> bool {
    pearlite! {
        levels.requested@ != 0
            || (levels.finalized != None && update_type != FeatureUpdateType::Upgrade)
    }
}

/// The row discards metadata, which only an unsafe downgrade may do.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn cleanup_required(facts: FeatureUpdateFacts) -> bool {
    pearlite! {
        match facts.metadata_version {
            Some(mv) => mv.cleanup_required,
            None => false,
        }
    }
}

/// Every rule of a feature finalization holds.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn feature_update_admissible(facts: FeatureUpdateFacts) -> bool {
    pearlite! {
        match facts.update_type {
            None => false,
            Some(update_type) => 0 <= facts.levels.requested@
                && facts.levels.requested@ <= facts.levels.max_supported@
                && facts.cluster.all_nodes_support
                && direction_valid(update_type, facts.levels)
                && metadata_downgrade_gates_hold(facts)
                && (facts.levels.requested@ == 0
                    || facts.levels.requested@ >= facts.levels.floor@)
                && facts.cluster.dependencies_met
                && delete_valid(update_type, facts.levels)
                && (!cleanup_required(facts) || update_type == FeatureUpdateType::UnsafeDowngrade)
        }
    }
}

/// The reference decision: the first failed rule in precedence order, or
/// the record plan.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn feature_update_model(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    pearlite! {
        match facts.update_type {
            None => FeatureUpdateDecision::UnknownUpdateType,
            Some(update_type) => {
                let levels = facts.levels;
                if levels.requested@ < 0 || levels.requested@ > levels.max_supported@ {
                    FeatureUpdateDecision::OutOfSupportedRange
                } else if !facts.cluster.all_nodes_support {
                    FeatureUpdateDecision::UnsupportedByNode
                } else if is_downgrade(levels) && update_type == FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DowngradeWithoutFlag
                } else if !direction_valid(update_type, levels) {
                    FeatureUpdateDecision::DowngradeToNewerLevel
                } else if !metadata_downgrade_gates_hold(facts) {
                    match facts.metadata_version {
                        Some(mv) => if levels.requested@ < mv.online_downgrade_min_level@ {
                            FeatureUpdateDecision::OnlineDowngradeUnsupported
                        } else if !mv.all_controllers_registered {
                            FeatureUpdateDecision::UnregisteredController
                        } else {
                            FeatureUpdateDecision::NodeCannotDowngrade
                        },
                        None => FeatureUpdateDecision::NodeCannotDowngrade,
                    }
                } else if levels.requested@ > 0 && levels.requested@ < levels.floor@ {
                    FeatureUpdateDecision::BelowFloor
                } else if !facts.cluster.dependencies_met {
                    FeatureUpdateDecision::DependencyUnmet
                } else if levels.requested@ == 0 && levels.finalized == None {
                    FeatureUpdateDecision::DeleteMissingFeature
                } else if levels.requested@ == 0 && update_type == FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DeleteWithoutFlag
                } else if cleanup_required(facts)
                    && update_type != FeatureUpdateType::UnsafeDowngrade
                {
                    FeatureUpdateDecision::LossyDowngradeNotUnsafe
                } else if cleanup_required(facts) {
                    FeatureUpdateDecision::EmitCleanupThenFeature
                } else {
                    FeatureUpdateDecision::EmitFeature
                }
            }
        }
    }
}

/// Whether a decision emits records.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn emits(decision: FeatureUpdateDecision) -> bool {
    pearlite! {
        decision == FeatureUpdateDecision::EmitFeature
            || decision == FeatureUpdateDecision::EmitCleanupThenFeature
    }
}

/// Decide one feature finalization from the host's facts.
///
/// A row emits records exactly when every rule holds, and a lossy downgrade
/// is admitted only as an explicit unsafe downgrade whose cleanup records
/// precede the feature-level record that makes the older format
/// authoritative. A rejection names the first failed rule.
#[cfg_attr(creusot, ensures(result == feature_update_model(facts)))]
#[cfg_attr(creusot, ensures(emits(result) == feature_update_admissible(facts)))]
#[cfg_attr(creusot, ensures((result == FeatureUpdateDecision::EmitCleanupThenFeature)
    == (feature_update_admissible(facts) && cleanup_required(facts))))]
#[must_use]
pub fn feature_update_decision(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    let Some(update_type) = facts.update_type else {
        return FeatureUpdateDecision::UnknownUpdateType;
    };
    let levels = facts.levels;
    if levels.requested < 0 || levels.requested > levels.max_supported {
        return FeatureUpdateDecision::OutOfSupportedRange;
    }
    if !facts.cluster.all_nodes_support {
        return FeatureUpdateDecision::UnsupportedByNode;
    }
    let finalized = levels.finalized.unwrap_or(0);
    let upgrade = matches!(update_type, FeatureUpdateType::Upgrade);
    let downgrade = levels.requested < finalized;
    if downgrade && upgrade {
        return FeatureUpdateDecision::DowngradeWithoutFlag;
    }
    if levels.requested > finalized && !upgrade {
        return FeatureUpdateDecision::DowngradeToNewerLevel;
    }
    let mut cleanup_required = false;
    if let Some(mv) = facts.metadata_version {
        if downgrade {
            if levels.requested < mv.online_downgrade_min_level {
                return FeatureUpdateDecision::OnlineDowngradeUnsupported;
            }
            if !mv.all_controllers_registered {
                return FeatureUpdateDecision::UnregisteredController;
            }
            if !mv.all_nodes_downgrade_capable {
                return FeatureUpdateDecision::NodeCannotDowngrade;
            }
        }
        cleanup_required = mv.cleanup_required;
    }
    if levels.requested > 0 && levels.requested < levels.floor {
        return FeatureUpdateDecision::BelowFloor;
    }
    if !facts.cluster.dependencies_met {
        return FeatureUpdateDecision::DependencyUnmet;
    }
    if levels.requested == 0 {
        if levels.finalized.is_none() {
            return FeatureUpdateDecision::DeleteMissingFeature;
        }
        if upgrade {
            return FeatureUpdateDecision::DeleteWithoutFlag;
        }
    }
    if !cleanup_required {
        FeatureUpdateDecision::EmitFeature
    } else if matches!(update_type, FeatureUpdateType::UnsafeDowngrade) {
        FeatureUpdateDecision::EmitCleanupThenFeature
    } else {
        FeatureUpdateDecision::LossyDowngradeNotUnsafe
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        FeatureClusterFacts, FeatureLevels, FeatureUpdateDecision as D, FeatureUpdateFacts,
        FeatureUpdateType as T, MetadataVersionFacts, feature_update_decision,
    };

    /// `group.version` finalized at 1, supported up to 1, no floor above 0.
    const GROUP: FeatureUpdateFacts = FeatureUpdateFacts {
        update_type: Some(T::Upgrade),
        levels: FeatureLevels {
            requested: 1,
            finalized: Some(1),
            max_supported: 1,
            floor: 0,
        },
        cluster: FeatureClusterFacts {
            all_nodes_support: true,
            dependencies_met: true,
        },
        metadata_version: None,
    };

    /// `metadata.version` finalized at 25 with online downgrade from 18.
    const METADATA: FeatureUpdateFacts = FeatureUpdateFacts {
        update_type: Some(T::SafeDowngrade),
        levels: FeatureLevels {
            requested: 21,
            finalized: Some(25),
            max_supported: 27,
            floor: 7,
        },
        cluster: FeatureClusterFacts {
            all_nodes_support: true,
            dependencies_met: true,
        },
        metadata_version: Some(MV),
    };

    const MV: MetadataVersionFacts = MetadataVersionFacts {
        online_downgrade_min_level: 18,
        all_controllers_registered: true,
        all_nodes_downgrade_capable: true,
        cleanup_required: false,
    };

    const fn levels(requested: i16, finalized: Option<i16>) -> FeatureLevels {
        FeatureLevels {
            requested,
            finalized,
            ..GROUP.levels
        }
    }

    const fn row(update_type: T, requested: i16, finalized: Option<i16>) -> FeatureUpdateFacts {
        FeatureUpdateFacts {
            update_type: Some(update_type),
            levels: levels(requested, finalized),
            ..GROUP
        }
    }

    #[test]
    fn feature_rows_follow_update_feature_order() {
        let no_node = FeatureClusterFacts {
            all_nodes_support: false,
            ..GROUP.cluster
        };
        let no_dependency = FeatureClusterFacts {
            dependencies_met: false,
            ..GROUP.cluster
        };
        let cases = [
            ("upgrade to finalized", GROUP, D::EmitFeature),
            (
                "first finalization",
                row(T::Upgrade, 1, None),
                D::EmitFeature,
            ),
            (
                "safe downgrade",
                row(T::SafeDowngrade, 0, Some(1)),
                D::EmitFeature,
            ),
            // An unknown type fails before every other rule.
            (
                "unknown type",
                FeatureUpdateFacts {
                    update_type: None,
                    cluster: no_node,
                    ..row(T::Upgrade, -1, None)
                },
                D::UnknownUpdateType,
            ),
            (
                "negative level",
                row(T::Upgrade, -1, Some(1)),
                D::OutOfSupportedRange,
            ),
            (
                "above supported",
                row(T::Upgrade, 2, Some(1)),
                D::OutOfSupportedRange,
            ),
            // reasonNotSupported precedes the direction check.
            (
                "node lacks level",
                FeatureUpdateFacts {
                    cluster: no_node,
                    ..row(T::SafeDowngrade, 1, None)
                },
                D::UnsupportedByNode,
            ),
            (
                "downgrade as upgrade",
                row(T::Upgrade, 0, Some(1)),
                D::DowngradeWithoutFlag,
            ),
            (
                "downgrade raises",
                row(T::SafeDowngrade, 1, Some(0)),
                D::DowngradeToNewerLevel,
            ),
            // Kafka reads an unfinalized feature as level 0.
            (
                "downgrade raises unset",
                row(T::UnsafeDowngrade, 1, None),
                D::DowngradeToNewerLevel,
            ),
            (
                "below floor",
                FeatureUpdateFacts {
                    levels: FeatureLevels {
                        floor: 2,
                        max_supported: 3,
                        ..levels(1, Some(3))
                    },
                    update_type: Some(T::SafeDowngrade),
                    ..GROUP
                },
                D::BelowFloor,
            ),
            (
                "delete ignores floor",
                FeatureUpdateFacts {
                    levels: FeatureLevels {
                        floor: 2,
                        ..levels(0, Some(1))
                    },
                    update_type: Some(T::SafeDowngrade),
                    ..GROUP
                },
                D::EmitFeature,
            ),
            (
                "dependency unmet",
                FeatureUpdateFacts {
                    cluster: no_dependency,
                    ..GROUP
                },
                D::DependencyUnmet,
            ),
            (
                "delete unset",
                row(T::SafeDowngrade, 0, None),
                D::DeleteMissingFeature,
            ),
            (
                "delete level-0 as upgrade",
                row(T::Upgrade, 0, Some(0)),
                D::DeleteWithoutFlag,
            ),
        ];
        for (case, facts, expected) in cases {
            assert!(feature_update_decision(facts) == expected, "{case}");
        }
    }

    #[test]
    fn metadata_version_downgrade_gates_and_lossy_cleanup() {
        let with = |mv: MetadataVersionFacts, update_type| FeatureUpdateFacts {
            update_type: Some(update_type),
            metadata_version: Some(mv),
            ..METADATA
        };
        let lossy = MetadataVersionFacts {
            cleanup_required: true,
            ..MV
        };
        let cases = [
            ("safe online downgrade", METADATA, D::EmitFeature),
            (
                "before online downgrade",
                FeatureUpdateFacts {
                    levels: FeatureLevels {
                        requested: 17,
                        ..METADATA.levels
                    },
                    metadata_version: Some(MetadataVersionFacts {
                        all_controllers_registered: false,
                        ..MV
                    }),
                    ..METADATA
                },
                D::OnlineDowngradeUnsupported,
            ),
            (
                "unregistered controller",
                with(
                    MetadataVersionFacts {
                        all_controllers_registered: false,
                        all_nodes_downgrade_capable: false,
                        ..MV
                    },
                    T::SafeDowngrade,
                ),
                D::UnregisteredController,
            ),
            (
                "incapable node",
                with(
                    MetadataVersionFacts {
                        all_nodes_downgrade_capable: false,
                        ..MV
                    },
                    T::SafeDowngrade,
                ),
                D::NodeCannotDowngrade,
            ),
            // The gates apply only to a downgrade.
            (
                "upgrade skips gates",
                FeatureUpdateFacts {
                    update_type: Some(T::Upgrade),
                    levels: FeatureLevels {
                        requested: 26,
                        ..METADATA.levels
                    },
                    metadata_version: Some(MetadataVersionFacts {
                        all_controllers_registered: false,
                        all_nodes_downgrade_capable: false,
                        ..MV
                    }),
                    ..METADATA
                },
                D::EmitFeature,
            ),
            (
                "lossy safe downgrade",
                with(lossy, T::SafeDowngrade),
                D::LossyDowngradeNotUnsafe,
            ),
            (
                "lossy unsafe downgrade",
                with(lossy, T::UnsafeDowngrade),
                D::EmitCleanupThenFeature,
            ),
            (
                "floor wins over lossy",
                FeatureUpdateFacts {
                    levels: FeatureLevels {
                        floor: 22,
                        ..METADATA.levels
                    },
                    ..with(lossy, T::UnsafeDowngrade)
                },
                D::BelowFloor,
            ),
        ];
        for (case, facts, expected) in cases {
            assert!(feature_update_decision(facts) == expected, "{case}");
        }
    }
}
