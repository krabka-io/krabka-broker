use assert2::assert;

use super::*;

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
