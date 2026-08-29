//! KIP-584 `FeatureUpdate.UpgradeType` decoding.
//!
//! This module turns the request-version-dependent downgrade flags into a
//! single [`UpdateType`], so the validation path does not repeat the v0
//! `allow_downgrade` boolean against the v1+ `upgrade_type` wire code.

/// KIP-584 `FeatureUpdate.UpgradeType` wire code for a safe downgrade, which
/// loses nothing.
pub(super) const UPGRADE_TYPE_SAFE_DOWNGRADE: i8 = 2;

/// KIP-584 `FeatureUpdate.UpgradeType` wire code for an unsafe downgrade. The
/// caller accepts the loss of metadata written at the higher feature level.
pub(super) const UPGRADE_TYPE_UNSAFE_DOWNGRADE: i8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpdateType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

pub(super) fn update_type(
    version: i16,
    allow_downgrade: bool,
    upgrade_type: i8,
) -> Option<UpdateType> {
    if version == 0 {
        return Some(if allow_downgrade {
            UpdateType::SafeDowngrade
        } else {
            UpdateType::Upgrade
        });
    }
    match upgrade_type {
        1 => Some(UpdateType::Upgrade),
        UPGRADE_TYPE_SAFE_DOWNGRADE => Some(UpdateType::SafeDowngrade),
        UPGRADE_TYPE_UNSAFE_DOWNGRADE => Some(UpdateType::UnsafeDowngrade),
        _ => None,
    }
}

/// KIP-584 `FeatureUpdate.UpgradeType`: 1 is UPGRADE, 2 is `SAFE_DOWNGRADE`,
/// and 3 is `UNSAFE_DOWNGRADE`. Request v0 comes from before this field and
/// carries the boolean `allow_downgrade` flag instead.
#[cfg(test)]
fn downgrade_allowed(version: i16, allow_downgrade: bool, upgrade_type: i8) -> bool {
    update_type(version, allow_downgrade, upgrade_type)
        .is_some_and(|kind| kind != UpdateType::Upgrade)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn downgrade_flag_v0_uses_allow_downgrade() {
        assert!(downgrade_allowed(0, true, 1));
        assert!(!downgrade_allowed(0, false, 2));
    }

    #[test]
    fn downgrade_flag_v1_uses_upgrade_type() {
        // upgrade_type: 1 = UPGRADE, 2 = SAFE_DOWNGRADE, 3 = UNSAFE_DOWNGRADE.
        let cases = [
            // (allow_downgrade, upgrade_type, want); allow_downgrade is
            // ignored at v1+ — only upgrade_type decides.
            (true, 1, false),
            (false, 2, true),
            (false, 3, true),
        ];
        for (allow_downgrade, upgrade_type, want) in cases {
            assert!(
                downgrade_allowed(1, allow_downgrade, upgrade_type) == want,
                "allow_downgrade {allow_downgrade}, upgrade_type {upgrade_type}"
            );
        }
    }

    #[test]
    fn update_type_rejects_unknown_v1_value() {
        assert!(update_type(1, false, 0).is_none());
        assert!(update_type(1, false, 4).is_none());
    }
}
