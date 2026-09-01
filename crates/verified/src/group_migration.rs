//! Classic and consumer group migration admission and durable record plans.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum GroupMigrationDirection {
    Upgrade,
    Downgrade,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum GroupMigrationRecordAction {
    Write,
    Tombstone,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct GroupMigrationRecordPlan {
    pub classic_group: GroupMigrationRecordAction,
    pub next_gen_group: GroupMigrationRecordAction,
    pub next_gen_target: GroupMigrationRecordAction,
    pub member_metadata: GroupMigrationRecordAction,
    pub target_member: GroupMigrationRecordAction,
    pub current_member: GroupMigrationRecordAction,
    pub member_count: usize,
}

/// Admit a classic-to-consumer upgrade exactly when the protocol and every
/// member subscription have a consumer representation. The returned epoch is
/// the classic generation clamped to the first valid consumer epoch.
#[ensures((result != None) == (consumer_protocol && every_subscription_decodable))]
#[ensures(forall<epoch: i32> result == Some(epoch) ==> epoch@ >= 0)]
#[ensures(forall<epoch: i32> result == Some(epoch) ==>
    (generation@ >= 0 && epoch@ == generation@)
        || (generation@ < 0 && epoch@ == 0))]
#[must_use]
pub fn classic_upgrade_epoch(
    consumer_protocol: bool,
    every_subscription_decodable: bool,
    generation: i32,
) -> Option<i32> {
    if !consumer_protocol || !every_subscription_decodable {
        return None;
    }
    Some(if generation < 0 { 0 } else { generation })
}

/// Admit a consumer-to-classic downgrade exactly when every member carries a
/// classic facade. The returned classic generation is nonnegative and keeps
/// every already-valid consumer epoch unchanged.
#[ensures((result != None) == every_member_hosted_classic)]
#[ensures(forall<epoch: i32> result == Some(epoch) ==> epoch@ >= 0)]
#[ensures(forall<epoch: i32> result == Some(epoch) ==>
    (group_epoch@ >= 0 && epoch@ == group_epoch@)
        || (group_epoch@ < 0 && epoch@ == 0))]
#[must_use]
pub fn consumer_downgrade_epoch(
    every_member_hosted_classic: bool,
    group_epoch: i32,
) -> Option<i32> {
    if !every_member_hosted_classic {
        return None;
    }
    Some(if group_epoch < 0 { 0 } else { group_epoch })
}

/// Select the exact keyed-record actions for one atomic migration batch.
///
/// An upgrade tombstones the classic group and writes the full next-gen group.
/// A downgrade does the inverse. Both directions apply the same action to all
/// three per-member next-gen record families.
#[ensures(result.member_count@ == member_count@)]
#[ensures(result.member_metadata == result.target_member)]
#[ensures(result.target_member == result.current_member)]
#[ensures(match direction {
    GroupMigrationDirection::Upgrade =>
        result.classic_group == GroupMigrationRecordAction::Tombstone
            && result.next_gen_group == GroupMigrationRecordAction::Write
            && result.next_gen_target == GroupMigrationRecordAction::Write
            && result.member_metadata == GroupMigrationRecordAction::Write,
    GroupMigrationDirection::Downgrade =>
        result.classic_group == GroupMigrationRecordAction::Write
            && result.next_gen_group == GroupMigrationRecordAction::Tombstone
            && result.next_gen_target == GroupMigrationRecordAction::Tombstone
            && result.member_metadata == GroupMigrationRecordAction::Tombstone,
})]
#[must_use]
pub fn group_migration_record_plan(
    direction: GroupMigrationDirection,
    member_count: usize,
) -> GroupMigrationRecordPlan {
    match direction {
        GroupMigrationDirection::Upgrade => GroupMigrationRecordPlan {
            classic_group: GroupMigrationRecordAction::Tombstone,
            next_gen_group: GroupMigrationRecordAction::Write,
            next_gen_target: GroupMigrationRecordAction::Write,
            member_metadata: GroupMigrationRecordAction::Write,
            target_member: GroupMigrationRecordAction::Write,
            current_member: GroupMigrationRecordAction::Write,
            member_count,
        },
        GroupMigrationDirection::Downgrade => GroupMigrationRecordPlan {
            classic_group: GroupMigrationRecordAction::Write,
            next_gen_group: GroupMigrationRecordAction::Tombstone,
            next_gen_target: GroupMigrationRecordAction::Tombstone,
            member_metadata: GroupMigrationRecordAction::Tombstone,
            target_member: GroupMigrationRecordAction::Tombstone,
            current_member: GroupMigrationRecordAction::Tombstone,
            member_count,
        },
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{
        GroupMigrationDirection, GroupMigrationRecordAction, classic_upgrade_epoch,
        consumer_downgrade_epoch, group_migration_record_plan,
    };

    #[test]
    fn representability_is_exact_and_epochs_do_not_overflow() {
        check!(classic_upgrade_epoch(true, true, -1) == Some(0));
        check!(classic_upgrade_epoch(true, true, i32::MAX) == Some(i32::MAX));
        check!(classic_upgrade_epoch(false, true, 3).is_none());
        check!(classic_upgrade_epoch(true, false, 3).is_none());

        check!(consumer_downgrade_epoch(true, -1) == Some(0));
        check!(consumer_downgrade_epoch(true, i32::MAX) == Some(i32::MAX));
        assert!(consumer_downgrade_epoch(false, 3).is_none());
    }

    #[test]
    fn record_plans_flip_every_key_family() {
        use GroupMigrationRecordAction::{Tombstone, Write};

        let upgrade = group_migration_record_plan(GroupMigrationDirection::Upgrade, 2);
        check!(upgrade.classic_group == Tombstone);
        check!(upgrade.next_gen_group == Write);
        check!(upgrade.next_gen_target == Write);
        check!(upgrade.member_metadata == Write);
        check!(upgrade.target_member == Write);
        check!(upgrade.current_member == Write);
        check!(upgrade.member_count == 2);

        let downgrade = group_migration_record_plan(GroupMigrationDirection::Downgrade, usize::MAX);
        check!(downgrade.classic_group == Write);
        check!(downgrade.next_gen_group == Tombstone);
        check!(downgrade.next_gen_target == Tombstone);
        check!(downgrade.member_metadata == Tombstone);
        check!(downgrade.target_member == Tombstone);
        check!(downgrade.current_member == Tombstone);
        assert!(downgrade.member_count == usize::MAX);
    }
}
