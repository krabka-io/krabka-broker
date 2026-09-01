//! The KIP-848 downgrade trigger.
//!
//! A consumer group that has lost its last native member but still hosts
//! classic members flips back to the classic protocol in place. The flip is
//! one atomic batch, so it lives on its own rather than inside the membership
//! paths that call it.

use super::{MetadataProvider, chrono_now_ms};
use crate::coordinator::unified::{
    GroupCoordinator,
    config::NextGenConfig,
    group::{CoordinatorGroup, GroupKind},
    migration,
    offsets_log::OffsetsLog,
};

#[cfg(test)]
mod tests;

/// KIP-848 DOWNGRADE trigger. After a membership change on a consumer-kind
/// group, flip it back to classic in place when no NATIVE consumer member
/// remains, there ARE hosted classic members, and policy allows it. The flip
/// is one atomic batch: tombstone the next-gen k3 + k6 (both group-level) +
/// every member's k5/k7/k8, and write the classic k2 `GroupMetadata`. Returns `Ok(true)` if a flip
/// happened, `Ok(false)` if the conditions weren't met, `Err` on a log-write
/// failure (the caller exits the actor loop).
// Matches Kafka's `validateOnlineDowngradeWithFencedMembers`: downgrade only
// when the remaining group is nonempty, every remaining member uses the
// classic protocol, and the migration policy permits downgrade.
pub(super) async fn maybe_downgrade(
    group: &mut CoordinatorGroup,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<bool, crate::error::BrokerError> {
    let Some(state) = group.as_consumer() else {
        return Ok(false);
    };
    if !config.migration_policy.allows_downgrade() {
        return Ok(false);
    }
    if state.members.is_empty() {
        // Fully empty: normal cleanup (a tombstoned next-gen group), not a
        // downgrade — there are no hosted classic members to re-express.
        return Ok(false);
    }
    if !migration::consumer_is_convertible(state) {
        // A native consumer member is still present: the group stays next-gen.
        return Ok(false);
    }

    let image = metadata.snapshot();

    // The leave or expiration path already reconciled the surviving members,
    // so the target covers the departed native member's partitions.
    let state = group.as_consumer().expect("consumer-kind verified above");
    let classic = migration::convert_consumer_to_classic(state, &image);
    let pending = migration::downgrade_pending_records(state, &classic);
    let group_id = group.group_id.clone();
    let batch = pending.to_batch(&group_id, chrono_now_ms());
    offsets_log.append(&group_id, batch).await?;
    coordinator.mark_classic_after_downgrade(&group_id);
    *group.kind_mut() = GroupKind::Classic(classic);
    Ok(true)
}
