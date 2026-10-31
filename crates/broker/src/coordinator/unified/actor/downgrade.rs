//! The KIP-848 downgrade trigger.
//!
//! A consumer group whose last native member is fenced, while it still hosts
//! classic members, flips back to the classic protocol in place, as Kafka's
//! `consumerGroupFenceMembers` does: the flip replaces the fence's records
//! with one atomic batch of the conversion's.

use {
    super::{ActorServices, chrono_now_ms, heartbeat::fence_members},
    crate::{
        coordinator::unified::{
            config::NextGenConfig,
            consumer_state::GroupState,
            group::{CoordinatorGroup, GroupKind},
            migration,
        },
        error::BrokerError,
    },
    std::time::Instant,
};

#[cfg(test)]
mod tests;

/// Kafka's `validateOnlineDowngradeWithFencedMembers`: a consumer group whose
/// members are about to be fenced downgrades to a classic group instead when
/// every other member uses the classic protocol, at least one remains, the
/// migration policy allows a downgrade and the remaining members fit in a
/// classic group.
pub(super) fn downgrades_without(
    state: &GroupState,
    config: &NextGenConfig,
    fenced: &[String],
) -> bool {
    let mut remaining = state
        .members
        .values()
        .filter(|member| !fenced.contains(&member.member_id))
        .peekable();
    let nonempty = remaining.peek().is_some();
    let all_classic = remaining.all(|member| member.classic.is_some());
    let remaining_count = state.members.len()
        - fenced
            .iter()
            .filter(|member_id| state.members.contains_key(*member_id))
            .count();
    all_classic
        && nonempty
        && config.migration_policy.allows_downgrade()
        && remaining_count <= config.classic_max_size
}

/// Kafka's `convertToClassicGroup` for a fence: the consumer group, fenced
/// members included, is tombstoned and the classic group of the remaining
/// members is written, in one batch. The classic group starts at the consumer
/// group's epoch and, as Kafka's fence asks with `rebalance = true`, prepares
/// a rebalance at once.
///
/// It returns `Err` on a log-write failure, and the actor then exits.
pub(super) async fn downgrade_fencing(
    group: &mut CoordinatorGroup,
    fenced: &[String],
    services: ActorServices<'_>,
) -> Result<(), BrokerError> {
    let Some(state) = group.as_consumer() else {
        return Ok(());
    };
    let image = services.metadata.snapshot();
    let now_ms = chrono_now_ms();
    let Some(mut classic) = migration::convert_consumer_to_classic(state, fenced, &image) else {
        return Ok(());
    };
    let pending = migration::downgrade_pending_records(state, &classic, now_ms);
    let group_id = group.group_id.clone();
    let batch = pending.to_batch(&group_id, now_ms)?;
    services.offsets_log.append(&group_id, batch).await?;
    services.coordinator.mark_classic_after_downgrade(&group_id);
    classic.prepare_rebalance(
        services.config.classic_initial_rebalance_delay,
        Instant::now(),
    );
    *group.kind_mut() = GroupKind::Classic(classic);
    Ok(())
}

/// Which way [`fence_or_downgrade`] went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FenceOutcome {
    /// The group downgraded to a classic group in place of the fence.
    Downgraded,
    /// The members were fenced from the consumer group.
    Fenced,
}

/// The log write that failed in [`fence_or_downgrade`].
#[derive(Debug)]
pub(super) enum FenceFailure {
    Downgrade(BrokerError),
    Fence(BrokerError),
}

impl FenceFailure {
    pub(super) fn into_error(self) -> BrokerError {
        match self {
            Self::Downgrade(error) | Self::Fence(error) => error,
        }
    }
}

/// Kafka's `consumerGroupFenceMembers`: `member_ids` leave the consumer group
/// in one batch, or the group downgrades in their place when only classic
/// members would remain. A group that is not a consumer group is left as it
/// is.
///
/// It returns `Err` on a log-write failure, and the actor then exits.
pub(super) async fn fence_or_downgrade(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    member_ids: &[String],
) -> Result<FenceOutcome, FenceFailure> {
    if group
        .as_consumer()
        .is_some_and(|state| downgrades_without(state, services.config, member_ids))
    {
        downgrade_fencing(group, member_ids, services)
            .await
            .map_err(FenceFailure::Downgrade)?;
        return Ok(FenceOutcome::Downgraded);
    }
    if let Some(state) = group.as_consumer_mut() {
        let pending = fence_members(state, services.metadata, member_ids);
        services
            .flush(state, pending, chrono_now_ms())
            .await
            .map_err(FenceFailure::Fence)?;
    }
    Ok(FenceOutcome::Fenced)
}
