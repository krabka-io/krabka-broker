//! The heartbeat-interval tick that expires members whose session timed out.
//! It is separate from the request path because it runs on a timer rather
//! than on a client request, and an eviction is the one membership change the
//! group makes on its own.

use std::time::Instant;

use super::{
    heartbeat::fence_member,
    records::{chrono_now_ms, flush_pending},
};
use crate::coordinator::unified::{
    GroupCoordinator,
    actor::MetadataProvider,
    offsets_log::OffsetsLog,
    share::{config::ShareGroupConfig, state::ShareGroupState},
};

/// Called on every heartbeat-interval tick. Each member whose session
/// expired is fenced on its own, with a batch of its own, as each of Kafka's
/// session timers runs `shareGroupFenceMember` for its member. Returns `Err`
/// if a log write fails, and the actor must then exit.
pub(super) async fn handle_session_tick(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let expired = state.expired_members(Instant::now(), config.session_timeout);
    for member_id in expired {
        let Some(pending) = fence_member(state, metadata, &member_id) else {
            return Err(crate::error::BrokerError::Share(
                "group epoch is exhausted".to_owned(),
            ));
        };
        if let Err(e) =
            flush_pending(state, pending, offsets_log, coordinator, chrono_now_ms()).await
        {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "share-group actor exiting after tick log-write failure",
            );
            return Err(e);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::check;

    use super::*;
    use crate::coordinator::unified::{
        config::NextGenConfig,
        offsets_log::fake::InMemoryOffsetsLog,
        share::{
            actor::{records::PendingShareRecords, test_support::metadata_with_topic},
            persistence::ShareGroupMetadataValue,
            state::ShareMemberState,
        },
    };

    /// Kafka runs a session timer per member, and each one that fires runs
    /// `shareGroupFenceMember` for its member alone: two members that expire
    /// together are fenced in two batches, each with the member's tombstones
    /// and an epoch bump, and no target is computed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_expired_member_is_fenced_in_a_batch_of_its_own() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let config = ShareGroupConfig {
            session_timeout: Duration::from_millis(1),
            ..ShareGroupConfig::default()
        };
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            config.clone(),
            metadata.clone(),
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let mut state = ShareGroupState::new("g");
        for member_id in ["m1", "m2"] {
            let mut m = ShareMemberState::joining(member_id, "c", "h", ["t".to_owned()].into());
            m.last_seen = Instant::now()
                .checked_sub(Duration::from_millis(50))
                .expect("50ms is always within Instant range");
            state.add_or_update_member(m);
        }
        state.group_epoch = 2;
        state.target.epoch = 2;

        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        // The first fence leaves the other member subscribed to `t`.
        let t_hash = metadata.snapshot().metadata_hash(["t"]);
        let fence = |member_id: &str, epoch, metadata_hash| {
            PendingShareRecords {
                member_metadata: vec![(member_id.into(), None)],
                target_per_member: vec![(member_id.into(), None)],
                current_per_member: vec![(member_id.into(), None)],
                group_metadata: Some(ShareGroupMetadataValue {
                    epoch,
                    metadata_hash,
                }),
                ..PendingShareRecords::default()
            }
            .into_batch("g", 0)
            .unwrap()
            .records
        };
        let written: Vec<_> = log
            .batches()
            .await
            .into_iter()
            .map(|batch| batch.records)
            .collect();
        let mut expected = vec![fence("m1", 3, t_hash), fence("m2", 4, 0)];
        if written.first().is_some_and(|batch| batch != &expected[0]) {
            // The members expire together: either may be fenced first.
            expected = vec![fence("m2", 3, t_hash), fence("m1", 4, 0)];
        }
        check!(written == expected);
        check!((state.members.len(), state.group_epoch, state.target.epoch) == (0, 4, 2));
    }
}
