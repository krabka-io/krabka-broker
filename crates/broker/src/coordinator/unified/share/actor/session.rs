//! The heartbeat-interval tick that expires members whose session timed out.
//! It is separate from the request path because it runs on a timer rather
//! than on a client request, and an eviction is the one membership change the
//! group makes on its own.

use std::time::Instant;

use super::{
    assignment::reconcile,
    records::{PendingShareRecords, chrono_now_ms, flush_pending},
    share_state::reconcile_share_state,
};
use crate::coordinator::unified::{
    GroupCoordinator,
    actor::MetadataProvider,
    offsets_log::OffsetsLog,
    share::{
        config::ShareGroupConfig,
        persistence::{ShareGroupMetadataValue, ShareGroupTargetAssignmentMetadataValue},
        state::ShareGroupState,
    },
};

/// Called on every heartbeat-interval tick. It evicts expired members and
/// writes the resulting tombstones to `__consumer_offsets`. Returns `Err` if
/// the log write fails, and the actor must then exit.
pub(super) async fn handle_session_tick(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = state.evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` set `dirty`; the reconcile owns the single `bump_epoch`.
    if !reconcile(state, metadata) {
        return Err(crate::error::BrokerError::Share(
            "group epoch is exhausted".to_owned(),
        ));
    }
    let mut pending = PendingShareRecords {
        group_metadata: Some(ShareGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        tracing::warn!(
            group_id = %state.group_id,
            error = %e,
            "share-group actor exiting after tick log-write failure",
        );
        return Err(e);
    }
    // Eviction may drop subscriptions owned by remaining members. Preserve
    // state when the last member leaves so the durable queue cursor survives.
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;

    use super::*;
    use crate::coordinator::unified::{
        config::NextGenConfig,
        offsets_log::fake::InMemoryOffsetsLog,
        share::actor::{heartbeat::build_member, test_support::metadata_with_topic},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_tick_evicts() {
        use std::time::Duration;
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
        let mut m = build_member(
            "m1",
            &ShareGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "/127.0.0.1",
            },
            Instant::now(),
        );
        m.last_seen = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .expect("50ms is always within Instant range");
        state.add_or_update_member(m);
        reconcile(&mut state, &*metadata);
        assert!(!state.dirty, "baseline must be clean before eviction");
        let epoch_before = state.group_epoch;

        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        assert!(state.members.is_empty(), "expired member evicted");
        assert!(
            state.group_epoch == epoch_before + 1,
            "single eviction advances epoch by exactly 1"
        );
    }
}
