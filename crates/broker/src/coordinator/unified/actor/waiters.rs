//! Resolution of the parked classic `JoinGroup` and `SyncGroup` waiters.
//!
//! The classic protocol blocks a client inside the RPC until the rebalance
//! boundary, so the actor parks the reply `oneshot::Sender` and hands it to one
//! of these functions when the boundary arrives: the rebalance deadline, an
//! early completion, a member removal, or the leader's `SyncGroup`.

use std::collections::HashMap;

use tokio::sync::oneshot;

use super::{JoinResult, SyncResult};
use crate::{
    codes,
    coordinator::unified::{
        classic_ops,
        classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
    },
};

/// Runs the rebalance vote and resolves every parked joiner. It mirrors
/// `join_group.rs` block 5 and `notify_waiters()`.
///
/// It also drains any stale parked follower with `REBALANCE_IN_PROGRESS`. Such
/// a follower belongs to a previous `CompletingRebalance` whose leader was
/// dead and never sent `SyncGroup`. The notification lets the client rejoin at
/// once instead of waiting for the 30-second request timeout.
pub(super) fn complete_classic_rebalance(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for (_, sender) in followers.drain() {
        let _ = sender.send(SyncResult {
            error_code: codes::REBALANCE_IN_PROGRESS,
            ..SyncResult::default()
        });
    }
    let inconsistent = classic_ops::try_complete(state).is_err();
    if inconsistent {
        state.rebalance_deadline = None;
        state.joined_this_round.clear();
    }
    for (member_id, sender) in joiners.drain() {
        let result = if inconsistent {
            JoinResult {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                member_id: member_id.clone(),
                protocol_type: state.protocol_type.clone(),
                ..JoinResult::default()
            }
        } else {
            classic_ops::build_join_result(state, &member_id)
        };
        let _ = sender.send(result);
    }
}

pub(super) fn drain_removed_classic_waiters(
    removed: &[String],
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for member_id in removed {
        if let Some(sender) = joiners.remove(member_id) {
            let _ = sender.send(JoinResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                member_id: member_id.clone(),
                ..JoinResult::default()
            });
        }
        if let Some(sender) = followers.remove(member_id) {
            let _ = sender.send(SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            });
        }
    }
}

/// Completes the rebalance early if and only if every still-live member has
/// joined this round and the group has rebalanced before. This mirrors
/// `wake_other_joiners`.
pub(super) fn maybe_complete_classic(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let should = state.generation_id > 0
        && matches!(state.state, ClassicGroupState::PreparingRebalance)
        && state.all_members_joined_this_round();
    if should {
        complete_classic_rebalance(state, joiners, followers);
    }
}

/// Delivers to each parked follower its installed assignment, after the leader
/// sync.
pub(super) fn drain_parked_followers(
    state: &ClassicState,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let protocol_type = state.protocol_type.clone();
    let protocol_name = state.protocol_name.clone();
    for (member_id, sender) in followers.drain() {
        let result = classic_ops::read_sync_result(
            state,
            &member_id,
            protocol_type.clone(),
            protocol_name.clone(),
        );
        let _ = sender.send(result);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;

    #[test]
    fn inconsistent_classic_completion_clears_deadline() {
        use crate::coordinator::unified::classic_state::Member;

        let mut state = ClassicState::new("g");
        state.protocol_type = Some("consumer".into());
        state.add_member(Member::new(
            "m1",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::new())],
        ));
        state.add_member(Member::new(
            "m2",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("cooperative-sticky".into(), Bytes::new())],
        ));
        state.rebalance_deadline = Some(
            Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        );
        assert!(state.state == ClassicGroupState::PreparingRebalance);

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        let mut joiners = HashMap::from([("m1".to_string(), tx1), ("m2".to_string(), tx2)]);
        let mut followers = HashMap::new();

        complete_classic_rebalance(&mut state, &mut joiners, &mut followers);

        assert!(
            state.rebalance_deadline.is_none(),
            "a failed protocol vote must not leave an already-fired deadline armed"
        );
    }

    #[tokio::test]
    async fn removed_classic_members_drain_parked_waiters() {
        let (join_tx, join_rx) = tokio::sync::oneshot::channel();
        let (sync_tx, sync_rx) = tokio::sync::oneshot::channel();
        let mut joiners = HashMap::from([("m1".to_string(), join_tx)]);
        let mut followers = HashMap::from([("m1".to_string(), sync_tx)]);

        drain_removed_classic_waiters(&["m1".to_string()], &mut joiners, &mut followers);

        check!(joiners.is_empty());
        check!(followers.is_empty());
        check!(join_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
        check!(sync_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
    }
}
