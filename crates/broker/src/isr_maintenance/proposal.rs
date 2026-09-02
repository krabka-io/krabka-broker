//! Decides whether a leader partition's ISR should change. It holds the
//! `replica_state` lock once and returns everything the scan loop needs, so
//! the lag comparison is testable without the surrounding scan.

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use krabka_ids::LeaderEpoch;
use krabka_raft::NodeId;

use crate::partition::Partition;

/// A computed ISR change proposal. `compute_proposal` captures all fields
/// within its single `replica_state` lock scope, so the caller
/// can classify the shrink or expand and submit the proposal without a second
/// lock. That also removes the TOCTOU window in which the ISR could shift
/// between two locks.
#[derive(Debug, PartialEq)]
pub(super) struct Proposal {
    /// The pre-proposal ISR, sorted. The caller uses it to classify the
    /// shrink or expand metric.
    pub(super) prev_isr: Vec<NodeId>,
    /// The proposed new ISR, sorted. It is always `!= prev_isr`.
    pub(super) new_isr: Vec<NodeId>,
    /// Leader epoch to stamp on the `AlterPartition` request.
    pub(super) leader_epoch: LeaderEpoch,
}

/// Returns `Some(Proposal)` if the ISR should change, else `None`.
pub(super) async fn compute_proposal(part: &Partition, lag_max: Duration) -> Option<Proposal> {
    let st = part.replica_state.lock().await;
    let now = Instant::now();
    // Capture the pre-proposal ISR (sorted) once, inside this lock scope.
    let mut prev_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    prev_isr.sort_unstable();
    let (Some(leader), replicas) = st.leader_and_replicas() else {
        return None;
    };
    // A malformed metadata installation must never propose an ISR without its
    // assigned leader. Normal installations establish both facts together.
    if !replicas.contains(&leader) || !st.isr.contains(&leader) {
        return None;
    }

    // BTreeSet supplies one sorted decision per assigned/current replica. The
    // kernel then proves the exact keep/remove/add predicate for each member.
    let candidates: BTreeSet<NodeId> = replicas.union(&st.isr).copied().collect();
    let mut new_isr = Vec::with_capacity(candidates.len());
    for node in candidates {
        let stats = st.per_follower.get(&node);
        let fetch_recent =
            stats.is_some_and(|stats| now.saturating_duration_since(stats.last_fetch) <= lag_max);
        let caught_up_recent = stats
            .is_some_and(|stats| now.saturating_duration_since(stats.last_caught_up) <= lag_max);
        if krabka_verified::isr::isr_maintenance_selected((
            replicas.contains(&node),
            node == leader,
            st.isr.contains(&node),
            fetch_recent,
            caught_up_recent,
        )) {
            new_isr.push(node);
        }
    }
    let removed = prev_isr
        .iter()
        .filter(|node| new_isr.binary_search(node).is_err())
        .count();
    let added = new_isr
        .iter()
        .filter(|node| prev_isr.binary_search(node).is_err())
        .count();
    if krabka_verified::isr::isr_proposal_changed(removed, added) {
        Some(Proposal {
            prev_isr,
            new_isr,
            leader_epoch: st.current_leader_epoch,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::isr_maintenance::test_support::{fixture_partition, set_replica_state};

    #[tokio::test]
    async fn compute_proposal_shrinks_lagging_isr_member() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            7,
            &[
                (NodeId(2), Duration::from_secs(1), Duration::from_secs(1)),
                (NodeId(3), Duration::from_secs(30), Duration::from_secs(30)),
            ],
        )
        .await;

        let proposal = compute_proposal(&part, Duration::from_secs(5))
            .await
            .expect("lagging ISR member should produce a shrink proposal");

        let expected = Proposal {
            prev_isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            new_isr: vec![NodeId(1), NodeId(2)],
            leader_epoch: LeaderEpoch(7),
        };
        assert2::assert!((proposal) == (expected));
    }

    #[tokio::test]
    async fn compute_proposal_expands_recently_caught_up_replica() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            8,
            &[
                (NodeId(2), Duration::from_secs(1), Duration::from_secs(1)),
                (NodeId(3), Duration::from_secs(1), Duration::from_secs(1)),
            ],
        )
        .await;

        let proposal = compute_proposal(&part, Duration::from_secs(5))
            .await
            .expect("caught-up replica should produce an expand proposal");

        let expected = Proposal {
            prev_isr: vec![NodeId(1), NodeId(2)],
            new_isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(8),
        };
        assert2::assert!((proposal) == (expected));
    }

    #[tokio::test]
    async fn compute_proposal_ignores_stale_non_isr_replica() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            9,
            &[
                (NodeId(2), Duration::from_secs(1), Duration::from_secs(1)),
                (NodeId(3), Duration::from_secs(1), Duration::from_secs(30)),
            ],
        )
        .await;

        assert2::assert!(
            compute_proposal(&part, Duration::from_secs(5))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn leader_lag_and_duplicate_assignment_do_not_change_the_isr() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[NodeId(1), NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(1), NodeId(2), NodeId(2)],
            NodeId(1),
            10,
            &[
                (NodeId(1), Duration::from_secs(30), Duration::from_secs(30)),
                (NodeId(2), Duration::from_secs(1), Duration::from_secs(1)),
            ],
        )
        .await;

        assert2::assert!(
            compute_proposal(&part, Duration::from_secs(5))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn caught_up_but_no_longer_live_replica_is_not_admitted() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(log_dir.path(), "t", 0);
        set_replica_state(
            &part,
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            11,
            &[
                (NodeId(2), Duration::from_secs(1), Duration::from_secs(1)),
                (NodeId(3), Duration::from_secs(30), Duration::from_secs(1)),
            ],
        )
        .await;

        assert2::assert!(
            compute_proposal(&part, Duration::from_secs(5))
                .await
                .is_none()
        );
    }
}
