//! Decides whether a leader partition's ISR should change. It holds the
//! `replica_state` lock once and returns everything the scan loop needs, so
//! the lag comparison is testable without the surrounding scan.

use std::time::{Duration, Instant};

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
    let mut new_isr: Vec<NodeId> = prev_isr.clone();
    // Shrink: drop followers lagging > lag_max.
    new_isr.retain(|n| {
        st.per_follower
            .get(n)
            .is_none_or(|stats| now.duration_since(stats.last_fetch) <= lag_max)
    });
    // Expand: add followers in per_follower not in current ISR that have
    // been recently caught up.
    for (n, stats) in &st.per_follower {
        if !st.isr.contains(n)
            && now.duration_since(stats.last_caught_up) <= lag_max
            && !new_isr.contains(n)
        {
            new_isr.push(*n);
        }
    }
    new_isr.sort_unstable();
    let no_change = new_isr == prev_isr;
    if no_change {
        None
    } else {
        Some(Proposal {
            prev_isr,
            new_isr,
            leader_epoch: st.current_leader_epoch,
        })
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
}
