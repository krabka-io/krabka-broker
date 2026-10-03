use super::*;

pub(super) fn in_flight(s: &ReassignState) -> bool {
    !s.adding.is_empty() || !s.removing.is_empty()
}

/// The target replica set that the reassignment converges to, which is
/// replicas − removing.
pub(super) fn target_of(s: &ReassignState) -> Vec<NodeId> {
    s.replicas
        .iter()
        .filter(|r| !s.removing.contains(r))
        .copied()
        .collect()
}

/// Build a `PartitionRecord` from the model state to drive the real
/// `reassign_one`. `directories` does not affect the safety properties.
pub(super) fn pr_of(s: &ReassignState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: krabka_metadata::LeaderEpoch(s.leader_epoch),
        adding_replicas: s.adding.clone(),
        removing_replicas: s.removing.clone(),
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `reassign_one` decision against the pre-state. These are the
/// safety-critical invariants, and they hold per decision under any ordering.
pub(super) fn assert_step(pre: &ReassignState, next: &PartitionRecord) {
    assert2::assert!(
        next.leader_epoch >= pre.leader_epoch,
        "leader_epoch regressed: {} -> {}",
        pre.leader_epoch,
        next.leader_epoch
    );
    assert2::assert!(
        pre.adding.iter().all(|n| pre.isr.contains(n)),
        "decision emitted before adding caught up: adding={:?} isr={:?}",
        pre.adding,
        pre.isr
    );
    let target = target_of(pre);
    // Kafka's `maybeCompleteReassignment`: a replication factor decrease waits
    // until every target replica is in the ISR, so neither the completion nor
    // the handoff that precedes it ever shrinks the ISR.
    if pre.adding.len() < pre.removing.len() {
        assert2::assert!(
            target.iter().all(|n| pre.isr.contains(n)),
            "replication factor decrease advanced with target replicas outside the ISR: \
             target={target:?} isr={:?}",
            pre.isr
        );
    }
    if next.leader != pre.leader {
        // Handoff.
        assert2::assert!(
            pre.isr.contains(&next.leader),
            "handoff to non-ISR {}",
            next.leader
        );
        assert2::assert!(
            target.contains(&next.leader),
            "handoff to non-target {}",
            next.leader
        );
        assert2::assert!(
            pre.alive.contains(&next.leader),
            "handoff to dead {}",
            next.leader
        );
        assert2::assert!(
            !pre.removing.contains(&next.leader),
            "handoff to a removing replica {}",
            next.leader
        );
        assert2::assert!(
            next.replicas == pre.replicas,
            "handoff changed the replica set"
        );
        assert2::assert!(next.adding_replicas == pre.adding, "handoff changed adding");
        assert2::assert!(
            next.removing_replicas == pre.removing,
            "handoff changed removing"
        );
        assert2::assert!(
            next.leader_epoch == pre.leader_epoch + 1,
            "handoff did not bump leader_epoch by exactly 1"
        );
    } else if next.adding_replicas.is_empty() && next.removing_replicas.is_empty() {
        // Completion.
        assert2::assert!(
            next.replicas.contains(&next.leader),
            "completion switched the replica set off the leader {}: replicas={:?}",
            next.leader,
            next.replicas
        );
        assert2::assert!(
            next.replicas == target,
            "completion replicas != target: {:?} vs {:?}",
            next.replicas,
            target
        );
        assert2::assert!(
            next.isr.iter().all(|n| next.replicas.contains(n)),
            "completion ISR not a subset of replicas"
        );
        assert2::assert!(
            next.leader_epoch == pre.leader_epoch,
            "completion bumped leader_epoch"
        );
    } else {
        panic!("unexpected reassign_one decision shape: {next:?} from {pre:?}");
    }
}
