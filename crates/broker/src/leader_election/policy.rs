//! The pure failover policy. [`failover_one`] answers what to do with one
//! partition when a replica of it is gone, and [`FailoverPlan`] is the shape
//! the two controller scans build out of those answers. Nothing here does
//! I/O, so the policy is unit-testable and model-checkable on its own.

use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use krabka_verified::consensus::{FailoverAction, FailoverRecovery, failover_action};

use crate::config_keys::RecoveryStrategy;

/// Output of a failover scan: immediate metadata changes plus partitions
/// that need asynchronous offset-aware recovery through the URM.
#[derive(Default)]
pub(crate) struct FailoverPlan {
    pub changes: Vec<MetadataRecord>,
    pub recoveries: Vec<(String, i32, RecoveryStrategy)>,
    /// Partitions the dead broker leads that have no live ISR replica to
    /// elect. The scan leaves them alone. The caller decides how loudly to
    /// report them: the death edge warns once, the per-tick sweep does not
    /// repeat that warning every second.
    pub unavailable: Vec<(String, i32)>,
}

/// The pure per-partition failover decision shared by the dead-broker scan
/// (`compute_failover_changes`) and the offline-log-dir scan
/// (`compute_offline_dir_failover_changes`). No I/O: the callers handle
/// partition filtering, the alive snapshot, record construction, metrics, and
/// recovery enqueue. This enum is separate so the failover policy is
/// independently unit-testable and model-checkable, and so the two scans share
/// one copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailoverDecision {
    /// Elect `leader` with `isr`. The caller bumps `leader_epoch + 1` and, when
    /// `unclean`, records the unclean-election metric.
    Elect {
        leader: NodeId,
        isr: Vec<NodeId>,
        unclean: bool,
    },
    /// Defer to the offset-aware Unclean Recovery Manager (KIP-966).
    Recover(RecoveryStrategy),
    /// Dead broker was a non-leader ISR member: shrink ISR (leader/epoch kept).
    ShrinkIsr { isr: Vec<NodeId> },
    /// Leader is dead, ISR empty, and no unclean path is permitted/available.
    Unavailable,
    /// Nothing to do for this partition.
    NoChange,
}

/// Decide the failover action for one partition. `alive` is the controller's
/// snapshot of live brokers. `witnesses` is the set of data-bearing witness
/// nodes, which replicate the partition and count toward
/// `min.insync.replicas` but never lead it. `strategy` and `unclean_enabled`
/// are the topic's resolved recovery policy.
///
/// A witness stays in the emitted ISR. It holds every committed record, so it
/// is what keeps `acks=all` writable after a site loss. Only the leader pick
/// excludes it.
pub(crate) fn failover_one(
    pr: &PartitionRecord,
    dead: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    // The ISR after dropping the dead broker AND any other non-alive member.
    // Witness members stay: they carry the data and the min-ISR count.
    let alive_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| **n != dead && alive.contains(n))
        .copied()
        .collect();
    // The new leader is the first alive ISR member that can serve clients.
    let electable = alive_isr.iter().copied().find(|n| !witnesses.contains(n));
    let unclean_candidate = pr
        .replicas
        .iter()
        .find(|n| **n != dead && alive.contains(n) && !witnesses.contains(n))
        .copied();
    let recovery = match strategy {
        RecoveryStrategy::None => FailoverRecovery::None,
        RecoveryStrategy::Balanced => FailoverRecovery::Balanced,
        RecoveryStrategy::Aggressive => FailoverRecovery::Aggressive,
    };
    match failover_action(
        pr.leader == dead,
        electable.is_some(),
        alive_isr.is_empty(),
        recovery,
        unclean_enabled,
        unclean_candidate.is_some(),
        alive_isr.len() < pr.isr.len(),
    ) {
        FailoverAction::ElectClean => {
            let new_leader = electable.expect("verified clean election has a candidate");
            // Clean: the new leader was in the ISR, so it holds every committed
            // record. No data loss.
            FailoverDecision::Elect {
                leader: new_leader,
                isr: alive_isr,
                unclean: false,
            }
        }
        FailoverAction::Recover(_) => FailoverDecision::Recover(strategy),
        FailoverAction::ElectUnclean => {
            // KIP-841: ISR is dead but the operator opted into possible data
            // loss. Elect the first alive non-witness replica, singleton ISR.
            let new_leader = unclean_candidate.expect("verified unclean election has a candidate");
            FailoverDecision::Elect {
                leader: new_leader,
                isr: vec![new_leader],
                unclean: true,
            }
        }
        FailoverAction::Unavailable => {
            // Every alive ISR member is a witness. The partition is
            // unavailable, and that is the safe answer.
            //
            // A live witness is a full ISR member, so it holds every committed
            // record. An unclean election, or an offset-aware recovery that
            // excludes the witness, would move leadership to a data replica
            // that is behind the witness and would discard those records.
            // Loss of availability is recoverable. Loss of an acknowledged
            // write is not. The partition comes back as soon as one data
            // replica returns, and an operator who prefers availability can
            // still force an unclean election with `kafka-leader-election`.
            FailoverDecision::Unavailable
        }
        FailoverAction::ShrinkIsr => FailoverDecision::ShrinkIsr { isr: alive_isr },
        FailoverAction::NoChange => FailoverDecision::NoChange,
    }
}

/// Decide what one partition needs when `returning` re-registers under a new
/// incarnation id, which is a broker that stopped and came back without the
/// controller being able to prove the stop was clean.
///
/// Apache Kafka answers the same event in
/// `ReplicationControlManager.handleBrokerShutdown`, whose unclean branch --
/// read out of `kafka-metadata-4.3.1.jar` -- runs
/// `generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1,
/// brokerId, records, brokersToIsrs.partitionsWithBrokerInIsr(brokerId))`.
/// That call sets `targetIsr` to `Replicas.copyWithout(partition.isr, {-1,
/// brokerId})`, so it removes exactly the returning broker and leaves every
/// other ISR member alone, however the controller currently rates its
/// liveness. This does the same, which is why it is not
/// [`failover_one`]: a returning broker is one event about one broker, not a
/// reason to re-decide the whole ISR against a liveness registry that a
/// controller which has just been elected may not have populated yet.
///
/// The one case that does need the full policy is the partition the returning
/// broker is still recorded as leading. A bare ISR rewrite there would leave a
/// leader that is not in its own ISR, so that case is handed to
/// [`failover_one`] unchanged: the broker is dead as far as liveness is
/// concerned -- that is the precondition the registration was accepted under
/// -- so it is the same question the dead-broker scan asks, and it deserves
/// the same answer, up to and including an offset-aware recovery.
pub(crate) fn unclean_restart_one(
    pr: &PartitionRecord,
    returning: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    if pr.leader == returning {
        return failover_one(pr, returning, alive, witnesses, strategy, unclean_enabled);
    }
    if !pr.isr.contains(&returning) {
        return FailoverDecision::NoChange;
    }
    FailoverDecision::ShrinkIsr {
        isr: pr.isr.iter().copied().filter(|n| *n != returning).collect(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::LeaderEpoch;

    use super::*;
    use crate::leader_election::test_support::witnesses;

    /// The full failover decision for one partition, with `witnesses` given
    /// directly. This keeps the witness tests on the pure policy function.
    fn decide(
        pr: &PartitionRecord,
        dead: u64,
        alive: &[u64],
        witness_ids: &[u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
    ) -> super::FailoverDecision {
        let alive: std::collections::HashSet<NodeId> = alive.iter().copied().map(NodeId).collect();
        failover_one(
            pr,
            NodeId(dead),
            &alive,
            &witnesses(witness_ids),
            strategy,
            unclean_enabled,
        )
    }

    fn partition_record(leader: u64, replicas: &[u64], isr: &[u64]) -> PartitionRecord {
        PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }
    }

    #[test]
    fn clean_failover_skips_a_witness_that_sorts_first_in_the_isr() {
        // Leader 1 dies. The ISR order is [1, 2, 3] and broker 2 is the
        // witness, so the pre-witness code would have elected 2. The data
        // replica behind it, broker 3, must take leadership instead. The
        // whole decision is compared, so the emitted ISR is pinned too: it
        // still carries the witness, which is what keeps `acks=all` writable.
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2, 3],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            false,
        );
        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(3),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                }
        );
    }

    #[test]
    fn only_witnesses_alive_is_unavailable_whatever_the_unclean_flag_says() {
        // Leader 1 and data replica 3 are dead. Only witness 2 is alive, and
        // it holds every committed record. Electing 3 would discard them, so
        // the answer is Unavailable: never Recover, never an unclean Elect.
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let cases: [(RecoveryStrategy, bool); 4] = [
            (RecoveryStrategy::None, false),
            (RecoveryStrategy::None, true),
            (RecoveryStrategy::Balanced, false),
            (RecoveryStrategy::Aggressive, true),
        ];
        for (strategy, unclean_enabled) in cases {
            let decision = decide(
                &pr,
                /*dead*/ 1,
                /*alive*/ &[2],
                /*witness_ids*/ &[2],
                strategy,
                unclean_enabled,
            );
            assert!(
                decision == super::FailoverDecision::Unavailable,
                "strategy {strategy:?}, unclean_enabled {unclean_enabled}"
            );
        }
    }

    #[test]
    fn unclean_election_never_picks_a_witness() {
        // ISR is {1} and broker 1 dies, so the KIP-841 out-of-ISR pick runs.
        // Replica 2 is alive but is the witness; the pick must fall through
        // to data replica 3.
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2, 3],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            true,
        );
        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(3),
                    isr: vec![NodeId(3)],
                    unclean: true,
                }
        );
    }

    #[test]
    fn unclean_election_is_unavailable_when_every_alive_replica_is_a_witness() {
        // Empty alive ISR and the only alive replica is the witness.
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            true,
        );
        assert!(decision == super::FailoverDecision::Unavailable);
    }

    #[test]
    fn isr_shrink_for_a_non_leader_death_keeps_the_witness() {
        // Broker 3 dies and the leader is alive, so this is a plain shrink.
        // The witness stays in the emitted ISR.
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let decision = decide(
            &pr,
            /*dead*/ 3,
            /*alive*/ &[1, 2],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            false,
        );
        assert!(
            decision
                == super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                }
        );
    }

    /// The full unclean-restart decision for one partition.
    fn restart_decide(
        pr: &PartitionRecord,
        returning: u64,
        alive: &[u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
    ) -> super::FailoverDecision {
        let alive: std::collections::HashSet<NodeId> = alive.iter().copied().map(NodeId).collect();
        unclean_restart_one(
            pr,
            NodeId(returning),
            &alive,
            &witnesses(&[]),
            strategy,
            unclean_enabled,
        )
    }

    /// A returning broker is one event about one broker. The follower case
    /// takes that broker out of the ISR and leaves every other member where
    /// it is, however the controller currently rates its liveness, which is
    /// Kafka's `Replicas.copyWithout(partition.isr, {-1, brokerId})`.
    ///
    /// The second half is why this is not [`failover_one`]: a registration is
    /// answered whenever liveness says the broker is dead, and a controller
    /// that has just been elected has an empty liveness registry, so the
    /// dead-broker policy would answer the same event by emptying the ISR.
    #[test]
    fn an_unclean_restart_removes_only_the_returning_broker_from_the_isr() {
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[],
            RecoveryStrategy::None,
            false,
        );

        assert!(
            decision
                == super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                }
        );
        assert!(
            decide(
                &pr,
                /*dead*/ 3,
                /*alive*/ &[],
                &[],
                RecoveryStrategy::None,
                false
            ) == super::FailoverDecision::ShrinkIsr { isr: vec![] }
        );
    }

    /// A partition the returning broker is still recorded as leading cannot
    /// take a bare ISR rewrite: it would leave a leader that is not in its own
    /// ISR. That case is the dead-broker policy, unchanged, because the broker
    /// is dead as far as liveness is concerned.
    #[test]
    fn an_unclean_restart_of_a_leader_takes_the_failover_policy() {
        let pr = partition_record(/*leader*/ 3, &[1, 2, 3], &[1, 2, 3]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[1, 2],
            RecoveryStrategy::None,
            false,
        );

        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(1),
                    isr: vec![NodeId(1), NodeId(2)],
                    unclean: false,
                }
        );
    }

    /// A partition the returning broker neither leads nor is in the ISR of has
    /// nothing to withdraw, even when it is still one of the replicas.
    #[test]
    fn an_unclean_restart_leaves_a_partition_it_is_not_in_the_isr_of_alone() {
        let pr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[1, 2],
            RecoveryStrategy::None,
            false,
        );

        assert!(decision == super::FailoverDecision::NoChange);
    }

    /// One row of the no-witness regression table.
    struct FailoverCase<'a> {
        pr: &'a PartitionRecord,
        dead: u64,
        alive: &'a [u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        expected: super::FailoverDecision,
    }

    #[test]
    fn an_empty_witness_set_leaves_every_failover_decision_unchanged() {
        // The regression guard for non-stretch clusters: each case is decided
        // with no witnesses, and the expected value is the pre-witness answer.
        let clean = partition_record(/*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let empty_isr = partition_record(/*leader*/ 1, &[1, 2, 3], &[1]);
        let cases = [
            // Clean election picks the first alive ISR member.
            FailoverCase {
                pr: &clean,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                },
            },
            // Non-leader death shrinks the ISR and keeps the leader.
            FailoverCase {
                pr: &clean,
                dead: 3,
                alive: &[1, 2],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                },
            },
            // Empty ISR with unclean off stays unavailable.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::Unavailable,
            },
            // Empty ISR with unclean on picks the first alive replica.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: true,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2)],
                    unclean: true,
                },
            },
            // Empty ISR with an offset-aware strategy defers to the URM.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::Balanced,
                unclean_enabled: false,
                expected: super::FailoverDecision::Recover(RecoveryStrategy::Balanced),
            },
            // An unrelated broker changes nothing.
            FailoverCase {
                pr: &clean,
                dead: 9,
                alive: &[1, 2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::NoChange,
            },
        ];
        for case in cases {
            let decision = decide(
                case.pr,
                case.dead,
                case.alive,
                &[],
                case.strategy,
                case.unclean_enabled,
            );
            assert!(
                decision == case.expected,
                "dead {}, alive {:?}, strategy {:?}, unclean {}",
                case.dead,
                case.alive,
                case.strategy,
                case.unclean_enabled
            );
        }
    }
}
