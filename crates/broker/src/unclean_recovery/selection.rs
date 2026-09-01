//! The pure winner-selection helpers of KIP-966 unclean recovery.
//!
//! These functions rank the log states reported by the surviving replicas and
//! detect a recovery that a newer leader has already superseded. They hold no
//! I/O and no controller state, so the stateright models and the unit tests
//! drive them directly.
//!
//! # The eligible leader replicas come first
//!
//! "Most complete surviving log" is a guess. It is the best guess available
//! once every replica that is known to be complete is gone, but it can still
//! drop committed records: the longest surviving log is only the longest one
//! that answered. KIP-966's eligible-leader-replica set is the answer that is
//! not a guess. An ELR member left the ISR while the partition still had
//! `min.insync.replicas` members, so it holds every record the partition ever
//! acknowledged, and electing it loses nothing.
//!
//! Apache Kafka orders the two the same way, in
//! `PartitionChangeBuilder.electAnyLeader`. Its `isValidNewLeader`, read out
//! of `kafka-metadata-4.3.1.jar`, is
//!
//! ```text
//! (targetIsr.contains(id) || (targetIsr.isEmpty() && targetElr.contains(id)))
//!     && isAcceptableLeader.test(id)
//! ```
//!
//! so an unfenced ELR member is a valid new leader for a partition whose ISR
//! has emptied, and `electAnyLeader` returns it as `ElectionResult(node,
//! false)` -- `false` being `unclean`. Only when no such replica exists does
//! Kafka reach its unclean branch. [`select_leader`] is that order, and
//! [`ElectionBasis`] is the `unclean` flag under a name that says which rule
//! fired.
//!
//! The ordering does not depend on `unclean.recovery.strategy`. Kafka's check
//! is strategy-blind, and so is this one: a `Balanced` recovery prefers an ELR
//! member over a longer non-ELR log, and so does an `Aggressive` one.

use std::collections::BTreeSet;

use krabka_raft::NodeId;

/// One replica's reported log state, from a `GetReplicaLogInfo` response.
///
/// This type is separate from the generated wire type, so a unit test can
/// drive the selection logic without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Picks the replica with the most complete log. It ranks by the highest
/// `last_written_leader_epoch`, then the highest `log_end_offset`, then the
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    responses
        .iter()
        .max_by(|a, b| {
            a.last_written_leader_epoch
                .cmp(&b.last_written_leader_epoch)
                .then(a.log_end_offset.cmp(&b.log_end_offset))
                .then(b.broker_id.cmp(&a.broker_id)) // lower broker_id wins ties
        })
        .map(|r| r.broker_id)
}

/// Which rule chose the leader, and so whether the election lost anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionBasis {
    /// The winner was in the partition's eligible-leader-replica set, so it
    /// held every committed record when it left the ISR. Kafka reports this
    /// election as clean.
    EligibleLeaderReplica,
    /// No eligible leader replica answered, so the winner is only the most
    /// complete log among the survivors that did. Committed records the
    /// partition acknowledged may not be in it.
    MostCompleteLog,
}

impl ElectionBasis {
    /// Whether this election may have dropped committed records. It is the
    /// `unclean` flag on Kafka's `PartitionChangeBuilder.ElectionResult`.
    pub(crate) fn loses_data(self) -> bool {
        self == Self::MostCompleteLog
    }

    /// The clause an operator-facing log line or audit reason reads, after the
    /// broker id: "elected broker 3 <this>".
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::EligibleLeaderReplica => {
                "from the eligible leader replicas, so no committed record is lost"
            }
            Self::MostCompleteLog => {
                "as the most complete surviving log, so committed records may be lost"
            }
        }
    }
}

/// The leader one recovery elects, and the rule that chose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Election {
    pub leader: NodeId,
    pub basis: ElectionBasis,
}

/// Picks the leader out of the replicas that answered the poll.
///
/// `eligible` is the partition's published eligible-leader-replica set. A
/// responder that is in it wins outright, however short its log, because it is
/// known to hold every committed record and a longer non-ELR log is not.
/// [`select_best_replica`] then ranks within whichever group won, so a
/// partition with several surviving ELR members elects one of them
/// deterministically. Kafka takes the first such replica in assignment order
/// instead; every ELR member is equally complete, so the two choices differ
/// only in which correct answer they give, and reusing the one comparator
/// keeps a single ranking in the code.
///
/// Returns `None` when nothing answered.
pub(crate) fn select_leader(responses: &[ReplicaLogInfo], eligible: &[i32]) -> Option<Election> {
    let eligible: BTreeSet<i32> = eligible.iter().copied().collect();
    let from_elr: Vec<ReplicaLogInfo> = responses
        .iter()
        .copied()
        .filter(|r| i32::try_from(r.broker_id.0).is_ok_and(|id| eligible.contains(&id)))
        .collect();
    if let Some(leader) = select_best_replica(&from_elr) {
        return Some(Election {
            leader,
            basis: ElectionBasis::EligibleLeaderReplica,
        });
    }
    select_best_replica(responses).map(|leader| Election {
        leader,
        basis: ElectionBasis::MostCompleteLog,
    })
}

/// Returns true if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition. A
/// newer leader then already exists, and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn ri(broker_id: u64, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(broker_id),
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert!(select_best_replica(&r) == Some(NodeId(1)));
    }

    #[test]
    fn empty_input_returns_none() {
        assert!(select_best_replica(&[]) == None);
    }

    /// The eligible-leader-replica set outranks the log length. Each case
    /// gives the same three responders a different ELR and names the whole
    /// election it must produce.
    #[test]
    fn the_eligible_leader_replicas_outrank_the_longest_log() {
        // Broker 2 holds the longest log; brokers 1 and 3 are shorter, and 3
        // is shorter than 1.
        let responses = [ri(1, 5, 40), ri(2, 5, 400), ri(3, 5, 20)];
        let elr_of = |leader: u64| Election {
            leader: NodeId(leader),
            basis: ElectionBasis::EligibleLeaderReplica,
        };
        let cases = [
            (
                "the shortest log wins when it is the only ELR member",
                vec![3],
                Some(elr_of(3)),
            ),
            (
                "the best ELR member wins when several answered",
                vec![1, 3],
                Some(elr_of(1)),
            ),
            (
                "an empty ELR falls back to the most complete log",
                vec![],
                Some(Election {
                    leader: NodeId(2),
                    basis: ElectionBasis::MostCompleteLog,
                }),
            ),
            (
                "an ELR member that did not answer cannot be elected",
                vec![9],
                Some(Election {
                    leader: NodeId(2),
                    basis: ElectionBasis::MostCompleteLog,
                }),
            ),
        ];
        for (label, eligible, expected) in cases {
            check!(select_leader(&responses, &eligible) == expected, "{label}");
        }
    }

    #[test]
    fn no_response_elects_nobody_however_the_elr_reads() {
        check!(select_leader(&[], &[]) == None);
        check!(select_leader(&[], &[1, 2]) == None);
    }

    #[test]
    fn only_the_fallback_loses_data() {
        check!(ElectionBasis::MostCompleteLog.loses_data());
        check!(!ElectionBasis::EligibleLeaderReplica.loses_data());
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: NodeId(2),
            last_written_leader_epoch: 5,
            log_end_offset: 10,
            current_leader_epoch: 7,
        }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}
