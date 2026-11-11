//! The pure failover policy. [`failover_one`] answers what to do with one
//! partition when a replica of it is gone, and [`FailoverPlan`] is the shape
//! the two controller scans build out of those answers. Nothing here does
//! I/O, so the policy is unit-testable and model-checkable on its own.

use {
    crate::{config_keys::RecoveryStrategy, elr::state::PartitionElr},
    krabka_metadata::{
        LeaderEpoch, LeaderRecoveryState, MetadataRecord, PartitionRecord, PartitionRecoveryRecord,
    },
    krabka_raft::NodeId,
    krabka_verified::consensus::{
        FailoverAction, FailoverFacts, FailoverRecovery, LiveIsr, OutOfIsrFacts, failover_action,
    },
    std::collections::HashSet,
};

/// Output of a failover scan: immediate metadata changes plus partitions
/// that need asynchronous offset-aware recovery through the URM.
#[derive(Default)]
pub struct FailoverPlan {
    pub changes: Vec<MetadataRecord>,
    pub recoveries: Vec<(String, i32, RecoveryStrategy)>,
    /// Partitions the dead broker leads that have no live ISR replica to
    /// elect. The scan leaves their records alone and marks them leaderless
    /// through the ELR they publish. The caller decides how loudly to
    /// report them: the death edge warns once, the per-tick sweep does not
    /// repeat that warning every second.
    pub unavailable: Vec<(String, i32)>,
}

/// The partition epoch and leader epoch a partition change writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NextEpochs {
    /// The `PartitionRecord.partition_epoch` the change writes.
    pub partition: i32,
    /// The `PartitionRecord.leader_epoch` the change writes.
    pub leader: LeaderEpoch,
}

/// Whether the leader a [`PartitionChange`] installs must recover its log
/// before it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderRecovery {
    /// The leader serves as soon as it is elected.
    Recovered,
    /// The leader is marked `RECOVERING`, which is what an unclean election
    /// writes.
    Recovering,
}

/// The new leadership a partition change writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionChange {
    /// The leader the change installs.
    pub leader: NodeId,
    /// The ISR the change installs.
    pub isr: Vec<NodeId>,
    /// The epochs the change writes.
    pub epochs: NextEpochs,
    /// Whether `leader` must recover first.
    pub recovery: LeaderRecovery,
}

impl PartitionChange {
    /// The change an election of `leader` with `isr` at `epochs` writes. An
    /// unclean election's leader recovers first; a clean one's does not.
    pub fn election(leader: NodeId, isr: Vec<NodeId>, epochs: NextEpochs, unclean: bool) -> Self {
        let recovery = if unclean {
            LeaderRecovery::Recovering
        } else {
            LeaderRecovery::Recovered
        };
        Self {
            leader,
            isr,
            epochs,
            recovery,
        }
    }
}

/// Push the `PartitionRecord` that moves `pr` to `change`'s leader, ISR and
/// epochs, every other field carried over from `pr`. When the change's leader
/// is [`LeaderRecovery::Recovering`], a `PartitionRecoveryRecord` marking it
/// `RECOVERING` follows.
pub fn push_partition_change(
    changes: &mut Vec<MetadataRecord>,
    pr: &PartitionRecord,
    PartitionChange {
        leader,
        isr,
        epochs,
        recovery,
    }: PartitionChange,
) {
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        leader,
        isr,
        leader_epoch: epochs.leader,
        partition_epoch: epochs.partition,
        ..pr.clone()
    }));
    if recovery == LeaderRecovery::Recovering {
        changes.push(MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                state: LeaderRecoveryState::Recovering,
            },
        ));
    }
}

/// The pure per-partition failover decision shared by the dead-broker scan
/// (`compute_failover_changes`) and the offline-log-dir scan
/// (`compute_offline_dir_failover_changes`). No I/O: the callers handle
/// partition filtering, the alive snapshot, record construction, metrics, and
/// recovery enqueue. This enum is separate so the failover policy is
/// independently unit-testable and model-checkable, and so the two scans share
/// one copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverDecision {
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
    /// Kafka writes `leader = -1` for this; the scans keep the record and
    /// publish the last-known ELR that marks the partition as leaderless.
    Unavailable,
    /// Nothing to do for this partition.
    NoChange,
}

/// Decide the failover action for one partition. `alive` is the controller's
/// snapshot of live brokers. `witnesses` is the set of data-bearing witness
/// nodes, which replicate the partition and count toward
/// `min.insync.replicas` but never lead it. `elr` is the partition's published
/// KIP-966 eligible and last-known replicas. `strategy` and
/// `unclean_enabled` are the topic's resolved recovery policy.
///
/// # One broker leaves, and the leader is re-checked
///
/// This is Apache Kafka's answer to a fenced or unregistered broker:
/// `ReplicationControlManager.handleBrokerFenced` and
/// `handleBrokerUnregistered` both run `generateLeaderAndIsrUpdates` over
/// `brokersToIsrs.partitionsWithBrokerInIsr(brokerId)`, which builds one
/// `PartitionChangeBuilder` per partition with
///
/// ```text
/// targetIsr          = Replicas.copyWithout(partition.isr, brokerId)
/// isAcceptableLeader = r -> r != brokerId && clusterControl.isActive(r)
/// ```
///
/// Three rules follow, and this function keeps all three.
///
/// - Only a partition whose ISR holds `dead` is touched. The leader is always
///   an ISR member, so that includes every partition `dead` leads; a
///   partition `dead` only replicates is [`FailoverDecision::NoChange`].
/// - Only `dead` leaves the ISR. Every other member stays, however this
///   controller rates its liveness right now: its own death is its own event,
///   and a sweep runs [`failover_one`] for it too. A member that is kept
///   although it is down blocks the high watermark, so it still holds every
///   committed record, and keeping it loses nothing.
/// - The leader is re-elected whenever it is not a valid new leader of the
///   target ISR -- `electAnyLeader`'s first test is
///   `isValidNewLeader(partition.leader)`. That covers the dead leader, and
///   also a leader that died earlier and whose own failover has not run yet
///   when a follower's does. The follower's event elects, exactly as Kafka's
///   would, rather than rewriting the ISR under a leader that cannot serve.
///
/// A witness stays in the emitted ISR. It holds every committed record, so it
/// is what keeps `acks=all` writable after a site loss. Only the leader pick
/// excludes it: every election path adds "not a witness" to
/// `isAcceptableLeader`.
///
/// # The election ladder
///
/// `PartitionChangeBuilder.isValidNewLeader`, read out of Kafka trunk, is
///
/// ```text
/// (targetIsr.contains(id) || (targetIsr.isEmpty() && targetElr.contains(id)))
///     && isAcceptableLeader.test(id)
/// ```
///
/// and `electAnyLeader` takes the first replica **in assignment order**
/// (`targetReplicas.stream().filter(this::isValidNewLeader).findFirst()`)
/// that satisfies it, as `ElectionResult(node, false)` -- `false` being
/// `unclean`. The clean pick therefore walks `pr.replicas`, not `pr.isr`, and
/// the emitted ISR is the target ISR itself.
///
/// With no valid ISR member, a surviving ELR member is elected, called clean,
/// and never reaches the last-known-leader branch or the `Election.UNCLEAN`
/// branch below it. Nothing about that pick consults
/// `unclean.leader.election.enable`, and Kafka has no
/// `unclean.recovery.strategy` in front of it either: an ELR member left the
/// ISR while the partition still had `min.insync.replicas` members, so it
/// holds every committed record and electing it loses nothing.
/// [`FailoverAction::ElectFromElr`] is that rung, and it sits above both the
/// KIP-966 offset-aware recovery and the KIP-841 out-of-ISR election for the
/// same reason. Kafka's `tryElection` answers it with `targetIsr =
/// List.of(node)` and leaves `leaderRecoveryState` alone, which is the
/// singleton ISR and the `unclean: false` returned here.
///
/// Kafka opens the ELR disjunct only once `targetIsr` is empty. This opens it
/// once the target ISR has no *live* member, which is the same set in every
/// state Kafka can reach: Kafka removes a broker from every ISR in the same
/// record that fences it, so a target ISR member that is down is one whose
/// own removal krabka's per-broker scans have not run yet. Gating on the
/// literal emptiness would make the answer depend on which of two dead
/// brokers' scans ran first, and could elect uncleanly, or defer to the
/// offset-aware recovery, while an eligible replica was waiting.
///
/// The published set is enough on its own. Kafka recomputes `targetElr` as
/// `(elr ∪ isr) - targetIsr - uncleanShutdownReplicas` immediately before the
/// election, but the only id that recomputation adds is `dead`, which fails
/// the acceptable-leader half of the test regardless.
///
/// The last known leader is the next rung. `canElectLastKnownLeader` fires
/// when ELR is enabled, the target ISR and the target ELR are both empty, the
/// partition's `lastKnownElr` holds exactly one replica, and that replica is
/// acceptable; Kafka elects it as `ElectionResult(node, true)`, so
/// `tryElection` sets the ISR to that replica alone and the leader recovery
/// state to `RECOVERING`. It sits under the ELR pick and above the
/// `Election.UNCLEAN` branch in both `electAnyLeader` and
/// `electPreferredLeader`, and like the ELR pick it consults neither
/// `unclean.leader.election.enable` nor `unclean.recovery.strategy`.
/// [`FailoverAction::ElectLastKnown`] is that rung and it returns
/// `unclean: true`, which is what puts the partition in `Recovering`. The
/// target ELR is the published one plus `dead` when `dead` is in the ISR the
/// change leaves, so an ordinary death never empties it; the rung is open to
/// [`elect_leaderless_one`], and to a replica that restarted uncleanly, which
/// Kafka keeps out of the target ELR.
///
/// # Min ISR, and a partition that cannot elect
///
/// `min.insync.replicas` never blocks the ISR change: Kafka's builder lets the
/// ISR fall below it, and to empty under KIP-966, and only the ELR tracks the
/// shortfall. That bookkeeping is not here. Every scan runs
/// [`ElrPublisher`](crate::elr::ElrPublisher) over the records it emits, which
/// applies `maybePopulateTargetElr`'s rule to the change.
///
/// When no rung can elect, Kafka writes `leader = -1` alongside the target
/// ISR, the ELR that ISR implies, and the last leader as `lastKnownElr`. A
/// krabka partition record always names a leader, so the change is split. A
/// partition led by `dead` is [`FailoverDecision::Unavailable`] and its record
/// is left alone, because a dead leader dropped from its own ISR would be a
/// leader outside its ISR; the scans publish the rest through
/// [`ElrPublisher::leaderless`](crate::elr::ElrPublisher::leaderless), and the
/// one-member last-known ELR is what marks it as having no leader. A partition
/// led by a broker that is down, but whose failover has not run, still loses
/// `dead` from its ISR, which is [`FailoverDecision::ShrinkIsr`]; that leader
/// stays in the ISR, and its own scan reports the partition unavailable.
pub fn failover_one(pr: &PartitionRecord, dead: NodeId, ladder: &Ladder<'_>) -> FailoverDecision {
    // `partitionsWithBrokerInIsr(brokerId)`: the leader is an ISR member, so
    // this keeps every partition `dead` leads.
    if pr.leader != dead && !pr.isr.contains(&dead) {
        return FailoverDecision::NoChange;
    }
    let departure = Departure {
        node: dead,
        down: true,
        // `dead` joins the target ELR: it is in the ISR the change leaves.
        joins_elr: pr.isr.contains(&dead),
    };
    decide(pr, &departure, ladder)
}

/// Decide what to do with a partition that has no leader, read out of the
/// last-known ELR that marks it (see [`PartitionElr::is_leaderless`]), now that
/// `alive` is the cluster as the caller sees it.
///
/// This is Kafka's `handleBrokerUnfenced`, which runs
/// `generateLeaderAndIsrUpdates` over `brokersToIsrs.partitionsWithNoLeader()`
/// with the unfencing broker as an acceptable leader, so the election ladder of
/// [`failover_one`] runs again for a partition that had nothing to elect when
/// it lost its leader. Nothing leaves the partition this time. The last leader
/// is out of the ISR Kafka holds, as it was when the partition lost its leader,
/// and it can lead once `alive` names it: from the ELR when the fence put it
/// there, and as the last known leader when an unclean restart kept it out.
/// A partition that is not marked leaderless is [`FailoverDecision::NoChange`].
pub fn elect_leaderless_one(pr: &PartitionRecord, ladder: &Ladder<'_>) -> FailoverDecision {
    if !ladder.elr.is_leaderless(pr.leader) {
        return FailoverDecision::NoChange;
    }
    let departure = Departure {
        node: pr.leader,
        // Whether the last leader is alive is the caller's `alive` to say.
        down: false,
        // It is in the published ELR when it belongs there.
        joins_elr: false,
    };
    decide(pr, &departure, ladder)
}

/// What the ladder reads about the cluster and the partition's topic.
#[derive(bon::Builder)]
pub struct Ladder<'a> {
    pub(crate) alive: &'a HashSet<NodeId>,
    pub(crate) witnesses: &'a HashSet<NodeId>,
    pub(crate) elr: &'a PartitionElr,
    pub(crate) strategy: RecoveryStrategy,
    pub(crate) unclean_enabled: bool,
}

/// The replica whose removal from the ISR the ladder answers.
struct Departure {
    node: NodeId,
    /// It cannot lead, whatever the alive set says of it.
    down: bool,
    /// Kafka's `targetElr` gains it.
    joins_elr: bool,
}

/// The election ladder for one partition: `Replicas.copyWithout(partition.isr,
/// brokerId)` is the target ISR, and the rungs are the ones the module docs
/// walk through.
fn decide(pr: &PartitionRecord, gone: &Departure, ctx: &Ladder<'_>) -> FailoverDecision {
    let Ladder {
        alive,
        witnesses,
        elr,
        strategy,
        unclean_enabled,
    } = *ctx;
    // `Replicas.copyWithout(partition.isr, brokerId)`: `gone` alone leaves.
    let target_isr: Vec<NodeId> = pr.isr.iter().copied().filter(|n| *n != gone.node).collect();
    let live = |n: &NodeId| !(gone.down && *n == gone.node) && alive.contains(n);
    // Kafka's `isAcceptableLeader`, plus the witness rule krabka adds to
    // every election path.
    let acceptable = |n: &NodeId| live(n) && !witnesses.contains(n);
    // `isValidNewLeader(partition.leader)`: the leader keeps its place only
    // while it is an acceptable member of the target ISR.
    let leader_stays = target_isr.contains(&pr.leader) && acceptable(&pr.leader);
    // Every pick walks `targetReplicas`, in assignment order, and they differ
    // only in the test.
    let pick = |test: &dyn Fn(&NodeId) -> bool| pr.replicas.iter().copied().find(|n| test(n));
    let clean_candidate = pick(&|n| target_isr.contains(n) && acceptable(n));
    let elr_candidate = pick(&|n| {
        acceptable(n)
            && i32::try_from(n.0).is_ok_and(|id| elr.eligible_leader_replicas.contains(&id))
    });
    // `canElectLastKnownLeader`: the target ELR is empty, and the one replica
    // `lastKnownElr` holds is acceptable.
    let target_elr_empty = elr.eligible_leader_replicas.is_empty() && !gone.joins_elr;
    let last_known_candidate = match elr.last_known_elr.as_slice() {
        [only] if target_elr_empty => pick(&|n| acceptable(n) && u64::try_from(*only) == Ok(n.0)),
        _ => None,
    };
    let unclean_candidate = pick(&acceptable);
    let recovery = match strategy {
        RecoveryStrategy::None => FailoverRecovery::None,
        RecoveryStrategy::Balanced => FailoverRecovery::Balanced,
        RecoveryStrategy::Aggressive => FailoverRecovery::Aggressive,
    };
    let live_isr = if clean_candidate.is_some() {
        LiveIsr::Electable
    } else if target_isr.iter().any(live) {
        LiveIsr::WitnessesOnly
    } else {
        LiveIsr::Empty
    };
    match failover_action(FailoverFacts {
        // The kernel's "the leader went away": the leader must be replaced.
        leader_dead: !leader_stays,
        isr_shrunk: target_isr.len() < pr.isr.len(),
        live_isr,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr: elr_candidate.is_some(),
            last_known_leader_electable: last_known_candidate.is_some(),
            recovery,
            unclean_election_available: unclean_enabled && unclean_candidate.is_some(),
        },
    }) {
        // The kernel elects only on a candidate's presence, so each `Elect*`
        // action has its candidate. Were one missing, nothing could lead, and
        // `Unavailable` is the safe answer.
        FailoverAction::ElectClean => {
            // Clean: the new leader was in the ISR, so it holds every committed
            // record. No data loss, and the ISR is Kafka's target ISR.
            clean_candidate.map_or(FailoverDecision::Unavailable, |new_leader| {
                FailoverDecision::Elect {
                    leader: new_leader,
                    isr: target_isr,
                    unclean: false,
                }
            })
        }
        FailoverAction::ElectFromElr => {
            // KIP-966: the live ISR is empty, but a surviving replica is
            // published as eligible to lead, so it holds every record the
            // partition ever acknowledged. Kafka reports this election as
            // clean and gates it on nothing, and so does this: the ISR
            // narrows to the winner, and no unclean-election meter moves.
            elr_candidate.map_or(FailoverDecision::Unavailable, |new_leader| {
                FailoverDecision::Elect {
                    leader: new_leader,
                    isr: vec![new_leader],
                    unclean: false,
                }
            })
        }
        FailoverAction::ElectLastKnown => {
            // The one replica that led when the partition lost its leader is
            // back. It may have lost an unflushed tail, so Kafka calls this
            // election unclean -- singleton ISR, `RECOVERING` -- and takes it
            // without `unclean.leader.election.enable` or
            // `unclean.recovery.strategy`.
            last_known_candidate.map_or(FailoverDecision::Unavailable, |new_leader| {
                FailoverDecision::Elect {
                    leader: new_leader,
                    isr: vec![new_leader],
                    unclean: true,
                }
            })
        }
        FailoverAction::Recover(_) => FailoverDecision::Recover(strategy),
        FailoverAction::ElectUnclean => {
            // KIP-841: ISR is dead but the operator opted into possible data
            // loss. Elect the first alive non-witness replica, singleton ISR.
            unclean_candidate.map_or(FailoverDecision::Unavailable, |new_leader| {
                FailoverDecision::Elect {
                    leader: new_leader,
                    isr: vec![new_leader],
                    unclean: true,
                }
            })
        }
        FailoverAction::Unavailable
            if pr.leader != gone.node && target_isr.contains(&pr.leader) =>
        {
            // Nothing can replace a leader that is down but whose own failover
            // has not run. Kafka's record would still carry the target ISR, so
            // `dead` leaves it; the leader stays in it, and its own scan
            // answers for the partition.
            FailoverDecision::ShrinkIsr { isr: target_isr }
        }
        FailoverAction::Unavailable => {
            // Every alive ISR member is a witness, or nothing above this rung
            // could elect: no live ISR, no surviving eligible leader replica,
            // no offset-aware strategy, and no permitted unclean election. The
            // partition is unavailable, and that is the safe answer.
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
        FailoverAction::ShrinkIsr => FailoverDecision::ShrinkIsr { isr: target_isr },
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
/// liveness. This does the same.
///
/// For a partition the returning broker only follows, it stops there, and
/// that is where it is not [`failover_one`]. Kafka's builder would also
/// re-elect a leader its `isActive` no longer accepts, and so would
/// [`failover_one`]. But a registration is answered whenever liveness says
/// the returning broker is dead, and a controller that has just been elected
/// may not have populated its liveness registry yet, so that re-election
/// would be decided against a registry that calls every leader dead. The
/// leader of such a partition is left to its own failover, which runs only
/// once liveness has really declared it dead.
///
/// The one case that does need the full policy is the partition the returning
/// broker is still recorded as leading. A bare ISR rewrite there would leave a
/// leader that is not in its own ISR, so that case is handed to
/// [`failover_one`] unchanged: the broker is dead as far as liveness is
/// concerned -- that is the precondition the registration was accepted under
/// -- so it is the same question the dead-broker scan asks, and it deserves
/// the same answer, up to and including an offset-aware recovery.
///
/// `elr` is the partition's published ELR, and it is read as the image still
/// holds it: the withdrawal this event also performs takes `returning` out of
/// every eligible set, and `returning` is the one node [`failover_one`] will
/// not elect anyway, so the two orders agree. Kafka's `uncleanShutdownReplicas`
/// keeps `returning` out of the target ELR, so it is not what keeps that set
/// from being empty, as a fenced broker is.
pub fn unclean_restart_one(
    pr: &PartitionRecord,
    returning: NodeId,
    alive: &HashSet<NodeId>,
    witnesses: &HashSet<NodeId>,
    elr: &PartitionElr,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    if pr.leader == returning {
        let departure = Departure {
            node: returning,
            down: true,
            joins_elr: false,
        };
        return decide(pr, &departure, &Ladder {
            alive,
            witnesses,
            elr,
            strategy,
            unclean_enabled,
        });
    }
    if !pr.isr.contains(&returning) {
        return FailoverDecision::NoChange;
    }
    FailoverDecision::ShrinkIsr {
        isr: pr.isr.iter().copied().filter(|n| *n != returning).collect(),
    }
}

#[cfg(test)]
mod failover_scenarios;

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            leader_election::test_support::{seed_partition, witnesses},
            test_support::{partition_elr as elr, replica_nodes},
        },
        assert2::assert,
    };

    fn partition_record(leader: u64, replicas: &[u64], isr: &[u64]) -> PartitionRecord {
        seed_partition()
            .leader(leader)
            .replicas(&replica_nodes(replicas))
            .isr(&replica_nodes(isr))
            .call()
    }

    #[derive(serde::Deserialize)]
    struct LeaderlessReplicaInput {
        leader: u64,
        replicas: Vec<u64>,
        isr: Vec<u64>,
        eligible: Vec<i32>,
        last_known: Vec<i32>,
        alive: Vec<u64>,
        witness_ids: Vec<u64>,
        strategy: String,
        unclean_enabled: bool,
    }

    #[derive(serde::Deserialize)]
    #[serde(tag = "kind")]
    enum LeaderlessDecisionExpectation {
        Elect {
            leader: u64,
            isr: Vec<u64>,
            unclean: bool,
        },
        Unavailable,
        NoChange,
    }

    #[derive(Default)]
    struct LeaderlessElection {
        partition: Option<PartitionRecord>,
        alive: HashSet<NodeId>,
        witnesses: HashSet<NodeId>,
        elr: Option<PartitionElr>,
        strategy: Option<RecoveryStrategy>,
        unclean_enabled: bool,
        decision: Option<FailoverDecision>,
    }

    #[rstest::fixture]
    fn leaderless_election() -> LeaderlessElection {
        // Each election uses its own assignment, availability and recovery policy.
        LeaderlessElection::default()
    }

    #[rstest_bdd_macros::given(
        "the recorded leader, replicas, ISR, ELR, live brokers and recovery policy are JSON {partition}"
    )]
    fn supplied_leaderless_election(
        leaderless_election: &mut LeaderlessElection,
        partition: String,
    ) {
        let input: LeaderlessReplicaInput = serde_json::from_str(&partition)
            .expect("explicit leaderless partition and broker eligibility facts");
        leaderless_election.partition =
            Some(partition_record(input.leader, &input.replicas, &input.isr));
        leaderless_election.alive = input.alive.into_iter().map(NodeId).collect();
        leaderless_election.witnesses = witnesses(&input.witness_ids);
        leaderless_election.elr = Some(elr(&input.eligible, &input.last_known));
        leaderless_election.strategy = Some(match input.strategy.as_str() {
            "None" => RecoveryStrategy::None,
            "Balanced" => RecoveryStrategy::Balanced,
            "Aggressive" => RecoveryStrategy::Aggressive,
            _ => panic!("unknown recovery strategy {}", input.strategy),
        });
        leaderless_election.unclean_enabled = input.unclean_enabled;
    }

    #[rstest_bdd_macros::when("the controller elects a leader for the leaderless partition")]
    fn elect_supplied_leaderless_partition(leaderless_election: &mut LeaderlessElection) {
        let ladder = Ladder::builder()
            .alive(&leaderless_election.alive)
            .witnesses(&leaderless_election.witnesses)
            .elr(
                leaderless_election
                    .elr
                    .as_ref()
                    .expect("ELR metadata was given"),
            )
            .strategy(
                leaderless_election
                    .strategy
                    .expect("recovery strategy was given"),
            )
            .unclean_enabled(leaderless_election.unclean_enabled)
            .build();
        leaderless_election.decision = Some(elect_leaderless_one(
            leaderless_election
                .partition
                .as_ref()
                .expect("recorded partition was given"),
            &ladder,
        ));
    }

    #[rstest_bdd_macros::then("the complete leaderless election decision is JSON {decision}")]
    fn expected_leaderless_election(leaderless_election: &LeaderlessElection, decision: String) {
        let expected: LeaderlessDecisionExpectation =
            serde_json::from_str(&decision).expect("independent complete election outcome");
        let expected = match expected {
            LeaderlessDecisionExpectation::Elect {
                leader,
                isr,
                unclean,
            } => FailoverDecision::Elect {
                leader: NodeId(leader),
                isr: isr.into_iter().map(NodeId).collect(),
                unclean,
            },
            LeaderlessDecisionExpectation::Unavailable => FailoverDecision::Unavailable,
            LeaderlessDecisionExpectation::NoChange => FailoverDecision::NoChange,
        };
        assert!(leaderless_election.decision.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/leader_election/policy.feature",
        fixtures = [leaderless_election: LeaderlessElection]
    );
}
