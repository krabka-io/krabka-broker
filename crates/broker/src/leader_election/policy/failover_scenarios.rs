//! Step definitions for `tests/features/migrated/src/leader_election/failover`:
//! the per-partition decisions [`failover_one`] takes when a broker fails and
//! [`unclean_restart_one`] takes when one returns from an unclean shutdown.

use {
    super::{FailoverDecision, Ladder, PartitionElr, failover_one, unclean_restart_one},
    crate::{
        config_keys::RecoveryStrategy,
        leader_election::test_support::{Brokers, seed_partition},
    },
    assert2::assert,
    krabka_bdd::Given,
    krabka_metadata::PartitionRecord,
    krabka_raft::NodeId,
    std::{collections::HashSet, str::FromStr},
};

/// The recovery policy of the partition's topic: Kafka's
/// `unclean.leader.election.enable` and `unclean.recovery.strategy` together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, krabka_bdd::StepParam)]
enum RecoveryPolicy {
    #[default]
    #[step_param("clean only")]
    CleanOnly,
    #[step_param("unclean election")]
    UncleanElection,
    #[step_param("balanced recovery")]
    BalancedRecovery,
    #[step_param("aggressive recovery")]
    AggressiveRecovery,
}

impl RecoveryPolicy {
    /// The strategy and the unclean-election flag this policy configures.
    fn settings(self) -> (RecoveryStrategy, bool) {
        match self {
            Self::CleanOnly => (RecoveryStrategy::None, false),
            Self::UncleanElection => (RecoveryStrategy::None, true),
            Self::BalancedRecovery => (RecoveryStrategy::Balanced, false),
            Self::AggressiveRecovery => (RecoveryStrategy::Aggressive, true),
        }
    }
}

/// The decision a scenario expects, as its `Then` step words it.
#[derive(Debug)]
struct ExpectedDecision(FailoverDecision);

impl FromStr for ExpectedDecision {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let brokers = |list: &str| {
            list.parse::<Brokers>()
                .map(|brokers| brokers.nodes())
                .map_err(|error| format!("{list}: {error}"))
        };
        let broker = |id: &str| {
            id.parse()
                .map(NodeId)
                .map_err(|error| format!("{id}: {error}"))
        };
        let decision = match text {
            "mark the partition unavailable" => FailoverDecision::Unavailable,
            "leave the partition unchanged" => FailoverDecision::NoChange,
            "hand the partition to balanced unclean recovery" => {
                FailoverDecision::Recover(RecoveryStrategy::Balanced)
            }
            _ => {
                if let Some(isr) = text.strip_prefix("shrink the ISR to ") {
                    FailoverDecision::ShrinkIsr { isr: brokers(isr)? }
                } else if let Some(rest) = text.strip_prefix("elect broker ") {
                    if let Some(leader) = rest.strip_suffix(" uncleanly") {
                        let leader = broker(leader)?;
                        FailoverDecision::Elect {
                            leader,
                            isr: vec![leader],
                            unclean: true,
                        }
                    } else if let Some((leader, isr)) = rest.split_once(" cleanly with ISR ") {
                        FailoverDecision::Elect {
                            leader: broker(leader)?,
                            isr: brokers(isr)?,
                            unclean: false,
                        }
                    } else {
                        return Err(format!("unknown election {text:?}"));
                    }
                } else {
                    return Err(format!("unknown failover decision {text:?}"));
                }
            }
        };
        Ok(Self(decision))
    }
}

/// One partition, the cluster around it, and the decision the controller took.
struct FailoverScenario {
    partition: Given<PartitionRecord>,
    elr: PartitionElr,
    witnesses: HashSet<NodeId>,
    policy: RecoveryPolicy,
    decision: Given<FailoverDecision>,
}

#[rstest::fixture]
fn failover() -> FailoverScenario {
    FailoverScenario {
        partition: Given::new(
            "partition",
            "Given a partition with replicas <replicas> led by broker <leader> with ISR <isr>",
        ),
        elr: PartitionElr::default(),
        witnesses: HashSet::new(),
        policy: RecoveryPolicy::default(),
        decision: Given::new(
            "decision",
            "When broker <failed> fails while brokers <alive> stay alive",
        ),
    }
}

impl FailoverScenario {
    fn seed(&mut self, leader: u64, replicas: &Brokers, isr: &Brokers) {
        self.partition.set(
            seed_partition()
                .leader(leader)
                .replicas(&replicas.nodes())
                .isr(&isr.nodes())
                .call(),
        );
    }

    fn fail(&mut self, failed: u64, alive: &HashSet<NodeId>) {
        let (strategy, unclean_enabled) = self.policy.settings();
        let ladder = Ladder::builder()
            .alive(alive)
            .witnesses(&self.witnesses)
            .elr(&self.elr)
            .strategy(strategy)
            .unclean_enabled(unclean_enabled)
            .build();
        let decision = failover_one(self.partition.get(), NodeId(failed), &ladder);
        self.decision.set(decision);
    }

    fn restart(&mut self, returning: u64, alive: &HashSet<NodeId>) {
        let (strategy, unclean_enabled) = self.policy.settings();
        let decision = unclean_restart_one(
            self.partition.get(),
            NodeId(returning),
            alive,
            &self.witnesses,
            &self.elr,
            strategy,
            unclean_enabled,
        );
        self.decision.set(decision);
    }
}

#[krabka_bdd::given("a partition with replicas {replicas} led by broker {leader} with ISR {isr}")]
fn a_partition(failover: &mut FailoverScenario, replicas: Brokers, leader: u64, isr: Brokers) {
    failover.seed(leader, &replicas, &isr);
}

#[krabka_bdd::given(
    "a partition with replicas {replicas} led by broker {leader} with ISR {isr}, whose eligible leader replicas are {eligible}"
)]
fn a_partition_with_an_elr(
    failover: &mut FailoverScenario,
    replicas: Brokers,
    leader: u64,
    isr: Brokers,
    eligible: Brokers,
) {
    failover.seed(leader, &replicas, &isr);
    failover.elr.eligible_leader_replicas = eligible.elr_ids();
}

#[krabka_bdd::given(
    "a partition with replicas {replicas} led by broker {leader} with ISR {isr}, whose last known eligible leader replica is {last_known}"
)]
fn a_partition_with_a_last_known_elr(
    failover: &mut FailoverScenario,
    replicas: Brokers,
    leader: u64,
    isr: Brokers,
    last_known: Brokers,
) {
    failover.seed(leader, &replicas, &isr);
    failover.elr.last_known_elr = last_known.elr_ids();
}

#[krabka_bdd::given("broker {witness} is a witness")]
fn a_witness(failover: &mut FailoverScenario, witness: u64) {
    failover.witnesses.insert(NodeId(witness));
}

#[krabka_bdd::given("the topic's recovery policy is {policy}")]
fn recovery_policy(failover: &mut FailoverScenario, policy: RecoveryPolicy) {
    failover.policy = policy;
}

#[krabka_bdd::when(
    "broker {failed} fails while brokers {alive} stay alive",
    "broker {failed} fails while broker {alive} stays alive"
)]
fn broker_fails(failover: &mut FailoverScenario, failed: u64, alive: Brokers) {
    failover.fail(failed, &alive.set());
}

#[krabka_bdd::when("broker {failed} fails while no broker is known to be alive")]
fn broker_fails_unseen(failover: &mut FailoverScenario, failed: u64) {
    failover.fail(failed, &HashSet::new());
}

#[krabka_bdd::when(
    "broker {returning} returns from an unclean shutdown while brokers {alive} stay alive"
)]
fn broker_returns(failover: &mut FailoverScenario, returning: u64, alive: Brokers) {
    failover.restart(returning, &alive.set());
}

#[krabka_bdd::when(
    "broker {returning} returns from an unclean shutdown before any broker is known to be alive"
)]
fn broker_returns_unseen(failover: &mut FailoverScenario, returning: u64) {
    failover.restart(returning, &HashSet::new());
}

#[krabka_bdd::then("the controller decides to {decision}")]
fn the_decision(failover: &FailoverScenario, decision: ExpectedDecision) {
    assert!(failover.decision.get() == &decision.0);
}

krabka_bdd::scenarios!(
    "tests/features/migrated/src/leader_election/failover",
    fixtures = [failover: FailoverScenario],
);
