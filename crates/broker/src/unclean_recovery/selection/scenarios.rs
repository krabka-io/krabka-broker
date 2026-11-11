//! Step definitions for `tests/features/unclean_recovery_election`: which
//! responder [`select_leader`] elects, and by which rule.

use {
    super::{Election, ElectionBasis, ReplicaLogInfo, select_leader},
    crate::leader_election::test_support::Brokers,
    assert2::assert,
    krabka_bdd::{Given, table},
    krabka_raft::NodeId,
    std::collections::HashSet,
};

/// The rule a scenario expects to elect the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Basis {
    #[step_param("an in-sync replica")]
    InSyncReplica,
    #[step_param("an eligible leader replica")]
    EligibleLeaderReplica,
    #[step_param("the most complete log")]
    MostCompleteLog,
}

impl From<Basis> for ElectionBasis {
    fn from(basis: Basis) -> Self {
        match basis {
            Basis::InSyncReplica => Self::InSyncReplica,
            Basis::EligibleLeaderReplica => Self::EligibleLeaderReplica,
            Basis::MostCompleteLog => Self::MostCompleteLog,
        }
    }
}

/// One row of a recovery-poll table: a replica's reported log state.
#[derive(Debug, krabka_bdd::TableRow)]
struct PolledReplica {
    broker: u64,
    #[column("last epoch")]
    last_epoch: i32,
    #[column("log end")]
    log_end: i64,
}

/// The poll's answers, the partition's ISR and ELR, and the election.
struct RecoveryElection {
    responses: Vec<ReplicaLogInfo>,
    in_sync: Vec<NodeId>,
    eligible: Vec<i32>,
    witnesses: HashSet<NodeId>,
    elected: Given<Option<Election>>,
}

#[rstest::fixture]
fn recovery_election() -> RecoveryElection {
    RecoveryElection {
        responses: Vec::new(),
        in_sync: Vec::new(),
        eligible: Vec::new(),
        witnesses: HashSet::new(),
        elected: Given::new(
            "elected",
            "When the controller elects a leader for the recovery",
        ),
    }
}

/// A replica's answer to the poll, written in its last epoch.
fn answer(broker: u64, last_epoch: i32, log_end: i64) -> ReplicaLogInfo {
    ReplicaLogInfo {
        broker_id: NodeId(broker),
        last_written_leader_epoch: last_epoch,
        log_end_offset: log_end,
        current_leader_epoch: last_epoch,
    }
}

#[krabka_bdd::given("these replicas answered the recovery poll")]
fn these_replicas_answered(recovery_election: &mut RecoveryElection, datatable: Vec<Vec<String>>) {
    let rows: Vec<PolledReplica> = table::rows(&datatable).expect("recovery poll table");
    recovery_election.responses = rows
        .into_iter()
        .map(|row| answer(row.broker, row.last_epoch, row.log_end))
        .collect();
}

#[krabka_bdd::given("replicas 1, 2, 3 answered the recovery poll with log ends 40, 400, 20")]
fn three_replicas_answered(recovery_election: &mut RecoveryElection) {
    recovery_election.responses = vec![answer(1, 5, 40), answer(2, 5, 400), answer(3, 5, 20)];
}

#[krabka_bdd::given("the partition names ISR {isr} with eligible leader replicas {eligible}")]
fn the_partition_names(recovery_election: &mut RecoveryElection, isr: Brokers, eligible: Brokers) {
    recovery_election.in_sync = isr.nodes();
    recovery_election.eligible = eligible.elr_ids();
}

#[krabka_bdd::given("brokers {witnesses} are witnesses")]
fn these_witnesses(recovery_election: &mut RecoveryElection, witnesses: Brokers) {
    recovery_election.witnesses = witnesses.set();
}

#[krabka_bdd::when("the controller elects a leader for the recovery")]
fn elect(recovery_election: &mut RecoveryElection) {
    let elected = select_leader(
        &recovery_election.responses,
        &recovery_election.in_sync,
        &recovery_election.eligible,
        &recovery_election.witnesses,
    );
    recovery_election.elected.set(elected);
}

#[krabka_bdd::then("broker {leader} is elected as {basis}")]
fn elected_as(recovery_election: &RecoveryElection, leader: u64, basis: Basis) {
    let expected = Election {
        leader: NodeId(leader),
        basis: basis.into(),
    };
    assert!(recovery_election.elected.get() == &Some(expected));
}

#[krabka_bdd::then("no leader is elected")]
fn nobody_elected(recovery_election: &RecoveryElection) {
    assert!(recovery_election.elected.get() == &None);
}

krabka_bdd::scenarios!(
    "tests/features/unclean_recovery_election",
    fixtures = [recovery_election: RecoveryElection],
);
