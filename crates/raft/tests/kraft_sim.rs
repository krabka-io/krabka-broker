//! Deterministic acceptance scenarios for the in-memory `KRaft` consensus core.
#![cfg(test)]

mod sim_harness;

use {
    krabka_ids::NodeId,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    sim_harness::{cluster::Sim, sim_log::SimLog},
};

struct ConsensusScenario {
    sim: Option<Sim<SimLog>>,
    leader: Option<NodeId>,
    replacements: Vec<NodeId>,
    target: Option<i64>,
}

impl ConsensusScenario {
    fn sim(&mut self) -> &mut Sim<SimLog> { self.sim.as_mut().expect("three voters were given") }
}

#[fixture]
fn consensus() -> ConsensusScenario {
    // Start this scenario with fresh inputs and observations.

    ConsensusScenario {
        sim: None,
        leader: None,
        replacements: Vec::new(),
        target: None,
    }
}

#[given("three in-memory KRaft voters")]
fn three_voters(consensus: &mut ConsensusScenario) {
    consensus.sim = Some(Sim::new_with(&[NodeId(1), NodeId(2), NodeId(3)], |_id| {
        SimLog::default()
    }));
}

#[when("the voters stabilize")]
fn stabilize(consensus: &mut ConsensusScenario) {
    consensus.leader = Some(consensus.sim().stabilize_one_leader(10_000));
}

#[given("the voters have elected a leader")]
fn elected_leader(consensus: &mut ConsensusScenario) { stabilize(consensus); }

#[then("all voters agree on one leader epoch")]
fn agreed_epoch(consensus: &ConsensusScenario) {
    let sim = consensus.sim.as_ref().expect("three voters were given");
    let leader = consensus.leader.expect("the voters stabilized");
    assert2::assert!(sim.leaders() == vec![leader]);
    assert2::assert!(sim.distinct_epochs().len() == 1);
}

#[when("the leader is isolated")]
fn isolate_leader(consensus: &mut ConsensusScenario) {
    let leader = consensus.leader.expect("a leader was elected");
    consensus.sim().partition(leader);
    consensus.sim().run_until_stable(10_000);
    consensus.replacements = consensus
        .sim()
        .leaders()
        .into_iter()
        .filter(|&id| id != leader)
        .collect();
}

#[then("the majority elects exactly one replacement leader")]
fn replacement_leader(consensus: &ConsensusScenario) {
    assert2::assert!(consensus.replacements.len() == 1);
}

#[when("the partition heals")]
fn heal_partition(consensus: &mut ConsensusScenario) {
    let leader = consensus.leader.expect("a leader was elected");
    consensus.sim().heal(leader);
    consensus.sim().stabilize_one_leader(10_000);
}

#[then("the replacement remains the only leader")]
fn replacement_remains(consensus: &mut ConsensusScenario) {
    let [replacement] = consensus.replacements.as_slice() else {
        panic!("the majority did not elect exactly one replacement");
    };
    let replacement = *replacement;
    assert2::assert!(consensus.sim().leaders() == vec![replacement]);
}

#[when("the leader appends {records} records and replication settles")]
fn append_records(consensus: &mut ConsensusScenario, records: usize) {
    let leader = consensus.leader.expect("a leader was elected");
    let before = consensus.sim().log_end_offset(leader);
    consensus.sim().leader_append(leader, records);
    consensus.target =
        Some(before + i64::try_from(records).expect("record count fits in an offset"));
    consensus.sim().run_until_stable(10_000);
}

#[then("the leader commits the appended records")]
fn records_committed(consensus: &mut ConsensusScenario) {
    let leader = consensus.leader.expect("a leader was elected");
    let target = consensus.target.expect("records were appended");
    assert2::assert!(consensus.sim().leader_high_watermark(leader) >= target);
}

#[then("all voters fetch through the committed high watermark")]
fn voters_caught_up(consensus: &mut ConsensusScenario) {
    let leader = consensus.leader.expect("a leader was elected");
    let watermark = consensus.sim().leader_high_watermark(leader);
    assert2::assert!(consensus.sim().all_voters_fetched_to(watermark));
}

scenarios!("tests/features/kraft_sim.feature", fixtures = [consensus: ConsensusScenario]);
