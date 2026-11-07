//! Step definitions more than one barrier feature uses, and the scenario
//! state they share.
//!
//! Every suite that declares `mod steps;` registers all of these. The step
//! check of each `scenarios!` call only asks that each step of its own
//! features matches one definition, so a definition a suite does not use is
//! harmless.

use {
    crate::support::{CutQuery, GROUP, GroupDefinition, LazyCluster, Transcript},
    krabka_bdd::{Given, TokioMultiThreadContext},
    rstest_bdd_macros::{given, then, when},
    std::collections::BTreeSet,
};

/// The state of one barrier scenario.
pub struct BarrierScenario {
    /// The scenario's broker, started by the first step that needs it.
    pub cluster: LazyCluster,
    /// The topics a step has created, so a cut over a topic nobody named
    /// creates it once.
    pub topics: BTreeSet<String>,
    /// What every cut a Given took printed, oldest first.
    pub cuts: Vec<Transcript>,
    /// What the scenario's When printed.
    pub transcript: Given<Transcript>,
}

impl BarrierScenario {
    /// Create `topic` with `partitions` partitions, once.
    pub fn create_topic(&mut self, runtime: &TokioMultiThreadContext, topic: &str, partitions: i32) {
        let cluster = self.cluster.get(runtime);
        runtime.block_on(cluster.create_topic(topic, partitions));
        self.topics.insert(topic.to_owned());
    }

    /// Define the scenario's group, and require the broker to accept it.
    pub fn define(&mut self, runtime: &TokioMultiThreadContext, definition: GroupDefinition) {
        let cluster = self.cluster.get(runtime);
        let defined = runtime.block_on(cluster.operator().define(definition));
        assert2::assert!(defined.exit == 0, "{defined:?}");
    }

    /// Define the scenario's group over `topic`, creating the topic with one
    /// partition if no step has, and cut it `times` times.
    pub fn cut_group(
        &mut self,
        runtime: &TokioMultiThreadContext,
        topic: &str,
        retained_cuts: Option<i32>,
        times: usize,
    ) {
        if !self.topics.contains(topic) {
            self.create_topic(runtime, topic, 1);
        }
        self.define(
            runtime,
            GroupDefinition::builder()
                .topics(vec![topic.to_owned()])
                .maybe_retained_cuts(retained_cuts)
                .build(),
        );
        for _ in 0..times {
            let cluster = self.cluster.get(runtime);
            let cut = runtime.block_on(cluster.operator().trigger(GROUP));
            assert2::assert!(cut.exit == 0, "{cut:?}");
            self.cuts.push(cut);
        }
    }
}

#[rstest::fixture]
pub fn barrier() -> BarrierScenario {
    BarrierScenario {
        cluster: LazyCluster::default(),
        topics: BTreeSet::new(),
        cuts: Vec::new(),
        transcript: Given::new("transcript", "the scenario's When"),
    }
}

#[given("the {topic} topic has {partitions:i32} partitions")]
fn topic_has_partitions(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
    partitions: i32,
) {
    barrier.create_topic(runtime, &topic, partitions);
}

#[given("an operator has defined a barrier group over the {topic} topic")]
fn defined_over_one_topic(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
) {
    barrier.define(
        runtime,
        GroupDefinition::builder().topics(vec![topic]).build(),
    );
}

#[given("an operator has defined a barrier group over the {first} and {second} topics")]
fn defined_over_two_topics(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    first: String,
    second: String,
) {
    barrier.define(
        runtime,
        GroupDefinition::builder()
            .topics(vec![first, second])
            .build(),
    );
}

#[given("a barrier group over the {topic} topic has been cut once")]
fn cut_once(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
) {
    barrier.cut_group(runtime, &topic, None, 1);
}

#[given("a barrier group over the {topic} topic has been cut {times:usize} times")]
fn cut_times(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
    times: usize,
) {
    barrier.cut_group(runtime, &topic, None, times);
}

#[when("the operator triggers a cut of the group")]
fn trigger(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(cluster.operator().trigger(GROUP));
    barrier.transcript.set(transcript);
}

#[when("the operator describes the group")]
fn describe(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(cluster.operator().describe(GROUP));
    barrier.transcript.set(transcript);
}

#[when("the operator lists the cuts of the group")]
fn list(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(
        cluster
            .operator()
            .list(CutQuery::builder().build()),
    );
    barrier.transcript.set(transcript);
}

/// `RESOURCE_NOT_FOUND` (91) is what KFC-4 gives for a group that is not live.
#[then("the broker refuses the request as an unknown group")]
fn refused_as_unknown(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    assert2::check!(transcript.exit == 1, "{transcript:?}");
    assert2::check!(transcript.stdout.is_empty(), "{transcript:?}");
    assert2::check!(
        transcript.stderr.starts_with("error 91"),
        "{transcript:?}"
    );
}
