//! Step definitions more than one barrier feature uses, and the scenario
//! state they share.
//!
//! Every suite that declares `mod steps;` registers all of these. The step
//! check of each `scenarios!` call only asks that each step of its own
//! features matches one definition, so a definition a suite does not use is
//! harmless.

use {
    crate::support::{CutQuery, GROUP, GroupDefinition, LazyCluster, Transcript},
    krabka_bdd::Given,
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
    pub async fn create_topic(&mut self, topic: &str, partitions: i32) {
        let cluster = self.cluster.get().await;
        cluster.create_topic(topic, partitions).await;
        self.topics.insert(topic.to_owned());
    }

    /// Define the scenario's group, and require the broker to accept it.
    pub async fn define(&mut self, definition: GroupDefinition) {
        let cluster = self.cluster.get().await;
        let defined = cluster.operator().define(definition).await;
        assert2::assert!(defined.exit == 0, "{defined:?}");
    }

    /// Define the scenario's group over `topic`, creating the topic with one
    /// partition if no step has, and cut it `times` times.
    pub async fn cut_group(&mut self, topic: &str, retained_cuts: Option<i32>, times: usize) {
        if !self.topics.contains(topic) {
            self.create_topic(topic, 1).await;
        }
        self.define(
            GroupDefinition::builder()
                .topics(vec![topic.to_owned()])
                .maybe_retained_cuts(retained_cuts)
                .build(),
        )
        .await;
        for _ in 0..times {
            let cluster = self.cluster.get().await;
            let cut = cluster.operator().trigger(GROUP).await;
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

#[given("the {topic} topic has {partitions} partitions")]
async fn topic_has_partitions(barrier: &mut BarrierScenario, topic: String, partitions: i32) {
    barrier.create_topic(&topic, partitions).await;
}

#[given("an operator has defined a barrier group over the {topic} topic")]
async fn defined_over_one_topic(barrier: &mut BarrierScenario, topic: String) {
    barrier
        .define(GroupDefinition::builder().topics(vec![topic]).build())
        .await;
}

#[given("an operator has defined a barrier group over the {first} and {second} topics")]
async fn defined_over_two_topics(barrier: &mut BarrierScenario, first: String, second: String) {
    barrier
        .define(
            GroupDefinition::builder()
                .topics(vec![first, second])
                .build(),
        )
        .await;
}

#[given("a barrier group over the {topic} topic has been cut once")]
async fn cut_once(barrier: &mut BarrierScenario, topic: String) {
    barrier.cut_group(&topic, None, 1).await;
}

#[given("a barrier group over the {topic} topic has been cut {times} times")]
async fn cut_times(barrier: &mut BarrierScenario, topic: String, times: usize) {
    barrier.cut_group(&topic, None, times).await;
}

#[when("the operator triggers a cut of the group")]
async fn trigger(barrier: &mut BarrierScenario) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().trigger(GROUP).await;
    barrier.transcript.set(transcript);
}

#[when("the operator describes the group")]
async fn describe(barrier: &mut BarrierScenario) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().describe(GROUP).await;
    barrier.transcript.set(transcript);
}

#[when("the operator lists the cuts of the group")]
async fn list(barrier: &mut BarrierScenario) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().list(CutQuery::builder().build()).await;
    barrier.transcript.set(transcript);
}

/// `RESOURCE_NOT_FOUND` (91) is what KFC-4 gives for a group that is not live.
#[then("the broker refuses the request as an unknown group")]
fn refused_as_unknown(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    assert2::check!(transcript.exit == 1, "{transcript:?}");
    assert2::check!(transcript.stdout.is_empty(), "{transcript:?}");
    assert2::check!(transcript.stderr.starts_with("error 91"), "{transcript:?}");
}

#[then("the tool refuses the command line")]
fn command_line_refused(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    assert2::check!(transcript.command_line_refused, "{transcript:?}");
    assert2::check!(transcript.exit != 0, "{transcript:?}");
    assert2::check!(transcript.stdout.is_empty(), "{transcript:?}");
}
