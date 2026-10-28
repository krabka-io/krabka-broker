//! Freeze scopes reject writes without affecting a neighboring topic.

use {
    crate::{
        control_plane::freeze_scope,
        support::{self, InProcess},
        wire::{ProduceOutcome, accepted, create_topic, produce_outcome, refused},
    },
    assert2::check,
    krabka_protocol::{
        krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::collections::HashMap,
};

#[derive(Default)]
struct FreezeWrites {
    broker: Option<InProcess>,
    topics: HashMap<String, Uuid>,
    writes: HashMap<String, ProduceOutcome>,
    pattern: String,
    scope: String,
    reason: String,
}

impl FreezeWrites {
    fn broker(&self) -> &InProcess { self.broker.as_ref().expect("the freeze broker is running") }

    async fn shutdown(self) {
        self.broker
            .expect("the freeze broker is running")
            .broker
            .shutdown()
            .await;
    }

    async fn create(&mut self, name: String) {
        let id = create_topic(&self.broker().broker, &self.broker().client, &name).await;
        self.topics.insert(name, id);
    }
}

#[fixture]
fn freeze_writes() -> FreezeWrites {
    // Each scenario owns fresh services and observations.
    FreezeWrites {
        broker: None,
        ..Default::default()
    }
}

#[given("a broker with unfrozen topics {topics}")]
async fn unfrozen_topics(freeze_writes: &mut FreezeWrites, topics: String) {
    freeze_writes.broker = Some(support::start().await);
    for name in topics.split(',') {
        freeze_writes.create(name.to_owned()).await;
    }
}

#[when("the operator freezes {pattern} scope {scope} because {reason}")]
async fn freeze_topic_scope(
    freeze_writes: &mut FreezeWrites,
    pattern: String,
    scope: String,
    reason: String,
) {
    let pattern_type = match pattern.as_str() {
        "literal" => PATTERN_TYPE_LITERAL,
        "prefixed" => PATTERN_TYPE_PREFIXED,
        _ => panic!("unsupported freeze pattern {pattern}"),
    };
    freeze_scope(
        &freeze_writes.broker().client,
        pattern_type,
        &scope,
        &reason,
    )
    .await;
    freeze_writes.pattern = pattern;
    freeze_writes.scope = scope;
    freeze_writes.reason = reason;
}

#[when("the client writes one record to {topic}")]
async fn write_topic(freeze_writes: &mut FreezeWrites, topic: String) {
    let id = freeze_writes.topics[&topic];
    let result = produce_outcome(
        &freeze_writes.broker().broker,
        &freeze_writes.broker().client,
        &topic,
        id,
    )
    .await;
    freeze_writes.writes.insert(topic, result);
}

#[when("topic {topic} is created after the freeze")]
async fn late_topic(freeze_writes: &mut FreezeWrites, topic: String) {
    freeze_writes.create(topic).await;
}

#[then("the write to {topic} is accepted and its log end is {offset}")]
fn write_accepted(freeze_writes: &FreezeWrites, topic: String, offset: i64) {
    check!(
        freeze_writes
            .writes
            .get(&topic)
            .expect("the client wrote to the topic")
            == &accepted(offset)
    );
}

#[then("the write to {topic} is refused and its log end stays {offset}")]
fn write_refused(freeze_writes: &FreezeWrites, topic: String, offset: i64) {
    check!(
        freeze_writes
            .writes
            .get(&topic)
            .expect("the client wrote to the topic")
            == &refused(
                &freeze_writes.pattern,
                &freeze_writes.scope,
                &freeze_writes.reason,
                offset
            )
    );
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/produce_gate.feature",
    name = "a literal freeze refuses produce and the control topic still accepts"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_literal_freeze_refuses_produce_and_the_control_topic_still_accepts(
    freeze_writes: FreezeWrites,
) {
    freeze_writes.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/produce_gate.feature",
    name = "a prefix freeze refuses produce to every topic it covers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prefix_freeze_refuses_produce_to_every_topic_it_covers(freeze_writes: FreezeWrites) {
    freeze_writes.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/produce_gate.feature",
    name = "a topic created after a covering prefix freeze is frozen on arrival"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_created_after_a_covering_prefix_freeze_is_frozen_on_arrival(
    freeze_writes: FreezeWrites,
) {
    freeze_writes.shutdown().await;
}
