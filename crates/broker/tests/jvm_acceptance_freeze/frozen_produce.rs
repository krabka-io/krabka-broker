//! What a stock `kafka-console-producer` makes of a topic that a write freeze
//! covers, and of one that no freeze covers.
//!
//! The one case here walks three topics through the same tool: a topic frozen
//! by its own name, a topic frozen by the namespace above it, and the unfrozen
//! control topic that keeps the other two rows honest.

use {
    crate::{
        control_plane::{freeze, plain_topic_fixture},
        host_broker::JvmBroker,
        jvm_tool::{ToolRun, jvm_produce},
        vocabulary::POLICY_VIOLATION_EXCEPTION,
    },
    assert2::check,
    krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

struct FrozenWrites {
    broker: Option<JvmBroker>,
    topics: Vec<String>,
    runs: Vec<(String, ToolRun)>,
}
#[fixture]
fn writes() -> FrozenWrites {
    // Start this scenario with fresh inputs and observations.

    FrozenWrites {
        broker: None,
        topics: Vec::new(),
        runs: Vec::new(),
    }
}
#[given("these existing topics have the following write-freeze policies")]
async fn freezes(writes: &mut FrozenWrites, datatable: Vec<Vec<String>>) {
    let policies: Vec<[String; 4]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("topic pattern scope reason"))
        .collect();
    let topics: Vec<&str> = policies
        .iter()
        .map(|[topic, _, _, _]| topic.as_str())
        .collect();
    let (broker, client) = plain_topic_fixture(&topics).await;
    for [_, pattern, scope, reason] in &policies {
        let pattern = match pattern.as_str() {
            "literal" => PATTERN_TYPE_LITERAL,
            "prefixed" => PATTERN_TYPE_PREFIXED,
            "none" => continue,
            _ => panic!("freeze pattern"),
        };
        freeze(&client, scope, pattern, reason).await;
    }
    writes.topics = policies.into_iter().map(|[topic, _, _, _]| topic).collect();
    writes.broker = Some(broker);
}
#[when("the stock synchronous producer writes value {value} to each topic")]
fn produce(writes: &mut FrozenWrites, value: String) {
    let broker = writes.broker.as_ref().expect("freeze topology exists");
    for topic in &writes.topics {
        writes.runs.push((
            topic.clone(),
            jvm_produce(&broker.container, topic, None, &value),
        ));
    }
}
#[then("each producer exits and reports these exact freeze observations")]
fn outcomes(writes: &FrozenWrites, datatable: Vec<Vec<String>>) {
    let expected: Vec<[String; 3]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("topic outcome message"))
        .collect();
    check!(writes.runs.len() == expected.len());
    for ((topic, run), [expected_topic, outcome, message]) in writes.runs.iter().zip(expected) {
        check!(*topic == expected_topic);
        match outcome.as_str() {
            "refused" => {
                check!(!run.succeeded());
                check!(run.says(POLICY_VIOLATION_EXCEPTION));
                check!(run.says(&message), "{run:?}");
            }
            "accepted" => {
                check!(run.succeeded());
                check!(!run.says(POLICY_VIOLATION_EXCEPTION));
            }
            _ => panic!("accepted or refused"),
        }
    }
}

#[ignore = "requires docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_freeze/frozen_produce.feature",
    name = "Synchronous producers distinguish literal prefix and absent freezes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_jvm_console_producer_reads_a_freeze_as_a_policy_violation(writes: FrozenWrites) {
    drop(writes);
}
