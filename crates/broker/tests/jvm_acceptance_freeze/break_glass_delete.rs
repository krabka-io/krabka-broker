//! Stock topic administration exposes break-glass refusals without deleting the topic.
use {
    crate::{
        host_broker::{JvmBroker, gate_on, start_jvm_broker},
        jvm_tool::{ToolRun, run_tool},
    },
    assert2::check,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct ProtectedDeletion {
    broker: Option<JvmBroker>,
    topic: String,
    created: Option<ToolRun>,
    deleted: Option<ToolRun>,
    listed: Option<ToolRun>,
}
#[fixture]
fn deletion() -> ProtectedDeletion {
    // Start this scenario with fresh inputs and observations.

    ProtectedDeletion {
        broker: None,
        topic: String::new(),
        created: None,
        deleted: None,
        listed: None,
    }
}
#[given(
    "a healthy broker requires two person approval for privileged topic deletion with no approved proposals"
)]
async fn gate(deletion: &mut ProtectedDeletion) {
    deletion.broker = Some(start_jvm_broker(gate_on).await);
}
#[when("the stock topic tool creates topic {topic}")]
fn create(deletion: &mut ProtectedDeletion, topic: String) {
    let bootstrap = &deletion
        .broker
        .as_ref()
        .expect("gated broker started")
        .container;
    deletion.created = Some(run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--create",
        "--topic",
        &topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]));
    deletion.topic = topic;
}
#[then("ordinary topic creation remains successful")]
fn created(deletion: &ProtectedDeletion) {
    check!(
        deletion
            .created
            .as_ref()
            .expect("topic creation completed")
            .succeeded(),
        "topic creation must not be gated"
    );
}
#[when("the same stock tool requests deletion of that topic")]
fn delete(deletion: &mut ProtectedDeletion) {
    let bootstrap = &deletion
        .broker
        .as_ref()
        .expect("gated broker started")
        .container;
    deletion.deleted = Some(run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--delete",
        "--topic",
        &deletion.topic,
    ]));
}
#[then("the deletion exits nonzero and prints these policy observations")]
fn refused(deletion: &ProtectedDeletion, datatable: Vec<Vec<String>>) {
    let output = deletion.deleted.as_ref().expect("topic deletion completed");
    check!(!output.succeeded(), "unapproved delete must exit nonzero");
    for row in datatable.into_iter().skip(1) {
        let text = row.into_iter().next().expect("policy observation");
        check!(output.says(&text), "delete missing{text:?}: {output:?}");
    }
}
#[when("the operator lists topics after the rejected deletion")]
fn list(deletion: &mut ProtectedDeletion) {
    deletion.listed = Some(run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        &deletion
            .broker
            .as_ref()
            .expect("gated broker started")
            .container,
        "--list",
    ]));
}
#[then("topic {topic} remains visible in a successful listing")]
fn retained(deletion: &ProtectedDeletion, topic: String) {
    let output = deletion.listed.as_ref().expect("topics listed");
    check!(output.succeeded(), "listing must not be gated");
    check!(
        output.says(&topic),
        "rejected delete must leave topic in place: {output:?}"
    );
}
#[ignore = "requires docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_break_glass_tools.feature",
    name = "An unapproved stock topic deletion reports the gate and preserves the topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_topics_delete_carries_the_brokers_break_glass_refusal(deletion: ProtectedDeletion) {
    drop(deletion);
}
