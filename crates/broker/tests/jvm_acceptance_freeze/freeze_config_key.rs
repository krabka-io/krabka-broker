//! What `kafka-configs` can read of a freeze, and what it cannot do to one.
//!
//! The synthesised `write.freeze` topic config is the only place an operator
//! holding nothing but the JVM tools can see the scope that froze a topic. The
//! three alter rows are the other half of that rule: the key takes no write,
//! whichever operation the tool sends.

use {
    crate::{
        control_plane::{freeze, plain_topic_fixture},
        host_broker::JvmBroker,
        jvm_tool::{ToolRun, jvm_describe_configs, run_tool},
        vocabulary::{
            CONFIGS_REASON, CONFIGS_TOPIC, INVALID_CONFIG_EXCEPTION, WRITE_FREEZE_ALTER_REFUSAL,
            write_freeze_value,
        },
    },
    assert2::check,
    krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

struct FrozenConfig {
    broker: Option<JvmBroker>,
    described: Option<ToolRun>,
    altered: Vec<(String, ToolRun)>,
    after: Option<ToolRun>,
}
#[fixture]
fn frozen_config() -> FrozenConfig {
    // Start this scenario with fresh inputs and observations.

    FrozenConfig {
        broker: None,
        described: None,
        altered: Vec::new(),
        after: None,
    }
}

#[given("a literal write freeze protects the configuration inspection topic")]
async fn protected(frozen_config: &mut FrozenConfig) {
    let (broker, client) = plain_topic_fixture(&[CONFIGS_TOPIC]).await;
    freeze(&client, CONFIGS_TOPIC, PATTERN_TYPE_LITERAL, CONFIGS_REASON).await;
    frozen_config.broker = Some(broker);
}
#[when("the operator describes the frozen topic configuration")]
fn describe(frozen_config: &mut FrozenConfig) {
    let broker = frozen_config.broker.as_ref().expect("frozen topic exists");
    frozen_config.described = Some(jvm_describe_configs(&broker.container));
}
#[then("the read succeeds and exposes the synthesized freeze scope")]
fn visible(frozen_config: &FrozenConfig) {
    let frozen = write_freeze_value(CONFIGS_TOPIC);
    let described = frozen_config
        .described
        .as_ref()
        .expect("configuration was described");
    check!(
        described.succeeded(),
        "describing a frozen topic is not gated, got {described:?}"
    );
    check!(
        described.says(&frozen),
        "the describe must show {frozen:?}, got {described:?}"
    );
}
#[when("the operator tries setting clearing and deleting the write freeze config key")]
fn alter(frozen_config: &mut FrozenConfig) {
    let broker = frozen_config.broker.as_ref().expect("frozen topic exists");
    for (label, flag, value) in [
        ("set freeze", "--add-config", "write.freeze=true"),
        ("clear freeze", "--add-config", "write.freeze=false"),
        ("delete key", "--delete-config", "write.freeze"),
    ] {
        let run = run_tool(None, None, &[
            "kafka-configs",
            "--bootstrap-server",
            &broker.container,
            "--entity-type",
            "topics",
            "--entity-name",
            CONFIGS_TOPIC,
            "--alter",
            flag,
            value,
        ]);
        frozen_config.altered.push((label.to_owned(), run));
    }
}
#[then(
    "all three config writes fail with the invalid configuration exception and freeze command guidance"
)]
fn refused(frozen_config: &FrozenConfig) {
    check!(frozen_config.altered.len() == 3);
    for (label, run) in &frozen_config.altered {
        check!(!run.succeeded(), "{label}: must exit non-zero");
        check!(
            run.says(INVALID_CONFIG_EXCEPTION),
            "{label}: must name the invalid configuration, got {run:?}"
        );
        check!(
            run.says(WRITE_FREEZE_ALTER_REFUSAL),
            "{label}: must name the command that does set the key, got {run:?}"
        );
    }
}
#[when("the operator reads the freeze scope after the rejected writes")]
fn reread(frozen_config: &mut FrozenConfig) {
    let broker = frozen_config.broker.as_ref().expect("frozen topic exists");
    frozen_config.after = Some(jvm_describe_configs(&broker.container));
}
#[then("the original freeze scope remains unchanged")]
fn unchanged(frozen_config: &FrozenConfig) {
    let after = frozen_config
        .after
        .as_ref()
        .expect("configuration was reread");
    let frozen = write_freeze_value(CONFIGS_TOPIC);
    check!(
        after.says(&frozen),
        "the refusals must have left the freeze exactly as it was, got {after:?}"
    );
}

#[ignore = "requires docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_freeze/freeze_config_key.feature",
    name = "Stock config tools can read a freeze but cannot mutate it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_configs_reads_the_freeze_and_cannot_write_it(frozen_config: FrozenConfig) {
    drop(frozen_config);
}
