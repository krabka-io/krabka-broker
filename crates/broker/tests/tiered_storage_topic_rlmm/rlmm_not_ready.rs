//! The fail-closed test: while the topic-backed manager has not activated, the
//! copy task must tier nothing.
//!
//! The broker boots with a bootstrap address on a dead port, so the retry loop
//! never swaps the manager in and every `add_remote_log_segment_metadata` call
//! returns `NotReady`. The dead-port override keeps the manager unavailable.

use {
    crate::{
        TopicRlmmScenario,
        rlmm_cluster::{await_tiered_config, build_client, start_configured_topic_rlmm},
        rlmm_round_trip::remote_log_files,
        topic_fixture::{create_configured_topic, tiered_configs},
        topic_rlmm,
    },
    assert2::assert,
    krabka_broker::RlmmKind,
    std::time::Duration,
    tokio::time,
};

const TOPIC: &str = "tiered-not-ready-itest";

#[rstest_bdd_macros::given(
    "the topic RLMM bootstraps from dead port 127.0.0.1:1 and copy ticks every 200 milliseconds"
)]
fn unavailable_topic_rlmm(topic_rlmm: &mut TopicRlmmScenario) {
    topic_rlmm.cluster = Some(topic_rlmm.runtime.block_on(start_configured_topic_rlmm(
        |cfg, log_dir| {
            cfg.remote_log_manager_interval = krabka_units::millis(200);
            let RlmmKind::TopicBacked(metadata) = &mut cfg.remote_log_metadata else {
                unreachable!("topic-backed fixture");
            };
            metadata.bootstrap = "127.0.0.1:1".into();
            metadata.snapshot_dir = log_dir.join("rlmm-snap");
        },
    )));
    topic_rlmm.client = Some(
        topic_rlmm
            .runtime
            .block_on(build_client(topic_rlmm.broker())),
    );
    topic_rlmm.runtime.block_on(create_configured_topic(
        topic_rlmm.client(),
        TOPIC,
        tiered_configs(Some("1024")),
    ));
    topic_rlmm
        .runtime
        .block_on(await_tiered_config(topic_rlmm.broker(), TOPIC));
}

#[rstest_bdd_macros::when(
    "80 records seal several 1-KiB segments while the metadata manager remains unavailable"
)]
fn write_without_topic_rlmm(topic_rlmm: &TopicRlmmScenario) {
    topic_rlmm
        .runtime
        .block_on(topic_rlmm.broker().produce_records_for_test(TOPIC, 0, 80))
        .expect("produce records");
}

#[rstest_bdd_macros::then("after two seconds no remote log object has been uploaded")]
fn no_orphaned_tiered_objects(topic_rlmm: &TopicRlmmScenario) {
    topic_rlmm
        .runtime
        .block_on(async { time::sleep(Duration::from_secs(2)).await });
    let tiered = remote_log_files(topic_rlmm.remote_dir()).len();
    assert!(
        tiered == 0,
        "expected no tiered objects while RLMM not ready, found {tiered}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_topic_rlmm/rlmm_not_ready.feature",
    name = "copy task skips tiering while rlmm not ready"
)]
fn copy_task_skips_tiering_while_rlmm_not_ready(topic_rlmm: TopicRlmmScenario) {
    topic_rlmm.shutdown();
}
