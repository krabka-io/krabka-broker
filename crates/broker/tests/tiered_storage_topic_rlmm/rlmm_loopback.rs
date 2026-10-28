//! The two PLAINTEXT loopback tests: the topic-backed manager activates
//! against the broker's own listener, and a sealed segment copied through it
//! reads back at offset 0.
//!
//! Both boot the plain `start_broker_with_topic_rlmm` cluster, so they sit
//! together and away from the `SASL_PLAINTEXT` and fail-closed variants.

use {
    crate::{
        TopicRlmmScenario,
        rlmm_cluster::{await_activation, build_client, start_broker_with_topic_rlmm},
        rlmm_round_trip::{await_archived_round_trip, prepare_round_trip, read_first_value},
        topic_rlmm,
    },
    assert2::assert,
};

const METADATA_TOPIC: &str = "__remote_log_metadata";

#[rstest_bdd_macros::given(
    "a topic-backed metadata manager bootstraps against its broker's plaintext loopback listener"
)]
fn plaintext_loopback_manager(topic_rlmm: &mut TopicRlmmScenario) {
    topic_rlmm.cluster = Some(topic_rlmm.runtime.block_on(start_broker_with_topic_rlmm()));
}

#[rstest_bdd_macros::when("the topic-backed manager finishes activation")]
fn activate_loopback_manager(topic_rlmm: &TopicRlmmScenario) {
    topic_rlmm
        .runtime
        .block_on(await_activation(topic_rlmm.broker()));
}

#[rstest_bdd_macros::then("the broker hosts __remote_log_metadata partition 0")]
fn loopback_metadata_partition(topic_rlmm: &TopicRlmmScenario) {
    assert!(
        topic_rlmm.broker().has_partition(METADATA_TOPIC, 0),
        "__remote_log_metadata-0 should be hosted after bootstrap"
    );
}

#[rstest_bdd_macros::when(
    "80 records are appended to {topic} with 1-KiB segments and one byte local retention"
)]
fn append_loopback_records(topic_rlmm: &mut TopicRlmmScenario, topic: String) {
    topic_rlmm.topic = topic;
    if topic_rlmm.client.is_none() {
        topic_rlmm.client = Some(
            topic_rlmm
                .runtime
                .block_on(build_client(topic_rlmm.broker())),
        );
    }
    topic_rlmm.runtime.block_on(prepare_round_trip(
        topic_rlmm.broker(),
        topic_rlmm.client(),
        &topic_rlmm.topic,
    ));
}

#[rstest_bdd_macros::then("at least one completed segment is present in the remote archive")]
fn loopback_archived_records(topic_rlmm: &TopicRlmmScenario) {
    topic_rlmm
        .runtime
        .block_on(await_archived_round_trip(topic_rlmm.remote_dir()));
}

#[rstest_bdd_macros::when("Fetch reads offset 0 through the topic-backed metadata manager")]
fn fetch_loopback_records(topic_rlmm: &TopicRlmmScenario) {
    topic_rlmm.first_value.set(
        topic_rlmm
            .runtime
            .block_on(read_first_value(topic_rlmm.client(), &topic_rlmm.topic)),
    );
}

#[rstest_bdd_macros::then("the first fetched record value is test-record-0")]
fn loopback_first_record(topic_rlmm: &TopicRlmmScenario) {
    assert!(
        topic_rlmm
            .first_value
            .get()
            .expect("offset zero fetched")
            .as_deref()
            == Some(b"test-record-0".as_slice()),
        "offset 0 should read back the first produced record"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_topic_rlmm/rlmm_loopback.feature",
    name = "topic rlmm activates against loopback"
)]
fn topic_rlmm_activates_against_loopback(topic_rlmm: TopicRlmmScenario) { topic_rlmm.shutdown(); }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_topic_rlmm/rlmm_loopback.feature",
    name = "topic rlmm copy then fetch round trip"
)]
fn topic_rlmm_copy_then_fetch_round_trip(topic_rlmm: TopicRlmmScenario) { topic_rlmm.shutdown(); }
