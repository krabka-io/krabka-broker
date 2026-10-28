//! The cold downgrade itself: a drained streams group flips to classic on a
//! classic `JoinGroup` and keeps its committed offset, both in the live broker
//! and across a restart that replays the group from the log.

use {
    crate::{
        ERR_NONE,
        downgrade_classic_join::classic_join_sync,
        downgrade_harness::{
            DowngradeScenario, commit_offset_as_member, downgrade_case, rejoin_config,
        },
        downgrade_streams_join::streams_leave,
        support::{
            client::start_client,
            offsets::{offset_fetch_group, offset_fetch_request, offset_fetch_topic},
        },
    },
    assert2::{assert, check},
    krabka_broker::coordinator::unified::GroupType,
    krabka_protocol::owned::offset_fetch_response::OffsetFetchResponse,
    rstest::fixture,
    rstest_bdd_macros::{scenario, then, when},
    std::{sync::Arc, time::Duration},
    tokio::{time, time::error::Elapsed},
};

#[derive(Default)]
struct DowngradeOffsets {
    empty_wait: Option<Result<(), Elapsed>>,
    fetched: Option<OffsetFetchResponse>,
}

#[fixture]
fn downgrade_offsets() -> DowngradeOffsets {
    // Store timeout and offset-fetch results as actual observations of the live group.
    DowngradeOffsets::default()
}

#[when("the empty-group waiter observes the live streams group for 75 milliseconds")]
async fn observe_live_downgrade_group(
    downgrade_case: &DowngradeScenario,
    downgrade_offsets: &mut DowngradeOffsets,
) {
    downgrade_offsets.empty_wait = Some(
        time::timeout(
            Duration::from_millis(75),
            downgrade_case
                .broker()
                .wait_until_streams_group_empty(&downgrade_case.group),
        )
        .await,
    );
}

#[then("the empty-group waiter times out while the streams member remains live")]
fn occupied_downgrade_group(downgrade_offsets: &DowngradeOffsets) {
    check!(
        downgrade_offsets
            .empty_wait
            .as_ref()
            .expect("empty waiter observed")
            .is_err()
    );
}

#[when("the streams downgrade member commits input offset forty-two")]
async fn commit_streams_downgrade_offset(downgrade_case: &DowngradeScenario) {
    commit_offset_as_member(
        downgrade_case.streams(),
        &downgrade_case.group,
        (&downgrade_case.member, downgrade_case.epoch),
        &downgrade_case.topic,
        downgrade_case.topic_id.expect("topic identity"),
        0,
        42,
    )
    .await;
}

#[when("the streams member leaves and the downgrade group drains")]
async fn drain_streams_downgrade(downgrade_case: &DowngradeScenario) {
    streams_leave(
        downgrade_case.streams(),
        &downgrade_case.group,
        &downgrade_case.member,
    )
    .await;
    downgrade_case
        .broker()
        .wait_until_streams_group_empty(&downgrade_case.group)
        .await;
}

#[when("a classic JoinGroup synchronizes the drained streams group")]
async fn convert_streams_downgrade(downgrade_case: &DowngradeScenario) {
    classic_join_sync(downgrade_case.classic(), &downgrade_case.group).await;
}

#[then("the drained group becomes Classic-typed")]
async fn converted_streams_downgrade(downgrade_case: &DowngradeScenario) {
    downgrade_case
        .broker()
        .wait_until_group_type(&downgrade_case.group, GroupType::Classic)
        .await;
    assert!(
        downgrade_case
            .broker()
            .group_type_for_test(&downgrade_case.group)
            == Some(GroupType::Classic)
    );
}

#[when("the classic client fetches the downgraded group's input offset")]
async fn read_streams_downgrade_offset(
    downgrade_case: &DowngradeScenario,
    downgrade_offsets: &mut DowngradeOffsets,
) {
    downgrade_offsets.fetched = Some(
        downgrade_case
            .classic()
            .send(offset_fetch_request(offset_fetch_group(
                &downgrade_case.group,
                Some(vec![offset_fetch_topic(
                    &downgrade_case.topic,
                    downgrade_case.topic_id.expect("topic identity"),
                    vec![0],
                )]),
            )))
            .await
            .expect("OffsetFetch"),
    );
}

#[then("the downgraded group's committed offset is forty-two without a partition error")]
fn retained_streams_downgrade_offset(downgrade_offsets: &DowngradeOffsets) {
    let partition = &downgrade_offsets
        .fetched
        .as_ref()
        .expect("offset fetched")
        .groups[0]
        .topics[0]
        .partitions[0];
    assert!(partition.error_code == ERR_NONE, "{partition:?}");
    assert!(partition.committed_offset == 42);
}

#[when("the downgraded broker restarts on the same data directory")]
async fn restart_streams_downgrade(downgrade_case: &mut DowngradeScenario) {
    downgrade_case
        .broker
        .take()
        .expect("first downgrade broker")
        .shutdown()
        .await;
    let (broker, _, client) = start_client(
        rejoin_config(
            downgrade_case
                .directory
                .as_ref()
                .expect("durable directory")
                .path()
                .to_path_buf(),
        ),
        Some("c1"),
    )
    .await;
    downgrade_case.broker = Some(broker);
    downgrade_case.classic = Some(Arc::new(client));
}

#[then("replay seeds a classic actor and does not restore the group as Streams")]
async fn replayed_streams_downgrade(downgrade_case: &DowngradeScenario) {
    assert!(
        downgrade_case
            .broker()
            .classic_group_inspect_for_test(&downgrade_case.group)
            .await
            .is_some()
    );
    assert!(
        downgrade_case
            .broker()
            .group_type_for_test(&downgrade_case.group)
            != Some(GroupType::Streams)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_downgrade/downgrade_conversion.feature",
    name = "drained streams group downgrades and preserves offsets"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_streams_group_downgrades_and_preserves_offsets(
    downgrade_case: DowngradeScenario,
    downgrade_offsets: DowngradeOffsets,
) {
    let _ = downgrade_offsets;
    downgrade_case.shutdown().await;
    drop(downgrade_case);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_downgrade/downgrade_conversion.feature",
    name = "downgrade survives restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgrade_survives_restart(
    downgrade_case: DowngradeScenario,
    downgrade_offsets: DowngradeOffsets,
) {
    let _ = downgrade_offsets;
    downgrade_case.shutdown().await;
    drop(downgrade_case);
}
