//! The cold-upgrade scenario: a drained classic group flips to a streams group
//! in place when a `StreamsGroupHeartbeat` arrives, and its committed offsets
//! survive the flip.
//!
//! The scenario runs the full classic lifecycle first (join, commit, leave), so
//! it is kept apart from the rejection scenario that only needs a live member.

use {
    crate::{
        support::offsets::{offset_commit_partition, offset_commit_topic},
        upgrade_classic::classic_join_sync,
        upgrade_harness::{ERR_NONE, UpgradeScenario, assert_committed_offset, upgrade_case},
        upgrade_streams::{streams_join_and_converge, topology},
    },
    assert2::assert,
    krabka_broker::coordinator::unified::GroupType,
    krabka_protocol::owned::{
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        offset_commit_request::OffsetCommitRequest,
    },
    rstest_bdd_macros::{scenario, then, when},
};

#[when("a classic member joins and synchronizes the upgrade group")]
async fn join_classic_upgrade(upgrade_case: &mut UpgradeScenario) {
    let (member, generation) = classic_join_sync(upgrade_case.classic(), &upgrade_case.group).await;
    upgrade_case.member = member;
    upgrade_case.generation = generation;
}

#[when("the classic upgrade member commits input offset forty-two")]
async fn commit_classic_upgrade(upgrade_case: &mut UpgradeScenario) {
    let mut partition = offset_commit_partition(0, 42, Some(String::new()));
    partition.committed_leader_epoch = 0;
    let response = upgrade_case
        .classic()
        .send(OffsetCommitRequest {
            group_id: upgrade_case.group.clone(),
            generation_id_or_member_epoch: upgrade_case.generation,
            member_id: upgrade_case.member.clone(),
            topics: vec![offset_commit_topic(
                &upgrade_case.topic,
                upgrade_case.topic_id.expect("topic identity"),
                vec![partition],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    upgrade_case.committed = Some(response.topics[0].partitions[0].error_code);
}

#[then("the classic upgrade offset commit succeeds")]
fn classic_upgrade_committed(upgrade_case: &UpgradeScenario) {
    assert!(upgrade_case.committed == Some(ERR_NONE));
}

#[when("the classic member leaves the upgrade group")]
async fn drain_classic_upgrade(upgrade_case: &mut UpgradeScenario) {
    let response = upgrade_case
        .classic()
        .send(LeaveGroupRequest {
            group_id: upgrade_case.group.clone(),
            member_id: upgrade_case.member.clone(),
            members: vec![MemberIdentity {
                member_id: upgrade_case.member.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    upgrade_case.left = Some(response.error_code);
}

#[then("the classic upgrade member leaves successfully")]
fn classic_upgrade_left(upgrade_case: &UpgradeScenario) {
    assert!(upgrade_case.left == Some(ERR_NONE));
}

#[when("a streams member joins the drained classic group and converges")]
async fn convert_classic_upgrade(upgrade_case: &mut UpgradeScenario) {
    upgrade_case.response = Some(
        streams_join_and_converge(
            upgrade_case.streams(),
            &upgrade_case.group,
            topology(&upgrade_case.topic),
            1,
            15,
        )
        .await
        .1,
    );
}

#[then("the converted group accepts the streams heartbeat and becomes Streams-typed")]
async fn converted_classic_upgrade(upgrade_case: &UpgradeScenario) {
    let response = upgrade_case
        .response
        .as_ref()
        .expect("streams conversion answered");
    assert!(response.error_code == ERR_NONE, "{response:?}");
    upgrade_case
        .broker()
        .wait_until_group_type(&upgrade_case.group, GroupType::Streams)
        .await;
    assert!(
        upgrade_case
            .broker()
            .group_type_for_test(&upgrade_case.group)
            == Some(GroupType::Streams)
    );
}

#[then("OffsetFetch through the streams connection still returns input offset forty-two")]
async fn retained_classic_upgrade_offsets(upgrade_case: &UpgradeScenario) {
    assert_committed_offset(
        upgrade_case.streams(),
        upgrade_case.topic_id.expect("topic identity"),
        42,
    )
    .await;
}

#[scenario(
    path = "tests/features/migrated/tests/streams_classic_upgrade/upgrade_conversion.feature",
    name = "drained classic group converts and preserves offsets"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_classic_group_converts_and_preserves_offsets(upgrade_case: UpgradeScenario) {
    upgrade_case.shutdown().await;
    drop(upgrade_case);
}
