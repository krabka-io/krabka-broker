//! Streams-group membership scenarios: a lone stateless member converging on
//! the whole task assignment, and a member leaving with `member_epoch == -1`.
//!
//! Both scenarios use a stateless topology, so they exercise the membership
//! path on its own, without the internal-topic provisioning that a stateful
//! subtopology drags in.

use {
    crate::streams_harness::{
        StreamsGroupScenario, active_partition_count, active_partitions_for, follow_up,
        streams_group,
    },
    assert2::{assert, check},
    rstest_bdd_macros::{scenario, then, when},
};

#[then("the stateless streams member owns both partitions of subtopology zero")]
fn stateless_streams_assignment(streams_group: &StreamsGroupScenario) {
    let response = streams_group
        .heartbeat
        .as_ref()
        .expect("streams join answered");
    check!(
        active_partition_count(response) == 2,
        "{:?}",
        response.active_tasks
    );
    check!(active_partitions_for(response, "0") == vec![0, 1]);
}

#[when("the joined streams member leaves at epoch minus one")]
async fn leave_stateless_streams_group(streams_group: &mut StreamsGroupScenario) {
    streams_group.heartbeat = Some(
        streams_group
            .client()
            .send(
                follow_up()
                    .group(&streams_group.group)
                    .member_id(&streams_group.member)
                    .epoch(-1)
                    .call(),
            )
            .await
            .expect("leave heartbeat"),
    );
}

#[then("the streams leave heartbeat succeeds")]
fn streams_member_left(streams_group: &StreamsGroupScenario) {
    let response = streams_group.heartbeat.as_ref().expect("leave answered");
    assert!(response.error_code == 0, "{response:?}");
}

#[then("the retained streams group no longer contains the departed member")]
fn departed_streams_member_absent(streams_group: &StreamsGroupScenario) {
    let described = streams_group
        .described
        .as_ref()
        .expect("streams group described");
    assert!(described.groups.len() == 1);
    let group = &described.groups[0];
    assert!(group.error_code == 0, "{group:?}");
    assert!(
        !group
            .members
            .iter()
            .any(|member| member.member_id == streams_group.member)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_membership.feature",
    name = "stateless single member converges"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_single_member_converges(streams_group: StreamsGroupScenario) {
    streams_group.shutdown().await;
    drop(streams_group);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_membership.feature",
    name = "leave removes member"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member(streams_group: StreamsGroupScenario) {
    streams_group.shutdown().await;
    drop(streams_group);
}
