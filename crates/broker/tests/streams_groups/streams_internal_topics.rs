//! The internal-topic provisioning scenario: a stateful subtopology makes the
//! broker auto-create the state-changelog topic it declares.
//!
//! The scenario has its own heartbeat loop rather than reusing the shared
//! `join_and_converge`, because it must repeat the topology on every follow-up
//! heartbeat and must wait for the `MISSING_INTERNAL_TOPICS` status to clear as
//! well as for the active task to land.

use {
    crate::{
        streams_harness::{
            StreamsGroupScenario, active_partitions_for, first_join, follow_up, status_codes,
            streams_group, topology,
        },
        support::streams::request_active_tasks,
    },
    assert2::assert,
    krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::Duration,
    tokio::time,
};

#[given("the stateful streams topology declares app-store-changelog with derived partition count")]
fn stateful_streams_topology(streams_group: &mut StreamsGroupScenario) {
    streams_group.topology = Some(topology(&streams_group.topic, vec![TopicInfo {
        name: "app-store-changelog".into(),
        partitions: 0,
        replication_factor: 1,
        topic_configs: vec![],
        ..Default::default()
    }]));
}

#[when("the stateful member joins and retries heartbeats while internal topics are provisioned")]
async fn provision_streams_topics(streams_group: &mut StreamsGroupScenario) {
    let topology = streams_group.topology.as_ref().expect("stateful topology");
    let mut response = streams_group
        .client()
        .send(first_join(&streams_group.group, topology.clone()))
        .await
        .expect("first heartbeat");
    let mut member = response.member_id.clone();
    for _ in 0..15 {
        if response.error_code == 14 {
            response = streams_group
                .client()
                .send(first_join(&streams_group.group, topology.clone()))
                .await
                .expect("retry first heartbeat");
            member = response.member_id.clone();
            continue;
        }
        assert!(response.error_code == 0, "heartbeat error: {response:?}");
        if active_partitions_for(&response, "0") == vec![0] && !status_codes(&response).contains(&3)
        {
            break;
        }
        time::sleep(Duration::from_millis(300)).await;
        let request = follow_up()
            .group(&streams_group.group)
            .member_id(&member)
            .epoch(response.member_epoch)
            .maybe_active(request_active_tasks(&response))
            .call();
        response = streams_group
            .client()
            .send(request)
            .await
            .expect("follow-up heartbeat");
        member = response.member_id.clone();
    }
    streams_group.member = member;
    streams_group.heartbeat = Some(response);
}

#[then("the stateful member owns task zero without MISSING_INTERNAL_TOPICS")]
fn stateful_streams_converged(streams_group: &StreamsGroupScenario) {
    let response = streams_group
        .heartbeat
        .as_ref()
        .expect("stateful heartbeats finished");
    assert!(
        active_partitions_for(response, "0") == vec![0] && !status_codes(response).contains(&3),
        "member never converged: {response:?}"
    );
    assert!(!status_codes(response).contains(&3));
}

#[then("the controller image contains app-store-changelog with one partition")]
fn provisioned_streams_changelog(streams_group: &StreamsGroupScenario) {
    let image = streams_group
        .broker
        .as_ref()
        .expect("streams broker")
        .controller_image_for_test();
    let changelog = image
        .topic("app-store-changelog")
        .expect("changelog auto-created");
    assert!(changelog.partitions == 1);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_internal_topics.feature",
    name = "stateful member triggers internal topic creation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateful_member_triggers_internal_topic_creation(streams_group: StreamsGroupScenario) {
    streams_group.shutdown().await;
    drop(streams_group);
}
