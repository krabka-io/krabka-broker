//! Unit tests for the per-group `share.*` overrides of a share group's
//! session timeout, heartbeat interval and assignment interval: Kafka's
//! `GroupMetadataManager.shareGroupSessionTimeoutMs`,
//! `shareGroupHeartbeatIntervalMs` and `shareGroupAssignmentIntervalMs`.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_protocol::owned::{
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
};

use super::{
    ShareGroupActorHandle,
    test_support::{heartbeat, metadata_with_topic, seed_initialized},
};
// A metadata image that holds each group config in `overrides`: a group id
// and its `share.*` entries.
use crate::coordinator::unified::test_support::group_config_image as group_image;
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator, config::NextGenConfig, offsets_log::fake::InMemoryOffsetsLog,
        share::config::ShareGroupConfig, streams::config::StreamsGroupConfig,
        test_support::fixed_source,
    },
};

/// A coordinator over topic `t` with four partitions, whose metadata image
/// holds each group config in `overrides`: a group id and its `share.*`
/// entries. It returns the id of `t`.
fn coordinator(
    config: ShareGroupConfig,
    overrides: &[(&str, &[(&str, &str)])],
) -> (
    Arc<GroupCoordinator>,
    krabka_protocol::primitives::uuid::Uuid,
) {
    let (metadata, topic_id) = metadata_with_topic("t", 4);
    let coordinator = Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        config,
        metadata,
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ));
    coordinator.set_metadata_source(fixed_source(group_image(overrides)));
    (coordinator, topic_id)
}

/// A heartbeat is handled with the settings of the group config when it
/// arrives, not with those the actor read before it began to wait for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_runs_with_the_group_config_of_the_moment_it_arrives() {
    let (metadata, _) = metadata_with_topic("t", 4);
    let coordinator = Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
        metadata,
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ));
    let source = Arc::new(crate::test_support::FakeMetadataSource::builder().build());
    coordinator.set_metadata_source(source.clone());
    let handle = coordinator.get_or_create_share("g");
    let joined = join(&handle, "m1", 0).await;
    check!(joined.heartbeat_interval_ms == 5000);

    // The override lands while the actor waits for its next message.
    tokio::time::sleep(Duration::from_millis(100)).await;
    source.set_image(group_image(&[(
        "g",
        &[("share.heartbeat.interval.ms", "7000")],
    )]));

    assert!(
        join(&handle, "m1", joined.member_epoch)
            .await
            .heartbeat_interval_ms
            == 7000
    );
}

async fn join(
    handle: &ShareGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> ShareGroupHeartbeatResponse {
    heartbeat(
        handle,
        ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        },
    )
    .await
}

/// `heartbeat_interval_ms` of the join response of each group: the group's
/// `share.heartbeat.interval.ms`, or the broker's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reports_its_own_heartbeat_interval() {
    let (coordinator, _) = coordinator(
        ShareGroupConfig::assigning_at_once(),
        &[("tuned", &[("share.heartbeat.interval.ms", "7000")])],
    );
    let mut reported = Vec::new();
    for group_id in ["tuned", "plain"] {
        let handle = coordinator.get_or_create_share(group_id);
        reported.push((group_id, join(&handle, "m1", 0).await.heartbeat_interval_ms));
    }
    assert!(reported == [("tuned", 7000), ("plain", 5000)]);
}

/// A member of a group with a `share.session.timeout.ms` of 1 ms is expired
/// by the group's next session tick, which follows the group's own heartbeat
/// interval, while a group without an override keeps its member for the
/// broker's 45 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_expires_a_member_by_its_own_session_timeout() {
    // The broker's bounds admit the 1 ms and the 10 ms that the group asks for,
    // which the group's own overrides are capped to.
    let (coordinator, _) = coordinator(
        ShareGroupConfig {
            min_session_timeout: Duration::from_millis(1),
            min_heartbeat_interval: Duration::from_millis(10),
            ..ShareGroupConfig::assigning_at_once()
        },
        &[(
            "brief",
            &[
                ("share.session.timeout.ms", "1"),
                ("share.heartbeat.interval.ms", "10"),
            ],
        )],
    );
    let mut epochs = Vec::new();
    for group_id in ["brief", "plain"] {
        let handle = coordinator.get_or_create_share(group_id);
        let joined = join(&handle, "m1", 0).await;
        check!(joined.error_code == codes::NONE, "{group_id}");
        epochs.push((group_id, handle, joined.member_epoch));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut answers = Vec::new();
    for (group_id, handle, epoch) in &epochs {
        answers.push((*group_id, join(handle, "m1", *epoch).await.error_code));
    }
    assert!(answers == [("brief", codes::UNKNOWN_MEMBER_ID), ("plain", codes::NONE)]);
}

/// Kafka's `canComputeNextTargetAssignment`: a second member that joins
/// within the assignment interval of the first assignment joins at the epoch
/// of that assignment, and the group assigns for it when the interval has
/// elapsed. A group whose `share.assignment.interval.ms` is 0 assigns at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_assigns_no_sooner_than_its_own_assignment_interval() {
    let (coordinator, topic_id) = coordinator(
        ShareGroupConfig {
            assignment_interval: Duration::from_mins(1),
            ..ShareGroupConfig::default()
        },
        &[
            ("fast", &[("share.assignment.interval.ms", "0")]),
            ("paced", &[("share.assignment.interval.ms", "300")]),
        ],
    );
    let mut joined = Vec::new();
    let mut handles = Vec::new();
    for group_id in ["slow", "fast", "paced"] {
        let handle = coordinator.get_or_create_share(group_id);
        seed_initialized(&handle, topic_id, "t", vec![0, 1, 2, 3]).await;
        let first = join(&handle, "m1", 0).await;
        check!(first.member_epoch == 1, "{group_id}");
        joined.push((group_id, join(&handle, "m2", 0).await.member_epoch));
        handles.push((group_id, handle));
    }
    assert!(joined == [("slow", 1), ("fast", 2), ("paced", 1)]);

    // The paced group's interval elapses; the next heartbeat computes the
    // assignment that its second member was waiting for.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (_, paced) = &handles[2];
    check!(join(paced, "m1", 1).await.member_epoch == 2);
    check!(join(paced, "m2", 1).await.member_epoch == 2);
    // The slow group still waits for the broker's minute.
    let (_, slow) = &handles[0];
    check!(join(slow, "m2", 1).await.member_epoch == 1);
}
