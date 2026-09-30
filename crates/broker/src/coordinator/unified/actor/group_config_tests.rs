//! Unit tests for the per-group `consumer.*` overrides of a consumer group's
//! session timeout, heartbeat interval and assignment interval: Kafka's
//! `GroupMetadataManager.consumerGroupSessionTimeoutMs`,
//! `consumerGroupHeartbeatIntervalMs` and `consumerGroupAssignmentIntervalMs`.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_protocol::{
    owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    },
    primitives::uuid::Uuid,
};

use super::{GroupActorHandle, GroupActorMessage, test_support::StaticMetadata};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator, config::NextGenConfig, offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput, share::config::ShareGroupConfig,
        streams::config::StreamsGroupConfig, test_support::fixed_source,
    },
};

/// A coordinator over topic `t` with two partitions, whose metadata image
/// holds each group config in `overrides`: a group id and its `consumer.*`
/// entries.
fn coordinator(
    config: NextGenConfig,
    overrides: &[(&str, &[(&str, &str)])],
) -> Arc<GroupCoordinator> {
    let topic_id = Uuid([7; 16]);
    let metadata = Arc::new(StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_owned(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    });
    let coordinator = Arc::new(GroupCoordinator::new(
        config,
        ShareGroupConfig::assigning_at_once(),
        metadata,
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ));
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for (group_id, entries) in overrides {
        image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: (*group_id).to_owned(),
                configs: entries
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
            },
        ));
    }
    coordinator.set_metadata_source(fixed_source(image));
    coordinator
}

async fn heartbeat(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> ConsumerGroupHeartbeatResponse {
    let (reply, response) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                subscribed_topic_names: (member_epoch == 0).then(|| vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                topic_partitions: (member_epoch == 0).then(Vec::new),
                ..Default::default()
            },
            client_id: "client".into(),
            client_host: "host".into(),
            regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
            reply,
        })
        .await
        .unwrap();
    response.await.unwrap()
}

/// `heartbeat_interval_ms` of the join response of each group: the group's
/// `consumer.heartbeat.interval.ms`, or the broker's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reports_its_own_heartbeat_interval() {
    let coordinator = coordinator(
        NextGenConfig::assigning_at_once(),
        &[("tuned", &[("consumer.heartbeat.interval.ms", "7000")])],
    );
    let mut reported = Vec::new();
    for group_id in ["tuned", "plain"] {
        let handle = coordinator.get_or_create_consumer(group_id);
        reported.push((
            group_id,
            heartbeat(&handle, "m1", 0).await.heartbeat_interval_ms,
        ));
    }
    assert!(reported == [("tuned", 7000), ("plain", 5000)]);
}

/// A member of a group with a `consumer.session.timeout.ms` of 1 ms is
/// expired by the next session tick, while a group without one keeps its
/// member for the broker's 45 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_expires_a_member_by_its_own_session_timeout() {
    // The broker's minimum admits the 1 ms that the group asks for, which its
    // own override is capped to.
    let coordinator = coordinator(
        NextGenConfig {
            session_expiry_tick: Duration::from_millis(10),
            min_session_timeout: Duration::from_millis(1),
            ..NextGenConfig::assigning_at_once()
        },
        &[("brief", &[("consumer.session.timeout.ms", "1")])],
    );
    let mut epochs = Vec::new();
    for group_id in ["brief", "plain"] {
        let handle = coordinator.get_or_create_consumer(group_id);
        let joined = heartbeat(&handle, "m1", 0).await;
        check!(joined.error_code == codes::NONE, "{group_id}");
        epochs.push((group_id, handle, joined.member_epoch));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut answers = Vec::new();
    for (group_id, handle, epoch) in &epochs {
        answers.push((*group_id, heartbeat(handle, "m1", *epoch).await.error_code));
    }
    assert!(answers == [("brief", codes::UNKNOWN_MEMBER_ID), ("plain", codes::NONE)]);
}

/// Kafka's `canComputeNextTargetAssignment`: a second member that joins
/// within the assignment interval of the first assignment joins at the epoch
/// of that assignment, and the group assigns for it when the interval has
/// elapsed. A group whose `consumer.assignment.interval.ms` is 0 assigns at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_assigns_no_sooner_than_its_own_assignment_interval() {
    let coordinator = coordinator(
        NextGenConfig {
            assignment_interval: Duration::from_mins(1),
            ..NextGenConfig::default()
        },
        &[
            ("fast", &[("consumer.assignment.interval.ms", "0")]),
            ("paced", &[("consumer.assignment.interval.ms", "300")]),
        ],
    );
    // (group, epoch of the second member's join)
    let mut joined = Vec::new();
    let mut handles = Vec::new();
    for group_id in ["slow", "fast", "paced"] {
        let handle = coordinator.get_or_create_consumer(group_id);
        let first = heartbeat(&handle, "m1", 0).await;
        check!(first.member_epoch == 1, "{group_id}");
        joined.push((group_id, heartbeat(&handle, "m2", 0).await.member_epoch));
        handles.push((group_id, handle));
    }
    assert!(joined == [("slow", 1), ("fast", 2), ("paced", 1)]);

    // The paced group's interval elapses; the next heartbeat computes the
    // assignment that its second member was waiting for.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (_, paced) = &handles[2];
    check!(heartbeat(paced, "m1", 1).await.error_code == codes::NONE);
    check!(heartbeat(paced, "m2", 1).await.member_epoch == 2);
    // The slow group still waits for the broker's minute.
    let (_, slow) = &handles[0];
    check!(heartbeat(slow, "m2", 1).await.member_epoch == 1);
}
