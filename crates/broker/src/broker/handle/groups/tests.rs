use std::sync::Arc;

use assert2::{assert, check};

use super::*;
use crate::{broker::Broker, config::BrokerConfig};

fn consumer_group_seed(member_id: &str) -> crate::coordinator::unified::GroupSeed {
    let mut seed = crate::coordinator::unified::GroupSeed {
        group_epoch: 3,
        target_epoch: 4,
        ..Default::default()
    };
    seed.members.insert(
        member_id.to_string(),
        crate::coordinator::unified::persistence_next_gen::MemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "client".to_string(),
            client_host: "127.0.0.1".to_string(),
            subscribed_topic_names: vec!["orders".to_string()],
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout_ms: 60_000,
            classic: None,
        },
    );
    seed
}

fn classic_group_with_member(
    group_id: &str,
    member_id: &str,
) -> Box<crate::coordinator::unified::group::CoordinatorGroup> {
    let mut classic = crate::coordinator::unified::classic_state::ClassicGroup::new(group_id);
    classic.protocol_type = Some("consumer".to_string());
    classic.generation_id = 1;
    let member = crate::coordinator::unified::classic_state::Member::new(
        member_id,
        "client",
        "127.0.0.1",
        std::time::Duration::from_secs(30),
        std::time::Duration::from_mins(1),
        vec![("range".to_string(), bytes::Bytes::from_static(b"metadata"))],
    );
    let _ = classic.add_member(member);
    Box::new(crate::coordinator::unified::group::CoordinatorGroup {
        group_id: group_id.to_string(),
        kind: crate::coordinator::unified::group::GroupKind::Classic(classic),
        committed_offsets: std::collections::HashMap::new(),
    })
}

fn streams_group_seed(member_id: &str) -> crate::coordinator::unified::StreamsGroupSeed {
    let mut active = std::collections::BTreeMap::new();
    active.insert("subtopology-0".to_string(), vec![0, 1]);

    let mut members = std::collections::HashMap::new();
    members.insert(
        member_id.to_string(),
        crate::coordinator::unified::streams::persistence::StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "streams-client".to_string(),
            client_host: "127.0.0.1".to_string(),
            process_id: "process-1".to_string(),
            user_endpoint: None,
            client_tags: Vec::new(),
            rebalance_timeout_ms: 60_000,
            topology_epoch: 0,
        },
    );

    let mut target_per_member = std::collections::HashMap::new();
    target_per_member.insert(
        member_id.to_string(),
        crate::coordinator::unified::streams::persistence::StreamsGroupTargetAssignmentMemberValue {
            active: active.clone(),
            standby: std::collections::BTreeMap::new(),
            warmup: std::collections::BTreeMap::new(),
        },
    );

    let mut current_per_member = std::collections::HashMap::new();
    current_per_member.insert(
        member_id.to_string(),
        crate::coordinator::unified::streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: crate::coordinator::unified::streams::state::StreamsMemberAssignmentState::Stable
                .as_i8(),
            active,
            standby: std::collections::BTreeMap::new(),
            warmup: std::collections::BTreeMap::new(),
            active_pending_revocation: std::collections::BTreeMap::new(),
        },
    );

    crate::coordinator::unified::StreamsGroupSeed {
        group_epoch: 5,
        assignment_epoch: 6,
        topology: None,
        partition_metadata: None,
        members,
        target_per_member,
        current_per_member,
    }
}

async fn assert_streams_group_helpers_observe_live_actor_view(
    broker: &Arc<Broker>,
    handle: &BrokerHandle,
) {
    let streams_group_id = "handle-streams-group-mutant";
    let streams_member_id = "streams-member-1";
    let streams_actor = broker
        .group_coordinator
        .get_or_create_streams(streams_group_id);
    streams_actor
        .tx
        .send(
            crate::coordinator::unified::streams::actor::StreamsGroupActorMessage::Seed(
                streams_group_seed(streams_member_id),
            ),
        )
        .await
        .expect("seed streams group");
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_streams_group_member_count(streams_group_id, 1),
        )
        .await
        .is_ok()
    );
    let streams = handle
        .streams_group_describe_for_test(streams_group_id)
        .await
        .expect("streams group describe");
    let expected_active = {
        let mut active = std::collections::BTreeMap::new();
        active.insert("subtopology-0".to_string(), vec![0, 1]);
        active
    };
    check!(streams.group_id.as_str() == streams_group_id);
    check!(streams.members.len() == 1);
    check!(streams.members[0].member_id.as_str() == streams_member_id);
    check!(streams.members[0].active == expected_active);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(75),
            handle.wait_until_streams_group_empty(streams_group_id),
        )
        .await
        .is_err()
    );

    let empty_streams_group_id = "handle-empty-streams-group-mutant";
    let _ = broker
        .group_coordinator
        .get_or_create_streams(empty_streams_group_id);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_streams_group_empty(empty_streams_group_id),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn group_handle_helpers_observe_live_actor_views() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.expect("broker start");
    let broker = handle.broker_arc_for_test();

    let group_id = "handle-next-gen-group-mutant";
    let member_id = "member-1";
    let actor = broker.group_coordinator.get_or_create_group(
        group_id,
        crate::coordinator::unified::actor::GroupKindTag::Consumer,
    );
    actor
        .tx
        .send(crate::coordinator::unified::actor::GroupActorMessage::Seed(
            consumer_group_seed(member_id),
        ))
        .await
        .expect("seed next-gen group");
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_group_member_count(group_id, 1),
        )
        .await
        .is_ok()
    );
    let described = handle
        .group_describe_for_test(group_id)
        .await
        .expect("next-gen group describe");
    check!(described.group_id.as_str() == group_id);
    check!(described.members.len() == 1);
    check!(described.members[0].member_id.as_str() == member_id);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(75),
            handle.wait_until_group_empty(group_id),
        )
        .await
        .is_err()
    );

    let empty_group_id = "handle-empty-group-mutant";
    let _ = broker.group_coordinator.get_or_create_group(
        empty_group_id,
        crate::coordinator::unified::actor::GroupKindTag::Consumer,
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_group_empty(empty_group_id),
        )
        .await
        .is_ok()
    );

    let classic_group_id = "handle-classic-group-mutant";
    let classic_member_id = "classic-member-1";
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(75),
            handle.wait_until_classic_group_member_count(classic_group_id, 1),
        )
        .await
        .is_err()
    );
    broker.group_coordinator.seed_classic(
        classic_group_id,
        classic_group_with_member(classic_group_id, classic_member_id),
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_classic_group_member_count(classic_group_id, 1),
        )
        .await
        .is_ok()
    );
    let classic = handle
        .classic_group_inspect_for_test(classic_group_id)
        .await
        .expect("classic group inspect");
    check!(classic.group_id.as_str() == classic_group_id);
    check!(classic.members.len() == 1);
    check!(classic.members[0].member_id.as_str() == classic_member_id);

    let created_classic_group_id = "handle-create-classic-group-mutant";
    assert!(
        handle
            .classic_group_inspect_for_test(created_classic_group_id)
            .await
            .is_none()
    );
    handle.group_create_for_test(created_classic_group_id);
    let created = handle
        .classic_group_inspect_for_test(created_classic_group_id)
        .await
        .expect("created classic group inspect");
    assert!(created.group_id == created_classic_group_id);
    assert!(created.members.is_empty());

    let marked_classic_group_id = "handle-marked-classic-group-mutant";
    assert!(
        handle
            .group_type_for_test(marked_classic_group_id)
            .is_none()
    );
    broker
        .group_coordinator
        .mark_classic(marked_classic_group_id);
    assert!(
        handle.group_type_for_test(marked_classic_group_id)
            == Some(crate::coordinator::unified::GroupType::Classic)
    );

    assert_streams_group_helpers_observe_live_actor_view(&broker, &handle).await;

    handle.shutdown().await;
}
