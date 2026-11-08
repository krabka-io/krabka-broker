//! What each group RPC answers when the group's actor stops with the request
//! in its mailbox.
//!
//! An actor stops when this broker unloads the group's offsets partition, and
//! after a failed write, and it drops the requests still in its mailbox.
//! Kafka's `CoordinatorRuntime` fails the operations of a shard that unloads
//! with `NOT_COORDINATOR`, which `GroupCoordinatorService` hands back through
//! `handleOperationException`. Each RPC here therefore runs the routing check
//! again, and a broker that still serves the group answers the retriable
//! `COORDINATOR_NOT_AVAILABLE`: never `UNKNOWN_SERVER_ERROR`, a request error,
//! or a "not found" row for a group that exists.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        codes,
        coordinator::unified::{
            actor::{GroupActorHandle, GroupKindTag},
            share::actor::ShareGroupActorHandle,
        },
        handlers::{
            alter_share_group_offsets, consumer_group_describe, consumer_group_heartbeat,
            delete_share_group_offsets, describe_groups, heartbeat, offset_commit, offset_fetch,
            share_group_describe, share_group_heartbeat, test_support::create_topic,
        },
        test_support::start_share_broker,
    },
    assert2::check,
    krabka_protocol::owned::{
        alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
        describe_groups_request::DescribeGroupsRequest,
        heartbeat_request::HeartbeatRequest,
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::OffsetFetchRequest,
        share_group_describe_request::ShareGroupDescribeRequest,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    std::sync::Arc,
};

/// The group whose actor stops.
const GROUP: &str = "stopping";

krabka_broker_macros::wire_codec_helpers! {
    test_context ["admin-client"];
}

/// A group RPC that asks the group's actor.
#[derive(Clone, Copy, Debug)]
enum Rpc {
    ConsumerGroupHeartbeat,
    DescribeGroups,
    ConsumerGroupDescribe,
    Heartbeat,
    OffsetFetch,
    OffsetCommit,
    ShareGroupHeartbeat,
    ShareGroupDescribe,
    AlterShareGroupOffsets,
    DeleteShareGroupOffsets,
}

impl Rpc {
    /// Registers [`GROUP`] under an actor that drops every request it takes,
    /// of the kind this RPC reaches.
    fn install_stopping_actor(self, broker: &Broker) {
        let coordinator = &broker.group_coordinator;
        let kind = match self {
            Self::DescribeGroups | Self::Heartbeat | Self::OffsetFetch | Self::OffsetCommit => {
                GroupKindTag::Classic
            }
            Self::ConsumerGroupHeartbeat | Self::ConsumerGroupDescribe => GroupKindTag::Consumer,
            Self::ShareGroupHeartbeat
            | Self::ShareGroupDescribe
            | Self::AlterShareGroupOffsets
            | Self::DeleteShareGroupOffsets => {
                coordinator.mark_share(GROUP);
                coordinator.share_groups.insert(
                    GROUP.into(),
                    Arc::new(ShareGroupActorHandle::dropping_replies()),
                );
                return;
            }
        };
        coordinator.groups.insert(
            GROUP.into(),
            Arc::new(GroupActorHandle::dropping_replies(kind)),
        );
    }

    /// Sends this RPC for [`GROUP`] and returns the error code it answers for
    /// the group.
    async fn error_code(self, broker: &Broker) -> i16 {
        krabka_broker_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        match self {
            Self::Heartbeat => {
                let request = HeartbeatRequest {
                    group_id: GROUP.into(),
                    generation_id: 1,
                    member_id: "member-a".into(),
                    ..Default::default()
                };
                heartbeat::handle(broker, request, 4, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
            Self::OffsetFetch => {
                let request = OffsetFetchRequest {
                    group_id: GROUP.into(),
                    topics: None,
                    ..Default::default()
                };
                offset_fetch::handle(broker, request, 7, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
            Self::OffsetCommit => {
                let request = OffsetCommitRequest {
                    group_id: GROUP.into(),
                    generation_id_or_member_epoch: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic-a".into(),
                        partitions: vec![OffsetCommitRequestPartition {
                            committed_offset: 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let response = offset_commit::handle(broker, request, 8, &ctx)
                    .await
                    .expect("an answer");
                response.topics[0].partitions[0].error_code
            }
            Self::ConsumerGroupHeartbeat => {
                let request = ConsumerGroupHeartbeatRequest {
                    group_id: GROUP.into(),
                    member_id: "member-a".into(),
                    rebalance_timeout_ms: 30_000,
                    topic_partitions: Some(Vec::new()),
                    subscribed_topic_names: Some(vec!["topic-a".into()]),
                    ..Default::default()
                };
                consumer_group_heartbeat::handle(broker, request, 1, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
            Self::DescribeGroups => {
                let request = DescribeGroupsRequest {
                    groups: vec![GROUP.into()],
                    ..Default::default()
                };
                let response = describe_groups::handle(broker, request, 5, &ctx)
                    .await
                    .expect("an answer");
                response.groups[0].error_code
            }
            Self::ConsumerGroupDescribe => {
                let request = ConsumerGroupDescribeRequest {
                    group_ids: vec![GROUP.into()],
                    ..Default::default()
                };
                let response = consumer_group_describe::handle(broker, request, 0, &ctx)
                    .await
                    .expect("an answer");
                response.groups[0].error_code
            }
            Self::ShareGroupHeartbeat => {
                let request = ShareGroupHeartbeatRequest {
                    group_id: GROUP.into(),
                    member_id: "member-1".into(),
                    subscribed_topic_names: Some(vec!["topic-a".into()]),
                    ..Default::default()
                };
                share_group_heartbeat::handle(broker, request, 1, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
            Self::ShareGroupDescribe => {
                let request = ShareGroupDescribeRequest {
                    group_ids: vec![GROUP.into()],
                    ..Default::default()
                };
                let response = share_group_describe::handle(broker, request, 1, &ctx)
                    .await
                    .expect("an answer");
                response.groups[0].error_code
            }
            Self::AlterShareGroupOffsets => {
                let request = AlterShareGroupOffsetsRequest {
                    group_id: GROUP.into(),
                    ..Default::default()
                };
                alter_share_group_offsets::handle(broker, request, 0, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
            Self::DeleteShareGroupOffsets => {
                let request = DeleteShareGroupOffsetsRequest {
                    group_id: GROUP.into(),
                    ..Default::default()
                };
                delete_share_group_offsets::handle(broker, request, 0, &ctx)
                    .await
                    .expect("an answer")
                    .error_code
            }
        }
    }
}

/// A broker that still serves the group answers `COORDINATOR_NOT_AVAILABLE`
/// for a request its actor dropped.
#[rstest::rstest]
#[case::consumer_group_heartbeat(Rpc::ConsumerGroupHeartbeat)]
#[case::describe_groups(Rpc::DescribeGroups)]
#[case::heartbeat(Rpc::Heartbeat)]
#[case::offset_fetch(Rpc::OffsetFetch)]
#[case::offset_commit(Rpc::OffsetCommit)]
#[case::consumer_group_describe(Rpc::ConsumerGroupDescribe)]
#[case::share_group_heartbeat(Rpc::ShareGroupHeartbeat)]
#[case::share_group_describe(Rpc::ShareGroupDescribe)]
#[case::alter_share_group_offsets(Rpc::AlterShareGroupOffsets)]
#[case::delete_share_group_offsets(Rpc::DeleteShareGroupOffsets)]
#[tokio::test]
async fn a_request_the_stopped_actor_dropped_answers_coordinator_not_available(#[case] rpc: Rpc) {
    let started = start_share_broker()
        .authorizer(Arc::new(AllowAllAuthorizer))
        .call()
        .await;
    // The topic the heartbeats subscribe to and the commit names.
    create_topic()
        .broker(&started.broker)
        .name("topic-a")
        .call()
        .await;
    let broker = started.broker.broker_arc_for_test();
    rpc.install_stopping_actor(&broker);

    check!(rpc.error_code(&broker).await == codes::COORDINATOR_NOT_AVAILABLE);
    started.broker.shutdown().await;
}
