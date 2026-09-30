//! Each group protocol's join or heartbeat handler takes a group id, member id
//! or instance id of 32767 bytes, the longest a coordinator record can carry,
//! and refuses one of 32768 bytes before the request reaches any group.
//!
//! The refusal is what Kafka's request reader does with such a string: a
//! protocol error, which closes the connection. It is not an error response,
//! and it must not be a panic in the group actor that would write the record.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::{self, ConsumerGroupHeartbeatRequest},
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    heartbeat_request::{self, HeartbeatRequest},
    join_group_request::{self, JoinGroupRequest, JoinGroupRequestProtocol},
    join_group_response::JoinGroupResponse,
    share_group_heartbeat_request::{self, ShareGroupHeartbeatRequest},
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    streams_group_heartbeat_request::{self, StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use krabka_security::{AuthMethod, Principal};

use crate::{
    broker::{Broker, BrokerHandle},
    codes,
    error::BrokerError,
    handlers::{
        consumer_group_heartbeat, heartbeat, join_group, share_group_heartbeat,
        streams_group_heartbeat,
    },
    test_support::{decode_response, encode_request},
};

/// The longest string a coordinator record carries.
const LIMIT: usize = 32_767;

/// Which of a request's three ids is the long one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Long {
    Group,
    Member,
    Instance,
}

/// Every id at each side of the bound: `(the long id, its length)`.
fn cases() -> impl Iterator<Item = (Long, usize)> {
    [Long::Group, Long::Member, Long::Instance]
        .into_iter()
        .flat_map(|long| {
            [LIMIT, LIMIT + 1]
                .into_iter()
                .map(move |length| (long, length))
        })
}

/// The group id, member id and instance id of case `case`, with the one that
/// is `long` set to `length` bytes and the others short.
fn ids(case: usize, long: Long, length: usize, member: &str) -> (String, String, String) {
    let pick = |which: Long, short: String| {
        if which == long {
            "a".repeat(length)
        } else {
            short
        }
    };
    (
        pick(Long::Group, format!("record-strings-{case}")),
        pick(Long::Member, member.to_owned()),
        pick(Long::Instance, format!("instance-{case}")),
    )
}

/// A handler takes an id of 32767 bytes, and refuses one of 32768 bytes with a
/// protocol error.
fn check(label: &str, result: &Result<Bytes, BrokerError>, length: usize) {
    if length > LIMIT {
        assert!(
            matches!(result, Err(BrokerError::Protocol(_))),
            "{label}: {result:?}"
        );
    } else {
        assert!(result.is_ok(), "{label}: {result:?}");
    }
}

/// A running broker with the group, share and streams protocols on.
struct Env {
    handle: BrokerHandle,
    broker: Arc<Broker>,
    _dir: tempfile::TempDir,
    principal: Principal,
    peer: SocketAddr,
}

impl Env {
    async fn start() -> Self {
        let (handle, dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.streams_group.enable = true;
            cfg.classic_group_initial_rebalance_delay = krabka_units::secs(0);
        })
        .await;
        handle.wait_until_group_coordinator_ready().await;
        let broker = handle.broker_arc_for_test();
        for feature in [
            krabka_metadata::group_version::GROUP_VERSION_FEATURE,
            krabka_metadata::metadata_version::SHARE_VERSION_FEATURE,
            crate::features::STREAMS_VERSION,
        ] {
            finalize(&broker, feature).await;
        }
        create_source_topic(&broker, "in").await;
        Self {
            handle,
            broker,
            _dir: dir,
            principal: Principal {
                name: "ANONYMOUS".into(),
                auth_method: AuthMethod::Anonymous,
                groups: vec![],
            },
            peer: SocketAddr::from(([127, 0, 0, 1], 9092)),
        }
    }

    fn ctx(&self) -> crate::handlers::RequestContext<'_> {
        crate::test_support::request_context(&self.principal, &self.peer, "record-strings-test")
    }

    async fn stop(self) {
        self.handle.shutdown().await;
    }
}

/// Create a one-partition topic, so that a streams topology can read it.
async fn create_source_topic(broker: &Broker, name: &str) {
    use krabka_metadata::{LeaderEpoch, PartitionRecord, TopicRecord};

    let node_id = krabka_audit::NodeId(broker.config.node_id.0);
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: node_id,
                replicas: vec![node_id],
                isr: vec![node_id],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ])
        .await
        .expect("create the source topic");
}

/// Finalize `feature` at level 1 and wait until the image shows it.
async fn finalize(broker: &Broker, feature: &'static str) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: feature.into(),
            level: 1,
        })])
        .await
        .expect("finalize the feature");
    tokio::time::timeout(Duration::from_secs(5), async {
        while broker.controller.current_image().finalized_feature(feature) != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the image shows the feature");
}

#[tokio::test]
async fn consumer_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = consumer_group_heartbeat_request::MAX_VERSION;
    let env = Env::start().await;
    for (case, (long, length)) in cases().enumerate() {
        let label = format!("{long:?} of {length} bytes");
        let (group_id, member_id, instance_id) = ids(case, long, length, "member-short");
        let request = ConsumerGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id,
            instance_id: Some(instance_id),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };

        let result = consumer_group_heartbeat::handle(
            &env.broker,
            VERSION,
            1,
            &encode_request(&request, VERSION),
            &env.ctx(),
        )
        .await;

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<ConsumerGroupHeartbeatResponse>(&bytes, VERSION));
        assert!(
            env.broker.group_coordinator.find(&group_id).is_some() == (length <= LIMIT),
            "{label}: the group exists only when the request was taken: {response:?}"
        );
        assert!(
            response.is_none_or(|response| response.error_code == codes::NONE),
            "{label}: the join succeeds"
        );
    }
    env.stop().await;
}

#[tokio::test]
async fn share_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = share_group_heartbeat_request::MAX_VERSION;
    let env = Env::start().await;
    // A share group has no instance id.
    for (case, (long, length)) in cases()
        .filter(|(long, _)| *long != Long::Instance)
        .enumerate()
    {
        let label = format!("{long:?} of {length} bytes");
        let (group_id, member_id, _) = ids(case, long, length, "member-short");
        let request = ShareGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id,
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };

        let result = share_group_heartbeat::handle(
            &env.broker,
            VERSION,
            1,
            &encode_request(&request, VERSION),
            &env.ctx(),
        )
        .await;

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<ShareGroupHeartbeatResponse>(&bytes, VERSION));
        assert!(
            env.broker.group_coordinator.find_share(&group_id).is_some()
                == (length <= LIMIT && long == Long::Group),
            "{label}: the group exists only when a join was taken: {response:?}"
        );
        // Kafka's `isMemberIdValid` refuses a share member id over 36
        // characters, so a long one is answered `INVALID_REQUEST` and creates
        // no group.
        let want = if long == Long::Member {
            codes::INVALID_REQUEST
        } else {
            codes::NONE
        };
        assert!(
            response.is_none_or(|response| response.error_code == want),
            "{label}: the answer is {want}"
        );
    }
    env.stop().await;
}

#[tokio::test]
async fn streams_group_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = streams_group_heartbeat_request::MAX_VERSION;
    let env = Env::start().await;
    for (case, (long, length)) in cases().enumerate() {
        let label = format!("{long:?} of {length} bytes");
        let (group_id, member_id, instance_id) = ids(case, long, length, "member-short");
        let request = StreamsGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id,
            instance_id: (long == Long::Instance).then_some(instance_id),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            topology: Some(Topology {
                epoch: 1,
                subtopologies: vec![Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = streams_group_heartbeat::handle(
            &env.broker,
            VERSION,
            1,
            &encode_request(&request, VERSION),
            &env.ctx(),
        )
        .await;

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<StreamsGroupHeartbeatResponse>(&bytes, VERSION));
        // Kafka 4.3.1 refuses a streams member with an instance id (static
        // membership) as `INVALID_REQUEST`, so that request creates no group.
        assert!(
            env.broker
                .group_coordinator
                .find_streams(&group_id)
                .is_some()
                == (length <= LIMIT && long != Long::Instance),
            "{label}: the group exists only when a join was taken: {response:?}"
        );
    }
    env.stop().await;
}

#[tokio::test]
async fn classic_join_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = join_group_request::MAX_VERSION;
    let env = Env::start().await;
    for (case, (long, length)) in cases().enumerate() {
        let label = format!("{long:?} of {length} bytes");
        // A static member with no member id yet, which is the join that
        // records the group and its member in one round.
        let (group_id, member_id, instance_id) = ids(case, long, length, "");
        let request = JoinGroupRequest {
            group_id: group_id.clone(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            member_id,
            group_instance_id: Some(instance_id),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = tokio::time::timeout(
            Duration::from_secs(20),
            join_group::handle(
                &env.broker,
                VERSION,
                1,
                &encode_request(&request, VERSION),
                &env.ctx(),
            ),
        )
        .await
        .expect("the join answers");

        check(&label, &result, length);
        let response = result
            .ok()
            .map(|bytes| decode_response::<JoinGroupResponse>(&bytes, VERSION));
        // A join by an unknown member id creates no group, whatever its length.
        assert!(
            env.broker.group_coordinator.find(&group_id).is_some()
                == (length <= LIMIT && long != Long::Member),
            "{label}: the group exists only when a join was taken: {response:?}"
        );
    }
    env.stop().await;
}

#[tokio::test]
async fn classic_heartbeat_takes_32767_byte_ids_and_refuses_32768() {
    const VERSION: i16 = heartbeat_request::MAX_VERSION;
    let env = Env::start().await;
    for (case, (long, length)) in cases().enumerate() {
        let label = format!("{long:?} of {length} bytes");
        let (group_id, member_id, instance_id) = ids(case, long, length, "member-short");
        let request = HeartbeatRequest {
            group_id,
            generation_id: 1,
            member_id,
            group_instance_id: Some(instance_id),
            ..Default::default()
        };

        let result = heartbeat::handle(
            &env.broker,
            VERSION,
            1,
            &encode_request(&request, VERSION),
            &env.ctx(),
        )
        .await;

        check(&label, &result, length);
    }
    env.stop().await;
}
