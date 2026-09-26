//! Handler tests for `OffsetDelete`: the group-level refusals, Kafka's check
//! order and the subscription guard, each as a whole response.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use assert2::{assert, check};
use bytes::{BufMut as _, Bytes};
use krabka_protocol::{
    Encode as _,
    owned::{
        consumer_protocol_subscription::ConsumerProtocolSubscription,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        offset_delete_response::{self, OffsetDeleteResponse},
    },
};

use super::{
    handle,
    test_support::{expected_row, expected_topic, req_with_topics},
};
use crate::{
    authorizer::{AllowAllAuthorizer, Authorizer},
    broker::BrokerHandle,
    codes,
    coordinator::unified::{
        classic_state::{ClassicGroup, Member},
        consumer_state::{GroupState as ConsumerGroup, test_support::member},
        group::{CoordinatorGroup, GroupKind},
    },
    test_support::{DenyAll, decode_response, encode_request, request_context},
};

async fn start(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

async fn create_topic(broker: &BrokerHandle, name: &str) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("offset-delete-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
}

/// A classic `ConsumerProtocolSubscription` blob: the `i16` version, then the
/// v0 body.
fn subscription(topics: &[&str]) -> Bytes {
    let sub = ConsumerProtocolSubscription {
        topics: topics.iter().map(|s| (*s).to_string()).collect(),
        ..Default::default()
    };
    let mut out = bytes::BytesMut::new();
    out.put_i16(0);
    sub.encode(&mut out, 0).unwrap();
    out.freeze()
}

/// A `Stable` classic group with one member whose selected protocol carries
/// `metadata`.
fn classic_group(group_id: &str, protocol_type: &str, metadata: Bytes) -> CoordinatorGroup {
    let mut state = ClassicGroup::new(group_id);
    state.protocol_type = Some(protocol_type.into());
    state.add_member(Member::new(
        "m1",
        "client",
        "host",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), metadata)],
    ));
    state.resolve_selected_protocol_metadata("range");
    state.complete_rebalance("range");
    state.install_assignments(HashMap::from([("m1".to_string(), Bytes::new())]));
    CoordinatorGroup::seeded(group_id, GroupKind::Classic(state), HashMap::new())
}

/// A KIP-848 consumer group with one member that subscribes by regex.
fn consumer_group(group_id: &str, regex: &str, resolved: &[&str]) -> CoordinatorGroup {
    let mut state = ConsumerGroup::new(group_id);
    let mut m = member("m1");
    m.set_regex(Some(regex.into()));
    m.regex_authorized_topics = resolved.iter().map(|s| (*s).to_string()).collect();
    state.add_or_update_member(m);
    CoordinatorGroup::seeded(group_id, GroupKind::Consumer(state), HashMap::new())
}

fn whole(code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        ..Default::default()
    }
}

/// #734 and #813, row by row. Topics `t` and `u` exist with one partition
/// each; `ghost` does not.
/// An `OffsetDelete` case: name, broker, group id, request topics, and the
/// whole expected response.
type Case<'a> = (
    &'a str,
    &'a BrokerHandle,
    &'a str,
    &'a [(&'a str, &'a [i32])],
    OffsetDeleteResponse,
);

#[tokio::test]
async fn offset_delete_matches_kafka_whole_responses() {
    let (allowed, _allowed_dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let (denied, _denied_dir) = start(Arc::new(DenyAll)).await;
    create_topic(&allowed, "t").await;
    create_topic(&allowed, "u").await;
    let coordinator = allowed.broker_arc_for_test().group_coordinator.clone();
    for group in [
        classic_group("connect-g", "connect", Bytes::from_static(b"opaque")),
        classic_group("classic-g", "consumer", subscription(&["t", "ghost"])),
        classic_group("garbled-g", "consumer", Bytes::from_static(b"\x00")),
        consumer_group("kip848-g", "t.*", &["t"]),
    ] {
        let group_id = group.group_id.clone();
        coordinator.seed_classic(&group_id, Box::new(group));
    }

    let rows: Vec<Case<'_>> = vec![
        (
            "group Delete denied: top-level only",
            &denied,
            "classic-g",
            &[("t", &[0])],
            whole(codes::GROUP_AUTHORIZATION_FAILED),
        ),
        (
            "empty group id",
            &allowed,
            "",
            &[("t", &[0])],
            whole(codes::INVALID_GROUP_ID),
        ),
        (
            "missing group: top-level only",
            &allowed,
            "ghost-g",
            &[("t", &[0])],
            whole(codes::GROUP_ID_NOT_FOUND),
        ),
        (
            "non-empty group without the consumer protocol",
            &allowed,
            "connect-g",
            &[("t", &[0])],
            whole(codes::NON_EMPTY_GROUP),
        ),
        (
            "undecodable metadata subscribes to every topic",
            &allowed,
            "garbled-g",
            &[("u", &[0])],
            OffsetDeleteResponse {
                topics: vec![expected_topic(
                    "u",
                    vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)],
                )],
                ..whole(codes::NONE)
            },
        ),
        (
            "KIP-848 group subscribed by regex",
            &allowed,
            "kip848-g",
            &[("t", &[0])],
            OffsetDeleteResponse {
                topics: vec![expected_topic(
                    "t",
                    vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)],
                )],
                ..whole(codes::NONE)
            },
        ),
        (
            "existence before subscription, broker rows before the coordinator's",
            &allowed,
            "classic-g",
            &[("t", &[0]), ("ghost", &[0]), ("u", &[0, 5])],
            OffsetDeleteResponse {
                topics: vec![
                    expected_topic(
                        "ghost",
                        vec![expected_row(0, codes::UNKNOWN_TOPIC_OR_PARTITION)],
                    ),
                    expected_topic(
                        "u",
                        vec![
                            expected_row(5, codes::UNKNOWN_TOPIC_OR_PARTITION),
                            expected_row(0, codes::NONE),
                        ],
                    ),
                    expected_topic("t", vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)]),
                ],
                ..whole(codes::NONE)
            },
        ),
    ];

    let principal = crate::test_support::principal("alice");
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = request_context(&principal, &peer, "offset-delete-client");
    let version = offset_delete_response::MAX_VERSION;
    for (name, broker, group_id, topics, want) in rows {
        let req = krabka_protocol::owned::offset_delete_request::OffsetDeleteRequest {
            group_id: group_id.into(),
            ..req_with_topics(topics)
        };
        let resp = handle(
            &broker.broker_arc_for_test(),
            version,
            1,
            &encode_request(&req, version),
            &ctx,
        )
        .await
        .expect("OffsetDelete");
        let resp: OffsetDeleteResponse = decode_response(&resp, version);
        check!(resp == want, "{name}");
    }

    allowed.shutdown().await;
    denied.shutdown().await;
}
