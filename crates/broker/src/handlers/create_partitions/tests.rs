//! End-to-end tests of the `CreatePartitions` handler, driven over the wire
//! encoding against a running broker: the authorization gate, the per-topic
//! error rows, the `validate_only` dry run, a successful grow, and the
//! KIP-599 mutation quota.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse;
use krabka_security::Principal;

use super::*;
use crate::{
    handlers::create_partitions::test_support::{
        VERSION, assn, request, seed_controller_quota, seed_topic, topic_req,
    },
    test_support::{DenyAll, peer, principal},
};

crate::test_support::wire_helpers!(
    CreatePartitionsRequest,
    CreatePartitionsResponse,
    version = VERSION,
    client_id = "admin-client"
);

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &CreatePartitionsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> CreatePartitionsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

#[tokio::test]
async fn handle_denies_topic_alter_for_each_topic() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request(
        vec![topic_req("orders", 2, None), topic_req("payments", 2, None)],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![
            CreatePartitionsTopicResult {
                name: "orders".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            CreatePartitionsTopicResult {
                name: "payments".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_unknown_topic_and_rejects_same_partition_count() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "stable", 2, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(
        vec![topic_req("missing", 3, None), topic_req("stable", 2, None)],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![
            CreatePartitionsTopicResult {
                name: "missing".into(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown topic `missing`".into()),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            CreatePartitionsTopicResult {
                name: "stable".into(),
                error_code: codes::INVALID_PARTITIONS,
                error_message: Some(
                    "topic `stable` already has 2 partitions; cannot decrease to 2".into(),
                ),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("stable")
            .count()
            == 2
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn validate_only_reports_success_without_adding_partitions() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "dry-run", 1, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_req("dry-run", 3, None)], true);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "dry-run".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("dry-run")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_adds_new_partitions_and_preserves_response_identity() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "grow", 1, 1).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(
        vec![topic_req("grow", 3, Some(vec![assn(&[1]), assn(&[1])]))],
        false,
    );

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "grow".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("grow")
            .count()
            == 3
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_an_unplaceable_new_diskless_partition() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "diskless-grow", 1, 1).await;
    broker_handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "diskless-grow".into(),
                overrides: maplit::btreemap! {
                    crate::config_keys::DISKLESS.to_string() => "true".to_string()
                },
            },
        )])
        .await
        .expect("mark topic diskless");
    let broker = broker_handle.broker_arc_for_test();
    let req = request(
        vec![topic_req("diskless-grow", 2, Some(vec![assn(&[1])]))],
        false,
    );

    let resp = drive(&broker, &req, &principal("admin"), &peer()).await;

    assert!(resp.results[0].error_code == codes::INVALID_CONFIG);
    let message = resp.results[0].error_message.as_deref().unwrap_or_default();
    for needle in ["partition 1", "leader 1", "broker.rack"] {
        check!(message.contains(needle), "{message}");
    }
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("diskless-grow")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn strict_create_partitions_rejects_after_quota_exhaustion() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_topic(&broker_handle, "metered", 2, 1).await;
    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_req("metered", 5, None)], false);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreatePartitionsResponse {
        throttle_time_ms: 0,
        results: vec![CreatePartitionsTopicResult {
            name: "metered".into(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let rejected = drive(
        &broker,
        &request(vec![topic_req("metered", 6, None)], false),
        &p,
        &peer,
    )
    .await;
    let expected = CreatePartitionsResponse {
        throttle_time_ms: rejected.throttle_time_ms,
        results: vec![CreatePartitionsTopicResult {
            name: "metered".into(),
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(rejected == expected);
    check!(rejected.throttle_time_ms > 0 && rejected.throttle_time_ms <= 500);
    check!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("metered")
            .count()
            == 5
    );
    broker_handle.shutdown().await;
}
