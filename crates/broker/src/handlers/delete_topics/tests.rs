//! Tests for the `DeleteTopics` handler driven over the wire against a live
//! in-process broker.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};
use krabka_protocol::{
    owned::{
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_security::Principal;

use super::{
    test_support::{DOOMED, gated_config, id_state, named_state, request},
    *,
};
use crate::{
    broker::{Broker, BrokerHandle},
    codes,
    config::BreakGlassConfig,
    test_support::{
        DenyAll, peer, principal, start_broker_with_authorizer_no_audit as start_broker,
    },
};

const VERSION: i16 = 6;

crate::test_support::wire_helpers!(
    DeleteTopicsRequest,
    DeleteTopicsResponse,
    version = VERSION,
    client_id = "admin-client"
);

async fn drive(
    broker: &Broker,
    req: &DeleteTopicsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> DeleteTopicsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

#[tokio::test]
async fn handle_denied_topic_returns_authorization_failure() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request(vec![named_state("secret")]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteTopicsResponse {
        throttle_time_ms: 0,
        responses: vec![DeletableTopicResult {
            name: Some("secret".into()),
            topic_id: WireUuid::ZERO,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unknown_name_and_id_preserve_error_rows() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let bogus_id = WireUuid([8; 16]);
    let req = request(vec![named_state("missing"), id_state(bogus_id)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteTopicsResponse {
        throttle_time_ms: 0,
        responses: vec![
            DeletableTopicResult {
                name: Some("missing".into()),
                topic_id: WireUuid::ZERO,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            DeletableTopicResult {
                name: None,
                topic_id: bogus_id,
                error_code: codes::UNKNOWN_TOPIC_ID,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

// ── KFC-9: the break-glass gate over a topic deletion ───────────────

/// Run one `DeleteTopics` request for [`DOOMED`] against a broker with this
/// break-glass configuration, and answer the topic row.
async fn delete_doomed(break_glass: BreakGlassConfig) -> DeletableTopicResult {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(move |cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.break_glass = break_glass;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let ctx = test_context(&principal, &peer);
    let req = DeleteTopicsRequest {
        topics: vec![named_state(DOOMED)],
        timeout_ms: 5_000,
        ..Default::default()
    };

    let bytes = handle(&broker, VERSION, 1, &encode_request(&req), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&bytes);
    broker_handle.shutdown().await;
    resp.responses.into_iter().next().expect("one topic row")
}

#[tokio::test]
async fn the_wire_handler_refuses_a_deletion_that_no_proposal_covers() {
    let refused = delete_doomed(gated_config()).await;

    let expected = DeletableTopicResult {
        name: Some(DOOMED.to_owned()),
        topic_id: WireUuid::ZERO,
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(
            "break-glass refused delete_topic on doomed: no approved proposal covers the request"
                .to_owned(),
        ),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(refused == expected, "{refused:?}");
}

#[tokio::test]
async fn a_refused_deletion_never_reaches_the_metadata_quorum() {
    // The topic does not exist, so a broker that submits the delete record
    // hears `UNKNOWN_TOPIC_OR_PARTITION` back from the quorum. A broker
    // that answers `POLICY_VIOLATION` instead never submitted anything.
    let ungated = delete_doomed(BreakGlassConfig::default()).await;
    let gated = delete_doomed(gated_config()).await;

    check!(ungated.error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    check!(gated.error_code == codes::POLICY_VIOLATION);
}

/// Kafka's `ControllerApis.deleteTopics` answers `INVALID_REQUEST` for a v6
/// row with no name and the zero id, a row with a name and a non-zero id, a
/// duplicate name, a duplicate id, and a name whose id another row carries.
/// Each such request deletes nothing, and each response row carries the name,
/// the id and the message that Kafka puts on it.
#[tokio::test]
async fn invalid_topic_rows_answer_invalid_request_and_delete_nothing() {
    const TOPIC: &str = "kept";
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker_handle.listen_addr().to_string())
        .client_id("delete-topics-validation-test")
        .build()
        .await
        .expect("client build");
    let created = client
        .send(
            krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                topics: vec![
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: TOPIC.to_string(),
                        num_partitions: 1,
                        replication_factor: 1,
                        ..Default::default()
                    },
                ],
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
    broker_handle.wait_until_partition_present(TOPIC, 0).await;
    let topic_id = WireUuid(
        broker_handle
            .controller_image_for_test()
            .topic(TOPIC)
            .expect("topic in the image")
            .topic_id
            .into_bytes(),
    );
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();

    let invalid = |name: Option<&str>, id, message: &str| DeletableTopicResult {
        name: name.map(str::to_string),
        topic_id: id,
        error_code: codes::INVALID_REQUEST,
        error_message: Some(message.to_string()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    let both = krabka_protocol::owned::delete_topics_request::DeleteTopicState {
        name: Some(TOPIC.into()),
        topic_id,
        ..Default::default()
    };
    let cases = [
        (
            "no name and the zero id",
            vec![id_state(WireUuid::ZERO)],
            vec![invalid(
                None,
                WireUuid::ZERO,
                "Neither topic name nor id were specified.",
            )],
        ),
        (
            "a name and a non-zero id",
            vec![both],
            vec![invalid(
                Some(TOPIC),
                topic_id,
                "You may not specify both topic name and topic id.",
            )],
        ),
        (
            "a duplicate name",
            vec![named_state(TOPIC), named_state(TOPIC)],
            vec![invalid(
                Some(TOPIC),
                WireUuid::ZERO,
                "Duplicate topic name.",
            )],
        ),
        (
            "a duplicate id",
            vec![id_state(topic_id), id_state(topic_id)],
            vec![invalid(None, topic_id, "Duplicate topic id.")],
        ),
        (
            "a name whose id another row carries",
            vec![id_state(topic_id), named_state(TOPIC)],
            vec![invalid(
                Some(TOPIC),
                topic_id,
                "The provided topic name maps to an ID that was already supplied.",
            )],
        ),
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, rows, responses) in cases {
        let resp = drive(&broker, &request(rows), &p, &peer).await;
        let still_there = broker.controller.current_image().topic(TOPIC).is_some();
        actual.push((label, resp, still_there));
        expected.push((
            label,
            DeleteTopicsResponse {
                throttle_time_ms: 0,
                responses,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            },
            true,
        ));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("admin".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("admin-client".into()),
                },
            ],
            config_key: "controller_mutation_rate".into(),
            config_value: Some(rate),
        })])
        .await
        .expect("seed quota");
}

#[tokio::test]
async fn strict_delete_topics_rejects_after_quota_exhaustion() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker_handle.listen_addr().to_string())
        .client_id("admin-client")
        .build()
        .await
        .expect("client build");

    let created = client
        .send(
            krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                topics: vec![
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: "t1".to_string(),
                        num_partitions: 5,
                        replication_factor: 1,
                        ..Default::default()
                    },
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: "t2".to_string(),
                        num_partitions: 1,
                        replication_factor: 1,
                        ..Default::default()
                    },
                ],
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .expect("CreateTopics");
    assert!(
        created.topics.iter().all(|t| t.error_code == codes::NONE),
        "{created:?}"
    );
    broker_handle.wait_until_partition_present("t1", 0).await;
    broker_handle.wait_until_partition_present("t2", 0).await;

    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();

    let resp1 = drive(&broker, &request(vec![named_state("t1")]), &p, &peer).await;
    assert!(resp1.responses.len() == 1);
    assert!(resp1.responses[0].error_code == codes::NONE);
    assert!(broker.controller.current_image().topic("t1").is_none());

    let resp2 = drive(&broker, &request(vec![named_state("t2")]), &p, &peer).await;
    assert!(resp2.responses.len() == 1);
    assert!(resp2.responses[0].error_code == codes::THROTTLING_QUOTA_EXCEEDED);
    assert!(resp2.throttle_time_ms > 0);
    assert!(broker.controller.current_image().topic("t2").is_some());

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn non_strict_delete_topics_allows_when_quota_exhausted() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker_handle.listen_addr().to_string())
        .client_id("admin-client")
        .build()
        .await
        .expect("client build");

    let created = client
        .send(
            krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                topics: vec![
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: "t1".to_string(),
                        num_partitions: 5,
                        replication_factor: 1,
                        ..Default::default()
                    },
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: "t2".to_string(),
                        num_partitions: 1,
                        replication_factor: 1,
                        ..Default::default()
                    },
                ],
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .expect("CreateTopics");
    assert!(
        created.topics.iter().all(|t| t.error_code == codes::NONE),
        "{created:?}"
    );
    broker_handle.wait_until_partition_present("t1", 0).await;
    broker_handle.wait_until_partition_present("t2", 0).await;

    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();

    let resp1 = drive(&broker, &request(vec![named_state("t1")]), &p, &peer).await;
    assert!(resp1.responses[0].error_code == codes::NONE);

    let req_v4 = DeleteTopicsRequest {
        topic_names: vec!["t2".to_string()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let ctx = test_context(&p, &peer);
    let req_bytes = crate::test_support::encode_request(&req_v4, 4);
    let bytes = handle(&broker, 4, 124, &req_bytes, &ctx)
        .await
        .expect("handle");
    let resp_v4: DeleteTopicsResponse = crate::test_support::decode_response(&bytes, 4);
    assert!(resp_v4.responses.len() == 1);
    assert!(resp_v4.responses[0].error_code == codes::NONE);
    assert!(resp_v4.throttle_time_ms > 0);
    assert!(broker.controller.current_image().topic("t2").is_none());

    broker_handle.shutdown().await;
}
