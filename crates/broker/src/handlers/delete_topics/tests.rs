//! Tests for the `DeleteTopics` handler driven over the wire against a live
//! in-process broker, plus the quota-delay predicate the response path uses.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::{
    owned::{
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_security::Principal;
use krabka_units::{Time, convert::TimeExt, millis};

use super::{
    test_support::{id_state, named_state, request},
    *,
};
use crate::{
    broker::Broker,
    codes,
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

#[test]
fn should_wait_for_quota_delay_only_waits_for_positive_delay() {
    assert!(!should_wait_for_quota_delay(<Time as TimeExt>::ZERO));
    assert!(should_wait_for_quota_delay(millis(1)));
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
