//! End-to-end tests of the `StreamsGroupDescribe` handler against a running
//! broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group error rows the
//! KIP-1071 gates produce -- feature disabled, group unknown, streams actor
//! gone -- stay byte-for-byte what the JVM admin client expects.

use std::{collections::HashSet, sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use krabka_protocol::UnknownTaggedFields;

use super::{
    test_support::{
        describe, describe_as, error_group, finalize_streams_version, seed_streams_group_topology,
        start_broker, start_broker_with_authorizer, topology_with_source_topic,
    },
    *,
};
use crate::{
    authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
    codes,
    coordinator::unified::streams::actor::StreamsGroupActorMessage,
    handlers::authorized_operations::authorized_operations_bits,
    test_support::{DenyAll, peer, principal},
};

/// Commit one literal `Allow` ACL for `User:<user>` on `(resource_type,
/// resource_name)`, for the tests that drive a specific ACL gate rather than
/// allow or deny everything.
async fn grant(
    broker_handle: &crate::broker::BrokerHandle,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
    user: &str,
) {
    broker_handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type,
            resource_name: resource_name.to_string(),
            pattern_type: PatternType::Literal,
            principal: format!("User:{user}"),
            host: "*".to_string(),
            operation,
            permission_type: PermissionType::Allow,
        })])
        .await
        .expect("commit acl");
}

#[tokio::test]
async fn disabled_feature_returns_requested_group_error_rows() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();

    let resp = describe(&broker, &["g-disabled-a", "g-disabled-b"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![
            error_group("g-disabled-a", codes::UNSUPPORTED_VERSION),
            error_group("g-disabled-b", codes::UNSUPPORTED_VERSION),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn enabled_missing_group_returns_not_found_rows() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;

    let resp = describe(&broker, &["missing-a", "missing-b"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![
            error_group("missing-a", codes::GROUP_ID_NOT_FOUND),
            error_group("missing-b", codes::GROUP_ID_NOT_FOUND),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn closed_streams_actor_returns_load_in_progress_row() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;

    let actor = broker.group_coordinator.get_or_create_streams("stopped");
    let (tx, rx) = tokio::sync::oneshot::channel();
    actor
        .tx
        .send(StreamsGroupActorMessage::Shutdown(tx))
        .await
        .expect("send shutdown");
    rx.await.expect("actor shutdown");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !actor.tx.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actor sender closed");

    let resp = describe(&broker, &["stopped"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![error_group("stopped", codes::COORDINATOR_LOAD_IN_PROGRESS)],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `StreamsGroupDescribeRequest.getErrorResponse`: every requested
/// group id gets `UNSUPPORTED_VERSION` (35) when the protocol is disabled,
/// table-driven over one and several requested groups.
#[tokio::test]
async fn disabled_protocol_answers_every_requested_group_with_unsupported_version() {
    let cases: [&[&str]; 2] = [&["solo"], &["g-a", "g-b", "g-c"]];
    for group_ids in cases {
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        // Note: `finalize_streams_version` is never called, so
        // `streams.version` stays unfinalized and the protocol gate is off.

        let resp = describe(&broker, group_ids).await;

        let expected = StreamsGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: group_ids
                .iter()
                .map(|gid| error_group(gid, codes::UNSUPPORTED_VERSION))
                .collect(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{group_ids:?}");
        broker_handle.shutdown().await;
    }
}

/// Kafka's `KafkaApis.handleStreamsGroupDescribe` checks the protocol gate
/// before any ACL check: a principal the authorizer denies on every request
/// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`, when
/// the protocol is off.
#[tokio::test]
async fn protocol_gate_runs_before_the_group_acl_check() {
    let (broker_handle, _dir) = start_broker_with_authorizer(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    // Never finalized: the protocol is off regardless of the authorizer.
    let alice = principal("alice");

    let resp = describe_as(&broker, &alice, &["g1"], false).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![error_group("g1", codes::UNSUPPORTED_VERSION)],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// Kafka puts `GROUP_AUTHORIZATION_FAILED` rows first, ahead of every other
/// row, rather than preserving request order: the denied group sorts first
/// in the response even though it was requested second.
#[tokio::test]
async fn denied_group_rows_sort_first_regardless_of_request_order() {
    let (broker_handle, _dir) =
        start_broker_with_authorizer(Arc::new(SimpleAclAuthorizer::new(HashSet::new()))).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "allowed",
        AclOperation::Describe,
        "alice",
    )
    .await;
    let alice = principal("alice");

    // Requested in ["allowed", "denied"] order; "denied" has no ACL grant.
    let resp = describe_as(&broker, &alice, &["allowed", "denied"], false).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![
            error_group("denied", codes::GROUP_AUTHORIZATION_FAILED),
            error_group("allowed", codes::GROUP_ID_NOT_FOUND),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// Kafka hides a group whose topology names a topic the caller cannot
/// `Describe`: the row becomes `TOPIC_AUTHORIZATION_FAILED` (29) with no
/// topology and no members, and another group in the same request that names
/// only an authorized topic is unaffected.
#[tokio::test]
async fn topology_topic_denied_for_describe_hides_only_that_group() {
    let (broker_handle, _dir) =
        start_broker_with_authorizer(Arc::new(SimpleAclAuthorizer::new(HashSet::new()))).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "blocked",
        AclOperation::Describe,
        "alice",
    )
    .await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "open",
        AclOperation::Describe,
        "alice",
    )
    .await;
    // "secret" gets no Describe grant; "public" does.
    grant(
        &broker_handle,
        ResourceType::Topic,
        "public",
        AclOperation::Describe,
        "alice",
    )
    .await;
    seed_streams_group_topology(&broker, "blocked", topology_with_source_topic("secret")).await;
    seed_streams_group_topology(&broker, "open", topology_with_source_topic("public")).await;
    let alice = principal("alice");

    let resp = describe_as(&broker, &alice, &["blocked", "open"], false).await;

    check!(
        resp.groups[0]
            == DescribedGroup {
                group_id: "blocked".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some(
                    "The described group uses topics that the client is not authorized to \
                     describe."
                        .into()
                ),
                ..Default::default()
            },
        "{:?}",
        resp.groups[0]
    );
    check!(resp.groups[1].group_id.as_str() == "open");
    check!(
        resp.groups[1].error_code == codes::NONE,
        "{:?}",
        resp.groups[1]
    );
    check!(
        resp.groups[1]
            .topology
            .as_ref()
            .and_then(|t| t.subtopologies.as_ref())
            .map(|s| s[0].source_topics.clone())
            == Some(vec!["public".to_string()])
    );
    broker_handle.shutdown().await;
}

/// KIP-430: `include_authorized_operations` fills the Group operations
/// bitfield only when the request opted in, table-driven over both flag
/// values against the same allowed group.
#[tokio::test]
async fn include_authorized_operations_fills_the_bitfield_only_on_opt_in() {
    let authorizer = Arc::new(AllowAllAuthorizer);
    let (broker_handle, _dir) = start_broker_with_authorizer(authorizer.clone() as _).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;
    seed_streams_group_topology(&broker, "g1", topology_with_source_topic("t")).await;
    let alice = principal("alice");

    for include in [false, true] {
        let resp = describe_as(&broker, &alice, &["g1"], include).await;
        check!(resp.groups.len() == 1, "{resp:?}");
        check!(
            resp.groups[0].error_code == codes::NONE,
            "{:?}",
            resp.groups[0]
        );
        if include {
            let expected = authorized_operations_bits(
                authorizer.as_ref(),
                &broker.controller.current_image(),
                &alice,
                &peer(),
                ResourceType::Group,
                "g1",
            );
            check!(expected != i32::MIN);
            check!(
                resp.groups[0].authorized_operations == expected,
                "{include}"
            );
        } else {
            check!(
                resp.groups[0].authorized_operations == i32::MIN,
                "{include}"
            );
        }
    }
    broker_handle.shutdown().await;
}
