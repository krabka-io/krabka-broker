//! `DeleteShareGroupOffsets` (`api_key` 92), from KIP-932.
//!
//! It deletes the durable share state for every initialized partition of the
//! requested topics, in an *empty* share group. A non-empty group gets a
//! top-level `NON_EMPTY_GROUP` rejection.
//!
//! The request carries only `topic_name` for each topic, and no partition
//! list. The handler therefore lists the group's initialized partitions for
//! each topic from the cached `ShareGroupStatePartitionMetadata`.
//!
//! `network::dispatch` intercepts this request inline for the per-group
//! `Delete` ACL gate, which needs the principal and the peer `SocketAddr`.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
        delete_share_group_offsets_response::{
            DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::share::actor::{DeleteTopic, ShareGroupActorMessage},
    error::BrokerError,
};

#[tracing::instrument(
    name = "handle_delete_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DeleteShareGroupOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Delete` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
        return encode_top_level(version, error_code);
    }

    let metadata = ng_opt
        .as_ref()
        .and_then(|ng| ng.share_state_partition_metadata(&gid));

    let mut responses: Vec<DeleteShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());
    let mut actor_requests = Vec::new();
    let mut actor_response_slots = Vec::new();

    for rt in req.topics {
        let topic_name = rt.topic_name;

        let Some(topic_id) = image.topic(&topic_name).map(|t| t.topic_id) else {
            responses.push(DeleteShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id: Uuid::default(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            });
            continue;
        };

        actor_response_slots.push(responses.len());
        actor_requests.push(DeleteTopic {
            topic_id,
            topic_name: topic_name.clone(),
        });
        responses.push(DeleteShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: Uuid(*topic_id.as_bytes()),
            error_code: codes::NONE,
            ..Default::default()
        });
    }

    let actor = ng_opt
        .as_ref()
        .expect("group coordinator is installed")
        .get_or_create_share(&gid);
    let (tx, rx) = tokio::sync::oneshot::channel();
    if actor
        .tx
        .send(ShareGroupActorMessage::DeleteOffsets {
            requests: actor_requests,
            reply: tx,
        })
        .await
        .is_err()
    {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    }
    let actor_result = rx
        .await
        .map_err(|_| BrokerError::Share("share-group delete actor stopped".into()))?;
    let result_codes = match actor_result {
        Ok(result_codes) => result_codes,
        Err(error_code) => return encode_top_level(version, error_code),
    };
    if result_codes.len() != actor_response_slots.len() {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    }
    for (topic_slot, error_code) in actor_response_slots.into_iter().zip(result_codes) {
        responses[topic_slot].error_code = error_code;
        if error_code == codes::NONE {
            let topic_id = uuid::Uuid::from_bytes(responses[topic_slot].topic_id.0);
            let part_indices = metadata
                .as_ref()
                .and_then(|value| {
                    value
                        .initialized
                        .iter()
                        .find(|(candidate, _)| *candidate == topic_id)
                        .map(|(_, partitions)| partitions.as_slice())
                })
                .unwrap_or_default();
            for partition in part_indices {
                broker
                    .share_partition_leaders
                    .invalidate(&gid, topic_id, *partition);
            }
        }
    }

    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code,
        responses: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            create_topics_response::{self, CreateTopicsResponse},
            delete_share_group_offsets_request::{
                DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
            },
            delete_share_group_offsets_response::{
                self, DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    };
    use krabka_security::Principal;

    use super::{encode_top_level, handle};
    use crate::{
        authorizer::Authorizer,
        codes,
        coordinator::unified::{
            ShareGroupSeed,
            share::{
                actor::ShareGroupActorMessage, persistence::ShareGroupStatePartitionMetadataValue,
            },
        },
        test_support::DenyAll,
    };

    fn request(group_id: &str, topics: &[&str]) -> DeleteShareGroupOffsetsRequest {
        DeleteShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: topics
                .iter()
                .map(|topic_name| DeleteShareGroupOffsetsRequestTopic {
                    topic_name: (*topic_name).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        DeleteShareGroupOffsetsRequest,
        DeleteShareGroupOffsetsResponse,
        version = delete_share_group_offsets_response::MAX_VERSION,
        client_id = "admin-client"
    );

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    async fn create_topics(
        broker_handle: &crate::broker::BrokerHandle,
        broker: &crate::broker::Broker,
        topic_names: &[&str],
        ctx: &crate::handlers::RequestContext<'_>,
    ) {
        let version = create_topics_response::MAX_VERSION;
        let bytes = crate::test_support::encode_request(
            &CreateTopicsRequest {
                topics: topic_names
                    .iter()
                    .map(|topic_name| CreatableTopic {
                        name: (*topic_name).into(),
                        num_partitions: 1,
                        replication_factor: 1,
                        ..Default::default()
                    })
                    .collect(),
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let response = crate::handlers::create_topics::handle(broker, version, 1, &bytes, ctx)
            .await
            .expect("create topics");
        let response: CreateTopicsResponse =
            crate::test_support::decode_response(&response, version);
        assert!(
            response
                .topics
                .iter()
                .all(|topic| topic.error_code == codes::NONE),
            "{response:?}"
        );
        for topic_name in topic_names {
            broker_handle
                .wait_until_partition_present(topic_name, 0)
                .await;
        }
    }

    #[test]
    fn encode_top_level_preserves_error_fields() {
        let resp = encode_top_level(
            delete_share_group_offsets_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            responses: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_error_scenarios_preserve_expected_rows() {
        type Case<'a> = (
            &'a str,
            Arc<dyn Authorizer>,
            bool,
            Vec<&'a str>,
            DeleteShareGroupOffsetsResponse,
        );
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature returns top-level unsupported version",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                false,
                vec!["missing"],
                DeleteShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    responses: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "denied group returns top-level authorization failure",
                Arc::new(DenyAll),
                true,
                vec!["missing"],
                DeleteShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    responses: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "unknown topic preserves topic fields",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                true,
                vec!["missing-topic"],
                DeleteShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NONE,
                    error_message: None,
                    responses: vec![DeleteShareGroupOffsetsResponseTopic {
                        topic_name: "missing-topic".into(),
                        topic_id: Uuid::default(),
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        error_message: None,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
        ];
        for (case, authorizer, share_enabled, topics, expected) in cases {
            let (broker_handle, _dir) = start_broker(authorizer, share_enabled).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&request("g1", &topics));

            let resp = handle(&broker, version, 1, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp);

            assert!(resp == expected, "case: {case}");
            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn delete_fences_only_requested_state_and_retry_is_exact() {
        let (broker_handle, dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        create_topics(
            &broker_handle,
            &broker,
            &["delete-topic", "kept-topic"],
            &ctx,
        )
        .await;
        crate::share_coordinator::handlers::test_support::open_all_state_partitions(
            &broker.partitions,
            dir.path(),
            broker.config.share_coordinator.state_topic_num_partitions,
        );
        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let image = broker.controller.current_image();
        let deleted_id = image
            .topic("delete-topic")
            .expect("delete topic metadata")
            .topic_id;
        let kept_id = image
            .topic("kept-topic")
            .expect("kept topic metadata")
            .topic_id;
        drop(image);
        persister
            .initialize("g-delete", deleted_id, 0, 4, krabka_log::Offset(10))
            .await
            .expect("seed deleted state");
        persister
            .initialize("g-delete", kept_id, 0, 6, krabka_log::Offset(20))
            .await
            .expect("seed kept state");

        let actor = broker.group_coordinator.get_or_create_share("g-delete");
        actor
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                    initialized: vec![(deleted_id, vec![0]), (kept_id, vec![0])],
                    ..Default::default()
                },
                ..Default::default()
            }))
            .await
            .expect("seed share actor");

        for expected_epoch in [5, 5] {
            let response = handle(
                &broker,
                delete_share_group_offsets_response::MAX_VERSION,
                1,
                &encode_request(&request("g-delete", &["delete-topic"])),
                &ctx,
            )
            .await
            .expect("handle delete");
            let response = decode_response(&response);
            assert!(response.error_code == codes::NONE, "{response:?}");
            assert!(
                response.responses[0].error_code == codes::NONE,
                "{response:?}"
            );

            let deleted = persister
                .read_state("g-delete", deleted_id, 0)
                .await
                .expect("read deleted state")
                .expect("durable deletion fence");
            assert!(deleted.state_epoch == expected_epoch);
            assert!(
                deleted.start_offset
                    == crate::share_coordinator::coordinator::UNINITIALIZED_START_OFFSET
            );
        }
        let kept = persister
            .read_state("g-delete", kept_id, 0)
            .await
            .expect("read kept state")
            .expect("kept state");
        assert!(kept.state_epoch == 6);
        assert!(kept.start_offset == krabka_log::Offset(20));
        broker_handle.shutdown().await;
    }
}
