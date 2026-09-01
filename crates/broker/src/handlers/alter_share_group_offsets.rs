//! `AlterShareGroupOffsets` (`api_key` 91), from KIP-932.
//!
//! The handler resets the share-partition start offset (SPSO) for the
//! requested partitions of an *empty* share group. It bumps the state epoch
//! and initializes the persister state again. It rejects a non-empty group at
//! the top level with `NON_EMPTY_GROUP`.
//!
//! `network::dispatch` intercepts this RPC inline for the per-group `Alter`
//! ACL gate, which needs the principal and the peer `SocketAddr`.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
        alter_share_group_offsets_response::{
            AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
            AlterShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::share::actor::ShareGroupActorMessage,
    error::BrokerError,
};

#[tracing::instrument(
    name = "handle_alter_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "AlterShareGroupOffsets", version, req_bytes = req_bytes.len()),
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
    let req = AlterShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the RPC.
    if !broker.config.share_group.enable {
        return encode_top_level(version, codes::UNSUPPORTED_VERSION);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Alter` check. On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Alter,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
        return encode_top_level(version, error_code);
    }

    let mut responses: Vec<AlterShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());
    let mut actor_requests = Vec::new();
    let mut actor_response_slots = Vec::new();

    for rt in req.topics {
        let topic_name = rt.topic_name;
        let topic_id = image.topic(&topic_name).map(|t| t.topic_id);

        let mut partitions: Vec<AlterShareGroupOffsetsResponsePartition> =
            Vec::with_capacity(rt.partitions.len());

        for rp in rt.partitions {
            let partition_record = image.partition(&topic_name, rp.partition_index);
            let Some((topic_id, partition_record)) = topic_id.zip(partition_record) else {
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    ..Default::default()
                });
                continue;
            };

            actor_response_slots.push((responses.len(), partitions.len()));
            actor_requests.push(crate::coordinator::unified::share::actor::ResetPartition {
                topic_id,
                topic_name: topic_name.clone(),
                partition: rp.partition_index,
                start_offset: rp.start_offset,
                observed_leader_epoch: partition_record.leader_epoch.0,
            });
            partitions.push(AlterShareGroupOffsetsResponsePartition {
                partition_index: rp.partition_index,
                error_code: codes::NONE,
                ..Default::default()
            });
        }

        responses.push(AlterShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: topic_id.map_or_else(Uuid::default, |id| Uuid(*id.as_bytes())),
            partitions,
            ..Default::default()
        });
    }

    // The actor checks emptiness and applies the complete requested batch in
    // one mailbox turn, so a heartbeat cannot join between the gate and a
    // reset. Its seed message is queued first when this is a recovered group.
    let actor = ng_opt
        .as_ref()
        .expect("group coordinator is installed")
        .get_or_create_share(&gid);
    let (tx, rx) = oneshot::channel();
    if actor
        .tx
        .send(ShareGroupActorMessage::ResetOffsets {
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
        .map_err(|_| BrokerError::Share("share-group reset actor stopped".into()))?;
    let result_codes = match actor_result {
        Ok(result_codes) => result_codes,
        Err(error_code) => return encode_top_level(version, error_code),
    };
    if result_codes.len() != actor_response_slots.len() {
        return encode_top_level(version, codes::COORDINATOR_NOT_AVAILABLE);
    }
    for ((topic_slot, partition_slot), error_code) in
        actor_response_slots.into_iter().zip(result_codes)
    {
        let topic_id = uuid::Uuid::from_bytes(responses[topic_slot].topic_id.0);
        let partition = &mut responses[topic_slot].partitions[partition_slot];
        partition.error_code = error_code;
        if error_code == codes::NONE {
            broker
                .share_partition_leaders
                .invalidate(&gid, topic_id, partition.partition_index);
        }
    }

    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

fn encode_top_level(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = AlterShareGroupOffsetsResponse {
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
            alter_share_group_offsets_request::{
                AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
                AlterShareGroupOffsetsRequestTopic,
            },
            alter_share_group_offsets_response::{
                self, AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
                AlterShareGroupOffsetsResponseTopic,
            },
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            create_topics_response::{self, CreateTopicsResponse},
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        },
        primitives::uuid::Uuid,
    };
    use krabka_security::Principal;

    use super::{encode_top_level, handle};
    use crate::{
        authorizer::Authorizer, codes, coordinator::unified::share::actor::ShareGroupActorMessage,
        test_support::DenyAll,
    };

    fn request(
        group_id: &str,
        topic_name: &str,
        partitions: &[i32],
    ) -> AlterShareGroupOffsetsRequest {
        AlterShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: topic_name.into(),
                partitions: partitions
                    .iter()
                    .map(|partition_index| AlterShareGroupOffsetsRequestPartition {
                        partition_index: *partition_index,
                        start_offset: 42,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        AlterShareGroupOffsetsRequest,
        AlterShareGroupOffsetsResponse,
        version = alter_share_group_offsets_response::MAX_VERSION,
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

    async fn create_topic(
        broker_handle: &crate::broker::BrokerHandle,
        broker: &crate::broker::Broker,
        topic_name: &str,
        ctx: &crate::handlers::RequestContext<'_>,
    ) {
        let version = create_topics_response::MAX_VERSION;
        let bytes = crate::test_support::encode_request(
            &CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: topic_name.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let response = crate::handlers::create_topics::handle(broker, version, 1, &bytes, ctx)
            .await
            .expect("create topic");
        let response: CreateTopicsResponse =
            crate::test_support::decode_response(&response, version);
        assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
        broker_handle
            .wait_until_partition_present(topic_name, 0)
            .await;
    }

    #[test]
    fn encode_top_level_preserves_error_fields() {
        let resp = encode_top_level(
            alter_share_group_offsets_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = AlterShareGroupOffsetsResponse {
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
            &'a str,
            Vec<i32>,
            AlterShareGroupOffsetsResponse,
        );
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature returns top-level unsupported version",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                false,
                "missing",
                vec![0],
                AlterShareGroupOffsetsResponse {
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
                "missing",
                vec![0],
                AlterShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    responses: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "unknown topic preserves topic and partition fields",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                true,
                "missing-topic",
                vec![3, 5],
                AlterShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NONE,
                    error_message: None,
                    responses: vec![AlterShareGroupOffsetsResponseTopic {
                        topic_name: "missing-topic".into(),
                        topic_id: Uuid::default(),
                        partitions: vec![
                            AlterShareGroupOffsetsResponsePartition {
                                partition_index: 3,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: None,
                                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                            },
                            AlterShareGroupOffsetsResponsePartition {
                                partition_index: 5,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: None,
                                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
        ];
        for (case, authorizer, share_enabled, topic_name, partitions, expected) in cases {
            let (broker_handle, _dir) = start_broker(authorizer, share_enabled).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&request("g1", topic_name, &partitions));

            let resp = handle(&broker, version, 1, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp);

            assert!(resp == expected, "case: {case}");
            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn active_group_rejects_the_whole_reset_batch() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = broker.group_coordinator.clone();

        coordinator.mark_share("busy");
        let actor = coordinator.get_or_create_share("busy");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: ShareGroupHeartbeatRequest {
                    group_id: "busy".into(),
                    member_id: "member-1".into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(Vec::new()),
                    ..Default::default()
                },
                client_id: "client-a".into(),
                client_host: "127.0.0.1".into(),
                reply: tx,
            })
            .await
            .expect("send heartbeat");
        let resp = rx.await.expect("heartbeat response");
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let response = handle(
            &broker,
            alter_share_group_offsets_response::MAX_VERSION,
            1,
            &encode_request(&request("busy", "missing", &[0])),
            &ctx,
        )
        .await
        .expect("handle reset");
        let response = decode_response(&response);
        assert!(
            response.error_code == codes::NON_EMPTY_GROUP,
            "{response:?}"
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn reset_mutates_only_requested_valid_partitions_and_retry_is_exact() {
        let (broker_handle, dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        create_topic(&broker_handle, &broker, "reset-topic", &ctx).await;
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
        let topic_id = broker
            .controller
            .current_image()
            .topic("reset-topic")
            .expect("topic metadata")
            .topic_id;
        persister
            .initialize("g-reset", topic_id, 0, 4, krabka_log::Offset(10))
            .await
            .expect("seed share state");

        let reset_request = request("g-reset", "reset-topic", &[0, 9]);
        for expected_epoch in [5, 5] {
            let response = handle(
                &broker,
                alter_share_group_offsets_response::MAX_VERSION,
                1,
                &encode_request(&reset_request),
                &ctx,
            )
            .await
            .expect("handle reset");
            let response = decode_response(&response);
            assert!(response.error_code == codes::NONE, "{response:?}");
            assert!(response.responses[0].partitions[0].error_code == codes::NONE);
            assert!(
                response.responses[0].partitions[1].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION
            );

            let state = persister
                .read_state("g-reset", topic_id, 0)
                .await
                .expect("read state")
                .expect("state present");
            assert!(state.state_epoch == expected_epoch);
            assert!(state.start_offset == krabka_log::Offset(42));
        }
        let leader_epoch = broker
            .controller
            .current_image()
            .partition("reset-topic", 0)
            .expect("partition metadata")
            .leader_epoch
            .0;
        let actor = broker.group_coordinator.get_or_create_share("g-reset");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::ResetOffsets {
                requests: vec![crate::coordinator::unified::share::actor::ResetPartition {
                    topic_id,
                    topic_name: "reset-topic".into(),
                    partition: 0,
                    start_offset: 99,
                    observed_leader_epoch: leader_epoch + 1,
                }],
                reply: tx,
            })
            .await
            .expect("send stale reset");
        assert!(rx.await.expect("stale reset reply") == Ok(vec![codes::FENCED_LEADER_EPOCH]));

        persister
            .initialize("g-overflow", topic_id, 0, i32::MAX, krabka_log::Offset(10))
            .await
            .expect("seed exhausted state epoch");
        let overflow_response = handle(
            &broker,
            alter_share_group_offsets_response::MAX_VERSION,
            1,
            &encode_request(&request("g-overflow", "reset-topic", &[0])),
            &ctx,
        )
        .await
        .expect("handle overflow reset");
        let overflow_response = decode_response(&overflow_response);
        assert!(
            overflow_response.responses[0].partitions[0].error_code
                == codes::COORDINATOR_NOT_AVAILABLE
        );

        let state = persister
            .read_state("g-reset", topic_id, 9)
            .await
            .expect("read unrequested state");
        assert!(state.is_none());
        broker_handle.shutdown().await;
    }
}
