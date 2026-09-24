//! `AlterShareGroupOffsets` (`api_key` 91), from KIP-932.
//!
//! The handler resets the share-partition start offset (SPSO) for the
//! requested partitions of an *empty* share group. It bumps the GROUP epoch
//! once for the whole batch, writes a `ShareGroupMetadata` record, and
//! initializes the persister state at the new group epoch. It rejects a
//! non-empty group at the top level with `NON_EMPTY_GROUP`.
//!
//! Authorization, per Kafka's `KafkaApis.handleAlterShareGroupOffsetsRequest`:
//!   - `Read` on `Group(group_id)` for the whole response.
//!   - `Read` on `Topic(name)` for each topic, checked BEFORE the
//!     unknown-topic lookup. On Deny, every requested partition of that topic
//!     gets `TOPIC_AUTHORIZATION_FAILED` (29) and its offset is left
//!     untouched.
//!
//! `network::dispatch` intercepts this RPC inline for the per-group `Read`
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
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::unified::{GroupType, share::actor::ShareGroupActorMessage},
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
    // Per-group `Read` check, per Kafka's `handleAlterShareGroupOffsetsRequest`
    // (krabka previously checked `Alter`, which neither refuses a plain
    // `Alter` grant nor accepts the normal `Read`-only share-consumer grant).
    // On Deny → top-level `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Read,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode_top_level(version, codes::GROUP_AUTHORIZATION_FAILED);
    }
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
        return encode_top_level(version, error_code);
    }

    // Kafka's `GroupCoordinatorService.alterShareGroupOffsets` refuses the
    // empty group id before any group lookup.
    if gid.is_empty() {
        return encode_top_level(version, codes::INVALID_GROUP_ID);
    }
    // `GroupMetadataManager.getOrMaybeCreateShareGroup` throws
    // `GroupIdNotFoundException` for a group id already locked to another
    // protocol type. A share group not yet created (`None`) is fine: the
    // actor below creates and persists it.
    if let Some(existing_type) = broker.group_coordinator.group_type(&gid)
        && existing_type != GroupType::Share
    {
        return encode_top_level(version, codes::GROUP_ID_NOT_FOUND);
    }

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on
    // Deny, checked BEFORE the unknown-topic lookup below. Decided up front
    // (borrowing `req.topics`, not owning it) because the loop below moves
    // each topic's partitions out of `req.topics`.
    let topic_decisions: std::collections::HashMap<&str, AuthorizationResult> = {
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.topic_name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };
    let denied_topics: std::collections::HashSet<String> = topic_decisions
        .into_iter()
        .filter(|(_, result)| *result == AuthorizationResult::Deny)
        .map(|(name, _)| name.to_owned())
        .collect();

    let mut responses: Vec<AlterShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());
    let mut actor_requests = Vec::new();
    let mut actor_response_slots = Vec::new();

    for rt in req.topics {
        let topic_name = rt.topic_name;
        let topic_denied = denied_topics.contains(&topic_name);

        if topic_denied {
            let partitions = rt
                .partitions
                .into_iter()
                .map(|rp| AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect();
            responses.push(AlterShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id: Uuid::default(),
                partitions,
                ..Default::default()
            });
            continue;
        }

        let topic_id = image.topic(&topic_name).map(|t| t.topic_id);

        let mut partitions: Vec<AlterShareGroupOffsetsResponsePartition> =
            Vec::with_capacity(rt.partitions.len());

        for rp in rt.partitions {
            let partition_record = image.partition(&topic_name, rp.partition_index);
            let Some((topic_id, partition_record)) = topic_id.zip(partition_record) else {
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some(
                        crate::share_coordinator::coordinator::message::UNKNOWN_TOPIC_OR_PARTITION
                            .to_owned(),
                    ),
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
    //
    // `mark_share` + `get_or_create_share` is Kafka's
    // `getOrMaybeCreateShareGroup(groupId, true)`: a group id with no prior
    // type lock is created as a share group here, exactly as the first
    // `ShareGroupHeartbeat` would create it.
    let ng = ng_opt.as_ref().expect("group coordinator is installed");
    ng.mark_share(&gid);
    let actor = ng.get_or_create_share(&gid);
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
        authorizer::{AuthorizationResult, Authorizer},
        codes,
        coordinator::unified::{GroupType, ShareGroupSeed, share::actor::ShareGroupActorMessage},
        test_support::DenyAll,
    };

    const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str =
        crate::share_coordinator::coordinator::message::UNKNOWN_TOPIC_OR_PARTITION;

    /// An authorizer with independently controllable answers for `Group`
    /// `Read`/`Alter` and per-topic `Read`, for the KIP-932
    /// `AlterShareGroupOffsets` ACL table (issue #727).
    #[derive(Debug, Default)]
    struct ScenarioAuthorizer {
        group_read: bool,
        group_alter: bool,
        denied_topics: Vec<String>,
    }

    impl Authorizer for ScenarioAuthorizer {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            req: &crate::authorizer::AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            use krabka_metadata::{AclOperation, ResourceType};
            let allowed = match (req.resource_type, req.operation) {
                (ResourceType::Group, AclOperation::Read) => self.group_read,
                (ResourceType::Group, AclOperation::Alter) => self.group_alter,
                (ResourceType::Topic, AclOperation::Read) => !self
                    .denied_topics
                    .iter()
                    .any(|t| t.as_str() == req.resource_name),
                _ => true,
            };
            if allowed {
                AuthorizationResult::Allow
            } else {
                AuthorizationResult::Deny
            }
        }
    }

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
                                error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
                                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                            },
                            AlterShareGroupOffsetsResponsePartition {
                                partition_index: 5,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
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

    /// Issue #727: krabka checked `Alter` on the group and never checked
    /// topic `Read`. Kafka checks `Read` on the group and `Read` on each
    /// topic, the topic check running BEFORE the unknown-topic lookup. Every
    /// row uses a fresh broker and group id.
    ///
    /// `(group Read granted, group Alter granted, topic name is denied,
    /// topic exists, expected top-level code, expected partition code)`.
    #[tokio::test]
    async fn acl_scenarios_match_kafka_semantics() {
        type Row = (&'static str, bool, bool, bool, bool, i16, Option<i16>);
        let rows: [Row; 4] = [
            (
                "group Read only is allowed (the normal share-consumer grant)",
                true,
                false,
                false,
                true,
                codes::NONE,
                Some(codes::NONE),
            ),
            (
                "group Alter only, with no Read, is refused",
                false,
                true,
                false,
                true,
                codes::GROUP_AUTHORIZATION_FAILED,
                None,
            ),
            (
                "group Read granted but the topic is denied",
                true,
                false,
                true,
                true,
                codes::NONE,
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                "group Read granted, topic unknown to the cluster",
                true,
                false,
                false,
                false,
                codes::NONE,
                Some(codes::UNKNOWN_TOPIC_OR_PARTITION),
            ),
        ];

        for (index, (case, group_read, group_alter, topic_denied, topic_exists, top, partition)) in
            rows.into_iter().enumerate()
        {
            let topic_name = "acl-topic";
            let authorizer = Arc::new(ScenarioAuthorizer {
                group_read,
                group_alter,
                denied_topics: if topic_denied {
                    vec![topic_name.to_string()]
                } else {
                    Vec::new()
                },
            });
            let (broker_handle, _dir) = start_broker(authorizer, true).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);
            if topic_exists {
                create_topic(&broker_handle, &broker, topic_name, &ctx).await;
                crate::share_coordinator::handlers::test_support::lead_share_state_partitions(
                    &broker,
                )
                .await;
            }
            let topic_id_before = if topic_exists {
                let topic_id = broker
                    .controller
                    .current_image()
                    .topic(topic_name)
                    .expect("topic metadata")
                    .topic_id;
                broker
                    .group_coordinator
                    .share_persister()
                    .expect("share persister")
                    .read_summary("g-acl", topic_id, 0)
                    .await
                    .expect("read state")
            } else {
                None
            };

            let resp = handle(
                &broker,
                alter_share_group_offsets_response::MAX_VERSION,
                1,
                &encode_request(&request("g-acl", topic_name, &[0])),
                &ctx,
            )
            .await
            .expect("handle");
            let resp = decode_response(&resp);

            assert!(resp.error_code == top, "row {index} ({case}): {resp:?}");
            if let Some(expected_partition_code) = partition {
                assert!(
                    resp.responses[0].partitions[0].error_code == expected_partition_code,
                    "row {index} ({case}): {resp:?}"
                );
            }
            if topic_exists && partition != Some(codes::NONE) {
                // A denied or otherwise-refused topic must not have its
                // start offset reset.
                let topic_id = broker
                    .controller
                    .current_image()
                    .topic(topic_name)
                    .expect("topic metadata")
                    .topic_id;
                let after = broker
                    .group_coordinator
                    .share_persister()
                    .expect("share persister")
                    .read_summary("g-acl", topic_id, 0)
                    .await
                    .expect("read state");
                assert!(after == topic_id_before, "row {index} ({case})");
            }
            broker_handle.shutdown().await;
        }
    }

    /// Issue #944: an empty `group_id` is refused with `INVALID_GROUP_ID`
    /// (24), and a `group_id` already locked to another protocol type is
    /// refused with `GROUP_ID_NOT_FOUND` (69), before any actor is touched.
    #[tokio::test]
    async fn invalid_and_wrong_type_group_ids_are_refused() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let empty_id_resp = handle(
            &broker,
            alter_share_group_offsets_response::MAX_VERSION,
            1,
            &encode_request(&request("", "t", &[0])),
            &ctx,
        )
        .await
        .expect("handle empty id");
        assert!(decode_response(&empty_id_resp).error_code == codes::INVALID_GROUP_ID);

        let _ = broker.group_coordinator.get_or_create_classic("classic-g");
        broker.group_coordinator.mark_classic("classic-g");
        assert!(broker.group_coordinator.group_type("classic-g") == Some(GroupType::Classic));
        let wrong_type_resp = handle(
            &broker,
            alter_share_group_offsets_response::MAX_VERSION,
            1,
            &encode_request(&request("classic-g", "t", &[0])),
            &ctx,
        )
        .await
        .expect("handle wrong-type id");
        assert!(decode_response(&wrong_type_resp).error_code == codes::GROUP_ID_NOT_FOUND);
        // The classic lock must not have been disturbed.
        assert!(broker.group_coordinator.group_type("classic-g") == Some(GroupType::Classic));
        broker_handle.shutdown().await;
    }

    /// Issue #944: a share group that does not exist yet is created and
    /// persisted by `AlterShareGroupOffsets` — matching Kafka's
    /// `getOrMaybeCreateShareGroup(groupId, true)` — and the group epoch is
    /// bumped with a `ShareGroupMetadata` record written before the
    /// partitions are initialized. The regression guard: once a partition is
    /// initialized this way, later member joins must not re-initialize it and
    /// change its start offset back.
    #[tokio::test]
    async fn alter_creates_and_persists_a_new_share_group() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        create_topic(&broker_handle, &broker, "new-topic", &ctx).await;
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
            .await;
        let topic_id = broker
            .controller
            .current_image()
            .topic("new-topic")
            .expect("topic metadata")
            .topic_id;

        assert!(broker.group_coordinator.group_type("g-new").is_none());

        let response = handle(
            &broker,
            alter_share_group_offsets_response::MAX_VERSION,
            1,
            &encode_request(&request("g-new", "new-topic", &[0])),
            &ctx,
        )
        .await
        .expect("handle alter");
        let response = decode_response(&response);
        assert!(response.error_code == codes::NONE, "{response:?}");
        assert!(response.responses[0].partitions[0].error_code == codes::NONE);

        assert!(broker.group_coordinator.group_type("g-new") == Some(GroupType::Share));
        let seed = broker
            .group_coordinator
            .cached_share_seed("g-new")
            .expect("share group persisted");
        assert!(seed.group_epoch == 1, "the first alter bumps epoch 0 -> 1");
        assert!(
            seed.state_partition_metadata
                .initialized
                .iter()
                .any(|t| t.topic_id == topic_id && t.partitions == vec![0]),
            "the partition lands in the persisted initialized set: {:?}",
            seed.state_partition_metadata
        );

        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let (_, _, start_offset, _) = persister
            .read_summary("g-new", topic_id, 0)
            .await
            .expect("read state")
            .expect("state present");
        assert!(start_offset == krabka_log::Offset(42));

        // A member joining afterwards must not re-initialize the partition
        // and reset its start offset: `reconcile_share_state` skips any
        // `(topic_id, partition)` already in `state.initialized`.
        let actor = broker.group_coordinator.get_or_create_share("g-new");
        for member_id in ["m1", "m2"] {
            let (tx, rx) = tokio::sync::oneshot::channel();
            actor
                .tx
                .send(ShareGroupActorMessage::Heartbeat {
                    request: ShareGroupHeartbeatRequest {
                        group_id: "g-new".into(),
                        member_id: member_id.into(),
                        member_epoch: 0,
                        subscribed_topic_names: Some(vec!["new-topic".into()]),
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
        }
        let (_, _, start_offset_after, _) = persister
            .read_summary("g-new", topic_id, 0)
            .await
            .expect("read state")
            .expect("state present");
        assert!(
            start_offset_after == krabka_log::Offset(42),
            "member joins must not move the start offset back"
        );
        broker_handle.shutdown().await;
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
    async fn reset_mutates_only_requested_valid_partitions_and_bumps_group_epoch() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        create_topic(&broker_handle, &broker, "reset-topic", &ctx).await;
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
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

        // Every alter call bumps the group epoch by exactly one, per Kafka's
        // `GroupMetadataManager.alterShareGroupOffsets`; there is no
        // exact-retry short-circuit, unlike the old per-partition
        // `state_epoch` scheme this replaces.
        let reset_request = request("g-reset", "reset-topic", &[0, 9]);
        for expected_group_epoch in [1, 2] {
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

            let (state_epoch, _, start_offset, _) = persister
                .read_summary("g-reset", topic_id, 0)
                .await
                .expect("read state")
                .expect("state present");
            assert!(state_epoch == expected_group_epoch);
            assert!(start_offset == krabka_log::Offset(42));
            let seed = broker
                .group_coordinator
                .cached_share_seed("g-reset")
                .expect("share group persisted");
            assert!(seed.group_epoch == expected_group_epoch);
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

        // A group epoch already exhausted at `i32::MAX` fails the whole batch
        // top-level, before any partition is touched.
        broker.group_coordinator.mark_share("g-overflow");
        let overflow_actor = broker.group_coordinator.get_or_create_share("g-overflow");
        let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
        overflow_actor
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                group_epoch: i32::MAX,
                ..Default::default()
            }))
            .await
            .expect("send seed");
        // `Seed` has no reply; round-trip a `Describe` to know it was applied
        // before the alter request below is sent.
        overflow_actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: seed_tx })
            .await
            .expect("send describe");
        seed_rx.await.expect("describe reply");

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
            overflow_response.error_code == codes::COORDINATOR_NOT_AVAILABLE,
            "{overflow_response:?}"
        );
        assert!(overflow_response.responses.is_empty());

        let state = persister
            .read_summary("g-reset", topic_id, 9)
            .await
            .expect("read unrequested state");
        assert!(state.is_none());
        broker_handle.shutdown().await;
    }
}
