//! `DeleteRecords` (`api_key=21`). Only the leader trims its local segments.
//!
//! The follower picks up the new `log_start_offset` on its next Fetch, through
//! the existing `OFFSET_OUT_OF_RANGE` recovery path. This matches the Apache
//! Kafka model.
//!
//! KFC-1 adds one bound: on a topic that schedules delivery, the trim stops at
//! the partition's delivery watermark. See [`delivery_capped`].
//!
//! # KFC-9: trimming a partition needs two people
//!
//! A trim destroys committed records, so it is one of the transitions the
//! break-glass two-person rule gates. KIP-107 defines the request and it gains
//! no field for this: an operator gets an approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and then runs `kafka-delete-records`.
//!
//! # KFC-9: a frozen partition is never trimmed
//!
//! A write freeze refuses every operation that removes data from the topic it
//! covers, so a frozen partition answers `POLICY_VIOLATION (44)` here whatever
//! the caller holds. The check runs ahead of the two-person rule, because an
//! approval to trim does not defeat a freeze and a refusal must not spend one.
//!
//! A refused partition answers `POLICY_VIOLATION (44)` on its own row. No
//! version of the `DeleteRecords` response carries an `error_message`, so the
//! refusal text reaches the operator through the audit log and the broker's own
//! log rather than the wire.
//!
//! This trim writes no metadata record, so there is nothing for the consumed
//! proposal to ride beside. The broker appends the consume on its own and only
//! then trims. Consume-then-transition is the safe order of the two: a crash
//! between them loses the approval, where the reverse order would leave an
//! approval that a second trim could spend again. The gate is active only when
//! `[break_glass]` names an approver set.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_audit::PrivilegedPhase;
use krabka_log::Offset;
use krabka_metadata::{AclOperation, BreakGlassAction, MetadataImage, MetadataRecord};
use krabka_protocol::{
    Decode,
    owned::{
        delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest},
        delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        },
    },
};
use uuid::Uuid;

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    break_glass::{
        gate::{self, BreakGlassDenial},
        handlers::audit::{GatedTransition, audit_transition},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    config::BreakGlassConfig,
    error::BrokerError,
    handlers::RequestContext,
    time_util::now_ms,
};

fn denied_topic_names(
    acl_results: &std::collections::HashMap<&str, AuthorizationResult>,
) -> std::collections::HashSet<String> {
    acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect()
}

fn partition_result(
    partition_index: i32,
    low_watermark: i64,
    error_code: i16,
) -> DeleteRecordsPartitionResult {
    DeleteRecordsPartitionResult {
        partition_index,
        low_watermark,
        error_code,
        ..Default::default()
    }
}

fn error_partition_result(partition_index: i32, error_code: i16) -> DeleteRecordsPartitionResult {
    partition_result(partition_index, -1, error_code)
}

fn topic_result(
    name: String,
    partitions: Vec<DeleteRecordsPartitionResult>,
) -> DeleteRecordsTopicResult {
    DeleteRecordsTopicResult {
        name,
        partitions,
        ..Default::default()
    }
}

fn delete_records_response(topics: Vec<DeleteRecordsTopicResult>) -> DeleteRecordsResponse {
    DeleteRecordsResponse {
        topics,
        ..Default::default()
    }
}

fn target_offset(requested_offset: i64, high_watermark: i64) -> i64 {
    krabka_verified::delete_records_target(requested_offset, high_watermark)
}

fn offset_out_of_range(target: i64, log_end_offset: i64) -> bool {
    krabka_verified::delete_records_offset_out_of_range(target, log_end_offset)
}

/// KFC-1: the offset a trim may actually reach.
///
/// `watermark` is the partition's delivery watermark, and `None` on a topic
/// that delivers immediately. Such a topic has every durable record visible
/// already, so the resolved target stands and this is the identity.
///
/// A topic that schedules delivery stops the trim at the watermark. The `-1`
/// sentinel resolves to the high watermark, and on a scheduled partition that
/// sits above every record that has not come due, so a routine trim would
/// delete records the broker promised to deliver and no consumer was allowed
/// to read. An explicit target is capped for the same reason: what the cap
/// removes is exactly the undelivered tail, and the response reports the log
/// start offset the trim reached.
fn delivery_capped(target: Offset, watermark: Option<Offset>) -> Offset {
    watermark.map_or(target, |visible| target.min(visible))
}

#[tracing::instrument(
    name = "handle_delete_records",
    level = "info",
    skip_all,
    fields(api = "DeleteRecords", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteRecordsRequest::decode(&mut cur, version)?;

    let partitions = broker.partitions.clone();

    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the trim loop and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row for that topic.
    let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Delete,
        topic_names.iter().copied(),
    );
    let denied_topics = denied_topic_names(&acl_results);

    let env = TrimEnv {
        broker,
        image: &image,
        ctx,
        partitions: &partitions,
    };
    // KFC-9: the proposals this request already spent. One proposal on a bare
    // topic name covers every partition of it, and it is spent once.
    let mut spent: HashSet<Uuid> = HashSet::new();
    let mut topic_results: Vec<DeleteRecordsTopicResult> = Vec::with_capacity(req.topics.len());

    for topic in req.topics {
        // Per-topic ACL check: if denied, mark every partition in the topic.
        if denied_topics.contains(&topic.name) {
            let part_results: Vec<DeleteRecordsPartitionResult> = topic
                .partitions
                .iter()
                .map(|fp| {
                    error_partition_result(fp.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                })
                .collect();
            topic_results.push(topic_result(topic.name, part_results));
            continue;
        }

        let mut part_results: Vec<DeleteRecordsPartitionResult> =
            Vec::with_capacity(topic.partitions.len());

        for fp in topic.partitions {
            part_results.push(trim_one(&env, &mut spent, &topic.name, &fp).await);
        }

        topic_results.push(topic_result(topic.name, part_results));
    }

    let resp = delete_records_response(topic_results);
    crate::handlers::encode_response(&resp, version)
}

/// Everything one partition's trim reads, and nothing it writes.
struct TrimEnv<'a> {
    broker: &'a Broker,
    image: &'a MetadataImage,
    ctx: &'a RequestContext<'a>,
    partitions: &'a crate::partition_registry::PartitionRegistry,
}

/// Trim one partition, and answer the row the response carries for it.
///
/// `spent` carries the proposals this request already consumed, so one approval
/// that covers a whole topic is spent once however many partitions the request
/// names.
async fn trim_one(
    env: &TrimEnv<'_>,
    spent: &mut HashSet<Uuid>,
    topic: &str,
    fp: &DeleteRecordsPartition,
) -> DeleteRecordsPartitionResult {
    let index = fp.partition_index;
    let part_opt = env.partitions.get(topic, krabka_ids::PartitionIndex(index));
    let Some(part) = part_opt else {
        return error_partition_result(index, codes::UNKNOWN_TOPIC_OR_PARTITION);
    };

    let cur_leader = part
        .current_leader
        .load(std::sync::atomic::Ordering::Acquire);
    if cur_leader != env.broker.config.node_id {
        return error_partition_result(index, codes::NOT_LEADER_OR_FOLLOWER);
    }

    // KFC-9: a write freeze refuses every operation that removes data from the
    // topic it covers, and it answers ahead of the two-person rule. That order
    // is the rule: a break-glass approval to trim does not defeat a freeze, and
    // a trim the freeze refuses must not spend an approval on its way to being
    // refused.
    //
    // No version of the `DeleteRecords` response carries an `error_message`, so
    // the reason reaches the operator through the broker's log alone. Like the
    // produce gate, this refusal emits no privileged-action audit event: a
    // freeze is not a break-glass act, and the registry entry that caused it is
    // already in the metadata log.
    if let Some(verdict) = crate::freeze::resolve::resolve_freeze_verdict(env.image, topic) {
        tracing::warn!(
            %topic,
            partition = index,
            refusal = %verdict.removal_message(),
            "DeleteRecords refused by a freeze"
        );
        return error_partition_result(index, codes::POLICY_VIOLATION);
    }

    // KFC-9: the two-person rule answers here, before the broker reads a single
    // offset. The approval is not spent yet: a request that then fails its
    // range check leaves the proposal usable, so an operator does not have to
    // gather two signatures again over a typo.
    let consumed = match authorize_trim(env.image, &env.broker.config.break_glass, topic, index) {
        Ok(consumed) => consumed,
        Err(denial) => return refuse_trim(env, topic, index, &denial),
    };

    // Translate offset == -1 → high_watermark per Kafka semantics.
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // `hw`/`leo` are `Offset`; the boundary helpers work in raw `i64`, so
    // unwrap at the seam and re-wrap `requested` for the `Offset`-typed
    // `trim_to_offset` call below.
    let requested = Offset(target_offset(fp.offset, hw.0));

    // The range check reads the offset the admin asked for. A target above the
    // log end is still out of range, and the KFC-1 cap below must not turn that
    // mistake into a silent partial trim.
    if offset_out_of_range(requested.0, leo.0) {
        return error_partition_result(index, codes::OFFSET_OUT_OF_RANGE);
    }

    // KFC-1: hold the trim at the delivery watermark. The recompute runs under
    // the log mutex against the partition's own clock, so it agrees with the
    // cap a fetch at this instant would apply, rather than with whatever the
    // last scheduler sweep published. It answers `None` on a topic that
    // delivers immediately, where the target stands.
    let target = delivery_capped(
        requested,
        part.delivery
            .publish_now(&part.log)
            .map(|delivery| delivery.watermark),
    );

    // KFC-9: spend the approval before the trim removes anything.
    let proposal_id = match spend_approval(env.broker, spent, consumed).await {
        Ok(proposal_id) => proposal_id,
        Err(error) => {
            tracing::warn!(
                %topic, partition = index, %error,
                "DeleteRecords could not spend the break-glass approval"
            );
            return error_partition_result(index, codes::COORDINATOR_NOT_AVAILABLE);
        }
    };

    match part.trim_to_offset(target).await {
        Ok(new_start) => {
            audit_transition(
                &env.broker.audit_log,
                &env.broker.config.break_glass,
                env.ctx,
                &GatedTransition {
                    action: BreakGlassAction::DeleteRecords,
                    target: &trim_target(topic, index),
                    phase: PrivilegedPhase::Applied,
                    proposal_id,
                    reason: "records deleted below the trim point",
                },
            );
            // Unwrap the `Offset` into the wire `i64` `low_watermark`.
            partition_result(index, new_start.0, codes::NONE)
        }
        Err(e) => {
            tracing::warn!(
                %topic, partition = index, error = %e,
                "DeleteRecords: trim_to_offset failed"
            );
            error_partition_result(index, codes::UNKNOWN_SERVER_ERROR)
        }
    }
}

/// KFC-9: find the approved proposal that authorizes a trim of one partition,
/// and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. A trim then behaves as it does on a cluster with no such section.
fn authorize_trim(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    topic: &str,
    partition: i32,
) -> Result<Option<MetadataRecord>, BreakGlassDenial> {
    if !gate::is_gated(config) {
        return Ok(None);
    }
    gate::authorize(
        image,
        config,
        BreakGlassAction::DeleteRecords,
        &trim_target(topic, partition),
        now_ms(),
    )
    .map(Some)
}

/// The records that spending this approval appends, and the proposal they name.
///
/// A trim writes no metadata record, so the append carries the consume alone.
/// The record list is empty when there is nothing to append: the broker gates
/// nothing, or this request already spent the proposal on another partition
/// that one topic-wide approval covers.
fn consume_records(
    spent: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
) -> (Option<Uuid>, Vec<MetadataRecord>) {
    let Some(consumed) = consumed else {
        return (None, Vec::new());
    };
    let Some(proposal_id) = consumed_proposal_id(&consumed) else {
        return (None, Vec::new());
    };
    if spent.insert(proposal_id) {
        (Some(proposal_id), vec![consumed])
    } else {
        (Some(proposal_id), Vec::new())
    }
}

/// Append the consumed proposal, and answer the proposal it names.
///
/// # Errors
///
/// Returns the submit failure when the quorum did not take the consume. No trim
/// runs in that case, so the approval stays unspent.
async fn spend_approval(
    broker: &Broker,
    spent: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
) -> Result<Option<Uuid>, krabka_raft::RaftError> {
    let (proposal_id, records) = consume_records(spent, consumed);
    if !records.is_empty() {
        broker.controller.submit_change(records).await?;
    }
    Ok(proposal_id)
}

/// Refuse one partition: count it, audit it, and build its error row.
///
/// The row carries the code alone, because no `DeleteRecords` response version
/// has an `error_message` field to put the reason in.
fn refuse_trim(
    env: &TrimEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> DeleteRecordsPartitionResult {
    let message = denial.to_string();
    tracing::warn!(%topic, partition, refusal = %message, "DeleteRecords refused");
    break_glass_metrics::record_refusal(&env.broker.metrics, denial.action);
    audit_transition(
        &env.broker.audit_log,
        &env.broker.config.break_glass,
        env.ctx,
        &GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: &trim_target(topic, partition),
            phase: PrivilegedPhase::Refused,
            proposal_id: denial.proposal_id(),
            reason: &message,
        },
    );
    error_partition_result(partition, codes::POLICY_VIOLATION)
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
fn trim_target(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

/// The proposal that a consumed record names.
///
/// [`gate::authorize`] only ever answers with a proposal record, so the `None`
/// arm costs one match rather than a panic.
fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::{assert, check};
    use krabka_protocol::owned::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };
    use krabka_security::Principal;

    use super::*;
    use crate::{
        broker::Broker,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 2;

    fn request(topic: &str, partitions: &[(i32, i64)]) -> DeleteRecordsRequest {
        DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.into(),
                partitions: partitions
                    .iter()
                    .map(|(partition_index, offset)| DeleteRecordsPartition {
                        partition_index: *partition_index,
                        offset: *offset,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        DeleteRecordsRequest,
        DeleteRecordsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn drive(
        broker: &Broker,
        req: &DeleteRecordsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DeleteRecordsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    #[test]
    fn denied_topic_names_keeps_only_denied_decisions() {
        let acl_results = std::collections::HashMap::from([
            ("denied", AuthorizationResult::Deny),
            ("allowed", AuthorizationResult::Allow),
        ]);

        let denied = denied_topic_names(&acl_results);

        let expected = std::collections::HashSet::from(["denied".to_string()]);
        assert!(denied == expected);
    }

    #[test]
    fn offset_helpers_cover_delete_records_boundaries() {
        check!(target_offset(-1, 42) == 42);
        check!(target_offset(-2, 42) == -2);
        check!(target_offset(7, 42) == 7);

        check!(!offset_out_of_range(0, 10));
        check!(!offset_out_of_range(10, 10));
        check!(offset_out_of_range(-1, 10));
        check!(offset_out_of_range(11, 10));
    }

    #[test]
    fn the_delivery_cap_only_lowers_a_target_above_the_watermark() {
        let cases = [
            // A topic that delivers immediately has no watermark to cap with.
            (Offset(9), None, Offset(9)),
            (Offset(9), Some(Offset(4)), Offset(4)),
            (Offset(4), Some(Offset(4)), Offset(4)),
            (Offset(2), Some(Offset(4)), Offset(2)),
            // Nothing is visible yet, so nothing may be deleted.
            (Offset(9), Some(Offset(0)), Offset(0)),
        ];
        for (target, watermark, expected) in cases {
            check!(
                delivery_capped(target, watermark) == expected,
                "{target:?} {watermark:?}"
            );
        }
    }

    #[test]
    fn response_helpers_preserve_topic_and_partition_fields() {
        let denied = error_partition_result(7, codes::TOPIC_AUTHORIZATION_FAILED);
        let expected_denied = DeleteRecordsPartitionResult {
            partition_index: 7,
            low_watermark: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(denied == expected_denied);

        let ok = partition_result(3, 44, codes::NONE);
        let expected_ok = DeleteRecordsPartitionResult {
            partition_index: 3,
            low_watermark: 44,
            error_code: codes::NONE,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let topic = topic_result("orders".into(), vec![denied]);
        let expected_topic = DeleteRecordsTopicResult {
            name: "orders".into(),
            partitions: vec![expected_denied],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(topic == expected_topic);

        let resp = delete_records_response(vec![topic]);
        let expected_resp = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![expected_topic],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected_resp);
    }

    #[tokio::test]
    async fn handle_denied_topic_returns_topic_auth_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request("secret", &[(0, 3), (2, -1)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: "secret".into(),
                partitions: vec![
                    DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                    },
                    DeleteRecordsPartitionResult {
                        partition_index: 2,
                        low_watermark: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                    },
                ],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_partition_preserves_requested_index() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request("missing", &[(4, 0)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: "missing".into(),
                partitions: vec![DeleteRecordsPartitionResult {
                    partition_index: 4,
                    low_watermark: -1,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    // Activation time of a batch that has long since come due.
    const DELIVERED_MS: i64 = 1_700_000_000_000;
    // Activation time of a batch that still waits. It sits far enough ahead
    // that every clock this test can read calls it pending, so the schedule
    // holds without a mock timeline.
    const PENDING_MS: i64 = 4_100_000_000_000;

    // A two-record batch that activates at `activation_ms`, stamped with the
    // epoch the partition writer expects from a leader append.
    fn batch_at(activation_ms: i64, leader_epoch: i32) -> krabka_protocol::records::RecordBatch {
        krabka_protocol::records::RecordBatch {
            partition_leader_epoch: leader_epoch,
            ..crate::delivery::test_support::batch_at(activation_ms)
        }
    }

    // Create `topic` with the given `delivery.mode`, then append one batch
    // that has come due and one that has not. Two records per batch puts the
    // log end offset at 4, and a scheduled topic's delivery watermark at 2.
    async fn topic_holding_a_pending_batch(
        broker_handle: &crate::broker::BrokerHandle,
        broker: &Broker,
        topic: &str,
        delivery_mode: Option<&str>,
        ctx: &crate::handlers::RequestContext<'_>,
    ) {
        use krabka_protocol::owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
            create_topics_response::{self, CreateTopicsResponse},
        };

        let version = create_topics_response::MAX_VERSION;
        let create = crate::test_support::encode_request(
            &CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: topic.to_owned(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: delivery_mode
                        .map(|mode| CreatableTopicConfig {
                            name: crate::config_keys::DELIVERY_MODE.to_owned(),
                            value: Some(mode.to_owned()),
                            ..Default::default()
                        })
                        .into_iter()
                        .collect(),
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let bytes = crate::handlers::create_topics::handle(broker, version, 1, &create, ctx)
            .await
            .expect("CreateTopics");
        let created: CreateTopicsResponse = crate::test_support::decode_response(&bytes, version);
        assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
        broker_handle.wait_until_partition_present(topic, 0).await;

        let expected_policy = if delivery_mode == Some(crate::config_keys::DELIVERY_MODE_SCHEDULED)
        {
            krabka_log::DeliveryPolicy::Scheduled
        } else {
            krabka_log::DeliveryPolicy::Immediate
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if broker_handle
                    .partition_log_config_for_test(topic, 0)
                    .is_some_and(|config| config.delivery_policy == expected_policy)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the delivery mode reaches the partition log");

        let part = broker
            .partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        let leader_epoch = part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        for activation_ms in [DELIVERED_MS, PENDING_MS] {
            part.produce_batch(batch_at(activation_ms, leader_epoch))
                .await
                .expect("append a batch");
        }
    }

    #[tokio::test]
    async fn a_trim_stops_at_the_delivery_watermark_of_a_scheduled_topic() {
        // The `-1` sentinel resolves to the high watermark, which is 4 on both
        // topics: replication is never gated on delivery. The scheduled topic
        // keeps the batch that has not come due.
        let cases = [
            ("delete-records-immediate-delivery", None, 4),
            (
                "delete-records-scheduled-delivery",
                Some(crate::config_keys::DELIVERY_MODE_SCHEDULED),
                2,
            ),
        ];

        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let admin = principal("admin");
        let peer = peer();
        let ctx = test_context(&admin, &peer);

        for (topic, delivery_mode, expected_low_watermark) in cases {
            topic_holding_a_pending_batch(&broker_handle, &broker, topic, delivery_mode, &ctx)
                .await;

            let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

            let expected = DeleteRecordsResponse {
                throttle_time_ms: 0,
                topics: vec![DeleteRecordsTopicResult {
                    name: topic.into(),
                    partitions: vec![DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: expected_low_watermark,
                        error_code: codes::NONE,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            };
            check!(resp == expected, "{topic}");
        }

        broker_handle.shutdown().await;
    }

    // ── KFC-9: the break-glass gate over a trim ─────────────────────────

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const NOW_MS: i64 = 60_000;

    fn gated_config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..BreakGlassConfig::default()
        }
    }

    /// A proposal that two people approved, and that has not expired against
    /// the wall clock the gate reads.
    fn approved_proposal(target: &str) -> krabka_metadata::BreakGlassProposalRecord {
        let now = now_ms();
        krabka_metadata::BreakGlassProposalRecord {
            proposal_id: PROPOSAL,
            action: BreakGlassAction::DeleteRecords,
            target: target.to_owned(),
            proposer: "User:carol".to_owned(),
            reason: "the tail is poison".to_owned(),
            created_at_ms: now - 1_000,
            expires_at_ms: now + 600_000,
            approvals: vec![
                crate::break_glass::gate::tests::approval("User:alice"),
                crate::break_glass::gate::tests::approval("User:bob"),
            ],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    fn image_of(proposals: &[krabka_metadata::BreakGlassProposalRecord]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for proposal in proposals {
            image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        image
    }

    #[test]
    fn the_trim_gate_answers_from_the_proposal_registry() {
        let cases: [(&'static str, MetadataImage, BreakGlassConfig, bool); 5] = [
            (
                "an approved proposal on the partition",
                image_of(&[approved_proposal("orders-3")]),
                gated_config(),
                true,
            ),
            (
                "an approved proposal on the whole topic",
                image_of(&[approved_proposal("orders")]),
                gated_config(),
                true,
            ),
            (
                "a proposal for another partition",
                image_of(&[approved_proposal("orders-4")]),
                gated_config(),
                false,
            ),
            ("no proposal at all", image_of(&[]), gated_config(), false),
            (
                "no approver set, so nothing is gated",
                image_of(&[]),
                BreakGlassConfig::default(),
                true,
            ),
        ];
        for (label, image, config, expected) in cases {
            let authorized = authorize_trim(&image, &config, "orders", 3).is_ok();
            check!(authorized == expected, "case {label}");
        }
    }

    #[test]
    fn a_refused_trim_names_the_partition_it_refused() {
        let denial = authorize_trim(&image_of(&[]), &gated_config(), "orders", 3)
            .expect_err("no proposal covers orders-3");

        check!(denial.action == BreakGlassAction::DeleteRecords);
        check!(denial.target == "orders-3");
    }

    #[test]
    fn the_append_carries_the_consume_alone_and_spends_it_once() {
        let consumed =
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..approved_proposal("orders")
            });
        let mut spent = HashSet::new();

        let first = consume_records(&mut spent, Some(consumed.clone()));
        let second = consume_records(&mut spent, Some(consumed.clone()));
        let ungated = consume_records(&mut spent, None);

        // A trim writes no metadata record, so the consume is the whole append.
        assert!(first == (Some(PROPOSAL), vec![consumed]));
        assert!(second == (Some(PROPOSAL), vec![]));
        assert!(ungated == (None, vec![]));
    }

    #[tokio::test]
    async fn a_refused_trim_deletes_nothing() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.break_glass = gated_config();
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal("admin");
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        topic_holding_a_pending_batch(&broker_handle, &broker, "orders", None, &ctx).await;
        let part = broker
            .partitions
            .get("orders", krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        let before = part.log_start_offset();

        let resp = drive(&broker, &request("orders", &[(0, -1)]), &principal, &peer).await;

        let expected = vec![DeleteRecordsTopicResult {
            name: "orders".to_owned(),
            partitions: vec![DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: -1,
                error_code: codes::POLICY_VIOLATION,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }];
        assert!(resp.topics == expected, "{resp:?}");
        // The refusal refused: the log start offset did not move.
        check!(part.log_start_offset() == before);
        broker_handle.shutdown().await;
    }
}
