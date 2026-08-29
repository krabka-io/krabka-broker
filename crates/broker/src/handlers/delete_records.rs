//! `DeleteRecords` (`api_key=21`). Only the leader trims its local segments.
//!
//! The follower picks up the new `log_start_offset` on its next Fetch, through
//! the existing `OFFSET_OUT_OF_RANGE` recovery path. This matches the Apache
//! Kafka model.
//!
//! KFC-1 adds one bound: on a topic that schedules delivery, the trim stops at
//! the partition's delivery watermark. See [`delivery_capped`].
//!
//! This file is the module root and holds the request loop. The ACL reduction
//! lives in `authz`, the response constructors in `response`, and the offset
//! boundary decisions in `offsets`.

use bytes::Bytes;
use krabka_log::Offset;
use krabka_metadata::AclOperation;
use krabka_protocol::{
    Decode,
    owned::{
        delete_records_request::DeleteRecordsRequest,
        delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    },
};

mod authz;
mod offsets;
mod response;

#[cfg(test)]
mod tests;

use self::{
    authz::denied_topic_names,
    offsets::{delivery_capped, offset_out_of_range, target_offset},
    response::{delete_records_response, error_partition_result, partition_result, topic_result},
};
use crate::{authorizer::authorize_topics, broker::Broker, codes, error::BrokerError};

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
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteRecordsRequest::decode(&mut cur, version)?;

    let partitions = broker.partitions.clone();
    let node_id = broker.config.node_id;

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
            let part_opt =
                partitions.get(&topic.name, krabka_ids::PartitionIndex(fp.partition_index));
            let Some(part) = part_opt else {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            };

            let cur_leader = part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire);
            if cur_leader != node_id {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::NOT_LEADER_OR_FOLLOWER,
                ));
                continue;
            }

            // Translate offset == -1 → high_watermark per Kafka semantics.
            let leo = part.log_end_offset();
            let hw = part.high_watermark().await;
            // `hw`/`leo` are `Offset`; the boundary helpers work in raw
            // `i64`, so unwrap at the seam and re-wrap `requested` for the
            // `Offset`-typed `trim_to_offset` call below.
            let requested = Offset(target_offset(fp.offset, hw.0));

            // The range check reads the offset the admin asked for. A target
            // above the log end is still out of range, and the KFC-1 cap
            // below must not turn that mistake into a silent partial trim.
            if offset_out_of_range(requested.0, leo.0) {
                part_results.push(error_partition_result(
                    fp.partition_index,
                    codes::OFFSET_OUT_OF_RANGE,
                ));
                continue;
            }

            // KFC-1: hold the trim at the delivery watermark. The recompute
            // runs under the log mutex against the partition's own clock, so
            // it agrees with the cap a fetch at this instant would apply,
            // rather than with whatever the last scheduler sweep published.
            // It answers `None` on a topic that delivers immediately, where
            // the target stands.
            let target = delivery_capped(
                requested,
                part.delivery
                    .publish_now(&part.log)
                    .map(|delivery| delivery.watermark),
            );

            match part.trim_to_offset(target).await {
                Ok(new_start) => {
                    // Unwrap the `Offset` into the wire `i64` `low_watermark`.
                    part_results.push(partition_result(
                        fp.partition_index,
                        new_start.0,
                        codes::NONE,
                    ));
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %topic.name, partition = fp.partition_index, error = %e,
                        "DeleteRecords: trim_to_offset failed"
                    );
                    part_results.push(error_partition_result(
                        fp.partition_index,
                        codes::UNKNOWN_SERVER_ERROR,
                    ));
                }
            }
        }

        topic_results.push(topic_result(topic.name, part_results));
    }

    let resp = delete_records_response(topic_results);
    crate::handlers::encode_response(&resp, version)
}
