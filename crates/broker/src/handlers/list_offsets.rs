//! `ListOffsets` (`api_key=2`). The handler resolves the EARLIEST / LATEST
//! sentinels with each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager).
//! Local-retention deletes some offsets locally, but they still live in the
//! remote tier, and this keeps them visible. KIP-1005's latest-tiered (`-5`)
//! and KIP-1023's earliest-pending-upload (`-6`) sentinels read the same
//! metadata asynchronously. KIP-1075 bounds that remote work by the request
//! timeout and resolves all requested partitions concurrently.
//!
//! Positive-timestamp lookups resolve against the remote tier first, because
//! it holds the oldest records. They then fall back to the local log's
//! time index (KIP-405/734). The handler resolves the `MAX_TIMESTAMP` (-3) and
//! `EARLIEST_LOCAL_TIMESTAMP` (-4) sentinels against the local log.
//!
//! KFC-1 changes one sentinel and no other: on a topic that schedules
//! delivery, LATEST reports the partition's delivery watermark instead of its
//! log end offset. See [`latest_offset`](self::local::latest_offset).
//!
//! Every other answer is decided by one bound, Kafka's `lastFetchableOffset`:
//! the log end offset for a request that is not a client's, the high watermark
//! for a `read_uncommitted` client, and the last stable offset (KIP-98) for a
//! `read_committed` one. `Partition.fetchOffsetForTimestamp` chooses it once
//! and then uses it twice over. LATEST *is* the bound, so a `read_committed`
//! consumer that seeks to end stops in front of the records of a transaction
//! that is still open instead of stepping over them. Every sentinel that
//! resolves against record data -- `MAX_TIMESTAMP`, the two tiered sentinels,
//! and a positive timestamp -- is refused with `UNKNOWN_OFFSET` when it lands
//! at or above the bound, so no client can read an offset past its own end of
//! partition by asking for it a different way. EARLIEST and
//! `EARLIEST_LOCAL_TIMESTAMP` are the exceptions Kafka leaves unmeasured:
//! both resolve from the start of the log, which is never above the bound. See
//! [`FetchBound`](self::bound::FetchBound),
//! [`fetch_bound`](self::bound::fetch_bound) and
//! [`last_fetchable_offset`](self::bound::last_fetchable_offset).

use bytes::{Bytes, BytesMut};
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::{ListOffsetsResponse, ListOffsetsTopicResponse},
    },
    primitives::{
        array::{get_array_len, put_array_len},
        fixed::{get_i32, get_i64, put_i16, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    },
};

mod bound;
mod diskless;
mod local;
mod remote;
mod resolve;
mod response;
mod sentinels;
mod timestamp;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    bound::fetch_bound,
    remote::{concurrently, remote_timeout},
    resolve::resolve_partition,
    response::error_response,
};
use crate::{broker::Broker, codes, error::BrokerError};

fn decode_request(req_bytes: &[u8], version: i16) -> Result<ListOffsetsRequest, BrokerError> {
    if version != 0 {
        return Ok(ListOffsetsRequest::decode(&mut &*req_bytes, version)?);
    }

    let mut cur = req_bytes;
    let replica_id = get_i32(&mut cur)?;
    let topic_count = get_array_len(&mut cur, false)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = get_string_owned(&mut cur)?;
        let partition_count = get_array_len(&mut cur, false)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let partition_index = get_i32(&mut cur)?;
            let timestamp = get_i64(&mut cur)?;
            let _max_num_offsets = get_i32(&mut cur)?;
            partitions.push(ListOffsetsPartition {
                partition_index,
                timestamp,
                ..Default::default()
            });
        }
        topics.push(ListOffsetsTopic {
            name,
            partitions,
            ..Default::default()
        });
    }
    Ok(ListOffsetsRequest {
        replica_id,
        topics,
        ..Default::default()
    })
}

fn encode_v0_response(resp: &ListOffsetsResponse) -> Bytes {
    let mut buf = BytesMut::new();
    put_array_len(&mut buf, resp.topics.len(), false);
    for topic in &resp.topics {
        put_string(&mut buf, &topic.name);
        put_array_len(&mut buf, topic.partitions.len(), false);
        for partition in &topic.partitions {
            put_i32(&mut buf, partition.partition_index);
            put_i16(&mut buf, partition.error_code);
            let offsets = usize::from(partition.error_code == codes::NONE);
            put_array_len(&mut buf, offsets, false);
            if offsets == 1 {
                put_i64(&mut buf, partition.offset);
            }
        }
    }
    buf.freeze()
}

#[tracing::instrument(
    name = "handle_list_offsets",
    level = "info",
    skip_all,
    fields(api = "ListOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    {
        let req = decode_request(req_bytes, version)?;
        // `isolation_level` decodes only from v2 up; v1 leaves it at 0, which
        // is `read_uncommitted`, exactly as Kafka treats a v1 request.
        let bound = fetch_bound(req.replica_id, req.isolation_level);

        // ── ACL preamble ────────────────────────────────────────────
        // Per-topic `Describe` on `Topic(name)`. A denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested; authorized topics proceed unchanged.
        let acl_image = controller.current_image();

        let timeout = remote_timeout(
            version,
            req.timeout_ms,
            crate::config_keys::resolve_remote_list_offsets_timeout(
                &acl_image,
                broker.config.node_id,
            ),
        );
        let topics_out = concurrently(req.topics.into_iter().map(|topic| {
            let acl_image = acl_image.clone();
            async move {
                let name = topic.name;
                let partitions = if crate::handlers::acl_denied(
                    broker.config.authorizer.as_ref(),
                    &acl_image,
                    ctx,
                    ResourceType::Topic,
                    &name,
                    AclOperation::Describe,
                ) {
                    topic
                        .partitions
                        .into_iter()
                        .map(|part| {
                            error_response(part.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                        })
                        .collect()
                } else {
                    concurrently(topic.partitions.into_iter().map(|part| {
                        resolve_partition(broker, &name, part, version, timeout, bound)
                    }))
                    .await
                };
                ListOffsetsTopicResponse {
                    name,
                    partitions,
                    ..Default::default()
                }
            }
        }))
        .await;

        let resp = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        if version == 0 {
            Ok(encode_v0_response(&resp))
        } else {
            crate::handlers::encode_response(&resp, version)
        }
    }
}
