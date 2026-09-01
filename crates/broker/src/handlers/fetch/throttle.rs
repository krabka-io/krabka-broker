//! The two throttles a finished fetch passes: the KIP-73 leader-side
//! replication throttle on a follower fetch, and the KIP-13 consumer byte
//! rate together with the KIP-124 request quota on a client fetch.

use krabka_protocol::{owned::fetch_response::FetchableTopicResponse, records::RecordsPayload};
use krabka_units::{Time, convert::TimeExt};
use num_traits::ToPrimitive as _;

use crate::broker::Broker;

pub(super) fn throttle_follower_responses(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    follower_id: i32,
    responses: &mut [FetchableTopicResponse],
) {
    use crate::throttle::TopicThrottle;
    let follower_id = krabka_metadata::NodeId(u64::try_from(follower_id).unwrap_or(0));
    let mut byte_count = 0;
    let mut indexes = Vec::new();
    for (topic_index, topic) in responses.iter().enumerate() {
        let throttle = TopicThrottle::for_topic(image, &topic.topic);
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if throttle
                .leader
                .contains(partition.partition_index, follower_id)
            {
                byte_count += partition
                    .records
                    .as_ref()
                    .map_or(0, RecordsPayload::payload_len) as u64;
                indexes.push((topic_index, partition_index));
            }
        }
    }
    if byte_count > 0 {
        let granted = broker.throttle_state.leader_out.try_consume(byte_count);
        if granted < byte_count {
            truncate_throttled_responses(responses, &indexes, granted);
        }
    }
}

pub(super) fn apply_consumer_fetch_quota(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    handler_start: std::time::Instant,
    responses: &[FetchableTopicResponse],
) -> i32 {
    let data_delay = consume_consumer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        sum_response_bytes(responses),
        broker.config.quota_throttle_max,
    );
    let elapsed_micros = u64::try_from(
        handler_start
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)),
    )
    .expect("elapsed microseconds clamped to u64");
    let request_delay = crate::quota::consume_request_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        elapsed_micros,
        broker.config.quota_throttle_max,
    );
    // KIP-219: the connection is muted for the larger of the two delays.
    // Resolving it through the metric records the throttle phase and the quota
    // that caused it, and hands back the delay the response reports.
    let delay = broker.metrics.record_applied_throttle(
        super::FETCH_API_KEY,
        &[
            (crate::metrics::QuotaType::Fetch, data_delay),
            (crate::metrics::QuotaType::Request, request_delay),
        ],
    );
    if delay <= <Time as TimeExt>::ZERO {
        return 0;
    }
    // KIP-219: the window goes back to the connection loop, which mutes the
    // connection after the fetch plan is written. Sleeping here would delay the
    // records the client is already waiting on.
    context.record_throttle(delay);
    crate::quota::throttle_time_ms(delay)
}

/// KIP-73 leader-side throttle: walk `throttled_idxs` in order and drop
/// whole-partition chunks until the remaining throttled bytes fit in
/// `budget`.
///
/// The function drops a partition completely and sets its records to `None`.
/// It never truncates in the middle of a batch, because Kafka clients expect
/// complete record batches.
fn truncate_throttled_responses(
    responses: &mut [FetchableTopicResponse],
    throttled_idxs: &[(usize, usize)],
    budget: u64,
) {
    let mut remaining = budget;
    for &(ti, pi) in throttled_idxs {
        let part = &mut responses[ti].partitions[pi];
        let chunk_size = part.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
        } else {
            // Budget exhausted — drop this chunk and all subsequent throttled ones.
            part.records = None;
            remaining = 0;
        }
    }
}

/// Sum the encoded byte sizes of all record batches across all topic
/// partitions in the assembled Fetch response.
///
/// The KIP-13 `consumer_byte_rate` hook uses this sum.
fn sum_response_bytes(responses: &[FetchableTopicResponse]) -> u64 {
    responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| p.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64)
        .sum()
}

/// KIP-13 `consumer_byte_rate` enforcement.
///
/// The function looks up the matching quota for `(principal, client_id)`,
/// takes `bytes` from the bucket, and returns the throttle delay capped at 1
/// second. It returns `Duration::ZERO` when the config sets no quota, or when
/// the bucket has enough capacity.
fn consume_consumer_quota(
    image: &krabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
    maximum: Time,
) -> Time {
    let Some((entity_key, rate)) =
        crate::quota::lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return <Time as TimeExt>::ZERO;
    };
    if rate <= 0.0 {
        return <Time as TimeExt>::ZERO;
    }
    let bucket = buckets.get_or_create(
        "consumer_byte_rate",
        &entity_key,
        rate.to_u64().unwrap_or(u64::MAX),
    );
    let granted = bucket.try_consume(bytes);
    if granted >= bytes {
        return <Time as TimeExt>::ZERO;
    }
    let overage = bytes - granted;
    let delay_secs = overage.to_f64().unwrap_or(f64::MAX) / rate;
    Time::from_secs_f64(delay_secs).min(maximum)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt, millis};

    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "consumer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        let delay_match =
            super::consume_consumer_quota(&img, &buckets, "alice", "app-x", 4096, millis(25));
        assert!(
            delay_match == millis(25),
            "tuple quota match should honor the configured cap; got {delay_match:?}"
        );
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other =
            super::consume_consumer_quota(&img, &buckets2, "alice", "other", 4096, millis(25));
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }
}
