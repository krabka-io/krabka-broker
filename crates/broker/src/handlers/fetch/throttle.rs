//! The two throttles a finished fetch passes: the KIP-73 leader-side
//! replication throttle on a follower fetch, and the KIP-13 consumer byte
//! rate together with the KIP-124 request quota on a client fetch.

use krabka_protocol::{
    Encode as _,
    owned::fetch_response::{FetchResponse, FetchableTopicResponse},
    records::RecordsPayload,
};
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
        // KIP-73: the measured leader-side throttled-replication rate, which
        // Kafka publishes as
        // `kafka.server:type=LeaderReplication,name=byte-rate`. It is what
        // tells an operator whose reassignment is not moving whether the
        // throttle is biting or something else is wrong.
        broker.metrics.record_replication_throttled_out(granted);
        if granted < byte_count {
            truncate_throttled_responses(responses, &indexes, granted);
            // The bucket had less than the round asked for, so some of what
            // this follower was owed is held back. Kafka delays the fetch;
            // krabka truncates it and the follower re-asks next round.
            broker.metrics.record_replication_throttle_sleep();
        }
    }
}

/// The bytes a consumer may read in one fetch without a throttle: Kafka's
/// `ClientQuotaManager.maxValueInQuotaWindow`, which is
/// `consumer_byte_rate * (quota.window.num - 1) * quota.window.size.seconds`.
/// `quota_window` is the whole `quota.window.num * quota.window.size.seconds`
/// and `quota_throttle_max` is one `quota.window.size.seconds`. Without a
/// `consumer_byte_rate` the read is not capped.
pub(super) fn consumer_quota_window_bytes(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
) -> usize {
    let Some((_, rate)) = crate::quota::lookup_quota_with_key(
        image,
        &context.principal.name,
        context.client_id,
        "consumer_byte_rate",
    ) else {
        return usize::MAX;
    };
    if !rate.is_finite() || rate <= 0.0 {
        return usize::MAX;
    }
    let window_secs = (broker.config.quota_window - broker.config.quota_throttle_max)
        .secs_f64()
        .max(0.0);
    (rate * window_secs).to_usize().unwrap_or(usize::MAX)
}

/// The byte-rate charge a consumer fetch made, in bytes, which a throttled
/// fetch gives back.
pub(super) struct ConsumerCharge(Option<(std::sync::Arc<crate::throttle::TokenBucket>, u64)>);

impl ConsumerCharge {
    /// Kafka's `quotas.fetch.unrecordQuotaSensor`: a throttled fetch sends
    /// no records, so the bytes it was charged come off the quota. The whole
    /// charge comes back, the debt it left included.
    pub(super) fn refund(self) {
        if let Some((bucket, bytes)) = self.0 {
            bucket.refund(bytes);
        }
    }
}

/// Charge a consumer fetch whose whole response is `response_bytes` long to
/// its `consumer_byte_rate` and `request_percentage` quotas, as Kafka's
/// `KafkaApis.handleFetchRequest` records both with
/// `fetchContext.getResponseSize`. Returns the throttle the response reports,
/// and the byte-rate charge to give back when that throttle is above zero.
pub(super) fn apply_consumer_fetch_quota(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    handler_start: std::time::Instant,
    response_bytes: u64,
) -> (i32, ConsumerCharge) {
    let (data_delay, charge) = consume_consumer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        response_bytes,
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
            (crate::metrics::QuotaType::Fetch, data_delay).into(),
            (crate::metrics::QuotaType::Request, request_delay).into(),
        ],
    );
    if delay <= <Time as TimeExt>::ZERO {
        return (0, charge);
    }
    // KIP-219: the window goes back to the connection loop, which mutes the
    // connection after the response is written.
    context.record_throttle(delay);
    (crate::quota::throttle_time_ms(delay), charge)
}

/// The encoded size of a whole Fetch response at `version`, which is what
/// Kafka's `FetchContext.getResponseSize` charges to the consumer byte rate.
pub(super) fn fetch_response_size(response: &FetchResponse, version: i16) -> u64 {
    let size = if version < 4 {
        let legacy: krabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            response.clone().into();
        legacy.encoded_len(version)
    } else {
        response.encoded_len(version)
    };
    u64::try_from(size).unwrap_or(u64::MAX)
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

/// KIP-13 `consumer_byte_rate` enforcement.
///
/// The function looks up the matching quota for `(principal, client_id)`,
/// takes `bytes` from the bucket, and returns the throttle delay with the
/// charge it made. The delay is zero when the config sets no quota, or when
/// the bucket has enough capacity.
fn consume_consumer_quota(
    image: &krabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
) -> (crate::quota::QuotaDelay, ConsumerCharge) {
    let Some((entity_key, rate)) =
        crate::quota::lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return (crate::quota::QuotaDelay::zero(), ConsumerCharge(None));
    };
    if !rate.is_finite() || rate <= 0.0 {
        return (crate::quota::QuotaDelay::zero(), ConsumerCharge(None));
    }
    let user = entity_key
        .iter()
        .find(|(k, _)| k == "user")
        .and_then(|(_, v)| v.clone());
    let client_id_opt = entity_key
        .iter()
        .find(|(k, _)| k == "client-id")
        .and_then(|(_, v)| v.clone());
    // Kafka holds the quota as a double (`ClientQuotaManager`), so the bucket
    // runs at the configured rate, fractional part included. Like the
    // producer path it records the whole response and turns the debt into the
    // throttle (#1212); a throttled fetch then gives the whole charge back.
    let bucket = buckets.get_or_create("consumer_byte_rate", &entity_key, rate);
    let debt = bucket.record(bytes);
    let charge = ConsumerCharge(Some((std::sync::Arc::clone(&bucket), bytes)));
    let Some(overage) = crate::quota::debt_tokens(debt) else {
        return (crate::quota::QuotaDelay::zero(), charge);
    };
    let delay_secs = overage / rate;
    // Kafka's `ClientQuotaManager.throttleTime` does not bound a byte-rate
    // throttle.
    let delay = Time::from_secs_f64(delay_secs);
    (
        crate::quota::QuotaDelay::new(delay, user, client_id_opt),
        charge,
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt, millis, secs};

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
        // A one-second window, so 4096 bytes at 1024 B/s is over the burst
        // rather than inside the default 11-second one.
        let buckets = crate::quota::QuotaBuckets::with_window(secs(1));
        // 3072 bytes over at 1024 B/s is three seconds, reported whole: Kafka
        // does not bound a byte-rate throttle (#709).
        let (delay_match, _) =
            super::consume_consumer_quota(&img, &buckets, "alice", "app-x", 4096);
        assert!(
            delay_match > millis(2_900) && delay_match <= secs(3),
            "tuple quota match should throttle for the whole overage; got {delay_match:?}"
        );
        let buckets2 = crate::quota::QuotaBuckets::with_window(secs(1));
        let (delay_other, _) =
            super::consume_consumer_quota(&img, &buckets2, "alice", "other", 4096);
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    /// A fetch is charged in full and, when that throttles it, the whole
    /// charge comes back (`ClientQuotaManager.unrecordQuotaSensor`), the debt
    /// it left included: the fetch that follows the throttle finds the bucket
    /// as it was. A throttled fetch that is not refunded stays charged.
    #[test]
    fn a_throttled_fetch_gives_the_whole_charge_back() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
            }],
            config_key: "consumer_byte_rate".into(),
            config_value: Some(1_000.0),
        }));
        let consume = |buckets: &crate::quota::QuotaBuckets, bytes| {
            super::consume_consumer_quota(&img, buckets, "alice", "app", bytes)
        };

        let refunded = crate::quota::QuotaBuckets::with_window(secs(1));
        let (throttle, charge) = consume(&refunded, 1_500);
        charge.refund();
        let (after_refund, _) = consume(&refunded, 1_000);

        let kept = crate::quota::QuotaBuckets::with_window(secs(1));
        let _ = consume(&kept, 1_500);
        let (after_kept, _) = consume(&kept, 1_000);

        assert!(throttle > <Time as TimeExt>::ZERO, "{throttle:?}");
        assert!(after_refund == <Time as TimeExt>::ZERO, "{after_refund:?}");
        assert!(after_kept > millis(1_400), "{after_kept:?}");
    }

    /// Kafka's `ClientQuotaManager` holds `consumer_byte_rate` as a double,
    /// so a fractional rate throttles a fetch at that rate: the shortfall
    /// over the rate, neither unbounded nor rounded to a whole byte per
    /// second.
    #[test]
    fn a_fractional_consumer_byte_rate_throttles() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        // `(consumer_byte_rate, response bytes, expected throttle)`. The
        // one-second window gives the bucket a burst of exactly its rate.
        let cases = [
            (1024.0, 1024, <Time as TimeExt>::ZERO),
            (1024.0, 2048, secs(1)),
            (0.5, 1, secs(1)),
            (0.5, 100, secs(199)),
            (0.25, 1, secs(3)),
            (1.5, 1, <Time as TimeExt>::ZERO),
            (1.5, 3, secs(1)),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (rate, bytes, delay) in cases {
            let mut img = MetadataImage::new(uuid::Uuid::nil());
            img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: vec![QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                }],
                config_key: "consumer_byte_rate".into(),
                config_value: Some(rate),
            }));
            let buckets = crate::quota::QuotaBuckets::with_window(secs(1));
            let (throttle, _) =
                super::consume_consumer_quota(&img, &buckets, "alice", "app", bytes);
            actual.push((rate.to_string(), bytes, throttle.delay));
            expected.push((rate.to_string(), bytes, delay));
        }
        assert!(actual == expected);
    }
}
