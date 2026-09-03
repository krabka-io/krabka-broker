//! The KIP-13 producer byte-rate and KIP-124 request-percentage accounting,
//! and the throttled response encode that closes a `Produce` request.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::produce_response::{ProduceResponse, TopicProduceResponse},
};
use super::framing::FramedTopic;
use crate::{broker::Broker, error::BrokerError};

pub(super) fn produce_bytes_by_qos_tier(
    image: &krabka_metadata::MetadataImage,
    topics: &[FramedTopic],
) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    for topic in topics {
        let topic_name = match crate::topic_resolve::resolve(image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.as_str(),
            Err(_) => topic.name.as_str(),
        };
        let qos_tier = crate::config_keys::resolve_qos_tier(image, topic_name).to_string();
        let topic_bytes: u64 = topic
            .partition_data
            .iter()
            .map(|p| p.payload.payload_len() as u64)
            .sum();
        *out.entry(qos_tier).or_default() += topic_bytes;
    }
    out
}

pub(super) fn finish_produce_response(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    handler_start: std::time::Instant,
    bytes_by_qos: &std::collections::BTreeMap<String, u64>,
    topic_results: Vec<TopicProduceResponse>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    let data_delay = bytes_by_qos
        .iter()
        .map(|(tier, bytes)| {
            crate::quota::consume_producer_quota(
                image,
                &broker.quota_buckets,
                &context.principal.name,
                context.client_id,
                tier,
                *bytes,
                broker.config.quota_throttle_max,
            )
        })
        .fold(crate::quota::QuotaDelay::zero(), |acc, qd| {
            if qd.delay > acc.delay { qd } else { acc }
        });
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
    // that caused it, and hands back the delay the response reports and the
    // mute below honors.
    let delay = broker.metrics.record_applied_throttle(
        super::PRODUCE_API_KEY,
        &[
            (crate::metrics::QuotaType::Produce, data_delay).into(),
            (crate::metrics::QuotaType::Request, request_delay).into(),
        ],
    );
    let response = ProduceResponse {
        responses: topic_results,
        throttle_time_ms: crate::quota::throttle_time_ms(delay),
        ..Default::default()
    };
    // KIP-219: report the window in the response and hand it to the connection
    // loop, which mutes the connection once these bytes are written. Sleeping
    // here would hold the response back past the client's request timeout.
    context.record_throttle(delay);
    let mut encoded = BytesMut::new();
    if (0..3).contains(&version) {
        let legacy: krabka_protocol::kafka_3_6_2::owned::produce_response::ProduceResponse =
            response.into();
        encoded.reserve(legacy.encoded_len(version));
        legacy.encode(&mut encoded, version)?;
    } else {
        encoded.reserve(response.encoded_len(version));
        response.encode(&mut encoded, version)?;
    }
    Ok(encoded.freeze())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_units::secs;
    use uuid::Uuid;

    use super::*;
    use crate::handlers::produce::test_support::{framed_topic, image_with_topic, set_qos_tier};

    #[test]
    fn produce_bytes_by_qos_tier_groups_topic_payload_bytes() {
        let mut img = image_with_topic("gold-topic", &[1]);
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "default-topic".into(),
            topic_id: Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        set_qos_tier(&mut img, "gold-topic", "gold");

        let topics = vec![
            framed_topic("gold-topic", &[10, 15]),
            framed_topic("default-topic", &[7]),
            framed_topic("gold-topic", &[5]),
        ];

        let grouped = produce_bytes_by_qos_tier(&img, &topics);

        let expected: BTreeMap<String, u64> = maplit::btreemap! {
        "gold".to_string() => 30,
        crate::config_keys::DEFAULT_QOS_TIER.to_string() => 7};
        assert!(grouped == expected);
    }

    #[test]
    fn consume_producer_quota_tuple_match_overage_throttles() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        use krabka_units::{Time, convert::TimeExt};

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
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        // Tuple match → 4096 bytes overage at 1024 B/s → throttle > 0.
        let delay_match = crate::quota::consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app-x",
            "default",
            4096,
            secs(1),
        );
        assert!(
            delay_match.delay > <Time as TimeExt>::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other = crate::quota::consume_producer_quota(
            &img,
            &buckets2,
            "alice",
            "other",
            "default",
            4096,
            secs(1),
        );
        assert!(
            delay_other.delay == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }
}

