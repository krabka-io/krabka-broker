//! KIP-13 `producer_byte_rate` enforcement.

use {
    super::{QuotaConsumption, QuotaType, buckets::QuotaBuckets, record_client_quota},
    krabka_metadata::MetadataImage,
};

/// Records `bytes` on the `producer_byte_rate` quota of the quota entity
/// that `(principal, client_id)` resolves to, and returns its throttle.
///
/// Kafka keeps one bandwidth sensor per quota entity
/// (`ClientQuotaManager.getOrCreateQuotaSensors`), so every topic a producer
/// writes draws on the same quota. The sensor's quota is a double, so the
/// bound is the configured rate, fractional part included. Kafka's
/// `ClientQuotaManager.throttleTime` does not bound a byte-rate throttle.
#[must_use]
pub fn consume_producer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    bytes: u64,
) -> super::QuotaDelay {
    record_client_quota(QuotaConsumption {
        image,
        buckets,
        principal,
        client_id,
        quota_type: QuotaType::Produce,
        amount: bytes,
    })
    .delay
}

#[cfg(test)]
mod tests {
    use {
        super::consume_producer_quota,
        crate::quota::{
            QuotaBuckets,
            test_support::{
                QuotaEntityInput, check_fractional_bandwidth, image_with_rate, manual_buckets,
            },
        },
        assert2::{assert, check},
        krabka_units::{Time, convert::TimeExt, millis, secs},
    };

    fn img_with_quota(
        entity: Vec<QuotaEntityInput<'_>>,
        rate: f64,
    ) -> krabka_metadata::MetadataImage {
        image_with_rate(entity, "producer_byte_rate", rate)
    }

    fn user_and_client_quota(rate: f64) -> krabka_metadata::MetadataImage {
        img_with_quota(
            vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(Some("app")),
            ],
            rate,
        )
    }

    /// One quota per quota entity (#748): two charges share it, so the
    /// second one, which fits the rate on its own, is throttled by the first.
    /// At 100 B/s over Kafka's padded 10 s window, 1500 bytes are 150 B/s, a
    /// `(150 - 100) / 100 * 10 s` = 5 s throttle, and 100 more are 160 B/s,
    /// 6 s.
    #[test]
    fn producer_quota_keeps_one_quota_per_entity() {
        let img = user_and_client_quota(100.0);
        let (_, buckets) = manual_buckets();

        let first = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1_500);
        let second = consume_producer_quota(&img, &buckets, "alice", Some("app"), 100);

        check!((first.delay, second.delay) == (secs(5), secs(6)));
        check!(buckets.len() == 1);
    }

    /// Kafka records the whole request on the entity's `Rate` before it checks
    /// the quota (`ClientQuotaManager.recordAndGetThrottleTimeMs`), so the
    /// overage of a throttled request stays charged: the next request on the
    /// entity, from any connection, is throttled for the debt of both (#1212).
    /// Before, the bucket kept only what it held, the refill of the mute paid
    /// the overage back, and a producer sent about twice its quota.
    #[test]
    fn the_overage_of_a_throttled_request_stays_charged() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 1_000.0);
        let (_, buckets) = manual_buckets();

        // 15 000 bytes over the padded 10 s window are 1500 B/s, a 5 s
        // throttle at 1000 B/s. 1000 more on the same entity from another
        // client are 1600 B/s: `(1600 - 1000) / 1000 * 10 s` = 6 s.
        let first = consume_producer_quota(&img, &buckets, "alice", Some("app"), 15_000);
        let second = consume_producer_quota(&img, &buckets, "alice", Some("other-app"), 1_000);

        check!((first.delay, second.delay) == (secs(5), secs(6)));
    }

    #[test]
    fn producer_quota_uses_client_id_entity_precedence() {
        let img = user_and_client_quota(128.0);
        let buckets = QuotaBuckets::new();

        let matching = consume_producer_quota(&img, &buckets, "alice", Some("app"), 4096);
        let other_client = consume_producer_quota(&img, &buckets, "alice", Some("other"), 4096);

        assert!(matching > <Time as TimeExt>::ZERO);
        assert!(other_client == <Time as TimeExt>::ZERO);
    }

    /// Kafka's `QuotaUtils.throttleTime`: 12 500 bytes over the padded 10 s
    /// window are 1250 B/s, `(1250 - 1000) / 1000 * 10 s` = 2.5 s.
    #[test]
    fn producer_quota_delay_is_kafkas_throttle_time() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 1_000.0);
        let (_, buckets) = manual_buckets();

        let delay = consume_producer_quota(&img, &buckets, "alice", Some("app"), 12_500);

        assert!(delay == millis(2_500));
    }

    /// Kafka leaves a byte-rate throttle unbounded (#709): a producer far
    /// over its rate is told to back off for the whole of it, past the ten
    /// seconds the broker once capped it at. 31 KiB at 1 KiB/s over the
    /// padded 10 s window are 3.1 KiB/s, `(3.1 - 1) / 1 * 10 s` = 21 s.
    #[test]
    fn producer_quota_delay_is_not_capped() {
        let img = img_with_quota(vec![QuotaEntityInput::user(Some("alice"))], 1024.0);
        let (_, buckets) = manual_buckets();

        let delay = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1024 * 31);

        assert!(delay == secs(21));
    }

    /// Kafka enforces `producer_byte_rate` as a double, so a fractional rate
    /// throttles at that rate: neither unbounded nor rounded to a whole byte
    /// per second.
    #[test]
    fn a_fractional_producer_byte_rate_throttles() {
        // `(producer_byte_rate, request bytes, expected throttle)`, measured
        // over Kafka's padded window.
        check_fractional_bandwidth("producer_byte_rate", |image, buckets, bytes| {
            consume_producer_quota(image, buckets, "alice", Some("app"), bytes).delay
        });
    }
}
