//! KIP-13 `producer_byte_rate` enforcement.

use krabka_metadata::MetadataImage;
use krabka_units::{Time, convert::TimeExt as _};
use num_traits::cast::ToPrimitive as _;

use super::{QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota, u64_to_f64};

/// Charges `bytes` to the `producer_byte_rate` bucket of the quota entity
/// that `(principal, client_id)` resolves to.
///
/// Kafka keeps one bandwidth sensor per quota entity
/// (`ClientQuotaManager.getOrCreateQuotaSensors`), so every topic a producer
/// writes draws on the same bucket.
#[must_use]
pub fn consume_producer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
) -> super::QuotaDelay {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "producer_byte_rate",
            amount: bytes,
        },
        quota_rate_to_bucket_rate,
        |overage, rate, _| {
            let overage = u64_to_f64(overage);
            // Kafka's `ClientQuotaManager.throttleTime` does not bound a
            // byte-rate throttle.
            Time::from_secs_f64(overage / rate)
        },
    )
}

/// The token-bucket rate of a `producer_byte_rate`.
///
/// Kafka enforces the configured rate as a double, so a positive rate under
/// one byte per second still throttles. The bucket counts whole bytes, so such
/// a rate gets a bucket rate of 1; the delay is still the overage divided by
/// the real rate.
fn quota_rate_to_bucket_rate(rate: f64) -> Option<u64> {
    if !rate.is_finite() || rate <= 0.0 {
        return None;
    }

    rate.floor().to_u64().map(|whole| whole.max(1))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::MetadataImage;
    use krabka_units::{Time, convert::TimeExt, millis, secs};

    use super::consume_producer_quota;
    use crate::quota::{QuotaBuckets, test_support::image_with_quota as quota_image};

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "producer_byte_rate", rate)
    }

    /// One bucket per quota entity (#748): two charges share it, so the
    /// second one, which fits the rate on its own, is throttled by the first.
    #[test]
    fn producer_quota_keeps_one_bucket_per_entity() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        // A one-second window keeps the burst at the configured rate, so
        // these amounts are about the overage and not about the window.
        let buckets = QuotaBuckets::with_window(secs(1));

        let first = consume_producer_quota(&img, &buckets, "alice", "app", 1024);
        let second = consume_producer_quota(&img, &buckets, "alice", "app", 64);

        check!(first > <Time as TimeExt>::ZERO);
        check!(second > <Time as TimeExt>::ZERO);
        check!(buckets.len() == 1);
    }

    #[test]
    fn producer_quota_uses_client_id_entity_precedence() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        let buckets = QuotaBuckets::new();

        let matching = consume_producer_quota(&img, &buckets, "alice", "app", 4096);
        let other_client = consume_producer_quota(&img, &buckets, "alice", "other", 4096);

        assert!(matching > <Time as TimeExt>::ZERO);
        assert!(other_client == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn producer_quota_delay_reflects_exact_overage_at_configured_rate() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1_000.0);
        let buckets = QuotaBuckets::with_window(secs(1));

        let delay = consume_producer_quota(&img, &buckets, "alice", "app", 1_250);

        assert!(delay == millis(250));
    }

    /// Kafka leaves a byte-rate throttle unbounded (#709): a producer far
    /// over its rate is told to back off for the whole debt, past the ten
    /// seconds the broker once capped it at.
    #[test]
    fn producer_quota_delay_is_not_capped() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1024.0);
        // The default 11-second window gives an 11 KiB burst; 20 KiB past it
        // is 20 seconds of debt at 1 KiB/s.
        let buckets = QuotaBuckets::new();

        let delay = consume_producer_quota(&img, &buckets, "alice", "app", 1024 * (11 + 20));

        assert!(delay > secs(19) && delay <= secs(20), "{delay:?}");
    }

    /// Kafka enforces `producer_byte_rate` as a double, so a rate under one
    /// byte per second throttles instead of leaving the producer unbounded.
    #[test]
    fn a_fractional_producer_byte_rate_throttles() {
        // `(producer_byte_rate, request bytes, expected throttle)`. The
        // one-second window gives the bucket a burst of its rate, rounded up
        // to one whole byte.
        let cases = [
            (1024.0, 1024, <Time as TimeExt>::ZERO),
            (1024.0, 2048, secs(1)),
            (0.5, 1, <Time as TimeExt>::ZERO),
            (0.5, 100, secs(198)),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (rate, bytes, delay) in cases {
            let img = img_with_quota(vec![("user", Some("alice"))], rate);
            let buckets = QuotaBuckets::with_window(secs(1));
            let throttle = consume_producer_quota(&img, &buckets, "alice", "app", bytes);
            actual.push((rate.to_string(), bytes, throttle.delay));
            expected.push((rate.to_string(), bytes, delay));
        }
        assert!(actual == expected);
    }
}
