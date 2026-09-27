//! KIP-13 + KIP-124 + KIP-257 client quotas.

use krabka_metadata::MetadataImage;
use krabka_units::{
    ByteRate, Time,
    convert::{ByteRateExt as _, TimeExt},
};
use num_traits::cast::{NumCast, ToPrimitive as _};

mod buckets;
mod controller_mutation;
mod expiry;
mod lookup;
mod producer;
mod request;
mod throttle_slot;

pub use buckets::QuotaBuckets;
pub use controller_mutation::consume_controller_mutation_quota;
pub(crate) use controller_mutation::{ControllerMutationQuota, QuotaRequest};
pub use lookup::{lookup_ip_quota, lookup_ip_quota_with_key, lookup_quota, lookup_quota_with_key};
pub use producer::consume_producer_quota;
pub use request::consume_request_quota;
pub(crate) use throttle_slot::ThrottleSlot;

mod refresh;
pub(crate) use expiry::run as run_expiry;
pub use refresh::run;

/// Result of consuming a client quota, carrying the delay and the resolved
/// entity identity (`user` and `client_id`) that the match was charged to (#418).
#[derive(Debug, Clone, PartialEq)]
pub struct QuotaDelay {
    pub delay: Time,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

impl QuotaDelay {
    /// No throttle, charged to nobody.
    #[must_use]
    pub fn zero() -> Self {
        Self {
            delay: <Time as TimeExt>::ZERO,
            user: None,
            client_id: None,
        }
    }

    /// A throttle of `delay`, charged to the principal and client id whose
    /// quota produced it (KIP-599 labels the applied throttle by entity).
    #[must_use]
    pub fn new(delay: Time, user: Option<String>, client_id: Option<String>) -> Self {
        Self {
            delay,
            user,
            client_id,
        }
    }
}

impl std::ops::Deref for QuotaDelay {
    type Target = Time;
    fn deref(&self) -> &Self::Target {
        &self.delay
    }
}

impl PartialEq<Time> for QuotaDelay {
    fn eq(&self, other: &Time) -> bool {
        self.delay == *other
    }
}

impl PartialEq<QuotaDelay> for Time {
    fn eq(&self, other: &QuotaDelay) -> bool {
        *self == other.delay
    }
}

impl PartialOrd<Time> for QuotaDelay {
    fn partial_cmp(&self, other: &Time) -> Option<std::cmp::Ordering> {
        self.delay.partial_cmp(other)
    }
}

impl PartialOrd<QuotaDelay> for Time {
    fn partial_cmp(&self, other: &QuotaDelay) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.delay)
    }
}

#[derive(Clone, Copy)]
struct QuotaConsumption<'a> {
    image: &'a MetadataImage,
    buckets: &'a QuotaBuckets,
    principal: &'a str,
    client_id: &'a str,
    quota_key: &'a str,
    amount: u64,
}

/// Charges `request.amount` tokens to the bucket of the quota entity the
/// request resolves to, and returns the throttle for the part the bucket could
/// not cover.
///
/// `token_rate` turns the configured rate into the bucket's tokens per second,
/// fractional or not. `delay_for_overage` gets the tokens the bucket could not
/// cover, part token included, the configured rate, and the token rate.
fn consume_configured_quota(
    request: QuotaConsumption<'_>,
    token_rate: impl FnOnce(f64) -> f64,
    delay_for_overage: impl FnOnce(f64, f64, f64) -> Time,
) -> QuotaDelay {
    if request.amount == 0 {
        return QuotaDelay::zero();
    }
    let Some((entity_key, rate)) = lookup::lookup_quota_with_key(
        request.image,
        request.principal,
        request.client_id,
        request.quota_key,
    ) else {
        return QuotaDelay::zero();
    };
    if !rate.is_finite() || rate <= 0.0 {
        return QuotaDelay::zero();
    }
    let token_rate = token_rate(rate);
    let user = entity_key
        .iter()
        .find(|(k, _)| k == "user")
        .and_then(|(_, v)| v.clone());
    let client_id = entity_key
        .iter()
        .find(|(k, _)| k == "client-id")
        .and_then(|(_, v)| v.clone());

    let bucket = request.buckets.get_or_create(
        request.quota_key,
        &entity_key,
        request.principal,
        request.client_id,
        token_rate,
    );
    // Kafka holds the quota as a double, so the bucket grants a part token
    // too and the overage is the exact shortfall under a fractional rate.
    let granted_micros = bucket.try_consume_micros(request.amount);
    let Some(overage) = overage_tokens(request.amount, granted_micros) else {
        return QuotaDelay::zero();
    };
    // Kafka bounds only the request quota's throttle (`ClientRequestQuotaManager`
    // takes `boundedThrottleTime`), so the bound, where there is one, is the
    // caller's.
    let delay = delay_for_overage(overage, rate, token_rate);
    QuotaDelay::new(delay, user, client_id)
}

/// The part of a `requested`-token charge that a grant of `granted_micros`
/// micro-tokens did not cover, in tokens with its fractional part, or `None`
/// when the grant covered it all.
pub(crate) fn overage_tokens(requested: u64, granted_micros: u64) -> Option<f64> {
    let micros_per_token = crate::throttle::MICROS_PER_TOKEN;
    // The bucket's micro-token grant saturates the same way.
    let requested_micros = requested.saturating_mul(micros_per_token);
    let short = requested_micros.checked_sub(granted_micros)?;
    (short > 0).then(|| u64_to_f64(short) / u64_to_f64(micros_per_token))
}

/// A quota delay as Kafka's `throttle_time_ms` wire field.
///
/// The conversion truncates toward zero and does not round to the nearest
/// value. It reports a 1.6 ms delay as `1`. A client reads `throttle_time_ms`
/// back and sleeps on it, so the byte on the wire must not change because the
/// code carries the delay as a [`Time`]. A delay beyond `i32::MAX`
/// milliseconds saturates.
#[must_use]
pub(crate) fn throttle_time_ms(delay: Time) -> i32 {
    i32::try_from(delay.millis_i64_trunc()).unwrap_or(i32::MAX)
}

/// A raw quota rate as the [`TokenBucket`](crate::throttle::TokenBucket)'s
/// [`ByteRate`], fractional part included.
///
/// The bucket is byte-dimensioned, but Kafka drives `request_percentage` and
/// `controller_mutation_rate` through the same token arithmetic, and those are
/// not byte throughputs. Their raw magnitudes therefore cross into the
/// bucket's dimension here, in one place, instead of at each call site.
pub(crate) fn bucket_rate(raw: f64) -> ByteRate {
    ByteRate::from_bytes_per_sec_f64(raw)
}

/// A configured rate as a whole token count, truncated toward zero.
///
/// Negative and non-finite rates are not throughputs, so they collapse to `0`,
/// the bucket's "no limit configured" sentinel. Anything past `u64::MAX`
/// saturates.
pub(crate) fn positive_f64_to_u64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.trunc().to_u64().unwrap_or(u64::MAX)
}

/// A token count widened for the overage-over-rate division.
///
/// The conversion is exact below 2^53, which covers every quota magnitude
/// Kafka can express. `NumCast` never fails for `u64` into `f64`. The fallback
/// keeps the quota path total instead of panicking on a value that cannot
/// occur.
pub(crate) fn u64_to_f64(value: u64) -> f64 {
    NumCast::from(value).unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod test_support {
    use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};

    pub(super) fn image_with_quota(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> MetadataImage {
        image_with_quotas(vec![quota_record(entity, key, value)])
    }

    pub(super) fn image_with_quotas(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for record in records {
            image.apply(&MetadataRecord::V1ClientQuota(record));
        }
        image
    }

    pub(super) fn quota_record(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(entity_type, entity_name)| QuotaEntity {
                    entity_type: entity_type.into(),
                    entity_name: entity_name.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use assert2::{assert, check};
    use krabka_units::secs;

    use super::{test_support::image_with_quota, *};

    /// `throttle_time_ms` truncates: a sub-millisecond delay reports `0`, and
    /// a 1.6 ms delay reports `1`, not `2`.
    #[test]
    fn throttle_time_ms_truncates_toward_zero() {
        let cases = [
            (krabka_units::micros(0), 0),
            (krabka_units::micros(400), 0),
            (krabka_units::micros(999), 0),
            (krabka_units::millis(1), 1),
            (krabka_units::micros(1_600), 1),
            (krabka_units::micros(1_999), 1),
            (secs(1), 1_000),
        ];
        for (delay, want) in cases {
            check!(
                throttle_time_ms(delay) == want,
                "{delay:?} should report {want}ms"
            );
        }
    }

    #[test]
    fn throttle_time_ms_saturates_past_i32_milliseconds() {
        assert!(throttle_time_ms(krabka_units::days(36_500)) == i32::MAX);
    }

    #[test]
    fn quota_rate_conversion_is_checked_and_saturating() {
        assert!(positive_f64_to_u64(-1.0) == 0);
        assert!(positive_f64_to_u64(f64::NAN) == 0);
        assert!(positive_f64_to_u64(10.9) == 10);
        assert!(positive_f64_to_u64(f64::MAX) == u64::MAX);
        assert!(u64_to_f64(u64::MAX).is_finite());
    }

    /// The producer path once carried its own copy of this widening. The two
    /// agreed on every input, and this test pins that they still would. It
    /// compares bit patterns instead of using `==`, so the comparison is
    /// exact.
    #[test]
    fn widening_agrees_with_the_former_producer_copy() {
        for value in [0_u64, 1, 1024, 1 << 52, (1_u64 << 53) - 1, u64::MAX] {
            let former: f64 = value.to_string().parse().unwrap_or(f64::INFINITY);
            check!(u64_to_f64(value).to_bits() == former.to_bits());
        }
    }

    /// The overage keeps the part token a micro-token grant leaves short, and
    /// a grant that covers the request leaves none.
    #[test]
    fn overage_tokens_is_the_exact_shortfall() {
        let m = crate::throttle::MICROS_PER_TOKEN;
        let cases = [
            (0, 0, None),
            (1, m, None),
            (1, 2 * m, None),
            (1, m / 2, Some(0.5)),
            (1, 0, Some(1.0)),
            (100, m / 2, Some(99.5)),
            (3, 2 * m + m / 4, Some(0.75)),
        ];
        for (requested, granted_micros, want) in cases {
            check!(
                overage_tokens(requested, granted_micros) == want,
                "{requested} tokens, {granted_micros} micro-tokens granted"
            );
        }
    }

    #[test]
    fn consume_configured_quota_returns_zero_without_mutating_bucket_for_zero_amount() {
        let image = image_with_quota(vec![("user", Some("alice"))], "request_percentage", 100.0);
        let buckets = QuotaBuckets::new();
        let initial_rate_called = Arc::new(AtomicBool::new(false));
        let delay_for_overage_called = Arc::new(AtomicBool::new(false));

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: "",
                quota_key: "request_percentage",
                amount: 0,
            },
            {
                let called = Arc::clone(&initial_rate_called);
                move |_| {
                    called.store(true, Ordering::Relaxed);
                    100.0
                }
            },
            {
                let called = Arc::clone(&delay_for_overage_called);
                move |_, _, _| {
                    called.store(true, Ordering::Relaxed);
                    secs(1)
                }
            },
        );

        check!(delay == <Time as TimeExt>::ZERO);
        check!(buckets.is_empty());
        check!(!initial_rate_called.load(Ordering::Relaxed));
        assert!(!delay_for_overage_called.load(Ordering::Relaxed));
    }

    #[test]
    fn consume_configured_quota_ignores_non_positive_rates() {
        for rate in [-1.0, 0.0] {
            let image = image_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", rate);
            let buckets = QuotaBuckets::new();
            let initial_rate_called = Arc::new(AtomicBool::new(false));

            let delay = consume_configured_quota(
                QuotaConsumption {
                    image: &image,
                    buckets: &buckets,
                    principal: "alice",
                    client_id: "",
                    quota_key: "producer_byte_rate",
                    amount: 1,
                },
                {
                    let called = Arc::clone(&initial_rate_called);
                    move |_| {
                        called.store(true, Ordering::Relaxed);
                        1.0
                    }
                },
                |_, _, _| secs(1),
            );

            check!(delay == <Time as TimeExt>::ZERO);
            check!(buckets.is_empty());
            assert!(!initial_rate_called.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn consume_configured_quota_leaves_the_overage_delay_uncapped() {
        let image = image_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 1.0);
        // A one-second window: at 1 B/s the burst is one byte, so 10 bytes
        // leaves the 9-byte overage the closure below checks.
        let buckets = QuotaBuckets::with_window(secs(1));

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: "",
                quota_key: "producer_byte_rate",
                amount: 10,
            },
            |rate| rate,
            |overage, rate, token_rate| {
                check!((overage, rate, token_rate) == (9.0, 1.0, 1.0));
                secs(10)
            },
        );

        check!(delay == secs(10));
        assert!(buckets.len() == 1);
    }
}
