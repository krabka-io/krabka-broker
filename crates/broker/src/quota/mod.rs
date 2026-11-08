//! KIP-13 + KIP-124 + KIP-257 client quotas.

use {
    crate::throttle::ClientQuota,
    krabka_metadata::MetadataImage,
    krabka_units::{
        ByteRate, Time,
        convert::{ByteRateExt as _, TimeExt},
    },
    num_traits::cast::{NumCast, ToPrimitive as _},
    std::{cmp::Ordering, sync::Arc},
};

mod buckets;
mod controller_mutation;
mod expiry;
mod ip_names;
mod lookup;
mod producer;
mod quota_type;
mod request;
mod throttle_slot;

pub use {
    buckets::QuotaBuckets,
    controller_mutation::consume_controller_mutation_quota,
    ip_names::{IpNames, parse_ip_literal},
    lookup::{lookup_ip_quota, lookup_ip_quota_with_key, lookup_quota, lookup_quota_with_key},
    producer::consume_producer_quota,
    quota_type::QuotaType,
    request::consume_request_quota,
};
pub(crate) use {
    controller_mutation::{ControllerMutationQuota, QuotaRequest},
    lookup::entity_field,
    request::{ActiveClock, Timed, with_active_time},
    throttle_slot::ThrottleSlot,
};

mod refresh;
pub(crate) use expiry::run as run_expiry;
pub use refresh::run;

/// Result of consuming a client quota, carrying the delay and the resolved
/// entity identity (`user` and `client_id`) that the match was charged to (#418).
#[derive(Debug, Clone, PartialEq, derive_more::Deref)]
pub struct QuotaDelay {
    #[deref]
    pub delay: Time,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

impl QuotaDelay {
    /// No throttle, charged to nobody.
    #[must_use]
    pub fn zero() -> Self { Self::new(<Time as TimeExt>::ZERO, None, None) }

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

impl PartialEq<Time> for QuotaDelay {
    fn eq(&self, other: &Time) -> bool { self.delay == *other }
}

impl PartialEq<QuotaDelay> for Time {
    fn eq(&self, other: &QuotaDelay) -> bool { *self == other.delay }
}

impl PartialOrd<Time> for QuotaDelay {
    fn partial_cmp(&self, other: &Time) -> Option<Ordering> { self.delay.partial_cmp(other) }
}

impl PartialOrd<QuotaDelay> for Time {
    fn partial_cmp(&self, other: &QuotaDelay) -> Option<Ordering> { self.partial_cmp(&other.delay) }
}

#[derive(Clone, Copy)]
struct QuotaConsumption<'a> {
    image: &'a MetadataImage,
    buckets: &'a QuotaBuckets,
    principal: &'a str,
    client_id: Option<&'a str>,
    quota_type: QuotaType,
    amount: u64,
}

/// What recording a request on its client quota earned: the throttle, charged
/// to the entity the request resolved to, and the quota it was recorded on,
/// `None` when no quota applies.
pub(crate) struct QuotaRecorded {
    pub(crate) delay: QuotaDelay,
    pub(crate) quota: Option<Arc<ClientQuota>>,
}

/// Records `request.amount` on the client quota of the entity the request
/// resolves to, and returns the throttle Kafka's `QuotaUtils.throttleTime`
/// gives for the rate it leaves.
///
/// Kafka records the whole value on the entity's `Rate` sensor and checks the
/// quota after (`ClientQuotaManager.recordAndGetThrottleTimeMs` calls
/// `quotaSensor().record(value, timeMs, true)`), so a request over the quota
/// is charged in full and the next request on the entity, on any connection,
/// still pays for it until its sample leaves the window (#1212). A zero value
/// is recorded too, as Kafka records it, so a request that adds nothing is
/// still throttled while the entity is over its quota.
///
/// The bound is the configured rate in the unit the quota records
/// ([`QuotaType::metered_rate`]). Kafka bounds no byte-rate throttle, so the
/// caller applies the request quota's bound.
fn record_client_quota(request: QuotaConsumption<'_>) -> QuotaRecorded {
    let unmetered = || QuotaRecorded {
        delay: QuotaDelay::zero(),
        quota: None,
    };
    let Some((entity_key, rate)) = lookup::lookup_quota_with_key(
        request.image,
        request.principal,
        request.client_id,
        request.quota_type,
    ) else {
        return unmetered();
    };
    if !rate.is_finite() || rate <= 0.0 {
        return unmetered();
    }
    let quota = request.buckets.client_quota(
        request.quota_type,
        &entity_key,
        request.quota_type.metered_rate(rate),
    );
    let delay = quota.record(request.amount);
    let delay = if delay > <Time as TimeExt>::ZERO {
        QuotaDelay::new(
            delay,
            entity_field(&entity_key, "user"),
            entity_field(&entity_key, "client-id"),
        )
    } else {
        QuotaDelay::zero()
    };
    QuotaRecorded {
        delay,
        quota: Some(quota),
    }
}

/// KIP-13 `consumer_byte_rate` enforcement: records `bytes`, the size of the
/// whole response, on the quota of the entity `(principal, client_id)`
/// resolves to, and returns the throttle with the quota it was recorded on, so
/// that a throttled fetch can take its bytes back off it
/// ([`ClientQuota::unrecord`]). Kafka does not bound a byte-rate throttle.
pub(crate) fn record_consumer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    bytes: u64,
) -> QuotaRecorded {
    record_client_quota(QuotaConsumption {
        image,
        buckets,
        principal,
        client_id,
        quota_type: QuotaType::Fetch,
        amount: bytes,
    })
}

/// A quota delay as Kafka's `throttle_time_ms` wire field: the delay in
/// milliseconds, rounded to the nearest.
///
/// Kafka's `QuotaUtils.throttleTime` and `ControllerMutationQuotaManager
/// .throttleTimeMs` both return `Math.round(...)` of the millisecond value, so
/// a 1.6 ms delay is reported as `2` and a 0.6 ms one as `1` (#1241). A delay
/// beyond `i32::MAX` milliseconds saturates.
#[must_use]
pub(crate) fn throttle_time_ms(delay: Time) -> i32 {
    i32::try_from(delay.millis_i64()).unwrap_or(i32::MAX)
}

/// A raw quota rate as the [`TokenBucket`](crate::throttle::TokenBucket)'s
/// [`ByteRate`], fractional part included.
///
/// The bucket is byte-dimensioned, but the KIP-612 `connection_creation_rate`
/// it enforces counts connections, not bytes. Its raw magnitude therefore
/// crosses into the bucket's dimension here, in one place, instead of at each
/// call site.
pub(crate) fn bucket_rate(raw: f64) -> ByteRate { ByteRate::from_bytes_per_sec_f64(raw) }

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

/// A count widened for the quota arithmetic, which Kafka does in doubles.
///
/// The conversion is exact below 2^53, which covers every quota magnitude
/// Kafka can express. `NumCast` never fails for `u64` into `f64`. The fallback
/// keeps the quota path total instead of panicking on a value that cannot
/// occur.
pub(crate) fn u64_to_f64(value: u64) -> f64 { NumCast::from(value).unwrap_or(f64::INFINITY) }

#[cfg(test)]
pub(crate) mod test_support {
    use {
        super::QuotaType,
        krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity},
        krabka_units::{Time, convert::TimeExt, secs},
        std::sync::Arc,
    };

    /// One borrowed entity component of a quota fixture, in wire field order.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct QuotaEntityInput<'a> {
        pub(crate) entity_type: &'a str,
        pub(crate) entity_name: Option<&'a str>,
    }

    impl<'a> QuotaEntityInput<'a> {
        pub(crate) const fn user(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "user",
                entity_name: name,
            }
        }

        pub(crate) const fn client_id(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "client-id",
                entity_name: name,
            }
        }

        pub(crate) const fn ip(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "ip",
                entity_name: name,
            }
        }
    }

    /// A named input builder for one quota kind's rate tests.
    pub fn image_with_rate(
        entity: Vec<QuotaEntityInput<'_>>,
        key: &str,
        rate: f64,
    ) -> MetadataImage {
        image_with_quota()
            .entity(entity)
            .key(key)
            .value(rate)
            .call()
    }
    use {crate::throttle::ClientQuota, qubit_clock::ManualMonotonicClock};

    /// Quota meters over Kafka's default window on a manual clock, which a
    /// test advances instead of sleeping.
    pub fn manual_buckets() -> (Arc<ManualMonotonicClock>, super::QuotaBuckets) {
        let clock = ManualMonotonicClock::new_shared();
        let buckets = super::QuotaBuckets::configured()
            .clock(Arc::<ManualMonotonicClock>::clone(&clock))
            .call();
        (clock, buckets)
    }

    /// A quota rate, the bytes charged against it and the throttle that
    /// charge must earn.
    pub struct FractionalBandwidthCase {
        pub(crate) rate: f64,
        pub(crate) bytes: u64,
        pub(crate) delay: Time,
    }

    /// Independent expected throttles for producer and consumer quotas.
    ///
    /// A new client is measured over Kafka's padded window of
    /// `quota.window.num - 1` = 10 samples of 1 s, so `bytes` at once read
    /// `bytes / 10 s`, and Kafka's `QuotaUtils.throttleTime` is
    /// `(bytes / 10 s - rate) / rate * 10 s`, which is `bytes / rate - 10 s`
    /// once that is positive.
    pub fn fractional_bandwidth_cases() -> [FractionalBandwidthCase; 7] {
        let case = |rate, bytes, delay| FractionalBandwidthCase { rate, bytes, delay };
        [
            case(1024.0, 10_240, <Time as TimeExt>::ZERO),
            case(1024.0, 20_480, secs(10)),
            case(0.5, 5, <Time as TimeExt>::ZERO),
            case(0.5, 100, secs(190)),
            case(0.25, 3, secs(2)),
            case(1.5, 15, <Time as TimeExt>::ZERO),
            case(1.5, 18, secs(2)),
        ]
    }

    /// Apply the independent fractional-rate table to one byte-charging API.
    pub fn check_fractional_bandwidth(
        key: &str,
        consume: impl Fn(&MetadataImage, &super::QuotaBuckets, u64) -> Time,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for FractionalBandwidthCase { rate, bytes, delay } in fractional_bandwidth_cases() {
            let image = image_with_quota().key(key).value(rate).call();
            let (_, buckets) = manual_buckets();
            actual.push((rate.to_string(), bytes, consume(&image, &buckets, bytes)));
            expected.push((rate.to_string(), bytes, delay));
        }
        assert2::assert!(actual == expected);
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy)]
    pub struct QuotaValue(pub f64);

    #[derive(krabka_macros::FieldDefaults)]
    pub struct QuotaRecordSetup<'a> {
        #[default(vec![QuotaEntityInput::user(Some("alice"))])]
        pub entity: Vec<QuotaEntityInput<'a>>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(1_000.0))]
        pub value: QuotaValue,
    }

    fn image_from_quota_setup(setup: QuotaRecordSetup<'_>) -> MetadataImage {
        image_with_quotas(vec![
            quota_record()
                .entity(setup.entity)
                .key(setup.key)
                .value(setup.value)
                .call(),
        ])
    }

    pub fn shared_image_from_quota_setup(setup: QuotaRecordSetup<'_>) -> Arc<MetadataImage> {
        Arc::new(image_from_quota_setup(setup))
    }

    /// A quota table and the one client quota that `alice_bucket` created
    /// in it.
    pub struct AliceBucket {
        pub(crate) buckets: Arc<super::QuotaBuckets>,
        pub(crate) quota: Arc<ClientQuota>,
    }

    fn record_from_quota_setup(setup: QuotaRecordSetup<'_>) -> ClientQuotaRecord {
        let QuotaRecordSetup { entity, key, value } = setup;
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|entry| QuotaEntity {
                    entity_type: entry.entity_type.into(),
                    entity_name: entry.entity_name.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value.0),
        }
    }

    krabka_macros::fixture_constructors! { quota: pub(crate) image_with_quota -> MetadataImage = image_from_quota_setup; }
    krabka_macros::fixture_constructors! { quota: pub(crate) quota_record -> ClientQuotaRecord = record_from_quota_setup; }

    pub type QuotaEntitySpec<'a> = Vec<QuotaEntityInput<'a>>;

    #[derive(krabka_macros::FieldDefaults)]
    pub struct UniformQuotaImageSetup<'a> {
        pub entities: Vec<QuotaEntitySpec<'a>>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(64.0))]
        pub value: QuotaValue,
    }

    /// Configure every entity at one probe rate, retaining the input order.
    #[bon::builder]
    pub fn uniform_quota_image(
        #[builder(default)] entities: Vec<QuotaEntitySpec<'_>>,
        #[builder(default = "producer_byte_rate")] key: &'_ str,
        #[builder(into, default = QuotaValue (64.0))] value: QuotaValue,
    ) -> MetadataImage {
        let setup = UniformQuotaImageSetup {
            entities,
            key,
            value,
        };
        image_with_quotas(
            setup
                .entities
                .into_iter()
                .map(|entity| {
                    quota_record()
                        .entity(entity)
                        .key(setup.key)
                        .value(setup.value)
                        .call()
                })
                .collect(),
        )
    }

    pub fn producer_quota_image(entities: Vec<QuotaEntitySpec<'_>>) -> MetadataImage {
        uniform_quota_image().entities(entities).call()
    }

    pub fn app1_producer_quota() -> ClientQuotaRecord {
        quota_record()
            .entity(vec![QuotaEntityInput::client_id(Some("app1"))])
            .value(200.0)
            .call()
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    pub struct UserBucketSetup {
        #[default(QuotaType::Produce)]
        pub quota_type: QuotaType,
        #[default(QuotaValue(0.0))]
        pub rate: QuotaValue,
    }

    #[bon::builder]
    pub fn alice_bucket(
        #[builder(default = QuotaType::Produce)] quota_type: QuotaType,
        #[builder(into, default = QuotaValue (0.0))] rate: QuotaValue,
    ) -> AliceBucket {
        let setup = UserBucketSetup { quota_type, rate };
        let buckets = Arc::new(super::QuotaBuckets::new());
        let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        let quota = buckets.client_quota(setup.quota_type, &key, setup.rate.0);
        AliceBucket { buckets, quota }
    }

    /// The 5,000-byte pair used to compare lookup precedence with bucket refresh.
    pub fn alice_app1_producer_quota() -> ClientQuotaRecord {
        quota_record()
            .entity(vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(Some("app1")),
            ])
            .value(5_000.0)
            .call()
    }

    pub fn image_with_quotas(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for record in records {
            image.apply(&MetadataRecord::V1ClientQuota(record));
        }
        image
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            test_support::{image_with_quota, manual_buckets},
            *,
        },
        assert2::{assert, check},
        krabka_units::secs,
    };

    /// `throttle_time_ms` rounds to the nearest millisecond, half up, as
    /// Kafka's `Math.round` does: 0.4 ms reports `0`, 0.6 ms and 1.6 ms report
    /// `1` and `2` (#1241).
    #[test]
    fn throttle_time_ms_rounds_to_the_nearest_millisecond() {
        let cases = [
            (krabka_units::micros(0), 0),
            (krabka_units::micros(400), 0),
            (krabka_units::micros(500), 1),
            (krabka_units::micros(600), 1),
            (krabka_units::micros(999), 1),
            (krabka_units::millis(1), 1),
            (krabka_units::micros(1_400), 1),
            (krabka_units::micros(1_500), 2),
            (krabka_units::micros(1_600), 2),
            (krabka_units::micros(1_999), 2),
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
    #[yare::parameterized(
        zero = { 0_u64 },
        one = { 1 },
        kibibyte = { 1024 },
        exact_integer_boundary = { 1 << 52 },
        largest_consecutive_integer = { (1_u64 << 53) - 1 },
        maximum = { u64::MAX },
    )]
    fn widening_agrees_with_the_former_producer_copy(value: u64) {
        let former: f64 = value.to_string().parse().unwrap_or(f64::INFINITY);
        check!(u64_to_f64(value).to_bits() == former.to_bits());
    }

    /// A quota that is missing or not a positive rate records nothing and
    /// creates no quota.
    #[yare::parameterized(
        negative_rate = { -1.0 },
        zero_rate = { 0.0 },
        infinite_rate = { f64::INFINITY },
    )]
    fn record_client_quota_ignores_a_rate_that_is_not_positive(rate: f64) {
        let image = image_with_quota().value(rate).call();
        let buckets = QuotaBuckets::new();

        let QuotaRecorded { delay, quota } = record_client_quota(QuotaConsumption {
            image: &image,
            buckets: &buckets,
            principal: "alice",
            client_id: Some(""),
            quota_type: QuotaType::Produce,
            amount: 1,
        });

        check!(delay == <Time as TimeExt>::ZERO);
        check!(quota.is_none());
        check!(buckets.is_empty());
    }

    /// Kafka records every request on the entity's sensor, a zero value too,
    /// so a request that adds nothing still creates the quota, and is still
    /// throttled while the entity is over it: 20 bytes at 1 B/s are 2 B/s
    /// over the padded 10 s window, a 10 s throttle whatever comes next.
    #[test]
    fn record_client_quota_records_a_zero_value() {
        let image = image_with_quota().value(1.0).call();
        let (_, buckets) = manual_buckets();
        let record = |amount| {
            record_client_quota(QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: Some(""),
                quota_type: QuotaType::Produce,
                amount,
            })
            .delay
            .delay
        };

        let fresh = record(0);
        let over = record(20);
        let nothing_more = record(0);

        check!((fresh, over, nothing_more) == (<Time as TimeExt>::ZERO, secs(10), secs(10)));
        assert!(buckets.len() == 1);
    }

    /// The throttle is charged to the entity the request resolved to, and a
    /// byte-rate throttle is not bounded: 100 bytes at 1 B/s over the padded
    /// 10 s window are 10 B/s, `(10 - 1) / 1 * 10 s` = 90 s.
    #[test]
    fn record_client_quota_charges_the_entity_and_leaves_the_throttle_unbounded() {
        let image = image_with_quota().value(1.0).call();
        let (_, buckets) = manual_buckets();

        let QuotaRecorded { delay, quota } = record_client_quota(QuotaConsumption {
            image: &image,
            buckets: &buckets,
            principal: "alice",
            client_id: Some("app"),
            quota_type: QuotaType::Produce,
            amount: 100,
        });

        check!(delay == QuotaDelay::new(secs(90), Some("alice".into()), None));
        check!(quota.is_some());
    }
}
