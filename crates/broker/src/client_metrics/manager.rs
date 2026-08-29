//! Per-broker KIP-714 client-metrics state: the instance registry,
//! subscription matching, stable subscription-id computation, and push
//! throttling.
//!
//! All state is in memory. KIP-714 is per-broker, because a client pins its
//! telemetry to one broker, so this state needs no raft replication.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use krabka_metadata::MetadataImage;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use uuid::Uuid;

mod subscription;
#[cfg(test)]
mod test_support;

pub(crate) use self::subscription::{compute_subscription, subscription_id};

/// Connection-derived attributes used for subscription matching.
#[derive(Debug, Clone)]
pub(crate) struct ClientAttributes {
    pub client_instance_id: Uuid,
    pub client_id: String,
    pub software_name: String,
    pub software_version: String,
    pub source_address: String,
    pub source_port: u16,
}

/// The metric prefixes a client should send, and the push interval it must
/// use, after the union of every matched subscription.
///
/// The broker enforces the interval in `authorize_push`. It does not inspect
/// the payload, so the prefixes are advisory.
#[derive(Debug, Clone)]
pub(crate) struct ComputedSubscription {
    pub metrics: Vec<String>,
    pub push_interval_ms: i32,
}

#[derive(Debug)]
struct ClientInstance {
    subscription_id: i32,
    push_interval: Duration,
    last_get: Instant,
    last_push: Option<Instant>,
    terminating: bool,
    last_error: i16,
}

pub(crate) struct SubscriptionAssignment {
    pub subscription_id: i32,
    pub push_interval_ms: i32,
    pub metrics: Vec<String>,
}

pub(crate) enum SubscriptionDecision {
    Assign(SubscriptionAssignment),
    Reject { error_code: i16, throttle_ms: i32 },
}

pub(crate) enum PushDecision {
    Accept,
    Reject { error_code: i16, throttle_ms: i32 },
}

pub(crate) struct ClientMetricsManager {
    instances: Mutex<HashMap<Uuid, ClientInstance>>,
    default_interval: Time,
    telemetry_max: ByteSize,
}

/// Compression codecs that the broker advertises, in Kafka's fixed order:
/// ZSTD(4), LZ4(3), GZIP(1), and SNAPPY(2). The broker deliberately does not
/// advertise NONE.
pub(crate) const ACCEPTED_COMPRESSION_TYPES: [i8; 4] = [4, 3, 1, 2];

impl ClientMetricsManager {
    pub(crate) fn new(telemetry_max: ByteSize, default_interval: Time) -> Self {
        Self {
            instances: Mutex::new(HashMap::new()),
            default_interval,
            telemetry_max,
        }
    }

    /// The KIP-714 `PushTelemetry` size ceiling in the `int32` byte form the
    /// wire response carries.
    pub(crate) fn telemetry_max_bytes(&self) -> i32 {
        self.telemetry_max.bytes_i32()
    }

    pub(crate) fn assign(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> SubscriptionDecision {
        self.assign_at(image, attrs, Instant::now())
    }

    fn assign_at(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
        now: Instant,
    ) -> SubscriptionDecision {
        // `push_interval_ms` is both a wire field and a byte-exact input to the
        // subscription-id hash, so the interval crosses into milliseconds here.
        let computed = compute_subscription(image, attrs, self.default_interval.millis_i32());
        let sub_id = subscription_id(&computed, attrs.client_instance_id);
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");
        // push_interval_ms is validated in [100, 3_600_000] — always positive.
        let push_interval = Duration::from_millis(
            u64::try_from(computed.push_interval_ms).expect("validated positive push interval"),
        );
        if let Some(inst) = guard.get_mut(&attrs.client_instance_id) {
            inst.subscription_id = sub_id;
            inst.push_interval = push_interval;

            let retry_after_error = matches!(
                inst.last_error,
                crate::codes::UNKNOWN_SUBSCRIPTION_ID | crate::codes::UNSUPPORTED_COMPRESSION_TYPE
            );
            let last_message = inst
                .last_push
                .map_or(inst.last_get, |last_push| inst.last_get.max(last_push));
            if !retry_after_error && now.duration_since(last_message) < push_interval {
                inst.last_error = crate::codes::THROTTLING_QUOTA_EXCEEDED;
                return SubscriptionDecision::Reject {
                    error_code: crate::codes::THROTTLING_QUOTA_EXCEEDED,
                    throttle_ms: computed.push_interval_ms,
                };
            }

            inst.last_get = now;
            inst.last_error = crate::codes::NONE;
        } else {
            guard.insert(
                attrs.client_instance_id,
                ClientInstance {
                    subscription_id: sub_id,
                    push_interval,
                    last_get: now,
                    last_push: None,
                    terminating: false,
                    last_error: crate::codes::NONE,
                },
            );
        }
        SubscriptionDecision::Assign(SubscriptionAssignment {
            subscription_id: sub_id,
            push_interval_ms: computed.push_interval_ms,
            metrics: computed.metrics,
        })
    }

    pub(crate) fn authorize_push(
        &self,
        client_instance_id: Uuid,
        subscription_id_in: i32,
        terminating: bool,
        compression_supported: bool,
        payload_len: usize,
    ) -> PushDecision {
        let now = Instant::now();
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");

        // 1. Unknown instance → INVALID_REQUEST.
        let Some(inst) = guard.get_mut(&client_instance_id) else {
            return PushDecision::Reject {
                error_code: crate::codes::INVALID_REQUEST,
                throttle_ms: 0,
            };
        };

        // 2. Instance already terminating → INVALID_REQUEST.
        if inst.terminating {
            return PushDecision::Reject {
                error_code: crate::codes::INVALID_REQUEST,
                throttle_ms: 0,
            };
        }

        // 3. Subscription-id mismatch → UNKNOWN_SUBSCRIPTION_ID.
        if subscription_id_in != inst.subscription_id {
            inst.last_error = crate::codes::UNKNOWN_SUBSCRIPTION_ID;
            return PushDecision::Reject {
                error_code: crate::codes::UNKNOWN_SUBSCRIPTION_ID,
                throttle_ms: 0,
            };
        }

        // 4. Throttle check (Kafka order: throttle before codec/size).
        let interval_elapsed = inst
            .last_push
            .is_none_or(|lp| now.duration_since(lp) >= inst.push_interval);
        let first_after_get = inst.last_push.is_none_or(|lp| inst.last_get > lp);
        if !terminating && !interval_elapsed && !first_after_get {
            inst.last_error = crate::codes::THROTTLING_QUOTA_EXCEEDED;
            let throttle_ms = i32::try_from(inst.push_interval.as_millis()).unwrap_or(i32::MAX);
            return PushDecision::Reject {
                error_code: crate::codes::THROTTLING_QUOTA_EXCEEDED,
                throttle_ms,
            };
        }

        // 5. Unsupported compression codec → UNSUPPORTED_COMPRESSION_TYPE.
        //    Do NOT update last_push on this path.
        if !compression_supported {
            inst.last_error = crate::codes::UNSUPPORTED_COMPRESSION_TYPE;
            return PushDecision::Reject {
                error_code: crate::codes::UNSUPPORTED_COMPRESSION_TYPE,
                throttle_ms: 0,
            };
        }

        // 6. Payload oversize → TELEMETRY_TOO_LARGE.
        //    Do NOT update last_push on this path.
        let max_payload_len = self.telemetry_max.bytes_usize();
        if payload_len > max_payload_len {
            inst.last_error = crate::codes::TELEMETRY_TOO_LARGE;
            return PushDecision::Reject {
                error_code: crate::codes::TELEMETRY_TOO_LARGE,
                throttle_ms: 0,
            };
        }

        // 7. Success: update state. Metric prefixes are advisory to the
        // client; the broker does not filter the OTLP payload by name.
        inst.last_push = Some(now);
        inst.last_error = crate::codes::NONE;
        if terminating {
            inst.terminating = true;
        }
        PushDecision::Accept
    }

    /// Drops an instance that has been idle for longer than
    /// `max(interval * factor, floor)`.
    pub(crate) fn evict_stale(&self, factor: u32, floor: Duration) {
        let now = Instant::now();
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");
        guard.retain(|_, inst| {
            if inst.terminating {
                return false;
            }
            let ttl = (inst.push_interval * factor).max(floor);
            let last = inst.last_push.unwrap_or(inst.last_get);
            now.duration_since(last) < ttl
        });
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::{
        test_support::{attrs, expect_assignment, img_with},
        *,
    };

    #[test]
    fn push_throttle_ladder() {
        let m = ClientMetricsManager::new(krabka_units::kibibytes(1), krabka_units::minutes(5));
        let id = Uuid::from_u128(7);
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = ClientAttributes {
            client_instance_id: id,
            client_id: "c".into(),
            software_name: "n".into(),
            software_version: "v".into(),
            source_address: "1.2.3.4".into(),
            source_port: 1,
        };
        let assigned = expect_assignment(m.assign(&img, &attrs));
        // First push after assign is allowed (compression_supported=true).
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 10),
            PushDecision::Accept
        ));
        // Immediate second push (interval not elapsed, no new get) is throttled —
        // even if the payload would also be oversized; throttle wins per Kafka order.
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
        ));
        // Ordering assertion: oversized payload that is ALSO interval-not-elapsed
        // must return THROTTLING_QUOTA_EXCEEDED (throttle before size in ladder).
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 2048),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
        ));
        // Wrong subscription id → UNKNOWN_SUBSCRIPTION_ID.
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id ^ 0x5555, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::UNKNOWN_SUBSCRIPTION_ID
        ));
        // Unknown instance → INVALID_REQUEST.
        assert!(matches!(
            m.authorize_push(Uuid::from_u128(999), 0, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::INVALID_REQUEST
        ));
        // Re-assign to get a fresh get-timestamp (acts as a new allowance).
        let assigned2 = expect_assignment(m.assign(&img, &attrs));
        // Oversized payload with fresh get → TELEMETRY_TOO_LARGE.
        assert!(matches!(
            m.authorize_push(id, assigned2.subscription_id, false, true, 2048),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::TELEMETRY_TOO_LARGE
        ));
        // Unsupported compression on a fresh instance + small payload →
        // UNSUPPORTED_COMPRESSION_TYPE.
        let mut attrs2 = attrs.clone();
        attrs2.client_instance_id = Uuid::from_u128(8);
        let assigned3 = expect_assignment(m.assign(&img, &attrs2));
        assert!(matches!(
            m.authorize_push(
                attrs2.client_instance_id,
                assigned3.subscription_id,
                false,
                false,
                10
            ),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::UNSUPPORTED_COMPRESSION_TYPE
        ));
    }

    #[test]
    fn get_subscription_throttles_but_allows_error_recovery() {
        let m = ClientMetricsManager::new(krabka_units::kibibytes(1), krabka_units::minutes(5));
        let id = Uuid::from_u128(7);
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = ClientAttributes {
            client_instance_id: id,
            client_id: "c".into(),
            software_name: "n".into(),
            software_version: "v".into(),
            source_address: "1.2.3.4".into(),
            source_port: 1,
        };
        let assigned = expect_assignment(m.assign(&img, &attrs));

        assert!(matches!(
            m.assign(&img, &attrs),
            SubscriptionDecision::Reject { error_code, throttle_ms }
                if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
                    && throttle_ms == 60_000
        ));

        assert!(matches!(
            m.authorize_push(
                id,
                assigned.subscription_id ^ 0x5555,
                false,
                true,
                10
            ),
            PushDecision::Reject { error_code, .. }
                if error_code == crate::codes::UNKNOWN_SUBSCRIPTION_ID
        ));
        let assigned = expect_assignment(m.assign(&img, &attrs));

        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, false, 10),
            PushDecision::Reject { error_code, .. }
                if error_code == crate::codes::UNSUPPORTED_COMPRESSION_TYPE
        ));
        let _ = expect_assignment(m.assign(&img, &attrs));
    }

    #[test]
    fn get_subscription_is_throttled_after_a_recent_push() {
        let m = ClientMetricsManager::new(krabka_units::kibibytes(1), krabka_units::minutes(5));
        let id = Uuid::from_u128(7);
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "100")]);
        let attrs = ClientAttributes {
            client_instance_id: id,
            client_id: "c".into(),
            software_name: "n".into(),
            software_version: "v".into(),
            source_address: "1.2.3.4".into(),
            source_port: 1,
        };
        let assigned = expect_assignment(m.assign(&img, &attrs));
        std::thread::sleep(Duration::from_millis(120));
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 10),
            PushDecision::Accept
        ));

        assert!(matches!(
            m.assign(&img, &attrs),
            SubscriptionDecision::Reject { error_code, .. }
                if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
        ));
    }

    #[test]
    fn get_subscription_accepts_exact_interval_boundary() {
        let m = ClientMetricsManager::new(krabka_units::kibibytes(1), krabka_units::minutes(5));
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "100")]);
        let attrs = attrs();
        let start = Instant::now();
        let _ = expect_assignment(m.assign_at(&img, &attrs, start));

        assert!(matches!(
            m.assign_at(&img, &attrs, start + Duration::from_millis(99)),
            SubscriptionDecision::Reject { .. }
        ));
        assert!(matches!(
            m.assign_at(&img, &attrs, start + Duration::from_millis(100)),
            SubscriptionDecision::Assign(_)
        ));
    }
}
