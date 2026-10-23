//! KIP-124 request charging and combined quota accounting for dispatch.

use {
    crate::{
        broker::Broker,
        handlers::ApiKeyCode,
        metrics::{BrokerMetrics, QuotaCharge, QuotaType},
        network::{auth::ConnectionAuth, request::ParsedRequest},
        quota::{QuotaDelay, consume_request_quota},
    },
    krabka_protocol::api_key::ApiKey,
    krabka_units::{Time, convert::TimeExt},
};

/// Charge a finished request and resolve its deferred charge in one metrics call.
///
/// `handler_time` counts active handler time, excluding coordinator,
/// replication and raft waits. This function never waits; dispatch reports
/// the delay on the response and the connection loop enforces it after the
/// write.
pub(super) fn apply_request_quota(
    broker: &Broker,
    parsed: &ParsedRequest<'_>,
    auth: &ConnectionAuth,
    handler_time: std::time::Duration,
    deferred_charge: Option<QuotaCharge>,
) -> Time {
    let elapsed_micros = u64::try_from(handler_time.as_micros()).unwrap_or(u64::MAX);
    let request_charge = request_charge(
        parsed.api_key,
        auth.principal().map(|principal| principal.name.as_str()),
        |principal| {
            let image = broker.controller.current_image();
            consume_request_quota(
                &image,
                &broker.quota_buckets,
                principal,
                parsed.client_id,
                elapsed_micros,
                broker.config.quota_throttle_max,
            )
        },
    );
    resolve_charges(
        &broker.metrics,
        parsed.api_key,
        request_charge,
        deferred_charge,
    )
}

/// Resolve the charges once, preserving API-specific tie attribution.
fn resolve_charges(
    metrics: &BrokerMetrics,
    api_key: ApiKeyCode,
    request_charge: Option<QuotaCharge>,
    deferred_charge: Option<QuotaCharge>,
) -> Time {
    let Some(request_charge) = request_charge else {
        return <Time as TimeExt>::ZERO;
    };
    // Keep the API-specific charge first: it wins equal-delay metric attribution.
    let quota_charges: Vec<QuotaCharge> = deferred_charge
        .into_iter()
        .chain(std::iter::once(request_charge))
        .collect();
    metrics
        .record_applied_throttle(api_key, &quota_charges)
        .max(<Time as TimeExt>::ZERO)
}

/// Produce and Fetch account in their handlers. Pre-auth SASL requests
/// consume no quota but still contribute a zero to the request throttle
/// histogram. Authenticated ANONYMOUS connections consume their principal's
/// quota as usual.
fn request_charge(
    api_key: ApiKeyCode,
    principal: Option<&str>,
    consume: impl FnOnce(&str) -> QuotaDelay,
) -> Option<QuotaCharge> {
    if matches!(
        ApiKey::from_i16(api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    ) {
        return None;
    }
    let charged = principal.map_or_else(QuotaDelay::zero, consume);
    Some((QuotaType::Request, charged).into())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_units::millis};

    #[test]
    fn deferred_and_request_charges_resolve_to_the_largest_delay() {
        for (request_ms, deferred_ms, expected_ms) in
            [(100, 250, 250), (250, 100, 250), (250, 250, 250)]
        {
            let metrics = BrokerMetrics::new();
            let delay = resolve_charges(
                &metrics,
                ApiKey::Metadata as i16,
                Some((QuotaType::Request, millis(request_ms)).into()),
                Some((QuotaType::ControllerMutation, millis(deferred_ms)).into()),
            );
            assert!(delay == millis(expected_ms));
        }
    }

    #[test]
    fn self_accounted_requests_do_not_resolve_deferred_charges_twice() {
        let metrics = BrokerMetrics::new();
        let delay = resolve_charges(
            &metrics,
            ApiKey::Produce as i16,
            None,
            Some((QuotaType::Produce, millis(250)).into()),
        );
        assert!(delay == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn self_accounted_apis_do_not_consume_request_quota() {
        for api in [ApiKey::Produce, ApiKey::Fetch] {
            let charge = request_charge(api as i16, Some("alice"), |_| {
                panic!("handler already charged this request")
            });
            assert!(charge.is_none());
        }
    }

    #[test]
    fn pre_auth_charge_is_zero_but_is_accounted() {
        let charge = request_charge(ApiKey::ApiVersions as i16, None, |_| {
            panic!("pre-auth request has no quota principal")
        })
        .expect("request histogram observation");
        assert!(
            (
                charge.quota_type,
                charge.delay,
                charge.user,
                charge.client_id
            ) == (QuotaType::Request, <Time as TimeExt>::ZERO, None, None)
        );
    }

    #[test]
    fn authenticated_principals_preserve_charge_identity_and_delay() {
        for principal in ["alice", "ANONYMOUS"] {
            let expected = QuotaDelay::new(
                millis(250),
                Some(principal.to_owned()),
                Some("client".to_owned()),
            );
            let charge = request_charge(ApiKey::Metadata as i16, Some(principal), |name| {
                assert!(name == principal);
                expected.clone()
            })
            .expect("request charge");
            assert!(
                (
                    charge.quota_type,
                    charge.delay,
                    charge.user,
                    charge.client_id
                ) == (
                    QuotaType::Request,
                    expected.delay,
                    expected.user,
                    expected.client_id
                )
            );
        }
    }
}
