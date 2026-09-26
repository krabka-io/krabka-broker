//! `GetTelemetrySubscriptions` (`api_key=71`, KIP-714).
//!
//! This handler assigns or echoes the client instance id. It matches the
//! client against the configured `CLIENT_METRICS` subscriptions. It then
//! returns the computed subscription, which holds the metrics, the interval,
//! and the id. See `client_metrics::manager`.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
        get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use uuid::Uuid;

use crate::{
    broker::Broker,
    client_metrics::manager::{ACCEPTED_COMPRESSION_TYPES, ClientAttributes, SubscriptionDecision},
    error::BrokerError,
    handlers::context::TelemetryContext,
};

#[tracing::instrument(
    name = "handle_get_telemetry_subscriptions",
    level = "info",
    skip_all,
    fields(api = "GetTelemetrySubscriptions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = GetTelemetrySubscriptionsRequest::decode(&mut cur, version)?;

    let attrs = ClientAttributes {
        client_instance_id: Uuid::from_bytes(req.client_instance_id.0),
        client_id: ctx.client_id.to_string(),
        software_name: ctx.software_name.to_string(),
        software_version: ctx.software_version.to_string(),
        source_address: ctx.peer.ip().to_string(),
        source_port: ctx.peer.port(),
    };

    let image = broker.controller.current_image();
    let resp = match broker
        .client_metrics
        .manager
        .get_subscription(&image, &attrs)
    {
        SubscriptionDecision::Assign(assignment) => GetTelemetrySubscriptionsResponse {
            client_instance_id: WireUuid(assignment.client_instance_id.into_bytes()),
            subscription_id: assignment.subscription_id,
            accepted_compression_types: ACCEPTED_COMPRESSION_TYPES.to_vec(),
            push_interval_ms: assignment.push_interval_ms,
            telemetry_max_bytes: broker.client_metrics.manager.telemetry_max_bytes(),
            delta_temporality: true,
            requested_metrics: assignment.metrics,
            ..Default::default()
        },
        // Kafka answers a rejected request with `throttle_time_ms` 0; only the
        // request quota raises it.
        SubscriptionDecision::Reject { error_code } => GetTelemetrySubscriptionsResponse {
            error_code,
            ..Default::default()
        },
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::get_telemetry_subscriptions_response;

    use super::*;
    use crate::{
        client_metrics::{
            config::INTERVAL_MS_DEFAULT,
            manager::{ComputedSubscription, subscription_id},
        },
        codes,
    };

    crate::test_support::codec_helpers!(
        GetTelemetrySubscriptionsRequest,
        GetTelemetrySubscriptionsResponse,
        version = get_telemetry_subscriptions_response::MAX_VERSION
    );

    /// Kafka answers with the client instance id it used: a new one for a
    /// zero request id, the request's id otherwise (#665). An early repeat
    /// gets `THROTTLING_QUOTA_EXCEEDED` with `throttle_time_ms` 0 (#672).
    #[tokio::test]
    async fn get_answers_with_the_instance_id_and_throttles_without_delay() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|_cfg| {}).await;
        let broker = broker_handle.broker_arc_for_test();
        let peer = "127.0.0.1:9092".parse().unwrap();
        let ctx = TelemetryContext {
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        };

        let get = |id: WireUuid| {
            decode_response(
                &handle(
                    &broker,
                    get_telemetry_subscriptions_response::MAX_VERSION,
                    7,
                    &encode_request(&GetTelemetrySubscriptionsRequest {
                        client_instance_id: id,
                        ..Default::default()
                    }),
                    &ctx,
                )
                .expect("get"),
            )
        };
        let assigned = |id: WireUuid| GetTelemetrySubscriptionsResponse {
            client_instance_id: id,
            subscription_id: subscription_id(
                &ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: INTERVAL_MS_DEFAULT,
                },
                Uuid::from_bytes(id.0),
            ),
            accepted_compression_types: ACCEPTED_COMPRESSION_TYPES.to_vec(),
            push_interval_ms: INTERVAL_MS_DEFAULT,
            telemetry_max_bytes: broker.client_metrics.manager.telemetry_max_bytes(),
            delta_temporality: true,
            ..Default::default()
        };
        let throttled = GetTelemetrySubscriptionsResponse {
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            ..Default::default()
        };

        let fresh = get(WireUuid::ZERO);
        assert!(fresh.client_instance_id != WireUuid::ZERO);
        assert!(fresh == assigned(fresh.client_instance_id));

        let known = WireUuid(Uuid::from_u128(7).into_bytes());
        let rows = [
            ("a client's own id", known, assigned(known)),
            ("an early repeat", known, throttled.clone()),
            (
                "an early repeat of the new id",
                fresh.client_instance_id,
                throttled,
            ),
        ];
        for (name, id, expected) in rows {
            assert!(get(id) == expected, "row {name}");
        }

        broker_handle.shutdown().await;
    }
}
