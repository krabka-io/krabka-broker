//! Adapters for the two client-telemetry apis of KIP-714, whose handlers take
//! a [`TelemetryContext`] instead of a full `RequestContext`.

use bytes::Bytes;
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, TelemetryContext},
};

telemetry_adapter!(
    get_telemetry_subscriptions_adapter,
    crate::handlers::get_telemetry_subscriptions::handle
);
telemetry_adapter!(
    push_telemetry_adapter,
    crate::handlers::push_telemetry::handle
);
