//! Registration table for the krabka-private apis, whose wire codes sit above
//! the Kafka range and so name their `api_key` and `flexible_min` directly.

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::{ContextHandler, DispatchEntry, DispatchRegistry};
use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId, RequestContext},
};

// `KRABKA_PRIVATE_API_KEY_FLOOR` for why.
// `flexible_min` comes from the codec of each message, so the framing the
// registry reports and the framing the codec writes cannot drift apart.
krabka_private_context_dispatches!(register_krabka_private_context_dispatches;
    (
        alter_barrier_groups_adapter,
        crate::handlers::ALTER_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::alter_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::alter_groups::handle
    ),
    (
        describe_barrier_groups_adapter,
        crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::describe_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::describe_groups::handle
    ),
    (
        trigger_barrier_adapter,
        crate::handlers::TRIGGER_BARRIER_API_KEY,
        krabka_protocol::krabka::barrier::trigger_barrier::FLEXIBLE_MIN,
        crate::barrier::handlers::trigger::handle
    ),
    (
        list_barrier_cuts_adapter,
        crate::handlers::LIST_BARRIER_CUTS_API_KEY,
        krabka_protocol::krabka::barrier::list_barrier_cuts::FLEXIBLE_MIN,
        crate::barrier::handlers::list_cuts::handle
    ),
    (
        write_barrier_markers_adapter,
        crate::handlers::WRITE_BARRIER_MARKERS_API_KEY,
        krabka_protocol::krabka::barrier::write_barrier_markers::FLEXIBLE_MIN,
        crate::barrier::handlers::write_markers::handle
    ),
);
