//! `AlterConfigs` (`api_key=33`) for topic and broker resources.
//!
//! The handler builds each resource's full override map from the request.
//! That map is the *complete* set of non-default values for the resource.
//! Topic configs use one authoritative `V1TopicConfig` record. Broker configs
//! use Kafka-compatible per-key `V1BrokerConfig` records, including tombstones
//! for overrides omitted from the replacement. An empty broker resource name
//! targets Kafka's cluster-wide default broker config.
//!
//! Controller-managed broker keys stand outside the replacement. The handler
//! rejects a request that names one, and the tombstone sweep leaves them in
//! place. See [`crate::config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS`].
//!
//! This file holds the wire entry point and the resource-type constants. The
//! per-resource work lives in `resource`, and the record builders it
//! dispatches to live in `topic_configs` and `broker_configs`.

use bytes::Bytes;
use krabka_protocol::{
    Decode, UnknownTaggedFields,
    owned::{
        alter_configs_request::AlterConfigsRequest,
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
};

mod broker_configs;
mod resource;
mod topic_configs;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::resource::process_resource;
use crate::{broker::Broker, error::BrokerError};

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

#[tracing::instrument(
    name = "handle_alter_configs",
    level = "info",
    skip_all,
    fields(api = "AlterConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = AlterConfigsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let mut responses: Vec<AlterConfigsResourceResponse> = Vec::with_capacity(req.resources.len());

    for resource in req.resources {
        responses.push(process_resource(broker, &image, ctx, resource, req.validate_only).await);
    }

    let resp = AlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    crate::handlers::encode_response(&resp, version)
}
