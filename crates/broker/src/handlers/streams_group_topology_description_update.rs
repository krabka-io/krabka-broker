//! `StreamsGroupTopologyDescriptionUpdate` (`api_key` 93, KIP-1331, Kafka
//! trunk).
//!
//! A Streams client pushes its full topology description with this RPC after
//! a heartbeat answers `TopologyDescriptionRequired`. Kafka stores it through
//! the plugin `group.streams.topology.description.plugin.class` names, and a
//! broker has no plugin by default. krabka has no plugin at all, so it answers
//! every push exactly as a trunk broker without a plugin does:
//!
//! 1. `UNSUPPORTED_VERSION` (35) with no message while the streams protocol is
//!    off, which is `KafkaApis.handleStreamsGroupTopologyDescriptionUpdate`.
//! 2. `GROUP_AUTHORIZATION_FAILED` (30) with no message when the caller lacks
//!    `Read` on the group. KIP-1331 treats a push like an offset commit, so
//!    `Read` is enough.
//! 3. `UNSUPPORTED_VERSION` (35) with the message of the
//!    `UnsupportedVersionException` that
//!    `GroupCoordinatorService.throwIfStreamsGroupTopologyDescriptionUpdateInvalid`
//!    throws when `isPluginConfigured()` is false.
//!
//! The third answer comes before trunk's member and group validation, so no
//! request reaches the coordinator. The heartbeat never asks for a push for
//! the same reason (`StreamsGroupTopologyDescriptionManager.decorateHeartbeatResult`
//! returns the heartbeat untouched without a plugin), and `StreamsGroupDescribe`
//! v1 reports `NOT_STORED` for every described group.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest,
        streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, group_read_denied},
};

/// The message of trunk's `UnsupportedVersionException` for a broker with no
/// topology description plugin.
const NO_PLUGIN_MESSAGE: &str =
    "The broker has no streams group topology description plugin configured.";

/// Minimum finalized `streams.version` at which the broker serves the
/// KIP-1071 streams RPCs.
const STREAMS_VERSION_MIN_LEVEL: i16 = 1;

#[tracing::instrument(
    name = "handle_streams_group_topology_description_update",
    level = "info",
    skip_all,
    fields(api = "StreamsGroupTopologyDescriptionUpdate", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = StreamsGroupTopologyDescriptionUpdateRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();
    let (error_code, error_message) = answer(broker, &image, &req, ctx);
    crate::handlers::encode_response(
        &StreamsGroupTopologyDescriptionUpdateResponse {
            error_code,
            error_message: error_message.map(str::to_owned),
            ..Default::default()
        },
        version,
    )
}

/// The error code and message trunk answers `req` with on a broker that has
/// no topology description plugin.
fn answer(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    req: &StreamsGroupTopologyDescriptionUpdateRequest,
    ctx: &RequestContext<'_>,
) -> (i16, Option<&'static str>) {
    let streams_enabled = broker.config.streams_group.enable
        && crate::features::feature_enabled(
            image,
            crate::features::STREAMS_VERSION,
            STREAMS_VERSION_MIN_LEVEL,
        );
    if !streams_enabled {
        return (codes::UNSUPPORTED_VERSION, None);
    }
    if group_read_denied(broker.config.authorizer.as_ref(), image, ctx, &req.group_id) {
        return (codes::GROUP_AUTHORIZATION_FAILED, None);
    }
    (codes::UNSUPPORTED_VERSION, Some(NO_PLUGIN_MESSAGE))
}

#[cfg(test)]
mod tests;
