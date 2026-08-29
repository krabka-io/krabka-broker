//! `StreamsGroupDescribe` (`api_key` 89), from KIP-1071. It returns one
//! `DescribedGroup` per requested `group_id`, rendered from the streams actor's
//! `Describe` view.
//!
//! Mirrors the KIP-848 consumer-group describe handler and applies a
//! per-group `Describe` ACL before consulting the streams actor.
//!
//! This file holds only the wire entry point: it decodes the request, resolves
//! the KIP-1071 feature gate once, and collects one row per requested group.
//! `group` decides a single group's row, from the ACL gate through the streams
//! actor query, and `render` projects the actor's describe view onto the
//! response types.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::{DescribedGroup, StreamsGroupDescribeResponse},
    },
};

mod group;
mod render;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::group::describe_group;
use crate::{broker::Broker, error::BrokerError};

/// Minimum finalized `streams.version` feature level at which the broker
/// serves the KIP-1071 streams RPCs, heartbeat and describe.
const STREAMS_VERSION_MIN_LEVEL: i16 = 1;

// cargo-mutants: streams-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    let mut cur: &[u8] = req_bytes;
    let req = StreamsGroupDescribeRequest::decode(&mut cur, version)?;

    // KIP-1071: same gate as the heartbeat — finalized streams.version >= 1
    // AND the config kill-switch. If disabled, each requested group gets a
    // GROUP_ID_NOT_FOUND error row (the protocol does not serve here).
    let enabled = crate::features::feature_enabled(
        &image,
        crate::features::STREAMS_VERSION,
        STREAMS_VERSION_MIN_LEVEL,
    ) && streams_enabled;

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        groups.push(describe_group(broker, &ng, &image, ctx, enabled, gid).await);
    }

    let resp = StreamsGroupDescribeResponse {
        groups,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
