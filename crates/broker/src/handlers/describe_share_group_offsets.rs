//! `DescribeShareGroupOffsets` (`api_key` 90), from KIP-932. It returns the
//! share-partition start offset (SPSO), leader epoch, and best-effort lag for
//! each requested `(group, topic, partition)`, read from the share-state
//! persister.
//!
//! `network::dispatch` intercepts it inline, so the handler receives the
//! per-connection principal and peer `SocketAddr` for the per-group `Describe`
//! ACL gate.
//!
//! This file holds only the wire entry point and the broker-wide feature gate.
//! `group` resolves one requested group, from the ACL check to the persister
//! lookup; `topics` decides which topics that group reports when the request
//! names none; and `rows` builds the topic and partition rows themselves.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest,
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
        },
    },
};

mod group;
mod rows;
mod topics;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::group::describe_group;
use crate::{broker::Broker, codes, error::BrokerError};

#[tracing::instrument(
    name = "handle_describe_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DescribeShareGroupOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
// cargo-mutants: share-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the
    // RPC. The response has no top-level error code, so mark every requested
    // group with UNSUPPORTED_VERSION.
    if !broker.config.share_group.enable {
        let groups = req
            .groups
            .iter()
            .map(|g| DescribeShareGroupOffsetsResponseGroup {
                group_id: g.group_id.clone(),
                error_code: codes::UNSUPPORTED_VERSION,
                ..Default::default()
            })
            .collect();
        let resp = DescribeShareGroupOffsetsResponse {
            groups,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());

    let mut groups: Vec<DescribeShareGroupOffsetsResponseGroup> =
        Vec::with_capacity(req.groups.len());

    for group in req.groups {
        groups.push(describe_group(broker, ng_opt.as_deref(), &image, ctx, group).await);
    }

    let resp = DescribeShareGroupOffsetsResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
