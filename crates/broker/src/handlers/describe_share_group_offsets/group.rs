//! One requested group's row in a `DescribeShareGroupOffsets` response, from
//! the `Describe` ACL gate to the share-state persister lookup.
//!
//! KIP-932 gives the response no top-level error code, so every refusal is a
//! per-group `error_code` and an empty topic list. That is what this module
//! decides: authorization, coordinator routing, and whether a persister is
//! installed at all. Once those hold, it hands the group's topics to `rows`.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestGroup,
    describe_share_group_offsets_response::{
        DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponseTopic,
    },
};

use super::{rows::describe_topic, topics::requested_topics};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::GroupCoordinator,
};

/// Resolve one requested group into its response row.
// cargo-mutants: share-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_group(
    broker: &Broker,
    ng: Option<&GroupCoordinator>,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    group: DescribeShareGroupOffsetsRequestGroup,
) -> DescribeShareGroupOffsetsResponseGroup {
    let gid = group.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Describe` check. On Deny → group `error_code = 30`.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: gid.as_str(),
        operation: AclOperation::Describe,
    };
    if broker.config.authorizer.authorize(image, &acl_req) == AuthorizationResult::Deny {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            ..Default::default()
        };
    }
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code,
            ..Default::default()
        };
    }

    // The persister is required to read SPSO. Absent (share groups
    // disabled / not yet bootstrapped) → coordinator-not-available.
    let Some(persister) = ng.and_then(|ng| ng.share_persister().cloned()) else {
        return DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            error_code: codes::COORDINATOR_NOT_AVAILABLE,
            ..Default::default()
        };
    };

    let metadata = ng.and_then(|ng| ng.share_state_partition_metadata(&gid));

    let req_topics = requested_topics(group.topics, metadata.as_ref(), image);
    let mut topics: Vec<DescribeShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req_topics.len());

    for rt in req_topics {
        topics.push(describe_topic(broker, &persister, image, metadata.as_ref(), &gid, rt).await);
    }

    DescribeShareGroupOffsetsResponseGroup {
        group_id: gid,
        topics,
        error_code: codes::NONE,
        ..Default::default()
    }
}
