//! One group's row in a `StreamsGroupDescribe` response, from the `Describe`
//! ACL gate through the streams actor query.
//!
//! Every exit in this module produces a `DescribedGroup` rather than failing
//! the request, which is what lets a denied, misrouted, or unknown group sit
//! beside fully resolved groups in the same response. KIP-1071 gates the RPC
//! on the finalized `streams.version` feature level and on the broker's
//! streams kill-switch; the caller resolves that gate once per request and
//! passes the answer in as `enabled`.

use krabka_protocol::owned::streams_group_describe_response::DescribedGroup;
use tokio::sync::oneshot;

use super::render::render_group;
use crate::{
    broker::Broker,
    codes,
    coordinator::{GroupCoordinator, unified::streams::actor::StreamsGroupActorMessage},
};

/// Resolve one requested `group_id` into its `DescribedGroup` row.
// cargo-mutants: streams-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_group(
    broker: &Broker,
    ng: &GroupCoordinator,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    enabled: bool,
    gid: &str,
) -> DescribedGroup {
    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        krabka_metadata::ResourceType::Group,
        gid,
        krabka_metadata::AclOperation::Describe,
    ) {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            ..Default::default()
        };
    }
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, gid) {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code,
            ..Default::default()
        };
    }
    if !enabled {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::UNSUPPORTED_VERSION,
            ..Default::default()
        };
    }
    let Some(handle) = ng.find_streams(gid) else {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::GROUP_ID_NOT_FOUND,
            ..Default::default()
        };
    };
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(StreamsGroupActorMessage::Describe { reply: tx })
        .await
        .is_err()
    {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
            ..Default::default()
        };
    }
    match rx.await {
        Ok(view) => render_group(view),
        Err(_) => DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::UNKNOWN_SERVER_ERROR,
            ..Default::default()
        },
    }
}
