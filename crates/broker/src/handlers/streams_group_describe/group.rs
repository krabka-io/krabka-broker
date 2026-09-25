//! One group's row in a `StreamsGroupDescribe` response, from the streams
//! actor query through the topology topic-`Describe` filter.
//!
//! Every exit in this module produces a `DescribedGroup` rather than failing
//! the request, which is what lets a denied, misrouted, or unknown group sit
//! beside fully resolved groups in the same response. The caller
//! (`handle()`) already resolved the KIP-1071 protocol gate and the
//! per-group `Group:Describe` ACL before calling this function, matching
//! Kafka's `handleStreamsGroupDescribe`: the protocol gate is checked once
//! for the whole request, and denied groups are gathered separately so their
//! rows sort first in the response.

use krabka_protocol::owned::streams_group_describe_response::DescribedGroup;
use tokio::sync::oneshot;

use super::{render::render_group, topic_authz};
use crate::{
    broker::Broker,
    codes,
    coordinator::{GroupCoordinator, unified::streams::actor::StreamsGroupActorMessage},
    handlers::authorized_operations::authorized_operations_bits,
};

/// Kafka's message for a group whose topology names a topic the caller
/// cannot `Describe`.
const TOPIC_AUTHZ_DENIED_MESSAGE: &str =
    "The described group uses topics that the client is not authorized to describe.";

/// Resolve one requested `group_id`, already past the protocol gate and the
/// group `Describe` ACL check, into its `DescribedGroup` row.
// cargo-mutants: streams-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_group(
    broker: &Broker,
    ng: &GroupCoordinator,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    include_authorized_operations: bool,
    gid: &str,
) -> DescribedGroup {
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, gid) {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code,
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
    let Ok(view) = rx.await else {
        return DescribedGroup {
            group_id: gid.to_owned(),
            error_code: codes::UNKNOWN_SERVER_ERROR,
            ..Default::default()
        };
    };

    // Kafka hides a group whose topology names a topic the caller cannot
    // `Describe` (source, repartition sink, repartition source or changelog):
    // the row becomes `TOPIC_AUTHORIZATION_FAILED` with no topology and no
    // members, instead of disclosing the topic names via the topology.
    if let Some(topology) = view.topology.as_ref() {
        let required = topic_authz::required_topics(topology);
        if topic_authz::describe_denied(broker, image, ctx, &required) {
            return DescribedGroup {
                group_id: gid.to_owned(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some(TOPIC_AUTHZ_DENIED_MESSAGE.to_owned()),
                ..Default::default()
            };
        }
    }

    let mut row = render_group(view);
    // KIP-430: fill the bitfield of Group operations the caller is
    // authorized for only when the request opted in; otherwise leave the
    // wire-default `i32::MIN` "not set" sentinel `render_group` already set.
    if include_authorized_operations {
        row.authorized_operations = authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            image,
            ctx.principal,
            ctx.peer,
            krabka_metadata::ResourceType::Group,
            gid,
        );
    }
    row
}
