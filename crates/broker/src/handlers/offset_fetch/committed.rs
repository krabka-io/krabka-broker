//! Reading a group's committed offsets out of its coordinator actor.
//!
//! Both `OffsetFetch` request shapes need the same name-keyed offset map, and
//! both reach it the same way: find or create the group's actor and ask it for
//! `FetchCommitted`. An actor that has gone away yields an empty map rather
//! than an error, which is the "no committed offsets" answer the response
//! already encodes.

use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    coordinator::unified::actor::{GroupActorMessage, GroupKindTag},
};

/// Fetches every committed offset the group's actor holds, keyed by topic name
/// and partition.
pub(super) async fn fetch_committed(
    broker: &Broker,
    group_id: &str,
) -> std::collections::HashMap<(String, i32), crate::coordinator::unified::classic_state::OffsetEntry>
{
    let handle = broker.group_coordinator.find(group_id).unwrap_or_else(|| {
        broker
            .group_coordinator
            .get_or_create_group(group_id, GroupKindTag::Classic)
    });
    let (reply, response) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::FetchCommitted { reply })
        .await
        .is_err()
    {
        return std::collections::HashMap::new();
    }
    response.await.unwrap_or_default()
}
