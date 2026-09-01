//! Reading a group's offset state out of its coordinator actor.
//!
//! Both `OffsetFetch` request shapes need the same name-keyed offset map, and
//! both reach it the same way: find or create the group's actor and ask it for
//! `FetchOffsets`. An actor that has gone away yields an empty view rather
//! than an error, which is the "no committed offsets" answer the response
//! already encodes.
//!
//! The reply carries the stable offsets and the `(topic, partition)` keys an
//! unresolved transaction has written, because KIP-447's `require_stable`
//! decides between the two per partition and must see one consistent snapshot
//! of both.

use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag},
        group::GroupOffsets,
    },
};

/// Fetches the group's committed offsets, keyed by topic name and partition,
/// together with the keys its open transactions have not resolved yet.
pub(super) async fn fetch_offsets(broker: &Broker, group_id: &str) -> GroupOffsets {
    let handle = broker.group_coordinator.find(group_id).unwrap_or_else(|| {
        broker
            .group_coordinator
            .get_or_create_group(group_id, GroupKindTag::Classic)
    });
    let (reply, response) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .is_err()
    {
        return GroupOffsets::default();
    }
    response.await.unwrap_or_default()
}
